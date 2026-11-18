"""规则吃单 (rules.py, 1.153.0): the rules' windows in New York time, the book's offer, paper and real takes, the gates, the orders
to Predict's final word, the settlement, and what the positions check and the loss stop count. Entirely offline."""
import asyncio
import copy
import datetime as dt
import hashlib
import json
import sys
import time
import types
import unittest
from unittest import mock
from decimal import Decimal as D
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import offline  # noqa: F401
import main as m
import live as L
import preopen as p
import rules as R

BJ = m.BEIJING


def bj(y, mo, d, h, mi=0):
    return int(dt.datetime(y, mo, d, h, mi, tzinfo=BJ).timestamp() * 1000)


# Tuesday 2026-11-17, 05:30 Beijing = Monday 16:30 New York (EST): gold's shut quarter hours start at 06:00 Beijing
NOW = bj(2026, 11, 17, 5, 30)
GOLD_0600 = bj(2026, 11, 17, 6, 0)


def gold_market(start, mid=901, status="OPEN"):
    return {"id": mid, "categorySlug": f"xau-updown-15m-{start // 1000}", "conditionId": "0xc", "status": "REGISTERED", "tradingStatus": status,
            "question": "Gold Up or Down", "feeRateBps": 200,
            "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": str(mid * 2)}, {"name": "Down", "indexSet": 2, "onChainId": str(mid * 2 + 1)}]}


class Api:
    """GET /categories/<slug> (404 until listed), GET /markets/<id>/orderbook, the details, the orders."""
    def __init__(self, taker):
        self.taker = taker
        self.markets: dict[str, dict] = {}
        self.listed: set[str] = set()
        self.books: dict[str, dict] = {}
        self.sent, self.removed, self.rows = [], [], {}
        self.fill = {}          # hash -> shares filled at once
        self.price = None       # the averagePrice an order record states (None: none)
        self.timeout = False    # the next create_order's answer goes missing (the order exists)
        self.blocked_until = 0.0
        self.calls = []
        self.seen_state = []    # the record's order state as each create_order came in (the hash saved before the request)

    async def call(self, method, path, **kwargs):
        self.calls.append(path)
        if path.startswith("/categories/"):
            slug = path.removeprefix("/categories/")
            mkt = next((x for x in self.markets.values() if x["categorySlug"] == slug), None)
            if mkt is None or slug not in self.listed:
                raise m.RemoteError(f"HTTP 404: no category {slug}")
            return {"success": True, "data": {"slug": slug, "markets": [{"id": mkt["id"], "conditionId": mkt["conditionId"], "title": slug}]}}
        if path.startswith("/markets/") and path.endswith("/orderbook"):
            mid = path.split("/")[2]
            return {"success": True, "data": copy.deepcopy(self.books.get(mid, {"bids": [], "asks": []}))}
        raise m.RemoteError("HTTP 404")

    async def market(self, mid):
        return copy.deepcopy(self.markets[str(mid)])

    async def ensure_auth(self):
        pass

    async def create_order(self, body):
        signed = body["data"]["order"]
        rec = self.taker.record(self.taker.sending[signed["hash"].lower()])
        self.seen_state.append(rec["orders"][-1]["state"])
        if why := self.taker.before_send(body):
            raise p.NotSent(why)
        self.sent.append(copy.deepcopy(body))
        oid = str(len(self.sent))
        shares = L.from_wei(int(signed["takerAmount"]))
        got = self.fill.get(len(self.sent), 0.0)
        row = {"id": oid, "amount": signed["takerAmount"], "amountFilled": str(L.to_wei(got)), "status": "FILLED" if got >= shares - 1e-9 else "OPEN"}
        if self.price is not None:
            row["averagePrice"] = str(self.price)
        self.rows[signed["hash"].lower()] = row
        if self.timeout:
            self.timeout = False
            raise m.RemoteError("timeout: delivery unknown")
        return {"order_id": oid, "hash": signed["hash"]}

    async def order(self, oid, digest=""):
        return copy.deepcopy(self.rows.get(digest.lower()))

    async def remove_orders(self, ids):
        self.removed.extend(ids)
        for row in self.rows.values():
            if row["id"] in ids and row["status"] == "OPEN":
                row["status"] = "CANCELLED"
        return {"removed": list(ids), "noop": []}


def taker(mode="on", **env):
    # RULE_FAST_HOURS off unless a test asks: the suite's clock (05:30 Beijing) is inside the default per-second hours (01:00–07:00, 1.154.12),
    # and these tests are about the cadence outside them
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "STATE_DB": ":memory:", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64,
                             "RULE_TAKER": mode, "LIVE_MAX_ORDER_USD": "500", "LIVE_MAX_DAILY_LOSS_USD": "100", "RULE_FAST_HOURS": "off", **env})
    clock = types.SimpleNamespace(ms=NOW)
    wallet = types.SimpleNamespace(maker="0x" + "1" * 40,
                                   sign_order=lambda order, *flags: ("0x" + hashlib.sha256(json.dumps(order, sort_keys=True).encode()).hexdigest(), "0xsig"))
    notes, redeemed = [], []
    state = types.SimpleNamespace(paused="", killed="", day=0.0, usd=1000.0)
    owner = types.SimpleNamespace(
        config=cfg, market=types.SimpleNamespace(now_ms=lambda: clock.ms), stopping=asyncio.Event(), store=m.Store(":memory:"),
        live=types.SimpleNamespace(wallet=wallet, chain=types.SimpleNamespace(allowance=_allowance), ready=True, not_ready_why="",
                                   api=types.SimpleNamespace(blocked_until=0.0), usd=lambda: state.usd),
        live_paused=lambda: state.paused, live_killed=lambda: state.killed, sim_trades=lambda: {}, live_daily_pnl=lambda trades, now: state.day,
        preopen_pnl=lambda: 0.0, live_loss_floor=lambda cap: -cap, live_notify=lambda text, key="": notes.append(text),
        live_redeem_soon=lambda mid="": redeemed.append(mid))
    t = R.RuleTaker(owner)
    api = Api(t)
    t.api = api
    return t, api, clock, types.SimpleNamespace(notes=notes, redeemed=redeemed, state=state, owner=owner)


async def _allowance(key):
    return 10 ** 30


async def run_until(t, clock, ms, step=500):
    while clock.ms < ms:
        await t.tick()
        clock.ms += step
    await t.tick()


def connected(t, mid, book, age=0.0):
    """The push connection up and holding this market's book, pushed ``age`` seconds ago (a quiet market's is not sent again)."""
    t.stream.ws = t.stream.ws or object()
    t.stream.last_frame = time.monotonic()
    t.stream.subscribed.add(mid)
    t.stream.take(mid, book)
    bids, asks, at = t.stream.books[mid]
    t.stream.books[mid] = (bids, asks, at - age)
    t.stream.changed.discard(mid)


# the per-second hours alone: the shut quarter hours' windows, no makers' moment (RULE_LEAD_MINUTES: its own test)
FAST = {"RULE_FAST_HOURS": "01:00-07:00", "RULE_LEAD_MINUTES": "0", "RULE_OIL": "off", "RULE_GOLD_OPEN_CENTS": "0", "RULE_GOLD_0515_CENTS": "0"}


# --- 1.154.14: bought in the window's course (盘中吃单) ---
XAU = next(c for c in m.WINDOW_UPDOWN_MARKETS if c.key == "XAU-15M")
GOLD_0700 = bj(2026, 11, 17, 7, 0)  # the first quarter hour after CME's daily break (18:00 New York)
# gold's shut quarter hours alone, no makers' moment
IN = {"RULE_OIL": "off", "RULE_GOLD_OPEN_CENTS": "0", "RULE_GOLD_0515_CENTS": "0", "RULE_LEAD_MINUTES": "0"}


def gold_card(x, clock, start, line, price, strike=None, age_ms=0):
    """The bot's own XAU card (main.IndexUpDownMarket) on the window that starts at ``start``: the close of the index's minute
    candle ending there (``line``; None: not read yet), the index now (``age_ms`` old) and the start price Predict shows."""
    spec = XAU.window_spec(start)
    card = m.IndexUpDownMarket(x.owner.store, spec)
    if line is None:
        x.owner.store.delete_keys([f"updown:{spec.slug}:start"])
    else:
        x.owner.store.put(f"updown:{spec.slug}:start", {"open": spec.start_ms, "close": str(line), "read_ms": clock.ms})
    card.price = D(str(price)) if price is not None else None
    card.priced_ms = card.index_ms = clock.ms - age_ms
    card.strike = D(str(strike)) if strike is not None else None
    x.owner.updowns = {"XAU-15M": card}
    return card


class RuleTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self):
        # a taker these tests set "running" by hand never ends its run: what its ticks held of the collector (gc_guard) is let go
        while m.GC_WATCH.holders:
            m.GC_WATCH.release()

    def test_windows_follow_new_york_time(self):
        """The shut quarter hours, the daily reopen and 05:15 in New York time: winter and summer, weekdays only, the weekend's
        reopen and a holiday's left out."""
        self.assertEqual([R.daily_break(bj(2026, 11, 17, 6, mi)) for mi in (0, 15, 30, 45)], [True] * 4)  # Tue 06:00–06:45 Beijing (EST)
        self.assertFalse(R.daily_break(bj(2026, 11, 17, 7, 0)))
        self.assertFalse(R.daily_break(bj(2026, 11, 21, 6, 0)), "Saturday morning Beijing: the weekend's shut, not the daily break")
        self.assertTrue(R.daily_break(bj(2026, 7, 14, 5, 0)) and not R.daily_break(bj(2026, 7, 14, 6, 0)), "summer: 05:00–05:45 Beijing")
        self.assertTrue(R.daily_reopen(bj(2026, 11, 17, 7, 0)) and R.daily_reopen(bj(2026, 7, 14, 6, 0)))
        self.assertFalse(R.daily_reopen(bj(2026, 11, 16, 7, 0)), "Monday 07:00 Beijing: Sunday's reopen after the weekend")
        self.assertFalse(R.daily_reopen(bj(2026, 11, 27, 7, 0)), "Thanksgiving: shut from noon, the 18:00 reopen ends six hours")
        self.assertTrue(R.daily_reopen(bj(2026, 11, 26, 7, 0)), "the Wednesday before: an ordinary reopen")
        self.assertTrue(R.ny_0515(bj(2026, 11, 17, 18, 15)) and R.ny_0515(bj(2026, 7, 14, 17, 15)))
        self.assertFalse(R.ny_0515(bj(2026, 7, 14, 18, 15)), "summer: Beijing 18:15 is 06:15 New York")
        self.assertFalse(R.ny_0515(bj(2026, 11, 21, 18, 15)), "Saturday: CME shut")
        # 1.153.2: the slot scan's three, in New York time like the rest
        g0500, g1645, o0045 = (R.RULE_OF[k].matches for k in ("gold_0500", "gold_1645", "oil_0045"))
        self.assertTrue(g0500(bj(2026, 11, 17, 18, 0)) and g0500(bj(2026, 7, 14, 17, 0)) and not g0500(bj(2026, 7, 14, 18, 0)))
        self.assertTrue(g1645(bj(2026, 11, 18, 5, 45)) and g1645(bj(2026, 11, 21, 5, 45)), "Friday's last before the weekend counts")
        self.assertFalse(g1645(bj(2026, 11, 27, 5, 45)), "Thanksgiving: shut from noon")
        self.assertTrue(o0045(bj(2026, 11, 17, 13, 45)) and not o0045(bj(2026, 11, 22, 13, 45)), "Sunday: shut")
        week = R.windows(R.RULE_OF["gold_shut"], bj(2026, 11, 16, 0), bj(2026, 11, 23, 0))
        self.assertEqual(len(week), 16)  # Tuesday to Friday mornings, four each
        self.assertEqual({R.bj(s, False) for s in week}, {"06:00", "06:15", "06:30", "06:45"})
        self.assertEqual([R.bj(s) for s in R.windows(R.RULE_OF["gold_open"], bj(2026, 11, 16, 0), bj(2026, 11, 23, 0))],
                         ["11-17 07:00", "11-18 07:00", "11-19 07:00", "11-20 07:00"])

    def test_the_book_offers_the_side_at_or_under_the_price(self):
        bids, asks = ((D("0.47"), D("20")), (D("0.45"), D("30")), (D("0.40"), D("50"))), ((D("0.51"), D("30")), (D("0.55"), D("40")), (D("0.60"), D("100")))
        self.assertEqual(R.without([(D("0.51"), D("30")), (D("0.55"), D("40"))], D("45")), [(D("0.55"), D("25"))], "less what was taken from the cheapest")
        self.assertEqual(R.without([(D("0.51"), D("30"))], D("0")), [(D("0.51"), D("30"))])
        self.assertEqual(R.offered(bids, asks, "up", D("0.58")), [(D("0.51"), D("30")), (D("0.55"), D("40"))])
        self.assertEqual(R.offered(bids, asks, "down", D("0.55")), [(D("0.53"), D("20")), (D("0.55"), D("30"))], "an Up bid at p is Down for sale at 1 − p")
        self.assertEqual(R.walk(R.offered(bids, asks, "up", D("0.58")), D("50")), (D("0.51") * 30 + D("0.55") * 20) / 50)

    def test_settings_and_control_page(self):
        base = {"TELEGRAM_BOT_TOKEN": "offline"}
        c = m.Config.from_env(base).rules
        self.assertEqual((c.mode, c.price("gold_shut"), c.price("gold_open"), c.price("gold_0515"), c.price("oil_shut"), c.price("oil_open")),
                         ("off", 58, 50, 58, 0, 50), "1.153.2: oil's reopen at 50¢")
        self.assertEqual((c.price("gold_0500"), c.price("gold_1645"), c.price("oil_0045")), (0, 0, 0), "the scan's three: off until tried")
        self.assertEqual((c.on("gold"), c.on("oil"), c.shares_of("gold"), c.shares_of("oil"), c.max_day_usd), (True, True, 50, 50, 0.0))
        c = m.Config.from_env({**base, "RULE_TAKER": "on", "RULE_OIL": "off", "RULE_SHARES": "30", "RULE_GOLD_SHARES": "100", "RULE_MAX_DAY_USD": "250"}).rules
        self.assertEqual((c.on("gold"), c.on("oil"), c.shares_of("gold"), c.shares_of("oil"), c.max_day_usd), (True, False, 100, 30, 250.0),
                         "1.153.0's RULE_SHARES is the default of both")
        for bad in ({"RULE_GOLD": "maybe"}, {"RULE_OIL_SHARES": "0"}, {"RULE_MAX_DAY_USD": "-1"}):
            with self.assertRaises(ValueError, msg=str(bad)):
                m.Config.from_env({**base, "RULE_TAKER": "paper", **bad})
        with self.assertRaises(ValueError):
            m.Config.from_env({**base, "RULE_TAKER": "on", "RULE_GOLD_SHUT_CENTS": "99"})
        with self.assertRaises(ValueError):
            m.Config.from_env({**base, "RULE_TAKER": "maybe"})
        self.assertEqual(m.Config.from_env({**base, "RULE_GOLD_SHUT_CENTS": "99"}).rules.price("gold_shut"), 58, "off: read leniently")
        keys = {"RULE_TAKER", "RULE_MAX_DAY_USD", "RULE_FAST_HOURS", *m.RULE_SETTING_KEYS.values(), *(k for pair in m.RULE_MARKET_KEYS.values() for k in pair)}
        self.assertTrue(keys <= m.CONTROL_KEY_SET and all(m.CONTROL_FORMS[k]["group"] == "规则" for k in keys))
        # 1.154.12: the per-second hours on the control page — a text setting, every preset a value the settings take
        form = m.CONTROL_FORMS["RULE_FAST_HOURS"]
        self.assertEqual((form["kind"], form["presets"][:2], form["captions"]["off"]), ("text", ["off", "01:00-07:00"], "不做每秒检查"))
        for preset in form["presets"]:
            self.assertEqual(m.day_spans_text(m.Config.from_env({**base, "RULE_TAKER": "on", "RULE_FAST_HOURS": preset}).rules.fast), preset)
        bot = types.SimpleNamespace(config=m.Config.from_env({**base, "RULE_TAKER": "paper", "RULE_OIL_SHUT_CENTS": "45"}))
        value = lambda key: m.Bot.control_value(bot, key)
        self.assertEqual((value("RULE_TAKER"), value("RULE_OIL_SHUT_CENTS"), value("RULE_GOLD_SHARES"), value("RULE_GOLD_SHUT_CENTS"), value("RULE_OIL"), value("RULE_MAX_DAY_USD")),
                         ("paper", "45", "50", "58", "on", "0"))
        self.assertEqual(value("RULE_FAST_HOURS"), "01:00-07:00")
        # ...saved from the page like the others: taken at once, a wrong one refused with the reason, off switches the hours off
        saver = m.Bot(m.Config.from_env({**base, "RULE_TAKER": "paper", "WEB_CONTROL_KEY": "k" * 12}), m.Store(":memory:"),
                      types.SimpleNamespace(now_ms=lambda: NOW), None)
        answer = saver.control_set({"RULE_FAST_HOURS": "00:55-02:00"})
        self.assertTrue(answer["ok"] and "RULE_FAST_HOURS=00:55-02:00（原 01:00-07:00）" in answer["message"], answer)
        self.assertEqual(saver.config.rules.fast, ((55, 120),))
        refused = saver.control_set({"RULE_FAST_HOURS": "凌晨一点"})
        self.assertTrue(not refused["ok"] and "RULE_FAST_HOURS 写成 01:00-07:00 这样" in refused["message"] and saver.config.rules.fast == ((55, 120),), refused)
        self.assertTrue(saver.control_set({"RULE_FAST_HOURS": "off"})["ok"] and saver.config.rules.fast == ())
        self.assertEqual((R.review({}, saver.config.rules, NOW)["fast"], R.review({}, m.Config.from_env({**base, "RULE_TAKER": "on"}).rules, NOW)["fast"]),
                         ("off", "01:00-07:00"), "the review page's 规则吃单 says the hours")
        self.assertIn("规则吃单在真实交易模块里", m.Bot.cmd_rules(bot, None))

    async def test_paper_records_what_it_would_have_bought(self):
        """paper: the listing found, matching waited for, the book taken up to RULE_SHARES at the book's prices — no order; one
        window bought once; settled on Predict's result, not counted against the day's loss stop."""
        t, api, clock, x = taker("paper")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        mkt = api.markets["901"] = gold_market(GOLD_0600, status="MATCHING_NOT_ENABLED")
        api.books["901"] = {"bids": [[0.45, 30]], "asks": [[0.51, 30], [0.55, 40], [0.60, 100]]}
        await t.tick()
        self.assertIn(slug, t.track)
        self.assertEqual(t.track[slug]["word"], "还没发布")
        api.listed.add(slug)
        clock.ms += 3_000
        await t.tick()
        self.assertEqual(t.track[slug]["word"], "还没发布", "half an hour out, a day past its listing moment: looked for every 30 s")
        clock.ms += 28_000
        await t.tick()
        self.assertEqual(t.track[slug]["word"], "盘口已有 ≤58¢ 的涨 70 份，等撮合开启就买（现在 MATCHING_NOT_ENABLED）")
        mkt["tradingStatus"] = "OPEN"
        clock.ms += int(R.OFFER_STATUS_S * 1000)  # an offer waiting: the status read every OFFER_STATUS_S, bought the moment it opens
        await t.tick()
        rec = t.record(slug)
        self.assertIsNotNone(rec)
        order = rec["orders"][0]
        self.assertEqual((rec["mode"], rec["side"], order["paper"], order["filled"], round(order["avg"], 4)), ("paper", "up", True, 50.0, 0.526))
        self.assertEqual(api.sent, [])
        self.assertIn("🧪 规则吃单（只记录，没下单）", x.notes[-1])
        clock.ms += 61_000
        await t.tick()
        self.assertEqual(len(t.record(slug)["orders"]), 1, "bought up to RULE_SHARES: no more")
        self.assertEqual(t.track[slug]["word"], "这一场已买满 50 份")
        self.assertEqual(R.filled_by_outcome(t.recs, clock.ms), {}, "paper: no position to explain")
        # the result: Up won
        clock.ms = rec["end_ms"] + R.SETTLE_AFTER_MS
        mkt["outcomes"][0]["status"] = "WON"
        await t.tick()
        rec = t.record(slug)
        self.assertEqual((rec["result"], round(rec["pnl"], 4)), ("up", round(50 * (1 - R.fee_share(0.526, 200)) - 50 * 0.526, 4)),
                         "the shares held (the fee taken in shares) paid out, less the USDT spent")
        self.assertEqual(t.today_pnl(), 0.0, "paper does not count against the loss stop")
        review = R.review(t.recs, t.settings, clock.ms)
        row = next(r for r in review["rules"] if r["rule"] == "gold_shut" and r["mode"] == "paper")
        self.assertEqual((row["windows"], row["shares"], row["wins"], row["settled"]), (1, 50.0, 1, 1))
        self.assertIn("黄金休市 4 档 买涨 ≤58¢", t.status())

    async def test_real_orders_are_taken_and_their_rest_withdrawn(self):
        """on: a LIMIT order at the rule's price — with this much on offer, for all the window may still buy (RULE_SHARES; 1.154.15) —
        its hash saved before the request, signed to expire before the start, not post-only; read FILL_READ_MS on, its rest
        withdrawn, read to Predict's final word; the fills counted by the positions check, the P&L by the loss stop."""
        t, api, clock, x = taker("on", RULE_SHARES="40")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30], [0.57, 30]]}
        api.fill[1] = 30.0  # what the book really had when the order came: 30 of 40
        api.price = 0.51
        await t.tick()
        self.assertEqual(len(api.sent), 1)
        body = api.sent[0]["data"]
        self.assertEqual((body["strategy"], "isPostOnly" in body), ("LIMIT", False))
        signed = body["order"]
        self.assertEqual(L.from_wei(int(signed["takerAmount"])), 40.0)
        self.assertEqual(int(signed["expiration"]), GOLD_0600 // 1000 - R.EXPIRY_BEFORE_S)
        self.assertEqual(signed["tokenId"], "1802", "the Up token")
        self.assertEqual(api.seen_state, ["placing"], "the hash saved before the request")
        self.assertEqual(R.unverified(t.recs), {("901", 1)}, "in flight: the positions check leaves the outcome alone")
        clock.ms += R.FILL_READ_MS
        await t.tick()
        order = t.record(slug)["orders"][0]
        self.assertEqual((order["state"], api.removed), ("cancelling", ["1"]))
        clock.ms += R.FOLLOW_EVERY_MS
        await t.tick()
        order = t.record(slug)["orders"][0]
        self.assertEqual((order["state"], order["filled"], order["avg"]), ("closing", 30.0, 0.51), "off the book: final only LATE_FILL_MS on")
        self.assertEqual(R.unverified(t.recs), {("901", 1)}, "a late fill may still show: the positions check waits")
        clock.ms += 10_000
        await t.tick()
        self.assertEqual(len(api.sent), 1, "closing counts in full against the window: no second order meanwhile")
        api.rows[order["hash"]]["amountFilled"] = str(L.to_wei(32))  # two more matched just before the cancel, shown late
        clock.ms = order["left_ms"] + R.LATE_FILL_MS
        await t.tick()
        order = t.record(slug)["orders"][0]
        self.assertEqual((order["state"], order["filled"]), ("final", 32.0), "the late fill booked")
        self.assertTrue(any("🎯 规则吃单成交" in n for n in x.notes))
        fee = R.fee_share(0.51, 200)
        self.assertAlmostEqual(R.filled_by_outcome(t.recs, clock.ms)[("901", 1)], 32 * (1 - fee), 6, "what the account holds: the fee taken in shares")
        self.assertEqual(R.unverified(t.recs), set())
        # the book again: 30 more at 57¢ — 8 left of the 40
        api.books["901"] = {"bids": [], "asks": [[0.57, 30]]}
        api.fill[2] = 8.0
        api.price = None
        clock.ms += 31_000
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[1]["data"]["order"]["takerAmount"])), 8.0)
        clock.ms += R.FILL_READ_MS
        await t.tick()
        rec = t.record(slug)
        self.assertEqual((R.filled(rec), round(R.cost(rec), 6)), (40.0, round(32 * 0.51 + 8 * 0.58, 6)),
                         "USDT: the price a share (its limit when none is stated); the fee is paid in shares")
        clock.ms += 31_000
        await t.tick()
        self.assertEqual(len(api.sent), 2, "40 bought: the window is full")
        # settled: Down won — a loss, counted against the day
        clock.ms = rec["end_ms"] + R.SETTLE_AFTER_MS
        api.markets["901"]["outcomes"][1]["status"] = "WON"
        await t.tick()
        rec = t.record(slug)
        self.assertEqual(rec["result"], "down")
        self.assertAlmostEqual(t.today_pnl(), -R.cost(rec), 6)
        self.assertIn("❌ 规则吃单结算", x.notes[-1])
        self.assertEqual(x.redeemed, [], "a loss: nothing to redeem")

    async def test_the_gates_hold_real_orders(self):
        """No real order while real trading is paused, killed, off, at the loss stop, cooling down, short of USDT — nor within
        STOP_MS of the start."""
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        for change, word in ((lambda x: setattr(x.state, "paused", "管理员"), "真实交易已暂停"), (lambda x: setattr(x.state, "killed", "亏损"), "今日停止开新仓"),
                             (lambda x: setattr(x.state, "day", -100.0), "已达 LIVE_MAX_DAILY_LOSS_USD"), (lambda x: setattr(x.state, "usd", 0.5), "USDT 余额 $0.50 不够一张最小订单")):
            t, api, clock, x = taker("on")
            api.markets["901"] = gold_market(GOLD_0600)
            api.listed.add(slug)
            api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
            change(x)
            await t.tick()
            self.assertEqual(api.sent, [], word)
            self.assertIn(word, t.track[slug]["word"])
        t, api, clock, x = taker("on", LIVE="pause")
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        await t.tick()
        self.assertEqual(api.sent, [])
        self.assertIn("LIVE=pause", t.track[slug]["word"])
        t, api, clock, x = taker("on")
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        clock.ms = GOLD_0600 - R.STOP_MS + 1
        await t.tick()
        self.assertEqual(api.sent, [], "within STOP_MS of the start")
        self.assertNotIn(slug, t.track)

    async def test_a_lost_answer_is_found_by_its_hash(self):
        t, api, clock, x = taker("on")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 50]]}
        api.timeout, api.fill[1] = True, 50.0
        await t.tick()
        order = t.record(slug)["orders"][0]
        self.assertEqual((order["state"], api.removed), ("unknown", []), "no answer, no id: nothing to withdraw yet")
        clock.ms += 31_000
        await t.tick()
        self.assertEqual(len(api.sent), 1, "never sent again while its fate is unknown")
        clock.ms += R.UNKNOWN_EVERY_MS
        await t.tick()
        order = t.record(slug)["orders"][0]
        self.assertEqual((order["state"], order["filled"], order["id"]), ("final", 50.0, "1"))
        # one that never reached Predict: given up GIVE_UP_MS past its expiry
        t, api, clock, x = taker("on")
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        api.timeout = True
        await t.tick()
        api.rows.clear()
        order = t.record(slug)["orders"][0]
        clock.ms = order["expires"] * 1000 + R.GIVE_UP_MS
        await t.tick()
        self.assertEqual(t.record(slug)["orders"][0]["state"], "not_found")
        self.assertEqual(R.filled(t.record(slug)), 0.0)

    async def test_a_failing_read_keeps_nothing_else_waiting(self):
        """One window's book read failing: noted on that window; the orders in flight are still followed in the same tick."""
        t, api, clock, x = taker("on")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        later = bj(2026, 11, 17, 6, 15)  # the next shut window, listed too
        slug2 = f"xau-updown-15m-{later // 1000}"
        api.markets["901"], api.markets["902"] = gold_market(GOLD_0600), gold_market(later, mid=902)
        api.listed |= {slug, slug2}
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        await t.tick()
        self.assertEqual((len(api.sent), t.track[slug2]["word"]), (1, "盘口没有 ≤58¢ 的涨（没人卖）"))
        reads = api.call

        async def failing(method, path, **kwargs):
            if path.endswith("/orderbook"):
                raise m.RemoteError("HTTP 502: bad gateway")
            return await reads(method, path, **kwargs)
        api.call = failing
        clock.ms += int(R.SLOW_S * 1000)
        await t.tick()
        self.assertEqual(api.removed, ["1"], "the first window's order followed despite the second's failing read")
        self.assertIn("读取失败", t.track[slug2]["word"])
        self.assertIn("HTTP 502", t.error)

    async def test_paper_records_a_window_once(self):
        """paper buys nothing: the offer it recorded is still on the book — recorded again, it would be counted twice."""
        t, api, clock, x = taker("paper")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        await t.tick()
        clock.ms += 31_000
        await t.tick()
        rec = t.record(slug)
        self.assertEqual((len(rec["orders"]), R.filled(rec)), (1, 30.0))
        self.assertIn("这一场已记过", t.track[slug]["word"])

    async def test_its_own_pre_open_bids_are_never_bought(self):
        """Gold's 05:15 window may be one the pre-open mode quotes: its Up bid at 49¢ is a Down offer at 51¢ — not taken."""
        t, api, clock, x = taker("on")
        start = bj(2026, 11, 17, 18, 15)
        clock.ms = start - 20 * 60_000
        slug = f"xau-updown-15m-{start // 1000}"
        api.markets["903"] = gold_market(start, mid=903)
        api.listed.add(slug)
        api.books["903"] = {"bids": [[0.49, 10], [0.40, 25]], "asks": [[0.53, 40]]}  # Down offers at 51¢ (its own) and 60¢ (over the price)
        pair = {"slug": slug, "price": "0.49", "shares": "10", "phase": "active",
                "orders": {"up": {"state": "open", "filled": 0.0}, "down": {"state": "open", "filled": 0.0}}}
        x.owner.preopen = types.SimpleNamespace(ledger=types.SimpleNamespace(rows={slug: pair}))
        await t.tick()
        self.assertEqual(api.sent, [])
        self.assertIn("盘口没有 ≤50¢ 的跌（最低 51¢）", t.track[slug]["word"], "priced a cent under its own offer at 51¢")
        # someone else's 5 beside its own at 51¢ are not bought (a buy there takes its own first); 7 at 50¢ (an Up bid at 50¢) are
        api.books["903"] = {"bids": [[0.50, 7], [0.49, 15]], "asks": [[0.53, 40]]}
        clock.ms += 31_000
        await t.tick()
        body = api.sent[0]["data"]["order"]
        self.assertEqual((L.from_wei(int(body["takerAmount"])), L.from_wei(int(body["makerAmount"]))), (7.0, 3.5), "7 at the 50¢ limit")
        self.assertEqual(body["tokenId"], "1807", "the Down token")

    async def test_a_refusal_pauses_the_window(self):
        t, api, clock, x = taker("on")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        sent = api.create_order

        async def refuse(body):
            await sent(body)
            raise L.OrderRejected("下单被拒绝：HTTP 400: invalid order")
        api.create_order = refuse
        await t.tick()
        self.assertEqual(t.record(slug)["orders"][0]["state"], "rejected")
        clock.ms += int(R.SLOW_S * 1000)
        await t.tick()
        self.assertEqual(len(api.sent), 1, "not again within REJECT_PAUSE_MS")
        for _ in range(R.MAX_REJECTS + 2):
            clock.ms += R.REJECT_PAUSE_MS + int(R.SLOW_S * 1000)
            await t.tick()
        self.assertEqual(len(api.sent), R.MAX_REJECTS)
        self.assertIn(f"拒绝了 {R.MAX_REJECTS} 次", t.track[slug]["word"])

    async def test_each_market_on_its_own_with_its_own_size(self):
        """RULE_GOLD / RULE_OIL switch a market's rules on or off by themselves; RULE_GOLD_SHARES / RULE_OIL_SHARES size each."""
        t, api, clock, x = taker("on", RULE_OIL="off", RULE_GOLD_SHARES="20")
        await t.tick()
        self.assertTrue(t.track and all(R.RULE_OF[w["rule"]].market == "gold" for w in t.track.values()), "oil switched off: none of its windows")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 300]]}
        clock.ms += int(R.SLOW_S * 1000)
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[0]["data"]["order"]["takerAmount"])), 20.0, "RULE_GOLD_SHARES")
        t.configure(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "RULE_TAKER": "on", "RULE_GOLD": "off"}).rules)
        await t.tick()
        self.assertTrue(t.track and all(R.RULE_OF[w["rule"]].market == "oil" for w in t.track.values()), "gold off, oil on: only oil's windows")
        status = t.status()
        self.assertIn("黄金：关", status)
        self.assertIn("原油：开｜每场最多 50 份", status)

    async def test_the_day_is_capped(self):
        """RULE_MAX_DAY_USD: the orders placed in a Beijing day — fills at their price and fee, open ones in full at the limit —
        stop at the cap."""
        t, api, clock, x = taker("on", RULE_MAX_DAY_USD="20")
        s1, s2 = (f"xau-updown-15m-{GOLD_0600 // 1000}", f"xau-updown-15m-{(GOLD_0600 + 15 * 60_000) // 1000}")
        api.markets["901"], api.markets["902"] = gold_market(GOLD_0600), gold_market(GOLD_0600 + 15 * 60_000, mid=902)
        api.listed |= {s1, s2}
        api.books["901"] = {"bids": [], "asks": [[0.51, 100]]}
        api.books["902"] = {"bids": [], "asks": [[0.51, 100]]}
        api.fill[1] = api.fill[2] = 34.0
        api.price = 0.51
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[0]["data"]["order"]["takerAmount"])), 34.0, "$20 at the 58¢ limit: 34 shares")
        self.assertIn("到了每天上限", t.track[s2]["word"], "the first order open: counted in full at its limit")
        clock.ms += R.FILL_READ_MS
        await t.tick()
        clock.ms += int(R.SLOW_S * 1000)
        await t.tick()
        # 34 × (51¢ + fee) = $17.67 spent: $2.33 left, 4 shares at the 58¢ limit
        self.assertEqual(L.from_wei(int(api.sent[1]["data"]["order"]["takerAmount"])), 4.0)
        clock.ms += R.FILL_READ_MS
        await t.tick()
        clock.ms += int(R.SLOW_S * 1000)
        await t.tick()
        self.assertEqual(len(api.sent), 2)
        self.assertIn("到了每天上限", t.track[s2]["word"])
        self.assertIn("每天最多花 $20", t.status())

    async def test_an_offer_is_bought_the_moment_matching_opens(self):
        """An offer at the rule's price before matching opens: the status read every OFFER_STATUS_S (every FAST_S near the
        start), and bought in that same look once it says OPEN."""
        t, api, clock, x = taker("on")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        mkt = api.markets["901"] = gold_market(GOLD_0600, status="MATCHING_NOT_ENABLED")
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        await t.tick()
        self.assertEqual((api.sent, t.track[slug]["offer"]), ([], True))
        clock.ms += int(R.OFFER_STATUS_S * 1000) - 1
        await t.tick()
        self.assertEqual(sum(1 for c in api.calls if c.endswith("/orderbook")), 1, "not looked at again before OFFER_STATUS_S")
        mkt["tradingStatus"] = "OPEN"
        clock.ms += 1
        await t.tick()
        self.assertEqual(len(api.sent), 1)

    async def test_an_offer_waiting_for_matching_has_its_status_asked_at_once_and_fast(self):
        """1.154.22: an offer at the rule's price on a market whose matching has not been seen open — its status is asked beside
        the looks, at once and every STATUS_RUSH_S, each read ahead of the key's other reads (priority); the moment it says OPEN
        the window is looked at and bought, and the order's record says how long the offer waited for that. 11-18 13:15:07: the
        status had just been read, was not read again for FAST_S, and the order left 2.7 s after the offer's book had arrived."""
        t, api, clock, x = taker("on")
        asked: list = []

        async def market(mid, priority=False):
            asked.append(priority)
            return copy.deepcopy(api.markets[str(mid)])
        api.market = market
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        mkt = api.markets["901"] = gold_market(GOLD_0600, status="MATCHING_NOT_ENABLED")
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        with mock.patch.object(R, "STATUS_RUSH_S", 0.01):
            await t.tick()  # listed and read: the offer is there, matching is not open
            w = t.track[slug]
            self.assertEqual((api.sent, w["offer"], t.rushing(w)), ([], True, True))
            self.assertIn("等撮合开启就买（现在 MATCHING_NOT_ENABLED，每 0.01 秒读一次状态）", w["word"])
            listing = len(asked)
            await asyncio.sleep(0.08)
            reads = asked[listing:]
            self.assertGreaterEqual(len(reads), 3, "the status asked again and again, no look needed")
            self.assertTrue(all(reads), "each ahead of the key's other reads")
            self.assertEqual((api.sent, w["open_ms"], [c for c in api.calls if c.endswith("/orderbook")]), ([], 0, ["/markets/901/orderbook"]),
                             "no order yet, and the book not read again for it")
            mkt["tradingStatus"] = "OPEN"
            await asyncio.sleep(0.05)
            self.assertTrue(w["open_ms"] and not t.rushing(w) and "901" in t.stream.changed and t.stream.pushed.is_set(), "open: the loop woken for this window")
            self.assertEqual((t.stream.arrivals != t.looked, t.rest_s()), (True, R.REST_SOON_S), "as a change that has just come: no tenth of a second's rest before its look")
            n = len(asked)
            await t.tick()  # (the clock has not moved: the look is this change's)
            self.assertEqual(len(api.sent), 1, "bought at that look")
            order = t.record(slug)["orders"][0]
            self.assertEqual((order["open_reads"], order["open_wait"] >= 0, len(asked) - n), (w["rush_reads"], True, 0), "no request more before the order")
            self.assertRegex(R.race_text(R.races(t.recs, "on", 0)[0]), r"卖单先到、等撮合开启 \d+ ms（读了 \d+ 次状态）")
            await asyncio.sleep(0.03)
            self.assertEqual(len(asked), n, "open: asked no more")
        # the offer gone before matching opens: the asking ends; its next showing is asked about afresh — RUSH_READS_MAX reads a window
        t2, api2, clock2, _ = taker("on")
        mkt2 = api2.markets["901"] = gold_market(GOLD_0600, status="MATCHING_NOT_ENABLED")
        api2.listed.add(slug)
        api2.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        with mock.patch.object(R, "STATUS_RUSH_S", 0.01):
            await t2.tick()
            w2 = t2.track[slug]
            self.assertTrue(t2.rushing(w2))
            api2.books["901"] = {"bids": [], "asks": []}
            clock2.ms += int(R.OFFER_STATUS_S * 1000)
            await t2.tick()
            await asyncio.sleep(0.03)
            self.assertEqual((w2["offer"], t2.rushing(w2), api2.sent), (False, False, []))
            api2.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
            clock2.ms += int(R.CLOSED_SLOW_S * 1000)
            await t2.tick()
            self.assertTrue(t2.rushing(w2), "shown again: asked about again")
            w2["rush_reads"] = R.RUSH_READS_MAX
            await asyncio.sleep(0.03)
            self.assertFalse(t2.rushing(w2))
        # never faster than the key's own pace (the reads may borrow as an order does: what they borrowed the order could not),
        # shared by the windows rushing at once — gold and oil have their quarter hours in common
        with mock.patch.object(m, "PREDICT_PACING", True), mock.patch.dict(m.PREDICT_RATE, {"per_min": 220.0}):
            self.assertAlmostEqual(t2.rush_pace(), 0.3)
            with mock.patch.dict(m.PREDICT_RATE, {"per_min": 600.0}):
                self.assertEqual(t2.rush_pace(), R.STATUS_RUSH_S)
            waiting = [asyncio.ensure_future(asyncio.sleep(30)) for _ in range(2)]
            t2.rush_tasks.update(waiting)
            self.assertAlmostEqual(t2.rush_pace(), 0.6, msg="two windows: half the pace each")
            for task in waiting:
                task.cancel()
            await asyncio.gather(*waiting, return_exceptions=True)
            t2.rush_tasks.difference_update(waiting)
        self.assertEqual((t2.rush_pace(), t2.rush_tasks), (R.STATUS_RUSH_S, set()), "the pacing off (these tests): the set pace; no rush left behind")
        # 只记录 asks nothing fast (no order to be first with)
        t3, api3, clock3, _ = taker("paper")
        api3.markets["901"] = gold_market(GOLD_0600, status="MATCHING_NOT_ENABLED")
        api3.listed.add(slug)
        api3.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        await t3.tick()
        self.assertFalse(t3.rushing(t3.track[slug]))
        self.assertIn("等撮合开启就买（现在 MATCHING_NOT_ENABLED）", t3.track[slug]["word"])

    async def test_an_orders_request_leaves_within_its_look(self):
        """1.154.22: an order signed is on its way before the tick goes on to the next window — it used to start only when the
        tick next waited for something: the looks of the windows after it, and their reads, stood between an order signed and
        its request."""
        t, api, clock, x = taker("on")
        events: list = []
        call, create = api.call, api.create_order

        async def logged_call(method, path, **kwargs):
            events.append(path)
            return await call(method, path, **kwargs)

        async def logged_create(body):
            events.append("order")
            return await create(body)
        api.call, api.create_order = logged_call, logged_create
        t.journal = lambda rec, order: True  # (as with a store on disk: the order's line written to the taker's journal, no thread to wait for)
        slugs = [f"xau-updown-15m-{(GOLD_0600 + i * 900_000) // 1000}" for i in range(2)]
        for i, mid in enumerate(("901", "902")):
            api.markets[mid] = gold_market(GOLD_0600 + i * 900_000, mid=int(mid))
            api.listed.add(slugs[i])
            api.books[mid] = {"bids": [], "asks": [[0.70, 30]]}
        await t.tick()
        self.assertEqual(api.sent, [])
        api.books["901"] = {"bids": [], "asks": [[0.52, 25]]}
        clock.ms += int(R.SLOW_S * 1000)
        del events[:]
        await t.tick()
        self.assertEqual(len(api.sent), 1)
        self.assertEqual([e for e in events if e == "order" or e.endswith("/orderbook")], ["/markets/901/orderbook", "order", "/markets/902/orderbook"],
                         "the first window's order out before the second window is read")
        order = t.record(slugs[0])["orders"][0]
        self.assertLess(order["sent_wall"] - order["signed_wall"], 500)
        row = R.races(t.recs, "on", 0)[0]["orders"][0]
        self.assertEqual((row["wake"] in (None, 0), row["prep"] >= 0, row["out"] >= 0), (True, True, True), "a book the look read itself: no wait for the look to count")

    async def test_at_the_makers_moment_a_market_not_seen_open_is_asked_for_its_status(self):
        """1.154.22: at the makers' moment the reads beside the pushes are of the window's status, not its book, until matching is
        seen open — the book comes pushed, and an offer found on it would wait for that very read."""
        own, api, clock, _ = taker("on", RULE_FAST_HOURS="off", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0",
                                   RULE_API_KEY="rule-key", PREDICT_API_KEY="main-key")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        mkt = api.markets["901"] = gold_market(GOLD_0600, status="MATCHING_NOT_ENABLED")
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": []}
        clock.ms = GOLD_0600 - 300 * 60_000 - 5_000  # five seconds before the 06:00 window's moment
        hold, own.verify_tick = own.verify_tick, lambda: None
        await own.tick()
        own.verify_tick = hold
        connected(own, "901", {"bids": [], "asks": []}, age=300)
        w = own.track[slug]
        self.assertTrue(own.hot(w, clock.ms) and not w["open_ms"])
        asked: list = []
        market = api.market

        async def logged(mid):
            asked.append(str(mid))
            return await market(mid)
        api.market = logged
        books = lambda: sum(1 for c in api.calls if c.endswith("/orderbook"))
        n = books()
        for _ in range(2):
            own.verify_at -= R.VERIFY_EVERY_OWN_S
            w["rest_at"] = time.monotonic() - 60
            own.verify_tick()
            await own.verifying
        # (the sprint's own preparation may read it once more beside these: warm)
        self.assertEqual((set(asked), len(asked) >= 2, books() - n, own.status_reads, w["open_ms"]), ({"901"}, True, 0, 2, 0), "its status, not its book")
        mkt["tradingStatus"] = "OPEN"
        own.stream.take("901", {"version": 9, "bids": [], "asks": [[0.52, 25]]})  # the makers' offer, pushed
        await own.tick()
        self.assertEqual(own.api.sent, [], "matching not seen open yet")
        own.verify_at -= R.VERIFY_EVERY_OWN_S
        w["rest_at"] = time.monotonic() - 60
        own.stream.changed.clear()
        own.verify_tick()
        await own.verifying
        self.assertTrue(w["open_ms"] and "901" in own.stream.changed and own.stream.pushed.is_set(), "seen open with the offer in hand: looked at now")
        await own.tick()
        self.assertEqual(len(api.sent), 1)
        # the push down: the read beside it stays the book's — the looks go on on those reads alone, and without them the window
        # would wait for a book that never comes (its status asked forty times, its offer never seen)
        down, api5, clock5, _ = taker("on", RULE_FAST_HOURS="off", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0",
                                      RULE_API_KEY="rule-key", PREDICT_API_KEY="main-key")
        api5.markets["901"] = gold_market(GOLD_0600, status="MATCHING_NOT_ENABLED")
        api5.listed.add(slug)
        api5.books["901"] = {"bids": [], "asks": [[0.70, 25]]}  # (nothing at the rule's price yet: no rush of its own)
        clock5.ms = GOLD_0600 - 300 * 60_000 - 5_000
        hold5, down.verify_tick = down.verify_tick, lambda: None
        await down.tick()
        down.verify_tick = hold5
        w5 = down.track[slug]
        self.assertTrue(down.hot(w5, clock5.ms) and not w5["open_ms"] and not down.stream.alive())
        n5 = sum(1 for c in api5.calls if c.endswith("/orderbook"))
        for _ in range(3):
            down.verify_at -= R.VERIFY_EVERY_OWN_S
            w5["rest_at"] = time.monotonic() - 60
            down.verify_tick()
            await down.verifying
        self.assertEqual((sum(1 for c in api5.calls if c.endswith("/orderbook")) - n5, down.status_reads), (3, 0), "no push: its book, as before")
        self.assertEqual(down.rush_tasks, set())
        own.verify_at -= R.VERIFY_EVERY_OWN_S
        w["rest_at"] = time.monotonic() - 60
        w["look_ms"] = 0
        n, m_asked = books(), len(asked)
        if not own.finished(slug, R.RULE_OF[w["rule"]]):
            own.verify_tick()
            await own.verifying
            self.assertEqual((books() - n, len(asked) - m_asked), (1, 0), "open: the book again")

    async def test_from_a_minute_before_its_hours_the_rule_taker_goes_first(self):
        """1.154.23 — "每天凌晨 00:59 分开始规则吃单的优先级就要调到最高". From WARM_MS before the per-second hours to their end (and
        at a makers' moment outside them) a pushed book is looked at in the turn of the loop that brought it — the order signed and
        its request handed over before take() even returns, where the loop's own tick came round three or four turns later, each
        behind whatever else was ready; one tick at a time all the same; and around the makers' moment of a window it can still
        buy the rest of the bot is told to keep out of the loop's way (quiet_ms)."""
        day = lambda h, mi, s=0: bj(2026, 11, 17, h, mi) + s * 1000
        t, api, clock, x = taker("on", RULE_FAST_HOURS="01:00-07:00", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0")
        t.verify_tick = lambda: None
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.70, 30]]}
        # when it goes first: 00:59 to 07:00 — a minute before the hours, to their end
        for at, first in ((day(0, 58, 59), False), (day(0, 59), True), (day(1, 0), True), (day(6, 59, 59), True), (day(7, 0), False), (day(12, 0), False)):
            clock.ms = at
            t.fast = t.settings.fast_at(at)  # (as pace finds it at each tick)
            self.assertEqual(t.first_now(), first, m.stamp(at))
        clock.ms, t.fast = day(12, 0), True  # a window at the makers' moment or bought in its course, outside the hours (pace: sprint, course)
        self.assertTrue(t.first_now())
        t.fast = False
        paper, *_ = taker("paper", RULE_FAST_HOURS="01:00-07:00")
        paper.fast = True
        self.assertFalse(paper.first_now(), "只记录: no order to be first with")
        # 01:00:03, the taker's loop running: the pushed offer is bought before take() returns
        clock.ms = day(1, 0, 3)
        await t.tick()  # (listed, its book read; the hours found running)
        w = t.track[slug]
        self.assertTrue(t.fast and t.first_now() and w["open_ms"])
        connected(t, "901", {"bids": [], "asks": [[0.70, 30]]})
        t.running, t.stream.on_change = True, t.pushed_now  # as run() sets them
        t.stream.pushed.clear()  # (the loop at rest, waiting for it)
        ticks, asked = t.ticks, len(api.calls)
        t.stream.take("901", {"version": 3, "bids": [], "asks": [[0.52, 25]]})  # no await from here to the checks: this very turn of the loop
        if R.EAGER:
            self.assertEqual((len(api.sent), t.first_ticks, t.ticks - ticks), (1, 1, 1), "looked at, signed and its request made within take()")
            self.assertEqual(L.from_wei(int(api.sent[0]["data"]["order"]["takerAmount"])), 50.0)
            self.assertEqual((api.calls[asked:], t.tick_lock.locked(), "901" in t.stream.changed), ([], False, False), "no request before the order; the tick over")
            self.assertFalse(t.stream.pushed.is_set(), "taken whole here: the loop is not woken for it (a second tick with nothing to do)")
        else:
            self.assertEqual((api.sent, t.first_ticks), ([], 0), "an older Python: the loop's own tick takes it")
        # one tick at a time: a book pushed while a tick runs or waits for its turn (an order's answer awaited) is left to the
        # loop — woken for it, as ever
        sent = len(api.sent)
        t.ticking += 1
        t.stream.take("901", {"version": 4, "bids": [], "asks": [[0.51, 40]]})
        t.ticking -= 1
        self.assertEqual((len(api.sent), t.first_ticks, t.stream.pushed.is_set()), (sent, 1 if R.EAGER else 0, True))
        self.assertIn("901", t.stream.changed)
        # nor once the taker or the bot is stopping (the loop begins no tick then either): no order begun into a shutdown
        for version, stopping in ((5, t.stop), (6, x.owner.stopping)):
            stopping.set()
            t.stream.take("901", {"version": version, "bids": [], "asks": [[0.51, 40 + version]]})
            stopping.clear()
            self.assertEqual((len(api.sent), t.first_ticks), (sent, 1 if R.EAGER else 0))
        # not while the loop is not running (a tick driven by hand: these tests), nor outside the hours
        t.running = False
        t.stream.take("901", {"version": 7, "bids": [], "asks": [[0.50, 40]]})
        t.running, clock.ms, t.fast = True, day(12, 0), False
        t.stream.take("901", {"version": 8, "bids": [], "asks": [[0.49, 40]]})
        self.assertEqual((len(api.sent), t.first_ticks), (sent, 1 if R.EAGER else 0))
        # a tick that fails is noted, as the loop notes it — never the stream's own failure
        clock.ms, t.fast = day(1, 0, 5), True
        t.turn = mock.AsyncMock(side_effect=RuntimeError("坏了"))
        t.stream.take("901", {"version": 9, "bids": [], "asks": [[0.48, 40]]})
        await asyncio.sleep(0)
        if R.EAGER:
            self.assertEqual((t.error, t.tick_lock.locked()), ("坏了", False))
        del t.turn
        # the taker's own loop running (run): the same — and the loop's tick, come round, finds the change taken
        live, api5, clock5, _ = taker("on", RULE_FAST_HOURS="01:00-07:00", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0")
        live.verify_tick = lambda: None
        api5.markets["901"] = gold_market(GOLD_0600)
        api5.listed.add(slug)
        api5.books["901"] = {"bids": [], "asks": [[0.70, 30]]}
        clock5.ms = day(1, 0, 3)

        async def no_stream():
            await asyncio.Event().wait()
        live.stream.run = no_stream
        runner = asyncio.create_task(live.run())
        for _ in range(50):
            await asyncio.sleep(0.002)
            if live.track.get(slug, {}).get("market") is not None and live.fast:
                break
        self.assertTrue(live.running and live.stream.on_change == live.pushed_now and live.first_now())
        connected(live, "901", {"bids": [], "asks": [[0.70, 30]]})
        before = live.first_ticks
        live.stream.take("901", {"version": 3, "bids": [], "asks": [[0.52, 25]]})
        if R.EAGER:
            self.assertEqual((len(api5.sent), live.first_ticks - before), (1, 1), "bought in the turn of the loop that brought the book")
        await asyncio.sleep(0.05)
        self.assertEqual(len(api5.sent), 1, "once: the loop's own tick places nothing more")
        live.stop.set()
        await asyncio.wait_for(runner, 5)
        self.assertEqual((live.running, live.first_tasks, live.rush_tasks), (False, set(), set()))
        # stopped while a tick a pushed book began still waits for its order's answer: that tick ends first — the answer onto
        # the record — as the loop's own would have (cancelled, the record stayed "placing" with no id)
        slow, api7, clock7, _ = taker("on", RULE_FAST_HOURS="01:00-07:00", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0")
        slow.verify_tick = lambda: None
        api7.markets["901"] = gold_market(GOLD_0600)
        api7.listed.add(slug)
        api7.books["901"] = {"bids": [], "asks": [[0.70, 30]]}
        clock7.ms = day(1, 0, 3)
        slow.stream.run = no_stream
        runner7 = asyncio.create_task(slow.run())
        for _ in range(50):
            await asyncio.sleep(0.002)
            if slow.track.get(slug, {}).get("market") is not None and slow.fast:
                break
        connected(slow, "901", {"bids": [], "asks": [[0.70, 30]]})
        answer, create7 = asyncio.Event(), api7.create_order

        async def answered_late(body):
            await answer.wait()
            return await create7(body)
        api7.create_order = answered_late
        slow.stream.take("901", {"version": 3, "bids": [], "asks": [[0.52, 25]]})
        if R.EAGER:
            self.assertEqual((len(slow.first_tasks), slow.record(slug)["orders"][0]["state"]), (1, "placing"))
        slow.stop.set()
        await asyncio.sleep(0.02)
        self.assertFalse(runner7.done() and R.EAGER, "the taker waits for the tick in progress")
        answer.set()
        await asyncio.wait_for(runner7, 5)
        order7 = slow.record(slug)["orders"][0]
        self.assertEqual((order7["state"], order7["id"], slow.first_tasks), ("open", "1", set()))
        # the threads gone (the process ending): a request that could not be handed to one was never sent — not "unknown"
        gone, api8, clock8, _ = taker("on", RULE_FAST_HOURS="01:00-07:00", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0")
        gone.verify_tick = lambda: None
        api8.markets["901"] = gold_market(GOLD_0600)
        api8.listed.add(slug)
        api8.books["901"] = {"bids": [], "asks": [[0.52, 25]]}
        clock8.ms = day(1, 0, 3)

        async def no_threads(body):
            raise RuntimeError("cannot schedule new futures after shutdown")
        api8.create_order = no_threads
        await gone.tick()
        order8 = gone.record(slug)["orders"][0]
        self.assertEqual((order8["state"], order8["error"], gone.finished(slug, R.RULE_OF[gone.track[slug]["rule"]])), ("not_sent", "进程正在停止，请求没有发出", ""))
        # ...and with the real client (the request path of production: the sign-in in hand, the key's pace, the gate, the check
        # right before the bytes): the POST is handed to a thread of the rule taker's own before take() returns
        real, api6, clock6, x6 = taker("on", RULE_FAST_HOURS="01:00-07:00", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0")
        real.verify_tick = lambda: None
        api6.markets["901"] = gold_market(GOLD_0600)
        api6.listed.add(slug)
        api6.books["901"] = {"bids": [], "asks": [[0.70, 30]]}
        clock6.ms = day(1, 0, 3)
        await real.tick()
        client = p.safe_api("https://api.invalid/v1", "rule-key", None, real.before_send, None)
        client.jwt, client.pool = "token", m.first_pool()
        real.api = client
        connected(real, "901", {"bids": [], "asks": [[0.70, 30]]})
        real.running, real.stream.on_change = True, real.pushed_now
        handed, threads = [], []
        submit = client.pool.submit

        def fake_get(url, payload=None, timeout=15, headers=None):
            threads.append((__import__("threading").current_thread().name, url, (headers or {}).get("x-api-key")))
            return json.dumps({"success": True, "data": {"orderId": "77", "orderHash": payload["data"]["order"]["hash"]}}).encode()
        client.pool.submit = lambda fn, *args, **kwargs: handed.append(args[0] if args else "") or submit(fn, *args, **kwargs)
        with mock.patch.object(m, "_http_get", fake_get):
            try:
                real.stream.take("901", {"version": 3, "bids": [], "asks": [[0.52, 25]]})
                if R.EAGER:
                    self.assertEqual(handed, ["https://api.invalid/v1/orders"], "the order's request with its thread before take() returns")
                for _ in range(200):
                    await asyncio.sleep(0.005)
                    if real.record(slug) and real.record(slug)["orders"] and real.record(slug)["orders"][0]["state"] == "open":
                        break
            finally:
                del client.pool.submit
        order6 = real.record(slug)["orders"][0]
        self.assertEqual((order6["state"], order6["id"], [(name.startswith("predict-first"), url, key) for name, url, key in threads]),
                         ("open", "77", [(True, "https://api.invalid/v1/orders", "rule-key")]))
        self.assertLessEqual(order6["sent_wall"] - order6["look_wall"], 500, "from the look to the request: this turn of the loop (ms on the wall clock: a busy builder's allowance)")
        for task in list(real.first_tasks):
            await asyncio.wait_for(task, 2)
        # the status says it
        line = t.first_line(day(1, 0, 5))
        self.assertIn("优先：每天 00:59–07:00（每秒检查时段和它前 1 分钟）、对手挂单时刻前后和盘中吃单时，规则吃单排最前——", line)
        self.assertIn("请求走规则吃单自己的线程；对手挂单时刻前 1 秒到后 15 秒、卖单已到而撮合还没开启时（都要这一场还能买），预挂不发新单", line)
        own, *_ = taker("on", RULE_FAST_HOURS="01:00-07:00", RULE_API_KEY="rule-key", PREDICT_API_KEY="main-key")
        self.assertIn("请求走规则吃单自己的线程；这把 Key 不外借；对手挂单时刻前 1 秒到后 15 秒", own.first_line(day(1, 0, 5)))
        self.assertIn("｜现在：优先中", line)
        t.fast = False
        self.assertIn("｜现在不在优先时段，下一次 00:59 开始", t.first_line(day(12, 0)))
        self.assertEqual(paper.first_line(day(1, 0)), "")

    async def test_around_the_makers_moment_the_rest_of_the_bot_keeps_out_of_the_way(self):
        """1.154.23: quiet_ms — the ms left of the seconds around a makers' moment (QUIET_BEFORE_MS before it to QUIET_AFTER_MS
        after) of a window the taker can still buy: what the pre-open mode's new pairs, the model trader's step and the minute's
        checks wait out. Nothing to wait for once the window is bought, has no price, or no order could go out."""
        t, api, clock, x = taker("on", RULE_FAST_HOURS="off", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0")
        t.verify_tick = lambda: None
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": []}
        moment = GOLD_0600 - 300 * 60_000  # 01:00:00: the 06:00 window's makers post (RULE_LEAD_MINUTES 300)
        clock.ms = moment - 30_000
        await t.tick()
        for at, left in ((moment - R.QUIET_BEFORE_MS - 1, 0), (moment - R.QUIET_BEFORE_MS, R.QUIET_AFTER_MS + R.QUIET_BEFORE_MS), (moment, R.QUIET_AFTER_MS),
                         (moment + 4_780, R.QUIET_AFTER_MS - 4_780), (moment + R.QUIET_AFTER_MS - 1, 1), (moment + R.QUIET_AFTER_MS, 0)):
            self.assertEqual(t.quiet_ms(at), left, at - moment)
        clock.ms = moment + 3_000
        t.quiet_held = (time.monotonic() + 3600, 4_321)  # (worked out "just now")
        self.assertEqual(t.quiet_ms(), 4_321, "worked out at most every 50 ms (asked before every pair of the pre-open mode)")
        t.quiet_held = (0.0, 0)
        self.assertEqual((t.quiet_ms(), t.quiet_held[1]), (R.QUIET_AFTER_MS - 3_000, R.QUIET_AFTER_MS - 3_000))
        # nobody waits for an order that cannot go out, a window with no price, one bought in full, or 只记录
        x.state.paused = "测试暂停"
        self.assertEqual(t.quiet_ms(moment), 0)
        x.state.paused = ""
        self.assertEqual(t.quiet_ms(moment), R.QUIET_AFTER_MS)
        rec = t.new_record(slug, t.track[slug], R.RULE_OF[t.track[slug]["rule"]], 1)
        rec["orders"].append({"state": "final", "shares": float(t.settings.shares_of("gold")), "filled": float(t.settings.shares_of("gold")), "limit": 0.6})
        t.save(rec)
        self.assertEqual(t.quiet_ms(moment), 0, "bought in full")
        t.recs.clear()
        none, api0, clock0, _ = taker("on", RULE_FAST_HOURS="off", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0", RULE_GOLD_SHUT_CENTS="0")
        api0.markets["901"] = gold_market(GOLD_0600)
        api0.listed.add(slug)
        clock0.ms = moment - 30_000
        await none.tick()
        self.assertEqual(none.quiet_ms(moment), 0, "no price: nothing to buy before its start")
        paper, api1, clock1, _ = taker("paper", RULE_FAST_HOURS="off", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0")
        api1.markets["901"] = gold_market(GOLD_0600)
        api1.listed.add(slug)
        clock1.ms = moment - 30_000
        await paper.tick()
        self.assertEqual(paper.quiet_ms(moment), 0)
        self.assertIn("｜现在：优先中，其他活儿正在让路（还有 13 秒）", (t.__setattr__("fast", True), t.first_line(moment + 2_000))[1])
        # ...and no automatic garbage collection from GC_BEFORE_MS before the moment to the end of those seconds: a collection of
        # the oldest generation stops the whole process for a quarter of a second and more with the bot's records (GcWatch)
        import gc
        watch = m.GC_WATCH
        held0 = watch.held
        self.assertTrue(gc.isenabled() and not watch.holders)
        t.gc_guard(moment)
        self.assertTrue(gc.isenabled() and not t.gc_off, "the taker's own loop not running (a tick by hand): left alone")
        t.running = True
        try:
            for at, off in ((moment - R.GC_BEFORE_MS - R.QUIET_BEFORE_MS - 1, False), (moment - R.GC_BEFORE_MS - R.QUIET_BEFORE_MS, True), (moment, True),
                            (moment + R.QUIET_AFTER_MS - 1, True), (moment + R.QUIET_AFTER_MS, False)):
                t.gc_guard(at)
                self.assertEqual((t.gc_off, gc.isenabled(), watch.holders), (off, not off, int(off)), at - moment)
            self.assertEqual(watch.held - held0, 1, "held once for the whole stretch")
            # the taker stopping lets go of it: never left off
            t.gc_guard(moment)
            self.assertFalse(gc.isenabled())

            async def no_stream():
                await asyncio.Event().wait()
            t.stream.run, t.running = no_stream, False
            t.stop.set()
            t.gc_off = True  # (as the guard left it)
            await asyncio.wait_for(t.run(), 5)
            self.assertTrue(gc.isenabled() and not t.gc_off and not watch.holders)
        finally:
            while watch.holders:
                watch.release()
        # what /diag says of the collector: a collection of the oldest generation, timed
        watch.start()
        try:
            before = len(watch.runs)
            gc.collect()
            self.assertEqual((len(watch.runs) - before, watch.runs[-1][1]), (1, 2))
            self.assertRegex(watch.line(), r"^最近 10 分钟整堆回收 \d+ 次、共 \d+\.\d 秒、最长 \d+ ms.*整堆回收时整个进程停着，盘口和下单都在等；规则吃单抢单的十几秒里不自动回收，本次启动 \d+ 次）$")
        finally:
            watch.stop()
        self.assertTrue(gc.isenabled() and watch.note not in gc.callbacks)

    async def test_a_pushed_offer_is_bought_at_once(self):
        """The listed windows' books come pushed (RuleStream): a book that changed wakes the loop and its window is looked at
        at once — no waiting for the read cadence, no REST read of the book while the push is fresh."""
        t, api, clock, x = taker("on")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.70, 30]]}
        await t.tick()
        self.assertEqual((api.sent, t.stream.wanted), ([], {"901"}), "listed: its book wanted pushed")
        sent: list = []
        frames: asyncio.Queue = asyncio.Queue()

        class Socket:
            async def send(self, text): sent.append(json.loads(text))
            async def recv(self): return await frames.get()
            async def close(self): pass

        async def connect(url, headers):
            self.assertEqual(url, m.PREDICT_WS_URL)
            return Socket()
        t.stream.connect = connect
        streaming = asyncio.create_task(t.stream.run())
        for _ in range(50):
            await asyncio.sleep(0)
        self.assertEqual(sent[0], {"method": "subscribe", "requestId": 1, "params": ["predictOrderbook/901"]})
        await frames.put(json.dumps({"type": "R", "requestId": 1, "success": True}))
        await frames.put(json.dumps({"type": "M", "topic": "heartbeat", "data": 7}))
        await frames.put(json.dumps({"type": "M", "topic": "predictOrderbook/901", "data": {"version": 2, "bids": [], "asks": [[0.52, 25]]}}))
        for _ in range(50):
            await asyncio.sleep(0)
        self.assertIn({"method": "heartbeat", "data": 7}, sent)
        self.assertTrue(t.stream.pushed.is_set() and "901" in t.stream.changed)
        reads = sum(1 for c in api.calls if c.endswith("/orderbook"))
        clock.ms += 1_000  # a second on: far from its read cadence (SLOW_S), looked at because the push changed it
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[0]["data"]["order"]["takerAmount"])), 50.0, "25 on offer: an order for all the window may buy (1.154.15)")
        self.assertEqual((t.record(slug)["orders"][0]["seen"], t.record(slug)["orders"][0]["fresh"]), (25.0, True), "what the book showed; its look answered a push")
        self.assertEqual(sum(1 for c in api.calls if c.endswith("/orderbook")), reads, "the pushed book, not a REST read")
        await frames.put(json.dumps({"type": "M", "topic": "predictOrderbook/901", "data": {"version": 1, "bids": [], "asks": [[0.40, 99]]}}))
        for _ in range(20):
            await asyncio.sleep(0)
        self.assertEqual(t.stream.book("901")[1], ((D("0.52"), D("25")),), "an older version is dropped")
        t.stop.set()
        await asyncio.wait_for(streaming, 2)

    async def test_each_watch_is_kept_for_tuning(self):
        """1.153.2: each watched window's watch kept on its record at its end — the lowest offer of the side, the most shares
        offered at the price at once, when matching opened — one bought from and one that never offered at the price alike."""
        t, api, clock, x = taker("paper")
        s1, s2 = f"xau-updown-15m-{GOLD_0600 // 1000}", f"xau-updown-15m-{(GOLD_0600 + 15 * 60_000) // 1000}"
        mkt = api.markets["901"] = gold_market(GOLD_0600, status="MATCHING_NOT_ENABLED")
        api.markets["902"] = gold_market(GOLD_0600 + 15 * 60_000, mid=902)
        api.listed |= {s1, s2}
        api.books["901"] = {"bids": [], "asks": [[0.51, 30], [0.55, 12]]}
        api.books["902"] = {"bids": [], "asks": [[0.62, 80]]}
        await t.tick()
        mkt["tradingStatus"] = "OPEN"
        clock.ms = GOLD_0600 - 20 * 60_000  # forty minutes on: the status read, bought
        await t.tick()
        self.assertEqual(R.filled(t.record(s1)), 42.0)
        clock.ms = GOLD_0600 + 15 * 60_000 - R.STOP_MS  # both windows past their end of watch
        await t.tick()
        w1, w2 = t.record(s1)["seen"], t.record(s2)["seen"]
        self.assertEqual((w1["best"], w1["offer_max"], w1["open_ms"] > 0, w1["start_ms"]), (0.51, 42.0, True, GOLD_0600))
        self.assertEqual((w2["best"], w2["offer_max"], t.record(s2)["orders"]), (0.62, 0.0, []), "no offer at the price: watched, nothing bought")
        st = R.watch_stats(t.recs, "gold_shut", "paper", 0)
        self.assertEqual((st["watched"], st["offered"], st["offer_avg"], round(st["best_mid"], 4)), (2, 1, 42.0, 0.565))
        self.assertEqual(st["open_lead"], (20 * 60 + 45 * 60) / 2, "opened 20 minutes before the first's start, at listing for the second")
        self.assertIn("近 30 天看了 2 场，1 场有 ≤58¢ 的卖单（平均 42 份），最低卖价中位 56¢，撮合开启在开场前中位 1950 秒", t.status())
        row = next(r for r in R.review(t.recs, t.settings, clock.ms)["rules"] if r["rule"] == "gold_shut" and r["mode"] == "paper")
        self.assertEqual(row["watch"]["watched"], 2)
        self.assertEqual(R.filled_by_outcome(t.recs, clock.ms), {}, "a watch-only record holds nothing")

    async def test_the_loop_never_holds_the_bot(self):
        """1.153.3: a pushed change of a window no look acts on (an order still in flight there) used to send run() round without
        a wait, starving the event loop — the whole bot frozen. The loop now always waits; the change is taken as the look begins."""
        t, api, clock, x = taker("on")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        await t.tick()
        self.assertEqual(len(api.sent), 1)  # in flight now: every look returns early
        never = asyncio.Event()

        async def hang(url, headers):
            await never.wait()
        t.stream.connect = hang
        beats = 0

        async def heartbeat():
            nonlocal beats
            while True:
                beats += 1
                await asyncio.sleep(0.01)
        ticks = 0
        tick = t.tick

        async def counted():
            nonlocal ticks
            ticks += 1
            await tick()
            t.stream.changed.add("901")  # a change keeps coming, still there as the loop rests
        t.tick = counted
        running, beating = asyncio.create_task(t.run()), asyncio.create_task(heartbeat())
        await asyncio.sleep(0.5)
        t.stop.set()
        await asyncio.wait_for(running, 2)
        beating.cancel()
        self.assertGreater(beats, 20, "the rest of the bot keeps running")
        self.assertLess(ticks, 20, f"{ticks} ticks in half a second: the loop waits between them")

    async def test_paper_then_real_keep_their_own_records(self):
        """1.153.3: the recommended rollout — paper first, then on, within a window's watch: the real order on a record of its own,
        followed, counted by the positions check and the day's cap; the paper one apart. A record mixing them (an older version's)
        is split at the start."""
        t, api, clock, x = taker("paper")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 50]]}
        await t.tick()
        self.assertEqual(R.filled(t.record(slug, "paper")), 50.0)
        t.configure(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "RULE_TAKER": "on"}).rules)
        api.fill[1], api.price = 50.0, 0.51
        clock.ms += int(R.SLOW_S * 1000)
        await t.tick()
        real = t.record(slug, "on")
        self.assertEqual((len(api.sent), real["mode"], len(real["orders"]), len(t.record(slug, "paper")["orders"])), (1, "on", 1, 1))
        self.assertEqual(set(t.recs), {slug, f"paper:{slug}"})
        clock.ms += R.FILL_READ_MS
        await t.tick()
        self.assertEqual(t.record(slug, "on")["orders"][0]["state"], "final", "followed: filled in full")
        self.assertAlmostEqual(R.filled_by_outcome(t.recs, clock.ms)[("901", 1)], 50 * (1 - R.fee_share(0.51, 200)), 6, "the real one alone")
        self.assertAlmostEqual(R.spent_on(t.recs, R.day_of(clock.ms), "on"), 50 * 0.51, 6)
        # an older version's mixed record: split as the taker starts
        store = m.Store(":memory:")
        mixed = {"slug": slug, "rule": "gold_shut", "side": "up", "mode": "paper", "market_id": "901", "index_set": 1, "start_ms": GOLD_0600,
                 "orders": [{"paper": True, "state": "final", "shares": 10.0, "filled": 10.0, "avg": 0.51, "fee_bps": 200},
                            {"state": "final", "shares": 5.0, "filled": 5.0, "avg": 0.52, "hash": "0xab", "id": "9", "fee_bps": 200}], "pnl": None}
        store.put(R.PREFIX + slug, mixed)
        x.owner.store = store
        again = R.RuleTaker(x.owner)
        self.assertEqual({k: (r["mode"], len(r["orders"])) for k, r in again.recs.items()}, {slug: ("on", 1), f"paper:{slug}": ("paper", 1)})
        self.assertEqual({k for k, _ in store.items(R.PREFIX)}, {R.PREFIX + slug, R.PREFIX + f"paper:{slug}"})

    async def test_every_order_ends_before_the_start(self):
        """1.153.3: signed to expire 5 s before the start however late it goes out (an order of the last 70 s was signed for 65 s,
        matchable up to a minute into the window); one that stays on the book past its expiry is final GIVE_UP_MS on, on the fills
        known; a 429 is the rate limiter's, not a refusal of the order."""
        t, api, clock, x = taker("on")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        clock.ms = GOLD_0600 - 20_000
        await t.tick()
        self.assertEqual(int(api.sent[0]["data"]["order"]["expiration"]), GOLD_0600 // 1000 - 5)
        api.remove_orders = lambda ids: asyncio.sleep(0, {"removed": [], "noop": []})  # the removal never confirmed
        order = t.record(slug)["orders"][0]
        for _ in range(3):
            clock.ms += 2_000
            await t.tick()
        self.assertEqual(t.record(slug)["orders"][0]["state"], "cancelling")
        clock.ms = order["expires"] * 1000 + R.GIVE_UP_MS
        await t.tick()
        self.assertEqual(t.record(slug)["orders"][0]["state"], "final", "past its expiry: final on what is known")
        t2, api2, clock2, _ = taker("on")
        api2.markets["901"] = gold_market(GOLD_0600)
        api2.listed.add(slug)
        api2.books["901"] = {"bids": [], "asks": [[0.51, 30]]}

        async def limited(body):
            raise m.RemoteError("HTTP 429: too many requests")
        api2.create_order = limited
        await t2.tick()
        self.assertEqual(t2.record(slug)["orders"][0]["state"], "not_sent", "429: not one of MAX_REJECTS")

    def test_per_second_hours_setting(self):
        """1.154.12 — RULE_FAST_HOURS: the Beijing hours in which every listed window is looked at each second (凌晨 1 点后每秒检查),
        01:00–07:00 by default; several spans, one over midnight, the whole day, off; a wrong value refused while the taker is on."""
        cfg = lambda **env: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", **env}).rules
        day = lambda h, mi: bj(2026, 11, 18, h, mi)
        s = cfg(RULE_TAKER="on")
        self.assertEqual((s.fast, m.day_spans_text(s.fast)), (((60, 420),), "01:00-07:00"))
        self.assertEqual([s.fast_at(day(h, mi)) for h, mi in ((0, 59), (1, 0), (6, 59), (7, 0))], [False, True, True, False])
        self.assertEqual((s.fast_in(day(0, 59)), s.fast_in(day(3, 0)), s.fast_in(day(7, 0))), (60_000, 0, 18 * 3_600_000))
        over = cfg(RULE_TAKER="on", RULE_FAST_HOURS="23:00～02:00, 13-14:30")
        self.assertEqual((over.fast, m.day_spans_text(over.fast)), (((1380, 120), (780, 870)), "23:00-02:00,13:00-14:30"))
        self.assertEqual([over.fast_at(day(h, mi)) for h, mi in ((22, 59), (23, 0), (0, 30), (1, 59), (2, 0), (13, 0), (14, 30))],
                         [False, True, True, True, False, True, False])
        off = cfg(RULE_TAKER="on", RULE_FAST_HOURS="off")
        self.assertEqual((off.fast, off.fast_in(day(1, 0)), off.fast_at(day(1, 0)), m.day_spans_text(off.fast)), ((), None, False, "off"))
        whole = cfg(RULE_TAKER="on", RULE_FAST_HOURS="00:00-24:00")
        self.assertTrue(all(whole.fast_at(day(h, 0)) for h in (0, 12, 23)))
        for bad in ("7", "25:00-26:00", "01:00-01:00", "01:70-02:00", "一点到七点"):
            with self.assertRaises(ValueError, msg=bad):
                cfg(RULE_TAKER="on", RULE_FAST_HOURS=bad)
        self.assertEqual(cfg(RULE_TAKER="off", RULE_FAST_HOURS="7").fast, ((60, 420),), "off: read leniently, as the other rule settings are")

    async def test_per_second_hours_look_every_second_at_the_book_in_hand(self):
        """1.154.12: in the hours every listed window is looked at each second — on the book the push connection holds, which the
        connection vouches for while it lives (a quiet market's book is not sent again), no REST read a look; an offer at the
        rule's price is bought at that look with no request before the order. Outside them the cadence of before."""
        t, api, clock, x = taker("on", **FAST)
        t.verify_tick = lambda: None  # the REST reads beside the pushes: their own test below
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.70, 30]]}
        reads = lambda: sum(1 for c in api.calls if c.endswith("/orderbook"))
        await t.tick()  # 05:30, in the hours: the market found; nothing in hand yet — read once
        w = t.track[slug]
        self.assertEqual((t.fast, reads(), api.sent), (True, 1, []))
        connected(t, "901", {"bids": [], "asks": [[0.70, 30]]}, age=300)  # pushed five minutes ago, the connection alive
        # ...and a look costs next to nothing: the order gate (it sums the day's P&L over every record) worked out once a tick, the
        # pre-open mode's ledger walked only when something is offered at the price
        gates, walks, plain_block, plain_own = [], [], t.block, t.own_asks
        t.block = lambda usd: gates.append(usd) or plain_block(usd)
        t.own_asks = lambda slug, side: walks.append(slug) or plain_own(slug, side)
        for mid, start in (("902", GOLD_0600 + 900_000), ("903", GOLD_0600 + 1_800_000)):  # two more windows, quiet too
            api.markets[mid] = gold_market(start, mid=int(mid))
            t.track[api.markets[mid]["categorySlug"]].update(market=api.markets[mid], open_ms=NOW, listed_ms=NOW)
            connected(t, mid, {"bids": [], "asks": [[0.70, 30]]}, age=300)
        looked = []
        for _ in range(5):
            clock.ms += 1_000
            await t.tick()
            looked.append(w["look_ms"])
        self.assertEqual(looked, [NOW + 1_000 * i for i in range(1, 6)], "every second")
        self.assertEqual((reads(), api.sent, t.fast_looks), (1, [], 1 + 5 * 3), "on the book in hand: no read a look")
        self.assertEqual((gates, walks), ([0.0] * 5, []), "three windows a tick: the gate once, the ledger not at all")
        t.block, t.own_asks = plain_block, plain_own
        for mid in ("902", "903"):
            t.track.pop(api.markets[mid]["categorySlug"])
        t.planned_ms = clock.ms  # (not planned again in this test: the two stay out)
        details = len(api.calls)
        t.stream.take("901", {"version": 3, "bids": [], "asks": [[0.52, 25]]})  # an offer at the rule's price, pushed
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[0]["data"]["order"]["takerAmount"])), 50.0)
        # (1.154.22: the order's request starts within its look, the tick letting go of the loop for it — a listing the warm-up asked
        # to be looked for again may be probed in the same tick, after it; nothing is asked of the order's own market)
        self.assertEqual((reads(), [c for c in api.calls[details:] if not c.startswith("/categories/")]), (1, []), "no request before the order")
        self.assertIn("每秒检查时段（北京时间）：01:00-07:00｜进行中：每秒看 1 场（已看 17 次）；推送正常；REST 核对 0 次", t.status())
        # the connection gone quiet (no frame for ALIVE_S): the book in hand stands only while young — then the look waits for a REST read
        t.stream.last_frame = time.monotonic() - R.ALIVE_S - 1
        bids, asks, at = t.stream.books["901"]
        t.stream.books["901"] = (bids, asks, at - 300)
        t.record(slug)["orders"][0]["state"] = "final"  # (the first order done with, nothing of it filled: the window still to buy)
        clock.ms += 1_000
        await t.tick()
        self.assertEqual(w["word"], "每秒检查：等盘口（推送还没送到，REST 轮流读取中）")
        self.assertEqual((reads(), len(api.sent)), (1, 1), "read a moment ago: not again at every look")
        w["rest_at"] -= R.FAST_STALE_S  # ...and none since: read here, once
        api.books["901"] = {"bids": [], "asks": [[0.70, 30]]}
        clock.ms += 1_000
        await t.tick()
        self.assertEqual((reads(), w["word"]), (2, "盘口没有 ≤58¢ 的涨（最低 70¢）"))
        # outside the hours: every SLOW_S, as before
        t2, api2, clock2, _ = taker("on", **FAST)
        clock2.ms = bj(2026, 11, 17, 8, 0)
        nxt = bj(2026, 11, 18, 6, 0)
        api2.markets["911"] = gold_market(nxt, mid=911)
        api2.listed.add(api2.markets["911"]["categorySlug"])
        api2.books["911"] = {"bids": [], "asks": [[0.70, 30]]}
        await t2.tick()
        w2 = t2.track[api2.markets["911"]["categorySlug"]]
        clock2.ms += 1_000
        await t2.tick()
        self.assertEqual((t2.fast, w2["look_ms"], t2.fast_looks), (False, bj(2026, 11, 17, 8, 0), 0))
        self.assertIn("每秒检查时段（北京时间）：01:00-07:00｜下一次 01:00 开始，提前 1 分钟准备", t2.status())

    async def test_per_second_hours_rest_reads_beside_the_pushes(self):
        """1.154.12: in the hours one window's book is read over REST at a time beside the pushes — the longest unread first, one a
        second at most, none again within three (every window read every second would be the key's whole budget); a change the
        push did not bring wakes the taker and is bought; a push that came after the read was asked for stays the newer; a window
        bought in full is read no more. On a key of its own (RULE_API_KEY) three reads a second."""
        t, api, clock, x = taker("on", **FAST, RULE_GOLD_SHARES="25")
        hold, t.verify_tick = t.verify_tick, lambda: None
        mids = ["901", "902", "903"]
        for i, mid in enumerate(mids):
            api.markets[mid] = gold_market(GOLD_0600 + i * 900_000, mid=int(mid))
            api.listed.add(api.markets[mid]["categorySlug"])
            api.books[mid] = {"bids": [], "asks": [[0.70, 30]]}
        await t.tick()
        t.verify_tick = hold
        slugs = {mid: api.markets[mid]["categorySlug"] for mid in mids}
        for i, mid in enumerate(mids):
            connected(t, mid, {"bids": [], "asks": [[0.70, 30]]}, age=300)
            t.track[slugs[mid]]["rest_at"] = time.monotonic() - 30 + 10 * i  # 901 read longest ago
        read = lambda: [c.split("/")[2] for c in api.calls if c.endswith("/orderbook")]
        n = len(read())
        t.verify_tick()
        await t.verifying
        self.assertEqual(read()[n:], ["901"], "the longest unread first")
        t.verify_tick()
        self.assertTrue(t.verifying.done() and len(read()) == n + 1, "one a second at most")
        for want in ("902", "903"):
            t.verify_at -= R.VERIFY_EVERY_S
            t.verify_tick()
            await t.verifying
            self.assertEqual(read()[-1], want)
        t.verify_at -= R.VERIFY_EVERY_S
        t.verify_tick()
        self.assertEqual((len(read()), t.verifies, t.verify_diffs), (n + 3, 3, 0), "each read within the last three seconds: none again yet")
        # a change the push did not bring: seen by the read, the taker woken, the offer bought
        api.books["902"] = {"bids": [], "asks": [[0.52, 25]]}
        t.track[slugs["902"]]["rest_at"] -= R.VERIFY_AGAIN_S
        t.verify_at -= R.VERIFY_EVERY_S
        t.verify_tick()
        await t.verifying
        self.assertEqual((t.verify_diffs, "902" in t.stream.changed, t.stream.pushed.is_set()), (1, True, True))
        api.fill[1] = 25.0
        await t.tick()
        self.assertEqual((len(api.sent), L.from_wei(int(api.sent[0]["data"]["order"]["takerAmount"]))), (1, 25.0))
        self.assertIn("REST 核对 4 次，1 次发现推送没送到的变化", t.status())
        # a push that came after the read was asked for is the newer: the read is dropped
        before = t.stream.held("903")
        self.assertFalse(t.stream.merge("903", (), (), time.monotonic() - 5 - 300))
        t.stream.take("903", {"version": 9, "bids": [], "asks": [[0.66, 5]]})
        self.assertFalse(t.stream.merge("903", (), (), time.monotonic() - 5))
        self.assertEqual((t.stream.held("903")[:2] == before[:2], t.stream.held("903")[1][0][0]), (False, D("0.66")))
        # bought in full (25 of 25): that window is read no more — the other two take the turns
        for slug in slugs.values():
            t.track[slug]["rest_at"] = time.monotonic() - 60
        seen = []
        for _ in range(4):
            t.verify_at -= R.VERIFY_EVERY_S
            for slug in slugs.values():
                t.track[slug]["rest_at"] -= R.VERIFY_AGAIN_S
            t.verify_tick()
            await t.verifying
            seen.append(read()[-1])
        self.assertEqual(sorted(set(seen)), ["901", "903"], seen)
        # not while no order could go out (the pause): nothing read
        x.state.paused = "测试暂停"
        n = len(read())
        t.verify_at -= R.VERIFY_EVERY_S
        t.verify_tick()
        self.assertEqual(len(read()), n)
        # a key of the taker's own: three reads a second, a window again after one
        own, _, _, _ = taker("on", **FAST, RULE_API_KEY="rule-key")
        self.assertTrue(own.own_key)
        self.assertEqual((R.VERIFY_EVERY_OWN_S, R.VERIFY_AGAIN_OWN_S), (0.34, 1.0))

    async def test_per_second_hours_are_prepared_a_minute_before(self):
        """1.154.12 (凌晨 1 点前 1 分钟开始准备): from WARM_MS before the hours — and every WARM_EVERY_MS through them — the sign-in and
        the exchange allowance are in hand and a market not yet seen open for matching is read once more, so the first offer's
        order goes out with no request before it; on paper nothing of the account is asked."""
        allowance, auth = [], []

        async def asked(key):
            allowance.append(key)
            return 10 ** 30
        t, api, clock, x = taker("on", **FAST)
        x.owner.live.chain.allowance = asked

        async def signed_in():
            auth.append(clock.ms)
        api.ensure_auth = signed_in
        t.verify_tick = lambda: None
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600, status="MATCHING_NOT_ENABLED")
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.70, 30]]}
        details, plain_market = [], api.market

        async def market(mid):
            details.append(mid)
            return await plain_market(mid)
        api.market = market
        asked_of = lambda: (sum(1 for c in api.calls if c.endswith("/orderbook")), len(details))  # this window's book and details read
        start = bj(2026, 11, 17, 1, 0)
        clock.ms = start - R.WARM_MS - 1_000  # 00:58:59
        await t.tick()
        w = t.track[slug]
        self.assertEqual((t.fast, t.warming, w["open_ms"], allowance, auth), (False, None, 0, [], []), "not yet")
        self.assertIn("｜下一次 01:00 开始，提前 1 分钟准备", t.fast_line(clock.ms))
        api.markets["901"]["tradingStatus"] = "OPEN"  # matching opened since the listing was read
        clock.ms = start - R.WARM_MS  # 00:59:00
        await t.tick()
        await t.warming
        self.assertEqual((t.fast, w["open_ms"], len(allowance), auth), (False, clock.ms, 1, [clock.ms]), "a minute before: ready")
        self.assertIn("，正在准备（登录、授权额度、撮合状态、推送订阅）", t.fast_line(clock.ms))
        clock.ms = start  # 01:00:00
        connected(t, "901", {"bids": [], "asks": [[0.52, 25]]})
        before = asked_of()
        await t.tick()
        await t.warming
        self.assertEqual((t.fast, len(api.sent), asked_of(), len(allowance)), (True, 1, before, 2),
                         "the first offer's order with no request before it (the allowance renewed beside it, not for it)")
        clock.ms += R.WARM_EVERY_MS
        await t.tick()
        await t.warming
        self.assertEqual((len(allowance), len(auth)), (3, 4), "renewed through the hours (the sign-in checked by the order itself too: no request while it stands)")
        # on paper: the windows' markets made ready, nothing of the account asked
        allowance.clear(); auth.clear()
        paper, papi, pclock, px = taker("paper", **FAST)
        px.owner.live.chain.allowance = asked
        papi.ensure_auth = signed_in
        papi.markets["901"] = gold_market(GOLD_0600, status="MATCHING_NOT_ENABLED")
        papi.listed.add(slug)
        pclock.ms = start - R.WARM_MS - 1_000
        await paper.tick()
        papi.markets["901"]["tradingStatus"] = "OPEN"
        pclock.ms = start - R.WARM_MS
        await paper.tick()
        await paper.warming
        self.assertEqual((paper.track[slug]["open_ms"], allowance, auth), (pclock.ms, [], []))

    async def test_ready_after_a_start_and_through_a_windows_last_quarter_hour(self):
        """1.154.25 — 11-18 18:06: an offer nine minutes before the 18:15 window, outside the per-second hours and no makers'
        moment; the order was signed 914 ms into its look — the sign-in and the allowance's read came first (the process half
        an hour old: nothing in hand). The preparation (warm) is made once after a start, as soon as a window's market is
        known, and kept up through a window's last NEAR_MS before its start, where an offer is bought as it shows too."""
        t, api, clock, x = taker("on", **IN)  # no per-second hours, no makers' moment, nothing bought in a window's course
        t.verify_tick = lambda: None
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": []}
        warms, real_warm, signed_in = [], t.warm, []

        async def warm():
            warms.append(clock.ms)
            await real_warm()

        async def ensure_auth():
            signed_in.append(clock.ms)
        t.warm, api.ensure_auth = warm, ensure_auth

        async def at(ms):
            clock.ms = ms
            await t.tick()
            if t.warming is not None:
                await asyncio.wait([t.warming], timeout=2)
        await at(GOLD_0600 - 40 * 60_000)  # 05:20: the first tick finds the market; the next one, a market known, prepares
        self.assertEqual((warms, t.warmed, t.fast), ([], False, False))
        await at(GOLD_0600 - 40 * 60_000 + 1_000)
        self.assertEqual((warms, t.warmed, signed_in, list(t.allowance)), ([clock.ms], True, [clock.ms], ["CTF_EXCHANGE"]), "once after a start: the sign-in and the allowance in hand")
        await at(GOLD_0600 - 30 * 60_000)
        await at(GOLD_0600 - R.NEAR_MS - 1_000)
        self.assertEqual(len(warms), 1, "not again: half an hour before a window is no time an order is likelier in")
        await at(GOLD_0600 - R.NEAR_MS)  # the window's last quarter hour: kept up, every WARM_EVERY_MS
        self.assertEqual((len(warms), warms[-1]), (2, GOLD_0600 - R.NEAR_MS))
        await at(GOLD_0600 - R.NEAR_MS + R.WARM_EVERY_MS - 1_000)
        self.assertEqual(len(warms), 2)
        await at(GOLD_0600 - R.NEAR_MS + R.WARM_EVERY_MS)
        self.assertEqual(len(warms), 3)
        # an offer there: its order with no request before it
        connected(t, "901", {"bids": [], "asks": [[0.55, 30]]})
        t.stream.changed.add("901")
        asked, signed = len(api.calls), len(signed_in)
        chain = []

        async def counted(key):
            chain.append(key)
            return 10 ** 30
        x.owner.live.chain.allowance = counted
        before, create = [], api.create_order

        async def created(body):
            before.append((list(api.calls[asked:]), list(chain)))  # what was asked between the offer and its order
            return await create(body)
        api.create_order = created
        clock.ms += 1_000
        await t.tick()
        self.assertEqual((len(api.sent), before, len(signed_in) - signed), (1, [([], [])], 1), "(the client asked whether its sign-in is fresh: it is)")
        # a rule with no price before its window is not prepared for one; nor is 只记录, which sends nothing
        cold, api2, clock2, _ = taker("on", RULE_GOLD_SHUT_CENTS="0", RULE_GOLD_SHUT_LIVE_CENTS="60", **IN)
        cold.verify_tick = lambda: None
        api2.markets["901"] = gold_market(GOLD_0600)
        api2.listed.add(slug)
        cold.warmed = True  # (the one after a start made)
        made = []
        cold.warm = lambda: made.append(clock2.ms) or real_warm()
        clock2.ms = GOLD_0600 - 10 * 60_000
        await cold.tick()
        clock2.ms += 1_000
        await cold.tick()
        self.assertEqual(made, [], "bought in its course alone: prepared from a minute before the start, as before")
        clock2.ms = GOLD_0600 - R.WARM_MS
        await cold.tick()
        self.assertEqual(len(made), 1)
        if cold.warming is not None:
            await asyncio.wait([cold.warming], timeout=2)
        paper, api3, clock3, x3 = taker("paper", **IN)
        paper.verify_tick = lambda: None
        api3.markets["901"] = gold_market(GOLD_0600)
        api3.listed.add(slug)
        api3.books["901"] = {"bids": [], "asks": []}
        signs = []

        async def never():
            signs.append(1)
        api3.ensure_auth = never
        for ms in (GOLD_0600 - 10 * 60_000, GOLD_0600 - 10 * 60_000 + 1_000):
            clock3.ms = ms
            await paper.tick()
            if paper.warming is not None:
                await asyncio.wait([paper.warming], timeout=2)
        self.assertEqual((paper.warmed, signs, paper.allowance), (True, [], {}))

    async def test_orders_for_several_windows_go_out_side_by_side(self):
        """1.154.12: offers showing on several windows at once — each window's order is on its way before another's answer is in
        (they went one after another, a round trip each); the day's cap still counts the first while it is on its way."""
        t, api, clock, x = taker("on", **FAST, RULE_MAX_DAY_USD="40")
        t.verify_tick = lambda: None
        for i, mid in enumerate(("901", "902")):
            api.markets[mid] = gold_market(GOLD_0600 + i * 900_000, mid=int(mid))
            api.listed.add(api.markets[mid]["categorySlug"])
            api.books[mid] = {"bids": [], "asks": [[0.51, 20]]}
        flying, most, plain = [], [], api.create_order

        async def slow(body):
            flying.append(1)
            most.append(len(flying))
            await asyncio.sleep(0.02)
            try:
                return await plain(body)
            finally:
                flying.pop()
        api.create_order = slow
        await t.tick()
        self.assertEqual((len(api.sent), max(most)), (2, 2), "both on their way together")
        sizes = sorted(L.from_wei(int(b["data"]["order"]["takerAmount"])) for b in api.sent)
        self.assertEqual(sizes, [18.0, 50.0], "the cap counts the first order while it flies: $40 − 50 × 58¢ leaves 18 shares for the second")
        self.assertTrue(all(o["state"] == "open" and o["answered_wall"] for r in t.recs.values() for o in r["orders"]), "the tick waited for both answers")

    async def test_the_makers_moment_is_sprinted(self):
        """1.154.13 — someone posts five hours before each window and the fastest taker has it. RULE_LEAD_MINUTES (300): each window
        sprints from a minute before that moment to a quarter hour after — looked at each second, prepared — whatever the
        per-second hours say, and right at the moment the REST reads beside the pushes are that window's alone, one a second."""
        t, api, clock, x = taker("on", RULE_FAST_HOURS="off", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0")
        self.assertEqual((t.settings.lead_minutes, m.CONTROL_FORMS["RULE_LEAD_MINUTES"]["group"]), (300, "规则"))
        starts = [GOLD_0600 + i * 900_000 for i in range(2)]  # 06:00 and 06:15: their moments 01:00 and 01:15
        for i, start in enumerate(starts):
            mid = str(901 + i)
            api.markets[mid] = gold_market(start, mid=901 + i)
            api.listed.add(api.markets[mid]["categorySlug"])
            api.books[mid] = {"bids": [], "asks": []}
        slugs = [api.markets[mid]["categorySlug"] for mid in ("901", "902")]
        hold, t.verify_tick = t.verify_tick, lambda: None
        moment = GOLD_0600 - 300 * 60_000  # 01:00:00
        clock.ms = moment - R.WARM_MS - 1_000
        await t.tick()
        self.assertEqual((t.fast, t.warming), (False, None), "00:58:59: nothing yet")
        self.assertIn("每秒检查时段：关（RULE_FAST_HOURS=off）：推送一有变化就看", t.fast_line(clock.ms))
        self.assertIn("｜对手提前 300 分钟挂单的时刻前后也每秒检查（RULE_LEAD_MINUTES），下一个 01:00（06:00 的场）", t.fast_line(clock.ms))
        clock.ms = moment - R.WARM_MS
        await t.tick()
        await t.warming
        self.assertTrue(t.fast and t.sprinting(t.track[slugs[0]], clock.ms) and not t.sprinting(t.track[slugs[1]], clock.ms), "00:59: the 06:00 window sprints")
        # right at the moment the REST reads are that window's alone, one a second
        t.verify_tick = hold
        for mid in ("901", "902"):
            connected(t, mid, {"bids": [], "asks": []}, age=300)
        clock.ms = moment - 5_000
        for slug in slugs:
            t.track[slug]["rest_at"] = time.monotonic() - 60
        read = lambda: [c.split("/")[2] for c in api.calls if c.endswith("/orderbook")]
        n = len(read())
        for _ in range(3):
            t.verify_at -= R.VERIFY_EVERY_S
            t.track[slugs[0]]["rest_at"] -= R.VERIFY_HOT_S
            t.verify_tick()
            await t.verifying
        self.assertEqual(read()[n:], ["901"] * 3, "the hot window alone, again after a second (not three)")
        t.verify_tick = lambda: None
        # an offer pushed in the sprint is bought at that look; a quarter hour after the moment the window is as any other again
        clock.ms = moment + 1_500
        t.stream.take("901", {"version": 2, "bids": [], "asks": [[0.53, 30]]})
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[0]["data"]["order"]["takerAmount"])), 50.0)
        clock.ms = moment + R.SPRINT_MS  # 01:15: the 06:00 window's sprint over — and the 06:15 window's own moment
        await t.tick()
        self.assertTrue(t.fast and not t.sprinting(t.track[slugs[0]], clock.ms) and t.sprinting(t.track[slugs[1]], clock.ms))
        clock.ms = moment + 3 * 900_000 + R.SPRINT_MS  # 02:00: the last shut window's (06:45, its moment 01:45) sprint over
        await t.tick()
        self.assertFalse(t.fast)
        off, _, _, _ = taker("on", RULE_FAST_HOURS="off", RULE_LEAD_MINUTES="0")
        self.assertEqual((off.settings.lead_minutes, off.moment({"start": GOLD_0600})), (0, 0), "0: no such moment")
        with self.assertRaises(ValueError):
            m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "RULE_TAKER": "on", "RULE_LEAD_MINUTES": "2000"})

    async def test_the_race_is_timed(self):
        """1.154.13: Predict's stamps on the books time the race — when the first offer at the price showed and when it was gone
        (bought by a faster taker: how long it stood), and an order's own way: the stamp of the book it is sized on, that book's
        arrival, the request leaving, its answer. /rules and the review page sum them up."""
        t, api, clock, x = taker("on", **FAST)
        t.verify_tick = lambda: None
        mids = {"901": GOLD_0600, "902": GOLD_0600 + 900_000}
        for mid, start in mids.items():
            api.markets[mid] = gold_market(start, mid=int(mid))
            api.listed.add(api.markets[mid]["categorySlug"])
            api.books[mid] = {"bids": [], "asks": []}
        slug1, slug2 = (api.markets[mid]["categorySlug"] for mid in ("901", "902"))
        await t.tick()
        for mid in mids:
            connected(t, mid, {"bids": [], "asks": []})
        wall = int(time.time() * 1000)
        # 901: an offer at 52¢ shows (Predict's stamp: 300 ms ago) — bought by this taker
        t.stream.take("901", {"version": 2, "updateTimestampMs": wall - 300, "bids": [], "asks": [[0.52, 25]]})
        self.assertEqual(t.stream.stamps["901"][0], wall - 300)
        self.assertTrue(250 <= t.stream.lags[-1] <= 2_000, t.stream.lags)
        api.fill[1] = 25.0
        clock.ms += 1_000
        await t.tick()
        order = t.record(slug1)["orders"][0]
        self.assertEqual((order["book_ts"], order["seen_wall"]), t.stream.stamps["901"])
        self.assertTrue(order["seen_wall"] <= order["sent_wall"] <= order["answered_wall"], order)
        t.stream.take("901", {"version": 3, "updateTimestampMs": wall + 300, "bids": [], "asks": []})  # the book after this taker's fill: 600 ms after the offer showed
        # 902: an offer shows; this taker's order goes out, but a faster taker has it 180 ms after it showed (Predict's stamps)
        t.stream.take("902", {"version": 2, "updateTimestampMs": wall - 250, "bids": [], "asks": [[0.53, 30]]})
        clock.ms += 1_000
        await t.tick()
        self.assertEqual((len(api.sent), R.in_flight(t.record(slug2))), (2, True), "sent, nothing filled")
        t.stream.take("902", {"version": 3, "updateTimestampMs": wall - 70, "bids": [], "asks": []})
        clock.ms += 1_000
        await t.tick()  # this taker's order still on its way: the look buys nothing — the offer's going is seen all the same
        seen = t.track[slug2]["seen"]
        self.assertEqual((seen["offer_ts"], seen["gone_ts"], seen["gone_ms"] - seen["offer_ms"]), (wall - 250, wall - 70, 1_000))
        t.stream.take("902", {"version": 4, "updateTimestampMs": wall - 10, "bids": [[0.40, 5]], "asks": []})  # a later change: not the offer's going
        clock.ms += 1_000
        await t.tick()
        self.assertEqual(seen["gone_ts"], wall - 70, "kept as first seen")
        # the windows end: the watches are kept; the statistics say what the race was
        clock.ms = GOLD_0600 + 900_000 - R.STOP_MS
        await t.tick()
        st = R.watch_stats(t.recs, "gold_shut", "on", 0)
        self.assertEqual((st["offered"], st["lost"], st["life"]), (2, 1, 180), "one bought, one lost to a taker 180 ms after it showed")
        self.assertEqual((st["won"], st["won_life"]), (1, 600), "the one bought: gone 600 ms after it showed, by Predict's stamps")
        self.assertAlmostEqual(st["offer_lead"], (30 * 60_000 - 1_000 + 45 * 60_000 - 2_000) / 2 / 60_000, 3)
        sp = R.speed_stats(t.recs, 0)
        self.assertEqual((sp["orders"], sp["filled"]), (2, 1))
        self.assertTrue(250 <= sp["lag"] <= 2_000 and 0 <= sp["own"] <= 2_000 and 0 <= sp["trip"] <= 2_000, sp)
        text = t.status()
        self.assertIn("下单速度（近 30 天真实订单 2 张，1 张有成交）：盘口变化→推送到达 中位 ", text)
        self.assertIn("，1 场的卖单没买到就没了（存活中位 0.2 秒），买到的 1 场从卖单出现到被吃掉中位 0.6 秒", text)
        self.assertIn("，卖单首次出现在开场前中位 37 分钟", text)
        page = R.review(t.recs, t.settings, clock.ms)
        self.assertEqual((page["speed"]["orders"], page["lead_minutes"]), (2, 0))
        row = next(r for r in page["rules"] if r["rule"] == "gold_shut" and r["mode"] == "on")
        self.assertEqual((row["watch"]["lost"], row["watch"]["life"], row["watch"]["won"], row["watch"]["won_life"]), (1, 180, 1, 600))

    async def test_an_order_costs_no_walk_of_the_records(self):
        """1.154.13: between an offer showing and the order leaving — the pre-open mode's own bids found by the window's own keys in
        its ledger (the window's slug, "#<level>": 39,000 records were walked, 17 ms and more), a record renamed "@<attempt>"
        (withdrawn, final) never one; and the day's P&L for the loss stop summed once a tick, not at each of the order's three
        checks."""
        t, api, clock, x = taker("on")
        start = bj(2026, 11, 17, 18, 15)
        clock.ms = start - 20 * 60_000
        slug = f"xau-updown-15m-{start // 1000}"
        api.markets["903"] = gold_market(start, mid=903)
        api.listed.add(slug)
        api.books["903"] = {"bids": [[0.50, 7], [0.48, 15], [0.47, 9]], "asks": [[0.53, 40]]}  # Down offers at 50¢, 52¢ (its own, level 1), 53¢

        class Rows(dict):
            walked = 0

            def values(self):
                Rows.walked += 1
                return super().values()
        pair = lambda key, price, state: {"slug": key, "price": price, "shares": "15", "phase": "active",
                                          "orders": {"up": {"state": state, "filled": 0.0}, "down": {"state": state, "filled": 0.0}}}
        rows = Rows({f"{slug}#1": pair(f"{slug}#1", "0.48", "open"),               # its deeper level: an Up bid at 48¢ = a Down offer at 52¢
                     f"{slug}@1": pair(f"{slug}@1", "0.50", "final"),              # a withdrawn quote renamed aside: nothing rests of it
                     **{f"btc-updown-15m-{i}": pair(f"btc-updown-15m-{i}", "0.47", "open") for i in range(50)}})
        x.owner.preopen = types.SimpleNamespace(ledger=types.SimpleNamespace(rows=rows))
        self.assertEqual(t.own_asks(slug, "down"), {D("0.52"): D("15")})
        sums = []
        x.owner.live_daily_pnl = lambda trades, now: sums.append(now) or 0.0
        await t.tick()
        body = api.sent[0]["data"]["order"]
        self.assertEqual(L.from_wei(int(body["takerAmount"])), 7.0, "the 7 at 50¢ — a cent under its own 52¢ offer leaves 51¢: the 50¢ ones")
        self.assertEqual((Rows.walked, len(sums)), (0, 1), "no walk of the ledger; the loss stop's sums once for the tick's three checks")

    async def test_a_key_of_its_own(self):
        """1.154.12 — RULE_API_KEY: the rule taker's reads, orders and push connection on a key of its own, with its own cooldown:
        a 429 on the bot's key (11-17: ninety in 2.3 hours, twenty seconds each) no longer stops it. Without one: the bot's key
        and one cooldown with the bot's own client, as before."""
        _, _, _, x = taker("on", RULE_API_KEY=" rule-key ", PREDICT_API_KEY="main-key")
        own = R.RuleTaker(x.owner)  # (the helper's taker has the tests' stand-in client: this one its real client)
        self.assertEqual((own.key, own.own_key, own.api.api_key, own.stream.headers()), ("rule-key", True, "rule-key", {"x-api-key": "rule-key"}))
        x.owner.live.api.blocked_until = time.monotonic() + 30  # the bot's own client in its cooldown
        self.assertLessEqual(own.cooling(), 0, "its own key: not paused by the bot's")
        own.api.blocked_until = time.monotonic() + 30
        self.assertGreater(own.cooling(), 0)
        self.assertIn("｜请求用规则吃单自己的 Key（RULE_API_KEY）", own.fast_line(NOW))
        _, _, _, y = taker("on", PREDICT_API_KEY="main-key")
        plain = R.RuleTaker(y.owner)
        self.assertEqual((plain.key, plain.own_key, plain.api.api_key, plain.stream.headers()), ("main-key", False, "main-key", {"x-api-key": "main-key"}))
        y.owner.live.api.blocked_until = time.monotonic() + 30
        self.assertGreater(plain.cooling(), 0, "one cooldown with the bot's own client")
        self.assertNotIn("RULE_API_KEY", plain.fast_line(NOW))
        _, _, _, z = taker("on", RULE_API_KEY="main-key", PREDICT_API_KEY="main-key")  # set to the bot's own key: not a key of its own
        self.assertFalse(R.RuleTaker(z.owner).own_key)

    # --- 1.154.14: 盘中吃单 ---
    def test_in_window_settings(self):
        """RULE_GOLD_SHUT_LIVE_CENTS / RULE_GOLD_OPEN_LIVE_CENTS: off by default, 0 or 1–97; on the control page; the rules that can
        judge a window's course and the settings are the same two."""
        base = {"TELEGRAM_BOT_TOKEN": "offline"}
        c = m.Config.from_env(base).rules
        self.assertEqual([c.live_price(k) for k in ("gold_shut", "gold_open", "oil_shut", "gold_0515")], [0, 0, 0, 0], "off until set")
        c = m.Config.from_env({**base, "RULE_TAKER": "on", "RULE_GOLD_SHUT_LIVE_CENTS": "97", "RULE_GOLD_OPEN_LIVE_CENTS": "70"}).rules
        self.assertEqual((c.live_price("gold_shut"), c.live_price("gold_open"), c.price("gold_shut")), (97, 70, 58), "beside the price before the window")
        for bad in ("98", "-1", "x"):
            with self.assertRaises(ValueError, msg=bad):
                m.Config.from_env({**base, "RULE_TAKER": "paper", "RULE_GOLD_OPEN_LIVE_CENTS": bad})
        self.assertEqual(m.Config.from_env({**base, "RULE_GOLD_SHUT_LIVE_CENTS": "98"}).rules.live_price("gold_shut"), 0, "off: read leniently")
        self.assertEqual({r.key for r in R.RULES if r.lead > 0}, set(m.RULE_LIVE_KEYS), "a way to judge the course, and a price for it")
        self.assertEqual({k: R.RULE_OF[k].lead for k in m.RULE_LIVE_KEYS}, {"gold_shut": D("0.00005"), "gold_open": D("0.0002")})
        self.assertEqual([R.lead_note(R.RULE_OF[k]) for k in ("gold_shut", "gold_open")], ["指数比起点高 0.005% 以上才买", "指数比起点低 0.02% 以上才买"])
        for key in m.RULE_LIVE_KEYS.values():
            form = m.CONTROL_FORMS[key]
            self.assertTrue(key in m.CONTROL_KEY_SET and form["group"] == "规则" and form["presets"][0] == "0" and form["zero"] == "盘中不做", key)
        bot = types.SimpleNamespace(config=m.Config.from_env({**base, "RULE_TAKER": "paper", "RULE_GOLD_SHUT_LIVE_CENTS": "80"}))
        self.assertEqual([m.Bot.control_value(bot, k) for k in ("RULE_GOLD_SHUT_LIVE_CENTS", "RULE_GOLD_OPEN_LIVE_CENTS")], ["80", "0"])
        off = m.Config.from_env({**base, "RULE_TAKER": "on", "RULE_GOLD": "off", "RULE_GOLD_SHUT_LIVE_CENTS": "80"}).rules
        self.assertEqual(R.live_price_of(off, R.RULE_OF["gold_shut"]), 0, "its market switched off: not in the course either")
        self.assertEqual(R.live_price_of(c, R.RULE_OF["oil_shut"]), 0, "oil: before the window alone")

    async def test_in_its_course_up_is_bought_while_up_leads(self):
        """盘中吃单, a shut quarter hour (gold_shut): past the window's start its side is bought at its own price — but only while
        the bot's card of the market is on this window, has its start (the candle; Predict's own start price too when it
        agrees) and a fresh index, and the index is over the start by 0.005%. Nothing in the seconds around the start; in the
        window's first minute no dearer than before it; the order is signed to end 5 s before the window's end; the window's
        size is one, before it and in its course together."""
        t, api, clock, x = taker("on", RULE_GOLD_SHUT_LIVE_CENTS="70", RULE_SHARES="40", **IN)
        t.verify_tick = lambda: None
        slug, end = f"xau-updown-15m-{GOLD_0600 // 1000}", GOLD_0600 + 900_000
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.65, 30], [0.72, 50]]}
        reads = lambda: sum(1 for c in api.calls if c.endswith("/orderbook"))
        clock.ms = GOLD_0600 - 60_000
        await t.tick()
        w = t.track[slug]
        self.assertEqual((w["word"], t.fast), ("盘口没有 ≤58¢ 的涨（最低 65¢）", False), "before the window: the rule's own price")
        for at in (GOLD_0600 - 10_000, GOLD_0600 + 2_000):  # the seconds around the start
            clock.ms = at
            await t.tick()
            self.assertEqual((w["word"], api.sent, slug in t.track), ("开场前后这几秒不下单（等这一场的起点价）", [], True))
        seen = t.record(slug)["seen"]
        self.assertEqual((seen["best"], seen["offer_max"], seen["bought"]), (0.65, 0.0, 0.0), "the watch of the time before the start kept as it ends")
        clock.ms = GOLD_0600 + 4_000
        before = reads()
        await t.tick()
        self.assertEqual((w["word"], t.fast), ("盘中：机器人没有 XAU-15M 这张卡，判断不了涨跌谁领先", True), "in its course: looked at each second")
        for card, word in (
                (dict(start=GOLD_0600 - 900_000, line=4000, price=4000), "盘中：卡片还没换到这一场，等起点价"),
                (dict(start=GOLD_0600, line=None, price=4000), "盘中：还没读到这一场的起点价"),
                (dict(start=GOLD_0600, line=4000, price=None), "盘中：还没有实时指数"),
                (dict(start=GOLD_0600, line=4000, price="3999.99"), "盘中：涨还没领先（指数 3,999.99，起点 4,000，要高出 0.005% 以上），不买"),
                (dict(start=GOLD_0600, line=4000, price=4000), "盘中：涨还没领先（指数 4,000，起点 4,000，要高出 0.005% 以上），不买"),
                (dict(start=GOLD_0600, line=4000, price="4000.19"), "盘中：涨还没领先（指数 4,000.19，起点 4,000，要高出 0.005% 以上），不买"),
                # over the candle by 0.005%, but not over Predict's own start price (agreeing with the candle within 0.1%) by as much
                (dict(start=GOLD_0600, line=4000, price="4000.6", strike="4000.5"), "盘中：涨还没领先（指数 4,000.6，起点 4,000.5，要高出 0.005% 以上），不买")):
            clock.ms += 1_000
            gold_card(x, clock, **card)
            await t.tick()
            self.assertEqual((w["word"], api.sent), (word, []), str(card))
        clock.ms += 1_000
        gold_card(x, clock, GOLD_0600, 4000, 4001, age_ms=60_000)
        await t.tick()
        self.assertTrue(w["word"].startswith("盘中：指数价 60 秒未更新") and not api.sent, w["word"])
        self.assertEqual(reads(), before, "no book read while Up does not lead, or the index is not known")
        # Up leads by 0.005% (Predict's start price 0.25% off the candle is another window's: the candle alone) — but in the window's
        # first minute no dearer than before it: the 65¢ on offer is over the rule's 58¢
        clock.ms += 1_000
        gold_card(x, clock, GOLD_0600, 4000, "4000.2", strike="4010")
        await t.tick()
        self.assertEqual((w["word"], api.sent, clock.ms - GOLD_0600 < R.LIVE_FULL_MS), ("盘中：盘口没有 ≤58¢ 的涨（最低 65¢）", [], True))
        # a minute in: at the course's own price
        clock.ms = GOLD_0600 + R.LIVE_FULL_MS
        gold_card(x, clock, GOLD_0600, 4000, "4000.2", strike="4010")
        api.fill[1], api.price = 30.0, 0.65
        await t.tick()
        self.assertEqual(len(api.sent), 1)
        signed = api.sent[0]["data"]["order"]
        self.assertEqual((L.from_wei(int(signed["takerAmount"])), int(signed["expiration"])), (40.0, end // 1000 - R.EXPIRY_BEFORE_S),
                         "30 on offer at the course's price: all the window may buy asked for; signed to end before the window does")
        order = t.record(slug)["orders"][-1]
        self.assertEqual((order["live"], order["limit"], w["word"]), (True, 0.7, "盘中 已下单：涨 ≤70¢ × 40 份"))
        clock.ms += R.FILL_READ_MS
        await t.tick()
        self.assertEqual((R.filled(t.record(slug)), order["state"]), (30.0, "cancelling"), "30 of the 40 filled, the rest withdrawn")
        self.assertIn("🎯 规则吃单·盘中成交", x.notes[-1])
        self.assertIn("黄金休市 4 档盘中 ≤70¢", x.notes[-1])
        clock.ms += 1_000
        gold_card(x, clock, GOLD_0600, 4000, "4000.75")
        await t.tick()
        self.assertEqual((w["word"], len(api.sent)), ("这一场的 40 份已全部下单，等结果（撤回的部分 60 秒后确认没有晚到的成交才算空出来）", 1))
        clock.ms = order["left_ms"] + R.LATE_FILL_MS  # its final word: 30 — ten of the window's 40 free again
        gold_card(x, clock, GOLD_0600, 4000, "4000.75")
        await t.tick()
        clock.ms += 1_000
        gold_card(x, clock, GOLD_0600, 4000, "4000.75")
        api.fill[2] = 10.0
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[1]["data"]["order"]["takerAmount"])), 10.0, "RULE_SHARES is the window's, before it and in its course")
        clock.ms += R.FILL_READ_MS
        await t.tick()
        clock.ms += 1_000
        await t.tick()
        self.assertEqual((w["word"], len(api.sent)), ("这一场已买满 40 份", 2))
        self.assertEqual(t.record(slug)["seen"]["bought"], 0.0, "the watch stays that of the time before the start")
        self.assertEqual(R.course_part(t.record(slug))[:2], (40.0, 40 * 0.65))
        status = t.status()
        self.assertIn("设了盘中价格的规则开场后接着买（开场 3 秒后到结束前 15 秒，第 1 分钟最高按盘前的价格）", status)
        self.assertIn("· 黄金休市 4 档 买涨 ≤58¢｜盘中 ≤70¢（指数比起点高 0.005% 以上才买）", status)
        self.assertIn("｜这一场 11-17 06:00：这一场已买满 40 份", status)
        self.assertIn("（其中盘中 40 份）", status)
        # the window's last seconds: followed no more
        clock.ms = end - R.LIVE_STOP_MS
        await t.tick()
        self.assertNotIn(slug, t.track)
        self.assertEqual(len([r for r in t.recs.values() if r["slug"] == slug]), 1, "one record, its watch kept once")

    async def test_in_its_course_nothing_is_sent_in_the_last_seconds(self):
        """The course ends LIVE_STOP_MS before the window does: an offer a second before that is still bought (its order ends 5 s
        before the window's end), one after it is not; and a taker started in a window's course follows it at once."""
        t, api, clock, x = taker("on", RULE_GOLD_SHUT_LIVE_CENTS="90", **IN)
        t.verify_tick = lambda: None
        slug, end = f"xau-updown-15m-{GOLD_0600 // 1000}", GOLD_0600 + 900_000
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.88, 20]]}
        clock.ms = end - R.LIVE_STOP_MS - 1_000  # started 14′44″ into the window
        gold_card(x, clock, GOLD_0600, 4000, "4000.3")
        await t.tick()
        self.assertEqual(len(api.sent), 1, "followed from the first tick, bought")
        self.assertEqual(int(api.sent[0]["data"]["order"]["expiration"]) * 1000 - clock.ms, 11_000)
        t2, api2, clock2, x2 = taker("on", RULE_GOLD_SHUT_LIVE_CENTS="90", **IN)
        api2.markets["901"] = gold_market(GOLD_0600)
        api2.listed.add(slug)
        api2.books["901"] = {"bids": [], "asks": [[0.88, 20]]}
        clock2.ms = end - R.LIVE_STOP_MS
        gold_card(x2, clock2, GOLD_0600, 4000, "4000.3")
        await t2.tick()
        self.assertEqual((api2.sent, slug in t2.track), ([], False), "the last 15 s: not a window to buy in")
        # without a price for the course a window is the rule's until 15 s before its start, as before
        t3, api3, clock3, x3 = taker("on", **IN)
        clock3.ms = GOLD_0600 + 60_000
        await t3.tick()
        self.assertEqual((slug in t3.track, t3.fast), (False, False))

    async def test_in_its_course_the_reopen_buys_down_only_when_down_leads(self):
        """盘中吃单, the reopen's first quarter hour (gold_open): Down is bought in the window's course only while the index
        is under the start by 0.02% — by the lower of the candle and Predict's own start price. On paper: recorded once before
        the window and once in its course; the course's part of the record reckoned apart (review, /rules)."""
        t, api, clock, x = taker("paper", RULE_GOLD_OPEN_CENTS="50", RULE_GOLD_OPEN_LIVE_CENTS="80", RULE_OIL="off", RULE_GOLD_SHUT_CENTS="0",
                                 RULE_GOLD_0515_CENTS="0", RULE_LEAD_MINUTES="0")
        t.verify_tick = lambda: None
        slug = f"xau-updown-15m-{GOLD_0700 // 1000}"
        mkt = api.markets["907"] = gold_market(GOLD_0700, mid=907)
        api.listed.add(slug)
        api.books["907"] = {"bids": [[0.55, 10], [0.25, 40]], "asks": [[0.78, 40]]}  # Down for sale: 10 at 45¢, 40 at 75¢
        clock.ms = GOLD_0700 - 60_000
        await t.tick()
        w = t.track[slug]
        self.assertEqual((w["word"], R.filled(t.record(slug))), ("只记录：跌 45.0¢ × 10 份", 10.0), "before the window: the rule's 50¢")
        clock.ms += 2_000
        await t.tick()
        self.assertTrue(w["word"].startswith("只记录：这一场已记过"), w["word"])
        clock.ms = GOLD_0700 + 30_000  # the course's first minute: 只记录 waits for the course's own price
        gold_card(x, clock, GOLD_0700, 4000, 3990)
        await t.tick()
        self.assertEqual((w["word"], len(t.record(slug)["orders"])), ("盘中：只记录模式从开场 1 分钟后按盘中的价格记一次（第 1 分钟的价格和盘前一样）", 1))
        clock.ms = GOLD_0700 + 60_000
        for card, word in (
                (dict(line=4000, price=4000), "盘中：跌还没领先（指数 4,000，起点 4,000，要低出 0.02% 以上），不买"),
                (dict(line=4000, price=4003), "盘中：跌还没领先（指数 4,003，起点 4,000，要低出 0.02% 以上），不买"),
                (dict(line=4000, price="3999.5"), "盘中：跌还没领先（指数 3,999.5，起点 4,000，要低出 0.02% 以上），不买"),
                # under the candle by 0.02%, but not under Predict's own start price by as much
                (dict(line=4000, price="3999.2", strike="3999.6"), "盘中：跌还没领先（指数 3,999.2，起点 3,999.6，要低出 0.02% 以上），不买")):
            clock.ms += 1_000
            gold_card(x, clock, GOLD_0700, **card)
            await t.tick()
            self.assertEqual((w["word"], len(t.record(slug)["orders"])), (word, 1), str(card))
        clock.ms += 1_000
        gold_card(x, clock, GOLD_0700, 4000, "3999.2")  # 0.02% under the start
        await t.tick()
        rec = t.record(slug)
        self.assertEqual([(o.get("live", False), o["filled"], round(o["avg"], 4), o["limit"]) for o in rec["orders"]],
                         [(False, 10.0, 0.45, 0.5), (True, 40.0, 0.75, 0.8)],
                         "the 40 left of the window's 50, at the course's price — the 10 at 45¢ the paper order before the window took not again")
        self.assertEqual(w["word"], "盘中 只记录：跌 75.0¢ × 40 份")
        self.assertIn("🧪 规则吃单·盘中（只记录，没下单）", x.notes[-1])
        self.assertIn("黄金开盘第一档盘中 ≤80¢", x.notes[-1])
        self.assertEqual(api.sent, [])
        # the result: Down won
        clock.ms = rec["end_ms"] + R.SETTLE_AFTER_MS
        mkt["outcomes"][1]["status"] = "WON"
        await t.tick()
        rec = t.record(slug)
        part = round(40 * (1 - R.fee_share(0.75, 200)) - 40 * 0.75, 4)
        self.assertEqual((rec["result"], R.course_part(rec)[:2], round(R.course_part(rec)[2], 4)), ("down", (40.0, 30.0), part))
        self.assertEqual(round(rec["pnl"], 4), round(part + 10 * (1 - R.fee_share(0.45, 200)) - 4.5, 4), "the two parts are the record's whole")
        stats = R.course_stats(t.recs, "gold_open", "paper")
        self.assertEqual(stats, {"windows": 1, "shares": 40.0, "cost": 30.0, "settled": 1, "wins": 1, "losses": 0, "pnl": part})
        review = R.review(t.recs, t.settings, clock.ms)
        row = next(r for r in review["rules"] if r["rule"] == "gold_open" and r["mode"] == "paper")
        self.assertEqual((row["cents"], row["live_cents"], row["course"], row["shares"]), (50, 80, stats, 50.0))
        self.assertEqual([(r["shares"], r["live"]) for r in review["recent"]], [(50.0, 40.0)])
        status = t.status()
        self.assertIn("· 黄金开盘第一档 买跌 ≤50¢｜盘中 ≤80¢（指数比起点低 0.02% 以上才买）", status)
        self.assertIn(f"｜盘中近 30 天买到 1 场 40 份（均价 75.0¢），已结算 1 场：赢 1 输 0，{R.money(part)}", status)

    async def test_in_its_course_alone_with_no_price_before_the_window(self):
        """The price before the window 0 and one for its course: nothing is bought before the start — the market is found and its
        book subscribed all the same, no watch of the time before the start is kept — and the course is bought as any."""
        t, api, clock, x = taker("on", RULE_GOLD_SHUT_CENTS="0", RULE_GOLD_SHUT_LIVE_CENTS="60", **IN)
        t.verify_tick = lambda: None
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.55, 20]]}
        clock.ms = GOLD_0600 - 5 * 60_000
        await t.tick()
        w = t.track[slug]
        self.assertEqual((w["word"], api.sent, t.stream.wanted), ("盘前不买这条（盘前价格是 0），开场 1 分钟后按盘中的价格买", [], {"901"}))
        clock.ms = GOLD_0600 + 30_000
        gold_card(x, clock, GOLD_0600, 4000, 4001)
        await t.tick()
        self.assertEqual((w["word"], api.sent), ("盘中：开场后第 1 分钟最高按盘前的价格买（盘前价格是 0，不买），1 分钟后按盘中的价格", []))
        self.assertEqual(sum(1 for c in api.calls if c.endswith("/orderbook")), 0, "no book read for a time nothing is bought in")
        clock.ms = GOLD_0600 + R.LIVE_FULL_MS
        gold_card(x, clock, GOLD_0600, 4000, 4001)
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[0]["data"]["order"]["takerAmount"])), 50.0)
        self.assertNotIn("seen", t.record(slug), "no watch kept of a time the rule buys nothing in")
        self.assertIn("· 黄金休市 4 档 买涨 盘前不买｜盘中 ≤60¢（指数比起点高 0.005% 以上才买）", t.status())
        review = R.review(t.recs, t.settings, clock.ms)
        self.assertEqual([(r["cents"], r["live_cents"]) for r in review["rules"] if r["rule"] == "gold_shut"], [(0, 60)])

    # --- 1.154.24: the course goes first too ---
    async def test_in_its_course_the_index_brings_the_look(self):
        """1.154.24 — "这五个开盘后，盘中的优先级也是最高的". A window in its course is bought while the index stands on the rule's
        side of its start. The index comes each second (Binance's push: the bot's priced()), and the looks came each second
        too, of their own: half a second, on average, before the taker saw the index that put its side ahead. Now the index's
        arrival is the look — within priced() itself while the taker goes first, the order's request made before it returns;
        left to the loop's own tick (woken at once) while a tick runs. Once for an index; not for another card, a window not
        in its course, one all ordered, or a taker whose loop does not run. The order carries the index's arrival, and is not
        counted as an answer to a book that had just come."""
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"

        async def in_course():
            """A taker twenty seconds into the 06:00 window: Up not ahead, 30 for sale at 55¢ in the push connection's book."""
            t, api, clock, x = taker("on", RULE_GOLD_SHUT_LIVE_CENTS="70", RULE_SHARES="40", **IN)
            t.verify_tick = lambda: None
            api.markets["901"] = gold_market(GOLD_0600)
            api.listed.add(slug)
            api.books["901"] = {"bids": [], "asks": [[0.65, 30]]}
            clock.ms = GOLD_0600 - 60_000
            await t.tick()
            clock.ms = GOLD_0600 + 20_000
            gold_card(x, clock, GOLD_0600, 4000, 4000)
            await t.tick()
            self.assertEqual((t.track[slug]["word"], t.fast, api.sent), ("盘中：涨还没领先（指数 4,000，起点 4,000，要高出 0.005% 以上），不买", True, []))
            connected(t, "901", {"bids": [], "asks": [[0.55, 30]]})
            if t.warming is not None:
                await asyncio.wait([t.warming], timeout=2)  # the preparation done (pace began it): the sign-in and the allowance in hand
            self.assertTrue(t.allowance, "the exchange allowance read ahead of any order")
            x.chain_reads = []

            async def counted(key):
                x.chain_reads.append(key)
                return 10 ** 30
            x.owner.live.chain.allowance = counted
            t.running, t.stream.on_change = True, t.pushed_now  # as run() sets them
            t.stream.pushed.clear()  # (the loop at rest)
            return t, api, clock, x, t.track[slug]

        t, api, clock, x, w = await in_course()
        ticks = t.ticks
        # the index moves, Up still not ahead by 0.005%: looked at there and then — no await between the index and the look's word
        clock.ms += 300
        gold_card(x, clock, GOLD_0600, 4000, "4000.1")
        t.priced(["XAUUSDT", "XAU-15M"])
        if R.EAGER:
            self.assertEqual((w["word"], t.index_looks, t.ticks - ticks, t.first_ticks), ("盘中：涨还没领先（指数 4,000.1，起点 4,000，要高出 0.005% 以上），不买", 1, 1, 1))
            self.assertFalse(t.stream.pushed.is_set() or "901" in t.stream.changed, "taken whole within priced(): the loop is not woken for it")
        else:
            self.assertTrue(t.stream.pushed.is_set() and "901" in t.stream.changed, "an older Python: the loop is woken at once")
            await t.tick()
        # the same index told again (the push, then the fair values it moved): not looked at twice; nor for another card's
        t.priced(["XAU-15M"])
        t.priced(["BTC-15M", "CL-15M"])
        self.assertEqual(t.index_looks, 1)
        # while a tick runs (an order's answer awaited): left to the loop — woken for it at once, mid-tick too
        t.ticking += 1
        clock.ms += 300
        gold_card(x, clock, GOLD_0600, 4000, "4000.15")
        t.priced(["XAU-15M"])
        t.ticking -= 1
        self.assertEqual((t.index_looks, t.stream.pushed.is_set(), "901" in t.stream.changed, t.rest_s()), (2, True, True, R.REST_SOON_S))
        self.assertIn("4,000.1，", w["word"], "not looked at yet")
        await t.tick()
        self.assertIn("指数 4,000.15，", w["word"])
        self.assertEqual(api.sent, [])
        # Up ahead by 0.005%: the order's request made before priced() returns — the offer in the book in hand, no read before it
        t.stream.pushed.clear()
        asked, first = len(api.calls), t.first_ticks
        clock.ms += 300
        gold_card(x, clock, GOLD_0600, 4000, "4000.3")
        api.fill[1], api.price = 30.0, 0.55
        t.priced(["XAU-15M"])
        if not R.EAGER:
            await t.tick()
        self.assertEqual((len(api.sent), api.calls[asked:], x.chain_reads, t.index_looks), (1, [], [], 3), "no request before the order's: not of Predict, not of the chain")
        if R.EAGER:
            self.assertEqual(t.first_ticks - first, 1, "in the turn of the loop that brought the index")
        order = t.record(slug)["orders"][-1]
        self.assertEqual((order["live"], order["fresh"], order["shares"], order["limit"]), (True, False, 40.0, 0.58), "the course's first minute: no dearer than before the window")
        self.assertTrue(0 <= order["sent_wall"] - order.get("index_wall", 0) < 2_000, "from the index's arrival to the request: this turn (ms on the wall clock — a stalled builder's allowance)")
        for task in list(t.first_tasks):
            await asyncio.wait_for(task, 2)
        # all the window's shares ordered: an index that moves on brings no look
        clock.ms += 300
        gold_card(x, clock, GOLD_0600, 4000, "4000.5")
        t.priced(["XAU-15M"])
        self.assertEqual((t.index_looks, len(api.sent)), (3, 1))
        # how fast, in /rules: the course's orders the index brought, apart from those that answered a book
        speed = R.course_speed(t.recs, 0)
        self.assertEqual((speed["orders"], speed["own"] == speed["best"] == order["sent_wall"] - order["index_wall"]), (1, True))
        self.assertEqual(R.speed_stats(t.recs, 0)["timed"], 0, "not an answer to a book that had just come")
        self.assertRegex(t.speed_line(clock.ms), r"｜盘中由指数带来的 1 张：指数到达→发出 中位 \d+ ms（最快 \d+）$")
        self.assertIn("；盘中由指数带来的检查 3 次", t.first_line(clock.ms))
        self.assertIsNone(R.course_speed({}, 0))
        # a book that comes after a look the index brought is its own: its order answers that book, and carries no index
        b, apib, clockb, xb, wb = await in_course()
        clockb.ms += 300
        gold_card(xb, clockb, GOLD_0600, 4000, "4000.3")  # (Up ahead: seen by the looks, nothing for sale at the price)
        connected(b, "901", {"bids": [], "asks": [[0.65, 30]]})
        b.priced(["XAU-15M"])
        if not R.EAGER:
            await b.tick()
        self.assertEqual((wb["word"], apib.sent), ("盘中：盘口没有 ≤58¢ 的涨（最低 65¢）", []))
        clockb.ms += 300
        b.stream.take("901", {"version": 9, "bids": [], "asks": [[0.55, 30]]})
        if not R.EAGER:
            await b.tick()
        orderb = b.record(slug)["orders"][-1]
        self.assertEqual((len(apib.sent), orderb["fresh"], "index_wall" in orderb, R.course_speed(b.recs, 0)), (1, True, False, None))
        for task in list(b.first_tasks):
            await asyncio.wait_for(task, 2)
        # not a window before its start, nor one in the seconds around it; not while the taker's loop does not run
        e, apie, clocke, xe = taker("on", RULE_GOLD_SHUT_LIVE_CENTS="70", **IN)
        e.verify_tick = lambda: None
        apie.markets["901"] = gold_market(GOLD_0600)
        apie.listed.add(slug)
        apie.books["901"] = {"bids": [], "asks": [[0.65, 30]]}
        clocke.ms = GOLD_0600 - 60_000
        await e.tick()
        e.running, e.stream.on_change = True, e.pushed_now
        for at in (GOLD_0600 - 60_000, GOLD_0600 + R.LIVE_START_MS - 1):
            clocke.ms = at
            gold_card(xe, clocke, GOLD_0600, 4000, 4000 + at % 7)
            e.priced(["XAU-15M"])
        self.assertEqual((e.index_looks, e.ticks), (0, 1))
        clocke.ms = GOLD_0600 + R.LIVE_START_MS  # the course begins: the first index to come is looked at — the taker first from it on (pace)
        gold_card(xe, clocke, GOLD_0600, 4000, 4000)
        self.assertFalse(e.fast)
        e.priced(["XAU-15M"])
        self.assertEqual((e.index_looks, e.fast), (1, True))
        if not R.EAGER:
            await e.tick()
        self.assertIn("盘中：涨还没领先", e.track[slug]["word"])
        self.assertTrue(e.gc_off and e.quiet_ms(clocke.ms) == R.COURSE_QUIET_MS, "the course's first seconds: the rest of the bot out of the way, no collection")
        e.running = False
        gold_card(xe, clocke, GOLD_0600, 4000, 4002)
        e.priced(["XAU-15M"])
        self.assertEqual(e.index_looks, 1)
        if e.warming is not None:
            await asyncio.wait([e.warming], timeout=2)
        # --- what an index must not do (the review of 1.154.24) ---
        g, apig, clockg, xg, wg = await in_course()
        looks = lambda: (g.index_looks, g.ticks)
        # no order could go out (the pause, the loss stop, the API's cooldown), or the window is paused after a refusal: no tick
        # begun for nothing each second
        before = looks()
        xg.state.paused = "测试暂停"
        clockg.ms += 300
        gold_card(xg, clockg, GOLD_0600, 4000, "4000.11")
        g.priced(["XAU-15M"])
        xg.state.paused = ""
        apig.blocked_until = time.monotonic() + 60
        clockg.ms += 300
        gold_card(xg, clockg, GOLD_0600, 4000, "4000.12")
        g.priced(["XAU-15M"])
        apig.blocked_until = 0.0
        wg["look_ms"] = clockg.ms + R.REJECT_PAUSE_MS
        clockg.ms += 300
        gold_card(xg, clockg, GOLD_0600, 4000, "4000.13")
        g.priced(["XAU-15M"])
        self.assertEqual((looks(), "indexed" in wg), (before, False))
        # ...and a mark left on a window whose look is put off does not outlive the pause (due)
        wg["indexed"] = int(time.time() * 1000)
        g.stream.changed.add("901")
        self.assertEqual((g.due(wg, clockg.ms), "indexed" in wg, "901" in g.stream.changed), (False, False, False))
        wg["look_ms"] = clockg.ms
        # an index that comes old (a socket catching up after a stall: frames of seconds ago, one after another) brings no look
        # of its own — the newest one does
        clockg.ms += 300
        gold_card(xg, clockg, GOLD_0600, 4000, "4000.3", age_ms=R.INDEX_FRESH_MS + 1)  # Up "ahead", nine seconds ago for all the taker knows
        g.priced(["XAU-15M"])
        self.assertEqual((looks(), g.index_late, apig.sent), (before, 1, []))
        gold_card(xg, clockg, GOLD_0600, 4000, "4000.14", age_ms=R.INDEX_FRESH_MS)
        g.priced(["XAU-15M"])
        if not R.EAGER:
            await g.tick()
        self.assertEqual((g.index_looks - before[0], apig.sent), (1, []))
        self.assertIn("指数 4,000.14，", wg["word"])
        self.assertIn("（另有 1 次指数到达时已超过 1.5 秒，留给每秒的检查", g.first_line(clockg.ms))
        # the same index told again is not looked at twice — but it is once the card has moved on to this window, or got its
        # start: what the first look of a course waits for
        clockg.ms += 300
        gold_card(xg, clockg, GOLD_0600 - 900_000, 4000, "4000.14")  # (the card still on the window before)
        g.priced(["XAU-15M"])
        if not R.EAGER:
            await g.tick()
        self.assertEqual(wg["word"], "盘中：卡片还没换到这一场，等起点价")
        gold_card(xg, clockg, GOLD_0600, None, "4000.14")  # on this window, its start not read yet
        g.priced(["XAU-15M"])
        if not R.EAGER:
            await g.tick()
        self.assertEqual(wg["word"], "盘中：还没读到这一场的起点价")
        gold_card(xg, clockg, GOLD_0600, 4000, "4000.14")  # the start read: the same index, looked at now
        g.priced(["XAU-15M"])
        g.priced(["XAU-15M"])
        if not R.EAGER:
            await g.tick()
        self.assertIn("盘中：涨还没领先（指数 4,000.14，起点 4,000，", wg["word"])
        self.assertEqual(g.index_looks - before[0], 4, "one look each: the card's window, its start, the start read — and not twice")
        # the minute's plan is not on the way of a look an index (or a book) brought: left to the loop's own tick
        g.planned_ms = planned = clockg.ms - R.PLAN_EVERY_MS - 1
        clockg.ms += 300
        gold_card(xg, clockg, GOLD_0600, 4000, "4000.16")
        g.priced(["XAU-15M"])
        if R.EAGER:
            self.assertEqual((g.planned_ms, g.first_tick), (planned, False))
            self.assertIn("指数 4,000.16，", wg["word"])
        await g.tick()
        self.assertEqual(g.planned_ms, clockg.ms)
        # a look that comes seconds after the index that marked it (a tick was running) answers neither that index nor a book
        # that just came: its order is counted with neither
        g.ticking += 1
        clockg.ms += 300
        gold_card(xg, clockg, GOLD_0600, 4000, "4000.3")
        g.priced(["XAU-15M"])
        g.ticking -= 1
        self.assertTrue(wg.get("indexed") and "901" in g.stream.changed)
        wg["indexed"] -= 5_000  # (five seconds ago)
        await g.tick()
        orderg = g.record(slug)["orders"][-1]
        self.assertEqual((len(apig.sent), orderg["fresh"], "index_wall" in orderg), (1, False, False))
        self.assertEqual((R.speed_stats(g.recs, 0)["timed"], R.course_speed(g.recs, 0)), (0, None))
        # the bot's own priced(): the rule taker is told first — whatever it was handed — and the express lane all the same,
        # the taker's failure its own
        bot = L.LiveBot.__new__(L.LiveBot)
        got, logged = [], []
        bot.price_dirty, bot.price_look = set(), None
        bot.log_limited = lambda key, text, every=60: logged.append((key, text))

        async def no_looks():
            pass
        bot.price_looks = no_looks
        bot.priced(["HSI"])  # (no rule taker yet)
        bot.rules = types.SimpleNamespace(running=True, priced=lambda keys: got.append((list(keys), set(bot.price_dirty))))
        bot.priced(key for key in ("XAUUSDT", "XAU-15M"))
        self.assertEqual((got, bot.price_dirty), ([(["XAUUSDT", "XAU-15M"], {"HSI"})], {"HSI", "XAUUSDT", "XAU-15M"}))

        def broken(keys):
            raise RuntimeError("坏了")
        bot.rules.priced = broken
        bot.priced(["CL-15M"])
        self.assertEqual((logged, "CL-15M" in bot.price_dirty), ([("rules-priced", "规则吃单按指数检查出错：坏了")], True))
        bot.rules = types.SimpleNamespace(running=False, priced=broken)
        bot.priced(["BTC-15M"])
        self.assertEqual(len(logged), 1, "a taker whose loop does not run is not told")
        await bot.price_look

    async def test_the_courses_first_seconds_and_a_wait_for_matching_are_kept_clear_too(self):
        """1.154.24: quiet_ms's other seconds. The course of a window begins LIVE_START_MS after a quarter hour — where the
        pre-open mode signs the windows just listed — and at the reopen the index has gapped by then: from QUIET_BEFORE_MS
        before it to COURSE_QUIET_MS after, for a rule that buys there, the rest of the bot keeps out of the loop's way as it
        does at the makers' moment. And for as long as an offer in hand waits for matching to open (rush), however long after
        the moment it showed. Nobody waits for a window bought in full or an order that cannot go out; the collector is
        held off with them; /rules says what the taker goes first for in the day ahead."""
        t, api, clock, x = taker("on", RULE_GOLD_SHUT_LIVE_CENTS="70", **IN)  # (no makers' moment: RULE_LEAD_MINUTES 0)
        t.verify_tick = lambda: None
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": []}
        clock.ms = GOLD_0600 - 60_000
        await t.tick()
        begins = GOLD_0600 + R.LIVE_START_MS
        self.assertEqual(t.quiet_ms(begins), 0, "the bot has no card of the market: nothing says who is ahead, nothing is bought")
        gold_card(x, clock, GOLD_0600, 4000, 4000)
        for at, left in ((begins - R.QUIET_BEFORE_MS - 1, 0), (begins - R.QUIET_BEFORE_MS, R.COURSE_QUIET_MS + R.QUIET_BEFORE_MS), (begins, R.COURSE_QUIET_MS),
                         (begins + 5_000, R.COURSE_QUIET_MS - 5_000), (begins + R.COURSE_QUIET_MS - 1, 1), (begins + R.COURSE_QUIET_MS, 0)):
            self.assertEqual(t.quiet_ms(at), left, at - begins)
        self.assertEqual((R.LIVE_START_MS - R.QUIET_BEFORE_MS, R.LIVE_START_MS + R.COURSE_QUIET_MS), (2_000, 15_000), "2 to 15 seconds after the start")
        x.state.paused = "测试暂停"
        self.assertEqual(t.quiet_ms(begins), 0, "no order could go out: nobody waits for it")
        x.state.paused = ""
        rule = R.RULE_OF[t.track[slug]["rule"]]
        rec = t.new_record(slug, t.track[slug], rule, 1)
        rec["orders"].append({"state": "final", "shares": float(t.settings.shares_of("gold")), "filled": float(t.settings.shares_of("gold")), "limit": 0.6})
        t.save(rec)
        self.assertEqual(t.quiet_ms(begins), 0, "bought in full")
        t.recs.clear()
        plain, api0, clock0, x0 = taker("on", **IN)  # no price for the course: the window is the rule's until its start alone
        api0.markets["901"] = gold_market(GOLD_0600)
        api0.listed.add(slug)
        clock0.ms = GOLD_0600 - 60_000
        await plain.tick()
        gold_card(x0, clock0, GOLD_0600, 4000, 4000)
        self.assertEqual((plain.quiet_ms(begins), slug in plain.track), (0, True))
        # no price before the window: the course's first minute is bought no dearer than that — nothing in those seconds
        late, api2, clock2, x2 = taker("on", RULE_GOLD_SHUT_CENTS="0", RULE_GOLD_SHUT_LIVE_CENTS="60", **IN)
        api2.markets["901"] = gold_market(GOLD_0600)
        api2.listed.add(slug)
        clock2.ms = GOLD_0600 - 60_000
        await late.tick()
        gold_card(x2, clock2, GOLD_0600, 4000, 4000)
        self.assertEqual((late.quiet_ms(begins), slug in late.track, late.live_cents(rule)), (0, True, 60))
        # the day's cap spent: nobody waits for an order that will not be made (the makers' moment's seconds likewise)
        capped, api3, clock3, x3 = taker("on", RULE_GOLD_SHUT_LIVE_CENTS="70", RULE_MAX_DAY_USD="50", **IN)
        api3.markets["901"] = gold_market(GOLD_0600)
        api3.listed.add(slug)
        clock3.ms = GOLD_0600 - 60_000
        await capped.tick()
        gold_card(x3, clock3, GOLD_0600, 4000, 4000)
        self.assertEqual(capped.quiet_ms(begins), R.COURSE_QUIET_MS)
        other = f"xau-updown-15m-{(GOLD_0600 - 900_000) // 1000}"
        spent = capped.new_record(other, {"rule": "gold_shut", "start": GOLD_0600 - 900_000, "market": gold_market(GOLD_0600 - 900_000, mid=899)}, rule, 1)
        spent["orders"].append({"state": "final", "shares": 100.0, "filled": 100.0, "limit": 0.5, "avg": 0.5, "at": GOLD_0600 - 900_000})
        capped.save(spent)
        self.assertEqual((R.spent_on(capped.recs, R.day_of(begins), "on") >= 49.0, capped.quiet_ms(begins)), (True, 0))
        # an offer in hand, matching not open yet (rush): for as long as that lasts, RUSH_QUIET_MS at a time
        w, far = t.track[slug], GOLD_0600 - 30 * 60_000
        self.assertEqual(t.quiet_ms(far), 0)
        w["rush_task"] = asyncio.get_running_loop().create_future()
        self.assertEqual((t.rushing(w), t.quiet_ms(far), t.quiet_ms(far + 60_000)), (True, R.RUSH_QUIET_MS, R.RUSH_QUIET_MS))
        w["rush_task"].set_result(None)
        self.assertEqual(t.quiet_ms(far), 0, "matching open (or the offer gone): the others go on")
        # the collector is held off with them
        import gc
        watch = m.GC_WATCH
        self.assertTrue(gc.isenabled() and not watch.holders)
        t.running = True
        try:
            for at, off in ((begins - R.QUIET_BEFORE_MS - R.GC_BEFORE_MS - 1, False), (begins - R.QUIET_BEFORE_MS - R.GC_BEFORE_MS, True), (begins, True),
                            (begins + R.COURSE_QUIET_MS - 1, True), (begins + R.COURSE_QUIET_MS, False)):
                t.gc_guard(at)
                self.assertEqual((t.gc_off, gc.isenabled()), (off, not off), at - begins)
            w["rush_task"] = asyncio.get_running_loop().create_future()
            t.gc_guard(far)
            self.assertTrue(t.gc_off and not gc.isenabled())
            w["rush_task"].set_result(None)
            t.gc_guard(far)
            self.assertTrue(gc.isenabled() and not t.gc_off and not watch.holders)
        finally:
            t.running = False
            while watch.holders:
                watch.release()
        # /rules: the user's own five — gold's four shut quarter hours and the reopen's first, before their start and in their
        # course — as the day ahead has them at 00:30 (their makers' moments 01:00 to 02:00; 13:15 is the 18:15 window's)
        u, _, clocku, _ = taker("on", RULE_FAST_HOURS="01:00-07:00", RULE_OIL="off", RULE_GOLD_SHUT_CENTS="60", RULE_GOLD_SHUT_LIVE_CENTS="60",
                                RULE_GOLD_OPEN_CENTS="60", RULE_GOLD_OPEN_LIVE_CENTS="60")
        clocku.ms = bj(2026, 11, 17, 0, 30)
        u.plan(clocku.ms)
        self.assertEqual(u.first_plan(clocku.ms), "接下来一天：盘前抢单时刻 01:00、01:15、01:30、01:45、02:00、13:15；盘中 06:00–07:15")
        line = u.first_line(clocku.ms)
        self.assertIn("（不等主循环轮到）；盘中的场次，指数一到也当场看（不等下一秒的检查）；请求走规则吃单自己的线程；", line)
        self.assertIn("；对手挂单时刻前 1 秒到后 15 秒、盘中开头（开场后 2～15 秒）、卖单已到而撮合还没开启时（都要这一场还能买），预挂不发新单", line)
        self.assertTrue(line.endswith("｜接下来一天：盘前抢单时刻 01:00、01:15、01:30、01:45、02:00、13:15；盘中 06:00–07:15｜现在不在优先时段，下一次 00:59 开始"), line)
        # in the 06:00 window's course: what is left of today, and tomorrow's with its day (a stretch that reaches the end of
        # what is planned is told by its beginning)
        clocku.ms = bj(2026, 11, 17, 6, 5)
        u.plan(clocku.ms)
        self.assertEqual(u.first_plan(clocku.ms), "接下来一天：盘前抢单时刻 13:15、11-18 01:00；盘中 06:00–07:15、11-18 06:00 起")
        self.assertEqual(plain.first_plan(GOLD_0600 - 60_000), "", "no makers' moment set, no course bought: nothing to tell")

    # --- 1.154.15: a window bought at once, the order's way without its waits ---
    async def test_a_ladder_is_bought_in_one_order(self):
        """11-18, the 06:45 window: a ladder of offers from 50¢ up, some 300 shares within the rule's 60¢. It was bought in two bites —
        the order sized on the book in hand and cut to LIVE_MAX_ORDER_USD (166 shares), the next one only after that one's word
        (46 shares four seconds on, the rest gone). Now one order asks for all the window may buy, whatever the book in hand shows
        of the ladder; and the window's shares are spoken for until its final word."""
        t, api, clock, x = taker("on", RULE_GOLD_SHARES="300", RULE_GOLD_SHUT_CENTS="60", LIVE_MAX_ORDER_USD="100", **IN)
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.50, 100]]}  # the ladder's first step in hand; the others reach the book before the order does
        api.fill[1] = 293.5  # what stood at 60¢ or under when the order arrived: 100 + 49.2 + 46.2 + 57.4 + 40.7
        await t.tick()
        self.assertEqual([L.from_wei(int(b["data"]["order"]["takerAmount"])) for b in api.sent], [300.0],
                         "one order, for the whole window: not the 100 in hand, not LIVE_MAX_ORDER_USD's 166")
        order = t.record(slug)["orders"][0]
        self.assertEqual((order["shares"], order["seen"], order["limit"], order["state"]), (300.0, 100.0, 0.6, "open"))
        connected(t, "901", {"bids": [], "asks": [[0.55, 60]]})  # more offered a moment later: the order on the book is there to buy it — no second one
        t.stream.changed.add("901")
        clock.ms += 500
        await t.tick()
        self.assertEqual(len(api.sent), 1)
        self.assertEqual(t.track[slug]["word"], "这一场的 300 份已全部下单，等结果（撤回的部分 60 秒后确认没有晚到的成交才算空出来）")
        clock.ms += R.FILL_READ_MS
        await t.tick()
        self.assertEqual((order["state"], order["filled"], api.removed), ("cancelling", 293.5, ["1"]), "read, and what it did not take withdrawn")
        self.assertIn("累计 293.5/300", x.notes[-1])
        clock.ms += R.FOLLOW_EVERY_MS
        await t.tick()
        self.assertEqual(order["state"], "closing")
        # its final word a minute on: 6 of the window's 300 are free again, and bought of what is on offer then
        clock.ms = order["left_ms"] + R.LATE_FILL_MS
        await t.tick()
        self.assertEqual(order["state"], "final")
        t.stream.changed.add("901")
        clock.ms += 1_000
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[1]["data"]["order"]["takerAmount"])), 6.0)

    async def test_a_small_offer_is_bought_as_it_shows_and_orders_go_together(self):
        """A stray small offer is bought as the book shows it — an order for the whole window would hold the window's shares for a
        minute after its rest is withdrawn. The offers in hand are first those of the orders already on their way (not bought
        twice); what shows beyond them is ordered at once, beside them — with enough of it, all the window may still buy; three
        on their way at the most; and no more than the USDT at hand buys."""
        t, api, clock, x = taker("on", RULE_GOLD_SHARES="300", **IN)
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 10]]}
        sizes = lambda: [L.from_wei(int(b["data"]["order"]["takerAmount"])) for b in api.sent]

        async def push(asks):
            connected(t, "901", {"bids": [], "asks": asks})
            t.stream.changed.add("901")
            clock.ms += 200
            await t.tick()
        await t.tick()
        self.assertEqual(sizes(), [10.0], "10 on offer, the window may buy 300: the ten")
        await push([[0.51, 10]])
        self.assertEqual((sizes(), t.track[slug]["word"]), ([10.0], "盘口 ≤58¢ 的涨 10 份，在途的单正在吃"), "the same ten: the first order's")
        await push([[0.51, 10], [0.52, 12]])
        self.assertEqual(sizes(), [10.0, 12.0], "twelve more: ordered beside the first, not after its word")
        await push([[0.51, 10], [0.52, 12], [0.53, 15]])
        self.assertEqual((sizes(), R.claims(t.record(slug)), len(R.open_orders(t.record(slug)))), ([10.0, 12.0, 15.0], 37.0, 3))
        await push([[0.51, 10], [0.52, 12], [0.53, 15], [0.54, 18]])
        self.assertEqual((sizes(), t.track[slug]["word"]), ([10.0, 12.0, 15.0], "这一场已有 3 张单在途，等它们的结果"))
        # a ladder behind a small offer: the small one bought as it showed, then — beside it — all the window may still buy
        t2, api2, clock2, x2 = taker("on", RULE_GOLD_SHARES="300", **IN)
        api2.markets["901"] = gold_market(GOLD_0600)
        api2.listed.add(slug)
        api2.books["901"] = {"bids": [], "asks": [[0.51, 10]]}
        await t2.tick()
        connected(t2, "901", {"bids": [], "asks": [[0.51, 10], [0.52, 100], [0.53, 50]]})
        t2.stream.changed.add("901")
        clock2.ms += 200
        await t2.tick()
        self.assertEqual([L.from_wei(int(b["data"]["order"]["takerAmount"])) for b in api2.sent], [10.0, 290.0])
        self.assertEqual(R.held(t2.record(slug)), 300.0)
        # the USDT at hand: $60 buys 103 shares at the order's 58¢ — that many, not none
        t3, api3, clock3, x3 = taker("on", RULE_GOLD_SHARES="300", **IN)
        x3.state.usd = 60.0
        api3.markets["901"] = gold_market(GOLD_0600)
        api3.listed.add(slug)
        api3.books["901"] = {"bids": [], "asks": [[0.50, 100]]}
        await t3.tick()
        self.assertEqual([L.from_wei(int(b["data"]["order"]["takerAmount"])) for b in api3.sent], [103.0])

    async def test_an_order_is_journaled_before_it_leaves(self):
        """1.154.15: the order's hash where a restart finds it, before the request — a line in the taker's own journal beside the
        database, not a wait for the store's writer to have written every record the bot had queued (in a thread it first had
        to wait for). An order the store never got — the process ended in between — is found in the journal at the next
        start and looked for by its hash, never sent again."""
        import os
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "state.db")
            _, _, clock, x = taker("on")
            x.owner.store = m.Store(path)
            t = R.RuleTaker(x.owner)
            api = t.api = Api(t)
            journal = path + R.JOURNAL_SUFFIX
            self.assertTrue(os.path.exists(journal) and t.journal_fd is not None)
            slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
            api.markets["901"] = gold_market(GOLD_0600)
            api.listed.add(slug)
            api.books["901"] = {"bids": [], "asks": [[0.51, 50]]}
            api.fill[1] = 50.0
            flushes, lines, plain_flush, plain_create = [], [], x.owner.store.flush, api.create_order
            x.owner.store.flush = lambda *a: flushes.append(1) or plain_flush(*a)

            async def create(body):
                lines.append(open(journal, encoding="utf-8").read())  # the journal as the request leaves
                return await plain_create(body)
            api.create_order = create
            await t.tick()
            digest = api.sent[0]["data"]["order"]["hash"].lower()
            entry = json.loads(lines[0].strip().splitlines()[-1])
            self.assertEqual((entry["key"], entry["order"]["hash"], entry["order"]["state"], entry["rec"]["slug"]), (slug, digest, "placing", slug))
            self.assertEqual(flushes, [], "no wait for the store's writer between the offer and the order")
            # the process ends before the store has the record: the journal has the order
            x.owner.store.flush = plain_flush
            x.owner.store.delete_keys([R.PREFIX + slug])
            x.owner.store.flush()
            again = R.RuleTaker(x.owner)
            order = again.record(slug)["orders"][0]
            self.assertEqual((order["hash"], order["state"], len(again.record(slug)["orders"])), (digest, "unknown", 1))
            self.assertEqual((os.path.getsize(journal), x.owner.store.get(R.PREFIX + slug)["orders"][0]["hash"]), (0, digest),
                             "on the record and in the store: the journal begun anew")
            api2 = again.api = Api(again)
            api2.markets, api2.listed, api2.books, api2.rows = api.markets, api.listed, api.books, api.rows
            await again.tick()
            order = again.record(slug)["orders"][0]
            self.assertEqual((api2.sent, order["state"], order["filled"], order["id"]), ([], "final", 50.0, "1"), "found by its hash, not sent again")
            # one the store does hold is left as it is; a line cut short is passed over
            with open(journal, "a", encoding="utf-8") as f:
                f.write(lines[0] + '{"key": "x", "order": {"hash": "0xcu')
            third = R.RuleTaker(x.owner)
            self.assertEqual([(o["state"], o["filled"]) for o in third.record(slug)["orders"]], [("final", 50.0)])
            x.owner.store.close()
        # a store in memory has no such file: the store's flush before the request, as before
        t, api, clock, x = taker("on")
        self.assertEqual((t.journal_fd, t.journal({"slug": "s", "mode": "on"}, {})), (None, False))

    async def test_the_orders_way_has_no_waits_of_its_own(self):
        """1.154.15, what stood between an offer and its order: the day's P&L for the loss stop summed over every record of the
        bot (after a tick's looks now: an order finds it summed); a chain read of an allowance that has no limit; a tenth of
        a second before the loop looked at a book that changed while it was busy; the window in question behind the others'
        looks; the hot window's REST reads a second apart on a key of the taker's own."""
        t, api, clock, x = taker("on", **FAST)
        t.verify_tick = lambda: None
        t.warm = lambda: asyncio.sleep(0)  # (the hours' own preparation reads the allowance too: this is about the order's way)
        sums, reads = [], []
        x.owner.live_daily_pnl = lambda trades, now: sums.append(now) or 0.0

        async def allowance(key):
            reads.append(key)
            return 2 ** 256 - 1
        x.owner.live.chain.allowance = allowance
        mids = {"901": GOLD_0600, "902": GOLD_0600 + 900_000}
        for mid, start in mids.items():
            api.markets[mid] = gold_market(start, mid=int(mid))
            api.listed.add(api.markets[mid]["categorySlug"])
            api.books[mid] = {"bids": [], "asks": []}
        await t.tick()
        self.assertEqual(len(sums), 1, "summed once, at the first gate")
        for mid in mids:
            connected(t, mid, {"bids": [], "asks": []})
        # both windows are offered at once; the later one's book is the one that just changed: its order first
        connected(t, "901", {"bids": [], "asks": [[0.52, 25]]})
        t.stream.take("902", {"version": 9, "bids": [], "asks": [[0.53, 30]]})
        summed, plain = [], api.create_order

        async def create(body):
            summed.append(len(sums))  # how often the day was summed, as the request leaves
            return await plain(body)
        api.create_order = create
        clock.ms += 1_000
        await t.tick()
        self.assertEqual([b["data"]["order"]["tokenId"] for b in api.sent], ["1804", "1802"], "the window whose book just came, then the other")
        self.assertEqual((summed, len(reads)), ([1, 1], 1), "three checks an order (the gate, its own, the one before its bytes): none summed the day again")
        self.assertEqual(len(sums), 2, "summed after the tick's looks: in hand for the next order")
        # an allowance without a limit is not read again a minute on (one with a limit is: ALLOWANCE_MS)
        slug3 = f"xau-updown-15m-{(GOLD_0600 + 1_800_000) // 1000}"
        api.markets["903"] = gold_market(GOLD_0600 + 1_800_000, mid=903)
        api.listed.add(slug3)
        api.books["903"] = {"bids": [], "asks": [[0.52, 25]]}
        clock.ms += 5 * 60_000
        await t.tick()
        self.assertEqual((len(api.sent), len(reads)), (3, 1))
        # the loop's rest: a tick; a tenth of a second on a change its looks left; at once for one that came while it was busy
        t.stream.changed.clear()
        self.assertEqual(t.rest_s(), R.TICK_S)
        t.stream.changed.add("901")
        t.looked = t.stream.arrivals
        self.assertEqual(t.rest_s(), R.REST_MIN_S)
        t.stream.take("901", {"version": 99, "bids": [], "asks": [[0.59, 1]]})
        self.assertEqual(t.rest_s(), R.REST_SOON_S)
        # the loop's own delay, as /rules shows it
        t.loop_lags.extend([1.0, 3.0, 40.0])
        self.assertIn("；机器人主循环延迟近 1 分钟中位 3 ms、最长 40 ms", t.fast_line(clock.ms))
        # the window at the makers' moment on a key of the taker's own: read again a third of a second on (a second on the bot's key)
        own, api4, clock4, _ = taker("on", RULE_FAST_HOURS="off", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0",
                                     RULE_API_KEY="rule-key", PREDICT_API_KEY="main-key")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api4.markets["901"] = gold_market(GOLD_0600)
        api4.listed.add(slug)
        api4.books["901"] = {"bids": [], "asks": []}
        clock4.ms = GOLD_0600 - 300 * 60_000 - 5_000  # five seconds before the 06:00 window's moment (01:00)
        hold, own.verify_tick = own.verify_tick, lambda: None
        await own.tick()
        own.verify_tick = hold
        connected(own, "901", {"bids": [], "asks": []}, age=300)
        self.assertTrue(own.own_key and own.hot(own.track[slug], clock4.ms))
        own.track[slug]["rest_at"] = time.monotonic() - 60
        read = lambda: sum(1 for c in api4.calls if c.endswith("/orderbook"))
        n = read()
        for _ in range(3):
            own.verify_at -= R.VERIFY_EVERY_OWN_S
            own.track[slug]["rest_at"] -= R.VERIFY_EVERY_OWN_S
            own.verify_tick()
            await own.verifying
        self.assertEqual(read() - n, 3, "again after a third of a second, three times running")
        own.verify_at -= R.VERIFY_EVERY_OWN_S
        own.track[slug]["rest_at"] -= R.VERIFY_EVERY_OWN_S / 3
        own.verify_tick()
        self.assertEqual(read() - n, 3, "not sooner")

    def test_speed_is_that_of_the_orders_that_answered_a_book(self):
        """The taker's own time — a book's arrival to the request leaving — counts the orders whose look answered a book that had
        just come: 11-18 it read 2,098 ms, the median of a first order and of the bite that waited four seconds for that one's
        word. The fastest of each beside the medians (/rules: 最快)."""
        first = {"sent_wall": 1_100, "seen_wall": 1_040, "look_wall": 1_050, "book_ts": 800, "answered_wall": 1_400, "fresh": True, "filled": 166.0}
        bite = {"sent_wall": 6_000, "seen_wall": 2_000, "look_wall": 5_990, "book_ts": 1_800, "answered_wall": 6_350, "fresh": False, "filled": 46.0}
        recs = {"a": {"mode": "on", "start_ms": 10, "orders": [first, bite]}}
        sp = R.speed_stats(recs, 0)
        self.assertEqual((sp["orders"], sp["filled"], sp["own"], sp["wake"], sp["timed"]), (2, 2, 60, 10, 1), "the first order's 60 ms, not the bite's four seconds")
        self.assertEqual((sp["lag"], sp["trip"], sp["best"]), (220.0, 325.0, {"lag": 200, "own": 60, "trip": 300}))
        old = {"a": {"mode": "on", "start_ms": 10, "orders": [{k: v for k, v in o.items() if k not in {"fresh", "look_wall"}} for o in (first, bite)]}}
        sp = R.speed_stats(old, 0)
        self.assertEqual((sp["own"], sp["wake"], sp["best"]["own"]), (2_030.0, None, 60), "the orders of before carry no mark: all of them, as it was")
        t, api, clock, x = taker("on")
        t.recs = {"a": {**recs["a"], "start_ms": clock.ms}}
        self.assertEqual(t.speed_line(clock.ms), "下单速度（近 30 天真实订单 2 张，2 张有成交）：盘口变化→推送到达 中位 220 ms（最快 200）、到达→发出 60 ms（最快 60）"
                                                 "［其中到达→开始处理 10 ms］、发出→应答 325 ms（最快 300）")


    async def test_each_windows_race_is_in_the_status_and_survives_a_restart(self):
        """1.154.16 — "没买到的卖单存活了多久，也就是抢走它的人有多快：这个在哪里看". /rules had a month's medians, and those only
        once a window had begun; the watch was in memory alone (11-18: three deploys after the offers of 01:00–01:45, the watches
        of those windows knew of none). Now the watch is on the window's record the moment an offer shows, is taken from, is
        gone — a restart finds it — and /rules lists the latest windows' races, a line each: when the offer showed and how long
        before the start, how much, how soon it was first taken from and gone (Predict's own stamps), what was bought, each
        order against the clock."""
        t, api, clock, x = taker("on", RULE_GOLD_SHARES="300", RULE_GOLD_SHUT_CENTS="60", **FAST)
        t.verify_tick = lambda: None
        mids = {"901": GOLD_0600, "902": GOLD_0600 + 900_000}
        for mid, start in mids.items():
            api.markets[mid] = gold_market(start, mid=int(mid))
            api.listed.add(api.markets[mid]["categorySlug"])
            api.books[mid] = {"bids": [], "asks": []}
        slug1, slug2 = (api.markets[mid]["categorySlug"] for mid in ("901", "902"))
        clock.ms = GOLD_0600 - 300 * 60_000 - 2_000  # 00:59:58, five hours before the 06:00 window
        await t.tick()
        for mid in mids:
            connected(t, mid, {"bids": [], "asks": []})
        self.assertNotIn("抢单记录", t.status())
        wall = int(time.time() * 1000)
        # 901: a ladder shows at 01:00:01 — bought in part: a faster taker begins on it 0.4 s after it showed, it is gone 1.2 s after
        clock.ms += 3_000
        t.stream.take("901", {"version": 2, "updateTimestampMs": wall - 300, "bids": [], "asks": [[0.50, 100], [0.51, 49.2], [0.52, 46.2]]})
        api.fill[1] = 212.0
        await t.tick()
        kept = x.owner.store.get(R.PREFIX + slug1)["seen"]
        self.assertEqual((kept["open"], kept["offer_ts"], kept["offer_max"], kept["start_ms"], kept["cents"]), (True, wall - 300, 195.4, GOLD_0600, 60),
                         "on the record the moment the offer shows — not at the window's start, five hours on")
        self.assertEqual((t.record(slug1)["index_set"], R.unverified(t.recs)), (1, {("901", 1)}), "the record the watch made knows its outcome once an order goes on it")
        t.stream.take("901", {"version": 3, "updateTimestampMs": wall + 100, "bids": [], "asks": [[0.52, 20], [0.53, 57.4]]})
        clock.ms += 200
        await t.tick()
        t.stream.take("901", {"version": 4, "updateTimestampMs": wall + 900, "bids": [], "asks": []})
        clock.ms += 800
        await t.tick()
        seen = t.track[slug1]["seen"]
        self.assertEqual((seen["cut_ts"] - seen["offer_ts"], seen["gone_ts"] - seen["offer_ts"]), (400, 1_200))
        self.assertEqual(x.owner.store.get(R.PREFIX + slug1)["seen"]["gone_ts"], wall + 900, "...and as it is taken from and gone")
        clock.ms += R.FILL_READ_MS
        await t.tick()
        # the process is restarted: the watch of the window, hours from its start, is as it stood
        again = R.RuleTaker(x.owner)
        api2 = again.api = Api(again)
        api2.markets, api2.listed, api2.books, api2.rows = api.markets, api.listed, api.books, api.rows
        again.verify_tick = lambda: None
        await again.tick()
        back = again.track[slug1]["seen"]
        self.assertEqual((back["offer_ts"], back["cut_ts"], back["gone_ts"], back["offer_max"], back["listed_ms"]),
                         (wall - 300, wall + 100, wall + 900, 195.4, seen["listed_ms"]), "restored, the listing's moment as first seen")
        for mid in mids:
            connected(again, mid, {"bids": [], "asks": []})
        # 902: an offer shows at 01:15:01 and a faster taker has all of it 0.35 s later; this taker's order finds nothing
        clock.ms = GOLD_0600 + 900_000 - 300 * 60_000 + 1_000
        again.stream.take("902", {"version": 2, "updateTimestampMs": wall - 250, "bids": [], "asks": [[0.50, 100], [0.51, 50]]})
        await again.tick()
        again.stream.take("902", {"version": 3, "updateTimestampMs": wall + 100, "bids": [], "asks": []})
        clock.ms += 400
        await again.tick()
        clock.ms += R.FILL_READ_MS
        await again.tick()
        rows = R.races(again.recs, "on", 0)
        self.assertEqual([(r["slug"], r["lead_min"], r["peak"], r["cut_ms"], r["gone_ms"], r["asked"], r["bought"], r["open"]) for r in rows],
                         [(slug2, 300.0, 150.0, 350, 350, 300.0, 0.0, True), (slug1, 300.0, 195.4, 400, 1_200, 300.0, 212.0, True)], "the latest window first")
        first = rows[1]["orders"][0]
        self.assertEqual((first["shares"], first["filled"], first["seen"], first["fresh"], first["after"] - first["lag"] - first["own"]), (300.0, 212.0, 195.4, True, 0),
                         "how long after the offer showed the order left: the push's lag and the taker's own time")
        status = again.status()
        self.assertIn("抢单记录（最近 2 场，北京时间；“开始被吃 / 被吃光”是卖单出现之后多久，按 Predict 盘口自己的时间戳——抢走它的人有多快）：", status)
        self.assertRegex(status, r"  11-17 06:15 黄金休市 4 档：≤60¢ 的卖单 \d\d:\d\d:\d\d\.\d{3} 出现（开场前 300 分钟），最多 150 份；0\.35 秒后开始被吃、0\.35 秒后被吃光；"
                                 # (1.154.22: the taker's own time in its parts — to the look, to the order signed, to its request leaving)
                                 r"下了 300 份，没买到｜第 1 张 要 300 成交 0（当时盘口上 150 份）：出现后 \d+ ms 发出、推送 \d+ ms、"
                                 r"到达→发出 \d+ ms［到达→开始处理 \d+ ms、处理→签好 \d+ ms、签好→发出 \d+ ms］、往返 \d+ ms")
        self.assertRegex(status, r"  11-17 06:00 黄金休市 4 档：≤60¢ 的卖单 \d\d:\d\d:\d\d\.\d{3} 出现（开场前 300 分钟），最多 195\.4 份；0\.40 秒后开始被吃、1\.20 秒后被吃光；"
                                 r"买到 212/300 份｜第 1 张 要 300 成交 212（当时盘口上 195\.4 份）：出现后 \d+ ms 发出")
        # 1.154.17: the races in a line — how regular the makers' moment is (to time an order to it), how soon the first taker was
        # on the offers, how soon after their showing this taker's orders left
        self.assertRegex(status, r"  近 30 天：卖单出现在整刻钟后 中位 \d+\.\d\d 秒（\d+\.\d\d～\d+\.\d\d，2 场）；最快的人在出现后 中位 0\.38 秒下手（最快 0\.35）；"
                                 r"我们的单在出现后 中位 \d+ ms 发出（最快 \d+），再加半个往返才到")
        self.assertEqual(R.race_summary([]), "")
        # ...and how far Predict is: the taker's own requests' round trips, the handshake of new connections, the site they enter at
        self.assertEqual(again.net_line(), "", "nothing measured yet (the tests' stand-in client keeps no clock)")
        again.api.base, again.api.trips = "https://api.predict.fun/v1", [("GET", 310.0), ("GET", 290.0), ("GET", 350.0), ("POST", 330.0), ("POST", 300.0)]
        m.http_net("api.predict.fun")["connect"].extend([12.0, 9.0, 15.0])
        m.http_net("api.predict.fun")["edge"] = "8a1b2c3d4e5f-AMS"
        try:
            self.assertEqual(again.net_line(), "网络（不含排队）：机器人在 未知区域｜到 api.predict.fun 新建连接 中位 12 ms（最快 9），是 2～3 个来回｜入口 8a1b2c3d4e5f-AMS"
                                               "｜读一次 中位 310 ms（最快 290）｜下单 / 撤单一次 中位 315 ms（最快 300）"
                                               "｜入口很近、请求却慢：Predict 的服务器离这个区域远（或它处理慢）——换一个部署区域试：在那个区域新建一个服务、启动命令填 python -u rules.py latency，量出来再比")
            self.assertIn(again.net_line(), again.status())
            again.api.trips = [("GET", 42.0), ("POST", 61.0)]
            self.assertTrue(again.net_line().endswith("｜读一次 中位 42 ms（最快 42）｜下单 / 撤单一次 中位 61 ms（最快 61）"), "near: no hint")
        finally:
            m.HTTP_NET.pop("api.predict.fun", None)
        # the month's medians count the windows whose watch is over: none yet — then both
        self.assertEqual(R.watch_stats(again.recs, "gold_shut", "on", 0)["watched"], 0)
        clock.ms = GOLD_0600 + 900_000 - R.STOP_MS
        await again.tick()
        st = R.watch_stats(again.recs, "gold_shut", "on", 0)
        self.assertEqual((st["watched"], st["offered"], st["lost"], st["life"], st["won"], st["won_life"]), (2, 2, 1, 350, 1, 1_200))
        self.assertEqual([(r["open"], r["bought"]) for r in R.races(again.recs, "on", 0)], [(False, 0.0), (False, 212.0)])
        page = R.review(again.recs, again.settings, clock.ms)
        self.assertEqual([(r["slug"], r["gone_ms"]) for r in page["races"]], [(slug2, 350), (slug1, 1_200)])
        # a window nothing was seen of — a restart's, an older version's — with an order: the order's own clock is what there is
        text = R.race_text({"start_ms": GOLD_0600, "name": "黄金休市 4 档", "cents": 60, "offer_at": 0, "lead_min": None, "peak": 0.0, "cut_ms": None,
                            "gone_ms": None, "open": False, "asked": 212.0, "bought": 212.0, "orders": [
                                {"shares": 166.0, "filled": 166.0, "paper": False, "seen": None, "fresh": None, "after": None, "lag": 240, "own": 96, "trip": 350},
                                {"shares": 46.0, "filled": 46.0, "paper": False, "seen": None, "fresh": None, "after": None, "lag": 252, "own": 4_100, "trip": 366}]})
        self.assertEqual(text, "11-17 06:00 黄金休市 4 档：卖单出现的时刻没有记到（重启前的，或没看到规则价以内的卖单）；买到 212/212 份"
                               "｜第 1 张 要 166 成交 166：推送 240 ms、到达→发出 96 ms、往返 350 ms｜第 2 张 要 46 成交 46：推送 252 ms、到达→发出 4100 ms、往返 366 ms")


    async def test_the_latency_probe_reads_and_never_orders(self):
        """1.154.17 — ``python -u rules.py latency``: how far Predict is from the machine it runs on (to compare deploy regions) — on
        the bot's own request path, reads alone: a market found, its book read ten times, the push connection's ping and first
        book. A refusal (no key) is still a way there and back, and said so."""
        asked, lines, plain_get, plain_ws = [], [], m._http_get, m.ws_connect

        def get(url, payload=None, timeout=15, headers=None):
            asked.append((url, payload, dict(headers or {})))
            if "/categories/" in url:
                return json.dumps({"success": True, "data": {"slug": url.rsplit("/", 1)[1], "markets": [{"id": 4242}]}}).encode()
            return json.dumps({"success": True, "data": {"bids": [], "asks": []}}).encode()

        class Socket:
            sent: list = []

            async def ping(self):
                done = asyncio.get_running_loop().create_future()
                done.set_result(0.01)
                return done

            async def send(self, text): self.sent.append(json.loads(text))
            async def recv(self): return json.dumps({"type": "M", "topic": "predictOrderbook/4242", "data": {"version": 1, "bids": [], "asks": []}})
            async def close(self): self.sent.append("closed")

        async def connect(url, headers):
            self.assertEqual((url, headers), (m.PREDICT_WS_URL, {"x-api-key": "probe-key"}))
            return Socket()
        m._http_get, m.ws_connect = get, connect
        sleep, asyncio.sleep = asyncio.sleep, lambda s, *a: sleep(0, *a)  # (the probe's pauses between its reads)
        try:
            found = await R.latency_probe("probe-key", lines.append, rounds=4)
            self.assertEqual((len(found["reads"]), len(found["pings"]), found["ws_book"] is not None, found["refused"]), (4, 5, True, []))
            self.assertTrue(all(payload is None and headers == {"x-api-key": "probe-key"} for _, payload, headers in asked), "reads alone: no request carries a body")
            self.assertRegex(asked[0][0], r"^https://api\.predict\.fun/v1/categories/btc-updown-15m-\d+$")
            self.assertEqual({url for url, _, _ in asked[1:]}, {"https://api.predict.fun/v1/markets/4242/orderbook"})
            self.assertEqual(Socket.sent, [{"method": "subscribe", "requestId": 1, "params": ["predictOrderbook/4242"]}, "closed"])
            text = "\n".join(lines)
            self.assertIn("Predict 网络延迟探测（只读，不下单）｜区域 未知", lines[0])
            self.assertRegex(text, r"读盘口 4 次（同一条连接）：中位 \d+ ms（最快 \d+，最慢 \d+）")
            self.assertRegex(text, r"推送连接：握手 \d+ ms｜ping 来回 中位 \d+ ms（最快 \d+，最慢 \d+）｜订阅到第一份盘口 \d+ ms")

            def refuse(url, payload=None, timeout=15, headers=None):
                raise m.RemoteError("HTTP 401: authorization error")
            m._http_get, lines[:] = refuse, []
            found = await R.latency_probe("", lines.append, rounds=3)
            self.assertEqual((found["reads"], len(found["refused"])), ([], 3))
            self.assertIn("｜没有设置 API Key：可能被拒绝", lines[0])
            self.assertRegex("\n".join(lines), r"读盘口被拒绝（HTTP 401: authorization error：没带 Key 或 Key 不对），3 次被拒的应答来回 中位 \d+ ms")
        finally:
            m._http_get, m.ws_connect, asyncio.sleep = plain_get, plain_ws, sleep


    async def test_the_race_lines_of_the_first_night(self):
        """1.154.18 — the first night's /rules (11-18, the watches of 1.154.13). 01:45, the 06:45 window: 46 shares in the first
        book of a ladder that grew to 506 and stood six seconds — bought as 46, then 166 five seconds on. Around the makers'
        moment an offer of SWEEP_SHARES is a ladder's first step, whatever part of the window it is: the whole window is asked
        for at once. And the lines say what is known, no more: a watch of before 1.154.16 knows when the offers were gone, not
        when they were first taken from; an order that was never placed is not one that bought nothing."""
        env = dict(RULE_GOLD_SHARES="300", RULE_GOLD_SHUT_CENTS="60", RULE_FAST_HOURS="off", RULE_OIL="off", RULE_GOLD_OPEN_CENTS="0", RULE_GOLD_0515_CENTS="0")
        slug, moment = f"xau-updown-15m-{GOLD_0600 // 1000}", GOLD_0600 - 300 * 60_000
        sizes = lambda api: [L.from_wei(int(b["data"]["order"]["takerAmount"])) for b in api.sent]
        for at, want in ((moment + 2_359, [300.0]), (moment + 3_600_000, [46.0])):  # at the moment — and an hour after it: bought as it shows
            t, api, clock, x = taker("on", **env)
            t.verify_tick = lambda: None
            api.markets["901"] = gold_market(GOLD_0600)
            api.listed.add(slug)
            api.books["901"] = {"bids": [], "asks": [[0.50, 46]]}
            clock.ms = at
            await t.tick()
            self.assertEqual((sizes(api), t.sprinting(t.track[slug], clock.ms)), (want, want == [300.0]))
        shown = bj(2026, 11, 18, 2, 0) + 4_782
        lost = {"start_ms": bj(2026, 11, 18, 7, 0), "name": "黄金开盘第一档", "cents": 60, "offer_at": shown, "stamped": True, "lead_min": 299.9, "peak": 407.8,
                "cut_ms": None, "gone_ms": 420, "open": False, "asked": 166.0, "bought": 0.0,
                "orders": [{"shares": 166.0, "filled": 0.0, "paper": False, "seen": None, "fresh": None, "state": "final", "error": "",
                            "after": 289, "lag": 178, "own": 111, "trip": 308}]}
        self.assertEqual(R.race_text(lost), "11-18 07:00 黄金开盘第一档：≤60¢ 的卖单 02:00:04.782 出现（开场前 299.9 分钟），最多 407.8 份；0.42 秒后被吃光；"
                                            "下了 166 份，没买到｜第 1 张 要 166 成交 0：出现后 289 ms 发出、推送 178 ms、到达→发出 111 ms、往返 308 ms")
        tail = lambda **change: R.race_text({**lost, "orders": [], **change}).split("份；", 1)[1]
        self.assertEqual([tail(cut_ms=300, gone_ms=None, open=True), tail(cut_ms=300, gone_ms=None), tail(cut_ms=None, gone_ms=None, open=True),
                          tail(cut_ms=None, gone_ms=None), tail(cut_ms=300)],
                         ["0.30 秒后开始被吃、还没吃光；没下单（这一场还在看）", "0.30 秒后开始被吃、到开场前没吃光；没下单", "还没有被吃；没下单（这一场还在看）",
                          "到开场前没人吃；没下单", "0.30 秒后开始被吃、0.42 秒后被吃光；没下单"])
        # an order that never left: said so
        t, api, clock, x = taker("on", **env)
        t.recs = {slug: {"slug": slug, "mode": "on", "rule": "gold_shut", "name": "黄金 15分涨跌 06:00–06:15", "start_ms": GOLD_0600, "cents": 60, "orders": [
            {"state": "not_sent", "shares": 166.0, "filled": 0.0, "error": "Predict 接口限流冷却中（12 秒后重试）", "at": NOW}]}}
        row = R.races(t.recs, "on", 0)[0]
        self.assertEqual((row["asked"], row["bought"], row["orders"][0]["error"]), (0.0, 0.0, "Predict 接口限流冷却中（12 秒后重试）"))
        self.assertEqual(R.race_text(row), "11-17 06:00 黄金休市 4 档：卖单出现的时刻没有记到（重启前的，或没看到规则价以内的卖单）；单没下成"
                                           "｜第 1 张 要 166：没下成（Predict 接口限流冷却中（12 秒后重试））")


    async def test_off_finishes_what_is_open_and_plans_nothing(self):
        t, api, clock, x = taker("on")
        slug = f"xau-updown-15m-{GOLD_0600 // 1000}"
        api.markets["901"] = gold_market(GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        await t.tick()
        t.configure(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline"}).rules)
        clock.ms += R.FILL_READ_MS
        await t.tick()
        self.assertEqual(t.track, {})
        self.assertEqual(t.record(slug, "on")["orders"][0]["state"], "cancelling", "its order still followed to the end")
        self.assertIn("规则吃单：关", t.status())


if __name__ == "__main__":
    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(RuleTests))
    if not result.wasSuccessful(): raise SystemExit(1)
    print("RULES_OK")
