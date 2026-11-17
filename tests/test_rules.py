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


FAST = {"RULE_FAST_HOURS": "01:00-07:00", "RULE_OIL": "off", "RULE_GOLD_OPEN_CENTS": "0", "RULE_GOLD_0515_CENTS": "0"}  # the shut quarter hours alone


class RuleTests(unittest.IsolatedAsyncioTestCase):
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
        """on: a LIMIT order at the rule's price for what the book shows (capped by RULE_SHARES), its hash saved before the
        request, signed to expire before the start, not post-only; read FILL_READ_MS on, its rest withdrawn, read to
        Predict's final word; the fills counted by the positions check, the P&L by the loss stop."""
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
                             (lambda x: setattr(x.state, "day", -100.0), "已达 LIVE_MAX_DAILY_LOSS_USD"), (lambda x: setattr(x.state, "usd", 5.0), "USDT 余额")):
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
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        api.timeout, api.fill[1] = True, 30.0
        await t.tick()
        order = t.record(slug)["orders"][0]
        self.assertEqual(order["state"], "unknown")
        clock.ms += 31_000
        await t.tick()
        self.assertEqual(len(api.sent), 1, "never sent again while its fate is unknown")
        clock.ms += R.UNKNOWN_EVERY_MS
        await t.tick()
        order = t.record(slug)["orders"][0]
        self.assertEqual((order["state"], order["filled"], order["id"]), ("final", 30.0, "1"))
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
        self.assertEqual(L.from_wei(int(api.sent[0]["data"]["order"]["takerAmount"])), 25.0)
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
        api.books["901"] = {"bids": [], "asks": [[0.51, 20]]}
        await t.tick()
        self.assertEqual(R.filled(t.record(slug, "paper")), 20.0)
        t.configure(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "RULE_TAKER": "on"}).rules)
        api.fill[1], api.price = 20.0, 0.51
        clock.ms += int(R.SLOW_S * 1000)
        await t.tick()
        real = t.record(slug, "on")
        self.assertEqual((len(api.sent), real["mode"], len(real["orders"]), len(t.record(slug, "paper")["orders"])), (1, "on", 1, 1))
        self.assertEqual(set(t.recs), {slug, f"paper:{slug}"})
        clock.ms += R.FILL_READ_MS
        await t.tick()
        self.assertEqual(t.record(slug, "on")["orders"][0]["state"], "final", "followed: filled in full")
        self.assertAlmostEqual(R.filled_by_outcome(t.recs, clock.ms)[("901", 1)], 20 * (1 - R.fee_share(0.51, 200)), 6, "the real one alone")
        self.assertAlmostEqual(R.spent_on(t.recs, R.day_of(clock.ms), "on"), 20 * 0.51, 6)
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
        details = sum(1 for c in api.calls if not c.endswith("/orderbook"))
        t.stream.take("901", {"version": 3, "bids": [], "asks": [[0.52, 25]]})  # an offer at the rule's price, pushed
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[0]["data"]["order"]["takerAmount"])), 25.0)
        self.assertEqual((reads(), sum(1 for c in api.calls if not c.endswith("/orderbook"))), (1, details), "no request before the order")
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

    async def test_orders_for_several_windows_go_out_side_by_side(self):
        """1.154.12: offers showing on several windows at once — each window's order is on its way before another's answer is in
        (they went one after another, a round trip each); the day's cap still counts the first while it is on its way."""
        t, api, clock, x = taker("on", **FAST, RULE_MAX_DAY_USD="15")
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
        self.assertEqual(sizes, [5.0, 20.0], "the cap counts the first order while it flies: $15 − 20 × 58¢ leaves 5 shares for the second")
        self.assertTrue(all(o["state"] == "open" for r in t.recs.values() for o in r["orders"]), "the tick waited for both answers")

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
