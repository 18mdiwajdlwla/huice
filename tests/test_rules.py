"""规则吃单 (rules.py, 1.153.0): the rules' windows in New York time, the book's offer, paper and real takes, the gates, the orders
to Predict's final word, the settlement, and what the positions check and the loss stop count. Entirely offline."""
import asyncio
import copy
import datetime as dt
import hashlib
import json
import sys
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
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "STATE_DB": ":memory:", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64,
                             "RULE_TAKER": mode, "LIVE_MAX_ORDER_USD": "500", "LIVE_MAX_DAILY_LOSS_USD": "100", **env})
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
        keys = {"RULE_TAKER", "RULE_MAX_DAY_USD", *m.RULE_SETTING_KEYS.values(), *(k for pair in m.RULE_MARKET_KEYS.values() for k in pair)}
        self.assertTrue(keys <= m.CONTROL_KEY_SET and all(m.CONTROL_FORMS[k]["group"] == "规则" for k in keys))
        bot = types.SimpleNamespace(config=m.Config.from_env({**base, "RULE_TAKER": "paper", "RULE_OIL_SHUT_CENTS": "45"}))
        value = lambda key: m.Bot.control_value(bot, key)
        self.assertEqual((value("RULE_TAKER"), value("RULE_OIL_SHUT_CENTS"), value("RULE_GOLD_SHARES"), value("RULE_GOLD_SHUT_CENTS"), value("RULE_OIL"), value("RULE_MAX_DAY_USD")),
                         ("paper", "45", "50", "58", "on", "0"))
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
        fee = m.taker_fee(0.526, 200)
        self.assertEqual((rec["result"], round(rec["pnl"], 4)), ("up", round(50 * (1 - 0.526 - fee), 4)))
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
        self.assertEqual((order["state"], order["filled"], order["avg"]), ("final", 30.0, 0.51))
        self.assertTrue(any("🎯 规则吃单成交" in n for n in x.notes))
        self.assertEqual(R.filled_by_outcome(t.recs, clock.ms), {("901", 1): 30.0})
        self.assertEqual(R.unverified(t.recs), set())
        # the book again: 30 more at 57¢ — 10 left of the 40
        api.books["901"] = {"bids": [], "asks": [[0.57, 30]]}
        api.fill[2] = 10.0
        api.price = None
        clock.ms += 31_000
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[1]["data"]["order"]["takerAmount"])), 10.0)
        clock.ms += R.FILL_READ_MS
        await t.tick()
        rec = t.record(slug)
        self.assertEqual((R.filled(rec), round(R.cost(rec), 6)),
                         (40.0, round(30 * (0.51 + m.taker_fee(0.51, 200)) + 10 * (0.58 + m.taker_fee(0.58, 200)), 6)), "no price stated: costed at the limit")
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
        self.assertIn("盘口没有 ≤58¢ 的跌", t.track[slug]["word"])
        pair["orders"]["up"]["filled"] = 4.0  # 4 of its 10 filled: 6 of its own left at 51¢, and someone else's 5 beside them
        api.books["903"] = {"bids": [[0.49, 11]], "asks": [[0.53, 40]]}
        clock.ms += 31_000
        await t.tick()
        self.assertEqual(L.from_wei(int(api.sent[0]["data"]["order"]["takerAmount"])), 5.0)
        self.assertEqual(api.sent[0]["data"]["order"]["tokenId"], "1807", "the Down token")

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
        self.assertEqual(t.record(slug)["orders"][0]["state"], "cancelling", "its order still followed to the end")
        self.assertIn("规则吃单：关", t.status())


if __name__ == "__main__":
    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(RuleTests))
    if not result.wasSuccessful(): raise SystemExit(1)
    print("RULES_OK")
