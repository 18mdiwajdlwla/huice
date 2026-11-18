"""1.154.21 — every API key used to its pace. Predict limits each key by itself (240 requests a minute). 11-17: the bot's key
(the model trader and the orderbook feed) and the pre-open mode's ran at their pace all day — a new window's orders 13 s behind
its listing, the account snapshot 481 s a read, ninety 429s on the bot's key in 2.3 hours — while the rule taker's own key
did nothing outside its hours. Here: the rule taker says when it needs its key; outside that the feed's reads go out on it and
the pre-open mode's requests overflow onto it (one policy, a refusal pauses it); a fresh listing's orders go ahead of the
mode's low traffic, every card's first level before anyone's deeper ones; /diag says how each key stands. Entirely offline."""
import asyncio
import copy
import sys
import time
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import offline  # noqa: F401
import main as m
import live as L
import preopen as p
import test_preopen as T
import test_rules as TR

KEYS = {"PREDICT_API_KEY": "main-key", "PREOPEN_API_KEY": "pre-key", "RULE_API_KEY": "rule-key"}


def bucket(key: str, tokens: float) -> m.PredictPacer:
    """A key's pacer holding ``tokens`` now (the pace slow enough that it stays so for the test)."""
    pacer = m.predict_pacer(key)
    pacer.tokens, pacer.at = tokens, time.monotonic()
    return pacer


def rule_taker(busy):
    """A rule taker as the lending asks it: ``busy`` (a list of one) ms until it needs its key at speed."""
    return types.SimpleNamespace(alive=True, api=types.SimpleNamespace(blocked_until=0.0), now=lambda: 0, busy_in=lambda now: busy[0])


class KeyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.kept = (m.PREDICT_PACING, m.PREDICT_RATE["per_min"], dict(m.PREDICT_PACERS))
        m.PREDICT_PACING, m.PREDICT_RATE["per_min"] = True, 6.0  # a token every ten seconds: a bucket set by hand stays as set
        m.PREDICT_PACERS.clear()

    def tearDown(self):
        m.PREDICT_PACING, m.PREDICT_RATE["per_min"] = self.kept[0], self.kept[1]
        m.PREDICT_PACERS.clear()
        m.PREDICT_PACERS.update(self.kept[2])

    async def finish(self, ctl):
        for task in [*ctl.settling.values(), *([ctl.lent_warm] if ctl.lent_warm is not None else [])]:
            task.cancel()
        await asyncio.gather(*ctl.settling.values(), return_exceptions=True)
        ctl.ledger.close()

    async def test_a_keys_bucket_says_whether_a_request_would_go_now_and_who_waited(self):
        pacer = bucket("k", 8.0)
        self.assertTrue(pacer.spare() and pacer.spare(low=True) and pacer.spare(low=True, leave=2.0))
        pacer = bucket("k", 4.5)
        self.assertTrue(pacer.spare(low=True) and not pacer.spare(low=True, leave=2.0), "a snapshot page on a borrowed key leaves two more behind")
        pacer = bucket("k", 2.5)
        self.assertTrue(pacer.spare() and not pacer.spare(low=True))
        self.assertFalse(bucket("k", 0.5).spare())
        self.assertEqual(pacer.waited_last(), (0, 0.0))
        m.PREDICT_RATE["per_min"] = 6000.0  # 100 a second: the waits stay short
        fast = bucket("fast", 4.9)
        fast.per_min = 6000.0
        began = time.monotonic()
        await fast.take(low=True, leave=2.0)  # needs five in the bucket: a moment's wait
        took = time.monotonic() - began
        self.assertTrue(0.0005 <= took < 0.2, took)
        n, seconds = fast.waited_last()
        self.assertTrue(n == 1 and 0 < seconds < 0.2, (n, seconds))
        await asyncio.sleep(0.05)
        await fast.take(low=True)  # ...which a plain low read does not need
        self.assertEqual(fast.waited_last()[0], 1)
        # a client's own mark (PredictApi.leave) reaches its key's pacer; an order never leaves anything behind
        asked = []
        real = m.PredictPacer.take

        async def take(self, write=False, low=False, leave=0.0):
            asked.append((write, low, leave))
        api = L.PredictApi("https://x", "leaving", None)
        api.low, api.leave = True, p.LEND_SNAPSHOT_LEAVE

        async def send(method, path, payload, timeout):
            return b"{}"
        api.send = send
        m.PredictPacer.take = take
        try:
            await api.call("GET", "/orders")
            await api.call("POST", "/orders", {}, priority=True)
        finally:
            m.PredictPacer.take = real
        self.assertEqual(asked, [(False, True, 2.0), (True, False, 0.0)])
        m.PREDICT_PACING = False
        self.assertTrue(bucket("off", -5.0).spare(low=True, leave=2.0), "no pacing: nothing to wait for")

    async def test_the_rule_taker_says_when_it_needs_its_key(self):
        off, _, _, _ = TR.taker("off")
        self.assertIsNone(off.busy_in(TR.NOW), "switched off: never")
        # the per-second hours: 05:30 Beijing is inside 01:00–07:00
        fast, _, clock, _ = TR.taker("on", **TR.FAST)
        self.assertEqual(fast.busy_in(clock.ms), 0)
        # ...and ahead of them, with no window followed: the wait for them
        later, _, clock, _ = TR.taker("on", RULE_FAST_HOURS="08:00-09:00", RULE_GOLD="off", RULE_OIL="off")
        later.plan(clock.ms)
        self.assertEqual((later.track, later.busy_in(clock.ms)), ({}, int(2.5 * 3_600_000)))
        # gold's shut quarter hours (06:00–06:45 Beijing), no per-second hours, no makers' moment: the last quarter hour before a start
        t, _, clock, _ = TR.taker("on", **TR.IN)
        t.plan(clock.ms)
        self.assertEqual(sorted(w["start"] for w in t.track.values())[0], TR.GOLD_0600)
        self.assertEqual(t.busy_in(clock.ms), TR.GOLD_0600 - TR.R.NEAR_MS - clock.ms, "05:30: fifteen minutes to the first window's last quarter hour")
        clock.ms = TR.GOLD_0600 - 14 * 60_000
        self.assertEqual(t.busy_in(clock.ms), 0)
        last = max(w["start"] for w in t.track.values() if w["start"] < TR.GOLD_0600 + 3_600_000)  # 06:45
        clock.ms = last - TR.R.STOP_MS + 1_000  # past the last window's buying: the next thing is tomorrow's
        t.plan(clock.ms)
        self.assertGreater(t.busy_in(clock.ms), 20 * 3_600_000)
        # a rule that buys in the window's course too: the key is the taker's to the window's last seconds
        course, _, clock, _ = TR.taker("on", RULE_GOLD_SHUT_LIVE_CENTS="60", **TR.IN)
        clock.ms = TR.GOLD_0600 + 5 * 60_000
        course.plan(clock.ms)
        self.assertEqual(course.busy_in(clock.ms), 0)
        # the makers' moment (RULE_LEAD_MINUTES before a start): from a minute before it to a quarter hour after
        lead, _, clock, _ = TR.taker("on", **{**TR.IN, "RULE_LEAD_MINUTES": "300"})
        clock.ms = TR.bj(2026, 11, 17, 12, 0)  # noon: tomorrow's 06:00 window is posted at 01:00
        lead.plan(clock.ms)
        moment = TR.bj(2026, 11, 18, 6, 0) - 300 * 60_000
        self.assertEqual(lead.busy_in(clock.ms), moment - TR.R.WARM_MS - clock.ms)
        for at, busy in ((moment - 30_000, True), (moment + 10 * 60_000, True)):
            clock.ms = at
            self.assertEqual(lead.busy_in(at) == 0, busy, at - moment)

    async def test_the_key_is_lent_only_while_the_rule_taker_is_idle(self):
        config = lambda **env: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", **KEYS, **env})
        owner = types.SimpleNamespace(config=config(), rules=None)
        lend = p.KeyLending(owner)
        self.assertEqual((lend.key(), lend.why(), lend.busy_in()), ("rule-key", "", None), "no rule taker running: the key idles")
        for env, note in (({"RULE_KEY_LEND": "off"}, "switched off"), ({"RULE_API_KEY": ""}, "no key of its own"), ({"RULE_API_KEY": "main-key"}, "the bot's own key"),
                          ({"RULE_API_KEY": "pre-key"}, "the pre-open mode's key"), ({"PREDICT_BOOK_API_KEY": "rule-key"}, "the feed's own key")):
            other = p.KeyLending(types.SimpleNamespace(config=config(**env), rules=None))
            self.assertEqual((other.key(), other.why()), ("", "没有可借的 Key"), note)
        self.assertTrue(config().rule_key_lend and not config(RULE_KEY_LEND="off").rule_key_lend)
        busy = [None]
        owner.rules = rule_taker(busy)
        self.assertEqual(lend.why(), "", "nothing ahead for the rule taker")
        for ms, why in ((20 * 60_000, ""), (p.LEND_BEFORE_MS + 1, ""), (p.LEND_BEFORE_MS, "规则吃单 5 分钟后要用"), (61_000, "规则吃单 2 分钟后要用"),
                        (1, "规则吃单 1 分钟后要用"), (0, "规则吃单正在用")):
            busy[0] = ms
            self.assertEqual(lend.why(), why, ms)
        busy[0] = 14 * 60_000  # a read that takes ten minutes more than the margin does not start now
        self.assertEqual((lend.why(), lend.why(10 * 60_000)), ("", "规则吃单 14 分钟后要用"))
        busy[0] = None
        owner.rules.api.blocked_until = time.monotonic() + 15  # the rule taker's own 429: its cooldown is everyone's on the key
        self.assertEqual((lend.why(), lend.blocked_until), ("那把 Key 在限流冷却中", owner.rules.api.blocked_until))
        owner.rules.api.blocked_until = 0.0
        lend.blocked_until = time.monotonic() + 9  # a borrower's 429 pauses the rule taker's client too: one key, one limit
        self.assertGreater(owner.rules.api.blocked_until, time.monotonic() + 8)
        lend.blocked_until = time.monotonic() + 1  # (never shortened)
        self.assertGreater(owner.rules.api.blocked_until, time.monotonic() + 8)
        owner.rules.api.blocked_until = 0.0
        lend.off("那把 Key 被限流（429）")
        self.assertEqual(lend.why(), "那把 Key 被限流（429），10 分钟内不借")
        lend.off_until = 0.0
        until = time.monotonic() + 20
        lend.refused(until, True)  # the feed's read answered 429 on the borrowed key
        self.assertEqual((owner.rules.api.blocked_until, lend.off_why), (until, "那把 Key 被限流（429）"))
        lend.off_until, owner.rules.api.blocked_until = 0.0, 0.0
        owner.rules.alive = False  # a taker that stopped needs nothing
        busy[0] = 0
        self.assertEqual(lend.why(), "")

    async def test_the_preopen_mode_overflows_onto_the_idle_key(self):
        ctl, api, clock = T.controller(**KEYS)
        self.assertTrue(ctl.lent_made is not None and ctl.lent_made.api_key == "rule-key" and ctl.lent_made.low and ctl.lent_made.patient)
        self.assertEqual((ctl.lent_snap_api.api_key, ctl.lent_snap_api.leave, ctl.lent_snap_api.low), ("rule-key", p.LEND_SNAPSHOT_LEAVE, True))
        self.assertTrue(ctl.first_api.api_key == "pre-key" and ctl.first_api.patient and not ctl.first_api.low, "a fresh listing's client: the mode's key, not low")
        self.assertEqual(ctl.borrow_why(), "没有可借的 Key", "the tests' own api in: the real client on the rule key is not used")
        self.assertIs(ctl.first(), api)
        await self.finish(ctl)
        none, _, _ = T.controller(PREDICT_API_KEY="main-key", PREOPEN_API_KEY="pre-key")
        self.assertTrue(none.lent_api is None and none.borrow_line() == "" and none.lane(none.api) is none.api)
        await self.finish(none)
        same, _, _ = T.controller(PREDICT_API_KEY="main-key", PREOPEN_API_KEY="rule-key", RULE_API_KEY="rule-key")
        self.assertIsNone(same.lent_api, "the rule taker's key is this mode's own: nothing to borrow")
        await self.finish(same)

        ctl, api, clock = T.controller(**KEYS)
        busy = [None]
        ctl.owner.rules = rule_taker(busy)
        api.api_key, api.low = "pre-key", True
        lent = T.Api(ctl, list(api.markets.values()))
        lent.api_key, lent.low, lent.jwt = "rule-key", True, "t"
        ctl.lent_api = lent
        first = T.Api(ctl, list(api.markets.values()))
        first.api_key, first.low = "pre-key", False
        # the lane: the mode's key when it has a token for the request now; else the idle key when that has one to spare
        bucket("pre-key", 8.0), bucket("rule-key", 8.0)
        self.assertIs(ctl.lane(api), api)
        bucket("pre-key", 2.5)  # a low request needs three: short
        self.assertIs(ctl.lane(api), lent)
        self.assertIs(ctl.lane(first), first, "a fresh listing's request needs one: it has it")
        self.assertEqual(ctl.lent_last(), 1)
        bucket("pre-key", 0.2)
        self.assertIs(ctl.lane(first), lent)
        bucket("rule-key", 2.5)
        self.assertIs(ctl.lane(api), api, "both short: it waits on its own key, as ever")
        bucket("rule-key", 8.0)
        for ms, lends in ((p.LEND_BEFORE_MS + 1, True), (p.LEND_BEFORE_MS, False), (0, False), (None, True)):
            busy[0] = ms
            self.assertIs(ctl.lane(api), lent if lends else api, ms)
        lent.jwt = ""
        self.assertEqual((ctl.borrow_why(), ctl.lane(api)), ("那把 Key 还在登录", api))
        signed = lent.ensure_auth

        async def no_sign_in():
            raise m.RemoteError("HTTP 503: sign-in down")
        lent.ensure_auth = no_sign_in  # a sign-in that fails is this mode's own trouble: tried again a minute on, the lending itself not paused
        ctl.lent_keep_warm()
        await ctl.lent_warm
        self.assertEqual((ctl.borrow_why(), ctl.lending.why()), ("那把 Key 还在登录（上次没登上：HTTP 503: sign-in down）", ""))
        task = ctl.lent_warm
        ctl.lent_keep_warm()
        self.assertIs(ctl.lent_warm, task, "not again within the minute")
        lent.ensure_auth, ctl.lent_warmed = signed, -1e9
        ctl.lent_keep_warm()  # ...signed in beside the loop while it may be borrowed
        self.assertIsNot(ctl.lent_warm, task)
        await ctl.lent_warm
        self.assertEqual(ctl.lent_warm_error, "")
        lent.jwt = "t"
        lent.blocked_until = time.monotonic() + 5
        self.assertEqual((ctl.borrow_why(), ctl.lane(api)), ("那把 Key 在限流冷却中", api))
        lent.blocked_until = 0.0
        self.assertIn("规则吃单的 Key 空闲时借来用（RULE_KEY_LEND）：现在在借，最近一分钟走了", ctl.status())
        busy[0] = 0
        self.assertIn("规则吃单的 Key 空闲时借来用（RULE_KEY_LEND）：现在不借——规则吃单正在用，", ctl.status())
        busy[0] = None

        # through: the borrowed key's refusal — nothing done with the request — sends it on the mode's own, and pauses the borrowing
        seen = []

        async def answer(client, error=None):
            seen.append(client)
            if error is not None and client is lent:
                raise error
            return "ok"
        for error, why in ((m.RemoteError("HTTP 429: Too Many Requests"), "那把 Key 被限流（429）"), (m.RemoteError("HTTP 401: unauthorized"), "那把 Key 拒绝了请求（HTTP 401）"),
                           (m.RemoteError("Predict 接口限流冷却中（3 秒后重试）", 3), "那把 Key 被限流（429）")):
            seen.clear()
            self.assertEqual(await ctl.through(api, lambda client: answer(client, error)), "ok")
            self.assertEqual((seen, ctl.lending.off_why), ([lent, api], why))
            self.assertIs(ctl.lane(api), api, "paused: nothing more goes out there for ten minutes")
            self.assertIn("分钟内不借", ctl.borrow_why())
            ctl.lending.off_until = 0.0
        seen.clear()
        with self.assertRaises(m.RemoteError):  # a timeout is the request's own: never sent again (an order may be on the book)
            await ctl.through(api, lambda client: answer(client, m.RemoteError("timeout: delivery unknown")))
        self.assertEqual((seen, ctl.lending.why()), ([lent], ""))

        # a pair's orders go out on the idle key while the mode's own has no token; the same signed orders, the same record
        c = p.candidate(T.market(), T.NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair({**c, "fresh": False})
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual((len(lent.sent), len(api.sent), pair["phase"], sorted(o["state"] for o in pair["orders"].values())), (2, 0, "active", ["open", "open"]))
        self.assertEqual({o["hash"] for o in pair["orders"].values()}, {b["data"]["order"]["hash"].lower() for b in lent.sent})
        bucket("pre-key", 8.0)  # the mode's key has tokens again: on it
        again = p.candidate(T.market(at=T.NOW + 2 * 900_000 + 3600_000, mid=124), T.NOW, ctl.owner.config, dataclasses_ahead(ctl))
        api.markets["124"] = lent.markets["124"] = T.market(at=T.NOW + 2 * 900_000 + 3600_000, mid=124)
        ctl.settings = dataclasses_ahead(ctl)
        await ctl.place_pair(again)
        self.assertEqual((len(lent.sent), len(api.sent)), (2, 2))
        # ...and the idle key refusing an order (its limit): the order goes out on the mode's own, the pair is placed whole
        bucket("pre-key", 0.2)
        third = p.candidate(T.market(at=T.NOW + 4 * 900_000 + 3600_000, mid=125), T.NOW, ctl.owner.config, ctl.settings)
        api.markets["125"] = lent.markets["125"] = T.market(at=T.NOW + 4 * 900_000 + 3600_000, mid=125)
        real_create = lent.create_order

        async def limited(body):
            raise m.RemoteError("HTTP 429: Too Many Requests")
        lent.create_order = limited
        await ctl.place_pair(third)
        self.assertEqual((len(api.sent), ctl.ledger.rows[third["slug"]]["phase"], ctl.lending.off_why), (4, "active", "那把 Key 被限流（429）"))
        lent.create_order = real_create
        ctl.lending.off_until = 0.0

        # the account snapshot: read on the idle key (behind what else is borrowed there) when the rule taker leaves it long enough
        ctl.account_pages = 242  # 121 s of pages, a minute's margin, and the five minutes
        need = p.LEND_BEFORE_MS + int(242 * p.LEND_PAGE_S * 1000) + 60_000
        for ms, there in ((need + 1, True), (need, False), (None, True)):
            busy[0] = ms
            self.assertIs(ctl.snapshot_lane(), lent if there else api, ms)
        lent.rows.update(copy.deepcopy(api.rows))  # (one account behind both clients)
        before = ctl.lent_last()
        await ctl.account()
        self.assertEqual(sorted(r["id"] for r in ctl.account_rows), sorted(r["id"] for r in lent.rows.values() if r["status"] == "OPEN"))
        self.assertGreater(ctl.lent_last(), before)
        real_orders = lent.orders
        # its pages yield to what else is borrowed there (leave) — for half the read's own pace at the most, then beside it: a
        # backlog borrowing without end never keeps the snapshot from finishing; the mark is back for the next read
        lent.leave, seen_leave = p.LEND_SNAPSHOT_LEAVE, []

        async def slow_orders(status="OPEN"):
            seen_leave.append(lent.leave)
            await asyncio.sleep(0.08)
            seen_leave.append(lent.leave)
            return await real_orders(status)
        lent.orders = slow_orders
        kept_min, p.LEND_YIELD_MIN_S, ctl.account_pages = p.LEND_YIELD_MIN_S, 0.02, 0
        try:
            await ctl.account()
        finally:
            p.LEND_YIELD_MIN_S = kept_min
        self.assertEqual((seen_leave, lent.leave), ([p.LEND_SNAPSHOT_LEAVE, 0.0], p.LEND_SNAPSHOT_LEAVE))
        ctl.account_pages = 242

        async def no_orders(status="OPEN"):
            raise m.RemoteError("HTTP 429: Too Many Requests")
        lent.orders = no_orders  # the idle key refuses the read: the whole read on the mode's own key, the borrowing paused
        await ctl.account()
        self.assertTrue(ctl.account_at and "分钟内不借" in ctl.borrow_why())
        lent.orders = real_orders
        await self.finish(ctl)

    async def test_a_listing_goes_out_level_by_level_ahead_of_the_low_traffic(self):
        ctl, api, clock = T.controller(PREOPEN_MARKETS="btc,eth", PREOPEN_AHEAD_MINUTES="1440", PREOPEN_LEVELS="46x20,45x20")
        at, later = T.NOW + 20 * 3_600_000, T.NOW + 20 * 3_600_000 + 900_000
        made = lambda stem, level, start, fresh: {"slug": f"{stem}-updown-15m-{start // 1000}" + (f"#{level}" if level else ""), "start_ms": start, "level": level, "fresh": fresh}
        cands = [made(stem, level, start, True) for stem in ("btc", "eth") for level in (0, 1, 2) for start in (at, later)]
        cands += [made("btc", 0, T.NOW + 3_600_000, False)]  # the backlog: a window listed long ago, its start the soonest
        order = [(c["slug"].split("-")[0], c["level"], c["start_ms"] == later, c["fresh"]) for c in sorted(cands, key=lambda c: (p.queue_rank(c), c["slug"]))]
        self.assertEqual(order, [("btc", 0, False, True), ("eth", 0, False, True), ("btc", 1, False, True), ("eth", 1, False, True), ("btc", 2, False, True),
                                 ("eth", 2, False, True),  # the listing's windows of one start: every card's first level, then the second…
                                 ("btc", 0, True, True), ("eth", 0, True, True), ("btc", 1, True, True), ("eth", 1, True, True), ("btc", 2, True, True),
                                 ("eth", 2, True, True),  # ...a later start after them, level by level too
                                 ("btc", 0, False, False)])  # ...and the backlog behind every fresh one
        # a fresh listing's orders on the mode's key ahead of its low traffic (first); the backlog's on the low client, as before
        fast = T.Api(ctl, list(api.markets.values()))
        ctl.first = lambda: fast
        c = p.candidate(T.market(), T.NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair({**c, "fresh": True})
        self.assertEqual((len(fast.sent), len(api.sent)), (2, 0))
        api.markets["124"] = T.market(at=T.NOW + 2 * 3_600_000, mid=124)
        old = p.candidate(api.markets["124"], T.NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair({**old, "fresh": False})
        self.assertEqual((len(fast.sent), len(api.sent)), (2, 2))
        await self.finish(ctl)

    async def test_the_feeds_reads_go_out_on_the_idle_key(self):
        config = lambda **env: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", **KEYS, **env})
        feed = m.PredictFeed(config())
        why = [""]
        refused = []
        feed.lend = types.SimpleNamespace(key=lambda: "rule-key", why=lambda: why[0], refused=lambda until, limited=True: refused.append((until, limited)))
        sent = []
        fail = [None]

        def fake_get(url, payload=None, timeout=15, headers=None):
            sent.append((headers or {}).get("x-api-key"))
            if fail[0] is not None and sent[-1] == fail[0][0]:
                raise fail[0][1]
            return b"{}"
        real_get, m._http_get = m._http_get, fake_get
        try:
            url = m.PREDICT_REST + "/markets/1/orderbook"
            bucket("main-key", 8.0), bucket("rule-key", 8.0)
            await feed.fetch(url)
            self.assertEqual((sent, feed.lent_last()), (["rule-key"], 1), "the rule taker idle, its key with tokens to spare: the read goes out there")
            bucket("rule-key", 2.5)  # nothing to spare there (a low read leaves two behind): the bot's own key
            await feed.fetch(url)
            bucket("rule-key", 8.0)
            why[0] = "规则吃单正在用"
            await feed.fetch(url)
            self.assertEqual((sent, feed.lent_last()), (["rule-key", "main-key", "main-key"], 1))
            why[0] = ""
            # the borrowed key's 429 is not the feed's: the rule taker's client and the borrowing pause, and the read is asked once
            # more on the feed's own key — answered there
            sent.clear()
            fail[0] = ("rule-key", m.RemoteError("HTTP 429: Too Many Requests"))
            self.assertEqual(await feed.fetch(url), {})
            self.assertEqual(sent, ["rule-key", "main-key"])
            self.assertTrue(len(refused) == 1 and refused[0][1] is True and refused[0][0] > time.monotonic() + m.PREDICT_BACKOFF_SECONDS - 2)
            self.assertEqual((feed.blocked_until, len(feed.limited), len(m.predict_pacer("rule-key").limits), len(m.predict_pacer("main-key").limits)), (0.0, 0, 1, 0))
            offs = []
            feed.lend.off = lambda text: offs.append(text)
            fail[0] = ("rule-key", m.RemoteError("HTTP 401: invalid api key"))  # a key that is refused outright: the same, without a cooldown
            self.assertEqual(await feed.fetch(url), {})
            self.assertEqual((offs, len(refused), feed.blocked_until), (["那把 Key 拒绝了请求（HTTP 401）"], 1, 0.0))
            fail[0] = ("rule-key", m.RemoteError("HTTP 404: Orderbook not found"))  # no book is an answer about the market, on any key
            with self.assertRaises(m.RemoteError):
                await feed.fetch(url)
            self.assertEqual((sent[-1], offs, len(refused)), ("rule-key", ["那把 Key 拒绝了请求（HTTP 401）"], 1))
            why[0] = "那把 Key 被限流（429），10 分钟内不借"
            fail[0] = ("main-key", m.RemoteError("HTTP 429: Too Many Requests"))
            with self.assertRaises(m.RemoteError):  # ...its own key's 429 is the feed's, as ever
                await feed.fetch(url)
            self.assertTrue(feed.blocked_until > time.monotonic() and len(feed.limited) == 1 and len(m.predict_pacer("main-key").limits) == 1)
            fail[0], feed.blocked_until, why[0] = None, 0.0, ""
            # a feed with a key of its own borrows nothing (the bot's key carries none of its reads anyway); nor one with no lender
            own = m.PredictFeed(config(PREDICT_BOOK_API_KEY="book-key"))
            own.lend = feed.lend
            bucket("book-key", 8.0)
            sent.clear()
            await own.fetch(url)
            feed.lend = None
            await feed.fetch(url)
            self.assertEqual(sent, ["book-key", "main-key"])
        finally:
            m._http_get = real_get

    async def test_diag_says_how_hard_each_key_is_pressed(self):
        cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "LIVE": "on", "PREDICT_PRIVATE_KEY": "0x" + "a" * 64, **KEYS})
        bot = L.LiveBot(cfg, m.Store(":memory:"), types.SimpleNamespace(now_ms=lambda: T.NOW, config=None), None)
        self.assertTrue(isinstance(bot.key_lending, p.KeyLending) and bot.predict.lend is bot.key_lending, "one policy: the feed's and the pre-open mode's")
        now = time.monotonic()
        for key, n, waits in (("main-key", 118, [(now, 0.4), (now, 0.2)]), ("pre-key", 219, [(now, 1.5)] * 40), ("rule-key", 150, [])):
            pacer = m.predict_pacer(key)
            pacer.sent.extend([now] * n)
            pacer.waits.extend(waits)
        bot.predict.lent_at.extend([now] * 90)
        bot.preopen = types.SimpleNamespace(lent_last=lambda: 55)
        line = bot.live_key_load()
        self.assertIn("主 Key 118 次（等节拍 2 次共 1 秒）", line)
        self.assertIn("预挂专用 Key 219 次（等节拍 40 次共 60 秒）", line)
        self.assertIn("规则吃单专用 Key 150 次（其中盘口借用 90 次、预挂借用 55 次；空闲，借给盘口和预挂用）", line)
        self.assertIn("“等节拍”= 这把 Key 已经用满、请求在排队", line)
        bot.rules = rule_taker([0])
        self.assertIn("规则吃单专用 Key 150 次（其中盘口借用 90 次、预挂借用 55 次；现在不借——规则吃单正在用）", bot.live_key_load())
        off = L.LiveBot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "LIVE": "on", "PREDICT_PRIVATE_KEY": "0x" + "a" * 64, **KEYS, "RULE_KEY_LEND": "off"}),
                        m.Store(":memory:"), types.SimpleNamespace(now_ms=lambda: T.NOW, config=None), None)
        self.assertIn("规则吃单专用 Key 150 次（节流每分钟", off.live_key_load())  # the lending off: the key is the rule taker's alone, nothing said of it
        self.assertIsNone(off.predict.lent_key() or None)


def dataclasses_ahead(ctl):
    """The controller's settings with a day's horizon (the windows these tests place start hours on)."""
    import dataclasses
    return dataclasses.replace(ctl.settings, ahead_minutes=1440, deep_ahead_minutes=1440)


if __name__ == "__main__":
    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(KeyTests))
    if not result.wasSuccessful(): raise SystemExit(1)
    print("KEYS_OK")
