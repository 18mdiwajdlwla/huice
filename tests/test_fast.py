"""1.154.60 — the pre-open mode at Predict's real limits. 11-26 the user sent the Rate limits page of Predict's developer
console: the limits are an app's — order placements and cancels (Trading) counted apart from every other request (General),
thousands of each a minute by the account's volume, 240 a second the ceiling on orders — where the bot paced each key to 220
a minute of both together ("撤单后，重新挂满要多久……我看这里写的是每秒 240").

Here: the two limits as settings and the paces they give the mode's key (a share of each, less what the bot's other keys may
send); the key's orders and cancels on a bucket of their own; what follows the paces inside the mode — pairs placed side by
side, the clients' gates, withdrawn pairs verified side by side with both orders read together and one save, known windows
re-read in groups, one read of a market's details for the levels that ask together, the snapshot's pace; nothing of it
without the limits; the status, /diag, the speed test, the control page. Entirely offline.

Run: python tests/test_fast.py
"""
import asyncio
import dataclasses
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

BASE = {"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on"}
KEYS = {"PREDICT_API_KEY": "main-key", "PREOPEN_API_KEY": "pre-key", "RULE_API_KEY": "rule-key"}
BIG = {"PREOPEN_APP_TRADING_PER_MIN": "5000", "PREOPEN_APP_GENERAL_PER_MIN": "5000"}      # the $50K tier's page
SMALL = {"PREOPEN_APP_TRADING_PER_MIN": "2000", "PREOPEN_APP_GENERAL_PER_MIN": "1000"}    # the $15K tier's
MIN = 60_000


def paces(**env) -> tuple[float, float]:
    return m.preopen_paces(m.Config.from_env({**BASE, **env}))


class Fast(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.kept = (m.PREDICT_PACING, m.PREDICT_RATE["per_min"], dict(m.PREDICT_PACERS), dict(m.PREDICT_RATE_OF), dict(m.PREDICT_TRADE_OF), dict(m.PREDICT_TRADE_PACERS))
        m.PREDICT_PACERS.clear(); m.PREDICT_TRADE_PACERS.clear(); m.PREDICT_RATE_OF.clear(); m.PREDICT_TRADE_OF.clear(); m.PREOPEN_CUTS.clear()

    def tearDown(self):
        m.PREOPEN_CUTS.clear()
        m.PREDICT_PACING, m.PREDICT_RATE["per_min"] = self.kept[0], self.kept[1]
        for table, was in ((m.PREDICT_PACERS, self.kept[2]), (m.PREDICT_RATE_OF, self.kept[3]), (m.PREDICT_TRADE_OF, self.kept[4]), (m.PREDICT_TRADE_PACERS, self.kept[5])):
            table.clear(); table.update(was)

    async def finish(self, ctl):
        tasks = [*ctl.settling.values(), *(t for t in (ctl.lent_warm, ctl.more_warm) if t is not None), *ctl.detail_reads.values()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        ctl.ledger.close()

    def test_the_limits_and_the_paces_they_give(self):
        config = m.Config.from_env({**BASE, **KEYS})
        self.assertEqual((config.preopen.app_trading, config.preopen.app_general, m.preopen_paces(config)), (0, 0, (0.0, 0.0)))
        # whole numbers, each 0 or 240 to 100,000
        for env in ({**BIG, "PREOPEN_APP_GENERAL_PER_MIN": "239"}, {**BIG, "PREOPEN_APP_TRADING_PER_MIN": "100001"}, {**BIG, "PREOPEN_APP_TRADING_PER_MIN": "-5"},
                    {"PREOPEN_APP_TRADING_PER_MIN": "100"}):
            with self.assertRaisesRegex(ValueError, "要么填 0"):
                m.Config.from_env({**BASE, **KEYS, **env})
        # 1.154.62: one alone is kept (the control page saves one setting at a time) — and nothing is in force until both are
        for env in ({"PREOPEN_APP_TRADING_PER_MIN": "5000"}, {"PREOPEN_APP_GENERAL_PER_MIN": "5000"}):
            half = m.Config.from_env({**BASE, **KEYS, **env})
            self.assertEqual((half.preopen.app_trading + half.preopen.app_general, m.preopen_paces(half)), (5000, (0.0, 0.0)))
        with self.assertRaisesRegex(ValueError, "必须是整数"):
            m.Config.from_env({**BASE, **KEYS, **BIG, "PREOPEN_APP_GENERAL_PER_MIN": "5k"})
        # the $50K tier (5,000 and 5,000) with the bot's key and the rule taker's beside the mode's: 80% of each limit, less 120
        # of the Trading bucket a minute for each other key (1.154.63: its order reads too — 60, orders and cancels alone, before),
        # and each other key's whole pace (220) off the General one
        self.assertEqual(paces(**KEYS, **BIG), (4000 - 2 * 120, 4000 - 2 * 220))
        self.assertEqual(paces(**KEYS, **BIG), (3760.0, 3560.0))
        self.assertEqual(paces(**KEYS, **BIG, PREDICT_BOOK_API_KEY="book-key"), (3640.0, 3340.0), "the orderbook feed on a key of its own: a third to leave room for")
        self.assertEqual(paces(PREOPEN_API_KEY="pre-key", **BIG), (4000.0, 4000.0), "no other key: the whole share")
        # the $15K tier (2,000 and 1,000): General is what is short — and it now holds no order read (1.154.63)
        self.assertEqual(paces(**KEYS, **SMALL), (1360.0, 360.0))
        # never under the pace the key has without them
        self.assertEqual(paces(**KEYS, PREDICT_BOOK_API_KEY="book-key", PREOPEN_APP_TRADING_PER_MIN="1000", PREOPEN_APP_GENERAL_PER_MIN="1000"), (440.0, 220.0))
        self.assertEqual(paces(**KEYS, **SMALL, PREOPEN_RATE_PER_MIN="500"), (1360.0, 500.0))
        # the others not paced at all (PREDICT_RATE_PER_MIN=0): reckoned at the usual pace
        self.assertEqual(paces(**KEYS, **BIG, PREDICT_RATE_PER_MIN="0"), (3760.0, 3560.0))
        # a further key of the mode's (PREOPEN_API_KEYS) may send its whole pace of either kind
        self.assertEqual(paces(**KEYS, **BIG, PREOPEN_API_KEYS="k2"), (3760.0 - 220, 3560.0 - 220))
        # only a key that is the mode's alone
        self.assertEqual(paces(PREDICT_API_KEY="main-key", **BIG), (0.0, 0.0))
        self.assertEqual(paces(PREDICT_API_KEY="main-key", PREOPEN_API_KEY="main-key", **BIG), (0.0, 0.0))

    def test_the_keys_orders_have_a_bucket_of_their_own(self):
        # Predict's Trading bucket: orders, cancels — and the order reads (1.154.63: 11-26 16:29–16:30 the answers to GET /orders
        # said 2,000 a minute with the orders' own, the markets', categories' and the sign-in's 1,000)
        for method, path in (("POST", "/orders"), ("POST", "/orders/remove"), ("POST", "/orders/"), ("GET", "/orders"), ("GET", "/orders/0xabc"),
                             ("GET", "/orders?status=OPEN&first=100"), ("POST", "/orders/0xabc")):
            self.assertTrue(m.predict_trading(method, path), (method, path))
        for method, path in (("POST", "/auth"), ("GET", "/markets/1"), ("GET", "/categories/x"), ("GET", "/positions"), ("GET", "/ordersx"), ("GET", "/markets/1/orderbook")):
            self.assertFalse(m.predict_trading(method, path), (method, path))
        self.assertEqual([m.predict_kind(meth, pth) for meth, pth in (("GET", "/v1/orders/0xab"), ("POST", "/v1/orders"), ("GET", "/v1/markets/3"))], ["trade", "trade", "read"])
        config = m.Config.from_env({**BASE, **KEYS, **BIG})
        m.set_predict_rates(config)
        self.assertEqual((m.PREDICT_RATE_OF, m.PREDICT_TRADE_OF), ({"pre-key": 3560.0}, {"pre-key": 3760.0}))
        reads, orders = m.predict_pacer("pre-key"), m.predict_pacer("pre-key", True)
        self.assertIsNot(reads, orders)
        self.assertEqual((reads.per_min, orders.per_min), (3560.0, 3760.0))
        self.assertIs(m.predict_pacer("pre-key", True), orders)
        # every other key: one bucket for both, as ever
        for key in ("main-key", "rule-key"):
            self.assertIs(m.predict_pacer(key, True), m.predict_pacer(key))
            self.assertEqual(m.predict_pacer(key).per_min, 220.0)
        # the limits taken away again (the control page): one bucket again, at the pace the key has without them
        m.set_predict_rates(m.Config.from_env({**BASE, **KEYS, "PREOPEN_RATE_PER_MIN": "300"}))
        self.assertEqual((m.PREDICT_TRADE_OF, m.PREDICT_RATE_OF), ({}, {"pre-key": 300.0}))
        self.assertIs(m.predict_pacer("pre-key", True), m.predict_pacer("pre-key"))
        self.assertEqual(m.predict_pacer("pre-key").per_min, 300.0)

    async def test_the_client_takes_each_request_from_its_bucket(self):
        m.PREDICT_PACING = True
        m.set_predict_rates(m.Config.from_env({**BASE, **KEYS, **BIG}))
        sent = []

        async def send(self, method, path, payload, timeout):
            sent.append((method, path))
            return b'{"success": true}'
        real, L.PredictApi.send = L.PredictApi.send, send
        try:
            for key in ("pre-key", "main-key"):
                client = L.PredictApi("https://x", key, None)
                for method, path, priority in (("GET", "/markets/1", False), ("POST", "/orders", True), ("POST", "/orders/remove", True), ("POST", "/auth", False),
                                               ("GET", "/orders/0xabc", False)):
                    await client.call(method, path, {} if method == "POST" else None, priority=priority)
            self.assertEqual((m.predict_pacer("pre-key").last_minute(), m.predict_pacer("pre-key", True).last_minute()), (2, 3), "its orders, cancels and order reads apart")
            self.assertEqual(m.predict_pacer("main-key").last_minute(), 5, "another key: all of them in its one bucket")
        finally:
            L.PredictApi.send = real
        # a 429 is noted on the bucket the request was of (the real send: the request itself refused)
        async def refused(fn, *args, pool=None):
            raise m.RemoteError("HTTP 429: Too Many Requests")
        kept, m.predict_blocking = m.predict_blocking, refused
        try:
            client = L.PredictApi("https://x", "pre-key", None)
            with self.assertRaises(m.RemoteError):
                await client.call("POST", "/orders", {}, priority=True)
            self.assertEqual((len(m.predict_pacer("pre-key", True).limits), len(m.predict_pacer("pre-key").limits)), (1, 0))
            client.blocked_until = 0.0
            with self.assertRaises(m.RemoteError):
                await client.call("GET", "/markets/1")
            self.assertEqual((len(m.predict_pacer("pre-key", True).limits), len(m.predict_pacer("pre-key").limits)), (1, 1))
        finally:
            m.predict_blocking = kept

    async def test_what_follows_the_paces_inside_the_mode(self):
        ctl, api, clock = T.controller(**KEYS, **BIG)
        self.assertTrue(ctl.fast())
        self.assertEqual((ctl.trade_pace, ctl.read_pace), (3760.0, 3560.0))
        self.assertEqual(ctl.place_workers(), 19, "a pair side by side for every 200 of the Trading bucket a minute")
        self.assertEqual((ctl.fast_reads(), ctl.fast_verify()), (29, 16), "(the final reads are order reads: by the Trading pace, 1.154.63)")
        self.assertEqual(ctl.reconcile_gate._value, 16)
        for client in (ctl.api_made, ctl.first_api):  # the mode's low client and the fresh listings': as many reads in flight, and two orders a pair beside them
            self.assertEqual((client.read_gate._value, client.gate._value), (29, 29 + 38), client)
        self.assertEqual(ctl.cancel_made.gate._value, L.PredictApi.PARALLEL, "the cancels' client as it was")
        self.assertEqual(ctl.reread_batch(), int(3560 / 60 * 5 * 0.25))
        self.assertEqual((ctl.reread_batch(), ctl.reread_queue_max()), (74, 760))
        self.assertAlmostEqual(ctl.account_page_s(), 0.3)
        ctl.account_pages = 242
        self.assertAlmostEqual(ctl.account_interval_s(), 72.6, places=1)  # (24,000 open orders: every 73 s, not every six minutes)
        began = time.monotonic()
        ctl.placing.add("x"); ctl.rereading = True
        await ctl.behind_placements()
        self.assertLess(time.monotonic() - began, 0.05, "the orders have a bucket of their own: a late final read stands behind nothing")
        await self.finish(ctl)
        # the $15K tier: the orders, the final reads and the snapshot by the Trading pace (1,360); the re-reads of the listings by the
        # General one (360), as before
        small, _, _ = T.controller(**KEYS, **SMALL)
        self.assertEqual((small.place_workers(), small.fast_reads(), small.fast_verify(), small.reread_batch(), small.reread_queue_max()), (7, 11, 6, 8, 280))
        self.assertAlmostEqual(small.account_page_s(), 0.3)
        await self.finish(small)
        # the limits not given — or the mode on the bot's own key: everything as it was
        for env in (KEYS, {"PREDICT_API_KEY": "main-key", **BIG}):
            plain, plain_api, _ = T.controller(**env)
            self.assertFalse(plain.fast())
            self.assertEqual((plain.place_workers(), plain.fast_reads(), plain.fast_verify(), plain.reread_batch(), plain.reread_queue_max(), plain.account_page_s()),
                             (p.PLACE_WORKERS, 3, 2, p.DISCOVER_READS, p.REREAD_QUEUE_MAX, p.ACCOUNT_PAGE_S))
            self.assertEqual((plain.reconcile_gate._value, plain.api_made.read_gate._value, plain.api_made.gate._value), (2, 3, L.PredictApi.PARALLEL))
            self.assertFalse(plain.finals_only({"phase": "closing", "start_ms": T.NOW + 3_600_000, "orders": {}}))
            await self.finish(plain)

    async def withdrawn(self, **env):
        """A pair placed and withdrawn, its orders off the book and their final reads due: (ctl, api, clock, pair)."""
        ctl, api, clock = T.controller(**env)
        c = p.candidate(T.market(), T.NOW, ctl.owner.config, ctl.settings)
        self.assertEqual(await ctl.place_pair({**c, "market": T.market(), "read_ms": T.NOW}), "")
        pair = ctl.ledger.rows[c["slug"]]
        await ctl.cancel_many([pair], "设置不同，撤掉后重新报价", free=True)
        self.assertEqual({o["state"] for o in pair["orders"].values()}, {"removed"})
        return ctl, api, clock, pair

    async def test_a_withdrawn_pairs_orders_are_read_together_and_saved_once(self):
        outcomes = {}
        for name, env, filled in (("fast", {**KEYS, **BIG}, None), ("slow", KEYS, None), ("fast filled", {**KEYS, **BIG}, "up"), ("slow filled", KEYS, "up")):
            ctl, api, clock, pair = await self.withdrawn(**env)
            self.assertFalse(ctl.finals_only(pair), "not before a read of its records can end them (a minute after they left the book)")
            clock.ms += p.LATE_FILL_MS
            self.assertTrue(ctl.finals_only(pair))
            self.assertEqual(pair["phase"], "closed", "nothing of it left to cancel: closed, its final reads still to make")
            if filled:  # Predict's record of one order shows a late fill of half of it
                row = api.rows[pair["orders"][filled]["hash"]]
                row["amountFilled"] = str(int(row["amount"]) // 2)
            flying, most, saves = [0], [0], [0]
            read, save = api.order, ctl.ledger.save

            async def order(oid, digest="", read=read):
                flying[0] += 1
                most[0] = max(most[0], flying[0])
                await asyncio.sleep(0.01)
                flying[0] -= 1
                return await read(oid, digest)

            def counted(row, save=save):
                saves[0] += 1
                return save(row)
            api.order, ctl.ledger.save = order, counted
            await ctl.cancel_pair(pair, "设置不同，撤掉后重新报价")
            api.order, ctl.ledger.save = read, save
            outcomes[name] = ({side: (o["state"], float(o.get("filled") or 0)) for side, o in pair["orders"].items()}, pair["phase"], bool(pair.get("accounted")),
                              pair.get("pnl"), pair.get("reason"))
            if ctl.fast():
                self.assertEqual((most[0], saves[0]), (2, 1), "both orders read together, the pair saved once")
            else:
                self.assertEqual((most[0], saves[0]), (1, 4), "one after the other, a save at the start, after each read and at the end — as it was")
            self.assertEqual(ctl.ledger.rows[pair["slug"]], pair)
            await self.finish(ctl)
        self.assertEqual(outcomes["fast"], outcomes["slow"], "the same record either way")
        self.assertEqual(outcomes["fast"], ({"up": ("final", 0.0), "down": ("final", 0.0)}, "closed", True, 0.0, "设置不同，撤掉后重新报价"))
        self.assertEqual(outcomes["fast filled"], outcomes["slow filled"])
        self.assertEqual(outcomes["fast filled"][0]["up"][0], "final")
        self.assertGreater(outcomes["fast filled"][0]["up"][1], 0)
        self.assertFalse(outcomes["fast filled"][2], "a fill: the pair waits for the window's result, its money kept")

    async def test_a_read_that_fails_leaves_the_pair_to_its_next_try(self):
        ctl, api, clock, pair = await self.withdrawn(**KEYS, **BIG)
        clock.ms += p.LATE_FILL_MS
        read = api.order
        down_hash = pair["orders"]["down"]["hash"]

        async def order(oid, digest=""):
            if digest == down_hash:
                raise m.RemoteError("timeout")
            return await read(oid, digest)
        api.order = order
        with self.assertRaises(m.RemoteError):
            await ctl.cancel_pair(pair, "设置不同，撤掉后重新报价")
        self.assertEqual((pair["orders"]["down"]["state"], bool(pair.get("accounted")), pair["slug"] in ctl.cancelling), ("removed", False, False))
        api.order = read
        await ctl.cancel_pair(pair, "设置不同，撤掉后重新报价")  # the next try reads them again: nothing was lost
        self.assertTrue(pair.get("accounted"))
        # what is not such a pair goes the way it always did: an order still to cancel, a window long over
        ctl2, api2, clock2 = T.controller(**KEYS, **BIG)
        c = p.candidate(T.market(), T.NOW, ctl2.owner.config, ctl2.settings)
        await ctl2.place_pair({**c, "market": T.market(), "read_ms": T.NOW})
        resting = ctl2.ledger.rows[c["slug"]]
        self.assertFalse(ctl2.finals_only(resting))
        resting["phase"] = "closing"
        self.assertFalse(ctl2.finals_only(resting), "its orders still rest: to cancel first")
        await self.finish(ctl)
        await self.finish(ctl2)

    async def test_the_known_windows_are_re_read_in_groups(self):
        rows = [T.market(at=T.NOW + n * 15 * MIN, mid=500 + n) for n in range(1, 37)]  # 36 windows, the next nine hours'
        ctl, api, clock = T.controller(rows, **KEYS, **BIG, PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="720")
        ctl.listed_by_card["BTC-15M"] = T.NOW + 36 * 15 * MIN  # every one of them known listed, none quoted
        flying, most = [0], [0]
        call = api.call

        async def slow(method, path, **kw):
            flying[0] += 1
            most[0] = max(most[0], flying[0])
            await asyncio.sleep(0.01)
            flying[0] -= 1
            return await call(method, path, **kw)
        api.call = slow
        st = {"reads": 0, "asked": 0, "found": 0, "deferred": 0, "pending": 0, "stopped": "", "probed": 0}
        await ctl.reread_known(dict(ctl.listed_by_card), p.Attempts(ctl.ledger), ctl.ledger.strikes(), st)
        self.assertEqual((st["reads"], st["deferred"], st["stopped"]), (36, 0, ""))
        self.assertEqual(most[0], p.FAST_REREAD_SIDE, "sixteen asked at a time")
        self.assertEqual(len(ctl.queued), 36 * len(ctl.settings.levels))
        starts = [c["start_ms"] for _, _, c in sorted(ctl.queue._queue)]
        self.assertEqual(starts, sorted(starts), "the soonest first, as ever")
        # more windows than a pass reads: the rest the next pass
        ctl.queued.clear()
        while not ctl.queue.empty():
            ctl.queue.get_nowait()
        ctl.read_pace = 12 * 60 / (5 * p.FAST_REREAD_SHARE)  # (576 reads a minute: a pace that gives twelve windows a pass)
        self.assertEqual(ctl.reread_batch(), 12)
        st = {"reads": 0, "asked": 0, "found": 0, "deferred": 0, "pending": 0, "stopped": "", "probed": 0}
        await ctl.reread_known(dict(ctl.listed_by_card), p.Attempts(ctl.ledger), ctl.ledger.strikes(), st)
        self.assertEqual((st["reads"], st["deferred"]), (12, 24))
        # a read that fails ends the pass, what was answered before it queued; a window Predict no longer lists is left for a minute
        ctl.queued.clear()
        while not ctl.queue.empty():
            ctl.queue.get_nowait()
        ctl.read_pace = 3560.0
        gone, broken = rows[2]["categorySlug"], rows[20]["categorySlug"]
        api.unlisted = {gone}

        async def failing(method, path, **kw):
            if path.endswith(broken):
                raise m.RemoteError("HTTP 500: boom")
            return await call(method, path, **kw)
        api.call = failing
        st = {"reads": 0, "asked": 0, "found": 0, "deferred": 0, "pending": 0, "stopped": "", "probed": 0}
        await ctl.reread_known(dict(ctl.listed_by_card), p.Attempts(ctl.ledger), ctl.ledger.strikes(), st)
        self.assertIn("HTTP 500: boom", st["stopped"])
        self.assertEqual(st["reads"], 32, "the group the failing read was in is the last asked")
        self.assertEqual(ctl.skipped.get(gone), "已发布的场次读不到（404）")
        self.assertEqual(len(ctl.queued), (32 - 2) * len(ctl.settings.levels), "every window answered is queued; the one gone and the one that failed are not")
        await self.finish(ctl)

    async def test_one_read_of_a_markets_details_for_the_levels_that_ask_together(self):
        for env in ({**KEYS, **BIG}, KEYS):  # (with the limits or without: a window's levels never read its market each for itself)
            ctl, api, clock = T.controller(**env, PREOPEN_LEVELS="45x30,43x30,41x30")
            reads = []
            market = api.market

            async def slow(mid, market=market):
                reads.append(mid)
                await asyncio.sleep(0.02)
                return await market(mid)
            api.market = slow
            c = p.candidate(T.market(), T.NOW, ctl.owner.config, ctl.settings)
            levels = [{**c, "slug": c["slug"] if level == 0 else f"{c['slug']}#{level}", "window": c["slug"], "level": level, "price": str(price), "shares": str(shares)}
                      for level, (price, shares) in enumerate(ctl.settings.levels)]
            self.assertEqual(await asyncio.gather(*(ctl.place_pair(level) for level in levels)), [""] * 4)
            self.assertEqual(reads, ["123"], "four levels, one read")
            self.assertEqual((len(api.sent), ctl.detail_reads), (8, {}))
            await self.finish(ctl)
        # a read that fails fails for each of them; the next one asks again
        ctl, api, clock = T.controller(**KEYS, PREOPEN_LEVELS="45x30")
        calls = []

        async def broken(mid):
            calls.append(mid)
            await asyncio.sleep(0.01)
            raise m.RemoteError("HTTP 502: bad gateway")
        market, api.market = api.market, broken
        c = p.candidate(T.market(), T.NOW, ctl.owner.config, ctl.settings)
        results = await asyncio.gather(ctl.place_pair({**c}), ctl.place_pair({**c, "slug": c["slug"] + "#1", "window": c["slug"], "level": 1, "price": "0.45", "shares": "30"}),
                                       return_exceptions=True)
        self.assertTrue(all(isinstance(r, m.RemoteError) for r in results) and calls == ["123"], (results, calls))
        api.market = market
        self.assertEqual(await ctl.place_pair({**c}), "")
        await self.finish(ctl)

    async def test_the_lane_looks_at_the_bucket_the_request_is_of(self):
        m.PREDICT_PACING, m.PREDICT_RATE["per_min"] = True, 6.0  # (a token every ten seconds: a bucket set by hand stays as set)

        def bucket(key, tokens, trade=False):
            pacer = m.predict_pacer(key, trade)
            pacer.tokens, pacer.at = tokens, time.monotonic()
        # a key whose orders have a bucket of their own, the rule taker's key lent beside it
        ctl, api, clock = T.controller(**KEYS)
        api.api_key, api.low = "pre-key", True
        lent = T.Api(ctl, list(api.markets.values()))
        lent.api_key, lent.low, lent.jwt = "rule-key", True, "t"
        ctl.lent_api = lent
        ctl.owner.rules = types.SimpleNamespace(alive=True, api=types.SimpleNamespace(blocked_until=0.0), now=lambda: 0, busy_in=lambda now: None)
        m.PREDICT_TRADE_OF["pre-key"] = 6.0
        bucket("pre-key", 0.0), bucket("pre-key", 8.0, trade=True), bucket("rule-key", 8.0)
        self.assertIs(ctl.lane(api), lent, "a read: the key's own bucket is empty, the idle key has a token")
        self.assertIs(ctl.lane(api, trade=True), api, "an order: its bucket on the key is full — it does not go off to the borrowed key")
        bucket("pre-key", 8.0), bucket("pre-key", 0.0, trade=True)
        self.assertIs(ctl.lane(api), api)
        self.assertIs(ctl.lane(api, trade=True), lent)
        # place_pair says so for its orders: both go out on the mode's own key although its reads' bucket is empty
        bucket("pre-key", 0.0), bucket("pre-key", 8.0, trade=True)
        c = p.candidate(T.market(), T.NOW, ctl.owner.config, ctl.settings)
        self.assertEqual(await ctl.place_pair({**c, "market": T.market(), "read_ms": T.NOW}), "")
        self.assertEqual((len(api.sent), len(lent.sent)), (2, 0))
        await self.finish(ctl)
        # at Predict's real limits nothing is borrowed at all: the rule taker's key is left to its taker and to the orderbook feed
        fast, fast_api, _ = T.controller(**KEYS, **BIG)
        fast_api.api_key, fast_api.low = "pre-key", True
        other = T.Api(fast, list(fast_api.markets.values()))
        other.api_key, other.low, other.jwt = "rule-key", True, "t"
        fast.lent_api = other
        fast.owner.rules = types.SimpleNamespace(alive=True, api=types.SimpleNamespace(blocked_until=0.0), now=lambda: 0, busy_in=lambda now: None)
        m.set_predict_rates(fast.owner.config)
        bucket("pre-key", 0.0), bucket("rule-key", 8.0)
        self.assertEqual(fast.borrow_why(), "预挂自己的 Key 已经按 Predict 后台的限额在跑，用不着借")
        self.assertIs(fast.lane(fast_api), fast_api)
        self.assertIn("规则吃单的 Key 空闲时借来用（RULE_KEY_LEND）：现在不借——预挂自己的 Key 已经按 Predict 后台的限额在跑，用不着借", fast.borrow_line())
        await self.finish(fast)
        # ...but where the app's General limit leaves the mode few reads (the 1,000 tier: 360 a minute) the idle key is borrowed
        # as before — for reads: its share of that limit is kept for it in the mode's pace. Never for an order
        short, short_api, _ = T.controller(**KEYS, **SMALL)
        short_api.api_key, short_api.low = "pre-key", True
        idle = T.Api(short, list(short_api.markets.values()))
        idle.api_key, idle.low, idle.jwt = "rule-key", True, "t"
        short.lent_api = idle
        short.owner.rules = types.SimpleNamespace(alive=True, api=types.SimpleNamespace(blocked_until=0.0), now=lambda: 0, busy_in=lambda now: None)
        m.set_predict_rates(short.owner.config)
        self.assertEqual((short.read_pace, short.borrow_why()), (360.0, ""))
        bucket("pre-key", 0.0), bucket("pre-key", 0.0, trade=True), bucket("rule-key", 8.0)
        self.assertIs(short.lane(short_api), idle, "a read: on the idle key")
        self.assertIs(short.lane(short_api, trade=True), short_api, "an order waits for its own bucket's token (25 a second there)")
        await self.finish(short)

    async def test_what_is_said(self):
        m.PREDICT_PACING = True
        ctl, api, clock = T.controller(**KEYS, **BIG)
        api.api_key = "pre-key"
        m.set_predict_rates(ctl.owner.config)
        self.assertTrue(ctl.pace_line().startswith(
            "用预挂自己的 API Key（PREOPEN_API_KEY），按 Predict 后台给这个 App 的限额跑（Trading 5000、General 5000 次/分钟，PREOPEN_APP_*）："
            "下单、撤单和读订单每分钟最多 3760 次，其他请求（市场、分类、登录）最多 3560 次——各用到限额的 80%，再扣掉机器人别的 Key 可能用的份额；同时挂 19 对、同时核对 16 对；"), ctl.pace_line())
        now = time.monotonic()
        m.predict_pacer("pre-key").sent.extend([now] * 3400)
        m.predict_pacer("pre-key", True).sent.extend([now] * 1200)
        m.predict_pacer("pre-key", True).note_429()
        line = ctl.speed_line()
        self.assertIn("；预挂专用 Key 最近 1 分钟：下单、撤单和读订单 1200 次（节拍每分钟 3760）、其他请求 3400 次（节拍 3560，跑满了）；被拒绝（429）：最近 10 分钟 1 次、24 小时 1 次", line)
        self.assertIn("预挂已经按 Predict 后台的限额在跑（PREOPEN_APP_*：下单、撤单和读订单每分钟 3760 次，其他请求 3560 次）", ctl.speed_refusal())
        self.assertEqual(ctl.speed_start()[0], None)
        ns = types.SimpleNamespace(config=ctl.owner.config, KEY_SHARED_BELOW=L.LiveBot.KEY_SHARED_BELOW)
        ns.preopen_own_key = lambda: L.LiveBot.preopen_own_key(ns)
        ns.book_own_key = lambda: L.LiveBot.book_own_key(ns)
        load = L.LiveBot.live_key_load(ns)
        self.assertIn("预挂专用 Key 3400 次（按 Predict 后台的限额：这是 General 一类（读市场、分类、登录等），节拍每分钟 3560 次；"
                      "Trading 一类（下单、撤单、读订单）另算，最近 60 秒 1200 次、节拍 3760 次", load)
        await self.finish(ctl)
        # the limits given where the mode has no key of its own: said, and nothing changed
        shared, _, _ = T.controller(PREDICT_API_KEY="main-key", **BIG)
        self.assertIn("PREOPEN_APP_*（Predict 后台的限额）填了但没起作用：预挂没有自己单独的 Key", shared.pace_line())
        await self.finish(shared)
        # one of the two alone (1.154.62): said which is missing; nothing in force
        for env, words in (({"PREOPEN_APP_TRADING_PER_MIN": "2000"}, "——“Predict 限额”只填了 Trading，还差 General：两项都填了才生效；"),
                           ({"PREOPEN_APP_GENERAL_PER_MIN": "2000"}, "——“Predict 限额”只填了 General，还差 Trading：两项都填了才生效；")):
            half, _, _ = T.controller(**KEYS, **env)
            self.assertFalse(half.fast())
            self.assertIn(words, half.pace_line())
            await self.finish(half)

    async def test_a_withdrawn_pairs_final_reads_wait_out_a_cooldown(self):
        """A 429's twenty seconds: the thousands of withdrawn pairs waiting do not each spend a try on a read that cannot go out."""
        kept, p.COOL_WAIT_S = p.COOL_WAIT_S, 5.0
        try:
            ctl, api, clock, pair = await self.withdrawn(**KEYS, **BIG)
            clock.ms += p.LATE_FILL_MS
            read, when = api.order, []

            async def order(order_id, digest=""):
                when.append(time.monotonic())
                return await read(order_id, digest)
            api.order = order
            began = time.monotonic()
            api.blocked_until = began + 0.3
            await ctl.cancel_pair(pair, pair["reason"])
            self.assertEqual(len(when), 2)
            self.assertGreaterEqual(min(when) - began, 0.29, "both reads after the cooldown, none into it")
            self.assertTrue(pair.get("accounted"))
            await self.finish(ctl)
            # no longer than COOL_WAIT_S: a cooldown that does not end leaves the pair to its try as before
            p.COOL_WAIT_S = 0.2
            ctl, api, clock, pair = await self.withdrawn(**KEYS, **BIG)
            clock.ms += p.LATE_FILL_MS
            began = time.monotonic()
            api.blocked_until = began + 60.0
            await ctl.cancel_pair(pair, pair["reason"])
            self.assertLess(time.monotonic() - began, 1.0)
            await self.finish(ctl)
            # without the limits nothing waits here (the pair's try as it always was)
            ctl, api, clock, pair = await self.withdrawn(**KEYS)
            clock.ms += p.LATE_FILL_MS
            began = time.monotonic()
            api.blocked_until = began + 60.0
            await ctl.cancel_pair(pair, pair["reason"])
            self.assertLess(time.monotonic() - began, 0.2)
            await self.finish(ctl)
        finally:
            p.COOL_WAIT_S = kept

    def test_predicts_refusal_cuts_the_pace_of_the_bucket_refused(self):
        """The figures given are not what Predict counts the key by (another app's page, a tier that moved): a 429 at them."""
        config = m.Config.from_env({**BASE, **KEYS, **BIG})
        m.set_predict_rates(config)
        again = lambda bucket: m.PREOPEN_CUTS[m.preopen_cut_key(config)].__setitem__(bucket + "_at", -1e9)  # (the episode over)
        # the page of the 5,000 app copied for a key of the 1,000 one: the reads refused 15 seconds in, 875 of them let through —
        # to 80% of what Predict did accept, at once
        self.assertEqual(m.preopen_refused(config, "read", 875), (3560.0, 700.0))
        self.assertEqual((m.PREDICT_RATE_OF, m.PREDICT_TRADE_OF), ({"pre-key": 700.0}, {"pre-key": 3760.0}), "the reads' pace cut and in force; the orders' as it was")
        self.assertEqual((m.predict_pacer("pre-key").per_min, m.predict_pacer("pre-key", True).per_min), (700.0, 3760.0))
        self.assertEqual((m.preopen_paces(config), m.preopen_paces(config, whole=True)), ((3760.0, 700.0), (3760.0, 3560.0)))
        # Predict's minute is still full, and the requests already out meet it too: the same episode, one cut
        self.assertIsNone(m.preopen_refused(config, "read", 600))
        self.assertEqual(m.preopen_paces(config), (3760.0, 700.0))
        # still too fast a minute and a half on (700 and the other keys' 440 of a limit of 1,000): again by what did go through
        again("read")
        self.assertEqual(m.preopen_refused(config, "read", 600), (700.0, 480.0))
        # a setting saved — any of them — does not bring back what Predict refused
        m.set_predict_rates(m.Config.from_env({**BASE, **KEYS, **BIG, "PREOPEN_SHARES": "20"}))
        self.assertEqual((m.PREDICT_RATE_OF, m.PREDICT_TRADE_OF), ({"pre-key": 480.0}, {"pre-key": 3760.0}))
        # a refusal with the bucket at its pace for a whole minute (no more let through than the pace): a fifth off, no less
        self.assertEqual(m.preopen_refused(config, "trade", 3890), (3760.0, 3008.0))
        # a count that says little (a 429 in a quiet minute): a tenth of the pace at the least in one step
        quiet = m.Config.from_env({**BASE, **KEYS, "PREOPEN_APP_TRADING_PER_MIN": "6000", "PREOPEN_APP_GENERAL_PER_MIN": "6000"})
        self.assertEqual(m.preopen_refused(quiet, "read", 30), (4360.0, 436.0))
        self.assertEqual(list(m.PREOPEN_CUTS), [("pre-key", 6000, 6000)], "(other limits given: the cuts of the ones before are gone)")
        m.PREOPEN_CUTS.clear()
        for bucket, sent, cut in (("read", 875, (3560.0, 700.0)), ("read", 600, (700.0, 480.0)), ("trade", 3890, (3760.0, 3008.0))):
            self.assertEqual(m.preopen_refused(config, bucket, sent), cut)
            again(bucket)
        # down to the pace the key has without the limits, and no further
        for sent, cut in ((300, (480.0, 240.0)), (100, (240.0, 220.0)), (50, (220.0, 220.0)), (0, (220.0, 220.0))):
            again("read")
            self.assertEqual(m.preopen_refused(config, "read", sent), cut)
        self.assertEqual(m.preopen_paces(config), (3008.0, 220.0))
        # other limits given (the page read again): those, uncut — and a refusal at them forgets the old ones'
        small = m.Config.from_env({**BASE, **KEYS, **SMALL})
        self.assertEqual(m.preopen_paces(small), (1360.0, 360.0))
        self.assertEqual(m.preopen_refused(small, "read", 400), (360.0, 288.0))
        self.assertEqual(list(m.PREOPEN_CUTS), [("pre-key", 2000, 1000)])
        self.assertEqual(m.preopen_paces(config), (3760.0, 3560.0), "the first limits given again: from their own figures")
        # no such paces in force (the limits not given; the mode on the bot's own key): nothing to cut
        for plain in (m.Config.from_env({**BASE, **KEYS}), m.Config.from_env({**BASE, "PREDICT_API_KEY": "main-key", **BIG})):
            self.assertIsNone(m.preopen_refused(plain, "read", 100))
        self.assertEqual(list(m.PREOPEN_CUTS), [("pre-key", 2000, 1000)])

    async def test_the_mode_answers_predicts_refusal_as_its_loop_goes_round(self):
        m.PREDICT_PACING = True
        ctl, api, clock = T.controller(**KEYS, **BIG)
        api.api_key = "pre-key"
        m.set_predict_rates(ctl.owner.config)
        said = []
        ctl.owner.live_notify = lambda text, key="": said.append((key, text))
        reads, orders = m.predict_pacer("pre-key"), m.predict_pacer("pre-key", True)
        ctl.fast_watch()
        self.assertEqual((said, ctl.read_pace, ctl.cut_line()), ([], 3560.0, ""), "no 429: nothing")
        now = time.monotonic()
        reads.sent.extend([now] * 875)
        reads.note_429()
        ctl.fast_watch()
        self.assertEqual((ctl.trade_pace, ctl.read_pace), (3760.0, 700.0))
        self.assertEqual((m.PREDICT_RATE_OF, m.PREDICT_TRADE_OF), ({"pre-key": 700.0}, {"pre-key": 3760.0}))
        self.assertEqual([key for key, _ in said], ["preopen:cut:read:700"])
        self.assertIn("被 Predict 拒绝了（429）——读市场、分类等其他请求在被拒前的 60 秒发了 875 次，节拍已从每分钟 3560 次自动降到 700 次", said[0][1])
        self.assertIn("填的限额（Trading 5000 / General 5000）可能不是预挂这把 Key 所在 App 的，或者交易量档位变了", said[0][1])
        self.assertEqual((ctl.fast_reads(), ctl.fast_verify(), ctl.reread_batch()), (29, 16, 14), "the listings' re-reads follow the General pace down; the final reads go by the Trading one")
        # the 429s of the same episode (the client's requests already out, Predict's minute still full): looked at, not answered again
        reads.note_429(); reads.note_429()
        ctl.fast_watch()
        ctl.fast_watch()
        self.assertEqual((ctl.read_pace, len(said)), (700.0, 1))
        # an order refused: the orders' bucket, by what it had let through
        orders.sent.extend([now] * 1200)
        orders.note_429()
        ctl.fast_watch()
        self.assertEqual((ctl.trade_pace, ctl.read_pace), (960.0, 700.0))
        self.assertEqual([key for key, _ in said], ["preopen:cut:read:700", "preopen:cut:trade:960"])
        self.assertIn("下单、撤单和读订单在被拒前的 60 秒发了 1200 次，节拍已从每分钟 3760 次自动降到 960 次", said[1][1])
        self.assertIn("同时核对 4 对；⚠️ 被 Predict 拒绝过（429），已自动降速：下单、撤单和读订单从每分钟 3760 次降到 960 次、其他请求从每分钟 3560 次降到 700 次——", ctl.pace_line())
        self.assertIn("下单、撤单和读订单每分钟最多 960 次，其他请求（市场、分类、登录）最多 700 次", ctl.pace_line())
        await self.finish(ctl)
        # a setting saved builds the mode anew: at the paces as cut, and the 429s already answered are not answered again
        fresh, fresh_api, _ = T.controller(**KEYS, **BIG)
        fresh_api.api_key = "pre-key"
        fresh.owner.live_notify = lambda text, key="": said.append((key, text))
        self.assertEqual((fresh.trade_pace, fresh.read_pace, fresh.place_workers()), (960.0, 700.0, 5))
        m.PREOPEN_CUTS[m.preopen_cut_key(fresh.owner.config)].update(read_at=-1e9, trade_at=-1e9)  # (long after)
        fresh.fast_watch()
        self.assertEqual((fresh.trade_pace, fresh.read_pace, len(said)), (960.0, 700.0, 2))
        # at the lowest pace and refused still: said, with what to do
        for later, sent in enumerate((300, 100, 50), 1):
            m.PREOPEN_CUTS[m.preopen_cut_key(fresh.owner.config)]["read_at"] = -1e9
            reads.limits.append((time.monotonic() + later, sent))  # (a later 429 each, with what the bucket had let through before it)
            fresh.fast_watch()
        self.assertEqual(fresh.read_pace, 220.0)
        self.assertEqual([key for key, _ in said][2:], ["preopen:cut:read:240", "preopen:cut:read:220", "preopen:cut:read:floor"])
        self.assertIn("读市场、分类等其他请求已经降到最低的每分钟 220 次，仍被 Predict 拒绝（429）", said[-1][1])
        self.assertIn("把“Predict 限额”那两项改成 0（不填），预挂就回到原来的方式", said[-1][1])
        await self.finish(fresh)
        # the look is never the loop's problem (that loop withdraws the orders at their time): a failure in it is logged, no more
        broken, _, _ = T.controller(**KEYS, **BIG)
        broken.refusal_last = lambda bucket: 1 / 0
        with self.assertLogs(p.LOG, "WARNING") as logged:
            broken.fast_watch()
        self.assertIn("被拒绝后的降速没做成", logged.output[0])
        await self.finish(broken)
        # without the limits the loop's look does nothing, whatever the key's 429s
        plain, plain_api, _ = T.controller(**KEYS)
        plain_api.api_key = "pre-key"
        plain.owner.live_notify = lambda text, key="": said.append((key, text))
        m.set_predict_rates(plain.owner.config)
        m.predict_pacer("pre-key").note_429()
        plain.fast_watch()
        self.assertEqual((len(said), plain.cut_line(), list(m.PREOPEN_CUTS)), (5, "", [("pre-key", 5000, 5000)]), "nothing said, nothing cut (the table as the limits' controller left it)")
        await self.finish(plain)

    def test_the_control_page(self):
        class FM:
            def __init__(self, now): self.now, self.config = now, None
            def now_ms(self): return self.now
        for key in ("PREOPEN_APP_TRADING_PER_MIN", "PREOPEN_APP_GENERAL_PER_MIN"):
            self.assertEqual((m.CONTROL_FORMS[key]["kind"], m.CONTROL_FORMS[key]["group"]), ("number", "预挂"))
            self.assertIn(key, m.CONTROL_KEY_SET)
        env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "PREOPEN_15M": "on", **KEYS}
        bot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(T.NOW), None)
        self.assertEqual((bot.control_value("PREOPEN_APP_TRADING_PER_MIN"), bot.control_value("PREOPEN_APP_GENERAL_PER_MIN")), ("0", "0"))
        # 1.154.62: one at a time, as the page saves them (each box its own 保存) — the first kept and not in force, the second
        # puts both in force. They were refused one at a time ("要一起填") and could never be saved from the page at all
        answer = bot.control_set({"PREOPEN_APP_TRADING_PER_MIN": "5000"})
        self.assertTrue(answer["ok"], answer)
        self.assertEqual((bot.config.preopen.app_trading, bot.config.preopen.app_general, m.PREDICT_TRADE_OF), (5000, 0, {}))
        answer = bot.control_set({"PREOPEN_APP_GENERAL_PER_MIN": "5000"})
        self.assertTrue(answer["ok"], answer)
        self.assertEqual((m.PREDICT_RATE_OF, m.PREDICT_TRADE_OF), ({"pre-key": 3560.0}, {"pre-key": 3760.0}))
        self.assertEqual(bot.config.preopen.app_trading, 5000)
        self.assertFalse(bot.control_set({"PREOPEN_APP_GENERAL_PER_MIN": "100"})["ok"], "under 240: refused, the value in force kept")
        self.assertEqual(bot.config.preopen.app_general, 5000)
        # ...and the pre-open key's own pace, typed 2000 there by mistake (11-26 16:05): refused, as ever (60～1000)
        self.assertFalse(bot.control_set({"PREOPEN_RATE_PER_MIN": "2000"})["ok"])
        self.assertTrue(bot.control_set({"PREOPEN_APP_TRADING_PER_MIN": "0", "PREOPEN_APP_GENERAL_PER_MIN": "0"})["ok"])
        self.assertEqual((m.PREDICT_RATE_OF, m.PREDICT_TRADE_OF), ({}, {}))


if __name__ == "__main__":
    result = unittest.main(exit=False).result
    if result.wasSuccessful():
        print("FAST_OK")
    else:
        sys.exit(1)
