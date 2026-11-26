"""1.154.59 — the pre-open mode on several keys of its own (PREOPEN_API_KEYS). The user: "预挂有几把 key 可以用，然后看怎么用可以性能最高".
Predict limits each key by itself, and the mode had one (and the rule taker's while that taker idles). Here: the setting — the
further keys, another role's left out; a client a key, its cooldown its own; the lane — the mode's first key while it has a
token, then its further keys in their order, then the borrowed one, and a further key at once while the first cools down; a
further key's refusal sends the request on the first and never pauses the lending; a pair's orders on a further key; a
window's cancel on one while the first key waits out a 429; the sign-ins kept; more pairs side by side; what the status and
/diag say of each key. Entirely offline.

Run: python tests/test_morekeys.py
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

BASE = {"TELEGRAM_BOT_TOKEN": "offline"}
KEYS = {"PREDICT_API_KEY": "main-key", "PREOPEN_API_KEY": "pre-key", "RULE_API_KEY": "rule-key", "PREOPEN_API_KEYS": "k2,k3"}


def bucket(key: str, tokens: float) -> m.PredictPacer:
    """A key's pacer holding ``tokens`` now (the pace slow enough that it stays so for the test)."""
    pacer = m.predict_pacer(key)
    pacer.tokens, pacer.at = tokens, time.monotonic()
    return pacer


def stand_in(ctl, api, key: str, **more):
    """A client of the tests' on ``key``, signed in."""
    client = T.Api(ctl, list(api.markets.values()))
    client.api_key, client.jwt, client.low = key, "t", False
    for name, value in more.items():
        setattr(client, name, value)
    return client


class MoreKeys(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.kept = (m.PREDICT_PACING, m.PREDICT_RATE["per_min"], dict(m.PREDICT_PACERS), dict(m.PREDICT_RATE_OF))
        m.PREDICT_PACING, m.PREDICT_RATE["per_min"] = True, 6.0  # a token every ten seconds: a bucket set by hand stays as set
        m.PREDICT_PACERS.clear()

    def tearDown(self):
        m.PREDICT_PACING, m.PREDICT_RATE["per_min"] = self.kept[0], self.kept[1]
        m.PREDICT_PACERS.clear(); m.PREDICT_PACERS.update(self.kept[2])
        m.PREDICT_RATE_OF.clear(); m.PREDICT_RATE_OF.update(self.kept[3])

    async def finish(self, ctl):
        tasks = [*ctl.settling.values(), *(t for t in (ctl.lent_warm, ctl.more_warm) if t is not None)]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        ctl.ledger.close()

    def mode(self, **env):
        """The mode with two further keys, the tests' clients standing for all of them: (ctl, api, clock, e2, e3, lent)."""
        ctl, api, clock = T.controller(**{**KEYS, **env})
        api.api_key, api.low = "pre-key", True
        e2, e3 = stand_in(ctl, api, "k2"), stand_in(ctl, api, "k3")
        ctl.more = [e2, e3]
        lent = stand_in(ctl, api, "rule-key", low=True)
        ctl.lent_api = lent
        ctl.owner.rules = types.SimpleNamespace(alive=True, api=types.SimpleNamespace(blocked_until=0.0), now=lambda: 0, busy_in=lambda now: None)
        return ctl, api, clock, e2, e3, lent

    def test_the_setting(self):
        keys = lambda **env: m.preopen_keys_of(env)
        self.assertEqual(keys(), ("", ()))
        self.assertEqual(keys(PREOPEN_API_KEY=" a "), ("a", ()))
        self.assertEqual(keys(PREOPEN_API_KEY="a", PREOPEN_API_KEYS=" b, c\nd;e  f ,,"), ("a", ("b", "c", "d", "e", "f")), "commas, spaces, line breaks")
        self.assertEqual(keys(PREOPEN_API_KEY="a", PREOPEN_API_KEYS="b,a,b,c"), ("a", ("b", "c")), "each once; the mode's first key is not a further one")
        self.assertEqual(keys(PREOPEN_API_KEYS="b,c"), ("b", ("c",)), "no first key given: the first of the list is it")
        # another role's key is not the mode's to fill: left out (the rule taker's is borrowed by its own rule, while it idles)
        self.assertEqual(keys(PREDICT_API_KEY="main", PREDICT_BOOK_API_KEY="book", RULE_API_KEY="rule", PREOPEN_API_KEY="a", PREOPEN_API_KEYS="main,b,book,rule,c"), ("a", ("b", "c")))
        self.assertEqual(len(keys(PREOPEN_API_KEY="a", PREOPEN_API_KEYS=",".join(f"k{i}" for i in range(m.PREOPEN_KEYS_MAX)))[1]), m.PREOPEN_KEYS_MAX)
        with self.assertRaisesRegex(ValueError, "PREOPEN_API_KEYS 最多 8 把（现在 9 把）"):
            keys(PREOPEN_API_KEY="a", PREOPEN_API_KEYS=",".join(f"k{i}" for i in range(m.PREOPEN_KEYS_MAX + 1)))
        config = m.Config.from_env({**BASE, **KEYS})
        self.assertEqual((config.preopen_api_key, config.preopen_api_keys), ("pre-key", ("k2", "k3")))
        self.assertNotIn("k2", repr(config))
        self.assertEqual(m.Config.from_env(BASE).preopen_api_keys, ())
        self.assertEqual(m.preopen_own_keys(config), ("pre-key", "k2", "k3"))
        self.assertEqual(m.preopen_own_keys(m.Config.from_env({**BASE, "PREDICT_API_KEY": "main-key", "PREOPEN_API_KEY": "main-key", "PREOPEN_API_KEYS": "k2"})), ("k2",),
                         "the mode's first key the bot's own: the further ones are still its alone")
        # the mode's own pace (PREOPEN_RATE_PER_MIN) is each of its keys'; the others keep every key's
        m.PREDICT_RATE["per_min"] = 220.0
        m.set_predict_rates(dataclasses.replace(config, predict_rate_per_min=220.0, preopen_rate_per_min=240.0))
        self.assertEqual(m.PREDICT_RATE_OF, {"pre-key": 240.0, "k2": 240.0, "k3": 240.0})
        self.assertEqual([m.predict_pacer(key).per_min for key in ("pre-key", "k2", "k3", "main-key", "rule-key")], [240.0, 240.0, 240.0, 220.0, 220.0])
        m.set_predict_rates(dataclasses.replace(config, predict_rate_per_min=220.0))
        self.assertEqual(m.PREDICT_RATE_OF, {})

    async def test_a_client_a_key(self):
        ctl, api, clock = T.controller(**KEYS)
        self.assertEqual([c.api_key for c in ctl.more], ["k2", "k3"])
        self.assertTrue(all(c.patient and not c.low for c in ctl.more), "its orders wait for the key's token, as a read does; the key is the mode's alone")
        self.assertEqual([ctl.more_cancel[id(c)].api_key for c in ctl.more], ["k2", "k3"])
        self.assertTrue(all(not ctl.more_cancel[id(c)].patient for c in ctl.more), "a cancel there goes ahead")
        # a cooldown is a key's own: a 429 on one stops nothing on the others, nor on the mode's first key — and a key's cancel client shares its key's
        ctl.more[0].blocked_until = time.monotonic() + 20
        self.assertGreater(ctl.more_cancel[id(ctl.more[0])].blocked_until, time.monotonic())
        self.assertEqual((ctl.more[1].blocked_until, ctl.more_cancel[id(ctl.more[1])].blocked_until, ctl.first_api.blocked_until, api.blocked_until), (0.0, 0.0, 0.0, 0.0))
        ctl.more[0].blocked_until = 0.0
        self.assertEqual(ctl.more_ready(), [], "the tests' own client in for the mode's: the made ones on the further keys are not used")
        self.assertIs(ctl.lane(api), api)
        self.assertEqual(ctl.place_workers(), p.PLACE_WORKERS + 2 * p.PLACE_WORKERS_MORE)
        self.assertEqual(ctl.lent_made.api_key, "rule-key", "the rule taker's key is still lent beside them")
        await self.finish(ctl)
        none, _, _ = T.controller(PREDICT_API_KEY="main-key", PREOPEN_API_KEY="pre-key")
        self.assertEqual((none.more, none.more_cancel, none.place_workers(), none.more_usage()), ([], {}, p.PLACE_WORKERS, []))
        none.more_keep_warm()
        self.assertIsNone(none.more_warm)
        await self.finish(none)
        many, _, _ = T.controller(PREDICT_API_KEY="main-key", PREOPEN_API_KEY="pre-key", PREOPEN_API_KEYS="a,b,c,d,e,f")
        self.assertEqual((len(many.more), many.place_workers()), (6, p.PLACE_WORKERS_MAX))
        await self.finish(many)
        # the rule taker's key given as one of the mode's own: the setting leaves it out, and it stays lent by its own rule
        same, _, _ = T.controller(**{**KEYS, "PREOPEN_API_KEYS": "k2,rule-key"})
        self.assertEqual(([c.api_key for c in same.more], same.lent_made.api_key), (["k2"], "rule-key"))
        await self.finish(same)

    async def test_the_lane(self):
        ctl, api, clock, e2, e3, lent = self.mode()
        first = stand_in(ctl, api, "pre-key")
        self.assertEqual(ctl.more_ready(), [e2, e3])
        # the mode's first key while it has a token for the request; then its further keys, in their order; then the borrowed one
        bucket("pre-key", 8.0), bucket("k2", 8.0), bucket("k3", 8.0), bucket("rule-key", 8.0)
        self.assertIs(ctl.lane(api), api)
        bucket("pre-key", 2.5)  # a low request needs three: short
        self.assertIs(ctl.lane(api), e2)
        self.assertIs(ctl.lane(first), first, "a fresh listing's request needs one: its own key has it")
        bucket("pre-key", 0.2)
        self.assertIs(ctl.lane(first), e2)
        bucket("k2", 0.9)
        self.assertIs(ctl.lane(api), e3)
        bucket("k2", 1.0)
        self.assertIs(ctl.lane(api), e2, "a further key is the mode's alone: one token is enough there")
        bucket("k2", 0.0), bucket("k3", 0.0)
        self.assertIs(ctl.lane(api), lent)
        self.assertEqual(ctl.lent_last(), 1)
        bucket("rule-key", 2.5)
        self.assertIs(ctl.lane(api), api, "every key short: it waits on its first, as ever")
        # a further key is passed by while it cools down, is not signed in, or is left alone after a refusal
        bucket("k2", 8.0), bucket("k3", 8.0)
        e2.blocked_until = time.monotonic() + 20
        self.assertEqual((ctl.more_ready(), ctl.lane(api)), ([e3], e3))
        e2.blocked_until, e2.jwt = 0.0, ""
        self.assertEqual((ctl.more_ready(), ctl.lane(api)), ([e3], e3))
        e2.jwt = "t"
        ctl.more_off[id(e2)] = (time.monotonic() + 60, "拒绝了请求（HTTP 401）")
        self.assertEqual(ctl.more_ready(), [e3])
        ctl.more_off[id(e2)] = (time.monotonic() - 1, "拒绝了请求（HTTP 401）")
        self.assertEqual(ctl.more_ready(), [e2, e3], "tried again once its time is over")
        # the first key waiting out a 429: it would refuse the request itself, so a further key takes it — at once with a
        # token, else it waits for one there; with no further key ready, the borrowed one, else home as before
        bucket("pre-key", 8.0)
        api.blocked_until = time.monotonic() + 20
        self.assertIs(ctl.lane(api), e2)
        bucket("k2", 0.0)
        self.assertIs(ctl.lane(api), e3)
        bucket("k3", 0.0)
        self.assertIs(ctl.lane(api), e2, "no token anywhere: it waits on the first of the further keys, not refused on its own")
        self.assertEqual(ctl.hold(), "", "pairs go on being placed: another of the mode's keys carries them")
        e2.blocked_until = e3.blocked_until = time.monotonic() + 20
        bucket("rule-key", 8.0)
        self.assertIs(ctl.lane(api), lent)
        self.assertIn("Predict 接口限流冷却中", ctl.hold())
        bucket("rule-key", 0.0)
        self.assertIs(ctl.lane(api), api)
        await self.finish(ctl)

    async def test_a_request_waits_for_whichever_key_has_a_token_first(self):
        # how long until a key has a token for a request, at its pace
        pacer = bucket("w", 0.25)
        self.assertAlmostEqual(pacer.wait_for(), 7.5, places=1)  # (six a minute: three quarters of a token, ten seconds each)
        self.assertAlmostEqual(pacer.wait_for(low=True), 27.5, places=1)
        self.assertEqual((bucket("w", 3.0).wait_for(low=True), bucket("w", 1.0).wait_for()), (0.0, 0.0))
        m.PREDICT_PACING = False
        self.assertEqual(bucket("w", 0.0).wait_for(), 0.0)
        m.PREDICT_PACING = True
        ctl, api, clock, e2, e3, lent = self.mode()
        ctl.lent_api = None
        own = bucket("pre-key", 8.0)
        self.assertIs(await ctl.lane_wait(api), api, "a token on its first key: no wait")
        self.assertEqual(len(own.waits), 0)
        # no key has one: it looks again when the soonest is due (a quarter of a second at the most), and takes the first that has
        bucket("pre-key", 0.0), bucket("k2", 0.0), bucket("k3", 0.0)
        waiting = asyncio.create_task(ctl.lane_wait(api))
        await asyncio.sleep(0.05)
        self.assertFalse(waiting.done())
        bucket("k3", 1.0)
        self.assertIs(await asyncio.wait_for(waiting, 2), e3)
        self.assertEqual(len(own.waits), 1, "what it waited is written to its first key's pacer (/diag: 等节拍)")
        self.assertGreater(own.waits[-1][1], 0.04)
        # ...its own first key's token as well as any (a low request needs three there)
        bucket("k3", 0.0)
        waiting = asyncio.create_task(ctl.lane_wait(api))
        await asyncio.sleep(0.02)
        bucket("pre-key", 3.0)
        self.assertIs(await asyncio.wait_for(waiting, 2), api)
        # the mode stopping ends the wait (the request then fails or waits on its own client, as a stop finds any request)
        bucket("pre-key", 0.0)
        waiting = asyncio.create_task(ctl.lane_wait(api))
        await asyncio.sleep(0.02)
        ctl.stop.set()
        self.assertIs(await asyncio.wait_for(waiting, 2), api)
        ctl.stop.clear()

        # a fresh listing's first-level order goes ahead of every other request waiting for a token
        first_client = stand_in(ctl, api, "pre-key")

        async def order(home, first=False):
            """A request as through sends it: the lane, and — in the same breath, as the client's own take is — its token."""
            client = await ctl.lane_wait(home, first)
            m.predict_pacer(client.api_key).tokens -= 1.0
            return client
        ahead = asyncio.create_task(order(first_client, True))
        behind = asyncio.create_task(order(api))
        await asyncio.sleep(0.02)
        self.assertEqual((ctl.firsts_waiting, ahead.done(), behind.done()), (1, False, False))
        bucket("k2", 1.0)
        with self.assertRaises(asyncio.TimeoutError):  # one that asks now, a token in sight, does not take it from under the first level
            await asyncio.wait_for(order(api), 0.03)
        self.assertIs(await asyncio.wait_for(ahead, 2), e2)
        self.assertEqual(ctl.firsts_waiting, 0)
        await asyncio.sleep(0.03)
        self.assertFalse(behind.done(), "the first level's order took that token: the other still waits")
        bucket("k3", 1.0)
        self.assertIs(await asyncio.wait_for(behind, 2), e3)
        # a first-level waiter cancelled, or ended by an error, is counted out: nobody yields to one that is gone
        gone = asyncio.create_task(ctl.lane_wait(first_client, True))
        bucket("k3", 0.0)
        await asyncio.sleep(0.02)
        gone.cancel()
        await asyncio.gather(gone, return_exceptions=True)
        self.assertEqual(ctl.firsts_waiting, 0)

        # place_pair: a fresh listing's first level says so; its deeper levels, and a window quoted again, do not
        asked = []
        real = ctl.lane_wait

        async def spy(home, first=False):
            asked.append(first)
            return await real(home, first)
        ctl.lane_wait = spy
        bucket("pre-key", 8.0), bucket("k2", 8.0), bucket("k3", 8.0)
        c = p.candidate(T.market(), T.NOW, ctl.owner.config, ctl.settings)
        for level, fresh in ((0, True), (1, True)):
            asked.clear()
            slug = c["slug"] if level == 0 else f"{c['slug']}#{level}"
            self.assertEqual(await ctl.place_pair({**c, "slug": slug, "window": c["slug"], "level": level, "market": T.market(), "read_ms": T.NOW, "fresh": fresh}), "")
            self.assertEqual(asked, [level == 0] * 2, (level, asked))
        later = T.market(at=T.NOW + 45 * 60_000, mid=456)
        api.markets[str(later["id"])] = later
        c2 = p.candidate(later, T.NOW, ctl.owner.config, ctl.settings)
        asked.clear()
        self.assertEqual(await ctl.place_pair({**c2, "market": later, "read_ms": T.NOW}), "")
        self.assertEqual(asked, [False, False], "a window of the backlog: not a fresh listing")
        await self.finish(ctl)
        # one key (and the borrowed one beside it): the request goes as it always did — no waiting here, it waits on its own key's pacer
        one, one_api, _ = T.controller(PREDICT_API_KEY="main-key", PREOPEN_API_KEY="pre-key", RULE_API_KEY="rule-key")
        one_api.api_key, one_api.low = "pre-key", True

        async def never(*args):
            raise AssertionError("lane_wait with one key")
        one.lane_wait = never
        seen = []

        async def answer(client):
            seen.append(client)
            return "ok"
        bucket("pre-key", 0.0)
        self.assertEqual(await one.through(one_api, answer), "ok")
        self.assertEqual(seen, [one_api])
        await self.finish(one)

    async def test_a_further_keys_refusal_sends_the_request_on_the_first(self):
        ctl, api, clock, e2, e3, lent = self.mode()
        told = []
        ctl.owner.live_notify = lambda text, key="": told.append((key, text))
        seen = []

        async def answer(client, error=None):
            seen.append(client)
            if error is not None and client is not api:
                raise error
            return "ok"
        bucket("pre-key", 0.0), bucket("k2", 8.0), bucket("k3", 8.0), bucket("rule-key", 8.0)
        # a 429 (the client's own cooldown is what keeps the lane off it), and the cooldown's own refusal: nothing more is marked
        for error in (m.RemoteError("HTTP 429: Too Many Requests"), m.RemoteError("Predict 接口限流冷却中（3 秒后重试）", 3)):
            seen.clear()
            self.assertEqual(await ctl.through(api, lambda client: answer(client, error)), "ok")
            self.assertEqual((seen, ctl.more_off, told), ([e2, api], {}, []))
        self.assertEqual(ctl.lending.why(), "", "the lending of the rule taker's key is not paused by another key's refusal")
        # a 401 / 403 — a key withdrawn, or not this account's: left alone for ten minutes, and said
        seen.clear()
        self.assertEqual(await ctl.through(api, lambda client: answer(client, m.RemoteError("HTTP 401: unauthorized"))), "ok")
        self.assertEqual(seen, [e2, api])
        self.assertEqual((ctl.more_ready(), ctl.lane(api)), ([e3], e3))
        self.assertEqual(len(told), 1)
        self.assertEqual(told[0][0], "preopen:key:2")
        self.assertIn("第 2 把 Key（PREOPEN_API_KEYS 里的第 1 把）拒绝了请求（HTTP 401）", told[0][1])
        self.assertIn("第 2 把 0 次（拒绝了请求（HTTP 401），停用中）", "、".join(ctl.more_usage()))
        # any other failure is the request's own: raised, never sent again (an order may be on the book)
        seen.clear()
        with self.assertRaises(m.RemoteError):
            await ctl.through(api, lambda client: answer(client, m.RemoteError("timeout: delivery unknown")))
        self.assertEqual(seen, [e3])
        # the borrowed key's refusal still pauses the borrowing, as before
        bucket("k3", 0.0)
        seen.clear()
        self.assertEqual(await ctl.through(api, lambda client: answer(client, m.RemoteError("HTTP 429: Too Many Requests"))), "ok")
        self.assertEqual((seen, ctl.lending.off_why), ([lent, api], "那把 Key 被限流（429）"))
        await self.finish(ctl)

    async def test_a_pairs_orders_go_out_on_a_further_key(self):
        ctl, api, clock, e2, e3, lent = self.mode()
        c = p.candidate(T.market(), T.NOW, ctl.owner.config, ctl.settings)
        bucket("pre-key", 0.0), bucket("k2", 8.0), bucket("k3", 8.0)
        self.assertEqual(await ctl.place_pair({**c, "market": T.market(), "read_ms": T.NOW}), "")
        self.assertEqual((len(e2.sent), len(api.sent), len(e3.sent), len(lent.sent)), (2, 0, 0, 0), "both orders on the further key")
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual((pair["phase"], {side: order["state"] for side, order in pair["orders"].items()}), ("active", {"up": "open", "down": "open"}))
        self.assertEqual(len(ctl.placed_at), 2, "counted among the orders Predict accepted (the status's 速度)")
        # a second window with the further keys in their cooldown and the first key short: on the first key, as ever (it waits there)
        e2.blocked_until = e3.blocked_until = time.monotonic() + 20
        ctl.lent_api = None
        bucket("pre-key", 8.0)
        later = T.market(at=T.NOW + 45 * 60_000, mid=456)
        api.markets[str(later["id"])] = later
        c2 = p.candidate(later, T.NOW, ctl.owner.config, ctl.settings)
        self.assertIsNotNone(c2)
        self.assertEqual(await ctl.place_pair({**c2, "market": later, "read_ms": T.NOW}), "")
        self.assertEqual((len(api.sent), len(e2.sent)), (2, 2))
        await self.finish(ctl)

    async def test_a_cancel_does_not_wait_out_the_first_keys_cooldown(self):
        ctl, api, clock, e2, e3, lent = self.mode()
        c2, c3 = stand_in(ctl, api, "k2"), stand_in(ctl, api, "k3")
        ctl.more_cancel = {id(e2): c2, id(e3): c3}
        self.assertIs(ctl.cancel_now(), api)
        ctl.cancel_made = ctl.cancel_api  # (the mode's own cancel client: the tests' stand-in taken for it)
        self.assertIs(ctl.cancel_now(), api, "no cooldown: the mode's own")
        api.blocked_until = time.monotonic() + 20
        self.assertIs(ctl.cancel_now(), c2, "the first key cooling down: the first further key that is ready")
        e2.blocked_until = time.monotonic() + 20
        self.assertIs(ctl.cancel_now(), c3)
        e3.jwt = ""
        self.assertIs(ctl.cancel_now(), api, "none ready: its own, which waits the cooldown out as before")
        e2.blocked_until, e3.jwt, api.blocked_until = 0.0, "t", 0.0
        # a window at its cancel time while the first key waits out a 429: its orders are withdrawn through the further key
        bucket("pre-key", 8.0)
        c = p.candidate(T.market(), T.NOW, ctl.owner.config, ctl.settings)
        self.assertEqual(await ctl.place_pair({**c, "market": T.market(), "read_ms": T.NOW}), "")
        pair = ctl.ledger.rows[c["slug"]]
        ids = sorted(order["id"] for order in pair["orders"].values())
        c2.rows = api.rows  # (one Predict behind them all)
        api.blocked_until = time.monotonic() + 20
        result = await ctl.cancel_many([pair], "到时撤单或真实交易暂停", free=True)
        self.assertEqual((sorted(c2.removed), api.removed, sorted(result["removed"])), (ids, [], ids))
        self.assertEqual({order["state"] for order in pair["orders"].values()}, {"removed"})
        await self.finish(ctl)

    async def test_the_sign_ins_are_kept(self):
        ctl, api, clock, e2, e3, lent = self.mode()
        c2 = stand_in(ctl, api, "k2")
        ctl.more_cancel = {id(e2): c2}
        asked = []
        for name, client in (("e2", e2), ("e3", e3), ("c2", c2)):
            async def signed(name=name):
                asked.append(name)
            client.auth = signed
        e2.jwt = ""
        self.assertEqual(ctl.more_ready(), [e3], "not used until it is signed in: no order waits for a sign-in")
        ctl.more_keep_warm()
        await ctl.more_warm
        self.assertEqual((asked, ctl.more_warm_error), (["e2", "c2", "e3"], ""), "each key's client, and its cancel client")
        task = ctl.more_warm
        ctl.more_keep_warm()
        self.assertIs(ctl.more_warm, task, "not again within the minute")

        async def down():
            raise m.RemoteError("HTTP 503: sign-in down")
        e2.auth, ctl.more_warmed = down, -1e9
        ctl.more_keep_warm()
        await ctl.more_warm
        self.assertEqual(ctl.more_warm_error, "HTTP 503: sign-in down")
        self.assertIn("第 2 把 0 次（还没登录上：HTTP 503: sign-in down）", "、".join(ctl.more_usage()))
        self.assertEqual(asked[-2:], ["c2", "e3"], "one key's sign-in failing keeps none of the others from theirs")
        await self.finish(ctl)

    async def test_what_is_said_of_each_key(self):
        ctl, api, clock, e2, e3, lent = self.mode()
        now = time.monotonic()
        for key, n in (("pre-key", 200), ("k2", 150), ("k3", 40), ("main-key", 9), ("rule-key", 5)):
            m.predict_pacer(key).sent.extend([now] * n)
        m.predict_pacer("k3").note_429()
        e3.blocked_until = now + 20
        self.assertEqual(ctl.more_usage(), ["第 2 把 150 次", "第 3 把 40 次（限流冷却中）（10 分钟内 429 1 次）"])
        line = ctl.speed_line()
        self.assertIn("预挂专用 Key 最近 1 分钟发出 200 次请求", line)
        self.assertIn("；预挂另外的 Key：第 2 把 150 次、第 3 把 40 次（限流冷却中）（10 分钟内 429 1 次）；", line)
        self.assertIn("用预挂自己的 3 把 API Key（PREOPEN_API_KEY 和 PREOPEN_API_KEYS 的 2 把：第一把没有名额时依次用后面的，各有各的额度），请求每分钟最多 220 次（PREDICT_RATE_PER_MIN", ctl.pace_line())
        self.assertIn("\n速度：", ctl.status())
        # /diag: each key's minute by name, never the key
        ns = types.SimpleNamespace(config=ctl.owner.config, KEY_SHARED_BELOW=L.LiveBot.KEY_SHARED_BELOW)
        ns.preopen_own_key = lambda: L.LiveBot.preopen_own_key(ns)
        ns.book_own_key = lambda: L.LiveBot.book_own_key(ns)
        load = L.LiveBot.live_key_load(ns)
        self.assertTrue(load.startswith("主 Key 9 次、预挂专用 Key 200 次、预挂专用 Key 2 150 次、预挂专用 Key 3 40 次（24 小时内 429 1 次"), load)
        self.assertIn("规则吃单专用 Key 5 次", load)
        self.assertFalse(any(key in load for key in ("pre-key", "k2", "k3", "main-key", "rule-key")))
        # the speed test watches them as the bot's other keys: a 429 on one of them ends it
        names = [name for name, _ in ctl.speed_others(m.predict_pacer("pre-key"))]
        self.assertEqual(names, ["主 Key", "规则吃单 Key", "预挂第 2 把 Key", "预挂第 3 把 Key"])
        await self.finish(ctl)
        one, one_api, _ = T.controller(PREDICT_API_KEY="main-key", PREOPEN_API_KEY="pre-key")
        self.assertIn("用预挂自己的 API Key（PREOPEN_API_KEY），请求", one.pace_line())
        self.assertNotIn("预挂另外的 Key", one.speed_line())
        await self.finish(one)


if __name__ == "__main__":
    result = unittest.main(exit=False).result
    if result.wasSuccessful():
        print("MOREKEYS_OK")
    else:
        sys.exit(1)
