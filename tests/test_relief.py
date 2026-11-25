"""1.154.45 — the model trader's reads on the rule taker's idle key (live.ReadRelief, LIVE_READ_LEND).

11-25 17:52, production: 模拟交易 and Predict 盘口 both timed out at 300 s, 主 Key 220 次（等节拍 33 次共 1257 秒）. Every re-quote
is three requests on the bot's key — the cancel, the cancelled order's fills read by its hash, the new order — and orders
and cancels take their token ahead of the reads (main.PredictPacer): at 1.3 re-quotes a second they ask for more than the
key's 3.67 a second, and everything left queuing is a read — the fills, the sync's open list, the positions. The rule
taker's own key, idle outside its hours, was already lent to the orderbook feed and the pre-open mode (preopen.KeyLending).

Here: a read of the bot's own client goes out on that key when — and only when — the bot's key has had no token for it for
ReadRelief.WAIT_S, the key is lent, and the bot is signed in on it; an order or a cancel never does; a list that has moved
there stays there; that key's refusal pauses the borrowing and the read is asked on the bot's key, any other failure is
the read's own; the sign-in on that key is made beside the read that would have used it; /diag counts them; and, with the
real pacer under more orders and cancels than the key has tokens, the reads that starved go through. Entirely offline.

Run: python tests/test_relief.py
"""
import asyncio
import json
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

KEYS = {"PREDICT_API_KEY": "main-key", "PREOPEN_API_KEY": "pre-key", "RULE_API_KEY": "rule-key"}
HASH = "0x" + "ab" * 32


def bucket(key: str, tokens: float) -> m.PredictPacer:
    """A key's pacer holding ``tokens`` now (the pace slow enough that it stays so for the test)."""
    pacer = m.predict_pacer(key)
    pacer.tokens, pacer.at = tokens, time.monotonic()
    return pacer


def rule_taker(busy):
    """A rule taker as the lending asks it: ``busy`` (a list of one) ms until it needs its key at speed."""
    return types.SimpleNamespace(alive=True, api=types.SimpleNamespace(blocked_until=0.0), now=lambda: 0, busy_in=lambda now: busy[0])


def make_bot(**env) -> L.LiveBot:
    # (PREDICT_RATE_PER_MIN: a bot sets the keys' pace as it is made — a token every ten seconds, so a bucket set by hand stays as set)
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "LIVE": "on", "PREDICT_PRIVATE_KEY": "0x" + "a" * 64, "PREDICT_RATE_PER_MIN": "6", **KEYS, **env})
    bot = L.LiveBot(cfg, m.Store(":memory:"), types.SimpleNamespace(now_ms=lambda: T.NOW, config=None), None)
    bot.live.api.jwt = "home-token"  # signed in on its own key
    bot.live_relief.WAIT_S = 0.03    # (two seconds in production: how long a read waits on the bot's key before it is borrowed for)
    return bot


def order_row(order_id: str = "77", status: str = "CANCELLED") -> dict:
    return {"id": order_id, "status": status, "amount": "100", "amountFilled": "0", "order": {"hash": HASH}}


class Wire:
    """Predict, as the clients' send() meets it: every request noted (key, method, path, token); the answers by key."""

    def __init__(self):
        self.sent: list[tuple[str, str, str, str]] = []
        self.fail: dict[str, list] = {}   # key -> errors raised, one a request of that key's GETs of orders, in turn
        self.before: list = []            # called with (key, method, path) before each answer
        self.sign_in = True               # whether a sign-in is answered with a token
        self.pages: dict[str, tuple[list, str]] = {}  # cursor asked with ("" the first page) -> (rows, the next cursor)

    def answer(self, key: str, method: str, path: str) -> dict:
        if path == "/auth/message":
            return {"data": {"message": "sign this"}}
        if path == "/auth":
            return {"data": {"token": f"token-of-{key}"}} if self.sign_in else {"data": {}}
        if method == "GET" and path.startswith("/orders/"):
            return {"success": True, "data": order_row()}
        if method == "GET" and path.startswith("/markets/"):
            return {"success": True, "data": {"id": 5, "title": "a market"}}
        if method == "GET" and path.startswith(("/orders?", "/positions?")):
            after = dict(part.split("=", 1) for part in path.split("?", 1)[1].split("&")).get("after", "")
            rows, cursor = self.pages.get(after, ([], ""))
            return {"success": True, "data": rows, **({"cursor": {"after": cursor}} if cursor else {})}
        if method == "POST" and path == "/orders":
            return {"success": True, "data": {"orderId": "901", "orderHash": HASH}}
        if method == "POST" and path == "/orders/remove":
            return {"success": True, "removed": ["77"], "noop": []}
        raise AssertionError(f"a request the test did not expect: {method} {path}")

    def install(self):
        wire = self

        async def send(api, method, path, payload, timeout):
            key = api.headers().get("x-api-key", "")
            wire.sent.append((key, method, path.split("?")[0] if not path.startswith(("/orders?", "/positions?")) else path, api.headers().get("Authorization", "")))
            for hook in wire.before:
                hook(key, method, path)
            if method == "GET" and path.startswith(("/orders", "/positions", "/markets")) and wire.fail.get(key):
                raise wire.fail[key].pop(0)
            return json.dumps(wire.answer(key, method, path)).encode()
        self.original = L.PredictApi.send
        L.PredictApi.send = send
        return self

    def remove(self):
        L.PredictApi.send = self.original

    def keys(self, method: str = "GET", start: str = "/orders") -> list[str]:
        return [key for key, how, path, _ in self.sent if how == method and path.startswith(start)]


class ReliefTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.kept = (m.PREDICT_PACING, m.PREDICT_RATE["per_min"], dict(m.PREDICT_PACERS))
        m.PREDICT_PACING, m.PREDICT_RATE["per_min"] = True, 6.0  # a token every ten seconds: a bucket set by hand stays as set
        m.PREDICT_PACERS.clear()
        self.wire = Wire().install()

    def tearDown(self):
        self.wire.remove()
        m.PREDICT_PACING, m.PREDICT_RATE["per_min"] = self.kept[0], self.kept[1]
        m.PREDICT_PACERS.clear()
        m.PREDICT_PACERS.update(self.kept[2])

    def signed(self, bot) -> L.LentApi:
        """The bot's client on the lent key, signed in there."""
        client = bot.live_relief.made()
        client.jwt = "lent-token"
        return client

    async def test_a_read_goes_to_the_idle_key_only_when_the_bots_key_has_no_token(self):
        bot = make_bot()
        api, relief = bot.live.api, bot.live_relief
        self.assertIs(api.relief, relief)
        lent = self.signed(bot)
        self.assertTrue(isinstance(lent, L.LentApi) and lent.api_key == "rule-key" and lent.wallet is api.wallet and lent is relief.made(), "one client, made once")
        # a token on the bot's key: there, as ever — whatever the other key holds
        home, there = bucket("main-key", 2.0), bucket("rule-key", 8.0)
        began = time.monotonic()
        row = await api.order("77", HASH)
        self.assertEqual((row["id"], self.wire.sent), ("77", [("main-key", "GET", f"/orders/{HASH}", "Bearer home-token")]))
        self.assertTrue(abs(home.tokens - 1.0) < 0.01 and abs(there.tokens - 8.0) < 0.01 and relief.lent_last() == 0 and time.monotonic() - began < relief.WAIT_S)
        # the bot's key in debt to its orders and cancels: the read waits there WAIT_S, gets none, and goes out on the idle key —
        # signed in there, at that key's cost; the bot's own client sent nothing for it
        self.wire.sent.clear()
        home, there = bucket("main-key", -3.0), bucket("rule-key", 8.0)
        began = time.monotonic()
        row = await api.order("77", HASH)
        self.assertGreaterEqual(time.monotonic() - began, relief.WAIT_S)
        self.assertEqual((row["id"], self.wire.sent), ("77", [("rule-key", "GET", f"/orders/{HASH}", "Bearer lent-token")]))
        self.assertTrue(abs(home.tokens + 3.0) < 0.02 and abs(there.tokens - 7.0) < 0.02, (home.tokens, there.tokens))
        self.assertEqual((relief.lent_last(), api.calls, lent.calls), (1, 1, 1))
        # ...a whole token there, as the rule taker's own reads: the low borrowers (the feed, the pre-open mode) leave it two
        bucket("rule-key", 1.2)
        await api.order("77", HASH)
        self.assertEqual(self.wire.keys()[-1], "rule-key")
        # a token that comes within the wait: the bot's key after all, nothing borrowed
        self.wire.sent.clear()
        relief.WAIT_S = 0.5
        m.PREDICT_RATE["per_min"] = 6000.0  # (a token every 10 ms)
        bucket("main-key", -1.0), bucket("rule-key", 8.0)
        began = time.monotonic()
        await api.order("77", HASH)
        self.assertEqual(self.wire.keys(), ["main-key"])
        self.assertTrue(0.01 <= time.monotonic() - began < 0.4 and relief.lent_last() == 2)
        m.PREDICT_RATE["per_min"], relief.WAIT_S = 6.0, 0.03
        # an order and a cancel never: the bot's key, where they wait for nothing (a burst ahead of the reads)
        self.wire.sent.clear()
        bucket("main-key", -3.0), bucket("rule-key", 8.0)
        made = await api.create_order({"data": {}})
        gone = await api.remove_orders(["77"])
        self.assertEqual((made["order_id"], gone["removed"]), ("901", ["77"]))
        self.assertEqual([(key, how, path) for key, how, path, _ in self.wire.sent], [("main-key", "POST", "/orders"), ("main-key", "POST", "/orders/remove")])
        # what Predict knows nothing of is "nothing" on either key (a 404 is no refusal of the key's: the borrowing goes on)
        self.wire.fail["rule-key"] = [m.RemoteError("HTTP 404: not found")]
        bucket("main-key", -3.0), bucket("rule-key", 8.0)
        self.assertIsNone(await api.order("77", HASH))
        self.assertEqual((bot.key_lending.why(), relief.why(), self.wire.keys()[-1]), ("", "", "rule-key"))
        # a market's details (no sign-in asked of the read): as the other reads — but one on an order's way keeps the bot's key, ahead of its reads
        self.wire.sent.clear()
        bucket("main-key", -3.0), bucket("rule-key", 8.0)
        self.assertEqual((await api.market("5"))["id"], 5)
        self.assertEqual((await api.market("5", priority=True))["id"], 5)
        self.assertEqual([(key, path) for key, _, path, _ in self.wire.sent], [("rule-key", "/markets/5"), ("main-key", "/markets/5")])

    async def test_the_wait_for_a_token_is_given_up_before_anything_is_sent(self):
        api = L.PredictApi("https://x", "kp", None)
        bucket("kp", -3.0)
        token = L.TOKEN_WAIT.set(0.03)
        try:
            began = time.monotonic()
            with self.assertRaises(L.NoToken):
                await api.call("GET", "/orders/0x1")
            self.assertTrue(0.03 <= time.monotonic() - began < 0.5 and api.calls == 0 and self.wire.sent == [], "nothing went out")
            self.assertTrue(isinstance(L.NoToken("x"), m.RemoteError) and abs(m.predict_pacer("kp").tokens + 3.0) < 0.02, "no token taken")
            await api.call("POST", "/orders", {}, priority=True)  # an order is not a read: it takes its token ahead, as ever
            bucket("kp", 1.0)
            await api.call("GET", "/auth/message")  # (a sign-in's requests are not given up: they wait for theirs)
            self.assertEqual([(how, path) for _, how, path, _ in self.wire.sent], [("POST", "/orders"), ("GET", "/auth/message")])
        finally:
            L.TOKEN_WAIT.reset(token)
        self.assertIsNone(L.TOKEN_WAIT.get())

    async def test_when_nothing_is_borrowed(self):
        bucket("main-key", -3.0), bucket("rule-key", 8.0)
        bot = make_bot()
        relief, lent = bot.live_relief, self.signed(bot)
        self.assertIs(relief.lane(), lent)
        # the rule taker needs its key (now, or within five minutes): its alone
        busy = [0]
        bot.rules = rule_taker(busy)
        self.assertIsNone(relief.lane())
        busy[0] = p.LEND_BEFORE_MS - 1
        self.assertIsNone(relief.lane())
        busy[0] = p.LEND_BEFORE_MS + 1
        self.assertIs(relief.lane(), lent)
        # that key in its cooldown (a 429 of the rule taker's own), or the borrowing paused after a refusal
        bot.rules.api.blocked_until = time.monotonic() + 20
        self.assertIsNone(relief.lane())
        bot.rules.api.blocked_until = 0.0
        bot.key_lending.off("那把 Key 被限流（429）", 30)
        self.assertIsNone(relief.lane())
        bot.key_lending.off_until = 0.0
        self.assertIs(relief.lane(), lent)
        # ...or this client's own pause there (a 401 / 403 answered to it): the key is still lent to the others
        relief.off_until, relief.off_why = time.monotonic() + 30, "那把 Key 拒绝了原策略的读取（HTTP 403）"
        self.assertTrue(relief.lane() is None and bot.key_lending.why() == "" and "HTTP 403" in relief.why())
        relief.off_until = 0.0
        self.assertIs(relief.lane(), lent)
        # the switches, and a key that is not the rule taker's alone
        for env, why in (({"LIVE_READ_LEND": "off"}, "its own switch"), ({"RULE_KEY_LEND": "off"}, "the lending off"),
                         ({"RULE_API_KEY": ""}, "no key of the rule taker's own"), ({"RULE_API_KEY": "main-key"}, "the bot's own key")):
            other = make_bot(**env)
            self.assertTrue(other.live_relief.made() is None and other.live_relief.lane() is None, why)
        self.assertTrue(make_bot().config.live_read_lend and not make_bot(LIVE_READ_LEND="off").config.live_read_lend)
        # a stand-in for the bot's client (the tests') borrows nothing
        bot.live.api = types.SimpleNamespace(api_key="main-key")
        self.assertTrue(relief.made() is None and relief.lane() is None)
        self.assertEqual(self.wire.sent, [], "asking sends nothing")

    async def test_the_sign_in_on_that_key_is_made_beside_the_read(self):
        bot = make_bot()
        api, relief = bot.live.api, bot.live_relief
        lent = relief.made()
        self.assertFalse(lent.fresh())
        # a token on the bot's key: nothing asked of the other key, no sign-in either
        bucket("main-key", 2.0), bucket("rule-key", 8.0)
        self.assertTrue(relief.lane() is None and relief.warm_task is None)
        await api.order("77", HASH)
        self.assertTrue(self.wire.keys() == ["main-key"] and relief.warm_task is None)
        self.wire.sent.clear()
        # the bot's key dry: the read stays there (it does not wait for a sign-in) and the sign-in is made beside it
        bucket("main-key", -3.0)
        self.assertIsNone(relief.lane())
        task = relief.warm_task
        self.assertIsNotNone(task)
        self.assertIsNone(relief.lane())
        self.assertIs(relief.warm_task, task, "one sign-in at a time")
        await task
        self.assertEqual([(key, how, path) for key, how, path, _ in self.wire.sent], [("rule-key", "GET", "/auth/message"), ("rule-key", "POST", "/auth")])
        self.assertTrue(lent.fresh() and lent.jwt == "token-of-rule-key" and relief.warm_error == "" and api.jwt == "home-token")
        self.assertIs(relief.lane(), lent)
        # a token about to expire: no longer borrowed on, signed in again — not more often than WARM_S
        lent.jwt_exp = int(time.time()) + 60
        self.wire.sent.clear()
        self.assertIsNone(relief.lane())
        self.assertIs(relief.warm_task, task, "tried a moment ago: not yet again")
        relief.warmed = time.monotonic() - relief.WARM_S - 1
        self.assertIsNone(relief.lane())
        self.assertIsNot(relief.warm_task, task)
        await relief.warm_task
        self.assertTrue(lent.fresh() and len(self.wire.sent) == 2)
        # a sign-in that fails: said (/diag), nothing borrowed, tried again WARM_S on; the lending itself is not paused for it
        lent.jwt, self.wire.sign_in = "", False
        relief.warmed = -1e9
        self.assertIsNone(relief.lane())
        await relief.warm_task
        self.assertTrue(relief.warm_error and not lent.fresh() and bot.key_lending.why() == "", relief.warm_error)
        failed = relief.warm_task
        self.assertTrue(relief.lane() is None and relief.warm_task is failed)
        # not while the key is the rule taker's alone
        relief.warmed = -1e9
        bot.rules = rule_taker([0])
        self.assertTrue(relief.lane() is None and relief.warm_task is failed)

    async def test_that_keys_refusal_pauses_the_borrowing_and_the_read_is_asked_on_the_bots_key(self):
        for error, everyone, word in ((m.RemoteError("HTTP 429: too many requests"), True, "被限流（429）"),
                                      (m.RemoteError("Predict 接口限流冷却中（5 秒后重试）", 5), True, "被限流（429）"),
                                      (m.RemoteError("HTTP 403: forbidden"), False, "拒绝了原策略的读取（HTTP 403）"),
                                      (m.RemoteError("HTTP 401: unauthorized"), False, "拒绝了原策略的读取（HTTP 401）")):
            m.PREDICT_PACERS.clear()
            bot = make_bot()
            bot.rules = rule_taker([10 ** 9])
            api, relief = bot.live.api, bot.live_relief
            self.signed(bot)
            bucket("main-key", -3.0), bucket("rule-key", 8.0)
            self.wire.sent.clear()
            self.wire.fail["rule-key"] = [error]
            self.wire.before[:] = [lambda key, how, path: bucket("main-key", 2.0) if key == "rule-key" else None]  # (the bot's key has one again by then)
            row = await api.order("77", HASH)
            self.assertEqual((row["id"], [(key, how) for key, how, _, _ in self.wire.sent]), ("77", [("rule-key", "GET"), ("main-key", "GET")]),
                             "asked once more, on the bot's own key — and no sign-in made on the read's way")
            self.assertEqual(relief.client.jwt, "" if "401" in str(error) else "lent-token", "the token it was refused with is not used again")
            self.assertIsNone(relief.lane(), "nothing more is borrowed for a while")
            if everyone:  # the key's own limit: the lending's rule — nobody borrows there for a while
                self.assertTrue(word in bot.key_lending.why() and relief.why() == "", (bot.key_lending.why(), relief.why()))
            else:         # this client's sign-in refused there: its own pause; the feed and the pre-open mode borrow on
                self.assertTrue(word in relief.why() and bot.key_lending.why() == "", (bot.key_lending.why(), relief.why()))
        self.wire.before.clear()
        # any other failure is the read's own: raised as it is, not asked again, nothing paused
        for error in (m.RemoteError("HTTP 500: upstream"), m.RemoteError("Predict 请求超时")):
            m.PREDICT_PACERS.clear()
            bot = make_bot()
            api = bot.live.api
            self.signed(bot)
            bucket("main-key", -3.0), bucket("rule-key", 8.0)
            self.wire.sent.clear()
            self.wire.fail["rule-key"] = [error]
            with self.assertRaises(m.RemoteError) as caught:
                await api.order("77", HASH)
            self.assertIs(caught.exception, error)
            self.assertEqual((self.wire.keys(), bot.key_lending.why(), bot.live_relief.why()), (["rule-key"], "", ""))

    async def test_one_key_one_cooldown(self):
        bot = make_bot()
        bot.rules = rule_taker([10 ** 9])
        api, lent = bot.live.api, self.signed(bot)
        self.assertEqual(lent.blocked_until, 0.0)
        # the rule taker's own 429: the bot's client on that key is in the same cooldown — nothing goes out there
        bot.rules.api.blocked_until = time.monotonic() + 20
        self.assertGreater(lent.blocked_until, time.monotonic() + 10)
        bucket("rule-key", 8.0)
        with self.assertRaises(m.RemoteError):
            await lent.call("GET", "/orders/0x1")
        self.assertEqual(self.wire.sent, [])
        bot.rules.api.blocked_until = 0.0
        # a 429 answered to the bot's read there (the real send): the rule taker's client pauses with it, the key's minute is noted,
        # the borrowing stops — and the read is asked on the bot's own key
        self.wire.remove()
        real, asked = m._http_get, []

        def http_get(url, payload=None, timeout=0, headers=None, **kwargs):
            asked.append((headers or {}).get("x-api-key"))
            if asked[-1] == "rule-key":
                bucket("main-key", 2.0)  # (the bot's key has a token again by the time it is asked there)
                raise m.RemoteError("HTTP 429: too many requests")
            return json.dumps({"success": True, "data": order_row()}).encode()
        m._http_get = http_get
        try:
            bucket("main-key", -3.0), bucket("rule-key", 8.0)
            row = await api.order("77", HASH)
        finally:
            m._http_get = real
            self.wire.install()
        self.assertEqual((row["id"], asked), ("77", ["rule-key", "main-key"]))
        self.assertGreater(bot.rules.api.blocked_until, time.monotonic() + 10, "one key, one limit: the rule taker's client pauses with it")
        self.assertTrue(len(m.predict_pacer("rule-key").limits) == 1 and "被限流" in bot.key_lending.why() and api.blocked_until == 0.0)

    async def test_a_lists_pages_stay_on_one_key(self):
        rows = [order_row(str(n), "OPEN") for n in range(120)]
        self.wire.pages = {"": (rows[:50], "c1"), "c1": (rows[50:100], "c2"), "c2": (rows[100:], "")}
        bot = make_bot()
        api, relief = bot.live.api, bot.live_relief
        self.signed(bot)
        # its first page finds the bot's key dry and goes to the idle key: the pages after it too, though the bot's key has tokens again
        bucket("main-key", -3.0), bucket("rule-key", 8.0)
        self.wire.before[:] = [lambda key, how, path: bucket("main-key", 5.0)]
        got = await api.orders("OPEN")
        self.assertEqual(([r["id"] for r in got], self.wire.keys()), ([str(n) for n in range(120)], ["rule-key"] * 3))
        self.assertTrue(api.orders_complete and api.paged_stats["/orders"] == (3, 50) and relief.lent_last() == 3, "the read's own figures are the bot's client's")
        # begun with a token on the bot's key: every page there
        self.wire.sent.clear()
        self.wire.before.clear()
        bucket("main-key", 3.5), bucket("rule-key", 8.0)
        got = await api.orders("OPEN")
        self.assertEqual((len(got), self.wire.keys()), (120, ["main-key"] * 3))
        # the idle key refuses the second page: that page and the rest on the bot's key, every row once
        self.wire.sent.clear()
        bucket("main-key", -3.0), bucket("rule-key", 8.0)
        calls = {"n": 0}

        def refuse_second(key, how, path):
            if key == "rule-key" and how == "GET":
                calls["n"] += 1
                bucket("main-key", 5.0)  # (the bot's key has tokens again by then)
                if calls["n"] == 2:
                    self.wire.fail["rule-key"] = [m.RemoteError("HTTP 429: too many requests")]
        self.wire.before[:] = [refuse_second]
        got = await api.orders("OPEN")
        self.assertEqual(([r["id"] for r in got], self.wire.keys()), ([str(n) for n in range(120)], ["rule-key", "rule-key", "main-key", "main-key"]))
        self.assertTrue(api.orders_complete and "被限流" in bot.key_lending.why())
        # a list begun on the bot's key whose later page finds no token there: that page and the rest on the idle key
        self.wire.before.clear()
        self.wire.sent.clear()
        bot.key_lending.off_until = 0.0
        bucket("main-key", 1.0), bucket("rule-key", 8.0)
        got = await api.orders("OPEN")
        self.assertEqual(([r["id"] for r in got], self.wire.keys()), ([str(n) for n in range(120)], ["main-key", "rule-key", "rule-key"]))
        # the positions' pages likewise (the minute's check)
        self.wire.before.clear()
        self.wire.sent.clear()
        bot.key_lending.off_until = 0.0
        bucket("main-key", -3.0), bucket("rule-key", 8.0)
        self.wire.pages = {"": ([{"market": {"id": 5}, "outcome": {"indexSet": 1}, "amount": "3"}], "")}
        held = await api.positions()
        self.assertEqual((len(held), self.wire.keys(start="/positions")), (1, ["rule-key"]))

    async def test_a_client_with_no_relief_reads_as_before(self):
        # the pre-open mode's clients, the rule taker's, a test's stand-in for authed: no relief, the same one call
        api = L.PredictApi("https://x", "kp", None)
        self.assertIsNone(api.relief)
        asked = []

        async def authed(method, path, payload=None, timeout=0, priority=False):
            asked.append((method, path, payload, priority))
            return {"success": True, "data": order_row()}
        api.authed = authed
        self.assertEqual((await api.order("77", HASH))["id"], "77")
        self.assertEqual(asked, [("GET", f"/orders/{HASH}", None, False)])
        self.assertEqual(await L.relieved(api, "/orders/0x1"), ({"success": True, "data": order_row()}, None))
        # ...and an object that only borrows the client's paging (a test's stand-in: no relief attribute at all)
        stand_in = types.SimpleNamespace(authed=authed)
        self.assertEqual((await L.relieved(stand_in, "/orders/0x2"))[1], None)
        self.assertEqual(asked[-1], ("GET", "/orders/0x2", None, False))

    async def test_diag_counts_the_traders_reads_on_that_key(self):
        bot = make_bot()
        now = time.monotonic()
        for key, n in (("main-key", 220), ("rule-key", 221)):
            m.predict_pacer(key).sent.extend([now] * n)
        bot.predict.lent_at.extend([now] * 20)
        bot.preopen = types.SimpleNamespace(lent_last=lambda: 131)
        bot.live_relief.sent.extend([now] * 88)
        line = bot.live_key_load()
        self.assertIn("规则吃单专用 Key 221 次（其中原策略借用 88 次、盘口借用 20 次、预挂借用 131 次；空闲，借给盘口和预挂用，原策略的读请求在主 Key 上等不到时也走这里）", line)
        bot.live_relief.warm_error = "登录未返回 token"
        self.assertIn("也走这里；原策略现在不借——在这把 Key 上没登录成：登录未返回 token）", bot.live_key_load())
        bot.live_relief.off_until, bot.live_relief.off_why = time.monotonic() + 590, "那把 Key 拒绝了原策略的读取（HTTP 403）"
        self.assertIn("也走这里；原策略现在不借——那把 Key 拒绝了原策略的读取（HTTP 403），10 分钟内不借）", bot.live_key_load())
        # the switch off: the line as it was before
        off = make_bot(LIVE_READ_LEND="off")
        off.predict.lent_at.extend([now] * 20)
        off.preopen = types.SimpleNamespace(lent_last=lambda: 131)
        self.assertIn("规则吃单专用 Key 221 次（其中盘口借用 20 次、预挂借用 131 次；空闲，借给盘口和预挂用）", off.live_key_load())

    async def test_the_reads_that_starved_behind_the_orders_go_through(self):
        """The real pacer, a hundred requests a second a key: orders and cancels asked without end keep the bot's key in debt —
        as the re-quotes did at 1.3 a second — and its reads never get a token there; lent the idle key, all of them do."""
        async def run(env: dict) -> tuple[int, list[str]]:
            m.PREDICT_PACERS.clear()
            bot = make_bot(PREDICT_RATE_PER_MIN="6000", **env)
            bot.live_relief.WAIT_S = 0.05
            api = bot.live.api
            if bot.live_relief.made() is not None:
                self.signed(bot)
            home = m.predict_pacer("main-key")
            stop = asyncio.Event()

            async def writes() -> None:  # the orders and cancels: each takes its token the moment the key may give one ahead
                while not stop.is_set():
                    await home.take(write=True)
            writer = asyncio.create_task(writes())
            await asyncio.sleep(0.05)
            self.assertLess(home.tokens, 0, "the bot's key is in debt to its orders")
            self.wire.sent.clear()
            reads = [asyncio.create_task(api.order(str(n), HASH)) for n in range(20)]
            done, pending = await asyncio.wait(reads, timeout=0.6)
            stop.set()
            for task in [*pending, writer]:
                task.cancel()
            await asyncio.gather(*pending, writer, return_exceptions=True)
            return len(done), self.wire.keys()
        done, keys = await run({})
        self.assertEqual((done, set(keys)), (20, {"rule-key"}), "every read went through, on the idle key")
        done, keys = await run({"LIVE_READ_LEND": "off"})
        self.assertLessEqual(done, 2, "on the bot's key alone they wait behind its orders — as on 11-25")
        self.assertTrue(set(keys) <= {"main-key"})


if __name__ == "__main__":
    result = unittest.main(exit=False, verbosity=0).result
    if not result.wasSuccessful():
        sys.exit(1)
    print("RELIEF_OK")
