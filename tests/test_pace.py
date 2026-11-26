"""The pre-open key's own pace, and the speed the mode quotes at (1.154.57 — the user: "怎么测试速度，看看最多可以挂多少").

PREOPEN_RATE_PER_MIN paces the pre-open mode's key alone (the other keys keep PREDICT_RATE_PER_MIN), and only while that key
is the mode's own; /preopen's 速度 line counts the orders Predict accepted (the last minute, the last ten, the fastest minute
since the start), says whether the key ran at its pace, counts its 429s and where the last run of them began, and repeats
what Predict's own answers say of the limit (main.note_limit_headers). Entirely offline.

Run: python tests/test_pace.py
"""
import asyncio
import dataclasses
import sys
import time
import types
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L
import preopen as p
from test_preopen import NOW, controller, market

BASE = {"TELEGRAM_BOT_TOKEN": "offline"}
KEYS = {"PREDICT_API_KEY": "main-key", "PREOPEN_API_KEY": "pre-key", "RULE_API_KEY": "rule-key", "PREDICT_BOOK_API_KEY": "book-key"}
MIN = 60_000


def refused(env: dict) -> str:
    try:
        m.Config.from_env({**BASE, **env})
    except ValueError as error:
        return str(error)
    raise AssertionError(f"accepted: {env}")


def the_setting() -> None:
    assert m.Config.from_env(BASE).preopen_rate_per_min == 0.0, "not set: the pre-open key is paced as every key"
    assert m.Config.from_env({**BASE, "PREOPEN_RATE_PER_MIN": " 260 "}).preopen_rate_per_min == 260.0
    assert m.Config.from_env({**BASE, "PREOPEN_RATE_PER_MIN": ""}).preopen_rate_per_min == 0.0
    assert m.Config.from_env({**BASE, "PREOPEN_RATE_PER_MIN": "60"}).preopen_rate_per_min == 60.0
    assert m.Config.from_env({**BASE, "PREOPEN_RATE_PER_MIN": "1000"}).preopen_rate_per_min == 1000.0
    for bad in ("59", "1001", "-5"):
        assert "60～1000" in refused({"PREOPEN_RATE_PER_MIN": bad}), bad
    for bad in ("abc", "2.5", "240次"):
        assert "必须是整数" in refused({"PREOPEN_RATE_PER_MIN": bad}), bad
    # in force for a key of the mode's own alone: raised on the bot's key, the feed's or the rule taker's it would try their limit too
    own = m.Config.from_env({**BASE, **KEYS, "PREOPEN_RATE_PER_MIN": "260"})
    assert m.preopen_own_rate(own) == 260.0
    for same in ("main-key", "rule-key", "book-key"):
        assert m.preopen_own_rate(m.Config.from_env({**BASE, **KEYS, "PREOPEN_API_KEY": same, "PREOPEN_RATE_PER_MIN": "260"})) == 0.0, same
    assert m.preopen_own_rate(m.Config.from_env({**BASE, "PREDICT_API_KEY": "main-key", "PREOPEN_RATE_PER_MIN": "260"})) == 0.0, "no key of its own"
    assert m.preopen_own_rate(m.Config.from_env({**BASE, **KEYS})) == 0.0, "not set"
    print("THE_SETTING_OK")


def one_key_alone() -> None:
    kept = (m.PREDICT_RATE["per_min"], dict(m.PREDICT_RATE_OF), dict(m.PREDICT_PACERS))
    try:
        own = m.Config.from_env({**BASE, **KEYS, "PREOPEN_RATE_PER_MIN": "260"})
        m.PREDICT_PACERS.clear()
        m.set_predict_rates(own)
        pre = m.predict_pacer("pre-key")
        assert pre.per_min == 260.0 and m.predict_rate("pre-key") == 260.0
        for other in ("main-key", "rule-key", "book-key", "someone-else"):
            assert m.predict_pacer(other).per_min == 220.0 == m.predict_rate(other), other
        # changed while the bot runs (the control page): the key's pacer — the same one, its tokens and its counts kept — follows at its next request
        pre.sent.append(time.monotonic())
        m.set_predict_rates(dataclasses.replace(own, preopen_rate_per_min=300.0))
        assert m.predict_pacer("pre-key") is pre and pre.per_min == 300.0 and pre.last_minute() == 1
        assert m.predict_pacer("main-key").per_min == 220.0
        m.set_predict_rates(dataclasses.replace(own, preopen_rate_per_min=0.0))  # cleared: every key's pace again
        assert m.predict_pacer("pre-key").per_min == 220.0 and not m.PREDICT_RATE_OF
        m.set_predict_rates(dataclasses.replace(own, predict_rate_per_min=200.0))  # PREDICT_RATE_PER_MIN moves the others; this key keeps its own
        assert (m.predict_pacer("pre-key").per_min, m.predict_pacer("main-key").per_min, m.predict_pacer("rule-key").per_min) == (260.0, 200.0, 200.0)
        # the key not the mode's own: nothing is set for it
        m.set_predict_rates(m.Config.from_env({**BASE, **KEYS, "PREOPEN_API_KEY": "rule-key", "PREOPEN_RATE_PER_MIN": "300"}))
        assert not m.PREDICT_RATE_OF and m.predict_pacer("rule-key").per_min == 220.0
        # both places the pace is set from go through it: the feed's start, and a configuration applied
        m.PREDICT_RATE_OF.clear()
        m.PredictFeed(own)
        assert m.PREDICT_RATE_OF == {"pre-key": 260.0}
        # /diag's line on each key's load names the pace this one has to itself — never the key
        m.PREDICT_PACERS.clear()
        for key, n in (("main-key", 3), ("pre-key", 7)):
            m.predict_pacer(key).sent.extend([time.monotonic()] * n)
        ns = types.SimpleNamespace(config=types.SimpleNamespace(predict_api_key="main-key", preopen_api_key="pre-key"), KEY_SHARED_BELOW=L.LiveBot.KEY_SHARED_BELOW)
        ns.preopen_own_key = lambda: L.LiveBot.preopen_own_key(ns)
        ns.book_own_key = lambda: L.LiveBot.book_own_key(ns)
        line = L.LiveBot.live_key_load(ns)
        assert line.startswith("主 Key 3 次、预挂专用 Key 7 次（单独节流每分钟 260 次：PREOPEN_RATE_PER_MIN）（节流每分钟 220 次") and "pre-key" not in line, line
    finally:
        m.PREDICT_RATE["per_min"] = kept[0]
        m.PREDICT_RATE_OF.clear(); m.PREDICT_RATE_OF.update(kept[1])
        m.PREDICT_PACERS.clear(); m.PREDICT_PACERS.update(kept[2])
    print("ONE_KEY_ALONE_OK")


async def the_pace_is_kept() -> None:
    """The real pacer, the pacing on: the key with its own pace — three times the others' here — lets its requests through
    three times as fast. Measured on the clock, so on a machine that stands still for a moment a look may come out wrong:
    three looks at the most, one that holds is enough (a pacer that ignored the key's own figure fails all three)."""
    kept = (m.PREDICT_PACING, m.PREDICT_RATE["per_min"], dict(m.PREDICT_RATE_OF), dict(m.PREDICT_PACERS))
    try:
        m.PREDICT_PACING = True
        config = dataclasses.replace(m.Config.from_env({**BASE, **KEYS}), predict_rate_per_min=1200.0, preopen_rate_per_min=3600.0)  # 20 and 60 a second
        seen = []
        for _ in range(3):
            m.PREDICT_PACERS.clear()
            m.set_predict_rates(config)
            until = time.monotonic() + 0.5

            async def count(key: str) -> int:
                n = 0
                while time.monotonic() < until:
                    await m.predict_pacer(key).take()
                    n += 1
                return n
            mine, other = await asyncio.gather(count("pre-key"), count("main-key"))
            seen.append((mine, other))
            if 6 <= other <= 14 and 20 <= mine <= 36 and mine >= 2 * other:
                break
        else:
            raise AssertionError(f"(the pre-open key's requests, another key's) in half a second, three times: {seen}")
        # the first request of each on an empty bucket waits no longer than its own pace says: a twentieth of a second, and a
        # sixtieth (what the pacer reckoned, not the clock: a machine standing still only shortens it)
        m.PREDICT_PACERS.clear()
        for key, most in (("main-key", 60 / 1200), ("pre-key", 60 / 3600)):
            pacer = m.predict_pacer(key)
            pacer.tokens, pacer.at = 0.0, time.monotonic()
            await pacer.take()
            assert pacer.waited <= most * 1.05, (key, pacer.waited, most)
    finally:
        m.PREDICT_PACING = kept[0]
        m.PREDICT_RATE["per_min"] = kept[1]
        m.PREDICT_RATE_OF.clear(); m.PREDICT_RATE_OF.update(kept[2])
        m.PREDICT_PACERS.clear(); m.PREDICT_PACERS.update(kept[3])
    print("THE_PACE_IS_KEPT_OK")


async def place(ctl, mk, at) -> None:
    c = p.candidate(mk, at, ctl.owner.config, ctl.settings)
    assert c is not None, mk["categorySlug"]
    for level, (price, shares) in enumerate(ctl.settings.levels):
        await ctl.place_pair({**c, "slug": c["slug"] if level == 0 else f"{c['slug']}#{level}", "window": c["slug"], "level": level,
                              "price": str(price), "shares": str(shares), "attempt": 0, "market": mk, "read_ms": at})


async def the_speed_line() -> None:
    kept = (m.PREDICT_PACING, m.PREDICT_RATE["per_min"], dict(m.PREDICT_RATE_OF), dict(m.PREDICT_PACERS), dict(m.PREDICT_LIMITS_SAID))
    rows = [market(at=NOW + n * 15 * MIN, mid=1000 + n) for n in range(1, 6)]
    ctl, api, clock = controller(rows, PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="300", PREOPEN_LEVELS="43x60",  # two levels: the first and one deeper
                                 PREDICT_API_KEY="main-key", PREOPEN_API_KEY="pre-key")
    api.api_key = "pre-key"  # (the tests' stand-in for the mode's client: on the mode's own key, as the real one is)
    try:
        m.PREDICT_PACERS.clear(); m.PREDICT_LIMITS_SAID.clear(); m.PREDICT_BUCKETS_SAID.clear(); m.PREDICT_SENT.clear()
        assert ctl.speed_line() == "\n速度：最近 1 分钟挂上 0 张单", ctl.speed_line()  # nothing placed, no request on the key yet: nothing more to say
        for mk in rows[:3]:
            await place(ctl, mk, NOW)
        assert len(api.sent) == 12 and len(ctl.placed_at) == 12, "three windows, two levels, two sides: each order Predict accepted, once"
        assert ctl.speed_line() == "\n速度：最近 1 分钟挂上 12 张单；本次启动后最快的一分钟 12 张（09:00:00）", ctl.speed_line()
        clock.ms = NOW + 30_000
        await place(ctl, rows[3], clock.ms)
        assert ctl.speed_line().startswith("\n速度：最近 1 分钟挂上 16 张单；本次启动后最快的一分钟 16 张（09:00:30）"), ctl.speed_line()
        # an order Predict is not known to have taken is not counted: a request that timed out (its result unknown; the pair's
        # other side is then never sent) — the window's deeper level, both sides accepted, is
        api.fail_side = len(api.sent) + 1
        await place(ctl, rows[4], clock.ms)
        assert len(api.sent) == 19 and len(ctl.placed_at) == 18, (len(api.sent), len(ctl.placed_at))
        ctl.note_speed(clock.ms)  # (the loop's look, every half second: the fastest minute is kept whether or not anyone asks for the status)
        assert ctl.fastest == (18, NOW + 30_000)
        # a minute on: the last minute's alone; the longer look names the windows too
        clock.ms = NOW + 61_000
        assert ctl.speed_line().startswith("\n速度：最近 1 分钟挂上 6 张单；本次启动后最快的一分钟 18 张（09:00:30）"), ctl.speed_line()
        clock.ms = NOW + 5 * MIN
        assert ctl.speed_line() == "\n速度：最近 1 分钟挂上 0 张单；最近 5 分钟 18 张、5 场（每分钟 4 张 ≈ 1.0 场）；本次启动后最快的一分钟 18 张（09:00:30）", ctl.speed_line()
        clock.ms = NOW + 30 * MIN  # ten minutes at the most
        assert ctl.speed_line() == "\n速度：最近 1 分钟挂上 0 张单；本次启动后最快的一分钟 18 张（09:00:30）", ctl.speed_line()
        # the loop keeps the fastest minute as it goes round, whether or not anyone asks for the status
        ctl.fastest = (0, 0)
        ctl.placed_at.extend([(clock.ms, "w")] * 30)
        ctl.note_speed(clock.ms)
        assert ctl.fastest == (30, clock.ms)
        ctl.note_speed(clock.ms + 61_000)
        assert ctl.fastest == (30, clock.ms), "a slower minute after it changes nothing"
        # how long the listed levels not quoted yet take at the last minute's speed
        assert ctl.speed_line(40).endswith("；照最近一分钟的速度，已发布未挂的 40 档（80 张单）还要约 3 分钟"), ctl.speed_line(40)
        clock.ms += 2 * MIN
        assert "还要约" not in ctl.speed_line(40), "nothing placed in the last minute: no guess"
        clock.ms -= 2 * MIN

        # --- the key: at its pace or not, its 429s, what Predict says ---
        pacer = m.predict_pacer("pre-key")
        now = time.monotonic()
        pacer.sent.extend([now] * 37)
        line = ctl.speed_line()
        assert "；预挂专用 Key 最近 1 分钟发出 37 次请求（不节流）；被拒绝（429）：最近 10 分钟 0 次、24 小时 0 次" in line, line  # (the suites run unpaced)
        m.PREDICT_PACING = True
        line = ctl.speed_line()
        assert ("；预挂专用 Key 最近 1 分钟发出 37 次请求、节拍每分钟 220 次——没用满（17%）：现在没有那么多要发的，这时调高节拍看不出区别，"
                "要测上限得等有一批单在补挂的时候；被拒绝（429）：最近 10 分钟 0 次、24 小时 0 次") in line, line
        pacer.sent.extend([now] * 163)  # 200 of 220: at its pace (nine tenths of it)
        pacer.waits.extend([(now, 2.5)] * 4)
        line = ctl.speed_line()
        assert "；预挂专用 Key 最近 1 分钟发出 200 次请求、节拍每分钟 220 次——用满了（4 次请求排队等节拍，共等 10 秒）：现在的速度就是这个节拍下的上限；被拒绝" in line, line
        ctl.lent_at.extend([now] * 3)
        assert "上限；另借规则吃单的 Key 走了 3 次；被拒绝" in ctl.speed_line(), ctl.speed_line()
        # its own pace, where it has one
        m.set_predict_rates(dataclasses.replace(ctl.owner.config, preopen_rate_per_min=260.0))
        m.predict_pacer("pre-key")
        assert "发出 200 次请求、节拍每分钟 260 次——没用满（77%）" in ctl.speed_line(), ctl.speed_line()
        # the 429s: counted, and the minute's count before the one that began the last run of them — those right after a
        # cooldown (the key silent for twenty seconds of the minute before) say nothing of where the limit is
        pacer.limits.extend([(now - 90_000, 300), (now - 3_000, 251), (now - 2_975, 170), (now - 500, 247), (now - 478, 163), (now - 455, 158)])
        line = ctl.speed_line()
        assert ("；被拒绝（429）：最近 10 分钟 3 次、24 小时 5 次，最近一轮是这把 Key 一分钟内发到第 247 次请求时开始被拒的"
                "——Predict 给它的上限就在这附近，节拍要设得比它低") in line, line
        # the list of them full (a run of 429s longer than it holds): its first entry is from the middle of that run, no beginning
        full = [(now - 8_000 + 20 * i, 160) for i in range(pacer.limits.maxlen)]  # (the last of them 67 minutes ago)
        pacer.limits.clear(); pacer.limits.extend(full)
        assert "最近一轮" not in ctl.speed_line() and f"24 小时 {len(full)} 次" in ctl.speed_line(), ctl.speed_line()
        pacer.limits.append((now - 1, 244))  # ...the next one that does begin a run is taken
        assert "发到第 244 次请求时开始被拒的" in ctl.speed_line(), ctl.speed_line()
        pacer.limits.clear()
        pacer.limits.append((now - 90_000, 300))  # more than a day ago: nothing of it
        assert ctl.speed_line().endswith("；被拒绝（429）：最近 10 分钟 0 次、24 小时 0 次"), ctl.speed_line()
        # what Predict's own answers say of the limit, as they came
        m.note_limit_headers("api.predict.fun", {"x-api-key": "pre-key"}, 200, types.SimpleNamespace(headers={"Content-Type": "application/json"}))
        assert ctl.speed_line().endswith("24 小时 0 次；Predict 的应答里没有带限额信息（看了 1 个应答）"), ctl.speed_line()
        m.note_limit_headers("api.predict.fun", {"x-api-key": "pre-key"}, 200, types.SimpleNamespace(headers={"X-RateLimit-Policy": "240;w=60", "Content-Type": "application/json"}))
        assert "的应答，原样）：x-ratelimit-policy 240;w=60" in ctl.speed_line(), ctl.speed_line()  # (no count to read: the headers as they came)
        # 1.154.61: with a limit and what is left, read by the kind of request, beside the bot's own count
        m.note_limit_headers("api.predict.fun", {"x-api-key": "pre-key"}, 200,
                             types.SimpleNamespace(headers={"X-RateLimit-Limit": "240", "X-RateLimit-Remaining": "37", "Content-Type": "application/json"}), "GET", "/v1/markets/1")
        assert "；Predict 应答里报的限额：General（市场）每分钟 240 次——" in ctl.speed_line() and "已算 203 次、剩 37；" in ctl.speed_line(), ctl.speed_line()
        # 1.154.64: PREOPEN_APP_AUTO on (the default) — the mode takes them itself once both kinds are said: nothing to suggest;
        # it waits for them, and says so. Off: the figures to set, as 1.154.61 said them
        assert "可以填" not in ctl.speed_line() and "自动提速" not in ctl.pace_line(), (ctl.speed_line(), ctl.pace_line())
        ctl.auto_missed = True  # (the wait over and nothing said: the usual pace, and why)
        assert "——Predict 的应答没有报出这把 Key 的限额，照原来的速度；" in ctl.pace_line(), ctl.pace_line()
        ctl.auto_missed = False
        settings = ctl.settings
        ctl.settings = dataclasses.replace(settings, app_auto=False)
        ctl.owner.config = dataclasses.replace(ctl.owner.config, preopen=ctl.settings)
        assert "→ “Predict 限额”的 General 可以填 240；Trading 等下单的应答带回来再看" in ctl.speed_line(), ctl.speed_line()
        assert "自动提速" not in ctl.pace_line(), ctl.pace_line()
        ctl.settings = settings
        ctl.owner.config = dataclasses.replace(ctl.owner.config, preopen=settings)
        # the key shared with the model trader: named so
        shared, shared_api, _ = controller(rows, PREOPEN_MARKETS="btc", PREDICT_API_KEY="main-key")
        shared_api.api_key = "main-key"
        m.predict_pacer("main-key").sent.append(now)
        assert "；主 Key（和原策略共用）最近 1 分钟发出 1 次请求、节拍每分钟 260 次" not in shared.speed_line()
        assert "；主 Key（和原策略共用）最近 1 分钟发出 1 次请求、节拍每分钟 220 次——没用满（0%）" in shared.speed_line(), shared.speed_line()
        shared.ledger.close()

        # --- the whole status carries the line, and names the pace the key has to itself ---
        ctl.owner.config = dataclasses.replace(ctl.owner.config, preopen_rate_per_min=260.0)
        status = ctl.status()
        assert "\n速度：最近 1 分钟挂上 30 张单；" in status and "请求每分钟最多 260 次（PREOPEN_RATE_PER_MIN：只管预挂这把 Key，别的 Key 仍是 220；读等节拍、下单撤单优先）；" in status, status
        ctl.owner.config = dataclasses.replace(ctl.owner.config, preopen_rate_per_min=0.0)
        assert "请求每分钟最多 220 次（PREDICT_RATE_PER_MIN，读等节拍、下单撤单优先）；" in ctl.pace_line(), ctl.pace_line()
        # every other key unpaced (PREDICT_RATE_PER_MIN=0): this one still keeps the pace it was given
        ctl.owner.config = dataclasses.replace(ctl.owner.config, preopen_rate_per_min=260.0, predict_rate_per_min=0.0)
        assert "请求每分钟最多 260 次（PREOPEN_RATE_PER_MIN：只管预挂这把 Key，别的 Key 不节流；读等节拍、下单撤单优先）；" in ctl.pace_line(), ctl.pace_line()
        m.set_predict_rates(ctl.owner.config)
        assert (m.predict_pacer("pre-key").per_min, m.predict_pacer("main-key").per_min) == (260.0, 0.0)
        ctl.owner.config = dataclasses.replace(ctl.owner.config, predict_rate_per_min=220.0, preopen_api_key="")
        assert ("请求每分钟最多 220 次（PREDICT_RATE_PER_MIN，读等节拍、下单撤单优先）——PREOPEN_RATE_PER_MIN=260 没起作用：预挂没有自己单独的 Key"
                "（PREOPEN_API_KEY 没填，或和主 Key、盘口 Key、规则吃单 Key 是同一把）；") in ctl.pace_line(), ctl.pace_line()
    finally:
        m.PREDICT_PACING = kept[0]
        m.PREDICT_RATE["per_min"] = kept[1]
        m.PREDICT_RATE_OF.clear(); m.PREDICT_RATE_OF.update(kept[2])
        m.PREDICT_PACERS.clear(); m.PREDICT_PACERS.update(kept[3])
        m.PREDICT_LIMITS_SAID.clear(); m.PREDICT_LIMITS_SAID.update(kept[4])
        m.PREDICT_BUCKETS_SAID.clear(); m.PREDICT_SENT.clear()
        for task in ctl.settling.values():
            task.cancel()
        await asyncio.gather(*ctl.settling.values(), return_exceptions=True)
        ctl.ledger.close()
    print("THE_SPEED_LINE_OK")


class Answer:
    def __init__(self, status=200, body=b"{}", headers=None):
        self.status, self.body, self.headers, self.will_close, self.pos = status, body, dict(headers or {}), False, 0

    def read(self, n=-1):
        chunk = self.body[self.pos:self.pos + n] if n is not None and n >= 0 else self.body[self.pos:]
        self.pos += len(chunk)
        return chunk

    def getheader(self, name, default=None):
        return self.headers.get(name, default)


class Conn:
    script: list = []

    def __init__(self, host, port, timeout=None, context=None):
        self.host, self.port, self.timeout, self.sock, self.pending = host, port, timeout, None, None

    def request(self, method, path, body=None, headers=None):
        self.sock = self.sock or types.SimpleNamespace(settimeout=lambda t: None)
        self.pending = Conn.script.pop(0)

    def getresponse(self):
        return self.pending

    def close(self):
        self.sock = None


def what_predict_says() -> None:
    kept = dict(m.PREDICT_LIMITS_SAID)
    answer = lambda headers: types.SimpleNamespace(headers=headers)
    try:
        m.PREDICT_LIMITS_SAID.clear()
        assert m.predict_limits_said("k") == ""
        # another service's answers are none of this
        m.note_limit_headers("api.binance.com", {"x-api-key": "k"}, 200, answer({"X-RateLimit-Limit": "1200"}))
        assert not m.PREDICT_LIMITS_SAID
        # Predict's, with nothing of the kind in them: said so, with how many were looked at
        for _ in range(3):
            m.note_limit_headers("api.predict.fun", {"x-api-key": "k"}, 200, answer({"Content-Type": "application/json", "cf-ray": "abc"}))
        assert m.predict_limits_said("k") == "Predict 的应答里没有带限额信息（看了 3 个应答）", m.predict_limits_said("k")
        # whatever the spelling — X-RateLimit-*, RateLimit-*, X-Rate-Limit-* — as it came, lower case
        m.note_limit_headers("api.predict.fun", {"x-api-key": "k"}, 200, answer({"X-RateLimit-Limit": "240", "X-Ratelimit-Remaining": "12", "RateLimit-Policy": "240;w=60",
                                                                             "X-Rate-Limit-Reset": "34", "Retry-After": "5", "Server": "cloudflare"}))
        said = m.predict_limits_said("k")
        assert said.startswith("Predict 应答里自己报的限额（") and said.endswith(
            "的应答，原样）：x-ratelimit-limit 240、x-ratelimit-remaining 12、ratelimit-policy 240;w=60、x-rate-limit-reset 34"), said
        # an answer without them later leaves what was seen
        m.note_limit_headers("api.predict.fun", {"x-api-key": "k"}, 200, answer({"Content-Type": "application/json"}))
        assert m.predict_limits_said("k") == said and m.PREDICT_LIMITS_SAID["k"]["answers"] == 5
        # a 429's own headers beside them: its Retry-After too
        m.note_limit_headers("api.predict.fun", {"x-api-key": "k"}, 429, answer({"Retry-After": "20", "X-RateLimit-Remaining": "0"}))
        said = m.predict_limits_said("k")
        assert "原样）：x-ratelimit-remaining 0；上次被拒绝（429，" in said and said.endswith("）的应答带的是：retry-after 20、x-ratelimit-remaining 0"), said
        # a 429 that says nothing: said so (and a Retry-After alone is no stated limit)
        m.PREDICT_LIMITS_SAID.clear()
        m.note_limit_headers("api.predict.fun", {"x-api-key": "q"}, 429, answer({"Content-Type": "text/plain"}))
        assert m.predict_limits_said("q").startswith("上次被拒绝（429，") and m.predict_limits_said("q").endswith("）的应答没有带限额信息"), m.predict_limits_said("q")
        m.note_limit_headers("api.predict.fun", {"x-api-key": "r"}, 429, answer({"Retry-After": "20"}))
        assert "自己报的限额" not in m.predict_limits_said("r") and m.predict_limits_said("r").endswith("）的应答带的是：retry-after 20"), m.predict_limits_said("r")
        # each key's own; a request without a key under ""
        m.note_limit_headers("api.predict.fun", {}, 200, answer({"X-RateLimit-Limit": "60"}))
        assert m.PREDICT_LIMITS_SAID[""]["seen"] == {"x-ratelimit-limit": "60"} and "x-ratelimit-limit" not in m.predict_limits_said("q")
        # what a server puts in a header is text from outside: cut, and nothing in it but plain characters; a few headers at the most
        m.PREDICT_LIMITS_SAID.clear()
        m.note_limit_headers("api.predict.fun", {"x-api-key": "k"}, 200, answer({"X-RateLimit-Limit": "<b>240</b>\n&" + "9" * 200}))
        (value,) = m.PREDICT_LIMITS_SAID["k"]["seen"].values()
        assert value.startswith("?b?240?/b???999") and len(value) == 60, value
        m.note_limit_headers("api.predict.fun", {"x-api-key": "k"}, 200, answer({f"X-RateLimit-{i}": "1" for i in range(30)}))
        assert len(m.PREDICT_LIMITS_SAID["k"]["seen"]) == m.LIMIT_HEADERS_KEPT
        # it stands in every request's way: whatever it is handed, it never raises
        for args in ((None, None, None, None), ("api.predict.fun", None, 200, answer({})), ("api.predict.fun", {"x-api-key": "k"}, 200, None),
                     ("api.predict.fun", {"x-api-key": "k"}, 200, types.SimpleNamespace(headers=object())),
                     ("api.predict.fun", {"x-api-key": "k"}, 200, types.SimpleNamespace(headers={1: 2, None: None})),
                     ("api.predict.fun", {"x-api-key": ["unhashable"]}, 200, answer({}))):
            m.note_limit_headers(*args)

        # --- where the answers come in: the kept-alive requests, an error's answer too ---
        m.PREDICT_LIMITS_SAID.clear()
        real = (m.HTTPS_CONNECTION, urllib.request.getproxies)
        m.HTTPS_CONNECTION, urllib.request.getproxies = Conn, lambda: {}
        m._POOL.clear()
        try:
            Conn.script = [Answer(200, b'{"ok": true}', {"X-RateLimit-Limit": "240", "X-RateLimit-Remaining": "239"}),
                           Answer(429, b'{"message": "Too Many Requests"}', {"Retry-After": "20", "X-RateLimit-Remaining": "0"}),
                           Answer(200, b'{"ok": true}', {"X-RateLimit-Limit": "9"})]
            assert m._http_get("https://api.predict.fun/v1/orders", headers={"x-api-key": "pre-key"}) == b'{"ok": true}'
            assert m.PREDICT_LIMITS_SAID["pre-key"]["seen"] == {"x-ratelimit-limit": "240", "x-ratelimit-remaining": "239"}
            try:
                m._http_get("https://api.predict.fun/v1/orders", {"a": 1}, headers={"x-api-key": "pre-key"})
                raise AssertionError("no error")
            except m.RemoteError as error:
                assert error.retry_after == 20 and "429" in str(error), error  # the error is what it was
            assert m.PREDICT_LIMITS_SAID["pre-key"]["refused"] == {"retry-after": "20", "x-ratelimit-remaining": "0"} and m.PREDICT_LIMITS_SAID["pre-key"]["answers"] == 2
            assert m._http_get("https://api.example.test/x", headers={"x-api-key": "pre-key"}) == b'{"ok": true}'
            assert m.PREDICT_LIMITS_SAID["pre-key"]["answers"] == 2 and list(m.PREDICT_LIMITS_SAID) == ["pre-key"], "another host: not looked at"
            assert "pre-key" not in m.predict_limits_said("pre-key")
        finally:
            m.HTTPS_CONNECTION, urllib.request.getproxies = real
            m._POOL.clear()
    finally:
        m.PREDICT_LIMITS_SAID.clear(); m.PREDICT_LIMITS_SAID.update(kept)
    print("WHAT_PREDICT_SAYS_OK")


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def the_control_page() -> None:
    kept = (m.PREDICT_RATE["per_min"], dict(m.PREDICT_RATE_OF))
    try:
        form = m.CONTROL_FORMS["PREOPEN_RATE_PER_MIN"]
        assert form["kind"] == "number" and form["group"] == "预挂" and form["presets"][:3] == ["0", "220", "240"], form
        assert "PREOPEN_RATE_PER_MIN" in m.CONTROL_KEY_SET and "PREDICT_RATE_PER_MIN" not in m.CONTROL_KEY_SET, "the pace of every key stays a deploy variable"
        env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", **KEYS}
        bot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)
        assert bot.control_value("PREOPEN_RATE_PER_MIN") == "0" and not m.PREDICT_RATE_OF
        answer = bot.control_set({"PREOPEN_RATE_PER_MIN": "260"})
        assert answer == {"ok": True, "message": "已保存并生效：PREOPEN_RATE_PER_MIN=260（原 0）"}, answer
        assert m.PREDICT_RATE_OF == {"pre-key": 260.0} and m.predict_pacer("pre-key").per_min == 260.0 and m.predict_pacer("rule-key").per_min == 220.0
        answer = bot.control_set({"PREOPEN_RATE_PER_MIN": "2000"})
        assert not answer["ok"] and "60～1000" in answer["message"] and bot.control_value("PREOPEN_RATE_PER_MIN") == "260", answer
        assert bot.control_set({"PREOPEN_RATE_PER_MIN": "0"})["ok"] and not m.PREDICT_RATE_OF and m.predict_pacer("pre-key").per_min == 220.0
        # saved across a restart
        store = m.Store(":memory:")
        m.Bot(m.Config.from_env(env), store, FM(NOW), None).control_set({"PREOPEN_RATE_PER_MIN": "240"})
        m.PREDICT_RATE_OF.clear()
        again = m.Bot(m.Config.from_env(env), store, FM(NOW), None)
        assert again.control_value("PREOPEN_RATE_PER_MIN") == "240" and m.PREDICT_RATE_OF == {"pre-key": 240.0}
    finally:
        m.PREDICT_RATE["per_min"] = kept[0]
        m.PREDICT_RATE_OF.clear(); m.PREDICT_RATE_OF.update(kept[1])
    print("THE_CONTROL_PAGE_OK")


the_setting()
one_key_alone()
asyncio.run(the_pace_is_kept())
asyncio.run(the_speed_line())
what_predict_says()
the_control_page()
print("PACE_OK")
