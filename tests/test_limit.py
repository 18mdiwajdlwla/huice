"""/preopen 测速 (1.154.58): where Predict's limit for the pre-open key really lies — the user, told a key is allowed 240
requests a minute: "这个加个测速的，检查下最大上限".

The key's whole traffic is paced a step at a time above what it runs at — reads made for the test filling what the mode's own
requests leave, behind them — each step held, until Predict answers the first 429. Here: the steps and what ends them, on the
real client, the real pacer and a stand-in for Predict's limiter, with the clock's scale brought down (a "minute" of half a
second); the stretches a step may run in (clear of the mode's cancel times, its listings, the rule taker's seconds); what
cannot be tested and why; the stop; the report's words; the command. Entirely offline.

Run: python tests/test_limit.py
"""
import asyncio
import collections
import json
import sys
import threading
import time
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L
import preopen as p
from test_preopen import NOW, controller, market

MIN = 60_000
SCALED = dict(SPEED_TEST_STEPS=(2400, 3600, 6000), SPEED_TEST_STEP_S=1.0, SPEED_TEST_WINDOW_S=0.5, SPEED_TEST_TICK_S=0.02, SPEED_TEST_REACHED=0.5)


class Predict:
    """Predict under main._http_get: each key's requests counted over the last ``window`` seconds, and the one that would be
    more than ``limit`` of them refused with a 429 (its answer says Retry-After, as the real path would note it). ``slow``:
    seconds an answer takes; ``broken``: every read fails with a 500."""
    def __init__(self, limit: int, window: float = 0.5, slow: float = 0.0, broken: bool = False):
        self.limit, self.window, self.slow, self.broken = limit, window, slow, broken
        self.seen: dict[str, collections.deque] = {}
        self.paths: list[str] = []
        self.refused = 0
        self.lock = threading.Lock()

    def __call__(self, url, payload=None, timeout=15, headers=None):
        key, path = (headers or {}).get("x-api-key", ""), url.split("/v1", 1)[-1]
        if self.slow:
            time.sleep(self.slow)
        with self.lock:
            now, times = time.monotonic(), self.seen.setdefault(key, collections.deque())
            while times and times[0] <= now - self.window:
                times.popleft()
            self.paths.append(path)
            if len(times) >= self.limit:
                self.refused += 1
                m.note_limit_headers("api.predict.fun", headers or {}, 429, types.SimpleNamespace(headers={"Retry-After": "20"}))
                raise m.RemoteError("HTTP 429: Too Many Requests")
            times.append(now)
        if path == "/auth/message":
            return json.dumps({"success": True, "data": {"message": "sign me"}}).encode()
        if path == "/auth":
            return json.dumps({"success": True, "data": {"token": "a.b.c"}}).encode()
        if self.broken:
            raise m.RemoteError("HTTP 500: Internal Server Error")
        return json.dumps({"success": True, "data": {"id": "1", "status": "OPEN", "amount": "1", "amountFilled": "0"}}).encode()


class Kept:
    """The module's figures a test changes, put back as they were."""
    def __init__(self, **scaled):
        self.scaled = {"SPEED_TEST_WAIT_S": p.SPEED_TEST_WAIT_S, "SPEED_TEST_STEP_S": p.SPEED_TEST_STEP_S, **scaled}  # (those a test sets on its way, too)

    def __enter__(self):
        self.kept = ({name: getattr(p, name) for name in self.scaled}, m.PREDICT_PACING, m.PREDICT_RATE["per_min"], dict(m.PREDICT_RATE_OF),
                     dict(m.PREDICT_RATE_TEST), dict(m.PREDICT_PACERS), dict(m.PREDICT_LIMITS_SAID), m._http_get, dict(p.SPEED_LAST))
        for name, value in self.scaled.items():
            setattr(p, name, value)
        m.PREDICT_PACING = True
        m.PREDICT_PACERS.clear(); m.PREDICT_RATE_OF.clear(); m.PREDICT_RATE_TEST.clear(); m.PREDICT_LIMITS_SAID.clear(); p.SPEED_LAST.clear()
        return self

    def __exit__(self, *exc):
        for name, value in self.kept[0].items():
            setattr(p, name, value)
        m.PREDICT_PACING, m.PREDICT_RATE["per_min"], m._http_get = self.kept[1], self.kept[2], self.kept[7]
        for table, was in ((m.PREDICT_RATE_OF, self.kept[3]), (m.PREDICT_RATE_TEST, self.kept[4]), (m.PREDICT_PACERS, self.kept[5]),
                           (m.PREDICT_LIMITS_SAID, self.kept[6]), (p.SPEED_LAST, self.kept[8])):
            table.clear(); table.update(was)


async def place(ctl, mk, at) -> None:
    c = p.candidate(mk, at, ctl.owner.config, ctl.settings)
    assert c is not None, mk["categorySlug"]
    await ctl.place_pair({**c, "slug": c["slug"], "window": c["slug"], "level": 0, "attempt": 0, "market": mk, "read_ms": at})


async def mode(pairs: int = 1, **env):
    """The pre-open mode on a key of its own, ``pairs`` windows quoted (the first one the soonest), the wallet able to sign in."""
    rows = [market(at=NOW + n * 15 * MIN, mid=1000 + n) for n in range(1, pairs + 1)] or [market()]
    ctl, api, clock = controller(rows, **{"PREOPEN_MARKETS": "btc", "PREOPEN_AHEAD_MINUTES": "300", "PREDICT_API_KEY": "main-key", "PREOPEN_API_KEY": "pre-key", **env})
    api.api_key = "pre-key"  # (the tests' stand-in for the mode's client: on the mode's own key, as the real one is)
    ctl.owner.live.wallet.sign_text = lambda message: "0xsig"
    for mk in rows[:pairs]:
        await place(ctl, mk, NOW)
    return ctl, api, clock, rows


async def over(ctl) -> None:
    if ctl.speed_task is not None and not ctl.speed_task.done():
        ctl.speed_task.cancel()
    await asyncio.gather(*[t for t in (ctl.speed_task,) if t is not None], return_exceptions=True)
    for task in ctl.settling.values():
        task.cancel()
    await asyncio.gather(*ctl.settling.values(), return_exceptions=True)
    ctl.ledger.close()


async def the_test_finds_the_limit() -> None:
    """Three steps on a key Predict allows 35 a window: 20 and 30 a window are held, 50 is refused — where, by whom, with what
    the key had let through; the key's pace as configured again, the mode's own client never stopped. (On the clock: three
    tries at the most, a machine that stood still may cost one.)"""
    seen = []
    for _ in range(3):
        with Kept(**SCALED):
            m.PREDICT_RATE["per_min"] = 1200.0
            ctl, api, clock, rows = await mode(2)
            server = m._http_get = Predict(35)
            digest = next(iter(ctl.ledger.rows[rows[1]["categorySlug"]]["orders"].values()))["hash"]  # the window farthest ahead: it rests longest
            task, words = ctl.speed_start(6000)
            assert task is not None and words.startswith("🧪 开始测速") and "每分钟 2400、3600、6000 次，每档保持 1 秒" in words and "回到现在的每分钟 1200 次" in words, words
            again, said = ctl.speed_start()
            assert again is None and said.startswith("测速已经在跑（/preopen 测速 停 可以中止）：09:00:00 开始，要测每分钟 2400、3600、6000 次这几档"), said
            waits: list[float] = []

            async def own() -> None:  # the mode's own low requests while the steps run: each has its token at once
                pacer = m.predict_pacer("pre-key")
                for _ in range(12):
                    await asyncio.sleep(0.12)
                    began = time.monotonic()
                    await pacer.take(False, low=True)
                    waits.append(time.monotonic() - began)
            mine = asyncio.create_task(own())
            await asyncio.sleep(0.3)
            running = ctl.speed_test_line()
            text = await asyncio.wait_for(task, 30)
            await mine
            steps = ctl.speed_state["steps"]
            seen.append([s["verdict"] for s in steps])
            if seen[-1] != ["held", "held", "refused"]:
                await over(ctl)
                continue
            assert running.startswith("\n测速：进行中——09:00:00 开始，要测每分钟 2400、3600、6000 次这几档") and "（/preopen 测速 停 可以中止）" in running, running
            last = steps[2]
            assert last["by"] == "测速的读请求" and last["said"] == {"retry-after": "20"} and 0 <= last["seconds"] <= 1.05, last
            assert 31 <= last["minute"] <= 41 and last["sixth"] <= last["minute"] and last["sixtieth"] <= last["sixth"], last  # (Predict's 35, counted here a moment apart)
            assert last["others"] == [], last
            assert all(s["rate"] >= 0.5 * s["pace"] for s in steps[:2]) and steps[0]["seconds"] >= 1.0, steps
            # the key's pace is the configured one again, nothing of the test left in the pacer's tables; the mode's client was never stopped
            assert not m.PREDICT_RATE_TEST and m.predict_pacer("pre-key").per_min == 1200.0
            assert ctl.cooldown() == "" and api.blocked_until == 0.0 and ctl.speed_api is not api and ctl.speed_api.blocked_until > time.monotonic()
            assert "send" not in ctl.speed_api.__dict__, "the client's own send again"
            # the reads were the mode's own kind — one of its resting orders — after one sign-in; and they went behind the mode's requests
            assert set(server.paths) == {"/auth/message", "/auth", f"/orders/{digest}"} and server.paths.count("/auth") == 1, set(server.paths)
            assert max(waits) < 0.5, ("the mode's own requests never queue behind the test's", waits)
            assert server.refused >= 1
            # the report
            lines = text.split("\n")
            assert lines[0].startswith("🧪 预挂专用 Key 测速（09:00:00 开始，用时 1 分钟；读的是预挂自己的一张挂单，排在预挂自己的请求后面）"), lines[0]
            assert lines[1].startswith("① 每分钟 2400 次：保持了 1 秒，没被拒绝（最后 0 秒这把 Key 实发 ") or lines[1].startswith("① 每分钟 2400 次：保持了 1 秒，没被拒绝（最后 1 秒"), lines[1]
            assert lines[2].startswith("② 每分钟 3600 次：保持了 1 秒，没被拒绝"), lines[2]
            assert lines[3].startswith("③ 每分钟 6000 次：第 ") and "秒被拒绝（429），被拒的是测速的读请求——那一刻这把 Key 最近" in lines[3] \
                and lines[3].endswith("；Predict 这次拒绝的应答带的是：retry-after 20"), lines[3]
            limit = (last["minute"] - 1) * 120  # (as a minute's: this clock's "minute" is half a second)
            assert lines[4] == f"结论：这把 Key 一分钟最多约 {limit} 次（每分钟 3600 次那一档整整保持住了）。", lines[4]
            assert lines[5].startswith(f"建议：把控制台“预挂专用 Key：每分钟请求数”设成 {(limit - 20) // 5 * 5}（比上限低 20") and "现在是 1200，能快约 " in lines[5], lines[5]
            assert lines[6] == "测速已结束，这把 Key 的节拍是每分钟 1200 次（和测之前一样）。说明：量的是读请求；下单请求如果另有单独的限额，这个测不出来。", lines[6]
            # kept for the status, and across the controller's restarts
            assert p.SPEED_LAST["text"] == text and p.SPEED_LAST["line"].startswith("这把 Key 一分钟最多约 ")
            line = ctl.speed_test_line()
            assert line == f"\n上次测速（09:00）：{p.SPEED_LAST['line']}（重新测：/preopen 测速）" and line in ctl.status(), line
            later, _, _ = controller([market()], PREOPEN_MARKETS="btc", PREDICT_API_KEY="main-key", PREOPEN_API_KEY="pre-key")
            assert later.speed_test_line() == line
            later.ledger.close()
            assert ctl.speed_stop().startswith("现在没有在跑的测速。上次（09:00:00）：这把 Key 一分钟最多约 ")
            await over(ctl)
            break
    else:
        raise AssertionError(f"the steps' outcomes, three times: {seen}")
    print("THE_TEST_FINDS_THE_LIMIT_OK")


async def what_ends_a_step() -> None:
    # a request of the mode's own met the 429 (the pacer notes each on the key, whoever sent it): the step is over, and says whose
    with Kept(**{**SCALED, "SPEED_TEST_STEPS": (2400, 3600)}):
        m.PREDICT_RATE["per_min"] = 1200.0
        ctl, api, clock, rows = await mode(1)
        m._http_get = Predict(10_000)
        task, _ = ctl.speed_start(3600)
        await asyncio.sleep(0.3)
        m.predict_pacer("main-key").sent.extend([time.monotonic()] * 5)  # (the bot's own key in the same minute: said beside it)
        m.predict_pacer("").sent.extend([time.monotonic()] * 3)          # (requests under no key: none of the keys named)
        m.predict_pacer("pre-key").note_429()
        m._http_get = Predict(0)  # ...and the reads of the test's still on their way are refused after it: the first on the key was the mode's
        text = await asyncio.wait_for(task, 30)
        (step,) = ctl.speed_state["steps"]
        assert step["verdict"] == "refused" and step["by"] == "预挂自己的请求" and 0.2 <= step["seconds"] <= 0.6, step
        assert step["others"] == [("主 Key", 5)] and "；同一分钟主 Key 5 次" in text, step
        assert "① 每分钟 2400 次：第 0 秒被拒绝（429），被拒的是预挂自己的请求" in text and "（第一档就被拒绝了）" in text, text
        assert not m.PREDICT_RATE_TEST and m.predict_pacer("pre-key").per_min == 1200.0
        await over(ctl)
    # another of the bot's keys refused meanwhile — Predict's limit may not be a key's alone: the test ends there, saying whose and what each had sent
    with Kept(**{**SCALED, "SPEED_TEST_STEPS": (2400, 3600)}):
        m.PREDICT_RATE["per_min"] = 1200.0
        ctl, api, clock, rows = await mode(1)
        m._http_get = Predict(10_000)
        m.predict_pacer("main-key").note_429()  # (one before the test: none of its doing)
        task, _ = ctl.speed_start(3600)
        await asyncio.sleep(0.3)
        assert "（正在测速：里面有测速的读请求）" in ctl.speed_line(), ctl.speed_line()
        m.predict_pacer("main-key").sent.extend([time.monotonic()] * 7)
        m.predict_pacer("main-key").note_429()
        text = await asyncio.wait_for(task, 30)
        (step,) = ctl.speed_state["steps"]
        assert step["verdict"] == "elsewhere" and step["where"] == "主 Key" and step["theirs"] == 7 and 0.2 <= step["seconds"] <= 0.6 and step["minute"] >= 5, step
        assert ("① 每分钟 2400 次：第 0 秒，主 Key（不是在测的这一把）被 Predict 拒绝了一次（429），测速马上停了——那一刻在测的这把 Key 最近 0 秒发了 " in text
                and "主 Key自己最近 0 秒发了 7 次。可能是巧合（主 Key自己发到了上限），也可能 Predict 的限额不只按 Key 算（比如还按 IP 或按账户）：先别再往上测，发 /diag 看各把 Key 的 429" in text
                and "结论：没有测出结果。" in text), text
        assert not m.PREDICT_RATE_TEST and m.predict_pacer("pre-key").per_min == 1200.0 and "（正在测速" not in ctl.speed_line()
        assert ctl.speed_progress().endswith("；已完成：2400 别的 Key 被拒绝")
        await over(ctl)
    # the pace not reached (an answer takes a quarter of a second, eight reads at once: thirty a second for forty): said, and no step above it
    with Kept(**{**SCALED, "SPEED_TEST_REACHED": 0.95}):
        m.PREDICT_RATE["per_min"] = 1200.0
        ctl, api, clock, rows = await mode(1)
        m._http_get = Predict(10_000, slow=0.25)
        task, _ = ctl.speed_start(6000)
        text = await asyncio.wait_for(task, 30)
        (step,) = ctl.speed_state["steps"]
        assert step["verdict"] == "short" and step["rate"] < 0.95 * 2400, step
        assert "① 每分钟 2400 次：没被拒绝，但没发到这个速度——最后" in text and "结论：没有测出结果。" in text, text
        await over(ctl)
    # the reads fail otherwise than by a refusal: counted, and the test ends on them
    with Kept(**{**SCALED, "SPEED_TEST_FAILS": 3}):
        m.PREDICT_RATE["per_min"] = 1200.0
        ctl, api, clock, rows = await mode(1)
        m._http_get = Predict(10_000, broken=True)
        task, _ = ctl.speed_start(6000)
        text = await asyncio.wait_for(task, 30)
        (step,) = ctl.speed_state["steps"]
        assert step["verdict"] == "failed" and "HTTP 500" in step["why"], step
        assert "① 每分钟 2400 次：读请求连续失败（不是被拒绝）：HTTP 500" in text and not m.PREDICT_RATE_TEST, text
        await over(ctl)
    # no refusal up to the top: said so, with how to go on
    with Kept(**{**SCALED, "SPEED_TEST_STEPS": (2400, 3600, 6000, 9000)}):
        m.PREDICT_RATE["per_min"] = 1200.0
        ctl, api, clock, rows = await mode(1)
        m._http_get = Predict(10_000)
        task, words = ctl.speed_start(3600)
        assert "每分钟 2400、3600 次，每档" in words, words
        text = await asyncio.wait_for(task, 30)
        assert [s["verdict"] for s in ctl.speed_state["steps"]] == ["held", "held"], ctl.speed_state["steps"]
        assert "结论：到每分钟 3600 次都没被拒绝，上限不低于 " in text and "想知道到底多高，发 /preopen 测速 " in text \
            and f"想先用上，把控制台“预挂专用 Key：每分钟请求数”设成 {3600 - p.SPEED_TEST_MARGIN}。" in text, text
        await over(ctl)
    print("WHAT_ENDS_A_STEP_OK")


async def stopped_and_refused() -> None:
    with Kept(**{**SCALED, "SPEED_TEST_STEP_S": 5.0}):
        m.PREDICT_RATE["per_min"] = 1200.0
        ctl, api, clock, rows = await mode(1)
        m._http_get = Predict(10_000)
        assert ctl.speed_stop() == "现在没有在跑的测速。"
        # /preopen 测速 停: over at once, the key's pace back before the task has even been told
        task, _ = ctl.speed_start()
        await asyncio.sleep(0.3)
        assert m.PREDICT_RATE_TEST == {"pre-key": 2400.0} and m.predict_pacer("pre-key").per_min == 2400.0, "a step's pace in force"
        # a setting saved meanwhile (the control page) neither ends the step nor is lost under it
        m.set_predict_rates(types.SimpleNamespace(predict_rate_per_min=1500.0, preopen_api_key="pre-key", preopen_rate_per_min=0.0,
                                                  predict_api_key="main-key", predict_book_api_key="", rule_api_key=""))
        assert m.predict_pacer("pre-key").per_min == 2400.0 and m.predict_pacer("main-key").per_min == 1500.0
        assert ctl.speed_stop() == "测速已中止，这把 Key 的节拍回到每分钟 1500 次。"
        assert not m.PREDICT_RATE_TEST and m.predict_pacer("pre-key").per_min == 1500.0
        await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled() and "中途结束：手动停止" in p.SPEED_LAST["text"] and p.SPEED_LAST["line"] == "没有测出结果（手动停止）", p.SPEED_LAST
        assert not any(t.get_name().startswith("preopen-speed-") and not t.done() for t in asyncio.all_tasks()), "no reader left behind"
        # the mode's own end (a setting changed, a deploy) takes the test with it: run()'s end cancels it as it does its other tasks
        task, _ = ctl.speed_start()
        await asyncio.sleep(0.2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert not m.PREDICT_RATE_TEST and "中途结束：中止了（预挂模式重启，或部署）" in p.SPEED_LAST["text"], p.SPEED_LAST
        assert "self.speed_task" in Path(p.__file__).read_text().split("bulk = [task for task in (", 1)[1].split("\n", 1)[0], "run() ends the test with its other tasks"
        # what cannot be tested, and why
        api.api_key = "main-key"
        assert ctl.speed_start() == (None, "❌ 现在不能测速：预挂没有自己单独的 Key（PREOPEN_API_KEY 没填，或和主 Key、盘口 Key、规则吃单 Key 是同一把）"
                                           "——测速要把一把 Key 顶到被 Predict 拒绝一次，和原策略、盘口或规则吃单共用的 Key 不测")
        api.api_key = ""
        assert "预挂没有自己单独的 Key" in ctl.speed_refusal()
        api.api_key = "pre-key"
        m.PREDICT_PACING = False
        assert ctl.speed_refusal() == "请求节流关着（PREDICT_PACING=off）——测速靠节流来定每一档的速度"
        m.PREDICT_PACING = True
        assert ctl.speed_refusal() == ""
        ctl.winding = "预挂模式已关闭"
        assert ctl.speed_refusal() == "预挂在收尾中（预挂模式已关闭）"
        ctl.winding = ""
        await over(ctl)
        # nothing to read: no order resting, no market read — a market read serves when there is one
        bare, bare_api, _, _ = await mode(0)
        assert bare.speed_target() is None and bare.speed_refusal() == "预挂现在没有挂着的单、也没读过市场，没有可以拿来读的东西；等挂上单再测"
        bare.keep_details("777", {"id": 777})
        assert bare.speed_target() == (False, "/markets/777") and bare.speed_refusal() == ""
        await over(bare)
    print("STOPPED_AND_REFUSED_OK")


async def the_stretches_a_step_runs_in() -> None:
    """A step begins only where a refusal's cooldown can meet none of the mode's moments: twenty seconds after a cancel time
    or an expected listing, ending forty-five before the next — and not while a listing is expected, the key cools down, or
    the rule taker is at (or near) its seconds."""
    with Kept():
        ctl, api, clock, rows = await mode(2)
        first, second = (ctl.ledger.rows[mk["categorySlug"]] for mk in rows)
        free = lambda: ctl.speed_free_at(clock.ms, 120.0) - clock.ms
        first["cancel_ms"], second["cancel_ms"] = NOW + 3_600_000, NOW + 7_200_000
        assert ctl.speed_moments(NOW, 900_000) == [] and free() == 0, "nothing near: now"
        # a cancel time within the step and the forty-five seconds after it: the step waits until twenty seconds past it
        first["cancel_ms"] = NOW + 164_000
        assert ctl.speed_moments(NOW, 900_000) == [NOW + 164_000] and free() == 184_001
        first["cancel_ms"] = NOW + 166_000
        assert free() == 0, "the step and its margin end before it"
        first["cancel_ms"] = NOW - 19_000  # one just gone: its cancels, their reads
        assert free() == 1_001
        first["cancel_ms"] = NOW - 21_000
        assert ctl.speed_moments(NOW, 900_000) == [] and free() == 0
        # only a pair that rests has a cancel time to keep clear of
        first["cancel_ms"], first["phase"] = NOW + 60_000, "closing"
        assert free() == 0
        first["phase"] = "active"
        assert free() == 80_001
        # one moment after another, each within reach of the last: after the last of them
        second["cancel_ms"] = NOW + 240_000
        assert free() == 260_001
        first["cancel_ms"], second["cancel_ms"] = NOW + 3_600_000, NOW + 7_200_000
        # a card's coming listings — a cycle after the last one seen, and on by cycles; a card not quoted, or overdue, has none
        ctl.listed_at = {"BTC-15M": NOW - 14 * MIN, "ETH-15M": NOW - 14 * MIN + 5_000}
        assert ctl.speed_moments(NOW, 30 * MIN) == [NOW + MIN, NOW + 16 * MIN] and free() == 80_001, ctl.speed_moments(NOW, 30 * MIN)
        ctl.listed_at = {"BTC-15M": NOW - 5_000}  # a listing just seen: its orders are going out
        assert ctl.speed_moments(NOW, 10 * MIN) == [NOW - 5_000] and free() == 15_001
        ctl.listed_at = {"BTC-15M": NOW - 15 * MIN - 21_000}
        assert ctl.speed_moments(NOW, 30 * MIN) == [] and free() == 0
        # as the bot runs with five-minute cards — a cancel ten seconds before each boundary, a listing a minute after it —
        # one step fits a cycle: it begins twenty seconds after the listing and is clear forty-five before the next cancel
        ctl.settings = types.SimpleNamespace(markets={"BTC-5M"})
        base = NOW - NOW % (5 * MIN) + 5 * MIN  # the next boundary
        clock.ms = base - 120_000
        first["cancel_ms"], second["cancel_ms"] = base - 10_000, base + 290_000
        ctl.listed_at = {"BTC-5M": base - 239_000}  # (listed 61 s after the last boundary: the next is expected 61 s after this one)
        at = ctl.speed_free_at(clock.ms, 120.0)
        assert at == base + 61_000 + 20_001, (at - base)
        assert at + 165_000 < base + 290_000 and ctl.speed_free_at(at, 120.0) == at, "that step fits; the one after it waits for the next cycle"
        assert ctl.speed_free_at(at + 120_000, 120.0) > base + 290_000
    # the wait itself
    with Kept(SPEED_TEST_TICK_S=0.01, SPEED_TEST_WAIT_S=0.3):
        ctl, api, clock, rows = await mode(1)
        ctl.speed_state = {"next": 240}
        assert await ctl.speed_wait(120.0) == "", "nothing in the way: now"
        # the rule taker at its seconds, or due within the step: waited out (the bot's own word on it, asked for the step and its margin)
        asked, ok = [], [False]
        ctl.owner.rules_clear = lambda seconds: asked.append(seconds) or ok[0]
        waiting = asyncio.create_task(ctl.speed_wait(120.0))
        await asyncio.sleep(0.1)
        assert not waiting.done() and asked[0] == 165.0 and ctl.speed_state["note"] == "等空档——避开规则吃单在抢单时段；下一档每分钟 240 次", ctl.speed_state
        ok[0] = True
        assert await asyncio.wait_for(waiting, 5) == ""
        ok[0] = False
        assert await ctl.speed_wait(120.0) == "等了 0 分钟都没有等到能跑一档的空档（一直碰上：规则吃单在抢单时段），先停在这里；过了这段时间再发 /preopen 测速"
        ok[0] = True
        # a listing expected about now (the discovery is probing every half second for it)
        ctl.listed_at = {"BTC-15M": NOW - 15 * MIN - 30_000}
        assert ctl.sprinting() and ctl.poll_wait() == p.FAST_POLL_S and "一直碰上：新场次正要发布" in await ctl.speed_wait(120.0)
        ctl.listed_at = {}
        assert not ctl.sprinting() and ctl.poll_wait() > p.FAST_POLL_S
        # the key cooling down after a 429
        api.blocked_until = time.monotonic() + 30
        assert "一直碰上：这把 Key 在限流冷却中" in await ctl.speed_wait(120.0)
        api.blocked_until = 0.0
        # a moment of the mode's near: the wait says when the step can begin
        next(iter(ctl.ledger.rows.values()))["cancel_ms"] = NOW + 100_000
        waiting = asyncio.create_task(ctl.speed_wait(120.0))
        await asyncio.sleep(0.05)
        assert ctl.speed_state["note"] == "等空档——避开撤单 / 新场次发布的时刻，约 120 秒后开始；下一档每分钟 240 次", ctl.speed_state
        clock.ms = NOW + 120_001
        assert await asyncio.wait_for(waiting, 5) == ""
        # the mode stopping ends the wait
        next(iter(ctl.ledger.rows.values()))["cancel_ms"] = clock.ms + 100_000
        ctl.stop.set()
        assert await ctl.speed_wait(120.0) == "预挂模式停止了"
        await over(ctl)
    # how long a step lasts: two minutes — or what the mode's moments leave room for, a minute and a quarter at the least
    with Kept():
        ctl, api, clock, rows = await mode(7)
        m._http_get = Predict(10_000)
        pairs = [ctl.ledger.rows[mk["categorySlug"]] for mk in rows]
        assert ctl.speed_room(NOW) == 835.0, "the soonest cancel time 880 s on (a quarter of an hour, less its twenty seconds): all the time to it but the forty-five before it"

        async def begun(every: int):
            for i, pair in enumerate(pairs):
                pair["cancel_ms"] = NOW + 100_000 + i * every
            task, words = ctl.speed_start()
            if task is not None:
                ctl.speed_stop()
                await asyncio.gather(task, return_exceptions=True)
            return ctl.speed_room(NOW), task is not None, words
        room, started, words = await begun(300_000)  # as five-minute cards leave it: 300 s less the margins
        assert (room, started) == (235.0, True) and "每档保持 120 秒，到 Predict" in words and ctl.speed_state["step_s"] == 120.0, (room, words)
        room, started, words = await begun(150_000)
        assert (room, started) == (85.0, True) and "每档保持 85 秒（空档不够 120 秒，这次缩短了），到 Predict" in words and ctl.speed_state["step_s"] == 85.0, (room, words)
        room, started, words = await begun(130_000)
        assert (room, started) == (65.0, False) and words == (
            "❌ 现在不能测速：撤单 / 新场次发布的时刻排得太密——接下来 15 分钟里最长的空档只够一档跑 65 秒，至少要 75 秒（每档前后还要各留 20 和 45 秒）"), (room, words)
        await over(ctl)
    # ...and a test that finds no stretch ends saying so, nothing of it left behind
    with Kept(**{**SCALED, "SPEED_TEST_TICK_S": 0.01}):
        m.PREDICT_RATE["per_min"] = 1200.0
        ctl, api, clock, rows = await mode(1)
        server = m._http_get = Predict(10_000)
        ctl.owner.rules_clear = lambda seconds: False
        task, _ = ctl.speed_start()
        p.SPEED_TEST_WAIT_S = 0.2  # (the wait's patience, once the test has begun)
        text = await asyncio.wait_for(task, 30)
        assert ctl.speed_state["steps"] == [] and "中途结束：等了 0 分钟都没有等到能跑一档的空档（一直碰上：规则吃单在抢单时段）" in text and "结论：没有测出结果" in text, text
        assert server.paths == [] and not m.PREDICT_RATE_TEST, "not one request went out"
        await over(ctl)
    print("THE_STRETCHES_A_STEP_RUNS_IN_OK")


def the_report() -> None:
    """The report's words on outcomes made by hand: the limit is one less than the key's minute at the refusal; the pace advised
    leaves a cancel's borrowing room under it; and what is said when that is no more than the pace in force."""
    with Kept():
        m.PREDICT_RATE["per_min"] = 220.0
        ctl, api, clock = controller([market()], PREOPEN_MARKETS="btc", PREDICT_API_KEY="main-key", PREOPEN_API_KEY="pre-key")
        api.api_key = "pre-key"
        held = lambda pace: {"pace": pace, "verdict": "held", "seconds": 120.0, "mine": pace * 2 - 40, "got": pace, "span": 60.0, "rate": float(pace)}
        refused = lambda pace, minute, **more: {"pace": pace, "verdict": "refused", "seconds": 55.4, "mine": 200, "by": "测速的读请求", "minute": minute, "sixth": 44,
                                                "sixtieth": 5, "clock": 71, "into": 17.2, "others": [("主 Key", 190), ("规则吃单 Key", 6)], "said": {"retry-after": "20"}, **more}
        state = lambda *steps, **more: {"began_ms": NOW, "took": 540.0, "reads": "预挂自己的一张挂单", "steps": list(steps), "ended": "", **more}
        # Predict's 240 found: 240 held, 260 refused at its 241st — and 220, the pace in force, is already the one to keep
        text, line = ctl.speed_report(state(held(240), refused(260, 241)))
        assert text.split("\n") == [
            "🧪 预挂专用 Key 测速（09:00:00 开始，用时 9 分钟；读的是预挂自己的一张挂单，排在预挂自己的请求后面）",
            "① 每分钟 240 次：保持了 120 秒，没被拒绝（最后 60 秒这把 Key 实发 240 次，合每分钟 240 次）",
            "② 每分钟 260 次：第 55 秒被拒绝（429），被拒的是测速的读请求——那一刻这把 Key 最近 60 秒发了 241 次（含被拒的这一次），最近 10 秒 44 次、最近 1 秒 5 次；"
            "按钟表上的整分钟算，这一分钟（已过 17 秒）发了 71 次；同一分钟主 Key 190 次、规则吃单 Key 6 次；Predict 这次拒绝的应答带的是：retry-after 20",
            "结论：这把 Key 一分钟最多约 240 次（每分钟 240 次那一档整整保持住了）。",
            "建议：不用改。现在的节拍每分钟 220 次已经是贴着上限的安全值（撤单可以预支名额，一分钟里最多会多出 16 次，所以要比上限低 20 左右）"
            "——单靠这一把 Key 没有再提速的余地，要更快只能再加一把预挂用的 Key，或者减少每场要发的请求。",
            "测速已结束，这把 Key 的节拍是每分钟 220 次（和测之前一样）。说明：量的是读请求；下单请求如果另有单独的限额，这个测不出来。"], text
        assert line == "这把 Key 一分钟最多约 240 次（每分钟 240 次那一档整整保持住了）"
        # a higher limit than Predict's word: the pace to set, twenty under it and on a five, and what it gains
        text, line = ctl.speed_report(state(held(240), held(260), held(280), held(300), refused(330, 306, said={})))
        assert "结论：这把 Key 一分钟最多约 305 次（每分钟 300 次那一档整整保持住了）。" in text and text.split("\n")[-2] == (
            "建议：把控制台“预挂专用 Key：每分钟请求数”设成 285（比上限低 20：撤单可以预支名额，一分钟里最多会多出 16 次）。现在是 220，能快约 30%。"), text
        assert "⑤ 每分钟 330 次：第 55 秒被拒绝" in text and "；Predict 这次拒绝的应答没有带限额信息" in text
        # refused with fewer in its minute than the step before held: not a minute's limit alone, or the key is used elsewhere
        text, _ = ctl.speed_report(state(held(240), refused(260, 150, said=None, others=[])))
        assert ("结论：这把 Key 一分钟最多约 149 次（每分钟 240 次那一档整整保持住了）。但被拒绝时一分钟里的请求数比上一档保持住的还少：Predict 的限额可能不只按每分钟算"
                "（比如还按更短的时间），或者这把 Key 还有别处在用——这个数只能当参考。") in text, text
        assert "；同一分钟" not in text and "Predict 这次拒绝的应答" not in text, "another key's count, and the answer's own words, only where there were any"
        # Predict counting by the clock's minutes would refuse the request one past its limit in the minute under way — fewer than
        # the last sixty seconds held. Where that reading does not contradict what was held, the limit lies between the two and
        # the lower is gone by (a stand-in limiter of 240 by the clock, at the real scale: 249 in the last minute, the 241st of
        # the clock's — and 248 was the answer, 225 the pace advised)
        text, line = ctl.speed_report(state(held(240), refused(260, 249, clock=241, into=57.9)))
        assert ("结论：这把 Key 一分钟最多 240～248 次（Predict 要是按钟表上的整分钟计数，就是 240：被拒的是那一分钟里的第 241 次；按 240 算）"
                "（每分钟 240 次那一档整整保持住了）。") in text and "建议：不用改。现在的节拍每分钟 220 次" in text, text
        # ...and where it does contradict it — 240 was held two minutes, the clock's minute had 71 — it is the last sixty seconds' count
        text, line = ctl.speed_report(state(held(240), refused(260, 249)))
        assert "结论：这把 Key 一分钟最多约 248 次（每分钟 240 次那一档整整保持住了）。" in text, text
        # nothing held: the pace the key has long run at (220) is what a clock's minute is set against
        text, line = ctl.speed_report(state(refused(240, 244, clock=231, into=55.0)))
        assert "结论：这把 Key 一分钟最多 230～243 次（Predict 要是按钟表上的整分钟计数，就是 230：被拒的是那一分钟里的第 231 次；按 230 算）（第一档就被拒绝了）。" in text, text
        text, line = ctl.speed_report(state(refused(240, 241, clock=135, into=33.4)))
        assert "结论：这把 Key 一分钟最多约 240 次（第一档就被拒绝了）。" in text, text
        # the first step refused: nothing held to set it against
        text, line = ctl.speed_report(state(refused(240, 238)))
        assert "结论：这把 Key 一分钟最多约 237 次（第一档就被拒绝了）。" in text and "建议：不用改。" in text and line.endswith("（第一档就被拒绝了）"), text
        # another key refused at a later step: what was held stands, and no pace above the one in force is advised on it
        elsewhere = {"pace": 300, "verdict": "elsewhere", "seconds": 41.0, "mine": 150, "where": "主 Key", "theirs": 221, "minute": 262}
        text, line = ctl.speed_report(state(held(240), held(260), held(280), elsewhere))
        assert ("④ 每分钟 300 次：第 41 秒，主 Key（不是在测的这一把）被 Predict 拒绝了一次（429），测速马上停了——那一刻在测的这把 Key 最近 60 秒发了 262 次、没被拒绝，"
                "主 Key自己最近 60 秒发了 221 次。") in text and text.split("\n")[-2] == "结论：到每分钟 280 次都没被拒绝，上限不低于 280（每档保持 120 秒）。", text
        # ended on the way: what was held stands
        text, line = ctl.speed_report(state(held(240), ended="手动停止"))
        assert "中途结束：手动停止" in text and "结论：到每分钟 240 次都没被拒绝，上限不低于 240（每档保持 120 秒）。" in text and "想知道到底多高" not in text, text
        text, line = ctl.speed_report(state(ended="预挂模式停止了"))
        assert "结论：没有测出结果（预挂模式停止了）。" in text and line == "没有测出结果（预挂模式停止了）"
        # the pace in force is the key's own where it has one
        m.PREDICT_RATE_OF["pre-key"] = 240.0
        text, _ = ctl.speed_report(state(held(240), held(260), refused(280, 268)))
        assert "建议：把控制台“预挂专用 Key：每分钟请求数”设成 245（" in text and "现在是 240，能快约 2%。" in text, text
        ctl.ledger.close()
    print("THE_REPORT_OK")


async def the_command() -> None:
    with Kept(**SCALED):
        m.PREDICT_RATE["per_min"] = 1200.0
        ctl, api, clock, rows = await mode(1)
        m._http_get = Predict(35)
        told: list[str] = []

        async def reply(req, text):
            told.append(text)
        bot = types.SimpleNamespace(preopen=ctl, reply=reply, preopen_share_line=lambda: "")
        ask = lambda *words: L.LiveBot.cmd_preopen(bot, m.Request("/preopen", list(words), 1, 0, 42))
        assert await ask("测速", "多少") == "用法：/preopen 测速（测到每分钟 400 次为止）｜/preopen 测速 600（测到 600，最多 1000）｜/preopen 测速 停"
        assert (await ask("测速", "停")).startswith("现在没有在跑的测速。") and (await ask("speed", "stop")).startswith("现在没有在跑的测速。")
        assert (await ask("nonsense")).endswith("｜/preopen probe｜/preopen 测速")
        words = await ask("测速", "6000")
        assert words.startswith("🧪 开始测速") and told == [], "said at once; the report when it is over"
        assert (await ask("测速")).startswith("测速已经在跑")
        await asyncio.wait_for(asyncio.gather(ctl.speed_task, return_exceptions=True), 30)
        for _ in range(200):
            if told:
                break
            await asyncio.sleep(0.01)
        assert len(told) == 1 and told[0] == p.SPEED_LAST["text"] and told[0].startswith("🧪 预挂专用 Key 测速（"), told
        # stopped on the way: the one who asked is told that too
        ctl.speed_api = None
        m._http_get = Predict(10_000)
        p.SPEED_TEST_STEP_S = 5.0
        assert (await ask("测速")).startswith("🧪 开始测速")
        await asyncio.sleep(0.2)
        assert (await ask("测速", "停")).startswith("测速已中止，这把 Key 的节拍回到每分钟 1200 次。")
        for _ in range(200):
            if len(told) == 2:
                break
            await asyncio.sleep(0.01)
        assert len(told) == 2 and "中途结束：手动停止" in told[1], told
        await over(ctl)
    # the paces a test may ask for: from the first step to the last, 400 unless said
    with Kept():
        ctl, api, clock, rows = await mode(1)
        m._http_get = Predict(10_000)
        for top, paces in ((0, [240, 260, 280, 300, 330, 360, 400]), (100, [240]), (600, [240, 260, 280, 300, 330, 360, 400, 450, 500, 550, 600]), (99_999, list(p.SPEED_TEST_STEPS))):
            task, words = ctl.speed_start(top)
            assert ctl.speed_state["paces"] == paces and f"每分钟 {'、'.join(map(str, paces))} 次，每档保持 120 秒" in words, (top, ctl.speed_state["paces"])
            ctl.speed_stop()
            await asyncio.gather(task, return_exceptions=True)
        await over(ctl)
    print("THE_COMMAND_OK")


def the_pace_of_a_step() -> None:
    """The pacer's tables: a step's pace stands over the key's own and every key's, for that key alone; a setting applied
    meanwhile leaves it; taken out, the configured pace is back."""
    with Kept():
        m.PREDICT_RATE["per_min"] = 220.0
        m.PREDICT_RATE_OF["pre-key"] = 240.0
        assert (m.predict_rate("pre-key"), m.predict_rate("main-key")) == (240.0, 220.0)
        m.PREDICT_RATE_TEST["pre-key"] = 300.0
        assert (m.predict_rate("pre-key"), m.predict_rate("main-key"), m.predict_pacer("pre-key").per_min) == (300.0, 220.0, 300.0)
        m.set_predict_rates(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREDICT_API_KEY": "main-key", "PREOPEN_API_KEY": "pre-key", "PREOPEN_RATE_PER_MIN": "260"}))
        assert m.predict_rate("pre-key") == 300.0 and m.PREDICT_RATE_OF == {"pre-key": 260.0}
        del m.PREDICT_RATE_TEST["pre-key"]
        assert m.predict_pacer("pre-key").per_min == 260.0
    assert p.SPEED_TEST_STEPS[0] == 240 and list(p.SPEED_TEST_STEPS) == sorted(set(p.SPEED_TEST_STEPS)) and p.SPEED_TEST_STEPS[-1] == 1000
    assert p.SPEED_TEST_STEP_S >= 2 * p.SPEED_TEST_WINDOW_S, "a step holds one whole minute of Predict's, however it cuts its minutes"
    assert p.SPEED_TEST_CLEAR_S >= L.PredictApi.BACKOFF_SECONDS + 20, "a 429's cooldown is over well before the mode's next moment"
    assert 1 + m.PredictPacer.LOW_RESERVE + p.SPEED_TEST_LEAVE <= m.PredictPacer.BURST, "the bucket can hold what a read of the test's waits for"
    print("THE_PACE_OF_A_STEP_OK")


the_pace_of_a_step()
the_report()
asyncio.run(the_stretches_a_step_runs_in())
asyncio.run(stopped_and_refused())
asyncio.run(what_ends_a_step())
asyncio.run(the_test_finds_the_limit())
asyncio.run(the_command())
print("LIMIT_OK")
