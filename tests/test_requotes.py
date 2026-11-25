"""1.154.49 — /diag says which slots re-quote, and why.

"撤单后随即重挂 N 单" counted the orders placed right after a confirmed cancel and no more. 11-25: 0.79 a second on 11-18, then
1.05, 1.3, 1.5 — each a cancel, the cancelled order's fills read and a new order on the bot's key: at 1.5 a second 270
requests a minute, more than the key has, and the reads it pushes out take the rule taker's idle key whole. Whether that is
a 买1 followed every two seconds, an edge flickering about the exit line or one slot gone wrong, nothing said.

Here: a withdrawal's reason without its figures (main.requote_kind) — one kind for every price it names; each re-quote
noted where it is made (LiveBot.live_requote_freed) with its slot and that kind; and the lines /diag adds — how many in the
last ten minutes and how fast, by reason and by slot, the most first. A display: nothing here decides anything. Offline.

Run: python tests/test_requotes.py
"""
import asyncio, pathlib, sys, time
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L

# test_trading's fakes (FakeApi, make_bot, step, hsi …) without running its suites: only the definitions before "async def run()"
_src = (ROOT / "tests" / "test_trading.py").read_text(encoding="utf-8").split("\nasync def run():")[0]
T: dict = {"__file__": str(ROOT / "tests" / "test_trading.py")}
exec(compile(_src, "test_trading_head", "exec"), T)
make_bot, step, hsi, NOW, HSI_SLUG = T["make_bot"], T["step"], T["hsi"], T["NOW"], T["HSI_SLUG"]
MAKER = f"{HSI_SLUG}|up|挂"
market = lambda at, fair=0.70, bids=(("0.55", "300"),), asks=(("0.58", "400"),): hsi(fair, list(bids), list(asks), at)


def the_kind_of_a_withdrawal() -> None:
    kind = m.requote_kind
    # the same rule at any price is one kind; the 1 of 买1 is no amount, nor are the digits of a name
    assert kind("买1 移到 54.0¢，改跟") == kind("买1 移到 61.5¢，改跟") == "买1 移到，改跟"
    assert kind("净优势降到 3.2¢，低于撤单线 5.0¢") == kind("净优势降到 -1.0¢，低于撤单线 4.0¢") == "净优势降到，低于撤单线"
    assert kind("买1 不足 100 份，不跟") == "买1 不足，不跟" and kind("买1 被顶到 54.0¢，改挂 55.0¢") == "买1 被顶到，改挂"
    assert kind("盘口断流超过 90 秒，撤单") == kind("盘口断流超过 5 分钟，撤单") == "盘口断流超过，撤单"
    assert kind("午后开盘后 3 分钟内不开新单，挂单撤掉（SIM_OPEN_QUIET_MINUTES）") == "午后开盘后 内不开新单，挂单撤掉"
    assert kind("A50 报价已 95 秒没有更新，撤单") == "A50 报价已 没有更新，撤单" and kind("KR200 代理价过旧（120 秒），撤单") == "KR200 代理价过旧，撤单"
    assert kind("每次份数已改为 100 份，按新份数重挂") == "每次份数已改为，按新份数重挂"
    assert kind("") == kind(None) == kind("12.5¢") == "其他" and len(kind("很长的理由" * 20)) <= 24
    # ...and the kinds differ where the rules do
    assert len({kind(w) for w in ("买1 移到 54.0¢，改跟", "买1 移到 54.0¢，改挂 55.0¢", "净优势降到 3.2¢，低于撤单线 5.0¢", "买1 不足 100 份，不跟")}) == 4
    print("THE_KIND_OF_A_WITHDRAWAL_OK")


async def a_requote_is_noted_where_it_is_made() -> None:
    bot = make_bot(SIM_WAYS="maker")
    await bot.live_prepare()
    await bot.live_prefetch(NOW)
    await step(bot, NOW, market(NOW))
    t = bot.sim_trades()[MAKER]
    assert t["live"]["state"] == "open" and not bot.live_requotes and bot.live_requote_lines() == []
    # the strategy's own withdrawal (the 买1 moved), its cancel confirmed: the slot is quoted again at once — and noted
    bot.sim_withdraw(t, "买1 移到 54.0¢，改跟", NOW + 1_000)
    t["withdrawn"]["rule"] = True
    bot.sim_save([(MAKER, t)])
    await bot.live_cancel_blind({MAKER: t}, NOW + 1_000)
    await bot.live_fills_wait()
    was = len(bot.live.api.calls)
    await bot.live_requote_freed([MAKER], NOW + 1_000)
    assert [k for k, _ in bot.live.api.calls[was:]] == ["create"] and bot.sim_requote_live == 1, bot.live.api.calls[was:]
    assert len(bot.live_requotes) == 1 and bot.live_requotes[0][1:] == ("恒生指数 挂涨", "买1 移到，改跟"), list(bot.live_requotes)
    lines = bot.live_requote_lines()
    assert lines[0].startswith("重挂明细（真实订单，近 ") and " 1 单 ≈ 每秒 " in lines[0] and "1 个档位" in lines[0], lines
    assert lines[1] == "· 按原因：买1 移到，改跟 1 单（100%）" and lines[2] == "· 恒生指数 挂涨：1 单（买1 移到，改跟 1）" and len(lines) == 3, lines
    # an operator's cancel (no rule's withdrawal) is followed by no order here, and noted as none
    fresh = bot.sim_trades()[MAKER]
    bot.sim_withdraw(fresh, "管理员撤单", NOW + 2_000)
    bot.sim_save([(MAKER, fresh)])
    await bot.live_cancel_blind({MAKER: fresh}, NOW + 2_000)
    await bot.live_fills_wait()
    await bot.live_requote_freed([MAKER], NOW + 2_000)
    assert len(bot.live_requotes) == 1 and bot.sim_requote_live == 1
    print("A_REQUOTE_IS_NOTED_WHERE_IT_IS_MADE_OK")


async def the_lines_diag_adds() -> None:
    bot = make_bot()
    await bot.live_prepare()
    now = time.monotonic()
    bot.live_requotes_since = now - 3600  # (an hour into the run: the ten minutes are whole)
    made = ([("恒生指数 挂涨", "买1 移到，改跟")] * 300 + [("恒生指数 挂涨", "净优势降到，低于撤单线")] * 60 + [("SHEIN 希音 挂跌", "买1 移到，改跟")] * 200
            + [("上证指数 挂涨", "净优势降到，低于撤单线")] * 120 + [("长鑫 CXMT 挂涨", "买1 不足，不跟")] * 40)
    made += [(f"档位 {n}", f"原因 {n % 7}") for n in range(180)]  # a long tail: a slot each
    bot.live_requotes.extend((now - 30 - (i % 500), name, kind) for i, (name, kind) in enumerate(made))
    bot.live_requotes.extend((now - 700, "很久以前 挂涨", "买1 移到，改跟") for _ in range(50))  # older than the ten minutes: not counted
    lines = bot.live_requote_lines()
    total = len(made)
    assert lines[0] == f"重挂明细（真实订单，近 10 分钟 {total} 单 ≈ 每秒 {total / 600:.2f} 单，184 个档位；每单在主 Key 上是撤单、读成交、新单 3 个请求）：", lines[0]
    assert lines[1].startswith("· 按原因：买1 移到，改跟 500 单（55%）｜净优势降到，低于撤单线 180 单（20%）｜买1 不足，不跟 40 单（4%）｜"), lines[1]
    assert lines[1].count("｜") == bot.REQUOTE_LINES_KINDS and lines[1].endswith(f"｜其余 {total - 500 - 180 - 40 - 26 - 26} 单"), lines[1]
    assert lines[2] == "· 恒生指数 挂涨：360 单（买1 移到，改跟 300｜净优势降到，低于撤单线 60）", lines[2]
    assert lines[3] == "· SHEIN 希音 挂跌：200 单（买1 移到，改跟 200）" and lines[4] == "· 上证指数 挂涨：120 单（净优势降到，低于撤单线 120）", lines[3:5]
    assert lines[5] == "· 长鑫 CXMT 挂涨：40 单（买1 不足，不跟 40）" and len(lines) == 2 + bot.REQUOTE_LINES_SLOTS + 1, lines
    assert lines[-1] == f"· 另有 {184 - bot.REQUOTE_LINES_SLOTS} 个档位共 {total - 360 - 200 - 120 - 40 - 4} 单", lines[-1]
    # early in a run the rate is over the run so far, not over ten minutes that have not passed
    bot.live_requotes.clear()
    bot.live_requotes_since = now - 120
    bot.live_requotes.extend((now - 10, "恒生指数 挂涨", "买1 移到，改跟") for _ in range(180))
    assert bot.live_requote_lines()[0].startswith("重挂明细（真实订单，近 2 分钟 180 单 ≈ 每秒 1.50 单，1 个档位"), bot.live_requote_lines()[0]
    # /diag carries them under the trader's line, before the takers' — and has none of it to say with none made
    bot.reference_state = {"模拟交易": {"ok_at": time.time(), "error": "", "error_at": 0.0, "ms": 5, "runs": 3, "running_since": 0.0, "status": "ok"}}
    text = "\n".join(bot.diag_state(NOW))
    assert "    重挂明细（真实订单，近 2 分钟 180 单" in text and "    · 恒生指数 挂涨：180 单（买1 移到，改跟 180）" in text, text[-1500:]
    bot.live_requotes.clear()
    assert "重挂明细" not in "\n".join(bot.diag_state(NOW))
    print("THE_LINES_DIAG_ADDS_OK")


async def main() -> None:
    the_kind_of_a_withdrawal()
    await a_requote_is_noted_where_it_is_made()
    await the_lines_diag_adds()
    print("REQUOTES_OK")


if __name__ == "__main__":
    asyncio.run(main())
