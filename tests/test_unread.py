"""1.154.46 — the confirmed cancels whose fills were never read back.

A maker's cancel confirmed, its record ends at once and the fills it took before the cancel are read beside the lane
(live_cancel_fills); until that read the record carries a mark (fills_pending): its unfilled shares stay reserved, for
FILLS_PENDING_MAX_MS at the most, and a record carrying one is an open record (core.trade_open). Two of them nothing
looked at again: one whose first read was lost with the process that began it (a deploy), and one whose mark had run out —
it reserved nothing any more, nothing read it again, and it stayed open for good.

11-25, production: the bot's key past its pace for hours (1.154.45), those reads queuing behind its orders and lost at each
of three deploys — "25778 条记录（其中 4207 条还没结束" where 46 were a week before, and the event loop held 4.0% of the time,
176 stalls in ten minutes under live_exposure / live_reserved / fills_pending / side_held: every look at what the account
ties up walked four thousand dead records.

Here: a lost read is read again (counted, by the usual waits) and what it finds booked; a mark that has run out is taken
off and the record is no longer open — its own times untouched, every figure the caps go by the same to the last digit,
a read still in flight left alone, a few hundred a step at the most; the pause and the loss switch are read from the
store once a write, not at every gate; and the attempts are pruned at the step after the first whole view of the account,
not an hour into every start. Entirely offline.

Run: python tests/test_unread.py
"""
import asyncio, copy, pathlib, sys, time
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L

# test_trading's fakes (FakeApi, make_bot, step, hsi …) without running its suites: only the definitions before "async def run()"
_src = (ROOT / "tests" / "test_trading.py").read_text(encoding="utf-8").split("\nasync def run():")[0]
T: dict = {"__file__": str(ROOT / "tests" / "test_trading.py")}
exec(compile(_src, "test_trading_head", "exec"), T)
make_bot, step, hsi, NOW, HSI_SLUG, world = T["make_bot"], T["step"], T["hsi"], T["NOW"], T["HSI_SLUG"], T["world"]
MAKER = f"{HSI_SLUG}|up|挂"
market = lambda at, fair=0.70, bids=(("0.55", "300"),), asks=(("0.58", "400"),): hsi(fair, list(bids), list(asks), at)
OLD = lambda: int(time.time() * 1000) - m.FILLS_PENDING_MAX_MS - 60_000  # a mark that ran out a minute ago


def record_of(bot, oid: str) -> tuple[str, dict]:
    return next((k, v) for k, v in bot.sim_trades().items() if (v.get("live") or {}).get("order_id") == oid)


async def maker_out(**env):
    """A bot with one resting maker order of 100 at 55¢ on Predict (the fake): (bot, fake, its record, its order id)."""
    bot = make_bot(SIM_WAYS="maker", **env)
    await bot.live_prepare()
    fake = bot.live.api
    await bot.live_prefetch(NOW)
    await step(bot, NOW, market(NOW))
    t = bot.sim_trades()[MAKER]
    assert t["live"]["state"] == "open" and float(t["order"]) == 100, t["live"]
    return bot, fake, t, t["live"]["order_id"]


async def cancelled_with_its_read_lost(filled: float = 0.0):
    """...its cancel confirmed, the read of its fills begun — and the process replaced before Predict answered it."""
    bot, fake, t, oid = await maker_out()
    if filled:
        fake.fill(oid, filled)  # matched just before the cancel: after the bot's last read
    never = asyncio.Event()

    async def hangs(oid_, hash_=""):
        await never.wait()
    fake.order = hangs
    bot.sim_withdraw(t, "买1 变了（测试）", NOW + 1_000)
    bot.sim_save([(MAKER, t)])
    await bot.live_cancel_blind({MAKER: t}, NOW + 1_000)
    assert len(bot.live_fill_tasks) == 1 and oid in bot.live_fills_reading
    for task in list(bot.live_fill_tasks):  # the deploy: the task is gone, the record as it was saved
        task.cancel()
    await asyncio.gather(*list(bot.live_fill_tasks), return_exceptions=True)
    await asyncio.sleep(0)
    del fake.order
    tid, t = record_of(bot, oid)
    st = t["live"]
    assert st["state"] == "done" and st.get("fills_pending") and not st.get("fills_reread_at") and not st.get("fills_reads") and not bot.live_fills_reading, st
    assert float(t["shares"]) == 0 and m.trade_open(t) and abs(bot.live_reserved(t) - 55.0) < 1e-9, "its 100 shares at 55¢ still held on the mark"
    return bot, fake, tid, t, oid


async def a_lost_read_is_read_again():
    bot, fake, tid, t, oid = await cancelled_with_its_read_lost(filled=30)
    st = t["live"]
    # the step's look: counted, due again by the usual waits should this read be lost too — and read now, once
    bot.live_fills_reread(NOW + 2_000)
    assert len(bot.live_fill_tasks) == 1 and st["fills_reads"] == 1 and st["fills_reread_at"] == NOW + 2_000 + bot.LIVE_FILLS_REREAD_MS[0], st
    bot.live_fills_reread(NOW + 2_000)
    assert len(bot.live_fill_tasks) == 1, "one read at a time an order"
    await bot.live_fills_wait()
    tid, t = record_of(bot, oid)
    st = t["live"]
    assert float(t["shares"]) == 30 and t["status"] == "filled" and not st.get("fills_pending") and not st.get("fills_reread_at") and "fills_unread" not in st, (t["shares"], st)
    assert abs(bot.live_reserved(t) - 16.5) < 1e-9 and any("撤单确认后读到成交 30 份" in e["what"] for e in st["events"]), st["events"]
    # one whose next read is set (a read that was not Predict's last word): as before — not before its time, then read
    bot, fake, tid, t, oid = await cancelled_with_its_read_lost()
    t["live"].update(fills_reads=1, fills_reread_at=NOW + 5_000)
    bot.live_fills_reread(NOW + 4_999)
    assert not bot.live_fill_tasks and t["live"]["fills_reads"] == 1
    bot.live_fills_reread(NOW + 5_000)
    assert len(bot.live_fill_tasks) == 1 and t["live"]["fills_reads"] == 1, "its own read counts itself when it ends"
    await bot.live_fills_wait()
    assert not record_of(bot, oid)[1]["live"].get("fills_pending")
    # not one whose order is not at its end (a cancel in flight carries no mark; were it to, it is the cancel's own to read)
    bot, fake, tid, t, oid = await cancelled_with_its_read_lost()
    t["live"]["state"] = "cancelling"
    bot.live_fills_reread(NOW + 2_000)
    assert not bot.live_fill_tasks and t["live"].get("fills_pending") and not t["live"].get("fills_reads")
    print("A_LOST_READ_IS_READ_AGAIN_OK")


async def a_mark_run_out_is_closed():
    bot, fake, tid, t, oid = await cancelled_with_its_read_lost()
    st = t["live"]
    index = bot.sim_kept()
    st["fills_pending"] = at = OLD()
    st["fills_reread_at"] = NOW + 60_000  # (a read that had not been Predict's last word, its next one set — never made)
    bot.sim_save([(tid, t)])
    assert tid in index.open and m.trade_open(t) and not m.fills_pending(st) and bot.live_reserved(t) == 0.0, "run out: it holds nothing — and is open all the same"
    updated, events, exposure = m.trade_updated(t), copy.deepcopy(st["events"]), bot.live_exposure(bot.sim_trades())
    held = m.side_held(bot.sim_trades(), t["market"], "up")
    # a read of it still on its way: left alone (its end takes the mark off itself, or sets the next read)
    bot.live_fills_reading.add(oid)
    bot.live_fills_reread(NOW + 2_000)
    assert st.get("fills_pending") == at and tid in index.open
    bot.live_fills_reading.discard(oid)
    # the step's look: the mark off, said on the record, saved — no longer open; nothing read
    bot.live_fills_reread(NOW + 2_000)
    assert "fills_pending" not in st and "fills_reread_at" not in st and st["fills_unread"] == at and not bot.live_fill_tasks, st
    assert tid not in index.open and not m.trade_open(t) and bot.live_fills_closed == 1
    assert bot.store.get(f"sim:{tid}")["live"].get("fills_unread") == at and "fills_pending" not in bot.store.get(f"sim:{tid}")["live"], "saved"
    # its own times as they were (the minute's positions check goes by them: which ended order is the newest), its events too
    assert m.trade_updated(t) == updated and st["events"] == events and bot.live_updated(tid, t) == updated
    # and every figure the caps go by, to the last digit
    assert bot.live_exposure(bot.sim_trades()) == exposure and m.side_held(bot.sim_trades(), t["market"], "up") == held
    assert not m.sim_open_order(t) and bot.live_reserved(t) == 0.0
    bot.live_fills_reread(NOW + 3_000)
    assert bot.live_fills_closed == 1, "once"
    # a position (shares filled before the cancel, read; a later mark run out): the mark off, the position open as ever
    bot, fake, tid, t, oid = await cancelled_with_its_read_lost()
    t.update(status="filled", shares=40.0)
    t["live"]["fills_pending"] = OLD()
    bot.sim_save([(tid, t)])
    bot.live_fills_reread(NOW + 2_000)
    assert "fills_pending" not in t["live"] and m.trade_open(t) and tid in bot.sim_kept().open and abs(bot.live_reserved(t) - 22.0) < 1e-9
    print("A_MARK_RUN_OUT_IS_CLOSED_OK")


async def thousands_are_closed_a_few_hundred_a_step():
    bot, fake, tid, t, oid = await cancelled_with_its_read_lost()
    index = bot.sim_kept()
    opened = len(index.open)
    stale = OLD()
    rows = []
    for n in range(600):  # the attempts of an afternoon of re-quotes, each ended on a confirmed cancel whose read never came back
        r = copy.deepcopy(t)
        r["live"].update(order_id=f"9{n:05d}", hash=f"0x{n:064x}", fills_pending=stale - n)
        rows.append((f"{MAKER}#{n + 1}", r))
    bot.sim_save(rows)
    t["live"]["fills_pending"] = stale
    bot.sim_save([(tid, t)])
    assert len(index.open) == opened + 600 and all(bot.live_reserved(r) == 0.0 for _, r in rows)
    trades = bot.sim_trades()
    exposure, group = bot.live_exposure(trades), bot.sim_window_open_usd(trades, "HSI")
    fit = bot.live_fit_shares(trades, 0.55)
    left, a_step = [], bot.LIVE_FILLS_CLOSE_STEP
    for n in range(8):
        bot.live_fills_reread(NOW + 2_000 * (n + 1))
        left.append(sum(1 for _, r in bot.sim_trades().items() if (r.get("live") or {}).get("fills_pending")))
    assert 100 <= a_step <= 300 and left == [max(0, 601 - a_step * (n + 1)) for n in range(8)] and left[-1] == 0 and bot.live_fills_closed == 601, left
    assert len(index.open) == opened - 1 and not bot.live_fill_tasks, "none of them read: nothing reads a mark run out — and none open"
    trades = bot.sim_trades()
    assert (bot.live_exposure(trades), bot.sim_window_open_usd(trades, "HSI"), bot.live_fit_shares(trades, 0.55)) == (exposure, group, fit), "the same figures"
    assert index.drift() == [], "the index and the records in step"
    print("THOUSANDS_ARE_CLOSED_A_FEW_HUNDRED_A_STEP_OK")


async def the_switches_are_read_once_a_write():
    bot = make_bot()
    await bot.live_prepare()
    reads: list[str] = []
    get = bot.store.get

    def counted(key, default=None):
        reads.append(key)
        return get(key, default)
    bot.store.get = counted
    for _ in range(50):
        assert bot.live_paused() == "" and bot.live_killed() == "" and bot.live_kill_record() == {}
    assert reads.count("live:paused") == 1 and reads.count("live:killed") == 1, reads
    # a pause (the owner's): read again at the next gate, and once
    bot.store.put("live:paused", {"at": NOW, "why": "先停一下"})
    assert [bot.live_paused() for _ in range(20)] == ["先停一下"] * 20 and reads.count("live:paused") == 2
    assert "先停一下" in bot.live_room({}, market(NOW), "up", 0.55, 100), "the gate says so"
    bot.store.delete_keys(["live:paused"])
    assert bot.live_paused() == "" and reads.count("live:paused") == 3
    # the loss switch: tripped, lifted — and its day still judged at every call
    day = m.beijing_day(NOW / 1000)
    bot.store.put("live:killed", {"day": day, "why": "今日已结算亏损 -$50.00 达到 LIVE_MAX_DAILY_LOSS_USD $50", "at": NOW})
    assert bot.live_killed().startswith("今日已结算亏损") and bot.live_killed().startswith("今日已结算亏损")
    record = bot.live_kill_record()
    record["why"] = "changed by the caller"  # (a copy of its top: what the next caller is handed is the store's)
    assert bot.live_kill_record()["why"].startswith("今日已结算亏损")
    bot.store.put("live:killed", {**bot.live_kill_record(), "resumed": True, "resumed_pnl": -50.0})
    assert bot.live_killed() == "" and bot.live_kill_record()["resumed"] is True
    n = reads.count("live:killed")
    bot.market.now = NOW + 2 * m.DAY_MS  # another day: yesterday's record is none
    assert bot.live_kill_record() == {} and bot.live_killed() == "" and reads.count("live:killed") == n, "no read for it"
    bot.market.now = NOW
    # a write of another family is no reason to read again; a store that keeps no count is read every time (a test's stand-in)
    bot.live_paused(); bot.live_killed()  # (each read once since the family's last write)
    n = len(reads)
    bot.store.put("sim:x", {"status": "cancelled"})
    bot.live_paused(); bot.live_killed()
    assert len(reads) == n, reads[n:]
    bot.store = type("Plain", (), {"get": staticmethod(lambda key, default=None: {"why": "手动"} if key == "live:paused" else default)})()
    assert bot.live_paused() == "手动"
    print("THE_SWITCHES_ARE_READ_ONCE_A_WRITE_OK")


async def the_attempts_are_pruned_once_the_account_is_seen_whole():
    bot, fake, t, oid = await maker_out()
    assert bot.live_open_ids_at, "the first sync saw the account whole"
    # as a start finds it: the records of an afternoon's re-quotes, the account not read yet, the start's own prune just made
    rows = []
    for n in range(30):
        r = copy.deepcopy(t)
        r.update(status="cancelled", shares=0.0, unfilled=100.0, opened=NOW - 3_600_000 + n)
        r["live"].update(state="done", order_id=f"8{n:05d}", hash=f"0x{n + 1000:064x}", events=[{"at": NOW - 3_600_000 + n, "what": "撤单"}])
        rows.append((f"{MAKER}#{n + 1}", r))
    bot.sim_save(rows)
    bot.live_open_ids, bot.live_open_ids_at = set(), 0
    bot.sim_pruned_at = time.monotonic()
    assert bot.sim_prune() == 0, "no whole view yet: none of a real order's attempts may go (prunable)"
    kept = len(bot.sim_trades())
    await step(bot, NOW + 2_000, market(NOW + 2_000))
    assert bot.live_open_ids_at and bot.sim_pruned_at == -1e9 and len(bot.sim_trades()) == kept, "seen whole: looked for at the next step"
    await step(bot, NOW + 4_000, market(NOW + 4_000))
    assert len(bot.sim_trades()) == kept - (30 - bot.SIM_ATTEMPTS_KEEP) and bot.sim_pruned_at > 0, (len(bot.sim_trades()), kept)
    assert MAKER in bot.sim_trades() and bot.sim_trades()[MAKER]["live"]["state"] == "open", "the resting order as it was"
    at = bot.sim_pruned_at
    await step(bot, NOW + 6_000, market(NOW + 6_000))
    assert bot.sim_pruned_at == at, "once a start: then every SIM_PRUNE_SECONDS, as before"
    print("THE_ATTEMPTS_ARE_PRUNED_ONCE_THE_ACCOUNT_IS_SEEN_WHOLE_OK")


async def main() -> None:
    await a_lost_read_is_read_again()
    await a_mark_run_out_is_closed()
    await thousands_are_closed_a_few_hundred_a_step()
    await the_switches_are_read_once_a_write()
    await the_attempts_are_pruned_once_the_account_is_seen_whole()
    print("UNREAD_OK")


if __name__ == "__main__":
    asyncio.run(main())
