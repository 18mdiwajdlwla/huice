"""1.154.31: the probability page's 模拟交易 card is not summed afresh at every answer. sim_report was kept by the records'
generation — with a real maker a new one every second (11-19: 4,338 re-quotes in an hour and a half), so each answer of the page
summed all 14,668 records on the event loop: 142 stalls of 0.1–0.48 s in ten minutes, 5.4% of the loop while the page was open.
The page's report is kept a hundred times the seconds its build took, whatever was written meanwhile."""
import asyncio, sys, time, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

NOW = int(dt.datetime(2026, 11, 19, 10, 12, tzinfo=m.BEIJING).timestamp() * 1000)


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def record(n):
    return {"v": 2, "status": "resting", "market": f"m{n}", "slug": f"m{n}", "market_id": str(9000 + n), "item": "恒生指数", "kind": "close", "key": "HSI",
            "side": "up", "label": "挂涨", "maker": True, "fair": 0.5, "opened": NOW + n, "price": 0.4, "signal": 0.1, "edge": 0.1, "order": 10.0,
            "shares": 0.0, "filled": None, "queue_ahead": 0.0, "queue_min": 0.0, "settle": {"key": "HSI", "close_ms": NOW + 3_600_000},
            "driver": "HSI", "driver_name": "恒生指数", "fills": [], "revisions": [],
            "entry": {"at": NOW, "fair": 0.5, "fair_up": 0.5, "need": 0.03, "book": {}, "card": []}, "version": {}}


async def run():
    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}), m.Store(":memory:"), FM(NOW), None)
    assert (m.Bot.SIM_REPORT_KEEP_FACTOR, m.Bot.SIM_REPORT_KEEP_MAX_S, m.Bot.SIM_REPORT_KEEP_MIN_S) == (100.0, 120.0, 0.02)
    bot.sim_save([("t0", record(0))])
    sums, plain = [], m.sim_stats

    def slow(values, *args, **kwargs):  # the sum over every record, as slow as 14,668 of them are
        sums.append(len(list(values)))
        time.sleep(0.03)
        return plain(values, *args, **kwargs)
    m.sim_stats = slow
    try:
        first = bot.sim_report(public=True)
        n = len(sums)
        assert n >= 1 and first["total"]["trades"] == 1, first["total"]
        left = bot.sim_report_kept[1] - time.monotonic()
        assert 2.5 < left <= m.Bot.SIM_REPORT_KEEP_MAX_S, f"kept a hundred times the build ({left:.1f} s)"
        # a record written (the generation moves, as at every re-quote): the page's report stands — nothing summed
        bot.sim_save([("t1", record(1))])
        assert bot.sim_report(public=True) is first and len(sums) == n
        # /sim's own (and the control page's: not ``public``) is built afresh, and does not replace the page's
        mine = bot.sim_report(recent=10)
        assert mine is not first and mine["total"]["trades"] == 2 and len(sums) > n and bot.sim_report(public=True) is first
        # a setting the report states changed: built afresh at once
        n = len(sums)
        bot.config = m.dataclasses.replace(bot.config, sim_shares=bot.config.sim_shares + 1)
        changed = bot.sim_report(public=True)
        assert changed is not first and changed["shares"] == bot.config.sim_shares and changed["total"]["trades"] == 2 and len(sums) > n
        # its time over: built afresh, with what was written meanwhile
        bot.sim_save([("t2", record(2))])
        assert bot.sim_report(public=True) is changed
        bot.sim_report_kept = (bot.sim_report_kept[0], time.monotonic() - 0.001, bot.sim_report_kept[2])
        later = bot.sim_report(public=True)
        assert later is not changed and later["total"]["trades"] == 3
        # never longer than SIM_REPORT_KEEP_MAX_S, however slow the build
        bot.SIM_REPORT_KEEP_MAX_S = 1.0
        bot.sim_save([("t3", record(3))])
        bot.sim_report_kept = None
        bot.sim_report(public=True)
        assert 0.5 < bot.sim_report_kept[1] - time.monotonic() <= 1.0
    finally:
        m.sim_stats = plain
    # a build that cost nothing (a few records): not kept — the page shows a write at its next answer, as before
    quick = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}), m.Store(":memory:"), FM(NOW), None)
    quick.sim_save([("t0", record(0))])
    one = quick.sim_report(public=True)
    assert "sim_report_kept" not in quick.__dict__ and quick.sim_report(public=True) is one, "(the same records: the generation's cache, as ever)"
    quick.sim_save([("t1", record(1))])
    assert quick.sim_report(public=True)["total"]["trades"] == 2
    print("REPORT_KEEP_OK")


asyncio.run(run())
