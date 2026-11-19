"""1.154.31 — the stocks' realtime quotes come round faster. A daily card of a stock is taken on the exchange's own price while
it trades; the job read Tencent (the A-shares and Hong Kong, one request) and then Naver (each Korean stock) one after the other
and came round a second after both — a round every 1.8 s on 11-19. Now the Korean part runs beside it, the job comes round on
the second, and a stock whose price moved has its card looked at now (1.154.32: at once, not after the pushes' gathering)."""
import asyncio, sys, json, time, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
D = m.D
tz8 = dt.timezone(dt.timedelta(hours=8))
NOW = int(dt.datetime(2026, 9, 28, 10, 1, tzinfo=tz8).timestamp() * 1000)  # Shanghai and Seoul both trading


def tencent(price):
    return (f'v_sh688836="1~宇树科技~688836~{price}~76.50~75.20~' + "~".join(["0"] * 24) + '~20260928100100~x";').encode("gbk")


def naver(price):
    return json.dumps({"datas": [{"stockName": "SK하이닉스", "closePrice": price, "compareToPreviousClosePrice": "13,000",
                                  "compareToPreviousPrice": {"code": "2"}, "localTradedAt": "2026-09-28T11:01:00+09:00", "marketStatus": "OPEN"}]}).encode()


class FakeMarket(m.Binance):
    def __init__(self, c): super().__init__(c); self.now = NOW
    def now_ms(self): return self.now


def bot(symbols):
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": symbols})
    return m.Bot(cfg, m.Store(":memory:"), FakeMarket(cfg), None)


async def run():
    world = {"sh": "78.00", "kr": "1,870,000", "naver_s": 0.4, "calls": []}

    async def fake_get(url, timeout=15, headers=None):
        world["calls"].append(("腾讯" if "gtimg" in url else "Naver" if "naver" in url else url, time.monotonic()))
        if "gtimg" in url:
            return tencent(world["sh"])
        if "naver" in url:
            await asyncio.sleep(world["naver_s"])  # far from here: the slow one
            return naver(world["kr"])
        raise AssertionError(url)
    m.http_get = fake_get

    # --- the two kinds of source, each a part of its own: Tencent's round does not wait for Naver's ------------------------------
    b = bot("UNITREEUSDT,SKHYNIXUSDT")
    looked = []
    b.predict.books.update({"UNITREEUSDT": None, "SKHYNIXUSDT": None})  # (a daily card's book key is its symbol)
    b.predict.updated = looked.append
    began = time.monotonic()
    result = await b.refresh_stock_live(NOW)
    took = time.monotonic() - began
    assert isinstance(result, m.Refreshed) and took < world["naver_s"] * 0.75, (result, took)
    assert b.stocks.live["UNITREEUSDT"].last == D("78.00") and "SKHYNIXUSDT" not in b.stocks.live, "Shanghai's is in; Seoul's still on its way"
    assert looked == ["UNITREEUSDT"] and b.price_seen == {"UNITREEUSDT"}, "a price that moved: its card is looked at now (priced, at once)"
    assert b.price_dirty == set() and b.price_look is None, "...not gathered with the pushes for PRICE_LOOK_DELAY (1.154.32)"
    kr_task = b.stock_kr_task
    assert not kr_task.done() and await b.refresh_stock_live(NOW) is False and b.stock_kr_task is kr_task, "within the second: nothing asked again, no second Korean round"
    await kr_task
    assert b.stocks.live["SKHYNIXUSDT"].last == D("1870000") and [name for name, _ in world["calls"]] == ["腾讯", "Naver"]
    assert looked == ["UNITREEUSDT", "SKHYNIXUSDT"], "...and Seoul's, when it came"
    # the job says a Korean round ended even when its own part was not due, once
    assert b.stock_kr_ended is True and isinstance(await b.refresh_stock_live(NOW), m.Refreshed) and b.stock_kr_ended is False
    assert await b.refresh_stock_live(NOW) is False
    # the same prices again: nothing moved, nothing looked at
    b.stocks.live_refreshed_part.clear()
    await b.refresh_stock_live(NOW)
    await b.stock_kr_task
    assert looked == ["UNITREEUSDT", "SKHYNIXUSDT"], looked
    # a new print in Seoul alone
    world["kr"] = "1,880,000"
    b.stocks.live_refreshed_part.clear()
    await b.refresh_stock_live(NOW)
    await b.stock_kr_task
    assert b.stocks.live["SKHYNIXUSDT"].last == D("1880000") and looked == ["UNITREEUSDT", "SKHYNIXUSDT", "SKHYNIXUSDT"]
    # --- StockMarket.refresh_live: a part at a time, each on its own stamp; due a hair before its second is up ---------------------
    s = b.stocks
    n = len(world["calls"])
    assert await s.refresh_live(NOW, only="rest") is False and await s.refresh_live(NOW, only="kr") is False and len(world["calls"]) == n
    s.live_refreshed_part["rest"] = time.monotonic() - (b.config.quote_refresh - s.LIVE_DUE_SLACK / 2)
    assert await s.refresh_live(NOW, only="rest") is not False and [name for name, _ in world["calls"][n:]] == ["腾讯"], "0.975 s on: due (the loop comes round on the second)"
    assert await s.refresh_live(NOW, force=True, only="kr") is not False and [name for name, _ in world["calls"][n:]] == ["腾讯", "Naver"]
    assert s.live_refreshed == -1e9 and await s.refresh_live(NOW) is not False and len(world["calls"]) == n + 4, "the whole of them (no part named): as ever, its own stamp"
    assert await s.refresh_live(NOW) is False
    # --- one kind of source alone: the round as it was (nothing beside it) ------------------------------------------------------------
    for symbols, name in (("UNITREEUSDT", "腾讯"), ("SKHYNIXUSDT", "Naver")):
        one = bot(symbols)
        n = len(world["calls"])
        assert isinstance(await one.refresh_stock_live(NOW), m.Refreshed) and [x for x, _ in world["calls"][n:]] == [name]
        # (no Predict book of the card's in hand: nothing to look at, nothing taken as looked at)
        assert "stock_kr_task" not in one.__dict__ and one.price_dirty == set() and one.price_look is None and not one.__dict__.get("price_seen")
    # --- the job comes round on the second: REFERENCE_TICK between two rounds' beginnings, not after a round's end -----------------
    plain_tick, m.REFERENCE_TICK = m.REFERENCE_TICK, 0.3
    try:
        loop_bot = bot("UNITREEUSDT")
        loop_bot.ON_THE_SECOND_MIN = 0.05
        starts = {"股票实时": [], "汇率": []}

        def job(name):
            async def run_once():
                starts[name].append(time.monotonic())
                await asyncio.sleep(0.12)
                return m.Refreshed("ok")
            return run_once
        tasks = [asyncio.create_task(loop_bot.reference_loop(name, job(name))) for name in starts]
        await asyncio.sleep(1.5)
        loop_bot.stopping.set()
        await asyncio.gather(*tasks)
        gaps = {name: sorted(b2 - a for a, b2 in zip(at, at[1:])) for name, at in starts.items()}
        mid = {name: g[len(g) // 2] for name, g in gaps.items()}
        # (medians, and the two against each other: a busy builder stretches every sleep alike)
        assert len(gaps["股票实时"]) >= 3 and mid["股票实时"] >= 0.29 and mid["汇率"] >= 0.41 and mid["汇率"] - mid["股票实时"] >= 0.06, (mid, gaps)
        assert "股票实时" in m.Bot.ON_THE_SECOND and "汇率" not in m.Bot.ON_THE_SECOND and m.Bot.ON_THE_SECOND_MIN == 0.2
    finally:
        m.REFERENCE_TICK = plain_tick
    for x in (b,):
        if x.price_look is not None:
            x.price_look.cancel()
    print("STOCK_LIVE_OK")


asyncio.run(run())
