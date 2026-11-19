"""1.154.32 — the three index feeds come round as the stocks' quotes of 1.154.31 do. A daily card of an index (恒生, KOSPI, 上证)
is taken on the index's own price while its market trades, and that price is polled. 11-19: a round every 1.5–2.7 s for a cadence
of one second — the job rested a whole second after each round, the round read two things one after the other, and the HSI's
was the futures page or the cash index, never both; then the move waited for the fair signature's next check (one a second for
all the feeds together) and the pushes' gathering. Now: the jobs come round on the second, the two reads of a round go side by
side, the HSI's cash index is read beside the page, and the card is looked at the moment its price is stored."""
import asyncio, dataclasses, sys, json, time, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live
D = m.D
tz8, tz9 = m.BEIJING, dt.timezone(dt.timedelta(hours=9))


def bj(d, h, mi, s=0): return int(dt.datetime(2026, 9, d, h, mi, s, tzinfo=tz8).timestamp() * 1000)


NOW = bj(28, 10, 30)  # Monday 09-28, 10:30 in Beijing: Shanghai, Hong Kong and Seoul (11:30 there) all trading
CFG = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT"})
HK, KR, SH = (CFG.holidays[x] for x in ("hk", "kr", "sh"))
# (etnet's page as it is on a trading morning: the day block running, the night block the one before it — the same 前收市)
PAGE = """<div><h3>恒生指數期貨(09/2026)</h3><span>日市</span><div>&#9660;24,522 -169 (-0.68%) 高水12</div>
<div>最高: 24,643 最低: 24,265 前收市: 24,691 開市: 24,630</div><img alt="chart"></div>
<div><h3>恒生指數期貨(09/2026)</h3><span>夜市</span><div>&#9660;24,650 -41 (-0.17%) 低水9</div>
<div>最高: 24,700 最低: 24,600 前收市: 24,691 開市: 24,680</div><img alt="chart"></div>
<div>未平倉 (到期日：29/09/2026)</div>
<div>恒生指數現貨 &#9660;24,510.09 -251.04 (-1.01%) 最高: 24,537.14 最低: 24,275.56 前收市: 24,761.13 開市: 24,523.50</div>"""


def naver(price, at_ms):
    return json.dumps({"datas": [{"itemCode": "KOSPI", "stockName": "코스피", "closePrice": price, "compareToPreviousClosePrice": "12.34",
                                  "compareToPreviousPrice": {"code": "2"}, "fluctuationsRatio": "0.37", "openPrice": "3,360.10",
                                  "highPrice": "3,380.50", "lowPrice": "3,355.20", "marketStatus": "OPEN",
                                  "localTradedAt": dt.datetime.fromtimestamp(at_ms / 1000, tz9).isoformat()}]}).encode()


def tencent_sh(price, at_ms):
    return (f'v_sh000001="1~上证指数~000001~{price}~3830.00~3835.00~' + "~".join(["0"] * 24)
            + f'~{dt.datetime.fromtimestamp(at_ms / 1000, tz8):%Y%m%d%H%M%S}~x";').encode("gbk")


def a50(price, at_ms):
    return json.dumps({"data": {"f43": price, "f60": 14100, "f86": at_ms // 1000, "f58": "A50期指当月连续"}}).encode()


def sina_hsi(price, at_ms):
    when = dt.datetime.fromtimestamp(at_ms / 1000, tz8)
    return (f'var hq_str_rt_hkHSI="HSI,恒生指数,24523.50,24761.13,24537.14,24275.56,{price},' + ",".join(["0"] * 10)
            + f',{when:%Y/%m/%d},{when:%H:%M:%S}";').encode("gbk")


class FakeMarket(m.Binance):
    def __init__(self, c): super().__init__(c); self.now = NOW
    def now_ms(self): return self.now


def bot(cls=m.Bot):
    cfg = CFG if cls is m.Bot else m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "LIVE": "on",
                                                      "PREDICT_PRIVATE_KEY": "a" * 64, "PREDICT_API_KEY": "k"})
    return cls(cfg, m.Store(":memory:"), FakeMarket(cfg), None)


def kind(url):
    for name, part in (("页", "etnet"), ("恒指现货", "rt_hkHSI"), ("KOSPI200", "realtime/domestic/index/KPI200"), ("KOSPI", "realtime/domestic/index/KOSPI"),
                       ("上证", "qt.gtimg.cn/q=sh000001"), ("A50", "push2.eastmoney.com/api/qt/stock/get?fltt=2&invt=2&fields=f43,f44,f45,f46,f57,f58,f60,f86&secid=104.CN00Y")):
        if part in url:
            return name
    return "其他"  # (the daily bars, the sources further down the lists: answered "skip")


async def run():
    world = {"wait": {}, "calls": [], "kospi": "3,371.89", "sse": "3850.12", "a50": 14120, "hsi": "24600.00", "at": NOW}

    async def fake_get(url, timeout=15, headers=None):
        name = kind(url)
        world["calls"].append((name, time.monotonic()))
        await asyncio.sleep(world["wait"].get(name, 0))
        if name in ("KOSPI", "KOSPI200"):
            return naver(world["kospi"], world["at"])
        if name == "上证":
            return tencent_sh(world["sse"], world["at"])
        if name == "A50":
            return a50(world["a50"], world["at"])
        if name == "页":
            return PAGE.encode("utf-8")
        if name == "恒指现货":
            return sina_hsi(world["hsi"], world["at"])
        raise m.RemoteError("skip")  # (the daily bars, the other sources: not this test's)

    async def no_json(*a, **k):
        raise m.RemoteError("skip")
    m.http_get, m.http_json, m.SOURCE_HEALTH = fake_get, no_json, m.SourceHealth()
    asked = lambda since: [name for name, _ in world["calls"][since:] if name != "其他"]

    # --- KOSPI: the index and KOSPI200 side by side; the index told the moment it is stored --------------------------------------
    k = m.KospiIndex(True, KR)
    assert k.cadence(NOW) == 1 and m.KospiIndex.on_quote is None if hasattr(m.KospiIndex, "on_quote") else k.cadence(NOW) == 1
    told = []
    k.on_quote = lambda: told.append((time.monotonic(), k.quote.last if k.quote else None))
    world["wait"] = {"KOSPI": 0.4, "KOSPI200": 0.4}  # (wide of each other: a busy builder stretches a sleep, not by this much)
    began = time.monotonic()
    result = await k.refresh(NOW)
    took = time.monotonic() - began
    assert isinstance(result, m.Refreshed) and k.quote.last == D("3371.89") and k.quote200.last == D("3371.89") and not k.error
    assert took < 0.7, f"side by side: the two reads together (0.4 s), not one after the other (0.8 s) — {took:.2f} s"
    assert sorted(asked(0)) == ["KOSPI", "KOSPI200"] and len(told) == 1 and told[0][1] == D("3371.89")
    world["wait"] = {"KOSPI": 0.02, "KOSPI200": 0.5}
    world["kospi"], world["at"] = "3,372.50", NOW + 1000
    began, told[:] = time.monotonic(), []
    await k.refresh(NOW + 1000, force=True)
    assert told and told[0][0] - began < 0.35 and told[0][1] == D("3372.50") and time.monotonic() - began >= 0.49, \
        "the card's price is told as it arrives, not when KOSPI200's answer ends the round"
    assert await k.refresh(NOW + 1000) is False, "within the second: nothing asked again"
    # KOSPI200 — shown, no card taken on it — every K200_SECONDS at the most: the second's round is the index alone
    k.refreshed -= 1.0
    n = len(world["calls"])
    await k.refresh(NOW + 2000)
    assert asked(n) == ["KOSPI"] and k.K200_SECONDS == 5, asked(n)
    k.refreshed -= 1.0
    k.refreshed200 -= 5.0
    n = len(world["calls"])
    await k.refresh(NOW + 3000)
    assert sorted(asked(n)) == ["KOSPI", "KOSPI200"], asked(n)

    # --- 上证 / A50: likewise ------------------------------------------------------------------------------------------------------
    c = m.CnIndex(True, SH)
    told = []
    c.on_quote = lambda: told.append((time.monotonic(), c.quote.last if c.quote else None, c.a50.last if c.a50 else None))
    world["wait"], world["at"] = {"上证": 0.4, "A50": 0.4}, NOW
    n, began = len(world["calls"]), time.monotonic()
    result = await c.refresh(NOW)
    took = time.monotonic() - began
    assert c.quote.last == D("3850.12") and c.a50.last == D("14120") and not c.error and not c.a50_error, (c.error, c.a50_error)
    assert took < 0.7 and sorted(asked(n)) == ["A50", "上证"], (took, asked(n))
    assert c.minutes == {NOW // 60_000 * 60_000: 3850.12}, "the minute's price is still kept (note_minute)"
    world["wait"] = {"上证": 0.02, "A50": 0.5}
    world["sse"], world["a50"], world["at"] = "3851.00", 14130, NOW + 1000
    began, told[:] = time.monotonic(), []
    await c.refresh(NOW + 1000, force=True)
    assert [(q, a) for _, q, a in told] == [(D("3851.00"), D("14120")), (D("3851.00"), D("14130"))], told
    assert told[0][0] - began < 0.35 and told[1][0] - began >= 0.49, "the Composite told as it arrives; the A50 when it does"
    # a source that fails: the other part still read and told, the round says which failed
    world["wait"] = {}
    real = fake_get

    async def a50_down(url, timeout=15, headers=None):
        if "104.CN00Y" in url or "CHA50" in url:
            raise m.RemoteError("HTTP 502")
        return await real(url, timeout, headers)
    m.http_get = a50_down
    world["sse"], world["at"] = "3852.00", NOW + 2000
    result = await c.refresh(NOW + 2000, force=True)
    assert c.quote.last == D("3852.00") and c.a50.last == D("14130") and c.a50_error and result.status == "partial" and "A50" in result.error, result
    m.http_get, m.SOURCE_HEALTH = fake_get, m.SourceHealth()

    # --- 恒指: the cash index beside the futures page, each on its own clock ----------------------------------------------------------
    h = m.IndexFutures(True, HK)
    assert h.cash_open(NOW) and h.cadence(NOW) == 2 and h.SPOT_SECONDS == 1
    told = []
    h.on_quote = lambda: told.append((time.monotonic(), h.quote.spot, h.quote.spot_source))
    world["wait"], world["at"] = {"页": 0.5, "恒指现货": 0.02}, NOW
    n = len(world["calls"])
    assert isinstance(await h.refresh(NOW), m.Refreshed) and asked(n) == ["页"], "nothing in hand yet: the page alone, with the index it shows"
    assert h.quote.last == D("24522") and h.quote.spot == D("24510.09") and h.quote.spot_source == "etnet" and h.quote.spot_time == NOW
    # the next tick: the index alone (the page keeps its two seconds)
    world["hsi"], world["at"] = "24600.00", NOW + 1000
    n, told[:] = len(world["calls"]), []
    assert isinstance(await h.refresh(NOW + 1000), m.Refreshed) and asked(n) == ["恒指现货"]
    assert h.quote.spot == D("24600.00") and h.quote.spot_source == "新浪" and [x[1:] for x in told] == [(D("24600.00"), "新浪")]
    assert await h.refresh(NOW + 1000) is False, "neither part is due"
    # two seconds on: both are due — read together, the index not waiting for the page's turn to be over
    h.refreshed -= 2.0
    h.spot_refreshed -= 1.0
    world["hsi"], world["at"] = "24610.00", NOW + 2000
    n, began, told[:] = len(world["calls"]), time.monotonic(), []
    result = await h.refresh(NOW + 2000)
    took = time.monotonic() - began
    assert result == m.Refreshed("ok") and sorted(asked(n)) == ["恒指现货", "页"], (result, asked(n))
    assert took >= 0.49 and told[0][0] - began < 0.35 and told[0][1:] == (D("24610.00"), "新浪"), "the index is told as it arrives, the page still on its way"
    assert h.quote.spot == D("24610.00") and h.quote.spot_source == "新浪" and h.quote.last == D("24522"), "the page's own index, first seen earlier, does not take its place"
    assert await h.refresh(NOW + 2000) is False
    # a forced round is the page's, as it was
    h.spot_refreshed = -1e9
    n = len(world["calls"])
    await h.refresh(NOW + 2500, force=True)
    assert asked(n) == ["页"], asked(n)
    # how the two parts' outcomes are put together: the page's, unless the index alone has something to say
    for page, cash, want in ((m.Refreshed("ok"), m.Refreshed("ok"), m.Refreshed("ok")),
                             (m.Refreshed("ok"), m.Refreshed("partial", "恒指现货：无报价"), m.Refreshed("partial", "恒指现货：无报价")),
                             (m.Refreshed("failed", "etnet: HTTP 502"), m.Refreshed("ok"), m.Refreshed("failed", "etnet: HTTP 502")),
                             (m.Refreshed("partial", "恒指现货：x"), m.Refreshed("partial", "恒指现货：y"), m.Refreshed("partial", "恒指现货：x"))):
        two = m.IndexFutures(True, HK)
        two.quote = h.quote

        async def page_round(now_ms, page=page): return page
        async def cash_round(now_ms, cash=cash): return cash
        two.refresh_page, two.refresh_spot = page_round, cash_round
        assert await two.refresh(NOW) == want, (page, cash)
    # outside the cash session there is no cash index round at all
    night = m.IndexFutures(True, HK)
    night.quote = h.quote
    seen = []

    async def page_only(now_ms): seen.append("页"); return m.Refreshed("ok")
    async def never(now_ms): raise AssertionError("no cash index round while the cash market is shut")
    night.refresh_page, night.refresh_spot = page_only, never
    assert not night.cash_open(bj(28, 20, 0)) and await night.refresh(bj(28, 20, 0)) == m.Refreshed("ok") and seen == ["页"]
    night.refreshed, night.refreshed_ms = time.monotonic(), bj(28, 20, 0)  # (what the page's round stamps)
    assert await night.refresh(bj(28, 20, 0)) is False

    # --- the bot: a price that moved has its card looked at now, once -----------------------------------------------------------------
    for cls in (m.Bot, live.LiveBot):  # (the running bot is the second: its own priced() passes the "now" on)
        b = bot(cls)
        looked = []
        b.predict.books.update({"HSI": None, "KOSPI": None})  # (no book of the 上证 card's in hand)
        b.predict.updated = looked.append
        assert b.hsi.on_quote and b.kospi.on_quote and b.cn.on_quote and b.QUOTE_KEYS == {"恒指期货": ("HSI",), "KOSPI": ("KOSPI",), "上证/A50": ("SSE",)}
        b.kospi.quote = k.quote
        b.kospi.on_quote()
        assert looked == [] and b.quote_marks == {"KOSPI": {"现货": D("3372.50")}}, "the first read after a start moved nothing"
        b.kospi.on_quote()
        b.kospi.quote = dataclasses.replace(k.quote, quoted_ms=NOW + 5000)  # (a later print at the same price)
        b.kospi.on_quote()
        assert looked == []
        b.kospi.quote = dataclasses.replace(k.quote, last=D("3373.10"))
        b.kospi.on_quote()
        assert looked == ["KOSPI"] and b.price_seen == {"KOSPI"}, "the price moved: looked at at once"
        assert b.price_dirty == set() and b.price_look is None, "...not gathered with the pushes (PRICE_LOOK_DELAY)"
        b.quote_read("KOSPI")  # (the end of the round asks again: the same price, nothing more)
        assert looked == ["KOSPI"]
        # 上证: the Composite, and the A50 (the after-hours price); no book in hand, nothing to look at
        b.cn.quote, b.cn.a50 = c.quote, c.a50
        b.cn.on_quote()
        b.cn.a50 = dataclasses.replace(c.a50, last=D("14140"))
        b.cn.on_quote()
        assert looked == ["KOSPI"] and "SSE" not in b.price_seen and b.quote_marks["上证/A50"] == {"现货": D("3852.00"), "A50": D("14140")}
        b.predict.books["SSE"] = None
        b.cn.a50 = None  # (a price gone, then back: no move of its own — the fair signature's check has that)
        b.cn.on_quote()
        b.cn.a50 = dataclasses.replace(c.a50, last=D("14150"))
        b.cn.on_quote()
        assert looked == ["KOSPI"] and b.quote_marks["上证/A50"] == {"现货": D("3852.00"), "A50": D("14150")}
        b.cn.quote = dataclasses.replace(c.quote, last=D("3853.00"))
        b.cn.on_quote()
        assert looked == ["KOSPI", "SSE"]
        # 恒指: the cash index, the future, and each family's future the after-hours card may be taken on
        b.hsi.quote = h.quote
        b.hsi.on_quote()
        for change in ({"spot": D("24611.00")}, {"last": D("24523")}):
            b.hsi.quote = dataclasses.replace(b.hsi.quote, **change)
            b.hsi.on_quote()
        b.hsi.families["CFD"] = dataclasses.replace(h.quote, last=D("24530"))
        b.hsi.on_quote()
        assert looked == ["KOSPI", "SSE", "HSI", "HSI"], "a family's future first in hand: no move"
        b.hsi.families["CFD"] = dataclasses.replace(h.quote, last=D("24531"))
        b.hsi.on_quote()
        assert looked == ["KOSPI", "SSE", "HSI", "HSI", "HSI"], looked
        # the fair signature's next check takes those keys as looked at — and wakes the trader all the same
        b.priced = lambda keys, now=False: looked.append(("签名", set(keys)))
        b.fair_signature_keys = lambda now_ms: ((("hsi-market", world["fair"]), ("other-market", world["fair"])), {"hsi-market": "HSI", "other-market": "K9"})
        world["fair"] = 0.50
        b.fair_sig_at = -1e9; b.note_job("恒指期货", NOW)
        assert b.price_seen == set() and looked[-1] == "HSI", "(the first signature: nothing to compare; the keys seen are spent)"
        b.price_seen = {"HSI"}
        world["fair"], b.sim_wake = 0.52, False
        b.fair_sig_at = -1e9; b.note_job("恒指期货", NOW)
        assert looked[-1] == ("签名", {"K9"}) and b.sim_wake and b.price_seen == set(), "the index's key was looked at when its quote came; the rest goes to the lane"
        world["fair"] = 0.54
        b.fair_sig_at = -1e9; b.note_job("恒指期货", NOW)
        assert looked[-1] == ("签名", {"HSI", "K9"}), "nothing seen since: the signature's own look, as ever"
        # a look that cannot be asked for is the feed's no error
        del b.priced
        b.quote_mark = lambda name: 1 / 0
        b.quote_read("KOSPI")
        b.kospi.on_quote()
    # --- the feeds' own rounds reach the bot's look -------------------------------------------------------------------------------------
    b = bot()
    looked = []
    b.predict.books.update({"HSI": None, "KOSPI": None, "SSE": None})
    b.predict.updated = looked.append
    world.update(wait={}, kospi="3,371.89", sse="3850.12", a50=14120, hsi="24600.00", at=NOW)
    await b.kospi.refresh(NOW); await b.cn.refresh(NOW); await b.refresh_hsi(NOW)
    assert looked == [], looked
    world.update(kospi="3,372.50", sse="3851.00", hsi="24610.00", at=NOW + 1000)
    await b.kospi.refresh(NOW + 1000, force=True)
    assert looked == ["KOSPI"]
    await b.cn.refresh(NOW + 1000, force=True)
    assert looked == ["KOSPI", "SSE"]
    b.hsi.spot_refreshed -= 1.0
    await b.refresh_hsi(NOW + 1000)
    assert looked == ["KOSPI", "SSE", "HSI"] and b.hsi.quote.spot == D("24610.00")

    # --- the jobs come round on the second, a hair past it; a job that was not due waits the plain tick --------------------------------
    assert m.Bot.ON_THE_SECOND == frozenset({"股票实时", "恒指期货", "KOSPI", "上证/A50"}) and 0 < m.Bot.ON_THE_SECOND_EDGE <= 0.05
    plain_tick, m.REFERENCE_TICK = m.REFERENCE_TICK, 0.3
    try:
        loop_bot = bot()
        loop_bot.ON_THE_SECOND_MIN = 0.05
        starts = {"KOSPI": [], "上证/A50": [], "恒指期货": [], "汇率": [], "没到点": []}
        loop_bot.ON_THE_SECOND = m.Bot.ON_THE_SECOND | {"没到点"}
        ends = []
        loop_bot.quote_read = ends.append

        def job(name):
            async def run_once():
                starts[name].append(time.monotonic())
                if name == "没到点":
                    return False
                await asyncio.sleep(0.12)
                return m.Refreshed("ok")
            return run_once
        tasks = [asyncio.create_task(loop_bot.reference_loop(name, job(name))) for name in starts]
        await asyncio.sleep(1.6)
        loop_bot.stopping.set()
        await asyncio.gather(*tasks)
        gaps = {name: sorted(b2 - a for a, b2 in zip(at, at[1:])) for name, at in starts.items()}
        mid = {name: g[len(g) // 2] for name, g in gaps.items()}
        # (medians, and against the plain job's: a busy builder stretches every sleep alike)
        for name in ("KOSPI", "上证/A50", "恒指期货"):
            assert len(gaps[name]) >= 3 and mid[name] >= 0.3 + loop_bot.ON_THE_SECOND_EDGE - 0.005 and mid["汇率"] - mid[name] >= 0.05, (name, mid)
        assert mid["汇率"] >= 0.41 and mid["没到点"] >= 0.295, mid
        assert set(ends) == {"KOSPI", "上证/A50", "恒指期货"}, "the end of an index feed's round asks too (whatever it stored without saying)"
    finally:
        m.REFERENCE_TICK = plain_tick
    print("INDEX_LIVE_OK")


asyncio.run(run())
