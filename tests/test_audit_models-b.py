"""Audit regression tests for the models-b cluster."""
import sys, math, random, re
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m


# --- MOD-FIT-WALKFORWARD-STRIKE: /calib's proxy-fit walk-forward must price P(close > shown ref), the reference the
# hit is judged on (the Predict target price when the card used one), not P(close > ref_raw) ------------------------
def fit_walkforward(strike_factor: float) -> tuple[float, float]:
    """(Brier on the 拟合模型 line of calibration_report, Brier of the same walk-forward priced on the shown reference)."""
    rng = random.Random(3)
    preds, outcomes = [], {}
    for i in range(40):
        day = f"2026-{6 + i // 28:02d}-{i % 28 + 1:02d}"
        ref_raw = 3000.0
        strike = ref_raw * strike_factor
        move = rng.gauss(0, 0.006)
        close = ref_raw * math.exp(move + rng.gauss(0, 0.005))
        outcomes[f"SSE:{day}"] = close
        for j in range(3):
            eff = ref_raw * math.exp(move) * strike / ref_raw
            up = 1 - m.norm_cdf(math.log(strike / eff) / 0.005)
            row = {"key": "SSE", "t": i * 10 + j, "target": day, "mode": "盘后", "ref": strike, "ref_raw": ref_raw,
                   "eff": eff, "up": up, "move": move, "beta": 1.0, "sigma": 0.005, "sigma_raw": 0.005, "R": 1.0}
            if strike != ref_raw:
                row["strike"] = strike
            preds.append(row)
    text = "\n".join(m.calibration_report(preds, outcomes))
    line = next(l for l in text.splitlines() if l.startswith("  逐日向前检验（") and "拟合模型" in l)
    shown = float(re.search(r"拟合模型 Brier ([0-9.]+)", line).group(1))
    done = [dict(p, close=outcomes[f"SSE:{p['target']}"]) for p in preds]
    for r in done:
        r["hit"] = 1.0 if r["close"] > r["ref"] else 0.5 if r["close"] == r["ref"] else 0.0
        r["y"] = math.log(r["close"] / r["ref_raw"])
    days = sorted({r["target"] for r in done})
    ps, hs = [], []
    for day in days[m.CALIB_MIN_DAYS:]:
        a, b, k = m.fit_proxy([r for r in done if r["target"] < day])
        for r in (r for r in done if r["target"] == day):
            ps.append(m.norm_cdf((a + b * r["move"] - math.log(r["ref"] / r["ref_raw"])) / (k * math.sqrt(r["R"]))))
            hs.append(r["hit"])
    return shown, m.brier_scores(ps, hs)[0]


shown, expected = fit_walkforward(1.004)  # Predict's target 0.4% above the local close
assert abs(shown - round(expected, 3)) < 1e-9, (shown, expected)
assert abs(shown - 0.144) < 1e-9 and abs(shown - 0.220) > 0.05, shown
shown0, expected0 = fit_walkforward(1.0)  # no strike: ref == ref_raw, the line is what it always was
assert abs(shown0 - round(expected0, 3)) < 1e-9, (shown0, expected0)

# --- MOD-YAHOO-SORT: parse_yahoo_daily keeps one bar per date (the later stamp), so Yahoo's live duplicate of today
# with a null open no longer raises TypeError, and two numeric opens no longer order the copies by open --------------
import datetime as _dt, json as _json
from decimal import Decimal as _D


def yahoo_chart(opens: list) -> bytes:
    kst = _dt.timezone(_dt.timedelta(hours=9))
    stamps = [int(_dt.datetime(2026, 11, d, h, mi, tzinfo=kst).timestamp()) for d, h, mi in ((9, 9, 0), (10, 9, 0), (10, 15, 30))]
    return _json.dumps({"chart": {"result": [{"meta": {"gmtoffset": 32400}, "timestamp": stamps,
                        "indicators": {"quote": [{"open": opens, "close": [4005, 4020, 4021.5]}]}}]}}).encode()


bars = m.parse_yahoo_daily(yahoo_chart([4000, 4010, None]))
assert [b[0] for b in bars] == [_dt.date(2026, 11, 9), _dt.date(2026, 11, 10)], bars
assert bars[1][2] == _D("4021.50") and bars[1][1] == _D("4010"), bars
bars = m.parse_yahoo_daily(yahoo_chart([4000, 4012, 4011]))
assert len(bars) == 2 and bars[1][2] == _D("4021.50") and bars[1][1] == _D("4011"), bars
bars = m.parse_yahoo_daily(yahoo_chart([4000, None, 4010]))
assert len(bars) == 2 and bars[1] == (_dt.date(2026, 11, 10), _D("4010"), _D("4021.50")), bars

# --- MOD-KOSPI-AFTER-CLOSE: 15:30-15:33 KST the KRX session still reads live (cards roll over at KRX_SETTLED), so
# kospi_odds prices today's already-decided close 盘中 with the 1-minute floor; record_predictions must not save that as
# a prediction (it would be scored by /calib and fed into the (KOSPI, 盘中) σ factor). Before the close it still saves --
_kst = _dt.timezone(_dt.timedelta(hours=9))
_at = lambda h, mi, s=0: int(_dt.datetime(2026, 11, 10, h, mi, s, tzinfo=_kst).timestamp() * 1000)  # a Tuesday
_cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "SSE_INDEX": "off"})
_bot = m.Bot(_cfg, m.Store(":memory:"), m.Binance(_cfg), None)
_bot.sse_odds = _bot.hsi_odds = lambda now: None
_now = _at(15, 31)
_bot.kospi.quote = m.IndexQuote("KOSPI", _D("4105.20"), _D("4100.00"), None, None, None, _at(15, 30, 40), "Naver", fetched_ms=_now)
_odds = _bot.kospi_odds(_now)
assert isinstance(_odds, m.CloseOdds) and _odds.mode == "盘中" and _odds.target == _dt.date(2026, 11, 10), _odds  # the premise
_bot.record_predictions(_now)
assert [v for _, v in _bot.store.items("pred:KOSPI")] == [], list(_bot.store.items("pred:KOSPI"))
assert "KOSPI" not in _bot.pred_last  # the skipped tick does not use up the half-hour slot
_bot2 = m.Bot(_cfg, m.Store(":memory:"), m.Binance(_cfg), None)
_bot2.sse_odds = _bot2.hsi_odds = lambda now: None
_now = _at(15, 20)
_bot2.kospi.quote = m.IndexQuote("KOSPI", _D("4105.20"), _D("4100.00"), None, None, None, _now - 5_000, "Naver", fetched_ms=_now)
_bot2.record_predictions(_now)
_rows = [v for _, v in _bot2.store.items("pred:KOSPI")]
assert len(_rows) == 1 and _rows[0]["mode"] == "盘中" and _rows[0]["target"] == "2026-11-10", _rows


# --- MOD-SHAPE-RETRY: while there is no shape of the day (too few 15-minute candles, or a failed read), read_vol must not
# re-read the 15-minute candles on every σ read: read_shape's SHAPE_RETRY_SECONDS stamp decides when it is tried again ---------
import asyncio as _asyncio


class _ShapeFeed:
    """Candles with no shape: [] for 15m (vol_shape -> None, as for a new listing), flat-ish 1-minute rows otherwise."""
    def __init__(self):
        self.calls, self.now = [], 0

    async def __call__(self, path, **p):
        self.calls.append(p["interval"])
        if p["interval"] == "15m":
            return []
        end = (p.get("endTime") or self.now) // 60_000 * 60_000
        return [[o, "0", "0", "0", str(20 * (1 + 0.001 * (o // 60_000 % 2))), "0", o + 59_999]
                for o in range(end - (p["limit"] - 1) * 60_000, end + 1, 60_000)]


async def _shape_retry():
    _U = next(s for s in m.WINDOW_UPDOWN_MARKETS if s.key == "UNITREE-15M")
    now = int(_dt.datetime(2026, 11, 10, 10, 5, tzinfo=m.BEIJING).timestamp() * 1000)
    fut = _ShapeFeed()
    card = m.IndexUpDownMarket(m.Store(":memory:"), _U.current(now), fut, "sh", m.session_minute_fn("sh"))
    for i in range(4):  # four σ reads a minute apart (VOL_SECONDS near a window)
        fut.now = now + i * 60_000
        await card.read_vol(fut.now)
    assert fut.calls.count("15m") == 1, fut.calls
    assert card.shape is None and card.shape_error and card.sigma is not None  # the plain σ meanwhile
    nxt = card.carry(m.IndexUpDownMarket(card.store, _U.current(now + 240_000), fut, "sh", m.session_minute_fn("sh")))
    fut.now = now + 240_000
    await nxt.read_vol(fut.now)  # the next window's card keeps the retry stamp
    assert fut.calls.count("15m") == 1, fut.calls
    fut.now = now + m.IndexUpDownMarket.SHAPE_RETRY_SECONDS * 1000
    await nxt.read_vol(fut.now)  # tried again after SHAPE_RETRY_SECONDS
    assert fut.calls.count("15m") == 2, fut.calls

    _B = next(s for s in m.WINDOW_UPDOWN_MARKETS if s.key == "BTC-15M")
    start = int(_dt.datetime(2026, 11, 10, 14, 0, tzinfo=_dt.timezone.utc).timestamp() * 1000)
    spot = m.SpotWindowMarket(m.Store(":memory:"), _B.window_spec(start))
    feed = _ShapeFeed()
    spot.get = feed
    for i in range(4):
        feed.now = start + i * 60_000
        await spot.read_vol(feed.now)
    assert feed.calls.count("15m") == 1, feed.calls
    feed.now = start + m.SpotWindowMarket.SHAPE_RETRY_SECONDS * 1000
    await spot.read_vol(feed.now)
    assert feed.calls.count("15m") == 2, feed.calls

_asyncio.run(_shape_retry())

# MOD-BOOK-DISPUTES: book_disputes judged max(best bid, best ask) >= 90c as "the market agrees", so a thin book bid
# 20c / offered 95c let a ZEC level listed after the category's window low show 已触及 and settle a paper Yes as a win.
import time as _time
_bk = lambda bids, asks: m.PredictBook("K", "s", "M", "t", tuple((_D(p), _D("300")) for p in bids),
                                       tuple((_D(p), _D("300")) for p in asks), 0, 0)
assert m.book_disputes(_bk(["0.20"], ["0.95"])) is True  # the far ask no longer "agrees"
assert m.book_disputes(_bk(["0.95"], ["0.97"])) is False
assert m.book_disputes(_bk([], ["0.95"])) is False and m.book_disputes(_bk([], ["0.50"])) is True  # no bid: the ask decides
assert m.book_disputes(_bk([], [])) is False and m.book_disputes(None) is False  # no quotes: nothing disputed

def _zec_scenario():
    utc = lambda *a: int(_dt.datetime(*a, tzinfo=_dt.timezone.utc).timestamp() * 1000)
    NOW, CAT0, LATER, LOW_AT, H = utc(2026, 11, 10, 12), utc(2026, 10, 1), utc(2026, 11, 5), utc(2026, 10, 3), 3_600_000

    class FM:
        def now_ms(self): return NOW
        async def prices(self): return {}
        async def get(self, path, **params): return {}

    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "SIM_MARKETS": "all", "PROBABILITY": "off"})
    bot = m.Bot(cfg, m.Store(":memory:"), FM(), None)
    rm = bot.ranges["ZEC-HIT-2027"]
    spec = rm.spec
    rules = ('Resolves "Yes" if any Binance 1 minute candle for ZEC/USDT from the creation of this market has a final '
             '"Low" price equal to or lower than the price in the title.')
    meta = lambda created, title: ({"outcomes": ["Yes", "No"], "created_ms": created, "rules": rules,
                                    "question": f"Will Zcash dip to {title}?", "status": "OPEN", "trading_status": "OPEN"},
                                   _time.monotonic())
    bot.predict.market_meta["A"] = meta(CAT0, "$300")
    bot.predict.market_meta["B"] = meta(LATER, "$400")
    book_a = m.PredictBook("ZEC-HIT-2027", spec.slug, "A", "↓ 300", ((_D("0.10"), _D("300")),), ((_D("0.14"), _D("300")),), NOW, 0)
    book_b = m.PredictBook("ZEC-HIT-2027", spec.slug, "B", "↓ 400", ((_D("0.20"), _D("300")),), ((_D("0.95"), _D("300")),), NOW, 0)
    rows = [m.LadderRow(_D("300"), "A", "↓ 300", book_a, "", "Will Zcash dip to $300?"),
            m.LadderRow(_D("400"), "B", "↓ 400", book_b, "", "Will Zcash dip to $400?")]
    bot.predict.ladders["ZEC-HIT-2027"] = rows
    rm.learn(rows, bot.predict.market_meta)
    bot.store.put(f"range:{spec.slug}", {"start": rm.start_ms, "through": NOW - NOW % H, "open": 450.0, "high": 610.0,
                                         "high_at": CAT0, "low": 380.0, "low_at": LOW_AT})
    rm.price, rm.priced_ms, rm.sigma, rm.sigma_ms, rm.scanned_ms = _D("520"), NOW, 1.1, NOW, NOW
    trade = {"kind": "range", "key": "ZEC-HIT-2027", "market": f"{spec.slug}#B",
             "settle": {"target": "400", "dir": "down", "end": rm.end_ms}}
    assert bot.sim_result(trade, NOW) is None  # Predict's own result settles it, not a pre-listing low
    rb = next(r for r in bot.range_payload(rm, NOW)["ladder"]["rows"] if r["label"] == "↓ $400")
    assert not rb["touched"] and "请核实" in (rb["error"] or ""), rb

_zec_scenario()


# --- MOD-FLIP-LEG: one coin's pushes must not keep the pair "fresh" nor defer the allMids read while the other leg is
# silent (its subscription refused/stalled): the pair is as fresh as its older leg --------------------------------------
def _flip_leg_scenario(mids_fail: bool) -> tuple[m.FlipMarket, list[str], int]:
    import asyncio as _asyncio, dataclasses as _dc, time as _time

    class _Clock:  # main's view of the time module, with a monotonic clock the test moves (one simulated minute a step)
        mono = 1_000.0

        def monotonic(self):
            return self.mono

        def __getattr__(self, name):
            return getattr(_time, name)
    clock, real_time = _Clock(), m.time
    spec = _dc.replace(m.FLIP_MARKETS[0], start_ms=m.et_ms(2026, 11, 1, 0, 0, -4))
    fm = m.FlipMarket(m.Store(":memory:"), spec)
    fm.sigma, fm.sigma_ms = 0.9, 0
    asked: list[str] = []

    async def info(payload):
        asked.append(payload["type"])
        if payload["type"] == "allMids":
            if mids_fail:
                raise ValueError("allMids down")
            return {spec.coin: "41.0", spec.other: "120.0"}  # the silent leg really trades at 120 now
        return []
    fm.info = info
    m.time = clock
    try:
        t0 = 1_794_000_000_000
        fm.push_mid(spec.coin, m.D("40.0"), t0)
        fm.push_mid(spec.other, m.D("160.0"), t0)  # the other leg's last push: 160, then its channel goes quiet
        assert fm.priced_ms == t0
        now = t0
        for k in range(1, 7):  # six minutes of pushes from the first coin only
            now, clock.mono = t0 + k * 60_000, 1_000.0 + k * 60
            fm.push_mid(spec.coin, m.D("40.0"), now)
            fm.times["vol"] = fm.times["scan"] = clock.mono  # isolate the price read
            _asyncio.run(fm.refresh(now))
    finally:
        m.time = real_time
    return fm, asked, now

_fm, _asked, _now = _flip_leg_scenario(mids_fail=False)
assert _asked.count("allMids") >= 1, _asked  # the silent leg ages the read timer: allMids re-read
assert _fm.prices[_fm.spec.other] == m.D("120.0") and _now - _fm.priced_ms < 60_000, (_fm.prices, _now - _fm.priced_ms)
_fm, _asked, _now = _flip_leg_scenario(mids_fail=True)
assert _asked.count("allMids") >= 1, _asked
_odds = _fm.odds(_now)
assert isinstance(_odds, str) and "价格停在" in _odds, _odds  # REST down too: the frozen leg holds the odds after 5 min


# --- MOD-FLIP-LEG fix-up: with one leg silent and allMids FAILING (a Hyperliquid 429), the next push of the live coin must
# not wind the read timer back to the silent leg's old push time: allMids is retried once per PRICE_SECONDS, not on
# every 1-second refresh tick (REFERENCE_TICK) --------------------------------------------------------------------------
def _flip_leg_cadence(mids_fail: bool) -> tuple[m.FlipMarket, int, int]:
    import asyncio as _asyncio, dataclasses as _dc, time as _time

    class _Clock:
        mono = 1_000.0

        def monotonic(self):
            return self.mono

        def __getattr__(self, name):
            return getattr(_time, name)
    clock, real_time = _Clock(), m.time
    spec = _dc.replace(m.FLIP_MARKETS[0], start_ms=m.et_ms(2026, 11, 1, 0, 0, -4))
    fm = m.FlipMarket(m.Store(":memory:"), spec)
    fm.sigma, fm.sigma_ms = 0.9, 0
    asked: list[str] = []

    async def info(payload):
        asked.append(payload["type"])
        if payload["type"] == "allMids":
            if mids_fail:
                raise ValueError("429 Too Many Requests")
            return {spec.coin: "41.0", spec.other: "120.0"}
        return []
    fm.info = info
    m.time = clock
    seconds = 330  # past PRICE_STALE_MS (5 min) so the stale hold can be checked too
    try:
        t0 = 1_794_000_000_000
        fm.push_mid(spec.coin, m.D("40.0"), t0)
        fm.push_mid(spec.other, m.D("160.0"), t0)
        now = t0
        for k in range(1, seconds + 1):  # the first coin pushes every second, refresh runs every second
            now, clock.mono = t0 + k * 1_000, 1_000.0 + k
            fm.push_mid(spec.coin, m.D("40.0"), now)
            fm.times["vol"] = fm.times["scan"] = clock.mono
            _asyncio.run(fm.refresh(now))
    finally:
        m.time = real_time
    return fm, asked.count("allMids"), now

_most = 330 // m.FlipMarket.PRICE_SECONDS + 1
for _fail in (True, False):
    _fm, _reads, _now = _flip_leg_cadence(mids_fail=_fail)
    assert 1 <= _reads <= _most, (_fail, _reads, _most)  # ~11 reads in 330 s, not ~300
    if _fail:
        _odds = _fm.odds(_now)
        assert isinstance(_odds, str) and "价格停在" in _odds, _odds  # the stale hold still trips on the silent leg
    else:
        assert _fm.prices[_fm.spec.other] == m.D("120.0"), _fm.prices


# --- MOD-HYPE-MARKPRICE: with HYPEUSDT in SYMBOLS, a markPriceUpdate frame re-sends the merged row (the LAST aggTrade's
# price and time); it must not stamp the futures ladder's price "now" nor defer its 30-second REST read ---------------
def _hype_markprice() -> None:
    import json as _json, time as _time
    now = [1_794_000_000_000]

    class _FM:
        def now_ms(self): return now[0]
        async def prices(self): return {}
        async def get(self, path, **params): return {}

    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT,HYPEUSDT", "PROBABILITY": "off"})
    bot = m.Bot(cfg, m.Store(":memory:"), _FM(), None)
    bot.priced = lambda keys: None
    rm = bot.ranges["HYPE-HIT-11"]
    st = bot.binance_streams["futures"]
    st.connected_at = _time.monotonic()

    async def go():
        await st.frame(None, _json.dumps({"e": "aggTrade", "s": "HYPEUSDT", "p": "45.000", "T": now[0]}))
        assert rm.price == m.D("45.000") and rm.priced_ms == now[0], (rm.price, rm.priced_ms)
        traded = now[0]
        rm.times["price"] = -1e9  # as if PRICE_SECONDS had passed since the trade: the REST read is due
        for _ in range(360):  # six minutes of mark prices while aggTrade is stalled (PRICE_STALE_MS is five)
            now[0] += 1000
            await st.frame(None, _json.dumps({"e": "markPriceUpdate", "s": "HYPEUSDT", "p": "47.000", "i": "47.010", "E": now[0]}))
        assert rm.priced_ms == traded, now[0] - rm.priced_ms  # the frozen trade ages
        assert rm.times["price"] == -1e9, rm.times["price"]  # the REST read is not deferred by the mark frames
        assert _time.monotonic() - rm.times["price"] >= rm.PRICE_SECONDS
        assert rm.price_stale(now[0])
        now[0] += 500
        await st.frame(None, _json.dumps({"e": "aggTrade", "s": "HYPEUSDT", "p": "47.100", "T": now[0]}))
        assert rm.price == m.D("47.100") and rm.priced_ms == now[0] and rm.times["price"] > 0  # a new trade still pushes
        await st.frame(None, _json.dumps({"e": "aggTrade", "s": "HYPEUSDT", "p": "47.200", "T": now[0]}))
        assert rm.price == m.D("47.200")  # another trade in the same millisecond at another price still pushes

    _asyncio.run(go())

_hype_markprice()


# --- MOD-FLIP-VOL-RETRY: while σ is unknown a failing σ read (Hyperliquid 429/outage, too little history) must be retried
# after a back-off (1 minute, as RangeMarket), not on every 1-second refresh of the reference loop -------------------------
def _flip_vol_retry() -> None:
    import asyncio as _asyncio, dataclasses as _dc, time as _time

    class _Clock:  # main's view of the time module, with a monotonic clock the test moves one second per refresh
        mono = 1_000.0

        def monotonic(self):
            return self.mono

        def __getattr__(self, name):
            return getattr(_time, name)
    clock, real_time = _Clock(), m.time
    spec = _dc.replace(m.FLIP_MARKETS[0], start_ms=m.et_ms(2026, 11, 1, 0, 0, -4))
    fm = m.FlipMarket(m.Store(":memory:"), spec)
    state = {"fail": True}
    asked: list[str] = []
    t0 = 1_794_000_000_000

    async def info(payload):
        asked.append(payload["type"])
        if payload["type"] == "allMids":
            return {spec.coin: "41.0", spec.other: "120.0"}
        if state["fail"]:
            raise m.RemoteError("HTTP 429: Too Many Requests")
        req = payload["req"]
        base = 40.0 if req["coin"] == spec.coin else 120.0  # far apart: the path scan never opens minute candles
        first = req["startTime"] - req["startTime"] % m.HOUR_MS
        wiggle = 0.002 if req["coin"] == spec.coin else 0.0  # only A moves: the ratio has a σ
        return [{"t": t, "o": base, "h": base * 1.003, "l": base * 0.997, "c": base * (1 + wiggle * ((t // m.HOUR_MS) % 3 - 1))}
                for t in range(first, req["endTime"], m.HOUR_MS)]
    fm.info = info
    m.time = clock
    try:
        for k in range(60):  # one minute of 1-second refreshes with every candleSnapshot rate-limited
            _asyncio.run(fm.refresh(t0 + k * 1000))
            clock.mono += 1.0
        assert fm.sigma is None and "波动率" in fm.error and "429" in fm.error, (fm.sigma, fm.error)  # kept between retries
        assert asked.count("candleSnapshot") <= 3, asked.count("candleSnapshot")  # was ~61: one σ read per refresh
        state["fail"] = False  # Hyperliquid answers again: the back-off expires and σ is read
        clock.mono += 61.0
        _asyncio.run(fm.refresh(t0 + 121_000))
        assert fm.sigma is not None and fm.sigma > 0 and fm.sigma_ms == t0 + 121_000 and not fm.error, (fm.sigma, fm.error)
        n = asked.count("candleSnapshot")
        clock.mono += 1.0
        _asyncio.run(fm.refresh(t0 + 122_000))  # σ known: no new σ read for VOL_SECONDS
        assert asked.count("candleSnapshot") == n, asked.count("candleSnapshot") - n
    finally:
        m.time = real_time

_flip_vol_retry()

# audit MOD-RANGE-WICK: a trade pushed through a ladder level is a touch; the next push back under it must not un-reach
# the level before the next scan (up to SCAN_SECONDS) reads the running hour's candle.
def _range_wick():
    D, H = m.D, 3_600_000
    import dataclasses as _dc
    spec = next(s for s in m.RANGE_MARKETS if s.symbol == "ETHUSDT" and s.start_ms)  # ETH November ladder
    rm = m.RangeMarket(m.Store(":memory:"), spec)
    now = spec.start_ms + 9 * 24 * H + 30 * 60_000
    hour = now - now % H
    rm.store.put(f"range:{spec.slug}", {"start": spec.start_ms, "through": hour, "open": 3900.0, "high": 4480.0,
                                        "high_at": spec.start_ms, "low": 3700.0, "low_at": spec.start_ms})
    rm.running, rm.scanned_ms, rm.sigma, rm.sigma_ms = {"open": hour, "high": 4470.0, "low": 4400.0}, now - 5_000, 0.7, now
    rm.push_price(D("4500.10"), now)        # a trade through ↑ 4,500
    assert rm.reached(D("4500"), "up") and rm.probability(D("4500"), "up", now) == 1.0
    rm.push_price(D("4492.00"), now + 400)  # the next trade, back under (no scan in between)
    assert rm.reached(D("4500"), "up"), rm.marks()
    assert rm.probability(D("4500"), "up", now + 400) == 1.0
    assert rm.marks()["high"] == 4500.10 and rm.marks()["high_at"] == hour, rm.marks()
    rm.push_price(D("3650.00"), now + 800)  # a low pushed in the same hour counts for ↓ levels too
    rm.push_price(D("3720.00"), now + 900)
    assert rm.reached(D("3660"), "down") and rm.marks()["low"] == 3650.0, rm.marks()
    # once the persisted hours cover that hour, its pushed extremes are dropped (the kline High/Low carries them)
    rm.store.put(f"range:{spec.slug}", {**rm.history, "through": hour + H, "high": 4500.10, "low": 3650.0})
    rm.push_price(D("4490.00"), hour + H + 1000)
    assert list(rm.pushed) == [hour + H], rm.pushed
    # a window not known yet (start 0) records nothing
    rm2 = m.RangeMarket(m.Store(":memory:"), _dc.replace(spec, start_ms=0))
    assert not rm2.start_ms
    rm2.push_price(D("4500"), now)
    assert rm2.pushed == {}, rm2.pushed

_range_wick()

# 1.154.26 (the audit of 11-18, 20): a trade pushed in was stamped with the moment it arrived. A stalled socket catching up
# delivers its backlog in a burst — each old trade made the first-touch, the daily up/down and the range cards look priced this
# very second (past price_problem, the card's own 30-second read put off again) and the look that followed priced, and bought,
# on a price minutes old. A pushed trade is now as old as it says (never newer than now), one older than the price in hand is
# not taken, and only a trade of this moment defers the read; a read's answer never replaces a trade pushed in while it was out.
def _late_trades():
    import asyncio as _asyncio
    import dataclasses as _dc
    D, H = m.D, 3_600_000
    rspec = next(s for s in m.RANGE_MARKETS if s.symbol == "ETHUSDT" and s.start_ms)
    now = rspec.start_ms + 9 * 24 * H + 30 * 60_000
    tspec = next(s for s in m.TOUCH_MARKETS if getattr(s, "symbol", ""))
    uspec = m.DAILY_UPDOWN_MARKETS[0].current(now)  # BTC's daily up/down, the day ``now`` falls in
    cards = [m.RangeMarket(m.Store(":memory:"), rspec), m.TouchMarket(m.Store(":memory:"), tspec), m.UpDownMarket(m.Store(":memory:"), uspec)]
    mid = lambda card: (D(str(card.spec.low)) + D(str(card.spec.high))) / 2 if isinstance(card, m.TouchMarket) else D("4400")
    for card in cards:
        p0, never = mid(card), card.times["price"]
        card.push_price(p0, now, now - 40)  # a trade of this moment: its own time, the read deferred
        assert (card.price, card.priced_ms) == (p0, now - 40) and card.times["price"] > never, type(card).__name__
        deferred = card.times["price"]
        card.push_price(p0 + 1, now + 60_000, now + 5_000)  # a minute on, the socket catching up: a trade 55 s old
        assert (card.price, card.priced_ms, card.times["price"]) == (p0 + 1, now + 5_000, deferred), "taken as old as it says, the read left due"
        card.push_price(p0 + 2, now + 60_100, now - 10_000)  # older than the price in hand
        assert (card.price, card.priced_ms) == (p0 + 1, now + 5_000), "not taken"
        card.push_price(p0 + 3, now + 61_000, now + 99_000)  # a stamp ahead of the local clock: never newer than now
        assert (card.price, card.priced_ms) == (p0 + 3, now + 61_000) and card.times["price"] > deferred
        card.push_price(p0 + 4, now + 62_000)  # no time of its own: now, as before
        assert (card.price, card.priced_ms) == (p0 + 4, now + 62_000)
        assert m.pushed_stamp(5, None) == 5 and m.pushed_stamp(5, 3) == 3 and m.pushed_stamp(5, 9) == 5
    # the daily up/down card says so itself: priced by a late trade it is not fresh, by one of this moment it is
    ud = m.UpDownMarket(m.Store(":memory:"), uspec)
    at = uspec.end_ms - 10 * 60_000  # ten minutes to the end: the price may be TRADE_STALE_MIN_MS old at the least
    ud.push_price(D("100"), at, at - 5 * 60_000)
    assert ud.price_problem(at) != "", "a trade five minutes old delivered now is a price five minutes old"
    ud.push_price(D("100"), at + 10, at - 30)
    assert ud.price_problem(at + 10) == ""
    # the range card: a trade's extreme is its own hour's, and one made before the window is not the window's touch
    rm = m.RangeMarket(m.Store(":memory:"), rspec)
    hour = now - now % H
    rm.store.put(f"range:{rspec.slug}", {"start": rspec.start_ms, "through": hour - H, "open": 3900.0, "high": 4480.0,
                                         "high_at": rspec.start_ms, "low": 3700.0, "low_at": rspec.start_ms})
    rm.push_price(D("4500.10"), now, hour - 60_000)  # made in the hour before, delivered in this one
    assert list(rm.pushed) == [hour - H] and rm.pushed[hour - H][0] == 4500.10, rm.pushed
    early = m.RangeMarket(m.Store(":memory:"), rspec)
    early.push_price(D("9999"), rspec.start_ms + 2_000, rspec.start_ms - 3_000)  # before the window, delivered after its start
    assert early.pushed == {} and (early.price, early.priced_ms) == (D("9999"), rspec.start_ms - 3_000), early.pushed
    # a read's answer does not replace a trade pushed in while the read was out
    for card in (m.RangeMarket(m.Store(":memory:"), rspec), m.TouchMarket(m.Store(":memory:"), tspec)):
        p0 = mid(card)
        card.sigma, card.sigma_ms = 0.5, now
        card.times.update({k: _time.monotonic() for k in card.times if k != "price"})  # (only the price read is due)

        async def get(path, card=card, p0=p0, **params):
            if path == "ticker/price":
                card.push_price(p0 + 7, now + 300, now + 250)  # a trade arrives while the read is out
                return {"price": str(p0)}
            raise m.RemoteError("offline")
        card.get = get
        _asyncio.run(card.refresh(now))
        assert (card.price, card.priced_ms) == (p0 + 7, now + 250), (type(card).__name__, card.price, card.priced_ms)
    ud = m.UpDownMarket(m.Store(":memory:"), uspec)

    async def get_ud(path, **params):
        ud.push_price(D("101"), now + 300, now + 250)
        return {"price": "100"}
    ud.get = get_ud
    _asyncio.run(ud.read_price(now))
    assert (ud.price, ud.priced_ms) == (D("101"), now + 250)
    ud2 = m.UpDownMarket(m.Store(":memory:"), uspec)

    async def get_plain(path, **params):
        return {"price": "100"}
    ud2.get = get_plain
    _asyncio.run(ud2.read_price(now))
    assert (ud2.price, ud2.priced_ms) == (D("100"), now), "with no push meanwhile: the read's, as before"
    # ...and the bot hands a pushed trade's own time to the card (Bot.price_pushed)
    import types as _types
    got, looked = [], []
    card = _types.SimpleNamespace(spec=_types.SimpleNamespace(key="ETH-RANGE"), push_price=lambda price, now_ms, at_ms=None: got.append((price, now_ms, at_ms)))
    bot = _types.SimpleNamespace(market=_types.SimpleNamespace(now_ms=lambda: now), config=_types.SimpleNamespace(symbols=()), snapshots={}, index_markets={},
                                 stock_touches=lambda symbol: (), pair_markets={("spot", "ETHUSDT"): (card,)}, pair_trades={}, priced=looked.append)
    m.Bot.price_pushed(bot, "spot", "ETHUSDT", {"price": "4400", "time": now - 70_000})
    m.Bot.price_pushed(bot, "spot", "ETHUSDT", {"price": "4400", "time": now - 70_000})  # the same trade again (a mark frame): nothing
    m.Bot.price_pushed(bot, "spot", "ETHUSDT", {"price": "4401"})                        # no time of its own
    assert got == [(D("4400"), now, now - 70_000), (D("4401"), now, None)] and looked == [["ETH-RANGE"], ["ETH-RANGE"]], (got, looked)

_late_trades()

# 1.154.26 (the audit of 11-18, 26): the first-touch and the up/down cards read σ again at every pass while they had none ("or
# self.sigma is None", a pass a second): hundreds of candles a card a second for as long as the read failed — a few such cards
# were the futures weight limit, whose cooldown stops every futures read. Backed off now, as the flip and the range cards
# are since 1.150.0: 5 s, 10, 20, 40, a minute with no σ yet; five minutes with one in hand; never past the regular cadence —
# and on the first-touch card a σ read failing no longer keeps the path's scan from running.
def _vol_backoff():
    import asyncio as _asyncio
    D, H = m.D, 3_600_000
    assert [m.vol_again(n, False, 3600) for n in (1, 2, 3, 4, 5, 6, 60)] == [5, 10, 20, 40, 60, 60, 60]
    assert m.vol_again(1, True, 3600) == 300 and m.vol_again(3, True, 60) == 60 and m.vol_again(5, False, 30) == 30
    rspec = next(s for s in m.RANGE_MARKETS if s.symbol == "ETHUSDT" and s.start_ms)
    now = rspec.start_ms + 9 * 24 * H + 30 * 60_000
    real_time, clock = m.time, type("Clock", (), {"mono": 1000.0})()
    m.time = type("T", (), {"monotonic": staticmethod(lambda: clock.mono), "time": staticmethod(real_time.time), "perf_counter": staticmethod(real_time.perf_counter)})()
    try:
        # the daily up/down card (and the 15-minute ones, which read through the same read_quote)
        ud = m.UpDownMarket(m.Store(":memory:"), m.DAILY_UPDOWN_MARKETS[0].current(now))
        asked, state = [], {"fail": True}

        async def price(now_ms):
            ud.price, ud.priced_ms = D("100"), now_ms

        async def vol(now_ms):
            asked.append(clock.mono)
            if state["fail"]:
                raise m.RemoteError("HTTP 500: boom")
            ud.sigma, ud.sigma_ms = 0.5, now_ms
        ud.read_price, ud.read_vol = price, vol
        began = clock.mono
        for second in range(130):  # a pass a second for two minutes, the read failing throughout
            clock.mono = began + second
            try:
                _asyncio.run(ud.read_quote(now + second * 1000, clock.mono))
            except m.RemoteError:
                pass
        assert [round(a - began) for a in asked] == [0, 5, 15, 35, 75], [round(a - began) for a in asked]  # was 130 reads, one a pass
        assert ud.vol_error == "HTTP 500: boom", "said until a read answers, not only in the pass that tried"
        _asyncio.run(ud.refresh(now + 131_000))  # (a pass with no σ read due: the error still stands on the card)
        assert "HTTP 500: boom" in ud.error and len(asked) == 5, (ud.error, asked)
        state["fail"] = False
        clock.mono = began + 135
        _asyncio.run(ud.read_quote(now + 135_000, clock.mono))
        assert ud.sigma == 0.5 and ud.vol_fails == 0 and len(asked) == 6 and ud.vol_error == ""
        clock.mono += 600  # σ in hand: not read again before VOL_SECONDS
        _asyncio.run(ud.read_quote(now + 735_000, clock.mono))
        assert len(asked) == 6
        clock.mono += ud.VOL_SECONDS
        state["fail"] = True
        try:
            _asyncio.run(ud.read_quote(now + 5_000_000, clock.mono))
        except m.RemoteError:
            pass
        failed_at = clock.mono
        assert len(asked) == 7 and ud.sigma == 0.5
        clock.mono = failed_at + 299
        _asyncio.run(ud.read_quote(now + 5_299_000, clock.mono))
        assert len(asked) == 7, "with a σ in hand: again in five minutes, not at every pass"
        clock.mono = failed_at + 300
        try:
            _asyncio.run(ud.read_quote(now + 5_300_000, clock.mono))
        except m.RemoteError:
            pass
        assert len(asked) == 8
        # a read that comes back with no σ to be had yet (too few candles of the session) is tried again the same way
        ud2 = m.UpDownMarket(m.Store(":memory:"), m.DAILY_UPDOWN_MARKETS[0].current(now))
        seen = []

        async def none_yet(now_ms):
            seen.append(clock.mono)
        ud2.read_price, ud2.read_vol = price, none_yet
        began = clock.mono = 50_000.0
        for second in range(40):
            clock.mono = began + second
            _asyncio.run(ud2.read_quote(now, clock.mono))
        assert [round(a - began) for a in seen] == [0, 5, 15, 35], seen
        # the first-touch card: σ failing every time — read with the same back-off, and the path's scan still runs
        tm = m.TouchMarket(m.Store(":memory:"), next(s for s in m.TOUCH_MARKETS if getattr(s, "symbol", "")))
        tm.start_ms = now - 24 * H
        vols, scans = [], []

        async def get(path, **params):
            if path == "ticker/price":
                return {"price": str((D(str(tm.spec.low)) + D(str(tm.spec.high))) / 2)}
            vols.append(clock.mono)
            raise m.RemoteError("HTTP 500: boom")

        async def scan(now_ms):
            scans.append(clock.mono)
        tm.get, tm.scan = get, scan
        began = clock.mono = 90_000.0
        for second in range(40):
            clock.mono = began + second
            _asyncio.run(tm.refresh(now + second * 1000))
        assert [round(a - began) for a in vols] == [0, 5, 15, 35], vols
        assert scans and round(scans[0] - began) == 0 and tm.error == "HTTP 500: boom" and tm.sigma is None, (scans, tm.error)
        # a σ read cancelled in its middle (the round timed out, a shutdown) learnt nothing: due again as it was, not a whole cadence on
        for card, read in ((m.UpDownMarket(m.Store(":memory:"), m.DAILY_UPDOWN_MARKETS[0].current(now)), "quote"),
                           (m.TouchMarket(m.Store(":memory:"), next(s for s in m.TOUCH_MARKETS if getattr(s, "symbol", ""))), "refresh")):
            card.start_ms = getattr(card, "start_ms", 0)
            before = card.times["vol"]

            async def hang(*a, **k):
                await _asyncio.Event().wait()

            async def cancelled(card=card, read=read):
                if read == "quote":
                    card.read_price, card.read_vol = hang, hang
                    task = _asyncio.ensure_future(card.read_quote(now, clock.mono))
                else:
                    card.get = hang
                    card.times["price"] = clock.mono  # (only σ is due)
                    task = _asyncio.ensure_future(card.refresh(now))
                await _asyncio.sleep(0)
                await _asyncio.sleep(0)
                task.cancel()
                try:
                    await task
                except _asyncio.CancelledError:
                    pass
            _asyncio.run(cancelled())
            assert card.times["vol"] == before and card.sigma is None, (type(card).__name__, card.times["vol"], before)
    finally:
        m.time = real_time

_vol_backoff()

# audit MOD-RANGE-SCAN-EMPTY: an hourly klines answer that is empty (or stops short) without raising must not stamp the
# scan as "read up to now": "through" stays hours behind, the running hour is unknown, and advice must hold.
def _range_scan_empty():
    D, H = m.D, 3_600_000
    spec = m.RANGE_MARKETS[0]
    now = spec.start_ms + 9 * 24 * H + 30 * 60_000
    hour = now - now % H
    through = hour - 3 * H  # persisted: read up to three hours ago
    saved = {"start": spec.start_ms, "through": through, "open": 100000.0, "high": 104000.0, "high_at": spec.start_ms,
             "low": 96000.0, "low_at": spec.start_ms}
    rm = m.RangeMarket(m.Store(":memory:"), spec)
    rm.store.put(f"range:{spec.slug}", dict(saved))

    async def empty(path, **p):
        return []  # Binance (or a proxy in BINANCE_BASE_URL) answers [] for the range
    rm.get = empty
    _asyncio.run(rm.scan(now))
    rm.price, rm.priced_ms, rm.sigma, rm.sigma_ms = D("99000"), now, 0.5, now
    assert rm.history["through"] == through and rm.running == {}, (rm.history, rm.running)
    assert rm.scanned_ms != now, rm.scanned_ms
    assert rm.advice_problem(now) != "", rm.advice_problem(now)

    # a normal scan (the finished hours plus the running candle) still counts as read up to now
    rm2 = m.RangeMarket(m.Store(":memory:"), spec)
    rm2.store.put(f"range:{spec.slug}", dict(saved))

    async def full(path, **p):
        assert p["interval"] == "1h", p
        return [[t, "100000", "101000", "99000", "100500", "1", t + H - 1] for t in range(p["startTime"], hour + H, H)]
    rm2.get = full
    _asyncio.run(rm2.scan(now))
    rm2.price, rm2.priced_ms, rm2.sigma, rm2.sigma_ms = D("99000"), now, 0.5, now
    assert rm2.history["through"] == hour and rm2.running.get("open") == hour, (rm2.history, rm2.running)
    assert rm2.scanned_ms == now and rm2.advice_problem(now) == "", (rm2.scanned_ms, rm2.advice_problem(now))

_range_scan_empty()

# --- MOD-CAP-FIRST-HOUR: a cap window opening mid-hour reads that hour's minute bars again on every scan until the hour
# is over; a first scan inside that hour no longer marks it read and loses a spike later in it ------------------------
def _cap_first_hour():
    import asyncio as _asyncio
    import retired  # the retired cap specs (fixtures only; main's market lists are left alone)
    spec = next(s for s in retired.CAP_MARKETS if s.key == "NIULAI")
    H = 3600
    start_s = spec.start_ms // 1000
    assert start_s % H, start_s  # the fixture's window opens mid-hour
    first_hour = start_s - start_s % H
    spike = first_hour + 50 * 60  # 0.9 USD inside the window, after the first scan
    now = [start_s + 600]
    cap = m.CapMarket(m.Store(":memory:"), spec)
    cap.pool = "0xPOOL"

    async def get(url, payload=None):
        q = dict(p.split("=") for p in url.split("?")[1].split("&"))
        before, limit = int(q["before_timestamp"]), int(q["limit"])
        if "/ohlcv/minute" in url:
            rows = [[first_hour + i * 60, 0.1, 0.9 if first_hour + i * 60 == spike else 0.1, 0.1, 0.1, 1] for i in range(60)]
            rows = [r for r in rows if r[0] <= now[0]]  # only minutes that exist by then
        else:  # the opening hour's hourly bar carries the spike too but starts before the window: it must stay out
            rows = [[t, 0.1, 0.9 if t == first_hour else 0.12, 0.1, 0.1, 5] for t in range(first_hour - 10 * H, now[0], H)]
        return {"data": {"attributes": {"ohlcv_list": [r for r in rows if r[0] < before][-limit:][::-1]}}}
    cap.get = get
    _asyncio.run(cap.scan(now[0] * 1000))
    assert "first" not in cap.history, cap.history  # the opening hour is still running: read again next scan
    assert cap.coverage() != "", cap.coverage()
    now[0] = first_hour + 3 * H + 120
    _asyncio.run(cap.scan(now[0] * 1000))
    assert cap.history["first"] == "done" and cap.history["bar_high"] >= 0.9, cap.history
    assert cap.history["bar_at"] == spike, cap.history

    # the hourly bar of the opening hour is still left out: no minute spike, no 0.9
    now[0] = first_hour + 3 * H + 120
    cap2 = m.CapMarket(m.Store(":memory:"), spec)
    cap2.pool = "0xPOOL"

    async def flat(url, payload=None):
        if "/ohlcv/minute" in url:
            return {"data": {"attributes": {"ohlcv_list": []}}}
        return await get(url, payload)
    cap2.get = flat
    _asyncio.run(cap2.scan(now[0] * 1000))
    assert cap2.history["first"] == "skipped" and cap2.history["bar_high"] < 0.9, cap2.history

_cap_first_hour()

# audit MOD-PREWINDOW: before a ladder's window opens (a next-month ladder added early) a level already beyond today's
# price is not reached yet; the touch model must not price it as certain (1.0), which let the trader buy Yes near its cap.
def _prewindow():
    D = m.D
    import dataclasses as _dc
    import retired
    dec = m.RangeSpec("BTC-HIT-12", "what-price-will-bitcoin-hit-in-december-2026", "BTCUSDT", "spot", "BTC 12月价格",
                      m.et_ms(2026, 12, 1, 0, 0, -5), m.et_ms(2026, 12, 31, 23, 59, -5))
    rm = m.RangeMarket(m.Store(":memory:"), dec)
    now = m.et_ms(2026, 11, 25, 12, 0, -5)  # six days before the window opens
    rm.price, rm.priced_ms, rm.sigma, rm.sigma_ms = D("94000"), now, 0.55, now
    rm.fetched_ms = now
    assert not rm.reached(D("95000"), "down")
    assert rm.probability(D("95000"), "down", now) is None, rm.probability(D("95000"), "down", now)
    assert rm.probability(D("90000"), "up", now) is None
    # review fix-up: levels just short of today's price were still priced by the touch model from today's price (0.995 for
    # 0.1% inside, 0.952 for 1%, true P about 0.85 with six days to go) with no book_disputes hold: before the window opens
    # no level gets a price, near or far, on either side
    for lv, way in (("93900", "down"), ("93500", "down"), ("93000", "down"), ("90000", "down"), ("94100", "up"), ("100000", "up")):
        assert rm.probability(D(lv), way, now) is None, (lv, way, rm.probability(D(lv), way, now))
    assert rm.probability(D("93900"), "down", dec.start_ms - 1) is None
    # once the window is open the live price inside it is a touch: certain
    inside = dec.start_ms + 1
    rm.price, rm.priced_ms, rm.sigma_ms, rm.fetched_ms = D("94000"), inside, inside, inside
    assert rm.reached(D("95000"), "down") and rm.probability(D("95000"), "down", inside) == 1.0
    assert 0 < rm.probability(D("93000"), "down", inside) < 1  # and levels not reached get the touch model again
    # the same for a market-cap ladder whose window has not opened yet
    spec = next(s for s in retired.CAP_MARKETS if s.key == "NIULAI")
    spec = _dc.replace(spec, start_ms=now + 6 * 86_400_000, end_ms=now + 36 * 86_400_000)
    cap = m.CapMarket(m.Store(":memory:"), spec)
    cap.price, cap.priced_ms, cap.sigma = D("1"), now, 2.0
    cap.supply = D("1000000")
    assert cap.input_problem(now) == "" and cap.cap == D("1000000")
    assert cap.probability(D("900000"), now) is None
    assert cap.probability(D("1010000"), now) is None and cap.probability(D("2000000"), now) is None
    opened = spec.start_ms + 1
    cap.priced_ms = opened
    assert cap.input_problem(opened) == "" and 0 < cap.probability(D("2000000"), opened) < 1

_prewindow()


# --- MOD-PRICE-LEVEL-20XX: a level from 2000 to 2099 written without a thousands separator right after an arrow or $
# is the price, not a year; years elsewhere are still not read as levels ---------------------------------------------
from decimal import Decimal as _D
for _title, _want in [("↑ 2050", "2050"), ("↓ 2000", "2000"), ("↑ $2050", "2050"), ("↑ 2099", "2099"),
                      ("Will Ethereum reach $2050 in November 2026?", "2050"), ("↓ 2050 November 2026", "2050"),
                      ("Will BTC reach $150k in 2026?", "150000"), ("October 2026 ↑ 130k", "130000"),
                      ("↑ 130k before 2027", "130000"), ("↑ 2,050", "2050"), ("↑ 130,000", "130000")]:
    assert m.price_level(_title) == _D(_want), (_title, m.price_level(_title))
assert m.price_level("October 2026") is None and m.price_level("Yes") is None


# --- MOD-STOCKRANGE-CHOOSE: once a stock ladder's card moves to a later deadline's market (choose / learn move its
# window), a trade on the earlier market is not judged on the new market's window and extremes -------------------------
def _stock_range_switch():
    import retired, types, datetime as dt, time as t
    saved = m.STOCK_HIT_MARKETS
    m.STOCK_HIT_MARKETS = retired.STOCK_HIT_MARKETS
    try:
        strc, = m.STOCK_HIT_MARKETS
        utc = lambda *a: int(dt.datetime(*a, tzinfo=dt.timezone.utc).timestamp() * 1000)
        rm = m.StockRangeMarket(m.Store(":memory:"), strc)
        old = m.LadderRow(m.D("100"), "41", "STRC hits $100 by September 30?", None, "", "Will STRC hit $100 by September 30?")
        new = m.LadderRow(m.D("100"), "77", "STRC hits $100 by December 31?", None, "", "Will STRC hit $100 by December 31?")
        meta = {"41": ({"created_ms": utc(2026, 6, 18, 0, 16), "question": old.question, "rules": ""}, t.monotonic()),
                "77": ({"created_ms": utc(2026, 9, 1, 14, 0), "question": new.question, "rules": ""}, t.monotonic())}
        rm.learn(rm.choose([old], meta), meta)
        old_end = rm.end_ms
        assert m.et_date(old_end) == dt.date(2026, 9, 30), m.et_date(old_end)
        picked = rm.choose([old, new], meta)
        assert [r.market_id for r in picked] == ["77"], picked
        rm.learn(picked, meta)
        assert m.et_date(rm.end_ms) == dt.date(2026, 12, 31) and rm.end_ms != old_end
        bars, d = [], dt.date(2026, 9, 1)
        while d <= dt.date(2026, 10, 9):  # $100 is touched on 10-05, after the September market's deadline
            if m.us_session(d):
                bars.append((m.us_session(d)[0], 99.0, 100.4 if d == dt.date(2026, 10, 5) else 99.6, 98.0, 99.5))
            d += dt.timedelta(days=1)
        now = utc(2026, 10, 9, 18, 0)
        rm.scan_days(bars, now)
        fake = types.SimpleNamespace(ranges={strc.key: rm}, SIM_SETTLE_MS=m.Bot.SIM_SETTLE_MS,
                                     predict=types.SimpleNamespace(ladders={strc.key: picked}, yes_book=lambda row: (None, None)))
        trade = lambda mid, end: {"kind": "range", "key": strc.key, "market": f"{strc.slug}#{mid}",
                                  "settle": {"target": "100", "dir": "up", "end": end}}
        assert m.Bot.sim_result(fake, trade("41", old_end), now) is None  # the old market: Predict settles it
        res = m.Bot.sim_result(fake, trade("77", rm.end_ms), now)  # the card's current market: still a touch
        assert res is not None and res[0] == 1.0, res
        legacy = {"kind": "range", "key": strc.key, "market": f"{strc.slug}#77", "settle": {"target": "100", "dir": "up"}}
        assert m.Bot.sim_result(fake, legacy, now)[0] == 1.0  # a record without "end" keeps the previous behaviour
    finally:
        m.STOCK_HIT_MARKETS = saved

_stock_range_switch()

print("AUDIT_MODELS_B_OK")
