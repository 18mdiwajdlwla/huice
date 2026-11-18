"""Regression tests for the bot-core audit findings (cluster botcore)."""
import csv, io, math, sys, time
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

# --- BOT-CSV-REALIZED-Z: the /samples CSV's realized_z is the residual /calib and SIGMA_CALIB use (sample_z) -------------
cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "BTCUSDT"})
store = m.Store(":memory:")
class FakeMarket:
    def now_ms(self): return 1_790_000_000_000
bot = m.Bot(cfg, store, FakeMarket(), None)
store.delete_prefix("wsample:")
# a daily crypto sample at 12 h left whose price had already moved +1% from the line; the day closed +1.5% from the line
tau = 12 * 3600 / (365 * 86400)
settled = {"t": 1_789_990_000_000, "slug": "btc-updown-day-x", "key": "BTC-DAY", "item": "BTC 日涨跌", "kind": "day", "card": "BTC 日涨跌",
           "start": 1, "end": 2, "left": 43200, "up": 0.7, "sigma": 0.5, "sigma_raw": 0.5, "tau": tau, "price": 101.0, "line": 100.0, "model": 2}
assert bot.window_settle_row(settled, 101.5)
store.put("wsample:btc-updown-day-x:43200", settled)
# a sample of a market not ended yet: no result, so no realized_z
store.put("wsample:btc-updown-day-y:43200", {**settled, "slug": "btc-updown-day-y", "final": None, "move": None, "result": None, "t": 1_789_990_000_001})
table = list(csv.reader(io.StringIO(bot.window_csv().lstrip("﻿"))))
head, rows = table[0], {r[3]: r for r in table[1:]}
row = next(r for r in bot.window_rows() if r["slug"] == "btc-updown-day-x")
st = 0.5 * math.sqrt(tau)
z_cell = rows["btc-updown-day-x"][head.index("realized_z")]
assert z_cell == f"{m.sample_z(row):.6g}" and abs(float(z_cell) - math.log(101.5 / 101.0) / st) < 1e-5, z_cell
assert abs(float(z_cell) - 0.266851) < 1e-5 and abs(float(z_cell) - row["move"] / st) > 0.5, z_cell  # not the move from the line (≈0.8045)
assert rows["btc-updown-day-x"][head.index("sigma_sqrt_tau")] == f"{st:.6g}"
assert rows["btc-updown-day-y"][head.index("realized_z")] == ""
store.delete_prefix("wsample:")

# --- BOT-STOCK-QUOTE-CLOSE: a realtime quote stamped before the closing auction is not that day's close -----------------
import asyncio, datetime as dt
D = m.D
def bj_ms(y, mo, d, h, mi): return int(dt.datetime(y, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
def tencent_sh(last, prev, when):
    fields = ["0"] * 40
    fields[3], fields[4], fields[30] = last, prev, when
    return ('v_sh688836="' + "~".join(fields) + '";').encode("gbk")
sh_info = m.STOCK_MARKETS["sh"]
stuck = tencent_sh("47.90", "50.00", "20261009143000")  # feed stuck on the 14:30 print
try:
    got = m.parse_quote_close("腾讯", "sh", stuck, sh_info, bj_ms(2026, 10, 9, 15, 20))
    raise AssertionError(f"a 14:30 print was taken as the close: {got}")
except ValueError as error:
    assert "早于收盘竞价" in str(error), error
try:  # also on a later day (the stuck record still dated 10-09)
    m.parse_quote_close("腾讯", "sh", stuck, sh_info, bj_ms(2026, 10, 10, 10, 0)); raise AssertionError("stale close accepted")
except ValueError:
    pass
# while the session runs the record still gives the previous close (unchanged behaviour)
assert m.parse_quote_close("腾讯", "sh", stuck, sh_info, bj_ms(2026, 10, 9, 14, 40))[:2] == (None, D("50.00"))
good = tencent_sh("48.10", "50.00", "20261009150003")
assert m.parse_quote_close("腾讯", "sh", good, sh_info, bj_ms(2026, 10, 9, 15, 20))[:2] == (dt.date(2026, 10, 9), D("48.10"))
hk_info = m.STOCK_MARKETS["hk"]
sina_hk_early = ('var hq_str_rt_hk00625="SHEIN,希音,38.00,38.10,38.20,37.50,37.76,' + ",".join(["0"] * 10) + ',2026/10/09,15:40:00";').encode("gbk")
try:
    m.parse_quote_close("新浪", "hk", sina_hk_early, hk_info, bj_ms(2026, 10, 9, 17, 0)); raise AssertionError("HK 15:40 print accepted")
except ValueError:
    pass

async def quote_close_fallback():
    # Eastmoney down, Tencent stuck on 14:30 -> StockMarket asks Sina, whose 15:00:03 close is used
    sina_good = ('var hq_str_sh688836="宇树科技,49.00,50.00,48.10,' + ",".join(["0"] * 26) + ',2026-10-09,15:00:03,00";').encode("gbk")
    async def fake_get(url, timeout=15, headers=None):
        if "eastmoney" in url: raise m.RemoteError("网络错误 (RemoteDisconnected)")
        if "qt.gtimg.cn" in url: return stuck
        if "sina" in url: return sina_good
        raise AssertionError(url)
    real_get, real_sleep = m.http_get, m.asyncio.sleep
    async def no_sleep(_s): await real_sleep(0)
    m.http_get, m.asyncio.sleep = fake_get, no_sleep
    try:
        cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT"})
        sm = m.StockMarket(cfg, m.Store(":memory:"))
        await sm.refresh(bj_ms(2026, 10, 9, 15, 20), force=True)
        close = sm.closes["UNITREEUSDT"]
        assert close.value == D("48.10") and "新浪" in close.source, (close, sm.errors)
        # Sina stuck too -> no close for the day is invented from a 14:30 print
        sina_stuck = ('var hq_str_sh688836="宇树科技,49.00,50.00,47.90,' + ",".join(["0"] * 26) + ',2026-10-09,14:30:00,00";').encode("gbk")
        async def all_stuck(url, timeout=15, headers=None):
            if "eastmoney" in url: raise m.RemoteError("网络错误 (RemoteDisconnected)")
            return stuck if "qt.gtimg.cn" in url else sina_stuck
        m.http_get = all_stuck
        sm2 = m.StockMarket(cfg, m.Store(":memory:"))
        await sm2.refresh(bj_ms(2026, 10, 9, 15, 20), force=True)
        assert "UNITREEUSDT" not in sm2.closes or sm2.closes["UNITREEUSDT"].value != D("47.90"), sm2.closes
        assert "早于收盘竞价" in sm2.errors.get("UNITREEUSDT", ""), sm2.errors
    finally:
        m.http_get, m.asyncio.sleep = real_get, real_sleep
asyncio.run(quote_close_fallback())

# --- BOT-NOTE-CLOSES-SIM-OFF: with the paper trader off, the stock cards' closes are still recorded for /calib ----------
close_ms = bj_ms(2026, 10, 5, 15, 0)
for env in ({"SIM": "off"}, {"PREDICT": "off"}):
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", **env})
    store = m.Store(":memory:")
    bot = m.Bot(cfg, store, m.Binance(cfg), None)
    assert "模拟交易" not in [n for n, _ in bot.reference_jobs()]  # sim_step (the old only caller) is not scheduled
    bot.sse_odds = bot.kospi_odds = bot.hsi_odds = lambda now: "暂缺"
    bot.stocks.closes["UNITREEUSDT"] = m.Baseline(D("75.5"), "k", "label", 0, close_ms, "", "src")
    bot.record_predictions(close_ms + 3_600_000)
    assert store.get("outcome:UNITREEUSDT:2026-10-05") == 75.5, (env, store.get("outcome:UNITREEUSDT:2026-10-05"))
    assert store.get("outsrc:UNITREEUSDT:2026-10-05")["source"] == "src", env
    bot.record_predictions(close_ms + 7_200_000)  # again: idempotent
    assert store.get("outcome:UNITREEUSDT:2026-10-05") == 75.5

# --- BOT-SIGMA-CALIB-SCAN: the windows settling at a quarter hour cost one pass over the samples, not one per card ---------
# (each sigma_calib miss used to re-read, parse and sort every wsample: row for the one key asked for; refresh_updown asks for
# every card's, so the refresh after twenty-odd windows settled made twenty-odd full passes back to back on the event loop)
T_QH = bj_ms(2026, 11, 10, 21, 0)  # a Tuesday night quarter hour: the round-the-clock window cards end together
class Clock:
    now = T_QH - 1_000
    def now_ms(self): return self.now
clock, sstore = Clock(), m.Store(":memory:")
sbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), sstore, clock, None)
assert sbot.config.sigma_calib  # on by default
async def no_read(*a): return None
for mk in sbot.updowns.values():
    mk.refresh = no_read  # the card's own Binance reads: not under test
sbot.updown_strike = no_read
def srow(key, slug, kind, left, t, final, model):
    """A sample; settled (final, move, result) when ``final`` is given."""
    tau = left / (365 * 86400)
    return {"t": t, "slug": slug, "key": key, "item": slug, "kind": kind, "card": key, "start": t - 60_000, "end": t + left * 1000, "left": left,
            "up": 0.5, "sigma": 0.6, "sigma_raw": 0.6, "calib": 1.0, "tau": tau, "price": 100.0, "line": 100.0, "model": model,
            "final": final, "move": None if final is None else math.log(final / 100.0), "result": None if final is None else float(final > 100.0)}
for ci, (key, wspec) in enumerate(sbot.updown_windows.items()):  # six settled past windows a card, z from -1.4 to 1.4
    model, lefts = sbot.updowns[key].SIGMA_MODEL, (300, 120) if wspec.minutes == 5 else (840, 300)
    for i in range(6):
        t = T_QH - (i + 2) * 900_000
        for left in lefts:
            final = 100.0 * math.exp(0.6 * math.sqrt(left / (365 * 86400)) * (((ci + i) % 5) - 2) * 0.7 * (1 if left == lefts[0] else 3))
            sstore.put(f"wsample:{key.lower()}-past-{i}:{left}", srow(key, f"{key.lower()}-past-{i}", "window", left, t, final, model))
    sstore.put(f"wsample:{key.lower()}-old:840", srow(key, f"{key.lower()}-old", "window", 840, T_QH - 99 * 900_000, 103.0, model - 1))  # older estimator
for di, key in enumerate(sbot.updown_daily):
    for d in range(3):
        sstore.put(f"wsample:{key.lower()}-past-{d}:43200", srow(key, f"{key.lower()}-past-{d}", "day", 43200, T_QH - (d + 1) * 86_400_000,
                                                                 100.0 * math.exp(0.6 * math.sqrt(0.5 / 365) * (d - di) * 0.9), m.UpDownMarket.SIGMA_MODEL))
passes: list[int] = []
real_each = sbot.window_each
def counted_each(late=None):
    """A pass over every sample (1.154.25: read one at a time — window_each; the reports' window_rows goes through it too)."""
    passes.append(1)
    return real_each(late)
sbot.window_each = counted_each
refbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), sstore, clock, None)  # the reference reads the samples through a bot of its own
real_rows = refbot.window_rows

def calib_alone(key):
    """One key measured on its own, as sigma_calib did before (the reference: same rows, same rule)."""
    current = m.UpDownMarket.SIGMA_MODEL if key == m.CRYPTO_DAY_CALIB else getattr(sbot.updowns.get(key), "SIGMA_MODEL", m.IndexUpDownMarket.SIGMA_MODEL)
    firsts: dict = {}
    for r in real_rows():
        mine = r.get("kind") == "day" if key == m.CRYPTO_DAY_CALIB else (r.get("key") == key and r.get("kind") == "window")
        if not mine or r.get("move") is None or int(r.get("model") or 1) < current or not r.get("sigma_raw") or not r.get("tau"):
            continue
        if r["slug"] not in firsts or int(r["left"]) > int(firsts[r["slug"]]["left"]):
            firsts[r["slug"]] = r
    zs = [z for z in (m.sample_z(r, raw=True) for r in firsts.values()) if z is not None]
    # (the exactly rounded sum since 1.154.25: the factor does not depend on the order its windows were counted in)
    f = math.sqrt((math.fsum(z * z for z in zs) + sbot.SIGMA_CALIB_PRIOR) / (len(zs) + sbot.SIGMA_CALIB_PRIOR)) if zs else 1.0
    return min(max(f, sbot.SIGMA_CALIB_RANGE[0]), sbot.SIGMA_CALIB_RANGE[1]), len(zs)

def calib_key(key, mk): return key if isinstance(mk, m.IndexUpDownMarket) else m.CRYPTO_DAY_CALIB
def check_factors():
    for key, mk in sbot.updowns.items():
        assert mk.calib == calib_alone(calib_key(key, mk)), (key, mk.calib, calib_alone(calib_key(key, mk)))  # exactly the same figure

def settle_boundary(end_ms, close):
    """The cards whose window ends at end_ms: their own samples (not settled yet) and their final close kept, as the card's refresh reads it."""
    ending = [key for key in sbot.updown_windows if sbot.updowns[key].spec.end_ms + 60_000 == end_ms]
    for key in ending:
        spec = sbot.updowns[key].spec
        for left in (300, 120) if sbot.updown_windows[key].minutes == 5 else (840, 600, 300, 120):
            sstore.put(f"wsample:{spec.slug}:{left}", srow(key, spec.slug, "window", left, end_ms - left * 1000, None, sbot.updowns[key].SIGMA_MODEL))
        sstore.put(f"updown:{spec.slug}:end", {"open": spec.end_ms, "close": close, "read_ms": end_ms + 2_000})
    return ending

def fresh_bot():
    """Another bot on the same samples: its factors are those of one pass over every sample (the reference of 1.154.25)."""
    fresh = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), sstore, clock, None)
    for mk in fresh.updowns.values():
        mk.refresh = no_read
    fresh.updown_strike = no_read
    return fresh

def check_against_a_pass(why):
    """The factors in hand — a market at a time since the first measure — are those of a pass over every sample now."""
    fresh = fresh_bot()
    fresh.sigma_measure()
    for key in {*fresh.sigma_calib_cache, *sbot.sigma_calib_cache}:
        assert sbot.sigma_calib(key) == fresh.sigma_calib(key) == calib_alone(key), (why, key, sbot.sigma_calib(key), fresh.sigma_calib(key), calib_alone(key))
    assert {k: v for k, v in sbot.sigma_moves.items() if v} == {k: v for k, v in fresh.sigma_moves.items() if v}, why

async def calib_scan():
    await sbot.refresh_updown(clock.now)  # the start: every card's factor measured
    assert len(passes) == 1, f"the first refresh read every sample {len(passes)} times"  # 24 before: one pass per card (the daily cards share one)
    check_factors()
    assert all(sbot.updowns[k].calib[1] for k in sbot.updown_rolling)  # every rolling card has windows behind its factor
    ending = settle_boundary(T_QH, "103.5")
    assert len(ending) >= 3, ending  # the round-the-clock 15-minute and 5-minute cards (twenty in 1.149.0)
    slugs, before = {k: mk.spec.slug for k, mk in sbot.updowns.items()}, {k: mk.calib for k, mk in sbot.updowns.items()}
    passes.clear()
    clock.now = T_QH + 2_000
    await sbot.refresh_updown(clock.now)  # the roll: window_resolve settles each card's samples and counts them into its factor
    rolled = sorted(k for k, mk in sbot.updowns.items() if mk.spec.slug != slugs[k])
    assert rolled == sorted(ending) and not passes, (rolled, passes)
    # 1.154.25: the factor is there at once — the market counted in by itself (it was dropped, and measured again at the next
    # refresh by a pass over every sample: seconds of the event loop at every quarter hour and 5-minute mark)
    assert m.CRYPTO_DAY_CALIB in sbot.sigma_calib_cache and all(k in sbot.sigma_calib_cache for k in rolled)
    assert all(sbot.sigma_calib_cache[k] == calib_alone(k) != before[k] for k in rolled), "the settled windows (a 3.5% end) move their cards' factors"
    clock.now = T_QH + 3_000
    await sbot.refresh_updown(clock.now)
    assert not passes, f"{len(rolled)} windows settled; the refresh after read every sample {len(passes)} times"  # 1 in 1.150.0, len(rolled) + 1 before
    check_factors()
    assert all(sbot.updowns[k].calib != before[k] for k in rolled)
    check_against_a_pass("a quarter hour")
    # the 5-minute mark: only the 5-minute cards end — no pass either, the pool and the 15-minute cards' factors as they were
    five = settle_boundary(T_QH + 300_000, "99.0")
    assert all(sbot.updown_windows[k].minutes == 5 for k in five), five
    if five:  # (while the bot has 5-minute cards: BTC / ETH / BNB in 1.149.0)
        slugs, others = {k: mk.spec.slug for k, mk in sbot.updowns.items()}, {k: v for k, v in sbot.sigma_calib_cache.items() if k not in five}
        passes.clear()
        clock.now = T_QH + 302_000
        await sbot.refresh_updown(clock.now)
        assert sorted(k for k, mk in sbot.updowns.items() if mk.spec.slug != slugs[k]) == sorted(five) and not passes
        assert {k: v for k, v in sbot.sigma_calib_cache.items() if k not in five} == others
        clock.now = T_QH + 303_000
        await sbot.refresh_updown(clock.now)
        assert not passes, passes  # 1 in 1.150.0; 4 before: the three cards' and the pool's
        check_factors()
        check_against_a_pass("a 5-minute mark")
    # a daily market settling moves the pool, by itself too
    btc_day = next(k for k in sbot.updown_daily)
    day_spec = sbot.updowns[btc_day].spec
    pool = sbot.sigma_calib(m.CRYPTO_DAY_CALIB)
    sstore.put(f"wsample:{day_spec.slug}:43200", srow(btc_day, day_spec.slug, "day", 43200, T_QH - 43_200_000, None, m.UpDownMarket.SIGMA_MODEL))
    sbot.window_resolve(sbot.updowns[btc_day], day_spec, D("109"))
    assert not passes and sbot.sigma_calib(m.CRYPTO_DAY_CALIB) == calib_alone(m.CRYPTO_DAY_CALIB) != pool
    sbot.window_resolve(sbot.updowns[btc_day], day_spec, D("109"))  # (told twice: counted once)
    assert sbot.sigma_calib(m.CRYPTO_DAY_CALIB) == calib_alone(m.CRYPTO_DAY_CALIB)
    check_against_a_pass("a daily market")
    # quarter hours and 5-minute marks on end, closes at random: always the figure of a pass over every sample
    import random
    rng = random.Random(24)
    for step in range(1, 13):
        end_ms = T_QH + step * 300_000 + 300_000
        clock.now = end_ms - 1_000
        await sbot.refresh_updown(clock.now)
        ended = settle_boundary(end_ms, str(round(100 * math.exp(rng.gauss(0, 0.002)), 4)))
        clock.now = end_ms + 2_000
        await sbot.refresh_updown(clock.now)
        clock.now = end_ms + 3_000
        await sbot.refresh_updown(clock.now)
        assert not passes, (step, passes)
        check_factors()
        if step % 4 == 0 or not ended:
            check_against_a_pass(f"step {step}")
    check_against_a_pass("the last step")
    # a sample settled late — its market ended while the card was elsewhere; a report reads the samples and settles it
    # (window_rows) — is counted in from there
    key15 = next(k for k in sbot.updown_windows if sbot.updown_windows[k].minutes == 15)
    sstore.put("wsample:late-window:840", srow(key15, "late-window", "window", 840, T_QH - 5 * 900_000 - 840_000, None, sbot.updowns[key15].SIGMA_MODEL))
    sstore.put("updown:late-window:end", {"open": T_QH - 5 * 900_000, "close": "104.2", "read_ms": clock.now})
    had = sbot.sigma_calib(key15)
    assert len(sbot.window_rows()) == len(sstore.keys("wsample:")) and passes == [1]  # (the report's own read: /calib)
    passes.clear()
    assert sbot.sigma_calib(key15) == calib_alone(key15) != had and sbot.sigma_calib(key15)[1] == had[1] + 1
    check_against_a_pass("a sample settled late")
    # the day's pruning: a market whose earliest sample is too old is counted on a later one still kept, one with nothing
    # kept counts no more — from the rows the pruning read, no pass after it
    now = clock.now
    cut = now - m.WSAMPLE_KEEP_DAYS * m.DAY_MS
    model = sbot.updowns[key15].SIGMA_MODEL
    sstore.put("wsample:aged-a:840", srow(key15, "aged-a", "window", 840, cut - 60_000, 107.0, model))   # too old...
    sstore.put("wsample:aged-a:300", srow(key15, "aged-a", "window", 300, cut + 480_000, 101.0, model))  # ...its later sample is not
    sstore.put("wsample:aged-b:840", srow(key15, "aged-b", "window", 840, cut - 900_000, 95.0, model))    # all of it too old
    aged = fresh_bot()
    aged.window_each = lambda late=None, each=aged.window_each: passes.append(2) or each(late)
    assert aged.sigma_calib(key15) == calib_alone(key15) and aged.sigma_calib(key15)[1] == sbot.sigma_calib(key15)[1] + 2 and passes == [2]
    passes.clear()
    assert aged.window_prune(now) == 2 and sstore.get("wsample:aged-a:840") is None and sstore.get("wsample:aged-a:300") is not None
    assert not passes and aged.sigma_calib(key15) == calib_alone(key15) and aged.sigma_calib(key15)[1] == sbot.sigma_calib(key15)[1] + 1
    assert "aged-b" not in aged.sigma_moves[key15] and aged.sigma_moves[key15]["aged-a"] == m.sample_z(sstore.get("wsample:aged-a:300"), raw=True) ** 2
    # ...and a sample settled late on the pruning's way (its market's end kept after the card had moved on) is counted in
    # there: the pass every 5 minutes used to find those
    sstore.put("wsample:late-b:840", srow(key15, "late-b", "window", 840, T_QH - 7 * 900_000 - 840_000, None, model))
    sstore.put("updown:late-b:end", {"open": T_QH - 7 * 900_000, "close": "97.1", "read_ms": clock.now})
    had = aged.sigma_calib(key15)
    assert aged.window_prune(now) == 0 and sstore.get("wsample:late-b:840")["final"] == 97.1
    assert not passes and aged.sigma_calib(key15) == calib_alone(key15) != had and aged.sigma_calib(key15)[1] == had[1] + 1
    sstore.delete_keys(["wsample:aged-a:300"])
    sbot.sigma_moves = None  # (its figures measured again below: the rows above were put in, and taken out, by hand)
    # ...and it is not begun while the rule taker goes first (seconds of the event loop: any other time of the day will do)
    sbot.wsample_pruned, pruned = time.monotonic() - sbot.DAY_PASS_S - 60, []
    sbot.window_prune = lambda now_ms: pruned.append(now_ms) or 0
    sbot.rules_clear = lambda seconds: False  # (it goes first now, or will within three minutes: Bot.day_pass_due)
    stale = sbot.wsample_pruned
    sbot.window_sample(clock.now)
    assert not pruned and sbot.wsample_pruned == stale
    del sbot.rules_clear
    assert sbot.rules_first() is False and sbot.rules_clear(180) is True
    sbot.window_sample(clock.now)
    assert pruned == [clock.now] and sbot.wsample_pruned > stale
    sbot.window_sample(clock.now)
    assert len(pruned) == 1, "once a day"
    del sbot.window_prune
    # a fresh start on the same samples: one pass for all the cards, the same figures
    fresh = fresh_bot()
    fresh_passes: list[int] = []
    fresh.window_each = lambda late=None, each=fresh.window_each: fresh_passes.append(1) or each(late)
    await fresh.refresh_updown(clock.now)
    await sbot.refresh_updown(clock.now)
    assert len(fresh_passes) == 1 and all(fresh.updowns[k].calib == sbot.updowns[k].calib for k in sbot.updowns), fresh_passes
asyncio.run(calib_scan())

# --- 1.154.25: Store.each — the records one at a time, in the keys' order, the queued ones in their place — and the pruning
# on a database on disk with records still queued (the review of 1.154.25: the queued ones came last of all, a market's
# samples in two pieces: counted on a part of them)
import tempfile
with tempfile.TemporaryDirectory() as folder:
    disk = m.Store(folder + "/each.db")
    try:
        for i in range(6):
            disk.put(f"wsample:m{i}:840", {"n": i, "left": 840})
            disk.put(f"wsample:m{i}:300", {"n": i, "left": 300})
        disk.put("other:x", {"n": -1})
        assert disk.flush() and not disk.pending
        with disk.lock:  # (the writer waits for it: what is put now stays queued)
            disk.put("wsample:m2:840", {"n": 2, "left": 840, "again": True})   # a newer version of a record in the table
            disk.put("wsample:m3:120", {"n": 3, "left": 120})                   # a new record: between m3:840's neighbours
            disk.put("wsample:a0:840", {"n": 9, "left": 840})                   # ...one before every key, one after
            disk.put("wsample:z9:840", {"n": 8, "left": 840})
            disk.delete_keys(["wsample:m4:300"])                                # ...and one deleted
            assert len(disk.pending) == 5
            got = list(disk.each("wsample:"))
            assert got == disk.items("wsample:"), "the same records, in the same order, as items() — one at a time"
            keys = [k for k, _ in got]
            assert keys == sorted(keys) and len(keys) == len(set(keys)) == 14 and "wsample:m4:300" not in keys
            assert dict(got)["wsample:m2:840"].get("again") and keys.index("wsample:m3:120") + 1 == keys.index("wsample:m3:300")
            assert list(disk.each("none:")) == [] and [k for k, _ in disk.each("other:")] == ["other:x"]
        assert disk.flush() and list(disk.each("wsample:")) == got == disk.items("wsample:")
    finally:
        disk.close()
    # the pruning, some of a market's samples still queued when it begins (its own flush cannot finish: the writer is held)
    dstore = m.Store(folder + "/prune.db")
    try:
        dclock = Clock()
        dclock.now = T_QH
        dbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), dstore, dclock, None)
        dkey = next(k for k in dbot.updown_windows if dbot.updown_windows[k].minutes == 15)
        dmodel, dcut = dbot.updowns[dkey].SIGMA_MODEL, T_QH - m.WSAMPLE_KEEP_DAYS * m.DAY_MS
        for i in range(5):
            dstore.put(f"wsample:past-{i}:840", srow(dkey, f"past-{i}", "window", 840, T_QH - (i + 2) * 900_000, 100.0 + i, dmodel))
        dstore.put("wsample:aged-m:840", srow(dkey, "aged-m", "window", 840, dcut - 60_000, 107.0, dmodel))
        assert dstore.flush()
        assert dbot.sigma_calib(dkey)[1] == 6  # (the five, and aged-m on its first sample)
        # a market whose end was kept after the card had moved on: its sample not settled yet
        dstore.put("wsample:late-m:840", srow(dkey, "late-m", "window", 840, T_QH - 9 * 900_000 - 840_000, None, dmodel))
        dstore.put("updown:late-m:end", {"open": T_QH - 9 * 900_000, "close": "96.4", "read_ms": T_QH})
        assert dstore.flush()
        real_flush, dstore.flush = dstore.flush, lambda timeout=10.0: False  # (as when the writer does not finish in time)
        with dstore.lock:
            dstore.put("wsample:aged-m:300", srow(dkey, "aged-m", "window", 300, dcut + 480_000, 101.0, dmodel))   # its later sample: queued
            dstore.put("wsample:late-m:300", srow(dkey, "late-m", "window", 300, T_QH - 9 * 900_000 - 300_000, None, dmodel))
            assert len(dstore.pending) == 2 and dbot.window_prune(T_QH) == 1
        dstore.flush = real_flush
        assert dstore.flush()
        dfresh = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), dstore, dclock, None)
        assert dbot.sigma_calib(dkey) == dfresh.sigma_calib(dkey) and dbot.sigma_calib(dkey)[1] == 7, (dbot.sigma_calib(dkey), dfresh.sigma_calib(dkey))
        assert dbot.sigma_moves[dkey] == dfresh.sigma_moves[dkey]
        assert dbot.sigma_moves[dkey]["aged-m"] == m.sample_z(dstore.get("wsample:aged-m:300"), raw=True) ** 2, "counted on what is kept of it"
        assert dbot.sigma_moves[dkey]["late-m"] == m.sample_z(dstore.get("wsample:late-m:840"), raw=True) ** 2, "on its earliest sample, both settled"
        # a pass that fails half-way has changed nothing: the old samples are dropped, and the factors touched, after it
        before = (dict(dbot.sigma_moves[dkey]), dbot.sigma_calib(dkey), dstore.keys("wsample:"))
        dstore.put("wsample:past-0:840", {**dstore.get("wsample:past-0:840"), "t": dcut - 1})
        assert dstore.flush()
        real_items = dbot.window_items

        def broken_items(late=None):
            for n, item in enumerate(real_items(late)):
                if n == 3:
                    raise RuntimeError("读到一半坏了")
                yield item
        dbot.window_items = broken_items
        failed = False
        try:
            dbot.window_prune(T_QH)
        except RuntimeError:
            failed = True
        del dbot.window_items
        assert failed and (dict(dbot.sigma_moves[dkey]), dbot.sigma_calib(dkey), dstore.keys("wsample:")) == before
        assert dbot.window_prune(T_QH) == 1 and dstore.get("wsample:past-0:840") is None and dbot.sigma_calib(dkey)[1] == 6
        # a market's samples that cannot be read as such (never this code's own): nothing half counted — measured again from
        # the samples at the next call
        logged = []
        dbot.log_limited = lambda key, text, every=60: logged.append(key)
        dbot.sigma_note("past-1", [{"kind": "window", "key": dkey, "slug": "past-1", "left": "x", "move": 0.1, "sigma_raw": 0.5, "tau": 1e-5, "model": dmodel}] * 2)
        assert dbot.sigma_moves is None and logged == ["sigma-note"]
        assert dbot.sigma_calib(dkey) == m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), dstore, dclock, None).sigma_calib(dkey) and dbot.sigma_calib(dkey)[1] == 6
    finally:
        dstore.close()

# index_sigma_calibs (on each outcome): the three indices' own snapshots and outcomes, not every card's
import random
rnd = random.Random(5)
for key in ("HSI", "KOSPI", "SSE", "UNITREEUSDT", "SKHYNIXUSDT"):
    for i in range(30):
        day, ms = f"2026-09-{1 + i % 25:02d}", 1_790_000_000_000 + i * 1_800_000
        sstore.put(f"pred:{key}:{ms}", {"key": key, "t": ms, "target": day, "mode": "盘中" if i % 3 else "盘后", "ref": 100.0, "ref_raw": 100.0 + i % 2,
                                        "up": 0.5, "move": 0.001 * (i % 5), "beta": 1.0, "sigma": 0.01, "R": 0.5 + (i % 4) / 10})
        sstore.put(f"outcome:{key}:{day}", 100.0 * math.exp(rnd.gauss(0, 0.012)))
sstore.put("pred:KOSPI:9000001", {"key": "KOSPI", "t": 9000001, "target": "2026-09-02", "mode": "盘中", "ref": 100.0, "up": 0.5, "move": 0.0,
                                  "beta": 1.0, "sigma": 0.01, "R": 1.0})  # a short stamp sorts after the others, in either read
def index_calibs_reference():
    """The factors as measured before: every card's pred: and outcome: records read, the indices' kept."""
    saved = [v for _, v in sstore.items("pred:") if isinstance(v, dict) and m.Bot.PRED_FIELDS <= v.keys() and v.get("key") in m.PREDICT_KEYS.values()]
    outcomes = {k.removeprefix("outcome:"): float(v) for k, v in sstore.items("outcome:")}
    groups: dict = {}
    for p in saved:
        close = outcomes.get(f"{p['key']}:{p['target']}")
        if close is not None and p.get("ref_raw", p.get("ref")):
            groups.setdefault((p["key"], str(p.get("mode") or "")), []).append(dict(p, y=math.log(close / float(p.get("ref_raw", p["ref"])))))
    return {key: m.residual_factor(rows) for key, rows in groups.items()}
read: list[str] = []
real_items = sstore.items
sstore.items = lambda prefix: read.append(prefix) or real_items(prefix)
got = sbot.index_sigma_calibs()
sstore.items = real_items
assert list(got.items()) == list(index_calibs_reference().items()) and len(got) == 6, got
assert sorted(read) == ["outcome:HSI:", "outcome:KOSPI:", "outcome:SSE:", "pred:HSI:", "pred:KOSPI:", "pred:SSE:"], read  # not "pred:" / "outcome:"

print("AUDIT_BOTCORE_OK")
