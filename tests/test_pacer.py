"""The API key's request pacer (main.PredictPacer, 1.154.0): reads wait for a token, orders and cancels may borrow a burst ahead of
them, one pacer a key whose pace follows PREDICT_RATE_PER_MIN; the pre-open mode's own key (PREOPEN_API_KEY); its listing
probes fast only near the moment Predict has been listing. Entirely offline."""
import asyncio
import sys
import time
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import offline  # noqa: F401
import main as m


async def run() -> None:
    m.PREDICT_PACING = True  # the suites run unpaced (offline.py); this one is about the pacing
    try:
        pacer = m.PredictPacer(6000)  # 100 a second: the waits stay short
        started = time.monotonic()
        for _ in range(int(m.PredictPacer.BURST)):
            await pacer.take()
        assert time.monotonic() - started < 0.05, "a burst goes at once"
        await pacer.take()
        took = time.monotonic() - started
        assert 0.005 <= took < 0.1, f"the next read waits for its token ({took:.3f} s)"
        # orders and cancels borrow ahead of the reads: the bucket empty, they still go at once — a burst's worth
        started = time.monotonic()
        for _ in range(int(m.PredictPacer.BURST)):
            await pacer.take(write=True)
        assert time.monotonic() - started < 0.05, "writes never queue behind the reads"
        assert pacer.tokens < 0
        started = time.monotonic()
        await pacer.take()
        assert time.monotonic() - started >= 0.07, "a read pays the writes' debt back first"
        # 1.154.1: a patient client's orders (the pre-open mode's placements) wait like a read; a taker's or a cancel does not
        import live as L
        m.PREDICT_PACERS.clear()
        m.PREDICT_RATE["per_min"] = 600.0  # 10 a second
        sent = []

        async def send(self, method, path, payload, timeout):
            sent.append((self.patient, time.monotonic()))
            return b"{}"
        urgent, patient = L.PredictApi("https://x", "kp", None), L.PredictApi("https://x", "kp", None)
        patient.patient = True
        original = L.PredictApi.send
        L.PredictApi.send = send
        try:
            for _ in range(int(m.PredictPacer.BURST)):
                await urgent.call("GET", "/a")  # the bucket emptied by reads
            started = time.monotonic()
            await urgent.call("POST", "/orders", {}, priority=True)
            assert time.monotonic() - started < 0.05, "a taker's order borrows: at once"
            started = time.monotonic()
            await patient.call("POST", "/orders", {}, priority=True)
            assert time.monotonic() - started >= 0.08, "a placement made ahead of time waits for the pace"
        finally:
            L.PredictApi.send = original
        # off: nothing waits
        assert await asyncio.wait_for(m.PredictPacer(0).take(), 0.01) is None
        # one pacer a key, its pace PREDICT_RATE_PER_MIN as configured
        m.PREDICT_PACERS.clear()
        m.PREDICT_RATE["per_min"] = 120.0
        a, b = m.predict_pacer("k1"), m.predict_pacer("k2")
        assert a is m.predict_pacer("k1") and a is not b and a.per_min == 120.0
        cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREDICT_RATE_PER_MIN": "200", "PREOPEN_API_KEY": " k2 "})
        assert (cfg.predict_rate_per_min, cfg.preopen_api_key) == (200.0, "k2")
        m.PredictFeed(cfg)
        assert m.predict_pacer("k1").per_min == 200.0, "the feed's start sets every key's pace"
        assert m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline"}).predict_rate_per_min == 220.0, "under Predict's 240 by default"
    finally:
        m.PREDICT_PACING = False
        m.PREDICT_PACERS.clear()
        m.PREDICT_RATE["per_min"] = 220.0
    # the pre-open mode on a key of its own: its three clients on it, one cooldown among themselves, none with the bot's client
    import preopen as p
    from test_preopen import controller
    ctl, _, _ = controller(PREOPEN_API_KEY="second-key", PREDICT_API_KEY="main-key")
    own = p.Controller(ctl.owner)
    assert {own.api.api_key, own.cancel_api.api_key, own.bulk_api.api_key} == {"second-key"}
    assert (own.api.patient, own.cancel_api.patient, own.bulk_api.patient) == (True, False, False), "its placements wait, its cancels do not"
    own.api.blocked_until = time.monotonic() + 30
    assert own.cancel_api.blocked_until > time.monotonic() and ctl.owner.live.api.blocked_until == 0.0, "its 429 pauses its own clients alone"
    plain = p.Controller(controller(PREDICT_API_KEY="main-key")[0].owner)
    assert plain.api.api_key == "main-key"
    # the listing probes: fast only from SPRINT_EARLY_MS before the soonest recent listing delay (none known: from the entry)
    now = own.now()
    assert own.sprint_lead_ms() == 0
    own.listing_lags.extend([84_000, 86_000, 90_000, 85_000, 88_000])
    assert own.sprint_lead_ms() == 84_000 - p.SPRINT_EARLY_MS
    own.listed_at.clear()
    own.awaiting = {"BTC-15M": now - 30_000}
    assert own.poll_wait() != p.FAST_POLL_S, "30 s after the entry: Predict lists some 85 s on — no sprint yet"
    own.awaiting = {"BTC-15M": now - 70_000}
    assert own.poll_wait() == p.FAST_POLL_S, "15 s before the soonest delay: the sprint"
    assert "进入范围到发布中位 86 秒（最早 84；5 次），进入范围 69 秒后开始每 0.5 秒探测" in own.timing_line(), own.timing_line()


    # 1.154.2: a backlog's reads — one market read for all the levels of a window far from its start (not one a level), a fresh one
    # near it; and the known windows not read again while a long queue waits (their details would be stale by their turn)
    from test_preopen import NOW, market
    ctl, api, clock = controller(PREOPEN_LEVELS="48x20,47x40", PREOPEN_AHEAD_MINUTES="1440")
    reads = []
    plain_market = api.market

    async def counted(mid):
        reads.append(str(mid))
        return await plain_market(mid)
    api.market = counted
    for mid, ahead_ms in ((321, 5 * 3_600_000), (322, 30 * 60_000)):
        mk = market(at=NOW + ahead_ms, mid=mid)
        api.markets[str(mid)] = mk
        ctl.keep_details(str(mid), mk)
    clock.ms = NOW + 60_000  # a minute on: past MARKET_FRESH_MS, within MARKET_REUSE_MS
    ctl.account_at = clock.ms
    for mid in ("321", "322"):
        c = p.candidate(api.markets[mid], clock.ms, ctl.owner.config, ctl.settings)
        assert c is not None, mid
        for level, (price, shares) in enumerate(ctl.settings.levels):
            key = c["slug"] if level == 0 else f"{c['slug']}#{level}"
            await ctl.place_pair({**c, "slug": key, "window": c["slug"], "level": level, "price": str(price), "shares": str(shares),
                                  "attempt": 0, "market": api.markets[mid], "read_ms": NOW})
    assert reads.count("321") == 0, ("five hours out: its three levels on the one read", reads)
    assert reads.count("322") == 1, ("half an hour out: read afresh once, the others on it", reads)
    assert len([k for k in ctl.ledger.rows if k.startswith("btc-updown-15m-")]) == 6, "all six pairs placed"
    asked = []

    async def reread(*args):
        asked.append(1)
    ctl.reread_known = reread
    ctl.queued = {f"x{i}" for i in range(p.REREAD_QUEUE_MAX + 1)}
    ctl.rescan_ms = 0
    await ctl.discover()
    assert asked == [], "a long queue: the known windows not read again"
    ctl.queued = set()
    await ctl.discover()
    assert asked == [1], "the queue short again: read"


asyncio.run(run())
print("PACER_OK")
