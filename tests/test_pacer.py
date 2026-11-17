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


asyncio.run(run())
print("PACER_OK")
