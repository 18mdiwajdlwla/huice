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
        # 1.154.3: a new pacer starts empty — a process just started does not know how much of Predict's minute the one before it
        # used (a redeploy: the old process's last requests still count there): its first request keeps the pace; the burst is
        # there once the bucket has filled
        assert pacer.tokens == 0
        started = time.monotonic()
        await pacer.take()
        assert time.monotonic() - started >= 0.008, "the first request waits for its token"
        await asyncio.sleep(m.PredictPacer.BURST / 100 + 0.03)  # the bucket fills
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
            assert m.predict_pacer("kp").tokens == 0  # the bucket empty (a new pacer)
            started = time.monotonic()
            await urgent.call("POST", "/orders", {}, priority=True)
            assert time.monotonic() - started < 0.05, "a taker's order borrows: at once"
            started = time.monotonic()
            await patient.call("POST", "/orders", {}, priority=True)
            assert time.monotonic() - started >= 0.08, "a placement made ahead of time waits for the pace"
            # 1.154.11: a low client (the pre-open mode's placements and re-reads) yields to a plain one on the key (its account snapshot)
            m.PREDICT_PACERS.clear()
            yielding, snapshot, done = L.PredictApi("https://x", "kq", None), L.PredictApi("https://x", "kq", None), []
            yielding.low = yielding.patient = True

            async def one(client, name, method, priority):
                await client.call(method, "/x", {} if method == "POST" else None, priority=priority)
                done.append(name)
            await asyncio.gather(one(yielding, "placement", "POST", True), one(yielding, "re-read", "GET", False),
                                 one(snapshot, "snapshot page", "GET", False))
            assert done[0] == "snapshot page", done
        finally:
            L.PredictApi.send = original
        # 1.154.9: a low read (the orderbook feed's: dozens a round) leaves LOW_RESERVE tokens behind — the trader's read, asked after
        # it, goes first (11-17 14:41: the trader's sync queued 22 s behind the book rounds on a key at 220 a minute)
        low_pacer, order = m.PredictPacer(600), []  # 10 a second, empty

        async def reader(name, low):
            await low_pacer.take(low=low)
            order.append(name)
        await asyncio.gather(reader("feed", True), reader("trader", False))
        assert order == ["trader", "feed"], order
        # off: nothing waits
        assert await asyncio.wait_for(m.PredictPacer(0).take(), 0.01) is None
        # 1.154.3: what a key let through in the last 60 seconds, every client's — counted whether paced or not (/diag)
        counter = m.PredictPacer(0)
        for _ in range(5):
            await counter.take()
        assert counter.last_minute() == 5
        counter.sent[0] = counter.sent[0] - 61  # a minute and a second ago: out of it
        assert counter.last_minute() == 4
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
        # /diag (1.154.3): each key's minute by name — never the key itself; the pre-open mode's own key only when it is another one
        m.PREDICT_PACERS.clear()
        for key, n in (("main-key", 3), ("second-key", 7)):
            for _ in range(n):
                m.predict_pacer(key).sent.append(time.monotonic())
        ns = types.SimpleNamespace(config=types.SimpleNamespace(predict_api_key="main-key", preopen_api_key="second-key"))
        ns.preopen_own_key = lambda: L.LiveBot.preopen_own_key(ns)
        ns.book_own_key = lambda: L.LiveBot.book_own_key(ns)
        line = L.LiveBot.live_key_load(ns)
        assert line.startswith("主 Key 3 次、预挂专用 Key 7 次（节流每分钟 200 次") and "main-key" not in line and "second-key" not in line, line
        ns.config.preopen_api_key = "main-key"  # set to the bot's own key: one key, one figure
        assert L.LiveBot.live_key_load(ns).startswith("主 Key 3 次（"), L.LiveBot.live_key_load(ns)
        # 1.154.5: the orderbook feed's own key (PREDICT_BOOK_API_KEY), and each 429 with the count the key had let through in the minute
        # before it (11-17: seven on the bot's key in nine minutes at this pace — is Predict counting as the pacer does?)
        ns.config.preopen_api_key, ns.config.predict_book_api_key = "second-key", "book-key"
        for _ in range(5):
            m.predict_pacer("book-key").sent.append(time.monotonic())
        main_pacer = m.predict_pacer("main-key")
        main_pacer.note_429()
        main_pacer.sent.extend([time.monotonic()] * 4)
        main_pacer.note_429()
        ns.KEY_SHARED_BELOW = L.LiveBot.KEY_SHARED_BELOW
        line = L.LiveBot.live_key_load(ns)
        # 1.154.6: every 429 with the bot far under Predict's 240 — someone else's requests filled that minute (11-17: a key shared with friends)
        assert line.startswith("主 Key 7 次（24 小时内 429 2 次，当时前 60 秒 3～7 次、中位 5 次，远低于 240：这个 Key 可能还有别的程序或别人在用）、"
                               "预挂专用 Key 7 次、盘口专用 Key 5 次（节流每分钟 "), line
        assert "book-key" not in line
        # 1.154.11: judged by the median — one 429 at the bot's own full pace (11-17 17:24: 90 at 135～211) keeps the note
        main_pacer.limits.append((time.monotonic(), 231))
        assert "3～231 次、中位 7 次，远低于 240：这个 Key 可能还有别的程序" in L.LiveBot.live_key_load(ns), L.LiveBot.live_key_load(ns)
        main_pacer.limits.extend([(time.monotonic(), 232), (time.monotonic(), 233)])  # most at the bot's own full pace: nothing to say
        assert "可能还有别的程序" not in L.LiveBot.live_key_load(ns) and "3～233 次、中位 231 次）" in L.LiveBot.live_key_load(ns), L.LiveBot.live_key_load(ns)
        feed_cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREDICT_API_KEY": "main-key", "PREDICT_BOOK_API_KEY": " book-key "})
        book_feed = m.PredictFeed(feed_cfg)
        assert (feed_cfg.predict_book_api_key, book_feed.api_key(), book_feed.headers()) == ("book-key", "book-key", {"x-api-key": "book-key"})
        assert m.PredictFeed(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREDICT_API_KEY": "main-key"})).headers() == {"x-api-key": "main-key"}
        paced = []
        real_pacer = m.predict_pacer

        def watching(key):
            paced.append(key)
            return real_pacer(key)
        real_blocking, m.predict_pacer = m.predict_blocking, watching

        async def answered(fn, *args):
            return b'{"data": {"bids": [], "asks": []}}'
        m.predict_blocking = answered
        try:
            await book_feed.fetch(m.PREDICT_REST + "/markets/1/orderbook")
        finally:
            m.predict_pacer, m.predict_blocking = real_pacer, real_blocking
        assert paced == ["book-key"], ("the feed's reads on its own key's budget", paced)
        ns.config.predict_book_api_key = ""
        # 1.154.12: the rule taker's own key (RULE_API_KEY) has its line too
        ns.config.rule_api_key = "rule-key"
        for _ in range(2):
            m.predict_pacer("rule-key").sent.append(time.monotonic())
        assert "、规则吃单专用 Key 2 次（节流每分钟 " in L.LiveBot.live_key_load(ns) and "rule-key" not in L.LiveBot.live_key_load(ns), L.LiveBot.live_key_load(ns)
        ns.config.rule_api_key = "main-key"  # the bot's own key again: no line of its own
        assert "规则吃单专用 Key" not in L.LiveBot.live_key_load(ns)
        ns.config.rule_api_key = ""
        assert m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "RULE_API_KEY": " rk "}).rule_api_key == "rk"
        # the clients' rates: the pre-open mode's said to be on its own key
        ns.config.preopen_api_key, ns.live_rate_samples, ns.predict = "second-key", [], None
        ns.preopen = types.SimpleNamespace(request_rate=lambda: 2.8)
        assert L.LiveBot.live_request_rates(ns) == ["预挂 2.8（专用 Key）"], L.LiveBot.live_request_rates(ns)
        # the 429s: the rule taker's own client too, once it is on (or has one to tell)
        ns.live, ns.preopen = types.SimpleNamespace(api=types.SimpleNamespace(limited=[])), None
        ns.rules = types.SimpleNamespace(api=types.SimpleNamespace(limited=[time.monotonic() - 10]), settings=types.SimpleNamespace(mode="paper"))
        assert L.LiveBot.live_limited_counts(ns) == "原策略 0 次、规则吃单 1 次", L.LiveBot.live_limited_counts(ns)
        ns.rules.settings.mode, ns.rules.api.limited = "off", []
        assert L.LiveBot.live_limited_counts(ns) == "原策略 0 次", "off with none: not listed"
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
    # 1.154.11: the account snapshot on a client of its own on that key, its reads ahead of the mode's others (the placement client's
    # reads and patient orders leave the pacer's reserve: low); its cooldown the same as theirs
    assert own.snap_api.api_key == "second-key" and own.snapshot_client() is own.snap_api and own.snap_api.ORDERS_PAGE == own.api.ORDERS_PAGE
    assert (own.api.low, own.cancel_api.low, own.bulk_api.low, own.snap_api.low) == (True, False, False, False)
    assert own.snap_api.blocked_until > time.monotonic(), "one cooldown for the mode's clients"
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
