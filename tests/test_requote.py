"""1.154.54 — a changed setting no longer empties the pre-open book for hours.

11-26 09:03: the cancel time set from 20 to 10 seconds before the start, and with it (the expiry must be 5 seconds nearer
the start than the cancel) the signed expiry from 10 to 5. The user, twenty minutes on: "我改了撤单时间，会多久开始重新挂上
——目前过了 20 分钟，还没挂上", with two /preopen reports 17 minutes apart: 35 pairs resting, 已发布未挂 1397 → 1373 场 (some
9,700 levels), 已结清 32,803 → 34,409, and 扫描 standing at 09:06:14 in both.

What had happened: (1) the expiry is in an order's signature, so not one resting pair was "as the settings say" and all
9,400 were withdrawn to be signed again — every listed window of the next day, each placed at its listing, first in the
queue at its price; (2) each withdrawn order is read once more a minute after it left the book (Audit PRE-LATE-FILL): 18,700
reads at the key's 220 a minute; (3) those reads went out on the cancel client, each taking the key's next token the moment
it came, and the mode's low traffic — the re-reads of the known windows, the backlog's placements — never found the three
tokens it waits for: the discovery stood in one re-read for an hour and a half (the same loop probes the listings: none
was found), the placement workers in theirs.

Here: a pair signed to leave the book EARLIER than the settings now say rests on, its orders and its queue position
untouched, withdrawn at the new cancel time or its own signed expiry, whichever is first — one signed to stay LONGER is
withdrawn as before; the late final reads go out as the mode's low traffic does (on the cancel client again once the
window is long over, and whenever a read must say whether an order still rests), and wait while a pair is being placed
or a known window re-read; a re-read that gets no answer ends its pass, and the next pass finds the new listing all the
same; /preopen says how many withdrawn pairs wait for their reads, at what pace, and which resting pairs keep an earlier
expiry. Entirely offline.

Run: python tests/test_requote.py
"""
import asyncio, pathlib, sys, tempfile, time
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import preopen as p
from test_preopen import NOW, Api, controller, market

MIN = 60_000


async def closed(ctl) -> None:
    for task in ctl.settling.values():
        task.cancel()
    await asyncio.gather(*ctl.settling.values(), return_exceptions=True)
    ctl.ledger.close()


async def placed(folder, name, rows, **env):
    """A controller on a file of its own with every window of ``rows`` quoted (its first level and one deeper), stopped as a
    settings change stops it: the orders stay on the book. (the file's settings, the api the next controller takes over)."""
    db = {"STATE_DB": str(pathlib.Path(folder) / f"{name}.sqlite3"), "PREOPEN_MARKETS": "btc,btc5", "PREOPEN_AHEAD_MINUTES": "240", "PREOPEN_LEVELS": "43x60"}
    ctl, api, clock = controller(rows, **db, **env)
    for mk in rows:
        c = p.candidate(mk, NOW, ctl.owner.config, ctl.settings)
        for level, (price, shares) in enumerate(ctl.settings.levels):
            await ctl.place_pair({**c, "slug": c["slug"] if level == 0 else f"{c['slug']}#{level}", "window": c["slug"], "level": level,
                                  "price": str(price), "shares": str(shares), "attempt": 0, "market": mk, "read_ms": NOW})
    assert len(ctl.ledger.rows) == 2 * len(rows) and all(x["phase"] == "active" for x in ctl.ledger.rows.values())
    ctl.keep_on_stop = True
    await ctl.on_stop()
    ctl.ledger.close()
    return db, api


def taken_over(db, api, rows, **env):
    ctl, _, clock = controller(rows, **db, **env)
    ctl.api = ctl.cancel_api = api
    api.ctl = ctl
    ctl.adopt()
    return ctl, clock


async def an_earlier_expiry_rests_on() -> None:
    rows = [market(at=NOW + 30 * MIN, mid=7), market(at=NOW + 45 * MIN, mid=8), market(at=NOW + 20 * MIN, mid=9, minutes=5)]
    with tempfile.TemporaryDirectory() as folder:
        # --- the morning of 11-26: 20 / 10 seconds as the pairs were signed, then 10 / 5
        db, api = await placed(folder, "later", rows)
        ids = sorted(o["id"] for x in api.rows.values() for o in [x])
        ctl, clock = taken_over(db, api, rows, PREOPEN_CANCEL_SECONDS="10", PREOPEN_EXPIRY_SECONDS="5")
        pairs = list(ctl.ledger.rows.values())
        assert ctl.resumed == 6 and all(x["phase"] == "active" for x in pairs) and api.removed == [], "pairs signed to expire earlier were withdrawn"
        assert sorted(o["id"] for x in pairs for o in x["orders"].values()) == sorted(r["id"] for r in api.rows.values()), "their orders as they were: the queue position"
        for x in pairs:
            assert (x["start_ms"] // 1000 - x["expires"], x["start_ms"] - x["cancel_ms"]) == (10, 10_000), "signed for 10 s before the start: withdrawn at the new cancel time, which is that"
        assert not ctl.due(clock.ms), "nothing to do for them now"
        text = ctl.status()
        assert "撤单：开场前 10 秒发出，之后到开场前 5 秒到期为止" in text
        assert "（6 条挂着，0 条已结清）；挂着的里面 6 条是改设置前签的，到期时间签在单里改不了：它们最晚在原来签的到期时间离场（比现在的设置早），排队位置保留，新挂的按现在的设置" in text, text
        # a window quoted from now on is signed as the settings say now
        new = market(at=NOW + 60 * MIN, mid=10)
        api.markets["10"] = new
        await ctl.place_pair(p.candidate(new, NOW, ctl.owner.config, ctl.settings))
        fresh = ctl.ledger.rows[new["categorySlug"]]
        assert (fresh["start_ms"] // 1000 - fresh["expires"], fresh["start_ms"] - fresh["cancel_ms"]) == (5, 10_000)
        assert "挂着的里面 6 条是改设置前签的" in ctl.status(), "the new pair is none of them"
        # ...at its time each is withdrawn like any other
        first = min(pairs, key=lambda x: x["cancel_ms"])
        clock.ms = first["cancel_ms"]
        assert [x["slug"] for x in ctl.due(clock.ms)] == sorted(x["slug"] for x in pairs if x["cancel_ms"] == first["cancel_ms"])
        await closed(ctl)

        # --- signed to leave the book before the new cancel time (60 / 30, then 10 / 5): withdrawn at its own expiry, not after
        db, api = await placed(folder, "own", rows, PREOPEN_CANCEL_SECONDS="60", PREOPEN_EXPIRY_SECONDS="30")
        ctl, clock = taken_over(db, api, rows, PREOPEN_CANCEL_SECONDS="10", PREOPEN_EXPIRY_SECONDS="5")
        assert ctl.resumed == 6 and api.removed == []
        assert all((x["start_ms"] // 1000 - x["expires"], x["start_ms"] - x["cancel_ms"]) == (30, 30_000) for x in ctl.ledger.rows.values())
        await closed(ctl)

        # --- only the cancel time changed (20 → 12, the expiry as signed): taken over as ever, the new cancel time
        db, api = await placed(folder, "cancel", rows)
        ctl, clock = taken_over(db, api, rows, PREOPEN_CANCEL_SECONDS="15")
        assert ctl.resumed == 6 and api.removed == [] and all(x["start_ms"] - x["cancel_ms"] == 15_000 for x in ctl.ledger.rows.values())
        assert "改设置前签的" not in ctl.status()
        await closed(ctl)

        # --- the expiry moved away from the start (10 → 20): the pairs would outstay it — withdrawn and quoted again, as before
        db, api = await placed(folder, "sooner", rows)
        ctl, clock = taken_over(db, api, rows, PREOPEN_CANCEL_SECONDS="30", PREOPEN_EXPIRY_SECONDS="20")
        assert ctl.resumed == 0 and all((x["phase"], x.get("free"), x.get("reason")) == ("closing", True, "设置不同，撤掉后重新报价") for x in ctl.ledger.rows.values())
        await closed(ctl)

        # --- BTC 5-minute's own expiry nearer the start: its pairs rest on too, the others' untouched
        db, api = await placed(folder, "five", rows)
        ctl, clock = taken_over(db, api, rows, PREOPEN_BTC5_CANCEL_SECONDS="10", PREOPEN_BTC5_EXPIRY_SECONDS="5")
        assert ctl.resumed == 6 and api.removed == []
        gaps = {x["key"]: x["start_ms"] - x["cancel_ms"] for x in ctl.ledger.rows.values()}
        assert gaps == {"BTC-15M": 20_000, "BTC-5M": 10_000}, gaps
        assert "挂着的里面 2 条是改设置前签的" in ctl.status(), "BTC 5-minute's two, by its own expiry"
        await closed(ctl)

        # --- a price changed: withdrawn whatever the expiry (not what the settings say at this level)
        db, api = await placed(folder, "price", rows)
        ctl, clock = taken_over(db, api, rows, PREOPEN_PRICE_CENTS="48", PREOPEN_CANCEL_SECONDS="10", PREOPEN_EXPIRY_SECONDS="5")
        states = sorted((int(x.get("level") or 0), x["phase"]) for x in ctl.ledger.rows.values())
        assert states == [(0, "closing")] * 3 + [(1, "active")] * 3 and ctl.resumed == 3, states
        await closed(ctl)
    print("AN_EARLIER_EXPIRY_RESTS_ON_OK")


class Counted(Api):
    """The offline api, its order reads counted."""
    def __init__(self, ctl, markets, name, asked):
        super().__init__(ctl, markets)
        self.name, self.asked = name, asked

    async def order(self, oid, digest=""):
        self.asked.append(self.name)
        return await super().order(oid, digest)


async def the_late_reads_behind_the_placements() -> None:
    """The read LATE_FILL_MS after a withdrawal goes out on the mode's low client (the api: behind the listings' probes and
    orders, beside the backlog's placements), not on the cancel client — but there again once the window is long over, and
    whenever a read must say whether an order still rests."""
    rows = [market(at=NOW + 30 * MIN, mid=7), market(at=NOW + 45 * MIN, mid=8)]
    ctl, api, clock = controller(rows, PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="240")
    asked: list[str] = []
    low, cancel = Counted(ctl, rows, "low", asked), Counted(ctl, rows, "cancel", asked)
    cancel.rows = low.rows  # (one Predict behind both clients)
    ctl.api = ctl.api_made = low  # the mode's own low client, as the controller makes it (not a test's stand-in for every client)
    ctl.cancel_api = cancel
    assert ctl.first() is ctl.first_api, "the made api: the listings' client is its own"
    ctl.first_api = low  # (...which here is the offline one too)
    for mk in rows:
        await ctl.place_pair(p.candidate(mk, NOW, ctl.owner.config, ctl.settings))
    a, b = sorted(ctl.ledger.rows.values(), key=lambda x: x["start_ms"])
    await ctl.cancel_many([a, b], "设置不同，撤掉后重新报价", free=True)
    assert {o["state"] for x in (a, b) for o in x["orders"].values()} == {"removed"} and asked == [], "a confirmed removal is not read right after (it could not end the order)"
    assert not a.get("accounted") and ctl.finals_wait(a) == clock.ms + p.LATE_FILL_MS
    # a minute on: the final reads — on the low client, both orders of the pair; released
    clock.ms += p.LATE_FILL_MS
    ctl.book_due([a])
    await asyncio.gather(*ctl.settling.values())
    assert asked == ["low", "low"] and a["accounted"] and a["phase"] == "closed" and list(ctl.released_at) == [clock.ms], (asked, a["phase"])
    # ...the other pair's, read only once its window is long over (the result's booking waits on it): the cancel client, as ever
    del asked[:]
    clock.ms = b["start_ms"] + p.GIVE_UP_MS
    ctl.book_due([b])
    await asyncio.gather(*ctl.settling.values())
    assert asked == ["cancel", "cancel"] and b["accounted"], asked
    # an order a removal did not remove: what its record says now is asked on the cancel client (does it rest on?)
    third = market(at=NOW + 60 * MIN, mid=9)
    low.markets["9"] = cancel.markets["9"] = third
    clock.ms = NOW
    ctl.account_at = clock.ms
    await ctl.place_pair(p.candidate(third, NOW, ctl.owner.config, ctl.settings))
    c = ctl.ledger.rows[third["categorySlug"]]
    cancel.stuck = low.stuck = {o["id"] for o in c["orders"].values()}
    del asked[:]
    await ctl.cancel_pair(c, "撤单状态核对")
    assert asked == ["cancel", "cancel"] and {o["state"] for o in c["orders"].values()} == {"open"}, (asked, c["orders"])
    # a test's own api (one stand-in for every client, as the other suites use it): read where it always was
    ctl2, api2, clock2 = controller([market(at=NOW + 30 * MIN, mid=7)], PREOPEN_MARKETS="btc")
    asked2: list[str] = []
    one = Counted(ctl2, [market(at=NOW + 30 * MIN, mid=7)], "one", asked2)
    ctl2.api = ctl2.cancel_api = one
    await ctl2.place_pair(p.candidate(market(at=NOW + 30 * MIN, mid=7), NOW, ctl2.owner.config, ctl2.settings))
    d = next(iter(ctl2.ledger.rows.values()))
    await ctl2.cancel_many([d], "设置不同，撤掉后重新报价", free=True)
    clock2.ms += p.LATE_FILL_MS
    ctl2.book_due([d])
    await asyncio.gather(*ctl2.settling.values())
    assert asked2 == ["one", "one"] and d["accounted"]
    await closed(ctl)
    await closed(ctl2)
    print("THE_LATE_READS_BEHIND_THE_PLACEMENTS_OK")


async def the_late_reads_give_way() -> None:
    """The key's pace gives no turns (whoever asks at the right instant has the token: two streams of reads can keep a third
    reader waiting for seconds), so the order is kept by the mode itself: a late final read waits while a pair is being
    placed or a known window re-read, goes out when neither is, and after FINALS_YIELD_S whatever holds them up."""
    rows = [market(at=NOW + 30 * MIN, mid=7)]
    ctl, api, clock = controller(rows, PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="240")
    asked: list[str] = []
    one = Counted(ctl, rows, "one", asked)
    ctl.api = ctl.cancel_api = one
    await ctl.place_pair(p.candidate(rows[0], NOW, ctl.owner.config, ctl.settings))
    a = next(iter(ctl.ledger.rows.values()))
    await ctl.cancel_many([a], "设置不同，撤掉后重新报价", free=True)
    clock.ms += p.LATE_FILL_MS
    was = p.FINALS_YIELD_S, p.FINALS_YIELD_STEP_S
    p.FINALS_YIELD_S, p.FINALS_YIELD_STEP_S = 30.0, 0.005
    try:
        # a pair is being placed: the read waits — and goes out the moment it is placed
        ctl.placing.add("btc-updown-15m-being-placed")
        ctl.book_due([a])
        await asyncio.sleep(0.1)
        assert asked == [] and not a.get("accounted"), "a late final read went out while a pair was being placed"
        ctl.placing.clear()
        ctl.rereading = True  # ...a known window is being re-read (what feeds the placements): it waits for that too
        await asyncio.sleep(0.1)
        assert asked == []
        ctl.rereading = False
        await asyncio.gather(*ctl.settling.values())
        assert asked == ["one", "one"] and a["accounted"]
        # ...and whatever holds a placement up, no longer than FINALS_YIELD_S: the reads go on
        b_market = market(at=NOW + 45 * MIN, mid=8)
        one.markets["8"] = b_market
        clock.ms = NOW
        ctl.account_at = clock.ms
        await ctl.place_pair(p.candidate(b_market, NOW, ctl.owner.config, ctl.settings))
        b = ctl.ledger.rows[b_market["categorySlug"]]
        await ctl.cancel_many([b], "设置不同，撤掉后重新报价", free=True)
        clock.ms += p.LATE_FILL_MS
        del asked[:]
        p.FINALS_YIELD_S = 0.05
        ctl.placing.add("btc-updown-15m-stuck")
        began = time.monotonic()
        ctl.book_due([b])
        await asyncio.wait_for(asyncio.gather(*ctl.settling.values()), 5.0)
        assert asked == ["one", "one"] and b["accounted"] and time.monotonic() - began >= 0.09, (asked, time.monotonic() - began)  # (each of the two reads waited its 0.05 s)
        ctl.placing.clear()
        # a read that says whether an order still rests waits for nothing
        c_market = market(at=NOW + 60 * MIN, mid=9)
        one.markets["9"] = c_market
        clock.ms = NOW
        ctl.account_at = clock.ms
        await ctl.place_pair(p.candidate(c_market, NOW, ctl.owner.config, ctl.settings))
        c = ctl.ledger.rows[c_market["categorySlug"]]
        one.stuck = {o["id"] for o in c["orders"].values()}
        p.FINALS_YIELD_S = 30.0
        ctl.placing.add("btc-updown-15m-being-placed")
        del asked[:]
        await asyncio.wait_for(ctl.cancel_pair(c, "撤单状态核对"), 5.0)
        assert asked == ["one", "one"]
    finally:
        p.FINALS_YIELD_S, p.FINALS_YIELD_STEP_S = was
    await closed(ctl)
    print("THE_LATE_READS_GIVE_WAY_OK")


async def a_re_read_never_holds_the_probes() -> None:
    """A known window's re-read that gets no answer within REREAD_WAIT_S ends the pass — said, no error of the mode's — and
    the next pass probes the listings all the same: the new one is found and queued while the re-read still hangs."""
    rows = [market(at=NOW + n * 15 * MIN, mid=n) for n in range(1, 5)]
    ctl, api, clock = controller(rows, PREOPEN_AHEAD_MINUTES="240", PREOPEN_MARKETS="btc")
    ctl.listed_by_card["BTC-15M"] = NOW + 3 * 15 * MIN  # three known listed, none quoted: to re-read; the fourth is the next listing
    api.unlisted = {rows[3]["categorySlug"]}
    hung = rows[0]["categorySlug"]  # the soonest: the first re-read
    state = {"asked": 0, "given_up": 0}
    real = api.call

    async def call(method, path, **kw):
        if path.endswith(hung):
            state["asked"] += 1
            try:
                await asyncio.Event().wait()  # (a token that never comes)
            except asyncio.CancelledError:
                state["given_up"] += 1
                raise
        return await real(method, path, **kw)
    api.call = call
    was = p.REREAD_WAIT_S
    p.REREAD_WAIT_S = 0.05
    try:
        began = time.monotonic()
        await ctl.discover()
        took = time.monotonic() - began
        await asyncio.sleep(0)  # (the read given up hears of it at the loop's next turn)
        assert took < 2.0 and state == {"asked": 1, "given_up": 1}, (took, dict(state))
        assert ctl.scan_note.endswith(f"；停在 {hung.rsplit('-', 1)[-1]}：0.05 秒没等到（请求额度紧），下次再读") and not ctl.error, (ctl.scan_note, ctl.error)
        assert ctl.queue.qsize() == 0 and not ctl.queued, "nothing found: nothing queued, nothing marked"
        # the next listing appears; the re-read hangs again — the pass before it has found and queued the listing
        api.unlisted = set()
        clock.ms += 6_000
        ctl.account_at = clock.ms
        await ctl.discover()
        await asyncio.sleep(0)
        queued = [ctl.queue.get_nowait()[2] for _ in range(ctl.queue.qsize())]
        assert [(c["slug"], c["fresh"]) for c in queued] == [(rows[3]["categorySlug"], True)] and state == {"asked": 2, "given_up": 2}, (queued, dict(state))
        assert ctl.listed_by_card["BTC-15M"] == NOW + 4 * 15 * MIN
        # the key's pace back: the re-reads as ever
        api.call = real
        ctl.queued.clear()
        clock.ms += 6_000
        ctl.account_at = clock.ms
        await ctl.discover()
        # (the three, and the fourth — taken off the queue above without being placed: a known window with a level to quote, like them)
        assert sorted(ctl.queued) == sorted(r["categorySlug"] for r in rows) and "停在" not in ctl.scan_note, (ctl.queued, ctl.scan_note)
    finally:
        p.REREAD_WAIT_S = was
    await closed(ctl)
    print("A_RE_READ_NEVER_HOLDS_THE_PROBES_OK")


async def the_status_says_what_waits() -> None:
    """/preopen: how many withdrawn pairs wait for their final reads, how many were released in the last minute and how long
    the rest takes at that pace — from FINALS_SAID waiting on."""
    rows = [market(at=NOW + (n + 2) * 15 * MIN, mid=n) for n in range(1, 31)]
    ctl, api, clock = controller(rows, PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="1440")
    for mk in rows:
        await ctl.place_pair(p.candidate(mk, NOW, ctl.owner.config, ctl.settings))
    pairs = sorted(ctl.ledger.rows.values(), key=lambda x: x["start_ms"])
    assert ctl.finals_line() == "" and "撤后待核对" not in ctl.status()
    await ctl.cancel_many(pairs, "设置不同，撤掉后重新报价", free=True)
    assert ctl.finals_line() == ("\n撤后待核对：30 条（撤掉的单要在撤后 60 秒逐张读一次成交，确认没成交才释放、才能重新挂；离开场近的先读，读完一场重挂一场，"
                                 "新发布的场次不受影响；最近一分钟还没有核对完的）"), ctl.finals_line()
    assert ctl.finals_line() in ctl.status()
    clock.ms += p.LATE_FILL_MS
    assert [x["slug"] for x in ctl.due(clock.ms)] == [x["slug"] for x in pairs], "the soonest window's reads first"
    ctl.book_due(pairs[:6])
    await asyncio.gather(*ctl.settling.values())
    assert "撤后待核对：24 条（" in ctl.finals_line() and "最近一分钟核对完 6 条，照这个速度还要约 4 分钟）" in ctl.finals_line(), ctl.finals_line()
    clock.ms += 61_000  # ...a minute with none: said so, no pace made up
    assert "撤后待核对：24 条（" in ctl.finals_line() and "最近一分钟还没有核对完的" in ctl.finals_line(), ctl.finals_line()
    # the pairs still waiting for their turn (two reconciles at a time): looked at again at their next try, but not booked and
    # not written again — nothing of them has changed
    writes = []
    real = ctl.ledger.save_many
    ctl.ledger.save_many = lambda rows: (writes.append(len(rows)), real(rows))[1]
    held = asyncio.Event()
    read_final = ctl.read_final

    async def gated(order, pair, late=False):
        await held.wait()
        await read_final(order, pair, late)
    ctl.read_final = gated
    ctl.book_due(pairs[6:12])
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert writes == [6] and sum(1 for task in ctl.settling.values() if not task.done()) == 6, writes
    tries = [x["tries"] for x in pairs[6:12]]
    clock.ms += p.RETRY_MAX_MS
    assert pairs[6] in ctl.due(clock.ms)
    ctl.book_due(pairs[6:12])
    assert writes == [6, 0] and [x["tries"] for x in pairs[6:12]] == tries and pairs[6] not in ctl.due(clock.ms), (writes, tries)
    assert pairs[6]["retry_ms"] == clock.ms + p.RETRY_MAX_MS
    ctl.ledger.save_many = real
    held.set()
    await asyncio.gather(*ctl.settling.values())
    assert all(x["accounted"] for x in pairs[:12]) and ctl.finals_line() == "", "under FINALS_SAID waiting: the line goes"
    await closed(ctl)
    print("THE_STATUS_SAYS_WHAT_WAITS_OK")


async def run() -> None:
    await an_earlier_expiry_rests_on()
    await the_late_reads_behind_the_placements()
    await the_late_reads_give_way()
    await a_re_read_never_holds_the_probes()
    await the_status_says_what_waits()
    print("REQUOTE_OK")


if __name__ == "__main__":
    asyncio.run(run())
