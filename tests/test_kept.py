"""1.154.40 — what pricing a market did again at every look, now done once: a card's kept record read from the database
(kept_read / kept_record: touch:, flip:, range:, updown:<slug>:start | end | lead) once a write of its family, not once a
look; a ladder level's direction read off its rules once a text (level_direction → level_read); a reward period's dates
parsed once a period (reward_window); the odds of a card whose market has no book not worked out at all (sim_markets);
the mark that tells whether a look changed a record (trade_mark); the order's wait for its own record on threads of its
own (store_wait).

Each must give exactly what the plain way gave — checked here against that way itself (the store read at every access,
the functions as they were written before, kept below), through writes, deletions, cards moving on to another market and
stores of another kind — and must do the work once: the reads and the parses are counted.

Run: python tests/test_kept.py
"""
import asyncio
import copy
import datetime as dt
import decimal
import json
import math
import pickle
import random
import re
import sys
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

D = m.D
BJ = lambda mo, d, h, mi=0: int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
NOW = BJ(11, 25, 11, 0)


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


class Counted:
    """A store's reads counted (get), everything else its own."""
    def __init__(self, store):
        self.store, self.reads = store, []
        self.plain = store.get
        store.get = self.get

    def get(self, key, default=None):
        self.reads.append(key)
        return self.plain(key, default)

    def take(self):
        got, self.reads[:] = list(self.reads), []
        return got


def plain_record(store, key, start_ms):
    """A card's history as every access read it before 1.154.40."""
    saved = store.get(key, {})
    return saved if isinstance(saved, dict) and saved.get("start") == start_ms else {}


# --- the cards' own records ------------------------------------------------------------------------------------------------------
def the_records_are_read_once_a_write() -> None:
    store = m.Store(":memory:")
    touch, flip, rng = m.TouchMarket(store, m.TOUCH_MARKETS[0]), m.FlipMarket(store, m.FLIP_MARKETS[0]), m.RangeMarket(store, m.RANGE_MARKETS[0])
    cards = [(touch, "touch", f"touch:{touch.spec.slug}", lambda: touch.start_ms), (flip, "flip", f"flip:{flip.spec.slug}", lambda: flip.spec.start_ms),
             (rng, "range", f"range:{rng.spec.slug}", lambda: rng.start_ms)]
    counted = Counted(store)
    for card, family, key, start in cards:
        assert card.history == {} and counted.take() == [key], "nothing kept yet: read once"
        assert card.history == {} and card.history == {} and counted.take() == [], "...and not again"
        record = {"kind": "clear", "through": 123, "start": start(), "high": 1.5, "low": None}
        store.put(key, record)
        first = card.history
        assert first == record and first is not record and counted.take() == [key], "a write of the family: read again, once"
        for _ in range(50):
            assert card.history == record
        assert counted.take() == [], "fifty looks, no read"
        # what is handed out is the caller's own (the scans add to it before they save): the next look is not changed by it
        first["through"], first["more"] = 999, 1
        again = card.history
        assert again == record and again is not first and counted.take() == []
        # a record with a list or a dict in it (none of these three writes one; a cap's does): the caller's own all the way down
        nested = {"start": start(), "spike": [1, 2.5, 3], "parts": {"a": [1]}}
        store.put(key, nested)
        got = card.history
        got["spike"].append(4); got["parts"]["a"].append(2)
        assert card.history == nested and counted.take() == [key]
        # a write of another family is none of this card's; one of its own family's other keys reads it again (the same answer)
        store.put("sim:x", {"status": "resting"}); store.put("notice:1", {"a": 1})
        assert card.history == nested and counted.take() == []
        store.put(f"{family}:another-market", {"start": 1})
        assert card.history == nested and counted.take() == [key]
        # the record of another window is no record of this one
        store.put(key, {**record, "start": start() + 1})
        assert card.history == {} == plain_record(store, key, start()) and card.history == {}
        assert counted.take() == [key, key], "(the plain read just above is the second)"
        # deleted, replaced, a whole family dropped: each is a write
        store.put(key, record)
        assert card.history == record
        store.delete_keys([key])
        assert card.history == {} and card.history == {}
        store.replace([], [(key, record)])
        assert card.history == record
        store.delete_prefix(f"{family}:")
        assert card.history == {}
        counted.take()
    # a card that moves on to another window, or another market, is asked about that one
    store.put(f"touch:{touch.spec.slug}", {"kind": "low", "start": touch.start_ms})
    assert touch.history["kind"] == "low"
    was = touch.start_ms
    touch.start_ms = was + 60_000
    assert touch.history == {}
    touch.start_ms = was
    assert touch.history["kind"] == "low"
    other = m.dataclasses.replace(touch.spec, slug="another-touch-market")
    store.put(f"touch:{other.slug}", {"kind": "high", "start": touch.start_ms})
    touch.spec = other
    assert touch.history["kind"] == "high"
    # a store that keeps no count of its writes (a stand-in) is read every time; another store is another store, whatever its counts
    rows = {"touch:x": {"kind": "clear", "start": 5}}
    stand_in = types.SimpleNamespace(reads=0)
    def get(key, default=None):
        stand_in.reads += 1
        return copy.deepcopy(rows.get(key, default))
    stand_in.get = get
    loose = types.SimpleNamespace(store=stand_in, spec=types.SimpleNamespace(slug="x"), start_ms=5)
    assert m.kept_record(loose, "touch", "touch:x", 5) == rows["touch:x"] and m.kept_record(loose, "touch", "touch:x", 5) == rows["touch:x"] and stand_in.reads == 2
    a, b = m.Store(":memory:"), m.Store(":memory:")
    a.put("touch:x", {"kind": "low", "start": 5}); b.put("touch:x", {"kind": "high", "start": 5})
    assert a.touched == b.touched
    moved = types.SimpleNamespace(store=a, spec=types.SimpleNamespace(slug="x"), start_ms=5)
    assert m.kept_record(moved, "touch", "touch:x", 5)["kind"] == "low"
    moved.store = b
    assert m.kept_record(moved, "touch", "touch:x", 5)["kind"] == "high"
    for s in (store, a, b):
        s.close()


def the_records_through_any_sequence() -> None:
    """Whatever is written, deleted and moved, in whatever order: every look gives what a read of the store gives — and reads
    it only when the family was written or the card asked about another market since its last look."""
    rnd = random.Random(40)
    store = m.Store(":memory:")
    touch, flip, rng = m.TouchMarket(store, m.TOUCH_MARKETS[1]), m.FlipMarket(store, m.FLIP_MARKETS[0]), m.RangeMarket(store, m.RANGE_MARKETS[1])
    cards = {"touch": (touch, lambda: f"touch:{touch.spec.slug}", lambda: touch.start_ms), "flip": (flip, lambda: f"flip:{flip.spec.slug}", lambda: flip.spec.start_ms),
             "range": (rng, lambda: f"range:{rng.spec.slug}", lambda: rng.start_ms)}
    start0 = touch.start_ms
    counted = Counted(store)
    seen: dict = {}  # family -> (the store's count of the family's writes, the record asked for) at the card's last look
    looks = reads = 0
    for step in range(3000):
        family = rnd.choice(list(cards))
        card, key, start = cards[family]
        what = rnd.randrange(12)
        if what == 0:
            store.put(key(), {"kind": rnd.choice(["clear", "low", "high", "flip"]), "through": rnd.randrange(10 ** 6), "start": start() + rnd.choice([0, 0, 0, 1]),
                              "top": rnd.random(), **({"spike": [1, rnd.random()]} if rnd.random() < 0.2 else {})})
        elif what == 1:
            (store.delete_keys if rnd.random() < 0.5 else lambda keys: store.replace(keys, []))([key()])
        elif what == 2:
            store.put(f"{family}:someone-else", {"start": step})
        elif what == 3:
            store.put(rnd.choice(["sim:a", "wsample:b", "updown:c:start", "notice:d"]), {"n": step})  # other families (updown: is none of these cards')
        elif what == 4 and family == "touch":
            touch.start_ms = start0 + rnd.choice([0, 0, 60_000])  # the card asked about another window (or the same again)
        else:
            counted.take()
            got = card.history
            took = counted.take()
            looks, reads = looks + 1, reads + len(took)
            now = (store.touched.get(family, 0), (key(), start()))
            assert took == ([key()] if seen.get(family) != now else []), (step, family, took, seen.get(family), now)  # read: its family written, or another record asked for, since the last look
            seen[family] = now
            assert got == plain_record(store, key(), start()) and json.dumps(got, sort_keys=True) == json.dumps(plain_record(store, key(), start()), sort_keys=True), (step, got)
            if got and rnd.random() < 0.3:
                got["through"] = -1  # the caller's own dict
                got.setdefault("spike", []).append(9)
    assert looks > 1500 and reads < looks / 2, (looks, reads)
    store.close()


def the_closes_and_the_lead() -> None:
    """UpDownMarket.close_of and IndexUpDownMarket.lead_start: the plain reads' answers, read once a write of the cards' closes."""
    store = m.Store(":memory:")
    spec = next(u.spec for u in m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}), m.Store(":memory:"), FM(NOW), None).updowns.values()
                if type(u) is m.UpDownMarket)  # a daily crypto card's market of today
    card = m.UpDownMarket(store, spec)
    counted = Counted(store)
    key = lambda which, s=None: f"updown:{(s or card.spec).slug}:{which}"
    assert card.close_of("start") is None and card.close_of("end") is None and counted.take() == [key("start"), key("end")]
    assert card.close_of("start") is None and card.close_of("end") is None and counted.take() == []
    store.put(key("start"), {"open": spec.start_ms, "close": "61234.5", "read_ms": 1})
    assert card.close_of("start") == D("61234.5") and card.close_of("end") is None and counted.take() == [key("start"), key("end")]
    for _ in range(40):
        assert card.close_of("start") == D("61234.5") and card.close_of("end") is None
        assert isinstance(card.odds(NOW), (str, m.UpDownOdds))
    assert counted.take() == [], "forty looks (and their odds): no read"
    # every shape the plain read turned down is turned down: another candle's close, a close that is no positive number, no record
    for saved, want in (({"open": spec.start_ms + 60_000, "close": "5"}, None), ({"open": spec.start_ms, "close": "0"}, None), ({"open": spec.start_ms, "close": "x"}, None),
                        ({"open": spec.start_ms}, None), ("text", None), ({"open": str(spec.start_ms), "close": 7}, D("7")), ({"open": spec.start_ms, "close": "-3"}, None)):
        store.put(key("start"), saved)
        assert card.close_of("start") == want == card.close_read("start", spec.start_ms), (saved, card.close_of("start"))
    store.put(key("start"), {"open": spec.start_ms, "close": "100"}); store.put(key("end"), {"open": spec.end_ms, "close": "101"})
    odds = card.odds(NOW)
    assert isinstance(odds, m.UpDownOdds) and odds.settled and odds.up == 1.0
    # the card on another market (a daily card rolled on, a position's own window: at) asks about that one
    later = m.dataclasses.replace(spec, slug=spec.slug + "-next")
    card.spec = later
    assert card.close_of("start") is None and card.close_of("end") is None
    store.put(key("start", later), {"open": later.start_ms, "close": "9"})
    assert card.close_of("start") == D("9")
    card.spec = spec
    assert card.close_of("start") == D("100") and card.at(later).close_of("start") == D("9")
    store.delete_prefix(f"updown:{spec.slug}")
    assert card.close_of("start") is None and card.close_of("end") is None
    # the exchange's price at a window's start (LEAD_QUOTE): kept the same way, and found once note_lead has written it
    W = m.WINDOW_UPDOWN_MARKETS[0]
    window = W.current(NOW)
    lead = m.IndexUpDownMarket(store, window, None, "sh", m.session_minute_fn("sh"))
    counted.take()
    lead_key = f"updown:{window.slug}:lead"
    assert lead.lead_start() is None and lead.lead_start() is None and counted.take() == [lead_key]
    store.put(lead_key, {"missed": True, "read_ms": 1})
    assert lead.lead_start() is None and counted.take() == [lead_key]
    store.put(lead_key, {"price": "88.01", "at": window.start_ms + 60_500, "read_ms": 2, "source": "腾讯"})
    assert lead.lead_start() == (D("88.01"), window.start_ms + 60_500) == lead.lead_read() and lead.lead_start() == lead.lead_start()
    assert counted.take() == [lead_key, lead_key], "(one read for the look, one for the plain read beside it)"
    nxt = lead.at(W.window_spec(window.start_ms + 15 * 60_000))
    assert nxt.lead_start() is None, "another window: its own record"
    store.close()


# --- a ladder level's direction ---------------------------------------------------------------------------------------------------
def old_level_direction(rules, *titles):
    """level_direction as it was written before 1.154.40."""
    text = re.sub(r"\s+", " ", re.sub(r"[\"'“”‘’*]", "", str(rules))).lower()
    down = re.search(r"\blow(?: price)? (?:is )?(?:equal to or (?:lower|less|below)|at or below|<=|≤)|\bfinal low\b", text)
    up = re.search(r"\bhigh(?: price)? (?:is )?(?:equal to or (?:greater|higher|above)|at or above|>=|≥)|\bfinal high\b", text)
    by_rules = ("down" if down else "up") if bool(down) != bool(up) else ""
    arrow = next((d for t in titles for d, mark in (("down", r"[↓▼]"), ("up", r"[↑▲]")) if re.search(mark, str(t))), "")
    if by_rules and arrow and arrow != by_rules:
        return arrow, "冲突"
    if by_rules:
        return by_rules, "规则"
    if arrow:
        return arrow, "标题"
    for title in titles:
        t = str(title).lower()
        if re.search(r"\bdips?\b|\bdrops?\b|\bfalls?\b|\bbelow\b|跌", t):
            return "down", "标题"
        if re.search(r"\breach(?:es)?\b|\babove\b|\brises?\b|\bhigh(?:er)?\b|涨", t):
            return "up", "标题"
    return "", ""


def the_direction_is_read_once_a_text() -> None:
    rnd = random.Random(7)
    filler = "This market resolves according to Binance 1 minute candles for the pair, as published. " * 30
    low = "resolves to Yes if any 1 minute candle has a final “Low” price equal to or lower than the price specified"
    high = 'resolves to Yes if any candle has a final "High" price that is equal to or greater than the listed price'
    rules = ["", filler, filler + low, filler + high + filler, low + " " + high, "the Low price is <= the level", "HIGH PRICE IS AT OR ABOVE", None, 5, {"a": 1}, ["x"]]
    titles = ["", "↑ 130,000", "↓ $100k", "▼ 95", "▲ 2,050", "$95,000", "Will Bitcoin reach $130,000 in October 2026?", "Will Bitcoin dip to $95,000?",
              "Will Solana hit $250?", "涨到 300", "跌破 200", None, 12]
    m.level_read.cache_clear()
    calls = 0
    for _ in range(4):  # every combination four times over: the same answer, worked out the first time alone
        for r in rules:
            for a in titles:
                for b in (titles[rnd.randrange(len(titles))], ):
                    got = m.level_direction(r, a, b)
                    assert got == old_level_direction(r, a, b), (r if not isinstance(r, str) else r[-40:], a, b, got)
                    calls += 1
        assert m.level_direction(filler + low) == ("down", "规则") and m.level_direction(high) == ("up", "规则") and m.level_direction("") == ("", "")
    info = m.level_read.cache_info()
    assert info.misses <= len(rules) * len(titles) * len(titles) + 3 and info.hits >= calls - info.misses, info
    # the same texts handed over as other objects (a market's details read anew every minute) are the same texts
    before = m.level_read.cache_info().misses
    for _ in range(20):
        assert m.level_direction("".join([filler, low]), "".join(["↑ ", "130,000"])) == ("up", "冲突")
    assert m.level_read.cache_info().misses == before + 1
    # kept for no more than a thousand texts
    for i in range(1500):
        m.level_direction(f"rules {i} final low", f"title {i}")
    assert m.level_read.cache_info().currsize <= 1024


# --- a reward period ---------------------------------------------------------------------------------------------------------------
def old_reward_status(meta, now_ms):
    """predict_reward_status as it was written before 1.154.40."""
    out = {"points_active": None, "points_note": "积分状态暂缺", "points_rate": None, "points_end_ms": None}
    rewards = meta.get("rewards")
    if not isinstance(rewards, dict) or "current" not in rewards:
        return out
    current = rewards["current"]
    if current is None:
        return {**out, "points_active": False, "points_note": "积分未激活"}
    if not isinstance(current, dict):
        return out
    try:
        rate = D(str(current.get("hourlyRate")))
        start = dt.datetime.fromisoformat(str(current.get("startsAt")).replace("Z", "+00:00"))
        end = dt.datetime.fromisoformat(str(current.get("endsAt")).replace("Z", "+00:00"))
        if not rate.is_finite() or rate < 0 or start.utcoffset() is None or end.utcoffset() is None or end <= start:
            return out
        start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        rate_float = float(rate)
        if not math.isfinite(rate_float):
            return out
    except (ValueError, TypeError, OverflowError, decimal.InvalidOperation):
        return out
    active = rate > 0 and start_ms <= now_ms < end_ms
    note = "积分已激活" if active else "积分未开始" if now_ms < start_ms else "积分已结束" if now_ms >= end_ms else "积分未激活"
    return {"points_active": active, "points_note": note, "points_rate": rate_float, "points_end_ms": end_ms}


def the_reward_period_is_parsed_once() -> None:
    iso = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).isoformat().replace("+00:00", "Z")
    rates = ["25", 25, 12.5, "0", 0, "-1", "NaN", "Infinity", "1e400", "x", None, "", [1], {"a": 1}, True, "0.000000000000000001"]
    starts = [iso(NOW - 3_600_000), iso(NOW + 3_600_000), "2026-11-25T03:00:00+08:00", "2026-11-25T03:00:00", "2026-11-25", "soon", None, 5, "", "9999-12-31T23:59:59Z"]
    ends = [iso(NOW + 3_600_000), iso(NOW - 60_000), iso(NOW - 7_200_000), "2026-11-26T03:00:00+00:00", "2026-11-26T03:00:00", "never", None, "", "0001-01-01T00:00:00Z"]
    metas = [{}, {"rewards": None}, {"rewards": []}, {"rewards": {}}, {"rewards": {"current": None}}, {"rewards": {"current": "x"}}, {"rewards": {"current": {}}}]
    metas += [{"rewards": {"current": {"hourlyRate": r, "startsAt": s, "endsAt": e}}} for r in rates for s in starts for e in ends]
    m.reward_window.cache_clear()
    seen = set()
    for now in (NOW, NOW - 2 * 3_600_000, NOW + 2 * 3_600_000, NOW + 3_600_000, NOW - 3_600_000, 0):
        for meta in metas:
            got, want = m.predict_reward_status(meta, now), old_reward_status(meta, now)
            assert got == want and json.dumps(got) == json.dumps(want) and [type(v) for v in got.values()] == [type(v) for v in want.values()], (meta, now, got, want)
            seen.add(got["points_note"])
    assert seen == {"积分状态暂缺", "积分未激活", "积分已激活", "积分未开始", "积分已结束"}, seen
    # a period is parsed once, however often its level is priced (the 62 levels of the six ladders, at every pricing)
    m.reward_window.cache_clear()
    some = metas[7:7 + 62]
    for now in range(NOW, NOW + 300_000, 1000):
        for meta in some:
            m.predict_reward_status(meta, now)
    info = m.reward_window.cache_info()
    periods = {tuple(str(meta["rewards"]["current"].get(k)) for k in ("hourlyRate", "startsAt", "endsAt")) for meta in some}
    assert info.misses == len(periods) and info.hits == 300 * len(some) - len(periods), (info, len(periods))
    # what is handed out is a new dict each time, as before (a caller adds its own keys to it: Bot.points_status)
    meta = metas[-1]
    a, b = m.predict_reward_status(meta, NOW), m.predict_reward_status(meta, NOW)
    assert a == b and a is not b


# --- the odds of a card with no book ------------------------------------------------------------------------------------------------
def a_card_without_a_book_is_not_priced() -> None:
    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "PROBABILITY": "off"}), m.Store(":memory:"), FM(NOW), None)
    assert len(bot.updowns) >= 5
    asked = []
    for key, card in bot.updowns.items():
        card.odds = lambda now_ms, key=key, plain=card.odds: asked.append(key) or plain(now_ms)
    bot.predict.books.clear()
    assert [mk for mk in bot.sim_markets(NOW) if mk.kind == "updown"] == [] and asked == [], asked
    # ...and one whose market has its book is priced as before: its odds asked for once, the market as its card has it
    key, card = next((k, c) for k, c in bot.updowns.items() if not isinstance(c, m.IndexUpDownMarket))
    spec = card.spec
    bot.store.put(f"updown:{spec.slug}:start", {"open": spec.start_ms, "close": "100"})
    card.price, card.priced_ms, card.sigma = D("101"), NOW, 0.5
    side = lambda rows: tuple((D(p), D(q)) for p, q in rows)
    bot.predict.books[spec.key] = m.PredictBook(spec.key, spec.slug, "777", "t", side([("0.40", "100")]), side([("0.60", "100")]), NOW, None)
    bot.predict.info[spec.slug] = {"outcomes": ["Up", "Down"]}
    priced = [mk for mk in bot.sim_markets(NOW) if mk.kind == "updown"]
    if isinstance(card.odds(NOW), m.UpDownOdds):  # (the card prices now: its market is in the list, once)
        asked.clear()
        priced = [mk for mk in bot.sim_markets(NOW) if mk.kind == "updown"]
        assert [mk.market for mk in priced] == [spec.slug] and asked == [key] and priced[0].fair_up == card.odds(NOW).fair_up, (asked, priced)
    else:
        assert priced == [] and asked.count(key) >= 1, (asked, card.odds(NOW))
    bot.store.close()


# --- whether a look changed a record -------------------------------------------------------------------------------------------------
def the_mark_tells_a_change() -> None:
    rnd = random.Random(3)
    def record():
        return {"v": 2, "status": "resting", "market": "m1", "price": 0.47, "shares": 0.0, "order": 100.0, "fair": 0.5123, "filled": None, "maker": True,
                "fills": [{"at": 1, "shares": 5.0, "how": "x"}], "entry": {"book": {"bids": [[0.46, 100.0], [0.45, 50.0]], "asks": [[0.5, 10.0]]}, "card": ["a", "b"]},
                "live": {"state": "open", "order_id": "55", "hash": "0x" + "ab" * 32, "events": [{"at": 1, "what": "下单"}], "want": {"cap": 0.5}}, "settle": {"key": "HSI", "close_ms": 9}}
    t = record()
    before = m.trade_mark(t)
    assert isinstance(before, bytes) and m.trade_mark(t) == before == m.trade_mark(copy.deepcopy(record())) and pickle.loads(before) == t
    # every change a look makes is a change of the mark — as it was of the sorted JSON — at whatever depth
    changes = [lambda t: t.update(status="filled"), lambda t: t.update(shares=5.0), lambda t: t.update(shares=0), lambda t: t.update(filled=1), lambda t: t.pop("fair"),
               lambda t: t.update(note="x"), lambda t: t["fills"].append({"at": 2, "shares": 1.0}), lambda t: t["fills"][0].update(shares=6.0),
               lambda t: t["live"].update(state="cancelling"), lambda t: t["live"]["events"].append({"at": 2, "what": "撤单"}), lambda t: t["live"]["want"].update(cap=0.51),
               lambda t: t["entry"]["book"]["bids"][1].__setitem__(1, 51.0), lambda t: t["entry"]["card"].reverse(), lambda t: t["settle"].update(close_ms=10),
               lambda t: t.update(maker=1), lambda t: t.update(price=D("0.47")), lambda t: t.update(markout={"1分": {"at": 1, "mid": None}})]
    for change in changes:
        t = record()
        was, was_json = m.trade_mark(t), json.dumps(t, sort_keys=True, default=str)
        change(t)
        assert json.dumps(t, sort_keys=True, default=str) != was_json or t.get("maker") == 1, "(each of these changed the record)"
        assert m.trade_mark(t) != was, change
    # a look that touches nothing leaves the mark as it was, through any number of reads of the record
    t = record()
    was = m.trade_mark(t)
    for _ in range(200):
        float(t["price"]) * float(t.get("shares") or 0); sorted(t); str(t["live"].get("order_id") or ""); (t.get("settle") or {}).get("window")
        key = rnd.choice(list(t)); t[key] = t[key]
    assert m.trade_mark(t) == was
    # a record that cannot be pickled is marked as before 1.154.40 (its sorted JSON), and a change still shows
    odd = {**record(), "evidence": m.LazyEvidence(lambda: {"a": 1})} if hasattr(m, "LazyEvidence") else {**record(), "x": (lambda: 0)}
    odd["fn"] = lambda: 0
    mark = m.trade_mark(odd)
    assert isinstance(mark, str) and mark == json.dumps(odd, sort_keys=True, default=str) == m.trade_mark(odd)
    odd["status"] = "filled"
    assert m.trade_mark(odd) != mark and m.trade_mark({"a": 1}) != m.trade_mark({"a": 1.0})


# --- the order's wait for its record -------------------------------------------------------------------------------------------------
async def the_wait_has_threads_of_its_own() -> None:
    loop = asyncio.get_running_loop()
    held, one = threading.Event(), ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(one)
    blocker = loop.run_in_executor(None, held.wait, 20)  # the default pool's only thread is taken (a request at its timeout)
    queued = asyncio.ensure_future(asyncio.to_thread(lambda: "late"))
    await asyncio.sleep(0.05)
    began = time.perf_counter()
    names = []
    assert await m.store_wait(lambda a, b: names.append(threading.current_thread().name) or a + b, 3, 4) == 7
    assert time.perf_counter() - began < 2 and names[0].startswith("store-wait") and not queued.done(), (names, "the wait did not stand behind the default pool's")
    store = m.Store(":memory:")
    store.put("sim:a", {"x": 1})
    assert await m.store_wait(store.reached, store.mark()) is True
    held.set()
    assert await blocker and await queued == "late"
    pool = m._STORE_POOL
    m.store_wait_close()
    assert m._STORE_POOL is None and pool is not None and await m.store_wait(lambda: 8) == 8 and m._STORE_POOL is not pool, "made again by the next wait"
    m.store_wait_close(); m.store_wait_close()
    one.shutdown(wait=False)
    store.close()


def main() -> None:
    the_records_are_read_once_a_write()
    the_records_through_any_sequence()
    the_closes_and_the_lead()
    the_direction_is_read_once_a_text()
    the_reward_period_is_parsed_once()
    a_card_without_a_book_is_not_priced()
    the_mark_tells_a_change()
    asyncio.run(the_wait_has_threads_of_its_own())
    print("KEPT_OK")


if __name__ == "__main__":
    main()
