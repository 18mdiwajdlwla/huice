"""1.154.43 — what a start reads. The kept records (the model trader's sim: family, the pre-open ledger's pairs) were parsed
a row a call of the JSON decoder, and the decoder shares a key's string only within one call: every record in memory had
strings of its own for its keys, a few hundred words 61,000 times over — a quarter of the 900 MB the records took after a
start. They are parsed a chunk at a call now (main.parse_rows), read as they come and not all held as text first; the
collector is off while they are read (main.gc_held: it walked the growing heap again and again, a heap with no garbage in
it) and what was read is set aside from it at once (GcWatch.set_aside: the first settle walked it all once more, 0.7–1.0 s
of the event loop ninety seconds into every start).

Here: parse_rows gives what json.loads gives for each text alone — every kind of value, the keys shared and nothing else,
a text that does not parse (or not alone) raising as it always did; the store's and the ledger's reads through it give the
records they gave; the collector's state is as it was found after each read, whatever happens in it.

Run: python tests/test_start.py
"""
import gc
import json
import math
import random
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import preopen as p

NOW = 1_795_600_000_000


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def written(value) -> str:
    """A value as json.dumps writes it, NaN and all (two parses are the same when they are written the same)."""
    return json.dumps(value, ensure_ascii=False, sort_keys=False)


def record(i: int, rnd: random.Random) -> dict:
    return {"v": 2, "market": f"m{i}", "status": rnd.choice(["settled", "cancelled", "resting"]), "price": rnd.choice([0.47, 0.5, 0.613]), "shares": rnd.choice([0, 0.0, 100.0, 33.3]),
            "maker": rnd.random() < 0.5, "filled": None, "note": rnd.choice(["", "挂涨 47¢", "吃跌"]), "opened": NOW + i,
            "entry": {"book": {"bids": [[0.46, 100.0], [0.45, 50.0]], "asks": [[0.5, 10.0]]}, "card": ["a", "b"], "basis": {"fair_up": 0.5, "ref": 24600.0}},
            "live": {"state": "done", "order_id": str(5000 + i), "hash": f"0x{i:064x}", "events": [{"at": NOW, "what": "下单"}, {"at": NOW + 1, "what": "撤单"}]},
            "settle": {"key": "HSI", "close_ms": NOW + 5}, "fills": [], "big": 10 ** 30 + i}


def rows_parse_as_each_alone() -> None:
    rnd = random.Random(43)
    texts = [json.dumps(record(i, rnd), ensure_ascii=False) for i in range(300)]
    texts += ['"中文 text"', "[]", "{}", "null", "true", "false", "0", "-0.0", "1.50", "-17", "1e400", "-1e400", "NaN", "Infinity", "-Infinity", "12345678901234567890123",
              '[1, [2, [3, {"a": null}]]]', '{"a": {"a": {"a": 1}}, "b": "\\u4e2d\\n"}', '""', "2.5e-7", '{"k": 1, "k": 2}']
    rnd.shuffle(texts)
    for n in (0, 1, 2, 3, 7, 255, 256, 257, len(texts)):
        part = texts[:n]
        got, want = m.parse_rows(part), [json.loads(t) for t in part]
        assert len(got) == len(want) and all(type(a) is type(b) and written(a) == written(b) for a, b in zip(got, want)), n
    # the keys of the rows' objects are the same strings — at every depth — and nothing else is shared
    rows = m.parse_rows([json.dumps(record(i, rnd), ensure_ascii=False) for i in range(300)])
    keys = lambda row: [id(k) for k in row] + [id(k) for k in row["entry"]] + [id(k) for k in row["live"]] + [id(k) for k in row["live"]["events"][0]] + [id(k) for k in row["entry"]["book"]]
    assert all(keys(row) == keys(rows[0]) for row in rows), "one string a key, whichever row"
    alone = [json.loads(json.dumps(record(i, rnd), ensure_ascii=False)) for i in range(3)]
    assert keys(alone[0]) != keys(alone[1]), "(parsed a row a call, each row has strings of its own: what this is about)"
    assert all(a is not b and a["live"] is not b["live"] and a["live"]["events"] is not b["live"]["events"] and a["entry"]["book"]["bids"] is not b["entry"]["book"]["bids"]
               for a, b in zip(rows, rows[1:])), "every value its record's own"
    rows[0]["live"]["events"].append({"at": 9})
    rows[0]["entry"]["book"]["bids"][0][1] = -1.0
    assert len(rows[1]["live"]["events"]) == 2 and rows[1]["entry"]["book"]["bids"][0][1] == 100.0
    # a text that does not parse raises as json.loads raises for it, wherever it stands among the others
    good = [json.dumps(record(i, rnd), ensure_ascii=False) for i in range(5)]
    for bad in ('{"a": 1', "", "{'a': 1}", '{"a": 1}}', "nul", '{"a": 1} x', "[1, 2", '"open', "1,2", "tru"):
        try:
            json.loads(bad)
        except ValueError as error:
            want = (type(error), str(error))
        else:
            raise AssertionError(bad)
        for at in (0, 2, 5):
            try:
                m.parse_rows(good[:at] + [bad] + good[at:])
            except ValueError as error:
                assert (type(error), str(error)) == want, (bad, at, str(error), want)
            else:
                raise AssertionError(f"{bad!r} was taken among the others")
    # texts that parse only together — each no JSON value alone — are not taken for the values they make side by side
    for texts in (["1,2", "[3", "4]"], ['{"a": 1', '"b": 2}'], ["[1", "2]"], ['{"a":1},{"b":2}', '{"c":3'], ['"a', 'b"'], ["1", "2,3"], ["[1],[2]", "[3]"][:1] + ["x"]):
        try:
            m.parse_rows(list(texts))
        except ValueError:
            pass
        else:
            raise AssertionError(texts)
    # a text json.loads takes that the one call would not take as it stands (leading space: none of the bot's writes) is parsed alone
    assert m.parse_rows(['  {"a": 1}', "\n[2]", " 3 "]) == [{"a": 1}, [2], 3] and m.parse_rows([" 7"]) == [7] and m.parse_rows([]) == []
    assert math.isnan(m.parse_rows(["NaN", "1"])[0]) and m.parse_rows(["-Infinity", "Infinity", "-0.0"])[:2] == [-math.inf, math.inf]


def the_store_reads_in_chunks() -> None:
    rnd = random.Random(44)
    with tempfile.TemporaryDirectory() as folder:
        path = str(Path(folder) / "state.sqlite3")
        store = m.Store(path)
        records = {f"sim:t{i:04d}": record(i, rnd) for i in range(700)}
        store.put_many(records.items())
        store.put_many([("simx:other", {"a": 1}), ("si:before", {"b": 2}), ("wsample:w:1", {"t": 5}), ("sim:中文键", record(7000, rnd))])
        records["sim:中文键"] = store.get("sim:中文键")
        assert store.flush()
        want = [(k, records[k]) for k in sorted(records)]
        calls, plain = [], m.json.loads
        m.json.loads = lambda text, *a, **k: calls.append(len(text)) or plain(text, *a, **k)
        try:
            got = store.items("sim:")
            assert len(calls) == 3, f"{len(calls)} calls of the decoder for 701 rows (256 a call)"
            del calls[:]
            assert [k for k, _ in store.each("sim:")] == [k for k, _ in want] and len(calls) == 3
        finally:
            m.json.loads = plain
        assert [k for k, _ in got] == [k for k, _ in want] and all(written(a) == written(b) for (_, a), (_, b) in zip(got, want)), "the rows it gave before, in the keys' order"
        assert all([id(k) for k in row] == [id(k) for k in got[0][1]] for _, row in got[:256]) and got[0][1] is not got[1][1], "the keys shared within a chunk"
        assert [k for k, _ in store.items("sim:t00")] == [f"sim:t{i:04d}" for i in range(100)] and store.items("nothing:") == [] and list(store.each("nothing:")) == []
        assert store.keys("sim:") == [k for k, _ in want] and written(store.get("sim:t0005")) == written(records["sim:t0005"])
        # a pass left early leaves the store as it was
        for n, (k, row) in enumerate(store.each("sim:")):
            if n == 300:
                break
        assert len(store.items("sim:")) == 701
        # what is queued and not written yet is read in its place: a record changed, one deleted, new ones before, among and after
        store.close()
        held = m.Store(path, persist=False)  # (every write stays queued)
        held.put("sim:t0005", {"changed": True})
        held.put("sim:t0300a", {"new": 1}); held.put("sim:a-first", {"new": 2}); held.put("sim:zz-last", {"new": 3})
        held.delete_keys(["sim:t0010", "sim:t0255", "sim:t0256"])
        merged = dict(want)
        merged.update({"sim:t0005": {"changed": True}, "sim:t0300a": {"new": 1}, "sim:a-first": {"new": 2}, "sim:zz-last": {"new": 3}})
        for k in ("sim:t0010", "sim:t0255", "sim:t0256"):
            del merged[k]
        for read in (held.items("sim:"), list(held.each("sim:"))):
            assert [k for k, _ in read] == sorted(merged) and all(written(v) == written(merged[k]) for k, v in read)
        held.close()
        # a row that does not parse stops the read, as before (the others are not taken without it)
        import sqlite3
        conn = sqlite3.connect(path)
        conn.execute("UPDATE records SET v = ? WHERE k = ?", ('{"cut": ', "sim:t0100"))
        conn.commit(); conn.close()
        broken = m.Store(path)
        for read in (lambda: broken.items("sim:"), lambda: list(broken.each("sim:"))):
            try:
                read()
            except ValueError:
                pass
            else:
                raise AssertionError("a row that is no JSON was read")
        assert len(broken.items("sim:t00")) == 100, "(the rows before it are still read by themselves)"
        broken.close()
    # a store in memory: a row at a time, as before (it shows a pass each row as it stands)
    memory = m.Store(":memory:")
    memory.put_many((f"sim:t{i:03d}", {"n": i, "live": {"state": "done"}}) for i in range(600))
    assert [v["n"] for _, v in memory.items("sim:")] == list(range(600)) == [v["n"] for _, v in memory.each("sim:")]
    seen = []
    for k, v in memory.each("sim:"):
        if v["n"] == 10:
            memory.put("sim:t020", {"n": -20, "live": {}})  # changed ahead of the pass: seen as it stands when the pass comes to it
        seen.append(v["n"])
    assert seen[20] == -20 and len(seen) == 600
    memory.close()


def the_ledger_reads_in_chunks() -> None:
    rnd = random.Random(45)
    def pair(n):
        return {"slug": f"w{n // 4}#{n % 4}", "key": "BTC-15M", "window": f"w{n // 4}", "level": n % 4, "attempt": 0, "phase": rnd.choice(["closed", "active", "closing"]),
                "accounted": rnd.random() < 0.6, "start_ms": NOW + n, "market_id": str(700 + n // 4), "name": f"场次 {n // 4}", "pnl": rnd.choice([0.0, 0.0, 1.5, -2.25]),
                "accounted_day": "2026-11-25", "price": "0.47", "shares": "10",
                "orders": {side: {"hash": f"0x{n}{side}", "id": f"{n}{side[0]}", "state": rnd.choice(["final", "open", "removed"]), "filled": rnd.choice([0.0, 0.0, 5.0]),
                                  "index_set": 1 + (side == "up"), "body": {"data": {"order": {"tokenId": f"{n}{side}", "makerAmount": "1"}}}} for side in ("up", "down")}}
    with tempfile.TemporaryDirectory() as folder:
        path = str(Path(folder) / "bot.preopen.sqlite3")
        first = p.Ledger(path)
        first.save_many([pair(n) for n in range(700)])
        first.rename("w3#1", "w3#1@0")  # (a record out of its place: the table's order is the order of entry)
        first.save({**pair(9000), "slug": "last-in"})
        indexes = lambda ledger: {name: getattr(ledger, name) for name in ("by_hash", "tries", "tries_key", "ids", "hashes", "windows", "markets", "tokens", "market_name",
                                                                             "token_name", "scored_day", "marks")} | {
            "working": list(ledger.working), "struck": list(ledger.struck), "unsure": list(ledger.unsure), "filled": list(ledger.filled),
            "scored": {day: list(rows) for day, rows in ledger.scored.items()}, "pending": [r["slug"] for r in ledger.pending()]}
        want_rows, want_order, want_indexes = json.loads(json.dumps(first.rows)), list(first.rows), json.loads(json.dumps(indexes(first), default=list))
        first.close()
        calls, plain = [], m.json.loads
        m.json.loads = lambda text, *a, **k: calls.append(1) or plain(text, *a, **k)
        try:
            again = p.Ledger(path)
        finally:
            m.json.loads = plain
        assert len(calls) == 3, f"{len(calls)} calls of the decoder for 701 records"
        assert again.rows == want_rows and list(again.rows) == want_order and again.in_step() and gc.isenabled()
        assert json.loads(json.dumps(indexes(again), default=list)) == want_indexes, "every index as the records' own order makes it"
        rows = list(again.rows.values())
        shared = lambda row: [id(k) for k in row] + [id(k) for k in row["orders"]["up"]] + [id(k) for k in row["orders"]["up"]["body"]["data"]["order"]]
        assert all(shared(row) == shared(rows[0]) for row in rows[:256]) and rows[0]["orders"] is not rows[1]["orders"]
        again.close()
        # a record that does not parse stops the start, as before
        import sqlite3
        conn = sqlite3.connect(path)
        conn.execute("UPDATE pairs SET body = ? WHERE slug = ?", ('{"slug": "w5#0", ', "w5#0"))
        conn.commit(); conn.close()
        try:
            p.Ledger(path)
        except ValueError:
            assert gc.isenabled() and not m.GC_WATCH.holders, "the collector as it was found, whatever happened in the read"
        else:
            raise AssertionError("a record that is no JSON was read")


def the_collector_is_off_while_a_start_reads() -> None:
    assert gc.isenabled() and not m.GC_WATCH.holders and not m.GC_WATCH.watching
    base = gc.get_freeze_count()  # (the interpreter's own: a few hundred objects on Python 3.12, none on 3.13)
    with m.gc_held():
        assert not gc.isenabled()
        with m.gc_held():
            assert not gc.isenabled()
        assert not gc.isenabled(), "held until the last holder lets go"
    assert gc.isenabled()
    try:
        with m.gc_held():
            raise RuntimeError("the read failed")
    except RuntimeError:
        pass
    assert gc.isenabled() and not m.GC_WATCH.holders
    m.GC_WATCH.hold()  # (the rule taker's seconds: gc_guard) — a read inside them leaves the collector off, as it was
    with m.gc_held():
        pass
    assert not gc.isenabled()
    m.GC_WATCH.release()
    assert gc.isenabled()
    # the model trader's records: read with the collector off, on again after — and nothing set aside by a bot that is not running
    rnd = random.Random(46)
    with tempfile.TemporaryDirectory() as folder:
        store = m.Store(str(Path(folder) / "state.sqlite3"))
        store.put_many((f"sim:m{i}|up|吃", {**record(i, rnd), "kind": "close", "key": "HSI", "side": "up", "order": 100.0, "payout": 1.0, "slug": f"m{i}"}) for i in range(600))
        assert store.flush()
        bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}), store, FM(NOW), None)
        states, plain = [], m.sim_upgrade
        m.sim_upgrade = lambda t: states.append(gc.isenabled()) or plain(t)
        try:
            index = bot.sim_kept()
        finally:
            m.sim_upgrade = plain
        assert len(index.records) == 600 and states == [False] * 600 and gc.isenabled() and gc.get_freeze_count() == base and m.GC_WATCH.early == 0
        assert bot.sim_kept() is index and len(bot.sim_trades()) == 600, "read once: the next look reads nothing"
        rows = list(index.records.values())
        assert all([id(k) for k in row] == [id(k) for k in rows[0]] for row in rows[:256]), "the kept records' keys are shared too"
        # a read that fails leaves the collector on (and the records unread: the next look reads again)
        bot.sim_cache = None
        items = store.items
        store.items = lambda prefix: (_ for _ in ()).throw(m.RemoteError("the disk"))
        try:
            bot.sim_kept()
        except m.RemoteError:
            pass
        else:
            raise AssertionError("no error")
        store.items = items
        assert gc.isenabled() and not m.GC_WATCH.holders and len(bot.sim_kept().records) == 600
        # while the bot runs (the watch on), what a start reads is set aside from the collector at once: the ledger's records, the trader's
        m.GC_WATCH.start()
        try:
            ledger_path = str(Path(folder) / "bot.preopen.sqlite3")
            made = p.Ledger(ledger_path)
            made.save_many([{"slug": f"w{n}", "key": "BTC-15M", "phase": "closed", "accounted": True, "start_ms": NOW + n, "orders": {}} for n in range(300)])
            made.close()
            before = m.GC_WATCH.early
            ledger = p.Ledger(ledger_path)
            assert m.GC_WATCH.early == before + 1 and gc.get_freeze_count() > base + 300 and gc.isenabled() and len(ledger.rows) == 300
            frozen = gc.get_freeze_count()
            bot.sim_cache = None
            assert len(bot.sim_kept().records) == 600 and m.GC_WATCH.early == before + 2 and gc.get_freeze_count() > frozen
            ms = m.GC_WATCH.settle()  # the first settle after it: what was made since, not everything again
            assert m.GC_WATCH.settles == 1 and ms >= 0
            ledger.close()
        finally:
            m.GC_WATCH.stop()
        assert gc.get_freeze_count() <= base and m.GC_WATCH.early == 0 and m.GC_WATCH.settles == 0 and gc.isenabled(), "as it was found"
        store.close()


def main() -> None:
    rows_parse_as_each_alone()
    the_store_reads_in_chunks()
    the_ledger_reads_in_chunks()
    the_collector_is_off_while_a_start_reads()
    print("START_OK")


if __name__ == "__main__":
    main()
