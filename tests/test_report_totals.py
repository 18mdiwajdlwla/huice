"""1.154.39: the probability page's 模拟交易 card, with a real account behind it (the totals alone), is added up from each
record's kept figures — worked out once a saved record — not from a walk of every record at every build. 11-24 /diag: 18 holds
of 0.25–0.4 s in ten minutes (sim_stats ← sim_report), one each time the kept report ran out, with 16,000 records kept.

The figures must be the walk's to the last digit — which is why each total is still one sum() over every record's term, in
the walk's order, and not a running total brought up to date record by record: sum() carries a correction term, and a total
kept by adding and taking away, or added up in parts, does not come to the same digits (shown below). Checked here: the two
against each other over random records of every shape and every subset, and through the bot — saves, a record moved to tid#n,
old attempts removed, the records read afresh from the store, an open record changed in place (seen at once), a closed one
changed in place and never saved (found by the check that goes round the records), and a record put past the index (all worked
out again)."""
import asyncio, json, random, sys, time, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

NOW = int(dt.datetime(2026, 11, 24, 11, 25, tzinfo=m.BEIJING).timestamp() * 1000)
KINDS = list(m.SIM_KINDS)


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def record(rnd, n, status=None, kind=None, maker=None):
    """A record of any shape the totals read: every status, prices and sizes whose products do not add up exactly, fills at
    other fair prices, marks at some horizons, a mark without a middle, whole numbers among the floats."""
    status = status or rnd.choice(["settled"] * 5 + ["filled", "resting", "resting", "expired", "cancelled", "cancelled"])
    maker = rnd.choice([True, False, 1, 0]) if maker is None else maker
    order = rnd.choice([10, 33.3, 100.0, 250, 7.77])
    shares = (order if rnd.random() < 0.7 else round(order * rnd.random(), 3)) if status in ("settled", "filled") else \
        rnd.choice([0, 0.0, round(order * rnd.random(), 3)]) if status in ("resting", "cancelled") else 0.0
    price, fair = rnd.choice([0.1, 0.07, 0.33, 0.47, 0.5, 0.613, 0.9]), rnd.choice([0.2, 0.41, 0.55, 0.66, 0.97])
    t = {"v": 2, "status": status, "market": f"m{n % 40}", "slug": f"m{n % 40}", "market_id": str(9000 + n % 40), "item": "恒生指数",
         "kind": kind or rnd.choice(KINDS[:-1]), "key": "HSI", "side": "up", "label": "挂涨" if maker else "吃涨", "maker": maker, "fair": fair,
         "opened": NOW + rnd.choice([0, 0, 1, 5, 5, 60, 3600]) * 1000 + n % 3, "price": price, "signal": 0.1, "edge": fair - price, "order": order,
         "shares": shares, "filled": NOW if shares else None, "queue_ahead": 0.0, "queue_min": 0.0, "settle": {"key": "HSI", "close_ms": NOW + 3_600_000},
         "driver": "HSI", "driver_name": "恒生指数", "fills": [], "revisions": [], "final": status in ("settled", "expired", "cancelled"),
         "entry": {"at": NOW, "fair": fair, "fair_up": fair, "need": 0.03, "book": {}, "card": []}, "version": {}}
    if shares and rnd.random() < 0.6:
        parts = [round(shares * 0.3, 4), round(shares * 0.7, 4)]
        t["fills"] = [{"at": NOW, "shares": part, **({"fair": rnd.choice([0.3, 0.58, 0.71])} if rnd.random() < 0.7 else {}), "how": "x"} for part in parts]
    if status == "settled":
        t.update(payout=rnd.choice([1, 1.0, 0, 0.0, 0.5]), settled=NOW + 1, confirm=rnd.choice(["confirmed", "confirmed", "mismatch", None, "pending"]))
        if rnd.random() < 0.2:
            del t["confirm"]
    if shares and rnd.random() < 0.7:
        mark = lambda: rnd.choice([{"at": NOW, "mid": rnd.choice([0.31, 0.52, 0.777]), "move": rnd.choice([0.013, -0.027, 0.1, 0]),
                                    **({"fair_move": rnd.choice([0.011, -0.003, None])} if rnd.random() < 0.8 else {})},
                                   {"at": NOW, "mid": None, "move": 0.5}, "n/a"])
        t["markout"] = {label: mark() for _, label in m.MARKOUT_HORIZONS if rnd.random() < 0.6}
        if rnd.random() < 0.1:
            t["markout"] = None
    return t


def walked(values):
    """sim_report's total, kinds and modes as it walks them (the report's own lines, before 1.154.39 and for every other caller)."""
    return {"total": m.sim_stats(values),
            "kinds": [{"name": name, **m.sim_stats([t for t in values if t["kind"] == kind])}
                      for kind, name in m.SIM_KINDS.items() if any(t["kind"] == kind for t in values)],
            "modes": [{"name": name, **m.sim_stats([t for t in values if t["maker"] == maker])}
                      for name, maker in (("挂单", True), ("吃单", False)) if any(t["maker"] == maker for t in values)]}


def of_bot(bot):
    return walked([t for _, t in sorted(bot.sim_trades().items(), key=lambda kv: kv[1].get("opened", 0))])


def totals_of(report):
    return {k: report[k] for k in ("total", "kinds", "modes")}


def same(a, b, what):
    """Equal as written out (the digits, the order of the keys, a whole 0 where the walk gives one)."""
    assert json.dumps(a, ensure_ascii=False) == json.dumps(b, ensure_ascii=False), f"{what}: {diff(a, b)}"


def diff(a, b, at=""):
    if isinstance(a, dict) and isinstance(b, dict) and list(a) == list(b):
        return next((d for k in a if (d := diff(a[k], b[k], f"{at}.{k}"))), "")
    if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        return next((d for i, (x, y) in enumerate(zip(a, b)) if (d := diff(x, y, f"{at}[{i}]"))), "")
    return "" if a == b and type(a) is type(b) else f"{at}: {a!r} != {b!r}"


def figures_alone():
    """sim_stats_of over sim_stat_row against sim_stats, on its own."""
    rnd = random.Random(39)
    records = [record(rnd, n) for n in range(600)]
    rows = [m.sim_stat_row(t) for t in records]
    assert all(len(row) == 8 and (row[m.SIM_STAT_DONE] is None or len(row[m.SIM_STAT_DONE]) == 8) for row in rows)
    same(m.sim_stats_of(rows), m.sim_stats(records), "every record")
    full = m.sim_stats(records)
    assert full["settled"] > 200 and full["wins"] and full["losses"] and full["ties"] and full["mismatch"] and full["local"] and full["open"] and full["resting"] \
        and full["partial"] and full["expired"] and full["cancelled"] and full["open_cost"] and len(full["markout"]) == 3, full  # (every figure has something to add)
    assert full["expected"] != full["expected_fill"], "(fills at their own fair prices: the two expectations differ)"
    same(m.sim_stats_of(rows[::-1]), m.sim_stats(records[::-1]), "the other way round")
    # why the terms are kept and not the totals: a total brought up to date record by record does not come to sum()'s digits
    running = {"cost": 0.0, "payout": 0.0, "expected": 0.0}
    for t in records:
        if t["status"] == "settled":
            running["cost"] += t["price"] * t["shares"]
            running["payout"] += t["payout"] * t["shares"]
            running["expected"] += t["edge"] * t["shares"]
    assert any(running[k] != full[k] for k in running) and all(abs(running[k] - full[k]) < 1e-6 for k in running), (running, full)
    # any part of them: none, one of each status, random picks, each kind, each way
    same(m.sim_stats_of([]), m.sim_stats([]), "no record")
    assert m.sim_stats_of([])["cost"] == 0 and type(m.sim_stats_of([])["cost"]) is int and m.sim_stats_of([])["roi"] == 0.0 and m.sim_stats_of([])["markout"] == {}
    for status in ("settled", "filled", "resting", "expired", "cancelled"):
        for maker in (True, False):
            one = [record(rnd, 7, status=status, maker=maker)]
            same(m.sim_stats_of([m.sim_stat_row(t) for t in one]), m.sim_stats(one), f"one {status} record")
        part = [t for t in records if t["status"] == status]
        same(m.sim_stats_of([m.sim_stat_row(t) for t in part]), m.sim_stats(part), f"the {status} records")
    for _ in range(60):
        picked = sorted(rnd.sample(range(len(records)), rnd.randrange(1, 90)))
        same(m.sim_stats_of([rows[i] for i in picked]), m.sim_stats([records[i] for i in picked]), "a part of them")
    # a record the walk cannot add up (a settled one without its payout) is refused here too, not left out
    broken = {k: v for k, v in record(rnd, 1, status="settled").items() if k != "payout"}
    for fn in (lambda: m.sim_stats([broken]), lambda: m.sim_stat_row(broken)):
        try:
            fn()
        except KeyError:
            continue
        raise AssertionError("a settled record without a payout was added up")


async def through_the_bot():
    rnd = random.Random(1154)
    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}), m.Store(":memory:"), FM(NOW), None)
    bot.config = m.dataclasses.replace(bot.config, live_key="ab" * 32)  # a real account behind the page: the totals alone
    ways = {True: "挂", False: "吃"}
    slots = [f"m{n}|up|{ways[bool(n % 3)]}" for n in range(40)]
    # the records as the store hands them over at a start: old attempts (slot#n), then each slot's own
    first = [(f"{slot}#{k}", record(rnd, n * 7 + k, status=rnd.choice(["cancelled", "expired", "settled"]), maker=bool(n % 3))) for n, slot in enumerate(slots) for k in range(1, 9)]
    first += [(slot, record(rnd, n, maker=bool(n % 3))) for n, slot in enumerate(slots)]
    bot.store.put_many((f"sim:{tid}", t) for tid, t in first)
    calls, plain_row, plain_stats = [], m.sim_stat_row, m.sim_stats

    def counted(t):
        calls.append(1)
        return plain_row(t)
    m.sim_stat_row = counted
    try:
        n = len(bot.sim_trades())
        assert n == len(first) == 360
        index = bot.sim_kept()
        assert index.stat_rows is None and not index.stat_touched, "nothing is kept until the page asks"
        bot.sim_save([(slots[0], dict(bot.sim_kept().records[slots[0]]))])
        assert not index.stat_touched, "...nor marked: a bot nobody looks at pays nothing for it"
        same(bot.sim_report_totals(), of_bot(bot), "the first look")
        assert len(calls) == n and bot.sim_stat_builds == 1 and len(index.stat_rows) == n
        # nothing saved since: only the open records and the round's share are worked out again
        bot.SIM_STAT_AUDIT = 16
        opened = len(index.open)
        assert 0 < opened < 100, opened
        del calls[:]
        same(bot.sim_report_totals(), of_bot(bot), "nothing saved since")
        assert len(calls) == opened + 16, (len(calls), opened)
        # the report itself: the walk, with the figures checked against it (the suites: TRADE_AUDIT) — and by the figures alone
        assert m.TRADE_AUDIT
        slow = bot.sim_report(public=True)
        assert slow["private"] and slow["rows"] == [] and slow["groups"] == [] and slow["sources"] == [] and slow["blocks"] == [] and not m.TRADE_AUDIT_FAILS
        same(totals_of(slow), of_bot(bot), "the report, walked")

        def quick_report():
            bot.sim_report_cache = {}
            bot.__dict__.pop("sim_report_kept", None)
            m.TRADE_AUDIT, m.sim_stats = False, refuse
            try:
                return bot.sim_report(public=True)
            finally:
                m.TRADE_AUDIT, m.sim_stats = True, plain_stats

        def refuse(values):
            raise AssertionError("the page's totals walked every record")
        lent = index.lent = False
        quick = quick_report()
        same(quick, slow, "the report by the figures against the walk's")
        assert list(quick) == list(slow) and index.lent is lent, "(no copy of the records is taken for it: the index's lists stay its own)"
        # /sim's report and a paper bot's page are walked as before (the list of trades, the groups: every record is read anyway)
        seen = []
        m.sim_stats = lambda values: seen.append(1) or plain_stats(values)
        m.TRADE_AUDIT = False
        try:
            assert bot.sim_report(recent=10)["rows"] and seen and not bot.sim_report(recent=10)["private"]
        finally:
            m.TRADE_AUDIT, m.sim_stats = True, plain_stats

        # --- the records change as the step changes them ---
        def step(what):
            del calls[:]
            same(bot.sim_report_totals(), of_bot(bot), what)
            same(totals_of(quick_report()), of_bot(bot), what + " (the report)")
            return len(calls)
        records = bot.sim_kept().records
        # a record saved again with another result; a new record in a new market; one saved unchanged
        target = next(tid for tid, t in records.items() if t["status"] == "settled" and t["payout"] in (0, 0.0))
        bot.sim_save([(target, {**records[target], "payout": 1.0, "confirm": "mismatch"})])
        bot.sim_save([("new|up|挂", record(rnd, 999, status="resting", maker=True)), ("new2|down|吃", record(rnd, 998, status="settled", maker=False, kind="ladder"))])
        assert step("saved records") <= 3 + 2 * (len(index.open) + 16), "the three saved, and for each of the two looks the open ones and the round's share"
        assert any(row["name"] == m.SIM_KINDS["ladder"] for row in bot.sim_report_totals()["kinds"]), "(a kind seen for the first time gets its line)"
        # a slot's record moved to slot#n (a re-quote), a fresh one put in its place — the order among records opened together follows the cache's
        moved = next(tid for tid in slots if tid in records)
        was = records[moved]
        new_tid = bot.sim_rekey(moved, was)
        assert new_tid != moved and moved not in records and new_tid in records
        step("a record moved")
        bot.sim_save([(moved, record(rnd, 5, status="resting", maker=True))])
        step("the slot filled again")
        # an open record changed in place and not saved yet (a fill, a mark, a settlement the step is about to save): seen at once
        tid = next(tid for tid in index.open if records[tid]["status"] == "resting")
        records[tid].update(status="filled", shares=records[tid]["order"], fills=[{"at": NOW, "shares": records[tid]["order"], "fair": 0.71, "how": "x"}],
                            markout={"1m": {"at": NOW, "mid": 0.6, "move": 0.123, "fair_move": -0.004}})
        before = bot.sim_report_totals()["total"]["open"]
        records[tid].update(status="settled", payout=1.0, settled=NOW)
        assert bot.sim_report_totals()["total"]["open"] == before - 1
        step("an open record changed in place")
        bot.sim_save([(tid, {**records[tid], "final": True})])
        step("...and saved")
        # old attempts removed (sim_prune keeps the newest of each slot)
        bot.SIM_ATTEMPTS_KEEP = 2
        gone = bot.sim_prune()
        assert gone > 50 and len(bot.sim_kept().records) == len(index.stat_rows) - gone, gone
        assert step("old attempts removed") < 200 and len(index.stat_rows) == len(records)
        assert bot.sim_stat_builds == 1 and bot.sim_kept() is index, "all of it by the marks: the figures were never worked out afresh"
        # a closed record changed in place and never saved (nothing does that): found by the check as it goes round, put right, said
        said = []
        bot.log_limited = lambda key, text, *a, **k: said.append((key, text))
        closed = next(tid for tid, t in records.items() if t["status"] == "settled" and tid not in index.open)
        records[closed]["payout"] = 0.5 if records[closed]["payout"] != 0.5 else 1.0
        rounds = 0
        while json.dumps(bot.sim_report_totals()) != json.dumps(of_bot(bot)):
            rounds += 1
            assert rounds <= len(records) // 16 + 2, "the check never came round to it"
        assert bot.sim_stat_stale["n"] == 1 and bot.sim_stat_stale["tids"] == [closed] and said and said[-1][0] == "sim_stat" and closed in said[-1][1], (bot.sim_stat_stale, said)
        assert bot.sim_stat_builds == 1 and not m.TRADE_AUDIT_FAILS
        bot.reference_state["模拟交易"] = {"ok_at": time.time() - 2, "error": "", "error_at": 0, "ms": 900, "runs": 50}
        bot.sim_step_ms = {"决策": 100}
        text = "\n".join(bot.diag_state(NOW))
        assert f"；合计自检 1 条改了没保存（{m.stamp(NOW, seconds=False)}，如 {closed}），已重算" in text, text  # (/diag says it, beside the index's own check)
        # a record put past the index (never in this code): the figures no longer match the records key for key — all worked out again
        records["past|up|吃"] = record(rnd, 3, status="settled", maker=False)
        same(bot.sim_report_totals(), of_bot(bot), "a record put past the index")
        assert bot.sim_stat_builds == 2 and m.TRADE_AUDIT_FAILS and m.TRADE_AUDIT_FAILS[-1].startswith("sim_stat_rows"), m.TRADE_AUDIT_FAILS
        del m.TRADE_AUDIT_FAILS[:]
        del records["past|up|吃"]
        same(bot.sim_report_totals(), of_bot(bot), "...and taken out again")
        assert bot.sim_stat_builds == 3 and m.TRADE_AUDIT_FAILS
        del m.TRADE_AUDIT_FAILS[:]
        # a record written past sim_save: the records are read afresh from the store (new objects, a new index) — the figures with them
        bot.store.put("sim:late|up|吃", record(rnd, 4, status="settled", maker=False))
        same(bot.sim_report_totals(), of_bot(bot), "the records read afresh")
        fresh = bot.sim_kept()
        assert fresh is not index and index.dead and fresh.stat_rows is not None and len(fresh.stat_rows) == len(fresh.records) and bot.sim_stat_builds == 4
        # many steps of everything at once, the report asked now and then
        for i in range(120):
            records = bot.sim_kept().records
            roll = rnd.random()
            tid = rnd.choice(list(records))
            if roll < 0.45:
                bot.sim_save([(tid, record(rnd, i, maker=bool(records[tid]["maker"])))])
            elif roll < 0.6:
                bot.sim_save([(f"n{i}|up|{rnd.choice('挂吃')}", record(rnd, i))])
            elif roll < 0.8 and "#" not in tid:
                bot.sim_rekey(tid, records[tid])
            elif roll < 0.9:
                open_now = [k for k in bot.sim_kept().open if records[k]["status"] in ("resting", "filled")]
                if open_now:
                    records[rnd.choice(open_now)]["shares"] = rnd.choice([0.0, 3.3, 10])
            else:
                bot.sim_prune()
            if i % 7 == 0:
                same(bot.sim_report_totals(), of_bot(bot), f"step {i}")
        same(bot.sim_report_totals(), of_bot(bot), "the last step")
        same(totals_of(quick_report()), of_bot(bot), "the last report")
        assert bot.sim_stat_builds == 4 and not m.TRADE_AUDIT_FAILS and bot.sim_index_check(force=True) is not None
    finally:
        m.sim_stat_row, m.sim_stats, m.TRADE_AUDIT = plain_row, plain_stats, True
    # a bot with real records and no key (the key taken out, the database kept): private all the same, by the figures too
    paper = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}), m.Store(":memory:"), FM(NOW), None)
    paper.sim_save([("a|up|吃", {**record(rnd, 1, status="settled", maker=False), "live": {"state": "done", "events": []}}), ("b|up|挂", record(rnd, 2, status="resting", maker=True))])
    m.TRADE_AUDIT = False
    try:
        rep = paper.sim_report(public=True)
    finally:
        m.TRADE_AUDIT = True
    assert rep["private"] and rep["rows"] == [] and rep["total"]["trades"] == 2 and paper.sim_stat_builds == 1
    same(totals_of(rep), of_bot(paper), "real records, no key")


figures_alone()
asyncio.run(through_the_bot())
print("REPORT_TOTALS_OK")
