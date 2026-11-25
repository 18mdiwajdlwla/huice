"""1.154.41 — the views of the kept records (Bot.sim_trades → TradeMap.of) no longer copy them. A view was a copy of the
whole map, made at once: 16,000 entries, a third of a millisecond to make and as much to throw away, at every express look
(some 135 a second), nine times a step and seven on the way of one order. It now reads the kept records in place
(main.LazyTradeMap) and takes its copy at the first moment the two could differ — its holder changes it, or the kept
records' keys are about to change (TradeIndex.detach) — so it holds at every moment what the copy would hold.

Here: a view against the copy made at once, through any sequence of saves, moves, removals and changes of the views
themselves, by everything Python asks of a mapping; the copies counted (none for a view that is only read, none for a
record saved again under its key); the tests' own check (every view beside its copy, in every suite) shown to catch a
kept map changed behind a view's back; the switch back (SIM_VIEW_COPY); and through the bot — an express look and an
order's gates over thousands of records without one copy.

Run: python tests/test_views.py
"""
import asyncio
import copy
import datetime as dt
import gc
import random
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L

D = m.D
WEI = 10 ** 18
KEY = "0x" + "a" * 64
BJ = lambda mo, d, h, mi=0: int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
NOW = BJ(11, 18, 10, 0)
CLOSE = BJ(11, 18, 16, 10)
world = {"markets": []}


def record(i: int, rnd: random.Random) -> dict:
    status = rnd.choice(["filled", "resting", "cancelled", "cancelled", "expired", "settled", "settled"])
    t = {"market": f"m{rnd.randrange(12)}", "kind": "close", "key": "K", "driver": f"D{rnd.randrange(4)}", "side": rnd.choice(["up", "down"]), "status": status,
         "shares": rnd.choice([0.0, 5.0, 100.0]), "order": 100.0, "price": 0.5, "opened": 1000 + i}
    if status == "settled":
        t["final"] = {"up": 1.0}
    if rnd.random() < 0.6:
        t["live"] = {"state": rnd.choice(["done", "done", "open", "cancelling"]), "order_id": str(5000 + i), "hash": f"0x{i:064x}", "events": []}
    return t


def same(view, model: dict, why: str) -> None:
    """A view is the copy made at once, by every way a mapping is read."""
    keys = list(model)
    assert len(view) == len(model) and list(view) == keys and list(view.keys()) == keys and [k for k in view] == keys, why
    assert all(a is b for a, b in zip(view.values(), model.values())) and all(k == q and a is b for (k, a), (q, b) in zip(view.items(), model.items())), why
    assert all(view[k] is model[k] and view.get(k) is model[k] and k in view for k in keys[:40] + keys[-40:]), why
    assert "no-such-key" not in view and view.get("no-such-key") is None and view.get("no-such-key", 7) == 7, why
    assert view == model and model == view and not (view != model) and not (model != view) and bool(view) == bool(model), why
    assert dict(view) == model and list(dict(view)) == keys and {**view} == model and list(reversed(view)) == keys[::-1] and sorted(view) == sorted(model), why
    assert (view | {"zz": 1}) == (model | {"zz": 1}) and ({"zz": 1} | view) == ({"zz": 1} | model) and repr(view) == repr(model), why
    try:
        view["no-such-key"]
    except KeyError:
        pass
    else:
        raise AssertionError(why)


def the_view_is_the_copy() -> None:
    """Random saves, moves and removals of the kept records, and random changes of the views themselves: every view alive —
    whether it has copied yet or not — holds what a copy made when it was taken, changed the same way, holds."""
    rnd = random.Random(41)
    records = {f"t{i}": record(i, rnd) for i in range(300)}
    index = m.TradeIndex(records)
    live: list[tuple[m.TradeMap, dict]] = []  # (view, the plain dict copied when it was taken and changed as the view was)
    n = 300
    made_before, copied_before = m.LAZY_VIEWS["made"], m.LAZY_VIEWS["copied"]
    for step in range(1200):
        what = rnd.randrange(16)
        if what < 4 or not live:
            view = m.TradeMap.of(index)
            assert type(view) is m.LazyTradeMap and view.kept() is index
            live.append((view, dict(records)))
            if len(live) > 8:
                del live[rnd.randrange(len(live) - 1)]  # (a handful alive at a time, of every age)
        elif what == 4:  # a record saved again under its key: nothing a view holds changes
            tid = rnd.choice(list(records))
            records[tid]["shares"] = float(step)
            index.put(tid, records[tid])
        elif what == 5:  # a new record saved
            n += 1
            index.put(f"t{n}", record(n, rnd))
        elif what == 6:  # another record under a key the map has
            tid = rnd.choice(list(records))
            index.put(tid, record(step, rnd))
        elif what == 7 and len(records) > 50:  # removed (sim_prune), or moved to tid#n (sim_rekey)
            tid = rnd.choice(list(records))
            if rnd.random() < 0.5:
                index.drop(tid)
            else:
                n += 1
                index.move(tid, f"{tid}#{n}", records[tid])
        elif what == 8:
            del live[rnd.randrange(len(live))]
        else:  # a view changed by its holder, as a plain dict is
            view, model = rnd.choice(live)
            change = rnd.randrange(11)
            if change == 0:
                key = f"own{step}"
                view[key] = model[key] = record(step, rnd)
            elif change == 1 and model:
                key = rnd.choice(list(model))
                view[key] = model[key]  # the same record under its key: no copy for that
            elif change == 2 and model:
                key = rnd.choice(list(model))
                value = record(step, rnd)
                view[key] = model[key] = value
            elif change == 3 and model:
                key = rnd.choice(list(model))
                del view[key]; del model[key]
            elif change == 4 and model:
                key = rnd.choice(list(model))
                assert view.pop(key) is model.pop(key)
            elif change == 5:
                assert view.pop("no-such-key", 5) == 5 == model.pop("no-such-key", 5)
                try:
                    view.pop("no-such-key")
                except KeyError:
                    pass
                else:
                    raise AssertionError("pop of a key it has not")
            elif change == 6 and model:
                assert view.popitem() == model.popitem()
            elif change == 7 and model:
                key = rnd.choice(list(model))
                assert view.setdefault(key, {"x": 1}) is model.setdefault(key, {"x": 2})
            elif change == 8:
                value = record(step, rnd)
                assert view.setdefault(f"new{step}", value) is model.setdefault(f"new{step}", value)
            elif change == 9:
                more = {f"u{step}": record(step, rnd), **({rnd.choice(list(model)): record(step + 1, rnd)} if model else {})}
                view.update(more); model.update(more)
                view.update({}); view.update()
            elif rnd.random() < 0.05:
                view.clear(); model.clear()
        if step % 2 == 0:
            for view, model in live:
                same(view, model, f"step {step}")
    for view, model in live:
        same(view, model, "the end")
    lazy = m.TradeMap.of(index)
    assert type(lazy.copy()) is m.TradeMap and lazy.copy() == records and type(lazy) is m.LazyTradeMap, "a copy asked for is a copy; the view reads on"
    made, copied = m.LAZY_VIEWS["made"] - made_before, m.LAZY_VIEWS["copied"] - copied_before
    assert made > 250 and 0 < copied <= made, (made, copied)
    assert not m.TRADE_AUDIT_FAILS, m.TRADE_AUDIT_FAILS[:3]


def copies_only_when_needed() -> None:
    rnd = random.Random(5)
    records = {f"t{i}": record(i, rnd) for i in range(2000)}
    index = m.TradeIndex(records)
    copied = lambda: m.LAZY_VIEWS["copied"]
    # read in every way, saved again under their keys, their records changed in place: no copy
    views, before = [m.TradeMap.of(index) for _ in range(50)], copied()
    for view in views:
        assert len(view) == 2000 and "t7" in view and view["t7"] is records["t7"] and len(view.of_market("m3")) > 0 and view.open_items() is not None
        assert sum(1 for _ in view.values()) == 2000 and view.kept() is index and len(view.ids_of_market("m3")) == len(view.of_market("m3"))
        view["t9"] = records["t9"]
        assert view.taken is None, "(the same record put back: the view counts as changed, as before — and still reads on)"
    for tid in ("t1", "t500", "t1999"):
        records[tid]["status"] = "filled"
        index.put(tid, records[tid])
    assert copied() == before and all(type(v) is m.LazyTradeMap for v in views), "a record saved again under its key told no view"
    # the kept records' keys about to change: every view alive takes its copy first — once; the ones gone cost nothing
    gone = [m.TradeMap.of(index) for _ in range(30)]
    del gone
    gc.collect()
    index.put("brand-new", record(1, rnd))
    assert copied() == before + 50 and all(type(v) is m.TradeMap and "brand-new" not in v and len(v) == 2000 for v in views), copied() - before
    index.put("another", record(2, rnd)); index.remove("t5"); index.drop("t6")
    assert copied() == before + 50, "views that have their copy are told nothing more"
    assert all("t5" in v and "t6" in v and "another" not in v for v in views) and "t5" not in records
    # a view changed by its holder copies for itself alone
    a, b = m.TradeMap.of(index), m.TradeMap.of(index)
    a["mine"] = {"market": "m1", "status": "resting"}
    assert copied() == before + 51 and type(a) is m.TradeMap and type(b) is m.LazyTradeMap and "mine" not in b and "mine" not in records
    assert [k for k, _ in a.open_items()][-1] == "mine" and a.kept() is None and b.kept() is index
    # the list of views does not grow with the views handed out
    for _ in range(3000):
        m.TradeMap.of(index)
    assert len(index.views) <= 257, len(index.views)
    # the records read afresh or replaced (Bot.sim_cache set): the views of the old ones keep them as they were
    bot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}), m.Store(":memory:"), FM(NOW), None)
    bot.sim_save([(f"m{i}|up|吃", {"v": 2, "market": f"m{i}", "slug": f"m{i}", "kind": "close", "key": "K", "side": "up", "status": "settled", "shares": 1.0, "order": 1.0,
                                  "price": 0.5, "payout": 1.0, "opened": 1 + i, "settled": 2, "final": {"up": 1.0}, "confirm": "confirmed"}) for i in range(20)])
    held = bot.sim_trades()
    old = bot.sim_cache
    assert type(held) is m.LazyTradeMap and held._src is old
    bot.sim_cache = None
    assert type(held) is m.TradeMap and len(held) == 20
    old.clear()  # (whatever becomes of the old dict)
    assert len(held) == 20 and len(bot.sim_trades()) == 20
    bot.store.close()
    assert not m.TRADE_AUDIT_FAILS, m.TRADE_AUDIT_FAILS[:3]


def the_audit_catches_a_change_behind_a_view() -> None:
    """What the suites' check is for: the kept map changed past the index (no code of the bot's does) while a view reads it."""
    assert m.TRADE_AUDIT, "(tests/offline.py turns the audit on for every suite)"
    rnd = random.Random(9)
    records = {f"t{i}": record(i, rnd) for i in range(50)}
    index = m.TradeIndex(records)
    notes, error = [], m.LOG.error
    m.LOG.error = lambda *a, **k: notes.append(a)
    try:
        view = m.TradeMap.of(index)
        records["behind"] = record(99, rnd)  # past TradeIndex.put: no view was told
        assert "behind" in view, "(the view reads the kept records: this is what must never happen)"
        view["x"] = {"market": "m1", "status": "resting"}  # it takes its copy now: compared with the one made at once
        assert len(m.TRADE_AUDIT_FAILS) == 1 and m.TRADE_AUDIT_FAILS[0].startswith("lazy view: at its copy"), m.TRADE_AUDIT_FAILS
        view = m.TradeMap.of(index)
        del records["t3"]
        del view  # ...or at its end, when it never copied
        gc.collect()
        assert len(m.TRADE_AUDIT_FAILS) == 2 and m.TRADE_AUDIT_FAILS[1].startswith("lazy view: at its end"), m.TRADE_AUDIT_FAILS
        other = m.TradeMap.of(index)
        records["t4"] = record(100, rnd)  # another record under a key it has
        del other
        gc.collect()
        assert len(m.TRADE_AUDIT_FAILS) == 3 and len(notes) == 3
    finally:
        m.LOG.error = error
        m.TRADE_AUDIT_FAILS.clear()  # (made on purpose: not this suite's failure)


def the_switch_back() -> None:
    rnd = random.Random(11)
    records = {f"t{i}": record(i, rnd) for i in range(100)}
    index = m.TradeIndex(records)
    assert not m.TRADE_VIEWS_COPY, "the default: a view reads the kept records"
    made = m.LAZY_VIEWS["made"]
    m.TRADE_VIEWS_COPY = True  # SIM_VIEW_COPY=on
    try:
        view = m.TradeMap.of(index)
        assert type(view) is m.TradeMap and view == records and view.kept() is index and view.open_items() is not None and m.LAZY_VIEWS["made"] == made
        index.put("new", record(1, rnd))
        assert "new" not in view and len(view) == 100
    finally:
        m.TRADE_VIEWS_COPY = False
    assert type(m.TradeMap.of(index)) is m.LazyTradeMap


# --- through the bot ------------------------------------------------------------------------------------------------------------
class FakeApi:
    def __init__(self):
        self.jwt, self.jwt_exp, self.auth_error, self.lookup = "t", 0, "", ""
        self.markets, self.open, self.positions_rows = {}, {}, []
        self.next_id, self.orders_complete = 100, True

    async def ensure_auth(self): pass
    async def market(self, mid): return self.markets[str(mid)]

    async def create_order(self, body):
        self.next_id += 1
        oid, data = str(self.next_id), body["data"]
        self.open[oid] = {"id": oid, "status": "OPEN", "amount": data["order"]["takerAmount"], "amountFilled": "0", "strategy": data["strategy"], "order": {**data["order"]}}
        return {"order_id": oid, "hash": data["order"]["hash"], "code": None}

    async def remove_orders(self, ids):
        removed = [i for i in ids if self.open.pop(i, None) is not None]
        return {"removed": removed, "noop": [i for i in ids if i not in removed]}

    async def orders(self, status="OPEN", **params):
        return [dict(r) for r in self.open.values()] if status == "OPEN" else []

    async def order(self, oid, hash_=""):
        row = self.open.get(oid)
        return dict(row) if row is not None else None

    async def positions(self): return self.positions_rows


class FakeChain:
    def __init__(self): self.owner = "0x8fd379246834eac74B8419FfdA202CF8051F7A03"
    async def account_owner(self, account): return self.owner
    async def usdt_balance(self, owner=None): return 100000 * WEI
    async def bnb_balance(self, owner=None): return 10 ** 17
    async def allowance(self, key, owner=None): return 10 ** 30


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def mk(slug, fair, bids, asks, at, mid):
    settle = {"key": "K" + mid, "target": "2026-11-18", "line": 24600.0, "close_ms": CLOSE}
    side = lambda rows: tuple((D(str(p)), D(str(q))) for p, q in rows)
    book = m.PredictBook("K" + mid, slug, mid, "t", side(bids), side(asks), at, None)
    return m.SimMarket(slug, "恒生指数", "close", "K" + mid, fair, book, 0.03, "", ("涨", "跌"), settle,
                       {"basis": {"fair_up": fair, "ref": 24600.0}, "sources": [{"what": "恒指期货", "source": "etnet"}], "proxy": None})


def make_bot(**env):
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
                             "SIM_WAYS": "taker", "SIM_MARKETS": "all", "LIVE": "on", "PREDICT_PRIVATE_KEY": KEY, "PREDICT_API_KEY": "k",
                             "SIM_TAKER_MAX_SHARES": "0", "SIM_TAKER_SWEEP_SHARES": "0", "SIM_TAKER_HOLD_SECONDS": "0", "SIM_OPEN_QUIET_MINUTES": "0",
                             "LIVE_TAKER_WAIT_SECONDS": "600", "LIVE_MAX_ORDER_USD": "100", "LIVE_MAX_OPEN_USD": "100000",
                             "LIVE_MAX_DAILY_LOSS_USD": "500", **env})
    bot = L.LiveBot(cfg, m.Store(":memory:"), FM(NOW), None)
    bot.live.api, bot.live.chain = FakeApi(), FakeChain()
    bot.sim_markets = lambda now: world["markets"]
    bot.sim_markets_for = lambda key, now: world["markets"]
    return bot


def market_of(bot, mid):
    bot.live.api.markets[mid] = {"id": int(mid), "status": "REGISTERED", "isNegRisk": False, "isYieldBearing": False, "feeRateBps": 200, "conditionId": "0x" + "ab" * 32,
                                 "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": f"{mid}1"}, {"name": "Down", "indexSet": 2, "onChainId": f"{mid}2"}]}


async def the_bot_reads_without_copies() -> None:
    bot = make_bot()
    await bot.live_prepare()
    market_of(bot, "101")
    world["markets"] = [mk("hsi-101", 0.70, [], [("0.58", "400")], NOW, "101")]
    bot.market.now, bot.sim_ran = NOW, -1e9
    await bot.live_prefetch(NOW)
    await bot.sim_step(NOW)
    tid = "hsi-101|up|吃"
    seed, old = bot.sim_trades()[tid], {}
    assert seed["live"]["state"] == "open" and len(bot.live.api.open) == 1
    for i in range(4000):  # the history kept beside it
        t = copy.deepcopy(seed)
        t["market"] = t["slug"] = f"old-{i // 4}"
        t["live"].update(state="done", order_id=str(900000 + i), hash="0x" + f"{i:064x}")
        t.update(status="cancelled", shares=0.0) if i % 10 < 6 else t.update(status="settled", shares=100.0, payout=1.0, settled=NOW - 3600_000, confirm="confirmed", final={"up": 1.0, "name": "Up", "status": "RESOLVED"})
        old[f"old-{i // 4}|up|吃#{i % 4 + 1}"] = t
    bot.sim_save(list(old.items()))
    assert len(bot.sim_trades()) == 4001
    copied = lambda: m.LAZY_VIEWS["copied"]
    # an express look with nothing to do, the order's gates, the money reserved, the lists of what to send and to cancel: read alone
    before = copied()
    world["markets"] = [mk("hsi-101", 0.70, [], [("0.90", "400")], NOW + 20_000, "101")]  # (nothing to take at 90¢)
    bot.market.now = NOW + 20_000
    for _ in range(50):
        assert await bot.sim_express_look("K101", NOW + 20_000) == []
        assert bot.live_exposure(bot.sim_trades()) > 0
        bot.live_send_block(seed, bot.sim_trades(), NOW + 20_000)
        bot.live_cancels_unsent()
        await bot.live_place_pending(NOW + 20_000)
        bot.sim_group_room(bot.sim_trades(), world["markets"][0], "up", 0.58, 10.0)
    assert copied() == before, f"{copied() - before} copies of the records for looks that changed nothing"
    # a step with nothing to do: its own view, held across its awaits, and the sync's — none copied (no key came or went)
    for i in range(5):
        bot.market.now, bot.sim_ran = NOW + 25_000 + i * 5000, -1e9
        world["markets"] = [mk("hsi-101", 0.70, [], [("0.90", "400")], bot.market.now, "101")]
        await bot.sim_step(bot.market.now)
    assert copied() == before, f"{copied() - before} copies in five steps that changed no key"
    # a second order: the step puts its record in its view (one copy, the step's own), the save tells the views alive
    market_of(bot, "102")
    at = NOW + 60_000
    world["markets"] = [mk("hsi-102", 0.70, [], [("0.58", "400")], at, "102")]
    bot.market.now, bot.sim_ran = at, -1e9
    await bot.live_prefetch(at)
    await bot.sim_step(at)
    assert bot.sim_trades()["hsi-102|up|吃"]["live"]["state"] == "open" and len(bot.live.api.open) == 2
    assert 1 <= copied() - before <= 4, f"{copied() - before} copies for one order (seven views were taken on its way before 1.154.41, each a copy)"
    # /diag says how often a view had to copy
    import time as _time
    bot.reference_state["模拟交易"] = {"ok_at": _time.time() - 2, "error": "", "error_at": 0, "ms": 900, "runs": 50}
    diag = "\n".join(bot.diag_state(at))
    told = __import__("re").search(r"；记录表取用 ([\d,]+) 次、其中 ([\d,]+) 次要整表复制", diag)
    assert told and 0 < int(told[2].replace(",", "")) < int(told[1].replace(",", "")) <= m.LAZY_VIEWS["made"], diag
    m.TRADE_VIEWS_COPY = True
    try:
        assert "；记录表每次取用都整表复制（SIM_VIEW_COPY=on）" in "\n".join(bot.diag_state(at))
    finally:
        m.TRADE_VIEWS_COPY = False
    assert not m.TRADE_AUDIT_FAILS, m.TRADE_AUDIT_FAILS[:3]
    bot.store.close()


async def main():
    the_view_is_the_copy()
    copies_only_when_needed()
    the_audit_catches_a_change_behind_a_view()
    the_switch_back()
    await the_bot_reads_without_copies()
    assert not m.TRADE_AUDIT_FAILS, m.TRADE_AUDIT_FAILS[:3]


asyncio.run(main())
print("VIEWS_OK")
