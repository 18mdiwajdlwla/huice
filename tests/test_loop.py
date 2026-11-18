"""1.154.20 — what holds the event loop. 11-18: the model trader's takers had become slow, and it was the pre-open mode's
39,000 records and 24,000 open orders walked over and over on the one loop everything shares. Here: the loop's own delay
is measured and named (LoopWatch, /diag: 主循环); the trader's sync digests that mode's account snapshot once a snapshot;
the figures asked before every order (the exposure, the day's P&L) are what the plain walks gave, without walking every
record ever kept; the control page's list and its pre-open block are built for what it shows.

Run: python tests/test_loop.py
"""
import asyncio
import copy
import datetime as dt
import random
import sys
import time
import types

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


class FakeApi:
    def __init__(self):
        self.jwt, self.jwt_exp, self.auth_error, self.lookup = "t", 0, "", ""
        self.markets, self.open, self.closed, self.positions_rows = {}, {}, {}, []
        self.next_id, self.orders_complete, self.created = 100, True, []

    async def ensure_auth(self): pass
    async def market(self, mid): return self.markets[str(mid)]

    async def create_order(self, body):
        self.next_id += 1
        oid, data = str(self.next_id), body["data"]
        self.created.append(oid)
        self.open[oid] = {"id": oid, "status": "OPEN", "amount": data["order"]["takerAmount"], "amountFilled": "0", "strategy": data["strategy"],
                          "order": {**data["order"]}}
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
    def __init__(self): self.usdt, self.bnb, self.allow, self.owner = 100000 * WEI, 10 ** 17, 10 ** 30, "0x8fd379246834eac74B8419FfdA202CF8051F7A03"
    async def account_owner(self, account): return self.owner
    async def usdt_balance(self, owner=None): return self.usdt
    async def bnb_balance(self, owner=None): return self.bnb
    async def allowance(self, key, owner=None): return self.allow


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def mk(slug, fair, bids, asks, at, mid):
    settle = {"key": "K" + mid, "target": "2026-11-18", "line": 24600.0, "close_ms": CLOSE}
    side = lambda rows: tuple((D(str(p)), D(str(q))) for p, q in rows)
    book = m.PredictBook("K" + mid, slug, mid, "t", side(bids), side(asks), at, None)
    return m.SimMarket(slug, "恒生指数", "close", "K" + mid, fair, book, 0.03, "", ("涨", "跌"), settle,
                       {"basis": {"fair_up": fair, "ref": 24600.0}, "sources": [{"what": "恒指期货", "source": "etnet"}], "proxy": None})


def market_json(mid):
    return {"id": int(mid), "status": "REGISTERED", "isNegRisk": False, "isYieldBearing": False, "feeRateBps": 200, "conditionId": "0x" + "ab" * 32,
            "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": f"{mid}1"}, {"name": "Down", "indexSet": 2, "onChainId": f"{mid}2"}]}


def make_bot(**env):
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
                             "SIM_WAYS": "taker", "SIM_MARKETS": "all", "LIVE": "on", "PREDICT_PRIVATE_KEY": KEY, "PREDICT_API_KEY": "k",
                             "SIM_TAKER_MAX_SHARES": "0", "SIM_TAKER_SWEEP_SHARES": "0", "SIM_TAKER_HOLD_SECONDS": "0", "SIM_OPEN_QUIET_MINUTES": "0",
                             "SIM_GROUP_USD": "0", "LIVE_TAKER_WAIT_SECONDS": "600", "LIVE_MAX_ORDER_USD": "100", "LIVE_MAX_OPEN_USD": "100000",
                             "LIVE_MAX_DAILY_LOSS_USD": "500", **env})
    bot = L.LiveBot(cfg, m.Store(":memory:"), FM(NOW), None)
    bot.live.api, bot.live.chain = FakeApi(), FakeChain()
    bot.sim_markets = lambda now: world["markets"]
    bot.sim_markets_for = lambda key, now: world["markets"]
    return bot


async def taker(bot, at, mid):
    """A step that opens a real taker order on a market of its own; its record's id."""
    slug = f"hsi-{mid}"
    bot.live.api.markets[mid] = market_json(mid)
    world["markets"] = [mk(slug, 0.70, [], [("0.58", "400")], at, mid)]
    bot.market.now, bot.sim_ran = at, -1e9
    await bot.live_prefetch(at)
    await bot.sim_step(at)
    return f"{slug}|up|吃"


# --- the loop's delay, and what held it ----------------------------------------------------------------------------------------
def hold_the_loop(seconds):
    until = time.perf_counter() + seconds
    while time.perf_counter() < until:
        sum(range(500))


async def loop_watch():
    watch, stop = m.LoopWatch(), asyncio.Event()
    assert watch.line().startswith("还没有数据") and watch.summary() is None
    task = asyncio.create_task(watch.run(stop))
    await asyncio.sleep(0.4)
    calm = watch.summary()
    assert calm["n"] >= 1 and calm["seconds"] > 0, calm
    if not calm["stalls"]:  # (a builder under load may stall this process by itself: the figures below are floors, never exact)
        assert "没有卡过 100 ms 以上" in watch.line(), watch.line()
    for _ in range(3):  # the loop held 0.3 s, three times: nothing else — a pushed book, an order's request — runs meanwhile
        hold_the_loop(0.3)
        await asyncio.sleep(0.15)
    s = watch.summary()
    assert s["stalls"] >= 3 and s["stall_s"] > 0.6 and s["max"] >= 200, s
    named = [row for row in s["by"] if "hold_the_loop" in row[0]]
    assert named and named[0][1] >= 1 and named[0][3] >= 200, s["by"]  # named by the thread beside the loop: the function that held it
    line = watch.line()
    assert "卡 100 ms 以上" in line and "hold_the_loop" in line and "最长" in line, line
    stop.set()
    await task
    assert not watch.alive
    # the arithmetic itself, on beats given by hand: three late ones under one name, one under another, the calm ones between
    by_hand = m.LoopWatch()
    t0 = time.monotonic() - 30
    by_hand.started = t0
    for k in range(100):
        began = t0 + k * 0.2
        late = {10: 0.30, 20: 0.26, 30: 0.40, 40: 0.12}.get(k, 0.001)
        if late >= 0.1:
            by_hand.caught = (began, {"preopen.status ← live.preopen_page": 3, "main.sim_stats": 1} if k != 40 else {"live.live_sync_orders": 2})
        by_hand.note(began, began + by_hand.BEAT_S + late)
    s = by_hand.summary()
    assert (s["n"], s["stalls"], round(s["stall_s"], 2), round(s["max"])) == (100, 4, 1.08, 400) and abs(s["median"] - 1.0) < 1e-6, s
    assert [(what, n, round(secs, 2), round(longest)) for what, n, secs, longest in s["by"]] == [
        ("preopen.status ← live.preopen_page", 3, 0.96, 400), ("live.live_sync_orders", 1, 0.12, 120)], s["by"]
    line = by_hand.line()
    assert ("卡 100 ms 以上 4 次、共 1.1 秒" in line and "卡在：preopen.status ← live.preopen_page 3 次共 1.0 秒（最长 400 ms）；live.live_sync_orders 1 次共 0.1 秒（最长 120 ms）" in line
            and "延迟中位 1.0 ms" in line and "最长 400 ms" in line), line
    assert 0 < by_hand.summary(15.0)["n"] < 100 and by_hand.summary(15.0)["stalls"] == 0, "a shorter span: its own beats"
    # a stack in words: this code's innermost functions, the caller after each ←; another file's function alone when none is this code's
    frame = types.SimpleNamespace(f_code=types.SimpleNamespace(co_filename="/app/preopen.py", co_name="status"), f_back=types.SimpleNamespace(
        f_code=types.SimpleNamespace(co_filename="/app/live.py", co_name="preopen_page"), f_back=types.SimpleNamespace(
            f_code=types.SimpleNamespace(co_filename="/usr/lib/python3.12/asyncio/events.py", co_name="_run"), f_back=types.SimpleNamespace(
                f_code=types.SimpleNamespace(co_filename="/app/main.py", co_name="control_json"), f_back=None))))
    assert m.LoopWatch.where(frame) == "preopen.status ← live.preopen_page ← main.control_json"
    lone = types.SimpleNamespace(f_code=types.SimpleNamespace(co_filename="/usr/lib/python3.12/json/encoder.py", co_name="iterencode"), f_back=None)
    assert m.LoopWatch.where(lone) == "encoder.iterencode" and m.LoopWatch.where(None) == "?"
    # /diag says it
    bot = make_bot()
    assert any(line.startswith("⏱️ 主循环：还没有数据") for line in bot.diag_state(NOW)), bot.diag_state(NOW)
    bot.loop_watch.note(10.0, 10.0 + 0.05 + 0.25)  # a beat a quarter second late, nothing caught for it
    bot.loop_watch.lags.clear(); bot.loop_watch.stalls.clear()
    now = time.monotonic()
    bot.loop_watch.started = now - 120
    bot.loop_watch.note(now - 1.0, now - 1.0 + 0.05 + 0.25)
    text = next(line for line in bot.diag_state(NOW) if line.startswith("⏱️ 主循环："))
    assert "卡 100 ms 以上 1 次" in text and "没抓到是哪一段" in text and "最近 2 分钟" in text, text


# --- the trader's sync: the pre-open mode's snapshot digested once ---------------------------------------------------------
async def snapshot_once():
    bot = make_bot(LIVE_ORPHAN_CANCEL="off")  # (an unrecorded order is reported here, not withdrawn)
    fake = bot.live.api
    await bot.live_prepare()
    tid = await taker(bot, NOW, "201")
    mine = bot.sim_trades()[tid]["live"]["order_id"]
    assert mine in fake.open
    # the account: the trader's order, 2,000 of the pre-open mode's, one nobody recorded
    theirs = [{"id": str(9000 + i), "status": "OPEN", "amount": str(30 * WEI), "amountFilled": "0",
               "order": {"hash": f"0xpre{i:05d}", "side": 0, "makerAmount": str(14 * WEI), "takerAmount": str(30 * WEI)}} for i in range(2000)]
    stray = {"id": "777", "status": "OPEN", "amount": str(30 * WEI), "amountFilled": "0", "market": {"id": 201},
             "order": {"hash": "0xstray", "side": 0, "makerAmount": str(14 * WEI), "takerAmount": str(30 * WEI)}}
    asked = []

    def unmanaged(rows):
        asked.append(len(rows))
        return [r for r in rows if not str(r["order"]["hash"]).startswith("0xpre")]
    fake.orders_complete = False  # this bot's own read is cut (the account holds thousands): the snapshot stands for the whole account
    bot.preopen = types.SimpleNamespace(account_rows=[dict(fake.open[mine]), *theirs, stray], account_at=NOW + 1_000, unmanaged_rows=unmanaged,
                                        request_rate=lambda: None, limited_at=(), now=lambda: NOW, alive=True, winding="")
    first = bot.preopen.account_rows
    for k in range(4):
        await bot.live_sync_orders({t: r for t, r in bot.sim_trades().items() if r["live"].get("state") == "open"}, NOW + 2_000 + k * 1_000)
    assert asked == [2002], f"that mode's records are asked about a snapshot once, not at every step: {asked}"
    held = bot.live_snapshot_held
    assert held["rows"] is first and held["n"] == 2002 and bot.live_whole[0] is held["ids"] and bot.live_whole[2] == NOW + 1_000
    assert {mine, "777", "9000", "10999"} <= bot.live_whole[0] and "0xstray" in bot.live_whole[1] and bot.live_open_ids is held["ids"]
    assert [u["id"] for u in bot.live_unknown_open] == ["777"], "the order nobody recorded is still found, the trader's own is not one"
    # an order of the trader's that left the snapshot is counted missing once a snapshot (live_counts_miss), as before
    st = {"order_id": "31337", "hash": "0xgone"}
    assert bot.live_counts_miss(st, 0) and not bot.live_counts_miss(st, 0)
    # the next snapshot: a new list — digested again, once; the stray order gone from it is no longer unrecorded
    bot.preopen.account_rows, bot.preopen.account_at = [dict(fake.open[mine]), *theirs[:1500]], NOW + 60_000
    for k in range(3):
        await bot.live_sync_orders({}, NOW + 61_000 + k * 1_000)
    assert asked == [2002, 1501] and bot.live_snapshot_held["n"] == 1501 and "777" not in bot.live_whole[0] and bot.live_unknown_open == []
    assert bot.live_counts_miss(st, 0), "a new snapshot: one more look"
    # a list changed in place (never by that mode; a test's stand-in may) is seen by its length
    bot.preopen.account_rows.append(stray)
    await bot.live_sync_orders({}, NOW + 70_000)
    assert asked == [2002, 1501, 1502] and [u["id"] for u in bot.live_unknown_open] == ["777"]
    # ...and another controller's answer is never reused for the same list
    bot.preopen = types.SimpleNamespace(account_rows=bot.preopen.account_rows, account_at=NOW + 60_000, unmanaged_rows=lambda rows: [],
                                        request_rate=lambda: None, limited_at=(), now=lambda: NOW, alive=True, winding="")
    await bot.live_sync_orders({}, NOW + 71_000)
    assert bot.live_unknown_open == []
    # this bot's own whole read needs no snapshot: its own list, as before
    fake.orders_complete, bot.preopen = True, None
    await bot.live_sync_orders({}, NOW + 80_000)
    assert bot.live_whole[0] == {mine} and bot.live_whole[2] == NOW + 80_000


# --- the figures asked before every order: the plain walks' own -----------------------------------------------------------------
def plain_exposure(bot, trades):
    return sum(bot.live_reserved(t) for t in trades.values())


def plain_pnl(bot, trades, now_ms):
    """live_daily_pnl as it walked every record (1.154.19)."""
    since = int(dt.datetime.fromtimestamp(now_ms / 1000, m.BEIJING).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    total = 0.0
    for t in trades.values():
        if not isinstance(t.get("live"), dict) or t.get("status") != "settled":
            continue
        if int(t.get("settled") or 0) >= since:
            total += (float(t["payout"]) - float(t["price"])) * float(t["shares"])
        elif revisions := t.get("revisions"):
            moved = sum(float(r.get("to") or 0) - float(r.get("from") or 0) for r in revisions if isinstance(r, dict) and int(r.get("at") or 0) >= since)
            total += min(0.0, moved) * float(t["shares"])
    return total


async def gates():
    bot = make_bot()
    await bot.live_prepare()
    tid = await taker(bot, NOW, "301")
    seed = bot.sim_trades()[tid]
    rng = random.Random(20261118)
    made = {}
    for i in range(1500):  # every kind of record a long-running bot keeps, most of them over
        t = copy.deepcopy(seed)
        t["market"] = t["slug"] = f"old-{i}"
        st = t["live"]
        st.update(order_id=str(5000 + i), hash=f"0x{i:064x}")
        kind = rng.choice(["cancelled0", "cancelled0", "cancelled0", "settled", "settled", "expired", "filled", "resting", "cancelled_part", "placing",
                           "cancelling", "unknown", "fills_pending", "paper", "settled_before", "revised", "settled_open", "odd_shares"])
        price, shares = round(rng.uniform(0.05, 0.95), 4), float(rng.choice([10, 37.5, 100, 250]))
        t.update(price=price, order=shares, shares=0.0)
        if kind == "cancelled0":
            t.update(status="cancelled"); st.update(state="done", final="cancelled")
        elif kind == "settled":
            t.update(status="settled", shares=shares, payout=float(rng.choice([0, 1, 0.5])), settled=NOW - rng.randrange(0, 9 * 3600_000)); st.update(state="done")
        elif kind == "settled_before":
            t.update(status="settled", shares=shares, payout=float(rng.choice([0, 1])), settled=NOW - 3 * m.DAY_MS); st.update(state="done")
        elif kind == "revised":  # settled before today, its result corrected today (a loss counts today) or yesterday (it does not)
            at = NOW - rng.choice([3600_000, 30 * 3600_000])
            t.update(status="settled", shares=shares, payout=0.0, settled=NOW - 2 * m.DAY_MS,
                     revisions=[{"at": at, "by": "Predict 最终结果", "from": 1.0, "to": 0.0, "note": ""}]); st.update(state="done")
        elif kind == "settled_open":  # a result came while its order still rests: settled, the order being withdrawn
            t.update(status="settled", shares=shares / 2, payout=1.0, settled=NOW - 60_000); st.update(state="cancelling")
        elif kind == "expired":
            t.update(status="expired", settled=NOW - 3600_000); st.update(state="done")
        elif kind == "filled":
            t.update(status="filled", shares=shares); st.update(state="done", final="filled")
        elif kind == "resting":
            t.update(status="resting", shares=shares / 4, maker=True); st.update(state="open")
        elif kind == "cancelled_part":
            t.update(status="cancelled", shares=shares / 2); st.update(state="done", final="cancelled")
        elif kind == "placing":
            t.update(status="resting"); st.update(state="placing")
        elif kind == "cancelling":
            t.update(status="cancelled"); st.update(state="cancelling")
        elif kind == "unknown":
            t.update(status="cancelled"); st.update(state="unknown")
        elif kind == "fills_pending":  # cancelled, its fills not read back yet: still counted (a stale mark no longer)
            t.update(status="cancelled"); st.update(state="done", fills_pending=time.time() * 1000 - rng.choice([1_000, 11 * 60_000]))
        elif kind == "odd_shares":  # a count kept as text: judged by the walk's own arithmetic, never passed by
            t.update(status="cancelled", shares="0"); st.update(state="done")
        elif kind == "paper":
            t.update(status="filled", shares=shares); t.pop("live")
        made[f"old-{i}|up|吃#{i}"] = t
    bot.sim_save(list(made.items()))
    trades = bot.sim_trades()
    assert len(trades) == 1501 and trades.taken == (bot.sim_built, bot.sim_gen)
    assert bot.live_exposure(trades) == plain_exposure(bot, trades) > 0, "the same sum, term for term"
    assert bot.live_exposure({}) == 0 and bot.live_exposure(dict(list(trades.items())[:40])) == plain_exposure(bot, dict(list(trades.items())[:40]))
    calls = []
    plain_reserved = bot.live_reserved
    bot.live_reserved = lambda t: (calls.append(1), plain_reserved(t))[1]
    bot.live_exposure(trades)
    bot.live_reserved = plain_reserved
    assert 0 < len(calls) < 900, f"only the records that may hold something are worked out ({len(calls)} of 1501)"
    # the day's settled P&L: the walk's figure, from the day's own records
    assert bot.live_daily_pnl(trades, NOW) == plain_pnl(bot, trades, NOW) != 0
    rows = bot.live_pnl_held[2]
    assert 0 < len(rows) < 600 and all(t["status"] == "settled" for t in rows), len(rows)
    assert bot.live_daily_pnl(bot.sim_trades(), NOW) == plain_pnl(bot, trades, NOW) and bot.live_pnl_held[2] is rows, "asked again with nothing written: not looked for again"
    # 1.154.22: ...nor after a write of a record that is not a settled one — an order's own record is written on its way out, and
    # each of its gates began with a walk of every record again; a settled record saved is looked for again
    resting = next((k, t) for k, t in trades.items() if t.get("status") == "resting" and isinstance(t.get("live"), dict))
    bot.sim_save([resting])
    assert bot.live_daily_pnl(bot.sim_trades(), NOW) == plain_pnl(bot, trades, NOW) and bot.live_pnl_held[2] is rows, "another record written: the day's rows stand"
    bot.sim_save([next((k, t) for k, t in trades.items() if t is rows[0])])
    assert bot.live_daily_pnl(bot.sim_trades(), NOW) == plain_pnl(bot, trades, NOW) and bot.live_pnl_held[2] is not rows, "a settled record written: looked for again"
    rows = bot.live_pnl_held[2]
    assert bot.live_daily_pnl(dict(trades), NOW) == plain_pnl(bot, trades, NOW), "a dict of the caller's own is walked whole"
    # a record settled this very moment counts before it is even saved (sim_settle): no order goes out past a loss just settled
    loser = next(t for t in trades.values() if t.get("status") == "filled" and isinstance(t.get("live"), dict))
    before = bot.live_daily_pnl(bot.sim_trades(), NOW)
    bot.sim_settle(loser, 0.0, "测试", NOW, "本地预结算")
    assert loser["status"] == "settled" and loser["payout"] == 0.0
    after = bot.live_daily_pnl(bot.sim_trades(), NOW)
    assert after == plain_pnl(bot, bot.sim_trades(), NOW) and abs(after - (before - float(loser["price"]) * float(loser["shares"]))) < 1e-9
    assert "已达 LIVE_MAX_DAILY_LOSS_USD" not in bot.live_send_block(seed, bot.sim_trades(), NOW) or after <= -500
    # a fill's price corrected on a settled record, in place: the figure follows at once (each record is judged as it stands)
    loser["price"] = float(loser["price"]) + 0.01
    assert bot.live_daily_pnl(bot.sim_trades(), NOW) == plain_pnl(bot, bot.sim_trades(), NOW) != after
    # a result revised: a revision of today on a record settled days ago
    old = next(t for t in trades.values() if t.get("status") == "settled" and int(t.get("settled") or 0) < NOW - 2 * m.DAY_MS and not t.get("revisions")
               and float(t.get("payout") or 0) == 1.0)
    old.setdefault("revisions", [])
    bot.sim_settle(old, 0.0, "更正", NOW, "Predict 最终结果")
    assert old["revisions"] and bot.live_daily_pnl(bot.sim_trades(), NOW) == plain_pnl(bot, bot.sim_trades(), NOW)
    # a record written settled (saved by whoever): the store's count changes, the day's records are looked for again
    late = copy.deepcopy(seed)
    late["market"] = late["slug"] = "late"
    late.update(status="settled", shares=100.0, payout=0.0, price=0.6, settled=NOW)
    late["live"].update(state="done", order_id="999999", hash="0x" + "9" * 64)
    bot.sim_save([("late|up|吃", late)])
    assert bot.live_daily_pnl(bot.sim_trades(), NOW) == plain_pnl(bot, bot.sim_trades(), NOW)
    assert late in bot.live_pnl_held[2]
    # a map the caller changed is no longer "the records as kept": walked whole
    view = bot.sim_trades()
    extra = copy.deepcopy(late)
    extra["live"]["order_id"] = "1000000"
    view["extra|up|吃"] = extra
    assert view.taken is None and bot.live_daily_pnl(view, NOW) == plain_pnl(bot, view, NOW) != bot.live_daily_pnl(bot.sim_trades(), NOW)
    # midnight: the next day's own records
    tomorrow = NOW + m.DAY_MS
    assert bot.live_daily_pnl(bot.sim_trades(), tomorrow) == plain_pnl(bot, bot.sim_trades(), tomorrow)
    # a write that is not the bot's own save (a record put straight into the store): the kept records are read afresh
    bot.store.put("sim:direct|up|吃", {**copy.deepcopy(late), "market": "direct"})
    assert bot.live_daily_pnl(bot.sim_trades(), NOW) == plain_pnl(bot, bot.sim_trades(), NOW)
    # the kept records read afresh (their objects are new ones): never the rows found on the reading before
    built = bot.sim_built
    bot.sim_cache = None
    fresh = bot.sim_trades()
    assert bot.sim_built == built + 1 and fresh.taken == (built + 1, bot.sim_gen)
    assert bot.live_daily_pnl(fresh, NOW) == plain_pnl(bot, fresh, NOW) and all(any(t is x for x in fresh.values()) for t in bot.live_pnl_held[2])
    # a bot made without its start (a test's) has no kept records: the walk
    bare = L.LiveBot.__new__(L.LiveBot)
    assert bare.live_daily_pnl({"a": late}, NOW) == plain_pnl(bot, {"a": late}, NOW)


# --- the control page: built for what it shows ---------------------------------------------------------------------------------
def plain_records(bot, trades, history=200):
    """control_records as it wrote out every record (1.154.19)."""
    rows = []
    for tid, t in trades.items():
        st = t.get("live") if isinstance(t.get("live"), dict) else {}
        status = t.get("status")
        group = "resting" if status == "resting" else "position" if status == "filled" else "history"
        when = int((t.get("settled") or (t.get("withdrawn") or {}).get("at") or t.get("opened") or 0) if group == "history" else (t.get("opened") or 0))
        failed = st.get("final") == "failed"
        why = str((t.get("withdrawn") or {}).get("why") or st.get("error") or (t.get("note") if status in {"expired", "cancelled"} else "") or "")
        rows.append({"id": tid, "opened": m.stamp(int(t.get("opened") or 0), seconds=False), "when": when, "item": str(t.get("item") or ""),
                     "label": str(t.get("label") or ""), "side": str(t.get("side") or ""), "maker": bool(t.get("maker")),
                     "price": float(t.get("price") or 0), "order": float(t.get("order") or 0), "shares": float(t.get("shares") or 0),
                     "status": "失败" if failed else L.sim_status(t), "state": m.sim_state(t), "group": group, "why": why, "failed": failed,
                     "cost_pending": t.get("cost_confirmed") is False and float(t.get("shares") or 0) > 0,
                     "order_id": str(st.get("order_id") or ""), "live_state": str(st.get("state") or ""),
                     "cancellable": status == "resting" and st.get("state") in {"open", "placing"} and bool(st.get("order_id")),
                     "url": bot.sim_url(t)})
    rows.sort(key=lambda r: r["when"], reverse=True)
    kept = [r for r in rows if r["group"] != "history"] + [r for r in rows if r["group"] == "history"][:history]
    kept.sort(key=lambda r: r["when"], reverse=True)
    return kept


async def control_page():
    bot = make_bot()
    await bot.live_prepare()
    tid = await taker(bot, NOW, "401")
    seed = bot.sim_trades()[tid]
    rng = random.Random(7)
    made = {}
    for i in range(900):
        t = copy.deepcopy(seed)
        t["market"] = t["slug"] = f"old-{i}"
        t["opened"] = NOW - rng.randrange(0, 50) * 60_000  # many of them at the same minute: the order among equals is the records'
        kind = rng.choice(["history", "history", "history", "position", "resting"])
        if kind == "history":
            t.update(status=rng.choice(["settled", "cancelled", "expired"]), settled=rng.choice([0, t["opened"] + 60_000]), shares=0.0, payout=0.0)
            t["live"].update(state="done", final=rng.choice(["cancelled", "failed", "filled"]))
        elif kind == "position":
            t.update(status="filled", shares=100.0); t["live"].update(state="done", final="filled")
        made[f"old-{i}|up|吃#{i}"] = t
    bot.sim_save(list(made.items()))
    trades = bot.sim_trades()
    plain = plain_records(bot, trades)
    assert bot.control_records(trades) == plain and len(plain) < 901 and sum(1 for r in plain if r["group"] == "history") == 200
    assert bot.control_records(trades, history=7) == plain_records(bot, trades, 7)
    states = []
    real_state = m.sim_state
    m.sim_state = lambda t: (states.append(1), real_state(t))[1]
    try:
        bot.control_records(trades)
    finally:
        m.sim_state = real_state
    assert len(states) == len(plain), "only the records shown are written out"
    # the pre-open mode's block: its status is worked out every PREOPEN_PAGE_SECONDS at the most, and afresh after an action of the page's
    built = []
    stand_in = dict(risk=lambda: 12.5, alive=True, winding="", daily_pnl=lambda: 0.0, account_rows=None, account_at=0, limited_at=(), request_rate=lambda: None,
                    now=lambda: NOW, filled_by_outcome=lambda days=3: {}, manual_pause=False, error="")
    bot.preopen = types.SimpleNamespace(status=lambda: (built.append(1), f"状态 {len(built)}")[1], **stand_in)
    first = bot.control_payload()["preopen"]
    assert first == {"status": "状态 1", "reserved_usd": 12.5} and bot.control_payload()["preopen"] == first and built == [1]
    first["status"] = "改了"  # (the page's copy is its own)
    assert bot.control_payload()["preopen"]["status"] == "状态 1"
    held = bot.preopen_page_held
    bot.__dict__["preopen_page_held"] = (held[0], held[1] - bot.PREOPEN_PAGE_SECONDS - 1, held[2])  # ...fifteen seconds on
    assert bot.control_payload()["preopen"]["status"] == "状态 2" and len(built) == 2
    assert (await bot.control_action({"action": "没有这个动作"}))["ok"] is False  # whatever the page asked for, done or refused
    assert "preopen_page_held" not in bot.__dict__ and bot.control_payload()["preopen"]["status"] == "状态 3"
    other = types.SimpleNamespace(status=lambda: "另一个", **{**stand_in, "risk": lambda: 0.0})
    bot.preopen = other  # the mode started again: never the copy of the controller before
    assert bot.control_payload()["preopen"]["status"] == "另一个"


async def the_rule_taker_first():
    """1.154.23 — "每天凌晨 00:59 分开始规则吃单的优先级就要调到最高". Around a makers' moment the rule taker says how long the
    rest of the bot keeps out of the event loop's way (rules.RuleTaker.quiet_ms, the bot's rules_quiet): the model trader's
    step — tens of milliseconds of the loop in one piece — waits those seconds out, STEP_QUIET_MAX_S at the most since its
    last run, and so do the minute's positions check and the index's own check; the rule taker's requests have threads of
    their own (core.first_pool), never in the shared pool's queue."""
    import threading
    bot = make_bot()
    await bot.live_prepare()
    await taker(bot, NOW, "101")
    assert bot.rules_quiet() == 0.0, "no rule taker: nobody waits"
    quiet = [7_000]
    bot.rules = types.SimpleNamespace(running=False, quiet_ms=lambda: quiet[0], recs={})
    assert bot.rules_quiet() == 0.0, "one that is not running: nobody waits"
    bot.rules.running = True
    assert bot.rules_quiet() == 7.0
    # 1.154.24: a price that moved is told to the rule taker first (its windows in their course hang on the index: looked at
    # in the turn that brought it), then to the express lane as ever — the taker's failure its own
    told = []
    bot.rules.priced = lambda keys: told.append((list(keys), set(bot.price_dirty)))
    bot.priced(["XAU-15M"])
    assert told == [(["XAU-15M"], set())] and bot.price_dirty == {"XAU-15M"} and bot.price_look is not None, told
    bot.rules.priced = lambda keys: 1 / 0
    bot.priced({"K101"})
    assert bot.price_dirty == {"XAU-15M", "K101"}
    bot.rules.running = False
    bot.rules.priced = lambda keys: told.append("not running")
    bot.priced(["K101"])
    assert len(told) == 1
    bot.rules.running = True
    await bot.price_look
    del bot.rules.priced
    # the step: due, and it waits; the express lane does not ask
    due = lambda ago: time.monotonic() - bot.config.sim_seconds - ago
    bot.sim_ran = at = due(0.5)
    assert await bot.sim_step(NOW + 10_000) is False and bot.sim_ran == at and bot.live_step_waits == 1, "not run: the rule taker's seconds"
    world["markets"] = [mk("hsi-101", 0.70, [], [("0.90", "400")], NOW + 10_000, "101")]
    bot.market.now = NOW + 10_000
    await bot.sim_express_look("K101", NOW + 10_000)  # (a pushed book is still judged)
    bot.sim_ran = time.monotonic() - bot.STEP_QUIET_MAX_S - 1
    assert await bot.sim_step(NOW + 10_000) and bot.live_step_waits == 1, "never longer than STEP_QUIET_MAX_S since its last run"
    quiet[0] = 0
    bot.sim_ran = due(0.5)
    assert await bot.sim_step(NOW + 15_000) and bot.live_step_waits == 1, "the seconds over: as ever"
    # the minute's positions check and the index's own check wait too
    quiet[0] = 7_000
    bot.live_late_checked = 0
    await bot.live_late_fills(NOW + 200_000)
    assert bot.live_late_checked == 0
    due_check = lambda: time.monotonic() - bot.SIM_INDEX_CHECK_SECONDS - 1  # (its minute over — whatever the host's clock reads)
    bot.sim_index_checked, checks = due_check(), bot.sim_index_checks
    assert bot.sim_index_check() == [] and bot.sim_index_checks == checks and bot.sim_index_check(force=True) == [] and bot.sim_index_checks == checks + 1
    quiet[0] = 0
    await bot.live_late_fills(NOW + 200_000)
    bot.sim_index_checked = due_check()
    assert bot.live_late_checked == NOW + 200_000 and bot.sim_index_check() == [] and bot.sim_index_checks == checks + 2
    bot.rules.quiet_ms = lambda: 1 / 0
    assert bot.rules_quiet() == 0.0, "a figure that cannot be had keeps nobody waiting"
    # threads of the rule taker's own
    pool = m.first_pool()
    assert pool is m.first_pool() and pool._max_workers == m.FIRST_POOL_WORKERS and pool is not m.predict_pool()
    name = lambda: threading.current_thread().name
    assert (await m.predict_blocking(name, pool=pool)).startswith("predict-first") and (await m.predict_blocking(name)).startswith("predict_")
    seen = []

    async def blocking(fn, *args, **kwargs):
        seen.append(kwargs)
        return b'{"success": true}'
    real, m.predict_blocking = m.predict_blocking, blocking
    try:
        api = L.PredictApi("https://x.invalid/v1", "k", None)
        await api.call("GET", "/markets/1")
        api.pool = pool
        await api.call("GET", "/markets/1")
    finally:
        m.predict_blocking = real
    assert seen == [{}, {"pool": pool}], seen
    import rules as R
    owner = types.SimpleNamespace(config=bot.config, market=bot.market, stopping=asyncio.Event(), store=m.Store(":memory:"), live=bot.live)
    assert R.RuleTaker(owner).api.pool is pool, "the rule taker's client: on them"


async def main():
    await loop_watch()
    await snapshot_once()
    await gates()
    await control_page()
    await the_rule_taker_first()


asyncio.run(main())
print("LOOP_OK")
