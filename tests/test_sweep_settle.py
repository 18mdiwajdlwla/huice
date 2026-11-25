"""1.154.50 — a sweep order's own settle for its moves up (SIM_MAKER_SWEEP_SETTLE_SECONDS, 30 by default).

A sweep order (SIM_MAKER_DEEP_ONLY: the front of the book has no edge) rests a cent over the best bid of anyone else under
the threshold price, and is re-quoted when that bid changes — once its new place has held the follow's settle, two seconds.
11-25 21:32, /diag's first 重挂明细: 438 re-quotes in six minutes on 11 slots (the daily cards, after hours), 421 of them
"下面的买单变了，击穿单改挂"; four slots at 56–67 each — one every 5.6 s, which is this bot's two seconds and its cancel and
order, then somebody else's two seconds and theirs: two quoters by the same rule, each a cent over the other up to the
threshold, down again, up again. A re-quote is three requests on the bot's key; these took all of it.

Here: the setting (its default, its bounds, the control page, the record's version); a sweep order's move up over
somebody's bid waits it — never less than the follow's settle, 0 = the follow's — its clock starting anew when the place
changes; every move down (the bid under it gone, the threshold come down under it) goes by the follow's two seconds as
before, what the mode's line says at once, the move to the front by the follow's too; the whole way through the trader's
step; the two quoters, by the rule itself (Bot.sweep_price): a re-quote every 5.6 s each at two seconds against two,
one every half minute with thirty against two — the other's order then the one in front; and the real trader against such
a quoter for ten minutes: the cancels and orders that reach Predict, a sixth of what they were. Entirely offline.

Run: python tests/test_sweep_settle.py
"""
import asyncio, pathlib, sys
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

# test_trading's fixtures (book, hsi, FM, NOW …) without running its suites: only the definitions before "async def run()"
_src = (ROOT / "tests" / "test_trading.py").read_text(encoding="utf-8").split("\nasync def run():")[0]
T: dict = {"__file__": str(ROOT / "tests" / "test_trading.py")}
exec(compile(_src, "test_trading_head", "exec"), T)
book, hsi, FM, NOW, HSI_SLUG, step, world = T["book"], T["hsi"], T["FM"], T["NOW"], T["HSI_SLUG"], T["step"], T["world"]

ENV = {"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SIM_WAYS": "maker", "SIM_MARKETS": "all", "SIM_MAKER_POINTS": "off",
       "SIM_MAKER_DEEPER": "off", "SIM_MAKER_FLOOR_SHARES": "0", "SIM_MAKER_DEEP_CENTS": "25", "SIM_MAKER_DEEP_ONLY": "on", "SIM_MAKER_AFTER_HOURS_CENTS": "10"}
ASKS = (("0.53", "1720"), ("0.99", "1000"))
BASE = (("0.48", "3430"), ("0.01", "1000"))  # the front (no edge there at a fair price of 50¢) and a bid at 1¢: the sweep order goes to 2¢
TID = f"{HSI_SLUG}|up|挂"


def card(bids=BASE, at=NOW, fair=0.50):
    """The card before its open (the after-hours rules too), its book as given."""
    return m.dataclasses.replace(hsi(fair, list(bids), list(ASKS), at), in_session=False)


def make(**env) -> m.Bot:
    return m.Bot(m.Config.from_env({**ENV, **env}), m.Store(":memory:"), FM(NOW), None)


def swept(bot: m.Bot) -> dict:
    """A sweep order of the bot's, as it places one on BASE: 100 at 2¢."""
    trades, changed = m.TradeMap(), []
    bot.sim_quote(card(), trades, NOW, changed)
    placed = {k: (t["price"], t.get("sweep")) for k, t in changed}
    assert placed == {TID: (0.02, True), f"{HSI_SLUG}|down|挂": (0.02, True)}, placed  # (a sweep order on each side: the other side's book mirrors this one's)
    return trades[TID]


def the_setting() -> None:
    bot = make()
    c = bot.config
    assert (c.sim_maker_sweep_settle, c.sim_maker_follow_settle, bot.sim_sweep_settle()) == (30.0, 2.0, 30.0)
    assert bot.control_value("SIM_MAKER_SWEEP_SETTLE_SECONDS") == "30" and "SIM_MAKER_SWEEP_SETTLE_SECONDS" in m.CONTROL_KEY_SET
    for bad in ("601", "-1", "abc"):
        try:
            m.Config.from_env({**ENV, "SIM_MAKER_SWEEP_SETTLE_SECONDS": bad})
        except ValueError:
            continue
        raise AssertionError(f"{bad} was taken")
    assert make(SIM_MAKER_SWEEP_SETTLE_SECONDS="600").sim_sweep_settle() == 600.0
    # never less than the follow's settle; 0 = the follow's
    assert make(SIM_MAKER_SWEEP_SETTLE_SECONDS="0").sim_sweep_settle() == 2.0
    assert make(SIM_MAKER_SWEEP_SETTLE_SECONDS="1", SIM_MAKER_FOLLOW_SETTLE_SECONDS="5").sim_sweep_settle() == 5.0
    assert make(SIM_MAKER_SWEEP_SETTLE_SECONDS="0", SIM_MAKER_FOLLOW_SETTLE_SECONDS="0").sim_sweep_settle() == 0.0
    # kept with the settings a record is made under where there are sweep orders — not elsewhere (an older record says nothing of it)
    assert bot.sim_version()["sim_maker_sweep_settle"] == 30.0 and m.VERSION_KNOBS["sim_maker_sweep_settle"][0] == "SIM_MAKER_SWEEP_SETTLE_SECONDS"
    assert "sim_maker_sweep_settle" not in make(SIM_MAKER_DEEP_ONLY="off").sim_version() and "sim_maker_sweep_settle" not in make(SIM_MAKER_DEEP_CENTS="0").sim_version()
    print("THE_SETTING_OK")


def a_sweep_orders_move_waits_its_own_settle() -> None:
    bot = make()
    order = swept(bot)
    waked: list[float] = []
    bot.sim_follow_wake = lambda key, settle: waked.append(settle)
    why = lambda bids, ms, fair=0.50, b=None: (b or bot).sim_requote_reason(order, card(bids, NOW + ms, fair), NOW + ms)
    other = (BASE[0], ("0.10", "200"), BASE[1])  # somebody rests 200 at 10¢, under the threshold (50¢ − 25¢): a cent over them is 11¢
    assert why(BASE, 500) == "", "as placed: nothing to do"
    assert why(other, 1_000) == "" and bot.sim_follow_since == {f"{HSI_SLUG}|up": (0.11, NOW + 1_000)} and waked == [30.0], (bot.sim_follow_since, waked)
    # the follow's two seconds are not this move's: nor ten, nor twenty-nine
    assert [why(other, ms) for ms in (3_100, 11_000, 30_900)] == ["", "", ""]
    assert why(other, 31_000) == "下面的买单变了，击穿单改挂 11.0¢"
    # the place changing meanwhile starts the clock anew (they went to 12¢: a cent over is 13¢) — thirty seconds from then
    moved = (BASE[0], ("0.12", "200"), BASE[1])
    assert why(moved, 32_000) == "" and bot.sim_follow_since[f"{HSI_SLUG}|up"] == (0.13, NOW + 32_000)
    assert why(moved, 61_900) == "" and why(moved, 62_000) == "下面的买单变了，击穿单改挂 13.0¢"
    # ...and back where the order is, nothing is left to wait for
    assert why(BASE, 63_000) == "" and bot.sim_follow_since == {}
    assert waked == [30.0, 30.0], waked
    # a move down is the follow's to time, as before: the order at 11¢ and the 10¢ bid it stood over gone — two seconds, to 2¢
    up = make()
    high = swept(up)
    high["price"] = 0.11
    woke: list[float] = []
    up.sim_follow_wake = lambda key, settle: woke.append(settle)
    down = lambda bids, ms: up.sim_requote_reason(high, card(bids, NOW + ms), NOW + ms)
    assert down(other, 500) == "" and up.sim_follow_since == {}  # in place: a cent over their 10¢
    assert down(BASE, 1_000) == "" and up.sim_follow_since == {f"{HSI_SLUG}|up": (0.02, NOW + 1_000)} and woke == [2.0], (up.sim_follow_since, woke)
    assert down(BASE, 2_900) == "" and down(BASE, 3_000) == "下面的买单变了，击穿单改挂 2.0¢"
    # ...also down to a cent over a lower bid of theirs (they went from 10¢ to 5¢)
    lower = (BASE[0], ("0.05", "200"), BASE[1])
    up.sim_follow_since.clear()
    assert down(lower, 4_000) == "" and down(lower, 5_900) == "" and down(lower, 6_000) == "下面的买单变了，击穿单改挂 6.0¢"
    # ...and up from there (they are at 20¢ now) is the sweep order's own again
    higher = (BASE[0], ("0.20", "200"), BASE[1])
    assert down(higher, 7_000) == "" and woke == [2.0, 2.0, 30.0] and down(higher, 36_900) == "" and down(higher, 37_000) == "下面的买单变了，击穿单改挂 21.0¢"
    # the setting at 0: the follow's two seconds for this move too (as it was until 1.154.49)
    as_was = make(SIM_MAKER_SWEEP_SETTLE_SECONDS="0")
    assert why(other, 1_000, b=as_was) == "" and why(other, 2_900, b=as_was) == "" and why(other, 3_000, b=as_was) == "下面的买单变了，击穿单改挂 11.0¢"
    # both at 0: at once
    at_once = make(SIM_MAKER_SWEEP_SETTLE_SECONDS="0", SIM_MAKER_FOLLOW_SETTLE_SECONDS="0")
    assert why(other, 1_000, b=at_once) == "下面的买单变了，击穿单改挂 11.0¢" and at_once.sim_follow_since == {}
    # the follow's settle longer than the sweep order's: that one
    longer = make(SIM_MAKER_SWEEP_SETTLE_SECONDS="1", SIM_MAKER_FOLLOW_SETTLE_SECONDS="5")
    assert why(other, 1_000, b=longer) == "" and why(other, 5_900, b=longer) == "" and why(other, 6_000, b=longer) == "下面的买单变了，击穿单改挂 11.0¢"
    print("A_SWEEP_ORDERS_MOVE_WAITS_ITS_OWN_SETTLE_OK")


def what_the_edge_says_goes_as_before() -> None:
    """The thirty seconds are for the order's way up over the others' bids only. An order resting at 20¢ (as after a
    climb), somebody's 200 at 19¢ under it: in place at a fair price of 45¢ (the threshold 20¢)."""
    bot = make()
    order = swept(bot)
    order["price"] = 0.20
    waked: list[float] = []
    bot.sim_follow_wake = lambda key, settle: waked.append(settle)
    under = (BASE[0], ("0.19", "200"), BASE[1])
    why = lambda ms, fair, bids=under: bot.sim_requote_reason(order, card(bids, NOW + ms, fair), NOW + ms)
    assert why(500, 0.45) == "" and bot.sim_follow_since == {}
    # the fair price half a cent lower: the threshold is 19¢, a cent under the order — whose edge (24.5¢) is within the cent the
    # mode's line leaves. Down by the follow's two seconds, as before 1.154.50, and said for what it is
    assert why(1_000, 0.445) == "" and bot.sim_follow_since == {f"{HSI_SLUG}|up": (0.02, NOW + 1_000)} and waked == [2.0], (bot.sim_follow_since, waked)
    assert why(2_900, 0.445) == ""
    assert why(3_000, 0.445) == "公平价减门槛降到 19.0¢，击穿单改挂 2.0¢"
    assert m.requote_kind(why(3_000, 0.445)) == "公平价减门槛降到，击穿单改挂" and not m.Bot.requote_urgent(why(3_000, 0.445))
    # ...the same place wanted for the others' bids instead (the fair price back, the 19¢ bid gone: a cent over the 1¢ one) is a
    # move down as well: the follow's two seconds, which the place has held already
    assert why(4_000, 0.45, BASE) == "下面的买单变了，击穿单改挂 2.0¢"
    # further down and the mode's line withdraws it — at once, no clock asked
    bot.sim_follow_since.clear()
    assert why(40_000, 0.43) == "盘后、低价挂单只挂净优势 ≥ 25.0¢ 的，现 23.0¢ 低于盘后低价挂单撤单线 24.0¢", why(40_000, 0.43)
    assert m.Bot.requote_urgent(why(40_000, 0.43)) and bot.sim_follow_since == {}
    # the threshold under 1¢: nothing to rest at — at once
    low = make()
    cent = swept(low)
    cent["price"] = 0.01
    assert low.sim_requote_reason(cent, card(BASE, NOW + 1_000, 0.255), NOW + 1_000) == "公平价 25.5¢ 留不出 25.0¢，击穿单撤掉" and low.sim_follow_since == {}
    # the move to the front once the front has the edge is the follow's to time (two seconds), as before
    front_bot = make()
    front_order = swept(front_bot)
    away = (("0.20", "3430"), BASE[1])  # the book's front fell to 20¢: a cent over it leaves 29¢, more than the 25¢ the mode asks
    ask = lambda ms: front_bot.sim_requote_reason(front_order, card(away, NOW + ms), NOW + ms)
    assert ask(1_000) == "" and ask(2_900) == ""
    assert ask(3_000) == "盘口离公平价远了，击穿单改挂到买1 之上 1¢（21.0¢）", ask(3_000)
    print("WHAT_THE_EDGE_SAYS_GOES_AS_BEFORE_OK")


async def through_the_step() -> None:
    """The whole way, by the trader's own step and the default settings: the order stays through the half minute, then moves."""
    bot = make()
    bot.sim_markets = lambda now: world["markets"]
    other = (BASE[0], ("0.10", "200"), BASE[1])
    await step(bot, NOW, card())
    assert bot.sim_trades()[TID]["price"] == 0.02 and bot.sim_trades()[TID]["sweep"] is True
    for ms in (10_000, 15_000, 25_000, 39_000):  # somebody's 200 at 10¢ from the tenth second on
        await step(bot, NOW + ms, card(other, NOW + ms))
        trades = bot.sim_trades()
        assert trades[TID]["price"] == 0.02 and trades[TID]["status"] == "resting" and f"{TID}#1" not in trades, (ms, trades[TID]["price"])
    await step(bot, NOW + 40_500, card(other, NOW + 40_500))
    trades = bot.sim_trades()
    assert trades[f"{TID}#1"]["note"] == "撤单：下面的买单变了，击穿单改挂 11.0¢，一份都没成交", trades.get(f"{TID}#1")
    await step(bot, NOW + 41_500, card(other, NOW + 41_500))
    assert bot.sim_trades()[TID]["price"] == 0.11 and bot.sim_trades()[TID]["sweep"] is True
    # the setting at 0 (the follow's two seconds): moved by the step after those
    old = make(SIM_MAKER_SWEEP_SETTLE_SECONDS="0")
    old.sim_markets = lambda now: world["markets"]
    await step(old, NOW, card())
    await step(old, NOW + 10_000, card(other, NOW + 10_000))
    assert f"{TID}#1" not in old.sim_trades()
    await step(old, NOW + 12_500, card(other, NOW + 12_500))
    assert old.sim_trades()[f"{TID}#1"]["note"] == "撤单：下面的买单变了，击穿单改挂 11.0¢，一份都没成交"
    print("THROUGH_THE_STEP_OK")


def two_quoters_by_the_same_rule() -> None:
    """Each a cent over the best bid of anyone else under the threshold (Bot.sweep_price, on the book as each sees it):
    what the settle does to the re-quotes of both. A move is decided once its place has held the quoter's settle — its
    own for a move up, the follow's two seconds for a move down; the order is then off the book for ``lag`` (its cancel,
    its new order) and back at the new price."""
    fair, deep, lag, step, follow = 0.50, 0.25, 0.8, 0.1, 2.0

    class Quoter:
        def __init__(self, settle: float, price: float):
            self.settle, self.price, self.target, self.since = settle, price, None, 0.0
            self.landing: tuple[float, float] | None = None  # (the price it is being placed afresh at, when that is on the book)
            self.moves, self.prices = 0, [price]

    def run(settle_a: float, settle_b: float, seconds: float = 600.0) -> tuple[Quoter, Quoter, float]:
        a, b = Quoter(settle_a, 0.02), Quoter(settle_b, 0.03)
        first = both = 0  # looks with a over b, of those with both on the book
        for tick in range(int(seconds / step)):
            t = round(tick * step, 6)
            for q in (a, b):
                if q.landing is not None and t >= q.landing[1] - 1e-9:
                    q.price, q.landing = q.landing[0], None
            if a.landing is None and b.landing is None:
                first, both = first + (a.price > b.price), both + 1
            for me, other in ((a, b), (b, a)):
                if me.landing is not None:
                    continue  # its cancel and its new order are on their way
                levels: dict[float, float] = {0.48: 3430.0, 0.01: 1000.0}
                for q in (me, other):
                    if q.landing is None:
                        levels[q.price] = levels.get(q.price, 0.0) + 100.0
                view = book([(f"{p:.2f}", f"{n:g}") for p, n in sorted(levels.items(), reverse=True)], list(ASKS), NOW)
                want = m.Bot.sweep_price(view, "up", fair, deep, own=100.0, own_price=me.price)[0]
                if abs(want - me.price) < m.SIM_MAKER_FOLLOW - 1e-9:
                    me.target = None
                    continue
                if me.target is None or abs(me.target - want) >= m.SIM_MAKER_FOLLOW - 1e-9:
                    me.target, me.since = want, t  # (sim_follow_settled: the clock starts anew when the place changes)
                if t - me.since >= (me.settle if want > me.price else follow) - 1e-9:
                    me.landing, me.target, me.moves = (want, t + lag), None, me.moves + 1
                    me.prices.append(want)
        return a, b, first / max(both, 1)

    a, b, share = run(2.0, 2.0)
    # two seconds against two: each re-quotes every 5.6 s (its 2 s, its cancel and order, then the other's 2 s and theirs) — the
    # 56–67 a slot in six minutes of 11-25 — a cent over each other up to the threshold (25¢), down to the bid under both, up again
    assert 100 <= a.moves <= 108 and abs(a.moves - b.moves) <= 1, (a.moves, b.moves)
    assert 56 <= a.moves * 360 / 600 <= 67, a.moves
    assert max(a.prices) == 0.24 and max(b.prices) == 0.25 and min(a.prices[1:]) == 0.02 and min(b.prices[1:]) == 0.03, (a.prices[:30], b.prices[:30])
    assert all(round(y - x, 2) == 0.02 for q in (a, b) for x, y in zip(q.prices, q.prices[1:]) if y > x), "a cent over the one a cent over us: two cents a step"
    assert sum(1 for x, y in zip(a.prices, a.prices[1:]) if y < x) >= 7, a.prices
    assert 0.4 <= share <= 0.6, share  # ...each in front of the other half the time
    fast = a.moves
    slow, slow_b, slow_share = run(30.0, 2.0)
    # ours at thirty: a re-quote every 33.6 s on the way up (the drop from the top the two seconds it was) — and the other's
    # no more often (it has nothing to answer until we move)
    assert 17 <= slow.moves <= 21 and abs(slow.moves - slow_b.moves) <= 1, (slow.moves, slow_b.moves)
    assert sum(1 for x, y in zip(slow.prices, slow.prices[1:]) if y < x) >= 1, slow.prices
    assert slow.moves * 5 < fast, (slow.moves, fast)
    # what it costs: the other's order is the one in front (a cent over ours) nearly all the time
    assert 0.05 <= slow_share <= 0.15, slow_share
    c, d, _ = run(30.0, 30.0)
    assert 8 <= c.moves <= 11 and abs(c.moves - d.moves) <= 1, (c.moves, d.moves)
    print(f"TWO_QUOTERS_BY_THE_SAME_RULE_OK (re-quotes in ten minutes: {fast} at 2 s against 2 s, {slow.moves} at 30 s against 2 s, {c.moves} at 30 s against 30 s; "
          f"ours in front {share:.0%} of the time, then {slow_share:.0%})")


async def the_real_trader_against_another_quoter() -> None:
    """The real trader (LiveBot, Predict stood in for by test_trading's FakeApi) and another quoter by the same rule with
    two seconds' settle, on one book for ten minutes: the cancels and the orders that reach Predict."""
    async def run(sweep_settle: str, seconds: int = 600) -> tuple[int, int, int]:
        bot = T["make_bot"](SIM_WAYS="maker", SIM_MAKER_DEEP_CENTS="25", SIM_MAKER_DEEP_ONLY="on", SIM_MAKER_AFTER_HOURS_CENTS="10", SIM_MAKER_FOLLOW_SETTLE_SECONDS="2",
                            SIM_MAKER_SWEEP_SETTLE_SECONDS=sweep_settle, SIM_MAKER_FLOOR_SHARES="0", SIM_MAKER_DEEPER="off")
        api, theirs = bot.live.api, {"price": None, "target": None, "since": 0, "moves": 0}

        def ours() -> float | None:
            trade = bot.sim_trades().get(TID)
            state = ((trade or {}).get("live") or {}).get("state")
            return float(trade["price"]) if trade and trade["status"] == "resting" and state == "open" else None

        def bids(mine: float | None, other: float | None) -> tuple:
            levels: dict[float, float] = {0.48: 3430.0, 0.01: 1000.0}
            for price, shares in ((mine, 100.0), (other, 200.0)):
                if price is not None:
                    levels[price] = levels.get(price, 0.0) + shares
            return tuple((f"{p:.2f}", f"{n:g}") for p, n in sorted(levels.items(), reverse=True))

        await step(bot, NOW, card())
        assert ours() == 0.02 and [c[0] for c in api.calls] == ["create", "create"], api.calls  # (a sweep order on each side)
        for sec in range(1, seconds + 1):
            if sec >= 10:  # the other quoter arrives: a cent over the best bid of anyone else under the threshold, as we are
                view = book(list(bids(ours(), theirs["price"])), list(ASKS), NOW + sec * 1000)
                want = m.Bot.sweep_price(view, "up", 0.50, 0.25, own=200.0, own_price=theirs["price"])[0]
                if theirs["price"] is None:
                    theirs["price"] = want
                elif abs(want - theirs["price"]) < m.SIM_MAKER_FOLLOW - 1e-9:
                    theirs["target"] = None
                else:
                    if theirs["target"] is None or abs(theirs["target"] - want) >= m.SIM_MAKER_FOLLOW - 1e-9:
                        theirs["target"], theirs["since"] = want, sec
                    if sec - theirs["since"] >= 2:
                        theirs["price"], theirs["target"], theirs["moves"] = want, None, theirs["moves"] + 1
            await step(bot, NOW + sec * 1000, card(bids(ours(), theirs["price"]), NOW + sec * 1000))
        calls = [c[0] for c in api.calls]
        return calls.count("remove"), calls.count("create") - 2, theirs["moves"]

    cancels, orders, their_moves = await run("0")  # as it was: the follow's two seconds for every move
    assert 100 <= cancels <= 125 and abs(orders - cancels) <= 1 and abs(their_moves - cancels) <= 2, (cancels, orders, their_moves)
    now_cancels, now_orders, now_theirs = await run("30")
    assert 15 <= now_cancels <= 21 and abs(now_orders - now_cancels) <= 1 and abs(now_theirs - now_cancels) <= 2, (now_cancels, now_orders, now_theirs)
    assert now_cancels * 5 < cancels
    print(f"THE_REAL_TRADER_AGAINST_ANOTHER_QUOTER_OK (ten minutes on one slot: {cancels} cancels and {orders} orders with the setting at 0, {now_cancels} and {now_orders} at 30)")


def main() -> None:
    the_setting()
    a_sweep_orders_move_waits_its_own_settle()
    what_the_edge_says_goes_as_before()
    asyncio.run(through_the_step())
    two_quoters_by_the_same_rule()
    asyncio.run(the_real_trader_against_another_quoter())
    print("SWEEP_SETTLE_OK")


if __name__ == "__main__":
    main()
