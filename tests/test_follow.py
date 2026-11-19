"""1.154.29: PREDICT_FOLLOW_UNTRADED — on (as ever) every 15- / 5-minute card's market is followed on Predict; off, only a card
somebody uses: the model trader (its kind chosen, the card not left out, or an open record on it) or the rule taker (the card its
windows' course is judged by). 11-18: 23 of the feed's 40 keys were such cards with the model trader on none of them — 23 of the
stream's 80 topics, which were full, and most of the feed's reads on a key at its pace."""
import asyncio, sys, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L

# Wednesday 2026-11-18 23:34 Beijing: the US session open (every US card has a window), Shanghai, Seoul and Hong Kong shut
NOW = int(dt.datetime(2026, 11, 18, 23, 34, tzinfo=m.BEIJING).timestamp() * 1000)
BASE = {"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "PREDICT_API_KEY": "k1"}
SCOPE = {"SIM_MARKETS": "close,touch,flip,range"}  # the user's of 11-19: no up/down kind at all


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now
    async def get(self, path, **params): return []


def bot(cls=m.Bot, **env):
    return cls(m.Config.from_env({**BASE, **env}), m.Store(":memory:"), FM(NOW), None)


def windows(b, targets):
    return {key for key in targets if isinstance(b.updowns.get(key), m.IndexUpDownMarket)}


async def run():
    # --- the setting: on by default, a choice of the control page's 策略 group, right after the scope's own -----------------
    on = bot(**SCOPE)
    assert on.config.predict_follow_untraded is True and on.control_value("PREDICT_FOLLOW_UNTRADED") == "on"
    keys = [k for k, _, _ in m.CONTROL_KEYS]
    assert keys.index("PREDICT_FOLLOW_UNTRADED") == keys.index("SIM_SKIP") + 1
    assert m.CONTROL_FORMS["PREDICT_FOLLOW_UNTRADED"] == {"kind": "choice", "group": "策略", "options": [["on", "跟（照旧）"], ["off", "不跟（省推送名额和读取）"]]}
    off = bot(**SCOPE, PREDICT_FOLLOW_UNTRADED="off")
    assert off.config.predict_follow_untraded is False and off.control_value("PREDICT_FOLLOW_UNTRADED") == "off"

    # --- on: as ever — every card whose window is on, whoever trades it -------------------------------------------------------
    all_on = on.predict_targets(NOW)
    asleep = {"UNITREE-15M", "SKHYNIX-15M", "ZHIPU-15M"}  # their exchanges shut: a window hours away is followed by nobody (WINDOW_LEAD_MS)
    assert windows(on, all_on) == {s.key for s in m.WINDOW_UPDOWN_MARKETS} - asleep and len(windows(on, all_on)) == 20
    assert on.window_idle(NOW) == [k for k in on.updowns if k in windows(on, all_on)], "nobody uses any of them"
    # --- off: none of them; the daily cards and everything else as before ---------------------------------------------------
    all_off = off.predict_targets(NOW)
    assert windows(off, all_off) == set()
    assert all_off == {k: v for k, v in all_on.items() if k not in windows(on, all_on)}
    assert {"BTC-DAY", "ETH-DAY", "BNB-DAY"} <= set(all_off), "a daily card is followed whatever (its alerts are everybody's)"
    assert len(all_on) - len(all_off) == 20 and len(off.window_idle(NOW)) == 20
    # --- off, the model trader on the kind: its cards followed, those it leaves out (SIM_SKIP: by default the pre-open mode's own) not
    some = bot(SIM_MARKETS="all", PREDICT_FOLLOW_UNTRADED="off")
    traded = {s.key for s in m.WINDOW_UPDOWN_MARKETS if s.model} - asleep
    assert traded == {"BTC-15M", "ETH-15M", "BNB-15M", "XAU-15M", "CL-15M"} and windows(some, some.predict_targets(NOW)) == traded
    assert (some.window_use("BTC-15M"), some.window_use("MU-15M"), some.window_use("BTC-5M")) == ("原策略在做", "", "")
    skipped = bot(SIM_MARKETS="all", SIM_SKIP="BTC-15M,XAU-15M", PREDICT_FOLLOW_UNTRADED="off")  # (set: as given — the US cards are back in)
    got = windows(skipped, skipped.predict_targets(NOW))
    assert "BTC-15M" not in got and "XAU-15M" not in got and {"ETH-15M", "MU-15M", "BTC-5M"} <= got
    no_trader = bot(SIM="off", SIM_MARKETS="all", PREDICT_FOLLOW_UNTRADED="off")
    assert windows(no_trader, no_trader.predict_targets(NOW)) == set() and no_trader.window_use("BTC-15M") == ""
    # --- ...and a card the trader left with a record still open on it: followed until that is over ----------------------------
    assert off.window_busy() == set() and off.window_use("MU-15M") == ""
    off.sim_save([("t1", {"status": "resting", "key": "MU-15M", "market": off.updowns["MU-15M"].spec.slug, "side": "up", "kind": "updown",
                           "at": NOW, "price": 0.4, "order": 10, "shares": 0})])
    assert off.window_busy() == {"MU-15M"} and off.window_use("MU-15M") == "原策略还有没结算的单"
    assert windows(off, off.predict_targets(NOW)) == {"MU-15M"}
    off.sim_save([("t1", {**off.sim_trades()["t1"], "status": "cancelled"})])
    assert off.window_busy() == set() and windows(off, off.predict_targets(NOW)) == set()

    # --- the rule taker: gold's card while a rule buys in its windows' course (RULE_GOLD_*_LIVE_CENTS), the taker not off ------
    live = {"LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "PREDICT_FOLLOW_UNTRADED": "off", **SCOPE}
    ruled = bot(L.LiveBot, **live, RULE_TAKER="on", RULE_GOLD_SHUT_LIVE_CENTS="60")
    assert (ruled.window_use("XAU-15M"), ruled.window_use("CL-15M"), ruled.window_use("BTC-15M")) == ("规则吃单盘中要用", "", "")
    assert windows(ruled, ruled.predict_targets(NOW)) == {"XAU-15M"}
    assert bot(L.LiveBot, **live, RULE_TAKER="paper", RULE_GOLD_OPEN_LIVE_CENTS="60").window_use("XAU-15M") == "规则吃单盘中要用"
    for env in ({"RULE_TAKER": "off", "RULE_GOLD_SHUT_LIVE_CENTS": "60"},   # the taker off
                {"RULE_TAKER": "on"},                                        # no rule buys in a window's course: its own books before the window
                {"RULE_TAKER": "on", "RULE_GOLD_SHUT_LIVE_CENTS": "60", "RULE_GOLD": "off"}):  # gold switched off
        idle = bot(L.LiveBot, **live, **env)
        assert idle.window_use("XAU-15M") == "" and windows(idle, idle.predict_targets(NOW)) == set(), env
    assert windows(bot(L.LiveBot, **{**live, "PREDICT_FOLLOW_UNTRADED": "on"}), all_on) == windows(on, all_on)

    # --- nothing is asked of Predict for a card not followed: the round's reads, and the start price asked for after a start --
    for b, any_window in ((on, True), (off, False)):
        asked = []

        async def fetch(url, payload=None, ahead=False, asked=asked):
            asked.append(url)
            raise m.RemoteError("HTTP 404: not found")
        b.predict.fetch = fetch
        await b.refresh_books(NOW)
        if b.predict.cold_task is not None:
            await b.predict.cold_task
        card = b.updowns["BTC-15M"]
        await b.updown_strike(card, card.spec.start_ms + 61_000)
        of_windows = [u for u in asked if "-updown-15m-" in u or "-updown-5m-" in u]
        assert bool(of_windows) == any_window, (any_window, of_windows[:3])
        assert any("up-or-down-on" in u for u in asked), "the daily cards' all the same"
    # --- the page says why such a card shows no book; /diag counts them ---------------------------------------------------------
    card = off.updowns["BTC-15M"]
    assert off.window_payload(card, NOW)["predict"]["idle"] == "没有策略在做这张卡：不跟 Predict 盘口"
    assert "idle" not in on.window_payload(on.updowns["BTC-15M"], NOW)["predict"]
    assert "idle" not in ruled.window_payload(ruled.updowns["XAU-15M"], NOW)["predict"] and "idle" in ruled.window_payload(ruled.updowns["BTC-15M"], NOW)["predict"]
    assert 'else if(p.idle)q.append($("span","mut",p.idle));' in m.WEB_PAGE
    assert off.follow_note(NOW) == "没人做的 15 / 5 分钟涨跌卡 20 张没跟（PREDICT_FOLLOW_UNTRADED=off：不订阅、不读起点价）"
    assert on.follow_note(NOW) == "其中 20 张 15 / 5 分钟涨跌卡没有策略在做，也订阅着、读着起点价（控制页“没人做的 15 分钟 / 5 分钟涨跌卡”选“不跟”可省）"
    on.predict.stream.wanted = {str(i): ("k", False) for i in range(m.PREDICT_WS_MAX_TOPICS)}
    assert "——推送名额 80 个已满，在做的阶梯档位有的只能靠轮询读（控制页" in on.follow_note(NOW)
    assert some.follow_note(NOW).startswith("没人做的 15 / 5 分钟涨跌卡 15 张没跟") and bot(SIM_MARKETS="all", SIM_SKIP="").follow_note(NOW) == ""
    # --- chosen on the control page: in force at the next round, and back -------------------------------------------------------
    page = bot(**SCOPE)
    answer = page.control_set({"PREDICT_FOLLOW_UNTRADED": "off"})
    assert answer == {"ok": True, "message": "已保存并生效：PREDICT_FOLLOW_UNTRADED=off（原 on）"}, answer
    assert page.config.predict_follow_untraded is False and windows(page, page.predict_targets(NOW)) == set()
    assert page.control_set({"PREDICT_FOLLOW_UNTRADED": None})["ok"] and len(windows(page, page.predict_targets(NOW))) == 20
    assert not page.control_set({"PREDICT_FOLLOW_UNTRADED": "x" * 300})["ok"]
    print("FOLLOW_OK")


asyncio.run(run())
