"""规则吃单 (RULE_TAKER, 1.153.0): a few fixed windows of the gold and oil 15-minute cards whose outcome is not the coin flip the
book prices — gold's index stands still while CME is shut (an end equal to the start settles Up), the first window after the
daily reopen and gold's 05:15 New York window lean Down — are bought on the side the rule names whenever, before the window
starts, the book offers that side at or under the rule's price. A taker: a LIMIT order at the rule's price for what the book
shows there (never more than RULE_SHARES a window), what does not fill at once withdrawn a moment later.

The windows are set in New York time from CME's schedule (main.cme_shuts), so they follow daylight saving: gold's shut
quarter hours are 06:00–06:45 Beijing in winter and 05:00–05:45 in summer. Each rule's price is a setting (RULE_*_CENTS,
0 = the rule off), RULE_TAKER switches the whole: off, paper (只记录：what it would have bought, at the book's prices,
settled on Predict's result — no order), on (real orders, with LIVE=on and every stop the model trader has: the pause,
the day's loss stop, the operator's switches, the API's cooldown, the balance).

Records: one a window, in the bot's store under ``rule:<window slug>`` — its orders (each one's hash saved before the
request goes out: a crash in between is reconciled by it, never sent again), the shares filled, their cost (price and taker
fee), Predict's result and the P&L. The positions check counts their fills as recorded (LiveBot.side_filled), the day's
loss stop counts their P&L (LiveBot.side_pnl), the review page lists them (复盘分析 · 规则吃单), /rules shows the state."""
from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import datetime as dt
import hashlib
import logging
import time
from decimal import Decimal as D
from typing import Any, Callable

import main as core

LOG = logging.getLogger("rules")

PREFIX = "rule:"              # the records' keys in the store: rule:<window slug>
TICK_S = 1.0                  # the loop's pace
PLAN_EVERY_MS = 60_000        # the rules' windows of the next day worked out again this often (and at once after a settings change)
HORIZON_MS = 24 * 3_600_000 + 10 * 60_000  # windows followed this far ahead: Predict lists a window a day before its start
LISTED_AHEAD_MS = 24 * 3_600_000  # ...about 85 s after it enters that day (11-06): its category is looked for from then
PROBE_EARLY_MS = 120_000      # the listing is looked for from this long before it is expected
PROBE_FAST_S, PROBE_SLOW_S = 2.0, 30.0  # ...every PROBE_FAST_S until PROBE_LATE_MS past the moment, then every PROBE_SLOW_S
PROBE_LATE_MS = 5 * 60_000
NEAR_MS = 15 * 60_000         # the last stretch before the start: the status and the book read every FAST_S (matching opens then)
FAST_S, SLOW_S, CLOSED_SLOW_S = 2.0, 30.0, 60.0  # the book of an open market (fast near the start), a market not yet open for matching
STOP_MS = 10_000              # nothing placed this close to the start: the rule is about the window before it begins
EXPIRY_BEFORE_S = 5           # an order's signed expiry: this long before the start...
MIN_LIFE_S = 65               # ...or this long after it is built, whichever is later (no order in this bot is signed for under a minute)
FILL_READ_MS = 1_500          # an order is read this long after its answer; what has not filled then is withdrawn
FOLLOW_EVERY_MS = 1_000       # a withdrawn order read again this often until Predict's word is final...
UNKNOWN_EVERY_MS = 10_000     # ...an order whose placement's answer went missing, looked for by its hash this often
GIVE_UP_MS = 15 * 60_000      # ...and given up as never placed this long after its signed expiry
REJECT_PAUSE_MS = 30_000      # a window whose order Predict refused is looked at again this long on...
MAX_REJECTS = 5               # ...and left alone after this many refusals
SETTLE_AFTER_MS = 120_000     # Predict's result asked for this long after the window's end, then every SETTLE_EVERY_MS
SETTLE_EVERY_MS = 60_000
KEEP_DAYS = 90                # finished records kept this long (the review page's history)
ALLOWANCE_MS = 60_000         # the exchange allowance read again this often
DUST = 1e-6
FINAL_STATES = {"final", "rejected", "not_sent", "not_found"}
NEVER_PLACED = {"rejected", "not_sent", "not_found"}
MODES = {"off": "关", "paper": "只记录（不下单）", "on": "真实吃单"}


# --- the windows ------------------------------------------------------------------------------------------------------------
def et_parts(ms: int) -> tuple[int, int, int]:
    """(weekday — Monday 0, hour, minute) of an instant in New York, daylight saving followed (main.us_eastern_offset)."""
    at = dt.datetime.fromtimestamp(ms / 1000 + core.us_eastern_offset(ms) * 3600, dt.timezone.utc)
    return at.weekday(), at.hour, at.minute


def daily_break(start_ms: int) -> bool:
    """A quarter hour of CME's daily one-hour break: 17:00–17:45 New York, Monday to Thursday (北京冬令时周二至周五 06:00–06:45，
    夏令时 05:00–05:45). The weekend's same hour is not one: the index moves there more (2026: Up 66–70% against 90%)."""
    weekday, hour, minute = et_parts(start_ms)
    return weekday <= 3 and hour == 17 and minute % 15 == 0


def daily_reopen(start_ms: int) -> bool:
    """The first quarter hour after CME's daily break: 18:00 New York, Monday to Thursday, the stretch shut before it the
    one-hour break itself — not the weekend's (Sunday's 18:00, 北京周一早上: no lean, 2026: Down 41%) nor a holiday's."""
    weekday, hour, minute = et_parts(start_ms)
    if weekday > 3 or (hour, minute) != (18, 0):
        return False
    ending = [s for s in core.market_shuts("cme", start_ms - 60_000, start_ms) if s[1] == start_ms]
    return bool(ending) and ending[0][1] - ending[0][0] <= 65 * 60_000


def ny_0515(start_ms: int) -> bool:
    """The quarter hour from 05:15 New York (北京冬令时 18:15，夏令时 17:15), Monday to Friday, CME open: before the London
    morning fix (10:30 London) gold has leaned down (2026: Down 65%, the quarter hours around it 38–55%)."""
    weekday, hour, minute = et_parts(start_ms)
    return weekday <= 4 and (hour, minute) == (5, 15) and not core.market_shuts("cme", start_ms, start_ms + 15 * 60_000)


@dataclasses.dataclass(frozen=True)
class Rule:
    key: str                       # its price setting (RuleSettings, RULE_SETTING_KEYS) and the records' "rule"
    card: str                      # the card whose windows it buys (WINDOW_UPDOWN_MARKETS)
    side: str                      # "up" / "down"
    name: str                      # 黄金休市 4 档
    when: str                      # its windows, in New York and Beijing time
    matches: Callable[[int], bool]

    @property
    def word(self) -> str:
        return "涨" if self.side == "up" else "跌"


RULES: tuple[Rule, ...] = (
    Rule("gold_shut", "XAU-15M", "up", "黄金休市 4 档", "纽约 17:00–17:45，周一至周四（北京冬令时 06:00–06:45、夏令时 05:00–05:45）", daily_break),
    Rule("gold_open", "XAU-15M", "down", "黄金开盘第一档", "纽约 18:00，周一至周四（北京冬令时 07:00、夏令时 06:00；周一早上那场不做）", daily_reopen),
    Rule("gold_0515", "XAU-15M", "down", "黄金纽约 05:15", "纽约 05:15，周一至周五（北京冬令时 18:15、夏令时 17:15）", ny_0515),
    Rule("oil_shut", "CL-15M", "down", "原油休市 4 档", "纽约 17:00–17:45，周一至周四（北京冬令时 06:00–06:45、夏令时 05:00–05:45）", daily_break),
    Rule("oil_open", "CL-15M", "down", "原油开盘第一档", "纽约 18:00，周一至周四（北京冬令时 07:00、夏令时 06:00；周一早上那场不做）", daily_reopen),
)
RULE_OF = {r.key: r for r in RULES}


def card_of(rule: Rule) -> Any:
    return next(c for c in core.WINDOW_UPDOWN_MARKETS if c.key == rule.card)


def windows(rule: Rule, from_ms: int, to_ms: int) -> list[int]:
    """The starts of the rule's windows in [from_ms, to_ms)."""
    card = card_of(rule)
    out, start = [], card.first_start(from_ms)
    while start < to_ms:
        if rule.matches(start):
            out.append(start)
        start = card.next_start(start)
    return out


def money(x: float) -> str:
    return ("+$" if x >= 0 else "−$") + f"{abs(x):,.2f}"


def bj(ms: int, day: bool = True) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, core.BEIJING).strftime("%m-%d %H:%M" if day else "%H:%M")


# --- the book -----------------------------------------------------------------------------------------------------------------
def offered(bids: tuple, asks: tuple, side: str, limit: D) -> list[tuple[D, D]]:
    """What a buy of ``side`` at ``limit`` or under takes, the cheapest first: (price, shares). Predict's book is quoted for 涨
    (Up): its asks are the Up shares for sale; a bid for Up at p is Down for sale at 1 − p (the two buys mint a pair)."""
    if side == "up":
        return [(p, q) for p, q in sorted(asks) if p <= limit]
    return [(1 - p, q) for p, q in sorted(bids, reverse=True) if 1 - p <= limit]


def walk(levels: list[tuple[D, D]], shares: D) -> D:
    """The average price of ``shares`` bought from ``levels`` (cheapest first)."""
    left, cost = shares, D(0)
    for price, size in levels:
        took = min(size, left)
        cost += price * took
        left -= took
        if left <= 0:
            break
    return cost / (shares - left) if shares > left else D(0)


# --- the records ----------------------------------------------------------------------------------------------------------------
def records(store: Any) -> dict[str, dict]:
    return {k.removeprefix(PREFIX): v for k, v in store.items(PREFIX) if isinstance(v, dict)}


def filled(rec: dict) -> float:
    return sum(float(o.get("filled") or 0) for o in rec.get("orders") or [])


def cost(rec: dict) -> float:
    """What the fills cost, the taker fee in: each order's shares at its average price (its limit while Predict states none)."""
    return sum(float(o.get("filled") or 0) * (float(o.get("avg") or o.get("limit") or 0) + float(o.get("fee") or 0)) for o in rec.get("orders") or [])


def avg_price(rec: dict) -> float:
    """The fills' average price, the fee apart (its limit for an order whose record states no price)."""
    shares = filled(rec)
    return sum(float(o.get("filled") or 0) * float(o.get("avg") or o.get("limit") or 0) for o in rec.get("orders") or []) / shares if shares > DUST else 0.0


def in_flight(rec: dict) -> bool:
    return any(o.get("state") not in FINAL_STATES for o in rec.get("orders") or [])


def held(rec: dict) -> float:
    """The window's shares bought or being bought: the fills, and the whole of an order whose word is not final."""
    return sum(float(o.get("shares") or 0) if o.get("state") not in FINAL_STATES else float(o.get("filled") or 0) for o in rec.get("orders") or [])


def day_of(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, core.BEIJING).date().isoformat()


def filled_by_outcome(recs: dict[str, dict], now_ms: int, days: int = 30) -> dict[tuple[str, int], float]:
    """The real orders' fills by (market id, index set), the windows of the last ``days`` days: shares the positions check
    counts as recorded (LiveBot.side_filled), never adopts onto an order of the model trader's."""
    out: dict[tuple[str, int], float] = {}
    for rec in recs.values():
        if rec.get("mode") != "on" or int(rec.get("start_ms") or 0) < now_ms - days * core.DAY_MS:
            continue
        shares = filled(rec)
        if shares > DUST:
            k = (str(rec.get("market_id") or ""), int(rec.get("index_set") or 0))
            out[k] = out.get(k, 0.0) + shares
    return out


def unverified(recs: dict[str, dict]) -> set[tuple[str, int]]:
    """The outcomes where a real order's fills are not final yet: a position there may be its own (LiveBot.side_unverified)."""
    return {(str(r.get("market_id") or ""), int(r.get("index_set") or 0)) for r in recs.values() if r.get("mode") == "on" and in_flight(r)}


def day_pnl(recs: dict[str, dict], day: str) -> float:
    """The real records' P&L settled on a Beijing day: counted with the model trader's against LIVE_MAX_DAILY_LOSS_USD."""
    return sum(float(r.get("pnl") or 0) for r in recs.values() if r.get("mode") == "on" and r.get("settled_day") == day)


def review(recs: dict[str, dict], settings: Any, now_ms: int) -> dict | None:
    """The review page's 规则吃单: each rule's totals (windows bought, shares, cost, settled P&L, wins and losses, open), the
    paper and the real apart, and the last records; None when nothing is recorded and the mode is off."""
    if not recs and settings.mode == "off":
        return None
    rows = []
    for rule in RULES:
        for mode in ("on", "paper"):
            mine = [r for r in recs.values() if r.get("rule") == rule.key and r.get("mode") == mode and filled(r) > DUST]
            if not mine and (mode == "paper" or settings.price(rule.key) <= 0):
                continue
            done = [r for r in mine if r.get("pnl") is not None]
            rows.append({"rule": rule.key, "name": rule.name, "side": rule.side, "cents": settings.price(rule.key), "when": rule.when,
                         "mode": mode, "windows": len(mine), "shares": round(sum(filled(r) for r in mine), 4),
                         "cost": round(sum(cost(r) for r in mine), 4), "settled": len(done),
                         "wins": sum(1 for r in done if float(r["pnl"]) > DUST), "losses": sum(1 for r in done if float(r["pnl"]) < -DUST),
                         "pnl": round(sum(float(r["pnl"]) for r in done), 4), "open": len(mine) - len(done)})
    last = sorted((r for r in recs.values() if filled(r) > DUST), key=lambda r: -int(r.get("start_ms") or 0))[:60]
    return {"mode": settings.mode, "shares": settings.shares, "rules": rows,
            "recent": [{"slug": r.get("slug"), "rule": r.get("rule"), "name": (RULE_OF.get(r.get("rule")) or RULES[0]).name,
                        "window": r.get("name"), "start_ms": r.get("start_ms"), "side": r.get("side"), "mode": r.get("mode"),
                        "shares": round(filled(r), 4), "avg": round(avg_price(r), 4), "cost": round(cost(r), 4),
                        "result": r.get("result") or "", "pnl": r.get("pnl")} for r in last]}


# --- the taker --------------------------------------------------------------------------------------------------------------------
class RuleTaker:
    """Runs beside the model trader and the pre-open mode (LiveBot.rules_start): one loop that works out the rules' windows of
    the next day, finds each one's market as Predict lists it, watches its matching status and book before the start, buys
    what the rule allows, follows its orders to Predict's final word and settles each window on Predict's result. With
    RULE_TAKER off it only finishes what its records still hold (orders in flight, results to read)."""

    def __init__(self, owner: Any):
        import live
        import preopen
        self.owner = owner
        self.settings = owner.config.rules
        shared = getattr(owner.live, "api", None)  # one rate-limit cooldown with the bot's own client (a 429 on either pauses both)
        self.api = preopen.safe_api(live.API_BASES[owner.config.live_chain], owner.config.predict_api_key, owner.live.wallet, self.before_send, shared)
        self.track: dict[str, dict] = {}   # window slug -> what is known of it (its rule, start, market, when last looked at, the last word)
        self.planned_ms = 0
        self.sending: dict[str, str] = {}  # hash -> slug while its request is out (before_send's lookup)
        self.allowance: dict[str, tuple[int, int]] = {}
        self.errors: collections.deque = collections.deque(maxlen=20)
        self.error = ""
        self.stop = asyncio.Event()
        self.alive = True
        self.reads = 0
        self.recs: dict[str, dict] = records(owner.store)  # the records, kept in memory: only this taker writes them

    # -- plumbing --
    def now(self) -> int:
        return int(self.owner.market.now_ms())  # whole ms: the slugs are built from it

    @property
    def store(self) -> Any:
        return self.owner.store

    def record(self, slug: str) -> dict | None:
        return self.recs.get(slug)

    def save(self, rec: dict) -> None:
        self.recs[rec["slug"]] = rec
        self.store.put(PREFIX + rec["slug"], rec)

    def note(self, text: str) -> None:
        self.error = text
        if text:
            self.errors.append((self.now(), text))
            LOG.warning("规则吃单：%s", text)

    def notify(self, text: str, key: str = "") -> None:
        send = getattr(self.owner, "live_notify", None)
        if callable(send):
            with contextlib.suppress(Exception):
                send(text, key=key)

    def cooling(self) -> float:
        return getattr(self.api, "blocked_until", 0.0) - time.monotonic()

    def configure(self, settings: Any) -> None:
        """The control page's values: the next tick plans with them (a rule switched off stops at once: its windows are dropped)."""
        self.settings = settings
        self.planned_ms = 0

    # -- the gates --
    def block(self, usd: float) -> str:
        """Why no real order goes out now ("" to send): the mode, real trading off or paused, the day's loss stop, the account,
        the API's cooldown, the balance."""
        owner, c = self.owner, self.owner.config
        if self.settings.mode != "on":
            return f"规则吃单是“{MODES.get(self.settings.mode, self.settings.mode)}”，不下单"
        if c.live_mode != "on":
            return f"LIVE={c.live_mode}，规则吃单不下单"
        if not getattr(c, "live_allowed", True):
            return "部署变量 LIVE_TRADING_ALLOWED=off，不下单"
        if paused := owner.live_paused():
            return f"真实交易已暂停（{paused}）"
        if killed := owner.live_killed():
            return f"今日停止开新仓：{killed}"
        cap = c.live_max_daily_loss_usd
        if cap:
            pnl = owner.live_daily_pnl(owner.sim_trades(), self.now()) + float(getattr(owner, "preopen_pnl", lambda: 0.0)()) + self.today_pnl()
            floor = owner.live_loss_floor(cap) if hasattr(owner, "live_loss_floor") else -cap
            if pnl <= floor:
                return f"今日已结算亏损 {money(pnl)} 已达 LIVE_MAX_DAILY_LOSS_USD ${cap:g}"
        if not owner.live.ready:
            return f"真实交易未就绪：{getattr(owner.live, 'not_ready_why', '')}"
        if (left := self.cooling()) > 0:
            return f"Predict 接口限流冷却中（{int(left) + 1} 秒后重试）"
        usd_left = owner.live.usd() if callable(getattr(owner.live, "usd", None)) else None
        if usd_left is not None and usd_left < usd:
            return f"USDT 余额 ${usd_left:,.2f} 不足本单 ${usd:,.2f}"
        return ""

    def before_send(self, body: dict) -> str:
        """Judged right before an order's bytes go out (preopen.safe_api): the gates again, and the window's start not passed."""
        digest = str(body.get("data", {}).get("order", {}).get("hash") or "").lower()
        slug = self.sending.get(digest)
        if slug is None:
            return "不是规则吃单这一刻要发的订单"
        rec = self.record(slug) or {}
        order = next((o for o in rec.get("orders") or [] if o.get("hash") == digest), None)
        if order is None:
            return "订单记录不见了"
        if self.now() > int(rec.get("start_ms") or 0) - STOP_MS // 2:
            return "已到开场，不再发送"
        return self.block(float(order.get("shares") or 0) * float(order.get("limit") or 0))

    def today_pnl(self) -> float:
        return day_pnl(self.recs, day_of(self.now()))

    # -- the loop --
    async def run(self) -> None:
        try:
            while not self.stop.is_set() and not self.owner.stopping.is_set():
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.note(core.clean_error(error) or type(error).__name__)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), TICK_S)
        finally:
            self.alive = False

    async def tick(self) -> None:
        now = self.now()
        if now - self.planned_ms >= PLAN_EVERY_MS:
            self.plan(now)
            self.planned_ms = now
        if self.settings.mode != "off":
            for slug, w in sorted(list(self.track.items()), key=lambda kv: kv[1]["start"]):
                if self.stop.is_set() or self.cooling() > 0:
                    break
                try:  # one window's failed read never keeps the others, nor the orders' follow-up, waiting
                    await self.look(slug, w)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    w["word"] = f"读取失败：{core.clean_error(error) or type(error).__name__}"
                    self.note(f"{slug}：{w['word']}")
        await self.follow()
        await self.settle()

    def plan(self, now: int) -> None:
        """The windows of the enabled rules from now to a day on (HORIZON_MS); a window of a rule switched off is dropped, and so
        is one this close to its start (STOP_MS). The records past KEEP_DAYS and finished are removed."""
        want: dict[str, tuple[str, int]] = {}
        if self.settings.mode != "off":
            for rule in RULES:
                if self.settings.price(rule.key) > 0:
                    for start in windows(rule, now + STOP_MS, now + HORIZON_MS):
                        want[card_of(rule).slug_of(start)] = (rule.key, start)
        for slug in list(self.track):
            if slug not in want:
                self.track.pop(slug)
        for slug, (key, start) in want.items():
            self.track.setdefault(slug, {"rule": key, "start": start, "market": None, "listed_ms": 0, "open_ms": 0, "probe_ms": 0,
                                         "look_ms": 0, "word": ""})
        old = [slug for slug, r in self.recs.items()
               if int(r.get("start_ms") or 0) < now - KEEP_DAYS * core.DAY_MS and not in_flight(r) and (r.get("pnl") is not None or filled(r) <= DUST)]
        if old:
            self.store.delete_keys([PREFIX + slug for slug in old])
            for slug in old:
                self.recs.pop(slug, None)

    def due(self, w: dict, now: int) -> bool:
        """Whether the window is to be looked at this tick (its cadence: the listing looked for around its moment, the matching
        status until it opens, the book once it has — every FAST_S near the start)."""
        start, near = w["start"], w["start"] - now <= NEAR_MS
        if w["market"] is None:
            expected = start - LISTED_AHEAD_MS
            if now < expected - PROBE_EARLY_MS:
                return False
            every = PROBE_FAST_S if now <= expected + PROBE_LATE_MS or near else PROBE_SLOW_S
            return now - w["probe_ms"] >= every * 1000
        every = FAST_S if near else SLOW_S if w["open_ms"] else CLOSED_SLOW_S
        return now - w["look_ms"] >= every * 1000

    async def look(self, slug: str, w: dict) -> None:
        now = self.now()
        rule = RULE_OF[w["rule"]]
        if w["start"] - now <= STOP_MS:
            self.track.pop(slug, None)
            return
        if not self.due(w, now):
            return
        rec = self.record(slug)
        if rec is not None and (held(rec) >= self.settings.shares - DUST or in_flight(rec)):
            w["word"] = "这一场已买满" if not in_flight(rec) else "上一张单还没有结果"
            return
        if rec is not None and self.settings.mode == "paper" and any(o.get("paper") for o in rec["orders"]):
            w["word"] = "只记录：这一场已记过（只记一次：没真买，盘口上的单还在，再记就重复了）"
            return
        if rec is not None and sum(1 for o in rec["orders"] if o.get("state") == "rejected") >= MAX_REJECTS:
            w["word"] = f"Predict 拒绝了 {MAX_REJECTS} 次，这一场不再下单"
            return
        if self.settings.mode == "on" and (why := self.block(0.0)):
            w["word"] = why  # nothing read while no order could go out
            return
        if w["market"] is None:
            w["probe_ms"] = now
            import preopen
            self.reads += 1
            market = await preopen.category_market(self.api, slug)
            if market is None:
                w["word"] = "还没发布"
                return
            question = str(market.get("question") or market.get("title") or "")
            if problem := card_of(rule).title_problem(question, w["start"]):
                w["word"] = f"市场标题和场次对不上：{problem}"
                w["probe_ms"] = now + 10 * 60_000  # looked at again in ten minutes, not every two seconds
                return
            w["market"], w["listed_ms"] = market, now
        w["look_ms"] = now
        if not w["open_ms"]:
            if w["listed_ms"] != now:  # the listing's own read is fresh; later, the details are read for the status
                self.reads += 1
                w["market"] = {**await self.api.market(str(w["market"]["id"])), "categorySlug": slug}
            if str(w["market"].get("tradingStatus") or "").upper() != "OPEN":
                w["word"] = f"已发布，撮合未开启（{w['market'].get('tradingStatus') or '?'}）"
                return
            w["open_ms"] = now
        self.reads += 1
        bids, asks = await self.book(str(w["market"]["id"]))
        limit = D(self.settings.price(rule.key)) / 100
        own = self.own_asks(slug, rule.side)  # the pre-open mode's own bids there: never bought from (the account trading with itself)
        levels = [(p, q - own.get(p, D(0))) for p, q in offered(bids, asks, rule.side, limit) if q - own.get(p, D(0)) > 0]
        best = offered(bids, asks, rule.side, D("0.99"))[:1]
        if not levels:
            w["word"] = f"盘口没有 ≤{int(limit * 100)}¢ 的{rule.word}" + (f"（最低 {float(best[0][0]) * 100:g}¢）" if best else "（没人卖）")
            return
        room = D(str(self.settings.shares)) - D(str(held(rec) if rec else 0.0))
        take = min(sum(q for _, q in levels), room)
        if self.settings.mode == "on":
            take = min(take, D(str(self.owner.config.live_max_order_usd)) / limit)  # LIVE_MAX_ORDER_USD per order
        take = take.to_integral_value(rounding="ROUND_FLOOR")
        if take < 1 or float(take * limit) < 1.0:  # Predict's minimum order ($1, live.MIN_ORDER_USD)
            w["word"] = f"盘口 ≤{int(limit * 100)}¢ 的{rule.word}只有 {sum(q for _, q in levels):g} 份，不够一张最小订单"
            return
        if self.settings.mode == "paper":
            self.paper(slug, w, rule, levels, take, limit, rec)
        else:
            await self.place(slug, w, rule, levels, take, limit, rec)

    def own_asks(self, slug: str, side: str) -> dict[D, D]:
        """The pre-open mode's resting bids on this window's other side, as offers of ``side`` (price -> shares): its Up bid at
        49¢ is a Down offer at 51¢ in the book. Only gold's 05:15 window can be one it quotes (the shut and the reopen's first
        are never quoted, 1.150.1 / 1.151.1)."""
        ctl = getattr(self.owner, "preopen", None)
        rows = getattr(getattr(ctl, "ledger", None), "rows", None)
        if not rows:
            return {}
        import preopen
        other, out = ("down" if side == "up" else "up"), {}
        for pair in list(rows.values()):
            order = (pair.get("orders") or {}).get(other) or {}
            if preopen.window_of(pair) != slug or order.get("state") not in {"prepared", "placing", "open", "unknown"}:
                continue
            with contextlib.suppress(Exception):
                price, left = D(str(pair["price"])), D(str(pair["shares"])) - D(str(order.get("filled") or 0))
                if left > 0:
                    out[1 - price] = out.get(1 - price, D(0)) + left
        return out

    async def book(self, market_id: str) -> tuple[tuple, tuple]:
        answer = await self.api.call("GET", f"/markets/{core.urllib.parse.quote(market_id)}/orderbook")
        data = answer.get("data", answer) if isinstance(answer, dict) else {}
        if not isinstance(data, dict) or ("bids" not in data and "asks" not in data):
            raise core.RemoteError("订单簿格式异常")
        return core.predict_levels(data.get("bids"), True), core.predict_levels(data.get("asks"), False)

    def new_record(self, slug: str, w: dict, rule: Rule, index_set: int) -> dict:
        card, start = card_of(rule), w["start"]
        return {"slug": slug, "rule": rule.key, "card": rule.card, "side": rule.side, "mode": self.settings.mode,
                "name": card.window_spec(start).name if hasattr(card.window_spec(start), "name") else slug,
                "start_ms": start, "end_ms": card.end_of(start), "market_id": str(w["market"]["id"]), "index_set": index_set,
                "cents": self.settings.price(rule.key), "orders": [], "created_ms": self.now(), "result": "", "pnl": None}

    def paper(self, slug: str, w: dict, rule: Rule, levels: list, take: D, limit: D, rec: dict | None) -> None:
        """只记录: what the order would have bought, at the book's prices — no order."""
        import live
        info = live.parse_market(w["market"], self.owner.config.predict_fee_bps)
        outcome, _ = live.outcome_for_side("updown", rule.side, info.outcomes)
        rec = rec or self.new_record(slug, w, rule, int((outcome or {}).get("index_set") or 0))
        avg = walk(levels, take)
        rec["orders"].append({"paper": True, "state": "final", "shares": float(take), "filled": float(take), "limit": float(limit),
                              "avg": float(avg), "fee": core.taker_fee(float(avg), info.fee_bps), "at": self.now(),
                              "book": [[float(p), float(q)] for p, q in levels[:5]]})
        self.save(rec)
        w["word"] = f"只记录：{rule.word} {float(avg) * 100:.1f}¢ × {take:g} 份"
        self.notify(f"🧪 规则吃单（只记录，没下单）：{rec['name']} 买{rule.word} 均价 {float(avg) * 100:.1f}¢ × {take:g} 份（{rule.name} ≤{int(limit * 100)}¢）")

    async def place(self, slug: str, w: dict, rule: Rule, levels: list, take: D, limit: D, rec: dict | None) -> None:
        import live
        import preopen
        usd = float(take * limit)
        if why := self.block(usd):
            w["word"] = why
            return
        info = live.parse_market(w["market"], self.owner.config.predict_fee_bps)
        outcome, assumed = live.outcome_for_side("updown", rule.side, info.outcomes)
        if outcome is None or assumed:
            w["word"] = f"认不出{rule.word}的代币：{assumed or '缺字段'}"
            return
        exchange = live.exchange_key(info.neg_risk, info.yield_bearing)
        cached = self.allowance.get(exchange)
        if cached is None or self.now() - cached[0] > ALLOWANCE_MS:
            cached = (self.now(), int(await self.owner.live.chain.allowance(exchange)))
            self.allowance[exchange] = cached
        if cached[1] < live.to_wei(usd):
            w["word"] = "交易所授权额度不足"
            return
        await self.api.ensure_auth()
        now = self.now()
        rec = self.record(slug) or rec or self.new_record(slug, w, rule, int(outcome.get("index_set") or 0))
        if in_flight(rec):
            return
        expires = max(w["start"] // 1000 - EXPIRY_BEFORE_S, now // 1000 + MIN_LIFE_S)
        amounts = live.limit_amounts(True, live.to_wei(limit), live.to_wei(take))
        wallet = self.owner.live.wallet
        identity = f"rule-v1|{self.owner.config.live_chain}|{wallet.maker.lower()}|{info.market_id}|{outcome['token']}|{w['start']}|{len(rec['orders'])}"
        salt = int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big") % live.MAX_SALT
        signed = live.build_order(wallet, outcome["token"], amounts["maker"], amounts["taker"], info.fee_bps, expires, salt=salt)
        digest, signature = wallet.sign_order(signed, info.neg_risk, info.yield_bearing)
        order = {"hash": digest.lower(), "id": "", "state": "placing", "shares": float(take), "limit": float(limit), "filled": 0.0,
                 "avg": None, "fee": core.taker_fee(float(limit), info.fee_bps), "fee_bps": info.fee_bps, "at": now, "expires": expires,
                 "book": [[float(p), float(q)] for p, q in levels[:5]]}
        rec["orders"].append(order)
        self.save(rec)
        if not await asyncio.to_thread(self.store.flush):  # the hash on disk before the request: a crash in between is found by it
            order.update(state="not_sent", error="数据库写入未完成，没有发单")
            self.save(rec)
            return
        body = {"data": {"order": {**signed, "signature": signature, "hash": digest}, "strategy": "LIMIT",
                         "pricePerShare": str(amounts["price_per_share"])}}
        self.sending[order["hash"]] = slug
        try:
            result = await self.api.create_order(body)
            order.update(id=str(result.get("order_id") or ""), state="open", next_ms=self.now() + FILL_READ_MS)
        except preopen.NotSent as error:
            order.update(state="not_sent", error=core.clean_error(error))
        except Exception as error:
            text = core.clean_error(error) or type(error).__name__
            status = live.http_status(error)
            definite = isinstance(error, live.OrderRejected) or 400 <= status < 500
            if status == 429 or "限流冷却中" in text:
                order.update(state="rejected" if status == 429 else "not_sent", error=text)
            elif definite and not any(word in text.lower() for word in preopen.DUPLICATE_WORDS):
                order.update(state="rejected", error=text)
            else:  # a timeout, a lost answer: it may exist — looked for by its hash, never sent again
                order.update(state="unknown", error=text, next_ms=self.now() + UNKNOWN_EVERY_MS)
        finally:
            self.sending.pop(order["hash"], None)
        self.save(rec)
        w["word"] = {"open": f"已下单：{rule.word} ≤{int(limit * 100)}¢ × {take:g} 份", "unknown": f"下单结果未知（{order.get('error')}），按哈希核对"}.get(
            order["state"], f"没下成：{order.get('error') or order['state']}")
        if order["state"] in NEVER_PLACED:
            self.note(f"{rec['name']} 没下成：{order.get('error') or order['state']}")
            w["look_ms"] = self.now() + REJECT_PAUSE_MS  # not again for half a minute (a refusal repeated every two seconds helps nobody)

    # -- the orders --
    async def follow(self) -> None:
        """Each real order to Predict's final word: read FILL_READ_MS after its answer, its rest withdrawn, read again until
        final; an order whose answer went missing looked for by its hash until GIVE_UP_MS past its signed expiry."""
        import live
        now = self.now()
        for slug, rec in list(self.recs.items()):
            if rec.get("mode") != "on" or not in_flight(rec):
                continue
            for order in rec["orders"]:
                if order.get("state") in FINAL_STATES or now < int(order.get("next_ms") or 0) or self.cooling() > 0:
                    continue
                try:
                    await self.follow_order(rec, order, now)
                except Exception as error:
                    order["error"] = core.clean_error(error) or type(error).__name__
                    order["next_ms"] = now + UNKNOWN_EVERY_MS
                self.save(rec)

    async def follow_order(self, rec: dict, order: dict, now: int) -> None:
        import live
        state = order["state"]
        if state == "placing":  # a request that left with the process gone before its answer (a restart): looked for by its hash
            order.update(state="unknown", next_ms=now)
            state = "unknown"
        self.reads += 1
        row = await self.api.order(order.get("id") or "", order["hash"])
        if row is None:
            if state == "unknown" and now >= int(order.get("expires") or 0) * 1000 + GIVE_UP_MS:
                order.update(state="not_found", error="按哈希一直查不到：视为没下成")
            else:
                order["next_ms"] = now + (UNKNOWN_EVERY_MS if state == "unknown" else FOLLOW_EVERY_MS)
            return
        order["id"] = str(row.get("id") or order.get("id") or "")
        self.take_fills(rec, order, row)
        if live.order_final(row, float(order["shares"])):
            order["state"] = "final"
            return
        if order.get("removed"):
            order["next_ms"] = now + FOLLOW_EVERY_MS
            return
        if order.get("id"):  # still open: the rest withdrawn (a taker never waits on the book)
            result = await self.api.remove_orders([order["id"]])
            if order["id"] in (result.get("removed") or []) or order["id"] in (result.get("noop") or []):
                order["removed"] = now
            order.update(state="cancelling", next_ms=now + FOLLOW_EVERY_MS)
        else:
            order["next_ms"] = now + UNKNOWN_EVERY_MS

    def take_fills(self, rec: dict, order: dict, row: dict) -> None:
        """An order record's fills onto ours: the shares, the average price it states (its limit while none is stated)."""
        import live
        shares, _ = live.order_fill(row, float(order["shares"]))
        price = live.order_price(row)
        if price is not None:
            order.update(avg=price, fee=core.taker_fee(price, int(order.get("fee_bps") or 0)), avg_stated=True)
        before = float(order.get("filled") or 0)
        if shares > before + DUST:
            order["filled"] = shares
            avg = float(order.get("avg") or order["limit"])
            rule = RULE_OF.get(rec.get("rule"))
            self.notify(f"🎯 规则吃单成交：{rec['name']} 买{rule.word if rule else rec.get('side')} {avg * 100:.1f}¢ × {shares - before:g} 份"
                        f"（累计 {shares:g}/{order['shares']:g}；{rule.name if rule else ''} ≤{rec.get('cents')}¢）")

    # -- the results --
    async def settle(self) -> None:
        """Each window with fills, on Predict's result: SETTLE_AFTER_MS past its end, then every SETTLE_EVERY_MS until it is in.
        A window whose orders all ended without a fill needs none."""
        now = self.now()
        for slug, rec in list(self.recs.items()):
            if rec.get("pnl") is not None or in_flight(rec) or filled(rec) <= DUST:
                continue
            if now < int(rec.get("end_ms") or 0) + SETTLE_AFTER_MS or now - int(rec.get("settle_checked") or 0) < SETTLE_EVERY_MS:
                continue
            if self.cooling() > 0:
                return
            rec["settle_checked"] = now
            try:
                self.reads += 1
                resolution = core.predict_resolution(await self.api.market(str(rec["market_id"])))
            except Exception as error:
                rec["settle_error"] = core.clean_error(error) or type(error).__name__
                self.save(rec)
                continue
            if not resolution:
                self.save(rec)
                continue
            import live
            name = str(resolution.get("name") or "").lower()
            up = 0.5 if resolution.get("split") else 1.0 if name in live.UP_WORDS else 0.0 if name in live.DOWN_WORDS else None
            if up is None:
                rec["settle_error"] = f"结果名称认不出：{resolution.get('name')}"
                self.save(rec)
                continue
            pay = up if rec["side"] == "up" else 1 - up
            pnl = sum(float(o.get("filled") or 0) * (pay - float(o.get("avg") or o.get("limit") or 0) - float(o.get("fee") or 0)) for o in rec["orders"])
            rec.update(result="split" if up == 0.5 else "up" if up == 1.0 else "down", pnl=round(pnl, 6), settled_ms=now, settled_day=day_of(now))
            self.save(rec)
            word = "赢" if pnl > DUST else "输" if pnl < -DUST else "平"
            rule = RULE_OF.get(rec.get("rule"))
            self.notify(f"{'✅' if pnl > DUST else '❌' if pnl < -DUST else '➖'} 规则吃单结算{'（只记录）' if rec.get('mode') == 'paper' else ''}："
                        f"{rec['name']} 买{rule.word if rule else rec['side']} {filled(rec):g} 份，结果{'涨' if up == 1.0 else '跌' if up == 0.0 else '平'}，"
                        f"{word} {money(pnl)}")
            if rec.get("mode") == "on" and pay > 0 and callable(soon := getattr(self.owner, "live_redeem_soon", None)):
                with contextlib.suppress(Exception):
                    soon(rec["market_id"])

    # -- what it says --
    def status(self) -> str:
        now, s = self.now(), self.settings
        recs = self.recs
        lines = [f"规则吃单：{MODES.get(s.mode, s.mode)}（RULE_TAKER={s.mode}）｜每场最多 {s.shares:g} 份｜下单到开场前 {STOP_MS // 1000} 秒"]
        if s.mode == "on" and (why := self.block(0.0)):
            lines.append(f"⏸ 现在不下单：{why}")
        for rule in RULES:
            cents = s.price(rule.key)
            if cents <= 0:
                lines.append(f"· {rule.name}：不做（价格设为 0）")
                continue
            nxt = min((w for w in self.track.values() if w["rule"] == rule.key), key=lambda w: w["start"], default=None)
            lines.append(f"· {rule.name} 买{rule.word} ≤{cents}¢：{rule.when}"
                         + (f"｜下一场 {bj(nxt['start'])}{('：' + nxt['word']) if nxt['word'] else ''}" if nxt else ""))
        today = day_of(now)
        mine = [r for r in recs.values() if day_of(int(r.get("start_ms") or 0)) == today and filled(r) > DUST]
        if mine:
            done = [r for r in mine if r.get("pnl") is not None]
            lines.append(f"今天：{len(mine)} 场买到 {sum(filled(r) for r in mine):g} 份，成本 ${sum(cost(r) for r in mine):,.2f}"
                         + (f"；已结算 {len(done)} 场 {money(sum(float(r['pnl']) for r in done))}" if done else ""))
        last = sorted((r for r in recs.values() if filled(r) > DUST), key=lambda r: -int(r.get("start_ms") or 0))[:5]
        for r in last:
            lines.append(f"  {bj(int(r['start_ms']))} {(RULE_OF.get(r['rule']) or RULES[0]).name} 买{'涨' if r['side'] == 'up' else '跌'} "
                         f"{avg_price(r) * 100:.1f}¢×{filled(r):g}" + ("（只记录）" if r.get("mode") == "paper" else "")
                         + (f" → {money(float(r['pnl']))}" if r.get("pnl") is not None else "（待结算）"))
        if self.error:
            lines.append(f"最近问题：{self.error}")
        return "\n".join(lines)
