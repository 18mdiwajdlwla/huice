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

The per-second hours (RULE_FAST_HOURS, 1.154.12; Beijing time, 01:00–07:00 by default): in them every listed window is looked
at each second on the book the push connection holds, one window's book read over REST each second in rotation beside the
pushes, the sign-in / allowance / matching status made ready from a minute before, and the orders of several windows sent side
by side. RULE_API_KEY gives the taker a key of its own (its own budget and 429 cooldown).

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
import json
import logging
import statistics
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
OFFER_STATUS_S = 10.0         # 1.153.1: an offer at the rule's price on a market not yet open for matching: its status read this often (it buys the moment it opens)
STOP_MS = 15_000              # nothing placed this close to the start: the rule is about the window before it begins
EXPIRY_BEFORE_S = 5           # an order's signed expiry: this long before the start, always — a cancel that cannot get through leaves
#                               nothing matchable once the window runs (1.153.3: an order sent in the last 70 s was signed for 65 s,
#                               valid up to a minute into the window)
SEND_MIN_LEFT_MS = 5_000      # ...and none is sent with less than this left to it
FILL_READ_MS = 1_500          # an order is read this long after its answer; what has not filled then is withdrawn
LATE_FILL_MS = 60_000         # an order off the book is final only by a read this long after it left: an earlier one may not show its
#                               last matches yet (the pre-open mode's LATE_FILL_MS; 1.153.3 — the first read after the cancel ended it,
#                               a later fill was never booked and the window's cap could be passed)
REST_MIN_S = 0.1              # the loop never goes round without a wait (1.153.3)
FOLLOW_EVERY_MS = 1_000       # a withdrawn order read again this often until Predict's word is final...
UNKNOWN_EVERY_MS = 10_000     # ...an order whose placement's answer went missing, looked for by its hash this often
GIVE_UP_MS = 15 * 60_000      # ...and given up as never placed this long after its signed expiry
REJECT_PAUSE_MS = 30_000      # a window whose order Predict refused is looked at again this long on...
MAX_REJECTS = 5               # ...and left alone after this many refusals
SETTLE_AFTER_MS = 120_000     # Predict's result asked for this long after the window's end, then every SETTLE_EVERY_MS
SETTLE_EVERY_MS = 60_000
KEEP_DAYS = 90                # finished records kept this long (the review page's history)
# 1.154.12 — the per-second hours (RULE_FAST_HOURS, Beijing time; 01:00–07:00 by default): 凌晨 1 点后每秒检查，符合规则就下单
WARM_MS = 60_000              # they are prepared for this long before they begin (the sign-in, the allowance, the windows' markets and matching status)...
WARM_EVERY_MS = 30_000        # ...and the preparation renewed this often through them
FAST_LOOK_S = 1.0             # in them every listed window is looked at this often
ALIVE_S = 25.0                # the push connection vouches for the books it holds while a frame (its heartbeat: every 15 s) is this recent
FAST_STALE_S = 20.0           # ...with the push down, a book read over REST is looked at up to this old
VERIFY_EVERY_S, VERIFY_AGAIN_S = 1.0, 3.0  # in them one REST read of a window's book at most this often, the longest unread first, none read again sooner
VERIFY_EVERY_OWN_S, VERIFY_AGAIN_OWN_S = 0.34, 1.0  # ...on a key of the rule taker's own (RULE_API_KEY): three a second
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


def ny_slot(hour: int, minute: int) -> Callable[[int], bool]:
    """The quarter hour from hour:minute New York, Monday to Friday, CME open through it (a holiday's shut and the weekend's
    left out)."""
    def matches(start_ms: int) -> bool:
        weekday, h, mi = et_parts(start_ms)
        return weekday <= 4 and (h, mi) == (hour, minute) and not core.market_shuts("cme", start_ms, start_ms + 15 * 60_000)
    matches.__name__ = f"ny_{hour:02d}{minute:02d}"
    return matches


# 05:15 New York (北京冬令时 18:15，夏令时 17:15): before the London morning fix (10:30 London) gold has leaned down (2025-12 to
# 2026-11: Down 65%, both halves of the year); the slot scan (1.153.2) found two more of gold's slots and one of oil's leaning
# the same way in both halves — 05:00 Up 62%, 16:45 (the last before the daily shut) Up 60%, oil's 00:45 Down 62% — off by default
ny_0515 = ny_slot(5, 15)


@dataclasses.dataclass(frozen=True)
class Rule:
    key: str                       # its price setting (RuleSettings, RULE_SETTING_KEYS) and the records' "rule"
    market: str                    # "gold" / "oil": the switch and the size it goes by (RULE_GOLD / RULE_OIL, RULE_*_SHARES; 1.153.1)
    card: str                      # the card whose windows it buys (WINDOW_UPDOWN_MARKETS)
    side: str                      # "up" / "down"
    name: str                      # 黄金休市 4 档
    when: str                      # its windows, in New York and Beijing time
    matches: Callable[[int], bool]

    @property
    def word(self) -> str:
        return "涨" if self.side == "up" else "跌"


RULES: tuple[Rule, ...] = (
    Rule("gold_shut", "gold", "XAU-15M", "up", "黄金休市 4 档", "纽约 17:00–17:45，周一至周四（北京冬令时 06:00–06:45、夏令时 05:00–05:45）", daily_break),
    Rule("gold_open", "gold", "XAU-15M", "down", "黄金开盘第一档", "纽约 18:00，周一至周四（北京冬令时 07:00、夏令时 06:00；周一早上那场不做）", daily_reopen),
    Rule("gold_0515", "gold", "XAU-15M", "down", "黄金纽约 05:15", "纽约 05:15，周一至周五（北京冬令时 18:15、夏令时 17:15）", ny_0515),
    Rule("gold_0500", "gold", "XAU-15M", "up", "黄金纽约 05:00", "纽约 05:00，周一至周五（北京冬令时 18:00、夏令时 17:00）", ny_slot(5, 0)),
    Rule("gold_1645", "gold", "XAU-15M", "up", "黄金休市前一档", "纽约 16:45，周一至周五（北京冬令时 05:45、夏令时 04:45）", ny_slot(16, 45)),
    Rule("oil_shut", "oil", "CL-15M", "down", "原油休市 4 档", "纽约 17:00–17:45，周一至周四（北京冬令时 06:00–06:45、夏令时 05:00–05:45）", daily_break),
    Rule("oil_0045", "oil", "CL-15M", "down", "原油纽约 00:45", "纽约 00:45，周一至周五（北京冬令时 13:45、夏令时 12:45）", ny_slot(0, 45)),
    Rule("oil_open", "oil", "CL-15M", "down", "原油开盘第一档", "纽约 18:00，周一至周四（北京冬令时 07:00、夏令时 06:00；周一早上那场不做）", daily_reopen),
)
RULE_OF = {r.key: r for r in RULES}
MARKET_NAMES = {"gold": "黄金", "oil": "原油"}


def live_rule(settings: Any, rule: Rule) -> bool:
    """Whether a rule trades now by the settings: its market switched on and a price set."""
    return settings.on(rule.market) and settings.price(rule.key) > 0


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
def rid(slug: str, mode: str) -> str:
    """A record's id (its store key after PREFIX): the window's slug for a real one, ``paper:<slug>`` for a paper one — one record
    a window and a mode (1.153.3: a switch between paper and on in a window's life put real orders on the paper record and paper
    fills on the real one)."""
    return slug if mode == "on" else f"{mode}:{slug}"


def records(store: Any) -> dict[str, dict]:
    return {k.removeprefix(PREFIX): v for k, v in store.items(PREFIX) if isinstance(v, dict)}


def sorted_out(recs: dict[str, dict]) -> tuple[dict[str, dict], list[str]]:
    """The records each under its own id (rid), a record mixing paper and real orders split in two (1.153.3); the old ids no
    longer used."""
    out: dict[str, dict] = {}
    for rec in recs.values():
        orders = rec.get("orders") or []
        for mode, mine in (("on", [o for o in orders if not o.get("paper")]), ("paper", [o for o in orders if o.get("paper")])):
            if not mine and rec.get("mode") != mode:
                continue
            key = rid(str(rec.get("slug") or ""), mode)
            if key in out:
                out[key]["orders"].extend(mine)
            else:
                out[key] = {**rec, "mode": mode, "orders": list(mine)}
                if rec.get("mode") != mode:
                    out[key].pop("seen", None)
                    out[key].update(pnl=None, result="")  # settled again on its own orders
    return out, [k for k in recs if k not in out]


def records_differ(a: dict, b: dict) -> bool:
    return a != b


def fee_share(avg: float, bps: int) -> float:
    """The part of a taker's shares Predict keeps as its fee: the fee (rate × min(p, 1 − p) a share) is taken in shares, not
    USDT (README 1.119.1 / 1.150.0: 97.74 bought, 96.30 held)."""
    return bps / 10_000 * min(avg, 1 - avg) / avg if avg > 0 else 0.0


def order_avg(o: dict) -> float:
    return float(o.get("avg") or o.get("limit") or 0)


def order_bps(o: dict) -> int:
    return int(o.get("fee_bps") if o.get("fee_bps") is not None else 200)


def filled(rec: dict) -> float:
    return sum(float(o.get("filled") or 0) for o in rec.get("orders") or [])


def cost(rec: dict) -> float:
    """What the fills cost in USDT: each order's shares at its average price (its limit while Predict states none). The fee is paid
    in shares (net), not here (1.153.3: it was added to the price as well)."""
    return sum(float(o.get("filled") or 0) * order_avg(o) for o in rec.get("orders") or [])


def net(rec: dict) -> float:
    """The shares the account holds of the fills: those bought less the fee Predict takes in shares."""
    return sum(float(o.get("filled") or 0) * (1 - fee_share(order_avg(o), order_bps(o))) for o in rec.get("orders") or [])


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
        shares = net(rec)  # what the account holds: the fee is taken in shares (1.153.3)
        if shares > DUST:
            k = (str(rec.get("market_id") or ""), int(rec.get("index_set") or 0))
            out[k] = out.get(k, 0.0) + shares
    return out


def unverified(recs: dict[str, dict]) -> set[tuple[str, int]]:
    """The outcomes where a real order's fills are not final yet: a position there may be its own (LiveBot.side_unverified)."""
    return {(str(r.get("market_id") or ""), int(r.get("index_set") or 0)) for r in recs.values() if r.get("mode") == "on" and in_flight(r)}


def spent_on(recs: dict[str, dict], day: str, mode: str) -> float:
    """What the orders placed on a Beijing day cost in USDT, the mode's own records (paper's and the real ones apart): the fills at
    their price, and the whole of an order whose word is not final at its limit (RULE_MAX_DAY_USD)."""
    total = 0.0
    for rec in recs.values():
        if rec.get("mode") != mode:
            continue
        for o in rec.get("orders") or []:
            if day_of(int(o.get("at") or 0)) != day:
                continue
            if o.get("state") in FINAL_STATES:
                total += float(o.get("filled") or 0) * order_avg(o)
            else:
                total += float(o.get("shares") or 0) * float(o.get("limit") or 0)
    return total


def known_orders(recs: dict[str, dict], now_ms: int) -> tuple[set[str], set[str]]:
    """The real orders' ids and hashes of the last two days: the bot's checks of the account's open orders take them as its own
    (1.153.3: a rule order caught on the list was announced as one nobody here placed)."""
    ids, hashes, since = set(), set(), now_ms - 2 * core.DAY_MS
    for rec in recs.values():
        if rec.get("mode") != "on" or int(rec.get("start_ms") or 0) < since:
            continue
        for o in rec.get("orders") or []:
            if o.get("id"):
                ids.add(str(o["id"]))
            if o.get("hash"):
                hashes.add(str(o["hash"]).lower())
    return ids, hashes


def day_pnl(recs: dict[str, dict], day: str) -> float:
    """The real records' P&L settled on a Beijing day: counted with the model trader's against LIVE_MAX_DAILY_LOSS_USD."""
    return sum(float(r.get("pnl") or 0) for r in recs.values() if r.get("mode") == "on" and r.get("settled_day") == day)


def watch_stats(recs: dict[str, dict], rule: str, mode: str, since_ms: int) -> dict:
    """What the watches of a rule's windows saw since ``since_ms`` (RuleTaker.watch, 1.153.2): the windows watched, those that
    offered the rule's side at its price, the shares offered at once on average there, the median lowest offer of the side, the
    median lead of matching's opening over the start (seconds)."""
    seen = [r["seen"] for r in recs.values() if r.get("rule") == rule and r.get("mode") == mode and isinstance(r.get("seen"), dict)
            and int(r["seen"].get("start_ms") or 0) >= since_ms]
    offered = [x for x in seen if float(x.get("offer_max") or 0) > 0]
    mid = lambda xs: statistics.median(xs) if xs else None
    return {"watched": len(seen), "offered": len(offered),
            "offer_avg": round(sum(float(x["offer_max"]) for x in offered) / len(offered), 2) if offered else None,
            "best_mid": mid([float(x["best"]) for x in seen if x.get("best") is not None]),
            "open_lead": mid([(int(x["start_ms"]) - int(x["open_ms"])) / 1000 for x in seen if x.get("open_ms")])}


def review(recs: dict[str, dict], settings: Any, now_ms: int) -> dict | None:
    """The review page's 规则吃单: each rule's totals (windows bought, shares, cost, settled P&L, wins and losses, open), the
    paper and the real apart, and the last records; None when nothing is recorded and the mode is off."""
    if not recs and settings.mode == "off":
        return None
    rows = []
    for rule in RULES:
        for mode in ("on", "paper"):
            mine = [r for r in recs.values() if r.get("rule") == rule.key and r.get("mode") == mode and filled(r) > DUST]
            stats = watch_stats(recs, rule.key, mode, now_ms - 30 * core.DAY_MS)
            if not mine and not stats["watched"] and (mode == "paper" or not live_rule(settings, rule)):
                continue
            done = [r for r in mine if r.get("pnl") is not None]
            rows.append({"rule": rule.key, "name": rule.name, "side": rule.side, "cents": settings.price(rule.key) if live_rule(settings, rule) else 0, "when": rule.when,
                         "mode": mode, "windows": len(mine), "shares": round(sum(filled(r) for r in mine), 4),
                         "cost": round(sum(cost(r) for r in mine), 4), "settled": len(done),
                         "wins": sum(1 for r in done if float(r["pnl"]) > DUST), "losses": sum(1 for r in done if float(r["pnl"]) < -DUST),
                         "pnl": round(sum(float(r["pnl"]) for r in done), 4), "open": len(mine) - len(done), "watch": stats})
    last = sorted((r for r in recs.values() if filled(r) > DUST), key=lambda r: -int(r.get("start_ms") or 0))[:60]
    return {"mode": settings.mode, "markets": {m: {"on": settings.on(m), "shares": settings.shares_of(m), "name": n} for m, n in MARKET_NAMES.items()},
            "max_day_usd": settings.max_day_usd, "rules": rows, "fast": core.day_spans_text(getattr(settings, "fast", ())),
            "recent": [{"slug": r.get("slug"), "rule": r.get("rule"), "name": (RULE_OF.get(r.get("rule")) or RULES[0]).name,
                        "window": r.get("name"), "start_ms": r.get("start_ms"), "side": r.get("side"), "mode": r.get("mode"),
                        "shares": round(filled(r), 4), "avg": round(avg_price(r), 4), "cost": round(cost(r), 4),
                        "result": r.get("result") or "", "pnl": r.get("pnl")} for r in last]}


# --- the pushed books ------------------------------------------------------------------------------------------------------------
class RuleStream:
    """Predict's WebSocket for the rule taker's own windows (1.153.1): each listed window's book pushed the moment it changes
    (predictOrderbook/<market id>; main.PredictStream's protocol: subscribe / unsubscribe by request, the heartbeat echoed), so an
    offer at a rule's price is bought at once — not at the next read. A connection of its own beside the model trader's (whose
    subscriptions follow the cards on its page). While it is down, or a book has not come lately, the taker reads it over REST."""
    FRESH_S = 20.0               # a pushed book stands for this long (the server sends a book again every second or so)
    SILENT_S = 45.0              # no frame for this long (three heartbeats): the connection is made anew
    BACKOFF = (2.0, 120.0)       # the wait before a new connection, doubled per failure
    SYNC_S = 30.0                # the subscriptions brought in step at least this often (at once when the wants change)

    def __init__(self, taker: "RuleTaker", connect: Any = None):
        self.taker = taker
        self.connect = connect or core.ws_connect
        self.wanted: set[str] = set()        # market ids to have pushed
        self.subscribed: set[str] = set()
        self.books: dict[str, tuple] = {}    # market id -> (bids, asks, monotonic time of the push)
        self.versions: dict[str, int] = {}
        self.changed: set[str] = set()       # markets whose book changed since the taker last looked
        self.pushed = asyncio.Event()        # ...set as one comes: the taker's loop wakes at once
        self.wake = asyncio.Event()
        self.pending: dict[int, tuple[str, str]] = {}
        self.request_id = 0
        self.ws: Any = None
        self.connects = 0
        self.frames = 0
        self.last_frame = 0.0                # monotonic time of the last frame of any kind (the heartbeat included): the connection lives
        self.error = ""
        self.backoff = self.BACKOFF[0]

    def want(self, ids: set[str]) -> None:
        if ids != self.wanted:
            self.wanted = set(ids)
            self.wake.set()

    def book(self, mid: str) -> tuple | None:
        """(bids, asks) as last pushed while the connection lives, None when there is none fresh (the REST read stands in)."""
        got = self.books.get(mid)
        if got is None or self.ws is None or time.monotonic() - got[2] > self.FRESH_S:
            return None
        return got[0], got[1]

    def alive(self) -> bool:
        """The connection lives: a frame — a book, or the heartbeat every 15 s — within ALIVE_S. While it does, the book it holds of a
        subscribed market is the market's (a quiet one's is not sent again: 11-17, 75 topics and 1.2 re-sends a second in all)."""
        return self.ws is not None and time.monotonic() - self.last_frame <= ALIVE_S

    def held(self, mid: str) -> tuple | None:
        """(bids, asks, when) as last pushed — or read over REST beside the pushes (merge) — however long ago; None for none."""
        return self.books.get(mid)

    def merge(self, mid: str, bids: tuple, asks: tuple, asked: float) -> bool:
        """A REST read of a window's book beside the pushes (RuleTaker.verify, the per-second hours): dropped when a push came after
        the read was asked for (the push is the newer); else it is the book in hand, and the taker is woken when it differs from
        the one before. True when it differed from a book held — a change no push brought."""
        old = self.books.get(mid)
        if old is not None and old[2] > asked:
            return False
        self.books[mid] = (bids, asks, time.monotonic())
        if old is None or (bids, asks) != (old[0], old[1]):
            self.changed.add(mid)
            self.pushed.set()
            return old is not None
        return False

    def stopped(self) -> bool:
        return self.taker.stop.is_set() or self.taker.owner.stopping.is_set()

    async def pause(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.taker.stop.wait(), seconds)

    def headers(self) -> dict[str, str]:
        key = getattr(self.taker, "key", "") or getattr(self.taker.owner.config, "predict_api_key", "")  # the taker's own key when it has one (1.154.12)
        return {"x-api-key": key} if key else {}

    async def run(self) -> None:
        while not self.stopped():
            if not self.wanted:
                self.wake.clear()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.wake.wait(), 5.0)
                continue
            try:
                ws = await self.connect(core.PREDICT_WS_URL, self.headers())
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.error = core.clean_error(error) or type(error).__name__
                await self.pause(self.backoff)
                self.backoff = min(self.backoff * 2, self.BACKOFF[1])
                continue
            since = self.last_frame = time.monotonic()
            self.ws, self.connects = ws, self.connects + 1
            self.subscribed.clear(); self.pending.clear(); self.versions.clear(); self.books.clear(); self.changed.clear()
            why = ""
            try:
                why = await self.session(ws)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                why = core.clean_error(error) or type(error).__name__
            finally:
                self.ws = None
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(ws.close(), 1.0)
            if self.stopped():
                return
            self.error = why or "连接结束"
            self.backoff = self.BACKOFF[0] if time.monotonic() - since >= 60 else min(self.backoff * 2, self.BACKOFF[1])
            await self.pause(self.backoff)

    async def session(self, ws: Any) -> str:
        tasks = {asyncio.create_task(self.read(ws)), asyncio.create_task(self.sync_loop(ws)), asyncio.create_task(self.taker.stop.wait())}
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            task = next(iter(done))
            if self.stopped():
                return ""
            error = task.exception()
            return (core.clean_error(error) or type(error).__name__) if error else (task.result() or "连接结束")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def read(self, ws: Any) -> str:
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=self.SILENT_S)
            except asyncio.TimeoutError:
                return f"{self.SILENT_S:.0f} 秒没有收到任何帧"
            self.frames += 1
            self.last_frame = time.monotonic()
            await self.handle(ws, raw)

    async def sync_loop(self, ws: Any) -> str:
        while True:
            self.wake.clear()
            for mid in sorted(self.wanted - self.subscribed):
                await self.request(ws, "subscribe", mid)
                self.subscribed.add(mid)
            for mid in sorted(self.subscribed - self.wanted):
                await self.request(ws, "unsubscribe", mid)
                self.subscribed.discard(mid)
                self.books.pop(mid, None)
                self.versions.pop(mid, None)
                self.changed.discard(mid)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.wake.wait(), self.SYNC_S)

    async def request(self, ws: Any, method: str, mid: str) -> None:
        self.request_id += 1
        self.pending[self.request_id] = (method, mid)
        await ws.send(json.dumps({"method": method, "requestId": self.request_id, "params": [core.PREDICT_WS_TOPIC + mid]}))

    async def handle(self, ws: Any, raw: Any) -> None:
        try:
            msg = json.loads(raw)
        except (TypeError, ValueError):
            return
        if not isinstance(msg, dict):
            return
        if msg.get("type") == "R":
            with contextlib.suppress(TypeError, ValueError):
                method, mid = self.pending.pop(int(msg.get("requestId")), ("", ""))
                if mid and (msg.get("success") is False or (msg.get("success") is None and msg.get("error"))):
                    if method == "subscribe":
                        self.subscribed.discard(mid)  # asked for again at the next sync
                    self.error = f"{method} {mid}：{msg.get('error') or '拒绝'}"
        elif msg.get("type") == "M":
            topic = str(msg.get("topic") or "")
            if topic == core.PREDICT_WS_HEARTBEAT:
                await ws.send(json.dumps({"method": "heartbeat", "data": msg.get("data")}))
            elif topic.startswith(core.PREDICT_WS_TOPIC):
                self.take(topic[len(core.PREDICT_WS_TOPIC):], msg.get("data"))

    def take(self, mid: str, data: Any) -> None:
        """A pushed book: kept (an older version dropped, a side left out keeps its own), and the taker woken when it changed."""
        if mid not in self.subscribed or not isinstance(data, dict):
            return
        version = data.get("version")
        if isinstance(version, (int, float)) and not isinstance(version, bool):
            if version < self.versions.get(mid, -1):
                return
            self.versions[mid] = int(version)
        old = self.books.get(mid)
        bids = core.predict_levels(data.get("bids"), True) if isinstance(data.get("bids"), list) else (old[0] if old else ())
        asks = core.predict_levels(data.get("asks"), False) if isinstance(data.get("asks"), list) else (old[1] if old else ())
        self.books[mid] = (bids, asks, time.monotonic())
        if old is None or (bids, asks) != (old[0], old[1]):
            self.changed.add(mid)
            self.pushed.set()


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
        # 1.154.12: RULE_API_KEY — a key of the rule taker's own: its reads, its orders and its push connection on their own budget
        # (PredictPacer, one a key) and their own cooldown; without one, the bot's key and one cooldown with the bot's own client (a
        # 429 on either pauses both — 11-17: ninety of them in 2.3 hours, the taker blind for twenty seconds each)
        own = str(getattr(owner.config, "rule_api_key", "") or "")
        self.key = own or owner.config.predict_api_key
        self.own_key = bool(own) and own != owner.config.predict_api_key
        shared = None if self.own_key else getattr(owner.live, "api", None)
        self.api = preopen.safe_api(live.API_BASES[owner.config.live_chain], self.key, owner.live.wallet, self.before_send, shared)
        self.fast = False                  # in the per-second hours now (RULE_FAST_HOURS; pace)
        self.warm_ms = 0                   # when their preparation last began (warm)
        self.warming: asyncio.Task | None = None
        self.verifying: asyncio.Task | None = None  # the REST read of a window's book under way beside the pushes (verify)
        self.verify_at = 0.0               # monotonic: when the last one began
        self.verifies = self.verify_diffs = self.fast_looks = 0  # REST reads beside the pushes, those that differed, the per-second looks (status)
        self.verify_error = ""
        self.collect: list | None = None   # the orders' requests of the tick under way: sent side by side, awaited at its end (send)
        self.ticks, self.in_tick = 0, False  # the loop's ticks so far, one under way...
        self.gated: tuple[int, str] = (-1, "")  # ...and block(0.0) as the tick under way found it (gate)
        self.track: dict[str, dict] = {}   # window slug -> what is known of it (its rule, start, market, when last looked at, the last word)
        self.planned_ms = 0
        self.sending: dict[str, str] = {}  # hash -> slug while its request is out (before_send's lookup)
        self.allowance: dict[str, tuple[int, int]] = {}
        self.errors: collections.deque = collections.deque(maxlen=20)
        self.error = ""
        self.stop = asyncio.Event()
        self.alive = True
        self.reads = 0
        self.recs: dict[str, dict] = {}  # the records by id (rid), kept in memory: only this taker writes them
        recs, gone = sorted_out(records(owner.store))
        for key, rec in recs.items():
            if records_differ(rec, (owner.store.get(PREFIX + key) or {})):
                owner.store.put(PREFIX + key, rec)
        if gone:
            owner.store.delete_keys([PREFIX + k for k in gone])
        self.recs = recs
        self.stream = RuleStream(self)  # the listed windows' books pushed as they change (1.153.1)

    # -- plumbing --
    def now(self) -> int:
        return int(self.owner.market.now_ms())  # whole ms: the slugs are built from it

    @property
    def store(self) -> Any:
        return self.owner.store

    def record(self, slug: str, mode: str | None = None) -> dict | None:
        """The window's record of the mode (the mode in force by default)."""
        return self.recs.get(rid(slug, mode or self.settings.mode))

    def save(self, rec: dict) -> None:
        key = rid(rec["slug"], rec["mode"])
        self.recs[key] = rec
        self.store.put(PREFIX + key, rec)

    def known(self) -> tuple[set[str], set[str]]:
        return known_orders(self.recs, self.now())

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
        if int(order.get("expires") or 0) * 1000 - self.now() < SEND_MIN_LEFT_MS:
            return "距订单到期不足 5 秒（开场前 5 秒到期），不再发送"
        return self.block(float(order.get("shares") or 0) * float(order.get("limit") or 0))

    def today_pnl(self) -> float:
        return day_pnl(self.recs, day_of(self.now()))

    # -- the loop --
    async def run(self) -> None:
        streaming = asyncio.create_task(self.stream.run(), name="rules-stream")
        try:
            while not self.stop.is_set() and not self.owner.stopping.is_set():
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.note(core.clean_error(error) or type(error).__name__)
                await self.rest()
        finally:
            self.alive = False
            side = [t for t in (streaming, self.warming, self.verifying) if t is not None]
            for task in side:
                task.cancel()
            await asyncio.gather(*side, return_exceptions=True)

    async def rest(self) -> None:
        """Until the next tick: TICK_S, or less — a pushed book that changed wakes the loop at once (an offer bought within
        the moment it shows)."""
        self.stream.pushed.clear()
        waits = {asyncio.ensure_future(self.stop.wait()), asyncio.ensure_future(self.stream.pushed.wait())}
        try:  # 1.153.3: always a wait, however short — a change no look took used to send the loop round without one, the bot frozen
            await asyncio.wait(waits, timeout=REST_MIN_S if self.stream.changed else TICK_S, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waits:
                w.cancel()
            await asyncio.gather(*waits, return_exceptions=True)

    async def tick(self) -> None:
        self.ticks += 1
        self.in_tick = True
        try:
            await self.turn()
        finally:
            self.in_tick = False

    async def turn(self) -> None:
        """One tick's work: the plan, the pace, every window's look, the orders' sends, a REST read beside the pushes, the follow-ups."""
        now = self.now()
        if now - self.planned_ms >= PLAN_EVERY_MS:
            self.plan(now)
            self.planned_ms = now
        self.pace(now)
        watched = {str(w["market"]["id"]) for w in self.track.values() if w["market"] is not None}
        self.stream.changed &= watched  # a change of a window no longer watched is nobody's to take (1.153.3)
        if self.settings.mode == "off" or self.cooling() > 0:
            self.stream.changed.clear()  # nothing looked at now: the next look reads the book as it stands then
        if self.settings.mode != "off":
            self.collect = []
            try:
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
            finally:
                sends, self.collect = self.collect, None
            if sends:  # 1.154.12: the windows' orders go out side by side (offers showing on several at once: none waits for another's answer)
                for failed in await asyncio.gather(*sends, return_exceptions=True):
                    if isinstance(failed, asyncio.CancelledError):
                        raise failed
                    if isinstance(failed, BaseException):  # (send answers Predict's failures itself: this is its own — the hash stays, found by it)
                        self.note(f"下单过程出错：{core.clean_error(failed) or type(failed).__name__}")
            if self.fast:
                self.verify_tick()
        self.stream.want({str(w["market"]["id"]) for w in self.track.values() if w["market"] is not None})
        await self.follow()
        await self.settle()

    def pace(self, now: int) -> None:
        """The per-second hours (RULE_FAST_HOURS, 1.154.12): whether they run now, and their preparation — begun WARM_MS before
        them and renewed every WARM_EVERY_MS through them, beside the loop (warm)."""
        wait = self.settings.fast_in(now) if self.settings.mode != "off" else None
        fast = wait == 0
        if fast != self.fast:
            self.fast = fast
            LOG.info("规则吃单：每秒检查时段%s", "开始" if fast else "结束")
        if wait is not None and wait <= WARM_MS and now - self.warm_ms >= WARM_EVERY_MS and (self.warming is None or self.warming.done()):
            self.warm_ms = now
            self.warming = asyncio.create_task(self.warm(), name="rules-warm")

    async def warm(self) -> None:
        """Ready before an offer shows: every window not yet found looked for at the next tick; a market not yet seen open for matching
        read once more (an offer found on it would wait for that read); and, for real orders, the sign-in and the exchange
        allowances in hand — an order then goes out on the first look, no request before it."""
        import live
        try:
            for w in self.track.values():
                if w["market"] is None:
                    w["probe_ms"] = 0
            if self.cooling() > 0:
                return
            for slug, w in sorted(list(self.track.items()), key=lambda kv: kv[1]["start"]):
                if w["market"] is None or w["open_ms"] or self.stop.is_set() or slug not in self.track:
                    continue
                now = self.now()
                self.reads += 1
                w["market"], w["status_ms"] = {**await self.api.market(str(w["market"]["id"])), "categorySlug": slug}, now
                if str(w["market"].get("tradingStatus") or "").upper() == "OPEN":
                    w["open_ms"] = now
                    self.watch(w)["open_ms"] = now
            if self.settings.mode != "on" or self.block(0.0):
                return
            await self.api.ensure_auth()
            exchanges = set()
            for w in list(self.track.values()):
                if w["market"] is not None:
                    with contextlib.suppress(Exception):
                        info = live.parse_market(w["market"], self.owner.config.predict_fee_bps)
                        exchanges.add(live.exchange_key(info.neg_risk, info.yield_bearing))
            for exchange in sorted(exchanges):
                self.allowance[exchange] = (self.now(), int(await self.owner.live.chain.allowance(exchange)))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.note(f"每秒检查的准备没做完：{core.clean_error(error) or type(error).__name__}")

    def gate(self) -> str:
        """block(0.0) — why no real order could go out now — worked out once a tick: every window's look asked it afresh, and it
        sums the day's P&L over the model trader's and the pre-open mode's records (tens of thousands) for the loss stop; in the
        per-second hours that was several times a second (1.154.12). An order's own checks (place, before_send) stay fresh.
        Outside a tick (a look called by itself) it is worked out there and then."""
        if not self.in_tick:
            return self.block(0.0)
        if self.gated[0] != self.ticks:
            self.gated = (self.ticks, self.block(0.0))
        return self.gated[1]

    def finished(self, slug: str, rule: Rule) -> str:
        """Why nothing more is bought in this window, "" while something may be: bought in full, an order still out, recorded on
        paper, refused too often."""
        rec = self.record(slug)
        if rec is None:
            return ""
        size = self.settings.shares_of(rule.market)
        if held(rec) >= size - DUST or in_flight(rec):
            return f"这一场已买满 {size:g} 份" if not in_flight(rec) else "上一张单还没有结果"
        if self.settings.mode == "paper" and any(o.get("paper") for o in rec["orders"]):
            return "只记录：这一场已记过（只记一次：没真买，盘口上的单还在，再记就重复了）"
        if sum(1 for o in rec["orders"] if o.get("state") == "rejected") >= MAX_REJECTS:
            return f"Predict 拒绝了 {MAX_REJECTS} 次，这一场不再下单"
        return ""

    def verify_tick(self) -> None:
        """The per-second hours' REST read beside the pushes: one window's book at a time, the longest unread first, at most one
        every VERIFY_EVERY_S and none again sooner than VERIFY_AGAIN_S (three a second on a key of the taker's own) — a change
        the push did not bring is seen within seconds, and with the push down the looks go on on these reads. Bounded: seven
        windows read every second would be the key's whole budget. Not while no order could go out, nor for a window done with."""
        if self.verifying is not None and not self.verifying.done():
            return
        mono = time.monotonic()
        every, again = (VERIFY_EVERY_OWN_S, VERIFY_AGAIN_OWN_S) if self.own_key else (VERIFY_EVERY_S, VERIFY_AGAIN_S)
        if mono - self.verify_at < every or self.cooling() > 0 or (self.settings.mode == "on" and self.gate()):
            return
        now = self.now()
        due = [(w.get("rest_at", 0.0), slug) for slug, w in self.track.items()
               if w["market"] is not None and w["start"] - now > STOP_MS and now >= w["look_ms"] and mono - w.get("rest_at", 0.0) >= again
               and not self.finished(slug, RULE_OF[w["rule"]])]
        if not due:
            return
        slug = min(due)[1]
        w = self.track[slug]
        w["rest_at"] = self.verify_at = mono
        self.verifying = asyncio.create_task(self.verify(str(w["market"]["id"])), name="rules-verify")

    async def verify(self, mid: str) -> None:
        asked = time.monotonic()
        try:
            self.reads += 1
            self.verifies += 1
            bids, asks = await self.book(mid)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.verify_error = f"{bj(self.now(), False)} {core.clean_error(error) or type(error).__name__}"
            return
        self.verify_error = ""
        if self.stream.merge(mid, bids, asks, asked):
            self.verify_diffs += 1

    def plan(self, now: int) -> None:
        """The windows of the enabled rules from now to a day on (HORIZON_MS); a window of a rule switched off is dropped, and so
        is one this close to its start (STOP_MS). The records past KEEP_DAYS and finished are removed."""
        want: dict[str, tuple[str, int]] = {}
        if self.settings.mode != "off":
            for rule in RULES:
                if live_rule(self.settings, rule):  # its market switched on (RULE_GOLD / RULE_OIL) and a price set
                    for start in windows(rule, now + STOP_MS, now + HORIZON_MS):
                        want[card_of(rule).slug_of(start)] = (rule.key, start)
        for slug in list(self.track):
            if slug not in want:
                w = self.track.pop(slug)
                if w["start"] - now <= STOP_MS:  # its end reached between two looks: its watch kept all the same
                    self.keep_watch(slug, w)
        for slug, (key, start) in want.items():
            self.track.setdefault(slug, {"rule": key, "start": start, "market": None, "listed_ms": 0, "open_ms": 0, "probe_ms": 0,
                                         "look_ms": 0, "status_ms": 0, "offer": False, "word": ""})
        old = [key for key, r in self.recs.items()
               if int(r.get("start_ms") or 0) < now - KEEP_DAYS * core.DAY_MS and not in_flight(r) and (r.get("pnl") is not None or filled(r) <= DUST)]
        if old:
            self.store.delete_keys([PREFIX + key for key in old])
            for key in old:
                self.recs.pop(key, None)

    def due(self, w: dict, now: int) -> bool:
        """Whether the window is to be looked at this tick: at once when its pushed book changed (1.153.1); else by its cadence —
        the listing looked for around its moment, every FAST_S in the last NEAR_MS, every OFFER_STATUS_S while an offer at the
        price waits for matching to open, the rest every SLOW_S (CLOSED_SLOW_S before matching opens); in the per-second hours
        (RULE_FAST_HOURS, 1.154.12) every listed window every FAST_LOOK_S."""
        start, near = w["start"], w["start"] - now <= NEAR_MS
        if w["market"] is None:
            expected = start - LISTED_AHEAD_MS
            if now < expected - PROBE_EARLY_MS:
                return False
            every = PROBE_FAST_S if now <= expected + PROBE_LATE_MS or near else PROBE_SLOW_S
            return now - w["probe_ms"] >= every * 1000
        mid = str(w["market"]["id"])
        if now < w["look_ms"]:  # paused (a refusal): a change meanwhile is not one to act on (1.153.3: it kept the loop going round)
            self.stream.changed.discard(mid)
            return False
        if mid in self.stream.changed:
            return True
        every = (FAST_LOOK_S if self.fast  # the per-second hours (1.154.12)
                 else FAST_S if near else OFFER_STATUS_S if (w["offer"] and not w["open_ms"]) else SLOW_S if w["open_ms"] else CLOSED_SLOW_S)
        return now - w["look_ms"] >= every * 1000

    async def look(self, slug: str, w: dict) -> None:
        """One look at a window before its start: its market found as Predict lists it; its book (pushed, or read); the offers at
        the rule's price (the pre-open mode's own bids taken out); matching's status read when there is something to buy and
        it has not opened; then the take — up to the market's RULE_*_SHARES a window and RULE_MAX_DAY_USD a day."""
        now = self.now()
        rule = RULE_OF[w["rule"]]
        if w["start"] - now <= STOP_MS:
            self.track.pop(slug, None)
            self.keep_watch(slug, w)
            return
        if not self.due(w, now):
            return
        if w["market"] is not None:
            self.stream.changed.discard(str(w["market"]["id"]))  # taken now, whatever this look decides (1.153.3)
        rec = self.record(slug)
        size = self.settings.shares_of(rule.market)
        if done := self.finished(slug, rule):
            w["word"] = done
            return
        if self.settings.mode == "on" and (why := self.gate()):
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
            w["market"], w["listed_ms"], w["status_ms"] = market, now, now  # the listing's own read carries the status
            if str(market.get("tradingStatus") or "").upper() == "OPEN":
                w["open_ms"] = now
            self.watch(w)["listed_ms"] = now
        w["look_ms"] = now
        mid = str(w["market"]["id"])
        pushed = self.stream.book(mid)
        asked = time.monotonic()
        if self.fast:
            # 1.154.12, the per-second hours: the book in hand — the push connection's while it lives (it vouches for a quiet market's
            # book, which is not sent again), else a REST read's (verify) while young. Read here only with none in hand and no read
            # of this window in the last FAST_STALE_S (the hours' first look, a connection just made): every window read every
            # second would be the key's whole budget — otherwise the look waits for verify's next read
            self.fast_looks += 1
            if pushed is None:
                got = self.stream.held(mid)
                if got is not None and (self.stream.alive() or asked - got[2] <= FAST_STALE_S):
                    pushed = got[:2]
                elif asked - w.get("rest_at", -1e9) < FAST_STALE_S:
                    w["word"] = "每秒检查：等盘口（推送还没送到，REST 轮流读取中）"
                    return
                else:
                    w["rest_at"] = asked
        if pushed is None:
            self.reads += 1
        bids, asks = pushed if pushed is not None else await self.book(mid)
        if pushed is None and self.fast:
            self.stream.merge(mid, bids, asks, asked)  # in hand for the next looks
            self.stream.changed.discard(mid)           # ...and taken by this one
        limit = D(self.settings.price(rule.key)) / 100
        levels = offered(bids, asks, rule.side, limit) if limit > 0 else []
        if levels:  # (1.154.12: the pre-open mode's ledger — tens of thousands of records — is walked only when something is offered at all)
            own = self.own_asks(slug, rule.side)
            if own and min(own) <= limit:
                # 1.153.3: the pre-open mode's own bids there are offers of this side in the book; a buy at the rule's price would take
                # them first (the best price first) — the account trading with itself, the fee paid for nothing. Priced a cent under
                # the cheapest of them instead, sized from the others' offers below it
                limit = min(own) - D("0.01")
                levels = offered(bids, asks, rule.side, limit) if limit > 0 else []
        w["offer"] = bool(levels)
        best = offered(bids, asks, rule.side, D("0.99"))[:1]
        self.note_offers(w, levels, best, now)
        if not levels:
            w["word"] = f"盘口没有 ≤{int(limit * 100)}¢ 的{rule.word}" + (f"（最低 {float(best[0][0]) * 100:g}¢）" if best else "（没人卖）")
            return
        if not w["open_ms"]:
            near = w["start"] - now <= NEAR_MS
            if now - w["status_ms"] >= (FAST_S if near or self.fast else OFFER_STATUS_S) * 1000:
                w["status_ms"] = now
                self.reads += 1
                w["market"] = {**await self.api.market(mid), "categorySlug": slug}
                if str(w["market"].get("tradingStatus") or "").upper() == "OPEN":
                    w["open_ms"] = now
                    self.watch(w)["open_ms"] = now
            if not w["open_ms"]:
                w["word"] = (f"盘口已有 ≤{int(limit * 100)}¢ 的{rule.word} {sum(q for _, q in levels):g} 份，等撮合开启就买"
                             f"（现在 {w['market'].get('tradingStatus') or '?'}）")
                return
        room = D(str(size)) - D(str(held(rec) if rec else 0.0))
        take = min(sum(q for _, q in levels), room)
        if self.settings.mode == "on":
            take = min(take, D(str(self.owner.config.live_max_order_usd)) / limit)  # LIVE_MAX_ORDER_USD per order
        if self.settings.max_day_usd:
            left = self.settings.max_day_usd - spent_on(self.recs, day_of(now), self.settings.mode)
            take = min(take, D(str(max(left, 0.0))) / limit)
            if left < 1.0:
                w["word"] = f"今天已花 ${self.settings.max_day_usd - left:,.2f}，到了每天上限 ${self.settings.max_day_usd:g}（RULE_MAX_DAY_USD）"
                return
        take = take.to_integral_value(rounding="ROUND_FLOOR")
        if take < 1 or float(take * limit) < 1.0:  # Predict's minimum order ($1, live.MIN_ORDER_USD)
            w["word"] = f"盘口 ≤{int(limit * 100)}¢ 的{rule.word}只有 {sum(q for _, q in levels):g} 份，不够一张最小订单（或额度剩得不够）"
            return
        if self.settings.mode == "paper":
            self.paper(slug, w, rule, levels, take, limit, rec)
        else:
            await self.place(slug, w, rule, levels, take, limit, rec)

    def own_asks(self, slug: str, side: str) -> dict[D, D]:
        """The pre-open mode's resting bids on this window's other side, as offers of ``side`` (price -> shares): its Up bid at
        49¢ is a Down offer at 51¢ in the book. The shut and the reopen's first are never quoted (1.150.1 / 1.151.1); the slots
        05:15, 05:00, 16:45 and oil's 00:45 can be."""
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

    @staticmethod
    def watch(w: dict) -> dict:
        """What the window's watch saw (1.153.2), kept on its record at the end: the lowest offer of the rule's side at any price
        (best), the most shares offered at the rule's price at once (offer_max) and when first (offer_ms), when it was listed and
        when matching was first seen open — what /rules and the review page sum up to show whether a price or a size is off."""
        return w.setdefault("seen", {"best": None, "offer_max": 0.0, "offer_ms": 0, "listed_ms": 0, "open_ms": 0})

    def note_offers(self, w: dict, levels: list, best: list, now: int) -> None:
        seen = self.watch(w)
        if best:
            seen["best"] = min(float(best[0][0]), seen["best"] if seen["best"] is not None else 1.0)
        total = float(sum(q for _, q in levels))
        if total > seen["offer_max"]:
            seen["offer_max"] = total
        if total > 0 and not seen["offer_ms"]:
            seen["offer_ms"] = now
        if w["open_ms"] and not seen["open_ms"]:
            seen["open_ms"] = w["open_ms"]

    def keep_watch(self, slug: str, w: dict) -> None:
        """The window's watch onto its record as it ends (one made for it when nothing was bought, so the windows that offered
        nothing — or nothing at the price — are counted too)."""
        seen = w.get("seen")
        if not seen or not seen.get("listed_ms") or w["market"] is None or self.settings.mode == "off":
            return
        rule = RULE_OF[w["rule"]]
        rec = self.record(slug) or self.new_record(slug, w, rule, 0)
        rec["seen"] = {**seen, "start_ms": w["start"], "cents": self.settings.price(rule.key)}
        self.save(rec)

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
                              "avg": float(avg), "fee_bps": info.fee_bps, "at": self.now(),
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
        expires = w["start"] // 1000 - EXPIRY_BEFORE_S  # before the start, always: nothing of it matchable once the window runs
        amounts = live.limit_amounts(True, live.to_wei(limit), live.to_wei(take))
        wallet = self.owner.live.wallet
        identity = f"rule-v1|{self.owner.config.live_chain}|{wallet.maker.lower()}|{info.market_id}|{outcome['token']}|{w['start']}|{len(rec['orders'])}"
        salt = int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big") % live.MAX_SALT
        signed = live.build_order(wallet, outcome["token"], amounts["maker"], amounts["taker"], info.fee_bps, expires, salt=salt)
        digest, signature = wallet.sign_order(signed, info.neg_risk, info.yield_bearing)
        order = {"hash": digest.lower(), "id": "", "state": "placing", "shares": float(take), "limit": float(limit), "filled": 0.0,
                 "avg": None, "fee_bps": info.fee_bps, "at": now, "expires": expires,
                 "book": [[float(p), float(q)] for p, q in levels[:5]]}
        rec["orders"].append(order)
        self.save(rec)
        body = {"data": {"order": {**signed, "signature": signature, "hash": digest}, "strategy": "LIMIT",
                         "pricePerShare": str(amounts["price_per_share"])}}
        # 1.154.12: the request itself — the hash onto the disk, the POST, its answer — goes beside the other windows' (send). The
        # looks still go one by one and the record above is made within this one, so the next window's look counts this order (the
        # day's cap) while it is on its way; the tick waits for every send before it follows the orders ("placing" = in flight)
        task = asyncio.ensure_future(self.send(slug, w, rule, rec, order, body, take, limit))
        if self.collect is not None:
            self.collect.append(task)
        else:
            await task

    async def send(self, slug: str, w: dict, rule: Rule, rec: dict, order: dict, body: dict, take: D, limit: D) -> None:
        """One prepared order out: its hash on the disk first (a crash in between is found by it), then the request; Predict's answer
        onto the record. Run beside the other windows' sends of the same tick (place)."""
        import live
        import preopen
        if not await asyncio.to_thread(self.store.flush):  # the hash on disk before the request: a crash in between is found by it
            order.update(state="not_sent", error="数据库写入未完成，没有发单")
            self.save(rec)
            return
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
                order.update(state="not_sent", error=text)  # the rate limiter, not the order: not one of MAX_REJECTS (1.153.3)
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
                if order.get("paper") or order.get("state") in FINAL_STATES or now < int(order.get("next_ms") or 0) or self.cooling() > 0:
                    continue
                try:
                    await self.follow_order(rec, order, now)
                except Exception as error:
                    order["error"] = core.clean_error(error) or type(error).__name__
                    order["next_ms"] = now + UNKNOWN_EVERY_MS
                self.save(rec)

    async def follow_order(self, rec: dict, order: dict, now: int) -> None:
        """One read of a real order: its fills taken; filled in full, final; off the book (withdrawn, cancelled, expired) it is
        final only by a read LATE_FILL_MS after it left (a fill matched just before may show late), still on it its rest is
        withdrawn. Past its signed expiry by GIVE_UP_MS it is final whatever its state, on the fills known (1.153.3)."""
        import live
        state = order["state"]
        if state == "placing":  # a request that left with the process gone before its answer (a restart): looked for by its hash
            order.update(state="unknown", next_ms=now)
            state = "unknown"
        late = now >= int(order.get("expires") or 0) * 1000 + GIVE_UP_MS
        self.reads += 1
        row = await self.api.order(order.get("id") or "", order["hash"])
        if row is None:
            if late:
                if state == "unknown" and not order.get("id"):
                    order.update(state="not_found", error="按哈希一直查不到：视为没下成")
                else:
                    order.update(state="final", error=f"到期后 {GIVE_UP_MS // 60_000} 分钟仍查不到这张单：按已知成交 {float(order.get('filled') or 0):g} 份记")
            else:
                order["next_ms"] = now + (UNKNOWN_EVERY_MS if state == "unknown" else FOLLOW_EVERY_MS)
            return
        order["id"] = str(row.get("id") or order.get("id") or "")
        self.take_fills(rec, order, row)
        if float(order.get("filled") or 0) >= float(order["shares"]) - DUST:
            order["state"] = "final"
            return
        if live.order_final(row, float(order["shares"])) or order.get("removed"):  # off the book
            order["left_ms"] = int(order.get("left_ms") or now)
            if now - order["left_ms"] >= LATE_FILL_MS or late:
                order["state"] = "final"
            else:
                order.update(state="closing", next_ms=order["left_ms"] + LATE_FILL_MS)
            return
        if late:
            order.update(state="final", error="到期后仍显示挂着：按已知成交记")
            return
        if order.get("id"):  # still open: the rest withdrawn (a taker never waits on the book)
            result = await self.api.remove_orders([order["id"]])
            if order["id"] in (result.get("removed") or []) or order["id"] in (result.get("noop") or []):
                order.update(removed=now, left_ms=now)
            order.update(state="cancelling", next_ms=now + FOLLOW_EVERY_MS)
        else:
            order["next_ms"] = now + UNKNOWN_EVERY_MS

    def take_fills(self, rec: dict, order: dict, row: dict) -> None:
        """An order record's fills onto ours: the shares, the average price it states (its limit while none is stated)."""
        import live
        shares, _ = live.order_fill(row, float(order["shares"]))
        price = live.order_price(row)
        if price is not None:
            order.update(avg=price, avg_stated=True)
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
            pnl = net(rec) * pay - cost(rec)  # the shares held (the fee taken in shares) paid out, less the USDT spent (1.153.3)
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
    def fast_line(self, now: int) -> str:
        """The per-second hours in a line (/rules): the hours, whether they run, what the looks go on, how the REST reads beside the
        pushes fared; which key the taker's requests use."""
        s = self.settings
        key = "｜请求用规则吃单自己的 Key（RULE_API_KEY）" if self.own_key else ""
        if not s.fast:
            return "每秒检查时段：关（RULE_FAST_HOURS=off）：推送一有变化就看，另外每 30 秒读一次、开场前 15 分钟每 2 秒" + key
        text, wait = f"每秒检查时段（北京时间）：{core.day_spans_text(s.fast)}", s.fast_in(now)
        if wait == 0:
            listed = sum(1 for w in self.track.values() if w["market"] is not None)
            text += (f"｜进行中：每秒看 {listed} 场（已看 {self.fast_looks} 次）；"
                     + ("推送正常" if self.stream.alive() else "推送断开，靠 REST 轮流读")
                     + f"；REST 核对 {self.verifies} 次" + (f"，{self.verify_diffs} 次发现推送没送到的变化" if self.verify_diffs else "")
                     + (f"；核对读取失败：{self.verify_error}" if self.verify_error else ""))
        else:
            text += f"｜下一次 {bj(now + wait, False)} 开始" + ("，正在准备（登录、授权额度、撮合状态、推送订阅）" if wait <= WARM_MS else "，提前 1 分钟准备")
        return text + key

    def status(self) -> str:
        now, s = self.now(), self.settings
        recs = self.recs
        lines = [f"规则吃单：{MODES.get(s.mode, s.mode)}（RULE_TAKER={s.mode}）｜只在开场前吃，盘口一有就买，到开场前 {STOP_MS // 1000} 秒"
                 + (f"｜每天最多花 ${s.max_day_usd:g}（今天已花 ${spent_on(recs, day_of(now), s.mode):,.2f}）" if s.max_day_usd else "")
                 + f"｜推送 {'已连接' if self.stream.ws is not None else '未连接' + ('（' + self.stream.error + '）' if self.stream.error else '')}"]
        if s.mode == "on" and (why := self.block(0.0)):
            lines.append(f"⏸ 现在不下单：{why}")
        if s.mode != "off":
            lines.append(self.fast_line(now))
        for market, name in MARKET_NAMES.items():
            lines.append(f"{name}：{'开' if s.on(market) else '关'}" + (f"｜每场最多 {s.shares_of(market):g} 份" if s.on(market) else "（这个市场的规则都不做）"))
            if not s.on(market):
                continue
            for rule in (r for r in RULES if r.market == market):
                cents = s.price(rule.key)
                if cents <= 0:
                    lines.append(f"· {rule.name}：不做（价格设为 0）")
                    continue
                nxt = min((w for w in self.track.values() if w["rule"] == rule.key), key=lambda w: w["start"], default=None)
                st = watch_stats(recs, rule.key, s.mode, now - 30 * core.DAY_MS) if s.mode != "off" else {"watched": 0}
                lines.append(f"· {rule.name} 买{rule.word} ≤{cents}¢：{rule.when}"
                             + (f"｜下一场 {bj(nxt['start'])}{('：' + nxt['word']) if nxt['word'] else ''}" if nxt else "")
                             + (f"｜近 30 天看了 {st['watched']} 场，{st['offered']} 场有 ≤{cents}¢ 的卖单"
                                + (f"（平均 {st['offer_avg']:g} 份）" if st["offer_avg"] else "")
                                + (f"，最低卖价中位 {st['best_mid'] * 100:.0f}¢" if st["best_mid"] is not None else "")
                                + (f"，撮合开启在开场前中位 {st['open_lead']:.0f} 秒" if st["open_lead"] is not None else "") if st["watched"] else ""))
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
