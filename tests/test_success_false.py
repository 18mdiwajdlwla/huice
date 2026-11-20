"""1.154.33 (an outside review, R17): Predict's refusal of an order in a 2xx answer — {"success": false, "error": …} — through the
client the pre-open mode and the rule taker share (preopen.safe_api). Its success check raised a plain error with no status
before live.PredictApi.create_order could read the answer as the refusal it is (OrderRejected, as the bot's own client does),
so both took the refused order for one that may exist ("unknown"): its money held and its hash looked up until its end, its
outcome kept from the model trader's positions check. Now the order's refusal is a refusal, in the same words; the
duplicate's wording is still an order that may exist, and any other request's success=false the plain error it was.
Entirely offline."""
import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import offline  # noqa: F401
import main as m
import live as L
import preopen as p
import rules as R
import test_preopen as TP
import test_rules as TR

REFUSED = {"success": False, "error": {"message": "post-only order would cross the book"}}
DUPLICATE = {"success": False, "message": "Order already exists"}


def answering(body):
    """The request itself (main.predict_blocking) answered with ``body`` and a 2xx status, whatever was asked. It lets the event
    loop go once, as a request on its way does: the two sides of a pair both go out before either answer is in."""
    async def transport(fn, *args, pool=None):
        await asyncio.sleep(0)
        return json.dumps(body).encode()
    return transport


async def the_clients():
    """The same answer to the bot's own client and to the shared one: the same refusal, in the same words."""
    own, shared = L.PredictApi("https://offline.invalid/v1", "k", None), p.safe_api("https://offline.invalid/v1", "k", None)
    own.jwt = shared.jwt = "offline"
    errors = []
    with patch.object(m, "predict_blocking", answering(REFUSED)):
        for client in (own, shared):
            try:
                await client.create_order({"data": {}})
            except Exception as error:
                errors.append(error)
        assert [type(e) for e in errors] == [L.OrderRejected, L.OrderRejected], errors
        assert str(errors[0]) == str(errors[1]) == "下单被拒绝：post-only order would cross the book", [str(e) for e in errors]
        # any other request's success=false: the plain error, as before (a list is never read as empty, a cancel keeps its state)
        for name, call in (("POST /orders/remove", lambda: shared.remove_orders(["1"])), ("GET /orders", shared.orders),
                           ("GET /orders/0xh", lambda: shared.order("1", "0xh")), ("GET /markets/1", lambda: shared.market("1"))):
            try:
                await call()
            except L.OrderRejected as error:
                raise AssertionError(f"{name}: no order's refusal ({error})")
            except m.RemoteError as error:
                assert f"Predict 应答不是已确认成功的数据（{name}）" in str(error), (name, str(error))
            else:
                raise AssertionError(f"{name}: success=false read as a success")
    with patch.object(m, "predict_blocking", answering({"success": False})):  # no words of its own
        try:
            await shared.create_order({"data": {}})
        except L.OrderRejected as error:
            assert str(error) == "下单被拒绝：Predict 没有说明原因", str(error)
        else:
            raise AssertionError("refused, without a reason: still a refusal")
    with patch.object(m, "predict_blocking", answering(["?"])):  # not even a dict: an answer that tells nothing, as before
        try:
            await shared.create_order({"data": {}})
        except L.OrderRejected as error:
            raise AssertionError(f"no refusal in that answer ({error})")
        except m.RemoteError as error:
            assert "（POST /orders）" in str(error), str(error)


async def the_pre_open_mode():
    """place_pair: both sides refused so — rejected, nothing reserved, no look-up by hash, the window not quoted again (as for
    an HTTP 400); the duplicate's wording — unknown, its money kept and looked for by its hash, as before."""
    for body, state in ((REFUSED, "rejected"), (DUPLICATE, "unknown")):
        ctl, api, clock = TP.controller()
        made = ctl.api_made  # the client the mode makes itself (Controller.__init__: safe_api, its before_send)
        made.jwt = "offline"
        api.create_order = made.create_order  # (the orders on it; every other request on the stand-in)
        reads, plain = [], api.order

        async def order(oid, digest=""):
            reads.append(digest)
            return await plain(oid, digest)
        api.order = order
        c = p.candidate(TP.market(), TP.NOW, ctl.owner.config, ctl.settings)
        with patch.object(m, "predict_blocking", answering(body)):
            await ctl.place_pair(c)
        await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)
        pair = ctl.ledger.rows[c["slug"]]
        assert {o["state"] for o in pair["orders"].values()} == {state}, (body, {s: (o["state"], o.get("error")) for s, o in pair["orders"].items()})
        assert all(o["error"].startswith("下单被拒绝：") for o in pair["orders"].values()), [o["error"] for o in pair["orders"].values()]
        if state == "rejected":
            assert (pair.get("accounted"), pair.get("pnl"), ctl.risk(), reads, ctl.unverified_outcomes()) == (True, 0.0, 0.0, [], set()), \
                (pair.get("accounted"), ctl.risk(), reads, ctl.unverified_outcomes())
            assert not ctl.supersedable(pair), "Predict's refusal: the same order would be refused again"
        else:
            assert (pair.get("accounted"), ctl.risk(), len(reads), len(ctl.unverified_outcomes())) == (None, 49.0, 2, 2), \
                (pair.get("accounted"), ctl.risk(), reads, ctl.unverified_outcomes())
        for task in ctl.settling.values():
            task.cancel()
        await asyncio.gather(*ctl.settling.values(), return_exceptions=True)
        ctl.ledger.close()


async def the_rule_taker():
    """rules.RuleTaker.send through the client the taker makes itself (preopen.safe_api, in RuleTaker.__init__): rejected — the
    window paused for REJECT_PAUSE_MS, as for an HTTP 400 — and the duplicate's wording unknown, looked for by its hash, as before."""
    for body, state in ((REFUSED, "rejected"), (DUPLICATE, "unknown")):
        _, _, clock, x = TR.taker("on")
        t = R.RuleTaker(x.owner)  # the taker's own client, made where the taker makes it
        client, api = t.api, TR.Api(t)
        t.api = api
        client.jwt = "offline"
        slug = f"xau-updown-15m-{TR.GOLD_0600 // 1000}"
        api.markets["901"] = TR.gold_market(TR.GOLD_0600)
        api.listed.add(slug)
        api.books["901"] = {"bids": [], "asks": [[0.51, 30]]}
        api.create_order = client.create_order  # (the order on it; every other request on the stand-in)
        try:
            with patch.object(m, "predict_blocking", answering(body)):
                await t.tick()
            order = t.record(slug)["orders"][0]
            assert (order["state"], order["error"][:6]) == (state, "下单被拒绝："), (body, order)
            if state == "rejected":
                assert t.track[slug]["look_ms"] == clock.ms + R.REJECT_PAUSE_MS, "a refusal pauses the window (REJECT_PAUSE_MS)"
            else:
                assert order["next_ms"] == clock.ms + R.UNKNOWN_EVERY_MS, "an order that may exist: looked for by its hash"
        finally:
            while m.GC_WATCH.holders:  # (as tests/test_rules.py lets go of what a tick held of the collector)
                m.GC_WATCH.release()


async def main():
    await the_clients()
    await the_pre_open_mode()
    await the_rule_taker()


asyncio.run(main())
print("SUCCESS_FALSE_OK")
