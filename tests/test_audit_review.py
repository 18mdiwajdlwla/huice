"""1.154.33 — an outside review of 1.154.32 (two reports, CR-01..12 and R01..24), its findings checked against the code one by
one; the core module's that held (main.py) — the rule taker's, the pre-open mode's and the real trader's have their own suites:
R11 an after-hours floor order past the after-hours price limit, R13 a minutes-old trade leading a 15-minute stock window,
R19 late pushed index frames keeping the REST read off, R21 /samples reading every sample twice on the event loop, R22 a body
trickling past the total deadline, R23 `--calib` writing to the database it reports on (R14: tests/test_store.py; CR-07:
tests/test_audit_live-b.py)."""
import asyncio, contextlib, io, json, os, socket, sqlite3, sys, tempfile, threading, time, http.client, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP; a local plain socket is no network)
import main as m
D = m.D


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


# --- R11: after hours a floor order only at or under SIM_AFTER_HOURS_MAX_PRICE_CENTS, as the ordinary ones --------------------------
NOW = int(dt.datetime(2026, 11, 10, 12, 0, tzinfo=m.BEIJING).timestamp() * 1000)
SETTLE = {"key": "HSI", "target": "2026-11-10", "line": 24600.0, "close_ms": NOW + 4 * 3600_000}


def card(in_session, bids=(("0.30", "50"),), asks=(("0.62", "100"),)):
    book = m.PredictBook("HSI", "hsi-floor", "401", "t", tuple((D(p), D(q)) for p, q in bids), tuple((D(p), D(q)) for p, q in asks), NOW, 200)
    return m.SimMarket("hsi-floor", "恒生指数", "close", "HSI", 0.50, book, 0.03, "", ("涨", "跌"), SETTLE, {}, in_session=in_session)


env = {"TELEGRAM_BOT_TOKEN": "1:x", "SIM_WAYS": "maker", "SIM_MAKER_POINTS": "off", "SIM_MAKER_AFTER_HOURS_CENTS": "10", "SIM_MAKER_FLOOR_CENTS": "20"}
bot = m.Bot(m.Config.from_env({**env, "SIM_AFTER_HOURS_MAX_PRICE_CENTS": "15"}), m.Store(":memory:"), FM(NOW), None)
notes: dict = {}
cand, why = bot.sim_maker_reason(card(False), bot.sim_trades(), NOW, notes=notes)
assert cand is None and why.startswith("盘后只挂 15.0¢ 以下的"), (cand, why)
assert notes["sides"]["up"]["why"] == "盘后只挂 15.0¢ 以下的，托底单价 20.0¢ 高于它", notes["sides"]
cand, why = bot.sim_maker_reason(card(True), bot.sim_trades(), NOW)  # in session: no after-hours limit, the floor order as before
assert cand is not None and cand.floor and cand.price == 0.20 and why == "", (cand, why)
loose = m.Bot(m.Config.from_env({**env, "SIM_AFTER_HOURS_MAX_PRICE_CENTS": "25"}), m.Store(":memory:"), FM(NOW), None)
cand, why = loose.sim_maker_reason(card(False), loose.sim_trades(), NOW)  # under the limit: placed after hours too
assert cand is not None and cand.floor and cand.price == 0.20, (cand, why)

# --- R13: the exchange's quote leads a 15-minute window only when its trade, not just its read, is recent ---------------------------
BJ = lambda h, mi, s=0: int(dt.datetime(2026, 11, 4, h, mi, s, tzinfo=m.BEIJING).timestamp() * 1000)
T0 = BJ(11, 0)
spec = m.WINDOW_UPDOWN_MARKETS[0].current(BJ(11, 5, 30))  # UNITREE-15M
assert m.WINDOW_UPDOWN_MARKETS[0].key == "UNITREE-15M"
quote = lambda last, quoted, fetched: m.IndexQuote("宇树科技", D(last), D("149.00"), None, None, None, quoted, "腾讯", "交易中", fetched)


def window():
    store = m.Store(":memory:")
    w = m.IndexUpDownMarket(store, spec, None, "sh", m.session_minute_fn("sh"))
    store.put(f"updown:{spec.slug}:start", {"open": spec.start_ms, "close": "90.0000", "read_ms": T0 + 1000})
    w.lead_mode, w.price, w.index_ms = "on", D("90.4000"), T0 + 299_000
    return w


at = spec.start_ms + 60_000
stale = window()  # a cached snapshot: its trade four minutes before the start, read just after it
stale.note_lead(quote("150.00", at - 240_000, at + 2_000), at + 2_000)
assert stale.lead_start() is None, "a start trade that far ahead of the start: not taken (the window priced on the index)"
stale.note_lead(quote("150.00", at - 240_000, at + 25_000), at + 25_000)
assert stale.lead_start() is None and stale.store.get(f"updown:{spec.slug}:lead") == {"missed": True, "read_ms": at + 25_000}
quiet = window()  # a stock that last traded a minute before the start: that trade is the price in force
quiet.note_lead(quote("150.00", at - 60_000, at + 2_000), at + 2_000)
assert quiet.lead_start() == (D("150.00"), at - 60_000), quiet.lead_start()
now = at + 30_000
quiet.note_lead(quote("150.60", now - 2_000, now - 500), now)
price, gap, why = quiet.lead_price(now)
assert price == D("90.36") and why == "", (price, gap, why)
quiet.note_lead(quote("150.60", now - 70_000, now - 500), now)  # read half a second ago, traded 70 s ago
assert quiet.lead_price(now) == (None, 0.0, "交易所最后一笔成交在 70 秒前，按指数"), quiet.lead_price(now)
assert quiet.effective(now)[0] == D("90.4000"), "the index, what settles the window"

# --- R19: a late pushed index frame is taken, but only a current one puts the REST read off -------------------------------------------
reads = []


async def futures(path, **params):
    reads.append(path)
    return {"indexPrice": "90.5000", "time": clock["now"]}
clock = {"now": T0 + 300_000}
w = m.IndexUpDownMarket(m.Store(":memory:"), spec, futures, "sh", m.session_minute_fn("sh"))
w.times["vol"] = time.monotonic() + 1e9  # (σ not due: the price read alone)


async def r19():
    # a frame 30 s late, its REST read due: the frame is taken (newer than what is held), the read is not put off
    w.times["price"] = time.monotonic() - 5
    assert w.push_index(D("90.10"), clock["now"] - 30_000, clock["now"]) is True and w.price == D("90.10")
    await w.read_quote(clock["now"], time.monotonic())
    assert reads.count("premiumIndex") == 1 and w.price == D("90.5000") and w.index_ms == clock["now"], (reads, w.price)
    # a current frame puts it off, as before
    clock["now"] += 1000
    w.times["price"] = time.monotonic() - 5
    assert w.push_index(D("90.60"), clock["now"] - 300, clock["now"]) is True
    await w.read_quote(clock["now"], time.monotonic())
    assert reads.count("premiumIndex") == 1 and w.price == D("90.60"), (reads, w.price)
asyncio.run(r19())
assert m.IndexUpDownMarket.PUSH_CURRENT_MS == 2_000

# --- R21: /samples reads the samples once, and not while the rule taker goes first ---------------------------------------------------
sbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), m.Store(":memory:"), FM(NOW), None)
calls = []
rows = [{"t": NOW - 1000, "card": "BTC-15M", "kind": "window", "slug": "btc-1", "start": NOW - 60_000, "end": NOW + 840_000, "left": 840,
         "up": 0.55, "mkt": 0.5, "result": None}]
sbot.window_rows = lambda: calls.append(1) or list(rows)
doc = asyncio.run(sbot.cmd_samples(None))
assert isinstance(doc, m.Document) and len(calls) == 1 and doc.caption.startswith("模型样本 1 条（已有结果 0 条），1 个市场"), (calls, doc)
assert "btc-1" in doc.data.decode("utf-8") and doc.data.decode("utf-8") == sbot.window_csv(rows)
sbot.rules_clear = lambda seconds: False
calls.clear()
got = asyncio.run(sbot.cmd_samples(None))
assert isinstance(got, str) and "样本导出这段时间不做" in got and calls == [], got

# --- R22: a body trickling in is cut at the total deadline (2 × timeout + 1), not when it ends -------------------------------------
def serve(sock, chunked):
    conn, _ = sock.accept()
    conn.recv(4096)
    head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" if chunked else b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\n\r\n"
    with contextlib.suppress(OSError):
        conn.sendall(head)
        for _ in range(16):
            time.sleep(0.5)
            conn.sendall(b"1\r\nx\r\n" if chunked else b"x")
        if chunked:
            conn.sendall(b"0\r\n\r\n")
    conn.close()


for chunked in (False, True):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    threading.Thread(target=serve, args=(sock, chunked), daemon=True).start()
    c = http.client.HTTPConnection("127.0.0.1", sock.getsockname()[1], timeout=1)
    c.request("GET", "/")
    began = time.monotonic()
    try:
        m._read_body(c.getresponse(), 1)
        raise AssertionError("a trickle must not be read whole past the deadline")
    except m.RemoteError as error:
        took = time.monotonic() - began
        assert "3 秒内没有读完" in str(error) and took < 5.0, (chunked, took, error)
    c.close()
    sock.close()


class Whole:  # a body that comes at once: read whole, as before (read1 when the response has it, else read)
    def __init__(self, data): self.data = data
    def read(self, n): out, self.data = self.data[:n], self.data[n:]; return out
assert m._read_body(Whole(b"y" * 300_000), 1) == b"y" * 300_000

# --- R23: `--calib` leaves the database as it found it -----------------------------------------------------------------------------------
folder = tempfile.mkdtemp()
db = os.path.join(folder, "calib.sqlite3")
con = sqlite3.connect(db)
con.execute("CREATE TABLE records (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
con.execute("INSERT INTO records VALUES (?, ?)", ("control:env", json.dumps({"SIM_EDGE_CENTS": "12", "SIM_MAKER_FLOOR_CENTS": "80"})))
con.commit()
con.close()
before = dict(sqlite3.connect(db).execute("SELECT k, v FROM records"))
saved = {k: os.environ.get(k) for k in ("STATE_DB", "PORT", "TELEGRAM_BOT_TOKEN", "WEB_TOKEN")}
argv = sys.argv
try:
    os.environ.update({"STATE_DB": db, "PORT": "8080", "TELEGRAM_BOT_TOKEN": ""})
    os.environ.pop("WEB_TOKEN", None)
    sys.argv = ["main.py", "--calib"]
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        assert m.main() == 0
    assert out.getvalue().strip(), "the report printed"
finally:
    sys.argv = argv
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
after = dict(sqlite3.connect(db).execute("SELECT k, v FROM records"))
assert after == before, (before, after)  # (an invalid saved setting not pruned, no web token saved)
mem = m.Store(db, persist=False)
mem.put("x", 1)
assert mem.get("x") == 1 and mem.writer is None and mem.flush() and dict(sqlite3.connect(db).execute("SELECT k, v FROM records")) == before
mem.close()
print("AUDIT_REVIEW_OK")
