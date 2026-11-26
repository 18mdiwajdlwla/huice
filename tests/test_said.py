"""1.154.61 — what Predict's answers say of each of its buckets, by the kind of request, beside the bot's own counts. 11-26
15:11 the pre-open key's answer said "ratelimit-limit 2000, remaining 1832, reset 42" — the last answer's alone, a read's or
an order's could not be told, nor whether the bot's other keys count in it.

Here: each request to Predict counted as it leaves (by key, as an order or a read); an answer's figures read — the classic
headers and the IETF draft's — and kept by key and group with the counts beside them; whose requests the answer's count is;
what /preopen and /diag say, and the figures they suggest for PREOPEN_APP_*; through the real _http_get on a fake connection.
Entirely offline.

Run: python tests/test_said.py
"""
import sys
import threading
import time
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import offline  # noqa: F401
import main as m

HOST = "api.predict.fun"
DRAFT = {"ratelimit-remaining": "1832", "ratelimit-limit": "2000", "ratelimit-reset": "42",
         "ratelimit-policy": '"rpm";q=2000;w=60', "ratelimit": '"rpm";r=1832;t=42'}  # the answer of 11-26 15:11:18, as it came


def answer(headers):
    return types.SimpleNamespace(headers=dict(headers))


def sent(key, method, path, times=1):
    for _ in range(times):
        m.note_predict_sent(HOST, {"x-api-key": key}, method, path)


class Said(unittest.TestCase):
    def setUp(self):
        self.kept = (dict(m.PREDICT_SENT), dict(m.PREDICT_BUCKETS_SAID), dict(m.PREDICT_LIMITS_SAID))
        m.PREDICT_SENT.clear(); m.PREDICT_BUCKETS_SAID.clear(); m.PREDICT_LIMITS_SAID.clear(); m.KEYS_APART.clear()

    def tearDown(self):
        for table, was in zip((m.PREDICT_SENT, m.PREDICT_BUCKETS_SAID, m.PREDICT_LIMITS_SAID), self.kept):
            table.clear(); table.update(was)

    def test_the_kind_of_a_request(self):
        for method, path, group in (("POST", "/v1/orders", "下单撤单"), ("POST", "/v1/orders/remove", "下单撤单"), ("POST", "/orders/", "下单撤单"),
                                    ("GET", "/v1/orders/0xab", "订单"), ("GET", "/v1/orders?status=OPEN", "订单"), ("GET", "/v1/markets/12", "市场"),
                                    ("GET", "/v1/markets/12/orderbook", "盘口"), ("GET", "/v1/categories/btc-updown-5m-1", "分类"),
                                    ("POST", "/v1/auth", "登录"), ("GET", "/v1/auth/message", "登录"), ("GET", "/v1/positions", "持仓"),
                                    ("GET", "/v2/markets", "市场"), ("", "", "其他"), ("GET", "/graphql", "其他"), ("post", "/v1/orders", "下单撤单")):
            self.assertEqual(m.predict_group(method, path), group, (method, path))

    def test_the_requests_counted_as_they_leave(self):
        count = m.SendCount()
        now = time.time()
        for at in (now - 200, now - 100, now - 61, now - 30, now - 30, now - 1, now):
            count.add(at)
        self.assertEqual((count.since(now - 60), count.since(now - 30), count.since(now), count.since(now + 5)), (4, 4, 1, 0))
        self.assertEqual(count.seconds[0][0], int(now - 100), "three minutes kept, no more")
        # by key and kind; another host is not counted
        sent("pre", "GET", "/v1/markets/1", 5)
        sent("pre", "POST", "/v1/orders", 2)
        sent("pre", "POST", "/v1/orders/remove")
        sent("main", "GET", "/v1/markets/1/orderbook", 4)
        m.note_predict_sent("api.binance.com", {"x-api-key": "pre"}, "GET", "/v1/markets/1")
        self.assertEqual(set(m.PREDICT_SENT), {("pre", "read"), ("pre", "trade"), ("main", "read")})
        since = time.time() - 60
        self.assertEqual((m.predict_sent_since(since, "read", "pre"), m.predict_sent_since(since, "trade", "pre"), m.predict_sent_since(since, "read")), (5, 3, 9))
        # never raises (it stands in every request's way)
        m.note_predict_sent(HOST, None, "GET", "/x")
        # the HTTP threads add while the status reads
        many = m.SendCount()
        threads = [threading.Thread(target=lambda: [many.add(time.time()) for _ in range(2000)]) for _ in range(4)]
        for t in threads:
            t.start()
        while any(t.is_alive() for t in threads):
            many.since(time.time() - 60)
        self.assertLessEqual(many.since(time.time() - 60), 8000)

    def test_an_answers_figures_are_read_and_kept_by_kind(self):
        sent("pre", "GET", "/v1/markets/1", 168)
        sent("pre", "POST", "/v1/orders", 42)
        sent("main", "GET", "/v1/markets/1", 300)
        m.note_limit_headers(HOST, {"x-api-key": "pre"}, 200, answer({**DRAFT, "Content-Type": "application/json"}), "GET", "/v1/markets/1")
        sample = m.PREDICT_BUCKETS_SAID["pre"]["市场"]
        self.assertEqual({k: sample[k] for k in ("limit", "remaining", "reset", "window", "kind", "mine", "all", "mine_any", "status")},
                         {"limit": 2000, "remaining": 1832, "reset": 42, "window": 60.0, "kind": "read", "mine": 168, "all": 468, "mine_any": 210, "status": 200})
        self.assertLessEqual(sample["mine_clock"], 168)
        # the raw capture as before (1.154.57), its quotes shown now
        self.assertIn('ratelimit-policy "rpm";q=2000;w=60', m.predict_limits_said("pre"))
        # the draft's fields alone; the "x-" names; a reset given as an epoch second
        m.note_limit_headers(HOST, {"x-api-key": "pre"}, 200, answer({"RateLimit-Policy": '"rpm";q=2000;w=60', "RateLimit": '"rpm";r=1958;t=40'}), "POST", "/v1/orders")
        self.assertEqual({k: m.PREDICT_BUCKETS_SAID["pre"]["下单撤单"][k] for k in ("limit", "remaining", "reset", "kind", "mine", "all")},
                         {"limit": 2000, "remaining": 1958, "reset": 40, "kind": "trade", "mine": 42, "all": 42})
        m.note_limit_headers(HOST, {"x-api-key": "k"}, 200, answer({"X-RateLimit-Limit": "240", "X-RateLimit-Remaining": "12",
                                                                    "X-RateLimit-Reset": str(int(time.time()) + 30)}), "GET", "/v1/categories/x")
        self.assertIn(m.PREDICT_BUCKETS_SAID["k"]["分类"]["reset"], (29, 30))
        # without a count to read (no limit, or nothing left said): nothing kept by kind — the raw capture still has it
        m.note_limit_headers(HOST, {"x-api-key": "q"}, 200, answer({"RateLimit-Policy": "240;w=60"}), "GET", "/v1/markets/1")
        self.assertNotIn("q", m.PREDICT_BUCKETS_SAID)
        self.assertIn("ratelimit-policy 240;w=60", m.predict_limits_said("q"))
        # a group's figures at most once a second — but a 429's always
        m.note_limit_headers(HOST, {"x-api-key": "pre"}, 200, answer({**DRAFT, "ratelimit-remaining": "1700"}), "GET", "/v1/markets/2")
        self.assertEqual(m.PREDICT_BUCKETS_SAID["pre"]["市场"]["remaining"], 1832)
        m.note_limit_headers(HOST, {"x-api-key": "pre"}, 429, answer({**DRAFT, "ratelimit-remaining": "0", "Retry-After": "20"}), "GET", "/v1/markets/2")
        self.assertEqual((m.PREDICT_BUCKETS_SAID["pre"]["市场"]["remaining"], m.PREDICT_BUCKETS_SAID["pre"]["市场"]["status"]), (0, 429))
        # whatever the answer, never raises
        m.note_limit_headers(HOST, {"x-api-key": "pre"}, 200, answer({"ratelimit-limit": "lots", "ratelimit-remaining": "-"}), "GET", "/v1/markets/1")
        m.note_limit_headers(HOST, {"x-api-key": "pre"}, 200, object(), "GET", "/v1/markets/1")

    def test_whose_requests_the_count_is(self):
        base = {"limit": 2000, "window": 60.0}
        whose = lambda remaining, mine, every, mine_clock=None, all_clock=None: m.predict_bucket_whose(
            {**base, "remaining": remaining, "mine": mine, "all": every, "mine_clock": mine if mine_clock is None else mine_clock,
             "all_clock": every if all_clock is None else all_clock})
        self.assertEqual(whose(1832, 168, 468), "只算这把 Key 自己的", "11-26 15:11: 168 counted, the key's own reads 168, every key's 468")
        self.assertEqual(whose(1532, 168, 468), "机器人几把 Key 合在一起算（同一个 App）")
        self.assertEqual(whose(1832, 210, 470, mine_clock=60, all_clock=165), "机器人几把 Key 合在一起算（同一个 App）", "by the clock's minute: all of them in 18 s")
        self.assertEqual(whose(1000, 168, 468), "比机器人所有 Key 同期发的还多——还有别的程序在用同一个 App 或 Key")
        self.assertEqual(whose(1958, 42, 45), "和这把 Key 同期发的对得上", "the other keys sent next to no orders: not to be told apart")
        self.assertEqual(whose(1958, 42, 45, mine_clock=5, all_clock=5), "和这把 Key 同期发的对得上")
        self.assertEqual(whose(1700, 42, 45), "比机器人所有 Key 同期发的还多——还有别的程序在用同一个 App 或 Key")
        self.assertEqual(whose(1650, 168, 468), "", "between the two: nothing said")
        # 1.154.66: a limiter by sliding window counter — this clock window's count and the last one's in the share still under the
        # minute: 11-26 19:10:33, 360 counted where the key had sent 215 since 19:10:00 and some 320 in the minute before
        slid = {"limit": 2000, "window": 60.0, "remaining": 1640, "elapsed": 33.0, "mine": 520, "mine_clock": 215, "mine_prev": 320,
                "all": 900, "all_clock": 500, "all_prev": 500}
        self.assertEqual(m.predict_bucket_whose(slid), "只算这把 Key 自己的")

    def test_what_the_status_says_and_suggests(self):
        self.assertEqual((m.predict_buckets_line("pre"), m.predict_buckets_short("pre")), ("", ""))
        sent("pre", "GET", "/v1/markets/1", 168)
        sent("pre", "POST", "/v1/orders", 42)
        sent("main", "GET", "/v1/markets/1", 300)
        m.note_limit_headers(HOST, {"x-api-key": "pre"}, 200, answer(DRAFT), "GET", "/v1/markets/1")
        line = m.predict_buckets_line("pre", (0, 0))
        self.assertIn("Predict 应答里报的限额：General（市场）每分钟 2000 次——", line)
        self.assertIn("那次（市场）已算 168 次、剩 1832、42 秒后重置；同期这把 Key 发了 168 次读取、机器人所有 Key 468 次→只算这把 Key 自己的", line)
        self.assertTrue(line.endswith("。→ “Predict 限额”的 General 可以填 2000；Trading 等下单的应答带回来再看（下一次挂单后）"), line)
        m.note_limit_headers(HOST, {"x-api-key": "pre"}, 200, answer({"ratelimit-limit": "2000", "ratelimit-remaining": "1958", "ratelimit-reset": "40"}), "POST", "/v1/orders")
        line = m.predict_buckets_line("pre", (0, 0))
        self.assertIn("；Trading（下单撤单）每分钟 2000 次——", line)
        self.assertIn("同期这把 Key 发了 42 次下单、撤单和读订单", line)
        self.assertTrue(line.endswith("。→ 控制台“开盘前预挂”里的“Predict 限额”两项可以照这个填：Trading 2000、General 2000"), line)
        # the figures set: compared, not suggested
        # (1.154.67: one given above what the key said is not what the mode goes by — preopen_app_capped)
        self.assertTrue(m.predict_buckets_line("pre", (5000, 1000)).endswith(
            "。→ 填的 Trading 5000 比 Predict 报的 2000 大——按 Predict 报的 2000 跑（两项都清成 0 就全照 Predict 报的自动跑）；填的 General 1000 比 Predict 报的 2000 小，可以改大"))
        self.assertNotIn("。→ ", m.predict_buckets_line("pre", (2000, 2000)), "the figures set are Predict's: nothing to say")
        # one of the two set (1.154.62): the other said missing, with Predict's figure
        self.assertTrue(m.predict_buckets_line("pre", (2000, 0)).endswith("。→ 还没填 General（两项都填了才生效），Predict 报的是 2000"), m.predict_buckets_line("pre", (2000, 0)))
        self.assertNotIn("。→ ", m.predict_buckets_line("pre"), "(another key's line: nothing suggested)")
        # reads of several groups at different limits: each said
        m.note_limit_headers(HOST, {"x-api-key": "pre"}, 200, answer({"ratelimit-limit": "120", "ratelimit-remaining": "100"}), "GET", "/v1/categories/x")
        line = m.predict_buckets_line("pre", (0, 0))
        self.assertIn("General：市场 每分钟 2000 次、分类 每分钟 120 次——", line)
        self.assertTrue(line.endswith("可以照这个填：Trading 2000、General 120"), line)
        # a figure said under 240 is no bucket the mode's paces are made for: one given above it is not capped — said as refused
        self.assertTrue(m.predict_buckets_line("pre", (2000, 1000)).endswith("。→ 填的 General 1000 比 Predict 报的 120 大——会被拒"), m.predict_buckets_line("pre", (2000, 1000)))
        # keys of other apps (their answers said other limits): never "together", whatever the counts
        m.PREDICT_SENT.clear(); m.KEYS_APART.clear()
        sent("pre", "POST", "/v1/orders", 42)
        sent("main", "POST", "/v1/orders", 400)
        m.note_limit_headers(HOST, {"x-api-key": "main"}, 200, answer({"ratelimit-limit": "5000", "ratelimit-remaining": "4000"}), "POST", "/v1/orders")
        m.note_limit_headers(HOST, {"x-api-key": "main"}, 200, answer({"ratelimit-limit": "5000", "ratelimit-remaining": "4900"}), "GET", "/v1/markets/1")
        m.PREDICT_BUCKETS_SAID["pre"]["下单撤单"].update(remaining=2000 - 440, at=time.time())
        self.assertIn("→只算这把 Key 自己的——别的 Key 报的限额和它不一样，不在同一个 App", m.predict_buckets_line("pre"))
        m.PREDICT_BUCKETS_SAID["pre"]["下单撤单"]["remaining"] = 1958
        # /diag's few words
        self.assertEqual(m.predict_buckets_short("pre"), "Predict 报：General 每分钟 120 次（剩 100）、Trading（含读订单） 每分钟 2000 次（剩 1958）")
        # figures older than half an hour are not today's word
        for sample in m.PREDICT_BUCKETS_SAID["pre"].values():
            sample["at"] -= m.BUCKET_FRESH_S + 1
        self.assertEqual((m.predict_buckets_line("pre", (0, 0)), m.predict_buckets_short("pre")), ("", ""))

    def test_through_the_real_http_path(self):
        """_http_get counts the request as it leaves and hands the answer's look its method and path (the kept-alive way)."""
        class Answer:
            def __init__(self, status, headers):
                self.status, self.headers, self.body, self.will_close = status, dict(headers), b'{"success": true}', False

            def read(self, n=-1):
                body, self.body = self.body, b""
                return body

            def getheader(self, name, default=None):
                return self.headers.get(name, default)

        class Conn:
            def __init__(self, host, port, timeout=None, context=None):
                self.sock = None

            def connect(self):
                pass

            def request(self, method, path, body=None, headers=None):
                self.answer = Answer(200, {"ratelimit-limit": "2000", "ratelimit-remaining": "1999" if method == "GET" else "1998",
                                           "ratelimit-reset": "30", "Content-Type": "application/json"})

            def getresponse(self):
                return self.answer

            def close(self):
                pass

        real, proxies = m.HTTPS_CONNECTION, m.urllib.request.getproxies
        m.HTTPS_CONNECTION, m.urllib.request.getproxies = Conn, (lambda: {})
        m._POOL.clear()
        try:
            m._http_get(f"https://{HOST}/v1/markets/7", None, 5, {"x-api-key": "pre"})
            m._http_get(f"https://{HOST}/v1/orders", {"data": {}}, 5, {"x-api-key": "pre"})
        finally:
            m.HTTPS_CONNECTION, m.urllib.request.getproxies = real, proxies
            m._POOL.clear()
        self.assertEqual({g: (s["kind"], s["remaining"], s["mine"]) for g, s in m.PREDICT_BUCKETS_SAID["pre"].items()},
                         {"市场": ("read", 1999, 1), "下单撤单": ("trade", 1998, 1)})


if __name__ == "__main__":
    result = unittest.main(exit=False).result
    if result.wasSuccessful():
        print("SAID_OK")
    else:
        sys.exit(1)
