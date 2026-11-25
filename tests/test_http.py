"""Kept-alive HTTP (1.62.0): one connection per host reused across requests; a pooled connection the server had closed
is retried once on a fresh one for a GET, for a POST only before anything was sent; HTTP errors mapped as before
(status, the service's words, Retry-After, hints); redirects followed; gzip inflated; the pool bounded; a proxy or
plain http takes the urllib way. All with a fake connection class: nothing touches the network."""
import gzip, json, sys, time, urllib.request
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m


class Answer:
    def __init__(self, status=200, body=b"", headers=None, will_close=False):
        self.status, self.body, self.headers, self.will_close, self.pos = status, body, dict(headers or {}), will_close, 0

    def read(self, n=-1):
        chunk = self.body[self.pos:self.pos + n] if n is not None and n >= 0 else self.body[self.pos:]
        self.pos += len(chunk)
        return chunk

    def getheader(self, name, default=None):
        return self.headers.get(name, default)


SCRIPT = {}  # host -> the answers in order: an Answer, "send-fail" (the request could not be written), "recv-fail" (no answer came)
LOG = []     # (connection id, method, path, body)
SENT = []    # (host, the headers of the request) — which host was handed what (1.154.26)


class FakeConn:
    made = 0

    def __init__(self, host, port, timeout=None, context=None):
        FakeConn.made += 1
        self.id, self.host, self.port, self.timeout, self.sock, self.closed, self.pending = FakeConn.made, host, port, timeout, None, False, None

    def request(self, method, path, body=None, headers=None):
        assert not self.closed and headers["Accept-Encoding"] == "gzip" and headers["User-Agent"].startswith("CloseAlert/")
        LOG.append((self.id, method, path, body))
        SENT.append((self.host, dict(headers)))
        step = SCRIPT[self.host].pop(0)
        if step == "send-fail":
            raise ConnectionResetError("Connection reset by peer")
        self.sock = self.sock or _Sock()
        self.pending = step

    def getresponse(self):
        if self.pending == "recv-fail":
            raise m.http.client.RemoteDisconnected("Remote end closed connection without response")
        return self.pending

    def close(self):
        self.closed, self.sock = True, None


class _Sock:
    def settimeout(self, t): self.timeout = t


def run():
    m.HTTPS_CONNECTION = FakeConn
    real_proxies = urllib.request.getproxies
    urllib.request.getproxies = lambda: {}  # the kept-alive path, whatever this machine's proxy settings
    m._POOL.clear(); m.HTTP_STATS.update(new=0, reused=0, retried=0)
    try:
        host = "api.example.test"
        ok = lambda body=b'{"ok": true}', **kw: Answer(200, body, **kw)
        # 1. two GETs, one connection
        SCRIPT[host] = [ok(b"a"), ok(b"b")]
        assert m._http_get(f"https://{host}/x?q=1") == b"a" and m._http_get(f"https://{host}/y") == b"b"
        assert [(c, meth, p) for c, meth, p, _ in LOG] == [(1, "GET", "/x?q=1"), (1, "GET", "/y")] and m.HTTP_STATS == {"new": 1, "reused": 1, "retried": 0}
        # 2. the pooled connection was closed by the server: the request could not be written -> once more, on a fresh one
        LOG.clear(); SCRIPT[host] = ["send-fail", ok(b"c")]
        assert m._http_get(f"https://{host}/z") == b"c" and [c for c, *_ in LOG] == [1, 2] and m.HTTP_STATS["retried"] == 1
        # 3. ...or no answer came to a GET: again on a fresh one
        LOG.clear(); SCRIPT[host] = ["recv-fail", ok(b"d")]
        assert m._http_get(f"https://{host}/z") == b"d" and [c for c, *_ in LOG] == [2, 3] and m.HTTP_STATS["retried"] == 2
        # 4. a POST on a fresh pooled connection with no answer: never sent twice (the caller treats it as "result unknown")
        LOG.clear(); SCRIPT[host] = ["recv-fail"]
        try:
            m._http_get(f"https://{host}/orders", {"a": 1}); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error).startswith("网络错误 (RemoteDisconnected") and len(LOG) == 1 and LOG[0][1] == "POST" and json.loads(LOG[0][3]) == {"a": 1}, error
        # 5. a POST reuses only a connection used within KEEPALIVE_POST_IDLE_SECONDS; a GET within KEEPALIVE_IDLE_SECONDS
        LOG.clear(); SCRIPT[host] = [ok(b"e"), ok(b"f"), ok(b"g")]
        assert m._http_get(f"https://{host}/a") == b"e"; fresh = LOG[-1][0]
        m._POOL[(host, 443)][-1] = (m._POOL[(host, 443)][-1][0], time.monotonic() - 10)  # idle for 10 s
        assert m._http_get(f"https://{host}/orders", {"b": 2}) == b"f" and LOG[-1][0] != fresh, "a stale-ish connection is not trusted with a body"
        posted = LOG[-1][0]
        m._POOL[(host, 443)][-1] = (m._POOL[(host, 443)][-1][0], time.monotonic() - 10)
        # 1.154.40: on a host that is sent requests with a body, a read takes the pooled connection unused the longest (within
        # KEEPALIVE_IDLE_SECONDS) and leaves the most recent one for the next request with a body (tests/test_audit_feeds.py)
        assert m._http_get(f"https://{host}/a") == b"g" and LOG[-1][0] == fresh != posted, "a GET reuses one: here the one unused longest"
        assert [c.id for c, _ in m._POOL[(host, 443)]] == [posted, fresh]
        m._POOL[(host, 443)][:] = [(c, time.monotonic() - 60) for c, _ in m._POOL[(host, 443)]]
        SCRIPT[host] = [ok(b"h")]
        assert m._http_get(f"https://{host}/a") == b"h" and LOG[-1][0] not in (fresh, posted), "a connection idle a minute is dropped"
        # 6. HTTP errors: the status, the service's words, Retry-After (and Binance's 418/429 floor), Predict's 403 hint
        SCRIPT[host] = [Answer(429, b'{"message": "slow down"}', {"Retry-After": "7"})]
        try:
            m._http_get(f"https://{host}/a"); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error) == "HTTP 429: 接口限流，等待后重试" and error.retry_after == 7, (str(error), error.retry_after)  # the hint for a generic host, the service's words for Predict
        SCRIPT["api.predict.fun"] = [Answer(403, b"{}")]
        try:
            m._http_get("https://api.predict.fun/v1/orders", {"x": 1}); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error).startswith("HTTP 403: ") and "受限地区" in str(error), error
        SCRIPT["fapi.binance.com"] = [Answer(418, b"")]
        try:
            m._http_get("https://fapi.binance.com/fapi/v1/time"); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error).startswith("HTTP 418") and error.retry_after == 120, (str(error), error.retry_after)
        assert not m._POOL.get(("api.predict.fun", 443)) and not m._POOL.get(("fapi.binance.com", 443)), "an error answer's connection is not pooled"
        # 7. a redirect is followed (a GET), the body of the final answer returned; gzip is inflated; Connection: close is honoured
        LOG.clear(); SCRIPT[host] = [Answer(302, b"", {"Location": f"https://{host}/moved"}), ok(gzip.compress(b"zipped"), headers={"Content-Encoding": "gzip"}, will_close=True), ok(b"i")]
        assert m._http_get(f"https://{host}/old") == b"zipped" and [p for _, _, p, _ in LOG] == ["/old", "/moved"]
        assert m._http_get(f"https://{host}/next") == b"i" and LOG[-1][0] != LOG[-2][0], "the closed connection was not reused"
        # 7b. 1.154.26 (the audit of 11-18, 12): a redirect to another host is followed without the credentials — the API key and
        # the sign-in's token went wherever a Location led; one within the host keeps them; one down to plain http loses them
        # too (the urllib way: offline here, the guard answers — and the request it was handed carried none); a redirect that
        # is not followed (a POST's, one with no Location) is an error, not an answer
        secret = {"x-api-key": "KEY", "Authorization": "Bearer JWT"}
        had = lambda headers: sorted(k for k in headers if k.lower() in m.SECRET_HEADERS)
        del SENT[:]
        SCRIPT[host] = [Answer(302, b"", {"Location": f"https://{host}/v2/x"}), Answer(302, b"", {"Location": "https://elsewhere.example.test/x"})]
        SCRIPT["elsewhere.example.test"] = [ok(b"there")]
        assert m._http_get(f"https://{host}/v1/x", headers=secret) == b"there"
        assert [(h, had(headers)) for h, headers in SENT] == [(host, ["Authorization", "x-api-key"]), (host, ["Authorization", "x-api-key"]),
                                                              ("elsewhere.example.test", [])], SENT
        assert SENT[-1][1]["User-Agent"].startswith("CloseAlert/"), "the other headers go on as they were"
        del SENT[:]
        SCRIPT[host] = [Answer(302, b"", {"Location": f"https://{host}:8443/x"}), ok(b"other port")]  # another port is another origin (the fake's script is by host)
        assert m._http_get(f"https://{host}/v1/p", headers=secret) == b"other port"
        assert [had(headers) for _, headers in SENT] == [["Authorization", "x-api-key"], []], SENT
        del SENT[:]
        SCRIPT[host] = [Answer(302, b"", {"Location": "/v1/q?x=1"}), ok(b"same place")]  # a relative one stays where it is: kept
        assert m._http_get(f"https://{host}/v1/p", headers=secret) == b"same place"
        assert [had(headers) for _, headers in SENT] == [["Authorization", "x-api-key"], ["Authorization", "x-api-key"]], SENT
        handed = []
        real_urlopen = urllib.request.urlopen

        def urlopen(req, *a, **k):
            handed.append((req.full_url, dict(req.header_items())))
            raise OSError("offline")
        urllib.request.urlopen = urlopen
        try:
            SCRIPT[host] = [Answer(301, b"", {"Location": "http://plain.example.test/x"})]
            try:
                m._http_get(f"https://{host}/v1/y", headers=secret); raise AssertionError("no error")
            except m.RemoteError as error:
                assert str(error) == "网络错误 (OSError)", error
            assert handed[-1][0] == "http://plain.example.test/x" and had(handed[-1][1]) == [], handed
            SCRIPT[host] = [Answer(301, b"", {"Location": f"http://{host}/v1/y"})]  # the same host, down to plain http: none either
            try:
                m._http_get(f"https://{host}/v1/y", headers=secret); raise AssertionError("no error")
            except m.RemoteError as error:
                assert str(error) == "网络错误 (OSError)", error
            assert handed[-1][0] == f"http://{host}/v1/y" and had(handed[-1][1]) == [], handed
            # ...and the urllib way itself (a proxy): the credentials go with the request, marked as not to be carried over a redirect
            urllib.request.getproxies = lambda: {"https": "http://proxy:3128"}
            seen = []

            def opened(req, *a, **k):
                seen.append((sorted(k for k in req.unredirected_hdrs), sorted(k for k in req.headers if k.lower() in m.SECRET_HEADERS)))
                raise OSError("offline")
            urllib.request.urlopen = opened
            try:
                m._http_get(f"https://{host}/v1/z", headers=secret); raise AssertionError("no error")
            except m.RemoteError:
                pass
            assert seen == [(["Authorization", "X-api-key"], [])], seen
        finally:
            urllib.request.urlopen = real_urlopen
            urllib.request.getproxies = lambda: {}
        for answer, payload, word in ((Answer(302, b"<html>", {"Location": f"https://{host}/login"}), {"a": 1}, "POST 不会重发"), (Answer(303, b""), None, "没有给出去向")):
            SCRIPT[host] = [answer]
            try:
                m._http_get(f"https://{host}/v1/orders", payload); raise AssertionError("no error")
            except m.RemoteError as error:
                assert str(error) == f"HTTP {answer.status}: 重定向没有跟随（{word}）", error
        # 8. the pool is bounded per host
        for i in range(m.KEEPALIVE_PER_HOST + 3):
            m._pool_put(host, 443, FakeConn(host, 443))
        assert len(m._POOL[(host, 443)]) == m.KEEPALIVE_PER_HOST
        # 9. a proxy, or plain http: the urllib way (offline here: the guard answers)
        urllib.request.getproxies = lambda: {"https": "http://proxy:3128"}
        n = FakeConn.made
        try:
            m._http_get(f"https://{host}/a"); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error) == "网络错误 (OSError)" and FakeConn.made == n, error  # urllib's guard, as before; no pooled connection touched
        urllib.request.getproxies = lambda: {}
        try:
            m._http_get("http://plain.example.test/a"); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error) == "网络错误 (OSError)" and FakeConn.made == n, error
        # 10. the real connection class is guarded by offline.py too: connecting raises, mapped to 网络错误
        m.HTTPS_CONNECTION = m.http.client.HTTPSConnection
        try:
            m._http_get("https://nowhere.example.test/a"); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error).startswith("网络错误 (OSError: network disabled in tests"), error
    finally:
        m.HTTPS_CONNECTION = m.http.client.HTTPSConnection
        urllib.request.getproxies = real_proxies
        m._POOL.clear()
    print("HTTP_OK")


run()
