"""Imported first by every suite: any real HTTP request fails at once, so results never depend on the network."""
import http.client
import os
import urllib.request

os.environ["PREDICT_PACING"] = "off"  # the request pacer (main.PredictPacer, 1.154.0) waits on the clock: the suites run unpaced
# 1.154.22: every answer the records' index gives (main.TradeIndex: a market's records, a driver's, the open ones, the orders')
# is checked against the walk of every record it stands for, in every suite, at every call; a suite in which one differed
# fails whatever its own checks said (below) — the code that asked may have caught the error and gone on
os.environ.setdefault("SIM_INDEX_AUDIT", "1")


def _index_audit() -> None:
    import sys
    fails = getattr(sys.modules.get("main"), "TRADE_AUDIT_FAILS", None)
    if fails:
        sys.stdout.flush()
        sys.stderr.write(f"AssertionError: the records' index differed from the walk {len(fails)} time(s): {fails[:3]}\n")
        sys.stderr.flush()
        os._exit(1)


import atexit  # noqa: E402
atexit.register(_index_audit)


def _no_network(request, *args, **kwargs):
    raise OSError(f"network disabled in tests: {getattr(request, 'full_url', request)}")


def _no_connect(self, *args, **kwargs):
    raise OSError(f"network disabled in tests: {self.host}")


urllib.request.urlopen = _no_network
http.client.HTTPSConnection.connect = _no_connect  # the kept-alive path (1.62.0) connects here


def _no_websocket(uri, *args, **kwargs):
    raise OSError(f"network disabled in tests: {uri}")


try:  # the streams (ws_connect, 1.86.0) import connect from here at call time: a bot run in a test connects nowhere
    import websockets.asyncio.client as _ws_client
except ImportError:
    _ws_client = None
if _ws_client is not None:
    _ws_client.connect = _no_websocket
