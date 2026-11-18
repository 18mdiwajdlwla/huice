FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STATE_DB=/data/bot.sqlite3

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
# 1.154.22: the native secp256k1 library for the orders' signatures — eth-keys uses it when it is there: 0.45 ms an order where its
# own pure-Python curve takes 3.4 ms (the same signature, byte for byte), every one of them on the bot's one event loop. Only a
# ready-made wheel, and the build goes on without it when none fits this machine: the bot then signs as before.
RUN pip install --no-cache-dir --only-binary=:all: coincurve==21.0.0 || echo "coincurve: no wheel for this platform, signing in pure Python"
COPY main.py live.py preopen.py rules.py ./
COPY tests ./tests

# Offline suites (no network, no token, no key): a failing test fails the build instead of shipping.
# The headless-browser check in tests/test_web.py is skipped here because Chromium is not installed.
RUN python -m py_compile main.py live.py preopen.py rules.py && python tests/run_all.py && rm -rf tests

CMD ["python", "-u", "main.py"]

