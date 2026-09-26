# -*- coding: utf-8 -*-
"""
A1 驗收測試（一）— 共用 REST 閘門：速率上限、429 封鎖冷卻、優先順序、非 ASCII 合約名、伺服器時鐘。

  AC-5  連續送出的請求，任何 1 秒窗口內不超過速率上限；收到一次 429 之後冷卻期間送出的請求數 = 0，
        冷卻結束才恢復；tests/test_market_static.py 仍全過
  AC-6  哈基米_USDT_PERP 的 klines：symbol 放在 params 裡，不是拼進 path；最終 URL 是 percent-encoded
  FR-5  前景請求優先於背景請求；foreground_hold() 期間背景請求一個都不放
  其他  live.pionex_api 的 ApiError.status_code 是向下相容的新增

全程離線：假時鐘（不真的 sleep）、假 api_get；整個 runner 包在 socket 籠子裡，任何連線企圖都會失敗並被計數。
不依賴 pytest：直接 `python tests/test_rest_gate.py`。
"""
import contextlib
import heapq
import itertools
import logging
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests  # noqa: E402

import live.pionex_api as lh  # noqa: E402
from live import config, klines, rest_gate  # noqa: E402
from live.rest_gate import (PRIORITY_BACKGROUND, PRIORITY_FOREGROUND, PRIORITY_NORMAL,  # noqa: E402
                            RestBanned, RestGate, RestLimiter, ServerClock)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
START_MS = 1_790_146_800_000


# ============================== 離線籠子 ==============================
class NetworkBlocked(RuntimeError):
    pass


_blocked_attempts = []


@contextlib.contextmanager
def socket_cage():
    """把 socket 連線全部擋下並記錄。離開時原樣還原。"""
    originals = (socket.socket.connect, socket.socket.connect_ex, socket.create_connection)

    def deny(*args, **kwargs):
        _blocked_attempts.append(args[1:] if len(args) > 1 else args)
        raise NetworkBlocked("測試企圖連網：%r" % (args[1:] if len(args) > 1 else args,))

    socket.socket.connect = deny
    socket.socket.connect_ex = deny
    socket.create_connection = deny
    try:
        yield
    finally:
        socket.socket.connect, socket.socket.connect_ex, socket.create_connection = originals


# ============================== 假時鐘 ==============================
class FakeClock:
    """monotonic 從 1000 秒起；sleep / wait 只推進時間。call_at 排程事件，推進時間跨過就同步執行。
    wait(cond, None)（等 notify）→ 推進到下一個事件；沒有事件代表死結，直接報錯。"""

    def __init__(self, start_ms=START_MS):
        self._mono = 1000.0
        self._offset_ms = start_ms - 1000.0 * 1000.0
        self._events = []
        self._seq = itertools.count()
        self.sleeps = []

    def monotonic(self):
        return self._mono

    def time_ms(self):
        return int(round(self._offset_ms + self._mono * 1000.0))

    def call_at(self, mono_t, fn):
        heapq.heappush(self._events, (mono_t, next(self._seq), fn))

    def advance_to(self, t):
        while self._events and self._events[0][0] <= t:
            et, _, fn = heapq.heappop(self._events)
            self._mono = max(self._mono, et)
            fn()
        self._mono = max(self._mono, t)

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.advance_to(self._mono + max(0.0, seconds))

    def wait(self, cond, timeout):
        if timeout is None:
            if not self._events:
                raise AssertionError("假時鐘死結：無限期等待，卻沒有任何排程事件會叫醒它")
            self.advance_to(self._events[0][0])
        else:
            self.advance_to(self._mono + timeout)


class FakeApi:
    """假的 api_get：記錄每次呼叫的 (假時鐘單調時刻, path, params)；responder 決定回什麼。"""

    def __init__(self, clock, responder=None):
        self.clock = clock
        self.calls = []
        self.kwargs = []
        self.responder = responder or (lambda path, params: {"result": True, "data": {}, "timestamp": clock.time_ms()})

    def __call__(self, path, params=None, retries=3, timeout=20):
        self.calls.append((self.clock.monotonic(), path, dict(params or {})))
        self.kwargs.append({"retries": retries, "timeout": timeout})
        return self.responder(path, params)


def make_gate(clock, api, rate=None, cooldown=None):
    rate = config.A1_REST_RATE_PER_SECOND if rate is None else rate
    cooldown = config.REST_BAN_COOLDOWN_SECONDS if cooldown is None else cooldown
    return RestGate(RestLimiter(rate, cooldown, clock=clock), ServerClock(clock), clock=clock, api_get=api)


def assert_window(times, rate, window=1.0):
    """任何 [a, a+window) 窗口內最多 rate 個 ⇔ 排序後第 i 個與第 i+rate 個至少相距 window。"""
    ts = sorted(times)
    for i in range(len(ts) - rate):
        assert ts[i + rate] - ts[i] >= window - 1e-9, \
            "第 %d 與第 %d 個請求只相距 %.6f 秒，1 秒窗口內超過 %d 個" % (i, i + rate, ts[i + rate] - ts[i], rate)


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def messages(self, level=None):
        return [r.getMessage() for r in self.records if level is None or r.levelno == level]


@contextlib.contextmanager
def capture_logs(name="live"):
    logger = logging.getLogger(name)
    h = _Capture()
    old = (logger.level, logger.propagate)
    logger.addHandler(h)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    try:
        yield h
    finally:
        logger.removeHandler(h)
        logger.setLevel(old[0])
        logger.propagate = old[1]


# ============================== AC-5：速率 ==============================
def test_ac5_sliding_window_never_exceeds_rate():
    clock = FakeClock()
    lim = RestLimiter(config.A1_REST_RATE_PER_SECOND, config.REST_BAN_COOLDOWN_SECONDS, clock=clock)
    times = []
    for _ in range(50):
        lim.acquire(PRIORITY_FOREGROUND)
        times.append(clock.monotonic())
    assert_window(times, config.A1_REST_RATE_PER_SECOND)
    rate = config.A1_REST_RATE_PER_SECOND
    # 不是只會慢：一開始可以連發 rate 個（收盤後候選並發靠這個），之後每秒 rate 個
    assert times[:rate] == [times[0]] * rate, times[:rate]
    assert abs((times[-1] - times[0]) - (49 // rate)) < 1e-9, times[-1] - times[0]


def test_ac5_window_holds_across_priorities_and_endpoints_through_gate():
    """tickers / klines / 補種子 / 對帳混著打，實際送到 api_get 的時刻仍守住 1 秒窗口。"""
    clock = FakeClock()
    api = FakeApi(clock)
    gate = make_gate(clock, api)
    pattern = [("/api/v1/market/tickers", PRIORITY_FOREGROUND), ("/api/v1/market/klines", PRIORITY_NORMAL),
               ("/api/v1/market/klines", PRIORITY_BACKGROUND), ("/api/v1/market/klines", PRIORITY_FOREGROUND)]
    for i in range(60):
        path, prio = pattern[i % len(pattern)]
        gate.get(path, {"symbol": "S%d" % i}, priority=prio)
        if i % 7 == 0:
            clock.sleep(0.3)
    assert len(api.calls) == 60
    assert_window([c[0] for c in api.calls], config.A1_REST_RATE_PER_SECOND)
    # 閘門一律以 retries=1 呼叫 api_get：重試若發生在 api_get 裡面，閘門算不到
    assert all(k["retries"] == 1 for k in api.kwargs), api.kwargs[:3]
    assert all(k["timeout"] == config.A1_HTTP_TIMEOUT_SECONDS for k in api.kwargs)


def test_ac5_window_detects_a_bursty_limiter():
    """鑑別力：assert_window 對「每秒補滿的 token bucket」這種會超量的節奏必須失敗。"""
    rate = config.A1_REST_RATE_PER_SECOND
    bucket_like = [0.0] * rate + [1.0 / rate]          # t=0 用掉 rate 個，1/rate 秒後又補 1 個
    try:
        assert_window(bucket_like, rate)
    except AssertionError:
        return
    raise AssertionError("assert_window 沒有抓到 1 秒內 rate+1 個請求")


# ============================== AC-5：429 ==============================
def _429_responder(clock, fail_on_call):
    state = {"n": 0}

    def responder(path, params):
        state["n"] += 1
        if state["n"] == fail_on_call:
            raise lh.ApiError(f"{path}: 重試 1 次仍失敗：{path}: HTTP 429 請求過於頻繁", status_code=429)
        return {"result": True, "data": {}, "timestamp": clock.time_ms()}
    return responder


def test_ac5_zero_requests_during_cooldown_then_resume():
    clock = FakeClock()
    api = FakeApi(clock)
    api.responder = _429_responder(clock, fail_on_call=3)
    gate = make_gate(clock, api)
    gate.get("/a", {})
    gate.get("/b", {})
    with capture_logs() as cap:
        try:
            gate.get("/c", {"symbol": "X"})
        except RestBanned:
            pass
        else:
            raise AssertionError("429 應轉成 RestBanned")
    t_429 = clock.monotonic()
    sent_before = len(api.calls)
    assert sent_before == 3
    assert any("429" in m for m in cap.messages(logging.ERROR)), "429 沒有記 ERROR"
    # 冷卻期間：各種優先順序、各種端點，一個都不能送
    refused = 0
    while clock.monotonic() < t_429 + config.REST_BAN_COOLDOWN_SECONDS - 0.5:
        for prio in (PRIORITY_FOREGROUND, PRIORITY_NORMAL, PRIORITY_BACKGROUND):
            try:
                gate.get("/api/v1/market/klines", {"symbol": "Y"}, priority=prio)
            except RestBanned as e:
                refused += 1
                assert e.remaining_seconds > 0
        clock.sleep(2.5)
    assert len(api.calls) == sent_before, "冷卻期間送出了 %d 個請求" % (len(api.calls) - sent_before)
    assert refused > 50
    # 冷卻結束才恢復
    clock.advance_to(t_429 + config.REST_BAN_COOLDOWN_SECONDS)
    gate.get("/d", {})
    assert len(api.calls) == sent_before + 1
    assert gate.stats()["http_429"] == 1


def test_ac5_requests_already_waiting_are_refused_when_ban_starts():
    """已經在排隊等額度的請求，429 一來也要放棄，不可以等到額度出來就送出去。"""
    clock = FakeClock()
    lim = RestLimiter(config.A1_REST_RATE_PER_SECOND, config.REST_BAN_COOLDOWN_SECONDS, clock=clock)
    for _ in range(config.A1_REST_RATE_PER_SECOND):
        lim.acquire()
    clock.call_at(clock.monotonic() + 0.4, lim.trip_ban)     # 在排隊期間收到別人的 429
    granted_before = lim.granted
    try:
        lim.acquire(PRIORITY_FOREGROUND)
    except RestBanned:
        pass
    else:
        raise AssertionError("排隊中遇到封鎖應拋 RestBanned")
    assert lim.granted == granted_before
    assert lim.waiting() == 0


def test_ac5_second_429_extends_the_ban():
    clock = FakeClock()
    lim = RestLimiter(1, config.REST_BAN_COOLDOWN_SECONDS, clock=clock)
    lim.trip_ban()
    clock.sleep(30)
    lim.trip_ban()                                       # 在飛的請求又收到 429：從現在重新起算
    assert abs(lim.ban_remaining() - config.REST_BAN_COOLDOWN_SECONDS) < 1e-9


def test_ac5_real_api_get_is_called_once_on_429_through_gate():
    """接真的 live.pionex_api.api_get（只換掉 requests.get）：429 只打 1 次，不會在 api_get 裡重試。"""
    calls = []

    class _R:
        status_code = 429
        text = "too many"

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append((url, params))
        return _R()

    clock = FakeClock()
    gate = RestGate(RestLimiter(config.A1_REST_RATE_PER_SECOND, config.REST_BAN_COOLDOWN_SECONDS, clock=clock),
                    ServerClock(clock), clock=clock)            # api_get 用預設（真的那支）
    orig_get, orig_sleep = lh.requests.get, lh.time.sleep
    slept = []
    lh.requests.get, lh.time.sleep = fake_get, slept.append
    try:
        with capture_logs():
            try:
                gate.get("/api/v1/market/klines", {"symbol": "BTC_USDT_PERP"})
            except RestBanned:
                pass
            else:
                raise AssertionError("應拋 RestBanned")
            try:
                gate.get("/api/v1/market/tickers", {"type": "PERP"})
            except RestBanned:
                pass
            else:
                raise AssertionError("冷卻中應拋 RestBanned")
    finally:
        lh.requests.get, lh.time.sleep = orig_get, orig_sleep
    assert len(calls) == 1, calls
    assert slept == [], "api_get 在 429 後睡了 %s（代表它打算重試）" % slept
    assert gate.limiter.ban_remaining() > 0


def test_429_log_lists_the_trigger_and_requests_still_in_flight():
    """429 的 ERROR 日誌要附「當下的請求序列」（§8 第 4 條）：觸發 429 的那一個、當時還在飛的，都要在裡面。
    用巢狀呼叫模擬：/slow 還在飛的時候，另一個請求收到 429。"""
    clock = FakeClock()
    gate_box = {}

    def responder(path, params):
        if path == "/slow":
            try:
                gate_box["gate"].get("/boom", {"symbol": "B"}, priority=PRIORITY_NORMAL)
            except RestBanned:
                pass
            return {"result": True, "data": {}, "timestamp": clock.time_ms()}
        if path == "/boom":
            raise lh.ApiError("/boom: HTTP 429", status_code=429)
        return {"result": True, "data": {}, "timestamp": clock.time_ms()}

    gate = make_gate(clock, FakeApi(clock, responder))
    gate_box["gate"] = gate
    gate.get("/done", {"symbol": "D"})
    with capture_logs() as cap:
        gate.get("/slow", {"symbol": "A"})
    err = [m for m in cap.messages(logging.ERROR) if "429" in m]
    assert len(err) == 1, cap.messages(logging.ERROR)
    for piece in ("'/slow', 'A', 'in_flight'", "'/boom', 'B', '429'", "'/done', 'D', 'ok'"):
        assert piece in err[0], (piece, err[0])
    # 完成之後序列裡的結果會被補上
    outcomes = {(r[2], r[4]) for r in gate.recent_requests()}
    assert outcomes == {("/done", "ok"), ("/slow", "ok"), ("/boom", "429")}, outcomes


def test_ac5_market_static_suite_still_passes():
    r = subprocess.run([sys.executable, os.path.join(REPO_ROOT, "tests", "test_market_static.py")],
                       cwd=REPO_ROOT, capture_output=True, timeout=300)
    out = r.stdout.decode("utf-8", "replace")
    assert r.returncode == 0, out[-2000:] + r.stderr.decode("utf-8", "replace")[-2000:]
    assert " 0 failed" in out, out[-500:]


# ============================== pionex_api 的最小修改 ==============================
def test_api_error_status_code_is_backward_compatible():
    e = lh.ApiError("x")
    assert e.status_code is None and str(e) == "x" and e.args == ("x",)
    assert lh.ApiError("y", status_code=429).status_code == 429


def test_api_get_carries_status_code_on_final_error():
    class _R:
        def __init__(self, sc):
            self.status_code = sc
            self.text = "boom"

    def run(sc, retries):
        orig_get, orig_sleep = lh.requests.get, lh.time.sleep
        lh.requests.get = lambda url, params=None, headers=None, timeout=None: _R(sc)
        lh.time.sleep = lambda s: None
        try:
            lh.api_get("/p", {}, retries=retries)
        except lh.ApiError as err:
            return err
        finally:
            lh.requests.get, lh.time.sleep = orig_get, orig_sleep
        raise AssertionError("應拋 ApiError")

    e503 = run(503, 2)
    assert e503.status_code == 503 and "重試 2 次" in str(e503) and "503" in str(e503), str(e503)
    e429 = run(429, 1)
    assert e429.status_code == 429 and "429" in str(e429)
    e404 = run(404, 3)
    assert e404.status_code == 404
    e403 = run(403, 3)
    assert e403.status_code == 403 and "封鎖" in str(e403)


# ============================== FR-5：優先順序 ==============================
def test_hold_blocks_background_but_not_foreground():
    clock = FakeClock()
    lim = RestLimiter(config.A1_REST_RATE_PER_SECOND, config.REST_BAN_COOLDOWN_SECONDS, clock=clock)
    t0 = clock.monotonic()
    lim.hold_background()
    clock.call_at(t0 + 3.0, lim.release_background)
    lim.acquire(PRIORITY_FOREGROUND)
    assert clock.monotonic() == t0, "hold 期間前景請求不可以被擋"
    lim.acquire(PRIORITY_BACKGROUND)
    assert clock.monotonic() == t0 + 3.0, "背景請求在 hold 解除前就放行了（t=%s）" % (clock.monotonic() - t0)
    lim.hold_background()
    clock.call_at(clock.monotonic() + 2.0, lim.release_background)
    lim.acquire(PRIORITY_NORMAL)
    assert clock.monotonic() == t0 + 5.0, "補種子（NORMAL）也要讓給前景"
    assert not lim.holding()


class _ManualClock:
    """只有測試說了才前進的時鐘，給多執行緒測試用。wait() 是真的在 cond 上等（最多 1 秒保險），
    由測試推進時間後 notify_all 叫醒 —— 不靠 sleep 輪詢。"""

    def __init__(self):
        self.t = 1000.0

    def monotonic(self):
        return self.t

    def time_ms(self):
        return int(self.t * 1000)

    def sleep(self, s):
        raise AssertionError("這個測試不應該 sleep")

    def wait(self, cond, timeout):
        cond.wait(1.0)


def _until(pred, what, limit=5.0):
    end = time.monotonic() + limit
    while not pred():
        if time.monotonic() > end:
            raise AssertionError("等不到：" + what)
        time.sleep(0.001)


def test_waiting_foreground_goes_before_waiting_background():
    """兩個執行緒同時在等額度：後到的前景請求要比先到的背景請求先放行。"""
    clock = _ManualClock()
    lim = RestLimiter(2, config.REST_BAN_COOLDOWN_SECONDS, clock=clock)
    lim.acquire()
    clock.t = 1000.5
    lim.acquire()                                   # 窗口滿：下一個額度在 1001.0，再下一個在 1001.5
    order = []

    def worker(name, prio):
        lim.acquire(prio)
        order.append((name, clock.t))

    tb = threading.Thread(target=worker, args=("background", PRIORITY_BACKGROUND), daemon=True)
    tb.start()
    _until(lambda: lim.waiting() == 1, "背景請求開始排隊")
    tf = threading.Thread(target=worker, args=("foreground", PRIORITY_FOREGROUND), daemon=True)
    tf.start()
    _until(lambda: lim.waiting() == 2, "前景請求開始排隊")

    def tick(t):
        clock.t = t
        with lim._cond:
            lim._cond.notify_all()

    tick(1001.0)                                     # 只空出一個額度
    tf.join(3)
    assert order == [("foreground", 1001.0)], order
    assert tb.is_alive() and lim.waiting() == 1, "背景請求不應該跟著放行"
    tick(1001.5)
    tb.join(3)
    assert order == [("foreground", 1001.0), ("background", 1001.5)], order


# ============================== AC-6：非 ASCII 合約名 ==============================
def test_ac6_non_ascii_symbol_goes_in_params_not_path():
    sym = "哈基米_USDT_PERP"
    clock = FakeClock()
    api = FakeApi(clock, lambda path, params: {"result": True, "data": {"klines": []}, "timestamp": clock.time_ms()})
    gate = make_gate(clock, api)
    klines.fetch_klines(gate, sym, "5M", config.KLINES_LIMIT)
    (_, path, params), = api.calls
    assert path == klines.KLINES_PATH, path
    assert sym not in path and "%" not in path and path.isascii(), path
    assert params["symbol"] == sym, params
    url = requests.Request("GET", config.PIONEX_BASE_URL + path, params=params).prepare().url
    assert url.isascii(), url
    assert "symbol=" + urllib.parse.quote(sym, safe="") in url, url
    assert sym not in url


# ============================== 伺服器時鐘 ==============================
def test_server_clock_offset_is_median_of_recent_samples():
    clock = FakeClock()
    sc = ServerClock(clock)
    assert sc.offset_ms() == 0 and not sc.has_estimate()
    now = clock.time_ms()
    for off in (100, 120, 5000, 110, 130):          # 一個離群值不影響中位數
        sc.observe(now + off, now - 200, now + 200)
    assert sc.offset_ms() == 120, sc.offset_ms()
    assert sc.now_ms() == clock.time_ms() + 120


def test_server_clock_warns_when_offset_exceeds_threshold():
    clock = FakeClock()
    sc = ServerClock(clock, samples=3)
    now = clock.time_ms()
    with capture_logs() as cap:
        sc.observe(now + 200, now, now)
        assert not cap.messages(logging.WARNING)
        for _ in range(3):
            sc.observe(now + config.CLOCK_OFFSET_WARN_MS + 500, now, now)
    warns = cap.messages(logging.WARNING)
    assert len(warns) == 1 and "偏移" in warns[0], warns


def test_gate_updates_server_clock_from_every_response():
    clock = FakeClock()
    api = FakeApi(clock, lambda path, params: {"result": True, "data": {}, "timestamp": clock.time_ms() + 250})
    gate = make_gate(clock, api)
    for _ in range(3):
        gate.get("/x", {})
    assert gate.server_clock.offset_ms() == 250


def test_shared_gate_is_one_per_process_and_follows_config():
    g1 = rest_gate.shared_gate()
    g2 = rest_gate.shared_gate()
    assert g1 is g2
    assert g1.limiter.rate == config.A1_REST_RATE_PER_SECOND
    assert g1.limiter.ban_cooldown == config.REST_BAN_COOLDOWN_SECONDS


def test_socket_cage_blocks_connections():
    """自證：籠子真的會擋。runner 把每個測試都包在籠子裡。"""
    before = len(_blocked_attempts)
    with socket_cage():
        try:
            socket.create_connection(("example.invalid", 80), timeout=1)
        except NetworkBlocked:
            pass
        else:
            raise AssertionError("籠子沒擋住 create_connection")
    del _blocked_attempts[before:]


# ============================== 不用 pytest 也能跑 ==============================
if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    with socket_cage():
        for name, fn in tests:
            try:
                fn()
                print(f"PASS  {name}")
            except Exception as e:  # noqa: BLE001
                failed += 1
                print(f"FAIL  {name}: {type(e).__name__}: {e}")
    if _blocked_attempts:
        failed += 1
        print(f"FAIL  有測試企圖連網 {len(_blocked_attempts)} 次：{_blocked_attempts[:3]}")
    else:
        print("PASS  全程沒有任何連網企圖（socket 籠子記錄 0 次）")
    print(f"\n{len(tests) - failed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
