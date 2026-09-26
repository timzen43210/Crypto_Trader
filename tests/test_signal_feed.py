# -*- coding: utf-8 -*-
"""
A1 驗收測試（二）— A 頻道資料層：粗篩門檻、剛收完那根、補洞一致、離線恆等式、FR-8 對帳、背景對帳。

  AC-1  門檻是推導的：MIN_RET_2H 暫改 0.11 → 門檻 0.09，且行為跟著變；A1 程式碼裡沒有寫死的門檻數字
  AC-2  送進 s4_signal 的最後一列恰好是目標 K 棒；未收完那根永遠不進去；缺目標會重試、用盡就記未到
        （兩種時序都測：回應裡「有」未收完那根、「還沒有」未收完那根）
  AC-3  live.klines.prepare_klines 與 research/g2_measure.prepare_frame 逐欄位相同，features 最後一列相同
  AC-4  tickers 樣本 = K 棒收盤價時，所有觸發 s4_signal 的 symbol 都在候選裡（跑 A1 自己的程式路徑）；
        門檻調到高於 MIN_RET_2H 時這條必須失敗（鑑別力）
  AC-7  各種故障組合下 FR-8 兩條等式每根都成立，非零計數都出現在日誌摘要
  AC-8  背景對帳：注入的漏失被報出（ERROR）、一致資料報 0、請求攤平且前景期間不搶額度
  AC-9  execution_params / python -m live 列出 A1 參數；模組名不撞標準庫
  S-3   並發取數時 worker 的例外從 future 取回：失敗 +1、ERROR 一行、其他候選照常完成
  其他  主迴圈的輪詢對齊伺服器時間整數倍；--record 不可寫進 state/ output/；jsonl 是 UTF-8；
        日誌在 cp1252 + strict 的 stderr、TZ=UTC0 下仍輸出中文合約名與台北時間

全程離線：假交易所（假 api_get）、假時鐘（不真的 sleep），runner 包在 socket 籠子裡。
不依賴 pytest：直接 `python tests/test_signal_feed.py`。
"""
import collections
import concurrent.futures
import contextlib
import heapq
import io
import itertools
import json
import logging
import math
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import tokenize
from datetime import datetime, timedelta, timezone

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

import live.pionex_api as lh  # noqa: E402
from live import config, klines, signal_feed  # noqa: E402
from live.price_buffer import PriceBuffer  # noqa: E402
from live.reconcile import Reconciler  # noqa: E402
from live.rest_gate import RestGate, RestLimiter, ServerClock  # noqa: E402
from strategy import s4_signal  # noqa: E402

BAR = klines.interval_ms(config.KLINE_INTERVAL)
BPH = klines.bars_per_hour(config.KLINE_INTERVAL)
T0 = 1_790_146_800_000            # 一個 K 棒邊界（UTC 2026-09-23 07:00）
assert T0 % BAR == 0
A1_FILES = ["live/signal_feed.py", "live/rest_gate.py", "live/klines.py", "live/price_buffer.py",
            "live/reconcile.py", "live/config.py", "live/pionex_api.py",
            "tests/test_signal_feed.py", "tests/test_rest_gate.py"]


# ============================== 離線籠子 ==============================
class NetworkBlocked(RuntimeError):
    pass


_blocked_attempts = []


@contextlib.contextmanager
def socket_cage():
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


# ============================== 假時鐘 / executor / 日誌 ==============================
class FakeClock:
    """monotonic 從 1000 秒起；sleep / wait 只推進時間。call_at 排程事件（模擬別的執行緒在那一刻做的事）。"""

    def __init__(self, start_ms):
        self._mono = 1000.0
        self._offset_ms = start_ms - 1000.0 * 1000.0
        self._events = []
        self._seq = itertools.count()
        self.sleeps = []
        self._lock = threading.RLock()

    def monotonic(self):
        return self._mono

    def time_ms(self):
        return int(round(self._offset_ms + self._mono * 1000.0))

    def mono_of(self, ms):
        return (ms - self._offset_ms) / 1000.0

    def call_at(self, mono_t, fn):
        heapq.heappush(self._events, (mono_t, next(self._seq), fn))

    def advance_to(self, t):
        with self._lock:
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


def goto(clock, server_ms):
    d = (server_ms - clock.time_ms()) / 1000.0
    if d > 0:
        clock.sleep(d)


class InlineExecutor:
    """submit 當場同步執行，結果 / 例外放進 Future（語意同 ThreadPoolExecutor）。defer 裡的 symbol 先不跑。"""

    def __init__(self, defer=()):
        self.defer = set(defer)
        self.deferred = []

    def submit(self, fn, *args, **kwargs):
        f = concurrent.futures.Future()
        if args and args[0] in self.defer:
            self.deferred.append((f, fn, args, kwargs))
            return f
        try:
            f.set_result(fn(*args, **kwargs))
        except BaseException as e:  # noqa: BLE001
            f.set_exception(e)
        return f

    def shutdown(self, wait=True, cancel_futures=False):
        pass


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


@contextlib.contextmanager
def patched(obj, name, value):
    old = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, old)


@contextlib.contextmanager
def spy_features():
    """記下每一次送進 s4_signal.features 的 DataFrame。"""
    seen = []
    orig = s4_signal.features

    def spy(df, bars_per_hour):
        seen.append(df.copy())
        return orig(df, bars_per_hour)

    s4_signal.features = spy
    try:
        yield seen
    finally:
        s4_signal.features = orig


# ============================== 合成 K 棒 ==============================
def _r(x):
    return float("%.10g" % x)


def make_frame(n, first_open, seed, events=None, base_price=1.0, high_cpos=()):
    """n 根等距 5 分 K。events = {索引: 目標 ret2h}：該根之前 2 小時等量拉抬到約這個漲幅、本根爆量 4 倍、
    收盤壓在低位（符合竭盡）；high_cpos 裡的索引則收在高位（不符合竭盡）。成交額由策略參數推導。"""
    p = config.strategy_params()
    rng = np.random.default_rng(seed)
    hi_turn = p["MAX_TURN24H"] if p["MAX_TURN24H"] is not None else 4 * p["MIN_TURN24H"]
    vol_per_bar = (p["MIN_TURN24H"] + hi_turn) / 2.0 / (24 * BPH) / base_price
    ramp = 2 * BPH
    logret = rng.normal(0.0, 0.0002, n)
    events = dict(events or {})
    for e, r in events.items():
        logret[e - ramp + 1:e + 1] += math.log1p(r) / ramp
    close = base_price * np.exp(np.cumsum(logret))
    open_ = np.empty(n)
    open_[0] = close[0]
    open_[1:] = close[:-1]
    wig = rng.uniform(0.0005, 0.002, n)
    high = np.maximum(open_, close) * (1 + wig)
    low = np.minimum(open_, close) * (1 - wig)
    vol = vol_per_bar * np.exp(rng.normal(0.0, 0.2, n))
    for e in events:
        vol[e] *= 4.0
        low[e] = min(open_[e], close[e]) * (1 - 0.001)
        high[e] = max(open_[e], close[e]) * (1.0005 if e in high_cpos else 1.03)
    return pd.DataFrame({
        "time": first_open + np.arange(n, dtype="int64") * BAR,
        "open": [_r(x) for x in open_], "high": [_r(x) for x in high], "low": [_r(x) for x in low],
        "close": [_r(x) for x in close], "volume": [_r(x) for x in vol],
    })


class FakeExchange:
    """假派網：依假時鐘回 tickers（每個 symbol 最後一根已收完 K 棒的收盤價）與 klines（新到舊、字串數值、
    含未收完那根；未收完那根的數值刻意放大，誤用就會被抓到）。各種故障可用 hook 注入。"""

    def __init__(self, clock, frames):
        self.clock = clock
        self.frames = {s: df.reset_index(drop=True) for s, df in frames.items()}
        self.calls = []
        self.tickers_exclude = set()
        self.tickers_invalid = set()
        self.tickers_price_hook = None        # fn(sym, now_ms, price) -> price
        self.tickers_fail = []                # 依序拋出的例外
        self.klines_hook = {}                 # sym -> fn(attempt, now_ms, rows) -> rows（可拋例外）
        self.klines_attempts = collections.Counter()
        self.delays = []                      # [(after_ms, path, 秒)]：該時刻之後第一個 path 請求卡住這麼久
        self.starts = []                      # [(開始送出的時刻, path)]（calls 記的是回應時刻）
        self._lock = threading.Lock()

    def api_get(self, path, params=None, retries=3, timeout=20):
        with self._lock:
            self.starts.append((self.clock.time_ms(), path))
        for d in list(self.delays):
            if d[1] == path and self.clock.time_ms() >= d[0]:
                self.delays.remove(d)
                self.clock.sleep(d[2])        # 請求本身卡住（DNS、connect + read 逾時）
                break
        now = self.clock.time_ms()
        params = dict(params or {})
        with self._lock:
            self.calls.append((self.clock.monotonic(), now, path, params.get("symbol")))
        if path == signal_feed.TICKERS_PATH:
            if self.tickers_fail:
                raise self.tickers_fail.pop(0)
            rows = []
            for sym, df in self.frames.items():
                if sym in self.tickers_exclude:
                    continue
                closed = df[df["time"] + BAR <= now]
                if not len(closed):
                    continue
                price = float(closed["close"].iloc[-1])
                if self.tickers_price_hook is not None:
                    price = self.tickers_price_hook(sym, now, price)
                rows.append({"symbol": sym, "time": now - 300, "open": "1", "high": "1", "low": "1",
                             "close": "abc" if sym in self.tickers_invalid else repr(price),
                             "volume": "1", "amount": "1", "count": 1})
            return {"result": True, "data": {"tickers": rows}, "timestamp": now}
        if path == klines.KLINES_PATH:
            sym = params["symbol"]
            df = self.frames[sym]
            upto = df[df["time"] <= now].tail(int(params["limit"]))
            rows = []
            for r in upto.itertuples(index=False):
                forming = r.time + BAR > now
                c = r.close * (3.0 if forming else 1.0)
                v = r.volume * (50.0 if forming else 1.0)
                rows.append({"time": int(r.time), "open": repr(r.open), "close": repr(c),
                             "high": repr(max(r.high, c)), "low": repr(r.low), "volume": repr(v)})
            rows.reverse()                    # 派網是新到舊
            with self._lock:
                self.klines_attempts[sym] += 1
                attempt = self.klines_attempts[sym]
            hook = self.klines_hook.get(sym)
            if hook is not None:
                rows = hook(attempt, now, rows)
            return {"result": True, "data": {"klines": rows}, "timestamp": now}
        raise AssertionError("沒預期的 path %s" % path)


def build_feed(clock, ex, syms, rate=None, **kw):
    rate = config.A1_REST_RATE_PER_SECOND if rate is None else rate
    gate = RestGate(RestLimiter(rate, config.REST_BAN_COOLDOWN_SECONDS, clock=clock), ServerClock(clock),
                    clock=clock, api_get=ex.api_get)
    kw.setdefault("fg_executor", InlineExecutor())
    kw.setdefault("bg_executor", InlineExecutor())
    feed = signal_feed.SignalFeed(gate=gate, clock=clock, universe_fn=lambda initial=False: list(syms), **kw)
    feed.refresh_universe(initial=True)
    return feed, gate


def truth_signals(frame, lo_idx, hi_idx):
    """用研究端的 prepare_frame + s4_signal 在完整資料上算的「真實」訊號（開盤時刻集合）。"""
    from research import g2_measure
    df, _ = g2_measure.prepare_frame(frame, BAR)
    sig = s4_signal.signal(df, BPH)
    return {int(df["time"].iloc[i]) for i in range(lo_idx, hi_idx + 1) if sig.iloc[i] == -1}


def _min_ret():
    return config.strategy_params()["MIN_RET_2H"]


# ============================== AC-1：門檻是推導的 ==============================
def test_ac1_threshold_is_min_ret_minus_margin():
    p = config.strategy_params()
    assert signal_feed.screen_threshold() == p["MIN_RET_2H"] - config.SCREEN_RET2H_MARGIN


def test_ac1_threshold_follows_min_ret_2h_when_it_changes():
    with patched_param("MIN_RET_2H", 0.11):
        thr = signal_feed.screen_threshold()
        assert abs(thr - 0.09) < 1e-12, thr


@contextlib.contextmanager
def patched_param(key, value):
    old = s4_signal.DEFAULT_PARAMS[key]
    s4_signal.DEFAULT_PARAMS[key] = value
    try:
        yield
    finally:
        s4_signal.DEFAULT_PARAMS[key] = old


def test_ac1_screen_behaviour_follows_min_ret_2h():
    """行為層面的鑑別：MIN_RET_2H = 0.11 時，近似 ret2h 0.10 的 symbol 必須成為候選。
    粗篩裡寫死任何高於 0.10 的門檻（例如舊的 0.12）這條都會失敗。"""
    with patched_param("MIN_RET_2H", 0.11):
        first_open = T0 - 300 * BAR
        frames = {"UP": make_frame(320, first_open, 1, events={299: 0.10}),
                  "FLAT": make_frame(320, first_open, 2)}
        clock = FakeClock(T0 - 60_000)
        ex = FakeExchange(clock, frames)
        feed, _ = build_feed(clock, ex, sorted(frames))
        with capture_logs():
            feed.start_seeding(sorted(frames), initial=True)
            goto(clock, T0 + 200)
            res = feed.judge_bar(T0)
        assert abs(res.threshold - 0.09) < 1e-12
        approx = res.approx_ret2h["UP"]
        assert 0.09 < approx < 0.11, approx
        assert [c.symbol for c in res.candidates] == ["UP"], res.candidates


def test_ac1_no_hardcoded_thresholds_in_a1_code():
    """A1 新增 / 修改的程式碼（不含註解與字串 / docstring）裡沒有 0.12、0.14；0.02 只出現在
    SCREEN_RET2H_MARGIN 的定義那一行。用 tokenize 掃，註解與字串天然被排除。"""
    banned = {float("0.12"), float("0.14")}
    margin = float("0.02")
    hits, margin_hits = [], []
    for rel in A1_FILES:
        path = os.path.join(REPO_ROOT, rel)
        with open(path, encoding="utf-8") as f:
            src = f.read()
        lines = src.splitlines()
        for tok in tokenize.generate_tokens(io.StringIO(src).readline):
            if tok.type != tokenize.NUMBER:
                continue
            try:
                val = float(tok.string.replace("_", ""))
            except ValueError:
                continue
            where = "%s:%d: %s" % (rel, tok.start[0], lines[tok.start[0] - 1].strip())
            if val in banned:
                hits.append(where)
            if val == margin:
                margin_hits.append((rel, lines[tok.start[0] - 1].strip()))
    assert not hits, "寫死的門檻數字：%s" % hits
    assert margin_hits == [("live/config.py", "SCREEN_RET2H_MARGIN = 0.02")], margin_hits


# ============================== AC-2：永遠判定剛收完的那根 ==============================
def _single_candidate(hook, sym="S"):
    """一個 symbol、強制當候選，目標 K 棒 = 開盤 T0-5 分（索引 299），收盤 T0。回傳 (res, ex, clock, seen)。"""
    first_open = T0 - 300 * BAR
    frames = {sym: make_frame(320, first_open, 7, events={299: _min_ret() + 0.03})}
    clock = FakeClock(T0 - 60_000)
    ex = FakeExchange(clock, frames)
    feed, _ = build_feed(clock, ex, [sym], force_candidates=1)
    with capture_logs() as cap:
        feed.start_seeding([sym], initial=True)
        ex.klines_attempts.clear()
        if hook is not None:
            ex.klines_hook[sym] = hook
        goto(clock, T0 + 200)
        with spy_features() as seen:
            res = feed.judge_bar(T0)
    return res, ex, clock, seen, frames[sym], cap


def _assert_judged_exactly_target(res, seen, frame, sym="S"):
    target_open = T0 - BAR
    assert len(seen) == 1, "features 被呼叫 %d 次" % len(seen)
    df = seen[0]
    assert int(df["time"].iloc[-1]) == target_open, "最後一列不是目標 K 棒：%s" % df["time"].iloc[-1]
    assert int(df["time"].max()) == target_open
    assert T0 not in set(df["time"].tolist()), "未收完的那根被送進 s4_signal"
    want = frame[frame["time"] == target_open].iloc[0]
    assert res.judged[sym]["target_bar"] == [float(want[c]) for c in ("open", "high", "low", "close", "volume")]
    assert sym in res.fetch_ok


def test_ac2_forming_bar_present_is_never_used():
    """時序一：回應裡已經有未收完的那根（降冪、在第一筆、數值被放大 3 倍），外加一筆重複的舊 K 棒。"""
    def hook(attempt, now, rows):
        assert int(rows[0]["time"]) == T0, "假交易所應該回傳未收完那根"
        dup = dict(rows[5])
        dup["volume"] = repr(float(dup["volume"]) * 2)
        return rows[:3] + [dup] + rows[3:]
    res, ex, clock, seen, frame, _ = _single_candidate(hook)
    _assert_judged_exactly_target(res, seen, frame)
    assert res.judged["S"]["attempts"] == 1
    # 訊號價是目標 K 棒的收盤價，不是未收完那根（假交易所把它放大了 3 倍）
    target_close = float(frame[frame["time"] == T0 - BAR]["close"].iloc[0])
    assert res.signals and res.signals[0].signal_price == target_close, res.signals


def test_ac2_forming_bar_absent_still_judges_target():
    """時序二：收盤後極短時間內，回應裡還沒有未收完的那根（第一筆就是目標）。"""
    def hook(attempt, now, rows):
        return [r for r in rows if int(r["time"]) < T0]
    res, ex, clock, seen, frame, _ = _single_candidate(hook)
    _assert_judged_exactly_target(res, seen, frame)


def test_ac2_missing_target_is_retried_then_used():
    def hook(attempt, now, rows):
        if attempt <= 2:                        # 前兩次：目標與未收完那根都還沒出現
            return [r for r in rows if int(r["time"]) < T0 - BAR]
        return rows
    res, ex, clock, seen, frame, _ = _single_candidate(hook)
    _assert_judged_exactly_target(res, seen, frame)
    assert res.judged["S"]["attempts"] == 3
    assert ex.klines_attempts["S"] == 3
    assert clock.sleeps.count(config.TARGET_BAR_RETRY_WAIT_SECONDS) >= 2


def test_ac2_missing_target_never_falls_back_to_previous_or_forming_bar():
    """目標 K 棒一直不在（前一根與未收完那根都在）→ 重試用盡、記「目標 K 棒未到」，完全不判定。"""
    def hook(attempt, now, rows):
        return [r for r in rows if int(r["time"]) != T0 - BAR]
    res, ex, clock, seen, frame, _ = _single_candidate(hook)
    assert seen == [], "目標未到卻呼叫了 s4_signal.features"
    assert res.fetch_failed.get(signal_feed.FETCH_TARGET_MISSING) == ["S"], res.fetch_failed
    assert res.fetch_detail["S"]["attempts"] == config.TARGET_BAR_ATTEMPTS
    assert ex.klines_attempts["S"] == config.TARGET_BAR_ATTEMPTS
    assert res.fetch_detail["S"]["has_next_bar"] is True
    assert not res.judged and not res.signals and not res.fetch_ok
    assert signal_feed.FETCH_TARGET_MISSING in res.degraded_reasons
    assert res.accounting_problems() == []


def test_ac2_evaluate_target_returns_none_without_target():
    first_open = T0 - 300 * BAR
    frame = make_frame(320, first_open, 3)
    rows = [{"time": int(t), "open": repr(o), "high": repr(h), "low": repr(lo), "close": repr(c),
             "volume": repr(v)} for t, o, h, lo, c, v in frame.itertuples(index=False) if t <= T0]
    rows.reverse()
    without_target = [r for r in rows if int(r["time"]) != T0 - BAR]
    with spy_features() as seen:
        assert signal_feed.evaluate_target(without_target, T0 - BAR, BAR, BPH, config.strategy_params()) is None
    assert seen == []
    ev = signal_feed.evaluate_target(rows, T0 - BAR, BAR, BPH, config.strategy_params())
    assert ev is not None and ev["bars"] >= 24 * BPH + 1


# ============================== AC-3：補洞與回測一致 ==============================
def _messy_rows(seed, n=420, with_amount=False):
    rng = random.Random(seed)
    first_open = T0 - (n - 1) * BAR
    frame = make_frame(n, first_open, seed)
    rows = []
    for t, o, h, lo, c, v in frame.itertuples(index=False):
        if rng.random() < 0.06:                  # 缺漏
            continue
        row = {"time": int(t), "open": repr(o), "high": repr(h), "low": repr(lo), "close": repr(c), "volume": repr(v)}
        if with_amount:
            row["amount"] = repr(c * v)
        rows.append(row)
        if rng.random() < 0.04:                  # 同一時刻、不同數值的重複
            dup = dict(row)
            dup["close"] = repr(c * 1.01)
            dup["volume"] = repr(v * 3)
            rows.insert(rng.randrange(len(rows) + 1), dup)
    rows.append({"time": T0 + BAR, "open": "9", "high": "9", "low": "9", "close": "9", "volume": "9"})   # 未收完
    rows.append({"time": "bad", "open": "1", "high": "1", "low": "1", "close": "1", "volume": "1"})
    rows[10]["close"] = "not-a-number"
    order = rng.choice(["shuffle", "desc", "asc"])
    if order == "shuffle":
        rng.shuffle(rows)
    elif order == "desc":
        rows.sort(key=lambda r: -1 if r["time"] == "bad" else int(r["time"]), reverse=True)
    return rows


def test_ac3_prepare_klines_matches_research_prepare_frame():
    from research import g2_measure
    checked = 0
    for seed in range(8):
        for with_amount in (False, True):
            rows = _messy_rows(seed, with_amount=with_amount)
            for t_now in (None, T0, T0 + BAR):
                mine, f1 = klines.prepare_klines(klines.raw_frame(rows), BAR, t_now=t_now)
                ref, f2 = g2_measure.prepare_frame(pd.DataFrame(rows), BAR, t_now=t_now)
                pd.testing.assert_frame_equal(mine, ref, check_exact=True)
                assert f1 == f2, (f1, f2)
                if len(mine) > 24 * BPH:
                    a = s4_signal.features(mine, BPH).iloc[-1]
                    b = s4_signal.features(ref, BPH).iloc[-1]
                    pd.testing.assert_series_equal(a, b, check_exact=True)
                checked += 1
    assert checked == 48


def test_ac3_duplicates_keep_last_in_input_order_like_backtest():
    rows = [{"time": T0 - 2 * BAR, "open": "1", "high": "1", "low": "1", "close": "1", "volume": "1"},
            {"time": T0 - BAR, "open": "2", "high": "2", "low": "2", "close": "2", "volume": "2"},
            {"time": T0 - 2 * BAR, "open": "3", "high": "3", "low": "3", "close": "3", "volume": "3"}]
    df, filled = klines.prepare_klines(klines.raw_frame(rows), BAR)
    assert df["close"].tolist() == [3.0, 2.0] and filled == 0


def test_ac3_gap_fill_rules():
    rows = [{"time": T0 - 4 * BAR, "open": "1", "high": "2", "low": "0.5", "close": "1.5", "volume": "10"},
            {"time": T0 - BAR, "open": "1.5", "high": "3", "low": "1", "close": "2", "volume": "20"}]
    df, filled = klines.prepare_klines(klines.raw_frame(rows), BAR)
    assert filled == 2 and len(df) == 4
    gap = df.iloc[1]
    assert (gap["open"], gap["high"], gap["low"], gap["close"], gap["volume"]) == (1.5, 1.5, 1.5, 1.5, 0.0)


# ============================== AC-4：離線恆等式跑在 A1 自己的程式路徑上 ==============================
_SCEN_FIRST_OPEN = T0 - 400 * BAR
_SCEN_LO, _SCEN_HI = 330, 399                    # 判定的目標 K 棒索引（收盤 = 開盤 + 1 根）


def _scenario_frames():
    m = _min_ret()
    spec = {
        "EV_A": ({340: m + 0.01, 380: m + 0.06}, ()),
        "EV_B": ({352: m + 0.01}, ()),
        "EV_C": ({365: m + 0.03}, ()),
        "EV_D": ({390: m + 0.01}, ()),
        "NEAR": ({345: m - 0.005}, ()),          # 近似會進候選，但 ret2h 沒到 MIN，不是訊號
        "HICPOS": ({370: m + 0.03}, (370,)),     # 漲幅夠、爆量，但收在高位 → 不是訊號
    }
    frames = {}
    for i, (sym, (ev, hc)) in enumerate(sorted(spec.items())):
        frames[sym] = make_frame(420, _SCEN_FIRST_OPEN, 100 + i, events=ev, high_cpos=hc)
    for i in range(6):
        frames["FLAT_%d" % i] = make_frame(420, _SCEN_FIRST_OPEN, 200 + i, base_price=0.5 + i)
    return frames


def _close_of(idx):
    return _SCEN_FIRST_OPEN + (idx + 1) * BAR


def _scenario_run(margin=None, tickers_price_hook=None, reconciler_kw=None):
    frames = _scenario_frames()
    clock = FakeClock(_SCEN_FIRST_OPEN + 330 * BAR + 1000)
    ex = FakeExchange(clock, frames)
    ex.tickers_price_hook = tickers_price_hook
    syms = sorted(frames)
    reconciler = None
    feed, gate = build_feed(clock, ex, syms)
    if reconciler_kw is not None:
        reconciler = Reconciler(gate, clock=clock, **reconciler_kw)
    results = []
    ctx = patched(config, "SCREEN_RET2H_MARGIN", margin) if margin is not None else contextlib.nullcontext()
    with ctx, capture_logs() as cap:
        feed.start_seeding(syms, initial=True)
        for idx in range(_SCEN_LO, _SCEN_HI + 1):
            goto(clock, _close_of(idx) + 200)
            res = feed.judge_bar(_close_of(idx))
            results.append(res)
            if reconciler is not None:
                reconciler.add_bar(res)
    truth = {(s, t) for s, f in frames.items() for t in truth_signals(f, _SCEN_LO, _SCEN_HI)}
    by_open = {r.bar_open_ms: r for r in results}
    misses = [(s, t) for s, t in truth if s not in {c.symbol for c in by_open[t].candidates}]
    return {"frames": frames, "clock": clock, "ex": ex, "feed": feed, "gate": gate, "results": results,
            "truth": truth, "misses": misses, "by_open": by_open, "reconciler": reconciler, "logs": cap}


def test_ac4_every_s4_signal_is_a_candidate_and_is_found():
    run = _scenario_run()
    truth, by_open = run["truth"], run["by_open"]
    assert len(truth) >= 4, "情境裡應該有好幾筆真實訊號，否則這條測試沒有意義：%s" % truth
    assert run["misses"] == [], "有訊號不在候選清單裡：%s" % run["misses"]
    found = {(s.symbol, s.bar_open_ms) for r in run["results"] for s in r.signals}
    assert found == truth, "A1 判定的訊號與完整資料重算不一致：多 %s 少 %s" % (found - truth, truth - found)
    for r in run["results"]:
        assert r.accounting_problems() == [], r.accounting_problems()
        assert not r.degraded, (r.bar_close_ms, r.degraded_reasons)
    # 近似 ret2h 在這個情境下就是 s4_signal 的 ret2h（樣本 = K 棒收盤價）
    s, t = sorted(truth)[0]
    ev = by_open[t].judged[s]
    assert abs(by_open[t].approx_ret2h[s] - ev["ret2h"]) < 1e-12


def test_ac4_identity_check_has_discriminating_power():
    """門檻調到高於 MIN_RET_2H（餘裕為負）時，同一個檢查必須抓到漏失。"""
    run = _scenario_run(margin=-0.03)
    assert run["results"][0].threshold > _min_ret()
    assert run["misses"], "門檻高於 MIN_RET_2H 卻沒有漏失：恆等式檢查沒有鑑別力"


# ============================== AC-7：FR-8 對帳在各種故障組合下恆成立 ==============================
FAULTS = ("missing", "invalid", "nobase", "cold", "tgt", "api", "exc", "short", "ban")
_FAULT_FRAMES = {}


def _fault_frames():
    if not _FAULT_FRAMES:
        first_open = T0 - 300 * BAR
        up = _min_ret() + 0.03
        f = _FAULT_FRAMES
        f["OK_A"] = make_frame(320, first_open, 11, events={299: up})
        f["OK_B"] = make_frame(320, first_open, 12, events={299: up + 0.03})
        for i in range(3):
            f["FLAT_%d" % i] = make_frame(320, first_open, 13 + i)
        f["MISS"] = make_frame(320, first_open, 20)
        f["INV"] = make_frame(320, first_open, 21)
        f["NB_1"] = make_frame(320, first_open, 22)
        f["NB_2"] = make_frame(320, first_open, 23)
        f["COLD"] = make_frame(320, first_open, 24)
        for i, name in enumerate(("C_TGT", "C_API", "C_EXC", "C_BAN")):
            f[name] = make_frame(320, first_open, 30 + i, events={299: up + 0.001 * i})
        f["C_SHORT"] = make_frame(60, first_open + 260 * BAR, 40, events={39: up})
    return _FAULT_FRAMES


_FAULT_SYMS = {"missing": ["MISS"], "invalid": ["INV"], "nobase": ["NB_1", "NB_2"], "cold": ["COLD"],
               "tgt": ["C_TGT"], "api": ["C_API"], "exc": ["C_EXC"], "short": ["C_SHORT"], "ban": ["C_BAN"]}


def _fault_bar(faults, tickers_fail=()):
    all_frames = _fault_frames()
    syms = ["OK_A", "OK_B", "FLAT_0", "FLAT_1", "FLAT_2"]
    for f in faults:
        syms += _FAULT_SYMS[f]
    frames = {s: all_frames[s] for s in syms}
    clock = FakeClock(T0 - 60_000)
    ex = FakeExchange(clock, frames)
    if "missing" in faults:
        ex.tickers_exclude.add("MISS")
    if "invalid" in faults:
        ex.tickers_invalid.add("INV")
    feed, gate = build_feed(clock, ex, syms, bg_executor=InlineExecutor(defer={"COLD"}))
    with patched(config, "SCREEN_NO_BASE_MAX_CANDIDATES", 1), capture_logs() as cap:
        feed.start_seeding([s for s in syms if not s.startswith("NB_")], initial=True)
        if "tgt" in faults:
            ex.klines_hook["C_TGT"] = lambda a, now, rows: [r for r in rows if int(r["time"]) != T0 - BAR]
        if "api" in faults:
            def api_err(a, now, rows):
                raise lh.ApiError("/api/v1/market/klines: HTTP 500 boom", status_code=500)
            ex.klines_hook["C_API"] = api_err
        if "exc" in faults:
            def boom(a, now, rows):
                raise RuntimeError("模擬 worker 內的未預期例外")
            ex.klines_hook["C_EXC"] = boom
        if "ban" in faults:
            def ban(a, now, rows):
                raise lh.ApiError("/api/v1/market/klines: 重試 1 次仍失敗：HTTP 429", status_code=429)
            ex.klines_hook["C_BAN"] = ban
        ex.tickers_fail = list(tickers_fail)
        goto(clock, T0 + 200)
        res = feed.judge_bar(T0)
    return res, cap, ex


def _check_fault_result(faults, res, cap):
    assert res.accounting_problems() == [], (faults, res.accounting_problems())
    uns, failed = res.unscreened, res.fetch_failed
    if "missing" in faults:
        assert "MISS" in uns[signal_feed.UNSCREENED_NOT_IN_TICKERS]
    if "invalid" in faults:
        assert "INV" in uns[signal_feed.UNSCREENED_TICKER_INVALID]
    if "nobase" in faults:
        assert uns[signal_feed.UNSCREENED_NO_BASE] == ["NB_1", "NB_2"]
        assert len(res.promoted_no_base) == 1 and "no_base_over_cap" in res.degraded_reasons
    if "cold" in faults:
        assert uns[signal_feed.UNSCREENED_COLD_START] == ["COLD"]
        assert signal_feed.UNSCREENED_COLD_START in res.degraded_reasons
    # 有 429 時，排在 C_BAN 後面的候選一律變成「封鎖冷卻中」（沒送出），這是 FR-5 要的行為
    banned = set(failed.get(signal_feed.FETCH_BANNED, ())) if "ban" in faults else set()
    for fault, sym, reason in (("tgt", "C_TGT", signal_feed.FETCH_TARGET_MISSING),
                               ("api", "C_API", signal_feed.FETCH_API_ERROR),
                               ("exc", "C_EXC", signal_feed.FETCH_EXCEPTION),
                               ("short", "C_SHORT", signal_feed.FETCH_INSUFFICIENT_HISTORY)):
        if fault in faults:
            assert sym in failed.get(reason, ()) or sym in banned, (faults, sym, failed)
    if "ban" in faults:
        assert "C_BAN" in banned
        order = [c.symbol for c in res.candidates]
        after = set(order[order.index("C_BAN") + 1:])
        assert banned == {"C_BAN"} | after, (order, banned)
    # 任何非零計數都要能從日誌看到：摘要行帶每個非零原因
    summary = [m for m in cap.messages(logging.INFO) if m.startswith("K 棒 ") and " 收盤 | " in m]
    assert len(summary) == 1, summary
    for reason in list(res.unscreened_counts()) + list(res.fetch_failed_counts()):
        assert reason + "=" in summary[0], (reason, summary[0])
    assert ("DEGRADED" in summary[0]) == res.degraded


def test_ac7_accounting_holds_for_each_single_fault():
    for f in FAULTS:
        res, cap, _ = _fault_bar((f,))
        _check_fault_result((f,), res, cap)


def test_ac7_accounting_holds_for_all_faults_at_once():
    res, cap, _ = _fault_bar(FAULTS)
    _check_fault_result(FAULTS, res, cap)
    assert res.fetch_ok, "所有故障一起發生時，正常的候選仍應完成"


def test_ac7_accounting_holds_for_random_fault_combinations():
    rng = random.Random(7)
    for _ in range(30):
        faults = tuple(f for f in FAULTS if rng.random() < 0.5)
        res, cap, _ = _fault_bar(faults)
        _check_fault_result(faults, res, cap)


def test_ac7_ban_stops_remaining_candidates_without_sending():
    res, cap, ex = _fault_bar(("ban",))
    banned = res.fetch_failed[signal_feed.FETCH_BANNED]
    sent = [c for c in ex.calls if c[2] == klines.KLINES_PATH and c[1] >= T0]
    sent_syms = [c[3] for c in sent]
    assert "C_BAN" in sent_syms
    after = sent_syms[sent_syms.index("C_BAN") + 1:]
    assert after == [], "收到 429 之後還送了 %s" % after
    assert set(banned) >= {"C_BAN"}
    assert any("封鎖" in m for m in cap.messages(logging.ERROR))


def test_ac7_tickers_failure_and_ban_at_close():
    err = lh.ApiError("/api/v1/market/tickers: HTTP 502 bad gateway", status_code=502)
    res, cap, _ = _fault_bar((), tickers_fail=[err, err])
    assert res.accounting_problems() == []
    assert res.unscreened[signal_feed.UNSCREENED_TICKERS_FAILED] == sorted(res.unscreened[signal_feed.UNSCREENED_TICKERS_FAILED])
    assert len(res.unscreened[signal_feed.UNSCREENED_TICKERS_FAILED]) == res.universe_size
    assert res.candidates == [] and res.degraded
    # 第一次失敗、第二次成功 → 正常
    res2, _, _ = _fault_bar((), tickers_fail=[err])
    assert not res2.degraded and res2.accounting_problems() == [] and res2.fetch_ok
    # 收盤 tickers 本身收到 429
    ban = lh.ApiError("/api/v1/market/tickers: 重試 1 次仍失敗", status_code=429)
    res3, cap3, _ = _fault_bar(("tgt",), tickers_fail=[ban])
    assert res3.accounting_problems() == []
    assert len(res3.unscreened[signal_feed.UNSCREENED_BANNED]) == res3.universe_size
    assert signal_feed.UNSCREENED_BANNED in res3.degraded_reasons
    assert any("封鎖" in m for m in cap3.messages(logging.ERROR))


# ============================== S-3：future 裡的例外不可以安靜消失 ==============================
def test_s3_worker_exception_in_thread_pool_is_counted_and_logged():
    first_open = T0 - 300 * BAR
    up = _min_ret() + 0.03
    frames = {name: make_frame(320, first_open, 50 + i, events={299: up + 0.001 * i})
              for i, name in enumerate(("W_A", "W_B", "W_BOOM", "W_C"))}
    clock = FakeClock(T0 - 60_000)
    ex = FakeExchange(clock, frames)

    def boom(attempt, now, rows):
        raise RuntimeError("worker 爆了")
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="test-fetch")
    try:
        feed, _ = build_feed(clock, ex, sorted(frames), rate=1000, fg_executor=pool)
        with capture_logs() as cap:
            feed.start_seeding(sorted(frames), initial=True)
            ex.klines_hook["W_BOOM"] = boom
            goto(clock, T0 + 200)
            res = feed.judge_bar(T0)
    finally:
        pool.shutdown(wait=True)
    assert res.fetch_failed.get(signal_feed.FETCH_EXCEPTION) == ["W_BOOM"], res.fetch_failed
    assert sorted(res.fetch_ok) == ["W_A", "W_B", "W_C"], res.fetch_ok
    assert res.accounting_problems() == []
    errors = [r for r in cap.records if r.levelno == logging.ERROR and "W_BOOM" in r.getMessage()]
    assert len(errors) == 1, [r.getMessage() for r in cap.records if r.levelno >= logging.WARNING]
    assert errors[0].exc_info and "worker 爆了" in str(errors[0].exc_info[1])
    assert signal_feed.FETCH_EXCEPTION in res.degraded_reasons


# ============================== AC-8：背景對帳 ==============================
_RECON_KW = {"interval_s": 3600, "spread_fraction": config.RECONCILE_SPREAD_FRACTION, "start_delay_s": 0}


def test_ac8_reconcile_consistent_run_reports_zero_misses():
    run = _scenario_run(reconciler_kw=_RECON_KW)
    rec, clock = run["reconciler"], run["clock"]
    with capture_logs() as cap:
        rr = rec.run_batch(_close_of(_SCEN_HI))
    assert rr.bars == 12 and rr.symbols == len(run["frames"]), (rr.bars, rr.symbols)
    assert rr.misses == [] and rr.misses_on_degraded == 0
    assert rr.pairs_compared == rr.bars * rr.symbols, (rr.pairs_compared, rr.true_unavailable)
    assert rr.abs_error["n"] == rr.pairs_compared and rr.abs_error["max"] < 1e-9, rr.abs_error
    assert rr.ohlcv_checked > 0 and rr.ohlcv_mismatches == [], (rr.ohlcv_checked, rr.ohlcv_mismatches)
    assert not cap.messages(logging.ERROR), cap.messages(logging.ERROR)


def test_ac8_reconcile_reports_injected_miss_with_error_log():
    """真實 ret2h >= MIN_RET_2H，但收盤那次 tickers 報的價格偏低、近似值落在粗篩門檻下 → 漏失 1 筆 + ERROR。"""
    miss_close = _close_of(390)                       # EV_D 的訊號 K 棒

    def hook(sym, now, price):
        return price * 0.9 if sym == "EV_D" and miss_close <= now < miss_close + 5000 else price
    run = _scenario_run(tickers_price_hook=hook, reconciler_kw=_RECON_KW)
    r390 = run["by_open"][miss_close - BAR]
    assert r390.approx_ret2h["EV_D"] < r390.threshold, "注入沒有生效"
    assert ("EV_D", miss_close - BAR) in run["misses"], "粗篩本身就該漏掉它（這是注入的漏失）"
    with capture_logs() as cap:
        rr = run["reconciler"].run_batch(_close_of(_SCEN_HI))
    assert [(m["symbol"], m["bar_close_ms"], m["why"]) for m in rr.misses] == [("EV_D", miss_close, "below_threshold")], rr.misses
    assert rr.misses[0]["true_ret2h"] >= _min_ret()
    errors = cap.messages(logging.ERROR)
    assert len(errors) == 1 and "EV_D" in errors[0] and "漏失 1 筆" in errors[0], errors


def test_ac8_reconcile_miss_on_degraded_bar_is_not_an_error_but_counted():
    miss_close = _close_of(390)

    def hook(sym, now, price):
        return price * 0.9 if sym == "EV_D" and miss_close <= now < miss_close + 5000 else price
    run = _scenario_run(tickers_price_hook=hook, reconciler_kw=_RECON_KW)
    rec = run["reconciler"]
    r390 = run["by_open"][miss_close - BAR]
    r390.degraded_reasons.append("tickers_failed")   # 假裝那根是 degraded
    rec.add_bar(r390)
    with capture_logs() as cap:
        rr = rec.run_batch(_close_of(_SCEN_HI))
    assert rr.misses == [] and rr.misses_on_degraded == 1
    assert not cap.messages(logging.ERROR)


def test_ac8_reconcile_detects_ohlcv_change_after_close():
    run = _scenario_run(reconciler_kw=_RECON_KW)
    ex = run["ex"]
    target_open = _close_of(390) - BAR
    judged_syms = sorted(run["by_open"][target_open].judged)
    assert judged_syms, "那根應該有候選被判定"
    sym = judged_syms[0]
    df = ex.frames[sym]
    df.loc[df["time"] == target_open, "volume"] = df.loc[df["time"] == target_open, "volume"] * 1.5
    with capture_logs() as cap:
        rr = run["reconciler"].run_batch(_close_of(_SCEN_HI))
    assert [(m["symbol"], m["bar_close_ms"]) for m in rr.ohlcv_mismatches] == [(sym, target_open + BAR)]
    assert any("不一致" in m for m in cap.messages(logging.WARNING))


def test_ac8_reconcile_requests_are_spread_and_yield_to_foreground():
    run = _scenario_run(reconciler_kw=_RECON_KW)
    clock, gate, ex, rec = run["clock"], run["gate"], run["ex"], run["reconciler"]
    n_before = len(ex.calls)
    t_start_ms = clock.time_ms()
    holds = []
    fg_sent = []
    first_close = (t_start_ms // BAR + 1) * BAR
    guard = config.FOREGROUND_GUARD_SECONDS
    for k in range(13):                               # 對帳這一個小時裡的每根 K 棒收盤
        close = first_close + k * BAR
        on, off = clock.mono_of(close) - guard, clock.mono_of(close) + 3.0
        holds.append((on, off))

        def start(close=close):
            gate.limiter.hold_background()
            gate.get(signal_feed.TICKERS_PATH, dict(signal_feed.TICKERS_PARAMS))   # 前景取數照常拿到額度
            fg_sent.append(clock.monotonic())
        clock.call_at(on, start)
        clock.call_at(off, gate.limiter.release_background)
    t0 = clock.monotonic()
    with capture_logs():
        rr = rec.run_batch(_close_of(_SCEN_HI))
    bg = [c[0] for c in ex.calls[n_before:] if c[2] == klines.KLINES_PATH]
    n = rr.symbols
    assert len(bg) == n, (len(bg), n)
    spacing = rec.interval_s * rec.spread_fraction / n
    # 攤平：第 i 個請求約在 t0 + i * spacing，被前景保留期間延後最多一個保留期（guard + 3 秒）
    for i, t in enumerate(bg):
        assert t0 + i * spacing - 1e-9 <= t <= t0 + i * spacing + guard + 3.0 + 1e-9, (i, t - t0, i * spacing)
    assert bg[-1] - bg[0] >= (n - 1) * spacing - 1e-9
    # 前景保留期間，對帳一個請求都沒拿到額度；前景請求每一次都拿到了
    for t in bg:
        for on, off in holds:
            assert not (on <= t < off), "對帳請求在前景保留期間送出（t=%.3f，保留 %.3f–%.3f）" % (t, on, off)
    fired = [h for h in holds if h[0] <= clock.monotonic()]          # 批次結束後的保留期不會觸發
    assert len(fired) >= 8, len(fired)
    assert len(fg_sent) == len(fired) and all(abs(t - on) < 1e-9 for t, (on, _) in zip(fg_sent, fired)), \
        (len(fg_sent), len(fired))
    assert gate.limiter.granted_by_priority[2] >= n    # 對帳走 BACKGROUND
    assert any(on <= t0 + i * spacing < off for i in range(n) for on, off in holds), \
        "情境裡應該至少有一個對帳請求原本排在保留期間，否則沒測到讓出"


# ============================== 主迴圈：輪詢對齊、每根一個結果 ==============================
def test_run_loop_polls_on_aligned_server_slots_and_judges_each_close():
    first_open = T0 - 300 * BAR
    frames = {"A": make_frame(320, first_open, 61), "B": make_frame(320, first_open, 62)}
    clock = FakeClock(T0 + 3300)                    # 刻意不對齊
    ex = FakeExchange(clock, frames)
    results = []
    feed, gate = build_feed(clock, ex, sorted(frames), on_result=results.append)
    with capture_logs():
        feed.run(duration_s=660)
    feed.close()
    tick_times = [c[1] for c in ex.calls if c[2] == signal_feed.TICKERS_PATH]
    poll_ms = config.TICKERS_POLL_SECONDS * 1000
    assert tick_times and all(t % poll_ms == 0 for t in tick_times), [t % poll_ms for t in tick_times][:5]
    assert len(tick_times) == len(set(tick_times)), "同一個輪詢時刻打了兩次"
    assert [r.bar_close_ms for r in results] == [T0 + BAR, T0 + 2 * BAR], [r.bar_close_ms for r in results]
    for r in results:
        assert r.tickers_server_ts_ms == r.bar_close_ms and not r.degraded, (r.bar_close_ms, r.degraded_reasons)
        assert r.accounting_problems() == []
    assert not gate.limiter.holding()
    snap = feed.latest_tickers()
    assert snap is not None and snap.server_ts_ms == tick_times[-1] and set(snap.prices) == {"A", "B"}


def test_cold_start_bars_are_degraded_not_skipped():
    first_open = T0 - 300 * BAR
    frames = {"A": make_frame(320, first_open, 71), "B": make_frame(320, first_open, 72)}
    clock = FakeClock(T0 - 60_000)
    ex = FakeExchange(clock, frames)
    bg = InlineExecutor(defer={"B"})
    feed, _ = build_feed(clock, ex, sorted(frames), bg_executor=bg)
    with capture_logs():
        feed.start_seeding(sorted(frames), initial=True)
        assert feed.cold_start_active()
        goto(clock, T0 + 200)
        res = feed.judge_bar(T0)
        assert res.unscreened == {signal_feed.UNSCREENED_COLD_START: ["B"]} and res.screened == 1
        assert signal_feed.UNSCREENED_COLD_START in res.degraded_reasons
        f, fn, args, kwargs = bg.deferred.pop()
        fn(*args, **kwargs)                              # 補完 B
        assert not feed.cold_start_active()
        goto(clock, T0 + BAR + 200)
        res2 = feed.judge_bar(T0 + BAR)
    assert res2.screened == 2 and not res2.degraded


def test_price_buffer_lookup_rules():
    buf = PriceBuffer(retention_ms=3 * 3_600_000)
    buf.add_snapshot(T0 + 150, {"A": 2.0, "B": 3.0})
    buf.add_snapshot(T0 + 10_150, {"A": 2.5})
    buf.add_seed("A", [(T0, 1.9)])
    assert buf.price_at("A", T0, 10_000) == (1.9, T0, "seed")                 # 恰好的種子優先
    assert buf.price_at("B", T0, 10_000) == (3.0, T0 + 150, "tickers")
    assert buf.price_at("B", T0 + 10_000, 10_000) == (3.0, T0 + 150, "tickers")   # 下一份沒有 B → 找得到的最近那份
    assert buf.price_at("B", T0 + 20_200, 10_000) is None
    assert buf.price_at("C", T0, 10_000) is None
    buf.prune(T0 + 3 * 3_600_000 + 5000)                                      # 截止 T0+5s：第二份還在
    assert buf.snapshot_count() == 1 and buf.price_at("A", T0, 10_000) is None           # 種子被 prune、快照超出容忍
    assert buf.price_at("A", T0 + 10_000, 10_000) == (2.5, T0 + 10_150, "tickers")
    buf.prune(T0 + 3 * 3_600_000 + 20_000)
    assert buf.price_at("A", T0 + 10_000, 10_000) is None and buf.snapshot_count() == 0
    assert buf.seeded_symbols() == set()


def test_horizon_and_bars_per_hour_are_derived():
    assert klines.bars_per_hour("5M") == klines.HOUR_MS // klines.interval_ms("5M")
    assert klines.bars_per_hour("15M") * 15 == 60 and klines.bars_per_hour("60M") == 1
    clock = FakeClock(T0)
    feed, _ = build_feed(clock, FakeExchange(clock, {}), [])
    assert feed.horizon_ms == 2 * klines.HOUR_MS and feed.bph == BPH


# ============================== AC-9：範圍與約定 ==============================
def test_ac9_a1_params_are_reported_by_execution_params_and_python_m_live():
    params = config.execution_params()
    for name in ("KLINE_INTERVAL", "TICKERS_POLL_SECONDS", "A1_REST_RATE_PER_SECOND", "REST_BAN_COOLDOWN_SECONDS",
                 "SCREEN_RET2H_MARGIN", "SCREEN_NO_BASE_MAX_CANDIDATES", "KLINES_LIMIT", "TARGET_BAR_ATTEMPTS",
                 "RECONCILE_INTERVAL_SECONDS", "FOREGROUND_GUARD_SECONDS", "SIGNAL_FEED_RECORD_DIR",
                 "BAR_FINALIZE_WAIT_SECONDS", "MISSED_CLOSE_TOLERANCE_SECONDS", "FINALITY_PROBE_OFFSETS_SECONDS",
                 "FINALITY_PROBE_KLINES_LIMIT"):
        assert params[name] == getattr(config, name), name
    assert config.SIGNAL_FEED_RECORD_DIR.startswith(config.RUNTIME_DIR)
    r = subprocess.run([sys.executable, "-m", "live"], cwd=REPO_ROOT, capture_output=True, timeout=120)
    out = r.stdout.decode("utf-8", "replace")
    assert r.returncode == 0, r.stderr.decode("utf-8", "replace")[-1500:]
    for name in ("A1_REST_RATE_PER_SECOND", "SCREEN_RET2H_MARGIN", "REST_BAN_COOLDOWN_SECONDS",
                 "BAR_FINALIZE_WAIT_SECONDS", "MISSED_CLOSE_TOLERANCE_SECONDS"):
        assert name in out, name


def test_ac9_module_names_do_not_shadow_stdlib():
    names = [f[:-3] for f in os.listdir(os.path.join(REPO_ROOT, "live")) if f.endswith(".py") and not f.startswith("__")]
    for mod in ("signal_feed", "rest_gate", "klines", "price_buffer", "reconcile"):
        assert mod in names, mod
    clash = [n for n in names if n in sys.stdlib_module_names]
    assert not clash, clash
    assert "signal" not in names


def test_ac9_live_does_not_import_research_or_root_scripts():
    forbidden = ("research", "pionex_backtest", "pionex_dryrun", "pionex_strategy4", "pionex_reversal")
    for rel in A1_FILES:
        if not rel.startswith("live/"):
            continue
        with open(os.path.join(REPO_ROOT, rel), encoding="utf-8") as f:
            src = f.read()
        for line in src.splitlines():
            words = line.strip().split()
            if len(words) >= 2 and words[0] in ("import", "from"):
                mod = words[1].split(".")[0].rstrip(",")
                assert mod not in forbidden, (rel, line.strip())


# ============================== FR-9：命令列與紀錄 ==============================
def test_record_dir_refuses_state_and_output():
    for bad in ("state", "output", os.path.join("output", "sub"), os.path.join("state", "a", "b")):
        try:
            signal_feed.check_record_dir(os.path.join(REPO_ROOT, bad))
        except ValueError:
            continue
        raise AssertionError("應拒絕 %s" % bad)
    ok = signal_feed.check_record_dir(config.SIGNAL_FEED_RECORD_DIR)
    assert ok == os.path.abspath(config.SIGNAL_FEED_RECORD_DIR)
    assert signal_feed.check_record_dir(os.path.join(REPO_ROOT, "outputs_not_really"))


def test_cli_rejects_bad_arguments_before_touching_anything():
    for argv in (["--force-candidates", "-1"], ["--duration", "0"], ["--record", os.path.join(REPO_ROOT, "state")]):
        err = io.StringIO()
        with patched(sys, "stderr", err):
            try:
                signal_feed.main(argv)
            except SystemExit as e:
                assert e.code == 2, (argv, e.code)
            else:
                raise AssertionError("應拒絕 %s" % argv)


def test_jsonl_record_is_utf8_and_nan_free():
    tmp = tempfile.mkdtemp(prefix="a1rec_")
    try:
        rec = signal_feed.JsonlRecorder(os.path.join(tmp, "sub"))
        res = signal_feed.BarResult(bar_open_ms=T0 - BAR, bar_close_ms=T0, interval="5M", min_ret_2h=1.0, threshold=0.5)
        res.approx_ret2h = {"哈基米_USDT_PERP": float("nan"), "BTC_USDT_PERP": np.float64(0.01)}
        res.judged = {"哈基米_USDT_PERP": {"cpos": None, "volr": float("inf")}}
        rec.write("bar", res.to_dict())
        rec.close()
        with open(rec.path, encoding="utf-8") as f:
            line = f.read()
        assert "哈基米_USDT_PERP" in line and "NaN" not in line and "Infinity" not in line
        d = json.loads(line)
        assert d["type"] == "bar" and d["approx_ret2h"]["哈基米_USDT_PERP"] is None
        assert d["approx_ret2h"]["BTC_USDT_PERP"] == 0.01
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ============================== 編碼與時區：cp1252 + strict stderr、TZ=UTC0 ==============================
def _child_log_scenario(tmp):
    """子行程：用 B3 的 logsetup 把日誌導到暫存目錄，跑一根有中文合約名的 K 棒。"""
    from live import logsetup
    log_path = logsetup.setup(log_file=os.path.join(tmp, "a1.log"))
    try:
        first_open = T0 - 300 * BAR
        frames = {"哈基米_USDT_PERP": make_frame(320, first_open, 81, events={299: _min_ret() + 0.03}),
                  "龙虾_USDT_PERP": make_frame(320, first_open, 82, events={299: _min_ret() + 0.03})}
        clock = FakeClock(T0 - 60_000)
        ex = FakeExchange(clock, frames)
        feed, _ = build_feed(clock, ex, sorted(frames))
        feed.start_seeding(sorted(frames), initial=True)
        ex.klines_hook["龙虾_USDT_PERP"] = lambda a, now, rows: [r for r in rows if int(r["time"]) != T0 - BAR]
        goto(clock, T0 + 200)
        res = feed.judge_bar(T0)
        assert res.signals and res.fetch_failed.get(signal_feed.FETCH_TARGET_MISSING) == ["龙虾_USDT_PERP"]
    finally:
        logsetup.teardown()
    return log_path


def test_logs_with_chinese_symbols_survive_cp1252_strict_stderr_and_use_taipei_time():
    tmp = tempfile.mkdtemp(prefix="a1log_")
    try:
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = "cp1252:strict"
        env["TZ"] = "UTC0"
        r = subprocess.run([sys.executable, os.path.abspath(__file__), "--child-log", tmp], cwd=REPO_ROOT,
                           env=env, capture_output=True, timeout=300)
        err = r.stderr.decode("cp1252", "replace")
        assert r.returncode == 0, err[-3000:]
        assert "Logging error" not in err and "Traceback" not in err, err[-3000:]
        with open(os.path.join(tmp, "a1.log"), encoding="utf-8") as f:
            log = f.read()
        assert "原始訊號 哈基米_USDT_PERP" in log and "龙虾_USDT_PERP" in log, log[-2000:]
        escaped = "哈基米".encode("ascii", "backslashreplace").decode("ascii")
        assert escaped in err, "終端機上應該是跳脫序列：%s" % err[-1500:]
        taipei = (datetime.fromtimestamp(T0 / 1000, timezone.utc) + timedelta(hours=8)).strftime("%m-%d %H:%M")
        utc = datetime.fromtimestamp(T0 / 1000, timezone.utc).strftime("%m-%d %H:%M")
        assert taipei != utc
        assert ("K 棒 %s 收盤" % taipei) in log, log[-2000:]
        assert ("K 棒 %s 收盤" % utc) not in log
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ============================== 第 2 輪：BUG-005（不可跳根） ==============================
class FreezeClock(FakeClock):
    """sleep 跨過 freezes 裡某段的起點時，直接醒在它的終點（闔蓋 / VM 暫停 / 行程被卡住）。時刻是伺服器 ms。"""

    def __init__(self, start_ms, freezes=()):
        super().__init__(start_ms)
        self.freezes = sorted(freezes)

    def sleep(self, seconds):
        super().sleep(seconds)
        while self.freezes and self.time_ms() >= self.freezes[0][0]:
            _, end = self.freezes.pop(0)
            self.advance_to(self.mono_of(end))


_LOOP_FIRST_OPEN = T0 - 300 * BAR


def _loop_close(k):
    """主迴圈情境的第 k 根收盤（k = 0 是啟動後第一根）。"""
    return T0 + k * BAR


def _loop_frames():
    up = _min_ret() + 0.03
    frames = {"PUMP_3": make_frame(520, _LOOP_FIRST_OPEN, 91, events={302: up}),     # 真實訊號落在第 3 根（close = T0+3 根）
              "PUMP_8": make_frame(520, _LOOP_FIRST_OPEN, 92, events={307: up})}
    for i in range(4):
        frames["FLAT_%d" % i] = make_frame(520, _LOOP_FIRST_OPEN, 93 + i, base_price=0.4 + i)
    return frames


def _loop_run(n_closes, freezes=(), delays=(), reconciler_kw=None):
    """從第 0 根收盤前 60 秒啟動，跑 run() 到第 n_closes-1 根收盤之後。回傳 (results, ex, feed, gate, cap, rec)。"""
    frames = _loop_frames()
    clock = FreezeClock(_loop_close(0) - 60_000, freezes)
    ex = FakeExchange(clock, frames)
    ex.delays = list(delays)
    results = []
    feed, gate = build_feed(clock, ex, sorted(frames), on_result=results.append)
    rec = None
    if reconciler_kw is not None:
        rec = Reconciler(gate, clock=clock, **reconciler_kw)
        feed.reconciler = rec
        rec.start = lambda stop, sc: None            # 測試裡同步呼叫 run_batch，不開執行緒
    with capture_logs() as cap:
        feed.run(duration_s=(_loop_close(n_closes - 1) + 5_000 - clock.time_ms()) / 1000.0)
    feed.close()
    return results, ex, feed, gate, cap, rec


def _missed_error_lines(cap):
    return [m for m in cap.messages(logging.ERROR) if "missed_close" in m]


def test_bug005_freeze_across_closes_every_close_has_a_result():
    """S1：凍結跨過第 3..6 根收盤 → 12 根都有 BarResult，第 3..6 根是 missed_close（不打 REST、不補判）。"""
    wake = _loop_close(6) + 80_000
    results, ex, feed, gate, cap, _ = _loop_run(12, freezes=[(_loop_close(2) + 132_000, wake)])
    closes = [r.bar_close_ms for r in results]
    assert closes == [_loop_close(k) for k in range(12)], [(c - T0) // BAR for c in closes]
    universe = sorted(_loop_frames())
    for r in results:
        assert r.accounting_problems() == [], r.accounting_problems()
        k = (r.bar_close_ms - T0) // BAR
        if 3 <= k <= 6:
            assert r.degraded_reasons == [signal_feed.DEGRADED_MISSED_CLOSE], (k, r.degraded_reasons)
            assert r.unscreened == {signal_feed.UNSCREENED_MISSED_CLOSE: universe}
            assert r.close_delay_s > config.MISSED_CLOSE_TOLERANCE_SECONDS
            assert abs(r.close_delay_s - (wake - r.bar_close_ms) / 1000.0) < 1.0, (k, r.close_delay_s)
            assert not r.candidates and not r.signals and not r.judged
        else:
            assert not r.degraded, (k, r.degraded_reasons)
    # 醒來後到下一根收盤之前，沒有為了錯過的根打任何 klines（不補判）
    assert not [c for c in ex.calls if c[2] == klines.KLINES_PATH and wake <= c[1] < _loop_close(7)]
    # 日誌：第一根、最後一根各一行 ERROR + 一行停頓摘要；不再出現不指名 K 棒的「跳過幾次輪詢」
    errs = _missed_error_lines(cap)
    assert len(errs) == 3, errs
    assert signal_feed._taipei(_loop_close(3)) in errs[0] and signal_feed._taipei(_loop_close(6)) in errs[1]
    assert "missed_close 4 根" in errs[2] and "停頓" in errs[2], errs[2]
    assert not [m for m in cap.messages(logging.WARNING) if "跳過" in m]


def test_bug005_wake_just_before_close_processes_close_first():
    """S2a：收盤前 2 秒醒來。不可以先跑過期的例行輪詢；那一根照常判定，不是 missed_close。"""
    wake = _loop_close(6) - 2_000
    results, ex, *_ = _loop_run(8, freezes=[(_loop_close(2) + 132_000, wake)],
                                delays=[(wake, signal_feed.TICKERS_PATH, 2.5)])
    by_close = {r.bar_close_ms: r for r in results}
    assert sorted(by_close) == [_loop_close(k) for k in range(8)]
    r6 = by_close[_loop_close(6)]
    assert not r6.degraded and r6.screened > 0, r6.degraded_reasons
    assert r6.close_delay_s <= 0.01, r6.close_delay_s
    assert r6.latency["tickers"] >= 2.5 - 1e-9          # 卡住的是收盤那一次 tickers，延遲如實記錄
    # 醒來到收盤之間，一個 tickers 都沒有送（過期的例行輪詢被略過）
    stale = [t for t, p in ex.starts if p == signal_feed.TICKERS_PATH and wake <= t < _loop_close(6)]
    assert stale == [], [(t - _loop_close(6)) / 1000 for t in stale]
    assert [by_close[_loop_close(k)].degraded_reasons for k in (3, 4, 5)] == [["missed_close"]] * 3


def test_bug005_slow_poll_right_before_close_is_judged_late_within_tolerance():
    """S3：沒有停頓，只有收盤前 10 秒那次例行 tickers 卡了 10.5 秒 → 那根晚 0.5 秒照常判定。"""
    results, ex, feed, gate, cap, _ = _loop_run(8, delays=[(_loop_close(5) - 10_000, signal_feed.TICKERS_PATH, 10.5)])
    by_close = {r.bar_close_ms: r for r in results}
    assert sorted(by_close) == [_loop_close(k) for k in range(8)]
    r5 = by_close[_loop_close(5)]
    assert not r5.degraded, r5.degraded_reasons
    assert abs(r5.close_delay_s - 0.5) < 1e-6 and abs(r5.latency["settle"] - 0.5) < 1e-6, (r5.close_delay_s, r5.latency)
    assert any("花了 10.5 秒" in m for m in cap.messages(logging.WARNING))
    assert not _missed_error_lines(cap)


def test_bug005_hang_beyond_tolerance_gives_one_named_missed_close():
    """收盤前 10 秒那次輪詢卡了 16 秒 → 收盤處理晚 6 秒 > 5 秒容忍 → 那根 missed_close，一行 ERROR 指名。"""
    results, ex, feed, gate, cap, _ = _loop_run(8, delays=[(_loop_close(5) - 10_000, signal_feed.TICKERS_PATH, 16.0)])
    by_close = {r.bar_close_ms: r for r in results}
    assert sorted(by_close) == [_loop_close(k) for k in range(8)]
    r5 = by_close[_loop_close(5)]
    assert r5.degraded_reasons == ["missed_close"] and abs(r5.close_delay_s - 6.0) < 1e-6
    errs = _missed_error_lines(cap)
    assert len(errs) == 1 and signal_feed._taipei(_loop_close(5)) in errs[0] and "6.0 秒" in errs[0], errs
    assert not by_close[_loop_close(6)].degraded


def test_bug005_long_stall_is_one_summary_not_one_error_per_bar():
    """睡一整晚（12 小時 = 144 根）：每一根都有 BarResult，但日誌只有頭、尾、摘要三行 ERROR。"""
    wake = _loop_close(2) + 12 * 3_600_000 + 200_000
    results, ex, feed, gate, cap, _ = _loop_run(2 + 144 + 2, freezes=[(_loop_close(2) + 60_000, wake)])
    closes = [r.bar_close_ms for r in results]
    assert closes == [_loop_close(k) for k in range(2 + 144 + 2)], (len(closes), 2 + 144 + 2)
    missed = [r for r in results if "missed_close" in r.degraded_reasons]
    assert len(missed) == 144
    errs = _missed_error_lines(cap)
    assert len(errs) == 3, (len(errs), errs[:5])
    assert "missed_close 144 根" in errs[2] and "12h" in errs[2], errs[2]
    assert len(cap.messages(logging.ERROR)) <= 4, cap.messages(logging.ERROR)


def test_bug005_reconcile_counts_expected_missing_and_undetermined():
    """FR-10：凍結跨過真實訊號那根 → 那根是 missed_close、訊號列為「未判定」（不是漏失）；
    另外把一根的紀錄整個拿掉 → 應有根數抓得到它，記 ERROR 並列出。"""
    wake = _loop_close(6) + 80_000
    results, ex, feed, gate, _, rec = _loop_run(12, freezes=[(_loop_close(2) + 132_000, wake)],
                                                reconciler_kw=_RECON_KW)
    assert rec._expect_from == _loop_close(0), "run() 應告訴對帳從第一根該處理的收盤算起"
    window_end = _loop_close(11)
    drop = _loop_close(9)
    rec._records = [r for r in rec._records if r.close_ms != drop]      # 模擬一根完全沒有紀錄
    with capture_logs() as cap:
        rr = rec.run_batch(window_end)
    expected = [c for c in range(window_end - 3_600_000 + BAR, window_end + 1, BAR) if c >= _loop_close(0)]
    assert rr.bars_expected == len(expected) == 12
    assert rr.bars_missing == [drop]
    assert rr.bars_missed_close == [_loop_close(k) for k in (3, 4, 5, 6)]
    und = {(u["symbol"], u["bar_close_ms"], u["why"]) for u in rr.undetermined}
    assert ("PUMP_3", _loop_close(3), "missed_close") in und, und     # 真實訊號那根
    assert all(u["true_ret2h"] >= _min_ret() for u in rr.undetermined)
    assert rr.misses == [] and rr.misses_on_degraded == 0
    errs = cap.messages(logging.ERROR)
    assert any("完全沒有紀錄" in m and signal_feed._taipei(drop) in m for m in errs), errs
    assert any("未判定" in m for m in cap.messages(logging.WARNING))


def test_bug005_reconcile_sees_missed_bars_recorded_after_batch_started():
    """批次開始時那幾根的 missed_close 紀錄還沒進來（兩個執行緒同時醒來），比對時用的是做完取數後的紀錄。"""
    run = _scenario_run(reconciler_kw=_RECON_KW)
    rec, feed = run["reconciler"], run["feed"]
    late = _close_of(395)
    rec._records = [r for r in rec._records if r.close_ms != late]
    clock = run["clock"]
    clock.call_at(clock.monotonic() + 30.0, lambda: rec.add_bar(feed.missed_bar(late, 900.0)))
    with capture_logs() as cap:
        rr = rec.run_batch(_close_of(_SCEN_HI))
    assert rr.bars_missing == [] and rr.bars_missed_close == [late], (rr.bars_missing, rr.bars_missed_close)
    assert not [m for m in cap.messages(logging.ERROR) if "完全沒有紀錄" in m]


def test_reconcile_respreads_remaining_requests_after_a_stall():
    """對帳做到一半行程停了 30 分鐘：剩下的 symbol 重新攤平到剩餘時間，不集中補打。"""
    run = _scenario_run(reconciler_kw=_RECON_KW)
    rec, clock, ex = run["reconciler"], run["clock"], run["ex"]
    n = len(run["frames"])
    step = rec.interval_s * rec.spread_fraction / n
    t0 = clock.monotonic()

    def freeze():
        clock._mono += 1800.0
    clock.call_at(t0 + 2.5 * step, freeze)
    before = len(ex.calls)
    with capture_logs():
        rr = rec.run_batch(_close_of(_SCEN_HI))
    sent = [c[0] for c in ex.calls[before:] if c[2] == klines.KLINES_PATH]
    assert len(sent) == n and rr.respread == 1, (len(sent), rr.respread)
    wake = t0 + 2.5 * step + 1800.0
    after = [t for t in sent if t >= wake]
    left = len(after)
    new_step = (t0 + rec.interval_s * rec.spread_fraction - wake) / left
    gaps = [b - a for a, b in zip(after, after[1:])]
    assert gaps and all(abs(g - new_step) < 1e-6 for g in gaps), (gaps, new_step)
    assert after[-1] <= t0 + rec.interval_s * rec.spread_fraction + 1e-6


# ============================== 第 2 輪：K 棒定稿等待 ==============================
def test_finalize_wait_klines_are_sent_only_after_close_plus_wait():
    res, ex, clock, seen, frame, _ = _single_candidate(None)
    wait_ms = int(config.BAR_FINALIZE_WAIT_SECONDS * 1000)
    kl = [t for t, p in ex.starts if p == klines.KLINES_PATH and t >= T0]
    assert kl and min(kl) >= T0 + wait_ms, [(t - T0) / 1000 for t in kl]
    lat = res.latency
    assert abs(lat["finalize_wait"] - (config.BAR_FINALIZE_WAIT_SECONDS - lat["settle"] - lat["tickers"]
                                        - lat["screen"])) < 1e-6, lat
    assert lat["klines"] < lat["finalize_wait"] + 1.0            # klines 取數本身與等待分開記
    ev = res.judged["S"]
    assert ev["fetched_after_close_s"] >= config.BAR_FINALIZE_WAIT_SECONDS
    assert ev["has_next_bar"] is True                             # 取數成功的也記（裁決 1 第 4 點）
    for key in ("settle", "tickers", "screen", "finalize_wait", "klines", "signal", "total"):
        assert key in lat, key


def test_finalize_wait_is_skipped_without_candidates_or_when_already_late():
    first_open = T0 - 300 * BAR
    frames = {"A": make_frame(320, first_open, 5), "B": make_frame(320, first_open, 6)}
    clock = FakeClock(T0 - 60_000)
    ex = FakeExchange(clock, frames)
    feed, _ = build_feed(clock, ex, sorted(frames))
    with capture_logs():
        feed.start_seeding(sorted(frames), initial=True)
        goto(clock, T0 + 200)
        res = feed.judge_bar(T0)
    assert not res.candidates and res.latency["finalize_wait"] == 0.0
    # 收盤後 3 秒才開始處理（仍在容忍內）：已經過了定稿等待，不再多等
    feed2, _ = build_feed(clock, ex, sorted(frames), force_candidates=1)
    with capture_logs():
        feed2.start_seeding(sorted(frames), initial=True)
        goto(clock, T0 + BAR + 3000)
        res2 = feed2.judge_bar(T0 + BAR)
    assert res2.candidates and res2.latency["finalize_wait"] == 0.0 and abs(res2.close_delay_s - 3.0) < 1e-6


# ============================== 第 2 輪：定稿量測（--finality-probe） ==============================
def test_finality_probe_refetches_at_offsets_with_low_priority():
    first_open = T0 - 300 * BAR
    up = _min_ret() + 0.03
    frames = {"P": make_frame(320, first_open, 7, events={299: up}), "Q": make_frame(320, first_open, 8, events={299: up})}
    clock = FakeClock(T0 - 60_000)
    ex = FakeExchange(clock, frames)
    feed, gate = build_feed(clock, ex, sorted(frames))
    probes = []
    prober = signal_feed.FinalityProber(gate, clock=clock, on_probe=probes.append)
    with capture_logs() as cap:
        feed.start_seeding(sorted(frames), initial=True)
        goto(clock, T0 + 200)
        res = feed.judge_bar(T0)
        assert sorted(res.judged) == ["P", "Q"]
        prober.schedule(res)
        assert prober.pending() == 2 * len(config.FINALITY_PROBE_OFFSETS_SECONDS)
        assert prober.run_due() == 0                                  # 還沒到收盤後第一個時點
        normal_before = gate.limiter.granted_by_priority[1]
        offsets = config.FINALITY_PROBE_OFFSETS_SECONDS
        # 第一個時點：前景保留期間到期 → 不可以送，等保留解除才送（不擋前景取數）
        gate.limiter.hold_background()
        clock.call_at(clock.mono_of(T0 + offsets[0] * 1000) + 1.5, gate.limiter.release_background)
        goto(clock, T0 + offsets[0] * 1000)
        assert prober.run_due() == 2
        first = probes[-2:]
        assert all(p["fetched_after_close_s"] >= offsets[0] + 1.5 - 1e-9 for p in first), first
        assert all(p["same_as_judged"] is True and p["has_next_bar"] is True for p in first), first
        # 第一次重抓之後派網改寫了 P 那一根 → 下一個時點看得到
        df = ex.frames["P"]
        df.loc[df["time"] == T0 - BAR, "volume"] = df.loc[df["time"] == T0 - BAR, "volume"] * 1.0004
        for off in offsets[1:]:
            goto(clock, T0 + off * 1000)
            assert prober.run_due() == 2
    assert gate.limiter.granted_by_priority[1] - normal_before == 2 * len(offsets)   # 全部走 PRIORITY_NORMAL
    assert len(probes) == 2 * len(offsets) and prober.probes == len(probes)
    by = {(p["symbol"], p["offset_s"]): p for p in probes}
    assert by[("P", offsets[1])]["same_as_judged"] is False and by[("Q", offsets[1])]["same_as_judged"] is True
    assert by[("P", offsets[1])]["judged_target_bar"] == res.judged["P"]["target_bar"]
    assert prober.changed == len(offsets) - 1
    assert any("定稿量測" in m and "P" in m for m in cap.messages(logging.INFO))
    json.dumps(by[("P", offsets[1])])                                   # 寫得進 jsonl


def test_cli_finality_probe_requires_record():
    err = io.StringIO()
    with patched(sys, "stderr", err):
        try:
            signal_feed.main(["--finality-probe", "--duration", "10"])
        except SystemExit as e:
            assert e.code == 2
        else:
            raise AssertionError("--finality-probe 沒有 --record 應該被拒絕")
    assert "record" in err.getvalue()


# ============================== 第 2 輪：定稿比對的字串轉浮點一致 ==============================
def _ulp_string():
    """找一個 float() 與 pd.to_numeric 轉出來差 1 ULP 的字串（DQA 建議 1 的情境）。"""
    rng = random.Random(5)
    for _ in range(100000):
        s = "%d.%08d" % (rng.randrange(10 ** 8, 10 ** 9), rng.randrange(10 ** 8))
        if float(s) != float(pd.to_numeric(pd.Series([s])).iloc[0]):
            return s
    raise AssertionError("找不到會差 1 ULP 的字串")


def test_ohlcv_compare_uses_the_same_parser_as_judgement():
    s = _ulp_string()
    first_open = T0 - 300 * BAR
    frame = make_frame(320, first_open, 9)
    rows = [{"time": int(t), "open": repr(o), "high": repr(h), "low": repr(lo), "close": repr(c), "volume": repr(v)}
            for t, o, h, lo, c, v in frame.itertuples(index=False) if t < T0]
    rows.reverse()
    rows[0]["volume"] = s                                              # 目標 K 棒的成交量是那個字串
    ev = signal_feed.evaluate_target(rows, T0 - BAR, BAR, BPH, config.strategy_params())
    assert ev["target_bar"][4] != float(s), "這個字串應該讓 float() 與 pd.to_numeric 不同（否則測不到）"
    assert klines.ohlcv_by_open(rows[:30])[T0 - BAR] == {tuple(ev["target_bar"])}   # 周圍的列不同也一樣
    from live.reconcile import BarRecord
    recon = Reconciler(None, interval_s=3600)
    record = BarRecord(close_ms=T0, degraded=False, universe=frozenset({"S"}), candidates=frozenset({"S"}),
                       approx={"S": 0.0}, unscreened={}, target_bars={"S": tuple(ev["target_bar"])})
    from live.reconcile import ReconcileResult
    rr = ReconcileResult(T0 - 3_600_000, T0, _min_ret())
    recon._compare("S", recon._digest(rows[:100], T0 - 3_600_000, T0), [record], [], _min_ret(), rr, [])
    assert rr.ohlcv_checked == 1 and rr.ohlcv_mismatches == [], rr.ohlcv_mismatches


def test_socket_cage_blocks_connections():
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
    if len(sys.argv) == 3 and sys.argv[1] == "--child-log":
        with socket_cage():
            _child_log_scenario(sys.argv[2])
        sys.exit(0)
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
