# -*- coding: utf-8 -*-
"""
A5（live.s5_feed，策略五資料層）驗收測試。全程離線（socket 籠子自證）、假時鐘、不真的 sleep。

PRD §7 AC-1 的 13 項（測試函式名 test_ac1_NN*，NN = 項次）：

   1   粗篩門檻來自注入的 params（不寫死）；呼叫當下推導
   2   第 60 分（H+1:00 收盤）歸 H；同一刻 H+1 的粗篩也做了
   3   first_per_hour：同一小時第二個成立的根不送，下一小時可以再送
   4   補送：取數失敗 / missed 的那一分鐘，下一分鐘補送、K 棒時刻用原本那根；同一 (symbol, H) 只送一次
   5   不跨小時回判：整個小時停頓後，下一小時不送上一小時的訊號
   6   冷啟動：沒有 H−1:00 樣本記未粗篩，有樣本的那一分鐘起才追蹤
   7   停止追蹤：精確 ① 不成立 / 前一小時不完整 → 本小時不再取數（用請求數驗證）
   8   MARKET_INVALID_SYMBOL：A5、背景對帳、補種子三處都不重試、同一天一行 INFO、不算 degraded / 失敗
   9   SignalFeed._deliver：add_bar 拋例外時 on_result 仍恰好呼叫一次，並記一行 ERROR
  10   例外隔離：某幣取數 / evaluate 拋例外，同一分鐘其他幣照送；on_result 拋例外不影響送訊號
  11   RestBanned：該分鐘記 degraded（banned）、不在冷卻中硬等，冷卻結束後補送
  12   a_channel 接線：factory 注入假的 A1 / A3，真的 A5 送進 submit_signal(strategy="s5")；關閉順序 A5 → A1 → A3
  13   ApiError 回溯相容；code 由 result=false（與帶 code 的 4xx body）帶入

其他：

  FR-6   A1 持有 foreground_hold 時先讓，最多 S5_YIELD_MAX_SECONDS
  NFR-1  import 只有 live / strategy / 標準庫；模組名不撞標準庫；S5_* 列在 execution_params()
  NFR-2  s5_feed.py 與本檔的程式碼裡沒有 MIN_RISE_FROM_OPEN（與 margin、粗篩門檻）的數值（tokenize 掃描）
  NFR-4  每分鐘請求數 = 仍在追蹤的候選數（沒有重試時）

AC-2 的 mutant：(i) 第 60 分歸 H+1 → 第 2 項等；(ii) 補送用目標 K 棒時刻 → 第 4、11 項；
(iii) _deliver 拿掉 try → 第 9 項，MARKET_INVALID 重試 → 第 8 項（A5 / 對帳 / 補種子各一個）。

測試裡的所有門檻都由 s5_signal.DEFAULT_PARAMS 與 config 推導，不寫任何策略參數的數值。

    python tests/test_s5_feed.py
"""

import ast
import collections
import io
import json
import logging
import math
import os
import sys
import threading
import tokenize
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import pandas as pd  # noqa: E402

import live.pionex_api as lh  # noqa: E402
import test_signal_feed as tsf  # noqa: E402
from live import a_channel, config, klines, s5_feed  # noqa: E402
from live.price_buffer import PriceBuffer  # noqa: E402
from live.rest_gate import RestGate, RestLimiter, ServerClock  # noqa: E402
from live.signal_feed import TickersSnapshot, _taipei  # noqa: E402
from strategy import s5_signal  # noqa: E402

H0 = tsf.T0                               # 小時邊界（UTC 07:00 = 台北 15:00）
MIN = s5_feed.MINUTE_MS
HOUR = s5_feed.HOUR_MS
P = s5_signal.DEFAULT_PARAMS
M = P["MIN_RISE_FROM_OPEN"]
G = config.S5_SCREEN_RISE_MARGIN
R_UP = M * 1.5                            # ① 精確成立、粗篩也過
R_LOW = M - G / 2                         # 粗篩過（>= M − G），精確 ① 不成立（< M）
WIG = 1e-4                                # 每根 high / low 相對 open / close 的影線
BREAK_EPS = 1e-3                          # 爆量那根過前高的幅度
WAIT_MS = int(round(config.BAR_FINALIZE_WAIT_SECONDS * 1000))
INVALID = lh.MARKET_INVALID_SYMBOL


# ============================== 假資料 ==============================
def make_1m(spec, first_hour=-3, last_hour=2, base=1.0, vol=100.0):
    """1M K 棒（time 升冪、無缺漏）。spec: 小時索引 h（H0 + h 小時）→ (r, spike)：
      r      本小時 收盤 ÷ 開盤 − 1（逐分線性上漲）；下一小時的 ① 就是它
      spike  第幾分（1～60）爆量並過前高（量 = MIN_VOL_MULT × 前一小時量 × 1.01，收盤 > 前一小時最高）；None = 沒有
    沒有 spike 的小時量固定，volx = k/60 < 1，② 永遠不成立；spike 之後量與價都維持，所以本小時三條件會一直成立
    （first_per_hour 只取第一根）。r = 0 的小時收盤 = 開盤 < 前一小時最高，③ 不成立。"""
    rows = []
    level, prev_high, prev_vol = base, None, None
    for h in range(first_hour, last_hour + 1):
        r, spike = spec.get(h, (0.0, None))
        open_h, jump, hi, hv, o = level, 1.0, 0.0, 0.0, level
        for k in range(1, 61):
            t = H0 + h * HOUR + (k - 1) * MIN
            base_c = open_h * (1 + r * k / 60)
            v = vol
            if spike is not None and k == spike and prev_high is not None:
                jump = max(1.0, prev_high * (1 + P["MIN_ABOVE_PH"]) * (1 + BREAK_EPS) / base_c)
                v = P["MIN_VOL_MULT"] * prev_vol * 1.01
            c = base_c * jump
            high = max(o, c) * (1 + WIG)
            rows.append((t, o, high, min(o, c) * (1 - WIG), c, v))
            hi, hv, o = max(hi, high), hv + v, c
        prev_high, prev_vol, level = hi, hv, o
    return pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume"])


def kline_rows(frame, now, limit=None):
    """派網格式的 1M klines：time <= now 的根、新到舊、字串數值；未收完那根（time + 1 分 > now）數值刻意放大。"""
    upto = frame[frame["time"] <= now]
    if limit is not None:
        upto = upto.tail(int(limit))
    rows = []
    for t, o, h, lo, c, v in zip(upto["time"].tolist(), upto["open"].tolist(), upto["high"].tolist(),
                                 upto["low"].tolist(), upto["close"].tolist(), upto["volume"].tolist()):
        forming = t + MIN > now
        c2 = c * (3.0 if forming else 1.0)
        v2 = v * (50.0 if forming else 1.0)
        rows.append({"time": int(t), "open": repr(float(o)), "close": repr(float(c2)),
                     "high": repr(float(max(h, c2))), "low": repr(float(lo)), "volume": repr(float(v2))})
    rows.reverse()
    return rows


def hour_seeds(frame):
    """整點的價格樣本 (H:00, H−1 小時最後一根的收盤)——等同 A1 的收盤 tickers 樣本。"""
    return [(int(t) + MIN, float(c)) for t, c in zip(frame["time"].tolist(), frame["close"].tolist())
            if (int(t) + MIN) % HOUR == 0]


class FakeExchange1M:
    """假派網（只有 1M klines）。hooks[sym](attempt, now, rows) 可改寫回應或拋例外（attempt 從 1 起、先計數）。"""

    def __init__(self, clock, frames):
        self.clock = clock
        self.frames = {s: df.reset_index(drop=True) for s, df in frames.items()}
        self.calls = []                       # (伺服器時刻, symbol)
        self.attempts = collections.Counter()
        self.hooks = {}
        self._lock = threading.Lock()

    def api_get(self, path, params=None, retries=3, timeout=20):
        assert path == klines.KLINES_PATH, path
        assert params["interval"] == config.S5_KLINE_INTERVAL, params
        assert params["limit"] == config.S5_KLINES_LIMIT, params
        assert retries == 1, retries          # 閘門自己不重試（429 走封鎖冷卻）
        now = self.clock.time_ms()
        sym = params["symbol"]
        with self._lock:
            self.calls.append((now, sym))
            self.attempts[sym] += 1
            attempt = self.attempts[sym]
        rows = kline_rows(self.frames[sym], now, params["limit"])
        hook = self.hooks.get(sym)
        if hook is not None:
            rows = hook(attempt, now, rows)
        return {"result": True, "data": {"klines": rows}, "timestamp": now}

    def count(self, sym, lo, hi):
        return sum(1 for t, s in self.calls if s == sym and lo <= t < hi)


def make_gate(clock, api_get):
    return RestGate(RestLimiter(config.A1_REST_RATE_PER_SECOND, config.REST_BAN_COOLDOWN_SECONDS, clock=clock),
                    ServerClock(clock), clock=clock, api_get=api_get)


def invalid_error(*args):
    raise lh.ApiError("%s: result=false code=%s message=invalid symbol" % (klines.KLINES_PATH, INVALID), code=INVALID)


def run_s5(frames, until_close, *, universe=None, clock=None, drop=(), extra_seeds=None, hooks=None, params=None,
           tickers_fn=None, on_result=None, setup=None, executor=None):
    """在假時鐘上跑 S5Feed.run()，從 H0 − 30 秒跑到收盤 until_close 處理完。第一個處理的收盤是 H0（H−1 的第 60 分）。

    價格緩衝放每個整點的樣本（drop 裡的 (symbol, 時刻) 不放），extra_seeds: symbol → [(時刻, 價格)]。
    """
    clock = clock or tsf.FakeClock(H0 - 30_000)
    ex = FakeExchange1M(clock, frames)
    ex.hooks.update(hooks or {})
    gate = make_gate(clock, ex.api_get)
    buf = PriceBuffer(100 * HOUR)
    dropped = set(drop)
    for sym, df in frames.items():
        buf.add_seed(sym, [(t, p) for t, p in hour_seeds(df) if (sym, t) not in dropped])
    for sym, pts in (extra_seeds or {}).items():
        buf.add_seed(sym, pts)
    submits, results = [], []
    syms = sorted(frames) if universe is None else list(universe)

    def submit(**kw):
        submits.append(dict(kw, at=clock.time_ms()))

    def on_res(res):
        results.append(res)
        if on_result is not None:
            on_result(res)
    s5 = s5_feed.S5Feed(gate=gate, buffer=buf, universe_fn=lambda: list(syms), submit_fn=submit, clock=clock,
                        params=params, tickers_fn=tickers_fn, executor=executor or tsf.InlineExecutor(),
                        on_result=on_res)
    ns = types.SimpleNamespace(submits=submits, results=results, ex=ex, s5=s5, clock=clock, buf=buf, gate=gate,
                               cap=None, by_close=None)
    if setup is not None:
        setup(ns)
    with tsf.capture_logs() as cap:
        s5.run(duration_s=(until_close + WAIT_MS + 1000 - clock.time_ms()) / 1000.0)
        assert s5.close()
    ns.cap = cap
    ns.by_close = {r.bar_close_ms: r for r in results}
    return ns


def hour_summary(run, hour_ms):
    head = "策略五 小時 " + _taipei(hour_ms)
    lines = [m for m in run.cap.messages(logging.INFO) if m.startswith(head)]
    assert len(lines) == 1, lines
    return lines[0]


def sent(run):
    return [(s["symbol"], s["bar_open_ms"], s["bar_close_ms"]) for s in run.submits]


# ============================== AC-1 第 1 項：粗篩門檻來自 params ==============================
def test_ac1_01a_screen_threshold_is_derived_at_call_time():
    params2 = dict(P, MIN_RISE_FROM_OPEN=M * 2)
    assert math.isclose(s5_feed.screen_threshold(), M - G)
    assert math.isclose(s5_feed.screen_threshold(P), M - G)
    assert math.isclose(s5_feed.screen_threshold(params2), M * 2 - G)
    with tsf.patched(s5_signal, "DEFAULT_PARAMS", params2):
        assert math.isclose(s5_feed.screen_threshold(), M * 2 - G)      # 呼叫當下讀，不快取
    with tsf.patched(config, "S5_SCREEN_RISE_MARGIN", G * 2):
        assert math.isclose(s5_feed.screen_threshold(), M - G * 2)
    assert math.isclose(s5_feed.screen_threshold(), M - G)


def test_ac1_01b_screen_result_follows_injected_params():
    frames = {"EDGE": make_1m({-1: (M * 2 - G / 2, None)}),     # 兩種門檻下都是候選；params2 下精確 ① 不成立
              "MID": make_1m({-1: (M * 1.5 - G, None)})}         # 只有預設門檻下是候選
    base = run_s5(frames, H0 + MIN)
    r1 = base.by_close[H0 + MIN]
    assert r1.candidates == ["EDGE", "MID"], r1.candidates
    assert r1.stopped == {} and r1.fetches == 2, (r1.stopped, r1.fetches)
    assert "門檻 %.4f" % (M - G) in hour_summary(base, H0)

    params2 = dict(P, MIN_RISE_FROM_OPEN=M * 2)
    alt = run_s5(frames, H0 + MIN, params=params2)
    r1 = alt.by_close[H0 + MIN]
    assert r1.candidates == ["EDGE"], r1.candidates
    assert r1.stopped == {"EDGE": s5_feed.STOP_RISE_BELOW}, r1.stopped
    assert alt.ex.attempts["MID"] == 0 and alt.ex.attempts["EDGE"] == 1, alt.ex.attempts
    assert "門檻 %.4f" % (M * 2 - G) in hour_summary(alt, H0)


# ============================== AC-1 第 2 項：第 60 分歸 H，同一刻做 H+1 的粗篩 ==============================
def test_ac1_02_minute_60_belongs_to_h_and_next_hour_is_screened_at_the_same_tick():
    frames = {"SIG60": make_1m({-1: (R_UP, None), 0: (0.0, 60)}),
              "NEXT": make_1m({0: (R_UP, None)})}
    queries = []

    def setup(ns):
        orig = ns.buf.price_at

        def spy(sym, t, tol):
            queries.append((sym, int(t), ns.clock.time_ms()))
            return orig(sym, t, tol)
        ns.buf.price_at = spy
    run = run_s5(frames, H0 + HOUR + 2 * MIN, setup=setup)

    assert sent(run) == [("SIG60", H0 + 59 * MIN, H0 + HOUR)], sent(run)
    s = run.submits[0]
    assert s["strategy"] == "s5"
    assert H0 + HOUR < s["at"] < H0 + HOUR + MIN, s["at"]
    feats = s["features"]
    assert feats["minute"] == 60, feats
    assert {"minute", "rise", "volx", "above", "branch_code", "late_s"} <= set(feats), feats
    for k, v in feats.items():
        assert type(v) in (int, float, str, bool), (k, type(v))
    assert type(s["signal_price"]) is float and type(s["bar_open_ms"]) is int and type(s["bar_close_ms"]) is int

    r60 = run.by_close[H0 + HOUR]
    assert (r60.hour_ms, r60.minute) == (H0, 60), (r60.hour_ms, r60.minute)
    assert [(x.symbol, x.late) for x in r60.signals] == [("SIG60", False)]
    # H+1 的粗篩在第 60 分那一刻（H+1:00 + 定稿等待）就做了，不是等到 H+1 第 1 分
    q = [at for sym, t, at in queries if sym == "NEXT" and t == H0 + HOUR]
    assert q and min(q) < H0 + HOUR + MIN, q
    r1 = run.by_close[H0 + HOUR + MIN]
    assert (r1.hour_ms, r1.minute, r1.candidates) == (H0 + HOUR, 1, ["NEXT"]), (r1.hour_ms, r1.minute, r1.candidates)
    assert "粗篩於第 0 分" in hour_summary(run, H0 + HOUR)


# ============================== AC-1 第 3 項：first_per_hour ==============================
def test_ac1_03_first_per_hour_then_next_hour_can_send_again():
    frame = make_1m({-1: (R_UP, None), 0: (R_UP, 10), 1: (R_UP, 20)})
    ev = s5_signal.evaluate(frame, MIN, P)
    h0 = ev[ev["hid"] == H0 // HOUR]
    all_ok = h0["rise_ok"] & h0["burst_ok"] & h0["break_ok"]
    assert int(all_ok.sum()) > 1 and int((h0["signal"] == -1).sum()) == 1, "資料要讓同一小時有第二根成立"

    run = run_s5({"TWICE": frame}, H0 + HOUR + 25 * MIN)
    assert sent(run) == [("TWICE", H0 + 9 * MIN, H0 + 10 * MIN),
                         ("TWICE", H0 + HOUR + 19 * MIN, H0 + HOUR + 20 * MIN)], sent(run)
    assert run.by_close[H0 + 10 * MIN].stopped == {"TWICE": s5_feed.STOP_SENT}
    # 送出後本小時不再取數：第 1～10 分、下一小時第 1～20 分
    assert run.ex.count("TWICE", H0 + MIN, H0 + HOUR + MIN) == 10
    assert run.ex.count("TWICE", H0 + HOUR + MIN, H0 + 2 * HOUR + MIN) == 20
    st = run.s5.stats()
    assert (st["signals"], st["late_submits"], st["requests"], st["http_429"]) == (2, 0, 30, 0), st
    assert st["minutes"] == 86 and st["degraded"] == 0 and st["missed"] == 0, st
    assert st["latency_p50_s"] is not None and st["latency_max_s"] < WAIT_MS / 1000.0 + 1, st
    assert st["worker_alive"] is False and st["worker_crashed"] is False


# ============================== AC-1 第 4 項：補送 ==============================
def test_ac1_04a_fetch_failure_minute_is_sent_late_next_minute_with_original_bar():
    frames = {"LATE": make_1m({-1: (R_UP, None), 0: (0.0, 5)})}

    def hook(attempt, now, rows):
        if H0 + 5 * MIN <= now < H0 + 6 * MIN:
            raise lh.ApiError("boom")
        return rows
    run = run_s5(frames, H0 + 8 * MIN, hooks={"LATE": hook})
    r5 = run.by_close[H0 + 5 * MIN]
    assert r5.fetches == config.TARGET_BAR_ATTEMPTS and r5.failed == {"api_error": ["LATE"]}, (r5.fetches, r5.failed)
    assert r5.degraded_reasons == ["api_error"] and r5.signals == []
    assert sent(run) == [("LATE", H0 + 4 * MIN, H0 + 5 * MIN)], sent(run)
    r6 = run.by_close[H0 + 6 * MIN]
    assert [(x.symbol, x.late) for x in r6.signals] == [("LATE", True)]
    assert run.submits[0]["features"]["minute"] == 5
    assert run.submits[0]["features"]["late_s"] > 60
    line = [m for m in run.cap.messages(logging.INFO) if m.startswith("原始訊號 LATE")]
    assert len(line) == 1 and ("補送、K 棒 " + _taipei(H0 + 5 * MIN)) in line[0], line
    assert run.ex.count("LATE", H0 + 7 * MIN, H0 + 2 * HOUR) == 0      # 同一 (symbol, H) 只送一次
    assert run.s5.stats()["late_submits"] == 1


def test_ac1_04b_missed_minute_is_sent_late_next_minute_with_original_bar():
    frames = {"LATE": make_1m({-1: (R_UP, None), 0: (0.0, 5)})}
    clock = tsf.FreezeClock(H0 - 30_000, [(H0 + 5 * MIN + 1000, H0 + 6 * MIN + 3000)])
    run = run_s5(frames, H0 + 7 * MIN, clock=clock)
    r5 = run.by_close[H0 + 5 * MIN]
    assert r5.degraded_reasons == ["missed"] and r5.fetches == 0 and r5.candidates == ["LATE"], r5
    errs = [m for m in run.cap.messages(logging.ERROR) if "missed_close" in m]
    assert len(errs) == 1 and "missed_close 1 根" in errs[0], errs
    assert sent(run) == [("LATE", H0 + 4 * MIN, H0 + 5 * MIN)], sent(run)
    assert [(x.symbol, x.late) for x in run.by_close[H0 + 6 * MIN].signals] == [("LATE", True)]
    assert run.ex.count("LATE", H0 + 5 * MIN, H0 + 6 * MIN) == 0


# ============================== AC-1 第 5 項：不跨小時回判 ==============================
def test_ac1_05_whole_hour_stall_never_sends_previous_hour_signal():
    frame = make_1m({-1: (R_UP, None), 0: (R_UP, 30), 1: (0.0, None)})
    j = s5_feed.judge_rows(kline_rows(frame, H0 + 30 * MIN), H0 + 30 * MIN)
    assert j.signal is not None and j.signal["bar_open_ms"] == H0 + 29 * MIN, "資料要讓 H 的第 30 分有訊號"

    clock = tsf.FreezeClock(H0 - 30_000, [(H0 + 4 * MIN + 3000, H0 + HOUR + 10_000)])
    run = run_s5({"STALL": frame}, H0 + HOUR + 3 * MIN, clock=clock)
    assert run.submits == [], run.submits
    missed = [r.bar_close_ms for r in run.results if "missed" in r.degraded_reasons]
    assert missed == [H0 + m * MIN for m in range(5, 61)], missed
    errs = [m for m in run.cap.messages(logging.ERROR) if "missed_close" in m]
    assert len(errs) == 1 and "missed_close 56 根" in errs[0], errs
    assert run.ex.count("STALL", H0, H0 + HOUR) == 4
    assert run.ex.count("STALL", H0 + HOUR, H0 + 2 * HOUR) == 3
    assert "粗篩於第 1 分" in hour_summary(run, H0 + HOUR)
    assert run.s5.stats()["missed"] == 56


# ============================== AC-1 第 6 項：冷啟動 ==============================
def test_ac1_06_cold_start_unscreened_until_sample_arrives():
    frame = make_1m({-1: (R_UP, None), 0: (0.0, 8)})
    price = float(frame.loc[frame["time"] == H0 - HOUR - MIN, "close"].iloc[0])
    snap = TickersSnapshot(server_ts_ms=H0, slot_ms=H0, local_recv_ms=H0, prices={"COLD": 1.0, "BADT": 1.0},
                           row_times={"COLD": H0}, invalid=("BADT",))

    def setup(ns):
        ns.clock.call_at(ns.clock.mono_of(H0 + 2 * MIN + 30_000),
                         lambda: ns.buf.add_seed("COLD", [(H0 - HOUR, price)]))
    # 標的池刻意不照字母排，摘要的「未粗篩」要自己排序
    run = run_s5({"COLD": frame}, H0 + 8 * MIN, universe=["COLD", "GONE", "BADT"], drop=[("COLD", H0 - HOUR)],
                 tickers_fn=lambda: snap, setup=setup)
    assert run.by_close[H0].degraded_reasons == ["unscreened"]
    for m in (1, 2):
        r = run.by_close[H0 + m * MIN]
        assert (r.degraded_reasons, r.fetches, r.candidates) == (["unscreened"], 0, []), (m, r)
    r3 = run.by_close[H0 + 3 * MIN]
    assert (r3.candidates, r3.fetches, r3.degraded_reasons) == (["COLD"], 1, []), r3
    assert run.ex.attempts["COLD"] == 6 and run.ex.attempts["GONE"] == 0 and run.ex.attempts["BADT"] == 0
    assert sent(run) == [("COLD", H0 + 7 * MIN, H0 + 8 * MIN)], sent(run)
    warn = [m for m in run.cap.messages(logging.WARNING) if "仍沒有" in m]
    assert len(warn) == 1 and _taipei(H0 - HOUR) in warn[0] and "COLD" in warn[0], warn
    assert "GONE" not in warn[0] and "BADT" not in warn[0], warn   # tickers 沒有 / 不合法：未粗篩但不算 degraded

    # 每小時摘要的「未粗篩」= 小時結束時仍未粗篩的 {symbol: 原因}，三種原因都列、依 symbol 排序、可以機器解析
    def unscreened(hour_ms):
        return ast.literal_eval(hour_summary(run, hour_ms).rsplit("、未粗篩 ", 1)[1])
    prev, cur = unscreened(H0 - HOUR), unscreened(H0)
    assert prev == {"BADT": "ticker_invalid", "COLD": "no_sample", "GONE": "not_in_tickers"}, prev
    assert cur == {"BADT": "ticker_invalid", "GONE": "not_in_tickers"}, cur     # COLD 第 3 分有樣本後移除
    assert list(prev) == sorted(prev) and list(cur) == sorted(cur), (prev, cur)


# ============================== AC-1 第 7 項：停止追蹤 ==============================
def test_ac1_07_stop_tracking_when_rise_fails_or_previous_hour_incomplete():
    new = make_1m({-1: (R_UP, None)})
    new = new[new["time"] >= H0 - 30 * MIN].reset_index(drop=True)
    frames = {"LOW": make_1m({-1: (R_LOW, None)}), "NEW": new, "KEEP": make_1m({-1: (R_UP, None)})}
    run = run_s5(frames, H0 + 5 * MIN, extra_seeds={"NEW": [(H0 - 2 * HOUR, 1.0), (H0 - HOUR, 1.0)]})
    r1 = run.by_close[H0 + MIN]
    assert r1.candidates == ["KEEP", "LOW", "NEW"], r1.candidates
    assert r1.stopped == {"LOW": s5_feed.STOP_RISE_BELOW, "NEW": s5_feed.STOP_INCOMPLETE}, r1.stopped
    assert dict(run.ex.attempts) == {"LOW": 1, "NEW": 1, "KEEP": 5}, run.ex.attempts
    assert run.by_close[H0 + 2 * MIN].candidates == ["KEEP"]
    # NFR-4：沒有重試的分鐘，請求數 = 仍在追蹤的候選數
    for r in run.results:
        assert r.fetches == len(r.candidates), (r.bar_close_ms, r.fetches, r.candidates)
    stops = hour_summary(run, H0)
    assert "精確①通過 1 ['KEEP']" in stops, stops


# ============================== AC-1 第 8 項：MARKET_INVALID_SYMBOL ==============================
def test_ac1_08a_a5_invalid_symbol_stops_the_hour_without_retry_or_degraded():
    frames = {"INV": make_1m({-1: (R_UP, None), 0: (R_UP, None)})}
    run = run_s5(frames, H0 + HOUR + 2 * MIN, hooks={"INV": lambda a, n, r: invalid_error()})
    assert run.ex.attempts["INV"] == 2, run.ex.attempts          # 每小時一次，不重試
    for r in run.results:
        assert not r.degraded and r.failed == {}, (r.bar_close_ms, r.degraded_reasons, r.failed)
    for h in (H0, H0 + HOUR):
        r = run.by_close[h + MIN]
        assert (r.invalid_symbols, r.stopped, r.fetches) == (["INV"], {"INV": s5_feed.STOP_INVALID_SYMBOL}, 1), r
    info = [m for m in run.cap.messages(logging.INFO) if m.startswith("策略五 INV：klines 回 " + INVALID)]
    assert len(info) == 1, info
    assert not run.cap.messages(logging.WARNING) and not run.cap.messages(logging.ERROR)
    st = run.s5.stats()
    assert st["invalid_symbol_stops"] == 2 and st["degraded"] == 0, st


def test_ac1_08b_a5_invalid_symbol_info_once_per_taipei_day():
    clock = tsf.FakeClock(H0)
    gate = make_gate(clock, FakeExchange1M(clock, {}).api_get)
    s5 = s5_feed.S5Feed(gate=gate, buffer=PriceBuffer(HOUR), universe_fn=list, submit_fn=lambda **kw: None,
                        clock=clock, executor=tsf.InlineExecutor())
    with tsf.capture_logs() as cap:
        s5._log_invalid("X")
        s5._log_invalid("X")
        tsf.goto(clock, H0 + 24 * HOUR)
        s5._log_invalid("X")
    assert len([m for m in cap.messages(logging.INFO) if m.startswith("策略五 X：")]) == 2
    assert s5.stats()["invalid_symbol_stops"] == 3


def test_ac1_08c_reconcile_invalid_symbol_not_retried_not_failed_one_info():
    run = tsf._scenario_run(reconciler_kw=tsf._RECON_KW)
    ex, rec = run["ex"], run["reconciler"]
    ex.klines_hook["FLAT_0"] = lambda a, n, r: invalid_error()
    before = ex.klines_attempts["FLAT_0"]
    with tsf.capture_logs() as cap:
        rr1 = rec.run_batch(tsf._close_of(tsf._SCEN_HI))
        mid = ex.klines_attempts["FLAT_0"]
        rr2 = rec.run_batch(tsf._close_of(tsf._SCEN_HI))
    assert (mid - before, ex.klines_attempts["FLAT_0"] - mid) == (1, 1), (before, mid, ex.klines_attempts["FLAT_0"])
    for rr in (rr1, rr2):
        assert rr.invalid_symbols == ["FLAT_0"], rr.invalid_symbols
        assert all("FLAT_0" not in v for v in rr.fetch_failed.values()), rr.fetch_failed
        assert rr.misses == [], rr.misses
    info = [m for m in cap.messages(logging.INFO) if m.startswith("背景對帳：FLAT_0")]
    assert len(info) == 1, info
    bad = [m for m in cap.messages(logging.WARNING) + cap.messages(logging.ERROR) if "FLAT_0" in m]
    assert not bad, bad


def test_ac1_08d_seed_invalid_symbol_not_retried_not_failed_one_info():
    clock = tsf.FakeClock(tsf.T0)
    ex = tsf.FakeExchange(clock, {"INV": tsf.make_frame(40, tsf.T0 - 40 * tsf.BAR, 1)})
    ex.klines_hook["INV"] = lambda a, n, r: invalid_error()
    feed, _ = tsf.build_feed(clock, ex, ["INV"])
    with tsf.capture_logs() as cap:
        feed.start_seeding(["INV"], initial=True)
        feed.start_seeding(["INV"], initial=False)
    assert ex.klines_attempts["INV"] == 2, ex.klines_attempts      # 每次補種子一個請求，不重試
    info = [m for m in cap.messages(logging.INFO) if m.startswith("補種子 INV")]
    assert len(info) == 1 and INVALID in info[0], info
    assert not [m for m in cap.messages() if "補種子失敗" in m]
    assert not cap.messages(logging.WARNING) and not cap.messages(logging.ERROR)
    assert "INV" not in feed._seed_failed
    done = [m for m in cap.messages(logging.INFO) if m.startswith("冷啟動補種子完成")]
    assert len(done) == 1 and "失敗 0 個" in done[0], done


# ============================== AC-1 第 9 項：_deliver ==============================
def test_ac1_09_deliver_add_bar_exception_still_calls_on_result_once():
    clock = tsf.FakeClock(tsf.T0)
    calls = []
    feed, _ = tsf.build_feed(clock, tsf.FakeExchange(clock, {}), [], on_result=calls.append)

    def boom(res):
        raise RuntimeError("add_bar 壞了")
    feed.reconciler = types.SimpleNamespace(add_bar=boom)
    res = types.SimpleNamespace(bar_close_ms=tsf.T0)
    with tsf.capture_logs() as cap:
        feed._deliver(res)
    assert calls == [res], calls
    errs = cap.messages(logging.ERROR)
    assert len(errs) == 1 and "add_bar 失敗" in errs[0], errs
    assert cap.records[-1].exc_info is not None


# ============================== AC-1 第 10 項：例外隔離 ==============================
def test_ac1_10a_one_symbol_exception_does_not_block_others_in_the_same_minute():
    spec = {-1: (R_UP, None), 0: (0.0, 3)}
    frames = {"A": make_1m(spec), "B": make_1m(spec, base=5.0), "C": make_1m({-1: (R_UP, None)})}

    def c_hook(attempt, now, rows):
        raise RuntimeError("C 的連線炸了")
    orig = s5_signal.evaluate

    def evaluate(df, bar_ms, params=None):
        if float(df["close"].iloc[-1]) < 2:                    # A 的價位 ~1，B 的價位 ~5
            raise ValueError("A 的計算炸了")
        return orig(df, bar_ms, params)
    with tsf.patched(s5_signal, "evaluate", evaluate):
        run = run_s5(frames, H0 + 3 * MIN, hooks={"C": c_hook})
    assert sent(run) == [("B", H0 + 2 * MIN, H0 + 3 * MIN)], sent(run)
    for m in (1, 2, 3):
        r = run.by_close[H0 + m * MIN]
        assert r.failed == {"exception": ["A", "C"]} and r.degraded_reasons == ["exception"], (m, r.failed)
    errs = run.cap.messages(logging.ERROR)
    assert any("A 判定時發生未預期的例外" in e for e in errs), errs
    assert any("C 取數時發生未預期的例外" in e for e in errs), errs


def test_ac1_10b_on_result_exception_does_not_block_signal():
    def bad(res):
        raise RuntimeError("回呼壞了")
    run = run_s5({"B": make_1m({-1: (R_UP, None), 0: (0.0, 3)})}, H0 + 3 * MIN, on_result=bad)
    assert sent(run) == [("B", H0 + 2 * MIN, H0 + 3 * MIN)], sent(run)
    errs = [e for e in run.cap.messages(logging.ERROR) if "on_result 回呼失敗" in e]
    assert len(errs) == len(run.results) == 4, (len(errs), len(run.results))


# ============================== AC-1 第 11 項：RestBanned ==============================
def test_ac1_11_rest_banned_minute_is_degraded_and_signal_is_sent_after_cooldown():
    def hook(attempt, now, rows):
        if H0 + 3 * MIN <= now < H0 + 4 * MIN:
            raise lh.ApiError("HTTP 429", status_code=429)
        return rows
    run = run_s5({"BAN": make_1m({-1: (R_UP, None), 0: (0.0, 3)})}, H0 + 5 * MIN, hooks={"BAN": hook})
    r3, r4, r5 = (run.by_close[H0 + m * MIN] for m in (3, 4, 5))
    assert (r3.fetches, r3.http_429, r3.failed, r3.degraded_reasons) == (1, 1, {"banned": ["BAN"]}, ["banned"]), r3
    # 冷卻中：閘門直接拒絕（沒有送出），不硬等
    assert (r4.fetches, r4.failed, r4.degraded_reasons) == (0, {"banned": ["BAN"]}, ["banned"]), r4
    assert r4.latency_s <= WAIT_MS / 1000.0 + 1, r4.latency_s
    assert sent(run) == [("BAN", H0 + 2 * MIN, H0 + 3 * MIN)], sent(run)
    assert [(x.symbol, x.late) for x in r5.signals] == [("BAN", True)]
    assert run.ex.attempts["BAN"] == 4, run.ex.attempts
    st = run.s5.stats()
    assert st["http_429"] == 1 and st["late_submits"] == 1, st
    assert any("取數失敗 banned" in m for m in run.cap.messages(logging.ERROR))


# ============================== FR-6：讓 A1 先做 ==============================
def test_fr6_waits_for_a1_foreground_hold_before_fetching():
    def setup(ns):
        lim = ns.gate.limiter
        ns.clock.call_at(ns.clock.mono_of(H0 + MIN + 1000), lim.hold_background)
        ns.clock.call_at(ns.clock.mono_of(H0 + MIN + 2700), lim.release_background)
    run = run_s5({"Y": make_1m({-1: (R_UP, None)})}, H0 + MIN, setup=setup)
    r1 = run.by_close[H0 + MIN]
    assert abs(r1.yield_s - 0.7) < 0.1, r1.yield_s
    assert r1.fetches == 1
    assert min(t for t, s in run.ex.calls if s == "Y") >= H0 + MIN + 2700
    assert run.by_close[H0].yield_s == 0


def test_fr6_yield_is_capped_and_foreground_fetch_still_goes_out():
    def setup(ns):
        lim = ns.gate.limiter
        ns.clock.call_at(ns.clock.mono_of(H0 + MIN + 1000), lim.hold_background)
        ns.clock.call_at(ns.clock.mono_of(H0 + MIN + 30_000), lim.release_background)
    run = run_s5({"Y": make_1m({-1: (R_UP, None)})}, H0 + MIN, setup=setup)
    r1 = run.by_close[H0 + MIN]
    assert abs(r1.yield_s - config.S5_YIELD_MAX_SECONDS) < 0.1, r1.yield_s
    assert r1.fetches == 1 and r1.failed == {}, r1
    first = min(t for t, s in run.ex.calls if s == "Y")
    assert H0 + MIN + WAIT_MS + config.S5_YIELD_MAX_SECONDS * 1000 - 1 <= first < H0 + MIN + 30_000, first


# ============================== AC-1 第 12 項：a_channel 接線 ==============================
class _A1ForWiring:
    """假 A1：有 gate / buffer / clock / universe() / latest_tickers()（A5 要共用的東西）。run() 等 A5 的執行緒跑完。"""

    def __init__(self, order, clock=None, gate=None, buffer=None, syms=()):
        self.order, self.clock, self.gate, self.buffer, self._syms = order, clock, gate, buffer, list(syms)
        self.ran = False
        self.s5 = None
        self.s5_alive_at_close = None

    def universe(self):
        return list(self._syms)

    def latest_tickers(self):
        return None

    def run(self, duration_s=None):
        self.ran = True
        if self.s5 is not None and self.s5._thread is not None:
            self.s5._thread.join(30)

    def close(self):
        t = getattr(self.s5, "_thread", None)
        self.s5_alive_at_close = t is not None and t.is_alive()
        self.order.append("A1")


class _A3ForWiring:
    specs = {}
    ready_error = None

    def __init__(self, order):
        self.order = order
        self.submits = []
        self.stats = {"fake": True}

    def start(self):
        pass

    def wait_ready(self, timeout):
        return True

    def bar_result_handler(self, strategy):
        return lambda res: None

    def submit_signal(self, **kw):
        self.submits.append(kw)

    def stop(self):
        self.order.append("A3")

    def join(self, timeout=None):
        return True


class _S5Stub:
    def __init__(self, order, close_rv=True, stats=None, start_error=None):
        self.order, self.close_rv, self._stats, self.start_error = order, close_rv, stats or {}, start_error

    def start(self):
        if self.start_error is not None:
            raise self.start_error

    def close(self, timeout=None):
        self.order.append("A5")
        return self.close_rv

    def stats(self):
        return dict(self._stats)


def _main(a1, a3, s5_factory):
    with tsf.capture_logs() as cap:
        rc = a_channel.main(["--duration", "5"], setup_logging=False, tracker_factory=lambda bus: a3,
                            feed_factory=lambda on_result: a1, s5_feed_factory=s5_factory)
    return rc, cap


def test_ac1_12a_a_channel_wires_real_a5_into_tracker_and_closes_a5_first():
    order = []
    clock = tsf.FakeClock(H0 - 30_000)
    frame = make_1m({-1: (R_UP, None), 0: (0.0, 2)})
    ex = FakeExchange1M(clock, {"W": frame})
    gate = make_gate(clock, ex.api_get)
    buf = PriceBuffer(100 * HOUR)
    buf.add_seed("W", hour_seeds(frame))
    a1 = _A1ForWiring(order, clock, gate, buf, ["W"])
    a3 = _A3ForWiring(order)
    duration = (H0 + 2 * MIN + WAIT_MS + 1000 - clock.time_ms()) / 1000.0

    def factory(feed, tracker):
        s5 = a_channel.default_s5_feed(feed, tracker, executor=tsf.InlineExecutor())
        start, close = s5.start, s5.close
        s5.start = lambda: start(duration_s=duration)          # 正式執行不給 duration（跑到 close 為止）

        def closing(timeout=None):
            order.append("A5")
            return close(timeout)
        s5.close = closing
        feed.s5 = s5
        return s5
    rc, cap = _main(a1, a3, factory)
    assert rc == 0, cap.messages(logging.ERROR)
    assert a1.ran
    assert [(s["strategy"], s["symbol"], s["bar_open_ms"], s["bar_close_ms"]) for s in a3.submits] == \
        [("s5", "W", H0 + MIN, H0 + 2 * MIN)], a3.submits
    assert order == ["A5", "A1", "A3"], order
    assert a1.s5_alive_at_close is False                       # A5 的執行緒結束了才關 A1
    assert a1.s5.gate is gate and a1.s5.buffer is buf and a1.s5.clock is clock
    assert any(m.startswith("A5 總結") for m in cap.messages(logging.INFO))


def test_ac1_12b_a5_build_or_start_failure_exits_1_and_never_runs_s4_alone():
    order = []
    a1 = _A1ForWiring(order)

    def boom(feed, tracker):
        raise RuntimeError("A5 建不起來")
    rc, cap = _main(a1, _A3ForWiring(order), boom)
    assert rc == 1 and not a1.ran and order == ["A1", "A3"], (rc, a1.ran, order)
    assert any("（A5）建立或啟動失敗" in m for m in cap.messages(logging.ERROR))

    order = []
    a1 = _A1ForWiring(order)
    stub = _S5Stub(order, start_error=RuntimeError("A5 啟動失敗"))
    rc, cap = _main(a1, _A3ForWiring(order), lambda feed, tracker: stub)
    assert rc == 1 and not a1.ran and order == ["A5", "A1", "A3"], (rc, a1.ran, order)


def test_ac1_12c_a5_crash_or_join_timeout_makes_exit_code_1():
    order = []
    a1 = _A1ForWiring(order)
    stub = _S5Stub(order, stats={"worker_crashed": True})
    rc, cap = _main(a1, _A3ForWiring(order), lambda feed, tracker: stub)
    assert rc == 1 and a1.ran and order == ["A5", "A1", "A3"], (rc, order)
    assert any("A5 的工作執行緒在執行中意外結束" in m for m in cap.messages(logging.ERROR))

    order = []
    a1 = _A1ForWiring(order)
    rc, _ = _main(a1, _A3ForWiring(order), lambda feed, tracker: _S5Stub(order, close_rv=False))
    assert rc == 1 and a1.ran and order == ["A5", "A1", "A3"], (rc, order)

    order = []
    a1 = _A1ForWiring(order)
    rc, _ = _main(a1, _A3ForWiring(order), lambda feed, tracker: _S5Stub(order))
    assert rc == 0 and order == ["A5", "A1", "A3"], (rc, order)


# ============================== AC-1 第 13 項：ApiError ==============================
class _Resp:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body
        self.text = body if isinstance(body, str) else json.dumps(body)

    def json(self):
        if isinstance(self._body, str):
            raise ValueError("不是 JSON")
        return self._body


def _api_get_error(resp):
    calls = []

    def get(url, params=None, headers=None, timeout=None):
        calls.append(url)
        return resp
    with tsf.patched(lh.requests, "get", get):
        try:
            lh.api_get(klines.KLINES_PATH, {"symbol": "X"}, retries=3)
        except lh.ApiError as e:
            return e, calls
    raise AssertionError("沒有拋 ApiError")


def test_ac1_13a_api_error_is_backward_compatible():
    e = lh.ApiError("x")
    assert (str(e), e.args, e.status_code, e.code) == ("x", ("x",), None, None)
    e = lh.ApiError("y", 429)
    assert (str(e), e.status_code, e.code) == ("y", 429, None)
    e = lh.ApiError("z", status_code=400, code="C")
    assert (str(e), e.status_code, e.code) == ("z", 400, "C")


def test_ac1_13b_code_comes_from_result_false_and_4xx_body():
    err, calls = _api_get_error(_Resp(200, {"result": False, "code": INVALID, "message": "invalid symbol"}))
    assert (err.code, err.status_code, len(calls)) == (INVALID, None, 1), (err.code, err.status_code, calls)
    assert ("code=%s" % INVALID) in str(err)
    err, calls = _api_get_error(_Resp(400, {"result": False, "code": INVALID, "message": "invalid symbol"}))
    assert (err.code, err.status_code, len(calls)) == (INVALID, 400, 1)
    err, calls = _api_get_error(_Resp(403, "forbidden"))
    assert (err.code, err.status_code, len(calls)) == (None, 403, 1)
    err, calls = _api_get_error(_Resp(404, "<html>not found</html>"))
    assert (err.code, err.status_code) == (None, 404)
    err, calls = _api_get_error(_Resp(200, {"result": False, "code": 7, "message": "x"}))
    assert err.code == 7 and err.status_code is None           # result=false 原樣帶入（呼叫端比對字串常數）


# ============================== 其他：訊號日誌、工作執行緒 ==============================
def test_signal_log_line_has_branch_label_and_on_time_tail():
    run = run_s5({"BL": make_1m({-1: (R_UP, None), 0: (0.0, 3)})}, H0 + 3 * MIN)
    assert sent(run) == [("BL", H0 + 2 * MIN, H0 + 3 * MIN)], sent(run)
    assert all(r.failed == {} for r in run.results), [r.failed for r in run.results]
    code = run.submits[0]["features"]["branch_code"]
    label = s5_signal.branch_label(code, P)
    assert code & 1 and label != "-", (code, label)
    line = [m for m in run.cap.messages(logging.INFO) if m.startswith("原始訊號 BL @ K 棒 " + _taipei(H0 + 3 * MIN))]
    assert len(line) == 1 and ("分支 " + label) in line[0] and "收盤後 2.0 秒送出" in line[0], line
    assert run.submits[0]["features"]["late_s"] == WAIT_MS / 1000.0


def test_worker_crash_is_logged_and_reported_not_restarted():
    clock = tsf.FakeClock(H0)
    gate = make_gate(clock, FakeExchange1M(clock, {}).api_get)
    s5 = s5_feed.S5Feed(gate=gate, buffer=PriceBuffer(HOUR), universe_fn=list, submit_fn=lambda **kw: None,
                        clock=clock, executor=tsf.InlineExecutor())

    def boom(now_ms):
        raise RuntimeError("迴圈炸了")
    s5._due_closes = boom
    with tsf.capture_logs() as cap:
        s5.start(duration_s=60)
        s5._thread.join(10)
        assert s5.close(timeout=5)
    st = s5.stats()
    assert st["worker_crashed"] is True and st["worker_alive"] is False, st
    errs = [m for m in cap.messages(logging.ERROR) if "意外結束" in m]
    assert len(errs) == 1, errs


# ============================== NFR-1 / NFR-2 ==============================
S5_FILES = ("live/s5_feed.py", "tests/test_s5_feed.py")


def _number_hits(src, banned):
    hits = []
    lines = src.splitlines()
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type != tokenize.NUMBER:
            continue
        try:
            val = float(tok.string.replace("_", ""))
        except ValueError:
            continue
        for name, v in banned.items():
            if math.isclose(val, v, rel_tol=0.0, abs_tol=1e-12):
                hits.append("%s = %s @ %d: %s" % (name, tok.string, tok.start[0], lines[tok.start[0] - 1].strip()))
    return hits


def test_nfr2_no_hardcoded_s5_threshold_numbers():
    """程式碼（不含註解、字串）裡沒有 MIN_RISE_FROM_OPEN、S5_SCREEN_RISE_MARGIN、粗篩門檻的數值。用 tokenize 掃。"""
    banned = {"MIN_RISE_FROM_OPEN": M, "S5_SCREEN_RISE_MARGIN": G, "粗篩門檻": s5_feed.screen_threshold()}
    # 掃描器本身要抓得到（探針由參數值產生，不寫字面值）；註解與字串要略過
    assert _number_hits("x = %r\n" % M, banned) and not _number_hits("# %r\ny = '%r'\n" % (M, M), banned)
    hits = []
    for rel in S5_FILES:
        with open(os.path.join(REPO_ROOT, rel), encoding="utf-8") as f:
            hits += ["%s: %s" % (rel, h) for h in _number_hits(f.read(), banned)]
    assert not hits, hits


def test_nfr1_imports_module_name_and_execution_params():
    with open(s5_feed.__file__, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    mods = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            mods.add((node.module or "").split(".")[0])
    bad = sorted(m for m in mods if m not in ("live", "strategy") and m not in sys.stdlib_module_names)
    assert not bad, bad
    assert "s5_feed" not in sys.stdlib_module_names
    ep = config.execution_params()
    for k in ("S5_SCREEN_RISE_MARGIN", "S5_KLINES_LIMIT", "S5_FETCH_CONCURRENCY", "S5_YIELD_MAX_SECONDS",
              "S5_JOIN_TIMEOUT_SECONDS"):
        assert ep[k] == getattr(config, k), k


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)
             and getattr(f, "__module__", None) == "__main__"]
    failed = 0
    before = len(tsf._blocked_attempts)
    with tsf.socket_cage():
        for name, fn in tests:
            try:
                fn()
                print(f"PASS  {name}")
            except Exception as e:  # noqa: BLE001
                failed += 1
                print(f"FAIL  {name}: {type(e).__name__}: {e}")
    blocked = tsf._blocked_attempts[before:]
    if blocked:
        failed += 1
        print(f"FAIL  有測試企圖連網 {len(blocked)} 次：{blocked[:3]}")
    else:
        print("PASS  全程沒有任何連網企圖（socket 籠子記錄 0 次）")
    print(f"\n{len(tests) - failed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
