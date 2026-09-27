# -*- coding: utf-8 -*-
"""
A1-f 驗收測試 — A1 後續小修：長停頓對帳不誤報且會補做、market_static 經過閘門、degraded 的排除範圍。

  AC-1  停頓 3 小時 / 12 小時後醒來：
          * 停頓前那一小時的批次不再誤報「完全沒有紀錄」（涵蓋內 → 照常對帳；超出涵蓋 → 一行 WARNING）
          * 排程在對帳攤平到一半時被凍結（最常見的情況）：醒來後把這一輪交回，涵蓋內的錯過窗口合併成
            一輪取數、依時間順序各交出結果；請求攤平、PRIORITY_BACKGROUND、前景保留期間一個都不送
          * 超出涵蓋的批次只有一行 WARNING；真的沒有紀錄的根仍然記 ERROR
  AC-2  停頓 24 小時醒來：紀錄數量任何時刻都不超過 RECONCILE_KLINES_LIMIT；批次處理完的紀錄會修掉
  AC-3  A1 的標的池刷新經過共用閘門（計入閘門統計、PRIORITY_NORMAL、retries=1）；吃到 429 → 閘門冷卻、
        HTTP 請求恰好 1 個（market_static / api_get 不重試、啟動重試落在冷卻中不送）；不注入時行為不變
  AC-4  只有個別候選取數失敗的根：同一根其他 symbol 的漏失照算（ERROR），失敗的候選個別排除；
        影響粗篩的 degraded（tickers 失敗 / tickers 被封鎖）整根排除，同修改前；"banned" 撞名用結構分辨

鑑別力（DQA 拿修改前的程式 ee12b85 跑同一個情境）：每個情境的關鍵斷言放在最前面，而且只經過修改前也存在的
介面（Reconciler.run_batch / _loop / add_bar、SignalFeed.refresh_universe、MarketUniverse），修改前的程式
會在那一行失敗（各測試的註解寫了修改前會看到什麼）。

全程離線：重用 tests/test_signal_feed.py 的假時鐘、假交易所、socket 籠子；不真的 sleep。
不依賴 pytest：直接 `python tests/test_a1f_followups.py`。
"""
import importlib
import inspect
import logging
import os
import re
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import test_signal_feed as tsf  # noqa: E402  —— 假時鐘、假交易所、socket 籠子、情境產生器都用這一份
from test_market_static import RISK_FIXTURE, SYMBOLS_FIXTURE  # noqa: E402

import live.pionex_api as lh  # noqa: E402
from live import config, klines, market_static, rest_gate, signal_feed  # noqa: E402
from live import reconcile  # noqa: E402
from live.reconcile import Reconciler  # noqa: E402
from live.rest_gate import RestGate, RestLimiter, ServerClock  # noqa: E402

BAR, T0 = tsf.BAR, tsf.T0
HOUR = 3_600_000
FreezeClock, FakeClock, FakeExchange = tsf.FreezeClock, tsf.FakeClock, tsf.FakeExchange
capture_logs, InlineExecutor = tsf.capture_logs, tsf.InlineExecutor
# 排程情境用正式的開始延遲（窗口結束後 30 秒才開始），讓最後一根先交進來；直接呼叫 run_batch 的沿用 _RECON_KW
REPLAY_KW = {"interval_s": 3600, "spread_fraction": config.RECONCILE_SPREAD_FRACTION,
             "start_delay_s": config.RECONCILE_START_DELAY_SECONDS}


def _make_gate(clock, api_get=None):
    return RestGate(RestLimiter(config.A1_REST_RATE_PER_SECOND, config.REST_BAN_COOLDOWN_SECONDS, clock=clock),
                    ServerClock(clock), clock=clock, api_get=api_get)


def _missing_errors(cap):
    return [m for m in cap.messages(logging.ERROR) if "完全沒有紀錄" in m]


def _coverage_warnings(cap):
    return [m for m in cap.messages(logging.WARNING) if "超出 klines 涵蓋" in m]


# ============================== AC-1 / AC-2 共用：停頓情境 ==============================
# 從 T0 前 60 秒啟動 A1（第 0 根收盤 = T0），在 freeze_at 凍結，醒在 T0 + N 小時 27 分前 0.5 秒。
#   _FREEZE_AT        判定完第 4 根（T0+20 分）之後。對帳排程此時正在攤平 (T0-1h, T0] 的請求（最常見的狀態）
#   _FREEZE_WAITING   判定完第 10 根（T0+50 分）之後。對帳排程此時已經做完 (T0-1h, T0]、在等 T0+1h 的窗口
# 醒來的時刻刻意挑過：排程補做那一輪的第 2 個請求剛好排在某根收盤的前景保留期內，才測得到「讓前景」。
_FREEZE_AT = T0 + 21 * 60_000
_FREEZE_WAITING = T0 + 51 * 60_000
_STALL_CACHE = {}


def _wake(hours):
    return T0 + hours * HOUR + 27 * 60_000 - 500


def _stall_feed(hours, freeze_at=_FREEZE_AT):
    """跑 A1 主迴圈（test_signal_feed._loop_run：對帳器掛著但不開執行緒），凍結到 T0 + hours 小時多。
    回傳 (每根的 BarResult, 對帳器（紀錄是 A1 交給它的）, 醒來時刻)。同一組參數只跑一次。"""
    key = (hours, freeze_at)
    if key not in _STALL_CACHE:
        wake = _wake(hours)
        n = (wake - T0) // BAR + 3                     # 醒來後再判定兩根
        results, _, _, _, _, rec = tsf._loop_run(n, freezes=[(freeze_at, wake)], reconciler_kw=tsf._RECON_KW)
        closes = [r.bar_close_ms for r in results]
        assert closes == [T0 + k * BAR for k in range(n)], "每一根收盤都要有結果（BUG-005）"
        _STALL_CACHE[key] = (results, rec, wake)
    return _STALL_CACHE[key]


def _replay(hours, *, until_window, drop_closes=(), holds=False, freeze_at=_FREEZE_AT):
    """排程情境：把 A1 在停頓情境裡交出的每根結果，照 A1 當時交出的時刻（收盤 + 開始處理的延誤 + 0.5 秒）
    交給一個新的對帳器，同步跑它的排程 _loop（同一個凍結）。對帳器 T0 + 30 秒開始對 (T0-1h, T0]。
    on_result 收到 until_window 那個窗口就停。
    holds=True：醒來後每根收盤前 1 秒到收盤後 3 秒閘門只放前景（同 A1 的前景保留）。"""
    results, _, wake = _stall_feed(hours, freeze_at)
    frames = tsf._loop_frames()
    clock = FreezeClock(T0 - 60_000, [(freeze_at, wake)])
    ex = FakeExchange(clock, frames)
    gate = _make_gate(clock, ex.api_get)
    out = []
    stop = threading.Event()

    def on_result(rr):
        out.append(rr)
        if rr.window_end_ms >= until_window and not rr.out_of_coverage:
            stop.set()
    rec = Reconciler(gate, clock=clock, on_result=on_result, **REPLAY_KW)
    rec.expect_bars_from(T0)
    for res in results:
        if res.bar_close_ms in drop_closes:
            continue
        at = res.bar_close_ms + int(max(0.0, res.close_delay_s or 0.0) * 1000) + 500
        clock.call_at(clock.mono_of(at), lambda res=res: rec.add_bar(res))
    hold_spans = []
    if holds:
        guard = config.FOREGROUND_GUARD_SECONDS
        first = (wake // BAR + 1) * BAR
        for k in range(14):
            c = first + k * BAR
            on, off = clock.mono_of(c) - guard, clock.mono_of(c) + 3.0
            hold_spans.append((on, off))
            clock.call_at(on, gate.limiter.hold_background)
            clock.call_at(off, gate.limiter.release_background)
    clock.call_at(clock.mono_of(wake + 3 * HOUR), stop.set)      # 保險：修改前的程式不會補做，不能讓它一直跑
    with capture_logs() as cap:
        rec._loop(stop, gate.server_clock)
    return {"out": out, "cap": cap, "ex": ex, "gate": gate, "clock": clock, "rec": rec, "wake": wake,
            "holds": hold_spans}


# ============================== AC-1：長停頓不誤報、補做 ==============================
def test_ac1_3h_pre_stall_hour_is_reconciled_without_false_missing_error():
    """對帳器在停頓期間是「等下一個整點」的狀態：醒來直接對停頓前那一小時 (T0, T0+1h]。
    修改前：醒來時停頓期間的 missed_close 一口氣進來，最新收盤往前跳 3 小時，這一小時的紀錄全被修掉
    → ERROR「12 根應有的 K 棒完全沒有紀錄」。"""
    _, rec, _ = _stall_feed(3)
    with capture_logs() as cap:
        rr = rec.run_batch(T0 + HOUR)
    assert _missing_errors(cap) == [], _missing_errors(cap)
    assert rr.bars_expected == 12 and rr.bars == 12 and rr.bars_missing == [], (rr.bars, rr.bars_missing)
    assert rr.bars_missed_close == [T0 + k * BAR for k in range(5, 13)], rr.bars_missed_close
    assert not rr.out_of_coverage and rr.pairs_compared > 0 and rr.requests == rr.symbols > 0
    assert not cap.messages(logging.ERROR), cap.messages(logging.ERROR)


def test_ac1_12h_pre_stall_hour_out_of_coverage_is_one_warning_not_error():
    """停頓 12 小時：停頓前那一小時早已超出 klines 涵蓋（100 根 5 分K + ret2h 要往前 2 小時）→ 不取數、
    不比對、一行 WARNING。修改前：紀錄被修掉 → ERROR「完全沒有紀錄」（外加一整輪白打的請求）。"""
    _, rec, _ = _stall_feed(12)
    with capture_logs() as cap:
        rr = rec.run_batch(T0 + HOUR)
    assert _missing_errors(cap) == [], _missing_errors(cap)
    assert not cap.messages(logging.ERROR), cap.messages(logging.ERROR)
    assert rr.out_of_coverage and rr.requests == 0 and rr.pass_windows == 0
    warns = _coverage_warnings(cap)
    assert len(warns) == 1 and "1 個批次" in warns[0], warns


def test_ac1_3h_scheduler_catches_up_in_order_spread_and_yielding():
    """排程在攤平到一半時凍結 3 小時：醒來後 (T0-1h,T0]、(T0,T0+1h]、…、(T0+2h,T0+3h] 四個窗口共用一輪取數、
    依時間順序各交出結果；停頓前那一小時沒有誤報；另外拿掉一根的紀錄 → 只有那一根記 ERROR。
    修改前：醒來後把剩下的請求一口氣補打、只交出 (T0-1h,T0]，接著跳到 T0+4h，中間三個小時的批次永遠不做。"""
    drop = T0 + 2 * HOUR + 30 * 60_000                        # 停頓期間的一根：連 missed_close 都沒有
    run = _replay(3, until_window=T0 + 3 * HOUR, drop_closes=(drop,), holds=True)
    out, cap, wake = run["out"], run["cap"], run["wake"]
    assert [r.window_end_ms for r in out] == [T0 + k * HOUR for k in range(4)], \
        [(r.window_end_ms - T0) / HOUR for r in out]
    missing = _missing_errors(cap)
    assert len(missing) == 1 and tsf.signal_feed._taipei(drop) in missing[0], missing
    by_end = {r.window_end_ms: r for r in out}
    assert by_end[T0 + 3 * HOUR].bars_missing == [drop]
    for k in range(3):
        assert by_end[T0 + k * HOUR].bars_missing == [], (k, by_end[T0 + k * HOUR].bars_missing)
    pre = by_end[T0 + HOUR]
    assert pre.bars == 12 and pre.bars_missed_close == [T0 + k * BAR for k in range(5, 13)]
    assert all(not r.out_of_coverage and r.pass_windows == 4 and not r.aborted for r in out)
    assert not _coverage_warnings(cap), _coverage_warnings(cap)
    assert any("交回排程" in m for m in cap.messages(logging.INFO)), "攤平到一半停頓應該交回排程"
    # 一輪取數：每個 symbol 只打一次 klines（不是每個窗口各打一次），攤平、背景優先、前景保留期間不送
    clock, ex, gate = run["clock"], run["ex"], run["gate"]
    w_mono = clock.mono_of(wake)
    bg = [c[0] for c in ex.calls if c[2] == klines.KLINES_PATH and c[0] >= w_mono]
    n = len(tsf._loop_frames())
    assert len(bg) == n == out[-1].requests, (len(bg), n, out[-1].requests)
    step = 3600 * config.RECONCILE_SPREAD_FRACTION / n
    guard = config.FOREGROUND_GUARD_SECONDS
    for i, t in enumerate(bg):
        assert w_mono + i * step - 1e-6 <= t <= w_mono + i * step + guard + 3.0 + 1e-6, (i, t - w_mono, i * step)
    for t in bg:
        for on, off in run["holds"]:
            assert not (on <= t < off), "補做的請求在前景保留期間送出（t=%.1f，保留 %.1f–%.1f）" % (t, on, off)
    assert any(on <= w_mono + i * step < off for i in range(n) for on, off in run["holds"]), \
        "情境裡應該至少有一個補做請求原本排在前景保留期間，否則沒測到讓出"
    total_klines = sum(1 for c in ex.calls if c[2] == klines.KLINES_PATH)
    assert gate.limiter.granted_by_priority[rest_gate.PRIORITY_BACKGROUND] == total_klines
    assert gate.stats()["http_429"] == 0


def test_ac1_3h_scheduler_waiting_at_freeze_catches_up_missed_hours():
    """排程已做完 (T0-1h, T0]、在等 T0+1h 的窗口時凍結 3 小時：醒來後 (T0,T0+1h]、(T0+1h,T0+2h]、(T0+2h,T0+3h]
    合併成一輪依序補做，停頓前那一小時（判定 10 根 + missed_close 2 根）沒有誤報。
    修改前：醒來先對 (T0,T0+1h]，紀錄已被修掉 → ERROR「完全沒有紀錄」；之後直接跳到 T0+4h。"""
    run = _replay(3, until_window=T0 + 3 * HOUR, freeze_at=_FREEZE_WAITING)
    out, cap = run["out"], run["cap"]
    assert _missing_errors(cap) == [], _missing_errors(cap)
    assert [r.window_end_ms for r in out] == [T0 + k * HOUR for k in range(4)], \
        [(r.window_end_ms - T0) / HOUR for r in out]
    assert [r.pass_windows for r in out] == [1, 3, 3, 3]
    pre = out[1]
    assert pre.bars == 12 and pre.bars_missed_close == [T0 + 11 * BAR, T0 + 12 * BAR], pre.bars_missed_close
    assert not _coverage_warnings(cap) and not cap.messages(logging.ERROR), cap.messages(logging.ERROR)
    assert not any("交回排程" in m for m in cap.messages(logging.INFO)), "在等待中凍結，沒有進行中的一輪可交回"


def test_ac1_12h_scheduler_one_warning_for_uncovered_and_catches_up_the_rest():
    """排程在攤平到一半時凍結 12 小時：(T0-1h,T0] … (T0+6h,T0+7h] 共 8 個批次超出涵蓋 → 一行 WARNING；
    (T0+7h,T0+8h] … (T0+11h,T0+12h] 共 5 個合併成一輪依序補做；整段沒有任何「完全沒有紀錄」ERROR。"""
    run = _replay(12, until_window=T0 + 12 * HOUR)
    out, cap = run["out"], run["cap"]
    assert _missing_errors(cap) == [], _missing_errors(cap)
    warns = _coverage_warnings(cap)
    assert len(warns) == 1, warns
    assert "8 個批次" in warns[0], warns[0]
    assert [r.window_end_ms for r in out] == [T0 + k * HOUR for k in range(13)], \
        [(r.window_end_ms - T0) / HOUR for r in out]
    assert [r.out_of_coverage for r in out] == [True] * 8 + [False] * 5
    for r in out[:8]:
        assert r.requests == 0 and r.pass_windows == 0 and r.bars_missing == []
    for r in out[8:]:
        assert r.pass_windows == 5 and r.bars_missing == [] and r.bars == 12 and len(r.bars_missed_close) == 12
    assert not cap.messages(logging.ERROR), cap.messages(logging.ERROR)
    assert run["gate"].stats()["http_429"] == 0


def test_ac1_coverage_boundary_matches_real_digest():
    """涵蓋範圍的算式與實際資料一致：在時刻 t 取 100 根 klines，最早可對帳的窗口每一根都算得出真實 ret2h，
    再早一根的窗口第一根就算不出來。"""
    frames = {"S": tsf.make_frame(520, tsf._LOOP_FIRST_OPEN, 5)}
    fetch_ms = T0 + 12 * HOUR + 7 * 60_000 + 1234
    clock = FakeClock(fetch_ms)
    ex = FakeExchange(clock, frames)
    gate = _make_gate(clock, ex.api_get)
    rec = Reconciler(gate, clock=clock, **tsf._RECON_KW)
    rows = klines.fetch_klines(gate, "S", config.KLINE_INTERVAL, config.RECONCILE_KLINES_LIMIT)
    earliest = rec.earliest_coverable_end(fetch_ms)
    cur_open = (fetch_ms // BAR) * BAR
    assert earliest - HOUR - 2 * HOUR == cur_open - (config.RECONCILE_KLINES_LIMIT - 1) * BAR
    assert rec.covers(earliest, fetch_ms) and not rec.covers(earliest - BAR, fetch_ms)
    import math
    ok, _ = rec._digest(rows, earliest - HOUR, earliest)
    assert len(ok) == 12 and all(math.isfinite(v) for v in ok.values()), ok
    short, _ = rec._digest(rows, earliest - BAR - HOUR, earliest - BAR)
    first = earliest - BAR - HOUR
    assert not (first in short and math.isfinite(short[first])), "再早一根的窗口第一根不應該算得出真實 ret2h"


# ============================== AC-2：紀錄保留有上限 ==============================
def test_ac2_records_bounded_after_24h_stall_and_pruned_when_batches_done():
    up = tsf._min_ret() + 0.03
    frames = {"PUMP": tsf.make_frame(700, tsf._LOOP_FIRST_OPEN, 191, events={302: up})}
    for i in range(3):
        frames["FLAT_%d" % i] = tsf.make_frame(700, tsf._LOOP_FIRST_OPEN, 193 + i, base_price=0.4 + i)
    wake = _wake(24)
    n = (wake - T0) // BAR + 3
    clock = FreezeClock(T0 - 60_000, [(_FREEZE_AT, wake)])
    ex = FakeExchange(clock, frames)
    feed, gate = tsf.build_feed(clock, ex, sorted(frames))
    rec = Reconciler(gate, clock=clock, **tsf._RECON_KW)
    rec.start = lambda stop, sc: None
    feed.reconciler = rec
    sizes = []
    orig_add = rec.add_bar

    def add_bar(res):
        orig_add(res)
        sizes.append(len(rec.records()))
    rec.add_bar = add_bar
    with capture_logs():
        feed.run(duration_s=(T0 + (n - 1) * BAR + 5_000 - clock.time_ms()) / 1000.0)
    feed.close()
    limit = config.RECONCILE_KLINES_LIMIT
    assert len(sizes) == n, (len(sizes), n)
    assert max(sizes) <= limit, "紀錄數量超過上限：max %d > %d" % (max(sizes), limit)
    kept = sorted(r.close_ms for r in rec.records())
    assert kept == [T0 + k * BAR for k in range(n - limit, n)], (len(kept), (kept[0] - T0) // BAR)
    # 排程處理完到期的窗口之後，那些窗口的紀錄就修掉
    with capture_logs() as cap:
        nxt = rec.catch_up(T0, gate.server_clock.now_ms())
    last_done = nxt - HOUR
    assert all(r.close_ms > last_done for r in rec.records()), [(r.close_ms - T0) // BAR for r in rec.records()]
    assert len(rec.records()) <= HOUR // BAR
    assert len(_coverage_warnings(cap)) == 1 and not _missing_errors(cap)


# ============================== AC-3：market_static 經過閘門 ==============================
SYMBOLS_PATH, RISK_TABLE_PATH = market_static.SYMBOLS_PATH, market_static.RISK_TABLE_PATH


def _fresh_ms():
    """重新載入 market_static（回到「從未 refresh」）。signal_feed.MarketUniverse 每次呼叫才 import 它，拿到的
    是同一個模組物件。"""
    return importlib.reload(market_static)


def _envelope(path, clock):
    data = {SYMBOLS_PATH: SYMBOLS_FIXTURE, RISK_TABLE_PATH: RISK_FIXTURE}[path]
    return {"result": True, "data": {"symbols": data}, "timestamp": clock.time_ms()}


def _feed_with_default_universe(gate, clock):
    """A1 的正式路徑：不給 universe_fn → SignalFeed 自己建 MarketUniverse(gate)。"""
    return signal_feed.SignalFeed(gate=gate, clock=clock, fg_executor=InlineExecutor(), bg_executor=InlineExecutor())


def test_ac3_universe_refresh_goes_through_gate_and_is_counted():
    ms = _fresh_ms()

    def bypass(*args, **kwargs):
        raise AssertionError("A1 的標的池刷新直接呼叫了 market_static.api_get（繞過閘門）")
    ms.api_get = bypass                              # 修改前：刷新走這支 → 失敗 3 次 → UniverseUnavailable
    clock = FakeClock(T0)
    calls = []

    def api(path, params=None, retries=3, timeout=20):
        calls.append((path, retries))
        return _envelope(path, clock)
    gate = _make_gate(clock, api)
    feed = _feed_with_default_universe(gate, clock)
    with capture_logs():
        syms = feed.refresh_universe(initial=True)
    assert syms == ["ABC_USDT_PERP", "BTC_USDT_PERP"], syms
    assert calls == [(SYMBOLS_PATH, 1), (RISK_TABLE_PATH, 1)], calls        # 閘門一律 retries=1
    st = gate.stats()
    assert st["requests"] == 2 and st["granted_by_priority"]["normal"] == 2, st
    assert [(r[2], r[1], r[4]) for r in gate.recent_requests()] == [(SYMBOLS_PATH, "normal", "ok"),
                                                                   (RISK_TABLE_PATH, "normal", "ok")]
    with capture_logs():
        feed._after_bar()                            # 未逾時：不送
    assert len(calls) == 2
    ms.last_refresh_mono = time.monotonic() - ms.STALE_SECONDS - 1
    with capture_logs():
        feed._after_bar()                            # 逾時：再刷一次，一樣經過閘門
    assert len(calls) == 4 and gate.stats()["requests"] == 4 and ms.last_refresh_ok is True


class _HttpResp:
    def __init__(self, status_code, js=None):
        self.status_code = status_code
        self._js = js
        self.text = "" if js is not None else "too many requests"

    def json(self):
        return self._js


def _with_fake_http(status_for_path, clock):
    """把 live.pionex_api 的 requests.get 與 time.sleep 換掉（閘門用真的 pionex_api.api_get）。
    status_for_path(path) → HTTP 狀態碼。回傳 (http 呼叫清單, sleep 清單, 還原函式)。"""
    http, slept = [], []

    def fake_get(url, params=None, headers=None, timeout=None):
        path = url[len(config.PIONEX_BASE_URL):]
        http.append(path)
        sc = status_for_path(path)
        return _HttpResp(sc, _envelope(path, clock) if sc == 200 else None)
    orig = (lh.requests.get, lh.time.sleep)
    lh.requests.get, lh.time.sleep = fake_get, slept.append

    def restore():
        lh.requests.get, lh.time.sleep = orig
    return http, slept, restore


def test_ac3_startup_refresh_429_is_exactly_one_request_and_trips_gate():
    """啟動時刷新吃到 429：HTTP 請求恰好 1 個（api_get 不在裡面退避重試、啟動的第 2、3 次嘗試落在冷卻中不送），
    閘門進入冷卻。修改前：market_static 直接呼叫 api_get（預設 retries=3）→ 同一個 429 送 3 次、睡 1、2 秒。"""
    _fresh_ms()
    clock = FakeClock(T0)
    gate = _make_gate(clock)                          # api_get 用預設（真的 live.pionex_api.api_get）
    feed = _feed_with_default_universe(gate, clock)
    http, slept, restore = _with_fake_http(lambda path: 429, clock)
    try:
        with capture_logs() as cap:
            try:
                feed.refresh_universe(initial=True)
            except signal_feed.UniverseUnavailable:
                pass
            else:
                raise AssertionError("標的池載入失敗應該拋 UniverseUnavailable")
    finally:
        restore()
    assert len(http) == 1, "吃到 429 之後又送了 %d 個請求：%s" % (len(http) - 1, http)
    assert slept == [], "api_get 在 429 後睡了 %s（代表它打算重試）" % slept
    st = gate.stats()
    assert st["requests"] == 1 and st["http_429"] == 1 and gate.limiter.bans == 1, st
    assert gate.limiter.ban_remaining() > 0
    assert sum(1 for m in cap.messages(logging.ERROR) if "429" in m) == 1     # 閘門那一行（含請求序列）


def test_ac3_periodic_refresh_429_is_one_request_and_nothing_during_ban():
    """已經載入過，逾時刷新吃到 429：只送 1 個（symbols），riskTable 不送、不重試；冷卻期間的刷新一個都不送，
    冷卻結束後恢復。修改前：同一個 429 送 3 次。"""
    ms = _fresh_ms()
    clock = FakeClock(T0)
    gate = _make_gate(clock)
    feed = _feed_with_default_universe(gate, clock)
    status = {"code": 200}
    http, slept, restore = _with_fake_http(lambda path: status["code"], clock)
    try:
        with capture_logs() as cap:
            assert feed.refresh_universe(initial=True) == ["ABC_USDT_PERP", "BTC_USDT_PERP"]
            assert len(http) == 2
            status["code"] = 429
            ms.last_refresh_mono = time.monotonic() - ms.STALE_SECONDS - 1
            feed._after_bar()
            assert http[2:] == [SYMBOLS_PATH], http              # 修改前：[symbols, symbols, symbols]
            assert gate.limiter.ban_remaining() > 0 and gate.stats()["http_429"] == 1
            clock.sleep(10)
            feed._after_bar()                                    # 冷卻中：不送
            assert len(http) == 3, http
            assert feed.universe() == ["ABC_USDT_PERP", "BTC_USDT_PERP"]      # 沿用舊清單
            status["code"] = 200
            clock.sleep(config.REST_BAN_COOLDOWN_SECONDS)
            feed._after_bar()                                    # 冷卻結束：恢復
            assert http[3:] == [SYMBOLS_PATH, RISK_TABLE_PATH] and ms.last_refresh_ok is True
    finally:
        restore()
    assert slept == []
    assert any("封鎖冷卻中" in m for m in cap.messages(logging.WARNING))


def test_ac3_market_static_injected_fetch_and_default_path():
    """market_static 的注入點：給 fetch 就只走它（不碰模組的 api_get）；它拋的例外照樣接住、保留舊快取、
    symbols 失敗就不打 riskTable；不給 fetch 走模組的 api_get（呼叫當下才查，monkeypatch 有效）。"""
    ms = _fresh_ms()
    default_calls = []

    def fake_api_get(path, params=None, retries=3, timeout=20):
        default_calls.append((path, retries))
        return _envelope(path, FakeClock(T0))
    ms.api_get = fake_api_get
    assert ms.refresh() is True and default_calls == [(SYMBOLS_PATH, 3), (RISK_TABLE_PATH, 3)]
    injected = []

    def fetch(path, params):
        injected.append((path, dict(params)))
        return _envelope(path, FakeClock(T0))
    assert ms.refresh(fetch=fetch) is True
    assert injected == [(SYMBOLS_PATH, ms.SYMBOLS_PARAMS), (RISK_TABLE_PATH, ms.RISK_TABLE_PARAMS)]
    assert len(default_calls) == 2
    assert ms.refresh_if_stale(fetch=fetch) is True and len(injected) == 2          # 未逾時不送

    def banned(path, params):
        injected.append((path, None))
        raise rest_gate.RestBanned(60.0, path)
    assert ms.refresh(fetch=banned) is False
    assert injected[-1] == (SYMBOLS_PATH, None) and len(injected) == 3               # riskTable 沒有打
    assert ms.status()["last_error"].startswith("RestBanned") and ms.max_leverage("BTC_USDT_PERP") == 100
    assert len(default_calls) == 2


def test_ac3_signal_feed_no_longer_preacquires_or_parses_429_strings():
    src = inspect.getsource(signal_feed.MarketUniverse)
    for bad in ("HTTP 429", "trip_ban", "limiter.acquire"):
        assert bad not in src, "MarketUniverse 還留著舊的補救：%s" % bad


# ============================== AC-4：degraded 的排除範圍 ==============================
def test_ac4_reason_constants_match_signal_feed():
    assert set(reconcile.FETCH_DEGRADED_REASONS) == set(signal_feed._DEGRADING_FETCH_FAILURES)
    assert reconcile.SCREEN_DEGRADED_REASONS == (
        signal_feed.UNSCREENED_COLD_START, signal_feed.UNSCREENED_TICKERS_FAILED, signal_feed.UNSCREENED_BANNED,
        signal_feed.DEGRADED_LATE_CLOSE_SAMPLE, signal_feed.DEGRADED_NO_BASE_OVER_CAP,
        signal_feed.DEGRADED_MISSED_CLOSE)
    assert reconcile._BANNED == signal_feed.UNSCREENED_BANNED == signal_feed.FETCH_BANNED     # 撞名是事實
    assert reconcile._MISSED_CLOSE == signal_feed.DEGRADED_MISSED_CLOSE
    # signal_feed 加 degraded 原因的地方全部用具名常數或上面分類過的變數；新增原因時這條會提醒要分類
    with open(signal_feed.__file__, encoding="utf-8") as f:
        src = f.read()
    assert 'add_degraded("' not in src and "add_degraded('" not in src
    args = set(re.findall(r"add_degraded\((\w+)\)", src))
    assert args == {"fail", "k", "UNSCREENED_COLD_START", "DEGRADED_LATE_CLOSE_SAMPLE", "DEGRADED_NO_BASE_OVER_CAP",
                    "DEGRADED_MISSED_CLOSE"}, args


def _bar_result(reasons=(), unscreened=None, fetch_failed=None, candidates=()):
    res = signal_feed.BarResult(bar_open_ms=T0 - BAR, bar_close_ms=T0, interval=config.KLINE_INTERVAL,
                                min_ret_2h=1.0, threshold=0.5)
    res.approx_ret2h = {"A": 0.1, "B": 0.2, "X": 0.9, "Z": 0.8}
    res.unscreened = dict(unscreened or {})
    res.candidates = [signal_feed.Candidate(s, res.approx_ret2h.get(s), "screen") for s in candidates]
    res.fetch_failed = dict(fetch_failed or {})
    res.degraded_reasons = list(reasons)
    return res


def test_ac4_record_classifies_degraded_scope_and_does_not_touch_bar_result():
    cases = [
        # (degraded_reasons, unscreened, fetch_failed, 只排除個別候選?)
        ((), None, None, False),
        (("api_error",), None, {"api_error": ["X"]}, True),
        (("target_missing", "exception"), None, {"target_missing": ["X"], "exception": ["Z"]}, True),
        (("banned",), None, {"banned": ["X", "Z"]}, True),                          # 候選 klines 被封鎖
        (("banned",), {"banned": ["A", "B", "X", "Z"]}, None, False),               # 收盤 tickers 被封鎖
        (("banned",), None, None, False),                                           # 結構看不出 → 保守
        (("tickers_failed",), {"tickers_failed": ["A", "B", "X", "Z"]}, None, False),
        (("cold_start", "api_error"), {"cold_start": ["B"]}, {"api_error": ["X"]}, False),
        (("late_close_sample",), None, None, False),
        (("no_base_over_cap", "api_error"), None, {"api_error": ["X"]}, False),
        (("missed_close",), {"missed_close": ["A", "B", "X", "Z"]}, None, False),
        (("some_future_reason",), None, None, False),                               # 不認得 → 保守
    ]
    for reasons, uns, ff, only in cases:
        res = _bar_result(reasons, uns, ff, candidates=("X", "Z") if ff else ())
        before = res.to_dict()
        rec = reconcile.record_from_result(res)
        assert res.to_dict() == before, "record_from_result 改了 BarResult（A3 也吃同一個物件）"
        assert rec.candidates_only is only, (reasons, uns, ff, rec.candidates_only)
        assert rec.degraded is bool(reasons) and rec.degraded_reasons == tuple(reasons)
        assert rec.excluded_whole is (bool(reasons) and not only)
        want = {s: k for k, syms in (ff or {}).items() for s in syms}
        assert rec.fetch_failed == want, (rec.fetch_failed, want)
        assert rec.missed_close is ("missed_close" in reasons)


_AC4_BAD_IDX = 390                     # EV_D 的真實訊號那根（test_signal_feed 的情境）
_AC4_WINDOW_END = tsf._close_of(399)   # = T0：窗口 (T0-1h, T0] = 第 388..399 根


def _ac4_window(fault):
    """test_signal_feed 的情境 K 棒 + 一個在第 390 根拉抬的候選 X_CAND。判定第 388..399 根、交給對帳。
    第 390 根：Y = EV_D 的收盤 tickers 價格壓低 1 成 → 近似 ret2h 落在門檻下、不在候選，但真實 ret2h >= MIN；
    fault 決定同一根另外發生什麼：
      api / klines_ban     X_CAND 的 klines 回 500 / 429（只影響這個候選）
      tickers_failed / tickers_ban
                           收盤那次 tickers 失敗 / 429（整根沒篩）
    回傳 (第 390 根的 BarResult, 對帳結果, 對帳時的日誌)。"""
    m = tsf._min_ret()
    frames = tsf._scenario_frames()
    frames["X_CAND"] = tsf.make_frame(420, tsf._SCEN_FIRST_OPEN, 300, events={_AC4_BAD_IDX: m + 0.03})
    bad_close = tsf._close_of(_AC4_BAD_IDX)
    clock = FakeClock(tsf._close_of(387) + 1000)
    ex = FakeExchange(clock, frames)
    ex.tickers_price_hook = (lambda sym, now, price:
                             price * 0.9 if sym == "EV_D" and bad_close <= now < bad_close + 5000 else price)
    if fault in ("api", "klines_ban"):
        code = 500 if fault == "api" else 429

        def fail(attempt, now, rows):
            if bad_close <= now < bad_close + 60_000:        # 只在判定時失敗，對帳時重抓正常
                raise lh.ApiError("/api/v1/market/klines: HTTP %d 模擬" % code, status_code=code)
            return rows
        ex.klines_hook["X_CAND"] = fail
    syms = sorted(frames)
    feed, gate = tsf.build_feed(clock, ex, syms)
    rec = Reconciler(gate, clock=clock, **tsf._RECON_KW)
    bad = None
    with capture_logs():
        feed.start_seeding(syms, initial=True)
        for idx in range(388, 400):
            close = tsf._close_of(idx)
            tsf.goto(clock, close + 200)
            if idx == _AC4_BAD_IDX and fault == "tickers_failed":
                err = lh.ApiError("/api/v1/market/tickers: HTTP 502 bad gateway", status_code=502)
                ex.tickers_fail = [err] * config.TICKERS_CLOSE_ATTEMPTS
            if idx == _AC4_BAD_IDX and fault == "tickers_ban":
                ex.tickers_fail = [lh.ApiError("/api/v1/market/tickers: 重試 1 次仍失敗", status_code=429)]
            res = feed.judge_bar(close)
            rec.add_bar(res)
            if idx == _AC4_BAD_IDX:
                bad = res
    with capture_logs() as cap:
        rr = rec.run_batch(_AC4_WINDOW_END)
    return bad, rr, cap


def _misses(rr):
    return [(m["symbol"], m["bar_close_ms"]) for m in rr.misses]


def test_ac4_candidate_fetch_failure_only_excludes_that_candidate():
    """候選 X_CAND 取數失敗（degraded：api_error），Y = EV_D 真實 ret2h >= MIN 但不在候選 → Y 算漏失（ERROR），
    X_CAND 個別排除。修改前：整根 degraded → Y 被算進 misses_on_degraded（misses == []、沒有 ERROR）。"""
    bad, rr, cap = _ac4_window("api")
    bad_close = tsf._close_of(_AC4_BAD_IDX)
    assert _misses(rr) == [("EV_D", bad_close)], (_misses(rr), rr.misses_on_degraded)
    assert rr.misses_on_degraded == 0
    errs = cap.messages(logging.ERROR)
    assert len(errs) == 1 and "EV_D" in errs[0] and "漏失 1 筆" in errs[0], errs
    # 情境本身：那一根確實只因為 X_CAND 取數失敗而 degraded，Y 確實不在候選
    assert bad.degraded_reasons == [signal_feed.FETCH_API_ERROR] and bad.fetch_failed == {"api_error": ["X_CAND"]}
    assert "EV_D" not in {c.symbol for c in bad.candidates} and "X_CAND" in {c.symbol for c in bad.candidates}
    # X_CAND：個別排除（它在候選清單裡，不會算成漏失），真實 ret2h 也夠高 → 「可能的訊號沒判定」
    assert [(e["symbol"], e["bar_close_ms"], e["why"]) for e in rr.excluded_fetch_failed] == \
        [("X_CAND", bad_close, "api_error")], rr.excluded_fetch_failed
    assert rr.excluded_fetch_failed[0]["true_ret2h"] >= tsf._min_ret() and rr.excluded_hits() == 1
    assert (rr.bars_degraded, rr.bars_degraded_whole, rr.bars_degraded_partial) == (1, 0, 1)
    summary = [m for m in cap.messages(logging.INFO) if m.startswith("背景對帳 ")]
    assert summary and "只排除取數失敗的候選 1" in summary[-1] and "個別排除取數失敗的候選 1 組" in summary[-1], summary


def test_ac4_candidate_klines_banned_is_also_only_that_candidate():
    """候選 klines 吃到 429（FETCH_BANNED，字串也是 "banned"）→ 仍是個別排除：Y 算漏失。
    修改前：misses_on_degraded。"""
    bad, rr, cap = _ac4_window("klines_ban")
    bad_close = tsf._close_of(_AC4_BAD_IDX)
    assert _misses(rr) == [("EV_D", bad_close)], (_misses(rr), rr.misses_on_degraded)
    assert rr.misses_on_degraded == 0
    assert bad.degraded_reasons == ["banned"] and bad.fetch_failed.get("banned") == ["X_CAND"]
    assert not bad.unscreened.get("banned")
    assert [(e["symbol"], e["why"]) for e in rr.excluded_fetch_failed] == [("X_CAND", "banned")]
    assert (rr.bars_degraded_whole, rr.bars_degraded_partial) == (0, 1)


def test_ac4_screen_affecting_degraded_still_excludes_whole_bar():
    """收盤 tickers 失敗 / 被封鎖（影響粗篩）→ 整根排除，同修改前：不算漏失、沒有 ERROR，
    那一根上真實 ret2h >= MIN 的（EV_D、X_CAND）都進 misses_on_degraded。
    前半段只用修改前就有的欄位（修改前的程式應該通過）；新欄位的斷言在後半段。"""
    runs = []
    for fault, reason in (("tickers_failed", signal_feed.UNSCREENED_TICKERS_FAILED),
                          ("tickers_ban", signal_feed.UNSCREENED_BANNED)):
        bad, rr, cap = _ac4_window(fault)
        assert rr.misses == [] and rr.misses_on_degraded == 2, (fault, _misses(rr), rr.misses_on_degraded)
        assert not cap.messages(logging.ERROR), (fault, cap.messages(logging.ERROR))
        assert bad.degraded_reasons == [reason] and len(bad.unscreened[reason]) == bad.universe_size
        assert rr.bars_degraded == 1
        runs.append((fault, rr))
    for fault, rr in runs:                                     # A1-f 新增的欄位
        assert rr.excluded_fetch_failed == [] and (rr.bars_degraded_whole, rr.bars_degraded_partial) == (1, 0), \
            (fault, rr.excluded_fetch_failed, rr.bars_degraded_whole, rr.bars_degraded_partial)


# ============================== 不用 pytest 也能跑 ==============================
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
