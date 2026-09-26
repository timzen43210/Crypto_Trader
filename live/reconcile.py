# -*- coding: utf-8 -*-
"""
live.reconcile — A1 的背景對帳（FR-10）：把「粗篩漏失率」變成持續監控的數字
============================================================================
近似 ret2h 的粗篩一旦開始漏訊號，不會有任何症狀：系統正常、日誌乾淨、只是某些訊號從來沒出現。
tickers 行為改變、新幣上架、網路品質變差都可能讓它開始漏。所以 A1 每隔一段時間，用 klines
重算這段期間每根 K 棒、每個 symbol 的**真實** ret2h，與當時的粗篩紀錄逐一比對。

做法（captain 定案：每小時批次，不是 R-6 原設想的 4 req/s 持續掃描）：
  * 每 RECONCILE_INTERVAL_SECONDS（預設 3600）一批，對帳區間 = (R - 1 小時, R]，R 是該間隔的整數倍
    （伺服器時間），批次在 R + RECONCILE_START_DELAY_SECONDS 開始，讓區間最後一根先判定完
  * 對這段期間出現過的每個 symbol 打一次 klines（不帶 endTime，limit RECONCILE_KLINES_LIMIT），
    整理規則與候選判定完全相同（live.klines.prepare_klines，t_now = R），ret2h 直接取
    s4_signal.features() 的輸出 —— 不重抄公式
  * 請求攤平在 RECONCILE_SPREAD_FRACTION x 間隔內（約 561 個 / 48 分鐘 ≈ 0.2 req/s），走共用閘門、
    PRIORITY_BACKGROUND：K 棒收盤前後的前景保留期間不送、有前景請求在等時讓它先。
    行程停頓後醒來、進度落後超過一格時，剩下的 symbol 重新攤平到窗口剩餘的時間，不集中補打
  * 比對（取數全部做完後，用**當下**的粗篩紀錄比；停頓醒來那一刻補上的 missed_close 紀錄也算得到）：
      應有根數     區間內、feed 運作期間應有的每一根收盤。完全沒有紀錄的根 > 0 記 ERROR，列出
      未判定       missed_close 或完全沒有紀錄的根上，真實 ret2h >= MIN_RET_2H 的 (symbol, K 棒)。
                   與漏失分開計（那幾根根本沒有粗篩），> 0 記 WARNING，列出
      漏失         真實 ret2h >= MIN_RET_2H，但當時不在候選清單，且該根非 degraded → > 0 記 ERROR，列出
      近似誤差     |近似 - 真實| 的 p50 / p99 / max（有近似值的 (symbol, K 棒) 全部算）→ INFO
      K 棒定稿     當時判定用的目標 K 棒 OHLCV vs 對帳時重抓的同一根 → 不一致 > 0 記 WARNING。
                   兩邊的字串一律用 live.klines.ohlcv_by_open / prepare_klines 的同一個轉法（pd.to_numeric），
                   不會因為 float() 與 pd.to_numeric 差 1 ULP 而誤報。這就是上線後 K 棒定稿的持續監控
  * 不回頭補發訊號（晚一小時的訊號沒有意義），只記錄與告警。日後 A4 接這裡的 ERROR。

需要的粗篩紀錄只放記憶體：每根 K 棒一筆 BarRecord（每個 symbol 的近似 ret2h、候選、未篩原因、
判定用的目標 K 棒），保留約兩個對帳間隔。
"""

import logging
import math
import threading
from dataclasses import dataclass, field

import numpy as np

from live import config, klines, rest_gate
from live.pionex_api import ApiError
from strategy import s4_signal

logger = logging.getLogger(__name__)

_MISSED_CLOSE = "missed_close"     # 與 live.signal_feed.DEGRADED_MISSED_CLOSE 相同（不 import 它，避免循環相依）


@dataclass(frozen=True)
class BarRecord:
    """一根 K 棒當時的粗篩紀錄（從 BarResult 摘出來，對帳只需要這些）。"""
    close_ms: int
    degraded: bool
    universe: frozenset
    candidates: frozenset
    approx: dict            # symbol → 近似 ret2h
    unscreened: dict        # symbol → 未篩原因
    target_bars: dict       # symbol → (o, h, l, c, v)：當時實際判定的目標 K 棒
    missed_close: bool = False


def record_from_result(res):
    """signal_feed.BarResult → BarRecord。"""
    unscreened = {s: reason for reason, syms in res.unscreened.items() for s in syms}
    universe = frozenset(res.approx_ret2h) | frozenset(unscreened)
    return BarRecord(
        close_ms=int(res.bar_close_ms),
        degraded=bool(res.degraded),
        universe=universe,
        candidates=frozenset(c.symbol for c in res.candidates),
        approx=dict(res.approx_ret2h),
        unscreened=unscreened,
        target_bars={s: tuple(ev["target_bar"]) for s, ev in res.judged.items() if ev.get("target_bar")},
        missed_close=_MISSED_CLOSE in res.degraded_reasons,
    )


@dataclass
class ReconcileResult:
    window_start_ms: int
    window_end_ms: int
    min_ret_2h: float
    bars: int = 0
    bars_degraded: int = 0
    bars_expected: int = 0                                 # 區間內 feed 運作期間應有的根數
    bars_missing: list = field(default_factory=list)       # 應有但完全沒有紀錄的收盤時刻
    bars_missed_close: list = field(default_factory=list)  # 有紀錄但是 missed_close 的收盤時刻
    symbols: int = 0
    requests: int = 0
    respread: int = 0                                      # 落後後重新攤平了幾次
    fetch_failed: dict = field(default_factory=dict)       # 原因 → [symbol]
    pairs_compared: int = 0                                # 有真實 ret2h 的 (symbol, K 棒)，不含未判定的根
    true_unavailable: int = 0                              # 重抓的資料算不出真實 ret2h 的 (symbol, K 棒)
    misses: list = field(default_factory=list)             # 非 degraded 根上的漏失明細
    misses_on_degraded: int = 0
    undetermined: list = field(default_factory=list)       # missed_close / 沒紀錄的根上真實 ret2h >= MIN 的
    abs_error: dict = field(default_factory=dict)          # n / p50 / p99 / max
    ohlcv_checked: int = 0
    ohlcv_mismatches: list = field(default_factory=list)
    ohlcv_unverifiable: int = 0
    send_monotonic: list = field(default_factory=list)     # 每個對帳請求領到額度的單調時刻（診斷 / 測試）
    aborted: bool = False
    elapsed_s: float = 0.0

    def to_dict(self):
        d = dict(self.__dict__)
        d.pop("send_monotonic", None)
        return d


class Reconciler:
    """背景對帳。add_bar() 收每根 K 棒的結果；start() 開背景執行緒每個間隔跑一批；
    run_batch() 可直接同步呼叫（測試用）。所有等待經過 clock，測試可注入假時鐘。

    「應有根數」從哪一根算起：SignalFeed.run() 會呼叫 expect_bars_from(第一根該處理的收盤)；
    沒有呼叫過（例如測試直接餵紀錄）就從收過的最早那一根算起。
    """

    def __init__(self, gate, *, clock=None, interval=None, interval_s=None, spread_fraction=None,
                 klines_limit=None, start_delay_s=None, params_fn=None, on_result=None):
        self.gate = gate
        self.clock = clock or rest_gate.SystemClock()
        self.interval = interval or config.KLINE_INTERVAL
        self.bar_ms = klines.interval_ms(self.interval)
        self.bph = klines.bars_per_hour(self.interval)
        self.interval_s = float(config.RECONCILE_INTERVAL_SECONDS if interval_s is None else interval_s)
        self.spread_fraction = float(config.RECONCILE_SPREAD_FRACTION if spread_fraction is None
                                     else spread_fraction)
        self.klines_limit = int(config.RECONCILE_KLINES_LIMIT if klines_limit is None else klines_limit)
        self.start_delay_s = float(config.RECONCILE_START_DELAY_SECONDS if start_delay_s is None
                                   else start_delay_s)
        self._params_fn = params_fn or config.strategy_params
        self._on_result = on_result
        self._lock = threading.Lock()
        self._records = []
        self._expect_from = None
        self._first_close_seen = None
        self._thread = None
        self._stop = None

    # ---------------- 紀錄 ----------------
    def expect_bars_from(self, close_ms):
        """feed 從這一根收盤開始負責（之前的根不算「應有」）。"""
        with self._lock:
            self._expect_from = int(close_ms)

    def add_bar(self, res):
        rec = record_from_result(res)
        keep_after = rec.close_ms - int(2 * self.interval_s * 1000) - self.bar_ms
        with self._lock:
            self._records = [r for r in self._records if r.close_ms > keep_after and r.close_ms != rec.close_ms]
            self._records.append(rec)
            if self._first_close_seen is None or rec.close_ms < self._first_close_seen:
                self._first_close_seen = rec.close_ms

    def records(self):
        with self._lock:
            return list(self._records)

    def _window_records(self, start_ms, end_ms):
        with self._lock:
            return sorted((r for r in self._records if start_ms < r.close_ms <= end_ms), key=lambda r: r.close_ms)

    def expected_closes(self, start_ms, end_ms):
        """(start_ms, end_ms] 內、feed 運作期間應有的每一根收盤時刻。"""
        with self._lock:
            base = self._expect_from if self._expect_from is not None else self._first_close_seen
        if base is None:
            return []
        first = max(base, (start_ms // self.bar_ms + 1) * self.bar_ms)
        first = -(-first // self.bar_ms) * self.bar_ms          # 對齊 K 棒邊界
        return list(range(first, end_ms + 1, self.bar_ms))

    # ---------------- 一批 ----------------
    def run_batch(self, window_end_ms, stop_event=None):
        """對帳 (window_end_ms - 間隔, window_end_ms] 內的 K 棒。回傳 ReconcileResult（也交給 on_result）。"""
        window_end_ms = int(window_end_ms)
        window_start_ms = window_end_ms - int(self.interval_s * 1000)
        recs = self._window_records(window_start_ms, window_end_ms)
        expected = self.expected_closes(window_start_ms, window_end_ms)
        params = self._params_fn()
        result = ReconcileResult(window_start_ms, window_end_ms, params["MIN_RET_2H"])
        t0 = self.clock.monotonic()
        if not recs and not expected:
            logger.info("背景對帳 %s：區間內沒有粗篩紀錄，略過", _span(window_start_ms, window_end_ms))
            return result
        source = recs or self.records()              # 區間內一根紀錄都還沒有時，用最近的標的池
        symbols = sorted(set().union(*(r.universe for r in source))) if source else []
        result.symbols = len(symbols)

        # ---- 取數：攤平在 span 內；落後超過一格就把剩下的重新攤平到剩餘時間 ----
        digests = {}
        n = len(symbols)
        span = self.interval_s * self.spread_fraction
        deadline = t0 + span
        base_t, base_i, step = t0, 0, (span / n if n else 0.0)
        for i, sym in enumerate(symbols):
            if stop_event is not None and stop_event.is_set():
                result.aborted = True
                break
            target = base_t + (i - base_i) * step
            self._sleep_until(target, stop_event)
            # 停頓發生在睡覺或上一個請求期間，所以醒來之後才檢查：落後超過一格 → 從這一個起，
            # 剩下的重新攤平到期限前（step 已經是 0、期限已過就不再重排）
            now = self.clock.monotonic()
            if i and step > 0 and now > target + step:
                left = n - i
                step = max(0.0, deadline - now) / left
                base_t, base_i = now, i
                result.respread += 1
                logger.info("背景對帳 %s：進度落後（行程停頓？），剩下 %d 個 symbol 重新攤平到 %.0f 秒內",
                            _span(window_start_ms, window_end_ms), left, max(0.0, deadline - now))
            rows, fail = self._fetch(sym, result, stop_event)
            if rows is None:
                if fail is None:          # 被 stop 打斷
                    result.aborted = True
                    break
                result.fetch_failed.setdefault(fail, []).append(sym)
                continue
            try:
                digests[sym] = self._digest(rows, window_start_ms, window_end_ms)
            except Exception as e:  # noqa: BLE001 —— 單一 symbol 的資料怪異不可以讓整批停掉
                result.fetch_failed.setdefault("exception", []).append(sym)
                logger.error("背景對帳 %s 整理資料時發生未預期的例外：%s", sym, e,
                             exc_info=(type(e), e, e.__traceback__))

        # ---- 比對：用現在的紀錄（取數期間補上的 missed_close 也算得到）----
        recs = self._window_records(window_start_ms, window_end_ms)
        recorded = {r.close_ms for r in recs}
        result.bars = len(recs)
        result.bars_degraded = sum(1 for r in recs if r.degraded)
        result.bars_expected = len(expected)
        result.bars_missing = [c for c in expected if c not in recorded]
        result.bars_missed_close = [r.close_ms for r in recs if r.missed_close]
        errors = []
        for sym, digest in digests.items():
            self._compare(sym, digest, recs, result.bars_missing, params["MIN_RET_2H"], result, errors)
        result.undetermined.sort(key=lambda u: (u["bar_close_ms"], u["symbol"]))
        if errors:
            arr = np.asarray(errors, dtype=float)
            result.abs_error = {"n": int(arr.size), "p50": float(np.percentile(arr, 50)),
                                "p99": float(np.percentile(arr, 99)), "max": float(arr.max())}
        else:
            result.abs_error = {"n": 0, "p50": None, "p99": None, "max": None}
        result.elapsed_s = self.clock.monotonic() - t0
        self._log(result)
        if self._on_result is not None:
            try:
                self._on_result(result)
            except Exception:  # noqa: BLE001
                logger.exception("背景對帳 on_result 回呼失敗")
        return result

    def _fetch(self, sym, result, stop_event):
        """回傳 (rows, None) 或 (None, 失敗原因)；被 stop 打斷回 (None, None)。封鎖冷卻不算嘗試，等它過。
        API 錯誤隔 2 秒再試一次（停頓剛醒來時 DNS 常常還沒恢復，立刻重試只會一起失敗）。"""
        attempts = 0
        last = None
        while attempts < 2:
            if stop_event is not None and stop_event.is_set():
                return None, None
            try:
                rows = klines.fetch_klines(self.gate, sym, self.interval, self.klines_limit,
                                           priority=rest_gate.PRIORITY_BACKGROUND)
                result.requests += 1
                result.send_monotonic.append(self.clock.monotonic())
                return rows, None
            except rest_gate.RestBanned as e:
                self._sleep_until(self.clock.monotonic() + e.remaining_seconds + 0.05, stop_event)
            except ApiError as e:
                result.requests += 1
                attempts += 1
                last = e
                if attempts < 2:
                    self._sleep_until(self.clock.monotonic() + 2.0, stop_event)
            except Exception as e:  # noqa: BLE001
                logger.error("背景對帳取 %s 的 klines 時發生未預期的例外：%s", sym, e,
                             exc_info=(type(e), e, e.__traceback__))
                return None, "exception"
        logger.warning("背景對帳取 %s 的 klines 失敗：%s", sym, last)
        return None, "api_error"

    def _digest(self, rows, window_start_ms, window_end_ms):
        """一個 symbol 的 klines → (開盤時刻 → 真實 ret2h, 開盤時刻 → {OHLCV})，只留區間內的根。"""
        df, _ = klines.prepare_klines(klines.raw_frame(rows), self.bar_ms, t_now=window_end_ms)
        true_by_open = {}
        if df is not None and len(df):
            feats = s4_signal.features(df, self.bph)
            for t, r in zip((int(t) for t in df["time"]), feats["ret2h"].tolist()):
                if window_start_ms < t + self.bar_ms <= window_end_ms:
                    true_by_open[t] = r
        raw = {t: v for t, v in klines.ohlcv_by_open(rows).items()
               if window_start_ms < t + self.bar_ms <= window_end_ms}
        return true_by_open, raw

    def _compare(self, sym, digest, recs, missing, min_ret, result, errors):
        true_by_open, raw = digest
        for rec in recs:
            if sym not in rec.universe:
                continue
            target_open = rec.close_ms - self.bar_ms
            true = true_by_open.get(target_open)
            valid = true is not None and math.isfinite(true)
            if rec.missed_close:
                if valid and true >= min_ret:
                    result.undetermined.append({"symbol": sym, "bar_close_ms": rec.close_ms, "true_ret2h": true,
                                                "why": _MISSED_CLOSE})
                continue
            if not valid:
                result.true_unavailable += 1
            else:
                result.pairs_compared += 1
                approx = rec.approx.get(sym)
                if approx is not None:
                    errors.append(abs(approx - true))
                if true >= min_ret and sym not in rec.candidates:
                    if rec.degraded:
                        result.misses_on_degraded += 1
                    else:
                        result.misses.append({
                            "symbol": sym, "bar_close_ms": rec.close_ms, "true_ret2h": true,
                            "approx_ret2h": approx,
                            "why": "below_threshold" if approx is not None else rec.unscreened.get(sym, "unknown"),
                        })
            tb = rec.target_bars.get(sym)
            if tb is not None:
                seen = raw.get(target_open)
                if not seen:
                    result.ohlcv_unverifiable += 1
                else:
                    result.ohlcv_checked += 1
                    if seen != {tuple(tb)}:
                        result.ohlcv_mismatches.append({"symbol": sym, "bar_close_ms": rec.close_ms,
                                                        "at_close": list(tb), "refetched": sorted(seen)})
        for close in missing:
            true = true_by_open.get(close - self.bar_ms)
            if true is not None and math.isfinite(true) and true >= min_ret:
                result.undetermined.append({"symbol": sym, "bar_close_ms": close, "true_ret2h": true,
                                            "why": "no_record"})

    def _log(self, r):
        span = _span(r.window_start_ms, r.window_end_ms)
        e = r.abs_error
        logger.info("背景對帳 %s：應有 %d 根、有紀錄 %d 根（degraded %d，其中 missed_close %d）、缺紀錄 %d 根、"
                    "symbol %d、比對 %d 組（算不出真實值 %d）、漏失 %d（degraded 根上另有 %d）、未判定 %d、"
                    "近似誤差 n=%s p50=%s p99=%s max=%s、K 棒定稿比對 %d 筆不一致 %d（無法比對 %d）、"
                    "取數失敗 %s、請求 %d、重新攤平 %d 次、耗時 %.0fs%s",
                    span, r.bars_expected, r.bars, r.bars_degraded, len(r.bars_missed_close), len(r.bars_missing),
                    r.symbols, r.pairs_compared, r.true_unavailable, len(r.misses), r.misses_on_degraded,
                    len(r.undetermined), e.get("n"), _fmt(e.get("p50")), _fmt(e.get("p99")), _fmt(e.get("max")),
                    r.ohlcv_checked, len(r.ohlcv_mismatches), r.ohlcv_unverifiable,
                    {k: len(v) for k, v in r.fetch_failed.items()}, r.requests, r.respread, r.elapsed_s,
                    "（中途停止）" if r.aborted else "")
        if r.bars_missing:
            logger.error("背景對帳 %s：%d 根應有的 K 棒完全沒有紀錄（沒有 BarResult，也不是 missed_close）：%s",
                         span, len(r.bars_missing), [_hhmm(c) for c in r.bars_missing])
        if r.bars_missed_close:
            logger.warning("背景對帳 %s：%d 根是 missed_close（收盤時行程沒在處理）：%s",
                           span, len(r.bars_missed_close), [_hhmm(c) for c in r.bars_missed_close])
        if r.undetermined:
            logger.warning("背景對帳 %s：未判定 %d 筆（missed_close / 沒紀錄的根上真實 ret2h >= MIN_RET_2H %s，"
                           "與漏失分開計）：%s", span, len(r.undetermined), r.min_ret_2h,
                           [(u["symbol"], _hhmm(u["bar_close_ms"]), round(u["true_ret2h"], 5), u["why"])
                            for u in r.undetermined])
        if r.misses:
            logger.error("背景對帳 %s：漏失 %d 筆（真實 ret2h >= MIN_RET_2H %s 但不在候選，K 棒非 degraded）：%s",
                         span, len(r.misses), r.min_ret_2h,
                         [(m["symbol"], _hhmm(m["bar_close_ms"]), round(m["true_ret2h"], 5),
                           None if m["approx_ret2h"] is None else round(m["approx_ret2h"], 5), m["why"])
                          for m in r.misses])
        if r.ohlcv_mismatches:
            logger.warning("背景對帳 %s：收盤後判定用的目標 K 棒與重抓不一致 %d 筆：%s",
                           span, len(r.ohlcv_mismatches), r.ohlcv_mismatches[:10])
        if r.fetch_failed:
            logger.warning("背景對帳 %s：取數失敗 %s", span, r.fetch_failed)

    # ---------------- 背景執行緒 ----------------
    def start(self, stop_event, server_clock):
        """開背景執行緒：每個間隔結束後跑一批。stop_event 設起來就在下一個請求前停。"""
        if self._thread is not None:
            return
        self._stop = stop_event
        self._thread = threading.Thread(target=self._loop, args=(stop_event, server_clock),
                                        name="a1-reconcile", daemon=True)
        self._thread.start()

    def join(self, timeout=None):
        if self._thread is not None:
            self._thread.join(timeout)

    def _loop(self, stop, server_clock):
        interval_ms = int(self.interval_s * 1000)
        while not stop.is_set():
            window_end = (server_clock.now_ms() // interval_ms + 1) * interval_ms
            start_at = window_end + self.start_delay_s * 1000
            while not stop.is_set():
                remaining = (start_at - server_clock.now_ms()) / 1000.0
                if remaining <= 0:
                    break
                self.clock.sleep(min(remaining, 0.5))
            if stop.is_set():
                break
            try:
                self.run_batch(window_end, stop)
            except Exception:  # noqa: BLE001 —— 一批出錯不可以讓之後的對帳全部停掉
                logger.exception("背景對帳這一批發生未預期的例外，下一個間隔再試")

    def _sleep_until(self, mono_t, stop_event):
        while stop_event is None or not stop_event.is_set():
            remaining = mono_t - self.clock.monotonic()
            if remaining <= 0:
                return
            self.clock.sleep(min(remaining, 0.5))


def _span(a, b):
    return f"{_hhmm(a)}–{_hhmm(b)}"


def _hhmm(ms):
    from datetime import datetime
    from live.logsetup import TAIPEI
    return datetime.fromtimestamp(ms / 1000.0, TAIPEI).strftime("%m-%d %H:%M")


def _fmt(x):
    return "None" if x is None else "%.5f" % x
