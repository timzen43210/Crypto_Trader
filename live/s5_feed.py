# -*- coding: utf-8 -*-
"""
live.s5_feed — A 頻道策略五資料層（A5）：每分鐘判定策略5，把原始訊號送進 A3
=============================================================================
整條路：

    與 A1（live.signal_feed.SignalFeed）共用閘門、價格緩衝、標的池，**不另開 tickers 輪詢**
      → 每小時一次 ① 粗篩：approx = price_at(H:00) ÷ price_at(H−1:00) − 1（樣本來自 A1 的價格緩衝）
         approx >= MIN_RISE_FROM_OPEN − S5_SCREEN_RISE_MARGIN 的幣才是本小時的候選（門檻呼叫當下推導）
      → 每根 1 分K 收盤 T，在 T + BAR_FINALIZE_WAIT_SECONDS 對「仍在追蹤中」的候選各打一次 1M klines
      → prepare_klines(raw_frame(rows), bar_ms, t_now=T) → s5_signal.evaluate(df, bar_ms, params)
      → 本小時第一個 signal = -1 的那根（first_per_hour 已由 s5_signal 套用），本小時還沒送過就
         tracker.submit_signal(strategy="s5", ...)，K 棒時刻一律用那根原本的時刻（補送也一樣）

規則與參數只有 strategy/s5_signal 一份（本模組不複製任何數值）；資料處理用 live.klines，與 A1、dry run 相同。

──────────────────────────────────────────────────────────────────────
一小時的生命週期
──────────────────────────────────────────────────────────────────────
小時 H 的 1 分K 是第 1～60 分（第 m 分 = 收盤在 H:00 + m 分）。第 60 分的那根在 H+1:00 收盤，**屬於 H**；
同一刻也要做 H+1 的粗篩，兩件事都做（先篩再取數，H 的候選照常取第 60 分）。
  * 粗篩：H 內第一次處理到的分鐘做（正常是 H:00 那一刻，行程停頓就是醒來後第一分鐘）。兩端任一沒有樣本
    （冷啟動、種子還沒補完）記「未粗篩」，H 內每分鐘重試；到 H 結束仍沒有樣本的，該小時該幣記 degraded。
    tickers 本來就沒有、或 tickers 的價格不合法的幣，比照 A1 記未粗篩但不算 degraded。
  * 追蹤：候選在 H 內第一次成功判定時，前一小時不完整或精確 ① 不成立 → 本小時不再取數；送出訊號 → 本小時
    不再取數；klines 回 MARKET_INVALID_SYMBOL → 本小時不再取數。其餘下一分鐘繼續。
  * 補送：本小時的第一個訊號比這次的目標 K 棒早（該分鐘 missed、封鎖、取數失敗、粗篩晚了）也照送，
    K 棒時刻用原本那根；日誌標「補送、K 棒 X、晚了 N 秒」。超過 5 分鐘的由 T2 標 expired，不在這裡擋。
  * 不跨小時回判：只看目標 K 棒所屬小時的訊號。整個小時都錯過的，只記 missed / degraded。

──────────────────────────────────────────────────────────────────────
閘門與 A1（FR-6）
──────────────────────────────────────────────────────────────────────
A5 的 klines 走共用閘門、優先權 PRIORITY_FOREGROUND（與 A1 候選同級）。**不使用 foreground_hold()**：
A5 每分鐘都要取數，每分鐘拿一次 hold 等於一小時有大半時間把補種子（NORMAL）與背景對帳（BACKGROUND）擋在門外。
改成「讓 A1 先做」：送出前若 A1 正持有 foreground_hold（5M 收盤前 1 秒起到判定完成），先等它放掉，最多等
S5_YIELD_MAX_SECONDS。於是 5M 收盤那一刻 A1 的候選 klines 永遠排在 A5 前面，A1 的延遲不因 A5 變差；
A5 自己的取數最多晚零點幾秒。讓完才做粗篩，也保證 H:00 那次 A1 的收盤 tickers 樣本已經進了緩衝。
RestBanned：該分鐘記 degraded（banned），不在冷卻中硬等；冷卻結束後照補送規則送。

──────────────────────────────────────────────────────────────────────
例外隔離（FR-1）
──────────────────────────────────────────────────────────────────────
A5 在自己的執行緒跑。粗篩、取數判定、每個幣的取數與計算、on_result 回呼、每小時摘要各自包 try，未預期的
例外記 ERROR（含 traceback）後繼續。整條執行緒意外結束時記 ERROR、stats()["worker_alive"] 變 False，
不自動重啟，也不影響 A1 / A3。

──────────────────────────────────────────────────────────────────────
對外介面
──────────────────────────────────────────────────────────────────────
    s5 = S5Feed(gate=feed.gate, buffer=feed.buffer, universe_fn=feed.universe,
                submit_fn=tracker.submit_signal, clock=feed.clock, tickers_fn=feed.latest_tickers)
    s5.start()          # 自己的執行緒（名稱 a5-s5-feed）
    ...
    s5.close()          # 停止並等執行緒收尾（最多 S5_JOIN_TIMEOUT_SECONDS 秒）
    s5.stats()          # 累計數字（給總結與日後的 A4）

on_result(MinuteResult) 每分鐘呼叫一次（含 missed 的分鐘）；回呼拋例外只記 ERROR。
judge_rows(rows, close_ms, params) 是不打網路的純函式，回放（AC-6）直接用它或用假閘門跑 S5Feed。
"""

import concurrent.futures
import logging
import threading
from dataclasses import dataclass, field

from live import config, klines, rest_gate
from live.pionex_api import MARKET_INVALID_SYMBOL, ApiError
from live.signal_feed import (UNSCREENED_NOT_IN_TICKERS, UNSCREENED_TICKER_INVALID, _duration, _num, _pct, _taipei,
                              _taipei_day, _taipei_s)
from strategy import s5_signal

logger = logging.getLogger(__name__)

STRATEGY = "s5"
HOUR_MS = s5_signal.HOUR_MS
MINUTE_MS = s5_signal.MINUTE_MS

# 每分鐘結果的 degraded 原因
DEGRADED_MISSED = "missed"                 # 開始處理時已比收盤晚超過 MISSED_CLOSE_TOLERANCE_SECONDS，不取數
DEGRADED_BANNED = "banned"                 # 429 封鎖冷卻中（或這一分鐘收到 429）
DEGRADED_API_ERROR = "api_error"           # 派網回錯誤 / 連線失敗，重試用盡
DEGRADED_TARGET_MISSING = "target_missing"  # 重試用盡，回應裡仍沒有目標 K 棒
DEGRADED_UNSCREENED = "unscreened"         # 目標小時還有幣因為沒有價格樣本而未粗篩
DEGRADED_EXCEPTION = "exception"           # 未預期的例外

# 取數結果（失敗的沿用上面的 degraded 原因字串）
FETCH_OK = "ok"
FETCH_INVALID_SYMBOL = "invalid_symbol"    # klines 回 MARKET_INVALID_SYMBOL：不重試、不算 degraded

# 未粗篩原因。只有 no_sample 算 degraded（另外兩個沿用 A1 的定義：tickers 本來就沒有 / 價格不合法）
UNSCREENED_NO_SAMPLE = "no_sample"

# 本小時停止追蹤的原因
STOP_INCOMPLETE = "prev_hour_incomplete"   # 前一小時不完整（補格後不足 60 根）
STOP_RISE_BELOW = "rise_below"             # 精確 ① 不成立
STOP_SENT = "sent"                         # 本小時已送出訊號
STOP_INVALID_SYMBOL = "invalid_symbol"     # klines 回 MARKET_INVALID_SYMBOL


def screen_threshold(params=None):
    """① 粗篩門檻 = params 的 MIN_RISE_FROM_OPEN − S5_SCREEN_RISE_MARGIN（呼叫當下推導，不寫死）。"""
    p = s5_signal.DEFAULT_PARAMS if params is None else params
    return p["MIN_RISE_FROM_OPEN"] - config.S5_SCREEN_RISE_MARGIN


@dataclass
class Judgement:
    """judge_rows() 的結果：目標 K 棒所屬小時的 ① 狀態，與該小時第一個訊號（沒有就 None）。"""
    hour_ms: int
    target_open_ms: int
    complete: bool
    rise: object            # float；前一小時沒有資料時 None
    rise_ok: bool           # 精確 ①（含前一小時完整）
    signal: object          # dict 或 None：bar_open_ms / bar_close_ms / signal_price / minute / rise / volx / above / branch_code


def judge_rows(rows, close_ms, params=None):
    """1M klines 原始 rows（任意順序，可含未收完那根）→ 收盤在 close_ms 的那根所屬小時的判定。純函式，不打網路。

    目標 K 棒（開盤 = close_ms − 1 分）不在整理後的資料最後一根 → 回 None（呼叫端記 target_missing）。
    訊號只取「目標 K 棒所屬小時」裡 signal = -1 的第一根（不跨小時回判）；它可以比目標 K 棒早（補送）。
    """
    bar_ms = klines.interval_ms(config.S5_KLINE_INTERVAL)
    close_ms = int(close_ms)
    target_open = close_ms - bar_ms
    df, _ = klines.prepare_klines(klines.raw_frame(rows), bar_ms, t_now=close_ms)
    if df is None or len(df) == 0 or int(df["time"].iloc[-1]) != target_open:
        return None
    ev = s5_signal.evaluate(df, bar_ms, params)
    hour = target_open // HOUR_MS
    last = ev.iloc[-1]
    hit = ev.index[(ev["hid"] == hour) & (ev["signal"] == -1)]
    sig = None
    if len(hit):
        i = hit[0]
        bar_open = int(df.at[i, "time"])
        sig = {
            "bar_open_ms": bar_open,
            "bar_close_ms": bar_open + bar_ms,
            "signal_price": float(df.at[i, "close"]),
            "minute": int(ev.at[i, "minute"]),
            "rise": float(ev.at[i, "rise"]),
            "volx": float(ev.at[i, "volx"]),
            "above": float(ev.at[i, "above"]),
            "branch_code": int(ev.at[i, "branch_code"]),
        }
    return Judgement(hour_ms=int(hour) * HOUR_MS, target_open_ms=target_open, complete=bool(last["complete"]),
                     rise=_num(last["rise"]), rise_ok=bool(last["rise_ok"]), signal=sig)


@dataclass
class S5Signal:
    """送進 A3 的一筆策略5 原始訊號（features 與 submit_signal 收到的相同）。"""
    symbol: str
    bar_open_ms: int
    bar_close_ms: int
    signal_price: float
    features: dict
    late: bool              # 訊號 K 棒比這次的目標 K 棒早（補送）
    late_s: float           # 送出時距訊號 K 棒收盤幾秒


@dataclass
class MinuteResult:
    """一根 1 分K 的處理結果（FR-7）。時間一律 UTC epoch ms（伺服器時間），秒數以收盤時刻為 0 點。

    hour_ms / minute    目標 K 棒所屬小時的開始時刻、第幾分（1～60；第 60 分在 H+1:00 收盤）
    candidates          這一分鐘仍在追蹤、要取數的候選（missed 的分鐘是「本來要取」的那些）
    fetches / http_429  這一分鐘 A5 實際送出的 klines 請求數、其中收到 429 的數
    signals             送出的 S5Signal
    failed              取數或計算失敗：原因 → symbol 清單（原因同 degraded 原因字串）
    invalid_symbols     klines 回 MARKET_INVALID_SYMBOL 的幣（不算失敗、不算 degraded）
    stopped             這一分鐘起本小時不再追蹤的幣：symbol → 原因
    close_delay_s       收盤 → 開始處理；yield_s 讓 A1 先做花的秒數；latency_s 收盤 → 這一分鐘處理完（訊號都已送出）
    """
    hour_ms: int
    minute: int
    bar_close_ms: int
    candidates: list = field(default_factory=list)
    fetches: int = 0
    http_429: int = 0
    signals: list = field(default_factory=list)
    failed: dict = field(default_factory=dict)
    invalid_symbols: list = field(default_factory=list)
    stopped: dict = field(default_factory=dict)
    degraded_reasons: list = field(default_factory=list)
    close_delay_s: float = 0.0
    yield_s: float = 0.0
    latency_s: float = 0.0

    @property
    def degraded(self):
        return bool(self.degraded_reasons)

    def add_degraded(self, reason):
        if reason not in self.degraded_reasons:
            self.degraded_reasons.append(reason)


class _HourState:
    """小時 H（hour_ms = H:00）的粗篩與追蹤狀態。只有 A5 的執行緒碰它。"""

    def __init__(self, hour_ms):
        self.hour_ms = hour_ms
        self.threshold = None
        self.screened_minute = None     # 第一次粗篩是在目標 K 棒第幾分的那一刻做的（0 = H:00 那一刻）
        self.seen = set()               # 做過粗篩的幣（含未粗篩的）
        self.candidates = {}            # symbol → approx
        self.unscreened = {}            # symbol → 未粗篩原因（有樣本後移除）
        self.tracking = set()
        self.stopped = {}               # symbol → 停止追蹤原因
        self.judged = set()             # 本小時至少成功判定過一次的幣
        self.rise_pass = {}             # symbol → 精確 ①（通過的）
        self.minutes = 0
        self.degraded_minutes = 0
        self.missed = 0
        self.requests = 0
        self.signals = 0
        self.late = 0


class S5Feed:
    """A5 的主體。所有外部相依都可注入，測試全程離線。

    gate          live.rest_gate.RestGate（與 A1 同一個）
    buffer        live.price_buffer.PriceBuffer（A1 的 feed.buffer）
    universe_fn   universe_fn() → symbol 清單（A1 的 feed.universe）
    submit_fn     submit_fn(*, strategy, symbol, bar_open_ms, bar_close_ms, signal_price, features)
                  （A3 的 tracker.submit_signal；只排佇列就返回）
    clock         與 gate 同一個時鐘物件（A1 的 feed.clock）；預設 SystemClock
    params        s5_signal 參數 dict；None = 呼叫當下的 s5_signal.DEFAULT_PARAMS（不複製）
    tickers_fn    tickers_fn() → 最新的 TickersSnapshot 或 None（A1 的 feed.latest_tickers）；只拿來分類未粗篩的原因
    executor      候選取數用；預設自己建 ThreadPoolExecutor(S5_FETCH_CONCURRENCY)
    on_result     每分鐘結束時呼叫 on_result(MinuteResult)；例外只記 ERROR
    """

    def __init__(self, *, gate, buffer, universe_fn, submit_fn, clock=None, params=None, tickers_fn=None,
                 executor=None, on_result=None):
        self.gate = gate
        self.buffer = buffer
        self.clock = clock or rest_gate.SystemClock()
        self._universe_fn = universe_fn
        self._submit_fn = submit_fn
        self._params = params
        self._tickers_fn = tickers_fn
        self._on_result = on_result
        self.bar_ms = klines.interval_ms(config.S5_KLINE_INTERVAL)
        self.wait_ms = int(round(config.BAR_FINALIZE_WAIT_SECONDS * 1000))
        self.tol_ms = int(round(config.MISSED_CLOSE_TOLERANCE_SECONDS * 1000))
        self.base_tol_ms = int(round(config.SCREEN_BASE_TOLERANCE_SECONDS * 1000))
        self._own_executor = executor is None
        self._executor = executor or concurrent.futures.ThreadPoolExecutor(
            max_workers=config.S5_FETCH_CONCURRENCY, thread_name_prefix="a5-fetch")
        self._stop = threading.Event()
        self._thread = None
        self._crashed = False
        self._hours = {}                # hour_ms → _HourState（只有 A5 的執行緒碰）
        self._invalid_logged = {}       # symbol → 台北日期：MARKET_INVALID_SYMBOL 的 INFO 當天記過了
        self._last_close_ms = None
        self._last_seen_ms = None
        # 累計數字（stats()，可能從別的執行緒讀）
        self._lock = threading.Lock()
        self._minutes = 0
        self._degraded = 0
        self._missed = 0
        self._signals = 0
        self._late = 0
        self._requests = 0
        self._http_429 = 0
        self._invalid_stops = 0
        self._unscreened_symbol_hours = 0
        self._latencies = []            # 有取數的分鐘：收盤 → 處理完（秒）
        self._cand_per_hour = []

    # ---------------- 對外 ----------------
    def start(self, duration_s=None):
        """開 A5 的工作執行緒（跑 run()）。可重複呼叫（第二次起不做事）。"""
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._thread_main, args=(duration_s,), name="a5-s5-feed",
                                        daemon=True)
        self._thread.start()

    def close(self, timeout=None):
        """停止並等工作執行緒收尾（最多 timeout，預設 S5_JOIN_TIMEOUT_SECONDS 秒），收掉自己建的 executor。

        回傳 True = 執行緒已結束（或從沒啟動）；False = 逾時仍在跑（已記 ERROR）。可重複呼叫。
        """
        self._stop.set()
        ok = True
        t = self._thread
        if t is not None and t is not threading.current_thread():
            t.join(config.S5_JOIN_TIMEOUT_SECONDS if timeout is None else timeout)
            if t.is_alive():
                ok = False
                logger.error("策略五資料層（A5）的工作執行緒 %s 秒內沒有結束",
                             config.S5_JOIN_TIMEOUT_SECONDS if timeout is None else timeout)
        if self._own_executor:
            self._executor.shutdown(wait=False, cancel_futures=True)
        return ok

    def stats(self):
        """累計數字（FR-7）。延遲只算有取數的分鐘（收盤 → 該分鐘處理完，訊號都已送出）。"""
        with self._lock:
            lat = sorted(self._latencies)
            cph = sorted(self._cand_per_hour)
            t = self._thread
            return {
                "minutes": self._minutes,
                "degraded": self._degraded,
                "missed": self._missed,
                "signals": self._signals,
                "late_submits": self._late,
                "requests": self._requests,
                "http_429": self._http_429,
                "latency_p50_s": _pct(lat, 50) if lat else None,
                "latency_p95_s": _pct(lat, 95) if lat else None,
                "latency_max_s": lat[-1] if lat else None,
                "candidates_per_hour_p50": _pct(cph, 50) if cph else None,
                "candidates_per_hour_max": cph[-1] if cph else None,
                "invalid_symbol_stops": self._invalid_stops,
                "unscreened_symbol_hours": self._unscreened_symbol_hours,
                "worker_alive": t is not None and t.is_alive(),
                "worker_crashed": self._crashed,
            }

    # ---------------- 主迴圈 ----------------
    def _thread_main(self, duration_s):
        try:
            self.run(duration_s)
        except Exception:  # noqa: BLE001 —— 不自動重啟，也不可以把 A1 / A3 拖下水
            self._crashed = True
            logger.exception("策略五資料層（A5）的工作執行緒意外結束，不自動重啟（A1 / A3 照常運作）")

    def run(self, duration_s=None):
        """跑到 duration_s 秒後（或 close()）為止。阻塞呼叫者（start() 會把它放進自己的執行緒）。

        每一圈先找出 (上次處理的收盤, 現在 − BAR_FINALIZE_WAIT_SECONDS] 之間的每一個 1 分K 收盤，依序處理：
        延誤 <= MISSED_CLOSE_TOLERANCE_SECONDS 照常判定，超過就記 missed（不取數，同一小時內下一個處理到的
        分鐘補送）。啟動前已經收盤的根不算這次運作的範圍。結束時把還沒摘要的小時各記一行（標「不完整」）。
        """
        start = self._now()
        self._last_close_ms = (start // self.bar_ms) * self.bar_ms
        self._last_seen_ms = start
        end_ms = None if duration_s is None else start + duration_s * 1000.0
        try:
            while not self._stop.is_set():
                now = self._now()
                gap_from, self._last_seen_ms = self._last_seen_ms, now
                due = self._due_closes(now)
                if due:
                    self._handle_due(due, gap_from)
                    continue
                wake = self._last_close_ms + self.bar_ms + self.wait_ms
                if end_ms is not None and wake > end_ms:
                    break
                self._sleep_until(wake)
        finally:
            self._finalize_hours(None)

    def _now(self):
        return self.gate.server_clock.now_ms()

    def _params_now(self):
        return s5_signal.DEFAULT_PARAMS if self._params is None else self._params

    def _due_closes(self, now_ms):
        """已經到了處理時刻（收盤 + BAR_FINALIZE_WAIT_SECONDS <= now）的每一個收盤，由舊到新。"""
        out = []
        c = self._last_close_ms + self.bar_ms
        while c + self.wait_ms <= now_ms:
            out.append(c)
            c += self.bar_ms
        return out

    def _handle_due(self, due, gap_from_ms):
        missed = []
        for close in due:
            if self._stop.is_set():
                break
            delay_ms = self._now() - close
            self._last_close_ms = close
            if delay_ms > self.tol_ms:
                missed.append(self._missed_minute(close, delay_ms / 1000.0))
                continue
            self._flush_missed(missed, gap_from_ms)
            missed = []
            self._finish(self._tick(close))
        self._flush_missed(missed, gap_from_ms)

    def _sleep_until(self, server_ms):
        while not self._stop.is_set():
            now = self._now()
            remaining = (server_ms - now) / 1000.0
            if remaining <= 0:
                return
            # 停頓摘要用：真的要睡之前記下這一刻（同 A1）
            self._last_seen_ms = now
            self.clock.sleep(min(remaining, 0.5))

    def _state(self, hour_ms):
        st = self._hours.get(hour_ms)
        if st is None:
            st = self._hours[hour_ms] = _HourState(hour_ms)
        return st

    @staticmethod
    def _hour_minute(close_ms, bar_ms):
        """收盤 close_ms 的那根 → (所屬小時 H:00, 第幾分)。所屬小時以開盤時刻切：H+1:00 收盤的是 H 的第 60 分。"""
        hour_ms = (close_ms - bar_ms) // HOUR_MS * HOUR_MS
        return hour_ms, int((close_ms - hour_ms) // MINUTE_MS)

    # ---------------- 一分鐘 ----------------
    def _tick(self, close):
        target_open = close - self.bar_ms
        hm, minute = self._hour_minute(close, self.bar_ms)
        hs = close // HOUR_MS * HOUR_MS          # 收盤那一刻所在的小時：第 60 分時是 H+1，要順便做它的粗篩
        res = MinuteResult(hour_ms=hm, minute=minute, bar_close_ms=close)
        res.close_delay_s = (self._now() - close) / 1000.0
        params = self._params_now()
        st = self._state(hm)
        try:
            res.yield_s = self._yield_to_a1()
            self._screen(st, params, minute)
            if hs != hm:
                self._screen(self._state(hs), params, 0)
            if any(r == UNSCREENED_NO_SAMPLE for r in st.unscreened.values()):
                res.add_degraded(DEGRADED_UNSCREENED)
        except Exception:  # noqa: BLE001
            res.add_degraded(DEGRADED_EXCEPTION)
            logger.exception("策略五 1分K %s：粗篩時發生未預期的例外，這一分鐘照常對已追蹤的候選取數",
                             _taipei(close))
        try:
            self._step(res, st, target_open, params)
        except Exception:  # noqa: BLE001
            res.add_degraded(DEGRADED_EXCEPTION)
            logger.exception("策略五 1分K %s：取數判定時發生未預期的例外", _taipei(close))
        res.latency_s = (self._now() - close) / 1000.0
        self._log_minute(res)
        return res

    def _yield_to_a1(self):
        """A1 持有 foreground_hold（5M 收盤前後）時先讓它做完，最多 S5_YIELD_MAX_SECONDS。回傳等了幾秒。"""
        limiter = self.gate.limiter
        t0 = self.clock.monotonic()
        while not self._stop.is_set() and limiter.holding():
            waited = self.clock.monotonic() - t0
            if waited >= config.S5_YIELD_MAX_SECONDS:
                break
            self.clock.sleep(min(config.S5_YIELD_MAX_SECONDS - waited, 0.05))
        return self.clock.monotonic() - t0

    def _screen(self, st, params, minute):
        """小時 st.hour_ms 的 ① 粗篩：沒篩過的幣、以及之前沒有樣本的幣。已篩過的不重篩（approx 在小時內固定）。"""
        if st.threshold is None:
            st.threshold = screen_threshold(params)
        universe = list(self._universe_fn() or [])
        uset = set(universe)
        for sym in [s for s in st.unscreened if s not in uset]:
            del st.unscreened[sym]            # 已經不在標的池：不再篩，也不算未粗篩
        todo = [s for s in universe if s not in st.seen or s in st.unscreened]
        if not todo:
            return
        if st.screened_minute is None:
            st.screened_minute = minute
        base_ms = st.hour_ms - HOUR_MS
        snap, snap_loaded = None, False
        for sym in todo:
            st.seen.add(sym)
            now_s = self.buffer.price_at(sym, st.hour_ms, self.base_tol_ms)
            base_s = self.buffer.price_at(sym, base_ms, self.base_tol_ms)
            if now_s is None or base_s is None:
                if not snap_loaded:
                    snap, snap_loaded = (self._tickers_fn() if self._tickers_fn is not None else None), True
                st.unscreened[sym] = self._unscreened_reason(sym, snap)
                continue
            st.unscreened.pop(sym, None)
            approx = now_s.price / base_s.price - 1
            if approx >= st.threshold:
                st.candidates[sym] = approx
                st.tracking.add(sym)

    @staticmethod
    def _unscreened_reason(sym, snap):
        if snap is not None:
            if sym in (snap.invalid or ()):
                return UNSCREENED_TICKER_INVALID
            if sym not in snap.prices:
                return UNSCREENED_NOT_IN_TICKERS
        return UNSCREENED_NO_SAMPLE

    def _step(self, res, st, target_open, params):
        """對仍在追蹤中的候選並發取數，依 symbol 順序判定、送訊號。"""
        syms = sorted(st.tracking)
        res.candidates = syms
        if not syms:
            return
        infos = {s: {"requests": 0, "http_429": 0, "attempts": 0} for s in syms}
        futs = {s: self._executor.submit(self._fetch_one, s, target_open, infos[s]) for s in syms}
        concurrent.futures.wait(list(futs.values()))
        failed = {}
        for sym in syms:
            info = infos[sym]
            res.fetches += info["requests"]
            res.http_429 += info["http_429"]
            try:
                status, rows = futs[sym].result()
            except Exception as e:  # noqa: BLE001 —— worker 的例外從 future 取回
                failed.setdefault(DEGRADED_EXCEPTION, []).append(sym)
                logger.error("策略五 1分K %s：%s 取數時發生未預期的例外：%s", _taipei(res.bar_close_ms), sym, e,
                             exc_info=(type(e), e, e.__traceback__))
                continue
            if status == FETCH_INVALID_SYMBOL:
                res.invalid_symbols.append(sym)
                self._stop_tracking(st, res, sym, STOP_INVALID_SYMBOL)
                self._log_invalid(sym)
                continue
            if status != FETCH_OK:
                failed.setdefault(status, []).append(sym)
                continue
            try:
                if not self._judge(res, st, sym, rows, target_open + self.bar_ms, params):
                    failed.setdefault(DEGRADED_TARGET_MISSING, []).append(sym)
            except Exception as e:  # noqa: BLE001 —— 一個幣算錯不影響同一分鐘的其他幣
                failed.setdefault(DEGRADED_EXCEPTION, []).append(sym)
                logger.error("策略五 1分K %s：%s 判定時發生未預期的例外：%s", _taipei(res.bar_close_ms), sym, e,
                             exc_info=(type(e), e, e.__traceback__))
        for reason, lst in sorted(failed.items()):
            res.failed[reason] = lst
            res.add_degraded(reason)
            level = logging.ERROR if reason == DEGRADED_BANNED else logging.WARNING
            logger.log(level, "策略五 1分K %s：取數失敗 %s %d 個：%s", _taipei(res.bar_close_ms), reason, len(lst), lst)

    def _fetch_one(self, sym, target_open, info):
        """worker：打 1M klines 直到回應裡有開盤 = target_open 的那根。回傳 (狀態, rows)。

        目標未到與 API 錯誤在 TARGET_BAR_ATTEMPTS 次內重試（間隔 TARGET_BAR_RETRY_WAIT_SECONDS，同 A1）；
        429 封鎖冷卻與 MARKET_INVALID_SYMBOL 不重試。其他例外進 future，由 _step 取出、計數、記 ERROR。
        info["requests"] 記實際送出的請求數（冷卻中被閘門擋下、沒送出的不算）。
        """
        status = DEGRADED_TARGET_MISSING
        for attempt in range(1, config.TARGET_BAR_ATTEMPTS + 1):
            info["attempts"] = attempt
            try:
                rows = klines.fetch_klines(self.gate, sym, config.S5_KLINE_INTERVAL, config.S5_KLINES_LIMIT,
                                           priority=rest_gate.PRIORITY_FOREGROUND)
            except rest_gate.RestBanned as e:
                if isinstance(e.__cause__, ApiError):     # 這個請求本身收到 429（有送出）
                    info["requests"] += 1
                    info["http_429"] += 1
                return DEGRADED_BANNED, None
            except ApiError as e:
                info["requests"] += 1
                if e.code == MARKET_INVALID_SYMBOL:
                    return FETCH_INVALID_SYMBOL, None
                status = DEGRADED_API_ERROR
                info["error"] = str(e)
            except Exception:
                info["requests"] += 1
                raise
            else:
                info["requests"] += 1
                if klines.has_bar(rows, target_open):
                    return FETCH_OK, rows
                status = DEGRADED_TARGET_MISSING
            if attempt >= config.TARGET_BAR_ATTEMPTS or self._stop.is_set():
                break
            self.clock.sleep(config.TARGET_BAR_RETRY_WAIT_SECONDS)
        return status, None

    def _judge(self, res, st, sym, rows, close_ms, params):
        """判定一個幣；送出訊號（本小時第一次）。回傳 False = 整理後沒有目標 K 棒。"""
        j = judge_rows(rows, close_ms, params)
        if j is None:
            return False
        if sym not in st.judged:
            st.judged.add(sym)
            if not j.complete:
                self._stop_tracking(st, res, sym, STOP_INCOMPLETE)
                return True
            if not j.rise_ok:
                self._stop_tracking(st, res, sym, STOP_RISE_BELOW)
                return True
            st.rise_pass[sym] = j.rise
        if j.signal is not None:
            self._submit(res, st, sym, j.signal, j.target_open_ms, params)
        return True

    def _submit(self, res, st, sym, s, target_open, params):
        late_s = (self._now() - s["bar_close_ms"]) / 1000.0
        features = {"minute": s["minute"], "rise": s["rise"], "volx": s["volx"], "above": s["above"],
                    "branch_code": s["branch_code"], "late_s": late_s}
        self._submit_fn(strategy=STRATEGY, symbol=sym, bar_open_ms=s["bar_open_ms"], bar_close_ms=s["bar_close_ms"],
                        signal_price=s["signal_price"], features=features)
        # 送出成功才算：submit_fn 拋例外時這個幣照常追蹤，下一分鐘再送
        late = s["bar_open_ms"] < target_open
        res.signals.append(S5Signal(symbol=sym, bar_open_ms=s["bar_open_ms"], bar_close_ms=s["bar_close_ms"],
                                    signal_price=s["signal_price"], features=features, late=late, late_s=late_s))
        self._stop_tracking(st, res, sym, STOP_SENT)
        st.signals += 1
        st.late += int(late)
        tail = ("，補送、K 棒 %s、晚了 %.1f 秒" % (_taipei(s["bar_close_ms"]), late_s) if late
                else "，收盤後 %.1f 秒送出" % late_s)
        logger.info("原始訊號 %s @ K 棒 %s（策略五 第 %d 分）：訊號價 %s rise %.4f volx %.2f above %.4f 分支 %s%s",
                    sym, _taipei(s["bar_close_ms"]), s["minute"], s["signal_price"], s["rise"], s["volx"],
                    s["above"], s5_signal.branch_label(s["branch_code"], params), tail)

    @staticmethod
    def _stop_tracking(st, res, sym, reason):
        st.tracking.discard(sym)
        st.stopped[sym] = reason
        res.stopped[sym] = reason

    def _log_invalid(self, sym):
        with self._lock:
            self._invalid_stops += 1
        day = _taipei_day(self._now())
        if self._invalid_logged.get(sym) == day:
            return
        self._invalid_logged[sym] = day
        logger.info("策略五 %s：klines 回 %s（標的池有、klines 不收），本小時停止追蹤、不重試、不算 degraded"
                    "（每幣每日只記這一行）", sym, MARKET_INVALID_SYMBOL)

    def _log_minute(self, res):
        if not (res.fetches or res.degraded_reasons or res.signals):
            return
        # 只有「沒有樣本、未粗篩」的分鐘用 INFO：冷啟動那幾分鐘本來就會這樣，每小時摘要另有 WARNING
        serious = [r for r in res.degraded_reasons if r != DEGRADED_UNSCREENED]
        logger.log(logging.WARNING if serious else logging.INFO,
                   "策略五 1分K %s（%s 第 %d 分）：追蹤 %d 個、請求 %d、訊號 %d、延遲 %.2f 秒（讓 A1 %.2f 秒）%s",
                   _taipei(res.bar_close_ms), _taipei(res.hour_ms), res.minute, len(res.candidates), res.fetches,
                   len(res.signals), res.latency_s, res.yield_s,
                   "、degraded %s" % res.degraded_reasons if res.degraded_reasons else "")

    # ---------------- missed ----------------
    def _missed_minute(self, close, delay_s):
        hm, minute = self._hour_minute(close, self.bar_ms)
        st = self._state(hm)
        res = MinuteResult(hour_ms=hm, minute=minute, bar_close_ms=close, candidates=sorted(st.tracking))
        res.close_delay_s = float(delay_s)
        res.latency_s = float(delay_s)
        res.add_degraded(DEGRADED_MISSED)
        return res

    def _flush_missed(self, missed, gap_from_ms):
        """交出一串 missed 的分鐘：合併成一行 ERROR（格式參照 A1 的「停頓 … missed_close N 根」），每一分鐘照樣交給 on_result。"""
        if not missed:
            return
        now = self._now()
        first, last = missed[0], missed[-1]
        logger.error("策略五 停頓 %s（%s～%s），missed_close %d 根：1分K %s～%s，延誤 > %s 秒，記為 missed（degraded）、"
                     "不取數；同一小時內的訊號由下一個處理到的分鐘補送",
                     _duration(now - gap_from_ms), _taipei_s(gap_from_ms), _taipei_s(now), len(missed),
                     _taipei(first.bar_close_ms), _taipei(last.bar_close_ms), config.MISSED_CLOSE_TOLERANCE_SECONDS)
        for res in missed:
            self._finish(res)

    # ---------------- 結果、摘要 ----------------
    def _finish(self, res):
        missed = DEGRADED_MISSED in res.degraded_reasons
        with self._lock:
            self._minutes += 1
            self._degraded += int(res.degraded)
            self._missed += int(missed)
            self._signals += len(res.signals)
            self._late += sum(1 for s in res.signals if s.late)
            self._requests += res.fetches
            self._http_429 += res.http_429
            if res.fetches:
                self._latencies.append(res.latency_s)
        st = self._hours.get(res.hour_ms)
        if st is not None:
            st.minutes += 1
            st.degraded_minutes += int(res.degraded)
            st.missed += int(missed)
            st.requests += res.fetches
        # 監控路徑（on_result、每小時摘要）不可以擋住訊號路徑：各自包 try
        if self._on_result is not None:
            try:
                self._on_result(res)
            except Exception:  # noqa: BLE001
                logger.exception("策略五 on_result 回呼失敗（1分K %s）", _taipei(res.bar_close_ms))
        try:
            self._finalize_hours(res.hour_ms)
        except Exception:  # noqa: BLE001
            logger.exception("策略五 每小時摘要失敗（1分K %s）", _taipei(res.bar_close_ms))

    def _finalize_hours(self, current_hour_ms):
        """把早於 current_hour_ms 的小時各記一行摘要後丟掉；None = 全部（結束時，標「不完整」）。"""
        for h in sorted(self._hours):
            if current_hour_ms is not None and h >= current_hour_ms:
                break
            st = self._hours.pop(h)
            self._log_hour(st, partial=current_hour_ms is None)

    def _log_hour(self, st, partial):
        # 未粗篩：小時結束時仍未粗篩的每個幣 → 原因（三種都列），依 symbol 排序，可以 ast.literal_eval
        unscreened = dict(sorted(st.unscreened.items()))
        stop_counts = {}
        for r in st.stopped.values():
            stop_counts[r] = stop_counts.get(r, 0) + 1
        no_sample = sorted(s for s, r in st.unscreened.items() if r == UNSCREENED_NO_SAMPLE)
        with self._lock:
            self._cand_per_hour.append(len(st.candidates))
            self._unscreened_symbol_hours += len(no_sample)
        logger.info("策略五 小時 %s%s：門檻 %s（粗篩於第 %s 分）、標的 %d、候選 %d %s、精確①通過 %d %s、訊號 %d（補送 %d）、"
                    "分鐘 %d（degraded %d、missed %d）、請求 %d、停止追蹤 %s、未粗篩 %s",
                    _taipei(st.hour_ms), "（不完整：程式結束時還沒走完）" if partial else "",
                    "-" if st.threshold is None else "%.4f" % st.threshold,
                    "-" if st.screened_minute is None else st.screened_minute, len(st.seen), len(st.candidates),
                    {s: round(a, 4) for s, a in sorted(st.candidates.items())}, len(st.rise_pass),
                    sorted(st.rise_pass), st.signals, st.late, st.minutes, st.degraded_minutes, st.missed,
                    st.requests, stop_counts, unscreened)
        if no_sample:
            logger.warning("策略五 小時 %s：%d 個幣到%s仍沒有 H:00 / H−1:00 的價格樣本，未粗篩（degraded）：%s",
                           _taipei(st.hour_ms), len(no_sample), "程式結束" if partial else "小時結束",
                           no_sample)
