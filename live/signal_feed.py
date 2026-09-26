# -*- coding: utf-8 -*-
"""
live.signal_feed — A 頻道資料層（A1）：tickers 輪詢 → ret2h 粗篩 → 候選 klines → 剛收完那根上的原始訊號
=====================================================================================================
整條路：

    每 10 秒打一次 tickers（1 個請求拿全市場現價），對齊伺服器時間的 10 秒整數倍
      → PriceBuffer 保留每個 symbol 過去 2 小時多的價格樣本
      → K 棒收盤（伺服器時間 T）那次輪詢的現價 ÷ T-2h 的樣本 - 1 = 近似 ret2h
      → 近似 ret2h >= MIN_RET_2H - SCREEN_RET2H_MARGIN 的才是候選（門檻現場推導，不寫死）
      → 只對候選並發打 klines，找出開盤時刻恰好 = T-5 分的那根（剛收完的那根）
      → 用與回測逐欄位一致的補洞規則整理（live.klines.prepare_klines），
        跑 s4_signal.signal_from_features(features(df, bars_per_hour))，取目標 K 棒那一列

每根 K 棒交出一個 BarResult（FR-7），以回呼交給呼叫端（日後的 A3、或 --record 寫 jsonl）。
本模組**無狀態**（除了價格緩衝）：不發 TG、不判斷冷卻、不追蹤出場、不寫資料庫 —— 那些要知道
「哪些部位開著」，是 A3 的事。同一個幣連續幾根都滿足條件時，這裡每根都會交出原始訊號。

──────────────────────────────────────────────────────────────────────
「剛收完的那根」
──────────────────────────────────────────────────────────────────────
klines 不帶 endTime 時回應是新到舊，而且**可能**含還沒收完的那根：收盤後 0.3 秒時 BTC 還沒有、
1 秒時就有了（captain 實測）。所以「取最後一根」有時對有時錯。這裡一律用開盤時刻比對：
回應裡沒有開盤 = T-5 分的那根 → 等 TARGET_BAR_RETRY_WAIT_SECONDS 再打，總共 TARGET_BAR_ATTEMPTS 次；
用盡就記「目標 K 棒未到」。**絕不拿前一根或未收完的那根頂替。**
prepare_klines 的 t_now = T，未收完的那根與之後的都不會進到 s4_signal。

「在」不等於「定稿」：實盤見過收盤後 0.33 秒取到的那根，之後被派網改寫（1 tick、成交量 +0.04%，
窄幅 K 棒的 cpos 因此移動 0.028）。所以候選 klines 等到 T + BAR_FINALIZE_WAIT_SECONDS 才送出
（粗篩仍在收盤當下）。「下一根出現了」不當作定稿的證據。等待秒數由 --finality-probe 的量測校正。

──────────────────────────────────────────────────────────────────────
每一根收盤都要有結果（BUG-005）
──────────────────────────────────────────────────────────────────────
主迴圈每一圈先處理 (上次處理的收盤, 現在] 之間的每一個收盤：延誤 <= MISSED_CLOSE_TOLERANCE_SECONDS
照常判定，超過就交出 missed_close（degraded、整個標的池記為未篩、不打 REST、不補判）。收盤到了就先處理
收盤，不跑過期的例行輪詢。行程停頓（闔蓋、VM 暫停）或一次卡住的輪詢，都不會讓某一根悄悄消失。

──────────────────────────────────────────────────────────────────────
設計取捨（PRD 要求 RD 決定並說明的地方）
──────────────────────────────────────────────────────────────────────
樣本時刻（FR-2）：每次 tickers 快照一個時刻 = 回應信封的 timestamp（伺服器時間），不用逐筆的 `time`、
    也不用排定的輪詢時刻。理由寫在 live.price_buffer 的模組 docstring。
2 小時前的樣本（FR-3）：在 T-2h ±SCREEN_BASE_TOLERANCE_SECONDS 內找最近的樣本（冷啟動補的種子優先）。
    找不到時：
      * 該 symbol 還在初始補種子的佇列裡 → 未篩原因「冷啟動中」，該根 degraded
      * 否則 → 未篩原因「沒有合格的 2 小時前樣本」，並依「最早可得樣本到現在的漲幅」由高到低，
        最多 SCREEN_NO_BASE_MAX_CANDIDATES 個升格為候選（寧可多打不要漏）；超過上限的部分
        就真的沒篩到，該根記 degraded（no_base_over_cap）。上限的理由：tickers 若在 2 小時前中斷過，
        整個標的池都會缺樣本，全部當候選就是一根 K 棒 560 個請求
並發（FR-5）：候選用 ThreadPoolExecutor(A1_FETCH_CONCURRENCY=6) 並發取數；每秒送幾個由共用的
    live.rest_gate 管（滑動窗口 6 個 / 1 秒，一開始可以連發）。worker 裡的例外一律從 future 取回，
    計入「取數失敗 / exception」並記 ERROR（future 收走的例外不會觸發 threading.excepthook）。
優先順序（FR-5）：收盤前 FOREGROUND_GUARD_SECONDS 起到判定完成，閘門只放行前景請求；
    補種子（NORMAL）與背景對帳（BACKGROUND）在這段期間不送。

──────────────────────────────────────────────────────────────────────
對外介面
──────────────────────────────────────────────────────────────────────
    feed = SignalFeed(gate=rest_gate.shared_gate(), on_result=callback)
    feed.run(duration_s=None)        # 阻塞；close() 或 Ctrl+C 結束
    feed.latest_tickers()            # 最新一次 tickers 快照（TickersSnapshot），還沒有回 None
    screen_threshold()               # 目前的粗篩門檻
    BarResult                        # 每根 K 棒一個，欄位見 class docstring

觀察用命令列（FR-9）：
    python -m live.signal_feed --duration 900 --record [DIR] --force-candidates N [--finality-probe]
"""

import argparse
import concurrent.futures
import heapq
import itertools
import json
import logging
import math
import os
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone

from live import config, klines, rest_gate
from live.paths import REPO_ROOT
from live.pionex_api import ApiError
from live.price_buffer import PriceBuffer
from strategy import s4_signal

logger = logging.getLogger(__name__)

TICKERS_PATH = "/api/v1/market/tickers"
TICKERS_PARAMS = {"type": "PERP"}

# ---- 未篩原因（FR-8：標的池數量 = 有篩到 + Σ 各原因的未篩數）----
UNSCREENED_NOT_IN_TICKERS = "not_in_tickers"     # tickers 沒有這個 symbol（例如 USD_USDT_PERP）
UNSCREENED_TICKER_INVALID = "ticker_invalid"     # tickers 有，但 close 不是正的有限數字
UNSCREENED_NO_BASE = "no_base_sample"            # 沒有合格的 2 小時前樣本（其中一部分可能升格為候選）
UNSCREENED_COLD_START = "cold_start"             # 冷啟動中：初始補種子還沒輪到它
UNSCREENED_TICKERS_FAILED = "tickers_failed"     # 收盤那次 tickers 失敗，整根沒得篩
UNSCREENED_BANNED = "banned"                     # 收盤那次 tickers 因 429 封鎖冷卻沒送
UNSCREENED_MISSED_CLOSE = "missed_close"         # 收盤時行程沒在處理（停頓 / 輪詢卡住），延誤超過容忍
DEGRADED_MISSED_CLOSE = "missed_close"

# ---- 取數失敗原因（FR-8：候選數 = 取數成功 + Σ 各原因的取數失敗數）----
FETCH_TARGET_MISSING = "target_missing"          # 重試用盡，回應裡仍沒有剛收完的那根
FETCH_API_ERROR = "api_error"                    # 派網回錯誤 / 連線失敗
FETCH_BANNED = "banned"                          # 429 封鎖冷卻中，沒送
FETCH_INSUFFICIENT_HISTORY = "insufficient_history"   # features 在目標列 ret2h / volr / turn 為 NaN
FETCH_EXCEPTION = "exception"                    # 未預期的例外（從 future 或計算中取回）

# 這些取數失敗代表「這根 K 棒沒有判完」→ degraded。歷史不足不算：回測在同一列也是 NaN、同樣不發訊號。
_DEGRADING_FETCH_FAILURES = (FETCH_TARGET_MISSING, FETCH_API_ERROR, FETCH_BANNED, FETCH_EXCEPTION)


def screen_threshold(params=None):
    """粗篩門檻 = MIN_RET_2H - SCREEN_RET2H_MARGIN。每次呼叫現場推導（params 預設取 strategy_params()）。"""
    p = config.strategy_params() if params is None else params
    return p["MIN_RET_2H"] - config.SCREEN_RET2H_MARGIN


# ============================== 資料結構 ==============================
@dataclass(frozen=True)
class TickersSnapshot:
    """一次 tickers 輪詢的結果。給 A3 讀最新現價用（SignalFeed.latest_tickers()）。

    server_ts_ms   回應信封的 timestamp（伺服器時間）= 這份快照的樣本時刻
    slot_ms        這次輪詢排定的時刻（伺服器時間，輪詢間隔的整數倍）
    local_recv_ms  本機收到回應的時刻
    prices         symbol → 現價（tickers 的 close）；只含通過驗證的
    row_times      symbol → 該筆 ticker 的 time（最後更新時刻）
    invalid        close 不是正的有限數字的 symbol
    """
    server_ts_ms: int
    slot_ms: int
    local_recv_ms: int
    prices: dict
    row_times: dict
    invalid: tuple = ()


@dataclass
class Candidate:
    symbol: str
    approx_ret2h: object      # float；升格的「沒有 2 小時前樣本」為 None
    source: str               # "screen" | "forced"（--force-candidates）| "no_base"


@dataclass
class RawSignal:
    """觸發 s4_signal 的一筆原始訊號。訊號價 = 目標 K 棒收盤價。四個特徵值供稽核。"""
    symbol: str
    bar_open_ms: int
    bar_close_ms: int
    signal_price: float
    ret2h: float
    volr: float
    cpos: object              # h == l 時 s4_signal 給 NaN，這裡記 None
    turn: float


@dataclass
class BarResult:
    """一根 K 棒的完整結果（FR-7 + FR-8）。

    時間一律 UTC epoch ms（伺服器時間）。latency 單位秒，0 點 = K 棒收盤時刻（伺服器時間）：
        settle         收盤 → 開始處理收盤 tickers（主迴圈的喚醒誤差；收盤已過才處理時就是那段延誤）
        tickers        收盤 tickers 這一步（含閘門排隊、失敗時的重試）
        screen         粗篩計算
        finalize_wait  等到收盤 + BAR_FINALIZE_WAIT_SECONDS（沒有候選就是 0；收盤處理本身晚了會變短）
        klines         候選 klines 並發取數本身（含目標 K 棒未到的重試）
        signal         補洞 + features + 訊號計算
        total          收盤 → 結果完成
    close_delay_s：開始處理這根時已經比收盤晚了幾秒。missed_close 的根沒有 latency，只有這個欄位。
    unscreened / fetch_failed：原因 → symbol 清單（清單長度就是計數）。
    judged：取數成功（含歷史不足）的候選 → 特徵值、訊號、實際判定的目標 K 棒 OHLCV、嘗試次數。
    """
    bar_open_ms: int
    bar_close_ms: int
    interval: str
    min_ret_2h: float
    threshold: float
    universe_size: int = 0
    approx_ret2h: dict = field(default_factory=dict)
    unscreened: dict = field(default_factory=dict)
    promoted_no_base: list = field(default_factory=list)
    candidates: list = field(default_factory=list)
    fetch_ok: list = field(default_factory=list)
    fetch_failed: dict = field(default_factory=dict)
    fetch_detail: dict = field(default_factory=dict)
    judged: dict = field(default_factory=dict)
    signals: list = field(default_factory=list)
    latency: dict = field(default_factory=dict)
    degraded_reasons: list = field(default_factory=list)
    clock_offset_ms: int = 0
    tickers_server_ts_ms: object = None
    close_sample_lag_ms: object = None
    ticker_row_lag_ms: object = None
    close_delay_s: object = None
    gate: dict = field(default_factory=dict)

    @property
    def degraded(self):
        return bool(self.degraded_reasons)

    @property
    def screened(self):
        return len(self.approx_ret2h)

    def unscreened_counts(self):
        return {k: len(v) for k, v in self.unscreened.items() if v}

    def fetch_failed_counts(self):
        return {k: len(v) for k, v in self.fetch_failed.items() if v}

    def accounting_problems(self):
        """FR-8 的兩條等式；成立回空清單，不成立回說明字串。"""
        problems = []
        uns = sum(len(v) for v in self.unscreened.values())
        if self.universe_size != self.screened + uns:
            problems.append(f"標的池 {self.universe_size} != 有篩到 {self.screened} + 未篩 {uns} "
                            f"{self.unscreened_counts()}")
        failed = sum(len(v) for v in self.fetch_failed.values())
        if len(self.candidates) != len(self.fetch_ok) + failed:
            problems.append(f"候選 {len(self.candidates)} != 取數成功 {len(self.fetch_ok)} + 失敗 {failed} "
                            f"{self.fetch_failed_counts()}")
        return problems

    def add_degraded(self, reason):
        if reason not in self.degraded_reasons:
            self.degraded_reasons.append(reason)

    def to_dict(self):
        """可直接 json.dumps 的 dict（NaN / inf 轉成 None）。含全部有篩到 symbol 的近似 ret2h。"""
        return _clean({
            "bar_open_ms": self.bar_open_ms,
            "bar_close_ms": self.bar_close_ms,
            "interval": self.interval,
            "min_ret_2h": self.min_ret_2h,
            "threshold": self.threshold,
            "universe_size": self.universe_size,
            "screened": self.screened,
            "unscreened": self.unscreened,
            "promoted_no_base": self.promoted_no_base,
            "candidates": [c.__dict__ for c in self.candidates],
            "fetch_ok": self.fetch_ok,
            "fetch_failed": self.fetch_failed,
            "fetch_detail": self.fetch_detail,
            "judged": self.judged,
            "signals": [s.__dict__ for s in self.signals],
            "latency": self.latency,
            "degraded": self.degraded,
            "degraded_reasons": self.degraded_reasons,
            "clock_offset_ms": self.clock_offset_ms,
            "tickers_server_ts_ms": self.tickers_server_ts_ms,
            "close_sample_lag_ms": self.close_sample_lag_ms,
            "ticker_row_lag_ms": self.ticker_row_lag_ms,
            "close_delay_s": self.close_delay_s,
            "gate": self.gate,
            "approx_ret2h": self.approx_ret2h,
        })

    def summary(self):
        """一行摘要（INFO 日誌用）。K 棒時刻以台北時間顯示，非零的未篩 / 失敗計數都列出來。"""
        lat = self.latency
        nan = float("nan")
        parts = [
            f"K 棒 {_taipei(self.bar_close_ms)} 收盤",
            f"標的池 {self.universe_size} 篩到 {self.screened} 候選 {len(self.candidates)} "
            f"成功 {len(self.fetch_ok)} 訊號 {len(self.signals)}",
            "延遲 %.2fs [結算 %.2f tickers %.2f 粗篩 %.2f 定稿等待 %.2f klines %.2f 訊號 %.2f]" % (
                lat.get("total", nan), lat.get("settle", nan), lat.get("tickers", nan), lat.get("screen", nan),
                lat.get("finalize_wait", nan), lat.get("klines", nan), lat.get("signal", nan)),
            f"時鐘偏移 {self.clock_offset_ms:+d}ms",
        ]
        if self.close_delay_s is not None and self.close_delay_s >= 1.0:
            parts.append("收盤後 %.1fs 才開始處理" % self.close_delay_s)
        uns = self.unscreened_counts()
        if uns:
            parts.append("未篩 " + " ".join(f"{k}={v}" for k, v in sorted(uns.items())))
        if self.promoted_no_base:
            parts.append(f"無樣本升格 {len(self.promoted_no_base)}")
        failed = self.fetch_failed_counts()
        if failed:
            parts.append("取數失敗 " + " ".join(f"{k}={v}" for k, v in sorted(failed.items())))
        parts.append("DEGRADED(" + ",".join(self.degraded_reasons) + ")" if self.degraded else "OK")
        return " | ".join(parts)


# ============================== 純函式 ==============================
def parse_tickers(js, slot_ms, local_recv_ms, fallback_server_ms):
    """tickers 回應信封 → TickersSnapshot。

    樣本時刻取信封的 timestamp；信封沒有 timestamp（不應發生）就用 fallback_server_ms（伺服器時間估計）。
    close 解析不出、不是正的有限數字的 symbol 放進 invalid，不進 prices。
    """
    data = js.get("data") if isinstance(js, dict) else None
    rows = data.get("tickers") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        raise ValueError(f"{TICKERS_PATH}: 回應沒有 data.tickers 清單")
    ts = js.get("timestamp")
    if not isinstance(ts, (int, float)) or isinstance(ts, bool):
        ts = fallback_server_ms
    prices, row_times, invalid = {}, {}, []
    for r in rows:
        sym = r.get("symbol") if isinstance(r, dict) else None
        if not sym:
            continue
        try:
            p = float(r["close"])
        except (KeyError, TypeError, ValueError):
            invalid.append(sym)
            continue
        if not math.isfinite(p) or p <= 0:
            invalid.append(sym)
            continue
        prices[sym] = p
        try:
            row_times[sym] = int(r["time"])
        except (KeyError, TypeError, ValueError):
            pass
    return TickersSnapshot(int(ts), int(slot_ms), int(local_recv_ms), prices, row_times, tuple(invalid))


def evaluate_target(rows, target_open_ms, bar_ms, bars_per_hour, params):
    """在剛收完的那根（開盤 = target_open_ms）上跑 s4_signal。回傳 dict；目標 K 棒不在 rows 裡回 None。

    rows 先經 prepare_klines（t_now = 目標收盤時刻：未收完的那根與之後的全部丟掉），
    整理後最後一列必須恰好是目標 K 棒，否則不判定（回 None）—— 不可以退而求其次用前一根。
    回傳的 insufficient=True 代表 ret2h / volr / turn 任一在目標列是 NaN（歷史不足）。
    """
    df, filled = klines.prepare_klines(klines.raw_frame(rows), bar_ms, t_now=target_open_ms + bar_ms)
    if df is None or len(df) == 0 or int(df["time"].iloc[-1]) != int(target_open_ms):
        return None
    feats = s4_signal.features(df, bars_per_hour)
    sig = s4_signal.signal_from_features(feats, params)
    last = feats.iloc[-1]
    row = df.iloc[-1]
    vals = {k: _num(last[k]) for k in s4_signal.FEATURE_COLS}
    return {
        **vals,
        "signal": int(sig.iloc[-1]),
        "insufficient": any(vals[k] is None for k in ("ret2h", "volr", "turn")),
        "target_bar": [float(row[c]) for c in ("open", "high", "low", "close", "volume")],
        "bars": int(len(df)),
        "filled_bars": int(filled),
    }


def seed_points(rows, bar_ms, t_now_ms):
    """冷啟動種子：已收完的 K 棒 → [(收盤時刻, 收盤價)]。補洞規則同 prepare_klines。

    開盤 t 那根的收盤價 = 時刻 t + bar_ms 的價格，這正是 s4_signal 定義 ret2h 用的點。
    """
    df, _ = klines.prepare_klines(klines.raw_frame(rows), bar_ms, t_now=t_now_ms)
    if df is None or len(df) == 0:
        return []
    return [(int(t) + bar_ms, float(c)) for t, c in zip(df["time"], df["close"])]


# ============================== 標的池 ==============================
class UniverseUnavailable(RuntimeError):
    """啟動時拿不到標的池（market_static 載入失敗）。"""


class MarketUniverse:
    """標的池 = live.market_static.trading_symbols(quote="USDT")，依 B5 既有機制刷新。

    market_static 直接呼叫 api_get（不經閘門、429 會自己重試），它不在 A1 的修改範圍內。
    這裡的補救：真的要刷新時先向閘門領 2 個額度（它一次刷新打 2 個請求），封鎖冷卻中就不刷；
    刷新失敗且原因是 429 時，讓閘門進入冷卻。每小時 2 個請求，剩下的風險可以接受。
    """

    STARTUP_ATTEMPTS = 3
    STARTUP_RETRY_SECONDS = 10

    def __init__(self, gate, clock=None):
        self.gate = gate
        self.clock = clock or rest_gate.SystemClock()

    def __call__(self, initial=False):
        from live import market_static as ms
        if initial or not ms.is_loaded():
            for attempt in range(self.STARTUP_ATTEMPTS):
                if self._refresh(ms, force=True):
                    break
                if attempt < self.STARTUP_ATTEMPTS - 1:
                    self.clock.sleep(self.STARTUP_RETRY_SECONDS)
            else:
                raise UniverseUnavailable(f"market_static 載入失敗：{ms.last_error}")
        elif self._stale(ms):
            self._refresh(ms, force=False)
        return ms.trading_symbols(quote="USDT")

    @staticmethod
    def _stale(ms):
        import time as _t   # market_static 自己用 time.monotonic() 記刷新時刻，這裡必須用同一個時鐘比
        return ms.last_refresh_mono is None or _t.monotonic() - ms.last_refresh_mono >= ms.STALE_SECONDS

    def _refresh(self, ms, force):
        try:
            for _ in range(2):
                self.gate.limiter.acquire(rest_gate.PRIORITY_NORMAL)
        except rest_gate.RestBanned:
            logger.warning("標的池刷新延後：REST 封鎖冷卻中")
            return False
        ok = ms.refresh() if force else ms.refresh_if_stale()
        if not ok:
            if "HTTP 429" in str(ms.last_error or ""):
                self.gate.limiter.trip_ban()
                logger.error("標的池刷新收到 429，整個行程停止送出 REST 請求 %.0f 秒", self.gate.limiter.ban_cooldown)
            logger.warning("標的池刷新失敗，沿用舊清單：%s", ms.last_error)
        return ok


# ============================== 主體 ==============================
class SignalFeed:
    """A1 的主體。所有外部相依都可注入（gate / clock / universe_fn / executor），測試全程離線。

    gate          live.rest_gate.RestGate（正式用 shared_gate()）
    clock         與 gate 同一個時鐘物件
    universe_fn   universe_fn(initial: bool) → symbol 清單；預設 MarketUniverse(gate)
    params_fn     回傳策略參數 dict；預設 config.strategy_params（每根 K 棒現場取）
    force_candidates
                  壓測用：每根 K 棒至少取近似 ret2h 最高的 N 個當候選
    fg_executor / bg_executor
                  候選取數 / 補種子用的 executor；預設各一個 ThreadPoolExecutor
    on_result     每根 K 棒結束時呼叫 on_result(BarResult)；例外會被記 ERROR，不會中斷主迴圈
    reconciler    live.reconcile.Reconciler；有給就把每根結果交給它，並在 run() 啟動它的執行緒
    finality_prober
                  FinalityProber（--finality-probe，驗證用）；有給就替每根已判定的候選排定稿重抓
    """

    def __init__(self, *, gate, clock=None, universe_fn=None, params_fn=None, interval=None,
                 force_candidates=0, fg_executor=None, bg_executor=None, on_result=None, reconciler=None,
                 finality_prober=None):
        self.gate = gate
        self.clock = clock or rest_gate.SystemClock()
        self.interval = interval or config.KLINE_INTERVAL
        self.bar_ms = klines.interval_ms(self.interval)
        self.bph = klines.bars_per_hour(self.interval)
        self.poll_ms = int(round(config.TICKERS_POLL_SECONDS * 1000))
        if self.poll_ms <= 0 or self.bar_ms % self.poll_ms:
            raise ValueError(f"K 棒週期 {self.bar_ms}ms 必須是輪詢間隔 {self.poll_ms}ms 的整數倍")
        # s4_signal.features 的 ret2h = close / close.shift(2 * bars_per_hour) - 1，也就是 2 小時
        self.horizon_ms = 2 * self.bph * self.bar_ms
        self.base_tolerance_ms = int(round(config.SCREEN_BASE_TOLERANCE_SECONDS * 1000))
        self.buffer = PriceBuffer(self.horizon_ms + self.base_tolerance_ms
                                  + int(config.PRICE_BUFFER_EXTRA_SECONDS * 1000))
        self._universe_fn = universe_fn or MarketUniverse(gate, self.clock)
        self._params_fn = params_fn or config.strategy_params
        if int(force_candidates) < 0:
            raise ValueError("force_candidates 不可以是負數")
        self.force_candidates = int(force_candidates)
        self._own_fg = fg_executor is None
        self._fg = fg_executor or concurrent.futures.ThreadPoolExecutor(
            max_workers=config.A1_FETCH_CONCURRENCY, thread_name_prefix="a1-fetch")
        self._own_bg = bg_executor is None
        self._bg = bg_executor or concurrent.futures.ThreadPoolExecutor(
            max_workers=config.A1_FETCH_CONCURRENCY, thread_name_prefix="a1-seed")
        self._on_result = on_result
        self.reconciler = reconciler
        self.prober = finality_prober
        self._last_close_ms = None     # run() 最後處理（判定或 missed_close）的收盤時刻
        self._last_seen_ms = None      # 主迴圈最後一次看到的伺服器時間（停頓摘要用）
        self._lock = threading.Lock()
        self._universe = []
        self._cold_pending = set()     # 初始補種子還沒完成的 symbol
        self._seed_inflight = set()
        self._seed_failed = set()
        self._seed_ok = 0
        self._initial_seed_started = None
        self._initial_seed_total = 0
        self._latest = None
        self._stop = threading.Event()

    # ---------------- 對外 ----------------
    def latest_tickers(self):
        """最新一次成功的 tickers 快照（TickersSnapshot）；還沒有回 None。"""
        return self._latest

    def universe(self):
        return list(self._universe)

    def cold_start_active(self):
        with self._lock:
            return bool(self._cold_pending)

    def stop(self):
        self._stop.set()

    def close(self):
        """停止主迴圈、背景對帳與補種子，收掉自己建的 executor。可重複呼叫。"""
        self._stop.set()
        if self.reconciler is not None:
            self.reconciler.join(timeout=5)
        if self.prober is not None:
            self.prober.join(timeout=5)
        if self._own_bg:
            self._bg.shutdown(wait=False, cancel_futures=True)
        if self._own_fg:
            self._fg.shutdown(wait=True)

    # ---------------- 標的池與補種子 ----------------
    def refresh_universe(self, initial=False):
        """重抓標的池；非初始時，新出現且沒有樣本的 symbol 會排進補種子（不算冷啟動）。"""
        syms = list(self._universe_fn(initial=initial))
        old = set(self._universe)
        self._universe = syms
        if not initial and old:
            seeded = self.buffer.seeded_symbols()
            new = [s for s in syms if s not in old and s not in seeded]
            if new:
                logger.info("標的池新增 %d 個 symbol，排進補種子：%s", len(new), new)
                self.start_seeding(new, initial=False)
        return syms

    def start_seeding(self, symbols, initial):
        """把 symbols 排進補種子（背景 executor、PRIORITY_NORMAL）。initial=True 的在完成前算「冷啟動中」。"""
        with self._lock:
            todo = [s for s in symbols if s not in self._seed_inflight]
            self._seed_inflight.update(todo)
            if initial:
                self._cold_pending.update(todo)
                self._initial_seed_total = len(todo)
                self._initial_seed_started = self.clock.monotonic()
        if initial:
            logger.info("冷啟動：補種子 %d 個 symbol（每個 1 個 klines 請求，limit %d）",
                        len(todo), config.SEED_KLINES_LIMIT)
        for sym in todo:
            self._bg.submit(self._seed_task, sym, initial)

    def _seed_task(self, sym, initial):
        """補一個 symbol 的種子。不會往外拋例外（沒有人取這個 future），任何例外都在這裡記 ERROR。"""
        ok, last = False, None
        try:
            attempts = 0
            while attempts < config.SEED_ATTEMPTS and not self._stop.is_set():
                try:
                    rows = klines.fetch_klines(self.gate, sym, self.interval, config.SEED_KLINES_LIMIT,
                                               priority=rest_gate.PRIORITY_NORMAL)
                except rest_gate.RestBanned as e:
                    # 封鎖冷卻不算一次嘗試：等冷卻結束再來
                    self._sleep(e.remaining_seconds + 0.05)
                    continue
                except ApiError as e:
                    attempts += 1
                    last = e
                    continue
                self.buffer.add_seed(sym, seed_points(rows, self.bar_ms, self.gate.server_clock.now_ms()))
                ok = True
                break
        except Exception as e:  # noqa: BLE001 —— 背景 worker 的例外不可以安靜消失
            last = e
            logger.exception("補種子 %s 發生未預期的例外", sym)
        finally:
            with self._lock:
                self._seed_inflight.discard(sym)
                self._cold_pending.discard(sym)
                if ok:
                    self._seed_ok += 1
                    self._seed_failed.discard(sym)
                else:
                    self._seed_failed.add(sym)
                finished = initial and not self._cold_pending and self._initial_seed_started is not None
                if finished:
                    started, self._initial_seed_started = self._initial_seed_started, None
                    failed = sorted(self._seed_failed)
            if not ok and not self._stop.is_set():
                logger.warning("補種子失敗 %s：%s（改由之後的 tickers 累積樣本）", sym, last)
            if finished:
                logger.info("冷啟動補種子完成：%d 個 symbol，失敗 %d 個%s，耗時 %.1f 秒",
                            self._initial_seed_total, len(failed), (" " + str(failed)) if failed else "",
                            self.clock.monotonic() - started)

    # ---------------- tickers ----------------
    def poll_tickers(self, slot_ms, priority=rest_gate.PRIORITY_FOREGROUND):
        """打一次 tickers，存進價格緩衝並成為最新快照。例外照閘門規則往外拋。"""
        js = self.gate.get(TICKERS_PATH, dict(TICKERS_PARAMS), priority=priority)
        snap = parse_tickers(js, slot_ms, self.clock.time_ms(), self.gate.server_clock.now_ms())
        self.buffer.add_snapshot(snap.server_ts_ms, snap.prices)
        self._latest = snap
        return snap

    def _close_tickers(self, close_ms):
        """收盤那次 tickers。回傳 (snapshot, None) 或 (None, 未篩原因)。429 冷卻中不重試。"""
        last = None
        for attempt in range(1, config.TICKERS_CLOSE_ATTEMPTS + 1):
            try:
                return self.poll_tickers(close_ms), None
            except rest_gate.RestBanned as e:
                logger.error("K 棒 %s：收盤 tickers 因 REST 封鎖冷卻沒有送出（%s），整根無法粗篩",
                             _taipei(close_ms), e)
                return None, UNSCREENED_BANNED
            except Exception as e:  # noqa: BLE001 —— ApiError 或回應格式不符，都當這次失敗
                last = e
                logger.warning("K 棒 %s：收盤 tickers 第 %d 次失敗：%s", _taipei(close_ms), attempt, e)
        logger.error("K 棒 %s：收盤 tickers 失敗 %d 次，整根無法粗篩：%s",
                     _taipei(close_ms), config.TICKERS_CLOSE_ATTEMPTS, last)
        return None, UNSCREENED_TICKERS_FAILED

    # ---------------- 一根 K 棒 ----------------
    def judge_bar(self, close_ms):
        """在收盤時刻 close_ms（伺服器時間）判定剛收完的那根。呼叫時機由 run() 決定：收盤當下，
        或收盤已過但延誤不超過 MISSED_CLOSE_TOLERANCE_SECONDS（延誤如實記在 close_delay_s 與 settle）。

        粗篩在收盤當下用 tickers 做；候選 klines 等到收盤 + BAR_FINALIZE_WAIT_SECONDS 才送出（沒有候選就不等）。
        """
        close_ms = int(close_ms)
        start_delay_s = (self.gate.server_clock.now_ms() - close_ms) / 1000.0
        mono_close = self.clock.monotonic() - start_delay_s

        def since_close():
            return self.clock.monotonic() - mono_close

        params = self._params_fn()
        target_open = close_ms - self.bar_ms
        res = BarResult(bar_open_ms=target_open, bar_close_ms=close_ms, interval=self.interval,
                        min_ret_2h=params["MIN_RET_2H"], threshold=screen_threshold(params))
        res.close_delay_s = start_delay_s
        universe = list(self._universe)
        res.universe_size = len(universe)
        marks = {"settle": since_close()}

        snap, fail = self._close_tickers(close_ms)
        marks["tickers"] = since_close()
        if snap is None:
            res.unscreened[fail] = sorted(universe)
            res.add_degraded(fail)
            marks["screen"] = marks["finalize"] = marks["klines"] = marks["signal"] = since_close()
        else:
            self._record_close_sample(res, snap, close_ms)
            self._screen(res, snap, universe, close_ms)
            marks["screen"] = since_close()
            if res.candidates:
                self._wait_until_finalized(close_ms)
            marks["finalize"] = since_close()
            fetched = self._fetch_candidates(res, target_open)
            marks["klines"] = since_close()
            self._evaluate(res, fetched, target_open, params)
            marks["signal"] = since_close()

        total = since_close()
        res.latency = {
            "settle": marks["settle"],
            "tickers": marks["tickers"] - marks["settle"],
            "screen": marks["screen"] - marks["tickers"],
            "finalize_wait": marks["finalize"] - marks["screen"],
            "klines": marks["klines"] - marks["finalize"],
            "signal": marks["signal"] - marks["klines"],
            "total": total,
        }
        if any(res.fetch_failed.get(k) for k in _DEGRADING_FETCH_FAILURES):
            for k in _DEGRADING_FETCH_FAILURES:
                if res.fetch_failed.get(k):
                    res.add_degraded(k)
        res.clock_offset_ms = self.gate.server_clock.offset_ms()
        res.gate = self.gate.stats()
        problems = res.accounting_problems()
        if problems:
            logger.error("K 棒 %s：FR-8 對帳不成立（程式錯誤）：%s", _taipei(close_ms), problems)
        self.buffer.prune(close_ms)
        self._log_result(res)
        return res

    def _wait_until_finalized(self, close_ms):
        """等到伺服器時間 >= 收盤 + BAR_FINALIZE_WAIT_SECONDS。收盤處理本身晚了就不用再等。

        最多等 BAR_FINALIZE_WAIT_SECONDS，不理會 stop：判定做到一半就停，不如做完這一根。
        """
        target = close_ms + int(round(config.BAR_FINALIZE_WAIT_SECONDS * 1000))
        while True:
            remaining = (target - self.gate.server_clock.now_ms()) / 1000.0
            if remaining <= 0:
                return
            self.clock.sleep(min(remaining, 0.5))

    def missed_bar(self, close_ms, delay_s):
        """收盤時沒能處理、延誤超過 MISSED_CLOSE_TOLERANCE_SECONDS 的那一根：交出一個 degraded 結果。

        不打任何 REST、不事後補判（captain 裁決：晚幾分鐘的訊號對 A 頻道沒有意義，收盤當下的 tickers
        樣本也已經不存在）。整個標的池記為未篩原因 missed_close，FR-8 兩條等式照樣成立；
        事後的真實情況由 FR-10 算（列為「未判定」，與漏失分開）。
        """
        close_ms = int(close_ms)
        params = self._params_fn()
        res = BarResult(bar_open_ms=close_ms - self.bar_ms, bar_close_ms=close_ms, interval=self.interval,
                        min_ret_2h=params["MIN_RET_2H"], threshold=screen_threshold(params))
        universe = list(self._universe)
        res.universe_size = len(universe)
        if universe:
            res.unscreened[UNSCREENED_MISSED_CLOSE] = sorted(universe)
        res.add_degraded(DEGRADED_MISSED_CLOSE)
        res.close_delay_s = float(delay_s)
        res.clock_offset_ms = self.gate.server_clock.offset_ms()
        res.gate = self.gate.stats()
        return res

    def _record_close_sample(self, res, snap, close_ms):
        res.tickers_server_ts_ms = snap.server_ts_ms
        res.close_sample_lag_ms = snap.server_ts_ms - close_ms
        lags = sorted(t - close_ms for t in snap.row_times.values())
        if lags:
            res.ticker_row_lag_ms = {"min": lags[0], "p50": lags[len(lags) // 2], "max": lags[-1]}
        if res.close_sample_lag_ms > config.CLOSE_SAMPLE_MAX_LAG_SECONDS * 1000:
            res.add_degraded("late_close_sample")
            logger.warning("K 棒 %s：收盤 tickers 的伺服器時間比收盤晚 %.1f 秒，不算收盤當下的價格",
                           _taipei(close_ms), res.close_sample_lag_ms / 1000.0)

    def _screen(self, res, snap, universe, close_ms):
        base_t = close_ms - self.horizon_ms
        with self._lock:
            cold = set(self._cold_pending)
        invalid = set(snap.invalid)
        no_base = []
        for sym in universe:
            p_now = snap.prices.get(sym)
            if p_now is None:
                reason = UNSCREENED_TICKER_INVALID if sym in invalid else UNSCREENED_NOT_IN_TICKERS
                res.unscreened.setdefault(reason, []).append(sym)
                continue
            base = self.buffer.price_at(sym, base_t, self.base_tolerance_ms)
            if base is None:
                if sym in cold:
                    res.unscreened.setdefault(UNSCREENED_COLD_START, []).append(sym)
                else:
                    no_base.append(sym)
                continue
            res.approx_ret2h[sym] = p_now / base.price - 1.0

        ranked = sorted(res.approx_ret2h.items(), key=lambda kv: (-kv[1], kv[0]))
        cands = [Candidate(s, a, "screen") for s, a in ranked if a >= res.threshold]
        if self.force_candidates > len(cands):
            cands += [Candidate(s, a, "forced") for s, a in ranked[len(cands):self.force_candidates]]

        if no_base:
            res.unscreened[UNSCREENED_NO_BASE] = sorted(no_base)

            def rank(sym):
                first = self.buffer.first_after(sym, base_t, close_ms)
                partial = snap.prices[sym] / first.price - 1.0 if first is not None else None
                return (partial is None, -(partial or 0.0), sym)

            order = sorted(no_base, key=rank)
            cap = config.SCREEN_NO_BASE_MAX_CANDIDATES
            res.promoted_no_base = order[:cap]
            cands += [Candidate(s, None, "no_base") for s in res.promoted_no_base]
            if len(no_base) > cap:
                res.add_degraded("no_base_over_cap")
                logger.warning("K 棒 %s：%d 個 symbol 沒有合格的 2 小時前樣本，只升格 %d 個為候選，其餘沒有篩到",
                               _taipei(close_ms), len(no_base), cap)
        if res.unscreened.get(UNSCREENED_COLD_START):
            res.add_degraded(UNSCREENED_COLD_START)
        res.candidates = cands

    def _fetch_candidates(self, res, target_open):
        """並發取候選的 klines。回傳 {symbol: (rows, info)}（有拿到目標 K 棒的）；失敗寫進 res.fetch_failed。"""
        futs = {}
        for c in res.candidates:
            futs[self._fg.submit(self._fetch_target, c.symbol, target_open)] = c.symbol
        concurrent.futures.wait(list(futs))
        fetched = {}
        for fut, sym in futs.items():
            try:
                status, rows, info = fut.result()
            except Exception as e:  # noqa: BLE001 —— future 收走的例外不會進日誌，一定要在這裡取出來
                res.fetch_failed.setdefault(FETCH_EXCEPTION, []).append(sym)
                res.fetch_detail[sym] = {"error": f"{type(e).__name__}: {e}"}
                logger.error("K 棒 %s：候選 %s 取 klines 時發生未預期的例外：%s",
                             _taipei(res.bar_close_ms), sym, e, exc_info=(type(e), e, e.__traceback__))
                continue
            if status == "ok":
                fetched[sym] = (rows, info)
            else:
                res.fetch_failed.setdefault(status, []).append(sym)
                res.fetch_detail[sym] = info
        return fetched

    def _fetch_target(self, sym, target_open):
        """worker：打 klines 直到回應裡有開盤 = target_open 的那根。回傳 (狀態, rows, info)。

        API 錯誤與目標未到都在 TARGET_BAR_ATTEMPTS 次內重試；429 封鎖冷卻不重試。
        其他例外不在這裡接 —— 讓它進 future，由 _fetch_candidates 取出、計數、記 ERROR。
        """
        info = {"attempts": 0}
        status = FETCH_TARGET_MISSING
        for attempt in range(1, config.TARGET_BAR_ATTEMPTS + 1):
            info["attempts"] = attempt
            try:
                rows = klines.fetch_klines(self.gate, sym, self.interval, config.KLINES_LIMIT,
                                           priority=rest_gate.PRIORITY_FOREGROUND)
            except rest_gate.RestBanned:
                return FETCH_BANNED, None, info
            except ApiError as e:
                status = FETCH_API_ERROR
                info["error"] = str(e)
            else:
                if klines.has_bar(rows, target_open):
                    info["has_next_bar"] = klines.has_bar(rows, target_open + self.bar_ms)
                    info["fetched_after_close_s"] = (self.gate.server_clock.now_ms()
                                                     - (target_open + self.bar_ms)) / 1000.0
                    return "ok", rows, info
                status = FETCH_TARGET_MISSING
                info["has_next_bar"] = klines.has_bar(rows, target_open + self.bar_ms)
            if attempt < config.TARGET_BAR_ATTEMPTS:
                self.clock.sleep(config.TARGET_BAR_RETRY_WAIT_SECONDS)
        return status, None, info

    def _evaluate(self, res, fetched, target_open, params):
        for c in res.candidates:
            if c.symbol not in fetched:
                continue
            rows, info = fetched[c.symbol]
            try:
                ev = evaluate_target(rows, target_open, self.bar_ms, self.bph, params)
            except Exception as e:  # noqa: BLE001
                res.fetch_failed.setdefault(FETCH_EXCEPTION, []).append(c.symbol)
                res.fetch_detail[c.symbol] = {"error": f"{type(e).__name__}: {e}", **info}
                logger.error("K 棒 %s：候選 %s 計算訊號時發生未預期的例外：%s",
                             _taipei(res.bar_close_ms), c.symbol, e, exc_info=(type(e), e, e.__traceback__))
                continue
            if ev is None:   # 不應發生：取數時已確認目標 K 棒在回應裡
                res.fetch_failed.setdefault(FETCH_TARGET_MISSING, []).append(c.symbol)
                res.fetch_detail[c.symbol] = info
                continue
            ev["attempts"] = info.get("attempts")
            # 下一根在不在、收盤後多久取到：定稿分析用（取數成功的也記，captain 裁決 1 第 4 點）
            ev["has_next_bar"] = info.get("has_next_bar")
            ev["fetched_after_close_s"] = info.get("fetched_after_close_s")
            res.judged[c.symbol] = ev
            if ev["insufficient"]:
                res.fetch_failed.setdefault(FETCH_INSUFFICIENT_HISTORY, []).append(c.symbol)
                continue
            res.fetch_ok.append(c.symbol)
            if ev["signal"] == -1:
                res.signals.append(RawSignal(
                    symbol=c.symbol, bar_open_ms=target_open, bar_close_ms=res.bar_close_ms,
                    signal_price=ev["target_bar"][3], ret2h=ev["ret2h"], volr=ev["volr"],
                    cpos=ev["cpos"], turn=ev["turn"]))

    def _log_result(self, res):
        logger.info(res.summary())
        for sig in res.signals:
            logger.info("原始訊號 %s @ K 棒 %s：訊號價 %s ret2h %.4f volr %.2f cpos %s turn %.0f",
                        sig.symbol, _taipei(sig.bar_close_ms), sig.signal_price, sig.ret2h, sig.volr,
                        "NaN" if sig.cpos is None else "%.3f" % sig.cpos, sig.turn)
        for reason, syms in sorted(res.fetch_failed.items()):
            if not syms:
                continue
            level = logging.INFO if reason == FETCH_INSUFFICIENT_HISTORY else logging.WARNING
            if reason == FETCH_BANNED:
                level = logging.ERROR
            logger.log(level, "K 棒 %s：取數失敗 %s %d 個：%s", _taipei(res.bar_close_ms), reason, len(syms), syms)
        if UNSCREENED_BANNED in res.degraded_reasons or FETCH_BANNED in res.degraded_reasons:
            logger.error("K 棒 %s 落在 REST 封鎖冷卻期間，結果不完整（degraded）", _taipei(res.bar_close_ms))

    # ---------------- 主迴圈 ----------------
    def run(self, duration_s=None):
        """跑到 duration_s 秒後（或 stop() / close()）為止。阻塞呼叫者。

        啟動：載入標的池 → 背景補種子（冷啟動）→ 啟動背景對帳（與定稿量測）→ 進入輪詢迴圈。
        迴圈：每個輪詢時刻（伺服器時間的 TICKERS_POLL_SECONDS 整數倍）打一次 tickers；
        是 K 棒收盤時刻的那一次，改走 judge_bar()，並從收盤前 FOREGROUND_GUARD_SECONDS 起
        把 REST 額度保留給前景，直到判定完成。

        不跳根（BUG-005）：每一圈開頭先找出 (上次處理的收盤, 現在] 之間的每一個收盤時刻，依序各交出
        一個 BarResult —— 延誤 <= MISSED_CLOSE_TOLERANCE_SECONDS 照常判定，超過就交出 missed_close。
        行程停頓（闔蓋、VM 暫停）、例行輪詢卡住、或只是醒來時剛好跨過收盤幾百毫秒，都不會讓某一根
        悄悄消失。收盤（或它的前景保留期）已經到了，就不再跑例行輪詢，先處理收盤；
        睡過頭的例行輪詢時刻（已過超過一個間隔）直接略過，不跑過期的輪詢。
        啟動前已經收盤的根不算這次運作的範圍。
        """
        self.refresh_universe(initial=True)
        self.start_seeding(self._universe, initial=True)
        start = self.gate.server_clock.now_ms()
        self._last_close_ms = (start // self.bar_ms) * self.bar_ms
        self._last_seen_ms = start
        if self.reconciler is not None:
            self.reconciler.expect_bars_from(self._last_close_ms + self.bar_ms)
            self.reconciler.start(self._stop, self.gate.server_clock)
        if self.prober is not None:
            self.prober.start(self._stop)
        guard_ms = int(round(config.FOREGROUND_GUARD_SECONDS * 1000))
        tol_ms = int(round(config.MISSED_CLOSE_TOLERANCE_SECONDS * 1000))
        end_ms = None if duration_s is None else start + duration_s * 1000.0
        last_slot = None
        while not self._stop.is_set():
            now = self.gate.server_clock.now_ms()
            gap_from, self._last_seen_ms = self._last_seen_ms, now
            due = self._due_closes(now)
            if due:
                self._handle_due(due, gap_from)
                last_slot = None              # 停頓 / 延誤已經逐根記過了，不再報一次「跳過幾次輪詢」
                continue
            next_close = self._last_close_ms + self.bar_ms
            slot = (now // self.poll_ms + 1) * self.poll_ms
            if end_ms is not None and slot > end_ms:
                break
            if last_slot is not None and slot - last_slot > self.poll_ms:
                logger.warning("輪詢落後，跳過 %d 次輪詢", (slot - last_slot) // self.poll_ms - 1)
            last_slot = slot
            if slot >= next_close:
                # 下一格就是收盤（K 棒週期是輪詢間隔的整數倍，所以 slot == next_close）
                self._sleep_until(next_close - guard_ms)
                if self._stop.is_set():
                    break
                if self.gate.server_clock.now_ms() >= next_close:
                    continue                  # 睡過頭（停頓）：回到開頭，和其他已過去的收盤一起處理
                with self.gate.limiter.foreground_hold():
                    self._sleep_until(next_close)
                    if self._stop.is_set():
                        break
                    if self.gate.server_clock.now_ms() - next_close > tol_ms:
                        continue              # 最後這一小段也停頓了：回到開頭當作已過去的收盤處理
                    res = self.judge_bar(next_close)
                self._last_close_ms = next_close
                self._finish_bar(res)
                self._after_bar()
                continue
            self._sleep_until(slot)
            if self._stop.is_set():
                break
            now = self.gate.server_clock.now_ms()
            if now >= next_close - guard_ms:
                continue                      # 收盤或它的前景保留期已到：先處理收盤，不跑例行輪詢
            if now - slot > self.poll_ms:
                logger.info("例行輪詢時刻 %s 已過 %.1f 秒（行程停頓？），略過這一格",
                            _taipei_s(slot), (now - slot) / 1000.0)
                continue
            self._routine_poll(slot)

    def _due_closes(self, now_ms):
        """(上次處理的收盤, now_ms] 之間的每一個收盤時刻，由舊到新。"""
        out = []
        c = self._last_close_ms + self.bar_ms
        while c <= now_ms:
            out.append(c)
            c += self.bar_ms
        return out

    def _handle_due(self, due, gap_from_ms):
        """依序處理已經到了（或已過去）的收盤：延誤在容忍內照常判定，超過就交出 missed_close。"""
        tol_ms = int(round(config.MISSED_CLOSE_TOLERANCE_SECONDS * 1000))
        missed = []
        for close in due:
            delay_ms = self.gate.server_clock.now_ms() - close
            if delay_ms > tol_ms:
                missed.append(self.missed_bar(close, delay_ms / 1000.0))
                self._last_close_ms = close
                continue
            self._flush_missed(missed, gap_from_ms)
            missed = []
            with self.gate.limiter.foreground_hold():
                res = self.judge_bar(close)
            self._last_close_ms = close
            self._finish_bar(res)
        self._flush_missed(missed, gap_from_ms)
        self._after_bar()

    def _flush_missed(self, missed, gap_from_ms):
        """交出一串 missed_close。日誌：第一根與最後一根各一行 ERROR，兩根以上再加一行停頓摘要 ——
        睡一整晚會有上百根，不讓上百行 ERROR 淹掉其他訊息；每一根照樣交給 on_result 與對帳。"""
        if not missed:
            return
        tol = config.MISSED_CLOSE_TOLERANCE_SECONDS
        first, last = missed[0], missed[-1]
        for res in ([first] if len(missed) == 1 else [first, last]):
            logger.error("K 棒 %s 沒有在收盤時處理（延誤 %.1f 秒 > %s 秒），記為 missed_close（degraded），不補判",
                         _taipei(res.bar_close_ms), res.close_delay_s, tol)
        if len(missed) > 1:
            now = self.gate.server_clock.now_ms()
            logger.error("停頓 %s（%s～%s），missed_close %d 根：K 棒 %s～%s",
                         _duration(now - gap_from_ms), _taipei_s(gap_from_ms), _taipei_s(now), len(missed),
                         _taipei(first.bar_close_ms), _taipei(last.bar_close_ms))
        for res in missed:
            self._finish_bar(res)

    def _finish_bar(self, res):
        self._deliver(res)
        if self.prober is not None and res.judged:
            self.prober.schedule(res)

    def _routine_poll(self, slot):
        t0 = self.clock.monotonic()
        try:
            self.poll_tickers(slot)
        except rest_gate.RestBanned as e:
            logger.warning("例行 tickers 輪詢略過（封鎖冷卻中，還剩 %.0f 秒）", e.remaining_seconds)
        except Exception as e:  # noqa: BLE001
            logger.warning("例行 tickers 輪詢失敗：%s", e)
        took = self.clock.monotonic() - t0
        if took * 1000 > self.poll_ms:
            logger.warning("例行 tickers 輪詢（%s）花了 %.1f 秒，超過一個輪詢間隔", _taipei_s(slot), took)

    def _deliver(self, res):
        if self.reconciler is not None:
            self.reconciler.add_bar(res)
        if self._on_result is not None:
            try:
                self._on_result(res)
            except Exception:  # noqa: BLE001
                logger.exception("on_result 回呼失敗（K 棒 %s）", _taipei(res.bar_close_ms))

    def _after_bar(self):
        try:
            self.refresh_universe()
        except Exception:  # noqa: BLE001
            logger.exception("標的池刷新時發生未預期的例外，沿用舊清單")

    def _sleep_until(self, server_ms):
        while not self._stop.is_set():
            now = self.gate.server_clock.now_ms()
            remaining = (server_ms - now) / 1000.0
            if remaining <= 0:
                return
            # 停頓摘要用：真的要睡之前記下這一刻。醒來後（remaining <= 0）不覆寫，停頓的起點才不會被蓋掉
            self._last_seen_ms = now
            self.clock.sleep(min(remaining, 0.5))

    def _sleep(self, seconds):
        end = self.clock.monotonic() + seconds
        while not self._stop.is_set():
            remaining = end - self.clock.monotonic()
            if remaining <= 0:
                return
            self.clock.sleep(min(remaining, 0.5))


# ============================== 定稿量測（--finality-probe，驗證用） ==============================
class FinalityProber:
    """收盤後 FINALITY_PROBE_OFFSETS_SECONDS（預設 5 / 15 / 60 秒）各重抓一次每個已判定候選的目標 K 棒，
    記下每個時點的 OHLCV，事後算「最後一次變動發生在收盤後多久」，用來校正 BAR_FINALIZE_WAIT_SECONDS。

    這是驗證工具，不是正式行為：它不影響判定、不回頭改任何結果。
      * 每個候選多 len(offsets) 個請求，走共用閘門、PRIORITY_NORMAL：前景保留期間不送、有前景請求在等時
        讓它先，所以不會擋到收盤後的前景取數。被延後的照實記錄實際取到的時刻（fetched_after_close_s）
      * 數值用 live.klines.ohlcv_by_open 轉（與判定時的 prepare_klines 同一個轉法），比對不會因為
        字串轉浮點的 1 ULP 差異而誤報
      * 每一次重抓交給 on_probe(dict)；命令列把它寫進 --record 的 jsonl（type = "finality_probe"）
    schedule() 由主迴圈呼叫；run_due() 處理到期的（背景執行緒，或測試直接呼叫）。
    """

    def __init__(self, gate, *, clock=None, interval=None, offsets_s=None, limit=None, on_probe=None):
        self.gate = gate
        self.clock = clock or rest_gate.SystemClock()
        self.interval = interval or config.KLINE_INTERVAL
        self.bar_ms = klines.interval_ms(self.interval)
        self.offsets_s = tuple(config.FINALITY_PROBE_OFFSETS_SECONDS if offsets_s is None else offsets_s)
        self.limit = int(config.FINALITY_PROBE_KLINES_LIMIT if limit is None else limit)
        self._on_probe = on_probe
        self._lock = threading.Lock()
        self._tasks = []
        self._seq = itertools.count()
        self._thread = None
        self.probes = 0
        self.changed = 0
        self.errors = 0

    def schedule(self, res):
        """為這根已判定的每個候選排好 len(offsets) 次重抓。"""
        with self._lock:
            for sym, ev in res.judged.items():
                bar = ev.get("target_bar")
                if not bar:
                    continue
                for off in self.offsets_s:
                    heapq.heappush(self._tasks, (res.bar_close_ms + int(round(off * 1000)), next(self._seq),
                                                 res.bar_close_ms, sym, off, tuple(bar),
                                                 ev.get("fetched_after_close_s")))

    def pending(self):
        with self._lock:
            return len(self._tasks)

    def next_due_ms(self):
        with self._lock:
            return self._tasks[0][0] if self._tasks else None

    def run_due(self, stop_event=None):
        """把已經到期的重抓全部做完。回傳做了幾個。"""
        done = 0
        while stop_event is None or not stop_event.is_set():
            with self._lock:
                if not self._tasks or self._tasks[0][0] > self.gate.server_clock.now_ms():
                    break
                task = heapq.heappop(self._tasks)
            self._probe(task)
            done += 1
        return done

    def _probe(self, task):
        _, _, close_ms, sym, off, judged, judged_after = task
        target_open = close_ms - self.bar_ms
        rec = {"symbol": sym, "bar_close_ms": close_ms, "offset_s": off,
               "judged_target_bar": list(judged), "judged_fetched_after_close_s": judged_after}
        try:
            rows = klines.fetch_klines(self.gate, sym, self.interval, self.limit,
                                       priority=rest_gate.PRIORITY_NORMAL)
        except rest_gate.RestBanned as e:
            rec["error"] = f"banned: {e}"
        except ApiError as e:
            rec["error"] = f"api_error: {e}"
        except Exception as e:  # noqa: BLE001 —— 背景執行緒的例外不可以安靜消失
            rec["error"] = f"{type(e).__name__}: {e}"
            logger.error("定稿量測 %s 取 klines 時發生未預期的例外：%s", sym, e, exc_info=(type(e), e, e.__traceback__))
        else:
            rec["fetched_after_close_s"] = (self.gate.server_clock.now_ms() - close_ms) / 1000.0
            bars = klines.ohlcv_by_open(rows)
            seen = bars.get(target_open)
            rec["target_bar"] = [list(v) for v in sorted(seen)] if seen else None
            rec["has_next_bar"] = (target_open + self.bar_ms) in bars
            rec["same_as_judged"] = None if not seen else seen == {tuple(judged)}
            if seen and seen != {tuple(judged)}:
                with self._lock:
                    self.changed += 1
                logger.info("定稿量測：%s K 棒 %s 在收盤後 %.1f 秒的版本與判定時（收盤後 %s 秒）不同：%s → %s",
                            sym, _taipei(close_ms), rec["fetched_after_close_s"], judged_after, list(judged),
                            rec["target_bar"])
        with self._lock:
            self.probes += 1
            if "error" in rec:
                self.errors += 1
        if "error" in rec:
            logger.warning("定稿量測 %s K 棒 %s（收盤後 %s 秒那次）失敗：%s", sym, _taipei(close_ms), off, rec["error"])
        if self._on_probe is not None:
            try:
                self._on_probe(rec)
            except Exception:  # noqa: BLE001
                logger.exception("定稿量測 on_probe 回呼失敗")

    def start(self, stop_event):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, args=(stop_event,), name="a1-finality-probe", daemon=True)
        self._thread.start()

    def join(self, timeout=None):
        if self._thread is not None:
            self._thread.join(timeout)

    def _loop(self, stop):
        while not stop.is_set():
            try:
                self.run_due(stop)
            except Exception:  # noqa: BLE001
                logger.exception("定稿量測執行緒發生未預期的例外，繼續")
            due = self.next_due_ms()
            wait = 0.5 if due is None else (due - self.gate.server_clock.now_ms()) / 1000.0
            self.clock.sleep(min(max(wait, 0.05), 0.5))


# ============================== 紀錄與命令列 ==============================
def check_record_dir(path):
    """--record 的目錄 → 絕對路徑。落在 repo 的 state/ 或 output/（含子目錄）就拋 ValueError。

    那兩個目錄每小時被 dry run 自動 commit push，實盤紀錄絕不可以進去（live/ 約定第 5 條）。
    """
    real = os.path.normcase(os.path.realpath(os.path.abspath(path)))
    for name in ("state", "output"):
        forbidden = os.path.normcase(os.path.realpath(os.path.join(REPO_ROOT, name)))
        if real == forbidden or real.startswith(forbidden + os.sep):
            raise ValueError(f"--record 不可以寫進 {name}/（每小時會被自動 commit push）：{path}")
    return os.path.abspath(path)


class JsonlRecorder:
    """每根 K 棒 / 每批對帳一行 JSON（UTF-8，中文合約名原字元）。目錄由這裡建（呼叫者的責任）。"""

    def __init__(self, directory):
        os.makedirs(directory, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.path = os.path.join(directory, f"signal_feed_{stamp}.jsonl")
        self._lock = threading.Lock()
        self._f = open(self.path, "a", encoding="utf-8")

    def write(self, kind, payload):
        line = json.dumps({"type": kind, **_clean(payload)}, ensure_ascii=False, allow_nan=False)
        with self._lock:
            self._f.write(line + "\n")
            self._f.flush()

    def close(self):
        with self._lock:
            if not self._f.closed:
                self._f.close()


class _RunStats:
    """命令列結束時的總結（給冒煙與壓測看）。"""

    def __init__(self):
        self.bars = []

    def add(self, res):
        self.bars.append((len(res.candidates), res.latency.get("total"), res.degraded,
                          tuple(res.degraded_reasons), len(res.signals), res.clock_offset_ms,
                          res.latency.get("finalize_wait"), res.latency.get("klines")))

    def log_summary(self, gate, prober=None):
        stats = gate.stats()
        if not self.bars:
            logger.info("本次沒有判定任何 K 棒。REST：%s", stats)
            return
        cands = sorted(b[0] for b in self.bars)
        lats = sorted(b[1] for b in self.bars if b[1] is not None)
        waits = sorted(b[6] for b in self.bars if b[6] is not None and b[0])
        kl = sorted(b[7] for b in self.bars if b[7] is not None and b[0])
        offs = [b[5] for b in self.bars]
        degraded = [b[3] for b in self.bars if b[2]]
        missed = sum(1 for b in self.bars if DEGRADED_MISSED_CLOSE in b[3])
        logger.info("總結：K 棒 %d 根（degraded %d，其中 missed_close %d：%s）、原始訊號 %d 筆、候選數 p50 %s max %s、"
                    "總延遲 p50 %.2fs p95 %.2fs max %.2fs（有候選的根：定稿等待 p50 %.2fs、klines p50 %.2fs max %.2fs）、"
                    "時鐘偏移 %+dms..%+dms、REST 請求 %d（429 %d 次）",
                    len(self.bars), len(degraded), missed, degraded, sum(b[4] for b in self.bars),
                    _pct(cands, 50), cands[-1], _pct(lats, 50), _pct(lats, 95), lats[-1] if lats else float("nan"),
                    _pct(waits, 50), _pct(kl, 50), kl[-1] if kl else float("nan"),
                    min(offs), max(offs), stats["requests"], stats["http_429"])
        if prober is not None:
            logger.info("定稿量測：重抓 %d 次、與判定時版本不同 %d 次、失敗 %d 次、尚未執行 %d 次",
                        prober.probes, prober.changed, prober.errors, prober.pending())


class _SafeArgumentParser(argparse.ArgumentParser):
    """說明與錯誤訊息是中文；Bash 下 stdout / stderr 是 cp1252，argparse 直接寫會 UnicodeEncodeError。

    跟 live.logsetup 的終端機 handler 同一招：先降級成串流編碼寫得出來的字元（編不出的變跳脫序列）
    再寫，不 reconfigure 任何全域串流，也不依賴串流本身的 errors 設定（stderr 被設成 strict 也不會炸）。
    """

    def _print_message(self, message, file=None):
        if message:
            stream = file if file is not None else sys.stderr
            encoding = getattr(stream, "encoding", None)
            if encoding:
                try:
                    message = message.encode(encoding, "backslashreplace").decode(encoding, "replace")
                except LookupError:
                    message = message.encode("ascii", "backslashreplace").decode("ascii")
        super()._print_message(message, file)


def main(argv=None):
    """python -m live.signal_feed 的進入點。回傳 exit code。"""
    from live import logsetup
    from live.reconcile import Reconciler

    parser = _SafeArgumentParser(
        prog="python -m live.signal_feed",
        description="A 頻道資料層（A1）：tickers 輪詢 → ret2h 粗篩 → 候選 klines → 原始訊號。連網實跑，觀察用。")
    parser.add_argument("--duration", type=float, default=None, metavar="SECONDS",
                        help="跑幾秒後正常結束（不給就一直跑到 Ctrl+C）")
    parser.add_argument("--record", nargs="?", const=config.SIGNAL_FEED_RECORD_DIR, default=None, metavar="DIR",
                        help="每根 K 棒的完整結果寫成 jsonl；只給旗標不給目錄時寫到 %s"
                             % config.SIGNAL_FEED_RECORD_DIR)
    parser.add_argument("--force-candidates", type=int, default=0, metavar="N",
                        help="壓測用：每根 K 棒至少取近似 ret2h 最高的 N 個當候選")
    parser.add_argument("--finality-probe", action="store_true",
                        help="驗證用：收盤後 %s 秒各重抓一次每個候選的目標 K 棒，寫進 --record 的 jsonl"
                             "（type = finality_probe），用來校正 BAR_FINALIZE_WAIT_SECONDS；需要搭配 --record"
                             % "/".join(str(x) for x in config.FINALITY_PROBE_OFFSETS_SECONDS))
    args = parser.parse_args(argv)
    if args.duration is not None and args.duration <= 0:
        parser.error("--duration 必須是正數")
    if args.force_candidates < 0:
        parser.error("--force-candidates 不可以是負數")
    record_dir = None
    if args.record:
        try:
            record_dir = check_record_dir(args.record)
        except ValueError as e:
            parser.error(str(e))
    if args.finality_probe and not record_dir:
        parser.error("--finality-probe 需要搭配 --record（量測結果寫在 jsonl 裡）")

    log_path = logsetup.setup()
    logger.info("A1 signal_feed 啟動：週期 %s、輪詢 %ss、速率上限 %d req/s、粗篩門檻 %.4f（MIN_RET_2H - %s）、"
                "定稿等待 %ss、missed_close 容忍 %ss、force-candidates %d、定稿量測 %s、duration %s、紀錄 %s、日誌 %s",
                config.KLINE_INTERVAL, config.TICKERS_POLL_SECONDS, config.A1_REST_RATE_PER_SECOND,
                screen_threshold(), config.SCREEN_RET2H_MARGIN, config.BAR_FINALIZE_WAIT_SECONDS,
                config.MISSED_CLOSE_TOLERANCE_SECONDS, args.force_candidates,
                config.FINALITY_PROBE_OFFSETS_SECONDS if args.finality_probe else "關", args.duration,
                record_dir or "不寫", log_path)

    gate = rest_gate.shared_gate()
    recorder = JsonlRecorder(record_dir) if record_dir else None
    if recorder is not None:
        logger.info("紀錄檔：%s", recorder.path)
    stats = _RunStats()

    def on_result(res):
        stats.add(res)
        if recorder is not None:
            recorder.write("bar", res.to_dict())

    def on_reconcile(rr):
        if recorder is not None:
            recorder.write("reconcile", rr.to_dict())

    def on_probe(rec):
        if recorder is not None:
            recorder.write("finality_probe", rec)

    prober = FinalityProber(gate, on_probe=on_probe) if args.finality_probe else None
    feed = SignalFeed(gate=gate, force_candidates=args.force_candidates, on_result=on_result,
                      reconciler=Reconciler(gate, on_result=on_reconcile), finality_prober=prober)
    rc = 0
    try:
        feed.run(duration_s=args.duration)
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，結束")
    except UniverseUnavailable as e:
        logger.error("無法取得標的池，結束：%s", e)
        rc = 1
    finally:
        feed.close()
        stats.log_summary(gate, prober)
        if recorder is not None:
            recorder.close()
    return rc


# ============================== 小工具 ==============================
def _num(x):
    """數值 → float；NaN / inf / None → None。"""
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _clean(obj):
    """遞迴把 NaN / inf 換成 None、numpy 數值換成 Python 數值，讓 json.dumps(allow_nan=False) 可以寫。"""
    if isinstance(obj, dict):
        return {str(k): _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_clean(v) for v in obj]
    if isinstance(obj, bool) or obj is None or isinstance(obj, str):
        return obj
    if isinstance(obj, int):
        return int(obj)
    if hasattr(obj, "item") and not isinstance(obj, (bytes, bytearray)):
        try:
            obj = obj.item()
        except (TypeError, ValueError):
            pass
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int):
        return int(obj)
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    return obj


def _taipei(ms):
    """UTC epoch ms → 台北時間 'MM-DD HH:MM'（給人看的日誌用；與主機時區無關）。"""
    from live.logsetup import TAIPEI
    return datetime.fromtimestamp(ms / 1000.0, TAIPEI).strftime("%m-%d %H:%M")


def _taipei_s(ms):
    """UTC epoch ms → 台北時間 'MM-DD HH:MM:SS'。"""
    from live.logsetup import TAIPEI
    return datetime.fromtimestamp(ms / 1000.0, TAIPEI).strftime("%m-%d %H:%M:%S")


def _duration(ms):
    """毫秒 → '12h03m' / '4m12s' / '8.5s'。"""
    s = max(0.0, ms / 1000.0)
    if s >= 3600:
        return "%dh%02dm" % (s // 3600, (s % 3600) // 60)
    if s >= 60:
        return "%dm%02ds" % (s // 60, s % 60)
    return "%.1fs" % s


def _pct(sorted_vals, q):
    if not sorted_vals:
        return float("nan")
    k = min(len(sorted_vals) - 1, max(0, int(math.ceil(q / 100.0 * len(sorted_vals))) - 1))
    return sorted_vals[k]


if __name__ == "__main__":
    # `python -m live.signal_feed` 會把本檔載入成 __main__：logger 名稱變成 "__main__"，而且別的模組
    # import live.signal_feed 時會拿到第二份模組物件。所以從正式的模組名取 main() 來跑。
    from live.signal_feed import main as _main
    raise SystemExit(_main())
