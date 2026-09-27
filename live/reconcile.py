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
      應有根數     區間內、feed 運作期間應有的每一根收盤。完全沒有紀錄（既沒有判定、也不是 missed_close）
                   的根 > 0 記 ERROR，列出
      未判定       missed_close 或完全沒有紀錄的根上，真實 ret2h >= MIN_RET_2H 的 (symbol, K 棒)。
                   與漏失分開計（那幾根根本沒有粗篩），> 0 記 WARNING，列出
      漏失         真實 ret2h >= MIN_RET_2H，但當時不在候選清單 → > 0 記 ERROR，列出。
                   degraded 的根依原因決定排除範圍（A1-f FR-3，見下面「degraded 的排除範圍」）
      近似誤差     |近似 - 真實| 的 p50 / p99 / max（有近似值的 (symbol, K 棒) 全部算）→ INFO
      K 棒定稿     當時判定用的目標 K 棒 OHLCV vs 對帳時重抓的同一根 → 不一致 > 0 記 WARNING。
                   兩邊的字串一律用 live.klines.ohlcv_by_open / prepare_klines 的同一個轉法（pd.to_numeric），
                   不會因為 float() 與 pd.to_numeric 差 1 ULP 而誤報。這就是上線後 K 棒定稿的持續監控
  * 不回頭補發訊號（晚一小時的訊號沒有意義），只記錄與告警。日後 A4 接這裡的 ERROR。

──────────────────────────────────────────────────────────────────────
長停頓後：紀錄保留與補做（A1-f FR-1）
──────────────────────────────────────────────────────────────────────
以前每收一根就把「最新收盤 - 2 個間隔 - 一根」之前的紀錄修掉。停頓超過約 2 小時 5 分醒來時，
停頓期間的 missed_close 一口氣進來、最新收盤往前跳，停頓前那一小時的紀錄隨即被修掉，
那一小時的批次再跑就誤報「應有的根完全沒有紀錄」；停頓期間的整點批次也不會補做。現在：

  紀錄保留   一筆紀錄留到它所屬的批次處理完（對帳完成，或判定超出涵蓋）才修掉；另有上限：
             只留最新一根往前 RECONCILE_KLINES_LIMIT 根主週期（100 x 5 分 = 8 小時 20 分，最多 100 筆）。
             再舊的根，任何一次對帳取的 klines 都涵蓋不到它的真實 ret2h，留著也沒用
  涵蓋範圍   klines 不帶 endTime、limit N：回應最舊的一根開盤 >= 目前這根的開盤 - (N-1) 根（保守地假設
             回應含未收完那根）。窗口 (R-1h, R] 第一根的 ret2h 還要往前 2 小時的收盤，所以在伺服器時刻 t
             取數能完整對帳的條件是 R - 1h - 2h >= open(t) - (N-1) 根，也就是 R 在 t 之前約 5 小時 15 分內
             （N=100）。判斷用的 t 取「這一輪最後一個請求最晚送出的時刻」= 開始時刻 + 攤平時間，
             比對前再用當下時刻檢查一次（中途又停頓就不拿過期的資料比）
  補做       排程記住「下一個還沒做的窗口」，不再每次從現在往後找下一個整點。醒來時已經到期的窗口：
               * 還在涵蓋內的 → 全部合併成一輪取數（每個 symbol 只打一次 klines，同一份資料切給各窗口），
                 照常攤平在 RECONCILE_SPREAD_FRACTION x 間隔內、PRIORITY_BACKGROUND，比完依時間順序
                 每個窗口各交出一個結果、各記自己的日誌。請求數跟一般的一批一樣，不會因為補做而加倍
               * 已經超出涵蓋的 → 合併成一行 WARNING（幾個批次、哪一段、為什麼），不逐批記 ERROR；
                 每個窗口仍交出一個 out_of_coverage=True 的結果（給 jsonl）
             取數途中停頓（最常見：闔蓋時對帳正在攤平）：醒來發現已經有下一個窗口到期、或這一輪的窗口
             已經超出涵蓋，就把這一輪交回排程，與其他到期的窗口一起重新分類、重新攤平 —— 不會把剩下的
             幾百個請求在期限過後一口氣補打，超出涵蓋的也只記那一行 WARNING
             run_batch() 直接呼叫（測試 / 手動）不會交回：照舊重新攤平，超出涵蓋的窗口標 out_of_coverage

──────────────────────────────────────────────────────────────────────
degraded 的排除範圍（A1-f FR-3）
──────────────────────────────────────────────────────────────────────
以前任何 degraded 都讓整根的所有 symbol 退出漏失統計。現在依原因分兩種（常數見下方）：
  影響粗篩的（SCREEN_DEGRADED_REASONS）  整根排除，計入 misses_on_degraded（原本的語意）
  只影響個別候選的（FETCH_DEGRADED_REASONS）某些候選的 klines 沒拿到、沒判定完。粗篩本身是完整的，
                                         所以同一根其他 symbol 的漏失**照算**；取數失敗的那幾個候選另外
                                         記在 excluded_fetch_failed（它們在候選清單裡，本來就不會算成漏失）
不認得的原因一律當作影響粗篩（保守：整根排除，同修改前）。

需要的粗篩紀錄只放記憶體：每根 K 棒一筆 BarRecord（每個 symbol 的近似 ret2h、候選、未篩原因、
判定用的目標 K 棒、degraded 原因與取數失敗的候選）。
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

# 下面的字串都與 live.signal_feed 的常數相同（不 import signal_feed：它 import 本模組的使用者，會循環相依）。
# 兩邊一致由 tests/test_a1f_followups.py 斷言。
_MISSED_CLOSE = "missed_close"     # = signal_feed.DEGRADED_MISSED_CLOSE

# ---- degraded 原因的影響範圍（FR-3）----
# 影響粗篩 → 整根退出漏失統計：
#   "cold_start"         signal_feed.UNSCREENED_COLD_START       冷啟動中，部分 symbol 還沒有 2 小時前樣本
#   "tickers_failed"     signal_feed.UNSCREENED_TICKERS_FAILED   收盤那次 tickers 失敗，整根沒篩
#   "banned"             signal_feed.UNSCREENED_BANNED           收盤那次 tickers 因 429 封鎖冷卻沒送，整根沒篩
#   "late_close_sample"  signal_feed.DEGRADED_LATE_CLOSE_SAMPLE  收盤樣本太晚，近似 ret2h 不是收盤價算的
#   "no_base_over_cap"   signal_feed.DEGRADED_NO_BASE_OVER_CAP   沒有 2 小時前樣本的超過升格上限，有的沒篩到
#   "missed_close"       signal_feed.DEGRADED_MISSED_CLOSE       收盤時沒處理，整根沒篩（另外列「未判定」）
SCREEN_DEGRADED_REASONS = ("cold_start", "tickers_failed", "banned", "late_close_sample", "no_base_over_cap",
                           _MISSED_CLOSE)
# 只影響個別候選 → 只排除取數失敗的那幾個 symbol。= signal_feed._DEGRADING_FETCH_FAILURES：
#   "target_missing"     signal_feed.FETCH_TARGET_MISSING        重試用盡，回應裡仍沒有剛收完的那根
#   "api_error"          signal_feed.FETCH_API_ERROR             派網回錯誤 / 連線失敗
#   "banned"             signal_feed.FETCH_BANNED                候選 klines 因 429 封鎖冷卻沒送
#   "exception"          signal_feed.FETCH_EXCEPTION             取數或計算時的未預期例外
FETCH_DEGRADED_REASONS = ("target_missing", "api_error", "banned", "exception")
# 撞名陷阱："banned" 同時是 UNSCREENED_BANNED（tickers，整根）與 FETCH_BANNED（候選 klines，個別），
# 是同一個字串，光看 degraded_reasons 分不出來。一律看 BarResult 的結構：unscreened["banned"] 有東西
# = tickers 被封鎖 → 整根；只有 fetch_failed["banned"] 有東西 = 候選 klines 被封鎖 → 個別。
_BANNED = "banned"


@dataclass(frozen=True)
class BarRecord:
    """一根 K 棒當時的粗篩紀錄（從 BarResult 摘出來，對帳只需要這些）。

    degraded_reasons  BarResult.degraded_reasons 原樣
    fetch_failed      取數失敗（會讓該根 degraded 的那幾種，FETCH_DEGRADED_REASONS）的候選 → 原因
    candidates_only   degraded 只因為 fetch_failed 裡的個別候選，粗篩本身完整 → 漏失統計只排除那幾個。
                      預設 False：沒有原因資訊的 degraded 紀錄（例如直接建構的）保守地整根排除，同修改前
    """
    close_ms: int
    degraded: bool
    universe: frozenset
    candidates: frozenset
    approx: dict            # symbol → 近似 ret2h
    unscreened: dict        # symbol → 未篩原因
    target_bars: dict       # symbol → (o, h, l, c, v)：當時實際判定的目標 K 棒
    missed_close: bool = False
    degraded_reasons: tuple = ()
    fetch_failed: dict = field(default_factory=dict)
    candidates_only: bool = False

    @property
    def excluded_whole(self):
        """整根退出漏失統計（degraded 且原因影響粗篩，或原因不明）。"""
        return self.degraded and not self.candidates_only


def _candidates_only(res):
    """BarResult 的 degraded 是否只來自個別候選的取數失敗（粗篩完整）。不是 degraded 回 False。"""
    reasons = list(res.degraded_reasons or ())
    if not reasons:
        return False
    for reason in reasons:
        if reason == _BANNED:
            # 見 _BANNED 的註解：tickers 被封鎖（或結構上看不出是哪一種）→ 保守地整根排除
            if res.unscreened.get(_BANNED) or not res.fetch_failed.get(_BANNED):
                return False
            continue
        if reason not in FETCH_DEGRADED_REASONS:
            return False          # 影響粗篩的原因，或不認得的原因
    return True


def record_from_result(res):
    """signal_feed.BarResult → BarRecord。只讀 BarResult，不改它（A3 也吃同一個物件）。"""
    unscreened = {s: reason for reason, syms in res.unscreened.items() for s in syms}
    universe = frozenset(res.approx_ret2h) | frozenset(unscreened)
    fetch_failed = {}
    for reason in FETCH_DEGRADED_REASONS:
        for s in res.fetch_failed.get(reason) or ():
            fetch_failed.setdefault(s, reason)
    reasons = tuple(res.degraded_reasons)
    return BarRecord(
        close_ms=int(res.bar_close_ms),
        degraded=bool(res.degraded),
        universe=universe,
        candidates=frozenset(c.symbol for c in res.candidates),
        approx=dict(res.approx_ret2h),
        unscreened=unscreened,
        target_bars={s: tuple(ev["target_bar"]) for s, ev in res.judged.items() if ev.get("target_bar")},
        missed_close=_MISSED_CLOSE in reasons,
        degraded_reasons=reasons,
        fetch_failed=fetch_failed,
        candidates_only=_candidates_only(res),
    )


@dataclass
class ReconcileResult:
    window_start_ms: int
    window_end_ms: int
    min_ret_2h: float
    bars: int = 0
    bars_degraded: int = 0
    bars_degraded_whole: int = 0                           # 其中整根排除的（影響粗篩的原因，含 missed_close）
    bars_degraded_partial: int = 0                         # 其中只有個別候選取數失敗的（其他 symbol 照算漏失）
    bars_expected: int = 0                                 # 區間內 feed 運作期間應有的根數
    bars_missing: list = field(default_factory=list)       # 應有但完全沒有紀錄的收盤時刻
    bars_missed_close: list = field(default_factory=list)  # 有紀錄但是 missed_close 的收盤時刻
    symbols: int = 0
    requests: int = 0
    respread: int = 0                                      # 落後後重新攤平了幾次
    fetch_failed: dict = field(default_factory=dict)       # 原因 → [symbol]
    pairs_compared: int = 0                                # 有真實 ret2h 的 (symbol, K 棒)，不含未判定的根
    true_unavailable: int = 0                              # 重抓的資料算不出真實 ret2h 的 (symbol, K 棒)
    misses: list = field(default_factory=list)             # 漏失明細（整根排除的 degraded 根不算）
    misses_on_degraded: int = 0                            # 整根排除的 degraded 根上，本來會算成漏失的組數
    excluded_fetch_failed: list = field(default_factory=list)   # 個別排除：只有個別候選取數失敗的根上，
    #                                                        取數失敗的候選 (symbol, K 棒)，含真實 ret2h
    undetermined: list = field(default_factory=list)       # missed_close / 沒紀錄的根上真實 ret2h >= MIN 的
    abs_error: dict = field(default_factory=dict)          # n / p50 / p99 / max
    ohlcv_checked: int = 0
    ohlcv_mismatches: list = field(default_factory=list)
    ohlcv_unverifiable: int = 0
    send_monotonic: list = field(default_factory=list)     # 每個對帳請求領到額度的單調時刻（診斷 / 測試）
    aborted: bool = False
    out_of_coverage: bool = False                          # 超出 klines 涵蓋、沒有對帳（只有這個欄位有意義）
    pass_windows: int = 0                                  # 幾個窗口共用這一輪取數（requests / respread /
    #                                                        fetch_failed / symbols 是整輪的）；0 = 沒有取數
    elapsed_s: float = 0.0

    def to_dict(self):
        d = dict(self.__dict__)
        d.pop("send_monotonic", None)
        return d

    def excluded_hits(self):
        """個別排除的候選裡，真實 ret2h >= MIN_RET_2H 的組數。"""
        return sum(1 for e in self.excluded_fetch_failed
                   if e["true_ret2h"] is not None and e["true_ret2h"] >= self.min_ret_2h)


class Reconciler:
    """背景對帳。add_bar() 收每根 K 棒的結果；start() 開背景執行緒依序處理每個窗口（含停頓後的補做）；
    run_batch() 可直接同步呼叫單一窗口（測試用）。所有等待經過 clock，測試可注入假時鐘。

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
        self._interval_ms = int(self.interval_s * 1000)
        self._delay_ms = int(self.start_delay_s * 1000)
        # s4_signal.features 的 ret2h = close / close.shift(2 * bars_per_hour) - 1：往前看 2 小時
        self.horizon_ms = 2 * self.bph * self.bar_ms
        # 紀錄保留上限（見模組 docstring「紀錄保留」）：最新一根往前 klines_limit 根，最多 klines_limit 筆
        self.retention_ms = self.klines_limit * self.bar_ms
        self._params_fn = params_fn or config.strategy_params
        self._on_result = on_result
        self._lock = threading.Lock()
        self._records = []
        self._expect_from = None
        self._first_close_seen = None
        self._done_through = None      # 這個窗口結束時刻（含）以前的窗口都處理完了（對帳完成或超出涵蓋）
        self._server_clock = None
        self._thread = None
        self._stop = None

    # ---------------- 紀錄 ----------------
    def expect_bars_from(self, close_ms):
        """feed 從這一根收盤開始負責（之前的根不算「應有」）。"""
        with self._lock:
            self._expect_from = int(close_ms)

    def add_bar(self, res):
        rec = record_from_result(res)
        with self._lock:
            self._records = [r for r in self._records if r.close_ms != rec.close_ms]
            self._records.append(rec)
            if self._first_close_seen is None or rec.close_ms < self._first_close_seen:
                self._first_close_seen = rec.close_ms
            self._prune_locked()

    def _prune_locked(self):
        """修掉所屬批次已經處理完的紀錄，以及超出保留上限的紀錄。呼叫時必須持有 self._lock。"""
        if not self._records:
            return
        floor = max(r.close_ms for r in self._records) - self.retention_ms
        done = self._done_through
        self._records = [r for r in self._records
                         if r.close_ms > floor and (done is None or r.close_ms > done)]

    def _mark_done(self, window_end_ms):
        """window_end_ms（含）以前的窗口都處理完了：它們的紀錄可以修掉。"""
        with self._lock:
            if self._done_through is None or window_end_ms > self._done_through:
                self._done_through = int(window_end_ms)
            self._prune_locked()

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

    # ---------------- 涵蓋範圍 ----------------
    def earliest_coverable_end(self, fetch_ms):
        """在伺服器時刻 fetch_ms 打的 klines（不帶 endTime、limit klines_limit）能完整對帳的最早窗口結束時刻。

        回應最舊的一根開盤 >= 目前這根的開盤 - (limit - 1) 根（保守：假設回應含未收完那根）。窗口
        (R - 間隔, R] 第一根（開盤 = R - 間隔）的 ret2h 還要再往前 horizon 那一根的收盤，所以
        R - 間隔 - horizon >= 最舊開盤 ⇔ R >= 最舊開盤 + horizon + 間隔。
        """
        oldest_open = (int(fetch_ms) // self.bar_ms) * self.bar_ms - (self.klines_limit - 1) * self.bar_ms
        return oldest_open + self.horizon_ms + self._interval_ms

    def covers(self, window_end_ms, fetch_ms):
        """在伺服器時刻 fetch_ms 取數，能不能完整對帳 (window_end_ms - 間隔, window_end_ms]。"""
        return int(window_end_ms) >= self.earliest_coverable_end(fetch_ms)

    def _now_ms(self):
        sc = self._server_clock or getattr(self.gate, "server_clock", None)
        return sc.now_ms() if sc is not None else self.clock.time_ms()

    # ---------------- 一批 / 一輪 ----------------
    def run_batch(self, window_end_ms, stop_event=None):
        """對帳 (window_end_ms - 間隔, window_end_ms] 內的 K 棒。回傳 ReconcileResult（也交給 on_result）。
        窗口已經超出 klines 涵蓋 → 不取數、不比對，回傳 out_of_coverage=True 的結果並記一行 WARNING。"""
        results, _ = self._run_pass([window_end_ms], stop_event, scheduler=False)
        return results[0]

    def catch_up(self, first_end_ms, now_ms=None, stop_event=None):
        """排程用：處理 first_end_ms 起、到 now_ms（伺服器時間）為止已經到了開始時刻的每一個窗口。

        涵蓋內的合併成一輪取數、依序各交出結果；超出涵蓋的合併成一行 WARNING。取數途中停頓而交回的，
        回傳交回的第一個窗口（呼叫端立刻再呼叫一次，會連同新到期的窗口重新分類）。
        回傳下一個還沒處理的窗口結束時刻。
        """
        now_ms = self._now_ms() if now_ms is None else int(now_ms)
        first = int(first_end_ms)
        last = ((now_ms - self._delay_ms) // self._interval_ms) * self._interval_ms
        if last < first:
            return first
        due = list(range(first, last + 1, self._interval_ms))
        _, handed_back = self._run_pass(due, stop_event, scheduler=True)
        if handed_back:
            self._mark_done(handed_back[0] - self._interval_ms)   # 交回之前的窗口（略過 / 超出涵蓋）已處理完
            return handed_back[0]
        self._mark_done(due[-1])
        return due[-1] + self._interval_ms

    def _run_pass(self, window_ends, stop_event, scheduler):
        """一輪取數，對帳一個或多個窗口。回傳 (由舊到新每個窗口的 ReconcileResult, 交回排程的窗口)。

        scheduler=True（catch_up）：取數途中停頓到「已經有下一個窗口到期」或「這一輪有窗口超出涵蓋」，
        就不再送這一輪剩下的請求，整輪交回（回傳 ([], 交回的窗口)，不交出任何結果）。
        scheduler=False（run_batch）：照舊重新攤平；超出涵蓋的窗口標 out_of_coverage、不比對。
        """
        interval_ms = self._interval_ms
        params = self._params_fn()
        min_ret = params["MIN_RET_2H"]
        span = self.interval_s * self.spread_fraction
        t0 = self.clock.monotonic()
        start_ms = self._now_ms()
        results = {}
        active, uncovered = [], []
        for w in sorted({int(x) for x in window_ends}):
            ws = w - interval_ms
            results[w] = ReconcileResult(ws, w, min_ret)
            if not self._window_records(ws, w) and not self.expected_closes(ws, w):
                logger.info("背景對帳 %s：區間內沒有粗篩紀錄，略過", _span(ws, w))
                continue
            # 最後一個請求最晚在 t0 + span 送出：以那個時刻判斷涵蓋
            if self.covers(w, start_ms + int(span * 1000)):
                active.append(w)
            else:
                results[w].out_of_coverage = True
                uncovered.append(w)
        self._report_uncovered(uncovered, start_ms + int(span * 1000), start_ms)
        for w in uncovered:
            self._emit(results[w])

        acc = ReconcileResult(0, 0, min_ret)            # 這一輪取數的共用統計
        digests = {w: {} for w in active}
        symbols = []
        if active:
            source = [r for w in active for r in self._window_records(w - interval_ms, w)] or self.records()
            symbols = sorted(set().union(*(r.universe for r in source))) if source else []
            if len(active) > 1:
                logger.info("背景對帳補做 %d 個窗口（%s）：共用一輪取數，%d 個 symbol 攤平在 %.0f 秒內",
                            len(active), _windows_label(active, interval_ms), len(symbols), span)
        acc.symbols = len(symbols)
        dropped = []

        def drop(lost):
            for w in lost:
                active.remove(w)
                digests.pop(w, None)
                results[w].out_of_coverage = True
            dropped.extend(lost)

        # ---- 取數：攤平在 span 內；落後超過一格就把剩下的重新攤平到剩餘時間（或交回排程）----
        n = len(symbols)
        deadline = t0 + span
        base_t, base_i, step = t0, 0, (span / n if n else 0.0)
        for i, sym in enumerate(symbols):
            if not active:
                break
            if stop_event is not None and stop_event.is_set():
                acc.aborted = True
                break
            target = base_t + (i - base_i) * step
            self._sleep_until(target, stop_event)
            # 停頓發生在睡覺或上一個請求期間，所以醒來之後才檢查：落後超過一格 → 從這一個起，
            # 剩下的重新攤平到期限前（step 已經是 0、期限已過就不再重排）
            now = self.clock.monotonic()
            if i and step > 0 and now > target + step:
                server_now = self._now_ms()
                lost = [w for w in active if not self.covers(w, server_now + int(max(0.0, deadline - now) * 1000))]
                behind = server_now >= active[-1] + interval_ms + self._delay_ms
                if scheduler and (lost or behind):
                    logger.info("背景對帳 %s：取數途中停頓（落後 %s）%s，這一輪交回排程，與其他到期的窗口一起重新安排",
                                _windows_label(active, interval_ms), _dur((now - target) * 1000),
                                "，有窗口已超出 klines 涵蓋" if lost else "，下一個窗口已經到期")
                    return [], list(active)
                if lost:
                    drop(lost)
                    if not active:
                        break
                left = n - i
                step = max(0.0, deadline - now) / left
                base_t, base_i = now, i
                acc.respread += 1
                logger.info("背景對帳 %s：進度落後（行程停頓？），剩下 %d 個 symbol 重新攤平到 %.0f 秒內",
                            _windows_label(active, interval_ms), left, max(0.0, deadline - now))
            rows, fail = self._fetch(sym, acc, stop_event)
            if rows is None:
                if fail is None:          # 被 stop 打斷
                    acc.aborted = True
                    break
                acc.fetch_failed.setdefault(fail, []).append(sym)
                continue
            try:
                d = {w: self._digest(rows, w - interval_ms, w) for w in active}
            except Exception as e:  # noqa: BLE001 —— 單一 symbol 的資料怪異不可以讓整批停掉
                acc.fetch_failed.setdefault("exception", []).append(sym)
                logger.error("背景對帳 %s 整理資料時發生未預期的例外：%s", sym, e,
                             exc_info=(type(e), e, e.__traceback__))
                continue
            for w, v in d.items():
                digests[w][sym] = v

        # ---- 比對前再確認一次涵蓋：最後一個請求期間又停頓的話，紀錄可能已經被保留上限修掉 ----
        if active and not acc.aborted:
            server_now = self._now_ms()
            lost = [w for w in active if not self.covers(w, server_now)]
            if lost and scheduler:
                logger.info("背景對帳 %s：取數完成時已有窗口超出 klines 涵蓋（行程停頓？），這一輪交回排程重新安排",
                            _windows_label(active, interval_ms))
                return [], list(active)
            if lost:
                drop(lost)
        if dropped:
            self._report_uncovered(dropped, self._now_ms(), start_ms)
            for w in sorted(dropped):
                self._emit(results[w])

        # ---- 比對：用現在的紀錄（取數期間補上的 missed_close 也算得到），依時間順序 ----
        for w in active:
            self._finish_window(results[w], digests[w], acc, len(active), min_ret, t0)
        return [results[w] for w in sorted(results)], []

    def _finish_window(self, result, digests, acc, pass_windows, min_ret, t0):
        ws, we = result.window_start_ms, result.window_end_ms
        recs = self._window_records(ws, we)
        expected = self.expected_closes(ws, we)
        recorded = {r.close_ms for r in recs}
        result.bars = len(recs)
        result.bars_degraded = sum(1 for r in recs if r.degraded)
        result.bars_degraded_whole = sum(1 for r in recs if r.excluded_whole)
        result.bars_degraded_partial = result.bars_degraded - result.bars_degraded_whole
        result.bars_expected = len(expected)
        result.bars_missing = [c for c in expected if c not in recorded]
        result.bars_missed_close = [r.close_ms for r in recs if r.missed_close]
        result.symbols = acc.symbols
        result.requests = acc.requests
        result.respread = acc.respread
        result.fetch_failed = {k: list(v) for k, v in acc.fetch_failed.items()}
        result.send_monotonic = list(acc.send_monotonic)
        result.aborted = acc.aborted
        result.pass_windows = pass_windows
        errors = []
        for sym, digest in digests.items():
            self._compare(sym, digest, recs, result.bars_missing, min_ret, result, errors)
        result.undetermined.sort(key=lambda u: (u["bar_close_ms"], u["symbol"]))
        result.excluded_fetch_failed.sort(key=lambda u: (u["bar_close_ms"], u["symbol"]))
        if errors:
            arr = np.asarray(errors, dtype=float)
            result.abs_error = {"n": int(arr.size), "p50": float(np.percentile(arr, 50)),
                                "p99": float(np.percentile(arr, 99)), "max": float(arr.max())}
        else:
            result.abs_error = {"n": 0, "p50": None, "p99": None, "max": None}
        result.elapsed_s = self.clock.monotonic() - t0
        self._log(result)
        self._emit(result)

    def _emit(self, result):
        if self._on_result is not None:
            try:
                self._on_result(result)
            except Exception:  # noqa: BLE001
                logger.exception("背景對帳 on_result 回呼失敗")

    def _report_uncovered(self, windows, check_ms, start_ms):
        """超出涵蓋的窗口合併成一行 WARNING（不逐批記）。"""
        if not windows:
            return
        windows = sorted(windows)
        logger.warning("背景對帳：%d 個批次超出 klines 涵蓋、不對帳（窗口 %s～%s，最舊的已結束 %s；"
                       "每個 symbol 只取最近 %d 根 %s，真實 ret2h 還要往前 2 小時，現在只對得到 %s 以後結束的窗口）。"
                       "多半是行程停頓過久",
                       len(windows), _hhmm(windows[0] - self._interval_ms), _hhmm(windows[-1]),
                       _dur(start_ms - windows[0]), self.klines_limit, self.interval,
                       _hhmm(self.earliest_coverable_end(check_ms)))

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
            if rec.degraded and rec.candidates_only and sym in rec.fetch_failed:
                # 個別排除：這個候選當時取數失敗、沒有判定完。它在候選清單裡，本來就不會算成漏失；
                # 另外記下來（真實 ret2h 夠高的就是「可能的訊號沒判定」）。同一根其他 symbol 照常比對
                result.excluded_fetch_failed.append({"symbol": sym, "bar_close_ms": rec.close_ms,
                                                     "true_ret2h": true if valid else None,
                                                     "why": rec.fetch_failed[sym]})
            if not valid:
                result.true_unavailable += 1
            else:
                result.pairs_compared += 1
                approx = rec.approx.get(sym)
                if approx is not None:
                    errors.append(abs(approx - true))
                if true >= min_ret and sym not in rec.candidates:
                    if rec.excluded_whole:
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
        logger.info("背景對帳 %s：應有 %d 根、有紀錄 %d 根（degraded %d：整根排除 %d（其中 missed_close %d）、"
                    "只排除取數失敗的候選 %d）、缺紀錄 %d 根、symbol %d、比對 %d 組（算不出真實值 %d）、"
                    "漏失 %d（整根排除的根上另有 %d；個別排除取數失敗的候選 %d 組，其中真實 ret2h >= MIN %d）、"
                    "未判定 %d、近似誤差 n=%s p50=%s p99=%s max=%s、K 棒定稿比對 %d 筆不一致 %d（無法比對 %d）、"
                    "取數失敗 %s、請求 %d、重新攤平 %d 次、耗時 %.0fs%s%s",
                    span, r.bars_expected, r.bars, r.bars_degraded, r.bars_degraded_whole,
                    len(r.bars_missed_close), r.bars_degraded_partial, len(r.bars_missing),
                    r.symbols, r.pairs_compared, r.true_unavailable, len(r.misses), r.misses_on_degraded,
                    len(r.excluded_fetch_failed), r.excluded_hits(),
                    len(r.undetermined), e.get("n"), _fmt(e.get("p50")), _fmt(e.get("p99")), _fmt(e.get("max")),
                    r.ohlcv_checked, len(r.ohlcv_mismatches), r.ohlcv_unverifiable,
                    {k: len(v) for k, v in r.fetch_failed.items()}, r.requests, r.respread, r.elapsed_s,
                    "（%d 個窗口共用這一輪取數，請求數是整輪的）" % r.pass_windows if r.pass_windows > 1 else "",
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
            logger.error("背景對帳 %s：漏失 %d 筆（真實 ret2h >= MIN_RET_2H %s 但不在候選；K 棒沒有影響粗篩的 "
                         "degraded）：%s",
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
        """開背景執行緒：依序處理每個窗口（停頓後補做）。stop_event 設起來就在下一個請求前停。"""
        if self._thread is not None:
            return
        self._stop = stop_event
        self._server_clock = server_clock
        self._thread = threading.Thread(target=self._loop, args=(stop_event, server_clock),
                                        name="a1-reconcile", daemon=True)
        self._thread.start()

    def join(self, timeout=None):
        if self._thread is not None:
            self._thread.join(timeout)

    def _loop(self, stop, server_clock):
        """排程：next_end 是下一個還沒處理的窗口。等到它的開始時刻，把到期的窗口一次處理掉（catch_up）。
        以前每一圈都從「現在」往後找下一個整點，停頓期間的窗口因此永遠不會補做。"""
        self._server_clock = server_clock
        next_end = (server_clock.now_ms() // self._interval_ms + 1) * self._interval_ms
        while not stop.is_set():
            start_at = next_end + self._delay_ms
            while not stop.is_set():
                remaining = (start_at - server_clock.now_ms()) / 1000.0
                if remaining <= 0:
                    break
                self.clock.sleep(min(remaining, 0.5))
            if stop.is_set():
                break
            try:
                next_end = self.catch_up(next_end, server_clock.now_ms(), stop)
            except Exception:  # noqa: BLE001 —— 一批出錯不可以讓之後的對帳全部停掉
                logger.exception("背景對帳這一輪發生未預期的例外，跳過已到期的窗口，下一個間隔再試")
                now = server_clock.now_ms()
                skip_to = ((now - self._delay_ms) // self._interval_ms + 1) * self._interval_ms
                self._mark_done(skip_to - self._interval_ms)
                next_end = max(next_end + self._interval_ms, skip_to)

    def _sleep_until(self, mono_t, stop_event):
        while stop_event is None or not stop_event.is_set():
            remaining = mono_t - self.clock.monotonic()
            if remaining <= 0:
                return
            self.clock.sleep(min(remaining, 0.5))


def _span(a, b):
    return f"{_hhmm(a)}–{_hhmm(b)}"


def _windows_label(window_ends, interval_ms):
    if len(window_ends) == 1:
        return _span(window_ends[0] - interval_ms, window_ends[0])
    return "%s～%s 共 %d 個窗口" % (_hhmm(window_ends[0] - interval_ms), _hhmm(window_ends[-1]), len(window_ends))


def _hhmm(ms):
    from datetime import datetime
    from live.logsetup import TAIPEI
    return datetime.fromtimestamp(ms / 1000.0, TAIPEI).strftime("%m-%d %H:%M")


def _dur(ms):
    """毫秒 → '12h03m' / '4m12s' / '8.5s'（與 live.signal_feed._duration 同格式）。"""
    s = max(0.0, ms / 1000.0)
    if s >= 3600:
        return "%dh%02dm" % (s // 3600, (s % 3600) // 60)
    if s >= 60:
        return "%dm%02ds" % (s // 60, s % 60)
    return "%.1fs" % s


def _fmt(x):
    return "None" if x is None else "%.5f" % x
