# -*- coding: utf-8 -*-
"""
live.notional_tracker — A 頻道名目部位追蹤（A3）：原始訊號 → 進場 → 出場監控 → 出場，重啟可復原
====================================================================================================
A 頻道發出進場訊號之後，要自己追蹤那筆「名目部位」直到止盈或止損，才發得出出場訊息。這一層做四件事：

  1. 收原始訊號（A1 的 BarResult；策略五資料層日後走 submit_signal()），排除「同策略同幣仍持倉中」
     與「冷卻中」的訊號 —— 這兩件只有這一層知道
  2. 以**訊號價**算止盈 / 止損價（R-2），寫庫（live.store）、發**進場事件**（live.bus）
  3. 每一根 1 分K 收盤後盯住所有未平倉的名目部位，判定先碰止盈還是止損，平倉寫庫、發**出場事件**
  4. 程式重啟後從資料庫復原未平倉部位、冷卻狀態與未發布的事件（FR-5）

**名目結果必須與 dry run 逐筆一致**：dry run（pionex_dryrun.step_symbol() + pionex_backtest.resolve_with_5m()）
是這套策略的參考答案。本模組不 import 它們（live/ 不可相依 pionex_*），判定規則照抄在下面「判定語意」，
tests/test_notional_tracker.py 直接拿 dry run 的真實程式當對照組逐筆比對。

──────────────────────────────────────────────────────────────────────
判定語意（對照 pionex_dryrun.step_symbol() 704–781 行、pionex_backtest.resolve_with_5m() 332–362 行）
──────────────────────────────────────────────────────────────────────
  主週期        s4 = config.KLINE_INTERVAL（A1 的判定週期）、s5 = config.S5_KLINE_INTERVAL
  監控週期      RESOLVE_SAME_BAR_WITH_5M 為真時 = RESOLVE_INTERVALS 裡比主週期小的那一個（只允許一個），
                否則 = 主週期。s4（5M，["1M"]）→ 1M；s5（1M，沒有 RESOLVE_*）→ 1M。由參數推導，不寫死
  進場          訊號 K 棒收盤價 = 訊號價 = 名目進場價；開倉時刻 opened_ms = 訊號 K 棒收盤
                （= dry run 的 entry_time = t[j] + BAR）
  止盈 / 止損   做空：止盈 = 訊號價 × (1 − TAKE_PROFIT)、止損 = 訊號價 × (1 + STOP_LOSS)，不取整
                （dry run：entry × (1 + d × tp_pct) / entry × (1 − d × sl_pct)，d = −1，浮點結果逐位元相同）
  出場檢查起點  訊號 K 棒收盤之後的第一根監控 K 棒（dry run：t[j] > entry_bar）
  觸價          逐根監控 K 棒：最低 ≤ 止盈 → 碰止盈；最高 ≥ 止損 → 碰止損；第一根碰到的那邊就是結果；
                同一根兩邊都碰 → 止損（dry run：1 分K 仍同根 → 保守計止損；s5 同根雙觸 → 保守計止損）
  出場價        止盈 = 止盈價；止損 = 止損價，但觸價那根所屬「主週期 K 棒」的開盤 > 止損價（跳空）→ 該開盤價。
                主週期開盤 = 該主週期 K 棒內第一根（真實存在的）監控 K 棒的開盤，**不是**觸價那根的開盤
                （dry run：gap = o[j] > sl，o[j] 是主週期 K 棒開盤；exit = o[j] if gap and o[j] > 0 else sl）
  出場時刻      觸價那根監控 K 棒的收盤（dry run 只碰一邊時記主週期收盤、兩碰經 1 分K 判定時記 1 分K 收盤；
                本模組只會更早或相同，且落在同一根主週期 K 棒內）
  冷卻          出場後，同策略同幣下一筆訊號的 K 棒開盤必須 ≥ 出場所屬主週期 K 棒開盤 + cooldown_bars × 主週期
                （dry run：cool_until = t[j] + COOLDOWN_BARS × BAR；進場條件 t[j] >= cool_until）。
                s4 = s4_signal.cooldown_bars(每小時根數)、s5 = s5_signal.cooldown_bars(每小時根數)。
                冷卻 ≥ 1 根，所以出場那一根本身不會再進場
  持倉中        同策略同幣有 open 部位 → 新訊號不進場（INFO，不寫庫、不發事件）
  s5 每小時一次 已經在 strategy.s5_signal.evaluate() 的 first_per_hour 做掉（訊號層只標每小時第一次成立的
                那根，與持倉無關）。這裡**不再**做每小時限制，被持倉擋掉之後也不改找同小時的下一根
  時間出場      MAX_HOLD_HOURS 兩個策略目前都是 None，不實作；不是 None（或 EXIT_MODE 不是 "fixed"）時
                build_specs() 拋 UnsupportedExitParams，拒絕啟動

為什麼用監控週期（1 分K）逐根判定，結果仍與 dry run（5 分K 偵測 + 1 分K 判先後）相同：
  * 5 分K 只碰一邊：第一根碰到的 1 分K 一定在這根 5 分K 裡，碰的是同一邊 → 結果、出場價、所屬 5 分K 相同
  * 5 分K 兩邊都碰：dry run 本來就用 1 分K 逐根找先碰的一邊（同根兩碰 → 止損），完全相同
  * 冷卻以出場所屬主週期 K 棒起算，兩邊是同一根 → 冷卻相同
  前提是 5 分K 的 OHLC = 其 1 分K 的彙總（pionex_cache 實測 876,011 根 5 分K 全部成立，見 code summary）。

**唯一無法在判定當下重現的是 dry run 的備註「同1h觸發；1分K判定先止盈 / 先止損」**：它的意思是「這根 5 分K
兩邊都碰過」，而觸價之後、同一根 5 分K 剩下的 1 分K 會不會碰到另一邊，在 1 分K 觸價當下還不知道。所以出場
稽核旗標 minor_resolved 只在「判定當下已知主週期兩邊都碰過」（= 觸價那根 1 分K 自己兩邊都碰）時為真；
結果、出場價、所屬主週期 K 棒、冷卻都不受影響。見 EXIT_FEATURE_KEYS。

──────────────────────────────────────────────────────────────────────
出場稽核旗標（ExitEvent.features，也存進 positions.exit_features_json）
──────────────────────────────────────────────────────────────────────
  gap_open          出場價是主週期開盤（開盤跳空穿越止損）  ↔ dry run 備註「開盤跳空穿越止損」
  same_bar_both     判定那根 K 棒兩邊都碰 → 保守計止損      ↔「1分K仍同根→保守計止損」（s4）/「同1h觸發→保守計止損」（s5）
  minor_resolved    判定當下已知主週期 K 棒兩邊都碰、由較小週期決定（見上一段的限制）
  minor_missing     重啟補判的主週期降級路徑：主週期兩邊都碰、小週期找不到 → 保守計止損
                                                          ↔「同1h觸發；無小週期K資料→保守計止損」
  judged_interval   實際用來偵測觸價的週期（平常 = 監控週期；降級路徑 = 主週期）
  exit_bar_open_ms  出場所屬主週期 K 棒的開盤（冷卻由它起算）
  recovered         出場時刻落在停機期間（重啟復原當下之前就已觸價），事件是延遲發布的
  data_gap          這筆部位的監控期間有一段拿不到任何 K 棒、無法判定（已記 ERROR）
  judged_ms         A3 判定出場的時刻（也是 ExitEvent.created_ms；重送時由它重建出同一個事件）

──────────────────────────────────────────────────────────────────────
執行緒與佇列
──────────────────────────────────────────────────────────────────────
  * 一個 A3 工作執行緒（start()，名稱 a3-tracker）。**Store 只在這條執行緒裡開、裡面用**
    （B4′：一個 Store 只在建立它的執行緒使用）。匯流排的 publish 也只在這條執行緒呼叫。
  * A1 的 on_result 回呼在 A1 主迴圈執行緒裡被呼叫：bar_result_handler() 回傳的回呼只把原始訊號與
    missed_close 摘要排進 queue.Queue 就返回，不碰 Store、不打 REST、不 publish。submit_signal()
    （策略五入口）同理，可以從任何執行緒呼叫。
  * 工作執行緒：每一根監控 K 棒收盤 + BAR_FINALIZE_WAIT_SECONDS（與 A1 同一個常數）做一次 tick()：
    檢查所有 open 部位、重試延後的訊號、補發未發布的事件；兩次 tick 之間處理佇列裡的訊號。
  * **不在任何匯流排 handler 裡發布事件**：本模組只在自己的執行緒、自己的程式路徑裡 publish。

──────────────────────────────────────────────────────────────────────
發布：先寫庫、at-least-once、進場先於出場
──────────────────────────────────────────────────────────────────────
進場：建 EntryEvent（驗證）→ store.record_entry() → publish → 送達報告 ok 才 mark_published()。
出場：store.close_position()（連同稽核旗標）→ publish → ok 才 mark_exit_published()。
發布一律走 _flush()：先補發所有未發布的進場（依寫入時刻），再發「進場已發布」的未發布出場（依平倉時刻）。
所以同一個 signal_id 的出場永遠不會比進場先發。report.ok 為 False 就不標記，下一次 tick 重送：
**訂閱者以 (signal_id, 事件種類) 去重**（重送的事件由資料庫同一筆紀錄重建，內容相同）。

──────────────────────────────────────────────────────────────────────
取數（FR-3）
──────────────────────────────────────────────────────────────────────
每個 open 部位每根監控 K 棒一個 klines 請求（endTime = 目標收盤 − 1，limit = 從「上次檢查點所屬的主週期
K 棒開盤」到目標收盤的根數；多取的前幾根是給跳空判定找主週期開盤用的）。全部經 rest_gate 共用閘門，
PRIORITY_NORMAL：低於 A1 的收盤前景取數，A1 前景保留期間不送。補洞一律用 live.klines.prepare_klines
（與 A1、回測同一套）。取數失敗或目標 K 棒還沒有：記 WARNING，**下一次 tick 再檢查**，不跳過、不臆測。
根數超過 A3_KLINES_PAGE_LIMIT 時用 endTime 分頁往後取（重啟補判）。

──────────────────────────────────────────────────────────────────────
重啟復原（FR-5，recover()，工作執行緒一開始就做）
──────────────────────────────────────────────────────────────────────
  1. 載入所有 open 部位（user_id = config.STRATEGY_USER_ID），止盈 / 止損價用資料庫裡進場當時的值
  2. 每個 open 部位從進場後第一根監控 K 棒起重新判定到現在（分頁取）；停機期間已觸價的，以歷史出場時刻與
     價格平倉（稽核旗標 recovered = True；這一步取數失敗的，之後的 tick 判到停機期間的出場也一樣標 recovered）。監控週期的 K 棒超過派網保留期（1 分K 約 7 天，回
     MARKET_INVALID_TIME）的區段改用 dry run 的原始做法：主週期 K 棒偵測、兩邊都碰再找小週期、找不到 →
     止損（minor_missing）；主週期也拿不到 → ERROR、data_gap，部位保持 open、從拿得到的地方繼續監控
  3. 從每個 (strategy, symbol) 最後一筆已平倉部位重建冷卻（store.last_closed_positions()）
  4. 補發所有已寫庫、未發布的進場與出場事件，進場先於出場
取數失敗的部位不阻塞復原：留到下一次 tick 繼續判定。在復原完成前收到的訊號排在佇列裡，復原後才處理。
停機期間 A1 沒有交出的訊號（寫庫之前就停了）不會補：那些訊號沒有進過資料庫、也沒有發過事件。
"""

import logging
import math
import queue
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
import pandas as pd

from live import config, klines, rest_gate
from live import store as store_mod
from live.logsetup import TAIPEI
from live.pionex_api import ApiError
from live.signal_events import EntryEvent, ExitEvent
from strategy import s4_signal, s5_signal

logger = logging.getLogger(__name__)

# 派網永續合約的後綴；signal_id 的幣名 = symbol 去掉它（WBS §5.2 的 #ACE-20260918-1535 格式）
PERP_SUFFIX = "_USDT_PERP"

# A1（live.signal_feed）degraded 原因裡的 missed_close（與 live.signal_feed.DEGRADED_MISSED_CLOSE 相同；
# 不 import signal_feed，本模組才不會把 A1 整包拖進來）
_A1_MISSED_CLOSE = "missed_close"

# 出場稽核旗標的鍵（見模組 docstring）
EXIT_FEATURE_KEYS = ("gap_open", "same_bar_both", "minor_resolved", "minor_missing", "judged_interval",
                     "exit_bar_open_ms", "recovered", "data_gap", "judged_ms")

# 依策略分派的表：每個策略的參數模組與主週期來源。它是「每個策略各自的東西」，不是策略名單；
# 鍵必須剛好等於 config.STRATEGIES（build_specs() 檢查、tests 也斷言）。
_STRATEGY_SOURCES = {
    "s4": (s4_signal, lambda: config.KLINE_INTERVAL),
    "s5": (s5_signal, lambda: config.S5_KLINE_INTERVAL),
}


class UnsupportedExitParams(ValueError):
    """strategy/ 的出場參數是本模組沒有實作的組合（EXIT_MODE 不是 fixed、有時間出場、多個小週期……）。"""


class KlinesUnavailable(Exception):
    """這段 K 棒已超過派網保留期（MARKET_INVALID_TIME），不是暫時性錯誤。"""


class _FetchFailed(Exception):
    """暫時性的取數失敗（API 錯誤、429 冷卻、回應格式不對）：下一次 tick 再試。"""


# 資料庫操作失敗（鎖、磁碟、中毒、「不該發生」的 StoreError）。任何一種都讓記憶體狀態作廢，見 _invalidate()。
_STORE_ERRORS = (sqlite3.Error, store_mod.StoreError)


# ============================== 策略規格 ==============================
@dataclass(frozen=True)
class StrategySpec:
    """一個策略的出場判定規格。全部由 strategy/ 的參數與 live/config.py 的週期推導，本模組不寫任何數值。"""
    strategy: str
    tag: str                  # signal_id 裡的策略標記（"S4" / "S5"）
    main_interval: str
    main_ms: int
    monitor_interval: str
    monitor_ms: int
    take_profit: float
    stop_loss: float
    cooldown_bars: int

    @property
    def uses_minor(self):
        """監控週期比主週期小（= dry run 的同根雙觸發會用小週期判先後）。"""
        return self.monitor_ms < self.main_ms


def _interval_ms_or_zero(interval):
    return klines.INTERVAL_MS.get(interval, 0)


def build_spec(strategy):
    """依 strategy/ 的 exit_params()、cooldown_bars() 與 config 的主週期組出 StrategySpec。不支援的組合拋例外。"""
    if strategy not in _STRATEGY_SOURCES:
        raise KeyError("策略 %r 沒有對應的參數來源（_STRATEGY_SOURCES）" % (strategy,))
    module, main_fn = _STRATEGY_SOURCES[strategy]
    main_interval = main_fn()
    main_ms = klines.interval_ms(main_interval)
    ex = module.exit_params()
    where = "strategy.%s.EXIT_PARAMS" % module.__name__.split(".")[-1]
    if ex.get("EXIT_MODE") != "fixed":
        raise UnsupportedExitParams("%s['EXIT_MODE'] = %r：A3 只實作固定比例出場（\"fixed\"），拒絕啟動"
                                    % (where, ex.get("EXIT_MODE")))
    if ex.get("MAX_HOLD_HOURS") is not None:
        raise UnsupportedExitParams("%s['MAX_HOLD_HOURS'] = %r：A3 沒有實作時間出場，拒絕啟動（要用時間出場"
                                    "得先實作並與 dry run 對照）" % (where, ex.get("MAX_HOLD_HOURS")))
    monitor_interval = main_interval
    if ex.get("RESOLVE_SAME_BAR_WITH_5M", False):
        minors = [iv for iv in ex.get("RESOLVE_INTERVALS", [])
                  if 0 < _interval_ms_or_zero(iv) < main_ms]
        if len(minors) > 1:
            raise UnsupportedExitParams(
                "%s['RESOLVE_INTERVALS'] 有多個比主週期 %s 小的週期 %r：dry run 依序嘗試、先拿到資料的那個決定，"
                "單一監控週期無法等價重現，拒絕啟動" % (where, main_interval, minors))
        if minors:
            monitor_interval = minors[0]
    monitor_ms = klines.interval_ms(monitor_interval)
    if main_ms % monitor_ms:
        raise UnsupportedExitParams("監控週期 %s 不能整除主週期 %s" % (monitor_interval, main_interval))
    tp, sl = ex.get("TAKE_PROFIT"), ex.get("STOP_LOSS")
    for name, v in (("TAKE_PROFIT", tp), ("STOP_LOSS", sl)):
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0:
            raise UnsupportedExitParams("%s[%r] = %r：必須是有限的正數" % (where, name, v))
    if tp >= 1:
        raise UnsupportedExitParams("%s['TAKE_PROFIT'] = %r：做空的止盈價會 ≤ 0" % (where, tp))
    return StrategySpec(strategy=strategy, tag=strategy.upper(), main_interval=main_interval, main_ms=main_ms,
                        monitor_interval=monitor_interval, monitor_ms=monitor_ms, take_profit=float(tp),
                        stop_loss=float(sl),
                        cooldown_bars=int(module.cooldown_bars(klines.bars_per_hour(main_interval))))


def build_specs():
    """config.STRATEGIES 裡每個策略的 StrategySpec。分派表的鍵與名單不一致時拋 KeyError（漏接新策略）。"""
    if set(_STRATEGY_SOURCES) != set(config.STRATEGIES):
        raise KeyError("A3 的分派表 %r 與 config.STRATEGIES %r 不一致" % (sorted(_STRATEGY_SOURCES),
                                                                    list(config.STRATEGIES)))
    return {s: build_spec(s) for s in config.STRATEGIES}


def levels(spec, signal_price):
    """做空的 (止盈價, 止損價)，以訊號價計，不取整。"""
    return signal_price * (1 - spec.take_profit), signal_price * (1 + spec.stop_loss)


def make_signal_id(spec, symbol, bar_close_ms):
    """#<幣名>-<S4|S5>-<YYYYMMDD>-<HHMM>：台北時間、訊號 K 棒收盤時刻；幣名 = symbol 去掉 _USDT_PERP。"""
    coin = symbol[:-len(PERP_SUFFIX)] if symbol.endswith(PERP_SUFFIX) and len(symbol) > len(PERP_SUFFIX) \
        else symbol
    t = datetime.fromtimestamp(bar_close_ms / 1000.0, TAIPEI)
    return "#%s-%s-%s" % (coin, spec.tag, t.strftime("%Y%m%d-%H%M"))


def floor_to(ms, step):
    return (int(ms) // step) * step


def exit_bar_open(closed_ms, main_ms):
    """平倉時刻（某根 K 棒的收盤，落在主週期 K 棒 (開盤, 開盤 + 主週期] 內）→ 那根主週期 K 棒的開盤。"""
    return floor_to(int(closed_ms) - 1, main_ms)


def cooldown_until(spec, closed_ms):
    """出場之後，同策略同幣的訊號 K 棒開盤要 ≥ 這個時刻才可以再進場。"""
    return exit_bar_open(closed_ms, spec.main_ms) + spec.cooldown_bars * spec.main_ms


# ============================== K 棒與判定（純函式） ==============================
class Bars:
    """整理好的 K 棒（live.klines.prepare_klines 補洞後）+ 每根是否為真實資料（不是補出來的）。"""

    __slots__ = ("step", "t", "o", "h", "l", "c", "real")

    def __init__(self, step, t, o, h, l, c, real):
        self.step, self.t, self.o, self.h, self.l, self.c, self.real = step, t, o, h, l, c, real

    @classmethod
    def empty(cls, step):
        z = np.zeros(0)
        return cls(step, np.zeros(0, dtype=np.int64), z, z, z, z, np.zeros(0, dtype=bool))

    @classmethod
    def from_rows(cls, rows, step, t_now):
        """派網 klines 的原始 rows → Bars。t_now：只留 time + step <= t_now（已收完）的 K 棒。"""
        raw = klines.raw_frame(rows)
        df, _ = klines.prepare_klines(raw, step, t_now=t_now)
        if df is None or len(df) == 0:
            return cls.empty(step)
        t = df["time"].to_numpy(dtype=np.int64)
        real_times = pd.to_numeric(raw["time"], errors="coerce").dropna().astype(np.int64).to_numpy()
        return cls(step, t, df["open"].to_numpy(dtype=float), df["high"].to_numpy(dtype=float),
                   df["low"].to_numpy(dtype=float), df["close"].to_numpy(dtype=float), np.isin(t, real_times))

    @classmethod
    def from_arrays(cls, step, t, o, h, l, c, real=None):
        t = np.asarray(t, dtype=np.int64)
        return cls(step, t, np.asarray(o, dtype=float), np.asarray(h, dtype=float), np.asarray(l, dtype=float),
                   np.asarray(c, dtype=float), np.ones(len(t), dtype=bool) if real is None else np.asarray(real))

    def __len__(self):
        return len(self.t)

    def first_real_time(self):
        idx = np.flatnonzero(self.real)
        return int(self.t[idx[0]]) if len(idx) else None

    def last_close(self):
        return int(self.t[-1]) + self.step if len(self.t) else None


@dataclass(frozen=True)
class Outcome:
    """一筆出場判定。"""
    reason: str                 # config.EXIT_TAKE_PROFIT / EXIT_STOP_LOSS
    exit_price: float
    exit_ms: int                # 判定那根 K 棒的收盤
    exit_bar_open_ms: int       # 所屬主週期 K 棒開盤
    gap_open: bool = False
    same_bar_both: bool = False
    minor_resolved: bool = False
    minor_missing: bool = False
    judged_interval: str = ""


def _stop_loss_outcome(spec, sl, main_open, exit_ms, bar_open, **flags):
    gap = bool(main_open > sl)
    price = float(main_open) if gap and main_open > 0 else float(sl)
    return Outcome(config.EXIT_STOP_LOSS, price, int(exit_ms), int(bar_open), gap_open=gap, **flags)


def scan_monitor(spec, tp, sl, bars, start_ms):
    """在監控週期的 K 棒上，從開盤 >= start_ms 的那根起逐根判定。回傳 (Outcome 或 None, 檢查到的收盤時刻或 None)。

    跳空判定用觸價那根所屬主週期 K 棒的開盤 = 該主週期內第一根真實監控 K 棒的開盤（bars 要從那根主週期
    K 棒的開盤起涵蓋；呼叫端取數時已經往前多取）。
    """
    if not len(bars) or int(bars.t[-1]) < start_ms:
        return None, None
    t = bars.t
    hit = (t >= start_ms) & ((bars.l <= tp) | (bars.h >= sl))
    idx = np.flatnonzero(hit)
    if not len(idx):
        return None, bars.last_close()
    k = int(idx[0])
    hit_tp, hit_sl = bool(bars.l[k] <= tp), bool(bars.h[k] >= sl)
    bar_open = floor_to(t[k], spec.main_ms)
    same_main = np.flatnonzero((t >= bar_open) & (t <= t[k]) & bars.real)
    main_open = float(bars.o[same_main[0]]) if len(same_main) else float(bars.o[k])
    exit_ms = int(t[k]) + bars.step
    common = dict(judged_interval=spec.monitor_interval)
    if hit_tp and hit_sl:
        return _stop_loss_outcome(spec, sl, main_open, exit_ms, bar_open, same_bar_both=True,
                                  minor_resolved=spec.uses_minor, **common), exit_ms
    if hit_sl:
        return _stop_loss_outcome(spec, sl, main_open, exit_ms, bar_open, **common), exit_ms
    return Outcome(config.EXIT_TAKE_PROFIT, float(tp), exit_ms, bar_open, **common), exit_ms


def scan_main(spec, tp, sl, main_bars, minor_bars, start_ms):
    """降級路徑（dry run 的原始做法）：主週期 K 棒偵測；兩邊都碰時用小週期（只看真實的 K 棒）判先後，
    小週期同根兩碰 → 止損，找不到任何觸價的小週期 K 棒 → 止損（minor_missing）。回傳同 scan_monitor()。"""
    if not len(main_bars) or int(main_bars.t[-1]) < start_ms:
        return None, None
    t = main_bars.t
    hit = (t >= start_ms) & ((main_bars.l <= tp) | (main_bars.h >= sl))
    idx = np.flatnonzero(hit)
    if not len(idx):
        return None, main_bars.last_close()
    j = int(idx[0])
    bar_open, main_open = int(t[j]), float(main_bars.o[j])
    bar_close = bar_open + spec.main_ms
    hit_tp, hit_sl = bool(main_bars.l[j] <= tp), bool(main_bars.h[j] >= sl)
    common = dict(judged_interval=spec.main_interval)
    if hit_tp and hit_sl:
        if not spec.uses_minor:
            return _stop_loss_outcome(spec, sl, main_open, bar_close, bar_open, same_bar_both=True,
                                      **common), bar_close
        mt = minor_bars.t
        sub = np.flatnonzero((mt >= bar_open) & (mt < bar_close) & minor_bars.real
                             & ((minor_bars.l <= tp) | (minor_bars.h >= sl)))
        if not len(sub):
            return _stop_loss_outcome(spec, sl, main_open, bar_close, bar_open, same_bar_both=True,
                                      minor_missing=True, **common), bar_close
        k = int(sub[0])
        m_tp, m_sl = bool(minor_bars.l[k] <= tp), bool(minor_bars.h[k] >= sl)
        exit_ms = int(mt[k]) + minor_bars.step
        if m_tp and m_sl:
            return _stop_loss_outcome(spec, sl, main_open, exit_ms, bar_open, same_bar_both=True,
                                      minor_resolved=True, **common), exit_ms
        if m_sl:
            return _stop_loss_outcome(spec, sl, main_open, exit_ms, bar_open, minor_resolved=True,
                                      **common), exit_ms
        return Outcome(config.EXIT_TAKE_PROFIT, float(tp), exit_ms, bar_open, minor_resolved=True,
                       **common), exit_ms
    if hit_sl:
        return _stop_loss_outcome(spec, sl, main_open, bar_close, bar_open, **common), bar_close
    return Outcome(config.EXIT_TAKE_PROFIT, float(tp), bar_close, bar_open, **common), bar_close


# ============================== 取數 ==============================
class GateKlineFetcher:
    """經共用閘門取 K 棒：fetch(symbol, interval, end_close_ms, limit) → 原始 rows（新到舊）。

    endTime = end_close_ms − 1（派網實測 endTime 以 K 棒開盤時刻比較、含等號：回應是開盤 <= endTime 的最後
    limit 根，所以不會含還沒收完的那根）。symbol 放 params，由 requests 做 percent-encode。
    超過保留期（MARKET_INVALID_TIME）拋 KlinesUnavailable；其他錯誤（ApiError / RestBanned）原樣往外拋。
    """

    def __init__(self, gate, priority=rest_gate.PRIORITY_NORMAL):
        self.gate = gate
        self.priority = priority

    def __call__(self, symbol, interval, end_close_ms, limit):
        params = {"symbol": symbol, "interval": interval, "endTime": int(end_close_ms) - 1, "limit": int(limit)}
        try:
            js = self.gate.get(klines.KLINES_PATH, params, priority=self.priority)
        except ApiError as e:
            if "INVALID_TIME" in str(e).upper():
                raise KlinesUnavailable(str(e)) from e
            raise
        data = js.get("data") if isinstance(js, dict) else None
        rows = data.get("klines") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            raise ValueError("%s %s: 回應沒有 data.klines 清單" % (klines.KLINES_PATH, symbol))
        return rows


# ============================== 訊號與內部狀態 ==============================
@dataclass(frozen=True)
class RawSignalIn:
    """與策略無關的原始訊號（A3 的入口格式）。"""
    strategy: str
    symbol: str
    bar_open_ms: int
    bar_close_ms: int
    signal_price: float
    features: dict = field(hash=False, compare=False)


@dataclass
class _Tracked:
    """記憶體裡的一筆 open 名目部位（資料庫那一筆的影子 + 檢查進度）。"""
    signal_id: str
    strategy: str
    symbol: str
    spec: StrategySpec
    entry_price: float
    take_profit: float
    stop_loss: float
    opened_ms: int
    checked_until: int          # 監控 K 棒已判定到這個收盤時刻（下一根要判定的開盤）
    data_gap: bool = False
    restart_ms: object = None   # 從資料庫復原的部位：復原當下判定得到的最後收盤；出場時刻 <= 它 = 停機期間觸價
    lag_warned: bool = False    # 已經記過「拿不到新 K 棒、判定落後」的 WARNING（追上時清掉）


class _MissedRun:
    """一段連續的 missed_close（A1 的 degraded 根）。"""

    def __init__(self, close_ms):
        self.first_ms = self.last_ms = int(close_ms)
        self.count = 1


_STOP = object()


def _taipei(ms):
    return datetime.fromtimestamp(ms / 1000.0, TAIPEI).strftime("%m-%d %H:%M")


# ============================== 主體 ==============================
class NotionalTracker:
    """A3。所有外部相依都可注入（store_factory / fetcher / now_ms / bus），測試全程離線、用假時鐘。

    bus            live.bus.SignalBus（由執行入口建好、接好訂閱者）
    gate           live.rest_gate.RestGate（正式用 shared_gate()）；給了而沒給 fetcher / now_ms 時由它推導
    store_factory  無參數、回傳 live.store.Store 的函式；在工作執行緒裡呼叫（預設 store.open_store，
                   clock 用 now_ms）
    fetcher        fetch(symbol, interval, end_close_ms, limit) → rows（預設 GateKlineFetcher(gate)）
    now_ms         伺服器時間（UTC epoch ms）；預設 gate.server_clock.now_ms
    specs          {strategy: StrategySpec}；預設 build_specs()
    """

    def __init__(self, *, bus, gate=None, store_factory=None, fetcher=None, now_ms=None, specs=None,
                 user_id=None, finalize_wait_s=None, page_limit=None):
        if fetcher is None or now_ms is None:
            if gate is None:
                raise ValueError("沒給 fetcher / now_ms 時必須給 gate")
        self.bus = bus
        self.fetcher = fetcher or GateKlineFetcher(gate)
        self._now_fn = now_ms or gate.server_clock.now_ms
        self.specs = dict(specs) if specs is not None else build_specs()
        self.user_id = config.STRATEGY_USER_ID if user_id is None else user_id
        wait_s = config.BAR_FINALIZE_WAIT_SECONDS if finalize_wait_s is None else finalize_wait_s
        self.finalize_wait_ms = int(round(float(wait_s) * 1000))
        self.page_limit = int(config.A3_KLINES_PAGE_LIMIT if page_limit is None else page_limit)
        self.tick_ms = min(s.monitor_ms for s in self.specs.values())
        self._store_factory = store_factory
        self.store = None
        self._queue = queue.Queue()
        self._positions = {}        # (strategy, symbol) -> _Tracked
        self._cool = {}             # (strategy, symbol) -> 可再進場的最早 K 棒開盤
        self._deferred = []         # 無法確認持倉狀態、延後判定的 RawSignalIn
        self._missed = {}           # strategy -> _MissedRun（連續的 A1 missed_close，一段記一行 WARNING）
        self._recovered = False     # recover() 的載入步驟做完了沒；沒做完之前收到的訊號一律延後
        self._last_target = None    # 最後一次判定到的監控 K 棒收盤
        self._stop_evt = threading.Event()
        self._ready = threading.Event()
        self._ready_error = None
        self._thread = None
        self._publish_failures = {}
        self.stats = {"signals": 0, "entries": 0, "exits": 0, "skipped_holding": 0, "skipped_cooldown": 0,
                      "deferred": 0, "fetches": 0, "fetch_failures": 0, "publish_failures": 0,
                      "recovered_exits": 0, "store_failures": 0}

    # ---------------- 別的執行緒呼叫的入口（只排佇列） ----------------
    def bar_result_handler(self, strategy):
        """給 A1 的 SignalFeed(on_result=...)：把 BarResult 的原始訊號與 missed_close 摘要排進佇列，立刻返回。"""
        if strategy not in self.specs:
            raise ValueError("未知的策略 %r（A3 的規格：%s）" % (strategy, sorted(self.specs)))

        def on_result(res):
            if _A1_MISSED_CLOSE in (res.degraded_reasons or ()):
                self._queue.put(("missed", strategy, int(res.bar_close_ms)))
            else:
                self._queue.put(("bar_ok", strategy, int(res.bar_close_ms)))
            for s in res.signals:
                self._queue.put(("signal", RawSignalIn(
                    strategy=strategy, symbol=s.symbol, bar_open_ms=int(s.bar_open_ms),
                    bar_close_ms=int(s.bar_close_ms), signal_price=float(s.signal_price),
                    features={k: getattr(s, k) for k in s4_signal.FEATURE_COLS})))
        return on_result

    def submit_signal(self, *, strategy, symbol, bar_open_ms, bar_close_ms, signal_price, features):
        """與策略無關的原始訊號入口（策略五資料層日後接這裡）。任何執行緒都可以呼叫；只排佇列就返回。

        features：判定特徵（dict，值是純量；進場事件原樣帶上）。型別錯誤當場拋（呼叫端的程式錯誤）。
        """
        sig = RawSignalIn(strategy=strategy, symbol=symbol, bar_open_ms=int(bar_open_ms),
                          bar_close_ms=int(bar_close_ms), signal_price=float(signal_price),
                          features=dict(features))
        self._queue.put(("signal", sig))

    # ---------------- 執行緒 ----------------
    def start(self):
        """開工作執行緒：開資料庫 → 復原 → 迴圈。wait_ready() 可以等到資料庫開好（或失敗）。"""
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._thread_main, name="a3-tracker", daemon=True)
        self._thread.start()

    def wait_ready(self, timeout=None):
        """等工作執行緒開好資料庫。回傳 True = 可以用；False = 逾時或失敗（ready_error 有原因）。"""
        self._ready.wait(timeout)
        return self._ready.is_set() and self._ready_error is None

    @property
    def ready_error(self):
        return self._ready_error

    def stop(self):
        self._stop_evt.set()
        self._queue.put(_STOP)

    def join(self, timeout=None):
        if self._thread is not None:
            self._thread.join(timeout)
            return not self._thread.is_alive()
        return True

    def _thread_main(self):
        try:
            self.open()
        except Exception as e:  # noqa: BLE001 —— 開不了資料庫就不能追蹤，讓執行入口知道並結束
            self._ready_error = e
            logger.exception("A3 開啟資料庫失敗，名目部位追蹤沒有啟動")
            self._ready.set()
            return
        self._ready.set()
        try:
            self._safe(self.recover)
            self._loop()
        finally:
            self.close()

    def _loop(self):
        while not self._stop_evt.is_set():
            now = self._now()
            target = self._tick_target(now)
            if self._last_target is None or target > self._last_target:
                # 不論這次 tick 成敗，下一次都是下一根：tick 一開始就失敗（例如資料庫開不了）時不可以原地空轉
                self._last_target = target
                self._safe(self.tick, target)
                continue
            next_at = target + self.tick_ms + self.finalize_wait_ms
            job = self._wait_job(max(0.0, (next_at - now) / 1000.0))
            if job is _STOP:
                break
            if job is not None:
                self._safe(self._handle_job, job)
        self._drain()

    def _drain(self):
        """停止時把佇列裡 _STOP 之前的工作做完再離開（例如 --duration 到了、A1 最後一根的訊號還排在佇列裡）。
        做完之後還在延後判定的訊號只存在記憶體裡，停機就沒了：逐筆記 WARNING，不安靜丟掉。"""
        while True:
            try:
                job = self._queue.get_nowait()
            except queue.Empty:
                break
            if job is _STOP:
                break
            self._safe(self._handle_job, job)
        self._flush_missed()
        for sig in self._deferred:
            logger.warning("A3 停止時訊號 %s %s @ K 棒 %s 仍在延後判定，沒有處理（沒有寫庫、沒有發事件）",
                           sig.strategy, sig.symbol, _taipei(sig.bar_close_ms))

    def _wait_job(self, timeout):
        """等佇列最多 timeout 秒；逾時回 None。測試可以換掉它（用假時鐘推進）。"""
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def _safe(self, fn, *args):
        """工作執行緒裡的一件事出錯不可以讓整個追蹤停掉：記 ERROR（含 traceback）後繼續。
        資料庫操作失敗 → 記憶體狀態作廢（_invalidate），下一次 tick 從資料庫重建。
        只攔 Exception：KeyboardInterrupt / SystemExit 照樣往外傳。
        （手上的訊號不靠這裡保住：_decide_or_defer() 自己會把它放回延後清單。）"""
        try:
            return fn(*args)
        except _STORE_ERRORS as e:
            self._invalidate(e, getattr(fn, "__name__", fn))
        except Exception:  # noqa: BLE001
            logger.exception("A3 處理 %s 時發生未預期的例外，繼續運作", getattr(fn, "__name__", fn))
        return None

    def _invalidate(self, err, what):
        """資料庫操作失敗（寫入或讀取、中毒與否、哪一個寫入點都一樣）：記憶體裡的部位 / 冷卻 / 檢查點不再可信 ——
        可能跟資料庫不一致（例如平倉沒寫進去、或本來就不同步）。一律作廢：下一次 tick 先重開（中毒或開不了時）
        再 recover() 從資料庫重建；已判定但沒寫進去的出場會從進場後第一根重新判定出來（結果相同，出場時刻是
        歷史時刻）。作廢期間收到的訊號一律延後，重建後依時間順序重試。"""
        self._recovered = False
        self.stats["store_failures"] += 1
        poisoned = self.store is not None and self.store.poisoned
        logger.error("A3 %s 時資料庫操作失敗（%s: %s%s）：記憶體狀態作廢，下一次 tick 從資料庫重建；手上的訊號延後重試",
                     what, type(err).__name__, err, "；連線已中毒，下一次 tick 重新開啟" if poisoned else "",
                     exc_info=(type(err), err, err.__traceback__))

    def _ensure_store(self):
        """資料庫連線不在（之前開不了）或已中毒：關掉重開。重開之後記憶體一定要從資料庫重建。"""
        if self.store is not None and not self.store.poisoned:
            return
        if self.store is not None:
            try:
                self.store.close()
            except Exception:  # noqa: BLE001 —— 中毒的連線早就關了；關不掉也不影響重開
                pass
            self.store = None
        self._recovered = False
        self.open()

    # ---------------- 工作執行緒裡的操作（測試直接呼叫） ----------------
    def open(self):
        """在目前的執行緒開資料庫（Store 只能在這條執行緒用）。"""
        if self._store_factory is None:
            self.store = store_mod.open_store(clock=self._now)
        else:
            self.store = self._store_factory()
        return self.store

    def close(self):
        if self.store is not None:
            try:
                self.store.close()
            finally:
                self.store = None

    def process_pending(self):
        """把佇列裡目前有的工作全部做完（不等待）。回傳做了幾件。測試與迴圈外的呼叫用。"""
        n = 0
        while True:
            try:
                job = self._queue.get_nowait()
            except queue.Empty:
                return n
            if job is _STOP:
                self._stop_evt.set()
                return n
            self._safe(self._handle_job, job)
            n += 1

    def recover(self):
        """重啟復原（FR-5），見模組 docstring。"""
        self._positions.clear()
        self._cool.clear()
        target = self._tick_target(self._now())
        for row in self.store.open_positions(user_id=self.user_id):
            spec = self.specs.get(row["strategy"])
            if spec is None:
                logger.error("A3 復原：open 部位 %s 的策略 %r 不在目前的規格裡，無法監控（保持 open）",
                             row["signal_id"], row["strategy"])
                continue
            pos = self._tracked_from_row(row, spec)
            pos.restart_ms = target
            self._positions[(row["strategy"], row["symbol"])] = pos
        logger.info("A3 復原：載入 open 部位 %d 筆 %s", len(self._positions),
                    [p.signal_id for p in self._positions.values()])
        closed = 0
        for key in sorted(self._positions):
            pos = self._positions.get(key)
            if pos is None:
                continue
            try:
                outcome = self._advance(pos, floor_to(target, pos.spec.monitor_ms))
            except _FetchFailed as e:
                logger.warning("A3 復原：%s 重新判定時取數失敗（%s），下一次 tick 繼續", pos.signal_id, e)
                continue
            except Exception:  # noqa: BLE001 —— 一個部位出錯不可以擋住其他部位的復原；它留到下一次 tick 繼續判
                logger.exception("A3 復原：%s 重新判定時發生未預期的例外，下一次 tick 繼續", pos.signal_id)
                continue
            if outcome is not None:
                self._close(pos, outcome, publish=False)      # 寫庫失敗就往外拋：這次復原不算數，下一次 tick 重來
                closed += 1
        self._rebuild_cooldowns()
        self._recovered = True
        self._last_target = target
        unpub_e = self.store.unpublished_signals(user_id=self.user_id)
        unpub_x = self.store.unpublished_exits(user_id=self.user_id)
        logger.info("A3 復原：停機期間觸價平倉 %d 筆；仍 open %d 筆；冷卻 %d 組；待補發進場 %d、出場 %d",
                    closed, len(self._positions), len(self._cool), len(unpub_e), len(unpub_x))
        legacy = [r["signal_id"] for r in unpub_x if r.get("exit_features") is None]
        if legacy:
            logger.warning("A3 復原：%d 筆已平倉部位沒有出場稽核旗標（schema v1 時代留下、當時沒有記錄出場是否發布過），"
                           "一律當成未發布、補發出場事件（寧可重送、不可漏發）：%s", len(legacy), legacy)
        self._flush()

    def tick(self, target_close_ms):
        """一根監控 K 棒收盤（target_close_ms）之後：檢查所有 open 部位、重試延後的訊號、補發未發布的事件。

        資料庫失敗在這裡吸收（記憶體作廢，下一次 tick 重建），不往外拋；每個部位各自隔離，一個部位出錯
        不會擋住其他部位、延後訊號的重試與補發。"""
        self._last_target = max(self._last_target or 0, int(target_close_ms))
        try:
            self._ensure_store()        # 之前開不了、或已中毒：每次 tick 再試一次
            if not self._recovered:
                self.recover()
        except _STORE_ERRORS as e:
            self._invalidate(e, "tick 重建記憶體狀態")
            return
        self._flush_missed()
        for key in sorted(self._positions):
            if not self._recovered:
                break                   # 前一個部位寫庫失敗、記憶體已作廢：剩下的等下一次 tick 從資料庫重建後再判
            pos = self._positions.get(key)
            if pos is None:
                continue
            target = floor_to(target_close_ms, pos.spec.monitor_ms)
            try:
                outcome = self._advance(pos, target)
                if outcome is not None:
                    self._close(pos, outcome, publish=False)
                else:
                    self._check_lag(pos, target)
            except _FetchFailed as e:
                logger.warning("A3 取 %s %s K 棒失敗（%s），%s 下一次 tick 再檢查", pos.symbol,
                               pos.spec.monitor_interval, e, pos.signal_id)
            except _STORE_ERRORS as e:
                self._invalidate(e, "%s 平倉寫庫" % pos.signal_id)
            except Exception:  # noqa: BLE001 —— 一個部位（例如某幣回應格式異常）不可以讓其他部位每分鐘都判不到
                logger.exception("A3 判定 %s 時發生未預期的例外，這個部位下一次 tick 再判，其他部位照常",
                                 pos.signal_id)
        if self._recovered:
            self._retry_deferred()
        if self._recovered:
            try:
                self._flush()
            except _STORE_ERRORS as e:
                self._invalidate(e, "補發事件")

    # ---------------- 工作 ----------------
    def _handle_job(self, job):
        kind = job[0]
        if kind == "signal":
            self.stats["signals"] += 1
            self._handle_signal(job[1])
        elif kind == "missed":
            _, strategy, close_ms = job
            m = self._missed.get(strategy)
            if m is None:
                self._missed[strategy] = _MissedRun(close_ms)
            else:
                m.last_ms = close_ms
                m.count += 1
        elif kind == "bar_ok":
            self._flush_missed(job[1])

    def _flush_missed(self, strategy=None):
        """把累積的連續 missed_close 記成一行 WARNING（名稱與根數）。停頓一整晚會有上百根，不逐根記。"""
        for strat in ([strategy] if strategy is not None else list(self._missed)):
            m = self._missed.pop(strat, None)
            if m is not None:
                logger.warning("A1（%s）missed_close（degraded）%d 根：K 棒 %s～%s 收盤時沒有判定。A3 不把這些根當成"
                               "「沒有訊號」，名目部位與冷卻狀態不受影響；告警由 A1 / A4 處理",
                               strat, m.count, _taipei(m.first_ms), _taipei(m.last_ms))

    def _handle_signal(self, sig):
        spec = self.specs.get(sig.strategy)
        if spec is None:
            logger.error("A3 收到未知策略 %r 的訊號（%s），丟棄", sig.strategy, sig.symbol)
            return
        if sig.bar_close_ms - sig.bar_open_ms != spec.main_ms:
            logger.error("A3 收到 %s %s 的訊號，K 棒 %s～%s 不是主週期 %s，丟棄", sig.strategy, sig.symbol,
                         sig.bar_open_ms, sig.bar_close_ms, spec.main_interval)
            return
        if not self._recovered:
            self._defer(sig, "重啟復原還沒完成")
            return
        key = (sig.strategy, sig.symbol)
        if any((d.strategy, d.symbol) == key for d in self._deferred):
            self._defer(sig, "同策略同幣有更早的訊號還在延後判定")
            return
        self._decide_or_defer(spec, sig)

    def _decide_or_defer(self, spec, sig):
        """_decide()，出任何錯都把訊號放回延後清單：還沒寫進資料庫的訊號不可以因為一次失敗就不見。
        資料庫錯誤另外讓記憶體作廢（_invalidate）。回傳 True = 有結論；False = 延後了。"""
        try:
            return self._decide(spec, sig)
        except _STORE_ERRORS as e:
            self._invalidate(e, "處理訊號 %s %s @ K 棒 %s" % (sig.strategy, sig.symbol, _taipei(sig.bar_close_ms)))
            self._defer(sig, "資料庫操作失敗，等下一次 tick 從資料庫重建後重試")
        except Exception:  # noqa: BLE001
            logger.exception("A3 處理訊號 %s %s @ K 棒 %s 時發生未預期的例外", sig.strategy, sig.symbol,
                             _taipei(sig.bar_close_ms))
            self._defer(sig, "處理時發生未預期的例外，下一次 tick 重試")
        return False

    def _decide(self, spec, sig):
        """回傳 True = 已經有結論（進場或略過）；False = 仍無法判定（延後）。資料庫錯誤往外拋（呼叫端處理）。"""
        key = (sig.strategy, sig.symbol)
        signal_id = make_signal_id(spec, sig.symbol, sig.bar_close_ms)
        if self.store.get_signal(signal_id) is not None:
            # 已經寫進資料庫：之前處理到一半出錯（例如寫庫成功、之後的發布標記失敗）後的重試、或重複交付。
            # 資料庫說了算，不再進場；持倉由 recover() 從資料庫讀回、事件由 _flush() 補發
            logger.info("A3 訊號 %s 已經寫進資料庫（重試或重複交付），不重複進場", signal_id)
            return True
        pos = self._positions.get(key)
        if pos is not None:
            # dry run 在訊號那根先做出場檢查再做進場檢查：部位在訊號 K 棒之前出場的話要套冷卻，還沒出場就是持倉中。
            # 所以先把這筆部位判定到訊號 K 棒開盤（= 前一根主週期收盤）為止；訊號那根自己的觸價不影響結論
            # （在那根出場 → 冷卻 >= 1 根擋掉；沒出場 → 持倉中擋掉）。
            if pos.checked_until < sig.bar_open_ms:
                target = floor_to(min(sig.bar_close_ms, self._tick_target(self._now())), spec.monitor_ms)
                try:
                    outcome = self._advance(pos, target)
                except _FetchFailed as e:
                    self._defer(sig, "持倉 %s 取數失敗（%s），無法確認它在訊號之前有沒有出場" % (pos.signal_id, e))
                    return False
                if outcome is not None:
                    self._close(pos, outcome, publish=True)
                    pos = None
            if pos is not None:
                if pos.checked_until < sig.bar_open_ms:
                    self._defer(sig, "持倉 %s 的 K 棒還沒拿到訊號 K 棒之前（檢查到 %s）"
                                % (pos.signal_id, _taipei(pos.checked_until)))
                    return False
                self.stats["skipped_holding"] += 1
                logger.info("A3 略過訊號 %s %s @ K 棒 %s（訊號價 %s）：同策略同幣持倉中（%s）", sig.strategy,
                            sig.symbol, _taipei(sig.bar_close_ms), sig.signal_price, pos.signal_id)
                return True
        cool = self._cool.get(key)
        if cool is not None and sig.bar_open_ms < cool:
            self.stats["skipped_cooldown"] += 1
            logger.info("A3 略過訊號 %s %s @ K 棒 %s（訊號價 %s）：冷卻中，K 棒開盤要 >= %s 才可再進場",
                        sig.strategy, sig.symbol, _taipei(sig.bar_close_ms), sig.signal_price, _taipei(cool))
            return True
        self._enter(spec, sig, signal_id)
        return True

    def _defer(self, sig, why):
        self._deferred.append(sig)
        self.stats["deferred"] += 1
        logger.warning("A3 訊號 %s %s @ K 棒 %s 延後判定：%s（下一次 tick 再試，不臆測）", sig.strategy, sig.symbol,
                       _taipei(sig.bar_close_ms), why)

    def _retry_deferred(self):
        if not self._deferred:
            return
        pending, self._deferred = sorted(self._deferred, key=lambda s: (s.bar_close_ms, s.strategy, s.symbol)), []
        blocked = set()
        for sig in pending:
            key = (sig.strategy, sig.symbol)
            if key in blocked:
                self._deferred.append(sig)
                continue
            if not self._recovered:
                self._deferred.append(sig)      # 前一筆寫庫失敗、記憶體已作廢：等下一次 tick 重建後再試
                continue
            if not self._decide_or_defer(self.specs[sig.strategy], sig):
                # 已經放回 _deferred（仍無法判定、或出錯）；同一組的後續訊號也要等
                blocked.add(key)
        # 放回去的時候保持時間順序
        self._deferred.sort(key=lambda s: (s.bar_close_ms, s.strategy, s.symbol))

    def _enter(self, spec, sig, signal_id):
        tp, sl = levels(spec, sig.signal_price)
        created = self._now()
        try:
            event = EntryEvent(strategy=sig.strategy, signal_id=signal_id, symbol=sig.symbol,
                               direction=config.DIRECTION_SHORT, created_ms=created, bar_open_ms=sig.bar_open_ms,
                               signal_price=sig.signal_price, take_profit_price=tp, stop_loss_price=sl,
                               features=sig.features)
        except (TypeError, ValueError) as e:
            logger.error("A3 訊號 %s 建不出進場事件（%s），不寫庫、不發布", signal_id, e)
            return
        self.store.record_entry(signal_id=signal_id, user_id=self.user_id, strategy=sig.strategy,
                                symbol=sig.symbol, side=config.DIRECTION_SHORT, bar_open_ms=sig.bar_open_ms,
                                signal_price=event.signal_price, take_profit_price=event.take_profit_price,
                                stop_loss_price=event.stop_loss_price, features=event.features,
                                created_ms=created, opened_ms=sig.bar_close_ms)
        self._positions[(sig.strategy, sig.symbol)] = _Tracked(
            signal_id=signal_id, strategy=sig.strategy, symbol=sig.symbol, spec=spec,
            entry_price=event.signal_price, take_profit=event.take_profit_price, stop_loss=event.stop_loss_price,
            opened_ms=sig.bar_close_ms, checked_until=sig.bar_close_ms)
        self.stats["entries"] += 1
        logger.info("A3 進場 %s：%s %s 訊號價 %s 止盈 %s 止損 %s（K 棒 %s 收盤）", signal_id, sig.strategy,
                    sig.symbol, event.signal_price, event.take_profit_price, event.stop_loss_price,
                    _taipei(sig.bar_close_ms))
        self._flush()

    def _close(self, pos, outcome, *, publish):
        # 停機期間觸價（出場時刻 <= 復原當下的判定點）的出場是延遲發布的，事件註明 recovered
        recovered = pos.restart_ms is not None and outcome.exit_ms <= pos.restart_ms
        feats = {
            "gap_open": outcome.gap_open, "same_bar_both": outcome.same_bar_both,
            "minor_resolved": outcome.minor_resolved, "minor_missing": outcome.minor_missing,
            "judged_interval": outcome.judged_interval, "exit_bar_open_ms": outcome.exit_bar_open_ms,
            "recovered": bool(recovered), "data_gap": bool(pos.data_gap), "judged_ms": self._now(),
        }
        row = self.store.close_position(pos.signal_id, exit_reason=outcome.reason, exit_price=outcome.exit_price,
                                        closed_ms=outcome.exit_ms, exit_features=feats)
        self._positions.pop((pos.strategy, pos.symbol), None)
        self._cool[(pos.strategy, pos.symbol)] = cooldown_until(pos.spec, row["closed_ms"])
        self.stats["exits"] += 1
        if recovered:
            self.stats["recovered_exits"] += 1
        logger.info("A3 出場 %s：%s @ %s（觸價 K 棒 %s 收盤、所屬主週期 %s%s%s%s）", pos.signal_id, outcome.reason,
                    outcome.exit_price, _taipei(outcome.exit_ms), _taipei(outcome.exit_bar_open_ms),
                    "、開盤跳空" if outcome.gap_open else "", "、同根兩碰保守計止損" if outcome.same_bar_both else "",
                    "、重啟補判（延遲發布）" if recovered else "")
        if publish:
            self._flush()

    def _check_lag(self, pos, target):
        """取數成功卻一直拿不到新 K 棒（例如下架、長時間沒資料）時不可以安靜：落後超過一根主週期記一次
        WARNING，追上時記 INFO。取數失敗另外每次記 WARNING（tick() 裡）。"""
        behind = target - pos.checked_until
        if behind > pos.spec.main_ms and not pos.lag_warned:
            pos.lag_warned = True
            logger.warning("A3 %s（%s）的 %s K 棒只判定到 %s，落後目標 %s；派網還沒有新的 K 棒，繼續等，不臆測",
                           pos.signal_id, pos.symbol, pos.spec.monitor_interval, _taipei(pos.checked_until),
                           _taipei(target))
        elif behind <= pos.spec.main_ms and pos.lag_warned:
            pos.lag_warned = False
            logger.info("A3 %s 的 K 棒追上了（判定到 %s）", pos.signal_id, _taipei(pos.checked_until))

    def _rebuild_cooldowns(self):
        self._cool.clear()
        for row in self.store.last_closed_positions(user_id=self.user_id):
            spec = self.specs.get(row["strategy"])
            if spec is not None:
                self._cool[(row["strategy"], row["symbol"])] = cooldown_until(spec, row["closed_ms"])

    # ---------------- 發布 ----------------
    def _flush(self):
        """補發所有未發布的事件：先進場（依寫入時刻）、再「進場已發布」的出場（依平倉時刻）。"""
        for row in self.store.unpublished_signals(user_id=self.user_id):
            event = entry_event_from_row(row)
            if self._publish(event):
                self.store.mark_published(row["signal_id"], published_ms=self._now())
        for row in self.store.unpublished_exits(user_id=self.user_id):
            if row["entry_published_ms"] is None:
                continue                  # 進場還沒送達：出場不可以超車，等下一次
            event = exit_event_from_row(row)
            if self._publish(event):
                self.store.mark_exit_published(row["signal_id"], published_ms=self._now())

    def _publish(self, event):
        report = self.bus.publish(event)
        key = (type(event).__name__, event.signal_id)
        if report.ok:
            if self._publish_failures.pop(key, None):
                logger.info("A3 %s %s 重送後全部送達", key[0], key[1])
            return True
        n = self._publish_failures.get(key, 0) + 1
        self._publish_failures[key] = n
        self.stats["publish_failures"] += 1
        logger.warning("A3 %s %s 發布沒有全部送達（第 %d 次；送達 %s、失敗 %s），不標記已發布，下一次 tick 重送"
                       "（訂閱者以 (signal_id, 事件種類) 去重）", key[0], key[1], n, list(report.delivered),
                       list(report.failed))
        return False

    # ---------------- 判定與取數 ----------------
    def _advance(self, pos, target_close):
        """把 pos 從 checked_until 判定到 target_close（監控 K 棒收盤時刻）。回傳 Outcome 或 None（還沒出場）。

        取數失敗拋 _FetchFailed（checked_until 不動，下一次再從同一處開始）。目標 K 棒還沒有時判定到有的地方為止。
        """
        spec = pos.spec
        ratio = spec.main_ms // spec.monitor_ms
        span_ms = max(1, self.page_limit // ratio) * spec.main_ms
        while pos.checked_until < target_close:
            start = pos.checked_until
            win_from = floor_to(start, spec.main_ms)
            win_to = min(int(target_close), win_from + span_ms)
            try:
                rows = self._fetch(pos.symbol, spec.monitor_interval, win_to, (win_to - win_from) // spec.monitor_ms)
            except KlinesUnavailable:
                rows = None
            if rows is None:
                # 整段監控週期都超過保留期：這段改走降級路徑
                outcome = self._fallback(pos, win_from, win_to, Bars.empty(spec.monitor_ms))
                if outcome is not None or pos.checked_until <= start:
                    return outcome
                continue
            bars = Bars.from_rows(rows, spec.monitor_ms, win_to)
            first = bars.first_real_time()
            if first is None:
                return None                     # 還沒有任何資料（剛收盤）：下一次再看
            if first > start:
                # 開頭一段拿不到（保留期邊緣）：那幾根主週期 K 棒改走降級路徑（可用的小週期 K 棒照樣拿來判先後）
                boundary = min(win_to, floor_to(first + spec.main_ms - 1, spec.main_ms))
                outcome = self._fallback(pos, win_from, boundary, bars)
                if outcome is not None or pos.checked_until <= start:
                    return outcome
                continue
            outcome, checked = scan_monitor(spec, pos.take_profit, pos.stop_loss, bars, start)
            if outcome is not None:
                # 檢查點不推過觸價那根：平倉寫庫失敗時，下一次一定還判得到同一根（記憶體另外也會整個作廢重建）
                return outcome
            if checked is None or checked <= start:
                return None
            pos.checked_until = checked
            if checked < win_to:
                return None                     # 目標 K 棒還沒到：判定到有的地方為止
        return None

    def _fallback(self, pos, f, b, minor_bars):
        """[f, b) 這段監控週期拿不到：用主週期 K 棒判定（dry run 的原始做法）。回傳 Outcome 或 None。
        只處理「已經完整」的主週期 K 棒（b 往下對齊主週期）：b 落在某根主週期中間時，那一根留給下一次，
        不可以連同它裡面已經有的監控 K 棒一起跳過。沒出場時 checked_until 推到對齊後的 b（主週期也拿不到的
        段落記 ERROR、data_gap）。"""
        spec = pos.spec
        if not spec.uses_minor:
            self._data_gap(pos, f, b, "監控週期就是主週期（%s），沒有更粗的 K 棒可以補判" % spec.main_interval)
            return None
        cursor = floor_to(f, spec.main_ms)
        b_main = floor_to(b, spec.main_ms)
        if b_main <= cursor:
            return None                     # 還沒有任何一根完整的主週期 K 棒可以補判：下一次再看
        logger.warning("A3 %s：%s～%s 這段 %s K 棒拿不到（超過派網保留期、或這段開頭沒有資料），改用 %s K 棒補判"
                       "（dry run 的原始做法）", pos.signal_id, _taipei(cursor), _taipei(b_main),
                       spec.monitor_interval, spec.main_interval)
        span_ms = self.page_limit * spec.main_ms
        while cursor < b_main:
            page_to = min(b_main, cursor + span_ms)
            try:
                rows = self._fetch(pos.symbol, spec.main_interval, page_to, (page_to - cursor) // spec.main_ms)
            except KlinesUnavailable:
                self._data_gap(pos, cursor, page_to, "%s K 棒也超過保留期" % spec.main_interval)
                cursor = page_to
                continue
            main_bars = Bars.from_rows(rows, spec.main_ms, page_to)
            first = main_bars.first_real_time()
            if first is None:
                self._data_gap(pos, cursor, page_to, "%s K 棒沒有資料" % spec.main_interval)
                cursor = page_to
                continue
            if first > max(cursor, floor_to(pos.checked_until, spec.main_ms)):
                self._data_gap(pos, cursor, first, "%s K 棒開頭一段沒有資料" % spec.main_interval)
            # 主週期 K 棒從檢查點所屬的那根起判（檢查點在主週期中間時，那一整根照 dry run 的做法看）
            outcome, checked = scan_main(spec, pos.take_profit, pos.stop_loss, main_bars, minor_bars,
                                         floor_to(pos.checked_until, spec.main_ms))
            if outcome is not None:
                return outcome              # 同 _advance：寫庫成功之前不推進檢查點
            if checked is not None:
                pos.checked_until = max(pos.checked_until, checked)
            cursor = page_to
        pos.checked_until = max(pos.checked_until, b_main)
        return None

    def _data_gap(self, pos, f, b, why):
        pos.data_gap = True
        pos.checked_until = max(pos.checked_until, b)
        logger.error("A3 %s：%s～%s 拿不到任何 K 棒（%s），這段無法判定；部位保持 open，從之後繼續監控"
                     "（之後的出場事件會標 data_gap）", pos.signal_id, _taipei(f), _taipei(b), why)

    def _fetch(self, symbol, interval, end_close_ms, limit):
        """取 [end_close_ms − limit 根, end_close_ms) 的 K 棒。KlinesUnavailable 原樣往外拋；其他失敗包成 _FetchFailed。"""
        self.stats["fetches"] += 1
        try:
            return self.fetcher(symbol, interval, int(end_close_ms), int(limit))
        except KlinesUnavailable:
            raise
        except rest_gate.RestBanned as e:
            self.stats["fetch_failures"] += 1
            raise _FetchFailed("REST 封鎖冷卻中，還剩 %.0f 秒" % e.remaining_seconds) from e
        except Exception as e:  # noqa: BLE001 —— ApiError、格式錯誤、連線錯誤都一樣：下一次再試
            self.stats["fetch_failures"] += 1
            raise _FetchFailed("%s: %s" % (type(e).__name__, e)) from e

    # ---------------- 小工具 ----------------
    def _now(self):
        return int(self._now_fn())

    def _tick_target(self, now):
        """現在（伺服器時間）可以判定到哪一根監控 K 棒的收盤：收盤 + 定稿等待已過的最後一根。"""
        return floor_to(int(now) - self.finalize_wait_ms, self.tick_ms)

    def _tracked_from_row(self, row, spec):
        return _Tracked(signal_id=row["signal_id"], strategy=row["strategy"], symbol=row["symbol"], spec=spec,
                        entry_price=row["entry_price"], take_profit=row["take_profit_price"],
                        stop_loss=row["stop_loss_price"], opened_ms=row["opened_ms"],
                        checked_until=row["opened_ms"])

    def open_positions(self):
        """記憶體裡的 open 部位（給測試與診斷）：{(strategy, symbol): signal_id}。"""
        return {k: p.signal_id for k, p in self._positions.items()}

    def cooldowns(self):
        """{(strategy, symbol): 可再進場的最早 K 棒開盤}（給測試與診斷）。"""
        return dict(self._cool)

    def deferred(self):
        return list(self._deferred)


# ============================== 事件重建 ==============================
def entry_event_from_row(row):
    """store 的訊號列 → EntryEvent（重送時用；與第一次發布的內容相同）。欄位名 side → direction 由這裡對應。"""
    return EntryEvent(strategy=row["strategy"], signal_id=row["signal_id"], symbol=row["symbol"],
                      direction=row["side"], created_ms=row["created_ms"], bar_open_ms=row["bar_open_ms"],
                      signal_price=row["signal_price"], take_profit_price=row["take_profit_price"],
                      stop_loss_price=row["stop_loss_price"], features=row["features"])


def exit_event_from_row(row):
    """store.unpublished_exits() 的列 → ExitEvent。exit_reason → reason、side → direction 由這裡對應；
    created_ms = 稽核旗標裡的 judged_ms（沒有就用平倉時刻），重送時內容不變。"""
    feats = dict(row.get("exit_features") or {})
    created = feats.get("judged_ms") or row["closed_ms"]
    return ExitEvent(strategy=row["strategy"], signal_id=row["signal_id"], symbol=row["symbol"],
                     direction=row["side"], created_ms=int(created), reason=row["exit_reason"],
                     exit_price=row["exit_price"], entry_price=row["entry_price"], opened_ms=row["opened_ms"],
                     closed_ms=row["closed_ms"], features=feats)
