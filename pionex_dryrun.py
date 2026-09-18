#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
派網策略 Dry Run（前瞻模擬，不下單）
===================================
同時追蹤 watch（策略1已退役，續作ATR區間監控）、策略2（大戶提款）與策略4（爆量竭盡，5分K）
每次執行：抓最新的 1h K棒 → 依序處理「上次執行之後新收完的每一根K棒」
         → 先檢查持倉是否止盈/止損，再檢查新訊號 → 寫入狀態檔、Excel、SUMMARY.md

・進出場規則與 pionex_backtest.py 完全相同（直接呼叫它的函式），結果可直接和回測比較。
・START_FROM 可指定回補起算時間（受 API 上限約 20 天）；設 None 則從第一次執行當下開始。
・中間漏跑幾次也沒關係：下次執行會把漏掉的K棒補處理（K棒上限 500 根 ≈ 20 天）。
・測試途中要改策略參數直接改即可：`book_fingerprint()` 會偵測到參數指紋變動，自動把
  該本帳清空重跑並記錄 forward_from，SUMMARY 會把「回填（回測）」與「前進測試」分開統計，
  不會把新舊參數的紀錄混在一起。**不需要、也不建議**手動刪除 state/ 重來——那樣做會把
  全部帳本的長期紀錄與 baseline（同期基準）樣本一次歸零，比讓版本控管機制自動處理更糟。
  `START_FROM` 這個模組常數不列入指紋，改它不會觸發任何帳本重置（見 book_fingerprint()）。

執行：python pionex_dryrun.py
"""
import hashlib
import json
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import numpy as np
import pandas as pd
from openpyxl.styles import Font

import pionex_backtest as pb
import pionex_reversal as rv
import pionex_strategy4 as s4

# ============================== 起算時間 ==============================
# 第一次執行時，從這個時間開始回補統計（台北時間）。設 None = 只從執行當下開始。
# 派網 1h K棒單次最多取 500 根（約 20.8 天），太舊的抓不到，請盡早開始跑。
START_FROM = "2026-09-01 00:00"

# ============================== 策略設定（固定，勿在測試途中修改） ==============================
BASE_CONFIG = dict(
    MARKET_TYPE="PERP", INTERVAL="60M",
    TAKE_PROFIT=0.03, STOP_LOSS=0.05, FEE_RATE=0.0005, EXIT_MODE="fixed",
    LIQ_MIN_USD=50_000, LIQ_MODE="avg_hourly",
    MOM_MODE="fixed", MOM_THRESHOLD=0.02,
    ATR_PERIOD=14, ATR_MIN_PCT=0.04, ATR_MAX_PCT=0.05,
    MA_PERIOD=20, OBV_LOOKBACK=20, HTF_MA_PERIOD=100, MAX_PRICE=None,
    MAX_MA_DEV=0.06, MAX_MOM=0.10, COOLDOWN_BARS=1, MIN_VOL_RATIO=0.35,
    EXCLUDE_FLAT_24H=None, BTC_MAX_ALIGNED_DEV=None, REQUIRE_HTF_TREND=False,
    BTC_TREND_FILTER=False, MIN_CLOSE_POS=None, MAX_HOLD_HOURS=None,
    RESOLVE_SAME_BAR_WITH_5M=True, RESOLVE_INTERVALS=["5M", "15M", "30M"],
    # 資金模擬頁（Excel 每次重新產生，要改請改這裡）
    EQUITY_INITIAL=100, EQUITY_ORDER_PCT=0.02, EQUITY_STEP=50,
    EQUITY_LEVERAGE=50, EQUITY_SIZING_MODE=2, EQUITY_STOP_BELOW=0,
    REQUEST_SLEEP=0.1, CA_BUNDLE=None,
)

# ---- 策略2：大戶提款（只做空低價幣）----
S2_CONFIG = dict(
    MARKET_TYPE="PERP", INTERVAL="60M",
    TAKE_PROFIT=0.03, STOP_LOSS=0.05, FEE_RATE=0.0005, EXIT_MODE="fixed",
    TP_ATR_MULT=1.2, SL_ATR_MULT=0.8,
    LIQ_MIN_USD=50_000, LIQ_MODE="avg_hourly",
    ATR_PERIOD=14, ATR_MIN_PCT=0.02, ATR_MAX_PCT=0.06,
    MA_PERIOD=20, OBV_LOOKBACK=20, HTF_MA_PERIOD=100, MAX_PRICE=1.0,
    COOLDOWN_BARS=1, MAX_HOLD_HOURS=None,
    RESOLVE_SAME_BAR_WITH_5M=True, RESOLVE_INTERVALS=["5M", "15M", "30M"],
    MOM_MODE="fixed", MOM_THRESHOLD=0.02, MOM_ATR_MULT=0.45,
    MAX_MA_DEV=None, MAX_MOM=None, MIN_VOL_RATIO=None, MIN_CLOSE_POS=None,
    EXCLUDE_FLAT_24H=None, BTC_MAX_ALIGNED_DEV=None,
    REQUIRE_HTF_TREND=False, BTC_TREND_FILTER=False,
    EQUITY_INITIAL=100, EQUITY_ORDER_PCT=0.02, EQUITY_STEP=50,
    EQUITY_LEVERAGE=50, EQUITY_SIZING_MODE=2, EQUITY_STOP_BELOW=0,
    REQUEST_SLEEP=0.1, CA_BUNDLE=None,
)
S2_REV = dict(
    MODE="distribution", DIRECTION=None,
    PUMP_WINDOW=8, BASE_WINDOW=72,
    MIN_PUMP_RET=0.08, MAX_PRIOR_RET=0.05,
    MIN_VOL_SURGE=1.5, MIN_BUY_RATIO=0.20,
    BREAKOUT_HOURS=24, MAX_PEAK_VOL_RATIO=0.85, MIN_PEAK_VOL_RATIO=None,
    MIN_MA_DEV=0.05, MIN_RSI=None, MAX_RSI=None,
    MAX_CLOSE_POS=None, MAX_BTC_DEV=None,
    VOL_SPIKE=None, MIN_RUN_24H=None, MIN_RUN_BARS=None,
    ENTRY_TIMING="breakdown", BREAKDOWN_WINDOW=6, RECHECK_ATR_AT_ENTRY=True,
)

# ---- 策略4：爆量竭盡（5分K、只做空）----
S4_CONFIG = dict(S2_CONFIG)
S4_CONFIG.update(
    INTERVAL="5M", MAX_PRICE=None, ATR_MIN_PCT=0, ATR_MAX_PCT=None,
    LIQ_MIN_USD=0, LIQ_MODE="sum24", RESOLVE_INTERVALS=["1M"],
    TAKE_PROFIT=0.04, STOP_LOSS=0.05, EXIT_MODE="fixed",   # 2026-09-17 定為 4%/5%，理由見 pionex_strategy4.py
)
S4_RULE = dict(s4.S4)          # 條件沿用 pionex_strategy4.py 的 S4

BOOKS = {
    # 策略1「延續」已於 2026-09-17 退役。理由：用 22.6 萬筆標註資料測其核心假設，
    # 做多 -0.1pt、做空 -0.0pt（相對同日同方向隨機進場），連反著做也是零 ——
    # 1h動能 + MA20 + OBV 這組訊號對 3%/5% 的觸發順序不帶任何資訊。
    # 先前看到的 62.6% 完全由市場漂移解釋。舊紀錄保留在 state 檔中不再更新。
    # watch 續留：它是無差別進場的實況樣本，兼作 ATR 區間監控（見 ATR 區間監控頁）。
    "watch": {"s": 1, "label": "策略1 觀察組（ATR ≥ 2%，已退役，續跑作 ATR 區間監控）",
              "overrides": {"ATR_MIN_PCT": 0.02, "ATR_MAX_PCT": None}},
    "rev": {"s": 2, "label": "策略2 大戶提款 正式版（啟動前漲幅 ≤ 5%）", "overrides": {}, "rev": {}},
    "rev_wide": {"s": 2, "label": "策略2 大戶提款 放寬版（啟動前漲幅不限）",
                 "overrides": {}, "rev": {"MAX_PRIOR_RET": None}},
    "s4": {"s": 4, "label": "策略4 爆量竭盡（5分K）", "overrides": {}},
}
BOOK_INTERVAL = {b: ("5M" if m["s"] == 4 else "60M") for b, m in BOOKS.items()}
ATR_BANDS = [(0.02, 0.03), (0.03, 0.04), (0.04, 0.05), (0.05, 0.06), (0.06, 0.08), (0.08, None)]

KLINE_LIMIT = 500
WARMUP = {"60M": 150, "5M": 400}   # 指標暖機需要的根數（5M 要涵蓋 24 小時 = 288 根）
WORKERS = 3
HERE = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(HERE, "state", "dryrun_state.json")
OUT_DIR = os.path.join(HERE, "output")
HOUR = pb.HOUR_MS


# ============================== 同期基準 ==============================
# 為什麼要這個：實測「隨機進場」的當日勝率標準差高達 17pt（做多 30%~93%），
# 也就是一筆單子會不會贏，當天行情就解釋掉絕大部分。只看絕對勝率無法分辨
# 「策略有本事」和「那幾天剛好好做」。所以每次執行都抽樣隨機進場當對照，
# 累積成每日基準，報告裡同時給 絕對勝率 / 同期基準 / 超額。
#
# 兩個指標都要看，缺一不可：
#   絕對勝率 < 盈虧平衡勝率 → 賠錢（不管超額多漂亮）
#   超額 ≈ 0                → 只是在吃市場漂移，環境一翻就死
#
# 基準必須按「簽章」（interval + TP + SL + 前視根數）分桶，不能全部帳本共用一份：
# watch/rev/rev_wide 是 60M/3%/5%，s4 是 5M/4%/5%，兩組的K棒解析度、止盈止損門檻、
# 持倉時間尺度完全不同，拿同一份基準相減算「超額」在方法論上不成立（BUG-004）。
# watch/rev/rev_wide 簽章相同，共用一桶反而能互相加大樣本，不必分開抽三次。
BASE_SAMPLES_PER_COIN = 4      # 每次執行每個幣抽幾根當基準樣本（會逐次累積）
# 前視窗改用「根數」而非「小時」，因為不同帳本的 interval 不同，固定小時數換算成根數
# 會讓 5M 帳本抽到離譜地長的前視窗。48 根是依 2026-09-17 對現有已平倉紀錄實測持倉時間
# 選定（median/p75/p90/p95/max，單位小時）：
#   watch     n= 256  median= 3.00  p75= 6.00  p90=12.00  p95=21.00  max=79.00
#   rev       n=   4  median= 2.50  p75= 4.00  p90= 5.80  p95= 6.40  max= 7.00
#   rev_wide  n=  13  median= 2.00  p75= 4.00  p90= 7.80  p95= 9.20  max=11.00
#   s4        n=  69  median= 0.25  p75= 0.42  p90= 0.50  p95= 0.87  max= 1.92
# 60M×48根=48小時，涵蓋 60M 系帳本的 p95=21h 綽綽有餘（與改動前的 BASE_FWD_HOURS=48 等價）；
# 5M×48根=4小時，涵蓋 s4 實測 max=1.92h 全部。用「根數」讓同一個常數對不同 interval
# 自動換算出合理的絕對時間長度，不必為每個 interval 各自維護一個小時數。
BASE_FWD_BARS = 48
_BASE_RNG = np.random.default_rng()


def baseline_key(interval, tp, sl):
    """基準的『簽章』：interval + TP + SL + 前視根數完全相同才可比、才共用同一桶樣本。
       字串格式固定為 f"{interval}_tp{tp}_sl{sl}_fwd{BASE_FWD_BARS}"，
       例如 "60M_tp0.03_sl0.05_fwd48"、"5M_tp0.04_sl0.05_fwd48"。"""
    return f"{interval}_tp{tp:g}_sl{sl:g}_fwd{BASE_FWD_BARS}"


def _baseline_title(key):
    """把簽章字串還原成 SUMMARY 標題用的可讀文字，例如
       "5 分 K／止盈4%／止損5%／前視 4 小時"。"""
    iv, tp_s, sl_s, fwd_s = key.split("_")
    tp, sl, fwd_bars = float(tp_s[2:]), float(sl_s[2:]), int(fwd_s[3:])
    hours = fwd_bars * pb.INTERVAL_MS.get(iv, HOUR) / 3_600_000
    h_txt = f"{hours:g} 小時" if hours >= 1 else f"{hours * 60:g} 分鐘"
    iv_label = {"60M": "60 分 K", "5M": "5 分 K"}.get(iv, iv)
    return f"{iv_label}／止盈{tp * 100:g}%／止損{sl * 100:g}%／前視 {h_txt}"


LEGACY_BASELINE_KEY = baseline_key("60M", BASE_CONFIG["TAKE_PROFIT"], BASE_CONFIG["STOP_LOSS"])


def _migrate_legacy_baseline(state):
    """本次改動之前 baseline 是扁平格式（頂層鍵直接是日期字串，例如 "2026-09-17"），
       全部樣本其實都是用 60M/TP3%/SL5%/前視48小時 抽出來的（當時唯一的抽樣邏輯）。
       用「頂層鍵是否長得像日期」而非版本欄位判斷，因為舊格式從來沒有版本欄位；
       偵測到就整份搬進對應的簽章桶，這是無損且正確的遷移，不能直接丟掉。"""
    B = state.get("baseline")
    if not B:
        return
    if all(_looks_like_date(k) for k in B):
        state["baseline"] = {LEGACY_BASELINE_KEY: B}


def _looks_like_date(s):
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except (TypeError, ValueError):
        return False


def _baseline_sample(state, sym, df, bar, key, tp, sl):
    """從『前視已足夠』的區段隨機抽樣，累積每日的多空隨機進場勝率，存進 key 對應的那一桶。
       同一根K棒的結果是固定的，所以重複執行只會讓樣本數變多、不會改變期望值。"""
    if len(df) < 60:
        return
    t = df["time"].to_numpy()
    hi, lo, cl = df["high"].to_numpy(), df["low"].to_numpy(), df["close"].to_numpy()
    fwd = BASE_FWD_BARS
    ok = np.flatnonzero(t <= t[-1] - fwd * bar)
    if not len(ok):
        return
    B = state.setdefault("baseline", {}).setdefault(key, {})
    for i in _BASE_RNG.choice(ok, min(BASE_SAMPLES_PER_COIN, len(ok)), replace=False):
        i = int(i); e = float(cl[i])
        ltp, lsl, stp, ssl = e * (1 + tp), e * (1 - sl), e * (1 - tp), e * (1 + sl)
        lres = sres = None
        for j in range(i + 1, min(i + 1 + fwd, len(hi))):
            if lres is None:
                a, b = hi[j] >= ltp, lo[j] <= lsl
                lres = "x" if (a and b) else ("w" if a else ("l" if b else None))
            if sres is None:
                a, b = lo[j] <= stp, hi[j] >= ssl
                sres = "x" if (a and b) else ("w" if a else ("l" if b else None))
            if lres and sres:
                break
        d = B.setdefault(f"{pb.to_dt(int(t[i])):%Y-%m-%d}",
                         {"lw": 0, "ll": 0, "sw": 0, "sl": 0})
        if lres in ("w", "l"):
            d["lw" if lres == "w" else "ll"] += 1
        if sres in ("w", "l"):
            d["sw" if sres == "w" else "sl"] += 1


def baseline_rates(B, min_n=30):
    """{日期: (做多基準, 做空基準, 樣本數)}；樣本太少的日子不給值。"""
    out = {}
    for day, d in B.items():
        nl, ns = d["lw"] + d["ll"], d["sw"] + d["sl"]
        out[day] = (d["lw"] / nl if nl >= min_n else None,
                    d["sw"] / ns if ns >= min_n else None, min(nl, ns))
    return out


# 下判定所需的最低樣本。按日叢集自助法的有效樣本是「天數」，所以兩個門檻都要過。
VERDICT_MIN_TRADES = 30
VERDICT_MIN_DAYS = 8


def excess_stats(df, B, n_boot=2000):
    """回傳 dict：同期基準、超額、以及按日叢集自助法的 95% 區間。
       按日重抽而非按筆重抽，因為同一天的單子高度相關，按筆會把區間算得太窄。"""
    if df.empty or not B:
        return None
    rates = baseline_rates(B)
    day = df["entry_time"].map(lambda ms: f"{pb.to_dt(int(ms)):%Y-%m-%d}")
    islong = df["dir"].astype(str).str.contains("多")
    base = pd.Series([(rates.get(d, (None, None, 0))[0] if L else rates.get(d, (None, None, 0))[1])
                      for d, L in zip(day, islong)], index=df.index, dtype="float64")
    m = base.notna()
    if int(m.sum()) < 5:
        return None
    wv = df.loc[m, "win"].to_numpy(dtype=float)
    bv = base[m].to_numpy(dtype=float)
    dv = day[m].to_numpy()
    wr, bl = wv.mean(), bv.mean()
    days = np.unique(dv)
    idx = {d: np.flatnonzero(dv == d) for d in days}
    rng = np.random.default_rng(0)
    bw = np.empty(n_boot); bx = np.empty(n_boot)
    for k in range(n_boot):
        pick = np.concatenate([idx[days[j]] for j in rng.integers(0, len(days), len(days))])
        bw[k] = wv[pick].mean(); bx[k] = wv[pick].mean() - bv[pick].mean()
    # 天數少時單純取自助法百分位會低估區間（實測 15 天時名目95%只涵蓋約88%），
    # 改用自助標準差 × t 分位，把天數自由度算進去。
    from statistics import NormalDist
    D = len(days)
    tmul = (NormalDist().inv_cdf(0.975) if D > 30 else
            {2: 12.71, 3: 4.30, 4: 3.18, 5: 2.78, 6: 2.57, 7: 2.45, 8: 2.36, 9: 2.31,
             10: 2.26, 11: 2.23, 12: 2.20, 13: 2.18, 14: 2.16, 15: 2.14, 16: 2.13,
             17: 2.12, 18: 2.11, 19: 2.10, 20: 2.09, 21: 2.09, 22: 2.08, 23: 2.07,
             24: 2.07, 25: 2.06, 26: 2.06, 27: 2.06, 28: 2.05, 29: 2.05,
             30: 2.05}.get(D, 2.04))
    sw, sx = bw.std(ddof=1), bx.std(ddof=1)
    # 勝率是比例，區間不能跑到 0~1 之外（天數少時 t 分位很大，很容易算出 -41%~201%
    # 這種明顯無意義的數字，印出來會誤導）。超額本身可以是負的，不夾。
    return dict(n=int(m.sum()), wr=wr, base=bl, exc=wr - bl, days=D,
                wr_lo=max(0.0, wr - tmul * sw), wr_hi=min(1.0, wr + tmul * sw),
                ex_lo=(wr - bl) - tmul * sx, ex_hi=(wr - bl) + tmul * sx,
                p_exc=float((bx > 0).mean()))


# ============================== 參數版本控管 ==============================
# 問題：dry run 的紀錄一旦混入不同參數跑出來的交易，勝率就沒有意義了。
# 做法：對每本帳算一個「參數指紋」（只涵蓋會影響訊號與出場的設定）。
#   * 指紋沒變 → 照常累積。
#   * 指紋變了 → 該本帳自動清空重跑，並把變更當下記為 forward_from。
#     從 START_FROM 到 forward_from 之間是「回填」（等於回測，參數是照著這段調出來的，
#     不具樣本外意義）；forward_from 之後才是真正的前進測試。SUMMARY 會分開統計。
# 想一次性強制重置某本帳（例如手動改了不在指紋內的東西），把帳本名放進 RESET_BOOKS，
# 跑完一次後再清空。
RESET_BOOKS = []      # 策略4已於2026-09-17重置過（ret2h .14 / 收盤 .60 / TP 4%）

FP_KEYS = ["INTERVAL", "TAKE_PROFIT", "STOP_LOSS", "FEE_RATE", "EXIT_MODE",
           "TP_ATR_MULT", "SL_ATR_MULT", "LIQ_MIN_USD", "LIQ_MODE",
           "MOM_MODE", "MOM_THRESHOLD", "MOM_ATR_MULT",
           "ATR_PERIOD", "ATR_MIN_PCT", "ATR_MAX_PCT", "MA_PERIOD", "OBV_LOOKBACK",
           "HTF_MA_PERIOD", "MAX_PRICE", "MAX_MA_DEV", "MAX_MOM", "COOLDOWN_BARS",
           "MIN_VOL_RATIO", "MIN_CLOSE_POS", "EXCLUDE_FLAT_24H", "BTC_MAX_ALIGNED_DEV",
           "REQUIRE_HTF_TREND", "BTC_TREND_FILTER", "MAX_HOLD_HOURS",
           "RESOLVE_SAME_BAR_WITH_5M", "RESOLVE_INTERVALS"]


def book_fingerprint(book):
    """回傳這本帳當前參數的指紋。只涵蓋影響訊號與出場的設定；
       資金模擬、抓取速度那類不影響交易結果的不算在內。

       注意：各 *_CONFIG 沒有定義的鍵會殘留前一本帳的值（pb.CONFIG 是全域字典），
       所以這裡只取「當前模式下真正生效」的參數，否則指紋會隨帳本處理順序而變。
       那些殘留值不影響交易（例如 EXIT_MODE="fixed" 時不會用到 *_ATR_MULT）。"""
    use_config(book)
    keys = list(FP_KEYS)
    if pb.CONFIG.get("EXIT_MODE") == "atr":
        keys = [k for k in keys if k not in ("TAKE_PROFIT", "STOP_LOSS")]
    else:
        keys = [k for k in keys if k not in ("TP_ATR_MULT", "SL_ATR_MULT")]
    if pb.CONFIG.get("MOM_MODE") == "atr":
        keys = [k for k in keys if k != "MOM_THRESHOLD"]
    else:
        keys = [k for k in keys if k != "MOM_ATR_MULT"]
    payload = {k: pb.CONFIG.get(k) for k in keys}
    # START_FROM 不列入指紋。它只決定「帳本第一次執行要回補多早」：帳本一旦有紀錄，
    # step_symbol() 走的是 st["last"]、backfill_from 也只對 symbols 為空的帳本設值，
    # 所以改 START_FROM 對運行中的帳本本來就沒有任何作用。把它算進指紋只會把一個
    # 無害的修改變成「全部帳本一次清空」，弊大於利。
    k = BOOKS[book]["s"]
    if k == 2:
        payload["REV"] = {kk: rv.REV.get(kk) for kk in sorted(rv.REV)}
    elif k == 4:
        payload["S4"] = {kk: s4.S4.get(kk) for kk in sorted(s4.S4)}
    return hashlib.sha1(json.dumps(payload, sort_keys=True, default=str)
                        .encode("utf-8")).hexdigest()[:12]


def apply_param_versioning(state, t_now):
    """比對指紋；有變更就清空該本帳並標記前進測試起點。回傳被重置的帳本名單。"""
    reset = []
    for book in BOOKS:
        bk = state["books"][book]
        fp = book_fingerprint(book)
        old = bk.get("fp")
        # RESET_BOOKS 只在「同一個指紋下」生效一次：忘了把名字改回 [] 時，不會變成
        # 每小時清空一次、每次都對全部交易對重做歷史回補。之後若真的又改了參數，
        # 指紋會變，屆時本來就會重置（也會再被記成新指紋下的一次強制重置）。
        forced = book in RESET_BOOKS and bk.get("forced_fp") != fp
        if (old is not None and old != fp) or forced:
            why = "手動指定重置" if forced and old == fp else f"參數變更（{old} → {fp}）"
            n_old = len(bk.get("closed", []))
            bk["hist"] = bk.get("hist", []) + [
                {"fp": old, "until": t_now, "closed": n_old, "why": why}]
            bk["symbols"], bk["closed"] = {}, []
            bk["forward_from"] = t_now
            reset.append((book, why, n_old))
        bk["fp"] = fp
        bk.setdefault("forward_from", None)
        if book in RESET_BOOKS:
            bk["forced_fp"] = fp        # 標記這個指紋下已強制重置過
    return reset


def use_config(book):
    meta = BOOKS[book]
    if meta["s"] == 1:
        pb.CONFIG.update(BASE_CONFIG)
        pb.PARAM_ROWS_HOOK = pb.DIAG_COLUMNS_HOOK = None
    elif meta["s"] == 2:
        pb.CONFIG.update(S2_CONFIG)
        rv.REV.update(S2_REV)
        rv.REV.update(meta.get("rev", {}))
        pb.PARAM_ROWS_HOOK, pb.DIAG_COLUMNS_HOOK = rv.param_rows, s2_diag
    else:
        pb.CONFIG.update(S4_CONFIG)
        s4.S4.update(S4_RULE)
        pb.CONFIG["COOLDOWN_BARS"] = max(1, round(s4.S4["COOLDOWN_HOURS"] * s4.H()))
        pb.PARAM_ROWS_HOOK, pb.DIAG_COLUMNS_HOOK = s4.param_rows, s4.diag_columns
    pb.CONFIG.update(meta["overrides"])


def indicators(book, df, btc):
    k = BOOKS[book]["s"]
    return (pb.add_indicators if k == 1 else rv.add_indicators if k == 2
            else s4.add_indicators)(df, btc)


def s2_diag():
    """策略2 的診斷欄位；trig_time 在狀態檔裡以毫秒儲存，輸出時轉成日期。"""
    def vals(tr):
        out = []
        for _, key, _, _ in rv.DIAG:
            v = tr.get(key)
            out.append(pb.to_dt(int(v)) if key == "trig_time" and v is not None else v)
        return out
    return [n for n, *_ in rv.DIAG], [w for _, _, w, _ in rv.DIAG], vals


def start_ms(bar=None):
    if not START_FROM:
        return None
    dt = datetime.strptime(START_FROM, "%Y-%m-%d %H:%M").replace(tzinfo=pb.TPE)
    bar = bar or HOUR
    return int(dt.timestamp() * 1000) // bar * bar


def now_ms():
    env = os.environ.get("DRYRUN_NOW_MS")          # 測試用
    return int(env) if env else int(time.time() * 1000)


def f(x):
    """轉成 JSON 可存的 float（NaN → None）。"""
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return None if np.isnan(x) else x


# ============================== 狀態 ==============================
def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, encoding="utf-8") as fh:
            st = json.load(fh)
    else:
        st = {"created": start_ms() or now_ms(), "books": {}, "runs": []}
    for b in BOOKS:
        st["books"].setdefault(b, {"symbols": {}, "closed": []})
    _migrate_legacy_baseline(st)
    return st


def save_state(st):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(st, fh, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, STATE_PATH)


# ============================== 資料 ==============================
def prepare(df, t_now, bar=None):
    """去掉未收完的K棒、補齊缺漏時段（與回測 load_hourly 相同處理）。"""
    bar = bar or HOUR
    if df.empty:
        return df
    df = df[df["time"] + bar <= t_now].sort_values("time").reset_index(drop=True)
    if df.empty:
        return df
    grid = pd.DataFrame({"time": np.arange(df["time"].min(), df["time"].max() + 1, bar, dtype="int64")})
    df = grid.merge(df, on="time", how="left")
    df["close"] = df["close"].ffill()
    for c in ("open", "high", "low"):
        df[c] = df[c].fillna(df["close"])
    df["volume"] = df["volume"].fillna(0)
    if "amount" in df.columns:
        df["amount"] = df["amount"].fillna(0)
    return df


def fetch(symbol, t_now, interval="60M", since=None):
    """since = 最早需要的K棒時間（含暖機）。沒給就抓 KLINE_LIMIT 根。"""
    bar = pb.INTERVAL_MS[interval]
    stop = t_now - KLINE_LIMIT * bar
    if since is not None:
        stop = min(stop, since)
    raw = pb.fetch_klines_raw(symbol, interval, t_now, stop)
    return prepare(raw, t_now, bar)


# ============================== 核心：逐根K棒推進 ==============================
def snapshot2(row, bar_time):
    """策略2：取觸發當根的指標值（進場可能落後 1~6 根）。"""
    tt = row.get("trig_time")
    tt = None if tt is None or tt != tt else int(tt)
    out = {"trig_time": tt,
           "wait_bars": int((bar_time - tt) // HOUR) if tt is not None else None,
           "ma_dev_raw": f(row["trig_ma_dev"]), "close_pos_raw": f(row["trig_close_pos"]),
           "btc_dev_raw": f(row["trig_btc_ma_dev"])}
    for k in ("pump_ret", "prior_ret", "vol_surge", "buy_ratio", "peak_vol_ratio", "brk", "rsi", "ret24"):
        out[k] = f(row["trig_" + k])
    return out


def snapshot(row, d):
    return {
        "ret1h": f(row["ret1h"]), "atr_pct": f(row["atr_pct"]), "ma_dev": f(row["ma_dev"]),
        "obv_chg": f(row["obv_chg"]), "liq24": f(row["liq24"]), "vol_ratio": f(row["vol_ratio"]),
        "close_str": f(row["close_pos"] if d == 1 else 1 - row["close_pos"]),
        "htf_al": f(d * row["htf_dev"]), "ret24_al": f(d * row["ret24"]),
        "btc1h_al": f(d * row["btc_ret1h"]), "btcma_al": f(d * row["btc_ma_dev"]),
    }


def snapshot4(row):
    return {k: f(row[k]) for k in s4.TRIG}


def step_symbol(bk, sym, df, strategy=1, bar=None, backfill_from=None):
    """處理此交易對自上次以來新收完的K棒。回傳 (新開倉數, 新平倉數)。"""
    BAR = bar or HOUR
    cfg = pb.CONFIG
    st = bk["symbols"].setdefault(sym, {"last": None, "pos": None, "cool_until": 0, "last_close": None})
    if df.empty:
        return 0, 0
    t = df["time"].to_numpy()
    o, h, l, c = (df[x].to_numpy() for x in ("open", "high", "low", "close"))
    sig, atrp = df["signal"].to_numpy(), df["atr_pct"].to_numpy()
    n = len(df)
    # 第一次看到這個幣：只有這本帳本的首次執行才回補（backfill_from 由 main() 傳入）；
    # 之後才出現的新幣（後上架、或原本被 classify() 濾掉才通過）一律從最新一根開始。
    if st["last"] is None:
        start = int(np.searchsorted(t, backfill_from)) if backfill_from else n - 1
    else:
        start = int(np.searchsorted(t, st["last"], side="right"))
    opened = closed = 0
    for j in range(start, n):
        pos = st["pos"]
        if pos is not None and t[j] > pos["entry_bar"]:
            d, entry, tp, sl = pos["d"], pos["entry"], pos["tp"], pos["sl"]
            hit_tp = h[j] >= tp if d == 1 else l[j] <= tp
            hit_sl = l[j] <= sl if d == 1 else h[j] >= sl
            result, exit_px, exit_ms, note = None, None, t[j] + BAR, ""
            if hit_tp and hit_sl:
                if cfg["RESOLVE_SAME_BAR_WITH_5M"]:
                    result, exit_ms, note = pb.resolve_with_5m(sym, int(t[j]), d, tp, sl)
                else:
                    result, note = "止損", "同1h觸發→保守計止損"
            elif hit_sl:
                result = "止損"
            elif hit_tp:
                result = "止盈"
            if result == "止盈":
                exit_px = tp
                pos["mfe"] = max(pos["mfe"], pos["tp_pct"])
            elif result == "止損":
                gap = (o[j] < sl) if d == 1 else (o[j] > sl)
                exit_px = float(o[j]) if gap and o[j] > 0 else sl
                if gap:
                    note = (note + "；" if note else "") + "開盤跳空穿越止損"
                pos["mae"] = max(pos["mae"], pos["sl_pct"])
            else:
                fav = (h[j] / entry - 1) if d == 1 else (1 - l[j] / entry)
                adv = (1 - l[j] / entry) if d == 1 else (h[j] / entry - 1)
                pos["mfe"], pos["mae"] = max(pos["mfe"], float(fav)), max(pos["mae"], float(adv))
                mh = cfg["MAX_HOLD_HOURS"]
                if mh and (t[j] + BAR - pos["entry_time"]) >= mh * HOUR:
                    result, exit_px, note = "時間出場", float(c[j]), f"持倉達{mh}h"
            if result:
                tr = {k: v for k, v in pos.items() if k not in ("d", "entry_bar")}
                tr.update(exit_time=int(exit_ms), exit=float(exit_px), result=result, note=note)
                bk["closed"].append(tr)
                st["pos"] = None
                st["cool_until"] = int(t[j] + cfg["COOLDOWN_BARS"] * BAR)
                closed += 1
        if st["pos"] is None and sig[j] != 0 and t[j] >= st["cool_until"]:
            d = int(sig[j])
            entry = float(c[j])
            if cfg["EXIT_MODE"] == "atr":
                tp_pct, sl_pct = cfg["TP_ATR_MULT"] * atrp[j], cfg["SL_ATR_MULT"] * atrp[j]
            else:
                tp_pct, sl_pct = cfg["TAKE_PROFIT"], cfg["STOP_LOSS"]
            st["pos"] = {
                "symbol": sym, "dir": "做多" if d == 1 else "做空", "d": d,
                "entry_bar": int(t[j]), "entry_time": int(t[j] + BAR), "entry": entry,
                "tp": entry * (1 + d * tp_pct), "sl": entry * (1 - d * sl_pct),
                "tp_pct": float(tp_pct), "sl_pct": float(sl_pct), "mfe": 0.0, "mae": 0.0,
                **snapshot(df.iloc[j], d),
                **(snapshot2(df.iloc[j], int(t[j])) if strategy == 2 else {}),
                **(snapshot4(df.iloc[j]) if strategy == 4 else {}),
            }
            opened += 1
    st["last"] = int(t[-1])
    st["last_close"] = float(c[-1])
    return opened, closed


def open_as_trades(bk, bar=None):
    bar = bar or HOUR
    out = []
    for sym, st in bk["symbols"].items():
        pos = st.get("pos")
        if pos:
            tr = {k: v for k, v in pos.items() if k not in ("d", "entry_bar")}
            tr.update(exit_time=int(st["last"] + bar), exit=st["last_close"] or pos["entry"],
                      result="未平倉", note="持倉中（以最新收盤估值）")
            out.append(tr)
    return out


# ============================== 輸出 ==============================
def band_label(lo, hi):
    return f"{lo:.0%}–{hi:.0%}" if hi else f"{lo:.0%} 以上"


def closed_df(bk):
    df = pd.DataFrame(bk["closed"])
    if df.empty:
        return df
    d = np.where(df["dir"] == "做多", 1, -1)
    df["ret"] = d * (df["exit"] / df["entry"] - 1) - 2 * BASE_CONFIG["FEE_RATE"]
    df["win"] = df["result"] == "止盈"
    df["month"] = pd.to_datetime(df["entry_time"], unit="ms").dt.tz_localize("UTC").dt.tz_convert("Asia/Taipei").dt.strftime("%Y-%m")
    return df


def atr_monitor(watch):
    """回傳 (表格列, 近6個月最佳區間說明)。"""
    df = closed_df(watch)
    if df.empty:
        return [], "觀察組尚無已平倉交易"
    rows = []
    for m, g in df.groupby("month"):
        row = [m]
        for lo, hi in ATR_BANDS:
            s = g[(g["atr_pct"] > lo) & ((g["atr_pct"] <= hi) if hi else True)]
            row += [len(s), s["win"].mean() if len(s) else None, s["ret"].mean() if len(s) else None]
        rows.append(row)
    months = sorted(df["month"].unique())[-6:]
    recent = df[df["month"].isin(months)]
    best, best_ev = None, None
    for lo, hi in ATR_BANDS:
        s = recent[(recent["atr_pct"] > lo) & ((recent["atr_pct"] <= hi) if hi else True)]
        if len(s) >= 20 and (best_ev is None or s["ret"].mean() > best_ev):
            best, best_ev = band_label(lo, hi), s["ret"].mean()
    txt = (f"近 {len(months)} 個月最佳區間：{best}（每筆 {best_ev:+.2%}）" if best
           else "近6個月各區間交易數不足 20 筆，暫不判斷")
    return rows, txt


def make_extra(state, book, monitor):
    def extra(wb):
        ws = wb.create_sheet("運行紀錄", 1)
        ws.append(["執行時間(台北)", "最新K棒(開盤時間)", "交易對數", "新開倉", "新平倉", "錯誤數", "耗時(秒)"])
        pb.style_header(ws, 1, 7)
        for r in reversed(state["runs"][-200:]):
            b = r["books"].get(book, {})
            ws.append([pb.to_dt(r["time"]), pb.to_dt(r["last_bar"]) if r.get("last_bar") else None,
                       r["symbols"], b.get("opened", 0), b.get("closed", 0), r["errors"], r["seconds"]])
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.font = pb.FONT
            row[0].number_format = row[1].number_format = "yyyy-mm-dd hh:mm"
        pb.set_widths(ws, [18, 18, 10, 8, 8, 8, 9])
        if monitor is not None:
            rows, txt = monitor
            wm = wb.create_sheet("ATR區間監控", 2)
            wm["A1"] = "ATR 區間監控（資料來自觀察組：ATR ≥ 2%，其他條件與主策略相同）"
            wm["A1"].font = Font(name="Arial", size=12, bold=True)
            wm["A2"] = txt
            wm["A2"].font = Font(name="Arial", size=11, bold=True, color="C00000")
            wm["A3"] = "若最佳區間連續數個月不再是 4–5%，代表市場結構可能改變，需要重新檢查策略。"
            wm["A3"].font = pb.FONT
            hdr = ["月份"]
            for lo, hi in ATR_BANDS:
                lab = band_label(lo, hi)
                hdr += [f"{lab} 筆數", f"{lab} 勝率", f"{lab} 每筆報酬"]
            wm.append([])
            wm.append(hdr)
            pb.style_header(wm, 5, len(hdr))
            for r in rows:
                wm.append(r)
            for row in wm.iter_rows(min_row=6):
                for k, cell in enumerate(row):
                    cell.font = pb.FONT
                    if k and k % 3 in (2, 0):
                        cell.number_format = "0.0%" if k % 3 == 2 else "0.00%"
            pb.set_widths(wm, [10] + [9, 9, 10] * len(ATR_BANDS))
    return extra


def write_outputs(state, t_now):
    os.makedirs(OUT_DIR, exist_ok=True)
    monitor = atr_monitor(state["books"]["watch"])
    start_txt = f"{pb.to_dt(state['created']):%Y-%m-%d %H:%M}"
    lines = [f"# 派網策略 Dry Run", "",
             f"開始：{start_txt}　最後更新：{pb.to_dt(t_now):%Y-%m-%d %H:%M}（台北時間）", ""]
    for book, meta in BOOKS.items():
        use_config(book)
        bk = state["books"][book]
        trades = sorted(bk["closed"] + open_as_trades(bk, pb.INTERVAL_MS[BOOK_INTERVAL[book]]),
                        key=lambda x: (x["entry_time"], x["symbol"]))
        scan = [{"symbol": s, "status": "已掃描", "reason": "持倉中" if v.get("pos") else "",
                 "bars": None} for s, v in sorted(bk["symbols"].items())]
        period = f"{start_txt} ~ {pb.to_dt(t_now):%Y-%m-%d %H:%M}（Dry Run）"
        pb.write_excel(trades, scan, os.path.join(OUT_DIR, f"dryrun_{book}.xlsx"), period,
                       extra=make_extra(state, book, monitor if book == "watch" else None))
        pb.PARAM_ROWS_HOOK = pb.DIAG_COLUMNS_HOOK = None
        # ---- SUMMARY.md ----
        df = closed_df(bk)
        opens = open_as_trades(bk, pb.INTERVAL_MS[BOOK_INTERVAL[book]])
        lines += [f"## {meta['label']}", ""]
        if df.empty:
            lines += ["尚無已平倉交易。", ""]
        else:
            be = ((df["sl_pct"] + 2 * BASE_CONFIG["FEE_RATE"]) / (df["tp_pct"] + df["sl_pct"])).mean()
            lines += ["| 已平倉 | 止盈 | 止損 | 勝率 | 盈虧平衡勝率 | 每筆平均報酬 |", "|---|---|---|---|---|---|",
                      f"| {len(df)} | {(df['result'] == '止盈').sum()} | {(df['result'] == '止損').sum()} "
                      f"| {df['win'].mean():.1%} | {be:.1%} | {df['ret'].mean():+.2%} |", ""]
            by_m = df.groupby("month").agg(n=("win", "size"), wr=("win", "mean"), ev=("ret", "mean"))
            lines += ["| 月份 | 筆數 | 勝率 | 每筆報酬 |", "|---|---|---|---|"]
            lines += [f"| {m} | {int(r.n)} | {r.wr:.1%} | {r.ev:+.2%} |" for m, r in by_m.iterrows()]
            lines += [""]
            ff = bk.get("forward_from")
            if ff:
                bf, fw = df[df.entry_time < ff], df[df.entry_time >= ff]
                lines += [f"參數自 {pb.to_dt(ff):%Y-%m-%d %H:%M} 起生效。之前為回填"
                          f"（等同回測，參數是照著那段資料調出來的，不具樣本外意義），"
                          f"之後才是前進測試。", "",
                          "| 區段 | 已平倉 | 止盈 | 止損 | 勝率 | 每筆平均報酬 |",
                          "|---|---|---|---|---|---|"]
                for nm2, seg in (("回填（回測）", bf), ("前進測試", fw)):
                    if seg.empty:
                        lines.append(f"| {nm2} | 0 | — | — | — | — |")
                    else:
                        lines.append(
                            f"| {nm2} | {len(seg)} | {(seg['result'] == '止盈').sum()} "
                            f"| {(seg['result'] == '止損').sum()} | {seg['win'].mean():.1%} "
                            f"| {seg['ret'].mean():+.2%} |")
                lines += [""]
                if len(fw) < 30:
                    lines += [f"_前進測試僅 {len(fw)} 筆，還不足以判斷；"
                              f"以下超額統計含回填段，僅供參考。_", ""]
            bkey = baseline_key(BOOK_INTERVAL[book], pb.CONFIG["TAKE_PROFIT"], pb.CONFIG["STOP_LOSS"])
            es = excess_stats(df, state.get("baseline", {}).get(bkey, {}))
            if es:
                # 按日叢集自助法的有效樣本數是「天數」，不是「筆數」。天數太少時
                # t 分位會非常大（D=2 時 12.71），區間寬到沒有資訊量，而 P(超額>0)
                # 也只是反映那兩天剛好都贏。這種情況只列數字、不下判定，
                # 否則會印出「10 筆交易 → 賺錢且有真本事」這種誤導性結論。
                enough = es["n"] >= VERDICT_MIN_TRADES and es["days"] >= VERDICT_MIN_DAYS
                lines += ["| | 數值 | 95% 區間 |", "|---|---|---|",
                          f"| 勝率 | {es['wr']:.1%} | {es['wr_lo']:.1%} ~ {es['wr_hi']:.1%} |",
                          f"| 同期基準（同日同方向隨機進場） | {es['base']:.1%} | |",
                          f"| 超額 | {es['exc']*100:+.1f}pt | {es['ex_lo']*100:+.1f} ~ {es['ex_hi']*100:+.1f} |", ""]
                if enough:
                    verdict = ("賺錢且有真本事" if es["wr"] > be and es["p_exc"] > 0.9 else
                               "賺錢，但主要是吃市場漂移" if es["wr"] > be else
                               "有本事但仍在賠錢（勝率未過打平）" if es["p_exc"] > 0.9 else "兩邊都不成立")
                    lines += [f"比對 {es['n']} 筆 / {es['days']} 天；"
                              f"P(超額>0) = {es['p_exc']:.0%}；**判定：{verdict}**", ""]
                else:
                    lines += [f"比對 {es['n']} 筆 / {es['days']} 天 —— "
                              f"**未達判定門檻（需 {VERDICT_MIN_TRADES} 筆且 {VERDICT_MIN_DAYS} 天），"
                              f"上表僅供參考，不下判定**。天數少時 95% 區間會寬到沒有意義。", ""]
            else:
                lines += ["_同期基準樣本不足，超額待累積_", ""]
        if opens:
            lines += [f"持倉中 {len(opens)} 筆：", "", "| 交易對 | 方向 | 進場時間 | 進場價 | 止盈價 | 止損價 | 最新價 |",
                      "|---|---|---|---|---|---|---|"]
            for p in sorted(opens, key=lambda x: x["entry_time"]):
                lines.append(f"| {p['symbol']} | {p['dir']} | {pb.to_dt(p['entry_time']):%m-%d %H:%M} | "
                             f"{p['entry']:.6g} | {p['tp']:.6g} | {p['sl']:.6g} | {p['exit']:.6g} |")
            lines += [""]
        if book == "watch":
            lines += [f"**{monitor[1]}**", ""]
    # 每個簽章（interval/TP/SL/前視根數）各印一段，不能混在一起——s4 的 5M/4%/5% 跟
    # watch/rev/rev_wide 的 60M/3%/5% 是不同東西（見 baseline_key() 註解）。
    # 注意：以下 sv.mean()/.std()/.min()/.max() 沿用改動前既有寫法，未對空陣列防呆
    # （BUG-011/F8，本次不處理，範圍見 code.md）；拆成多個簽章桶後，任何一桶只要「當天
    # 做多基準達門檻、做空基準未達門檻」都可能各自觸發同一個既有例外，行為與改動前相同，
    # 沒有刻意修補也沒有刻意繞開。
    for bkey2 in sorted(state.get("baseline", {})):
        B = state["baseline"][bkey2]
        rates = baseline_rates(B)
        rows = [(d, l, sh, n) for d, (l, sh, n) in sorted(rates.items()) if l is not None]
        if not rows:
            continue
        # 做多與做空的 min_n 是分開判定的，所以某一桶可能只有做多達門檻、做空一天都沒達到，
        # 此時 sv 會是空陣列，.min()/.max() 會丟 ValueError 讓整份報告產不出來。
        # 分桶之後每桶樣本變少（5M 桶前視 4 小時，只有約一成樣本會解析），更容易踩到，
        # 所以這裡補上防呆：沒有可用資料就顯示「—」。
        def _stat(arr):
            if len(arr) == 0:
                return "— | — | — | —"
            return (f"{arr.mean():.1%} | {arr.std() * 100:.1f}pt | "
                    f"{arr.min():.1%} | {arr.max():.1%}")
        lv = np.array([r[1] for r in rows if r[1] is not None])
        sv = np.array([r[2] for r in rows if r[2] is not None])
        lines += [f"## 市場基準（隨機進場對照組）—— {_baseline_title(bkey2)}", "",
                  f"每次執行從每個幣隨機抽 {BASE_SAMPLES_PER_COIN} 根K棒，"
                  f"用同一組止盈止損往後模擬，累積成每日的「隨便進場會怎樣」。", "",
                  "| | 平均 | 標準差 | 最低 | 最高 |", "|---|---|---|---|---|",
                  f"| 做多基準 | {_stat(lv)} |",
                  f"| 做空基準 | {_stat(sv)} |",
                  "", "<details><summary>逐日基準</summary>", "",
                  "| 日期 | 做多 | 做空 | 樣本 |", "|---|---|---|---|"]
        lines += [f"| {d} | {l:.1%} | {f'{sh:.1%}' if sh is not None else '—'} | {n} |"
                  for d, l, sh, n in rows[-40:]]
        lines += ["", "</details>", ""]
    with open(os.path.join(OUT_DIR, "SUMMARY.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


def _step(state, books_iv, sym, df, err, interval, bar, need, btc, counts, errors, backfill_from, baseline_sigs):
    if err or df is None or len(df) < need:
        errors.append((sym, err or f"{interval} K棒不足"))
        return
    # 每一輪都對這輪出現過的簽章各抽一次（baseline_sigs 由呼叫端依 books_iv 算好、已去重），
    # 而不是只在 60M 那一輪抽——5M 的 s4 帳本本次改動之前完全沒有自己的基準（BUG-004）。
    # 同一簽章若被多本帳共用（例如 watch/rev/rev_wide 三本帳簽章相同），這裡只會抽一次，
    # 不會因為帳本數重複計數。
    for key, (tp, sl) in baseline_sigs.items():
        try:
            _baseline_sample(state, sym, df, bar, key, tp, sl)
        except Exception:
            pass
    for book in books_iv:
        use_config(book)
        try:
            ind = indicators(book, df.copy(), btc if BOOKS[book]["s"] != 4 else None)
            o_, c_ = step_symbol(state["books"][book], sym, ind, BOOKS[book]["s"], bar, backfill_from[book])
            counts[book]["opened"] += o_
            counts[book]["closed"] += c_
        except Exception:
            errors.append((sym, traceback.format_exc(limit=1)[-200:]))


# ============================== 主程式 ==============================
def main():
    t0 = time.time()
    t_now = now_ms()
    use_config(next(iter(BOOKS)))
    state = load_state()
    for book, why, n_old in apply_param_versioning(state, t_now):
        print(f"[{BOOKS[book]['label']}] {why} → 清空 {n_old} 筆舊紀錄，"
              f"從 {START_FROM} 重新回填；{pb.to_dt(t_now):%m-%d %H:%M} 之後才算前進測試")
    try:
        symbols = pb.get_symbols()
    except Exception as e:
        sys.exit(f"[錯誤] 無法取得交易對清單：{e}")
    universe = set()
    for s in symbols:
        base, reason = pb.classify(s)
        if reason == "":
            universe.add(s["symbol"])
    for bk in state["books"].values():                # 持倉中的幣即使下架也要繼續追蹤
        universe |= {sym for sym, st in bk["symbols"].items() if st.get("pos")}
    btc_sym = "BTC_USDT_PERP" if BASE_CONFIG["MARKET_TYPE"] == "PERP" else "BTC_USDT"
    b = fetch(btc_sym, t_now, "60M")
    btc = pd.DataFrame({"time": b["time"], "btc_ret1h": b["close"] / b["close"].shift(1) - 1,
                        "btc_ma_dev": b["close"] / b["close"].rolling(20).mean() - 1})

    def job_for(interval, since_map):
        def job(sym):
            try:
                return sym, fetch(sym, t_now, interval, since_map.get(sym)), None
            except Exception as e:
                return sym, None, str(e)[:200]
        return job

    # 回補只在「這本帳本」的首次執行生效（state 內尚無任何交易對紀錄）；
    # 之後才進 universe 的新幣，即使是舊帳本也一律從最新一根開始（見 step_symbol()）。
    s0 = start_ms()
    fresh_books = [bk for bk in BOOKS if s0 and not state["books"][bk]["symbols"]]
    if fresh_books:
        earliest = int(b["time"].iloc[0]) if len(b) else None
        print(f"首次執行（{'/'.join(fresh_books)}）：回補 {pb.to_dt(s0):%Y-%m-%d %H:%M} 起的K棒")
        if earliest and earliest > s0:
            print(f"[警告] 60M K棒只能取到 {pb.to_dt(earliest):%Y-%m-%d %H:%M} 之後，"
                  f"{pb.to_dt(s0):%m-%d} ~ {pb.to_dt(earliest):%m-%d} 這段無法回補")
        lim5 = t_now - 10000 * pb.INTERVAL_MS["5M"]
        if lim5 > s0:
            print(f"[警告] 5M K棒只保留到 {pb.to_dt(lim5):%Y-%m-%d %H:%M}，"
                  f"策略4 只能從那時開始回補")

    errors, last_bar = [], None
    counts = {bk: {"opened": 0, "closed": 0} for bk in BOOKS}
    for interval in sorted(set(BOOK_INTERVAL.values()), key=lambda x: -pb.INTERVAL_MS[x]):
        books_iv = [b for b in BOOKS if BOOK_INTERVAL[b] == interval]
        bar = pb.INTERVAL_MS[interval]
        warm = WARMUP.get(interval, 150)
        need = min(warm, 60 if interval == "60M" else 300)
        s0i = start_ms(bar)
        # 這本帳本是否首次執行（state 內尚無任何交易對紀錄），才允許回補新幣
        backfill_from = {bk: (s0i if s0i and not state["books"][bk]["symbols"] else None) for bk in books_iv}
        # 每個幣要抓多早：新的幣從 START_FROM 起算，已在追蹤的只要補到上次處理的位置
        since_map = {}
        for sym in universe:
            lasts = [state["books"][b]["symbols"].get(sym, {}).get("last") for b in books_iv]
            lasts = [x for x in lasts if x]
            base_t = min(lasts) if len(lasts) == len(books_iv) else (s0i or t_now)
            since_map[sym] = base_t - warm * bar
        # 這一輪（同一個 interval）裡各帳本各自的基準簽章，去重後才不會重複抽樣；
        # 用 setdefault 而非硬性要求每本帳都一樣，是為了容許未來同 interval 下
        # 出現不同 TP/SL 的帳本（目前 60M 三本帳、5M 一本帳，剛好都各自只有一種簽章）。
        # 只收 EXIT_MODE=="fixed" 的帳本：baseline 抽樣模擬的是固定 TP/SL 的隨機進場，
        # 對 ATR 動態出場的帳本沒有對應的固定門檻可比（目前沒有帳本用 atr，先留這條防線）。
        sigs = {}
        for bk_ in books_iv:
            use_config(bk_)
            if pb.CONFIG.get("EXIT_MODE") == "fixed":
                tp_, sl_ = pb.CONFIG["TAKE_PROFIT"], pb.CONFIG["STOP_LOSS"]
                sigs.setdefault(baseline_key(interval, tp_, sl_), (tp_, sl_))
        print(f"  抓取 {interval} K棒（{'、'.join(BOOKS[b]['label'] for b in books_iv)}）")
        with ThreadPoolExecutor(WORKERS) as ex:
            for sym, df, err in ex.map(job_for(interval, since_map), sorted(universe)):
                _step(state, books_iv, sym, df, err, interval, bar, need, btc, counts, errors, backfill_from, sigs)
                if df is not None and len(df):
                    last_bar = max(last_bar or 0, int(df["time"].iloc[-1]))
    state["runs"].append({"time": t_now, "last_bar": last_bar, "symbols": len(universe),
                          "errors": len(errors), "seconds": round(time.time() - t0, 1), "books": counts})
    state["runs"] = state["runs"][-500:]
    save_state(state)
    write_outputs(state, t_now)
    print(f"完成：{len(universe)} 個交易對，錯誤 {len(errors)}，耗時 {time.time() - t0:.0f} 秒")
    for book, cnt in counts.items():
        print(f"  {BOOKS[book]['label']}：新開倉 {cnt['opened']}、新平倉 {cnt['closed']}")
    for sym, err in errors[:10]:
        print(f"  [略過] {sym}: {err}")


if __name__ == "__main__":
    main()
