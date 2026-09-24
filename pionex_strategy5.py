#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略5 — 爆量過高（只做空，小時K條件、盤中觸發即進場）
======================================================
條件（以 1 小時 K 為單位）：
  ① 前一根：收盤價比該根最低點漲 ≥ MIN_RISE_FROM_LOW（預設 6%）
  ② 爆量（兩個分支，符合任一個即可）：
       A. 當根累計量 ≥ MIN_VOL_MULT × 前一根量（預設 2 倍，即多一倍以上）
       B. 開盤 FAST_WINDOW_MIN 分鐘內（預設 30），當根累計量 ≥ FAST_VOL_MULT × 前一根量（預設 1 倍＝持平），
          且現價 > 前一根最高價
  ③ 當根：最高價 > 前一根最高價（分支 B 成立時必然成立）
進場：三個條件「第一次同時成立的那一刻」就用當下價格進場做空，不等整點收盤。

v2（目前預設，依群組訊號校準）：① 6%　② 當根量 ≥ 1 倍前根　③ 現價 > 前高
  校準結果：15 種寫法中最像群組（抓到率 64%、命中率 51%、每天 15 筆 vs 群組 12 筆）。
  改回原版：MIN_VOL_MULT=2.0、FAST_VOL_MULT=1.0、HH_MODE="high"。

變體（S5 的兩個開關）：
  REQUIRE_VOL_BURST=True,  REQUIRE_HIGHER_HIGH=True  → ①+②+③（原版，預設）
  REQUIRE_VOL_BURST=False, REQUIRE_HIGHER_HIGH=True  → ①+③：前根漲 6% 後，當根一過前高就進場
  REQUIRE_VOL_BURST=False, REQUIRE_HIGHER_HIGH=False → 只有 ①：那一根 1h K 收盤時若收盤比最低漲 ≥ 6%，
                                                       就在收盤當下進場（不用等下一根）

回測怎麼模擬「盤中觸發」：
  量只會累積、高點只會墊高，所以把這一小時拆成 SUB_INTERVAL 的小K逐根檢查，
  找到三條件第一次同時成立的那根小K，以它的收盤價進場。
  分支 B 的「現價」用小K收盤價判斷；時間窗用小K收盤時刻判斷
  （15M → 第 15、30 分鐘那兩根；5M → 第 5~30 分鐘那六根；30M → 只有第 30 分鐘那根）。
  等於模擬「每 SUB_INTERVAL 掃描一次、掃到就下單」。實盤掃得更快的話，進場會更早一點。
  出場（止盈止損）也在小K上逐根判斷，同根雙觸再用更小的K判先後。

SUB_INTERVAL 的取捨（派網每個週期只保留約 10,000 根）：
  "5M"  → 進場時點最精確（每 5 分鐘掃一次），但只有約 33 天歷史 —— 只裝得下一種行情
  "15M" → 每 15 分鐘掃一次，約 100 天歷史，能跨過好幾輪行情（預設）
  "30M" → 每 30 分鐘掃一次，約 200 天
  "60M" → 不做盤中檢查，約 410 天。只有 ① 的變體本來就在整點收盤進場，用 60M 結果不失真，
          而且能跨過一年多的行情，是驗證「只有 ①」最好的設定。
          出場用 1 小時K；同根雙觸依 5M→15M→30M 判先後，超過約 200 天的舊資料無小K可判 → 保守計止損。
          （①+③ 用 60M 會變成等整點收盤才判斷過前高，進場比實盤晚，不建議）
  建議兩個都跑：15M 看策略在不同行情下是否成立，5M 看掃描頻率對結果影響多大。

輸出除了一般的交易明細與資金模擬，另附：
  * 「驗證」：同期基準（同一天所有幣隨機做空的勝率）、超額、按日叢集 95% 區間、逐塊結果
  * 「條件拆解」：各條件單獨/兩兩/全部的訊號層級勝率與超額，以及「同樣條件但等整點收盤才進場」
    的對照 —— 直接看出盤中觸發進場到底值多少

用法：與 pionex_backtest.py 放同一資料夾
      python pionex_strategy5.py
止盈止損要找最佳組合：pionex_tpsl_sweep.py 把 STRATEGY 改成 "s5" 再跑。
"""
import os
import time
from datetime import datetime
from statistics import NormalDist

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view
from openpyxl.styles import Font, PatternFill

import pionex_backtest as pb

# ============================== 進場條件 ==============================
# pionex_dryrun.py 的策略5 帳本整份取用這個 S5（以及下方 CONFIG 的止盈止損、手續費、出場模式）。
# dry run 只實作 v2 組合；把開關切到 dry run 沒實作的變體，dry run 會在開頭停下並說明是哪個鍵
# （見 pionex_dryrun.check_s5_variant()），要改變體得先改 pionex_dryrun.s5_indicators()。
S5 = {
    "MIN_RISE_FROM_LOW": 0.06,     # ① 前一根：收盤 ÷ 最低 − 1 下限
    "MIN_VOL_MULT": 1.0,           # ②A 當根累計量 ÷ 前一根量 下限；1.0 = 持平（v2）；原版 2.0 = 多一倍以上
    "FAST_VOL_MULT": None,         # ②B 當根累計量 ÷ 前一根量 下限；1.0 = 持平（None = 關閉分支 B；v2 關閉，原版 1.0）
    "FAST_WINDOW_MIN": 30,         # ②B 必須在開盤幾分鐘內達成
    "REQUIRE_VOL_BURST": True,     # ② 爆量開關；False = 不看量（變體，見檔頭）
    "REQUIRE_HIGHER_HIGH": True,   # ③ 過前高；②③ 都關 = 只有 ①，前根收盤即進場
    "HH_MODE": "price",            # ③ 怎麼算過前高："price" = 現價（小K收盤）> 前高（v2，和群組 99.6% 吻合）；
                                   #                 "high" = 當根曾經碰過前高之上（原版）
    "MIN_ABOVE_PH": 0.0,           # ③ 用 "price" 時，現價須高於前高至少此比例（0.01 = 1%）
    # ---- 以下預設關閉（None = 不限），想加過濾再開 ----
    "MIN_TURN24H": None,           # 近24h成交額下限（USDT），例 20_000
    "MAX_TURN24H": None,           # 近24h成交額上限，例 500_000
    "MAX_PRICE": None,             # 價格上限
    # 出場後冷卻幾小時。2026-09-24 使用者決定以 dry run 為準（群組校準版），由 1.0 改為 0：
    # 0 代表「出場的下一根 K 棒起可再進場」（main() 以 max(1, round(COOLDOWN_HOURS × H())) 換算，
    # 0 → 1 根）；同一小時仍最多進場一次（first_per_hour）。
    # pionex_dryrun.py 的策略5 直接取用這份 S5（單一來源），改這裡 dry run 會跟著變。
    "COOLDOWN_HOURS": 0,
}

SUB_INTERVAL = "15M"               # 盤中觸發與出場模擬用的小K："5M" / "15M" / "30M" / "60M"（取捨見檔頭說明）
_LOOKBACK = {"5M": 33, "15M": 100, "30M": 200, "60M": 410}
_RESOLVE = {"5M": ["1M"], "15M": ["5M", "1M"], "30M": ["15M", "5M"], "60M": ["5M", "15M", "30M"]}
HOUR_MS = 3_600_000

CONFIG = {
    "MARKET_TYPE": "PERP",
    "INTERVAL": SUB_INTERVAL,
    "LOOKBACK_DAYS": _LOOKBACK[SUB_INTERVAL],
    "FREEZE_END": None,            # 例 "2026-09-24 00:00"：固定區間結尾，重跑可完全走快取
    "WARMUP_BARS": max(120, 30 * {"5M": 12, "15M": 4, "30M": 2, "60M": 1}[SUB_INTERVAL]),  # 涵蓋 30 小時

    "EXIT_MODE": "fixed",
    "TAKE_PROFIT": 0.03,           # 先比照策略4；最佳組合請用 pionex_tpsl_sweep.py（STRATEGY="s5"）找
    "STOP_LOSS": 0.05,
    "FEE_RATE": 0.0005,
    "MAX_HOLD_HOURS": None,
    "RESOLVE_SAME_BAR_WITH_5M": True,
    "RESOLVE_INTERVALS": _RESOLVE[SUB_INTERVAL],

    "ATR_PERIOD": 14, "MA_PERIOD": 20, "OBV_LOOKBACK": 20, "HTF_MA_PERIOD": 100,
    "LIQ_MIN_USD": 0, "LIQ_MODE": "sum24",
    "ATR_MIN_PCT": 0, "ATR_MAX_PCT": None, "MAX_PRICE": None,
    "MOM_MODE": "fixed", "MOM_THRESHOLD": 0.06, "MOM_ATR_MULT": 0.45,
    "TP_ATR_MULT": 0.7, "SL_ATR_MULT": 1.1,
    "MAX_MA_DEV": None, "MAX_MOM": None, "MIN_VOL_RATIO": None, "MIN_CLOSE_POS": None,
    "EXCLUDE_FLAT_24H": None, "BTC_MAX_ALIGNED_DEV": None,
    "REQUIRE_HTF_TREND": False, "BTC_TREND_FILTER": False, "COOLDOWN_BARS": 1,

    "EQUITY_INITIAL": 100, "EQUITY_ORDER_PCT": 0.02, "EQUITY_STEP": 50,
    "EQUITY_LEVERAGE": 50, "EQUITY_SIZING_MODE": 2, "EQUITY_STOP_BELOW": 0,

    "REQUEST_SLEEP": 0.1,
    "CACHE_DIR": "pionex_cache",
    "CA_BUNDLE": None,
    "OUTPUT": "strategy5_{ts}.xlsx",
    "EXTRA_EXCLUDE": [], "ONLY_SYMBOLS": [],
}

# ---- 驗證用 ----
BASE_FWD_HOURS = 48     # 基準與條件拆解的前視窗（做空基準對窗長不敏感，實測 4h→48h 只差約 1pt）
N_BLOCKS = 6
VERDICT_MIN_TRADES = 30
VERDICT_MIN_DAYS = 8

DIAG = [("① 前根收盤比最低", "sig_rise", 13, "0.00%"), ("前根漲幅(收對收)", "prev_ret", 13, "0.00%"),
("② 爆量分支", "branch", 10, "0"), ("② 觸發時量倍數", "volx", 12, "0.00"), ("③ 觸發時高點超過前高", "hh_pct", 16, "0.00%"),
        ("進場價vs前高", "entry_vs_ph", 12, "0.00%"), ("觸發於第幾分鐘", "minute", 12, "0"),
        ("MA20乖離", "madev", 10, "0.00%"), ("RSI(1h)", "rsi", 9, "0.0"),
        ("ATR%(1h)", "atr1h", 10, "0.00%"), ("24h成交額", "turn", 13, "#,##0")]
TRIG = [k for _, k, _, _ in DIAG]
BRANCH_LABEL = {1: "A 量翻倍", 2: "B 快速持平+過前高", 3: "A+B"}


def H():
    """一小時等於幾根 K 棒（依 INTERVAL 自動換算）。"""
    return max(1, round(pb.bars_per_hour()))


def rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def hour_context(df):
    """在小K上附加：所屬小時、前一小時的 OHLCV、本小時到目前為止的累計量與最高價。"""
    t = df["time"].to_numpy()
    hid = t // HOUR_MS
    g = df.groupby(hid, sort=True)
    hr = pd.DataFrame({"h": g["high"].max(), "l": g["low"].min(), "c": g["close"].last(),
                       "v": g["volume"].sum(), "n": g.size()})
    prev = hr.reindex(hr.index - 1)              # 前一小時（必須剛好是 hid-1）
    prev.index = hr.index
    pprev = hr.reindex(hr.index - 2)
    pprev.index = hr.index
    m = lambda s: pd.Series(hid).map(s).to_numpy()
    df["hid"] = hid
    df["ph_high"], df["ph_low"], df["ph_close"] = m(prev["h"]), m(prev["l"]), m(prev["c"])
    df["ph_vol"], df["ph_n"] = m(prev["v"]), m(prev["n"])
    df["pph_close"] = m(pprev["c"])
    df["cum_vol"] = df.groupby(hid)["volume"].cumsum().to_numpy()
    df["cum_high"] = df.groupby(hid)["high"].cummax().to_numpy()
    df["cum_low"] = df.groupby(hid)["low"].cummin().to_numpy()
    df["n_in_hour"] = (df.groupby(hid).cumcount() + 1).to_numpy()
    df["first_in_hour"] = (df.groupby(hid).cumcount() == 0).to_numpy()
    df["last_in_hour"] = ((t % HOUR_MS) == HOUR_MS - pb.bar_ms())
    return df


def first_per_hour(mask, hid):
    """每小時只保留第一個 True（三條件『第一次』同時成立的那根小K）。"""
    s = pd.Series(mask.astype(int)).groupby(hid).cumsum().to_numpy()
    return mask & (s == 1)


def only_rule1():
    """②③ 都關閉 → 只有 ①，在該根 1h K 收盤當下進場。"""
    return not S5["REQUIRE_VOL_BURST"] and not S5["REQUIRE_HIGHER_HIGH"]


def close_rise_mask(df):
    """只有 ① 的進場點：一小時的最後一根小K（該小時資料完整），且這一小時 收盤 ÷ 最低 − 1 ≥ 門檻。"""
    done = df["last_in_hour"].to_numpy() & (df["n_in_hour"].to_numpy() == H())
    return done & (df["self_rise"] >= S5["MIN_RISE_FROM_LOW"]).fillna(False).to_numpy()


def c3_mask(df):
    """③ 過前高（依 HH_MODE）。"""
    if S5.get("HH_MODE", "high") == "price":
        return (df["close"] > df["ph_high"] * (1 + S5.get("MIN_ABOVE_PH", 0.0))).fillna(False).to_numpy()
    return (df["hh_pct"] > 0).fillna(False).to_numpy()


def burst_branches(df):
    """② 爆量的兩個分支（布林陣列）。
       A：當根累計量 ≥ MIN_VOL_MULT × 前根量
       B：開盤 FAST_WINDOW_MIN 分鐘內、累計量 ≥ FAST_VOL_MULT × 前根量、且現價（小K收盤）> 前根最高價"""
    volx = df["volx"].to_numpy(float)
    with np.errstate(invalid="ignore"):
        a = volx >= S5["MIN_VOL_MULT"]
        if S5.get("FAST_VOL_MULT") is None:
            b = np.zeros(len(df), dtype=bool)
        else:
            b = ((df["minute"].to_numpy() <= S5["FAST_WINDOW_MIN"])
                 & (volx >= S5["FAST_VOL_MULT"])
                 & (df["close"].to_numpy(float) > df["ph_high"].to_numpy(float)))
    return a, b


def add_indicators(df, btc=None):
    C = pb.CONFIG
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    # ---- pb.backtest / 報表需要的共用欄位（皆換算成小時尺度）----
    df["ret1h"] = c / c.shift(1) - 1
    prev = c.shift(1)
    tr = pd.concat([h - l, (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / C["ATR_PERIOD"], adjust=False, min_periods=C["ATR_PERIOD"]).mean()
    df["atr"] = atr
    df["atr_pct"] = atr / c
    df["atr1h"] = atr / c * np.sqrt(H())
    df["ma"] = c.rolling(20 * H()).mean()
    df["ma_dev"] = df["madev"] = c / df["ma"] - 1
    obv = (np.sign(c.diff()).fillna(0) * v).cumsum()
    df["obv_chg"] = obv - obv.shift(C["OBV_LOOKBACK"])
    df["ret24"] = c / c.shift(24 * H()) - 1
    df["vol_ratio"] = v / v.shift(1).rolling(24 * H()).mean()
    rngbar = (h - l).replace(0, np.nan)
    df["cpos"] = df["close_pos"] = (c - l) / rngbar
    df["rsi"] = rsi(c.iloc[::H()], 14).reindex(c.index).ffill()
    turnover = df["amount"] if "amount" in df.columns and df["amount"].sum() > 0 else c * v
    df["turn"] = df["liq24"] = turnover.rolling(24 * H()).sum()
    df["htf_dev"] = c / c.rolling(C["HTF_MA_PERIOD"]).mean() - 1
    if btc is not None:
        df = df.merge(btc, on="time", how="left")
    else:
        df["btc_ret1h"] = np.nan
        df["btc_ma_dev"] = np.nan

    # ---- 策略5 條件（小時尺度，盤中逐根小K檢查）----
    df = hour_context(df)
    df["rise_low"] = df["ph_close"] / df["ph_low"] - 1                    # ①
    df["prev_ret"] = df["ph_close"] / df["pph_close"] - 1                 # 診斷用
    df["volx"] = df["cum_vol"] / df["ph_vol"].replace(0, np.nan)          # ②
    df["hh_pct"] = df["cum_high"] / df["ph_high"] - 1                     # ③（>0 = 已過前高）
    df["entry_vs_ph"] = c / df["ph_high"] - 1                             # 進場價 vs 前高
    df["minute"] = ((df["time"] % HOUR_MS) + pb.bar_ms()) // 60_000       # 這根小K收盤時是該小時第幾分鐘
    df["self_rise"] = c / df["cum_low"] - 1                               # 本小時到目前為止：收盤 ÷ 最低 − 1
    complete = (df["ph_n"] == H()).fillna(False)                          # 前一小時資料要完整才算數

    bA, bB = burst_branches(df)
    df["branch"] = np.where(bA, 1, 0) + np.where(bB, 2, 0)                # 1=A、2=B、3=兩個都成立
    if only_rule1():
        ok = pd.Series(close_rise_mask(df), index=df.index)
        df["sig_rise"] = df["self_rise"]
        df["branch"] = 0
    else:
        df["sig_rise"] = df["rise_low"]
        ok = complete & (df["rise_low"] >= S5["MIN_RISE_FROM_LOW"])
        if S5["REQUIRE_VOL_BURST"]:
            ok &= bA | bB
        else:
            df["branch"] = 0
        if S5["REQUIRE_HIGHER_HIGH"]:
            ok &= c3_mask(df)
    if S5["MIN_TURN24H"] is not None:
        ok &= df["turn"] >= S5["MIN_TURN24H"]
    if S5["MAX_TURN24H"] is not None:
        ok &= df["turn"] <= S5["MAX_TURN24H"]
    if S5["MAX_PRICE"] is not None:
        ok &= c < S5["MAX_PRICE"]
    first = first_per_hour(ok.fillna(False).to_numpy(), df["hid"].to_numpy())
    df["signal"] = np.where(first, -1, 0)
    return df


# ============================== 驗證：逐根結果 ==============================
def short_outcomes(df, tp, sl, fwd, chunk=2000):
    """每一根收盤做空、同一組止盈止損，fwd 根內的結果（向量化、分段以控制記憶體）。
       回傳 float 陣列：1=止盈、0=止損、nan=同根雙觸或視窗內未觸發。"""
    n = len(df)
    out = np.full(n, np.nan)
    if n <= fwd + 1:
        return out
    c = df["close"].to_numpy(float)
    hw_all = sliding_window_view(df["high"].to_numpy(float)[1:], fwd)
    lw_all = sliding_window_view(df["low"].to_numpy(float)[1:], fwd)
    m = len(hw_all)
    big = fwd + 10
    for a in range(0, m, chunk):
        b = min(m, a + chunk)
        e = c[a:b, None]
        hit_sl = np.maximum.accumulate(hw_all[a:b], axis=1) >= e * (1 + sl)
        hit_tp = np.minimum.accumulate(lw_all[a:b], axis=1) <= e * (1 - tp)
        fs = np.where(hit_sl.any(axis=1), hit_sl.argmax(axis=1), big)
        ft = np.where(hit_tp.any(axis=1), hit_tp.argmax(axis=1), big)
        r = np.where(ft < fs, 1.0, np.where(fs < ft, 0.0, np.nan))
        r[(ft == big) & (fs == big)] = np.nan
        out[a:b] = r
    return out


CONDITION_NAMES = [
    "全部K棒（隨機做空）",
    "① 只看前根（前根收盤當下進場）",
    "②A 當根量 ≥ 倍數",
    "②B 開盤快速量持平 + 現價 > 前高",
    "② 爆量（A 或 B）",
    "③ 過前高（依 HH_MODE）",
    "①+②", "①+③", "②+③",
    "①+②A+③（舊版：只有量翻倍）",
    "①+②B（只靠新分支）",
    "①+②+③（策略5，盤中觸發即進場）",
    "新分支額外帶來的：舊版該小時不會觸發",
    "對照：策略5 條件，但等整點收盤才進場",
    "延伸：觸發時價格仍在前高之上",
    "延伸：觸發時價格已跌回前高之下",
]
S5_ROW = CONDITION_NAMES[11]


def current_row():
    """條件拆解頁要加粗的那一列 = 目前設定的版本。"""
    if only_rule1():
        return CONDITION_NAMES[1]
    if not S5["REQUIRE_VOL_BURST"]:
        return CONDITION_NAMES[7] if S5["REQUIRE_HIGHER_HIGH"] else CONDITION_NAMES[1]
    return S5_ROW if S5["REQUIRE_HIGHER_HIGH"] else CONDITION_NAMES[6]


def entry_points(df):
    """條件拆解用：各條件組合的『進場點』（每小時第一次成立的那根小K）。
       只含 ① 時，它在該小時一開始就已知，所以進場點是該小時第一根小K。"""
    hid = df["hid"].to_numpy()
    complete = (df["ph_n"] == H()).fillna(False).to_numpy()
    c1 = complete & (df["rise_low"] >= S5["MIN_RISE_FROM_LOW"]).fillna(False).to_numpy()
    bA, bB = burst_branches(df)
    c2a, c2b = complete & bA, complete & bB
    c2 = c2a | c2b
    c3 = complete & c3_mask(df)
    fp = lambda mk: first_per_hour(mk, hid)
    any_hour = lambda mk: pd.Series(mk).groupby(hid).transform("any").to_numpy()
    all3 = fp(c1 & c2 & c3)
    old = fp(c1 & c2a & c3)
    above = (df["entry_vs_ph"] >= 0).fillna(False).to_numpy()
    n = CONDITION_NAMES
    return {
        n[0]: np.ones(len(df), dtype=bool),
        n[1]: close_rise_mask(df),
        n[2]: fp(c2a),
        n[3]: fp(c2b),
        n[4]: fp(c2),
        n[5]: fp(c3),
        n[6]: fp(c1 & c2),
        n[7]: fp(c1 & c3),
        n[8]: fp(c2 & c3),
        n[9]: old,
        n[10]: fp(c1 & c2b & c3),
        n[11]: all3,
        n[12]: all3 & ~any_hour(old),
        n[13]: any_hour(all3) & df["last_in_hour"].to_numpy(),
        n[14]: all3 & above,
        n[15]: all3 & ~above,
    }


class Validator:
    """邊跑回測邊累積：每日基準（全部幣、全部小K隨機做空）與各條件組合的逐日勝負。"""

    def __init__(self, tp, sl, fwd):
        self.tp, self.sl, self.fwd = tp, sl, fwd
        self.acc, self.unres = {}, {}

    def add(self, df, test_start):
        res = short_outcomes(df, self.tp, self.sl, self.fwd)
        keep = df["time"].to_numpy() >= test_start
        day = pd.to_datetime(df["time"], unit="ms", utc=True).dt.tz_convert("Asia/Taipei") \
                .dt.strftime("%Y-%m-%d").to_numpy()
        for name, mk in entry_points(df).items():
            sel = mk & keep
            if not sel.any():
                continue
            r, d = res[sel], day[sel]
            u = self.unres.setdefault(name, [0, 0])
            u[0] += int(np.isnan(r).sum()); u[1] += int(len(r))
            ok = ~np.isnan(r)
            if not ok.any():
                continue
            g = pd.DataFrame({"d": d[ok], "w": r[ok]}).groupby("d")["w"].agg(["sum", "count"])
            a = self.acc.setdefault(name, {})
            for dd, row in g.iterrows():
                cur = a.setdefault(dd, [0, 0])
                cur[0] += int(row["sum"]); cur[1] += int(row["count"] - row["sum"])

    def base_by_day(self, min_n=30):
        a = self.acc.get(CONDITION_NAMES[0], {})
        return {d: w / (w + l) for d, (w, l) in a.items() if w + l >= min_n}


def cluster_ci(win, base, day, n_boot=4000, seed=0):
    """按日叢集自助法；天數少時改用自助標準差 × t 分位（純百分位會低估）。"""
    win, base, day = np.asarray(win, float), np.asarray(base, float), np.asarray(day)
    days = np.unique(day)
    D = len(days)
    idx = {d: np.flatnonzero(day == d) for d in days}
    rng = np.random.default_rng(seed)
    bw, bx = np.empty(n_boot), np.empty(n_boot)
    for k in range(n_boot):
        pick = np.concatenate([idx[days[j]] for j in rng.integers(0, D, D)])
        bw[k] = win[pick].mean(); bx[k] = win[pick].mean() - base[pick].mean()
    tt = {2: 12.71, 3: 4.30, 4: 3.18, 5: 2.78, 6: 2.57, 7: 2.45, 8: 2.36, 9: 2.31, 10: 2.26,
          12: 2.20, 15: 2.14, 20: 2.09, 25: 2.06, 30: 2.05}
    tm = NormalDist().inv_cdf(0.975) if D > 30 else tt.get(D, tt[min(tt, key=lambda x: abs(x - D))])
    wr, ex = win.mean(), win.mean() - base.mean()
    return dict(days=D, wr=wr, base=base.mean(), exc=ex,
                wr_lo=max(0.0, wr - tm * bw.std(ddof=1)), wr_hi=min(1.0, wr + tm * bw.std(ddof=1)),
                ex_lo=max(-1.0, ex - tm * bx.std(ddof=1)), ex_hi=min(1.0, ex + tm * bx.std(ddof=1)),
                p_exc=float((bx > 0).mean()))


# ============================== 報表 ==============================
def param_rows(rows):
    C = pb.CONFIG
    head = list(rows[:6])
    head[2] = ("K棒週期", f"條件看 1 小時K；進出場用 {pb.interval_label()}",
               "1h K 收盤當下進場" if only_rule1() else
               f"模擬每 {pb.interval_label()} 掃描一次、條件成立即以當下價格進場")
    head[3] = ("止盈", C["TAKE_PROFIT"], "做空：價格下跌此比例")
    head[4] = ("止損", C["STOP_LOSS"], "做空：價格上漲此比例")
    opt = lambda k: S5[k] if S5[k] is not None else "不限"
    mid = [                                            # 8 列，讓「盈虧平衡勝率」落在第 16 列（資金模擬頁會引用）
        ("策略", "策略5 — 爆量過高（只做空）", variant_name()),
        ("① 前根收盤比最低點漲", S5["MIN_RISE_FROM_LOW"], "前一根 1h K：收盤 ÷ 最低 − 1"),
        ("②A 當根量 ÷ 前一根量", S5["MIN_VOL_MULT"] if S5["REQUIRE_VOL_BURST"] else "關閉（不看量）",
         "當根到目前為止的累計量；2.0 = 多一倍以上（A、B 符合一個即可）"),
        ("②B 開盤快速量持平", "關閉" if S5.get("FAST_VOL_MULT") is None or not S5["REQUIRE_VOL_BURST"] else
         f'{S5["FAST_WINDOW_MIN"]} 分鐘內量 ≥ {S5["FAST_VOL_MULT"]:g} 倍前根，且現價 > 前高',
         f"現價 = 那根 {pb.interval_label()} 的收盤價"),
        ("③ 過前高", ("否" if not S5["REQUIRE_HIGHER_HIGH"] else
                     f'現價 > 前高 × (1+{S5.get("MIN_ABOVE_PH", 0):g})' if S5.get("HH_MODE") == "price"
                     else "當根最高價 > 前高"),
         f"現價 = 那根 {pb.interval_label()} 的收盤價"),
        ("進場時機", "該根 1h K 收盤當下" if only_rule1() else "開啟的條件第一次同時成立",
         "以 1h 收盤價進場" if only_rule1() else
         f"以那根 {pb.interval_label()} 的收盤價進場；每小時最多觸發一次"),
        ("24h成交額 / 價格上限", f"{opt('MIN_TURN24H')} ~ {opt('MAX_TURN24H')} / {opt('MAX_PRICE')}", "USDT"),
        ("出場後冷卻", f'{S5["COOLDOWN_HOURS"]} 小時（{C["COOLDOWN_BARS"]} 根）', ""),
    ]
    be = [r for r in rows if r[0] == "盈虧平衡勝率"]
    tail = [("同根雙觸發", "/".join(C["RESOLVE_INTERVALS"]) + " 依序判定",
             "小週期超出保留期限時保守計止損"),
            ("驗證前視窗", f"{BASE_FWD_HOURS} 小時", "同期基準與條件拆解用")]
    return head + mid + be + tail


def variant_name():
    if only_rule1():
        return "變體：只有 ①（前根收盤即進場）"
    hh = ("現價過前高" + (f" {S5.get('MIN_ABOVE_PH', 0):.0%}" if S5.get("MIN_ABOVE_PH") else "")
          if S5.get("HH_MODE") == "price" else "最高價過前高")
    parts = [f"① {S5['MIN_RISE_FROM_LOW']:.0%}"]
    if S5["REQUIRE_VOL_BURST"]:
        parts.append(f"② 量 ≥ {S5['MIN_VOL_MULT']:g} 倍" + ("＋快速持平分支" if S5.get("FAST_VOL_MULT") else ""))
    if S5["REQUIRE_HIGHER_HIGH"]:
        parts.append(f"③ {hh}")
    v2 = (S5["REQUIRE_VOL_BURST"] and S5["REQUIRE_HIGHER_HIGH"] and S5["MIN_VOL_MULT"] == 1.0
          and not S5.get("FAST_VOL_MULT") and S5.get("HH_MODE") == "price" and not S5.get("MIN_ABOVE_PH"))
    return ("v2：" if v2 else "自訂：") + "　".join(parts)


def diag_columns():
    def values(tr):
        v = [tr.get(k) for k in TRIG]
        b = tr.get("branch")
        v[TRIG.index("branch")] = BRANCH_LABEL.get(int(b), "-") if b is not None else None
        return v
    return [n for n, *_ in DIAG], [w for _, _, w, _ in DIAG], values


def validation_sheets(trades, val, test_start, now):
    """回傳 extra(wb)：輸出「驗證」與「條件拆解」兩頁。"""
    C = pb.CONFIG
    tp, sl, fee = C["TAKE_PROFIT"], C["STOP_LOSS"], C["FEE_RATE"]
    be = (sl + 2 * fee) / (tp + sl)
    base = val.base_by_day()
    BAR = pb.bar_ms()
    BOLD = Font(name="Arial", size=10, bold=True)
    TITLE = Font(name="Arial", size=13, bold=True)
    GOOD = PatternFill("solid", start_color="E2F0D9")
    BAD = PatternFill("solid", start_color="FBE5D6")

    closed = [t for t in trades if t["result"] in ("止盈", "止損")]
    rows = []
    for t in closed:
        d = f"{pb.to_dt(t['entry_time'] - BAR):%Y-%m-%d}"
        rows.append(dict(day=d, win=1.0 if t["result"] == "止盈" else 0.0,
                         base=base.get(d, np.nan), t=t["entry_time"]))
    T = pd.DataFrame(rows)

    def extra(wb):
        # ---------------- 驗證 ----------------
        ws = wb.create_sheet("驗證", 1)
        ws["A1"] = "驗證：這個勝率是策略的本事，還是市場剛好順風？"
        ws["A1"].font = TITLE
        ws.append([])
        ws.append([f"同期基準 = 同一天、所有幣、每一根 {pb.interval_label()} 收盤隨機做空，用同一組止盈止損的勝率。"])
        ws.append(["超額 = 策略勝率 − 同期基準。兩個都要看：勝率 < 盈虧平衡 → 賠錢；超額 ≈ 0 → 只是吃漂移。"])
        ws.append([])
        summary = [("已平倉筆數", len(T)), ("盈虧平衡勝率", f"{be:.2%}")]
        if len(T):
            wr = T["win"].mean()
            summary += [("勝率", f"{wr:.1%}"),
                        ("每筆期望報酬", f"{(wr * tp - (1 - wr) * sl - 2 * fee):+.2%}")]
        M = T.dropna(subset=["base"]) if len(T) else T
        if len(M) >= 5:
            ci = cluster_ci(M["win"], M["base"], M["day"])
            enough = len(M) >= VERDICT_MIN_TRADES and ci["days"] >= VERDICT_MIN_DAYS
            summary += [
                ("比對筆數 / 天數", f"{len(M)} 筆 / {ci['days']} 天"),
                ("勝率 95% 區間", f"{ci['wr_lo']:.1%} ~ {ci['wr_hi']:.1%}"),
                ("同期基準", f"{ci['base']:.1%}"),
                ("超額", f"{ci['exc'] * 100:+.1f}pt"),
                ("超額 95% 區間", f"{ci['ex_lo'] * 100:+.1f} ~ {ci['ex_hi'] * 100:+.1f}pt"),
                ("P(超額 > 0)", f"{ci['p_exc']:.0%}"),
            ]
            if enough:
                verdict = ("賺錢且有真本事" if ci["wr"] > be and ci["p_exc"] > 0.9 else
                           "賺錢，但主要是吃市場漂移" if ci["wr"] > be else
                           "有本事但仍在賠錢（勝率未過打平）" if ci["p_exc"] > 0.9 else "兩邊都不成立")
            else:
                verdict = f"樣本不足（需 {VERDICT_MIN_TRADES} 筆且 {VERDICT_MIN_DAYS} 天），不下判定"
            summary.append(("判定", verdict))
        else:
            summary.append(("判定", "可比對的交易太少，無法計算"))
        for k, v in summary:
            ws.append([k, v])
            ws.cell(ws.max_row, 1).font = BOLD
            ws.cell(ws.max_row, 2).font = pb.FONT

        ws.append([])
        ws.append([f"逐塊驗證：回測期間切成 {N_BLOCKS} 塊，看表現是否每一段都成立（只有少數幾段在撐的策略不可信）"])
        ws.cell(ws.max_row, 1).font = BOLD
        hdr = ["區塊", "期間", "筆數", "勝率", "同期基準", "超額(pt)", "每筆EV"]
        ws.append(hdr)
        pb.style_header(ws, ws.max_row, len(hdr))
        edges = np.linspace(test_start, now, N_BLOCKS + 1)
        pos = tot = 0
        for b in range(N_BLOCKS):
            lo_, hi_ = edges[b], edges[b + 1]
            x = T[(T["t"] >= lo_) & (T["t"] < hi_)] if len(T) else T
            per = f"{pb.to_dt(int(lo_)):%Y-%m-%d} ~ {pb.to_dt(int(hi_)):%Y-%m-%d}"
            if len(x) == 0:
                ws.append([f"B{b + 1}", per, 0, "", "", "", ""])
                continue
            w = x["win"].mean()
            xb = x.dropna(subset=["base"])
            bb = xb["base"].mean() if len(xb) else np.nan
            ex = (xb["win"].mean() - bb) * 100 if len(xb) else np.nan
            ev = w * tp - (1 - w) * sl - 2 * fee
            ws.append([f"B{b + 1}", per, len(x), round(w, 4), None if np.isnan(bb) else round(bb, 4),
                       None if np.isnan(ex) else round(ex, 1), round(ev, 4)])
            r = ws.max_row
            ws.cell(r, 4).number_format = ws.cell(r, 5).number_format = "0.0%"
            ws.cell(r, 7).number_format = "+0.00%;-0.00%"
            if not np.isnan(ex):
                tot += 1
                pos += ex > 0
                ws.cell(r, 6).fill = GOOD if ex > 0 else BAD
        ws.append([])
        ws.append([f"超額為正的區塊：{pos} / {tot}"])
        ws.cell(ws.max_row, 1).font = BOLD
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                if not (cell.font and cell.font.bold):
                    cell.font = pb.FONT
        pb.set_widths(ws, [22, 26, 8, 10, 10, 10, 10])

        # ---------------- 條件拆解 ----------------
        wc = wb.create_sheet("條件拆解", 2)
        wc["A1"] = "條件拆解：每個條件到底有沒有用、盤中進場值多少（訊號層級，不含持倉與冷卻）"
        wc["A1"].font = TITLE
        wc.append([])
        wc.append([f"每一列 = 該條件組合在每個小時第一次成立的那根 {pb.interval_label()}，以收盤價做空、"
                   f"止盈 {tp:.0%} / 止損 {sl:.0%}、{BASE_FWD_HOURS} 小時內的結果。"
                   f"和實際交易不同：沒有持倉與冷卻限制、同根雙觸不計。用途是比較條件之間的差異。"])
        wc.append([])
        hdr = ["條件", "訊號數", "每天", f"{BASE_FWD_HOURS}h內未觸發", "勝率", "同日基準",
               "超額(pt)", "每筆EV", "超額為正的區塊"]
        wc.append(hdr)
        pb.style_header(wc, wc.max_row, len(hdr))
        ndays = max(1, (now - test_start) / 86_400_000)
        blk_edges = pd.to_datetime(np.linspace(test_start, now, N_BLOCKS + 1), unit="ms", utc=True) \
            .tz_convert("Asia/Taipei").strftime("%Y-%m-%d")
        for name in CONDITION_NAMES:
            a = val.acc.get(name, {})
            u = val.unres.get(name, [0, 0])
            if not a:
                wc.append([name, u[1], round(u[1] / ndays, 2), "", "", "", "", "", ""])
                continue
            dfa = pd.DataFrame([(d, w, l) for d, (w, l) in a.items()], columns=["d", "w", "l"])
            dfa["b"] = dfa["d"].map(base)
            n_res = dfa["w"].sum() + dfa["l"].sum()
            wr = dfa["w"].sum() / n_res
            m = dfa.dropna(subset=["b"])
            nm_ = (m["w"] + m["l"]).sum()
            bl = (m["b"] * (m["w"] + m["l"])).sum() / nm_ if nm_ else np.nan
            ex = (m["w"].sum() / nm_ - bl) * 100 if nm_ else np.nan
            ev = wr * tp - (1 - wr) * sl - 2 * fee
            pb_ = tb_ = 0
            for b in range(N_BLOCKS):
                mb = m[(m["d"] >= blk_edges[b]) & (m["d"] < blk_edges[b + 1])]
                nn = (mb["w"] + mb["l"]).sum()
                if nn >= 10:
                    tb_ += 1
                    pb_ += (mb["w"].sum() / nn - (mb["b"] * (mb["w"] + mb["l"])).sum() / nn) > 0
            wc.append([name, u[1], round(u[1] / ndays, 2), round(u[0] / max(1, u[1]), 4),
                       round(wr, 4), None if np.isnan(bl) else round(bl, 4),
                       None if np.isnan(ex) else round(ex, 1), round(ev, 4),
                       f"{pb_}/{tb_}" if tb_ else "-"])
            r = wc.max_row
            for col in (4, 5, 6):
                wc.cell(r, col).number_format = "0.0%"
            wc.cell(r, 8).number_format = "+0.00%;-0.00%"
            if not np.isnan(ex) and name != CONDITION_NAMES[0]:
                wc.cell(r, 7).fill = GOOD if ex > 0 else BAD
            if name == current_row():
                for col in range(1, 10):
                    wc.cell(r, col).font = BOLD
        wc.append([])
        wc.append([f"粗體列 = 目前設定的版本（{variant_name()}），和實際回測是同一批訊號，只是沒有冷卻與持倉限制。"])
        wc.append(["讀法：拿掉某一條後「超額」掉很多 → 那一條在扛；拿掉後幾乎不變 → 那一條只是裝飾，可以考慮放寬換訊號量。"])
        wc.append(["新分支值不值得加：看「舊版」與「策略5」兩列的勝率差，以及「新分支額外帶來的」那列本身勝率是否過盈虧平衡。"])
        wc.append(["「對照」列 = 同一批小時、同樣三條件，但等到整點收盤才進場。和上一列相比，就是盤中觸發進場的價值。"])
        wc.append(["「延伸」兩列不屬於策略5 條件，只是觀察：觸發當下價格是否還在前高之上，勝率差多少。"])
        for row in wc.iter_rows(min_row=2):
            for cell in row:
                if not (cell.font and cell.font.bold):
                    cell.font = pb.FONT
        pb.set_widths(wc, [38, 10, 8, 14, 9, 10, 10, 10, 14])
    return extra


# ============================== 主程式 ==============================
def main():
    pb.CONFIG.update(CONFIG)
    pb.CONFIG["COOLDOWN_BARS"] = max(1, round(S5["COOLDOWN_HOURS"] * H()))
    pb.PARAM_ROWS_HOOK, pb.DIAG_COLUMNS_HOOK = param_rows, diag_columns
    C = pb.CONFIG
    BAR = pb.bar_ms()
    if C["FREEZE_END"]:
        now = int(datetime.strptime(C["FREEZE_END"][:16], "%Y-%m-%d %H:%M")
                  .replace(tzinfo=pb.TPE).timestamp() * 1000) // BAR * BAR
        print(f"（已固定區間結尾 {C['FREEZE_END']}，完全使用快取）")
    else:
        now = int(time.time() * 1000) // BAR * BAR
    test_start = (now - C["LOOKBACK_DAYS"] * 24 * pb.HOUR_MS) // HOUR_MS * HOUR_MS
    fetch_start = test_start - C["WARMUP_BARS"] * BAR
    period = f"{pb.to_dt(test_start):%Y-%m-%d %H:%M} ~ {pb.to_dt(now):%Y-%m-%d %H:%M}"
    print(f"策略5 爆量過高（只做空）　條件：1小時K　進出場：{pb.interval_label()}　區間：{period}")
    print(f"版本：{variant_name()}")
    fb = ("" if S5.get("FAST_VOL_MULT") is None or not S5["REQUIRE_VOL_BURST"] else
          f" 或 {S5['FAST_WINDOW_MIN']}分內量 ≥ {S5['FAST_VOL_MULT']:g} 倍且現價 > 前高")
    conds = [f"① 前根收盤比最低 ≥ {S5['MIN_RISE_FROM_LOW']:.0%}"]
    if S5["REQUIRE_VOL_BURST"]:
        conds.append(f"② 當根量 ≥ {S5['MIN_VOL_MULT']:g} 倍前根{fb}")
    if S5["REQUIRE_HIGHER_HIGH"]:
        conds.append("③ 現價 > 前高" if S5.get("HH_MODE") == "price" else "③ 當根高點 > 前高")
    how = "該根收盤當下進場" if only_rule1() else "第一次同時成立即進場"
    print(f"條件：{'　'.join(conds)}　→ {how}　止盈 {C['TAKE_PROFIT']:.0%} / 止損 {C['STOP_LOSS']:.0%}")

    symbols = pb.get_symbols()
    only = {s.upper() for s in C["ONLY_SYMBOLS"]}
    scan_rows, todo = [], []
    for s in symbols:
        base, reason = pb.classify(s)
        if reason is None or (only and base not in only):
            continue
        if reason:
            scan_rows.append({"symbol": s["symbol"], "status": "排除", "reason": reason})
        else:
            todo.append(s["symbol"])
    print(f"待掃描 {len(todo)} 個交易對（排除 {len(scan_rows)}）")

    val = Validator(C["TAKE_PROFIT"], C["STOP_LOSS"], int(BASE_FWD_HOURS * H()))
    all_trades = []
    for k, sym in enumerate(sorted(todo), 1):
        try:
            df = pb.load_hourly(sym, fetch_start, now)
            if len(df) < C["WARMUP_BARS"] + 48 * H():
                scan_rows.append({"symbol": sym, "status": "略過", "reason": "K棒不足", "bars": len(df)})
                continue
            df = add_indicators(df)
            val.add(df, test_start)
            trades = pb.backtest(sym, df, test_start)
            idx = df.set_index("time")
            for tr in trades:
                row = idx.loc[tr["entry_time"] - BAR]
                tr.update({k2: float(row[k2]) if pd.notna(row[k2]) else None for k2 in TRIG})
            all_trades.extend(trades)
            win = df[df["time"] >= test_start]
            scan_rows.append({"symbol": sym, "status": "已掃描", "reason": f"{len(trades)} 筆交易",
                              "bars": len(win), "min": float(win["close"].min()),
                              "max": float(win["close"].max()), "sig_long": 0,
                              "sig_short": int((win["signal"] == -1).sum())})
            if k % 25 == 0:
                print(f"  [{k}/{len(todo)}] 累計 {len(all_trades)} 筆")
        except Exception as e:
            scan_rows.append({"symbol": sym, "status": "失敗", "reason": str(e)[:200]})

    all_trades.sort(key=lambda x: (x["entry_time"], x["symbol"]))
    scan_rows.sort(key=lambda x: ({"已掃描": 0, "略過": 1, "失敗": 2, "排除": 3}[x["status"]], x["symbol"]))
    out = C["OUTPUT"].format(ts=datetime.now(pb.TPE).strftime("%Y%m%d_%H%M"))
    pb.write_excel(all_trades, scan_rows, out, period + f"（策略5，{pb.interval_label()}進出場）",
                   extra=validation_sheets(all_trades, val, test_start, now))

    # ---- 終端摘要 ----
    closed = [t for t in all_trades if t["result"] in ("止盈", "止損")]
    days = max(1, C["LOOKBACK_DAYS"])
    tp, sl, fee = C["TAKE_PROFIT"], C["STOP_LOSS"], C["FEE_RATE"]
    be = (sl + 2 * fee) / (tp + sl)
    print(f"\n訊號 {len(all_trades)} 筆（每天 {len(all_trades) / days:.2f} 筆）")
    if closed:
        wins = sum(t["result"] == "止盈" for t in closed)
        wr = wins / len(closed)
        conservative = sum("保守計止損" in (t.get("note") or "") for t in closed)
        print(f"已平倉 {len(closed)}，止盈 {wins}，勝率 {wr:.1%}（盈虧平衡 {be:.2%}），"
              f"每筆 EV {(wr * tp - (1 - wr) * sl - 2 * fee):+.2%}")
        for code, lab in BRANCH_LABEL.items():
            sub = [t for t in closed if t.get("branch") == code]
            if sub:
                w_ = sum(t["result"] == "止盈" for t in sub) / len(sub)
                print(f"  爆量分支 {lab}：{len(sub)} 筆，勝率 {w_:.1%}")
        if conservative:
            print(f"  其中 {conservative} 筆是同根雙觸且更小週期已超出保留期限 → 保守計止損"
                  f"（真實勝率可能略高）")
        base = val.base_by_day()
        M = [(1.0 if t["result"] == "止盈" else 0.0,
              base.get(f"{pb.to_dt(t['entry_time'] - BAR):%Y-%m-%d}"),
              f"{pb.to_dt(t['entry_time'] - BAR):%Y-%m-%d}") for t in closed]
        M = [x for x in M if x[1] is not None]
        if len(M) >= 5:
            ci = cluster_ci([x[0] for x in M], [x[1] for x in M], [x[2] for x in M])
            print(f"同期基準 {ci['base']:.1%}　超額 {ci['exc'] * 100:+.1f}pt"
                  f"（95% {ci['ex_lo'] * 100:+.1f} ~ {ci['ex_hi'] * 100:+.1f}）"
                  f"　P(超額>0)={ci['p_exc']:.0%}　{len(M)} 筆 / {ci['days']} 天")
    print(f"輸出：{os.path.abspath(out)}（看「驗證」與「條件拆解」兩頁）")


if __name__ == "__main__":
    main()
