# -*- coding: utf-8 -*-
"""
策略4 訊號核心 — 爆量竭盡（只做空）
==================================
由 pionex_strategy4.py 抽離出來的「進場訊號」唯一實作。回測（pionex_strategy4.add_indicators）
與實盤 / paper trading 端都要呼叫這裡，訊號邏輯只允許存在這一份。

相依限制：只 import numpy / pandas 與標準庫。禁止 import pionex_backtest、pionex_strategy4、
requests、openpyxl——實盤端不能因為要算訊號就把整套回測程式（網路、Excel、全域 CONFIG）拖進來。

進場（每根 K 棒收盤判斷，以收盤價進場）：
  拉抬       近 2 小時漲幅 ≥ MIN_RET_2H
  爆量       本根量 ÷ 前 24 小時每根均量 ≥ MIN_VOL_RATIO
  竭盡       收盤位置 ≤ MAX_CLOSE_POS（1=收最高、0=收最低）
  中小盤     近 24 小時成交額 在 [MIN_TURN24H, MAX_TURN24H] 之間（MAX_TURN24H=None 表示不設上限）
訊號值：-1 = 做空進場、0 = 無訊號。出場邏輯不在這裡（由呼叫端決定）。

輸入契約（呼叫端負責，本模組不檢查、不補洞、不排序）：
  * df 依 time 升冪、固定週期、無缺漏。缺漏時段要先補成 close=前值、open/high/low=close、volume=0
    （即 pionex_backtest.load_hourly / pionex_dryrun.prepare 的輸出）。
  * 至少有 open / high / low / close / volume 五欄（數值型）；amount（計價幣成交額）可選，
    有且總和 > 0 時成交額用它，否則以 close × volume 估算。
  * bars_per_hour 是整數（5M → 12、15M → 4、60M → 1），由呼叫端依 K 棒週期換算。
  * 回傳的 Series / DataFrame 與 df 共用同一個 index。

NaN 行為（與抽離前逐字等價）：
  * 暖機期（前 24 小時）ret2h / volr / turn 為 NaN；h == l 的 K 棒 cpos 為 NaN。
  * 任一比較遇到 NaN 視為 False，不會誤發訊號。
"""
import operator

import numpy as np
import pandas as pd

# ============================== 進場參數 ==============================
DEFAULT_PARAMS = {
    # 近2小時漲幅下限（最關鍵的條件）。2026-09-17 由 0.16 降為 0.14：
    #   訊號量 4.1 → 5.4 筆/天（+32%），品質幾乎不變。94天 15M 資料實測風險標準化
    #   報酬在 0.14 與 0.16 幾乎相同（3%/5% 下 0.16/0.19、10%/2% 下 0.72/0.71），
    #   0.12 以下才開始明顯變差。此結論在改用其他 TP/SL 後同樣成立。
    "MIN_RET_2H": 0.14,
    "MIN_VOL_RATIO": 2.0,        # 本根量 ÷ 前24h每根均量
    # 收盤位置上限：衝高後被壓回才算竭盡。
    # 2026-09-17 由 0.70 改為 0.60。依據（94天 15M 標註資料，24.5 萬列）：
    #   收盤≤0.85 → 69.2% ／ ≤0.70 → 71.9% ／ ≤0.60 → 75.4% ／ ≤0.50 → 72.0%
    #   單調改善到 0.60 後進入平台，是區域不是尖峰；6 塊分塊驗證全數超額為正（原本 5/6）。
    #   六塊各自獨立選參數時，六次全部選到 ≤0.6 或 ≤0.5。
    #   34天 5M 資料（不同K棒建構）獨立確認同一方向：0.70→77.0%、0.60→78.8%。
    #   代價：訊號量 5.2 → 4.1 筆/天，但每筆 EV 由 +0.65% 升到 +0.93%，每日期望仍上升。
    #   想換回訊號量：搭配 MIN_RET_2H 降到 0.14，可得 5.4 筆/天 / 73.4% / 6 塊全正。
    "MAX_CLOSE_POS": 0.60,
    "MAX_TURN24H": 500_000,      # 近24h成交額上限（None=不限）
    "MIN_TURN24H": 20_000,       # 下限，避免流動性太差
    "COOLDOWN_HOURS": 1.0,       # 出場後冷卻幾小時（換算根數見 cooldown_bars()）
}
PARAM_KEYS = tuple(DEFAULT_PARAMS)          # 本模組認得的 6 個鍵；params 裡多出來的鍵一律忽略
FEATURE_COLS = ("ret2h", "volr", "cpos", "turn")


def _params(params):
    """params=None → 預設值；否則只取認得的 6 個鍵，缺鍵直接報錯（不悄悄退回預設，避免打錯鍵名沒人發現）。"""
    if params is None:
        return dict(DEFAULT_PARAMS)
    missing = [k for k in PARAM_KEYS if k not in params]
    if missing:
        raise KeyError(f"策略4 參數缺少 {missing}")
    return {k: params[k] for k in PARAM_KEYS}


def _bars(bars_per_hour):
    n = operator.index(bars_per_hour)       # 只收整數（含 numpy 整數），float 直接報錯
    if n < 1:
        raise ValueError(f"bars_per_hour 必須 ≥ 1，收到 {bars_per_hour!r}")
    return n


# ============================== 特徵與訊號 ==============================
def features(df, bars_per_hour):
    """回傳新的 DataFrame（與 df 同 index），含四個訊號特徵：
         ret2h  近 2 小時漲幅          close / close.shift(2H) - 1
         volr   本根量 ÷ 前 24h 每根均量  volume / volume.shift(1).rolling(24H).mean()（shift(1)：本根不算進均量）
         cpos   收盤位置              (close - low) / (high - low)，h == l → NaN
         turn   近 24h 成交額         成交額.rolling(24H).sum()（含本根）
       不會改動傳入的 df。"""
    H = _bars(bars_per_hour)
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    out = pd.DataFrame(index=df.index)
    out["ret2h"] = c / c.shift(2 * H) - 1
    out["volr"] = v / v.shift(1).rolling(24 * H).mean()
    out["cpos"] = (c - l) / (h - l).replace(0, np.nan)
    # 成交額：優先用 API 的 amount（計價幣成交額），沒有則以 收盤×成交量 估算
    turnover = df["amount"] if "amount" in df.columns and df["amount"].sum() > 0 else c * v
    out["turn"] = turnover.rolling(24 * H).sum()
    return out


def signal_from_features(feats, params=None):
    """用 features() 的輸出算訊號。回傳 int Series（與 feats 同 index）：-1 = 做空進場、0 = 無訊號。
       呼叫端已經算好特徵（例如回測要把特徵當診斷欄輸出）時用這個，免得算兩次。"""
    p = _params(params)
    cond = (feats["ret2h"] >= p["MIN_RET_2H"]) & (feats["volr"] >= p["MIN_VOL_RATIO"])
    cond &= feats["cpos"] <= p["MAX_CLOSE_POS"]
    cond &= feats["turn"] >= p["MIN_TURN24H"]
    if p["MAX_TURN24H"] is not None:
        cond &= feats["turn"] <= p["MAX_TURN24H"]
    return pd.Series(np.where(cond.fillna(False), -1, 0), index=feats.index, name="signal")


def signal(df, bars_per_hour, params=None):
    """一步到位：df → 訊號。回傳 int Series（與 df 同 index），-1 = 做空進場、0 = 無訊號。
       params=None 用 DEFAULT_PARAMS；傳入的 dict 只讀 PARAM_KEYS 那 6 個鍵。"""
    return signal_from_features(features(df, bars_per_hour), params)


def cooldown_bars(bars_per_hour, params=None):
    """出場後冷卻根數 = max(1, round(COOLDOWN_HOURS × bars_per_hour))。"""
    p = _params(params)
    return max(1, round(p["COOLDOWN_HOURS"] * _bars(bars_per_hour)))
