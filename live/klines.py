# -*- coding: utf-8 -*-
"""
live.klines — K 棒取數與整理，補洞規則與回測逐欄位一致
=====================================================
A 頻道判定訊號前，候選的 K 棒要整理成 strategy/s4_signal 的輸入契約：time 升冪、固定週期、無缺漏。
「怎麼整理」不是細節：回測（pionex_backtest.load_hourly）與 G2 量測（research/g2_measure.prepare_frame）
對缺漏 K 棒的處理是 close 沿用前值、open/high/low = close、volume = 0。實盤若補得不一樣，
volr / turn 會算出不同的數字，訊號就會靜默偏離回測 —— 沒有錯誤訊息，回測報表照樣漂亮。

live/ 不可 import research/ 或 pionex_*.py，所以 prepare_klines() 在這裡自己實作一份，
**逐行照抄 research/g2_measure.prepare_frame 的步驟與順序**，由 tests/test_signal_feed.py
對拍證明輸出逐欄位相同（AC-3）。要改其中任何一步，兩邊與回測要一起改，並重跑那組對拍。

注意步驟順序是「轉數值 → 依 time 去重（keep="last"，依輸入順序）→ 排序」，不是先排序再去重：
這是 prepare_frame 與 pionex_backtest.load_hourly 的實際順序。同一個 time 出現兩筆不同數值時，
兩種順序可能留下不同的那一筆；以與回測一致為準。

派網 klines 的實測事實（captain 2026-09-23，A1 PRD §2）：
  * 回應 data.klines[]，**新到舊（降冪）**，數值是字串，欄位 time/open/high/low/close/volume，沒有 amount
  * time 是 K 棒**開盤**時刻（UTC epoch ms）
  * 不帶 endTime 時，**最後一根（也就是回應的第一筆）可能是還沒收完的那根**；收盤後極短時間內
    則可能還沒有它。所以「剛收完那根」一律用開盤時刻比對，不看位置
  * limit 上限 500（1000 會回 limit error）
  * 非 ASCII 合約名（哈基米_USDT_PERP 等）用 requests 的 params 傳就會自動 percent-encode，
    所以 symbol 一律放在 params dict，不可以拼進 path
"""

import numpy as np
import pandas as pd

from live import rest_gate

KLINES_PATH = "/api/v1/market/klines"
HOUR_MS = 3_600_000
# 派網 K 棒週期代碼 → 毫秒。bars_per_hour 由此推導，不在別處寫死。
INTERVAL_MS = {"1M": 60_000, "5M": 300_000, "15M": 900_000, "30M": 1_800_000, "60M": HOUR_MS}
KLINE_COLUMNS = ("time", "open", "high", "low", "close", "volume")


def interval_ms(interval):
    """K 棒週期代碼（"5M"）→ 毫秒。不認得的代碼直接 KeyError，不猜。"""
    return INTERVAL_MS[interval]


def bars_per_hour(interval):
    """一小時幾根（5M → 12）。s4_signal.features() 的 bars_per_hour 參數一律用這個推導。"""
    ms = interval_ms(interval)
    if HOUR_MS % ms:
        raise ValueError(f"K 棒週期 {interval} 不能整除 1 小時")
    return HOUR_MS // ms


def raw_frame(rows):
    """klines 回應的 data.klines（list of dict）→ 原樣的 DataFrame，不轉型、不排序、不去重。

    空清單回傳只有欄名的空表（prepare_klines 會原樣回傳它）。
    """
    rows = list(rows or [])
    if not rows:
        return pd.DataFrame(columns=list(KLINE_COLUMNS))
    return pd.DataFrame(rows)


def prepare_klines(df, bar_ms, t_now=None):
    """K 棒 DataFrame → s4_signal 的輸入契約（time 升冪、固定週期、無缺漏）。回傳 (df, filled_bars)。

    與 research/g2_measure.prepare_frame 逐步相同（見模組 docstring）：
      1. time/open/high/low/close/volume/amount 轉數值（壞值變 NaN）
      2. 丟掉 time 是 NaN 的列；依 time 去重，留輸入順序的最後一筆
      3. 依 time 升冪排序
      4. t_now 有給：只留 time + bar_ms <= t_now 的 K 棒（= 在 t_now 之前已收完的）。
         A1 傳 t_now = 目標 K 棒收盤時刻，等於「只留開盤時刻 <= 目標開盤時刻」，
         未收完的那根與之後的一律丟掉
      5. 在首末根之間補成等距網格：缺的 K 棒 close 沿用前值、open/high/low = close、volume = 0
         （amount 若有也補 0）；開頭 close 仍是 NaN 的列丟掉
    filled_bars = 補出來的根數。df 是 None 或空的就原樣回傳 (df, 0)。不改動傳入的 df。
    """
    if df is None or len(df) == 0:
        return df, 0
    df = df.copy()
    for c in ("time", "open", "high", "low", "close", "volume", "amount"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["time"]).drop_duplicates("time", keep="last")
    df["time"] = df["time"].astype("int64")
    df = df.sort_values("time").reset_index(drop=True)
    if t_now is not None:
        df = df[df["time"] + bar_ms <= t_now].reset_index(drop=True)
    if len(df) == 0:
        return df, 0
    grid = pd.DataFrame({"time": np.arange(int(df["time"].min()), int(df["time"].max()) + 1,
                                           bar_ms, dtype="int64")})
    before = len(df)
    df = grid.merge(df, on="time", how="left")
    filled = len(df) - before
    df["close"] = df["close"].ffill()
    for c in ("open", "high", "low"):
        df[c] = df[c].fillna(df["close"])
    df["volume"] = df["volume"].fillna(0.0)
    if "amount" in df.columns:
        df["amount"] = df["amount"].fillna(0.0)
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    return df, filled


OHLCV_COLUMNS = ("open", "high", "low", "close", "volume")


def ohlcv_by_open(rows):
    """klines 原始 rows → {開盤時刻: {(o, h, l, c, v), ...}}（同一時刻若回了不同數值，集合裡就有多筆）。

    數值用與 prepare_klines 相同的 pd.to_numeric 轉，**不可以改用 float()**：兩者對 16 位以上有效數字的
    十進位字串會差 1 ULP（DQA 實測 pandas 3.0.5），同一個字串從兩條路轉出來就不相等，K 棒定稿比對
    會誤報「不一致」。凡是要跟 BarResult.judged[*]["target_bar"]（來自 prepare_klines）比的，都走這裡。
    缺欄位、時刻或數值轉不出來的列略過。
    """
    df = raw_frame(rows)
    if len(df) == 0 or any(c not in df.columns for c in ("time",) + OHLCV_COLUMNS):
        return {}
    times = pd.to_numeric(df["time"], errors="coerce")
    cols = [pd.to_numeric(df[c], errors="coerce") for c in OHLCV_COLUMNS]
    out = {}
    for i in range(len(df)):
        t = times.iloc[i]
        vals = tuple(float(col.iloc[i]) for col in cols)
        if pd.isna(t) or any(v != v for v in vals):
            continue
        out.setdefault(int(t), set()).add(vals)
    return out


def row_open_ms(row):
    """單筆 kline 的開盤時刻（int ms）；缺欄位或不是數字回 None。"""
    try:
        return int(float(row["time"]))
    except (KeyError, TypeError, ValueError):
        return None


def has_bar(rows, open_ms):
    """回應裡有沒有開盤時刻恰好是 open_ms 的那根。只看時刻，不看位置（位置在收盤前後會變）。"""
    return any(row_open_ms(r) == open_ms for r in rows or [])


def fetch_klines(gate, symbol, interval, limit, *, priority=rest_gate.PRIORITY_FOREGROUND):
    """經過閘門打一次 klines（不帶 endTime），回傳 data.klines 原始 list（新到舊，可能含未收完那根）。

    symbol 放在 params dict 裡，由 requests 做 percent-encode；不可以拼進 path。
    例外照閘門的規則往外拋（RestBanned / live.pionex_api.ApiError）。
    """
    params = {"symbol": symbol, "interval": interval, "limit": int(limit)}
    js = gate.get(KLINES_PATH, params, priority=priority)
    data = js.get("data") if isinstance(js, dict) else None
    rows = data.get("klines") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ValueError(f"{KLINES_PATH} {symbol}: 回應沒有 data.klines 清單")
    return rows
