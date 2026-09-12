#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
派網 (Pionex) USDT 交易對 — 動能 + ATR + MA20 + OBV + 流動性 進場回測
=====================================================================
進場條件（皆以 1 小時 K 棒「收盤」時判斷，於該收盤價進場）：
  做多：1h 漲幅 >  +2%  且 收盤 > MA20  且 OBV(now) > OBV(20h前)
  做空：1h 跌幅 <  -2%  且 收盤 < MA20  且 OBV(now) < OBV(20h前)
  共同：ATR(14)/收盤 > 2%、近24h成交額 > 5萬 USDT、收盤價 < 1 USDT
出場：同方向 +3% 止盈 / 反方向 -5% 止損（盤中觸價）
同一交易對同時間只持有一筆；出場後才接受下一個訊號。

需求套件：pip install requests pandas numpy openpyxl
執行：python pionex_backtest.py
"""
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import requests
from openpyxl import Workbook
from openpyxl.chart import LineChart, Reference
from openpyxl.formatting.rule import CellIsRule
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

# ============================== 參數設定 ==============================
CONFIG = {
    "MARKET_TYPE": "PERP",        # "PERP"=永續合約(可做空，建議) / "SPOT"=現貨
    "LOOKBACK_DAYS": 90,          # 回測區間（天）
    "WARMUP_BARS": 120,           # 指標暖機用的額外K棒數

    "TAKE_PROFIT": 0.03,          # 止盈 3%
    "STOP_LOSS": 0.05,            # 止損 5%
    "FEE_RATE": 0.0005,           # 單邊手續費（請改成你的實際費率；進出各扣一次）

    "LIQ_MIN_USD": 50_000,        # 流動性門檻
    "LIQ_MODE": "avg_hourly",          # "sum24"=近24根1h成交額加總 / "avg_hourly"=近24根每小時平均
    "MOM_THRESHOLD": 0.02,        # 動能：1h 漲跌幅門檻（MOM_MODE="fixed" 時使用）
    "MOM_MODE": "fixed",          # "fixed"=固定比例 / "atr"=1h漲跌幅 > MOM_ATR_MULT × ATR%
    "MOM_ATR_MULT": 0.45,
    "EXIT_MODE": "fixed",         # "fixed"=TAKE_PROFIT/STOP_LOSS 固定比例 / "atr"=依ATR倍數
    "TP_ATR_MULT": 0.7,           # EXIT_MODE="atr"：止盈 = 0.7 × ATR%
    "SL_ATR_MULT": 1.1,           # EXIT_MODE="atr"：止損 = 1.1 × ATR%
    "ATR_PERIOD": 14,
    "ATR_MIN_PCT": 0.04,          # ATR/價格 下限
    "ATR_MAX_PCT": 0.05,          # ATR/價格 上限（None=不限）；固定3%/5%止盈止損最適合 ATR 約4–5% 的幣
    "MA_PERIOD": 20,
    "OBV_LOOKBACK": 20,
    "MAX_PRICE": None,            # 訊號當下收盤價須 < 此值（None=不限）

    # ---- 過濾條件（None / 0 = 不啟用）----
    "MAX_MA_DEV": 0.06,           # 價格距MA20乖離上限，例 0.06 = 偏離超過6%不追
    "MAX_MOM": 0.10,              # 1h漲跌幅上限，例 0.10 = 單小時已漲跌超過10%不追
    "COOLDOWN_BARS": 1,           # 出場後冷卻K棒數，例 1 = 出場那根收盤不立即再進場
    "MIN_VOL_RATIO": 0.35,        # 量比下限：訊號K棒成交量 ÷ 前20根平均
    "EXCLUDE_FLAT_24H": None,     # 24h漲跌幅絕對值 < 此值不進場（盤整後的單根急動），None=不啟用
    "BTC_MAX_ALIGNED_DEV": None,  # BTC 已朝同方向偏離自身MA20超過此值不進場，例 0.006；None=不啟用
    "MIN_CLOSE_POS": None,        # 收盤強度下限(0~1)：收在K棒順勢端的位置，例 0.6
    "HTF_MA_PERIOD": 100,         # 長週期均線(1h×100 ≈ 4H MA25)，只用於診斷欄位與下方過濾
    "REQUIRE_HTF_TREND": False,   # True = 做多須在長週期均線上、做空須在其下
    "BTC_TREND_FILTER": False,    # True = 做多須 BTC 在自身 MA20 上、做空須在其下

    "MAX_HOLD_HOURS": None,       # 時間停損（小時），None=不設
    "RESOLVE_SAME_BAR_WITH_5M": True,  # 同一根1h同時碰到止盈/止損時，抓小週期K判斷先後
    "RESOLVE_INTERVALS": ["5M", "15M", "30M"],  # 依序嘗試（派網5分K歷史約只有1個月，舊的改用15/30分K）

    "REQUEST_SLEEP": 0.12,        # 每次API請求間隔（秒）
    "CACHE_DIR": "pionex_cache",  # K棒快取（改參數重跑時不用重新下載）
    "CA_BUNDLE": None,            # 公司SSL檢查憑證 .pem 路徑；None=系統預設
    "OUTPUT": "pionex_backtest_{ts}.xlsx",

    # ---- 資金模擬（依時間順序下單）；以下數值也可在 Excel『資金模擬』頁直接修改 ----
    "EQUITY_INITIAL": 100,        # 初始本金
    "EQUITY_ORDER_PCT": 0.02,     # 每筆下單金額 = 基準本金 × 此比例
    "EQUITY_STEP": 50,            # 本金達到此級距的倍數才上調下單金額（不回滾）
    "EQUITY_LEVERAGE": 1,         # 槓桿倍數：部位大小 = 下單金額 × 槓桿
    "EQUITY_SIZING_MODE": 1,      # 1=不回滾(本金達級距才上調，不下調) / 2=即時本金(每筆 = 當下本金 × 比例)
    "EQUITY_STOP_BELOW": 0,       # 本金 ≤ 此值即停止開新倉（模擬爆倉或個人停損線）

    "EXTRA_EXCLUDE": [],          # 額外排除的幣（base，例如 ["XYZ"]）
    "ONLY_SYMBOLS": [],           # 測試用：只跑這些 base，例如 ["DOGE", "PEPE"]
}

# ---- 強掛勾 / 包裝 / 穩定幣 ----
PEGGED = {
    "USDT", "USDC", "FDUSD", "TUSD", "DAI", "USDE", "USDD", "PYUSD", "USDP", "BUSD",
    "USD1", "RLUSD", "USDS", "USDX", "USDXO", "USDB", "GUSD", "LUSD", "FRAX", "SUSD",
    "USD0", "USDF", "BFUSD", "USDG", "EURC", "EURT", "EURI", "AEUR", "EURS",
    "XAUT", "PAXG", "KAU", "KAG",
    "WBTC", "WETH", "STETH", "WSTETH", "WBETH", "CBBTC", "BTCB", "CBETH", "RETH",
    "METH", "WEETH", "EZETH", "BNSOL", "JITOSOL", "MSOL", "LBTC", "SOLVBTC",
}
STABLE_RE = re.compile(r"^(USD[A-Z0-9]*|[A-Z]*USD|EUR[A-Z]?)$")
LEVERAGED_RE = re.compile(r"^[A-Z0-9]+\d+[LS]$")      # BTC3L / ETH5S 之類槓桿代幣

# ---- 股票代幣化（價格幾乎都 >1，實際上也會被 MAX_PRICE 擋掉，這裡雙重保險）----
STOCK_TICKERS = {
    "AAPL", "TSLA", "NVDA", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NFLX", "COIN",
    "MSTR", "HOOD", "CRCL", "PLTR", "AMD", "INTC", "ORCL", "AVGO", "BABA", 
    "SPY", "QQQ", "IWM", "TQQQ", "SQQQ", "GLD", "SLV", "MCD", "JPM",
    "DIS", "UBER", "ABNB", "SHOP", "PYPL", "CRM", "ADBE", "TSM", "ASML", "SMCI",
    "QCOM", "NKE", "WMT", "COST", "LLY", "UNH", "JNJ", "PFE", "SBUX", "RIVN",
    "LCID", "NIO", "XPEV", "BIDU", "PDD", "CRWD", "PANW", "ROKU", "RDDT",
    "CVNA", "IBM", "CSCO", "MRVL", "AMAT", "BRKB",
}


# ---- 派網的美股/ETF 永續多以「代號+X」命名（AAOIX、SOXLX…）；以下是結尾剛好是 X 的真加密貨幣 ----
CRYPTO_X_WHITELIST = {"AVAX", "BEAMX", "POLYX", "FLUX", "APEX", "DYDX", "IOTX", "ZETAX", "SAFEX"}
# ---- 非加密的商品/個股（不以 X 結尾）----
NON_CRYPTO = {"WTI", "BRENT", "XAG", "XAU", "XPT", "XPD", "NATGAS", "COPPER", "SKHY", "SMSN", "KORU"}


def is_stock_token(base: str) -> bool:
    if base in NON_CRYPTO:
        return True
    if base.endswith("X") and len(base) >= 4 and base not in CRYPTO_X_WHITELIST:
        return True
    if base in STOCK_TICKERS:
        return True
    for suf in ("X", "ON", "B", "STOCK"):
        if base.endswith(suf) and base[: -len(suf)] in STOCK_TICKERS:
            return True
    return False


# ============================== API ==============================
BASE_URL = "https://api.pionex.com"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "pionex-backtest/1.0"})
HOUR_MS = 3_600_000
TPE = timezone(timedelta(hours=8))


class ApiError(Exception):
    pass


def api_get(path, params, retries=5):
    verify = CONFIG["CA_BUNDLE"] if CONFIG["CA_BUNDLE"] is not None else True
    last = None
    for k in range(retries):
        try:
            r = SESSION.get(BASE_URL + path, params=params, timeout=20, verify=verify)
            if r.status_code in (403, 451):
                raise ApiError(f"HTTP {r.status_code}：連線被拒，可能是所在地區/IP 被派網封鎖")
            if r.status_code == 429:
                time.sleep(2 ** k)
                continue
            js = r.json()
            if not js.get("result", False):
                raise ApiError(f"{js.get('code')} {js.get('message')}")
            time.sleep(CONFIG["REQUEST_SLEEP"])
            return js
        except requests.exceptions.SSLError as e:
            sys.exit(f"[SSL錯誤] {e}\n→ 公司網路有 SSL inspection 時，請把公司根憑證 .pem 路徑填到 CONFIG['CA_BUNDLE']")
        except ApiError:
            raise
        except Exception as e:  # 網路抖動 → 重試
            last = e
            time.sleep(1.5 * (k + 1))
    raise ApiError(f"重試 {retries} 次失敗：{last}")


def get_symbols():
    js = api_get("/api/v1/common/symbols", {"type": CONFIG["MARKET_TYPE"]})
    return js["data"]["symbols"]


def fetch_klines_raw(symbol, interval, end_ms, stop_ms):
    """由 end_ms 往回翻頁，直到涵蓋 stop_ms。"""
    rows, cursor = [], end_ms
    while True:
        js = api_get("/api/v1/market/klines",
                     {"symbol": symbol, "interval": interval, "endTime": cursor, "limit": 500})
        kl = js["data"]["klines"]
        if not kl:
            break
        rows.extend(kl)
        oldest = min(int(k["time"]) for k in kl)
        if oldest <= stop_ms or len(kl) < 500 or oldest >= cursor:
            break
        cursor = oldest - 1
    if not rows:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    keep = [c for c in ["time", "open", "high", "low", "close", "volume", "amount"] if c in df.columns]
    df = df[keep].copy()
    for c in keep:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["time"] = df["time"].astype("int64")
    return df.drop_duplicates("time").sort_values("time").reset_index(drop=True)


def load_hourly(symbol, start_ms, now_ms):
    """讀快取 + 增量下載 1h K棒，並補齊缺漏小時。"""
    os.makedirs(CONFIG["CACHE_DIR"], exist_ok=True)
    path = os.path.join(CONFIG["CACHE_DIR"], f"{symbol}_60M.csv")
    cached = pd.read_csv(path) if os.path.exists(path) else None
    if cached is not None and len(cached) and cached["time"].min() <= start_ms:
        new = fetch_klines_raw(symbol, "60M", now_ms, int(cached["time"].max()))
        df = pd.concat([cached, new]).drop_duplicates("time", keep="last")
    else:
        df = fetch_klines_raw(symbol, "60M", now_ms, start_ms)
    if df.empty:
        return df
    df = df.sort_values("time").reset_index(drop=True)
    df = df[df["time"] + HOUR_MS <= now_ms]            # 丟掉尚未收完的K棒
    df.to_csv(path, index=False)
    df = df[df["time"] >= start_ms]
    if df.empty:
        return df
    # 補齊缺漏的小時（無成交）→ 價格沿用前收、量=0
    grid = pd.DataFrame({"time": np.arange(df["time"].min(), df["time"].max() + 1, HOUR_MS, dtype="int64")})
    df = grid.merge(df, on="time", how="left")
    df["close"] = df["close"].ffill()
    for c in ("open", "high", "low"):
        df[c] = df[c].fillna(df["close"])
    df["volume"] = df["volume"].fillna(0)
    if "amount" in df.columns:
        df["amount"] = df["amount"].fillna(0)
    return df.reset_index(drop=True)


# ============================== 指標與訊號 ==============================
def add_indicators(df, btc=None):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["ret1h"] = c / c.shift(1) - 1
    prev_c = c.shift(1)
    tr = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / CONFIG["ATR_PERIOD"], adjust=False,
                       min_periods=CONFIG["ATR_PERIOD"]).mean()    # Wilder ATR
    df["atr_pct"] = df["atr"] / c
    df["ma"] = c.rolling(CONFIG["MA_PERIOD"]).mean()
    df["ma_dev"] = c / df["ma"] - 1
    obv = (np.sign(c.diff()).fillna(0) * v).cumsum()
    df["obv_chg"] = obv - obv.shift(CONFIG["OBV_LOOKBACK"])
    # 成交額：優先用API的 amount（計價幣成交額），沒有則以 收盤×成交量 估算
    turnover = df["amount"] if "amount" in df.columns and df["amount"].sum() > 0 else c * v
    df["liq24"] = turnover.rolling(24).sum()
    liq_val = df["liq24"] if CONFIG["LIQ_MODE"] == "sum24" else df["liq24"] / 24
    mp = CONFIG["MAX_PRICE"]
    common = (df["atr_pct"] > CONFIG["ATR_MIN_PCT"]) & (liq_val > CONFIG["LIQ_MIN_USD"])
    if CONFIG["ATR_MAX_PCT"] is not None:
        common &= df["atr_pct"] <= CONFIG["ATR_MAX_PCT"]
    if mp is not None:
        common &= c < mp
    if CONFIG["MAX_MA_DEV"] is not None:
        common &= df["ma_dev"].abs() <= CONFIG["MAX_MA_DEV"]
    if CONFIG["MAX_MOM"] is not None:
        common &= df["ret1h"].abs() <= CONFIG["MAX_MOM"]
    # ---- 診斷欄位 ----
    df["vol_ratio"] = v / v.shift(1).rolling(20).mean()
    rngbar = (h - l).replace(0, np.nan)
    df["close_pos"] = (c - l) / rngbar                         # 1=收最高、0=收最低
    df["htf_dev"] = c / c.rolling(CONFIG["HTF_MA_PERIOD"]).mean() - 1
    df["ret24"] = c / c.shift(24) - 1
    if btc is not None:
        df = df.merge(btc, on="time", how="left")
    else:
        df["btc_ret1h"] = np.nan
        df["btc_ma_dev"] = np.nan
    th = CONFIG["MOM_ATR_MULT"] * df["atr_pct"] if CONFIG["MOM_MODE"] == "atr" else CONFIG["MOM_THRESHOLD"]
    long_ = common & (df["ret1h"] > th) & (c > df["ma"]) & (df["obv_chg"] > 0)
    short = common & (df["ret1h"] < -th) & (c < df["ma"]) & (df["obv_chg"] < 0)
    if CONFIG["MIN_VOL_RATIO"] is not None:
        long_ &= df["vol_ratio"] >= CONFIG["MIN_VOL_RATIO"]
        short &= df["vol_ratio"] >= CONFIG["MIN_VOL_RATIO"]
    if CONFIG["MIN_CLOSE_POS"] is not None:
        long_ &= df["close_pos"] >= CONFIG["MIN_CLOSE_POS"]
        short &= (1 - df["close_pos"]) >= CONFIG["MIN_CLOSE_POS"]
    if CONFIG["EXCLUDE_FLAT_24H"] is not None:
        flat = df["ret24"].abs() < CONFIG["EXCLUDE_FLAT_24H"]
        long_ &= ~flat
        short &= ~flat
    if CONFIG["BTC_MAX_ALIGNED_DEV"] is not None:
        long_ &= df["btc_ma_dev"] <= CONFIG["BTC_MAX_ALIGNED_DEV"]
        short &= df["btc_ma_dev"] >= -CONFIG["BTC_MAX_ALIGNED_DEV"]
    if CONFIG["REQUIRE_HTF_TREND"]:
        long_ &= df["htf_dev"] > 0
        short &= df["htf_dev"] < 0
    if CONFIG["BTC_TREND_FILTER"]:
        long_ &= df["btc_ma_dev"] > 0
        short &= df["btc_ma_dev"] < 0
    df["signal"] = np.where(long_, 1, np.where(short, -1, 0))
    return df


def resolve_with_5m(symbol, bar_open, d, tp, sl):
    """同一根1h同時碰到止盈與止損 → 依序用 5M/15M/30M K棒判斷誰先。回傳 (結果, 出場時間ms, 說明)。"""
    bar_end = bar_open + HOUR_MS
    for iv in CONFIG["RESOLVE_INTERVALS"]:
        minutes = int(iv[:-1])
        step = minutes * 60_000
        try:
            js = api_get("/api/v1/market/klines",
                         {"symbol": symbol, "interval": iv, "endTime": bar_end - 1, "limit": 60 // minutes})
            kl = sorted((k for k in js["data"]["klines"] if bar_open <= int(k["time"]) < bar_end),
                        key=lambda k: int(k["time"]))
        except Exception:
            kl = []
        if not kl:
            continue                        # 這個週期沒有歷史資料 → 換下一個
        label = f"{minutes}分K"
        for k in kl:
            hi, lo, t = float(k["high"]), float(k["low"]), int(k["time"])
            hit_tp = hi >= tp if d == 1 else lo <= tp
            hit_sl = lo <= sl if d == 1 else hi >= sl
            if hit_tp and hit_sl:
                return "止損", t + step, f"同1h觸發；{label}仍同根→保守計止損"
            if hit_sl:
                return "止損", t + step, f"同1h觸發；{label}判定先止損"
            if hit_tp:
                return "止盈", t + step, f"同1h觸發；{label}判定先止盈"
    return "止損", bar_end, "同1h觸發；無小週期K資料→保守計止損"


def backtest(symbol, df, test_start_ms):
    t = df["time"].to_numpy()
    o, h, l, c = (df[x].to_numpy() for x in ("open", "high", "low", "close"))
    sig = df["signal"].to_numpy()
    max_hold = CONFIG["MAX_HOLD_HOURS"]
    atrp = df["atr_pct"].to_numpy()
    n, trades = len(df), []
    i = int(np.searchsorted(t, test_start_ms))
    while i < n:
        if sig[i] == 0:
            i += 1
            continue
        d, entry = int(sig[i]), c[i]
        entry_ms = t[i] + HOUR_MS
        if CONFIG["EXIT_MODE"] == "atr":
            tp_pct, sl_pct = CONFIG["TP_ATR_MULT"] * atrp[i], CONFIG["SL_ATR_MULT"] * atrp[i]
        else:
            tp_pct, sl_pct = CONFIG["TAKE_PROFIT"], CONFIG["STOP_LOSS"]
        tp = entry * (1 + d * tp_pct)
        sl = entry * (1 - d * sl_pct)
        mfe = mae = 0.0
        result, exit_px, exit_ms, note, j = "未平倉", c[-1], t[-1] + HOUR_MS, "", n - 1
        for j in range(i + 1, n):
            fav = (h[j] / entry - 1) if d == 1 else (1 - l[j] / entry)
            adv = (1 - l[j] / entry) if d == 1 else (h[j] / entry - 1)
            hit_tp = h[j] >= tp if d == 1 else l[j] <= tp
            hit_sl = l[j] <= sl if d == 1 else h[j] >= sl
            if hit_tp and hit_sl:
                if CONFIG["RESOLVE_SAME_BAR_WITH_5M"]:
                    result, exit_ms, note = resolve_with_5m(symbol, t[j], d, tp, sl)
                else:
                    result, exit_ms, note = "止損", t[j] + HOUR_MS, "同1h觸發→保守計止損"
            elif hit_sl:
                result, exit_ms = "止損", t[j] + HOUR_MS
            elif hit_tp:
                result, exit_ms = "止盈", t[j] + HOUR_MS
            if result == "止盈":
                exit_px, mfe = tp, max(mfe, tp_pct)
                break
            if result == "止損":
                # 跳空穿越止損 → 以開盤價成交（較差價）
                gap = (o[j] < sl) if d == 1 else (o[j] > sl)
                exit_px = o[j] if gap and o[j] > 0 else sl
                if gap:
                    note = (note + "；" if note else "") + "開盤跳空穿越止損"
                mae = max(mae, sl_pct)
                break
            mfe, mae = max(mfe, fav), max(mae, adv)
            if max_hold and (t[j] + HOUR_MS - entry_ms) >= max_hold * HOUR_MS:
                result, exit_px, exit_ms, note = "時間出場", c[j], t[j] + HOUR_MS, f"持倉達{max_hold}h"
                break
        row = df.iloc[i]
        trades.append({
            "symbol": symbol, "dir": "做多" if d == 1 else "做空",
            "entry_time": entry_ms, "entry": entry, "tp": tp, "sl": sl,
            "exit_time": exit_ms, "exit": exit_px, "result": result,
            "note": note if result != "未平倉" else "回測結束仍持倉（以最後收盤估值）",
            "ret1h": row["ret1h"], "atr_pct": row["atr_pct"], "ma_dev": row["ma_dev"],
            "obv_chg": row["obv_chg"], "liq24": row["liq24"], "mfe": mfe, "mae": mae,
            "vol_ratio": row["vol_ratio"],
            "close_str": row["close_pos"] if d == 1 else 1 - row["close_pos"],
            "htf_al": d * row["htf_dev"], "ret24_al": d * row["ret24"],
            "btc1h_al": d * row["btc_ret1h"], "btcma_al": d * row["btc_ma_dev"],
            "tp_pct": tp_pct, "sl_pct": sl_pct,
        })
        if result == "未平倉":
            break
        i = j + CONFIG["COOLDOWN_BARS"]   # 0 = 出場那根收盤若又有訊號可立即再進場
    return trades


# ============================== Excel 輸出 ==============================
FONT = Font(name="Arial", size=10)
HFONT = Font(name="Arial", size=10, bold=True, color="FFFFFF")
HFILL = PatternFill("solid", start_color="305496")
BLUE = Font(name="Arial", size=10, color="0000FF")
GREEN_FILL = PatternFill("solid", start_color="C6EFCE")
RED_FILL = PatternFill("solid", start_color="FFC7CE")
YELLOW_FILL = PatternFill("solid", start_color="FFEB9C")


def to_dt(ms):
    return datetime.fromtimestamp(ms / 1000, TPE).replace(tzinfo=None)


def style_header(ws, row, ncol):
    for cidx in range(1, ncol + 1):
        cell = ws.cell(row=row, column=cidx)
        cell.font, cell.fill = HFONT, HFILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def set_widths(ws, widths):
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w


def write_excel(trades, scan_rows, path, period_txt, extra=None):
    wb = Workbook()
    ws_ov = wb.active
    ws_ov.title = "總覽"
    ws_p = wb.create_sheet("參數")
    ws_t = wb.create_sheet("交易明細")
    ws_s = wb.create_sheet("幣種統計")
    ws_scan = wb.create_sheet("掃描清單")
    ws_eq = wb.create_sheet("資金模擬", 1)
    ws_ev = wb.create_sheet("資金明細", 2)

    # ---------- 參數 ----------
    params = [
        ("市場", CONFIG["MARKET_TYPE"], "PERP=永續合約 / SPOT=現貨"),
        ("回測區間", period_txt, "台北時間"),
        ("K棒週期", "1小時", "訊號於K棒收盤判斷、以收盤價進場"),
        ("止盈", CONFIG["TAKE_PROFIT"], "同方向" if CONFIG["EXIT_MODE"] == "fixed"
         else f"（未使用）改依ATR：止盈 {CONFIG['TP_ATR_MULT']}×ATR"),
        ("止損", CONFIG["STOP_LOSS"], "反方向" if CONFIG["EXIT_MODE"] == "fixed"
         else f"（未使用）改依ATR：止損 {CONFIG['SL_ATR_MULT']}×ATR"),
        ("單邊手續費", CONFIG["FEE_RATE"], "進場、出場各扣一次（可直接改此格，報酬率會重算）"),
        ("流動性門檻 (USDT)", CONFIG["LIQ_MIN_USD"],
         "近24h成交額加總" if CONFIG["LIQ_MODE"] == "sum24" else "近24h平均每小時成交額"),
        ("動能門檻", CONFIG["MOM_THRESHOLD"] if CONFIG["MOM_MODE"] == "fixed" else f"{CONFIG['MOM_ATR_MULT']}×ATR",
         "收盤 vs 1小時前收盤"),
        ("ATR 門檻", CONFIG["ATR_MIN_PCT"], f"ATR({CONFIG['ATR_PERIOD']}) ÷ 收盤"
         + (f"，上限 {CONFIG['ATR_MAX_PCT']:.1%}" if CONFIG["ATR_MAX_PCT"] is not None else "")),
        ("均線", f"MA{CONFIG['MA_PERIOD']}", "做多站上 / 做空跌破"),
        ("OBV 比較", f"{CONFIG['OBV_LOOKBACK']}小時前", "做多 OBV 上升 / 做空 OBV 下降"),
        ("價格上限", CONFIG["MAX_PRICE"] if CONFIG["MAX_PRICE"] is not None else "不限", "訊號當下收盤價須低於此值"),
        ("時間停損", CONFIG["MAX_HOLD_HOURS"] or "無", "小時"),
        ("同K棒雙觸發", "/".join(CONFIG["RESOLVE_INTERVALS"]) + " 依序判斷"
         if CONFIG["RESOLVE_SAME_BAR_WITH_5M"] else "保守計止損", ""),
        ("盈虧平衡勝率", f"=IFERROR(AVERAGE(交易明細!$AC$2:$AC${max(len(trades), 1) + 1}),(B6+2*B7)/(B5+B6))",
         "含手續費，各筆平均；勝率低於此值 = 長期虧損"),
        ("MA20乖離上限", CONFIG["MAX_MA_DEV"] if CONFIG["MAX_MA_DEV"] is not None else "不限", "超過不進場"),
        ("1h漲跌幅上限", CONFIG["MAX_MOM"] if CONFIG["MAX_MOM"] is not None else "不限", "超過不進場"),
        ("出場後冷卻", CONFIG["COOLDOWN_BARS"], "K棒數"),
        ("量比下限", CONFIG["MIN_VOL_RATIO"] if CONFIG["MIN_VOL_RATIO"] is not None else "不限", "訊號K量 ÷ 前20根均量"),
        ("收盤強度下限", CONFIG["MIN_CLOSE_POS"] if CONFIG["MIN_CLOSE_POS"] is not None else "不限", "1=收在順勢端極值"),
        ("長週期趨勢過濾", f"MA{CONFIG['HTF_MA_PERIOD']}" if CONFIG["REQUIRE_HTF_TREND"] else "關閉", "1h K棒"),
        ("BTC 趨勢過濾", "開啟" if CONFIG["BTC_TREND_FILTER"] else "關閉", "BTC 收盤 vs 自身 MA20"),
        ("24h盤整排除", CONFIG["EXCLUDE_FLAT_24H"] if CONFIG["EXCLUDE_FLAT_24H"] is not None else "關閉", "|24h漲跌| 低於此值不進場"),
        ("BTC同向乖離上限", CONFIG["BTC_MAX_ALIGNED_DEV"] if CONFIG["BTC_MAX_ALIGNED_DEV"] is not None else "關閉", "BTC 已同向偏離MA20超過此值不進場"),
    ]
    ws_p.append(["項目", "數值", "說明"])
    style_header(ws_p, 1, 3)
    for r in params:
        ws_p.append(list(r))
    for row in ws_p.iter_rows(min_row=2):
        for cell in row:
            cell.font = FONT
    for addr in ("B5", "B6", "B7"):
        ws_p[addr].font = BLUE
        ws_p[addr].fill = YELLOW_FILL
    for addr in ("B5", "B6", "B9", "B10", "B16", "B17", "B18"):
        ws_p[addr].number_format = "0.00%"
    ws_p["B7"].number_format = "0.000%"
    ws_p["B8"].number_format = "#,##0"
    set_widths(ws_p, [20, 28, 50])

    # ---------- 交易明細 ----------
    headers = ["編號", "交易對", "方向", "進場時間", "進場價", "止盈價", "止損價", "出場時間",
               "出場價", "結果", "持倉小時", "報酬率(含費)", "判定說明", "1h漲跌幅", "ATR%",
               "價格vsMA20", "OBV 20h變化", "24h成交額(USDT)", "最大有利(MFE)", "最大不利(MAE)",
               "量比", "收盤強度", "長週期趨勢(順勢)", "24h漲跌(順勢)", "BTC 1h漲跌(順勢)", "BTC vs MA20(順勢)",
               "止盈%", "止損%", "盈虧平衡勝率"]
    ws_t.append(headers)
    style_header(ws_t, 1, len(headers))
    for k, tr in enumerate(trades, start=1):
        r = k + 1
        ws_t.append([
            k, tr["symbol"], tr["dir"], to_dt(tr["entry_time"]), tr["entry"], tr["tp"], tr["sl"],
            to_dt(tr["exit_time"]), tr["exit"], tr["result"],
            f"=(H{r}-D{r})*24",
            f'=IF(C{r}="做多",I{r}/E{r}-1,1-I{r}/E{r})-2*參數!$B$7',
            tr["note"], tr["ret1h"], tr["atr_pct"], tr["ma_dev"], tr["obv_chg"], tr["liq24"],
            tr["mfe"], tr["mae"],
            *[None if pd.isna(tr[k]) else float(tr[k])
              for k in ("vol_ratio", "close_str", "htf_al", "ret24_al", "btc1h_al", "btcma_al")],
            tr["tp_pct"], tr["sl_pct"], f"=(AB{r}+2*參數!$B$7)/(AA{r}+AB{r})",
        ])
    last = max(len(trades), 1) + 1
    fmts = {"D": "yyyy-mm-dd hh:mm", "H": "yyyy-mm-dd hh:mm", "E": "0.00000000", "F": "0.00000000",
            "G": "0.00000000", "I": "0.00000000", "K": "0.0", "L": "0.00%", "N": "0.00%", "O": "0.00%",
            "P": "0.00%", "Q": "#,##0", "R": "#,##0", "S": "0.00%", "T": "0.00%",
            "U": "0.00", "V": "0.00", "W": "0.00%", "X": "0.00%", "Y": "0.00%", "Z": "0.00%",
            "AA": "0.00%", "AB": "0.00%", "AC": "0.00%"}
    for row in ws_t.iter_rows(min_row=2, max_row=last):
        for cell in row:
            cell.font = FONT
            if cell.column_letter in fmts:
                cell.number_format = fmts[cell.column_letter]
    ws_t.conditional_formatting.add(f"J2:J{last}", CellIsRule(operator="equal", formula=['"止盈"'], fill=GREEN_FILL))
    ws_t.conditional_formatting.add(f"J2:J{last}", CellIsRule(operator="equal", formula=['"止損"'], fill=RED_FILL))
    ws_t.conditional_formatting.add(f"J2:J{last}", CellIsRule(operator="equal", formula=['"未平倉"'], fill=YELLOW_FILL))
    ws_t.freeze_panes = "C2"
    ws_t.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{last}"
    set_widths(ws_t, [7, 18, 7, 17, 13, 13, 13, 17, 13, 9, 9, 11, 30, 10, 9, 11, 16, 16, 12, 12,
                     8, 9, 14, 12, 14, 15, 8, 8, 12])

    # 參照範圍
    rng = lambda col: f"交易明細!${col}$2:${col}${last}"
    SYM, DIR, RES, HOLD, RET = rng("B"), rng("C"), rng("J"), rng("K"), rng("L")

    def crit(extra):
        return ",".join(f'{a},{b}' for a, b in extra)

    # ---------- 總覽 ----------
    ws_ov["A1"] = "派網 USDT 交易對 動能策略回測總覽"
    ws_ov["A1"].font = Font(name="Arial", size=14, bold=True)
    ws_ov["A2"] = (f"回測區間：{period_txt}（台北時間）；市場：{CONFIG['MARKET_TYPE']}；"
                   "勝率 = 止盈筆數 ÷ 已平倉筆數；正報酬比例 = 報酬 > 0 的筆數 ÷ 已平倉（含時間出場）")
    ws_ov["A2"].font = FONT
    ov_headers = ["方向", "總筆數", "已平倉", "止盈", "止損", "時間出場", "未平倉", "勝率",
                  "盈虧平衡勝率", "平均報酬/筆", "累計報酬(單利加總)", "平均持倉h",
                  "止盈平均持倉h", "止損平均持倉h", "時間出場平均報酬", "正報酬比例"]
    ws_ov.append([])
    ws_ov.append(ov_headers)
    style_header(ws_ov, 4, len(ov_headers))
    Q = lambda x: f'"{x}"'
    for idx, (label, dcrit) in enumerate([("全部", None), ("做多", Q("做多")), ("做空", Q("做空"))]):
        r = 5 + idx
        base = [(DIR, dcrit)] if dcrit else []
        closed = crit(base + [(RES, Q("<>未平倉"))])
        row = [
            label,
            f"=COUNTIFS({crit(base + [(SYM, Q('<>'))])})",
            f"=D{r}+E{r}+F{r}",
            f"=COUNTIFS({crit(base + [(RES, Q('止盈'))])})",
            f"=COUNTIFS({crit(base + [(RES, Q('止損'))])})",
            f"=COUNTIFS({crit(base + [(RES, Q('時間出場'))])})",
            f"=COUNTIFS({crit(base + [(RES, Q('未平倉'))])})",
            f"=IFERROR(D{r}/C{r},0)",
            f"=IFERROR(AVERAGEIFS({rng('AC')},{closed}),參數!$B$16)",
            f"=IFERROR(AVERAGEIFS({RET},{closed}),0)",
            f"=SUMIFS({RET},{closed})",
            f"=IFERROR(AVERAGEIFS({HOLD},{closed}),0)",
            f"=IFERROR(AVERAGEIFS({HOLD},{crit(base + [(RES, Q('止盈'))])}),0)",
            f"=IFERROR(AVERAGEIFS({HOLD},{crit(base + [(RES, Q('止損'))])}),0)",
            f"=IFERROR(AVERAGEIFS({RET},{crit(base + [(RES, Q('時間出場'))])}),0)",
            f"=IFERROR(COUNTIFS({closed},{RET},{Q('>0')})/C{r},0)",
        ]
        ws_ov.append(row)
    for row in ws_ov.iter_rows(min_row=5, max_row=7):
        for cell in row:
            cell.font = FONT
            if cell.column_letter in "HIJKOP":
                cell.number_format = "0.00%"
            elif cell.column_letter in "LMN":
                cell.number_format = "0.0"
    ws_ov["A9"] = "說明"
    ws_ov["A9"].font = Font(name="Arial", size=10, bold=True)
    notes = [
        "・持倉時間以1小時K棒為精度（觸價當根收盤計），5分K判定的交易精度為5分鐘。",
        "・止盈以止盈價成交；止損若遇開盤跳空穿越，以開盤價成交（較保守）。",
        "・同一根1h同時碰到止盈與止損時，依參數頁設定判斷（見交易明細『判定說明』）。",
        "・只含目前仍上架的交易對，已下架幣種不在內（存活者偏差，實際表現可能較差）。",
        "・可在『參數』頁修改手續費，報酬率欄位會自動重算。",
    ]
    for i, t in enumerate(notes):
        ws_ov.cell(row=10 + i, column=1, value=t).font = FONT
    set_widths(ws_ov, [8, 9, 9, 8, 8, 9, 9, 9, 12, 12, 16, 11, 13, 13, 15, 11])

    # ---------- 幣種統計 ----------
    s_headers = ["交易對", "總筆數", "做多", "做空", "已平倉", "止盈", "止損", "勝率",
                 "平均報酬/筆", "累計報酬", "平均持倉h", "未平倉"]
    ws_s.append(s_headers)
    style_header(ws_s, 1, len(s_headers))
    order = pd.Series([t["symbol"] for t in trades]).value_counts() if trades else pd.Series(dtype=int)
    for k, sym in enumerate(order.index, start=2):
        a = f"$A{k}"
        ws_s.append([
            sym,
            f"=COUNTIFS({SYM},{a})",
            f'=COUNTIFS({SYM},{a},{DIR},"做多")',
            f'=COUNTIFS({SYM},{a},{DIR},"做空")',
            f'=F{k}+G{k}+COUNTIFS({SYM},{a},{RES},"時間出場")',
            f'=COUNTIFS({SYM},{a},{RES},"止盈")',
            f'=COUNTIFS({SYM},{a},{RES},"止損")',
            f"=IFERROR(F{k}/E{k},0)",
            f'=IFERROR(AVERAGEIFS({RET},{SYM},{a},{RES},"<>未平倉"),0)',
            f'=SUMIFS({RET},{SYM},{a},{RES},"<>未平倉")',
            f'=IFERROR(AVERAGEIFS({HOLD},{SYM},{a},{RES},"<>未平倉"),0)',
            f'=COUNTIFS({SYM},{a},{RES},"未平倉")',
        ])
    for row in ws_s.iter_rows(min_row=2):
        for cell in row:
            cell.font = FONT
            if cell.column_letter in "HIJ":
                cell.number_format = "0.00%"
            elif cell.column_letter == "K":
                cell.number_format = "0.0"
    ws_s.freeze_panes = "B2"
    ws_s.auto_filter.ref = f"A1:L{max(len(order), 1) + 1}"
    set_widths(ws_s, [18, 8, 7, 7, 8, 7, 7, 9, 12, 11, 11, 8])

    # ---------- 掃描清單 ----------
    sc_headers = ["交易對", "狀態", "原因/備註", "K棒數", "期間最低價", "期間最高價", "訊號數(多)", "訊號數(空)"]
    ws_scan.append(sc_headers)
    style_header(ws_scan, 1, len(sc_headers))
    for r in scan_rows:
        ws_scan.append([r.get(k) for k in ("symbol", "status", "reason", "bars", "min", "max", "sig_long", "sig_short")])
    for row in ws_scan.iter_rows(min_row=2):
        for cell in row:
            cell.font = FONT
            if cell.column_letter in "EF":
                cell.number_format = "0.00000000"
    ws_scan.freeze_panes = "B2"
    ws_scan.auto_filter.ref = f"A1:H{max(len(scan_rows), 1) + 1}"
    set_widths(ws_scan, [18, 10, 36, 8, 14, 14, 10, 10])

    write_equity(ws_eq, ws_ev, trades)
    if extra is not None:          # 讓 dry run 等外部程式追加分頁
        extra(wb)
    wb.save(path)


def write_equity(ws_eq, ws_ev, trades):
    """依時間順序模擬下單：同一時間點先處理出場、再處理進場。"""
    P = "資金模擬!"
    INIT, PCT, STEP, LEV = f"{P}$B$4", f"{P}$B$5", f"{P}$B$6", f"{P}$B$7"
    MODE, STOPV = f"{P}$B$9", f"{P}$B$10"

    # ---------- 事件排序 ----------
    events = []
    for k, tr in enumerate(trades, start=1):
        events.append((tr["entry_time"], 1, k, "進場"))
        if tr["result"] != "未平倉":
            events.append((tr["exit_time"], 0, k, "出場"))
    events.sort()

    # ---------- 資金明細 ----------
    headers = ["序號", "時間", "種類", "交易編號", "交易對", "方向", "開倉價", "止盈價", "止損價",
               "出場價", "結果", "持倉小時", "下單金額", "狀態", "部位大小", "報酬率(含費)", "損益",
               "剩餘本金", "占用保證金", "可用資金", "持倉數", "下單基準本金", "最高本金", "回撤", "總曝險倍數", "已停止"]
    ws_ev.append(headers)
    style_header(ws_ev, 1, len(headers))
    ws_ev.append([0, None, "起始", None, None, None, None, None, None, None, None, None, None, None,
                  None, None, 0, f"={INIT}", 0, "=R2-S2", 0, f"={INIT}", "=R2", 0, 0, 0])
    entry_row, open_rows = {}, []
    for n, (ms, _, k, kind) in enumerate(events, start=1):
        r, p = n + 2, n + 1
        tr, trow = trades[k - 1], k + 1
        base = [n, to_dt(ms), kind, k, tr["symbol"], tr["dir"], tr["entry"], tr["tp"], tr["sl"]]
        tail = [f"=R{r}-S{r}", None,
                f"=IF({MODE}=2,MAX(R{r},0),MAX(V{p},FLOOR(MAX(R{r},0),{STEP})))",
                f"=MAX(W{p},R{r})", f"=IF(W{r}>0,R{r}/W{r}-1,-1)",
                f"=IF(R{r}>0,S{r}*{LEV}/R{r},0)",
                f"=IF(OR(Z{p}=1,R{r}<={STOPV}),1,0)"]
        if kind == "進場":
            entry_row[k] = r
            if tr["result"] == "未平倉":
                open_rows.append((r, trow))
            row = base + [None, "未平倉" if tr["result"] == "未平倉" else None, None,
                          f"={PCT}*V{p}",
                          f'=IF(Z{p}=1,"已停止",IF(T{p}>=M{r},"已下單","資金不足略過"))',
                          f'=IF(N{r}="已下單",M{r}*{LEV},0)',
                          None, 0, f"=R{p}+Q{r}",
                          f'=S{p}+IF(N{r}="已下單",M{r},0)']
            tail[1] = f'=U{p}+IF(N{r}="已下單",1,0)'
        else:
            e = entry_row[k]
            row = base + [tr["exit"], tr["result"], f"=交易明細!K{trow}",
                          f"=M{e}", f"=N{e}", f"=O{e}", f"=交易明細!L{trow}",
                          f'=IF(N{r}="已下單",O{r}*P{r},0)', f"=R{p}+Q{r}",
                          f'=S{p}-IF(N{r}="已下單",M{r},0)']
            tail[1] = f'=U{p}-IF(N{r}="已下單",1,0)'
        ws_ev.append(row + tail)
    last = len(events) + 2
    fmts = {"B": "yyyy-mm-dd hh:mm", "G": "0.00000000", "H": "0.00000000", "I": "0.00000000",
            "J": "0.00000000", "L": "0.0", "M": "0.00", "O": "0.00", "P": "0.00%", "Q": "0.0000",
            "R": "0.0000", "S": "0.00", "T": "0.00", "V": "0.00", "W": "0.0000", "X": "0.00%", "Y": "0.00"}
    for row in ws_ev.iter_rows(min_row=2, max_row=last):
        for cell in row:
            cell.font = FONT
            if cell.column_letter in fmts:
                cell.number_format = fmts[cell.column_letter]
    ws_ev.conditional_formatting.add(f"K2:K{last}", CellIsRule(operator="equal", formula=['"止盈"'], fill=GREEN_FILL))
    ws_ev.conditional_formatting.add(f"K2:K{last}", CellIsRule(operator="equal", formula=['"止損"'], fill=RED_FILL))
    ws_ev.conditional_formatting.add(f"N2:N{last}", CellIsRule(operator="equal", formula=['"資金不足略過"'], fill=YELLOW_FILL))
    ws_ev.conditional_formatting.add(f"N2:N{last}", CellIsRule(operator="equal", formula=['"已停止"'], fill=PatternFill("solid", start_color="D9D9D9")))
    ws_ev.conditional_formatting.add(f"Q2:Q{last}", CellIsRule(operator="lessThan", formula=["0"], font=Font(name="Arial", size=10, color="C00000")))
    ws_ev.freeze_panes = "D2"
    ws_ev.auto_filter.ref = f"A1:Z{last}"
    set_widths(ws_ev, [7, 17, 7, 8, 18, 7, 12, 12, 12, 12, 7, 9, 9, 12, 9, 11, 10, 11, 10, 10, 7, 11, 11, 9, 11, 8])

    # ---------- 資金模擬（參數 + 摘要 + 曲線） ----------
    E = "資金明細!"
    rng = lambda col: f"{E}${col}$2:${col}${last}"
    ws_eq["A1"] = "資金模擬：依訊號時間順序下單"
    ws_eq["A1"].font = Font(name="Arial", size=14, bold=True)
    ws_eq["A2"] = "藍字黃底的參數可直接修改，整份模擬會自動重算"
    ws_eq["A2"].font = FONT
    ws_eq.append([])
    params = [("初始本金", CONFIG["EQUITY_INITIAL"], "0.00"),
              ("下單比例", CONFIG["EQUITY_ORDER_PCT"], "0.00%"),
              ("本金更新級距", CONFIG["EQUITY_STEP"], "0"),
              ("槓桿倍數", CONFIG["EQUITY_LEVERAGE"], "0.0"),
              ("單邊手續費", "=參數!B7", "0.000%"),
              ("下單基準模式", CONFIG["EQUITY_SIZING_MODE"], "0"),
              ("停止交易門檻", CONFIG["EQUITY_STOP_BELOW"], "0.00")]
    ws_eq["A3"], ws_eq["D3"] = "參數", "績效摘要"
    for addr in ("A3", "D3"):
        ws_eq[addr].font = HFONT
        ws_eq[addr].fill = HFILL
    ws_eq["A14"] = "下單基準模式：1=不回滾（原規則） 2=即時本金"
    ws_eq["A15"] = "停止交易門檻：本金 ≤ 此值即停止開新倉"
    ws_eq["A12"] = "每筆止損約虧本金"
    ws_eq["B12"] = f"=B5*B7*IFERROR(AVERAGE(交易明細!$AB$2:$AB${max(len(trades), 1) + 1}),參數!B6)"
    ws_eq["B12"].number_format = "0.00%"
    ws_eq["A13"] = "（以全倉、本金=基準本金計；模式1在本金回落時實際比例會更高）"
    for addr in ("A14", "A15", "A12", "B12", "A13"):
        ws_eq[addr].font = FONT
    for i, (lab, val, fmt) in enumerate(params, start=4):
        ws_eq.cell(row=i, column=1, value=lab).font = FONT
        c = ws_eq.cell(row=i, column=2, value=val)
        c.number_format = fmt
        editable = i in (4, 5, 6, 7, 9, 10)
        c.font = BLUE if editable else FONT
        if editable:
            c.fill = YELLOW_FILL
    closed_ok = f'{rng("C")},"出場",{rng("N")},"已下單"'
    float_pnl = "+".join(f'IF({E}N{r}="已下單",{E}O{r}*交易明細!L{trow},0)' for r, trow in open_rows) or "0"
    summary = [
        ("最終本金（已實現）", f"={E}R{last}", "0.0000"),
        ("總報酬率", "=E4/B4-1", "0.00%"),
        ("最高本金", f"=MAX({rng('R')})", "0.0000"),
        ("最低本金", f"=MIN({rng('R')})", "0.0000"),
        ("最大回撤", f"=MIN({rng('X')})", "0.00%"),
        ("已下單筆數", f'=COUNTIFS({rng("C")},"進場",{rng("N")},"已下單")', "0"),
        ("資金不足略過", f'=COUNTIFS({rng("C")},"進場",{rng("N")},"資金不足略過")', "0"),
        ("止盈筆數", f'=COUNTIFS({closed_ok},{rng("K")},"止盈")', "0"),
        ("止損筆數", f'=COUNTIFS({closed_ok},{rng("K")},"止損")', "0"),
        ("勝率", f"=IFERROR(E11/COUNTIFS({closed_ok}),0)", "0.00%"),
        ("最大同時持倉", f"=MAX({rng('U')})", "0"),
        ("最大占用保證金", f"=MAX({rng('S')})", "0.00"),
        ("期末下單金額", f"=B5*{E}V{last}", "0.00"),
        ("未平倉浮動損益（未計入）", f"={float_pnl}", "0.0000"),
        ("最大總曝險倍數", f"=MAX({rng('Y')})", "0.00"),
        ("當時全部止損約虧本金", "=E18*參數!B6", "0.00%"),
        ("停止交易時間", f'=IFERROR(INDEX({rng("B")},MATCH(1,{rng("Z")},0)),"未觸發")', "yyyy-mm-dd hh:mm"),
        ("停止後略過筆數", f'=COUNTIFS({rng("C")},"進場",{rng("N")},"已停止")', "0"),
    ]
    for i, (lab, val, fmt) in enumerate(summary, start=4):
        ws_eq.cell(row=i, column=4, value=lab).font = FONT
        c = ws_eq.cell(row=i, column=5, value=val)
        c.number_format, c.font = fmt, FONT
    notes = [
        "規則說明",
        "・下單金額 = 下單基準本金 × 下單比例。模式1：基準本金 = 本金曾達到的最高『級距倍數』（不回滾）；模式2：基準本金 = 當下本金。",
        "・本金 ≤ 停止交易門檻後不再開新倉（已持有的單照常結算），避免模擬出現本金歸零後又『復活』的情況。",
        "・剩餘本金 = 已實現本金（只在出場時結算）；可用資金 = 剩餘本金 − 占用保證金，不足下單金額時略過該訊號。",
        "・同一時間點先處理出場再處理進場；報酬率已含進出場手續費。",
        "・以全倉模式計算：總曝險倍數 = 所有持倉部位合計 ÷ 本金；未模擬持倉中的浮動虧損與維持保證金。",
        "・回測結束仍持倉的交易不計入最終本金，浮動損益另列。",
    ]
    for i, t in enumerate(notes, start=23):
        ws_eq.cell(row=i, column=1, value=t).font = Font(name="Arial", size=10, bold=(i == 23))
    set_widths(ws_eq, [16, 12, 3, 24, 16])

    chart = LineChart()
    chart.title = "剩餘本金（依事件順序）"
    chart.y_axis.title = "本金"
    chart.x_axis.title = "事件序號"
    chart.legend = None
    chart.height, chart.width = 9, 22
    chart.add_data(Reference(ws_ev, min_col=18, min_row=1, max_row=last), titles_from_data=True)
    chart.series[0].smooth = False
    chart.series[0].graphicalProperties.line.width = 12000
    ws_eq.add_chart(chart, "G3")


# ============================== 主程式 ==============================
def classify(sym_info):
    symbol = sym_info["symbol"]
    base = (sym_info.get("baseCurrency") or symbol.split("_")[0]).upper()
    quote = (sym_info.get("quoteCurrency") or symbol.split("_")[1]).upper()
    if quote != "USDT":
        return base, None               # 非 USDT 對，直接忽略不列出
    if sym_info.get("enable") is False:
        return base, "交易對已停用"
    if base in PEGGED or STABLE_RE.match(base):
        return base, "強掛勾/穩定幣/包裝幣"
    if LEVERAGED_RE.match(base):
        return base, "槓桿代幣"
    if is_stock_token(base):
        return base, "股票/ETF/商品代幣（依名稱推定，可調整 CRYPTO_X_WHITELIST）"
    if base in {b.upper() for b in CONFIG["EXTRA_EXCLUDE"]}:
        return base, "手動排除"
    return base, ""


def main():
    now_ms = int(time.time() * 1000) // HOUR_MS * HOUR_MS
    test_start = now_ms - CONFIG["LOOKBACK_DAYS"] * 24 * HOUR_MS
    fetch_start = test_start - CONFIG["WARMUP_BARS"] * HOUR_MS
    period_txt = f"{to_dt(test_start):%Y-%m-%d %H:%M} ~ {to_dt(now_ms):%Y-%m-%d %H:%M}"
    print(f"回測區間：{period_txt}  市場：{CONFIG['MARKET_TYPE']}")

    symbols = get_symbols()
    only = {s.upper() for s in CONFIG["ONLY_SYMBOLS"]}
    scan_rows, todo = [], []
    for s in symbols:
        base, reason = classify(s)
        if reason is None or (only and base not in only):
            continue
        if reason:
            scan_rows.append({"symbol": s["symbol"], "status": "排除", "reason": reason})
        else:
            todo.append(s["symbol"])
    print(f"USDT 交易對：{len(todo) + len(scan_rows)}，排除 {len(scan_rows)}，待掃描 {len(todo)}")

    btc = None
    btc_sym = "BTC_USDT_PERP" if CONFIG["MARKET_TYPE"] == "PERP" else "BTC_USDT"
    try:
        b = load_hourly(btc_sym, fetch_start, now_ms)
        bc = b["close"]
        btc = pd.DataFrame({"time": b["time"], "btc_ret1h": bc / bc.shift(1) - 1,
                            "btc_ma_dev": bc / bc.rolling(20).mean() - 1})
    except Exception as e:
        print(f"[警告] 取得 {btc_sym} 失敗，BTC 欄位留空、BTC 過濾停用：{e}")
        CONFIG["BTC_TREND_FILTER"] = False

    all_trades = []
    for k, sym in enumerate(sorted(todo), 1):
        try:
            df = load_hourly(sym, fetch_start, now_ms)
            if len(df) < 60:
                scan_rows.append({"symbol": sym, "status": "略過", "reason": "K棒不足（新上架？）", "bars": len(df)})
                continue
            df = add_indicators(df, btc)
            win = df[df["time"] >= test_start]
            tr = backtest(sym, df, test_start)
            all_trades.extend(tr)
            cheap = CONFIG["MAX_PRICE"] is not None and win["close"].min() >= CONFIG["MAX_PRICE"]
            scan_rows.append({
                "symbol": sym, "status": "已掃描",
                "reason": f"{len(tr)} 筆交易" + ("（期間價格皆 ≥ 價格上限）" if cheap else ""),
                "bars": len(win), "min": float(win["close"].min()), "max": float(win["close"].max()),
                "sig_long": int((win["signal"] == 1).sum()), "sig_short": int((win["signal"] == -1).sum()),
            })
            print(f"[{k}/{len(todo)}] {sym}: {len(tr)} 筆")
        except Exception as e:
            scan_rows.append({"symbol": sym, "status": "失敗", "reason": str(e)[:200]})
            print(f"[{k}/{len(todo)}] {sym}: 失敗 {e}")

    all_trades.sort(key=lambda x: (x["entry_time"], x["symbol"]))
    scan_rows.sort(key=lambda x: ({"已掃描": 0, "略過": 1, "失敗": 2, "排除": 3}[x["status"]], x["symbol"]))
    out = CONFIG["OUTPUT"].format(ts=datetime.now(TPE).strftime("%Y%m%d_%H%M"))
    write_excel(all_trades, scan_rows, out, period_txt)

    closed = [t for t in all_trades if t["result"] in ("止盈", "止損", "時間出場")]
    wins = sum(t["result"] == "止盈" for t in closed)
    print(f"\n完成：{len(all_trades)} 筆（已平倉 {len(closed)}，止盈 {wins}，"
          f"勝率 {wins / len(closed):.1%}）" if closed else "\n完成：沒有任何交易")
    print(f"輸出：{os.path.abspath(out)}")


if __name__ == "__main__":
    main()
