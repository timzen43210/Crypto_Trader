#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略2 — 反轉（做空）：大戶提款
==============================
論點：大戶在短時間內持續買進、把價格拉起來，量買到一個程度之後開始快速出貨。
      整個過程不超過 PUMP_WINDOW（預設 8）小時。

觸發（1小時K收盤判斷）：
  拉抬       近 8 小時漲幅 ≥ MIN_PUMP_RET
  啟動前平靜 更早之前（8~24小時）漲幅 ≤ MAX_PRIOR_RET  ← 確保是「快速」拉抬
  買到一個量 近 8 小時量能 ÷ 之前平均 ≥ MIN_VOL_SURGE
  買盤一面倒 近 8 小時 OBV淨變化 ÷ 總量 ≥ MIN_BUY_RATIO
  開始出貨   突破新高，但該根量 ÷ 近8小時最大量 ≤ MAX_PEAK_VOL_RATIO（量價背離）
  過熱       收盤 vs MA20 乖離 ≥ MIN_MA_DEV、RSI ≥ MIN_RSI
  共同       流動性、ATR 範圍、價格上限

進場：ENTRY_TIMING="breakdown" 時，觸發後等到收盤跌破前一根最低價才進場。
出場：固定比例（預設止盈 3% / 止損 5%，方向為做空）或依 ATR 倍數。

用法：與 pionex_backtest.py 放在同一資料夾，執行 python pionex_reversal.py
      （會共用同一份 K 棒快取，不必重新下載）
"""
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd

import pionex_backtest as pb

# ============================== 反轉策略參數 ==============================
REV = {
    # ---- 型態 ----
    # "distribution" = 大戶提款：快速拉抬 → 出貨 → 做空（原始設計）
    # "accumulation" = 大戶進貨：快速下殺 → 吸貨 → 做多（上面的鏡像）
    "MODE": "distribution",

    # ---- 階段一：大戶買進（須在 PUMP_WINDOW 小時內完成）----
    "PUMP_WINDOW": 8,            # 觀察窗（小時）
    "MIN_PUMP_RET": 0.08,        # 近 N 小時漲幅下限（None=不限）
    "MAX_PRIOR_RET": 0.05,       # 更早之前（N ~ 3N 小時）的漲幅上限 → 確保是突然啟動
    "MIN_VOL_SURGE": 1.5,        # 近 N 小時量能 ÷ 更早的平均量能（None=不限）
    "MIN_BUY_RATIO": 0.20,       # 近 N 小時 OBV 淨變化 ÷ 總成交量（None=不限）
    "BASE_WINDOW": 72,           # 量能基準期間（小時）

    # ---- 階段二：開始出貨 ----
    "BREAKOUT_HOURS": 24,        # 收盤須突破前 N 小時最高價
    "MAX_PEAK_VOL_RATIO": 0.85,  # 量價背離：突破當根量 ÷ 近 PUMP_WINDOW 小時最大量（None=不限）
    "MIN_PEAK_VOL_RATIO": None,  # 反過來要求「量仍在放大」時用（例 0.85）；做第一階段順勢時才需要
    "MIN_MA_DEV": 0.05,          # 乖離下限（過熱，None=不限）
    "MIN_RSI": None,
    "MAX_RSI": None,
    "MAX_CLOSE_POS": None,       # 例 0.5 = 只在收盤落在K棒下半部（上影線）時觸發
    "MAX_BTC_DEV": None,         # 例 0.02 = BTC 太強時不做空

    # ---- 舊版條件（資料顯示爆量反而不利做空，預設關閉）----
    "VOL_SPIKE": None,
    "MIN_RUN_24H": None,
    "MIN_RUN_BARS": None,

    # ---- 進場時機與方向 ----
    "ENTRY_TIMING": "breakdown", # "breakout"=突破當根 / "breakdown"=等收盤跌破前一根最低價
    "BREAKDOWN_WINDOW": 6,
    "RECHECK_ATR_AT_ENTRY": True,  # 進場當根再檢查一次 ATR 是否仍在範圍內（觸發後 ATR 會漂移）
    "DIRECTION": None,           # None = 依 MODE 自動（distribution→short、accumulation→long）
}

# ============================== 共用參數 ==============================
CONFIG = {
    "MARKET_TYPE": "PERP",
    "LOOKBACK_DAYS": 365,
    "WARMUP_BARS": 150,

    "EXIT_MODE": "fixed",      # "fixed" 或 "atr"
    "TAKE_PROFIT": 0.03,       # 做空：價格下跌 3% 止盈
    "STOP_LOSS": 0.05,         # 做空：價格上漲 5% 止損
    "TP_ATR_MULT": 1.2,        # EXIT_MODE="atr" 時使用
    "SL_ATR_MULT": 0.8,
    "FEE_RATE": 0.0005,

    "LIQ_MIN_USD": 50_000,
    "LIQ_MODE": "avg_hourly",
    "ATR_PERIOD": 14,
    "ATR_MIN_PCT": 0.02,
    "ATR_MAX_PCT": 0.06,
    "MA_PERIOD": 20,
    "OBV_LOOKBACK": 20,
    "HTF_MA_PERIOD": 100,
    "MAX_PRICE": 1.0,          # 低價幣（本策略的核心假設）
    "MAX_HOLD_HOURS": None,
    "COOLDOWN_BARS": 1,
    "RESOLVE_SAME_BAR_WITH_5M": True,
    "RESOLVE_INTERVALS": ["5M", "15M", "30M"],

    # 策略1的過濾條件在此策略不使用，設為關閉（沿用同一套程式需要這些key）
    "MOM_MODE": "fixed", "MOM_THRESHOLD": 0.02, "MOM_ATR_MULT": 0.45,
    "MAX_MA_DEV": None, "MAX_MOM": None, "MIN_VOL_RATIO": None, "MIN_CLOSE_POS": None,
    "EXCLUDE_FLAT_24H": None, "BTC_MAX_ALIGNED_DEV": None,
    "REQUIRE_HTF_TREND": False, "BTC_TREND_FILTER": False,

    "EQUITY_INITIAL": 100, "EQUITY_ORDER_PCT": 0.02, "EQUITY_STEP": 50,
    "EQUITY_LEVERAGE": 50, "EQUITY_SIZING_MODE": 2, "EQUITY_STOP_BELOW": 0,

    "REQUEST_SLEEP": 0.12,
    "CACHE_DIR": "pionex_cache",     # 與策略1共用快取
    "CA_BUNDLE": None,
    "OUTPUT": "reversal_backtest_{ts}.xlsx",
    "EXTRA_EXCLUDE": [],
    "ONLY_SYMBOLS": [],
}

HOUR = pb.HOUR_MS
DIAG = [("觸發時間", "trig_time", 17, "yyyy-mm-dd hh:mm"), ("等待K棒數", "wait_bars", 10, "0"),
        ("拉抬漲幅", "pump_ret", 10, "0.00%"), ("啟動前漲幅", "prior_ret", 11, "0.00%"),
        ("量能倍數", "vol_surge", 10, "0.00"), ("買盤佔比", "buy_ratio", 10, "0.00"),
        ("量價背離比", "peak_vol_ratio", 11, "0.00"), ("突破幅度", "brk", 10, "0.00%"),
        ("MA20乖離", "ma_dev_raw", 10, "0.00%"), ("RSI(14)", "rsi", 9, "0.0"),
        ("收盤位置", "close_pos_raw", 9, "0.00"), ("24h漲幅", "ret24", 10, "0.00%"),
        ("BTC乖離", "btc_dev_raw", 10, "0.00%")]
TRIG_COLS = ["pump_ret", "prior_ret", "vol_surge", "buy_ratio", "peak_vol_ratio",
             "brk", "ma_dev", "rsi", "close_pos", "ret24", "btc_ma_dev"]


def rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def add_indicators(df, btc=None):
    """取代策略1的訊號邏輯；欄位名稱保持相容，讓 pb.backtest 可直接使用。"""
    C = pb.CONFIG
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["ret1h"] = c / c.shift(1) - 1
    prev = c.shift(1)
    tr = pd.concat([h - l, (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / C["ATR_PERIOD"], adjust=False, min_periods=C["ATR_PERIOD"]).mean()
    df["atr_pct"] = df["atr"] / c
    df["ma"] = c.rolling(C["MA_PERIOD"]).mean()
    df["ma_dev"] = c / df["ma"] - 1
    obv = (np.sign(c.diff()).fillna(0) * v).cumsum()
    df["obv_chg"] = obv - obv.shift(C["OBV_LOOKBACK"])
    turnover = df["amount"] if "amount" in df.columns and df["amount"].sum() > 0 else c * v
    df["liq24"] = turnover.rolling(24).sum()
    liq_val = df["liq24"] if C["LIQ_MODE"] == "sum24" else df["liq24"] / 24

    df["vol_ratio"] = v / v.shift(1).rolling(20).mean()
    rng = (h - l).replace(0, np.nan)
    df["close_pos"] = (c - l) / rng
    df["htf_dev"] = c / c.rolling(C["HTF_MA_PERIOD"]).mean() - 1
    df["ret24"] = c / c.shift(24) - 1
    df["ret3"] = c / c.shift(3) - 1
    df["rsi"] = rsi(c, 14)
    if REV["MODE"] == "distribution":
        ref = h.shift(1).rolling(REV["BREAKOUT_HOURS"]).max()
    else:
        ref = l.shift(1).rolling(REV["BREAKOUT_HOURS"]).min()
    df["prior_high"] = ref
    df["brk"] = c / ref - 1
    up = (c > c.shift(1)).astype(int)
    df["run_bars"] = up * (up.groupby((up == 0).cumsum()).cumcount() + 1)

    W, B = REV["PUMP_WINDOW"], REV["BASE_WINDOW"]
    df["pump_ret"] = c / c.shift(W) - 1                       # 拉抬幅度
    df["prior_ret"] = c.shift(W) / c.shift(W * 3) - 1         # 啟動前是否平靜
    vol_sum = v.rolling(W).sum()
    df["vol_surge"] = vol_sum / (v.shift(W).rolling(B).mean() * W)
    df["buy_ratio"] = (obv - obv.shift(W)) / vol_sum.replace(0, np.nan)
    df["peak_vol_ratio"] = v / v.rolling(W).max()             # 突破當根是否量縮
    if btc is not None:
        df = df.merge(btc, on="time", how="left")
    else:
        df["btc_ret1h"] = np.nan
        df["btc_ma_dev"] = np.nan

    cond = (liq_val > C["LIQ_MIN_USD"]) & (df["atr_pct"] > C["ATR_MIN_PCT"])
    if C["ATR_MAX_PCT"] is not None:
        cond &= df["atr_pct"] <= C["ATR_MAX_PCT"]
    if C["MAX_PRICE"] is not None:
        cond &= c < C["MAX_PRICE"]
    # k = +1：大戶提款（拉抬後出貨）；k = -1：大戶進貨（下殺後吸貨），所有方向性條件鏡像
    k = 1 if REV["MODE"] == "distribution" else -1
    cond &= k * df["brk"] > 0                                    # 突破新高 / 跌破新低
    if REV["MIN_PUMP_RET"] is not None:                          # 快速拉抬 / 快速下殺
        cond &= k * df["pump_ret"] >= REV["MIN_PUMP_RET"]
    if REV["MIN_VOL_SURGE"] is not None:                         # 量能放大（不分方向）
        cond &= df["vol_surge"] >= REV["MIN_VOL_SURGE"]
    if REV["MIN_BUY_RATIO"] is not None:                         # 買盤 / 賣盤一面倒
        cond &= k * df["buy_ratio"] >= REV["MIN_BUY_RATIO"]
    if REV["MAX_PEAK_VOL_RATIO"] is not None:                    # 量價背離＝動能衰竭
        cond &= df["peak_vol_ratio"] <= REV["MAX_PEAK_VOL_RATIO"]
    if REV["MIN_PEAK_VOL_RATIO"] is not None:                    # 反過來：量仍在放大
        cond &= df["peak_vol_ratio"] >= REV["MIN_PEAK_VOL_RATIO"]
    if REV["MAX_PRIOR_RET"] is not None:                         # 啟動前平靜
        cond &= k * df["prior_ret"] <= REV["MAX_PRIOR_RET"]
    if REV["MIN_MA_DEV"] is not None:                            # 過熱 / 過冷
        cond &= k * df["ma_dev"] >= REV["MIN_MA_DEV"]
    if REV["VOL_SPIKE"] is not None:
        cond &= df["vol_ratio"] >= REV["VOL_SPIKE"]
    if REV["MIN_RUN_24H"] is not None:
        cond &= k * df["ret24"] >= REV["MIN_RUN_24H"]
    rsi_dir = df["rsi"] if k == 1 else 100 - df["rsi"]
    if REV["MIN_RSI"] is not None:
        cond &= rsi_dir >= REV["MIN_RSI"]
    if REV["MAX_RSI"] is not None:
        cond &= rsi_dir <= REV["MAX_RSI"]
    pos_dir = df["close_pos"] if k == 1 else 1 - df["close_pos"]
    if REV["MAX_CLOSE_POS"] is not None:
        cond &= pos_dir <= REV["MAX_CLOSE_POS"]
    if REV["MIN_RUN_BARS"] is not None:
        cond &= df["run_bars"] >= REV["MIN_RUN_BARS"]
    if REV["MAX_BTC_DEV"] is not None:
        cond &= ~(k * df["btc_ma_dev"] > REV["MAX_BTC_DEV"])
    trigger = cond.fillna(False).to_numpy()
    src = np.where(trigger, np.arange(len(df)), -1)     # 每個進場點對應的「觸發K棒」
    if REV["ENTRY_TIMING"] == "breakdown":
        # 反轉確認：提款→收盤跌破前一根最低價；進貨→收盤突破前一根最高價
        broke = ((c < l.shift(1)) if k == 1 else (c > h.shift(1))).to_numpy().copy()
        if REV["RECHECK_ATR_AT_ENTRY"]:                # 進場當根 ATR 仍須在範圍內
            ok = (df["atr_pct"] > C["ATR_MIN_PCT"])
            if C["ATR_MAX_PCT"] is not None:
                ok &= df["atr_pct"] <= C["ATR_MAX_PCT"]
            broke &= ok.fillna(False).to_numpy()
        fire = np.zeros(len(df), dtype=bool)
        src = np.full(len(df), -1)
        left, at = 0, -1
        for i in range(len(df)):
            if left > 0 and broke[i]:
                fire[i], src[i], left = True, at, 0
            elif trigger[i]:
                left, at = REV["BREAKDOWN_WINDOW"], i
            elif left > 0:
                left -= 1
        trigger = fire
    side = REV["DIRECTION"] or ("short" if REV["MODE"] == "distribution" else "long")
    df["signal"] = np.where(trigger, -1 if side == "short" else 1, 0)
    idx = np.where(src >= 0, src, 0)
    for col in TRIG_COLS:
        df["trig_" + col] = np.where(src >= 0, df[col].to_numpy()[idx], np.nan)
    df["trig_time"] = np.where(src >= 0, df["time"].to_numpy()[idx], np.nan)
    return df


def param_rows(rows):
    """改寫「參數」頁：保留前6列與第16列（盈虧平衡勝率）的位置。"""
    C = pb.CONFIG
    head = list(rows[:6])
    short = (REV["DIRECTION"] or ("short" if REV["MODE"] == "distribution" else "long")) == "short"
    side = "做空" if short else "做多"
    head[3] = ("止盈", C["TAKE_PROFIT"], f"{side}：價格{'下跌' if short else '上漲'}此比例"
               if C["EXIT_MODE"] == "fixed" else f"（未使用）改依ATR：{C['TP_ATR_MULT']}×ATR")
    head[4] = ("止損", C["STOP_LOSS"], f"{side}：價格{'上漲' if short else '下跌'}此比例"
               if C["EXIT_MODE"] == "fixed" else f"（未使用）改依ATR：{C['SL_ATR_MULT']}×ATR")
    mid = [
        ("策略", f"策略2 — {'大戶提款' if REV['MODE'] == 'distribution' else '大戶進貨'}（{side}）",
         "快速拉抬→出貨" if REV["MODE"] == "distribution" else "快速下殺→吸貨"),
        ("流動性門檻 (USDT)", C["LIQ_MIN_USD"], "近24h平均每小時成交額"),
        ("拉抬窗口", f"{REV['PUMP_WINDOW']} 小時", "大戶買進須在此期間內完成"),
        ("突破", f"前{REV['BREAKOUT_HOURS']}小時" + ("最高價" if REV["MODE"] == "distribution" else "最低價"),
         "收盤須突破" if REV["MODE"] == "distribution" else "收盤須跌破"),
        ("進場時機", "觸發當根" if REV["ENTRY_TIMING"] == "breakout"
         else f"觸發後{REV['BREAKDOWN_WINDOW']}根內、收盤"
              + ("跌破前一根最低價" if REV["MODE"] == "distribution" else "突破前一根最高價"), "ENTRY_TIMING"),
        ("方向", side + ("" if REV["DIRECTION"] is None else "（手動指定）"), "DIRECTION"),
        ("拉抬漲幅下限", REV["MIN_PUMP_RET"] if REV["MIN_PUMP_RET"] is not None else "不限",
         f"近{REV['PUMP_WINDOW']}小時漲幅"),
        ("量能倍數下限", REV["MIN_VOL_SURGE"] if REV["MIN_VOL_SURGE"] is not None else "不限",
         f"近{REV['PUMP_WINDOW']}小時量能 ÷ 之前平均"),
        ("RSI 範圍", f"{REV['MIN_RSI'] if REV['MIN_RSI'] is not None else '不限'} ~ "
         + (f"{REV['MAX_RSI']}" if REV['MAX_RSI'] is not None else "不限"), "RSI(14)"),
        ("ATR 範圍", f"{C['ATR_MIN_PCT']:.1%} ~ "
         + (f"{C['ATR_MAX_PCT']:.1%}" if C["ATR_MAX_PCT"] is not None else "不限"), "ATR(14) ÷ 收盤"),
        ("價格上限", C["MAX_PRICE"] if C["MAX_PRICE"] is not None else "不限", "低價幣假設"),
    ]
    tail_be = [r for r in rows if r[0] == "盈虧平衡勝率"]
    out = head + mid[:9] + tail_be + mid[9:]     # 盈虧平衡固定在第16列
    out += [
        ("買盤佔比下限", REV["MIN_BUY_RATIO"] if REV["MIN_BUY_RATIO"] is not None else "不限",
         "OBV淨變化 ÷ 總量，越接近1代表買盤越一面倒"),
        ("量價背離上限", REV["MAX_PEAK_VOL_RATIO"] if REV["MAX_PEAK_VOL_RATIO"] is not None else "不限",
         "觸發當根量 ÷ 該段最大量，越小代表動能越衰竭"),
        ("量價背離下限", REV["MIN_PEAK_VOL_RATIO"] if REV["MIN_PEAK_VOL_RATIO"] is not None else "不限",
         "反過來要求量仍在放大時使用"),
        ("進場前複查ATR", "是" if REV["RECHECK_ATR_AT_ENTRY"] else "否", "進場當根 ATR 仍須在範圍內"),
        ("啟動前漲幅上限", REV["MAX_PRIOR_RET"] if REV["MAX_PRIOR_RET"] is not None else "不限", "更早之前須平靜"),
        ("MA20乖離下限", REV["MIN_MA_DEV"] if REV["MIN_MA_DEV"] is not None else "不限", "過熱程度"),
        ("收盤位置上限", REV["MAX_CLOSE_POS"] if REV["MAX_CLOSE_POS"] is not None else "不限", "0=收最低、1=收最高"),
        ("爆量門檻（舊）", REV["VOL_SPIKE"] if REV["VOL_SPIKE"] is not None else "關閉", "訊號K量 ÷ 前20根均量"),
        ("24h漲幅下限（舊）", REV["MIN_RUN_24H"] if REV["MIN_RUN_24H"] is not None else "關閉", ""),
        ("BTC乖離上限", REV["MAX_BTC_DEV"] if REV["MAX_BTC_DEV"] is not None else "不限", "BTC 自身 vs MA20，超過不做空"),
        ("時間停損", C["MAX_HOLD_HOURS"] or "無", "小時"),
        ("出場後冷卻", C["COOLDOWN_BARS"], "K棒數"),
        ("同K棒雙觸發", "/".join(C["RESOLVE_INTERVALS"]) + " 依序判斷"
         if C["RESOLVE_SAME_BAR_WITH_5M"] else "保守計止損", "同一根1h同時碰到止盈止損時的判斷方式"),
    ]
    return out


def diag_columns():
    return ([n for n, *_ in DIAG], [w for _, _, w, _ in DIAG],
            lambda tr: [tr.get(k) for _, k, _, _ in DIAG])


def main():
    pb.CONFIG.update(CONFIG)
    pb.PARAM_ROWS_HOOK = param_rows
    pb.DIAG_COLUMNS_HOOK = diag_columns
    C = pb.CONFIG

    now = int(time.time() * 1000) // HOUR * HOUR
    test_start = now - C["LOOKBACK_DAYS"] * 24 * HOUR
    fetch_start = test_start - C["WARMUP_BARS"] * HOUR
    period = f"{pb.to_dt(test_start):%Y-%m-%d %H:%M} ~ {pb.to_dt(now):%Y-%m-%d %H:%M}"
    print(f"策略2 反轉（{'做空' if REV['DIRECTION'] == 'short' else '做多對照'}／"
          f"{REV['ENTRY_TIMING']}）　回測區間：{period}")

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

    btc = None
    btc_sym = "BTC_USDT_PERP" if C["MARKET_TYPE"] == "PERP" else "BTC_USDT"
    try:
        b = pb.load_hourly(btc_sym, fetch_start, now)
        bc = b["close"]
        btc = pd.DataFrame({"time": b["time"], "btc_ret1h": bc / bc.shift(1) - 1,
                            "btc_ma_dev": bc / bc.rolling(20).mean() - 1})
    except Exception as e:
        print(f"[警告] 取得 {btc_sym} 失敗，BTC 欄位留空：{e}")

    all_trades = []
    for k, sym in enumerate(sorted(todo), 1):
        try:
            df = pb.load_hourly(sym, fetch_start, now)
            if len(df) < 60:
                scan_rows.append({"symbol": sym, "status": "略過", "reason": "K棒不足", "bars": len(df)})
                continue
            df = add_indicators(df, btc)
            trades = pb.backtest(sym, df, test_start)
            idx = df.set_index("time")
            for tr in trades:                       # 診斷欄位取「觸發當根」的數值
                row = idx.loc[tr["entry_time"] - HOUR]
                tt = row["trig_time"]
                tr.update(trig_time=pb.to_dt(int(tt)) if tt == tt else None,
                          wait_bars=int((tr["entry_time"] - HOUR - tt) // HOUR) if tt == tt else None,
                          ma_dev_raw=row["trig_ma_dev"], close_pos_raw=row["trig_close_pos"],
                          btc_dev_raw=row["trig_btc_ma_dev"],
                          **{k: row["trig_" + k] for k in
                             ("pump_ret", "prior_ret", "vol_surge", "buy_ratio",
                              "peak_vol_ratio", "brk", "rsi", "ret24")})
            all_trades.extend(trades)
            win = df[df["time"] >= test_start]
            scan_rows.append({"symbol": sym, "status": "已掃描", "reason": f"{len(trades)} 筆交易",
                              "bars": len(win), "min": float(win["close"].min()),
                              "max": float(win["close"].max()),
                              "sig_long": int((win["signal"] == 1).sum()),
                              "sig_short": int((win["signal"] == -1).sum())})
            print(f"[{k}/{len(todo)}] {sym}: {len(trades)} 筆")
        except Exception as e:
            scan_rows.append({"symbol": sym, "status": "失敗", "reason": str(e)[:200]})
            print(f"[{k}/{len(todo)}] {sym}: 失敗 {e}")

    all_trades.sort(key=lambda x: (x["entry_time"], x["symbol"]))
    scan_rows.sort(key=lambda x: ({"已掃描": 0, "略過": 1, "失敗": 2, "排除": 3}[x["status"]], x["symbol"]))
    out = C["OUTPUT"].format(ts=datetime.now(pb.TPE).strftime("%Y%m%d_%H%M"))
    pb.write_excel(all_trades, scan_rows, out, period + "（策略2 反轉）")

    closed = [t for t in all_trades if t["result"] != "未平倉"]
    if closed:
        wins = sum(t["result"] == "止盈" for t in closed)
        be = np.mean([(t["sl_pct"] + 2 * C["FEE_RATE"]) / (t["tp_pct"] + t["sl_pct"]) for t in closed])
        print(f"\n完成：{len(all_trades)} 筆（已平倉 {len(closed)}，止盈 {wins}，"
              f"勝率 {wins / len(closed):.1%}，盈虧平衡勝率 {be:.1%}）")
    else:
        print("\n完成：沒有任何交易，可考慮放寬 VOL_SPIKE / MIN_MA_DEV / MIN_RUN_24H")
    print(f"輸出：{os.path.abspath(out)}")


if __name__ == "__main__":
    main()
