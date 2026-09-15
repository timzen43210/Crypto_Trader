#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
派網策略 Dry Run（前瞻模擬，不下單）
===================================
同時追蹤策略1（延續、做多做空）與策略2（大戶提款、只做空）
每次執行：抓最新的 1h K棒 → 依序處理「上次執行之後新收完的每一根K棒」
         → 先檢查持倉是否止盈/止損，再檢查新訊號 → 寫入狀態檔、Excel、SUMMARY.md

・進出場規則與 pionex_backtest.py 完全相同（直接呼叫它的函式），結果可直接和回測比較。
・START_FROM 可指定回補起算時間（受 API 上限約 20 天）；設 None 則從第一次執行當下開始。
・中間漏跑幾次也沒關係：下次執行會把漏掉的K棒補處理（K棒上限 500 根 ≈ 20 天）。
・請勿在測試途中修改策略參數；要改的話請刪掉 state/ 重新開始，否則紀錄會混在一起。

執行：python pionex_dryrun.py
"""
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

# ============================== 起算時間 ==============================
# 第一次執行時，從這個時間開始回補統計（台北時間）。設 None = 只從執行當下開始。
# 派網 1h K棒單次最多取 500 根（約 20.8 天），太舊的抓不到，請盡早開始跑。
START_FROM = "2026-09-01 00:00"

# ============================== 策略設定（固定，勿在測試途中修改） ==============================
BASE_CONFIG = dict(
    MARKET_TYPE="PERP",
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
    MARKET_TYPE="PERP",
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

BOOKS = {
    "main": {"s": 1, "label": "策略1 延續（ATR 4–5%）", "overrides": {}},
    # 觀察組：只放寬 ATR 範圍，用來每月檢查哪個 ATR 區間最好（見 ATR 區間監控頁）
    "watch": {"s": 1, "label": "策略1 觀察組（ATR ≥ 2%，供區間監控）",
              "overrides": {"ATR_MIN_PCT": 0.02, "ATR_MAX_PCT": None}},
    "rev": {"s": 2, "label": "策略2 大戶提款 正式版（啟動前漲幅 ≤ 5%）", "overrides": {}, "rev": {}},
    "rev_wide": {"s": 2, "label": "策略2 大戶提款 放寬版（啟動前漲幅不限）",
                 "overrides": {}, "rev": {"MAX_PRIOR_RET": None}},
}
ATR_BANDS = [(0.02, 0.03), (0.03, 0.04), (0.04, 0.05), (0.05, 0.06), (0.06, 0.08), (0.08, None)]

KLINE_LIMIT = 500
WORKERS = 3
HERE = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(HERE, "state", "dryrun_state.json")
OUT_DIR = os.path.join(HERE, "output")
HOUR = pb.HOUR_MS


def use_config(book):
    meta = BOOKS[book]
    if meta["s"] == 1:
        pb.CONFIG.update(BASE_CONFIG)
        pb.PARAM_ROWS_HOOK = pb.DIAG_COLUMNS_HOOK = None
    else:
        pb.CONFIG.update(S2_CONFIG)
        rv.REV.update(S2_REV)
        rv.REV.update(meta.get("rev", {}))
        pb.PARAM_ROWS_HOOK, pb.DIAG_COLUMNS_HOOK = rv.param_rows, s2_diag
    pb.CONFIG.update(meta["overrides"])


def indicators(book, df, btc):
    return (pb.add_indicators if BOOKS[book]["s"] == 1 else rv.add_indicators)(df, btc)


def s2_diag():
    """策略2 的診斷欄位；trig_time 在狀態檔裡以毫秒儲存，輸出時轉成日期。"""
    def vals(tr):
        out = []
        for _, key, _, _ in rv.DIAG:
            v = tr.get(key)
            out.append(pb.to_dt(int(v)) if key == "trig_time" and v is not None else v)
        return out
    return [n for n, *_ in rv.DIAG], [w for _, _, w, _ in rv.DIAG], vals


def start_ms():
    if not START_FROM:
        return None
    dt = datetime.strptime(START_FROM, "%Y-%m-%d %H:%M").replace(tzinfo=pb.TPE)
    return int(dt.timestamp() * 1000) // HOUR * HOUR


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
    return st


def save_state(st):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(st, fh, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, STATE_PATH)


# ============================== 資料 ==============================
def prepare(df, t_now):
    """去掉未收完的K棒、補齊缺漏小時（與回測 load_hourly 相同處理）。"""
    if df.empty:
        return df
    df = df[df["time"] + HOUR <= t_now].sort_values("time").reset_index(drop=True)
    if df.empty:
        return df
    grid = pd.DataFrame({"time": np.arange(df["time"].min(), df["time"].max() + 1, HOUR, dtype="int64")})
    df = grid.merge(df, on="time", how="left")
    df["close"] = df["close"].ffill()
    for c in ("open", "high", "low"):
        df[c] = df[c].fillna(df["close"])
    df["volume"] = df["volume"].fillna(0)
    if "amount" in df.columns:
        df["amount"] = df["amount"].fillna(0)
    return df


def fetch(symbol, t_now):
    raw = pb.fetch_klines_raw(symbol, "60M", t_now, t_now - KLINE_LIMIT * HOUR)
    return prepare(raw, t_now)


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


def step_symbol(bk, sym, df, strategy=1, backfill_from=None):
    """處理此交易對自上次以來新收完的K棒。回傳 (新開倉數, 新平倉數)。"""
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
            result, exit_px, exit_ms, note = None, None, t[j] + HOUR, ""
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
                if mh and (t[j] + HOUR - pos["entry_time"]) >= mh * HOUR:
                    result, exit_px, note = "時間出場", float(c[j]), f"持倉達{mh}h"
            if result:
                tr = {k: v for k, v in pos.items() if k not in ("d", "entry_bar")}
                tr.update(exit_time=int(exit_ms), exit=float(exit_px), result=result, note=note)
                bk["closed"].append(tr)
                st["pos"] = None
                st["cool_until"] = int(t[j] + cfg["COOLDOWN_BARS"] * HOUR)
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
                "entry_bar": int(t[j]), "entry_time": int(t[j] + HOUR), "entry": entry,
                "tp": entry * (1 + d * tp_pct), "sl": entry * (1 - d * sl_pct),
                "tp_pct": float(tp_pct), "sl_pct": float(sl_pct), "mfe": 0.0, "mae": 0.0,
                **snapshot(df.iloc[j], d),
                **(snapshot2(df.iloc[j], int(t[j])) if strategy == 2 else {}),
            }
            opened += 1
    st["last"] = int(t[-1])
    st["last_close"] = float(c[-1])
    return opened, closed


def open_as_trades(bk):
    out = []
    for sym, st in bk["symbols"].items():
        pos = st.get("pos")
        if pos:
            tr = {k: v for k, v in pos.items() if k not in ("d", "entry_bar")}
            tr.update(exit_time=int(st["last"] + HOUR), exit=st["last_close"] or pos["entry"],
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
        trades = sorted(bk["closed"] + open_as_trades(bk), key=lambda x: (x["entry_time"], x["symbol"]))
        scan = [{"symbol": s, "status": "已掃描", "reason": "持倉中" if v.get("pos") else "",
                 "bars": None} for s, v in sorted(bk["symbols"].items())]
        period = f"{start_txt} ~ {pb.to_dt(t_now):%Y-%m-%d %H:%M}（Dry Run）"
        pb.write_excel(trades, scan, os.path.join(OUT_DIR, f"dryrun_{book}.xlsx"), period,
                       extra=make_extra(state, book, monitor if book == "main" else None))
        pb.PARAM_ROWS_HOOK = pb.DIAG_COLUMNS_HOOK = None
        # ---- SUMMARY.md ----
        df = closed_df(bk)
        opens = open_as_trades(bk)
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
        if opens:
            lines += [f"持倉中 {len(opens)} 筆：", "", "| 交易對 | 方向 | 進場時間 | 進場價 | 止盈價 | 止損價 | 最新價 |",
                      "|---|---|---|---|---|---|---|"]
            for p in sorted(opens, key=lambda x: x["entry_time"]):
                lines.append(f"| {p['symbol']} | {p['dir']} | {pb.to_dt(p['entry_time']):%m-%d %H:%M} | "
                             f"{p['entry']:.6g} | {p['tp']:.6g} | {p['sl']:.6g} | {p['exit']:.6g} |")
            lines += [""]
        if book == "main":
            lines += [f"**{monitor[1]}**", ""]
    with open(os.path.join(OUT_DIR, "SUMMARY.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


# ============================== 主程式 ==============================
def main():
    t0 = time.time()
    t_now = now_ms()
    use_config("main")
    state = load_state()
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
    b = fetch(btc_sym, t_now)
    btc = pd.DataFrame({"time": b["time"], "btc_ret1h": b["close"] / b["close"].shift(1) - 1,
                        "btc_ma_dev": b["close"] / b["close"].rolling(20).mean() - 1})

    def job(sym):
        try:
            return sym, fetch(sym, t_now), None
        except Exception as e:
            return sym, None, str(e)[:200]

    # 回補只在「這本帳本」的首次執行生效（state 內尚無任何交易對紀錄）；
    # 之後才進 universe 的新幣，即使是舊帳本也一律從最新一根開始（見 step_symbol()）。
    s0 = start_ms()
    backfill_from = {bk: (s0 if s0 and not state["books"][bk]["symbols"] else None) for bk in BOOKS}
    fresh_books = [bk for bk, v in backfill_from.items() if v]
    if fresh_books:
        earliest = int(b["time"].iloc[0]) if len(b) else None
        print(f"首次執行（{'/'.join(fresh_books)}）：回補 {pb.to_dt(s0):%Y-%m-%d %H:%M} 起的K棒")
        if earliest and earliest > s0:
            print(f"[警告] API 只能取到 {pb.to_dt(earliest):%Y-%m-%d %H:%M} 之後的K棒，"
                  f"{pb.to_dt(s0):%m-%d} ~ {pb.to_dt(earliest):%m-%d} 這段無法回補")

    errors, last_bar = [], None
    counts = {bk: {"opened": 0, "closed": 0} for bk in BOOKS}
    with ThreadPoolExecutor(WORKERS) as ex:
        for sym, df, err in ex.map(job, sorted(universe)):
            if err or df is None or len(df) < 60:
                errors.append((sym, err or "K棒不足"))
                continue
            last_bar = max(last_bar or 0, int(df["time"].iloc[-1]))
            for book in BOOKS:
                use_config(book)
                try:
                    o_, c_ = step_symbol(state["books"][book], sym, indicators(book, df.copy(), btc),
                                         BOOKS[book]["s"], backfill_from[book])
                    counts[book]["opened"] += o_
                    counts[book]["closed"] += c_
                except Exception:
                    errors.append((sym, traceback.format_exc(limit=1)[-200:]))
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
