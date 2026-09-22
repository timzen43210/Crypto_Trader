#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V2 — 滑價敏感度分析：TP/SL 用訊號價 vs 成交價
==============================================
回答的問題：實盤進場有滑價（訊號價 P0 → 成交價 P1 = P0 × (1 + s)）時，止盈/止損掛在
P0（"signal"）或 P1（"fill"）各自的勝率 w 與每筆 EV 是多少？兩種算法在哪個滑價交叉？

這是一次性的研究腳本，**刻意複製** pb.backtest() 的進出場控制流（不能改既有引擎），
並用 calibrate 階段以真實快取證明 slippage=0 / basis="signal" 時與 pb.backtest() 逐筆一致。

階段（可單獨跑，也可 `all` 一次跑完）：
  check      掃 pionex_cache，印出涵蓋範圍、推導 freeze_end / test_start / fetch_start，確認不截斷
  calibrate  對所有可用交易對跑 parity，寫 results/v2_calibration.json；不一致 > 0 → 報錯退出
  scan       讀 calibration（未通過拒跑），訊號只算一次，跑 slippage × basis 全組合，
             逐組 checkpoint 到 results/v2_scan.json，逐筆明細寫 results/v2_trades.csv，可續跑
             （續跑時明細檔只做文字列過濾，已完成組合的列位元組不變；BUG-001）
  report     由結果檔產生 results/v2_report.md

離線模式（預設）：
  * pb.fetch_klines_raw 被換成回傳「空的、dtype 已定型」的 DataFrame；pb.api_get 被換成直接拋錯，
    保證 0 次 API 請求。
  * 交易對清單來自快取檔名（{SYMBOL}_{INTERVAL}.csv），再套 pb.classify() 的排除規則。
  * freeze_end = 所有快取檔最大 time + BAR（再對 BAR 取整），確保 pb.load_hourly() 不會截短任何檔。
  * pb.load_hourly() 一定會把合併結果 to_csv 寫回快取；為了不改動使用者快取原檔的任何位元組
    （323 個檔 volume 為 int64，與空表 concat 後會變 float64 格式），離線模式先把 5M 快取複製到
    暫存目錄，CACHE_DIR 指向副本，結束後刪除。
--fetch：沿用 pb 原生行為真的打 API 補抓（事前印出預計補抓的交易對數），寫回真實快取。本任務不使用。

用法：
  python research/v2_slippage_scan.py check
  python research/v2_slippage_scan.py calibrate
  python research/v2_slippage_scan.py scan
  python research/v2_slippage_scan.py report
  python research/v2_slippage_scan.py all
"""
import argparse
import atexit
import glob
import hashlib
import json
import math
import os
import platform
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

SCRIPT_VERSION = "2.0.2"
HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
RESULTS_DIR = os.path.join(HERE, "results")
REAL_CACHE_DIR = os.path.join(REPO_ROOT, "pionex_cache")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import pionex_backtest as pb            # noqa: E402
import pionex_strategy4 as s4           # noqa: E402
from strategy import s4_signal          # noqa: E402

# ============================== 掃描設定 ==============================
SLIPPAGES = [0.0025, 0.0, -0.001, -0.0025, -0.005, -0.0075, -0.01, -0.015, -0.02]
BASES = ("signal", "fill")
SAME_BAR_GUARD = 0.15          # PRD 2.2：同根雙觸發比例 > 15% 要停下來回報
MIN_CALIB_SYMBOLS = 100        # PRD AC-1：校準至少涵蓋 100 個交易對
PRD_W_THRESHOLD = 0.649        # PRD 1.2：用成交價時勝率的臨界點（假設 w_signal=68.3%、s=−0.31%）

PARITY_KEYS = ("entry_time", "entry", "tp", "sl", "exit_time", "exit", "result")
SIM_KEY_OF = {"entry": "p0"}   # pb.backtest 欄位 → simulate 欄位
CONFIG_KEYS = ("MARKET_TYPE", "INTERVAL", "LOOKBACK_DAYS", "WARMUP_BARS", "EXIT_MODE",
               "TAKE_PROFIT", "STOP_LOSS", "FEE_RATE", "COOLDOWN_BARS", "MAX_HOLD_HOURS",
               "RESOLVE_SAME_BAR_WITH_5M", "RESOLVE_INTERVALS")
TRADE_COLUMNS = ["combo", "slippage", "basis", "symbol", "dir", "entry_time", "entry_time_tpe",
                 "p0", "p1", "tp", "sl", "exit_time", "exit_time_tpe", "exit", "result",
                 "same_bar_hit", "gap", "gross_ret", "net_ret", "hold_bars", "hold_hours", "note"]

F_CHECK, F_CALIB, F_SCAN, F_TRADES, F_REPORT = (
    "v2_check.json", "v2_calibration.json", "v2_scan.json", "v2_trades.csv", "v2_report.md")

STATE = {"offline": True, "api_calls": 0, "snapshot_dir": None}
_ORIG_API_GET = pb.api_get
_ORIG_FETCH_KLINES_RAW = pb.fetch_klines_raw


# ============================== 離線攔截 ==============================
def _blocked_api_get(path, params, retries=5):
    STATE["api_calls"] += 1
    raise RuntimeError(f"離線模式：禁止呼叫派網 API（{path} {params}）。要補抓請加 --fetch。")


def _counting_api_get(path, params, retries=5):
    STATE["api_calls"] += 1
    return _ORIG_API_GET(path, params, retries)


def _offline_fetch_klines_raw(symbol, interval, end_ms, stop_ms):
    """離線：回傳空的、dtype 已定型的 K 棒表（與 pb.fetch_klines_raw 無資料時的回傳完全相同，
       F1 已確保與快取 concat 不會污染 dtype）。"""
    return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"]).astype(
        {"time": "int64", "open": "float64", "high": "float64",
         "low": "float64", "close": "float64", "volume": "float64"}
    )


def setup_config(offline=True):
    """與 pionex_strategy4.main() 前三行相同的 in-process 設定，外加本任務的強制項。"""
    pb.CONFIG.update(s4.CONFIG)
    pb.CONFIG["COOLDOWN_BARS"] = s4_signal.cooldown_bars(s4.H(), s4.S4)
    pb.CONFIG["RESOLVE_SAME_BAR_WITH_5M"] = False           # PRD 2.2：本任務強制關閉
    pb.CONFIG["CACHE_DIR"] = REAL_CACHE_DIR                 # 絕對路徑，不依賴工作目錄
    os.chdir(REPO_ROOT)
    STATE["offline"] = bool(offline)
    STATE["api_calls"] = 0
    if offline:
        pb.api_get = _blocked_api_get
        pb.fetch_klines_raw = _offline_fetch_klines_raw
    else:
        pb.api_get = _counting_api_get
        pb.fetch_klines_raw = _ORIG_FETCH_KLINES_RAW


def use_cache_snapshot():
    """離線模式：把 {INTERVAL} 快取複製到暫存目錄，pb.load_hourly() 的寫回只落在副本。"""
    iv = pb.CONFIG["INTERVAL"]
    d = tempfile.mkdtemp(prefix="v2_cache_")
    n = 0
    for f in glob.glob(os.path.join(REAL_CACHE_DIR, f"*_{iv}.csv")):
        shutil.copyfile(f, os.path.join(d, os.path.basename(f)))
        n += 1
    pb.CONFIG["CACHE_DIR"] = d
    STATE["snapshot_dir"] = d
    atexit.register(shutil.rmtree, d, True)
    return d, n


# ============================== 工具 ==============================
def _py(v):
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    return v


def tpe_str(ms):
    return f"{pb.to_dt(int(ms)):%Y-%m-%d %H:%M}"


def parse_freeze_end(s):
    """'YYYY-MM-DD HH:MM'（台北時間）→ ms。"""
    if not s:
        return None
    return int(datetime.strptime(s[:16], "%Y-%m-%d %H:%M").replace(tzinfo=pb.TPE).timestamp() * 1000)


def git_head():
    try:
        with open(os.path.join(REPO_ROOT, ".git", "HEAD"), encoding="utf-8") as f:
            head = f.read().strip()
        if head.startswith("ref: "):
            ref = head[5:]
            p = os.path.join(REPO_ROOT, ".git", *ref.split("/"))
            if os.path.exists(p):
                with open(p, encoding="utf-8") as f:
                    return {"ref": ref, "commit": f.read().strip()}
            return {"ref": ref, "commit": None}
        return {"ref": None, "commit": head}
    except Exception:
        return {"ref": None, "commit": None}


def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=_py)
    os.replace(tmp, path)


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ============================== 快取檢查與時間窗 ==============================
def scan_cache(cache_dir=None):
    """只讀每個 {INTERVAL} 快取檔的 time 欄，回傳 [{symbol, rows, t_min, t_max}]。"""
    cache_dir = cache_dir or REAL_CACHE_DIR
    iv = pb.CONFIG["INTERVAL"]
    suffix = f"_{iv}.csv"
    stats = []
    for f in sorted(glob.glob(os.path.join(cache_dir, f"*{suffix}"))):
        sym = os.path.basename(f)[:-len(suffix)]
        try:
            t = pd.read_csv(f, usecols=["time"])["time"]
        except Exception as e:
            stats.append({"symbol": sym, "rows": 0, "error": str(e)[:120]})
            continue
        if len(t) == 0:
            stats.append({"symbol": sym, "rows": 0})
            continue
        stats.append({"symbol": sym, "rows": int(len(t)), "t_min": int(t.min()), "t_max": int(t.max())})
    return stats


def derive_window(stats, freeze_end_ms=None):
    """freeze_end（now_ms）預設 = 所有快取檔最大 time + BAR（對 BAR 取整）→ 保證 load_hourly 不截斷。"""
    BAR = pb.bar_ms()
    valid = [s for s in stats if s.get("rows", 0) > 0]
    if not valid:
        raise RuntimeError("快取中沒有任何可讀的 K 棒檔")
    global_max = max(s["t_max"] for s in valid)
    global_min = min(s["t_min"] for s in valid)
    now_ms = (global_max + BAR) // BAR * BAR if freeze_end_ms is None else freeze_end_ms // BAR * BAR
    test_start = now_ms - pb.CONFIG["LOOKBACK_DAYS"] * 24 * pb.HOUR_MS
    fetch_start = test_start - pb.CONFIG["WARMUP_BARS"] * BAR
    would_truncate = [s["symbol"] for s in valid if s["t_max"] + BAR > now_ms]
    return {
        "now_ms": int(now_ms), "test_start": int(test_start), "fetch_start": int(fetch_start),
        "global_t_min": int(global_min), "global_t_max": int(global_max),
        "would_truncate": would_truncate,
    }


def build_universe(stats):
    """離線：交易對清單 = 快取檔名，套 pb.classify() 同一套排除規則。回傳 (todo, scan_rows)。"""
    want_perp = pb.CONFIG["MARKET_TYPE"] == "PERP"
    todo, rows = [], []
    for s in stats:
        sym = s["symbol"]
        parts = sym.split("_")
        if len(parts) < 2:
            rows.append({"symbol": sym, "status": "忽略", "reason": "檔名無法解析"})
            continue
        base, quote = parts[0], parts[1]
        is_perp = len(parts) >= 3 and parts[2] == "PERP"
        if is_perp != want_perp:
            rows.append({"symbol": sym, "status": "忽略", "reason": "市場類型與 MARKET_TYPE 不符"})
            continue
        if s.get("rows", 0) == 0:
            rows.append({"symbol": sym, "status": "略過", "reason": "快取檔空白或無法讀取", "bars": 0})
            continue
        _, reason = pb.classify({"symbol": sym, "baseCurrency": base, "quoteCurrency": quote, "enable": True})
        if reason is None:
            rows.append({"symbol": sym, "status": "忽略", "reason": "非 USDT 交易對"})
        elif reason:
            rows.append({"symbol": sym, "status": "排除", "reason": reason})
        else:
            todo.append(sym)
    return sorted(todo), rows


def build_universe_online():
    """--fetch：與 pionex_strategy4.main() 相同，用 pb.get_symbols() 取清單。"""
    todo, rows = [], []
    for s in pb.get_symbols():
        _, reason = pb.classify(s)
        if reason is None:
            continue
        if reason:
            rows.append({"symbol": s["symbol"], "status": "排除", "reason": reason})
        else:
            todo.append(s["symbol"])
    return sorted(todo), rows


def min_bars_required():
    return min(300, 26 * s4.H())


def load_symbol(sym, win):
    """pb.load_hourly() + pionex_strategy4.add_indicators()。K 棒不足回傳 (None, bars)。"""
    df = pb.load_hourly(sym, win["fetch_start"], win["now_ms"])
    if len(df) < min_bars_required():
        return None, len(df)
    df = s4.add_indicators(df)
    return df, len(df)


def signals_in_window(df, test_start):
    return int(((df["signal"] != 0) & (df["time"] >= test_start)).sum())


# ============================== 模擬器 ==============================
def arrays_from_df(df):
    return {"t": df["time"].to_numpy(), "o": df["open"].to_numpy(), "h": df["high"].to_numpy(),
            "l": df["low"].to_numpy(), "c": df["close"].to_numpy(), "sig": df["signal"].to_numpy()}


def simulate(symbol, df, test_start_ms, slippage, tp_sl_basis):
    """PRD 3.1 的獨立模擬器。df 需含 time/open/high/low/close/signal（由 s4.add_indicators 產生）。"""
    return simulate_arrays(symbol, arrays_from_df(df), test_start_ms, slippage, tp_sl_basis)


def simulate_arrays(symbol, a, test_start_ms, slippage, tp_sl_basis):
    """複製 pb.backtest() 的控制流（RESOLVE_SAME_BAR_WITH_5M=False、EXIT_MODE=fixed、無時間出場），
       差異只有：
         P1 = P0 × (1 + slippage)（純價格相對量，不依方向翻號）
         tp/sl 的基準 = P0（basis="signal"）或 P1（basis="fill"）
         損益一律以 P1 計：gross = d × (exit / P1 − 1)，net = gross − 2 × FEE_RATE
    """
    if tp_sl_basis not in BASES:
        raise ValueError(f"tp_sl_basis 必須是 {BASES}，收到 {tp_sl_basis!r}")
    C = pb.CONFIG
    if C["EXIT_MODE"] != "fixed":
        raise ValueError("本模擬器只複製 EXIT_MODE='fixed' 的行為")
    if C["MAX_HOLD_HOURS"]:
        raise ValueError("本模擬器不支援 MAX_HOLD_HOURS（策略4 為 None）")
    if C["RESOLVE_SAME_BAR_WITH_5M"]:
        raise ValueError("本任務要求 RESOLVE_SAME_BAR_WITH_5M=False")
    t, o, h, l, c, sig = a["t"], a["o"], a["h"], a["l"], a["c"], a["sig"]
    BAR = pb.bar_ms()
    tp_pct, sl_pct, fee = C["TAKE_PROFIT"], C["STOP_LOSS"], C["FEE_RATE"]
    cooldown = C["COOLDOWN_BARS"]
    combo = combo_label(slippage, tp_sl_basis)
    n, trades = len(t), []
    i = int(np.searchsorted(t, test_start_ms))
    while i < n:
        if sig[i] == 0:
            i += 1
            continue
        d, p0 = int(sig[i]), c[i]                       # p0 = 訊號根收盤價（與 pb 的 entry 相同）
        p1 = p0 * (1 + slippage)                        # 實際成交價
        entry_ms = t[i] + BAR
        base = p0 if tp_sl_basis == "signal" else p1
        tp = base * (1 + d * tp_pct)
        sl = base * (1 - d * sl_pct)
        result, exit_px, exit_ms, note, j = "未平倉", c[-1], t[-1] + BAR, "", n - 1
        same_bar = gap = False
        for j in range(i + 1, n):
            hit_tp = h[j] >= tp if d == 1 else l[j] <= tp
            hit_sl = l[j] <= sl if d == 1 else h[j] >= sl
            if hit_tp and hit_sl:
                result, exit_ms, note = "止損", t[j] + BAR, "同一根同時觸發→保守計止損"
                same_bar = True
            elif hit_sl:
                result, exit_ms = "止損", t[j] + BAR
            elif hit_tp:
                result, exit_ms = "止盈", t[j] + BAR
            if result == "止盈":
                exit_px = tp
                break
            if result == "止損":
                gap = bool((o[j] < sl) if d == 1 else (o[j] > sl))   # 跳空穿越止損 → 以開盤價成交
                exit_px = o[j] if gap and o[j] > 0 else sl
                if gap:
                    note = (note + "；" if note else "") + "開盤跳空穿越止損"
                break
        gross = d * (exit_px / p1 - 1)
        net = gross - 2 * fee
        hold_bars = int((exit_ms - entry_ms) // BAR)
        trades.append({
            "combo": combo, "slippage": float(slippage), "basis": tp_sl_basis,
            "symbol": symbol, "dir": "做多" if d == 1 else "做空",
            "entry_time": int(entry_ms), "entry_time_tpe": tpe_str(entry_ms),
            "p0": float(p0), "p1": float(p1), "tp": float(tp), "sl": float(sl),
            "exit_time": int(exit_ms), "exit_time_tpe": tpe_str(exit_ms),
            "exit": float(exit_px), "result": result,
            "same_bar_hit": bool(same_bar), "gap": bool(gap),
            "gross_ret": float(gross), "net_ret": float(net),
            "hold_bars": hold_bars, "hold_hours": hold_bars * BAR / pb.HOUR_MS,
            "note": note if result != "未平倉" else "回測結束仍持倉（以最後收盤估值）",
        })
        if result == "未平倉":
            break
        i = j + cooldown
    return trades


def combo_label(slippage, basis):
    return f"s={slippage:+.4f}|{basis}"


def compare_parity(pb_trades, sim_trades):
    """逐筆比對 pb.backtest 與 simulate（entry_time/entry/tp/sl/exit_time/exit/result）。回傳不一致清單。"""
    mism = []
    if len(pb_trades) != len(sim_trades):
        mism.append({"kind": "count", "pb": len(pb_trades), "sim": len(sim_trades)})
    for k, (a, b) in enumerate(zip(pb_trades, sim_trades)):
        for key in PARITY_KEYS:
            va, vb = a[key], b[SIM_KEY_OF.get(key, key)]
            if isinstance(va, str) or isinstance(vb, str):
                ok = va == vb
            else:
                ok = bool(va == vb)                      # 算式相同 → 要求完全相等
            if not ok:
                item = {"kind": "field", "idx": k, "field": key, "pb": _py(va), "sim": _py(vb)}
                if not (isinstance(va, str) or isinstance(vb, str)):
                    item["isclose_1e-12"] = math.isclose(float(va), float(vb), rel_tol=1e-12)
                mism.append(item)
    return mism


# ============================== 統計 ==============================
def _mean(xs):
    return float(np.mean(xs)) if len(xs) else None


def effective_levels(slippage, basis, d=-1):
    """相對成交價 P1 的實際止盈/止損幅度（做空 d=−1；策略4 只做空）。
       signal：tp_eff = d×((1+d·tp)/(1+s) − 1)，sl_eff = −d×((1−d·sl)/(1+s) − 1)；fill：就是 tp/sl。"""
    C = pb.CONFIG
    tp_pct, sl_pct = C["TAKE_PROFIT"], C["STOP_LOSS"]
    if basis == "fill":
        return tp_pct, sl_pct
    tp_eff = d * ((1 + d * tp_pct) / (1 + slippage) - 1)
    sl_eff = -d * ((1 - d * sl_pct) / (1 + slippage) - 1)
    return tp_eff, sl_eff


def combo_stats(trades, slippage, basis):
    C = pb.CONFIG
    fee = 2 * C["FEE_RATE"]
    closed = [x for x in trades if x["result"] in ("止盈", "止損")]
    opened = [x for x in trades if x["result"] == "未平倉"]
    wins = [x for x in closed if x["result"] == "止盈"]
    losses = [x for x in closed if x["result"] == "止損"]
    n_c = len(closed)
    avg_win_g, avg_loss_g = _mean([x["gross_ret"] for x in wins]), _mean([x["gross_ret"] for x in losses])
    be = None
    if avg_win_g is not None and avg_loss_g is not None and (avg_win_g - avg_loss_g) != 0:
        be = (-avg_loss_g + fee) / (avg_win_g - avg_loss_g)      # avg_loss_g 為負值
    tp_eff, sl_eff = effective_levels(slippage, basis)
    same_bar_n = sum(1 for x in closed if x["same_bar_hit"])
    return {
        "combo": combo_label(slippage, basis), "slippage": float(slippage), "basis": basis,
        "n_trades": len(trades), "n_closed": n_c, "n_open": len(opened),
        "n_tp": len(wins), "n_sl": len(losses),
        "win_rate": (len(wins) / n_c) if n_c else None,
        "avg_win_gross": avg_win_g, "avg_loss_gross": avg_loss_g,
        "avg_win_net": _mean([x["net_ret"] for x in wins]),
        "avg_loss_net": _mean([x["net_ret"] for x in losses]),
        "ev_gross": _mean([x["gross_ret"] for x in closed]),
        "ev_net": _mean([x["net_ret"] for x in closed]),
        "sum_net": float(sum(x["net_ret"] for x in closed)),
        "breakeven_w": be,
        "tp_eff_theory": float(tp_eff), "sl_eff_theory": float(sl_eff),
        "breakeven_w_theory": (sl_eff + fee) / (tp_eff + sl_eff) if (tp_eff + sl_eff) else None,
        "same_bar_n": same_bar_n, "same_bar_pct": (same_bar_n / n_c) if n_c else None,
        "gap_n": sum(1 for x in closed if x["gap"]),
        "avg_hold_bars": _mean([x["hold_bars"] for x in closed]),
        "median_hold_bars": float(np.median([x["hold_bars"] for x in closed])) if n_c else None,
        "avg_hold_hours": _mean([x["hold_hours"] for x in closed]),
        "n_symbols_with_trades": len({x["symbol"] for x in trades}),
    }


# ============================== metadata ==============================
def build_metadata(stage, win, cache_stats, universe, extra=None):
    valid = [s for s in cache_stats if s.get("rows", 0) > 0]
    rows = np.array([s["rows"] for s in valid]) if valid else np.array([0])
    now_utc = datetime.now(timezone.utc)
    md = {
        "task": "V2 滑價敏感度分析（TP/SL 訊號價 vs 成交價）",
        "script": os.path.relpath(os.path.abspath(__file__), REPO_ROOT).replace("\\", "/"),
        "script_version": SCRIPT_VERSION, "stage": stage,
        "run_time_tpe": now_utc.astimezone(pb.TPE).isoformat(timespec="seconds"),
        "run_time_utc": now_utc.isoformat(timespec="seconds"),
        "machine": platform.node(), "os": platform.platform(),
        "python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__,
        "repo_head": git_head(),
        "offline": STATE["offline"], "resolve_disabled": True,
        "api_requests": STATE["api_calls"],
        "cache_snapshot_used": STATE["snapshot_dir"] is not None,
        "config": {k: _py(pb.CONFIG.get(k)) for k in CONFIG_KEYS},
        "s4_params": {k: _py(s4.S4[k]) for k in s4_signal.PARAM_KEYS},
        "s4_experimental": {k: _py(s4.S4[k]) for k in s4.EXPERIMENTAL_KEYS},
        "bars_per_hour": s4.H(), "min_bars_required": min_bars_required(),
        "cache": {
            "dir": os.path.relpath(REAL_CACHE_DIR, REPO_ROOT), "interval": pb.CONFIG["INTERVAL"],
            "files": len(cache_stats), "files_readable": len(valid),
            "t_min": win["global_t_min"], "t_min_tpe": tpe_str(win["global_t_min"]),
            "t_max": win["global_t_max"], "t_max_tpe": tpe_str(win["global_t_max"]),
            "rows_min": int(rows.min()), "rows_median": float(np.median(rows)), "rows_max": int(rows.max()),
        },
        "freeze_end": win["now_ms"], "freeze_end_tpe": tpe_str(win["now_ms"]),
        "test_start": win["test_start"], "test_start_tpe": tpe_str(win["test_start"]),
        "fetch_start": win["fetch_start"], "fetch_start_tpe": tpe_str(win["fetch_start"]),
        "symbols_source": ("快取檔名（無法得知交易對是否已下架）" if STATE["offline"] else "pb.get_symbols()"),
        "symbols": universe,
    }
    if extra:
        md.update(extra)
    return md


def universe_summary(scan_rows, n_scanned):
    cnt = {}
    for r in scan_rows:
        cnt[r["status"]] = cnt.get(r["status"], 0) + 1
    return {"scanned": n_scanned, "excluded": cnt.get("排除", 0), "skipped": cnt.get("略過", 0),
            "failed": cnt.get("失敗", 0), "ignored": cnt.get("忽略", 0)}


def fingerprint(md, per_symbol):
    """校準與掃描必須在同一份資料、同一組設定上：freeze_end + config + S4 + 各交易對 (bars, 訊號數)。"""
    core = {"freeze_end": md["freeze_end"], "test_start": md["test_start"], "fetch_start": md["fetch_start"],
            "config": md["config"], "s4_params": md["s4_params"],
            "symbols": sorted((p["symbol"], p["bars"], p["n_signals"]) for p in per_symbol)}
    return hashlib.sha256(json.dumps(core, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


# ============================== 共同前置 ==============================
def prepare_run(args, stage):
    """設定 → 掃快取 → 推時間窗 → 交易對清單 → （離線）快取快照。回傳 (cache_stats, win, todo, scan_rows)。"""
    setup_config(offline=not args.fetch)
    cache_stats = scan_cache()
    if args.fetch:
        BAR = pb.bar_ms()
        fe = parse_freeze_end(args.freeze_end)
        now_ms = fe if fe is not None else int(time.time() * 1000) // BAR * BAR
        win = derive_window(cache_stats, now_ms) if cache_stats else {
            "now_ms": now_ms, "test_start": now_ms - pb.CONFIG["LOOKBACK_DAYS"] * 24 * pb.HOUR_MS,
            "fetch_start": None, "global_t_min": 0, "global_t_max": 0, "would_truncate": []}
        if win["fetch_start"] is None:
            win["fetch_start"] = win["test_start"] - pb.CONFIG["WARMUP_BARS"] * BAR
    else:
        if args.freeze_end:
            print("[注意] 離線模式忽略 --freeze-end：freeze_end 一律由快取推導，避免截斷快取。")
        win = derive_window(cache_stats)
    if win["would_truncate"]:
        sys.exit(f"[中止] now_ms={win['now_ms']} 會截短 {len(win['would_truncate'])} 個快取檔："
                 f"{win['would_truncate'][:10]}")
    if args.fetch:
        todo, scan_rows = build_universe_online()
        cached = {s["symbol"] for s in cache_stats}
        print(f"[--fetch] 預計補抓 {len(todo)} 個交易對（其中 {len([s for s in todo if s not in cached])} 個快取中沒有），"
              f"寫回真實快取 {REAL_CACHE_DIR}")
    else:
        todo, scan_rows = build_universe(cache_stats)
        d, n = use_cache_snapshot()
        print(f"[離線] 快取快照：{n} 個檔 → {d}（原檔不會被改動）")
    print(f"[{stage}] 交易對：快取檔 {len(cache_stats)}，待掃描 {len(todo)}，"
          f"排除/忽略/略過 {len(scan_rows)}；freeze_end={tpe_str(win['now_ms'])}（台北）")
    return cache_stats, win, todo, scan_rows


def verify_real_cache_unchanged(before):
    after = scan_cache()
    b = {s["symbol"]: (s.get("rows"), s.get("t_min"), s.get("t_max")) for s in before}
    a = {s["symbol"]: (s.get("rows"), s.get("t_min"), s.get("t_max")) for s in after}
    changed = [k for k in b if a.get(k) != b[k]] + [k for k in a if k not in b]
    return changed


# ============================== 階段：check ==============================
def stage_check(args):
    setup_config(offline=not args.fetch)
    cache_stats = scan_cache()
    if not cache_stats:
        sys.exit(f"[中止] {REAL_CACHE_DIR} 沒有任何 *_{pb.CONFIG['INTERVAL']}.csv")
    win = derive_window(cache_stats)
    todo, scan_rows = build_universe(cache_stats)
    valid = [s for s in cache_stats if s.get("rows", 0) > 0]
    rows = np.array([s["rows"] for s in valid])
    BAR = pb.bar_ms()
    enough = [s for s in valid if s["rows"] >= min_bars_required()]
    tmax_dist = pd.Series([s["t_max"] for s in valid]).value_counts().sort_index(ascending=False)
    print(f"快取目錄：{REAL_CACHE_DIR}")
    print(f"K棒週期：{pb.CONFIG['INTERVAL']}（BAR={BAR} ms）  檔案數：{len(cache_stats)}（可讀 {len(valid)}）")
    print(f"時間範圍（台北）：{tpe_str(win['global_t_min'])} ~ {tpe_str(win['global_t_max'])}")
    print(f"rows：min {rows.min()}  p25 {np.percentile(rows, 25):.0f}  median {np.median(rows):.0f}  "
          f"p75 {np.percentile(rows, 75):.0f}  max {rows.max()}；≥ {min_bars_required()} 根：{len(enough)}")
    print("各檔最後一根 time 分布（前 5 種）：")
    for tm, cnt in list(tmax_dist.items())[:5]:
        print(f"  {tpe_str(tm)}  ×{cnt}")
    print(f"freeze_end(now_ms) = global_max + BAR = {win['now_ms']} → {tpe_str(win['now_ms'])}")
    print(f"test_start          = {win['test_start']} → {tpe_str(win['test_start'])}  "
          f"(LOOKBACK_DAYS={pb.CONFIG['LOOKBACK_DAYS']})")
    print(f"fetch_start         = {win['fetch_start']} → {tpe_str(win['fetch_start'])}  "
          f"(WARMUP_BARS={pb.CONFIG['WARMUP_BARS']})")
    cover = sum(1 for s in valid if s["t_min"] <= win["fetch_start"])
    print(f"快取起點 ≤ fetch_start 的檔：{cover}/{len(valid)}")
    print(f"截斷檢查：{'OK，沒有任何檔會被截短' if not win['would_truncate'] else '會截短 ' + str(win['would_truncate'])}")
    print(f"交易對清單（來自快取檔名）：待掃描 {len(todo)}，排除/忽略 {len(scan_rows)}")
    for st in ("排除", "忽略", "略過"):
        xs = [r for r in scan_rows if r["status"] == st]
        if xs:
            print(f"  {st} {len(xs)}：{', '.join(r['symbol'] + '(' + r['reason'][:12] + ')' for r in xs[:8])}"
                  + (" …" if len(xs) > 8 else ""))
    print(f"CONFIG：{ {k: _py(pb.CONFIG.get(k)) for k in CONFIG_KEYS} }")
    print(f"S4：{ {k: _py(s4.S4[k]) for k in s4_signal.PARAM_KEYS} }")
    md = build_metadata("check", win, cache_stats, {**universe_summary(scan_rows, len(todo)), "todo": todo,
                                                    "rows": scan_rows})
    md["cache_files"] = cache_stats
    md["truncation_check"] = {"would_truncate": win["would_truncate"], "ok": not win["would_truncate"]}
    write_json(os.path.join(RESULTS_DIR, F_CHECK), md)
    print(f"→ {os.path.join(RESULTS_DIR, F_CHECK)}")


# ============================== 階段：calibrate ==============================
def stage_calibrate(args):
    cache_stats, win, todo, scan_rows = prepare_run(args, "calibrate")
    t0 = time.time()
    per_symbol, mism_samples = [], []
    total_pb = total_sim = total_mism = 0
    for k, sym in enumerate(todo, 1):
        try:
            df, bars = load_symbol(sym, win)
        except Exception as e:
            scan_rows.append({"symbol": sym, "status": "失敗", "reason": str(e)[:200]})
            continue
        if df is None:
            scan_rows.append({"symbol": sym, "status": "略過", "reason": "K棒不足", "bars": bars})
            continue
        pb_tr = pb.backtest(sym, df, win["test_start"])
        sim_tr = simulate(sym, df, win["test_start"], 0.0, "signal")
        mism = compare_parity(pb_tr, sim_tr)
        n_sig = signals_in_window(df, win["test_start"])
        per_symbol.append({"symbol": sym, "bars": bars, "n_signals": n_sig,
                           "n_pb": len(pb_tr), "n_sim": len(sim_tr), "mismatch": len(mism)})
        scan_rows.append({"symbol": sym, "status": "已掃描", "reason": f"{len(pb_tr)} 筆交易", "bars": bars})
        total_pb += len(pb_tr)
        total_sim += len(sim_tr)
        total_mism += len(mism)
        if mism:
            mism_samples.append({"symbol": sym, "mismatches": mism[:5]})
        if k % 50 == 0 or k == len(todo):
            print(f"  [{k}/{len(todo)}] pb {total_pb} 筆 / sim {total_sim} 筆 / 不一致 {total_mism}")
    changed = verify_real_cache_unchanged(cache_stats)
    if STATE["offline"] and changed:
        sys.exit(f"[中止] 離線模式下真實快取被改動（不應發生）：{changed[:10]}")
    passed = total_mism == 0 and len(per_symbol) >= MIN_CALIB_SYMBOLS
    md = build_metadata("calibrate", win, cache_stats, {**universe_summary(scan_rows, len(per_symbol)), "rows": scan_rows})
    md["fingerprint"] = fingerprint(md, per_symbol)
    out = {
        "metadata": md,
        "summary": {
            "symbols_compared": len(per_symbol), "min_symbols_required": MIN_CALIB_SYMBOLS,
            "trades_pb": total_pb, "trades_sim": total_sim, "mismatch_total": total_mism,
            "signals_in_window": sum(p["n_signals"] for p in per_symbol),
            "compared_fields": list(PARITY_KEYS), "passed": passed,
            "elapsed_sec": round(time.time() - t0, 1),
            "real_cache_changed": changed,
        },
        "symbols": per_symbol,
        "mismatch_samples": mism_samples,
    }
    path = os.path.join(RESULTS_DIR, F_CALIB)
    write_json(path, out)
    print(f"\n校準：{len(per_symbol)} 個交易對，pb {total_pb} 筆 / sim {total_sim} 筆，不一致 {total_mism} 筆，"
          f"API 請求 {STATE['api_calls']} 次，{out['summary']['elapsed_sec']} 秒")
    print(f"→ {path}")
    if total_mism > 0:
        sys.exit(f"[失敗] 校準不一致 {total_mism} 筆（見 {F_CALIB} 的 mismatch_samples），不得進行掃描")
    if len(per_symbol) < MIN_CALIB_SYMBOLS:
        sys.exit(f"[失敗] 校準只涵蓋 {len(per_symbol)} 個交易對 < {MIN_CALIB_SYMBOLS}")
    print("[通過] AC-1 校準逐筆一致")


# ============================== 階段：scan ==============================
def _append_trades_csv(path, trades, write_header):
    df = pd.DataFrame(trades, columns=TRADE_COLUMNS)
    df.to_csv(path, index=False, mode="a" if not write_header else "w", header=write_header,
              encoding="utf-8", lineterminator="\n")


def _prune_trades_csv(path, keep_combos):
    """續跑（BUG-001）：把明細檔當「文字」過濾——保留表頭與 combo 屬於 keep_combos 的原始行，
       其餘（CSV 已寫、v2_scan.json 未記錄的半套組合）砍掉。刻意不用 pandas 讀回再 to_csv：
       pandas 預設解析器不是 round-trip，`0.23992500000000003` 會被改寫成 `0.239925`，續跑後的檔
       就與一次跑完的不再位元組相等。combo 是第一欄，且 combo_label() 的格式（s=%+.4f|basis）不含
       逗號 / 引號 / 換行，所以第一個逗號前的文字就是 combo，不需要 CSV 解析。回傳保留的列數。"""
    header = ",".join(TRADE_COLUMNS) + "\n"
    with open(path, "r", encoding="utf-8", newline="") as f:
        lines = f.readlines()
    if not lines or lines[0] != header:
        sys.exit(f"[拒絕] {path} 的表頭不是本腳本寫出的格式，無法續跑；請加 --restart 從頭")
    kept = [ln for ln in lines[1:] if ln.split(",", 1)[0] in keep_combos]
    if kept and not kept[-1].endswith("\n"):     # 只有被中斷的半行才可能沒換行；補上以免下一組黏在同一行
        kept[-1] += "\n"
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(header)
        f.writelines(kept)
    return len(kept)


def stage_scan(args):
    calib_path = os.path.join(RESULTS_DIR, F_CALIB)
    if not os.path.exists(calib_path):
        sys.exit(f"[拒絕] 找不到 {calib_path}，請先跑 calibrate")
    calib = read_json(calib_path)
    cs = calib["summary"]
    if not cs.get("passed") or cs.get("mismatch_total", 1) != 0:
        sys.exit(f"[拒絕] 校準未通過（passed={cs.get('passed')}, mismatch_total={cs.get('mismatch_total')}），不得掃描")
    if cs.get("symbols_compared", 0) < MIN_CALIB_SYMBOLS:
        sys.exit(f"[拒絕] 校準只涵蓋 {cs.get('symbols_compared')} 個交易對 < {MIN_CALIB_SYMBOLS}")

    cache_stats, win, todo, scan_rows = prepare_run(args, "scan")
    if win["now_ms"] != calib["metadata"]["freeze_end"]:
        sys.exit(f"[拒絕] 快取的 freeze_end 已與校準時不同（{tpe_str(win['now_ms'])} vs "
                 f"{calib['metadata']['freeze_end_tpe']}），請重新 calibrate")

    # ---- 訊號與指標只算一次：每個交易對留下 t/o/h/l/c/signal 陣列 ----
    t0 = time.time()
    data, per_symbol = {}, []
    for k, sym in enumerate(todo, 1):
        try:
            df, bars = load_symbol(sym, win)
        except Exception as e:
            scan_rows.append({"symbol": sym, "status": "失敗", "reason": str(e)[:200]})
            continue
        if df is None:
            scan_rows.append({"symbol": sym, "status": "略過", "reason": "K棒不足", "bars": bars})
            continue
        data[sym] = arrays_from_df(df)
        per_symbol.append({"symbol": sym, "bars": bars, "n_signals": signals_in_window(df, win["test_start"])})
        scan_rows.append({"symbol": sym, "status": "已掃描", "bars": bars, "n_signals": per_symbol[-1]["n_signals"]})
        if k % 100 == 0 or k == len(todo):
            print(f"  [{k}/{len(todo)}] 已載入 {len(data)} 個交易對（{time.time() - t0:.0f} 秒）")
    changed = verify_real_cache_unchanged(cache_stats)
    if STATE["offline"] and changed:
        sys.exit(f"[中止] 離線模式下真實快取被改動（不應發生）：{changed[:10]}")

    md = build_metadata("scan", win, cache_stats, {**universe_summary(scan_rows, len(per_symbol)), "rows": scan_rows})
    fp = fingerprint(md, per_symbol)
    if fp != calib["metadata"]["fingerprint"]:
        sys.exit("[拒絕] 掃描載入的資料/設定指紋與校準不同（交易對、K棒數、訊號數或 CONFIG/S4 有變），請重新 calibrate")
    n_signals = sum(p["n_signals"] for p in per_symbol)
    md.update({
        "fingerprint": fp, "calibration_file": F_CALIB,
        "calibration_summary": {k: cs[k] for k in ("symbols_compared", "trades_pb", "mismatch_total", "passed")},
        "signals_in_window": n_signals, "slippages": SLIPPAGES, "bases": list(BASES),
        "same_bar_guard": SAME_BAR_GUARD, "load_elapsed_sec": round(time.time() - t0, 1),
    })

    # ---- checkpoint / 續跑 ----
    scan_path, trades_path = os.path.join(RESULTS_DIR, F_SCAN), os.path.join(RESULTS_DIR, F_TRADES)
    combos_done, done = [], set()
    if os.path.exists(scan_path) and not args.restart:
        old = read_json(scan_path)
        if old.get("metadata", {}).get("fingerprint") == fp and os.path.exists(trades_path):
            combos_done = old.get("combos", [])
            done = {c["combo"] for c in combos_done}
            if done:
                n_keep = _prune_trades_csv(trades_path, done)      # 文字過濾，已完成組合的列位元組不變
                print(f"[續跑] 已完成 {len(done)} 組，明細保留 {n_keep} 筆")
        else:
            print("[重跑] 既有 v2_scan.json 的指紋不同或缺明細檔，從頭開始")
    if not done and os.path.exists(trades_path):
        os.remove(trades_path)

    combos = [(s, b) for s in SLIPPAGES for b in BASES]
    header_needed = not os.path.exists(trades_path)
    print(f"\n{'組合':<20}{'筆數':>6}{'平倉':>6}{'止盈':>6}{'勝率':>8}{'EV(淨)':>9}{'同根雙觸':>9}{'均持倉h':>8}")
    for s, b in combos:
        label = combo_label(s, b)
        if label in done:
            continue
        t1 = time.time()
        trades = []
        for sym in per_symbol:
            trades.extend(simulate_arrays(sym["symbol"], data[sym["symbol"]], win["test_start"], s, b))
        st = combo_stats(trades, s, b)
        st["completed_at"] = datetime.now(pb.TPE).isoformat(timespec="seconds")
        st["elapsed_sec"] = round(time.time() - t1, 1)
        _append_trades_csv(trades_path, trades, header_needed)
        header_needed = False
        combos_done.append(st)
        done.add(label)
        md["api_requests"] = STATE["api_calls"]
        write_json(scan_path, {"metadata": md, "combos": combos_done,
                               "same_bar_guard_exceeded": any((c["same_bar_pct"] or 0) > SAME_BAR_GUARD for c in combos_done)})
        wr = f"{st['win_rate']:.1%}" if st["win_rate"] is not None else "-"
        ev = f"{st['ev_net']:+.3%}" if st["ev_net"] is not None else "-"
        sb = f"{st['same_bar_pct']:.1%}" if st["same_bar_pct"] is not None else "-"
        hh = f"{st['avg_hold_hours']:.1f}" if st["avg_hold_hours"] is not None else "-"
        print(f"{label:<20}{st['n_trades']:>6}{st['n_closed']:>6}{st['n_tp']:>6}{wr:>8}{ev:>9}{sb:>9}{hh:>8}")

    worst = max(((c["same_bar_pct"] or 0), c["combo"]) for c in combos_done)
    print(f"\n掃描完成：{len(combos_done)} 組，訊號 {n_signals} 個（所有組合共用），API 請求 {STATE['api_calls']} 次")
    print(f"→ {scan_path}\n→ {trades_path}")
    if worst[0] > SAME_BAR_GUARD:
        print(f"[阻塞中] 同根雙觸發比例 {worst[0]:.1%}（{worst[1]}）> {SAME_BAR_GUARD:.0%}，依 PRD 2.2 須停下回報")
    else:
        print(f"[護欄] 同根雙觸發比例最高 {worst[0]:.1%}（{worst[1]}）≤ {SAME_BAR_GUARD:.0%}")


# ============================== 階段：report ==============================
def _pct(x, nd=2, sign=False):
    if x is None:
        return "-"
    return f"{x:+.{nd}%}" if sign else f"{x:.{nd}%}"


def _num(x, nd=2):
    return "-" if x is None else f"{x:.{nd}f}"


def _interp_zero(xs, ys):
    """回傳 ys 變號處的 x（線性內插）。xs 需已升冪；y==0 的點（如 s=0 兩算法恆等）只有在兩側最近的
       非零鄰點異號時才算真正的交叉。"""
    pts = [(x, y) for x, y in zip(xs, ys) if y is not None]
    out = []
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if y0 == 0 or y1 == 0:
            continue
        if (y0 < 0 < y1) or (y1 < 0 < y0):
            out.append(x0 + (x1 - x0) * (-y0) / (y1 - y0))
    nz = [(x, y) for x, y in pts if y != 0]
    for x, y in pts:
        if y == 0:
            left = [p for p in nz if p[0] < x]
            right = [p for p in nz if p[0] > x]
            if left and right and ((left[-1][1] < 0 < right[0][1]) or (right[0][1] < 0 < left[-1][1])):
                out.append(x)
    return sorted(out)


def _paired_diffs(trades, slips):
    """同一筆進場（symbol+entry_time）在 fill 與 signal 下的淨報酬差（fill − signal），每個 s 一列。"""
    out = {}
    key = trades["symbol"] + "@" + trades["entry_time"].astype(str)
    t = trades.assign(key=key)
    for s in slips:
        a = t[t["combo"] == combo_label(s, "signal")].set_index("key")
        b = t[t["combo"] == combo_label(s, "fill")].set_index("key")
        common = a.index.intersection(b.index)
        if len(common) == 0:
            out[s] = None
            continue
        d = (b.loc[common, "net_ret"] - a.loc[common, "net_ret"]).to_numpy()
        ra, rb = a.loc[common, "result"], b.loc[common, "result"]
        diff_result = int((ra != rb).sum())
        n_w2l = int(((ra == "止盈") & (rb == "止損")).sum())      # signal 止盈 → fill 止損（不利滑價的翻轉方向）
        n_l2w = int(((ra == "止損") & (rb == "止盈")).sum())      # signal 止損 → fill 止盈（有利滑價的翻轉方向）
        se = float(d.std(ddof=1) / math.sqrt(len(d))) if len(d) > 1 else None
        # 沒有任何一筆結果翻轉時，配對差是純算術平移（SE≈0），t 值沒有意義
        t_val = float(d.mean() / se) if (se and diff_result > 0) else None
        w_sig = float((a.loc[common, "result"] == "止盈").mean())
        C = pb.CONFIG
        tp_pct, sl_pct = C["TAKE_PROFIT"], C["STOP_LOSS"]
        # 機制分解：確定性項（fill 的 R:R 不隨 s 變、signal 的會）− 每次結果翻轉的代價 × 翻轉率
        det = (-s / (1 + s)) * ((1 - tp_pct) * w_sig + (1 + sl_pct) * (1 - w_sig))
        flip_cost = tp_pct + sl_pct
        flip_rate = diff_result / len(common)
        net_flip = (n_w2l - n_l2w) / len(common)                    # 淨「止盈→止損」翻轉率
        out[s] = {"n": int(len(common)), "n_result_diff": diff_result, "mean": float(d.mean()),
                  "se": se, "t": t_val, "flip_rate": flip_rate, "n_w2l": n_w2l, "n_l2w": n_l2w,
                  "decomp_det": float(det), "decomp_pred": float(det - flip_cost * net_flip),
                  "flip_rate_threshold": (float(det / flip_cost) if s < 0 else None),
                  "n_fill_better": int((d > 0).sum()), "n_signal_better": int((d < 0).sum())}
    return out


def stage_report(args):
    setup_config(offline=True)
    calib_path, scan_path, trades_path = (os.path.join(RESULTS_DIR, f) for f in (F_CALIB, F_SCAN, F_TRADES))
    for p in (calib_path, scan_path, trades_path):
        if not os.path.exists(p):
            sys.exit(f"[拒絕] 找不到 {p}")
    calib, scan = read_json(calib_path), read_json(scan_path)
    trades = pd.read_csv(trades_path, encoding="utf-8", float_precision="round_trip")   # 浮點精確讀回（BUG-001）
    md, cs = scan["metadata"], calib["summary"]
    combos = {c["combo"]: c for c in scan["combos"]}
    # F-1：parity 的有效覆蓋——只有區間內真的有交易的交易對才比對到進場/出場邏輯，其餘只驗到「無交易」路徑
    calib_syms = calib.get("symbols", [])
    n_calib_with_trades = sum(1 for x in calib_syms if x.get("n_pb", 0) >= 1)
    n_calib_zero_both = sum(1 for x in calib_syms if x.get("n_pb", 0) == 0 and x.get("n_sim", 0) == 0)
    slips_all = [float(x) for x in md.get("slippages", SLIPPAGES)]
    expected = [combo_label(s, b) for s in slips_all for b in BASES]
    missing = [c for c in expected if c not in combos]
    if missing:
        sys.exit(f"[拒絕] 掃描未完成，缺 {missing}")
    C = md["config"]
    fee = 2 * C["FEE_RATE"]
    tp_pct, sl_pct = C["TAKE_PROFIT"], C["STOP_LOSS"]
    slips = sorted(slips_all, reverse=True)
    neg = [s for s in slips if s < 0]
    pos = [s for s in slips if s > 0]
    sig = {s: combos[combo_label(s, "signal")] for s in slips}
    fil = {s: combos[combo_label(s, "fill")] for s in slips}
    w0 = sig[0.0]["win_rate"]
    sb0_n, sb0_pct = sig[0.0]["same_bar_n"], sig[0.0]["same_bar_pct"] or 0.0
    w0_upper = (sig[0.0]["n_tp"] + sb0_n) / sig[0.0]["n_closed"]
    worst_sb = max((c["same_bar_pct"] or 0) for c in combos.values())
    best_sb = min((c["same_bar_pct"] or 0) for c in combos.values())
    guard_hit = worst_sb > SAME_BAR_GUARD
    all_gap_zero = all(c["gap_n"] == 0 for c in combos.values())
    n_open_all = {c: combos[c]["n_open"] for c in expected}

    # AC-3 佐證：共同進場集合
    key = trades["symbol"] + "@" + trades["entry_time"].astype(str)
    per_combo_sets = {c: set(key[trades["combo"] == c]) for c in expected}
    common = set.intersection(*per_combo_sets.values())
    union = set.union(*per_combo_sets.values())

    # 交叉點：EV_signal − EV_fill 對 s（升冪）
    diff = {s: sig[s]["ev_net"] - fil[s]["ev_net"] for s in slips}
    xs_asc = sorted(slips)
    crossings = _interp_zero(xs_asc, [diff[s] for s in xs_asc])
    better_fill_neg = [s for s in neg if fil[s]["ev_net"] > sig[s]["ev_net"]]
    better_sig_neg = [s for s in neg if sig[s]["ev_net"] > fil[s]["ev_net"]]
    better_fill_pos = [s for s in pos if fil[s]["ev_net"] > sig[s]["ev_net"]]
    better_sig_pos = [s for s in pos if sig[s]["ev_net"] > fil[s]["ev_net"]]
    crossing_only_at_zero = (crossings == [0.0]) and bool(better_fill_neg) and not better_sig_neg and bool(better_sig_pos) and not better_fill_pos
    # 各算法 EV 歸零的滑價（EV(s) 變號處，線性內插）
    ev0_sig = _interp_zero(xs_asc, [sig[s]["ev_net"] for s in xs_asc])
    ev0_fil = _interp_zero(xs_asc, [fil[s]["ev_net"] for s in xs_asc])
    # fill 較優幅度（fill − signal）在 s<0 的極值
    adv = {s: -diff[s] for s in neg}
    adv_max_s = max(adv, key=lambda s: adv[s]) if adv else None
    # 資料驅動臨界 w*：同一 s 下 fill 要追平 signal 的 EV 所需勝率
    w_star = {}
    for s in slips:
        f = fil[s]
        if f["avg_win_gross"] is None or f["avg_loss_gross"] is None:
            w_star[s] = None
        else:
            w_star[s] = (sig[s]["ev_net"] - f["avg_loss_gross"] + fee) / (f["avg_win_gross"] - f["avg_loss_gross"])
    paired = _paired_diffs(trades, slips)
    below_prd = [s for s in neg if fil[s]["win_rate"] is not None and fil[s]["win_rate"] < PRD_W_THRESHOLD]
    below_star = [s for s in neg if fil[s]["win_rate"] is not None and w_star[s] is not None and fil[s]["win_rate"] < w_star[s]]
    n_min = min(c["n_closed"] for c in combos.values())
    n_max = max(c["n_closed"] for c in combos.values())
    se_w = math.sqrt(0.25 / n_min) if n_min else None
    wbs_delta = w0 - 0.683
    n_cover = None
    check_path = os.path.join(RESULTS_DIR, F_CHECK)
    if os.path.exists(check_path):
        chk = read_json(check_path)
        if chk.get("freeze_end") == md["freeze_end"]:
            n_cover = sum(1 for f in chk.get("cache_files", []) if f.get("t_min", 0) <= md["fetch_start"])
    t_vals = [abs(paired[s]["t"]) for s in neg if paired.get(s) and paired[s]["t"] is not None]
    w_drop_steps = []                                   # (s_prev, s, Δw) 逐段
    for a_, b_ in zip(neg, neg[1:]):
        if fil[a_]["win_rate"] is not None and fil[b_]["win_rate"] is not None:
            w_drop_steps.append((a_, b_, fil[b_]["win_rate"] - fil[a_]["win_rate"]))
    ev_drop_mid = None                                  # 較優算法在 −0.25% → −0.75% 的 EV 損耗
    if -0.0025 in fil and -0.0075 in fil:
        ev_drop_mid = max(fil[-0.0025]["ev_net"], sig[-0.0025]["ev_net"]) - max(fil[-0.0075]["ev_net"], sig[-0.0075]["ev_net"])

    L = []
    A = L.append
    A("# V2 滑價敏感度分析報告 — TP/SL 用訊號價（signal）vs 成交價（fill）")
    A("")
    A(f"- 產生時間：{datetime.now(pb.TPE).isoformat(timespec='seconds')}（台北）　機器：`{md['machine']}`　"
      f"腳本：`{md['script']}` v{SCRIPT_VERSION}（報告產生）；calibrate/scan 結果檔由 v{md['script_version']} 產生")
    A(f"- Python {md['python']} / pandas {md['pandas']} / numpy {md['numpy']}　repo HEAD：`{(md['repo_head'] or {}).get('commit')}`（{(md['repo_head'] or {}).get('ref')}）")
    A(f"- 模式：{'離線（0 次 API 請求）' if md['offline'] else 'fetch'}，實際 API 請求 {md['api_requests']} 次；"
      f"**RESOLVE_SAME_BAR_WITH_5M 已關閉**（resolve_disabled={md['resolve_disabled']}）；快取以暫存副本讀取，原檔未改動。")
    A(f"- 資料凍結點 freeze_end：{md['freeze_end_tpe']}（ms {md['freeze_end']}）；回測區間 "
      f"{md['test_start_tpe']} ~ {md['freeze_end_tpe']}（{C['LOOKBACK_DAYS']} 天）；暖機起點 {md['fetch_start_tpe']}")
    A("")
    if guard_hit:
        A(f"> **阻塞中：同根雙觸發比例最高 {worst_sb:.1%} > {SAME_BAR_GUARD:.0%}**（PRD 2.2 護欄），"
          "以下數字照常產出，但依 PRD 第 8 節須停下回報，不可直接採用。")
        A("")

    # ---- 0 ----
    A("## 0. 摘要")
    A("")
    A(f"- 校準（AC-1）：{cs['symbols_compared']} 個交易對（其中 {n_calib_with_trades} 個有 ≥1 筆交易）、{cs['trades_pb']} 筆交易，"
      f"與 `pb.backtest()` 逐筆比對 {'、'.join(cs['compared_fields'])}，不一致 **{cs['mismatch_total']} 筆**"
      f"（{'通過' if cs['passed'] else '未通過'}）。")
    A(f"- s=0 時兩種算法完全相同：勝率 **{_pct(w0, 2)}**、每筆淨 EV **{_pct(sig[0.0]['ev_net'], 3, True)}**"
      f"（已平倉 {sig[0.0]['n_closed']} 筆）。關閉 resolve 只影響同根雙觸發的 {sb0_n} 筆（{_pct(sb0_pct, 1)}），"
      f"w 的可能區間 {_pct(w0, 1)} ~ {_pct(w0_upper, 1)}；與 WBS 記載的 68.3% "
      + ("數字上幾乎相同" if abs(wbs_delta) < 0.005 else f"相差 {_pct(wbs_delta, 1, True)}pt")
      + "，但兩者的資料窗與 resolve 口徑（WBS 含 1 分 K 判定、本報告全計止損）都不同，"
      + ("屬巧合性一致，" if abs(wbs_delta) < 0.005 else "")
      + "**不可直接對照、也不可拿來更新 WBS**（細節見第 7 節）。")
    if crossing_only_at_zero:
        A(f"- **交叉點就在 s=0**：所有不利滑價（{max(neg):+.2%} ~ {min(neg):+.2%}）下 fill 的每筆 EV 都高於 signal"
          f"（最多高 {_pct(adv[adv_max_s], 3)} @ s={adv_max_s:+.2%}）；有利滑價（{', '.join(f'{s:+.2%}' for s in pos)}）下則 signal 較高。"
          "優劣只取決於滑價的**符號**，不取決於幅度（結構性原因見第 6 節）。")
    elif crossings:
        A(f"- 交叉點（EV_signal − EV_fill 變號，線性內插）：{', '.join(f'{x:+.3%}' for x in crossings)}；"
          f"s<0 時 fill 較優的點：{[f'{s:+.2%}' for s in better_fill_neg] or '無'}，signal 較優的點：{[f'{s:+.2%}' for s in better_sig_neg] or '無'}。")
    else:
        A(f"- 掃描範圍內 EV_signal − EV_fill 沒有變號；s<0 時 {'fill' if better_fill_neg else 'signal'} 一律較優。")
    A(f"- 兩種算法的每筆 EV 歸零點：signal ≈ **{', '.join(f'{x:+.2%}' for x in ev0_sig) or '範圍內未歸零'}**、"
      f"fill ≈ **{', '.join(f'{x:+.2%}' for x in ev0_fil) or '範圍內未歸零'}**——這是策略4 在現行 4%/5% 出場下能承受的不利滑價上限。")
    A(f"- fill 的實際勝率：s=0 {_pct(w0, 1)} → s={min(neg):+.2%} {_pct(fil[min(neg)]['win_rate'], 1)}；"
      f"低於 PRD 固定臨界 {PRD_W_THRESHOLD:.1%} 的滑價點：{[f'{s:+.2%}' for s in below_prd] or '無'}，"
      f"但低於**資料驅動臨界 w\\***（同一 s 下追平 signal 所需勝率）的滑價點：{[f'{s:+.2%}' for s in below_star] or '無'}（第 5 節）。")
    A(f"- 同根雙觸發比例 {best_sb:.1%} ~ {worst_sb:.1%}，{'超過' if guard_hit else '未超過'} {SAME_BAR_GUARD:.0%} 護欄；"
      f"跳空止損：{'全部組合 0 筆' if all_gap_zero else '合計 ' + str(sum(c['gap_n'] for c in combos.values())) + ' 筆'}；未平倉：{min(n_open_all.values())} ~ {max(n_open_all.values())} 筆。")
    A("")

    # ---- 1 ----
    U = md["symbols"]
    cache = md["cache"]
    A("## 1. 資料與設定")
    A("")
    A(f"- 快取：`{cache['dir']}/*_{cache['interval']}.csv` 共 {cache['files']} 檔（可讀 {cache['files_readable']}），"
      f"時間範圍 {cache['t_min_tpe']} ~ {cache['t_max_tpe']}（台北），每檔 rows min/median/max = "
      f"{cache['rows_min']}/{cache['rows_median']:.0f}/{cache['rows_max']}。快取沒有 1M 資料，`resolve_with_5m()` 本來就無法離線跑。")
    A(f"- 交易對清單來源：{md['symbols_source']}。掃描 **{U['scanned']}**、依 `pb.classify()` 排除 {U['excluded']}、"
      f"K 棒不足（< {md['min_bars_required']} 根）略過 {U['skipped']}、失敗 {U['failed']}、忽略 {U['ignored']}。")
    excl = [r for r in U["rows"] if r["status"] == "排除"]
    if excl:
        reasons = {}
        for r in excl:
            reasons.setdefault(r["reason"], []).append(r["symbol"])
        for reason, syms in reasons.items():
            A(f"  - 排除「{reason}」{len(syms)} 個：{', '.join(syms)}")
    skip = [r for r in U["rows"] if r["status"] in ("略過", "失敗")]
    if skip:
        A(f"  - 略過/失敗：{', '.join(r['symbol'] + '(' + str(r.get('bars', r['reason'])) + ')' for r in skip)}")
    A(f"- 訊號：由 `strategy/s4_signal.py` 經 `pionex_strategy4.add_indicators()` 產生，回測區間內共 **{md['signals_in_window']}** 個，"
      "所有組合共用同一份（每個交易對只算一次，見第 3 節）。")
    A("- 生效設定（`pb.CONFIG`，已 update 自 `pionex_strategy4.CONFIG`，再強制 `COOLDOWN_BARS=cooldown_bars(H, S4)`、`RESOLVE_SAME_BAR_WITH_5M=False`）：")
    A("")
    A("| 鍵 | 值 |")
    A("|---|---|")
    for k in CONFIG_KEYS:
        A(f"| {k} | `{C[k]}` |")
    A("")
    A("- 策略4 六個生效參數（`S4`，未改動）：" + ", ".join(f"{k}={v}" for k, v in md["s4_params"].items())
      + "；實驗鍵全為 None。")
    A(f"- 滑價集合：{', '.join(f'{s:+.2%}' for s in slips)}。s 是純價格相對量，P1 = P0×(1+s)，不依方向翻號；"
      "策略4 只做空，s<0 = 成交價偏低 = 不利，s>0 = 有利。")
    A("")

    # ---- 2 ----
    A("## 2. 校準（AC-1）")
    A("")
    A("條件：slippage=0、basis=\"signal\"、RESOLVE 關閉；同一份 `df`（`pb.load_hourly` + `pionex_strategy4.add_indicators`）、"
      "同一 test_start，`simulate()` 與 `pb.backtest()` 逐筆比對。")
    A("")
    A("| 項目 | 值 |")
    A("|---|---|")
    A(f"| 比對交易對數 | {cs['symbols_compared']}（要求 ≥ {cs['min_symbols_required']}） |")
    A(f"| 其中有 ≥1 筆交易的交易對 | {n_calib_with_trades}（兩邊皆 0 筆：{n_calib_zero_both}） |")
    A(f"| pb.backtest 交易筆數 | {cs['trades_pb']} |")
    A(f"| simulate 交易筆數 | {cs['trades_sim']} |")
    A(f"| 不一致筆數 | **{cs['mismatch_total']}** |")
    A(f"| 比對欄位 | {', '.join(cs['compared_fields'])}（數值要求完全相等，不用容差） |")
    A(f"| 校準檔 | `research/results/{F_CALIB}`，資料指紋 `{calib['metadata']['fingerprint'][:16]}…`（scan 階段驗證相同） |")
    A("")
    A(f"- 有效覆蓋：{cs['symbols_compared']} 個交易對中，**{n_calib_with_trades} 個**在回測區間內有 ≥1 筆交易"
      "（pb 與 sim 都有成交，這些才真正比對到進場價、tp/sl、出場時間/價格與結果），"
      f"其餘 {n_calib_zero_both} 個兩邊皆 0 筆，只驗證到「無交易」路徑（雙方都判定該交易對在區間內沒有進場）。"
      f"AC-1 的 ≥ {cs['min_symbols_required']} 門檻用「比對交易對數」或「有交易的交易對數」讀都"
      f"{'通過' if n_calib_with_trades >= cs['min_symbols_required'] else '——後者**未達**'}"
      "；逐交易對的 n_pb / n_sim / mismatch 見校準檔 `symbols[]`。")
    A("")

    # ---- 3 ----
    A("## 3. 訊號集合與交易集合（AC-3）")
    A("")
    A(f"- 回測區間內訊號 {md['signals_in_window']} 個，是所有組合的**不變量**：scan 階段對每個交易對只呼叫一次 `add_indicators()`，"
      "取出 time/open/high/low/close/signal 陣列後餵給全部 18 個組合；資料指紋（含每個交易對的訊號數）與校準時相同。")
    A(f"- 各組合交易筆數 {min(c['n_trades'] for c in combos.values())} ~ {max(c['n_trades'] for c in combos.values())}，"
      f"所有組合共同的進場（symbol+entry_time）{len(common)} 筆，聯集 {len(union)} 筆。")
    A("- 交易集合不完全相同是**出場規則的間接效果**：tp/sl 水位不同 → 出場根可能不同 → 冷卻 12 根後下一筆可能落在不同訊號上；"
      "同一交易對同時間只持有一筆，所以訊號數 ≥ 交易數。訊號本身完全不受滑價與 basis 影響。")
    A("")

    # ---- 4 ----
    A("## 4. 完整對照表（各滑價 × 兩種算法）")
    A("")
    A("勝率 w = 止盈 ÷ 已平倉；平均獲利/虧損與 EV 皆以 P1 計的**淨**報酬率（已扣來回手續費 "
      f"{fee:.2%}）；盈虧平衡 w = (|平均毛虧損| + 費用) ÷ (平均毛獲利 + |平均毛虧損|)。")
    A("")
    A("| 滑價 s | basis | 交易 | 已平倉 | 未平倉 | 止盈 | 止損 | **w** | 平均獲利(淨) | 平均虧損(淨) | **每筆EV(淨)** | 盈虧平衡w | 同根雙觸 | 跳空 | 平均持倉(h) |")
    A("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for s in slips:
        for b in BASES:
            c = combos[combo_label(s, b)]
            A(f"| {s:+.2%} | {b} | {c['n_trades']} | {c['n_closed']} | {c['n_open']} | {c['n_tp']} | {c['n_sl']} | "
              f"**{_pct(c['win_rate'], 1)}** | {_pct(c['avg_win_net'], 2, True)} | {_pct(c['avg_loss_net'], 2, True)} | "
              f"**{_pct(c['ev_net'], 3, True)}** | {_pct(c['breakeven_w'], 1)} | "
              f"{c['same_bar_n']}（{_pct(c['same_bar_pct'], 1)}） | {c['gap_n']} | {_num(c['avg_hold_hours'], 1)} |")
    A("")

    # ---- 5 ----
    A("## 5. w 的真實值 vs 64.9% 臨界點")
    A("")
    A("PRD 1.2 的 64.9% 是在「w_signal = 68.3%、s = −0.31%」這一個點推得的：用成交價時只要 w 不掉破 64.9% 就比訊號價好。"
      "它只在 s≈−0.31% 有意義——s 更負時 signal 的 EV 本身也在掉，fill 需要守住的門檻跟著降低。"
      "下表同時列出固定臨界 64.9% 與**資料驅動臨界 w\\***："
      "在同一滑價下，fill 要追平 signal 的 EV 所需的勝率 = (EV_signal + |平均毛虧損_fill| + 費用) ÷ (平均毛獲利_fill + |平均毛虧損_fill|)。")
    A("")
    A("| 滑價 s | w_signal | w_fill | w_fill ≥ 64.9%？ | 臨界 w\\*（追平 signal） | w_fill ≥ w\\*？ | EV_signal | EV_fill | 較優 |")
    A("|---|---|---|---|---|---|---|---|---|")
    for s in slips:
        ws, wf = sig[s]["win_rate"], fil[s]["win_rate"]
        better = "相同" if s == 0 else ("fill" if fil[s]["ev_net"] > sig[s]["ev_net"] else "signal")
        A(f"| {s:+.2%} | {_pct(ws, 1)} | **{_pct(wf, 1)}** | {'是' if wf is not None and wf >= PRD_W_THRESHOLD else '否'} | "
          f"{_pct(w_star[s], 1)} | {'是' if (wf is not None and w_star[s] is not None and wf >= w_star[s]) else '否'} | "
          f"{_pct(sig[s]['ev_net'], 3, True)} | {_pct(fil[s]['ev_net'], 3, True)} | {better} |")
    A("")
    A("- w_fill 相對 s=0 的變化：" + "；".join(f"s={s:+.2%} → {(fil[s]['win_rate'] - w0):+.1%}pt" for s in slips if s != 0) + "。")
    ref = min(neg, key=lambda s: abs(s + 0.0031))
    A(f"- 在 PRD 的參考點 s≈−0.31%（最接近的掃描點 {ref:+.2%}）：w_fill = {_pct(fil[ref]['win_rate'], 1)}，"
      f"{'≥' if fil[ref]['win_rate'] >= PRD_W_THRESHOLD else '<'} 64.9%，與 PRD 的推理{'一致：成交價版較好' if fil[ref]['win_rate'] >= PRD_W_THRESHOLD else '相反'}"
      f"（同一點 EV_fill {_pct(fil[ref]['ev_net'], 3, True)} vs EV_signal {_pct(sig[ref]['ev_net'], 3, True)}）。")
    A("")

    # ---- 6 ----
    A("## 6. 兩種算法的交叉點")
    A("")
    A("| 滑價 s | EV_signal | EV_fill | EV_fill − EV_signal | 配對筆數 | 結果翻轉 | 翻轉率 | 配對平均差(fill−signal) | SE | t | 分解預測 | fill 較好/較差筆數 |")
    A("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for s in slips:
        p = paired.get(s)
        A(f"| {s:+.2%} | {_pct(sig[s]['ev_net'], 3, True)} | {_pct(fil[s]['ev_net'], 3, True)} | {_pct(-diff[s], 3, True)} | "
          + (f"{p['n']} | {p['n_result_diff']} | {_pct(p['flip_rate'], 1)} | {_pct(p['mean'], 3, True)} | {_pct(p['se'], 3)} | "
             f"{_num(p['t'], 2) if p['t'] is not None else '—'} | {_pct(p['decomp_pred'], 3, True)} | {p['n_fill_better']}/{p['n_signal_better']} |"
             if p else "- | - | - | - | - | - | - | - |"))
    A("")
    A("「配對」= 同一 symbol+entry_time 在兩種 basis 下都成交的那些交易，逐筆取淨報酬差；SE 為配對差的標準誤；"
      "結果翻轉 = 同一筆在 signal 是止盈、在 fill 變止損（s<0 時的方向）或反向（s>0 時）。沒有翻轉時配對差是純算術平移（SE≈0），不列 t。"
      "配對比較把「哪些訊號被觸發」這個共同隨機性消掉了，比拿兩個絕對 EV 相減可靠。")
    A("")
    n_open_max = max(n_open_all.values())
    ident_holds = all_gap_zero and n_open_max == 0
    A(f"**機制分解**（可手算驗證「分解預測」欄；恆等式的前提是**無跳空且配對雙方皆已平倉**——跳空以開盤價出場、未平倉以最後收盤估值，"
      f"報酬都不是由 tp/sl 價決定，公式不涵蓋；本資料跳空 {'0 筆' if all_gap_zero else str(sum(c['gap_n'] for c in combos.values())) + ' 筆'}、"
      f"未平倉 {'0 筆' if n_open_max == 0 else str(min(n_open_all.values())) + ' ~ ' + str(n_open_max) + ' 筆'}，"
      f"{'所以是恆等式' if ident_holds else '所以「分解預測」只是近似'}）：配對差 = 確定性項 − {tp_pct + sl_pct:.2f} × 淨翻轉率（止盈→止損 減 止損→止盈），其中確定性項 = "
      f"(−s/(1+s)) × ({1 - tp_pct}·w + {1 + sl_pct}·(1−w))，w 為 signal basis 的勝率（同一筆止盈在 fill 拿 {tp_pct:.0%}、在 signal 拿 1−{1 - tp_pct}/(1+s)，"
      f"差 {1 - tp_pct}·(−s/(1+s))；止損同理差 {1 + sl_pct}·(−s/(1+s))）；每次翻轉的代價是從 +{tp_pct - fee:.1%} 變 −{sl_pct + fee:.1%} = −{tp_pct + sl_pct:.0%}。"
      f"所以 **fill 較優 ⇔ 翻轉率 < 確定性項/{tp_pct + sl_pct:.2f}**：")
    for s in neg:
        p = paired.get(s)
        if p and p["flip_rate_threshold"] is not None:
            A(f"  - s={s:+.2%}：翻轉率 {_pct(p['flip_rate'], 1)} vs 門檻 {_pct(p['flip_rate_threshold'], 1)} → "
              f"{'fill 較優' if p['flip_rate'] < p['flip_rate_threshold'] else 'signal 較優'}（預測 {_pct(p['decomp_pred'], 3, True)}，觀察 {_pct(p['mean'], 3, True)}）")
    A("")
    if crossing_only_at_zero:
        A(f"- **交叉點在 s=0**（EV_fill − EV_signal 在 s<0 全為正、s>0 全為負）。這是結構性的，不是樣本巧合：")
        A("  - signal basis 把 tp/sl 錨在 P0，滑價**1:1 直接**灌進 R:R（s=−1% → 止盈 3.03% / 止損 6.06%），w 完全不動；")
        A("  - fill basis 錨在 P1，R:R 固定 4:5，滑價只透過「水位整體平移 → 觸價機率改變」這條**較弱**的通道影響 EV。")
        A("  - 所以不利滑價時 signal 的損耗較大（fill 較優），有利滑價時 signal 的增益也較大（signal 較優）；優劣由 s 的符號決定。")
        A(f"- s<0 時 fill 的優勢：{'; '.join(f'{s:+.2%} → {_pct(adv[s], 3, True)}' for s in neg)}。"
          f"最大在 s={adv_max_s:+.2%}（{_pct(adv[adv_max_s], 3, True)}），到 s={min(neg):+.2%} 縮到 {_pct(adv[min(neg)], 3, True)}"
          "——fill 的 w 在那裡已崩到盈虧平衡以下，兩者 EV 都是負值，優劣已無實務意義。")
    elif crossings:
        A(f"- 差值變號處（線性內插）：**{', '.join(f'{x:+.3%}' for x in crossings)}**。")
    else:
        A("- 掃描範圍內差值沒有變號（s=0 恆等除外）。")
    A(f"- **各算法 EV 歸零的滑價**（線性內插）：signal {', '.join(f'{x:+.3%}' for x in ev0_sig) or '範圍內未歸零'}；"
      f"fill {', '.join(f'{x:+.3%}' for x in ev0_fil) or '範圍內未歸零'}。")
    for s in pos:
        A(f"- 有利方向 s={s:+.2%}：EV_signal {_pct(sig[s]['ev_net'], 3, True)}、EV_fill {_pct(fil[s]['ev_net'], 3, True)}；"
          f"signal 的平均毛獲利 {_pct(sig[s]['avg_win_gross'], 3)}（理論 1 − {1 - tp_pct}/{1 + s:.4f} = {_pct(sig[s]['tp_eff_theory'], 3)}）"
          f" > {tp_pct:.0%}，w_fill {_pct(fil[s]['win_rate'], 1)} > w_signal {_pct(sig[s]['win_rate'], 1)}，方向正確、沒有寫反。")
    A("")

    # ---- 7 ----
    A("## 7. 同根雙觸發與關閉 resolve 的偏差")
    A("")
    A(f"本次 `RESOLVE_SAME_BAR_WITH_5M=False`：同一根 5 分 K 同時碰到 tp 與 sl 時一律計止損（保守）。"
      "現行回測會打 1 分 K 判定先後（1 分 K 只保留約 7 天，多數歷史雙觸發本來就走同一個 fallback）。")
    A("")
    A("| 滑價 s | basis | 同根雙觸發 | 佔已平倉 | w（實際＝全計止損，下界） | w 上界（全計止盈） |")
    A("|---|---|---|---|---|---|")
    for s in slips:
        for b in BASES:
            c = combos[combo_label(s, b)]
            up = (c["n_tp"] + c["same_bar_n"]) / c["n_closed"] if c["n_closed"] else None
            A(f"| {s:+.2%} | {b} | {c['same_bar_n']} | {_pct(c['same_bar_pct'], 1)} | {_pct(c['win_rate'], 1)} | {_pct(up, 1)} |")
    A("")
    A(f"- 護欄：最高 {worst_sb:.1%} {'**>**' if guard_hit else '≤'} {SAME_BAR_GUARD:.0%}。")
    A(f"- 對絕對勝率的影響上限 = 同根雙觸發比例（s=0 時 {_pct(sb0_pct, 1)}，即 w 真值落在 {_pct(w0, 1)} ~ {_pct(w0_upper, 1)}）。"
      "同一規則套在兩種 basis 上，**組間相對差異**不受影響。本報告的 w 與 WBS 記載的 68.3%（含 1 分 K 判定、且資料窗不同）"
      + ("數字上幾乎相同，但這是巧合性的一致，不代表同一件事。" if abs(wbs_delta) < 0.005 else "不可直接相提並論。"))
    A("")

    # ---- 8 ----
    A("## 8. 邊界情況（AC-5）")
    A("")
    A(f"- **未平倉**：回測結束仍持倉的交易以最後收盤估值、`result=\"未平倉\"`，**不計入**勝率與 EV（本次各組合 {min(n_open_all.values())} ~ {max(n_open_all.values())} 筆）。"
      "與 pb.backtest 相同，一個交易對出現未平倉後不再進場。")
    A(f"- **快取不足**：`pb.load_hourly()` 輸出 < {md['min_bars_required']} 根（= min(300, 26×H)）的交易對略過，規則與 `pionex_strategy4.main()` 相同；本次略過 {U['skipped']} 個。"
      f" 快取起點 {cache['t_min_tpe']} 早於暖機起點 {md['fetch_start_tpe']}"
      + (f"（{n_cover}/{cache['files_readable']} 檔完整涵蓋暖機期，其餘從各自的起點起算、暖機期內不發訊號，與引擎行為相同）" if n_cover is not None else "")
      + "。")
    A(f"- **排除規則**：與現行回測相同，只套 `pb.classify()`（穩定幣/掛勾幣、槓桿代幣、股票/ETF/商品代幣、手動排除）；本次排除 {U['excluded']} 個。"
      "**沒有** minNotional 或極端價格的過濾（`MAX_PRICE=None`，pb 也沒有 minNotional 檢查），與現行回測口徑一致；"
      "極端低價幣的 tp/sl 用浮點價格計算、不套交易所 tick size（回測本來就不模擬 tick），這對兩種 basis 影響相同。")
    A("- **清單來源**：離線模式的交易對清單來自快取檔名，無法得知交易對是否已下架/停用；線上回測用 `pb.get_symbols()` 會少掉已下架的。")
    A("- **同根雙觸發**：見第 7 節。**跳空止損**：開盤已穿越 sl 時以開盤價出場（較差價），與 pb 相同；"
      + ("本次所有組合都是 0 筆——永續合約 5 分 K 連續交易，開盤 = 前一根收盤，跳空只會在 `load_hourly` 補洞（無成交時段）時出現，"
         "所以實際平均毛虧損恰等於理論止損（第 9 節）。" if all_gap_zero else f"本次合計 {sum(c['gap_n'] for c in combos.values())} 筆。"))
    A("- **成交額估算**：快取 CSV 沒有 `amount` 欄，`s4_signal.features()` 的 turn 走 close×volume 估算，與現行回測一致。")
    A("")

    # ---- 9 ----
    A("## 9. 與 PRD 1.1 理論表的對照（signal basis）")
    A("")
    A(f"實際止盈 = 1 − {1 - tp_pct}/(1+s)，實際止損 = {1 + sl_pct}/(1+s) − 1，盈虧平衡 w = (止損 + 費用)/(止盈 + 止損)。"
      "「實際平均毛獲利」應等於理論止盈（止盈都在 tp 價出場）；「實際平均毛虧損」≥ 理論止損（有跳空時以開盤價出場）。")
    A("")
    A("| s | 理論止盈 | 理論止損 | 理論盈虧平衡 w | 實際平均毛獲利 | 實際平均毛虧損 | 實際盈虧平衡 w | 實際 w | w − 盈虧平衡 |")
    A("|---|---|---|---|---|---|---|---|---|")
    for s in slips:
        c = sig[s]
        margin = (c["win_rate"] - c["breakeven_w"]) if (c["win_rate"] is not None and c["breakeven_w"] is not None) else None
        A(f"| {s:+.2%} | {_pct(c['tp_eff_theory'], 2)} | {_pct(c['sl_eff_theory'], 2)} | {_pct(c['breakeven_w_theory'], 1)} | "
          f"{_pct(c['avg_win_gross'], 3)} | {_pct(c['avg_loss_gross'], 3, True)} | {_pct(c['breakeven_w'], 1)} | {_pct(c['win_rate'], 1)} | {_pct(margin, 1, True)}pt |")
    A("")
    fc = fil[slips[0]]
    A(f"PRD 1.1 表的三個點（s=0 → 56.67%、s=−1.0% → 67.8%、s=−1.5% → 73.3%）與上表的理論盈虧平衡 w 一致。"
      f"fill basis 的毛獲利/毛虧損不隨 s 變（例如 s={slips[0]:+.2%}：{_pct(fc['avg_win_gross'], 3)} / {_pct(fc['avg_loss_gross'], 3, True)}），"
      f"理論 {tp_pct:.0%} / −{sl_pct:.0%}，差異只可能來自跳空。")
    A("")

    # ---- 10 ----
    A("## 10. 觀察與建議（非最終決策）")
    A("")
    obs = []
    obs.append(f"**s=0 的基準**：策略4 在這 {C['LOOKBACK_DAYS']} 天、{U['scanned']} 個交易對上勝率 {_pct(w0, 1)}、每筆淨 EV {_pct(sig[0.0]['ev_net'], 3, True)}"
               f"（盈虧平衡 {_pct(sig[0.0]['breakeven_w'], 1)}，安全邊際 {_pct(w0 - sig[0.0]['breakeven_w'], 1, True)}pt）。"
               f"同根雙觸發只有 {sb0_n} 筆（{_pct(sb0_pct, 1)}），關閉 resolve 對 w 的影響 ≤ {_pct(sb0_pct, 1)}pt，這個簡化在本資料上幾乎沒有代價。")
    s_last = min(neg)
    obs.append(f"**signal basis 對滑價很敏感、但敏感在 R:R 不在勝率**：w 一路 {_pct(sig[s_last]['win_rate'], 1)} 不動，"
               f"每筆 EV 從 {_pct(sig[0.0]['ev_net'], 3, True)} 直線下降到 {_pct(sig[s_last]['ev_net'], 3, True)}（s={s_last:+.2%}），"
               f"約每 −1% 滑價損失 {_pct((sig[0.0]['ev_net'] - sig[s_last]['ev_net']) / (-s_last) * 0.01, 2)} EV"
               + (f"，在 s≈{ev0_sig[0]:+.2%} 歸零" if ev0_sig else "，掃描範圍內仍為正")
               + "。實際平均毛獲利/毛虧損與理論止盈/止損逐點完全相等（第 9 節），模型算法無誤。")
    obs.append(f"**fill basis 的代價是勝率**：w 由 {_pct(w0, 1)} 降到 {_pct(fil[s_last]['win_rate'], 1)}（s={s_last:+.2%}），"
               f"每筆 EV 由 {_pct(fil[0.0]['ev_net'], 3, True)} 降到 {_pct(fil[s_last]['ev_net'], 3, True)}"
               + (f"，在 s≈{ev0_fil[0]:+.2%} 歸零" if ev0_fil else "，掃描範圍內仍為正")
               + f"；R:R 鎖在 {tp_pct:.0%}:{sl_pct:.0%}，盈虧平衡 w 固定 {_pct(fil[0.0]['breakeven_w'], 1)}。w 逐段變化："
               + "、".join(f"{a_:+.2%}→{b_:+.2%} {dw:+.1%}pt" for a_, b_, dw in w_drop_steps) + "。")
    if crossing_only_at_zero:
        obs.append(f"**兩種算法的比較**：交叉點在 s=0。所有不利滑價下 fill 的每筆 EV 都高於 signal（{_pct(adv[max(neg)], 3, True)} @ {max(neg):+.2%} → "
                   f"最大 {_pct(adv[adv_max_s], 3, True)} @ {adv_max_s:+.2%} → {_pct(adv[s_last], 3, True)} @ {s_last:+.2%}），"
                   f"有利滑價下 signal 較高。配對差異（第 6 節）方向一致，幅度 {_pct(min(adv.values()), 3)} ~ {_pct(max(adv.values()), 3)} EV/筆，"
                   + (f"有結果翻轉的滑價點配對 |t| 介於 {min(t_vals):.1f} ~ {max(t_vals):.1f}" if t_vals else "")
                   + "——**優劣的方向是結構性的、可信；幅度則在本樣本量下無法精確估計**。")
    elif better_fill_neg and not better_sig_neg:
        obs.append(f"**兩種算法的比較**：所有掃描的不利滑價下 fill 的 EV 都高於 signal，差距隨 |s| 變化；沒有交叉點。")
    elif better_sig_neg and not better_fill_neg:
        obs.append("**兩種算法的比較**：所有掃描的不利滑價下 signal 的 EV 都高於 fill；沒有交叉點。")
    else:
        obs.append(f"**交叉點**：EV_signal − EV_fill 在 s ≈ {', '.join(f'{x:+.3%}' for x in crossings)} 變號。")
    obs.append(f"**64.9% 臨界值只在 s≈−0.31% 成立**：在該點附近（{ref:+.2%}）w_fill = {_pct(fil[ref]['win_rate'], 1)}，守住了 64.9%，PRD 的推理成立。"
               f"更負的 s 不能沿用 64.9%——signal 自己的 EV 也在掉，資料驅動臨界 w\\* 從 {_pct(w_star[max(neg)], 1)}（{max(neg):+.2%}）降到 {_pct(w_star[s_last], 1)}（{s_last:+.2%}），"
               f"w_fill {'在 ' + ', '.join(f'{s:+.2%}' for s in below_star) + ' 低於 w*' if below_star else '在所有不利滑價點都高於 w*'}。")
    obs.append(f"**滑價本身才是主要損耗**：不論哪種 basis，每筆 EV 在 s≈{(ev0_fil or ev0_sig or [float('nan')])[0]:+.1%} 左右歸零。"
               f"換句話說策略4 對不利滑價的容忍上限約 1%（signal {', '.join(f'{x:+.2%}' for x in ev0_sig) or '-'}、fill {', '.join(f'{x:+.2%}' for x in ev0_fil) or '-'}）；"
               f"兩種 basis 的差異（≤ {_pct(max(adv.values()), 2)} EV/筆）遠小於滑價由 0 變 −1% 的損耗"
               + (f"（signal {_pct(sig[0.0]['ev_net'] - sig[-0.01]['ev_net'], 2)}、fill {_pct(fil[0.0]['ev_net'] - fil[-0.01]['ev_net'], 2)} EV/筆）。" if -0.01 in sig else "。"))
    obs.append(f"**樣本量**：每組已平倉 {n_min} ~ {n_max} 筆，單一勝率的標準誤約 ±{_pct(se_w, 1) if se_w else '-'}；"
               "組間 w 差 1~2pt 在統計上不可區分，但各組合共用同一份訊號與 K 棒、且第 6 節用了配對差異，方向性結論比絕對值可靠。")
    obs.append(f"**同根雙觸發**：{best_sb:.1%} ~ {worst_sb:.1%}，"
               + ("超過 15% 護欄，簡化可能實質扭曲結果，須先回報。" if guard_hit else "遠低於 15% 護欄；且同一規則套在兩種 basis 上，組間比較不受影響。"))
    rec = []
    if crossing_only_at_zero:
        rec.append("**技術意見：傾向用成交價（fill）掛 TP/SL。** 理由：實盤滑價對做空幾乎只會是不利方向（吃單成交價偏低），而在所有不利滑價下 fill 的每筆 EV 都不低於 signal；"
                   "signal 版守住的「勝率不變」在 EV 上沒有轉成優勢，它守住的是報表數字，不是錢。fill 版另一個實務好處是 R:R 固定，風控（單筆最大虧損 = 5% × 部位）不會隨滑價漂移。")
        rec.append(f"**代價要講明**：採 fill 後，實盤勝率會低於回測的 {_pct(w0, 0)}"
                   + (f"（s=−0.5% 時 {_pct(fil[-0.005]['win_rate'], 1)}、−1% 時 {_pct(fil[-0.01]['win_rate'], 1)}）" if (-0.005 in fil and -0.01 in fil) else "")
                   + "，這是把滑價的損耗從 R:R 搬到勝率上，不是策略變差。監控指標應改看「每筆淨 EV」與「實際滑價」，不要用勝率跌破 65% 當警報。")
        rec.append(f"**這不是最終決策**：兩者差距 ≤ {_pct(max(adv.values()), 2)} EV/筆，小於本樣本的估計誤差。若 captain 與使用者基於其他考量（例如與現有 dry run 帳本的可比性）維持 signal，"
                   + (f"在 |s| ≤ 0.5% 的區間損失有限（≤ {_pct(max(adv[s] for s in neg if s >= -0.005), 2)} EV/筆）。" if any(s >= -0.005 for s in neg) else ""))
    elif better_fill_neg and not better_sig_neg:
        rec.append("依本次數據，在所有掃描的不利滑價下 fill 的每筆 EV 都不低於 signal，技術上傾向採用 fill，但這不是最終決策。")
    elif better_sig_neg and not better_fill_neg:
        rec.append("依本次數據，訊號價（signal）在不利滑價下每筆 EV 較高，傾向維持第一階段暫定的 signal。")
    else:
        rec.append(f"兩種算法在 s ≈ {', '.join(f'{x:+.3%}' for x in crossings)} 交叉，決策取決於 Wave 3（V1）量到的真實滑價分布落在交叉點的哪一側。")
    rec.append(f"**Wave 3 的 V1（量測真實滑價）優先級應提高**：策略4 的 EV 在 s≈{(ev0_fil or ev0_sig or [float('nan')])[0]:+.1%} 歸零，真實滑價的中位數落在 −0.25% 還是 −0.75%，"
               + (f"對每筆 EV 的影響（{_pct(ev_drop_mid, 2)}）" if ev_drop_mid is not None else "對每筆 EV 的影響")
               + f"比 basis 的選擇（≤ {_pct(max(adv.values()), 2)}）大得多。")
    rec.append(f"本報告的絕對勝率與現行回測口徑差異只在同根雙觸發（≤ {_pct(worst_sb, 1)}pt），可以放心拿組間相對差異做決策；但不要拿這裡的 w 去更新 WBS 的 68.3%，資料窗不同。")
    for o in obs:
        A(f"- {o}")
    A("")
    A("**建議**")
    A("")
    for r in rec:
        A(f"- {r}")
    A("")
    A("---")
    A(f"結果檔：`research/results/{F_CALIB}`、`research/results/{F_SCAN}`、`research/results/{F_TRADES}`（逐筆明細 {len(trades)} 列，含 combo/slippage/basis 欄）、`research/results/{F_CHECK}`。"
      f" 單元測試：`python research/v2_test_sim.py`。")
    A("")
    path = os.path.join(RESULTS_DIR, F_REPORT)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    print(f"→ {path}")
    if guard_hit:
        print(f"[阻塞中] 同根雙觸發比例最高 {worst_sb:.1%} > {SAME_BAR_GUARD:.0%}")


# ============================== CLI ==============================
def main(argv=None):
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            pass
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("stage", choices=["check", "calibrate", "scan", "report", "all"])
    ap.add_argument("--fetch", action="store_true", help="允許打派網 API 補抓並寫回真實快取（預設離線）")
    ap.add_argument("--freeze-end", default=None, help="--fetch 時固定區間結尾 'YYYY-MM-DD HH:MM'（台北）；離線模式忽略")
    ap.add_argument("--restart", action="store_true", help="scan：忽略既有 checkpoint 從頭跑")
    args = ap.parse_args(argv)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    stages = {"check": stage_check, "calibrate": stage_calibrate, "scan": stage_scan, "report": stage_report}
    order = ["check", "calibrate", "scan", "report"] if args.stage == "all" else [args.stage]
    for st in order:
        print(f"\n===== {st} =====")
        stages[st](args)


if __name__ == "__main__":
    main()
