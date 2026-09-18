#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略4 — 爆量竭盡（只做空）
==========================
從跟單機器人的 1,160 筆真實訊號反推而來，但不是複製：它的訊號我們只抓到約兩成，
這是用同一套骨架、自己挑出來的一組訊號，樣本外勝率反而更高。

核心邏輯：幣已經被拉抬一段（2小時漲幅夠大），盯著每根 5 分 K，
          出現爆量、且該根收盤位置偏弱（衝高後被壓回）時做空。

進場（每根 5 分 K 收盤判斷，以收盤價進場）：
  拉抬       近 2 小時漲幅 ≥ MIN_RET_2H
  爆量       本根量 ÷ 前 24 小時每根均量 ≥ MIN_VOL_RATIO
  竭盡       收盤位置 ≤ MAX_CLOSE_POS（1=收最高、0=收最低）
  中小盤     近 24 小時成交額 ≤ MAX_TURN24H（大幣的拉抬比較不會回吐）
出場：做空，跌 TAKE_PROFIT 止盈 / 漲 STOP_LOSS 止損。

同時會讀 signals.csv 比對跟單群組的真實訊號（有的話），輸出「訊號比對」分頁。
用法：與 pionex_backtest.py 放同一資料夾，python pionex_strategy4.py
"""
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
from openpyxl.styles import Font

import pionex_backtest as pb

# ============================== 進場條件 ==============================
S4 = {
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
    # ---- 以下預設關閉，實驗用 ----
    "MIN_VOL_VS_MAX24": None,    # 本根量 ÷ 近24h最大量，例 0.5
    "MIN_SPIKES_1H": None,       # 近1小時爆量次數，例 4
    "MIN_ATR": None,             # ATR(1h換算) 下限，例 0.04
    "MIN_RSI": None,
    "MAX_PRICE": None,
    "COOLDOWN_HOURS": 1.0,       # 出場後冷卻幾小時（換 K 棒週期時自動換算根數）
}

CONFIG = {
    "MARKET_TYPE": "PERP",
    # K棒週期與可回溯天數（派網每個週期只保留約 10,000 根）：
    #   5M → 33 天（預設）　15M → 100 天　30M → 200 天
    # 換週期時 LOOKBACK_DAYS / WARMUP_BARS / RESOLVE_INTERVALS 要一起改，其餘條件都是小時尺度，不用動。
    "INTERVAL": "5M",
    "LOOKBACK_DAYS": 33,
    "FREEZE_END": None,          # 例 "2026-09-16 16:25"：固定區間結尾，之後重跑完全走快取
    "WARMUP_BARS": 300,          # 需涵蓋 24 小時（5M=288 根、15M=96 根）

    "EXIT_MODE": "fixed",
    # 止盈/止損 2026-09-17 定為 4%/5%（原始 3%/5%，中途曾短暫設為 5%/5%）。
    # 依據：5分K 33天全組合掃描（144 組，pionex_tpsl_sweep.py）+ 三組實跑驗證。
    #   3%/5%  勝率 72.8% 每筆EV +0.72% 每天EV +4.18% 回撤 36.6% 總報酬 138%
    #   4%/4%  勝率 63.9% 每筆EV +1.01% 每天EV +5.85% 回撤 30.9% 總報酬 193%
    #   4%/5%  勝率 68.3% 每筆EV +1.04% 每天EV +5.97% 回撤 39.9% 總報酬 197%  ← 採用
    # 4%/4% 與 4%/5% 在報酬與風險上統計無法區分（每筆EV P=54%；回撤按日自助法
    # 中位 21.3% vs 23.7%，95%區間幾乎重疊，實際 30.9/39.9 的差距是順序運氣），
    # 兩者最大併發同為 2、最長連敗同為 3，差別只在勝率，故取勝率較高的 4%/5%。
    # 更寬的組合（如 7%/8%，每天EV +12.3%）不採用：最大併發 8 筆、最壞同時虧損
    # -64.8% 本金，且那 8 筆是同一波行情的相關空單；縮到同樣尾部風險後每天僅 +1.56%。
    "TAKE_PROFIT": 0.04,
    "STOP_LOSS": 0.05,
    "FEE_RATE": 0.0005,
    "MAX_HOLD_HOURS": None,
    "RESOLVE_SAME_BAR_WITH_5M": True,
    "RESOLVE_INTERVALS": ["1M"],   # 同根雙觸發用更小週期判定；15M 時改成 ["5M", "1M"]

    "ATR_PERIOD": 14, "MA_PERIOD": 20, "OBV_LOOKBACK": 20, "HTF_MA_PERIOD": 100,
    "LIQ_MIN_USD": 0, "LIQ_MODE": "sum24",
    "ATR_MIN_PCT": 0, "ATR_MAX_PCT": None, "MAX_PRICE": None,
    "MOM_MODE": "fixed", "MOM_THRESHOLD": 0.02, "MOM_ATR_MULT": 0.45,
    "TP_ATR_MULT": 0.7, "SL_ATR_MULT": 1.1,
    "MAX_MA_DEV": None, "MAX_MOM": None, "MIN_VOL_RATIO": None, "MIN_CLOSE_POS": None,
    "EXCLUDE_FLAT_24H": None, "BTC_MAX_ALIGNED_DEV": None,
    "REQUIRE_HTF_TREND": False, "BTC_TREND_FILTER": False, "COOLDOWN_BARS": 12,

    "EQUITY_INITIAL": 100, "EQUITY_ORDER_PCT": 0.02, "EQUITY_STEP": 50,
    "EQUITY_LEVERAGE": 50, "EQUITY_SIZING_MODE": 2, "EQUITY_STOP_BELOW": 0,

    "REQUEST_SLEEP": 0.1,
    "CACHE_DIR": "pionex_cache",
    "CA_BUNDLE": None,
    "OUTPUT": "strategy4_{ts}.xlsx",
    "EXTRA_EXCLUDE": [], "ONLY_SYMBOLS": [],
}

SIGNALS_CSV = "signals.csv"      # 真實訊號；設 None 就不做比對
MATCH_MINUTES = 30               # 時間差在幾分鐘內算同一個訊號

def H():
    """一小時等於幾根 K 棒（依 INTERVAL 自動換算）。"""
    return max(1, round(pb.bars_per_hour()))
DIAG = [("漲幅2h", "ret2h", 10, "0.00%"), ("漲幅1h", "ret1h_", 10, "0.00%"),
        ("量比(本根)", "volr", 11, "0.00"), ("收盤位置", "cpos", 9, "0.00"),
        ("量/24h最大量", "vmax24", 12, "0.00"), ("1h爆量次數", "spikes1h", 11, "0"),
        ("MA20乖離", "madev", 10, "0.00%"), ("RSI(1h)", "rsi", 9, "0.0"),
        ("ATR%(1h)", "atr1h", 10, "0.00%"), ("24h成交額", "turn", 13, "#,##0")]
TRIG = [k for _, k, _, _ in DIAG]


def rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


def between(series, rng):
    lo, hi = rng
    ok = pd.Series(True, index=series.index)
    if lo is not None:
        ok &= series >= lo
    if hi is not None:
        ok &= series <= hi
    return ok


def add_indicators(df, btc=None):
    C = pb.CONFIG
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["ret1h"] = c / c.shift(1) - 1              # pb.backtest 用的「前一根漲跌幅」
    prev = c.shift(1)
    tr = pd.concat([h - l, (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)
    atr5 = tr.ewm(alpha=1 / C["ATR_PERIOD"], adjust=False, min_periods=C["ATR_PERIOD"]).mean()
    df["atr"] = atr5
    df["atr_pct"] = atr5 / c
    df["atr1h"] = atr5 / c * np.sqrt(H())           # 換算成小時尺度，才和 3%/5% 出場可比
    df["ma"] = c.rolling(20 * H()).mean()           # 1小時MA20
    df["ma_dev"] = df["madev"] = c / df["ma"] - 1
    obv = (np.sign(c.diff()).fillna(0) * v).cumsum()
    df["obv_chg"] = obv - obv.shift(C["OBV_LOOKBACK"])

    df["ret1h_"] = c / c.shift(H()) - 1
    df["ret4h"] = c / c.shift(4 * H()) - 1
    df["ret24"] = c / c.shift(24 * H()) - 1
    df["volr"] = v / v.shift(1).rolling(24 * H()).mean()
    df["vol_ratio"] = df["volr"]
    df["pull"] = c / h.shift(1).rolling(H()).max() - 1
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

    df["ret2h"] = c / c.shift(2 * H()) - 1
    df["vmax24"] = v / v.rolling(24 * H()).max()
    df["spikes1h"] = (df["volr"] >= 3).rolling(H()).sum()

    cond = (df["ret2h"] >= S4["MIN_RET_2H"]) & (df["volr"] >= S4["MIN_VOL_RATIO"])
    cond &= df["cpos"] <= S4["MAX_CLOSE_POS"]
    cond &= df["turn"] >= S4["MIN_TURN24H"]
    if S4["MAX_TURN24H"] is not None:
        cond &= df["turn"] <= S4["MAX_TURN24H"]
    if S4["MIN_VOL_VS_MAX24"] is not None:
        cond &= df["vmax24"] >= S4["MIN_VOL_VS_MAX24"]
    if S4["MIN_SPIKES_1H"] is not None:
        cond &= df["spikes1h"] >= S4["MIN_SPIKES_1H"]
    if S4["MIN_ATR"] is not None:
        cond &= df["atr1h"] >= S4["MIN_ATR"]
    if S4["MIN_RSI"] is not None:
        cond &= df["rsi"] >= S4["MIN_RSI"]
    if S4["MAX_PRICE"] is not None:
        cond &= c < S4["MAX_PRICE"]
    df["signal"] = np.where(cond.fillna(False), -1, 0)
    return df


def param_rows(rows):
    C = pb.CONFIG
    head = list(rows[:6])
    head[3] = ("止盈", C["TAKE_PROFIT"], "做空：價格下跌此比例")
    head[4] = ("止損", C["STOP_LOSS"], "做空：價格上漲此比例")
    opt = lambda k: S4[k] if S4[k] is not None else "不限"
    mid = [
        ("策略", "策略4 — 爆量竭盡（只做空）", "5分K逐根判斷、小時尺度條件"),
        ("2小時漲幅下限", S4["MIN_RET_2H"], "最關鍵的條件（AUC 0.90）"),
        ("量比下限", S4["MIN_VOL_RATIO"], "本根量 ÷ 前24h每根均量"),
        ("收盤位置上限", S4["MAX_CLOSE_POS"], "1=收最高、0=收最低；偏低＝衝高被壓回"),
        ("24h成交額上限", opt("MAX_TURN24H"), "大幣的拉抬較不易回吐"),
        ("24h成交額下限", S4["MIN_TURN24H"], "流動性"),
        ("量/24h最大量下限", opt("MIN_VOL_VS_MAX24"), "實驗用"),
        ("1h爆量次數下限", opt("MIN_SPIKES_1H"), "實驗用"),
        ("ATR%(1h)下限", opt("MIN_ATR"), "實驗用"),
    ]
    be = [r for r in rows if r[0] == "盈虧平衡勝率"]
    out = head + mid + be
    out += [
        ("RSI(1h)下限", opt("MIN_RSI"), "實驗用"),
        ("價格上限", opt("MAX_PRICE"), "實驗用"),
        ("出場後冷卻", f'{S4["COOLDOWN_HOURS"]} 小時（{pb.CONFIG["COOLDOWN_BARS"]} 根）', ""),
        ("同根雙觸發", "/".join(C["RESOLVE_INTERVALS"]) + " 判定", "1分K 僅約7天內有資料"),
    ]
    return out


def diag_columns():
    return ([n for n, *_ in DIAG], [w for _, _, w, _ in DIAG],
            lambda tr: [tr.get(k) for k in TRIG])


def compare_sheet(trades, real):
    """回傳 extra(wb) 函式：輸出訊號比對分頁。"""
    def extra(wb, real=real):
        ws = wb.create_sheet("訊號比對", 1)
        ws["A1"] = "與跟單群組真實訊號的比對"
        ws["A1"].font = Font(name="Arial", size=13, bold=True)
        if real is None or real.empty:
            ws["A2"] = "找不到 signals.csv，未比對"
            ws["A2"].font = pb.FONT
            return
        mine = pd.DataFrame([{"coin": t["symbol"].split("_")[0],
                              "time": pd.Timestamp(pb.to_dt(t["entry_time"]))} for t in trades])
        lo, hi = real["dt"].min(), real["dt"].max()
        if not mine.empty:
            mine = mine[(mine["time"] >= lo) & (mine["time"] <= hi)]
        tol = pd.Timedelta(minutes=MATCH_MINUTES)
        hit = []
        for r in real.itertuples():
            m = mine[(mine["coin"] == r.coin) & (mine["time"] - r.dt).abs().le(tol) ] if not mine.empty else mine
            hit.append(len(m) > 0)
        real_h = real.assign(命中=hit)
        mine_hit = []
        for m in mine.itertuples():
            q = real_h[(real_h["coin"] == m.coin) & (real_h["dt"] - m.time).abs().le(tol)]
            mine_hit.append(len(q) > 0)
        rows = [
            ("比對期間", f"{lo:%Y-%m-%d %H:%M} ~ {hi:%Y-%m-%d %H:%M}"),
            ("它的訊號數", len(real)),
            ("我們的訊號數", len(mine)),
            ("命中（它發我們也發）", int(sum(hit))),
            ("命中率", f"{sum(hit) / len(real):.1%}" if len(real) else "-"),
            ("精確率（我們發的裡面它也發）",
             f"{sum(mine_hit) / len(mine):.1%}" if len(mine) else "-"),
            ("每天訊號數（我們）", round(len(mine) / max(1, (hi - lo).days), 1)),
            ("每天訊號數（它）", round(len(real) / max(1, (hi - lo).days), 1)),
            ("時間容忍", f"±{MATCH_MINUTES} 分鐘"),
        ]
        ws.append([])
        for k, v in rows:
            ws.append([k, v])
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.font = pb.FONT
        ws.append([])
        ws.append(["以下為它發、我們沒發的訊號（漏掉的）"])
        ws.append(["交易對", "時間", "群組結果"])
        pb.style_header(ws, ws.max_row, 3)
        for r in real_h[~real_h["命中"]].itertuples():
            ws.append([r.coin, r.dt, getattr(r, "群組結果", "")])
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                if cell.font is None or cell.font.bold is not True:
                    cell.font = pb.FONT
                if isinstance(cell.value, pd.Timestamp) or hasattr(cell.value, "minute"):
                    cell.number_format = "yyyy-mm-dd hh:mm"
        pb.set_widths(ws, [30, 22, 12])
    return extra


def load_real():
    if not SIGNALS_CSV or not os.path.exists(SIGNALS_CSV):
        return None
    d = pd.read_csv(SIGNALS_CSV)
    d["coin"] = d["交易對"].astype(str).str.upper()
    d["dt"] = pd.to_datetime(d["進場時間"])
    return d[["coin", "dt"] + [c for c in ("群組結果",) if c in d.columns]]


def main():
    pb.CONFIG.update(CONFIG)
    pb.CONFIG["COOLDOWN_BARS"] = max(1, round(S4["COOLDOWN_HOURS"] * H()))
    pb.PARAM_ROWS_HOOK, pb.DIAG_COLUMNS_HOOK = param_rows, diag_columns
    C = pb.CONFIG
    BAR = pb.bar_ms()
    if C["FREEZE_END"]:
        now = int(datetime.strptime(C["FREEZE_END"][:16], "%Y-%m-%d %H:%M")
                  .replace(tzinfo=pb.TPE).timestamp() * 1000) // BAR * BAR
        print(f"（已固定區間結尾 {C['FREEZE_END']}，完全使用快取）")
    else:
        now = int(time.time() * 1000) // BAR * BAR
    test_start = now - C["LOOKBACK_DAYS"] * 24 * pb.HOUR_MS
    fetch_start = test_start - C["WARMUP_BARS"] * BAR
    period = f"{pb.to_dt(test_start):%Y-%m-%d %H:%M} ~ {pb.to_dt(now):%Y-%m-%d %H:%M}"
    print(f"策略4 爆量竭盡（只做空）　K棒：{pb.interval_label()}　區間：{period}")

    real = load_real()
    print(f"真實訊號：{len(real) if real is not None else 0} 筆")

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

    all_trades = []
    for k, sym in enumerate(sorted(todo), 1):
        try:
            df = pb.load_hourly(sym, fetch_start, now)
            if len(df) < min(300, 26 * H()):
                scan_rows.append({"symbol": sym, "status": "略過", "reason": "K棒不足", "bars": len(df)})
                continue
            df = add_indicators(df)
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
    pb.write_excel(all_trades, scan_rows, out, period + "（策略4）",
                   extra=compare_sheet(all_trades, real))

    closed = [t for t in all_trades if t["result"] != "未平倉"]
    days = max(1, C["LOOKBACK_DAYS"])
    print(f"\n訊號 {len(all_trades)} 筆（每天 {len(all_trades) / days:.1f} 筆）")
    if closed:
        wins = sum(t["result"] == "止盈" for t in closed)
        print(f"已平倉 {len(closed)}，止盈 {wins}，勝率 {wins / len(closed):.1%}（盈虧平衡 63.75%）")
    print(f"輸出：{os.path.abspath(out)}")


if __name__ == "__main__":
    main()
