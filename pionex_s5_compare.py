#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略5（群組訊號複製）vs 群組 —— 逐筆比對
==========================================
輸入：signals.csv（群組，從聊天紀錄轉出）、output/s5_signals.csv（pionex_dryrun.py 的策略5 帳本產生）
只比對兩邊都有資料的期間：策略5 帳本的回補起點 ~ dry run 最後一次執行（且不晚於群組最後一筆）。

配對規則：同一個幣、進場時間相差 ≤ MATCH_MINUTES 分鐘 → 視為同一筆訊號
輸出：
  抓到率   群組的訊號有幾成被策略5 複製到（只算派網有的幣）
  命中率   我們的訊號有幾成群組也有出
  進場時間差、進場價差、出場結果是否一致、出場時間差
  兩邊各自漏掉的清單（用來找規則還差在哪）
用法：在 repo 根目錄（有 output/ 與 state/ 的地方）放好最新的 signals.csv 後執行
      python pionex_s5_compare.py
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

GROUP_CSV = "signals.csv"
MIRROR_CSV = "output/s5_signals.csv"
STATE_JSON = "state/dryrun_state.json"
MATCH_MINUTES = 60
OUTPUT = "s5_compare_{ts}.xlsx"


def load(path, who):
    df = pd.read_csv(path, encoding="utf-8-sig")
    df["coin"] = df["交易對"].astype(str).str.upper()
    df["t"] = pd.to_datetime(df["進場時間"])
    df["x"] = pd.to_datetime(df["出場時間"], errors="coerce")
    df["who"] = who
    return df.reset_index(drop=True)


def main():
    G = load(sys.argv[1] if len(sys.argv) > 1 else GROUP_CSV, "群組")
    M = load(sys.argv[2] if len(sys.argv) > 2 else MIRROR_CSV, "策略5")
    lo, hi = M["t"].min(), M["t"].max()
    if os.path.exists(STATE_JSON):                      # 用策略5 實際處理的期間，比「第一筆/最後一筆」準
        with open(STATE_JSON, encoding="utf-8") as fh:
            st = json.load(fh)
        tpe = timezone(timedelta(hours=8))
        to_ts = lambda ms: pd.Timestamp(datetime.fromtimestamp(ms / 1000, tz=tpe).strftime("%Y-%m-%d %H:%M"))
        s5 = st.get("books", {}).get("s5", {})
        if s5.get("start_ms"):
            lo = to_ts(s5["start_ms"])
        if st.get("runs"):
            hi = to_ts(st["runs"][-1]["time"])
    hi = min(hi, G["t"].max())
    pad = pd.Timedelta(minutes=MATCH_MINUTES)
    G = G[(G["t"] >= lo) & (G["t"] <= hi)].copy()
    M = M[(M["t"] >= lo - pad) & (M["t"] <= hi + pad)].copy()   # 邊界外一點也拿來配對，避免邊界漏配
    M["期間內"] = (M["t"] >= lo) & (M["t"] <= hi)
    pionex_coins = set(M["coin"])            # 近似：策略5 出現過的幣一定在派網上
    print(f"比對期間 {lo:%Y-%m-%d %H:%M} ~ {hi:%Y-%m-%d %H:%M}　群組 {len(G)} 筆、策略5 {int(M['期間內'].sum())} 筆")

    # 貪婪配對：同幣、時間差最小者優先
    pairs, used_m = [], set()
    cand = []
    for gi, g in G.iterrows():
        for mi, m in M[M["coin"] == g["coin"]].iterrows():
            d = (m["t"] - g["t"]).total_seconds() / 60
            if abs(d) <= MATCH_MINUTES:
                cand.append((abs(d), gi, mi, d))
    used_g = set()
    for _, gi, mi, d in sorted(cand):
        if gi in used_g or mi in used_m:
            continue
        used_g.add(gi); used_m.add(mi)
        g, m = G.loc[gi], M.loc[mi]
        pairs.append({"交易對": g["coin"], "群組進場": g["進場時間"], "策略5進場": m["進場時間"],
                      "時間差(分，策略5−群組)": d, "群組進場價": g["進場價"], "策略5進場價": m["進場價"],
                      "進場價差": m["進場價"] / g["進場價"] - 1,
                      "群組結果": g["群組結果"], "策略5結果": m["群組結果"],
                      "結果一致": (g["群組結果"] == m["群組結果"]) if pd.notna(g["群組結果"]) and pd.notna(m["群組結果"]) else None,
                      "群組出場": g["出場時間"], "策略5出場": m["出場時間"],
                      "出場時間差(分)": ((m["x"] - g["x"]).total_seconds() / 60) if pd.notna(g["x"]) and pd.notna(m["x"]) else None})
    P = pd.DataFrame(pairs)
    G_on = G[G["coin"].isin(pionex_coins)]
    miss_g = G[~G.index.isin(used_g)].copy()
    miss_g["派網有此幣"] = miss_g["coin"].isin(pionex_coins)
    miss_m = M[~M.index.isin(used_m) & M["期間內"]].copy()
    miss_m["群組做過此幣"] = miss_m["coin"].isin(set(G["coin"]))

    recall = len(P) / max(1, len(G_on))
    n_m = int(M["期間內"].sum())
    prec = int(M.loc[list(used_m), "期間內"].sum()) / max(1, n_m)
    agree = P["結果一致"].dropna().mean() if len(P) else np.nan
    gw = (G["群組結果"] == "止盈").sum() / max(1, G["群組結果"].isin(["止盈", "止損"]).sum())
    Mi = M[M["期間內"]]
    mw = (Mi["群組結果"] == "止盈").sum() / max(1, Mi["群組結果"].isin(["止盈", "止損"]).sum())
    summ = [("比對期間", f"{lo:%Y-%m-%d %H:%M} ~ {hi:%Y-%m-%d %H:%M}"),
            ("群組訊號 / 其中派網有的幣", f"{len(G)} / {len(G_on)}"),
            ("策略5訊號", n_m),
            ("配對成功", len(P)),
            ("抓到率（群組被策略5 複製到的比例）", f"{recall:.1%}"),
            ("命中率（策略5訊號群組也有的比例）", f"{prec:.1%}"),
            ("配對後 進場時間差 中位（分，正=策略5較晚）", round(P["時間差(分，策略5−群組)"].median(), 1) if len(P) else ""),
            ("配對後 |時間差| ≤ 2 分鐘的比例", f"{(P['時間差(分，策略5−群組)'].abs() <= 2).mean():.1%}" if len(P) else ""),
            ("配對後 進場價差 中位", f"{P['進場價差'].median():+.2%}" if len(P) else ""),
            ("配對後 出場結果一致", f"{agree:.1%}" if agree == agree else ""),
            ("群組勝率 / 策略5勝率", f"{gw:.1%} / {mw:.1%}"),
            ("群組漏掉（派網有的幣）", int(miss_g["派網有此幣"].sum())),
            ("策略5多出（群組做過的幣 / 群組沒做過的幣）",
             f"{int(miss_m['群組做過此幣'].sum())} / {int((~miss_m['群組做過此幣']).sum())}")]
    out = OUTPUT.format(ts=datetime.now().strftime("%Y%m%d_%H%M"))
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        pd.DataFrame(summ, columns=["項目", "數值"]).to_excel(w, sheet_name="總結", index=False)
        P.to_excel(w, sheet_name="配對", index=False)
        miss_g.drop(columns=["coin", "t", "x", "who"]).to_excel(w, sheet_name="群組有、策略5沒有", index=False)
        miss_m.drop(columns=["coin", "t", "x", "who", "期間內"]).to_excel(w, sheet_name="策略5有、群組沒有", index=False)
    for k, v in summ:
        print(f"  {k}：{v}")
    print(f"輸出：{out}")


if __name__ == "__main__":
    main()
