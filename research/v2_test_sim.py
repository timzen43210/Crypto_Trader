#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V2 模擬器單元測試（合成 K 棒，全程離線）
========================================
驗證 research/v2_slippage_scan.simulate() 的數學與控制流，以及 scan 續跑的明細檔文字過濾（BUG-001）；期望值全部用公式寫死（AC-2 手算對照），
不是從程式輸出反填。可用 pytest 跑，也可直接 `python research/v2_test_sim.py`。

設定：沿用 pionex_strategy4.CONFIG（5M、TP 4%、SL 5%、FEE 0.05%/邊、冷卻 12 根），RESOLVE 關閉。
合成資料一律 P0 = 100、做空（策略4 只做空），另有一個做多案例確認方向邏輯對稱。
"""
import hashlib
import math
import os
import shutil
import sys
import tempfile

import numpy as np
import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:  # noqa: BLE001
    pass
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import v2_slippage_scan as v2          # noqa: E402
pb, s4 = v2.pb, v2.s4

v2.setup_config(offline=True)
BAR = pb.bar_ms()                       # 300_000
T0 = 1_780_000_000_000 // BAR * BAR
TP, SL, FEE = pb.CONFIG["TAKE_PROFIT"], pb.CONFIG["STOP_LOSS"], pb.CONFIG["FEE_RATE"]
COOLDOWN = pb.CONFIG["COOLDOWN_BARS"]
ALL_COMBOS = [(s, b) for s in v2.SLIPPAGES for b in v2.BASES]
assert (TP, SL, FEE, COOLDOWN, BAR) == (0.04, 0.05, 0.0005, 12, 300_000), (TP, SL, FEE, COOLDOWN, BAR)
assert pb.CONFIG["RESOLVE_SAME_BAR_WITH_5M"] is False


def close(a, b):
    return math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-15)


def make_df(bars, signals):
    """bars: [(o, h, l, c), ...]；signals: {index: ±1}。補上 pb.backtest 會讀的診斷欄（全 NaN）。"""
    n = len(bars)
    df = pd.DataFrame(bars, columns=["open", "high", "low", "close"]).astype("float64")
    df.insert(0, "time", (T0 + np.arange(n) * BAR).astype("int64"))
    df["volume"] = 1.0
    sig = np.zeros(n, dtype="int64")
    for i, d in signals.items():
        sig[i] = d
    df["signal"] = sig
    for col in ("ret1h", "atr_pct", "ma_dev", "obv_chg", "liq24", "vol_ratio", "close_pos",
                "htf_dev", "ret24", "btc_ret1h", "btc_ma_dev"):
        df[col] = np.nan
    return df


FLAT = (100.0, 100.5, 99.5, 100.0)                 # 不會碰到任何 tp/sl 的 K 棒


def run(df, s, basis, symbol="TEST"):
    return v2.simulate(symbol, df, int(df["time"].iloc[0]), s, basis)


# ---------------------------------------------------------------- 案例資料
def df_short_tp():
    # bar0 訊號（收 100）；bar1 不碰；bar2 low=90 → 所有組合都止盈（tp ∈ [94.08, 96.24]，high 99.5 < 最小 sl 102.9）
    return make_df([(100, 101, 99, 100.0), (100, 101, 99, 99.5), (99, 99.5, 90, 91), FLAT, FLAT], {0: -1})


def df_short_sl():
    # bar2 high=110 → 所有組合都止損，open=101 < 最小 sl 102.9（無跳空），low=100.5 > 最大 tp 96.24（不碰 tp）
    return make_df([(100, 101, 99, 100.0), (100, 101, 99.5, 100.2), (101, 110, 100.5, 108), FLAT, FLAT], {0: -1})


def df_short_same_bar():
    # bar1 同時碰 tp 與 sl（low 89 ≤ 94.08、high 111 ≥ 105.26），open=100 未跳空 → 止損、exit=sl
    return make_df([(100, 101, 99, 100.0), (100, 111, 89, 100), FLAT, FLAT], {0: -1})


def df_short_gap():
    # bar1 開盤 106 > sl=105（signal, s=0）→ 跳空止損以 106 出場；low 105.5 不碰 tp
    return make_df([(100, 101, 99, 100.0), (106, 108, 105.5, 107), FLAT, FLAT], {0: -1})


def df_long_tp():
    # 做多：tp=104、sl=95（s=0）；bar1 high 104.5 ≥ 104、low 99 > 95 → 止盈
    return make_df([(100, 101, 99, 100.0), (100, 104.5, 99, 104), FLAT, FLAT], {0: 1})


# ---------------------------------------------------------------- 測試
def test_signal_basis_levels_do_not_move_with_slippage():
    df = df_short_tp()
    for s in v2.SLIPPAGES:
        tr = run(df, s, "signal")[0]
        assert tr["tp"] == 100.0 * (1 - TP) and tr["sl"] == 100.0 * (1 + SL), (s, tr["tp"], tr["sl"])
        assert close(tr["p1"], 100.0 * (1 + s))
        assert tr["p0"] == 100.0


def test_fill_basis_levels_shift_proportionally():
    df = df_short_tp()
    for s in v2.SLIPPAGES:
        tr = run(df, s, "fill")[0]
        p1 = 100.0 * (1 + s)
        assert tr["tp"] == p1 * (1 - TP) and tr["sl"] == p1 * (1 + SL), (s, tr["tp"], tr["sl"])
        assert close(tr["tp"] / tr["p1"], 1 - TP) and close(tr["sl"] / tr["p1"], 1 + SL)


def test_signal_basis_minus_1pct_short_tp_matches_prd_table():
    # PRD 1.1：s=−1% → 實際止盈 = 1 − 0.96/0.99 ≈ 3.03%
    tr = run(df_short_tp(), -0.01, "signal")[0]
    assert tr["result"] == "止盈" and tr["exit"] == 96.0
    assert close(tr["gross_ret"], 1 - 0.96 / 0.99), tr["gross_ret"]
    assert close(tr["net_ret"], 1 - 0.96 / 0.99 - 2 * FEE)
    assert abs(tr["gross_ret"] - 0.030303) < 1e-6


def test_signal_basis_minus_1pct_short_sl_matches_prd_table():
    # PRD 1.1：s=−1% → 實際止損 = 1.05/0.99 − 1 ≈ 6.06%
    tr = run(df_short_sl(), -0.01, "signal")[0]
    assert tr["result"] == "止損" and tr["exit"] == 105.0 and not tr["gap"] and not tr["same_bar_hit"]
    assert close(tr["gross_ret"], -(1.05 / 0.99 - 1)), tr["gross_ret"]
    assert abs(tr["gross_ret"] - (-0.060606)) < 1e-6


def test_fill_basis_any_slippage_short_tp_is_exactly_4pct():
    for s in v2.SLIPPAGES:
        tr = run(df_short_tp(), s, "fill")[0]
        assert tr["result"] == "止盈", (s, tr["result"])
        assert close(tr["gross_ret"], 0.04), (s, tr["gross_ret"])
        assert close(tr["net_ret"], 0.04 - 2 * FEE)


def test_fill_basis_any_slippage_short_sl_is_exactly_minus_5pct():
    for s in v2.SLIPPAGES:
        tr = run(df_short_sl(), s, "fill")[0]
        assert tr["result"] == "止損" and not tr["gap"], (s, tr)
        assert close(tr["gross_ret"], -0.05), (s, tr["gross_ret"])


def test_favourable_slippage_direction_is_correct():
    # s=+0.25%：做空成交價偏高（有利）→ signal basis 的止盈報酬 1 − 0.96/1.0025 > 4%，止損虧損 < 5%
    tr = run(df_short_tp(), 0.0025, "signal")[0]
    assert close(tr["gross_ret"], 1 - 0.96 / 1.0025) and tr["gross_ret"] > 0.04
    tr = run(df_short_sl(), 0.0025, "signal")[0]
    assert close(tr["gross_ret"], -(1.05 / 1.0025 - 1)) and tr["gross_ret"] > -0.05
    # 不利方向相反
    tr = run(df_short_tp(), -0.0025, "signal")[0]
    assert tr["gross_ret"] < 0.04


def test_same_bar_double_hit_counts_as_stop_loss():
    for s, b in ALL_COMBOS:
        tr = run(df_short_same_bar(), s, b)[0]
        assert tr["result"] == "止損" and tr["same_bar_hit"] is True, (s, b, tr)
        assert tr["exit"] == tr["sl"] and not tr["gap"]      # open=100 未跳空 → 以 sl 出場
        assert "同一根同時觸發" in tr["note"]
    tr = run(df_short_same_bar(), 0.0, "signal")[0]
    assert close(tr["gross_ret"], -0.05)


def test_gap_stop_loss_uses_open_price():
    tr = run(df_short_gap(), 0.0, "signal")[0]
    assert tr["result"] == "止損" and tr["gap"] is True and tr["exit"] == 106.0
    assert close(tr["gross_ret"], -(106.0 / 100.0 - 1)) and "跳空" in tr["note"]
    # fill basis、s=−1%：sl = 99×1.05 = 103.95，open 106 仍跳空 → 出場 106，以 P1=99 計損益
    tr = run(df_short_gap(), -0.01, "fill")[0]
    assert tr["gap"] is True and tr["exit"] == 106.0 and close(tr["gross_ret"], -(106.0 / 99.0 - 1))


def test_long_direction_is_symmetric():
    tr = run(df_long_tp(), 0.0, "signal")[0]
    assert tr["dir"] == "做多" and tr["result"] == "止盈" and tr["tp"] == 104.0 and tr["sl"] == 95.0
    assert close(tr["gross_ret"], 0.04)
    tr = run(df_long_tp(), -0.01, "signal")[0]         # 做多時 s<0 = 買得便宜 = 有利
    assert close(tr["gross_ret"], 104.0 / 99.0 - 1) and tr["gross_ret"] > 0.04


def test_open_trade_at_end_and_no_further_entries():
    # 訊號在最後一根 → 沒有後續 K 棒 → 未平倉，exit = 最後收盤，exit_time = t[-1]+BAR
    df = make_df([FLAT, FLAT, (100, 101, 99, 100.0)], {2: -1})
    tr = run(df, -0.005, "fill")
    assert len(tr) == 1 and tr[0]["result"] == "未平倉"
    assert tr[0]["exit"] == 100.0 and tr[0]["exit_time"] == int(df["time"].iloc[-1]) + BAR
    assert tr[0]["hold_bars"] == 0
    # 未平倉之後即使還有訊號也不再進場（與 pb 相同）
    bars = [(100, 101, 99, 100.0)] + [FLAT] * 30
    df = make_df(bars, {0: -1, 20: -1})
    tr = run(df, 0.0, "signal")
    assert len(tr) == 1 and tr[0]["result"] == "未平倉"


def test_cooldown_skips_signals_within_12_bars():
    # bar0 訊號、bar2 止盈 → i = 2 + 12 = 14；bar 5 與 bar 13 的訊號被略過，bar 14 的訊號進場
    bars = [(100, 101, 99, 100.0), (100, 101, 99, 99.5), (99, 99.5, 90, 91)] + [FLAT] * 20
    bars[14] = (100, 100.5, 99.5, 100.0)
    bars[16] = (99, 99.5, 90, 91)
    df = make_df(bars, {0: -1, 5: -1, 13: -1, 14: -1})
    tr = run(df, 0.0, "signal")
    assert [x["entry_time"] for x in tr] == [T0 + 1 * BAR, T0 + 15 * BAR]
    assert [x["result"] for x in tr] == ["止盈", "止盈"]


def test_entry_set_invariant_across_combos_when_exit_bar_same():
    # 兩個訊號相隔夠遠、出場根對所有組合相同 → 進場時間集合對 18 個組合完全一致（同一份 signal）
    bars = [(100, 101, 99, 100.0), (100, 101, 99, 99.5), (99, 99.5, 90, 91)] + [FLAT] * 40
    bars[20] = (100, 100.5, 99.5, 100.0)
    bars[22] = (101, 110, 100.5, 108)
    df = make_df(bars, {0: -1, 20: -1})
    ref = None
    for s, b in ALL_COMBOS:
        ent = [x["entry_time"] for x in run(df, s, b)]
        assert ent == [T0 + 1 * BAR, T0 + 21 * BAR], (s, b, ent)
        ref = ref or ent
        assert ent == ref
    # 訊號集合本身：simulate 只讀 df["signal"]，這一欄不因 s / basis 改變
    assert df["signal"].tolist().count(-1) == 2


def test_pnl_uses_p1_not_p0():
    # 同一筆止盈：signal basis、s=−2% → exit 96、P1 = 98 → gross = 1 − 96/98（若誤用 P0 會得到 0.04）
    tr = run(df_short_tp(), -0.02, "signal")[0]
    assert close(tr["gross_ret"], 1 - 0.96 / 0.98) and not close(tr["gross_ret"], 0.04)


def _rand_df(seed, n=600, every=7):
    """帶趨勢的隨機漫步 + 偶發大幅 K 棒（製造同根雙觸發與跳空），每 every 根放一個做空訊號。"""
    rng = np.random.default_rng(seed)
    c = 100 * np.exp(np.cumsum(rng.normal(0, 0.012, n)))
    jump = rng.choice([0, 0, 0, 0, 0, 0.07], n)                                # 偶發 +7% 開盤跳空
    o = np.r_[c[0], c[:-1]] * (1 + rng.normal(0, 0.004, n) + jump)
    spread = np.abs(rng.normal(0, 0.01, n)) + rng.choice([0, 0, 0, 0.06], n)   # 1/4 機率出現大幅 K 棒
    h = np.maximum(o, c) * (1 + spread)
    l = np.minimum(o, c) * (1 - spread)
    bars = list(zip(o, h, l, c))
    sig = {i: -1 for i in range(0, n, every)}
    return make_df(bars, sig)


def test_parity_with_pb_backtest_on_synthetic_data():
    """校準：slippage=0、basis="signal" 的 simulate 與 pb.backtest（RESOLVE 關閉）逐筆相等。"""
    total, kinds = 0, set()
    for seed in range(20):
        df = _rand_df(seed)
        start = int(df["time"].iloc[0])
        pb_tr = pb.backtest("SYN", df, start)
        sim_tr = v2.simulate("SYN", df, start, 0.0, "signal")
        mism = v2.compare_parity(pb_tr, sim_tr)
        assert not mism, (seed, mism[:3])
        total += len(pb_tr)
        kinds |= {x["result"] for x in pb_tr}
        kinds |= {"gap" for x in sim_tr if x["gap"]} | {"same_bar" for x in sim_tr if x["same_bar_hit"]}
    assert total > 100 and {"止盈", "止損", "gap", "same_bar"} <= kinds, (total, kinds)   # 各種路徑都要被走到


def test_parity_compare_detects_differences():
    df = df_short_tp()
    a = v2.simulate("X", df, T0, 0.0, "signal")
    b = v2.simulate("X", df, T0, -0.01, "fill")       # tp/sl/exit 不同 → 必須偵測到
    pb_like = [{"entry_time": x["entry_time"], "entry": x["p0"], "tp": x["tp"], "sl": x["sl"],
                "exit_time": x["exit_time"], "exit": x["exit"], "result": x["result"]} for x in a]
    assert v2.compare_parity(pb_like, a) == []
    assert v2.compare_parity(pb_like, b) != []
    assert v2.compare_parity(pb_like, []) and v2.compare_parity(pb_like, [])[0]["kind"] == "count"


def test_combo_stats_breakeven_formula():
    # 手算：2 筆止盈（毛 +4%）、1 筆止損（毛 −5%）→ w=2/3，be=(0.05+0.001)/(0.09)=56.67%，EV 淨 = (0.039×2 − 0.051)/3
    tr = [dict(result="止盈", gross_ret=0.04, net_ret=0.039, same_bar_hit=False, gap=False, hold_bars=3, hold_hours=0.25, symbol="A"),
          dict(result="止盈", gross_ret=0.04, net_ret=0.039, same_bar_hit=False, gap=False, hold_bars=5, hold_hours=5 / 12, symbol="B"),
          dict(result="止損", gross_ret=-0.05, net_ret=-0.051, same_bar_hit=True, gap=False, hold_bars=1, hold_hours=1 / 12, symbol="A"),
          dict(result="未平倉", gross_ret=0.0, net_ret=-0.001, same_bar_hit=False, gap=False, hold_bars=9, hold_hours=0.75, symbol="C")]
    st = v2.combo_stats(tr, 0.0, "fill")
    assert st["n_trades"] == 4 and st["n_closed"] == 3 and st["n_open"] == 1
    assert close(st["win_rate"], 2 / 3) and close(st["breakeven_w"], 0.051 / 0.09)
    assert close(st["ev_net"], (0.039 * 2 - 0.051) / 3) and st["same_bar_n"] == 1 and close(st["same_bar_pct"], 1 / 3)
    assert close(st["breakeven_w_theory"], (0.05 + 0.001) / 0.09)
    st = v2.combo_stats(tr, -0.01, "signal")
    assert close(st["tp_eff_theory"], 1 - 0.96 / 0.99) and close(st["sl_eff_theory"], 1.05 / 0.99 - 1)


def test_interp_zero_ignores_trivial_identity_at_zero():
    xs = [-0.02, -0.01, 0.0, 0.0025]
    assert v2._interp_zero(xs, [-0.01, -0.005, 0.0, -0.002]) == []         # 兩側同號，s=0 恆等不算交叉
    cx = v2._interp_zero(xs, [0.004, -0.002, 0.0, 0.001])
    assert len(cx) == 2 and close(cx[0], -0.02 + 0.01 * 0.004 / 0.006) and cx[1] == 0.0


# ---------------------------------------------------------------- BUG-001：續跑明細檔位元組不變
def _fake_trade(s, b, k):
    """合成一筆明細列。p0=0.2285 使 signal-basis 的 sl = 0.2285×1.05 = 0.23992500000000003（真實 CSV 裡
       被舊續跑碼改寫成 0.239925 的那個值），確保測試資料含 pandas 預設解析器會動到的長尾浮點。"""
    p0 = 0.2285 + k * 1e-4
    p1 = p0 * (1 + s)
    base = p0 if b == "signal" else p1
    tp, sl = base * (1 - TP), base * (1 + SL)
    gross = -(tp / p1 - 1)
    t_in, t_out = T0 + k * BAR, T0 + (k + 3) * BAR
    return {"combo": v2.combo_label(s, b), "slippage": float(s), "basis": b,
            "symbol": f"SYM{k}_USDT_PERP", "dir": "做空",
            "entry_time": t_in, "entry_time_tpe": v2.tpe_str(t_in),
            "p0": p0, "p1": p1, "tp": tp, "sl": sl,
            "exit_time": t_out, "exit_time_tpe": v2.tpe_str(t_out), "exit": tp, "result": "止盈",
            "same_bar_hit": False, "gap": False, "gross_ret": gross, "net_ret": gross - 2 * FEE,
            "hold_bars": 3, "hold_hours": 3 * BAR / pb.HOUR_MS,
            "note": "" if k % 5 else "同一根同時觸發→保守計止損"}


def _read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def test_resume_prune_keeps_completed_rows_byte_identical():
    """BUG-001：續跑只做文字列過濾。「寫全部 18 組 → 截到前 15 組 → 補寫後 3 組」的明細檔必須與
       一次寫完的檔 md5 / 位元組完全相同（不是 DataFrame.equals），已完成組合的原始行一個位元組都不能動。"""
    rows = {v2.combo_label(s, b): [_fake_trade(s, b, k) for k in range(7)] for s, b in ALL_COMBOS}
    labels = list(rows)
    done = set(labels[:15])
    with tempfile.TemporaryDirectory() as d:
        full, resumed = os.path.join(d, "full.csv"), os.path.join(d, "resumed.csv")
        for i, label in enumerate(labels):
            v2._append_trades_csv(full, rows[label], write_header=(i == 0))
        full_bytes = _read_bytes(full)
        assert b"0.23992500000000003" in full_bytes                 # 測試資料真的含長尾浮點文字
        assert full_bytes.count(b"\n") == 1 + 18 * 7
        # 預期砍完後的內容：表頭 + 前 15 組的原始行，直接從一次寫完的位元組切出，不經任何解析
        head = full_bytes.split(b"\n", 1)[0] + b"\n"
        expect_pruned = head + b"".join(
            ln for ln in full_bytes[len(head):].splitlines(keepends=True) if ln.split(b",", 1)[0].decode() in done)

        shutil.copyfile(full, resumed)
        assert v2._prune_trades_csv(resumed, done) == 15 * 7
        assert _read_bytes(resumed) == expect_pruned
        for label in labels[15:]:
            v2._append_trades_csv(resumed, rows[label], write_header=False)
        md5 = lambda p: hashlib.md5(_read_bytes(p)).hexdigest()    # noqa: E731
        assert md5(resumed) == md5(full) and _read_bytes(resumed) == full_bytes

        # 被中斷的半行（CSV 已寫一半、v2_scan.json 未記錄）屬於未完成組合 → 砍掉，其餘不動
        with open(resumed, "ab") as f:
            f.write(b"s=-0.0200|fill,-0.02,fill,PARTIAL_USDT_PERP")
        assert v2._prune_trades_csv(resumed, done) == 15 * 7
        assert _read_bytes(resumed) == expect_pruned

        # 表頭不是本腳本寫出的格式 → 拒絕續跑（SystemExit），不會亂砍
        with open(resumed, "wb") as f:
            f.write(b"foo,bar\n1,2\n")
        try:
            v2._prune_trades_csv(resumed, done)
            raise AssertionError("表頭不符應拒絕續跑")
        except SystemExit:
            pass


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS  {name}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {name}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"ERROR {name}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
