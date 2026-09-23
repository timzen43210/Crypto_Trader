# -*- coding: utf-8 -*-
"""
A0 (research/a0_screen.py) 驗收測試 — 全程離線、用合成快取，不碰 pionex_cache。

這支測試的重點不是「跑得起來」，是釘住三件容易安靜壞掉的事：

  1. **粗篩條件真的被訊號條件蘊含**（AC-1）。
     光看「漏失 0」沒有意義——檢查本身可能根本沒有鑑別力。所以這裡同時有
     正向（test_screen_is_implied_by_signal）與**反向對照**
     （test_leakage_check_detects_a_real_miss）：把粗篩看的特徵欄位換成一個
     與訊號條件方向相反的欄位，漏失必須 > 0。反向對照失敗，正向那條就不算數。

  2. **候選數的分母是 USDT 計價那一批**。非 USDT 計價與段數看不懂的 symbol 都要被排除。

  3. **錯誤訊息不得承諾不存在的檔案**（BUG-048 的反面）。

  4. **粗篩條件真的等於 signal_from_features 對同一項的判定**（captain PRD 11(a)）。
     這一組是 BUG-002 / BUG-003 的回歸測試：舊版的 parity 比對兩側都由粗篩自己算，
     signal_from_features 從未上場，所以計數永遠是 0；而真正抓得到問題的欄位又沒進 verdict，
     於是「粗篩看錯欄位」在真實資料上會印 PASS 並回傳離開碼 0。
     所以這裡直接把兩種失效模式打進去：
       * 把 screen_mask 的 NaN 語意改掉（test_screen_equivalence_detects_nan_semantics_drift）
       * 把粗篩看的欄位換成 volr（test_wrong_screen_feature_is_caught）
     兩者都必須出現在 stop_conditions_triggered、讓 verdict 變 STOP_AND_REPORT。

  5. **import 本檔不得留下全域 socket patch**（F4，test_import_does_not_leave_socket_patched）。
     籠子是 scoped 的，離開武裝區間要把 socket 的原函式放回去，
     不然 `pytest tests/` 同一個 session 內不相干的測試會收到來自本檔的 NetworkBlocked。

不依賴 pytest（這台機器不一定有）：直接 `python tests/test_a0_screen.py` 會逐一跑完並印結果；
用 pytest 跑也可以，函式都是 test_ 開頭、不用任何 fixture。
產物寫到 research/_tmp/test_a0/（已在 .gitignore 裡），不會動到 research/results/。
離開碼 0 = 全過，1 = 有失敗。
"""
import os
import shutil
import socket
import sys
import time
import traceback
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(REPO_ROOT), str(REPO_ROOT / "research")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# F4 的回歸測試要用：在 import a0_screen **之前**先抓住 socket 的原函式物件。
# import 完之後這三個必須還是同一個物件（籠子的 patch 不得外溢成 process 永久狀態）。
PRISTINE_SOCKET = {
    "connect": socket.socket.connect,
    "connect_ex": socket.socket.connect_ex,
    "create_connection": socket.create_connection,
}

import a0_screen as a0                          # noqa: E402
from strategy import s4_signal                  # noqa: E402

TMP = REPO_ROOT / "research" / "_tmp" / "test_a0"
CACHE = TMP / "cache"
OUT = TMP / "out"
VERBOSE = "-v" in sys.argv or "--verbose" in sys.argv
RESULTS = []

BPH = 12                                        # 5M → 12 根/小時
BAR_MS = a0.g2.INTERVAL_MS["5M"]
N_BARS = 400                                    # > 暖機 314 根
FIRE_AT = 360                                   # 植入訊號的 K 棒索引


# ============================== 迷你測試框架 ==============================
def check(name, fn):
    t0 = time.perf_counter()
    try:
        detail = fn() or ""
        RESULTS.append((True, name, detail))
        print(f"  PASS  {name}  {detail}  ({time.perf_counter() - t0:.2f}s)")
    except Exception as e:
        tb = traceback.format_exc() if VERBOSE else f"{type(e).__name__}: {e}"
        RESULTS.append((False, name, tb))
        print(f"  FAIL  {name}\n        {tb}")


def eq(a, b, what=""):
    if a != b:
        raise AssertionError(f"{what}：期待 {b!r}，實際 {a!r}")


def truthy(v, what=""):
    if not v:
        raise AssertionError(f"{what}：期待為真，實際 {v!r}")


def raises(fn, exc, *fragments):
    try:
        fn()
    except exc as e:
        msg = str(e)
        for f in fragments:
            if f not in msg:
                raise AssertionError(f"錯誤訊息裡找不到 {f!r}；實際：{msg}")
        return msg.splitlines()[0][:60]
    raise AssertionError(f"期待丟出 {exc.__name__}，但沒有")


# ============================== 合成快取 ==============================
def synth_frame(with_signal):
    """平盤基底 + （可選）一根刻意植入的訊號。

    訊號四條件全部踩線踩在「剛好成立」上，且參數一律從 DEFAULT_PARAMS 推出來，
    不寫死數字——策略參數日後調整時這份 fixture 會自動跟著動，不會安靜失效。
    """
    p = s4_signal.DEFAULT_PARAMS
    close = np.full(N_BARS, 100.0)
    # turn = close x volume 的 24 小時滾動和 → 取區間中點反推每根的量
    target_turn = (p["MIN_TURN24H"] + p["MAX_TURN24H"]) / 2
    base_vol = target_turn / (close[0] * 24 * BPH)
    volume = np.full(N_BARS, base_vol)

    if with_signal:
        ramp = 2 * BPH                                  # 2 小時
        gain = p["MIN_RET_2H"] * 1.5                    # 明顯高於門檻，不卡在浮點邊界
        close[FIRE_AT - ramp + 1: FIRE_AT + 1] = np.linspace(
            100.0 * (1 + gain / ramp), 100.0 * (1 + gain), ramp)
        volume[FIRE_AT] = base_vol * p["MIN_VOL_RATIO"] * 1.5

    high = close * 1.10
    low = close.copy()                                  # 收在最低 → cpos = 0，滿足竭盡條件
    open_ = close.copy()
    t0 = 1_700_000_000_000 // BAR_MS * BAR_MS
    return pd.DataFrame({
        "time": np.arange(t0, t0 + N_BARS * BAR_MS, BAR_MS, dtype="int64"),
        "open": open_, "high": high, "low": low, "close": close, "volume": volume,
    })


def build_cache():
    shutil.rmtree(TMP, ignore_errors=True)
    CACHE.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    for name, with_sig in (("AAA_USDT_PERP", True), ("BBB_USDT_PERP", True),
                           ("CCC_USDT_PERP", False)):
        synth_frame(with_sig).to_csv(CACHE / f"{name}_5M.csv", index=False)
    # 非 USDT 計價：必須被排除，不得計入候選數的分母
    synth_frame(True).to_csv(CACHE / "DDD_BTC_PERP_5M.csv", index=False)


class Args:
    """build_parser() 的預設值 + 覆寫，免得每個測試都要拼 argv。"""

    def __init__(self, **kw):
        ns = a0.build_parser().parse_args([])
        for k, v in vars(ns).items():
            setattr(self, k, v)
        self.cache_dir = str(CACHE)
        self.out = str(OUT / "a0_test.json")
        self.busy_symbols = 5
        # 合成快取只有 N_BARS 根（約 1.4 天），本來就會踩到 PRD 9.4 的「涵蓋太短」條件。
        # 那條是給真實快取用的，fixture 這裡關掉；另外有 test_short_cache_span_is_flagged
        # 專門驗它在預設值下會被觸發。
        self.min_span_days = 0.0
        for k, v in kw.items():
            setattr(self, k, v)


def run_collect(**kw):
    ctx = a0.Ctx(Args(**kw))
    return a0.aggregate(a0.collect(ctx)), ctx


# ============================== 1. 標的池口徑 ==============================
def test_quote_filter_keeps_only_usdt():
    names = ["AAA_USDT_PERP", "DDD_BTC_PERP", "EEE_USDT_PERP", "FFF_ETH_PERP"]
    kept, excluded = a0.split_universe(names, "USDT")
    eq(kept, ["AAA_USDT_PERP", "EEE_USDT_PERP"], "納入清單")
    eq(sorted(d["symbol"] for d in excluded), ["DDD_BTC_PERP", "FFF_ETH_PERP"], "排除清單")
    truthy(all("計價幣" in d["reason"] for d in excluded), "排除原因要說明計價幣")
    return f"納入 {len(kept)}、排除 {len(excluded)}"


def test_quote_of_does_not_guess():
    eq(a0.quote_of("BTC_USDT_PERP"), "USDT", "三段 PERP")
    eq(a0.quote_of("BTC_USDT"), "USDT", "兩段現貨式")
    eq(a0.quote_of("WEIRD"), None, "段數不足不猜")
    eq(a0.quote_of("A_B_C_D"), None, "段數過多不猜")
    # F5：兩段式 BASE_PERP 的第 2 段是合約種類，不是計價幣。以前會被讀成計價幣 "PERP"
    # ——結果仍正確（非 USDT → 排除）但理由是錯的，會誤導日後的人往錯方向修。
    eq(a0.quote_of("MYST_PERP"), None, "兩段式 BASE_PERP 不得把 PERP 當成計價幣")
    eq(a0.quote_of("myst_perp"), None, "大小寫不影響判定")
    eq(a0.quote_of("MYST_USDT_PERP"), "USDT", "三段式仍照 BASE_QUOTE_PERP 判讀")
    kept, excluded = a0.split_universe(["WEIRD", "MYST_PERP"], "USDT")
    eq(kept, [], "看不懂的不得納入")
    truthy(all("不猜" in d["reason"] for d in excluded), "看不懂要講明不猜")
    truthy(all(d["quote"] is None for d in excluded), "判不出來就是 None，不得填 PERP")
    return "看不懂的一律排除（含兩段式 BASE_PERP）"


def test_non_usdt_is_excluded_end_to_end():
    raw, _ = run_collect()
    eq(len(raw["used"]), 3, "納入的交易對數（DDD_BTC_PERP 必須被排除）")
    truthy(all(s.split("_")[1] == "USDT" for s in raw["used"]), "納入的全都是 USDT 計價")
    eq([d["symbol"] for d in raw["excluded"]], ["DDD_BTC_PERP"], "排除清單")
    return f"分母 {len(raw['used'])} 個、排除 {len(raw['excluded'])} 個"


# ============================== 2. 門檻與速率的解析 ==============================
def test_thresholds_always_end_with_strategy_param():
    min_ret = s4_signal.DEFAULT_PARAMS["MIN_RET_2H"]
    th = a0.parse_thresholds("0.10,0.12", min_ret)
    eq(th[-1], min_ret, "最後一格必須是 DEFAULT_PARAMS 的實際值")
    eq(len(th), 3, "去重後的門檻數")
    eq(a0.parse_thresholds("", min_ret), [min_ret], "沒給也要有邊界格")
    # 手寫一個等於策略門檻的值不會變成第二格（去重），也就不可能兩個字面值不一致
    eq(a0.parse_thresholds(str(min_ret), min_ret), [min_ret], "重複值要去重")
    # F3（BUG-004）：上面幾條在 min_ret 恰好等於某個字面值時，無法分辨「讀 DEFAULT_PARAMS」
    # 與「寫死那個字面值」。改用一個**不等於任何策略參數**的 param_value，
    # 邊界格必須跟著它走，寫死字面值的實作會在這裡紅掉。
    eq(a0.parse_thresholds("0.10", 0.11)[-1], 0.11, "邊界格必須跟著傳入的參數值走")
    eq(a0.parse_thresholds("", 0.0731)[-1], 0.0731, "邊界格必須跟著傳入的參數值走（單獨一格）")
    return f"門檻 {['%g' % x for x in th]}"


def test_thresholds_above_strategy_are_rejected():
    min_ret = s4_signal.DEFAULT_PARAMS["MIN_RET_2H"]
    return raises(lambda: a0.parse_thresholds(str(min_ret * 1.1), min_ret),
                  a0.A0Error, "超出允許範圍", "回報 captain")


def test_bad_inputs_are_rejected():
    min_ret = s4_signal.DEFAULT_PARAMS["MIN_RET_2H"]
    raises(lambda: a0.parse_thresholds("abc", min_ret), a0.A0Error, "不是數字")
    raises(lambda: a0.parse_thresholds("-1", min_ret), a0.A0Error, "超出允許範圍")
    raises(lambda: a0.parse_rates("abc"), a0.A0Error, "不是數字")
    raises(lambda: a0.parse_rates("0"), a0.A0Error, "必須大於 0")
    return "四種壞輸入都有指名道姓的訊息"


# ============================== 3. AC-1：粗篩被訊號蘊含（正向 + 反向對照）==========
def test_screen_is_implied_by_signal():
    raw, ctx = run_collect()
    truthy(raw["baseline_n"] > 0, "合成資料必須真的產生 baseline 訊號，否則這條測試是空轉")
    for thr in raw["thresholds"]:
        eq(raw["missed_count"][thr], 0, f"門檻 {thr:g} 的差集")
    res = a0.build_result(dict(raw, cache_write_check={"unchanged": True}), ctx)
    truthy(res["leakage_check"]["all_ok"], "leakage_check.all_ok")
    truthy(res["verdict"]["AC1_leakage_ok"], "verdict.AC1")
    return f"baseline {raw['baseline_n']} 筆、六個門檻差集全空"


def test_leakage_check_detects_a_real_miss():
    """反向對照：把粗篩看的欄位換成與訊號條件方向相反的 cpos，漏失必須被抓出來。

    合成資料的訊號棒收在最低（cpos = 0），而粗篩是 `>= 門檻`，
    所以「粗篩看 cpos」必然漏掉那些訊號。這條若過不了，
    test_screen_is_implied_by_signal 的「漏失 0」就只是檢查沒有鑑別力而已。
    """
    orig = a0.SCREEN_FEATURE
    a0.SCREEN_FEATURE = "cpos"
    try:
        raw, ctx = run_collect()
        truthy(raw["baseline_n"] > 0, "baseline 訊號數")
        missed = {t: raw["missed_count"][t] for t in raw["thresholds"]}
        truthy(all(v > 0 for v in missed.values()), f"每個門檻都該抓到漏失，實際 {missed}")
        res = a0.build_result(dict(raw, cache_write_check={"unchanged": True}), ctx)
        truthy(not res["leakage_check"]["all_ok"], "all_ok 必須為 False")
        truthy(not res["verdict"]["AC1_leakage_ok"], "AC-1 必須判 False")
        truthy(any("9.1" in s for s in res["verdict"]["stop_conditions_triggered"]),
               "必須列入『停下來回報』")
        truthy(res["leakage_check"]["by_threshold"][0]["missed_detail"],
               "要有漏失明細（symbol / bar_open / 特徵值），不能只給一個數字")
    finally:
        a0.SCREEN_FEATURE = orig
    return f"六個門檻都抓到漏失（{list(missed.values())}），差集邏輯有鑑別力"


def test_vacuous_leakage_check_is_flagged():
    """沒有 baseline 訊號時，「漏失 0」不得被當成 AC-1 通過。"""
    raw, ctx = run_collect()
    raw = dict(raw, baseline_n=0, cache_write_check={"unchanged": True})
    res = a0.build_result(raw, ctx)
    truthy(not res["verdict"]["AC1_leakage_ok"], "baseline=0 時 AC-1 不得為 True")
    truthy(any("空轉" in s for s in res["verdict"]["stop_conditions_triggered"]),
           "要明講這個驗證是空轉的")
    return "空轉的驗證會被標記，不會冒充通過"


# ============================== 4. NaN 與輸入契約 ==============================
def test_nan_handling_matches_signal_from_features():
    raw, ctx = run_collect()
    eq(raw["nan_parity_mismatches"], 0, "粗篩判定與 signal_from_features 的逐格差異")
    eq(raw["nan_ret2h_total"], 2 * BPH * len(raw["used"]), "ret2h 暖機 NaN 根數")
    eq(raw["nan_turn_total"], (24 * BPH - 1) * len(raw["used"]), "turn 暖機 NaN 根數")
    res = a0.build_result(dict(raw, cache_write_check={"unchanged": True}), ctx)
    truthy(res["input_contract"]["nan_ret2h_matches_expected"], "nan_ret2h_matches_expected")
    truthy(res["input_contract"]["nan_turn_matches_expected"], "nan_turn_matches_expected")
    truthy(res["input_contract"]["fixed_period_all"], "fixed_period_all")
    return "暖機 NaN 根數與 s4_signal 的行為逐格一致"


def test_screen_equivalence_really_calls_signal_from_features():
    """BUG-002 的回歸：等價性比對的一側必須真的是 signal_from_features 的輸出。

    不看實作細節，改用行為判定：把 signal_from_features 換成一個「永遠不發訊號」的假貨，
    如果比對真的有拿它當一側，mismatches 就必須從 0 變成正數。
    仍然是 0 的話代表兩側都是粗篩自己算的——那正是 BUG-002 那個恆等式。
    """
    orig = s4_signal.signal_from_features
    s4_signal.signal_from_features = lambda feats, params=None: pd.Series(
        np.zeros(len(feats), dtype=int), index=feats.index, name="signal")
    try:
        raw, ctx = run_collect()
        truthy(raw["nan_parity_mismatches"] > 0,
               f"signal 側被換成永不發訊號，不一致格數必須 > 0，實際 {raw['nan_parity_mismatches']}")
        truthy(not raw["probe_control"]["ok"], "控制組也必須察覺 signal 側不再認同任何一列")
        res = a0.build_result(dict(raw, cache_write_check={"unchanged": True}), ctx)
        truthy(not res["verdict"]["AC1_screen_equivalence_ok"], "等價性必須判 False")
        truthy(not res["verdict"]["AC1_leakage_ok"], "AC-1 必須連帶判 False")
        eq(res["verdict"]["overall"], "STOP_AND_REPORT", "判定")
    finally:
        s4_signal.signal_from_features = orig
    return "signal_from_features 真的參與比對（換掉它就會被抓到）"


def test_screen_equivalence_detects_nan_semantics_drift():
    """BUG-002 的另一面：把粗篩的 NaN 語意改成「暖機 NaN 視為通過」必須被抓到。

    這正是舊版 nan_parity_note 聲稱要抓、實際抓不到的那個分歧
    （DQA 變異 M4：`passes = np.isnan(ret2h) | (ret2h >= thr)` 存活、21/21 全過）。
    """
    orig = a0.screen_mask
    a0.screen_mask = lambda values, threshold: np.isnan(values) | (values >= threshold)
    try:
        raw, ctx = run_collect()
        truthy(raw["nan_parity_mismatches"] > 0,
               f"NaN 語意分歧時不一致格數必須 > 0，實際 {raw['nan_parity_mismatches']}")
        truthy(not raw["probe_control"]["ok"], "控制組（資料無關）也必須抓到")
        res = a0.build_result(dict(raw, cache_write_check={"unchanged": True}), ctx)
        truthy(not res["verdict"]["AC1_screen_equivalence_ok"], "等價性必須判 False")
        truthy(not res["verdict"]["AC1_leakage_ok"], "AC-1 必須連帶判 False")
        truthy(any("不等價" in s for s in res["verdict"]["stop_conditions_triggered"]),
               f"必須列入停下來回報，實際 {res['verdict']['stop_conditions_triggered']}")
        eq(res["verdict"]["overall"], "STOP_AND_REPORT", "判定")
    finally:
        a0.screen_mask = orig
    return "暖機 NaN 被當成通過時，等價性比對與控制組都會紅"


def test_wrong_screen_feature_is_caught():
    """BUG-003 的回歸：粗篩看錯欄位（captain PRD 11(a) 指名的失效模式）必須讓 verdict 變 FAIL。

    DQA 變異 M1：只把 SCREEN_FEATURE 由 ret2h 改成 volr，對真實快取跑會得到
    verdict PASS / stops 空 / 離開碼 0，而候選數高估約 40 倍。
    注意這條**不能**只靠「AC-1 空轉偵測」——合成 fixture 的量能固定所以 volr 恆為 1、
    粗篩從不擋人，空轉偵測才會紅；真實資料的 volr 會掉到 0.10 以下，那條不會觸發。
    所以這裡斷言的是暖機 NaN 根數與等價性比對這兩個**與資料無關於空轉**的檢查。
    """
    orig = a0.SCREEN_FEATURE
    a0.SCREEN_FEATURE = "volr"
    try:
        raw, ctx = run_collect()
        res = a0.build_result(dict(raw, cache_write_check={"unchanged": True}), ctx)
        ic = res["input_contract"]
        truthy(not ic["nan_ret2h_matches_expected"],
               f"看錯欄位時暖機 NaN 根數必須對不上（實際 {ic['nan_ret2h_total']} / "
               f"期待 {ic['expected_nan_ret2h_total']}）")
        truthy(not res["verdict"]["AC1_screen_equivalence_ok"], "等價性必須判 False")
        truthy(not res["verdict"]["AC1_leakage_ok"], "AC-1 必須判 False")
        truthy(any("取錯欄位" in s for s in res["verdict"]["stop_conditions_triggered"]),
               f"停下來回報要明講可能取錯欄位，實際 {res['verdict']['stop_conditions_triggered']}")
        eq(res["verdict"]["overall"], "STOP_AND_REPORT", "判定")
    finally:
        a0.SCREEN_FEATURE = orig
    return "粗篩看錯欄位會被暖機 NaN 根數與等價性比對同時抓到"


def test_input_contract_failures_become_stops():
    """BUG-003：有算、有寫進 input_contract 卻沒進 verdict 的檢查，全部要能擋下判定。"""
    raw, ctx = run_collect()
    cases = [("fixed_period_all", False, "固定週期"),
             ("monotonic_all", False, "升冪"),
             ("nan_turn_total", 1, "turn 的暖機 NaN 根數")]
    for key, bad, frag in cases:
        res = a0.build_result(
            dict(raw, cache_write_check={"unchanged": True}, **{key: bad}), ctx)
        stops = res["verdict"]["stop_conditions_triggered"]
        truthy(any(frag in s for s in stops), f"{key} 壞掉時要出現 {frag!r}，實際 {stops}")
        eq(res["verdict"]["overall"], "STOP_AND_REPORT", f"{key} 壞掉時的判定")
    return f"{len(cases)} 個輸入契約檢查都會讓判定變 STOP_AND_REPORT"


def test_equivalence_probe_control_is_not_vacuous():
    """控制組本身要有方向性：換成與訊號條件方向相反的欄位（cpos）必須當場不一致。

    這證明控制組不是「兩邊一起是 False 所以看起來一致」的假驗證。
    """
    p = dict(s4_signal.DEFAULT_PARAMS)
    good = a0.parity_probe_control("ret2h", p)
    truthy(good["ok"], f"正確欄位的控制組必須過，實際 {good}")
    eq(good["screen_says_pass"], [True, False, False], "正確欄位的三列判定")
    bad = a0.parity_probe_control("cpos", p)
    truthy(not bad["ok"], f"cpos 是 <= 型條件，控制組必須不一致，實際 {bad}")
    truthy(not bad["agree"], "兩側必須真的給出不同答案（不是一起 False）")
    return "控制組對正確欄位過、對方向相反的欄位紅"


def test_warmup_filter_matches_g2_stage_convention():
    eq(a0.warmup_bars_needed(BPH), 24 * BPH + 2 * BPH + 2, "暖機根數口徑")
    short = synth_frame(False).head(a0.warmup_bars_needed(BPH) - 1)
    short.to_csv(CACHE / "SHORT_USDT_PERP_5M.csv", index=False)
    try:
        raw, _ = run_collect()
        skipped = [d["symbol"] for d in raw["skipped"]]
        truthy("SHORT_USDT_PERP" in skipped, f"資料不足的要被跳過，實際跳過 {skipped}")
        truthy("SHORT_USDT_PERP" not in raw["used"], "不足暖機的不得計入分母")
    finally:
        os.remove(CACHE / "SHORT_USDT_PERP_5M.csv")
    return "不足暖機的交易對會被跳過並記錄原因"


# ============================== 5. 候選數與 REST 換算 ==============================
def test_candidate_distribution_and_rest_conversion():
    raw, ctx = run_collect()
    res = a0.build_result(dict(raw, cache_write_check={"unchanged": True}), ctx)
    primary = res["candidates_per_bar"]["primary_window"]
    row = res["candidates_per_bar"]["by_threshold"][0]
    d = row["windows"][primary]["distribution"]
    for k in ("p50", "p90", "p95", "p99", "max", "mean"):
        truthy(k in d, f"分布要有 {k}")
    truthy(len(row["busiest_bars"]) > 0, "要列出最忙的 K 棒")
    truthy(row["busiest_bars"][0]["candidates"] >= row["busiest_bars"][-1]["candidates"],
           "最忙清單要由多到少")
    truthy(row["busiest_bars"][0]["candidate_symbols"], "第 1 名要附候選交易對清單")
    rates = [x["rate_req_per_s"] for x in row["windows"][primary]["rest_seconds"]["per_rate"]]
    eq(rates, res["rates_req_per_s"], "兩組速率都要有")
    for x in row["windows"][primary]["rest_seconds"]["per_rate"]:
        eq(round(x["p99_s"], 9), round(d["p99"] / x["rate_req_per_s"], 9),
           f"{x['rate_req_per_s']} req/s 的 p99 秒數")
        for flag in ("p99_exceeds_target", "p99_exceeds_ceiling",
                     "max_exceeds_target", "max_exceeds_ceiling"):
            truthy(isinstance(x[flag], bool), f"{flag} 要是布林")
    truthy(res["verdict"]["AC2_distribution_complete"], "AC-2")
    truthy(res["verdict"]["AC3_rest_seconds_flagged"], "AC-3")
    return f"p50 {d['p50']:.1f} / p99 {d['p99']:.1f} / max {d['max']:.0f}，兩組速率都標了是否超標"


def test_per_bar_counts_are_cross_symbol_sums():
    """逐根 K 棒的候選數必須是「跨交易對相加」，不是某一個交易對的數字。"""
    raw, _ = run_collect()
    counts = raw["counts"][0]
    truthy(counts.max() > 1, f"合成資料有 2 個同時拉抬的交易對，最忙那根應該 > 1，實際 {counts.max()}")
    eq(int(raw["observed"].max()), len(raw["used"]), "有效交易對數的上限就是納入數")
    return f"最忙那根候選 {int(counts.max())}（納入 {len(raw['used'])} 個交易對）"


def test_signal_margin_distribution():
    raw, ctx = run_collect()
    res = a0.build_result(dict(raw, cache_write_check={"unchanged": True}), ctx)
    m = res["signal_margin"]["distribution"]
    for k in ("min", "p05", "p10", "p25", "p50"):
        truthy(k in m, f"餘裕分布要有 {k}")
    truthy(m["min"] >= s4_signal.DEFAULT_PARAMS["MIN_RET_2H"],
           "訊號的 ret2h 不可能低於策略門檻")
    tol = res["signal_margin"]["error_tolerance_by_threshold"][0]
    eq(round(tol["tolerable_underestimate_all_signals"], 9),
       round(m["min"] - tol["threshold"], 9), "可容忍低估量 = min(ret2h) - 門檻")
    truthy(res["verdict"]["AC4_margin_present"], "AC-4")
    return f"min {m['min']:.4f}、p05 {m['p05']:.4f}"


# ============================== 6. AC-5：離線與只讀 ==============================
def test_net_guard_is_armed():
    """籠子在武裝區間內必須真的攔得住，且自我測試要證明它是活的。"""
    before = len(a0.NET.attempts)
    try:
        with a0.NET.armed(reason="test"):
            eq(a0.NET.self_test_ok, True, "籠子自我測試")
            truthy(len(a0.NET.self_test_attempts) >= 2, "自我測試要留下紀錄")
            truthy(a0.NET.is_armed(), "區間內必須是武裝狀態")
            raises(lambda: socket.create_connection(("example.invalid", 80), timeout=1),
                   a0.NetworkBlocked, "離線籠子攔下")
            raises(lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(
                ("example.invalid", 80)), a0.NetworkBlocked, "離線籠子攔下")
            raises(lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect_ex(
                ("example.invalid", 80)), a0.NetworkBlocked, "離線籠子攔下")
            eq(len(a0.NET.attempts), before + 3, "真正的企圖要被記下來")
    finally:
        del a0.NET.attempts[before:]            # 還原，不要污染後面測試的 verdict
    return "create_connection / connect / connect_ex 三條出口都被攔下並記錄"


def test_import_does_not_leave_socket_patched():
    """F4：只是 import 本檔不得留下 process-wide 的 socket patch。

    舊版在 module 層級 `NET = NetGuard().install()`，patch 永不解除。日後 `pytest tests/`
    同一個 session 內任何需要真 socket 的測試，會收到一個來自不相干模組的 NetworkBlocked。
    這裡用「import 前抓住的原函式物件」做身分比對，不需要真的連任何位址。
    """
    truthy(not a0.NET.is_armed(), "測試主體執行時不該處於武裝狀態")
    for name, fn in PRISTINE_SOCKET.items():
        target = getattr(socket.socket, name, None) if name != "create_connection" \
            else socket.create_connection
        truthy(target is fn, f"socket.{name} 必須還是 import 前的那個物件（實際 {target!r}）")
    # 巢狀武裝：只有最外層會真的還原，內層離開時不得提早把籠子拆掉
    with a0.NET.armed(reason="outer"):
        truthy(a0.NET.is_armed(), "外層武裝")
        with a0.NET.armed(reason="inner"):
            truthy(a0.NET.is_armed(), "內層武裝")
        truthy(a0.NET.is_armed(), "內層離開後外層必須還在武裝（可重入）")
    truthy(not a0.NET.is_armed(), "最外層離開後必須還原")
    truthy(socket.create_connection is PRISTINE_SOCKET["create_connection"], "還原成原函式")
    return "import 不留全域 patch；巢狀武裝可重入，離開後完整還原"


def test_run_does_not_write_cache():
    before = a0.cache_fingerprint(CACHE)
    ctx = a0.Ctx(Args())
    res = a0.run(ctx)
    after = a0.cache_fingerprint(CACHE)
    eq(after["fingerprint_sha256"], before["fingerprint_sha256"], "快取指紋")
    truthy(res["cache_write_check"]["unchanged"], "cache_write_check.unchanged")
    eq(res["offline_proof"]["connect_attempts"], 0, "連線企圖數")
    truthy(res["verdict"]["AC5_offline_and_scope_ok"], "AC-5")
    return f"{before['files']} 個檔案、指紋前後一致"


# ============================== 7. BUG-048：錯誤訊息不得說謊 ==============================
def test_error_message_never_promises_a_missing_file():
    missing = OUT / "definitely_not_written.json"
    if missing.exists():
        os.remove(missing)
    msg = a0._output_state(missing)
    truthy("沒有任何結果檔寫出" in msg, "不存在時要明講沒有檔案")
    for lie in ("部分結果保留", "checkpoint 之前的都在"):
        truthy(lie not in msg, f"不得出現 {lie!r} 這種承諾")
    existing = OUT / "already_there.json"
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_text("{}", encoding="utf-8")
    msg2 = a0._output_state(existing)
    truthy("上一次" in msg2 and "沒有覆寫" in msg2, "存在時要說清楚那是上一次的結果")
    return "兩種狀態的訊息都與磁碟實況一致"


def test_existing_result_is_backed_up_not_overwritten():
    out = OUT / "backup_case.json"
    for f in OUT.glob("backup_case*.json"):
        os.remove(f)
    argv = ["--cache-dir", str(CACHE), "--out", str(out), "--min-span-days", "0"]
    eq(a0.main(argv), 0, "第一次執行的離開碼")
    first = out.read_text(encoding="utf-8")
    eq(a0.main(argv), 0, "第二次執行的離開碼")
    backups = [f for f in OUT.glob("backup_case.*.json")]
    eq(len(backups), 1, "第二次應該留下 1 份備份")
    truthy(backups[0].read_text(encoding="utf-8") == first, "備份內容必須是第一次的結果")
    truthy(out.exists(), "新結果仍要寫出")
    return "舊結果搬走保存，不靜默覆寫"


def test_main_end_to_end_exit_code():
    out = OUT / "e2e.json"
    if out.exists():
        os.remove(out)
    code = a0.main(["--cache-dir", str(CACHE), "--out", str(out), "--max-symbols", "2",
                    "--min-span-days", "0"])
    eq(code, 0, "判定 PASS 時離開碼")
    truthy(out.exists(), "結果 json 要寫出")
    import json
    res = json.loads(out.read_text(encoding="utf-8"))
    eq(res["metadata"]["rest_requests_total"], 0, "REST 請求數")
    truthy(res["metadata"]["script_sha256"], "要有腳本指紋")
    truthy(res["metadata"]["s4_params"]["MIN_RET_2H"], "metadata 要帶 DEFAULT_PARAMS 實際值")
    truthy(res["method_notes"].startswith("A0"), "method_notes 要是模組 docstring 原文")
    # F6：PRD 6 的字面要求是 metadata 帶「快取涵蓋範圍與交易對數」。
    # 既有位置（universe）必須原封不動，metadata 這邊是新增的引用複本，兩處內容要一致。
    md, uni = res["metadata"], res["universe"]
    eq(md["cache_coverage"], uni["cache_coverage"], "metadata 的快取涵蓋範圍複本")
    eq(md["symbols_used"], uni["symbols_used"], "metadata 的交易對數複本")
    eq(md["cache_files_for_interval"], uni["cache_files_for_interval"], "快取檔數複本")
    for k in ("cache_coverage", "symbols_used", "symbols_considered", "symbols_skipped",
              "quote_matched", "cache_files_for_interval"):
        truthy(k in uni, f"universe.{k} 必須留在原位（下游在 parse 它）")
    return f"離開碼 {code}、json {out.stat().st_size} bytes"


def test_short_cache_span_is_flagged():
    """PRD 9.4：快取涵蓋太短時，候選數的尾端沒有意義，必須被標記且離開碼非 0。"""
    out = OUT / "short_span.json"
    if out.exists():
        os.remove(out)
    code = a0.main(["--cache-dir", str(CACHE), "--out", str(out)])      # 用預設的 7 天門檻
    eq(code, 1, "有停下來回報的條件時，離開碼要非 0")
    import json
    v = json.loads(out.read_text(encoding="utf-8"))["verdict"]
    eq(v["overall"], "STOP_AND_REPORT", "判定")
    truthy(any("9.4" in s for s in v["stop_conditions_triggered"]), "要指名 PRD 9.4")
    return "涵蓋不足會被擋下，不會靜悄悄產出一份看起來正常的報告"


def test_missing_cache_dir_message_is_actionable():
    ctx = a0.Ctx(Args(cache_dir=str(TMP / "does_not_exist")))
    return raises(lambda: a0.collect(ctx), a0.g2.G2Error, "找不到快取目錄")


# ============================== main ==============================
TESTS = [
    ("標的池：只納入 USDT 計價", test_quote_filter_keeps_only_usdt),
    ("標的池：段數看不懂的不猜", test_quote_of_does_not_guess),
    ("標的池：非 USDT 端到端被排除", test_non_usdt_is_excluded_end_to_end),
    ("門檻：邊界格一律來自 DEFAULT_PARAMS", test_thresholds_always_end_with_strategy_param),
    ("門檻：高於策略門檻要被拒絕", test_thresholds_above_strategy_are_rejected),
    ("輸入：壞參數有指名道姓的訊息", test_bad_inputs_are_rejected),
    ("AC-1 正向：粗篩被訊號蘊含", test_screen_is_implied_by_signal),
    ("AC-1 反向對照：真的漏失要抓得到", test_leakage_check_detects_a_real_miss),
    ("AC-1：空轉的驗證會被標記", test_vacuous_leakage_check_is_flagged),
    ("NaN 處理與 signal_from_features 等價", test_nan_handling_matches_signal_from_features),
    ("BUG-002：等價性比對真的叫了 signal_from_features",
     test_screen_equivalence_really_calls_signal_from_features),
    ("BUG-002：NaN 語意分歧抓得到", test_screen_equivalence_detects_nan_semantics_drift),
    ("BUG-002：控制組有方向性不是空轉", test_equivalence_probe_control_is_not_vacuous),
    ("BUG-003：粗篩看錯欄位會被擋下", test_wrong_screen_feature_is_caught),
    ("BUG-003：輸入契約失敗會進 verdict", test_input_contract_failures_become_stops),
    ("暖機口徑與 g2 Stage 3/4 一致", test_warmup_filter_matches_g2_stage_convention),
    ("AC-2/3：候選數分布與 REST 換算", test_candidate_distribution_and_rest_conversion),
    ("候選數是跨交易對相加", test_per_bar_counts_are_cross_symbol_sums),
    ("AC-4：訊號餘裕分布", test_signal_margin_distribution),
    ("AC-5：離線籠子是活的", test_net_guard_is_armed),
    ("F4：import 不留下全域 socket patch", test_import_does_not_leave_socket_patched),
    ("AC-5：執行不寫入快取", test_run_does_not_write_cache),
    ("BUG-048：錯誤訊息不承諾不存在的檔案", test_error_message_never_promises_a_missing_file),
    ("舊結果備份不覆寫", test_existing_result_is_backed_up_not_overwritten),
    ("端到端：main() 離開碼與 metadata", test_main_end_to_end_exit_code),
    ("PRD 9.4：快取涵蓋太短要被擋下", test_short_cache_span_is_flagged),
    ("快取目錄不存在的訊息", test_missing_cache_dir_message_is_actionable),
]


def main():
    print(f"A0 離線自檢（完全不連網）　python {sys.version.split()[0]}　"
          f"pandas {pd.__version__}　numpy {np.__version__}")
    print(f"合成快取：{CACHE}")
    build_cache()
    for name, fn in TESTS:
        check(name, fn)
    ok = sum(1 for r in RESULTS if r[0])
    print(f"\n{ok}/{len(RESULTS)} 通過")
    if ok != len(RESULTS):
        print("失敗項目：" + "、".join(n for p, n, _ in RESULTS if not p))
    return 0 if ok == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
