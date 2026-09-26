# -*- coding: utf-8 -*-
"""
G5 驗收測試 — 策略5 訊號核心只有 strategy/s5_signal.py 一份。

釘住的是：
  1. 相依（AC-7）：s5_signal 只 import numpy / pandas / 標準庫；在全新的直譯器裡 import strategy.s5_signal
     之後，sys.modules 裡沒有 pionex_*、requests、openpyxl、live、research。
  2. 參數：DEFAULT_PARAMS 的 11 個鍵、順序與**型別**（EARLY_VOL_RULES 是 tuple of (int, float)、
     COOLDOWN_HOURS 是 int）。dry run 的 s5 帳本指紋用 json 序列化，看不出 tuple 與 list 的差別，
     型別只能在這裡擋。**數值不在測試裡重寫**（策略參數只有一份來源，包括測試的期望值）：
     值由第 4 項的指紋守住，其他測試的期望一律取自 DEFAULT_PARAMS / EXIT_PARAMS、由合成資料推導，
     或對顯式傳入的測試規則（CASE_RULES）下斷言——改參數時，只有指紋那條會提醒你。
     EXIT_PARAMS 是 4 個出場鍵、與 DEFAULT_PARAMS 不重疊、FEE_RATE 不在裡面；exit_params() 回傳深拷貝。
  3. 單一來源（AC-6，有鑑別力）：在全新子行程裡先改 s5_signal 的 DEFAULT_PARAMS / EXIT_PARAMS、再 import
     回測與 dry run，兩條路徑的參數、生效的 pb.CONFIG、訊號都跟著變；s5 帳本指紋改變、其他四本帳不變。
     每一個參數鍵都要接上（哨兵值測試），而且兩支呼叫端真的在呼叫 s5_signal 的函式（替換函式後輸出跟著變），
     不是各自留了一份公式。
  4. 五本帳目前的指紋（見 test_dryrun_book_fingerprints_are_pinned）。
  5. evaluate() 本身：已知案例的進場分鐘與分支、小K週期（1M / 5M / 15M）無關、NaN 與防呆路徑、
     不改動輸入、非 v3 參數與看不到窗口的限時分支會被拒絕。

為什麼用子行程：pionex_strategy5.S5 / CONFIG 與 pionex_dryrun.S5_RULE / S5_CONFIG 在 import 當下就建好，
pb.CONFIG 又是全域字典；每個情境各開一個全新的 Python 子行程，才看得到「import 時取值」是否真的取自來源。
離線籠子只裝在子行程裡（import pionex_* 之前）；本行程只在純函式測試期間暫時裝上、結束就還原
（F4：import 本檔、跑完所有測試，本行程的 socket 都必須是原函式）。
全程離線，不打任何 API、不寫任何檔案（state/、output/、pionex_cache/ 完全不碰）。
不依賴 pytest：直接 `python tests/test_s5_signal.py` 會逐一跑完並印結果。
"""
import ast
import contextlib
import json
import os
import socket
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, REPO_ROOT)

# F4：在 import 任何專案模組**之前**先抓住 socket 的原函式物件，最後比對仍是同一個。
PRISTINE_SOCKET = {
    "connect": socket.socket.connect,
    "connect_ex": socket.socket.connect_ex,
    "create_connection": socket.create_connection,
    "getaddrinfo": socket.getaddrinfo,
}

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from strategy import s5_signal  # noqa: E402

FIVE_BOOKS = ("watch", "rev", "rev_wide", "s4", "s5")
OTHER_BOOKS = ("watch", "rev", "rev_wide", "s4")
PARAM_KEYS = ("MIN_RISE_FROM_OPEN", "MIN_VOL_MULT", "EARLY_VOL_RULES", "REQUIRE_VOL_BURST", "REQUIRE_HIGHER_HIGH",
              "HH_MODE", "MIN_ABOVE_PH", "MIN_TURN24H", "MAX_TURN24H", "MAX_PRICE", "COOLDOWN_HOURS")
EXIT_KEYS = ("EXIT_MODE", "TAKE_PROFIT", "STOP_LOSS", "MAX_HOLD_HOURS")


# ============================== 離線籠子（本行程用，會還原）==============================
class NetworkBlocked(RuntimeError):
    pass


@contextlib.contextmanager
def _offline():
    """暫時封鎖連網，離開時把 socket 的原函式放回去（不留全域 patch）。"""
    def blocked(*a, **k):
        raise NetworkBlocked("tests/test_s5_signal.py：離線測試禁止連網")
    saved = {"connect": socket.socket.connect, "connect_ex": socket.socket.connect_ex,
             "create_connection": socket.create_connection, "getaddrinfo": socket.getaddrinfo}
    socket.socket.connect = blocked
    socket.socket.connect_ex = blocked
    socket.create_connection = blocked
    socket.getaddrinfo = blocked
    try:
        try:
            socket.create_connection(("example.invalid", 80), timeout=1)
        except NetworkBlocked:
            pass
        else:
            raise AssertionError("離線籠子沒有攔下連線，測試不可信")
        yield
    finally:
        socket.socket.connect = saved["connect"]
        socket.socket.connect_ex = saved["connect_ex"]
        socket.create_connection = saved["create_connection"]
        socket.getaddrinfo = saved["getaddrinfo"]


# ============================== 合成 1 分K ==============================
MIN = 60_000
HOUR = 3_600_000
T_C = 1_790_200_800_000 // HOUR * HOUR       # 被檢查的小時 C（整點）；W = C-2h 暖機、P = C-1h 前一小時


def _bars(t0, closes, vols, open0, high_at=None):
    rows, o = [], open0
    for i, (c, v) in enumerate(zip(closes, vols)):
        hi, lo = max(o, c) + 0.001, min(o, c) - 0.001
        if high_at and i in high_at:
            hi = high_at[i]
        rows.append({"time": t0 + i * MIN, "open": o, "high": hi, "low": lo, "close": c, "volume": v})
        o = c
    return rows


# 合成資料的常數（行情本身，不是策略參數）
P_OPEN, P_CLOSE, P_HIGH, P_VOL = 100.0, 105.5, 106.5, 10.0   # 前一小時：開、收、最高、每分量（合計 600）
P_RISE = P_CLOSE / P_OPEN - 1                                # ① 的值（約 +5.5%）
C_BELOW, C_ABOVE, BREAK_MIN = 105.0, 107.0, 40               # 本小時：第 40 分起收盤才高過前高
C_VOLS = [19.0] * 15 + [63.0] * 5 + [2.0] * 40               # 累計：第 15 分 285、第 20 分 600、第 40 分 640、全小時 680

# 合成案例用的規則（**測試輸入**，不是 DEFAULT_PARAMS 的拷貝）。數值刻意和出貨預設值不同：
#   一來 DEFAULT_PARAMS 日後調整時這裡不必跟著改；二來可以證明函式真的用了傳入的參數（誤用預設值就會對不上）。
# v3 開關與過濾鍵取自 s5_signal.V3_SWITCHES；COOLDOWN_HOURS 沿用 DEFAULT_PARAMS（evaluate() 用不到）。
# 在這組規則下：① 5.5% ≥ 5%；② A = 680 < 3 × 600 不成立、第一個限時分支 (45, 1.0) 在第 20 分累計 600 成立並黏著、
# 第二個 (15, 0.5) 第 15 分 285 < 300 不成立；③ 107 > 106.5 × 1.001 = 106.6065 從第 40 分起成立。
# → 本小時唯一訊號在第 40 分（1M / 5M）或第 45 分（15M），分支 code 2，標籤 B_LABEL。
CASE = {"MIN_RISE_FROM_OPEN": 0.05, "MIN_VOL_MULT": 3.0, "EARLY_VOL_RULES": ((45, 1.0), (15, 0.5)),
        "MIN_ABOVE_PH": 0.001}
CASE_RULES = {**s5_signal.DEFAULT_PARAMS, **s5_signal.V3_SWITCHES, **CASE}
B_LABEL = "45分1倍"                                          # CASE 的第一個限時分支 (45, 1.0) 的標籤


def close_minute_at(bar_minutes):
    """用 bar_minutes 分K 表達合成資料時，第一根收盤分鐘 ≥ BREAK_MIN 的小K（= 進場那根）的收盤分鐘。"""
    return -(-BREAK_MIN // bar_minutes) * bar_minutes


def synth_1m(prev_open=P_OPEN, prev_close=P_CLOSE, prev_vol=P_VOL, drop_prev_minute=None):
    """已知答案的 1 分K（與 TASK-010 的 c1 同型，行情常數見上方 P_* / C_*）：
         P（前一小時）開 prev_open、線性漲到 prev_close、第 51 根最高 P_HIGH、每分量 prev_vol
         C（本小時）收盤分鐘 1～39 收 C_BELOW（< 前高），BREAK_MIN 起收 C_ABOVE（> 前高）；量見 C_VOLS
       CASE_RULES 下：本小時唯一訊號在第 40 分，分支只有 CASE 的第一個限時分支（code 2，標籤 B_LABEL）。"""
    w = _bars(T_C - 2 * HOUR, [100.0] * 60, [10.0] * 60, 100.0)
    closes = [100.0 + (prev_close - 100.0) * (i + 1) / 60 for i in range(60)]
    closes[-1] = prev_close
    p = _bars(T_C - HOUR, closes, [prev_vol] * 60, prev_open, high_at={50: P_HIGH})
    c = _bars(T_C, [C_ABOVE if m >= BREAK_MIN else C_BELOW for m in range(1, 61)], C_VOLS, C_BELOW)
    rows = w + p + c
    if drop_prev_minute is not None:
        rows = [r for r in rows if r["time"] != T_C - HOUR + (drop_prev_minute - 1) * MIN]
    return pd.DataFrame(rows)


def agg(df1m, minutes):
    """1 分K 聚合成 N 分K（整點對齊）。"""
    step = minutes * MIN
    g = df1m.assign(b=df1m["time"] // step * step).groupby("b")
    return pd.DataFrame({"time": g["time"].first().index.astype("int64"), "open": g["open"].first().values,
                         "high": g["high"].max().values, "low": g["low"].min().values,
                         "close": g["close"].last().values, "volume": g["volume"].sum().values}).reset_index(drop=True)


def _signals_in_c(ev):
    s = ev[(ev["signal"] == -1) & (ev["hid"] == T_C // HOUR)]
    return [(int(m), int(c)) for m, c in zip(s["minute"], s["branch_code"])]


# ============================== 子行程 ==============================
_CHILD = r'''
import copy, json, socket, sys

class NetworkBlocked(RuntimeError):
    pass

def _blocked(*a, **k):
    raise NetworkBlocked("tests/test_s5_signal.py：離線測試禁止連網")

socket.socket.connect = _blocked
socket.socket.connect_ex = _blocked
socket.create_connection = _blocked
socket.getaddrinfo = _blocked
try:
    socket.create_connection(("example.invalid", 80), timeout=1)
    cage_ok = False
except NetworkBlocked:
    cage_ok = True

sys.dont_write_bytecode = True
sys.path.insert(0, sys.argv[1])
sys.path.insert(1, sys.argv[2])
spec = json.loads(sys.argv[3])

# 先改來源（或替換 s5_signal 的函式）、再 import 呼叫端
from strategy import s5_signal
for k, v in spec.get("DEFAULT_PARAMS", {}).items():
    s5_signal.DEFAULT_PARAMS[k] = tuple(tuple(x) for x in v) if k == "EARLY_VOL_RULES" else v
for k, v in spec.get("EXIT_PARAMS", {}).items():
    s5_signal.EXIT_PARAMS[k] = v
import numpy as np
if spec.get("patch") == "first_per_hour":
    s5_signal.first_per_hour = lambda mask, hid: np.zeros(len(mask), dtype=bool)
elif spec.get("patch") == "rise_from_open":
    s5_signal.rise_from_open = lambda o, c: np.full(len(o), 0.4242)

import pionex_backtest as pb
pristine = copy.deepcopy(pb.CONFIG)
import pionex_strategy5 as s5m
import pionex_dryrun as dr
import test_s5_signal as T

snap = lambda v: {"type": type(v).__name__, "repr": repr(v)}
out = {"cage_ok": cage_ok,
       "import_touched_pb_config": pb.CONFIG != pristine,
       "S5": {k: snap(v) for k, v in s5m.S5.items()},
       "S5_is_source": s5m.S5 is s5_signal.DEFAULT_PARAMS,
       "S5_RULE": {k: snap(v) for k, v in dr.S5_RULE.items()},
       "CONFIG": {k: snap(s5m.CONFIG[k]) for k in s5_signal.EXIT_PARAMS},
       "S5_CONFIG": {k: snap(dr.S5_CONFIG[k]) for k in s5_signal.EXIT_PARAMS},
       "FEE": [s5m.CONFIG["FEE_RATE"], dr.S5_CONFIG["FEE_RATE"]]}
dr.use_config(next(iter(dr.BOOKS)))
out["fp"] = {b: dr.book_fingerprint(b) for b in dr.BOOKS}
dr.use_config("s5")
out["dry_cfg"] = {k: snap(pb.CONFIG[k]) for k in list(s5_signal.EXIT_PARAMS) + ["COOLDOWN_BARS"]}
if spec.get("signals", True):
    df = T.synth_1m()
    ind = dr.s5_indicators(df.copy())
    sig = ind[(ind["signal"] == -1) & (ind["time"] >= T.T_C)]
    out["dry_sig"] = [[int(m), dr.snapshot5(r)["s5_branch"]] for (_, r), m in zip(sig.iterrows(), sig["s5_minute"])]
    out["dry_rise_at_c"] = float(ind.loc[ind["time"] == T.T_C, "s5_rise"].iloc[0])
    pb.CONFIG.clear()
    pb.CONFIG.update(copy.deepcopy(pristine))
    pb.CONFIG.update(s5m.CONFIG)
    pb.CONFIG["COOLDOWN_BARS"] = s5_signal.cooldown_bars(s5m.H(), s5m.S5)
    res = s5m.add_indicators(T.agg(df, 15))
    sig = res[(res["signal"] == -1) & (res["time"] >= T.T_C)]
    out["bt_sig"] = [[int(m), s5m.branch_label(int(c))] for m, c in zip(sig["minute"], sig["branch"])]
    out["bt_rise_at_c"] = float(res.loc[res["time"] == T.T_C, "rise_open"].iloc[0])
    out["bt_cfg"] = {k: snap(pb.CONFIG[k]) for k in s5_signal.EXIT_PARAMS}
print("@@RESULT@@" + json.dumps(out, ensure_ascii=True, sort_keys=True))
'''

_CACHE = {}


def _run_child(spec=None):
    """開全新子行程：先改 s5_signal（或替換它的函式），再 import pionex_*，回傳兩條路徑的參數、設定、指紋與訊號。"""
    key = json.dumps(spec or {}, sort_keys=True)
    if key not in _CACHE:
        env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONDONTWRITEBYTECODE="1")
        r = subprocess.run([sys.executable, "-c", _CHILD, REPO_ROOT, TESTS_DIR, key], cwd=REPO_ROOT, env=env,
                           capture_output=True, text=True, encoding="utf-8")
        lines = [ln for ln in r.stdout.splitlines() if ln.startswith("@@RESULT@@")]
        if r.returncode != 0 or not lines:
            raise AssertionError(f"子行程失敗（exit {r.returncode}）：\n{r.stderr[-3000:]}")
        res = json.loads(lines[-1][len("@@RESULT@@"):])
        assert res["cage_ok"] is True, "子行程的離線籠子沒有攔下連線，測試不可信"
        _CACHE[key] = res
    return _CACHE[key]


def _snap(v):
    return {"type": type(v).__name__, "repr": repr(v)}


def _case_spec(params=None, **extra):
    """子行程規格：先把合成案例的規則（v3 開關 + CASE）套進 s5_signal.DEFAULT_PARAMS，再疊上 params 的突變。
       子行程的訊號期望因此只依賴 CASE 與合成資料，不依賴出貨的參數值。"""
    p = {**s5_signal.V3_SWITCHES, **CASE, **(params or {})}
    p = {k: ([list(r) for r in v] if k == "EARLY_VOL_RULES" else v) for k, v in p.items()}
    return {"DEFAULT_PARAMS": p, **extra}


# ============================== 1. 相依（AC-7）==============================
def test_s5_signal_imports_only_numpy_pandas_stdlib():
    with open(os.path.join(REPO_ROOT, "strategy", "s5_signal.py"), encoding="utf-8") as f:
        tree = ast.parse(f.read())
    mods = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "s5_signal 不得相對 import 專案內其他模組"
            mods.add(node.module.split(".")[0])
    bad = sorted(m for m in mods if m not in ("numpy", "pandas") and m not in sys.stdlib_module_names)
    assert not bad, f"s5_signal 只能 import numpy / pandas / 標準庫，多了 {bad}"


def test_import_in_fresh_interpreter_pulls_no_heavy_modules():
    """全新直譯器只 import strategy.s5_signal：sys.modules 不可以有 pionex_*、requests、openpyxl、live、research。"""
    code = ("import sys; sys.dont_write_bytecode = True; sys.path.insert(0, sys.argv[1]); "
            "import strategy.s5_signal as m; "
            "bad = sorted(k for k in sys.modules if k.split('.')[0] in ('requests', 'openpyxl', 'live', 'research') "
            "or k.startswith('pionex_')); "
            "print('@@MODS@@' + ','.join(bad)); print('@@FILE@@' + m.__file__)")
    r = subprocess.run([sys.executable, "-c", code, REPO_ROOT], cwd=REPO_ROOT, capture_output=True, text=True,
                       env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
    assert r.returncode == 0, r.stderr[-2000:]
    mods = [ln for ln in r.stdout.splitlines() if ln.startswith("@@MODS@@")][0][len("@@MODS@@"):]
    where = [ln for ln in r.stdout.splitlines() if ln.startswith("@@FILE@@")][0][len("@@FILE@@"):]
    assert os.path.normcase(where).startswith(os.path.normcase(REPO_ROOT)), where
    assert mods == "", f"import strategy.s5_signal 帶進了 {mods}"


# ============================== 2. 參數 ==============================
_OPT_NUM = (type(None), int, float)          # None = 不限，或一個數字（bool 不算）


def test_default_params_keys_and_types_are_pinned():
    """鍵、順序、型別釘死；**數值不在這裡重寫**（值由 test_dryrun_book_fingerprints_are_pinned 的指紋守住，
       改任何值或 0 / 0.0 指紋就變）。型別之所以要釘：指紋用 json 序列化，EARLY_VOL_RULES 從 tuple 改成 list
       指紋看不出來。COOLDOWN_HOURS 固定為 int（0 改 0.0 會改變指紋；真的要改成小數小時時一併更新這裡）。"""
    P = s5_signal.DEFAULT_PARAMS
    assert tuple(P) == PARAM_KEYS and s5_signal.PARAM_KEYS == PARAM_KEYS, tuple(P)
    expect = {"MIN_RISE_FROM_OPEN": (float,), "MIN_VOL_MULT": (float,), "EARLY_VOL_RULES": (tuple,),
              "REQUIRE_VOL_BURST": (bool,), "REQUIRE_HIGHER_HIGH": (bool,), "HH_MODE": (str,),
              "MIN_ABOVE_PH": (float,), "MIN_TURN24H": _OPT_NUM, "MAX_TURN24H": _OPT_NUM, "MAX_PRICE": _OPT_NUM,
              "COOLDOWN_HOURS": (int,)}
    bad = {k: type(v).__name__ for k, v in P.items() if type(v) not in expect[k]}
    assert not bad, f"DEFAULT_PARAMS 的型別變了：{bad}"
    rules = P["EARLY_VOL_RULES"]
    assert all(type(r) is tuple and len(r) == 2 and type(r[0]) is int and type(r[1]) is float for r in rules), \
        f"EARLY_VOL_RULES 必須是 tuple of (int, float)：{rules!r}"


def test_exit_params_keys_and_separation():
    """出場參數的鍵、順序、型別；數值不重寫（理由同上）。"""
    E = s5_signal.EXIT_PARAMS
    assert tuple(E) == EXIT_KEYS, tuple(E)
    expect = {"EXIT_MODE": (str,), "TAKE_PROFIT": (float,), "STOP_LOSS": (float,), "MAX_HOLD_HOURS": _OPT_NUM}
    bad = {k: type(v).__name__ for k, v in E.items() if type(v) not in expect[k]}
    assert not bad, f"EXIT_PARAMS 的型別變了：{bad}"
    assert not set(E) & set(s5_signal.DEFAULT_PARAMS), "出場鍵混進了 DEFAULT_PARAMS（會改變 s5 指紋）"
    assert "FEE_RATE" not in E, "FEE_RATE 是回測帳務，留在 pionex_strategy5.CONFIG"


def test_exit_params_returns_a_deep_copy():
    got = s5_signal.exit_params()
    assert got == s5_signal.EXIT_PARAMS and got is not s5_signal.EXIT_PARAMS
    got["TAKE_PROFIT"] = 999
    assert s5_signal.EXIT_PARAMS["TAKE_PROFIT"] != 999


def test_callers_take_params_from_s5_signal():
    """回測 S5 / CONFIG 與 dry run S5_RULE / S5_CONFIG 的值與型別都等於來源；S5 是拷貝不是同一個物件；
       import 呼叫端不會改到 pb.CONFIG。"""
    res = _run_child({"signals": False})
    src = {k: _snap(v) for k, v in s5_signal.DEFAULT_PARAMS.items()}
    assert res["S5"] == src, "pionex_strategy5.S5 與 s5_signal.DEFAULT_PARAMS 不同"
    assert res["S5_RULE"] == src, "pionex_dryrun.S5_RULE 與 s5_signal.DEFAULT_PARAMS 不同"
    assert res["S5_is_source"] is False, "pionex_strategy5.S5 不可以直接是 DEFAULT_PARAMS（回測途中會被改）"
    ex = {k: _snap(v) for k, v in s5_signal.EXIT_PARAMS.items()}
    assert res["CONFIG"] == ex, f"pionex_strategy5.CONFIG 的出場鍵不等於 EXIT_PARAMS：{res['CONFIG']}"
    assert res["S5_CONFIG"] == ex, f"pionex_dryrun.S5_CONFIG 的出場鍵不等於 EXIT_PARAMS：{res['S5_CONFIG']}"
    fee_backtest, fee_dryrun = res["FEE"]          # pionex_strategy5.CONFIG / pionex_dryrun.S5_CONFIG
    assert type(fee_backtest) is float and fee_dryrun == fee_backtest, \
        f"dry run 的手續費必須取自 pionex_strategy5.CONFIG['FEE_RATE']：{res['FEE']}"
    assert res["import_touched_pb_config"] is False


# ============================== 3. 單一來源（AC-6，有鑑別力）==============================
def _assert_only_s5_fp_changed(base, mut):
    assert mut["fp"]["s5"] != base["fp"]["s5"], "改了策略5 的參數，s5 指紋卻沒變：dry run 會把新舊參數的紀錄混在一起"
    for b in OTHER_BOOKS:
        assert mut["fp"][b] == base["fp"][b], f"改策略5 的參數波及了 {b} 的指紋"


def test_baseline_synthetic_case():
    """套入 CASE 規則後：dry run（1 分K）第 40 分、回測（15 分K）第 45 分進場，分支都只有 CASE 的第一個限時分支。"""
    base = _run_child(_case_spec())
    assert base["dry_sig"] == [[BREAK_MIN, B_LABEL]], base["dry_sig"]
    assert base["bt_sig"] == [[close_minute_at(15), B_LABEL]], base["bt_sig"]


def test_min_rise_change_follows_source():
    """① 門檻改成比合成資料的前根漲幅（P_RISE）高一點點：兩條路徑都要跟著不發訊號。"""
    rise_mut = round(P_RISE + 0.0001, 6)
    base, mut = _run_child(_case_spec()), _run_child(_case_spec({"MIN_RISE_FROM_OPEN": rise_mut}))
    assert mut["S5"]["MIN_RISE_FROM_OPEN"] == mut["S5_RULE"]["MIN_RISE_FROM_OPEN"] == _snap(rise_mut)
    assert mut["dry_sig"] == [] and mut["bt_sig"] == [], f"前根 {P_RISE:+.4%} < 門檻 {rise_mut:.4%}，兩條路徑都不該有訊號"
    _assert_only_s5_fp_changed(base, mut)


def test_early_rules_change_follows_source():
    """CASE 的第一個限時分支倍數 1.0 → 1.2：第 45 分內累計最多 650 < 1.2 × 600，分支不成立，兩條路徑都不發訊號。"""
    (n1, _), second = CASE["EARLY_VOL_RULES"]
    early_mut = ((n1, 1.2), second)
    base, mut = _run_child(_case_spec()), _run_child(_case_spec({"EARLY_VOL_RULES": early_mut}))
    assert mut["S5"]["EARLY_VOL_RULES"] == mut["S5_RULE"]["EARLY_VOL_RULES"] == _snap(early_mut)
    assert mut["dry_sig"] == [] and mut["bt_sig"] == [], (mut["dry_sig"], mut["bt_sig"])
    _assert_only_s5_fp_changed(base, mut)


def test_take_profit_change_follows_source():
    """未改時四處的 TAKE_PROFIT 都等於 EXIT_PARAMS；改成另一個值（由來源推導，一定不同）後四處都跟著變。"""
    tp_now = s5_signal.EXIT_PARAMS["TAKE_PROFIT"]
    tp_mut = round(tp_now + 0.005, 6)
    assert tp_mut != tp_now
    base, mut = _run_child(_case_spec()), _run_child(_case_spec(EXIT_PARAMS={"TAKE_PROFIT": tp_mut}))
    for where in ("CONFIG", "S5_CONFIG", "dry_cfg", "bt_cfg"):
        assert base[where]["TAKE_PROFIT"] == _snap(tp_now), (where, base[where])
        assert mut[where]["TAKE_PROFIT"] == _snap(tp_mut), f"{where} 的 TAKE_PROFIT 沒跟著 EXIT_PARAMS 變"
    assert mut["dry_sig"] == base["dry_sig"] and mut["bt_sig"] == base["bt_sig"], "止盈不該影響進場訊號"
    _assert_only_s5_fp_changed(base, mut)


def _sentinel(v):
    if isinstance(v, bool):
        return not v
    if v is None:
        return 6
    if isinstance(v, (int, float)):
        return round(v + 0.0123, 6)
    if isinstance(v, tuple):
        return [[20, 0.7]]
    return f"{v}__sentinel__"


def test_every_param_key_is_wired_in_both_paths():
    """DEFAULT_PARAMS 與 EXIT_PARAMS 的**每一個**鍵同時換成哨兵值，回測與 dry run 都要跟著變。
       日後在來源加了鍵卻沒接到呼叫端，這裡會失敗。"""
    spec = {"DEFAULT_PARAMS": {k: _sentinel(v) for k, v in s5_signal.DEFAULT_PARAMS.items()},
            "EXIT_PARAMS": {k: _sentinel(v) for k, v in s5_signal.EXIT_PARAMS.items()}, "signals": False}
    base, mut = _run_child({"signals": False}), _run_child(spec)
    for k, v in spec["DEFAULT_PARAMS"].items():
        want = _snap(tuple(tuple(x) for x in v) if k == "EARLY_VOL_RULES" else v)
        assert want != base["S5"][k], f"{k} 的哨兵值與現值相同"
        assert mut["S5"][k] == want, f"回測 S5 沒有接上 DEFAULT_PARAMS[{k!r}]：{mut['S5'][k]}"
        assert mut["S5_RULE"][k] == want, f"dry run S5_RULE 沒有接上 DEFAULT_PARAMS[{k!r}]：{mut['S5_RULE'][k]}"
    for k, v in spec["EXIT_PARAMS"].items():
        for where in ("CONFIG", "S5_CONFIG", "dry_cfg"):
            assert mut[where][k] == _snap(v), f"{where} 沒有接上 EXIT_PARAMS[{k!r}]：{mut[where][k]}"
    _assert_only_s5_fp_changed(base, mut)


def test_callers_really_call_s5_signal_functions():
    """把 s5_signal 的函式換掉，兩條路徑的輸出都要跟著變——證明呼叫端沒有自己留一份公式。
         first_per_hour → 永遠 False：兩條路徑都不該有訊號
         rise_from_open → 常數 0.4242：dry run s5_rise 與回測 rise_open 都變成 0.4242"""
    base = _run_child(_case_spec())
    assert base["dry_sig"] and base["bt_sig"]
    fph = _run_child(_case_spec(patch="first_per_hour"))
    assert fph["dry_sig"] == [] and fph["bt_sig"] == [], (fph["dry_sig"], fph["bt_sig"])
    rise = _run_child(_case_spec(patch="rise_from_open"))
    assert abs(base["dry_rise_at_c"] - P_RISE) < 1e-12 and abs(base["bt_rise_at_c"] - P_RISE) < 1e-12
    assert rise["dry_rise_at_c"] == 0.4242, f"dry run 沒有用 s5_signal.rise_from_open：{rise['dry_rise_at_c']}"
    assert rise["bt_rise_at_c"] == 0.4242, f"回測沒有用 s5_signal.rise_from_open：{rise['bt_rise_at_c']}"


# ============================== 4. dry run 帳本指紋 ==============================
PINNED_FINGERPRINTS = {"watch": "7699d57dedbd", "rev": "319fbd7c392b", "rev_wide": "18d0a4c3d763",
                       "s4": "9d334afe808a", "s5": "e174a6d9ea1c"}


def test_dryrun_book_fingerprints_are_pinned():
    """五本帳目前的指紋（G5 前後相同，與 state/dryrun_state.json 記錄的一致）。
       刻意釘死：指紋一變，dry run 下一次執行就會把那本帳的前瞻紀錄清空，而且不會有任何錯誤、不會通知任何人。
         * 不是故意的（例如只是整理程式碼）→ 改動有問題，找出哪個生效值變了。
         * 是故意改參數 → 確認接受那本帳的前瞻紀錄歸零之後，再把這裡的指紋更新成新值。
       **絕對不要去改 state/dryrun_state.json 讓指紋對上**，那等於把問題藏起來。"""
    fp = _run_child({"signals": False})["fp"]
    changed = {b: (PINNED_FINGERPRINTS[b], fp[b]) for b in FIVE_BOOKS if fp[b] != PINNED_FINGERPRINTS[b]}
    assert not changed, "帳本指紋改變（釘住值 → 目前值）：" + ", ".join(f"{b}: {o} → {n}" for b, (o, n) in changed.items())


# ============================== 5. evaluate() 本身 ==============================
def test_evaluate_known_case():
    """CASE_RULES（顯式傳入）下的已知答案；期望值全部由合成資料常數與 CASE 推導。"""
    with _offline():
        df = synth_1m()
        ev = s5_signal.evaluate(df, MIN, CASE_RULES)
        assert tuple(ev.columns) == s5_signal.EVALUATE_COLUMNS
        assert _signals_in_c(ev) == [(BREAK_MIN, 2)], _signals_in_c(ev)
        assert s5_signal.branch_label(2, CASE_RULES) == B_LABEL
        row = ev[(ev["hid"] == T_C // HOUR) & (ev["minute"] == BREAK_MIN)].iloc[0]
        assert abs(row["rise"] - P_RISE) < 1e-12
        assert abs(row["volx"] - sum(C_VOLS[:BREAK_MIN]) / (P_VOL * 60)) < 1e-12
        assert abs(row["above"] - (C_ABOVE / P_HIGH - 1)) < 1e-12
        assert bool(row["rise_ok"]) and bool(row["burst_ok"]) and bool(row["break_ok"])
        assert (s5_signal.signal(df, MIN, CASE_RULES) == ev["signal"]).all()
        assert int((ev["signal"] == -1).sum()) == 1, "每小時最多一次，且其他小時不該有訊號"


def test_evaluate_is_bar_period_independent():
    """同一段行情用 1M / 5M / 15M 表達：同一小時進場，分支相同；進場分鐘 = 第一根收盤 ≥ BREAK_MIN 的小K。"""
    with _offline():
        df = synth_1m()
        assert _signals_in_c(s5_signal.evaluate(df, MIN, CASE_RULES)) == [(close_minute_at(1), 2)]
        assert _signals_in_c(s5_signal.evaluate(agg(df, 5), 5 * MIN, CASE_RULES)) == [(close_minute_at(5), 2)]
        assert _signals_in_c(s5_signal.evaluate(agg(df, 15), 15 * MIN, CASE_RULES)) == [(close_minute_at(15), 2)]


def test_evaluate_guard_paths():
    with _offline():
        R = CASE_RULES
        assert _signals_in_c(s5_signal.evaluate(synth_1m(drop_prev_minute=30), MIN, R)) == [], "前一小時缺一根 → 不完整"
        ev = s5_signal.evaluate(synth_1m(prev_vol=0.0), MIN, R)
        assert _signals_in_c(ev) == [] and ev.loc[ev["hid"] == T_C // HOUR, "volx"].isna().all(), "前一小時量 0 → NaN"
        ev = s5_signal.evaluate(synth_1m(prev_open=0.0), MIN, R)
        assert _signals_in_c(ev) == [] and ev.loc[ev["hid"] == T_C // HOUR, "rise"].isna().all(), "前一小時開盤 0 → NaN"
        ev = s5_signal.evaluate(synth_1m(), MIN, R)
        first = ev[ev["hid"] == ev["hid"].min()]
        assert first["rise"].isna().all() and not first["complete"].any() and (first["signal"] == 0).all()


def test_evaluate_does_not_mutate_input_and_keeps_index():
    with _offline():
        df = synth_1m()
        df.index = df.index + 1000
        before = df.copy()
        ev = s5_signal.evaluate(df, MIN, CASE_RULES)
        assert df.equals(before) and list(df.columns) == list(before.columns)
        assert ev.index.equals(df.index)


def test_shipped_defaults_are_supported_by_evaluate():
    """出貨的 DEFAULT_PARAMS 必須是 evaluate() / dry run 支援的 v3 組合（不比對數值，只檢查「可用」）；
       params=None 等於傳入 DEFAULT_PARAMS。"""
    with _offline():
        assert s5_signal.params_problems(s5_signal.DEFAULT_PARAMS, MIN) == []
        df = synth_1m()
        assert s5_signal.evaluate(df, MIN).equals(s5_signal.evaluate(df, MIN, s5_signal.DEFAULT_PARAMS))


def test_evaluate_rejects_unsupported_params():
    with _offline():
        df = synth_1m()
        P = CASE_RULES
        for bad in ({"REQUIRE_VOL_BURST": False}, {"REQUIRE_HIGHER_HIGH": 1}, {"HH_MODE": "high"},
                    {"MIN_TURN24H": 20_000}, {"MAX_PRICE": 1.0}, {"FAST_VOL_MULT": 1.0},
                    {"EARLY_VOL_RULES": ((0, 1.0),)}, {"EARLY_VOL_RULES": "x"}):
            try:
                s5_signal.evaluate(df, MIN, {**P, **bad})
            except ValueError:
                continue
            raise AssertionError(f"evaluate() 沒有拒絕 {bad}")
        try:
            s5_signal.evaluate(df, MIN, {k: v for k, v in P.items() if k != "MIN_VOL_MULT"})
            raise AssertionError("缺鍵沒有報錯")
        except KeyError:
            pass
        try:
            s5_signal.evaluate(agg(df, 5), 5 * MIN, {**P, "EARLY_VOL_RULES": ((7, 1.0),)})
            raise AssertionError("5 分K 看不到第 7 分鐘的窗口，卻沒有拒絕")
        except ValueError:
            pass
        for bar in (90_000, 7_200_000, 0):
            try:
                s5_signal.evaluate(df, bar, P)
                raise AssertionError(f"bar_ms={bar} 沒有拒絕")
            except ValueError:
                pass
        try:
            s5_signal.evaluate(df, 60_000.0, P)
            raise AssertionError("float 的 bar_ms 沒有拒絕")
        except TypeError:
            pass
        assert s5_signal.params_problems(P, MIN) == []


def test_helpers_examples():
    """標籤與冷卻換算對**顯式傳入**的規則下斷言；預設行為只驗證「預設 = DEFAULT_PARAMS」，不比對預設的數值。"""
    R = CASE_RULES                                 # A = 3 倍、限時分支 (45, 1.0)、(15, 0.5)
    assert s5_signal.branch_label(0, R) == "-"
    assert s5_signal.branch_label(1, R) == "3倍"
    assert s5_signal.branch_label(2, R) == B_LABEL
    assert s5_signal.branch_label(4, R) == "15分0.5倍"
    assert s5_signal.branch_label(7, R) == "3倍+45分1倍+15分0.5倍"
    assert all(s5_signal.branch_label(c) == s5_signal.branch_label(c, s5_signal.DEFAULT_PARAMS) for c in range(8))
    assert s5_signal.early_rules_problems(CASE["EARLY_VOL_RULES"]) == []
    assert s5_signal.early_rules_problems(()) == []
    assert len(s5_signal.early_rules_problems(((61, 1.0), (True, 1.0), (15, 0.0), (15,)))) == 4
    assert s5_signal.early_rules_window_problems(CASE["EARLY_VOL_RULES"], 15 * MIN) == []
    assert s5_signal.early_rules_window_problems(CASE["EARLY_VOL_RULES"], 30 * MIN) == [(0, 45, 1.0, 30),
                                                                                       (1, 15, 0.5, 30)]
    assert s5_signal.bar_minutes(15 * MIN) == 15 and s5_signal.bars_in_hour(5 * MIN) == 12
    assert s5_signal.cooldown_bars(60, {"COOLDOWN_HOURS": 0}) == 1          # 0 小時 → 至少 1 根
    assert s5_signal.cooldown_bars(60, {"COOLDOWN_HOURS": 1.0}) == 60
    assert s5_signal.cooldown_bars(12, {"COOLDOWN_HOURS": 0.5}) == 6
    assert s5_signal.cooldown_bars(60) == s5_signal.cooldown_bars(60, s5_signal.DEFAULT_PARAMS)


# ============================== F4 ==============================
def test_import_does_not_leave_socket_patched():
    """本檔的離線籠子只在子行程與 _offline() 範圍內，import 本檔、跑完測試，本行程的 socket 都必須是原函式。"""
    _run_child({"signals": False})
    with _offline():
        pass
    now = {"connect": socket.socket.connect, "connect_ex": socket.socket.connect_ex,
           "create_connection": socket.create_connection, "getaddrinfo": socket.getaddrinfo}
    for name, fn in PRISTINE_SOCKET.items():
        assert now[name] is fn, f"socket.{name} 被換掉了（實際 {now[name]!r}）"


# ============================== 不用 pytest 也能跑 ==============================
if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS  {name}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL  {name}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
