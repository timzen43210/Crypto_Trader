# -*- coding: utf-8 -*-
"""
G5 驗收測試 — 策略5 訊號核心只有 strategy/s5_signal.py 一份。

釘住的是：
  1. 相依（AC-7）：s5_signal 只 import numpy / pandas / 標準庫；在全新的直譯器裡 import strategy.s5_signal
     之後，sys.modules 裡沒有 pionex_*、requests、openpyxl、live、research。
  2. 參數：DEFAULT_PARAMS 的 11 個鍵、順序、值與**型別**（EARLY_VOL_RULES 是 tuple of tuple、
     COOLDOWN_HOURS 是 int 0、MIN_ABOVE_PH 是 float 0.0）。dry run 的 s5 帳本指紋用 json 序列化，
     看不出 tuple 與 list 的差別，型別只能在這裡擋；0 與 0.0 則會直接改變指紋。
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


def synth_1m(prev_open=100.0, prev_close=106.0, prev_vol=10.0, drop_prev_minute=None):
    """已知答案的 1 分K（與 TASK-010 的 c1 同型）：
         P（前一小時）開 prev_open、線性漲到 prev_close、第 51 根最高 106.5、每分量 prev_vol（預設合計 600）
         C（本小時）收盤分鐘 1～39 收 105（< 前高），40 起收 107（> 前高）；
           累計量：第 15 分 285（< 0.5 倍 = 300）、第 20 分 600（= 1 倍 → B 成立並黏著）、之後每分 2（全小時 680 < 2 倍）
       預設參數下：本小時唯一訊號在第 40 分，分支只有 B（code 2，標籤「30分1倍」）。"""
    w = _bars(T_C - 2 * HOUR, [100.0] * 60, [10.0] * 60, 100.0)
    closes = [100.0 + (prev_close - 100.0) * (i + 1) / 60 for i in range(60)]
    closes[-1] = prev_close
    p = _bars(T_C - HOUR, closes, [prev_vol] * 60, prev_open, high_at={50: 106.5})
    vols = [19.0] * 15 + [63.0] * 5 + [2.0] * 40
    c = _bars(T_C, [107.0 if m >= 40 else 105.0 for m in range(1, 61)], vols, 105.0)
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
def test_default_params_keys_values_and_types_are_pinned():
    """鍵、順序、值、型別全部釘死。改任何一項 dry run 的 s5 帳本就會換指紋（tuple→list 除外：指紋看不出來，
       所以型別只能在這裡擋）。真的要改參數時，確認接受 s5 帳本前瞻紀錄歸零後再更新這裡。"""
    P = s5_signal.DEFAULT_PARAMS
    assert tuple(P) == PARAM_KEYS and s5_signal.PARAM_KEYS == PARAM_KEYS, tuple(P)
    expect = {"MIN_RISE_FROM_OPEN": ("float", "0.06"), "MIN_VOL_MULT": ("float", "2.0"),
              "EARLY_VOL_RULES": ("tuple", "((30, 1.0), (15, 0.5))"), "REQUIRE_VOL_BURST": ("bool", "True"),
              "REQUIRE_HIGHER_HIGH": ("bool", "True"), "HH_MODE": ("str", "'price'"),
              "MIN_ABOVE_PH": ("float", "0.0"), "MIN_TURN24H": ("NoneType", "None"),
              "MAX_TURN24H": ("NoneType", "None"), "MAX_PRICE": ("NoneType", "None"),
              "COOLDOWN_HOURS": ("int", "0")}
    got = {k: (type(v).__name__, repr(v)) for k, v in P.items()}
    assert got == expect, {k: (got[k], expect[k]) for k in expect if got.get(k) != expect[k]}
    rules = P["EARLY_VOL_RULES"]
    assert all(type(r) is tuple and type(r[0]) is int and type(r[1]) is float for r in rules), rules


def test_exit_params_keys_and_separation():
    E = s5_signal.EXIT_PARAMS
    assert tuple(E) == EXIT_KEYS, tuple(E)
    assert {k: (type(v).__name__, v) for k, v in E.items()} == {
        "EXIT_MODE": ("str", "fixed"), "TAKE_PROFIT": ("float", 0.03), "STOP_LOSS": ("float", 0.05),
        "MAX_HOLD_HOURS": ("NoneType", None)}
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
    assert res["FEE"] == [0.0005, 0.0005]
    assert res["import_touched_pb_config"] is False


# ============================== 3. 單一來源（AC-6，有鑑別力）==============================
def _assert_only_s5_fp_changed(base, mut):
    assert mut["fp"]["s5"] != base["fp"]["s5"], "改了策略5 的參數，s5 指紋卻沒變：dry run 會把新舊參數的紀錄混在一起"
    for b in OTHER_BOOKS:
        assert mut["fp"][b] == base["fp"][b], f"改策略5 的參數波及了 {b} 的指紋"


def test_baseline_synthetic_case():
    """未改參數時：dry run（1 分K）第 40 分、回測（15 分K）第 45 分進場，分支都只有 B。"""
    base = _run_child()
    assert base["dry_sig"] == [[40, "30分1倍"]], base["dry_sig"]
    assert base["bt_sig"] == [[45, "30分1倍"]], base["bt_sig"]


def test_min_rise_change_follows_source():
    base, mut = _run_child(), _run_child({"DEFAULT_PARAMS": {"MIN_RISE_FROM_OPEN": 0.0601}})
    assert mut["S5"]["MIN_RISE_FROM_OPEN"] == mut["S5_RULE"]["MIN_RISE_FROM_OPEN"] == _snap(0.0601)
    assert mut["dry_sig"] == [] and mut["bt_sig"] == [], "前根 +6.00% < 6.01%，兩條路徑都不該有訊號"
    _assert_only_s5_fp_changed(base, mut)


def test_early_rules_change_follows_source():
    base, mut = _run_child(), _run_child({"DEFAULT_PARAMS": {"EARLY_VOL_RULES": [[30, 1.2], [15, 0.5]]}})
    assert mut["S5"]["EARLY_VOL_RULES"] == mut["S5_RULE"]["EARLY_VOL_RULES"] == _snap(((30, 1.2), (15, 0.5)))
    assert mut["dry_sig"] == [] and mut["bt_sig"] == [], "第 30 分累計 620 < 1.2 倍前根 720，B 不成立"
    _assert_only_s5_fp_changed(base, mut)


def test_take_profit_change_follows_source():
    base, mut = _run_child(), _run_child({"EXIT_PARAMS": {"TAKE_PROFIT": 0.035}})
    for where in ("CONFIG", "S5_CONFIG", "dry_cfg", "bt_cfg"):
        assert base[where]["TAKE_PROFIT"] == _snap(0.03), (where, base[where])
        assert mut[where]["TAKE_PROFIT"] == _snap(0.035), f"{where} 的 TAKE_PROFIT 沒跟著 EXIT_PARAMS 變"
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
    base = _run_child()
    assert base["dry_sig"] and base["bt_sig"]
    fph = _run_child({"patch": "first_per_hour"})
    assert fph["dry_sig"] == [] and fph["bt_sig"] == [], (fph["dry_sig"], fph["bt_sig"])
    rise = _run_child({"patch": "rise_from_open"})
    assert abs(base["dry_rise_at_c"] - 0.06) < 1e-12 and abs(base["bt_rise_at_c"] - 0.06) < 1e-12
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
    with _offline():
        df = synth_1m()
        ev = s5_signal.evaluate(df, MIN)
        assert tuple(ev.columns) == s5_signal.EVALUATE_COLUMNS
        assert _signals_in_c(ev) == [(40, 2)], _signals_in_c(ev)
        assert s5_signal.branch_label(2) == "30分1倍"
        row = ev[(ev["hid"] == T_C // HOUR) & (ev["minute"] == 40)].iloc[0]
        assert abs(row["rise"] - 0.06) < 1e-12 and abs(row["volx"] - 640 / 600) < 1e-12
        assert abs(row["above"] - (107.0 / 106.5 - 1)) < 1e-12
        assert bool(row["rise_ok"]) and bool(row["burst_ok"]) and bool(row["break_ok"])
        assert (s5_signal.signal(df, MIN) == ev["signal"]).all()
        assert int((ev["signal"] == -1).sum()) == 1, "每小時最多一次，且其他小時不該有訊號"


def test_evaluate_is_bar_period_independent():
    """同一段行情用 1M / 5M / 15M 表達：同一小時進場，分支相同；進場分鐘 = 第一根收盤 ≥ 40 分的小K。"""
    with _offline():
        df = synth_1m()
        assert _signals_in_c(s5_signal.evaluate(df, MIN)) == [(40, 2)]
        assert _signals_in_c(s5_signal.evaluate(agg(df, 5), 5 * MIN)) == [(40, 2)]
        assert _signals_in_c(s5_signal.evaluate(agg(df, 15), 15 * MIN)) == [(45, 2)]


def test_evaluate_guard_paths():
    with _offline():
        assert _signals_in_c(s5_signal.evaluate(synth_1m(drop_prev_minute=30), MIN)) == [], "前一小時缺一根 → 不完整"
        ev = s5_signal.evaluate(synth_1m(prev_vol=0.0), MIN)
        assert _signals_in_c(ev) == [] and ev.loc[ev["hid"] == T_C // HOUR, "volx"].isna().all(), "前一小時量 0 → NaN"
        ev = s5_signal.evaluate(synth_1m(prev_open=0.0), MIN)
        assert _signals_in_c(ev) == [] and ev.loc[ev["hid"] == T_C // HOUR, "rise"].isna().all(), "前一小時開盤 0 → NaN"
        ev = s5_signal.evaluate(synth_1m(), MIN)
        first = ev[ev["hid"] == ev["hid"].min()]
        assert first["rise"].isna().all() and not first["complete"].any() and (first["signal"] == 0).all()


def test_evaluate_does_not_mutate_input_and_keeps_index():
    with _offline():
        df = synth_1m()
        df.index = df.index + 1000
        before = df.copy()
        ev = s5_signal.evaluate(df, MIN)
        assert df.equals(before) and list(df.columns) == list(before.columns)
        assert ev.index.equals(df.index)


def test_evaluate_rejects_unsupported_params():
    with _offline():
        df = synth_1m()
        P = s5_signal.DEFAULT_PARAMS
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
                s5_signal.evaluate(df, bar)
                raise AssertionError(f"bar_ms={bar} 沒有拒絕")
            except ValueError:
                pass
        try:
            s5_signal.evaluate(df, 60_000.0)
            raise AssertionError("float 的 bar_ms 沒有拒絕")
        except TypeError:
            pass
        assert s5_signal.params_problems(P, MIN) == []


def test_helpers_examples():
    assert s5_signal.branch_label(0) == "-"
    assert s5_signal.branch_label(1) == "2倍"
    assert s5_signal.branch_label(4) == "15分0.5倍"
    assert s5_signal.branch_label(7) == "2倍+30分1倍+15分0.5倍"
    assert s5_signal.early_rules_problems(((30, 1.0), (15, 0.5))) == []
    assert s5_signal.early_rules_problems(()) == []
    assert len(s5_signal.early_rules_problems(((61, 1.0), (True, 1.0), (15, 0.0), (15,)))) == 4
    assert s5_signal.early_rules_window_problems(((30, 1.0), (15, 0.5)), 30 * MIN) == [(1, 15, 0.5, 30)]
    assert s5_signal.bar_minutes(15 * MIN) == 15 and s5_signal.bars_in_hour(5 * MIN) == 12
    assert s5_signal.cooldown_bars(60) == 1 and s5_signal.cooldown_bars(60, {"COOLDOWN_HOURS": 1.0}) == 60


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
