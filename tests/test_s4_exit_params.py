# -*- coding: utf-8 -*-
"""
G1b 驗收測試 — 策略4 出場參數只有 strategy/s4_signal.py 的 EXIT_PARAMS 一份來源。

這張任務是純粹的常數搬家，但失誤的代價特別高：pionex_dryrun.book_fingerprint() 把
TAKE_PROFIT / STOP_LOSS / RESOLVE_INTERVALS 等出場設定與整個 S4 字典算進帳本指紋，
指紋一變，下一次 dry run 就會把那本帳的前瞻紀錄**靜默**清空。所以這裡釘住的是：

  1. 回測（pionex_strategy4.main() 的設定步驟）與 dry run（use_config("s4")）的出場設定
     真的都來自 EXIT_PARAMS——在記憶體裡改 EXIT_PARAMS，兩條路徑都要跟著變
     （AC-3，有鑑別力：任何一處還寫著 0.04 的實作都會失敗），而且每一個鍵都要接上。
  2. 改出場參數只會讓 s4 帳本重新分版，不會波及 watch / rev / rev_wide。
  3. 四本帳目前的指紋（見 test_dryrun_book_fingerprints_are_pinned 的說明）。
  4. RESOLVE_INTERVALS 仍是 list，且放進 pb.CONFIG 的不是 EXIT_PARAMS 那一個物件。
     指紋用 json 序列化，list 與 tuple 算出來一樣，型別改了指紋看不出來，只能在這裡擋。
  5. 進場參數完全沒動：出場鍵沒有混進 DEFAULT_PARAMS，PARAM_KEYS 仍是 6 個（AC-4）。
  6. 範圍：兩支呼叫端的策略4 設定裡沒有出場參數的字面值；s4_signal 只 import
     numpy / pandas / 標準庫（AC-5）。

為什麼用子行程：pionex_strategy4.CONFIG 與 pionex_dryrun.S4_CONFIG 在 import 當下就建好，
pb.CONFIG 又是全域字典。每個情境各開一個全新的 Python 子行程，先改 EXIT_PARAMS、
再 import pionex_*，才看得到「import 時取值」是否真的取自來源，也不會互相污染全域狀態。
不改任何檔案，所有「暫改」都只存在子行程的記憶體裡。

離線籠子只裝在子行程裡（import pionex_* 之前），本檔自己的行程從不 patch socket——
import 本檔不會留下全域 socket patch（F4 的教訓，見 test_import_does_not_leave_socket_patched）。
子行程每次都會自我測試籠子是活的，攔不住就直接失敗。

全程離線，不打任何 API、不寫任何檔案；只讀三支原始碼做靜態檢查（state/ 與 output/ 完全不碰）。
不依賴 pytest：直接 `python tests/test_s4_exit_params.py` 會逐一跑完並印結果。
"""
import ast
import json
import os
import socket
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

# F4：在 import 任何專案模組**之前**先抓住 socket 的原函式物件，最後比對仍是同一個。
PRISTINE_SOCKET = {
    "connect": socket.socket.connect,
    "connect_ex": socket.socket.connect_ex,
    "create_connection": socket.create_connection,
    "getaddrinfo": socket.getaddrinfo,
}

from live import config  # noqa: E402
from strategy import s4_signal  # noqa: E402

EXIT_KEYS = ("EXIT_MODE", "TAKE_PROFIT", "STOP_LOSS", "MAX_HOLD_HOURS",
             "RESOLVE_SAME_BAR_WITH_5M", "RESOLVE_INTERVALS")
ENTRY_KEYS = ("MIN_RET_2H", "MIN_VOL_RATIO", "MAX_CLOSE_POS", "MAX_TURN24H", "MIN_TURN24H",
              "COOLDOWN_HOURS")
BOOKS = ("watch", "rev", "rev_wide", "s4")
OTHER_BOOKS = ("watch", "rev", "rev_wide")

# ============================== 子行程 ==============================
_CHILD = r'''
import copy, json, socket, sys

class NetworkBlocked(RuntimeError):
    pass

def _blocked(*a, **k):
    raise NetworkBlocked("tests/test_s4_exit_params.py：離線測試禁止連網")

socket.socket.connect = _blocked
socket.socket.connect_ex = _blocked
socket.create_connection = _blocked
socket.getaddrinfo = _blocked
try:
    socket.create_connection(("example.invalid", 80), timeout=1)
    cage_ok = False
except NetworkBlocked:
    cage_ok = True

sys.path.insert(0, sys.argv[1])
mutations = json.loads(sys.argv[2])

def snap(v):
    return {"type": type(v).__name__, "repr": repr(v)}

# 先改來源、再 import 呼叫端：呼叫端在 import 當下取值，這樣才測得到它們是不是真的從來源取
from strategy import s4_signal
for k, v in mutations.items():
    s4_signal.EXIT_PARAMS[k] = v
source_list = s4_signal.EXIT_PARAMS["RESOLVE_INTERVALS"]

import pionex_backtest as pb
import pionex_strategy4 as s4m
import pionex_dryrun as dr
pristine = copy.deepcopy(pb.CONFIG)
out = {"cage_ok": cage_ok, "exit_params": {k: snap(v) for k, v in s4_signal.EXIT_PARAMS.items()}}

# ---- 回測路徑：重現 pionex_strategy4.main() 開頭的設定步驟 ----
pb.CONFIG.update(s4m.CONFIG)
pb.CONFIG["COOLDOWN_BARS"] = s4_signal.cooldown_bars(s4m.H(), s4m.S4)
out["s4main"] = {k: snap(v) for k, v in pb.CONFIG.items()}
out["s4main_list_is_source"] = pb.CONFIG["RESOLVE_INTERVALS"] is source_list

# ---- dry run 路徑：從乾淨的 pb.CONFIG 開始，照 apply_param_versioning 的順序算四本帳指紋 ----
pb.CONFIG.clear()
pb.CONFIG.update(pristine)
out["fp"] = {b: dr.book_fingerprint(b) for b in dr.BOOKS}
out["books"] = {}
for b in dr.BOOKS:
    dr.use_config(b)
    out["books"][b] = {k: snap(v) for k, v in pb.CONFIG.items()}
    if b == "s4":
        out["s4_list_is_source"] = pb.CONFIG["RESOLVE_INTERVALS"] is source_list
out["S2_CONFIG"] = {k: snap(v) for k, v in dr.S2_CONFIG.items()}
print("@@RESULT@@" + json.dumps(out, ensure_ascii=True, sort_keys=True))
'''

_CACHE = {}


def _run_child(mutations=None):
    """開一個全新子行程：先在記憶體裡改 EXIT_PARAMS，再 import pionex_*，回傳兩條路徑的生效設定與指紋。"""
    mutations = mutations or {}
    key = json.dumps(mutations, sort_keys=True)
    if key not in _CACHE:
        env = dict(os.environ, PYTHONIOENCODING="utf-8")
        r = subprocess.run([sys.executable, "-c", _CHILD, REPO_ROOT, key], cwd=REPO_ROOT, env=env,
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


def _diff_keys(a, b):
    return sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k))


def _assert_propagates_and_reversions_only_s4(key, value):
    """AC-3 的共同檢查：EXIT_PARAMS[key] 改成 value 之後——
       兩條路徑都變成 value、整份 pb.CONFIG 只差這個鍵、s4 指紋改變、其他三本帳指紋與設定不變。"""
    base, mut = _run_child(), _run_child({key: value})
    want = _snap(value)
    assert want != base["s4main"][key], f"哨兵值 {value!r} 必須與現值不同，否則測不出東西"
    assert mut["s4main"][key] == want, f"回測路徑的 {key} 沒跟著 EXIT_PARAMS 變：{mut['s4main'][key]}"
    assert mut["books"]["s4"][key] == want, f"dry run s4 的 {key} 沒跟著 EXIT_PARAMS 變：{mut['books']['s4'][key]}"
    assert _diff_keys(base["s4main"], mut["s4main"]) == [key], "回測路徑的 pb.CONFIG 多變了別的鍵"
    assert _diff_keys(base["books"]["s4"], mut["books"]["s4"]) == [key], "dry run s4 的 pb.CONFIG 多變了別的鍵"
    assert mut["fp"]["s4"] != base["fp"]["s4"], "改了出場參數，s4 指紋卻沒變：dry run 會把新舊參數的紀錄混在一起"
    for b in OTHER_BOOKS:
        assert mut["fp"][b] == base["fp"][b], f"改策略4 的 {key} 波及了 {b} 的指紋"
        assert mut["books"][b] == base["books"][b], f"改策略4 的 {key} 波及了 {b} 的生效設定"
    assert mut["S2_CONFIG"] == base["S2_CONFIG"], "改策略4 的出場參數改到了 S2_CONFIG"


def _sentinel(v):
    """依型別產生一個一定與現值不同、可以 json 傳遞的哨兵值。"""
    if isinstance(v, bool):
        return not v
    if v is None:
        return 6
    if isinstance(v, (int, float)):
        return round(v + 0.0123, 6)
    if isinstance(v, list):
        return list(v) + ["__sentinel__"]
    return f"{v}__sentinel__"


# ============== 來源本身 ==============
def test_exit_params_has_the_six_exit_keys():
    assert tuple(s4_signal.EXIT_PARAMS) == EXIT_KEYS, f"EXIT_PARAMS 的鍵變了：{tuple(s4_signal.EXIT_PARAMS)}"


def test_resolve_intervals_is_a_list():
    """指紋看不出 list 與 tuple 的差別，型別只能在這裡釘。"""
    v = s4_signal.EXIT_PARAMS["RESOLVE_INTERVALS"]
    assert type(v) is list, f"RESOLVE_INTERVALS 必須是 list，實際 {type(v).__name__}"
    assert all(isinstance(x, str) for x in v), v


def test_exit_params_returns_a_deep_copy():
    got = s4_signal.exit_params()
    assert got == s4_signal.EXIT_PARAMS
    assert got is not s4_signal.EXIT_PARAMS
    assert got["RESOLVE_INTERVALS"] is not s4_signal.EXIT_PARAMS["RESOLVE_INTERVALS"], "list 與來源共用物件"
    before = list(s4_signal.EXIT_PARAMS["RESOLVE_INTERVALS"])
    got["RESOLVE_INTERVALS"].append("X")
    got["TAKE_PROFIT"] = 999
    assert s4_signal.EXIT_PARAMS["RESOLVE_INTERVALS"] == before, "改拷貝的 list 汙染了來源"
    assert s4_signal.EXIT_PARAMS["TAKE_PROFIT"] != 999


# ============== AC-4：進場參數完全沒動 ==============
def test_exit_keys_are_not_in_default_params():
    """出場鍵不可以混進 DEFAULT_PARAMS：S4 = {**DEFAULT_PARAMS, ...}，多一個鍵 s4 指紋就變。"""
    assert tuple(s4_signal.DEFAULT_PARAMS) == ENTRY_KEYS, tuple(s4_signal.DEFAULT_PARAMS)
    assert s4_signal.PARAM_KEYS == ENTRY_KEYS and len(s4_signal.PARAM_KEYS) == 6
    assert not set(s4_signal.EXIT_PARAMS) & set(s4_signal.DEFAULT_PARAMS)
    assert config.strategy_params() == s4_signal.DEFAULT_PARAMS
    assert set(config.strategy_params()) == set(ENTRY_KEYS)


# ============== 兩條路徑都取自來源 ==============
def test_both_paths_use_exit_params():
    res = _run_child()
    for k in s4_signal.EXIT_PARAMS:
        want = _snap(s4_signal.EXIT_PARAMS[k])
        assert res["s4main"][k] == want, f"回測路徑 {k}={res['s4main'][k]}，來源是 {want}"
        assert res["books"]["s4"][k] == want, f"dry run s4 {k}={res['books']['s4'][k]}，來源是 {want}"


def test_resolve_intervals_is_not_shared_with_the_source():
    """pb.CONFIG.update() 會把 list 物件原封不動放進全域字典；兩條路徑都必須放拷貝。"""
    res = _run_child()
    assert res["s4main_list_is_source"] is False, "回測路徑的 RESOLVE_INTERVALS 與 EXIT_PARAMS 是同一個 list"
    assert res["s4_list_is_source"] is False, "dry run s4 的 RESOLVE_INTERVALS 與 EXIT_PARAMS 是同一個 list"


# ============== AC-3：有鑑別力的單一來源測試 ==============
def test_take_profit_change_follows_source_and_reversions_only_s4():
    _assert_propagates_and_reversions_only_s4("TAKE_PROFIT", 0.035)


def test_resolve_intervals_change_follows_source_and_reversions_only_s4():
    _assert_propagates_and_reversions_only_s4("RESOLVE_INTERVALS", ["5M", "1M"])


def test_every_exit_key_is_wired_in_both_paths():
    """EXIT_PARAMS 的**每一個**鍵同時換成哨兵值，兩條路徑都要跟著變。
       日後在 EXIT_PARAMS 加了鍵卻沒接到 pionex_strategy4.CONFIG（或 dry run），這裡會失敗。
       MAX_HOLD_HOURS / RESOLVE_SAME_BAR_WITH_5M 以前是 s4 從 S2_CONFIG 繼承來的：
       現在 S2_CONFIG 不變、s4 卻跟著哨兵值走，證明策略4 已不再依賴策略2 的設定。"""
    sentinels = {k: _sentinel(v) for k, v in s4_signal.EXIT_PARAMS.items()}
    base, mut = _run_child(), _run_child(sentinels)
    for k, v in sentinels.items():
        want = _snap(v)
        assert want != base["s4main"][k], f"{k} 的哨兵值與現值相同"
        assert mut["s4main"][k] == want, f"回測路徑沒有接上 EXIT_PARAMS[{k!r}]：{mut['s4main'][k]}"
        assert mut["books"]["s4"][k] == want, f"dry run s4 沒有接上 EXIT_PARAMS[{k!r}]：{mut['books']['s4'][k]}"
    for b in OTHER_BOOKS:
        assert mut["fp"][b] == base["fp"][b], f"改策略4 的出場參數波及了 {b} 的指紋"
    assert mut["S2_CONFIG"] == base["S2_CONFIG"]


# ============== dry run 帳本指紋 ==============
PINNED_FINGERPRINTS = {
    "watch": "7699d57dedbd",
    "rev": "319fbd7c392b",
    "rev_wide": "18d0a4c3d763",
    "s4": "9d334afe808a",
}


def test_dryrun_book_fingerprints_are_pinned():
    """四本帳目前的指紋（2026-09-23，G1b 前後相同，與 state/dryrun_state.json 記錄的一致）。

       刻意釘死：指紋一變，dry run 下一次執行就會把那本帳的前瞻紀錄清空，而且不會有任何錯誤、
       不會通知任何人。這條測試失敗代表「這次改動會讓某本帳重新開始」：
         * 不是故意的（例如只是整理程式碼）→ 改動有問題，找出哪個生效值變了。
         * 是故意改參數 → 確認接受那本帳的前瞻紀錄歸零之後，再把這裡的指紋更新成新值。
       **絕對不要去改 state/dryrun_state.json 讓指紋對上**，那等於把問題藏起來。"""
    fp = _run_child()["fp"]
    changed = {b: (PINNED_FINGERPRINTS[b], fp[b]) for b in BOOKS if fp[b] != PINNED_FINGERPRINTS[b]}
    assert not changed, ("帳本指紋改變（釘住值 → 目前值）："
                         + ", ".join(f"{b}: {o} → {n}" for b, (o, n) in changed.items())
                         + "；合併後第一次 dry run 會清空這些帳本的前瞻紀錄，見本測試說明")


# ============== AC-5：範圍 ==============
def _is_literal(node):
    """數字 / 字串 / None / True / False，或全部由它們組成的 list / tuple / set，或 -數字。"""
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        return _is_literal(node.operand)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return all(_is_literal(e) for e in node.elts)
    return False


def _literal_exit_settings(src, target, name):
    """找出原始碼 src 裡對 target（設定字典名）以字面值設定出場鍵的地方，回傳 ["檔名:行號 鍵", ...]。
       涵蓋 target = {...}、target = dict(...)、target.update(...)、target[...] = ...；註解本來就不在 AST 裡。"""
    tree = ast.parse(src, filename=name)
    hits = []

    def check_dict(d, where):
        for k, v in zip(d.keys, d.values):
            if isinstance(k, ast.Constant) and k.value in EXIT_KEYS and _is_literal(v):
                hits.append(f"{where}:{v.lineno} {k.value}")

    def check_call(c, where):
        for kw in c.keywords:
            if kw.arg in EXIT_KEYS and _is_literal(kw.value):
                hits.append(f"{where}:{kw.value.lineno} {kw.arg}")
        for a in c.args:
            if isinstance(a, ast.Dict):
                check_dict(a, where)

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == target:
                    if isinstance(node.value, ast.Dict):
                        check_dict(node.value, name)
                    elif isinstance(node.value, ast.Call):
                        check_call(node.value, name)
                if (isinstance(t, ast.Subscript) and isinstance(t.value, ast.Name) and t.value.id == target
                        and isinstance(t.slice, ast.Constant) and t.slice.value in EXIT_KEYS
                        and _is_literal(node.value)):
                    hits.append(f"{name}:{node.lineno} {t.slice.value}")
        elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
              and node.func.attr == "update" and isinstance(node.func.value, ast.Name)
              and node.func.value.id == target):
            check_call(node, name)
    return hits


def _read(rel):
    with open(os.path.join(REPO_ROOT, rel), encoding="utf-8") as f:
        return f.read()


def test_no_exit_literals_in_strategy4_and_dryrun():
    hits = (_literal_exit_settings(_read("pionex_strategy4.py"), "CONFIG", "pionex_strategy4.py")
            + _literal_exit_settings(_read("pionex_dryrun.py"), "S4_CONFIG", "pionex_dryrun.py"))
    assert not hits, f"策略4 的出場參數仍有字面值（應取自 s4_signal.EXIT_PARAMS）：{hits}"


def test_literal_scanner_has_teeth():
    """反向對照：掃描器要抓得到 G1b 之前的寫法（字典字面值、dict()/update() 關鍵字、下標指定），
       否則上一條的 PASS 不算數。"""
    src = ('CONFIG = {"EXIT_MODE": "fixed", "TAKE_PROFIT": 0.04, "FEE_RATE": 0.0005,\n'
           '          "RESOLVE_INTERVALS": ["1M"]}\n'
           'S4_CONFIG = dict(A, STOP_LOSS=0.05)\n'
           'S4_CONFIG.update(RESOLVE_INTERVALS=["1M"], TAKE_PROFIT=0.04, INTERVAL="5M")\n'
           'S4_CONFIG["MAX_HOLD_HOURS"] = None\n'
           'S4_CONFIG.update(**exit_params())\n')
    assert len(_literal_exit_settings(src, "CONFIG", "t.py")) == 3      # FEE_RATE 不是出場鍵，不算
    assert len(_literal_exit_settings(src, "S4_CONFIG", "t.py")) == 4   # **展開與 INTERVAL 不算


def test_s4_signal_imports_only_numpy_pandas_stdlib():
    with open(os.path.join(REPO_ROOT, "strategy", "s4_signal.py"), encoding="utf-8") as f:
        tree = ast.parse(f.read())
    mods = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "s4_signal 不得相對 import 專案內其他模組"
            mods.add(node.module.split(".")[0])
    bad = sorted(m for m in mods if m not in ("numpy", "pandas") and m not in sys.stdlib_module_names)
    assert not bad, f"s4_signal 只能 import numpy / pandas / 標準庫，多了 {bad}"


def test_import_does_not_leave_socket_patched():
    """F4：本檔的離線籠子只裝在子行程裡，import 本檔、跑完子行程，本行程的 socket 都必須是原函式。"""
    _run_child()
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
