# -*- coding: utf-8 -*-
"""
B4′ (live.store) 驗收測試 — 訊號表 + 名目部位表，重啟之後狀態一定要找得回來。

  AC-1  建表冪等；寫入後查回，值與型別一致（價格 float、時間 int），資料庫本身也拒收錯型別
  AC-2  s4 / s5 同一個幣可以各有一筆 open；同 strategy 同 symbol 第二筆 open 拋例外；平倉後可再開。
        另外繞過本模組直接對資料庫下 SQL，證明擋下來的是部分唯一索引本身，不是 Python 的前置檢查
  AC-3  「訊號寫進去了、開倉失敗」→ 訊號表也不留。用真的會失敗的路徑證明：違反唯一索引、
        開倉當下拋任意例外、子行程在交易中途被 os._exit 砍掉；並用 trace 證明訊號的 INSERT
        確實執行過（不是在寫之前就被擋掉）。反向對照：不包交易的寫法同一個情境會留下孤兒
  AC-4  寫入數筆、平倉幾筆、關連線重開 → 未平倉 / 未發布查詢恰好是剩下的那些；
        子行程寫完不關連線直接 os._exit（模擬當機），已提交的內容一筆不少
  AC-5  對不存在 / 已平倉的部位平倉 → PositionNotOpenError（不是回傳值、不是靜默成功），
        資料庫一個 bit 都沒變；成功的平倉只改到目標那一筆的那四個欄位
  AC-6  策略層紀錄的 user_id 是 config.STRATEGY_USER_ID，不是 NULL；對照組證明 NULL 會讓
        同一個部分唯一索引失效
  AC-7  LIVE_DB_PATH 在 runtime/ 底下、被 .gitignore 擋住、列在 execution_params()；
        python -m live exit 0 並列出它且不建目錄；模組名不撞名；store 只 import 標準庫與 live

captain 補充 PRD（2026-09-26，與 A2 整合）：
  S-1    策略名單只有 config.STRATEGIES 一份：store 在呼叫當下讀它（改了名單 store 立刻跟著變）
  S-2a   features 的 ±inf / NaN 收下、原樣讀回（型別 float），連同重開之後
  S-2b   直接吃 A2 真正建出來的 EntryEvent(...).features（mappingproxy，NaN 已被轉成 None、inf 保留）
  S-2c   「修改前的實作跑這兩條必須失敗」不在本檔：用 git show 取出舊版在暫存副本裡跑，
         證據放在 AI-Team/data/workspace/TASK-011/（s2c_old_impl.py / s2c_old_impl.log）

資料庫一律開在系統暫存目錄，每個測試結束前關閉連線再刪目錄（刪不掉就讓它拋：代表有連線沒關）。
runner 會確認整輪測試沒有在真正的 runtime/ 底下留下任何東西。

全程離線：runner 把每個測試包在 socket 籠子裡（connect / connect_ex / create_connection 一律
拋 NetworkBlocked 並記錄），籠子只在測試執行期間武裝，import 本檔不留任何全域 patch。
時間一律用注入的假時鐘，不 sleep、不讀真時鐘。

測試資料的止盈 / 止損價由 strategy.s4_signal.exit_params() 推出來，不在這裡寫策略參數的數值。
不依賴 pytest：直接 `python tests/test_store.py` 會逐一跑完並印結果。
"""
import ast
import importlib.util
import json
import math
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import types
from collections import OrderedDict
from collections.abc import Mapping
from contextlib import closing, contextmanager
from decimal import Decimal

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import numpy as np  # noqa: E402

from live import config, paths, store  # noqa: E402
from live.signal_events import DIRECTION_SHORT, EntryEvent  # noqa: E402
from strategy import s4_signal  # noqa: E402

T0 = 1_758_000_000_000          # 2025-09-16 前後的 UTC epoch 毫秒
BAR_MS = 5 * 60 * 1000
SUBSCRIBER = "tg:1234567"       # 第二階段的真實 user_id 長這樣（只用來證明不同 user_id 互不干擾）

# 判定特徵：s4 與 s5 的欄位不同；s5 刻意含中文與 numpy 純量，驗 JSON 欄位原樣存回
FEATURES = {
    "s4": {"ret2h": 0.1834, "volr": 6.72, "cpos": 0.912, "turn": 3.4e6},
    "s5": {"漲幅": 0.31, "量倍數": 12.5, "爆量分支": "①"},
}


# ============================== 離線籠子 ==============================
class NetworkBlocked(RuntimeError):
    """測試期間有東西想連網。"""


NET_ATTEMPTS = []
_PRISTINE_SOCKET = {
    "connect": socket.socket.connect,
    "connect_ex": socket.socket.connect_ex,
    "create_connection": socket.create_connection,
}
_cage_depth = 0


def _blocked(name):
    def fn(*args, **kwargs):
        NET_ATTEMPTS.append(name)
        raise NetworkBlocked("離線籠子攔下 %s" % name)
    return fn


@contextmanager
def offline():
    """武裝 socket 籠子；可巢狀，只有最外層離開時才把原函式放回去。"""
    global _cage_depth
    if _cage_depth == 0:
        socket.socket.connect = _blocked("connect")
        socket.socket.connect_ex = _blocked("connect_ex")
        socket.create_connection = _blocked("create_connection")
    _cage_depth += 1
    try:
        yield
    finally:
        _cage_depth -= 1
        if _cage_depth == 0:
            socket.socket.connect = _PRISTINE_SOCKET["connect"]
            socket.socket.connect_ex = _PRISTINE_SOCKET["connect_ex"]
            socket.create_connection = _PRISTINE_SOCKET["create_connection"]


# ============================== 工具 ==============================
class FakeClock:
    """每呼叫一次往前走 step 毫秒。"""

    def __init__(self, start=T0, step=1000):
        self.now = start
        self.step = step
        self.calls = 0

    def __call__(self):
        self.calls += 1
        self.now += self.step
        return self.now


@contextmanager
def _tempdir():
    tmp = tempfile.mkdtemp(prefix="b4_store_")
    try:
        yield tmp
    finally:
        shutil.rmtree(tmp)          # 刪不掉就讓它拋：代表有連線沒關（Windows）


@contextmanager
def _opened(tmp, clock=None, name="live.sqlite3"):
    """在暫存目錄開一個 store，離開時一定關。"""
    s = store.open_store(os.path.join(tmp, name), clock=clock or FakeClock())
    try:
        yield s
    finally:
        s.close()


def _levels(price):
    """止盈 / 止損價由策略的出場參數推出來（做空：跌 TAKE_PROFIT 止盈、漲 STOP_LOSS 止損）。"""
    ex = s4_signal.exit_params()
    return price * (1 - ex["TAKE_PROFIT"]), price * (1 + ex["STOP_LOSS"])


def _entry(signal_id, strategy="s4", symbol="AAA_USDT", price=2.5, **over):
    tp, sl = _levels(price)
    kw = dict(signal_id=signal_id, user_id=config.STRATEGY_USER_ID, strategy=strategy,
              symbol=symbol, side="short", bar_open_ms=T0 - BAR_MS, signal_price=price,
              take_profit_price=tp, stop_loss_price=sl,
              features=dict(FEATURES.get(strategy, FEATURES["s4"])))   # 打錯的 strategy 也要組得出參數
    kw.update(over)
    return kw


def _raw(path):
    """繞過本模組的原生連線（自己管交易、開外鍵）。用 closing() 包起來確保會關。"""
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA foreign_keys=ON")
    return closing(conn)


def _dump(path):
    """兩張表的完整內容 + 每一格的儲存型別，用來比對「資料庫有沒有被動到」。"""
    with _raw(path) as c:
        out = {}
        for table in store.TABLES:
            cols = [r[1] for r in c.execute("PRAGMA table_info(%s)" % table)]
            sel = ", ".join("%s, typeof(%s)" % (col, col) for col in cols)
            out[table] = c.execute("SELECT %s FROM %s ORDER BY signal_id" % (sel, table)).fetchall()
        return out


def _schema_objects(path):
    with _raw(path) as c:
        return c.execute("SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY name").fetchall()


def _expect(exc_type, fn, *fragments):
    """fn() 必須拋 exc_type，且訊息含所有 fragments。回傳例外物件。"""
    try:
        fn()
    except exc_type as e:
        for f in fragments:
            assert f in str(e), "例外訊息裡找不到 %r：%s" % (f, e)
        return e
    raise AssertionError("期待拋出 %s，但沒有" % exc_type.__name__)


def _run_child(body, **consts):
    """在乾淨的子行程跑 body。回傳 (returncode, stdout, stderr)，輸出用 ASCII + backslashreplace 解碼。"""
    head = ["import json, os, sys", "sys.path.insert(0, %r)" % REPO_ROOT]
    head += ["%s = %s" % (k, ascii(v)) for k, v in consts.items()]
    code = "\n".join(head) + "\n" + textwrap.dedent(body)
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, stdin=subprocess.DEVNULL,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    return (r.returncode, r.stdout.decode("ascii", "backslashreplace"),
            r.stderr.decode("ascii", "backslashreplace"))


# ============================== AC-1：建表冪等、值與型別 ==============================
def test_ac1_init_is_idempotent():
    with _tempdir() as tmp:
        path = os.path.join(tmp, "live.sqlite3")
        s1 = store.open_store(path, clock=FakeClock())
        s2 = store.open_store(path, clock=FakeClock())       # 同時第二次開：不出錯、不重建
        try:
            first = _schema_objects(path)
            s1.record_entry(**_entry("s4-1"))
        finally:
            s1.close()
            s2.close()
        with _opened(tmp) as s3:                            # 第三次開：資料還在、物件完全相同
            assert s3.get_position("s4-1")["status"] == "open"
        assert _schema_objects(path) == first, "重複初始化改動了 schema"

        names = {row[1] for row in first if not row[1].startswith("sqlite_autoindex")}
        assert names == set(store.TABLES) | {store.OPEN_POSITION_INDEX}, names
        with _raw(path) as c:
            assert c.execute("PRAGMA user_version").fetchone()[0] == store.SCHEMA_VERSION
            idx = {r[1]: r for r in c.execute("PRAGMA index_list(positions)")}
            _seq, _name, unique, _origin, partial = idx[store.OPEN_POSITION_INDEX]
            assert unique == 1 and partial == 1, "一筆 open 的索引必須是 UNIQUE 且是部分索引"
            cols = [r[2] for r in c.execute("PRAGMA index_info(%s)" % store.OPEN_POSITION_INDEX)]
            assert cols == ["user_id", "strategy", "symbol"], cols
        sql = [row[3] for row in first if row[1] == store.OPEN_POSITION_INDEX][0]
        assert "WHERE status = 'open'" in sql, sql


def test_ac1_pragmas_wal_full_and_foreign_keys():
    with _tempdir() as tmp, _opened(tmp) as s:
        conn = s._conn
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2, "synchronous 不是 FULL"
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.isolation_level is None, "交易必須由本模組明確控制，不可交給標準庫隱式開"


def test_ac1_roundtrip_values_and_types():
    clock = FakeClock()
    with _tempdir() as tmp:
        with _opened(tmp, clock=clock) as s:
            # s4：訊號價故意傳 int，時刻全部省略（走假時鐘）
            pos4 = s.record_entry(**_entry("s4-1", price=3))
            created4 = clock.now
            # s5：時刻全部明確給；特徵含中文與 numpy 純量
            feats5 = {"漲幅": np.float64(0.31), "量倍數": np.int64(12), "爆量分支": "①",
                      "放量": np.bool_(True), "cpos": np.float32(0.5)}
            pos5 = s.record_entry(**_entry("s5-1", strategy="s5", price=0.0123, features=feats5,
                                           created_ms=T0 + 7, opened_ms=T0 + 9))
            path = s.path

            sig4, sig5 = s.get_signal("s4-1"), s.get_signal("s5-1")
            assert pos4 == s.get_position("s4-1") and pos5 == s.get_position("s5-1")

        e4, e5 = _entry("s4-1", price=3), _entry("s5-1", strategy="s5", price=0.0123)
        for sig, pos, e in ((sig4, pos4, e4), (sig5, pos5, e5)):
            for k in ("signal_id", "user_id", "strategy", "symbol", "side", "bar_open_ms"):
                assert sig[k] == e[k], (k, sig[k], e[k])
            for k in ("signal_price", "take_profit_price", "stop_loss_price"):
                assert sig[k] == float(e[k]) and type(sig[k]) is float, (k, sig[k])
            assert pos["entry_price"] == sig["signal_price"] and type(pos["entry_price"]) is float
            for k in ("take_profit_price", "stop_loss_price"):
                assert pos[k] == sig[k] and type(pos[k]) is float
            for k in ("bar_open_ms", "created_ms"):
                assert type(sig[k]) is int, (k, sig[k])
            assert type(pos["opened_ms"]) is int
            assert sig["published_ms"] is None
            assert (pos["status"], pos["exit_reason"], pos["exit_price"], pos["closed_ms"]) == \
                ("open", None, None, None)
            assert {pos[k] for k in ("user_id", "strategy", "symbol", "signal_id")} == \
                {sig[k] for k in ("user_id", "strategy", "symbol", "signal_id")}

        assert sig4["created_ms"] == created4 and pos4["opened_ms"] == created4, "省略時用注入的時鐘"
        assert sig5["created_ms"] == T0 + 7 and pos5["opened_ms"] == T0 + 9, "明確給的時刻原樣存"
        assert sig4["features"] == FEATURES["s4"]
        assert sig5["features"] == {"漲幅": 0.31, "量倍數": 12, "爆量分支": "①", "放量": True,
                                    "cpos": 0.5}, sig5["features"]

        # 資料庫裡實際的儲存型別（不是 Python 轉回來的樣子）
        dump = _dump(path)
        sig_row = dict(zip([r for r in _column_pairs(path, "signals")], dump["signals"][0]))
        pos_row = dict(zip([r for r in _column_pairs(path, "positions")], dump["positions"][0]))
        for k in ("signal_price", "take_profit_price", "stop_loss_price"):
            assert sig_row["typeof(%s)" % k] == "real", (k, sig_row)
        for k in ("bar_open_ms", "created_ms"):
            assert sig_row["typeof(%s)" % k] == "integer", (k, sig_row)
        assert sig_row["typeof(published_ms)"] == "null"
        assert sig_row["typeof(features_json)"] == "text"
        assert json.loads(sig_row["features_json"]) == FEATURES["s4"]
        for k in ("entry_price", "take_profit_price", "stop_loss_price"):
            assert pos_row["typeof(%s)" % k] == "real", (k, pos_row)
        assert pos_row["typeof(opened_ms)"] == "integer"


def _column_pairs(path, table):
    with _raw(path) as c:
        cols = [r[1] for r in c.execute("PRAGMA table_info(%s)" % table)]
    out = []
    for col in cols:
        out += [col, "typeof(%s)" % col]
    return out


def test_ac1_bad_values_are_rejected_before_touching_the_db():
    tp, sl = _levels(2.5)
    cases = [
        (TypeError, dict(signal_price="2.5")),              # 字串就算像數字也不收
        (TypeError, dict(signal_price=True)),
        (ValueError, dict(signal_price=float("nan"))),
        (ValueError, dict(signal_price=float("inf"))),
        (ValueError, dict(signal_price=0.0)),
        (ValueError, dict(take_profit_price=sl, stop_loss_price=tp)),     # 止盈止損傳反
        (TypeError, dict(bar_open_ms=float(T0))),           # 毫秒必須是整數型別
        (ValueError, dict(bar_open_ms=T0 // 1000)),         # 把秒當毫秒
        (ValueError, dict(bar_open_ms=T0 * 1000)),          # 把微秒當毫秒
        (TypeError, dict(created_ms=1.5)),
        (ValueError, dict(strategy="S4")),
        (ValueError, dict(strategy="s6")),
        (ValueError, dict(side="long")),
        (ValueError, dict(symbol=" AAA_USDT")),
        (ValueError, dict(signal_id="")),
        (TypeError, dict(user_id=None)),
        (ValueError, dict(user_id="")),
        # features：±inf / NaN 現在要收（見 test_s2a），但非純量、自訂物件、非字串的鍵仍然拒收
        (TypeError, dict(features=[("ret2h", 0.2)])),
        (TypeError, dict(features={"obj": object()})),
        (TypeError, dict(features={"nested": {"a": 1}})),
        (TypeError, dict(features={"seq": [1, 2]})),
        (TypeError, dict(features={"pair": (1, 2)})),
        (TypeError, dict(features={"arr": np.array([1.0, 2.0])})),
        (TypeError, dict(features={"dec": Decimal("1.5")})),
        (TypeError, dict(features={1: 0.5})),              # 存成 JSON 會被悄悄改成 "1"
        (ValueError, dict(features={"": 0.5})),
    ]
    with _tempdir() as tmp, _opened(tmp) as s:
        before = _dump(s.path)
        for exc, over in cases:
            kw = _entry("bad")
            kw.update(over)
            try:
                _expect(exc, lambda kw=kw: s.record_entry(**kw))
            except AssertionError as e:
                raise AssertionError("%s：%s" % (over, e)) from None
        assert _dump(s.path) == before, "驗證失敗卻動到了資料庫"
        # 驗證不影響之後的正常寫入
        s.record_entry(**_entry("good"))
        assert s.get_position("good")["status"] == "open"


def test_ac1_database_itself_refuses_wrong_types():
    """繞過本模組直接下 SQL：表層 CHECK 仍然擋得住錯型別與半套狀態。"""
    with _tempdir() as tmp:
        with _opened(tmp) as s:
            s.record_entry(**_entry("s4-1"))
            path = s.path
        good = ("x", "strategy", "s4", "B_USDT", "short", T0, 2.5, 2.4, 2.6, "{}", T0, None)
        sig_sql = "INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        with _raw(path) as c:
            for i, bad in ((6, "abc"), (5, 1.5), (5, "soon"), (1, None), (11, 2.5)):
                row = list(good)
                row[i] = bad
                _expect(sqlite3.IntegrityError, lambda row=row: c.execute(sig_sql, row))
            pos_sql = "INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            c.execute(sig_sql, good)
            base = ["x", "strategy", "s4", "B_USDT", 2.5, 2.4, 2.6, T0, "open", None, None, None]
            for i, bad in ((8, "pending"), (9, "take_profit"), (4, "abc")):
                row = list(base)
                row[i] = bad
                _expect(sqlite3.IntegrityError, lambda row=row: c.execute(pos_sql, row))
            half_closed = base[:8] + ["closed", "take_profit", None, None]
            _expect(sqlite3.IntegrityError, lambda: c.execute(pos_sql, half_closed))
            orphan = ["nosuch"] + base[1:]                  # 外鍵：部位一定要對應到一筆訊號
            _expect(sqlite3.IntegrityError, lambda: c.execute(pos_sql, orphan), "FOREIGN KEY")


# ============================== AC-2：兩個策略同一個幣 ==============================
def test_ac2_two_strategies_same_symbol():
    with _tempdir() as tmp, _opened(tmp) as s:
        s.record_entry(**_entry("s4-a", strategy="s4", symbol="AAA_USDT"))
        s.record_entry(**_entry("s5-a", strategy="s5", symbol="AAA_USDT"))
        assert {p["signal_id"] for p in s.open_positions()} == {"s4-a", "s5-a"}

        before = _dump(s.path)
        e = _expect(store.OpenPositionExistsError,
                    lambda: s.record_entry(**_entry("s4-b", strategy="s4", symbol="AAA_USDT")),
                    repr(config.STRATEGY_USER_ID), "'s4'", "'AAA_USDT'", "'s4-a'", "'s4-b'")
        assert (e.user_id, e.strategy, e.symbol, e.existing_signal_id, e.new_signal_id) == \
            (config.STRATEGY_USER_ID, "s4", "AAA_USDT", "s4-a", "s4-b")
        assert isinstance(e.__cause__, sqlite3.IntegrityError), "要保留資料庫原始錯誤當 cause"
        assert _dump(s.path) == before, "被擋下的那一筆留下了痕跡"

        # 不同 user_id 不互相干擾（第二階段訂閱者）
        s.record_entry(**_entry("sub-a", strategy="s4", symbol="AAA_USDT", user_id=SUBSCRIBER))

        # 平倉第一筆後，同鍵可以再開；同鍵的 closed 可以累積任意多筆
        s.close_position("s4-a", exit_reason="stop_loss", exit_price=2.7)
        s.record_entry(**_entry("s4-b", strategy="s4", symbol="AAA_USDT"))
        s.close_position("s4-b", exit_reason="take_profit", exit_price=2.3)
        s.record_entry(**_entry("s4-c", strategy="s4", symbol="AAA_USDT"))
        assert {p["signal_id"] for p in s.open_positions()} == {"s5-a", "sub-a", "s4-c"}
        with _raw(s.path) as c:
            n = c.execute("SELECT status, count(*) FROM positions WHERE user_id = ? AND "
                          "strategy = 's4' AND symbol = 'AAA_USDT' GROUP BY status ORDER BY status",
                          (config.STRATEGY_USER_ID,)).fetchall()
        assert n == [("closed", 2), ("open", 1)], n


def test_ac2_partial_unique_index_is_enforced_by_the_database():
    """繞過本模組：擋下第二筆 open 的是資料庫的部分唯一索引本身，不是 Python 的前置檢查。"""
    with _tempdir() as tmp:
        with _opened(tmp) as s:
            s.record_entry(**_entry("s4-a"))
            path = s.path
        with _raw(path) as c:
            c.execute("BEGIN")
            c.execute("INSERT INTO signals SELECT 'raw-2', user_id, strategy, symbol, side, "
                      "bar_open_ms, signal_price, take_profit_price, stop_loss_price, "
                      "features_json, created_ms, NULL FROM signals WHERE signal_id = 's4-a'")
            _expect(sqlite3.IntegrityError, lambda: c.execute(
                "INSERT INTO positions SELECT 'raw-2', user_id, strategy, symbol, entry_price, "
                "take_profit_price, stop_loss_price, opened_ms, 'open', NULL, NULL, NULL "
                "FROM positions WHERE signal_id = 's4-a'"),
                "UNIQUE constraint failed: positions.user_id, positions.strategy, positions.symbol")
            # 同一個鍵寫成 closed 就不受限制
            c.execute("INSERT INTO positions SELECT 'raw-2', user_id, strategy, symbol, entry_price, "
                      "take_profit_price, stop_loss_price, opened_ms, 'closed', 'stop_loss', 2.7, "
                      "opened_ms + 1 FROM positions WHERE signal_id = 's4-a'")
            c.execute("ROLLBACK")


# ============================== AC-3：原子性 ==============================
def test_ac3_unique_violation_rolls_back_the_signal_too():
    with _tempdir() as tmp:
        with _opened(tmp) as s:
            s.record_entry(**_entry("s4-a"))
            before = _dump(s.path)
            trace = []
            s._conn.set_trace_callback(trace.append)
            try:
                _expect(store.OpenPositionExistsError, lambda: s.record_entry(**_entry("s4-dup")))
            finally:
                s._conn.set_trace_callback(None)
            # 證明走的是「訊號寫進去了、開倉失敗」這條路，不是在寫之前就被擋掉
            first = [t.split("(")[0].split(" VALUES")[0].strip() for t in trace]
            i_sig = next(i for i, t in enumerate(first) if t.startswith("INSERT INTO signals"))
            i_pos = next(i for i, t in enumerate(first) if t.startswith("INSERT INTO positions"))
            i_rb = next(i for i, t in enumerate(first) if t == "ROLLBACK")
            assert first[0] == "BEGIN IMMEDIATE" and i_sig < i_pos < i_rb, trace
            assert "COMMIT" not in first, trace
            assert s.get_signal("s4-dup") is None, "訊號表留下了孤兒"
            assert _dump(s.path) == before
            assert not s._conn.in_transaction, "失敗後交易必須已經結束"
            s.record_entry(**_entry("s4-b", symbol="BBB_USDT"))       # store 仍然可用
        with _opened(tmp) as s2:                                       # 重開也一樣
            assert s2.get_signal("s4-dup") is None
            assert s2.get_signal("s4-b") is not None


def test_ac3_any_failure_while_opening_rolls_back():
    """開倉當下拋任何例外（含非 sqlite 的、含 KeyboardInterrupt）都整筆撤銷，例外原樣往外拋。"""
    real = store._insert_position
    with _tempdir() as tmp, _opened(tmp) as s:
        s.record_entry(**_entry("s4-a"))
        before = _dump(s.path)
        for exc in (RuntimeError("模擬開倉當下出錯"), KeyboardInterrupt()):
            def boom(conn, row, exc=exc):
                n = conn.execute("SELECT count(*) FROM signals WHERE signal_id = ?",
                                 (row["signal_id"],)).fetchone()[0]
                assert n == 1, "前提：訊號在交易裡已經寫進去了"
                raise exc
            store._insert_position = boom
            try:
                try:
                    s.record_entry(**_entry("s4-x", symbol="XXX_USDT"))
                except BaseException as e:      # noqa: BLE001 — 要連 KeyboardInterrupt 一起驗
                    assert e is exc, "例外被換掉了：%r" % (e,)
                else:
                    raise AssertionError("例外被吞掉了")
            finally:
                store._insert_position = real
            assert s.get_signal("s4-x") is None, "訊號表留下了孤兒（%s）" % type(exc).__name__
            assert _dump(s.path) == before
            assert not s._conn.in_transaction
        s.record_entry(**_entry("s4-x", symbol="XXX_USDT"))
        assert s.get_position("s4-x")["status"] == "open"


def test_ac3_control_without_transaction_would_leave_an_orphan():
    """反向對照：同樣兩句 SQL 不包交易，一樣違反唯一索引，訊號表就會留下孤兒。

    證明上面兩條的「訊號表沒留下」有鑑別力 —— 不是因為這個情境本來就寫不進去。
    """
    with _tempdir() as tmp:
        with _opened(tmp) as s:
            s.record_entry(**_entry("s4-a"))
            path = s.path
        row = _entry("s4-orphan")
        row.update(features_json="{}", created_ms=T0, opened_ms=T0)
        with _raw(path) as c:                   # autocommit：每一句自己提交
            store._insert_signal(c, row)
            _expect(sqlite3.IntegrityError, lambda: store._insert_position(c, row))
            n = c.execute("SELECT count(*) FROM signals WHERE signal_id = 's4-orphan'").fetchone()[0]
        assert n == 1, "對照組前提不成立：不包交易也沒留下孤兒，AC-3 的斷言就沒有鑑別力"


def test_ac3_duplicate_signal_id_is_rejected_and_nothing_changes():
    with _tempdir() as tmp, _opened(tmp) as s:
        s.record_entry(**_entry("sig-1"))
        before = _dump(s.path)
        e = _expect(store.DuplicateSignalError,
                    lambda: s.record_entry(**_entry("sig-1", strategy="s5", symbol="ZZZ_USDT")),
                    "'sig-1'")
        assert e.signal_id == "sig-1"
        assert _dump(s.path) == before


def test_ac3_process_killed_mid_transaction_leaves_nothing():
    """子行程在「訊號已寫入、開倉之前」被 os._exit 砍掉：重開之後那一筆不存在，之前提交的都在。"""
    with _tempdir() as tmp:
        path = os.path.join(tmp, "live.sqlite3")
        rc, out, err = _run_child("""
            from live import store
            s = store.open_store(DB, clock=lambda: T0)
            s.record_entry(**FIRST)
            def die(conn, row):
                n = conn.execute("SELECT count(*) FROM signals WHERE signal_id = ?",
                                 (row["signal_id"],)).fetchone()[0]
                print("SEEN-IN-TXN %d" % n)
                sys.stdout.flush()
                os._exit(3)
            store._insert_position = die
            s.record_entry(**SECOND)
            print("NOT-REACHED")
        """, DB=path, T0=T0, FIRST=_entry("s4-first"), SECOND=_entry("s4-second", symbol="BBB_USDT"))
        assert rc == 3 and "SEEN-IN-TXN 1" in out and "NOT-REACHED" not in out, (rc, out, err)
        with _opened(tmp) as s:
            assert s.get_signal("s4-first") is not None and s.get_position("s4-first") is not None
            assert s.get_signal("s4-second") is None, "交易中途當掉，訊號卻留下來了"
            assert [p["signal_id"] for p in s.open_positions()] == ["s4-first"]


# ============================== AC-4：重啟復原 ==============================
def _populate(s):
    """6 筆進場（s4 / s5 / 訂閱者混合），平倉 2 筆、發布 3 筆。回傳預期的未平倉與未發布集合。"""
    rows = [
        ("s4-1", "s4", "AAA_USDT", config.STRATEGY_USER_ID),
        ("s5-1", "s5", "AAA_USDT", config.STRATEGY_USER_ID),
        ("s4-2", "s4", "BBB_USDT", config.STRATEGY_USER_ID),
        ("s5-2", "s5", "CCC_USDT", config.STRATEGY_USER_ID),
        ("s4-3", "s4", "DDD_USDT", config.STRATEGY_USER_ID),
        ("sub-1", "s4", "AAA_USDT", SUBSCRIBER),
    ]
    for i, (sid, strat, sym, uid) in enumerate(rows):
        s.record_entry(**_entry(sid, strategy=strat, symbol=sym, user_id=uid, price=1.0 + i,
                                created_ms=T0 + i * BAR_MS))
    s.close_position("s4-1", exit_reason="take_profit", exit_price=0.95, closed_ms=T0 + 10 * BAR_MS)
    s.close_position("s5-2", exit_reason="stop_loss", exit_price=4.3, closed_ms=T0 + 11 * BAR_MS)
    for sid in ("s4-1", "s5-1", "s4-3"):
        s.mark_published(sid, published_ms=T0 + 12 * BAR_MS)
    return {"s5-1", "s4-2", "s4-3", "sub-1"}, {"s4-2", "s5-2", "sub-1"}


def test_ac4_close_and_reopen_recovers_open_positions():
    with _tempdir() as tmp:
        with _opened(tmp) as s:
            want_open, want_unpub = _populate(s)
            snap_open = s.open_positions()
            snap_unpub = s.unpublished_signals()
        with _opened(tmp) as s:                            # 全新的連線
            got = s.open_positions()
            assert {p["signal_id"] for p in got} == want_open, got
            assert got == snap_open, "重開後內容與關閉前不一致"
            assert [p["opened_ms"] for p in got] == sorted(p["opened_ms"] for p in got)
            assert {p["signal_id"] for p in s.open_positions(strategy="s4")} == {"s4-2", "s4-3", "sub-1"}
            assert {p["signal_id"] for p in s.open_positions(strategy="s5")} == {"s5-1"}
            mine = s.open_positions(user_id=config.STRATEGY_USER_ID)
            assert {p["signal_id"] for p in mine} == want_open - {"sub-1"}
            assert {p["signal_id"] for p in s.open_positions(strategy="s4",
                                                              user_id=config.STRATEGY_USER_ID)} \
                == {"s4-2", "s4-3"}

            unpub = s.unpublished_signals()
            assert {g["signal_id"] for g in unpub} == want_unpub, unpub
            assert unpub == snap_unpub
            assert {g["signal_id"] for g in s.unpublished_signals(strategy="s5")} == {"s5-2"}
            assert {g["signal_id"] for g in s.unpublished_signals(user_id=SUBSCRIBER)} == {"sub-1"}
            for g in unpub:
                assert g["features"] == FEATURES[g["strategy"]]

            _expect(ValueError, lambda: s.open_positions(strategy="S4"))    # 過濾值打錯要叫出來
            _expect(TypeError, lambda: s.unpublished_signals(user_id=123))


def test_ac4_committed_writes_survive_a_crash():
    """子行程寫完不關連線、直接 os._exit（模擬當機）：已提交的內容重開之後一筆不少。

    同時確認子行程死後旁邊留著 -wal 檔 —— 資料是 SQLite 從 WAL 復原回來的，不是 close() 幫忙寫的。
    """
    with _tempdir() as tmp:
        path = os.path.join(tmp, "live.sqlite3")
        rc, out, err = _run_child("""
            from live import store
            s = store.open_store(DB, clock=lambda: T0)
            for kw in ENTRIES:
                s.record_entry(**kw)
            s.close_position("s4-1", exit_reason="stop_loss", exit_price=2.7, closed_ms=T0 + 5)
            s.mark_published("s5-1", published_ms=T0 + 6)
            print("WROTE")
            sys.stdout.flush()
            os._exit(0)
        """, DB=path, T0=T0, ENTRIES=[_entry("s4-1"), _entry("s5-1", strategy="s5"),
                                      _entry("s4-2", symbol="BBB_USDT")])
        assert rc == 0 and "WROTE" in out, (rc, out, err)
        assert os.path.exists(path + "-wal"), "前提：子行程沒有正常關閉，WAL 檔應該還在"
        with _opened(tmp) as s:
            assert {p["signal_id"] for p in s.open_positions()} == {"s5-1", "s4-2"}
            closed = s.get_position("s4-1")
            assert (closed["status"], closed["exit_reason"], closed["exit_price"],
                    closed["closed_ms"]) == ("closed", "stop_loss", 2.7, T0 + 5)
            assert {g["signal_id"] for g in s.unpublished_signals()} == {"s4-1", "s4-2"}
            assert s.get_signal("s5-1")["published_ms"] == T0 + 6


# ============================== AC-5：平倉 / 發布的錯誤路徑 ==============================
def test_ac5_close_position_error_paths_and_scope():
    clock = FakeClock()
    with _tempdir() as tmp, _opened(tmp, clock=clock) as s:
        for sid, sym in (("s4-a", "AAA_USDT"), ("s4-b", "BBB_USDT"), ("s4-c", "CCC_USDT")):
            s.record_entry(**_entry(sid, symbol=sym))
        s.record_entry(**_entry("s5-a", strategy="s5", symbol="AAA_USDT"))

        before = _dump(s.path)
        e = _expect(store.PositionNotOpenError,
                    lambda: s.close_position("nope", exit_reason="take_profit", exit_price=2.4),
                    "'nope'", "不存在")
        assert (e.signal_id, e.reason) == ("nope", "missing")
        _expect(ValueError, lambda: s.close_position("s4-a", exit_reason="timeout", exit_price=2.4))
        _expect(TypeError, lambda: s.close_position("s4-a", exit_reason="take_profit",
                                                    exit_price="2.4"))
        assert _dump(s.path) == before, "失敗的平倉動到了資料庫"

        # 成功的平倉：只改目標那一筆的四個欄位，其他列一格都不動；省略 closed_ms 用注入的時鐘
        closed = s.close_position("s4-b", exit_reason="take_profit", exit_price=2.4)
        assert closed == s.get_position("s4-b")
        assert (closed["status"], closed["exit_reason"], closed["exit_price"], closed["closed_ms"]) \
            == ("closed", "take_profit", 2.4, clock.now)
        after = _dump(s.path)
        assert after["signals"] == before["signals"], "平倉不該動到訊號表"
        cols = _column_pairs(s.path, "positions")
        changed = {}
        for old, new in zip(before["positions"], after["positions"]):
            diff = {cols[i] for i in range(len(cols)) if old[i] != new[i]}
            if diff:
                changed[old[0]] = diff
        assert set(changed) == {"s4-b"}, changed
        assert {c for c in changed["s4-b"] if not c.startswith("typeof(")} == \
            {"status", "exit_reason", "exit_price", "closed_ms"}, changed

        # 對已平倉的再平倉：拋例外，第一次平倉的紀錄原封不動
        e = _expect(store.PositionNotOpenError,
                    lambda: s.close_position("s4-b", exit_reason="stop_loss", exit_price=9.9,
                                             closed_ms=T0 + 99),
                    "'s4-b'", "早已平倉")
        assert (e.signal_id, e.reason) == ("s4-b", "closed")
        assert _dump(s.path) == after


def test_ac5_mark_published_error_paths():
    with _tempdir() as tmp, _opened(tmp) as s:
        s.record_entry(**_entry("s4-a"))
        s.record_entry(**_entry("s4-b", symbol="BBB_USDT"))
        e = _expect(store.SignalNotPendingError, lambda: s.mark_published("nope"), "'nope'")
        assert e.reason == "missing"
        s.mark_published("s4-a", published_ms=T0 + 1)
        assert s.get_signal("s4-a")["published_ms"] == T0 + 1
        assert s.get_signal("s4-b")["published_ms"] is None, "只能標記目標那一筆"
        before = _dump(s.path)
        e = _expect(store.SignalNotPendingError,
                    lambda: s.mark_published("s4-a", published_ms=T0 + 2), "'s4-a'")
        assert e.reason == "published"
        assert _dump(s.path) == before, "重複標記不可以改掉原本的發布時刻"
        assert [g["signal_id"] for g in s.unpublished_signals()] == ["s4-b"]


def test_store_errors_share_a_base_class_and_closed_store_is_loud():
    for cls in (store.SchemaVersionError, store.DuplicateSignalError,
                store.OpenPositionExistsError, store.PositionNotOpenError,
                store.SignalNotPendingError):
        assert issubclass(cls, store.StoreError), cls
    with _tempdir() as tmp:
        with _opened(tmp) as s:
            pass
        s.close()                                           # 重複關閉沒關係
        _expect(store.StoreError, lambda: s.record_entry(**_entry("late")), "已經關閉")
        _expect(store.StoreError, lambda: s.open_positions(), "已經關閉")
        _expect(store.StoreError, lambda: s.get_signal("x"), "已經關閉")
        _expect(store.StoreError, lambda: s.close_position("x", exit_reason="stop_loss",
                                                           exit_price=1.0), "已經關閉")


def test_clock_values_are_validated():
    with _tempdir() as tmp, _opened(tmp, clock=lambda: T0 // 1000) as s:   # 時鐘回傳的是秒
        before = _dump(s.path)
        _expect(ValueError, lambda: s.record_entry(**_entry("s4-a")), "clock()")
        assert _dump(s.path) == before


def test_open_store_refuses_newer_or_foreign_databases():
    with _tempdir() as tmp:
        newer = os.path.join(tmp, "newer.sqlite3")
        with _raw(newer) as c:
            c.execute("PRAGMA user_version = %d" % (store.SCHEMA_VERSION + 1))
        _expect(store.SchemaVersionError, lambda: store.open_store(newer), "比本程式認得的")
        assert _schema_objects(newer) == [], "拒絕開啟卻建了表"

        foreign = os.path.join(tmp, "foreign.sqlite3")
        with _raw(foreign) as c:
            c.execute("CREATE TABLE something_else (x)")
        _expect(store.SchemaVersionError, lambda: store.open_store(foreign), "something_else")
        assert [r[1] for r in _schema_objects(foreign)] == ["something_else"]
        with _raw(foreign) as c:
            assert c.execute("PRAGMA user_version").fetchone()[0] == 0
            # 拒絕開啟的檔案連 journal mode 都不可以被改成 WAL
            assert c.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        with _raw(newer) as c:
            assert c.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    # 走到這裡代表 _tempdir 刪得掉：拒絕開啟時連線有確實關閉


# ============================== AC-6：user_id 保留值 ==============================
def test_ac6_strategy_records_use_the_reserved_constant_not_null():
    uid = config.STRATEGY_USER_ID
    assert isinstance(uid, str) and uid and uid == uid.strip(), repr(uid)
    with _tempdir() as tmp:
        with _opened(tmp) as s:
            s.record_entry(**_entry("s4-a"))
            s.record_entry(**_entry("s5-a", strategy="s5"))
            before = _dump(s.path)
            _expect(TypeError, lambda: s.record_entry(**_entry("s4-null", symbol="N_USDT",
                                                               user_id=None)),
                    "STRATEGY_USER_ID", "NULL")
            assert _dump(s.path) == before
            path = s.path
        with _raw(path) as c:
            for table in store.TABLES:
                rows = c.execute("SELECT user_id, typeof(user_id) FROM %s" % table).fetchall()
                assert rows and all(r == (uid, "text") for r in rows), (table, rows)
            # 欄位本身是 NOT NULL：繞過本模組也寫不進 NULL
            _expect(sqlite3.IntegrityError, lambda: c.execute(
                "INSERT INTO signals VALUES ('raw', NULL, 's4', 'N_USDT', 'short', ?, 2.5, 2.4, "
                "2.6, '{}', ?, NULL)", (T0, T0)), "NOT NULL")


def test_ac6_control_null_user_id_would_defeat_the_unique_index():
    """對照組：一模一樣的部分唯一索引（直接取 store 的 DDL），user_id 用 NULL 時兩筆 open 都寫得進去。

    這就是 user_id 不可以用 NULL 代表策略層的原因：SQLite（與 SQL 標準）認為 NULL 彼此不相等，
    唯一索引不會把兩個 NULL 當成重複，「同 (user_id, strategy, symbol) 只能一筆 open」就失效了。
    """
    index_ddl = [d for d in store._DDL if store.OPEN_POSITION_INDEX in d]
    assert len(index_ddl) == 1
    with closing(sqlite3.connect(":memory:", isolation_level=None)) as c:
        c.execute("CREATE TABLE positions (user_id TEXT, strategy TEXT, symbol TEXT, status TEXT)")
        c.execute(index_ddl[0])
        ins = "INSERT INTO positions VALUES (?, 's4', 'AAA_USDT', 'open')"
        c.execute(ins, (None,))
        c.execute(ins, (None,))                          # NULL：第二筆 open 照樣寫得進去
        n = c.execute("SELECT count(*) FROM positions WHERE user_id IS NULL AND status = 'open'")
        assert n.fetchone()[0] == 2, "對照組前提不成立"
        c.execute(ins, (config.STRATEGY_USER_ID,))
        _expect(sqlite3.IntegrityError, lambda: c.execute(ins, (config.STRATEGY_USER_ID,)),
                "UNIQUE")                                  # 保留值：第二筆被擋下


# ============================== captain 補充 S-1：策略名單只有一份 ==============================
def test_s1_strategy_list_is_read_from_config_at_call_time():
    """改 config.STRATEGIES，store 不用 reload 就跟著變：加一個代號就收、拿掉一個就拒。

    證明 store 沒有自己留一份名單，也沒有在 import 時把名單綁死（跟 live.signal_events 一致）。
    """
    saved = config.STRATEGIES
    extra = "s9"
    assert extra not in saved, "前提：測試用的代號不可以已經在名單裡"
    dropped = saved[-1]
    with _tempdir() as tmp, _opened(tmp) as s:
        _expect(ValueError, lambda: s.record_entry(**_entry("x-1", strategy=extra)), "strategy")
        try:
            config.STRATEGIES = saved + (extra,)
            s.record_entry(**_entry("x-1", strategy=extra))
            assert [p["signal_id"] for p in s.open_positions(strategy=extra)] == ["x-1"]
            assert [g["signal_id"] for g in s.unpublished_signals(strategy=extra)] == ["x-1"]

            config.STRATEGIES = tuple(c for c in saved if c != dropped)
            before = _dump(s.path)
            _expect(ValueError, lambda: s.record_entry(**_entry("d-1", strategy=dropped,
                                                                symbol="D_USDT")), "strategy")
            _expect(ValueError, lambda: s.open_positions(strategy=dropped), "strategy")
            assert _dump(s.path) == before
        finally:
            config.STRATEGIES = saved
        s.record_entry(**_entry("d-1", strategy=dropped, symbol="D_USDT"))   # 還原後照常可寫


# ============================== captain 補充 S-2：features 收下 ±inf / NaN ==============================
S2A_FEATURES = {"volr": math.inf, "x": -math.inf, "y": math.nan, "z": 1.5}


def _check_s2a(got, where):
    assert set(got) == set(S2A_FEATURES), (where, got)
    assert got["volr"] == math.inf, (where, got)
    assert got["x"] == -math.inf, (where, got)
    assert math.isnan(got["y"]), (where, got)
    assert got["z"] == 1.5, (where, got)
    for k in S2A_FEATURES:
        assert type(got[k]) is float, (where, k, type(got[k]))


def _reject_constant(name):
    raise ValueError("嚴格 JSON 不認得 %s" % name)


def test_s2a_features_keep_inf_and_nan_and_read_back():
    """AC-S2a：{"volr": inf, "x": -inf, "y": nan, "z": 1.5} 存得進去，讀回來值與型別都一樣（重開之後也是）。"""
    with _tempdir() as tmp:
        with _opened(tmp) as s:
            s.record_entry(**_entry("s4-inf", features=dict(S2A_FEATURES)))
            _check_s2a(s.get_signal("s4-inf")["features"], "get_signal")
            pending = [g for g in s.unpublished_signals() if g["signal_id"] == "s4-inf"]
            _check_s2a(pending[0]["features"], "unpublished_signals")
            # numpy 的 inf / NaN 走同一條路（A3 的特徵多半直接從 pandas 取出來）
            s.record_entry(**_entry("s5-np", strategy="s5", features={
                "volr": np.float64(np.inf), "x": np.float32(-np.inf), "y": np.float32(np.nan),
                "z": np.float64(1.5)}))
            _check_s2a(s.get_signal("s5-np")["features"], "numpy 純量")
            path = s.path
        with _opened(tmp) as s:
            _check_s2a(s.get_signal("s4-inf")["features"], "重開之後")
            _check_s2a(s.get_signal("s5-np")["features"], "重開之後（numpy）")

        # 落地的格式就是 docstring 寫的：Python 風格 JSON，含三個非標準記號；嚴格解析器會失敗
        with _raw(path) as c:
            text = c.execute("SELECT features_json FROM signals WHERE signal_id = 's4-inf'").fetchone()[0]
        for token in ("Infinity", "-Infinity", "NaN"):
            assert token in text, text
        _expect(ValueError, lambda: json.loads(text, parse_constant=_reject_constant), "嚴格 JSON")


class _PlainMapping(Mapping):
    """不是 dict 子類的 Mapping。"""

    def __init__(self, data):
        self._data = dict(data)

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)


def test_s2b_entry_event_features_mappingproxy_is_accepted():
    """AC-S2b：直接把 A2 真正建出來的 EntryEvent(...).features（mappingproxy）交給 record_entry。

    事件裡的 NaN 會被 A2 轉成 None、inf 會保留，兩種都要原樣存、原樣讀回。
    """
    tp, sl = _levels(2.5)
    ev = EntryEvent(strategy="s4", signal_id="s4-ev", symbol="EVT_USDT", direction=DIRECTION_SHORT,
                    created_ms=T0, bar_open_ms=T0 - BAR_MS, signal_price=2.5,
                    take_profit_price=tp, stop_loss_price=sl,
                    features={"ret2h": 0.1834, "volr": math.inf, "cpos": math.nan,
                              "turn": np.float64(3.4e6), "hot": np.bool_(True)})
    # 前提：這真的是 A2 的唯讀映射，而且 A2 已經把 NaN 轉成 None、inf 照樣保留
    assert isinstance(ev.features, types.MappingProxyType), type(ev.features)
    assert ev.features["cpos"] is None and ev.features["volr"] == math.inf, dict(ev.features)
    want = dict(ev.features)

    with _tempdir() as tmp:
        with _opened(tmp) as s:
            pos = s.record_entry(signal_id=ev.signal_id, user_id=config.STRATEGY_USER_ID,
                                 strategy=ev.strategy, symbol=ev.symbol, side=ev.direction,
                                 bar_open_ms=ev.bar_open_ms, signal_price=ev.signal_price,
                                 take_profit_price=ev.take_profit_price,
                                 stop_loss_price=ev.stop_loss_price, features=ev.features,
                                 created_ms=ev.created_ms)
            assert pos["status"] == "open" and pos["entry_price"] == ev.signal_price
            got = s.get_signal(ev.signal_id)["features"]
            assert got == want and type(got) is dict, got
            # 其他 Mapping 也收：OrderedDict、不是 dict 子類的自訂 Mapping
            s.record_entry(**_entry("s4-od", symbol="OD_USDT",
                                    features=OrderedDict([("volr", math.inf), ("turn", 1.0)])))
            s.record_entry(**_entry("s4-pm", symbol="PM_USDT",
                                    features=_PlainMapping({"volr": math.inf, "cpos": None})))
            assert s.get_signal("s4-pm")["features"] == {"volr": math.inf, "cpos": None}
        with _opened(tmp) as s:
            got = s.get_signal(ev.signal_id)["features"]
            assert got == want, got
            assert got["cpos"] is None and got["volr"] == math.inf and got["hot"] is True


# ============================== M-1（BUG-006）：lone surrogate ==============================
# 用 chr() 組字，原始碼裡不出現跳脫序列（這台機器的編輯工具會把 4 位數的跳脫改寫成字元本身）
LONE_HIGH = chr(0xD800)         # 單獨的高位 surrogate
LONE_LOW = chr(0xDCFF)          # surrogateescape 把解不開的位元組變成 U+DC80..U+DCFF 這一段


def _features_json_text(path, signal_id):
    """資料庫裡那一格 features_json 的原始文字。"""
    with _raw(path) as c:
        return c.execute("SELECT features_json FROM signals WHERE signal_id = ?",
                         (signal_id,)).fetchone()[0]


def test_m1_entry_event_features_with_lone_surrogates_round_trip():
    """M-1：A2 的 EntryEvent 收下「鍵與值都含 lone surrogate」的 features，store 也要收，關閉重開後原樣讀回。

    修正前：sqlite3 綁定 ensure_ascii=False 產生的文字時要編成 UTF-8，lone surrogate 在那一步拋
    UnicodeEncodeError —— A2 收、store 拒，訊號卡住發不出去。
    """
    tp, sl = _levels(2.5)
    ev = EntryEvent(strategy="s4", signal_id="s4-sur", symbol="SUR_USDT", direction=DIRECTION_SHORT,
                    created_ms=T0, bar_open_ms=T0 - BAR_MS, signal_price=2.5,
                    take_profit_price=tp, stop_loss_price=sl,
                    features={"k" + LONE_HIGH: 1.5, "v": "x" + LONE_HIGH,
                              "low" + LONE_LOW: "y" + LONE_LOW, "爆量分支": "①",
                              "volr": math.inf, "cpos": math.nan})
    want = dict(ev.features)
    # 前提：這是 A2 真正的唯讀映射，而且鍵與值裡的 lone surrogate 都被原樣收下
    assert isinstance(ev.features, types.MappingProxyType), type(ev.features)
    assert want["v"] == "x" + LONE_HIGH and want["k" + LONE_HIGH] == 1.5, ascii(want)
    assert want["low" + LONE_LOW] == "y" + LONE_LOW and want["cpos"] is None, ascii(want)

    with _tempdir() as tmp:
        with _opened(tmp) as s:
            s.record_entry(signal_id=ev.signal_id, user_id=config.STRATEGY_USER_ID,
                           strategy=ev.strategy, symbol=ev.symbol, side=ev.direction,
                           bar_open_ms=ev.bar_open_ms, signal_price=ev.signal_price,
                           take_profit_price=ev.take_profit_price,
                           stop_loss_price=ev.stop_loss_price, features=ev.features,
                           created_ms=ev.created_ms)
            assert s.get_signal(ev.signal_id)["features"] == want
            path = s.path
        # 關閉之後用全新的 open_store 重開
        with store.open_store(path, clock=FakeClock()) as s:
            got = s.get_signal(ev.signal_id)["features"]
            assert got == want, ascii(got)
            assert [p["signal_id"] for p in s.open_positions()] == [ev.signal_id]
            assert [g["signal_id"] for g in s.unpublished_signals()] == [ev.signal_id]
        text = _features_json_text(path, ev.signal_id)
        assert text.isascii(), ascii(text)


def test_m1_store_level_lone_surrogates_and_non_ascii_round_trip():
    """M-1（store 層，不經 EntryEvent）：一般 dict 的鍵 / 值含 lone surrogate、中文、emoji 都存得進去、原樣讀回。

    落地文字一律是純 ASCII（中文存成 JSON 跳脫序列），同樣的內容只有一種落地文字。
    """
    feats = {"k" + LONE_HIGH: "v" + LONE_LOW, "漲幅": 0.31, "爆量分支": "①", "訊號": "🔴",
             "volr": math.inf}
    with _tempdir() as tmp:
        with _opened(tmp) as s:
            s.record_entry(**_entry("other", symbol="OTHER_USDT"))
            before_other = s.get_signal("other")
            s.record_entry(**_entry("s5-sur", strategy="s5", features=dict(feats)))
            assert s.get_signal("s5-sur")["features"] == feats
            path = s.path
        with store.open_store(path, clock=FakeClock()) as s:
            got = s.get_signal("s5-sur")["features"]
            assert got == feats, ascii(got)
            assert s.get_signal("other") == before_other
        text = _features_json_text(path, "s5-sur")
        assert text.isascii(), ascii(text)
        assert "漲幅" not in text, "非 ASCII 字元應該以跳脫序列落地"
        assert json.loads(text) == feats
        # 同樣的內容 -> 同樣的落地文字（不論字串裡有沒有 lone surrogate，都走同一條路）
        plain = {"漲幅": 0.31, "爆量分支": "①"}
        assert store._features_json(plain) == json.dumps(plain, sort_keys=True)


# ============================== AC-7：範圍與約定 ==============================
def test_ac7_live_db_path_is_under_runtime_and_gitignored():
    db = os.path.abspath(config.LIVE_DB_PATH)
    runtime = os.path.abspath(paths.RUNTIME_DIR)
    assert os.path.commonpath([db, runtime]) == runtime, db
    for forbidden in ("state", "output"):
        top = os.path.join(paths.REPO_ROOT, forbidden)
        assert os.path.commonpath([db, top]) != top, db
    assert config.execution_params()["LIVE_DB_PATH"] == config.LIVE_DB_PATH
    rel = os.path.relpath(config.LIVE_DB_PATH, paths.REPO_ROOT).replace(os.sep, "/")
    for path in (rel, rel + "-wal", rel + "-shm"):
        r = subprocess.run(["git", "check-ignore", "-q", "--", path], cwd=paths.REPO_ROOT,
                           capture_output=True, timeout=30)
        assert r.returncode == 0, "%s 沒有被 .gitignore 擋住（rc=%d）" % (path, r.returncode)


def test_ac7_default_path_is_read_from_config_at_call_time():
    runtime_existed = os.path.exists(paths.RUNTIME_DIR)
    saved = config.LIVE_DB_PATH
    with _tempdir() as tmp:
        target = os.path.join(tmp, "nested", "db", "live.sqlite3")
        config.LIVE_DB_PATH = target
        try:
            s = store.open_store(clock=FakeClock())
            try:
                assert s.path == os.path.abspath(target), s.path
                assert os.path.isfile(target), "上層目錄要由 open_store() 自己建"
            finally:
                s.close()
        finally:
            config.LIVE_DB_PATH = saved
    assert os.path.exists(paths.RUNTIME_DIR) == runtime_existed, "測試碰到了真正的 runtime/"


def test_ac7_smoke_check_lists_live_db_path_without_creating_it():
    runtime_existed = os.path.exists(paths.RUNTIME_DIR)
    db_dir_existed = os.path.exists(os.path.dirname(config.LIVE_DB_PATH))
    r = subprocess.run([sys.executable, "-m", "live"], cwd=REPO_ROOT, stdin=subprocess.DEVNULL,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    out = r.stdout.decode("utf-8", "replace")
    assert r.returncode == 0, (r.returncode, out[-2000:], r.stderr[-2000:])
    line = [ln for ln in out.splitlines() if ln.strip().startswith("LIVE_DB_PATH")]
    assert len(line) == 1 and config.LIVE_DB_PATH in line[0], line
    assert os.path.exists(paths.RUNTIME_DIR) == runtime_existed
    assert os.path.exists(os.path.dirname(config.LIVE_DB_PATH)) == db_dir_existed


def test_ac7_module_name_does_not_shadow_anything():
    assert os.path.isfile(os.path.join(REPO_ROOT, "live", "store.py"))
    assert store.__name__ == "live.store"
    assert "store" not in sys.stdlib_module_names
    # 這台機器上沒有任何頂層的 store 模組 / 套件可以被遮蔽（第三方同名套件也算）
    assert importlib.util.find_spec("store") is None, importlib.util.find_spec("store")


def test_ac7_store_imports_only_stdlib_and_live():
    src = open(os.path.join(REPO_ROOT, "live", "store.py"), encoding="utf-8").read()
    tops = set()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Import):
            tops |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            tops.add((node.module or "").split(".")[0])
    outside = {t for t in tops if t != "live" and t not in sys.stdlib_module_names}
    assert not outside, "live/store.py import 了標準庫以外的東西：%s" % sorted(outside)
    assert "sqlite3" in tops


def test_offline_cage_blocks_network_and_restores():
    """籠子攔得住三條出口；離開後還原成進入前的樣子。

    runner 會在外層再武裝一層，所以這裡比對的是「進入前」而不是原函式；
    外層沒有武裝時（單獨呼叫本函式）進入前就是原函式，下面最後三行同時驗到。
    """
    before = len(NET_ATTEMPTS)
    entry_state = (socket.create_connection, socket.socket.connect, socket.socket.connect_ex)
    outer_armed = _cage_depth > 0
    with offline():
        with offline():                                   # 可巢狀
            _expect(NetworkBlocked, lambda: socket.create_connection(("example.invalid", 80), 1))
        with closing(socket.socket()) as sk:
            _expect(NetworkBlocked, lambda: sk.connect(("example.invalid", 80)))
            _expect(NetworkBlocked, lambda: sk.connect_ex(("example.invalid", 80)))
    assert len(NET_ATTEMPTS) == before + 3
    del NET_ATTEMPTS[before:]                             # 自我測試的紀錄不算真正的連網企圖
    assert (socket.create_connection, socket.socket.connect, socket.socket.connect_ex) \
        == entry_state, "離開籠子後沒有還原成進入前的樣子"
    if not outer_armed:
        assert socket.create_connection is _PRISTINE_SOCKET["create_connection"]
        assert socket.socket.connect is _PRISTINE_SOCKET["connect"]
        assert socket.socket.connect_ex is _PRISTINE_SOCKET["connect_ex"]


def test_importing_this_file_leaves_socket_unpatched():
    """import 本檔不留下全域 socket patch：在一個乾淨的子行程裡 import 後逐一比對身分。"""
    rc, out, err = _run_child("""
        import socket
        before = (socket.create_connection, socket.socket.connect, socket.socket.connect_ex)
        sys.path.insert(0, os.path.join(REPO, "tests"))
        import test_store
        after = (socket.create_connection, socket.socket.connect, socket.socket.connect_ex)
        print("SAME" if all(a is b for a, b in zip(before, after)) else "PATCHED")
    """, REPO=REPO_ROOT)
    assert rc == 0 and "SAME" in out, (rc, out, err)


# ============================== 不用 pytest 也能跑 ==============================
if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    runtime_existed = os.path.exists(paths.RUNTIME_DIR)
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            with offline():
                fn()
            print(f"PASS  {name}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL  {name}: {type(e).__name__}: {e}")
    # 整輪的兩條不變式：沒有任何連網企圖、沒有在真正的 runtime/ 留下東西
    runner_failed = 0
    if NET_ATTEMPTS:
        runner_failed += 1
        print(f"FAIL  <runner>: 測試期間有連網企圖 {NET_ATTEMPTS}")
    if os.path.exists(paths.RUNTIME_DIR) != runtime_existed:
        runner_failed += 1
        print(f"FAIL  <runner>: 測試在真正的 runtime/ 留下了東西（{paths.RUNTIME_DIR}）")
    print(f"\n{len(tests) - failed} passed, {failed} failed"
          + (f", {runner_failed} runner check(s) failed" if runner_failed else ""))
    sys.exit(1 if failed or runner_failed else 0)
