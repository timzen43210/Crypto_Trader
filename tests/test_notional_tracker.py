# -*- coding: utf-8 -*-
"""
A3（live.notional_tracker / live.a_channel）驗收測試 — 名目部位追蹤：結果與 dry run 逐筆一致、重啟可復原。

  AC-1  FR-0 前置小修：store ROLLBACK 失敗 → 中毒；STRATEGY_USER_ID 釘住；事件擋 lone surrogate（與 store 一致）；
        epoch 範圍 / 出場原因 / 方向只在 live/config.py 一份；schema v1（用 0d10ec4 的 store 建）自動升到 v2、
        資料完整、遷移失敗整段 ROLLBACK；出場「已發布」追蹤與「每個鍵最後一筆已平倉」查詢
  AC-2  §2 每一條規則（合成 K 棒）：只碰止盈 / 只碰止損 / 5 分K 兩碰經 1 分K 判先止盈 / 先止損 / 1 分K 同根兩碰 /
        沒有 1 分K（降級路徑）/ 跳空穿越止損（用 5 分K 開盤，不是 1 分K 開盤）/ 訊號那根本身的觸價不算 /
        冷卻邊界（剛好到期可進、差 1 根不可）/ 持倉中不進場 / s4 與 s5 同一個幣各自獨立；
        等價性：隨機路徑上與 dry run **真正的程式**（pionex_dryrun.step_symbol + pionex_backtest.resolve_with_5m，
        api_get 換成讀同一份合成 K 棒）逐筆比對，並拿「跳空改用 1 分K 開盤」的爛實作跑同一條必須失敗
  AC-3  發布失敗不標記、之後重送；出場不超車進場；每個 signal_id 的事件序列都是「進場 → 出場」
  AC-4  四種當機時點（寫庫前 / 寫庫後發布前 / 平倉後發布前 / 停機期間觸價）重啟後與「一直沒停過」逐筆相同，
        訂閱者去重後不重複
  AC-7  模組名不撞名；A3 程式碼沒有策略數值字面值（tokenize，含反向對照）；參數全部取自 strategy/；
        執行參數在 execution_params()；python -m live exit 0；執行入口的接線自檢與 --events-jsonl
  A4 FR-10（TASK-118 AC-9）資料庫失敗退避：重建嘗試依序在上一次失敗 +60 / +120 / +300 / +600 / +900 / +900… 秒；
        退避中的 tick 不開庫、不取數、不重試延後的訊號；成功一次歸零；退避表只在 config；資料庫正常時不跳過任何 tick

全程離線：假交易所（取數注入）、假時鐘（不 sleep），每個測試關在 socket 籠子裡；runner 另外確認整輪沒有連網企圖、
沒有在真正的 runtime/ 留下東西。策略參數一律由 strategy/ 推出來（exit_params / cooldown_bars），不寫數值。
不依賴 pytest：直接 `python tests/test_notional_tracker.py`。
"""
import ast
import contextlib
import functools
import importlib.util
import io
import json
import logging
import math
import os
import queue
import random
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import tokenize
import types

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

_PRISTINE = (socket.socket.connect, socket.socket.connect_ex, socket.create_connection)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from live import config, klines, paths, signal_events, store  # noqa: E402
from live import notional_tracker as nt  # noqa: E402
from live.bus import SignalBus  # noqa: E402
from live.pionex_api import ApiError  # noqa: E402
from live.signal_events import EntryEvent, ExitEvent  # noqa: E402
from strategy import s4_signal, s5_signal  # noqa: E402

M1 = klines.interval_ms("1M")
T0 = 1_790_146_800_000            # UTC 2026-09-23 07:00，整點（也是 5 分K 邊界）
SPECS = nt.build_specs()
S4, S5 = SPECS["s4"], SPECS["s5"]
MAIN4 = S4.main_ms
WAIT = int(round(config.BAR_FINALIZE_WAIT_SECONDS * 1000))
SYM = "TEST_USDT_PERP"


# ============================== 離線籠子 ==============================
class NetworkBlocked(RuntimeError):
    pass


NET_ATTEMPTS = []


@contextlib.contextmanager
def offline():
    saved = (socket.socket.connect, socket.socket.connect_ex, socket.create_connection)

    def deny(*a, **k):
        NET_ATTEMPTS.append(a[1:] if len(a) > 1 else a)
        raise NetworkBlocked("離線測試企圖連網")

    socket.socket.connect = deny
    socket.socket.connect_ex = deny
    socket.create_connection = deny
    try:
        try:
            socket.create_connection(("198.51.100.1", 9), 1)
        except NetworkBlocked:
            NET_ATTEMPTS.pop()
        else:
            raise AssertionError("籠子沒有生效")
        yield
    finally:
        socket.socket.connect, socket.socket.connect_ex, socket.create_connection = saved


def _offline(fn):
    @functools.wraps(fn)
    def wrapper():
        with offline():
            return fn()
    return wrapper


# ============================== 假時鐘 / 假交易所 / 訂閱者 ==============================
class Clock:
    def __init__(self, now):
        self.now = int(now)

    def __call__(self):
        return self.now


class FakeExchange:
    """記憶體裡的 K 棒。fetch() 的語意照派網實測：回傳開盤 <= endTime（= end_close - 1）的最後 limit 根，新到舊，
    數值是字串；尚未開盤的不給（開盤 <= 現在的那根給，含未收完的）。retention[interval] 之外 → KlinesUnavailable。"""

    def __init__(self, clock):
        self.clock = clock
        self.series = {}
        self.retention = {}
        self.fail_next = 0
        self.calls = []
        self.hidden_after = {}      # symbol -> 開盤 >= 這個時刻的 K 棒先藏起來（模擬「目標 K 棒還沒有」）

    def put(self, symbol, interval, t, o, h, l, c, v=1.0):
        self.series.setdefault((symbol, interval), {})[int(t)] = (float(o), float(h), float(l), float(c), float(v))

    def put_bars(self, symbol, interval, bars):
        for b in bars:
            self.put(symbol, interval, *b)

    def rows(self, symbol, interval, end_open, limit, now=None):
        s = self.series.get((symbol, interval), {})
        ts = [t for t in sorted(s) if t <= end_open and (now is None or t <= now)]
        hide = self.hidden_after.get(symbol)
        if hide is not None:
            ts = [t for t in ts if t < hide]
        ts = ts[-int(limit):] if limit else []
        return [{"time": t, "open": repr(s[t][0]), "high": repr(s[t][1]), "low": repr(s[t][2]),
                 "close": repr(s[t][3]), "volume": repr(s[t][4])} for t in reversed(ts)]

    def fetch(self, symbol, interval, end_close_ms, limit):
        self.calls.append((symbol, interval, end_close_ms, limit))
        if self.fail_next > 0:
            self.fail_next -= 1
            raise ApiError("/api/v1/market/klines: 假交易所的暫時錯誤")
        now = self.clock()
        ret = self.retention.get(interval)
        if ret is not None and end_close_ms - 1 < now - ret:
            raise nt.KlinesUnavailable("MARKET_INVALID_TIME（假）")
        rows = self.rows(symbol, interval, end_close_ms - 1, limit, now=now)
        if ret is not None:
            rows = [r for r in rows if r["time"] >= now - ret]
        return rows


class Recorder:
    """訂閱者：記下每一次送達（含重送）。fail_* 可以讓它失敗（Exception）或模擬當機（BaseException）。"""

    def __init__(self):
        self.raw = []
        self.fail = None            # None / "entry" / "exit" / "all"
        self.crash = None           # (kind, signal_id 或 None) → 拋 SimulatedCrash

    def __call__(self, event):
        kind = "entry" if isinstance(event, EntryEvent) else "exit"
        self.raw.append((kind, event.signal_id, event))
        if self.crash is not None and self.crash[0] == kind and self.crash[1] in (None, event.signal_id):
            self.crash = None
            raise SimulatedCrash("在 %s %s 發布途中當機" % (kind, event.signal_id))
        if self.fail in (kind, "all"):
            raise RuntimeError("訂閱者暫時失敗")

    def deduped(self):
        seen, out = set(), []
        for kind, sid, ev in self.raw:
            if (sid, kind) not in seen:
                seen.add((sid, kind))
                out.append((kind, sid, ev))
        return out


class SimulatedCrash(BaseException):
    """模擬行程死掉：BaseException，匯流排與 A3 都不攔（它們只攔 Exception）。"""


def make_bus(rec):
    bus = SignalBus()
    bus.subscribe(EntryEvent, rec, "recorder")
    bus.subscribe(ExitEvent, rec, "recorder")
    return bus


@contextlib.contextmanager
def tempdir():
    d = tempfile.mkdtemp(prefix="a3_test_")
    try:
        yield d
    finally:
        shutil.rmtree(d)


class Harness:
    """一套 A3：假時鐘 + 假交易所 + 暫存資料庫 + 記錄訂閱者。restart() 模擬重啟（同一個資料庫、同一個訂閱者）。"""

    def __init__(self, tmp, now=T0, specs=None):
        self.tmp = tmp
        self.clock = Clock(now)
        self.ex = FakeExchange(self.clock)
        self.rec = Recorder()
        self.bus = make_bus(self.rec)
        self.db_path = os.path.join(tmp, "live.sqlite3")
        self.specs = specs or SPECS
        self.tracker = None
        self.start()

    def new_tracker(self):
        return nt.NotionalTracker(bus=self.bus, fetcher=self.ex.fetch, now_ms=self.clock, specs=self.specs,
                                  store_factory=lambda: store.open_store(self.db_path, clock=self.clock))

    def start(self):
        self.tracker = self.new_tracker()
        self.tracker.open()
        self.tracker.recover()

    def restart(self, at=None):
        if self.tracker is not None and self.tracker.store is not None:
            self.tracker.store.close()          # 行程死了，OS 收回連線（未提交的交易丟棄）
        if at is not None:
            self.clock.now = int(at)
        self.start()

    def close(self):
        if self.tracker is not None:
            self.tracker.close()

    def signal(self, strategy, bar_open, price, symbol=SYM, features=None, deliver=True):
        spec = self.specs[strategy]
        self.tracker.submit_signal(strategy=strategy, symbol=symbol, bar_open_ms=bar_open,
                                   bar_close_ms=bar_open + spec.main_ms, signal_price=price,
                                   features=features or {"f": 1.0})
        if deliver:
            self.clock.now = max(self.clock.now, bar_open + spec.main_ms + WAIT)
            self.tracker.process_pending()

    def tick_until(self, end_close, every=M1):
        t = nt.floor_to(self.clock.now - WAIT, every)
        while t < end_close:
            t += every
            self.clock.now = max(self.clock.now, t + WAIT)
            self.tracker.tick(t)

    def db(self):
        return self.tracker.store

    def closed(self):
        with contextlib.closing(sqlite3.connect(self.db_path)) as c:
            c.row_factory = sqlite3.Row
            return [dict(r) for r in c.execute(
                "SELECT p.*, s.bar_open_ms, s.published_ms FROM positions p JOIN signals s USING (signal_id) "
                "WHERE p.status = 'closed' ORDER BY p.closed_ms, p.signal_id")]

    def all_positions(self):
        with contextlib.closing(sqlite3.connect(self.db_path)) as c:
            c.row_factory = sqlite3.Row
            return [dict(r) for r in c.execute(
                "SELECT p.*, s.published_ms FROM positions p JOIN signals s USING (signal_id) ORDER BY p.signal_id")]


def flat_bars(t_from, t_to, price, step=M1):
    """[t_from, t_to) 的平盤 K 棒（開高低收都是 price）。"""
    return [(t, price, price, price, price) for t in range(int(t_from), int(t_to), step)]


def with_harness(fn):
    @functools.wraps(fn)
    def wrapper():
        with tempdir() as tmp:
            h = Harness(tmp)
            try:
                return fn(h)
            finally:
                h.close()
    return wrapper


# ============================== AC-1：FR-0 前置小修 ==============================
class _RollbackFailingConn:
    """包住真的連線：ROLLBACK 一律失敗（模擬磁碟錯誤）。其他全部轉給真的連線。"""

    def __init__(self, conn):
        self._c = conn
        self.rollbacks = 0

    def execute(self, sql, *args):
        if sql.strip().upper() == "ROLLBACK":
            self.rollbacks += 1
            raise sqlite3.OperationalError("disk I/O error（測試注入）")
        return self._c.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._c, name)


def _levels(spec, price):
    return nt.levels(spec, price)


def _entry_kw(sid, strategy="s4", symbol="AAA_USDT_PERP", price=2.5, **over):
    tp, sl = _levels(SPECS[strategy], price)
    kw = dict(signal_id=sid, user_id=config.STRATEGY_USER_ID, strategy=strategy, symbol=symbol,
              side=config.DIRECTION_SHORT, bar_open_ms=T0, signal_price=price, take_profit_price=tp,
              stop_loss_price=sl, features={"f": 1.0}, created_ms=T0 + 1, opened_ms=T0 + MAIN4)
    kw.update(over)
    return kw


@_offline
def test_fr0_store_is_poisoned_when_rollback_fails():
    with tempdir() as tmp:
        path = os.path.join(tmp, "db.sqlite3")
        s = store.open_store(path, clock=Clock(T0 + 5))
        s.record_entry(**_entry_kw("s4-a"))
        real = s._conn
        s._conn = _RollbackFailingConn(real)
        try:
            # 同鍵第二筆 open：INSERT 失敗 → ROLLBACK 也失敗 → 原本的例外照樣拋出，store 中毒
            try:
                s.record_entry(**_entry_kw("s4-b"))
            except store.OpenPositionExistsError:
                pass
            else:
                raise AssertionError("應該拋 OpenPositionExistsError")
            assert s._conn is None and s.poisoned, "ROLLBACK 失敗後 store 應該中毒並關掉連線"
            for call in (lambda: s.open_positions(), lambda: s.get_signal("s4-a"),
                         lambda: s.unpublished_signals(), lambda: s.mark_published("s4-a"),
                         lambda: s.record_entry(**_entry_kw("s4-c", symbol="C_USDT_PERP"))):
                try:
                    call()
                except store.StorePoisonedError as e:
                    assert "ROLLBACK" in str(e) and "open_store" in str(e), e
                else:
                    raise AssertionError("中毒後的呼叫沒有拋 StorePoisonedError")
            assert issubclass(store.StorePoisonedError, store.StoreError)
            assert "(poisoned)" in repr(s)
        finally:
            with contextlib.suppress(Exception):
                real.close()
        # 重新開啟：未提交的第二筆完全不在（連線關閉時丟棄），第一筆完好
        with contextlib.closing(store.open_store(path, clock=Clock(T0 + 9))) as s2:
            assert [p["signal_id"] for p in s2.open_positions()] == ["s4-a"]
            assert s2.get_signal("s4-b") is None
            assert [g["signal_id"] for g in s2.unpublished_signals()] == ["s4-a"]


@_offline
def test_fr0_rollback_success_does_not_poison():
    """對照組：ROLLBACK 成功時照舊（store 可以繼續用），證明中毒只在 ROLLBACK 失敗時發生。"""
    with tempdir() as tmp, contextlib.closing(store.open_store(os.path.join(tmp, "d.sqlite3"),
                                                                clock=Clock(T0 + 5))) as s:
        s.record_entry(**_entry_kw("s4-a"))
        try:
            s.record_entry(**_entry_kw("s4-b"))
        except store.OpenPositionExistsError:
            pass
        assert not s.poisoned
        s.record_entry(**_entry_kw("s5-a", strategy="s5"))
        assert len(s.open_positions()) == 2


def test_fr0_strategy_user_id_is_pinned():
    """改了它，重啟復原就找不到既有的策略層紀錄（B4′ S-2）。這是資料的身分，不是策略參數。"""
    assert config.STRATEGY_USER_ID == "strategy", config.STRATEGY_USER_ID
    assert "STRATEGY_USER_ID" not in config.execution_params()


@_offline
def test_fr0_lone_surrogate_rejected_by_events_and_store_alike():
    lone = chr(0xD800)
    assert ascii(lone) == "'\\ud800'", "前提：測試字元真的是 lone surrogate"
    base = dict(strategy="s4", signal_id="#X-S4-20260923-1505", symbol="X_USDT_PERP",
                direction=config.DIRECTION_SHORT, created_ms=T0 + 1, bar_open_ms=T0, signal_price=2.5,
                take_profit_price=_levels(S4, 2.5)[0], stop_loss_price=_levels(S4, 2.5)[1], features={"f": 1.0})
    xbase = dict(strategy="s4", signal_id="#X-S4-20260923-1505", symbol="X_USDT_PERP",
                 direction=config.DIRECTION_SHORT, created_ms=T0 + 2, reason=config.EXIT_STOP_LOSS, exit_price=2.6,
                 entry_price=2.5, opened_ms=T0, closed_ms=T0 + 1, features={})
    for name in ("signal_id", "symbol"):
        for cls, kw in ((EntryEvent, base), (ExitEvent, xbase)):
            try:
                cls(**{**kw, name: "X" + lone + "_USDT"})
            except ValueError as e:
                assert name in str(e) and "surrogate" in str(e), e
            else:
                raise AssertionError("%s.%s 收下了 lone surrogate" % (cls.__name__, name))
    # features 的鍵與值照收（store 存成純 ASCII JSON）
    EntryEvent(**{**base, "features": {"k" + lone: "v" + lone}})
    with tempdir() as tmp, contextlib.closing(store.open_store(os.path.join(tmp, "d.sqlite3"),
                                                                clock=Clock(T0 + 5))) as s:
        with contextlib.closing(sqlite3.connect(s.path)) as raw:
            before = raw.execute("SELECT count(*) FROM signals").fetchone()
        for name in ("signal_id", "symbol", "user_id"):
            try:
                s.record_entry(**_entry_kw("ok", **{name: "X" + lone}))
            except ValueError as e:
                assert "surrogate" in str(e), e
            else:
                raise AssertionError("store 的 %s 收下了 lone surrogate" % name)
        with contextlib.closing(sqlite3.connect(s.path)) as raw:
            assert raw.execute("SELECT count(*) FROM signals").fetchone() == before, "被擋下的寫入碰到了資料庫"


def _code_constants(path):
    """模組裡的字串 / 數字常數（不含 docstring）。"""
    tree = ast.parse(open(path, encoding="utf-8").read())
    docs = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                docs.add(id(first.value))
    return [n for n in ast.walk(tree) if isinstance(n, ast.Constant) and id(n) not in docs]


@_offline
def test_fr0_epoch_range_is_one_shared_constant():
    lo, hi = config.EPOCH_MS_MIN, config.EPOCH_MS_MAX
    for mod in ("store.py", "signal_events.py"):
        tree = ast.parse(open(os.path.join(REPO_ROOT, "live", mod), encoding="utf-8").read())
        pows = [ast.unparse(n) for n in ast.walk(tree) if isinstance(n, ast.BinOp) and isinstance(n.op, ast.Pow)
                and isinstance(n.left, ast.Constant) and n.left.value == 10]
        big = [c.value for c in _code_constants(os.path.join(REPO_ROOT, "live", mod))
               if isinstance(c.value, int) and not isinstance(c.value, bool) and c.value >= 10 ** 11]
        assert not pows and not big, "live/%s 還有自己的 epoch 常數：%r %r" % (mod, pows, big)
    # 兩邊都在呼叫當下讀 config：暫時收窄上限，兩邊同時拒收
    tp, sl = _levels(S4, 2.5)
    ok = dict(strategy="s4", signal_id="#X", symbol="X_USDT_PERP", direction=config.DIRECTION_SHORT,
              created_ms=hi - 1, bar_open_ms=hi - 1, signal_price=2.5, take_profit_price=tp,
              stop_loss_price=sl, features={"f": 1.0})
    EntryEvent(**ok)
    for bad in (hi, lo - 1):
        with contextlib.suppress(ValueError):
            EntryEvent(**{**ok, "created_ms": bad})
            raise AssertionError("EntryEvent 收下了範圍外的 %r" % bad)
    with tempdir() as tmp, contextlib.closing(store.open_store(os.path.join(tmp, "d.sqlite3"),
                                                                clock=Clock(T0 + 5))) as s:
        s.record_entry(**_entry_kw("a", bar_open_ms=hi - 1, created_ms=hi - 1, opened_ms=hi - 1))
        for bad in (hi, lo - 1):
            with contextlib.suppress(ValueError):
                s.record_entry(**_entry_kw("b", symbol="B_USDT_PERP", bar_open_ms=bad))
                raise AssertionError("store 收下了範圍外的 %r" % bad)
        saved = config.EPOCH_MS_MAX
        try:
            config.EPOCH_MS_MAX = T0
            for fn in (lambda: EntryEvent(**{**ok, "created_ms": T0, "bar_open_ms": T0}),
                       lambda: s.record_entry(**_entry_kw("c", symbol="C_USDT_PERP", bar_open_ms=T0))):
                with contextlib.suppress(ValueError):
                    fn()
                    raise AssertionError("改了 config.EPOCH_MS_MAX 卻沒有跟著變")
        finally:
            config.EPOCH_MS_MAX = saved


@_offline
def test_fr0_exit_reasons_and_directions_live_only_in_config():
    values = {config.EXIT_TAKE_PROFIT, config.EXIT_STOP_LOSS, config.DIRECTION_SHORT}
    for mod in ("store.py", "signal_events.py", "notional_tracker.py", "a_channel.py"):
        strs = [c.value for c in _code_constants(os.path.join(REPO_ROOT, "live", mod)) if isinstance(c.value, str)]
        assert not values & set(strs), "live/%s 自己寫了出場原因 / 方向的值：%r" % (mod, values & set(strs))
    assert signal_events.DIRECTION_SHORT == config.DIRECTION_SHORT
    assert signal_events.EXIT_TAKE_PROFIT == config.EXIT_TAKE_PROFIT
    assert signal_events.EXIT_STOP_LOSS == config.EXIT_STOP_LOSS
    for gone in ("SIDES", "EXIT_REASONS", "MIN_EPOCH_MS", "MAX_EPOCH_MS"):
        assert not hasattr(store, gone), "store 還留著自己的 %s" % gone
    for gone in ("DIRECTIONS", "EXIT_REASONS", "_MIN_EPOCH_MS", "_MAX_EPOCH_MS"):
        assert not hasattr(signal_events, gone), "signal_events 還留著自己的 %s" % gone
    # 呼叫當下讀：暫時多一種出場原因，事件與 store 同時收；拿掉之後同時拒收
    saved = config.EXIT_REASONS
    x = dict(strategy="s4", signal_id="#X", symbol="X_USDT_PERP", direction=config.DIRECTION_SHORT,
             created_ms=T0 + 9, exit_price=2.6, entry_price=2.5, opened_ms=T0, closed_ms=T0 + 5, features={})
    with tempdir() as tmp, contextlib.closing(store.open_store(os.path.join(tmp, "d.sqlite3"),
                                                                clock=Clock(T0 + 5))) as s:
        s.record_entry(**_entry_kw("a"))
        try:
            config.EXIT_REASONS = saved + ("time_exit",)
            ExitEvent(**x, reason="time_exit")
            s.close_position("a", exit_reason="time_exit", exit_price=2.6, closed_ms=T0 + 5)
            config.EXIT_REASONS = (config.EXIT_TAKE_PROFIT,)
            for fn in (lambda: ExitEvent(**x, reason=config.EXIT_STOP_LOSS),
                       lambda: s.close_position("a", exit_reason=config.EXIT_STOP_LOSS, exit_price=2.6)):
                with contextlib.suppress(ValueError):
                    fn()
                    raise AssertionError("改了 config.EXIT_REASONS 卻沒有跟著變")
        finally:
            config.EXIT_REASONS = saved


def _v1_store_module(tmp):
    """0d10ec4（B4′）的 live/store.py 原檔，載入成獨立模組。"""
    r = subprocess.run(["git", "show", "0d10ec4:live/store.py"], cwd=REPO_ROOT, capture_output=True, timeout=60)
    assert r.returncode == 0, "取不到 0d10ec4 的 store（需要 git 歷史）：%s" % r.stderr[-500:]
    path = os.path.join(tmp, "store_v1_0d10ec4.py")
    with open(path, "wb") as f:
        f.write(r.stdout)
    spec = importlib.util.spec_from_file_location("store_v1_0d10ec4", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.SCHEMA_VERSION == 1
    return mod


def _table_dump(path, table, cols=None):
    with contextlib.closing(sqlite3.connect(path)) as c:
        cols = cols or [r[1] for r in c.execute("PRAGMA table_info(%s)" % table)]
        sel = ", ".join("%s, typeof(%s)" % (k, k) for k in cols)
        return cols, c.execute("SELECT %s FROM %s ORDER BY signal_id" % (sel, table)).fetchall()


@_offline
def test_fr0_schema_v1_database_upgrades_to_v2_with_data_intact():
    with tempdir() as tmp:
        v1 = _v1_store_module(tmp)
        assert v1._DDL == store._DDL, "store._DDL 必須是 v1 原封不動的 DDL（遷移的起點）"
        path = os.path.join(tmp, "old.sqlite3")
        s = v1.open_store(path, clock=Clock(T0 + 7))
        s.record_entry(**_entry_kw("s4-a"))
        s.record_entry(**_entry_kw("s5-a", strategy="s5", features={"漲幅": 0.3, "inf": math.inf}))
        s.record_entry(**_entry_kw("s4-b", symbol="BBB_USDT_PERP"))
        s.mark_published("s4-a", published_ms=T0 + 8)
        s.close_position("s4-a", exit_reason="take_profit", exit_price=2.4, closed_ms=T0 + 3 * MAIN4)
        s.close_position("s5-a", exit_reason="stop_loss", exit_price=2.63, closed_ms=T0 + 4 * MAIN4)
        s.close()
        with contextlib.closing(sqlite3.connect(path)) as c:
            assert c.execute("PRAGMA user_version").fetchone()[0] == 1
        sig_before = _table_dump(path, "signals")
        pos_cols, pos_before = _table_dump(path, "positions")

        with contextlib.closing(store.open_store(path, clock=Clock(T0 + 99))) as s2:
            assert s2.get_position("s4-a")["exit_features"] is None
            unpub = s2.unpublished_exits()
            # v1 沒有記錄出場是否發布過：升級後一律當成「未發布」（寧可重送，不可漏發；訂閱者會去重）
            assert [r["signal_id"] for r in unpub] == ["s4-a", "s5-a"], unpub
            assert [r["entry_published_ms"] for r in unpub] == [T0 + 8, None]
            s2.mark_exit_published("s4-a", published_ms=T0 + 100)
        with contextlib.closing(sqlite3.connect(path)) as c:
            assert c.execute("PRAGMA user_version").fetchone()[0] == store.SCHEMA_VERSION == 2
            cols_after = [r[1] for r in c.execute("PRAGMA table_info(positions)")]
        assert cols_after == pos_cols + ["exit_features_json", "exit_published_ms"], cols_after
        assert _table_dump(path, "signals") == sig_before, "升級動到了訊號表"
        _, pos_after = _table_dump(path, "positions", pos_cols)
        assert pos_after == pos_before, "升級動到了 v1 的欄位"
        # 升級後的 schema 與新建的 v2 逐字相同
        fresh = os.path.join(tmp, "fresh.sqlite3")
        store.open_store(fresh, clock=Clock(T0)).close()
        q = "SELECT type, name, sql FROM sqlite_master ORDER BY name"
        with contextlib.closing(sqlite3.connect(path)) as a, contextlib.closing(sqlite3.connect(fresh)) as b:
            assert a.execute(q).fetchall() == b.execute(q).fetchall(), "升級上來的 schema 與新建的不同"
        # 再開一次：冪等
        store.open_store(path, clock=Clock(T0)).close()


@_offline
def test_fr0_failed_migration_rolls_back_to_v1():
    with tempdir() as tmp:
        v1 = _v1_store_module(tmp)
        path = os.path.join(tmp, "old.sqlite3")
        s = v1.open_store(path, clock=Clock(T0 + 7))
        s.record_entry(**_entry_kw("s4-a"))
        s.close()
        before = _table_dump(path, "positions")
        saved = store._MIGRATIONS[2]
        store._MIGRATIONS[2] = (saved[0], "ALTER TABLE no_such_table ADD COLUMN x INTEGER")
        try:
            try:
                store.open_store(path, clock=Clock(T0))
            except sqlite3.OperationalError:
                pass
            else:
                raise AssertionError("遷移失敗卻沒有拋例外")
        finally:
            store._MIGRATIONS[2] = saved
        with contextlib.closing(sqlite3.connect(path)) as c:
            assert c.execute("PRAGMA user_version").fetchone()[0] == 1, "遷移失敗卻改了版本"
        assert _table_dump(path, "positions") == before, "遷移失敗卻留下了第一個 ALTER"
        with contextlib.closing(store.open_store(path, clock=Clock(T0))) as s2:     # 修好之後照常升級
            assert s2.get_position("s4-a")["status"] == "open"


@_offline
def test_fr0_exit_publication_tracking_and_last_closed_queries():
    with tempdir() as tmp, contextlib.closing(store.open_store(os.path.join(tmp, "d.sqlite3"),
                                                                clock=Clock(T0 + 5))) as s:
        s.record_entry(**_entry_kw("a1"))
        s.record_entry(**_entry_kw("b1", symbol="BBB_USDT_PERP"))
        s.record_entry(**_entry_kw("a5", strategy="s5"))
        for sid, reason in (("nope", "missing"), ("a1", "open")):
            try:
                s.mark_exit_published(sid)
            except store.ExitNotPendingError as e:
                assert e.reason == reason and e.signal_id == sid
            else:
                raise AssertionError("mark_exit_published(%r) 應該拋例外" % sid)
        s.mark_published("a1", published_ms=T0 + 6)
        s.close_position("a1", exit_reason="take_profit", exit_price=2.4, closed_ms=T0 + 2 * MAIN4,
                         exit_features={"gap_open": False, "judged_interval": "1M", "x": math.inf})
        s.close_position("a5", exit_reason="stop_loss", exit_price=2.63, closed_ms=T0 + MAIN4 + M1)
        got = s.unpublished_exits()
        assert [(r["signal_id"], r["entry_published_ms"], r["side"]) for r in got] == \
            [("a5", None, config.DIRECTION_SHORT), ("a1", T0 + 6, config.DIRECTION_SHORT)], got
        assert got[1]["exit_features"] == {"gap_open": False, "judged_interval": "1M", "x": math.inf}
        assert got[0]["exit_features"] is None
        s.mark_exit_published("a1", published_ms=T0 + 3 * MAIN4)
        try:
            s.mark_exit_published("a1")
        except store.ExitNotPendingError as e:
            assert e.reason == "published"
        assert s.get_position("a1")["exit_published_ms"] == T0 + 3 * MAIN4
        assert [r["signal_id"] for r in s.unpublished_exits()] == ["a5"]
        # 同鍵再開再平：最後一筆已平倉是最晚平倉的那筆
        s.record_entry(**_entry_kw("a2", bar_open_ms=T0 + 20 * MAIN4, opened_ms=T0 + 21 * MAIN4))
        s.close_position("a2", exit_reason="stop_loss", exit_price=2.63, closed_ms=T0 + 25 * MAIN4)
        last = {(r["strategy"], r["symbol"]): r["signal_id"] for r in s.last_closed_positions()}
        assert last == {("s4", "AAA_USDT_PERP"): "a2", ("s5", "AAA_USDT_PERP"): "a5"}, last
        assert [r["signal_id"] for r in s.last_closed_positions(strategy="s5")] == ["a5"]
        assert s.last_closed_positions(user_id="someone_else") == []
        # 表層 CHECK：open 的部位不可以有出場欄位（繞過本模組直接下 SQL）
        with contextlib.closing(sqlite3.connect(s.path)) as c:
            for col, val in (("exit_published_ms", T0), ("exit_features_json", "{}")):
                try:
                    c.execute("UPDATE positions SET %s = ? WHERE signal_id = 'b1'" % col, (val,))
                except sqlite3.IntegrityError:
                    pass
                else:
                    raise AssertionError("open 部位寫進了 %s" % col)


# ============================== AC-2：規則（合成 K 棒） ==============================
def _one_trade(h, strategy, price, bars_after, *, sig_bar=None, until=None, prefix=None):
    """訊號 K 棒（開盤 sig_bar）收盤進場，之後的 1 分K 由 bars_after 給（list of (o,h,l,c)），跑完回傳平倉列。"""
    spec = h.specs[strategy]
    sig_bar = T0 if sig_bar is None else sig_bar
    close = sig_bar + spec.main_ms
    pre = prefix if prefix is not None else flat_bars(sig_bar, close, price)
    h.ex.put_bars(SYM, "1M", pre)
    t = close
    for o, hi, lo, c in bars_after:
        h.ex.put(SYM, "1M", t, o, hi, lo, c)
        t += M1
    h.signal(strategy, sig_bar, price)
    h.tick_until(until or t)
    return h.closed()


def _only(rows):
    assert len(rows) == 1, rows
    r = rows[0]
    r["features"] = json.loads(r["exit_features_json"]) if r.get("exit_features_json") else None
    return r


@_offline
@with_harness
def test_ac2_only_take_profit(h):
    p = 100.0
    tp, sl = _levels(S4, p)
    r = _only(_one_trade(h, "s4", p, [(p, p, p, p), (p, p, tp, tp * 1.001), (p, p, p, p)]))
    assert (r["exit_reason"], r["exit_price"]) == (config.EXIT_TAKE_PROFIT, tp)
    assert r["closed_ms"] == T0 + MAIN4 + 2 * M1, "出場時刻 = 觸價那根 1 分K 的收盤"
    assert r["features"]["exit_bar_open_ms"] == T0 + MAIN4 and not r["features"]["gap_open"]


@_offline
@with_harness
def test_ac2_only_stop_loss(h):
    p = 100.0
    tp, sl = _levels(S4, p)
    r = _only(_one_trade(h, "s4", p, [(p, p, p, p), (p, p, p, p), (p, sl, p, p)]))
    assert (r["exit_reason"], r["exit_price"]) == (config.EXIT_STOP_LOSS, sl)
    assert r["closed_ms"] == T0 + MAIN4 + 3 * M1
    f = r["features"]
    assert not (f["gap_open"] or f["same_bar_both"] or f["minor_resolved"])


@_offline
@with_harness
def test_ac2_main_bar_both_minor_decides_take_profit_first(h):
    p = 100.0
    tp, sl = _levels(S4, p)
    # 同一根 5 分K：第 2 根 1 分K 碰止盈、第 4 根碰止損 → 先止盈
    r = _only(_one_trade(h, "s4", p, [(p, p, p, p), (p, p, tp, tp), (tp, tp, tp, tp), (tp, sl, tp, p), (p, p, p, p)]))
    assert (r["exit_reason"], r["exit_price"], r["closed_ms"]) == (config.EXIT_TAKE_PROFIT, tp, T0 + MAIN4 + 2 * M1)


@_offline
@with_harness
def test_ac2_main_bar_both_minor_decides_stop_loss_first(h):
    p = 100.0
    tp, sl = _levels(S4, p)
    r = _only(_one_trade(h, "s4", p, [(p, p, p, p), (p, sl, p, sl), (sl, sl, tp, tp), (tp, tp, tp, tp), (tp,) * 4]))
    assert (r["exit_reason"], r["exit_price"], r["closed_ms"]) == (config.EXIT_STOP_LOSS, sl, T0 + MAIN4 + 2 * M1)
    assert not r["features"]["gap_open"], "5 分K 開盤 <= 止損，不是跳空"


@_offline
@with_harness
def test_ac2_minor_same_bar_both_is_stop_loss(h):
    p = 100.0
    tp, sl = _levels(S4, p)
    r = _only(_one_trade(h, "s4", p, [(p, p, p, p), (p, sl * 1.01, tp * 0.99, p)]))
    assert (r["exit_reason"], r["exit_price"]) == (config.EXIT_STOP_LOSS, sl)
    f = r["features"]
    assert f["same_bar_both"] and f["minor_resolved"] and not f["gap_open"], f


@_offline
@with_harness
def test_ac2_s5_same_bar_both_is_stop_loss_without_minor(h):
    p = 50.0
    tp, sl = _levels(S5, p)
    r = _only(_one_trade(h, "s5", p, [(p, p, p, p), (p, sl, tp, p)]))
    assert (r["exit_reason"], r["exit_price"]) == (config.EXIT_STOP_LOSS, sl)
    f = r["features"]
    assert f["same_bar_both"] and not f["minor_resolved"] and f["judged_interval"] == S5.monitor_interval


@_offline
@with_harness
def test_ac2_gap_uses_main_bar_open_not_minor_open(h):
    """5 分K 開盤 <= 止損，但中間某根 1 分K 開盤 > 止損 → 出場價是止損價（不是那根 1 分K 的開盤）。"""
    p = 100.0
    tp, sl = _levels(S4, p)
    jump = sl * 1.02
    r = _only(_one_trade(h, "s4", p, [(p, p, p, p), (p, p, p, p), (jump, jump, jump, jump)]))
    assert (r["exit_reason"], r["exit_price"]) == (config.EXIT_STOP_LOSS, sl), r
    assert not r["features"]["gap_open"]


@_offline
@with_harness
def test_ac2_gap_when_main_bar_opens_above_stop_loss(h):
    """5 分K 開盤就 > 止損 → 出場價是 5 分K 開盤（= 該 5 分K 第一根 1 分K 的開盤），記開盤跳空。"""
    p = 100.0
    tp, sl = _levels(S4, p)
    gap_open = sl * 1.03
    bars = [(p, p, p, p)] * 5 + [(gap_open, gap_open, sl * 1.01, sl * 1.015), (sl * 1.015, sl * 1.04, sl, sl)]
    r = _only(_one_trade(h, "s4", p, bars))
    assert (r["exit_reason"], r["exit_price"]) == (config.EXIT_STOP_LOSS, gap_open), r
    assert r["closed_ms"] == T0 + 2 * MAIN4 + M1 and r["features"]["gap_open"]


@_offline
@with_harness
def test_ac2_touch_inside_the_signal_bar_does_not_count(h):
    p = 100.0
    tp, sl = _levels(S4, p)
    # 訊號那根 5 分K 內的 1 分K 曾碰止損與止盈，收盤回到訊號價；之後平盤 → 不可以出場
    pre = [(T0, p, sl * 1.1, tp * 0.9, p)] + flat_bars(T0 + M1, T0 + MAIN4, p)
    rows = _one_trade(h, "s4", p, [(p, p, p, p)] * 10, prefix=pre)
    assert rows == [] and h.tracker.open_positions() == {("s4", SYM): nt.make_signal_id(S4, SYM, T0 + MAIN4)}


@_offline
@with_harness
def test_ac2_no_minor_data_falls_back_to_dry_run_method(h):
    """1 分K 超過保留期（重啟補判）：改用 5 分K 判定；5 分K 兩邊都碰而 1 分K 拿不到 → 保守計止損、出場記 5 分K 收盤。"""
    p = 100.0
    tp, sl = _levels(S4, p)
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + MAIN4, p))
    h.signal("s4", T0, p)
    sid = nt.make_signal_id(S4, SYM, T0 + MAIN4)
    for k in range(1, 40):
        h.ex.put(SYM, "5M", T0 + k * MAIN4, p, p, p, p)
    h.ex.put(SYM, "5M", T0 + 40 * MAIN4, p, sl * 1.001, tp * 0.999, p)
    for t in range(T0 + MAIN4, T0 + 61 * MAIN4, M1):
        h.ex.put(SYM, "1M", t, p, p, p, p)              # 1 分K 一直都有，但現在只拿得到最近 5 分鐘（保留期）
    h.ex.retention["1M"] = 5 * M1                        # 只剩最近 5 分鐘
    h.restart(at=T0 + 60 * MAIN4)
    r = _only(h.closed())
    assert r["signal_id"] == sid
    assert (r["exit_reason"], r["exit_price"], r["closed_ms"]) == \
        (config.EXIT_STOP_LOSS, sl, T0 + 41 * MAIN4), r
    f = json.loads(r["exit_features_json"])
    assert f["minor_missing"] and f["same_bar_both"] and f["recovered"] and f["judged_interval"] == S4.main_interval


@_offline
@with_harness
def test_ac2_fallback_resolves_with_available_minor_bars(h):
    """降級路徑中 5 分K 兩邊都碰、那根的 1 分K 還拿得到（保留期邊緣）→ 用 1 分K 判先後（dry run 的 resolve）。"""
    p = 100.0
    tp, sl = _levels(S4, p)
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + MAIN4, p))
    h.signal("s4", T0, p)
    hit_bar = T0 + 30 * MAIN4
    for k in range(1, 31):
        o, hi, lo = (p, p, p) if k < 30 else (p, sl, tp)
        h.ex.put(SYM, "5M", T0 + k * MAIN4, o, hi, lo, p)
    minute = [(p, p, p, p), (p, p, tp, tp), (tp, sl, tp, p), (p, p, p, p), (p, p, p, p)]
    for i, (o, hi, lo, c) in enumerate(minute):
        h.ex.put(SYM, "1M", hit_bar + i * M1, o, hi, lo, c)
    for t in range(hit_bar + MAIN4, T0 + 40 * MAIN4, M1):
        h.ex.put(SYM, "1M", t, p, p, p, p)
    now = T0 + 40 * MAIN4
    h.ex.retention["1M"] = now - hit_bar                 # 1 分K 只從那根 5 分K 開始有
    h.restart(at=now + WAIT)
    r = _only(h.closed())
    assert (r["exit_reason"], r["exit_price"], r["closed_ms"]) == (config.EXIT_TAKE_PROFIT, tp, hit_bar + 2 * M1), r


@_offline
@with_harness
def test_ac2_cooldown_boundary(h):
    """s4：出場所屬 5 分K 開盤 + cooldown_bars 根：剛好到期的訊號可以進，差 1 根不可以。"""
    p = 100.0
    tp, sl = _levels(S4, p)
    _one_trade(h, "s4", p, [(p, p, p, p), (p, p, tp, tp)])
    exit_bar = T0 + MAIN4
    ok_bar = exit_bar + S4.cooldown_bars * MAIN4
    early_bar = ok_bar - MAIN4
    h.ex.put_bars(SYM, "1M", flat_bars(T0 + MAIN4 + 2 * M1, ok_bar + 3 * MAIN4, tp))
    h.tick_until(early_bar + MAIN4)
    h.signal("s4", early_bar, tp)
    assert h.tracker.open_positions() == {}, "差 1 根就進場了"
    assert h.tracker.stats["skipped_cooldown"] == 1
    h.tick_until(ok_bar + MAIN4)
    h.signal("s4", ok_bar, tp)
    assert h.tracker.open_positions() == {("s4", SYM): nt.make_signal_id(S4, SYM, ok_bar + MAIN4)}
    assert h.tracker.cooldowns()[("s4", SYM)] == ok_bar


@_offline
@with_harness
def test_ac2_s5_cooldown_next_bar_after_exit_can_enter(h):
    p = 50.0
    tp, sl = _levels(S5, p)
    _one_trade(h, "s5", p, [(p, p, p, p), (p, sl, p, sl)])      # 第 2 根 1 分K 止損
    exit_bar = T0 + S5.main_ms + M1
    h.ex.put_bars(SYM, "1M", flat_bars(exit_bar + M1, exit_bar + 10 * M1, sl))
    h.signal("s5", exit_bar, sl)                                # 出場那根：冷卻擋掉
    assert h.tracker.open_positions() == {}
    nxt = exit_bar + S5.cooldown_bars * S5.main_ms
    h.signal("s5", nxt, sl)
    assert ("s5", SYM) in h.tracker.open_positions()


@_offline
@with_harness
def test_ac2_holding_blocks_new_signal(h):
    p = 100.0
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + 20 * MAIN4, p))
    h.signal("s4", T0, p)
    first = h.tracker.open_positions()
    h.tick_until(T0 + 5 * MAIN4)
    h.signal("s4", T0 + 4 * MAIN4, p * 1.01)
    assert h.tracker.open_positions() == first and h.tracker.stats["skipped_holding"] == 1
    assert len(h.all_positions()) == 1 and [k for k, *_ in h.rec.raw] == ["entry"], "持倉中不寫庫、不發事件"


@_offline
@with_harness
def test_ac2_signal_after_unprocessed_exit_applies_cooldown_not_holding(h):
    """持倉在訊號之前就已觸價、但監控還沒判到那裡（例如取數失敗）：先補判再決定，不可以誤當成持倉中或直接進場。"""
    p = 50.0
    tp, sl = _levels(S5, p)
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + M1, p))
    h.signal("s5", T0, p)
    h.ex.put(SYM, "1M", T0 + M1, p, p, tp, tp)                   # T0+1m 那根止盈
    h.ex.put_bars(SYM, "1M", flat_bars(T0 + 2 * M1, T0 + 10 * M1, tp))
    h.signal("s5", T0 + 2 * M1, tp)                               # 沒有 tick 過，直接來下一筆訊號
    rows = h.closed()
    assert len(rows) == 1 and rows[0]["closed_ms"] == T0 + 2 * M1
    assert ("s5", SYM) in h.tracker.open_positions(), "冷卻 1 根已過，應該進場"


@_offline
@with_harness
def test_ac2_s4_and_s5_on_the_same_symbol_are_independent(h):
    p = 100.0
    tp4, sl4 = _levels(S4, p)
    tp5, sl5 = _levels(S5, p)
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + MAIN4, p))
    h.signal("s4", T0, p)
    h.ex.put_bars(SYM, "1M", flat_bars(T0 + MAIN4, T0 + MAIN4 + 2 * M1, p))
    h.signal("s5", T0 + MAIN4 + M1, p)
    assert set(h.tracker.open_positions()) == {("s4", SYM), ("s5", SYM)}
    # s5 的止盈先到（s5 止盈較淺時），s4 仍持倉；各自的冷卻互不影響
    lo = max(tp5, tp4) if tp5 != tp4 else tp5
    h.ex.put(SYM, "1M", T0 + MAIN4 + 2 * M1, p, p, lo, lo)
    h.ex.put_bars(SYM, "1M", flat_bars(T0 + MAIN4 + 3 * M1, T0 + 4 * MAIN4, lo))
    h.tick_until(T0 + 3 * MAIN4)
    closed = {r["strategy"] for r in h.closed()}
    open_ = set(h.tracker.open_positions())
    assert closed | {s for s, _ in open_} == {"s4", "s5"}
    assert all(k[0] not in closed for k in open_)
    for strat in closed:
        assert (strat, SYM) in h.tracker.cooldowns() and all(k[0] == strat for k in h.tracker.cooldowns())


@_offline
@with_harness
def test_ac2_missing_target_bar_waits_and_does_not_guess(h):
    p = 100.0
    tp, sl = _levels(S4, p)
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + MAIN4, p))
    h.signal("s4", T0, p)
    h.ex.put(SYM, "1M", T0 + MAIN4, p, p, p, p)
    h.ex.put(SYM, "1M", T0 + MAIN4 + M1, p, p, tp, tp)
    h.ex.hidden_after[SYM] = T0 + MAIN4 + M1                      # 觸價那根還沒出現
    h.tick_until(T0 + MAIN4 + 3 * M1)
    assert h.closed() == [], "拿不到的 K 棒被臆測了"
    h.ex.fail_next = 2                                             # 接著兩次取數失敗
    del h.ex.hidden_after[SYM]
    h.tick_until(T0 + MAIN4 + 5 * M1)
    assert h.closed() == [] and h.tracker.stats["fetch_failures"] == 2
    h.tick_until(T0 + MAIN4 + 6 * M1)
    r = _only(h.closed())
    assert (r["exit_reason"], r["closed_ms"]) == (config.EXIT_TAKE_PROFIT, T0 + MAIN4 + 2 * M1), "晚判定但時刻正確"


@_offline
@with_harness
def test_stale_klines_are_warned_once_not_silently_waited(h):
    """取數成功但一直沒有新 K 棒（下架、長時間沒資料）：落後超過一根主週期記一次 WARNING，追上後記 INFO。"""
    p = 50.0
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + 20 * M1, p))
    h.signal("s5", T0, p)
    h.ex.hidden_after[SYM] = T0 + 3 * M1
    cap = _Capture()
    lg = logging.getLogger("live.notional_tracker")
    saved = lg.level
    lg.addHandler(cap)
    lg.setLevel(logging.DEBUG)
    try:
        h.tick_until(T0 + 10 * M1)
        warn = [r for r in cap.records if r.levelno == logging.WARNING and "落後" in r.getMessage()]
        assert len(warn) == 1, [r.getMessage() for r in cap.records]
        del h.ex.hidden_after[SYM]
        h.tick_until(T0 + 12 * M1)
        assert any(r.levelno == logging.INFO and "追上" in r.getMessage() for r in cap.records)
    finally:
        lg.removeHandler(cap)
        lg.setLevel(saved)
    assert h.closed() == [] and ("s5", SYM) in h.tracker.open_positions()


@_offline
@with_harness
def test_ac2_tick_granularity_does_not_change_results(h):
    """每分鐘 tick 與隔很久才 tick 一次（分頁取數）結果完全相同。"""
    rng = random.Random(7)
    bars1 = _random_minutes(rng, T0, 900, 100.0)
    for b in bars1:
        h.ex.put(SYM, "1M", *b)
    sigs = [T0 + k * MAIN4 for k in range(0, 170, 9)]
    res = []
    for sparse in (False, True):
        with tempdir() as tmp:
            g = Harness(tmp)
            g.ex.series = h.ex.series
            g.tracker.page_limit = 7 if sparse else config.A3_KLINES_PAGE_LIMIT
            for sb in sigs:
                if not sparse:
                    g.tick_until(sb + MAIN4)
                price = [b for b in bars1 if b[0] == sb + MAIN4 - M1][0][4]
                g.signal("s4", sb, price)
            g.tick_until(T0 + 900 * M1)
            res.append([(r["signal_id"], r["exit_reason"], r["exit_price"], r["closed_ms"]) for r in g.closed()])
            g.close()
    assert res[0] == res[1] and len(res[0]) >= 3, res


# ============================== AC-2：與 dry run 的真正程式逐筆等價 ==============================
def _random_minutes(rng, t0, n, p0):
    """隨機 1 分K：一般波動 + 偶發的跳空（開盤離前收很遠）+ 偶發的長影線（同根兩碰）。"""
    out, p = [], p0
    q = lambda x: round(x, 6)       # 短小數：與派網的價格字串一樣，字串轉浮點沒有 1 ULP 的歧義
    for i in range(n):
        o = p
        r = rng.random()
        if r < 0.04:
            o = q(p * (1 + rng.choice((-1, 1)) * rng.uniform(0.02, 0.08)))    # 跳空
        c = q(o * (1 + rng.gauss(0, 0.012)))
        hi = q(max(o, c) * (1 + abs(rng.gauss(0, 0.006))))
        lo = q(min(o, c) * (1 - abs(rng.gauss(0, 0.006))))
        if rng.random() < 0.02:
            hi, lo = q(max(o, c) * 1.07), q(min(o, c) * 0.93)                # 長影線
        out.append((t0 + i * M1, o, max(hi, o, c), min(lo, o, c), c))
        p = c
    return out


def _aggregate(minutes, step):
    df = pd.DataFrame(minutes, columns=["time", "open", "high", "low", "close"])
    df["bucket"] = df["time"] // step * step
    g = df.groupby("bucket", sort=True)
    return pd.DataFrame({"time": g["time"].first().index.astype("int64"), "open": g["open"].first().values,
                         "high": g["high"].max().values, "low": g["low"].min().values,
                         "close": g["close"].last().values})


def _dry_run_trades(book, symbol, main_df, signal_mask, minute_ex):
    """用 pionex_dryrun.step_symbol()（+ pionex_backtest.resolve_with_5m，api_get 換成讀合成 1 分K）跑一遍。"""
    import pionex_backtest as pb
    import pionex_dryrun as dr
    dr.use_config(book)
    df = main_df.copy()
    df["signal"] = np.where(signal_mask, -1, 0)
    for col in ("atr_pct", "ret1h", "ma_dev", "obv_chg", "liq24", "vol_ratio", "close_pos", "htf_dev", "ret24",
                "btc_ret1h", "btc_ma_dev"):
        df[col] = np.nan

    def fake_api_get(path, params, retries=5):
        assert path == "/api/v1/market/klines" and params["interval"] == "1M", params
        return {"data": {"klines": minute_ex.rows(params["symbol"], "1M", params["endTime"], params["limit"])}}

    saved = pb.api_get
    pb.api_get = fake_api_get
    try:
        bk = {"symbols": {}, "closed": []}
        bar = pb.INTERVAL_MS[pb.CONFIG["INTERVAL"]]
        dr.step_symbol(bk, symbol, df, 1, bar, int(df["time"].iloc[0]))
    finally:
        pb.api_get = saved
    pos = bk["symbols"][symbol]["pos"]
    return bk["closed"], (None if pos is None else pos["entry_time"])


def _a3_trades(strategy, symbol, minutes, main_df, signal_mask, ex_series):
    spec = SPECS[strategy]
    with tempdir() as tmp:
        g = Harness(tmp)
        g.ex.series = ex_series
        for t, c in zip(main_df["time"][signal_mask], main_df["close"][signal_mask]):
            g.tick_until(int(t))                    # 訊號之前的 K 棒先判完（每根主週期 tick 一次也可以）
            g.signal(strategy, int(t), float(c), symbol=symbol)
        g.tick_until(int(main_df["time"].iloc[-1]) + spec.main_ms)
        rows = g.closed()
        for r in rows:
            r["features"] = json.loads(r["exit_features_json"])
        open_ = [p["opened_ms"] for p in g.all_positions() if p["status"] == "open"]
        g.close()
    return rows, (open_[0] if open_ else None)


_RESULT = {"止盈": config.EXIT_TAKE_PROFIT, "止損": config.EXIT_STOP_LOSS}


def _compare_with_dry_run(strategy, book, seed, n_minutes=3000, sig_prob=None):
    """回傳 (差異清單, 統計)。差異清單空 = 逐筆一致。"""
    spec = SPECS[strategy]
    rng = random.Random(seed)
    minutes = _random_minutes(rng, T0, n_minutes, 100.0)
    main_df = _aggregate(minutes, spec.main_ms)
    prob = sig_prob if sig_prob is not None else (0.12 if strategy == "s4" else 0.03)
    mask = np.array([rng.random() < prob for _ in range(len(main_df))])
    mask[-3:] = False
    ex = FakeExchange(Clock(0))
    for b in minutes:
        ex.put(SYM, "1M", *b)
    dry, dry_open = _dry_run_trades(book, SYM, main_df, mask, ex)
    a3, a3_open = _a3_trades(strategy, SYM, minutes, main_df, mask, ex.series)
    diffs, stats = [], {"trades": len(dry), "gap": 0, "same_bar": 0, "minor_order_note": 0, "earlier": 0}
    if len(dry) != len(a3):
        diffs.append("筆數不同：dry run %d、A3 %d" % (len(dry), len(a3)))
    for d, a in zip(dry, a3):
        where = "dry %s @ %s" % (d["result"], d["entry_time"])
        if d["entry_time"] != a["opened_ms"] or d["entry"] != a["entry_price"]:
            diffs.append("%s：進場不同 A3 %s @ %s" % (where, a["entry_price"], a["opened_ms"]))
            continue
        if (d["tp"], d["sl"]) != (a["take_profit_price"], a["stop_loss_price"]):
            diffs.append("%s：止盈止損價不同" % where)
        if _RESULT[d["result"]] != a["exit_reason"] or d["exit"] != a["exit_price"]:
            diffs.append("%s：結果 / 出場價不同 A3 %s @ %s（dry %s）" % (where, a["exit_reason"], a["exit_price"],
                                                                     d["exit"]))
        if nt.exit_bar_open(d["exit_time"], spec.main_ms) != a["features"]["exit_bar_open_ms"]:
            diffs.append("%s：出場所屬主週期 K 棒不同" % where)
        if not a["closed_ms"] <= d["exit_time"]:
            diffs.append("%s：A3 出場時刻晚於 dry run" % where)
        if a["closed_ms"] < d["exit_time"]:
            stats["earlier"] += 1
        note = d["note"] or ""
        gap = "開盤跳空穿越止損" in note
        same = ("仍同根" in note) or ("同1h觸發→保守計止損" in note)
        stats["gap"] += gap
        stats["same_bar"] += same
        if gap != a["features"]["gap_open"]:
            diffs.append("%s：跳空備註不同（dry %r / A3 %r）" % (where, note, a["features"]["gap_open"]))
        if same != a["features"]["same_bar_both"]:
            diffs.append("%s：同根兩碰備註不同（dry %r / A3 %r）" % (where, note, a["features"]["same_bar_both"]))
        if "判定先" in note:
            # 已知限制（見 live/notional_tracker 模組說明）：判定當下還不知道同一根 5 分K 之後會不會碰另一邊
            stats["minor_order_note"] += 1
            if a["features"]["minor_resolved"]:
                diffs.append("%s：minor_resolved 不該在判定當下為真" % where)
    if dry_open != a3_open:
        diffs.append("期末未平倉不同：dry %s、A3 %s" % (dry_open, a3_open))
    return diffs, stats


@_offline
def test_ac2_equivalence_with_dry_run_s4_random_paths():
    total = {"trades": 0, "gap": 0, "same_bar": 0, "minor_order_note": 0, "earlier": 0}
    for seed in range(12):
        diffs, stats = _compare_with_dry_run("s4", "s4", seed)
        assert not diffs, "seed %d：%s" % (seed, diffs[:5])
        for k in total:
            total[k] += stats[k]
    # 前提：隨機路徑真的涵蓋了要驗的情境（否則這條測試沒有鑑別力）
    assert total["trades"] >= 150, total
    assert total["gap"] >= 5 and total["same_bar"] >= 5 and total["minor_order_note"] >= 5, total
    assert total["earlier"] >= 50, total
    print("      s4 等價：%r" % total)


@_offline
def test_ac2_equivalence_with_dry_run_s5_random_paths():
    total = {"trades": 0, "gap": 0, "same_bar": 0, "minor_order_note": 0, "earlier": 0}
    for seed in range(6):
        diffs, stats = _compare_with_dry_run("s5", "s5", 100 + seed, n_minutes=2000)
        assert not diffs, "seed %d：%s" % (seed, diffs[:5])
        for k in total:
            total[k] += stats[k]
    assert total["trades"] >= 40 and total["gap"] >= 3 and total["same_bar"] >= 3, total
    assert total["earlier"] == 0, "s5 主週期就是 1 分K，出場時刻應該完全相同：%r" % total
    print("      s5 等價：%r" % total)


@_offline
def test_ac2_equivalence_test_rejects_gap_on_minor_open():
    """反向對照：跳空判定改用「觸價那根 1 分K 的開盤」的爛實作，同一條等價測試必須失敗。"""
    real = nt.scan_monitor

    def bad_scan(spec, tp, sl, bars, start_ms):
        outcome, checked = real(spec, tp, sl, bars, start_ms)
        if outcome is None or outcome.reason != config.EXIT_STOP_LOSS:
            return outcome, checked
        k = int(np.flatnonzero(bars.t == outcome.exit_ms - bars.step)[0])
        o = float(bars.o[k])
        gap = o > sl
        return nt.Outcome(outcome.reason, o if gap else sl, outcome.exit_ms, outcome.exit_bar_open_ms,
                          gap_open=gap, same_bar_both=outcome.same_bar_both,
                          minor_resolved=outcome.minor_resolved, judged_interval=outcome.judged_interval), checked

    nt.scan_monitor = bad_scan
    try:
        found = []
        for seed in range(12):
            diffs, _ = _compare_with_dry_run("s4", "s4", seed)
            found += diffs
    finally:
        nt.scan_monitor = real
    assert any("結果 / 出場價不同" in d or "跳空備註不同" in d for d in found), "爛實作沒有被抓到"


# ============================== AC-3：發布順序與重送 ==============================
@_offline
@with_harness
def test_ac3_publish_failure_is_not_marked_and_is_resent(h):
    p = 100.0
    h.rec.fail = "entry"
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + 3 * MAIN4, p))
    h.signal("s4", T0, p)
    sid = nt.make_signal_id(S4, SYM, T0 + MAIN4)
    assert h.db().get_signal(sid)["published_ms"] is None, "送達失敗卻標記了已發布"
    h.tick_until(T0 + MAIN4 + 2 * M1)
    assert h.db().get_signal(sid)["published_ms"] is None
    n_before = len(h.rec.raw)
    h.rec.fail = None
    h.tick_until(T0 + MAIN4 + 3 * M1)
    assert h.db().get_signal(sid)["published_ms"] is not None
    assert [k for k, s, _ in h.rec.raw].count("entry") == n_before + 1
    assert [(k, s) for k, s, _ in h.rec.deduped()] == [("entry", sid)], "去重後只有一筆"
    first = h.rec.raw[0][2]
    assert all(ev == first for k, s, ev in h.rec.raw), "重送的事件內容必須與第一次相同"


@_offline
@with_harness
def test_ac3_exit_never_overtakes_entry(h):
    p = 100.0
    tp, sl = _levels(S4, p)
    h.rec.fail = "entry"
    _one_trade(h, "s4", p, [(p, p, p, p), (p, p, tp, tp), (tp,) * 4])
    sid = nt.make_signal_id(S4, SYM, T0 + MAIN4)
    assert len(h.closed()) == 1
    assert [k for k, *_ in h.rec.raw if k == "exit"] == [], "進場還沒送達，出場就發出去了"
    assert h.db().get_position(sid)["exit_published_ms"] is None
    h.rec.fail = None
    h.tick_until(T0 + MAIN4 + 5 * M1)
    kinds = [k for k, s, _ in h.rec.deduped() if s == sid]
    assert kinds == ["entry", "exit"], kinds
    assert h.db().get_position(sid)["exit_published_ms"] is not None
    x = [ev for k, s, ev in h.rec.raw if k == "exit"][0]
    assert x.reason == config.EXIT_TAKE_PROFIT and x.direction == config.DIRECTION_SHORT
    assert x.opened_ms == T0 + MAIN4 and x.closed_ms == T0 + MAIN4 + 2 * M1
    assert set(x.features) == set(nt.EXIT_FEATURE_KEYS), sorted(x.features)


@_offline
@with_harness
def test_ac3_signal_handler_only_queues(h):
    """A1 的 on_result 回呼只排佇列：不寫庫、不發布（那是 A3 執行緒的事）。missed_close 記 WARNING。"""
    handler = h.tracker.bar_result_handler("s4")
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + 3 * MAIN4, 100.0))
    sig = types.SimpleNamespace(symbol=SYM, bar_open_ms=T0, bar_close_ms=T0 + MAIN4, signal_price=100.0,
                                ret2h=0.2, volr=math.inf, cpos=None, turn=1e5)
    handler(types.SimpleNamespace(bar_close_ms=T0 + MAIN4, degraded_reasons=[], signals=[sig]))
    miss = [types.SimpleNamespace(bar_close_ms=T0 + k * MAIN4, degraded_reasons=["missed_close"], signals=[])
            for k in (2, 3, 4)]
    for m in miss:
        handler(m)
    assert h.rec.raw == [] and h.all_positions() == []
    cap = _Capture()
    lg = logging.getLogger("live.notional_tracker")
    saved_level = lg.level
    lg.addHandler(cap)
    lg.setLevel(logging.DEBUG)
    try:
        h.clock.now = T0 + MAIN4 + WAIT
        h.tracker.process_pending()
        h.tick_until(T0 + 5 * MAIN4)
    finally:
        lg.removeHandler(cap)
        lg.setLevel(saved_level)
    assert [k for k, *_ in h.rec.raw] == ["entry"]
    ev = h.rec.raw[0][2]
    assert ev.features["volr"] == math.inf and ev.features["cpos"] is None
    warn = [r for r in cap.records if r.levelno == logging.WARNING and "missed_close" in r.getMessage()]
    assert len(warn) == 1 and "3 根" in warn[0].getMessage(), [r.getMessage() for r in cap.records]


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)


# ============================== AC-4：重啟復原 ==============================
def _scenario(h):
    """三個幣、兩個策略的固定劇本：回傳 (訊號清單 [(時刻, strategy, symbol, bar_open, price)], 結束時刻)。"""
    rng = random.Random(42)
    events = []
    for idx, sym in enumerate(("AAA_USDT_PERP", "BBB_USDT_PERP", "哈基米_USDT_PERP")):
        minutes = _random_minutes(rng, T0, 600, 10.0 + idx)
        for b in minutes:
            h.ex.put(sym, "1M", *b)
        closes = {b[0]: b[4] for b in minutes}
        for k in range(1 + idx, 110, 7):
            bo = T0 + k * MAIN4
            events.append((bo + MAIN4, "s4", sym, bo, closes[bo + MAIN4 - M1]))
        for k in range(3 + idx, 560, 41):
            bo = T0 + k * M1
            events.append((bo + M1, "s5", sym, bo, closes[bo]))
    events.sort()
    return events, T0 + 590 * M1


def _run_scenario(h, events, end, crash=None):
    """每分鐘 tick、訊號在收盤 + 定稿等待時送進來。crash = (時刻, 動作)：到那一刻執行動作（可能拋 SimulatedCrash），
    之後停機 downtime 再重啟。回傳是否真的當過。"""
    crashed = False
    i = 0
    t = T0
    down_until = None
    while t < end:
        t += M1
        h.clock.now = t + WAIT
        if down_until is not None:
            if t < down_until:
                continue
            h.restart(at=t + WAIT)
            down_until = None
        try:
            if crash is not None and not crashed and t >= crash[0]:
                crashed = True
                downtime = crash[1](h)
                if downtime:
                    down_until = t + downtime
                    continue
            h.tracker._safe(h.tracker.tick, t)      # 與 _loop() 相同：經 _safe（當機模擬的 BaseException 照樣穿透）
            while i < len(events) and events[i][0] <= t:
                _, strat, sym, bo, price = events[i]
                i += 1
                h.tracker.submit_signal(strategy=strat, symbol=sym, bar_open_ms=bo,
                                        bar_close_ms=bo + SPECS[strat].main_ms, signal_price=price,
                                        features={"f": 1.0})
                h.tracker.process_pending()
        except SimulatedCrash:
            down_until = t + M1
    if down_until is not None:
        h.restart(at=end + WAIT)
    h.tracker._safe(h.tracker.tick, end)
    return crashed


def _summary(h):
    rows = h.all_positions()
    return [(r["signal_id"], r["status"], r["exit_reason"], r["exit_price"], r["closed_ms"], r["opened_ms"],
             r["entry_price"]) for r in rows]


def _check_against_control(control, h, label):
    got = _summary(h)
    assert got == control["positions"], "%s：與一直沒停過的對照組不同\n%r\n%r" % (label, got, control["positions"])
    dd = [(k, s) for k, s, _ in h.rec.deduped()]
    assert sorted(dd) == sorted(control["events"]), "%s：去重後的事件與對照組不同" % label
    for sid in {s for _, s in dd}:
        kinds = [k for k, s in dd if s == sid]
        assert kinds in (["entry"], ["entry", "exit"]), (label, sid, kinds)
    for sid in {s for _, s in dd}:
        seq = [k for k, s, _ in h.rec.raw if s == sid]
        assert seq.index("entry") < (seq.index("exit") if "exit" in seq else len(seq)), (label, sid, seq)
    # 資料庫裡沒有未發布的事件（全部補發完）
    assert h.db().unpublished_signals() == [] and h.db().unpublished_exits() == [], label


def _control():
    with tempdir() as tmp:
        h = Harness(tmp)
        events, end = _scenario(h)
        _run_scenario(h, events, end)
        out = {"positions": _summary(h), "events": sorted((k, s) for k, s, _ in h.rec.deduped()),
               "events_list": events, "end": end}
        h.close()
    closed = [p for p in out["positions"] if p[1] == "closed"]
    assert len(closed) >= 8 and any(p[0].split("-")[1] == "S5" for p in closed), out["positions"]
    return out


_CONTROL = []


def _get_control():
    if not _CONTROL:
        _CONTROL.append(_control())
    return _CONTROL[0]


def _crash_run(label, crash_at_event, action):
    control = _get_control()
    with tempdir() as tmp:
        h = Harness(tmp)
        events, end = _scenario(h)
        try:
            crashed = _run_scenario(h, events, end, crash=(crash_at_event, action))
            assert crashed, "%s：當機點沒有被觸發" % label
            _check_against_control(control, h, label)
            dup = len(h.rec.raw) - len(h.rec.deduped())
        finally:
            h.close()
    return dup


@_offline
def test_ac4_crash_before_write():
    """寫庫前當機：資料庫與訂閱者都沒有那筆的痕跡；A1 在重啟後再交一次同一個訊號 → 與對照組相同。
    （實際上 A1 不會重交：那筆訊號就此遺失，但既沒有寫庫也沒有發過進場，不會出現「有進場沒出場」。）"""
    control = _get_control()
    first_sig = control["events_list"][0]

    def action(h):
        real = h.tracker.store.record_entry

        def crash_before(**kw):
            raise SimulatedCrash("寫庫前")
        h.tracker.store.record_entry = crash_before
        try:
            h.tracker.submit_signal(strategy=first_sig[1], symbol=first_sig[2], bar_open_ms=first_sig[3],
                                    bar_close_ms=first_sig[3] + SPECS[first_sig[1]].main_ms,
                                    signal_price=first_sig[4], features={"f": 1.0})
            h.tracker.process_pending()
        finally:
            h.tracker.store.record_entry = real
        return 0

    # 當機點 = 第一筆訊號送達的那一刻；之後重啟，劇本照常把同一筆訊號交進來（模擬重交）
    with tempdir() as tmp:
        h = Harness(tmp)
        events, end = _scenario(h)
        h.clock.now = first_sig[0] + WAIT
        try:
            action(h)
        except SimulatedCrash:
            pass
        assert h.all_positions() == [] and h.rec.raw == [], "寫庫前當機卻留下了痕跡"
        try:
            h.restart(at=first_sig[0] + WAIT)
            _run_scenario(h, events, end)
            _check_against_control(control, h, "寫庫前")
        finally:
            h.close()


@_offline
def test_ac4_crash_after_write_before_publish():
    control = _get_control()

    def action(h):
        h.rec.crash = ("entry", None)
        return 0
    first = control["events_list"][0][0]
    dup = _crash_run("寫庫後發布前", first - M1, action)
    assert dup >= 1, "前提：當機的那一筆進場應該被重送過一次"


@_offline
def test_ac4_crash_after_close_before_publish():
    control = _get_control()

    def action(h):
        h.rec.crash = ("exit", None)
        return 0
    dup = _crash_run("平倉後發布前", control["events_list"][0][0] - M1, action)
    assert dup >= 1


@_offline
def test_ac4_exit_touched_during_downtime():
    """停機 90 分鐘、期間有部位觸價：重啟後以歷史出場時刻與價格平倉（recovered），與「一直沒停過」相同。
    停機期間 A1 也沒在跑、不會有訊號，所以這條的對照組用「拿掉停機期間訊號」的同一份劇本另外跑。"""
    base = _get_control()
    # 挑一筆持倉夠久的部位：停機從它進場後開始、到它出場之後 30 分鐘才重啟（它的進場訊號在停機之前）
    target = min((p for p in base["positions"] if p[1] == "closed" and p[4] - p[5] >= 20 * M1),
                 key=lambda p: p[4])
    down_from, down_to = target[5] + 5 * M1, target[4] + 30 * M1
    keep = lambda evs: [e for e in evs if not down_from < e[0] <= down_to + WAIT]
    with tempdir() as tmp:
        h = Harness(tmp)
        try:
            events, end = _scenario(h)
            _run_scenario(h, keep(events), end)
            control = {"positions": _summary(h), "events": sorted((k, s) for k, s, _ in h.rec.deduped())}
        finally:
            h.close()

    def action(h):
        return down_to - down_from                # 停機，期間不 tick、不收訊號
    with tempdir() as tmp:
        h = Harness(tmp)
        try:
            events, end = _scenario(h)
            _run_scenario(h, keep(events), end, crash=(down_from, action))
            _check_against_control(control, h, "停機期間觸價")
            recovered = [r for r in h.closed() if json.loads(r["exit_features_json"])["recovered"]]
            assert recovered and all(down_from < r["closed_ms"] <= down_to + M1 for r in recovered), recovered
            assert all(r["closed_ms"] < down_to for r in recovered), "補判的出場時刻應該是停機期間的歷史時刻"
        finally:
            h.close()


# ============================== AC-7：範圍與約定 ==============================
def _strategy_values():
    vals = set()

    def add(v):
        if isinstance(v, bool) or v is None:
            return
        if isinstance(v, (int, float)):
            vals.add(float(v))
        elif isinstance(v, (tuple, list)):
            for x in v:
                add(x)
    for mod in (s4_signal, s5_signal):
        for d in (mod.DEFAULT_PARAMS, mod.exit_params()):
            for v in d.values():
                add(v)
    for spec in SPECS.values():
        add(spec.cooldown_bars)
    return vals


def _numeric_literals(source):
    out = []
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.NUMBER:
            out.append((tok.start[0], tok.string, float(eval(tok.string.replace("_", "")))))  # noqa: S307
    return out


# 0 / 1 是結構性的（計數起點、+1 根、布林索引），也剛好是 s5 的 COOLDOWN_HOURS / 冷卻根數；
# 1000 是毫秒換算。三者都不是策略參數的來源，明確放行。
_STRUCTURAL = {0.0, 1.0, 1000.0}


def _strategy_literal_hits(source):
    bad = _strategy_values() - _STRUCTURAL
    return [(line, text) for line, text, v in _numeric_literals(source) if v in bad]


def test_ac7_no_strategy_numbers_in_a3_code():
    for mod in ("notional_tracker.py", "a_channel.py"):
        src = open(os.path.join(REPO_ROOT, "live", mod), encoding="utf-8").read()
        hits = _strategy_literal_hits(src)
        assert not hits, "live/%s 有策略數值字面值：%r" % (mod, hits)
        others = [(l, t) for l, t, v in _numeric_literals(src) if v not in _STRUCTURAL]
        assert not others, "live/%s 有非結構性的數字（請放 config 或由參數推導）：%r" % (mod, others)


def test_ac7_literal_scan_discriminates():
    ex = s4_signal.exit_params()
    for sample in ("TP = %r\n" % ex["TAKE_PROFIT"], "x = p * (1 + %r)\n" % ex["STOP_LOSS"],
                   "COOL = %d\n" % S4.cooldown_bars):
        assert _strategy_literal_hits(sample), "掃描器漏抓：%r" % sample
    assert not _strategy_literal_hits("a = 0\nb = x + 1\nc = ms / 1000\n")


def test_ac7_specs_come_from_strategy_modules():
    assert set(nt._STRATEGY_SOURCES) == set(config.STRATEGIES)
    for strat, mod in (("s4", s4_signal), ("s5", s5_signal)):
        spec, ex = SPECS[strat], mod.exit_params()
        assert (spec.take_profit, spec.stop_loss) == (ex["TAKE_PROFIT"], ex["STOP_LOSS"])
        assert spec.cooldown_bars == mod.cooldown_bars(klines.bars_per_hour(spec.main_interval))
    assert S4.main_interval == config.KLINE_INTERVAL and S5.main_interval == config.S5_KLINE_INTERVAL
    assert S4.monitor_interval == s4_signal.exit_params()["RESOLVE_INTERVALS"][0]
    assert S5.monitor_interval == S5.main_interval
    # 參數改了 A3 跟著變（不是抄一份）
    saved = dict(s4_signal.EXIT_PARAMS)
    try:
        s4_signal.EXIT_PARAMS["TAKE_PROFIT"] = saved["TAKE_PROFIT"] / 2
        assert nt.build_spec("s4").take_profit == saved["TAKE_PROFIT"] / 2
    finally:
        s4_signal.EXIT_PARAMS.clear()
        s4_signal.EXIT_PARAMS.update(saved)


def test_ac7_unsupported_exit_params_refuse_to_start():
    saved = dict(s4_signal.EXIT_PARAMS)
    cases = [("MAX_HOLD_HOURS", saved["TAKE_PROFIT"] + 1, "MAX_HOLD_HOURS"), ("EXIT_MODE", "atr", "EXIT_MODE"),
             ("RESOLVE_INTERVALS", ["1M", "1M"], "RESOLVE_INTERVALS")]
    try:
        for key, value, frag in cases:
            s4_signal.EXIT_PARAMS.clear()
            s4_signal.EXIT_PARAMS.update(saved)
            s4_signal.EXIT_PARAMS[key] = value
            try:
                nt.build_specs()
            except nt.UnsupportedExitParams as e:
                assert frag in str(e), e
            else:
                raise AssertionError("%s = %r 應該拒絕啟動" % (key, value))
    finally:
        s4_signal.EXIT_PARAMS.clear()
        s4_signal.EXIT_PARAMS.update(saved)
    nt.build_specs()


def test_ac7_module_names_do_not_clash_and_imports_stay_in_live():
    for name in ("notional_tracker", "a_channel"):
        assert os.path.isfile(os.path.join(REPO_ROOT, "live", name + ".py"))
        assert name not in sys.stdlib_module_names
        assert importlib.util.find_spec(name) is None, name
        src = open(os.path.join(REPO_ROOT, "live", name + ".py"), encoding="utf-8").read()
        tops = set()
        for node in ast.walk(ast.parse(src)):
            if isinstance(node, ast.Import):
                tops |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom):
                tops.add((node.module or "").split(".")[0])
        assert not {t for t in tops if t.startswith("pionex_") or t == "research"}, (name, tops)
        assert tops <= set(sys.stdlib_module_names) | {"live", "strategy", "numpy", "pandas"}, (name, tops)


def test_ac7_execution_params_report_a3_params():
    p = config.execution_params()
    for k in ("S5_KLINE_INTERVAL", "A3_KLINES_PAGE_LIMIT", "A3_STORE_READY_TIMEOUT_SECONDS",
              "A3_JOIN_TIMEOUT_SECONDS", "A3_EVENTS_RECORD_DIR"):
        assert p[k] == getattr(config, k), k
    rd = os.path.abspath(config.A3_EVENTS_RECORD_DIR)
    assert os.path.commonpath([rd, paths.RUNTIME_DIR]) == os.path.abspath(paths.RUNTIME_DIR)


def test_ac7_python_m_live_exits_0():
    env = dict(os.environ)
    env.pop("PYTHONIOENCODING", None)
    env.pop("PYTHONUTF8", None)
    r = subprocess.run([sys.executable, "-m", "live"], cwd=REPO_ROOT, env=env, stdin=subprocess.DEVNULL,
                       capture_output=True, timeout=120)
    out = r.stdout.decode("utf-8", "replace")
    assert r.returncode == 0, (r.returncode, out[-1500:])
    assert "A3_KLINES_PAGE_LIMIT" in out and "S5_KLINE_INTERVAL" in out


# ============================== 執行緒與執行入口 ==============================
@_offline
def test_worker_thread_owns_the_store_and_ticks_on_schedule():
    """工作執行緒開資料庫、在自己的執行緒裡用；迴圈每根 1 分K 收盤 + 定稿等待 tick 一次（假時鐘推進，不真的等）。"""
    with tempdir() as tmp:
        clock = Clock(T0 + WAIT)
        ex = FakeExchange(clock)
        ex.put_bars(SYM, "1M", flat_bars(T0 - MAIN4, T0 + 30 * M1, 100.0))
        rec = Recorder()
        opened_in = []

        def factory():
            opened_in.append(threading.current_thread().name)
            return store.open_store(os.path.join(tmp, "d.sqlite3"), clock=clock)

        tr = nt.NotionalTracker(bus=make_bus(rec), fetcher=ex.fetch, now_ms=clock, specs=SPECS,
                                store_factory=factory)
        ticks = []
        real_tick = tr.tick

        def tick(target):
            ticks.append(target)
            real_tick(target)
        tr.tick = tick

        def fake_wait(timeout):
            try:
                return tr._queue.get_nowait()
            except Exception:  # noqa: BLE001
                pass
            clock.now += int(round(timeout * 1000)) or 1
            if clock.now > T0 + 20 * M1:
                tr.stop()
            return None
        tr._wait_job = fake_wait
        tr.submit_signal(strategy="s4", symbol=SYM, bar_open_ms=T0 - MAIN4, bar_close_ms=T0, signal_price=100.0,
                         features={"f": 1.0})
        tr.start()
        assert tr.wait_ready(5) and opened_in == ["a3-tracker"], opened_in
        assert tr.join(10), "工作執行緒沒有結束"
        # 復原時已經判定到 T0（啟動當下的最後一根），迴圈從下一根起每根 1 分K 一次，不重複、不跳
        assert ticks[:3] == [T0 + M1, T0 + 2 * M1, T0 + 3 * M1], ticks[:5]
        assert ticks == list(range(ticks[0], ticks[-1] + M1, M1)), ticks
        assert [k for k, *_ in rec.raw] == ["entry"]


@_offline
def test_worker_does_not_spin_when_the_database_cannot_be_opened_later():
    """資料庫中途開不了（重開失敗）：迴圈照每根 1 分K 試一次，不原地空轉；開得了之後復原、延後的訊號照常處理。"""
    with tempdir() as tmp:
        clock = Clock(T0 + WAIT)
        ex = FakeExchange(clock)
        ex.put_bars(SYM, "1M", flat_bars(T0 - MAIN4, T0 + 40 * M1, 100.0))
        rec = Recorder()
        path = os.path.join(tmp, "d.sqlite3")
        state = {"fail": False, "opens": 0}

        def factory():
            state["opens"] += 1
            if state["fail"]:
                raise sqlite3.OperationalError("unable to open database file（測試注入）")
            return store.open_store(path, clock=clock)

        tr = nt.NotionalTracker(bus=make_bus(rec), fetcher=ex.fetch, now_ms=clock, specs=SPECS,
                                store_factory=factory)
        tr.open()
        tr.recover()
        tr.store.close()
        tr.store = None                  # 模擬中毒後重開失敗：store 不見了
        tr._recovered = False
        state["fail"] = True
        ticks = []
        real_tick = tr.tick

        def tick(target):
            ticks.append(target)
            if target >= T0 + 10 * M1:
                state["fail"] = False      # 十分鐘後資料庫恢復
            real_tick(target)
        tr.tick = tick

        def fake_wait(timeout):
            try:
                return tr._queue.get_nowait()
            except Exception:  # noqa: BLE001
                pass
            clock.now += int(round(timeout * 1000)) or 1
            if clock.now > T0 + 20 * M1:
                tr.stop()
            return None
        tr._wait_job = fake_wait
        tr.submit_signal(strategy="s4", symbol=SYM, bar_open_ms=T0 - MAIN4, bar_close_ms=T0, signal_price=100.0,
                         features={"f": 1.0})
        lg = logging.getLogger("live.notional_tracker")
        saved = lg.level
        lg.setLevel(logging.CRITICAL + 1)
        seen = {}

        def worker():                       # Store 只能在開它的執行緒關，所以收尾也在這條執行緒
            try:
                tr._loop()
            finally:
                seen["store_open"] = tr.store is not None
                tr.close()
        try:
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            t.join(10)
        finally:
            lg.setLevel(saved)
        assert not t.is_alive(), "迴圈沒有結束"
        assert len(ticks) <= 22 and ticks == sorted(set(ticks)), "迴圈原地空轉了：%d 次 tick" % len(ticks)
        assert seen["store_open"] and [k for k, *_ in rec.raw] == ["entry"], (state, rec.raw)


@_offline
@with_harness
def test_poisoned_store_is_reopened_and_the_job_is_retried(h):
    """store 早就中毒（例如上一件事的 ROLLBACK 失敗）：這個訊號延後，下一次 tick 重開、從資料庫重建後進場。"""
    p = 100.0
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + 3 * MAIN4, p))
    h.tracker.store._poison(sqlite3.OperationalError("disk I/O error（測試注入）"), RuntimeError("原本的錯誤"))
    with _quiet():
        h.signal("s4", T0, p)
        assert h.tracker.deferred() and h.tracker.open_positions() == {}
        _safe_ticks(h, T0 + MAIN4 + M1, T0 + MAIN4 + M1)
    assert not h.tracker.store.poisoned
    assert h.tracker.open_positions() == {("s4", SYM): nt.make_signal_id(S4, SYM, T0 + MAIN4)}, "訊號在重開時丟了"
    assert [k for k, *_ in h.rec.raw] == ["entry"] and h.tracker.deferred() == []


# ============================== 修正輪 1：寫庫失敗（BUG-007 / BUG-008）與 N-1～N-3 ==============================
@contextlib.contextmanager
def _quiet():
    """寫庫失敗的測試會刻意產生 ERROR / WARNING；測試輸出只看結果。"""
    lg = logging.getLogger("live.notional_tracker")
    saved = lg.level
    lg.setLevel(logging.CRITICAL + 1)
    try:
        yield
    finally:
        lg.setLevel(saved)


def _safe_ticks(h, first, last):
    """照 NotionalTracker._loop() 的方式 tick（經 _safe）：first、first+1m … last。"""
    t = first
    while t <= last:
        h.clock.now = max(h.clock.now, t + WAIT)
        h.tracker._safe(h.tracker.tick, t)
        t += M1


class _FailOnceConn:
    """包住 store 的真實連線：第 nth 次執行含 trigger 的 SQL 時失敗一次（OperationalError）。
    rollback_fails=True 時之後的 ROLLBACK 也失敗（store 中毒）；False 時 ROLLBACK 照常成功（不中毒）。"""

    def __init__(self, conn, trigger, nth=1, rollback_fails=False):
        self._c, self.trigger, self.nth, self.rollback_fails = conn, trigger, nth, rollback_fails
        self.seen = 0
        self.fired = False

    def execute(self, sql, *args):
        if self.fired and self.rollback_fails and sql.strip().upper() == "ROLLBACK":
            raise sqlite3.OperationalError("disk I/O error（測試注入：ROLLBACK）")
        if not self.fired and self.trigger in sql:
            self.seen += 1
            if self.seen == self.nth:
                self.fired = True
                raise sqlite3.OperationalError("disk I/O error（測試注入：%s）" % self.trigger)
        return self._c.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._c, name)


def _dqa_bars(h, p):
    """DQA 重現用的路徑：進場後第 +1 根 1 分K 只碰止盈；第 +5 根碰止損（止盈判錯時才會走到這裡）。"""
    tp, sl = _levels(S4, p)
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + MAIN4, p))
    seq = [(p, p, p, p), (p, p, tp, tp * 1.001)] + [(tp * 1.001,) * 4] * 3 \
        + [(tp * 1.001, sl * 1.01, tp * 1.001, sl)] + [(sl,) * 4] * 10
    for i, b in enumerate(seq):
        h.ex.put(SYM, "1M", T0 + MAIN4 + i * M1, *b)
    return tp, sl


def _trades(h):
    return [(r["signal_id"], r["status"], r["exit_reason"], r["exit_price"], r["closed_ms"], r["opened_ms"],
             r["entry_price"]) for r in h.all_positions()]


def _deduped(h):
    return sorted((k, s) for k, s, _ in h.rec.deduped())


def _dqa_run(inject):
    """跑 DQA 的單筆劇本；inject(h, phase) 在 phase = "before_signal" / "after_signal" / "before_tp_tick" /
    "after_tp_tick" 被呼叫（回傳要在該階段之後關掉的東西）。回傳 (trades, 去重後事件, 出場事件, store_failures)。"""
    with tempdir() as tmp:
        h = Harness(tmp)
        try:
            h.tracker.store._conn.execute("PRAGMA busy_timeout = 20")    # 被鎖時等 20 毫秒就放棄，不要真的等 5 秒
            _dqa_bars(h, 100.0)
            with _quiet():
                undo = inject(h, "before_signal")
                h.signal("s4", T0, 100.0)
                if undo:
                    undo()
                _safe_ticks(h, T0 + MAIN4 + M1, T0 + MAIN4 + M1)
                undo = inject(h, "before_tp_tick")
                _safe_ticks(h, T0 + MAIN4 + 2 * M1, T0 + MAIN4 + 2 * M1)
                if undo:
                    undo()
                _safe_ticks(h, T0 + MAIN4 + 3 * M1, T0 + MAIN4 + 12 * M1)
            exits = [(ev.reason, ev.exit_price, ev.closed_ms) for k, s, ev in h.rec.deduped() if k == "exit"]
            return _trades(h), _deduped(h), exits, h.tracker.stats.get("store_failures", 0)
        finally:
            h.close()


def _write_lock(h):
    """第二條連線拿住寫入鎖（真實的 SQLite 鎖，不 monkeypatch store）。回傳解鎖函式。"""
    locker = sqlite3.connect(h.db_path, timeout=0, isolation_level=None)
    locker.execute("BEGIN IMMEDIATE")

    def release():
        locker.execute("ROLLBACK")
        locker.close()
    return release


_DQA_CONTROL = []


def _dqa_control():
    if not _DQA_CONTROL:
        _DQA_CONTROL.append(_dqa_run(lambda h, phase: None))
        trades, _, exits, _ = _DQA_CONTROL[0]
        tp, _ = _levels(S4, 100.0)
        assert exits == [(config.EXIT_TAKE_PROFIT, tp, T0 + MAIN4 + 2 * M1)], exits    # 前提：對照組是止盈
    return _DQA_CONTROL[0]


@_offline
def test_fix1_close_position_lock_without_poison_matches_control():
    """BUG-007：平倉寫庫碰到真實的寫入鎖（BEGIN IMMEDIATE 逾時、不中毒）→ 已判定的止盈不可以被跳過；
    結果與不失敗的對照組逐筆相同（不會變成之後的止損）。"""
    control = _dqa_control()
    got = _dqa_run(lambda h, phase: _write_lock(h) if phase == "before_tp_tick" else None)
    assert got[0] == control[0], "與對照組不同：\n%r\n%r" % (got[0], control[0])
    assert got[1] == control[1] and got[2] == control[2], (got[1:3], control[1:3])
    assert got[3] >= 1, "前提：這次平倉寫庫真的失敗過"


@_offline
def test_fix1_close_position_update_fails_rollback_ok_matches_control():
    """同上，改用包裝連線：UPDATE positions 失敗、ROLLBACK 成功（不中毒）。"""
    control = _dqa_control()

    def inject(h, phase):
        if phase == "before_tp_tick":
            h.tracker.store._conn = _FailOnceConn(h.tracker.store._conn, "UPDATE positions SET status")
    got = _dqa_run(inject)
    assert got[0] == control[0], "與對照組不同：\n%r\n%r" % (got[0], control[0])
    assert got[1] == control[1] and got[2] == control[2]
    assert got[3] >= 1


@_offline
def test_fix1_record_entry_lock_without_poison_is_retried_and_entered():
    """BUG-008：進場寫庫碰到寫入鎖（不中毒）→ 那筆訊號延後、重試、進場，結果與對照組相同。"""
    control = _dqa_control()
    got = _dqa_run(lambda h, phase: _write_lock(h) if phase == "before_signal" else None)
    assert got[0] == control[0], "訊號沒有被重試進場：\n%r\n%r" % (got[0], control[0])
    assert got[1] == control[1] and got[2] == control[2]
    assert got[3] >= 1


@_offline
def test_fix1_poisoning_during_the_entry_write_still_enters_that_signal():
    """BUG-008：「中毒發生在這一件工作進行中」（INSERT positions 失敗、ROLLBACK 也失敗）→ 就是這一筆訊號
    也要進場（下一次 tick 重開、重建、重試），結果與對照組相同。"""
    control = _dqa_control()

    def inject(h, phase):
        if phase == "before_signal":
            h.tracker.store._conn = _FailOnceConn(h.tracker.store._conn, "INSERT INTO positions",
                                                  rollback_fails=True)
    got = _dqa_run(inject)
    assert got[0] == control[0], "中毒當下那筆訊號遺失了：\n%r\n%r" % (got[0], control[0])
    assert got[1] == control[1] and got[2] == control[2]
    assert got[3] >= 1


@_offline
def test_fix1_every_store_write_point_failure_matches_control():
    """整類：AC-4 的多幣、多策略劇本裡，在每一個寫入點（record_entry 的兩個 INSERT、mark_published、
    close_position、mark_exit_published）各注入一次失敗，中毒與不中毒都試，第 1 次與第 2 次都試 ——
    結果與一直沒出錯的對照組逐筆相同，去重後事件相同、每個 signal_id 都是先進場後出場、最後沒有未發布的事件。"""
    control = _get_control()
    triggers = ("INSERT INTO signals", "INSERT INTO positions", "UPDATE signals SET published_ms",
                "UPDATE positions SET status", "UPDATE positions SET exit_published_ms")
    ran = 0
    for trigger in triggers:
        for rollback_fails in (False, True):
            for nth in (1, 2):
                label = "%s 第 %d 次%s" % (trigger, nth, "（中毒）" if rollback_fails else "")
                with tempdir() as tmp:
                    h = Harness(tmp)
                    try:
                        events, end = _scenario(h)
                        conn = _FailOnceConn(h.tracker.store._conn, trigger, nth, rollback_fails)
                        h.tracker.store._conn = conn
                        with _quiet():
                            _run_scenario(h, events, end)
                        _check_against_control(control, h, label)
                        assert conn.fired, "%s：前提不成立，注入點沒有被觸發" % label
                        assert h.tracker.stats.get("store_failures", 0) >= 1, label
                        ran += 1
                    finally:
                        h.close()
    assert ran == len(triggers) * 4


@_offline
def test_fix1_one_failing_position_does_not_block_others_deferred_or_flush():
    """N-1：某個部位判定時拋出未預期的例外（例如某幣回應格式異常）→ 其他部位照常判定、延後的訊號照常重試、
    未發布的事件照常補發。"""
    with tempdir() as tmp:
        h = Harness(tmp)
        try:
            bad, good, held = "AAA_USDT_PERP", "BBB_USDT_PERP", "CCC_USDT_PERP"
            p = 100.0
            tp4, _ = _levels(S4, p)
            for sym in (bad, good):
                h.ex.put_bars(sym, "1M", flat_bars(T0, T0 + MAIN4 + 10 * M1, p))
            h.ex.put(good, "1M", T0 + MAIN4 + 2 * M1, p, p, tp4, tp4)          # good 在 +2 根止盈
            h.ex.put_bars(held, "1M", flat_bars(T0 + MAIN4, T0 + MAIN4 + 10 * M1, p))
            h.signal("s4", T0, p, symbol=bad)
            h.signal("s4", T0, p, symbol=good)
            h.signal("s5", T0 + MAIN4, p, symbol=held)                        # held：s5 持倉
            state = {"bad": False, "held_fail": 0}
            real = h.ex.fetch

            def fetch(symbol, interval, end_close_ms, limit):
                if symbol == bad and state["bad"]:
                    return 5                     # 不是 list：Bars.from_rows 會拋 TypeError（不是 _FetchFailed）
                if symbol == held and state["held_fail"]:
                    state["held_fail"] -= 1
                    raise ApiError("暫時錯誤")
                return real(symbol, interval, end_close_ms, limit)
            h.tracker.fetcher = fetch
            # held 的持倉補判取數失敗 → 新的 s5 訊號延後
            state["held_fail"] = 1
            with _quiet():
                h.signal("s5", T0 + MAIN4 + 2 * M1, p, symbol=held)
            assert len(h.tracker.deferred()) == 1, "前提：要有一筆延後的訊號"
            state["bad"] = True
            with _quiet():
                _safe_ticks(h, T0 + MAIN4 + 3 * M1, T0 + MAIN4 + 3 * M1)
            closed = {r["symbol"]: r for r in h.closed()}
            assert good in closed and closed[good]["exit_reason"] == config.EXIT_TAKE_PROFIT, \
                "排在出錯部位後面的部位沒有被判定"
            assert h.tracker.deferred() == [], "延後的訊號沒有被重試"
            assert [ev.signal_id for k, s, ev in h.rec.raw if k == "exit"] == [closed[good]["signal_id"]], \
                "tick 最後的補發沒有做"
            assert (("s4", bad) in h.tracker.open_positions()), "出錯的部位應該還在，下一次 tick 再判"
        finally:
            h.close()


@_offline
def test_fix1_stop_drains_queued_signals():
    """N-2：stop() 時佇列裡還有好幾筆訊號（例如 --duration 到了、A1 最後一根的訊號剛排進來）→ 一筆都不可以丟。"""
    with tempdir() as tmp:
        clock = Clock(T0 + MAIN4 + WAIT)
        ex = FakeExchange(clock)
        syms = ("AAA_USDT_PERP", "BBB_USDT_PERP", "CCC_USDT_PERP", "DDD_USDT_PERP")
        for sym in syms:
            ex.put_bars(sym, "1M", flat_bars(T0, T0 + MAIN4 + 5 * M1, 100.0))
        rec = Recorder()
        tr = nt.NotionalTracker(bus=make_bus(rec), fetcher=ex.fetch, now_ms=clock, specs=SPECS,
                                store_factory=lambda: store.open_store(os.path.join(tmp, "d.sqlite3"), clock=clock))
        state = {"sent": False}

        def fake_wait(timeout):
            if not state["sent"]:
                state["sent"] = True
                for sym in syms:                 # 同一根 K 棒的四筆訊號排進佇列，接著立刻要求停止
                    tr.submit_signal(strategy="s4", symbol=sym, bar_open_ms=T0, bar_close_ms=T0 + MAIN4,
                                     signal_price=100.0, features={"f": 1.0})
                tr.stop()
            try:
                return tr._queue.get_nowait()
            except queue.Empty:
                clock.now += int(round(timeout * 1000)) or 1
                return None
        tr._wait_job = fake_wait
        tr.start()
        assert tr.wait_ready(5)
        assert tr.join(10), "工作執行緒沒有結束"
        entered = sorted(s for k, s, _ in rec.raw if k == "entry")
        assert entered == sorted(nt.make_signal_id(S4, sym, T0 + MAIN4) for sym in syms), \
            "停止時佇列裡的訊號被丟掉了：%r" % entered


@_offline
@with_harness
def test_fix1_fallback_does_not_skip_an_incomplete_main_bar(h):
    """N-3：重啟補判時，監控週期拿得到的起點落在「最後一根還沒收完的主週期 K 棒」裡 → 那一根留到它收完再用
    主週期補判，不可以把裡面已有的 1 分K 一起跳過（跳過的話止盈會變成之後的止損）。"""
    p = 100.0
    tp, sl = _levels(S4, p)
    h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + MAIN4, p))
    h.signal("s4", T0, p)
    bars = [(p, p, p, p), (p, p, p, p), (p, p, tp, tp * 1.001), (tp * 1.001,) * 4, (tp * 1.001,) * 4,
            (tp * 1.001, sl * 1.01, tp * 1.001, sl)] + [(sl,) * 4] * 10
    for i, b in enumerate(bars):
        h.ex.put(SYM, "1M", T0 + MAIN4 + i * M1, *b)
    h.ex.put(SYM, "5M", T0 + MAIN4, p, p, tp, tp * 1.001)
    h.ex.put(SYM, "5M", T0 + 2 * MAIN4, tp * 1.001, sl * 1.01, tp * 1.001, sl)
    restart = T0 + MAIN4 + 3 * M1
    h.ex.retention["1M"] = restart + WAIT - (T0 + MAIN4 + 2 * M1)      # 重啟當下 1 分K 只從 +2 根開始有
    with _quiet():
        h.restart(at=restart + WAIT)
        _safe_ticks(h, restart + M1, T0 + 3 * MAIN4)
    r = _only(h.closed())
    assert (r["exit_reason"], r["exit_price"]) == (config.EXIT_TAKE_PROFIT, tp), r
    assert r["closed_ms"] == T0 + 2 * MAIN4 and r["features"]["judged_interval"] == S4.main_interval, r


@_offline
def test_fix1_v1_closed_positions_are_republished_with_one_warning():
    """DQA §4-2（選做）：v1 資料庫升級後沒有出場發布紀錄的已平倉部位，A3 第一次啟動補發，並記一行 WARNING 列出筆數。"""
    with tempdir() as tmp:
        v1 = _v1_store_module(tmp)
        path = os.path.join(tmp, "live.sqlite3")
        s = v1.open_store(path, clock=Clock(T0 + 7))
        s.record_entry(**_entry_kw("s4-a"))
        s.mark_published("s4-a", published_ms=T0 + 8)
        s.close_position("s4-a", exit_reason="take_profit", exit_price=2.4, closed_ms=T0 + 3 * MAIN4)
        s.close()
        clock = Clock(T0 + 10 * MAIN4)
        rec = Recorder()
        tr = nt.NotionalTracker(bus=make_bus(rec), fetcher=FakeExchange(clock).fetch, now_ms=clock, specs=SPECS,
                                store_factory=lambda: store.open_store(path, clock=clock))
        cap = _Capture()
        lg = logging.getLogger("live.notional_tracker")
        saved = lg.level
        lg.addHandler(cap)
        lg.setLevel(logging.DEBUG)
        try:
            tr.open()
            tr.recover()
        finally:
            lg.removeHandler(cap)
            lg.setLevel(saved)
            tr.close()
        warn = [r.getMessage() for r in cap.records if r.levelno == logging.WARNING and "schema v1" in r.getMessage()]
        assert len(warn) == 1 and "1 筆" in warn[0] and "s4-a" in warn[0], warn
        assert [(k, sid) for k, sid, _ in rec.raw] == [("exit", "s4-a")]


@_offline
def test_a_channel_wiring_self_check_and_events_jsonl():
    from live import a_channel
    with tempdir() as tmp:
        clock = Clock(T0 + MAIN4 + WAIT)
        ex = FakeExchange(clock)
        ex.put_bars(SYM, "1M", flat_bars(T0, T0 + 3 * MAIN4, 100.0))
        sig = types.SimpleNamespace(symbol=SYM, bar_open_ms=T0, bar_close_ms=T0 + MAIN4, signal_price=100.0,
                                    ret2h=0.2, volr=3.0, cpos=0.4, turn=1e5)

        class FakeFeed:
            def __init__(self, on_result, **kw):
                self.on_result = on_result

            def run(self, duration_s=None):
                self.on_result(types.SimpleNamespace(bar_close_ms=T0 + MAIN4, degraded_reasons=[],
                                                     signals=[sig]))
                for _ in range(200):          # 讓 A3 執行緒處理完（不是 sleep 固定秒數：等條件成立）
                    if tracker_box and tracker_box[0].stats["entries"]:
                        break
                    threading.Event().wait(0.02)

            def close(self):
                pass

        class FakeS5:                         # A5（TASK-116）的替身：FakeFeed 沒有 gate / buffer，真的 A5 建不起來
            def start(self):
                pass

            def close(self, timeout=None):
                return True

            def stats(self):
                return {"fake": True}

        tracker_box = []

        def tracker_factory(bus):
            tr = nt.NotionalTracker(bus=bus, fetcher=ex.fetch, now_ms=clock, specs=SPECS,
                                    store_factory=lambda: store.open_store(os.path.join(tmp, "d.sqlite3"),
                                                                           clock=clock))
            tracker_box.append(tr)
            return tr

        jdir = os.path.join(tmp, "events")
        cap = _Capture()
        logging.getLogger("live.a_channel").addHandler(cap)
        logging.getLogger("live.a_channel").setLevel(logging.INFO)
        try:
            rc = a_channel.main(["--duration", "5", "--events-jsonl", jdir], setup_logging=False,
                                tracker_factory=tracker_factory,
                                feed_factory=lambda on_result: FakeFeed(on_result),
                                s5_feed_factory=lambda feed, tracker: FakeS5())
        finally:
            logging.getLogger("live.a_channel").removeHandler(cap)
        assert rc == 0
        msgs = [r.getMessage() for r in cap.records]
        wiring = [m for m in msgs if "接線自檢" in m]
        assert wiring and "EntryEvent 1" in wiring[0] and "ExitEvent 1" in wiring[0], msgs
        files = os.listdir(jdir)
        assert len(files) == 1
        lines = [json.loads(x) for x in open(os.path.join(jdir, files[0]), encoding="utf-8")]
        assert [x["event"] for x in lines] == ["EntryEvent"] and lines[0]["signal_id"].startswith("#TEST-S4-")
        # --events-jsonl 不可以寫進 state/ 或 output/
        for bad in ("state", "output"):
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    a_channel.main(["--events-jsonl", os.path.join(REPO_ROOT, bad, "x")], setup_logging=False)
            except SystemExit as e:
                assert e.code == 2
            else:
                raise AssertionError("--events-jsonl 寫進 %s/ 沒有被擋" % bad)


# ============================== A4（TASK-118）FR-10：資料庫失敗退避 ==============================
@contextlib.contextmanager
def _a3_records():
    """收 live.notional_tracker 的 INFO 以上（runner 把 live 調到 CRITICAL；這裡暫時打開，結束時還原）。"""
    lg = logging.getLogger("live.notional_tracker")
    saved = lg.level
    cap = _Capture()
    lg.setLevel(logging.INFO)
    lg.addHandler(cap)
    try:
        yield cap
    finally:
        lg.removeHandler(cap)
        lg.setLevel(saved)


def test_fr10_store_failure_backoff_schedule_skips_and_reset():
    """AC-9：資料庫連續失敗時，重建嘗試的時刻依序是上一次失敗 +60、+120、+300、+600、+900、+900……秒；
    退避中的 tick 不開庫、不取數、不重試延後的訊號（資料庫其實已經好了也一樣），只累計 store_backoff_skips；
    重建成功一次就歸零（之後再失敗又從 +60 起算）。每次失敗照舊記 ERROR，另外記一行 INFO 寫明第幾次、幾秒後重建。"""
    sym2 = "TEST2_USDT_PERP"
    with tempdir() as tmp, _a3_records() as cap:
        clock = Clock(T0 + WAIT)
        ex = FakeExchange(clock)
        for sym in (SYM, sym2):
            ex.put_bars(sym, "1M", flat_bars(T0 - MAIN4, T0 + 200 * M1, 100.0))
        rec = Recorder()
        path = os.path.join(tmp, "d.sqlite3")
        state = {"fail_left": 0}
        opens = []

        def factory():
            opens.append(clock.now)
            if state["fail_left"] > 0:
                state["fail_left"] -= 1
                raise sqlite3.OperationalError("unable to open database file（測試注入）")
            return store.open_store(path, clock=clock)

        tr = nt.NotionalTracker(bus=make_bus(rec), fetcher=ex.fetch, now_ms=clock, specs=SPECS,
                                store_factory=factory)

        def tick(minute):
            t = T0 + minute * M1
            clock.now = t + WAIT
            tr.tick(t)

        def submit(sym):
            tr.submit_signal(strategy="s4", symbol=sym, bar_open_ms=T0 - MAIN4, bar_close_ms=T0, signal_price=100.0,
                             features={"f": 1.0})
            tr.process_pending()

        try:
            # 正常：SYM 進場、持倉中（平盤不會出場），正常的 tick 每次都取數
            tr.open()
            tr.recover()
            submit(SYM)
            tick(1)
            assert [k for k, *_ in rec.raw] == ["entry"] and ex.calls, (rec.raw, ex.calls)
            assert tr.stats["store_backoff_skips"] == 0 and tr._store_fail_streak == 0
            # 連線中毒、接下來 8 次開庫都失敗：第 2 分鐘的 tick 是第 1 次失敗
            tr.store._poison(sqlite3.OperationalError("disk I/O error（測試注入）"), RuntimeError("原本的錯誤"))
            state["fail_left"] = 8
            del opens[:]
            tick(2)
            submit(sym2)                    # 作廢期間收到的訊號照舊延後
            assert len(tr.deferred()) == 1
            attempts = [2]
            for minute in range(3, 81):
                before = (len(opens), len(ex.calls), len(rec.raw), len(tr.deferred()))
                skips = tr.stats["store_backoff_skips"]
                tick(minute)
                if len(opens) == before[0]:
                    # 退避中：不開庫、不取數、不重試延後的訊號（第 65 分鐘之後資料庫其實已經好了）
                    assert (len(ex.calls), len(rec.raw), len(tr.deferred())) == before[1:], minute
                    assert tr.stats["store_backoff_skips"] == skips + 1, minute
                else:
                    assert len(opens) == before[0] + 1 and tr.stats["store_backoff_skips"] == skips, minute
                    attempts.append(minute)
            assert attempts == [2, 3, 5, 10, 20, 35, 50, 65, 80], attempts
            assert [(b - a) // 1000 for a, b in zip(opens, opens[1:])] == [60, 120, 300, 600, 900, 900, 900, 900]
            # 第 80 分鐘重建成功：延後的訊號進場、持倉照常取數，退避歸零
            assert [(k, sid) for k, sid, _ in rec.raw] == [("entry", nt.make_signal_id(S4, SYM, T0)),
                                                           ("entry", nt.make_signal_id(S4, sym2, T0))], rec.raw
            assert tr.deferred() == [] and tr._store_fail_streak == 0 and tr._store_retry_at_ms is None
            assert tr.stats["store_backoff_skips"] == 70 and tr.stats["store_failures"] == 8, tr.stats
            # 歸零之後再失敗一次：又從 +60 秒起算
            tr.store._poison(sqlite3.OperationalError("disk I/O error（測試注入）"), RuntimeError("原本的錯誤"))
            state["fail_left"] = 1
            del opens[:]
            tick(81)
            calls = len(ex.calls)
            tick(82)
            assert [(b - a) // 1000 for a, b in zip(opens, opens[1:])] == [60] and len(ex.calls) > calls
            assert tr._store_fail_streak == 0 and tr.stats["store_backoff_skips"] == 70
        finally:
            tr.close()
    infos = [r.getMessage() for r in cap.records if r.levelno == logging.INFO and "連續失敗" in r.getMessage()]
    assert infos == ["A3 資料庫第 %d 次連續失敗，下次重建在 %d 秒後" % kd for kd in
                     ((1, 60), (2, 120), (3, 300), (4, 600), (5, 900), (6, 900), (7, 900), (8, 900), (1, 60))], infos
    errors = [r for r in cap.records if r.levelno == logging.ERROR and "資料庫操作失敗" in r.getMessage()]
    assert len(errors) == 9, [r.getMessage() for r in errors]


def test_fr10_backoff_table_lives_in_config_and_healthy_runs_never_skip():
    """FR-10：退避表只在 config（notional_tracker.py 只用索引取值）；資料庫正常時不會有任何 tick 被跳過。"""
    assert config.A3_STORE_RETRY_DELAYS_SECONDS == (60, 120, 300, 600, 900)
    assert config.execution_params()["A3_STORE_RETRY_DELAYS_SECONDS"] == (60, 120, 300, 600, 900)
    with open(os.path.join(REPO_ROOT, "live", "notional_tracker.py"), encoding="utf-8") as f:
        src = f.read()
    assert "A3_STORE_RETRY_DELAYS_SECONDS" in src and not {60.0, 120.0, 300.0, 600.0, 900.0} & \
        {v for _, _, v in _numeric_literals(src)}
    with tempdir() as tmp:
        h = Harness(tmp)
        try:
            p = 100.0
            h.ex.put_bars(SYM, "1M", flat_bars(T0, T0 + 3 * MAIN4, p))
            h.signal("s4", T0, p)
            h.tick_until(T0 + 2 * MAIN4)
            assert h.tracker.stats["store_backoff_skips"] == 0 and h.tracker.stats["store_failures"] == 0
            assert h.tracker._store_retry_at_ms is None
        finally:
            h.close()


# ============================== runner ==============================
if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    logging.getLogger("live").setLevel(logging.CRITICAL)      # A3 的 INFO / WARNING 很多，測試輸出只看結果
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
    runner_failed = 0
    if NET_ATTEMPTS:
        runner_failed += 1
        print(f"FAIL  <runner>: 測試期間有連網企圖 {NET_ATTEMPTS}")
    if (socket.socket.connect, socket.socket.connect_ex, socket.create_connection) != _PRISTINE:
        runner_failed += 1
        print("FAIL  <runner>: socket 沒有還原")
    if os.path.exists(paths.RUNTIME_DIR) != runtime_existed:
        runner_failed += 1
        print(f"FAIL  <runner>: 測試在真正的 runtime/ 留下了東西（{paths.RUNTIME_DIR}）")
    print(f"\n{len(tests) - failed} passed, {failed} failed"
          + (f", {runner_failed} runner check(s) failed" if runner_failed else ""))
    sys.exit(1 if failed or runner_failed else 0)
