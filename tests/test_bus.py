# -*- coding: utf-8 -*-
"""
A2 (live.signal_events / live.bus) 驗收測試 — 事件送得到、壞事件出不去、一個訂閱者出事不拖累別人。

  AC-1  兩個訂閱者依訂閱順序收到同一個事件；不同事件型別只送到對應的訂閱者；
        handler 在發布者的執行緒裡同步執行
  AC-2  第一個 handler 拋例外 → 第二個照樣收到；publish 不拋；送達報告列出失敗者；
        日誌有一筆 ERROR，含 handler 名稱、signal_id 與完整 traceback
  AC-3  strategy 未知、價格 ≤ 0 / NaN / inf / bool、缺必要欄位……建構時就拋例外；
        features 是唯讀的複本；strategy 名單只有 live.config.STRATEGIES 一份（用 AST 掃 live/，
        docstring / 註解 / 日誌文字提到代號不算，dict 鍵不算，別名不算；掃描器本身有反向對照）
  AC-4  handler 裡再 publish：兩個事件都送達、不死鎖（在另一個執行緒跑，join 加逾時）
  AC-5  多個執行緒同時 publish：每個事件恰好送達每個訂閱者一次
  AC-6  沒有訂閱者時記 WARNING
  AC-7  s4 與 s5 對同一個 symbol 的事件都送達，訂閱者分得出來
  AC-8  python -m live exit 0；模組名不撞名；import 無副作用、不拖進 strategy/ 與 pionex_*

鑑別力（反向對照）：AC-2 / AC-4 / AC-5 / AC-6 的斷言都寫成「round 函式 + 匯流排工廠」。
除了拿真正的 SignalBus 跑，也拿 _BrokenBus 的幾種爛實作跑同一個 round —— 沒有 try/except、
出錯就停、不記日誌、日誌沒有 traceback / 名稱 / signal_id、呼叫 handler 時持有鎖、沒訂閱者
時靜默 —— 每一種都必須讓 round 失敗，而且失敗在對應的那條斷言上。另外拿「什麼都沒弄壞」的
_BrokenBus 跑一遍全部 round 必須通過，證明反向對照會失敗是因為那個缺陷，不是對照組本身寫壞。

逾時保護：可能卡住的情境一律在 daemon 執行緒裡跑、join(timeout) 後斷言已結束，不用 sleep 等。
正常情況 join 立刻返回；只有反向對照（故意死鎖的爛實作）才會真的等到逾時（0.5～1 秒）。

離線：每個測試都包在 scoped 的 socket 籠子裡（進入時 patch 並自我測試、離開時還原）；
import 本檔不留下任何全域 socket patch（test_import_does_not_leave_socket_patched）。
不讀時鐘：事件的時刻欄位都是固定整數。價格是任意的合法值，不代表任何策略的出場參數
（事件不計算價格，測試也不重寫策略參數）。

不依賴 pytest：直接 `python tests/test_bus.py` 會逐一跑完並印結果。
"""
import ast
import collections
import contextlib
import copy
import dataclasses
import decimal
import functools
import json
import logging
import math
import os
import socket
import subprocess
import sys
import textwrap
import threading
import types

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

# F4：在 import 任何專案模組之前先抓住 socket 的原函式物件，最後比對仍是同一個
PRISTINE_SOCKET = {
    "connect": socket.socket.connect,
    "connect_ex": socket.socket.connect_ex,
    "create_connection": socket.create_connection,
    "getaddrinfo": socket.getaddrinfo,
}

from live import bus, config, signal_events  # noqa: E402
from live.bus import PublishReport, SignalBus  # noqa: E402
from live.signal_events import (  # noqa: E402
    DIRECTION_SHORT, EXIT_STOP_LOSS, EXIT_TAKE_PROFIT, EntryEvent, ExitEvent, SignalEvent,
)

T0 = 1_758_000_000_000              # 固定的 UTC 毫秒（2025-09-16），不讀時鐘
BAR_MS = 15 * 60 * 1000


# ============================== 離線籠子（scoped） ==============================
class NetworkBlocked(RuntimeError):
    """離線測試企圖連網時拋出。"""


class _OfflineCage:
    """進入時把 socket 的出口換成直接拋 NetworkBlocked，並自我測試；離開時（含例外）還原。"""

    PROBE = ("198.51.100.1", 9)     # RFC 5737 TEST-NET-2；攔截發生在系統呼叫之前，不會有封包出去

    def __enter__(self):
        self._saved = {
            "connect": socket.socket.connect,
            "connect_ex": socket.socket.connect_ex,
            "create_connection": socket.create_connection,
            "getaddrinfo": socket.getaddrinfo,
        }

        def _blocked(*args, **kwargs):
            raise NetworkBlocked("tests/test_bus.py：離線測試禁止連網")

        socket.socket.connect = _blocked
        socket.socket.connect_ex = _blocked
        socket.create_connection = _blocked
        socket.getaddrinfo = _blocked
        try:
            self._self_test()
        except BaseException:
            self._restore()
            raise
        return self

    def __exit__(self, *exc_info):
        self._restore()
        return False

    def _restore(self):
        socket.socket.connect = self._saved["connect"]
        socket.socket.connect_ex = self._saved["connect_ex"]
        socket.create_connection = self._saved["create_connection"]
        socket.getaddrinfo = self._saved["getaddrinfo"]

    def _self_test(self):
        """證明籠子是活的：三條出口各試一次，都必須被攔下。"""
        def raw_connect():
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect(self.PROBE)
            finally:
                s.close()

        probes = (lambda: socket.create_connection(self.PROBE, timeout=1),
                  lambda: socket.getaddrinfo("example.invalid", 80),
                  raw_connect)
        for probe in probes:
            try:
                probe()
            except NetworkBlocked:
                continue
            raise AssertionError("離線籠子沒有生效")


def _offline(fn):
    """把一個測試包進籠子。用裝飾器而不是在 runner 裡包，換任何 runner 跑都一樣離線。"""
    @functools.wraps(fn)
    def wrapper():
        with _OfflineCage():
            return fn()
    return wrapper


# ============================== 小工具 ==============================
def _entry_kwargs():
    return dict(strategy="s4", signal_id="s4-TEST_USDT-%d" % T0, symbol="TEST_USDT",
                direction=DIRECTION_SHORT, bar_open_ms=T0, signal_price=100.0,
                take_profit_price=91.0, stop_loss_price=107.0,
                created_ms=T0 + BAR_MS + 1234,
                features={"ret2h": 0.21, "volr": 3.4, "cpos": 0.3})


def _exit_kwargs():
    return dict(strategy="s4", signal_id="s4-TEST_USDT-%d" % T0, symbol="TEST_USDT",
                direction=DIRECTION_SHORT, reason=EXIT_TAKE_PROFIT, exit_price=91.0,
                entry_price=100.0, opened_ms=T0 + BAR_MS, closed_ms=T0 + 5 * BAR_MS,
                created_ms=T0 + 5 * BAR_MS + 999)


def _entry(**overrides):
    return EntryEvent(**{**_entry_kwargs(), **overrides})


def _exit(**overrides):
    return ExitEvent(**{**_exit_kwargs(), **overrides})


def _raises(exc_types, fn, *fragments):
    """fn() 必須拋 exc_types，且訊息含每個 fragment。拋別的例外就原樣往外傳（測試失敗）。"""
    try:
        fn()
    except exc_types as e:
        msg = str(e)
        for fragment in fragments:
            assert fragment in msg, "例外訊息裡找不到 %r：%s" % (fragment, msg)
        return e
    raise AssertionError("期待拋出 %s，但沒有" % (exc_types,))


class _LogCapture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []
        self._formatter = logging.Formatter("%(levelname)s %(name)s: %(message)s")

    def emit(self, record):
        self.records.append(record)

    def at(self, level):
        return [r for r in self.records if r.levelno == level]

    def render(self, record):
        """含 traceback 的完整輸出，跟日誌檔裡看到的一樣。"""
        return self._formatter.format(record)


@contextlib.contextmanager
def _capture_bus_log():
    """暫時在 live.bus 的 logger 上掛一個收集器，離開時拆掉並還原等級。

    有 handler 接住，標準庫就不會走 logging.lastResort 把東西印到 stderr。
    """
    lg = logging.getLogger(bus.__name__)
    handler = _LogCapture()
    saved_level = lg.level
    lg.addHandler(handler)
    lg.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        lg.removeHandler(handler)
        lg.setLevel(saved_level)


def _run_in_thread(fn, timeout, name):
    """在 daemon 執行緒裡跑 fn，join(timeout)。回傳 (是否已結束, fn 的回傳值, fn 拋的例外)。"""
    box = {}

    def target():
        try:
            box["value"] = fn()
        except BaseException as e:  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=target, name=name, daemon=True)
    t.start()
    t.join(timeout)
    return not t.is_alive(), box.get("value"), box.get("error")


# ============================== 反向對照用的爛實作 ==============================
class _BrokenBus:
    """介面與 SignalBus 相同。參數全預設時是一個正確的實作；每個參數故意弄壞一件事。

    isolate=False        publish 裡沒有 try/except，handler 的例外直接拋給發布者
    keep_going=False     有 try/except，但第一個失敗就停，後面的 handler 收不到
    log=False            失敗時不記任何日誌（靜默吞掉）
    log_traceback=False  記了 ERROR 但沒有 traceback
    log_name=False       ERROR 訊息沒有 handler 名稱
    log_signal_id=False  ERROR 訊息沒有 signal_id
    hold_lock=True       呼叫 handler 時持有（不可重入的）鎖
    warn_empty=False     沒有訂閱者時靜默
    """

    def __init__(self, *, isolate=True, keep_going=True, log=True, log_traceback=True,
                 log_name=True, log_signal_id=True, hold_lock=False, warn_empty=True):
        self.isolate = isolate
        self.keep_going = keep_going
        self.log = log
        self.log_traceback = log_traceback
        self.log_name = log_name
        self.log_signal_id = log_signal_id
        self.hold_lock = hold_lock
        self.warn_empty = warn_empty
        self._lock = threading.Lock()
        self._subs = {}

    def subscribe(self, event_type, handler, name):
        with self._lock:
            self._subs[event_type] = self._subs.get(event_type, ()) + ((name, handler),)

    def publish(self, event):
        if self.hold_lock:
            with self._lock:
                return self._deliver(self._subs.get(type(event), ()), event)
        with self._lock:
            targets = self._subs.get(type(event), ())
        return self._deliver(targets, event)

    def _deliver(self, targets, event):
        lg = logging.getLogger(bus.__name__)
        if not targets:
            if self.warn_empty:
                lg.warning("no subscriber, signal_id=%s", event.signal_id)
            return PublishReport(delivered=(), failed=())
        delivered, failed = [], []
        for name, handler in targets:
            if not self.isolate:
                handler(event)
                delivered.append(name)
                continue
            try:
                handler(event)
            except Exception:
                failed.append(name)
                if self.log:
                    parts = ["handler failed"]
                    if self.log_name:
                        parts.append("name=%s" % name)
                    if self.log_signal_id:
                        parts.append("signal_id=%s" % event.signal_id)
                    lg.error("%s", " ".join(parts), exc_info=self.log_traceback)
                if not self.keep_going:
                    break
            else:
                delivered.append(name)
        return PublishReport(delivered=tuple(delivered), failed=tuple(failed))


def _must_fail(round_fn, make_bus, expected_fragment, **kwargs):
    """反向對照：拿爛實作跑同一個 round，必須以 AssertionError 失敗，而且失敗在預期的斷言上。"""
    try:
        with _capture_bus_log():            # 爛實作的日誌不要漏到 stderr
            round_fn(make_bus, **kwargs)
    except AssertionError as e:
        msg = str(e)
        assert expected_fragment in msg, \
            "%s 對爛實作失敗了，但不是失敗在預期的斷言（期待含 %r）：%s" % (
                round_fn.__name__, expected_fragment, msg)
        return msg
    raise AssertionError("%s 拿爛實作跑居然通過：這組斷言沒有鑑別力" % round_fn.__name__)


# ============================== AC-1：送達與順序 ==============================
@_offline
def test_ac1_delivery_follows_subscription_order_and_event_type():
    b = SignalBus()
    calls = []
    publisher = threading.get_ident()
    # 名稱刻意跟字母順序相反：送達順序要跟訂閱順序走，不是依名稱排序
    b.subscribe(EntryEvent, lambda e: calls.append(("zeta", e, threading.get_ident())), "zeta")
    b.subscribe(EntryEvent, lambda e: calls.append(("alpha", e, threading.get_ident())), "alpha")
    b.subscribe(ExitEvent, lambda e: calls.append(("exit-only", e, threading.get_ident())),
                "exit-only")
    entry, exit_ = _entry(), _exit()

    with _capture_bus_log() as cap:
        r_entry = b.publish(entry)
        # 同步：publish 返回時 handler 已經跑完，而且是在發布者的執行緒裡跑的
        assert [(n, e) for n, e, _ in calls] == [("zeta", entry), ("alpha", entry)], calls
        assert all(e is entry for _, e, _ in calls), "每個訂閱者拿到的應該是同一個事件物件"
        assert {tid for _, _, tid in calls} == {publisher}, "handler 應該在發布者的執行緒裡同步執行"
        assert r_entry == PublishReport(delivered=("zeta", "alpha"), failed=()) and r_entry.ok

        calls.clear()
        r_exit = b.publish(exit_)
        assert [(n, e) for n, e, _ in calls] == [("exit-only", exit_)], "出場事件送錯訂閱者：%r" % calls
        assert r_exit == PublishReport(delivered=("exit-only",), failed=()) and r_exit.ok
    assert not cap.at(logging.WARNING) and not cap.at(logging.ERROR), \
        [r.getMessage() for r in cap.records]
    assert b.subscribers(EntryEvent) == ("zeta", "alpha")
    assert b.subscribers(ExitEvent) == ("exit-only",)


# ============================== AC-2：例外隔離 ==============================
AC2_FAILING = "ac2-exploding-subscriber"
AC2_SURVIVOR = "ac2-survivor"
AC2_SIGNAL_ID = "s4-AC2_USDT-0001"


def _ac2_round(make_bus):
    b = make_bus()
    got = []

    def ac2_exploding_handler(event):
        # 例外訊息裡刻意不放 handler 名稱與 signal_id：那兩個必須由匯流排自己寫進日誌
        raise RuntimeError("A2-BOOM")

    b.subscribe(EntryEvent, ac2_exploding_handler, AC2_FAILING)
    b.subscribe(EntryEvent, got.append, AC2_SURVIVOR)
    event = _entry(signal_id=AC2_SIGNAL_ID)
    with _capture_bus_log() as cap:
        try:
            report = b.publish(event)
        except Exception as e:  # noqa: BLE001
            raise AssertionError("publish 把訂閱者的例外拋給了發布者：%r" % (e,)) from None

    assert got == [event] and got[0] is event, "第一個 handler 出事後，第二個沒收到：%r" % (got,)
    assert report.failed == (AC2_FAILING,), "送達報告沒有列出失敗者：%r" % (report,)
    assert report.delivered == (AC2_SURVIVOR,), "送達報告的成功名單不對：%r" % (report,)
    assert report.ok is False, report
    errors = cap.at(logging.ERROR)
    assert len(errors) == 1, "應該剛好一筆 ERROR：%r" % [r.getMessage() for r in cap.records]
    message = errors[0].getMessage()
    assert AC2_FAILING in message, "ERROR 訊息沒有 handler 名稱：%r" % message
    assert AC2_SIGNAL_ID in message, "ERROR 訊息沒有 signal_id：%r" % message
    full = cap.render(errors[0])
    for piece in ("Traceback (most recent call last):", "ac2_exploding_handler",
                  "RuntimeError: A2-BOOM"):
        assert piece in full, "ERROR 沒有完整 traceback（缺 %r）：\n%s" % (piece, full)


@_offline
def test_ac2_failing_handler_is_isolated():
    _ac2_round(SignalBus)


@_offline
def test_ac2_discriminates_bus_without_try_except():
    """DQA 指定的反向對照：publish 裡沒有 try/except 的爛實作，同一個 round 必須失敗。"""
    _must_fail(_ac2_round, lambda: _BrokenBus(isolate=False), "拋給了發布者")


@_offline
def test_ac2_discriminates_other_broken_isolation():
    """每一條斷言各自有鑑別力：停在第一個失敗、靜默、沒 traceback、沒名稱、沒 signal_id。"""
    variants = [
        (dict(keep_going=False), "第二個沒收到"),
        (dict(log=False), "剛好一筆 ERROR"),
        (dict(log_traceback=False), "traceback"),
        (dict(log_name=False), "handler 名稱"),
        (dict(log_signal_id=False), "signal_id"),
    ]
    for kwargs, fragment in variants:
        _must_fail(_ac2_round, functools.partial(_BrokenBus, **kwargs), fragment)


@_offline
def test_keyboard_interrupt_in_handler_is_not_swallowed():
    """只隔離 Exception；Ctrl+C（KeyboardInterrupt）不可以被匯流排吞掉。"""
    b = SignalBus()

    def interrupted(event):
        raise KeyboardInterrupt

    b.subscribe(EntryEvent, interrupted, "ctrl-c")
    _raises(KeyboardInterrupt, lambda: b.publish(_entry()))


# ============================== AC-3：事件驗證 ==============================
@_offline
def test_ac3_valid_events_are_normalized():
    """合法輸入照收；int 價格存成 float，numpy 純量轉成 Python 型別（A3 會從 DataFrame 取值）。"""
    import numpy as np
    e = _entry(signal_price=100, bar_open_ms=np.int64(T0), take_profit_price=np.float64(91.0),
               features={"ret2h": np.float32(0.25), "n": np.int64(3), "flag": True, "tag": "x",
                         "missing": None})
    assert type(e.signal_price) is float and e.signal_price == 100.0
    assert type(e.take_profit_price) is float
    assert type(e.bar_open_ms) is int and e.bar_open_ms == T0
    assert type(e.features["ret2h"]) is float and type(e.features["n"]) is int
    assert e.features["flag"] is True and e.features["missing"] is None
    # numpy.bool_ 不是 bool 的子類、也不是 numbers.Integral，要靠鴨子型別認出來（S-1）
    b = _entry(features={"np_true": np.bool_(True), "np_false": np.bool_(False),
                         "zero_dim": np.array(True)}).features
    assert b["np_true"] is True and b["np_false"] is False and b["zero_dim"] is True, \
        "numpy 布林純量應該轉成 Python bool：%r" % (dict(b),)
    x = _exit(opened_ms=np.int64(T0), closed_ms=T0)          # 同一刻開平倉是合法的
    assert type(x.opened_ms) is int and x.closed_ms == x.opened_ms
    assert _exit(reason=EXIT_STOP_LOSS, exit_price=107.0).reason == EXIT_STOP_LOSS


@_offline
def test_ac3_unknown_strategy_is_rejected():
    for build in (_entry, _exit):
        for bad in ("s3", "S4", " s4", "s4 ", ""):
            _raises(ValueError, lambda: build(strategy=bad), "strategy")
        for bad in (4, None, b"s4"):
            _raises(TypeError, lambda: build(strategy=bad), "strategy")


@_offline
def test_ac3_prices_must_be_finite_positive_reals():
    cases = [(_entry, ("signal_price", "take_profit_price", "stop_loss_price")),
             (_exit, ("exit_price", "entry_price"))]
    for build, names in cases:
        for name in names:
            for bad in (0, 0.0, -1, -0.01, math.nan, math.inf, -math.inf, 10 ** 400):
                _raises(ValueError, lambda: build(**{name: bad}), name)
            # bool 是 int 的子類（True == 1），價格收到 bool 一定是傳錯了
            for bad in (True, False, "100", None, decimal.Decimal("100"), [100.0]):
                _raises(TypeError, lambda: build(**{name: bad}), name)


@_offline
def test_ac3_missing_or_none_required_field_is_rejected():
    for cls, make_kwargs in ((EntryEvent, _entry_kwargs), (ExitEvent, _exit_kwargs)):
        names = [f.name for f in dataclasses.fields(cls)]
        assert set(names) == set(make_kwargs()), "測試的欄位清單跟事件定義對不上：%s" % names
        for name in names:
            kwargs = make_kwargs()
            del kwargs[name]
            _raises(TypeError, lambda: cls(**kwargs), name)
            kwargs[name] = None
            _raises((TypeError, ValueError), lambda: cls(**kwargs), name)


@_offline
def test_ac3_positional_arguments_are_rejected():
    """全部欄位只能用關鍵字給：好幾個欄位都是價格，位置一錯位型別也不會報錯。"""
    values = list(_entry_kwargs().values())
    _raises(TypeError, lambda: EntryEvent(*values))


@_offline
def test_ac3_timestamps_must_be_integer_utc_ms():
    cases = [(_entry, ("bar_open_ms", "created_ms")),
             (_exit, ("opened_ms", "closed_ms", "created_ms"))]
    for build, names in cases:
        for name in names:
            for bad in (float(T0), True, str(T0), None):
                _raises(TypeError, lambda: build(**{name: bad}), name)
            # 秒、微秒、負數、0：單位弄錯的典型樣子
            for bad in (T0 // 1000, T0 * 1000, -T0, 0):
                _raises(ValueError, lambda: build(**{name: bad}), name)


@_offline
def test_ac3_text_fields_must_be_non_empty_strings():
    for build in (_entry, _exit):
        for name in ("signal_id", "symbol"):
            for bad in ("", " ", " X_USDT", "X_USDT\n"):
                _raises(ValueError, lambda: build(**{name: bad}), name)
            for bad in (123, None, b"X_USDT"):
                _raises(TypeError, lambda: build(**{name: bad}), name)


@_offline
def test_ac3_direction_and_exit_reason_must_be_known():
    for build in (_entry, _exit):
        for bad in ("long", "SHORT", "sell"):
            _raises(ValueError, lambda: build(direction=bad), "direction")
    for bad in ("timeout", "tp", "TAKE_PROFIT"):
        _raises(ValueError, lambda: _exit(reason=bad), "reason")


@_offline
def test_ac3_take_profit_and_stop_loss_must_be_on_the_right_side():
    """做空：止盈價 < 訊號價 < 止損價。止盈止損放反、或跟訊號價相等，都是產生端的 bug。"""
    for tp, sl in ((107.0, 91.0), (100.0, 107.0), (91.0, 100.0), (101.0, 107.0), (91.0, 99.0)):
        _raises(ValueError, lambda: _entry(take_profit_price=tp, stop_loss_price=sl), "止盈價")


@_offline
def test_ac3_exit_cannot_close_before_it_opens():
    _raises(ValueError, lambda: _exit(opened_ms=T0 + BAR_MS, closed_ms=T0), "closed_ms")


@_offline
def test_ac3_features_are_a_read_only_copy():
    source = {"ret2h": 0.21, "volr": 3.4}
    e = _entry(features=source)
    source["ret2h"] = 999.0
    source["injected"] = 1.0
    assert dict(e.features) == {"ret2h": 0.21, "volr": 3.4}, "改原本的 dict 影響到事件了：%r" % (
        dict(e.features),)
    assert isinstance(e.features, types.MappingProxyType)

    def write():
        e.features["ret2h"] = 0.0

    def delete():
        del e.features["ret2h"]

    _raises(TypeError, write)
    _raises(TypeError, delete)
    # to_dict() 給出的是另一份一般 dict，改它也不會動到事件
    d = e.to_dict()
    d["features"]["ret2h"] = -1.0
    assert e.features["ret2h"] == 0.21


@_offline
def test_ac3_features_reject_bad_shapes():
    _raises(TypeError, lambda: _entry(features=[("ret2h", 0.2)]), "features")
    _raises(ValueError, lambda: _entry(features={}), "features")
    for bad in ({1: 0.2}, {"": 0.2}):
        _raises(TypeError, lambda: _entry(features=bad), "features")
    # 可變容器會讓「唯讀」只唯讀到第一層；Decimal 也不是實數型別；布林「陣列」是容器
    import numpy as np
    for bad in ({"a": [1.0]}, {"a": {"b": 1.0}}, {"a": decimal.Decimal("1")},
                {"a": np.array([True, False])}, {"a": np.array([True])}):
        _raises(TypeError, lambda: _entry(features=bad), "features")
    # numpy.bool_ 只在 features 放行；價格與時刻欄位照樣拒收
    _raises(TypeError, lambda: _entry(signal_price=np.bool_(True)), "signal_price")
    _raises(TypeError, lambda: _entry(bar_open_ms=np.bool_(True)), "bar_open_ms")


@_offline
def test_ac3_feature_nan_becomes_none_and_inf_is_kept():
    """S-3：NaN 表示沒有值，存成 None（跟 dry run 快照同一個慣例）；±inf 是有意義的值，保留。

    NaN 留著的話，to_dict() 再建回來的事件跟原本不相等（NaN != NaN），json.dumps 也會寫出非標準的 NaN。
    """
    import numpy as np
    e = _entry(features={"nan": math.nan, "np_nan": np.float64("nan"), "volr": math.inf,
                         "neg": -math.inf, "ret2h": 0.21})
    assert e.features["nan"] is None and e.features["np_nan"] is None, \
        "NaN 應該存成 None：%r" % (dict(e.features),)
    assert e.features["volr"] == math.inf and e.features["neg"] == -math.inf, \
        "±inf 是有意義的值，應該原樣保留：%r" % (dict(e.features),)
    # 經過 JSON 來回之後仍然相等（Python 的 json 讀得回 Infinity）
    assert EntryEvent(**json.loads(json.dumps(e.to_dict()))) == e
    # to_dict 的說明：嚴格 JSON 不收 inf，allow_nan=False 會當場拋錯而不是悄悄送出去
    _raises(ValueError, lambda: json.dumps(e.to_dict(), allow_nan=False))
    strict = _entry(features={"nan": math.nan, "ret2h": 0.21})
    assert json.loads(json.dumps(strict.to_dict(), allow_nan=False))["features"]["nan"] is None


@_offline
def test_events_copy_to_themselves():
    """S-2：事件完全不可變，copy / deepcopy 直接回傳同一個物件；放在容器裡整包 deepcopy 也可以。"""
    for event in (_entry(), _exit()):
        assert copy.copy(event) is event, "copy.copy 應該回傳同一個事件"
        assert copy.deepcopy(event) is event, "copy.deepcopy 應該回傳同一個事件"
        bundle = copy.deepcopy({"events": [event], "n": 1})
        assert bundle["events"][0] is event, "容器整包 deepcopy 時，裡面的事件應該是同一個物件"


@_offline
def test_ac3_events_are_frozen_hashable_values():
    e = _entry()

    def reassign():
        e.strategy = "s5"

    _raises(dataclasses.FrozenInstanceError, reassign)
    twin = _entry()
    assert e == twin and hash(e) == hash(twin) and len({e, twin}) == 1
    assert _exit() == _exit() and hash(_exit()) == hash(_exit())
    assert _entry(features={"ret2h": 0.5}) != e, "features 不同的事件不應該相等"


@_offline
def test_ac3_signal_event_base_cannot_be_built():
    kwargs = {k: _entry_kwargs()[k] for k in ("strategy", "signal_id", "symbol", "direction",
                                              "created_ms")}
    _raises(TypeError, lambda: SignalEvent(**kwargs), "EntryEvent")


@_offline
def test_ac3_strategies_come_from_config_only():
    """事件驗證 strategy 時讀的是 live.config.STRATEGIES，而且是建構當下才讀（不在 import 時綁死）。"""
    saved = config.STRATEGIES
    try:
        config.STRATEGIES = saved + ("s9",)
        assert _entry(strategy="s9").strategy == "s9"
    finally:
        config.STRATEGIES = saved
    _raises(ValueError, lambda: _entry(strategy="s9"), "strategy")


# ============================== AC-3：live/ 不可以有第二份策略名單 ==============================
def _binds_strategies(target):
    """指定的對象裡有沒有名稱 STRATEGIES（含 obj.STRATEGIES 與 tuple 拆包）。"""
    if isinstance(target, ast.Name):
        return target.id == "STRATEGIES"
    if isinstance(target, ast.Attribute):
        return target.attr == "STRATEGIES"
    if isinstance(target, (ast.Tuple, ast.List)):
        return any(_binds_strategies(t) for t in target.elts)
    if isinstance(target, ast.Starred):
        return _binds_strategies(target.value)
    return False


def _duplicate_strategy_lists(source, filename="<source>"):
    """找出 source 裡的「第二份策略名單」，回傳 [(行號, 說明), ...]，依行號排序；沒有就是空的。

    只看 AST：docstring 是一個字串常數、註解根本不進 AST，其他字串內容（日誌訊息、f-string
    的文字）也都只是單一個字串常數，所以一律不算。認得的策略代號取自**當下的** config.STRATEGIES，
    偵測邏輯裡不寫死任何代號。兩條規則：

      (1) 對名稱 STRATEGIES 的指定（Assign / AnnAssign / AugAssign / :=，任何層級，含 obj.STRATEGIES
          與拆包）。例外：值是單純的名稱或屬性引用，也就是別名（例如 STRATEGIES = config.STRATEGIES），
          那只是轉手同一個物件，不是第二份來源。字面值、運算、函式呼叫都不算別名，
          AugAssign（+=）一律算 —— 那就是在改名單。只宣告型別不給值（STRATEGIES: tuple）不算。
      (2) tuple / list / set 字面值裡，字串常數元素含 config.STRATEGIES 的 ≥ 2 個不同值
          （例如 KNOWN = [...]、if s in (...)、預設參數）。

    刻意**不抓 dict 的鍵**：依策略分派的表（例如 A3 的 {"s4": s4 的出場參數, "s5": ...}）是正當
    需求，它是「每個策略各自的東西」，不是「有哪些策略」的名單。這種表由擁有它的模組自己寫測試，
    斷言表的鍵 == set(config.STRATEGIES)，漏接新策略時會在那裡失敗。

    已知的漏網之魚（不是名單的正常寫法，這裡不追）："s4 s5".split()、s == "s4" or s == "s5"、
    match 的 case "s4" | "s5"。
    """
    known = set(config.STRATEGIES)
    found = []
    for node in ast.walk(ast.parse(source, filename)):
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, (ast.AnnAssign, ast.NamedExpr, ast.AugAssign)):
            targets, value = [node.target], node.value
        else:
            targets, value = (), None
        if any(_binds_strategies(t) for t in targets):
            if isinstance(node, ast.AugAssign):
                found.append((node.lineno, "就地修改 STRATEGIES：%s" % ast.unparse(node)[:100]))
            elif value is not None and not isinstance(value, (ast.Name, ast.Attribute)):
                found.append((node.lineno, "STRATEGIES 指定了別名以外的值：%s" % ast.unparse(node)[:100]))
        if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            codes = {e.value for e in node.elts
                     if isinstance(e, ast.Constant) and isinstance(e.value, str)} & known
            if len(codes) >= 2:
                found.append((node.lineno, "字面值裡有 %d 個策略代號：%s"
                              % (len(codes), ast.unparse(node)[:100])))
    return sorted(found)


def _scan_for_strategy_lists(live_dir):
    """掃 live_dir 底下（含子目錄）除了頂層 config.py 以外的每個 .py。

    回傳 (掃過的相對路徑清單, {相對路徑: _duplicate_strategy_lists 的結果})，後者只列有問題的檔。
    """
    scanned, hits = [], {}
    for root, dirs, files in os.walk(live_dir):
        dirs[:] = sorted(d for d in dirs if d != "__pycache__")
        for fname in sorted(files):
            if not fname.endswith(".py"):
                continue
            rel = os.path.relpath(os.path.join(root, fname), live_dir).replace(os.sep, "/")
            if rel == "config.py":
                continue
            with open(os.path.join(root, fname), encoding="utf-8") as f:
                found = _duplicate_strategy_lists(f.read(), rel)
            scanned.append(rel)
            if found:
                hits[rel] = found
    return scanned, hits


@_offline
def test_ac3_no_second_strategy_list_in_live():
    """live/ 底下除了 config.py，沒有任何模組自己寫一份策略名單（規則見 _duplicate_strategy_lists）。

    dict 的鍵刻意不抓：依策略分派的表是正當需求，由擁有它的模組自己斷言鍵 == set(config.STRATEGIES)。
    docstring / 註解 / 日誌訊息提到策略代號也不算 —— 其他任務不能改本檔，說明文字不可以讓 stage 誤報。
    """
    live_dir = os.path.join(REPO_ROOT, "live")
    scanned, hits = _scan_for_strategy_lists(live_dir)
    # 前提：真的有掃到東西，而且掃描器認得 config.py 裡那一份正本（不是對什麼都回空的）
    assert "signal_events.py" in scanned and "bus.py" in scanned, scanned
    assert "config.py" not in scanned
    with open(os.path.join(live_dir, "config.py"), encoding="utf-8") as f:
        assert _duplicate_strategy_lists(f.read(), "config.py"), "掃描器認不出 config.py 裡的正本"
    assert not hits, "live/ 底下有第二份策略名單（改成引用 config.STRATEGIES）：%r" % (hits,)


@_offline
def test_ac3_strategy_list_scan_discriminates():
    """反向對照：該抓的每一種都抓到、不該抓的（說明文字、dict 鍵、別名……）一個都不抓。

    範例原始碼用 config.STRATEGIES 的值組出來，名單日後改了，這組對照照樣有意義。
    """
    assert len(config.STRATEGIES) >= 2, "規則 (2) 需要至少兩個策略代號才有意義"
    a, b = config.STRATEGIES[0], config.STRATEGIES[1]
    qa, qb = json.dumps(a), json.dumps(b)             # 帶雙引號的字面值，例如 "s4"
    must_catch = {
        "字面值指定給 STRATEGIES": "STRATEGIES = (%s, %s)\n" % (qa, qb),
        "list 名單": "KNOWN = [%s, %s]\n" % (qa, qb),
        "set 名單": "ALLOWED = {%s, %s}\n" % (qa, qb),
        "if ... in (...)": "def check(s):\n    if s in (%s, %s):\n        return True\n" % (qa, qb),
        "預設參數、順序顛倒": "def f(s, allowed=(%s, %s)):\n    return s in allowed\n" % (qb, qa),
        "包在 frozenset() 裡": "NAMES = frozenset({%s, %s})\n" % (qa, qb),
        "只有一個代號的 STRATEGIES（帶型別）": "STRATEGIES: tuple = (%s,)\n" % qa,
        "別名之後再 +=": "STRATEGIES = config.STRATEGIES\nSTRATEGIES += ('s9',)\n",
        "由正本延伸": "STRATEGIES = config.STRATEGIES + ('s9',)\n",
        "類別屬性": "class Store:\n    STRATEGIES = ('s9',)\n",
        "改寫 config 的正本": "config.STRATEGIES = (%s, %s, 's9')\n" % (qa, qb),
        "海象": "if (STRATEGIES := ('s9',)):\n    pass\n",
        "拆包": "A, STRATEGIES = 1, ('s9',)\n",
    }
    must_not_catch = {
        "docstring（DQA 的實例）": '"""strategy  STRATEGIES 之一（%s / %s）"""\n' % (qa, qb),
        "函式 docstring": 'def f():\n    """strategy 是 %s 或 %s。"""\n' % (qa, qb),
        "註解": "# %s 的說明\nx = 1\n" % qb,
        "dict 鍵（依策略分派的表）": "TABLE = {%s: 1, %s: 2}\n" % (qa, qb),
        "別名": "from live import config\nSTRATEGIES = config.STRATEGIES\n",
        "別名（名稱）": "from live.config import STRATEGIES as _S\nSTRATEGIES = _S\n",
        "import": "from live.config import STRATEGIES\n",
        "只宣告型別": "STRATEGIES: tuple\n",
        "單一代號": "strategy = %s\n" % qa,
        "tuple 裡只有一個代號": "PAIR = (%s, 'BTC_USDT')\n" % qa,
        "日誌訊息": "logger.info('%s 與 %s 都上線')\n" % (a, b),
        "f-string": "msg = f'{strategy} 不是 %s / %s'\n" % (a, b),
    }
    for label, src in {**must_catch, **must_not_catch}.items():
        try:
            ast.parse(src)
        except SyntaxError as e:
            raise AssertionError("範例原始碼本身寫錯了（%s）：%s" % (label, e)) from None
    missed = [label for label, src in must_catch.items() if not _duplicate_strategy_lists(src)]
    assert not missed, "這些第二份名單沒被抓到：%s" % missed
    false_alarms = {label: _duplicate_strategy_lists(src) for label, src in must_not_catch.items()}
    false_alarms = {k: v for k, v in false_alarms.items() if v}
    assert not false_alarms, "這些不是名單卻被當成名單：%r" % (false_alarms,)


@_offline
def test_ac3_config_strategies_is_the_product_decision_and_reported():
    """2026-09-25 使用者決定策略4、5 都上線；名單是 tuple（執行期沒人能 append），並列入冒煙檢查。"""
    assert isinstance(config.STRATEGIES, tuple)
    assert set(config.STRATEGIES) == {"s4", "s5"}, config.STRATEGIES
    assert config.execution_params()["STRATEGIES"] == config.STRATEGIES


@_offline
def test_to_dict_round_trips_and_is_json_serializable():
    for event in (_entry(), _exit(reason=EXIT_STOP_LOSS, exit_price=107.0)):
        d = event.to_dict()
        assert type(event)(**d) == event
        back = json.loads(json.dumps(d))
        assert type(event)(**back) == event


# ============================== 匯流排的介面防呆 ==============================
@dataclasses.dataclass(frozen=True, kw_only=True)
class _EntrySubclass(EntryEvent):
    """匯流排只收精確型別；子類要被擋下。"""


@_offline
def test_subscribe_rejects_bad_arguments():
    b = SignalBus()
    for bad_type in (dict, SignalEvent, _EntrySubclass, "EntryEvent", None):
        _raises(TypeError, lambda: b.subscribe(bad_type, lambda e: None, "x"), "event_type")
    _raises(TypeError, lambda: b.subscribe(EntryEvent, "not callable", "x"), "handler")
    for bad_name in ("", " x", "x "):
        _raises(ValueError, lambda: b.subscribe(EntryEvent, lambda e: None, bad_name), "name")
    _raises(TypeError, lambda: b.subscribe(EntryEvent, lambda e: None, None), "name")
    b.subscribe(EntryEvent, lambda e: None, "tg_a_channel")
    _raises(ValueError, lambda: b.subscribe(EntryEvent, lambda e: None, "tg_a_channel"),
            "tg_a_channel")
    b.subscribe(ExitEvent, lambda e: None, "tg_a_channel")     # 同一個訂閱者訂兩種事件是正常的
    assert b.subscribers(EntryEvent) == ("tg_a_channel",)
    assert b.subscribers(ExitEvent) == ("tg_a_channel",)


@_offline
def test_publish_rejects_things_that_are_not_events():
    """傳錯東西是發布端的程式錯誤，當場拋 TypeError（這不是訂閱者出事）。"""
    b = SignalBus()
    b.subscribe(EntryEvent, lambda e: None, "x")
    sub = _EntrySubclass(**_entry_kwargs())
    for bad in (None, _entry().to_dict(), sub):
        _raises(TypeError, lambda: b.publish(bad), "EntryEvent")


@_offline
def test_subscribe_during_publish_only_affects_later_publishes():
    """publish 用的是快照：途中新增的訂閱者，這一次收不到、下一次才收到。"""
    b = SignalBus()
    late = []

    def adder(event):
        if not b.subscribers(EntryEvent)[1:]:
            b.subscribe(EntryEvent, late.append, "late")

    b.subscribe(EntryEvent, adder, "adder")
    first, second = _entry(signal_id="s4-SNAP-1"), _entry(signal_id="s4-SNAP-2")
    assert b.publish(first).delivered == ("adder",)
    assert late == []
    assert b.publish(second).delivered == ("adder", "late")
    assert late == [second]


# ============================== AC-4：巢狀發布 ==============================
def _ac4_round(make_bus, timeout=10.0):
    b = make_bus()
    log = []
    outer = _entry(signal_id="s4-AC4-outer")
    inner_exit = _exit(signal_id="s4-AC4-inner-exit")
    inner_entry = _entry(signal_id="s4-AC4-inner-entry")
    nested_reports = []

    def republisher(event):
        log.append(("republisher", event.signal_id))
        if event is outer:
            nested_reports.append(b.publish(inner_exit))    # 不同型別
            nested_reports.append(b.publish(inner_entry))   # 同型別：重入同一份訂閱清單

    b.subscribe(EntryEvent, republisher, "republisher")
    b.subscribe(EntryEvent, lambda e: log.append(("entry-tail", e.signal_id)), "entry-tail")
    b.subscribe(ExitEvent, lambda e: log.append(("exit-sub", e.signal_id)), "exit-sub")

    finished, report, error = _run_in_thread(lambda: b.publish(outer), timeout, "a2-ac4-nested")
    assert finished, "巢狀 publish %.1f 秒內沒有結束：死鎖" % timeout
    assert error is None, "巢狀 publish 拋了例外：%r" % (error,)
    # 深度優先：內層事件先送完它的所有訂閱者，外層才接著送下一個訂閱者
    expected = [
        ("republisher", outer.signal_id),
        ("exit-sub", inner_exit.signal_id),
        ("republisher", inner_entry.signal_id),
        ("entry-tail", inner_entry.signal_id),
        ("entry-tail", outer.signal_id),
    ]
    assert log == expected, "送達順序不對：%r" % (log,)
    assert report.ok and report.delivered == ("republisher", "entry-tail"), report
    assert [r.delivered for r in nested_reports] == [("exit-sub",), ("republisher", "entry-tail")]
    assert all(r.ok for r in nested_reports), nested_reports


@_offline
def test_ac4_nested_publish_delivers_both_without_deadlock():
    _ac4_round(SignalBus)


@_offline
def test_ac4_discriminates_bus_that_holds_its_lock_while_calling_handlers():
    """呼叫 handler 時持有鎖的爛實作，巢狀發布會死鎖 —— round 必須以逾時失敗，而不是卡住。"""
    _must_fail(_ac4_round, lambda: _BrokenBus(hold_lock=True), "死鎖", timeout=0.5)


# ============================== AC-5：多執行緒 ==============================
def _ac5_round(make_bus, n_threads=8, per_thread=120, rendezvous_timeout=10.0,
               join_timeout=60.0):
    b = make_bus()
    record_lock = threading.Lock()
    received = collections.defaultdict(list)          # 訂閱者名稱 -> [signal_id, ...]
    first_seen = set()                                # 已經在 gate 會合過的發布執行緒
    met = []                                          # 成功會合的次數
    rendezvous = threading.Barrier(n_threads, timeout=rendezvous_timeout)

    def gate(event):
        # 每個發布執行緒的第一個事件在這裡會合：要 N 個 handler 同時在跑才過得去。
        # 過得去就證明 publish 真的並行，而且呼叫 handler 時沒有持有匯流排的鎖。
        me = threading.get_ident()
        with record_lock:
            first = me not in first_seen
            first_seen.add(me)
            received["gate"].append(event.signal_id)
        if first:
            rendezvous.wait()
            with record_lock:
                met.append(me)

    def recorder(name):
        def handler(event):
            with record_lock:
                received[name].append(event.signal_id)
        return handler

    b.subscribe(EntryEvent, gate, "gate")
    b.subscribe(EntryEvent, recorder("entry-a"), "entry-a")
    b.subscribe(EntryEvent, recorder("entry-b"), "entry-b")
    b.subscribe(ExitEvent, recorder("exit-a"), "exit-a")

    start = threading.Barrier(n_threads, timeout=rendezvous_timeout)
    results = [[] for _ in range(n_threads)]          # 每個執行緒只寫自己那一格
    errors = []

    def worker(k):
        try:
            start.wait()                              # 同時起跑
            for i in range(per_thread):
                if i % 2 == 0:                        # 第一個一定是進場，才會經過 gate
                    ev = _entry(signal_id="ac5-t%d-%d-entry" % (k, i))
                else:
                    ev = _exit(signal_id="ac5-t%d-%d-exit" % (k, i))
                results[k].append((ev, b.publish(ev)))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(k,), name="a2-ac5-%d" % k, daemon=True)
               for k in range(n_threads)]
    saved_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)                       # 讓執行緒切換得更頻繁，交錯得更兇
    try:
        for t in threads:
            t.start()
        for t in threads:
            t.join(join_timeout)
    finally:
        sys.setswitchinterval(saved_interval)

    assert not any(t.is_alive() for t in threads), "有發布執行緒 %.0f 秒內沒結束" % join_timeout
    assert not errors, "發布執行緒拋了例外：%r" % (errors,)
    assert len(first_seen) == n_threads, "handler 應該在 %d 個不同的發布執行緒裡跑" % n_threads
    assert len(met) == n_threads, \
        "%d 個 handler 沒能同時在跑（只會合 %d 個）：publish 沒有並行，或呼叫 handler 時持有鎖" % (
            n_threads, len(met))

    entry_ids, exit_ids = [], []
    for k in range(n_threads):
        assert len(results[k]) == per_thread, "執行緒 %d 只發了 %d 個" % (k, len(results[k]))
        for ev, report in results[k]:
            want = ("gate", "entry-a", "entry-b") if type(ev) is EntryEvent else ("exit-a",)
            assert report == PublishReport(delivered=want, failed=()), (ev.signal_id, report)
            (entry_ids if type(ev) is EntryEvent else exit_ids).append(ev.signal_id)
    assert len(set(entry_ids)) == len(entry_ids) and len(set(exit_ids)) == len(exit_ids)
    for name, ids in (("gate", entry_ids), ("entry-a", entry_ids), ("entry-b", entry_ids),
                      ("exit-a", exit_ids)):
        counts = collections.Counter(received[name])
        assert counts == collections.Counter(ids), \
            "%s 沒有恰好每個事件各收到一次（漏 %d、多 %d）" % (
                name, len(collections.Counter(ids) - counts), len(counts - collections.Counter(ids)))


@_offline
def test_ac5_concurrent_publish_delivers_each_event_exactly_once():
    _ac5_round(SignalBus)


@_offline
def test_ac5_discriminates_bus_that_serializes_handlers_under_its_lock():
    _must_fail(_ac5_round, lambda: _BrokenBus(hold_lock=True), "沒能同時在跑",
               n_threads=4, per_thread=10, rendezvous_timeout=1.0, join_timeout=30.0)


# ============================== AC-6：沒有訂閱者 ==============================
def _ac6_round(make_bus):
    b = make_bus()
    b.subscribe(ExitEvent, lambda e: None, "exit-only")      # 接了出場、漏接進場
    event = _entry(strategy="s5", signal_id="s5-AC6-0001")
    with _capture_bus_log() as cap:
        try:
            report = b.publish(event)
        except Exception as e:  # noqa: BLE001
            raise AssertionError("沒有訂閱者時 publish 不該拋例外：%r" % (e,)) from None
    assert report.delivered == () and report.failed == () and report.ok is False, report
    warnings = [r for r in cap.at(logging.WARNING) if "s5-AC6-0001" in r.getMessage()]
    assert len(warnings) == 1, "沒有訂閱者應該剛好一筆含 signal_id 的 WARNING：%r" % [
        (r.levelname, r.getMessage()) for r in cap.records]
    # 有訂閱者時不應該有這一筆（不是無條件亂叫）
    with _capture_bus_log() as cap:
        assert b.publish(_exit()).ok
    assert not cap.at(logging.WARNING) and not cap.at(logging.ERROR), \
        [r.getMessage() for r in cap.records]


@_offline
def test_ac6_no_subscriber_logs_warning():
    _ac6_round(SignalBus)
    # 完全沒有任何訂閱者的匯流排也一樣：記 WARNING、不拋例外
    with _capture_bus_log() as cap:
        report = SignalBus().publish(_exit(signal_id="s4-AC6-empty"))
    assert not report.ok
    assert any("s4-AC6-empty" in r.getMessage() for r in cap.at(logging.WARNING))


@_offline
def test_ac6_discriminates_silent_bus():
    _must_fail(_ac6_round, lambda: _BrokenBus(warn_empty=False), "WARNING")


# ============================== 對照組本身沒寫壞 ==============================
@_offline
def test_reference_bus_with_nothing_broken_passes_every_round():
    """_BrokenBus 全預設（什麼都沒弄壞）必須通過全部 round：反向對照會失敗只因為那個缺陷。"""
    with _capture_bus_log():
        _ac2_round(_BrokenBus)
        _ac4_round(_BrokenBus)
        _ac5_round(_BrokenBus, n_threads=4, per_thread=20)
        _ac6_round(_BrokenBus)


# ============================== AC-7：兩個策略同一個幣 ==============================
@_offline
def test_ac7_s4_and_s5_on_the_same_symbol_are_both_delivered_and_distinguishable():
    b = SignalBus()
    entries, exits = [], []
    b.subscribe(EntryEvent, entries.append, "a-channel")
    b.subscribe(ExitEvent, exits.append, "a-channel")
    symbol = "SAME_USDT"
    s4 = _entry(strategy="s4", signal_id="s4-SAME_USDT-%d" % T0, symbol=symbol,
                features={"ret2h": 0.3, "volr": 2.5, "cpos": 0.4})
    s5 = _entry(strategy="s5", signal_id="s5-SAME_USDT-%d" % T0, symbol=symbol,
                features={"s5_score": 0.9})               # 兩個策略的判定特徵內容不同
    assert b.publish(s4).ok and b.publish(s5).ok
    assert entries == [s4, s5]
    # 訂閱者用 (strategy, signal_id) 當鍵，兩筆名目部位分得開
    positions = {(e.strategy, e.signal_id): e for e in entries}
    assert len(positions) == 2 and {e.symbol for e in entries} == {symbol}
    assert {e.strategy for e in entries} == {"s4", "s5"}

    # 就算 A3 的 signal_id 沒把策略編進去而撞號，事件本身仍然不同、匯流排也不會吞掉任何一個
    s5_same_id = dataclasses.replace(s5, signal_id=s4.signal_id)
    assert s5_same_id != s4
    assert b.publish(s5_same_id).ok and entries[-1] is s5_same_id and len(entries) == 3

    x4 = _exit(strategy="s4", signal_id=s4.signal_id, symbol=symbol)
    x5 = _exit(strategy="s5", signal_id=s5.signal_id, symbol=symbol, reason=EXIT_STOP_LOSS,
               exit_price=107.0)
    assert b.publish(x4).ok and b.publish(x5).ok
    assert [(x.strategy, x.reason) for x in exits] == [("s4", EXIT_TAKE_PROFIT),
                                                       ("s5", EXIT_STOP_LOSS)]


# ============================== AC-8：範圍與約定 ==============================
# 規則列出的雷 + 常見第三方套件名。新模組不可以叫這些名字。
_HAZARD_NAMES = {"logging", "json", "signal", "queue", "select", "socket", "time", "types", "csv",
                 "email", "sqlite3", "threading", "telegram", "events", "requests", "numpy",
                 "pandas", "urllib3", "blinker", "pubsub", "pydispatch"}


@_offline
def test_ac8_new_module_names_do_not_clash():
    for name in ("bus", "signal_events"):
        assert os.path.isfile(os.path.join(REPO_ROOT, "live", name + ".py")), name
        assert name not in sys.stdlib_module_names, "live/%s.py 跟標準庫同名" % name
        assert name not in _HAZARD_NAMES, "live/%s.py 撞到常見套件名" % name
    assert bus.__name__ == "live.bus" and signal_events.__name__ == "live.signal_events"


@_offline
def test_ac8_import_has_no_side_effects_and_pulls_in_nothing_else():
    """全新子行程裡 import 兩個新模組：不掛 handler、不換 excepthook、不開執行緒、不建目錄，
    也不會把 strategy/（策略參數）、pionex_*、research、pandas / numpy / requests 拖進來。"""
    code = textwrap.dedent("""
        import logging, os, sys, threading
        sys.path.insert(0, %r)
        from live import paths
        runtime_before = os.path.exists(paths.RUNTIME_DIR)
        threads_before = threading.active_count()
        import live.signal_events, live.bus
        assert logging.getLogger().handlers == [], logging.getLogger().handlers
        assert sys.excepthook is sys.__excepthook__
        assert threading.excepthook is threading.__excepthook__
        assert threading.active_count() == threads_before
        assert os.path.exists(paths.RUNTIME_DIR) == runtime_before
        heavy = sorted(m for m in sys.modules
                       if m.split(".")[0] in ("strategy", "research", "pandas", "numpy", "requests")
                       or m.startswith("pionex_"))
        assert not heavy, heavy
        print("IMPORT-CLEAN")
    """ % REPO_ROOT)
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, stdin=subprocess.DEVNULL,
                       capture_output=True, timeout=120)
    out = r.stdout.decode("ascii", "backslashreplace")
    err = r.stderr.decode("ascii", "backslashreplace")
    assert r.returncode == 0 and "IMPORT-CLEAN" in out, "rc=%d\n%s\n%s" % (r.returncode, out, err)


@_offline
def test_ac8_python_m_live_exits_0_and_reports_strategies():
    """cp1252 的 pipe 下（拿掉 PYTHONIOENCODING / PYTHONUTF8）python -m live exit 0，並列出 STRATEGIES。"""
    env = dict(os.environ)
    env.pop("PYTHONIOENCODING", None)
    env.pop("PYTHONUTF8", None)
    r = subprocess.run([sys.executable, "-m", "live"], cwd=REPO_ROOT, env=env,
                       stdin=subprocess.DEVNULL, capture_output=True, timeout=120)
    out = r.stdout.decode("utf-8", "replace")
    where = "rc=%d\n%s\n%s" % (r.returncode, out[-2000:],
                               r.stderr.decode("ascii", "backslashreplace")[-2000:])
    assert r.returncode == 0, where
    assert "STRATEGIES" in out and repr(config.STRATEGIES) in out, where


def test_import_does_not_leave_socket_patched():
    """F4：籠子是 scoped 的。import 本檔、跑完一個關在籠子裡的測試，socket 都必須是原函式。"""
    _offline(lambda: None)()
    now = {"connect": socket.socket.connect, "connect_ex": socket.socket.connect_ex,
           "create_connection": socket.create_connection, "getaddrinfo": socket.getaddrinfo}
    for name, fn in PRISTINE_SOCKET.items():
        assert now[name] is fn, "socket.%s 被換掉了（實際 %r）" % (name, now[name])


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
