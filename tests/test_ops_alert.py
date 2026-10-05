# -*- coding: utf-8 -*-
"""
A4 最小營運告警（live.ops_alert、live.tg_channel 的三個新參數、live.a_channel 的 --push-tg 接線）驗收測試。

  AC-1  ChannelSender 不給新參數 = A 頻道原狀；chat_id_env 讀那個環境變數、遮罩含它的值；執行緒名稱與日誌文字；
        record 的 lineno 是呼叫 _log 的那一行
  AC-2  handler：佇列滿、告警執行緒卡住時一萬次 emit 都不阻塞、丟棄數正確；ops-alert / tg-ops-sender 執行緒與
        live.ops_alert 的 record 被排除（無回饋迴圈）；WARNING 只計數；三個密鑰都遮罩
  AC-3  事件告警：新鍵批次成一則 🔴、⏰ 一小時後的次數、只發生一次的安靜結束、提醒過的 ✅；寬限期間併成一則 🟡；
        CRITICAL 不等批次、不受寬限；關閉期間的 ERROR 併進 ⚪
  AC-4  條件 C1–C6 各自 raised → 每小時 ⏰ → cleared（持續時間正確）；沒啟動的 C1 元件不報；C4 讀不到不 raise
        也不 clear；FR-4 的兩種一次性事件
  AC-5  心跳：連續三個台北 09:00 各恰好一則、差值與假 stats 相符、第一則的用語；未啟動 / 讀不到的寫法
  AC-6  七種訊息全文（--sample 的假資料）；每項截斷與整則截斷（項數、UTF-16 長度）
  AC-7  告警執行緒死掉或卡住時，包過的 on_result 照樣呼叫 A3；A3 的例外原樣往外拋
  AC-8  不帶 --push-tg 完全不變；缺維運密鑰 exit 1、T2 / A3 / A1 都沒啟動；各路徑的 ⚪ 原因與 exit code；
        全部啟動成功才發 🟢；A4 最後停
  AC-11 新常數在 execution_params() 與 python -m live；維運密鑰只列名稱與有沒有設
  其他  --sample 的 exit code、缺密鑰不連網；模組命名與相依

全程離線：socket 籠子（沿用 tests/test_tg_channel.py 的 OfflineCage，進場自我測試、禁止真的 sleep）；
告警器的時間一律注入假時鐘（epoch 秒），outbox 與報表發送紀錄一律開在暫存目錄，不碰 runtime/。
不依賴 pytest：直接 `python tests/test_ops_alert.py`。
"""
import ast
import collections
import contextlib
import io
import linecache
import logging
import os
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime
from types import SimpleNamespace

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, TESTS_DIR)

import test_tg_channel as tgt  # noqa: E402
from test_tg_channel import (FAKE_CHAT, FAKE_TOKEN, OfflineCage, exception_texts, find_leaks, harness,  # noqa: E402
                             http_error, ok, stop_within, wait_until)
from test_a_channel_push import capture, outbox_path_in, secret_reads_forbidden, tempdir  # noqa: E402

from live import a_channel, config, paths, reconcile, rest_gate, universe_seen  # noqa: E402
from live import a_channel_outbox as OB  # noqa: E402
from live import ops_alert as OA  # noqa: E402
from strategy import universe as universe_rules  # noqa: E402
from live.logsetup import TAIPEI  # noqa: E402
from live.signal_feed import UniverseUnavailable  # noqa: E402
from live.tg_channel import MASK, utf16_units  # noqa: E402

FAKE_OPS = "424242987654"
SECRETS = (FAKE_TOKEN, FAKE_CHAT, FAKE_OPS)
LABEL = "test-host"
T0 = datetime(2026, 9, 22, 10, 0, tzinfo=TAIPEI).timestamp()

GREEN, YELLOW, RED = chr(0x1F7E2), chr(0x1F7E1), chr(0x1F534)
ALARM, CHECK, HEART, WHITE = chr(0x23F0), chr(0x2705), chr(0x1F493), chr(0x26AA)
NEW = chr(0x1F195)
TITLE = {"startup": GREEN + " A 頻道程式啟動", "grace": YELLOW + " 啟動期間的錯誤", "alert": RED + " 告警",
         "remind": ALARM + " 仍未恢復", "recovered": CHECK + " 已恢復", "heartbeat": HEART + " 每日心跳",
         "listing": NEW + " 新上架", "shutdown": WHITE + " A 頻道程式結束"}
LOBSTER = "龙虾_USDT_PERP"
DEAD = "執行緒已經結束，不會自動重啟（要重啟程式才會恢復）"
TEST_LOGGER = "live.test_a4"


def tpe(t):
    """T0 + t 秒的台北時間字串（測試自己算，不用 OA.fmt_time）。"""
    return datetime.fromtimestamp(T0 + t, TAIPEI).strftime("%Y-%m-%d %H:%M:%S")


def message(kind, t, *lines):
    return "\n".join([TITLE[kind] + "  " + LABEL, "時間：" + tpe(t)] + list(lines))


# ============================== 替身 ==============================
class FakeSender:
    """維運發送器的替身：記下交出的 (key, 文字)。gate 設了的話，ops-alert 執行緒的 send() 會卡在 gate 上。"""

    def __init__(self, order=None, accept=True):
        self.order = order if order is not None else []
        self.accept = accept
        self.sent = []
        self.gate = None
        self.entered = threading.Event()
        self.raise_exc = None
        self.handler_at_stop = None

    def start(self):
        self.order.append("ops.start")

    def send(self, text, key=None, **kw):
        if self.gate is not None and threading.current_thread().name == OA.THREAD_NAME:
            self.entered.set()
            self.gate.wait(10)
        self.sent.append((key, text))
        self.order.append("ops.send:%s" % key)
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.accept

    def stop(self, timeout=None):
        self.handler_at_stop = any(isinstance(h, OA._AlertHandler) for h in logging.getLogger().handlers)
        self.order.append("ops.stop")
        return 0

    def stats(self):
        return {"worker_alive": False}

    def kinds(self):
        return [k[len("ops-"):] for k, _ in self.sent]

    def texts(self, kind):
        return [t for k, t in self.sent if k == "ops-" + kind]


class EpochClock:
    def __init__(self, t=T0):
        self.t = t
        self.fail = False

    def __call__(self):
        if self.fail:
            raise RuntimeError("假時鐘故障")
        return self.t


class Taker:
    """每次呼叫回傳上一次之後新交出的 [(種類, 文字)]。"""

    def __init__(self, sender):
        self.sender = sender
        self.n = 0

    def __call__(self):
        new = self.sender.sent[self.n:]
        self.n = len(self.sender.sent)
        return [(k[len("ops-"):], t) for k, t in new]


@contextlib.contextmanager
def seen_path_in(tmp):
    """A6：新上架追蹤檔（心跳唯讀開）也導到暫存目錄，不可以讀到 repo 的 runtime/ 裡的真實檔案。"""
    saved = config.UNIVERSE_SEEN_DB_PATH
    config.UNIVERSE_SEEN_DB_PATH = os.path.join(tmp, "db", "universe_seen.sqlite3")
    try:
        yield config.UNIVERSE_SEEN_DB_PATH
    finally:
        config.UNIVERSE_SEEN_DB_PATH = saved


@contextlib.contextmanager
def ops_env():
    """暫存目錄的 outbox / 報表發送紀錄 / 新上架追蹤檔 + 三個假密鑰 + 離線籠子。"""
    with tempdir() as tmp, outbox_path_in(tmp), seen_path_in(tmp):
        with tgt.env_vars({config.TG_BOT_TOKEN_ENV: FAKE_TOKEN, config.TG_CHANNEL_ID_ENV: FAKE_CHAT,
                           config.OPS_CHAT_ID_ENV: FAKE_OPS}):
            with OfflineCage() as cage:
                yield tmp
            assert not cage.attempts, "有程式企圖連網：%r" % cage.attempts
            assert not cage.sleeps, "有程式呼叫了真的 time.sleep：%r" % cage.sleeps


def _shutdown_alerter(a):
    sender = a._sender
    if getattr(sender, "gate", None) is not None:
        sender.gate.set()
    a._stop_requested = True
    a._wake.set()
    if a._thread is not None:
        a._thread.join(10)
    a._remove_handler()


@contextlib.contextmanager
def make_alerter(clock, sender=None, run_thread=False, outbox_path=None):
    a = OA.OpsAlerter(sender=sender if sender is not None else FakeSender(), clock=clock, label=LABEL,
                      outbox_path=outbox_path)
    a.start(run_thread=run_thread)
    try:
        yield a
    finally:
        _shutdown_alerter(a)


def emit(clock, t, level, msg, *, name=TEST_LOGGER, lineno=42, thread="MainThread", filename="a4.py"):
    """在假時鐘 T0 + t 做出一筆 record 交給 logging（與真的 logger.error 走同一條 handler 路徑）。"""
    clock.t = T0 + t
    rec = logging.LogRecord(name, level, filename, lineno, msg, None, None)
    rec.threadName = thread
    logging.getLogger(name).handle(rec)


def tick(a, clock, t, bar=True):
    clock.t = T0 + t
    if bar:
        a.note_bar(None)
    a.run_once(T0 + t)


# ---- 各元件的替身（AC-4 / AC-5）
class FTracker:
    def __init__(self):
        self.alive = True
        self.fail = False
        self.st = {"fetch_failures": 0, "entries": 0, "exits": 0, "store_failures": 0}

    @property
    def worker_alive(self):
        return self.alive

    @property
    def stats(self):
        if self.fail:
            raise RuntimeError("A3 stats 讀不到")
        return dict(self.st)


class FS5:
    def __init__(self):
        self.alive = True
        self.st = {"minutes": 0, "degraded": 0, "missed": 0, "signals": 0}

    def stats(self):
        return dict(self.st, worker_alive=self.alive, worker_crashed=False)


class FSenderStats:
    def __init__(self, alive=True):
        self.alive = alive

    def stats(self):
        return {"worker_alive": self.alive}


class FPush:
    def __init__(self, path):
        self.path = path
        with OB.open_outbox(path):
            pass
        self.alive = True
        self.sender = FSenderStats()

    @property
    def worker_alive(self):
        return self.alive


class FReporter:
    def __init__(self):
        self.st = {"worker_alive": True, "compose_errors": 0, "skipped": 0, "pending": 0, "delivered": 0,
                   "worker_crashed": False}
        self.fail = False
        self.path = "reports.sqlite3"

    def stats(self):
        if self.fail:
            raise RuntimeError("R-A stats 讀不到")
        return dict(self.st)


class FGate:
    def __init__(self):
        self.st = {"requests": 0, "http_429": 0}
        self.fail = False

    def stats(self):
        if self.fail:
            raise RuntimeError("gate stats 讀不到")
        return dict(self.st)


class FReconciler:
    def __init__(self):
        self._thread = None
        self.alive = True

    @property
    def worker_alive(self):
        return self.alive


class FFeed:
    def __init__(self, gate, reconciler):
        self.gate = gate
        self.reconciler = reconciler


class World:
    def __init__(self, a, clock, sender, tracker, s5, push, reporter, gate, reconciler):
        self.a, self.clock, self.sender = a, clock, sender
        self.tracker, self.s5, self.push, self.reporter = tracker, s5, push, reporter
        self.gate, self.reconciler = gate, reconciler

    def advance(self, t, *, bar=True, minutes=1):
        """時鐘走到 T0 + t、（預設）收到一根 K 棒、A5 多處理 minutes 分鐘，跑一輪。回傳這一輪交出的 [(種類, 文字)]。"""
        self.clock.t = T0 + t
        if bar:
            self.a.note_bar(None)
        self.s5.st["minutes"] += minutes
        n = len(self.sender.sent)
        self.a.run_once(T0 + t)
        return [(k[len("ops-"):], text) for k, text in self.sender.sent[n:]]


@contextlib.contextmanager
def world(attach=True):
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender) as a:
            tracker, s5, reporter, gate, reconciler = FTracker(), FS5(), FReporter(), FGate(), FReconciler()
            push = FPush(config.A_CHANNEL_OUTBOX_DB_PATH)
            universe_seen.open_db(config.UNIVERSE_SEEN_DB_PATH).close()     # A6：空的追蹤檔 → 心跳「新上架：無」
            if attach:
                a.attach(tracker=tracker, push=push, reporter=reporter)
                a.attach(feed=FFeed(gate, reconciler), s5=s5)
            yield World(a, clock, sender, tracker, s5, push, reporter, gate, reconciler)


def add_pending(path, signal_id, received_s, kind=OB.KIND_ENTRY):
    with OB.open_outbox(path) as ob:
        ob.insert(signal_id=signal_id, kind=kind, strategy="s4", symbol="ACE_USDT_PERP", event={}, snapshot={},
                  received_ms=int(received_s * 1000))


def finalize(path, signal_id, status, now_s, kind=OB.KIND_ENTRY):
    with OB.open_outbox(path) as ob:
        ob.finalize(ob.get(signal_id, kind)["seq"], status, now_ms=int(now_s * 1000))


def _threads(name):
    return [t for t in threading.enumerate() if t.name == name and t.is_alive()]


# ============================== AC-1：ChannelSender 的三個新參數 ==============================
def test_ac1_default_sender_is_the_a_channel_sender():
    with harness() as h:
        s = h.started()
        assert s._chat_id_env == config.TG_CHANNEL_ID_ENV
        assert (s._thread_name, s._log_label) == ("tg-channel-sender", "A 頻道")
        assert _threads("tg-channel-sender") and not _threads(OA.SENDER_THREAD_NAME)
        assert s.send("hi", key="k") is True
        wait_until(lambda: len(h.tg.calls) == 1, "A 頻道發送器送出一則")
        stop_within(s, 60)
        assert h.tg.calls[0].payload["chat_id"] == FAKE_CHAT
        assert any("A 頻道發送器已啟動" in line for line in h.logs.at(logging.INFO))


def test_ac1_ops_sender_reads_its_own_chat_id_masks_it_and_logs_its_label():
    with harness() as h, tgt.env_vars({config.OPS_CHAT_ID_ENV: FAKE_OPS}), capture("live.tg_channel") as cap:
        h.tg.plan(ok(), http_error(400, "Bad Request: chat " + FAKE_OPS + " not found"))
        s = h.started(chat_id_env=config.OPS_CHAT_ID_ENV, thread_name=OA.SENDER_THREAD_NAME,
                      log_label=OA.SENDER_LOG_LABEL)
        assert _threads(OA.SENDER_THREAD_NAME) and not _threads("tg-channel-sender")
        assert s.send("one", key="ops-a") is True and s.send("two", key="ops-b") is True
        assert s.send("", key="ops-c") is False
        wait_until(lambda: len(h.tg.calls) == 2, "維運發送器送出兩則")
        stop_within(s, 60)
        assert not _threads(OA.SENDER_THREAD_NAME), "stop() 之後 tg-ops-sender 還活著"
        assert [c.payload["chat_id"] for c in h.tg.calls] == [FAKE_OPS, FAKE_OPS]
        infos = cap.messages(logging.INFO)
        errors = cap.messages(logging.ERROR)
        assert any("維運告警發送器已啟動" in m for m in infos), infos
        assert any("維運告警訊息已送出" in m for m in infos), infos
        assert any("維運告警訊息送出失敗" in m for m in errors), errors
        all_msgs = cap.messages()
        assert not any("A 頻道" in m for m in all_msgs), [m for m in all_msgs if "A 頻道" in m]
        assert not find_leaks(all_msgs + h.logs.lines, SECRETS), "維運發送器的日誌有密鑰片段"
        assert cap.records
        for r in cap.records:
            src = linecache.getline(r.pathname, r.lineno).strip()
            assert r.funcName != "_log" and src.startswith("self._log("), (r.funcName, r.lineno, src)


def test_ac1_new_sender_uses_the_ops_parameters():
    s = OA.new_sender()
    assert (s._chat_id_env, s._thread_name, s._log_label) == \
        (config.OPS_CHAT_ID_ENV, "tg-ops-sender", "維運告警")
    assert config.OPS_CHAT_ID_ENV == "CRYPTO_TRADER_TG_OPS_CHAT_ID"


def test_ac1_missing_ops_chat_id_fails_at_start_without_values():
    with harness() as h, tgt.env_vars({config.OPS_CHAT_ID_ENV: None}):
        s = h.sender(chat_id_env=config.OPS_CHAT_ID_ENV, thread_name=OA.SENDER_THREAD_NAME,
                     log_label=OA.SENDER_LOG_LABEL)
        try:
            s.start()
        except config.MissingSecretError as e:
            assert config.OPS_CHAT_ID_ENV in str(e)
            assert not find_leaks(exception_texts(e), SECRETS)
        else:
            raise AssertionError("缺維運 chat id 應該在 start() 拋 MissingSecretError")
        assert not _threads(OA.SENDER_THREAD_NAME)


# ============================== AC-2：handler ==============================
def test_ac2_ten_thousand_emits_never_block_while_the_alert_thread_is_stuck():
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender, run_thread=True) as a:
            clock.t = T0 + 200
            sender.gate = threading.Event()
            emit(clock, 200, logging.CRITICAL, "卡住發送器的那一則", lineno=9)
            wait_until(sender.entered.is_set, "告警執行緒卡在 send()")
            worst = 0.0
            for i in range(10000):
                rec = logging.LogRecord(TEST_LOGGER, logging.ERROR, "a4.py", 1000 + i, "錯誤 %d" % i, None, None)
                t0 = time.perf_counter()
                logging.getLogger(TEST_LOGGER).handle(rec)
                worst = max(worst, time.perf_counter() - t0)
            assert worst < 0.25, "emit() 最慢一次 %.3f 秒" % worst
            assert a.counters() == (10001, 0, 9000), a.counters()
            with a._qlock:
                linenos = [e[4] for e in a._queue]
            assert linenos == list(range(10000, 11000))
            sender.gate.set()
            wait_until(lambda: len(a._pending) == 1000, "告警執行緒收下佇列裡的 1000 筆", limit=10.0)
            rc = a.finish("Ctrl+C", 0)
            assert rc == 0
            assert sender.kinds() == ["alert", "shutdown"], sender.kinds()
            text = sender.texts("shutdown")[0]
            for want in ("日誌佇列丟棄：9000 筆", "…另有 990 項（見日誌）", "關閉前還沒發出的告警：1000 項（一併列在下面）"):
                assert want in text, (want, text)


def test_ac2_records_from_the_alert_threads_and_its_own_logger_are_excluded():
    with ops_env():
        clock = EpochClock()
        with make_alerter(clock) as a:
            emit(clock, 1, logging.ERROR, "x", thread=OA.THREAD_NAME, lineno=1)
            emit(clock, 1, logging.ERROR, "x", thread=OA.SENDER_THREAD_NAME, lineno=2)
            emit(clock, 1, logging.ERROR, "x", name=OA.LOGGER_NAME, lineno=3)
            for name in (OA.THREAD_NAME, OA.SENDER_THREAD_NAME):
                th = threading.Thread(target=lambda: logging.getLogger(TEST_LOGGER).error("真執行緒的錯誤"),
                                      name=name)
                th.start()
                th.join(5)
            with a._qlock:
                assert not a._queue, list(a._queue)
            emit(clock, 2, logging.ERROR, "對照組", lineno=4)
            for i in range(3):
                emit(clock, 3, logging.WARNING, "只計數", lineno=5)
            with a._qlock:
                queued = [(e[2], e[4], e[6]) for e in a._queue]
            assert queued == [(TEST_LOGGER, 4, "對照組")], queued
            assert a.counters() == (6, 3, 0), a.counters()
        with make_alerter(clock) as b:
            b._wake.clear()
            emit(clock, 4, logging.ERROR, "第一筆", lineno=6)
            assert b._wake.is_set(), "佇列由空變非空要喚醒告警執行緒"
            b._wake.clear()
            emit(clock, 4, logging.ERROR, "第二筆", lineno=7)
            assert not b._wake.is_set(), "佇列本來就非空的 ERROR 不必喚醒"
            emit(clock, 4, logging.CRITICAL, "嚴重", lineno=8)
            assert b._wake.is_set(), "CRITICAL 一定喚醒"


def test_ac2_sender_failures_do_not_feed_back_into_the_queue():
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        sender.raise_exc = RuntimeError("x")
        with make_alerter(clock, sender, run_thread=True) as a:
            clock.t = T0 + 200
            emit(clock, 200, logging.CRITICAL, "觸發", lineno=11)
            wait_until(lambda: a.counters()[0] == 2, "交出失敗記一筆 ERROR")
            with a._qlock:
                assert not a._queue, list(a._queue)
            rc = a.finish("Ctrl+C", 0)
            assert rc == 0
            assert len(sender.sent) == 2, sender.kinds()
            assert "關閉過程中的 ERROR：無" in sender.texts("shutdown")[0]
            with a._qlock:
                assert not a._queue


def test_ac2_all_three_secrets_are_masked_before_and_after_clipping():
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender) as a:
            tick(a, clock, 200)
            emit(clock, 200, logging.ERROR, "x" * 185 + FAKE_TOKEN, lineno=1)
            emit(clock, 200, logging.ERROR, "chat " + FAKE_CHAT, lineno=2)
            emit(clock, 200, logging.ERROR, "ops " + FAKE_OPS, lineno=3)
            emit(clock, 200, logging.ERROR, config.TG_API_BASE_URL + "/bot" + FAKE_TOKEN + "/sendMessage", lineno=4)
            tick(a, clock, 230)
            a.finish("結束 " + FAKE_OPS, 0)
        texts = [t for _, t in sender.sent]
        assert sender.kinds() == ["alert", "shutdown"], sender.kinds()
        assert not find_leaks(texts, SECRETS), "訊息裡有密鑰片段"
        assert all(MASK in t for t in texts)
        assert texts[0].split("\n")[2] == "・[ERROR] live.test_a4 L1 ×1：" + "x" * 185 + MASK


# ============================== AC-3：事件告警 ==============================
def test_ac3_new_keys_batch_remind_and_recover():
    with ops_env(), capture("live.ops_alert") as cap:
        clock = EpochClock()
        sender = FakeSender()
        take = Taker(sender)
        with make_alerter(clock, sender) as a:
            tick(a, clock, 200)
            assert take() == []
            emit(clock, 200, logging.ERROR, "甲 失敗 #1", lineno=42)
            emit(clock, 205, logging.ERROR, "甲 失敗 #2", lineno=42)
            emit(clock, 210, logging.ERROR, "乙 失敗", lineno=43)
            tick(a, clock, 229)
            assert take() == []
            tick(a, clock, 230)
            assert take() == [("alert", RED + " 告警  test-host\n時間：2026-09-22 10:03:50\n"
                                        "・[ERROR] live.test_a4 L42 ×2：甲 失敗 #2\n"
                                        "・[ERROR] live.test_a4 L43 ×1：乙 失敗")]
            for i, t in ((3, 1000), (4, 2000), (5, 3000)):
                emit(clock, t, logging.ERROR, "甲 失敗 #%d" % i, lineno=42)
            tick(a, clock, 3829)
            assert take() == []
            tick(a, clock, 3830)
            assert take() == [("remind", ALARM + " 仍未恢復  test-host\n時間：2026-09-22 11:03:50\n"
                                         "・[ERROR] live.test_a4 L42 ×5：過去 1 小時又發生 3 次；最近一次：甲 失敗 #5")]
            tick(a, clock, 7430)
            assert take() == [("recovered", CHECK + " 已恢復  test-host\n時間：2026-09-22 12:03:50\n"
                                            "・[ERROR] live.test_a4 L42 ×5：過去 1 小時沒有再發生（共 5 次）；甲 失敗 #5")]
            tick(a, clock, 11030)
            assert take() == []
        handed = [m for m in cap.messages(logging.INFO) if "已交出" in m]
        assert len(handed) == 3, handed


def test_ac3_rejected_or_raising_sender_is_logged():
    with ops_env(), capture("live.ops_alert") as cap:
        clock = EpochClock()
        sender = FakeSender(accept=False)
        with make_alerter(clock, sender) as a:
            tick(a, clock, 200)
            emit(clock, 200, logging.ERROR, "被拒收", lineno=1)
            tick(a, clock, 230)
            assert any("沒有交出" in m for m in cap.messages(logging.WARNING)), cap.messages()
            assert a._sent["alert"] == 0
            assert not any("已交出" in m for m in cap.messages(logging.INFO))
        clock = EpochClock()          # 新的告警器從 T0 起算寬限
        sender = FakeSender()
        sender.raise_exc = RuntimeError("壞掉")
        with make_alerter(clock, sender) as a:
            tick(a, clock, 200)
            emit(clock, 200, logging.ERROR, "拋例外", lineno=2)
            tick(a, clock, 230)
            assert any("維運告警交給發送器時拋出例外" in m for m in cap.messages(logging.ERROR)), cap.messages()


def test_ac3_errors_during_startup_grace_become_one_yellow_message():
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        take = Taker(sender)
        with make_alerter(clock, sender) as a:
            emit(clock, 10, logging.ERROR, "丙 #1", lineno=50)
            emit(clock, 20, logging.ERROR, "丙 #2", lineno=50)
            emit(clock, 30, logging.ERROR, "丁", lineno=51)
            tick(a, clock, 60)
            tick(a, clock, 179)
            assert take() == []
            tick(a, clock, 180)
            assert take() == [("grace", YELLOW + " 啟動期間的錯誤  test-host\n時間：2026-09-22 10:03:00\n"
                                        "・[ERROR] live.test_a4 L50 ×2：丙 #2\n"
                                        "・[ERROR] live.test_a4 L51 ×1：丁")]
            emit(clock, 200, logging.ERROR, "丙 #3", lineno=50)
            tick(a, clock, 3779)
            assert take() == []
            tick(a, clock, 3780)
            got = take()
            assert [k for k, _ in got] == ["remind"], got
            assert "・[ERROR] live.test_a4 L50 ×3：過去 1 小時又發生 1 次；最近一次：丙 #3" in got[0][1], got
            assert "L51" not in got[0][1]


def test_ac3_critical_bypasses_grace_and_batching():
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        take = Taker(sender)
        with make_alerter(clock, sender) as a:
            emit(clock, 10, logging.CRITICAL, "嚴重 甲", lineno=60)
            tick(a, clock, 11)
            got = take()
            assert got == [("alert", message("alert", 11, "・[CRITICAL] live.test_a4 L60 ×1：嚴重 甲"))], got
            emit(clock, 20, logging.ERROR, "乙 先是 ERROR", lineno=61)
            tick(a, clock, 21)
            assert take() == []
            emit(clock, 22, logging.CRITICAL, "乙 變成 CRITICAL", lineno=61)
            tick(a, clock, 23)
            got = take()
            assert got == [("alert", message("alert", 23, "・[CRITICAL] live.test_a4 L61 ×2：乙 變成 CRITICAL"))], got
            tick(a, clock, 180)
            assert take() == [], "寬限內的鍵已經整個移到 🔴，寬限結束不該再發 🟡"
            emit(clock, 300, logging.ERROR, "丙", lineno=62)
            tick(a, clock, 301)
            assert take() == []
            emit(clock, 305, logging.CRITICAL, "丁", lineno=63)
            tick(a, clock, 306)
            got = take()
            assert got == [("alert", message("alert", 306, "・[ERROR] live.test_a4 L62 ×1：丙",
                                             "・[CRITICAL] live.test_a4 L63 ×1：丁"))], got
            emit(clock, 400, logging.ERROR, "戊", lineno=64)
            tick(a, clock, 401)
            assert take() == []
            emit(clock, 402, logging.CRITICAL, "戊 嚴重", lineno=64)
            tick(a, clock, 403)
            got = take()
            assert got == [("alert", message("alert", 403, "・[CRITICAL] live.test_a4 L64 ×2：戊 嚴重"))], got
            assert sender.kinds().count("alert") == 4


def test_ac3_errors_during_shutdown_go_into_the_shutdown_message():
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        take = Taker(sender)
        with make_alerter(clock, sender) as a:
            tick(a, clock, 200)
            emit(clock, 300, logging.ERROR, "己", lineno=70)
            tick(a, clock, 301)
            a.begin_shutdown()
            emit(clock, 310, logging.ERROR, "庚 #1", lineno=71)
            emit(clock, 310, logging.ERROR, "庚 #2", lineno=71)
            emit(clock, 310, logging.ERROR, "辛", lineno=72)
            tick(a, clock, 340)
            assert take() == []
            clock.t = T0 + 400
            rc = a.finish("Ctrl+C", 0)
            assert rc == 0
            assert take() == [("shutdown", WHITE + " A 頻道程式結束  test-host\n時間：2026-09-22 10:06:40\n"
                                           "運作時間：6 分 40 秒（自 2026-09-22 10:00:00 起）\n"
                                           "結束原因：Ctrl+C\n"
                                           "exit code：0\n"
                                           "outbox 待送：—（outbox 還沒建立）\n"
                                           "仍未恢復的條件：無\n"
                                           "日誌佇列丟棄：0 筆\n"
                                           "關閉過程中的 ERROR：3 筆\n"
                                           "關閉前還沒發出的告警：1 項（一併列在下面）\n"
                                           "・[ERROR] live.test_a4 L71 ×2：庚 #2\n"
                                           "・[ERROR] live.test_a4 L72 ×1：辛\n"
                                           "・[ERROR] live.test_a4 L70 ×1：己")]
            assert sender.handler_at_stop is False, "拆 handler 要在維運發送器 stop() 之前"


def test_ac3_grace_items_are_folded_into_the_shutdown_message():
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender) as a:
            emit(clock, 10, logging.ERROR, "壬 #1", lineno=80)
            tick(a, clock, 15)
            a.begin_shutdown()
            emit(clock, 30, logging.ERROR, "壬 #2", lineno=80)
            assert a.finish("Ctrl+C", 0) == 0
        assert sender.kinds() == ["shutdown"], sender.kinds()
        text = sender.texts("shutdown")[0]
        for want in ("・[ERROR] live.test_a4 L80 ×2：壬 #2", "關閉過程中的 ERROR：1 筆",
                     "關閉前還沒發出的告警：1 項（一併列在下面）"):
            assert want in text, (want, text)


# ============================== AC-4：條件 ==============================
C1_CASES = (("A3", lambda w, v: setattr(w.tracker, "alive", v)),
            ("A5", lambda w, v: setattr(w.s5, "alive", v)),
            ("T2 pusher", lambda w, v: setattr(w.push, "alive", v)),
            ("A 頻道發送器", lambda w, v: setattr(w.push.sender, "alive", v)),
            ("R-A", lambda w, v: w.reporter.st.__setitem__("worker_alive", v)),
            ("對帳", lambda w, v: setattr(w.reconciler, "alive", v)))


def test_ac4_c1_each_component_raises_reminds_hourly_and_clears():
    for label, set_alive in C1_CASES:
        head = "・[條件] C1 執行緒不在（%s）：" % label
        with world() as w:
            w.reconciler._thread = object()
            assert w.advance(540) == [], label
            set_alive(w, False)
            assert w.advance(600) == [("alert", message("alert", 600, head + DEAD))], label
            assert w.advance(4199) == [], label
            assert w.advance(4200) == [("remind", message("remind", 4200, head + "仍未恢復，已持續 1 小時；" + DEAD))]
            assert w.advance(7800) == [("remind", message("remind", 7800, head + "仍未恢復，已持續 2 小時；" + DEAD))]
            set_alive(w, True)
            assert w.advance(8400) == [("recovered", message("recovered", 8400, head + "已恢復，持續了 2 小時 10 分"))]
            assert w.advance(12000) == [], label


def test_ac4_c2_a1_stall():
    head = "・[條件] C2 A1 停擺："
    with world() as w:
        assert w.advance(600) == []
        assert w.advance(1260, bar=False) == []
        assert w.advance(1320, bar=False) == [("alert", message(
            "alert", 1320, head + "已經 12 分沒有收到新的 K 棒結果（上限 11 分）"))]
        assert w.advance(4920, bar=False) == [("remind", message(
            "remind", 4920, head + "仍未恢復，已持續 1 小時；已經 1 小時 12 分沒有收到新的 K 棒結果（上限 11 分）"))]
        assert w.advance(8520, bar=False) == [("remind", message(
            "remind", 8520, head + "仍未恢復，已持續 2 小時；已經 2 小時 12 分沒有收到新的 K 棒結果（上限 11 分）"))]
        assert w.advance(9000) == [("recovered", message("recovered", 9000, head + "已恢復，持續了 2 小時 8 分"))]
    with world() as w:
        assert w.advance(660, bar=False) == []
        assert w.advance(720, bar=False) == [("alert", message(
            "alert", 720, head + "啟動後已經 12 分還沒收到第一根 K 棒結果（上限 11 分）"))]
        assert w.advance(780) == [("recovered", message("recovered", 780, head + "已恢復，持續了 1 分"))]


def test_ac4_c3_a5_stall():
    head = "・[條件] C3 A5 停擺："
    with world() as w:
        assert w.advance(540) == []
        assert w.advance(600, minutes=9) == []
        assert w.advance(780, minutes=0) == []
        assert w.advance(840, minutes=0) == [("alert", message(
            "alert", 840, head + "處理的分鐘數已經 4 分沒有增加（上限 3 分；目前累計 10 分鐘）"))]
        assert w.advance(4440, minutes=0) == [("remind", message(
            "remind", 4440, head + "仍未恢復，已持續 1 小時；處理的分鐘數已經 1 小時 4 分沒有增加（上限 3 分；目前累計 10 分鐘）"))]
        assert w.advance(8040, minutes=0) == [("remind", message(
            "remind", 8040, head + "仍未恢復，已持續 2 小時；處理的分鐘數已經 2 小時 4 分沒有增加（上限 3 分；目前累計 10 分鐘）"))]
        assert w.advance(8100, minutes=1) == [("recovered", message("recovered", 8100, head + "已恢復，持續了 2 小時 1 分"))]


def test_ac4_c4_outbox_stuck():
    head = "・[條件] C4 A 頻道待送卡住："
    with world() as w:
        assert w.advance(540) == []
        add_pending(w.push.path, "s4-C4-1", T0 + 600)
        assert w.advance(1200) == []
        assert w.advance(1260) == [("alert", message(
            "alert", 1260, head + "最舊的待送列已經 11 分沒送出（上限 10 分；共 1 則待送）"))]
        assert w.advance(4860) == [("remind", message(
            "remind", 4860, head + "仍未恢復，已持續 1 小時；最舊的待送列已經 1 小時 11 分沒送出（上限 10 分；共 1 則待送）"))]
        assert w.advance(8460) == [("remind", message(
            "remind", 8460, head + "仍未恢復，已持續 2 小時；最舊的待送列已經 2 小時 11 分沒送出（上限 10 分；共 1 則待送）"))]
        finalize(w.push.path, "s4-C4-1", OB.STATUS_DELIVERED, T0 + 8900)
        assert w.advance(9000) == [("recovered", message("recovered", 9000, head + "已恢復，持續了 2 小時 9 分"))]


def _counter_values(t):
    if t < 120:
        return 0
    table = {120: 1, 180: 2, 240: 2, 300: 3, 360: 4, 420: 5}
    if t in table:
        return table[t]
    return min(35, 5 + (t - 420) // 240)


def test_ac4_c5_and_c6_counter_streaks():
    for key, name, what in (("C6", "C6 A3 取數持續失敗", "A3 取數失敗"), ("C5", "C5 報表組不出來", "組報表失敗")):
        head = "・[條件] %s：" % name
        with world() as w:
            got = []
            desc_7860 = None
            for t in range(60, 7921, 60):
                v = _counter_values(t)
                if key == "C6":
                    w.tracker.st["fetch_failures"] = v
                else:
                    w.reporter.st["compose_errors"] = v
                new = w.advance(t)
                if new:
                    got.append((t, new))
                if t == 7860:
                    desc_7860 = w.a._raised[key].desc
            assert got == [
                (420, [("alert", message("alert", 420, head + what + "連續 3 輪輪詢都有增加（累計 5 次）"))]),
                (4020, [("remind", message("remind", 4020, head + "仍未恢復，已持續 1 小時；"
                                           + what + "連續 1 輪輪詢都有增加（累計 20 次）"))]),
                (7620, [("remind", message("remind", 7620, head + "仍未恢復，已持續 2 小時；"
                                           + what + "連續 1 輪輪詢都有增加（累計 35 次）"))]),
                (7920, [("recovered", message("recovered", 7920, head + "已恢復，持續了 2 小時 5 分"))]),
            ], (key, got)
            assert desc_7860 == what + "累計 35 次，已連續 4 輪輪詢沒有增加（連續 5 輪沒有增加才算恢復）", desc_7860
            assert w.sender.kinds() == ["alert", "remind", "remind", "recovered"], w.sender.kinds()


def test_ac4_components_that_never_started_are_not_reported():
    with world(attach=False) as w:
        w.tracker.alive = w.s5.alive = w.push.alive = w.reconciler.alive = False
        w.reporter.st["worker_alive"] = False
        assert w.advance(600) == [] and w.advance(1200) == []
        assert w.sender.sent == []
    with world() as w:
        w.reconciler.alive = False
        assert w.reconciler._thread is None
        assert w.advance(600) == [], "對帳執行緒還沒啟動（_thread is None）不算不在"
        w.reconciler._thread = object()
        assert w.advance(660) == [("alert", message("alert", 660, "・[條件] C1 執行緒不在（對帳）：" + DEAD))]


def test_ac4_c4_unreadable_outbox_neither_raises_nor_clears():
    with world() as w, capture("live.ops_alert") as cap:
        add_pending(w.push.path, "s4-C4-2", T0)
        assert [k for k, _ in w.advance(660)] == ["alert"]
        assert "C4" in w.a._raised
        tmp = os.path.dirname(w.push.path)
        w.a._outbox_path = os.path.join(tmp, "missing.sqlite3")
        assert w.advance(720) == [] and w.advance(780) == []
        assert "C4" in w.a._raised
        warns = [m for m in cap.messages(logging.WARNING) if "維運告警讀不到 outbox（ReportError" in m]
        assert len(warns) == 1, cap.messages(logging.WARNING)
        garbage = os.path.join(tmp, "garbage.sqlite3")
        with open(garbage, "wb") as f:
            f.write(b"this is not a database " * 100)
        w.a._outbox_path = garbage
        assert w.advance(840) == [] and w.advance(900) == []
        warns = [m for m in cap.messages(logging.WARNING) if "DatabaseError" in m]
        assert len(warns) == 1, cap.messages(logging.WARNING)
        assert "C4" in w.a._raised
        w.a._outbox_path = None
        assert w.advance(960) == [] and "C4" in w.a._raised
        finalize(w.push.path, "s4-C4-2", OB.STATUS_DELIVERED, T0 + 1000)
        assert w.advance(1020) == [("recovered", message(
            "recovered", 1020, "・[條件] C4 A 頻道待送卡住：已恢復，持續了 6 分"))]
    with world() as w:
        add_pending(w.push.path, "s4-C4-3", T0)
        w.a._outbox_path = os.path.join(os.path.dirname(w.push.path), "missing.sqlite3")
        assert w.advance(660) == [] and w.advance(720) == []
        assert "C4" not in w.a._raised


def test_ac4_fr4_one_shot_events():
    with world() as w:
        w.a._wake.clear()
        w.a.note_reconcile(SimpleNamespace(out_of_coverage=False))
        assert not w.a._wake.is_set()
        for _ in range(3):
            w.a.note_reconcile(SimpleNamespace(out_of_coverage=True))
        assert w.a._wake.is_set()
        w.a.note_reconcile(None)          # 怪輸入也不拋
        assert w.advance(240) == [] and w.advance(269) == []
        assert w.advance(270) == [("alert", message(
            "alert", 270, "・[事件] 對帳：3 個小時窗口超出 klines 涵蓋，沒有對帳（多半是停頓過久）"))]
        w.reporter.st["skipped"] = 2
        assert w.advance(300) == []
        assert w.advance(330) == [("alert", message("alert", 330, "・[事件] 報表：2 期超過補發上限，跳過未發"))]
        w.reporter.st["skipped"] = 3
        assert w.advance(360) == []
        assert w.advance(390) == [("alert", message("alert", 390, "・[事件] 報表：1 期超過補發上限，跳過未發"))]
        assert w.advance(500) == []


# ============================== AC-5：心跳 ==============================
R = ("fetch_failed:api_error", "fetch_failed:banned", "late_bar", "buffer_gap")


def test_ac5_three_heartbeats_with_exact_deltas():
    windows = ((1, 46), (47, 94), (95, 142))
    acc = [collections.Counter() for _ in windows]
    reasons = [collections.Counter() for _ in windows]
    with world() as w:
        a, clock = w.a, w.clock
        for i in range(1, 143):
            wi = 0 if i <= 46 else (1 if i <= 94 else 2)
            c = acc[wi]
            now = T0 + 1800 * i
            clock.t = now
            bar_reasons = [[R[0]], [R[1]] if i % 2 == 0 else [], [R[2]] if i % 5 == 0 else [],
                           [R[3]] if i % 11 == 0 else []]
            for rs in bar_reasons:
                a.note_bar(SimpleNamespace(degraded_reasons=rs, signals=[]))
                reasons[wi].update(rs)
            a.note_bar(SimpleNamespace(degraded_reasons=[], signals=[1] if i % 3 == 0 else []))
            a.note_bar(SimpleNamespace(degraded_reasons=[], signals=[]))
            c["bars"] += 6
            c["degraded"] += sum(1 for rs in bar_reasons if rs)
            c["s4_signals"] += i % 3 == 0
            for k, inc in (("minutes", 30), ("degraded", i % 3 == 0), ("missed", i % 7 == 0),
                           ("signals", i % 4 == 0)):
                w.s5.st[k] += inc
                c["s5_" + k] += inc
            for k, inc in (("entries", i % 4 == 0), ("exits", i % 6 == 0), ("store_failures", i % 40 == 0)):
                w.tracker.st[k] += inc
                c[k] += inc
            with OB.open_outbox(w.push.path) as ob:
                for tag, kind, status, cond in (("ed", OB.KIND_ENTRY, OB.STATUS_DELIVERED, i % 10 == 0),
                                                ("xd", OB.KIND_EXIT, OB.STATUS_DELIVERED, i % 15 == 0),
                                                ("ee", OB.KIND_ENTRY, OB.STATUS_EXPIRED, i % 33 == 0),
                                                ("ef", OB.KIND_ENTRY, OB.STATUS_FAILED, i % 45 == 0)):
                    if cond:
                        sid = "hb-%d-%s" % (i, tag)
                        ob.insert(signal_id=sid, kind=kind, strategy="s4", symbol="ACE_USDT_PERP", event={},
                                  snapshot={}, received_ms=int(now * 1000))
                        ob.finalize(ob.get(sid, kind)["seq"], status, now_ms=int(now * 1000))
                        c[tag] += 1
            w.reporter.st["delivered"] += i % 48 == 0
            c["ra_delivered"] += i % 48 == 0
            if i == 70:
                w.reporter.st["skipped"] += 1
                c["ra_skipped"] += 1
            if i >= 120:
                w.reporter.st["worker_alive"] = False
            w.gate.st["requests"] += 100 + i
            w.gate.st["http_429"] += i % 50 == 0
            c["requests"] += 100 + i
            c["http_429"] += i % 50 == 0
            if i % 9 == 0:
                emit(clock, 1800 * i, logging.WARNING, "只計數的 WARNING", lineno=90)
                c["warnings"] += 1
            if i == 60:
                emit(clock, 1800 * i, logging.ERROR, "心跳期間的 ERROR", lineno=91)
                c["errors"] += 1
            a.run_once(now)
        assert w.sender.kinds() == ["heartbeat", "alert", "alert", "heartbeat", "alert"] + ["remind"] * 11 + \
            ["heartbeat"], w.sender.kinds()
        hbs = w.sender.texts("heartbeat")
    assert [tuple(n for _, n in r.most_common(3)) for r in reasons] == [(46, 23, 9), (48, 24, 9), (48, 24, 10)]
    spans = ["本次啟動以來 23.0 小時（自 2026-09-22 10:00:00 起）",
             "上一次心跳以來 24.0 小時（自 2026-09-23 09:00:00 起）",
             "上一次心跳以來 24.0 小時（自 2026-09-24 09:00:00 起）"]
    uptimes = ["23 小時", "1 天 23 小時", "2 天 23 小時"]
    times = ["2026-09-23 09:00:00", "2026-09-24 09:00:00", "2026-09-25 09:00:00"]
    alerts = ["發出 0 則（%s 0、%s 0、%s 0、%s 0）；仍未恢復：無" % (RED, YELLOW, ALARM, CHECK),
              "發出 2 則（%s 2、%s 0、%s 0、%s 0）；仍未恢復：無" % (RED, YELLOW, ALARM, CHECK),
              "發出 12 則（%s 1、%s 0、%s 11、%s 0）；仍未恢復：C1 執行緒不在（R-A）" % (RED, YELLOW, ALARM, CHECK)]
    for wi, text in enumerate(hbs):
        c = acc[wi]
        detail = "、".join("%s ×%d" % (r, n) for r, n in reasons[wi].most_common(3))
        want = "\n".join([
            HEART + " 每日心跳  " + LABEL,
            "時間：" + times[wi],
            "統計區間：" + spans[wi],
            "運作時間：%s（自 2026-09-22 10:00:00 起）" % uptimes[wi],
            "策略4 K 棒：處理 %d 根，其中 degraded %d 根（%s）" % (c["bars"], c["degraded"], detail),
            "策略5 分鐘：處理 %d 分鐘，degraded %d、missed %d" % (c["s5_minutes"], c["s5_degraded"], c["s5_missed"]),
            "原始訊號：策略4 %d 筆、策略5 %d 筆（含被持倉或冷卻擋掉的）" % (c["s4_signals"], c["s5_signals"]),
            "A3：進場 %d、出場 %d、資料庫失敗 %d" % (c["entries"], c["exits"], c["store_failures"]),
            "A 頻道：進場送達 %d、延遲不發 %d、永久失敗 %d、出場送達 %d" % (c["ed"], c["ee"], c["ef"], c["xd"]),
            "報表：送達 %d、跳過 %d" % (c["ra_delivered"], c["ra_skipped"]),
            "REST：請求 %d、429 %d 次" % (c["requests"], c["http_429"]),
            "日誌：ERROR %d、WARNING %d、佇列丟棄 0" % (c["errors"], c["warnings"]),
            "告警：" + alerts[wi],
            "新上架（過去 24 小時）：無",                                  # A6（追蹤檔是空的）
        ])
        assert text == want, (wi, text, want)
    assert [acc[i]["warnings"] for i in range(3)] == [5, 5, 5]
    assert [acc[i]["errors"] for i in range(3)] == [0, 1, 0]


def test_ac5_unattached_components_are_shown_as_not_started():
    with world(attach=False) as w:
        tick(w.a, w.clock, 82800)
        texts = w.sender.texts("heartbeat")
        assert len(texts) == 1, w.sender.kinds()
        text = texts[0]
        for want in ("\n策略5 分鐘：—（未啟動）\n",
                     "\n原始訊號：策略4 0 筆、策略5 —（未啟動）（含被持倉或冷卻擋掉的）\n",
                     "\nA3：—（未啟動）\n", "\nA 頻道：—（未啟動）\n", "\n報表：—（未啟動）\n", "\nREST：—（未啟動）\n",
                     "\n策略4 K 棒：處理 1 根，其中 degraded 0 根\n"):
            assert want in text, (want, text)


def test_ac5_unreadable_component_is_skipped_once_and_recovers_next_time():
    with world() as w, capture("live.ops_alert") as cap:
        w.gate.fail = True
        assert [k for k, _ in w.advance(82800)] == ["heartbeat"]
        assert "\nREST：—（讀不到）\n" in w.sender.texts("heartbeat")[0]
        warns = [m for m in cap.messages(logging.WARNING) if "讀 gate 的狀態失敗" in m]
        assert len(warns) == 1, cap.messages(logging.WARNING)
        w.gate.fail = False
        w.gate.st["requests"] = 800
        assert [k for k, _ in w.advance(169200)] == ["heartbeat"]
        assert "\nREST：請求 800、429 0 次\n" in w.sender.texts("heartbeat")[1]


# ============================== AC-6：訊息格式 ==============================
def test_ac6_title_lines_of_all_eight_kinds():
    # A6 加了「新上架」（排在結束之前）
    assert OA.KINDS == ("startup", "grace", "alert", "remind", "recovered", "heartbeat", "listing", "shutdown")
    for kind in OA.KINDS:
        text = OA.compose(kind, T0, LABEL)
        assert text.split("\n") == [TITLE[kind] + "  " + LABEL, "時間：2026-09-22 10:00:00"], text


SAMPLE_X = "SAMPLEX_USDT_PERP"
SAMPLE_X_WHY = "排除（股票／ETF／商品：名稱 X 結尾（長度 ≥ 4、不在 CRYPTO_X_WHITELIST））"
SAMPLE_CNLX_WHY = "納入（加密：在 CRYPTO_X_WHITELIST（例外納入））"


def test_ac6_sample_messages_full_text():
    msgs = OA.sample_messages(now_s=T0, label=LABEL)
    assert [k for k, _ in msgs] == ["ops-sample-%d" % i for i in range(1, 9)]
    ev2 = "・[ERROR] live.notional_tracker L668 ×3："
    fake2 = "[假資料] A3 資料庫寫入失敗：disk I/O error"
    c2 = "・[條件] C2 A1 停擺："
    want = [
        message("startup", 0, "參數：--push-tg、--duration 10800 秒", "outbox 待送：0 則", "報表待送：0 期",
                "標的池：納入 436、排除 128（股票／ETF／商品 123、強掛勾／穩定幣／包裝幣 5）"),
        message("grace", 0, "・[ERROR] live.a_channel_push L512 ×1：[假資料] outbox 記不回結果：database is locked"
                            "（signal_id=s4-ACE_USDT_PERP-202609181535）"),
        message("alert", 0, ev2 + fake2, c2 + "已經 12 分沒有收到新的 K 棒結果（上限 11 分）"),
        message("remind", 0, ev2 + "過去 1 小時又發生 2 次；最近一次：" + fake2,
                c2 + "仍未恢復，已持續 1 小時；已經 12 分沒有收到新的 K 棒結果（上限 11 分）"),
        message("recovered", 0, ev2 + "過去 1 小時沒有再發生（共 3 次）；" + fake2, c2 + "已恢復，持續了 1 小時 12 分"),
        message("heartbeat", 0,
                "統計區間：本次啟動以來 3.0 小時（自 2026-09-22 07:00:00 起）",
                "運作時間：3 小時（自 2026-09-22 07:00:00 起）",
                "策略4 K 棒：處理 36 根，其中 degraded 2 根（fetch_failed:api_error ×2）",
                "策略5 分鐘：處理 180 分鐘，degraded 1、missed 0",
                "原始訊號：策略4 3 筆、策略5 1 筆（含被持倉或冷卻擋掉的）",
                "A3：進場 3、出場 2、資料庫失敗 0",
                "A 頻道：進場送達 3、延遲不發 0、永久失敗 0、出場送達 2",
                "報表：送達 1、跳過 0",
                "REST：請求 4321、429 0 次",
                "日誌：ERROR 4、WARNING 12、佇列丟棄 0",
                "告警：發出 3 則（%s 1、%s 0、%s 1、%s 1）；仍未恢復：無" % (RED, YELLOW, ALARM, CHECK),
                "新上架（過去 24 小時）：共 2 個（納入 1、排除 1）：%s %s；%s（CNLX） %s"
                % (SAMPLE_X, SAMPLE_X_WHY, LOBSTER, SAMPLE_CNLX_WHY)),
        message("listing", 0, "合約：共 2 個（納入 1、排除 1）",
                "・%s：%s" % (SAMPLE_X, SAMPLE_X_WHY),
                "・%s（CNLX）：%s" % (LOBSTER, SAMPLE_CNLX_WHY)),
        message("shutdown", 0,
                "運作時間：3 小時（自 2026-09-22 07:00:00 起）",
                "結束原因：到達 --duration（10800 秒）",
                "exit code：0",
                "outbox 待送：0 則",
                "仍未恢復的條件：無",
                "日誌佇列丟棄：0 筆",
                "關閉過程中的 ERROR：無"),
    ]
    assert len(want) == len(msgs) == 8
    for (key, text), w in zip(msgs, want):
        assert text == "[測試] " + w, (key, text, w)


def test_ac6_startup_fields_and_announce_started():
    assert OA.startup_fields(None, "0 則", "0 期", "x")[0] == ("參數", "--push-tg（不帶 --duration，跑到 Ctrl+C 為止）")
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender) as a:
            push, reporter = FPush(config.A_CHANNEL_OUTBOX_DB_PATH), FReporter()
            add_pending(push.path, "s4-start-1", T0)
            reporter.st["pending"] = 2
            a.attach(push=push, reporter=reporter)
            a.announce_started(None)
        assert sender.texts("startup") == [message("startup", 0, "參數：--push-tg（不帶 --duration，跑到 Ctrl+C 為止）",
                                                   "outbox 待送：1 則", "報表待送：2 期",
                                                   "標的池：—（未啟動）")], sender.sent
    with ops_env():
        sender = FakeSender()
        with make_alerter(EpochClock(), sender) as a:
            a.announce_started(60)
        text = sender.texts("startup")[0]
        assert "\n參數：--push-tg、--duration 60 秒\n" in text and "\n報表待送：—（未啟動）\n" in text, text
        assert text.endswith("\n標的池：—（未啟動）"), text
        assert "\noutbox 待送：—（outbox 還沒建立）\n" in text, text


def test_ac6_item_clipping_and_whitespace():
    assert OA._clip("a" * 200) == "a" * 200
    assert OA._clip("a" * 201) == "a" * 199 + "…"
    assert OA._clip("a" * 500) == "a" * 199 + "…" and len(OA._clip("a" * 500)) == 200
    assert OA._clip("第一行\n  第二行\t\t第三行  ") == "第一行 第二行 第三行"


def test_ac6_message_level_truncation_by_count_and_by_utf16_length():
    items = [("k%d" % i, "[ERROR] x L%d ×1" % i, "訊息 %d" % i) for i in range(15)]
    lines = OA.compose("alert", T0, LABEL, items=items).split("\n")
    assert len([ln for ln in lines if ln.startswith("・")]) == 10
    assert lines[-1] == "…另有 5 項（見日誌）"
    big = [("k%d" % i, "[ERROR] x L%d ×1" % i, RED * 199) for i in range(15)]
    text = OA.compose("alert", T0, LABEL, items=big)
    assert utf16_units(text) <= config.TG_MAX_MESSAGE_CHARS, utf16_units(text)
    lines = text.split("\n")
    shown = len([ln for ln in lines if ln.startswith("・")])
    assert lines[-1].startswith("…另有 ") and lines[-1].endswith(" 項（見日誌）"), lines[-1]
    hidden = int(lines[-1][len("…另有 "):-len(" 項（見日誌）")])
    assert shown + hidden == 15 and hidden > 5, (shown, hidden)


# ============================== AC-7：包過的 on_result ==============================
def test_ac7_wrapper_still_calls_a3_when_the_alert_thread_crashed():
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender, run_thread=True) as a:
            clock.fail = True
            a._wake.set()
            wait_until(lambda: a.crashed and not a.worker_alive, "告警執行緒意外結束")
            assert a.wrap_on_result(lambda r: "A3-result")(None) == "A3-result"
            clock.fail = False
            rc = a.finish("Ctrl+C", 0)
            assert rc == 1
            text = sender.texts("shutdown")[0]
            assert "\n告警執行緒：執行中意外結束（見日誌），之後沒有再發告警" in text, text
            assert "\nexit code：1\n" in text, text


def test_ac7_wrapper_still_calls_a3_when_the_alert_thread_is_stuck():
    saved = config.OPS_ALERT_STOP_TIMEOUT_SECONDS
    try:
        with ops_env():
            clock = EpochClock()
            sender = FakeSender()
            with make_alerter(clock, sender, run_thread=True) as a:
                clock.t = T0 + 200
                sender.gate = threading.Event()
                emit(clock, 200, logging.CRITICAL, "卡住", lineno=7)
                wait_until(sender.entered.is_set, "告警執行緒卡在 send()")
                assert a.wrap_on_result(lambda r: "A3")(None) == "A3"
                config.OPS_ALERT_STOP_TIMEOUT_SECONDS = 0.05
                rc = a.finish("Ctrl+C", 0)
                assert rc == 1
                text = sender.texts("shutdown")[0]
                assert "\n告警執行緒：沒有在 0.05 秒內結束（見日誌）" in text, text
                sender.gate.set()
                a._thread.join(10)
                assert not a.worker_alive
    finally:
        config.OPS_ALERT_STOP_TIMEOUT_SECONDS = saved


def test_ac7_a3_exception_propagates_unchanged():
    with ops_env():
        with make_alerter(EpochClock()) as a:
            boom = ValueError("A3 壞了")

            def a3(res):
                raise boom
            try:
                a.wrap_on_result(a3)(None)
            except ValueError as e:
                assert e is boom
            else:
                raise AssertionError("A3 的例外應該原樣往外拋")
            assert a._bars == 1


# ============================== AC-8：live.a_channel 的接線 ==============================
class OTracker:
    def __init__(self, order, ready=True):
        self.order = order
        self.ready = ready
        self.specs = {}
        self.stats = {"fetch_failures": 0, "entries": 0, "exits": 0, "store_failures": 0}
        self.ready_error = None if ready else "假的：資料庫開不了"
        self.handler = lambda res: None
        self.worker_alive = True

    def start(self):
        self.order.append("A3.start")

    def wait_ready(self, timeout=None):
        return self.ready

    def stop(self):
        self.order.append("A3.stop")

    def join(self, timeout=None):
        return True

    def bar_result_handler(self, strategy):
        return self.handler


class OSender:
    def __init__(self, order):
        self.order = order

    def start(self):
        self.order.append("T2sender.start")

    def stop(self, timeout=None):
        self.order.append("T2sender.stop")
        return 0

    def stats(self):
        return {"worker_alive": True}


class OPush:
    def __init__(self, sender, order, tmp, fail_start=False):
        self.sender = sender
        self.order = order
        self.path = os.path.join(tmp, "absent", "outbox.sqlite3")
        self.fail_start = fail_start
        self.stats = {"fake": True}
        self.worker_alive = True

    def start(self):
        self.order.append("T2.start")
        if self.fail_start:
            raise RuntimeError("假的：outbox 開不了")

    def subscribe(self, bus):
        pass

    def set_specs(self, specs):
        pass

    def shutdown(self):
        self.order.append("T2.shutdown")


class OReporter:
    def __init__(self, order, fail_start=False, crashed=False):
        self.order = order
        self.fail_start = fail_start
        self.crashed = crashed
        self.path = "reports.sqlite3"

    def start(self):
        self.order.append("RA.start")
        if self.fail_start:
            raise RuntimeError("假的：發送紀錄開不了")

    def stop(self):
        self.order.append("RA.stop")
        return True

    def stats(self):
        return {"worker_crashed": self.crashed, "pending": 0, "worker_alive": not self.crashed,
                "compose_errors": 0, "skipped": 0, "delivered": 0}


class OS5:
    def __init__(self, order, fail_start=False):
        self.order = order
        self.fail_start = fail_start

    def start(self):
        self.order.append("A5.start")
        if self.fail_start:
            raise RuntimeError("假的：A5 起不來")

    def close(self, timeout=None):
        self.order.append("A5.close")
        return True

    def stats(self):
        return {"minutes": 0, "worker_crashed": False, "worker_alive": True, "degraded": 0, "missed": 0,
                "signals": 0}


class OFeed:
    def __init__(self, order, exc=None):
        self.order = order
        self.exc = exc
        self.gate = FGate()
        self.reconciler = FReconciler()

    def run(self, duration_s=None):
        self.order.append("A1.run")
        if self.exc is not None:
            raise self.exc

    def load_universe(self):            # A6：a_channel 的 default_feed 在 A5 之前先載入標的池
        self.order.append("A1.load_universe")
        return []

    def close(self):
        self.order.append("A1.close")


def run_main(argv, **kw):
    with capture("live.a_channel") as cap:
        rc = a_channel.main(argv, setup_logging=False, **kw)
    return rc, cap


@contextlib.contextmanager
def ac8_env():
    with ops_env() as tmp:
        order = []
        sender = FakeSender(order)
        a = OA.OpsAlerter(sender=sender, clock=EpochClock(), label=LABEL)
        try:
            yield tmp, order, sender, a
        finally:
            _shutdown_alerter(a)


def factories(a, order, tmp, *, tracker=None, push_fail=False, reporter=None, s5=None, feed=None):
    tracker = tracker or OTracker(order)
    reporter = reporter or OReporter(order)
    s5 = s5 or OS5(order)
    feed = feed or OFeed(order)
    t2sender = OSender(order)
    return dict(alerter_factory=lambda: a,
                tracker_factory=lambda bus: tracker,
                sender_factory=lambda: t2sender,
                pusher_factory=lambda s: OPush(s, order, tmp, push_fail),
                reporter_factory=lambda s: reporter,
                s5_feed_factory=lambda f, t: s5,
                feed_factory=lambda on_result: feed)


def _no_a4_left():
    assert not any(isinstance(h, OA._AlertHandler) for h in logging.getLogger().handlers), "handler 沒拆"
    assert not _threads(OA.THREAD_NAME) and not _threads(OA.SENDER_THREAD_NAME), "A4 的執行緒還活著"


def test_ac8_without_the_flag_nothing_changes():
    with ac8_env() as (tmp, order, sender, a), secret_reads_forbidden() as seen:
        called = []
        tracker = OTracker(order)
        got_on_result = []
        kw = factories(a, order, tmp, tracker=tracker)
        feed = OFeed(order)
        kw["alerter_factory"] = lambda: called.append(1) or a
        kw["feed_factory"] = lambda on_result: got_on_result.append(on_result) or feed
        rc, _ = run_main(["--duration", "5"], **kw)
        assert rc == 0
        assert called == [] and seen == [], (called, seen)
        assert got_on_result == [tracker.handler] and got_on_result[0] is tracker.handler
        assert sender.sent == [] and "ops.start" not in order
        assert order == ["A3.start", "A5.start", "A1.run", "A5.close", "A1.close", "A3.stop"], order
        _no_a4_left()


def test_ac8_default_feed_wires_note_reconcile_only_with_the_flag():
    saved = a_channel.SignalFeed, reconcile.Reconciler, rest_gate.shared_gate
    seen = []
    listings = []                        # A6：default_feed 一定會給 ListingTracker；只有 --push-tg 才接到 A4

    def fake_reconciler(gate, *, on_result=None, **kw):
        seen.append(on_result)
        return FReconciler()
    try:
        alerters = []
        for flag in (False, True):
            with ac8_env() as (tmp, order, sender, a):
                alerters.append(a)
                feed = OFeed(order)
                a_channel.SignalFeed = lambda gate, on_result, reconciler, **kw: listings.append(kw["listings"]) or feed
                reconcile.Reconciler = fake_reconciler
                rest_gate.shared_gate = FGate
                kw = factories(a, order, tmp)
                del kw["feed_factory"]
                rc, _ = run_main(["--duration", "5"] + (["--push-tg"] if flag else []), **kw)
                assert rc == 0, flag
                # 標的池在 A5 與 🟢 之前就載入（A6），run() 照常在後面
                assert order.index("A1.load_universe") < order.index("A5.start") < order.index("A1.run"), order
                if flag:
                    assert order.index("A1.load_universe") < order.index("ops.send:ops-startup"), order
        assert len(seen) == 2 and seen[0] is None, seen
        assert seen[1] == alerters[1].note_reconcile, seen
        assert len(listings) == 2 and all(isinstance(x, universe_seen.ListingTracker) for x in listings), listings
        assert listings[0].on_new is None and listings[1].on_new == alerters[1].note_listings, listings
    finally:
        a_channel.SignalFeed, reconcile.Reconciler, rest_gate.shared_gate = saved


def test_ac8_a_channel_does_not_import_ops_alert_at_module_level():
    with open(os.path.join(REPO_ROOT, "live", "a_channel.py"), encoding="utf-8") as f:
        tree = ast.parse(f.read())
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [n.name for n in node.names] + [getattr(node, "module", None) or ""]
            assert not any("ops_alert" in n for n in names), ast.dump(node)


def test_ac8_missing_ops_secret_exits_1_before_t2_a3_a1():
    with ac8_env() as (tmp, order, sender, a), tgt.env_vars({config.OPS_CHAT_ID_ENV: None}):
        kw = factories(a, order, tmp)
        del kw["alerter_factory"]
        rc, cap = run_main(["--push-tg", "--duration", "5"], **kw)
        assert rc == 1
        assert order == [], order
        errors = cap.messages(logging.ERROR)
        assert any(config.OPS_CHAT_ID_ENV in m and "不啟動" in m for m in errors), errors
        assert not find_leaks(cap.messages(), SECRETS)
        _no_a4_left()


def test_ac8_normal_run_order_and_messages():
    with ac8_env() as (tmp, order, sender, a):
        rc, _ = run_main(["--push-tg", "--duration", "5"], **factories(a, order, tmp))
        assert rc == 0
        assert sender.kinds() == ["startup", "shutdown"], sender.kinds()
        start = sender.texts("startup")[0]
        for want in ("\n參數：--push-tg、--duration 5 秒\n", "\noutbox 待送：—（outbox 還沒建立）\n", "\n報表待送：0 期"):
            assert want in start, (want, start)
        end = sender.texts("shutdown")[0]
        for want in ("\n結束原因：到達 --duration（5 秒）\n", "\nexit code：0\n"):
            assert want in end, (want, end)
        assert order[0] == "ops.start", order
        i_start = order.index("ops.send:ops-startup")
        assert order.index("A5.start") < i_start and order.index("RA.start") < i_start < order.index("A1.run"), order
        assert order[-7:] == ["A5.close", "A1.close", "A3.stop", "RA.stop", "T2.shutdown", "ops.send:ops-shutdown",
                              "ops.stop"], order
        assert sender.handler_at_stop is False
        _no_a4_left()


def test_ac8_run_outcomes_reason_and_exit_code():
    cases = ((KeyboardInterrupt(), {}, 0, ("結束原因：Ctrl+C",)),
             (UniverseUnavailable("x"), {}, 1, ("結束原因：UniverseUnavailable", "無法取得標的池", "exit code：1")),
             (None, {"crashed": True}, 1, ("關閉過程中的 ERROR：1 筆", "exit code：1")))
    for exc, rep_kw, want_rc, wants in cases:
        with ac8_env() as (tmp, order, sender, a):
            kw = factories(a, order, tmp, feed=OFeed(order, exc), reporter=OReporter(order, **rep_kw))
            rc, _ = run_main(["--push-tg", "--duration", "5"], **kw)
            assert rc == want_rc, (exc, rc)
            assert sender.kinds() == ["startup", "shutdown"], sender.kinds()
            end = sender.texts("shutdown")[0]
            for want in wants:
                assert want in end, (want, end)
            _no_a4_left()
    with ac8_env() as (tmp, order, sender, a):
        boom = RuntimeError("沒料到的")
        kw = factories(a, order, tmp, feed=OFeed(order, boom))
        try:
            run_main(["--push-tg", "--duration", "5"], **kw)
        except RuntimeError as e:
            assert e is boom
        else:
            raise AssertionError("未預期的例外應該原樣往外拋")
        end = sender.texts("shutdown")[0]
        assert "\n結束原因：未預期的例外 RuntimeError\n" in end and "\nexit code：1\n" in end, end
        _no_a4_left()


def test_ac8_start_failures_send_only_the_shutdown_message():
    cases = (("T2", {"push_fail": True}, "啟動失敗：T2（A 頻道推播）", "A 頻道推播的 outbox 開不了", "A3.start"),
             ("A3", {"tracker": "not-ready"}, "啟動失敗：A3 資料庫", "A3 的資料庫沒有在", "RA.start"),
             ("R-A", {"reporter": "fail"}, "啟動失敗：R-A 報表", "A 頻道報表（R-A）建立或啟動失敗", "A5.start"),
             ("A5", {"s5": "fail"}, "啟動失敗：A5", "策略五資料層（A5）建立或啟動失敗", "A1.run"))
    for what, opts, reason, logged, never in cases:
        with ac8_env() as (tmp, order, sender, a):
            kw = {}
            if opts.get("push_fail"):
                kw["push_fail"] = True
            if opts.get("tracker"):
                kw["tracker"] = OTracker(order, ready=False)
            if opts.get("reporter"):
                kw["reporter"] = OReporter(order, fail_start=True)
            if opts.get("s5"):
                kw["s5"] = OS5(order, fail_start=True)
            rc, _ = run_main(["--push-tg", "--duration", "5"], **factories(a, order, tmp, **kw))
            assert rc == 1, what
            assert sender.kinds() == ["shutdown"], (what, sender.kinds())
            end = sender.texts("shutdown")[0]
            assert "\n結束原因：%s\n" % reason in end and "\nexit code：1\n" in end, (what, end)
            assert logged in end, (what, logged, end)
            assert never not in order, (what, order)
            assert order[-2:] == ["ops.send:ops-shutdown", "ops.stop"], (what, order)
            _no_a4_left()


def test_a6_universe_load_failure_before_a5_sends_only_the_shutdown_message():
    """A6：標的池在 A1 建好時就載入（A5 與 🟢 之前）；載入失敗 → A5 / A1.run 都不跑、已啟動的收掉、⚪「啟動失敗：標的池」。"""
    saved = a_channel.SignalFeed, reconcile.Reconciler, rest_gate.shared_gate

    class FailingFeed(OFeed):
        def load_universe(self):
            self.order.append("A1.load_universe")
            raise UniverseUnavailable("market_static 載入失敗：假的")
    try:
        with ac8_env() as (tmp, order, sender, a):
            feed = FailingFeed(order)
            a_channel.SignalFeed = lambda gate, on_result, reconciler, **kw: feed
            reconcile.Reconciler = lambda gate, *, on_result=None, **kw: FReconciler()
            rest_gate.shared_gate = FGate
            kw = factories(a, order, tmp)
            del kw["feed_factory"]
            rc, cap = run_main(["--push-tg", "--duration", "5"], **kw)
            assert rc == 1
            assert sender.kinds() == ["shutdown"], sender.kinds()
            end = sender.texts("shutdown")[0]
            assert "\n結束原因：啟動失敗：標的池\n" in end and "\nexit code：1\n" in end, end
            assert "A5.start" not in order and "A1.run" not in order, order
            assert order.index("A1.load_universe") < order.index("A1.close") < order.index("A3.stop") < \
                order.index("RA.stop") < order.index("T2.shutdown") < order.index("ops.send:ops-shutdown"), order
            assert any("無法取得標的池，不啟動 A1 / A5" in m for m in cap.messages(logging.ERROR))
            _no_a4_left()
    finally:
        a_channel.SignalFeed, reconcile.Reconciler, rest_gate.shared_gate = saved


class JTracker(OTracker):
    """OTracker 加記 join（收尾順序要看得到 A3 有 stop 也有 join）。"""

    def join(self, timeout=None):
        self.order.append("A3.join")
        return True


SHUTDOWN_TAIL_PUSH = ["A3.stop", "A3.join", "RA.stop", "T2.shutdown", "ops.send:ops-shutdown", "ops.stop"]


def run_main_no_escape(argv, **kw):
    """run_main，但 KeyboardInterrupt 穿出 main() 時轉成 AssertionError：回歸時要報 FAIL，不可以把整個 runner 打斷。"""
    try:
        return run_main(argv, **kw)
    except KeyboardInterrupt:
        raise AssertionError("KeyboardInterrupt 穿出 main()：沒有收尾、沒有 ⚪") from None


def test_a6_ctrl_c_while_loading_universe_shuts_down_in_order_through_the_real_default_feed():
    """DQA M1 回歸：標的池改在 default_feed 裡載入（A5 與 🟢 之前），這段收到 Ctrl+C（systemctl stop 的 SIGINT）時
    要跟 feed.run() 期間的 Ctrl+C 一樣：A1 / A5 不啟動、已啟動的 A3 → R-A → T2 → A4 依序收掉、⚪「結束原因：Ctrl+C」、
    exit 0。走正式路徑：a_channel 的 default_feed → 真的 SignalFeed.load_universe() → MarketUniverse →
    market_static.refresh()，Ctrl+C 落在第一個 symbols 請求上（閘門的取數拋 KeyboardInterrupt）。"""
    import test_signal_feed as tsf
    from live.rest_gate import RestGate, RestLimiter, ServerClock
    saved = rest_gate.shared_gate
    try:
        for flag in (True, False):
            tsf.fresh_market_static()
            clock = tsf.FakeClock(tsf.T0)
            requests = []

            def interrupted(path, params=None, retries=3, timeout=20):
                requests.append(path)
                raise KeyboardInterrupt
            gate = RestGate(RestLimiter(config.A1_REST_RATE_PER_SECOND, config.REST_BAN_COOLDOWN_SECONDS, clock=clock),
                            ServerClock(clock), clock=clock, api_get=interrupted)
            rest_gate.shared_gate = lambda: gate
            with ac8_env() as (tmp, order, sender, a):
                kw = factories(a, order, tmp, tracker=JTracker(order))
                del kw["feed_factory"]                                  # 正式的 default_feed
                rc, cap = run_main_no_escape(["--duration", "5"] + (["--push-tg"] if flag else []), **kw)
                assert rc == 0, (flag, rc)
                assert requests == ["/api/v1/common/symbols"], requests  # Ctrl+C 之後一個請求都不再送
                assert "A5.start" not in order and "A1.run" not in order, order
                assert any("啟動中（載入標的池時）收到 Ctrl+C" in m for m in cap.messages(logging.INFO)), cap.messages()
                if flag:
                    assert order[order.index("RA.start") + 1:] == SHUTDOWN_TAIL_PUSH, order
                    assert sender.kinds() == ["shutdown"], sender.kinds()        # 沒有 🟢
                    end = sender.texts("shutdown")[0]
                    assert "\n結束原因：Ctrl+C\n" in end and "\nexit code：0\n" in end, end
                    _no_a4_left()
                else:
                    assert order == ["A3.start", "A3.stop", "A3.join"], order
                    assert sender.sent == [] and "ops.start" not in order
    finally:
        rest_gate.shared_gate = saved


def test_a6_ctrl_c_or_unexpected_exception_from_load_universe_shut_down_then_return_or_raise():
    """DQA M1 回歸：default_feed 的 load_universe() 拋 KeyboardInterrupt → 收尾後回傳 0；拋其他例外 → 收尾、⚪ 寫
    「未預期的例外 <類型>」、exit code 1，然後同一個例外原樣往外拋。兩種都驗帶與不帶 --push-tg。"""
    saved = a_channel.SignalFeed, reconcile.Reconciler, rest_gate.shared_gate
    try:
        for exc_type in (KeyboardInterrupt, RuntimeError):
            for flag in (True, False):
                with ac8_env() as (tmp, order, sender, a):
                    boom = exc_type("假的：載入標的池時出事") if exc_type is RuntimeError else KeyboardInterrupt()

                    class RaisingFeed(OFeed):
                        def load_universe(self):
                            self.order.append("A1.load_universe")
                            raise boom
                    feed = RaisingFeed(order)
                    a_channel.SignalFeed = lambda gate, on_result, reconciler, **kw: feed
                    reconcile.Reconciler = lambda gate, *, on_result=None, **kw: FReconciler()
                    rest_gate.shared_gate = FGate
                    kw = factories(a, order, tmp, tracker=JTracker(order))
                    del kw["feed_factory"]
                    argv = ["--duration", "5"] + (["--push-tg"] if flag else [])
                    if exc_type is KeyboardInterrupt:
                        rc, _ = run_main_no_escape(argv, **kw)
                        assert rc == 0, (flag, rc)
                    else:
                        try:
                            run_main_no_escape(argv, **kw)
                        except RuntimeError as e:
                            assert e is boom, e
                        else:
                            raise AssertionError("非預期的例外應該收尾之後原樣往外拋")
                    case = (exc_type.__name__, flag)
                    assert "A5.start" not in order and "A1.run" not in order, (case, order)
                    i = order.index("A1.load_universe")
                    if flag:
                        assert order[i + 1:] == ["A1.close"] + SHUTDOWN_TAIL_PUSH, (case, order)
                        assert sender.kinds() == ["shutdown"], (case, sender.kinds())
                        end = sender.texts("shutdown")[0]
                        if exc_type is KeyboardInterrupt:
                            assert "\n結束原因：Ctrl+C\n" in end and "\nexit code：0\n" in end, (case, end)
                        else:
                            assert "\n結束原因：未預期的例外 RuntimeError\n" in end and "\nexit code：1\n" in end, \
                                (case, end)
                        _no_a4_left()
                    else:
                        assert order[i + 1:] == ["A1.close", "A3.stop", "A3.join"], (case, order)
                        assert sender.sent == []
    finally:
        a_channel.SignalFeed, reconcile.Reconciler, rest_gate.shared_gate = saved


# ============================== A6：🆕 新上架、🟢 標的池、💓 新上架 ==============================
def _listing(symbol, base, tradable=True, reason=None, t=T0, initial=False):
    """用 strategy.universe 真的判定組一個 Listing（reason 給了就覆寫，測遮罩 / 截斷用）。"""
    c = universe_rules.classify({"symbol": symbol, "baseCurrency": base, "quoteCurrency": "USDT"})
    return universe_seen.Listing(symbol=symbol, base=c.base, first_seen_ms=int(t * 1000), tradable=c.tradable,
                                 category=c.category, reason=reason or c.reason, initial=initial)


def test_a6_listing_message_format_masking_caps_and_log_line():
    with ops_env(), capture("live.ops_alert") as cap:
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender) as a:
            a.announce_started(None)
            batch = [_listing(LOBSTER, "CNLX"), _listing("AAPLX_USDT_PERP", "AAPLX"),
                     _listing("LEAK_USDT_PERP", "LEAK", reason="理由裡夾了密鑰 " + FAKE_TOKEN + " 結尾"),
                     _listing("LONG_USDT_PERP", "LONG", reason="長" * 400)]
            batch += [_listing("N%02d_USDT_PERP" % i, "N%02d" % i) for i in range(8)]       # 共 12 個
            a.note_listings(batch)
            a.run_once(T0 + 1)
            texts = sender.texts("listing")
            assert len(texts) == 1, sender.kinds()
            lines = texts[0].split("\n")
            assert lines[0] == TITLE["listing"] + "  " + LABEL and lines[1] == "時間：" + tpe(1), lines[:2]
            assert lines[2] == "合約：共 12 個（納入 11、排除 1）", lines[2]
            assert lines[3] == "・%s（CNLX）：納入（加密：在 CRYPTO_X_WHITELIST（例外納入））" % LOBSTER, lines[3]
            assert lines[4] == "・AAPLX_USDT_PERP：排除（股票／ETF／商品：名稱 X 結尾（長度 ≥ 4、不在 CRYPTO_X_WHITELIST））"
            assert MASK in lines[5] and not find_leaks([texts[0]], SECRETS), lines[5]
            body = lines[6][len("・LONG_USDT_PERP："):]
            assert len(body) == config.OPS_ALERT_ITEM_MAX_CHARS and body.endswith("…"), (len(body), body[-3:])
            assert len([ln for ln in lines if ln.startswith("・")]) == config.OPS_ALERT_MAX_ITEMS
            assert lines[-1] == "…另有 2 項（見日誌）", lines[-1]
            assert "（CNLX）" not in lines[4] and "AAPLX_USDT_PERP（" not in lines[4]     # base 相同不加括號
            infos = [m for m in cap.messages(logging.INFO) if m.startswith("已交出 listing（新上架）")]
            assert infos == ["已交出 listing（新上架）：12 項，鍵 %s" % "、".join(x.symbol for x in batch)], infos
            # 不算告警：心跳的「告警：發出 N 則」不含它（🟢 也不算）
            tick(a, clock, 82800)
            hb = sender.texts("heartbeat")[0]
            assert "\n告警：發出 0 則（%s 0、%s 0、%s 0、%s 0）；仍未恢復：無\n" % (RED, YELLOW, ALARM, CHECK) in hb, hb
            assert a._sent["listing"] == 1


def test_a6_listings_wait_for_startup_and_each_refresh_is_one_message():
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender) as a:
            a.note_listings([_listing("AAA_USDT_PERP", "AAA")])          # 例如重啟時，啟動前的標的池載入就偵測到
            a.run_once(T0 + 1)
            assert sender.kinds() == [], sender.kinds()                  # 🟢 還沒發：先不發
            a.announce_started(None)
            a.note_listings([_listing("BBB_USDT_PERP", "BBB"), _listing("CCCX_USDT_PERP", "CCCX")])
            a.note_listings([])                                          # 空的一批：什麼都不做
            a.run_once(T0 + 2)
            assert sender.kinds() == ["startup", "listing", "listing"], sender.kinds()
            first, second = sender.texts("listing")
            assert "\n合約：共 1 個（納入 1、排除 0）\n・AAA_USDT_PERP：" in first, first
            assert "\n合約：共 2 個（納入 1、排除 1）\n・BBB_USDT_PERP：" in second and "\n・CCCX_USDT_PERP：排除（" in second
            a.run_once(T0 + 3)
            assert sender.kinds() == ["startup", "listing", "listing"], "同一批不可以再發一次"


def test_a6_listings_after_begin_shutdown_go_into_the_shutdown_message():
    """FR-5 第 2 點的二選一：選「併進 ⚪ 的項目」（那些合約已寫進追蹤檔，重啟後不會再當成新上架）。"""
    with ops_env():
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender) as a:
            a.announce_started(None)
            a.begin_shutdown()
            a.note_listings([_listing(LOBSTER, "CNLX"), _listing("ZZZX_USDT_PERP", "ZZZX")])
            a.run_once(T0 + 1)
            assert "listing" not in sender.kinds(), sender.kinds()
            a.finish("Ctrl+C", 0)
        assert sender.kinds() == ["startup", "shutdown"], sender.kinds()
        end = sender.texts("shutdown")[0]
        assert "\n關閉前還沒發出的新上架：2 個（一併列在下面）" in end, end
        assert "\n・[新上架] %s（CNLX）：納入（加密：在 CRYPTO_X_WHITELIST（例外納入））" % LOBSTER in end, end
        assert "\n・[新上架] ZZZX_USDT_PERP：排除（股票／ETF／商品：" in end, end
        assert "關閉前還沒發出的告警" not in end, end


def test_a6_startup_universe_field():
    cases = ((lambda: {"total": 5, "included": 3, "excluded": 2,
                       "by_category": {"stock": 1, "pegged": 1}},
              "標的池：納入 3、排除 2（股票／ETF／商品 1、強掛勾／穩定幣／包裝幣 1）"),
             (lambda: {"total": 3, "included": 3, "excluded": 0, "by_category": {}}, "標的池：納入 3、排除 0"),
             (lambda: None, "標的池：—（還沒載入）"),
             (lambda: 1 / 0, "標的池：讀不到（ZeroDivisionError）"))
    for fn, want in cases:
        with ops_env():
            sender = FakeSender()
            with make_alerter(EpochClock(), sender) as a:
                feed = OFeed([])
                feed.universe_summary = fn
                a.attach(feed=feed)
                a.announce_started(None)
            text = sender.texts("startup")[0]
            assert text.endswith("\n" + want), (want, text)


def test_a6_startup_field_uses_the_real_feed_summary():
    """真的 SignalFeed + MarketUniverse：🟢 的數字就是第一次載入時的分類結果（假交易所、離線）。"""
    import test_signal_feed as tsf
    with ops_env():
        feed = tsf.feed_with_market(tsf.SMALL_SPECS)
        with capture("live.signal_feed"):
            feed.load_universe()
        sender = FakeSender()
        with make_alerter(EpochClock(), sender) as a:
            a.attach(feed=feed)
            a.announce_started(None)
        want = universe_rules.summary_text(universe_rules.summarize(
            c for c in (universe_rules.classify(s) for s in tsf.SMALL_SPECS if s["status"] == "TRADING")
            if c is not None))
        assert sender.texts("startup")[0].endswith("\n標的池：" + want), sender.texts("startup")[0]
        assert want.startswith("納入 ") and "股票／ETF／商品" in want, want


def test_a6_heartbeat_lists_only_non_initial_listings_within_24_hours():
    with ops_env():
        hb_now = T0 + 82800                                   # 隔天台北 09:00
        day = config.OPS_HEARTBEAT_LISTING_HOURS * 3600
        tclock = EpochClock(hb_now - 3 * day)
        tracker = universe_seen.ListingTracker(clock=tclock)

        def classes(*specs):
            return {s[0]: universe_rules.classify({"symbol": s[0], "baseCurrency": s[1], "quoteCurrency": "USDT"})
                    for s in specs}
        base = [("BTC_USDT_PERP", "BTC"), ("AAPLX_USDT_PERP", "AAPLX")]
        assert tracker.observe(classes(*base)) == []                                 # 建檔（初始列）
        tclock.t = hb_now - day - 1
        assert len(tracker.observe(classes(*base, ("OLD_USDT_PERP", "OLD")))) == 1   # 24 小時又 1 秒前
        tclock.t = hb_now - day
        assert len(tracker.observe(classes(*base, ("OLD_USDT_PERP", "OLD"), ("EDGEX_USDT_PERP", "EDGEX")))) == 1
        tclock.t = hb_now - 3600
        assert len(tracker.observe(classes(*base, ("OLD_USDT_PERP", "OLD"), ("EDGEX_USDT_PERP", "EDGEX"),
                                           (LOBSTER, "CNLX")))) == 1
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender) as a:
            tick(a, clock, 82800)
        hb = sender.texts("heartbeat")[0]
        line = hb.split("\n")[-1]
        assert line == ("新上架（過去 24 小時）：共 2 個（納入 1、排除 1）："
                        "EDGEX_USDT_PERP 排除（股票／ETF／商品：名稱 X 結尾（長度 ≥ 4、不在 CRYPTO_X_WHITELIST））；"
                        "%s（CNLX） 納入（加密：在 CRYPTO_X_WHITELIST（例外納入））" % LOBSTER), line
        assert "OLD_USDT_PERP" not in hb and "BTC_USDT_PERP" not in hb and "AAPLX" not in hb, hb


def test_a6_heartbeat_does_not_list_initial_rows_even_inside_the_window():
    """第一次部署：建檔在心跳前 1 小時（在 24 小時窗內），初始列一個都不列（否則隔天 09:00 會列出全部 500 多個）。"""
    with ops_env():
        hb_now = T0 + 82800
        tracker = universe_seen.ListingTracker(clock=EpochClock(hb_now - 3600))
        initial = {s: universe_rules.classify({"symbol": s}) for s in ("BTC_USDT_PERP", "AAPLX_USDT_PERP",
                                                                      "ETH_USDT_PERP")}
        assert tracker.observe(initial) == [] and tracker.stats["bootstrapped"] == 3
        rows = universe_seen.read_all()
        assert all(r.initial and r.first_seen_ms == int((hb_now - 3600) * 1000) for r in rows), rows
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender) as a:
            tick(a, clock, 82800)
        assert sender.texts("heartbeat")[0].split("\n")[-1] == "新上架（過去 24 小時）：無", sender.texts("heartbeat")


def test_a6_heartbeat_listing_field_when_the_database_is_unreadable_or_has_many():
    with ops_env() as tmp, capture("live.ops_alert") as cap:
        clock = EpochClock()
        sender = FakeSender()
        with make_alerter(clock, sender) as a:                 # 追蹤檔不存在
            tick(a, clock, 82800)
            tick(a, clock, 82800 + 86400)
        lines = [t.split("\n")[-1] for t in sender.texts("heartbeat")]
        assert lines == ["新上架（過去 24 小時）：讀不到（UniverseSeenError）"] * 2, lines
        warns = [m for m in cap.messages(logging.WARNING) if "讀不到新上架追蹤檔" in m]
        assert len(warns) == 1, warns                           # 同一種錯誤只記一次
        os.makedirs(os.path.dirname(config.UNIVERSE_SEEN_DB_PATH), exist_ok=True)
        with open(config.UNIVERSE_SEEN_DB_PATH, "wb") as f:   # 不是 SQLite 檔
            f.write(b"not a database" * 100)
        sender2 = FakeSender()
        with make_alerter(EpochClock(), sender2, ) as a:
            tick(a, a._clock, 82800)
        assert sender2.texts("heartbeat")[0].split("\n")[-1] == "新上架（過去 24 小時）：讀不到（DatabaseError）"
        os.remove(config.UNIVERSE_SEEN_DB_PATH)
        tracker = universe_seen.ListingTracker(clock=EpochClock(T0))
        tracker.observe({"BTC_USDT_PERP": universe_rules.classify({"symbol": "BTC_USDT_PERP"})})
        many = {"M%02d_USDT_PERP" % i: universe_rules.classify({"symbol": "M%02d_USDT_PERP" % i}) for i in range(13)}
        tracker._clock = EpochClock(T0 + 3600)
        assert len(tracker.observe(many)) == 13
        sender3 = FakeSender()
        with make_alerter(EpochClock(), sender3) as a:
            tick(a, a._clock, 82800)
        line = sender3.texts("heartbeat")[0].split("\n")[-1]
        assert line.startswith("新上架（過去 24 小時）：共 13 個（納入 13、排除 0）：M00_USDT_PERP 納入（")
        assert line.endswith("；…另有 3 個（見日誌）") and line.count("_USDT_PERP 納入（") == 10, line


def test_a6_note_listings_does_no_io_and_never_raises():
    """A1 執行緒呼叫的 note_listings：不開檔、不連資料庫、不連網、不 sleep、不拋例外（壞輸入也一樣）。"""
    import builtins
    import sqlite3

    def forbidden(*a, **k):
        raise AssertionError("note_listings 做了 I/O")
    with ops_env():
        sender = FakeSender()
        with make_alerter(EpochClock(), sender) as a:
            saved = builtins.open, sqlite3.connect, os.makedirs
            builtins.open = sqlite3.connect = os.makedirs = forbidden
            try:
                class Boom:
                    def __iter__(self):
                        raise RuntimeError("壞的輸入")
                done = []
                t = threading.Thread(target=lambda: done.append(
                    (a.note_listings([_listing("AAA_USDT_PERP", "AAA")]), a.note_listings(Boom()),
                     a.note_listings(None))), name="a1-main-loop")
                t.start()
                t.join(5)
                assert done == [(None, None, None)], done
                a._lock = None                                      # 連鎖都壞了也不拋
                a.note_listings([_listing("BBB_USDT_PERP", "BBB")])
            finally:
                builtins.open, sqlite3.connect, os.makedirs = saved
                a._lock = threading.Lock()
            assert [len(b) for b in a._listing_batches] == [1], a._listing_batches
            assert sender.sent == []                                # 交出在 ops-alert 執行緒，不在呼叫端


# ============================== --sample ==============================
def test_sample_sends_eight_test_messages_to_the_ops_chat():
    for plan, want_rc, want_text in (((), OA.EXIT_SENT, "已送達 8"),
                                     ((http_error(400, "Bad Request: chat not found"),), OA.EXIT_FAILED, "最後錯誤")):
        with harness() as h, tgt.env_vars({config.OPS_CHAT_ID_ENV: FAKE_OPS}):
            h.tg.plan(*plan)

            def factory():
                return h.sender(chat_id_env=config.OPS_CHAT_ID_ENV, thread_name=OA.SENDER_THREAD_NAME,
                                log_label=OA.SENDER_LOG_LABEL)
            with contextlib.redirect_stdout(io.StringIO()) as out:
                rc = OA.sample(sender_factory=factory)
            assert rc == want_rc, (rc, out.getvalue())
            assert want_text in out.getvalue(), out.getvalue()
            assert len(h.tg.calls) == 8
            assert sum(1 for c in h.tg.calls if c.payload["text"].startswith("[測試] " + NEW + " 新上架")) == 1
            assert all(c.payload["text"].startswith("[測試]") for c in h.tg.calls)
            assert all(c.payload["chat_id"] == FAKE_OPS for c in h.tg.calls)
            assert not find_leaks([out.getvalue()] + h.logs.lines, SECRETS)
            wait_until(lambda: not _threads(OA.SENDER_THREAD_NAME), "tg-ops-sender 結束")


def test_sample_without_secrets_reports_not_tested_and_stays_offline():
    called = []
    with tgt.env_vars({config.TG_BOT_TOKEN_ENV: None, config.OPS_CHAT_ID_ENV: None}), OfflineCage() as cage:
        with contextlib.redirect_stdout(io.StringIO()) as out:
            rc = OA.sample(sender_factory=lambda: called.append(1))
    assert rc == OA.EXIT_NOT_TESTED == 3, rc
    text = out.getvalue()
    assert "未實測" in text and config.TG_BOT_TOKEN_ENV in text and config.OPS_CHAT_ID_ENV in text, text
    assert called == [] and not cage.attempts and not cage.sleeps


def test_main_without_sample_flag_is_a_usage_error():
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        try:
            OA.main([])
        except SystemExit as e:
            assert e.code == 2, e.code
        else:
            raise AssertionError("沒帶 --sample 應該是用法錯誤（argparse exit 2）")


# ============================== AC-11：參數 ==============================
def test_ac11_params_in_execution_params_and_python_m_live_and_env_listed_by_name_only():
    ops_names = sorted(n for n in dir(config) if n.startswith("OPS_") and n != "OPS_CHAT_ID_ENV")
    assert len(ops_names) == 16, ops_names                          # A6 加了 OPS_HEARTBEAT_LISTING_HOURS
    assert config.OPS_HEARTBEAT_LISTING_HOURS == 24
    names = ops_names + ["A3_STORE_RETRY_DELAYS_SECONDS", "TG_MAX_MESSAGE_CHARS"]
    params = config.execution_params()
    for n in names:
        assert params[n] == getattr(config, n), n
    assert not any(k.endswith("_ENV") for k in params), [k for k in params if k.endswith("_ENV")]
    assert (config.OPS_ALERT_INSTANCE_LABEL, config.OPS_ALERT_POLL_SECONDS, config.OPS_ALERT_BATCH_SECONDS,
            config.OPS_ALERT_REMIND_SECONDS, config.OPS_ALERT_STARTUP_GRACE_SECONDS, config.OPS_ALERT_QUEUE_MAX,
            config.OPS_ALERT_ITEM_MAX_CHARS, config.OPS_ALERT_MAX_ITEMS, config.OPS_ALERT_STOP_TIMEOUT_SECONDS,
            config.OPS_HEARTBEAT_TIME_TPE, config.OPS_A1_STALL_SECONDS, config.OPS_A5_STALL_SECONDS,
            config.OPS_OUTBOX_STUCK_SECONDS, config.OPS_COUNTER_RAISE_POLLS, config.OPS_COUNTER_CLEAR_POLLS,
            config.A3_STORE_RETRY_DELAYS_SECONDS, config.TG_MAX_MESSAGE_CHARS) == \
        (None, 60, 30, 3600, 180, 1000, 200, 10, 30, "09:00", 660, 180, 600, 3, 5, (60, 120, 300, 600, 900), 4096)
    assert config.SECRET_ENV_VARS[config.OPS_CHAT_ID_ENV] == "Telegram A4 維運告警私人聊天的 chat id"
    for with_secrets in (True, False):
        overrides = {config.TG_BOT_TOKEN_ENV: FAKE_TOKEN, config.TG_CHANNEL_ID_ENV: FAKE_CHAT,
                     config.OPS_CHAT_ID_ENV: FAKE_OPS} if with_secrets else {}
        r = subprocess.run([sys.executable, "-m", "live"], cwd=REPO_ROOT, env=tgt._child_env(**overrides),
                           stdin=subprocess.DEVNULL, capture_output=True, timeout=120)
        out = r.stdout.decode("utf-8", "replace")
        err = r.stderr.decode("utf-8", "replace")
        assert r.returncode == 0, (r.returncode, out[-2000:], err[-2000:])
        for n in names:
            assert "  %-28s: %s" % (n, params[n]) in out, n
        state = "已設定" if with_secrets else "未設定"
        line = "  %-28s: %s  (%s)" % (config.OPS_CHAT_ID_ENV, state, config.SECRET_ENV_VARS[config.OPS_CHAT_ID_ENV])
        assert line in out, out[-1500:]
        assert not find_leaks([out, err], SECRETS), "python -m live 印出了密鑰片段"


def test_module_name_and_dependencies():
    assert "ops_alert" not in sys.stdlib_module_names
    with open(os.path.join(REPO_ROOT, "live", "ops_alert.py"), encoding="utf-8") as f:
        tree = ast.parse(f.read())
    top = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            top.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            top.add(node.module.split(".")[0])
    # strategy 是本專案的套件（WBS §10 #1：live/ → strategy/ 是唯一允許的跨套件相依；A6 用 strategy.universe 組字）
    extra = top - set(sys.stdlib_module_names) - {"live", "strategy"}
    assert not extra, "live/ops_alert.py 用了標準庫以外的套件：%s" % extra
    assert "universe" in {a.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
                          and node.module == "strategy" for a in node.names}


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    # 測試輸出只看結果：根 logger 掛一個 NullHandler，WARNING 以上不會被 logging 的 lastResort 印到終端機
    logging.getLogger().addHandler(logging.NullHandler())
    runtime_existed = os.path.exists(paths.RUNTIME_DIR)
    outbox_existed = os.path.exists(config.A_CHANNEL_OUTBOX_DB_PATH)
    reports_existed = os.path.exists(config.A_CHANNEL_REPORT_DB_PATH)
    pristine = (socket.socket.connect, socket.create_connection, socket.getaddrinfo, time.sleep)
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS  {name}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL  {name}: {type(e).__name__}: {e}")
    runner_failed = 0
    if (socket.socket.connect, socket.create_connection, socket.getaddrinfo, time.sleep) != pristine:
        runner_failed += 1
        print("FAIL  <runner>: socket / time.sleep 沒有還原")
    if os.path.exists(paths.RUNTIME_DIR) != runtime_existed or \
            os.path.exists(config.A_CHANNEL_OUTBOX_DB_PATH) != outbox_existed or \
            os.path.exists(config.A_CHANNEL_REPORT_DB_PATH) != reports_existed:
        runner_failed += 1
        print(f"FAIL  <runner>: 測試在真正的 runtime/ 留下了東西（{paths.RUNTIME_DIR}）")
    leftover = [t.name for t in threading.enumerate()
                if t.name in (OA.THREAD_NAME, OA.SENDER_THREAD_NAME, "tg-channel-sender") and t.is_alive()]
    if leftover:
        runner_failed += 1
        print(f"FAIL  <runner>: 還有執行緒活著 {leftover}")
    if any(isinstance(h, OA._AlertHandler) for h in logging.getLogger().handlers):
        runner_failed += 1
        print("FAIL  <runner>: root logger 上還掛著 A4 的 handler")
    print(f"\n{len(tests) - failed} passed, {failed} failed"
          + (f", {runner_failed} runner check(s) failed" if runner_failed else ""))
    sys.exit(1 if failed or runner_failed else 0)
