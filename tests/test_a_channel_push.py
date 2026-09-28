# -*- coding: utf-8 -*-
"""
T2（live.a_channel_push / live.a_channel_outbox / live.a_channel 的 --push-tg）驗收測試。

  AC-2  去重與順序：同一事件 publish 三次只一列、只送一次；重開 outbox 後再 publish 仍不送；出場在進場還待送時
        不會先送；s4 與 s5 同幣同時段各自送出、互不干擾
  AC-3  持久化（真子行程、硬砍）：在「已寫入 outbox、還沒送出」與「已交給發送器、還沒回報」兩個時間點各砍 10 次，
        重啟後每一則恰好一個終態、沒有任何一則消失；未過期的照送、已過期的進場延遲不發（交出後才砍的算「不確定」，
        依 FR-3 第 3 點照送）；拿「只有記憶體佇列」的實作跑同一個情境，必須出現訊息消失
  AC-4  延遲進場（假時鐘）：299 秒送、301 秒不送、出場也不送；發送器內部 429 等待而過期 → 延遲不發；第一次嘗試逾時
        （不確定）之後才過期 → 照送、已送達；T2 層級的不確定重試同理；延遲 ≥ 門檻的出場照送並帶延遲註記
  AC-5  handler 規則：market_static 未載入 → handler 拋例外 → 真的 SignalBus 的送達報告 ok=False；outbox 被鎖 /
        唯讀 / 被刪掉 → 同上；handler 不連網、不交給發送器、單次耗時（印出 p50 / p99，數字由 DQA 量）
  AC-7  接線：不帶 --push-tg 與修改前相同（不讀密鑰、不建 outbox）；帶旗標缺密鑰 exit 1、A1 / A3 沒啟動；
        帶旗標時 T2 在 A3 啟動前就緒、接線自檢列出 T2、關閉時沒送完的留在 outbox；真 ChannelSender 端到端
  其他  outbox 的 schema / 終態不可改 / 不刪列 / ±inf；價格精度與槓桿快照；結果不確定時重送同一段文字；
        --sample 缺密鑰不連網（exit 3）與 exit code；execution_params、模組命名與相依；outbox 與日誌沒有密鑰片段

全程離線：socket 籠子（沿用 tests/test_tg_channel.py 的 OfflineCage，進場自我測試、禁止真的 sleep）；
時間一律注入假時鐘。outbox 一律開在暫存目錄，不碰 runtime/。
不依賴 pytest：直接 `python tests/test_a_channel_push.py`。

AC-3 的子行程入口也在本檔：`python tests/test_a_channel_push.py --ac3-child '<json>'`（見 _ac3_child）。
DQA 要拿別的實作跑同一個情境時，用 run_ac3_kill(impl=..., point=..., ...)；impl="memory" 是本檔的
「只有記憶體佇列」對照組 MemoryOnlyPusher。
"""
import collections
import contextlib
import json
import logging
import os
import queue
import shutil
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import textwrap
import threading
import time

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, TESTS_DIR)

import test_tg_channel as tgt  # noqa: E402
from test_tg_channel import (FAKE_TOKEN, FakeClock, OfflineCage, find_leaks, harness, read_timeout,  # noqa: E402
                             too_many, ok, wait_until)

from live import a_channel_outbox as OB  # noqa: E402
from live import a_channel_push as P  # noqa: E402
from live import a_channel_text as T  # noqa: E402
from live import config, market_static, paths  # noqa: E402
from live import notional_tracker as nt  # noqa: E402
from live.bus import SignalBus  # noqa: E402
from live.signal_events import EntryEvent, ExitEvent  # noqa: E402
from live.tg_channel import (RESULT_ABANDONED, RESULT_DELIVERED, RESULT_EXPIRED, RESULT_FAILED,  # noqa: E402
                             RESULT_GAVE_UP, ChannelSender, SendResult)

SPECS = nt.build_specs()
S4, S5 = SPECS["s4"], SPECS["s5"]
MIN = 60_000
SEC = 1000
DELAY_MS = config.A_CHANNEL_ENTRY_MAX_DELAY_SECONDS * SEC
RETRY_MS = config.A_CHANNEL_OUTBOX_RETRY_SECONDS * SEC
# 5 分鐘對齊的訊號 K 棒收盤：2026-09-22 左右（FakeClock 的牆上時間附近）
CLOSE = 1790000700000
assert CLOSE % S4.main_ms == 0 and CLOSE % S5.main_ms == 0


# ============================== 測試工具 ==============================
@contextlib.contextmanager
def tempdir():
    d = tempfile.mkdtemp(prefix="t2_push_")
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


class Clock:
    """假的 UTC epoch 毫秒時鐘。"""

    def __init__(self, ms):
        self.ms = int(ms)

    def __call__(self):
        return self.ms


class FakeMarket:
    """market_static 的替身：symbol_spec / max_leverage；loaded=False 時照真的模組拋 NotLoadedError。"""

    def __init__(self, specs=None, leverage=None, loaded=True):
        self.specs = {"ACE_USDT_PERP": {"symbol": "ACE_USDT_PERP", "quotePrecision": 5, "quoteStep": "0.00001"},
                      "BTC_USDT_PERP": {"symbol": "BTC_USDT_PERP", "quotePrecision": 1, "quoteStep": "0.1"}}
        if specs:
            self.specs.update(specs)
        self.leverage = {"ACE_USDT_PERP": 75, "BTC_USDT_PERP": 100}
        if leverage:
            self.leverage.update(leverage)
        self.loaded = loaded

    def _check(self):
        if not self.loaded:
            raise market_static.NotLoadedError("market_static 尚未載入任何資料：請先呼叫 refresh()")

    def symbol_spec(self, symbol):
        self._check()
        s = self.specs.get(symbol)
        return None if s is None else dict(s)

    def max_leverage(self, symbol):
        self._check()
        return self.leverage.get(symbol)


def make_entry(spec=S4, symbol="ACE_USDT_PERP", close_ms=CLOSE, price=0.15346, features=None):
    """本檔自建的進場事件（signal_id 用 A3 的 make_signal_id；止盈 / 止損價是隨手挑的，不是策略參數）。"""
    return EntryEvent(strategy=spec.strategy, signal_id=nt.make_signal_id(spec, symbol, close_ms), symbol=symbol,
                      direction=config.DIRECTION_SHORT, created_ms=close_ms + 2 * SEC,
                      bar_open_ms=close_ms - spec.main_ms, signal_price=price, take_profit_price=price * 0.93,
                      stop_loss_price=price * 1.07, features=features or {"ret2h": 0.2, "volr": float("inf")})


def make_exit(entry, hold_ms=30 * MIN, reason=config.EXIT_TAKE_PROFIT, **flags):
    opened = entry.bar_open_ms + SPECS[entry.strategy].main_ms
    feats = {"gap_open": False, "same_bar_both": False, "minor_resolved": False, "minor_missing": False,
             "recovered": False, "data_gap": False, "judged_ms": opened + hold_ms + 2 * SEC}
    feats.update(flags)
    price = entry.take_profit_price if reason == config.EXIT_TAKE_PROFIT else entry.stop_loss_price
    return ExitEvent(strategy=entry.strategy, signal_id=entry.signal_id, symbol=entry.symbol,
                     direction=entry.direction, created_ms=opened + hold_ms + 2 * SEC, reason=reason,
                     exit_price=price, entry_price=entry.signal_price, opened_ms=opened, closed_ms=opened + hold_ms,
                     features=feats)


def result(status, key, message_id=None, uncertain=False, detail=None):
    return SendResult(status=status, key=key, message_id=message_id, uncertain=uncertain, detail=detail)


class ScriptedSender:
    """ChannelSender 的替身（同步）：記下每一次 send()；auto 給了就當場回報結果（None = 先扣著，之後 complete()）。"""

    def __init__(self, auto=None):
        self.calls = []
        self.auto = auto
        self.accept = True
        self.stopped = False
        self._lock = threading.Lock()

    def send(self, text, key=None, *, on_done=None, expires_at=None):
        if not self.accept or self.stopped:
            return False
        call = {"key": key, "text": text, "expires_at": expires_at, "on_done": on_done, "n": len(self.calls) + 1}
        with self._lock:
            self.calls.append(call)
        if self.auto is not None:
            r = self.auto(call)
            if r is not None:
                on_done(r)
        return True

    def complete(self, idx, status, message_id=None, uncertain=False, detail=None):
        c = self.calls[idx]
        c["on_done"](result(status, c["key"], message_id=message_id, uncertain=uncertain, detail=detail))

    def stop(self, timeout=None):
        self.stopped = True
        return 0

    def keys(self):
        return [c["key"] for c in self.calls]


def deliver_all(call):
    return result(RESULT_DELIVERED, call["key"], message_id=100 + call["n"])


def make_pusher(tmp, sender, clock, market=None, name="outbox.sqlite3", **kw):
    kw.setdefault("specs", SPECS)
    return P.ChannelPusher(sender, path=os.path.join(tmp, name), market=market or FakeMarket(), now_ms=clock, **kw)


def settle(p, outbox, rounds=30):
    """同步模式：反覆 pump()，直到沒有新的結果與可以交出的列（或達上限）。"""
    for _ in range(rounds):
        p.pump(outbox)
        if not p.has_results() and p.in_flight() is None and not _due_rows(outbox, p._now()):
            return
    return


def _due_rows(outbox, now):
    return [r for r in outbox.pending() if not r["handoff_open"] and (r["next_attempt_ms"] or 0) <= now
            and not (r["kind"] == OB.KIND_EXIT and (outbox.get(r["signal_id"], OB.KIND_ENTRY) or {}).get(
                "status") == OB.STATUS_PENDING)]


def statuses(outbox):
    return {(r["signal_id"], r["kind"]): r["status"] for r in outbox.rows()}


class Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def messages(self, level=None):
        return [r.getMessage() for r in self.records if level is None or r.levelno == level]


@contextlib.contextmanager
def capture(*names):
    cap = Capture()
    loggers = [logging.getLogger(n) for n in (names or ("live",))]
    saved = [(lg, lg.level) for lg in loggers]
    for lg in loggers:
        lg.addHandler(cap)
        lg.setLevel(logging.DEBUG)
    try:
        yield cap
    finally:
        for lg, level in saved:
            lg.removeHandler(cap)
            lg.setLevel(level)


@contextlib.contextmanager
def synchronous(tmp, sender, clock, **kw):
    """同步模式的 ChannelPusher + 測試執行緒自己的 outbox 連線（不起工作執行緒）。"""
    p = make_pusher(tmp, sender, clock, **kw)
    p.prepare()
    outbox = OB.open_outbox(p.path)
    try:
        yield p, outbox
    finally:
        outbox.close()


def bus_with(p, extra=None):
    bus = SignalBus()
    if extra is not None:
        bus.subscribe(EntryEvent, extra, "other")
        bus.subscribe(ExitEvent, extra, "other")
    p.subscribe(bus)
    return bus


# ============================== AC-2 去重與順序 ==============================
def test_ac2_same_event_published_three_times_is_one_row_and_one_send():
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        sender = ScriptedSender(auto=deliver_all)
        with synchronous(tmp, sender, clock) as (p, outbox):
            bus = bus_with(p)
            e = make_entry()
            reports = [bus.publish(e) for _ in range(3)]
            assert all(r.ok for r in reports), reports
            assert len(outbox.rows()) == 1
            settle(p, outbox)
            for _ in range(2):
                assert bus.publish(e).ok
            settle(p, outbox)
            assert sender.keys() == [e.signal_id + "/entry"], sender.keys()
            assert statuses(outbox) == {(e.signal_id, "entry"): OB.STATUS_DELIVERED}
            row = outbox.get(e.signal_id, "entry")
            assert (row["message_id"], row["attempts"]) == (101, 1), row
            assert p.stats["received"] == 1 and p.stats["duplicates"] == 4, p.stats


def test_ac2_dedupe_survives_reopening_the_outbox():
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        e = make_entry()
        x = make_exit(e)
        s1 = ScriptedSender(auto=deliver_all)
        with synchronous(tmp, s1, clock) as (p, outbox):
            bus = bus_with(p)
            assert bus.publish(e).ok
            settle(p, outbox)
        # 「重啟」：新的 pusher、新的連線、同一個檔
        s2 = ScriptedSender(auto=deliver_all)
        clock.ms += 20 * MIN
        with synchronous(tmp, s2, clock) as (p2, outbox2):
            bus2 = bus_with(p2)
            for _ in range(2):
                assert bus2.publish(e).ok          # A3 重啟補發的舊進場
            for _ in range(2):
                assert bus2.publish(x).ok
            settle(p2, outbox2)
            assert s2.keys() == [x.signal_id + "/exit"], s2.keys()
            assert len(outbox2.rows()) == 2
            assert set(statuses(outbox2).values()) == {OB.STATUS_DELIVERED}
        assert s1.keys() == [e.signal_id + "/entry"]


def test_ac2_exit_never_overtakes_its_pending_entry():
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        sender = ScriptedSender()                  # 結果先扣著
        with synchronous(tmp, sender, clock) as (p, outbox):
            bus = bus_with(p)
            e = make_entry()
            x = make_exit(e, hold_ms=MIN)
            assert bus.publish(e).ok and bus.publish(x).ok
            settle(p, outbox)
            assert sender.keys() == [e.signal_id + "/entry"], "進場還沒有結果，出場不可以交出"
            sender.complete(0, RESULT_GAVE_UP, uncertain=False, detail="重試用盡")
            settle(p, outbox)
            assert sender.keys() == [e.signal_id + "/entry"], "進場還是待送（等重試），出場不可以先送"
            assert outbox.get(x.signal_id, "exit")["status"] == OB.STATUS_PENDING
            clock.ms += RETRY_MS
            settle(p, outbox)
            assert sender.keys() == [e.signal_id + "/entry"] * 2
            sender.complete(1, RESULT_DELIVERED, message_id=5)
            settle(p, outbox)
            assert sender.keys() == [e.signal_id + "/entry"] * 2 + [x.signal_id + "/exit"], sender.keys()
            sender.complete(2, RESULT_DELIVERED, message_id=6)
            settle(p, outbox)
            assert set(statuses(outbox).values()) == {OB.STATUS_DELIVERED}


def test_ac2_s4_and_s5_same_coin_same_time_are_independent():
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        sender = ScriptedSender(auto=deliver_all)
        with synchronous(tmp, sender, clock) as (p, outbox):
            bus = bus_with(p)
            e4, e5 = make_entry(S4), make_entry(S5)
            assert e4.signal_id != e5.signal_id and e4.symbol == e5.symbol
            x4, x5 = make_exit(e4, hold_ms=5 * MIN), make_exit(e5, hold_ms=3 * MIN, reason=config.EXIT_STOP_LOSS)
            for ev in (e4, e5, x5, x4):
                assert bus.publish(ev).ok
            settle(p, outbox)
            assert sorted(sender.keys()) == sorted([e4.signal_id + "/entry", e5.signal_id + "/entry",
                                                    x4.signal_id + "/exit", x5.signal_id + "/exit"])
            assert set(statuses(outbox).values()) == {OB.STATUS_DELIVERED}
            texts = {c["key"]: c["text"] for c in sender.calls}
            assert "策略4" in texts[e4.signal_id + "/entry"] and "策略5" in texts[e5.signal_id + "/entry"]
        # 一邊延遲不發，不影響另一邊
        with tempdir() as tmp:
            clock = Clock(CLOSE + 10 * SEC)
            sender = ScriptedSender(auto=deliver_all)
            with synchronous(tmp, sender, clock) as (p, outbox):
                bus = bus_with(p)
                late4 = make_entry(S4, close_ms=CLOSE - 10 * MIN)       # 已經晚 10 分鐘
                e5 = make_entry(S5)
                for ev in (late4, e5, make_exit(late4), make_exit(e5)):
                    assert bus.publish(ev).ok
                settle(p, outbox)
                st = statuses(outbox)
                assert st[(late4.signal_id, "entry")] == OB.STATUS_EXPIRED
                assert st[(late4.signal_id, "exit")] == OB.STATUS_NO_ENTRY
                assert st[(e5.signal_id, "entry")] == st[(e5.signal_id, "exit")] == OB.STATUS_DELIVERED


# ============================== AC-4 延遲進場 ==============================
def test_ac4_entry_299s_is_sent_301s_is_expired_and_its_exit_is_not_sent():
    for delay_s, expect in ((299, OB.STATUS_DELIVERED), (300, OB.STATUS_DELIVERED), (301, OB.STATUS_EXPIRED)):
        with tempdir() as tmp, capture("live.a_channel_push") as cap:
            clock = Clock(CLOSE + delay_s * SEC)
            sender = ScriptedSender(auto=deliver_all)
            with synchronous(tmp, sender, clock) as (p, outbox):
                bus = bus_with(p)
                e = make_entry()
                assert bus.publish(e).ok and bus.publish(make_exit(e)).ok
                settle(p, outbox)
                st = statuses(outbox)
                assert st[(e.signal_id, "entry")] == expect, (delay_s, st)
                if expect == OB.STATUS_EXPIRED:
                    assert st[(e.signal_id, "exit")] == OB.STATUS_NO_ENTRY
                    assert sender.calls == [], "延遲不發的進場與它的出場都不可以交給發送器"
                    warns = cap.messages(logging.WARNING)
                    assert any(e.signal_id in w and "301.0" in w and "延遲不發" in w for w in warns), warns
                    assert outbox.delivered_entry_signal_ids() == []
                else:
                    assert st[(e.signal_id, "exit")] == OB.STATUS_DELIVERED
                    # 交給發送器時帶期限 = 收盤 + 門檻：發送器每次實際發出請求之前再檢查一次
                    assert sender.calls[0]["expires_at"] == (CLOSE + DELAY_MS) / 1000.0
                    assert sender.calls[1]["expires_at"] is None, "出場不套延遲門檻"
                    assert outbox.delivered_entry_signal_ids() == [e.signal_id]


def _real_sender_setup(h, delay_s):
    """真的 ChannelSender（假 HTTP + 假時鐘）：假時鐘的牆上時間設成 收盤 + delay_s。"""
    h.clock.t = CLOSE / 1000.0 - FakeClock.EPOCH + delay_s
    sender = h.started()
    return sender, (lambda: int(round(h.clock.wall() * 1000)))


def _drive(p, outbox, sender_obj, limit=5.0):
    """同步模式 + 真的發送器：pump → 等發送器回報 → pump，直到沒有正在交出的。"""
    end = time.monotonic() + limit
    p.pump(outbox)
    while p.in_flight() is not None or p.has_results():
        if time.monotonic() > end:
            raise AssertionError("等不到發送器的結果")
        if p.has_results():
            p.pump(outbox)
        else:
            threading.Event().wait(0.002)
    p.pump(outbox)


def test_ac4_expired_while_waiting_for_429_inside_the_sender():
    with tempdir() as tmp, harness() as h:
        h.tg.plan(too_many(250))
        sender, now_ms = _real_sender_setup(h, 100)
        with synchronous(tmp, sender, now_ms) as (p, outbox):
            bus = bus_with(p)
            e = make_entry()
            assert bus.publish(e).ok and bus.publish(make_exit(e)).ok
            _drive(p, outbox, sender)
            _drive(p, outbox, sender)
            st = statuses(outbox)
            assert st[(e.signal_id, "entry")] == OB.STATUS_EXPIRED, st
            assert st[(e.signal_id, "exit")] == OB.STATUS_NO_ENTRY, st
            assert len(h.tg.calls) == 1, "429 之後已過期，不可以重送"
            assert "發送器" in outbox.get(e.signal_id, "entry")["detail"]
        tgt.stop_within(sender, 60)


def test_ac4_uncertain_first_attempt_then_expired_is_still_sent():
    with tempdir() as tmp, harness() as h:
        h.tg.plan(read_timeout, ok(77))
        sender, now_ms = _real_sender_setup(h, 299)
        with synchronous(tmp, sender, now_ms) as (p, outbox):
            bus = bus_with(p)
            e = make_entry()
            assert bus.publish(e).ok
            _drive(p, outbox, sender)
            row = outbox.get(e.signal_id, "entry")
            assert (row["status"], row["message_id"]) == (OB.STATUS_DELIVERED, 77), row
            t = h.tg.times()
            assert len(t) == 2 and h.clock.wall() * 1000 - CLOSE > DELAY_MS, "前提：第二次嘗試時已經過了門檻"
        tgt.stop_within(sender, 60)


def test_ac4_uncertain_at_t2_level_then_expired_is_still_sent_with_the_same_text():
    for uncertain, expect in ((True, OB.STATUS_DELIVERED), (False, OB.STATUS_EXPIRED)):
        with tempdir() as tmp:
            clock = Clock(CLOSE + 250 * SEC)
            sender = ScriptedSender()
            with synchronous(tmp, sender, clock) as (p, outbox):
                bus = bus_with(p)
                e = make_entry()
                assert bus.publish(e).ok
                settle(p, outbox)
                sender.complete(0, RESULT_GAVE_UP, uncertain=uncertain, detail="ReadTimeout")
                settle(p, outbox)
                clock.ms += RETRY_MS                       # 重試時已晚 310 秒
                settle(p, outbox)
                if uncertain:
                    assert len(sender.calls) == 2 and sender.calls[1]["expires_at"] is None
                    assert sender.calls[1]["text"] == sender.calls[0]["text"]
                    sender.complete(1, RESULT_DELIVERED, message_id=9)
                    settle(p, outbox)
                else:
                    assert len(sender.calls) == 1
                assert outbox.get(e.signal_id, "entry")["status"] == expect, uncertain


def test_ac4_late_exit_is_sent_with_the_delay_note():
    cases = ((299, False, False), (301, False, True), (10, True, True))    # (交出時晚於平倉幾秒, recovered, 註記)
    for late_s, recovered, noted in cases:
        with tempdir() as tmp:
            clock = Clock(CLOSE + 10 * SEC)
            sender = ScriptedSender(auto=deliver_all)
            with synchronous(tmp, sender, clock) as (p, outbox):
                bus = bus_with(p)
                e = make_entry()
                assert bus.publish(e).ok
                settle(p, outbox)
                x = make_exit(e, hold_ms=2 * 60 * MIN, recovered=recovered)
                clock.ms = x.closed_ms + late_s * SEC
                assert bus.publish(x).ok
                settle(p, outbox)
                assert outbox.get(x.signal_id, "exit")["status"] == OB.STATUS_DELIVERED
                text = sender.calls[-1]["text"]
                assert (T.DELAY_NOTE in text) == noted, (late_s, recovered, text)


def test_uncertain_exit_resend_keeps_the_original_text():
    """結果不確定的出場，重試時已晚過門檻：仍重送同一段文字（不會多出延遲註記、頻道上不會有兩個版本）。"""
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        sender = ScriptedSender()
        with synchronous(tmp, sender, clock) as (p, outbox):
            bus = bus_with(p)
            e = make_entry()
            assert bus.publish(e).ok
            settle(p, outbox)
            sender.complete(0, RESULT_DELIVERED, message_id=1)
            x = make_exit(e, hold_ms=MIN)
            clock.ms = x.closed_ms + 5 * SEC
            assert bus.publish(x).ok
            settle(p, outbox)
            sender.complete(1, RESULT_GAVE_UP, uncertain=True)
            settle(p, outbox)
            clock.ms += 10 * RETRY_MS
            settle(p, outbox)
            assert sender.calls[2]["text"] == sender.calls[1]["text"]
            assert T.DELAY_NOTE not in sender.calls[2]["text"]


# ============================== 其他結果分支 ==============================
def test_permanent_failure_is_terminal_and_its_exit_is_not_sent():
    with tempdir() as tmp, capture("live.a_channel_push") as cap:
        clock = Clock(CLOSE + 10 * SEC)
        sender = ScriptedSender(auto=lambda c: result(RESULT_FAILED, c["key"], detail="HTTP 403：Forbidden"))
        with synchronous(tmp, sender, clock) as (p, outbox):
            bus = bus_with(p)
            e = make_entry()
            assert bus.publish(e).ok and bus.publish(make_exit(e)).ok
            settle(p, outbox)
            clock.ms += 10 * RETRY_MS
            settle(p, outbox)
            st = statuses(outbox)
            assert st == {(e.signal_id, "entry"): OB.STATUS_FAILED, (e.signal_id, "exit"): OB.STATUS_NO_ENTRY}, st
            assert len(sender.calls) == 1, "永久失敗不可以重送"
            assert any("永久失敗" in m and e.signal_id in m for m in cap.messages(logging.ERROR))


def test_gave_up_and_abandoned_stay_pending_and_retry_after_the_interval():
    for status in (RESULT_GAVE_UP, RESULT_ABANDONED):
        with tempdir() as tmp:
            clock = Clock(CLOSE + 10 * SEC)
            sender = ScriptedSender()
            with synchronous(tmp, sender, clock) as (p, outbox):
                bus = bus_with(p)
                e = make_entry()
                assert bus.publish(e).ok
                settle(p, outbox)
                sender.complete(0, status)
                settle(p, outbox)
                row = outbox.get(e.signal_id, "entry")
                assert (row["status"], row["uncertain"], row["next_attempt_ms"]) == \
                    (OB.STATUS_PENDING, False, clock.ms + RETRY_MS), row
                clock.ms += RETRY_MS - 1
                settle(p, outbox)
                assert len(sender.calls) == 1, "重試間隔還沒到"
                clock.ms += 1
                settle(p, outbox)
                assert len(sender.calls) == 2


def test_sender_rejection_keeps_the_row_pending_and_certain():
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        sender = ScriptedSender(auto=deliver_all)
        sender.accept = False
        with synchronous(tmp, sender, clock) as (p, outbox):
            bus = bus_with(p)
            e = make_entry()
            assert bus.publish(e).ok
            settle(p, outbox)
            row = outbox.get(e.signal_id, "entry")
            assert (row["status"], row["handoff_open"], row["uncertain"]) == (OB.STATUS_PENDING, False, False), row
            sender.accept = True
            clock.ms += RETRY_MS
            settle(p, outbox)
            assert outbox.get(e.signal_id, "entry")["status"] == OB.STATUS_DELIVERED


def test_exit_without_an_entry_row_is_not_sent():
    """推播啟用之前就發布過的進場（outbox 裡沒有進場列）：出場標「因進場未出現而不發」。"""
    with tempdir() as tmp:
        clock = Clock(CLOSE + 60 * MIN)
        sender = ScriptedSender(auto=deliver_all)
        with synchronous(tmp, sender, clock) as (p, outbox):
            bus = bus_with(p)
            x = make_exit(make_entry())
            assert bus.publish(x).ok
            settle(p, outbox)
            assert statuses(outbox) == {(x.signal_id, "exit"): OB.STATUS_NO_ENTRY}
            assert sender.calls == []
            assert "推播啟用之前" in outbox.get(x.signal_id, "exit")["detail"]


def test_restart_treats_an_unreported_handoff_as_uncertain():
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        sender = ScriptedSender()
        with synchronous(tmp, sender, clock) as (p, outbox):
            bus = bus_with(p)
            e = make_entry()
            assert bus.publish(e).ok
            settle(p, outbox)
            assert outbox.get(e.signal_id, "entry")["handoff_open"] is True
        clock.ms = CLOSE + 30 * MIN                        # 重啟時早就過了門檻
        s2 = ScriptedSender(auto=deliver_all)
        with capture("live.a_channel_push") as cap:
            with synchronous(tmp, s2, clock) as (p2, outbox2):
                row = outbox2.get(e.signal_id, "entry")
                assert (row["uncertain"], row["handoff_open"]) == (True, False), row
                settle(p2, outbox2)
                assert outbox2.get(e.signal_id, "entry")["status"] == OB.STATUS_DELIVERED
                assert s2.calls[0]["expires_at"] is None and s2.calls[0]["text"] == sender.calls[0]["text"]
        assert any("沒有記回結果的 1 則" in m for m in cap.messages(logging.INFO)), cap.messages()


def test_shutdown_records_the_result_that_arrives_during_sender_stop():
    """兩段式關閉：停止派送 → sender.stop()（期間送達）→ 記回結果。送達的那一則不可以留成「待送」。"""
    with tempdir() as tmp, harness() as h:
        h.tg.gate = threading.Event()
        sender, now_ms = _real_sender_setup(h, 10)
        p = make_pusher(tmp, sender, now_ms)
        p.start()
        bus = bus_with(p)
        e = make_entry()
        x = make_exit(e)
        assert bus.publish(e).ok and bus.publish(x).ok
        assert h.tg.entered.wait(5)
        box = {}
        t = threading.Thread(target=lambda: box.setdefault("pending", p.shutdown(30)), daemon=True)
        t.start()
        wait_until(lambda: sender.stats()["state"] == "stopping", "sender.stop() 開始")
        h.tg.gate.set()
        t.join(10)
        assert not t.is_alive() and box["pending"] == 1, box            # 出場沒交出去：留在 outbox
        with OB.open_outbox(p.path) as outbox:
            st = statuses(outbox)
        assert st == {(e.signal_id, "entry"): OB.STATUS_DELIVERED, (x.signal_id, "exit"): OB.STATUS_PENDING}, st
        assert not p.worker_alive


# ============================== AC-5 handler 規則 ==============================
def test_ac5_market_static_not_loaded_makes_the_publish_report_fail():
    assert not market_static.is_loaded(), "前提：本行程沒有載入過 market_static"
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        seen = []
        with synchronous(tmp, ScriptedSender(auto=deliver_all), clock, market=market_static) as (p, outbox):
            bus = bus_with(p, extra=seen.append)
            e = make_entry()
            with capture("live.bus"):
                r = bus.publish(e)
            assert not r.ok and r.failed == (P.SUBSCRIBER_NAME,) and r.delivered == ("other",), r
            assert outbox.rows() == [], "未載入時不可以猜值寫進 outbox"
            # 出場不查 market_static（精度沿用進場列）：沒有進場列 → 照收、之後標「因進場未出現而不發」，不會卡住 A3
            r = bus.publish(make_exit(e))
            assert r.ok, r
            settle(p, outbox)
            assert statuses(outbox) == {(e.signal_id, "exit"): OB.STATUS_NO_ENTRY}
        # A3 下一次 tick 重送：載入之後就成功
        market = FakeMarket(loaded=False)
        with synchronous(tmp, ScriptedSender(auto=deliver_all), clock, market=market, name="o2.sqlite3") as (p, ob2):
            bus = bus_with(p)
            with capture("live.bus"):
                assert not bus.publish(e).ok
            market.loaded = True
            assert bus.publish(e).ok
            assert len(ob2.rows()) == 1


def test_ac5_outbox_write_failures_make_the_publish_report_fail():
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        with synchronous(tmp, ScriptedSender(auto=deliver_all), clock, busy_timeout=0.05) as (p, outbox):
            bus = bus_with(p)
            e = make_entry()
            # (a) 被鎖住（另一條連線拿著寫入鎖）
            lock = sqlite3.connect(p.path, isolation_level=None)
            lock.execute("BEGIN EXCLUSIVE")
            with capture("live.bus"):
                t0 = time.perf_counter()
                r = bus.publish(e)
                waited = time.perf_counter() - t0
            lock.execute("ROLLBACK")
            lock.close()
            assert not r.ok and r.failed == (P.SUBSCRIBER_NAME,), r
            assert waited < 2.0, "handler 等鎖等太久：%.3f 秒" % waited
            # (b) 唯讀檔案
            outbox.close()
            os.chmod(p.path, stat.S_IREAD)
            try:
                with capture("live.bus"):
                    r = bus.publish(e)
            finally:
                os.chmod(p.path, stat.S_IREAD | stat.S_IWRITE)
            assert not r.ok, r
            # (c) 寫入本身拋錯（例如磁碟錯誤）
            real = OB.Outbox.insert

            def broken(self, **kw):
                raise sqlite3.OperationalError("disk I/O error")
            OB.Outbox.insert = broken
            try:
                with capture("live.bus"):
                    assert not bus.publish(e).ok
            finally:
                OB.Outbox.insert = real
            # 恢復之後 A3 重送就成功，而且只有一列
            assert bus.publish(e).ok and bus.publish(e).ok
            with OB.open_outbox(p.path) as again:
                assert len(again.rows()) == 1
        # (d) outbox 檔被刪掉：不可以悄悄建一個空的
        with synchronous(tmp, ScriptedSender(), clock, name="gone.sqlite3") as (p, outbox):
            outbox.close()
            for suffix in ("", "-wal", "-shm"):
                if os.path.exists(p.path + suffix):
                    os.remove(p.path + suffix)
            bus = bus_with(p)
            with capture("live.bus"):
                assert not bus.publish(make_entry()).ok
            assert not os.path.exists(p.path)


def test_ac5_handler_is_local_only_and_fast():
    """handler 不連網、不交給發送器（只寫 outbox）；量 200 次的耗時（p50 / p99 印出來，正式數字由 DQA 量）。"""
    with tempdir() as tmp, OfflineCage() as cage:
        clock = Clock(CLOSE + 10 * SEC)
        sender = ScriptedSender(auto=deliver_all)
        with synchronous(tmp, sender, clock) as (p, outbox):
            durations = []
            for i in range(200):
                ev = make_entry(symbol="ACE_USDT_PERP", close_ms=CLOSE - i * S4.main_ms)
                t0 = time.perf_counter()
                p.handle(ev)
                durations.append(time.perf_counter() - t0)
            assert sender.calls == [], "handler 不可以直接交給發送器"
            assert len(outbox.rows()) == 200
        assert not cage.attempts and not cage.sleeps
    durations.sort()
    p50, p99 = durations[len(durations) // 2], durations[int(len(durations) * 0.99) - 1]
    print("      handler 耗時（本機 200 次）：p50 %.1f ms、p99 %.1f ms、最大 %.1f ms"
          % (p50 * 1000, p99 * 1000, durations[-1] * 1000))
    assert p99 < 1.0, "handler 的 p99 超過 1 秒：%.3f" % p99


# ============================== 第 1 輪：BUG-009 記回結果失敗 ==============================
class Mono:
    """假的單調時鐘（秒）：只推進 BUG-009 的退避，不動業務時鐘。"""

    def __init__(self, t=100.0):
        self.t = float(t)

    def __call__(self):
        return self.t


def _failing(real, n, calls, exc=None):
    """包一個 Outbox 方法：前 n 次呼叫拋 sqlite3.OperationalError（模擬被鎖住 / 磁碟錯誤），之後照常。calls 記次數。"""
    def wrapper(*a, **k):
        calls.append(1)
        if len(calls) <= n:
            raise exc or sqlite3.OperationalError("database is locked")
        return real(*a, **k)
    return wrapper


def test_bug009_delivered_result_is_kept_when_recording_fails_and_recorded_later():
    """BUG-009：記回「已送達」時 outbox 寫不進去 → 結果保留在記憶體、不交新的列、不忙等；重試間隔到了記成
    delivered，同一 signal_id 的止損出場在同一個行程內送出，進場不重送（頻道不重複）。"""
    with tempdir() as tmp, capture("live.a_channel_push") as cap:
        clock, mono = Clock(CLOSE + 10 * SEC), Mono()
        sender = ScriptedSender()
        with synchronous(tmp, sender, clock, mono=mono) as (p, outbox):
            bus = bus_with(p)
            e = make_entry()
            x = make_exit(e, hold_ms=MIN, reason=config.EXIT_STOP_LOSS)
            assert bus.publish(e).ok
            settle(p, outbox)
            assert sender.keys() == [e.signal_id + "/entry"]
            calls = []
            real = outbox.finalize
            outbox.finalize = _failing(real, 10 ** 6, calls)          # 一直寫不進去，直到換回來
            sender.complete(0, RESULT_DELIVERED, message_id=11)
            p.pump(outbox)
            assert len(calls) == 1 and p.has_results() and p.in_flight() is not None
            row = outbox.get(e.signal_id, "entry")
            assert (row["status"], row["handoff_open"]) == (OB.STATUS_PENDING, True), row
            assert any("記不回 outbox" in m for m in cap.messages(logging.ERROR)), cap.messages(logging.ERROR)
            assert bus.publish(x).ok                                    # handler 用自己的連線，照常落地
            for _ in range(20):                                         # 重試間隔還沒到：不再試、也不交新的列
                p.pump(outbox)
            mono.t += config.A_CHANNEL_OUTBOX_RETRY_SECONDS - 0.001
            p.pump(outbox)
            assert len(calls) == 1, "重試間隔還沒到就又試了（忙等）：%d 次" % len(calls)
            assert sender.keys() == [e.signal_id + "/entry"], "結果還沒記回之前不可以交出新的列"
            mono.t += 0.001
            p.pump(outbox)
            assert len(calls) == 2 and p.stats["record_failures"] == 2
            outbox.finalize = real                                      # outbox 恢復可寫
            mono.t += config.A_CHANNEL_OUTBOX_RETRY_SECONDS
            settle(p, outbox)
            row = outbox.get(e.signal_id, "entry")
            assert (row["status"], row["message_id"], row["handoff_open"]) == (OB.STATUS_DELIVERED, 11, False), row
            assert sender.keys() == [e.signal_id + "/entry", x.signal_id + "/exit"], sender.keys()
            sender.complete(1, RESULT_DELIVERED, message_id=12)
            settle(p, outbox)
            assert statuses(outbox) == {(e.signal_id, "entry"): OB.STATUS_DELIVERED,
                                        (x.signal_id, "exit"): OB.STATUS_DELIVERED}
            assert p.stats["orphans"] == 0


def test_bug009_real_lock_longer_than_busy_timeout_in_thread_mode():
    """DQA 的重現情境（真的工作執行緒、真的 BEGIN EXCLUSIVE 鎖住超過 busy timeout、業務時鐘凍結）：
    解鎖後同一行程內進場記成 delivered、止損出場送出、進場不重送；鎖住期間沒有忙等。"""
    with tempdir() as tmp, capture("live.a_channel_push") as cap:
        released = threading.Event()
        holder = {}

        class HoldFirst:
            """第一則扣住，等測試放行後在另一條執行緒回報 delivered（跟 ChannelSender 一樣不在呼叫端執行緒回報）。"""

            def __init__(self):
                self.keys = []

            def send(self, text, key=None, *, on_done=None, expires_at=None):
                self.keys.append(key)
                if len(self.keys) == 1:
                    holder["cb"] = (on_done, key)
                    released.set()
                else:
                    threading.Thread(target=on_done, args=(result(RESULT_DELIVERED, key, message_id=len(self.keys)),),
                                     daemon=True).start()
                return True

            def stop(self, timeout=None):
                return 0

        sender = HoldFirst()
        attempts = []
        real = OB.Outbox.finalize

        def spy(self, *a, **k):
            attempts.append(time.monotonic())
            return real(self, *a, **k)
        OB.Outbox.finalize = spy
        try:
            p = make_pusher(tmp, sender, Clock(CLOSE + 10 * SEC), retry_seconds=0.2, worker_busy_timeout=0.3)
            p.start()
            try:
                bus = bus_with(p)
                e = make_entry()
                assert bus.publish(e).ok
                assert released.wait(5), "第一則沒有交出去"
                lock = sqlite3.connect(p.path, isolation_level=None)
                lock.execute("BEGIN EXCLUSIVE")
                t0 = time.monotonic()
                on_done, key = holder["cb"]
                threading.Thread(target=on_done, args=(result(RESULT_DELIVERED, key, message_id=1),),
                                 daemon=True).start()
                threading.Event().wait(1.2)                             # 鎖 1.2 秒（busy timeout 0.3 秒）
                locked_attempts = len([a for a in attempts if a >= t0])
                lock.execute("ROLLBACK")
                lock.close()
                assert 1 <= locked_attempts <= 6, "鎖住期間嘗試 %d 次（忙等？）" % locked_attempts
                x = make_exit(e, hold_ms=MIN, reason=config.EXIT_STOP_LOSS)
                assert bus.publish(x).ok

                def both_delivered():
                    with OB.open_outbox(p.path) as ob:
                        return ob.counts()[OB.STATUS_DELIVERED] == 2
                wait_until(both_delivered, "解鎖後進場與出場都記成 delivered", limit=10.0)
                assert sender.keys == [e.signal_id + "/entry", x.signal_id + "/exit"], sender.keys
                assert p.stats["record_failures"] >= 1 and p.stats["orphans"] == 0, p.stats
                assert any("記不回 outbox" in m for m in cap.messages(logging.ERROR))
            finally:
                p.shutdown(1)
            with OB.open_outbox(p.path) as ob:
                assert ob.counts()[OB.STATUS_PENDING] == 0
        finally:
            OB.Outbox.finalize = real


def test_bug009_rejection_that_cannot_be_recorded_is_retried_and_stays_certain():
    """_handoff 的拒收路徑：record_retry 寫不進去 → 同樣保留、稍後再記；記回之後是「確定沒送出」（不會被當成
    孤兒標成不確定），下一次交出仍然帶期限。"""
    with tempdir() as tmp:
        clock, mono = Clock(CLOSE + 10 * SEC), Mono()
        sender = ScriptedSender(auto=deliver_all)
        sender.accept = False
        with synchronous(tmp, sender, clock, mono=mono) as (p, outbox):
            bus = bus_with(p)
            e = make_entry()
            assert bus.publish(e).ok
            calls = []
            real = outbox.record_retry
            outbox.record_retry = _failing(real, 1, calls)
            p.pump(outbox)                                              # 交出 → 拒收 → 結果排進佇列
            p.pump(outbox)                                              # 記回失敗
            assert len(calls) == 1 and p.in_flight() is not None
            assert outbox.get(e.signal_id, "entry")["handoff_open"] is True
            p.pump(outbox)
            assert len(calls) == 1, "重試間隔還沒到就又試了"
            mono.t += config.A_CHANNEL_OUTBOX_RETRY_SECONDS
            p.pump(outbox)
            assert len(calls) == 2 and p.in_flight() is None
            row = outbox.get(e.signal_id, "entry")
            assert (row["handoff_open"], row["uncertain"], row["next_attempt_ms"]) == (False, False,
                                                                                       clock.ms + RETRY_MS), row
            outbox.record_retry = real
            sender.accept = True
            clock.ms += RETRY_MS
            settle(p, outbox)
            assert outbox.get(e.signal_id, "entry")["status"] == OB.STATUS_DELIVERED
            assert sender.calls[-1]["expires_at"] == (CLOSE + DELAY_MS) / 1000.0, "確定沒送出的列仍要套期限"
            assert p.stats["orphans"] == 0


def test_bug009_orphan_handoff_is_released_as_uncertain():
    """第二道防線：交出去了、結果卻沒有記回來的列（不在 _in_flight）→ 比照重啟當成不確定、重送同一段文字。"""
    with tempdir() as tmp, capture("live.a_channel_push") as cap:
        clock = Clock(CLOSE + 10 * SEC)
        sender = ScriptedSender(auto=deliver_all)
        with synchronous(tmp, sender, clock) as (p, outbox):
            e = make_entry()
            p.handle(e)
            seq = outbox.get(e.signal_id, "entry")["seq"]
            outbox.mark_handoff(seq, now_ms=clock.ms, text="先前交出的那一段文字")   # 模擬結果遺失
            clock.ms = CLOSE + 20 * MIN                                 # 已過門檻：若判成確定沒送出就會 expired
            settle(p, outbox)
            row = outbox.get(e.signal_id, "entry")
            assert (row["status"], row["uncertain"]) == (OB.STATUS_DELIVERED, True), row
            assert sender.calls[0]["text"] == "先前交出的那一段文字" and sender.calls[0]["expires_at"] is None
            assert p.stats["orphans"] == 1
            assert any("結果沒有記回" in m for m in cap.messages(logging.WARNING)), cap.messages(logging.WARNING)


def test_bug009_final_pump_retries_once_without_waiting_for_the_interval():
    with tempdir() as tmp:
        clock, mono = Clock(CLOSE + 10 * SEC), Mono()
        sender = ScriptedSender()
        with synchronous(tmp, sender, clock, mono=mono) as (p, outbox):
            e = make_entry()
            p.handle(e)
            settle(p, outbox)
            calls = []
            real = outbox.finalize
            outbox.finalize = _failing(real, 1, calls)
            sender.complete(0, RESULT_DELIVERED, message_id=3)
            p.pump(outbox)
            assert len(calls) == 1
            p.pump(outbox)
            assert len(calls) == 1
            p.pump(outbox, final=True)                                  # 關閉前最後一輪：不等重試間隔
            assert len(calls) == 2
            assert outbox.get(e.signal_id, "entry")["status"] == OB.STATUS_DELIVERED


# ============================== 第 1 輪：F6 收到時就已過期的進場 ==============================
def test_f6_stale_entry_at_receipt_does_not_touch_market_static():
    """market_static 未載入 + 收到時已晚於門檻的進場（A3 重啟補發的舊進場）→ 真的 SignalBus 送達報告 ok=True（不記
    ERROR）、outbox 最終 expired、發送器沒收到；新鮮的進場碰到未載入照舊 ok=False。"""
    assert not market_static.is_loaded(), "前提：本行程沒有載入過 market_static"
    with tempdir() as tmp, capture("live.bus") as buscap:
        clock = Clock(CLOSE + DELAY_MS + 1)
        sender = ScriptedSender(auto=deliver_all)
        with synchronous(tmp, sender, clock, market=market_static) as (p, outbox):
            bus = bus_with(p)
            stale = make_entry()
            r = bus.publish(stale)
            assert r.ok, r
            snap = outbox.get(stale.signal_id, "entry")["snapshot"]
            assert snap["price_decimals_source"] == P.PRICE_SOURCE_STALE and snap["price_decimals"] is None, snap
            assert bus.publish(make_exit(stale)).ok
            settle(p, outbox)
            assert statuses(outbox) == {(stale.signal_id, "entry"): OB.STATUS_EXPIRED,
                                        (stale.signal_id, "exit"): OB.STATUS_NO_ENTRY}
            assert sender.calls == []
            assert not [x for x in buscap.records if x.levelno >= logging.ERROR], "舊進場不該讓匯流排記 ERROR"
            fresh = make_entry(close_ms=CLOSE + DELAY_MS)               # 收到時晚 1 毫秒以內：新鮮
            r = bus.publish(fresh)
            assert not r.ok and r.failed == (P.SUBSCRIBER_NAME,), "新鮮的進場碰到未載入仍要拋例外（不猜值）"
    # 邊界：剛好 300 秒不算過期，照常取快照
    with tempdir() as tmp:
        clock = Clock(CLOSE + DELAY_MS)
        with synchronous(tmp, ScriptedSender(), clock) as (p, outbox):
            e = make_entry()
            p.handle(e)
            assert outbox.get(e.signal_id, "entry")["snapshot"]["price_decimals_source"] == P.PRICE_SOURCE_SPEC


def test_f6_stale_entry_is_never_rendered_even_if_the_clock_goes_back():
    with tempdir() as tmp, capture("live.a_channel_push") as cap:
        clock = Clock(CLOSE + 400 * SEC)
        sender = ScriptedSender(auto=deliver_all)
        with synchronous(tmp, sender, clock) as (p, outbox):
            e = make_entry()
            p.handle(e)
            clock.ms = CLOSE + 10 * SEC                                 # 系統時鐘往回調
            settle(p, outbox)
            row = outbox.get(e.signal_id, "entry")
            assert row["status"] == OB.STATUS_EXPIRED and row["text"] is None and sender.calls == [], row
            assert "收到時" in row["detail"] and "400.0" in row["detail"], row["detail"]
            warns = cap.messages(logging.WARNING)
            assert any(e.signal_id in w and "延遲不發" in w and "400.0" in w for w in warns), warns


# ============================== 快照 ==============================
def test_snapshot_price_precision_and_leverage():
    with tempdir() as tmp, capture("live.a_channel_push") as cap:
        clock = Clock(CLOSE + 10 * SEC)
        market = FakeMarket(specs={"STR_USDT_PERP": {"quotePrecision": "4"}, "BOOL_USDT_PERP": {"quotePrecision": True},
                                   "BIG_USDT_PERP": {"quotePrecision": 99}},
                            leverage={"STR_USDT_PERP": 20, "BOOL_USDT_PERP": 0, "NOSPEC_USDT_PERP": None})
        with synchronous(tmp, ScriptedSender(), clock, market=market) as (p, outbox):
            want = {"ACE_USDT_PERP": (5, P.PRICE_SOURCE_SPEC, 75),
                    "STR_USDT_PERP": (4, P.PRICE_SOURCE_SPEC, 20),
                    "BOOL_USDT_PERP": (T.price_decimals_for(0.15346, 6), P.PRICE_SOURCE_FALLBACK, None),
                    "BIG_USDT_PERP": (6, P.PRICE_SOURCE_FALLBACK, None),
                    "NOSPEC_USDT_PERP": (6, P.PRICE_SOURCE_FALLBACK, None)}
            for symbol in want:
                p.handle(make_entry(symbol=symbol))
            for row in outbox.rows():
                s = row["snapshot"]
                got = (s["price_decimals"], s["price_decimals_source"], s["max_leverage"])
                assert got == want[row["symbol"]], (row["symbol"], got)
                assert s["signal_close_ms"] == CLOSE and s["label"] == config.STRATEGY_LABELS["s4"]
                assert (s["order_pct"], s["target_leverage"], s["deviation_warn_pct"]) == (
                    config.A_CHANNEL_ORDER_PCT, config.A_CHANNEL_TARGET_LEVERAGE, config.A_CHANNEL_DEVIATION_WARN_PCT)
        warns = cap.messages(logging.WARNING)
        assert any("NOSPEC_USDT_PERP" in w and "槓桿上限" in w for w in warns), warns
        assert any("NOSPEC_USDT_PERP" in w and "價格精度" in w for w in warns), warns
        assert any("BOOL_USDT_PERP" in w and "不合理" in w for w in warns), warns
    assert P.price_decimals_from_spec(None) is None and P.price_decimals_from_spec({"quotePrecision": -1}) is None
    assert P.price_decimals_from_spec({"quotePrecision": 3.0}) == 3


def test_exit_price_precision_follows_the_entry_snapshot():
    """出場的價格精度沿用進場列的快照：之後 market_static 沒載入、或精度變了，出場訊息的「進場價」仍與進場訊息的
    「訊號價」一模一樣。"""
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        sender = ScriptedSender(auto=deliver_all)
        market = FakeMarket(specs={"ACE_USDT_PERP": {"quotePrecision": 4}})
        with synchronous(tmp, sender, clock, market=market) as (p, outbox):
            bus = bus_with(p)
            e = make_entry(price=0.15346)
            assert bus.publish(e).ok
            settle(p, outbox)
            market.specs["ACE_USDT_PERP"] = {"quotePrecision": 6}   # 交易所之後改了跳動單位
            market.loaded = False                                    # 甚至沒載入
            x = make_exit(e)
            assert bus.publish(x).ok
            settle(p, outbox)
            snap = outbox.get(x.signal_id, "exit")["snapshot"]
            assert (snap["price_decimals"], snap["price_decimals_source"]) == (4, P.PRICE_SOURCE_ENTRY), snap
            entry_text, exit_text = sender.calls[0]["text"], sender.calls[1]["text"]
            assert "訊號價：0.1535" in entry_text.splitlines(), entry_text
            assert "進場價：0.1535" in exit_text.splitlines(), exit_text


def test_unknown_leverage_message_does_not_invent_a_limit():
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        sender = ScriptedSender(auto=deliver_all)
        market = FakeMarket(specs={"NEW_USDT_PERP": {"quotePrecision": 6}})
        with synchronous(tmp, sender, clock, market=market) as (p, outbox):
            bus_with(p).publish(make_entry(symbol="NEW_USDT_PERP", price=0.004321))
            settle(p, outbox)
            text = sender.calls[0]["text"]
            assert T.LEVERAGE_UNKNOWN_TEXT in text and "該幣上限 " not in text, text
            assert "以 %dx 槓桿計算" % config.A_CHANNEL_TARGET_LEVERAGE in text


def test_signal_close_comes_from_the_a3_specs_not_a_constant():
    """「發出時間」的主週期取自 A3 的規格：換一份主週期不同的規格，快照跟著變（沒有寫死 5 分 / 1 分）。"""
    with tempdir() as tmp:
        clock = Clock(CLOSE + 10 * SEC)
        fake_specs = {s: type("Spec", (), {"main_ms": 15 * MIN, "main_interval": "15M"})() for s in SPECS}
        with synchronous(tmp, ScriptedSender(), clock, specs=None) as (p, outbox):
            p.set_specs(fake_specs)
            e = make_entry()
            p.handle(e)
            assert outbox.get(e.signal_id, "entry")["snapshot"]["signal_close_ms"] == e.bar_open_ms + 15 * MIN
        # 沒有 set_specs() 時用 A3 預設的 build_specs()（與 A3 同一個來源）
        with synchronous(tmp, ScriptedSender(), clock, specs=None, name="o2.sqlite3") as (p, outbox):
            for spec in (S4, S5):
                e = make_entry(spec)
                p.handle(e)
                snap = outbox.get(e.signal_id, "entry")["snapshot"]
                assert snap["signal_close_ms"] == e.bar_open_ms + spec.main_ms == CLOSE
                assert snap["main_interval"] == spec.main_interval
                assert T.taipei_text(snap["signal_close_ms"]) == T.signal_id_time_text(e.signal_id)


# ============================== outbox ==============================
def test_outbox_schema_terminal_rows_and_infinity():
    with tempdir() as tmp:
        path = os.path.join(tmp, "o.sqlite3")
        with OB.open_outbox(path) as ob:
            e = make_entry()
            kw = dict(signal_id=e.signal_id, kind="entry", strategy="s4", symbol=e.symbol, event=e.to_dict(),
                      snapshot={"x": 1}, received_ms=CLOSE)
            assert ob.insert(**kw) is True and ob.insert(**kw) is False
            row = ob.get(e.signal_id, "entry")
            assert EntryEvent(**row["event"]) == e and row["event"]["features"]["volr"] == float("inf")
            ob.finalize(row["seq"], OB.STATUS_DELIVERED, now_ms=CLOSE + 1, message_id=3)
            for fn in (lambda: ob.finalize(row["seq"], OB.STATUS_FAILED, now_ms=CLOSE + 2),
                       lambda: ob.record_retry(row["seq"], next_attempt_ms=CLOSE, uncertain=True),
                       lambda: ob.mark_handoff(row["seq"], now_ms=CLOSE, text="x")):
                try:
                    fn()
                except OB.OutboxStateError:
                    pass
                else:
                    raise AssertionError("終態的列不可以再被改")
            assert ob.get(e.signal_id, "entry")["status"] == OB.STATUS_DELIVERED
            try:
                ob._c().execute("UPDATE outbox SET finalized_ms = NULL WHERE seq = ?", (row["seq"],))
            except sqlite3.IntegrityError:
                pass
            else:
                raise AssertionError("表層 CHECK 應該擋住「終態卻沒有 finalized_ms」")
            assert ob.counts()[OB.STATUS_DELIVERED] == 1
            assert ob._c().execute("PRAGMA user_version").fetchone()[0] == OB.SCHEMA_VERSION
            assert ob._c().execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        # 不是本模組建的檔：拒絕開啟、不在上面建表
        other = os.path.join(tmp, "other.sqlite3")
        c = sqlite3.connect(other)
        c.execute("CREATE TABLE foo (x)")
        c.commit()
        c.close()
        try:
            OB.open_outbox(other)
        except OB.OutboxSchemaError:
            pass
        else:
            raise AssertionError("別人的資料庫檔不可以被當成 outbox")
    # 保留：模組裡沒有任何刪除列的 SQL
    src = open(os.path.join(REPO_ROOT, "live", "a_channel_outbox.py"), encoding="utf-8").read().upper()
    assert "DELETE" not in src.replace("不自動刪除", "") and "DROP TABLE" not in src


def test_outbox_path_lives_under_runtime_db():
    path = os.path.abspath(config.A_CHANNEL_OUTBOX_DB_PATH)
    assert os.path.commonpath([path, paths.RUNTIME_DIR]) == os.path.abspath(paths.RUNTIME_DIR)
    assert os.path.dirname(path) == os.path.dirname(os.path.abspath(config.LIVE_DB_PATH))
    assert path != os.path.abspath(config.LIVE_DB_PATH)
    assert os.path.basename(path) == "a_channel_outbox.sqlite3"


# ============================== AC-7 接線（live.a_channel） ==============================
class FakeTracker:
    """A3 的替身：start() 時檢查 T2 已經就緒，並模擬重啟補發（發布 publish_on_start 裡的事件）。"""

    def __init__(self, bus, publish_on_start=(), record=None):
        self.bus = bus
        self.specs = SPECS
        self.stats = {"fake": True}
        self.publish_on_start = list(publish_on_start)
        self.record = record if record is not None else {}
        self.ready_error = None

    def start(self):
        self.record["subscribers_at_start"] = (self.bus.subscribers(EntryEvent), self.bus.subscribers(ExitEvent))
        self.record["t2_thread_alive_at_start"] = any(t.name == P.THREAD_NAME and t.is_alive()
                                                      for t in threading.enumerate())
        self.record["reports"] = [self.bus.publish(e) for e in self.publish_on_start]

    def wait_ready(self, timeout=None):
        return True

    def stop(self):
        pass

    def join(self, timeout=None):
        return True

    def bar_result_handler(self, strategy):
        return lambda res: None


class FakeFeed:
    def __init__(self, until=None):
        self.until = until

    def run(self, duration_s=None):
        if self.until is not None:
            wait_until(self.until, "FakeFeed 的等待條件", limit=10.0)

    def close(self):
        pass


class FakeS5:
    """A5（策略五資料層，TASK-116）的替身：這裡的測試只驗 T2 的接線，A5 的接線另見 tests/test_s5_feed.py。"""

    def start(self):
        pass

    def close(self, timeout=None):
        return True

    def stats(self):
        return {"fake": True}


def _run_a_channel(argv, **kw):
    from live import a_channel
    # FakeFeed 沒有 gate / buffer / universe，真的 A5（default_s5_feed）建不起來 → 一律注入替身
    kw.setdefault("s5_feed_factory", lambda feed, tracker: FakeS5())
    with capture("live.a_channel", "live.a_channel_push") as cap:
        rc = a_channel.main(argv, setup_logging=False, **kw)
    return rc, cap


@contextlib.contextmanager
def outbox_path_in(tmp):
    saved = config.A_CHANNEL_OUTBOX_DB_PATH
    config.A_CHANNEL_OUTBOX_DB_PATH = os.path.join(tmp, "db", "a_channel_outbox.sqlite3")
    try:
        yield config.A_CHANNEL_OUTBOX_DB_PATH
    finally:
        config.A_CHANNEL_OUTBOX_DB_PATH = saved


@contextlib.contextmanager
def secret_reads_forbidden():
    seen = []
    orig_require, orig_is_set = config.require_secret, config.secret_is_set

    def spy_require(name):
        seen.append(name)
        return orig_require(name)

    def spy_is_set(name):
        seen.append(name)
        return orig_is_set(name)
    config.require_secret, config.secret_is_set = spy_require, spy_is_set
    try:
        yield seen
    finally:
        config.require_secret, config.secret_is_set = orig_require, orig_is_set


def test_ac7_without_the_flag_nothing_changes():
    with tempdir() as tmp, outbox_path_in(tmp) as path, secret_reads_forbidden() as seen, OfflineCage() as cage:
        called = []
        rec = {}
        rc, cap = _run_a_channel(["--duration", "5"],
                                 tracker_factory=lambda bus: FakeTracker(bus, [make_entry()], rec),
                                 feed_factory=lambda on_result: FakeFeed(),
                                 sender_factory=lambda: called.append("sender"),
                                 pusher_factory=lambda s: called.append("pusher"))
        assert rc == 0 and called == [] and seen == [], (rc, called, seen)
        assert not os.path.exists(os.path.dirname(path)), "不帶 --push-tg 不可以建 outbox"
        wiring = [m for m in cap.messages() if "接線自檢" in m]
        assert wiring and "EntryEvent 1" in wiring[0] and "ExitEvent 1" in wiring[0], wiring
        assert rec["subscribers_at_start"] == (("event_log",), ("event_log",))
        assert all(r.ok for r in rec["reports"])
        assert not any(t.name in (P.THREAD_NAME, "tg-channel-sender") for t in threading.enumerate())
        assert not cage.attempts


def test_ac7_flag_without_secrets_exits_1_before_a3_and_a1():
    with tempdir() as tmp, outbox_path_in(tmp) as path, OfflineCage() as cage, \
            tgt.env_vars({config.TG_BOT_TOKEN_ENV: None, config.TG_CHANNEL_ID_ENV: None}):
        started = []
        rc, cap = _run_a_channel(["--duration", "5", "--push-tg"],
                                 tracker_factory=lambda bus: started.append("A3"),
                                 feed_factory=lambda on_result: started.append("A1"))
        assert rc == 1 and started == [], (rc, started)
        errors = cap.messages(logging.ERROR)
        assert any(config.TG_BOT_TOKEN_ENV in e and "不啟動" in e for e in errors), errors
        assert not os.path.exists(path), "缺密鑰時不可以建 outbox"
        assert not cage.attempts


def test_ac7_flag_wires_t2_before_a3_and_leaves_unsent_rows_in_the_outbox():
    with tempdir() as tmp, outbox_path_in(tmp) as path, OfflineCage():
        rec = {}
        sender = ScriptedSender()                  # 交出去之後永遠沒有結果（模擬卡住）
        e = make_entry(close_ms=CLOSE)
        clock = Clock(CLOSE + 10 * SEC)
        rc, cap = _run_a_channel(
            ["--duration", "5", "--push-tg"],
            tracker_factory=lambda bus: FakeTracker(bus, [e, make_exit(e)], rec),
            feed_factory=lambda on_result: FakeFeed(until=lambda: len(sender.calls) >= 1),
            sender_factory=lambda: type("Starter", (), {"start": lambda self: None, "send": sender.send,
                                                        "stop": sender.stop})(),
            pusher_factory=lambda s: P.ChannelPusher(s, market=FakeMarket(), now_ms=clock))
        assert rc == 0, rc
        subs = rec["subscribers_at_start"]
        assert subs == (("event_log", P.SUBSCRIBER_NAME), ("event_log", P.SUBSCRIBER_NAME)), subs
        assert rec["t2_thread_alive_at_start"], "T2 的工作執行緒必須在 A3 啟動之前就緒"
        assert all(r.ok for r in rec["reports"]), rec["reports"]
        msgs = cap.messages()
        wiring = [m for m in msgs if "接線自檢" in m]
        assert wiring and P.SUBSCRIBER_NAME in wiring[0] and "EntryEvent 2" in wiring[0], wiring
        assert any("outbox" in m and "現況" in m for m in msgs), msgs
        warns = cap.messages(logging.WARNING)
        assert any("還有 2 則待送" in w for w in warns), warns
        assert not any(t.name == P.THREAD_NAME and t.is_alive() for t in threading.enumerate())
        with OB.open_outbox(path) as ob:
            assert [r["status"] for r in ob.rows()] == [OB.STATUS_PENDING, OB.STATUS_PENDING]
            assert ob.get(e.signal_id, "entry")["handoff_open"] is True


def test_ac7_end_to_end_with_the_real_channel_sender():
    """真的 ChannelSender（假 HTTP、假時鐘）接在 python -m live.a_channel --push-tg 上：進場、出場都送達、
    outbox 記下 message_id；outbox 檔與日誌裡沒有密鑰片段（AC-10）。"""
    with tempdir() as tmp, outbox_path_in(tmp) as path, harness() as h:
        h.clock.t = CLOSE / 1000.0 - FakeClock.EPOCH + 20
        now_ms = lambda: int(round(h.clock.wall() * 1000))  # noqa: E731
        e = make_entry()
        x = make_exit(e, hold_ms=0)
        rec = {}

        def delivered_both():
            with OB.open_outbox(path) as ob:
                return ob.counts()[OB.STATUS_DELIVERED] == 2
        rc, cap = _run_a_channel(
            ["--duration", "5", "--push-tg"],
            tracker_factory=lambda bus: FakeTracker(bus, [e, x], rec),
            feed_factory=lambda on_result: FakeFeed(until=delivered_both),
            sender_factory=lambda: h.sender(),
            pusher_factory=lambda s: P.ChannelPusher(s, market=FakeMarket(), now_ms=now_ms))
        assert rc == 0
        assert [c.payload["text"].split("\n")[0] for c in h.tg.calls] == [
            T.ENTRY_TITLE + "  " + e.signal_id, T.TAKE_PROFIT_TITLE + "  " + x.signal_id]
        with OB.open_outbox(path) as ob:
            rows = ob.rows()
        assert [(r["status"], r["message_id"]) for r in rows] == [(OB.STATUS_DELIVERED, 1), (OB.STATUS_DELIVERED, 2)]
        assert [r["text"] for r in rows] == [c.payload["text"] for c in h.tg.calls]
        blobs = []
        for suffix in ("", "-wal"):
            if os.path.exists(path + suffix):
                with open(path + suffix, "rb") as f:
                    blobs.append(f.read().decode("latin-1"))
        assert blobs and not find_leaks(blobs + cap.messages() + h.logs.lines), "outbox 或日誌裡有密鑰片段"


# ============================== AC-3 持久化：真子行程、硬砍 ==============================
class FileChannelSender:
    """「頻道」= 一個檔：送達就追加一行 key（fsync）。mode：
         deliver  當場送達（期限已過就回報 expired，跟 ChannelSender 一樣）
         hold     收下之後不回報（模擬交給發送器、還沒回報）；deliver_first=True 時先寫進頻道再扣著
    """

    def __init__(self, channel, mode="deliver", now_ms=None, deliver_first=False):
        self.channel = channel
        self.mode = mode
        self.now_ms = now_ms
        self.deliver_first = deliver_first
        self.handed = threading.Event()
        self.calls = []

    def _append(self, key):
        with open(self.channel, "a", encoding="utf-8") as f:
            f.write(key + "\n")
            f.flush()
            os.fsync(f.fileno())

    def send(self, text, key=None, *, on_done=None, expires_at=None):
        self.calls.append(key)
        if self.mode == "hold":
            if self.deliver_first:
                self._append(key)
            self.handed.set()
            return True
        if expires_at is not None and self.now_ms() / 1000.0 > expires_at:
            if on_done:
                on_done(result(RESULT_EXPIRED, key))
            return True
        self._append(key)
        if on_done:
            on_done(result(RESULT_DELIVERED, key, message_id=len(self.calls)))
        return True

    def stop(self, timeout=None):
        return 0


class MemoryOnlyPusher:
    """對照組：只有記憶體佇列（等同 T2 之前「handler 直接排進 ChannelSender」的做法），沒有任何落地。"""

    def __init__(self, sender):
        self.sender = sender
        self.q = queue.Queue()
        self.thread = None

    def handle(self, event):
        kind = "entry" if type(event) is EntryEvent else "exit"
        self.q.put("%s/%s" % (event.signal_id, kind))

    def subscribe(self, bus):
        bus.subscribe(EntryEvent, self.handle, P.SUBSCRIBER_NAME)
        bus.subscribe(ExitEvent, self.handle, P.SUBSCRIBER_NAME)

    def start(self):
        def run():
            while True:
                key = self.q.get()
                if key is None:
                    return
                self.sender.send("text of " + key, key=key)
        self.thread = threading.Thread(target=run, name="memory-only-push", daemon=True)
        self.thread.start()

    def shutdown(self):
        self.q.put(None)
        if self.thread:
            self.thread.join(5)


def _event_from(d):
    d = dict(d)
    cls = EntryEvent if d.pop("_kind") == "entry" else ExitEvent
    return cls(**d)


def _event_dict(ev):
    d = ev.to_dict()
    d["_kind"] = "entry" if type(ev) is EntryEvent else "exit"
    return d


def _ac3_child(argv):
    """子行程：照 spec 發布事件，停在指定的時間點印 READY_TO_DIE，然後卡住等著被砍。"""
    spec = json.loads(argv[0])
    cage = OfflineCage()
    cage.__enter__()                                  # 子行程全程離線（被砍之前不會離開）
    now = Clock(spec["now_ms"])
    sender = FileChannelSender(spec["channel"], mode="hold", deliver_first=spec.get("deliver_first", False))
    if spec["impl"] == "outbox":
        pusher = P.ChannelPusher(sender, path=spec["outbox"], specs=SPECS, market=FakeMarket(), now_ms=now)
        if spec["point"] == "after_write":
            pusher.prepare()                          # 建 outbox，但不起工作執行緒：只有 handler 寫入
        else:
            pusher.start()
    else:
        pusher = MemoryOnlyPusher(sender)
        if spec["point"] == "after_handoff":
            pusher.start()
    bus = SignalBus()
    pusher.subscribe(bus)
    for d in spec["events"]:
        ev = _event_from(d)
        r = bus.publish(ev)
        print("PUBLISHED %s %s %s" % (d["_kind"], ev.signal_id, r.ok), flush=True)
    if spec["point"] == "after_handoff" and not sender.handed.wait(20):
        print("NO_HANDOFF", flush=True)
        return 2
    print("READY_TO_DIE", flush=True)
    threading.Event().wait()                          # 等著被硬砍
    return 0


def _child_env():
    env = dict(os.environ)
    for name in (config.TG_BOT_TOKEN_ENV, config.TG_CHANNEL_ID_ENV):
        env.pop(name, None)                           # 子行程絕不帶真的密鑰
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def run_ac3_kill(tmp, *, impl, point, events, now_ms, deliver_first=False):
    """開子行程跑到指定時間點、硬砍（Popen.kill = Windows TerminateProcess）。回傳 (outbox 路徑, 頻道檔, 已被確認的事件)。"""
    outbox, channel = os.path.join(tmp, "outbox.sqlite3"), os.path.join(tmp, "channel.txt")
    spec = {"impl": impl, "point": point, "outbox": outbox, "channel": channel, "now_ms": now_ms,
            "deliver_first": deliver_first, "events": [_event_dict(e) for e in events]}
    proc = subprocess.Popen([sys.executable, os.path.abspath(__file__), "--ac3-child", json.dumps(spec)],
                            cwd=REPO_ROOT, env=_child_env(), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    lines = queue.Queue()

    def reader():
        for raw in proc.stdout:
            lines.put(raw.decode("utf-8", "replace").strip())
        lines.put(None)
    threading.Thread(target=reader, daemon=True).start()
    acked, got = [], []
    try:
        while True:
            try:
                line = lines.get(timeout=60)
            except queue.Empty:
                raise AssertionError("子行程 60 秒內沒有走到當機點：%r" % got)
            if line is None:
                raise AssertionError("子行程提早結束（exit %s）：%r\n%s" % (proc.wait(), got,
                                                                       proc.stderr.read().decode("utf-8", "replace")[-2000:]))
            got.append(line)
            if line.startswith("PUBLISHED"):
                _, kind, sid, okay = line.split(" ")
                assert okay == "True", line
                acked.append((sid, kind))
            if line == "READY_TO_DIE":
                break
    finally:
        proc.kill()                                   # 硬砍：不跑 finally、不 flush、不關連線
        proc.wait(30)
        proc.stdout.close()
        proc.stderr.close()
    return outbox, channel, acked


def _restart_and_finish(outbox_path, channel, now_ms, impl="outbox"):
    """在本行程「重啟」：新的 pusher（工作執行緒）+ 會送達的發送器，跑到沒有待送為止。"""
    clock = Clock(now_ms)
    sender = FileChannelSender(channel, mode="deliver", now_ms=clock)
    if impl == "memory":
        p = MemoryOnlyPusher(sender)
        p.start()
        p.shutdown()
        return
    p = P.ChannelPusher(sender, path=outbox_path, specs=SPECS, market=FakeMarket(), now_ms=clock)
    p.start()
    try:
        def done():
            with OB.open_outbox(outbox_path) as ob:
                return ob.counts()[OB.STATUS_PENDING] == 0
        wait_until(done, "重啟後把 outbox 送完", limit=20.0)
    finally:
        p.shutdown()


def _channel_keys(channel):
    if not os.path.exists(channel):
        return collections.Counter()
    with open(channel, encoding="utf-8") as f:
        return collections.Counter(line.strip() for line in f if line.strip())


def _check_no_loss(outbox_path, channel, acked):
    """每一則被 A3 確認過的事件：恰好一列、恰好一個終態；頻道與紀錄一致（紀錄說沒有的，頻道上一定沒有）。"""
    with OB.open_outbox(outbox_path) as ob:
        rows = ob.rows()
    by_key = collections.Counter((r["signal_id"], r["kind"]) for r in rows)
    assert all(n == 1 for n in by_key.values()), by_key
    missing = [k for k in acked if k not in by_key]
    assert not missing, "A3 確認過的事件在 outbox 裡消失了：%r" % missing
    assert all(r["status"] in OB.TERMINAL_STATUSES for r in rows), [(r["signal_id"], r["status"]) for r in rows]
    chan = _channel_keys(channel)
    for r in rows:
        key = "%s/%s" % (r["signal_id"], r["kind"])
        if r["status"] == OB.STATUS_DELIVERED:
            assert chan[key] >= 1, "紀錄說已送達、頻道上沒有：%s" % key
        else:
            assert chan[key] == 0, "紀錄說 %s、頻道上卻有：%s" % (r["status"], key)
    return {(r["signal_id"], r["kind"]): r["status"] for r in rows}, chan


AC3_ROUNDS = 10


def _ac3_events(i):
    """第 i 輪的事件：進場 A（偶數輪另帶它的出場），每 3 輪再多一個 s5 的進場。"""
    e = make_entry(S4, symbol="ACE_USDT_PERP", close_ms=CLOSE + i * S4.main_ms)
    evs = [e]
    if i % 2 == 0:
        evs.append(make_exit(e, hold_ms=MIN))
    if i % 3 == 0:
        evs.append(make_entry(S5, symbol="BTC_USDT_PERP", close_ms=CLOSE + i * S4.main_ms, price=64069.4))
    return evs


def test_ac3_hard_kill_after_outbox_write_then_restart():
    """「已寫入 outbox、還沒送出」時硬砍 × 10：沒有一則消失；前 5 輪在門檻內重啟 → 照送；後 5 輪過了門檻 →
    進場延遲不發、出場因進場未出現而不發（頻道上都沒有）。"""
    for i in range(AC3_ROUNDS):
        evs = _ac3_events(i)
        close = evs[0].bar_open_ms + S4.main_ms
        late = i >= AC3_ROUNDS // 2
        with tempdir() as tmp:
            outbox, channel, acked = run_ac3_kill(tmp, impl="outbox", point="after_write", events=evs,
                                                  now_ms=close + 10 * SEC)
            assert len(acked) == len(evs)
            assert not os.path.exists(channel), "前提：當機前什麼都還沒送出"
            _restart_and_finish(outbox, channel, close + (DELAY_MS + (i + 1) * SEC if late else 60 * SEC))
            st, chan = _check_no_loss(outbox, channel, acked)
            for sid, kind in acked:
                want = ((OB.STATUS_EXPIRED if kind == "entry" else OB.STATUS_NO_ENTRY) if late
                        else OB.STATUS_DELIVERED)
                assert st[(sid, kind)] == want, (i, sid, kind, st)
                assert chan["%s/%s" % (sid, kind)] == (0 if late else 1), (i, chan)


def test_ac3_hard_kill_after_handoff_then_restart():
    """「已交給發送器、還沒回報」時硬砍 × 10：沒有一則消失；交出去的那一則結果不確定（可能已到 Telegram），
    重啟後**不論有沒有過門檻都照送**（FR-3 第 3 點），紀錄為已送達；單數輪模擬「砍之前其實已經送到頻道」。"""
    for i in range(AC3_ROUNDS):
        evs = _ac3_events(i)
        close = evs[0].bar_open_ms + S4.main_ms
        late = i >= AC3_ROUNDS // 2
        with tempdir() as tmp:
            outbox, channel, acked = run_ac3_kill(tmp, impl="outbox", point="after_handoff", events=evs,
                                                  now_ms=close + 10 * SEC, deliver_first=bool(i % 2))
            with OB.open_outbox(outbox) as ob:
                handed = [(r["signal_id"], r["kind"]) for r in ob.rows() if r["handoff_open"]]
            assert handed == [(evs[0].signal_id, "entry")], handed
            restart = close + (DELAY_MS + (i + 1) * SEC if late else 60 * SEC)
            _restart_and_finish(outbox, channel, restart)
            st, chan = _check_no_loss(outbox, channel, acked)
            assert st[(evs[0].signal_id, "entry")] == OB.STATUS_DELIVERED, (i, st)
            with OB.open_outbox(outbox) as ob:
                row = ob.get(evs[0].signal_id, "entry")
            assert row["uncertain"] is True and row["attempts"] == 2, row
            assert chan[evs[0].signal_id + "/entry"] == (2 if i % 2 else 1), (i, chan)
            for sid, kind in acked[1:]:
                if kind == "exit":
                    assert st[(sid, kind)] == OB.STATUS_DELIVERED      # 進場已送達 → 出場照送
                else:
                    assert st[(sid, kind)] == (OB.STATUS_EXPIRED if late else OB.STATUS_DELIVERED), (i, sid, st)


def test_ac3_memory_only_queue_loses_messages_in_the_same_scenario():
    """鑑別力：同一個情境換成「只有記憶體佇列」的實作，A3 已確認（ok=True、不會再重送）的訊息在重啟後消失。"""
    for point in ("after_write", "after_handoff"):
        evs = _ac3_events(0)
        close = evs[0].bar_open_ms + S4.main_ms
        with tempdir() as tmp:
            outbox, channel, acked = run_ac3_kill(tmp, impl="memory", point=point, events=evs,
                                                  now_ms=close + 10 * SEC)
            assert len(acked) == len(evs)
            _restart_and_finish(outbox, channel, close + 60 * SEC, impl="memory")
            chan = _channel_keys(channel)
            lost = [k for k in acked if chan["%s/%s" % k] == 0]
            assert lost, "只有記憶體佇列的實作居然沒有漏訊息：這個情境沒有鑑別力"
            assert set(lost) == set(acked), (point, lost, acked)


# ============================== --sample、設定、模組 ==============================
def test_sample_messages_cover_every_case_without_sending():
    msgs = P.sample_messages()
    assert len(msgs) == 9 and len({k for k, _ in msgs}) == len(msgs)
    for key, text in msgs:
        assert text.startswith("[測試] ")
        assert text.count("\n") > 10 and len(text) < 1024
    bodies = [t for _, t in msgs]
    for word in ("該幣上限 75x", "該幣上限 20x", T.LEVERAGE_UNKNOWN_TEXT, "策略5", T.REASON_TAKE_PROFIT,
                 T.REASON_STOP_LOSS, T.REASON_GAP, T.DELAY_NOTE, "策略：策略4", "出場時間：", "發出時間："):
        assert any(word in b for b in bodies), word
    assert not any("1分K" in b or "判定先" in b for b in bodies)


def test_sample_exit_codes_with_a_fake_sender():
    from live import logsetup
    saved_setup, saved_sender = logsetup.setup, P.ChannelSender
    try:
        logsetup.setup = lambda *a, **k: None          # 不在真的 runtime/ 建日誌檔
        for plan, want in (((), P.EXIT_SENT), ((tgt.http_error(400, "bad"),), P.EXIT_FAILED)):
            with harness() as h:
                h.tg.plan(*plan)
                P.ChannelSender = lambda: h.sender()
                import io
                with contextlib.redirect_stdout(io.StringIO()) as out:
                    rc = P.sample()
                assert rc == want, (rc, out.getvalue())
                assert "已送達" in out.getvalue() and not find_leaks([out.getvalue()])
                assert all(c.payload["text"].startswith("[測試]") for c in h.tg.calls)
    finally:
        logsetup.setup, P.ChannelSender = saved_setup, saved_sender


def test_sample_without_secrets_reports_not_tested_and_stays_offline():
    log_dir = os.path.dirname(config.LOG_FILE)
    before = os.path.exists(log_dir)
    code = textwrap.dedent("""
        import socket, sys
        sys.path.insert(0, %r)
        def _blocked(*a, **k):
            raise OSError("offline test: network blocked")
        socket.socket.connect = _blocked
        socket.create_connection = _blocked
        socket.getaddrinfo = _blocked
        from live import a_channel_push
        sys.exit(a_channel_push.main(sys.argv[1:]))
    """ % REPO_ROOT)
    r = subprocess.run([sys.executable, "-c", code, "--sample"], cwd=REPO_ROOT, env=_child_env(),
                       stdin=subprocess.DEVNULL, capture_output=True, timeout=120)
    out = r.stdout.decode("utf-8", "replace")
    assert r.returncode == P.EXIT_NOT_TESTED, (r.returncode, out, r.stderr[-1500:])
    assert "未實測" in out and config.TG_BOT_TOKEN_ENV in out, out
    assert os.path.exists(log_dir) == before, "缺密鑰的 --sample 不該建日誌目錄"
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, env=_child_env(), stdin=subprocess.DEVNULL,
                       capture_output=True, timeout=120)
    assert r.returncode == 2, "沒帶 --sample 應該是用法錯誤（argparse exit 2）"


def test_new_params_are_in_execution_params_and_python_m_live():
    names = ["STRATEGY_LABELS", "A_CHANNEL_ORDER_PCT", "A_CHANNEL_TARGET_LEVERAGE", "A_CHANNEL_DEVIATION_WARN_PCT",
             "A_CHANNEL_ENTRY_MAX_DELAY_SECONDS", "A_CHANNEL_OUTBOX_DB_PATH", "A_CHANNEL_OUTBOX_RETRY_SECONDS",
             "A_CHANNEL_OUTBOX_BUSY_TIMEOUT_SECONDS", "A_CHANNEL_PRICE_FALLBACK_SIGNIFICANT_DIGITS",
             "TG_MAX_RETRY_AFTER_SECONDS"]
    params = config.execution_params()
    for n in names + [n for n in dir(config) if n.startswith("A_CHANNEL_")]:
        assert params[n] == getattr(config, n), n
    assert (config.A_CHANNEL_ORDER_PCT, config.A_CHANNEL_TARGET_LEVERAGE, config.A_CHANNEL_DEVIATION_WARN_PCT,
            config.A_CHANNEL_ENTRY_MAX_DELAY_SECONDS, config.A_CHANNEL_OUTBOX_RETRY_SECONDS) == (0.02, 50, 0.01, 300, 60)
    r = subprocess.run([sys.executable, "-m", "live"], cwd=REPO_ROOT, env=_child_env(), stdin=subprocess.DEVNULL,
                       capture_output=True, timeout=120)
    out = r.stdout.decode("utf-8", "replace")
    assert r.returncode == 0, (r.returncode, out[-1500:])
    for n in names:
        assert n in out, n


def test_module_names_and_dependencies():
    import ast
    import importlib.util
    for name in ("a_channel_text", "a_channel_outbox", "a_channel_push"):
        assert name not in sys.stdlib_module_names and importlib.util.find_spec(name) is None, name
        src = open(os.path.join(REPO_ROOT, "live", name + ".py"), encoding="utf-8").read()
        tops = set()
        for node in ast.walk(ast.parse(src)):
            if isinstance(node, ast.Import):
                tops |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom):
                tops.add((node.module or "").split(".")[0])
        assert tops <= set(sys.stdlib_module_names) | {"live"}, (name, tops)
        assert not any(t.startswith("pionex_") or t == "research" for t in tops)
    # 沒有新套件：三個模組只用標準庫與 live（requirements*.txt 不動，由 DQA 以 git diff 確認範圍）


# ============================== runner ==============================
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--ac3-child":
        sys.exit(_ac3_child(sys.argv[2:]))
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    # 測試輸出只看結果：根 logger 掛一個 NullHandler，WARNING 以上不會被 logging 的 lastResort 印到終端機；
    # 不把 live.* 的等級調高，harness() 的 LogCapture 才收得到發送器的日誌（AC-10 的密鑰掃描要掃它）
    logging.getLogger().addHandler(logging.NullHandler())
    runtime_existed = os.path.exists(paths.RUNTIME_DIR)
    outbox_existed = os.path.exists(config.A_CHANNEL_OUTBOX_DB_PATH)
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
            os.path.exists(config.A_CHANNEL_OUTBOX_DB_PATH) != outbox_existed:
        runner_failed += 1
        print(f"FAIL  <runner>: 測試在真正的 runtime/ 留下了東西（{paths.RUNTIME_DIR}）")
    leftover = [t.name for t in threading.enumerate() if t.name in (P.THREAD_NAME, "tg-channel-sender") and t.is_alive()]
    if leftover:
        runner_failed += 1
        print(f"FAIL  <runner>: 還有執行緒活著 {leftover}")
    print(f"\n{len(tests) - failed} passed, {failed} failed"
          + (f", {runner_failed} runner check(s) failed" if runner_failed else ""))
    sys.exit(1 if failed or runner_failed else 0)
