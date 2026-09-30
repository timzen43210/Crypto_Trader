# -*- coding: utf-8 -*-
"""
R-A（live.a_channel_report_push：排程、發送紀錄、報表執行緒）與接線（live.a_channel --push-tg）的單元測試。

  AC-7  排程（假時鐘 + 假發送器，同步模式：ChannelReporter.prepare() + pump()，比照執行緒的等待規則推進時鐘）
        連續模擬 2027-01-25 → 2027-03-05：每一期恰好發一次、在排定時刻之後一個輪詢週期內、順序正確；
        重啟情境 1～7；組報表失敗（檔案不存在、版本不符、被鎖住）→ ERROR（不洗版）、不寫列、下一輪重試，
        同時間 A3 對 live.sqlite3 的寫入照常成功
  FR-8  發送紀錄：schema、UNIQUE、CHECK、user_version、建立時刻、synchronous = FULL、不刪列
  AC-8  接線：不帶 --push-tg 不建發送紀錄、沒有報表執行緒；帶旗標時 A3 ready 之後啟動、push.shutdown() 之前停止；
        啟動失敗 exit 1 且 A1 / A5 沒有啟動；執行中意外結束 → ERROR、A1 / A3 照跑、exit 1、worker_alive False
  AC-10 以假密鑰跑 --push-tg（真的 ChannelSender + 假 Telegram）：日誌、發送紀錄、stats 不含密鑰片段
  另有真執行緒的 start / stop 冒煙。

全程離線：每個測試都在 socket 籠子裡跑（tests/test_tg_channel.py 的 OfflineCage，禁止真的 sleep）。
資料庫一律開在暫存目錄，不碰 repo 的 runtime/。不依賴 pytest：直接 `python tests/test_a_channel_report_push.py`。
"""
import contextlib
import json
import logging
import os
import socket
import sqlite3
import sys
import threading
import time

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, TESTS_DIR)

import test_tg_channel as tgt  # noqa: E402
from test_a_channel_push import (Clock, FakeFeed, FakeMarket, FakeS5, FakeTracker, ScriptedSender, capture,  # noqa: E402
                                 deliver_all, outbox_path_in, result, secret_reads_forbidden, tempdir)
from test_a_channel_report import LATE_LINE, Books, day, tpe  # noqa: E402
from test_tg_channel import OfflineCage, find_leaks, wait_until  # noqa: E402

from live import a_channel_outbox as OB  # noqa: E402
from live import a_channel_push as P  # noqa: E402
from live import a_channel_report as R  # noqa: E402
from live import a_channel_report_push as RP  # noqa: E402
from live import config, paths, store  # noqa: E402
from live.tg_channel import (RESULT_ABANDONED, RESULT_DELIVERED, RESULT_FAILED, RESULT_GAVE_UP,  # noqa: E402
                             ChannelSender)

SEC = 1000
MIN = 60 * SEC
HOUR = 60 * MIN
DAY_MS = 24 * HOUR
POLL_MS = int(config.A_CHANNEL_REPORT_POLL_SECONDS * SEC)
RETRY_MS = int(config.A_CHANNEL_REPORT_RETRY_SECONDS * SEC)
DELAY_MS = int(config.A_CHANNEL_REPORT_SEND_DELAY_SECONDS * SEC)
CATCHUP_MS = int(config.A_CHANNEL_REPORT_CATCHUP_MAX_SECONDS * SEC)
LATE_MS = int(config.A_CHANNEL_REPORT_LATE_NOTE_SECONDS * SEC)
TP, SL = config.EXIT_TAKE_PROFIT, config.EXIT_STOP_LOSS
LOGGERS = ("live.a_channel", "live.a_channel_push", "live.a_channel_report_push")


# ============================== 測試工具 ==============================
def daily(iso):
    return "report:daily:" + iso


def stamped(clock, fn=deliver_all):
    """ScriptedSender 的 auto：記下交出時的假時鐘，再照 fn 回報（fn=None → 先扣著）。"""
    def auto(call):
        call["at"] = clock.ms
        return None if fn is None else fn(call)
    return auto


class Env:
    """一組暫存目錄：live.sqlite3 + outbox（Books，真實 API 寫入）與發送紀錄。"""

    def __init__(self, tmp, created_iso):
        self.tmp = tmp
        self.books_dir = os.path.join(tmp, "books")
        Books(self.books_dir).close()
        self.live = os.path.join(self.books_dir, "db", "live.sqlite3")
        self.outbox = os.path.join(self.books_dir, "db", "a_channel_outbox.sqlite3")
        self.path = os.path.join(tmp, "rec", "a_channel_reports.sqlite3")
        self.clock = Clock(tpe(created_iso))

    @contextlib.contextmanager
    def books(self):
        b = Books(self.books_dir)
        try:
            yield b
        finally:
            b.close()

    def session(self, sender, **kw):
        return Session(self, sender, **kw)

    def rows(self):
        with RP.open_record(self.path, now_ms=self.clock()) as rec:
            return rec.rows()


class Session:
    """一次「啟動」：ChannelReporter.prepare() + 測試執行緒自己開的發送紀錄連線，用 pump() 同步推進。"""

    def __init__(self, env, sender, **kw):
        self.env, self.sender = env, sender
        self.rep = RP.ChannelReporter(sender, path=env.path, live_db=env.live, outbox_db=env.outbox,
                                      now_ms=env.clock, **kw)
        self.prepared = self.rep.prepare()
        self.record = RP.open_record(env.path, now_ms=env.clock())

    def pump(self):
        """跑到沒有進展（比照執行緒：有結果就立刻再跑一輪）。回傳最後一輪的 wake_at。"""
        for _ in range(1000):
            n = len(self.sender.calls)
            wake = self.rep.pump(self.record)
            if not self.rep.has_results() and len(self.sender.calls) == n:
                return wake
        raise AssertionError("pump 停不下來")

    def run_until(self, end_ms):
        """比照執行緒的等待：下一輪在 min(輪詢間隔, wake_at − 現在) 之後。"""
        clock = self.env.clock
        while clock.ms <= end_ms:
            wake = self.pump()
            step = POLL_MS if wake is None else min(POLL_MS, wake - clock.ms)
            assert step > 0, (clock.ms, wake)
            clock.ms += step

    def close(self):
        self.rep.stop()
        self.record.close()


# ============================== AC-7 連續模擬 ==============================
# 月底 / 旬末多出來的期別（人工推算；接在同一個 end 的日報後面）
CONTINUOUS_EXTRA = {
    "2027-01-31": ["report:tenday:2027-01-21", "report:monthly:2027-01-01"],
    "2027-02-10": ["report:tenday:2027-02-01"],
    "2027-02-20": ["report:tenday:2027-02-11"],
    "2027-02-28": ["report:tenday:2027-02-21", "report:monthly:2027-02-01"],
}


def _continuous_expected():
    out = []
    d, last = day("2027-01-25"), day("2027-03-04")
    while d <= last:
        end = (d + (day("2027-01-02") - day("2027-01-01"))).isoformat()
        sched = tpe(end + "T00:05")
        out.append((daily(d.isoformat()), sched))
        out += [(k, sched) for k in CONTINUOUS_EXTRA.get(d.isoformat(), [])]
        d = day(end)
    return out


def test_ac7_continuous_run_sends_every_period_once_in_order_within_one_poll():
    with tempdir() as tmp:
        env = Env(tmp, "2027-01-25T00:00")
        with env.books() as b:
            x = b.entry("s4", "XXX_USDT_PERP", tpe("2027-02-28T10:00"))
            b.close_pos(x, tpe("2027-03-01T00:00") - 1)
            y = b.entry("s4", "YYY_USDT_PERP", tpe("2027-02-28T20:00"))
            b.close_pos(y, tpe("2027-03-01T00:00"), reason=SL, exit_price=105.0)
        sender = ScriptedSender()
        sender.auto = stamped(env.clock)
        s = env.session(sender)
        try:
            s.run_until(tpe("2027-03-05T00:10"))
        finally:
            s.close()
        expected = _continuous_expected()
        assert sender.keys() == [k for k, _ in expected], sender.keys()
        assert len(expected) == 45 and len(set(sender.keys())) == len(expected)
        for call, (key, sched) in zip(sender.calls, expected):
            assert sched <= call["at"] < sched + POLL_MS, (key, call["at"] - sched)
            assert call["expires_at"] is None
            assert LATE_LINE not in call["text"]
        # 月底的順序：日報 → 下旬 → 月報
        keys = sender.keys()
        i = keys.index(daily("2027-02-28"))
        assert keys[i:i + 4] == [daily("2027-02-28"), "report:tenday:2027-02-21", "report:monthly:2027-02-01",
                                 daily("2027-03-01")], keys[i:i + 4]
        # 建立時刻（01-25 00:00）= daily 01-24 的 end：不發；tenday 01-11（end 01-21）也不發
        assert daily("2027-01-24") not in keys and "report:tenday:2027-01-11" not in keys
        rows = env.rows()
        assert [r["report_key"] for r in rows] == keys
        assert all(r["status"] == RP.STATUS_DELIVERED and r["attempts"] == 1 and r["message_id"] and not r["late_note"]
                   and r["handoff_open"] == 0 for r in rows)
        by_key = {c["key"]: c["text"] for c in sender.calls}
        assert "平倉：1 筆（止盈 1、止損 0）" in by_key[daily("2027-02-28")].split("\n")
        assert "持倉中：1 筆（不計入）" in by_key[daily("2027-02-28")].split("\n")
        assert "平倉：1 筆（止盈 0、止損 1）" in by_key[daily("2027-03-01")].split("\n")
        assert "平倉：1 筆（止盈 1、止損 0）" in by_key["report:monthly:2027-02-01"].split("\n")
        assert "本期無平倉" in by_key["report:monthly:2027-01-01"].split("\n")
        summary = json.loads(rows[keys.index("report:tenday:2027-02-21")]["summary_json"])
        assert summary["s4"]["n"] == 1 and summary["s4"]["holding"] == 1, summary


def test_ac7_wake_up_never_passes_the_next_scheduled_time():
    """等待規則：沒有待送時，pump 回傳的 wake_at 就是下一個排定發送時刻（不會晚於它）。"""
    with tempdir() as tmp:
        env = Env(tmp, "2027-02-10T13:17")
        sender = ScriptedSender(deliver_all)
        s = env.session(sender)
        try:
            assert s.pump() == tpe("2027-02-11T00:05")
            env.clock.ms = tpe("2027-02-11T00:05") - 1
            assert s.pump() == tpe("2027-02-11T00:05") and sender.calls == []
            env.clock.ms += 1
            s.pump()
            assert sender.keys() == [daily("2027-02-10"), "report:tenday:2027-02-01"]
            assert s.pump() == tpe("2027-02-12T00:05")
        finally:
            s.close()


# ============================== AC-7 重啟情境 ==============================
def test_ac7_restart_1_same_day_does_not_resend():
    with tempdir() as tmp:
        env = Env(tmp, "2027-02-10T00:00")
        sender = ScriptedSender(deliver_all)
        s = env.session(sender)
        env.clock.ms = tpe("2027-02-11T00:06")
        s.pump()
        s.close()
        sent = sender.keys()
        assert sent == [daily("2027-02-10"), "report:tenday:2027-02-01"], sent
        env.clock.ms = tpe("2027-02-11T08:00")
        s = env.session(sender)
        try:
            s.pump()
            env.clock.ms = tpe("2027-02-11T23:59")
            s.pump()
        finally:
            s.close()
        assert sender.keys() == sent
        assert s.prepared[RP.STATUS_DELIVERED] == len(sent) and s.prepared[RP.STATUS_PENDING] == 0


def test_ac7_restart_2_downtime_across_the_send_time_catches_up_with_the_late_note():
    for restart_iso, late in (("2027-02-11T03:00", True), ("2027-02-11T01:05", False), ("2027-02-11T01:06", True)):
        with tempdir() as tmp:
            env = Env(tmp, "2027-02-10T00:00")
            s = env.session(ScriptedSender(deliver_all))
            env.clock.ms = tpe("2027-02-10T12:00")
            s.pump()
            s.close()
            env.clock.ms = tpe(restart_iso)
            sender = ScriptedSender(deliver_all)
            with capture(*LOGGERS) as cap:
                s = env.session(sender)
                try:
                    s.pump()
                finally:
                    s.close()
            assert sender.keys() == [daily("2027-02-10"), "report:tenday:2027-02-01"], sender.keys()
            for c in sender.calls:
                assert (LATE_LINE in c["text"]) == late, (restart_iso, c["key"])
            assert [r["late_note"] for r in env.rows()] == [int(late)] * len(sender.calls)
            warns = [m for m in cap.messages(logging.WARNING) if "延遲註記" in m]
            assert len(warns) == (len(sender.calls) if late else 0), (restart_iso, warns)
    # 門檻：晚 3600 秒整不加（01:05 = 排定時刻 00:05 + 3600 秒）、多 1 分鐘就加
    assert LATE_MS == HOUR


def test_ac7_restart_3_downtime_beyond_the_catch_up_limit_is_skipped():
    with tempdir() as tmp:
        env = Env(tmp, "2027-02-01T00:00")
        s = env.session(ScriptedSender(deliver_all))
        env.clock.ms = tpe("2027-02-01T01:00")
        s.pump()
        s.close()
        # 2027-02-12 00:05 重啟：排定時刻早於 02-05 00:05（晚超過 7 天）的日報跳過；剛好 7 天的 02-04 照發
        env.clock.ms = tpe("2027-02-12T00:05")
        sender = ScriptedSender(deliver_all)
        with capture(*LOGGERS) as cap:
            s = env.session(sender)
            try:
                s.pump()
            finally:
                s.close()
        assert CATCHUP_MS == 7 * DAY_MS
        want_sent = [daily(d) for d in ("2027-02-04", "2027-02-05", "2027-02-06", "2027-02-07", "2027-02-08",
                                        "2027-02-09", "2027-02-10")] + ["report:tenday:2027-02-01",
                                                                        daily("2027-02-11")]
        assert sender.keys() == want_sent, sender.keys()
        rows = {r["report_key"]: r for r in env.rows()}
        skipped = [k for k, r in rows.items() if r["status"] == RP.STATUS_SKIPPED]
        assert skipped == [daily("2027-02-01"), daily("2027-02-02"), daily("2027-02-03")], skipped
        for k in skipped:
            r = rows[k]
            assert r["text"] is None and r["attempts"] == 0 and r["finalized_ms"] == env.clock.ms, r
            assert "超過補發上限" in r["last_error"]
        warns = [m for m in cap.messages(logging.WARNING) if "超過補發上限" in m]
        assert len(warns) == len(skipped) and all(k in " ".join(warns) for k in skipped), warns
        texts = {c["key"]: c["text"] for c in sender.calls}
        assert LATE_LINE in texts[daily("2027-02-04")] and LATE_LINE not in texts[daily("2027-02-11")]
        assert daily("2027-01-31") not in rows, "end = 建立時刻的期別不發也不記"
        assert s.rep.stats()["skipped"] == len(skipped)


def test_ac7_restart_4_first_creation_never_sends_older_periods():
    with tempdir() as tmp:
        env = Env(tmp, "2027-02-15T13:00")
        sender = ScriptedSender(deliver_all)
        s = env.session(sender)
        try:
            s.run_until(tpe("2027-02-16T00:10"))
        finally:
            s.close()
        assert sender.keys() == [daily("2027-02-15")], sender.keys()
        assert [r["report_key"] for r in env.rows()] == [daily("2027-02-15")], "舊期別不可以寫成 skipped 或別的列"
        with RP.open_record(env.path, now_ms=tpe("2027-03-01T00:00")) as rec:
            assert rec.created_ms == tpe("2027-02-15T13:00"), "重開不可以改建立時刻"


def test_ac7_restart_5_in_flight_at_shutdown_is_resent_with_the_same_text():
    with tempdir() as tmp:
        env = Env(tmp, "2027-02-10T00:00")
        held = ScriptedSender()                      # 交出去之後沒有結果
        s = env.session(held)
        env.clock.ms = tpe("2027-02-11T00:05")
        s.pump()
        assert held.keys() == [daily("2027-02-10")], "同一時間最多交出一則"
        assert s.rep.in_flight() is not None
        s.close()
        (row,) = env.rows()
        assert row["handoff_open"] == 1 and row["attempts"] == 1 and row["status"] == RP.STATUS_PENDING, row
        first_text = held.calls[0]["text"]
        # 重啟前資料變了：重送仍用凍結的那段文字
        with env.books() as b:
            sid = b.entry("s4", "NEW_USDT_PERP", tpe("2027-02-10T10:00"))
            b.close_pos(sid, tpe("2027-02-10T11:00"))
        env.clock.ms = tpe("2027-02-11T03:00")
        sender = ScriptedSender(deliver_all)
        with capture(*LOGGERS) as cap:
            s = env.session(sender)
            try:
                s.pump()
            finally:
                s.close()
        assert sender.keys() == [daily("2027-02-10"), "report:tenday:2027-02-01"], sender.keys()
        assert sender.calls[0]["text"] == first_text and LATE_LINE not in first_text
        assert "平倉：1 筆" in sender.calls[1]["text"] and LATE_LINE in sender.calls[1]["text"]
        released = [m for m in cap.messages(logging.WARNING) if "結果沒有記回來" in m]
        assert len(released) == 1 and "1 則" in released[0], cap.messages()
        rows = env.rows()
        assert rows[0]["attempts"] == 2 and rows[0]["status"] == RP.STATUS_DELIVERED


def _retry_case(make_first):
    """第一次交出的結果由 make_first(sender) 決定；隔 RETRY 秒用同一段文字重送，重送前不越過它。"""
    with tempdir() as tmp:
        env = Env(tmp, "2027-02-10T00:00")
        RP.open_record(env.path, now_ms=env.clock()).close()
        sender = ScriptedSender()
        make_first(sender)
        env.clock.ms = tpe("2027-02-11T00:05")
        with capture(*LOGGERS) as cap:
            s = env.session(sender)
            try:
                wake = s.pump()
                first = [c for c in sender.calls]
                assert [c["key"] for c in first] in ([], [daily("2027-02-10")]), first
                assert wake == env.clock.ms + RETRY_MS, (wake, env.clock.ms)
                (row,) = env.rows()
                assert row["status"] == RP.STATUS_PENDING and row["handoff_open"] == 0
                assert row["next_attempt_ms"] == wake and row["attempts"] == 1
                text = row["text"]
                env.clock.ms = wake - 1
                s.pump()
                assert len(sender.calls) == len(first), "還沒到重試時刻就重送了"
                sender.accept, sender.auto = True, deliver_all
                env.clock.ms = wake
                s.pump()
            finally:
                s.close()
        keys = sender.keys()[len(first):]
        assert keys == [daily("2027-02-10"), "report:tenday:2027-02-01"], keys
        assert sender.calls[len(first)]["text"] == text
        rows = env.rows()
        assert rows[0]["status"] == RP.STATUS_DELIVERED and rows[0]["attempts"] == 2
        warns = [m for m in cap.messages(logging.WARNING) if "沒有確定送達" in m]
        assert len(warns) == 1, warns
        return s.rep.stats()


def test_ac7_restart_6_gave_up_abandoned_and_rejection_are_retried_with_the_same_text():
    for status in (RESULT_GAVE_UP, RESULT_ABANDONED):
        def first(sender, status=status):
            sender.auto = lambda call: result(status, call["key"], detail="sender says " + status)
        stats = _retry_case(first)
        assert stats["retries"] == 1 and stats["delivered"] == 2 and stats["failed"] == 0, stats

    def reject(sender):
        sender.accept = False                     # send() 回傳 False：不會有 on_done
    stats = _retry_case(reject)
    assert stats["retries"] == 1 and stats["handoffs"] == 2, stats

    def raising(sender):
        def boom(call):
            sender.auto = None
            raise RuntimeError("sender exploded")
        sender.auto = boom
    stats = _retry_case(raising)
    assert stats["retries"] == 1, stats


def test_ac7_restart_7_failed_is_terminal_error_and_not_resent():
    with tempdir() as tmp:
        env = Env(tmp, "2027-02-10T00:00")
        sender = ScriptedSender(lambda call: (result(RESULT_FAILED, call["key"], uncertain=True,
                                                     detail="HTTP 400 chat not found")
                                              if call["key"].startswith("report:daily") else deliver_all(call)))
        with capture(*LOGGERS) as cap:
            s = env.session(sender)
            try:
                env.clock.ms = tpe("2027-02-11T00:05")
                s.pump()
                env.clock.ms += RETRY_MS * 3
                s.pump()
            finally:
                s.close()
        assert sender.keys() == [daily("2027-02-10"), "report:tenday:2027-02-01"], sender.keys()
        rows = env.rows()
        assert rows[0]["status"] == RP.STATUS_FAILED and rows[0]["last_error"] == "HTTP 400 chat not found"
        assert rows[1]["status"] == RP.STATUS_DELIVERED
        errors = cap.messages(logging.ERROR)
        assert len(errors) == 1 and "永久失敗" in errors[0] and "可能其實已經收到" in errors[0], errors
        st = s.rep.stats()
        assert st["failed"] == 1 and "永久失敗" in st["last_error"], st


# ============================== AC-7 組報表失敗 ==============================
def test_ac7_compose_failures_log_once_write_nothing_and_recover_while_a3_keeps_writing():
    with tempdir() as tmp:
        env = Env(tmp, "2027-02-10T00:00")
        os.remove(env.live)                                   # ① live.sqlite3 不存在
        sender = ScriptedSender(deliver_all)
        with capture(*LOGGERS) as cap:
            s = env.session(sender, busy_timeout=0.1)
            try:
                env.clock.ms = tpe("2027-02-11T00:05")
                for _ in range(3):
                    s.pump()
                    env.clock.ms += POLL_MS
                assert sender.calls == [] and env.rows() == []
                missing = [m for m in cap.messages(logging.ERROR) if "組不出來" in m]
                assert len(missing) == 1 and "不存在" in missing[0], missing
                assert s.rep.stats()["compose_errors"] == 3 and "不存在" in s.rep.stats()["last_error"]
                # A3 在這段期間建檔、寫入照常
                with env.books() as b:
                    sid = b.entry("s4", "AAA_USDT_PERP", tpe("2027-02-10T10:00"))
                    b.close_pos(sid, tpe("2027-02-10T11:00"))
                # ② outbox 版本不符
                c = sqlite3.connect(env.outbox)
                c.execute("PRAGMA user_version = %d" % (OB.SCHEMA_VERSION + 9))
                c.commit()
                c.close()
                for _ in range(3):
                    s.pump()
                    env.clock.ms += POLL_MS
                version = [m for m in cap.messages(logging.ERROR) if "組不出來" in m and "schema 版本" in m]
                assert len(version) == 1 and sender.calls == [] and env.rows() == [], version
                c = sqlite3.connect(env.outbox)
                c.execute("PRAGMA user_version = %d" % OB.SCHEMA_VERSION)
                c.commit()
                c.close()
                # ③ outbox 被另一條連線鎖住（exclusive locking mode）：組不出來；A3 對 live.sqlite3 的寫入照常
                a3 = store.open_store(env.live)
                blocker = sqlite3.connect(env.outbox, isolation_level=None)
                try:
                    blocker.execute("PRAGMA locking_mode=EXCLUSIVE")
                    blocker.execute("BEGIN EXCLUSIVE")
                    blocker.execute("UPDATE outbox SET detail = detail")
                    for i in range(3):
                        s.pump()
                        sid = "LOCKTEST-%d" % i
                        opened = tpe("2027-02-10T12:00") + i * MIN
                        a3.record_entry(signal_id=sid, user_id=config.STRATEGY_USER_ID, strategy="s4",
                                        symbol="L%d_USDT_PERP" % i, side=config.DIRECTION_SHORT,
                                        bar_open_ms=opened - HOUR, signal_price=100.0, take_profit_price=90.0,
                                        stop_loss_price=110.0, features={}, created_ms=opened, opened_ms=opened)
                        a3.close_position(sid, exit_reason=TP, exit_price=96.0, closed_ms=opened + HOUR,
                                          exit_features={})
                        env.clock.ms += POLL_MS
                finally:
                    blocker.execute("ROLLBACK")
                    blocker.close()
                    a3.close()
                c = sqlite3.connect(env.live)
                n_locktest = c.execute("SELECT count(*) FROM positions WHERE signal_id LIKE 'LOCKTEST-%' "
                                       "AND status = 'closed'").fetchone()[0]
                c.close()
                assert n_locktest == 3, "outbox 被鎖住期間 A3 的寫入應該照常成功"
                locked = [m for m in cap.messages(logging.ERROR) if "組不出來" in m and "locked" in m]
                assert len(locked) == 1 and sender.calls == [] and env.rows() == [], cap.messages(logging.ERROR)
                # 恢復：下一輪就發，內容含失敗期間 A3 寫進去的交易
                s.pump()
            finally:
                s.close()
        assert sender.keys() == [daily("2027-02-10"), "report:tenday:2027-02-01"], sender.keys()
        assert "平倉：1 筆（止盈 1、止損 0）" in sender.calls[0]["text"].split("\n")
        assert all(r["status"] == RP.STATUS_DELIVERED for r in env.rows())
        errors = cap.messages(logging.ERROR)
        assert len([m for m in errors if "組不出來" in m]) == 3, errors
        assert s.rep.stats()["compose_errors"] == 9


# ============================== FR-8 發送紀錄 ==============================
def test_fr8_record_schema_constraints_and_pragmas():
    with tempdir() as tmp:
        path = os.path.join(tmp, "new", "dir", "a_channel_reports.sqlite3")
        created = tpe("2027-02-10T08:00")
        rec = RP.open_record(path, now_ms=created)
        try:
            conn = rec._conn
            assert conn.execute("PRAGMA user_version").fetchone()[0] == RP.SCHEMA_VERSION == 1
            assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
            assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2, "synchronous 要是 FULL"
            assert rec.created_ms == created
            assert [tuple(r) for r in conn.execute("SELECT created_ms FROM meta")] == [(created,)]
            p = R.period_of("daily", day("2027-02-10"))
            seq = rec.insert_pending(p, scheduled=R.scheduled_ms(p), text="t", late=False, summary={}, now_ms=created)
            try:
                rec.insert_pending(p, scheduled=R.scheduled_ms(p), text="t", late=False, summary={}, now_ms=created)
            except sqlite3.IntegrityError:
                pass
            else:
                raise AssertionError("同一期兩列應該被 UNIQUE 擋下")
            bad_rows = [
                dict(status="pending", text=None),                                     # 待送一定有凍結文字
                dict(status="delivered", text="t", finalized_ms=None),                 # 終態一定有 finalized
                dict(status="delivered", text="t", finalized_ms=created, handoff_open=1),
                dict(status="skipped", text="t", finalized_ms=created),                # skipped 沒有文字
                dict(status="skipped", text=None, finalized_ms=created, attempts=1),
                dict(status="bogus", text="t"),
                dict(status="pending", text="t", kind="weekly"),
                dict(status="pending", text="t", period_end_ms=p.start_ms),            # end > start
            ]
            for i, over in enumerate(bad_rows):
                cols = dict(kind="monthly", period_start_ms=p.start_ms + i, period_end_ms=p.end_ms, period_label="x",
                            report_key="k", scheduled_ms=p.end_ms, created_ms=created)
                cols.update(over)
                names = sorted(cols)
                try:
                    conn.execute("INSERT INTO reports (%s) VALUES (%s)" % (", ".join(names), ", ".join("?" * len(names))),
                                 [cols[n] for n in names])
                except sqlite3.IntegrityError:
                    continue
                raise AssertionError("CHECK 沒有擋下：%r" % (over,))
            try:
                rec.finalize(seq, RP.STATUS_SKIPPED, now_ms=created)
            except ValueError:
                pass
            else:
                raise AssertionError("finalize 只接受 delivered / failed")
            rec.finalize(seq, RP.STATUS_DELIVERED, now_ms=created, message_id=7)
            for fn in (lambda: rec.mark_handoff(seq, now_ms=created),
                       lambda: rec.record_retry(seq, next_attempt_ms=created, error="x"),
                       lambda: rec.finalize(seq, RP.STATUS_FAILED, now_ms=created)):
                try:
                    fn()
                except RP.RecordError:
                    continue
                raise AssertionError("終態的列不可以再改")
        finally:
            rec.close()
        with RP.open_record(path, now_ms=created + DAY_MS) as rec:
            assert rec.created_ms == created and [r["status"] for r in rec.rows()] == [RP.STATUS_DELIVERED]
        # 版本比較新、或不是本模組建的檔 → RecordError，不動它
        newer = os.path.join(tmp, "newer.sqlite3")
        foreign = os.path.join(tmp, "foreign.sqlite3")
        c = sqlite3.connect(newer)
        c.execute("PRAGMA user_version = %d" % (RP.SCHEMA_VERSION + 7))
        c.commit()
        c.close()
        c = sqlite3.connect(foreign)
        c.execute("CREATE TABLE something (a)")
        c.commit()
        c.close()
        for bad, needle in ((newer, "比本程式認得的"), (foreign, "不是")):
            before = open(bad, "rb").read()
            try:
                RP.open_record(bad, now_ms=created)
            except RP.RecordError as e:
                assert needle in str(e), e
            else:
                raise AssertionError("應該拋 RecordError：%s" % bad)
            assert open(bad, "rb").read() == before
    src = open(os.path.join(REPO_ROOT, "live", "a_channel_report_push.py"), encoding="utf-8").read()
    assert "DELETE" not in src.upper().replace("不自動刪除", ""), "發送紀錄不可以刪列"


# ============================== 真執行緒 ==============================
def test_thread_start_sends_due_reports_and_stop_ends_the_thread():
    with tempdir() as tmp:
        env = Env(tmp, "2027-02-10T00:00")
        RP.open_record(env.path, now_ms=env.clock()).close()
        env.clock.ms = tpe("2027-02-11T00:06")
        sender = ScriptedSender(deliver_all)
        rep = RP.ChannelReporter(sender, path=env.path, live_db=env.live, outbox_db=env.outbox, now_ms=env.clock)
        with capture(*LOGGERS) as cap:
            counts = rep.start()
            try:
                assert counts[RP.STATUS_PENDING] == 0
                assert any(t.name == RP.THREAD_NAME and t.is_alive() for t in threading.enumerate())
                wait_until(lambda: rep.stats()["delivered"] == 2, "報表執行緒送出兩則")
                try:
                    rep.start()
                except RuntimeError:
                    pass
                else:
                    raise AssertionError("start() 只能一次")
            finally:
                assert rep.stop() is True
        assert not rep.worker_alive and not any(t.name == RP.THREAD_NAME and t.is_alive() for t in threading.enumerate())
        st = rep.stats()
        for k in ("worker_alive", "delivered", "failed", "skipped", "pending", "compose_errors", "last_error"):
            assert k in st, k
        assert (st["worker_alive"], st["worker_crashed"], st["failed"], st["skipped"], st["pending"]) == (
            False, False, 0, 0, 0), st
        assert sender.keys() == [daily("2027-02-10"), "report:tenday:2027-02-01"]
        infos = [m for m in cap.messages(logging.INFO) if "）已送達（" in m]
        assert len(infos) == len(sender.calls) and "message_id=101" in infos[0] and "策略4" in infos[0], infos


def test_thread_open_failure_is_reported_by_start():
    with tempdir() as tmp:
        sender = ScriptedSender(deliver_all)
        rep = RP.ChannelReporter(sender, path=tmp, live_db=os.path.join(tmp, "l"), outbox_db=os.path.join(tmp, "o"),
                                 now_ms=Clock(tpe("2027-02-10T00:00")))
        try:
            rep.start()
        except (sqlite3.Error, RP.RecordError, OSError):
            pass
        else:
            raise AssertionError("發送紀錄開不了，start() 應該拋例外")
        finally:
            rep.stop()
        assert not rep.worker_alive


# ============================== AC-8 接線 ==============================
def _run(argv, **kw):
    from live import a_channel
    kw.setdefault("s5_feed_factory", lambda feed, tracker: FakeS5())
    with capture(*LOGGERS) as cap:
        rc = a_channel.main(argv, setup_logging=False, **kw)
    return rc, cap


class OrderTracker(FakeTracker):
    def __init__(self, bus, order):
        super().__init__(bus)
        self.order = order

    def start(self):
        self.order.append("A3 start")
        super().start()

    def wait_ready(self, timeout=None):
        self.order.append("A3 ready")
        return True

    def stop(self):
        self.order.append("A3 stop")

    def join(self, timeout=None):
        self.order.append("A3 join")
        return True


def _order_pusher(order, clock):
    class OrderPusher(P.ChannelPusher):
        def shutdown(self, *a, **k):
            order.append("T2 shutdown")
            return super().shutdown(*a, **k)
    return lambda s: OrderPusher(s, market=FakeMarket(), now_ms=clock)


def _starter(sender):
    return lambda: type("Starter", (), {"start": lambda self: None, "send": sender.send, "stop": sender.stop})()


def _a1(order, until=None):
    def factory(on_result):
        order.append("A1")
        order.append("R-A alive at A1" if any(t.name == RP.THREAD_NAME and t.is_alive() for t in threading.enumerate())
                     else "R-A not alive at A1")
        return FakeFeed(until=until)
    return factory


def _a5(order):
    def factory(feed, tracker):
        order.append("A5")
        return FakeS5()
    return factory


def test_ac8_without_the_flag_there_is_no_record_and_no_thread():
    with tempdir() as tmp, outbox_path_in(tmp), secret_reads_forbidden() as seen:
        called = []
        rc, cap = _run(["--duration", "5"], tracker_factory=lambda bus: FakeTracker(bus),
                       feed_factory=lambda on_result: FakeFeed(), reporter_factory=lambda s: called.append(s))
        assert rc == 0 and called == [] and seen == [], (rc, called, seen)
        assert not os.path.exists(config.A_CHANNEL_REPORT_DB_PATH)
        assert not os.path.exists(os.path.dirname(config.A_CHANNEL_REPORT_DB_PATH))
        assert not any(t.name == RP.THREAD_NAME for t in threading.enumerate())
        assert not [m for m in cap.messages() if "報表" in m], cap.messages()


def test_ac8_flag_starts_after_a3_ready_and_stops_before_push_shutdown():
    with tempdir() as tmp, outbox_path_in(tmp):
        env = Env(tmp, "2027-02-10T00:00")
        RP.open_record(config.A_CHANNEL_REPORT_DB_PATH, now_ms=env.clock()).close()
        env.clock.ms = tpe("2027-02-11T00:06")
        order, box = [], []
        sender = ScriptedSender(deliver_all)

        class OrderReporter(RP.ChannelReporter):
            def start(self):
                order.append("R-A start")
                return super().start()

            def stop(self, timeout=None):
                order.append("R-A stop")
                return super().stop(timeout)

        def reporter_factory(s):
            box.append(OrderReporter(s, live_db=env.live, outbox_db=env.outbox, now_ms=env.clock))
            return box[0]
        rc, cap = _run(["--duration", "5", "--push-tg"], tracker_factory=lambda bus: OrderTracker(bus, order),
                       feed_factory=_a1(order, until=lambda: box and box[0].stats()["delivered"] >= 2),
                       s5_feed_factory=_a5(order), sender_factory=_starter(sender),
                       pusher_factory=_order_pusher(order, env.clock), reporter_factory=reporter_factory)
        assert rc == 0, (rc, cap.messages(logging.ERROR))
        ix = order.index
        assert ix("A3 ready") < ix("R-A start") < ix("A1") < ix("A5"), order
        assert "R-A alive at A1" in order
        assert ix("A3 join") < ix("R-A stop") < ix("T2 shutdown"), order
        assert order.count("R-A start") == 1 and order.count("R-A stop") == 1
        assert os.path.isfile(config.A_CHANNEL_REPORT_DB_PATH)
        assert box[0].path == os.path.abspath(config.A_CHANNEL_REPORT_DB_PATH)
        assert sender.keys() == [daily("2027-02-10"), "report:tenday:2027-02-01"]
        msgs = cap.messages()
        assert any("A 頻道報表：已啟用" in m for m in msgs) and any("A 頻道報表總結" in m for m in msgs), msgs
        assert not any(t.name in (RP.THREAD_NAME, P.THREAD_NAME) and t.is_alive() for t in threading.enumerate())


def test_ac8_reporter_start_failure_exits_1_before_a1_and_a5():
    with tempdir() as tmp, outbox_path_in(tmp):
        order = []
        sender = ScriptedSender(deliver_all)
        clock = Clock(tpe("2027-02-10T00:00"))
        blocked = os.path.join(tmp, "is_a_directory")
        os.makedirs(blocked)
        rc, cap = _run(["--duration", "5", "--push-tg"], tracker_factory=lambda bus: OrderTracker(bus, order),
                       feed_factory=_a1(order), s5_feed_factory=_a5(order), sender_factory=_starter(sender),
                       pusher_factory=_order_pusher(order, clock),
                       reporter_factory=lambda s: RP.ChannelReporter(s, path=blocked, now_ms=clock))
        assert rc == 1, rc
        assert "A1" not in order and "A5" not in order, order
        assert order.index("A3 ready") < order.index("A3 stop") < order.index("A3 join") < order.index("T2 shutdown")
        errors = cap.messages(logging.ERROR)
        assert any("建立或啟動失敗" in m for m in errors), errors
        assert not any(t.name in (RP.THREAD_NAME, P.THREAD_NAME) and t.is_alive() for t in threading.enumerate())


def test_ac8_reporter_factory_raising_also_exits_1():
    with tempdir() as tmp, outbox_path_in(tmp):
        order = []

        def broken(s):
            raise RuntimeError("cannot build reporter")
        rc, cap = _run(["--duration", "5", "--push-tg"], tracker_factory=lambda bus: OrderTracker(bus, order),
                       feed_factory=_a1(order), s5_feed_factory=_a5(order),
                       sender_factory=_starter(ScriptedSender(deliver_all)),
                       pusher_factory=_order_pusher(order, Clock(tpe("2027-02-10T00:00"))), reporter_factory=broken)
        assert rc == 1 and "A1" not in order and "T2 shutdown" in order, (rc, order)


class _Boom(BaseException):
    """模擬報表執行緒意外結束（BaseException，不是一般 Exception）。"""


def test_ac8_reporter_crash_is_logged_others_keep_running_and_exit_is_1():
    with tempdir() as tmp, outbox_path_in(tmp):
        env = Env(tmp, "2027-02-10T00:00")
        order, box = [], []

        class Crashing(RP.ChannelReporter):
            def pump(self, record):
                raise _Boom("simulated crash")

        def reporter_factory(s):
            box.append(Crashing(s, live_db=env.live, outbox_db=env.outbox, now_ms=env.clock))
            return box[0]
        rc, cap = _run(["--duration", "5", "--push-tg"], tracker_factory=lambda bus: OrderTracker(bus, order),
                       feed_factory=_a1(order, until=lambda: box and box[0].stats()["worker_crashed"]),
                       s5_feed_factory=_a5(order), sender_factory=_starter(ScriptedSender(deliver_all)),
                       pusher_factory=_order_pusher(order, env.clock), reporter_factory=reporter_factory)
        assert rc == 1, rc
        assert "A1" in order and "A5" in order and order.index("A3 join") < order.index("T2 shutdown"), order
        st = box[0].stats()
        assert st["worker_alive"] is False and st["worker_crashed"] is True and "_Boom" in st["last_error"], st
        errors = cap.messages(logging.ERROR)
        assert any("意外結束" in m and "_Boom" in m for m in errors), errors
        assert any("報表執行緒在執行中意外結束" in m for m in errors), errors


# ============================== AC-10 密鑰 ==============================
def test_ac10_push_tg_with_fake_secrets_leaks_nothing():
    with tempdir() as tmp, outbox_path_in(tmp), tgt.harness() as h:
        env = Env(tmp, "2027-02-10T00:00")
        with env.books() as b:
            sid = b.entry("s4", "ACE_USDT_PERP", tpe("2027-02-10T10:00"))
            b.close_pos(sid, tpe("2027-02-10T11:00"))
        RP.open_record(config.A_CHANNEL_REPORT_DB_PATH, now_ms=env.clock()).close()
        env.clock.ms = tpe("2027-02-11T00:06")
        box = []

        def reporter_factory(s):
            assert isinstance(s, ChannelSender), type(s)
            box.append(RP.ChannelReporter(s, live_db=env.live, outbox_db=env.outbox, now_ms=env.clock))
            return box[0]
        order = []
        rc, cap = _run(["--duration", "5", "--push-tg"], tracker_factory=lambda bus: OrderTracker(bus, order),
                       feed_factory=_a1(order, until=lambda: box and box[0].stats()["delivered"] >= 2),
                       s5_feed_factory=_a5(order), sender_factory=lambda: h.sender(),
                       pusher_factory=lambda s: P.ChannelPusher(s, market=FakeMarket(), now_ms=env.clock),
                       reporter_factory=reporter_factory)
        assert rc == 0, (rc, cap.messages(logging.ERROR))
        posted = [c.payload.get("text", "") for c in h.tg.calls if c.payload]
        assert any(t.startswith(chr(0x1F4CA) + " 策略績效日報  2027-02-10") for t in posted), posted
        with RP.open_record(config.A_CHANNEL_REPORT_DB_PATH, now_ms=env.clock()) as rec:
            rows = rec.rows()
        assert [r["status"] for r in rows] == [RP.STATUS_DELIVERED] * len(rows) and rows
        db_dir = os.path.dirname(config.A_CHANNEL_REPORT_DB_PATH)
        blobs = []
        for n in os.listdir(db_dir):
            if n.startswith("a_channel_reports"):
                with open(os.path.join(db_dir, n), "rb") as f:
                    blobs.append(f.read().decode("utf-8", "replace"))
        texts = cap.messages() + list(h.logs.lines) + blobs + [repr(box[0].stats())] + [repr(r) for r in rows]
        assert blobs and not find_leaks(texts), find_leaks(texts)


# ============================== runner ==============================
OWN_CAGE = {"test_ac10_push_tg_with_fake_secrets_leaks_nothing"}   # tgt.harness() 自己有 socket 籠子（不可巢狀）


def _run_tests(tests):
    failed = 0
    for name, fn in tests:
        try:
            if name in OWN_CAGE:
                fn()
                print(f"PASS  {name}")
                continue
            with OfflineCage() as cage:
                fn()
            assert not cage.attempts, "有程式企圖連網：%r" % cage.attempts
            assert not cage.sleeps, "有程式呼叫了真的 time.sleep：%r" % cage.sleeps
            print(f"PASS  {name}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL  {name}: {type(e).__name__}: {e}")
    return failed


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    watched = (paths.RUNTIME_DIR, config.LIVE_DB_PATH, config.A_CHANNEL_OUTBOX_DB_PATH,
               config.A_CHANNEL_REPORT_DB_PATH)
    existed = [os.path.exists(p) for p in watched]
    pristine = (socket.socket.connect, socket.create_connection, socket.getaddrinfo, time.sleep)
    all_tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    n_failed = _run_tests(all_tests)
    runner_failed = 0
    if (socket.socket.connect, socket.create_connection, socket.getaddrinfo, time.sleep) != pristine:
        runner_failed += 1
        print("FAIL  <runner>: socket / time.sleep 沒有還原")
    if [os.path.exists(p) for p in watched] != existed:
        runner_failed += 1
        print(f"FAIL  <runner>: 測試在真正的 runtime/ 留下了東西（{paths.RUNTIME_DIR}）")
    leftover = [t.name for t in threading.enumerate()
                if t.name in (RP.THREAD_NAME, P.THREAD_NAME, "tg-channel-sender") and t.is_alive()]
    if leftover:
        runner_failed += 1
        print(f"FAIL  <runner>: 還有執行緒活著 {leftover}")
    print(f"\n{len(all_tests) - n_failed} passed, {n_failed} failed"
          + (f", {runner_failed} runner check(s) failed" if runner_failed else ""))
    sys.exit(1 if n_failed or runner_failed else 0)
