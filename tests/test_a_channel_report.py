# -*- coding: utf-8 -*-
"""
R-A（live.a_channel_report：期間、唯讀讀取、統計、訊息文字、離線 CLI）的單元測試。

  AC-1  期間：2026-11（30 天）、2026-12 → 2027-01（跨年）、2027-02（28 天）、2028-02（29 天）的日報 / 三旬 / 月報
        [start, end) 與發送時刻和人工推算相同；台北 00:00 的邊界（end − 1 算這一期、end 算下一期；UTC 日界不同的時刻）
  AC-2  篩選：合成資料經真實 API（store.open_store + record_entry / mark_published / close_position、
        open_outbox + insert / finalize）寫入；只有進場 delivered 的計入、別的 user_id 不計入、同幣兩策略各自一段、
        H(s) 的四種邊界；非做空拋例外
  AC-5  文字：三種報表 × 有 / 無平倉 × 有 / 無延遲註記的全文逐字比對（含 PRD 的日報樣本）；隨機 ≥ 3000 筆的明細百分比
        與 a_channel_text.format_exit() 的「名目報酬」逐字相同；費率 0.00045 顯示 0.045%；超過 4096 的截斷；
        全文逐行白名單（不含 features、稽核旗標、策略參數名稱）
  FR-5  唯讀：檔案不存在 / 版本不符拋例外且不建檔；delivered 集合來自 Outbox.delivered_entry_signal_ids()；
        WAL 三種狀態（① 正常關閉、② 寫入者開著且部分資料只在 -wal、③ 只有主檔 + -wal）讀得到 -wal 的資料、
        原本就存在的主檔與 -wal 逐位元組不變（詳細實測與 CLI 版本由 DQA 的 AC-6 腳本負責）
  FR-6  CLI（子行程）：exit 0 / 1 / 2、stdout 與 --out、期間未結束的 stderr 說明、cp950 / cp1252 主控台不當掉、
        不讀密鑰、不寫資料庫
  NFR-2 四個新檔的字面值掃描（附反向對照）；模組命名與相依

全程離線：每個測試都在 socket 籠子裡跑（tests/test_tg_channel.py 的 OfflineCage，禁止真的 sleep）。
資料庫一律開在暫存目錄，不碰 repo 的 runtime/。不依賴 pytest：直接 `python tests/test_a_channel_report.py`。
"""
import ast
import collections
import contextlib
import hashlib
import importlib.util
import io
import math
import os
import random
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import tokenize
from datetime import date, datetime, timedelta

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, TESTS_DIR)

import test_tg_channel as tgt  # noqa: E402
from test_tg_channel import FAKE_CHAT, FAKE_TOKEN, OfflineCage, find_leaks  # noqa: E402

from live import a_channel_outbox as OB  # noqa: E402
from live import a_channel_report as R  # noqa: E402
from live import a_channel_text as T  # noqa: E402
from live import config, paths, store  # noqa: E402
from live import notional_tracker as nt  # noqa: E402
from live.logsetup import TAIPEI  # noqa: E402
from live.signal_events import ExitEvent  # noqa: E402
from live.tg_channel import utf16_units  # noqa: E402
from strategy import s4_signal, s5_signal  # noqa: E402

SPECS = nt.build_specs()
SEC = 1000
MIN = 60 * SEC
HOUR = 60 * MIN
TP, SL = config.EXIT_TAKE_PROFIT, config.EXIT_STOP_LOSS

# 本檔寫進 features / 出場旗標的哨兵：報表全文絕對不可以出現
LEAK_FEATURE_KEY = "ret2h_leak_canary"
LEAK_FEATURE_VALUE = 0.918273645
LEAK_FLAG = "minor_resolved"

# 期望文字用到的符號（逐字比對：不引用被測模組的常數）
CHART, CHECK, CROSS = chr(0x1F4CA), chr(0x2705), chr(0x274C)
WARN, REF, ELLIPSIS = chr(0x26A0) + chr(0xFE0F), chr(0x203B), chr(0x2026)
FOOT_BASIS = REF + " 名目報酬以訊號價與理論出場價計；手續費以單邊 0.05% 估算，未計資金費率"
FOOT_SCOPE = REF + " 只統計頻道上發出過進場訊號的交易"
FOOT_DISCLAIMER = WARN + " 過去績效不代表未來表現，不構成投資建議"
LATE_LINE = WARN + " 延遲發布：本則晚於排定時間送出"


# ============================== 測試工具 ==============================
@contextlib.contextmanager
def tempdir(prefix="r_a_report_"):
    d = tempfile.mkdtemp(prefix=prefix)
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


def day(iso):
    return date.fromisoformat(iso)


def tpe(iso):
    """台北時間的 ISO 日期時間（例如 2026-09-29T15:35）→ UTC epoch 毫秒。"""
    return int(datetime.fromisoformat(iso).replace(tzinfo=TAIPEI).timestamp()) * SEC


def utc(iso):
    """帶時區的 ISO（例如 2026-11-30T16:05:00+00:00）→ UTC epoch 毫秒。"""
    return int(datetime.fromisoformat(iso).timestamp()) * SEC


def sha(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def listing(d):
    return {n: sha(os.path.join(d, n)) for n in sorted(os.listdir(d))}


def row(sid, strategy, opened, closed=None, *, reason=TP, entry=100.0, exit_price=96.0, user=None,
        side=None):
    """tally() 吃的 positions 列（純資料，不經資料庫）。closed=None 表示還開著。"""
    return {"signal_id": sid, "user_id": config.STRATEGY_USER_ID if user is None else user, "strategy": strategy,
            "status": store.STATUS_OPEN if closed is None else store.STATUS_CLOSED,
            "exit_reason": None if closed is None else reason, "entry_price": entry,
            "exit_price": None if closed is None else exit_price, "opened_ms": opened, "closed_ms": closed,
            "side": config.DIRECTION_SHORT if side is None else side}


class Books:
    """真的 live.sqlite3 + outbox（暫存目錄），合成資料一律經真實 API 寫入。連線開著時就是 AC-6 的狀態 ②。"""

    def __init__(self, tmp):
        self.live_path = os.path.join(tmp, "db", "live.sqlite3")
        self.outbox_path = os.path.join(tmp, "db", "a_channel_outbox.sqlite3")
        self.store = store.open_store(self.live_path)
        self.outbox = OB.open_outbox(self.outbox_path)
        self.message_ids = 0

    def close(self):
        self.store.close()
        self.outbox.close()

    def _outbox_row(self, sid, kind, strategy, symbol, status, at_ms):
        self.outbox.insert(signal_id=sid, kind=kind, strategy=strategy, symbol=symbol, event={}, snapshot={},
                           received_ms=at_ms)
        if status != OB.STATUS_PENDING:
            self.message_ids += 1
            seq = self.outbox.get(sid, kind)["seq"]
            self.outbox.finalize(seq, status, now_ms=at_ms + SEC,
                                 message_id=self.message_ids if status == OB.STATUS_DELIVERED else None)

    def entry(self, strategy, symbol, opened_ms, *, outbox=OB.STATUS_DELIVERED, user=None, price=100.0):
        """進場：record_entry + mark_published，outbox 的進場列照 outbox 參數（None = 沒有進場列）。回傳 signal_id。"""
        spec = SPECS[strategy]
        sid = nt.make_signal_id(spec, symbol, opened_ms)
        self.store.record_entry(
            signal_id=sid, user_id=config.STRATEGY_USER_ID if user is None else user, strategy=strategy,
            symbol=symbol, side=config.DIRECTION_SHORT, bar_open_ms=opened_ms - spec.main_ms, signal_price=price,
            take_profit_price=price * 0.9, stop_loss_price=price * 1.1,
            features={LEAK_FEATURE_KEY: LEAK_FEATURE_VALUE, "gap_ok": True}, created_ms=opened_ms,
            opened_ms=opened_ms)
        self.store.mark_published(sid, published_ms=opened_ms + SEC)
        if outbox is not None:
            self._outbox_row(sid, OB.KIND_ENTRY, strategy, symbol, outbox, opened_ms + SEC)
        return sid

    def close_pos(self, sid, closed_ms, *, reason=TP, exit_price=96.0, exit_outbox=None):
        pos = self.store.close_position(sid, exit_reason=reason, exit_price=exit_price, closed_ms=closed_ms,
                                        exit_features={LEAK_FLAG: True, "recovered": False})
        if exit_outbox is not None:
            self._outbox_row(sid, OB.KIND_EXIT, pos["strategy"], pos["symbol"], exit_outbox, closed_ms + SEC)
        return pos


# PRD 的日報樣本（2026-09-29）：策略4 三筆平倉（止盈 2、止損 1）+ 一筆持倉，策略5 沒有平倉
SAMPLE_ROWS = (
    row("#ACE-S4-20260929-1535", "s4", tpe("2026-09-29T15:35"), tpe("2026-09-29T15:50")),
    row("#NMR-S4-20260929-1820", "s4", tpe("2026-09-29T18:20"), tpe("2026-09-29T19:00")),
    row("#QNT-S4-20260929-2105", "s4", tpe("2026-09-29T21:05"), tpe("2026-09-29T22:00"), reason=SL,
        exit_price=105.0),
    row("#XYZ-S4-20260929-1000", "s4", tpe("2026-09-29T10:00")),
)
SAMPLE_DELIVERED = {r["signal_id"] for r in SAMPLE_ROWS}

PRD_DAILY_SAMPLE = "\n".join([
    CHART + " 策略績效日報  2026-09-29",
    "",
    "【策略4】",
    "平倉：3 筆（止盈 2、止損 1）",
    "勝率：66.7%",
    "名目報酬合計：+3.0%",
    "估算手續費：-0.3%",
    "淨報酬合計：+2.7%",
    "持倉中：1 筆（不計入）",
    "明細：",
    CHECK + " #ACE-S4-20260929-1535  +4.0%",
    CHECK + " #NMR-S4-20260929-1820  +4.0%",
    CROSS + " #QNT-S4-20260929-2105  -5.0%",
    "",
    "【策略5】",
    "本日無平倉",
    "持倉中：0 筆",
    "",
    REF + " 期間：2026-09-29 00:00～24:00（台北時間），依出場時間歸日",
    FOOT_BASIS,
    FOOT_SCOPE,
    FOOT_DISCLAIMER,
])


# ============================== AC-1 期間 ==============================
# 人工推算：(種類, 當期內的某一天, start, end, end 的 UTC, 排定發送時刻的 UTC)。台北 00:00 = UTC 前一天 16:00
AC1_TABLE = [
    # 2026-11（30 天）
    ("daily", "2026-11-01", "2026-11-01", "2026-11-02", "2026-11-01T16:00:00+00:00", "2026-11-01T16:05:00+00:00"),
    ("daily", "2026-11-30", "2026-11-30", "2026-12-01", "2026-11-30T16:00:00+00:00", "2026-11-30T16:05:00+00:00"),
    ("tenday", "2026-11-05", "2026-11-01", "2026-11-11", "2026-11-10T16:00:00+00:00", "2026-11-10T16:05:00+00:00"),
    ("tenday", "2026-11-20", "2026-11-11", "2026-11-21", "2026-11-20T16:00:00+00:00", "2026-11-20T16:05:00+00:00"),
    ("tenday", "2026-11-30", "2026-11-21", "2026-12-01", "2026-11-30T16:00:00+00:00", "2026-11-30T16:05:00+00:00"),
    ("monthly", "2026-11-01", "2026-11-01", "2026-12-01", "2026-11-30T16:00:00+00:00", "2026-11-30T16:05:00+00:00"),
    # 2026-12 → 2027-01（跨年，31 天）
    ("daily", "2026-12-31", "2026-12-31", "2027-01-01", "2026-12-31T16:00:00+00:00", "2026-12-31T16:05:00+00:00"),
    ("daily", "2027-01-01", "2027-01-01", "2027-01-02", "2027-01-01T16:00:00+00:00", "2027-01-01T16:05:00+00:00"),
    ("tenday", "2026-12-21", "2026-12-21", "2027-01-01", "2026-12-31T16:00:00+00:00", "2026-12-31T16:05:00+00:00"),
    ("tenday", "2026-12-31", "2026-12-21", "2027-01-01", "2026-12-31T16:00:00+00:00", "2026-12-31T16:05:00+00:00"),
    ("tenday", "2027-01-10", "2027-01-01", "2027-01-11", "2027-01-10T16:00:00+00:00", "2027-01-10T16:05:00+00:00"),
    ("monthly", "2026-12-31", "2026-12-01", "2027-01-01", "2026-12-31T16:00:00+00:00", "2026-12-31T16:05:00+00:00"),
    ("monthly", "2027-01-01", "2027-01-01", "2027-02-01", "2027-01-31T16:00:00+00:00", "2027-01-31T16:05:00+00:00"),
    # 2027-02（28 天）
    ("daily", "2027-02-28", "2027-02-28", "2027-03-01", "2027-02-28T16:00:00+00:00", "2027-02-28T16:05:00+00:00"),
    ("tenday", "2027-02-11", "2027-02-11", "2027-02-21", "2027-02-20T16:00:00+00:00", "2027-02-20T16:05:00+00:00"),
    ("tenday", "2027-02-28", "2027-02-21", "2027-03-01", "2027-02-28T16:00:00+00:00", "2027-02-28T16:05:00+00:00"),
    ("monthly", "2027-02-14", "2027-02-01", "2027-03-01", "2027-02-28T16:00:00+00:00", "2027-02-28T16:05:00+00:00"),
    # 2028-02（29 天）
    ("daily", "2028-02-28", "2028-02-28", "2028-02-29", "2028-02-28T16:00:00+00:00", "2028-02-28T16:05:00+00:00"),
    ("daily", "2028-02-29", "2028-02-29", "2028-03-01", "2028-02-29T16:00:00+00:00", "2028-02-29T16:05:00+00:00"),
    ("tenday", "2028-02-21", "2028-02-21", "2028-03-01", "2028-02-29T16:00:00+00:00", "2028-02-29T16:05:00+00:00"),
    ("tenday", "2028-02-29", "2028-02-21", "2028-03-01", "2028-02-29T16:00:00+00:00", "2028-02-29T16:05:00+00:00"),
    ("monthly", "2028-02-01", "2028-02-01", "2028-03-01", "2028-02-29T16:00:00+00:00", "2028-02-29T16:05:00+00:00"),
]

# 每個月的第一天與次月第一天（人工推算；日數 30 / 31 / 28 / 29 由這兩個日期決定）
AC1_MONTHS = [("2026-11-01", "2026-12-01"), ("2026-12-01", "2027-01-01"), ("2027-01-01", "2027-02-01"),
              ("2027-02-01", "2027-03-01"), ("2028-02-01", "2028-03-01")]


def test_ac1_periods_match_the_hand_computed_table():
    assert config.A_CHANNEL_REPORT_SEND_DELAY_SECONDS == 300
    for kind, inside, start, end, end_utc, sched_utc in AC1_TABLE:
        p = R.period_of(kind, day(inside))
        got = (p.kind, p.start.isoformat(), p.end.isoformat(), p.end_ms, R.scheduled_ms(p))
        want = (kind, start, end, utc(end_utc), utc(sched_utc))
        assert got == want, (inside, got, want)
        assert p.start_ms == tpe(start + "T00:00"), (inside, p.start_ms)
        # end − 1 毫秒仍屬這一期的最後一天
        assert R.taipei_date(p.end_ms - 1) == day(end) - timedelta(days=1)
        assert R.taipei_date(p.end_ms) == day(end)


def test_ac1_every_day_of_the_four_months_lands_in_the_right_periods():
    tenday_bounds = [(1, 10, 1, 11), (11, 20, 11, 21), (21, 31, 21, None)]     # (第幾日起, 到第幾日, start 日, end 日)
    for first, next_first in AC1_MONTHS:
        d, stop = day(first), day(next_first)
        seen = 0
        while d < stop:
            seen += 1
            daily = R.period_of("daily", d)
            assert (daily.start, daily.end) == (d, d + timedelta(days=1)), d
            lo, hi, s_day, e_day = next(b for b in tenday_bounds if b[0] <= d.day <= b[1])
            ten = R.period_of("tenday", d)
            want_end = stop if e_day is None else d.replace(day=e_day)
            assert (ten.start, ten.end) == (d.replace(day=s_day), want_end), (d, ten)
            month = R.period_of("monthly", d)
            assert (month.start, month.end) == (day(first), stop), (d, month)
            d += timedelta(days=1)
        assert daily.end == stop and seen == (stop - day(first)).days
    # 每個月的最後一天（人工推算）：2026-11 有 30 天、2027-02 有 28 天、2028-02 有 29 天
    last_days = [(day(b) - timedelta(days=1)).isoformat() for a, b in AC1_MONTHS]
    assert last_days == ["2026-11-30", "2026-12-31", "2027-01-31", "2027-02-28", "2028-02-29"], last_days


def test_ac1_taipei_midnight_boundary():
    feb28 = R.period_of("daily", day("2027-02-28"))
    mar01 = R.period_of("daily", day("2027-03-01"))
    feb = R.period_of("monthly", day("2027-02-01"))
    mar = R.period_of("monthly", day("2027-03-01"))
    opened = tpe("2027-02-28T09:00")
    last = row("#A-S4-X", "s4", opened, feb28.end_ms - 1)
    at_end = row("#B-S4-X", "s4", opened, feb28.end_ms)
    # 台北 03-01 00:30 = UTC 02-28 16:30：用 UTC 日界的話會被算進 02-28
    utc_trap = row("#C-S4-X", "s4", opened, tpe("2027-03-01T00:30"))
    still_open = row("#D-S4-X", "s4", opened)
    rows = [last, at_end, utc_trap, still_open]
    delivered = {r["signal_id"] for r in rows}

    def closed_ids(p):
        rep = R.tally(p, rows, delivered)
        return [d.signal_id for d in rep.strategies[0].details], rep.strategies[0].holding
    # end 那一刻與之後平倉的，在 02-28 期末還開著（B、C、D 三筆持倉）
    assert closed_ids(feb28) == (["#A-S4-X"], 3)
    assert closed_ids(mar01) == (["#B-S4-X", "#C-S4-X"], 1)
    assert closed_ids(feb) == (["#A-S4-X"], 3)
    assert closed_ids(mar) == (["#B-S4-X", "#C-S4-X"], 1)
    assert R.taipei_date(tpe("2027-03-01T00:30")) == day("2027-03-01")


def test_ac1_periods_ending_in_order_and_keys():
    ends = R.periods_ending_in(tpe("2027-02-27T12:00"), tpe("2027-03-01T00:00"))
    got = [(p.kind, p.start.isoformat()) for p in ends]
    assert got == [("daily", "2027-02-27"), ("daily", "2027-02-28"), ("tenday", "2027-02-21"),
                   ("monthly", "2027-02-01")], got
    # (lo, hi]：lo 本身不算、hi 本身算
    assert R.periods_ending_in(tpe("2027-03-01T00:00"), tpe("2027-03-01T00:00")) == []
    year_end = R.periods_ending_in(tpe("2026-12-31T00:00"), tpe("2027-01-01T00:00"))
    assert [(p.kind, p.start.isoformat()) for p in year_end] == [
        ("daily", "2026-12-31"), ("tenday", "2026-12-21"), ("monthly", "2026-12-01")]
    p = R.period_of("tenday", day("2026-09-29"))
    assert (R.period_key(p), R.period_label(p)) == ("report:tenday:2026-09-21", "2026-09 下旬")
    p = R.period_of("tenday", day("2026-09-01"))
    assert R.period_label(p) == "2026-09 上旬"
    p = R.period_of("tenday", day("2026-09-11"))
    assert R.period_label(p) == "2026-09 中旬"
    p = R.period_of("monthly", day("2026-09-29"))
    assert (R.period_key(p), R.period_label(p)) == ("report:monthly:2026-09-01", "2026-09")
    p = R.period_of("daily", day("2026-09-29"))
    assert (R.period_key(p), R.period_label(p)) == ("report:daily:2026-09-29", "2026-09-29")
    try:
        R.period_of("weekly", day("2026-09-29"))
    except ValueError:
        pass
    else:
        raise AssertionError("不認得的報表種類應該拋 ValueError")


# ============================== AC-2 篩選（經真實 API 寫入） ==============================
def test_ac2_filters_via_the_real_store_and_outbox():
    p = R.period_of("daily", day("2026-09-29"))
    with tempdir() as tmp:
        b = Books(tmp)
        try:
            # 計入 T(s4)：期中開倉期中平倉、期初之前開倉期中平倉、end − 1 平倉
            a = b.entry("s4", "ACE_USDT_PERP", tpe("2026-09-29T01:00"))
            b.close_pos(a, tpe("2026-09-29T03:00"), exit_outbox=OB.STATUS_DELIVERED)
            before = b.entry("s4", "BBB_USDT_PERP", tpe("2026-09-28T20:00"))
            b.close_pos(before, tpe("2026-09-29T05:00"), reason=SL, exit_price=105.0)
            mid = b.entry("s4", "DDD_USDT_PERP", tpe("2026-09-29T07:00"))
            b.close_pos(mid, tpe("2026-09-29T08:00"), exit_price=97.0)
            edge = b.entry("s4", "CCC_USDT_PERP", tpe("2026-09-29T02:00"), price=50.0)
            b.close_pos(edge, p.end_ms - 1, exit_price=49.0, exit_outbox=OB.STATUS_PENDING)
            # 不計入：進場不是 delivered、沒有 outbox 進場列、別的 user_id、進場待送但出場已送達
            excluded = []
            for sym, status in (("PEN_USDT_PERP", OB.STATUS_PENDING), ("EXP_USDT_PERP", OB.STATUS_EXPIRED),
                                ("FAI_USDT_PERP", OB.STATUS_FAILED), ("NON_USDT_PERP", None)):
                sid = b.entry("s4", sym, tpe("2026-09-29T04:00"), outbox=status)
                b.close_pos(sid, tpe("2026-09-29T06:00"))
                excluded.append(sid)
            other = b.entry("s4", "OTH_USDT_PERP", tpe("2026-09-29T04:00"), user="someone_else")
            b.close_pos(other, tpe("2026-09-29T06:00"))
            excluded.append(other)
            exit_only = b.entry("s4", "EXO_USDT_PERP", tpe("2026-09-29T04:00"), outbox=OB.STATUS_PENDING)
            b.close_pos(exit_only, tpe("2026-09-29T06:00"), exit_outbox=OB.STATUS_DELIVERED)
            excluded.append(exit_only)
            # H(s4)：期初之前開倉還開著、期中開倉還開著、end 那一刻平倉、期末之後才平倉
            h_before = b.entry("s4", "HB_USDT_PERP", tpe("2026-09-28T10:00"))
            h_mid = b.entry("s4", "HM_USDT_PERP", tpe("2026-09-29T10:00"))
            h_at_end = b.entry("s4", "HE_USDT_PERP", tpe("2026-09-29T11:00"))
            b.close_pos(h_at_end, p.end_ms)
            h_after = b.entry("s4", "HA_USDT_PERP", tpe("2026-09-29T12:00"))
            b.close_pos(h_after, tpe("2026-09-30T08:00"))
            # 不算 H：opened_ms = end、期初之前就平倉、持倉但進場沒送達、別的 user_id 的持倉
            not_h = [b.entry("s4", "HX_USDT_PERP", p.end_ms)]
            gone = b.entry("s4", "OLD_USDT_PERP", tpe("2026-09-27T10:00"))
            b.close_pos(gone, tpe("2026-09-28T23:00"))
            not_h += [gone, b.entry("s4", "HP_USDT_PERP", tpe("2026-09-29T10:00"), outbox=OB.STATUS_PENDING),
                      b.entry("s4", "HO_USDT_PERP", tpe("2026-09-29T10:00"), user="someone_else")]
            # 同一幣兩策略同時段：各自計入自己的段
            ace5 = b.entry("s5", "ACE_USDT_PERP", tpe("2026-09-29T01:00"))
            b.close_pos(ace5, tpe("2026-09-29T04:00"), reason=SL, exit_price=103.0)
            h5 = b.entry("s5", "ZZZ_USDT_PERP", tpe("2026-09-29T20:00"))

            # 寫入者的連線還開著（部分資料只在 -wal）時照樣讀得到
            text, rep = R.compose(p, live_db=b.live_path, outbox_db=b.outbox_path)
        finally:
            b.close()
        text_closed, rep_closed = R.compose(p, live_db=b.live_path, outbox_db=b.outbox_path)
    assert (text, rep) == (text_closed, rep_closed), "寫入者開著與關閉後讀到的報表不同"
    s4, s5 = rep.strategies
    assert (s4.strategy, s5.strategy) == ("s4", "s5")
    assert [d.signal_id for d in s4.details] == [a, before, mid, edge], s4.details
    assert (s4.n, s4.take_profit, s4.stop_loss, s4.holding) == (4, 3, 1, 4), s4
    want = [(100.0 - 96.0) / 100.0, (100.0 - 105.0) / 100.0, (100.0 - 97.0) / 100.0, (50.0 - 49.0) / 50.0]
    assert s4.gross == math.fsum(want) and s4.fee == 4 * 0.001 and s4.net == math.fsum(want) - 4 * 0.001
    assert [d.ret for d in s4.details] == want
    assert [d.signal_id for d in s5.details] == [ace5] and (s5.n, s5.take_profit, s5.holding) == (1, 0, 1), s5
    assert s5.gross == (100.0 - 103.0) / 100.0
    ids = set(text.split())
    assert not (set(excluded) | set(not_h) | {h_before, h_mid, h_at_end, h_after, h5}) & ids
    assert {a, before, mid, edge, ace5} <= ids


def test_ac2_non_short_side_raises_instead_of_guessing():
    p = R.period_of("daily", day("2026-09-29"))
    rows = [row("#L-S4-X", "s4", tpe("2026-09-29T01:00"), tpe("2026-09-29T03:00"), side="long")]
    for rs in (rows, [row("#L-S4-X", "s4", tpe("2026-09-29T01:00"), side="long")]):   # T 與 H 都要擋
        try:
            R.tally(p, rs, {"#L-S4-X"})
        except R.ReportError as e:
            assert "long" in str(e) and "#L-S4-X" in str(e), e
        else:
            raise AssertionError("非做空應該拋 ReportError")
    # 不在 T ∪ H 的列（別的期間、沒送達）不影響
    far = [row("#L-S4-X", "s4", tpe("2026-09-20T01:00"), tpe("2026-09-20T03:00"), side="long")]
    assert R.tally(p, far, {"#L-S4-X"}).strategies[0].n == 0
    assert R.tally(p, rows, set()).strategies[0].n == 0


def test_fr5_delivered_set_comes_from_the_outbox_method():
    """delivered 集合必須來自 Outbox.delivered_entry_signal_ids()（不另寫 SQL）：把它換掉，報表跟著變。"""
    p = R.period_of("daily", day("2026-09-29"))
    with tempdir() as tmp:
        b = Books(tmp)
        try:
            sid = b.entry("s4", "ACE_USDT_PERP", tpe("2026-09-29T01:00"), outbox=OB.STATUS_PENDING)
            b.close_pos(sid, tpe("2026-09-29T03:00"))
        finally:
            b.close()
        calls = []
        orig = OB.Outbox.delivered_entry_signal_ids

        def fake(self):
            calls.append(self.path)
            return [sid]
        OB.Outbox.delivered_entry_signal_ids = fake
        try:
            _, rep = R.compose(p, live_db=b.live_path, outbox_db=b.outbox_path)
        finally:
            OB.Outbox.delivered_entry_signal_ids = orig
        assert calls == [os.path.abspath(b.outbox_path)] and rep.strategies[0].n == 1, (calls, rep)
        _, rep = R.compose(p, live_db=b.live_path, outbox_db=b.outbox_path)
        assert rep.strategies[0].n == 0


# ============================== AC-5 文字 ==============================
def test_ac5_prd_daily_sample_verbatim():
    p = R.period_of("daily", day("2026-09-29"))
    text = R.render(R.tally(p, SAMPLE_ROWS, SAMPLE_DELIVERED))
    assert text == PRD_DAILY_SAMPLE, text


def _expected(kind, has_close, late):
    title = {"daily": CHART + " 策略績效日報  2026-09-29", "tenday": CHART + " 策略績效 10 日報  2026-09 下旬",
             "monthly": CHART + " 策略績效月報  2026-09"}[kind]
    span = {"daily": "2026-09-29 00:00～24:00", "tenday": "2026-09-21～2026-09-30",
            "monthly": "2026-09-01～2026-09-30"}[kind]
    none = "本日無平倉" if kind == "daily" else "本期無平倉"
    lines = [title, ""]
    if late:
        lines += [LATE_LINE, ""]
    lines.append("【策略4】")
    if has_close:
        lines += ["平倉：3 筆（止盈 2、止損 1）", "勝率：66.7%", "名目報酬合計：+3.0%", "估算手續費：-0.3%",
                  "淨報酬合計：+2.7%", "持倉中：1 筆（不計入）"]
        if kind == "daily":
            lines += ["明細：", CHECK + " #ACE-S4-20260929-1535  +4.0%", CHECK + " #NMR-S4-20260929-1820  +4.0%",
                      CROSS + " #QNT-S4-20260929-2105  -5.0%"]
    else:
        lines += [none, "持倉中：1 筆（不計入）"]
    lines += ["", "【策略5】", none, "持倉中：0 筆", "",
              REF + " 期間：" + span + "（台北時間），依出場時間歸日", FOOT_BASIS, FOOT_SCOPE, FOOT_DISCLAIMER]
    return "\n".join(lines)


def test_ac5_format_matrix_three_kinds_by_close_by_late():
    holding_only = [r for r in SAMPLE_ROWS if r["closed_ms"] is None]
    checked = 0
    for kind in R.KINDS:
        p = R.period_of(kind, day("2026-09-29"))
        for has_close in (True, False):
            rep = R.tally(p, SAMPLE_ROWS if has_close else holding_only, SAMPLE_DELIVERED)
            for late in (False, True):
                got = R.render(rep, late=late)
                want = _expected(kind, has_close, late)
                assert got == want, (kind, has_close, late, got)
                checked += 1
    assert checked == len(R.KINDS) * 4
    assert _expected("daily", True, False) == PRD_DAILY_SAMPLE


def test_ac5_win_rate_and_fee_rate_formatting():
    assert R.format_win_rate(3, 3) == "100.0%" and R.format_win_rate(0, 7) == "0.0%"
    assert R.format_win_rate(1, 3) == "33.3%" and R.format_win_rate(1, 16) == "6.3%"     # 6.25 → HALF_UP
    assert R.format_win_rate(1, 80) == "1.3%" and R.format_win_rate(1, 400) == "0.3%"   # 1.25、0.25 → HALF_UP
    assert R.format_fee_rate(0.0005) == "0.05%" and R.format_fee_rate(0.00045) == "0.045%"
    assert R.format_fee_rate(0.01) == "1%" and R.format_fee_rate(0.001) == "0.1%"
    assert T.format_plain_pct(0.0005) != R.format_fee_rate(0.0005), "費率顯示不可以用 format_plain_pct"
    # F = n × 2 × 費率：10 筆、0.0005 → -1.0%（只扣一次的話會是 -0.5%）
    p = R.period_of("daily", day("2026-09-29"))
    rows = [row("#F%d-S4-X" % i, "s4", tpe("2026-09-29T01:00"), tpe("2026-09-29T03:00"), exit_price=100.0)
            for i in range(10)]
    text = R.render(R.tally(p, rows, {r["signal_id"] for r in rows}))
    assert "估算手續費：-1.0%" in text.split("\n") and "淨報酬合計：-1.0%" in text.split("\n"), text


def test_ac5_fee_rate_0_00045_shows_in_the_footer_and_the_fee():
    saved = config.A_CHANNEL_REPORT_FEE_RATE
    config.A_CHANNEL_REPORT_FEE_RATE = 0.00045
    try:
        p = R.period_of("daily", day("2026-09-29"))
        rows = [row("#F%d-S4-X" % i, "s4", tpe("2026-09-29T01:00"), tpe("2026-09-29T03:00"), exit_price=100.0)
                for i in range(100)]
        rep = R.tally(p, rows, {r["signal_id"] for r in rows})
        text = R.render(rep)
    finally:
        config.A_CHANNEL_REPORT_FEE_RATE = saved
    lines = text.split("\n")
    assert REF + " 名目報酬以訊號價與理論出場價計；手續費以單邊 0.045% 估算，未計資金費率" in lines, lines[-4:]
    assert "估算手續費：-9.0%" in lines, lines            # 100 × 2 × 0.00045 = 0.09
    assert rep.strategies[0].fee == 100 * 0.0009


def _detail_map(text):
    out = {}
    for line in text.split("\n"):
        if line.startswith((CHECK + " ", CROSS + " ")):
            sid, pct = line[1:].strip().split("  ")
            out[sid] = (line[:1], pct)
    return out


def _exit_pct(ev):
    text = T.format_exit(ev, {"label": config.STRATEGY_LABELS[ev.strategy], "price_decimals": 8})
    hits = [ln for ln in text.split("\n") if ln.startswith("名目報酬" + T.LABEL_SEPARATOR)]
    assert len(hits) == 1, text
    return hits[0][len("名目報酬" + T.LABEL_SEPARATOR):]


def test_ac5_detail_percent_matches_format_exit_for_3000_random_trades():
    rng = random.Random(20260929)
    p = R.period_of("daily", day("2026-09-29"))
    events = []
    # 捨入邊界：r = 0.05%、0.15%、……（±x.x5%）
    for k in range(40):
        for entry in (1.0, 3.7, 100.0, 0.001234, 45678.9):
            for sign in (1, -1):
                events.append((entry, entry * (1 - sign * (k * 10 + 5) / 10000)))
    for entry in (1.0, 0.1, 7.3):
        events.append((entry, entry))                        # 報酬 0
    while len(events) < 3100:
        entry = 10 ** rng.uniform(-6, 5)
        events.append((entry, entry * (1 - rng.uniform(-0.3, 0.3))))
    rows, evs = [], []
    opened = tpe("2026-09-29T00:10")
    for i, (entry, exit_price) in enumerate(events):
        strategy = config.STRATEGIES[i % len(config.STRATEGIES)]
        sid = "#R%05d-%s-20260929-0010" % (i, SPECS[strategy].tag)
        reason = TP if exit_price < entry else SL
        closed = opened + (i % 1000) * MIN
        rows.append(row(sid, strategy, opened, closed, reason=reason, entry=entry, exit_price=exit_price))
        evs.append(ExitEvent(strategy=strategy, signal_id=sid, symbol="R%05d_USDT_PERP" % i,
                             direction=config.DIRECTION_SHORT, created_ms=closed + SEC, reason=reason,
                             exit_price=exit_price, entry_price=entry, opened_ms=opened, closed_ms=closed,
                             features={}))
    rep = R.tally(p, rows, {r["signal_id"] for r in rows})
    got = _detail_map(R.render(rep, max_chars=10 ** 7))
    assert len(got) == len(evs) >= 3000, len(got)
    for ev in evs:
        mark, pct = got[ev.signal_id]
        assert pct == _exit_pct(ev), (ev.signal_id, ev.entry_price, ev.exit_price, pct, _exit_pct(ev))
        assert mark == (CHECK if ev.reason == TP else CROSS)
    # 合計用未四捨五入的值（fsum），與逐筆四捨五入後相加不同
    for st in rep.strategies:
        mine = [ev for ev in evs if ev.strategy == st.strategy]
        assert st.gross == math.fsum((ev.entry_price - ev.exit_price) / ev.entry_price for ev in mine)


def test_ac5_details_through_the_real_store_keep_the_same_percent():
    """價格經 SQLite 存取後原樣讀回：明細百分比與出場訊息（同一組 float）逐字相同。"""
    rng = random.Random(117)
    p = R.period_of("daily", day("2026-09-29"))
    with tempdir() as tmp:
        b = Books(tmp)
        evs = []
        try:
            for i in range(120):
                entry = 10 ** rng.uniform(-6, 5)
                exit_price = entry * (1 - rng.uniform(-0.3, 0.3))
                opened = tpe("2026-09-29T00:05") + i * MIN
                sid = b.entry("s4", "Q%03d_USDT_PERP" % i, opened, price=entry)
                pos = b.close_pos(sid, opened + HOUR, reason=TP if exit_price < entry else SL, exit_price=exit_price)
                evs.append(ExitEvent(strategy="s4", signal_id=sid, symbol=pos["symbol"],
                                     direction=config.DIRECTION_SHORT, created_ms=opened + HOUR + SEC,
                                     reason=pos["exit_reason"], exit_price=exit_price, entry_price=entry,
                                     opened_ms=opened, closed_ms=opened + HOUR, features={}))
        finally:
            b.close()
        rows, delivered = R.read_period(p, live_db=b.live_path, outbox_db=b.outbox_path)
    got = _detail_map(R.render(R.tally(p, rows, delivered), max_chars=10 ** 7))
    assert len(got) == len(evs)
    for ev in evs:
        assert got[ev.signal_id][1] == _exit_pct(ev), ev.signal_id


def _many(strategy, n, start_iso="2026-09-29T00:01"):
    opened = tpe(start_iso)
    return [row("#LONGCOINNAME%04d-%s-20260929-0001" % (i, SPECS[strategy].tag), strategy, opened,
                opened + (i + 1) * SEC, reason=TP if i % 3 else SL, exit_price=96.0 if i % 3 else 105.0)
            for i in range(n)]


def _block_lines(text, label):
    lines = text.split("\n")
    i = lines.index("【%s】" % label)
    j = lines.index("", i)
    return lines[i:j]


def test_ac5_long_daily_report_is_truncated_from_the_end_of_the_details():
    p = R.period_of("daily", day("2026-09-29"))
    limit = config.TG_MAX_MESSAGE_CHARS
    assert limit == 4096
    small = [r for r in SAMPLE_ROWS]
    big = _many("s5", 400)
    rows = small + big
    rep = R.tally(p, rows, {r["signal_id"] for r in rows})
    full = R.render(rep, max_chars=10 ** 7)
    assert utf16_units(full) > limit
    text = R.render(rep)
    assert utf16_units(text) <= limit, utf16_units(text)
    s4_block, s5_block = _block_lines(text, "策略4"), _block_lines(text, "策略5")
    assert s4_block == _block_lines(full, "策略4"), "前面沒超過的策略段不可以被刪"
    kept = [ln for ln in s5_block if ln[:1] in (CHECK, CROSS)]
    n_kept = len(kept)
    assert 0 < n_kept < 400
    assert s5_block[-1] == ELLIPSIS + "另有 %d 筆未列出（見各則出場訊息）" % (400 - n_kept), s5_block[-1]
    full_s5 = [ln for ln in _block_lines(full, "策略5") if ln[:1] in (CHECK, CROSS)]
    assert kept == full_s5[:n_kept], "留下來的必須是依 (closed_ms, signal_id) 排序的前段"
    # 統計行與註腳不刪
    stats = [ln for ln in _block_lines(full, "策略5") if ln[:1] not in (CHECK, CROSS)]
    assert [ln for ln in s5_block if ln[:1] not in (CHECK, CROSS) and not ln.startswith(ELLIPSIS)] == stats
    assert text.split("\n")[-4:] == full.split("\n")[-4:]
    # 放得下就不動
    assert R.render(R.tally(p, SAMPLE_ROWS, SAMPLE_DELIVERED)) == PRD_DAILY_SAMPLE


def test_ac5_truncation_when_both_strategies_overflow():
    p = R.period_of("daily", day("2026-09-29"))
    rows = _many("s4", 300) + _many("s5", 300)
    rep = R.tally(p, rows, {r["signal_id"] for r in rows})
    text = R.render(rep)
    assert utf16_units(text) <= config.TG_MAX_MESSAGE_CHARS
    s4_block, s5_block = _block_lines(text, "策略4"), _block_lines(text, "策略5")
    assert s5_block[-1] == ELLIPSIS + "另有 300 筆未列出（見各則出場訊息）", "最後一段先刪光"
    assert s5_block[s5_block.index("明細：") + 1] == s5_block[-1], "策略5 的明細全刪，只剩「另有 N 筆」"
    k4 = len([ln for ln in s4_block if ln[:1] in (CHECK, CROSS)])
    assert 0 < k4 < 300 and s4_block[-1] == ELLIPSIS + "另有 %d 筆未列出（見各則出場訊息）" % (300 - k4)
    # 刪光明細還放不下 → 拋例外（不送出被截斷的統計）
    try:
        R.render(rep, max_chars=100)
    except R.ReportError:
        pass
    else:
        raise AssertionError("連統計行都放不下時應該拋 ReportError")


# 每一行只能是這些樣式之一（逐行白名單；不含 features、稽核旗標、策略參數名稱）
_ALLOWED_LINES = [
    "^" + CHART + " 策略績效(日報|月報| 10 日報)  [0-9]{4}-[0-9]{2}(-[0-9]{2}| [上中下]旬)?$",
    "^$",
    "^" + re.escape(LATE_LINE) + "$",
    "^【策略[45]】$",
    "^平倉：[0-9]+ 筆（止盈 [0-9]+、止損 [0-9]+）$",
    "^勝率：[0-9]+[.][0-9]%$",
    "^(名目報酬合計|估算手續費|淨報酬合計)：[-+]?[0-9]+[.][0-9]%$",
    "^持倉中：([0-9]+ 筆（不計入）|0 筆)$",
    "^明細：$",
    "^[" + CHECK + CROSS + "] #[A-Z0-9]+-S[45]-[0-9]{8}-[0-9]{4}  [-+]?[0-9]+[.][0-9]%$",
    "^" + ELLIPSIS + "另有 [0-9]+ 筆未列出（見各則出場訊息）$",
    "^本[日期]無平倉$",
    "^" + REF + " 期間：[0-9]{4}-[0-9]{2}-[0-9]{2}( 00:00～24:00|～[0-9]{4}-[0-9]{2}-[0-9]{2})（台北時間），依出場時間歸日$",
    "^" + re.escape(FOOT_BASIS) + "$",
    "^" + re.escape(FOOT_SCOPE) + "$",
    "^" + re.escape(FOOT_DISCLAIMER) + "$",
]


def _param_names():
    names = set()
    for mod in (s4_signal, s5_signal):
        for d in (mod.DEFAULT_PARAMS, mod.exit_params()):
            names |= set(d)
    return names


def test_ac5_every_line_is_whitelisted_and_nothing_leaks():
    with tempdir() as tmp:
        b = Books(tmp)
        try:
            for i, sym in enumerate(("ACE_USDT_PERP", "NMR_USDT_PERP", "QNT_USDT_PERP")):
                sid = b.entry("s4", sym, tpe("2026-09-29T10:00") + i * HOUR)
                b.close_pos(sid, tpe("2026-09-29T20:00") + i * MIN, reason=SL if i else TP,
                            exit_price=105.0 if i else 96.0)
            b.entry("s5", "ACE_USDT_PERP", tpe("2026-09-29T10:00"))
        finally:
            b.close()
        texts = []
        for kind in R.KINDS:
            for late in (False, True):
                texts.append(R.compose(R.period_of(kind, day("2026-09-29")), live_db=b.live_path,
                                       outbox_db=b.outbox_path, late=late)[0])
    rows = _many("s5", 400)
    texts.append(R.render(R.tally(R.period_of("daily", day("2026-09-29")), rows, {r["signal_id"] for r in rows})))
    patterns = [re.compile(p) for p in _ALLOWED_LINES]
    banned_words = {LEAK_FEATURE_KEY, str(LEAK_FEATURE_VALUE), LEAK_FLAG, "gap_ok", "recovered", "features",
                    "True", "False"} | _param_names()
    for text in texts:
        for line in text.split("\n"):
            assert any(p.match(line) for p in patterns), "不在白名單的行：%r" % line
        for w in banned_words:
            assert w not in text, (w, text)
    assert not find_leaks(texts)
    # 反向對照：白名單抓得到夾帶
    assert not any(re.compile(p).match("策略4 TAKE_PROFIT=0.1") for p in _ALLOWED_LINES)
    assert not any(re.compile(p).match(CHECK + " #ACE-S4-20260929-1535  +4.0% minor_resolved") for p in _ALLOWED_LINES)


# ============================== FR-5 唯讀讀取 ==============================
def _fake_db(path, version):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE x (a)")
    c.execute("PRAGMA user_version = %d" % version)
    c.commit()
    c.close()


def test_fr5_missing_file_and_version_mismatch_raise_without_creating_anything():
    p = R.period_of("daily", day("2026-09-29"))
    with tempdir() as tmp:
        b = Books(tmp)
        b.close()
        missing_dir = os.path.join(tmp, "nowhere", "sub")
        for live_db, outbox_db, needle in (
                (os.path.join(missing_dir, "live.sqlite3"), b.outbox_path, "live.sqlite3"),
                (b.live_path, os.path.join(missing_dir, "outbox.sqlite3"), "outbox")):
            try:
                R.compose(p, live_db=live_db, outbox_db=outbox_db)
            except R.ReportError as e:
                assert needle in str(e) and "不存在" in str(e) and os.path.abspath(
                    live_db if needle == "live.sqlite3" else outbox_db) in str(e), e
            else:
                raise AssertionError("檔案不存在應該拋 ReportError")
            assert not os.path.exists(os.path.join(tmp, "nowhere")), "不可以建目錄"
        bad = os.path.join(tmp, "bad")
        os.makedirs(bad)
        for name, which in (("live.sqlite3", "live"), ("outbox.sqlite3", "outbox")):
            path = os.path.join(bad, name)
            _fake_db(path, store.SCHEMA_VERSION + OB.SCHEMA_VERSION + 7)
            before = listing(bad)
            kw = {"live_db": b.live_path, "outbox_db": b.outbox_path}
            kw[which + "_db"] = path
            try:
                R.compose(p, **kw)
            except R.ReportError as e:
                assert "schema 版本" in str(e) and path in str(e), e
            else:
                raise AssertionError("版本不符應該拋 ReportError")
            assert listing(bad) == before, "版本不符的檔不可以被改"
            os.remove(path)


def test_fr5_readonly_reads_wal_only_rows_and_leaves_main_and_wal_untouched():
    """AC-6 的 RD 版本（精簡）：① 正常關閉、② 寫入者開著且部分資料只在 -wal、③ 只有主檔 + -wal。"""
    p = R.period_of("daily", day("2026-09-29"))
    with tempdir(prefix="r_a wal #%& 測試 ") as tmp:
        b = Books(tmp)
        try:
            for i in range(3):
                sid = b.entry("s4", "M%d_USDT_PERP" % i, tpe("2026-09-29T01:00") + i * MIN)
                b.close_pos(sid, tpe("2026-09-29T03:00"))
            for path in (b.live_path, b.outbox_path):          # 前三筆 checkpoint 進主檔
                c = sqlite3.connect(path)
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                c.close()
            for i in range(4):
                sid = b.entry("s4", "W%d_USDT_PERP" % i, tpe("2026-09-29T05:00") + i * MIN)
                b.close_pos(sid, tpe("2026-09-29T06:00"))
            db_dir = os.path.dirname(b.live_path)
            # 鑑別：只拿主檔去讀，會少掉只在 -wal 的四筆
            main_only = os.path.join(tmp, "main_only")
            os.makedirs(main_only)
            for path in (b.live_path, b.outbox_path):
                shutil.copy2(path, os.path.join(main_only, os.path.basename(path)))
            _, rep_main = R.compose(p, live_db=os.path.join(main_only, "live.sqlite3"),
                                    outbox_db=os.path.join(main_only, "a_channel_outbox.sqlite3"))
            assert rep_main.strategies[0].n == 3, rep_main
            # ② 寫入者開著
            before = listing(db_dir)
            _, rep2 = R.compose(p, live_db=b.live_path, outbox_db=b.outbox_path)
            assert rep2.strategies[0].n == 7, rep2
            after = listing(db_dir)
            for n in before:
                if not n.endswith("-shm"):
                    assert after[n] == before[n], "②：%s 被改動" % n
            assert set(after) == set(before)
            # 讀取期間寫入者照寫
            ro = R.open_readonly(b.live_path, "live.sqlite3", store.SCHEMA_VERSION)
            try:
                ro.execute("BEGIN")
                ro.execute("SELECT count(*) FROM positions").fetchone()
                sid = b.entry("s4", "DUR_USDT_PERP", tpe("2026-09-29T07:00"))
                b.close_pos(sid, tpe("2026-09-29T08:00"))
                ro.execute("COMMIT")
            finally:
                ro.close()
            # ③ 只有主檔 + -wal
            no_shm = os.path.join(tmp, "no_shm")
            os.makedirs(no_shm)
            for n in os.listdir(db_dir):
                if not n.endswith("-shm"):
                    shutil.copy2(os.path.join(db_dir, n), os.path.join(no_shm, n))
            before3 = listing(no_shm)
            try:
                _, rep3 = R.compose(p, live_db=os.path.join(no_shm, "live.sqlite3"),
                                    outbox_db=os.path.join(no_shm, "a_channel_outbox.sqlite3"))
                assert rep3.strategies[0].n == 8, "③：不可以產生漏掉 -wal 資料的報表（%r）" % (rep3,)
            except R.ReportError:
                pass                                                # 清楚報錯也可以接受
            after3 = listing(no_shm)
            for n in before3:
                assert after3[n] == before3[n], "③：%s 被改動" % n
            assert all(n.endswith("-shm") for n in set(after3) - set(before3)), after3
        finally:
            b.close()
        # ① 正常關閉（只剩主檔）
        db_dir = os.path.dirname(b.live_path)
        assert sorted(os.listdir(db_dir)) == ["a_channel_outbox.sqlite3", "live.sqlite3"], os.listdir(db_dir)
        before1 = listing(db_dir)
        _, rep1 = R.compose(p, live_db=b.live_path, outbox_db=b.outbox_path)
        assert rep1.strategies[0].n == 8, rep1
        after1 = listing(db_dir)
        for n in before1:
            assert after1[n] == before1[n], "①：%s 被改動" % n
        new = set(after1) - set(before1)
        # SQLite 以唯讀開 WAL 資料庫時會建 -shm 與空的 -wal（實測結果寫在 code summary）；只允許這兩種，-wal 必須是空的
        assert all(n.endswith(("-shm", "-wal")) for n in new), new
        assert all(os.path.getsize(os.path.join(db_dir, n)) == 0 for n in new if n.endswith("-wal"))


# ============================== FR-6 離線 CLI ==============================
def _cli(args, *, utf8=False, env_extra=None):
    env = tgt._child_env(**{config.TG_BOT_TOKEN_ENV: FAKE_TOKEN, config.TG_CHANNEL_ID_ENV: FAKE_CHAT})
    if utf8:
        env["PYTHONIOENCODING"] = "utf-8"
    env.update(env_extra or {})
    return subprocess.run([sys.executable, "-m", "live.a_channel_report"] + list(args), cwd=REPO_ROOT, env=env,
                          stdin=subprocess.DEVNULL, capture_output=True, timeout=120)


def _sample_books(tmp):
    """PRD 樣本那一天的資料（經真實 API），連線關好（狀態 ①）。"""
    b = Books(tmp)
    try:
        for sym, hm, reason, exit_price in (("ACE", "15:35", TP, 96.0), ("NMR", "18:20", TP, 96.0),
                                            ("QNT", "21:05", SL, 105.0)):
            opened = tpe("2026-09-29T" + hm)
            sid = b.entry("s4", sym + "_USDT_PERP", opened)
            b.close_pos(sid, opened + HOUR, reason=reason, exit_price=exit_price)
        b.entry("s4", "XYZ_USDT_PERP", tpe("2026-09-29T10:00"))
        old = b.entry("s5", "OLD_USDT_PERP", tpe("2026-01-05T10:00"))
        b.close_pos(old, tpe("2026-01-05T11:00"))
    finally:
        b.close()
    return b


def test_fr6_cli_stdout_out_file_and_exit_codes():
    with tempdir() as tmp:
        b = _sample_books(tmp)
        db_dir = os.path.dirname(b.live_path)
        mains = {n: sha(os.path.join(db_dir, n)) for n in os.listdir(db_dir)}
        base = ["--live-db", b.live_path, "--outbox-db", b.outbox_path]
        # 主控台預設編碼（cp950 / cp1252）：stdout 以 UTF-8 寫 binary buffer，emoji 不當掉
        r = _cli(["--type", "daily", "--date", "2026-09-29"] + base)
        assert r.returncode == 0, r.stderr[-800:]
        assert r.stdout.decode("utf-8") == PRD_DAILY_SAMPLE + "\n"
        # 已經結束的期間：stderr 沒有東西
        r = _cli(["--type", "daily", "--date", "2026-01-05"] + base, utf8=True)
        assert r.returncode == 0 and r.stderr == b"", r.stderr
        out = r.stdout.decode("utf-8")
        assert out.startswith(CHART + " 策略績效日報  2026-01-05") and "平倉：1 筆（止盈 1、止損 0）" in out, out
        assert LATE_LINE not in out
        # 還沒結束的期間：照樣產生，另外在 stderr 說明
        r = _cli(["--type", "monthly", "--date", "2099-01-05"] + base, utf8=True)
        assert r.returncode == 0 and "還沒結束" in r.stderr.decode("utf-8"), r.stderr
        assert r.stdout.decode("utf-8").startswith(CHART + " 策略績效月報  2099-01")
        # --out：寫 UTF-8 檔，stdout 沒有東西
        out_path = os.path.join(tmp, "out", "report.txt")
        os.makedirs(os.path.dirname(out_path))
        r = _cli(["--type", "daily", "--date", "2026-09-29", "--out", out_path] + base)
        assert r.returncode == 0 and r.stdout == b"", (r.returncode, r.stdout)
        with open(out_path, "rb") as f:
            assert f.read() == (PRD_DAILY_SAMPLE + "\n").encode("utf-8")
        tenday = _cli(["--type", "tenday", "--date", "2026-09-29"] + base).stdout.decode("utf-8")
        assert tenday == _expected("tenday", True, False) + "\n", tenday
        # exit 1：檔案不存在、版本不符、--out 寫不出去
        missing = os.path.join(tmp, "nope", "live.sqlite3")
        r = _cli(["--type", "daily", "--date", "2026-09-29", "--live-db", missing, "--outbox-db", b.outbox_path],
                 utf8=True)
        err = r.stderr.decode("utf-8")
        assert r.returncode == 1 and "live.sqlite3" in err and "不存在" in err and r.stdout == b"", (r.returncode, err)
        assert not os.path.exists(os.path.dirname(missing))
        bad = os.path.join(tmp, "bad_outbox.sqlite3")
        _fake_db(bad, OB.SCHEMA_VERSION + 9)
        r = _cli(["--type", "daily", "--date", "2026-09-29", "--live-db", b.live_path, "--outbox-db", bad], utf8=True)
        assert r.returncode == 1 and "schema 版本" in r.stderr.decode("utf-8"), r.stderr
        r = _cli(["--type", "daily", "--date", "2026-09-29", "--out", os.path.join(tmp, "no_dir", "x.txt")] + base)
        assert r.returncode == 1 and not os.path.exists(os.path.join(tmp, "no_dir"))
        # exit 2：參數錯誤（argparse 的 usage error）
        for args in (["--type", "weekly", "--date", "2026-09-29"], ["--type", "daily", "--date", "2026-13-01"],
                     ["--type", "daily"], ["--date", "2026-09-29"]):
            r = _cli(args + base, utf8=True)
            err = r.stderr.decode("utf-8")
            assert r.returncode == 2 and r.stdout == b"", (args, r.returncode, err[-400:])
            assert err.startswith("usage: python -m live.a_channel_report") and "error:" in err, err
        # 不寫資料庫：主檔逐位元組不變；唯讀開 WAL 資料庫只可能多出 -shm 與空的 -wal
        after = {n: sha(os.path.join(db_dir, n)) for n in os.listdir(db_dir)}
        assert all(after[n] == mains[n] for n in mains), "CLI 改動了資料庫"
        assert all(n.endswith(("-shm", "-wal")) for n in set(after) - set(mains)), after


def test_fr6_cli_output_has_no_secret_fragments():
    with tempdir() as tmp:
        b = _sample_books(tmp)
        texts = []
        for args in (["--type", "daily", "--date", "2026-09-29"], ["--type", "daily", "--date", "2099-01-01"],
                     ["--type", "weekly", "--date", "2026-09-29"]):
            r = _cli(args + ["--live-db", b.live_path, "--outbox-db", b.outbox_path], utf8=True)
            texts += [r.stdout.decode("utf-8", "replace"), r.stderr.decode("utf-8", "replace")]
        r = _cli(["--type", "daily", "--date", "2026-09-29", "--live-db", os.path.join(tmp, "x.sqlite3"),
                  "--outbox-db", b.outbox_path], utf8=True)
        texts += [r.stdout.decode("utf-8", "replace"), r.stderr.decode("utf-8", "replace")]
    assert not find_leaks(texts), find_leaks(texts)


class _EnvSpy(dict):
    """os.environ 的替身：記下被讀的 CRYPTO_TRADER_* 名稱。"""

    def __init__(self, base, seen):
        super().__init__(base)
        self.seen = seen

    def _note(self, key):
        if str(key).startswith("CRYPTO_TRADER"):
            self.seen.append(key)

    def __getitem__(self, key):
        self._note(key)
        return super().__getitem__(key)

    def get(self, key, default=None):
        self._note(key)
        return super().get(key, default)

    def __contains__(self, key):
        self._note(key)
        return super().__contains__(key)


def test_fr6_cli_in_process_reads_no_secrets_and_modules_never_mention_them():
    with tempdir() as tmp:
        b = _sample_books(tmp)
        out_path = os.path.join(tmp, "r.txt")
        seen = []
        orig_require, orig_is_set, orig_environ = config.require_secret, config.secret_is_set, os.environ
        config.require_secret = lambda name: seen.append(name)
        config.secret_is_set = lambda name: seen.append(name)
        os.environ = _EnvSpy(orig_environ, seen)
        try:
            with contextlib.redirect_stderr(io.StringIO()):       # 期間未結束時的說明不印到 runner 的輸出
                rc = R.main(["--type", "daily", "--date", "2026-09-29", "--live-db", b.live_path,
                             "--outbox-db", b.outbox_path, "--out", out_path])
        finally:
            config.require_secret, config.secret_is_set, os.environ = orig_require, orig_is_set, orig_environ
        assert rc == 0 and seen == [], (rc, seen)
        with open(out_path, "rb") as f:
            assert f.read().decode("utf-8") == PRD_DAILY_SAMPLE + "\n"
    # 程式碼（不含 docstring；docstring 可以說明「不讀 CRYPTO_TRADER_*」）不碰密鑰與環境變數
    words = ("CRYPTO_TRADER", "require_secret", "secret_is_set", "environ", "getenv", "TG_BOT_TOKEN",
             "TG_CHANNEL_ID")
    for name in ("a_channel_report", "a_channel_report_push"):
        tree = ast.parse(open(os.path.join(REPO_ROOT, "live", name + ".py"), encoding="utf-8").read())
        docstrings = {id(n.body[0].value) for n in ast.walk(tree)
                      if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                      and n.body and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant)}
        found = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                found.append(node.id)
            elif isinstance(node, ast.Attribute):
                found.append(node.attr)
            elif isinstance(node, ast.alias):
                found.append(node.name)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
                found.append(node.value)
        hits = [(w, f) for f in found for w in words if w in f]
        assert not hits, (name, hits)
        assert len(found) > len(docstrings), name
    probe = ast.parse("x = os.environ.get('CRYPTO_TRADER_X')")
    assert any(isinstance(n, ast.Attribute) and n.attr == "environ" for n in ast.walk(probe))


# ============================== NFR-2 字面值、模組命名與相依 ==============================
NEW_FILES = ("live/a_channel_report.py", "live/a_channel_report_push.py", "tests/test_a_channel_report.py",
             "tests/test_a_channel_report_push.py")


def _strategy_values():
    """比照 tests/test_notional_tracker.py：由 strategy/ 推導（不寫死）。"""
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


# 0 / 1 / 1000 是結構性的（計數起點、+1、毫秒換算），比照 test_notional_tracker 放行
_STRUCTURAL = {0.0, 1.0, 1000.0}


def _strategy_literal_hits(source):
    bad = _strategy_values() - _STRUCTURAL
    return [(line, text) for line, text, v in _numeric_literals(source) if v in bad]


# 數值 2 的行級白名單（PRD v4 NFR-2，captain 裁決 1、2）。禁用值裡的 2 來自 MIN_VOL_RATIO / MIN_VOL_MULT，下面各行
# 只是數值相同：手續費的進出兩次、CLI 參數錯誤的 exit code、PRAGMA synchronous 讀回 FULL、次數的期望值。
# (檔名, strip 後的整行)。「值為 2 的行」必須以多重集合完全等於白名單（collections.Counter；內容相同的兩行就列兩次，
# 不用行號），比照 tests/test_signal_feed.py 對 0.02 的等式斷言；白名單只放行 2，同一行若有其他禁用值照樣擋。
# 目標值比照 test_signal_feed 的 float("0.02") 寫法：本檔也在掃描範圍內。
_TWO = float("2")
_TWO_WHITELIST = (
    ("live/a_channel_report.py", "fee = n * 2 * rate"),
    ("tests/test_a_channel_report.py",
     'assert r.returncode == 2 and r.stdout == b"", (args, r.returncode, err[-400:])'),
    ("tests/test_a_channel_report_push.py",
     'assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2, "synchronous 要是 FULL"'),
    # 次數的期望值（attempts / delivered / handoffs，captain 裁決 2）
    ("tests/test_a_channel_report_push.py",
     'assert rows[0]["attempts"] == 2 and rows[0]["status"] == RP.STATUS_DELIVERED'),
    ("tests/test_a_channel_report_push.py",
     'assert rows[0]["status"] == RP.STATUS_DELIVERED and rows[0]["attempts"] == 2'),
    ("tests/test_a_channel_report_push.py",
     'assert stats["retries"] == 1 and stats["delivered"] == 2 and stats["failed"] == 0, stats'),
    ("tests/test_a_channel_report_push.py",
     'assert stats["retries"] == 1 and stats["handoffs"] == 2, stats'),
    ("tests/test_a_channel_report_push.py",
     'wait_until(lambda: rep.stats()["delivered"] == 2, "報表執行緒送出兩則")'),
    # 下面這行在 AC-8 與 AC-10 兩個 --push-tg 測試裡內容相同，所以列兩次
    ("tests/test_a_channel_report_push.py",
     'feed_factory=_a1(order, until=lambda: box and box[0].stats()["delivered"] >= 2),'),
    ("tests/test_a_channel_report_push.py",
     'feed_factory=_a1(order, until=lambda: box and box[0].stats()["delivered"] >= 2),'),
)


def _new_file_sources():
    out = {}
    for rel in NEW_FILES:
        with open(os.path.join(REPO_ROOT, rel), encoding="utf-8") as f:
            out[rel] = f.read()
    return out


def _nfr2_scan(sources):
    """sources：{檔名: 原始碼}。回傳 (others, twos)：
    others  2 以外的禁用值 [(檔名, 行號, 字面值)]，白名單行也照查
    twos    值為 2 的行 [(檔名, strip 後的整行)]，一行一筆，依 (檔名, 行號) 排序"""
    bad = _strategy_values() - _STRUCTURAL
    others, twos = [], {}
    for rel in sorted(sources):
        lines = sources[rel].splitlines()
        for line, text, v in _numeric_literals(sources[rel]):
            if v == _TWO:
                twos[(rel, line)] = (rel, lines[line - 1].strip())
            elif v in bad:
                others.append((rel, line, text))
    return others, [twos[k] for k in sorted(twos)]


def _nfr2_assert(sources, whitelist=_TWO_WHITELIST):
    others, twos = _nfr2_scan(sources)
    assert not others, "有策略參數的數值字面值：%r" % others
    got, want = collections.Counter(twos), collections.Counter(whitelist)
    extra = sorted((got - want).elements())
    missing = sorted((want - got).elements())
    assert got == want, "值為 2 的行與白名單不相等：多出 %r；白名單有但程式裡沒有 %r；掃到 %r" % (extra, missing, twos)


def _nfr2_failure(sources, whitelist=_TWO_WHITELIST):
    """反向對照用：_nfr2_assert 一定要失敗，回傳失敗訊息。"""
    try:
        _nfr2_assert(sources, whitelist)
    except AssertionError as e:
        return str(e)
    raise AssertionError("反向對照沒有讓 NFR-2 掃描失敗")


def test_nfr2_no_strategy_numbers_in_the_new_files():
    _nfr2_assert(_new_file_sources())


def test_nfr2_two_whitelist_discriminates():
    """數值 2 白名單的反向對照：四種情況都要讓 _nfr2_assert 失敗，而且是失敗在對應的那條規則上。"""
    real = _new_file_sources()
    fee_rel, fee_line = _TWO_WHITELIST[0]
    assert real[fee_rel].count(fee_line) == 1
    # ① 非白名單位置的 2（2.0 也一樣）
    for extra in ("EXTRA = 2", "EXTRA = 2.0"):
        src = dict(real)
        src[fee_rel] = real[fee_rel] + "\n" + extra + "\n"
        msg = _nfr2_failure(src)
        assert "多出 [%r]" % ((fee_rel, extra),) in msg and "白名單有但程式裡沒有 []" in msg, msg
    # ② 白名單行上另加一個禁用值。改過的行同時換進白名單，讓「值為 2 的行」仍然等於白名單，
    #    證明失敗來自「白名單只放行 2」，而不是整行字串對不上
    tp = s4_signal.exit_params()["TAKE_PROFIT"]
    bumped = "%s + %r" % (fee_line, tp)
    src = dict(real)
    src[fee_rel] = real[fee_rel].replace(fee_line, bumped)
    wl = tuple((r, bumped) if (r, t) == (fee_rel, fee_line) else (r, t) for r, t in _TWO_WHITELIST)
    others, twos = _nfr2_scan(src)
    assert collections.Counter(twos) == collections.Counter(wl) and [o[0] for o in others] == [fee_rel], (others, twos)
    msg = _nfr2_failure(src, wl)
    assert msg.startswith("有策略參數的數值字面值") and repr(repr(tp)) in msg, msg
    # ③ 白名單列了、但程式裡不存在的行
    ghost = (fee_rel, fee_line + " * ghost")
    msg = _nfr2_failure(real, _TWO_WHITELIST + (ghost,))
    assert "多出 []" in msg and "白名單有但程式裡沒有 [%r]" % (ghost,) in msg, msg
    # ④ 程式裡出現兩次的行，白名單少列一次
    (dup,) = [w for w, c in collections.Counter(_TWO_WHITELIST).items() if c > 1]
    once = list(_TWO_WHITELIST)
    once.remove(dup)
    msg = _nfr2_failure(real, tuple(once))
    assert "多出 [%r]" % (dup,) in msg and "白名單有但程式裡沒有 []" in msg, msg


def test_nfr2_literal_scan_discriminates():
    ex, ex5 = s4_signal.exit_params(), s5_signal.exit_params()
    for sample in ("TP = %r\n" % ex["TAKE_PROFIT"], "x = p * (1 + %r)\n" % ex["STOP_LOSS"],
                   "SL5 = %r\n" % ex5["STOP_LOSS"], "COOL = %d\n" % SPECS["s4"].cooldown_bars):
        assert _strategy_literal_hits(sample), "掃描器漏抓：%r" % sample
    assert not _strategy_literal_hits("a = 0\nb = x + 1\nc = ms / 1000\n")
    # 註解與字串不算（它們不是 NUMBER token）
    assert not _strategy_literal_hits("# TP = %r\ns = '%r'\n" % (ex["TAKE_PROFIT"], ex["TAKE_PROFIT"]))


def test_module_names_and_dependencies():
    for name in ("a_channel_report", "a_channel_report_push"):
        assert name not in sys.stdlib_module_names and importlib.util.find_spec(name) is None, name
        src = open(os.path.join(REPO_ROOT, "live", name + ".py"), encoding="utf-8").read()
        tops = set()
        for node in ast.walk(ast.parse(src)):
            if isinstance(node, ast.Import):
                tops |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom):
                tops.add((node.module or "").split(".")[0])
        assert tops <= set(sys.stdlib_module_names) | {"live"}, (name, tops)
        assert not any(t.startswith("pionex_") or t in ("research", "strategy") for t in tops), (name, tops)


def test_report_params_are_in_execution_params():
    params = config.execution_params()
    names = [n for n in dir(config) if n.startswith("A_CHANNEL_REPORT_")]
    assert len(names) >= 8, names
    for n in names:
        assert params[n] == getattr(config, n), n
    assert config.A_CHANNEL_REPORT_FEE_RATE == 0.0005
    assert config.A_CHANNEL_REPORT_DB_PATH == os.path.join(paths.RUNTIME_DIR, "db", "a_channel_reports.sqlite3")


# ============================== runner ==============================
def _run(tests):
    failed = 0
    for name, fn in tests:
        try:
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
    n_failed = _run(all_tests)
    runner_failed = 0
    if (socket.socket.connect, socket.create_connection, socket.getaddrinfo, time.sleep) != pristine:
        runner_failed += 1
        print("FAIL  <runner>: socket / time.sleep 沒有還原")
    if [os.path.exists(p) for p in watched] != existed:
        runner_failed += 1
        print(f"FAIL  <runner>: 測試在真正的 runtime/ 留下了東西（{paths.RUNTIME_DIR}）")
    leftover = [t.name for t in threading.enumerate()
                if t.name in ("a-channel-report", "tg-channel-sender") and t.is_alive()]
    if leftover:
        runner_failed += 1
        print(f"FAIL  <runner>: 還有執行緒活著 {leftover}")
    print(f"\n{len(all_tests) - n_failed} passed, {n_failed} failed"
          + (f", {runner_failed} runner check(s) failed" if runner_failed else ""))
    sys.exit(1 if n_failed or runner_failed else 0)
