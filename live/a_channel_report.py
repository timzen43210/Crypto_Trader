# -*- coding: utf-8 -*-
"""
live.a_channel_report — A 頻道報表（R-A）：期間、唯讀讀取、統計、訊息文字、離線 CLI
====================================================================================
日報 / 10 日報（三旬）/ 月報的內容。排程、發送紀錄、報表執行緒在 live.a_channel_report_push。

除了 CLI（main）與讀檔（open_readonly / read_period / compose）之外都是**純函式**：不讀時鐘、不打網路、
不讀密鑰（本模組與 CLI 完全不碰 CRYPTO_TRADER_*）。

──────────────────────────────────────────────────────────────────────
期間（台北時間，固定 UTC+8 = live.logsetup.TAIPEI；一律 [start, end)）
──────────────────────────────────────────────────────────────────────
  日報        D 00:00 → D+1 00:00
  10 日報     上旬 1 日 → 11 日、中旬 11 日 → 21 日、下旬 21 日 → 次月 1 日
  月報        1 日 → 次月 1 日
排定發送時刻 = end + A_CHANNEL_REPORT_SEND_DELAY_SECONDS。月底的日數由曆法（calendar.monthrange）推導，跨年也對。
同一個 end 有多則時，順序固定是 日報 → 10 日報 → 月報（KINDS 的順序）。

──────────────────────────────────────────────────────────────────────
統計對象（s = config.STRATEGIES 的每一個策略；delivered = outbox 的 delivered_entry_signal_ids()）
──────────────────────────────────────────────────────────────────────
  平倉 T(s)   positions 的 user_id = STRATEGY_USER_ID、strategy = s、status = 'closed'、start ≤ closed_ms < end、
              signal_id ∈ delivered。**依出場時間歸期**（closed_ms = 觸價那根監控 K 棒的收盤 = 出場訊息的出場時間）
  持倉 H(s)   user_id、strategy 同上、signal_id ∈ delivered、opened_ms < end，而且 status = 'open' 或 closed_ms ≥ end。
              定義成「期末那一刻還開著」，不是「組報表那一刻」：晚發、補發的報表數字與準時發的相同
只統計頻道上真的出現過進場訊息的訊號（使用者 2026-09-28 決定）：delivered 集合一律由
live.a_channel_outbox.Outbox.delivered_entry_signal_ids() 取得，本模組不另寫一份 SQL。

數字（n = |T(s)|）：
  止盈 / 止損   exit_reason = EXIT_TAKE_PROFIT 的筆數 / 其餘
  勝率          止盈 ÷ n，一位小數、ROUND_HALF_UP，一律顯示一位（100.0%、0.0%）
  每筆 r        做空：(entry_price - exit_price) / entry_price —— 寫法與 live.a_channel_text.format_exit() 的
                「名目報酬」一模一樣，所以明細的百分比與同一筆出場訊息逐字相同
  名目報酬合計   R = Σ r，用未四捨五入的值加總（math.fsum：正確捨入的和，與加總順序無關）
  估算手續費     F = n × 2 × A_CHANNEL_REPORT_FEE_RATE（與 dry run 的 2 * FEE_RATE 同一個口徑）
  淨報酬合計     R − F
只支援做空（side = DIRECTION_SHORT）；遇到其他方向拋 ReportError，不猜。

**為什麼合計是直接相加（單利）**：建議倉位的名目部位 = 本金 × 下單比例 × 目標槓桿 = 本金 × 100%
（A_CHANNEL_ORDER_PCT × A_CHANNEL_TARGET_LEVERAGE），照建議倉位跟單時，每一筆的名目報酬就是那一筆佔本金的
報酬，各筆相加就是這段期間的本金報酬。這條理由只寫在這裡，**訊息裡不寫**。

已知的設計內行為（不是 bug）：
  * 停機期間發生、重啟後才補判的出場，若補判晚於該期報表的發送時刻，**不會出現在已發出的那則報表**，但會出現在
    之後才組字的 10 日報 / 月報（兩者都依出場時間歸期）。所以某些情況下日報加總 ≠ 月報
  * 進場已送達、出場訊息還在待送的交易照樣計入（依資料庫的平倉紀錄，不看出場訊息有沒有送出）

──────────────────────────────────────────────────────────────────────
唯讀讀取（FR-5）
──────────────────────────────────────────────────────────────────────
兩個資料庫都用 SQLite URI `file:...?mode=ro`（uri=True）開：不執行任何 PRAGMA 寫入、不跑遷移、不建目錄、不建檔。
**不用 immutable=1**：immutable 會讓 SQLite 忽略 -wal，漏掉還沒 checkpoint 的資料。唯讀連線不會做 checkpoint，
也不會刪 -wal。開檔之後先比對 PRAGMA user_version（live.store.SCHEMA_VERSION / live.a_channel_outbox.SCHEMA_VERSION，
用模組常數比），不符就拋 ReportError。檔案不存在時拋 ReportError（寫明是哪個檔），不會當成空資料庫。
每次組報表開新連線、用完就關（不長時間持有讀交易）。WAL 的實測結果見 TASK-117 的 code summary。

──────────────────────────────────────────────────────────────────────
離線 CLI（FR-6）
──────────────────────────────────────────────────────────────────────
    python -m live.a_channel_report --type {daily,tenday,monthly} --date YYYY-MM-DD
                                    [--live-db PATH] [--outbox-db PATH] [--out FILE]
--date 是台北時間的日期，取包含這一天的那一期。資料庫路徑省略時用 config.LIVE_DB_PATH /
config.A_CHANNEL_OUTBOX_DB_PATH。給 --out 寫 UTF-8 檔，否則以 UTF-8 寫到 stdout 的 binary buffer（cp950 主控台
不會因為 emoji 當掉）。不讀密鑰、不連網、不寫任何資料庫。期間還沒結束也可以產生，另外在 stderr 印一行說明。
內容不含延遲註記（那是發送時的判斷）。exit code：0 成功、1 讀檔或版本錯誤、2 參數錯誤。不加進 `python -m live`。
"""

import argparse
import calendar
import collections
import math
import os
import pathlib
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

from live import a_channel_outbox, config, store
from live.a_channel_text import (CHECK_MARK, CROSS_MARK, LABEL_SEPARATOR, REFERENCE_MARK, WARNING_SIGN,
                                 format_signed_pct)
from live.logsetup import TAIPEI
from live.tg_channel import utf16_units

# ============================== 期間 ==============================
KIND_DAILY = "daily"
KIND_TENDAY = "tenday"
KIND_MONTHLY = "monthly"
KINDS = (KIND_DAILY, KIND_TENDAY, KIND_MONTHLY)     # 同一個 end 的發送順序：日報 → 10 日報 → 月報

# 10 日報三旬的起始日（第三旬到月底）與名稱
TENDAY_START_DAYS = (1, 11, 21)
TENDAY_LABELS = ("上旬", "中旬", "下旬")

_ONE_DAY = timedelta(days=1)

# start / end 是台北時間的日期（end 不含），start_ms / end_ms 是對應 00:00 的 UTC epoch 毫秒
Period = collections.namedtuple("Period", "kind start end start_ms end_ms")


def taipei_midnight_ms(day):
    """台北時間 day 00:00 的 UTC epoch 毫秒。"""
    return int(datetime(day.year, day.month, day.day, tzinfo=TAIPEI).timestamp()) * 1000


def taipei_date(ms):
    """UTC epoch 毫秒 → 台北時間的日期（整數運算取到秒，end − 1 毫秒仍屬前一天）。"""
    return datetime.fromtimestamp(int(ms) // 1000, TAIPEI).date()


def _next_month_first(day):
    first = day.replace(day=1)
    return first + timedelta(days=calendar.monthrange(first.year, first.month)[1])


def period_of(kind, day):
    """包含台北日期 day 的那一期。"""
    if kind == KIND_DAILY:
        start, end = day, day + _ONE_DAY
    elif kind == KIND_TENDAY:
        idx = max(i for i, d in enumerate(TENDAY_START_DAYS) if d <= day.day)
        start = day.replace(day=TENDAY_START_DAYS[idx])
        end = (day.replace(day=TENDAY_START_DAYS[idx + 1]) if idx + 1 < len(TENDAY_START_DAYS)
               else _next_month_first(day))
    elif kind == KIND_MONTHLY:
        start, end = day.replace(day=1), _next_month_first(day)
    else:
        raise ValueError("報表種類必須是 %s 之一，收到 %r" % ("/".join(KINDS), kind))
    return Period(kind, start, end, taipei_midnight_ms(start), taipei_midnight_ms(end))


def period_key(period):
    """發送器的關聯鍵，例如 report:daily:2026-09-29（種類 + 期間第一天）。"""
    return "report:%s:%s" % (period.kind, period.start.isoformat())


def period_label(period):
    """標題上的期間：日報 2026-09-29、10 日報 2026-09 上旬、月報 2026-09。"""
    if period.kind == KIND_DAILY:
        return period.start.isoformat()
    month = period.start.strftime("%Y-%m")
    if period.kind == KIND_TENDAY:
        return "%s %s" % (month, TENDAY_LABELS[TENDAY_START_DAYS.index(period.start.day)])
    return month


def scheduled_ms(period, delay_seconds=None):
    """排定發送時刻 = end + 發送延遲（省略時取 config.A_CHANNEL_REPORT_SEND_DELAY_SECONDS，呼叫當下讀）。"""
    delay = config.A_CHANNEL_REPORT_SEND_DELAY_SECONDS if delay_seconds is None else delay_seconds
    return period.end_ms + int(round(float(delay) * 1000))


def periods_ending_in(lo_ms, hi_ms):
    """end 落在 (lo_ms, hi_ms] 的每一期，依 (end, KINDS 的順序) 排好。每一期的 end 都是某天的台北 00:00。"""
    out = []
    day = taipei_date(lo_ms)
    while True:
        end_day = day + _ONE_DAY
        end_ms = taipei_midnight_ms(end_day)
        if end_ms > hi_ms:
            return out
        if end_ms > lo_ms:
            for kind in KINDS:
                p = period_of(kind, day)
                if p.end == end_day:
                    out.append(p)
        day = end_day


# ============================== 統計（純函式） ==============================
class ReportError(Exception):
    """報表產生不出來：檔案不存在、schema 版本不符、資料不是本模組能處理的（例如不是做空）。"""


# 一筆平倉明細 / 一個策略的統計 / 一期的報表
Detail = collections.namedtuple("Detail", "closed_ms signal_id take_profit ret")
StrategyStats = collections.namedtuple("StrategyStats",
                                       "strategy n take_profit stop_loss gross fee net holding details")
Report = collections.namedtuple("Report", "period strategies")


def short_return(entry_price, exit_price):
    """做空的名目報酬，寫法與 live.a_channel_text.format_exit() 相同。"""
    return (entry_price - exit_price) / entry_price


def tally(period, rows, delivered, *, fee_rate=None):
    """依模組 docstring 的口徑算出一期的報表（Report）。純函式。

    rows       positions 的列（dict；鍵 signal_id, user_id, strategy, status, exit_reason, entry_price,
               exit_price, opened_ms, closed_ms, side）。可以是任何超集合：T(s) / H(s) 的條件全部在這裡判斷
    delivered  進場已送達的 signal_id（任何可迭代物）
    fee_rate   省略時取 config（呼叫當下讀）
    """
    rate = config.A_CHANNEL_REPORT_FEE_RATE if fee_rate is None else fee_rate
    delivered = set(delivered)
    closed = {s: [] for s in config.STRATEGIES}
    holding = {s: 0 for s in config.STRATEGIES}
    for r in rows:
        s = r["strategy"]
        if r["user_id"] != config.STRATEGY_USER_ID or s not in closed or r["signal_id"] not in delivered:
            continue
        is_closed = r["status"] == store.STATUS_CLOSED
        in_t = is_closed and period.start_ms <= r["closed_ms"] < period.end_ms
        in_h = r["opened_ms"] < period.end_ms and (r["status"] == store.STATUS_OPEN
                                                   or (is_closed and r["closed_ms"] >= period.end_ms))
        if not (in_t or in_h):
            continue
        if r["side"] != config.DIRECTION_SHORT:
            raise ReportError("%s 的方向是 %r；報表只支援做空（%s），不猜" % (r["signal_id"], r["side"],
                                                                        config.DIRECTION_SHORT))
        if in_t:
            closed[s].append(Detail(int(r["closed_ms"]), r["signal_id"], r["exit_reason"] == config.EXIT_TAKE_PROFIT,
                                    short_return(r["entry_price"], r["exit_price"])))
        else:
            holding[s] += 1
    out = []
    for s in config.STRATEGIES:
        details = tuple(sorted(closed[s], key=lambda d: (d.closed_ms, d.signal_id)))
        n = len(details)
        tp = sum(1 for d in details if d.take_profit)
        gross = math.fsum(d.ret for d in details)
        # 進場、出場各扣一次，同 dry run 的 2 * FEE_RATE
        fee = n * 2 * rate
        out.append(StrategyStats(s, n, tp, n - tp, gross, fee, gross - fee, holding[s], details))
    return Report(period, tuple(out))


def summary(report):
    """{策略: {n, take_profit, stop_loss, gross, fee, net, holding}}（發送紀錄與日誌用）。"""
    return {st.strategy: {"n": st.n, "take_profit": st.take_profit, "stop_loss": st.stop_loss, "gross": st.gross,
                          "fee": st.fee, "net": st.net, "holding": st.holding} for st in report.strategies}


# ============================== 訊息文字（純函式） ==============================
CHART_MARK = chr(0x1F4CA)                       # 📊
TITLES = {KIND_DAILY: "策略績效日報", KIND_TENDAY: "策略績效 10 日報", KIND_MONTHLY: "策略績效月報"}
LATE_NOTE = WARNING_SIGN + " 延遲發布：本則晚於排定時間送出"
NO_CLOSE_TEXT = {KIND_DAILY: "本日無平倉", KIND_TENDAY: "本期無平倉", KIND_MONTHLY: "本期無平倉"}
DETAIL_HEADER = "明細" + LABEL_SEPARATOR
TRUNCATED_TEXT = chr(0x2026) + "另有 %d 筆未列出（見各則出場訊息）"
FOOTER_SCOPE = REFERENCE_MARK + " 只統計頻道上發出過進場訊號的交易"
FOOTER_DISCLAIMER = WARNING_SIGN + " 過去績效不代表未來表現，不構成投資建議"


def _row(label, value):
    return label + LABEL_SEPARATOR + value


def format_win_rate(take_profit, n):
    """止盈 ÷ n → 一位小數的百分比（ROUND_HALF_UP，一律顯示一位：100.0%、0.0%）。"""
    q = (Decimal(int(take_profit)) * 100 / Decimal(int(n))).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    return "%s%%" % format(q, "f")


def format_fee_rate(rate):
    """單邊費率 → 百分比，去掉多餘的零：0.0005 → 0.05%、0.00045 → 0.045%、0.01 → 1%。
    （不用 format_plain_pct：它只到一位小數，0.0005 會變成 0.1%）"""
    return format((Decimal(repr(float(rate))) * 100).normalize(), "f") + "%"


def _period_footer(period):
    if period.kind == KIND_DAILY:
        span = "%s 00:00～24:00" % period.start.isoformat()
    else:
        span = "%s～%s" % (period.start.isoformat(), (period.end - _ONE_DAY).isoformat())
    return REFERENCE_MARK + " 期間" + LABEL_SEPARATOR + span + "（台北時間），依出場時間歸日"


def _basis_footer(fee_rate):
    return (REFERENCE_MARK + " 名目報酬以訊號價與理論出場價計；手續費以單邊 %s 估算，未計資金費率"
            % format_fee_rate(fee_rate))


def _detail_line(d):
    return "%s %s  %s" % (CHECK_MARK if d.take_profit else CROSS_MARK, d.signal_id, format_signed_pct(d.ret))


def _block(st, kind, keep):
    """一個策略的段落。keep = 明細留幾行（只有日報有明細）。"""
    lines = ["【%s】" % config.STRATEGY_LABELS[st.strategy]]
    if st.n:
        lines += ["平倉" + LABEL_SEPARATOR + "%d 筆（止盈 %d、止損 %d）" % (st.n, st.take_profit, st.stop_loss),
                  _row("勝率", format_win_rate(st.take_profit, st.n)),
                  _row("名目報酬合計", format_signed_pct(st.gross)),
                  _row("估算手續費", format_signed_pct(-st.fee)),
                  _row("淨報酬合計", format_signed_pct(st.net))]
    else:
        lines.append(NO_CLOSE_TEXT[kind])
    lines.append(_row("持倉中", "%d 筆（不計入）" % st.holding if st.holding else "0 筆"))
    if kind == KIND_DAILY and st.n:
        lines.append(DETAIL_HEADER)
        lines += [_detail_line(d) for d in st.details[:keep]]
        if keep < st.n:
            lines.append(TRUNCATED_TEXT % (st.n - keep))
    return lines


def _assemble(report, keeps, late, fee_rate):
    period = report.period
    lines = ["%s %s  %s" % (CHART_MARK, TITLES[period.kind], period_label(period)), ""]
    if late:
        lines += [LATE_NOTE, ""]
    for st, keep in zip(report.strategies, keeps):
        lines += _block(st, period.kind, keep)
        lines.append("")
    lines += [_period_footer(period), _basis_footer(fee_rate), FOOTER_SCOPE, FOOTER_DISCLAIMER]
    return "\n".join(lines)


def render(report, *, late=False, fee_rate=None, max_chars=None):
    """報表 → 訊息文字（格式見 PRD FR-4）。

    late      加延遲註記（發送端判斷；CLI 一律不加）
    fee_rate  註腳顯示的單邊費率；省略時取 config.A_CHANNEL_REPORT_FEE_RATE（呼叫當下讀）
    max_chars 單則上限（UTF-16 code units）；省略時取 config.TG_MAX_MESSAGE_CHARS。超過時從明細尾端（最後一個
              策略的最後一行）往前刪，被刪過的策略在自己的明細最後補一行「…另有 N 筆未列出（見各則出場訊息）」，
              直到放得下。統計行與註腳不刪；刪光明細還放不下就拋 ReportError
    """
    rate = config.A_CHANNEL_REPORT_FEE_RATE if fee_rate is None else fee_rate
    limit = config.TG_MAX_MESSAGE_CHARS if max_chars is None else int(max_chars)
    keeps = [st.n for st in report.strategies]
    text = _assemble(report, keeps, late, rate)
    while utf16_units(text) > limit:
        excess = utf16_units(text) - limit
        # 從尾端一次刪掉「至少 excess 個 code units」的明細行（每行另含一個換行），再重組確認；
        # 補上的「另有 N 筆」那一行會讓長度略增，所以用迴圈直到真的放得下
        for i in reversed(range(len(keeps))):
            st = report.strategies[i]
            while keeps[i] > 0 and excess > 0:
                keeps[i] -= 1
                excess -= utf16_units(_detail_line(st.details[keeps[i]])) + 1
            if excess <= 0:
                break
        if excess > 0 and not any(keeps):
            text = _assemble(report, keeps, late, rate)
            if utf16_units(text) > limit:
                raise ReportError("報表連統計行與註腳都放不下單則上限 %d（%d）" % (limit, utf16_units(text)))
            return text
        text = _assemble(report, keeps, late, rate)
    return text


# ============================== 唯讀讀取 ==============================
def open_readonly(path, what, expected_version, busy_timeout=None):
    """以唯讀 URI（mode=ro）開 SQLite 檔並檢查 PRAGMA user_version。回傳 sqlite3 連線（呼叫端負責關）。
    what 是錯誤訊息裡的檔案名稱（例如「live.sqlite3」）。不存在、版本不符 → ReportError。"""
    path = os.path.abspath(path)
    if not os.path.isfile(path):
        raise ReportError("%s 不存在：%s（報表不產生，不會當成空資料庫）" % (what, path))
    kwargs = {"uri": True}
    if busy_timeout is not None:
        kwargs["timeout"] = float(busy_timeout)
    conn = sqlite3.connect(pathlib.Path(path).as_uri() + "?mode=ro", **kwargs)
    try:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version != expected_version:
            raise ReportError("%s（%s）的 schema 版本是 %d，報表只認得 %d；報表不產生"
                              % (what, path, version, expected_version))
    except BaseException:
        conn.close()
        raise
    return conn


_POSITIONS_SQL = (
    "SELECT p.signal_id, p.user_id, p.strategy, p.status, p.exit_reason, p.entry_price, p.exit_price, "
    "p.opened_ms, p.closed_ms, s.side FROM positions p JOIN signals s ON s.signal_id = p.signal_id "
    "WHERE p.user_id = ? AND p.opened_ms < ? AND (p.status = ? OR p.closed_ms >= ?)")


def read_period(period, *, live_db=None, outbox_db=None, busy_timeout=None):
    """讀一期需要的資料：(positions 的列（T ∪ H 的超集合）, delivered 的 signal_id 集合)。
    兩個資料庫都唯讀開、用完就關。路徑省略時取 config.LIVE_DB_PATH / config.A_CHANNEL_OUTBOX_DB_PATH（呼叫當下讀）。"""
    live_db = config.LIVE_DB_PATH if live_db is None else live_db
    outbox_db = config.A_CHANNEL_OUTBOX_DB_PATH if outbox_db is None else outbox_db
    conn = open_readonly(outbox_db, "outbox（a_channel_outbox.sqlite3）", a_channel_outbox.SCHEMA_VERSION,
                         busy_timeout)
    try:
        delivered = set(a_channel_outbox.Outbox(conn, os.path.abspath(outbox_db)).delivered_entry_signal_ids())
    finally:
        conn.close()
    conn = open_readonly(live_db, "live.sqlite3", store.SCHEMA_VERSION, busy_timeout)
    try:
        conn.row_factory = sqlite3.Row
        rows = [dict(r) for r in conn.execute(_POSITIONS_SQL,
                                              (config.STRATEGY_USER_ID, period.end_ms, store.STATUS_OPEN,
                                               period.start_ms))]
    finally:
        conn.close()
    return rows, delivered


def compose(period, *, live_db=None, outbox_db=None, late=False, busy_timeout=None):
    """讀檔 → 統計 → 組字。回傳 (text, report)。"""
    rows, delivered = read_period(period, live_db=live_db, outbox_db=outbox_db, busy_timeout=busy_timeout)
    report = tally(period, rows, delivered)
    return render(report, late=late), report


# ============================== 離線 CLI ==============================
EXIT_OK, EXIT_READ_ERROR = 0, 1      # 參數錯誤的 exit code 由 argparse 自己給（parser.error）


def _parse_date(text):
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("日期格式是 YYYY-MM-DD（台北時間），收到 %r" % text) from None


def _stderr_line(text):
    stream = sys.stderr
    if stream is None:
        return
    encoding = getattr(stream, "encoding", None) or "ascii"
    try:
        text = text.encode(encoding, "backslashreplace").decode(encoding, "replace")
    except LookupError:
        text = text.encode("ascii", "backslashreplace").decode("ascii")
    stream.write(text + "\n")
    stream.flush()


def main(argv=None):
    """python -m live.a_channel_report 的進入點。回傳 exit code（參數錯誤由 argparse 以 exit 2 結束）。"""
    stream = sys.stderr
    if stream is not None and hasattr(stream, "reconfigure"):
        try:
            stream.reconfigure(errors="backslashreplace")     # 中文的說明 / 錯誤訊息在 cp950 / cp1252 主控台不當掉
        except (OSError, ValueError):
            pass
    parser = argparse.ArgumentParser(
        prog="python -m live.a_channel_report",
        description="A 頻道報表（R-A）的離線產生器：唯讀讀 live.sqlite3 與 outbox，輸出一則報表的文字。"
                    "不讀密鑰、不連網、不寫任何資料庫。")
    parser.add_argument("--type", required=True, choices=KINDS, dest="kind",
                        help="daily = 日報、tenday = 10 日報（上 / 中 / 下旬）、monthly = 月報")
    parser.add_argument("--date", required=True, type=_parse_date, metavar="YYYY-MM-DD",
                        help="台北時間的日期，取包含這一天的那一期")
    parser.add_argument("--live-db", default=None, metavar="PATH",
                        help="live.sqlite3 的路徑（預設 %s）" % config.LIVE_DB_PATH)
    parser.add_argument("--outbox-db", default=None, metavar="PATH",
                        help="outbox 的路徑（預設 %s）" % config.A_CHANNEL_OUTBOX_DB_PATH)
    parser.add_argument("--out", default=None, metavar="FILE", help="寫成 UTF-8 檔；不給就寫到 stdout（UTF-8）")
    args = parser.parse_args(argv)

    period = period_of(args.kind, args.date)
    try:
        text, _ = compose(period, live_db=args.live_db, outbox_db=args.outbox_db)
    except (ReportError, sqlite3.Error) as e:
        _stderr_line("報表產生失敗：%s: %s" % (type(e).__name__, e))
        return EXIT_READ_ERROR
    now_ms = time.time_ns() // 1_000_000
    if now_ms < period.end_ms:
        _stderr_line("注意：這一期（%s %s）還沒結束，台北時間 %s 00:00 才結束；內容只到目前為止，僅供檢視"
                     % (args.kind, period_label(period), period.end.isoformat()))
    data = (text + "\n").encode("utf-8")
    try:
        if args.out:
            with open(args.out, "wb") as f:
                f.write(data)
        else:
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
    except OSError as e:
        _stderr_line("報表寫不出去：%s: %s" % (type(e).__name__, e))
        return EXIT_READ_ERROR
    return EXIT_OK


if __name__ == "__main__":
    # python -m live.a_channel_report 會把本檔載入成 __main__；從正式的模組名取 main()（同 live.a_channel 的理由）
    from live.a_channel_report import main as _main
    sys.exit(_main())
