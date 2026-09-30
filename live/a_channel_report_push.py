# -*- coding: utf-8 -*-
"""
live.a_channel_report_push — A 頻道報表（R-A）的排程、發送紀錄與報表執行緒
==========================================================================
報表的期間、統計與文字在 live.a_channel_report（純函式 + 唯讀讀檔）；這裡決定「哪一期、什麼時候、交給誰送」，
並把每一期在頻道上的最終狀態記在發送紀錄（config.A_CHANNEL_REPORT_DB_PATH）。

用法（由執行入口 live.a_channel 在 `--push-tg` 時接線：A3 wait_ready 之後、A1 之前啟動；A3 join 之後、
T2 的 push.shutdown() 之前停止 —— 報表用的是 T2 的 ChannelSender）：

    reporter = ChannelReporter(sender)
    reporter.start()            # 建發送紀錄（必要時）、記一行現況、啟動 a-channel-report 執行緒
    ...
    reporter.stop()             # 停止交出新的報表 → 記回已經回報的結果 → 結束執行緒
    reporter.stats()            # 給 A4 / 結束時的總結

本模組與 live.a_channel_report 都不讀任何 CRYPTO_TRADER_*：發送一律走既有的 ChannelSender。

──────────────────────────────────────────────────────────────────────
發送紀錄（SQLite，A_CHANNEL_REPORT_DB_PATH = runtime/db/a_channel_reports.sqlite3）
──────────────────────────────────────────────────────────────────────
獨立一個檔，不動 live.sqlite3 與 outbox。比照 outbox：WAL + synchronous=FULL、明確 BEGIN IMMEDIATE / COMMIT、
schema 版本記在 PRAGMA user_version（SCHEMA_VERSION）、一個檔只有一個寫入者（報表執行緒；啟動時的 prepare()
在主執行緒做完才起執行緒）。目錄由 open_record() 建。**不自動刪除任何一列。**

  meta     一列（id = 1）：created_ms = 第一次建檔的時刻（UTC epoch 毫秒整數）。FR-7 條件 2 用它：
           end ≤ created_ms 的期別永遠不發（第一次上線不補舊帳）
  reports  一列 = 一期，UNIQUE (kind, period_start_ms)。時刻一律 UTC epoch 毫秒整數
           kind / period_start_ms / period_end_ms / period_label / report_key（發送器的 key）/ scheduled_ms
           status        pending / delivered / failed / skipped
           text          凍結的訊息文字（組字一次就寫進來，之後重試、重啟重送都用它；skipped 為 NULL）
           late_note     組字時有沒有加延遲註記（0 / 1）
           summary_json  組字當下的各策略統計（n、止盈、止損、R、F、淨、持倉），日誌與稽核用
           attempts      交給發送器的次數；handoff_open = 1 表示「交出去了、結果還沒記回來」
           last_handoff_ms / next_attempt_ms / message_id / last_error（不含密鑰）/ created_ms / finalized_ms

狀態機（一期）：
  （沒有列）──組字成功──→ pending ──交出（attempts + 1，handoff_open = 1）──→ 等結果
  （沒有列）──已超過補發上限──→ skipped（終態，WARNING）
  等結果 ── delivered ──→ delivered（終態，INFO，記 message_id）
  等結果 ── failed ──→ failed（終態，ERROR，不再重送）
  等結果 ── gave_up / abandoned / 拒收 / 其他 ──→ pending（handoff_open = 0，A_CHANNEL_REPORT_RETRY_SECONDS 後
            用同一段文字重送；寧可多一則，不可漏發）
  重啟時 handoff_open = 1 的 pending（交出後當機 / 停止、結果沒記回）→ handoff_open = 0、立刻重送同一段文字
組字失敗（檔案不存在、版本不符、被鎖住、任何例外）**不寫列**：ERROR（同一期同一種錯誤只記一次），下一輪重試。

──────────────────────────────────────────────────────────────────────
報表執行緒（a-channel-report）
──────────────────────────────────────────────────────────────────────
每一輪（pump）：先把發送器回報的結果依序記回發送紀錄（COMMIT 成功才移出佇列），再（沒有正在交出的、也不在停止中）
依 (end, 日報 → 10 日報 → 月報) 的順序找下一則：
  * 候選 = 還是 pending 的列 ∪ 「end > created_ms、排定發送時刻 ≤ 現在、發送紀錄裡還沒有」的期別
  * 排在最前面的那一則決定這一輪（**不越過它**，順序因此固定）：
      pending 列、next_attempt_ms 還沒到 → 等到那一刻
      pending 列 → 交出同一段文字
      新的期別、現在 − 排定發送時刻 > A_CHANNEL_REPORT_CATCHUP_MAX_SECONDS → 記 skipped（WARNING），看下一則
      新的期別 → 組字（每次開新的唯讀連線）；晚於排定時刻超過 A_CHANNEL_REPORT_LATE_NOTE_SECONDS 就加延遲註記
      （WARNING）；寫進 pending 列（凍結）→ 交出
  * **同一時間最多交出一則**：上一則的結果記回來之前不交下一則
交出之前先 COMMIT「已交出」再呼叫 sender.send(text, key=report:<種類>:<期間第一天>, on_done=...)（不帶 expires_at）。
on_done 在發送器的執行緒（或呼叫 sender.stop() 的執行緒）被呼叫，只把結果排進佇列；sqlite 連線不跨執行緒（比照 T2）。
send() 回傳 False（拒收，不會有 on_done）或拋例外 → 由報表執行緒自己排一則「拒收」結果進同一個佇列。

等待（不忙等）：有結果就立刻再跑一輪；否則等到「下一個排定發送時刻 / 重試時刻」，但最多
A_CHANNEL_REPORT_POLL_SECONDS（牆上時鐘被調整時的上限）。所以報表在排定時刻之後一個輪詢週期內交出。
每一輪的未預期例外（例如發送紀錄寫不進去）→ ERROR、關掉連線、A_CHANNEL_REPORT_RETRY_SECONDS 後重開再試；
已回報的結果留在佇列裡不會遺失。執行緒因 BaseException 結束 → ERROR，stats()["worker_crashed"] = True，
其他元件照跑（執行入口結束時 exit 1）。

關閉：stop() 停止交出新的報表 → 最後一輪把已回報的結果記回 → 結束執行緒。**不停發送器**（那是 T2 的
push.shutdown()）。在途的那一則若結果沒記回來，列停在 handoff_open = 1，下次啟動重送同一段文字。

stats()：worker_alive、worker_crashed、delivered / failed / skipped（本次執行期間的次數）、pending（發送紀錄目前
待送的列數）、handoffs、retries、compose_errors、last_error（不含密鑰：組字錯誤是本機訊息，發送錯誤是 ChannelSender
已遮罩的 detail）。
"""

import collections
import functools
import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import timedelta

from live import config
from live import a_channel_report as report_mod
from live.a_channel_text import format_signed_pct
from live.tg_channel import RESULT_DELIVERED, RESULT_FAILED, SendResult

logger = logging.getLogger(__name__)

THREAD_NAME = "a-channel-report"
SCHEMA_VERSION = 1

STATUS_PENDING = "pending"
STATUS_DELIVERED = "delivered"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"
STATUSES = (STATUS_PENDING, STATUS_DELIVERED, STATUS_FAILED, STATUS_SKIPPED)

# 發送器拒收（send() 回傳 False 或拋例外）時，報表執行緒自己排進結果佇列的狀態（不是 ChannelSender 的回報）
RESULT_REJECTED = "rejected"

RELEASED_NOTE = "上次交出後結果沒有記回來，視為可能已送達，重送同一段文字"


def _system_now_ms():
    return time.time_ns() // 1_000_000


def _sql_in(values):
    return ", ".join("'%s'" % v for v in values)


_DDL = (
    """CREATE TABLE meta (
        id          INTEGER PRIMARY KEY CHECK (id = 1),
        created_ms  INTEGER NOT NULL
    )""",
    """CREATE TABLE reports (
        seq             INTEGER PRIMARY KEY AUTOINCREMENT,
        kind            TEXT    NOT NULL CHECK (kind IN (%s)),
        period_start_ms INTEGER NOT NULL,
        period_end_ms   INTEGER NOT NULL,
        period_label    TEXT    NOT NULL,
        report_key      TEXT    NOT NULL,
        scheduled_ms    INTEGER NOT NULL,
        status          TEXT    NOT NULL CHECK (status IN (%s)),
        text            TEXT,
        late_note       INTEGER NOT NULL DEFAULT 0 CHECK (late_note IN (0, 1)),
        summary_json    TEXT,
        attempts        INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        handoff_open    INTEGER NOT NULL DEFAULT 0 CHECK (handoff_open IN (0, 1)),
        last_handoff_ms INTEGER,
        next_attempt_ms INTEGER,
        message_id      INTEGER,
        last_error      TEXT,
        created_ms      INTEGER NOT NULL,
        finalized_ms    INTEGER,
        UNIQUE (kind, period_start_ms),
        CHECK (period_end_ms > period_start_ms),
        CHECK ((status = 'pending' AND finalized_ms IS NULL AND text IS NOT NULL)
            OR (status IN ('delivered', 'failed') AND finalized_ms IS NOT NULL AND handoff_open = 0
                AND text IS NOT NULL)
            OR (status = 'skipped' AND finalized_ms IS NOT NULL AND handoff_open = 0 AND text IS NULL
                AND attempts = 0))
    )""" % (_sql_in(report_mod.KINDS), _sql_in(STATUSES)),
)


class RecordError(Exception):
    """發送紀錄開不起來或狀態不對（版本不符、不是本模組建的檔、要更新的列不在預期狀態）。"""


def _check_version(conn, path):
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version == SCHEMA_VERSION:
        return version
    if version > SCHEMA_VERSION:
        raise RecordError("%s 的 schema 版本是 %d，比本程式認得的 %d 新；請用新版程式開啟"
                          % (path, version, SCHEMA_VERSION))
    existing = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")]
    if version or existing:
        raise RecordError("%s 不是 live.a_channel_report_push 建的發送紀錄（版本 %d，已有 %s）"
                          % (path, version, ", ".join(sorted(existing)) or "無"))
    return version


def open_record(path=None, *, now_ms, busy_timeout=None):
    """開啟（必要時建立）發送紀錄並回傳 Record（一條連線，只能在呼叫的這個執行緒用）。

    path          省略時取 config.A_CHANNEL_REPORT_DB_PATH（呼叫當下才讀）；上層目錄不存在會自動建
    now_ms        第一次建檔時寫進 meta.created_ms 的時刻（UTC epoch 毫秒）；已存在的檔不改
    busy_timeout  被鎖住時最多等幾秒（sqlite3 的 timeout）；省略用 sqlite3 預設
    """
    path = os.path.abspath(config.A_CHANNEL_REPORT_DB_PATH if path is None else path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    kwargs = {"isolation_level": None}
    if busy_timeout is not None:
        kwargs["timeout"] = float(busy_timeout)
    conn = sqlite3.connect(path, **kwargs)
    try:
        conn.row_factory = sqlite3.Row
        _check_version(conn, path)
        mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        if str(mode).lower() != "wal":
            raise RecordError("無法把發送紀錄切到 WAL 模式（目前是 %r）；請把 A_CHANNEL_REPORT_DB_PATH 放在本機磁碟上"
                              % (mode,))
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("BEGIN IMMEDIATE")
        try:
            if _check_version(conn, path) != SCHEMA_VERSION:
                for ddl in _DDL:
                    conn.execute(ddl)
                conn.execute("INSERT INTO meta (id, created_ms) VALUES (1, ?)", (int(now_ms),))
                conn.execute("PRAGMA user_version = %d" % SCHEMA_VERSION)
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
            raise
        row = conn.execute("SELECT created_ms FROM meta WHERE id = 1").fetchone()
        if row is None:
            raise RecordError("%s 的 meta 沒有建立時刻" % path)
    except BaseException:
        conn.close()
        raise
    return Record(conn, path, int(row[0]))


class Record:
    """一條發送紀錄連線 + 報表執行緒會用到的存取函式。請用 open_record() 取得。可以當 context manager 用。"""

    def __init__(self, conn, path, created_ms):
        self._conn = conn
        self.path = path
        self.created_ms = created_ms

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    @property
    def closed(self):
        return self._conn is None

    def _c(self):
        if self._conn is None:
            raise RecordError("發送紀錄已經關閉")
        return self._conn

    @contextmanager
    def _tx(self):
        conn = self._c()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    self.close()        # 交易狀態不可信：關掉，未提交的內容隨之丟棄
            raise

    # ------------------------------------------------------------ 讀
    def keys(self):
        """已經有列的 (kind, period_start_ms)。"""
        return {(r[0], r[1]) for r in self._c().execute("SELECT kind, period_start_ms FROM reports")}

    def get(self, seq):
        row = self._c().execute("SELECT * FROM reports WHERE seq = ?", (seq,)).fetchone()
        return None if row is None else dict(row)

    def pending(self):
        return [dict(r) for r in self._c().execute("SELECT * FROM reports WHERE status = ? ORDER BY seq",
                                                   (STATUS_PENDING,))]

    def rows(self):
        return [dict(r) for r in self._c().execute("SELECT * FROM reports ORDER BY seq")]

    def counts(self):
        out = {s: 0 for s in STATUSES}
        for status, n in self._c().execute("SELECT status, count(*) FROM reports GROUP BY status"):
            out[status] = n
        out["handoff_open"] = self._c().execute("SELECT count(*) FROM reports WHERE handoff_open = 1").fetchone()[0]
        return out

    # ------------------------------------------------------------ 寫
    def _insert(self, conn, period, status, scheduled, now_ms, **cols):
        cols.update(kind=period.kind, period_start_ms=period.start_ms, period_end_ms=period.end_ms,
                    period_label=report_mod.period_label(period), report_key=report_mod.period_key(period),
                    scheduled_ms=int(scheduled), status=status, created_ms=int(now_ms))
        names = sorted(cols)
        cur = conn.execute("INSERT INTO reports (%s) VALUES (%s)" % (", ".join(names), ", ".join("?" for _ in names)),
                           [cols[n] for n in names])
        return cur.lastrowid

    def insert_pending(self, period, *, scheduled, text, late, summary, now_ms):
        """寫一列待送（凍結的文字）。回傳 seq。這一期已經有列 → sqlite3.IntegrityError（UNIQUE）。"""
        with self._tx() as conn:
            return self._insert(conn, period, STATUS_PENDING, scheduled, now_ms, text=text, late_note=int(bool(late)),
                                summary_json=json.dumps(summary, ensure_ascii=True, sort_keys=True))

    def insert_skipped(self, period, *, scheduled, now_ms, error):
        with self._tx() as conn:
            return self._insert(conn, period, STATUS_SKIPPED, scheduled, now_ms, finalized_ms=int(now_ms),
                                last_error=error)

    def _update_one(self, conn, sql, params, seq, what):
        if conn.execute(sql, params).rowcount != 1:
            raise RecordError("發送紀錄第 %s 列不在可以%s的狀態" % (seq, what))

    def mark_handoff(self, seq, *, now_ms):
        """交出之前先 COMMIT「已交出」。回傳這是第幾次交出（attempts）。"""
        with self._tx() as conn:
            self._update_one(conn, "UPDATE reports SET attempts = attempts + 1, handoff_open = 1, last_handoff_ms = ?, "
                                   "next_attempt_ms = NULL WHERE seq = ? AND status = ? AND handoff_open = 0",
                             (int(now_ms), seq, STATUS_PENDING), seq, "交出")
            return conn.execute("SELECT attempts FROM reports WHERE seq = ?", (seq,)).fetchone()[0]

    def finalize(self, seq, status, *, now_ms, message_id=None, error=None):
        if status not in (STATUS_DELIVERED, STATUS_FAILED):
            raise ValueError("finalize 只接受 delivered / failed，收到 %r" % (status,))
        with self._tx() as conn:
            self._update_one(conn, "UPDATE reports SET status = ?, handoff_open = 0, finalized_ms = ?, message_id = ?, "
                                   "last_error = COALESCE(?, last_error), next_attempt_ms = NULL "
                                   "WHERE seq = ? AND status = ?",
                             (status, int(now_ms), message_id, error, seq, STATUS_PENDING), seq, "記終態")

    def record_retry(self, seq, *, next_attempt_ms, error):
        with self._tx() as conn:
            self._update_one(conn, "UPDATE reports SET handoff_open = 0, next_attempt_ms = ?, last_error = ? "
                                   "WHERE seq = ? AND status = ?",
                             (int(next_attempt_ms), error, seq, STATUS_PENDING), seq, "排重試")

    def release_open_handoffs(self, *, now_ms):
        """handoff_open = 1 的待送列（交出後結果沒有記回來）→ 立刻重送同一段文字。回傳幾列。"""
        with self._tx() as conn:
            return conn.execute("UPDATE reports SET handoff_open = 0, next_attempt_ms = ?, last_error = ? "
                                "WHERE status = ? AND handoff_open = 1",
                                (int(now_ms), RELEASED_NOTE, STATUS_PENDING)).rowcount


def _summary_text(summary_json):
    """日誌用：各策略的 n／止盈／R／F。"""
    try:
        data = json.loads(summary_json or "{}")
    except ValueError:
        return "（統計無法解析）"
    parts = []
    for s in config.STRATEGIES:
        st = data.get(s)
        if st is None:
            continue
        parts.append("%s n=%d 止盈=%d R=%s F=%s" % (config.STRATEGY_LABELS.get(s, s), st["n"], st["take_profit"],
                                                  format_signed_pct(st["gross"]), format_signed_pct(-st["fee"])))
    return "；".join(parts)


class ChannelReporter:
    """A 頻道報表的報表執行緒（見模組 docstring）。"""

    def __init__(self, sender, *, path=None, live_db=None, outbox_db=None, now_ms=None, busy_timeout=None):
        """sender         已 start() 的 ChannelSender（T2 那一個）
        path            發送紀錄；省略時取 config.A_CHANNEL_REPORT_DB_PATH
        live_db / outbox_db  報表讀的兩個資料庫；省略時取 config.LIVE_DB_PATH / config.A_CHANNEL_OUTBOX_DB_PATH
        now_ms          回傳 UTC epoch 毫秒的時鐘（測試注入假時鐘）
        busy_timeout    讀兩個資料庫被鎖住時最多等幾秒；省略用 sqlite3 預設
        時間參數（輪詢、補發上限、延遲註記、重試、發送延遲、啟動 / 結束等待）在建構時從 config 讀。"""
        self.sender = sender
        self.path = os.path.abspath(config.A_CHANNEL_REPORT_DB_PATH if path is None else path)
        self.live_db = os.path.abspath(config.LIVE_DB_PATH if live_db is None else live_db)
        self.outbox_db = os.path.abspath(config.A_CHANNEL_OUTBOX_DB_PATH if outbox_db is None else outbox_db)
        self._now_fn = now_ms or _system_now_ms
        self._busy_timeout = busy_timeout
        self._poll = float(config.A_CHANNEL_REPORT_POLL_SECONDS)
        self._catchup_ms = int(config.A_CHANNEL_REPORT_CATCHUP_MAX_SECONDS * 1000)
        self._late_ms = int(config.A_CHANNEL_REPORT_LATE_NOTE_SECONDS * 1000)
        self._retry_ms = int(config.A_CHANNEL_REPORT_RETRY_SECONDS * 1000)
        self._delay_ms = int(config.A_CHANNEL_REPORT_SEND_DELAY_SECONDS * 1000)
        self._ready_timeout = float(config.A_CHANNEL_REPORT_READY_TIMEOUT_SECONDS)
        self._join_timeout = float(config.A_CHANNEL_REPORT_JOIN_TIMEOUT_SECONDS)
        if self._poll <= 0 or self._retry_ms <= 0 or self._catchup_ms < 0 or self._late_ms < 0 or self._delay_ms < 0:
            raise ValueError("輪詢間隔與重試間隔必須 > 0；補發上限、延遲註記門檻、發送延遲不可為負")

        self._cond = threading.Condition()
        self._results = collections.deque()     # (token, SendResult)，由發送器的 on_done 放進來
        self._in_flight = None                  # 交出去、結果還沒記回來的 (seq, attempt)
        self._stopping = False
        self._thread = None
        self._ready = threading.Event()
        self._ready_error = None
        self._crashed = False
        self._last_compose_error = None         # (key, 例外型別, 訊息)：同一期同一種錯誤只記一次 ERROR
        self._counts = {"delivered": 0, "failed": 0, "skipped": 0, "handoffs": 0, "retries": 0,
                        "compose_errors": 0}
        self._pending = 0
        self._last_error = None

    # ================================================================ 生命週期
    def prepare(self):
        """start() 的前半（不起執行緒）：建發送紀錄（必要時；建立時刻 = 現在）、把上次交出後沒記回結果的列改成
        立刻重送、記一行現況。回傳 counts()。測試的同步模式（直接呼叫 pump()）也從這裡開始。"""
        now = self._now()
        with open_record(self.path, now_ms=now) as record:
            released = record.release_open_handoffs(now_ms=now)
            counts = record.counts()
            created = record.created_ms
        with self._cond:
            self._pending = counts[STATUS_PENDING]
        logger.info("A 頻道報表發送紀錄（%s，建立於 %s）現況：待送 %d 則；累計 已送達 %d、永久失敗 %d、跳過 %d",
                    self.path, report_mod.taipei_date(created).isoformat(), counts[STATUS_PENDING],
                    counts[STATUS_DELIVERED], counts[STATUS_FAILED], counts[STATUS_SKIPPED])
        if released:
            logger.warning("A 頻道報表：%d 則報表上次交出後結果沒有記回來，視為可能已送達，重送同一段文字", released)
        return counts

    def start(self):
        """prepare()，再啟動報表執行緒並等它開好自己的連線。失敗就拋例外（執行入口 exit 1）。回傳 prepare() 的 counts。"""
        if self._thread is not None:
            raise RuntimeError("ChannelReporter 只能 start() 一次")
        counts = self.prepare()
        self._thread = threading.Thread(target=self._thread_main, name=THREAD_NAME, daemon=True)
        self._thread.start()
        if not self._ready.wait(self._ready_timeout):
            raise RuntimeError("A 頻道報表執行緒 %s 秒內沒有開好發送紀錄" % self._ready_timeout)
        if self._ready_error is not None:
            raise self._ready_error
        return counts

    def stop(self, timeout=None):
        """停止交出新的報表 → 報表執行緒把已回報的結果記回 → 結束。不停發送器。回傳執行緒是否已結束。"""
        with self._cond:
            self._stopping = True
            self._cond.notify_all()
        thread = self._thread
        if thread is None:
            return True
        thread.join(self._join_timeout if timeout is None else timeout)
        if thread.is_alive():
            logger.error("A 頻道報表執行緒 %s 秒內沒有結束", self._join_timeout if timeout is None else timeout)
            return False
        return True

    @property
    def worker_alive(self):
        return self._thread is not None and self._thread.is_alive()

    def stats(self):
        with self._cond:
            out = dict(self._counts)
            out.update(worker_alive=self.worker_alive, worker_crashed=self._crashed, pending=self._pending,
                       last_error=self._last_error)
        return out

    def in_flight(self):
        with self._cond:
            return self._in_flight

    def has_results(self):
        with self._cond:
            return bool(self._results)

    # ================================================================ 報表執行緒
    def _open_worker_record(self):
        return open_record(self.path, now_ms=self._now())

    def _thread_main(self):
        try:
            try:
                record = self._open_worker_record()
            except Exception as e:  # noqa: BLE001
                self._ready_error = e
                logger.exception("A 頻道報表執行緒開不了發送紀錄 %s", self.path)
                self._ready.set()
                return
            self._ready.set()
            self._loop(record)
        except BaseException as e:  # noqa: BLE001 —— 執行緒意外結束：記 ERROR，其他元件照跑（執行入口 exit 1）
            with self._cond:
                self._crashed = True
                self._last_error = "報表執行緒意外結束：%s" % type(e).__name__
            if not self._ready.is_set():
                self._ready_error = RuntimeError("A 頻道報表執行緒在開好發送紀錄之前就結束了（%s）" % type(e).__name__)
                self._ready.set()
            logger.exception("A 頻道報表執行緒意外結束（%s）；之後的報表不會發送，A1 / A5 / A3 / T2 照跑",
                             type(e).__name__)

    def _loop(self, record):
        """主迴圈。等待規則見模組 docstring。收到 stop 之後再跑最後一輪（只記結果）就結束。"""
        try:
            while True:
                with self._cond:
                    final = self._stopping
                wake_at = None
                failed = False
                try:
                    if record is None:
                        record = self._open_worker_record()
                    wake_at = self.pump(record)
                except Exception as e:  # noqa: BLE001 —— 執行緒不可以死：ERROR，隔一個重試間隔再試
                    failed = True
                    with self._cond:
                        self._last_error = "%s: %s" % (type(e).__name__, e)
                    logger.exception("A 頻道報表處理發送紀錄時發生未預期的例外，%s 秒後再試", self._retry_ms / 1000)
                    wake_at = self._now() + self._retry_ms
                    if record is not None:
                        try:
                            record.close()      # 連線狀態可能已經不可信：下一輪重開
                        except Exception:  # noqa: BLE001
                            pass
                        record = None
                with self._cond:
                    if final:
                        return
                    if self._stopping:
                        continue            # 剛收到 stop：再跑最後一輪
                    if self._results and not failed:
                        continue
                    timeout = self._poll
                    if wake_at is not None:
                        timeout = min(timeout, max(0.0, (wake_at - self._now()) / 1000.0))
                    if timeout > 0:
                        self._cond.wait(timeout)
        finally:
            if record is not None:
                record.close()

    def pump(self, record):
        """一輪：把發送器回報的結果依序記回發送紀錄，再（沒有正在交出的、也不在停止中）處理下一則。
        回傳下一個該醒來的 epoch ms（沒有就 None）。報表執行緒與測試共用這一個函式。
        記回失敗等發送紀錄的錯誤往外拋（結果留在佇列裡）。"""
        self._record_results(record)
        with self._cond:
            busy = self._stopping or self._in_flight is not None
        wake_at = None if busy else self._dispatch(record)
        pending = record.counts()[STATUS_PENDING]
        with self._cond:
            self._pending = pending
        return wake_at

    def _record_results(self, record):
        """結果佇列一次處理最前面的一則：COMMIT 成功才移出、才清掉「正在交出」。寫不進去就往外拋，結果留著。"""
        while True:
            with self._cond:
                if not self._results:
                    return
                token, result = self._results[0]
            self._record_result(record, token, result)
            with self._cond:
                self._results.popleft()         # 只有報表執行緒會移出；on_done 只從尾端加入
                if self._in_flight == token:
                    self._in_flight = None

    def _record_result(self, record, token, result):
        seq, attempt = token
        row = record.get(seq)
        if (row is None or row["status"] != STATUS_PENDING or not row["handoff_open"]
                or row["attempts"] != attempt):
            logger.warning("A 頻道報表收到一則對不上的發送結果（發送紀錄第 %s 列第 %s 次交出：%s），忽略", seq, attempt,
                           result.status)
            return
        key = row["report_key"]
        now = self._now()
        if result.status == RESULT_DELIVERED:
            record.finalize(seq, STATUS_DELIVERED, now_ms=now, message_id=result.message_id)
            with self._cond:
                self._counts["delivered"] += 1
            logger.info("A 頻道報表：%s（%s）已送達（message_id=%s）；%s", key, row["period_label"], result.message_id,
                        _summary_text(row["summary_json"]))
        elif result.status == RESULT_FAILED:
            detail = result.detail or result.status
            record.finalize(seq, STATUS_FAILED, now_ms=now, error=detail)
            with self._cond:
                self._counts["failed"] += 1
                self._last_error = "%s 永久失敗：%s" % (key, detail)
            logger.error("A 頻道報表：%s（%s）永久失敗，不再重送%s：%s", key, row["period_label"],
                         "（Telegram 可能其實已經收到）" if result.uncertain else "", detail)
        else:
            # gave_up / abandoned / 拒收 / 其他（本模組不帶 expires_at，不會有 expired）：維持待送、同一段文字重送
            detail = result.detail or result.status
            record.record_retry(seq, next_attempt_ms=now + self._retry_ms, error="%s: %s" % (result.status, detail))
            with self._cond:
                self._counts["retries"] += 1
            logger.warning("A 頻道報表：%s 這次沒有確定送達（%s），維持待送，%s 秒後用同一段文字重送", key,
                           result.status, self._retry_ms / 1000)

    def _next_scheduled_ms(self, now):
        """now 之後的下一個排定發送時刻（每一期的 end 都是某天的台北 00:00，而日報每天都有）。"""
        day = report_mod.taipei_date(now - self._delay_ms)
        return report_mod.taipei_midnight_ms(day + timedelta(days=1)) + self._delay_ms

    def _dispatch(self, record):
        now = self._now()
        released = record.release_open_handoffs(now_ms=now)
        if released:
            # 走到這裡時一定沒有正在交出的、也沒有待記的結果：handoff_open 的列是孤兒，比照重啟時的做法
            logger.warning("A 頻道報表：%d 則報表交出後結果沒有記回來，視為可能已送達，重送同一段文字", released)
        order = {k: i for i, k in enumerate(report_mod.KINDS)}
        candidates = [((row["period_end_ms"], order[row["kind"]]), row, None) for row in record.pending()]
        known = record.keys()
        for p in report_mod.periods_ending_in(record.created_ms, now - self._delay_ms):
            if (p.kind, p.start_ms) not in known:
                candidates.append(((p.end_ms, order[p.kind]), None, p))
        candidates.sort(key=lambda c: c[0])
        for _, row, period in candidates:
            if row is not None:
                nxt = row["next_attempt_ms"]
                if nxt is not None and nxt > now:
                    return nxt              # 排最前面的還沒到重試時刻：不越過它
                self._handoff(record, row, now)
                return None
            sched = report_mod.scheduled_ms(period, self._delay_ms / 1000)
            key = report_mod.period_key(period)
            if now - sched > self._catchup_ms:
                late_hours = (now - sched) / 1000 / 3600
                record.insert_skipped(period, scheduled=sched, now_ms=now,
                                      error="超過補發上限：晚於排定時刻 %.1f 小時（上限 %.1f 小時）"
                                            % (late_hours, self._catchup_ms / 1000 / 3600))
                with self._cond:
                    self._counts["skipped"] += 1
                logger.warning("A 頻道報表：%s（%s）已晚於排定時刻 %.1f 小時，超過補發上限 %.1f 小時，不發（記為跳過）",
                               key, report_mod.period_label(period), late_hours, self._catchup_ms / 1000 / 3600)
                continue
            late = now - sched > self._late_ms
            try:
                text, report = report_mod.compose(period, live_db=self.live_db, outbox_db=self.outbox_db, late=late,
                                                  busy_timeout=self._busy_timeout)
            except Exception as e:  # noqa: BLE001 —— 組不出來：不寫列、下一輪重試，執行緒不結束
                self._compose_failed(key, e)
                return None
            self._last_compose_error = None
            seq = record.insert_pending(period, scheduled=sched, text=text, late=late,
                                        summary=report_mod.summary(report), now_ms=now)
            if late:
                logger.warning("A 頻道報表：%s（%s）晚於排定時刻 %.1f 分鐘才組字，加上延遲註記", key,
                               report_mod.period_label(period), (now - sched) / 1000 / 60)
            self._handoff(record, record.get(seq), now)
            return None
        return self._next_scheduled_ms(now)

    def _compose_failed(self, key, error):
        signature = (key, type(error).__name__, str(error))
        with self._cond:
            self._counts["compose_errors"] += 1
            self._last_error = "組報表失敗 %s：%s: %s" % (key, type(error).__name__, error)
        if signature != self._last_compose_error:
            self._last_compose_error = signature
            logger.error("A 頻道報表：%s 組不出來（%s: %s），不寫發送紀錄，下一輪重試（同樣的錯誤不再重複記）", key,
                         type(error).__name__, error)

    def _handoff(self, record, row, now):
        seq, key, text = row["seq"], row["report_key"], row["text"]
        attempt = record.mark_handoff(seq, now_ms=now)      # 先 COMMIT「已交出」，再交出去
        token = (seq, attempt)
        with self._cond:
            self._in_flight = token
        try:
            accepted = self.sender.send(text, key=key, on_done=functools.partial(self._on_done, token))
        except Exception:  # noqa: BLE001 —— 例如發送器還沒 start：當成拒收，稍後重試
            logger.exception("A 頻道報表把 %s 交給發送器時拋出例外", key)
            accepted = False
        if not accepted:
            self._on_done(token, SendResult(status=RESULT_REJECTED, key=key, message_id=None, uncertain=False,
                                            detail="發送器拒收（已停止、訊息不合格、或發送執行緒不在），稍後重試"))
            return
        with self._cond:
            self._counts["handoffs"] += 1
        logger.info("A 頻道報表：%s（%s）交給發送器（第 %d 次）", key, row["period_label"], attempt)

    def _on_done(self, token, result):
        """ChannelSender 的 on_done（在發送器的執行緒）：只排進自己的佇列，由報表執行緒記回發送紀錄。"""
        with self._cond:
            self._results.append((token, result))
            self._cond.notify_all()

    def _now(self):
        return int(self._now_fn())
