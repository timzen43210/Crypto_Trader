# -*- coding: utf-8 -*-
"""
live.a_channel_outbox — A 頻道的頻道發送紀錄（T2 FR-2）：持久化 outbox（SQLite）
=================================================================================
記下每個事件在頻道上的最終狀態。兩個用途：
  1. 發送的持久化：ChannelSender 的佇列只在記憶體裡，handler 一返回 A3 就標記已發布。之後當機、重啟、或
     stop() 期限內沒送完，那則訊息就永遠不會送出 —— 漏掉的若是止損訊息，就是「報喜不報憂」。所以事件先落地到
     這裡，送出與否由 live.a_channel_push 依這份紀錄決定、重啟後接著送
  2. **R-A 報表的統計依據**：使用者 2026-09-28 決定「頻道上沒有出現進場訊息的訊號，報表不統計」。
     R-A 以本檔 kind='entry' AND status='delivered' 的 signal_id 過濾 live.sqlite3 的已平倉名目部位
     （delivered_entry_signal_ids()）。**不自動刪除任何一列**

獨立一個檔（config.A_CHANNEL_OUTBOX_DB_PATH = runtime/db/a_channel_outbox.sqlite3），不動 live/store.py 與
live.sqlite3，避免牽動 A3。目錄由 open_outbox() 自己建。

──────────────────────────────────────────────────────────────────────
一列 = 一個 (signal_id, 事件種類)，UNIQUE
──────────────────────────────────────────────────────────────────────
重複收到（A3 重送、重啟補發）時 INSERT OR IGNORE：忽略、不改既有列。持久化的去重跨重啟也有效。

  seq              寫入順序（AUTOINCREMENT），T2 依它 FIFO 取出
  signal_id, kind  kind = 'entry' / 'exit'
  strategy, symbol 事件的欄位（方便查詢；完整內容在 event_json）
  event_json       event.to_dict() 的 JSON（見下面「JSON」），由它重建出同一個事件
  snapshot_json    收到當下的快照值（策略名稱、價格小數位數與來源、槓桿上限、訊號 K 棒收盤、倉位規則、偏離門檻），
                   讓同一列不論何時重新組字，內容都相同
  received_ms      handler 寫入的時刻（UTC epoch ms）
  status           見下表
  attempts         交給發送器的次數
  uncertain        之前有沒有任何一次交出的結果「不確定」（請求可能已到 Telegram）。為 1 之後不再套延遲門檻
  handoff_open     已交給發送器、結果還沒記回來。重啟時仍為 1 → 當成不確定（mark_open_handoffs_uncertain）
  last_handoff_ms  最後一次交給發送器的時刻
  next_attempt_ms  「待送」的列最早什麼時候再交出去（重試間隔）
  text             最後一次交出去的文字（結果不確定時重送同一段文字；也給 AC-9 三方比對與稽核）
  message_id       Telegram 的 message_id（已送達才有）
  detail           最後的原因 / 錯誤描述（已遮罩，不含密鑰）
  finalized_ms     進入終態的時刻

狀態（名稱是本模組定的，語意照 PRD FR-2 第 6 點）：

  pending      待送              還沒確定送達                                              非終態
  delivered    已送達            Telegram 回 ok=true（記 message_id）                        終態
  expired      延遲不發          進場在送出前已超過延遲門檻（FR-3）                            終態
  no_entry     因進場未出現而不發  出場事件，但同一 signal_id 的進場不是「已送達」（含沒有進場列） 終態
  failed       永久失敗          400 / 401 / 403 等、或 HTTP 200 但 ok≠true（可能已送出，不可重送） 終態

表層 CHECK 擋住「終態卻沒有 finalized_ms」「終態卻還掛著 handoff_open」這類半套狀態。終態的列不會再被改
（finalize() / record_retry() 都只更新 status='pending' 的列）。

──────────────────────────────────────────────────────────────────────
連線、交易、耐久性（比照 live.store）
──────────────────────────────────────────────────────────────────────
  * 一個 Outbox（一條連線）只在建立它的執行緒使用（sqlite3 的 check_same_thread）。T2 的工作執行緒開一條長期的；
    匯流排 handler（在 A3 的執行緒裡）每次呼叫自己開一條短的（open_outbox(..., init=False)），用完就關
  * isolation_level=None，寫入一律明確 BEGIN IMMEDIATE / COMMIT / ROLLBACK；每個寫入函式回傳前 COMMIT 完畢
  * journal_mode=WAL、synchronous=FULL：COMMIT 回傳時已 fsync，行程被硬砍、斷電都不會丟
  * busy timeout 由呼叫端給（handler 用 config.A_CHANNEL_OUTBOX_BUSY_TIMEOUT_SECONDS，刻意短）
  * schema 版本記在 PRAGMA user_version（目前 1）；版本不對、或不是本模組建的檔就拋 OutboxSchemaError
  * **同一個 outbox 檔同時只能有一個 T2 在寫**（跟 live.sqlite3 同一個假設）

──────────────────────────────────────────────────────────────────────
JSON
──────────────────────────────────────────────────────────────────────
event_json / snapshot_json 與 live.store 的 features_json 同一種寫法：json.dumps(ensure_ascii=True, sort_keys=True,
allow_nan=True)。features 可能含 ±inf（例如 volr），會寫成 Python 風格的 Infinity / -Infinity（非標準 JSON）；
讀取端請用 Python 的 json（loads() 認得）。純 ASCII，lone surrogate 也存得進去。
"""

import json
import os
import sqlite3
from contextlib import contextmanager

from live import config

SCHEMA_VERSION = 1
TABLE = "outbox"

KIND_ENTRY = "entry"
KIND_EXIT = "exit"
KINDS = (KIND_ENTRY, KIND_EXIT)

STATUS_PENDING = "pending"
STATUS_DELIVERED = "delivered"
STATUS_EXPIRED = "expired"
STATUS_NO_ENTRY = "no_entry"
STATUS_FAILED = "failed"
TERMINAL_STATUSES = (STATUS_DELIVERED, STATUS_EXPIRED, STATUS_NO_ENTRY, STATUS_FAILED)
STATUSES = (STATUS_PENDING,) + TERMINAL_STATUSES

# 中文名稱（日誌用）
STATUS_LABELS = {STATUS_PENDING: "待送", STATUS_DELIVERED: "已送達", STATUS_EXPIRED: "延遲不發",
                 STATUS_NO_ENTRY: "因進場未出現而不發", STATUS_FAILED: "永久失敗"}

_DDL = (
    """
    CREATE TABLE IF NOT EXISTS outbox (
        seq              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id        TEXT    NOT NULL CHECK (typeof(signal_id) = 'text' AND signal_id <> ''),
        kind             TEXT    NOT NULL CHECK (kind IN ('entry', 'exit')),
        strategy         TEXT    NOT NULL CHECK (typeof(strategy) = 'text' AND strategy <> ''),
        symbol           TEXT    NOT NULL CHECK (typeof(symbol) = 'text' AND symbol <> ''),
        event_json       TEXT    NOT NULL CHECK (typeof(event_json) = 'text'),
        snapshot_json    TEXT    NOT NULL CHECK (typeof(snapshot_json) = 'text'),
        received_ms      INTEGER NOT NULL CHECK (typeof(received_ms) = 'integer'),
        status           TEXT    NOT NULL
                         CHECK (status IN ('pending', 'delivered', 'expired', 'no_entry', 'failed')),
        attempts         INTEGER NOT NULL DEFAULT 0 CHECK (typeof(attempts) = 'integer' AND attempts >= 0),
        uncertain        INTEGER NOT NULL DEFAULT 0 CHECK (uncertain IN (0, 1)),
        handoff_open     INTEGER NOT NULL DEFAULT 0 CHECK (handoff_open IN (0, 1)),
        last_handoff_ms  INTEGER CHECK (last_handoff_ms IS NULL OR typeof(last_handoff_ms) = 'integer'),
        next_attempt_ms  INTEGER CHECK (next_attempt_ms IS NULL OR typeof(next_attempt_ms) = 'integer'),
        text             TEXT    CHECK (text IS NULL OR typeof(text) = 'text'),
        message_id       INTEGER CHECK (message_id IS NULL OR typeof(message_id) = 'integer'),
        detail           TEXT    CHECK (detail IS NULL OR typeof(detail) = 'text'),
        finalized_ms     INTEGER CHECK (finalized_ms IS NULL OR typeof(finalized_ms) = 'integer'),
        UNIQUE (signal_id, kind),
        CHECK ((status = 'pending' AND finalized_ms IS NULL)
            OR (status <> 'pending' AND finalized_ms IS NOT NULL AND handoff_open = 0))
    )
    """,
)


class OutboxError(Exception):
    """本模組自訂例外的基底。"""


class OutboxSchemaError(OutboxError):
    """檔案的 schema 版本不是本程式能處理的（更新版，或根本不是本模組建的檔）。"""


class OutboxStateError(OutboxError):
    """要更新的列不存在、或已經是終態（呼叫端的邏輯錯誤；資料庫沒有任何變更）。"""


def to_json(obj):
    """event_json / snapshot_json 的寫法（見模組 docstring）。"""
    return json.dumps(obj, ensure_ascii=True, sort_keys=True, allow_nan=True)


def _apply_pragmas(conn):
    mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    if str(mode).lower() != "wal":
        raise OutboxError("無法把 outbox 切到 WAL 模式（目前是 %r）；請把 A_CHANNEL_OUTBOX_DB_PATH 放在本機磁碟上"
                          % (mode,))
    conn.execute("PRAGMA synchronous=FULL")
    if conn.execute("PRAGMA synchronous").fetchone()[0] != 2:        # 2 = FULL
        raise OutboxError("無法把 outbox 的 synchronous 設成 FULL")


def _check_version(conn, path, *, allow_new):
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version == SCHEMA_VERSION:
        return version
    if version > SCHEMA_VERSION:
        raise OutboxSchemaError("%s 的 schema 版本是 %d，比本程式認得的 %d 新；請用新版程式開啟"
                                % (path, version, SCHEMA_VERSION))
    existing = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")]
    if existing:
        raise OutboxSchemaError("%s 沒有 outbox 的 schema 版本卻已經有東西（%s），不是 live.a_channel_outbox 建的檔"
                                % (path, ", ".join(sorted(existing))))
    if not allow_new:
        raise OutboxSchemaError("%s 還沒有建立 outbox（T2 啟動時由 ChannelPusher.start() 建立）" % path)
    return version


def open_outbox(path=None, *, busy_timeout=None, init=True):
    """開啟 outbox 並回傳 Outbox（一條連線，只能在呼叫的這個執行緒用）。

    path          省略時取 config.A_CHANNEL_OUTBOX_DB_PATH（呼叫當下才讀）；上層目錄不存在會自動建（init=True 時）
    busy_timeout  資料庫被鎖住時最多等幾秒（sqlite3 的 timeout）；省略用 sqlite3 預設的 5 秒
    init          True：必要時建表（T2 啟動、工作執行緒）。False：只開既有的 outbox，不存在或版本不對就拋
                  OutboxSchemaError，不建任何東西（handler 用：檔案被刪掉時要大聲失敗，不是悄悄建一個空的）
    """
    path = os.path.abspath(path if path is not None else config.A_CHANNEL_OUTBOX_DB_PATH)
    if init:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    elif not os.path.isfile(path):
        raise OutboxSchemaError("%s 不存在（outbox 由 ChannelPusher.start() 建立）" % path)
    kwargs = {"isolation_level": None}
    if busy_timeout is not None:
        kwargs["timeout"] = float(busy_timeout)
    conn = sqlite3.connect(path, **kwargs)
    try:
        conn.row_factory = sqlite3.Row
        _check_version(conn, path, allow_new=init)
        _apply_pragmas(conn)
        if init:
            conn.execute("BEGIN IMMEDIATE")
            try:
                if _check_version(conn, path, allow_new=True) == 0:
                    for ddl in _DDL:
                        conn.execute(ddl)
                    conn.execute("PRAGMA user_version = %d" % SCHEMA_VERSION)
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                raise
    except BaseException:
        conn.close()
        raise
    return Outbox(conn, path)


def _row_dict(row):
    d = dict(row)
    d["event"] = json.loads(d["event_json"])
    d["snapshot"] = json.loads(d["snapshot_json"])
    d["uncertain"] = bool(d["uncertain"])
    d["handoff_open"] = bool(d["handoff_open"])
    return d


class Outbox:
    """一條 outbox 連線 + T2 會用到的存取函式。請用 open_outbox() 取得。可以當 context manager 用。"""

    def __init__(self, conn, path):
        self._conn = conn
        self.path = path

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
            raise OutboxError("outbox 已經關閉")
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
                    # ROLLBACK 本身失敗：這條連線的交易狀態不可信，直接關掉（未提交的內容隨之丟棄）
                    self.close()
            raise

    # ---------------------------------------------------------------- 寫入
    def insert(self, *, signal_id, kind, strategy, symbol, event, snapshot, received_ms):
        """寫入一列「待送」。(signal_id, kind) 已經存在就什麼都不改，回傳 False；新寫入回傳 True。"""
        if kind not in KINDS:
            raise ValueError("kind 必須是 %s 之一，收到 %r" % ("/".join(KINDS), kind))
        with self._tx() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO outbox (signal_id, kind, strategy, symbol, event_json, snapshot_json, "
                "received_ms, status) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')",
                (signal_id, kind, strategy, symbol, to_json(event), to_json(snapshot), int(received_ms)))
            return cur.rowcount == 1

    def mark_handoff(self, seq, *, now_ms, text):
        """交給發送器之前呼叫（先 COMMIT 再交出去：交出去之後才當掉的話，重啟時看得到 handoff_open）。
        回傳這一次是第幾次交出（attempts）。"""
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE outbox SET attempts = attempts + 1, handoff_open = 1, last_handoff_ms = ?, text = ? "
                "WHERE seq = ? AND status = 'pending' AND handoff_open = 0", (int(now_ms), text, int(seq)))
            if cur.rowcount != 1:
                raise OutboxStateError("outbox 第 %s 列不是可以交出的待送列" % seq)
            return conn.execute("SELECT attempts FROM outbox WHERE seq = ?", (int(seq),)).fetchone()[0]

    def record_retry(self, seq, *, next_attempt_ms, uncertain, detail=None):
        """這一次交出的結果是「不確定 / 放棄」（或發送器拒收）：維持待送，next_attempt_ms 之後再交。
        uncertain 只會由 0 變 1，不會變回 0。"""
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE outbox SET handoff_open = 0, next_attempt_ms = ?, "
                "uncertain = MAX(uncertain, ?), detail = ? WHERE seq = ? AND status = 'pending'",
                (int(next_attempt_ms), 1 if uncertain else 0, detail, int(seq)))
            if cur.rowcount != 1:
                raise OutboxStateError("outbox 第 %s 列不是待送列，無法排重試" % seq)

    def finalize(self, seq, status, *, now_ms, message_id=None, detail=None):
        """待送 → 終態。已經是終態的列拋 OutboxStateError（資料庫沒有變更）。"""
        if status not in TERMINAL_STATUSES:
            raise ValueError("status 必須是終態 %s 之一，收到 %r" % ("/".join(TERMINAL_STATUSES), status))
        if message_id is not None and (isinstance(message_id, bool) or not isinstance(message_id, int)):
            message_id = None           # Telegram 的 message_id 是整數；格式怪就不存，狀態照記
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE outbox SET status = ?, handoff_open = 0, finalized_ms = ?, message_id = ?, "
                "detail = COALESCE(?, detail) WHERE seq = ? AND status = 'pending'",
                (status, int(now_ms), message_id, detail, int(seq)))
            if cur.rowcount != 1:
                raise OutboxStateError("outbox 第 %s 列不是待送列，無法改成 %s" % (seq, status))

    def mark_open_handoffs_uncertain(self, *, now_ms):
        """重啟時呼叫：上次交給發送器、結果沒記回來的待送列（當機、stop 等不到發送執行緒）一律當成「不確定」
        （頻道上可能已經有它），立刻可以再交出。回傳這樣的列數。"""
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE outbox SET handoff_open = 0, uncertain = 1, next_attempt_ms = ?, "
                "detail = '上次交給發送器後結果沒有記回來（當機或停止），視為可能已送達' "
                "WHERE status = 'pending' AND handoff_open = 1", (int(now_ms),))
            return cur.rowcount

    def release_orphan_handoff(self, seq, *, now_ms):
        """執行中發現「交出去了、結果卻沒有記回來」的待送列（BUG-009 的第二道防線）：跟重啟時一樣當成不確定，
        立刻可以再交出。回傳是否真的改了這一列。"""
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE outbox SET handoff_open = 0, uncertain = 1, next_attempt_ms = ?, "
                "detail = '交出去之後結果沒有記回來，視為可能已送達' "
                "WHERE seq = ? AND status = 'pending' AND handoff_open = 1", (int(now_ms), int(seq)))
            return cur.rowcount == 1

    # ---------------------------------------------------------------- 讀取
    def get(self, signal_id, kind):
        row = self._c().execute("SELECT * FROM outbox WHERE signal_id = ? AND kind = ?",
                                (signal_id, kind)).fetchone()
        return None if row is None else _row_dict(row)

    def get_by_seq(self, seq):
        row = self._c().execute("SELECT * FROM outbox WHERE seq = ?", (int(seq),)).fetchone()
        return None if row is None else _row_dict(row)

    def exists(self, signal_id, kind):
        return self._c().execute("SELECT 1 FROM outbox WHERE signal_id = ? AND kind = ?",
                                 (signal_id, kind)).fetchone() is not None

    def pending(self):
        """所有待送的列，依寫入順序（seq）。"""
        return [_row_dict(r) for r in
                self._c().execute("SELECT * FROM outbox WHERE status = 'pending' ORDER BY seq")]

    def rows(self):
        """全部的列，依寫入順序（給測試、診斷與報表）。"""
        return [_row_dict(r) for r in self._c().execute("SELECT * FROM outbox ORDER BY seq")]

    def counts(self):
        """{狀態: 列數}，每個狀態都列（沒有就是 0）；另附 uncertain_pending（待送裡結果不確定的列數）。"""
        out = {s: 0 for s in STATUSES}
        for status, n in self._c().execute("SELECT status, COUNT(*) FROM outbox GROUP BY status"):
            out[status] = n
        out["uncertain_pending"] = self._c().execute(
            "SELECT COUNT(*) FROM outbox WHERE status = 'pending' AND uncertain = 1").fetchone()[0]
        return out

    def delivered_entry_signal_ids(self):
        """頻道上確定出現過進場訊息的 signal_id（R-A 報表的統計依據，使用者 2026-09-28 決定）。"""
        return [r[0] for r in self._c().execute(
            "SELECT signal_id FROM outbox WHERE kind = 'entry' AND status = 'delivered' ORDER BY seq")]
