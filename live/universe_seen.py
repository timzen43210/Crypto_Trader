# -*- coding: utf-8 -*-
"""
live.universe_seen — 標的池的新上架追蹤（A6 FR-4）：記住看過的合約，新合約出現時交給通知
==========================================================================================
派網沒有資產類型欄位，標的池只能靠 strategy.universe 的名稱規則判斷，判錯不可能完全避免。所以每一個新出現的
合約，不論判定納入或排除，都要讓使用者看得到判定與理由（判錯就改名單、重新部署）。這個模組負責「新」：

    SQLite 檔 config.UNIVERSE_SEEN_DB_PATH（runtime/db/universe_seen.sqlite3），一列一個 symbol：
        symbol / base（判定用的 baseCurrency）/ first_seen_ms（第一次看到的時刻）/ 當時的判定（tradable、
        category、reason）/ initial（建檔時寫入的初始列 = 1）

比對對象是 market_static 的 TRADING、USDT 計價**原始清單（過濾之前）**，被排除的新合約也要通知。

  建檔（檔案不存在或表是空的）  目前清單全部寫成初始列、記一行 INFO，**不發**新上架通知
  之後每次標的池刷新成功        不在表裡的 = 新上架：寫入 → 一行 INFO 列出每個新 symbol 與判定 → 交給 on_new
  已經在表裡的                  不算新上架（含下架後重新上架的）；下架不通知、也不刪列

由 live.signal_feed.MarketUniverse 在每次 market_static 刷新成功之後呼叫 ListingTracker.observe()（A1 的標的池
刷新：啟動一次、之後約每 MARKET_STATIC_STALE_SECONDS 一次，所以偵測延遲約 1 小時以內）。

失敗處理（WBS §10 #14：監控路徑不可以擋住訊號路徑）：observe() **不拋例外**。
  * 開不了 / 讀不了資料庫 → ERROR（由 A4 告警），這一次不比對、不通知
  * 寫入失敗 → ERROR，ROLLBACK，**這一批不通知**；它們沒有寫進表裡，下一次刷新會再偵測到、再試。
    只有寫入成功之後才通知，所以已寫入的幣不會每小時重複通知
  * on_new 回呼拋例外 → ERROR、不往外拋（那一批已寫入，不會再通知；心跳的「新上架」欄位仍列得出來）
不帶 --push-tg 時 on_new 是 None：照樣建檔、照樣記日誌，只是沒有維運通知。

連線一律短用短關（每次 observe() / read_recent() 開一條、用完就關），不跨執行緒共用。寫入是 BEGIN IMMEDIATE /
COMMIT；用 SQLite 預設的 rollback journal（表很小、寫入只在建檔與有新幣時發生），讀取端（A4 心跳）唯讀開
（mode=ro），不建目錄、不寫任何東西。schema 版本記在 PRAGMA user_version。

模組名不撞標準庫、也不撞常見的第三方套件（WBS §10 #9）。
"""

import logging
import os
import pathlib
import sqlite3
import time
from dataclasses import dataclass

from live import config
from strategy import universe as universe_rules

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
TABLE = "seen"

_DDL = """
CREATE TABLE IF NOT EXISTS seen (
    symbol         TEXT    PRIMARY KEY CHECK (typeof(symbol) = 'text' AND symbol <> ''),
    base           TEXT    NOT NULL CHECK (typeof(base) = 'text'),
    first_seen_ms  INTEGER NOT NULL CHECK (typeof(first_seen_ms) = 'integer'),
    tradable       INTEGER NOT NULL CHECK (tradable IN (0, 1)),
    category       TEXT    NOT NULL CHECK (typeof(category) = 'text' AND category <> ''),
    reason         TEXT    NOT NULL CHECK (typeof(reason) = 'text'),
    initial        INTEGER NOT NULL CHECK (initial IN (0, 1))
)
"""
_COLUMNS = ("symbol", "base", "first_seen_ms", "tradable", "category", "reason", "initial")


class UniverseSeenError(Exception):
    """追蹤檔不存在（唯讀開時）、schema 版本不是本程式認得的、或不是本模組建的檔。"""


@dataclass(frozen=True)
class Listing:
    """表裡的一列（或剛偵測到、剛寫入的一個新上架）。"""
    symbol: str
    base: str
    first_seen_ms: int
    tradable: bool
    category: str
    reason: str
    initial: bool = False

    @property
    def verdict(self):
        """「納入」或「排除」。"""
        return universe_rules.VERDICT_INCLUDED if self.tradable else universe_rules.VERDICT_EXCLUDED

    @property
    def display_symbol(self):
        """symbol；base 與 symbol 第一段不同時加「（base）」（例：龙虾_USDT_PERP（CNLX））。"""
        return universe_rules.display_symbol(self.symbol, self.base)

    @property
    def judgement(self):
        """「納入（理由）」/「排除（理由）」。"""
        return "%s（%s）" % (self.verdict, self.reason)

    def describe(self):
        """一行：「<symbol（base）>：納入／排除（<理由>）」。"""
        return "%s：%s" % (self.display_symbol, self.judgement)


def _listing_from_classification(c, now_ms, initial):
    return Listing(symbol=c.symbol, base=c.base, first_seen_ms=int(now_ms), tradable=bool(c.tradable),
                   category=c.category, reason=c.reason, initial=bool(initial))


def _row_to_listing(row):
    symbol, base, first_seen_ms, tradable, category, reason, initial = row
    return Listing(symbol=symbol, base=base, first_seen_ms=int(first_seen_ms), tradable=bool(tradable),
                   category=category, reason=reason, initial=bool(initial))


# ============================== 開檔 ==============================
def _check_version(conn, path, *, allow_new):
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version == SCHEMA_VERSION:
        return version
    if version > SCHEMA_VERSION:
        raise UniverseSeenError("%s 的 schema 版本是 %d，比本程式認得的 %d 新；請用新版程式開啟"
                                % (path, version, SCHEMA_VERSION))
    existing = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")]
    if existing:
        raise UniverseSeenError("%s 沒有 schema 版本卻已經有東西（%s），不是 live.universe_seen 建的檔"
                                % (path, ", ".join(sorted(existing))))
    if not allow_new:
        raise UniverseSeenError("%s 還沒有建立新上架追蹤的表" % path)
    return version


def open_db(path=None, *, busy_timeout=None):
    """開（必要時建立）追蹤檔，回傳 sqlite3 連線（isolation_level=None，呼叫端負責關）。

    path          省略時取 config.UNIVERSE_SEEN_DB_PATH（呼叫當下才讀）；上層目錄不存在會自動建
    busy_timeout  資料庫被鎖住時最多等幾秒；省略時取 config.UNIVERSE_SEEN_BUSY_TIMEOUT_SECONDS
    """
    path = os.path.abspath(path if path is not None else config.UNIVERSE_SEEN_DB_PATH)
    timeout = config.UNIVERSE_SEEN_BUSY_TIMEOUT_SECONDS if busy_timeout is None else busy_timeout
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, timeout=float(timeout), isolation_level=None)
    try:
        _check_version(conn, path, allow_new=True)
        conn.execute("BEGIN IMMEDIATE")
        try:
            if _check_version(conn, path, allow_new=True) == 0:
                conn.execute(_DDL)
                conn.execute("PRAGMA user_version = %d" % SCHEMA_VERSION)
            conn.execute("COMMIT")
        except BaseException:
            _rollback(conn)
            raise
    except BaseException:
        conn.close()
        raise
    return conn


def open_readonly(path=None, *, busy_timeout=None):
    """唯讀（URI mode=ro）開追蹤檔並檢查 schema 版本。不存在 / 版本不符 → UniverseSeenError。不建目錄、不寫。"""
    path = os.path.abspath(path if path is not None else config.UNIVERSE_SEEN_DB_PATH)
    if not os.path.isfile(path):
        raise UniverseSeenError("%s 不存在（第一次標的池刷新時才會建立）" % path)
    timeout = config.UNIVERSE_SEEN_BUSY_TIMEOUT_SECONDS if busy_timeout is None else busy_timeout
    conn = sqlite3.connect(pathlib.Path(path).as_uri() + "?mode=ro", uri=True, timeout=float(timeout))
    try:
        _check_version(conn, path, allow_new=False)
    except BaseException:
        conn.close()
        raise
    return conn


def _rollback(conn):
    if conn.in_transaction:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass


def read_recent(since_ms, path=None, *, busy_timeout=None):
    """第一次看到的時刻 >= since_ms、而且**不是初始列**的 Listing（依時刻、symbol 排序）。唯讀開、用完就關。
    給 A4 心跳用；檔案不存在、版本不符 → UniverseSeenError，其他 sqlite3 錯誤原樣往外拋。"""
    conn = open_readonly(path, busy_timeout=busy_timeout)
    try:
        rows = conn.execute("SELECT %s FROM seen WHERE initial = 0 AND first_seen_ms >= ? "
                            "ORDER BY first_seen_ms, symbol" % ", ".join(_COLUMNS), (int(since_ms),)).fetchall()
    finally:
        conn.close()
    return [_row_to_listing(r) for r in rows]


def read_all(path=None, *, busy_timeout=None):
    """全部的列（依 symbol 排序），唯讀。給測試與人工檢查用。"""
    conn = open_readonly(path, busy_timeout=busy_timeout)
    try:
        rows = conn.execute("SELECT %s FROM seen ORDER BY symbol" % ", ".join(_COLUMNS)).fetchall()
    finally:
        conn.close()
    return [_row_to_listing(r) for r in rows]


# ============================== 追蹤 ==============================
class ListingTracker:
    """記住看過的合約；observe() 偵測新上架、寫入、交給 on_new。

    path          追蹤檔；省略時每次呼叫當下讀 config.UNIVERSE_SEEN_DB_PATH
    on_new        on_new(listings)：一次刷新偵測到的新上架（list[Listing]，寫入成功之後才呼叫）；None = 只記日誌
                  （不帶 --push-tg）。--push-tg 時是 A4 的 note_listings（不做 I/O、不拋例外）
    clock         回傳 epoch 秒，預設 time.time（first_seen_ms 用它）
    busy_timeout  省略時取 config.UNIVERSE_SEEN_BUSY_TIMEOUT_SECONDS
    """

    def __init__(self, *, path=None, on_new=None, clock=None, busy_timeout=None):
        self._path = path
        self.on_new = on_new
        self._clock = clock or time.time
        self._busy_timeout = busy_timeout
        self.stats = {"observed": 0, "bootstrapped": 0, "new": 0, "notified": 0, "db_errors": 0,
                      "callback_errors": 0}

    @property
    def path(self):
        return os.path.abspath(self._path if self._path is not None else config.UNIVERSE_SEEN_DB_PATH)

    def _open(self):
        return open_db(self.path, busy_timeout=self._busy_timeout)

    def observe(self, classifications):
        """一次標的池刷新的分類結果（symbol → strategy.universe.Classification，TRADING、USDT 計價、過濾之前）。

        回傳這次偵測到、而且已寫入的新上架（list[Listing]）；建檔、沒有新的、或任何失敗都回 []。不拋例外。"""
        try:
            return self._observe(classifications)
        except Exception:  # noqa: BLE001 —— 最後一道防線：監控路徑不可以擋住訊號路徑
            self.stats["db_errors"] += 1
            logger.exception("新上架追蹤發生未預期的例外，這一次不通知（標的池照常）")
            return []

    def _observe(self, classifications):
        self.stats["observed"] += 1
        classes = dict(classifications)
        now_ms = int(round(self._clock() * 1000))
        path = self.path
        try:
            conn = self._open()
        except Exception as e:  # noqa: BLE001
            self.stats["db_errors"] += 1
            logger.error("新上架追蹤：開不了 %s（%s: %s），這一次不比對、不通知；下一次標的池刷新再試",
                         path, type(e).__name__, e)
            return []
        try:
            try:
                known = {r[0] for r in conn.execute("SELECT symbol FROM seen")}
            except Exception as e:  # noqa: BLE001
                self.stats["db_errors"] += 1
                logger.error("新上架追蹤：讀不了 %s（%s: %s），這一次不比對、不通知；下一次標的池刷新再試",
                             path, type(e).__name__, e)
                return []
            initial = not known
            fresh = [c for sym, c in sorted(classes.items()) if sym not in known]
            if not fresh:
                return []
            rows = [_listing_from_classification(c, now_ms, initial) for c in fresh]
            try:
                self._insert(conn, rows)
            except Exception as e:  # noqa: BLE001
                self.stats["db_errors"] += 1
                if initial:
                    what = "建檔沒有完成（%d 個初始列）" % len(rows)
                else:
                    what = "這一批 %d 個新上架不通知（%s）" % (len(rows), "、".join(r.symbol for r in rows))
                logger.error("新上架追蹤：寫入 %s 失敗（%s: %s），%s；下一次標的池刷新會再偵測到、再試",
                             path, type(e).__name__, e, what)
                return []
        finally:
            conn.close()

        if initial:
            self.stats["bootstrapped"] += len(rows)
            logger.info("新上架追蹤：建檔 %s，寫入目前 TRADING %s 共 %d 個（初始列，不發新上架通知）",
                        path, universe_rules.QUOTE, len(rows))
            return []
        self.stats["new"] += len(rows)
        included = sum(1 for r in rows if r.tradable)
        logger.info("新上架 %d 個（納入 %d、排除 %d）：%s", len(rows), included, len(rows) - included,
                    "；".join(r.describe() for r in rows))
        if self.on_new is not None:
            try:
                self.on_new(list(rows))
                self.stats["notified"] += len(rows)
            except Exception:  # noqa: BLE001
                self.stats["callback_errors"] += 1
                logger.exception("新上架通知的回呼拋出例外（這一批 %d 個已寫入，不會再通知；標的池照常）", len(rows))
        return rows

    @staticmethod
    def _insert(conn, rows):
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(
                "INSERT INTO seen (%s) VALUES (?, ?, ?, ?, ?, ?, ?)" % ", ".join(_COLUMNS),
                [(r.symbol, r.base, r.first_seen_ms, int(r.tradable), r.category, r.reason, int(r.initial))
                 for r in rows])
            conn.execute("COMMIT")
        except BaseException:
            _rollback(conn)
            raise
