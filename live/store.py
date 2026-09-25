# -*- coding: utf-8 -*-
"""
live.store — A 頻道的持久層：訊號表 + 名目部位表（SQLite）
==========================================================
A 頻道發出進場訊號之後，要一路追蹤那筆「名目部位」直到止盈或止損，才發得出出場訊息。
這個追蹤狀態**不能只放在記憶體裡**：paper 階段每改一次程式就重啟一次，狀態一丟，訂閱者
就會收到進場訊號、卻永遠等不到出場訊息 —— 對收費產品來說這是最難辯解的故障。

所以這支只做一件事：把「發過哪些訊號、哪些名目部位還開著」寫進磁碟，重啟後原樣讀回來。
出場判定（A3）、事件發布（A2）、交易所互動、TG 推播都不在這裡；dry run 也不用它
（dry run 維持用 state/dryrun_state.json）。交易表、費用表、帳戶快照延後到之後的任務。

用法（A3 直接呼叫；import 本模組沒有任何副作用，不開檔也不建目錄）：

    from live import config, store

    db = store.open_store()                       # 預設 config.LIVE_DB_PATH，目錄自己建
    db.record_entry(signal_id=..., user_id=config.STRATEGY_USER_ID, strategy="s4",
                    symbol="XXX_USDT", side="short", bar_open_ms=..., signal_price=...,
                    take_profit_price=..., stop_loss_price=..., features={...})
    ...發布事件成功之後...
    db.mark_published(signal_id)
    ...判定出場之後...
    db.close_position(signal_id, exit_reason="take_profit", exit_price=..., closed_ms=...)

    # 重啟復原
    db.open_positions(user_id=config.STRATEGY_USER_ID)          # 還開著的名目部位
    db.unpublished_signals(user_id=config.STRATEGY_USER_ID)     # 寫了庫、還沒發布的訊號

檔名刻意叫 store 而不是 sqlite3 / db：跟標準庫同名的模組只要 live/ 落到 sys.path 上就會把
標準庫遮蔽掉（B2 的 live/http.py、B3 的 logsetup 都是同一個理由）。

──────────────────────────────────────────────────────────────────────
兩張表（DDL 全文見下方 _DDL）
──────────────────────────────────────────────────────────────────────
  signals    每一筆發出的進場訊號。signal_id 是主鍵（格式由 A3 決定，這裡只保證唯一）。
             features_json 存判定特徵（稽核用）：s4 是 ret2h / volr / cpos / turn，s5 是
             漲幅 / 量倍數 / 爆量分支……兩個策略的欄位不同，所以存 JSON 文字，不拆欄位。
             published_ms 可為空：A3 先寫庫、再發事件、再標記已發布；寫庫後發布前當掉的話，
             重啟時 unpublished_signals() 會把它找出來，讓 A3 補發或判斷。
  positions  每一筆訊號對應的策略層名目部位，與 signals 一對一（signal_id 是主鍵也是外鍵）。
             status 只有 open / closed；closed 時出場原因、出場價、平倉時刻三者必須齊全，
             open 時三者必須全空（表層 CHECK 擋住半套狀態）。

核心約束：同一個 (user_id, strategy, symbol) **同時最多一筆 open**，用部分唯一索引
`... ON positions (user_id, strategy, symbol) WHERE status = 'open'` 由資料庫自己擋。
strategy 在鍵裡：使用者 2026-09-25 決定 s4、s5 都上線、同一個幣兩個策略可以同時持倉，所以
只看 symbol 會把合法的第二筆擋掉。已平倉的紀錄不受限制（同一個鍵可以有任意多筆 closed）。

user_id 不可以是 NULL：NULL 在 SQLite 的唯一索引裡彼此不算重複，user_id 用 NULL 代表策略層
的話，同一個 (strategy, symbol) 可以開出任意多筆 open，上面那條約束形同虛設。A 頻道的策略層
紀錄一律用保留值 config.STRATEGY_USER_ID；欄位是 NOT NULL，本模組也會在寫入前擋下 None。

型別：所有時間一律 UTC epoch 毫秒整數（台北時間的轉換是呈現層的事）；價格一律 REAL。
表層 CHECK (typeof(...) = 'real' / 'integer') 讓資料庫本身拒收錯型別的值，本模組在寫入前
也會先驗：價格必須是有限的正實數（bool、字串、NaN、inf 都拒收）；時間必須是整數且落在
[MIN_EPOCH_MS, MAX_EPOCH_MS) —— 這個範圍擋的是「把秒當成毫秒傳進來」這種最常見的單位錯誤。
做空時另外檢查 止盈價 < 訊號價 < 止損價，擋掉止盈止損兩個參數傳反。

沒有用 STRICT table：STRICT 要 SQLite 3.37+，雲端主機的系統 SQLite 版本還沒定（Ubuntu 20.04
內建 3.31）。typeof 的 CHECK 在所有版本上效果相同。

合法值的來源：
  strategy     live.config.STRATEGIES。策略名單只有那一份（A2 立的規則，tests/test_bus.py 會
               掃描 live/ 底下有沒有第二份）。每次驗證都在呼叫當下讀 config.STRATEGIES，不在
               import 時綁死 —— 跟 live.signal_events 建構事件時的讀法一致，改了名單，事件與
               持久層同時跟著變，不會出現「事件收、資料庫拒」的落差。
  side         本模組的 SIDES
  exit_reason  本模組的 EXIT_REASONS
這幾個都只在 Python 端驗，不寫成表層 CHECK：SQLite 不能 ALTER 一條 CHECK，日後加一個策略或
一種出場原因就得整張表重建。本模組是唯一的寫入者，在這裡擋效果相同。status 的兩個值是
schema 本身的語意，所以寫在表層。

──────────────────────────────────────────────────────────────────────
判定特徵（features → features_json）
──────────────────────────────────────────────────────────────────────
  ・features 收任何 Mapping（dict，或 A2 的 EntryEvent.features 那種 types.MappingProxyType），
    內部先轉成一般 dict 再存。鍵必須是非空字串；值只收純量：None / bool / str / 整數 / 實數，
    numpy 純量（含 numpy.bool_）轉成對應的 Python 型別。list、dict、陣列、自訂物件一律拒收
    （TypeError）—— 巢狀容器不是判定特徵；非字串的鍵存成 JSON 會被悄悄改成字串，讀回來就跟
    寫進去的不一樣。
  ・±inf 與 NaN **照樣收下、原樣讀回**（讀回來都是 float）。它們是真實會出現的值：s4 的 volr
    是「本根量 ÷ 前 24 小時每根均量」，前 24 小時零成交時就是 inf，而訊號照樣成立。持久層如果
    拒收，A3 就寫不進資料庫，一筆真實訊號會卡住發不出去 —— 那正是「交易系統不能靜默失敗」
    要防的事。（A2 的 EntryEvent 建構時已經把 NaN 轉成 None；直接呼叫本模組傳 NaN 也照樣存。）
  ・所以 **features_json 是 Python 風格的 JSON**：json.dumps(allow_nan=True) 把 inf / -inf / NaN
    寫成 Infinity / -Infinity / NaN 三個非標準記號。讀取端要用 Python 的 json（預設就認得），或
    其他允許這些記號的解析器；嚴格的 JSON 解析器會直接失敗。SQL 端也不要依賴它：SQLite 3.50
    實測 json_valid() 對這種內容回 0、json_extract() 讀得出 Infinity 卻把 NaN 讀成 NULL，沒有
    JSON5 支援的舊版（3.42 之前）直接報錯。要看特徵請用 get_signal() / unpublished_signals()，
    回傳的 features 已經用 Python 的 json 解回 dict。
  ・features_json 一律是**純 ASCII**（json.dumps(ensure_ascii=True)）：中文等非 ASCII 字元存成
    JSON 的跳脫序列（反斜線 u 加 4 位十六進位），讀回時 json.loads 還原成原字元。理由：鍵或字串
    值裡可能有 lone surrogate（例如 surrogateescape 解碼出來的字元），A2 的 EntryEvent 照收；
    如果保留原字元，sqlite3 綁定參數時要把文字編成 UTF-8，lone surrogate 在這一步就會拋
    UnicodeEncodeError —— 又是「A2 收、store 拒」、訊號卡住。跳脫之後整段都是 ASCII，編碼這一步
    不可能失敗；而且同樣的內容永遠只有一種落地文字（刻意不做「編碼失敗才改用跳脫」的雙軌）。
    代價：用 sqlite 命令列直接看這一欄時，中文是跳脫序列；get_signal() 讀回的內容不受影響。
    注意這只涵蓋 features_json：signal_id / symbol 等一般 TEXT 欄位照原字元存，含 lone surrogate
    的話寫入時仍會拋 UnicodeEncodeError（交易所的 symbol 與 A3 產生的 signal_id 不會有這種字元）。

──────────────────────────────────────────────────────────────────────
交易與原子性
──────────────────────────────────────────────────────────────────────
連線用 isolation_level=None 開，交易完全由本模組明確下 BEGIN IMMEDIATE / COMMIT / ROLLBACK。
不用標準庫的預設模式：預設會在 DML 前「隱式」開交易，而隱式交易的規則在不同 Python 版本
不一樣（3.6 改過 DDL 的行為、3.12 新增 autocommit 屬性），executescript() 還會先偷偷
COMMIT 一次。原子性要靠看得見的 BEGIN 與 ROLLBACK 保證，不能靠推論標準庫的行為。
（也不用 3.12 才有的 autocommit=True 參數：雲端主機的 Python 版本同樣還沒定。）

record_entry() 在一個交易裡依序寫訊號、再開名目部位。第二步因為任何原因失敗（違反唯一
open、磁碟錯誤、甚至不是 sqlite 的例外），整個交易 ROLLBACK，訊號表也不會留下那一筆 ——
不會出現「有訊號、沒部位」的孤兒紀錄。唯一 open 由資料庫的部分唯一索引擋下，本模組只負責
把 sqlite3.IntegrityError 翻譯成說得出是哪個 (user_id, strategy, symbol) 的例外。

每個寫入函式都在回傳前 COMMIT 完畢，呼叫端不需要、也不應該自己管交易。
BEGIN IMMEDIATE 一開始就拿寫入鎖，交易裡先查再寫之間不會被別的連線插隊。

──────────────────────────────────────────────────────────────────────
錯誤語意（全部是 StoreError 的子類別，除了參數驗證）
──────────────────────────────────────────────────────────────────────
  OpenPositionExistsError  record_entry()：同 (user_id, strategy, symbol) 已有一筆 open。
                           帶 user_id / strategy / symbol / existing_signal_id 屬性。
  DuplicateSignalError     record_entry()：signal_id 已經存在。
  PositionNotOpenError     close_position()：目標不存在（reason="missing"）或已平倉
                           （reason="closed"）。**平倉一律用例外回報，不用回傳值**：
                           對不存在或已平倉的部位平倉不可能是正常流程，靜默成功只會把
                           A3 的邏輯錯誤藏起來。
  SignalNotPendingError    mark_published()：目標不存在（reason="missing"）或早已標記
                           （reason="published"，原本的 published_ms 保留不動）。
  SchemaVersionError       open_store()：資料庫是更新版本的 schema，或不是本模組建的。
  StoreError               其他：WAL / synchronous 設不上、對已關閉的 store 操作。
  TypeError / ValueError   參數驗證失敗（型別錯 / 值不合法）。驗證全部在開交易之前做，
                           失敗時資料庫完全沒被碰過。

失敗的寫入一律 ROLLBACK，資料庫停在呼叫前的狀態，store 物件可以繼續用。

──────────────────────────────────────────────────────────────────────
耐久性
──────────────────────────────────────────────────────────────────────
開啟時設 journal_mode=WAL 與 synchronous=FULL：寫入量很小（一天幾筆到幾十筆訊號），耐久性
優先於速度 —— COMMIT 回傳時資料已經 fsync 進 WAL 檔，行程當場被砍、主機斷電都不會丟。
兩個設定都會回讀確認，設不上就在 open_store() 當下拋 StoreError，不要跑起來之後才發現。
foreign_keys 也在這裡打開（SQLite 預設關閉、每條連線各自設定）。

WAL 模式會在資料庫旁邊產生 -wal 與 -shm 兩個檔，最後一條連線 close() 時會併回主檔並刪掉。
行程當掉留下的 -wal 不要手動刪：下次開啟時 SQLite 會自己用它復原已提交的交易。

──────────────────────────────────────────────────────────────────────
假設：一個資料庫檔只能有一個寫入行程
──────────────────────────────────────────────────────────────────────
跟 B3 的日誌檔同理：**同一個資料庫檔同一時間只能有一個行程在寫**（A 頻道只有一個實例）。
BEGIN IMMEDIATE 讓並行寫入不至於寫壞資料庫，但兩個實例同時追蹤同一批名目部位，出場訊息就
會重複發 —— 那是業務層的錯，這裡擋不住。唯讀檢視（sqlite3 命令列、另一支分析腳本）沒問題，
WAL 模式下讀不會卡住寫。

另外，一個 Store 只能在建立它的那個執行緒裡用（標準庫 sqlite3 預設的 check_same_thread，
跨執行緒使用會直接拋 ProgrammingError）。要不要跨執行緒共用是 A3 的架構決定，不在這裡。

──────────────────────────────────────────────────────────────────────
schema 版本
──────────────────────────────────────────────────────────────────────
版本記在 PRAGMA user_version（資料庫檔頭的一個整數，不需要額外的表）。open_store() 的規則：
  0 而且資料庫是空的      新檔：在同一個交易裡建表、建索引、把版本設成 SCHEMA_VERSION
  == SCHEMA_VERSION       什麼都不做（所以可以重複呼叫，冪等）
  > SCHEMA_VERSION        拋 SchemaVersionError：這是新版程式建的，舊程式不可以亂寫
  0 但資料庫裡已經有東西  拋 SchemaVersionError：不是本模組建的檔，不要在上面亂建表
日後要改 schema：SCHEMA_VERSION + 1，在 _init_schema() 加一段「舊版本 -> 新版本」的遷移，
跟版本號更新放在同一個交易裡。
"""

import json
import math
import numbers
import os
import sqlite3
import time
from collections.abc import Mapping
from contextlib import contextmanager

from live import config

# 目前的 schema 版本（PRAGMA user_version）。改了 _DDL 就要加 1 並寫遷移。
SCHEMA_VERSION = 1

# side / exit_reason 的合法值。只在本模組驗、不寫成表層 CHECK，理由見模組 docstring。
# strategy 的合法值不在這裡：一律在呼叫當下讀 config.STRATEGIES（策略名單只有那一份）。
SIDES = ("short",)
EXIT_REASONS = ("take_profit", "stop_loss")

# positions.status 的兩個值（表層 CHECK 的那兩個）。給呼叫端比對用，例如 p["status"] == STATUS_OPEN；
# 本模組的 SQL 直接寫字面值，因為它們就是 schema 的一部分。
STATUS_OPEN = "open"
STATUS_CLOSED = "closed"

# epoch 毫秒的合理範圍：[2001-09-09, 西元 5138 年)。
# 下限擋「把秒當毫秒」（現在的秒數約 1.7e9，遠小於 1e12）；上限擋「把微秒 / 奈秒當毫秒」。
MIN_EPOCH_MS = 10 ** 12
MAX_EPOCH_MS = 10 ** 14

# 本模組建立的物件。只建這些；schema 檢查與測試都以這份為準。
TABLES = ("signals", "positions")
OPEN_POSITION_INDEX = "positions_one_open_per_key"

_DDL = (
    """
    CREATE TABLE IF NOT EXISTS signals (
        signal_id          TEXT    NOT NULL PRIMARY KEY
                                   CHECK (typeof(signal_id) = 'text' AND signal_id <> ''),
        user_id            TEXT    NOT NULL CHECK (typeof(user_id) = 'text' AND user_id <> ''),
        strategy           TEXT    NOT NULL CHECK (typeof(strategy) = 'text' AND strategy <> ''),
        symbol             TEXT    NOT NULL CHECK (typeof(symbol) = 'text' AND symbol <> ''),
        side               TEXT    NOT NULL CHECK (typeof(side) = 'text' AND side <> ''),
        bar_open_ms        INTEGER NOT NULL CHECK (typeof(bar_open_ms) = 'integer'),
        signal_price       REAL    NOT NULL CHECK (typeof(signal_price) = 'real'),
        take_profit_price  REAL    NOT NULL CHECK (typeof(take_profit_price) = 'real'),
        stop_loss_price    REAL    NOT NULL CHECK (typeof(stop_loss_price) = 'real'),
        features_json      TEXT    NOT NULL CHECK (typeof(features_json) = 'text'),
        created_ms         INTEGER NOT NULL CHECK (typeof(created_ms) = 'integer'),
        published_ms       INTEGER CHECK (published_ms IS NULL OR typeof(published_ms) = 'integer')
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
        signal_id          TEXT    NOT NULL PRIMARY KEY REFERENCES signals (signal_id),
        user_id            TEXT    NOT NULL CHECK (typeof(user_id) = 'text' AND user_id <> ''),
        strategy           TEXT    NOT NULL CHECK (typeof(strategy) = 'text' AND strategy <> ''),
        symbol             TEXT    NOT NULL CHECK (typeof(symbol) = 'text' AND symbol <> ''),
        entry_price        REAL    NOT NULL CHECK (typeof(entry_price) = 'real'),
        take_profit_price  REAL    NOT NULL CHECK (typeof(take_profit_price) = 'real'),
        stop_loss_price    REAL    NOT NULL CHECK (typeof(stop_loss_price) = 'real'),
        opened_ms          INTEGER NOT NULL CHECK (typeof(opened_ms) = 'integer'),
        status             TEXT    NOT NULL CHECK (status IN ('open', 'closed')),
        exit_reason        TEXT    CHECK (exit_reason IS NULL OR typeof(exit_reason) = 'text'),
        exit_price         REAL    CHECK (exit_price IS NULL OR typeof(exit_price) = 'real'),
        closed_ms          INTEGER CHECK (closed_ms IS NULL OR typeof(closed_ms) = 'integer'),
        CHECK ((status = 'open'   AND exit_reason IS NULL     AND exit_price IS NULL
                                  AND closed_ms IS NULL)
            OR (status = 'closed' AND exit_reason IS NOT NULL AND exit_price IS NOT NULL
                                  AND closed_ms IS NOT NULL))
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS positions_one_open_per_key
        ON positions (user_id, strategy, symbol) WHERE status = 'open'
    """,
)


# ============================== 例外 ==============================
class StoreError(Exception):
    """本模組所有自訂例外的基底。"""


class SchemaVersionError(StoreError):
    """資料庫的 schema 版本不是本程式能處理的（更新版，或根本不是本模組建的檔）。"""


class DuplicateSignalError(StoreError):
    """record_entry() 的 signal_id 已經存在。"""

    def __init__(self, signal_id):
        self.signal_id = signal_id
        super().__init__("signal_id %r 已經存在，不可重複寫入（訊號表以 signal_id 為唯一鍵）"
                         % (signal_id,))


class OpenPositionExistsError(StoreError):
    """record_entry() 違反「同一個 (user_id, strategy, symbol) 同時只能有一筆 open」。"""

    def __init__(self, user_id, strategy, symbol, existing_signal_id, new_signal_id):
        self.user_id = user_id
        self.strategy = strategy
        self.symbol = symbol
        self.existing_signal_id = existing_signal_id
        self.new_signal_id = new_signal_id
        super().__init__(
            "(user_id=%r, strategy=%r, symbol=%r) 已經有一筆未平倉的名目部位（signal_id=%r），"
            "不可再開第二筆（signal_id=%r）；本次寫入整筆取消，訊號表也沒有留下這一筆"
            % (user_id, strategy, symbol, existing_signal_id, new_signal_id)
        )


class PositionNotOpenError(StoreError):
    """close_position() 的目標不是一筆 open 的名目部位。reason 是 "missing" 或 "closed"。"""

    def __init__(self, signal_id, reason):
        self.signal_id = signal_id
        self.reason = reason
        what = "不存在" if reason == "missing" else "早已平倉"
        super().__init__("signal_id %r 的名目部位%s，無法平倉；資料庫沒有任何變更"
                         % (signal_id, what))


class SignalNotPendingError(StoreError):
    """mark_published() 的目標不是一筆「已寫入、未發布」的訊號。reason 是 "missing" 或 "published"。"""

    def __init__(self, signal_id, reason):
        self.signal_id = signal_id
        self.reason = reason
        what = "不存在" if reason == "missing" else "早已標記為已發布（原本的發布時刻保留不動）"
        super().__init__("signal_id %r 的訊號%s；資料庫沒有任何變更" % (signal_id, what))


# ============================== 參數驗證 ==============================
def _now_ms():
    """預設時鐘：目前的 UTC epoch 毫秒（time.time_ns() 與時區無關）。"""
    return time.time_ns() // 1_000_000


def _text(name, value):
    if not isinstance(value, str):
        raise TypeError("%s 必須是 str，收到 %s" % (name, type(value).__name__))
    if not value or value != value.strip():
        raise ValueError("%s 不可為空字串，也不可前後帶空白：%r" % (name, value))
    return value


def _user_id(value, name="user_id"):
    if value is None:
        raise TypeError(
            "%s 不可以是 None：NULL 在唯一索引裡彼此不算重複，會讓「同 (user_id, strategy, symbol) "
            "只能一筆 open」失效。A 頻道策略層紀錄請用 config.STRATEGY_USER_ID（%r）"
            % (name, config.STRATEGY_USER_ID)
        )
    return _text(name, value)


def _choice(name, value, allowed):
    if not isinstance(value, str) or value not in allowed:
        raise ValueError("%s 必須是 %s 之一，收到 %r" % (name, "/".join(allowed), value))
    return value


def _price(name, value):
    """有限的正實數，回傳 float。bool 與字串一律拒收（字串就算長得像數字也不收）。"""
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise TypeError("%s 必須是實數（int / float），收到 %s" % (name, type(value).__name__))
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("%s 必須是有限的正數，收到 %r" % (name, value))
    return value


def _epoch_ms(name, value):
    """UTC epoch 毫秒整數。float 一律拒收（就算是 1.7e12 這種整數值也不收，免得混進小數）。"""
    if isinstance(value, bool) or not isinstance(value, numbers.Integral):
        raise TypeError("%s 必須是整數的 UTC epoch 毫秒，收到 %s" % (name, type(value).__name__))
    value = int(value)
    if not MIN_EPOCH_MS <= value < MAX_EPOCH_MS:
        raise ValueError(
            "%s=%d 不像 UTC epoch 毫秒（合理範圍 [%d, %d)）；常見原因是把秒或微秒當成毫秒傳進來"
            % (name, value, MIN_EPOCH_MS, MAX_EPOCH_MS)
        )
    return value


def _feature_value(key, value):
    """features 的一個值 -> JSON 純量（None / bool / str / int / float）。±inf 與 NaN 原樣保留。

    A3 的特徵多半直接從 pandas 取出來。numpy 的整數 / 浮點數有登記成 numbers.Integral /
    numbers.Real，照一般數字處理；numpy.bool_ 兩者都不是，用鴨子型別認 0 維的 numpy 純量
    （本模組不 import numpy），陣列（shape 不是 ()）照樣拒收。
    """
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        try:
            return float(value)                 # inf / -inf / NaN 照樣是 float，不擋
        except OverflowError:
            raise ValueError("features[%r] 超出 float 範圍：%r" % (key, value)) from None
    item = getattr(value, "item", None)
    if callable(item) and getattr(value, "shape", None) == ():
        scalar = item()
        if scalar is None or isinstance(scalar, (bool, int, float, str)):
            return scalar
    raise TypeError("features[%r] 只收純量（None / bool / str / 整數 / 實數），收到 %s"
                    % (key, type(value).__name__))


def _features_json(features):
    """features（任何 Mapping）-> Python 風格的 JSON 文字。格式與讀回語意見模組 docstring。"""
    if not isinstance(features, Mapping):
        raise TypeError("features 必須是 dict（或其他 Mapping），收到 %s" % type(features).__name__)
    plain = {}
    for key, value in features.items():
        if not isinstance(key, str):
            raise TypeError("features 的鍵必須是字串，收到 %r" % (key,))
        if not key:
            raise ValueError("features 的鍵不可為空字串")
        plain[key] = _feature_value(key, value)
    # allow_nan=True：±inf / NaN 寫成 Infinity / -Infinity / NaN，讀回時 json.loads 原樣還原成 float
    # ensure_ascii=True：輸出純 ASCII，含 lone surrogate 的鍵 / 值也存得進 SQLite（見模組 docstring）
    return json.dumps(plain, ensure_ascii=True, sort_keys=True, allow_nan=True)


def _check_levels(side, signal_price, take_profit_price, stop_loss_price):
    """止盈 / 止損相對訊號價的方向。目前只有做空：止盈在下、止損在上。"""
    if side == "short" and not take_profit_price < signal_price < stop_loss_price:
        raise ValueError(
            "做空必須 止盈價 < 訊號價 < 止損價，收到 止盈 %r / 訊號 %r / 止損 %r"
            "（止盈止損是不是傳反了？）" % (take_profit_price, signal_price, stop_loss_price)
        )


# ============================== SQL 片段 ==============================
def _insert_signal(conn, row):
    conn.execute(
        "INSERT INTO signals (signal_id, user_id, strategy, symbol, side, bar_open_ms, "
        "signal_price, take_profit_price, stop_loss_price, features_json, created_ms, "
        "published_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        (row["signal_id"], row["user_id"], row["strategy"], row["symbol"], row["side"],
         row["bar_open_ms"], row["signal_price"], row["take_profit_price"],
         row["stop_loss_price"], row["features_json"], row["created_ms"]),
    )


def _insert_position(conn, row):
    conn.execute(
        "INSERT INTO positions (signal_id, user_id, strategy, symbol, entry_price, "
        "take_profit_price, stop_loss_price, opened_ms, status, exit_reason, exit_price, "
        "closed_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', NULL, NULL, NULL)",
        (row["signal_id"], row["user_id"], row["strategy"], row["symbol"], row["signal_price"],
         row["take_profit_price"], row["stop_loss_price"], row["opened_ms"]),
    )


def _signal_dict(row):
    d = dict(row)
    d["features"] = json.loads(d.pop("features_json"))
    return d


def _filters(strategy, user_id):
    """把 open_positions() / unpublished_signals() 的選用過濾條件轉成 SQL 片段。"""
    clauses, params = [], []
    if strategy is not None:
        clauses.append("strategy = ?")
        params.append(_choice("strategy", strategy, config.STRATEGIES))
    if user_id is not None:
        clauses.append("user_id = ?")
        params.append(_user_id(user_id))
    return "".join(" AND " + c for c in clauses), params


# ============================== 開啟與 schema ==============================
def _apply_pragmas(conn):
    """WAL + synchronous=FULL + foreign_keys，並回讀確認真的設上了。"""
    mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    if str(mode).lower() != "wal":
        raise StoreError("無法把資料庫切到 WAL 模式（目前是 %r）；檔案系統可能不支援，"
                         "請把 LIVE_DB_PATH 放在本機磁碟上" % (mode,))
    conn.execute("PRAGMA synchronous=FULL")
    if conn.execute("PRAGMA synchronous").fetchone()[0] != 2:        # 2 = FULL
        raise StoreError("無法把 synchronous 設成 FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise StoreError("無法開啟 foreign_keys（這個 SQLite 可能編譯時關掉了外鍵支援）")


def _check_version(conn, path):
    """讀 schema 版本；更新版或不是本模組建的檔就拋 SchemaVersionError。只讀，不改任何東西。

    open_store() 在切 WAL 之前先呼叫一次（拒絕開啟的檔案連 journal mode 都不可以被改掉），
    _init_schema() 在寫入鎖裡再呼叫一次（兩次之間檔案可能被別人動過）。
    """
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version > SCHEMA_VERSION:
        raise SchemaVersionError(
            "%s 的 schema 版本是 %d，比本程式認得的 %d 新；請用新版程式開啟，舊程式不寫入"
            % (path, version, SCHEMA_VERSION))
    if version == 0:
        existing = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")]
        if existing:
            raise SchemaVersionError(
                "%s 沒有 schema 版本卻已經有東西（%s），不是 live.store 建的資料庫，不在上面建表"
                % (path, ", ".join(sorted(existing))))
    elif version != SCHEMA_VERSION:
        # 0 < version < SCHEMA_VERSION：日後的遷移寫在 _init_schema()。目前只有版本 1，走不到。
        raise SchemaVersionError("%s 的 schema 版本 %d 沒有對應的遷移" % (path, version))
    return version


def _init_schema(conn, path):
    """建表、建索引、記版本；整段在一個交易裡，冪等。規則見模組 docstring「schema 版本」。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        if _check_version(conn, path) == 0:
            for ddl in _DDL:
                conn.execute(ddl)
            conn.execute("PRAGMA user_version = %d" % SCHEMA_VERSION)
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass                    # 原本的例外比較重要；呼叫端接著會關閉連線
        raise


def open_store(path=None, *, clock=None):
    """開啟（必要時建立）資料庫並回傳 Store。可重複呼叫，已經建好的資料庫不會被動到。

    path   資料庫檔路徑；省略時取 config.LIVE_DB_PATH（呼叫當下才讀）。上層目錄不存在會自動建。
           傳路徑是給測試用暫存目錄的；正式程式照 config 走，不要傳。
    clock  回傳 UTC epoch 毫秒整數的函式，寫入函式沒給時刻時用它。省略時用系統時鐘。
           給測試注入假時鐘用，不需要真的 sleep。

    WAL / synchronous 設不上、schema 版本不對時拋例外，連線會先關好，不留半開的檔案。
    """
    path = os.path.abspath(path if path is not None else config.LIVE_DB_PATH)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)
    try:
        conn.row_factory = sqlite3.Row
        _check_version(conn, path)
        _apply_pragmas(conn)
        _init_schema(conn, path)
    except BaseException:
        conn.close()
        raise
    return Store(conn, path, clock if clock is not None else _now_ms)


# ============================== Store ==============================
class Store:
    """一條資料庫連線 + A3 會用到的存取函式。請用 open_store() 取得，不要直接建構。

    可以當 context manager 用（離開時 close）。寫入函式全部在回傳前 COMMIT 完畢；
    讀取函式回傳的是 plain dict（價格是 float、時間是 int），改它不會影響資料庫。
    """

    def __init__(self, conn, path, clock):
        self._conn = conn
        self.path = path
        self._clock = clock

    def __repr__(self):
        return "<live.store.Store %s%s>" % (self.path, "" if self._conn else " (closed)")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def close(self):
        """關閉連線。重複呼叫沒關係。Windows 上沒關的話暫存目錄刪不掉。"""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ---------- 內部 ----------
    def _db(self):
        if self._conn is None:
            raise StoreError("store 已經關閉：%s" % self.path)
        return self._conn

    def _now(self, name):
        return _epoch_ms("%s（來自 clock()）" % name, self._clock())

    @contextmanager
    def _write_transaction(self):
        """BEGIN IMMEDIATE ... COMMIT；區塊內任何例外（含 COMMIT 本身失敗）都 ROLLBACK 後原樣拋出。"""
        conn = self._db()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    # 原本的例外比較重要。連 ROLLBACK 都失敗時，未提交的內容會在連線關閉時丟棄。
                    pass
            raise

    @staticmethod
    def _fetch_position(conn, signal_id):
        row = conn.execute("SELECT * FROM positions WHERE signal_id = ?", (signal_id,)).fetchone()
        return None if row is None else dict(row)

    # ---------- 寫入 ----------
    def record_entry(self, *, signal_id, user_id, strategy, symbol, side, bar_open_ms,
                     signal_price, take_profit_price, stop_loss_price, features,
                     created_ms=None, opened_ms=None):
        """寫入一筆進場訊號，並為它開一筆 open 的名目部位 —— 同一個交易，要嘛兩筆都在、要嘛都不在。

        signal_id          訊號編號，唯一（格式由 A3 決定）
        user_id            A 頻道策略層一律傳 config.STRATEGY_USER_ID；不可以是 None
        strategy           config.STRATEGIES 之一（呼叫當下讀）
        symbol             交易對，例如 "XXX_USDT"
        side               SIDES 之一（目前只有 "short"）
        bar_open_ms        訊號所在 K 棒的開盤時刻（UTC epoch 毫秒）
        signal_price       訊號價，也就是名目部位的進場價（R-2：A 頻道以訊號價計）
        take_profit_price  止盈價；做空時必須低於訊號價
        stop_loss_price    止損價；做空時必須高於訊號價
        features           判定特徵（稽核用），任何 Mapping（含 EntryEvent.features）；值只收
                           純量，numpy 純量可以，±inf / NaN 照樣存、原樣讀回；存成純 ASCII 的
                           JSON，含 lone surrogate 的鍵 / 值也存得進去（見模組 docstring）
        created_ms         寫入時刻；省略時用 clock()
        opened_ms          名目部位的開倉時刻；省略時等於 created_ms

        回傳新開的名目部位（dict，同 get_position()）。
        拋出：OpenPositionExistsError（同鍵已有 open）、DuplicateSignalError（signal_id 已存在）、
              TypeError / ValueError（參數不合法，資料庫完全沒被碰過）。
        任何失敗都整筆 ROLLBACK，訊號表不會留下孤兒。
        """
        row = {
            "signal_id": _text("signal_id", signal_id),
            "user_id": _user_id(user_id),
            "strategy": _choice("strategy", strategy, config.STRATEGIES),
            "symbol": _text("symbol", symbol),
            "side": _choice("side", side, SIDES),
            "bar_open_ms": _epoch_ms("bar_open_ms", bar_open_ms),
            "signal_price": _price("signal_price", signal_price),
            "take_profit_price": _price("take_profit_price", take_profit_price),
            "stop_loss_price": _price("stop_loss_price", stop_loss_price),
            "features_json": _features_json(features),
        }
        _check_levels(row["side"], row["signal_price"], row["take_profit_price"],
                      row["stop_loss_price"])
        row["created_ms"] = (_epoch_ms("created_ms", created_ms) if created_ms is not None
                             else self._now("created_ms"))
        row["opened_ms"] = (_epoch_ms("opened_ms", opened_ms) if opened_ms is not None
                            else row["created_ms"])

        with self._write_transaction() as conn:
            if conn.execute("SELECT 1 FROM signals WHERE signal_id = ?",
                            (row["signal_id"],)).fetchone():
                raise DuplicateSignalError(row["signal_id"])
            _insert_signal(conn, row)
            try:
                _insert_position(conn, row)
            except sqlite3.IntegrityError as e:
                # 違反部分唯一索引時，交易還開著（SQLite 只撤銷失敗的那一句），查得到佔住鍵的那筆
                hit = conn.execute(
                    "SELECT signal_id FROM positions WHERE user_id = ? AND strategy = ? "
                    "AND symbol = ? AND status = 'open'",
                    (row["user_id"], row["strategy"], row["symbol"]),
                ).fetchone()
                if hit is None:
                    raise
                raise OpenPositionExistsError(row["user_id"], row["strategy"], row["symbol"],
                                              hit["signal_id"], row["signal_id"]) from e
            position = self._fetch_position(conn, row["signal_id"])
        return position

    def mark_published(self, signal_id, *, published_ms=None):
        """把訊號標記為已發布。published_ms 省略時用 clock()。

        拋出 SignalNotPendingError：訊號不存在（reason="missing"），或早就標記過
        （reason="published"，原本的 published_ms 保留不動）。
        """
        signal_id = _text("signal_id", signal_id)
        published_ms = (_epoch_ms("published_ms", published_ms) if published_ms is not None
                        else self._now("published_ms"))
        with self._write_transaction() as conn:
            cur = conn.execute(
                "UPDATE signals SET published_ms = ? WHERE signal_id = ? AND published_ms IS NULL",
                (published_ms, signal_id))
            if cur.rowcount != 1:
                exists = conn.execute("SELECT 1 FROM signals WHERE signal_id = ?",
                                      (signal_id,)).fetchone()
                raise SignalNotPendingError(signal_id, "missing" if exists is None else "published")

    def close_position(self, signal_id, *, exit_reason, exit_price, closed_ms=None):
        """把一筆 open 的名目部位平倉。只動那一筆。

        exit_reason  EXIT_REASONS 之一（"take_profit" / "stop_loss"）
        exit_price   出場價
        closed_ms    平倉時刻；省略時用 clock()

        回傳平倉後的名目部位（dict）。
        拋出 PositionNotOpenError：部位不存在（reason="missing"）或已平倉（reason="closed"）。
        回報方式固定是例外、不是回傳值 —— 呼叫端不可能「忘了檢查」而讓錯誤靜默通過。
        """
        signal_id = _text("signal_id", signal_id)
        exit_reason = _choice("exit_reason", exit_reason, EXIT_REASONS)
        exit_price = _price("exit_price", exit_price)
        closed_ms = (_epoch_ms("closed_ms", closed_ms) if closed_ms is not None
                     else self._now("closed_ms"))
        with self._write_transaction() as conn:
            cur = conn.execute(
                "UPDATE positions SET status = 'closed', exit_reason = ?, exit_price = ?, "
                "closed_ms = ? WHERE signal_id = ? AND status = 'open'",
                (exit_reason, exit_price, closed_ms, signal_id))
            if cur.rowcount != 1:
                existing = self._fetch_position(conn, signal_id)
                raise PositionNotOpenError(signal_id, "missing" if existing is None else "closed")
            position = self._fetch_position(conn, signal_id)
        return position

    # ---------- 讀取 ----------
    def get_signal(self, signal_id):
        """單筆訊號（dict，features 已解回 dict），不存在回傳 None。"""
        row = self._db().execute("SELECT * FROM signals WHERE signal_id = ?",
                                 (_text("signal_id", signal_id),)).fetchone()
        return None if row is None else _signal_dict(row)

    def get_position(self, signal_id):
        """單筆名目部位（dict），不存在回傳 None。"""
        return self._fetch_position(self._db(), _text("signal_id", signal_id))

    def open_positions(self, *, strategy=None, user_id=None):
        """所有 open 的名目部位（重啟復原用），依開倉時刻排序。

        strategy / user_id 可選擇性過濾。A 頻道復原時請帶 user_id=config.STRATEGY_USER_ID：
        第二階段訂閱者的紀錄進來之後，不帶 user_id 會把別人的部位也撈回來。
        """
        where, params = _filters(strategy, user_id)
        rows = self._db().execute(
            "SELECT * FROM positions WHERE status = 'open'" + where +
            " ORDER BY opened_ms, signal_id", params).fetchall()
        return [dict(r) for r in rows]

    def unpublished_signals(self, *, strategy=None, user_id=None):
        """已寫入但還沒標記發布的訊號（重啟補發用），依寫入時刻排序。過濾條件同 open_positions()。"""
        where, params = _filters(strategy, user_id)
        rows = self._db().execute(
            "SELECT * FROM signals WHERE published_ms IS NULL" + where +
            " ORDER BY created_ms, signal_id", params).fetchall()
        return [_signal_dict(r) for r in rows]
