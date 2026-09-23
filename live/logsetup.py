# -*- coding: utf-8 -*-
"""
live.logsetup — 日誌與未攔截例外的基礎設定
==========================================
交易系統不能靜默失敗。這支只做一件事：讓「出事」一定會留在日誌檔裡。

用法（由進入點明確呼叫一次；import 本模組沒有任何副作用）：

    from live import logsetup
    logsetup.setup()

之後 live/ 底下任何模組照標準寫法 `logger = logging.getLogger(__name__)` 就會同時寫到
終端機與日誌檔，不需要任何自訂的取得方式。

檔名刻意叫 logsetup 而不是 logging / log：跟標準庫同名的模組只要 live/ 落到 sys.path
上就會把標準庫遮蔽掉，而 logging 被標準庫自己、requests、urllib3 大量使用，症狀會從
完全無關的地方冒出來（B2 剛修掉 live/http.py 的同型問題）。

──────────────────────────────────────────────────────────────────────
setup() 做了什麼
──────────────────────────────────────────────────────────────────────
在根 logger 上掛兩個 handler、設等級，並接管兩個未攔截例外的出口：

  檔案      runtime/logs/live.log（路徑、等級、輪替參數都在 live/config.py）。
            UTF-8、標準庫 RotatingFileHandler；目錄由 setup() 自己建。
  終端機    sys.stderr。見下面「終端機編碼」。
  例外      sys.excepthook（主執行緒）與 threading.excepthook（其他執行緒）。

重複呼叫 setup() 是安全的：先拆掉上一次自己掛的 handler 再掛新的，不會重複輸出。
新 handler 建立失敗（例如日誌目錄沒有寫入權限）時直接拋例外、舊設定原封不動 ——
寧可在啟動當下就死，也不要跑起來之後才發現什麼都沒記到。

──────────────────────────────────────────────────────────────────────
終端機編碼（FR-3）
──────────────────────────────────────────────────────────────────────
Windows 的 pipe / 主控台常是 cp950 或 cp1252，編不出 🔴✅❌⚠️📥（cp1252 連中文都編不出）。
標準庫 logging 遇到編碼錯誤不會往外拋，而是印一段 `--- Logging error ---` 然後把那一則
訊息丟掉 —— 程式照跑，終端機上那一行就這樣靜默消失。

做法：終端機 handler 在 format 完之後，先用「目前串流的編碼 + backslashreplace」把訊息
降級成那個編碼一定寫得出去的字串，再交給 StreamHandler 寫。編不出的字元變成 \\u26a0、
\\U0001f534 這類 Python 跳脫序列，資訊不丟、整行照樣出現。

  ・只影響這個 handler 的輸出，不 reconfigure 任何全域串流（別人的 print 不受影響）
  ・不依賴串流本身的 errors 設定：CPython 的 stderr 預設雖然是 backslashreplace，但那是
    別人隨時能改掉的全域狀態；這裡就算 stderr 被設成 strict 也照樣不會出錯
  ・日誌檔不做這個降級：檔案一律 UTF-8，字元原樣寫入、原樣讀回

──────────────────────────────────────────────────────────────────────
時間戳：台北時間，與主機時區無關
──────────────────────────────────────────────────────────────────────
雲端主機通常是 UTC，logging.Formatter 預設用 time.localtime，搬上去之後時間會靜默地差
8 小時。這裡固定用 UTC+8（跟 pionex_backtest.py 的 TPE 同一招；台灣 1979 年後沒有日光
節約）。不用 zoneinfo：Windows 上它要 tzdata 套件，那不是本專案宣告的相依。

──────────────────────────────────────────────────────────────────────
未攔截的例外（FR-5）
──────────────────────────────────────────────────────────────────────
  主執行緒   記一筆 CRITICAL（含完整 traceback）進日誌，然後照樣讓程序結束（exit code 1）。
             不吞、不繼續跑、不自動重啟、不送通知 —— 重啟是 service 管理的事，通知是 A4 的事。
  其他執行緒 記一筆 CRITICAL（含完整 traceback 與執行緒名稱），該執行緒照樣死、程序照樣
             繼續跑。「背景執行緒死了整個程序要不要跟著結束」是 A1 / A2 的架構決定，不在這裡。
  Ctrl+C     KeyboardInterrupt 不算 crash，不記進日誌，原樣交給 setup() 之前的 hook
             （預設就是印出 KeyboardInterrupt 然後結束）。執行緒裡的 SystemExit 同理交回
             預設處理（預設是安靜忽略）。
  被停用時   setup() 之後若有人停用或重設了 logging（dictConfig 預設會停用既有 logger、
             logging.disable()、把 handler 拆掉……），終端機 handler 收不到這筆 crash，
             就改由 setup() 之前的 hook 把 traceback 印到 stderr —— 最差也跟沒有 setup() 一樣，
             不會讓 traceback 完全消失。正常情況 stderr 上只有一份（見 _log_crash）。

──────────────────────────────────────────────────────────────────────
假設：一個日誌檔只能有一個寫入行程
──────────────────────────────────────────────────────────────────────
RotatingFileHandler 輪替時要把 live.log 改名成 live.log.1。Windows 上只要該檔被別的
handle 開著（第二個實例寫同一個檔、或某個工具持續開著它），改名就會失敗（WinError 32）。
之後每一則寫檔前都會再試一次輪替、再失敗一次：stderr 印一段 `--- Logging error ---`，
那一則**不會寫進日誌檔**。檔案停在輪替門檻附近不再增加，之後的紀錄全數從檔案裡消失
（終端機上照樣看得到）。程式照跑、檔案也還在，所以很難察覺。
（2026-09-23 laptop / Python 3.14.7 實測：400 則裡 383 次 WinError 32，只有 17 則進檔。）
Linux 雲端主機沒有這個問題，但規則一樣：
**同一個日誌檔同一時間只能有一個行程在寫**。日後若要多個行程，各自用 setup(log_file=...)
指定不同的檔，不要共用。

輪替判斷是標準庫以「字元數」估算的，訊息大量含中文或 emoji 時單檔實際 bytes 會略超過
LOG_MAX_BYTES（每則最多超一則的量），不影響「有上限」這件事。

teardown() 把 setup() 做的事全部撤掉並關檔，主要給測試用（Windows 上檔案沒關，暫存目錄
刪不掉）。正式程式不需要呼叫它，程序結束時標準庫會自己 flush 並關檔。
"""

import logging
import logging.handlers
import os
import sys
import threading
from datetime import datetime, timedelta, timezone

from live import config

# 台北時間。固定位移，不查時區資料庫。
TAIPEI = timezone(timedelta(hours=8))

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_logger = logging.getLogger(__name__)

# setup() 掛上去的 handler；重複呼叫與 teardown() 都靠它只拆自己掛的東西
_installed_handlers = []
# 第一次 setup() 之前的狀態 (sys.excepthook, threading.excepthook, 根 logger 等級)；
# 沒有 setup 過就是 None。只在第一次記，免得重複 setup 時把自己的 hook 當成「之前的」。
_saved_state = None


class _TaipeiFormatter(logging.Formatter):
    """時間戳一律換算成台北時間，不看主機時區。"""

    def formatTime(self, record, datefmt=None):
        return datetime.fromtimestamp(record.created, TAIPEI).strftime(datefmt or DATE_FORMAT)


class _ConsoleHandler(logging.StreamHandler):
    """寫終端機的 handler：先把訊息降級成串流編碼寫得出來的字元，再交給 StreamHandler。"""

    def format(self, record):
        text = super().format(record)
        encoding = getattr(self.stream, "encoding", None)
        if not encoding:
            # 沒有編碼的文字串流（例如 io.StringIO）什麼字元都收，不用降級
            return text
        try:
            return text.encode(encoding, "backslashreplace").decode(encoding, "replace")
        except LookupError:
            # 串流報了一個 Python 不認得的編碼名稱，退回純 ASCII 一定寫得出去
            return text.encode("ascii", "backslashreplace").decode("ascii")


def _crash_handlers_reached():
    """一筆 CRITICAL 從 _logger 出發，實際會送到哪幾個 setup() 掛的 handler。

    照標準庫 Logger.isEnabledFor + Logger.callHandlers 的派送規則算：logger 被停用
    （dictConfig 預設的 disable_existing_loggers）、logging.disable()、等級被調高、
    propagate 被關、handler 被拆掉或等級被調高，都會讓它回傳空的。
    沒有納入考量的是別人加在 logger / handler 上的 filter（無從預測）。
    """
    if not _logger.isEnabledFor(logging.CRITICAL):
        return []
    reached = []
    logger = _logger
    while logger is not None:
        reached += [h for h in logger.handlers
                    if h in _installed_handlers and logging.CRITICAL >= h.level]
        if not logger.propagate:
            break
        logger = logger.parent
    return reached


def _log_crash(fallback, msg, *args, exc_info):
    """把 crash 記進日誌；終端機 handler 收不到時，交回 setup() 之前的 hook 印到 stderr。

    守住的不變式：stderr 上剛好一份 traceback。
      ・正常情況：終端機 handler 印一份、日誌檔一份，不再呼叫原本的 hook（否則印兩次）
      ・終端機 handler 不在路徑上（logging 被別人停用 / 重設 / handler 被拆）：改由原本的
        hook 印，等於沒有 setup() 過 —— setup() 不可以讓「死了但沒有任何線索」變得更容易
      ・自己的 handler 一個都收不到時連 critical() 都不呼叫：否則標準庫找不到 handler 會
        走 logging.lastResort 再印一份，加上原本的 hook 就變兩份
    """
    reached = _crash_handlers_reached()
    if reached:
        _logger.critical(msg, *args, exc_info=exc_info)
    if not any(isinstance(h, _ConsoleHandler) for h in reached):
        fallback()


def _excepthook(exc_type, exc_value, exc_tb):
    """主執行緒的未攔截例外：先進日誌，hook 回傳後由直譯器照常結束程序。"""
    if issubclass(exc_type, KeyboardInterrupt):
        _previous_sys_excepthook()(exc_type, exc_value, exc_tb)
        return
    _log_crash(lambda: _previous_sys_excepthook()(exc_type, exc_value, exc_tb),
               "未攔截的例外，程序即將結束", exc_info=(exc_type, exc_value, exc_tb))


def _thread_excepthook(args):
    """非主執行緒的未攔截例外：進日誌，該執行緒照樣結束、程序照樣繼續。"""
    if args.exc_value is None or issubclass(args.exc_type, (SystemExit, KeyboardInterrupt)):
        _previous_thread_excepthook()(args)
        return
    name = args.thread.name if args.thread is not None else "<unknown>"
    _log_crash(lambda: _previous_thread_excepthook()(args),
               "執行緒 %s 發生未攔截的例外，該執行緒結束", name,
               exc_info=(args.exc_type, args.exc_value, args.exc_traceback))


def _previous_sys_excepthook():
    return _saved_state[0] if _saved_state else sys.__excepthook__


def _previous_thread_excepthook():
    return _saved_state[1] if _saved_state else threading.__excepthook__


def _remove_installed_handlers():
    root = logging.getLogger()
    for handler in _installed_handlers:
        root.removeHandler(handler)
        handler.close()
    _installed_handlers.clear()


def setup(*, log_file=None, level=None, max_bytes=None, backup_count=None):
    """設定日誌：根 logger 掛上檔案與終端機兩個 handler，並接管未攔截例外。

    參數全部可省略，省略時取 live/config.py 的 LOG_FILE / LOG_LEVEL / LOG_MAX_BYTES /
    LOG_BACKUP_COUNT（呼叫當下才讀，不在 import 時綁死）。覆寫參數是給測試把日誌導到
    暫存目錄、把輪替上限調小用的；正式程式照 config 走，不要傳。

    回傳實際使用的日誌檔絕對路徑。可重複呼叫，不會重複掛 handler。
    """
    global _saved_state

    log_file = os.path.abspath(log_file if log_file is not None else config.LOG_FILE)
    level = level if level is not None else config.LOG_LEVEL
    max_bytes = max_bytes if max_bytes is not None else config.LOG_MAX_BYTES
    backup_count = backup_count if backup_count is not None else config.LOG_BACKUP_COUNT

    # 先把新的 handler 全部建好；任何一步失敗就直接拋，舊設定維持原樣
    formatter = _TaipeiFormatter(LOG_FORMAT, DATE_FORMAT)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    # delay=False：開不了檔（權限、路徑）要在 setup() 當下就失敗，不要等到第一則日誌
    # errors="backslashreplace"：UTF-8 什麼字元都編得出來，只剩孤立 surrogate（例如
    # surrogateescape 解出來的檔名）會失敗；那種字元跳脫後寫入，不要整則丟掉
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count,
        encoding="utf-8", errors="backslashreplace",
    )
    file_handler.setFormatter(formatter)
    new_handlers = [file_handler]
    # 沒有主控台的環境（pythonw、某些服務）sys.stderr 是 None，那就只寫檔
    if sys.stderr is not None:
        console_handler = _ConsoleHandler(sys.stderr)
        console_handler.setFormatter(formatter)
        new_handlers.append(console_handler)

    root = logging.getLogger()
    previous_level = root.level
    try:
        root.setLevel(level)          # 等級名稱打錯在這裡就會拋 ValueError
    except (ValueError, TypeError):
        for handler in new_handlers:
            handler.close()
        raise

    if _saved_state is None:
        _saved_state = (sys.excepthook, threading.excepthook, previous_level)
    _remove_installed_handlers()
    for handler in new_handlers:
        root.addHandler(handler)
        _installed_handlers.append(handler)

    sys.excepthook = _excepthook
    threading.excepthook = _thread_excepthook
    return log_file


def teardown():
    """撤掉 setup() 做的一切：拆 handler 並關檔、還原兩個 excepthook 與根 logger 等級。

    主要給測試用。沒有 setup 過就什麼都不做。
    """
    global _saved_state
    _remove_installed_handlers()
    if _saved_state is None:
        return
    prev_sys_hook, prev_thread_hook, prev_level = _saved_state
    if sys.excepthook is _excepthook:
        sys.excepthook = prev_sys_hook
    if threading.excepthook is _thread_excepthook:
        threading.excepthook = prev_thread_hook
    logging.getLogger().setLevel(prev_level)
    _saved_state = None
