# -*- coding: utf-8 -*-
"""
live.bus — 訊號事件匯流排（同步、行程內）
==========================================
WBS §1.2 定案的解耦點：訊號只產出一個事件，推播與（日後的）下單都只是訂閱者之一。

  產生者   A3（名目部位追蹤）：建好 EntryEvent / ExitEvent 後 publish
  訂閱者   T2（A 頻道推播）、日後的 webhook 下單端（派網訊號機器人，之後一個客戶一個）……

這一層只負責「送達」：依訂閱順序把事件交給每個 handler，某個 handler 出事不影響其他人。
**不做**：非同步事件迴圈、跨行程傳遞、持久化、重送。重送由 A3 依 B4′ 的「未發布」紀錄處理。

用法（接線在啟動時由進入點做一次；本模組沒有全域實例，也沒有任何 import 副作用）：

    from live.bus import SignalBus
    from live.signal_events import EntryEvent, ExitEvent

    bus = SignalBus()
    bus.subscribe(EntryEvent, tg_channel.on_entry, "tg_a_channel")
    bus.subscribe(ExitEvent, tg_channel.on_exit, "tg_a_channel")
    ...把 bus 交給 A3...
    report = bus.publish(event)          # A3 依 report 決定要不要標記「已發布」

不做成模組層級的全域實例：全域單例會讓測試彼此共用訂閱清單，也把「誰接了誰」藏進 import
順序裡。由進入點建一個、明確傳給產生者與訂閱者。

──────────────────────────────────────────────────────────────────────
handler 必須很快（訂閱者的責任）
──────────────────────────────────────────────────────────────────────
publish() 是**同步**的：在發布者的執行緒裡、依訂閱順序逐一呼叫 handler，全部回傳之後
publish() 才回傳。發布者可能是 A1 的主迴圈，handler 卡住，主迴圈就跟著卡住。
所以會做 I/O 的訂閱者（TG、webhook）**只能把事件排進自己的佇列然後立刻返回**，由自己的
背景執行緒慢慢送（T1′ 本來就有發送佇列）。handler 裡不可以打網路、不可以 sleep、不可以
等鎖等很久。

連帶的語意：送達報告裡的「送達」是指 handler 正常返回。對排佇列的訂閱者來說那代表「已排進
佇列」，不代表 TG 訊息已經發出去 —— 真正送出與否是那個訂閱者自己的重試 / 告警要管的事。

handler 可能被多個執行緒同時呼叫（A1 主迴圈與 A3 的執行緒都可能 publish），handler 自己要
是執行緒安全的（排進 queue.Queue 天生就是）。

──────────────────────────────────────────────────────────────────────
例外隔離
──────────────────────────────────────────────────────────────────────
handler 拋出 Exception：記一筆 ERROR（含 traceback、handler 名稱、strategy、signal_id、symbol），
記進送達報告的 failed，**其他 handler 照常收到**；publish() 不會把它拋回給發布者。

刻意只攔 Exception：KeyboardInterrupt / SystemExit 照樣往外傳，Ctrl+C 不可以被匯流排吞掉。

publish() 只有一種情況會拋例外：發布者自己傳錯東西（不是 EntryEvent / ExitEvent）。那是
發布端的程式錯誤，不是訂閱者出事，當場拋 TypeError。

──────────────────────────────────────────────────────────────────────
執行緒與巢狀發布
──────────────────────────────────────────────────────────────────────
訂閱清單由一把鎖保護；publish() 在鎖內取一份快照就放掉鎖，**呼叫 handler 時不持有鎖**。
所以：
  ・多個執行緒同時 publish 可以並行，彼此不互等 handler
  ・handler 裡再 publish（巢狀發布）不會死鎖；巢狀的事件是深度優先送完的 —— 內層事件先送達
    它的所有訂閱者，外層事件才接著送給下一個訂閱者
  ・publish 途中另一個執行緒 subscribe，不影響已經取好快照的那次 publish（下一次才看得到）

──────────────────────────────────────────────────────────────────────
沒有訂閱者
──────────────────────────────────────────────────────────────────────
事件沒有任何訂閱者時記 WARNING，不是靜默：通常代表啟動時漏接線。送達報告兩個清單都是空的，
report.ok 為 False。啟動時可以用 subscribers() 先自己檢查一遍接線。
"""

import logging
import threading
from dataclasses import dataclass

from live.signal_events import EVENT_TYPES

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublishReport:
    """一次 publish 的送達結果。名稱都依訂閱順序排列。

    delivered  handler 正常返回的訂閱者
    failed     handler 拋出 Exception 的訂閱者（已記 ERROR）
    """

    delivered: tuple
    failed: tuple

    @property
    def ok(self):
        """至少有一個訂閱者，而且全部正常返回。沒有訂閱者時是 False。"""
        return bool(self.delivered) and not self.failed


def _event_type_error(what, value):
    names = " / ".join(t.__name__ for t in EVENT_TYPES)
    return TypeError("%s 必須是 %s（精確型別，不收子類），收到 %r" % (what, names, value))


class SignalBus:
    """訊號事件匯流排。一個行程建一個，由進入點傳給產生者與訂閱者。"""

    def __init__(self):
        self._lock = threading.Lock()
        # 事件型別 -> ((name, handler), ...)。每次 subscribe 都換一個新的 tuple、不就地修改，
        # publish 拿到的快照之後怎麼樣都不會被改到。
        self._subscribers = {}

    def subscribe(self, event_type, handler, name):
        """讓 handler 接收 event_type 的事件。在啟動時接線，依呼叫順序決定送達順序。

        event_type  EntryEvent 或 ExitEvent
        handler     handler(event)，回傳值忽略。必須很快，見模組說明
        name        訂閱者名稱，出現在日誌與送達報告裡。同一個 event_type 底下不可重複；
                    同一個訂閱者訂兩種事件用同一個名稱是正常的
        """
        if event_type not in EVENT_TYPES:
            raise _event_type_error("event_type", event_type)
        if not callable(handler):
            raise TypeError("handler 必須可呼叫，收到 %r" % (handler,))
        if not isinstance(name, str):
            raise TypeError("name 必須是字串，收到 %r" % (name,))
        if not name or name != name.strip():
            raise ValueError("name 不可為空字串，前後也不可有空白，收到 %r" % (name,))
        with self._lock:
            current = self._subscribers.get(event_type, ())
            if any(existing == name for existing, _ in current):
                raise ValueError("%s 已經有名為 %r 的訂閱者" % (event_type.__name__, name))
            self._subscribers[event_type] = current + ((name, handler),)

    def subscribers(self, event_type):
        """event_type 目前的訂閱者名稱（依訂閱順序）。給啟動時自我檢查接線用。"""
        if event_type not in EVENT_TYPES:
            raise _event_type_error("event_type", event_type)
        with self._lock:
            return tuple(name for name, _ in self._subscribers.get(event_type, ()))

    def publish(self, event):
        """同步依訂閱順序把 event 交給每個 handler，回傳 PublishReport。

        handler 的 Exception 不會往外拋（記 ERROR、列進 report.failed）；只有 event 本身
        不是 EntryEvent / ExitEvent 時拋 TypeError。可以從任何執行緒呼叫，也可以在 handler
        裡巢狀呼叫。
        """
        event_type = type(event)
        if event_type not in EVENT_TYPES:
            raise _event_type_error("publish 的事件", event)
        # 鎖只保護「取快照」這一步；呼叫 handler 時不持有鎖，巢狀發布與多執行緒才不會互卡
        with self._lock:
            targets = self._subscribers.get(event_type, ())

        if not targets:
            logger.warning("%s 沒有任何訂閱者，事件沒有送到任何地方（strategy=%s signal_id=%s "
                           "symbol=%s）；多半是啟動時漏了 subscribe",
                           event_type.__name__, event.strategy, event.signal_id, event.symbol)
            return PublishReport(delivered=(), failed=())

        delivered = []
        failed = []
        for name, handler in targets:
            try:
                handler(event)
            except Exception:
                failed.append(name)
                logger.exception("訂閱者 %s 處理 %s 時拋出例外（strategy=%s signal_id=%s "
                                 "symbol=%s）；其他訂閱者照常送達",
                                 name, event_type.__name__, event.strategy, event.signal_id,
                                 event.symbol)
            else:
                delivered.append(name)
        logger.debug("%s strategy=%s signal_id=%s 送達 %s，失敗 %s", event_type.__name__,
                     event.strategy, event.signal_id, delivered, failed)
        return PublishReport(delivered=tuple(delivered), failed=tuple(failed))
