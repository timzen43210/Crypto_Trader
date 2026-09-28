# -*- coding: utf-8 -*-
"""
live.a_channel_push — A 頻道推播（T2）：匯流排訂閱者 → outbox → 發送工作執行緒 → ChannelSender
====================================================================================================
把 A3 發到匯流排的進場 / 出場事件變成 Telegram 頻道上的訊息，並把每個事件在頻道上的最終狀態記在
outbox（live.a_channel_outbox，R-A 報表的統計依據）。訊息格式在 live.a_channel_text（純函式）。

用法（由執行入口 live.a_channel 在 `--push-tg` 時接線；A3 啟動之前做完）：

    sender = ChannelSender(); sender.start()          # 缺密鑰 → MissingSecretError
    pusher = ChannelPusher(sender)
    pusher.start()                                    # 建 outbox、記一行現況、啟動工作執行緒
    pusher.subscribe(bus)                             # 訂閱 EntryEvent / ExitEvent（名稱 tg_a_channel）
    pusher.set_specs(tracker.specs)                   # 主週期取自 A3 的策略規格
    ...A3 / A1 跑...
    pusher.shutdown()                                 # 停止派送 → sender.stop() → 記回結果 → 結束

──────────────────────────────────────────────────────────────────────
handler（在 A3 的 a3-tracker 執行緒裡被呼叫）
──────────────────────────────────────────────────────────────────────
只做本機的事，不打網路、不等發送結果、不 sleep：
  1. 開一條短的 outbox 連線（busy timeout = A_CHANNEL_OUTBOX_BUSY_TIMEOUT_SECONDS）
  2. (signal_id, 事件種類) 已經在 outbox → 直接返回（持久化去重，跨重啟有效；重送的事件內容相同）
  3. 取快照。進場：market_static.symbol_spec() / max_leverage()，**market_static 尚未載入 → NotLoadedError 往外拋**
     （不猜值），匯流排的送達報告 ok=False，A3 下一次 tick 重送。出場：價格精度沿用同一 signal_id 進場列的快照，
     不查 market_static（見下面「快照值」）。**收到時就已晚於延遲門檻的進場**（A3 重啟補發的舊進場）：派送時一定是
     延遲不發、永遠不組字，所以也不查 market_static，快照標 price_decimals_source = "stale_at_receipt"（第 1 輪 F6）
  4. 寫入一列「待送」並 COMMIT；寫入失敗（唯讀、被鎖住、磁碟錯誤）同樣往外拋，走 A3 的重送
  5. 通知工作執行緒，返回
handler 返回 = 這個事件已經落地；之後不論當機、重啟、stop() 沒送完，都會從 outbox 接著送。

快照值（存進 outbox 的 snapshot_json，重新組字時內容不變）：
  label                 config.STRATEGY_LABELS[strategy]
  price_decimals        價格小數位數。來源（price_decimals_source）：
                          "quotePrecision"  market_static.symbol_spec(symbol)["quotePrecision"]
                          "fallback"        查不到規格或欄位不合理 → 以參考價（訊號價 / 進場價）的
                                            A_CHANNEL_PRICE_FALLBACK_SIGNIFICANT_DIGITS 位有效數字推出小數位數，記 WARNING
                        為什麼是 quotePrecision（T2 RD 實測，不是猜的）：派網 2026-09-22 的 /common/symbols 實際回應
                        613 筆 PERP 全部 quoteStep == 10^-quotePrecision；拿 pionex_cache 裡 443 個 symbol 在同一週的
                        5 分K（OHLC）比對，**價格實際出現的最多小數位數 443 / 443 全部等於 quotePrecision**，
                        與 basePrecision（數量精度）則 413 個對不上。所以 quotePrecision 是合約的價格小數位數、
                        quoteStep 是價格跳動單位（兩者等價），basePrecision / baseStep 是數量
                        出場不查 market_static：沿用同一 signal_id 進場列的 price_decimals（來源 "entry_snapshot"），
                        出場訊息的「進場價」與進場訊息的「訊號價」因此顯示得一模一樣。outbox 沒有進場列的出場（進場在
                        推播啟用之前就發布了；A3 保證進場送達所有訂閱者之後才發出場，所以之後也不會補上）一定是
                        「因進場未出現而不發」、不會組字，price_decimals 記 None（來源 "no_entry"）。好處：A3 重啟時
                        在 A1 載入 market_static 之前補發的出場，不會因為 NotLoadedError 在匯流排記一筆 ERROR
  （進場另外）
  signal_close_ms       訊號 K 棒收盤 = bar_open_ms + 該策略的主週期（A3 的 StrategySpec.main_ms，set_specs() 給的；
                        沒給就用 live.notional_tracker.build_specs()，與 A3 預設同一個來源）
  max_leverage          market_static.max_leverage(symbol)；None（riskTable 沒這個幣）→ 不捏造，記 WARNING
  order_pct / target_leverage / deviation_warn_pct   當下的 config 值

──────────────────────────────────────────────────────────────────────
工作執行緒（a-channel-push）
──────────────────────────────────────────────────────────────────────
自己開一條長期的 outbox 連線。每一輪：先把發送器回報的結果記回 outbox，再依寫入順序（seq）找下一列可以交出的待送列：
  * next_attempt_ms 還沒到的先跳過（不擋後面的列）
  * 進場：送出前已晚於訊號 K 棒收盤超過 A_CHANNEL_ENTRY_MAX_DELAY_SECONDS → **延遲不發**（WARNING，含 signal_id 與
    延遲秒數）；否則交給發送器，並帶 expires_at = 收盤 + 門檻：發送器在每一次實際發出請求之前再檢查一次
    （內部佇列、限速、429 等待造成的延遲也算），過期就不發、回報 expired → 延遲不發
  * 出場：同一 signal_id 的進場是「已送達」才送；進場還是「待送」就等；其他（延遲不發、永久失敗、outbox 裡沒有
    進場列 —— 例如進場在推播啟用之前就發布了）→ **因進場未出現而不發**。出場不套延遲門檻：進場已經在頻道上的，
    訂閱者手上可能有部位，必須知道結果。recovered 為真、或交給發送器時已晚於 closed_ms 超過門檻 → 加延遲註記
  * **同一時間最多交出去一則**：上一則的結果記回 outbox 之前不交下一則。所以同一 signal_id 的出場不可能比進場先送出，
    期限也是在「真的輪到它」的時候才判斷
交出之前先 COMMIT「已交出」（handoff_open = 1）再呼叫 sender.send()：交出之後才當掉的話，重啟時看得到它。

結果（ChannelSender 的 on_done）：
  delivered            → 已送達（記 message_id）
  failed               → 永久失敗（ERROR；A4 之後告警）
  expired              → 延遲不發（WARNING）
  gave_up / abandoned  → 維持待送，A_CHANNEL_OUTBOX_RETRY_SECONDS 後重試；uncertain 記進 outbox
  發送器拒收（已停止）  → 維持待送，同上（確定沒送出）。拒收也排進同一個結果佇列，由工作執行緒記回

**結果記不回 outbox 時不遺失**（第 1 輪 BUG-009）：結果佇列一次只處理最前面的一則，COMMIT 成功才移出、才清掉
「正在交出」；寫不進去（被外部程式鎖住超過 busy timeout、磁碟錯誤）就記 ERROR，結果留在佇列頭、不交出新的列，
A_CHANNEL_OUTBOX_RETRY_SECONDS 之後再記（用單調時鐘計時：業務時鐘凍結或往回調都不會卡住，也不會忙等）。
第二道防線：派送時看到「已交出、結果卻沒有記回來」的孤兒列（不是正在交出的那一則），比照重啟時的做法當成不確定、
重新排送。

**不確定就不判延遲不發**（FR-3 第 3 點）：一列只要有過一次「結果不確定」的交出（逾時、連線中斷、5xx，或交出之後
當機 / 停止而結果沒記回來 —— 重啟時 mark_open_handoffs_uncertain() 一律當成不確定），之後就不再套延遲門檻、
照送到「已送達」或「永久失敗」，並且重送**同一段文字**（outbox 的 text）。頻道上可能已經有它，寧可多一則，
也不可以讓紀錄說「沒有」、頻道上卻有。

──────────────────────────────────────────────────────────────────────
關閉（shutdown）
──────────────────────────────────────────────────────────────────────
執行入口在 A1 → A3（join）之後呼叫：停止派送新的列 → ChannelSender.stop()（盡量送完手上那一則）→ 工作執行緒把
發送器最後回報的結果記回 outbox → 結束。沒送完的留在 outbox（待送），記 WARNING 寫明幾則，下次啟動再送。
（PRD 寫的順序是「T2 工作執行緒 → ChannelSender.stop()」；這裡把 T2 拆成「停止派送」與「記回結果」兩段、
夾住 sender.stop()，否則 stop() 期間送達的那一則結果會沒人記，下次啟動被當成不確定而重送。）

──────────────────────────────────────────────────────────────────────
樣本訊息（FR-5，AC-8 實機確認版面）
──────────────────────────────────────────────────────────────────────
    python -m live.a_channel_push --sample
寫死的假資料組出 9 種情境（「標籤：值」版面，使用者 2026-09-28 定案），開頭加 [測試]，經 ChannelSender 送到頻道。
不碰 outbox、不碰 live.sqlite3、不打派網。只印統計、不印密鑰。exit code：0 全部送達、1 有失敗、3 缺密鑰未實測。
"""

import argparse
import collections
import functools
import logging
import sys
import threading
import time

from live import a_channel_outbox as ob_mod
from live import a_channel_text as text_mod
from live import config
from live import market_static
from live.signal_events import EntryEvent, ExitEvent
from live.tg_channel import (RESULT_DELIVERED, RESULT_EXPIRED, RESULT_FAILED, ChannelSender, SendResult,
                             utf16_units)

logger = logging.getLogger(__name__)

SUBSCRIBER_NAME = "tg_a_channel"
THREAD_NAME = "a-channel-push"

PRICE_SOURCE_SPEC = "quotePrecision"
PRICE_SOURCE_FALLBACK = "fallback"
PRICE_SOURCE_ENTRY = "entry_snapshot"          # 出場：沿用進場列的價格精度
PRICE_SOURCE_NO_ENTRY = "no_entry"             # 出場：outbox 沒有進場列（一定不發），不記精度
PRICE_SOURCE_STALE = "stale_at_receipt"        # 進場：收到時就已晚於延遲門檻（一定不發），不查 market_static、不記精度

# 發送器拒收（send() 回傳 False）時，T2 自己排進結果佇列的狀態（不是 ChannelSender 的回報）
_RESULT_REJECTED = "rejected"

# quotePrecision 的合理範圍（實測 0～11）；超出就當作取不到
_MAX_PRICE_DECIMALS = 18

# 工作執行緒沒有任何通知時，最久多久自己醒來看一次 outbox（秒；只是保險，正常都靠通知與重試時刻）
_IDLE_POLL_SECONDS = 60.0

# start() 等工作執行緒開好 outbox、shutdown() 等它記完結果結束，各最多幾秒
_THREAD_READY_TIMEOUT_SECONDS = 30.0
_THREAD_JOIN_TIMEOUT_SECONDS = 30.0

# _dispatch_* 的回傳
_HANDED, _FINALIZED, _WAITING = "handed", "finalized", "waiting"


def _system_now_ms():
    return time.time_ns() // 1_000_000


def price_decimals_from_spec(spec):
    """symbol_spec() 的結果 → 價格小數位數（quotePrecision）；取不到或不合理回 None。依據見模組 docstring。"""
    if not isinstance(spec, dict):
        return None
    value = spec.get("quotePrecision")
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        try:
            value = int(value.strip())
        except ValueError:
            return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    if isinstance(value, int) and 0 <= value <= _MAX_PRICE_DECIMALS:
        return value
    return None


class ChannelPusher:
    """T2 的主體：匯流排 handler + outbox + 發送工作執行緒。設計見模組 docstring。

    可注入（測試全程離線、用假時鐘）：
      sender       有 send(text, key=, on_done=, expires_at=) 與 stop(timeout) 的物件（正式是 ChannelSender）
      path         outbox 檔；預設 config.A_CHANNEL_OUTBOX_DB_PATH
      specs        {strategy: 有 main_ms 的物件}；預設在第一次用到時取 live.notional_tracker.build_specs()
      market       有 symbol_spec(symbol) / max_leverage(symbol) 的物件；預設 live.market_static
      now_ms       回傳 UTC epoch 毫秒；預設系統時鐘（與 ChannelSender 的 wall_clock 同一個時間基準）
      worker_busy_timeout  工作執行緒連線的 busy timeout（秒）；None = sqlite3 預設 5 秒（測試用來縮短等待）
      mono         單調時鐘（秒）；預設 time.monotonic。只用在結果記不回 outbox 之後的退避（BUG-009）
    其餘參數省略時取 live.config 在建構當下的值。
    """

    def __init__(self, sender, *, path=None, specs=None, market=None, now_ms=None, max_delay_seconds=None,
                 retry_seconds=None, busy_timeout=None, fallback_significant_digits=None,
                 idle_poll_seconds=None, worker_busy_timeout=None, mono=None):
        def pick(value, default):
            return default if value is None else value

        self.sender = sender
        self.path = pick(path, config.A_CHANNEL_OUTBOX_DB_PATH)
        self._specs = specs
        self._market = market if market is not None else market_static
        self._now_fn = now_ms or _system_now_ms
        # 單調時鐘（秒）：只用在「結果記不回 outbox 之後多久再記」這種行程內的退避。不用 now_ms：業務時鐘被凍結或
        # 往回調（NTP）時，退避不可以因此卡住
        self._mono = mono or time.monotonic
        self._max_delay_ms = int(round(float(pick(max_delay_seconds, config.A_CHANNEL_ENTRY_MAX_DELAY_SECONDS)) * 1000))
        self._retry_ms = int(round(float(pick(retry_seconds, config.A_CHANNEL_OUTBOX_RETRY_SECONDS)) * 1000))
        self._busy_timeout = float(pick(busy_timeout, config.A_CHANNEL_OUTBOX_BUSY_TIMEOUT_SECONDS))
        self._fallback_digits = int(pick(fallback_significant_digits,
                                         config.A_CHANNEL_PRICE_FALLBACK_SIGNIFICANT_DIGITS))
        self._idle_poll = float(pick(idle_poll_seconds, _IDLE_POLL_SECONDS))
        # 工作執行緒連線的 busy timeout（秒）；None = sqlite3 預設的 5 秒。記回失敗時結果會保留、稍後再記（BUG-009）
        self._worker_busy_timeout = None if worker_busy_timeout is None else float(worker_busy_timeout)
        if self._max_delay_ms < 0 or self._retry_ms <= 0 or self._busy_timeout < 0 or self._idle_poll <= 0:
            raise ValueError("延遲門檻不可為負、重試間隔與輪詢間隔必須 > 0、busy timeout 不可為負")

        self._cond = threading.Condition()
        self._results = collections.deque()     # (token, SendResult)，由發送器的 on_done 放進來
        self._in_flight = None                  # 目前交出去、結果還沒記回來的 (seq, attempt)
        self._dirty = False                     # handler 寫了新的列
        self._stopping = False                  # 不再派送新的列
        self._finishing = False                 # 發送器已停：把結果記完就結束
        self._thread = None
        self._ready = threading.Event()
        self._ready_error = None
        self._final_counts = None
        self._results_retry_at = None           # 結果記不回 outbox 時，下一次再記的時刻（單調時鐘秒數）
        self.stats = {"received": 0, "duplicates": 0, "handoffs": 0, "delivered": 0, "expired": 0,
                      "no_entry": 0, "failed": 0, "retries": 0, "record_failures": 0, "orphans": 0}

    # ================================================================ 生命週期
    def set_specs(self, specs):
        """主週期的來源（A3 的 NotionalTracker.specs）。在 A3 啟動前呼叫。"""
        self._specs = dict(specs)

    def start(self):
        """prepare()，再啟動工作執行緒並等它開好自己的連線。失敗就拋例外（執行入口 exit 1）。回傳 prepare() 的 counts。"""
        if self._thread is not None:
            raise RuntimeError("ChannelPusher 只能 start() 一次")
        counts = self.prepare()
        self._thread = threading.Thread(target=self._thread_main, name=THREAD_NAME, daemon=True)
        self._thread.start()
        if not self._ready.wait(_THREAD_READY_TIMEOUT_SECONDS):
            raise RuntimeError("A 頻道推播的工作執行緒 %s 秒內沒有開好 outbox" % _THREAD_READY_TIMEOUT_SECONDS)
        if self._ready_error is not None:
            raise self._ready_error
        return counts

    def prepare(self):
        """start() 的前半（不起執行緒）：建 outbox（必要時）、把上次交出後沒記回結果的列標成不確定、記一行現況。
        回傳 counts()。測試的同步模式（直接呼叫 pump()）也從這裡開始。"""
        now = self._now()
        with ob_mod.open_outbox(self.path) as outbox:
            reopened = outbox.mark_open_handoffs_uncertain(now_ms=now)
            counts = outbox.counts()
        logger.info("A 頻道推播 outbox（%s）現況：待送 %d 則（其中結果不確定 %d 則%s）；累計 已送達 %d、延遲不發 %d、"
                    "因進場未出現而不發 %d、永久失敗 %d", self.path, counts[ob_mod.STATUS_PENDING],
                    counts["uncertain_pending"],
                    "，含上次交出後沒有記回結果的 %d 則" % reopened if reopened else "",
                    counts[ob_mod.STATUS_DELIVERED], counts[ob_mod.STATUS_EXPIRED], counts[ob_mod.STATUS_NO_ENTRY],
                    counts[ob_mod.STATUS_FAILED])
        return counts

    def subscribe(self, bus):
        bus.subscribe(EntryEvent, self.handle, SUBSCRIBER_NAME)
        bus.subscribe(ExitEvent, self.handle, SUBSCRIBER_NAME)

    @property
    def worker_alive(self):
        return self._thread is not None and self._thread.is_alive()

    def shutdown(self, sender_timeout=None):
        """停止派送 → sender.stop(sender_timeout) → 把最後的結果記回 outbox → 結束工作執行緒。回傳剩下幾則待送。"""
        with self._cond:
            self._stopping = True
            self._cond.notify_all()
        try:
            self.sender.stop(config.TG_STOP_TIMEOUT_SECONDS if sender_timeout is None else sender_timeout)
        except Exception:  # noqa: BLE001 —— 關機流程不可以因為這一步中斷；沒記回的列下次啟動會當成不確定
            logger.exception("A 頻道推播關閉時 ChannelSender.stop() 拋出例外")
        with self._cond:
            self._finishing = True
            self._cond.notify_all()
        thread = self._thread
        if thread is not None:
            thread.join(_THREAD_JOIN_TIMEOUT_SECONDS)
            if thread.is_alive():
                logger.error("A 頻道推播的工作執行緒 %s 秒內沒有結束", _THREAD_JOIN_TIMEOUT_SECONDS)
        counts = self._final_counts
        if counts is None:
            try:
                with ob_mod.open_outbox(self.path, busy_timeout=self._busy_timeout, init=False) as outbox:
                    counts = outbox.counts()
            except Exception:  # noqa: BLE001
                logger.exception("A 頻道推播關閉時讀不到 outbox 的現況")
                return None
        pending = counts[ob_mod.STATUS_PENDING]
        if pending:
            logger.warning("A 頻道推播已停止：outbox 還有 %d 則待送（其中結果不確定 %d 則），留在 %s，下次啟動會接著送",
                           pending, counts["uncertain_pending"], self.path)
        else:
            logger.info("A 頻道推播已停止：outbox 沒有待送的列（已送達 %d、延遲不發 %d、因進場未出現而不發 %d、永久失敗 %d）",
                        counts[ob_mod.STATUS_DELIVERED], counts[ob_mod.STATUS_EXPIRED],
                        counts[ob_mod.STATUS_NO_ENTRY], counts[ob_mod.STATUS_FAILED])
        return pending

    # ================================================================ handler（A3 的執行緒）
    def handle(self, event):
        """匯流排 handler：落地到 outbox 就返回。該讓 A3 重送的情況一律往外拋（見模組 docstring）。"""
        kind = ob_mod.KIND_ENTRY if type(event) is EntryEvent else ob_mod.KIND_EXIT
        with ob_mod.open_outbox(self.path, busy_timeout=self._busy_timeout, init=False) as outbox:
            if outbox.exists(event.signal_id, kind):
                self.stats["duplicates"] += 1
                return
            snapshot = self._snapshot(event, outbox)
            inserted = outbox.insert(signal_id=event.signal_id, kind=kind, strategy=event.strategy,
                                     symbol=event.symbol, event=event.to_dict(), snapshot=snapshot,
                                     received_ms=self._now())
        if not inserted:
            self.stats["duplicates"] += 1
            return
        self.stats["received"] += 1
        logger.info("A 頻道推播收到 %s %s（%s %s），已寫入 outbox 待送", type(event).__name__, event.signal_id,
                    event.strategy, event.symbol)
        with self._cond:
            self._dirty = True
            self._cond.notify_all()

    def _snapshot(self, event, outbox):
        """收到事件當下的快照值。進場：market_static 未載入 → NotLoadedError 原樣往外拋（不猜值）。
        出場：價格精度沿用同一 signal_id 進場列的快照（_exit_snapshot），不需要 market_static。
        收到時就已經晚於門檻的進場：必定延遲不發、永遠不組字，不查 market_static（第 1 輪 F6）。"""
        if type(event) is not EntryEvent:
            return self._exit_snapshot(event, outbox)
        label = config.STRATEGY_LABELS[event.strategy]
        spec_obj = self._strategy_spec(event.strategy)
        close_ms = int(event.bar_open_ms) + int(spec_obj.main_ms)
        main_interval = getattr(spec_obj, "main_interval", None)
        delay_ms = self._now() - close_ms
        if delay_ms > self._max_delay_ms:
            # 新寫入的列 uncertain = 0、延遲只會變大 → 派送時一定是「延遲不發」。不需要、也不去查 market_static，
            # 所以 A3 重啟補發的舊進場不會因為 A1 還沒載入標的池而在匯流排記 ERROR
            logger.info("A 頻道推播：進場 %s 收到時已晚於訊號 K 棒收盤 %.1f 秒（門檻 %s 秒），必定延遲不發，不查 market_static",
                        event.signal_id, delay_ms / 1000.0, self._max_delay_ms / 1000.0)
            return {"label": label, "signal_close_ms": close_ms, "main_interval": main_interval,
                    "price_decimals": None, "price_decimals_source": PRICE_SOURCE_STALE, "stale_delay_ms": delay_ms}
        spec = self._market.symbol_spec(event.symbol)          # 未載入 → NotLoadedError（A3 重送）
        decimals = price_decimals_from_spec(spec)
        source = PRICE_SOURCE_SPEC
        if decimals is None:
            decimals = text_mod.price_decimals_for(event.signal_price, self._fallback_digits)
            source = PRICE_SOURCE_FALLBACK
            logger.warning("A 頻道推播 %s：取不到 %s 的價格精度（symbol_spec %s），改以 %d 位有效數字顯示（%d 位小數）",
                           event.signal_id, event.symbol, "查不到" if spec is None else "的 quotePrecision 不合理",
                           self._fallback_digits, decimals)
        max_lev = self._market.max_leverage(event.symbol)
        if max_lev is not None and (isinstance(max_lev, bool) or not isinstance(max_lev, int) or max_lev < 1):
            logger.warning("A 頻道推播 %s：%s 的槓桿上限 %r 不合理，當作取不到", event.signal_id, event.symbol, max_lev)
            max_lev = None
        if max_lev is None:
            logger.warning("A 頻道推播 %s：取不到 %s 的槓桿上限，建議倉位以目標槓桿 %sx 計並註明「%s」",
                           event.signal_id, event.symbol, config.A_CHANNEL_TARGET_LEVERAGE,
                           text_mod.LEVERAGE_UNKNOWN_TEXT)
        return {"label": label, "price_decimals": int(decimals), "price_decimals_source": source,
                "signal_close_ms": close_ms, "main_interval": main_interval, "max_leverage": max_lev,
                "order_pct": config.A_CHANNEL_ORDER_PCT, "target_leverage": config.A_CHANNEL_TARGET_LEVERAGE,
                "deviation_warn_pct": config.A_CHANNEL_DEVIATION_WARN_PCT}

    def _exit_snapshot(self, event, outbox):
        """出場的快照：價格小數位數沿用進場列的快照，出場訊息的「進場價」與進場訊息的「訊號價」顯示得一模一樣。
        outbox 裡沒有進場列 = 進場在推播啟用之前就發布了（A3 保證進場送達所有訂閱者之後才發出場），這一則一定是
        「因進場未出現而不發」、永遠不會組字，所以不記價格精度（None），也不去查 market_static。"""
        snap = {"label": config.STRATEGY_LABELS[event.strategy]}
        entry_row = outbox.get(event.signal_id, ob_mod.KIND_ENTRY)
        if entry_row is None:
            snap.update(price_decimals=None, price_decimals_source=PRICE_SOURCE_NO_ENTRY)
        else:
            snap.update(price_decimals=entry_row["snapshot"]["price_decimals"],
                        price_decimals_source=PRICE_SOURCE_ENTRY)
        return snap

    def _strategy_spec(self, strategy):
        if self._specs is None:
            from live import notional_tracker      # 與 A3 預設同一個來源；只在沒有 set_specs() 時才用
            self._specs = notional_tracker.build_specs()
        return self._specs[strategy]

    # ================================================================ 工作執行緒
    def _open_worker_outbox(self):
        return ob_mod.open_outbox(self.path, busy_timeout=self._worker_busy_timeout)

    def _thread_main(self):
        try:
            outbox = self._open_worker_outbox()
        except Exception as e:  # noqa: BLE001
            self._ready_error = e
            logger.exception("A 頻道推播的工作執行緒開不了 outbox")
            self._ready.set()
            return
        self._ready.set()
        try:
            outbox = self._loop(outbox)
        finally:
            try:
                if outbox.closed:
                    outbox = self._open_worker_outbox()
                self._final_counts = outbox.counts()
            except Exception:  # noqa: BLE001
                logger.exception("A 頻道推播結束時讀不到 outbox 的現況")
            finally:
                outbox.close()

    def _loop(self, outbox):
        """工作執行緒的主迴圈。回傳最後使用的 outbox 連線（中途可能重開過）。

        等待規則（不忙等）：有新的列（handler 通知）、或有可以記回的結果就立刻再跑一輪；結果記不回 outbox 時
        （_results_retry_at 還沒到）不因為「佇列裡還有結果」而空轉，等到重試時刻。收到 finishing 之後再跑最後一輪就結束：
        那一輪還記不回的結果留給下次啟動（列停在 handoff_open，prepare() 會當成不確定）。
        """
        while True:
            with self._cond:
                final = self._finishing
            wake_at = None
            try:
                if outbox.closed:
                    outbox = self._open_worker_outbox()
                wake_at = self.pump(outbox, final=final)
            except Exception:  # noqa: BLE001 —— 工作執行緒不可以死：記 ERROR，隔一個重試間隔再試
                logger.exception("A 頻道推播處理 outbox 時發生未預期的例外，%s 秒後再試", self._retry_ms / 1000)
                wake_at = self._now() + self._retry_ms
                try:
                    outbox.close()          # 連線狀態可能已經不可信：下一輪重開
                except Exception:  # noqa: BLE001
                    pass
            with self._cond:
                if final:
                    return outbox
                if self._finishing:
                    continue                # 剛收到 finishing：再跑最後一輪
                if self._dirty or (self._results and not self._results_blocked()):
                    continue
                timeout = self._idle_poll
                if wake_at is not None:
                    timeout = min(timeout, max(0.0, (wake_at - self._now()) / 1000.0))
                if timeout > 0:
                    self._cond.wait(timeout)

    def _results_blocked(self):
        return self._results_retry_at is not None and self._mono() < self._results_retry_at

    def _results_wake_ms(self):
        """結果重試時刻換算成 now_ms 的時間軸（給 _loop 算等待秒數用）。"""
        remaining = max(0.0, self._results_retry_at - self._mono())
        return self._now() + int(remaining * 1000)

    def pump(self, outbox, final=False):
        """一輪：把發送器回報的結果依序記回 outbox，再（沒有正在交出的、也沒有在停止時）交出下一列。
        回傳下一個該醒來的 epoch ms（重試時刻；沒有就 None）。工作執行緒與測試共用這一個函式。
        final=True（關閉前最後一輪）時不等記回結果的重試間隔，直接再試一次。"""
        with self._cond:
            self._dirty = False
        if not self._record_results(outbox, final=final):
            return self._results_wake_ms()
        with self._cond:
            if self._stopping or self._in_flight is not None:
                return None
        return self._dispatch(outbox)

    def _record_results(self, outbox, final=False):
        """把發送器回報的結果依序記回 outbox（BUG-009）。

        一次只處理佇列頭的那一則：COMMIT 成功之後才把它移出佇列、清掉 _in_flight。記回失敗（outbox 被鎖住超過
        busy timeout、磁碟錯誤……）時結果**留在佇列頭**、_in_flight 不清 —— 所以不會交出新的列，同一 signal_id 的出場
        也不會越過進場 —— 並記 ERROR，A_CHANNEL_OUTBOX_RETRY_SECONDS 之後再記（以單調時鐘 mono 計，不忙等）。
        「已送達」這個結果因此不會遺失，頻道上也不會因為重啟時被當成不確定而重複一則。回傳 True = 全部記完。
        """
        if not final and self._results_blocked():
            return False
        try:
            while True:
                with self._cond:
                    if not self._results:
                        break
                    token, result = self._results[0]
                self._record_result(outbox, token, result)
                with self._cond:
                    self._results.popleft()         # 只有工作執行緒會移出；on_done 只從尾端加入
                    if self._in_flight == token:
                        self._in_flight = None
        except Exception as e:  # noqa: BLE001 —— 任何記回失敗都一樣：結果保留、稍後再記
            self._results_retry_at = self._mono() + self._retry_ms / 1000.0
            self.stats["record_failures"] += 1
            logger.error("A 頻道推播：發送器回報的結果記不回 outbox（%s: %s），結果先留在記憶體裡，%s 秒後再記；"
                         "記回之前不交出新的列", type(e).__name__, e, self._retry_ms / 1000)
            return False
        self._results_retry_at = None
        return True

    def has_results(self):
        """發送器已經回報、還沒記回 outbox 的結果有沒有（給測試等條件用）。"""
        with self._cond:
            return bool(self._results)

    def in_flight(self):
        with self._cond:
            return self._in_flight

    def _dispatch(self, outbox):
        now = self._now()
        wake_at = None
        for row in outbox.pending():
            if row["handoff_open"]:
                # 走到這裡時一定沒有正在交出的（pump 只在 _in_flight 是 None 時派送），所以這一列是「交出去了、
                # 結果卻沒有記回來」的孤兒（BUG-009 的第二道防線）。比照重啟時的做法：當成不確定、立刻重新排送
                if outbox.release_orphan_handoff(row["seq"], now_ms=now):
                    self.stats["orphans"] += 1
                    logger.warning("A 頻道推播：%s/%s 交出去之後結果沒有記回 outbox，視為可能已送達，重新排送"
                                   "（不再套延遲門檻、重送同一段文字）", row["signal_id"], row["kind"])
                row = outbox.get_by_seq(row["seq"])
                if row is None or row["status"] != ob_mod.STATUS_PENDING or row["handoff_open"]:
                    continue
            nxt = row["next_attempt_ms"]
            if nxt is not None and nxt > now:
                wake_at = nxt if wake_at is None else min(wake_at, nxt)
                continue
            try:
                if row["kind"] == ob_mod.KIND_ENTRY:
                    decided = self._dispatch_entry(outbox, row, now)
                else:
                    decided = self._dispatch_exit(outbox, row, now)
            except (TypeError, ValueError, KeyError) as e:
                # 事件重建不出來、快照缺欄位：這一列永遠組不出字，記永久失敗（ERROR），不讓它每分鐘卡住一次
                logger.error("A 頻道推播 %s（%s）組不出訊息，記為永久失敗：%s: %s", row["signal_id"], row["kind"],
                             type(e).__name__, e)
                outbox.finalize(row["seq"], ob_mod.STATUS_FAILED, now_ms=now,
                                detail="組不出訊息：%s: %s" % (type(e).__name__, e))
                self.stats["failed"] += 1
                continue
            if decided == _HANDED:
                return wake_at
        return wake_at

    def _finalize_expired(self, outbox, row, now, delay_ms, where):
        outbox.finalize(row["seq"], ob_mod.STATUS_EXPIRED, now_ms=now,
                        detail="%s已晚於訊號 K 棒收盤 %.1f 秒（門檻 %s 秒）"
                               % (where, delay_ms / 1000.0, self._max_delay_ms / 1000.0))
        self.stats["expired"] += 1
        logger.warning("A 頻道推播：進場 %s %s已晚於訊號 K 棒收盤 %.1f 秒（門檻 %s 秒），延遲不發；"
                       "之後的出場也不發、報表不計", row["signal_id"], where, delay_ms / 1000.0,
                       self._max_delay_ms / 1000.0)
        return _FINALIZED

    def _dispatch_entry(self, outbox, row, now):
        snap = row["snapshot"]
        if snap.get("price_decimals_source") == PRICE_SOURCE_STALE:
            # 收到時就已過門檻（F6）：不看現在的時鐘（時鐘往回調也一樣），永遠不組字
            return self._finalize_expired(outbox, row, now, int(snap.get("stale_delay_ms") or 0), "收到時")
        close_ms = int(snap["signal_close_ms"])
        delay_ms = now - close_ms
        if not row["uncertain"] and delay_ms > self._max_delay_ms:
            return self._finalize_expired(outbox, row, now, delay_ms, "送出前")
        if row["uncertain"] and row["text"]:
            text = row["text"]              # 上一次交出的結果不確定：重送同一段文字
        else:
            text = text_mod.render(EntryEvent(**row["event"]), snap)
        expires_at = None if row["uncertain"] else (close_ms + self._max_delay_ms) / 1000.0
        return self._handoff(outbox, row, text, expires_at, now)

    def _dispatch_exit(self, outbox, row, now):
        entry = outbox.get(row["signal_id"], ob_mod.KIND_ENTRY)
        if entry is not None and entry["status"] == ob_mod.STATUS_PENDING:
            return _WAITING                 # 進場還沒有終態：出場等它
        if entry is None or entry["status"] != ob_mod.STATUS_DELIVERED:
            why = ("outbox 裡沒有它的進場（進場在推播啟用之前就發布了）" if entry is None
                   else "進場是「%s」" % ob_mod.STATUS_LABELS.get(entry["status"], entry["status"]))
            outbox.finalize(row["seq"], ob_mod.STATUS_NO_ENTRY, now_ms=now, detail=why)
            self.stats["no_entry"] += 1
            level = logging.INFO if entry is not None and entry["status"] == ob_mod.STATUS_EXPIRED else logging.WARNING
            logger.log(level, "A 頻道推播：出場 %s 不發（因進場未出現）：%s", row["signal_id"], why)
            return _FINALIZED
        event = ExitEvent(**row["event"])
        if row["uncertain"] and row["text"]:
            text = row["text"]
        else:
            delayed = bool(event.features.get("recovered")) or now - event.closed_ms > self._max_delay_ms
            text = text_mod.render(event, row["snapshot"], delayed=delayed)
        return self._handoff(outbox, row, text, None, now)

    def _handoff(self, outbox, row, text, expires_at, now):
        seq, key = row["seq"], "%s/%s" % (row["signal_id"], row["kind"])
        if not text.strip() or utf16_units(text) > config.TG_MAX_MESSAGE_CHARS:
            outbox.finalize(seq, ob_mod.STATUS_FAILED, now_ms=now,
                            detail="訊息長度 %d 超過上限或是空的" % utf16_units(text))
            self.stats["failed"] += 1
            logger.error("A 頻道推播 %s 的訊息長度 %d 超過上限 %d（或是空的），記為永久失敗", key, utf16_units(text),
                         config.TG_MAX_MESSAGE_CHARS)
            return _FINALIZED
        attempt = outbox.mark_handoff(seq, now_ms=now, text=text)       # 先 COMMIT「已交出」，再交出去
        token = (seq, attempt)
        with self._cond:
            self._in_flight = token
        try:
            accepted = self.sender.send(text, key=key, on_done=functools.partial(self._on_done, token),
                                        expires_at=expires_at)
        except Exception:  # noqa: BLE001 —— 例如發送器還沒 start：當成拒收，稍後重試
            logger.exception("A 頻道推播把 %s 交給發送器時拋出例外", key)
            accepted = False
        if not accepted:
            # 拒收（確定沒送出）也走結果佇列：記回 outbox 失敗時跟其他結果一樣保留、稍後再記，不會留下孤兒列（BUG-009）
            self._on_done(token, SendResult(status=_RESULT_REJECTED, key=key, message_id=None, uncertain=False,
                                            detail="發送器拒收（已停止、或發送執行緒不在），稍後重試"))
            return _HANDED
        self.stats["handoffs"] += 1
        logger.info("A 頻道推播：%s 交給發送器（第 %d 次%s）", key, attempt,
                    "；之前的結果不確定，不套延遲門檻" if row["uncertain"] else "")
        return _HANDED

    def _on_done(self, token, result):
        """ChannelSender 的 on_done（在發送器的執行緒）：只排進自己的佇列，由工作執行緒記回 outbox。"""
        with self._cond:
            self._results.append((token, result))
            self._cond.notify_all()

    def _record_result(self, outbox, token, result):
        """把一則結果寫進 outbox。寫入失敗就往外拋（_record_results 會保留這則結果、稍後再記）。"""
        seq, attempt = token
        row = outbox.get_by_seq(seq)
        if (row is None or row["status"] != ob_mod.STATUS_PENDING or not row["handoff_open"]
                or row["attempts"] != attempt):
            logger.warning("A 頻道推播收到一則對不上的發送結果（outbox 第 %s 列第 %s 次交出：%s），忽略", seq, attempt,
                           result.status)
            return
        key = "%s/%s" % (row["signal_id"], row["kind"])
        now = self._now()
        if result.status == RESULT_DELIVERED:
            outbox.finalize(seq, ob_mod.STATUS_DELIVERED, now_ms=now, message_id=result.message_id)
            self.stats["delivered"] += 1
            logger.info("A 頻道推播：%s 已送達（message_id=%s）", key, result.message_id)
        elif result.status == RESULT_FAILED:
            outbox.finalize(seq, ob_mod.STATUS_FAILED, now_ms=now, detail=result.detail)
            self.stats["failed"] += 1
            logger.error("A 頻道推播：%s 永久失敗（不重送%s）：%s", key,
                         "；Telegram 可能其實已經收到" if result.uncertain else "", result.detail)
        elif result.status == RESULT_EXPIRED:
            close_ms = row["snapshot"].get("signal_close_ms")
            delay = (now - int(close_ms)) / 1000.0 if close_ms is not None else float("nan")
            outbox.finalize(seq, ob_mod.STATUS_EXPIRED, now_ms=now,
                            detail="發送器在送出前發現已過期限（晚於訊號 K 棒收盤 %.1f 秒）" % delay)
            self.stats["expired"] += 1
            logger.warning("A 頻道推播：進場 %s 在發送器裡等到過期（晚於訊號 K 棒收盤 %.1f 秒，門檻 %s 秒），延遲不發；"
                           "之後的出場也不發、報表不計", row["signal_id"], delay, self._max_delay_ms / 1000.0)
        elif result.status == _RESULT_REJECTED:
            outbox.record_retry(seq, next_attempt_ms=now + self._retry_ms, uncertain=False, detail=result.detail)
            self.stats["retries"] += 1
            logger.warning("A 頻道推播：發送器拒收 %s（已停止、或發送執行緒不在），%s 秒後重試", key,
                           self._retry_ms / 1000)
        else:
            outbox.record_retry(seq, next_attempt_ms=now + self._retry_ms, uncertain=result.uncertain,
                                detail=result.detail or result.status)
            self.stats["retries"] += 1
            logger.warning("A 頻道推播：%s 這次沒有確定送達（%s%s），維持待送，%s 秒後重試", key, result.status,
                           "；請求可能已到 Telegram，之後不套延遲門檻" if result.uncertain else "",
                           self._retry_ms / 1000)

    def _now(self):
        return int(self._now_fn())


# ============================== 樣本訊息（FR-5 / AC-8） ==============================
EXIT_SENT, EXIT_FAILED, EXIT_NOT_TESTED = 0, 1, 3


def sample_cases():
    """(說明, 事件, 快照, delayed) 的清單。**寫死的假資料**：不碰 outbox / live.sqlite3 / 派網。

    signal_id、止盈 / 止損價、主週期都經 A3 自己的函式（make_signal_id / levels / build_specs）產生，
    所以樣本跟實盤會發出的事件長得一樣；價格精度、槓桿上限是假的快照值。
    """
    from live import notional_tracker as nt
    specs = nt.build_specs()
    s4, s5 = specs["s4"], specs["s5"]
    # 2026-09-18 15:35 台北時間（= 07:35 UTC）收盤的訊號 K 棒
    close_ms = 1789716900000

    def entry(spec, symbol, price, bar_close_ms):
        tp, sl = nt.levels(spec, price)
        return EntryEvent(strategy=spec.strategy, signal_id=nt.make_signal_id(spec, symbol, bar_close_ms),
                          symbol=symbol, direction=config.DIRECTION_SHORT, created_ms=bar_close_ms,
                          bar_open_ms=bar_close_ms - spec.main_ms, signal_price=price, take_profit_price=tp,
                          stop_loss_price=sl, features={"sample": True})

    def snap(e, decimals, max_lev, spec):
        return {"label": config.STRATEGY_LABELS[e.strategy], "price_decimals": decimals,
                "price_decimals_source": PRICE_SOURCE_SPEC, "signal_close_ms": e.bar_open_ms + spec.main_ms,
                "max_leverage": max_lev, "order_pct": config.A_CHANNEL_ORDER_PCT,
                "target_leverage": config.A_CHANNEL_TARGET_LEVERAGE,
                "deviation_warn_pct": config.A_CHANNEL_DEVIATION_WARN_PCT}

    def exit_of(e, reason, exit_price, hold_ms, **flags):
        feats = {"gap_open": False, "same_bar_both": False, "minor_resolved": False, "minor_missing": False,
                 "recovered": False, "data_gap": False}
        feats.update(flags)
        opened = e.bar_open_ms + specs[e.strategy].main_ms
        return ExitEvent(strategy=e.strategy, signal_id=e.signal_id, symbol=e.symbol, direction=e.direction,
                         created_ms=opened + hold_ms, reason=reason, exit_price=exit_price,
                         entry_price=e.signal_price, opened_ms=opened, closed_ms=opened + hold_ms, features=feats)

    minute = 60000
    ace = entry(s4, "ACE_USDT_PERP", 0.15346, close_ms)
    ordi = entry(s4, "ORDI_USDT_PERP", 8.123, close_ms)
    newc = entry(s4, "NEWCOIN_USDT_PERP", 0.004321, close_ms)
    ace5 = entry(s5, "ACE_USDT_PERP", 0.15402, close_ms + minute)
    return [
        ("策略4 進場（該幣上限 ≥ 50x）", ace, snap(ace, 5, 75, s4), False),
        ("策略4 進場（該幣上限 20x）", ordi, snap(ordi, 3, 20, s4), False),
        ("策略4 進場（該幣上限取不到）", newc, snap(newc, 6, None, s4), False),
        ("策略5 進場", ace5, snap(ace5, 5, 50, s5), False),
        ("止盈出場", exit_of(ace, config.EXIT_TAKE_PROFIT, ace.take_profit_price, 85 * minute),
         {"label": config.STRATEGY_LABELS["s4"], "price_decimals": 5}, False),
        ("一般止損", exit_of(ordi, config.EXIT_STOP_LOSS, ordi.stop_loss_price, 42 * minute),
         {"label": config.STRATEGY_LABELS["s4"], "price_decimals": 3}, False),
        ("跳空止損", exit_of(ordi, config.EXIT_STOP_LOSS, round(ordi.stop_loss_price * 1.021, 3), 190 * minute,
                          gap_open=True), {"label": config.STRATEGY_LABELS["s4"], "price_decimals": 3}, False),
        ("同根兩碰止損", exit_of(newc, config.EXIT_STOP_LOSS, newc.stop_loss_price, 12 * minute, same_bar_both=True,
                            minor_resolved=True), {"label": config.STRATEGY_LABELS["s4"], "price_decimals": 6}, False),
        ("延遲註記的出場（重啟後補發）", exit_of(ace5, config.EXIT_TAKE_PROFIT, ace5.take_profit_price, 1570 * minute,
                                       recovered=True), {"label": config.STRATEGY_LABELS["s5"], "price_decimals": 5},
         True),
    ]


def sample_messages():
    """組出 [測試] 樣本訊息：[(key, 文字)]，9 則。"""
    cases = sample_cases()
    out = []
    for i, (label, event, snapshot, delayed) in enumerate(cases, 1):
        head = "[測試] %d/%d：%s" % (i, len(cases), label)
        body = text_mod.render(event, snapshot, delayed=delayed)
        out.append(("t2-sample-%d" % i, head + "\n\n" + body))
    return out


def sample(stop_timeout=180):
    """把樣本訊息經 ChannelSender 送到 A 頻道並等送完。回傳 exit code。缺密鑰時不碰網路、不建日誌檔。"""
    missing = [name for name in (config.TG_BOT_TOKEN_ENV, config.TG_CHANNEL_ID_ENV)
               if not config.secret_is_set(name)]
    if missing:
        print("未實測（缺密鑰）：%s 未設定，沒有送出任何訊息" % "、".join(missing))
        return EXIT_NOT_TESTED
    messages = sample_messages()
    from live import logsetup
    logsetup.setup()
    results = []
    lock = threading.Lock()

    def done(result):
        with lock:
            results.append(result)

    sender = ChannelSender()
    sender.start()
    for key, text in messages:
        sender.send(text, key=key, on_done=done)
    remaining = sender.stop(timeout=stop_timeout)
    with lock:
        delivered = sum(1 for r in results if r.status == RESULT_DELIVERED)
        by_status = collections.Counter(r.status for r in results)
    print("樣本訊息：共 %d 則，已送達 %d；結果分布 %s；stop() 未送出 %d"
          % (len(messages), delivered, dict(by_status), remaining))
    if delivered == len(messages):
        return EXIT_SENT
    print("有樣本沒有送達，最後錯誤：%s" % sender.stats()["last_error"])
    return EXIT_FAILED


def main(argv=None):
    stream = sys.stdout
    if stream is not None and hasattr(stream, "reconfigure"):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (OSError, ValueError):
            pass
    parser = argparse.ArgumentParser(
        prog="python -m live.a_channel_push",
        description="A 頻道推播（T2）的樣本訊息。必須明確帶 --sample 才會送出訊息。")
    parser.add_argument("--sample", action="store_true", required=True,
                        help="用寫死的假資料組出各種情境的 [測試] 訊息，送到 A 頻道（不碰 outbox / 資料庫 / 派網）")
    parser.parse_args(argv)
    return sample()


if __name__ == "__main__":
    # python -m live.a_channel_push 會把本檔載入成 __main__；從正式的模組名取 main()（同 live.a_channel 的理由）
    from live.a_channel_push import main as _main
    sys.exit(_main())
