# -*- coding: utf-8 -*-
"""
live.ops_alert — A4 最小營運告警：把 A 頻道程式的異常與每日心跳推到營運方自己的 Telegram 私人聊天
====================================================================================================
只在 `python -m live.a_channel --push-tg` 時由 live.a_channel 建立並啟動；不帶旗標時這個模組連 import 都不會。
發到使用者與機器人的**私人聊天**（環境變數 CRYPTO_TRADER_TG_OPS_CHAT_ID，與 A 頻道共用同一個 bot token），
不是 A 頻道。發送用另一個 live.tg_channel.ChannelSender（執行緒 tg-ops-sender、日誌文字「維運告警」）。

元件
    OpsAlerter        告警器本體：日誌 handler + 執行緒 ops-alert（分組、輪詢、心跳、組訊息、交給發送器）
    _AlertHandler     掛在 root logger 上的 logging.Handler（level WARNING）
    compose()         組一則訊息（七種，格式見下面「訊息格式」）
    python -m live.ops_alert --sample   寫死的假資料組出七種各一則，開頭加 [測試]，經維運發送器送出

──────────────────────────────────────────────────────────────────────
事件告警（日誌 ERROR / CRITICAL；FR-3）
──────────────────────────────────────────────────────────────────────
handler 的 emit() 只做本機的事：把 (時刻, levelno, logger 名稱, 檔名, lineno, threadName, getMessage()) 放進
有上限的佇列（OPS_ALERT_QUEUE_MAX，滿了丟最舊的並計數）就返回；不打網路、不做 I/O、不拋例外。
WARNING 與 ERROR（含 CRITICAL）的總數另外計數給心跳，只有 ERROR 以上才進佇列。
排除（避免告警自己出錯變成無限迴圈）：threadName 是 ops-alert / tg-ops-sender 的 record、logger 是 live.ops_alert 的 record。

ops-alert 執行緒依分組鍵 (logger 名稱, 檔名, lineno) 處理：
  1. 新鍵 → 等 OPS_ALERT_BATCH_SECONDS 把同一段時間的新鍵收齊，一起發一則 🔴（×N 是累計次數）
  2. 之後每滿 OPS_ALERT_REMIND_SECONDS 看這段時間的次數：> 0 → ⏰「過去 1 小時又發生 N 次」；= 0 → 鍵結束，
     曾經被提醒過的才在 ✅ 列「過去 1 小時沒有再發生（共 N 次）」，只發生過一次的安靜結束
  3. CRITICAL 不等批次、不受寬限，下一輪就發
  啟動後 OPS_ALERT_STARTUP_GRACE_SECONDS 內的 ERROR 不逐一告警，寬限結束時併成一則 🟡（沒有就不發）。
  begin_shutdown() 之後的 ERROR 不逐一告警，連同還沒發出的（寬限中、批次中）併進結束訊息 ⚪。

──────────────────────────────────────────────────────────────────────
由結構化結果觸發的一次性事件（FR-4）
──────────────────────────────────────────────────────────────────────
  對帳 out_of_coverage  note_reconcile(result) 數窗口數（不加總 requests 等整輪共用的欄位），跟同一段時間的
                        其他新事件一起進下一則 🔴：「對帳：N 個小時窗口超出 klines 涵蓋，沒有對帳（多半是停頓過久）」
  R-A 跳過              reporter.stats()["skipped"] 比上一輪多 →「報表：N 期超過補發上限，跳過未發」

──────────────────────────────────────────────────────────────────────
條件告警（每 OPS_ALERT_POLL_SECONDS 輪詢 stats；FR-5）：raised → 🔴、每 OPS_ALERT_REMIND_SECONDS ⏰、cleared → ✅
──────────────────────────────────────────────────────────────────────
  C1 執行緒不在   已啟動的元件存活為假：A3、A5、T2 pusher、A 頻道發送器、R-A、對帳（各自一個條件）。
                  attach() 之後才算已啟動；對帳的執行緒在 feed.run() 裡才啟動，_thread 還是 None 時不算不在
  C2 A1 停擺      距離上一次 note_bar() 超過 OPS_A1_STALL_SECONDS（還沒收到第一根之前從告警器啟動起算）
  C3 A5 停擺      s5.stats()["minutes"] 超過 OPS_A5_STALL_SECONDS 沒有增加
  C4 A 頻道待送卡住  outbox（唯讀開、用完就關）最舊的待送列超過 OPS_OUTBOX_STUCK_SECONDS；開不了 = 這一輪無法判斷
  C5 報表組不出來  reporter.stats()["compose_errors"] 連續 OPS_COUNTER_RAISE_POLLS 輪增加 / 連續 OPS_COUNTER_CLEAR_POLLS 輪沒增加
  C6 A3 取數持續失敗  tracker.stats["fetch_failures"]，規則同 C5
讀某個元件的 stats 拋例外 → 那一輪略過那個元件（不 raise、不 clear），同一種錯誤只記一次 WARNING。
開始關閉之後不再輪詢（各元件正在依序停止，存活為假是預期的）。

──────────────────────────────────────────────────────────────────────
心跳（FR-6）
──────────────────────────────────────────────────────────────────────
每天 OPS_HEARTBEAT_TIME_TPE（台北）發一則 💓；不持久化，程式那一刻沒在跑就不發。統計區間 = 上一次心跳
（或本次啟動）到現在，用各元件累計值的差值計算（本行程裡的計數器都從 0 起算）。A 頻道的進場 / 出場送達、
延遲不發、永久失敗從 outbox 讀「在統計區間內結案的列」（T2 的 stats 不分進場 / 出場）。

──────────────────────────────────────────────────────────────────────
訊息格式（FR-8）
──────────────────────────────────────────────────────────────────────
第一行「符號 + 標題 + 兩個空白 + 機器標籤」，第二行「時間：YYYY-MM-DD HH:MM:SS」（台北），接著「標籤：值」各行，
最後是項目行：
    ・[ERROR] <logger 名稱> L<lineno> ×<次數>：<訊息>        （CRITICAL 寫 [CRITICAL]）
    ・[條件] <條件名稱>：<說明>
    ・[事件] 對帳 / 報表：<說明>                               （FR-4 的一次性事件）
每項的訊息先遮罩、空白與換行併成一個空白，再截到 OPS_ALERT_ITEM_MAX_CHARS 字（含結尾的「…」）；一則最多
OPS_ALERT_MAX_ITEMS 項，其餘寫「…另有 N 項（見日誌）」；整則 ≤ TG_MAX_MESSAGE_CHARS（UTF-16 code units，
超過就再少列幾項）。發出前整段經過 SecretMasker（SECRET_ENV_VARS 裡所有有設定的值）。
同一輪同時有新告警、提醒、恢復時分成三則，依 🔴 → ⏰ → ✅ 的順序。每則交出時記一行 INFO（live.ops_alert）：
「已交出 <種類>（<中文名>）：N 項，鍵 …」；發送器拒收記 WARNING（「沒有交出」）。

──────────────────────────────────────────────────────────────────────
啟動與結束（由 live.a_channel 呼叫；FR-7）
──────────────────────────────────────────────────────────────────────
  start()              維運發送器 start()（缺密鑰拋 MissingSecretError）→ 掛 handler → 啟動 ops-alert 執行緒
  attach(...)          各元件建好 / 啟動之後交給告警器（tracker、push、reporter、s5、feed（取 gate 與 reconciler））
  wrap_on_result(fn)   A1 的 on_result 包一層：先呼叫原本的（A3），finally 再 note_bar()；原本的例外照樣往外拋
  note_reconcile(r)    給 Reconciler(on_result=...)
  announce_started()   全部啟動成功、feed.run() 之前：🟢
  begin_shutdown()     開始關閉：之後的 ERROR 併進 ⚪，不再輪詢
  finish(reason, rc)   最後一步：停 ops-alert 執行緒 → 組並交出 ⚪ → 拆 handler → 維運發送器 stop()；回傳最終 exit code
                       （ops-alert 執行緒執行中意外結束、或停不下來 → exit 1）
告警器的任何錯誤都不往外拋到訊號路徑：emit() / note_bar() / note_reconcile() 不拋例外，執行緒死掉只記 ERROR，
其他元件照跑（WBS §10 #14）。
"""

import argparse
import collections
import logging
import os
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta

from live import a_channel_outbox, config
from live.a_channel_report import open_readonly
from live.logsetup import TAIPEI
from live.tg_channel import RESULT_DELIVERED, ChannelSender, SecretMasker, utf16_units

LOGGER_NAME = "live.ops_alert"
logger = logging.getLogger(LOGGER_NAME)

THREAD_NAME = "ops-alert"
SENDER_THREAD_NAME = "tg-ops-sender"
SENDER_LOG_LABEL = "維運告警"
EXCLUDED_THREAD_NAMES = (THREAD_NAME, SENDER_THREAD_NAME)

# --sample 的 exit code（比照 live.a_channel_push）
EXIT_SENT, EXIT_FAILED, EXIT_NOT_TESTED = 0, 1, 3
SAMPLE_PREFIX = "[測試] "

# 七種訊息
KIND_STARTUP = "startup"
KIND_GRACE = "grace"
KIND_ALERT = "alert"
KIND_REMIND = "remind"
KIND_RECOVERED = "recovered"
KIND_HEARTBEAT = "heartbeat"
KIND_SHUTDOWN = "shutdown"
KINDS = (KIND_STARTUP, KIND_GRACE, KIND_ALERT, KIND_REMIND, KIND_RECOVERED, KIND_HEARTBEAT, KIND_SHUTDOWN)
KIND_NAMES = {KIND_STARTUP: "啟動", KIND_GRACE: "啟動期間的錯誤", KIND_ALERT: "告警", KIND_REMIND: "仍未恢復",
              KIND_RECOVERED: "已恢復", KIND_HEARTBEAT: "每日心跳", KIND_SHUTDOWN: "結束"}
# 程式用到的符號一律用 chr() 產生（docstring / 註解裡的符號只是說明）
SYMBOLS = {KIND_STARTUP: chr(0x1F7E2),      # 綠色圓
           KIND_GRACE: chr(0x1F7E1),        # 黃色圓
           KIND_ALERT: chr(0x1F534),        # 紅色圓
           KIND_REMIND: chr(0x23F0),        # 鬧鐘
           KIND_RECOVERED: chr(0x2705),     # 白色勾（綠底）
           KIND_HEARTBEAT: chr(0x1F493),    # 跳動的心
           KIND_SHUTDOWN: chr(0x26AA)}      # 白色圓
TITLES = {KIND_STARTUP: SYMBOLS[KIND_STARTUP] + " A 頻道程式啟動",
          KIND_GRACE: SYMBOLS[KIND_GRACE] + " 啟動期間的錯誤",
          KIND_ALERT: SYMBOLS[KIND_ALERT] + " 告警",
          KIND_REMIND: SYMBOLS[KIND_REMIND] + " 仍未恢復",
          KIND_RECOVERED: SYMBOLS[KIND_RECOVERED] + " 已恢復",
          KIND_HEARTBEAT: SYMBOLS[KIND_HEARTBEAT] + " 每日心跳",
          KIND_SHUTDOWN: SYMBOLS[KIND_SHUTDOWN] + " A 頻道程式結束"}
TITLE_SEPARATOR = "  "
FIELD_SEPARATOR = "："
BULLET = "・"
ELLIPSIS = "…"
TIME_LABEL = "時間"

# 心跳的「告警」那一行計哪幾種
ALERT_KINDS = (KIND_ALERT, KIND_GRACE, KIND_REMIND, KIND_RECOVERED)

# A1 只產生 s4、A5 只產生 s5（見 live.a_channel）；顯示名稱取自 config.STRATEGY_LABELS。
# 分兩行寫：這不是策略名單（名單只在 config.STRATEGIES），是兩個元件各自的策略代號
_A1_STRATEGY = "s4"
_A5_STRATEGY = "s5"

# 條件（FR-5）。C1 每個元件一個條件：(代號, 顯示名稱)
C1_COMPONENTS = (("a3", "A3"), ("a5", "A5"), ("t2", "T2 pusher"), ("sender", "A 頻道發送器"), ("ra", "R-A"),
                 ("reconcile", "對帳"))
C2, C3, C4, C5, C6 = "C2", "C3", "C4", "C5", "C6"
CONDITION_NAMES = {C2: "C2 A1 停擺", C3: "C3 A5 停擺", C4: "C4 A 頻道待送卡住", C5: "C5 報表組不出來",
                   C6: "C6 A3 取數持續失敗"}
CONDITION_NAMES.update({"C1:" + code: "C1 執行緒不在（%s）" % label for code, label in C1_COMPONENTS})

# FR-4 的一次性事件
ONESHOT_UNCOVERED = "FR4:reconcile_out_of_coverage"
ONESHOT_REPORT_SKIPPED = "FR4:report_skipped"

_UNSTARTED = "—（未啟動）"
_UNREADABLE = "—（讀不到）"


# ============================== 小工具 ==============================
def instance_label():
    """每則訊息標題後面的機器標籤：OPS_ALERT_INSTANCE_LABEL，沒設就是主機名稱。"""
    return config.OPS_ALERT_INSTANCE_LABEL or socket.gethostname()


def new_sender():
    """維運告警用的 ChannelSender：發到 OPS_CHAT_ID_ENV，執行緒 tg-ops-sender，日誌文字「維運告警」。"""
    return ChannelSender(chat_id_env=config.OPS_CHAT_ID_ENV, thread_name=SENDER_THREAD_NAME,
                         log_label=SENDER_LOG_LABEL)


def build_masker():
    """SECRET_ENV_VARS 裡所有**有設定**的值組成的 SecretMasker（bot token、A 頻道 id、維運 chat id）。"""
    values = [config.require_secret(name) for name in config.SECRET_ENV_VARS if config.secret_is_set(name)]
    return SecretMasker(values)


def fmt_time(epoch_s):
    """台北時間 YYYY-MM-DD HH:MM:SS。"""
    return datetime.fromtimestamp(epoch_s, TAIPEI).strftime("%Y-%m-%d %H:%M:%S")


def fmt_duration(seconds):
    """秒數 → 「N 秒」「M 分 S 秒」「H 小時 M 分」「D 天 H 小時」（為零的尾巴省略）。"""
    s = max(0, int(round(seconds)))
    if s < 60:
        return "%d 秒" % s
    m, sec = divmod(s, 60)
    if m < 60:
        return "%d 分" % m + (" %d 秒" % sec if sec else "")
    h, m = divmod(m, 60)
    if h < 24:
        return "%d 小時" % h + (" %d 分" % m if m else "")
    d, h = divmod(h, 24)
    return "%d 天" % d + (" %d 小時" % h if h else "")


def next_heartbeat_after(epoch_s):
    """epoch_s 之後（嚴格大於）的下一個台北 OPS_HEARTBEAT_TIME_TPE。"""
    hh, mm = (int(x) for x in config.OPS_HEARTBEAT_TIME_TPE.split(":"))
    now = datetime.fromtimestamp(epoch_s, TAIPEI)
    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target.timestamp()


def _clip(text):
    """一項的訊息：空白與換行併成一個空白，超過 OPS_ALERT_ITEM_MAX_CHARS 字就截斷，結尾換成「…」（總長不超過上限）。"""
    text = " ".join(str(text).split())
    limit = config.OPS_ALERT_ITEM_MAX_CHARS
    if len(text) > limit:
        text = text[:max(0, limit - len(ELLIPSIS))] + ELLIPSIS
    return text


def compose(kind, now_s, label, fields=(), items=(), masker=None):
    """組一則訊息。fields：[(標籤, 值)]；items：[(鍵, 項目標頭, 訊息)]。回傳遮罩後的全文。

    每項的訊息先遮罩再截斷（密鑰不會被截成漏網的片段），全文最後再遮罩一次（欄位、標籤也算）。
    超過 OPS_ALERT_MAX_ITEMS 項、或整則超過 TG_MAX_MESSAGE_CHARS 時少列幾項，最後一行寫「…另有 N 項（見日誌）」。
    """
    mask = masker if masker is not None else (lambda s: s)
    head = [TITLES[kind] + TITLE_SEPARATOR + label, TIME_LABEL + FIELD_SEPARATOR + fmt_time(now_s)]
    head += ["%s%s%s" % (name, FIELD_SEPARATOR, value) for name, value in fields]
    rendered = [BULLET + item_head + FIELD_SEPARATOR + _clip(mask(str(body))) for _, item_head, body in items]
    shown = rendered[:config.OPS_ALERT_MAX_ITEMS]
    hidden = len(rendered) - len(shown)
    while True:
        lines = head + shown
        if hidden:
            lines.append(ELLIPSIS + "另有 %d 項（見日誌）" % hidden)
        text = mask("\n".join(lines))
        if utf16_units(text) <= config.TG_MAX_MESSAGE_CHARS or not shown:
            return text
        shown.pop()
        hidden += 1


# ============================== 各種訊息的欄位（純函式，--sample 也用） ==============================
def startup_fields(duration_s, outbox_pending, report_pending):
    if duration_s is None:
        params = "--push-tg（不帶 --duration，跑到 Ctrl+C 為止）"
    else:
        params = "--push-tg、--duration %s 秒" % format(duration_s, "g")
    return [("參數", params), ("outbox 待送", outbox_pending), ("報表待送", report_pending)]


def shutdown_fields(now_s, started_s, reason, rc, outbox_pending, raised_names, dropped, closing_errors,
                    unsent_items, thread_note=None):
    fields = [("運作時間", "%s（自 %s 起）" % (fmt_duration(now_s - started_s), fmt_time(started_s))),
              ("結束原因", reason or "不明"),
              ("exit code", rc),
              ("outbox 待送", outbox_pending),
              ("仍未恢復的條件", "、".join(raised_names) if raised_names else "無"),
              ("日誌佇列丟棄", "%d 筆" % dropped),
              ("關閉過程中的 ERROR", "%d 筆" % closing_errors if closing_errors else "無")]
    if unsent_items:
        fields.append(("關閉前還沒發出的告警", "%d 項（一併列在下面）" % unsent_items))
    if thread_note:
        fields.append(("告警執行緒", thread_note))
    return fields


def heartbeat_fields(now_s, started_s, since_s, first, d):
    """d：統計區間內的差值。每一段是 dict，或是顯示用的字串（未啟動 / 讀不到）。"""
    hours = (now_s - since_s) / 3600.0
    span = ("本次啟動以來" if first else "上一次心跳以來") + " %.1f 小時（自 %s 起）" % (hours, fmt_time(since_s))
    s4, s5 = config.STRATEGY_LABELS[_A1_STRATEGY], config.STRATEGY_LABELS[_A5_STRATEGY]

    def sec(name, fn):
        v = d.get(name)
        return v if isinstance(v, str) else fn(v)

    def a1(v):
        top = v["reasons"].most_common(3)
        detail = "（%s）" % "、".join("%s ×%d" % (r, n) for r, n in top) if top else ""
        return "處理 %d 根，其中 degraded %d 根%s" % (v["bars"], v["degraded"], detail)

    def raw(_):
        a5 = d.get("a5")
        a5_text = a5 if isinstance(a5, str) else "%d 筆" % a5["signals"]
        return "%s %d 筆、%s %s（含被持倉或冷卻擋掉的）" % (s4, d["a1"]["signals"], s5, a5_text)

    alerts = d["alerts"]
    raised = d["raised"]
    return [
        ("統計區間", span),
        ("運作時間", "%s（自 %s 起）" % (fmt_duration(now_s - started_s), fmt_time(started_s))),
        (s4 + " K 棒", sec("a1", a1)),
        (s5 + " 分鐘", sec("a5", lambda v: "處理 %d 分鐘，degraded %d、missed %d"
                                           % (v["minutes"], v["degraded"], v["missed"]))),
        ("原始訊號", raw(None)),
        ("A3", sec("a3", lambda v: "進場 %d、出場 %d、資料庫失敗 %d" % (v["entries"], v["exits"], v["store_failures"]))),
        ("A 頻道", sec("outbox", lambda v: "進場送達 %d、延遲不發 %d、永久失敗 %d、出場送達 %d"
                                           % (v["entry_delivered"], v["expired"], v["failed"], v["exit_delivered"]))),
        ("報表", sec("ra", lambda v: "送達 %d、跳過 %d" % (v["delivered"], v["skipped"]))),
        ("REST", sec("gate", lambda v: "請求 %d、429 %d 次" % (v["requests"], v["http_429"]))),
        ("日誌", "ERROR %d、WARNING %d、佇列丟棄 %d" % (d["log"]["errors"], d["log"]["warnings"], d["log"]["dropped"])),
        ("告警", "發出 %d 則（%s）；仍未恢復：%s"
         % (sum(alerts.get(k, 0) for k in ALERT_KINDS),
            "、".join("%s %d" % (SYMBOLS[k], alerts.get(k, 0)) for k in ALERT_KINDS),
            "、".join(raised) if raised else "無")),
    ]


# ============================== 事件與條件的狀態 ==============================
class _Event:
    """一個日誌點（分組鍵 = (logger 名稱, 檔名, lineno)）的累計。"""

    __slots__ = ("name", "filename", "lineno", "levelno", "msg", "total", "window", "reminded", "next_check")

    def __init__(self, name, filename, lineno):
        self.name = name
        self.filename = filename
        self.lineno = lineno
        self.levelno = logging.ERROR
        self.msg = ""
        self.total = 0          # 累計次數（訊息裡的 ×N）
        self.window = 0         # 這個提醒週期內的次數
        self.reminded = False   # 發過 ⏰ 沒有（結束時只有提醒過的才發 ✅）
        self.next_check = None

    def add(self, levelno, msg, in_window):
        self.total += 1
        if in_window:
            self.window += 1
        self.levelno = max(self.levelno, levelno)
        self.msg = msg

    def merge(self, other):
        """併入 other（結束訊息用：self 是關閉過程中的、other 是更早的寬限中 / 批次中的），訊息留最近一次的。"""
        self.total += other.total
        self.levelno = max(self.levelno, other.levelno)
        self.msg = self.msg or other.msg

    @property
    def key_text(self):
        return "%s|%s|L%d" % (self.name, self.filename, self.lineno)

    def head(self):
        return "[%s] %s L%d ×%d" % (logging.getLevelName(self.levelno), self.name, self.lineno, self.total)

    def item(self, body=None):
        return self.key_text, self.head(), self.msg if body is None else body


class _Raised:
    """一個 raised 中的條件。"""

    __slots__ = ("key", "raised_at", "next_remind", "desc")

    def __init__(self, key, now, desc):
        self.key = key
        self.raised_at = now
        self.next_remind = now + config.OPS_ALERT_REMIND_SECONDS
        self.desc = desc


class _Streak:
    """計數器型條件（C5 / C6）：連續幾輪有增加 / 沒增加。基準是 0（本行程的計數器從 0 起算）。"""

    __slots__ = ("last", "up", "flat")

    def __init__(self):
        self.last = 0
        self.up = 0
        self.flat = 0

    def update(self, value):
        if value > self.last:
            self.up += 1
            self.flat = 0
        else:
            self.flat += 1
            self.up = 0
        self.last = value


def _condition_head(key):
    return "[條件] " + CONDITION_NAMES[key]


def _fields(stats, *keys):
    """從一份 stats 取出幾個欄位（缺欄位一樣拋例外，由 OpsAlerter._read 當成「讀不到」）。"""
    return tuple(stats[k] for k in keys)


# ============================== 日誌 handler ==============================
class _AlertHandler(logging.Handler):
    """掛在 root logger 上；emit() 只把 record 交給 OpsAlerter._on_record（本機、有上限、不拋例外）。"""

    def __init__(self, owner):
        super().__init__(logging.WARNING)
        self._owner = owner

    def emit(self, record):
        try:
            self._owner._on_record(record)
        except Exception:  # noqa: BLE001 —— 告警的 handler 不可以讓寫日誌的那一方出事
            pass

    def handleError(self, record):  # noqa: N802 —— logging 的介面名稱
        pass


# ============================== 告警器 ==============================
class OpsAlerter:
    """A4 告警器。介面與規則見模組 docstring。

    sender  省略時用 new_sender()（維運聊天的 ChannelSender）；測試注入替身（要有 start / send / stop）
    clock   回傳 epoch 秒，預設 time.time；佇列的時刻、寬限、批次、提醒、心跳、條件都用它
    label   機器標籤，省略時用 instance_label()
    outbox_path  省略時用 attach 進來的 T2 pusher 的 path，再沒有就讀 config.A_CHANNEL_OUTBOX_DB_PATH（呼叫當下的值）
    """

    def __init__(self, *, sender=None, clock=None, label=None, outbox_path=None):
        self._sender = sender if sender is not None else new_sender()
        self._clock = clock or time.time
        self._label = label
        self._outbox_path = outbox_path
        self._masker = SecretMasker(())
        self._handler = None
        self._thread = None
        self._wake = threading.Event()
        self._stop_requested = False
        self._crashed = False
        self._shutting_down = False

        # handler 寫、ops-alert 讀（_qlock）
        self._qlock = threading.Lock()
        self._queue = collections.deque()
        self._dropped = 0
        self._n_errors = 0
        self._n_warnings = 0

        # note_bar / note_reconcile / 交出計數（_lock）
        self._lock = threading.Lock()
        self._last_bar_at = None
        self._bars = 0
        self._bars_degraded = 0
        self._reasons = collections.Counter()
        self._a1_signals = 0
        self._uncovered = 0
        self._sent = collections.Counter()

        # 元件（attach 之後才有）
        self._tracker = None
        self._push = None
        self._reporter = None
        self._s5 = None
        self._gate = None
        self._reconciler = None
        self._s5_minutes = None
        self._s5_changed_at = None

        # 只有 ops-alert 執行緒碰（finish() 在執行緒停了之後才碰）
        self._started_at = None
        self._grace_end = None
        self._grace_done = False
        self._grace = collections.OrderedDict()      # 寬限中的事件
        self._pending = collections.OrderedDict()    # 等批次的新事件
        self._active = collections.OrderedDict()     # 已發出、還在提醒週期內的事件
        self._closing = collections.OrderedDict()    # 開始關閉之後的事件
        self._pending_conditions = []                # 這一輪 raised 的條件（項目）
        self._oneshots = collections.OrderedDict()   # FR-4：鍵 → 次數（等批次）
        self._batch_deadline = None
        self._flush_now = False
        self._raised = collections.OrderedDict()     # 條件鍵 → _Raised
        self._cleared = []                           # 這一輪 cleared 的條件（項目）
        self._streaks = {C5: _Streak(), C6: _Streak()}
        self._ra_skipped_seen = 0
        self._next_poll = None
        self._next_heartbeat = None
        self._hb_since = None
        self._hb_first = True
        self._hb_prev = {}
        self._warned = set()

    # ---------------------------------------------------------------- 生命週期
    @property
    def label(self):
        if self._label is None:
            self._label = instance_label()
        return self._label

    @property
    def worker_alive(self):
        thread = self._thread
        return thread is not None and thread.is_alive()

    @property
    def crashed(self):
        return self._crashed

    def start(self, *, run_thread=True):
        """維運發送器 start()（缺密鑰拋 MissingSecretError，什麼都不掛）→ 掛 handler → 啟動 ops-alert 執行緒。

        run_thread=False 給測試：不起執行緒，由測試自己呼叫 run_once(now)。
        """
        self._sender.start()
        try:
            self._masker = build_masker()
            now = self._clock()
            self._started_at = now
            self._grace_end = now + config.OPS_ALERT_STARTUP_GRACE_SECONDS
            self._next_poll = now + config.OPS_ALERT_POLL_SECONDS
            self._next_heartbeat = next_heartbeat_after(now)
            self._hb_since = now
            self._handler = _AlertHandler(self)
            logging.getLogger().addHandler(self._handler)
            if run_thread:
                self._thread = threading.Thread(target=self._run, name=THREAD_NAME, daemon=True)
                self._thread.start()
        except BaseException:
            self._remove_handler()
            self._sender.stop(0)
            raise
        logger.info("維運告警已啟動：標籤 %s、輪詢 %s 秒、批次 %s 秒、提醒 %s 秒、啟動寬限 %s 秒、心跳 %s（台北）",
                    self.label, config.OPS_ALERT_POLL_SECONDS, config.OPS_ALERT_BATCH_SECONDS,
                    config.OPS_ALERT_REMIND_SECONDS, config.OPS_ALERT_STARTUP_GRACE_SECONDS,
                    config.OPS_HEARTBEAT_TIME_TPE)

    def attach(self, *, tracker=None, push=None, reporter=None, s5=None, feed=None):
        """元件建好（已啟動）之後交給告警器。feed 只取它的 gate 與 reconciler。"""
        if tracker is not None:
            self._tracker = tracker
        if push is not None:
            self._push = push
        if reporter is not None:
            self._reporter = reporter
        if feed is not None:
            self._gate = getattr(feed, "gate", None)
            self._reconciler = getattr(feed, "reconciler", None)
        if s5 is not None:
            try:
                minutes = s5.stats()["minutes"]
            except Exception:  # noqa: BLE001 —— 讀不到就等第一輪輪詢
                minutes = None
            self._s5_minutes = minutes
            self._s5_changed_at = self._clock()
            self._s5 = s5

    def begin_shutdown(self):
        """開始關閉：之後的 ERROR 不逐一告警，併進結束訊息；不再輪詢。"""
        self._shutting_down = True
        self._wake.set()

    def finish(self, reason, rc):
        """停 ops-alert 執行緒 → 組並交出 ⚪ → 拆 handler → 維運發送器 stop()。回傳最終的 exit code。不拋例外。"""
        self.begin_shutdown()
        thread_note = None
        try:
            thread = self._thread
            if thread is not None:
                self._stop_requested = True
                self._wake.set()
                thread.join(config.OPS_ALERT_STOP_TIMEOUT_SECONDS)
                if thread.is_alive():
                    thread_note = "沒有在 %s 秒內結束（見日誌）" % config.OPS_ALERT_STOP_TIMEOUT_SECONDS
                    logger.error("維運告警執行緒 %s 秒內沒有結束", config.OPS_ALERT_STOP_TIMEOUT_SECONDS)
                    rc = rc or 1
            if self._crashed:
                thread_note = "執行中意外結束（見日誌），之後沒有再發告警"
                rc = rc or 1
            now = self._clock()
            self._drain()
            self._deliver(KIND_SHUTDOWN, now, *self._shutdown_message(now, reason, rc, thread_note))
        except Exception:  # noqa: BLE001
            logger.exception("維運告警：組或交出結束訊息失敗")
            rc = rc or 1
        finally:
            self._remove_handler()
            try:
                remaining = self._sender.stop(config.OPS_ALERT_STOP_TIMEOUT_SECONDS)
                if remaining:
                    logger.warning("維運告警發送器停止時還有 %d 則沒有確認送出（見上面的紀錄）", remaining)
            except Exception:  # noqa: BLE001
                logger.exception("維運告警發送器 stop() 失敗")
        return rc

    def _remove_handler(self):
        if self._handler is not None:
            logging.getLogger().removeHandler(self._handler)
            self._handler = None

    # ---------------------------------------------------------------- 給其他元件呼叫（不拋例外）
    def wrap_on_result(self, fn):
        """A1 的 on_result 包一層：先呼叫原本的（A3 的 handler），finally 再 note_bar()；原本的例外照樣往外拋。"""
        def on_result(res):
            try:
                return fn(res)
            finally:
                self.note_bar(res)
        return on_result

    def note_bar(self, res):
        """記下收到一根 A1 的 BarResult（C2 與心跳用）。只記時刻與計數，不做 I/O、不拋例外。"""
        try:
            now = self._clock()
            reasons = list(getattr(res, "degraded_reasons", None) or ())
            signals = len(getattr(res, "signals", None) or ())
            with self._lock:
                self._last_bar_at = now
                self._bars += 1
                if reasons:
                    self._bars_degraded += 1
                    self._reasons.update(reasons)
                self._a1_signals += signals
        except Exception:  # noqa: BLE001
            pass

    def note_reconcile(self, result):
        """給 Reconciler(on_result=...)：數 out_of_coverage 的窗口（一個結果 = 一個窗口）。不拋例外。"""
        try:
            if getattr(result, "out_of_coverage", False):
                with self._lock:
                    self._uncovered += 1
                self._wake.set()
        except Exception:  # noqa: BLE001
            pass

    def announce_started(self, duration_s):
        """全部啟動成功、feed.run() 之前：🟢。失敗只記 ERROR（live.ops_alert，不會變成告警），不影響其他元件。"""
        try:
            now = self._clock()
            report = _UNSTARTED
            if self._reporter is not None:
                try:
                    report = "%s 期" % self._reporter.stats()["pending"]
                except Exception as e:  # noqa: BLE001
                    report = "讀不到（%s）" % type(e).__name__
            self._deliver(KIND_STARTUP, now, startup_fields(duration_s, self._outbox_pending_text(), report), [])
        except Exception:  # noqa: BLE001
            logger.exception("維運告警：組或交出啟動訊息失敗")

    # ---------------------------------------------------------------- handler（任何執行緒）
    def _on_record(self, record):
        levelno = record.levelno
        with self._qlock:
            if levelno >= logging.ERROR:
                self._n_errors += 1
            elif levelno >= logging.WARNING:
                self._n_warnings += 1
        if levelno < logging.ERROR:
            return
        if record.threadName in EXCLUDED_THREAD_NAMES or record.name == LOGGER_NAME:
            return
        try:
            msg = record.getMessage()
        except Exception:  # noqa: BLE001
            msg = str(record.msg)
        entry = (self._clock(), levelno, record.name, record.filename, record.lineno, record.threadName, msg)
        with self._qlock:
            was_empty = not self._queue
            if len(self._queue) >= config.OPS_ALERT_QUEUE_MAX:
                self._queue.popleft()
                self._dropped += 1
            self._queue.append(entry)
        if was_empty or levelno >= logging.CRITICAL:
            self._wake.set()

    def counters(self):
        """(ERROR 數, WARNING 數, 佇列丟棄數)。"""
        with self._qlock:
            return self._n_errors, self._n_warnings, self._dropped

    # ---------------------------------------------------------------- ops-alert 執行緒
    def _run(self):
        try:
            while not self._stop_requested:
                self._wake.clear()
                now = self._clock()
                self.run_once(now)
                if self._stop_requested:
                    break
                delay = min(max(self._next_due(now) - now, 1.0), float(config.OPS_ALERT_POLL_SECONDS))
                self._wake.wait(delay)
        except BaseException:  # noqa: BLE001 —— 執行緒死掉只記 ERROR，其他元件照跑；結束時 exit 1
            self._crashed = True
            logger.exception("維運告警執行緒意外結束，之後不再發告警（其他元件照跑，結束時 exit 1）")

    def _next_due(self, now):
        due = [self._next_poll, self._next_heartbeat]
        if not self._grace_done:
            due.append(self._grace_end)
        if self._flush_now:
            due.append(now)
        elif self._batch_deadline is not None:
            due.append(self._batch_deadline)
        due.extend(ev.next_check for ev in self._active.values())
        due.extend(c.next_remind for c in self._raised.values())
        return min(due)

    def run_once(self, now=None):
        """一輪：收佇列 →（到時）輪詢 → 寬限結束 → 🔴 → ⏰ → ✅ →（到時）心跳。開始關閉之後只收佇列。"""
        now = self._clock() if now is None else now
        self._drain()
        if self._shutting_down:
            return
        if now >= self._next_poll:
            self._poll(now)
            self._next_poll = now + config.OPS_ALERT_POLL_SECONDS
        self._collect_uncovered(now)

        if not self._grace_done and now >= self._grace_end:
            self._grace_done = True
            if self._grace:
                self._deliver(KIND_GRACE, now, [], [ev.item() for ev in self._grace.values()])
                for key, ev in self._grace.items():
                    self._activate(key, ev, now)
                self._grace.clear()

        alerts = []
        has_batch = self._pending or self._oneshots
        if self._pending_conditions or (has_batch and (self._flush_now or now >= self._batch_deadline)):
            alerts = [ev.item() for ev in self._pending.values()] + self._pending_conditions + self._oneshot_items()
            for key, ev in self._pending.items():
                self._activate(key, ev, now)
            self._pending.clear()
            self._pending_conditions = []
            self._oneshots.clear()
            self._batch_deadline = None
            self._flush_now = False

        reminds, recovers = [], []
        remind_span = fmt_duration(config.OPS_ALERT_REMIND_SECONDS)
        for key, ev in list(self._active.items()):
            if now < ev.next_check:
                continue
            if ev.window > 0:
                reminds.append(ev.item("過去 %s又發生 %d 次；最近一次：%s" % (remind_span, ev.window, ev.msg)))
                ev.reminded = True
                ev.window = 0
                ev.next_check = now + config.OPS_ALERT_REMIND_SECONDS
            else:
                del self._active[key]
                if ev.reminded:
                    recovers.append(ev.item("過去 %s沒有再發生（共 %d 次）；%s" % (remind_span, ev.total, ev.msg)))
        for cond in self._raised.values():
            if now >= cond.next_remind:
                reminds.append((cond.key, _condition_head(cond.key),
                                "仍未恢復，已持續 %s；%s" % (fmt_duration(now - cond.raised_at), cond.desc)))
                cond.next_remind = now + config.OPS_ALERT_REMIND_SECONDS
        recovers += self._cleared
        self._cleared = []

        for kind, items in ((KIND_ALERT, alerts), (KIND_REMIND, reminds), (KIND_RECOVERED, recovers)):
            if items:
                self._deliver(kind, now, [], items)

        if now >= self._next_heartbeat:
            self._heartbeat(now)
            self._next_heartbeat = next_heartbeat_after(now)

    def _activate(self, key, ev, now):
        ev.window = 0
        ev.next_check = now + config.OPS_ALERT_REMIND_SECONDS
        self._active[key] = ev

    def _drain(self):
        with self._qlock:
            records = list(self._queue)
            self._queue.clear()
        for rec in records:
            self._ingest(rec)

    def _ingest(self, rec):
        t, levelno, name, filename, lineno, _thread_name, msg = rec
        key = (name, filename, lineno)
        if self._shutting_down:
            ev = self._closing.get(key)
            if ev is None:
                ev = self._closing[key] = _Event(name, filename, lineno)
            ev.add(levelno, msg, False)
            return
        ev = self._active.get(key)
        if ev is not None:
            ev.add(levelno, msg, True)
            return
        ev = self._pending.get(key)
        if ev is not None:
            ev.add(levelno, msg, False)
            if levelno >= logging.CRITICAL:
                self._flush_now = True
            return
        ev = self._grace.get(key)
        if ev is not None:
            ev.add(levelno, msg, False)
            if levelno >= logging.CRITICAL:     # CRITICAL 不受寬限：整個鍵移到下一則 🔴
                del self._grace[key]
                self._queue_new(key, ev, t, flush=True)
            return
        ev = _Event(name, filename, lineno)
        ev.add(levelno, msg, False)
        if levelno < logging.CRITICAL and not self._grace_done and t < self._grace_end:
            self._grace[key] = ev
            return
        self._queue_new(key, ev, t, flush=levelno >= logging.CRITICAL)

    def _queue_new(self, key, ev, t, flush):
        self._pending[key] = ev
        if flush:
            self._flush_now = True
        if self._batch_deadline is None:
            self._batch_deadline = t + config.OPS_ALERT_BATCH_SECONDS

    # ---------------------------------------------------------------- FR-4 一次性事件
    def _add_oneshot(self, key, n, now):
        self._oneshots[key] = self._oneshots.get(key, 0) + n
        if self._batch_deadline is None:
            self._batch_deadline = now + config.OPS_ALERT_BATCH_SECONDS

    def _collect_uncovered(self, now):
        with self._lock:
            n, self._uncovered = self._uncovered, 0
        if n:
            self._add_oneshot(ONESHOT_UNCOVERED, n, now)

    def _oneshot_items(self):
        items = []
        for key, n in self._oneshots.items():
            if key == ONESHOT_UNCOVERED:
                items.append((key, "[事件] 對帳", "%d 個小時窗口超出 klines 涵蓋，沒有對帳（多半是停頓過久）" % n))
            else:
                items.append((key, "[事件] 報表", "%d 期超過補發上限，跳過未發" % n))
        return items

    # ---------------------------------------------------------------- 條件（輪詢）
    def _warn_once(self, tag, msg, *args):
        if tag not in self._warned:
            self._warned.add(tag)
            logger.warning(msg, *args)

    def _read(self, component, fn):
        """讀一個元件的狀態；拋例外 → 回 None（這一輪略過它），同一種錯誤只記一次 WARNING。"""
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            self._warn_once(("stats", component, type(e).__name__),
                            "維運告警讀 %s 的狀態失敗（%s: %s），這一輪略過它；同一種錯誤只記這一次",
                            component, type(e).__name__, e)
            return None

    def _poll(self, now):
        observed = []     # (條件鍵, True / False / None, 說明)
        dead = "執行緒已經結束，不會自動重啟（要重啟程式才會恢復）"

        def c1(code, alive):
            observed.append(("C1:" + code, None if alive is None else not alive, dead))

        tracker = self._tracker
        if tracker is not None:
            st = self._read("A3", lambda: (tracker.worker_alive,) + _fields(tracker.stats, "fetch_failures"))
            c1("a3", None if st is None else bool(st[0]))
            if st is not None:
                observed.append(self._counter(C6, st[1], "A3 取數失敗"))
        s5 = self._s5
        if s5 is not None:
            st = self._read("A5", lambda: _fields(s5.stats(), "worker_alive", "minutes"))
            c1("a5", None if st is None else bool(st[0]))
            if st is not None:
                minutes = st[1]
                if self._s5_minutes is None or minutes > self._s5_minutes:
                    self._s5_minutes, self._s5_changed_at = minutes, now
                idle = now - self._s5_changed_at
                observed.append((C3, idle > config.OPS_A5_STALL_SECONDS,
                                 "處理的分鐘數已經 %s沒有增加（上限 %s；目前累計 %d 分鐘）"
                                 % (fmt_duration(idle), fmt_duration(config.OPS_A5_STALL_SECONDS), minutes)))
        push = self._push
        if push is not None:
            c1("t2", self._read("T2 pusher", lambda: bool(push.worker_alive)))
            c1("sender", self._read("A 頻道發送器", lambda: bool(push.sender.stats()["worker_alive"])))
            observed.append(self._outbox_condition(now))
        reporter = self._reporter
        if reporter is not None:
            st = self._read("R-A", lambda: _fields(reporter.stats(), "worker_alive", "compose_errors", "skipped"))
            c1("ra", None if st is None else bool(st[0]))
            if st is not None:
                observed.append(self._counter(C5, st[1], "組報表失敗"))
                skipped = st[2]
                if skipped > self._ra_skipped_seen:
                    self._add_oneshot(ONESHOT_REPORT_SKIPPED, skipped - self._ra_skipped_seen, now)
                self._ra_skipped_seen = skipped
        rec = self._reconciler
        if rec is not None and getattr(rec, "_thread", None) is not None:   # 還沒啟動不算不在
            c1("reconcile", self._read("對帳", lambda: bool(rec.worker_alive)))
        with self._lock:
            last_bar = self._last_bar_at
        idle = now - (self._started_at if last_bar is None else last_bar)
        what = "啟動後已經 %s還沒收到第一根 K 棒結果" if last_bar is None else "已經 %s沒有收到新的 K 棒結果"
        observed.append((C2, idle > config.OPS_A1_STALL_SECONDS,
                         (what % fmt_duration(idle)) + "（上限 %s）" % fmt_duration(config.OPS_A1_STALL_SECONDS)))

        for key, state, desc in observed:
            cur = self._raised.get(key)
            if state is True and cur is None:
                self._raised[key] = _Raised(key, now, desc)
                self._pending_conditions.append((key, _condition_head(key), desc))
            elif state is True:
                cur.desc = desc
            elif state is False and cur is not None:
                del self._raised[key]
                self._cleared.append((key, _condition_head(key), "已恢復，持續了 %s" % fmt_duration(now - cur.raised_at)))

    def _counter(self, key, value, what):
        streak = self._streaks[key]
        streak.update(value)
        if key in self._raised:
            state = streak.flat < config.OPS_COUNTER_CLEAR_POLLS
        else:
            state = streak.up >= config.OPS_COUNTER_RAISE_POLLS
        if streak.up:
            desc = "%s連續 %d 輪輪詢都有增加（累計 %d 次）" % (what, streak.up, value)
        else:
            desc = ("%s累計 %d 次，已連續 %d 輪輪詢沒有增加（連續 %d 輪沒有增加才算恢復）"
                    % (what, value, streak.flat, config.OPS_COUNTER_CLEAR_POLLS))
        return key, state, desc

    def _outbox_condition(self, now):
        try:
            rows = self._outbox_rows(pending_only=True)
        except Exception as e:  # noqa: BLE001
            self._warn_once(("outbox", type(e).__name__),
                            "維運告警讀不到 outbox（%s: %s），這一輪無法判斷 C4；同一種錯誤只記這一次", type(e).__name__, e)
            return C4, None, ""
        if not rows:
            return C4, False, ""
        oldest_ms = min(r["received_ms"] for r in rows)
        age = now - oldest_ms / 1000.0
        return (C4, age > config.OPS_OUTBOX_STUCK_SECONDS,
                "最舊的待送列已經 %s沒送出（上限 %s；共 %d 則待送）"
                % (fmt_duration(age), fmt_duration(config.OPS_OUTBOX_STUCK_SECONDS), len(rows)))

    # ---------------------------------------------------------------- outbox（唯讀，用完就關）
    def _outbox_db_path(self):
        """建構時給的 > T2 pusher 的 outbox（attach 之後）> config.A_CHANNEL_OUTBOX_DB_PATH。"""
        return self._outbox_path or getattr(self._push, "path", None) or config.A_CHANNEL_OUTBOX_DB_PATH

    def _outbox_rows(self, pending_only):
        path = self._outbox_db_path()
        conn = open_readonly(path, "outbox（a_channel_outbox.sqlite3）", a_channel_outbox.SCHEMA_VERSION,
                             busy_timeout=config.A_CHANNEL_OUTBOX_BUSY_TIMEOUT_SECONDS)
        try:
            conn.row_factory = sqlite3.Row
            outbox = a_channel_outbox.Outbox(conn, os.path.abspath(path))
            return outbox.pending() if pending_only else outbox.rows()
        finally:
            conn.close()

    def _outbox_pending_text(self):
        path = self._outbox_db_path()
        if not os.path.isfile(path):
            return "—（outbox 還沒建立）"
        try:
            return "%d 則" % len(self._outbox_rows(pending_only=True))
        except Exception as e:  # noqa: BLE001
            return "讀不到（%s）" % type(e).__name__

    # ---------------------------------------------------------------- 心跳
    def _snapshot(self):
        """各元件的累計值（差值的來源）。沒 attach 的元件是 _UNSTARTED，讀不到的是 _UNREADABLE。"""
        with self._lock:
            snap = {"a1": {"bars": self._bars, "degraded": self._bars_degraded, "signals": self._a1_signals,
                           "reasons": collections.Counter(self._reasons)},
                    "alerts": dict(self._sent)}
        errors, warnings, dropped = self.counters()
        snap["log"] = {"errors": errors, "warnings": warnings, "dropped": dropped}

        def take(name, comp, fn):
            if comp is None:
                snap[name] = _UNSTARTED
                return
            v = self._read(name, lambda: fn(comp))
            snap[name] = _UNREADABLE if v is None else v
        take("a5", self._s5, lambda c: {k: c.stats()[k] for k in ("minutes", "degraded", "missed", "signals")})
        take("a3", self._tracker, lambda c: {k: c.stats[k] for k in ("entries", "exits", "store_failures")})
        take("ra", self._reporter, lambda c: {k: c.stats()[k] for k in ("delivered", "skipped")})
        take("gate", self._gate, lambda c: {k: c.stats()[k] for k in ("requests", "http_429")})
        return snap

    def _outbox_finalized(self, since_s, now_s):
        """統計區間 (since, now] 內結案的 outbox 列：進場送達、延遲不發、永久失敗、出場送達。"""
        if self._push is None:
            return _UNSTARTED
        try:
            rows = self._outbox_rows(pending_only=False)
        except Exception as e:  # noqa: BLE001
            self._warn_once(("outbox", type(e).__name__),
                            "維運告警讀不到 outbox（%s: %s）；同一種錯誤只記這一次", type(e).__name__, e)
            return _UNREADABLE
        lo, hi = since_s * 1000.0, now_s * 1000.0
        done = [r for r in rows if r["finalized_ms"] is not None and lo < r["finalized_ms"] <= hi]

        def count(kind, status):
            return sum(1 for r in done if (kind is None or r["kind"] == kind) and r["status"] == status)
        return {"entry_delivered": count(a_channel_outbox.KIND_ENTRY, a_channel_outbox.STATUS_DELIVERED),
                "expired": count(None, a_channel_outbox.STATUS_EXPIRED),
                "failed": count(None, a_channel_outbox.STATUS_FAILED),
                "exit_delivered": count(a_channel_outbox.KIND_EXIT, a_channel_outbox.STATUS_DELIVERED)}

    def _heartbeat(self, now):
        cur = self._snapshot()
        prev = self._hb_prev
        delta = {}
        for name, v in cur.items():
            base = prev.get(name)
            if isinstance(v, str):
                delta[name] = v
                cur[name] = base           # 這次讀不到：下一次仍從上一次讀到的值算差值
                continue
            base = base if isinstance(base, dict) else {}
            delta[name] = {k: (x - base.get(k, 0) if k != "reasons" else x - base.get(k, collections.Counter()))
                           for k, x in v.items()}
        delta["outbox"] = self._outbox_finalized(self._hb_since, now)
        delta["raised"] = [CONDITION_NAMES[k] for k in self._raised]
        fields = heartbeat_fields(now, self._started_at, self._hb_since, self._hb_first, delta)
        self._deliver(KIND_HEARTBEAT, now, fields, [])
        self._hb_prev = cur
        self._hb_since = now
        self._hb_first = False

    # ---------------------------------------------------------------- 結束訊息
    def _shutdown_message(self, now, reason, rc, thread_note):
        """⚪ 的 (fields, items)。項目：關閉過程中的 ERROR + 還沒發出的（寬限中、批次中、一次性事件），同一個鍵合併。"""
        self._collect_uncovered(now)
        closing_errors = sum(ev.total for ev in self._closing.values())
        merged = collections.OrderedDict(self._closing)
        unsent = 0
        for bucket in (self._grace if not self._grace_done else {}, self._pending):
            for key, ev in bucket.items():
                unsent += 1
                if key in merged:
                    merged[key].merge(ev)
                else:
                    merged[key] = ev
        oneshots = self._oneshot_items()
        unsent += len(oneshots)
        items = [ev.item() for ev in merged.values()] + oneshots
        dropped = self.counters()[2]
        raised = [CONDITION_NAMES[k] for k in self._raised]
        fields = shutdown_fields(now, self._started_at, reason, rc, self._outbox_pending_text(), raised, dropped,
                                 closing_errors, unsent, thread_note)
        return fields, items

    # ---------------------------------------------------------------- 交出
    def _deliver(self, kind, now, fields, items):
        """組字、交給維運發送器、記一行 INFO（已交出）或 WARNING（沒有交出）。"""
        text = compose(kind, now, self.label, fields, items, self._masker)
        keys = [item[0] for item in items]
        try:
            ok = self._sender.send(text, key="ops-" + kind)
        except Exception:  # noqa: BLE001
            logger.exception("維運告警交給發送器時拋出例外")
            ok = False
        args = (kind, KIND_NAMES[kind], len(keys), "、".join(keys) if keys else "—")
        if ok:
            with self._lock:
                self._sent[kind] += 1
            logger.info("已交出 %s（%s）：%d 項，鍵 %s", *args)
        else:
            logger.warning("維運告警沒有交出（發送器拒收，原因見上一行）：%s（%s）：%d 項，鍵 %s", *args)
        return ok


# ============================== --sample ==============================
def sample_messages(now_s=None, label=None):
    """七種訊息各一則，**寫死的假資料**，開頭加 [測試]：[(key, 文字)]。不碰 outbox、派網、日誌佇列。"""
    now_s = time.time() if now_s is None else now_s
    label = instance_label() if label is None else label
    started = now_s - 3 * 3600
    ev = _Event("live.a_channel_push", "a_channel_push.py", 512)
    ev.add(logging.ERROR, "[假資料] outbox 記不回結果：database is locked（signal_id=s4-ACE_USDT_PERP-202609181535）",
           False)
    ev2 = _Event("live.notional_tracker", "notional_tracker.py", 668)
    for _ in range(3):
        ev2.add(logging.ERROR, "[假資料] A3 資料庫寫入失敗：disk I/O error", False)
    c2 = (C2, _condition_head(C2), "已經 %s沒有收到新的 K 棒結果（上限 %s）"
          % (fmt_duration(12 * 60), fmt_duration(config.OPS_A1_STALL_SECONDS)))
    remind_span = fmt_duration(config.OPS_ALERT_REMIND_SECONDS)
    hb = {"a1": {"bars": 36, "degraded": 2, "signals": 3,
                 "reasons": collections.Counter({"fetch_failed:api_error": 2})},
          "a5": {"minutes": 180, "degraded": 1, "missed": 0, "signals": 1},
          "a3": {"entries": 3, "exits": 2, "store_failures": 0},
          "outbox": {"entry_delivered": 3, "expired": 0, "failed": 0, "exit_delivered": 2},
          "ra": {"delivered": 1, "skipped": 0},
          "gate": {"requests": 4321, "http_429": 0},
          "log": {"errors": 4, "warnings": 12, "dropped": 0},
          "alerts": {KIND_ALERT: 1, KIND_REMIND: 1, KIND_RECOVERED: 1},
          "raised": []}
    messages = [
        (KIND_STARTUP, startup_fields(10800, "0 則", "0 期"), []),
        (KIND_GRACE, [], [ev.item()]),
        (KIND_ALERT, [], [ev2.item(), c2]),
        (KIND_REMIND, [], [ev2.item("過去 %s又發生 2 次；最近一次：%s" % (remind_span, ev2.msg)),
                           (c2[0], c2[1], "仍未恢復，已持續 1 小時；" + c2[2])]),
        (KIND_RECOVERED, [], [ev2.item("過去 %s沒有再發生（共 3 次）；%s" % (remind_span, ev2.msg)),
                              (c2[0], c2[1], "已恢復，持續了 1 小時 12 分")]),
        (KIND_HEARTBEAT, heartbeat_fields(now_s, started, started, True, hb), []),
        (KIND_SHUTDOWN, shutdown_fields(now_s, started, "到達 --duration（10800 秒）", 0, "0 則", [], 0, 0, 0), []),
    ]
    out = []
    for i, (kind, fields, items) in enumerate(messages, 1):
        out.append(("ops-sample-%d" % i, SAMPLE_PREFIX + compose(kind, now_s, label, fields, items)))
    return out


def sample(stop_timeout=180, sender_factory=None):
    """把七則 [測試] 訊息經維運發送器送到維運聊天並等送完。回傳 exit code。缺密鑰時不碰網路、不建日誌檔。

    只需要 bot token 與維運 chat id；不碰 A 頻道、不打派網。sender_factory 給測試注入。
    """
    missing = [name for name in (config.TG_BOT_TOKEN_ENV, config.OPS_CHAT_ID_ENV) if not config.secret_is_set(name)]
    if missing:
        print("未實測（缺密鑰）：%s 未設定，沒有送出任何訊息" % "、".join(missing))
        return EXIT_NOT_TESTED
    messages = sample_messages()
    if sender_factory is None:
        from live import logsetup
        logsetup.setup()
    results = []
    lock = threading.Lock()

    def done(result):
        with lock:
            results.append(result)

    sender = (sender_factory or new_sender)()
    sender.start()
    for key, text in messages:
        sender.send(text, key=key, on_done=done)
    remaining = sender.stop(timeout=stop_timeout)
    with lock:
        delivered = sum(1 for r in results if r.status == RESULT_DELIVERED)
        by_status = collections.Counter(r.status for r in results)
    print("維運告警樣本：共 %d 則，已送達 %d；結果分布 %s；stop() 未送出 %d"
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
        prog="python -m live.ops_alert",
        description="A4 維運告警的樣本訊息。必須明確帶 --sample 才會送出訊息。")
    parser.add_argument("--sample", action="store_true", required=True,
                        help="用寫死的假資料組出七種訊息各一則（開頭加 [測試]），送到維運聊天"
                             "（需要 %s 與 %s；不碰 A 頻道、outbox、派網）" % (config.TG_BOT_TOKEN_ENV,
                                                                          config.OPS_CHAT_ID_ENV))
    parser.parse_args(argv)
    return sample()


if __name__ == "__main__":
    # python -m live.ops_alert 會把本檔載入成 __main__；從正式的模組名取 main()（同 live.a_channel 的理由）
    from live.ops_alert import main as _main
    sys.exit(_main())
