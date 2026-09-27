# -*- coding: utf-8 -*-
"""
live.tg_channel — Telegram A 頻道廣播：發送佇列、限速與重試
==========================================================
A 頻道的最後一哩：把「已經組好的」文字訊息可靠地送到 Telegram Channel。
只負責「送」，不組訊息（格式是 T2 的事），沒有 DM、沒有指令、沒有 getUpdates / webhook。

用法：

    from live.tg_channel import ChannelSender
    sender = ChannelSender()
    sender.start()                       # 缺密鑰就在這裡拋 MissingSecretError
    sender.send(text, key="s4:BTCUSDT:...")   # 排進佇列後立刻返回
    ...
    sender.stop(timeout=30)              # 盡量送完；回傳沒送出去的則數

只用標準庫與 requests，不引入 Telegram SDK。模組名刻意不叫 telegram：那是
python-telegram-bot 的套件名，跟常見第三方套件撞名的下場跟撞標準庫一樣（見 live.logsetup）。

──────────────────────────────────────────────────────────────────────
最重要的一件事：token 在網址裡
──────────────────────────────────────────────────────────────────────
Bot API 的網址是 https://api.telegram.org/bot<TOKEN>/sendMessage，token 就在路徑裡。
requests / urllib3 的連線錯誤、逾時例外、以及 urllib3 自己的日誌都會帶著完整網址：

  ・requests.ConnectionError 的字串是
    "HTTPSConnectionPool(...): Max retries exceeded with url: /bot<TOKEN>/sendMessage (...)"
  ・urllib3 會在 **WARNING** 等級把網址寫進日誌（urllib3/connection.py 標頭解析失敗、
    urllib3/connectionpool.py 連線中斷重試），DEBUG 等級更會印出每一個請求行。
    預設 LOG_LEVEL=INFO 也擋不住 WARNING。

只要任何一處直接 logger.error("%s", e)、logger.exception(...)、或 raise X from e 往外拋，
token 就進了日誌檔，而日誌會輪替、保留好幾份。所以這裡的規則是：

  1. 集中遮罩（SecretMasker）：token 與 channel id 的全文、URL 編碼後的形式、以及它們
     **任何連續 8 個字元以上的片段**，一律換成 MASK。用「片段」而不是「全文」比對，是因為
     截斷過的字串（例如某處只印前 200 字）會留下半截 token，全文比對抓不到。
     8 對齊驗收標準 AC-6；token 的祕密部分是 35 個隨機字元，8 字元片段跟正常文字意外撞上的
     機率可忽略，就算撞上也只是多遮掉一段無害文字。
  2. 本模組自己的每一則日誌都經過 _log()：先把訊息完整格式化，再整段遮罩，最後才交給
     logging。例外一律先轉成「類別名: 遮罩後的訊息」字串（_describe_exception），
     traceback 用 traceback.format_exception 組成字串後整段遮罩再記（_log_unexpected）。
     **本模組不使用 logger.exception / exc_info**：那會讓 Formatter 在遮罩之外另外渲染原始例外。
  3. 原始例外物件不離開 except 區塊：分類結果只留下遮罩後的字串（_Outcome.detail），
     不保存例外物件，也不把它往外拋 —— 所以不存在 __cause__ / __context__ 帶著網址往外跑的路徑。
     本模組往外拋的例外只有 MissingSecretError（訊息只含環境變數名）、TypeError、RuntimeError，
     全部都不是在 except 區塊裡拋出，__context__ 是空的。
  4. 第三方 logger：start() 時在 urllib3 會寫網址的那幾個 logger 掛上遮罩 filter
     （_MaskingFilter，連 exc_info 的 traceback 一起遮），stop() 時拆掉。
     logger 上的 filter 只作用在「直接由該 logger 產生的紀錄」，所以是逐一列名
     （_THIRD_PARTY_LOGGERS，依 urllib3 2.x 實際會寫網址的模組列出）。
  5. stats()、repr(sender) 本來就不含密鑰，仍然過一次遮罩。

──────────────────────────────────────────────────────────────────────
佇列與執行緒
──────────────────────────────────────────────────────────────────────
send() 只做檢查與 append 就返回（A2 的訂閱者必須很快），一條背景工作執行緒依序送出。
嚴格 FIFO：一則沒處理完（成功、放棄、或 stop 期限到）之前，下一則不會送出 ——
同一筆訊號的出場訊息不可以超車進場訊息。代價是一則卡在重試時後面的都要等，所以每一種
重試都有上限（見下），不會無限期卡住整條佇列。

工作執行緒自己把每一輪包在 try/except 裡：未預期的例外記下遮罩後的 traceback、那一則記為
失敗，然後繼續下一則。B3 的 threading.excepthook 只負責記錄，執行緒照樣會死 —— 死掉的
發送執行緒是「之後所有訊息都安靜消失」，所以不能靠它。

生命週期：NEW → start() → RUNNING → stop() → STOPPING → STOPPED，只能走一次。
  ・start() 兩次、或 stop() 之後再 start()：拋 RuntimeError（要重來就建新的 ChannelSender）
  ・start() 因缺密鑰失敗：狀態維持 NEW，補好環境變數後可以再 start()
  ・start() 之前 send()：拋 RuntimeError —— 那是接線錯誤，應該在開發時就炸出來
  ・stop() 之後（含 stop 進行中）send()：不拋例外，記 ERROR、計入 rejected、回傳 False。
    關機時訂閱者可能還在觸發，拋例外只會把關機流程弄得更亂
  ・stop() 對從未 start 的發送器是 no-op（回傳 0）；stop() 兩次回傳同一個數字

──────────────────────────────────────────────────────────────────────
送出內容
──────────────────────────────────────────────────────────────────────
payload 是 chat_id、text、link_preview_options={"is_disabled": true}。
  ・不帶 parse_mode：訊息是純文字加 emoji，Markdown / HTML 的特殊字元（* _ [ < &）一旦被當成
    標記解析，整則會被 400 拒收。純文字模式下它們原樣顯示。
  ・關閉連結預覽：訊號訊息若含網址，預覽卡片會把訊息本身擠到很小。這是 Bot API 7.0 起的
    欄位（取代已棄用的 disable_web_page_preview）。

長度上限 4096（TG_MAX_MESSAGE_CHARS），以 **UTF-16 code units** 計：Bot API 文件寫的是
「1-4096 characters」，而 Telegram 所有跟文字位置有關的欄位（MessageEntity 的 offset / length）
都明文以 UTF-16 code units 計。這台開發機沒有外網文件可查證伺服器端的實際計法，所以採用兩種
解讀中較嚴格的一個：emoji 這類 BMP 以外的字元算 2，其餘算 1 —— 以 code units 計不超過 4096
的訊息，以 code points 計也一定不超過。超過就拒收並記 ERROR，**不自動切段**：切開的訊號訊息
會誤導人。去掉頭尾空白後是空字串的訊息也拒收（Telegram 會以 400 拒絕）。

──────────────────────────────────────────────────────────────────────
限速與重試（參數全部在 live/config.py 的 TG_*）
──────────────────────────────────────────────────────────────────────
限速是以「實際發出 HTTP 請求」的時間點計的滑動視窗：任何 60 秒內最多
TG_MAX_MESSAGES_PER_MINUTE 次請求，而且相鄰兩次至少隔 TG_MIN_INTERVAL_SECONDS。
重試與 429 之後的重送也是請求，一樣計入。新請求在時刻 t 送出的條件是 (t-60, t] 內的既有
請求數 < 上限，所以任何長度 60 秒的半開視窗 [s, s+60) 內都不會超過上限。

每一次請求的結果分四類：

  成功        HTTP 200 且 body 的 ok 是 true。
  429         讀 body 的 parameters.retry_after，**等滿那麼久**才重送同一則，不提早。
              重送時若限速視窗已滿，會再等到視窗空出來（只會更晚，不會更早）。
              429 **不計入** TG_SEND_MAX_ATTEMPTS：伺服器明確說了何時可以再來，那不是「這一則
              有問題」；計入的話，一段 flood control 就會把本來送得出去的訊息判成失敗。
              改由 TG_MAX_RATE_LIMITED_RETRIES 單獨設上限，超過就放棄這一則、換下一則。
              retry_after 缺少、不是數字、或不是正數時：先看 Retry-After 標頭，再不行就等
              TG_RETRY_AFTER_FALLBACK_SECONDS；兩種都記 WARNING（代表 Telegram 的回應格式跟
              預期不同，值得有人看一眼）。
  可重試      5xx、連線錯誤、逾時、傳輸中斷（ChunkedEncodingError）。退避後重試，連同第一次
              總共嘗試 TG_SEND_MAX_ATTEMPTS 次；只有「後面還要再試」才等（跟 live.pionex_api
              同一個原則），用盡就記 ERROR（含 key）、計入 failed、換下一則。
  不可重試    400 / 401 / 403 / 404 等其他 4xx、SSL 憑證錯誤、HTTP 200 但 ok 不是 true、
              其他 requests 例外。記 ERROR（含 key 與遮罩後的描述）、計入 failed、換下一則。
              HTTP 200 但 ok 不是 true 不重試：訊息可能其實已經發出去了，重試會變成重複發送。

注意：逾時與連線中斷時，請求可能其實已經送達 Telegram，重試就會讓頻道收到兩則一樣的訊息。
這是「寧可重複、不可漏發」的取捨（at-least-once），PRD 要求逾時要重試。

──────────────────────────────────────────────────────────────────────
stop(timeout)
──────────────────────────────────────────────────────────────────────
從呼叫當下（以注入的時鐘計）起算 timeout 秒為期限，工作執行緒繼續照 FIFO、照限速送。
任何一次等待（限速、退避、429）若會越過期限，就不等了：當下這一則連同佇列裡剩下的全部放棄，
記一筆 ERROR 寫明放棄幾則，計入 stats 的 abandoned，stop() 回傳這個數字。
正在等待中的工作執行緒會被 stop() 叫醒重新計算（不會傻等一個 600 秒的 retry_after 等完）。
正在進行中的單一 HTTP 請求無法中斷，最多再等 TG_HTTP_TIMEOUT_SECONDS；stop() 以真實時間
join 工作執行緒，上限是 timeout + TG_HTTP_TIMEOUT_SECONDS + 5 秒，真的等不到就記 ERROR 返回。

──────────────────────────────────────────────────────────────────────
可注入的東西（測試全程離線、不真的 sleep）
──────────────────────────────────────────────────────────────────────
  post        (url, json=..., timeout=...) -> 有 status_code / json() / headers 的回應物件。
              預設是 start() 時建立的 requests.Session().post（連線可重用）。
  clock       回傳單調遞增秒數，預設 time.monotonic。限速、退避、429、stop 期限都用它。
  wall_clock  回傳 epoch 秒數，預設 time.time。只用來填 stats 的 last_success_at。
  wait        (seconds) -> None，工作執行緒所有等待都走它。預設是一個會被 stop() 叫醒的
              Event.wait；可能提早返回，呼叫端一律以 clock 重新計算還要等多久。
其餘參數省略時取 live.config 在「建構當下」的值。

──────────────────────────────────────────────────────────────────────
stats()
──────────────────────────────────────────────────────────────────────
  state            new / starting / running / stopping / stopped
  sent             成功送出的則數（HTTP 200 且 ok=true）
  failed           放棄的則數（重試用盡、不可重試、429 超過上限、未預期例外）
  rejected         send() 當下就拒收的則數（超長、空白、已停止、執行緒已不在）
  abandoned        stop 期限到時還沒送出而放棄的則數
  queued           佇列中等待的則數（不含正在處理的那一則）
  in_flight        是否有一則正在處理（含重試等待中）
  last_success_at  最後一次成功的時刻，UTC ISO 8601 字串（例 2026-09-25T04:05:06+00:00），沒有就 None
  last_message_id  最後一次成功時 Telegram 回傳的 message_id
  last_error       最後一次失敗的描述（已遮罩），沒有就 None
  last_error_at    同上的時刻，UTC ISO 8601
  worker_alive     工作執行緒是否還活著
全部都不含密鑰；A4 營運告警可以直接拿去用。

──────────────────────────────────────────────────────────────────────
實機冒煙（AC-8）
──────────────────────────────────────────────────────────────────────
    python -m live.tg_channel --smoke
有設兩個密鑰就送出一則「[測試] T1′ 冒煙 <台北時間>」並等它送完：成功 exit 0、失敗 exit 1；
沒設就印「未實測（缺密鑰）」並 exit 3，不碰網路也不建日誌檔。不帶 --smoke 什麼都不送。
"""

import argparse
import collections
import logging
import math
import re
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from urllib.parse import quote

import requests

from live import config

logger = logging.getLogger(__name__)

# 遮罩後的替代字串（純 ASCII：cp1252 主控台也印得出來）
MASK = "[REDACTED]"

# 片段遮罩的最小長度，對齊 AC-6「任何連續 8 個字元」
MASK_FRAGMENT_CHARS = 8

# 比這短的密鑰只做全文替換（再短就不遮了：幾個字元的「密鑰」遮起來只會把正常文字弄花）
_MIN_EXACT_MASK_CHARS = 5

# urllib3 2.x 會把請求網址寫進日誌的模組（見 docstring 規則 4）
_THIRD_PARTY_LOGGERS = (
    "urllib3.connectionpool",
    "urllib3.connection",
    "urllib3.poolmanager",
    "urllib3.util.retry",
    "urllib3.response",
)

_RATE_WINDOW_SECONDS = 60.0

# stop() 以真實時間 join 工作執行緒時，在 timeout + HTTP 逾時之外再多給的寬限
_JOIN_GRACE_SECONDS = 5.0

# 主迴圈接到未預期例外之後，喘口氣再繼續（避免同一個錯誤原地狂轉、灌爆日誌）
_CRASH_PAUSE_SECONDS = 1.0

# requests 例外中可重試的那幾類。SSLError 是 ConnectionError 的子類，必須先接住另外處理。
_RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)

# 4xx 的常見原因，寫進 ERROR 讓值班的人不用翻文件
_STATUS_HINTS = {
    400: "請求被拒（常見：chat 不存在、channel id 錯誤、訊息內容不合法）",
    401: "token 無效或已被撤銷",
    403: "bot 沒有在這個 channel 發文的權限（需要把 bot 加為 channel 管理員）",
    404: "端點不存在（常見：token 格式錯誤）",
}

# 生命週期狀態
_NEW, _STARTING, _RUNNING, _STOPPING, _STOPPED = "new", "starting", "running", "stopping", "stopped"

# 每一次請求的分類
_OK, _RATE_LIMITED, _RETRYABLE, _FATAL = "ok", "rate_limited", "retryable", "fatal"

# 一則訊息處理完的結局
_SENT, _FAILED, _ABANDONED = "sent", "failed", "abandoned"


# ============================== 遮罩 ==============================
def _secret_variants(secret):
    """密鑰可能出現在輸出裡的幾種寫法：原樣、URL 編碼（大寫與小寫 hex 兩種）。"""
    encoded = quote(secret, safe="")
    lower_hex = re.sub(r"%[0-9A-F]{2}", lambda m: m.group(0).lower(), encoded)
    return {secret, encoded, lower_hex}


class SecretMasker:
    """把密鑰（及其 URL 編碼形式）的全文與任何 >= MASK_FRAGMENT_CHARS 字元的連續片段換成 MASK。

    做法：先蒐集每個密鑰每種寫法的所有 8 字元片段；遮罩時對輸入字串的每個位置查一次集合，
    命中就把那 8 個字元標記起來，最後把每一段連續被標記的區間換成一個 MASK。
    相鄰的命中會連成一段，所以整條 token 只會變成一個 MASK。
    """

    def __init__(self, secrets):
        self._fragments = set()
        exact = set()
        for secret in secrets:
            if not secret:
                continue
            for variant in _secret_variants(secret):
                if len(variant) >= MASK_FRAGMENT_CHARS:
                    n = MASK_FRAGMENT_CHARS
                    self._fragments.update(variant[i:i + n] for i in range(len(variant) - n + 1))
                elif len(variant) >= _MIN_EXACT_MASK_CHARS:
                    exact.add(variant)
        self._exact = sorted(exact, key=len, reverse=True)

    def __call__(self, text):
        if not isinstance(text, str):
            text = str(text)
        for secret in self._exact:
            if secret in text:
                text = text.replace(secret, MASK)
        n = MASK_FRAGMENT_CHARS
        if not self._fragments or len(text) < n:
            return text
        covered = None
        for i in range(len(text) - n + 1):
            if text[i:i + n] in self._fragments:
                if covered is None:
                    covered = [False] * len(text)
                for j in range(i, i + n):
                    covered[j] = True
        if covered is None:
            return text
        parts = []
        i = 0
        while i < len(text):
            j = i
            while j < len(text) and covered[j] == covered[i]:
                j += 1
            parts.append(MASK if covered[i] else text[i:j])
            i = j
        return "".join(parts)

    def __repr__(self):
        # 絕不把片段集合印出來
        return "<SecretMasker>"


class _MaskingFilter(logging.Filter):
    """掛在第三方 logger 上：把紀錄的訊息與 traceback 整段遮罩後才放行。"""

    def __init__(self, masker):
        super().__init__()
        self._masker = masker

    def filter(self, record):
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 — 格式化失敗就交給 logging 自己報錯
            return True
        extra = ""
        if record.exc_info and record.exc_info[1] is not None:
            extra = "\n" + "".join(traceback.format_exception(*record.exc_info)).rstrip("\n")
        elif record.exc_text:
            extra = "\n" + record.exc_text
        if record.stack_info:
            extra += "\n" + record.stack_info
        record.msg = self._masker(message + extra)
        record.args = None
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        return True


# ============================== 小工具 ==============================
def utf16_units(text):
    """Telegram 計長度的單位：UTF-16 code units（BMP 以外的字元算 2）。"""
    return len(text.encode("utf-16-le", "surrogatepass")) // 2


def _positive_seconds(value):
    """把 retry_after 之類的值轉成正的有限秒數；不合格回 None。bool 不算數字。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        try:
            value = float(value.strip())
        except ValueError:
            return None
    if not isinstance(value, (int, float)):
        return None
    try:
        value = float(value)
    except OverflowError:  # JSON 裡的超大整數
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return value


def _safe_repr(value, limit=200):
    try:
        text = repr(value)
    except Exception:  # noqa: BLE001
        text = "<repr 失敗的 %s>" % type(value).__name__
    return text if len(text) <= limit else text[:limit] + "..."


def _iso_utc(epoch_seconds):
    return datetime.fromtimestamp(epoch_seconds, timezone.utc).isoformat(timespec="seconds")


class _Item:
    __slots__ = ("text", "key")

    def __init__(self, text, key):
        self.text = text
        self.key = key


class _Outcome:
    """一次請求的分類結果。只留遮罩後的字串，不保存原始例外物件。"""

    __slots__ = ("kind", "detail", "retry_after", "note", "message_id")

    def __init__(self, kind, detail="", retry_after=None, note=None, message_id=None):
        self.kind = kind
        self.detail = detail
        self.retry_after = retry_after
        self.note = note
        self.message_id = message_id


# ============================== 發送器 ==============================
class ChannelSender:
    """A 頻道的發送器：FIFO 佇列 + 一條背景工作執行緒 + 限速 + 重試。設計見模組 docstring。"""

    def __init__(self, *, post=None, clock=None, wall_clock=None, wait=None,
                 api_base_url=None, max_per_minute=None, min_interval=None,
                 max_attempts=None, backoff_base=None, backoff_max=None,
                 max_rate_limited_retries=None, retry_after_fallback=None,
                 http_timeout=None, max_chars=None, stop_timeout=None):
        def pick(value, default):
            return default if value is None else value

        self._api_base_url = pick(api_base_url, config.TG_API_BASE_URL).rstrip("/")
        self._max_per_minute = int(pick(max_per_minute, config.TG_MAX_MESSAGES_PER_MINUTE))
        self._min_interval = float(pick(min_interval, config.TG_MIN_INTERVAL_SECONDS))
        self._max_attempts = int(pick(max_attempts, config.TG_SEND_MAX_ATTEMPTS))
        self._backoff_base = float(pick(backoff_base, config.TG_RETRY_BACKOFF_BASE_SECONDS))
        self._backoff_max = float(pick(backoff_max, config.TG_RETRY_BACKOFF_MAX_SECONDS))
        self._max_rate_limited_retries = int(pick(max_rate_limited_retries,
                                                  config.TG_MAX_RATE_LIMITED_RETRIES))
        self._retry_after_fallback = float(pick(retry_after_fallback,
                                                config.TG_RETRY_AFTER_FALLBACK_SECONDS))
        self._http_timeout = pick(http_timeout, config.TG_HTTP_TIMEOUT_SECONDS)
        self._max_chars = int(pick(max_chars, config.TG_MAX_MESSAGE_CHARS))
        self._stop_timeout = float(pick(stop_timeout, config.TG_STOP_TIMEOUT_SECONDS))
        if self._max_per_minute < 1 or self._max_attempts < 1 or self._max_chars < 1:
            raise ValueError("max_per_minute / max_attempts / max_chars 都必須 >= 1")
        if (self._min_interval < 0 or self._backoff_base < 0 or self._backoff_max < 0
                or self._max_rate_limited_retries < 0 or self._retry_after_fallback <= 0):
            raise ValueError("間隔、退避與重試上限不可為負，retry_after 保底秒數必須 > 0")

        self._injected_post = post
        self._clock = clock or time.monotonic
        self._wall_clock = wall_clock or time.time
        self._wakeup = threading.Event()
        self._wait = wait or self._default_wait

        self._cond = threading.Condition()
        self._queue = collections.deque()
        self._in_flight = None
        self._state = _NEW
        self._deadline = None          # stop() 設定的期限（clock 秒數）；None = 沒有在停
        self._abort_requested = False  # stop() 等不到工作執行緒時設起來，要它一回來就收工
        self._thread = None

        # start() 之後才有
        self._masker = SecretMasker(())
        self._url = None
        self._chat_id = None
        self._post = None
        self._session = None
        self._log_filter = None

        # 只有工作執行緒碰：限速用的請求時間戳
        self._request_times = collections.deque()
        self._last_request_at = None

        # 統計（在 _cond 底下讀寫）
        self._sent = 0
        self._failed = 0
        self._rejected = 0
        self._abandoned = 0
        self._last_success_at = None
        self._last_message_id = None
        self._last_error = None
        self._last_error_at = None

    # ---------------------------------------------------------------- 公開介面
    def start(self):
        """取密鑰、啟動工作執行緒。缺密鑰就在這裡拋 MissingSecretError（不啟動執行緒）。"""
        with self._cond:
            if self._state != _NEW:
                raise RuntimeError("ChannelSender 只能 start() 一次（目前狀態：%s）；"
                                   "要重來請建立新的 ChannelSender" % self._state)
            self._state = _STARTING
        log_filter = None
        try:
            # 缺哪一個就在這裡拋；訊息只含環境變數名，不含值（live.config.require_secret）
            token = config.require_secret(config.TG_BOT_TOKEN_ENV)
            chat_id = config.require_secret(config.TG_CHANNEL_ID_ENV)
            masker = SecretMasker((token, chat_id))
            session = None
            post = self._injected_post
            if post is None:
                session = requests.Session()
                post = session.post
            log_filter = _MaskingFilter(masker)
            for name in _THIRD_PARTY_LOGGERS:
                logging.getLogger(name).addFilter(log_filter)
            thread = threading.Thread(target=self._run, name="tg-channel-sender", daemon=True)
        except BaseException:
            # 失敗就退回 NEW（補好環境變數後可以再 start），已經掛上的 filter 拆掉
            if log_filter is not None:
                for name in _THIRD_PARTY_LOGGERS:
                    logging.getLogger(name).removeFilter(log_filter)
            with self._cond:
                self._state = _NEW
            raise
        with self._cond:
            self._masker = masker
            self._url = "%s/bot%s/sendMessage" % (self._api_base_url, token)
            self._chat_id = chat_id
            self._post = post
            self._session = session
            self._log_filter = log_filter
            self._thread = thread
            self._state = _RUNNING
        thread.start()
        self._log(logging.INFO,
                  "A 頻道發送器已啟動：每 60 秒最多 %d 次請求、相鄰至少隔 %s 秒、"
                  "5xx/連線錯誤總嘗試 %d 次、429 最多重送 %d 次",
                  self._max_per_minute, self._min_interval, self._max_attempts,
                  self._max_rate_limited_retries)

    def send(self, text, key=None):
        """把一則純文字訊息排進 FIFO 佇列後立刻返回，不等網路。

        key 是呼叫端的關聯鍵（例如 signal_id；不假設它只是 symbol），只用在日誌裡方便追查。
        回傳 True = 已排入；False = 拒收（原因記在 ERROR，計入 stats 的 rejected）。
        text 不是 str 拋 TypeError；還沒 start() 就呼叫拋 RuntimeError。
        """
        if not isinstance(text, str):
            raise TypeError("text 必須是 str，收到 %s" % type(text).__name__)
        units = utf16_units(text)
        with self._cond:
            if self._state in (_NEW, _STARTING):
                raise RuntimeError("還沒 start() 就呼叫 send()：請在接上訂閱者之前先 start()")
            if self._state != _RUNNING:
                reason = "發送器已停止（或正在停止），不再接受新訊息"
            elif not text.strip():
                reason = "訊息是空的（或只有空白），Telegram 會拒收"
            elif units > self._max_chars:
                reason = ("長度 %d 超過單則上限 %d（以 UTF-16 code units 計），不自動切段"
                          % (units, self._max_chars))
            elif self._thread is None or not self._thread.is_alive():
                reason = "發送執行緒已經不在了，訊息不會被送出"
            else:
                self._queue.append(_Item(text, key))
                self._cond.notify_all()
                return True
            self._rejected += 1
        self._log(logging.ERROR, "拒收一則 A 頻道訊息（key=%s）：%s", _safe_repr(key), reason)
        return False

    def stop(self, timeout=None):
        """停止接收新訊息，盡量在 timeout 秒內把佇列送完。回傳沒送出去的則數。

        timeout 省略時用 TG_STOP_TIMEOUT_SECONDS。逾時放棄時記 ERROR 寫明放棄幾則。
        """
        timeout = self._stop_timeout if timeout is None else max(0.0, float(timeout))
        with self._cond:
            if self._state in (_NEW, _STARTING):
                return 0
            if self._state == _STOPPED:
                return self._abandoned
            if self._state == _RUNNING:
                self._state = _STOPPING
                self._deadline = self._clock() + timeout
                self._cond.notify_all()
            thread = self._thread
        self._wakeup.set()  # 叫醒正在等待（限速 / 退避 / 429）的工作執行緒，讓它以期限重新計算

        thread.join(timeout + float(self._http_timeout) + _JOIN_GRACE_SECONDS)
        if thread.is_alive():
            with self._cond:
                self._abort_requested = True
                dropped = len(self._queue)
                self._abandoned += dropped
                self._queue.clear()
                stuck = dropped + (1 if self._in_flight is not None else 0)
                self._state = _STOPPED
            self._wakeup.set()
            self._log(logging.ERROR,
                      "stop()：發送執行緒在期限內沒有結束（可能卡在一次 HTTP 請求），"
                      "放棄 %d 則未送出的 A 頻道訊息", stuck)
            return stuck

        self._teardown()
        with self._cond:
            self._state = _STOPPED
            remaining = self._abandoned
            sent, failed = self._sent, self._failed
        if not remaining:
            self._log(logging.INFO, "A 頻道發送器已停止，佇列已處理完（已送出 %d、失敗 %d）",
                      sent, failed)
        return remaining

    def stats(self):
        """目前的統計。不含任何密鑰；欄位定義見模組 docstring。"""
        with self._cond:
            result = {
                "state": self._state,
                "sent": self._sent,
                "failed": self._failed,
                "rejected": self._rejected,
                "abandoned": self._abandoned,
                "queued": len(self._queue),
                "in_flight": self._in_flight is not None,
                "last_success_at": self._last_success_at,
                "last_message_id": self._last_message_id,
                "last_error": self._last_error,
                "last_error_at": self._last_error_at,
                "worker_alive": bool(self._thread is not None and self._thread.is_alive()),
            }
        if result["last_error"] is not None:
            result["last_error"] = self._mask(result["last_error"])
        return result

    def __repr__(self):
        s = self.stats()
        return self._mask("<ChannelSender state=%s sent=%d failed=%d rejected=%d abandoned=%d "
                          "queued=%d in_flight=%s>" % (s["state"], s["sent"], s["failed"],
                                                       s["rejected"], s["abandoned"],
                                                       s["queued"], s["in_flight"]))

    __str__ = __repr__

    # ---------------------------------------------------------------- 遮罩與日誌
    def _mask(self, text):
        return self._masker(text)

    def _log(self, level, msg, *args):
        """本模組唯一的寫日誌出口：先完整格式化、再整段遮罩，最後才交給 logging。"""
        if args:
            msg = msg % args
        logger.log(level, "%s", self._mask(msg))

    def _describe_exception(self, exc):
        """例外 → 「類別名: 遮罩後的訊息」。原始例外物件不離開呼叫端的 except 區塊。"""
        try:
            text = str(exc)
        except Exception:  # noqa: BLE001
            text = "<str() 失敗>"
        return self._mask("%s: %s" % (type(exc).__name__, text) if text else type(exc).__name__)

    def _log_unexpected(self, where, item, exc):
        """未預期的例外：整段 traceback（含 __cause__ / __context__ 鏈）組成字串、遮罩後記 ERROR。"""
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)).rstrip("\n")
        key = _safe_repr(item.key) if item is not None else "-"
        self._log(logging.ERROR, "A 頻道發送執行緒在%s發生未預期的例外（key=%s），"
                                 "這一則記為失敗，執行緒繼續運作。\n%s", where, key, tb)
        self._set_last_error(self._describe_exception(exc))

    def _set_last_error(self, detail):
        with self._cond:
            self._last_error = self._mask(detail)
            self._last_error_at = self._now_iso()

    def _now_iso(self):
        try:
            return _iso_utc(self._wall_clock())
        except Exception:  # noqa: BLE001 — 統計用的時間戳壞掉不值得讓送訊息失敗
            return None

    # ---------------------------------------------------------------- 工作執行緒
    def _default_wait(self, seconds):
        """預設的等待：可被 stop() 叫醒（叫醒後由呼叫端以 clock 重新計算還要等多久）。"""
        if self._wakeup.wait(seconds):
            self._wakeup.clear()

    def _run(self):
        while True:
            try:
                if not self._process_next():
                    return
            except Exception as exc:  # noqa: BLE001 — 最後一道防線：記錄後繼續，不讓執行緒死掉
                try:
                    self._log_unexpected("主迴圈", None, exc)
                    self._sleep_until(self._clock() + _CRASH_PAUSE_SECONDS)
                except Exception:  # noqa: BLE001 — 連記錄都失敗：至少用真實時間喘口氣再繼續
                    self._wakeup.wait(_CRASH_PAUSE_SECONDS)

    def _process_next(self):
        """等下一則並把它處理完。回傳 False 代表工作執行緒該結束了。"""
        with self._cond:
            while not self._queue and self._state == _RUNNING and not self._abort_requested:
                self._cond.wait()
            if self._abort_requested or not self._queue:
                return False
            item = self._queue.popleft()
            self._in_flight = item
        result = _FAILED
        try:
            result = self._deliver(item)
        except Exception as exc:  # noqa: BLE001
            self._log_unexpected("送出這一則時", item, exc)
            result = _FAILED
        finally:
            with self._cond:
                self._in_flight = None
                if result == _SENT:
                    self._sent += 1
                elif result == _FAILED:
                    self._failed += 1
                else:
                    dropped = 1 + len(self._queue)
                    self._abandoned += dropped
                    self._queue.clear()
                self._cond.notify_all()
        if result == _ABANDONED:
            self._log(logging.ERROR, "stop() 的期限已到，放棄 %d 則未送出的 A 頻道訊息"
                                     "（含處理中的 key=%s）", dropped, _safe_repr(item.key))
            return False
        return True

    def _deliver(self, item):
        """把一則送到成功、放棄、或 stop 期限到為止。回傳 _SENT / _FAILED / _ABANDONED。"""
        failures = 0        # 可重試錯誤的次數，計入 TG_SEND_MAX_ATTEMPTS
        rate_limited = 0    # 429 的次數，另計（TG_MAX_RATE_LIMITED_RETRIES）
        key = _safe_repr(item.key)
        while True:
            if not self._wait_for_send_slot():
                return _ABANDONED
            outcome = self._attempt(item)

            if outcome.kind == _OK:
                with self._cond:
                    self._last_success_at = self._now_iso()
                    self._last_message_id = outcome.message_id
                self._log(logging.INFO, "A 頻道訊息已送出（key=%s，message_id=%s）",
                          key, outcome.message_id)
                return _SENT

            if outcome.kind == _RATE_LIMITED:
                rate_limited += 1
                if rate_limited > self._max_rate_limited_retries:
                    return self._give_up(item, "連續收到 %d 次 HTTP 429（上限重送 %d 次），放棄這一則：%s"
                                         % (rate_limited, self._max_rate_limited_retries,
                                            outcome.detail))
                if outcome.note:
                    self._log(logging.WARNING, "HTTP 429 的回應格式跟預期不同（key=%s）：%s",
                              key, outcome.note)
                self._log(logging.WARNING, "HTTP 429（key=%s），等 %s 秒後重送同一則：%s",
                          key, outcome.retry_after, outcome.detail)
                if not self._sleep_until(self._clock() + outcome.retry_after):
                    return _ABANDONED
                continue

            if outcome.kind == _RETRYABLE:
                failures += 1
                if failures >= self._max_attempts:
                    return self._give_up(item, "嘗試 %d 次仍失敗，放棄這一則：%s"
                                         % (failures, outcome.detail))
                delay = min(self._backoff_base * (2 ** (failures - 1)), self._backoff_max)
                self._log(logging.WARNING, "A 頻道送出失敗（key=%s，第 %d/%d 次），%s 秒後重試：%s",
                          key, failures, self._max_attempts, delay, outcome.detail)
                if not self._sleep_until(self._clock() + delay):
                    return _ABANDONED
                continue

            return self._give_up(item, "不可重試的錯誤，放棄這一則：%s" % outcome.detail)

    def _give_up(self, item, detail):
        self._log(logging.ERROR, "A 頻道訊息送出失敗（key=%s）：%s", _safe_repr(item.key), detail)
        self._set_last_error(detail)
        return _FAILED

    def _attempt(self, item):
        """發出一次請求並分類結果。例外只留下遮罩後的描述字串。"""
        payload = {
            "chat_id": self._chat_id,
            "text": item.text,
            "link_preview_options": {"is_disabled": True},
        }
        self._record_request()
        try:
            response = self._post(self._url, json=payload, timeout=self._http_timeout)
        except requests.exceptions.SSLError as exc:
            # SSLError 是 ConnectionError 的子類，必須先接：憑證問題重試幾次都一樣
            return _Outcome(_FATAL, self._describe_exception(exc)
                            + "（SSL 憑證驗證失敗；這台機器若在 SSL inspection 後面，"
                              "把 CA bundle 路徑放進環境變數 REQUESTS_CA_BUNDLE）")
        except _RETRYABLE_EXCEPTIONS as exc:
            return _Outcome(_RETRYABLE, self._describe_exception(exc))
        except requests.exceptions.RequestException as exc:
            return _Outcome(_FATAL, self._describe_exception(exc))
        return self._classify_response(response)

    def _classify_response(self, response):
        status = getattr(response, "status_code", None)
        try:
            body = response.json()
        except Exception:  # noqa: BLE001 — 不是 JSON；例外訊息可能帶內容，直接丟掉
            body = None
        if not isinstance(body, dict):
            body = {}
        description = body.get("description")
        description = self._mask(str(description))[:300] if description is not None else "（沒有 description）"

        if status == 200:
            if body.get("ok") is True:
                result = body.get("result")
                message_id = result.get("message_id") if isinstance(result, dict) else None
                return _Outcome(_OK, message_id=message_id)
            return _Outcome(_FATAL, "HTTP 200 但回應不是 ok=true（%s）；不重試以免重複發送"
                            % description)
        if status == 429:
            retry_after, note = self._retry_after(body, response)
            return _Outcome(_RATE_LIMITED, "HTTP 429：%s" % description,
                            retry_after=retry_after, note=note)
        if isinstance(status, int) and 500 <= status <= 599:
            return _Outcome(_RETRYABLE, "HTTP %d：%s" % (status, description))
        hint = _STATUS_HINTS.get(status, "不可重試的回應")
        return _Outcome(_FATAL, "HTTP %s：%s（%s）" % (status, description, hint))

    def _retry_after(self, body, response):
        """429 要等幾秒：parameters.retry_after → Retry-After 標頭 → 保底。回傳 (秒數, 註記或 None)。"""
        parameters = body.get("parameters")
        if isinstance(parameters, dict):
            seconds = _positive_seconds(parameters.get("retry_after"))
            if seconds is not None:
                return seconds, None
        try:
            header = response.headers.get("Retry-After")
        except Exception:  # noqa: BLE001
            header = None
        seconds = _positive_seconds(header)
        if seconds is not None:
            return seconds, "JSON 沒有可用的 parameters.retry_after，改用 Retry-After 標頭（%s 秒）" % seconds
        return (self._retry_after_fallback,
                "JSON 與標頭都沒有可用的 retry_after，改等保底的 %s 秒" % self._retry_after_fallback)

    # ---------------------------------------------------------------- 限速與等待
    def _record_request(self):
        now = self._clock()
        self._request_times.append(now)
        self._last_request_at = now

    def _earliest_send_time(self, now):
        times = self._request_times
        while times and times[0] <= now - _RATE_WINDOW_SECONDS:
            times.popleft()
        earliest = now
        if len(times) >= self._max_per_minute:
            earliest = max(earliest, times[len(times) - self._max_per_minute] + _RATE_WINDOW_SECONDS)
        if self._last_request_at is not None and self._min_interval > 0:
            earliest = max(earliest, self._last_request_at + self._min_interval)
        return earliest

    def _wait_for_send_slot(self):
        """等到限速允許再發一次請求。回傳 False 代表 stop 的期限會先到，這一則放棄。"""
        while True:
            with self._cond:
                deadline, abort = self._deadline, self._abort_requested
            now = self._clock()
            if abort or (deadline is not None and now >= deadline):
                return False
            earliest = self._earliest_send_time(now)
            if earliest <= now:
                return True
            if not self._sleep_until(earliest):
                return False

    def _sleep_until(self, target):
        """等到 clock() >= target。stop 的期限若會先到就不等了，回傳 False。

        injected wait 可能提早返回（被 stop 叫醒），所以每一輪都以 clock 重新計算。
        """
        while True:
            with self._cond:
                deadline, abort = self._deadline, self._abort_requested
            if abort or (deadline is not None and target > deadline):
                return False
            now = self._clock()
            if now >= target:
                return True
            self._wait(target - now)

    def _teardown(self):
        """工作執行緒已結束：關 session、拆第三方 logger 的 filter。"""
        with self._cond:
            session, log_filter = self._session, self._log_filter
            self._session = None
            self._log_filter = None
        if log_filter is not None:
            for name in _THIRD_PARTY_LOGGERS:
                logging.getLogger(name).removeFilter(log_filter)
        if session is not None:
            try:
                session.close()
            except Exception:  # noqa: BLE001
                pass


# ============================== 實機冒煙（AC-8） ==============================
EXIT_SENT, EXIT_FAILED, EXIT_NOT_TESTED = 0, 1, 3


def smoke(stop_timeout=60):
    """送出一則「[測試] T1′ 冒煙 <台北時間>」並等它送完。回傳 exit code。

    缺密鑰時不碰網路、不建日誌檔，直接回報未實測。只印統計，不印任何密鑰。
    """
    missing = [name for name in (config.TG_BOT_TOKEN_ENV, config.TG_CHANNEL_ID_ENV)
               if not config.secret_is_set(name)]
    if missing:
        print("未實測（缺密鑰）：%s 未設定，沒有送出任何訊息" % "、".join(missing))
        return EXIT_NOT_TESTED

    from live import logsetup
    logsetup.setup()
    now = datetime.now(logsetup.TAIPEI).strftime("%Y-%m-%d %H:%M:%S")
    sender = ChannelSender()
    sender.start()
    sender.send("[測試] T1′ 冒煙 %s" % now, key="t1-smoke")
    remaining = sender.stop(timeout=stop_timeout)
    s = sender.stats()
    if s["sent"] == 1:
        print("冒煙成功：HTTP 200 且 ok=true，message_id=%s，時間 %s" % (s["last_message_id"], now))
        return EXIT_SENT
    print("冒煙失敗：sent=%d failed=%d abandoned=%d 未送出=%d，最後錯誤：%s"
          % (s["sent"], s["failed"], s["abandoned"], remaining, s["last_error"]))
    return EXIT_FAILED


def main(argv=None):
    stream = sys.stdout
    if stream is not None and hasattr(stream, "reconfigure"):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (OSError, ValueError):
            pass
    parser = argparse.ArgumentParser(
        prog="python -m live.tg_channel",
        description="A 頻道發送器的實機冒煙。必須明確帶 --smoke 才會送出訊息。")
    parser.add_argument("--smoke", action="store_true", required=True,
                        help="送出一則 [測試] 訊息到 A 頻道並確認 HTTP 200 與 ok=true")
    parser.parse_args(argv)
    return smoke()


if __name__ == "__main__":
    sys.exit(main())
