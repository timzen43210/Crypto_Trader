# -*- coding: utf-8 -*-
"""
T1′ (live.tg_channel) 驗收測試 — A 頻道廣播：發送佇列、限速與重試。

  AC-1  送出三則，假 HTTP 依序收到、內容與 chat_id 正確、沒有 parse_mode；send() 在網路
        卡住時照樣立刻返回
  AC-2  中間一則遇到 429 / 5xx 重試時，後面的不可以先送出（多則混合失敗時逐次比對完整呼叫順序）
  AC-3  retry_after = N → 恰好等 N 秒（假時鐘）才重送同一則；429 不吃 TG_SEND_MAX_ATTEMPTS；
        retry_after 缺少時的保底；連續 429 超過上限就放棄這一則、換下一則
  AC-4  5xx / 連線錯誤 / 逾時 → 退避重試到用盡，記 ERROR（含 key）並繼續；400/401/403/404、
        SSL、HTTP 200 但 ok 不是 true → 不重試，記 ERROR 並繼續
  AC-5  連續 50 則，任何 60 秒視窗內送出數 ≤ 上限；檢查函式本身拿「沒限速」的發送器證明有鑑別力
  AC-6  假 token 的任何 8 字元片段不出現在：所有日誌（含 traceback）、往外拋的例外與整條
        __cause__ / __context__ 鏈、stats()、repr(sender)。錯誤路徑全部走一遍，並且：
          ・拿「不遮罩」與「直接 logger.error(e) / logger.exception」兩種爛實作跑同一條檢查，必須抓到
          ・例外鏈檢查本身拿 from e / 隱含 context / from None 證明抓得到
          ・真的 requests（在離線籠子裡）產生的 ConnectionError 也遮得掉
          ・urllib3 自己寫網址的 logger 在 start() 之後被遮罩、stop() 之後 filter 拆掉
  AC-7  沒設密鑰 → start() 當下拋 MissingSecretError，不起執行緒、訊息不含值；補上後可再 start
  AC-8  （實機冒煙本身不在這裡跑）`python -m live.tg_channel --smoke` 缺密鑰時回報未實測、不連網
  AC-9  python -m live 列出 TG_* 參數且密鑰只顯示已設定 / 未設定；模組名不撞名；只依賴標準庫 + requests
  FR-4  stop(timeout) 逾時放棄並回報則數、stop 叫得醒長時間的 429 等待、工作執行緒遇到未預期例外
        記錄後繼續運作

全程離線：每個測試都包在 OfflineCage 裡（socket 的連線與 DNS 出口全擋，進場先自我測試），
離開時檢查沒有任何連線企圖；同一個籠子也把 time.sleep 換成會失敗的替身，證明沒有真的 sleep。
籠子是 scoped 的：import 本檔不會留下任何全域 patch（test_import_does_not_leave_socket_patched）。
時間一律用 FakeClock 推進；需要等工作執行緒的地方用 Event / join 並設上限，不靠排程的偶然時序。

假 token / channel id 只存在於本檔與本行程的環境變數（結束時還原），不寫進任何檔案。
不依賴 pytest：直接 `python tests/test_tg_channel.py` 會逐一跑完並印結果。
"""
import ast
import collections
import contextlib
import copy
import logging
import os
import socket
import subprocess
import sys
import textwrap
import threading
import time
import traceback
from datetime import datetime, timezone

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)


def _current_patchables():
    return {
        "socket.socket.connect": socket.socket.connect,
        "socket.socket.connect_ex": socket.socket.connect_ex,
        "socket.create_connection": socket.create_connection,
        "socket.getaddrinfo": socket.getaddrinfo,
        "socket.gethostbyname": socket.gethostbyname,
        "socket.gethostbyname_ex": socket.gethostbyname_ex,
        "socket.gethostbyaddr": socket.gethostbyaddr,
        "time.sleep": time.sleep,
    }


# F4：在 import 任何專案模組 / requests 之前先抓住原函式物件，之後以身分比對確認沒被換掉
PRISTINE = _current_patchables()
PRISTINE_SOCKET_CLASS_DICT = {name: name in socket.socket.__dict__ for name in ("connect", "connect_ex")}

import requests  # noqa: E402
from urllib3.connectionpool import HTTPSConnectionPool  # noqa: E402
from urllib3.exceptions import MaxRetryError, NewConnectionError  # noqa: E402

from live import config, tg_channel  # noqa: E402
from live.tg_channel import MASK, ChannelSender  # noqa: E402

# 特徵明顯的假密鑰（格式照真的 token：<bot id>:<35 字元左右的祕密>）
FAKE_TOKEN = "7777123456:AAE-t1pLeakCanary_Zq9xVw8uYt7sRp6oN5"
FAKE_CHAT = "-1009876543210"
EXPECTED_URL = config.TG_API_BASE_URL + "/bot" + FAKE_TOKEN + "/sendMessage"
SECRET_ENVS = (config.TG_BOT_TOKEN_ENV, config.TG_CHANNEL_ID_ENV)
PROXY_ENVS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")


# ============================== 離線籠子 ==============================
class NetworkBlocked(ConnectionRefusedError):
    """籠子攔下的連線企圖。繼承 OSError：urllib3 / requests 會把它當成一般的連線失敗，
    走跟真實斷網一模一樣的例外路徑（ConnectionError 的字串裡會帶完整網址）。"""


class RealSleepForbidden(AssertionError):
    """測試中呼叫了真的 time.sleep。"""


class OfflineCage:
    """scoped 的離線籠子：擋 socket 的連線與 DNS 出口、把 time.sleep 換成會失敗的替身。

    進場先自我測試（每條出口各試一次，都必須被攔下），離場一律還原成原本的物件。
    自我測試的企圖不計入 attempts。
    """

    PROBE = ("198.51.100.1", 9)   # RFC 5737 TEST-NET-2，不可路由；而且攔截發生在任何系統呼叫之前

    def __init__(self):
        self.attempts = []
        self.sleeps = []
        self._saved = None
        self._probing = False

    def _block(self, how, target):
        if not self._probing:
            self.attempts.append((how, repr(target)))
        return NetworkBlocked("離線測試禁止連網（%s -> %r）" % (how, target))

    def __enter__(self):
        cage = self

        def connect(sock, address):
            raise cage._block("socket.connect", address)

        def connect_ex(sock, address):
            raise cage._block("socket.connect_ex", address)

        def create_connection(address, *a, **k):
            raise cage._block("socket.create_connection", address)

        def getaddrinfo(host, port, *a, **k):
            raise cage._block("socket.getaddrinfo", (host, port))

        def gethostbyname(host):
            raise cage._block("socket.gethostbyname", host)

        def gethostbyname_ex(host):
            raise cage._block("socket.gethostbyname_ex", host)

        def gethostbyaddr(host):
            raise cage._block("socket.gethostbyaddr", host)

        def sleep(seconds):
            if not cage._probing:
                cage.sleeps.append(seconds)
            raise RealSleepForbidden("測試中禁止真的 sleep（%r 秒）" % (seconds,))

        self._saved = _current_patchables()
        socket.socket.connect = connect
        socket.socket.connect_ex = connect_ex
        socket.create_connection = create_connection
        socket.getaddrinfo = getaddrinfo
        socket.gethostbyname = gethostbyname
        socket.gethostbyname_ex = gethostbyname_ex
        socket.gethostbyaddr = gethostbyaddr
        time.sleep = sleep
        try:
            self._self_test()
        except BaseException:
            self._restore()
            raise
        return self

    def _self_test(self):
        def fresh_socket_connect():
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect(self.PROBE)
            finally:
                s.close()

        probes = [
            lambda: socket.create_connection(self.PROBE, timeout=1),
            fresh_socket_connect,
            lambda: socket.getaddrinfo("example.invalid", 80),
            lambda: socket.gethostbyname("example.invalid"),
            lambda: time.sleep(0),
        ]
        self._probing = True
        blocked = 0
        try:
            for probe in probes:
                try:
                    probe()
                except (NetworkBlocked, RealSleepForbidden):
                    blocked += 1
                except Exception:  # noqa: BLE001 — 其他例外代表沒攔到
                    pass
        finally:
            self._probing = False
        if blocked != len(probes):
            raise AssertionError("離線籠子自我測試沒過：%d/%d 條出口被攔下" % (blocked, len(probes)))

    def _restore(self):
        saved = self._saved
        for attr in ("connect", "connect_ex"):
            if PRISTINE_SOCKET_CLASS_DICT[attr]:
                setattr(socket.socket, attr, saved["socket.socket." + attr])
            else:
                # 原本是繼承來的，不是 socket.socket 自己的屬性：刪掉就回到原狀
                delattr(socket.socket, attr)
        socket.create_connection = saved["socket.create_connection"]
        socket.getaddrinfo = saved["socket.getaddrinfo"]
        socket.gethostbyname = saved["socket.gethostbyname"]
        socket.gethostbyname_ex = saved["socket.gethostbyname_ex"]
        socket.gethostbyaddr = saved["socket.gethostbyaddr"]
        time.sleep = saved["time.sleep"]

    def __exit__(self, *exc):
        self._restore()
        return False


# ============================== 假時鐘、假 Telegram ==============================
class FakeClock:
    """單調時鐘 + 等待函式。wait(s) 直接把時間往前推 s 秒並記下來，不真的等。"""

    EPOCH = 1_790_000_000.0

    def __init__(self, start=1000.0):
        self.t = float(start)
        self.waits = []

    def now(self):
        return self.t

    def wall(self):
        return self.EPOCH + self.t

    def wait(self, seconds):
        if not seconds > 0:
            raise AssertionError("wait(%r)：非正數的等待代表計算錯了（會變成原地空轉）" % (seconds,))
        self.waits.append(seconds)
        self.t += seconds


class FakeResponse:
    def __init__(self, status, body=None, headers=None, text=None):
        self.status_code = status
        self._body = body
        self.headers = dict(headers or {})
        self.text = text if text is not None else repr(body)

    def json(self):
        if isinstance(self._body, (dict, list)):
            return copy.deepcopy(self._body)
        raise ValueError("Expecting value: " + str(self.text)[:200])


def ok(message_id=None):
    return FakeResponse(200, {"ok": True, "result": {"message_id": message_id or 1}})


def http_error(status, description, headers=None, **parameters):
    body = {"ok": False, "error_code": status, "description": description}
    if parameters:
        body["parameters"] = parameters
    return FakeResponse(status, body, headers=headers)


def too_many(retry_after):
    return http_error(429, "Too Many Requests: retry after %s" % (retry_after,), retry_after=retry_after)


def conn_error(url):
    """跟 requests 真的拋出來的一樣：字串帶完整路徑（含 token），__context__ 掛著 urllib3 的例外。"""
    path = url[len(config.TG_API_BASE_URL):]
    reason = NewConnectionError(None, "Failed to establish a new connection: [WinError 10061] refused")
    try:
        try:
            raise MaxRetryError(HTTPSConnectionPool("api.telegram.org", 443), path, reason)
        except MaxRetryError as inner:
            raise requests.exceptions.ConnectionError(inner)
    except requests.exceptions.ConnectionError as outer:
        return outer


def read_timeout(url):
    try:
        try:
            raise TimeoutError("The read operation timed out while reading " + url)
        except TimeoutError as inner:
            raise requests.exceptions.ReadTimeout(
                "HTTPSConnectionPool(host='api.telegram.org', port=443): Read timed out. "
                "(read timeout=10) url=" + url) from inner
    except requests.exceptions.ReadTimeout as outer:
        return outer


def ssl_error(url):
    return requests.exceptions.SSLError("certificate verify failed while connecting to " + url)


def unexpected(url):
    try:
        try:
            raise ValueError("inner cause mentions " + url)
        except ValueError as inner:
            raise RuntimeError("未預期的錯誤，網址 " + url) from inner
    except RuntimeError as outer:
        return outer


Call = collections.namedtuple("Call", "t url payload timeout")


class FakeTelegram:
    """假的 post()。依 plan() 的順序回應每一次請求；plan 用完之後一律成功。

    plan 裡的元素可以是 FakeResponse、例外、或 callable(url) -> 兩者之一（讓例外帶上真正的網址）。
    gate 設了的話，每次請求都會卡在 gate 上直到它被打開（用來證明 send() 不等網路）。
    """

    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.script = collections.deque()
        self.gate = None
        self.entered = threading.Event()
        self._lock = threading.Lock()

    def plan(self, *actions):
        self.script.extend(actions)
        return self

    def __call__(self, url, json=None, timeout=None):
        with self._lock:
            self.calls.append(Call(self.clock.now(), url, copy.deepcopy(json), timeout))
            action = self.script.popleft() if self.script else None
            n = len(self.calls)
        self.entered.set()
        if self.gate is not None and not self.gate.wait(10):
            raise AssertionError("FakeTelegram：gate 10 秒內沒被打開")
        if action is None:
            action = ok(n)
        if callable(action):
            action = action(url)
        if isinstance(action, BaseException):
            raise action
        return action

    def texts(self):
        return [c.payload["text"] for c in self.calls]

    def times(self):
        return [c.t for c in self.calls]


# ============================== 日誌、環境、harness ==============================
class LogCapture(logging.Handler):
    """掛在根 logger 上收下所有紀錄的格式化結果（Formatter 會附上 exc_info 的 traceback）。"""

    def __init__(self):
        super().__init__(logging.DEBUG)
        self.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        self.entries = []

    def emit(self, record):
        self.entries.append((record.levelno, record.name, self.format(record)))

    @property
    def lines(self):
        return [e[2] for e in self.entries]

    def text(self):
        return "\n".join(self.lines)

    def at(self, level, name="live.tg_channel"):
        return [line for lv, nm, line in self.entries if lv == level and nm == name]


@contextlib.contextmanager
def env_vars(values):
    """暫時設定 / 拿掉環境變數（值是 None 代表拿掉），結束時原樣還原。"""
    saved = {n: os.environ.get(n) for n in values}
    try:
        for n, v in values.items():
            if v is None:
                os.environ.pop(n, None)
            else:
                os.environ[n] = v
        yield
    finally:
        for n, v in saved.items():
            if v is None:
                os.environ.pop(n, None)
            else:
                os.environ[n] = v


def secrets_env(token=FAKE_TOKEN, chat=FAKE_CHAT):
    return env_vars({config.TG_BOT_TOKEN_ENV: token, config.TG_CHANNEL_ID_ENV: chat})


class Harness:
    def __init__(self, cage, logs):
        self.cage = cage
        self.logs = logs
        self.clock = FakeClock()
        self.tg = FakeTelegram(self.clock)
        self.senders = []

    def sender(self, cls=ChannelSender, **kw):
        kw.setdefault("post", self.tg)
        kw.setdefault("clock", self.clock.now)
        kw.setdefault("wall_clock", self.clock.wall)
        kw.setdefault("wait", self.clock.wait)
        s = cls(**kw)
        self.senders.append(s)
        return s

    def started(self, cls=ChannelSender, **kw):
        s = self.sender(cls, **kw)
        s.start()
        return s


def _worker_threads():
    return [t for t in threading.enumerate() if t.name == "tg-channel-sender" and t.is_alive()]


@contextlib.contextmanager
def harness(token=FAKE_TOKEN, chat=FAKE_CHAT, allow_network_attempts=False):
    logs = LogCapture()
    root = logging.getLogger()
    saved_level = root.level
    root.addHandler(logs)
    root.setLevel(logging.DEBUG)
    try:
        with secrets_env(token, chat), OfflineCage() as cage:
            h = Harness(cage, logs)
            try:
                yield h
            finally:
                if h.tg.gate is not None:
                    h.tg.gate.set()
                for s in h.senders:
                    if s.stats()["state"] in ("running", "stopping"):
                        stop_within(s, 0)
        assert not _worker_threads(), "測試結束後還有發送執行緒活著"
        if not allow_network_attempts:
            assert not cage.attempts, "有程式企圖連網：%r" % cage.attempts
        assert not cage.sleeps, "有程式呼叫了真的 time.sleep：%r" % cage.sleeps
    finally:
        root.removeHandler(logs)
        root.setLevel(saved_level)


def stop_within(sender, timeout, limit=10.0):
    """在另一條執行緒呼叫 stop(timeout)，真實時間 limit 秒內沒返回就判失敗（不讓測試掛住）。"""
    box = {}

    def run():
        try:
            box["result"] = sender.stop(timeout)
        except BaseException as e:  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=run, name="test-stopper", daemon=True)
    t.start()
    t.join(limit)
    if t.is_alive():
        raise AssertionError("stop(%r) 在真實時間 %s 秒內沒有返回" % (timeout, limit))
    if "error" in box:
        raise box["error"]
    return box["result"]


def wait_until(predicate, what, limit=5.0):
    """以 Event.wait 輪詢等條件成立（不用 time.sleep），真實時間 limit 秒為上限。"""
    tick = threading.Event()
    end = time.monotonic() + limit
    while not predicate():
        if time.monotonic() > end:
            raise AssertionError("等不到：" + what)
        tick.wait(0.002)


# ============================== AC-6 的洩漏檢查工具 ==============================
def fragments(secret, n=8):
    return {secret[i:i + n] for i in range(len(secret) - n + 1)}


def find_leaks(texts, secrets=(FAKE_TOKEN, FAKE_CHAT), n=8):
    """回傳 [(第幾段文字, 命中的片段)]。任何密鑰的任何連續 n 字元片段出現就算洩漏。"""
    hits = []
    for secret in secrets:
        frags = fragments(secret, n)
        for idx, text in enumerate(texts):
            for i in range(len(text) - n + 1):
                if text[i:i + n] in frags:
                    hits.append((idx, text[i:i + n]))
                    break
    return hits


def exception_texts(exc):
    """一個例外與它整條 __cause__ / __context__ 鏈上每一個例外的 str、repr、args、traceback。

    __suppress_context__ 不影響這裡：被 `from None` 壓掉的 context 仍然掛在物件上，
    呼叫端一樣拿得到，所以照樣算。
    """
    out = []
    seen = set()
    stack = [exc]
    while stack:
        e = stack.pop()
        if e is None or id(e) in seen:
            continue
        seen.add(id(e))
        out.append(str(e))
        out.append(repr(e))
        out.extend(repr(a) for a in e.args)
        out.append("".join(traceback.format_exception(type(e), e, e.__traceback__)))
        stack.append(e.__cause__)
        stack.append(e.__context__)
    return out


class NoMaskSender(ChannelSender):
    """爛實作 1：完全不遮罩。"""

    def _mask(self, text):
        return text if isinstance(text, str) else str(text)


_naive_logger = logging.getLogger("live.tg_channel.naive")


class NaiveLoggingSender(ChannelSender):
    """爛實作 2：最常見的寫法 —— 直接把例外丟給 logger（其餘照正確實作）。"""

    def _describe_exception(self, exc):
        _naive_logger.error("送出失敗：%s", exc)
        return super()._describe_exception(exc)

    def _log_unexpected(self, where, item, exc):
        _naive_logger.exception("發送執行緒發生未預期的例外")
        super()._log_unexpected(where, item, exc)


def run_leak_scenario(cls):
    """把所有錯誤路徑走一遍，回傳 (所有收集到的文字, 往外拋的例外, 日誌全文, stats)。"""
    with harness() as h:
        outward = []

        def call(fn):
            try:
                return fn()
            except Exception as e:  # noqa: BLE001
                outward.append(e)

        h.tg.plan(
            conn_error, conn_error,                                               # 0 連線錯誤 x2
            read_timeout, read_timeout,                                           # 1 逾時 x2
            lambda url: http_error(429, "Too Many Requests " + url, retry_after=3), ok(),  # 2 429 → 成功
            lambda url: http_error(400, "Bad Request: chat not found %s chat_id=%s" % (url, FAKE_CHAT)),
            lambda url: http_error(401, "Unauthorized " + url),                   # 4
            lambda url: http_error(403, "Forbidden: bot is not a member " + url),  # 5
            lambda url: http_error(502, "Bad Gateway " + url),                    # 6 5xx x2
            lambda url: http_error(503, "Service Unavailable " + url),
            unexpected,                                                           # 7 未預期例外
            ssl_error,                                                            # 8
            lambda url: FakeResponse(200, {"ok": False, "description": "weird " + url}),   # 9
            lambda url: FakeResponse(200, None, text="<html>" + url),             # 10 不是 JSON
            lambda url: http_error(429, "Too Many Requests (no retry_after) " + url), ok(),  # 11 保底
        )
        s = h.started(cls, max_attempts=2)
        for i in range(12):
            call(lambda i=i: s.send("msg %d" % i, key="leak-%d" % i))
        call(lambda: s.send(12345))                        # TypeError
        call(lambda: s.send("x" * 5000, key="too-long"))   # 拒收
        call(s.start)                                      # RuntimeError（start 兩次）
        stop_within(s, 3600)
        call(lambda: s.send("after stop", key="late"))     # 拒收
        with secrets_env(FAKE_TOKEN, None):                # token 有設、channel id 沒設
            other = h.sender(cls)
            call(other.start)                              # MissingSecretError
            call(lambda: other.send("x"))                  # RuntimeError（還沒 start）
        st = s.stats()
        texts = list(h.logs.lines) + [repr(s), str(s), repr(st), str(st), repr(other), repr(other.stats())]
        for e in outward:
            texts.extend(exception_texts(e))
        return texts, outward, h.logs.text(), st


# ============================== 離線籠子自證 ==============================
def test_import_does_not_leave_socket_patched():
    """F4：import 本檔（連同 requests、live.tg_channel）不得留下任何全域 patch。"""
    now = _current_patchables()
    for name, fn in PRISTINE.items():
        assert now[name] is fn, "%s 被換掉了（實際 %r）" % (name, now[name])


def test_offline_cage_blocks_every_exit_and_restores():
    with OfflineCage() as cage:
        for probe in (lambda: socket.create_connection(("api.telegram.org", 443), timeout=1),
                      lambda: socket.getaddrinfo("api.telegram.org", 443),
                      lambda: requests.get("https://api.telegram.org/", timeout=1)):
            try:
                probe()
            except (NetworkBlocked, requests.exceptions.ConnectionError):
                pass
            else:
                raise AssertionError("籠子沒擋住")
        try:
            time.sleep(0.01)
        except RealSleepForbidden:
            pass
        else:
            raise AssertionError("time.sleep 沒被換掉")
        assert len(cage.attempts) >= 3, cage.attempts
        assert cage.sleeps == [0.01]
    now = _current_patchables()
    for name, fn in PRISTINE.items():
        assert now[name] is fn, "離開籠子後 %s 沒有還原" % name
    for attr, was_own in PRISTINE_SOCKET_CLASS_DICT.items():
        assert (attr in socket.socket.__dict__) == was_own, "socket.socket.%s 的歸屬被改了" % attr


# ============================== AC-1 ==============================
def test_ac1_send_returns_immediately_and_three_arrive_in_order():
    with harness() as h:
        h.tg.gate = threading.Event()
        s = h.started()
        texts = ["🔴 進場 *粗體* _底線_ [連結](x) <b>&amp; 100%", "第二則", "第三則"]
        assert s.send(texts[0], key="sig-1") is True
        assert h.tg.entered.wait(5), "工作執行緒沒有送出第一則"
        # 網路（假 post）此刻卡在 gate 上：send() 仍然立刻返回
        assert s.send(texts[1], key="sig-2") is True
        assert s.send(texts[2], key="sig-3") is True
        assert len(h.tg.calls) == 1 and not h.tg.gate.is_set()
        st = s.stats()
        assert (st["queued"], st["in_flight"], st["sent"]) == (2, True, 0), st
        h.tg.gate.set()
        assert stop_within(s, 3600) == 0
        assert h.tg.texts() == texts, h.tg.texts()
        for c in h.tg.calls:
            assert c.url == EXPECTED_URL
            assert c.payload["chat_id"] == FAKE_CHAT
            assert "parse_mode" not in c.payload, c.payload
            assert set(c.payload) == {"chat_id", "text", "link_preview_options"}, c.payload
            assert c.payload["link_preview_options"] == {"is_disabled": True}
            assert c.timeout == config.TG_HTTP_TIMEOUT_SECONDS
        st = s.stats()
        assert (st["sent"], st["failed"], st["rejected"], st["queued"]) == (3, 0, 0, 0), st
        expected_at = datetime.fromtimestamp(FakeClock.EPOCH + h.tg.calls[-1].t,
                                             timezone.utc).isoformat(timespec="seconds")
        assert st["last_success_at"] == expected_at, st
        assert datetime.fromisoformat(st["last_success_at"]).utcoffset().total_seconds() == 0
        assert st["last_message_id"] == 3


# ============================== AC-2 ==============================
def test_ac2_middle_429_does_not_let_later_messages_overtake():
    with harness() as h:
        h.tg.plan(ok(), too_many(5), ok(), ok())
        s = h.started()
        for text in ("A-進場", "B-出場", "C-下一筆"):
            s.send(text, key=text)
        stop_within(s, 3600)
        assert h.tg.texts() == ["A-進場", "B-出場", "B-出場", "C-下一筆"], h.tg.texts()
        t = h.tg.times()
        assert t[2] - t[1] == 5 and t[3] >= t[2], t


def test_ac2_middle_5xx_does_not_let_later_messages_overtake():
    with harness() as h:
        h.tg.plan(ok(), http_error(503, "x"), http_error(502, "y"), ok(), ok())
        s = h.started()
        for text in ("A", "B", "C"):
            s.send(text, key=text)
        stop_within(s, 3600)
        assert h.tg.texts() == ["A", "B", "B", "B", "C"], h.tg.texts()


def test_ac2_exact_call_order_under_mixed_failures():
    """30 則混合 5xx / 429 / 連線錯誤用盡：完整的請求序列必須剛好是逐則、逐次、不交錯。"""
    n_attempts = config.TG_SEND_MAX_ATTEMPTS
    plan, expected = [], []
    for i in range(30):
        if i % 6 == 1:
            plan += [http_error(503, "x"), ok()]
            expected += [i, i]
        elif i % 6 == 3:
            plan += [too_many(2), ok()]
            expected += [i, i]
        elif i % 6 == 5:
            plan += [conn_error] * n_attempts
            expected += [i] * n_attempts
        else:
            plan += [ok()]
            expected += [i]
    with harness() as h:
        h.tg.plan(*plan)
        s = h.started()
        for i in range(30):
            s.send("m%02d" % i, key=i)
        stop_within(s, 3600)
        got = [int(text[1:]) for text in h.tg.texts()]
        assert got == expected, got
        st = s.stats()
        assert (st["sent"], st["failed"]) == (25, 5), st


# ============================== AC-3 ==============================
def test_ac3_429_waits_exactly_retry_after_then_resends_same_message():
    for n in (1, 7, 45):
        with harness() as h:
            h.tg.plan(too_many(n), ok())
            s = h.started()
            s.send("訊號", key="k")
            stop_within(s, 3600)
            t = h.tg.times()
            assert len(t) == 2 and t[1] - t[0] == n, (n, t)
            assert h.tg.calls[0].payload == h.tg.calls[1].payload
            # 期間的等待加起來恰好 N 秒：沒有提早，也沒有多等
            assert sum(h.clock.waits) == n, (n, h.clock.waits)
            assert s.stats()["sent"] == 1


def test_ac3_429_does_not_consume_send_attempts():
    with harness() as h:
        h.tg.plan(too_many(3), too_many(3), too_many(3), ok())
        s = h.started(max_attempts=2)
        s.send("A", key="A")
        stop_within(s, 3600)
        assert h.tg.texts() == ["A"] * 4
        assert (s.stats()["sent"], s.stats()["failed"]) == (1, 0)
    with harness() as h:
        # 503 算 1 次、429 不算、503 算第 2 次 → 用盡
        h.tg.plan(http_error(503, "x"), too_many(3), http_error(503, "y"))
        s = h.started(max_attempts=2)
        s.send("A", key="A")
        stop_within(s, 3600)
        assert h.tg.texts() == ["A"] * 3
        assert s.stats()["failed"] == 1


def test_ac3_retry_after_missing_or_invalid_falls_back_with_warning():
    fallback = config.TG_RETRY_AFTER_FALLBACK_SECONDS
    cases = [
        (http_error(429, "no params", headers={"Retry-After": "12"}), 12, "Retry-After"),
        (http_error(429, "no params"), fallback, "保底"),
        (http_error(429, "bad", retry_after="abc"), fallback, "保底"),
        (http_error(429, "zero", retry_after=0), fallback, "保底"),
        (http_error(429, "negative", retry_after=-5), fallback, "保底"),
        (http_error(429, "bool", retry_after=True), fallback, "保底"),
        (http_error(429, "huge", retry_after=10 ** 400), fallback, "保底"),
    ]
    for response, expected_wait, word in cases:
        with harness() as h:
            h.tg.plan(response, ok())
            s = h.started()
            s.send("A", key="A")
            stop_within(s, 3600)
            t = h.tg.times()
            assert t[1] - t[0] == expected_wait, (response._body, t)
            warnings = h.logs.at(logging.WARNING)
            assert any(word in w and "格式" in w for w in warnings), warnings


def test_ac3_repeated_429_gives_up_and_moves_on():
    with harness() as h:
        h.tg.plan(too_many(1), too_many(1), too_many(1), ok())
        s = h.started(max_rate_limited_retries=2)
        s.send("A", key="sig-A")
        s.send("B", key="sig-B")
        stop_within(s, 3600)
        assert h.tg.texts() == ["A", "A", "A", "B"], h.tg.texts()
        st = s.stats()
        assert (st["sent"], st["failed"]) == (1, 1), st
        errors = h.logs.at(logging.ERROR)
        assert any("sig-A" in e and "429" in e for e in errors), errors


# ============================== AC-4 ==============================
def _backoffs(n_attempts, base, cap):
    return [min(base * 2 ** k, cap) for k in range(n_attempts - 1)]


def test_ac4_5xx_retried_with_backoff_until_exhausted_then_next():
    n = config.TG_SEND_MAX_ATTEMPTS
    with harness() as h:
        h.tg.plan(*[http_error(502, "Bad Gateway")] * n, ok())
        s = h.started()
        s.send("B", key="sig-B")
        s.send("C", key="sig-C")
        stop_within(s, 3600)
        assert h.tg.texts() == ["B"] * n + ["C"], h.tg.texts()
        t = h.tg.times()
        diffs = [b - a for a, b in zip(t[:n], t[1:n])]
        assert diffs == _backoffs(n, config.TG_RETRY_BACKOFF_BASE_SECONDS,
                                  config.TG_RETRY_BACKOFF_MAX_SECONDS), diffs
        # 最後一次失敗之後不再白等退避，只等一般的相鄰間隔就換下一則
        assert t[n] - t[n - 1] == config.TG_MIN_INTERVAL_SECONDS, t
        st = s.stats()
        assert (st["sent"], st["failed"]) == (1, 1), st
        errors = h.logs.at(logging.ERROR)
        assert any("sig-B" in e and "嘗試 %d 次" % n in e for e in errors), errors
        assert "HTTP 502" in st["last_error"], st


def test_ac4_connection_error_and_timeout_are_retried():
    with harness() as h:
        h.tg.plan(conn_error, read_timeout, ok())
        s = h.started()
        s.send("A", key="A")
        stop_within(s, 3600)
        assert h.tg.texts() == ["A"] * 3
        assert s.stats()["sent"] == 1
    for failure in (conn_error, read_timeout):
        with harness() as h:
            h.tg.plan(*[failure] * 3, ok())
            s = h.started(max_attempts=3)
            s.send("A", key="sig-A")
            s.send("B", key="sig-B")
            stop_within(s, 3600)
            assert h.tg.texts() == ["A"] * 3 + ["B"], h.tg.texts()
            assert (s.stats()["sent"], s.stats()["failed"]) == (1, 1)
            assert any("sig-A" in e for e in h.logs.at(logging.ERROR))


def test_ac4_non_retryable_errors_are_not_retried():
    cases = [(http_error(status, "desc-%d" % status), "HTTP %d" % status) for status in (400, 401, 403, 404)]
    cases += [
        (ssl_error, "SSLError"),
        (FakeResponse(200, {"ok": False, "description": "odd"}), "HTTP 200"),
        (FakeResponse(200, None, text="<html>oops</html>"), "HTTP 200"),
    ]
    for failure, word in cases:
        with harness() as h:
            h.tg.plan(failure, ok())
            s = h.started()
            s.send("A", key="sig-A")
            s.send("B", key="sig-B")
            stop_within(s, 3600)
            assert h.tg.texts() == ["A", "B"], (word, h.tg.texts())
            st = s.stats()
            assert (st["sent"], st["failed"]) == (1, 1), (word, st)
            errors = h.logs.at(logging.ERROR)
            assert any("sig-A" in e and word in e for e in errors), (word, errors)


def test_ac4_backoff_is_capped():
    with harness() as h:
        h.tg.plan(*[http_error(500, "x")] * 7)
        s = h.started(max_attempts=7, backoff_base=2.0, backoff_max=10.0)
        s.send("A", key="A")
        stop_within(s, 3600)
        t = h.tg.times()
        assert [b - a for a, b in zip(t, t[1:])] == [2.0, 4.0, 8.0, 10.0, 10.0, 10.0], t


# ============================== AC-5 ==============================
def max_in_any_window(times, window=60.0):
    """任何長度 window 的半開視窗 [s, s+window) 內最多有幾次請求（最密的視窗必然從某次請求開始）。"""
    times = sorted(times)
    return max((sum(1 for u in times if t <= u < t + window) for t in times), default=0)


def test_ac5_rate_limit_holds_for_50_messages():
    limit = config.TG_MAX_MESSAGES_PER_MINUTE
    with harness() as h:
        s = h.started()
        for i in range(50):
            s.send("m%02d" % i, key=i)
        stop_within(s, 3600)
        t = h.tg.times()
        assert len(t) == 50
        assert max_in_any_window(t) <= limit, max_in_any_window(t)
        # 不是過度保守：視窗裡剛好塞滿上限，第 limit+1 則恰好在第一則滿 60 秒時送出
        assert max_in_any_window(t) == limit
        assert t[limit] - t[0] == 60.0, t[:limit + 1]
        assert all(b - a >= config.TG_MIN_INTERVAL_SECONDS for a, b in zip(t, t[1:]))
        assert s.stats()["sent"] == 50


def test_ac5_window_check_catches_an_unthrottled_sender():
    """鑑別力：把限速拿掉，同一個檢查必須抓到超量。"""
    with harness() as h:
        s = h.started(max_per_minute=1000, min_interval=0)
        for i in range(50):
            s.send("m%02d" % i, key=i)
        stop_within(s, 3600)
        assert max_in_any_window(h.tg.times()) > config.TG_MAX_MESSAGES_PER_MINUTE


def test_ac5_429_resends_count_toward_the_window():
    with harness() as h:
        h.tg.plan(*[too_many(1) if i % 4 == 0 else ok() for i in range(80)])
        s = h.started()
        for i in range(50):
            s.send("m%02d" % i, key=i)
        stop_within(s, 3600)
        t = h.tg.times()
        assert len(t) > 50
        assert max_in_any_window(t) <= config.TG_MAX_MESSAGES_PER_MINUTE, max_in_any_window(t)
        assert s.stats()["sent"] == 50


# ============================== AC-6 ==============================
def test_ac6_no_token_fragment_in_any_output():
    texts, outward, log, st = run_leak_scenario(ChannelSender)
    leaks = find_leaks(texts)
    assert not leaks, "洩漏：%r" % leaks[:5]
    # 前提：每一條錯誤路徑都真的走過了（否則「沒洩漏」沒有意義）
    for word in ("ConnectionError", "ReadTimeout", "HTTP 429", "HTTP 400", "HTTP 401", "HTTP 403",
                 "HTTP 502", "HTTP 503", "RuntimeError", "Traceback", "SSLError",
                 "HTTP 200 但回應不是 ok=true", "保底", "拒收"):
        assert word in log, "日誌裡沒有 %r，這條路徑沒走到" % word
    assert log.count(MASK) >= 10, "帶網址的錯誤應該被遮罩，而不是被丟掉"
    assert "/bot" + MASK + "/sendMessage" in log
    kinds = sorted(type(e).__name__ for e in outward)
    assert kinds == ["MissingSecretError", "RuntimeError", "RuntimeError", "TypeError"], kinds
    assert (st["sent"], st["failed"], st["rejected"]) == (2, 10, 2), st


def test_ac6_leak_check_catches_a_sender_that_does_not_mask():
    texts, _, _, _ = run_leak_scenario(NoMaskSender)
    assert find_leaks(texts), "不遮罩的實作居然沒被抓到：檢查沒有鑑別力"


def test_ac6_leak_check_catches_naive_logging_of_exceptions():
    texts, _, log, _ = run_leak_scenario(NaiveLoggingSender)
    assert find_leaks(texts), "直接 logger.error(e) 的實作居然沒被抓到：檢查沒有鑑別力"
    assert "live.tg_channel.naive" in log


def test_ac6_exception_chain_check_has_teeth():
    def raised(kind):
        try:
            try:
                raise OSError("connect failed: " + EXPECTED_URL)
            except OSError as inner:
                if kind == "from_e":
                    raise RuntimeError("送出失敗（訊息已遮罩）") from inner
                if kind == "implicit":
                    raise RuntimeError("送出失敗（訊息已遮罩）")
                raise RuntimeError("送出失敗（訊息已遮罩）") from None
        except RuntimeError as e:
            return e

    for kind in ("from_e", "implicit", "from_none"):
        assert find_leaks(exception_texts(raised(kind))), "%s 的例外鏈應該被抓到" % kind
    try:
        raise RuntimeError("送出失敗（訊息已遮罩）")
    except RuntimeError as clean:
        assert not find_leaks(exception_texts(clean))


def test_ac6_real_requests_connection_error_is_masked():
    """預設的 HTTP 層（requests.Session）在籠子裡：真的 ConnectionError 字串帶著網址，必須被遮掉。"""
    with env_vars({n: None for n in PROXY_ENVS}), harness(allow_network_attempts=True) as h:
        s = h.sender(post=None)
        s.start()
        s.send("real-requests-path", key="real")
        stop_within(s, 3600)
        st = s.stats()
        assert (st["sent"], st["failed"]) == (0, 1), st
        dns = [a for a in h.cage.attempts if a[0] == "socket.getaddrinfo"]
        assert len(dns) == config.TG_SEND_MAX_ATTEMPTS, h.cage.attempts
        assert all("api.telegram.org" in a[1] for a in dns), dns
        log = h.logs.text()
        assert "ConnectionError" in log and "/bot" + MASK + "/sendMessage" in log, log[-2000:]
        assert not find_leaks(h.logs.lines + [repr(s), repr(st)])


def _emit_urllib3_style_records():
    path = "/bot" + FAKE_TOKEN + "/sendMessage"
    logging.getLogger("urllib3.connectionpool").warning(
        "Retrying (%r) after connection broken by '%r': %s", "Retry(total=0)", "err", path)
    logging.getLogger("urllib3.connectionpool").debug(
        '%s://%s:%s "%s %s %s" %s %s', "https", "api.telegram.org", 443, "POST", path, "HTTP/1.1", 200, None)
    try:
        raise ValueError("header parse failed for " + EXPECTED_URL)
    except ValueError:
        logging.getLogger("urllib3.connection").warning(
            "Failed to parse headers (url=%s): %s", EXPECTED_URL, "x", exc_info=True)
    for name in ("urllib3.poolmanager", "urllib3.util.retry", "urllib3.response"):
        logging.getLogger(name).info("Redirecting %s -> %s", EXPECTED_URL, EXPECTED_URL)


def test_ac6_urllib3_loggers_are_masked_while_running():
    with harness() as h:
        s = h.started()
        _emit_urllib3_style_records()
        stop_within(s, 60)
        lines = [line for line in h.logs.lines if " urllib3." in line]
        assert len(lines) == 6, lines
        assert all(MASK in line for line in lines), lines
        assert any("Traceback" in line and "ValueError" in line for line in lines), "traceback 要保留、只遮密鑰"
        assert not find_leaks(h.logs.lines)
        for name in tg_channel._THIRD_PARTY_LOGGERS:
            assert not [f for f in logging.getLogger(name).filters
                        if isinstance(f, tg_channel._MaskingFilter)], "stop() 之後 %s 的 filter 沒拆" % name
    # 鑑別力：沒有 start()（沒掛 filter）時，同樣的紀錄就會洩漏
    with harness() as h:
        _emit_urllib3_style_records()
        assert find_leaks(h.logs.lines)


def test_masker_handles_encoded_and_truncated_forms():
    m = tg_channel.SecretMasker((FAKE_TOKEN, FAKE_CHAT))
    samples = [
        "url: /bot%s/sendMessage" % FAKE_TOKEN,
        "encoded " + FAKE_TOKEN.replace(":", "%3A"),
        "encoded lower " + FAKE_TOKEN.replace(":", "%3a"),
        "truncated ...%s" % FAKE_TOKEN[:20],
        "tail %s!" % FAKE_TOKEN[-9:],
        "chat=%s" % FAKE_CHAT,
    ]
    out = [m(x) for x in samples]
    assert not find_leaks(out), out
    assert all(MASK in x for x in out), out
    assert m("完全無關的訊息 BTCUSDT 12345") == "完全無關的訊息 BTCUSDT 12345"
    assert out[0] == "url: /bot%s/sendMessage" % MASK
    assert repr(m) == "<SecretMasker>"


# ============================== AC-7 ==============================
def test_ac7_missing_secrets_fail_at_start_without_values():
    with harness(token=None, chat=None) as h:
        s = h.sender()
        try:
            s.start()
        except config.MissingSecretError as e:
            caught = e
        else:
            raise AssertionError("沒設密鑰卻沒拋 MissingSecretError")
        assert config.TG_BOT_TOKEN_ENV in str(caught), str(caught)
        assert caught.__cause__ is None and caught.__context__ is None
        st = s.stats()
        assert st["state"] == "new" and not st["worker_alive"], st
        assert not _worker_threads()
        try:
            s.send("x")
        except RuntimeError:
            pass
        else:
            raise AssertionError("start 失敗後 send() 應該拋 RuntimeError")

    with harness(token=FAKE_TOKEN, chat=None) as h:
        s = h.sender()
        try:
            s.start()
        except config.MissingSecretError as e:
            caught = e
        else:
            raise AssertionError("沒設 channel id 卻沒拋 MissingSecretError")
        assert config.TG_CHANNEL_ID_ENV in str(caught)
        assert not find_leaks(exception_texts(caught)), "錯誤訊息帶出了 token"
        assert not _worker_threads()
        # 補上之後同一個發送器可以再 start（harness 結束時會還原環境變數）
        os.environ[config.TG_CHANNEL_ID_ENV] = FAKE_CHAT
        s.start()
        s.send("補上之後", key="k")
        stop_within(s, 60)
        assert h.tg.texts() == ["補上之後"]

    with harness(token="   ", chat=FAKE_CHAT) as h:
        try:
            h.sender().start()
        except config.MissingSecretError:
            pass
        else:
            raise AssertionError("空白的 token 應該等於沒設")


def test_ac7_start_reads_secrets_via_require_secret():
    seen = []
    original = config.require_secret

    def spy(name):
        seen.append(name)
        return original(name)

    config.require_secret = spy
    try:
        with harness() as h:
            s = h.started()
            stop_within(s, 60)
    finally:
        config.require_secret = original
    assert seen == [config.TG_BOT_TOKEN_ENV, config.TG_CHANNEL_ID_ENV], seen


# ============================== FR-2：send() 的邊界 ==============================
def test_send_rejects_over_limit_by_utf16_units_and_never_splits():
    limit = config.TG_MAX_MESSAGE_CHARS
    emoji = chr(0x1F534)                       # BMP 以外：1 個 code point、2 個 UTF-16 code units
    ok_ascii = "a" * limit
    too_long_ascii = "a" * (limit + 1)
    too_long_utf16 = "a" * (limit - 1) + emoji  # len() == limit，但 UTF-16 是 limit + 1
    ok_emoji = emoji * (limit // 2)            # 剛好 limit 個 code units
    assert len(too_long_utf16) == limit and tg_channel.utf16_units(too_long_utf16) == limit + 1
    with harness() as h:
        s = h.started()
        results = [s.send(ok_ascii, key="ok-ascii"), s.send(too_long_ascii, key="long-ascii"),
                   s.send(too_long_utf16, key="long-utf16"), s.send(ok_emoji, key="ok-emoji")]
        stop_within(s, 3600)
        assert results == [True, False, False, True], results
        assert h.tg.texts() == [ok_ascii, ok_emoji], "拒收的不可以被切段送出"
        st = s.stats()
        assert (st["sent"], st["rejected"]) == (2, 2), st
        errors = h.logs.at(logging.ERROR)
        assert any("long-ascii" in e and str(limit + 1) in e for e in errors), errors
        assert any("long-utf16" in e and "UTF-16" in e for e in errors), errors


def test_send_rejects_empty_and_non_str():
    with harness() as h:
        s = h.started()
        assert s.send("", key="empty") is False
        assert s.send(" \n\t ", key="blank") is False
        for bad in (None, b"bytes", 123):
            try:
                s.send(bad)
            except TypeError:
                pass
            else:
                raise AssertionError("send(%r) 應該拋 TypeError" % (bad,))
        stop_within(s, 60)
        assert h.tg.calls == []
        assert s.stats()["rejected"] == 2


def test_send_before_start_raises():
    with harness() as h:
        s = h.sender()
        try:
            s.send("x")
        except RuntimeError:
            pass
        else:
            raise AssertionError("還沒 start 就 send 應該拋 RuntimeError")


# ============================== FR-4：生命週期 ==============================
def test_start_twice_and_restart_after_stop_raise():
    with harness() as h:
        s = h.started()
        for _ in range(2):
            try:
                s.start()
            except RuntimeError:
                pass
            else:
                raise AssertionError("第二次 start() 應該拋 RuntimeError")
            if s.stats()["state"] == "running":
                stop_within(s, 60)
        assert s.stats()["state"] == "stopped"


def test_stop_on_idle_twice_and_never_started():
    with harness() as h:
        assert h.sender().stop(5) == 0            # 從沒 start：no-op
        s = h.started()
        assert stop_within(s, 5) == 0
        assert stop_within(s, 5) == 0
        assert s.send("late", key="late") is False
        st = s.stats()
        assert (st["state"], st["rejected"], st["worker_alive"]) == ("stopped", 1, False), st
        assert any("已停止" in e and "late" in e for e in h.logs.at(logging.ERROR))


def test_stop_timeout_abandons_the_rest_and_reports_count():
    with harness() as h:
        h.tg.gate = threading.Event()
        s = h.started(max_per_minute=20, min_interval=1.0)
        for i in range(30):
            s.send("m%02d" % i, key="m%02d" % i)
        assert h.tg.entered.wait(5)
        deadline = h.clock.now() + 30
        box = {}
        stopper = threading.Thread(target=lambda: box.setdefault("r", s.stop(30)), daemon=True)
        stopper.start()
        wait_until(lambda: s.stats()["state"] == "stopping", "stop() 設好期限")
        h.tg.gate.set()
        stopper.join(10)
        assert not stopper.is_alive(), "stop() 沒有在期限後返回"
        # 期限 30 秒：每秒一則送出 20 則後視窗滿了，下一則最早要到第 60 秒 → 放棄剩下 10 則
        assert box["r"] == 10, box
        # 知道等不到期限內就不等：工作執行緒的時間不可以越過期限（否則真實環境會白等到第 60 秒）
        assert h.clock.now() <= deadline, (h.clock.now(), deadline)
        assert h.tg.texts() == ["m%02d" % i for i in range(20)]
        st = s.stats()
        assert (st["sent"], st["abandoned"], st["queued"], st["state"]) == (20, 10, 0, "stopped"), st
        assert any("放棄 10 則" in e for e in h.logs.at(logging.ERROR)), h.logs.at(logging.ERROR)
        assert stop_within(s, 30) == 10            # 再 stop 一次回傳同一個數字
        assert s.send("late") is False


def test_stop_wakes_the_worker_out_of_a_long_429_wait():
    """預設的等待（可被 stop 叫醒的 Event.wait）+ 不會自己走的假時鐘：retry_after 很長時
    stop(30) 必須馬上叫醒工作執行緒、放棄這一則，不可以等完 retry_after，也不可以提早重送。

    T2 FR-6 S4 修改：原本用 retry_after=600，但 S4 起超過 TG_MAX_RETRY_AFTER_SECONDS（300）就直接放棄、不等，
    這條測的「長時間等待中被 stop 叫醒」就走不到了。改用上限本身（仍會等滿的最長 retry_after），測的事情不變。"""
    with harness() as h:
        h.tg.plan(too_many(config.TG_MAX_RETRY_AFTER_SECONDS))
        s = h.sender(wait=None)
        entered = threading.Event()
        real_wait = s._wait

        def spy(seconds):
            entered.set()
            return real_wait(seconds)

        s._wait = spy
        s.start()
        s.send("B", key="B")
        s.send("C", key="C")
        assert entered.wait(5), "工作執行緒沒有進入 429 的等待"
        t0 = time.monotonic()
        assert stop_within(s, 30) == 2
        assert time.monotonic() - t0 < 5, "stop() 沒有叫醒等待中的工作執行緒"
        assert h.tg.texts() == ["B"], "429 的等待還沒到就重送了"


def test_worker_survives_unexpected_exception_during_delivery():
    with harness() as h:
        h.tg.plan(unexpected, ok())
        s = h.started()
        s.send("A", key="sig-A")
        s.send("B", key="sig-B")
        stop_within(s, 3600)
        assert h.tg.texts() == ["A", "B"]
        st = s.stats()
        assert (st["sent"], st["failed"]) == (1, 1), st
        errors = "\n".join(h.logs.at(logging.ERROR))
        assert "Traceback" in errors and "RuntimeError" in errors and "sig-A" in errors, errors
        assert "繼續運作" in errors
        assert not find_leaks(h.logs.lines + [repr(st)])


class _MainLoopBlowsUpOnce(ChannelSender):
    blew = False

    def _process_next(self):
        if not self.blew:
            self.blew = True
            raise RuntimeError("主迴圈炸了，網址 " + self._url)
        return super()._process_next()


def test_worker_survives_exception_in_its_main_loop():
    with harness() as h:
        s = h.started(_MainLoopBlowsUpOnce)
        wait_until(lambda: any("主迴圈" in e for e in h.logs.at(logging.ERROR)), "主迴圈的例外被記錄")
        assert s.stats()["worker_alive"]
        s.send("A", key="A")
        stop_within(s, 3600)
        assert h.tg.texts() == ["A"] and s.stats()["sent"] == 1
        assert not find_leaks(h.logs.lines)


def test_stats_fields_and_repr():
    with harness() as h:
        s = h.started()
        st = s.stats()
        assert set(st) == {"state", "sent", "failed", "rejected", "abandoned", "queued", "in_flight",
                           "last_success_at", "last_message_id", "last_error", "last_error_at",
                           "worker_alive"}, st
        assert st["state"] == "running" and st["worker_alive"] and st["last_success_at"] is None
        stop_within(s, 60)
        assert repr(s).startswith("<ChannelSender state=stopped")
        assert not find_leaks([repr(s), str(s), repr(s.stats())])


# ============================== AC-8 / AC-9：命令列與設定 ==============================
def _child_env(**overrides):
    env = dict(os.environ)
    # 所有密鑰（含 A4 的維運 chat id）都拿掉：父行程有設的話，子行程的「未設定」判斷才不會被它影響
    for name in config.SECRET_ENV_VARS:
        env.pop(name, None)
    env.pop("PYTHONIOENCODING", None)
    env.pop("PYTHONUTF8", None)
    env.update(overrides)
    return env


def test_ac8_smoke_without_secrets_reports_not_tested_offline():
    """缺密鑰時 --smoke 回報未實測（exit 3），不連網、不建日誌目錄；不帶 --smoke 什麼都不送。"""
    log_dir = os.path.dirname(config.LOG_FILE)
    before = os.path.exists(log_dir)
    code = textwrap.dedent("""
        import socket, sys
        sys.path.insert(0, %r)
        def _blocked(*a, **k):
            raise OSError("offline test: network blocked")
        socket.socket.connect = _blocked
        socket.create_connection = _blocked
        socket.getaddrinfo = _blocked
        from live import tg_channel
        sys.exit(tg_channel.main(sys.argv[1:]))
    """ % REPO_ROOT)
    r = subprocess.run([sys.executable, "-c", code, "--smoke"], cwd=REPO_ROOT, env=_child_env(),
                       stdin=subprocess.DEVNULL, capture_output=True, timeout=120)
    out = r.stdout.decode("utf-8", "replace")
    assert r.returncode == tg_channel.EXIT_NOT_TESTED, (r.returncode, out, r.stderr[-1000:])
    assert "未實測" in out and config.TG_BOT_TOKEN_ENV in out, out
    assert os.path.exists(log_dir) == before, "缺密鑰的冒煙不該建日誌目錄"
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, env=_child_env(
        **{config.TG_BOT_TOKEN_ENV: FAKE_TOKEN, config.TG_CHANNEL_ID_ENV: FAKE_CHAT}),
        stdin=subprocess.DEVNULL, capture_output=True, timeout=120)
    assert r.returncode == 2, "沒帶 --smoke 應該是用法錯誤（argparse exit 2）"
    assert not find_leaks([r.stdout.decode("utf-8", "replace"), r.stderr.decode("utf-8", "replace")])


def test_ac9_execution_params_include_every_tg_param():
    names = [n for n in dir(config) if n.startswith("TG_") and not n.endswith("_ENV")]
    assert len(names) >= 10, names
    params = config.execution_params()
    for n in names:
        assert params[n] == getattr(config, n), n
    assert config.TG_MAX_MESSAGES_PER_MINUTE == 20, "PRD 指定的預設值"
    assert config.TG_MAX_MESSAGE_CHARS == 4096
    # 既有的密鑰常數沒被動到，而且密鑰不在執行參數裡
    assert config.TG_BOT_TOKEN_ENV == "CRYPTO_TRADER_TG_BOT_TOKEN"
    assert config.TG_CHANNEL_ID_ENV == "CRYPTO_TRADER_TG_CHANNEL_ID"
    # A4（TASK-118）新增維運告警私人聊天的 chat id，也是密鑰（同樣不在執行參數裡）
    assert set(config.SECRET_ENV_VARS) == set(SECRET_ENVS) | {config.OPS_CHAT_ID_ENV}
    assert not any(k.endswith("_ENV") or "TOKEN" in k for k in params), params


def test_ac9_sender_defaults_come_from_config_at_construction():
    with harness() as h:
        s = ChannelSender()
        assert s._max_per_minute == config.TG_MAX_MESSAGES_PER_MINUTE
        assert s._min_interval == config.TG_MIN_INTERVAL_SECONDS
        assert s._max_attempts == config.TG_SEND_MAX_ATTEMPTS
        assert s._backoff_base == config.TG_RETRY_BACKOFF_BASE_SECONDS
        assert s._backoff_max == config.TG_RETRY_BACKOFF_MAX_SECONDS
        assert s._max_rate_limited_retries == config.TG_MAX_RATE_LIMITED_RETRIES
        assert s._retry_after_fallback == config.TG_RETRY_AFTER_FALLBACK_SECONDS
        assert s._http_timeout == config.TG_HTTP_TIMEOUT_SECONDS
        assert s._max_chars == config.TG_MAX_MESSAGE_CHARS
        assert s._stop_timeout == config.TG_STOP_TIMEOUT_SECONDS
        assert s._api_base_url == config.TG_API_BASE_URL
        saved = config.TG_SEND_MAX_ATTEMPTS
        config.TG_SEND_MAX_ATTEMPTS = saved + 7
        try:
            assert ChannelSender()._max_attempts == saved + 7, "應該在建構當下讀 config"
        finally:
            config.TG_SEND_MAX_ATTEMPTS = saved
        assert h.senders == []


def test_ac9_python_m_live_lists_tg_params_and_hides_secrets():
    for with_secrets in (True, False):
        overrides = {config.TG_BOT_TOKEN_ENV: FAKE_TOKEN, config.TG_CHANNEL_ID_ENV: FAKE_CHAT} \
            if with_secrets else {}
        r = subprocess.run([sys.executable, "-m", "live"], cwd=REPO_ROOT, env=_child_env(**overrides),
                           stdin=subprocess.DEVNULL, capture_output=True, timeout=120)
        out = r.stdout.decode("utf-8", "replace")
        err = r.stderr.decode("utf-8", "replace")
        assert r.returncode == 0, (r.returncode, out[-2000:], err[-2000:])
        for name in config.execution_params():
            if name.startswith("TG_"):
                assert name in out, "python -m live 沒列出 %s" % name
        assert ("已設定" in out) == with_secrets, out[-800:]
        assert not find_leaks([out, err]), "python -m live 印出了密鑰片段"


def test_ac9_module_name_and_dependencies():
    name = "tg_channel"
    assert name not in sys.stdlib_module_names
    assert name not in {"telegram", "telebot", "aiogram", "pyrogram", "telethon", "tg"}
    path = os.path.join(REPO_ROOT, "live", name + ".py")
    with open(path, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    top = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            top.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            top.add(node.module.split(".")[0])
    extra = top - set(sys.stdlib_module_names) - {"requests", "live"}
    assert not extra, "live/tg_channel.py 用了標準庫與 requests 以外的套件：%s" % extra
    assert not any(n.startswith("pionex_") or n == "research" for n in top), top
    live_names = [f[:-3] for f in os.listdir(os.path.join(REPO_ROOT, "live")) if f.endswith(".py")]
    assert "telegram" not in live_names


# ============================== T2（TASK-018）：FR-6 小修、on_done、expires_at ==============================
# 以下全部是 T2 新增的測試（只增不改）。舊的測試不給 on_done / expires_at，照常全過 = 向下相容（AC-6）。
import io as _io  # noqa: E402 —— T2 新增（只增不改：不動檔頭的 import）


class ResultBox:
    """on_done 的收集器：key -> [SendResult, ...]（用來斷言「恰好一次」）。執行緒安全。"""

    def __init__(self):
        self._lock = threading.Lock()
        self.by_key = collections.defaultdict(list)
        self.threads = []

    def __call__(self, result):
        with self._lock:
            self.by_key[result.key].append(result)
            self.threads.append(threading.current_thread().name)

    def one(self, key):
        with self._lock:
            got = list(self.by_key.get(key, ()))
        assert len(got) == 1, "key=%r 的 on_done 應該恰好一次，實際 %d 次：%r" % (key, len(got), got)
        return got[0]


def connect_timeout(url):
    return requests.exceptions.ConnectTimeout(
        "HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: "
        + url[len(config.TG_API_BASE_URL):] + " (Caused by ConnectTimeoutError(...))")


def test_t2_s4_retry_after_over_cap_gives_up_without_waiting():
    """S4：retry_after = 上限 + 1 → 不等、放棄這一則（ERROR），換下一則。（不用 on_done：舊版也跑得動這條，
    拿來證明修改前的程式碼在 301 時會等。on_done 的 gave_up 回報在 test_t2_on_done_reports_every_outcome_exactly_once。）"""
    cap = config.TG_MAX_RETRY_AFTER_SECONDS
    assert cap == 300, "PRD 指定的預設值"
    with harness() as h:
        h.tg.plan(too_many(cap + 1), ok(7))
        s = h.started()
        s.send("A", key="sig-A")
        s.send("B", key="sig-B")
        stop_within(s, 3600)
        assert not any(w >= cap for w in h.clock.waits), "超過上限還是等了：%r" % h.clock.waits
        assert h.tg.texts() == ["A", "B"], h.tg.texts()
        assert h.tg.times()[1] - h.tg.times()[0] == config.TG_MIN_INTERVAL_SECONDS
        errors = h.logs.at(logging.ERROR)
        assert any("sig-A" in e and str(cap + 1) in e and "上限" in e for e in errors), errors
        st = s.stats()
        assert (st["sent"], st["failed"]) == (1, 1), st


def test_t2_s4_retry_after_at_cap_still_waits_exactly():
    """S4：上限以內照舊等滿（= 300 → 恰好等 300 秒才重送同一則）。"""
    cap = config.TG_MAX_RETRY_AFTER_SECONDS
    with harness() as h:
        h.tg.plan(too_many(cap), ok())
        s = h.started()
        s.send("A", key="A")
        stop_within(s, 3600)
        t = h.tg.times()
        assert h.tg.texts() == ["A", "A"] and t[1] - t[0] == cap, t
        assert sum(h.clock.waits) == cap, h.clock.waits
        assert s.stats()["sent"] == 1


def test_t2_s4_cap_is_injectable_and_validated():
    with harness() as h:
        h.tg.plan(too_many(50), ok())
        s = h.started(max_retry_after=40)
        s.send("A", key="A")
        stop_within(s, 3600)
        assert h.tg.texts() == ["A"] and s.stats()["failed"] == 1
        for bad in (0, -1):
            try:
                ChannelSender(max_retry_after=bad)
            except ValueError:
                pass
            else:
                raise AssertionError("max_retry_after=%r 應該拒絕" % bad)


def _emit_broken_format_record(err):
    """兩個 %s 只給一個參數：getMessage() 會拋 TypeError；參數本身是帶 token 的網址。

    urllib3 的 logger 上暫時掛一個標準的 StreamHandler（跟 logsetup 的終端機 / 檔案 handler 一樣，格式化失敗時
    走 Handler.handleError，把 msg 與 args 印到 sys.stderr）。紀錄接著往上傳到根 logger 的 LogCapture，
    它不接格式化例外，TypeError 會一路拋回這裡 —— 那是測試工具本身的行為，接住即可。
    回傳 (stderr 內容, 標準 handler 寫出的內容)。"""
    out = _io.StringIO()
    handler = logging.StreamHandler(out)
    lg = logging.getLogger("urllib3.connectionpool")
    lg.addHandler(handler)
    try:
        with contextlib.redirect_stderr(err):
            try:
                lg.warning("Retrying %s after %s", EXPECTED_URL)
            except TypeError:
                pass
    finally:
        lg.removeHandler(handler)
    return err.getvalue(), out.getvalue()


def test_t2_s1_bad_format_record_is_masked_not_dumped_to_stderr():
    """S1：格式字串與參數不符的第三方紀錄，stderr（logging 的 handleError）與日誌都不可以出現 token 片段。"""
    with harness() as h:
        s = h.started()
        stderr, written = _emit_broken_format_record(_io.StringIO())
        stop_within(s, 60)
        lines = [line for line in h.logs.lines if " urllib3." in line]
        assert len(lines) == 1 and MASK in lines[0] and "格式化失敗" in lines[0], lines
        assert MASK in written and "格式化失敗" in written, written
        assert "--- Logging error ---" not in stderr, stderr[-500:]
        assert not find_leaks([stderr, written] + h.logs.lines), (stderr[-500:], written, lines)
    # 鑑別力前提：沒有掛 filter 時，同一筆紀錄會讓 logging 把原始參數（含 token）印到 stderr
    with harness() as h:
        stderr, _ = _emit_broken_format_record(_io.StringIO())
        leaked = "--- Logging error ---" in stderr and find_leaks([stderr])
        assert leaked, "前提不成立：沒有 filter 時 stderr 應該看得到 token：%r" % stderr[-500:]


def test_t2_s2_stuck_worker_tears_down_itself_and_stop_is_idempotent():
    """S2：stop() 等不到卡在 HTTP 請求裡的工作執行緒 → 先返回；執行緒日後結束時自己拆 filter、關 Session；
    兩次 stop() 回傳同一個數字。（不用 on_done：舊版也跑得動這條，拿來證明修改前 filter 會殘留、回傳值不一致。）"""
    saved = tg_channel._JOIN_GRACE_SECONDS
    tg_channel._JOIN_GRACE_SECONDS = 0.2
    try:
        with harness() as h:
            h.tg.gate = threading.Event()
            s = h.started(http_timeout=0)
            for k in ("A", "B", "C"):
                s.send(k, key=k)
            assert h.tg.entered.wait(5), "工作執行緒沒有送出第一則"
            first = stop_within(s, 0)
            assert first == 3, first                       # 卡住的 A + 佇列裡的 B、C
            assert s.stats()["worker_alive"], "前提：工作執行緒應該還卡在請求裡"
            h.tg.gate.set()
            wait_until(lambda: not s.stats()["worker_alive"], "卡住的工作執行緒結束")
            for name in tg_channel._THIRD_PARTY_LOGGERS:
                assert not [f for f in logging.getLogger(name).filters
                            if isinstance(f, tg_channel._MaskingFilter)], "等不到的執行緒結束後 %s 的 filter 殘留" % name
            assert s._session is None and s._log_filter is None
            assert stop_within(s, 0) == first, "第二次 stop() 回傳值要跟第一次一樣"
            assert s.stats()["sent"] == 1                  # A 其實送成功了：stats 照實記
    finally:
        tg_channel._JOIN_GRACE_SECONDS = saved


def test_t2_s2_stuck_worker_reports_via_on_done():
    """同上情境的 on_done：佇列裡被放棄的回報 abandoned（在呼叫 stop() 的執行緒）、卡住的那一則之後照實回報。"""
    saved = tg_channel._JOIN_GRACE_SECONDS
    tg_channel._JOIN_GRACE_SECONDS = 0.2
    try:
        with harness() as h:
            h.tg.gate = threading.Event()
            box = ResultBox()
            s = h.started(http_timeout=0)
            for k in ("A", "B", "C"):
                s.send(k, key=k, on_done=box)
            assert h.tg.entered.wait(5)
            assert stop_within(s, 0) == 3
            assert box.one("B").status == tg_channel.RESULT_ABANDONED
            assert box.one("C").status == tg_channel.RESULT_ABANDONED
            assert "A" not in box.by_key, "卡住的那一則還沒有結果，不可以先回報"
            h.tg.gate.set()
            wait_until(lambda: not s.stats()["worker_alive"], "卡住的工作執行緒結束")
            assert box.one("A").status == tg_channel.RESULT_DELIVERED
    finally:
        tg_channel._JOIN_GRACE_SECONDS = saved


def test_t2_on_done_reports_every_outcome_exactly_once():
    n = config.TG_SEND_MAX_ATTEMPTS
    with harness() as h:
        h.tg.plan(
            ok(101),                                                  # A 送達
            http_error(400, "Bad Request"),                           # B 永久失敗，確定沒送達
            *[read_timeout] * n,                                      # C 重試用盡：不確定
            *[too_many(1)] * (config.TG_MAX_RATE_LIMITED_RETRIES + 1),  # D 429 超過重送上限：確定沒送達
            FakeResponse(200, {"ok": False, "description": "odd"}),   # E 永久失敗，但可能已送出
            *[connect_timeout] * n,                                   # F 連線逾時用盡：確定沒送達
            http_error(503, "x"), ok(102),                            # G 5xx 之後送達：uncertain 但 delivered
            too_many(config.TG_MAX_RETRY_AFTER_SECONDS + 1),          # H retry_after 超過上限（S4）：確定沒送達
        )
        box = ResultBox()
        s = h.started()
        for k in "ABCDEFGH":
            s.send("msg " + k, key=k, on_done=box)
        stop_within(s, 3600)
        want = {"A": (tg_channel.RESULT_DELIVERED, False), "B": (tg_channel.RESULT_FAILED, False),
                "C": (tg_channel.RESULT_GAVE_UP, True), "D": (tg_channel.RESULT_GAVE_UP, False),
                "E": (tg_channel.RESULT_FAILED, True), "F": (tg_channel.RESULT_GAVE_UP, False),
                "G": (tg_channel.RESULT_DELIVERED, True), "H": (tg_channel.RESULT_GAVE_UP, False)}
        for k, (status, uncertain) in want.items():
            r = box.one(k)
            assert (r.status, r.uncertain) == (status, uncertain), (k, r)
            assert r.key == k
        assert box.one("A").message_id == 101 and box.one("G").message_id == 102
        assert box.one("B").detail and "HTTP 400" in box.one("B").detail
        assert set(box.threads) == {"tg-channel-sender"}, box.threads
        st = s.stats()
        assert (st["sent"], st["failed"]) == (2, 6), st
        assert not find_leaks([repr(r) for rs in box.by_key.values() for r in rs])


def test_t2_expires_at_is_checked_before_every_request():
    wall = FakeClock.EPOCH + FakeClock().now()
    # (a) 交給發送器時已過期：一次請求都不發
    with harness() as h:
        box = ResultBox()
        s = h.started()
        s.send("A", key="A", on_done=box, expires_at=wall - 1)
        stop_within(s, 3600)
        assert h.tg.calls == [] and box.one("A").status == tg_channel.RESULT_EXPIRED
        assert box.one("A").uncertain is False
        st = s.stats()
        assert (st["sent"], st["failed"], st["abandoned"]) == (0, 0, 0), st
    # (b) 429 的等待途中過期 → 等完之後不重送，回報 expired
    with harness() as h:
        h.tg.plan(too_many(20))
        box = ResultBox()
        s = h.started()
        s.send("A", key="A", on_done=box, expires_at=wall + 10)
        stop_within(s, 3600)
        assert h.tg.texts() == ["A"] and box.one("A").status == tg_channel.RESULT_EXPIRED
    # (c) 限速（相鄰間隔）的等待也算：第二則要等 1 秒，期限只剩 0.5 秒 → 不送
    with harness() as h:
        box = ResultBox()
        s = h.started()
        s.send("X", key="X", on_done=box)
        s.send("Y", key="Y", on_done=box, expires_at=wall + 0.5)
        stop_within(s, 3600)
        assert h.tg.texts() == ["X"] and box.one("Y").status == tg_channel.RESULT_EXPIRED
    # (d) 第一次讀取逾時（不確定）之後才過期 → 照送，回報 delivered 且 uncertain
    with harness() as h:
        h.tg.plan(read_timeout, ok(9))
        box = ResultBox()
        s = h.started()
        s.send("A", key="A", on_done=box, expires_at=wall + 1)      # 退避 2 秒之後已過期
        stop_within(s, 3600)
        t = h.tg.times()
        assert h.tg.texts() == ["A", "A"] and t[1] > t[0] + 1, t
        r = box.one("A")
        assert (r.status, r.uncertain, r.message_id) == (tg_channel.RESULT_DELIVERED, True, 9), r
    # (e) 第一次是連線逾時（確定沒送達）→ 過期後不再送
    with harness() as h:
        h.tg.plan(connect_timeout, ok())
        box = ResultBox()
        s = h.started()
        s.send("A", key="A", on_done=box, expires_at=wall + 1)
        stop_within(s, 3600)
        assert h.tg.texts() == ["A"] and box.one("A").status == tg_channel.RESULT_EXPIRED
    # (f) 邊界：剛好等於期限照送（「超過」才不送）
    with harness() as h:
        box = ResultBox()
        s = h.started()
        s.send("A", key="A", on_done=box, expires_at=wall)
        stop_within(s, 3600)
        assert box.one("A").status == tg_channel.RESULT_DELIVERED


def test_t2_stop_deadline_abandons_and_reports_the_rest():
    """stop() 的期限到了：放棄的每一則（含佇列裡還沒輪到的）都回報 abandoned，送出的回報 delivered。"""
    with harness() as h:
        h.tg.gate = threading.Event()
        box = ResultBox()
        s = h.started(max_per_minute=20, min_interval=1.0)
        for i in range(25):
            s.send("m%02d" % i, key="m%02d" % i, on_done=box)
        assert h.tg.entered.wait(5)
        result = {}
        stopper = threading.Thread(target=lambda: result.setdefault("r", s.stop(30)), daemon=True)
        stopper.start()
        wait_until(lambda: s.stats()["state"] == "stopping", "stop() 設好期限")
        h.tg.gate.set()
        stopper.join(10)
        assert not stopper.is_alive() and result["r"] == 5, result
        statuses = collections.Counter(box.one("m%02d" % i).status for i in range(25))
        assert statuses == {tg_channel.RESULT_DELIVERED: 20, tg_channel.RESULT_ABANDONED: 5}, statuses


def test_t2_on_done_exception_is_logged_and_worker_keeps_going():
    def boom(result):
        raise RuntimeError("callback 炸了 " + EXPECTED_URL)

    with harness() as h:
        box = ResultBox()
        s = h.started()
        s.send("A", key="A", on_done=boom)
        s.send("B", key="B", on_done=box)
        stop_within(s, 3600)
        assert h.tg.texts() == ["A", "B"] and box.one("B").status == tg_channel.RESULT_DELIVERED
        errors = "\n".join(h.logs.at(logging.ERROR))
        assert "on_done" in errors and "RuntimeError" in errors, errors
        assert not find_leaks(h.logs.lines)
        assert s.stats()["sent"] == 2


def test_t2_send_rejects_bad_on_done_and_expires_at():
    with harness() as h:
        s = h.started()
        for kwargs, exc in (({"on_done": 1}, TypeError), ({"expires_at": True}, TypeError),
                            ({"expires_at": "soon"}, TypeError), ({"expires_at": float("nan")}, ValueError),
                            ({"expires_at": float("inf")}, ValueError)):
            try:
                s.send("x", key="k", **kwargs)
            except exc:
                pass
            else:
                raise AssertionError("send(%r) 應該拋 %s" % (kwargs, exc.__name__))
        box = ResultBox()
        assert s.send("", key="empty", on_done=box) is False       # 拒收的不回報
        stop_within(s, 60)
        assert h.tg.calls == [] and not box.by_key


# ============================== T2 第 1 輪：F2（DNS 失敗 / 連線被拒）、F1（SSL） ==============================
# 例外照 requests 2.34 / urllib3 2.7 的實際包裝方式組（requests 預設 Retry(0, read=False)）：
#   連線階段  urllib3 _new_conn() 拋 NewConnectionError / NameResolutionError → Retry.increment 視為 connect error →
#             MaxRetryError(reason=...) from reason → requests 包成 ConnectionError(MaxRetryError)（__context__ 同一個）
#   讀回應    urlopen 把 OSError / HTTPException 包成 ProtocolError("Connection aborted.", ...)，read=False 原樣往外拋 →
#             requests 包成 ConnectionError(ProtocolError)
#   SSL       urlopen 把 ssl 例外包成 urllib3 SSLError(原始例外) → MaxRetryError(reason=SSLError) → requests SSLError
import http.client as _http_client  # noqa: E402 —— T2 第 1 輪新增
import socket as _socket  # noqa: E402
import ssl as _ssl  # noqa: E402

from urllib3.exceptions import NameResolutionError as _NameResolutionError  # noqa: E402
from urllib3.exceptions import ProtocolError as _ProtocolError  # noqa: E402
from urllib3.exceptions import SSLError as _Urllib3SSLError  # noqa: E402


def _requests_wrap(outer_cls, inner):
    """在 except 區塊裡 raise outer_cls(inner)，讓 __context__ 跟 requests 真的拋出來時一樣。"""
    try:
        try:
            raise inner
        except type(inner) as caught:
            raise outer_cls(caught)
    except outer_cls as outer:
        return outer


def _max_retry(url, reason):
    try:
        raise MaxRetryError(HTTPSConnectionPool("api.telegram.org", 443), url[len(config.TG_API_BASE_URL):],
                            reason) from reason
    except MaxRetryError as e:
        return e


def dns_error(url):
    try:
        raise _NameResolutionError("api.telegram.org", None, _socket.gaierror(11001, "getaddrinfo failed"))
    except _NameResolutionError as reason:
        return _requests_wrap(requests.exceptions.ConnectionError, _max_retry(url, reason))


def remote_disconnected(url):
    reason = _ProtocolError("Connection aborted.",
                            _http_client.RemoteDisconnected("Remote end closed connection without response"))
    return _requests_wrap(requests.exceptions.ConnectionError, reason)


def connection_reset(url):
    reason = _ProtocolError("Connection aborted.", ConnectionResetError(10054, "遠端主機已強制關閉一個現存的連線"))
    return _requests_wrap(requests.exceptions.ConnectionError, reason)


def ssl_cert_error(url):
    cause = _ssl.SSLCertVerificationError(1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: "
                                             "unable to get local issuer certificate")
    return _requests_wrap(requests.exceptions.SSLError, _max_retry(url, _Urllib3SSLError(cause)))


def ssl_read_error(url):
    cause = _ssl.SSLError(1, "[SSL: DECRYPTION_FAILED_OR_BAD_RECORD_MAC] decryption failed or bad record mac")
    return _requests_wrap(requests.exceptions.SSLError, _max_retry(url, _Urllib3SSLError(cause)))


def test_t2_f2_dns_failure_and_refused_connection_are_certain_not_delivered():
    """DNS 失敗 / 連線被拒：請求沒有離開本機 → 確定沒送達，期限照常檢查：退避之後已過期 → expired、不再送。"""
    wall = FakeClock.EPOCH + FakeClock().now()
    for failure in (dns_error, conn_error):
        with harness() as h:
            h.tg.plan(failure, ok())
            box = ResultBox()
            s = h.started()
            s.send("A", key="A", on_done=box, expires_at=wall + 1)     # 退避 2 秒之後已過期
            stop_within(s, 3600)
            r = box.one("A")
            assert (r.status, r.uncertain) == (tg_channel.RESULT_EXPIRED, False), (failure.__name__, r)
            assert len(h.tg.calls) == 1, failure.__name__
        with harness() as h:                                            # 沒有期限：照常重試到送達，而且不是不確定
            h.tg.plan(failure, ok(5))
            box = ResultBox()
            s = h.started()
            s.send("A", key="A", on_done=box)
            stop_within(s, 3600)
            r = box.one("A")
            assert (r.status, r.uncertain, r.message_id) == (tg_channel.RESULT_DELIVERED, False, 5), r
            assert any("確定沒送達" in w for w in h.logs.at(logging.WARNING)), h.logs.at(logging.WARNING)


def test_t2_f2_other_connection_errors_stay_uncertain():
    """連線中斷 / 被重設（ProtocolError、RemoteDisconnected）：請求可能已到 Telegram → 之後過期也照送。"""
    wall = FakeClock.EPOCH + FakeClock().now()
    for failure in (remote_disconnected, connection_reset, read_timeout):
        with harness() as h:
            h.tg.plan(failure, ok(8))
            box = ResultBox()
            s = h.started()
            s.send("A", key="A", on_done=box, expires_at=wall + 1)
            stop_within(s, 3600)
            r = box.one("A")
            assert (r.status, r.uncertain, r.message_id) == (tg_channel.RESULT_DELIVERED, True, 8), (failure.__name__,
                                                                                                    r)
            assert len(h.tg.calls) == 2


def test_t2_f2_uncertain_first_then_dns_failure_is_still_sent():
    """規則不變：同一次 send 裡先有一次不確定，之後就不再檢查期限 —— 後來的 DNS 失敗不會把它變回「可以判過期」。"""
    wall = FakeClock.EPOCH + FakeClock().now()
    with harness() as h:
        h.tg.plan(read_timeout, dns_error, ok(9))
        box = ResultBox()
        s = h.started()
        s.send("A", key="A", on_done=box, expires_at=wall + 1)
        stop_within(s, 3600)
        r = box.one("A")
        assert (r.status, r.uncertain, r.message_id) == (tg_channel.RESULT_DELIVERED, True, 9), r
        assert len(h.tg.calls) == 3


def test_t2_f2_classification_matches_the_real_requests_stack():
    """用真的 requests（在離線籠子裡）確認例外的實際包裝結構與分類：DNS 出口被擋（OSError）→ NewConnectionError；
    getaddrinfo 拋 socket.gaierror → NameResolutionError。兩者都判「連線沒有建立」，而且期限照常檢查。"""
    from urllib3.exceptions import NewConnectionError as _NewConnectionError
    with env_vars({n: None for n in PROXY_ENVS}), harness(allow_network_attempts=True) as h:
        caught = {}
        try:
            requests.post(EXPECTED_URL, json={"x": 1}, timeout=1)
        except requests.exceptions.ConnectionError as e:
            caught["blocked"] = e
        real_getaddrinfo = socket.getaddrinfo

        def gai_fail(host, port, *a, **k):
            raise _socket.gaierror(11001, "getaddrinfo failed")
        socket.getaddrinfo = gai_fail
        try:
            try:
                requests.post(EXPECTED_URL, json={"x": 1}, timeout=1)
            except requests.exceptions.ConnectionError as e:
                caught["gaierror"] = e
        finally:
            socket.getaddrinfo = real_getaddrinfo
        for name, exc in caught.items():
            assert isinstance(exc.args[0], MaxRetryError), (name, exc.args)
            assert isinstance(exc.args[0].reason, _NewConnectionError), (name, exc.args[0].reason)
            assert tg_channel._connection_never_established(exc), name
            assert not tg_channel._certificate_verification_failed(exc), name
        assert isinstance(caught["gaierror"].args[0].reason, _NameResolutionError)
        # 發送器走真的 requests：第一次失敗之後已過期 → expired，只試了一次
        box = ResultBox()
        s = h.sender(post=None)
        s.start()
        s.send("real-stack", key="real", on_done=box, expires_at=FakeClock.EPOCH + h.clock.now() + 1)
        stop_within(s, 3600)
        assert (box.one("real").status, box.one("real").uncertain) == (tg_channel.RESULT_EXPIRED, False)
        dns = [a for a in h.cage.attempts if a[0] == "socket.getaddrinfo"]
        assert len(dns) == 2, h.cage.attempts                          # 上面手動的一次 + 發送器的一次
        assert not find_leaks(h.logs.lines + [repr(box.by_key)])
    # 分類函式只認 urllib3 的類別（名字相同的其他類別不算）
    fake = type("NewConnectionError", (Exception,), {})
    assert not tg_channel._connection_never_established(fake("not urllib3"))
    assert not tg_channel._connection_never_established(remote_disconnected(EXPECTED_URL))
    assert not tg_channel._connection_never_established(read_timeout(EXPECTED_URL))


def test_t2_f1_only_certificate_verification_ssl_errors_are_certain():
    """F1：SSL 錯誤一律不重試（永久失敗）；憑證驗證失敗（握手階段）才算確定沒送達，其他 SSL 錯誤可能發生在請求送到之後。"""
    cases = ((ssl_cert_error, False, "憑證驗證失敗"), (ssl_read_error, True, "可能其實已經送達"))
    for failure, uncertain, word in cases:
        with harness() as h:
            h.tg.plan(failure, ok())
            box = ResultBox()
            s = h.started()
            s.send("A", key="sig-A", on_done=box)
            s.send("B", key="sig-B", on_done=box)
            stop_within(s, 3600)
            r = box.one("sig-A")
            assert (r.status, r.uncertain) == (tg_channel.RESULT_FAILED, uncertain), (failure.__name__, r)
            assert h.tg.texts() == ["A", "B"], "SSL 錯誤不重試"
            errors = h.logs.at(logging.ERROR)
            assert any("sig-A" in e and "SSLError" in e and word in e for e in errors), (failure.__name__, errors)
            assert not find_leaks(h.logs.lines + [repr(r)])
    assert tg_channel._certificate_verification_failed(ssl_cert_error(EXPECTED_URL))
    assert not tg_channel._certificate_verification_failed(ssl_read_error(EXPECTED_URL))
    assert not tg_channel._certificate_verification_failed(ssl_error(EXPECTED_URL))   # 沒有原因鏈：保守算不確定


# ============================== 不用 pytest 也能跑 ==============================
if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS  {name}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL  {name}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
