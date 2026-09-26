# -*- coding: utf-8 -*-
"""
live.rest_gate — 整個行程共用的 REST 閘門：速率上限、優先順序、429 封鎖冷卻、伺服器時鐘
=========================================================================================
派網公開端點的限制是「單 IP 10 req/s」，而 429 是「封鎖 60 秒，封鎖期間每多打一次再加 10 秒」。
這兩條都是以 IP 計，不是以元件計。所以速率上限與 429 冷卻必須是**一個行程一份**：
A1 的 tickers 輪詢、候選 klines、冷啟動補種子、背景對帳，以及日後 A3 替未平倉名目部位打的
5 分 K / 1 分 K，全部走同一個 RestGate。各元件各自以為自己有 6 req/s 的話，加起來就逼近 10。

用法（任何要打派網 REST 的 live/ 元件）：

    from live import rest_gate
    gate = rest_gate.shared_gate()                      # 行程內唯一一份
    js = gate.get("/api/v1/market/klines",
                  {"symbol": sym, "interval": "5M", "limit": 500},
                  priority=rest_gate.PRIORITY_FOREGROUND)
    gate.server_clock.now_ms()                          # 伺服器時間估計（UTC epoch ms）

    with gate.limiter.foreground_hold():                # 這段期間只放行前景請求
        ...

    RestBanned            封鎖冷卻中（或這個請求剛好收到 429）。呼叫端自己決定要等還是放棄；
                          不要自己 sleep 一下就重打 —— 冷卻結束前閘門一個請求都不會放行。
    live.pionex_api.ApiError
                          其他錯誤（5xx、result=false、連線錯誤…）。閘門不重試，重試是呼叫端的事，
                          而且重試也要再經過閘門，這樣每一個送出去的請求都有被算到。

──────────────────────────────────────────────────────────────────────
速率上限：滑動窗口，不是 token bucket
──────────────────────────────────────────────────────────────────────
要守的是「任何 1 秒窗口內不超過 N 個」。容量 N、每秒補 N 的 token bucket 做不到：t=0 用掉 N 個，
t=1/N 又補回 1 個，[0, 1) 裡就有 N+1 個。這裡的規則是「第 k 個請求必須比第 k-N 個晚至少 1 秒」，
等價於任何 1 秒窗口最多 N 個，同時允許一開始就連發 N 個（收盤後候選要並發，這很重要）。

計的是「閘門放行」的時刻，實際送出晚一點點（同一個執行緒緊接著送）。這個微小抖動加上網路延遲，
在派網那端看到的間隔會略有不同 —— 6 與 10 之間的距離就是為了吸收這種東西。

──────────────────────────────────────────────────────────────────────
優先順序：前景請求不可被背景請求搶走額度
──────────────────────────────────────────────────────────────────────
    PRIORITY_FOREGROUND   K 棒收盤後的 tickers 與候選 klines、例行 tickers 輪詢（時間敏感）
    PRIORITY_NORMAL       冷啟動補種子
    PRIORITY_BACKGROUND   背景對帳
兩道保護：
  1. 同時在等的請求，永遠先放優先順序高的（同級先到先放）。
  2. foreground_hold() 期間，只放行 PRIORITY_FOREGROUND。A1 在 K 棒收盤前 1 秒進入、判定完成後離開：
     收盤那一刻最近 1 秒內沒有背景請求佔著額度，收盤後的前景取數也不會被背景請求插隊。
     只靠第 1 道不夠：背景請求會在前景請求「還沒開始等」的空檔拿走額度（例如 tickers 回來、
     還在算粗篩的那幾毫秒），滑動窗口一被佔，前景請求就得多等將近 1 秒。

──────────────────────────────────────────────────────────────────────
429：整個行程停下
──────────────────────────────────────────────────────────────────────
任何一個請求收到 429，閘門立刻進入封鎖冷卻 REST_BAN_COOLDOWN_SECONDS（預設 70 秒）：
冷卻期間所有 acquire() 直接拋 RestBanned（包括已經在排隊的），**一個請求都不送**。
已經送出去、還在飛的請求無法收回，它們若也收到 429，冷卻從那一刻重新起算。

為什麼不在 live.pionex_api.api_get 裡處理：它碰到 429 會退避後重試（B5 的設計，每小時兩個請求
時沒有問題），閘門一律以 retries=1 呼叫它，429 由這裡接手。

──────────────────────────────────────────────────────────────────────
時鐘
──────────────────────────────────────────────────────────────────────
所有等待都經過 clock 物件（預設 SystemClock），測試注入假時鐘即可全程不真的 sleep。
間隔與冷卻用單調時鐘算（NTP 校時不影響），伺服器時間估計用 wall clock + 偏移。
ServerClock 從每個經過閘門的回應信封 `timestamp`（伺服器時間，ms）估本機時鐘偏移，
所以整個行程共用同一份伺服器時間估計。偏移超過 CLOCK_OFFSET_WARN_MS 記 WARNING。

模組名刻意不叫 ratelimit / time / queue 之類：live/ 不可以有跟標準庫同名的模組。
"""

import heapq
import itertools
import logging
import statistics
import threading
import time
from collections import deque
from contextlib import contextmanager

from live import config, pionex_api

logger = logging.getLogger(__name__)

PRIORITY_FOREGROUND = 0
PRIORITY_NORMAL = 1
PRIORITY_BACKGROUND = 2
PRIORITY_NAMES = {
    PRIORITY_FOREGROUND: "foreground",
    PRIORITY_NORMAL: "normal",
    PRIORITY_BACKGROUND: "background",
}


class RestBanned(Exception):
    """429 封鎖冷卻中：這個請求沒有送出（或它本身就是收到 429 的那一個）。

    remaining_seconds 是拋出當下冷卻還剩幾秒，呼叫端要等的話至少等這麼久。
    """

    def __init__(self, remaining_seconds, path=None):
        self.remaining_seconds = float(remaining_seconds)
        self.path = path
        where = f"{path}: " if path else ""
        super().__init__(f"{where}REST 封鎖冷卻中（收到過 429），還剩 {self.remaining_seconds:.1f} 秒")


class SystemClock:
    """真的時鐘。測試用同樣介面的假時鐘替換（見 tests/）。

    time_ms()    本機 wall clock，UTC epoch 毫秒
    monotonic()  單調時鐘（秒），只拿來算間隔
    sleep(s)     睡 s 秒；s <= 0 不睡
    wait(cond, timeout)
                 在 cond 上等 timeout 秒（None = 等到被 notify）。呼叫時必須持有 cond 的鎖。
    """

    def time_ms(self):
        return time.time_ns() // 1_000_000

    def monotonic(self):
        return time.monotonic()

    def sleep(self, seconds):
        if seconds > 0:
            time.sleep(seconds)

    def wait(self, cond, timeout):
        cond.wait(timeout)


class RestLimiter:
    """滑動窗口速率上限 + 優先順序 + 429 封鎖冷卻。thread-safe。

    rate_per_second  任何 window_seconds 窗口內最多放行幾個請求（>= 1）
    ban_cooldown_seconds
                     trip_ban() 之後完全不放行的秒數
    window_seconds   窗口長度，預設 1 秒；只有測試會改
    """

    def __init__(self, rate_per_second, ban_cooldown_seconds, *, clock=None, window_seconds=1.0):
        rate = int(rate_per_second)
        if rate < 1:
            raise ValueError(f"rate_per_second 必須 >= 1，收到 {rate_per_second!r}")
        self.rate = rate
        self.window = float(window_seconds)
        self.ban_cooldown = float(ban_cooldown_seconds)
        self._clock = clock or SystemClock()
        # RLock：假時鐘會在 wait() 裡同步觸發排程好的事件（例如解除 hold），事件會再進來拿同一把鎖
        self._cond = threading.Condition(threading.RLock())
        self._sent = deque(maxlen=rate)   # 最近 rate 次放行的單調時刻
        self._waiters = []                # heap of (priority, seq)
        self._seq = itertools.count()
        self._holds = 0
        self._ban_until = None            # 單調時刻；None = 從未封鎖
        # 統計（給日誌與測試看）
        self.granted = 0
        self.granted_by_priority = {p: 0 for p in PRIORITY_NAMES}
        self.bans = 0
        self.refused_banned = 0

    # ---------------- 放行 ----------------
    def acquire(self, priority=PRIORITY_FOREGROUND):
        """等到可以送出一個請求為止，然後把這次算進窗口。

        封鎖冷卻中（或等待期間進入冷卻）→ 拋 RestBanned，不會送、不算進窗口。
        沒有逾時：前景請求最多等一個窗口；背景請求在 hold 期間會一直等到 hold 解除。
        """
        if priority not in PRIORITY_NAMES:
            raise ValueError(f"未知的優先順序 {priority!r}")
        ticket = (priority, next(self._seq))
        with self._cond:
            heapq.heappush(self._waiters, ticket)
            try:
                while True:
                    now = self._clock.monotonic()
                    remaining = self._ban_remaining(now)
                    if remaining > 0:
                        self.refused_banned += 1
                        raise RestBanned(remaining)
                    wait = self._wait_needed(ticket, now)
                    if wait == 0:
                        self._sent.append(now)
                        self.granted += 1
                        self.granted_by_priority[priority] += 1
                        return
                    self._clock.wait(self._cond, wait)
            finally:
                self._waiters.remove(ticket)
                heapq.heapify(self._waiters)
                self._cond.notify_all()

    def _wait_needed(self, ticket, now):
        """0 = 現在就放行；正數 = 至少再等這麼多秒；None = 等別人 notify（輪不到或被 hold 擋住）。"""
        if self._waiters[0] != ticket:
            return None
        if ticket[0] > PRIORITY_FOREGROUND and self._holds:
            return None
        if len(self._sent) < self.rate:
            return 0
        ready_at = self._sent[0] + self.window
        return 0 if now >= ready_at else ready_at - now

    # ---------------- 前景保留 ----------------
    def hold_background(self):
        """開始只放行 PRIORITY_FOREGROUND。可巢狀（計數），每次都要配一個 release_background()。"""
        with self._cond:
            self._holds += 1

    def release_background(self):
        with self._cond:
            if self._holds <= 0:
                raise RuntimeError("release_background() 次數多於 hold_background()")
            self._holds -= 1
            self._cond.notify_all()

    @contextmanager
    def foreground_hold(self):
        """with 區塊內只放行前景請求（見模組 docstring「優先順序」）。"""
        self.hold_background()
        try:
            yield
        finally:
            self.release_background()

    def holding(self):
        with self._cond:
            return self._holds > 0

    # ---------------- 429 ----------------
    def trip_ban(self):
        """收到 429：從現在起 ban_cooldown 秒內不放行任何請求，並叫醒所有排隊的人讓他們拋 RestBanned。"""
        with self._cond:
            until = self._clock.monotonic() + self.ban_cooldown
            self._ban_until = until if self._ban_until is None else max(self._ban_until, until)
            self.bans += 1
            self._cond.notify_all()

    def _ban_remaining(self, now):
        if self._ban_until is None:
            return 0.0
        return max(0.0, self._ban_until - now)

    def ban_remaining(self):
        """封鎖冷卻還剩幾秒；沒有封鎖回 0.0。"""
        with self._cond:
            return self._ban_remaining(self._clock.monotonic())

    def waiting(self):
        """目前排隊中的請求數（給測試與診斷用）。"""
        with self._cond:
            return len(self._waiters)


class ServerClock:
    """從回應信封的 `timestamp` 估計「伺服器時間 - 本機時間」的偏移。thread-safe。

    每個樣本：偏移 = 伺服器時間 - 本機送出與收到的中點。RTT 約 0.4 秒，所以單一樣本的誤差可到
    RTT/2；取最近 samples 個樣本的中位數，對偶發的慢回應不敏感。還沒有任何樣本時偏移當 0。
    """

    def __init__(self, clock=None, *, samples=15, warn_ms=None):
        self._clock = clock or SystemClock()
        self._offsets = deque(maxlen=int(samples))
        self._lock = threading.Lock()
        self._warn_ms = config.CLOCK_OFFSET_WARN_MS if warn_ms is None else warn_ms
        self._warned = False
        self.observations = 0

    def observe(self, server_ts_ms, local_send_ms, local_recv_ms):
        offset = float(server_ts_ms) - (local_send_ms + local_recv_ms) / 2.0
        with self._lock:
            self._offsets.append(offset)
            self.observations += 1
            current = statistics.median(self._offsets)
            too_far = abs(current) > self._warn_ms
            crossed = too_far != self._warned
            self._warned = too_far
        if crossed and too_far:
            logger.warning("本機時鐘與派網伺服器時間偏移 %+.0f ms，超過 %d ms。所有延遲與 K 棒時刻以伺服器時間為準，"
                           "但偏移這麼大代表本機校時有問題", current, self._warn_ms)
        elif crossed:
            logger.info("本機時鐘偏移回到 %+.0f ms（門檻 %d ms 以內）", current, self._warn_ms)

    def offset_ms(self):
        """目前的偏移估計（ms，伺服器 - 本機）。沒有樣本回 0。"""
        with self._lock:
            return int(round(statistics.median(self._offsets))) if self._offsets else 0

    def has_estimate(self):
        with self._lock:
            return bool(self._offsets)

    def now_ms(self):
        """伺服器時間估計，UTC epoch ms。"""
        return self._clock.time_ms() + self.offset_ms()


class RestGate:
    """限速取數元件：每個請求都經過 limiter，429 觸發整個行程的封鎖冷卻，回應順便校時。

    api_get 預設是 live.pionex_api.api_get（測試注入假的）。一律以 retries=1 呼叫：
    重試若發生在 api_get 裡面，閘門看不到、算不到，429 更會越重試封鎖越久。
    """

    RECENT_REQUESTS = 50

    def __init__(self, limiter, server_clock, *, clock=None, api_get=None, timeout=None):
        self.limiter = limiter
        self.server_clock = server_clock
        self._clock = clock or SystemClock()
        self._api_get = api_get or pionex_api.api_get
        self._timeout = config.A1_HTTP_TIMEOUT_SECONDS if timeout is None else timeout
        self._lock = threading.Lock()
        self._recent = deque(maxlen=self.RECENT_REQUESTS)
        self.requests = 0
        self.errors = 0
        self.http_429 = 0

    def get(self, path, params=None, *, priority=PRIORITY_FOREGROUND):
        """經過閘門的 GET。回傳完整信封 dict（同 api_get）。

        拋 RestBanned：冷卻中沒送出，或這個請求收到 429（此時閘門已進入冷卻）。
        拋 live.pionex_api.ApiError：其他錯誤，不重試。
        params 一律以 dict 傳給 requests（非 ASCII 的合約名由 requests 做 percent-encode），
        不要把參數拼進 path。
        """
        self.limiter.acquire(priority)
        t0 = self._clock.time_ms()
        # 放行當下就記進「最近的請求」，結果欄先寫 in_flight、完成時再改：429 發生時要回報的序列
        # 必須包含觸發 429 的那一個，以及當時還在飛的其他請求（§8 第 4 條）
        entry = [round(self._clock.monotonic(), 3), PRIORITY_NAMES[priority], path,
                 (params or {}).get("symbol"), "in_flight"]
        with self._lock:
            self.requests += 1
            self._recent.append(entry)
        try:
            js = self._api_get(path, params, retries=1, timeout=self._timeout)
        except pionex_api.ApiError as e:
            if getattr(e, "status_code", None) == 429:
                entry[4] = "429"
                self._on_429(path, params)
                raise RestBanned(self.limiter.ban_remaining(), path) from e
            entry[4] = "error"
            with self._lock:
                self.errors += 1
            raise
        except Exception:
            entry[4] = "exception"
            with self._lock:
                self.errors += 1
            raise
        entry[4] = "ok"
        t1 = self._clock.time_ms()
        ts = js.get("timestamp") if isinstance(js, dict) else None
        if isinstance(ts, (int, float)) and not isinstance(ts, bool):
            self.server_clock.observe(ts, t0, t1)
        return js

    def _on_429(self, path, params):
        self.limiter.trip_ban()
        with self._lock:
            self.http_429 += 1
            recent = [tuple(e) for e in self._recent]
        logger.error("收到 HTTP 429（%s symbol=%s）：整個行程停止送出 REST 請求 %.0f 秒。速率上限 %d req/%.1fs；"
                     "最近 %d 個請求（放行時的單調時刻, 優先順序, 端點, symbol, 結果；in_flight = 當時還在飛）：%s",
                     path, (params or {}).get("symbol"), self.limiter.ban_cooldown, self.limiter.rate,
                     self.limiter.window, len(recent), recent)

    def recent_requests(self):
        """最近 RECENT_REQUESTS 個被放行的請求（放行時的單調時刻, 優先順序, 端點, symbol, 結果），
        給 429 事後追查。結果是 ok / error / exception / 429，還沒完成的是 in_flight。"""
        with self._lock:
            return [tuple(e) for e in self._recent]

    def stats(self):
        with self._lock:
            return {
                "requests": self.requests,
                "errors": self.errors,
                "http_429": self.http_429,
                "bans": self.limiter.bans,
                "refused_banned": self.limiter.refused_banned,
                "granted_by_priority": {PRIORITY_NAMES[p]: n
                                        for p, n in self.limiter.granted_by_priority.items()},
                "clock_offset_ms": self.server_clock.offset_ms(),
            }


# ---------------- 行程內唯一一份 ----------------
_shared = None
_shared_lock = threading.Lock()


def shared_gate():
    """行程內共用的 RestGate（第一次呼叫時依 live/config.py 建立）。A1 與日後 A3 都用這一份。"""
    global _shared
    with _shared_lock:
        if _shared is None:
            clock = SystemClock()
            _shared = RestGate(
                RestLimiter(config.A1_REST_RATE_PER_SECOND, config.REST_BAN_COOLDOWN_SECONDS, clock=clock),
                ServerClock(clock),
                clock=clock,
            )
        return _shared
