# -*- coding: utf-8 -*-
"""
live.price_buffer — 每個 symbol 過去 2 小時多的價格樣本，查「某個時刻的價格」
===========================================================================
A 頻道的粗篩要算「收盤當下的現價 ÷ 2 小時前的價格 - 1」。分子來自收盤那次 tickers，
分母就從這裡查。

兩種樣本：
  tickers 快照   每次輪詢一份，全市場 symbol 的現價（tickers 的 close）。
                 樣本時刻 = 該次回應信封的 timestamp（伺服器時間）。理由：
                   * 每筆 ticker 的 `time` 都在信封 timestamp 之前 0.15–0.9 秒內（captain 實測），
                     在 10 秒一次的取樣粒度下，逐筆時刻不帶來額外資訊，只會讓每個 symbol 的樣本
                     時刻各不相同、查找時多出分支
                   * 不用「排定的輪詢時刻」：輪詢若被延誤（排隊、網路慢），排定時刻就不是
                     價格被觀測到的時刻；伺服器自己的 timestamp 不受本機時鐘偏移與排程延誤影響
                   * 分子與分母都是同一套程序、同一個相位取到的樣本，系統性的快照延遲會互相抵銷
                 以 numpy 陣列存（symbol → 欄位索引），2 小時 720 份 x 600 個 symbol 約 4 MB。
  種子           冷啟動時從 klines 補的：開盤 t 那根的收盤價 = 時刻 t + 1 根的價格。這正是
                 s4_signal 定義 ret2h 用的那個點，所以時刻恰好落在 K 棒邊界上。

price_at() 查找規則：先找恰好在該時刻的種子；否則在 ±tolerance 內找離該時刻最近、且有這個
symbol 的樣本（種子或快照），同距離時種子優先（種子是 K 棒收盤價本身，不是近似）。
找不到回 None —— 呼叫端要把它當成「沒有合格的 2 小時前樣本」計數，不可默默略過。

thread-safe：輪詢在主執行緒寫快照，補種子在背景 worker 寫種子，粗篩在主執行緒讀。
"""

import bisect
import math
import threading
from collections import namedtuple

import numpy as np

Sample = namedtuple("Sample", "price time_ms source")   # source: "seed" | "tickers"


class PriceBuffer:
    """retention_ms：只保留樣本時刻 >= (最新時刻 - retention_ms) 的樣本（prune() 時生效）。"""

    def __init__(self, retention_ms):
        self.retention_ms = int(retention_ms)
        self._lock = threading.Lock()
        self._index = {}          # symbol -> 欄位索引
        self._snap_times = []     # 快照樣本時刻，升冪
        self._snap_prices = []    # 與 _snap_times 對齊的 float64 陣列；長度 = 建立當下的 symbol 數
        self._seeds = {}          # symbol -> {time_ms: price}

    # ---------------- 寫入 ----------------
    def add_snapshot(self, sample_ms, prices):
        """加一份 tickers 快照。prices: {symbol: 正的有限價格}；其他值略過（呼叫端應先驗證）。"""
        sample_ms = int(sample_ms)
        with self._lock:
            for sym in prices:
                if sym not in self._index:
                    self._index[sym] = len(self._index)
            arr = np.full(len(self._index), np.nan)
            for sym, p in prices.items():
                if _valid(p):
                    arr[self._index[sym]] = float(p)
            i = bisect.bisect_right(self._snap_times, sample_ms)
            self._snap_times.insert(i, sample_ms)
            self._snap_prices.insert(i, arr)

    def add_seed(self, symbol, points):
        """加種子樣本。points: iterable of (time_ms, price)。同一時刻重複加以後來的為準。"""
        with self._lock:
            d = self._seeds.setdefault(symbol, {})
            for t, p in points:
                if _valid(p):
                    d[int(t)] = float(p)

    def prune(self, now_ms):
        """丟掉比 now_ms - retention_ms 還舊的樣本。"""
        cutoff = int(now_ms) - self.retention_ms
        with self._lock:
            k = bisect.bisect_left(self._snap_times, cutoff)
            if k:
                del self._snap_times[:k]
                del self._snap_prices[:k]
            for sym in list(self._seeds):
                d = self._seeds[sym]
                for t in [t for t in d if t < cutoff]:
                    del d[t]
                if not d:
                    del self._seeds[sym]

    # ---------------- 查詢 ----------------
    def price_at(self, symbol, t_ms, tolerance_ms):
        """symbol 在 t_ms 的價格樣本（Sample）；±tolerance_ms 內都沒有就回 None。"""
        t_ms = int(t_ms)
        with self._lock:
            seeds = self._seeds.get(symbol)
            if seeds and t_ms in seeds:
                return Sample(seeds[t_ms], t_ms, "seed")
            best = None
            if seeds:
                for t, p in seeds.items():
                    d = abs(t - t_ms)
                    if d <= tolerance_ms and (best is None or d < best[0]):
                        best = (d, Sample(p, t, "seed"))
            snap = self._nearest_snapshot(symbol, t_ms, tolerance_ms)
            if snap is not None and (best is None or snap[0] < best[0]):
                best = snap
            return None if best is None else best[1]

    def _nearest_snapshot(self, symbol, t_ms, tolerance_ms):
        col = self._index.get(symbol)
        if col is None:
            return None
        times = self._snap_times
        i = bisect.bisect_left(times, t_ms)
        best = None
        # 往兩邊各掃到超出容忍範圍為止；10 秒一份、容忍 10 秒，實際只會看 2-3 份
        for j in range(i - 1, -1, -1):
            d = t_ms - times[j]
            if d > tolerance_ms:
                break
            p = self._price_in(j, col)
            if p is not None:
                best = (d, Sample(p, times[j], "tickers"))
                break
        for j in range(i, len(times)):
            d = times[j] - t_ms
            if d > tolerance_ms or (best is not None and d >= best[0]):
                break
            p = self._price_in(j, col)
            if p is not None:
                best = (d, Sample(p, times[j], "tickers"))
                break
        return best

    def first_after(self, symbol, t_ms, until_ms):
        """symbol 在 [t_ms, until_ms) 之間最早的一個樣本（種子或快照）；沒有回 None。

        只拿來替「沒有合格 2 小時前樣本」的 symbol 排升格順序，不參與 ret2h 的計算。
        """
        with self._lock:
            best = None
            for t, p in (self._seeds.get(symbol) or {}).items():
                if t_ms <= t < until_ms and (best is None or t < best.time_ms):
                    best = Sample(p, t, "seed")
            col = self._index.get(symbol)
            if col is not None:
                i = bisect.bisect_left(self._snap_times, t_ms)
                for j in range(i, len(self._snap_times)):
                    t = self._snap_times[j]
                    if t >= until_ms or (best is not None and t >= best.time_ms):
                        break
                    p = self._price_in(j, col)
                    if p is not None:
                        best = Sample(p, t, "tickers")
                        break
            return best

    def _price_in(self, j, col):
        arr = self._snap_prices[j]
        if col >= len(arr):
            return None
        p = arr[col]
        return None if math.isnan(p) else float(p)

    def snapshot_count(self):
        with self._lock:
            return len(self._snap_times)

    def seeded_symbols(self):
        with self._lock:
            return set(self._seeds)


def _valid(p):
    try:
        p = float(p)
    except (TypeError, ValueError):
        return False
    return math.isfinite(p) and p > 0
