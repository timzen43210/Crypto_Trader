#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
G2 量測腳本的離線自檢
=====================
**完全不連網**。用假的 REST 回應與假的 WebSocket 連線，把 g2_measure.py 的五個階段
各跑一遍，檢查主流程、輸出 schema、以及錯誤處理分支。

用途有兩個：
  1. 腳本是在不能連派網 API 的機器上寫的，這支自檢是它唯一的迴歸測試。
  2. laptop 第一次跑之前先執行它，確認這台機器的 Python 環境沒問題
     （pandas/numpy 版本、strategy 套件 import 得到、檔案寫得出去），
     再去投入 20-100 分鐘的真實量測。

用法：
    python research/g2_selftest.py            # 跑全部
    python research/g2_selftest.py -v         # 失敗時印完整 traceback

產物一律寫到 research/_tmp/selftest/（不進版控），不會動到 research/results/。
離開碼 0 = 全過，1 = 有失敗。

--- 這支自檢實際驗了什麼（對應 PRD 的 AC-2）---
* Aggregator 的 K 棒邊界（左閉右開）、亂序到達、重複 tradeId、封存後晚到與回填
* [D3] 觀測窗守衛的值域（守衛時間被改小會被抓到）、[D5] data_ready_ms 取最後一筆到達
* [D6] push lag 區間要留得住負值（本地時鐘快）
* WS 訊息解析：TRADE / PING→PONG / 訂閱 ack / 錯誤訊息 / 欄位對不上
* Stage 0：正常、429 退避降速、riskTable 失敗不致命、tickers 缺欄位的 FR-5 訊息
* Stage 1：假交易所（獨立實作的聚合）→ 端到端比對，volume_ratio 必須是 1.0
* Stage 1：交易所計雙邊量（量 ×2）時 volume_ratio 必須是 0.5 且判成「疑似雙邊量」
          ——比值的**方向**要有測試，否則 lv/ev、ev/lv、寫死 1.0 三者分不出來
* Stage 1：判定的四個分支、--smoke 判定必須被標記成「不算數」
* Stage 1：WS 連不上 / payload 欄位對不上，都要走到指名道姓的訊息且仍寫出 json
* Stage 2：延遲拆解欄位齊全、封存快照與事後真值並列、compute 步驟真的跑到 s4_signal
* Stage 3：輸入契約（time 升冪、固定週期、bars_per_hour 是 int）、NaN 根數符合預期
* Stage 4：合成快取裡「種」一個必然被粗篩漏掉的訊號，確認腳本抓得到它
* Stage 4：LAG 0/1/2 敏感度掃描（漏失率對 LAG 不單調，不能只給一個數字）
* method_notes 確實是 docstring 原文、metadata 帶 script_sha256
* 快取缺失、--ca-bundle 檔案不存在 等 FR-5 訊息；預期外例外的兜底指引與離開碼
"""
import asyncio
import contextlib
import hashlib
import io
import json
import math
import os
import shutil
import sys
import time
import traceback
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import g2_measure as g2  # noqa: E402

TMP = Path(__file__).resolve().parent / "_tmp" / "selftest"
OUT = TMP / "results"
CACHE = TMP / "synth_cache"
VERBOSE = "-v" in sys.argv or "--verbose" in sys.argv

RESULTS = []


def check(name, fn):
    t0 = time.perf_counter()
    try:
        detail = fn() or ""
        RESULTS.append((True, name, detail, time.perf_counter() - t0))
        print(f"  PASS  {name}  {detail}")
    except Exception as e:
        tb = traceback.format_exc() if VERBOSE else f"{type(e).__name__}: {e}"
        RESULTS.append((False, name, tb, time.perf_counter() - t0))
        print(f"  FAIL  {name}\n        {tb}")


def eq(a, b, what=""):
    if a != b:
        raise AssertionError(f"{what}：期待 {b!r}，實際 {a!r}")


def close_to(a, b, tol, what=""):
    if a is None or abs(a - b) > tol:
        raise AssertionError(f"{what}：期待 {b} ± {tol}，實際 {a!r}")


def truthy(v, what=""):
    if not v:
        raise AssertionError(f"{what}：期待為真，實際 {v!r}")


def has_keys(d, keys, what=""):
    missing = [k for k in keys if k not in d]
    if missing:
        raise AssertionError(f"{what}：缺少欄位 {missing}")


def raises_with(fn, *fragments):
    try:
        fn()
    except g2.G2Error as e:
        msg = str(e)
        for f in fragments:
            if f not in msg:
                raise AssertionError(f"錯誤訊息裡找不到 {f!r}；實際訊息：{msg}")
        return msg.splitlines()[0][:70]
    except Exception as e:
        raise AssertionError(f"期待 G2Error，實際是 {type(e).__name__}: {e}")
    raise AssertionError("期待丟出 G2Error，但沒有")


# ============================== 假 REST ==============================
class FakeResp:
    def __init__(self, payload, status=200, headers=None, text=""):
        self.status_code = status
        self.headers = headers or {}
        self._payload = payload
        self.text = text or json.dumps(payload)

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


def ok(data):
    return {"result": True, "data": data}


SYMBOLS_PAYLOAD = ok({"symbols": [
    {"symbol": "AAA_USDT_PERP", "status": "TRADING", "baseCurrency": "AAA", "quoteCurrency": "USDT"},
    {"symbol": "BBB_USDT_PERP", "status": "TRADING", "baseCurrency": "BBB", "quoteCurrency": "USDT"},
    {"symbol": "CCC_USDT_PERP", "status": "TRADING", "baseCurrency": "CCC", "quoteCurrency": "USDT"},
    {"symbol": "DDD_USDT_PERP", "status": "TRADING", "baseCurrency": "DDD", "quoteCurrency": "USDT"},
    {"symbol": "ZZZ_USDT_PERP", "status": "HALT", "baseCurrency": "ZZZ", "quoteCurrency": "USDT"},
]})
TICKERS_PAYLOAD = ok({"tickers": [
    {"symbol": "AAA_USDT_PERP", "amount": "80000"},      # 在 [2萬, 50萬] 內 → 候選
    {"symbol": "BBB_USDT_PERP", "amount": "250000"},     # 候選
    {"symbol": "CCC_USDT_PERP", "amount": "1500"},       # 太小 → 不是候選
    {"symbol": "DDD_USDT_PERP", "amount": "9000000"},    # 太大 → 不是候選
    {"symbol": "ZZZ_USDT_PERP", "amount": "60000"},      # 不是 TRADING
]})
RISK_PAYLOAD = ok({"riskTable": [
    {"symbol": "AAA_USDT_PERP", "tiers": [{"maxLeverage": 20}, {"maxLeverage": 10}]},
    {"symbol": "BBB_USDT_PERP", "tiers": [{"maxLeverage": 5}]},
]})


def make_http(routes, hits=None):
    """routes: path -> payload / FakeResp / callable(params, n_hit)。"""
    hits = hits if hits is not None else {}

    def _http(ctx, path, params):
        hits[path] = hits.get(path, 0) + 1
        r = routes.get(path)
        if r is None:
            return FakeResp({"result": False, "code": "NOT_FOUND", "message": f"no route {path}"})
        if callable(r):
            r = r(params, hits[path])
        if isinstance(r, FakeResp):
            return r
        if isinstance(r, Exception):
            raise r
        return FakeResp(r, headers={"Date": time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())})
    return _http, hits


# ============================== 假 WebSocket + 假交易所 ==============================
class FakeExchange:
    """記下所有『發生過』的成交，並用**獨立於 Aggregator 的實作**聚合成 K 棒。

    這樣 Stage 1 的比對才是真的在驗證 g2_measure 的聚合與時間邊界，
    而不是拿同一份程式跟自己比。

    vol_scale 用來模擬「交易所的 volume 不等於本地 taker 單邊量」——預設 1.0（兩邊一致），
    設 2.0 就是交易所把買賣雙邊都算進去。沒有這個旋鈕的話 local volume 恆等於 exchange
    volume，volume_ratio 永遠是 1.0，lv/ev、ev/lv、寫死 1.0 三種實作在測試上完全無法分辨。
    """

    def __init__(self, bar_ms, vol_scale=1.0):
        self.bar = bar_ms
        self.vol_scale = vol_scale
        self.trades = {}      # symbol -> [(ts, price, size)]

    def record(self, symbol, ts, price, size):
        self.trades.setdefault(symbol, []).append((ts, price, size))

    def klines(self, symbol, first_open, last_open):
        buckets = {}
        for ts, price, size in self.trades.get(symbol, []):
            bo = ts - (ts % self.bar)           # 刻意用另一種寫法算邊界
            if not (first_open <= bo <= last_open):
                continue
            b = buckets.setdefault(bo, {"time": bo, "open": price, "high": price,
                                        "low": price, "close": price, "volume": 0.0, "amount": 0.0})
            b["high"] = max(b["high"], price)
            b["low"] = min(b["low"], price)
            b["close"] = price
            b["volume"] += size * self.vol_scale
            b["amount"] += price * size * self.vol_scale
        return [buckets[k] for k in sorted(buckets)]


class FakeWS:
    """支援 send / async-for / close 三件事，與 websockets 的 ClientConnection 介面對得上。"""

    def __init__(self, exch, tick=0.02, emit_ping_every=25, seed=0):
        self.exch = exch
        self.subs = []
        self.closed = False
        self.sent = []
        self.tick = tick
        self.emit_ping_every = emit_ping_every
        self.n = 0
        self.rng = np.random.default_rng(seed)
        self.pongs = 0
        self.price = {}

    async def send(self, s):
        m = json.loads(s)
        self.sent.append(m)
        if m.get("op") == "SUBSCRIBE":
            self.subs.append(m["symbol"])
            self._ack = m["symbol"]
        elif m.get("op") == "PONG":
            self.pongs += 1

    async def close(self):
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(self.tick)
        if self.closed:
            raise StopAsyncIteration
        self.n += 1
        if self.n == 1:
            return json.dumps({"op": "SUBSCRIBED", "topic": "TRADE", "symbol": self.subs[0] if self.subs else None})
        if self.emit_ping_every and self.n % self.emit_ping_every == 0:
            return json.dumps({"op": "PING", "timestamp": g2.now_ms()})
        if not self.subs:
            return json.dumps({"op": "HEARTBEAT"})
        sym = self.subs[self.n % len(self.subs)]
        ts = g2.now_ms()
        # 推送出去的是 6 位小數的字串，假交易所必須記錄「送出去的那個值」，
        # 否則比對誤差會是浮點格式化造成的假象，而不是聚合邏輯的問題
        px = float(f"{self.price.get(sym, 100.0) * float(np.exp(self.rng.normal(0, 0.001))):.6f}")
        self.price[sym] = px
        size = float(round(self.rng.uniform(0.5, 5.0), 4))
        self.exch.record(sym, ts, px, size)
        return json.dumps({"topic": "TRADE", "symbol": sym, "timestamp": ts,
                           "data": [{"symbol": sym, "tradeId": f"{sym}-{self.n}", "price": f"{px:.6f}",
                                     "size": f"{size}", "side": "BUY" if self.n % 2 else "SELL",
                                     "timestamp": ts}]})


class Clock:
    """虛擬時鐘：真實 1 秒 = 虛擬 speed 秒。讓 1 分 K 的流程在幾秒內跑完。"""

    def __init__(self, speed):
        self.speed = speed
        self.t0_real = time.time() * 1000
        self.t0_virt = self.t0_real

    def __call__(self):
        return int(self.t0_virt + (time.time() * 1000 - self.t0_real) * self.speed)


class Patch:
    """暫時替換 g2_measure 的模組層 seam（_http_get_raw / _open_ws / now_ms / time.sleep）。"""

    def __init__(self, **kw):
        self.kw = kw
        self.old = {}

    def __enter__(self):
        for k, v in self.kw.items():
            if k == "sleep":
                self.old[k] = g2.time.sleep
                g2.time.sleep = v
            else:
                self.old[k] = getattr(g2, k)
                setattr(g2, k, v)
        return self

    def __exit__(self, *a):
        for k, v in self.old.items():
            if k == "sleep":
                g2.time.sleep = v
            else:
                setattr(g2, k, v)
        return False


def run_stage(argv, http=None, ws=None, clock=None, no_sleep=False):
    """用 g2_measure 自己的 parser 解析參數再執行，確保驗到的就是使用者會跑的那條路徑。"""
    argv = list(argv) + ["--out-dir", str(OUT), "--tmp-dir", str(TMP / "tmp"),
                         "--machine", "selftest"]
    args = g2.apply_defaults(g2.build_parser().parse_args(argv))
    patches = {}
    if http:
        patches["_http_get_raw"] = http
    if ws:
        patches["_open_ws"] = ws
    if clock:
        patches["now_ms"] = clock
    if no_sleep:
        patches["sleep"] = lambda s: None
    with Patch(**patches):
        ctx = g2.Ctx(args)
        fn = {0: g2.stage0, 1: g2.stage1, 2: g2.stage2, 3: g2.stage3, 4: g2.stage4}[args.stage]
        return fn(ctx), ctx


# ============================== 1. Aggregator ==============================
def t_agg_boundary():
    """[D2] 左閉右開：ts == bar_open 進新的一根，ts == bar_open-1 留在舊的一根。"""
    bar = 300_000
    a = g2.Aggregator(bar)
    b0 = 1_700_000_000_000 // bar * bar
    a.add_trade("X", 10.0, 1.0, b0, b0 + 10, tid="t1")            # 邊界起點
    a.add_trade("X", 11.0, 2.0, b0 + bar - 1, b0 + bar, tid="t2")  # 最後 1 毫秒
    a.add_trade("X", 12.0, 3.0, b0 + bar, b0 + bar + 5, tid="t3")  # 下一根
    eq(len(a.bars), 2, "應該產生兩根 K 棒")
    b = a.bars[("X", b0)]
    eq((b["open"], b["close"], b["high"], b["low"]), (10.0, 11.0, 11.0, 10.0), "第一根 OHLC")
    eq(b["volume"], 3.0, "第一根 volume")
    close_to(b["amount"], 10.0 * 1 + 11.0 * 2, 1e-9, "第一根 amount")
    eq(a.bars[("X", b0 + bar)]["trades"], 1, "第二根只有一筆")
    return "邊界 [open, open+BAR) 正確"


def t_agg_out_of_order_and_dupe():
    """亂序到達時 open/close 要看時間戳，不看到達順序；重複 tradeId 要丟掉。"""
    bar = 300_000
    a = g2.Aggregator(bar)
    b0 = 1_700_000_000_000 // bar * bar
    a.add_trade("X", 50.0, 1.0, b0 + 200_000, b0 + 200_100, tid="mid")
    a.add_trade("X", 10.0, 1.0, b0 + 1_000, b0 + 250_000, tid="early")   # 晚到但時間戳最早
    a.add_trade("X", 90.0, 1.0, b0 + 299_000, b0 + 299_100, tid="late")
    a.add_trade("X", 90.0, 1.0, b0 + 299_000, b0 + 299_200, tid="late")  # 重複
    b = a.bars[("X", b0)]
    eq(b["open"], 10.0, "open 應取時間戳最早的成交價")
    eq(b["close"], 90.0, "close 應取時間戳最晚的成交價")
    eq(b["trades"], 3, "重複 tradeId 應被丟棄")
    eq(a.dup_count, 1, "重複計數")
    eq(b["volume"], 3.0, "重複不該計入 volume")
    return "亂序 / 去重 正確"


def t_agg_seal_and_late():
    """封存後才到的成交要被計數（代表 --seal-grace 不夠），且不影響已輸出的數字判讀。"""
    bar = 300_000
    a = g2.Aggregator(bar)
    b0 = 1_700_000_000_000 // bar * bar
    a.add_trade("X", 10.0, 1.0, b0 + 1000, b0 + 1100, tid="a")
    recs = a.seal(b0, ["X", "Y"], b0 + bar + 2000)
    eq(len(recs), 2, "沒有成交的 symbol 也要出一筆")
    y = [r for r in recs if r["symbol"] == "Y"][0]
    truthy("local_no_trades" in y["flags"], "Y 應標記 local_no_trades")
    x = [r for r in recs if r["symbol"] == "X"][0]
    eq(x["data_ready_ms"], (b0 + 1100) - (b0 + bar), "data_ready_ms = 最後到達時刻 - 收盤時刻")
    eq(x["seal_ms"], 2000, "seal_ms = 封存時刻 - 收盤時刻")
    a.add_trade("X", 11.0, 1.0, b0 + 299_000, b0 + bar + 3000, tid="b")
    eq(a.bars[("X", b0)]["trades_after_seal"], 1, "封存後晚到要被計數")
    return "封存 / 晚到計數 正確"


def t_agg_data_ready_ms():
    """[D5] data_ready_ms = **最後一筆**成交到達 - 收盤，不是第一筆。

    單筆成交的情境下 first_recv_ms == max_recv_ms，兩種實作看起來一模一樣，
    所以這裡刻意給三筆、到達時刻亂序，讓「取第一筆」與「取最後一筆」差到符號都不同。
    """
    bar = 300_000
    a = g2.Aggregator(bar)
    b0 = 1_700_000_000_000 // bar * bar
    a.add_trade("X", 10.0, 1.0, b0 + 1_000, b0 + bar - 4_000, tid="1")   # 收盤前就到
    a.add_trade("X", 11.0, 1.0, b0 + 10_000, b0 + bar + 1_500, tid="2")  # 收盤後 1.5 秒
    a.add_trade("X", 12.0, 1.0, b0 + 5_000, b0 + bar + 700, tid="3")     # 收盤後 0.7 秒（亂序）
    x = a.seal(b0, ["X"], b0 + bar + 2_000)[0]
    eq(x["data_ready_ms"], 1_500, "data_ready_ms 要取最後一筆到達")
    eq(x["first_recv_ms"], b0 + bar - 4_000, "first_recv_ms 仍要照實記錄（只是不能拿來當 data_ready）")
    eq(x["max_recv_ms"], b0 + bar + 1_500, "max_recv_ms")
    truthy(x["data_ready_ms"] > 0, "資料齊全的時刻必定晚於收盤；量成負數代表用錯了起算點")
    return "data_ready_ms = max(recv) - bar_close = 1500 ms"


def t_agg_late_arrivals_backfill():
    """[D5] 封存後才到的成交必須回填進**輸出**。

    seal() 交出去的是快照複本，晚到成交只改得到 Aggregator 內部的 b。不回填的話
    trades_after_seal 在 json 上結構性恆為 0（「grace 不夠」的警告永遠不會觸發），
    data_ready_ms 與 volume 也都是右設限值，而使用者沒有任何線索知道自己看到的是截斷值。
    """
    bar = 300_000
    a = g2.Aggregator(bar)
    b0 = 1_700_000_000_000 // bar * bar
    a.add_trade("X", 10.0, 1.0, b0 + 1_000, b0 + 1_100, tid="a")
    recs = a.seal(b0, ["X"], b0 + bar + 2_000)
    x = recs[0]
    eq(x["trades_after_seal"], 0, "封存當下本來就不可能有晚到，快照是 0")
    a.add_trade("X", 11.0, 2.0, b0 + 299_000, b0 + bar + 5_000, tid="b")   # 晚到 3 秒
    a.add_trade("X", 11.0, 2.0, b0 + 299_000, b0 + bar + 6_000, tid="b")   # 重連補推的重複 → 不可灌水
    a.apply_late_arrivals(b0, recs)
    eq(x["trades_after_seal"], 1, "晚到筆數必須出現在輸出上，否則警告是死碼")
    eq(x["max_late_after_seal_ms"], 3_000, "最晚差多久 = 5000 - 2000")
    eq(x["trades"], 2, "事後真值：2 筆（重複的那筆不算）")
    eq(x["trades_at_seal"], 1, "封存快照：1 筆")
    eq(x["volume"], 3.0, "事後真值 volume")
    eq(x["volume_at_seal"], 1.0, "封存快照 volume（右設限）")
    eq(x["volume_after_seal"], 2.0, "差額")
    eq(x["close"], 11.0, "晚到那筆的時間戳最晚 → close 要跟著更新（[D4] 看時間戳不看到達順序）")
    eq(x["data_ready_ms"], 5_000, "事後真值：資料真正齊全是收盤後 5 秒")
    eq(x["data_ready_ms_at_seal"], 1_100 - bar, "封存快照是右設限值（這裡甚至是負的）")
    a.apply_late_arrivals(b0, recs)     # 冪等：重複呼叫不可把事後真值再存一次當快照
    eq(x["volume_at_seal"], 1.0, "重複回填不可覆蓋快照")
    eq(x["trades"], 2, "重複回填不可重複累加")
    return "晚到 1 筆回填成功、快照另存 *_at_seal、去重與冪等都成立"


def t_agg_clock_offset():
    """[D6](b) min/max_push_lag_ms = min/max(本地收到時刻 - 交易所時間戳)。

    min 必須留得住負數——本地時鐘快的時候它就是負的，而那正是判讀要看的訊號。
    """
    bar = 60_000
    a = g2.Aggregator(bar)
    b0 = 1_700_000_000_000 // bar * bar
    a.add_trade("X", 1.0, 1.0, b0 + 1_000, b0 + 1_120, tid="1")   # lag +120
    a.add_trade("X", 1.0, 1.0, b0 + 2_000, b0 + 1_950, tid="2")   # lag -50（本地時鐘快）
    a.add_trade("X", 1.0, 1.0, b0 + 3_000, b0 + 3_400, tid="3")   # lag +400
    eq(a.min_push_lag_ms, -50, "min_push_lag_ms 要保留負值")
    eq(a.max_push_lag_ms, 400, "max_push_lag_ms")
    return "push lag 區間 [-50, 400] ms，負值沒有被夾掉"


def t_first_full_bar_open():
    """[D3] 觀測窗守衛的**值域**：訂閱完成離邊界多近就得多丟一根。

    不斷言 FIRST_BAR_GUARD_MS 的字面值（那種斷言只是把常數抄第二遍），
    改成掃過守衛前後各一毫秒的邊界行為——守衛被改成 0 或 1000 都會讓這裡掛掉。
    """
    bar = 300_000
    b0 = 1_700_000_000_000 // bar * bar
    eq(g2.first_full_bar_open(b0 - 8_000, bar), b0,
       "離邊界 8 秒（守衛綽綽有餘）→ 下一根就算完整")
    eq(g2.first_full_bar_open(b0 - 5_000, bar), b0,
       "離邊界剛好 5 秒 → 仍算得及")
    eq(g2.first_full_bar_open(b0 - 4_999, bar), b0 + bar,
       "只差 1 毫秒不足守衛 → 必須再丟一根")
    eq(g2.first_full_bar_open(b0 - 3_000, bar), b0 + bar,
       "離邊界 3 秒 → 必須再丟一根")
    eq(g2.first_full_bar_open(b0, bar), b0 + bar,
       "訂閱完成正好踩在邊界上 → 那根已經開始，丟掉")
    eq(g2.first_full_bar_open(b0 + 1, bar), b0 + bar,
       "邊界後 1 毫秒 → 同一根，一樣丟掉")
    return "守衛值域：5 秒內再丟一根、5 秒（含）以上不丟"


def t_agg_taker_split():
    bar = 60_000
    a = g2.Aggregator(bar)
    b0 = 1_700_000_000_000 // bar * bar
    a.add_trade("X", 10.0, 2.0, b0, b0, side="BUY", tid="1")
    a.add_trade("X", 10.0, 3.0, b0, b0, side="SELL", tid="2")
    b = a.bars[("X", b0)]
    eq((b["taker_buy_volume"], b["taker_sell_volume"]), (2.0, 3.0), "taker 買賣拆分")
    return "taker 買/賣量拆分正確"


# ============================== 2. 訊息解析 ==============================
def t_parse_messages():
    kind, sym, data = g2.parse_trade_message(json.dumps(
        {"topic": "TRADE", "symbol": "A_USDT_PERP",
         "data": [{"tradeId": "1", "price": "1.5", "size": "10", "side": "BUY", "timestamp": 1700000000000}]}))
    eq(kind, "trade", "TRADE 應解析成 trade")
    eq(data[0]["price"], 1.5, "價格")
    eq(data[0]["size"], 10.0, "數量")
    eq(g2.parse_trade_message('{"op":"PING","timestamp":1}')[0], "ping", "PING")
    eq(g2.parse_trade_message('{"op":"SUBSCRIBED","topic":"TRADE","symbol":"A"}')[0], "ack", "ack")
    eq(g2.parse_trade_message('{"op":"ERROR","message":"bad topic","code":"X1"}')[0], "error", "error")
    eq(g2.parse_trade_message('not json')[0], "other", "非 JSON 不該爆")
    eq(g2.parse_trade_message('{"topic":"DEPTH","data":[]}')[0], "other", "非 TRADE topic")
    # 欄位對不上 → badtrade（而不是安靜地漏資料）
    eq(g2.parse_trade_message(json.dumps(
        {"topic": "TRADE", "symbol": "A", "data": [{"px": "1", "vol": "2"}]}))[0], "badtrade", "缺時間戳")
    # 帶 code 的正常資料訊息不可被誤判成 error
    eq(g2.parse_trade_message(json.dumps(
        {"topic": "TRADE", "symbol": "A", "code": 0,
         "data": [{"price": 1, "size": 1, "timestamp": 1}]}))[0], "trade", "code=0 的資料訊息")
    return "TRADE / PING / ack / error / 壞欄位 都分得出來"


# ============================== 3. Stage 0 ==============================
def t_stage0_ok():
    http, hits = make_http({"/api/v1/common/symbols": SYMBOLS_PAYLOAD,
                            "/api/v1/market/tickers": TICKERS_PAYLOAD,
                            "/api/v1/common/riskTable": RISK_PAYLOAD})
    res, ctx = run_stage(["--stage", "0"], http=http, no_sleep=True)
    eq(res["symbols_total"], 5, "symbols 總數")
    eq(res["symbols_trading"], 4, "只算 TRADING")
    eq(res["candidates_count"], 2, "候選數（AAA 8 萬、BBB 25 萬）")
    eq([c["symbol"] for c in res["candidates"]], ["BBB_USDT_PERP", "AAA_USDT_PERP"], "候選依成交額排序")
    eq(res["candidates"][1]["tier1_max_leverage"], 20.0, "AAA 的 tier1 槓桿")
    has_keys(res["metadata"], ["script", "machine", "started_utc", "python", "pandas",
                               "rest_requests_total", "s4_params", "params", "git_commit"], "FR-6 metadata")
    eq(res["metadata"]["rest_requests_total"], 3, "請求數")
    truthy((OUT / "stage0_universe.json").exists(), "json 應寫出")
    json.loads((OUT / "stage0_universe.json").read_text(encoding="utf-8"))
    return f"候選 2 個、3 次請求、metadata 齊全"


def t_stage0_429():
    """FR-3：429 要退避 + 降速，而不是繼續轟。"""
    def tickers(params, n):
        return FakeResp(None, status=429, text="rate limited") if n == 1 else FakeResp(TICKERS_PAYLOAD)
    http, _ = make_http({"/api/v1/common/symbols": SYMBOLS_PAYLOAD,
                         "/api/v1/market/tickers": tickers,
                         "/api/v1/common/riskTable": RISK_PAYLOAD})
    slept = []
    res, ctx = run_stage(["--stage", "0", "--rate", "8"], http=http,
                         no_sleep=False) if False else (None, None)
    with Patch(_http_get_raw=http, sleep=lambda s: slept.append(s)):
        args = g2.apply_defaults(g2.build_parser().parse_args(
            ["--stage", "0", "--rate", "8", "--out-dir", str(OUT), "--tmp-dir", str(TMP / "tmp")]))
        ctx = g2.Ctx(args)
        res = g2.stage0(ctx)
    eq(ctx.err_429, 1, "429 計數")
    close_to(ctx.rate, 4.0, 1e-9, "限速應減半")
    truthy(any(s >= 60 for s in slept), "應該睡至少 60 秒（派網封鎖時間）")
    truthy(any("429" in n for n in ctx.notes), "429 應寫進 runtime_notes")
    eq(res["candidates_count"], 2, "退避後仍拿到正確結果")
    return f"429 → 睡 {max(slept):.0f}s、限速 8→{ctx.rate:.0f} req/s、結果照常"


def t_stage0_risktable_nonfatal():
    http, _ = make_http({"/api/v1/common/symbols": SYMBOLS_PAYLOAD,
                         "/api/v1/market/tickers": TICKERS_PAYLOAD,
                         "/api/v1/common/riskTable": {"result": False, "code": "NO_SUCH_PATH",
                                                      "message": "not found"}})
    res, ctx = run_stage(["--stage", "0"], http=http, no_sleep=True)
    eq(res["candidates_count"], 2, "riskTable 掛掉不該影響候選")
    truthy(res["risk_table"]["error"], "錯誤應記在 json")
    truthy(any("riskTable" in n for n in ctx.notes), "應提示 --risk-table-path")
    return "riskTable 失敗 → 記錄後繼續"


def t_stage0_bad_tickers():
    bad = ok({"tickers": [{"symbol": "AAA_USDT_PERP", "lastPrice": "1.0"}]})
    http, _ = make_http({"/api/v1/common/symbols": SYMBOLS_PAYLOAD,
                         "/api/v1/market/tickers": bad,
                         "/api/v1/common/riskTable": RISK_PAYLOAD})
    msg = raises_with(lambda: run_stage(["--stage", "0"], http=http, no_sleep=True),
                      "24h 成交額", "raw_ticker_sample")
    return msg


def t_stage0_ssl_error():
    class SSLError(Exception):
        pass
    SSLError.__name__ = "SSLError"

    def http(ctx, path, params):
        raise SSLError("certificate verify failed")
    msg = raises_with(lambda: run_stage(["--stage", "0"], http=http, no_sleep=True),
                      "--ca-bundle", "TLS")
    return msg


def t_stage0_html_response():
    http, _ = make_http({"/api/v1/common/symbols": FakeResp(None, status=200,
                                                            text="<html>proxy login</html>")})
    return raises_with(lambda: run_stage(["--stage", "0"], http=http, no_sleep=True),
                       "不是 JSON", "proxy")


def t_ca_bundle_missing():
    return raises_with(lambda: run_stage(["--stage", "3", "--source", "synthetic",
                                          "--ca-bundle", str(TMP / "no_such_ca.pem")]),
                       "--ca-bundle", "不存在")


# ============================== 4. Stage 1 ==============================
def t_stage1_end_to_end():
    """假交易所用獨立實作聚合同一批成交 → volume_ratio 必須剛好是 1.0。
       只要 [D2] 的邊界或 [D3] 的可比對窗有一根之差，這個測試就會掛。"""
    exch = FakeExchange(g2.INTERVAL_MS["1M"])
    wss = []

    async def fake_open(uri, ssl_ctx, open_timeout, ping_interval):
        w = FakeWS(exch, tick=0.01, seed=len(wss))
        wss.append(w)
        return w

    def http(ctx, path, params):
        if path == "/api/v1/market/tickers":
            return FakeResp(TICKERS_PAYLOAD, headers={"Date": time.strftime(
                "%a, %d %b %Y %H:%M:%S GMT", time.gmtime())})
        if path == "/api/v1/market/klines":
            sym = params["symbol"]
            end = int(params["endTime"])
            bar = g2.INTERVAL_MS["1M"]
            rows = exch.klines(sym, end - int(params["limit"]) * bar, end)
            return FakeResp(ok({"klines": rows}))
        return FakeResp({"result": False, "code": "X", "message": path})

    clock = Clock(speed=60)
    res, ctx = run_stage(["--stage", "1", "--smoke", "--settle", "0", "--seal-grace", "1",
                          "--connections", "1", "--sub-rate", "20", "--max-minutes", "5"],
                         http=http, ws=fake_open, clock=clock, no_sleep=True)
    s = res["summary"]
    truthy(s["clean_bars"] >= 2, f"至少要有 2 根可比對的 K 棒（2 symbol × 1 bar），實際 {s['clean_bars']}")
    close_to(s["volume_ratio"]["p50"], 1.0, 1e-9, "volume_ratio 中位數")
    close_to(s["volume_ratio"]["min"], 1.0, 1e-9, "volume_ratio 最小值")
    close_to(s["volume_ratio"]["max"], 1.0, 1e-9, "volume_ratio 最大值")
    close_to(s["close_rel"]["p50"], 0.0, 1e-9, "close 相對誤差")
    close_to(s["open_rel"]["p50"], 0.0, 1e-9, "open 相對誤差")
    truthy("可還原" in res["verdict"]["label"], f"判定應為可還原，實際 {res['verdict']['label']}")
    b = res["bars"][0]
    has_keys(b, ["symbol", "bar_open", "bar_open_utc", "bar_close", "local", "exchange", "diff", "flags"],
             "逐根原始資料")
    has_keys(b["local"], ["open", "high", "low", "close", "volume", "amount", "trades",
                          "taker_buy_volume", "taker_sell_volume", "data_ready_ms"], "本地欄位")
    has_keys(res["feed"], ["subscribe_elapsed_s", "reconnects", "outages", "trades_received",
                           "clock_offset", "raw_message_samples"], "feed 摘要")
    truthy(res["feed"]["raw_message_samples"], "應保存原始訊息樣本")
    truthy(sum(w.pongs for w in wss) >= 1, "應該回過 PONG（不回會被伺服器斷線）")
    # 第一根（半根）必須被排除
    first_seen = min(t for t in exch.trades[list(exch.trades)[0]] and
                     [x[0] for x in exch.trades[list(exch.trades)[0]]])
    truthy(res["feed"]["first_full_bar_open"] > first_seen, "[D3] 第一根半根 K 棒必須被排除")
    return (f"clean {s['clean_bars']} 根、volume_ratio 全 = 1.0、"
            f"PONG {sum(w.pongs for w in wss)} 次")


def _stage1_run(vol_scale, extra_argv=()):
    """跑一次 Stage 1 端到端，交易所 volume = 本地 volume × vol_scale。"""
    exch = FakeExchange(g2.INTERVAL_MS["1M"], vol_scale=vol_scale)

    async def fake_open(uri, ssl_ctx, open_timeout, ping_interval):
        return FakeWS(exch, tick=0.01, seed=5)

    def http(ctx, path, params):
        if path == "/api/v1/market/tickers":
            return FakeResp(TICKERS_PAYLOAD, headers={"Date": time.strftime(
                "%a, %d %b %Y %H:%M:%S GMT", time.gmtime())})
        if path == "/api/v1/market/klines":
            bar = g2.INTERVAL_MS["1M"]
            end = int(params["endTime"])
            return FakeResp(ok({"klines": exch.klines(
                params["symbol"], end - int(params["limit"]) * bar, end)}))
        return FakeResp({"result": False, "code": "X", "message": path})

    return run_stage(["--stage", "1", "--smoke", "--settle", "0", "--seal-grace", "1",
                      "--connections", "1", "--sub-rate", "20", "--max-minutes", "5",
                      *extra_argv],
                     http=http, ws=fake_open, clock=Clock(speed=60), no_sleep=True)


def t_stage1_double_side_volume():
    """交易所把買賣雙邊都算進 volume 時，volume_ratio 必須量到 0.5 並判成「疑似雙邊量」。

    這是 t_stage1_end_to_end 補不上的洞：那裡 local volume 恆等於 exchange volume，
    比值永遠 1.0，所以 lv/ev、ev/lv、直接寫死 1.0 三種實作長得一模一樣。
    這裡讓兩邊差兩倍，比值的**方向**才有意義（0.5 是本地少、2.0 是本地多）。
    """
    res, ctx = _stage1_run(vol_scale=2.0)
    s = res["summary"]
    truthy(s["clean_bars"] >= 2, f"至少要有 2 根可比對的 K 棒，實際 {s['clean_bars']}")
    close_to(s["volume_ratio"]["p50"], 0.5, 1e-9, "volume_ratio 中位數（本地 ÷ 交易所）")
    close_to(s["volume_ratio"]["min"], 0.5, 1e-9, "volume_ratio 最小值")
    close_to(s["volume_ratio"]["max"], 0.5, 1e-9, "volume_ratio 最大值")
    close_to(s["amount_ratio"]["p50"], 0.5, 1e-9, "amount_ratio 應同步減半")
    close_to(s["close_rel"]["p50"], 0.0, 1e-9, "價格沒動過，close 仍要完全一致")
    truthy("雙邊量" in res["verdict"]["label"],
           f"判定應為「疑似交易所計雙邊量」，實際 {res['verdict']['label']}")
    truthy("可還原" not in res["verdict"]["label"],
           "本地只拿到一半的量，絕不可判成可還原")
    b = res["bars"][0]
    close_to(b["diff"]["volume_ratio"], 0.5, 1e-9, "逐根比值")
    truthy(b["local"]["volume"] < b["exchange"]["volume"], "本地量應小於交易所量")
    return f"交易所量 = 本地 ×2 → volume_ratio 0.5、判定「{res['verdict']['label']}」"


def t_stage1_verdict_branches():
    """三個判定分支都要走得到，而且 label / next 兩兩不同。

    _stage1_verdict 決定「方案 A 生不生存」，它自己一定要有測試釘住，
    不能只靠端到端那一條「剛好比值是 1.0」的路徑。
    """
    def summ(p50, p05=None, p95=None, close_rel=0.0, n=10):
        p05 = p50 if p05 is None else p05
        p95 = p50 if p95 is None else p95
        return {"volume_ratio": {"n": n, "p50": p50, "p05": p05, "p95": p95},
                "close_rel": {"n": n, "p50": close_rel}}

    good = g2._stage1_verdict(summ(1.0))
    half = g2._stage1_verdict(summ(0.5))
    bad = g2._stage1_verdict(summ(0.3, 0.1, 0.9))
    none_ = g2._stage1_verdict({"volume_ratio": {"n": 0}})
    truthy("可還原（初判通過）" in good["label"], f"比值 1.0 → 可還原，實際 {good['label']}")
    truthy("雙邊量" in half["label"], f"比值 0.5 → 雙邊量，實際 {half['label']}")
    truthy("無法還原" in bad["label"], f"比值 0.3 → 無法還原，實際 {bad['label']}")
    truthy("無法判定" in none_["label"], f"沒有可比對 K 棒 → 無法判定，實際 {none_['label']}")
    labels = [good["label"], half["label"], bad["label"], none_["label"]]
    eq(len(set(labels)), 4, "四個分支的判定文字必須互不相同")
    eq(len(set(v["next"] for v in (good, half, bad, none_))), 4, "四個分支的下一步也要互不相同")
    # 邊界：p50 落在雙邊量帶的外緣，不可被吃進「可還原」
    truthy("可還原（初判通過）" not in g2._stage1_verdict(summ(0.55))["label"], "0.55 不是可還原")
    truthy("可還原（初判通過）" not in g2._stage1_verdict(summ(1.03))["label"], "1.03 已偏離")
    # close 對不上就算比值漂亮也不能通過
    truthy("可還原（初判通過）" not in g2._stage1_verdict(summ(1.0, close_rel=0.05))["label"],
           "收盤價對不上時不可判通過")
    return "4 個分支 + 3 個邊界都走得到，文字互不相同"


def t_stage1_smoke_verdict_is_marked():
    """--smoke 的判定文字不可以跟正式版長得一樣（成功與失敗兩個方向都要）。

    README 叫使用者「跑完看最後幾行」，冒煙用的是 1 分 K × 2 symbol × 1 根，
    統計上毫無意義；如果那幾行跟正式版一字不差，使用者會拿冒煙結果當結論。
    """
    ok_s = {"volume_ratio": {"n": 10, "p50": 1.0, "p05": 1.0, "p95": 1.0},
            "close_rel": {"n": 10, "p50": 0.0}}
    bad_s = {"volume_ratio": {"n": 10, "p50": 0.3, "p05": 0.1, "p95": 0.9},
             "close_rel": {"n": 10, "p50": 0.0}}
    for what, s in (("成功方向", ok_s), ("失敗方向", bad_s)):
        formal = g2._stage1_verdict(s, smoke=False)
        smoke = g2._stage1_verdict(s, smoke=True)
        truthy(smoke["label"] != formal["label"], f"{what}：冒煙的判定標籤不可與正式版相同")
        truthy(smoke["next"] != formal["next"], f"{what}：冒煙的下一步不可與正式版相同")
        truthy("不算數" in smoke["label"], f"{what}：標籤要講明不算數")
        truthy("正式版" in smoke["next"], f"{what}：下一步要叫人去跑正式版")
        truthy(smoke.get("smoke") is True, f"{what}：json 也要標記 smoke")
        truthy(formal.get("smoke") is None, f"{what}：正式版不可被標成 smoke")
    # 端到端也確認一次：--smoke 跑出來的 json 帶著這個標記
    res, ctx = _stage1_run(vol_scale=1.0)
    truthy(res["verdict"].get("smoke") is True, "--smoke 的 json verdict 要有 smoke 標記")
    truthy("不算數" in res["verdict"]["label"], "--smoke 的 json label 要講明不算數")
    return "冒煙判定在成功與失敗兩個方向都被標記、下一步指向正式版"


def t_stage1_ws_connect_fail():
    async def fake_open(uri, ssl_ctx, open_timeout, ping_interval):
        raise OSError("getaddrinfo failed")
    http, _ = make_http({"/api/v1/market/tickers": TICKERS_PAYLOAD})
    msg = raises_with(lambda: run_stage(["--stage", "1", "--smoke", "--max-minutes", "1"],
                                        http=http, ws=fake_open, clock=Clock(60), no_sleep=True),
                      "連不上 WebSocket", "--ca-bundle", "--ws-url")
    truthy((OUT / "stage1_aggregation.json").exists(), "失敗時仍要寫出 json")
    js = json.loads((OUT / "stage1_aggregation.json").read_text(encoding="utf-8"))
    truthy(js.get("fatal_error"), "json 要記下致命錯誤")
    return msg


def t_stage1_bad_payload():
    """payload 欄位對不上 → 指名道姓的訊息 + 原始訊息一定要落在 json 裡。"""
    class BadWS(FakeWS):
        async def __anext__(self):
            await asyncio.sleep(0.01)
            self.n += 1
            return json.dumps({"topic": "TRADE", "symbol": "AAA_USDT_PERP",
                               "data": [{"p": "1.0", "sz": "2.0"}]})

    async def fake_open(uri, ssl_ctx, open_timeout, ping_interval):
        return BadWS(FakeExchange(60_000))
    http, _ = make_http({"/api/v1/market/tickers": TICKERS_PAYLOAD})
    msg = raises_with(lambda: run_stage(["--stage", "1", "--smoke", "--max-minutes", "1",
                                         "--sub-rate", "20"],
                                        http=http, ws=fake_open, clock=Clock(60), no_sleep=True),
                      "拆不出", "raw_message_samples")
    js = json.loads((OUT / "stage1_aggregation.json").read_text(encoding="utf-8"))
    truthy(js["feed"]["raw_message_samples"], "原始訊息必須存進 json（錯誤訊息說了會存）")
    truthy("sz" in json.dumps(js["feed"]["raw_message_samples"], ensure_ascii=False),
           "原始訊息要看得到真正的欄位名")
    return msg


# ============================== 5. Stage 2 ==============================
def t_stage2_end_to_end():
    exch = FakeExchange(g2.INTERVAL_MS["1M"])

    async def fake_open(uri, ssl_ctx, open_timeout, ping_interval):
        return FakeWS(exch, tick=0.01, seed=7)
    http, _ = make_http({"/api/v1/market/tickers": TICKERS_PAYLOAD})
    res, ctx = run_stage(["--stage", "2", "--smoke", "--seal-grace", "1", "--sub-rate", "20",
                          "--max-minutes", "5", "--symbols", "AAA_USDT_PERP,BBB_USDT_PERP"],
                         http=http, ws=fake_open, clock=Clock(60), no_sleep=True)
    eq(len(res["bars"]), 2, "smoke 應收 2 根")
    b = res["bars"][0]
    has_keys(b, ["bar_open", "bar_close", "seal_ms", "data_ready_ms", "data_ready_ms_max",
                 "trades_after_seal", "compute_ms", "compute_cpu_ms", "compute_done_ms",
                 "total_ms", "per_symbol_data_ready_ms", "degraded"], "[D5] 延遲拆解欄位")
    # [D5] 事後真值與封存快照必須並列，否則 data_ready_ms 是右設限值卻看不出來
    has_keys(b, ["data_ready_ms_at_seal", "data_ready_ms_max_at_seal", "volume_after_seal_total",
                 "max_late_after_seal_ms"], "[D5] 封存快照 / 事後真值並列")
    truthy(b["compute_ms"] is not None and b["compute_ms"] > 0, "compute 步驟應真的跑到 s4_signal")
    truthy(b["symbols_with_trades"] >= 1, "應該有 symbol 收到成交")
    has_keys(res["summary"], ["data_ready_ms_max", "data_ready_ms_max_at_seal", "seal_ms",
                              "compute_ms", "total_ms",
                              "bars_with_trades_after_seal", "coverage_pct"], "summary")
    truthy("synthetic" in res["compute_history_source"], "office 沒有快取 → 應標示 synthetic")
    eq(res["warm_bars"], g2.warm_bars_needed(60), "1 分 K 的暖機根數要自動放大到 26 小時")
    has_keys(res["feed"]["clock_offset"], ["min_push_lag_ms", "max_push_lag_ms",
                                           "http_date_offset_ms"], "[D6] 時鐘偏移估計")
    return f"2 根、compute {b['compute_ms']:.1f} ms、暖機 {res['warm_bars']} 根（synthetic）"


def t_stage2_reconnect():
    """斷線 → 重連，期間的 K 棒必須被標成 degraded_ws_outage。

    斷線點用「開始收 K 棒後的真實經過秒數」觸發，不用訊息序號——序號會落在
    訂閱剛完成、還沒進入第一根完整 K 棒的時間帶，那時候斷線是不該有 K 棒被標記的。
    重連 backoff 是 1 秒真實時間 = 60 秒虛擬時間，剛好蓋掉一整根 1 分 K。
    """
    exch = FakeExchange(g2.INTERVAL_MS["1M"])
    state = {"n": 0, "t0": None, "failed": False}

    class FlakyWS(FakeWS):
        def __init__(self, *a, may_fail=False, **kw):
            FakeWS.__init__(self, *a, **kw)
            self.may_fail = may_fail

        async def __anext__(self):
            if (self.may_fail and not state["failed"]
                    and time.time() - state["t0"] > 2.0):
                state["failed"] = True
                raise ConnectionError("連線被重置")
            return await FakeWS.__anext__(self)

    async def fake_open(uri, ssl_ctx, open_timeout, ping_interval):
        state["n"] += 1
        if state["t0"] is None:
            state["t0"] = time.time()
        # 只有第一條連線會斷，重連後那條正常，才驗得到「斷 → 重連 → 續收」整條路徑
        return FlakyWS(exch, tick=0.01, seed=3, may_fail=state["n"] == 1)
    http, _ = make_http({"/api/v1/market/tickers": TICKERS_PAYLOAD})
    res, ctx = run_stage(["--stage", "2", "--interval", "1M", "--bars", "6",
                          "--connections", "1", "--seal-grace", "1", "--sub-rate", "20",
                          "--max-minutes", "20", "--symbols", "AAA_USDT_PERP"],
                         http=http, ws=fake_open, clock=Clock(60), no_sleep=True)
    truthy(res["feed"]["reconnects"] >= 1, f"應記錄重連，實際 {res['feed']['reconnects']}")
    truthy(res["feed"]["outages"], "應記錄斷線區間")
    deg = [b for b in res["bars"] if b["degraded"]]
    clean = [b for b in res["bars"] if not b["degraded"]]
    truthy(deg, "斷線期間的 K 棒要標 degraded")
    truthy(clean, "斷線以外的 K 棒不該被標 degraded（否則等於全部作廢）")
    return (f"重連 {res['feed']['reconnects']} 次、斷線 {len(res['feed']['outages'])} 段、"
            f"{len(deg)}/{len(res['bars'])} 根標 degraded")


# ============================== 6. Stage 3 ==============================
def t_stage3_contract():
    res, ctx = run_stage(["--stage", "3", "--source", "synthetic", "--n-frames", "40"])
    ic = res["input_contract"]
    truthy(ic["bars_per_hour_is_int"], "AC-3：bars_per_hour 必須是整數")
    truthy(ic["time_monotonic_all"], "AC-3：time 必須升冪")
    truthy(ic["fixed_period_all"], "AC-3：必須固定週期、無缺漏")
    eq(ic["nan_feature_rows_total"], ic["expected_nan_rows_total"], "暖機 NaN 根數")
    eq(res["frames_measured"], 40, "frame 數")
    eq(res["bars_per_hour"], 12, "5M → 12")
    truthy(res["signals_found"] > 0, "合成資料要能產生訊號，否則量到的是全 NaN 的空路徑")
    truthy(res["per_frame_ms_stats"]["n"] == 40, "每個 frame 的原始耗時都要輸出")
    has_keys(res["projection"], ["universe_611_wall_s", "bar_period_s", "fits_in_bar"], "推估")
    return f"40 frames、訊號 {res['signals_found']} 筆、NaN {ic['nan_feature_rows_total']} 列符合預期"


def t_stage3_uses_real_s4():
    """AC-1/AC-3：必須是 import s4_signal，不是自己抄一份。把 s4 換掉，Stage 3 就要跟著改變。"""
    calls = {"n": 0}
    real = g2.s4_signal.features

    def spy(df, bph):
        calls["n"] += 1
        return real(df, bph)
    g2.s4_signal.features = spy
    try:
        run_stage(["--stage", "3", "--source", "synthetic", "--n-frames", "5", "--warmup-frames", "0"])
    finally:
        g2.s4_signal.features = real
    truthy(calls["n"] >= 5, f"s4_signal.features 應被呼叫至少 5 次，實際 {calls['n']}")
    src = (Path(__file__).resolve().parent / "g2_measure.py").read_text(encoding="utf-8")
    for formula in ["shift(2 * H)", "rolling(24 * H)", "(c - l) / (h - l)"]:
        truthy(formula not in src, f"g2_measure.py 不該出現指標公式 {formula!r}（必須 import s4_signal）")
    return f"features() 被呼叫 {calls['n']} 次、腳本內沒有重抄的指標公式"


def t_stage3_cache_missing():
    return raises_with(lambda: run_stage(["--stage", "3", "--source", "cache",
                                          "--cache-dir", str(TMP / "nope")]),
                       "找不到快取目錄", "pionex_cache", "--source synthetic")


def t_stage3_frame_too_short():
    return raises_with(lambda: run_stage(["--stage", "3", "--source", "synthetic",
                                          "--n-frames", "2", "--frame-bars", "100"]),
                       "--frame-bars", "暖機")


def t_stage3_auto_fallback():
    """--source auto 且沒有快取 → 應退回 synthetic 並留下提示，而不是直接死掉。"""
    res, ctx = run_stage(["--stage", "3", "--source", "auto", "--n-frames", "5",
                          "--cache-dir", str(TMP / "nope")])
    truthy("synthetic" in res["data_source"], "應退回合成資料")
    truthy(any("快取" in n for n in ctx.notes), "應留下提示")
    return "auto 找不到快取 → 退回 synthetic 並提示"


# ============================== 7. Stage 4 ==============================
def build_synth_cache():
    """做一份合成快取，裡面刻意「種」一個必然被粗篩漏掉的訊號。

    SLEEP_USDT_PERP：平時 24h 成交額約 1.9 萬（低於 MIN_TURN24H=2 萬），
      爆量那一根才把 24h 成交額推過 2 萬 → 全量算得到訊號，粗篩看上一根會漏掉。
      這正是 PRD §1.3 方案 B 的核心風險，Stage 4 必須量得到它。
    NORM_USDT_PERP：平時就在區間內 → 粗篩抓得到。
    """
    CACHE.mkdir(parents=True, exist_ok=True)
    bar = g2.INTERVAL_MS["5M"]
    end = 1_789_000_000_000 // bar * bar
    n = 340
    # seed 寫死：Python 的 hash(str) 每個 process 都不一樣，用它會讓這個測試時好時壞。
    # 這兩組 seed 是實際挑過的——SLEEP 在事件前一根 turn=19980（<2 萬），
    # 事件當根 turn=21773（進區間），剛好構成「粗篩看上一根會漏掉」的情境。
    specs = [("NORM_USDT_PERP", 200.0, 4.0, 11), ("SLEEP_USDT_PERP", 64.0, 25.0, 22)]
    made = {}
    for name, bv, vm, seed in specs:
        rng = np.random.default_rng(seed)
        df = g2.synth_frame(n, bar, end, rng, base_price=1.0, base_vol=bv,
                            event_idx=[n - 4], event_ret=0.16, event_vol_mult=vm,
                            bars_per_hour=12)
        df.to_csv(CACHE / f"{name}_5M.csv", index=False)
        made[name] = df
    return made


def t_stage4_missed_signal():
    build_synth_cache()
    res, ctx = run_stage(["--stage", "4", "--cache-dir", str(CACHE)])
    truthy(res["baseline_signals"] >= 2, f"baseline 應至少 2 筆，實際 {res['baseline_signals']}")
    m1 = [m for m in res["margin_sweep"] if m["margin"] == 1.0][0]
    truthy(m1["signals_missed"] >= 1, f"應該量到至少 1 筆被粗篩漏掉的訊號，實際 {m1['signals_missed']}")
    truthy(m1["signals_caught"] >= 1, f"也要有抓到的，實際 {m1['signals_caught']}")
    miss = res["missed_signals"][0]
    eq(miss["symbol"], "SLEEP_USDT_PERP", "漏掉的應該是種下去的那個")
    truthy(miss["turn_at_t"] >= 20000, "訊號當根的 24h 成交額應已進入區間")
    truthy(miss["turn_at_t_minus_lag"] < 20000, "上一根還在區間外 → 這就是漏掉的原因")
    truthy("MIN_TURN24H" in miss["reason"], "漏掉原因要寫清楚")
    # 放寬 margin 應該把它救回來
    m05 = [m for m in res["margin_sweep"] if m["margin"] == 0.5][0]
    truthy(m05["signals_missed"] < m1["signals_missed"], "放寬粗篩區間應減少漏失")
    has_keys(res["candidates_per_bar"], ["p50", "p95", "rest_seconds_at_10rps_p95"], "候選數分布")
    eq(res["screen_lag_bars"], 1, "[D7] 預設 lag=1")
    return (f"baseline {res['baseline_signals']} 筆、margin1.0 漏 {m1['signals_missed']} 筆、"
            f"margin0.5 漏 {m05['signals_missed']} 筆")


def t_stage4_lag0_no_miss():
    """反向對照：lag=0（等於用當根資料粗篩）在定義上不可能漏，
       若這裡漏了就代表 [D7] 的比較基準寫錯了。"""
    build_synth_cache()
    res, ctx = run_stage(["--stage", "4", "--cache-dir", str(CACHE), "--screen-lag", "0"])
    m1 = [m for m in res["margin_sweep"] if m["margin"] == 1.0][0]
    eq(m1["signals_missed"], 0, "lag=0 時粗篩條件與訊號條件完全相同，不該有漏失")
    return "lag=0 → 漏失 0 筆（比較基準的定義自洽）"


def t_stage4_lag_sweep():
    """[D7] 漏失率對 LAG 不單調，所以一次輸出 LAG=0/1/2，不再只給單一預設值。"""
    build_synth_cache()
    res, ctx = run_stage(["--stage", "4", "--cache-dir", str(CACHE)])
    eq(res["screen_lags_swept"], [0, 1, 2], "預設要同時掃 LAG 0/1/2")
    by_lag = {r["screen_lag_bars"]: r for r in res["lag_sweep"]}
    eq(sorted(by_lag), [0, 1, 2], "lag_sweep 三列都要在")
    for lag, row in by_lag.items():
        eq([m["margin"] for m in row["margin_sweep"]], [1.0, 0.75, 0.5, 0.25],
           f"LAG={lag} 的 margin 掃描要四段都有")
    m1 = {lag: [x for x in row["margin_sweep"] if x["margin"] == 1.0][0]
          for lag, row in by_lag.items()}
    eq(m1[0]["signals_missed"], 0, "LAG=0 粗篩條件與訊號條件相同，定義上不可能漏")
    truthy(m1[1]["signals_missed"] >= 1, "LAG=1 要量得到種下去的那個漏失")
    eq(sum(1 for r in res["lag_sweep"] if r["is_report_lag"]), 1, "只能有一列標成主值")
    eq(res["margin_sweep"], by_lag[res["screen_lag_bars"]]["margin_sweep"],
       "頂層 margin_sweep 必須等於主值那一列（複本不可漂移）")
    eq(res["missed_signals_total"], by_lag[res["screen_lag_bars"]]["missed_signals_total"],
       "頂層 missed_signals_total 也是主值那一列")
    eq(by_lag[1]["missed_signals"][0]["screen_lag_bars"], 1, "漏訊號明細要自帶 LAG，不然混在一起分不出")
    eq(by_lag[0]["missed_signals_total"], 0, "LAG=0 不該有任何漏訊號明細")
    return (f"LAG 0/1/2 漏失 {m1[0]['signals_missed']}/{m1[1]['signals_missed']}/"
            f"{m1[2]['signals_missed']} 筆（margin=1.0）")


def t_stage4_cache_missing():
    return raises_with(lambda: run_stage(["--stage", "4", "--cache-dir", str(TMP / "nope")]),
                       "找不到快取目錄", "laptop")


def t_stage4_empty_cache():
    d = TMP / "empty_cache"
    d.mkdir(parents=True, exist_ok=True)
    return raises_with(lambda: run_stage(["--stage", "4", "--cache-dir", str(d)]),
                       "沒有任何", "--interval")


# ============================== 8. CLI / 其他 ==============================
def t_cli_parses_every_stage():
    p = g2.build_parser()
    for st in range(5):
        a = g2.apply_defaults(p.parse_args(["--stage", str(st)]))
        eq(a.stage, st, "stage")
        a = g2.apply_defaults(p.parse_args(["--stage", str(st), "--smoke"]))
        truthy(a.smoke, "smoke")
    a = g2.apply_defaults(p.parse_args(["--stage", "1", "--smoke"]))
    eq((a.interval, a.bars, a.n_symbols), ("1M", 1, 2), "Stage 1 冒煙規模")
    a = g2.apply_defaults(p.parse_args(["--stage", "2", "--smoke"]))
    eq((a.interval, a.bars, a.max_symbols), ("1M", 2, 4), "Stage 2 冒煙規模")
    a = g2.apply_defaults(p.parse_args(["--stage", "1"]))
    eq((a.interval, a.bars), ("5M", 4), "Stage 1 正式規模")
    a = g2.apply_defaults(p.parse_args(["--stage", "2"]))
    eq(a.bars, 20, "Stage 2 正式規模 20 根")
    help_text = p.format_help()
    for frag in ["--stage", "--smoke", "--ca-bundle", "--max-requests", "--cache-dir",
                 "--screen-lag", "research/README_G2.md"]:
        truthy(frag in help_text, f"--help 應提到 {frag}")
    return "5 個 stage × (正常 / --smoke) 都解析得出來，--help 內容齊全"


def t_backup_not_overwrite():
    """FR-4：既有結果不可被靜默覆寫。"""
    OUT.mkdir(parents=True, exist_ok=True)
    p = OUT / "stage3_compute.json"
    p.write_text('{"marker": "old"}', encoding="utf-8")
    run_stage(["--stage", "3", "--source", "synthetic", "--n-frames", "3"])
    new = json.loads(p.read_text(encoding="utf-8"))
    truthy("marker" not in new, "新結果應寫入")
    backups = list((TMP / "tmp").glob("stage3_compute.*.json"))
    truthy(backups, "舊結果應被搬到 tmp 目錄保存")
    truthy(any("old" in b.read_text(encoding="utf-8") for b in backups), "備份內容要是舊的那份")
    return f"舊檔已備份 {len(backups)} 份，沒有被覆寫"


def t_json_is_clean():
    """office 端要能直接 json.load：不可出現 NaN / Infinity 這種非法 JSON。"""
    res, ctx = run_stage(["--stage", "3", "--source", "synthetic", "--n-frames", "3"])
    raw = (OUT / "stage3_compute.json").read_text(encoding="utf-8")
    for bad in ("NaN", "Infinity", "-Infinity"):
        truthy(bad not in raw, f"json 不該出現 {bad}")
    json.loads(raw)
    b = (OUT / "stage3_compute.json").read_bytes()
    eq(b.count(b"\r\n"), 0, "輸出應為 LF 行尾")
    truthy(b.endswith(b"\n"), "檔案要以換行結尾（POSIX 慣例，git diff 才不會抱怨）")
    return "合法 JSON、UTF-8、LF 行尾、結尾有換行"


def t_method_notes_are_the_docstring():
    """每個 json 的 method_notes 必須是 docstring 的**原文**，不是另外手寫的摘要。

    手寫摘要與 docstring 是兩份會各自漂移的文字，而真正被帶回 office 判讀的是 json。
    """
    eq(sorted(g2.DOC_BLOCKS), ["D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8"],
       "docstring 的量測定義區塊")
    for key, block in g2.DOC_BLOCKS.items():
        truthy(block.startswith(f"[{key}] "), f"{key} 區塊要從標題行開始")
        truthy(block in g2.__doc__, f"{key} 區塊必須逐字出現在 docstring 裡")
        truthy(len(block.splitlines()) >= 3, f"{key} 區塊看起來被截斷了（只有 {len(block.splitlines())} 行）")
    for stage, notes in g2.METHOD_NOTES.items():
        for note in notes:
            truthy("抽取失敗" not in note, f"Stage {stage} 的 method_notes 抽取失敗，退回了錯誤訊息")
    truthy(g2.DOC_BLOCKS["D7"] in g2.METHOD_NOTES[4], "Stage 4 的 method_notes 要帶 [D7] 原文")
    truthy(g2.DOC_BLOCKS["D8"] in g2.METHOD_NOTES[3], "Stage 3 的 method_notes 要帶 [D8] 原文")
    truthy(g2.DOC_BLOCKS["D5"] in g2.METHOD_NOTES[2], "Stage 2 的 method_notes 要帶 [D5] 原文")
    truthy(g2.DOC_BLOCKS["D6"] in g2.METHOD_NOTES[2], "Stage 2 的 method_notes 要帶 [D6] 原文")
    # [D7] 的偏誤描述曾經寫成「下界」，那是錯的（兩個方向相反的誤差同時存在）
    d7 = g2.DOC_BLOCKS["D7"]
    truthy("雙向有偏" in d7, "[D7] 要說明偏誤是雙向的")
    truthy("下界" not in d7.replace("「下界」", "").replace("不可當上界也不可當下界", ""),
           "[D7] 不可再把漏失率描述成下界")
    return f"{len(g2.DOC_BLOCKS)} 個定義區塊全部來自 docstring 原文"


def t_metadata_script_fingerprint():
    """FR-6：research/ 還沒進版控時 git_commit 指不出腳本版本，必須靠 script_sha256 認版本。"""
    http, _ = make_http({"/api/v1/common/symbols": SYMBOLS_PAYLOAD,
                         "/api/v1/market/tickers": TICKERS_PAYLOAD,
                         "/api/v1/common/riskTable": RISK_PAYLOAD})
    res, ctx = run_stage(["--stage", "0"], http=http, no_sleep=True)
    md = res["metadata"]
    has_keys(md, ["script_sha256", "git_commit", "git_commit_note"], "腳本指紋")
    want = hashlib.sha256(Path(g2.__file__).read_bytes()).hexdigest()
    eq(md["script_sha256"], want, "script_sha256 要等於 g2_measure.py 的內容雜湊")
    eq(len(md["script_sha256"]), 64, "sha256 長度")
    return f"script_sha256 = {want[:16]}…"


def t_main_unexpected_error_is_actionable():
    """FR-5 兜底：預期外的例外也要講清楚「跑到哪、東西在哪、要帶什麼回來」。

    既有的 BudgetStop / G2Error / KeyboardInterrupt 分支不可被這個兜底吃掉。
    """
    argv = ["--stage", "3", "--out-dir", str(OUT), "--tmp-dir", str(TMP / "tmp"),
            "--machine", "selftest", "--source", "synthetic", "--n-frames", "3"]

    def run_with(boom):
        buf = io.StringIO()
        old = g2.stage3
        g2.stage3 = boom
        try:
            with contextlib.redirect_stderr(buf), contextlib.redirect_stdout(io.StringIO()):
                rc = g2.main(argv)
        finally:
            g2.stage3 = old
        return rc, buf.getvalue()

    def unexpected(ctx):
        raise ZeroDivisionError("division by zero")
    rc, err = run_with(unexpected)
    eq(rc, 4, "預期外錯誤要有自己的離開碼（不可跟 G2Error 的 2 混在一起）")
    truthy("Traceback" in err, "要保留 traceback，那是唯一線索")
    for frag in ["ZeroDivisionError", "stage3_compute.json", "帶回", "script_sha256"]:
        truthy(frag in err, f"錯誤訊息要提到 {frag}；實際：{err[-300:]}")

    # 既有分支的行為不可被改壞
    def g2err(ctx):
        raise g2.G2Error("找不到快取目錄 xxx\n→ 請在 laptop 上先跑一次回測")
    rc, err = run_with(g2err)
    eq(rc, 2, "G2Error 仍要是 exit 2")
    truthy("Traceback" not in err, "已預期的錯誤不該噴 traceback（那是給使用者看的可操作指引）")
    truthy("找不到快取目錄" in err, "G2Error 的原訊息要照原樣印出")

    def budget(ctx):
        raise g2.BudgetStop("已達 --max-requests 上限")
    rc, err = run_with(budget)
    eq(rc, 3, "BudgetStop 仍要是 exit 3")
    truthy("--max-requests" in err, "BudgetStop 要提示怎麼繼續")

    def ctrlc(ctx):
        raise KeyboardInterrupt()
    rc, err = run_with(ctrlc)
    eq(rc, 130, "Ctrl-C 仍要是 exit 130")
    truthy("Traceback" not in err, "Ctrl-C 不該噴 traceback")
    return "預期外 → exit 4 + 檔案位置 + 指紋；既有 2/3/130 三條分支沒被改壞"


def t_prepare_frame_fills_gaps():
    """AC-3：補洞規則要與 pionex_backtest.load_hourly 一致，且補幾根要算得出來。"""
    bar = 300_000
    t0 = 1_700_000_000_000 // bar * bar
    df = pd.DataFrame({"time": [t0, t0 + bar, t0 + 4 * bar],
                       "open": [1, 2, 5], "high": [1, 2, 5], "low": [1, 2, 5],
                       "close": [1.0, 2.0, 5.0], "volume": [10, 20, 50]})
    out, filled = g2.prepare_frame(df, bar)
    eq(len(out), 5, "應補成 5 根連續 K 棒")
    eq(filled, 2, "補了 2 根")
    eq(list(out["close"]), [1.0, 2.0, 2.0, 2.0, 5.0], "close 沿用前值")
    eq(list(out["volume"]), [10, 20, 0, 0, 50], "補出來的根 volume=0")
    eq(list(out["open"][2:4]), [2.0, 2.0], "open 跟著 close")
    truthy(bool((out["time"].diff().dropna() == bar).all()), "固定週期")
    return "補洞規則與計數正確"


def t_bars_per_hour():
    eq(g2.bars_per_hour_of("5M"), 12, "5M")
    eq(g2.bars_per_hour_of("15M"), 4, "15M")
    eq(g2.bars_per_hour_of("60M"), 1, "60M")
    eq(g2.bars_per_hour_of("1M"), 60, "1M")
    for iv in ("1M", "5M", "15M", "60M"):
        truthy(isinstance(g2.bars_per_hour_of(iv), int), f"{iv} 必須是 int（s4_signal 不收 float）")
    return "5M→12、15M→4、60M→1，全部是 int"


class BlockImport:
    """把某個套件從 import 路徑上藏起來，用來驗『套件沒裝』的錯誤訊息。"""

    def __init__(self, name):
        self.name = name
        self.saved = {}

    def find_spec(self, fullname, path=None, target=None):
        if fullname == self.name or fullname.startswith(self.name + "."):
            raise ImportError(f"blocked {fullname}")
        return None

    def __enter__(self):
        for k in list(sys.modules):
            if k == self.name or k.startswith(self.name + "."):
                self.saved[k] = sys.modules.pop(k)
        sys.meta_path.insert(0, self)
        return self

    def __exit__(self, *a):
        sys.meta_path.remove(self)
        sys.modules.update(self.saved)
        return False


def t_require_deps_missing():
    """FR-5：laptop 很可能沒裝 websockets。這時候要直接說『去 pip install』，
       而不是等跑到連線那一步再報成『連不上 WebSocket』害人去查網路。"""
    with BlockImport("websockets"):
        msg = raises_with(lambda: g2.require_deps(1), "沒有裝", "pip install", "websockets")
    g2.require_deps(1)       # 解除封鎖後要能通過，否則等於永遠擋著
    g2.require_deps(3)       # 不需網路的階段不該檢查網路套件
    return msg


def t_max_requests_budget():
    """FR-3：--max-requests 達到就停，並保留已有結果。"""
    http, _ = make_http({"/api/v1/common/symbols": SYMBOLS_PAYLOAD,
                         "/api/v1/market/tickers": TICKERS_PAYLOAD,
                         "/api/v1/common/riskTable": RISK_PAYLOAD})
    args = g2.apply_defaults(g2.build_parser().parse_args(
        ["--stage", "0", "--max-requests", "1", "--out-dir", str(OUT), "--tmp-dir", str(TMP / "tmp")]))
    with Patch(_http_get_raw=http, sleep=lambda s: None):
        ctx = g2.Ctx(args)
        try:
            g2.stage0(ctx)
        except g2.BudgetStop as e:
            eq(ctx.req_count, 1, "應該只打 1 次就停")
            return f"達上限即停：{e}"
    raise AssertionError("超過 --max-requests 應丟 BudgetStop")


# ============================== main ==============================
def main():
    if TMP.exists():
        shutil.rmtree(TMP, ignore_errors=True)
    OUT.mkdir(parents=True, exist_ok=True)
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    print(f"G2 離線自檢（完全不連網）　python {sys.version.split()[0]}　"
          f"pandas {pd.__version__}　numpy {np.__version__}")
    print(f"產物目錄：{TMP}")

    groups = [
        ("聚合器（K 棒邊界 / 亂序 / 去重 / 封存）", [
            ("Aggregator 邊界左閉右開", t_agg_boundary),
            ("Aggregator 亂序與重複成交", t_agg_out_of_order_and_dupe),
            ("Aggregator 封存與晚到計數", t_agg_seal_and_late),
            ("data_ready_ms 取最後一筆到達", t_agg_data_ready_ms),
            ("封存後晚到成交回填輸出", t_agg_late_arrivals_backfill),
            ("[D6] push lag 區間含負值", t_agg_clock_offset),
            ("[D3] 觀測窗守衛的值域", t_first_full_bar_open),
            ("Aggregator taker 買賣拆分", t_agg_taker_split),
        ]),
        ("WS 訊息解析", [
            ("TRADE / PING / ack / error / 壞欄位", t_parse_messages),
        ]),
        ("Stage 0（mock REST）", [
            ("正常流程與 metadata", t_stage0_ok),
            ("429 退避降速", t_stage0_429),
            ("riskTable 失敗不致命", t_stage0_risktable_nonfatal),
            ("tickers 缺成交額欄位", t_stage0_bad_tickers),
            ("SSL 攔截的錯誤訊息", t_stage0_ssl_error),
            ("回應不是 JSON", t_stage0_html_response),
            ("--ca-bundle 檔案不存在", t_ca_bundle_missing),
            ("--max-requests 上限", t_max_requests_budget),
        ]),
        ("Stage 1（假交易所端到端）", [
            ("聚合值 vs 交易所值完全一致", t_stage1_end_to_end),
            ("交易所計雙邊量 → 比值 0.5", t_stage1_double_side_volume),
            ("判定的四個分支", t_stage1_verdict_branches),
            ("冒煙判定必須被標記", t_stage1_smoke_verdict_is_marked),
            ("WS 連不上", t_stage1_ws_connect_fail),
            ("payload 欄位對不上", t_stage1_bad_payload),
        ]),
        ("Stage 2（延遲拆解）", [
            ("延遲欄位與 compute 步驟", t_stage2_end_to_end),
            ("斷線重連與 degraded 標記", t_stage2_reconnect),
        ]),
        ("Stage 3（不連網）", [
            ("輸入契約與 NaN 根數", t_stage3_contract),
            ("確實 import s4_signal 而非重抄", t_stage3_uses_real_s4),
            ("--source cache 但快取不存在", t_stage3_cache_missing),
            ("--frame-bars 太短", t_stage3_frame_too_short),
            ("--source auto 退回 synthetic", t_stage3_auto_fallback),
        ]),
        ("Stage 4（粗篩漏訊號）", [
            ("種一個必漏訊號並量到它", t_stage4_missed_signal),
            ("lag=0 反向對照（不該有漏失）", t_stage4_lag0_no_miss),
            ("LAG 0/1/2 敏感度掃描", t_stage4_lag_sweep),
            ("快取目錄不存在", t_stage4_cache_missing),
            ("快取目錄是空的", t_stage4_empty_cache),
        ]),
        ("共用機制", [
            ("套件沒裝時的訊息", t_require_deps_missing),
            ("CLI 參數與 --help", t_cli_parses_every_stage),
            ("既有結果不被覆寫", t_backup_not_overwrite),
            ("輸出是合法 JSON / LF", t_json_is_clean),
            ("method_notes 就是 docstring 原文", t_method_notes_are_the_docstring),
            ("metadata 帶腳本指紋", t_metadata_script_fingerprint),
            ("預期外例外也要可操作", t_main_unexpected_error_is_actionable),
            ("prepare_frame 補洞", t_prepare_frame_fills_gaps),
            ("bars_per_hour 是整數", t_bars_per_hour),
        ]),
    ]
    for title, tests in groups:
        print(f"\n[{title}]")
        for name, fn in tests:
            check(name, fn)

    ok_n = sum(1 for r in RESULTS if r[0])
    bad = [r for r in RESULTS if not r[0]]
    print("\n" + "=" * 72)
    print(f"結果：{ok_n} 通過 / {len(bad)} 失敗 / 共 {len(RESULTS)} 項，"
          f"耗時 {sum(r[3] for r in RESULTS):.1f} 秒")
    if bad:
        print("\n失敗清單：")
        for _, name, detail, _ in bad:
            print(f"  - {name}\n      {detail.splitlines()[0] if detail else ''}")
        print("\n→ 自檢沒過就不要去跑真實量測，先把問題修掉（或回報給開發者）。")
        return 1
    print("全部通過。可以按 research/README_G2.md 的順序開始實際量測。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
