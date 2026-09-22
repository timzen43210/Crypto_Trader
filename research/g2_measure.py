#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
G2 — 5 分 K 即時資料層可行性量測
================================
回答一個問題：**5 分 K 收盤後，能否在可接受延遲內產出與交易所 K 棒一致的訊號？**

本腳本在 laptop 上獨立執行（不需要 Claude），分 5 個階段，每階段獨立可跑、獨立輸出：

    Stage 0  環境探測與候選清單          research/results/stage0_universe.json
    Stage 1  TRADE stream volume 還原驗證 research/results/stage1_aggregation.json
    Stage 2  訂閱容量與端到端延遲         research/results/stage2_latency.json
    Stage 3  指標計算耗時（不需網路）     research/results/stage3_compute.json
    Stage 4  方案 B 粗篩與漏訊號率        research/results/stage4_screen.json

用法見 research/README_G2.md。先跑 `python research/g2_selftest.py` 做離線自檢。


=========================== 量測定義（這一節最重要）===========================
下面每一條都是「可以被質疑、被驗證」的明確定義。數字的意義完全取決於這些定義，
分析階段（G2b）請先讀這裡再讀 json。
每個 json 的 `method_notes` 就是**從這一節程式化抽出來的同一份原文**（見 `_doc_blocks()`），
不是另外手寫的摘要——兩份文字遲早會漂移，而真正被帶回 office 判讀的是 json。

[D1] K 棒的時間基準
     派網 klines 的 `time` 欄位是**開盤時刻**（epoch 毫秒，UTC）。
     證據：pionex_backtest.load_hourly 用 `df["time"] + BAR <= now_ms` 丟掉未收完的 K 棒，
     且 pionex_backtest.backtest 用 `entry_ms = t[i] + BAR` 當進場時刻。
     因此：bar_close = bar_open + BAR。本腳本全程沿用這個定義。

[D2] 成交歸屬哪一根 K 棒
     一筆成交時間戳 ts 歸屬於 bar_open = floor(ts / BAR) * BAR，
     也就是區間 **[bar_open, bar_open + BAR)**（左閉右開）。
     BAR 能整除一小時，epoch 又從 UTC 午夜起算，所以 epoch 對齊 == UTC 時鐘對齊，
     不存在時區造成的邊界偏移。
     **歸屬用的是交易所給的成交時間戳，不是本地收到的時刻**——後者只用來算延遲。

[D3] 哪些 K 棒可以拿來比對（Stage 1 的 off-by-one 防線）
     只比對「完整被觀測到」的 K 棒：
         bar_open >= ceil_to_bar(訂閱完成時刻 + FIRST_BAR_GUARD_MS)
     訂閱完成當下那根一定是半根，必須丟掉；若訂閱完成時距離下一個邊界不到
     FIRST_BAR_GUARD_MS（預設 5 秒），連下一根也一起丟。
     這條規則只在 `first_full_bar_open()` 一個地方實作，守衛時間的值本身有自檢釘住
     （把 5000 改成 0 或 1000 都會讓 g2_selftest.py 變紅）。
     任何與 WS 斷線區間重疊的 K 棒標記 `degraded_ws_outage`，不列入 clean 統計。

[D4] 本地聚合的 OHLCV 怎麼算
     open  = 成交時間戳最小的那筆成交價（同毫秒時以到達順序決勝）
     close = 成交時間戳最大的那筆成交價（同上）
     high / low = 該根所有成交價的極值
     volume = Σ size（基礎幣數量）
     amount = Σ price × size（計價幣成交額）
     另外拆出 taker_buy_volume / taker_sell_volume，
     讓分析端能判斷「交易所 volume 是否等於 taker 單邊量」還是「雙邊都算」。

[D5] 延遲的起算點（Stage 2）
     一律以 **K 棒收盤時刻 bar_close（交易所時間基準）** 為 0 點，不是「收到第一筆推送」。
     逐根輸出下列原始時間戳，不只摘要：
         data_ready_ms   = max(該根所有成交的 local_recv_ms) - bar_close
                           → 「要等多久才拿得到這根的全部成交」。這是資料面的真延遲。
         seal_ms         = seal_recv_ms - bar_close
                           → 本腳本實際封存這根的時刻（由本地時鐘 + --seal-grace 決定）
         compute_ms      = compute_done_ms - seal_recv_ms
                           → 全市場跑完 s4_signal 的耗時
         total_ms        = compute_done_ms - bar_close
     注意 data_ready_ms / seal_ms / total_ms 是**混合時間基準**（本地時鐘 vs 交易所時鐘），
     所以同時輸出時鐘偏移估計 `clock_offset`（見 [D6]），分析時要自行決定是否扣掉。
     腳本**不自動扣**，避免把估計誤差混進原始數字。

     **封存快照 vs 事後真值（右設限問題）**
     `Aggregator.seal()` 交出去的是「封存當下」的快照，那正是實盤系統在 --seal-grace
     到期時手上真的有的東西。但封存之後才抵達、時間戳仍屬於這根的成交，會讓快照裡的
     data_ready_ms 與 volume 被**右設限**（censored）——量到的是「我停止等待之前已經
     等到多少」，不是「資料多久才齊」，而且 grace 設得越小數字越漂亮。
     因此收集結束後一律再回填一次事後真值（`Aggregator.apply_late_arrivals()`），
     逐根**同時**輸出兩組，絕不只給一組：
         volume / amount / trades / data_ready_ms          → 事後真值（含封存後才到的成交）
         volume_at_seal / ... / data_ready_ms_at_seal      → 封存當下的快照（右設限值）
         trades_after_seal / max_late_after_seal_ms        → 差幾筆、最晚差多久
     `trades_after_seal > 0` 就代表 --seal-grace 不夠，該根的 *_at_seal 只能當下界看。
     這條同樣適用於 Stage 1：本地 volume 若被右設限，volume_ratio 會假性 < 1，
     長得跟「交易所計雙邊量」一模一樣，所以兩組比值（volume_ratio 與
     volume_ratio_at_seal）必須並列，判讀才分得出來。

[D6] 本地時鐘與交易所時鐘的偏移估計
     兩個獨立估計，都輸出，不互相覆蓋：
       (a) http_date_offset_ms：REST 回應 Header `Date` 減去 (送出+收到)/2。解析度只有 1 秒。
       (b) min_push_lag_ms：所有成交的 min(local_recv_ms - trade_ts_ms)。
           這是「時鐘偏移 + 最小單向網路延遲」的上界估計；若它是負數，代表本地時鐘快了。
     判讀：|clock_offset| 若與量到的延遲同數量級，該批延遲數字就不可信，要先校時再重跑。

[D7] Stage 4 漏訊號率的比較基準
     baseline（全量）：對每個 symbol 的完整歷史跑 s4_signal.signal()，取所有 -1 的 (symbol, bar)。
     screened（粗篩後）：在 bar t 要決定「打哪些 symbol 的 klines」時，你只能用
       **t 之前**的資料，所以粗篩用的是 `turn` 在 bar t-LAG 的值。
       通過粗篩 = MIN_TURN24H×margin <= turn[t-LAG] <= MAX_TURN24H/margin。
     missed = baseline 有、screened 沒有。
     **這個量測的是粗篩的「時間落後」造成的漏失**（典型情境：低成交額的幣被一根爆量
     拉進 [MIN, MAX] 區間，但粗篩看的是上一根，還在區間外 → 整個漏掉）。

     **LAG 不是「取大一點比較保守」的參數，所以固定輸出一條敏感度曲線。**
     turn 是 24 小時滾動和，一筆大成交額**滾入**與**滾出**窗口是兩個不同時刻的事件，
     turn[t-k] 會在區間內外之間來回震盪，漏失率對 LAG **不單調**（實測 LAG=1 漏 75%、
     LAG=2 反而只漏 50%）。因此本階段一律同時算 LAG = 0 / 1 / 2 三組（--screen-lags），
     全部寫進 `lag_sweep`，不再只給單一預設值。判讀請看整條曲線，不要只取一個數字。

     **偏誤方向：雙向有偏、偏移量未知——「下界」與「上界」都不成立。**
     有兩個方向相反的誤差來源同時作用在這個數字上：
       (1) LAG 的選擇。實盤粗篩用的 tickers 快照是在 K 棒收盤當下取得的，它的 24 小時
           窗口結束於收盤時刻，與 turn[t]（288 根滾動和，窗口同樣結束於收盤時刻）是
           **同一個窗口**，所以現實接近 LAG=0；取 LAG>=1 只會單調增加漏失。
           → 這個方向讓量到的數字**高於**真實的時間落後效應。
       (2) 未被量測的「定義落差」。實盤粗篩讀的是交易所的 24h amount 欄位，與本腳本用
           klines 滾動加總算出來的 turn 不是同一個東西，這個落差會**額外增加**漏失。
           → 這個方向讓真實的總漏失**高於**量到的數字。
     兩者方向相反且偏移量未知，因此不要把任何一組數字當成單邊的界。(2) 只能在 laptop 上
     把 Stage 0 的 tickers 與同時刻的 turn 並列才驗得出來，不在本次量測範圍內。

[D8] Stage 3 計時的邊界（什麼算進耗時、什麼不算）
     只計 `s4_signal.features()` + `signal_from_features()` 兩個呼叫的時間，
     不含讀檔、建 DataFrame、補洞——那些在實盤是另一條路徑上的成本。
     同時記 wall time（time.perf_counter）與 CPU time（time.process_time）兩份。
     前 --warmup-frames 個 frame 不列入統計，排除首次呼叫的一次性成本（pandas 內部快取）。
     餵進去的 DataFrame 一律經過補洞（缺漏時段 close=前值、open/high/low=close、volume=0），
     補了幾根記在 input_contract.filled_bars_total。
     bars_per_hour = 3600000 // 週期毫秒，必須整除（5M → 12），符合 s4_signal 的輸入契約。
     **單次執行的 p50 抖動可達數十 %**（同機同參數三次實測 1.783 / 1.200 / 1.361 ms），
     判讀只看數量級、不要比較小數點；要比較就多跑幾次取分布。
==============================================================================

依賴：pandas / numpy（必要）、requests（Stage 0/1/2 需要）、websockets（Stage 1/2 需要）。
Stage 3/4 完全不碰網路，只需要 pandas / numpy。
"""
import argparse
import ast
import asyncio
import hashlib
import json
import math
import os
import re
import ssl
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# 讓 `python research/g2_measure.py`（sys.path[0] = research/）也找得到 repo 根目錄的 strategy 套件
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from strategy import s4_signal  # noqa: E402  ← 訊號邏輯唯一來源，禁止在本檔重抄一份

SCRIPT_VERSION = "g2_measure 1.1.0"

# ============================== 常數 ==============================
BASE_URL = "https://api.pionex.com"
WS_URL = "wss://ws.pionex.com/wsPub"
HOUR_MS = 3_600_000
INTERVAL_MS = {"1M": 60_000, "5M": 300_000, "15M": 900_000, "30M": 1_800_000, "60M": HOUR_MS}
TPE = timezone(timedelta(hours=8))

# [D3] 訂閱完成後，距離下一個 K 棒邊界至少要有這麼多毫秒，那根才算「完整觀測」
FIRST_BAR_GUARD_MS = 5_000

RESULT_FILES = {
    0: "stage0_universe.json",
    1: "stage1_aggregation.json",
    2: "stage2_latency.json",
    3: "stage3_compute.json",
    4: "stage4_screen.json",
}

def _doc_blocks():
    """從本檔 module docstring 程式化抽出 [D1]…[Dn] 區塊。

    json 的 method_notes 直接用這裡抽出來的**原文**，不另外手寫摘要：
    兩份文字放兩個地方遲早會漂移，而真正被帶回 office 判讀的是 json。
    """
    src = __doc__
    if src is None:          # python -OO 會把 docstring 拿掉，退回讀自己的原始碼
        try:
            src = ast.get_docstring(ast.parse(Path(__file__).read_text(encoding="utf-8")))
        except Exception:
            src = ""
    blocks, key, buf = {}, None, []
    for line in (src or "").splitlines():
        m = re.match(r"^\[(D\d+)\] ", line)
        if m:
            if key:
                blocks[key] = "\n".join(buf).rstrip()
            key, buf = m.group(1), [line]
        elif key is not None:
            if line and not line.startswith(" "):     # 下一個非縮排段落 → 區塊結束
                blocks[key] = "\n".join(buf).rstrip()
                key, buf = None, []
            else:
                buf.append(line)
    if key:
        blocks[key] = "\n".join(buf).rstrip()
    return blocks


DOC_BLOCKS = _doc_blocks()


def _doc(key):
    return DOC_BLOCKS.get(key) or \
        f"[{key}] （docstring 區塊抽取失敗，請直接看 research/g2_measure.py 開頭的「量測定義」一節）"


METHOD_NOTES = {
    0: [
        "候選 = tickers 的 24h amount 落在 [MIN_TURN24H, MAX_TURN24H]（值取自 strategy/s4_signal.DEFAULT_PARAMS）。",
        "amount 欄位以 API 原樣輸出，未做任何單位換算；若派網改欄位名，raw_ticker_sample 可供核對。",
        "riskTable 端點路徑未經本機實測（office 無法連線驗證），抓不到時只記錄錯誤、不影響本階段其餘產出。",
    ],
    1: [
        _doc("D1"), _doc("D2"), _doc("D3"), _doc("D4"), _doc("D5"),
        "volume_ratio = 本地 volume ÷ 交易所 volume（本地值為 [D5] 的事後真值）。"
        "接近 1 → 方案 A 可行；接近 0.5 → 交易所計雙邊；明顯 <1 且不穩 → 方案 A 不成立。"
        "volume_ratio_at_seal 是封存快照算出來的同一個比值，兩者不同就代表 --seal-grace 不夠。",
        "逐根原始值（本地與交易所並列）在 bars 陣列，統計摘要只是方便看，分析請用原始值重算。",
    ],
    2: [
        _doc("D5"), _doc("D6"),
        "compute_ms 的 history_source=synthetic 時，價格是合成的，只有耗時有意義、訊號數沒有意義。",
    ],
    3: [
        _doc("D8"),
        "直接 import strategy/s4_signal，未複製任何指標邏輯。",
    ],
    4: [
        _doc("D7"),
        "margin 掃描：粗篩區間放寬為 [MIN×margin, MAX/margin]，用來看『放寬多少可以換回漏掉的訊號、代價是多少候選數』。",
        "candidates_per_bar 是方案 B 每根 K 棒要打幾次 klines 的直接估計（rate limit 10 req/s → 除以 10 就是秒數）。",
    ],
}


# ============================== 錯誤與輸出 ==============================
class G2Error(Exception):
    """使用者看得懂的致命錯誤。main() 會印成 [錯誤] ...，不會吐 traceback。"""


class BudgetStop(Exception):
    """達到 --max-requests 上限。呼叫端要保存已有結果後收工。"""


def say(msg=""):
    print(msg, flush=True)


def warn(msg):
    print("[警告] " + msg, flush=True)


def now_ms():
    return int(time.time() * 1000)


def fmt_ms(ms):
    """毫秒 → 'YYYY-MM-DD HH:MM:SS UTC / HH:MM:SS+08'，兩個時區都給，避免看錯。"""
    if ms is None:
        return None
    dt = datetime.fromtimestamp(ms / 1000, timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S") + " UTC / " + dt.astimezone(TPE).strftime("%H:%M:%S") + "+08"


def bar_open_of(ts_ms, bar_ms):
    """[D2] 成交時間戳 → 所屬 K 棒開盤時刻。"""
    return (int(ts_ms) // bar_ms) * bar_ms


def ceil_bar(ts_ms, bar_ms):
    """向上對齊到 K 棒邊界。"""
    return -(-int(ts_ms) // bar_ms) * bar_ms


def first_full_bar_open(sub_done_ms, bar_ms):
    """[D3] 訂閱完成後，第一根「完整被觀測」的 K 棒開盤時刻。

    訂閱完成當下那根一定是半根；若完成時距離下一個邊界不到 FIRST_BAR_GUARD_MS，
    連下一根也一起丟（訂閱 ack 到第一筆推送之間還有時間差，太貼邊界會收不全）。
    這條規則只在這裡實作一次，Stage 1/2 共用。
    """
    return ceil_bar(int(sub_done_ms) + FIRST_BAR_GUARD_MS, bar_ms)


def bars_per_hour_of(interval):
    """s4_signal 要求整數。5M → 12、15M → 4、60M → 1。"""
    n, rem = divmod(HOUR_MS, INTERVAL_MS[interval])
    if rem:
        raise G2Error(f"週期 {interval} 無法整除一小時，s4_signal 的 bars_per_hour 必須是整數")
    return n


def _clean(obj):
    """把 numpy 型別、NaN/Inf 轉成合法 JSON（NaN → null）。office 端要能直接 json.load。"""
    if isinstance(obj, dict):
        return {str(k): _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return [_clean(v) for v in obj.tolist()]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        obj = float(obj)
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, (str, int, bool)) or obj is None:
        return obj
    return str(obj)


def write_json(path, obj):
    """原子寫入（先寫 .tmp 再 replace），中途斷電也不會留半個檔。"""
    path = str(path)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(_clean(obj), fh, ensure_ascii=False, indent=1)
        fh.write("\n")           # POSIX 慣例：檔案以換行結尾，diff / cat 才不會黏在一起
    os.replace(tmp, path)


def _script_sha256():
    """FR-6：唯一指認「哪一版腳本產生了這份數據」。

    git_commit 記的是 repo HEAD；research/ 目前不在版控裡，那個欄位對腳本版本零資訊量，
    所以額外對 __file__ 算雜湊——untracked 也指認得出來。
    """
    try:
        return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    except Exception:
        return None


def _git_commit():
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=str(REPO_ROOT),
                             capture_output=True, text=True, timeout=5)
        return out.stdout.strip() or None
    except Exception:
        return None


# ============================== 執行環境 ==============================
class Ctx:
    """一次執行的全部狀態：參數、請求計數、輸出路徑。FR-6 的 metadata 都從這裡長出來。"""

    def __init__(self, args):
        self.args = args
        self.stage = args.stage
        self.out_dir = Path(args.out_dir) if args.out_dir else (REPO_ROOT / "research" / "results")
        self.tmp_dir = Path(args.tmp_dir) if args.tmp_dir else (REPO_ROOT / "research" / "_tmp")
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.start_ms = now_ms()
        self.req_count = 0
        self.err_429 = 0
        self.retries = 0
        self.rate = float(args.rate)
        self._last_req_ms = 0.0
        self.http_date_offsets = []
        self.verify = args.ca_bundle if args.ca_bundle else True
        if args.ca_bundle and not os.path.exists(args.ca_bundle):
            raise G2Error(f"--ca-bundle 指定的檔案不存在：{args.ca_bundle}")
        self.machine = args.machine or _hostname()
        self.notes = []          # 執行中累積的提醒，會寫進 json
        self.out_path = self.out_dir / RESULT_FILES[self.stage]

    def note(self, msg):
        self.notes.append(msg)
        warn(msg)

    def budget_left(self):
        return self.args.max_requests - self.req_count


def _hostname():
    try:
        import socket
        return socket.gethostname()
    except Exception:
        return "unknown"


def build_meta(ctx, stage_name, params):
    """FR-6：每個 json 都要能重建『這批數據是在什麼條件下產生的』。"""
    end = now_ms()
    try:
        import websockets
        ws_ver = websockets.__version__
    except Exception:
        ws_ver = None
    try:
        import requests
        req_ver = requests.__version__
    except Exception:
        req_ver = None
    return {
        "script": SCRIPT_VERSION,
        "script_file": "research/g2_measure.py",
        "script_sha256": _script_sha256(),
        "stage": ctx.stage,
        "stage_name": stage_name,
        "machine": ctx.machine,
        "started_utc": fmt_ms(ctx.start_ms),
        "started_epoch_ms": ctx.start_ms,
        "finished_utc": fmt_ms(end),
        "finished_epoch_ms": end,
        "elapsed_s": round((end - ctx.start_ms) / 1000, 3),
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "websockets": ws_ver,
        "requests": req_ver,
        "git_commit": _git_commit(),
        "git_commit_note": "repo HEAD。research/ 若尚未進版控，這個欄位指不出腳本版本，"
                           "要認腳本請用 script_sha256。",
        "rest_requests_total": ctx.req_count,
        "rest_429_count": ctx.err_429,
        "rest_retry_count": ctx.retries,
        "ca_bundle": ctx.args.ca_bundle,
        "s4_params": dict(s4_signal.DEFAULT_PARAMS),
        "params": params,
        "cli_argv": sys.argv[1:],
        "runtime_notes": ctx.notes,
    }


def backup_if_exists(ctx, path):
    """FR-4：絕不靜默覆寫既有結果。舊檔移到 _tmp/ 並明確告知。"""
    path = Path(path)
    if not path.exists():
        return
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = ctx.tmp_dir / f"{path.stem}.{stamp}{path.suffix}"
    try:
        os.replace(str(path), str(dest))
        say(f"[注意] 已有舊結果 {path}，已搬到 {dest} 保存，本次會產生新的檔案。")
    except Exception as e:
        raise G2Error(f"無法備份既有結果 {path}：{e}\n→ 請手動改名或刪除後重跑。")


# ============================== 依賴預檢（FR-5）==============================
def require_deps(stage):
    """在真的開始跑之前先確認套件裝了沒。

    laptop 上 websockets 裝沒裝是未知數，等跑到 _open_ws 才爆會變成
    「連不上 WebSocket: ModuleNotFoundError」——那個訊息會讓人去查網路，方向全錯。
    """
    need = []
    if stage in (0, 1, 2):
        need.append(("requests", "requests"))
    if stage in (1, 2):
        need.append(("websockets", "websockets>=12"))
    missing = []
    for mod, spec in need:
        try:
            __import__(mod)
        except Exception:
            missing.append((mod, spec))
    if missing:
        names = "、".join(m for m, _ in missing)
        specs = " ".join(spec for _, spec in missing)
        raise G2Error(
            f"Stage {stage} 需要的套件沒有裝：{names}\n"
            f"→ 請執行：python -m pip install {specs}\n"
            f"→ 裝完再重跑同一行指令即可（前面階段的結果不受影響）。")
    if stage in (1, 2):
        import websockets
        try:
            import websockets.asyncio.client  # noqa: F401
        except Exception:
            raise G2Error(
                f"websockets {getattr(websockets, '__version__', '?')} 太舊，"
                "沒有 websockets.asyncio.client（需要 12 以上）。\n"
                "→ 請執行：python -m pip install -U websockets")


# ============================== REST ==============================
def _http_get_raw(ctx, path, params):
    """唯一真正發出 HTTP 的地方。g2_selftest.py 會整個換掉它來注入假回應。"""
    import requests
    sess = getattr(ctx, "_session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update({"User-Agent": "g2-measure/1.0"})
        ctx._session = sess
    return sess.get(BASE_URL + path, params=params, timeout=20, verify=ctx.verify)


def rest_get(ctx, path, params, what=""):
    """FR-3 / FR-5：限速、429 退避降速、請求計數、指名道姓的錯誤訊息。"""
    if ctx.req_count >= ctx.args.max_requests:
        raise BudgetStop(f"已達 --max-requests {ctx.args.max_requests}")
    label = what or path
    for attempt in range(5):
        gap = 1000.0 / max(ctx.rate, 0.1)
        wait = gap - (time.time() * 1000 - ctx._last_req_ms)
        if wait > 0:
            time.sleep(wait / 1000.0)
        t_send = time.time() * 1000
        try:
            resp = _http_get_raw(ctx, path, params)
        except Exception as e:
            name = type(e).__name__
            if "SSL" in name or "Certificate" in name:
                raise G2Error(
                    f"SSL 憑證驗證失敗（{label}）：{e}\n"
                    "→ 公司網路會攔截 TLS。laptop 上不該出現這個錯誤；若在公司網路，"
                    "請用 --ca-bundle <公司根憑證.pem> 指定憑證，本腳本不內建任何公司 CA 路徑。")
            if "Proxy" in name or "ConnectTimeout" in name or "ConnectionError" in name:
                raise G2Error(
                    f"連不上 {BASE_URL}（{label}）：{e}\n"
                    "→ 請確認：(1) 網路通不通 (2) 是否需要 proxy (3) 所在地區是否被派網封鎖。")
            ctx.retries += 1
            if attempt == 4:
                raise G2Error(f"請求 {label} 連續失敗 5 次：{e}")
            time.sleep(1.5 * (attempt + 1))
            continue
        finally:
            ctx._last_req_ms = time.time() * 1000
        ctx.req_count += 1
        # 時鐘偏移估計 (a)：Header Date 解析度只有 1 秒，只當 sanity check
        try:
            d = resp.headers.get("Date")
            if d:
                srv = datetime.strptime(d, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
                ctx.http_date_offsets.append(srv.timestamp() * 1000 - (t_send + time.time() * 1000) / 2)
        except Exception:
            pass
        sc = resp.status_code
        if sc == 429:
            ctx.err_429 += 1
            old = ctx.rate
            ctx.rate = max(1.0, ctx.rate / 2)
            cool = 65 + 10 * (ctx.err_429 - 1)
            ctx.note(f"HTTP 429（{label}）：派網封鎖 IP 至少 60 秒，每多打一次再加 10 秒。"
                     f"限速 {old:.1f} → {ctx.rate:.1f} req/s，先睡 {cool} 秒再續。"
                     f"（若反覆出現，請用 --rate 調更低後重跑）")
            time.sleep(cool)
            continue
        if sc in (403, 451):
            raise G2Error(f"HTTP {sc}（{label}）：連線被拒，通常是所在地區或 IP 被派網封鎖。"
                          "→ 換網路環境再試；不要反覆重打，會被延長封鎖。")
        if sc >= 500:
            ctx.retries += 1
            if attempt == 4:
                raise G2Error(f"派網伺服器錯誤 HTTP {sc}（{label}），重試 5 次仍失敗。→ 稍後再試。")
            time.sleep(2 ** attempt)
            continue
        try:
            js = resp.json()
        except Exception:
            raise G2Error(f"{label} 回應不是 JSON（HTTP {sc}）：{resp.text[:300]}\n"
                          "→ 可能被 proxy 或入口網頁攔截，請確認網路環境。")
        if not js.get("result", False):
            raise G2Error(f"{label} API 回報失敗：code={js.get('code')} message={js.get('message')}\n"
                          f"→ 請確認參數 {params} 是否正確；端點路徑可能已改版。")
        return js
    raise G2Error(f"{label} 重試 5 次仍失敗（含 {ctx.err_429} 次 429）。")


def fetch_symbols(ctx):
    js = rest_get(ctx, "/api/v1/common/symbols", {"type": "PERP"}, "symbols")
    syms = js["data"]["symbols"]
    if not isinstance(syms, list) or not syms:
        raise G2Error("symbols 回應為空或格式不符，無法產生候選清單。→ 請把 stage0 的 raw sample 貼回給開發者。")
    return syms


def _sym_is_trading(s):
    """symbols 回應的欄位名沒有實測過，能找到 status/state 就比對 TRADING，找不到就當作全數有效。"""
    for k in ("status", "tradingStatus", "state"):
        v = s.get(k)
        if isinstance(v, str):
            return v.upper() == "TRADING"
    for k in ("enable", "enabled", "tradable"):
        v = s.get(k)
        if isinstance(v, bool):
            return v
    return True


def fetch_tickers(ctx):
    js = rest_get(ctx, "/api/v1/market/tickers", {"type": "PERP"}, "tickers")
    data = js["data"]
    rows = data.get("tickers") if isinstance(data, dict) else data
    if not isinstance(rows, list) or not rows:
        raise G2Error("tickers 回應為空或格式不符。→ 請確認 /api/v1/market/tickers?type=PERP 是否改版。")
    return rows


def _ticker_amount(t):
    """24h 計價幣成交額。欄位名未實測，依序試幾個常見名稱；都沒有就回 None（會被記到 json）。"""
    for k in ("amount", "quoteVolume", "turnover", "value"):
        v = t.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return None


# ============================== 資料準備（AC-3）==============================
def prepare_frame(df, bar_ms, t_now=None):
    """把 K 棒 DataFrame 整成 s4_signal 的輸入契約：time 升冪、固定週期、無缺漏。

    缺漏時段補成 close=前值、open/high/low=close、volume=0（與 pionex_backtest.load_hourly /
    pionex_dryrun.prepare 相同處理）。這裡是**資料準備**不是指標邏輯，所以自行實作；
    指標一律走 s4_signal，本檔不抄任何指標公式。

    回傳 (df, filled_bars)。filled_bars 就是 AC-3 要求記在 metadata 的補洞根數。
    """
    if df is None or len(df) == 0:
        return df, 0
    df = df.copy()
    for c in ("time", "open", "high", "low", "close", "volume", "amount"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["time"]).drop_duplicates("time", keep="last")
    df["time"] = df["time"].astype("int64")
    df = df.sort_values("time").reset_index(drop=True)
    if t_now is not None:
        df = df[df["time"] + bar_ms <= t_now].reset_index(drop=True)
    if len(df) == 0:
        return df, 0
    grid = pd.DataFrame({"time": np.arange(int(df["time"].min()), int(df["time"].max()) + 1,
                                           bar_ms, dtype="int64")})
    before = len(df)
    df = grid.merge(df, on="time", how="left")
    filled = len(df) - before
    df["close"] = df["close"].ffill()
    for c in ("open", "high", "low"):
        df[c] = df[c].fillna(df["close"])
    df["volume"] = df["volume"].fillna(0.0)
    if "amount" in df.columns:
        df["amount"] = df["amount"].fillna(0.0)
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    return df, filled


def cache_path(cache_dir, symbol, interval):
    return os.path.join(str(cache_dir), f"{symbol}_{interval}.csv")


def list_cache_symbols(cache_dir, interval):
    """回傳快取裡有此週期資料的 symbol 清單。找不到目錄 → FR-5 訊息。"""
    cache_dir = str(cache_dir)
    if not os.path.isdir(cache_dir):
        raise G2Error(
            f"找不到快取目錄：{cache_dir}\n"
            "→ 這個階段需要 pionex_cache（在 laptop 上跑過 pionex_strategy4.py 就會有）。\n"
            "→ office 電腦沒有這個目錄是正常的：Stage 3 請改用 --source synthetic，\n"
            "   Stage 4 請先跑 `python research/g2_selftest.py` 產生合成快取，\n"
            "   再用 --cache-dir research/_tmp/selftest/synth_cache 驗流程"
            "（數字沒有意義，只驗程式路徑）。")
    suffix = f"_{interval}.csv"
    out = sorted(f[: -len(suffix)] for f in os.listdir(cache_dir) if f.endswith(suffix))
    if not out:
        raise G2Error(
            f"快取目錄 {cache_dir} 裡沒有任何 *{suffix} 檔案。\n"
            f"→ 目前的 K 棒週期參數是 {interval}。請確認 laptop 上的回測是用同一個週期跑的，"
            "或用 --interval 指定實際存在的週期。")
    return out


def load_cache_frame(cache_dir, symbol, interval, max_bars=None):
    path = cache_path(cache_dir, symbol, interval)
    if not os.path.exists(path):
        raise G2Error(f"找不到快取檔 {path}。→ 請確認 --cache-dir 與 --interval 是否正確。")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        raise G2Error(f"讀取快取檔 {path} 失敗：{e}\n→ 檔案可能寫壞了，刪掉它讓回測重抓即可。")
    need = {"time", "open", "high", "low", "close", "volume"}
    missing = need - set(df.columns)
    if missing:
        raise G2Error(f"快取檔 {path} 缺少欄位 {sorted(missing)}。→ 格式不符，請刪掉重抓。")
    df, filled = prepare_frame(df, INTERVAL_MS[interval])
    if max_bars and len(df) > max_bars:
        df = df.tail(max_bars).reset_index(drop=True)
    return df, filled


# ============================== 合成資料 ==============================
def synth_frame(n_bars, bar_ms, end_open_ms, rng, base_price=1.0, base_vol=200.0,
                event_idx=(), event_ret=0.16, event_vol_mult=4.0, event_cpos=0.25,
                bars_per_hour=12):
    """產生符合 s4_signal 輸入契約的合成 K 棒（office 沒有 pionex_cache 時用）。

    event_idx 指定的那幾根會被塑造成「近 2 小時漲 event_ret、本根爆量 event_vol_mult 倍、
    收盤位置 event_cpos」——也就是策略4 的進場條件，讓量測不是在跑全 NaN 的空路徑。
    """
    n = int(n_bars)
    times = np.arange(end_open_ms - (n - 1) * bar_ms, end_open_ms + 1, bar_ms, dtype="int64")
    rets = rng.normal(0.0, 0.004, n)
    ramp = 2 * int(bars_per_hour)                      # ret2h 的回看根數
    for e in event_idx:
        lo = max(1, e - ramp + 1)
        rets[lo:e + 1] += math.log1p(event_ret) / max(1, e - lo + 1)
    close = base_price * np.exp(np.cumsum(rets))
    open_ = np.empty(n)
    open_[0] = close[0] / (1 + rets[0])
    open_[1:] = close[:-1]
    vol = base_vol * np.exp(rng.normal(0.0, 0.35, n))
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.003, n)))
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.003, n)))
    for e in event_idx:
        if not (0 <= e < n):
            continue
        vol[e] = base_vol * event_vol_mult
        # 由 cpos 反解 high：cpos = (c-l)/(h-l) → h = l + (c-l)/cpos，並確保 l <= min(o,c) <= c <= h
        low[e] = min(open_[e], close[e]) * 0.999
        high[e] = low[e] + (close[e] - low[e]) / max(event_cpos, 1e-6)
        high[e] = max(high[e], open_[e], close[e])
    df = pd.DataFrame({"time": times, "open": open_, "high": high, "low": low,
                       "close": close, "volume": vol})
    df["amount"] = df["close"] * df["volume"]
    return df


def synth_universe(n_symbols, n_bars, bar_ms, end_open_ms, seed, bars_per_hour=12):
    """一組合成 symbol（名稱刻意做成 SYN0000_USDT_PERP，一眼就知道不是真資料）。"""
    rng = np.random.default_rng(seed)
    out = {}
    for i in range(n_symbols):
        # 成交額分佈：多數落在策略4 的 [2 萬, 50 萬] 區間內，少數在區間外
        base_vol = float(rng.choice([80.0, 200.0, 600.0, 1600.0], p=[0.2, 0.4, 0.3, 0.1]))
        ev = sorted(rng.choice(np.arange(2 * bars_per_hour + 24 * bars_per_hour, n_bars),
                               size=min(3, max(1, n_bars // 300)), replace=False).tolist()) \
            if n_bars > 26 * bars_per_hour + 2 else []
        out[f"SYN{i:04d}_USDT_PERP"] = synth_frame(
            n_bars, bar_ms, end_open_ms, rng, base_price=float(rng.uniform(0.5, 2.0)),
            base_vol=base_vol, event_idx=ev, bars_per_hour=bars_per_hour)
    return out


# ============================== 聚合器（Stage 1/2 核心）==============================
class Aggregator:
    """把 TRADE 推送聚合成 K 棒。純函數式狀態機，不碰網路，離線就能完整驗證。"""

    def __init__(self, bar_ms):
        self.bar = bar_ms
        self.bars = {}          # (symbol, bar_open) -> dict
        self.trade_count = 0
        self.dup_count = 0
        self.min_push_lag_ms = None   # [D6](b)
        self.max_push_lag_ms = None

    def _new(self, symbol, bo):
        return {"symbol": symbol, "bar_open": bo, "bar_close": bo + self.bar,
                "open": None, "high": None, "low": None, "close": None,
                "volume": 0.0, "amount": 0.0, "trades": 0,
                "taker_buy_volume": 0.0, "taker_sell_volume": 0.0,
                "first_ts": None, "last_ts": None, "first_seq": None, "last_seq": None,
                "first_recv_ms": None, "last_recv_ms": None,
                "max_recv_ms": None, "trades_after_seal": 0, "max_late_after_seal_ms": None,
                "sealed_at_ms": None, "_ids": set()}

    def add_trade(self, symbol, price, size, ts_ms, recv_ms, side=None, tid=None, seq=None):
        """[D2] 依交易所時間戳歸屬 K 棒；[D4] open/close 依時間戳先後決定，不是到達順序。"""
        bo = bar_open_of(ts_ms, self.bar)
        key = (symbol, bo)
        b = self.bars.get(key)
        if b is None:
            b = self.bars[key] = self._new(symbol, bo)
        if tid is not None:
            if tid in b["_ids"]:
                self.dup_count += 1
                return
            b["_ids"].add(tid)
        self.trade_count += 1
        seq = self.trade_count if seq is None else seq
        price, size = float(price), float(size)
        if b["first_ts"] is None or (ts_ms, seq) < (b["first_ts"], b["first_seq"]):
            b["first_ts"], b["first_seq"], b["open"] = ts_ms, seq, price
            if b["first_recv_ms"] is None:
                b["first_recv_ms"] = recv_ms
        if b["last_ts"] is None or (ts_ms, seq) > (b["last_ts"], b["last_seq"]):
            b["last_ts"], b["last_seq"], b["close"] = ts_ms, seq, price
        b["high"] = price if b["high"] is None else max(b["high"], price)
        b["low"] = price if b["low"] is None else min(b["low"], price)
        b["volume"] += size
        b["amount"] += price * size
        b["trades"] += 1
        if side is not None:
            s = str(side).upper()
            if s.startswith("B"):
                b["taker_buy_volume"] += size
            elif s.startswith("S"):
                b["taker_sell_volume"] += size
        b["first_recv_ms"] = recv_ms if b["first_recv_ms"] is None else min(b["first_recv_ms"], recv_ms)
        b["last_recv_ms"] = recv_ms if b["last_recv_ms"] is None else max(b["last_recv_ms"], recv_ms)
        b["max_recv_ms"] = recv_ms if b["max_recv_ms"] is None else max(b["max_recv_ms"], recv_ms)
        if b["sealed_at_ms"] is not None:
            # 封存之後才到的成交 → grace 不夠，這是 Stage 2 的關鍵訊號
            b["trades_after_seal"] += 1
            late = recv_ms - b["sealed_at_ms"]
            b["max_late_after_seal_ms"] = late if b["max_late_after_seal_ms"] is None \
                else max(b["max_late_after_seal_ms"], late)
        lag = recv_ms - ts_ms
        self.min_push_lag_ms = lag if self.min_push_lag_ms is None else min(self.min_push_lag_ms, lag)
        self.max_push_lag_ms = lag if self.max_push_lag_ms is None else max(self.max_push_lag_ms, lag)

    def seal(self, bar_open, symbols, seal_ms, degraded_reason=None):
        """封存某一根 K 棒的所有 symbol。沒有成交的 symbol 也會出一筆（trades=0），
           這樣分析端看得到覆蓋率，不會把『沒推送』誤讀成『沒這根』。"""
        out = []
        for sym in symbols:
            b = self.bars.get((sym, bar_open))
            if b is None:
                b = self.bars[(sym, bar_open)] = self._new(sym, bar_open)
            b["sealed_at_ms"] = seal_ms
            rec = {k: v for k, v in b.items() if k != "_ids"}
            rec["flags"] = []
            if rec["trades"] == 0:
                rec["flags"].append("local_no_trades")
            if degraded_reason:
                rec["flags"].append(degraded_reason)
            # [D5] 資料面真延遲：最後一筆成交到達時刻 - K 棒收盤時刻
            rec["data_ready_ms"] = (rec["max_recv_ms"] - rec["bar_close"]) if rec["max_recv_ms"] is not None else None
            rec["seal_ms"] = seal_ms - rec["bar_close"]
            out.append(rec)
        # 封存後仍要留著 _ids：晚到成交還是得去重，否則重連補推會把事後真值灌水。
        # 記憶體改成延後兩根再釋放——晚到超過一整根 K 棒的成交對本量測已經沒有意義。
        stale = bar_open - 2 * self.bar
        for (_sym, bo), bb in self.bars.items():
            if bo <= stale and bb["_ids"]:
                bb["_ids"] = set()
        return out

    # apply_late_arrivals 會用事後真值覆蓋掉的欄位（seal 當下的值另存成 *_at_seal）
    _LATE_KEYS = ("open", "high", "low", "close", "volume", "amount", "trades",
                  "taker_buy_volume", "taker_sell_volume", "first_ts", "last_ts",
                  "first_recv_ms", "last_recv_ms", "max_recv_ms",
                  "trades_after_seal", "max_late_after_seal_ms")

    def apply_late_arrivals(self, bar_open, recs):
        """[D5] 收集結束後對每一根呼叫一次：把「封存之後才抵達、時間戳仍屬於這根」的成交回填。

        seal() 交出去的 rec 是封存當下的**快照複本**，晚到的成交只會改到 Aggregator 內部的 b，
        改不到已經送出去的 rec——不回填的話 trades_after_seal 在輸出上結構性恆為 0，
        而 data_ready_ms 與 volume 都是右設限值，使用者沒有任何線索知道自己看到的是截斷值。

        回填後：volume / data_ready_ms 等欄位是事後真值，封存當下的值改放在 *_at_seal。
        """
        for rec in recs:
            b = self.bars.get((rec["symbol"], bar_open))
            if b is None or rec.get("late_arrivals_applied"):
                continue
            for k in ("volume", "amount", "trades", "data_ready_ms"):
                rec[k + "_at_seal"] = rec[k]
            for k in self._LATE_KEYS:
                rec[k] = b[k]
            rec["data_ready_ms"] = (b["max_recv_ms"] - rec["bar_close"]) \
                if b["max_recv_ms"] is not None else None
            rec["volume_after_seal"] = rec["volume"] - rec["volume_at_seal"]
            rec["late_arrivals_applied"] = True
        return recs


# ============================== WS 訊息解析 ==============================
_PRICE_KEYS = ("price", "p", "px")
_SIZE_KEYS = ("size", "qty", "quantity", "volume", "vol", "q", "v")   # 刻意不含 amount（那是計價幣額）
_TS_KEYS = ("timestamp", "time", "ts", "tradeTime", "T", "t")
_SIDE_KEYS = ("side", "S", "direction", "takerSide")
_ID_KEYS = ("tradeId", "id", "tid", "trade_id")


def _first_key(d, keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def parse_trade_message(msg):
    """把一則 WS 訊息拆成 ('trade', symbol, [trades]) / ('ping', payload) / ('ack', ...) / ('error', ...) / ('other', ...)。

    派網 public WS 的實際 payload 欄位名未經本機實測（office 不連線），所以這裡採
    『候選欄位名逐一嘗試』。任何一則 TRADE 訊息拆不出價量時間，會被上層當成致命錯誤，
    並把原始訊息存進結果 json，讓人看得到到底收到什麼（FR-5）。
    """
    if isinstance(msg, (bytes, bytearray)):
        msg = msg.decode("utf-8", "replace")
    if isinstance(msg, str):
        try:
            msg = json.loads(msg)
        except Exception:
            return ("other", None, None)
    if not isinstance(msg, dict):
        return ("other", None, None)
    op = str(msg.get("op") or msg.get("event") or "").upper()
    topic = str(msg.get("topic") or msg.get("channel") or "").upper()
    symbol = msg.get("symbol") or msg.get("s")
    data = msg.get("data")
    if op == "PING":
        return ("ping", None, msg)
    # 先認 TRADE 資料，再認控制訊息：避免資料訊息剛好帶個 code 欄位就被誤判成錯誤
    if topic != "TRADE" or data is None:
        if op in ("SUBSCRIBED", "SUBSCRIBE", "SUBSCRIPTION", "SUBSCRIBE_SUCCESS"):
            return ("ack", symbol, msg)
        if op == "ERROR" or (msg.get("code") not in (None, 0, "0") and msg.get("message")):
            return ("error", None, msg)
        return ("other", symbol, msg)
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return ("other", symbol, msg)
    trades = []
    for d in data:
        if not isinstance(d, dict):
            continue
        price, size = _first_key(d, _PRICE_KEYS), _first_key(d, _SIZE_KEYS)
        ts = _first_key(d, _TS_KEYS)
        if price is None or size is None or ts is None:
            return ("badtrade", symbol, msg)
        try:
            trades.append({"price": float(price), "size": float(size), "ts": int(float(ts)),
                           "side": _first_key(d, _SIDE_KEYS), "tid": _first_key(d, _ID_KEYS),
                           "symbol": d.get("symbol") or symbol})
        except (TypeError, ValueError):
            return ("badtrade", symbol, msg)
    return ("trade", symbol, trades)


# ============================== WS 連線 ==============================
async def _open_ws(uri, ssl_ctx, open_timeout, ping_interval):
    """唯一真正建立 WS 連線的地方。g2_selftest.py 會整個換掉它來注入假連線。"""
    import websockets.asyncio.client as wsc
    kwargs = {"open_timeout": open_timeout, "ping_interval": ping_interval,
              "ping_timeout": ping_interval, "max_queue": 4096, "max_size": 4 * 1024 * 1024}
    if ssl_ctx is not None:
        kwargs["ssl"] = ssl_ctx
    return await wsc.connect(uri, **kwargs)


class Feed:
    """多連線訂閱 TRADE，把成交餵進 Aggregator。負責重連、斷線區間記錄、訂閱計時。"""

    def __init__(self, ctx, symbols, agg, connections, sub_rate, raw_sample_limit=20):
        self.ctx = ctx
        self.symbols = list(symbols)
        self.agg = agg
        self.n_conn = max(1, int(connections))
        self.sub_rate = max(0.5, float(sub_rate))
        self.raw_samples = []
        self.raw_limit = raw_sample_limit
        self.sub_start_ms = None
        self.sub_sent_ms = None       # 「全部連線都把 SUBSCRIBE 送完」的時刻（不是第一則）
        self.sub_ack_ms = None        # 收到最後一則 ack 的時刻（沒有 ack 機制時為 None）
        self.ack_count = 0
        self.sub_total = 0
        self._subs_pending = 0        # 還沒完成首輪訂閱的連線數
        self.outages = []             # [(start_ms, end_ms)] WS 斷線區間
        self.reconnects = 0
        self.first_data_ms = None
        self.fatal = None
        self.stop = asyncio.Event()
        self._ever_connected = set()
        self._down_since = {}
        self._uri = ctx.args.ws_url

    # ---- 斷線區間 ----
    def outage_overlap(self, t0, t1):
        for s, e in self.outages:
            if s < t1 and (e or now_ms()) > t0:
                return True
        for s in self._down_since.values():
            if s < t1:
                return True
        return False

    def _mark_down(self, idx):
        if idx not in self._down_since:
            self._down_since[idx] = now_ms()

    def _mark_up(self, idx):
        s = self._down_since.pop(idx, None)
        if s is not None:
            self.outages.append((s, now_ms()))

    # ---- 主流程 ----
    async def run(self):
        chunks = [self.symbols[i::self.n_conn] for i in range(self.n_conn)]
        chunks = [c for c in chunks if c]
        self.sub_total = sum(len(c) for c in chunks)
        self._subs_pending = len(chunks)
        self.sub_start_ms = now_ms()
        self._tasks = [asyncio.create_task(self._conn_loop(i, c)) for i, c in enumerate(chunks)]
        await self.stop.wait()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _conn_loop(self, idx, syms):
        backoff = 1.0
        while not self.stop.is_set():
            ws = None
            try:
                ssl_ctx = None
                if self.ctx.args.ca_bundle:
                    ssl_ctx = ssl.create_default_context(cafile=self.ctx.args.ca_bundle)
                ws = await _open_ws(self._uri, ssl_ctx, self.ctx.args.ws_open_timeout,
                                    self.ctx.args.ws_ping_interval)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if idx not in self._ever_connected:
                    self.fatal = G2Error(
                        f"連不上 WebSocket {self._uri}：{type(e).__name__}: {e}\n"
                        "→ 請確認：(1) 網路通不通 (2) 公司網路通常會擋 wss，請換到不受限的網路\n"
                        "→ 若必須在有 TLS 攔截的網路，請用 --ca-bundle <根憑證.pem>\n"
                        "→ 端點若已改版，可用 --ws-url 指定新的位址。")
                    self.stop.set()
                    return
                self._mark_down(idx)
                self.reconnects += 1
                self.ctx.note(f"連線 #{idx} 重連失敗（{type(e).__name__}），{backoff:.0f} 秒後再試。")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            backoff = 1.0
            first = idx not in self._ever_connected
            self._ever_connected.add(idx)
            if not first:
                self._mark_up(idx)
            try:
                await self._subscribe(ws, syms, first)
                async for raw in ws:
                    await self._on_raw(ws, raw)
                # 正常結束（伺服器關閉）
                raise ConnectionError("伺服器關閉連線")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self.stop.is_set():
                    return
                self._mark_down(idx)
                self.reconnects += 1
                self.ctx.note(f"連線 #{idx} 斷線（{type(e).__name__}: {e}），重連中；"
                              f"這段期間的 K 棒會被標成 degraded_ws_outage，不列入 clean 統計。")
            finally:
                try:
                    if ws is not None:
                        await ws.close()
                except Exception:
                    pass

    async def _subscribe(self, ws, syms, first_round):
        """FR-3：每連線發送上限 5 msg/s，預設用 --sub-rate（4）留安全邊際。

        sub_sent_ms 只在「所有連線的首輪訂閱都送完」時才設定——[D3] 的第一根完整 K 棒
        是從這個時刻算起的，取成第一則 SUBSCRIBE 會讓觀測窗多算一段沒訂閱到的時間。
        重連時的補訂閱不會改寫這個時刻。
        """
        gap = 1.0 / self.sub_rate
        for s in syms:
            if self.stop.is_set():
                return
            await ws.send(json.dumps({"op": "SUBSCRIBE", "topic": "TRADE", "symbol": s}))
            await asyncio.sleep(gap)
        if first_round:
            self._subs_pending -= 1
            if self._subs_pending <= 0 and self.sub_sent_ms is None:
                self.sub_sent_ms = now_ms()

    async def _on_raw(self, ws, raw):
        recv_ms = now_ms()
        if len(self.raw_samples) < self.raw_limit:
            self.raw_samples.append({"recv_ms": recv_ms, "recv_utc": fmt_ms(recv_ms),
                                     "raw": raw if isinstance(raw, str) else str(raw)})
        kind, symbol, payload = parse_trade_message(raw)
        if kind == "ping":
            # 派網要求 10 秒內回 PONG，不回會被斷線。漏了這段整個量測都會壞掉。
            out = dict(payload)
            out["op"] = "PONG"
            await ws.send(json.dumps(out))
            return
        if kind == "ack":
            self.ack_count += 1
            self.sub_ack_ms = recv_ms
            return
        if kind == "error":
            self.ctx.note(f"WS 回報錯誤訊息：{payload}")
            return
        if kind == "badtrade":
            self.fatal = G2Error(
                "收到 TRADE 推送但拆不出 價格/數量/時間戳 三個欄位。\n"
                f"→ 原始訊息：{str(payload)[:600]}\n"
                "→ 派網的 payload 欄位名與本腳本的假設不符（見 g2_measure.py 的 _PRICE_KEYS 等）。\n"
                "→ 原始訊息已存進結果 json 的 raw_message_samples，請把該檔案帶回給開發者調整欄位對應。")
            self.stop.set()
            return
        if kind != "trade":
            return
        if self.first_data_ms is None:
            self.first_data_ms = recv_ms
        for t in payload:
            self.agg.add_trade(t["symbol"] or symbol, t["price"], t["size"], t["ts"], recv_ms,
                               side=t["side"], tid=t["tid"])


# ============================== 收集流程（Stage 1/2 共用）==============================
async def collect_bars(ctx, symbols, interval, n_bars, on_seal, compute_cb=None):
    """訂閱 → 等 n_bars 根完整 K 棒封存 → 每封存一根就呼叫 on_seal（用來做 checkpoint）。

    回傳 (feed, agg, info)。中途 Ctrl-C 或超時都會把已封存的部分交回給呼叫端保存（FR-4）。
    """
    bar = INTERVAL_MS[interval]
    agg = Aggregator(bar)
    feed = Feed(ctx, symbols, agg, ctx.args.connections, ctx.args.sub_rate, ctx.args.raw_samples)
    info = {"sealed": 0, "aborted": None}

    run_task = asyncio.create_task(feed.run())
    # 等訂閱送完（或失敗）
    t_wait0 = now_ms()
    while feed.sub_sent_ms is None and feed.fatal is None:
        if now_ms() - t_wait0 > (ctx.args.ws_open_timeout + 5) * 1000 + len(symbols) / feed.sub_rate * 1000:
            feed.fatal = G2Error("訂閱送不出去（超時）。→ 連線疑似卡住，請檢查網路後重跑。")
            break
        await asyncio.sleep(0.1)
    if feed.fatal:
        # 不直接 raise：呼叫端要先把 feed.raw_message_samples 寫進 json，
        # 錯誤訊息才對得上（「原始訊息已存進結果 json」不能是空頭支票）。
        feed.stop.set()
        await run_task
        info["fatal"] = feed.fatal
        info["first_full_bar_open"] = None
        info["sub_done_ms"] = feed.sub_sent_ms
        return feed, agg, info

    sub_done_ms = feed.sub_sent_ms
    say(f"訂閱完成：{feed.sub_total} 個 symbol / {feed.n_conn} 條連線，"
        f"耗時 {(sub_done_ms - feed.sub_start_ms) / 1000:.1f} 秒（ack {feed.ack_count} 則）")

    # [D3] 第一根完整 K 棒
    first_open = first_full_bar_open(sub_done_ms, bar)
    say(f"第一根完整 K 棒開盤 {fmt_ms(first_open)}；預計收集 {n_bars} 根，"
        f"約 {(first_open + n_bars * bar + ctx.args.seal_grace * 1000 - now_ms()) / 60000:.1f} 分鐘")

    grace_ms = int(ctx.args.seal_grace * 1000)
    deadline = now_ms() + int(ctx.args.max_minutes * 60000)
    target = first_open
    try:
        while info["sealed"] < n_bars:
            if feed.fatal:
                info["fatal"] = feed.fatal
                break
            if now_ms() > deadline:
                info["aborted"] = f"超過 --max-minutes {ctx.args.max_minutes} 分鐘上限，提前收工（已封存 {info['sealed']} 根）"
                ctx.note(info["aborted"])
                break
            seal_at = target + bar + grace_ms
            if now_ms() < seal_at:
                await asyncio.sleep(min(1.0, (seal_at - now_ms()) / 1000.0))
                continue
            seal_ms = now_ms()
            degraded = "degraded_ws_outage" if feed.outage_overlap(target, target + bar) else None
            recs = agg.seal(target, symbols, seal_ms, degraded)
            info["sealed"] += 1
            extra = None
            if compute_cb is not None:
                extra = compute_cb(target, recs)
            on_seal(target, recs, extra)
            with_trades = sum(1 for r in recs if r["trades"] > 0)
            dr = [r["data_ready_ms"] for r in recs if r["data_ready_ms"] is not None]
            say(f"  [{info['sealed']}/{n_bars}] {fmt_ms(target)} 封存："
                f"{with_trades}/{len(symbols)} 個 symbol 有成交，"
                f"data_ready max={max(dr) if dr else None} ms"
                + (f"，compute={extra['compute_ms']:.0f} ms" if extra else "")
                + (f"  <{degraded}>" if degraded else ""))
            target += bar
    except KeyboardInterrupt:
        info["aborted"] = f"使用者中斷（Ctrl-C），已封存 {info['sealed']} 根，結果仍會寫出"
        ctx.note(info["aborted"])
    finally:
        feed.stop.set()
        await run_task

    info["first_full_bar_open"] = first_open
    info["sub_done_ms"] = sub_done_ms
    return feed, agg, info


def _feed_summary(ctx, feed, agg, info):
    return {
        "ws_url": feed._uri,
        "connections": feed.n_conn,
        "symbols_subscribed": feed.sub_total,
        "subscribe_start_ms": feed.sub_start_ms,
        "subscribe_start_utc": fmt_ms(feed.sub_start_ms),
        "subscribe_done_ms": feed.sub_sent_ms,
        "subscribe_elapsed_s": round((feed.sub_sent_ms - feed.sub_start_ms) / 1000, 3)
        if feed.sub_sent_ms else None,
        "subscribe_ack_count": feed.ack_count,
        "subscribe_last_ack_ms": feed.sub_ack_ms,
        "first_data_ms": feed.first_data_ms,
        "first_full_bar_open": info.get("first_full_bar_open"),
        "first_full_bar_open_utc": fmt_ms(info.get("first_full_bar_open")),
        "first_bar_guard_ms": FIRST_BAR_GUARD_MS,
        "reconnects": feed.reconnects,
        "outages": [{"start_ms": s, "end_ms": e, "start_utc": fmt_ms(s),
                     "duration_s": round(((e or now_ms()) - s) / 1000, 2)} for s, e in feed.outages],
        "trades_received": agg.trade_count,
        "duplicate_trades_dropped": agg.dup_count,
        "clock_offset": {
            "note": "[D6] 兩個獨立估計，腳本不自動扣除。min_push_lag_ms 為負代表本地時鐘快於交易所。"
                    "http_date_offset_ms 來自本次所有 REST 回應的 Date 標頭中位數，解析度只有 1 秒。",
            "min_push_lag_ms": agg.min_push_lag_ms,
            "max_push_lag_ms": agg.max_push_lag_ms,
            "http_date_offset_ms": float(np.median(ctx.http_date_offsets))
            if ctx.http_date_offsets else None,
            "http_date_samples": len(ctx.http_date_offsets),
        },
        "aborted": info.get("aborted"),
        "raw_message_samples": feed.raw_samples,
    }


# ============================== Stage 0 ==============================
def stage0(ctx):
    say("=== Stage 0：環境探測與候選清單 ===")
    require_deps(0)
    backup_if_exists(ctx, ctx.out_path)
    p = s4_signal.DEFAULT_PARAMS
    result = {"stage": 0, "method_notes": METHOD_NOTES[0]}

    symbols = fetch_symbols(ctx)
    trading = [s for s in symbols if _sym_is_trading(s)]
    say(f"symbols：{len(symbols)} 個（TRADING {len(trading)} 個）")

    tickers = fetch_tickers(ctx)
    say(f"tickers：{len(tickers)} 筆")
    tmap = {t.get("symbol"): t for t in tickers if t.get("symbol")}
    if not tmap:
        raise G2Error("tickers 回應裡找不到 symbol 欄位。→ 端點可能改版，請把 raw_ticker_sample 帶回給開發者。")

    # riskTable：路徑未經實測，失敗不致命（只是拿不到 maxLeverage）
    risk_by_symbol, risk_err = {}, None
    try:
        js = rest_get(ctx, ctx.args.risk_table_path, {"type": "PERP"}, "riskTable")
        data = js["data"]
        rows = data.get("riskTable") if isinstance(data, dict) else data
        if isinstance(rows, list):
            for r in rows:
                sym = r.get("symbol")
                tiers = r.get("tiers") or r.get("riskTiers") or r.get("levels")
                lev = None
                if isinstance(tiers, list) and tiers:
                    t0 = tiers[0]
                    if isinstance(t0, dict):
                        lev = t0.get("maxLeverage") or t0.get("leverage") or t0.get("maxLever")
                if lev is None:
                    lev = r.get("maxLeverage")
                if sym:
                    risk_by_symbol[sym] = None if lev is None else float(lev)
    except (G2Error, BudgetStop, KeyError, TypeError) as e:
        risk_err = f"{type(e).__name__}: {e}"
        ctx.note(f"riskTable 抓取失敗（不影響本階段其餘產出）：{risk_err}\n"
                 f"→ 若知道正確端點，用 --risk-table-path 指定；預設值 {ctx.args.risk_table_path} 未經實測。")

    rows, no_amount = [], 0
    for s in trading:
        sym = s.get("symbol")
        t = tmap.get(sym)
        amt = _ticker_amount(t) if t else None
        if amt is None:
            no_amount += 1
        rows.append({"symbol": sym, "amount_24h": amt,
                     "tier1_max_leverage": risk_by_symbol.get(sym),
                     "base": s.get("baseCurrency") or s.get("base"),
                     "quote": s.get("quoteCurrency") or s.get("quote")})
    if no_amount == len(rows):
        raise G2Error("所有 symbol 都取不到 24h 成交額欄位。→ tickers 的欄位名與假設不符，"
                      "請把 raw_ticker_sample 帶回給開發者（候選清單無法產生）。")
    lo, hi = p["MIN_TURN24H"], p["MAX_TURN24H"]
    cands = [r for r in rows
             if r["amount_24h"] is not None and r["amount_24h"] >= lo
             and (hi is None or r["amount_24h"] <= hi)]
    cands.sort(key=lambda r: r["amount_24h"], reverse=True)

    result.update({
        "fetched_at_ms": now_ms(),
        "fetched_at_utc": fmt_ms(now_ms()),
        "symbols_total": len(symbols),
        "symbols_trading": len(trading),
        "tickers_total": len(tickers),
        "symbols_without_amount": no_amount,
        "screen_band": {"MIN_TURN24H": lo, "MAX_TURN24H": hi},
        "candidates_count": len(cands),
        "candidates": cands,
        "all_symbols": rows,
        "risk_table": {"path_used": ctx.args.risk_table_path, "error": risk_err,
                       "symbols_with_leverage": sum(1 for v in risk_by_symbol.values() if v)},
        "raw_ticker_sample": tickers[:3],
        "raw_symbol_sample": symbols[:3],
    })
    result["metadata"] = build_meta(ctx, "環境探測與候選清單", {
        "risk_table_path": ctx.args.risk_table_path, "rate": ctx.args.rate,
        "max_requests": ctx.args.max_requests})
    write_json(ctx.out_path, result)
    say(f"候選（24h amount 落在 [{lo}, {hi}]）：{len(cands)} 個 / TRADING {len(trading)} 個")
    say(f"已寫出 {ctx.out_path}（共 {ctx.req_count} 次 REST 請求）")
    return result


def _load_stage0_candidates(ctx):
    """Stage 1/2 的 symbol 來源：優先讀 Stage 0 的結果，沒有就現抓 tickers。"""
    path = ctx.out_dir / RESULT_FILES[0]
    if path.exists():
        try:
            js = json.loads(path.read_text(encoding="utf-8"))
            cands = js.get("candidates") or []
            if cands:
                say(f"沿用 {path} 的候選清單（{len(cands)} 個，抓取時間 {js.get('fetched_at_utc')}）")
                return cands, str(path)
        except Exception as e:
            ctx.note(f"讀取 {path} 失敗（{e}），改為現抓 tickers。")
    say("找不到可用的 Stage 0 結果，現抓 tickers 產生候選清單（建議先跑 --stage 0）")
    p = s4_signal.DEFAULT_PARAMS
    rows = []
    for t in fetch_tickers(ctx):
        amt = _ticker_amount(t)
        if amt is not None:
            rows.append({"symbol": t.get("symbol"), "amount_24h": amt, "tier1_max_leverage": None})
    lo, hi = p["MIN_TURN24H"], p["MAX_TURN24H"]
    cands = [r for r in rows if r["amount_24h"] >= lo and (hi is None or r["amount_24h"] <= hi)]
    cands.sort(key=lambda r: r["amount_24h"], reverse=True)
    if not cands:
        raise G2Error("現抓 tickers 後仍然沒有任何候選。→ 請先跑 --stage 0 檢查 tickers 欄位是否正確。")
    return cands, "live tickers"


def pick_stage1_symbols(ctx, n):
    """挑樣本：涵蓋高/中/低成交額。取法固定（可重現）：
       候選依 24h amount 由大到小排序後，等距取 n-2 個，再加上候選裡最高與最低各 1 個。"""
    cands, src = _load_stage0_candidates(ctx)
    syms = [c["symbol"] for c in cands if c.get("symbol")]
    if len(syms) <= n:
        return syms, src
    idx = sorted({0, len(syms) - 1} | {round(i * (len(syms) - 1) / (n - 1)) for i in range(n)})
    return [syms[i] for i in idx][:n], src


# ============================== Stage 1 ==============================
def stage1(ctx):
    say("=== Stage 1：TRADE stream volume 還原驗證（決定方案 A 生死）===")
    require_deps(1)
    backup_if_exists(ctx, ctx.out_path)
    interval = ctx.args.interval
    bar = INTERVAL_MS[interval]
    n_bars = ctx.args.bars

    if ctx.args.symbols:
        symbols, src = [s.strip() for s in ctx.args.symbols.split(",") if s.strip()], "--symbols"
    else:
        symbols, src = pick_stage1_symbols(ctx, ctx.args.n_symbols)
    say(f"樣本 {len(symbols)} 個（來源 {src}）：{', '.join(symbols)}")
    say(f"K 棒週期 {interval}，目標 {n_bars} 根完整 K 棒，"
        f"收盤後等 {ctx.args.settle} 秒再抓交易所 K 棒比對")

    sealed_bars = []
    result = {"stage": 1, "method_notes": METHOD_NOTES[1], "interval": interval,
              "bar_ms": bar, "target_bars": n_bars, "symbols": symbols, "symbol_source": src,
              "settle_seconds": ctx.args.settle, "seal_grace_seconds": ctx.args.seal_grace,
              "bars": [], "local_only": []}

    def on_seal(bar_open, recs, extra):
        sealed_bars.append((bar_open, recs))
        # FR-4：每封存一根就 checkpoint，斷線不會讓前面白費
        result["local_only"] = [dict(r) for _, rs in sealed_bars for r in rs]
        result["metadata"] = build_meta(ctx, "TRADE 聚合驗證（收集中）", _stage1_params(ctx))
        write_json(ctx.out_path, result)

    feed, agg, info = asyncio.run(collect_bars(ctx, symbols, interval, n_bars, on_seal))
    # [D5] 回填封存後才抵達的成交。不做的話本地 volume 會被右設限而假性偏低，
    # 在輸出上跟「交易所計雙邊量」長得一模一樣，卻沒有任何線索分得出來。
    for _bo, _recs in sealed_bars:
        agg.apply_late_arrivals(_bo, _recs)
    result["feed"] = _feed_summary(ctx, feed, agg, info)

    if info.get("fatal") or not sealed_bars:
        result["verdict"] = {"label": "無法判定", "reason": "收集階段中止",
                             "next": "看 feed.raw_message_samples 與下面的錯誤訊息"}
        result["fatal_error"] = str(info.get("fatal")) if info.get("fatal") else None
        result["metadata"] = build_meta(ctx, "TRADE 聚合驗證（中止）", _stage1_params(ctx))
        write_json(ctx.out_path, result)
        if info.get("fatal"):
            raise info["fatal"]
        raise G2Error("一根完整 K 棒都沒有收集到。\n"
                      "→ 可能原因：訂閱格式不被接受、樣本 symbol 太冷門沒有成交、或連線一直斷。\n"
                      f"→ 請檢查 {ctx.out_path} 的 feed.raw_message_samples，看實際收到什麼。")

    # ---- 收盤後等 settle 秒，讓交易所 K 棒落定，再抓來比對 ----
    last_close = sealed_bars[-1][0] + bar
    wait = last_close + ctx.args.settle * 1000 - now_ms()
    if wait > 0:
        say(f"等待 {wait / 1000:.0f} 秒讓交易所 K 棒落定…")
        time.sleep(wait / 1000.0)

    first_open = sealed_bars[0][0]
    last_open = sealed_bars[-1][0]
    exch = {}
    for sym in symbols:
        try:
            exch[sym] = fetch_klines_window(ctx, sym, interval, first_open, last_open)
        except BudgetStop as e:
            ctx.note(f"{e}：交易所 K 棒只抓到 {len(exch)}/{len(symbols)} 個 symbol，其餘無法比對。")
            break
        except G2Error as e:
            ctx.note(f"抓 {sym} 的交易所 K 棒失敗：{e}")
            exch[sym] = {}

    bars_out, clean = [], []
    for bar_open, recs in sealed_bars:
        for r in recs:
            sym = r["symbol"]
            e = (exch.get(sym) or {}).get(bar_open)
            rec = {
                "symbol": sym, "bar_open": bar_open, "bar_open_utc": fmt_ms(bar_open),
                "bar_close": bar_open + bar,
                # [D5] 事後真值與封存快照並列；*_at_seal 是右設限值，只能當下界看
                "local": {k: r.get(k) for k in ("open", "high", "low", "close", "volume", "amount",
                                                "trades", "taker_buy_volume", "taker_sell_volume",
                                                "first_ts", "last_ts", "first_recv_ms", "last_recv_ms",
                                                "data_ready_ms", "trades_after_seal",
                                                "volume_at_seal", "amount_at_seal", "trades_at_seal",
                                                "data_ready_ms_at_seal", "volume_after_seal",
                                                "max_late_after_seal_ms")},
                "exchange": e,
                "flags": list(r["flags"]),
            }
            if e is None:
                rec["flags"].append("exchange_missing")
            rec["diff"] = _bar_diff(r, e)
            if not rec["flags"] and rec["diff"].get("volume_ratio") is not None:
                clean.append(rec)
            bars_out.append(rec)

    result["bars"] = bars_out
    result.pop("local_only", None)
    result["exchange_fetch"] = {"symbols_fetched": len(exch),
                                "note": "endTime 取 last_bar_open + BAR，回來後以 bar_open 逐根對號入座，"
                                        "不依賴 endTime 的邊界語意。"}
    result["summary"] = _stage1_summary(clean, bars_out, symbols)
    result["verdict"] = _stage1_verdict(result["summary"], smoke=ctx.args.smoke)
    # klines 抓完後 Date 標頭樣本才齊，這裡重算一次時鐘偏移估計
    result["feed"]["clock_offset"]["http_date_offset_ms"] = \
        float(np.median(ctx.http_date_offsets)) if ctx.http_date_offsets else None
    result["feed"]["clock_offset"]["http_date_samples"] = len(ctx.http_date_offsets)
    result["metadata"] = build_meta(ctx, "TRADE 聚合驗證", _stage1_params(ctx))
    write_json(ctx.out_path, result)

    s = result["summary"]
    say("")
    say(f"可比對（clean）K 棒：{s['clean_bars']} / 總 {s['total_bars']}")
    if s["clean_bars"]:
        say(f"volume_ratio（本地 ÷ 交易所） 中位數 {s['volume_ratio']['p50']:.4f}"
            f"  p05 {s['volume_ratio']['p05']:.4f}  p95 {s['volume_ratio']['p95']:.4f}")
        say(f"close 相對誤差 中位數 {s['close_rel']['p50']:.6f}")
    if s.get("bars_with_trades_after_seal"):
        say(f"[注意] 有 {s['bars_with_trades_after_seal']} 根在封存後還收到該根的成交 → "
            f"--seal-grace {ctx.args.seal_grace} 秒不夠。上面的比值已回填事後真值；"
            f"封存快照的右設限比值另存在 summary.volume_ratio_at_seal。")
    if ctx.args.smoke:
        say("※ 冒煙模式：以下判定不算數，只確認流程跑得完。不論結果是什麼，都要再跑一次正式版。")
    say(f"判定：{result['verdict']['label']} — {result['verdict']['reason']}")
    say(f"下一步：{result['verdict']['next']}")
    say(f"已寫出 {ctx.out_path}")
    return result


def _stage1_params(ctx):
    a = ctx.args
    return {"interval": a.interval, "bars": a.bars, "n_symbols": a.n_symbols,
            "symbols": a.symbols, "settle": a.settle, "seal_grace": a.seal_grace,
            "connections": a.connections, "sub_rate": a.sub_rate, "smoke": a.smoke,
            "ws_url": a.ws_url}


def fetch_klines_window(ctx, symbol, interval, first_open, last_open):
    """抓涵蓋 [first_open, last_open] 的交易所 K 棒，回傳 {bar_open: row}。"""
    bar = INTERVAL_MS[interval]
    limit = min(500, (last_open - first_open) // bar + 6)
    js = rest_get(ctx, "/api/v1/market/klines",
                  {"symbol": symbol, "interval": interval,
                   "endTime": last_open + bar, "limit": int(limit)}, f"klines {symbol}")
    kl = js["data"]["klines"]
    out = {}
    for k in kl:
        try:
            t = int(k["time"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (first_open <= t <= last_open):
            continue
        row = {"time": t}
        for c in ("open", "high", "low", "close", "volume", "amount"):
            if c in k:
                try:
                    row[c] = float(k[c])
                except (TypeError, ValueError):
                    row[c] = None
        out[t] = row
    if last_open not in out:
        ctx.note(f"{symbol}：交易所尚未提供 {fmt_ms(last_open)} 這根 K 棒（settle={ctx.args.settle}s 可能太短）。")
    return out


def _rel(a, b):
    if a is None or b is None or b == 0:
        return None
    return (a - b) / b


def _bar_diff(local, e):
    if not e:
        return {}
    d = {}
    for c in ("open", "high", "low", "close", "volume", "amount"):
        d[c + "_rel"] = _rel(local.get(c), e.get(c))
    ev, lv = e.get("volume"), local.get("volume")
    d["volume_ratio"] = (lv / ev) if (ev not in (None, 0) and lv is not None) else None
    # [D5] 封存快照算出來的同一個比值。與 volume_ratio 不同 → --seal-grace 不夠，
    # 那個差額會被誤讀成「交易所計雙邊量」。
    lvs = local.get("volume_at_seal")
    d["volume_ratio_at_seal"] = (lvs / ev) if (ev not in (None, 0) and lvs is not None) else None
    ea, la = e.get("amount"), local.get("amount")
    d["amount_ratio"] = (la / ea) if (ea not in (None, 0) and la is not None) else None
    tb, ts_ = local.get("taker_buy_volume"), local.get("taker_sell_volume")
    if ev not in (None, 0) and tb is not None:
        d["taker_buy_over_exchange"] = tb / ev
        d["taker_sell_over_exchange"] = ts_ / ev
    return d


def _stats(vals):
    v = [x for x in vals if x is not None and not (isinstance(x, float) and math.isnan(x))]
    if not v:
        return {"n": 0}
    a = np.asarray(v, dtype=float)
    return {"n": int(a.size), "mean": float(a.mean()), "p05": float(np.percentile(a, 5)),
            "p50": float(np.percentile(a, 50)), "p95": float(np.percentile(a, 95)),
            "min": float(a.min()), "max": float(a.max()),
            "std": float(a.std(ddof=1)) if a.size > 1 else 0.0}


def _stage1_summary(clean, all_bars, symbols):
    by_sym = {}
    for r in clean:
        by_sym.setdefault(r["symbol"], []).append(r["diff"].get("volume_ratio"))
    return {
        "total_bars": len(all_bars),
        "clean_bars": len(clean),
        "excluded": {f: sum(1 for r in all_bars if f in r["flags"])
                     for f in ("local_no_trades", "exchange_missing", "degraded_ws_outage")},
        "volume_ratio": _stats([r["diff"].get("volume_ratio") for r in clean]),
        "volume_ratio_at_seal": _stats([r["diff"].get("volume_ratio_at_seal") for r in clean]),
        "bars_with_trades_after_seal": sum(1 for r in all_bars
                                           if (r["local"].get("trades_after_seal") or 0) > 0),
        "amount_ratio": _stats([r["diff"].get("amount_ratio") for r in clean]),
        "close_rel": _stats([r["diff"].get("close_rel") for r in clean]),
        "open_rel": _stats([r["diff"].get("open_rel") for r in clean]),
        "high_rel": _stats([r["diff"].get("high_rel") for r in clean]),
        "low_rel": _stats([r["diff"].get("low_rel") for r in clean]),
        "taker_buy_over_exchange": _stats([r["diff"].get("taker_buy_over_exchange") for r in clean]),
        "volume_ratio_by_symbol": {k: _stats(v) for k, v in by_sym.items()},
        "symbols_requested": len(symbols),
    }


def _stage1_verdict(s, smoke=False):
    """現場用的粗判斷（真正的結論在 G2b，不以此為準）。

    smoke=True 時整個判定**不算數**：冒煙組態是 1 分 K、2 個 symbol、1 根，
    統計上沒有意義。標籤與下一步都會被改寫，成功與失敗兩個方向都一樣——
    使用者被 README 指示「跑完看最後三行」，那三行絕不能跟正式版長得一樣。
    """
    v = _stage1_verdict_core(s)
    if s.get("bars_with_trades_after_seal"):
        v["reason"] += (f"；另有 {s['bars_with_trades_after_seal']} 根在封存後還收到成交，"
                        "上面的比值已用事後真值（volume_ratio_at_seal 才是封存快照的右設限值）")
    if smoke:
        v["smoke"] = True
        v["label"] = "【冒煙・不算數】" + v["label"]
        v["reason"] = ("冒煙樣本在統計上沒有意義，這行只證明整條路徑跑得完。原始判定："
                       + v["reason"])
        v["next"] = "請接著跑正式版：python research/g2_measure.py --stage 1"
    return v


def _stage1_verdict_core(s):
    """給一個粗判斷方便現場決定要不要跑 Stage 2。真正的結論在 G2b，不以此為準。"""
    vr = s.get("volume_ratio") or {}
    if not vr.get("n"):
        return {"label": "無法判定", "reason": "沒有任何可比對的 K 棒（clean_bars = 0）",
                "next": "檢查 feed.raw_message_samples 與 flags，處理後重跑 Stage 1"}
    p50, p05, p95 = vr["p50"], vr["p05"], vr["p95"]
    cr = (s.get("close_rel") or {}).get("p50")
    close_ok = cr is not None and abs(cr) < 0.001
    if 0.98 <= p50 <= 1.02 and 0.9 <= p05 and p95 <= 1.1 and close_ok:
        return {"label": "volume 可還原（初判通過）",
                "reason": f"volume_ratio 中位數 {p50:.4f}，p05/p95 = {p05:.4f}/{p95:.4f}，收盤價也一致",
                "next": "可以進行 Stage 2"}
    if 0.45 <= p50 <= 0.55:
        return {"label": "疑似交易所計雙邊量",
                "reason": f"volume_ratio 中位數 {p50:.4f} 接近 0.5，本地只收到 taker 單邊",
                "next": "仍可進 Stage 2，但方案 A 需要一個固定換算係數，且要先確認係數穩定"}
    return {"label": "volume 無法還原（初判不通過）",
            "reason": f"volume_ratio 中位數 {p50:.4f}，p05/p95 = {p05:.4f}/{p95:.4f}，偏離 1 且不穩定",
            "next": "依 PRD §3 Stage 1，可以不跑 Stage 2，直接回報方案 A 不成立；"
                    "請把本 json 帶回 office 分析"}


# ============================== Stage 2 ==============================
def _late_row_update(agg, row, recs):
    """[D5] 把一根 K 棒的「封存快照」升級成「事後真值」，原快照降級成 *_at_seal。

    封存當下的 data_ready_ms 是右設限的（只看得到 grace 到期前收到的東西），
    trades_after_seal 在快照裡結構上恆為 0。回填後兩者才有判讀意義。
    """
    agg.apply_late_arrivals(row["bar_open"], recs)
    dr = [r["data_ready_ms"] for r in recs if r["data_ready_ms"] is not None]
    row["data_ready_ms_at_seal"] = row["data_ready_ms"]
    row["data_ready_ms_max_at_seal"] = row["data_ready_ms_max"]
    row["data_ready_ms"] = _stats(dr)
    row["data_ready_ms_max"] = max(dr) if dr else None
    row["per_symbol_data_ready_ms"] = {r["symbol"]: r["data_ready_ms"] for r in recs
                                       if r["data_ready_ms"] is not None}
    row["trades_after_seal"] = sum(r["trades_after_seal"] for r in recs)
    row["max_late_after_seal_ms"] = max([r["max_late_after_seal_ms"] for r in recs
                                         if r["max_late_after_seal_ms"] is not None] or [None])
    row["volume_after_seal_total"] = sum(r.get("volume_after_seal") or 0.0 for r in recs)
    return row


def stage2(ctx):
    say("=== Stage 2：訂閱容量與端到端延遲 ===")
    require_deps(2)
    backup_if_exists(ctx, ctx.out_path)
    interval = ctx.args.interval
    bar = INTERVAL_MS[interval]
    bph = bars_per_hour_of(interval)
    n_bars = ctx.args.bars

    if ctx.args.symbols:
        symbols, src = [s.strip() for s in ctx.args.symbols.split(",") if s.strip()], "--symbols"
    else:
        cands, src = _load_stage0_candidates(ctx)
        symbols = [c["symbol"] for c in cands if c.get("symbol")]
    if ctx.args.max_symbols and len(symbols) > ctx.args.max_symbols:
        symbols = symbols[: ctx.args.max_symbols]
        ctx.note(f"symbol 數被 --max-symbols 限制為 {len(symbols)}，"
                 "訂閱耗時與計算耗時都不能外推到全量，請在分析時註明。")
    say(f"訂閱 {len(symbols)} 個 symbol / {ctx.args.connections} 條連線，目標 {n_bars} 根 K 棒")

    # 指標計算需要歷史暖機（前 24 小時）。優先用本機快取，沒有就用合成資料（只有耗時有意義）。
    want = ctx.args.warm_bars or warm_bars_needed(bph)
    if want < warm_bars_needed(bph):
        ctx.note(f"--warm-bars {want} 不足以涵蓋 {interval} 的 24 小時暖機，"
                 f"已自動提高到 {warm_bars_needed(bph)}，否則指標全是 NaN、compute_ms 量不到東西。")
        want = warm_bars_needed(bph)
    hist, hist_src, filled_total = _prime_history(ctx, symbols, interval, bph, want)
    say(f"暖機歷史來源：{hist_src}（每個 symbol {want} 根，補洞 {filled_total} 根）")

    result = {"stage": 2, "method_notes": METHOD_NOTES[2], "interval": interval, "bar_ms": bar,
              "target_bars": n_bars, "symbols_count": len(symbols), "symbol_source": src,
              "compute_history_source": hist_src, "seal_grace_seconds": ctx.args.seal_grace,
              "warm_bars": want, "warm_bars_filled": filled_total, "bars": []}
    per_bar = []
    sealed_recs = []        # 與 per_bar 同序，收集結束後用來回填晚到成交（[D5]）

    def compute_cb(bar_open, recs):
        """[D5] 封存後立刻跑一次全市場 s4_signal，量『指標算完』的時間。"""
        t0 = time.perf_counter()
        cpu0 = time.process_time()
        sig_count = 0
        for r in recs:
            h = hist.get(r["symbol"])
            if h is None:
                continue
            _append_bar(h, r, bar_open, bar, want)
            df = pd.DataFrame(h)
            if len(df) < warm_bars_needed(bph):
                continue
            feats = s4_signal.features(df, bph)
            sig = s4_signal.signal_from_features(feats, None)
            if int(sig.iloc[-1]) == -1:
                sig_count += 1
        ms = (time.perf_counter() - t0) * 1000
        return {"compute_ms": ms, "compute_cpu_ms": (time.process_time() - cpu0) * 1000,
                "signals": sig_count, "compute_done_ms": now_ms()}

    def on_seal(bar_open, recs, extra):
        dr = [r["data_ready_ms"] for r in recs if r["data_ready_ms"] is not None]
        row = {
            "bar_open": bar_open, "bar_open_utc": fmt_ms(bar_open), "bar_close": bar_open + bar,
            "symbols_with_trades": sum(1 for r in recs if r["trades"] > 0),
            "symbols_total": len(recs),
            "trades_in_bar": sum(r["trades"] for r in recs),
            "seal_ms": recs[0]["seal_ms"] if recs else None,
            "seal_recv_ms": recs[0]["sealed_at_ms"] if recs else None,
            "data_ready_ms": _stats(dr),
            "data_ready_ms_max": max(dr) if dr else None,
            "trades_after_seal": sum(r["trades_after_seal"] for r in recs),
            "max_late_after_seal_ms": max([r["max_late_after_seal_ms"] for r in recs
                                           if r["max_late_after_seal_ms"] is not None] or [None]),
            "degraded": any("degraded_ws_outage" in r["flags"] for r in recs),
            "compute_ms": (extra or {}).get("compute_ms"),
            "compute_cpu_ms": (extra or {}).get("compute_cpu_ms"),
            "signals": (extra or {}).get("signals"),
            "compute_done_ms": (extra or {}).get("compute_done_ms"),
            "per_symbol_data_ready_ms": {r["symbol"]: r["data_ready_ms"] for r in recs
                                         if r["data_ready_ms"] is not None},
        }
        row["total_ms"] = (row["compute_done_ms"] - row["bar_close"]) if row["compute_done_ms"] else None
        per_bar.append(row)
        sealed_recs.append(recs)
        result["bars"] = per_bar
        result["metadata"] = build_meta(ctx, "訂閱容量與端到端延遲（收集中）", _stage2_params(ctx))
        write_json(ctx.out_path, result)     # FR-4 checkpoint

    feed, agg, info = asyncio.run(collect_bars(ctx, symbols, interval, n_bars, on_seal, compute_cb))
    # [D5] 收集結束才回填晚到成交：on_seal 執行的當下不可能知道後面還會來什麼，
    # 但輸出如果停在那個快照，data_ready_ms 就是右設限值、trades_after_seal 恆為 0。
    for _row, _recs in zip(per_bar, sealed_recs):
        _late_row_update(agg, _row, _recs)
    result["feed"] = _feed_summary(ctx, feed, agg, info)
    result["fatal_error"] = str(info.get("fatal")) if info.get("fatal") else None
    result["summary"] = {
        "bars_measured": len(per_bar),
        "subscribe_elapsed_s": result["feed"]["subscribe_elapsed_s"],
        "data_ready_ms_max": _stats([b["data_ready_ms_max"] for b in per_bar]),
        "data_ready_ms_max_at_seal": _stats([b["data_ready_ms_max_at_seal"] for b in per_bar]),
        "seal_ms": _stats([b["seal_ms"] for b in per_bar]),
        "compute_ms": _stats([b["compute_ms"] for b in per_bar]),
        "total_ms": _stats([b["total_ms"] for b in per_bar]),
        "bars_with_trades_after_seal": sum(1 for b in per_bar if (b["trades_after_seal"] or 0) > 0),
        "degraded_bars": sum(1 for b in per_bar if b["degraded"]),
        "coverage_pct": _stats([100.0 * b["symbols_with_trades"] / max(1, b["symbols_total"])
                                for b in per_bar]),
    }
    result["metadata"] = build_meta(ctx, "訂閱容量與端到端延遲", _stage2_params(ctx))
    write_json(ctx.out_path, result)
    if info.get("fatal"):
        raise info["fatal"]
    s = result["summary"]
    say("")
    say(f"訂閱 {len(symbols)} 個耗時 {s['subscribe_elapsed_s']} 秒")
    if s["total_ms"].get("n"):
        say(f"total_ms（K 棒收盤 → 指標算完）中位數 {s['total_ms']['p50']:.0f} ms，"
            f"最大 {s['total_ms']['max']:.0f} ms")
    if s["bars_with_trades_after_seal"]:
        say(f"[注意] 有 {s['bars_with_trades_after_seal']} 根在封存後還收到該根的成交 → "
            f"--seal-grace {ctx.args.seal_grace} 秒不夠，分析時要把這些根排除或加大 grace 重跑")
    say(f"已寫出 {ctx.out_path}")
    return result


def _stage2_params(ctx):
    a = ctx.args
    return {"interval": a.interval, "bars": a.bars, "connections": a.connections,
            "sub_rate": a.sub_rate, "seal_grace": a.seal_grace, "warm_bars": a.warm_bars,
            "max_symbols": a.max_symbols, "smoke": a.smoke, "ws_url": a.ws_url,
            "cache_dir": str(a.cache_dir)}


def warm_bars_needed(bph):
    """暖機至少要涵蓋 24 小時（turn/volr）+ 2 小時（ret2h），多留 8 根緩衝。"""
    return 26 * int(bph) + 8


def _prime_history(ctx, symbols, interval, bph, want):
    """給 Stage 2 的計算步驟準備暖機歷史。不打網路：有快取就讀快取，沒有就合成。"""
    bar = INTERVAL_MS[interval]
    want = int(want)
    hist, filled_total, from_cache = {}, 0, 0
    cache_ok = os.path.isdir(str(ctx.args.cache_dir))
    end_open = bar_open_of(now_ms(), bar) - bar
    rng = np.random.default_rng(ctx.args.seed)
    for sym in symbols:
        df = None
        if cache_ok and os.path.exists(cache_path(ctx.args.cache_dir, sym, interval)):
            try:
                df, f = load_cache_frame(ctx.args.cache_dir, sym, interval, want)
                filled_total += f
                from_cache += 1
            except G2Error as e:
                ctx.note(f"{sym} 快取讀取失敗，改用合成暖機：{e}")
                df = None
        if df is None or len(df) < want:
            df = synth_frame(want, bar, end_open, rng, bars_per_hour=bph)
        if "amount" not in df.columns:
            df = df.copy()
            df["amount"] = df["close"] * df["volume"]
        hist[sym] = {c: list(df[c].to_numpy()) for c in
                     ("time", "open", "high", "low", "close", "volume", "amount")}
    if from_cache == len(symbols):
        src = f"pionex_cache（{ctx.args.cache_dir}）"
    elif from_cache == 0:
        src = "synthetic（本機沒有對應快取；只有耗時有意義，訊號數沒有意義）"
    else:
        src = f"mixed：{from_cache}/{len(symbols)} 來自快取，其餘為 synthetic"
    return hist, src, filled_total


def _append_bar(h, rec, bar_open, bar, max_len):
    """把封存的 K 棒接到暖機歷史後面。沒有成交的根依輸入契約補成 close=前值、量=0（AC-3）。"""
    prev_close = h["close"][-1] if h["close"] else 1.0
    c = rec["close"] if rec["close"] is not None else prev_close
    o = rec["open"] if rec["open"] is not None else c
    hi = rec["high"] if rec["high"] is not None else c
    lo = rec["low"] if rec["low"] is not None else c
    h["time"].append(bar_open)
    h["open"].append(o)
    h["high"].append(hi)
    h["low"].append(lo)
    h["close"].append(c)
    h["volume"].append(rec["volume"])
    h["amount"].append(rec["amount"])
    if len(h["time"]) > max_len:
        for k in h:
            del h[k][: len(h[k]) - max_len]


# ============================== Stage 3 ==============================
def stage3(ctx):
    say("=== Stage 3：指標計算耗時（不需網路）===")
    backup_if_exists(ctx, ctx.out_path)
    interval = ctx.args.interval
    bar = INTERVAL_MS[interval]
    bph = bars_per_hour_of(interval)
    n_sym, n_bars = ctx.args.n_frames, ctx.args.frame_bars
    need = 24 * bph + 2 * bph + 2
    if n_bars < need:
        raise G2Error(f"--frame-bars {n_bars} 太少：{interval} 的暖機需要至少 {need} 根"
                      f"（24 小時 {24 * bph} 根 + 2 小時 {2 * bph} 根），否則特徵全是 NaN。")

    frames, source, filled_total, syms = [], None, 0, []
    src_opt = ctx.args.source
    if src_opt in ("cache", "auto"):
        try:
            names = list_cache_symbols(ctx.args.cache_dir, interval)
            say(f"快取裡有 {len(names)} 個 symbol，取前 {min(n_sym, len(names))} 個")
            for sym in names[:n_sym]:
                df, f = load_cache_frame(ctx.args.cache_dir, sym, interval, n_bars)
                if len(df) < need:
                    continue
                frames.append(df)
                filled_total += f
                syms.append(sym)
            if not frames:
                raise G2Error(f"快取裡沒有任何 symbol 的 {interval} 資料長度達到 {need} 根。")
            source = f"pionex_cache（{ctx.args.cache_dir}）"
        except G2Error as e:
            if src_opt == "cache":
                raise
            ctx.note(f"讀不到快取，改用合成資料：{e}")
            frames = []
    if not frames:
        rng_end = bar_open_of(now_ms(), bar) - bar
        uni = synth_universe(n_sym, n_bars, bar, rng_end, ctx.args.seed, bph)
        syms = list(uni)
        frames = [uni[s] for s in syms]
        source = f"synthetic（seed={ctx.args.seed}）"
    say(f"資料來源：{source}；{len(frames)} 個 frame × {n_bars} 根")

    # 暖機：排除第一次呼叫的一次性成本（pandas 內部快取等）
    warm = min(ctx.args.warmup_frames, len(frames))
    for df in frames[:warm]:
        s4_signal.signal(df, bph)

    per_ms, sig_total, nan_rows = [], 0, 0
    t_all0, cpu_all0 = time.perf_counter(), time.process_time()
    for df in frames:
        t0 = time.perf_counter()
        feats = s4_signal.features(df, bph)
        sig = s4_signal.signal_from_features(feats, None)
        per_ms.append((time.perf_counter() - t0) * 1000)
        sig_total += int((sig == -1).sum())
        nan_rows += int(feats["turn"].isna().sum())
    wall = time.perf_counter() - t_all0
    cpu = time.process_time() - cpu_all0

    st = _stats(per_ms)
    n = len(frames)
    result = {
        "stage": 3, "method_notes": METHOD_NOTES[3],
        "data_source": source, "interval": interval, "bars_per_hour": bph,
        "frames_measured": n, "bars_per_frame": n_bars, "warmup_frames_excluded": warm,
        "symbols": syms[:50], "symbols_truncated": len(syms) > 50,
        "per_frame_ms": [round(x, 4) for x in per_ms],
        "per_frame_ms_stats": st,
        "total_wall_s": round(wall, 4),
        "total_cpu_s": round(cpu, 4),
        "signals_found": sig_total,
        "projection": {
            "note": "以實測的每 frame 中位數 × 611 推估全市場一次計算的耗時（單執行緒、同一台機器）。",
            "universe_611_wall_s": round(st.get("p50", 0) / 1000 * 611, 4) if st.get("n") else None,
            "measured_universe_wall_s": round(wall, 4),
            "bar_period_s": bar / 1000,
            "fits_in_bar": (wall < bar / 1000) if n else None,
        },
        "input_contract": {
            "note": "AC-3：餵給 s4_signal 的 DataFrame 一律 time 升冪、固定週期、無缺漏。",
            "bars_per_hour_is_int": isinstance(bph, int),
            "columns": list(frames[0].columns) if frames else [],
            "filled_bars_total": filled_total,
            "time_monotonic_all": all(bool(df["time"].is_monotonic_increasing) for df in frames),
            "fixed_period_all": all(bool((df["time"].diff().dropna() == bar).all()) for df in frames),
            "nan_feature_rows_total": nan_rows,
            # turn = rolling(24*bph).sum()，前 24*bph-1 根湊不滿視窗 → NaN。
            # 實測值應該等於 (24*bph-1) × frames_measured；不等就代表資料有洞或長度不足。
            "expected_nan_rows_per_frame": 24 * bph - 1,
            "expected_nan_rows_total": (24 * bph - 1) * len(frames),
        },
        "environment": {"cpu_count": os.cpu_count()},
    }
    result["metadata"] = build_meta(ctx, "指標計算耗時", {
        "n_frames": n_sym, "frame_bars": n_bars, "source": src_opt, "seed": ctx.args.seed,
        "cache_dir": str(ctx.args.cache_dir), "interval": interval,
        "warmup_frames": ctx.args.warmup_frames})
    write_json(ctx.out_path, result)
    say("")
    say(f"每個 frame：中位數 {st['p50']:.3f} ms、p95 {st['p95']:.3f} ms、最大 {st['max']:.3f} ms")
    say(f"{n} 個 frame 合計 wall {wall * 1000:.0f} ms / CPU {cpu * 1000:.0f} ms")
    proj = result["projection"]["universe_611_wall_s"]
    if proj is not None:
        say(f"推估 611 個 symbol：{proj * 1000:.0f} ms（K 棒週期 {bar // 1000} 秒，"
            f"{'放得下' if proj < bar / 1000 else '放不下'}）")
    say(f"訊號數 {sig_total}（合成資料時此數字沒有市場意義，只證明不是在跑全 NaN 的空路徑）")
    say(f"已寫出 {ctx.out_path}")
    return result


# ============================== Stage 4 ==============================
def stage4(ctx):
    say("=== Stage 4：方案 B 粗篩評估與漏訊號率 ===")
    backup_if_exists(ctx, ctx.out_path)
    interval = ctx.args.interval
    bar = INTERVAL_MS[interval]
    bph = bars_per_hour_of(interval)
    p = s4_signal.DEFAULT_PARAMS
    report_lag = int(ctx.args.screen_lag)
    # [D7] LAG 對漏失率不是單調的，單一個 LAG 的數字沒有代表性 → 一次掃 0/1/2 並列出
    lags = sorted({int(x) for x in str(ctx.args.screen_lags).split(",") if x.strip()} | {report_lag})
    need = 24 * bph + 2 * bph + 2

    names = list_cache_symbols(ctx.args.cache_dir, interval)
    if ctx.args.max_symbols:
        names = names[: ctx.args.max_symbols]
    say(f"回放 {len(names)} 個 symbol 的 {interval} 快取（--cache-dir {ctx.args.cache_dir}）")

    margins = [float(x) for x in str(ctx.args.margins).split(",") if x.strip()]
    if 1.0 not in margins:
        margins.insert(0, 1.0)
    say(f"LAG 掃描：{lags}（報告主值 LAG={report_lag}）；margin 掃描：{margins}")

    baseline_n = 0
    missed_by_lag = {g: [] for g in lags}
    per_cell = {(g, m): {"screened": 0, "missed": 0, "candidate_bar_sum": 0.0, "bars": 0}
                for g in lags for m in margins}
    cand_counts = {}        # bar_open -> 在 report_lag、margin=1.0 下通過粗篩的 symbol 數
    used, skipped, filled_total, bars_total = [], [], 0, 0

    for i, sym in enumerate(names, 1):
        try:
            df, f = load_cache_frame(ctx.args.cache_dir, sym, interval, ctx.args.replay_bars)
        except G2Error as e:
            skipped.append({"symbol": sym, "reason": str(e)[:200]})
            continue
        if len(df) < need:
            skipped.append({"symbol": sym, "reason": f"資料只有 {len(df)} 根，不足暖機 {need} 根"})
            continue
        filled_total += f
        used.append(sym)
        bars_total += len(df)
        feats = s4_signal.features(df, bph)
        sig = s4_signal.signal_from_features(feats, None)
        turn = feats["turn"]
        fires = np.flatnonzero((sig == -1).to_numpy())
        baseline_n += int(fires.size)
        times = df["time"].to_numpy()
        for lag in lags:
            turn_lag = turn.shift(lag)          # [D7] 粗篩只能用 t-LAG 的資訊
            for m in margins:
                lo = p["MIN_TURN24H"] * m
                hi = None if p["MAX_TURN24H"] is None else p["MAX_TURN24H"] / m
                passes = (turn_lag >= lo)
                if hi is not None:
                    passes &= (turn_lag <= hi)
                passes = passes.fillna(False).to_numpy()
                hit = int(passes[fires].sum()) if fires.size else 0
                cell = per_cell[(lag, m)]
                cell["screened"] += hit
                cell["missed"] += int(fires.size) - hit
                cell["candidate_bar_sum"] += float(passes.sum())
                cell["bars"] += int(passes.size)
                if m != 1.0:
                    continue
                if lag == report_lag:
                    for t in times[np.flatnonzero(passes)]:
                        cand_counts[int(t)] = cand_counts.get(int(t), 0) + 1
                for j in fires:
                    if passes[j]:
                        continue
                    tl = float(turn_lag.iloc[j])
                    if math.isnan(tl):
                        reason = "turn[t-lag] 還在暖機期（NaN），粗篩無從判斷"
                        tl_out = None
                    elif tl < p["MIN_TURN24H"]:
                        reason = "turn[t-lag] 低於 MIN_TURN24H：爆量那一根才把 24h 成交額推進區間，粗篩看的是上一根 → 漏掉"
                        tl_out = tl
                    else:
                        reason = "turn[t-lag] 高於 MAX_TURN24H：這根之前 24h 成交額已經衝出上限，粗篩把它濾掉"
                        tl_out = tl
                    missed_by_lag[lag].append({
                        "symbol": sym, "screen_lag_bars": lag, "bar_open": int(times[j]),
                        "bar_open_utc": fmt_ms(int(times[j])),
                        "turn_at_t": float(turn.iloc[j]), "turn_at_t_minus_lag": tl_out,
                        "ret2h": float(feats["ret2h"].iloc[j]),
                        "volr": float(feats["volr"].iloc[j]),
                        "cpos": float(feats["cpos"].iloc[j]),
                        "reason": reason,
                    })
        if i % 50 == 0:
            say(f"  已處理 {i}/{len(names)} 個 symbol…")

    if not used:
        raise G2Error(f"沒有任何 symbol 的資料長度達到暖機需求（{need} 根）。"
                      "→ 請確認 --cache-dir / --interval，或在 laptop 上先跑一次回測把快取補滿。")

    cc = list(cand_counts.values())

    def _margin_out(lag):
        rows = []
        for m in margins:
            d = per_cell[(lag, m)]
            tot = d["screened"] + d["missed"]
            rows.append({
                "margin": m,
                "screen_band": [p["MIN_TURN24H"] * m,
                                None if p["MAX_TURN24H"] is None else p["MAX_TURN24H"] / m],
                "signals_caught": d["screened"], "signals_missed": d["missed"],
                "missed_rate": (d["missed"] / tot) if tot else None,
                # pass_rate = 「任一 symbol 在任一根 K 棒通過粗篩」的比例；
                # 乘上全市場 symbol 數就是每根 K 棒要打幾次 klines 的期望值。
                "avg_pass_rate": (d["candidate_bar_sum"] / d["bars"]) if d["bars"] else None,
                "expected_candidates_per_bar_at_611": round(d["candidate_bar_sum"] / d["bars"] * 611, 1)
                if d["bars"] else None,
            })
        return rows

    lag_sweep = [{"screen_lag_bars": lag,
                  "is_report_lag": lag == report_lag,
                  "margin_sweep": _margin_out(lag),
                  "missed_signals": missed_by_lag[lag][: ctx.args.max_missed_records],
                  "missed_signals_total": len(missed_by_lag[lag]),
                  "missed_signals_truncated": len(missed_by_lag[lag]) > ctx.args.max_missed_records}
                 for lag in lags]
    margin_out = _margin_out(report_lag)
    missed_records = missed_by_lag[report_lag]

    stage0_path = ctx.out_dir / RESULT_FILES[0]
    ticker_screen = None
    if stage0_path.exists():
        try:
            js = json.loads(stage0_path.read_text(encoding="utf-8"))
            ticker_screen = {"source": str(stage0_path), "fetched_at_utc": js.get("fetched_at_utc"),
                             "symbols_trading": js.get("symbols_trading"),
                             "candidates_count": js.get("candidates_count"),
                             "note": "這是 tickers 快照的粗篩結果（實盤真正會用的粗篩）；"
                                     "與下面用 klines turn 回放出來的 pass rate 不是同一個定義，見 [D7]。"}
        except Exception as e:
            ctx.note(f"讀取 {stage0_path} 失敗：{e}")

    result = {
        "stage": 4, "method_notes": METHOD_NOTES[4], "interval": interval,
        "bars_per_hour": bph, "screen_lag_bars": report_lag,
        "screen_lags_swept": lags,
        "screen_band_base": {"MIN_TURN24H": p["MIN_TURN24H"], "MAX_TURN24H": p["MAX_TURN24H"]},
        "cache_dir": str(ctx.args.cache_dir),
        "symbols_used": len(used), "symbols_skipped": len(skipped),
        "skipped_detail": skipped[:50],
        "bars_replayed_total": bars_total,
        "baseline_signals": baseline_n,
        # lag_sweep 是主結果；margin_sweep / missed_signals* 是 screen_lag_bars 那一列的複本，
        # 只為了不打斷既有讀法，判讀請以 lag_sweep 為準（漏失率對 LAG 非單調，見 [D7]）。
        "lag_sweep": lag_sweep,
        "margin_sweep": margin_out,
        "missed_signals": missed_records[: ctx.args.max_missed_records],
        "missed_signals_truncated": len(missed_records) > ctx.args.max_missed_records,
        "missed_signals_total": len(missed_records),
        "candidates_per_bar": {
            "note": f"screen_lag_bars={report_lag}、margin=1.0 時，每根 K 棒通過粗篩、"
                    "需要打 klines 的 symbol 數。除以 rate limit 10 req/s 就是方案 B 每根 K 棒的 REST 秒數。",
            **_stats(cc),
            "rest_seconds_at_10rps_p95": (np.percentile(cc, 95) / 10.0) if cc else None,
        },
        "ticker_screen_from_stage0": ticker_screen,
        "input_contract": {"filled_bars_total": filled_total, "bars_per_hour_is_int": isinstance(bph, int)},
    }
    result["metadata"] = build_meta(ctx, "方案 B 粗篩評估", {
        "cache_dir": str(ctx.args.cache_dir), "screen_lag": report_lag, "screen_lags": lags,
        "margins": margins,
        "max_symbols": ctx.args.max_symbols, "replay_bars": ctx.args.replay_bars,
        "interval": interval})
    write_json(ctx.out_path, result)

    def _pct(x):
        return "—" if x is None else f"{x * 100:.2f}%"

    say("")
    say(f"baseline 訊號 {baseline_n} 筆（{len(used)} 個 symbol、{bars_total} 根 K 棒）")
    say("漏失率（列＝粗篩落後幾根 LAG，欄＝margin 放寬倍數）：")
    say("  LAG   " + "  ".join(f"margin={m:<6g}" for m in margins))
    for row in lag_sweep:
        cells = "  ".join(f"{_pct(x['missed_rate']):<13}" for x in row["margin_sweep"])
        say(f"  {row['screen_lag_bars']:<6}{cells}"
            + ("  ← 主值" if row["is_report_lag"] else ""))
    say("  每根 K 棒候選數（611 symbol 推估）：" + "  ".join(
        f"margin={x['margin']:g}→{x['expected_candidates_per_bar_at_611']}"
        for x in margin_out))
    if cc:
        say(f"LAG={report_lag} 實測：每根 K 棒平均打 {np.mean(cc):.1f} 次 klines"
            f"（p95 {np.percentile(cc, 95):.0f} 次 → 10 req/s 下約 {np.percentile(cc, 95) / 10:.1f} 秒）")
    say("提醒：漏失率對 LAG 不是單調的（roll-in / roll-out 是兩件事），不要只看一個 LAG。")
    say("提醒：這個數字是雙向有偏、偏移量未知——LAG≥1 的悲觀讓實測值偏高，"
        "tickers 與 turn 的定義落差讓真值偏高；兩邊誰大未知，不可當上界也不可當下界（見 [D7]）")
    say(f"已寫出 {ctx.out_path}")
    return result


# ============================== CLI ==============================
EPILOG = """\
建議執行順序（詳見 research/README_G2.md）：

  0) 離線自檢（不連網，約 1 分鐘）
     python research/g2_selftest.py

  1) Stage 0  環境探測（個位數請求，約 10 秒）
     python research/g2_measure.py --stage 0

  2) Stage 1  先冒煙再正式（冒煙用 1 分 K，約 4 分鐘；正式約 25 分鐘）
     python research/g2_measure.py --stage 1 --smoke
     python research/g2_measure.py --stage 1

     >>> Stage 1 判定 volume 無法還原 → 方案 A 不成立，直接跳過 Stage 2 <<<

  3) Stage 2  先冒煙再正式（冒煙約 5 分鐘；正式約 110 分鐘）
     python research/g2_measure.py --stage 2 --smoke
     python research/g2_measure.py --stage 2

  4) Stage 3  不需網路（約 1 分鐘）
     python research/g2_measure.py --stage 3

  5) Stage 4  不需網路，需要 pionex_cache（約 2-10 分鐘）
     python research/g2_measure.py --stage 4

  6) 把結果帶回
     git add research/results && git commit -m "G2 measurement results" && git push
"""


def build_parser():
    ap = argparse.ArgumentParser(
        prog="g2_measure.py",
        description="G2 — 5 分 K 即時資料層可行性量測（分階段獨立執行）",
        epilog=EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stage", type=int, required=True, choices=[0, 1, 2, 3, 4],
                    help="要執行的階段：0 環境探測 / 1 TRADE 聚合驗證 / 2 延遲 / 3 計算耗時 / 4 粗篩")
    ap.add_argument("--smoke", action="store_true",
                    help="冒煙模式：用極小規模跑完整條路徑（Stage 1/2 改用 1 分 K、少數 symbol、1-2 根）")

    g = ap.add_argument_group("共用")
    g.add_argument("--out-dir", default=None, help="結果輸出目錄（預設 research/results）")
    g.add_argument("--tmp-dir", default=None, help="中間產物目錄（預設 research/_tmp，不進版控）")
    g.add_argument("--machine", default=None, help="機器識別，寫進 metadata（預設 hostname）")
    g.add_argument("--interval", default="5M", choices=sorted(INTERVAL_MS),
                   help="K 棒週期（預設 5M）")
    g.add_argument("--seed", type=int, default=20260921, help="合成資料亂數種子（預設 20260921）")

    g = ap.add_argument_group("網路（Stage 0/1/2）")
    g.add_argument("--ca-bundle", default=None,
                   help="自訂 CA bundle 路徑（公司網路 TLS 攔截時才需要；laptop 不用）")
    g.add_argument("--rate", type=float, default=5.0,
                   help="REST 請求速率上限 req/s（派網上限 10，預設 5 留安全邊際）")
    g.add_argument("--max-requests", type=int, default=2000,
                   help="本次執行的 REST 請求總數上限，達到就停止並保存已有結果（預設 2000）")
    g.add_argument("--ws-url", default=WS_URL, help=f"public WS 端點（預設 {WS_URL}）")
    g.add_argument("--ws-open-timeout", type=float, default=15.0, help="WS 連線逾時秒數（預設 15）")
    g.add_argument("--ws-ping-interval", type=float, default=20.0, help="WS 心跳秒數（預設 20）")
    g.add_argument("--connections", type=int, default=4,
                   help="WS 並發連線數（派網每 IP 上限 10，預設 4；Stage 2 建議 10）")
    g.add_argument("--sub-rate", type=float, default=4.0,
                   help="每條連線每秒送幾則 SUBSCRIBE（派網上限 5，預設 4）")
    g.add_argument("--seal-grace", type=float, default=2.0,
                   help="K 棒收盤後再等幾秒才封存（預設 2）。封存後還收到該根成交會被計數。")
    g.add_argument("--max-minutes", type=float, default=180.0,
                   help="單一階段的最長執行時間，超過就提前收工並保存結果（預設 180 分）")
    g.add_argument("--raw-samples", type=int, default=20,
                   help="保存前幾則 WS 原始訊息到結果 json，供 office 核對欄位假設（預設 20）")

    g = ap.add_argument_group("Stage 0")
    g.add_argument("--risk-table-path", default="/api/v1/common/riskTable",
                   help="riskTable 端點路徑（未經實測，抓不到不影響其餘產出）")

    g = ap.add_argument_group("Stage 1 / 2")
    g.add_argument("--bars", type=int, default=None,
                   help="要收集幾根完整 K 棒（Stage 1 預設 4、Stage 2 預設 20）")
    g.add_argument("--symbols", default=None,
                   help="指定 symbol 清單，逗號分隔（不指定則依候選清單自動挑）")
    g.add_argument("--n-symbols", type=int, default=10, help="Stage 1 自動挑幾個樣本（預設 10）")
    g.add_argument("--max-symbols", type=int, default=None,
                   help="Stage 2/4 最多處理幾個 symbol（不設就是全部）")
    g.add_argument("--settle", type=float, default=30.0,
                   help="Stage 1：最後一根收盤後等幾秒再抓交易所 K 棒比對（預設 30）")
    g.add_argument("--warm-bars", type=int, default=None,
                   help="Stage 2：每個 symbol 的暖機歷史根數（預設依週期自動算，5M → 320）")

    g = ap.add_argument_group("Stage 3")
    g.add_argument("--n-frames", type=int, default=611,
                   help="要量測幾個 DataFrame（預設 611 = PERP TRADING 全量）")
    g.add_argument("--frame-bars", type=int, default=320,
                   help="每個 DataFrame 幾根 K 棒（預設 320，需涵蓋 24 小時暖機）")
    g.add_argument("--source", default="auto", choices=["auto", "cache", "synthetic"],
                   help="資料來源：auto（有快取用快取、沒有就合成）/ cache / synthetic")
    g.add_argument("--warmup-frames", type=int, default=3,
                   help="前幾個 frame 不列入統計（排除首次呼叫成本，預設 3）")

    g = ap.add_argument_group("Stage 3 / 4")
    g.add_argument("--cache-dir", default=str(REPO_ROOT / "pionex_cache"),
                   help="K 棒快取目錄（預設 <repo>/pionex_cache）")

    g = ap.add_argument_group("Stage 4")
    g.add_argument("--screen-lag", type=int, default=1,
                   help="報告主值用哪個 LAG（預設 1，代表用上一根收盤後的資訊）。"
                        "這只決定 margin_sweep/candidates_per_bar 取哪一列，全部 LAG 都會算。")
    g.add_argument("--screen-lags", default="0,1,2",
                   help="要同時計算的粗篩落後根數，逗號分隔（預設 0,1,2）。"
                        "漏失率對 LAG 非單調，單一個 LAG 的數字沒有代表性，見 [D7]。")
    g.add_argument("--margins", default="1.0,0.75,0.5,0.25",
                   help="粗篩區間放寬倍率掃描，逗號分隔（預設 1.0,0.75,0.5,0.25）")
    g.add_argument("--replay-bars", type=int, default=None,
                   help="每個 symbol 最多回放幾根 K 棒（不設就是快取全部）")
    g.add_argument("--max-missed-records", type=int, default=2000,
                   help="漏訊號明細最多輸出幾筆（預設 2000）")
    return ap


def apply_defaults(args):
    """階段相關的預設值與 --smoke 的縮小規模。"""
    if args.bars is None:
        args.bars = {1: 4, 2: 20}.get(args.stage, 4)
    if args.smoke:
        if args.stage in (1, 2):
            # 冒煙刻意改用 1 分 K：同一條程式路徑（含 REST 比對），但 4 分鐘內就跑完
            args.interval = "1M"
            args.bars = 1 if args.stage == 1 else 2
            args.n_symbols = 2
            if args.stage == 2:
                args.max_symbols = args.max_symbols or 4
                args.connections = min(args.connections, 2)
            args.settle = min(args.settle, 20.0)
        elif args.stage == 3:
            args.n_frames = min(args.n_frames, 20)
        elif args.stage == 4:
            args.max_symbols = args.max_symbols or 5
        elif args.stage == 0:
            pass
    if args.stage == 2 and args.connections < 1:
        args.connections = 1
    return args


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = apply_defaults(build_parser().parse_args(argv))
    try:
        ctx = Ctx(args)
    except G2Error as e:
        print(f"[錯誤] {e}", file=sys.stderr, flush=True)
        return 2
    say(f"{SCRIPT_VERSION}　stage={args.stage}　machine={ctx.machine}"
        + ("　[冒煙模式]" if args.smoke else ""))
    say(f"輸出：{ctx.out_path}")
    say("")
    fn = {0: stage0, 1: stage1, 2: stage2, 3: stage3, 4: stage4}[args.stage]
    try:
        fn(ctx)
    except BudgetStop as e:
        print(f"\n[停止] {e}\n→ 已有的結果保留在 {ctx.out_path}（可能不完整）。"
              f"\n→ 要繼續請加大 --max-requests 後重跑。", file=sys.stderr, flush=True)
        return 3
    except G2Error as e:
        print(f"\n[錯誤] {e}", file=sys.stderr, flush=True)
        return 2
    except KeyboardInterrupt:
        print(f"\n[中斷] 使用者按了 Ctrl-C。已寫出的部分結果保留在 {ctx.out_path}。",
              file=sys.stderr, flush=True)
        return 130
    except Exception as e:
        # FR-5 兜底：預期外的例外也要告訴使用者「東西在哪、要帶什麼回來」，
        # 不能只丟一段 traceback 就結束，否則跑了兩小時的人不知道該怎麼辦。
        traceback.print_exc()
        print(f"\n[預期外的錯誤] {type(e).__name__}: {e}"
              f"\n→ 這不是本腳本預期的錯誤路徑，上面的 traceback 是唯一線索。"
              f"\n→ 已寫出的部分結果保留在 {ctx.out_path}（可能不完整，但 checkpoint 之前的都在）。"
              f"\n→ 請把上面整段訊息連同該檔案一起帶回 office 分析，不要只帶結果檔。"
              f"\n→ 腳本指紋 script_sha256={_script_sha256()[:16]}…（確認兩邊跑的是同一份）",
              file=sys.stderr, flush=True)
        return 4
    say("")
    say(f"Stage {args.stage} 完成。REST 請求 {ctx.req_count} 次（429 {ctx.err_429} 次）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
