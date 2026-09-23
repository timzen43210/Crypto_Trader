#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A0 — 2 小時漲幅粗篩的候選數量測（完全離線）
==========================================
回答一個問題：**「tickers 輪詢 → 2 小時漲幅粗篩 → 只對候選打 klines」這條路徑，
每根 5 分 K 到底要打幾個 REST 請求、要花幾秒？**

這個數字是 A1 延遲預算的主要成分：候選數是個位數就撐得住端到端 3-5 秒；
候選數尾端破百，3-5 秒那個數字就不成立。

用法：
    python research/a0_screen.py                       # 全部 5M 快取
    python research/a0_screen.py --max-symbols 20      # 冒煙（數字沒有代表性，只驗流程）

結果寫到 research/results/a0_screen.json。本腳本**不連網、不寫入 pionex_cache**。


=========================== 量測定義（判讀前請先讀這裡）===========================

[A1] 粗篩條件的定義，以及它為什麼與 s4_signal 完全對齊
     粗篩 = `s4_signal.features()` 產出的 `ret2h` 欄位 >= 門檻。
     `ret2h` 沒有在本檔重算，是直接取 features() 的輸出欄位——這一點是刻意的：
     實盤粗篩用 tickers 的現價序列算 2 小時漲幅，離線能拿到的最佳代理就是 K 棒收盤價，
     而那正是 features() 算 ret2h 的同一個輸入。重抄一份公式只會製造漂移。
     暖機期（前 2 小時）ret2h 是 NaN，`NaN >= 門檻` 在 numpy 下為 False，
     應與 `signal_from_features()` 結尾的 `cond.fillna(False)` 等價。

     這條等價不靠推論，是**真的把 signal_from_features 叫起來對帳**（見
     input_contract.screen_equivalence）。做法：把 features() 的其他三個欄位釘在
     「必定通過各自條件」的參數值上（中性化），只留粗篩看的那一欄是真實資料，
     於是 `signal_from_features(...) == -1` 的位置**就是它對粗篩那一項的判定結果**，
     包含它對 NaN 的處理。拿它跟粗篩的判定（screen_mask）逐格比對，
     mismatches 必須是 0。另有一組資料無關的控制組（probe_control）餵入
     「必定通過 / 必定不通過 / NaN」三列，要求兩邊分別得到 True/-1、False/0、False/0
     ——這一步是為了防止「兩邊一起錯成 False，比對變成恆等式」這種沒有鑑別力的假驗證。

     BUG-002 的教訓：舊版的 parity 比對兩側都由粗篩自己算出來，signal_from_features
     從未參與，所以那個計數在任何輸入下都是 0，量不到任何東西。
     只有讓被驗的對象真的出現在比對的一側，這個數字才有意義。

     另外，粗篩的判定收斂成唯一一個函式 screen_mask()：候選數統計、漏失差集、
     等價性比對三處都走它。這是刻意的——若等價性比對自己另寫一份比較，
     它驗的就是它自己，不是量測實際用的那條路徑。

[A2] 漏失率在本量測裡是「定義決定的 0」，不是量測成果
     baseline 訊號是 `signal_from_features()` 的完整四條件，它的第一個合項就是
     `ret2h >= MIN_RET_2H`（參數實際值見 metadata.s4_params）。
     因此只要門檻 <= MIN_RET_2H，粗篩條件就被訊號條件完全蘊含，差集必然為空。
     **算它是為了驗證實作沒走偏，不是為了產出一個數字。**
     差集不為空 → 代表粗篩與 s4_signal 的 ret2h 定義脫鉤，那是 bug，要停下來回報。
     門檻恰等於 MIN_RET_2H 時兩邊是同一個浮點比較，結果也必然相同（不是碰巧）。

[A3] 候選數怎麼逐根 K 棒統計，分母是誰
     只計入**計價幣為 --quote-asset（預設 USDT）**的交易對。理由不是潔癖：
     策略4 的兩個成交額條件單位是 USDT，而 turn = close x volume 的 24 小時滾動和，
     close 以計價幣計價，所以 BTC 計價合約算出來的 turn 單位是 BTC，
     拿去跟成交額門檻比毫無意義。被排除的檔案會逐一列在 universe.quote_excluded。
     各交易對的快取長度不同（新上架的幣起點晚），所以逐根 K 棒同時輸出
     `symbols_observed`（該根有幾個交易對的 ret2h 已過暖機、是有效值），
     並給兩個統計窗口，**兩個都輸出、不擇一**：
       all_observed_bars   至少一個交易對有有效 ret2h 的所有 K 棒。窗口最長，
                           尾端估計最可靠，但前緣幾根因為部分交易對還沒資料而略為低估。
       full_universe_bars  全部納入的交易對都有有效 ret2h 的 K 棒。窗口較短，
                           但分母完全一致，是前者的對照組。
     判讀主值取 all_observed_bars（決定延遲上限的是尾端，需要最長的觀測窗口），
     若兩者的 p95/p99 差距明顯，以 full_universe_bars 為準重新判讀。

[A4] 候選數換算成 REST 秒數的邊界
     秒數 = 候選數 / 速率。這條換算**只含 klines 請求本身**，不含 tickers 那一次輪詢、
     不含連線建立、不含解析與訊號計算（後者見 G2a Stage 3）。
     所以它是端到端延遲的**下界**，不是端到端延遲。

[A5] 快取是「抓快取當下」的標的池，不是今天的全市場
     快取檔名代表抓取當時存在的合約。B5（TASK-098）實測今日全市場 TRADING PERP 613 個、
     其中 USDT 計價 568 個。兩個數字不同是正常的，不可混用：
     `candidates_per_bar` 一律是快取這批交易對的實測值，
     另外用 --market-usdt-symbols 做一次**線性外推**放在 projection_to_market，
     並明確標示它是外推不是實測（線性假設本身也可能低估：新上架的幣波動通常更大）。

[A6] 門檻餘裕與「可容忍的近似誤差」
     實盤粗篩用 tickers 取樣算出的 2 小時漲幅是近似值，離線無法模擬它的抖動，
     但可以量它有多少空間：對每一筆 baseline 訊號記下當下的 ret2h，
     若近似值比真值低估了 e，該訊號在門檻 thr 下仍被撈到的條件是 ret2h - e >= thr。
     所以 `min(訊號 ret2h) - thr` 就是「一筆都不漏」能容忍的最大低估量，
     `p05 - thr` 是「容忍 95% 的訊號」的對應值。這兩個數字才是 A1 選門檻的直接輸入，
     中位數沒有決策意義。
"""
import argparse
import contextlib
import hashlib
import math
import os
import socket
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(REPO_ROOT), str(Path(__file__).resolve().parent)):
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ============================== 離線籠子 ==============================
class NetworkBlocked(RuntimeError):
    """本腳本企圖建立網路連線時丟出。A0 不需要任何請求，會走到這裡就是設計被破壞了。"""


class NetGuard:
    """把 socket 連線攔在腳本內部，並記錄每一次企圖。

    這是 AC-5 要的「實證」而不是「宣稱」：
      * 攔的是 `socket.socket.connect` / `connect_ex` / `socket.create_connection`。
        requests / urllib / websockets / ssl 最後都會落到 socket.socket.connect
        （ssl.SSLSocket.connect 走 super().connect），所以這一層擋住就沒有出口。
      * install() 之後會**自己對一個 RFC 5737 測試網段位址試連一次**確認籠子是活的
        （攔截發生在任何系統呼叫之前，不會有封包離開這台機器）。
        只「裝了攔截器」但攔截器其實沒生效的情況，會在這一步被抓出來。
      * 自我測試那幾次記在 self_test_attempts，與真正的企圖 attempts 分開計。

    **武裝範圍是 scoped 的，不是 process 永久的**（F4）。socket 的 patch 是
    process-wide 的全域副作用，若在 module import 時裝上就永不解除，
    任何只是 `import a0_screen`（例如 `pytest tests/`）的 process 都會被波及：
    同一個 session 內不相干的測試若需要真 socket，會收到一個來自本檔的
    NetworkBlocked，極難追查。所以改成兩段 scoped 武裝（見檔尾 `NET` 與 run()/main()）：
    武裝區間仍然完整覆蓋「import 相依套件」與「整個量測流程」，離開區間就還原。

    install()/uninstall() 可重入（depth 計數），巢狀時只有最外層真的 patch 與還原，
    所以 main() 外層武裝 + run() 內層武裝不會互相拆台，自我測試也只做一次。
    """

    PROBE = ("198.51.100.1", 9)      # RFC 5737 TEST-NET-2 + discard port，不可路由

    def __init__(self):
        self.attempts = []
        self.self_test_attempts = []
        self.self_test_ok = None
        self.arm_log = []
        self._orig = {}
        self._probing = False
        self._depth = 0

    def _record(self, addr, how):
        item = {"address": repr(addr), "via": how, "at_utc": datetime.now(timezone.utc).isoformat()}
        (self.self_test_attempts if self._probing else self.attempts).append(item)
        return NetworkBlocked(
            f"離線籠子攔下一次網路連線企圖（{how} → {addr!r}）。\n"
            "→ A0 是純離線量測，不該有任何請求；會走到這裡代表程式邏輯被改壞了。")

    def install(self, self_test=True, reason=""):
        """武裝籠子。可重入：巢狀時只有最外層真的 patch，自我測試也只在最外層做一次。"""
        self._depth += 1
        if self._depth > 1:
            self.arm_log.append({"action": "nested_arm", "reason": reason, "depth": self._depth})
            return self
        guard = self

        def _connect(self, address):
            raise guard._record(address, "socket.socket.connect")

        def _connect_ex(self, address):
            raise guard._record(address, "socket.socket.connect_ex")

        def _create_connection(address, *a, **kw):
            raise guard._record(address, "socket.create_connection")

        self._orig["connect"] = socket.socket.connect
        self._orig["connect_ex"] = socket.socket.connect_ex
        self._orig["create_connection"] = socket.create_connection
        socket.socket.connect = _connect
        socket.socket.connect_ex = _connect_ex
        socket.create_connection = _create_connection
        if self_test:
            self._self_test()
        self.arm_log.append({"action": "arm", "reason": reason, "self_tested": bool(self_test)})
        return self

    def uninstall(self):
        """解除一層武裝。巢狀時只有最外層真的還原（把原函式放回 socket 模組）。"""
        if not self._depth:
            return
        self._depth -= 1
        if self._depth or not self._orig:
            return
        socket.socket.connect = self._orig["connect"]
        socket.socket.connect_ex = self._orig["connect_ex"]
        socket.create_connection = self._orig["create_connection"]
        self._orig.clear()
        self.arm_log.append({"action": "disarm"})

    @contextlib.contextmanager
    def armed(self, self_test=True, reason=""):
        """把一段程式包在籠子裡。離開區間（含例外）一定還原，不留全域副作用。"""
        self.install(self_test=self_test, reason=reason)
        try:
            yield self
        finally:
            self.uninstall()

    def is_armed(self):
        return bool(self._orig)

    def _self_test(self):
        """證明籠子是活的：兩條出口各試一次，兩次都必須被攔下來。"""
        self._probing = True
        blocked = 0
        try:
            probes = (lambda: socket.create_connection(self.PROBE, timeout=1),
                      lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(self.PROBE))
            for fn in probes:
                try:
                    fn()
                except NetworkBlocked:
                    blocked += 1
                except Exception:
                    pass
        finally:
            self._probing = False
        ok = (blocked == len(probes))
        # 一旦有任何一次自我測試沒過就永久記成沒過（不讓後面一次成功把它洗白）。
        self.self_test_ok = ok if self.self_test_ok is None else (self.self_test_ok and ok)

    def report(self):
        return {
            "guard_installed": bool(self._orig),
            "patched": sorted(self._orig),
            "self_test_probe": list(self.PROBE),
            "self_test_blocked_calls": len(self.self_test_attempts),
            "self_test_ok": self.self_test_ok,
            "connect_attempts": len(self.attempts),
            "connect_attempt_detail": self.attempts,
            "arm_log": self.arm_log,
            "arm_scope_note":
                "籠子是 scoped 武裝（F4）：(1) import g2_measure / strategy 的那一段，"
                "(2) run() 整段（讀快取、算特徵、算訊號、建結果、寫 json）。"
                "兩段合起來覆蓋本腳本所有會動到 I/O 的程式；區間外只剩 argparse 與 "
                "socket.gethostname()，沒有任何建立連線的能力。"
                "離開區間會把 socket 的原函式放回去，所以只是 import 本檔的 process "
                "（例如 pytest 跑整個 tests/）不會繼承這個全域 patch。",
            "note": "self_test_ok=true 代表籠子確實會攔（不是只裝了沒生效）；"
                    "connect_attempts=0 代表除了自我測試之外，全程沒有任何連線企圖。",
        }


# import 期間也要在籠子裡：g2_measure / strategy 若有任何連線副作用，必須當場被攔下並記錄。
# 但**離開這個區間就解除武裝**——socket 的 patch 是 process-wide 的，留著會波及任何
# 只是 import 本檔的 process（F4）。量測本身的覆蓋由 run() / main() 的 armed() 負責。
# 這裡不做自我測試：自我測試留給量測那一段做，免得 offline_proof 的計數被 import 摻進來。
NET = NetGuard()
with NET.armed(self_test=False, reason="import g2_measure / strategy"):
    import g2_measure as g2                 # noqa: E402  ← 四個快取 helper 的唯一來源，不重抄
    from strategy import s4_signal          # noqa: E402  ← 訊號邏輯唯一來源，不重抄

SCRIPT_VERSION = "a0_screen 1.1"
SCRIPT_REL_PATH = "research/a0_screen.py"
METHOD_NOTES = (__doc__ or "").strip()

# 粗篩看的特徵欄位名。下面會核對它真的在 s4_signal.FEATURE_COLS 裡，
# 避免日後 features() 改名時本檔安靜地拿到一個不存在的欄位。
SCREEN_FEATURE = "ret2h"
# 粗篩門檻對應的策略參數鍵。門檻掃描的最後一格一律用它的實際值，不寫死數字。
SCREEN_PARAM_KEY = "MIN_RET_2H"

# features() 的每個欄位，對應 signal_from_features() 裡拿它去比的那個參數鍵。
# 取該參數的實際值餵進去，那一項就「剛好通過」——這是 neutral_frame() 中性化其他三項的依據。
# 這張表是本檔對 s4_signal 的明文假設；probe_control 會真的拿 signal_from_features 驗它，
# 而 collect() 會檢查它涵蓋 FEATURE_COLS 全部欄位（features() 長出新欄位就當場報錯，不默認）。
NEUTRAL_PARAM_KEY = {
    "ret2h": "MIN_RET_2H",      # ret2h >= MIN_RET_2H
    "volr": "MIN_VOL_RATIO",    # volr  >= MIN_VOL_RATIO
    "cpos": "MAX_CLOSE_POS",    # cpos  <= MAX_CLOSE_POS
    "turn": "MIN_TURN24H",      # MIN_TURN24H <= turn <= MAX_TURN24H（取下界，同時滿足上界）
}

# 派網兩段式合約名 BASE_PERP 的第 2 段是合約種類，不是計價幣（F5）。
PERP_SUFFIX = "PERP"


class A0Error(g2.G2Error):
    """使用者看得懂的致命錯誤。main() 印成 [錯誤] ...，不吐 traceback。"""


# ============================== 小工具 ==============================
def _script_sha256():
    """指認「哪一版腳本產生了這份數據」。不能用 g2._script_sha256()——那個算的是 g2_measure.py。"""
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


def _hostname():
    try:
        return socket.gethostname()
    except Exception:
        return "unknown"


def dist(values, pcts=(50, 90, 95, 99)):
    """分布摘要。NaN / None 先剔掉；空集合回 {"n": 0}，呼叫端要自己判。"""
    vals = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not vals:
        return {"n": 0}
    a = np.asarray(vals, dtype=float)
    out = {"n": int(a.size), "mean": float(a.mean()), "min": float(a.min()), "max": float(a.max())}
    for q in pcts:
        out["p%02d" % q] = float(np.percentile(a, q))
    return out


def quote_of(symbol):
    """派網 symbol 是 BASE_QUOTE 或 BASE_QUOTE_PERP → 回傳計價幣（第 2 段，大寫）。

    段數不合預期就回 None，呼叫端一律當「無法判定」排除。刻意不猜：
    猜錯的代價是把非 USDT 計價的合約算進候選數，報告會看起來完全正常卻系統性高估。

    F5：兩段式的 `BASE_PERP` 第 2 段是合約種類不是計價幣，以前會被讀成計價幣 "PERP"。
    結果本來就正確（PERP != USDT → 排除），但理由是錯的，留著會誤導日後的人往錯方向修，
    所以改成回 None（無法判定 → 排除，理由寫「不猜」）。
    """
    parts = symbol.split("_")
    if len(parts) < 2 or len(parts) > 3 or not parts[1]:
        return None
    if len(parts) == 2 and parts[1].upper() == PERP_SUFFIX:
        return None
    return parts[1].upper()


def split_universe(names, quote_asset):
    """依計價幣把快取 symbol 分成納入 / 排除兩批（[A3]）。"""
    want = quote_asset.upper()
    kept, excluded = [], []
    for s in names:
        q = quote_of(s)
        if q == want:
            kept.append(s)
        else:
            excluded.append({
                "symbol": s, "quote": q,
                "reason": (f"計價幣 {q} 不是 {want}：成交額條件的單位是 {want}，"
                           "turn = close x volume 以計價幣計價，混進來會讓 turn 與門檻不同單位"
                           if q else f"名稱不符 BASE_{want}[_{PERP_SUFFIX}] 的形狀"
                                     f"（段數不對，或兩段式的第 2 段是 {PERP_SUFFIX} 而不是計價幣），"
                                     "無法判定計價幣，不猜"),
            })
    return kept, excluded


def parse_thresholds(raw, param_value):
    """門檻清單：CLI 給的低門檻 + 一定補上 SCREEN_PARAM_KEY 的實際值（邊界格），升冪去重。

    邊界格不接受手寫數字，只能來自 s4_signal.DEFAULT_PARAMS——這樣「門檻等於策略門檻時
    差集必然為空」才是同一個浮點比較的必然結果，而不是碰巧兩個字面值相等。
    """
    out = []
    for tok in str(raw).split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            v = float(tok)
        except ValueError:
            raise A0Error(f"--thresholds 裡的 {tok!r} 不是數字。→ 格式例：0.10,0.11,0.12")
        if not 0 < v <= param_value:
            raise A0Error(
                f"--thresholds 的 {v} 超出允許範圍 (0, {SCREEN_PARAM_KEY}]。\n"
                "→ 高於策略門檻的粗篩會真的漏訊號，本腳本不支援；"
                "要往上調門檻請先回報 captain（見 PRD 第 9 節第 2 條）。")
        out.append(v)
    out.append(float(param_value))
    return sorted(set(out))


def parse_rates(raw):
    out = []
    for tok in str(raw).split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            v = float(tok)
        except ValueError:
            raise A0Error(f"--rates 裡的 {tok!r} 不是數字。→ 格式例：10,6")
        if v <= 0:
            raise A0Error(f"--rates 的 {v} 必須大於 0。")
        out.append(v)
    if not out:
        raise A0Error("--rates 至少要有一個速率。")
    return out


def warmup_bars_needed(bph):
    """能算出完整四條件的最少根數：24 小時 turn 暖機 + 2 小時 ret2h 暖機 + 2 根餘裕。

    與 g2_measure 的 Stage 3 / Stage 4 同一個口徑，刻意保持一致，
    不然「哪些 symbol 被納入」兩支腳本會給出不同答案。
    """
    return 24 * bph + 2 * bph + 2


# ==================== 粗篩判定，與它對 s4_signal 的等價性（[A1]）====================
def screen_mask(values, threshold):
    """粗篩的**唯一**判定實作：值 >= 門檻，NaN 一律為 False（numpy / pandas 的比較語意）。

    收斂成一個函式是刻意的：候選數逐根累加、漏失差集、與 signal_from_features 的等價性
    比對，三個地方都必須走這同一條判定。否則等價性比對只是在比對它自己寫的另一份比較
    ——那就是 BUG-002：兩側都由粗篩自己算，被驗的對象根本沒上場，計數永遠是 0。
    """
    return values >= threshold


def neutral_frame(index, feature, values, params):
    """做一份只有 `feature` 這一欄在動的 features() 形狀的框，其餘欄位釘在「必定通過」的值。

    目的：把 signal_from_features() 的四個合項裡，除了 `feature` 以外的三個全部中性化，
    這樣 `signal_from_features(...) == -1` 的位置**恰好**是它對 `feature` 那一項的判定，
    包含它對 NaN 的處理。拿它跟 screen_mask() 比對，才是真的在驗「粗篩 == 訊號的那一項」。
    """
    data = {}
    for col in s4_signal.FEATURE_COLS:
        data[col] = values if col == feature else float(params[NEUTRAL_PARAM_KEY[col]])
    return pd.DataFrame(data, index=index)


def signal_says_pass(index, feature, values, params):
    """signal_from_features() 在「其他三項中性化」下對 `feature` 那一項的判定（bool ndarray）。"""
    sig = s4_signal.signal_from_features(neutral_frame(index, feature, values, params), None)
    return (sig.to_numpy() == -1)


def check_neutral_table(params):
    """中性化表必須涵蓋 features() 的每個欄位，且取的參數值真的落在通過區間內。

    features() 日後多長一欄、或參數鍵改名，這裡要當場報錯——不能悄悄少中性化一項，
    那會讓等價性比對兩側一起變成 False（又一次沒有鑑別力的假驗證）。
    """
    missing = [c for c in s4_signal.FEATURE_COLS if c not in NEUTRAL_PARAM_KEY]
    if missing:
        raise A0Error(
            f"s4_signal.FEATURE_COLS 有 {missing} 不在本檔的 NEUTRAL_PARAM_KEY 表裡。\n"
            "→ features() 長出新欄位了。先確認 signal_from_features() 拿它跟哪個參數比，"
            "把對應關係補進 NEUTRAL_PARAM_KEY，否則等價性比對會失去鑑別力。")
    bad = [(c, k) for c, k in NEUTRAL_PARAM_KEY.items() if k not in params]
    if bad:
        raise A0Error(f"NEUTRAL_PARAM_KEY 指到 DEFAULT_PARAMS 沒有的鍵：{bad}。"
                      "→ 策略參數鍵改名了，先確認語意再改這張表。")
    lo, hi = params[NEUTRAL_PARAM_KEY["turn"]], params.get("MAX_TURN24H")
    if hi is not None and float(lo) > float(hi):
        raise A0Error(
            f"成交額區間反了（下界 {lo} > 上界 {hi}），中性化用的 turn 值無法同時滿足兩邊。\n"
            "→ 先確認 DEFAULT_PARAMS 的成交額參數。")


def parity_probe_control(feature, params):
    """資料無關的控制組：證明「粗篩 vs signal_from_features」這個比對本身有鑑別力。

    餵三列——必定通過的值、必定不通過的值、NaN——要求兩側逐格一致且是
    [True, False, False] / [-1, 0, 0]。

    為什麼需要它：比對若建構錯誤（例如另外三項其實沒被中性化），兩側會一起是 False，
    mismatches 照樣 0，比對就退化成恆等式——BUG-002 就是這樣長出來的。
    控制組把「兩邊都得是 True」與「NaN 兩邊都得是 False」這兩個方向都釘住。
    順帶它也擋掉「粗篩看錯欄位」的一部分情形：例如錯看 cpos（訊號那邊是 <=、粗篩是 >=），
    必定不通過的那一列兩側會給出相反答案，當場不一致。
    """
    pass_v = float(params[NEUTRAL_PARAM_KEY[feature]])
    fail_v = -abs(pass_v) - 1.0          # 對 `>=` 型條件必定不通過；對 `<=` 型則會通過（正是要抓的）
    idx = pd.RangeIndex(3)
    vals = pd.Series([pass_v, fail_v, float("nan")], index=idx, dtype=float)
    screen = [bool(x) for x in screen_mask(vals.to_numpy(dtype=float), pass_v)]
    signal = [bool(x) for x in signal_says_pass(idx, feature, vals, params)]
    return {
        "note": "控制組（不依賴任何快取資料）：餵入「必定通過 / 必定不通過 / NaN」三列，"
                "粗篩判定與 signal_from_features 的判定必須逐格一致，且必須是 "
                "[true, false, false]——兩邊一起錯成 false 的話這個比對就沒有鑑別力。",
        "feature": feature,
        "pass_value_from": f"s4_signal.DEFAULT_PARAMS[{NEUTRAL_PARAM_KEY[feature]!r}]",
        "probe_values": ["pass_value", "definitely_below_pass_value", "nan"],
        "screen_says_pass": screen,
        "signal_says_pass": signal,
        "expected": [True, False, False],
        "agree": screen == signal,
        "expected_shape_ok": screen == [True, False, False],
        "ok": screen == signal == [True, False, False],
    }


# ============================== 執行環境與 metadata ==============================
class Ctx:
    """一次執行的全部狀態。

    刻意不用 g2.Ctx：那個帶著 REST 速率 / 請求預算 / CA bundle，A0 完全不連網，
    把那些欄位搬過來只會讓人以為本腳本會發請求（順便繼承 BUG-049 的預算漏洞）。
    """

    def __init__(self, args):
        self.args = args
        self.start_ms = g2.now_ms()
        self.start_perf = time.perf_counter()
        self.machine = args.machine or _hostname()
        self.notes = []
        out_dir = Path(args.out_dir) if args.out_dir else REPO_ROOT / "research" / "results"
        self.out_path = Path(args.out) if args.out else out_dir / "a0_screen.json"
        self.backup_path = None

    def note(self, msg):
        self.notes.append(msg)
        g2.warn(msg)


def build_meta(ctx, params):
    end = g2.now_ms()
    return {
        "script": SCRIPT_VERSION,
        "script_file": SCRIPT_REL_PATH,
        "script_sha256": _script_sha256(),
        "task": "A0（2 小時漲幅粗篩的候選數量測）",
        "machine": ctx.machine,
        "started_utc": g2.fmt_ms(ctx.start_ms),
        "started_epoch_ms": ctx.start_ms,
        "started_local": datetime.fromtimestamp(ctx.start_ms / 1000).astimezone().isoformat(),
        "finished_utc": g2.fmt_ms(end),
        "finished_epoch_ms": end,
        "finished_local": datetime.fromtimestamp(end / 1000).astimezone().isoformat(),
        "elapsed_s": round(time.perf_counter() - ctx.start_perf, 3),
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "git_commit": _git_commit(),
        "git_commit_note": "repo HEAD。要指認腳本本身請用 script_sha256。",
        "reused_from_g2_measure": ["list_cache_symbols", "load_cache_frame", "prepare_frame",
                                   "bars_per_hour_of", "INTERVAL_MS", "HOUR_MS", "fmt_ms",
                                   "write_json", "now_ms", "say", "warn", "G2Error"],
        "rest_requests_total": 0,
        "rest_note": "本腳本沒有呼叫 g2_measure.rest_get()，也沒有任何 HTTP / WebSocket 出口，"
                     "全程零請求（證據見 offline_proof）。",
        "s4_params": dict(s4_signal.DEFAULT_PARAMS),
        "s4_feature_cols": list(s4_signal.FEATURE_COLS),
        "screen_feature": SCREEN_FEATURE,
        "screen_param_key": SCREEN_PARAM_KEY,
        "params": params,
        "cli_argv": sys.argv[1:],
        "runtime_notes": ctx.notes,
        "output_backup": str(ctx.backup_path) if ctx.backup_path else None,
    }


# ============================== 主量測 ==============================
def collect(ctx):
    """逐 symbol 讀快取、算特徵、累積逐根 K 棒的候選數與漏失差集。"""
    args = ctx.args
    interval = args.interval
    bar_ms = g2.INTERVAL_MS[interval]
    bph = g2.bars_per_hour_of(interval)
    params = dict(s4_signal.DEFAULT_PARAMS)

    if SCREEN_FEATURE not in s4_signal.FEATURE_COLS:
        raise A0Error(
            f"s4_signal.FEATURE_COLS 裡沒有 {SCREEN_FEATURE!r}（實際：{s4_signal.FEATURE_COLS}）。\n"
            "→ 粗篩看的特徵欄位被改名了，本腳本的假設不再成立，先確認新的欄位語意再改這裡。")
    if SCREEN_PARAM_KEY not in params:
        raise A0Error(f"s4_signal.DEFAULT_PARAMS 裡沒有 {SCREEN_PARAM_KEY!r}。"
                      "→ 參數鍵改名了，先確認語意再改這裡。")
    check_neutral_table(params)
    probe_control = parity_probe_control(SCREEN_FEATURE, params)

    min_ret = float(params[SCREEN_PARAM_KEY])
    thresholds = parse_thresholds(args.thresholds, min_ret)
    need = warmup_bars_needed(bph)

    names = g2.list_cache_symbols(args.cache_dir, interval)
    kept, excluded = split_universe(names, args.quote_asset)
    if not kept:
        raise A0Error(
            f"快取裡沒有任何計價幣為 {args.quote_asset.upper()} 的 {interval} 檔案"
            f"（共 {len(names)} 個檔案）。\n→ 請確認 --cache-dir / --interval / --quote-asset。")
    considered = len(kept)
    if args.max_symbols:
        kept = kept[: args.max_symbols]
        ctx.note(f"--max-symbols {args.max_symbols}：只處理前 {len(kept)} 個交易對，"
                 "候選數不具全市場代表性（冒煙用）。")

    g2.say(f"快取 {args.cache_dir}：{interval} 共 {len(names)} 個檔案，"
           f"計價幣 {args.quote_asset.upper()} 納入 {considered} 個、排除 {len(excluded)} 個")
    g2.say("門檻掃描：" + ", ".join("%g" % t for t in thresholds)
           + f"（最後一格 = s4_signal.DEFAULT_PARAMS[{SCREEN_PARAM_KEY!r}]，不是手寫值）")

    used, series, skipped = [], [], []
    missed_records = {t: [] for t in thresholds}
    missed_count = {t: 0 for t in thresholds}
    screen_pass_bars = {t: 0 for t in thresholds}
    signal_ret2h = []
    filled_total = bars_total = baseline_n = 0
    nan_ret2h_total = nan_turn_total = nan_parity_mismatches = 0
    parity_rows = parity_screen_pass = parity_signal_pass = 0
    monotonic_all = fixed_period_all = True

    for i, sym in enumerate(kept, 1):
        try:
            df, filled = g2.load_cache_frame(args.cache_dir, sym, interval, args.replay_bars or None)
        except g2.G2Error as e:
            skipped.append({"symbol": sym, "reason": str(e)[:200]})
            continue
        if len(df) < need:
            skipped.append({"symbol": sym, "reason": f"資料只有 {len(df)} 根，不足暖機 {need} 根"})
            continue

        times = df["time"].to_numpy(dtype="int64")
        if not bool((np.diff(times) == bar_ms).all()):
            skipped.append({"symbol": sym, "reason": "prepare_frame 後仍非固定週期，不納入（資料異常）"})
            fixed_period_all = False
            continue
        if not bool(df["time"].is_monotonic_increasing):
            monotonic_all = False

        feats = s4_signal.features(df, bph)
        sig = s4_signal.signal_from_features(feats, None)
        ret2h = feats[SCREEN_FEATURE].to_numpy(dtype=float)
        fires = np.flatnonzero((sig == -1).to_numpy())

        # [A1] 粗篩判定是否真的等於 signal_from_features 對同一項的判定（含 NaN 語意）——
        # 不靠推論：右側是 screen_mask（量測實際走的那條路徑），
        # 左側是把其他三項中性化後、**真的呼叫 signal_from_features** 得到的判定。
        sig_side = signal_says_pass(feats.index, SCREEN_FEATURE, feats[SCREEN_FEATURE], params)
        screen_side = screen_mask(ret2h, min_ret)
        nan_parity_mismatches += int((sig_side != screen_side).sum())
        parity_rows += int(ret2h.size)
        parity_screen_pass += int(np.count_nonzero(screen_side))
        parity_signal_pass += int(np.count_nonzero(sig_side))

        nan_ret2h_total += int(np.isnan(ret2h).sum())
        nan_turn_total += int(feats["turn"].isna().sum())
        baseline_n += int(fires.size)
        if fires.size:
            signal_ret2h.extend(float(x) for x in ret2h[fires])

        # [A2] 對每個門檻算差集：baseline 有、粗篩沒撈到的
        for thr in thresholds:
            passes = screen_mask(ret2h, thr)           # NaN 一律 False，見 screen_mask()
            screen_pass_bars[thr] += int(passes.sum())
            if not fires.size:
                continue
            gap = fires[~passes[fires]]
            missed_count[thr] += int(gap.size)
            room = args.max_missed_records - len(missed_records[thr])
            for j in gap[: max(0, room)]:
                missed_records[thr].append({
                    "symbol": sym, "threshold": thr, "bar_open": int(times[j]),
                    "bar_open_utc": g2.fmt_ms(int(times[j])),
                    "ret2h": None if math.isnan(ret2h[j]) else float(ret2h[j]),
                    "volr": float(feats["volr"].iloc[j]), "cpos": float(feats["cpos"].iloc[j]),
                    "turn": float(feats["turn"].iloc[j]),
                })

        used.append(sym)
        series.append((int(times[0]), ret2h))
        filled_total += filled
        bars_total += len(df)
        if i % 50 == 0:
            g2.say(f"  已處理 {i}/{len(kept)} 個交易對…（納入 {len(used)}、跳過 {len(skipped)}）")

    if not used:
        raise A0Error(
            f"沒有任何交易對的資料長度達到暖機需求（{need} 根）。\n"
            "→ 請確認 --cache-dir / --interval，或先在本機跑一次回測把快取補滿。\n"
            f"→ 跳過原因前幾筆：{skipped[:3]}")

    return {
        "interval": interval, "bar_ms": bar_ms, "bars_per_hour": bph, "params": params,
        "min_ret": min_ret, "thresholds": thresholds, "need": need,
        "cache_files_total": len(names), "considered": considered, "excluded": excluded,
        "used": used, "series": series, "skipped": skipped,
        "missed_count": missed_count, "missed_records": missed_records,
        "screen_pass_bars": screen_pass_bars, "signal_ret2h": signal_ret2h,
        "filled_total": filled_total, "bars_total": bars_total, "baseline_n": baseline_n,
        "nan_ret2h_total": nan_ret2h_total, "nan_turn_total": nan_turn_total,
        "nan_parity_mismatches": nan_parity_mismatches,
        "parity_rows": parity_rows, "parity_screen_pass": parity_screen_pass,
        "parity_signal_pass": parity_signal_pass, "probe_control": probe_control,
        "monotonic_all": monotonic_all, "fixed_period_all": fixed_period_all,
    }


def aggregate(raw):
    """把 per-symbol 的 ret2h 攤到全局 K 棒網格上，逐根數候選（[A3]）。"""
    bar_ms, series, used = raw["bar_ms"], raw["series"], raw["used"]
    thresholds = raw["thresholds"]
    origin = min(t0 for t0, _ in series)
    last = max(t0 + (len(r) - 1) * bar_ms for t0, r in series)
    n_grid = (last - origin) // bar_ms + 1

    misaligned = [used[k] for k, (t0, _) in enumerate(series) if (t0 - origin) % bar_ms]
    if misaligned:
        raise A0Error(
            f"{len(misaligned)} 個交易對的 K 棒與全局網格不對齊（例：{misaligned[:5]}）。\n"
            "→ 逐根 K 棒的候選數無法跨交易對相加，先確認快取是不是混了不同週期的資料。")

    observed = np.zeros(n_grid, dtype=np.int32)
    counts = np.zeros((len(thresholds), n_grid), dtype=np.int32)
    for t0, r in series:
        off = (t0 - origin) // bar_ms
        sl = slice(off, off + r.size)
        observed[sl] += ~np.isnan(r)
        for j, thr in enumerate(thresholds):
            counts[j, sl] += screen_mask(r, thr)       # 與差集、等價性比對同一條判定

    raw.update({
        "origin": origin, "last": last, "n_grid": n_grid,
        "observed": observed, "counts": counts,
        "windows": {"all_observed_bars": observed > 0,
                    "full_universe_bars": observed == len(used)},
    })
    return raw


def candidates_at(raw, grid_idx, thr, limit):
    """指定 K 棒下通過門檻的交易對與其 ret2h（由高到低）。給最忙那根做事後檢核用。"""
    bar_ms, origin = raw["bar_ms"], raw["origin"]
    out = []
    for name, (t0, r) in zip(raw["used"], raw["series"]):
        i = grid_idx - (t0 - origin) // bar_ms
        if 0 <= i < r.size and r[i] >= thr:
            out.append({"symbol": name, "ret2h": float(r[i])})
    out.sort(key=lambda d: -d["ret2h"])
    return out[:limit], len(out)


def busiest_bars(raw, j, thr, mask, top_n, sym_limit):
    """最忙的前 top_n 根 K 棒。第 1 名額外列出候選清單，供判斷是不是全市場同時拉抬。"""
    counts, observed = raw["counts"], raw["observed"]
    origin, bar_ms = raw["origin"], raw["bar_ms"]
    idx = np.flatnonzero(mask)
    if idx.size == 0 or top_n <= 0:
        return []
    order = idx[np.argsort(-counts[j][idx], kind="stable")][:top_n]
    rows = []
    for rank, gi in enumerate(order, 1):
        gi = int(gi)
        bar_open = int(origin + gi * bar_ms)
        row = {"rank": rank, "bar_open": bar_open, "bar_open_utc": g2.fmt_ms(bar_open),
               "candidates": int(counts[j][gi]), "symbols_observed": int(observed[gi])}
        if rank == 1 and sym_limit > 0:
            row["candidate_symbols"], row["candidate_symbols_total"] = candidates_at(
                raw, gi, thr, sym_limit)
            row["candidate_symbols_note"] = (
                f"最忙那一根的候選清單（ret2h 由高到低，最多 {sym_limit} 筆）")
        rows.append(row)
    return rows


def build_result(raw, ctx):
    args = ctx.args
    thresholds, counts, observed = raw["thresholds"], raw["counts"], raw["observed"]
    used, bar_ms = raw["used"], raw["bar_ms"]
    rates = parse_rates(args.rates)
    target_s, ceiling_s = float(args.latency_target_s), float(args.latency_ceiling_s)
    ms_per_day = 24 * g2.HOUR_MS
    primary = "all_observed_bars"

    window_meta = {}
    for name, mask in raw["windows"].items():
        n = int(mask.sum())
        first_ms = last_ms = None
        if n:
            gi = np.flatnonzero(mask)
            first_ms = int(raw["origin"] + int(gi[0]) * bar_ms)
            last_ms = int(raw["origin"] + int(gi[-1]) * bar_ms)
        window_meta[name] = {
            "bars": n,
            "first_bar_open": first_ms, "first_bar_open_utc": g2.fmt_ms(first_ms),
            "last_bar_open": last_ms, "last_bar_open_utc": g2.fmt_ms(last_ms),
            "span_days": round((last_ms - first_ms) / ms_per_day, 3) if n else None,
            "symbols_observed": dist(observed[mask].tolist(), pcts=(5, 50, 95)) if n else {"n": 0},
            "is_primary": name == primary,
        }

    by_threshold = []
    for j, thr in enumerate(thresholds):
        per_window = {}
        for name, mask in raw["windows"].items():
            if not mask.any():
                per_window[name] = {"bars": 0, "distribution": {"n": 0}}
                continue
            cc = counts[j][mask]
            d = dist(cc.tolist())
            per_window[name] = {
                "bars": int(cc.size),
                "distribution": d,
                "zero_candidate_bars": int((cc == 0).sum()),
                "zero_candidate_share": float((cc == 0).mean()),
                "total_candidate_requests": int(cc.sum()),
                "rest_seconds": {
                    "target_s": target_s, "ceiling_s": ceiling_s,
                    "per_rate": [{
                        "rate_req_per_s": rate,
                        "p50_s": d["p50"] / rate, "p95_s": d["p95"] / rate,
                        "p99_s": d["p99"] / rate, "max_s": d["max"] / rate,
                        "p95_exceeds_target": bool(d["p95"] / rate > target_s),
                        "p95_exceeds_ceiling": bool(d["p95"] / rate > ceiling_s),
                        "p99_exceeds_target": bool(d["p99"] / rate > target_s),
                        "p99_exceeds_ceiling": bool(d["p99"] / rate > ceiling_s),
                        "max_exceeds_target": bool(d["max"] / rate > target_s),
                        "max_exceeds_ceiling": bool(d["max"] / rate > ceiling_s),
                    } for rate in rates],
                    "note": "[A4] 只含 klines 請求本身，不含 tickers 輪詢 / 連線建立 / 訊號計算，"
                            "所以是端到端延遲的下界。",
                },
            }
        by_threshold.append({
            "threshold": thr,
            "is_strategy_threshold": thr == raw["min_ret"],
            "threshold_source": (f"s4_signal.DEFAULT_PARAMS[{SCREEN_PARAM_KEY!r}]"
                                 if thr == raw["min_ret"] else "--thresholds"),
            "windows": per_window,
            "busiest_bars": busiest_bars(raw, j, thr, raw["windows"][primary],
                                         args.top_busy, args.busy_symbols),
        })

    # ---- 外推到全市場 USDT 計價（[A5]）----
    market_n = int(args.market_usdt_symbols)
    scale = market_n / len(used)
    projection = {
        "note": "[A5] 線性外推，不是實測。假設全市場其餘交易對的候選機率與快取這批相同；"
                "新上架的幣波動通常更大，這個假設可能低估。",
        "cache_symbols_used": len(used),
        "market_usdt_symbols": market_n,
        "market_usdt_source": "B5（TASK-098）實測全市場 TRADING PERP，由 --market-usdt-symbols 帶入",
        "scale_factor": scale,
        "window": primary,
        "by_threshold": [{
            "threshold": r["threshold"],
            "p95_candidates": r["windows"][primary]["distribution"]["p95"] * scale,
            "p99_candidates": r["windows"][primary]["distribution"]["p99"] * scale,
            "max_candidates": r["windows"][primary]["distribution"]["max"] * scale,
            "rest_seconds": [{
                "rate_req_per_s": rate,
                "p95_s": r["windows"][primary]["distribution"]["p95"] * scale / rate,
                "p99_s": r["windows"][primary]["distribution"]["p99"] * scale / rate,
                "max_s": r["windows"][primary]["distribution"]["max"] * scale / rate,
                "p99_exceeds_target": bool(
                    r["windows"][primary]["distribution"]["p99"] * scale / rate > target_s),
                "p99_exceeds_ceiling": bool(
                    r["windows"][primary]["distribution"]["p99"] * scale / rate > ceiling_s),
                "max_exceeds_target": bool(
                    r["windows"][primary]["distribution"]["max"] * scale / rate > target_s),
                "max_exceeds_ceiling": bool(
                    r["windows"][primary]["distribution"]["max"] * scale / rate > ceiling_s),
            } for rate in rates],
        } for r in by_threshold],
    }

    # ---- 漏失率驗證（[A2]）----
    leak_rows = []
    for thr in thresholds:
        n_missed = raw["missed_count"][thr]
        leak_rows.append({
            "threshold": thr,
            "below_strategy_threshold": thr < raw["min_ret"],
            "baseline_signals": raw["baseline_n"],
            "missed_signals": n_missed,
            "missed_rate": (n_missed / raw["baseline_n"]) if raw["baseline_n"] else None,
            "must_be_zero": thr <= raw["min_ret"],
            "ok": n_missed == 0,
            "missed_detail": raw["missed_records"][thr],
            "missed_detail_truncated": len(raw["missed_records"][thr]) < n_missed,
        })
    lowest_reject = int((counts[0] < observed).sum()) if observed.size else 0
    leakage = {
        "note": "[A2] 差集 = baseline 訊號有、粗篩沒撈到。門檻 <= 策略門檻時結構上必為 0；"
                "算它是為了驗實作沒走偏，不是量測成果。",
        "baseline_definition": "s4_signal.signal_from_features(features(df, bars_per_hour), None) == -1"
                               "（完整四條件、預設參數）",
        "screen_definition": f"features(df, bars_per_hour)[{SCREEN_FEATURE!r}] >= threshold",
        "baseline_signals_total": raw["baseline_n"],
        "by_threshold": leak_rows,
        "all_ok": all(r["ok"] for r in leak_rows if r["must_be_zero"]),
        "check_is_non_vacuous": {
            "note": "差集為空有兩種可能：粗篩真的被蘊含，或粗篩根本全通過（檢查本身沒有鑑別力）。"
                    "下面兩個數字都 > 0 才代表這個驗證有意義。",
            "baseline_signals": raw["baseline_n"],
            "bars_where_screen_rejects_someone_at_lowest_threshold": lowest_reject,
        },
    }

    # ---- 餘裕分布（[A6]）----
    sig_ret = raw["signal_ret2h"]
    has_sig = bool(sig_ret)
    margin = {
        "note": "[A6] 每一筆 baseline 訊號當下的 ret2h。最小值與低百分位才是決策數字："
                "它們決定實盤近似誤差能容忍多少。",
        "distribution": dist(sig_ret, pcts=(5, 10, 25, 50, 75, 95)),
        "above_strategy_threshold": dist([x - raw["min_ret"] for x in sig_ret],
                                         pcts=(5, 10, 25, 50)),
        "error_tolerance_by_threshold": [{
            "threshold": thr,
            "tolerable_underestimate_all_signals": (min(sig_ret) - thr) if has_sig else None,
            "tolerable_underestimate_95pct": (
                float(np.percentile(sig_ret, 5)) - thr) if has_sig else None,
            "tolerable_underestimate_90pct": (
                float(np.percentile(sig_ret, 10)) - thr) if has_sig else None,
            "note": "實盤近似值若比真值低估 e，該訊號仍被撈到的條件是 ret2h - e >= threshold。",
        } for thr in thresholds],
    }

    # ---- 標的池口徑（FR-5 / [A5]）----
    universe = {
        "note": "[A5] 這是抓快取當下的標的池，不是今天的全市場，兩個數字不可混用。",
        "cache_dir": str(args.cache_dir),
        "interval": raw["interval"],
        "cache_files_for_interval": raw["cache_files_total"],
        "quote_asset_required": args.quote_asset.upper(),
        "quote_matched": raw["considered"],
        "quote_excluded_count": len(raw["excluded"]),
        "quote_excluded": raw["excluded"][:100],
        "quote_excluded_truncated": len(raw["excluded"]) > 100,
        "quote_exclusion_reason": (
            "策略4 的成交額條件單位是計價幣；turn = close x volume 以計價幣計價，"
            "非 USDT 計價的合約算出來的 turn 與門檻不同單位，納入會系統性扭曲候選數。"),
        "symbols_considered": len(raw["used"]) + len(raw["skipped"]),
        "symbols_used": len(raw["used"]),
        "symbols_skipped": len(raw["skipped"]),
        "skipped_detail": raw["skipped"][:50],
        "warmup_bars_required": raw["need"],
        "bars_replayed_total": raw["bars_total"],
        "cache_coverage": {
            "first_bar_open": int(raw["origin"]),
            "first_bar_open_utc": g2.fmt_ms(int(raw["origin"])),
            "last_bar_open": int(raw["last"]),
            "last_bar_open_utc": g2.fmt_ms(int(raw["last"])),
            "span_days": round((raw["last"] - raw["origin"]) / ms_per_day, 3),
            "grid_bars": int(raw["n_grid"]),
            "symbols_observed_per_bar": dist(observed.tolist(), pcts=(5, 50, 95)),
        },
    }

    # ---- 粗篩 vs signal_from_features 的等價性（[A1] / captain PRD 11(a)）----
    exp_nan_ret2h = 2 * raw["bars_per_hour"] * len(used)
    exp_nan_turn = (24 * raw["bars_per_hour"] - 1) * len(used)
    nan_ret2h_ok = raw["nan_ret2h_total"] == exp_nan_ret2h
    nan_turn_ok = raw["nan_turn_total"] == exp_nan_turn
    ctrl = raw["probe_control"]
    parity_rows = raw["parity_rows"]
    parity_non_vacuous = (0 < raw["parity_signal_pass"] < parity_rows) if parity_rows else False
    equivalence = {
        "note": "[A1] captain PRD 11(a) 的驗證點：粗篩條件是否真的等於 signal_from_features "
                "對同一項的判定。比對的一側是量測實際走的 screen_mask()，"
                "另一側是**真的呼叫 signal_from_features()**（其他三項中性化）。"
                "這四個布林全部為 true 才算證明了等價；任何一個 false 都會進 "
                "verdict.stop_conditions_triggered 並讓離開碼非 0。",
        "screen_definition": f"screen_mask(features(df, bph)[{SCREEN_FEATURE!r}], threshold)",
        "signal_side_definition":
            f"s4_signal.signal_from_features(neutral_frame(..., {SCREEN_FEATURE!r}, ...), None) == -1"
            f"，門檻取 DEFAULT_PARAMS[{SCREEN_PARAM_KEY!r}]",
        "rows_compared": parity_rows,
        "mismatches": raw["nan_parity_mismatches"],
        "screen_says_pass_rows": raw["parity_screen_pass"],
        "signal_says_pass_rows": raw["parity_signal_pass"],
        "nan_rows_in_screen_feature": raw["nan_ret2h_total"],
        "probe_control": ctrl,
        "checks": {
            "mismatches_zero": raw["nan_parity_mismatches"] == 0,
            "probe_control_ok": bool(ctrl["ok"]),
            "non_vacuous": parity_non_vacuous,
            "warmup_nan_as_expected": nan_ret2h_ok and nan_turn_ok,
        },
        "non_vacuous_note": "signal 側必須同時出現「有通過」與「沒通過」兩種結果，"
                            "否則這份比對只證明了兩邊在單一分支上一致，沒有鑑別力。",
        "ok": (raw["nan_parity_mismatches"] == 0 and bool(ctrl["ok"])
               and parity_non_vacuous and nan_ret2h_ok and nan_turn_ok),
    }

    # ---- 判定與必須回報的條件（PRD 第 9 節）----
    stops = []
    if not leakage["all_ok"]:
        stops.append("PRD 9.1：門檻 <= 策略門檻卻有漏失 → 粗篩實作與 s4_signal 的 ret2h 不一致，"
                     "這是 fail 不是發現。")
    if raw["nan_parity_mismatches"]:
        stops.append(
            f"[A1] 粗篩判定與 signal_from_features 不等價（{raw['nan_parity_mismatches']} 格不同，"
            f"共比 {parity_rows} 格）→ 粗篩可能取錯欄位、或 NaN 語意與訊號那邊不一致。"
            "候選數會是錯的，AC-1 的差集為空不足以推翻這件事（captain PRD 11(a)）。")
    if not ctrl["ok"]:
        stops.append(
            f"[A1] 等價性比對的控制組沒過（screen={ctrl['screen_says_pass']}、"
            f"signal={ctrl['signal_says_pass']}、期待 {ctrl['expected']}）→ "
            "這份比對本身失去鑑別力（BUG-002 的成因），不可據此宣稱粗篩與訊號等價。")
    if parity_rows and not parity_non_vacuous:
        stops.append(
            f"[A1] 等價性比對是空轉的：signal 側在 {parity_rows} 格裡通過 "
            f"{raw['parity_signal_pass']} 格（需要同時出現通過與不通過才有鑑別力）。"
            "→ 換一份資料量較足的快取，或這次的結果不可用來證明等價。")
    if not nan_ret2h_ok:
        stops.append(
            f"[A1] 粗篩欄位 {SCREEN_FEATURE!r} 的暖機 NaN 根數與預期不符"
            f"（期待 {exp_nan_ret2h}、實際 {raw['nan_ret2h_total']}）→ "
            "很可能取錯欄位（例如拿到 volr 的 24 小時暖機），或 features() 的暖機長度變了。"
            "這種情況下漏失差集仍會是空的，但候選數會被系統性高估（captain PRD 11(a)）。")
    if not nan_turn_ok:
        stops.append(
            f"[A1] turn 的暖機 NaN 根數與預期不符（期待 {exp_nan_turn}、"
            f"實際 {raw['nan_turn_total']}）→ features() 的 24 小時暖機口徑與本檔的假設不一致，"
            "baseline 訊號數與候選數都不可信。")
    if not raw["fixed_period_all"]:
        stops.append("[輸入契約] 有交易對在 prepare_frame 之後仍不是固定週期（已跳過，"
                     "見 universe.skipped_detail）→ 快取可能混了不同週期的資料，先確認再判讀。")
    if not raw["monotonic_all"]:
        stops.append("[輸入契約] 有交易對的 time 不是升冪 → 違反 s4_signal 的輸入契約，"
                     "features() 的 shift / rolling 結果不可信。")
    if leakage["all_ok"] and (raw["baseline_n"] == 0 or lowest_reject == 0):
        stops.append("[A2] 漏失率驗證是空轉的（baseline 訊號為 0 或粗篩從未擋掉任何人），"
                     "差集為空沒有鑑別力，不可當成 AC-1 通過。")
    slowest = min(rates)
    boundary = by_threshold[-1]["windows"][primary]["distribution"]
    if boundary.get("p99") is not None and boundary["p99"] / slowest > ceiling_s:
        stops.append(f"PRD 9.2：連門檻等於策略門檻（最嚴）都在 {slowest:g} req/s 下 p99 超過 "
                     f"{ceiling_s:g} 秒上限 → 不可自行把門檻往上調，回報 captain。")
    span = universe["cache_coverage"]["span_days"]
    if span < args.min_span_days:
        stops.append(f"PRD 9.4：快取只涵蓋 {span} 天（< {args.min_span_days} 天），"
                     "候選數尾端失去意義。")
    if NET.attempts:
        stops.append(f"AC-5：離線籠子攔到 {len(NET.attempts)} 次真正的連線企圖。")
    if NET.self_test_ok is not True:
        stops.append("AC-5：離線籠子自我測試沒通過，無法證明全程零連線。")
    cw = raw["cache_write_check"]
    if not cw["unchanged"]:
        stops.append("AC-5：pionex_cache 執行前後的指紋不一致。")

    verdict = {
        "primary_window": primary,
        "primary_window_reason": "決定延遲上限的是尾端，尾端需要最長的觀測窗口；"
                                 "full_universe_bars 是分母完全一致的對照組。",
        # AC-1 不只看「差集為空」：差集為空可能是粗篩全通過（空轉），也可能是粗篩看錯欄位
        # （captain PRD 11(a)：差集照樣空、候選數卻整個錯）。所以等價性實證是 AC-1 的一部分。
        "AC1_leakage_ok": (leakage["all_ok"] and raw["baseline_n"] > 0 and lowest_reject > 0
                           and equivalence["ok"]),
        "AC1_screen_equivalence_ok": equivalence["ok"],
        "AC1_screen_equivalence_checks": dict(equivalence["checks"]),
        "AC2_distribution_complete": all(
            all(k in r["windows"][primary]["distribution"]
                for k in ("p50", "p90", "p95", "p99", "max"))
            and len(r["busiest_bars"]) > 0 for r in by_threshold),
        "AC3_rest_seconds_flagged": all(
            len(r["windows"][primary]["rest_seconds"]["per_rate"]) == len(rates)
            for r in by_threshold),
        "AC4_margin_present": margin["distribution"].get("n", 0) > 0,
        "AC5_offline_and_scope_ok": (NET.self_test_ok is True) and not NET.attempts
                                    and cw["unchanged"],
        "stop_conditions_triggered": stops,
        "overall": "PASS" if not stops else "STOP_AND_REPORT",
    }

    result = {
        "task": "A0", "method_notes": METHOD_NOTES,
        "interval": raw["interval"], "bar_ms": bar_ms, "bars_per_hour": raw["bars_per_hour"],
        "thresholds": thresholds, "rates_req_per_s": rates,
        "universe": universe,
        "windows": window_meta,
        "candidates_per_bar": {
            "note": "每根 K 棒有幾個交易對通過 ret2h 粗篩，也就是要打幾次 klines。"
                    f"分母是 universe.symbols_used 這批（全部為 {args.quote_asset.upper()} 計價）。",
            "primary_window": primary,
            "by_threshold": by_threshold,
        },
        "projection_to_market": projection,
        "leakage_check": leakage,
        "signal_margin": margin,
        "offline_proof": NET.report(),
        "cache_write_check": cw,
        "input_contract": {
            "note": "餵給 s4_signal 的 DataFrame 一律 time 升冪、固定週期、無缺漏"
                    "（由 g2_measure.prepare_frame 保證，本檔不自行補洞）。",
            "bars_per_hour_is_int": isinstance(raw["bars_per_hour"], int),
            "filled_bars_total": raw["filled_total"],
            "time_monotonic_all": raw["monotonic_all"],
            "fixed_period_all": raw["fixed_period_all"],
            "nan_ret2h_total": raw["nan_ret2h_total"],
            "expected_nan_ret2h_total": exp_nan_ret2h,
            "nan_ret2h_matches_expected": nan_ret2h_ok,
            "nan_turn_total": raw["nan_turn_total"],
            "expected_nan_turn_total": exp_nan_turn,
            "nan_turn_matches_expected": nan_turn_ok,
            "nan_parity_mismatches": raw["nan_parity_mismatches"],
            "nan_parity_note":
                f"粗篩的判定（screen_mask：{SCREEN_FEATURE} >= 門檻，NaN 為 False）與"
                "**實際呼叫 signal_from_features()**（其他三項中性化後）對同一項的判定，"
                f"在全部 {raw['parity_rows']} 格逐格比對的不一致格數，必須是 0。"
                "細節與控制組見 input_contract.screen_equivalence；"
                "這個欄位與 screen_equivalence.mismatches 是同一個數字。",
            "screen_equivalence": equivalence,
            "screen_pass_bars_by_threshold": {
                ("%g" % t): v for t, v in raw["screen_pass_bars"].items()},
        },
        "verdict": verdict,
    }
    result["metadata"] = build_meta(ctx, {
        "cache_dir": str(args.cache_dir), "interval": raw["interval"],
        "quote_asset": args.quote_asset.upper(),
        "thresholds_cli": args.thresholds, "thresholds_effective": thresholds,
        "rates": rates, "latency_target_s": target_s, "latency_ceiling_s": ceiling_s,
        "max_symbols": args.max_symbols, "replay_bars": args.replay_bars,
        "top_busy": args.top_busy, "busy_symbols": args.busy_symbols,
        "market_usdt_symbols": market_n, "max_missed_records": args.max_missed_records,
        "min_span_days": args.min_span_days,
    })
    # PRD 6 的字面要求是 metadata 要帶「快取涵蓋範圍與交易對數」，本檔原本放在 universe（F6）。
    # office 端已經在 parse 這份 json，所以既有欄位一律留在原位不動、不改名、不搬家，
    # 這裡只新增一份引用複本到 metadata，內容與 universe 下的完全相同。
    result["metadata"]["cache_coverage"] = dict(universe["cache_coverage"])
    result["metadata"]["cache_files_for_interval"] = universe["cache_files_for_interval"]
    result["metadata"]["quote_matched"] = universe["quote_matched"]
    result["metadata"]["symbols_considered"] = universe["symbols_considered"]
    result["metadata"]["symbols_used"] = universe["symbols_used"]
    result["metadata"]["symbols_skipped"] = universe["symbols_skipped"]
    result["metadata"]["cache_coverage_note"] = (
        "PRD 6 要求 metadata 帶「快取涵蓋範圍與交易對數」（F6）。這六個欄位是 "
        "universe.cache_coverage / universe.cache_files_for_interval / universe.quote_matched / "
        "universe.symbols_considered / universe.symbols_used / universe.symbols_skipped 的"
        "引用複本，內容完全相同；原欄位保留原位，避免打壞已經在 parse 這份 json 的下游。")
    return result


def cache_fingerprint(cache_dir):
    """pionex_cache 的（檔數 + 每個檔的 size/mtime）指紋。AC-5 要比對執行前後沒有寫入。"""
    cache_dir = str(cache_dir)
    if not os.path.isdir(cache_dir):
        return None
    items = {}
    for name in sorted(os.listdir(cache_dir)):
        try:
            st = os.stat(os.path.join(cache_dir, name))
        except OSError:
            continue
        items[name] = (int(st.st_size), int(st.st_mtime_ns))
    digest = hashlib.sha256(
        "\n".join(f"{k}\t{v[0]}\t{v[1]}" for k, v in items.items()).encode("utf-8")).hexdigest()
    newest = max((v[1] for v in items.values()), default=None)
    return {"files": len(items), "fingerprint_sha256": digest, "newest_mtime_ns": newest,
            "newest_mtime_utc": g2.fmt_ms(newest // 1_000_000) if newest else None}


def backup_existing(ctx):
    """已有同名結果就先搬走，絕不靜默覆寫。回傳搬到哪（沒有舊檔回 None）。"""
    path = Path(ctx.out_path)
    if not path.exists():
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = path.with_name(f"{path.stem}.{stamp}{path.suffix}")
    try:
        os.replace(str(path), str(dest))
    except OSError as e:
        raise A0Error(f"無法備份既有結果 {path}：{e}\n→ 請手動改名或刪除後重跑。")
    g2.say(f"[注意] 已有舊結果 {path}，已搬到 {dest} 保存。")
    return dest


def run(ctx):
    """整段量測都在離線籠子裡（AC-5）。

    F4：籠子在這裡（而不是 module import 時）武裝，離開就還原，不留 process-wide 副作用。
    覆蓋範圍沒有變弱——讀快取、算特徵、算訊號、建結果、寫 json 全部在區間內；
    main() 另外在更外層武裝一次（可重入，自我測試只做一次），所以直接呼叫 run() 的
    測試路徑同樣在籠子裡。唯一在區間外的是 argparse 與 socket.gethostname()。
    """
    with NET.armed(reason="run(): 整段量測"):
        return _run_inside_cage(ctx)


def _run_inside_cage(ctx):
    g2.say("=== A0：2 小時漲幅粗篩的候選數量測（完全離線）===")
    before = cache_fingerprint(ctx.args.cache_dir)
    raw = aggregate(collect(ctx))
    after = cache_fingerprint(ctx.args.cache_dir)
    raw["cache_write_check"] = {
        "note": "AC-5：只讀不寫。比對執行前後的檔數與每個檔的 size/mtime 指紋。",
        "cache_dir": str(ctx.args.cache_dir),
        "before": before, "after": after,
        "unchanged": bool(before and after
                          and before["fingerprint_sha256"] == after["fingerprint_sha256"]),
    }
    if not raw["cache_write_check"]["unchanged"]:
        ctx.note("pionex_cache 的指紋在執行前後不一致。本腳本只讀不寫，"
                 "請確認是不是有別的程式同時在動快取。")
    result = build_result(raw, ctx)

    # BUG-048 的教訓：舊結果的搬移壓到最後、緊貼寫入才做。
    # 中途崩潰時磁碟上還是上一次的完整結果，錯誤訊息也就不會承諾一個不存在的「部分結果」。
    ctx.backup_path = backup_existing(ctx)
    result["metadata"]["output_backup"] = str(ctx.backup_path) if ctx.backup_path else None
    g2.write_json(ctx.out_path, result)
    summarize(result, ctx)
    return result


def summarize(result, ctx):
    u = result["universe"]
    primary = result["candidates_per_bar"]["primary_window"]
    cov = u["cache_coverage"]
    rows = result["candidates_per_bar"]["by_threshold"]
    g2.say("")
    g2.say(f"標的池：快取 {u['cache_files_for_interval']} 個 {u['interval']} 檔案 → "
           f"{u['quote_asset_required']} 計價 {u['quote_matched']} 個、"
           f"排除 {u['quote_excluded_count']} 個；實際使用 {u['symbols_used']} 個"
           f"（跳過 {u['symbols_skipped']}）")
    g2.say(f"快取涵蓋：{cov['first_bar_open_utc']} → {cov['last_bar_open_utc']}　"
           f"{cov['span_days']} 天　{cov['grid_bars']} 根網格")
    g2.say(f"口徑提醒：這 {u['symbols_used']} 個是抓快取當下的標的池，不等於全市場 "
           f"{result['projection_to_market']['market_usdt_symbols']} 個 "
           f"{u['quote_asset_required']} 計價合約（見 method_notes [A5]）")
    for name, w in result["windows"].items():
        g2.say(f"  窗口 {name}：{w['bars']} 根、{w['span_days']} 天、每根有效交易對數 "
               f"p05 {w['symbols_observed'].get('p05')} / 中位 {w['symbols_observed'].get('p50')}"
               f" / 最多 {w['symbols_observed'].get('max')}"
               + ("　← 判讀主值" if w["is_primary"] else ""))
    g2.say("")
    g2.say(f"每根 K 棒候選數（窗口 {primary}）：")
    g2.say("  門檻    p50     p90     p95     p99     max   平均    候選為 0 的比例")
    for r in rows:
        w = r["windows"][primary]
        d = w["distribution"]
        g2.say("  %-7g %-7.1f %-7.1f %-7.1f %-7.1f %-5d %-7.2f %.1f%%%s" % (
            r["threshold"], d["p50"], d["p90"], d["p95"], d["p99"], d["max"], d["mean"],
            w["zero_candidate_share"] * 100,
            "  ← 策略門檻" if r["is_strategy_threshold"] else ""))
    g2.say("")
    g2.say("換算 REST 秒數（[A4] 只含 klines，是端到端下界）：")
    for r in rows:
        w = r["windows"][primary]
        for pr in w["rest_seconds"]["per_rate"]:
            g2.say("  門檻 %-6g %2g req/s → p50 %.2fs  p95 %.2fs  p99 %.2fs  max %.2fs"
                   "　[p99 vs 目標 %g s：%s／vs 上限 %g s：%s]" % (
                       r["threshold"], pr["rate_req_per_s"], pr["p50_s"], pr["p95_s"],
                       pr["p99_s"], pr["max_s"], w["rest_seconds"]["target_s"],
                       "超過" if pr["p99_exceeds_target"] else "未超過",
                       w["rest_seconds"]["ceiling_s"],
                       "超過" if pr["p99_exceeds_ceiling"] else "未超過"))
    g2.say("")
    g2.say("外推到全市場 %d 個（線性外推，不是實測）：" % (
        result["projection_to_market"]["market_usdt_symbols"]))
    for pr in result["projection_to_market"]["by_threshold"]:
        parts = "　".join("%g req/s p99 %.2fs / max %.2fs" % (
            x["rate_req_per_s"], x["p99_s"], x["max_s"]) for x in pr["rest_seconds"])
        g2.say("  門檻 %-6g p99 候選 %.0f、max 候選 %.0f　%s" % (
            pr["threshold"], pr["p99_candidates"], pr["max_candidates"], parts))
    g2.say("")
    if rows[0]["busiest_bars"]:
        g2.say(f"最忙的前 {len(rows[0]['busiest_bars'])} 根（門檻 {rows[0]['threshold']:g}）：")
        for row in rows[0]["busiest_bars"]:
            g2.say(f"  #{row['rank']:<2} {row['bar_open_utc']}　候選 {row['candidates']}"
                   f"（該根有效交易對 {row['symbols_observed']}）")
    g2.say("")
    lk = result["leakage_check"]
    g2.say(f"漏失率驗證（baseline 訊號 {lk['baseline_signals_total']} 筆）：")
    for r in lk["by_threshold"]:
        g2.say(f"  門檻 {r['threshold']:<7g} 漏失 {r['missed_signals']} 筆"
               f"{'（結構上必為 0）' if r['must_be_zero'] else ''}　"
               f"{'OK' if r['ok'] else 'FAIL'}")
    g2.say("  提醒：這是定義決定的 0（[A2]），不是量測成果；它的作用是驗實作沒走偏。")
    g2.say(f"  鑑別力檢查：粗篩在最低門檻下仍擋掉某些交易對的 K 棒數 = "
           f"{lk['check_is_non_vacuous']['bars_where_screen_rejects_someone_at_lowest_threshold']}"
           "（> 0 才代表這個驗證不是空轉）")
    eqv = result["input_contract"]["screen_equivalence"]
    g2.say("")
    g2.say("粗篩 vs signal_from_features 等價性實證（[A1]／captain PRD 11(a)）：")
    g2.say(f"  逐格比對 {eqv['rows_compared']} 格、不一致 {eqv['mismatches']} 格"
           f"（粗篩說通過 {eqv['screen_says_pass_rows']} 格、"
           f"signal 說通過 {eqv['signal_says_pass_rows']} 格、"
           f"NaN {eqv['nan_rows_in_screen_feature']} 格）")
    for k, v in eqv["checks"].items():
        g2.say(f"  {'OK  ' if v else 'FAIL'} {k}")
    g2.say(f"  → 等價性{'成立' if eqv['ok'] else '不成立（候選數不可用，見上面的停下來回報）'}")
    m = result["signal_margin"]["distribution"]
    g2.say("")
    g2.say(f"訊號 ret2h 餘裕分布（n={m['n']}）：min {m['min']:.4f}　p05 {m['p05']:.4f}"
           f"　p10 {m['p10']:.4f}　p25 {m['p25']:.4f}　中位 {m['p50']:.4f}　max {m['max']:.4f}")
    for row in result["signal_margin"]["error_tolerance_by_threshold"]:
        g2.say("  門檻 %-7g 一筆都不漏能容忍的低估 %.4f　容忍 95%% 訊號 %.4f" % (
            row["threshold"], row["tolerable_underestimate_all_signals"],
            row["tolerable_underestimate_95pct"]))
    g2.say("")
    op, cw = result["offline_proof"], result["cache_write_check"]
    g2.say(f"離線籠子：自我測試{'通過' if op['self_test_ok'] else '未通過'}"
           f"（攔下 {op['self_test_blocked_calls']} 次探測）、"
           f"真正的連線企圖 {op['connect_attempts']} 次")
    g2.say(f"pionex_cache 前後比對：{cw['before']['files']} → {cw['after']['files']} 個檔案、"
           f"指紋{'一致（未寫入）' if cw['unchanged'] else '不一致（有寫入！）'}")
    v = result["verdict"]
    g2.say("")
    g2.say(f"判定：{v['overall']}　AC-1 {v['AC1_leakage_ok']}／AC-2 {v['AC2_distribution_complete']}"
           f"／AC-3 {v['AC3_rest_seconds_flagged']}／AC-4 {v['AC4_margin_present']}"
           f"／AC-5 {v['AC5_offline_and_scope_ok']}")
    for s in v["stop_conditions_triggered"]:
        g2.say(f"  [必須停下來回報] {s}")
    g2.say(f"已寫出 {ctx.out_path}")


# ============================== CLI ==============================
EPILOG = """\
典型用法：

  1) 冒煙（少數交易對，只驗流程，數字沒有代表性）
     python research/a0_screen.py --max-symbols 20 --out research/_tmp/a0_smoke.json

  2) 正式量測（全部 5M 快取，數分鐘）
     python research/a0_screen.py

  3) 把結果帶回
     git add research/a0_screen.py research/results/a0_screen.json

全程不連網、不寫入 pionex_cache；兩件事都由程式自己出具證據
（結果 json 的 offline_proof / cache_write_check）。

離開碼：0 = 判定 PASS／1 = 有「必須停下來回報」的條件／2 = 可預期的錯誤／
        4 = 預期外的例外／5 = 離線籠子攔到連線企圖／130 = Ctrl-C
"""


def build_parser():
    ap = argparse.ArgumentParser(
        prog="a0_screen.py",
        description="A0 — 2 小時漲幅粗篩的候選數量測（完全離線，只讀 pionex_cache）",
        epilog=EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)

    g = ap.add_argument_group("資料來源")
    g.add_argument("--cache-dir", default=str(REPO_ROOT / "pionex_cache"),
                   help="K 棒快取目錄（預設 <repo>/pionex_cache，只讀）")
    g.add_argument("--interval", default="5M", choices=sorted(g2.INTERVAL_MS),
                   help="K 棒週期（預設 5M）")
    g.add_argument("--quote-asset", default="USDT",
                   help="只計入這個計價幣的交易對（預設 USDT，理由見 method_notes [A3]）")
    g.add_argument("--max-symbols", type=int, default=0,
                   help="最多處理幾個交易對，0 = 全部（>0 只適合冒煙，候選數會失去代表性）")
    g.add_argument("--replay-bars", type=int, default=0,
                   help="每個交易對最多取最後幾根 K 棒，0 = 快取全部")

    g = ap.add_argument_group("量測參數")
    g.add_argument("--thresholds", default="0.10,0.11,0.12,0.13,0.135",
                   help="粗篩門檻掃描，逗號分隔（預設 0.10,0.11,0.12,0.13,0.135）。"
                        f"s4_signal.DEFAULT_PARAMS[{SCREEN_PARAM_KEY!r}] 會自動補成最後一格，"
                        "不必也不可手寫；高於策略門檻的值會被拒絕。")
    g.add_argument("--rates", default="10,6",
                   help="REST 速率 req/s，逗號分隔（預設 10,6：派網官方上限與保守值）")
    g.add_argument("--latency-target-s", type=float, default=10.0,
                   help="A 頻道延遲目標值，超過就標記（預設 10 秒，WBS 6.3）")
    g.add_argument("--latency-ceiling-s", type=float, default=30.0,
                   help="A 頻道延遲上限，超過就標記（預設 30 秒，WBS 6.3）")
    g.add_argument("--market-usdt-symbols", type=int, default=568,
                   help="全市場 USDT 計價 TRADING PERP 數，只用於線性外推"
                        "（預設 568，來源 B5/TASK-098 實測）")
    g.add_argument("--min-span-days", type=float, default=7.0,
                   help="快取涵蓋天數低於此值就列為必須回報（預設 7）")

    g = ap.add_argument_group("輸出")
    g.add_argument("--out", default=None, help="結果 json 路徑（預設 <out-dir>/a0_screen.json）")
    g.add_argument("--out-dir", default=None, help="結果輸出目錄（預設 research/results）")
    g.add_argument("--machine", default=None, help="機器識別，寫進 metadata（預設 hostname）")
    g.add_argument("--top-busy", type=int, default=10,
                   help="每個門檻列出最忙的前幾根 K 棒（預設 10）")
    g.add_argument("--busy-symbols", type=int, default=30,
                   help="最忙那一根額外列出幾個候選交易對（預設 30，0 = 不列）")
    g.add_argument("--max-missed-records", type=int, default=50,
                   help="每個門檻的漏失明細最多輸出幾筆（預設 50）")
    return ap


def _output_state(path):
    """錯誤訊息只能講當下磁碟上的事實。

    BUG-048 的反面教材：g2_measure 的錯誤路徑會說「部分結果保留在 X」，但 Stage 3/4
    只在最後一次性寫檔、開頭又把舊檔搬走了，那句話是假的。本腳本沒有中途 checkpoint，
    所以這裡只說出當下真的成立的狀態——沒有人在旁邊判斷這句話是不是假的。
    """
    path = Path(path)
    if path.exists():
        return (f"→ 磁碟上的 {path} 是**上一次**執行的結果，本次沒有覆寫它。\n"
                "→ 本腳本沒有中途 checkpoint，這次沒有產生任何部分結果，直接重跑即可。")
    return (f"→ 沒有任何結果檔寫出（{path} 不存在）。\n"
            "→ 本腳本沒有中途 checkpoint，也不會留半成品，直接重跑即可。")


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = build_parser().parse_args(argv)
    # F4：從這裡開始到 main() 結束全程在籠子裡（run() 內層還會再武裝一次，可重入、
    # 自我測試只做一次）。區間外只剩 argparse 本身，它沒有任何建立連線的能力。
    with NET.armed(reason="main(): 整個執行"):
        return _main_inside_cage(args)


def _main_inside_cage(args):
    ctx = Ctx(args)
    g2.say(f"{SCRIPT_VERSION}　machine={ctx.machine}　"
           f"（完全離線；快取 helper 來自 {g2.SCRIPT_VERSION}）")
    g2.say(f"輸出：{ctx.out_path}")
    g2.say("")
    try:
        result = run(ctx)
    except NetworkBlocked as e:
        print(f"\n[錯誤] {e}\n{_output_state(ctx.out_path)}", file=sys.stderr, flush=True)
        return 5
    except g2.G2Error as e:
        print(f"\n[錯誤] {e}\n{_output_state(ctx.out_path)}", file=sys.stderr, flush=True)
        return 2
    except KeyboardInterrupt:
        print(f"\n[中斷] 使用者按了 Ctrl-C。\n{_output_state(ctx.out_path)}",
              file=sys.stderr, flush=True)
        return 130
    except Exception as e:
        traceback.print_exc()
        print(f"\n[預期外的錯誤] {type(e).__name__}: {e}"
              f"\n→ 這不是本腳本預期的錯誤路徑，上面的 traceback 是唯一線索。"
              f"\n{_output_state(ctx.out_path)}"
              f"\n→ 請把整段訊息帶回，腳本指紋 script_sha256={(_script_sha256() or '?')[:16]}…",
              file=sys.stderr, flush=True)
        return 4
    return 0 if result["verdict"]["overall"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
