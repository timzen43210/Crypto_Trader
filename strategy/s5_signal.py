# -*- coding: utf-8 -*-
"""
策略5 訊號核心 — 爆量過高 / 群組訊號複製（只做空）
==================================================
由 pionex_strategy5.py（回測）與 pionex_dryrun.s5_indicators()（dry run）抽離出來的「進場訊號」
唯一實作（G5）。回測、dry run 與日後的實盤 / paper trading 端都呼叫這裡，訊號邏輯只允許存在這一份。

相依限制：只 import numpy / pandas 與標準庫。禁止 import pionex_backtest、pionex_strategy5、
pionex_dryrun、requests、openpyxl——實盤端不能因為要算訊號就把整套回測程式（網路、Excel、全域 CONFIG）拖進來。

條件（以 1 小時 K 為單位，整點切；小K = 呼叫端的 K 棒，1M / 5M / 15M / 30M / 60M 都適用）：
  ① 前一小時：收盤 ÷ 開盤 − 1 ≥ MIN_RISE_FROM_OPEN（開盤 = 前一小時第一根小K 的 open）
  ② 爆量，分支符合任一個即可：
       A. 本小時到目前為止的累計量 ≥ MIN_VOL_MULT × 前一小時的量（不限時）
       限時分支 EARLY_VOL_RULES 的每個 (N 分鐘, k 倍)：小K收盤分鐘 ≤ N 且累計量 ≥ k × 前一小時的量，
       第一次成立的那根起，本小時剩下的小K都算成立（黏著）。純粹是量條件，突破前高是 ③ 的事。
  ③ 現價（這根小K收盤）> 前一小時最高價 × (1 + MIN_ABOVE_PH)
  前一小時的小K必須完整（根數 = 每小時根數），否則該小時不發訊號。
  三條件第一次同時成立的那根小K = 進場點（每小時最多一次），以它的收盤價進場。
訊號值：-1 = 做空進場、0 = 無訊號。

本模組只實作 v3（①②③ 全開、HH_MODE="price"、不加成交額 / 價格過濾）。回測的其他變體
（只有 ①、①+③、HH_MODE="high"、成交額 / 價格過濾）與條件拆解屬於研究用途，留在 pionex_strategy5.py，
但它們用到的共同計算（小時脈絡、①②③、分支編碼、每小時第一次）一律呼叫這裡的函式。
evaluate() / signal() 收到非 v3 的參數會直接拋 ValueError，不會靜默照 v3 算。

出場：出場**參數**（止盈 / 止損 / 出場模式 / 最長持倉）的唯一來源在本檔的 EXIT_PARAMS；
出場**邏輯**不在這裡，仍由呼叫端實作（回測與 dry run 是 pionex_backtest.backtest）。
手續費 FEE_RATE 屬於回測帳務，不在這裡（在 pionex_strategy5.CONFIG）。同根雙觸發的判定週期
（RESOLVE_*）跟著小K週期走，回測與 dry run 本來就不同，也不在這裡。

輸入契約（呼叫端負責，本模組不檢查、不補洞、不排序）：
  * df 至少有 time / open / high / low / close / volume 六欄（數值型）。
    time = 這根小K的**開盤**時刻，UTC 毫秒整數（int64），對齊 bar_ms 的整數倍。
  * df 依 time 升冪、固定週期 bar_ms、沒有重複的 time。
  * 缺漏的小K要先由呼叫端補成 close=前值、open/high/low=close、volume=0
    （即 pionex_backtest.load_hourly / pionex_dryrun.prepare 的輸出）。沒補的話，缺根的那一小時
    根數不足 → 下一小時視為「前一小時不完整」不發訊號（不會算錯，只是少訊號）。
  * 要判斷某一小時，df 必須從**前一小時的整點第一根**開始涵蓋（黏著狀態與累計量都從整點算起）；
    資料開頭那個不完整的小時不會被拿來當「前一小時」。
  * 價格 > 0、量 ≥ 0。前一小時開盤價 ≤ 0 → ① 為 NaN；前一小時量 ≤ 0 → ② 量倍數為 NaN（都不發訊號）。
  * bar_ms 是整數毫秒：60_000 的整數倍且整除一小時（1M / 5M / 15M / 30M / 60M）。
    限時分支的 N 必須是小K分鐘數的整數倍，否則看不到第 N 分鐘的邊界（evaluate() 會拒絕，
    回測 pionex_strategy5.check_rules() 也會在開頭擋下）。
  * 回傳的 Series / DataFrame 與 df 共用同一個 index；不會改動傳入的 df。

NaN 行為（與抽離前兩份實作逐值等價）：
  * 資料開頭的第一個小時沒有「前一小時」→ ph_* 為 NaN、① ② 為 NaN、③ 為 False、不完整。
  * 任一比較遇到 NaN 視為 False，不會誤發訊號。

抽離時兩份舊實作的差異有兩處，都只出現在交易所不可能給出的輸入上；這裡一律採 dry run 的寫法，
所以 dry run 在任何輸入下都不變，回測只在這些輸入下才和抽離前不同：
  * ① 前一小時開盤價 ≤ 0：回測原本算出 ±inf / NaN（可能因此發訊號），dry run 與這裡算出 NaN → 不發訊號。
    這是**訊號上**唯一的差異。
  * ② 前一小時量 < 0：回測原本算出負的量倍數，dry run 與這裡算出 NaN。只影響回測報表的 volx 診斷值，
    不影響訊號（限時分支倍數有 > 0 的格式檢查、MIN_VOL_MULT 實務上也 > 0，負值與 NaN 同樣不成立）。
    量 ≥ 0 時兩邊完全相同（量 = 0 → NaN 的防呆兩邊本來就一樣）。
"""
import copy
import operator

import numpy as np
import pandas as pd

HOUR_MS = 3_600_000
MINUTE_MS = 60_000

# ============================== 進場參數 ==============================
# pionex_strategy5.S5 = {**DEFAULT_PARAMS}（回測），pionex_dryrun.S5_RULE = dict(pionex_strategy5.S5）（dry run）。
# 注意：dry run 的 s5 帳本指紋把整份 S5（= 這份字典的全部鍵）算進去，改任何鍵名、值或數值型別
# （例如 0 改成 0.0、0.0 改成 0），s5 帳本就會以新參數清空重跑——真的要改參數時這是預期行為，
# 但不可以為了「只是整理程式碼」而動到它們。指紋用 json 序列化，看不出 tuple 與 list 的差別，
# EARLY_VOL_RULES 的型別由 tests/test_s5_signal.py 釘住。
DEFAULT_PARAMS = {
    "MIN_RISE_FROM_OPEN": 0.06,    # ① 前一根：收盤 ÷ 開盤 − 1 下限（v2 的 MIN_RISE_FROM_LOW 是 收盤 ÷ 最低，已停用）
    "MIN_VOL_MULT": 2.0,           # ②A 本小時累計量 ÷ 前一根量 下限，不限時；2.0 = 多一倍（上一根 10 → 這根 ≥ 20）
    # ② 限時分支：每個 (N 分鐘, k 倍) = 開盤 N 分鐘內（小K收盤分鐘 ≤ N）累計量曾 ≥ k × 前一根量，
    #    成立後本小時剩下的時間都算（黏著）。N 為 1～60 的整數，k > 0；() = 關閉，只剩 A。
    #    預設 B = (30, 1.0) 30 分內追平、C = (15, 0.5) 15 分內達一半。要增減分支只改這一行。
    #    小K週期必須整除每個 N（見 early_rules_window_problems()）；dry run 用 1 分K，1～60 都看得到。
    #    維持 tuple of tuple：型別改了指紋看不出來，由 tests/test_s5_signal.py 釘住。
    "EARLY_VOL_RULES": ((30, 1.0), (15, 0.5)),
    "REQUIRE_VOL_BURST": True,     # ② 爆量開關；False = 不看量（回測變體，本模組不實作）
    "REQUIRE_HIGHER_HIGH": True,   # ③ 過前高；②③ 都關 = 只有 ①，前根收盤即進場（回測變體，本模組不實作）
    "HH_MODE": "price",            # ③ 怎麼算過前高："price" = 現價（小K收盤）> 前高（v2 規則下和群組 99.6% 吻合）；
                                   #                 "high" = 當根曾經碰過前高之上（最早的原版，回測變體）
    "MIN_ABOVE_PH": 0.0,           # ③ 用 "price" 時，現價須高於前高至少此比例（0.01 = 1%）
    # ---- 以下預設關閉（None = 不限），想加過濾再開（回測變體，本模組不實作）----
    "MIN_TURN24H": None,           # 近24h成交額下限（USDT），例 20_000
    "MAX_TURN24H": None,           # 近24h成交額上限，例 500_000
    "MAX_PRICE": None,             # 價格上限
    # 出場後冷卻幾小時。2026-09-24 使用者決定以 dry run 為準（當時的 v2 群組校準版），由 1.0 改為 0，v3 沿用：
    # 0 代表「出場的下一根 K 棒起可再進場」（cooldown_bars() 以 max(1, round(COOLDOWN_HOURS × 每小時根數))
    # 換算，0 → 1 根）；同一小時仍最多進場一次（first_per_hour）。維持整數 0：改成 0.0 會改變 s5 指紋。
    "COOLDOWN_HOURS": 0,
}
PARAM_KEYS = tuple(DEFAULT_PARAMS)          # 本模組認得的 11 個鍵；params 裡多出來的鍵一律忽略（舊鍵除外，見下）

# 已移除的舊鍵：留在參數裡不會有任何作用，所以回測 check_rules()、dry run check_s5_variant()
# 與本模組的 evaluate() 都會擋下
LEGACY_KEYS = {"MIN_RISE_FROM_LOW": "改用 MIN_RISE_FROM_OPEN（① 已改成 收盤 ÷ 開盤）",
               "FAST_VOL_MULT": "改用 EARLY_VOL_RULES", "FAST_WINDOW_MIN": "改用 EARLY_VOL_RULES"}

# evaluate() 只實作 v3：這些開關必須是下列值（True / None 用 is 比較，字串用 ==）。
# pionex_dryrun.check_s5_variant() 用同一張表，dry run 與本模組對「支援哪些變體」只有一份定義。
V3_SWITCHES = {"REQUIRE_VOL_BURST": True, "REQUIRE_HIGHER_HIGH": True, "HH_MODE": "price",
               "MIN_TURN24H": None, "MAX_TURN24H": None, "MAX_PRICE": None}

# ============================== 出場參數 ==============================
# 策略5 出場參數的唯一來源（G5，比照策略4 的 G1b）。回測（pionex_strategy5.CONFIG）與 dry run
# （pionex_dryrun.S5_CONFIG，經 pionex_strategy5.CONFIG）都從這裡取值；實盤端判定出場也要從這裡取，
# 任何地方都不可以再抄一份數字。
# 刻意與 DEFAULT_PARAMS 分開：dry run 的 s5 帳本指紋把整份 S5（= DEFAULT_PARAMS）算進去，
# 多一個鍵 s5 指紋就變。這四個鍵另外經由 pb.CONFIG 進入指紋（pionex_dryrun.FP_KEYS）：改任何值，
# s5 帳本同樣會以新參數清空重跑。
# 鍵名與 pionex_backtest.CONFIG 相同；請用 exit_params() 取拷貝，不要直接拿這個字典去展開。
EXIT_PARAMS = {
    # 固定比例出場：做空，跌 TAKE_PROFIT 止盈、漲 STOP_LOSS 止損。
    # 日後若改成 "atr"，用到這份參數的地方（含 dry run 的 check_s5_variant()、實盤）必須一起檢查。
    "EXIT_MODE": "fixed",
    # 止盈：跌 3%。先比照策略4 當時的 3%；最佳組合請用 pionex_tpsl_sweep.py（STRATEGY="s5"）找。
    "TAKE_PROFIT": 0.03,
    # 止損：漲 5% = 群組第 1 次加倉點（群組把有加倉的單記為止損）。
    "STOP_LOSS": 0.05,
    # 持倉時限（小時）。None = 沒有時間停損，只靠止盈 / 止損出場。
    "MAX_HOLD_HOURS": None,
}


def exit_params():
    """回傳 EXIT_PARAMS 的深拷貝。要把出場參數放進 pb.CONFIG 或任何之後可能被就地修改的字典時一律用這個，
       呼叫端怎麼改都不會汙染 EXIT_PARAMS 這份唯一來源。"""
    return copy.deepcopy(EXIT_PARAMS)


# ============================== 參數檢查 ==============================
def early_rules_problems(rules):
    """EARLY_VOL_RULES 的格式檢查，回傳問題清單（空 = 沒問題）。
       必須是序列（tuple / list），每個元素是 (1～60 的整數分鐘, 正數倍數)；空序列 = 只剩分支 A。
       pionex_dryrun.check_s5_variant() 與回測 check_rules() 也用這個函式，規則只有一份。"""
    if not isinstance(rules, (tuple, list)):
        return [f"EARLY_VOL_RULES 必須是 (分鐘, 倍數) 的序列（tuple 或 list），目前是 {rules!r}"]
    out = []
    for i, r in enumerate(rules):
        if not isinstance(r, (tuple, list)) or len(r) != 2:
            out.append(f"EARLY_VOL_RULES[{i}] = {r!r}：每個分支必須是 (分鐘, 倍數)")
            continue
        n, k = r
        if isinstance(n, bool) or not isinstance(n, (int, np.integer)) or not 1 <= n <= 60:
            out.append(f"EARLY_VOL_RULES[{i}] = {r!r}：分鐘必須是 1～60 的整數")
        if (isinstance(k, bool) or not isinstance(k, (int, float, np.integer, np.floating))
                or not np.isfinite(k) or k <= 0):
            out.append(f"EARLY_VOL_RULES[{i}] = {r!r}：倍數必須是大於 0 的數字")
    return out


def early_rules_window_problems(rules, bar_ms):
    """小K週期看不看得到每個限時分支的窗口邊界。回傳 [(i, N, k, 小K分鐘數), ...]（空 = 全部看得到）。
       N 必須是小K分鐘數的整數倍，否則「第 N 分鐘」落在某根小K中間，只能用下一根收盤的累計量去判斷，
       會靜默算錯（例：30M 看不到 15 分、60M 看不到 30 分）。rules 必須先通過 early_rules_problems()。"""
    step = bar_minutes(bar_ms)
    return [(i, n, k, step) for i, (n, k) in enumerate(rules) if n % step]


def v3_switch_ok(key, value):
    """value 是不是 V3_SWITCHES[key] 要求的值（True / None 用 is 比較，其他用 ==）。"""
    want = V3_SWITCHES[key]
    return value is want if (want is True or want is None) else value == want


def params_problems(params, bar_ms=None):
    """evaluate() 能不能用這組參數：回傳問題清單（空 = 可以）。檢查舊鍵、EARLY_VOL_RULES 格式、
       v3 開關，給了 bar_ms 再檢查限時分支窗口。缺鍵不在這裡檢查（evaluate() 直接拋 KeyError）。"""
    out = [f"參數含已移除的舊鍵 {k!r}，不會有任何作用：{why}，並刪除這個鍵"
           for k, why in LEGACY_KEYS.items() if k in params]
    for k, want in V3_SWITCHES.items():
        if k in params and not v3_switch_ok(k, params[k]):
            out.append(f"{k} = {params[k]!r}：本模組只實作 v3（{k} 必須是 {want!r}），"
                       f"其他變體只在回測 pionex_strategy5.py 裡")
    if "EARLY_VOL_RULES" in params:
        probs = early_rules_problems(params["EARLY_VOL_RULES"])
        out += probs
        if not probs and bar_ms is not None:
            out += [f"限時分支 EARLY_VOL_RULES[{i}] = ({n}, {k:g})：小K每根 {step} 分鐘，看不到第 {n} 分鐘的邊界"
                    for i, n, k, step in early_rules_window_problems(params["EARLY_VOL_RULES"], bar_ms)]
    return out


def _params(params, bar_ms):
    """params=None → 預設值；否則要有全部 11 個鍵（缺鍵直接報錯，不悄悄退回預設），且通過 params_problems()。"""
    p = DEFAULT_PARAMS if params is None else params
    missing = [k for k in PARAM_KEYS if k not in p]
    if missing:
        raise KeyError(f"策略5 參數缺少 {missing}")
    probs = params_problems(p, bar_ms)
    if probs:
        raise ValueError("策略5 參數不是 strategy.s5_signal 支援的組合：\n  " + "\n  ".join(probs))
    return {k: p[k] for k in PARAM_KEYS}


def branch_label(code, rules=None):
    """② 爆量分支的文字標籤，由分支參數產生（不寫死）：code 的 bit0 = A，bit(i+1) = EARLY_VOL_RULES[i]。
       例：A → "2倍"、B → "30分1倍"、C → "15分0.5倍"、B+C → "30分1倍+15分0.5倍"；0 → "-"。
       rules 預設為 DEFAULT_PARAMS；回測傳入 pionex_strategy5.S5，dry run 傳入 S5_RULE。"""
    rules = DEFAULT_PARAMS if rules is None else rules
    code = int(code)
    names = [f"{rules['MIN_VOL_MULT']:g}倍"] + [f"{n}分{k:g}倍" for n, k in rules["EARLY_VOL_RULES"]]
    got = [nm for i, nm in enumerate(names) if code >> i & 1]
    return "+".join(got) if got else "-"


# ============================== 週期換算 ==============================
def bar_minutes(bar_ms):
    """一根小K幾分鐘。bar_ms 必須是整數、60_000 的整數倍且整除一小時，否則 ValueError。"""
    ms = operator.index(bar_ms)             # 只收整數（含 numpy 整數），float 直接報錯
    if ms <= 0 or ms % MINUTE_MS or HOUR_MS % ms:
        raise ValueError(f"bar_ms 必須是 60_000 的整數倍且整除一小時（1M/5M/15M/30M/60M），收到 {bar_ms!r}")
    return ms // MINUTE_MS


def bars_in_hour(bar_ms):
    """一小時有幾根小K（= 前一小時「完整」要求的根數）。"""
    bar_minutes(bar_ms)
    return HOUR_MS // operator.index(bar_ms)


def cooldown_bars(bars_per_hour, params=None):
    """出場後冷卻根數 = max(1, round(COOLDOWN_HOURS × bars_per_hour))（只讀 COOLDOWN_HOURS）。
       bars_per_hour 是整數（回測與 dry run 傳各自的 H()）。"""
    p = DEFAULT_PARAMS if params is None else params
    n = operator.index(bars_per_hour)
    if n < 1:
        raise ValueError(f"bars_per_hour 必須 ≥ 1，收到 {bars_per_hour!r}")
    return max(1, round(p["COOLDOWN_HOURS"] * n))


# ============================== 小時脈絡 ==============================
def hour_ids(time_ms):
    """每根小K所屬的小時編號 = time // HOUR_MS（整點切；UTC 整點 = 台北整點）。"""
    return np.asarray(time_ms) // HOUR_MS


def hour_table(df):
    """把小K彙整成每小時一列（index = 小時編號）：open（第一根的 open）/ high / low / close（最後一根）/
       volume（合計）/ n（根數）。"""
    hid = hour_ids(df["time"].to_numpy())
    g = df.groupby(hid, sort=True)
    return pd.DataFrame({"open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(),
                         "close": g["close"].last(), "volume": g["volume"].sum(), "n": g.size()})


def hour_context(df, bar_ms, hours=None):
    """每根小K的小時脈絡（DataFrame，與 df 同 index）：
         hid       所屬小時編號
         ph_open / ph_high / ph_low / ph_close / ph_vol / ph_n
                   前一小時（必須剛好是 hid-1）的開高低收、量、根數；沒有前一小時 → NaN
         cum_vol   本小時到這根為止的累計量（含這根）
         minute    這根小K**收盤**時是該小時第幾分鐘（1～60）
       數值欄保留輸入的 dtype（例如 volume 是 int64 時 cum_vol 也是 int64），不另外轉型。
       hours：呼叫端已經算過 hour_table(df) 時可傳入，省一次 groupby（必須是同一個 df 算出來的）。"""
    bar_minutes(bar_ms)
    t = df["time"].to_numpy()
    hid = hour_ids(t)
    hr = hour_table(df) if hours is None else hours
    prev = hr.reindex(hr.index - 1)              # 前一小時（必須剛好是 hid-1）
    prev.index = hr.index
    m = lambda s: pd.Series(hid).map(s).to_numpy()
    ctx = pd.DataFrame(index=df.index)
    ctx["hid"] = hid
    ctx["ph_open"], ctx["ph_high"], ctx["ph_low"] = m(prev["open"]), m(prev["high"]), m(prev["low"])
    ctx["ph_close"], ctx["ph_vol"], ctx["ph_n"] = m(prev["close"]), m(prev["volume"]), m(prev["n"])
    ctx["cum_vol"] = df.groupby(hid)["volume"].cumsum().to_numpy()
    ctx["minute"] = ((t % HOUR_MS) + bar_ms) // MINUTE_MS
    return ctx


def prev_hour_complete(ph_n, bar_ms):
    """前一小時的小K是否完整（根數 = 每小時根數）。NaN → False。回傳 bool ndarray。"""
    return np.asarray(ph_n, dtype=float) == bars_in_hour(bar_ms)


# ============================== ① ② ③ ==============================
def rise_from_open(ph_open, ph_close):
    """① 前一小時 收盤 ÷ 開盤 − 1。開盤 ≤ 0 或 NaN → NaN。回傳 float ndarray。"""
    o = np.asarray(ph_open, dtype=float)
    c = np.asarray(ph_close, dtype=float)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(o > 0, c / o - 1, np.nan)


def cond_rise(complete, rise, min_rise):
    """① 成立：前一小時完整 且 rise ≥ min_rise（NaN → False）。回傳 bool ndarray。"""
    with np.errstate(invalid="ignore"):
        return np.asarray(complete, dtype=bool) & (np.asarray(rise, dtype=float) >= min_rise)


def volume_multiple(cum_vol, ph_vol):
    """② 本小時累計量 ÷ 前一小時量。前一小時量 ≤ 0 或 NaN → NaN。回傳 float ndarray。"""
    cv = np.asarray(cum_vol, dtype=float)
    pv = np.asarray(ph_vol, dtype=float)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(pv > 0, cv / pv, np.nan)


def burst_branches(volx, minute, hid, min_vol_mult, early_rules):
    """② 爆量的各分支（bool ndarray），回傳 (A, [限時分支…])，限時分支順序同 early_rules。
       A：volx ≥ min_vol_mult（不限時）
       限時分支 (N, k)：minute ≤ N 且 volx ≥ k「第一次成立的那根起」，同一小時（hid）剩下的小K都算成立（黏著）。
       volx NaN → 不成立。"""
    volx = np.asarray(volx, dtype=float)
    minute = np.asarray(minute)
    hid = np.asarray(hid)
    with np.errstate(invalid="ignore"):
        a = volx >= min_vol_mult
        early = []
        for n, k in early_rules:
            hit = (minute <= n) & (volx >= k)
            early.append(pd.Series(hit.astype(np.int64)).groupby(hid).cumsum().to_numpy() > 0)
    return a, early


def branch_code(a, early):
    """把 burst_branches() 的結果編成整數：bit0 = A，bit(i+1) = 第 i 個限時分支（標籤見 branch_label()）。"""
    code = a.astype(np.int64)
    for i, e in enumerate(early):
        code |= e.astype(np.int64) << (i + 1)
    return code


def any_early(early, n):
    """任一限時分支成立（沒有限時分支 → 全 False）。"""
    return np.logical_or.reduce(early) if early else np.zeros(n, dtype=bool)


def burst_any(a, early):
    """② 成立：A 或任一限時分支。"""
    return a | any_early(early, len(a))


def pct_vs_prev_high(close, ph_high):
    """現價相對前一小時最高價：close ÷ ph_high − 1（診斷用；> 0 = 在前高之上）。
       傳 pandas Series 時照 pandas 規則對齊 index，傳 ndarray 則逐位置計算。"""
    with np.errstate(invalid="ignore", divide="ignore"):
        return close / ph_high - 1


def breaks_prev_high(close, ph_high, min_above):
    """③ 過前高：現價 > 前一小時最高價 × (1 + min_above)。NaN → False。回傳 bool ndarray。"""
    with np.errstate(invalid="ignore"):
        return np.asarray(close > ph_high * (1 + min_above), dtype=bool)


def first_per_hour(mask, hid):
    """每小時只保留第一個 True（條件『第一次』同時成立的那根小K）。mask 為 bool ndarray。"""
    s = pd.Series(mask.astype(int)).groupby(hid).cumsum().to_numpy()
    return mask & (s == 1)


# ============================== 一步到位 ==============================
EVALUATE_COLUMNS = ("hid", "minute", "complete", "rise", "volx", "above", "branch_code",
                    "rise_ok", "burst_ok", "break_ok", "signal")


def evaluate(df, bar_ms, params=None):
    """df（小K）→ 每根的條件與訊號（DataFrame，與 df 同 index，欄位見 EVALUATE_COLUMNS）：
         hid / minute     小時編號、這根收盤是第幾分鐘
         complete         前一小時完整
         rise             ① 前一小時 收盤 ÷ 開盤 − 1
         volx             ② 本小時累計量 ÷ 前一小時量
         above            現價 ÷ 前一小時最高 − 1（診斷）
         branch_code      ② 當下成立的分支（bit0 = A、bit(i+1) = EARLY_VOL_RULES[i]；標籤見 branch_label()）
         rise_ok / burst_ok / break_ok   ①（含 complete）/ ② / ③ 是否成立
         signal           -1 = 本小時三條件第一次同時成立的那根（做空進場）、0 = 其他
       params=None 用 DEFAULT_PARAMS；傳入的 dict 要有全部 11 個鍵且是 v3（見 params_problems()），
       否則 KeyError / ValueError。輸入契約見檔頭。"""
    p = _params(params, bar_ms)
    ctx = hour_context(df, bar_ms)
    hid = ctx["hid"].to_numpy()
    minute = ctx["minute"].to_numpy()
    close = df["close"].to_numpy(dtype=float)
    ph_high = ctx["ph_high"].to_numpy(dtype=float)
    complete = prev_hour_complete(ctx["ph_n"], bar_ms)
    rise = rise_from_open(ctx["ph_open"], ctx["ph_close"])
    volx = volume_multiple(ctx["cum_vol"], ctx["ph_vol"])
    a, early = burst_branches(volx, minute, hid, p["MIN_VOL_MULT"], p["EARLY_VOL_RULES"])
    c1 = cond_rise(complete, rise, p["MIN_RISE_FROM_OPEN"])
    c2 = burst_any(a, early)
    c3 = breaks_prev_high(close, ph_high, p["MIN_ABOVE_PH"])
    first = first_per_hour(c1 & c2 & c3, hid)
    return pd.DataFrame({
        "hid": hid, "minute": minute, "complete": complete, "rise": rise, "volx": volx,
        "above": pct_vs_prev_high(close, ph_high), "branch_code": branch_code(a, early),
        "rise_ok": c1, "burst_ok": c2, "break_ok": c3, "signal": np.where(first, -1, 0),
    }, index=df.index)


def signal(df, bar_ms, params=None):
    """一步到位：df → 訊號。回傳 int Series（與 df 同 index），-1 = 做空進場、0 = 無訊號。"""
    return evaluate(df, bar_ms, params)["signal"].rename("signal")
