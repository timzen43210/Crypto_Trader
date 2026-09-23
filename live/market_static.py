# -*- coding: utf-8 -*-
"""
live.market_static — 全市場交易對規格 + 槓桿上限的記憶體快取
===========================================================
兩個公開端點、每次刷新共 2 個請求，整理成以 symbol 為鍵的查詢結構，只放記憶體、不落地。

    GET /api/v1/common/symbols    type=PERP status=TRADING   → 交易對規格（baseStep 等）
    GET /api/v1/common/riskTable  type=PERP                  → 分層槓桿表，只取 tier1 maxLeverage

誰在用（B5 PRD §1.1）：A 頻道訊息要印該幣的槓桿上限、資料層要 TRADING PERP 清單、
日後的下單端要 baseStep / quotePrecision / minNotional。這裡只負責「存下來、查得到」，
不做取整、不做部位計算、不做跳層。我們的部位 notional 約 100 USDT，永遠落在 tier1。

真實回應結構（2026-09-22 在 laptop 實測，原文見 TASK-002 的 code attachment）：
    symbols   : data.symbols[]  每筆 {symbol, name, type, baseCurrency, quoteCurrency,
                basePrecision, quotePrecision, minNotional, baseStep, quoteStep, minSizeLimit,
                maxSizeLimit, maxImpactLimit, minSizeMarket, maxSizeMarket, maxImpactMarket,
                maxOrderNum, status, liquidationFeeRate}
    riskTable : data.symbols[]  每筆 {symbol, rows: [{rowNum, notionalLimit, maxLeverage,
                maintMarginRatio, quickDeduction}, ...]}，數值一律是字串，rowNum 從 1 起算，
                notionalLimit 與 rowNum 同向遞增、maxLeverage 遞減。
    規格 dict 原封不動存下來，不改欄位名、不轉型 —— 下單端需要哪個欄位自己轉。

刷新由呼叫者觸發（refresh / refresh_if_stale），沒有背景執行緒。查詢函式絕不自己打 API，
從沒載入過就拋 NotLoadedError。刷新失敗保留舊快取、記 last_error，不拋例外。

用法：
    from live import market_static as ms
    ms.refresh()                              # 啟動時強制載入一次；回傳 True/False
    ms.refresh_if_stale()                     # 每根 K 棒呼叫一次；超過 1 小時才會真的打 API
    ms.trading_symbols(quote="USDT")          # ['BTC_USDT_PERP', ...]
    ms.max_leverage("BTC_USDT_PERP")          # 100；查不到回 None
    ms.symbol_spec("BTC_USDT_PERP")["baseStep"]
    ms.status()                               # 各種時間戳與 last_error

人工驗證（真實連線，共 4 個請求）：python -m live.market_static
"""

import time

from live import config
from live.pionex_api import api_get

SYMBOLS_PATH = "/api/v1/common/symbols"
SYMBOLS_PARAMS = {"type": "PERP", "status": "TRADING"}
RISK_TABLE_PATH = "/api/v1/common/riskTable"
RISK_TABLE_PARAMS = {"type": "PERP"}

# refresh_if_stale() 的預設逾時。數值的單一來源在 live.config，這裡只是給預設參數用的別名，
# 要調整請改 live/config.py，不要改這一行。
STALE_SECONDS = config.MARKET_STATIC_STALE_SECONDS


class NotLoadedError(RuntimeError):
    """還沒成功 refresh() 過就來查詢。"""


# ---------------- 快取本體：模組層級、只在記憶體 ----------------
_specs = {}        # symbol -> 交易對規格 dict（symbols 端點的原始元素，原封不動）
_leverage = {}     # symbol -> tier1 maxLeverage (int)；riskTable 有這個幣但 rows 解析不出的不放
_loaded = False    # 至少成功 refresh 過一次

last_refresh_at = None   # 最近一次「成功」刷新的時間（epoch 秒）；給日誌 / status() 顯示用
last_refresh_mono = None # 同一次成功刷新當下的 time.monotonic() 讀數；逾時判斷只看這個，
                         # 因為雲端主機會 NTP 校時，wall clock 往前往後跳都會算錯經過多久。
                         # 它是「開機以來的秒數」這類相對值，不是可顯示的時間，別拿去印。
last_attempt_at = None   # 最近一次嘗試刷新的時間（不論成敗）
last_refresh_ok = None   # 最近一次嘗試的結果；None = 從未嘗試
last_error = None        # 最近一次失敗的訊息（"ExceptionType: message"）；成功不會清掉它
last_error_at = None     # 最近一次失敗的時間


# ---------------- 取數與解析 ----------------
def fetch_symbols():
    """打 symbols 端點，回傳 data.symbols 那個 list（原始元素）。失敗拋 live.pionex_api.ApiError。"""
    return api_get(SYMBOLS_PATH, SYMBOLS_PARAMS)["data"]["symbols"]


def fetch_risk_table():
    """打 riskTable 端點，回傳 data.symbols 那個 list（原始元素）。失敗拋 live.pionex_api.ApiError。"""
    return api_get(RISK_TABLE_PATH, RISK_TABLE_PARAMS)["data"]["symbols"]


def tier1_max_leverage(entry):
    """從 riskTable 的單筆條目取 tier1 的 maxLeverage，回傳 int；取不到回 None。

    tier1 的定義：rows 裡 notionalLimit 最小的那一列。不用 rowNum 或 list 順序，因為
    「我們的 notional 永遠落在最低那一檔」才是業務上的意思，靠數值比靠排序穩。實測
    623 筆裡 rowNum==1 的列與 notionalLimit 最小的列完全一致（見 code attachment）。

    rows 為空、或每一列都缺欄位 / 不是數字 → None；個別壞掉的列會被跳過。
    """
    best_limit, best_lev = None, None
    for row in entry.get("rows") or []:
        try:
            limit = float(row["notionalLimit"])
            lev = int(float(row["maxLeverage"]))
        except (KeyError, TypeError, ValueError):
            continue
        if best_limit is None or limit < best_limit:
            best_limit, best_lev = limit, lev
    return best_lev


def build_cache(symbols, risk_table):
    """把兩個端點的原始 list 整理成 (specs, leverage) 兩個以 symbol 為鍵的 dict。純函式。"""
    specs = {}
    for s in symbols:
        sym = s.get("symbol")
        if sym:
            specs[sym] = s
    leverage = {}
    for e in risk_table:
        sym = e.get("symbol")
        if not sym:
            continue
        lev = tier1_max_leverage(e)
        if lev is not None:
            leverage[sym] = lev
    return specs, leverage


# ---------------- 刷新 ----------------
def refresh():
    """強制刷新：打兩個端點、整理、整包換掉舊快取。回傳 True 成功 / False 失敗，不拋例外。

    兩個端點都成功、且都不是空清單，才算成功；任一失敗就整包不換（不會出現 symbols 是新的、
    槓桿是舊的這種半套狀態），舊快取繼續服務，失敗原因記在 last_error / last_error_at。
    這裡接住的是 Exception（含 ApiError 與解析時的 KeyError 等），不接 KeyboardInterrupt。
    """
    global _specs, _leverage, _loaded
    global last_refresh_at, last_refresh_mono, last_attempt_at, last_refresh_ok, last_error, last_error_at

    last_attempt_at = time.time()
    try:
        symbols = fetch_symbols()
        risk_table = fetch_risk_table()
        specs, leverage = build_cache(symbols, risk_table)
        if not specs:
            raise ValueError(f"{SYMBOLS_PATH} 回傳空清單，不拿它蓋掉舊快取")
        if not leverage:
            raise ValueError(f"{RISK_TABLE_PATH} 回傳空清單或全部解析失敗，不拿它蓋掉舊快取")
    except Exception as e:
        last_refresh_ok = False
        last_error = f"{type(e).__name__}: {e}"
        last_error_at = time.time()
        return False

    _specs, _leverage = specs, leverage
    _loaded = True
    last_refresh_at = time.time()
    last_refresh_mono = time.monotonic()
    last_refresh_ok = True
    return True


def refresh_if_stale(max_age=STALE_SECONDS):
    """距上次成功刷新不到 max_age 秒 → 不發任何請求、回傳 True；否則呼叫 refresh() 並回傳其結果。

    逾時是看「上次成功」的時間，所以刷新失敗之後下一次呼叫會再試，直到成功為止；
    呼叫節奏由呼叫者控制（預期是每根 K 棒一次）。

    經過多久是用 time.monotonic() 算的，不是 time.time()：單調時鐘不受 NTP 校時影響。
    用 wall clock 的話，系統時間往後跳會讓快取被當成永遠新鮮（跳多久就不刷多久），
    往前跳則會多打一次沒必要的請求。雲端主機有 NTP，這不是假想情境。
    """
    if last_refresh_mono is not None and time.monotonic() - last_refresh_mono < max_age:
        return True
    return refresh()


# ---------------- 查詢 ----------------
def _require_loaded():
    if not _loaded:
        raise NotLoadedError("market_static 尚未載入任何資料：請先呼叫 refresh()")


def is_loaded():
    """至少成功 refresh 過一次。"""
    return _loaded


def trading_symbols(quote=None):
    """目前 status=TRADING 的 PERP symbol 清單（排序過）。quote 給 "USDT" 之類可只取該計價幣。"""
    _require_loaded()
    return sorted(
        sym for sym, s in _specs.items()
        if s.get("status") == "TRADING" and (quote is None or s.get("quoteCurrency") == quote)
    )


def symbol_spec(symbol):
    """該 symbol 的交易對規格（symbols 端點的原始欄位，淺拷貝）。查不到回 None。"""
    _require_loaded()
    s = _specs.get(symbol)
    return None if s is None else dict(s)


def max_leverage(symbol):
    """該 symbol 的 tier1 maxLeverage (int)。riskTable 沒這個幣、或 rows 解析不出 → None。"""
    _require_loaded()
    return _leverage.get(symbol)


def status():
    """目前快取狀態，給日誌 / 健康檢查用。"""
    return {
        "loaded": _loaded,
        "symbols": len(_specs),
        "leverage": len(_leverage),
        "last_refresh_at": last_refresh_at,
        "last_attempt_at": last_attempt_at,
        "last_refresh_ok": last_refresh_ok,
        "last_error": last_error,
        "last_error_at": last_error_at,
    }


# ---------------- 人工驗證：python -m live.market_static ----------------
def _probe():
    """真實連線的人工驗證。第一段打兩個端點各一次印原始結構，第二段跑 refresh() 走正式路徑。"""
    import json
    import sys
    from collections import Counter

    from live.pionex_api import ApiError

    # stdout 接到 pipe 時 Windows 會用 cp1252，印中文會炸；這裡只影響這支 probe
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    def hit(name, path, params):
        print(f"\n[{name}] GET {path} params={params}")
        t0 = time.perf_counter()
        try:
            js = api_get(path, params)
        except ApiError as e:
            print(f"  失敗（{time.perf_counter() - t0:.3f}s）：{e}")
            return None
        dt = time.perf_counter() - t0
        print(f"  HTTP 200（api_get 只在 200 且 result=true 時回傳）  耗時 {dt:.3f}s"
              + ("  ※ 超過 5 秒" if dt > 5 else ""))
        print(f"  頂層鍵     : {list(js.keys())}")
        data = js.get("data")
        print(f"  data 的鍵  : {list(data.keys()) if isinstance(data, dict) else type(data).__name__}")
        return js

    print("=== live.market_static 人工驗證（第一段：原始端點） ===")
    js_s = hit("symbols", SYMBOLS_PATH, SYMBOLS_PARAMS)
    js_r = hit("riskTable", RISK_TABLE_PATH, RISK_TABLE_PARAMS)
    if js_s is None or js_r is None:
        print("\n任一端點失敗，後面不跑。")
        return 1

    symbols = js_s["data"]["symbols"]
    risk_table = js_r["data"]["symbols"]
    trading = [s["symbol"] for s in symbols if s.get("status") == "TRADING"]
    print(f"\nsymbols  : 總數 {len(symbols)}，status=TRADING {len(trading)}")
    print(f"  單筆欄位 : {list(symbols[0].keys())}")
    print(f"riskTable: 總數 {len(risk_table)}")
    print(f"  單筆欄位 : {list(risk_table[0].keys())}；rows 元素欄位 : {list(risk_table[0]['rows'][0].keys())}")

    specs, leverage = build_cache(symbols, risk_table)
    trading_set = set(trading)

    def dist(levs):
        c = Counter(levs)
        return " / ".join(f"{k}x({v})" for k, v in sorted(c.items(), reverse=True)) + f"  共 {sum(c.values())}"

    print(f"\ntier1 maxLeverage 分布（riskTable 全部條目）    : {dist(leverage.values())}")
    print(f"tier1 maxLeverage 分布（只算 TRADING symbols）: "
          f"{dist(v for k, v in leverage.items() if k in trading_set)}")
    unparsed = [e['symbol'] for e in risk_table if e.get('symbol') and e['symbol'] not in leverage]
    print(f"riskTable 有但 tier1 解析不出 : {len(unparsed)} {unparsed[:10]}")
    only_risk = sorted(set(leverage) - trading_set)
    only_sym = sorted(trading_set - set(leverage))
    print(f"在 riskTable 但不是 TRADING symbol : {len(only_risk)} {only_risk[:12]}")
    print(f"TRADING symbol 但 riskTable 沒有   : {len(only_sym)} {only_sym[:12]}")

    print("\n=== 第二段：refresh() 正式路徑 ===")
    ok = refresh()
    print(f"refresh() -> {ok}")
    print("status() ->", json.dumps(status(), ensure_ascii=False))
    if not ok:
        return 1

    samples = ["BTC_USDT_PERP", "ETH_USDT_PERP", "SOL_USDT_PERP", "DOGE_USDT_PERP"]
    low = next((s for s in trading_symbols(quote="USDT") if max_leverage(s) == 5), None)
    if low:
        samples.append(low)
    for sym in samples:
        print(f"\n--- {sym} ---")
        print(f"  max_leverage : {max_leverage(sym)}")
        print(f"  symbol_spec  : {json.dumps(symbol_spec(sym), ensure_ascii=False)}")
        row = next((e for e in risk_table if e.get("symbol") == sym), None)
        print(f"  riskTable 原文: {json.dumps(row, ensure_ascii=False)}")

    print(f"\ntrading_symbols()             : {len(trading_symbols())}")
    print(f"trading_symbols(quote='USDT') : {len(trading_symbols(quote='USDT'))}")
    print(f"查不到的 symbol -> max_leverage={max_leverage('NOPE_USDT_PERP')} "
          f"symbol_spec={symbol_spec('NOPE_USDT_PERP')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_probe())
