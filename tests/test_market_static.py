# -*- coding: utf-8 -*-
"""
B5 (market_static) 驗收測試 — riskTable tier1 解析、未載入即查詢、refresh_if_stale 不發請求、
刷新失敗保留舊快取，外加 live.http 的信封 / 429 / SSL 訊息。

全程離線：monkeypatch live.market_static.api_get 與 live.http.requests.get，不打任何外部 API。
不依賴 pytest（這台開發機沒裝）：直接 `python tests/test_market_static.py` 會逐一跑完並印結果；
用 pytest 跑也可以，函式都是 test_ 開頭、不用任何 fixture。
"""
import importlib
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import live.http as lh  # noqa: E402

# ============================== 假資料（欄位名照真實回應） ==============================
SYMBOLS_FIXTURE = [
    {"symbol": "BTC_USDT_PERP", "type": "PERP", "baseCurrency": "BTC", "quoteCurrency": "USDT",
     "basePrecision": 4, "quotePrecision": 1, "minNotional": "1", "baseStep": "0.0001",
     "status": "TRADING"},
    {"symbol": "ABC_USDT_PERP", "type": "PERP", "baseCurrency": "ABC", "quoteCurrency": "USDT",
     "basePrecision": 0, "quotePrecision": 4, "minNotional": "1", "baseStep": "1",
     "status": "TRADING"},
    {"symbol": "ETH_BTC_PERP", "type": "PERP", "baseCurrency": "ETH", "quoteCurrency": "BTC",
     "basePrecision": 3, "quotePrecision": 5, "minNotional": "0.0001", "baseStep": "0.001",
     "status": "TRADING"},
    {"symbol": "OLD_USDT_PERP", "type": "PERP", "baseCurrency": "OLD", "quoteCurrency": "USDT",
     "basePrecision": 0, "quotePrecision": 4, "minNotional": "1", "baseStep": "1",
     "status": "HALT"},
]

RISK_FIXTURE = [
    # 刻意把 rows 順序打亂：tier1 不能靠 list 第 0 個
    {"symbol": "BTC_USDT_PERP", "rows": [
        {"rowNum": 2, "notionalLimit": "12000000", "maxLeverage": "50", "maintMarginRatio": "0.01", "quickDeduction": "10000"},
        {"rowNum": 1, "notionalLimit": "2000000", "maxLeverage": "100", "maintMarginRatio": "0.005", "quickDeduction": "0"},
        {"rowNum": 3, "notionalLimit": "100000000", "maxLeverage": "20", "maintMarginRatio": "0.025", "quickDeduction": "190000"},
    ]},
    {"symbol": "ABC_USDT_PERP", "rows": [
        {"rowNum": 1, "notionalLimit": "10000", "maxLeverage": "25", "maintMarginRatio": "0.02", "quickDeduction": "0"},
    ]},
    {"symbol": "ETH_BTC_PERP", "rows": []},                                   # 空 rows → None
    {"symbol": "BAD_USDT_PERP", "rows": [
        {"rowNum": 1, "notionalLimit": "not-a-number", "maxLeverage": "25"},  # 壞列 → 跳過
        {"rowNum": 2, "notionalLimit": "50000", "maxLeverage": "10"},
    ]},
    {"symbol": "OLD_USDT_PERP", "rows": [
        {"rowNum": 1, "notionalLimit": "10000", "maxLeverage": "20"},
    ]},
]


def _envelope(symbols_list):
    return {"result": True, "data": {"symbols": symbols_list}, "timestamp": 1758500000000}


def _fake_api_get_factory(symbols=SYMBOLS_FIXTURE, risk=RISK_FIXTURE, fail_paths=()):
    """回傳 (fake_api_get, calls)。calls 記錄每次 (path, params)；path 在 fail_paths 裡就拋 ApiError。"""
    calls = []

    def fake_api_get(path, params=None, retries=3, timeout=20):
        calls.append((path, dict(params or {})))
        if path in fail_paths:
            raise lh.ApiError(f"{path}: 模擬失敗")
        if path == "/api/v1/common/symbols":
            return _envelope(symbols)
        if path == "/api/v1/common/riskTable":
            return _envelope(risk)
        raise AssertionError(f"沒預期的 path: {path}")

    return fake_api_get, calls


def _fresh_module():
    """重新載入 live.market_static，讓每個測試從「從未 refresh」的乾淨狀態開始。"""
    import live.market_static as ms
    return importlib.reload(ms)


def _loaded_module():
    ms = _fresh_module()
    fake, calls = _fake_api_get_factory()
    ms.api_get = fake
    assert ms.refresh() is True
    return ms, calls


# ============================== tier1 解析（唯一有分支的地方） ==============================
def test_tier1_picks_lowest_notional_not_list_order():
    ms = _fresh_module()
    assert ms.tier1_max_leverage(RISK_FIXTURE[0]) == 100      # rows[0] 是 50x，正解是 100x
    assert ms.tier1_max_leverage(RISK_FIXTURE[1]) == 25


def test_tier1_returns_none_on_empty_or_unparseable():
    ms = _fresh_module()
    assert ms.tier1_max_leverage({"symbol": "X", "rows": []}) is None
    assert ms.tier1_max_leverage({"symbol": "X"}) is None
    assert ms.tier1_max_leverage({"symbol": "X", "rows": [{"rowNum": 1}]}) is None
    assert ms.tier1_max_leverage({"symbol": "X", "rows": [{"notionalLimit": "1", "maxLeverage": "abc"}]}) is None


def test_tier1_skips_bad_row_and_uses_next():
    ms = _fresh_module()
    assert ms.tier1_max_leverage(RISK_FIXTURE[3]) == 10        # 第一列壞掉，取第二列


def test_tier1_accepts_float_strings():
    ms = _fresh_module()
    e = {"symbol": "X", "rows": [{"notionalLimit": "1e6", "maxLeverage": "75.0"}]}
    assert ms.tier1_max_leverage(e) == 75


def test_build_cache_keys_by_symbol():
    ms = _fresh_module()
    specs, lev = ms.build_cache(SYMBOLS_FIXTURE, RISK_FIXTURE)
    assert set(specs) == {"BTC_USDT_PERP", "ABC_USDT_PERP", "ETH_BTC_PERP", "OLD_USDT_PERP"}
    assert lev == {"BTC_USDT_PERP": 100, "ABC_USDT_PERP": 25, "BAD_USDT_PERP": 10, "OLD_USDT_PERP": 20}
    assert "ETH_BTC_PERP" not in lev                            # 空 rows 不放進去


# ============================== AC-3：未 refresh 就查詢 ==============================
def test_query_before_refresh_raises_with_hint():
    ms = _fresh_module()
    assert ms.is_loaded() is False
    for fn in (ms.trading_symbols, lambda: ms.symbol_spec("BTC_USDT_PERP"), lambda: ms.max_leverage("BTC_USDT_PERP")):
        try:
            fn()
        except ms.NotLoadedError as e:
            assert "refresh()" in str(e), str(e)
        else:
            raise AssertionError("未載入就查詢應該拋 NotLoadedError")
    assert ms.status()["loaded"] is False                        # status() 不拋，給健康檢查用


# ============================== AC-3：第一次 refresh 後查得到 ==============================
def test_refresh_then_query():
    ms, calls = _loaded_module()
    assert [c[0] for c in calls] == ["/api/v1/common/symbols", "/api/v1/common/riskTable"]
    assert calls[0][1] == {"type": "PERP", "status": "TRADING"}
    assert calls[1][1] == {"type": "PERP"}

    assert ms.is_loaded() is True
    assert ms.trading_symbols() == ["ABC_USDT_PERP", "BTC_USDT_PERP", "ETH_BTC_PERP"]   # HALT 的不算
    assert ms.trading_symbols(quote="USDT") == ["ABC_USDT_PERP", "BTC_USDT_PERP"]
    assert ms.max_leverage("BTC_USDT_PERP") == 100
    assert ms.max_leverage("ETH_BTC_PERP") is None              # riskTable 有但 rows 空
    assert ms.max_leverage("NOPE_USDT_PERP") is None            # 查不到 → None
    assert ms.symbol_spec("BTC_USDT_PERP")["baseStep"] == "0.0001"
    assert ms.symbol_spec("NOPE_USDT_PERP") is None

    spec = ms.symbol_spec("BTC_USDT_PERP")
    spec["baseStep"] = "tampered"                                 # 淺拷貝，改了不影響快取
    assert ms.symbol_spec("BTC_USDT_PERP")["baseStep"] == "0.0001"

    st = ms.status()
    assert st["loaded"] and st["symbols"] == 4 and st["leverage"] == 4
    assert st["last_refresh_ok"] is True and st["last_error"] is None
    assert st["last_refresh_at"] is not None and st["last_attempt_at"] is not None


# ============================== AC-3：refresh_if_stale 未逾時不發請求 ==============================
def test_refresh_if_stale_makes_no_request_when_fresh():
    ms, calls = _loaded_module()
    n = len(calls)
    assert n == 2
    assert ms.refresh_if_stale() is True
    assert ms.refresh_if_stale() is True
    assert len(calls) == n, "未逾時卻發了請求"


def test_refresh_if_stale_refreshes_when_stale():
    ms, calls = _loaded_module()
    ms.last_refresh_at = time.time() - (ms.STALE_SECONDS + 1)   # 假裝一小時零一秒前刷的
    assert ms.refresh_if_stale() is True
    assert len(calls) == 4, "逾時了卻沒發請求"
    assert time.time() - ms.last_refresh_at < 5


def test_refresh_if_stale_honours_custom_max_age():
    ms, calls = _loaded_module()
    ms.last_refresh_at = time.time() - 10
    assert ms.refresh_if_stale(max_age=60) is True and len(calls) == 2
    assert ms.refresh_if_stale(max_age=5) is True and len(calls) == 4


# ============================== AC-3 / FR-5：刷新失敗保留舊快取 ==============================
def test_refresh_failure_keeps_old_cache_and_records_error():
    ms, _ = _loaded_module()
    good_refresh_at = ms.last_refresh_at

    fake_fail, _ = _fake_api_get_factory(fail_paths=("/api/v1/common/symbols",))
    ms.api_get = fake_fail
    assert ms.refresh() is False                                # 回傳 False，不拋

    st = ms.status()
    assert st["last_refresh_ok"] is False
    assert st["last_error"] and "ApiError" in st["last_error"] and "模擬失敗" in st["last_error"]
    assert st["last_error_at"] is not None
    assert st["last_refresh_at"] == good_refresh_at             # 成功時間不變
    assert st["loaded"] is True
    assert ms.max_leverage("BTC_USDT_PERP") == 100              # 舊資料還在
    assert ms.trading_symbols(quote="USDT") == ["ABC_USDT_PERP", "BTC_USDT_PERP"]

    # 之後 refresh_if_stale 會再試（逾時是看上次成功），成功後 last_refresh_ok 回到 True
    fake_ok, calls = _fake_api_get_factory()
    ms.api_get = fake_ok
    ms.last_refresh_at = time.time() - (ms.STALE_SECONDS + 1)
    assert ms.refresh_if_stale() is True and len(calls) == 2
    assert ms.status()["last_refresh_ok"] is True
    assert ms.status()["last_error"] is not None                # 歷史錯誤不清掉，看 last_refresh_ok 判斷


def test_refresh_partial_failure_is_atomic():
    """symbols 成功、riskTable 失敗 → 整包不換，不會出現半新半舊。"""
    ms, _ = _loaded_module()
    new_symbols = SYMBOLS_FIXTURE + [{"symbol": "NEW_USDT_PERP", "quoteCurrency": "USDT", "status": "TRADING"}]
    fake, _ = _fake_api_get_factory(symbols=new_symbols, fail_paths=("/api/v1/common/riskTable",))
    ms.api_get = fake
    assert ms.refresh() is False
    assert ms.symbol_spec("NEW_USDT_PERP") is None              # 新 symbols 沒有被半套寫入
    assert ms.max_leverage("BTC_USDT_PERP") == 100


def test_refresh_rejects_empty_lists():
    ms, _ = _loaded_module()
    fake, _ = _fake_api_get_factory(symbols=[])
    ms.api_get = fake
    assert ms.refresh() is False
    assert "空清單" in ms.status()["last_error"]
    assert ms.status()["symbols"] == 4

    fake, _ = _fake_api_get_factory(risk=[])
    ms.api_get = fake
    assert ms.refresh() is False
    assert ms.status()["leverage"] == 4


def test_first_refresh_failure_stays_unloaded():
    ms = _fresh_module()
    fake, _ = _fake_api_get_factory(fail_paths=("/api/v1/common/symbols",))
    ms.api_get = fake
    assert ms.refresh() is False
    assert ms.is_loaded() is False
    try:
        ms.trading_symbols()
    except ms.NotLoadedError:
        pass
    else:
        raise AssertionError("第一次就失敗，查詢仍應拋 NotLoadedError")
    assert ms.refresh_if_stale() is False                       # 從沒成功過 → 會再試 → 仍失敗


def test_refresh_survives_unexpected_structure():
    """回應長得不一樣（沒有 data.symbols）→ KeyError 也要被接住，回 False 不崩。"""
    ms, _ = _loaded_module()

    def weird(path, params=None, retries=3, timeout=20):
        return {"result": True, "data": {"riskTable": []}}

    ms.api_get = weird
    assert ms.refresh() is False
    assert "KeyError" in ms.status()["last_error"]
    assert ms.max_leverage("BTC_USDT_PERP") == 100


# ============================== live.http：信封 / 429 / SSL ==============================
class _Resp:
    def __init__(self, status_code, js=None, text=""):
        self.status_code = status_code
        self._js = js
        self.text = text or (str(js) if js is not None else "")

    def json(self):
        if self._js is None:
            raise ValueError("no json")
        return self._js


def _with_patched(get_impl, fn):
    """暫時把 live.http 的 requests.get 與 time.sleep 換掉，跑 fn 後還原。"""
    orig_get, orig_sleep = lh.requests.get, lh.time.sleep
    lh.requests.get, lh.time.sleep = get_impl, lambda s: None
    try:
        return fn()
    finally:
        lh.requests.get, lh.time.sleep = orig_get, orig_sleep


def test_http_result_false_raises_with_code_and_path():
    def get(url, params=None, headers=None, timeout=None):
        return _Resp(200, {"result": False, "code": "INVALID_PARAM", "message": "bad type"})

    try:
        _with_patched(get, lambda: lh.api_get("/api/v1/common/symbols", {"type": "X"}))
    except lh.ApiError as e:
        s = str(e)
        assert "/api/v1/common/symbols" in s and "INVALID_PARAM" in s and "bad type" in s, s
    else:
        raise AssertionError("result=false 應拋 ApiError")


def test_http_429_then_ok_retries():
    seq = [_Resp(429, text="slow down"), _Resp(200, {"result": True, "data": {"x": 1}})]
    calls = []

    def get(url, params=None, headers=None, timeout=None):
        calls.append(url)
        return seq.pop(0)

    js = _with_patched(get, lambda: lh.api_get("/p", {}))
    assert js["data"] == {"x": 1} and len(calls) == 2


def test_http_retries_exhausted_reports_last_error():
    def get(url, params=None, headers=None, timeout=None):
        return _Resp(503, text="upstream down")

    try:
        _with_patched(get, lambda: lh.api_get("/p", {}, retries=2))
    except lh.ApiError as e:
        assert "重試 2 次" in str(e) and "503" in str(e), str(e)
    else:
        raise AssertionError("重試用盡應拋 ApiError")


def test_http_4xx_does_not_retry():
    calls = []

    def get(url, params=None, headers=None, timeout=None):
        calls.append(url)
        return _Resp(404, text="not found")

    try:
        _with_patched(get, lambda: lh.api_get("/nope", {}))
    except lh.ApiError as e:
        assert "404" in str(e) and len(calls) == 1
    else:
        raise AssertionError("404 應直接拋")


def test_http_ssl_error_explains_ca_bundle_without_builtin_path():
    def get(url, params=None, headers=None, timeout=None):
        raise lh.requests.exceptions.SSLError("certificate verify failed")

    try:
        _with_patched(get, lambda: lh.api_get("/p", {}))
    except lh.ApiError as e:
        s = str(e)
        assert "CA 憑證" in s and "REQUESTS_CA_BUNDLE" in s, s
    else:
        raise AssertionError("SSL 錯誤應拋 ApiError")


def test_http_connection_error_retries_then_raises():
    calls = []

    def get(url, params=None, headers=None, timeout=None):
        calls.append(url)
        raise lh.requests.exceptions.ConnectionError("boom")

    try:
        _with_patched(get, lambda: lh.api_get("/p", {}, retries=3))
    except lh.ApiError as e:
        assert len(calls) == 3 and "boom" in str(e)
    else:
        raise AssertionError("連線錯誤重試用盡應拋 ApiError")


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
