# -*- coding: utf-8 -*-
"""
B2 (config) 驗收測試 — 三層分離真的分開了嗎。

  第一層  策略參數只有 strategy/s4_signal.py 一份來源，live.config 只轉發（改那邊這邊跟著變）
  第二層  執行參數的值在 live.config，live 的其他模組從它取，不是各自寫一份
  第三層  密鑰只從環境變數讀：沒設也能 import、查得到「有沒有設」而不洩漏值、
          真的要用又沒設時拋出說得出該做什麼的例外

全程離線，不打任何 API、不讀寫任何檔案。動到的環境變數都會還原。
不依賴 pytest：直接 `python tests/test_config.py` 會逐一跑完並印結果。
"""
import importlib
import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from live import config  # noqa: E402


def _without_env(names, fn):
    """把這些環境變數暫時拿掉，跑 fn 後原樣還原。"""
    saved = {n: os.environ.pop(n, None) for n in names}
    try:
        return fn()
    finally:
        for n, v in saved.items():
            if v is not None:
                os.environ[n] = v


def _with_env(name, value, fn):
    """暫時把某個環境變數設成 value，跑 fn 後原樣還原。"""
    saved = os.environ.get(name)
    os.environ[name] = value
    try:
        return fn()
    finally:
        if saved is None:
            del os.environ[name]
        else:
            os.environ[name] = saved


# ============== 第一層：策略參數是引用，不是複製 ==============
def test_strategy_params_match_s4_signal():
    from strategy import s4_signal
    assert config.strategy_params() == s4_signal.DEFAULT_PARAMS
    assert set(config.strategy_params()) == set(s4_signal.PARAM_KEYS)


def test_strategy_params_follow_s4_signal_when_it_changes():
    """改 s4_signal.DEFAULT_PARAMS，設定層拿到的就跟著變 —— 證明不是在 live/ 抄了一份數值。"""
    from strategy import s4_signal
    key = "MIN_RET_2H"
    original = s4_signal.DEFAULT_PARAMS[key]
    sentinel = original + 0.4321
    s4_signal.DEFAULT_PARAMS[key] = sentinel
    try:
        assert config.strategy_params()[key] == sentinel, "設定層自己留了一份數值，沒有跟著來源走"
    finally:
        s4_signal.DEFAULT_PARAMS[key] = original
    assert config.strategy_params()[key] == original


def test_strategy_params_returns_a_copy():
    """拿到的是淺拷貝：呼叫端亂改不會汙染策略端那一份。"""
    from strategy import s4_signal
    params = config.strategy_params()
    params["MIN_RET_2H"] = 999
    assert s4_signal.DEFAULT_PARAMS["MIN_RET_2H"] != 999


# ============== 第二層：執行參數的單一來源 ==============
def test_live_modules_take_execution_params_from_config():
    from live import market_static, pionex_api
    assert market_static.STALE_SECONDS == config.MARKET_STATIC_STALE_SECONDS
    sig = inspect.signature(pionex_api.api_get).parameters
    assert sig["retries"].default == config.HTTP_RETRIES
    assert sig["timeout"].default == config.HTTP_TIMEOUT_SECONDS


def test_execution_params_reports_actual_values():
    params = config.execution_params()
    assert params["PIONEX_BASE_URL"] == config.PIONEX_BASE_URL
    assert params["HTTP_TIMEOUT_SECONDS"] == config.HTTP_TIMEOUT_SECONDS
    assert params["HTTP_RETRIES"] == config.HTTP_RETRIES
    assert params["MARKET_STATIC_STALE_SECONDS"] == config.MARKET_STATIC_STALE_SECONDS


# ============== 第三層：密鑰 ==============
def test_import_config_survives_machine_without_any_secret():
    """完全沒設任何密鑰的機器上，import live.config 不可以失敗（A1 / A0 都不用 TG）。"""
    _without_env(list(config.SECRET_ENV_VARS), lambda: importlib.reload(config))
    assert config.SECRET_ENV_VARS, "reload 後常數還在"


def test_secret_is_set_answers_without_the_value():
    env = config.TG_BOT_TOKEN_ENV
    assert _with_env(env, "123456:AA-fake-token-for-test", lambda: config.secret_is_set(env)) is True
    assert _with_env(env, "   ", lambda: config.secret_is_set(env)) is False   # 空字串 / 空白等於沒設
    assert _without_env([env], lambda: config.secret_is_set(env)) is False


def test_require_secret_returns_the_value_when_set():
    env = config.TG_CHANNEL_ID_ENV
    assert _with_env(env, " -1001234567890 ", lambda: config.require_secret(env)) == "-1001234567890"


def test_require_secret_fails_late_with_actionable_message():
    env = config.TG_BOT_TOKEN_ENV

    def go():
        try:
            config.require_secret(env)
        except config.MissingSecretError as e:
            return str(e)
        raise AssertionError("缺環境變數卻沒拋 MissingSecretError")

    msg = _without_env([env], go)
    assert env in msg, msg                      # 講得出缺的是哪一個
    assert "環境變數" in msg, msg               # 講得出該去哪裡設
    assert "export" in msg, msg


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
