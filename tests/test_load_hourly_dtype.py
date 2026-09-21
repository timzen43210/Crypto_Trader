# -*- coding: utf-8 -*-
"""
TASK-092 (F1) 驗收測試 — fetch_klines_raw() / load_hourly() 空表 concat 的 dtype 污染。

全程離線：monkeypatch pionex_backtest.api_get，不打任何外部 API、不寫入 pionex_cache/。
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pionex_backtest as pb

BAR = pb.HOUR_MS  # CONFIG["INTERVAL"] 預設 "60M"
SYMBOL = "TESTUSDT"


def _empty_klines_api_get(path, params, retries=5):
    """模擬派網回應：這個時間窗一根 K 棒都沒有。"""
    return {"data": {"klines": []}}


def _new_klines_api_get_factory(rows):
    """第一次呼叫回傳給定的 rows，之後（翻頁到底）一律回傳空，避免無窮迴圈。"""
    calls = {"n": 0}

    def _api_get(path, params, retries=5):
        if calls["n"] == 0:
            calls["n"] += 1
            return {"data": {"klines": rows}}
        return {"data": {"klines": []}}

    return _api_get


def _raw_rows(times):
    # 刻意用有小數點的字串（真實 K 棒價格幾乎不可能是整數），
    # 避免 pd.to_numeric 把欄位推斷成 int64 而誤判成功。
    return [
        {"time": t, "open": f"{100 + i}.5", "high": f"{101 + i}.5", "low": f"{99 + i}.5",
         "close": f"{100 + i}.75", "volume": f"{10 + i}.5"}
        for i, t in enumerate(times)
    ]


def _make_cached_frame(n=50, base_ms=1_700_000_000_000):
    base_ms -= base_ms % BAR
    times = base_ms + np.arange(n, dtype="int64") * BAR
    return pd.DataFrame({
        "time": times.astype("int64"),
        "open": (100 + np.arange(n)).astype("float64"),
        "high": (101 + np.arange(n)).astype("float64"),
        "low": (99 + np.arange(n)).astype("float64"),
        "close": (100.5 + np.arange(n)).astype("float64"),
        "volume": (10 + np.arange(n)).astype("float64"),
    })


# ============================== AC-1 ==============================
def test_fetch_klines_raw_empty_has_correct_dtypes(monkeypatch):
    monkeypatch.setattr(pb, "api_get", _empty_klines_api_get)

    df = pb.fetch_klines_raw(SYMBOL, "60M", end_ms=1_700_000_000_000, stop_ms=0)

    assert df.empty
    assert list(df.columns) == ["time", "open", "high", "low", "close", "volume"]
    assert df["time"].dtype == np.int64, df.dtypes.to_dict()
    for c in ("open", "high", "low", "close", "volume"):
        assert df[c].dtype == np.float64, df.dtypes.to_dict()


# ============================== AC-2 ==============================
def test_load_hourly_dtype_survives_empty_concat(tmp_path, monkeypatch):
    """核心案例：本機有快取、這次抓不到任何新 K 棒 → concat 後 dtype 不得污染。"""
    cached = _make_cached_frame(n=50)
    monkeypatch.setitem(pb.CONFIG, "CACHE_DIR", str(tmp_path))
    cache_path = tmp_path / f"{SYMBOL}_60M.csv"
    cached.to_csv(cache_path, index=False)

    monkeypatch.setattr(pb, "api_get", _empty_klines_api_get)  # 模擬「抓不到新K棒」

    start_ms = int(cached["time"].min())
    now_ms = int(cached["time"].max()) + BAR  # 讓最後一根算「已收完」

    result = pb.load_hourly(SYMBOL, start_ms, now_ms)

    dtypes = result.dtypes.to_dict()
    assert result["time"].dtype == np.int64, f"time dtype 退化: {dtypes}"
    for c in ("open", "high", "low", "close", "volume"):
        assert result[c].dtype == np.float64, f"{c} dtype 退化: {dtypes}"

    expected = cached.reset_index(drop=True)
    actual = result[["time", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected)


# ============================== AC-3 ==============================
def test_load_hourly_with_cache_and_new_data(tmp_path, monkeypatch):
    """回歸：有快取 + 這次抓得到新 K 棒 → 行為不變。"""
    cached = _make_cached_frame(n=20)
    monkeypatch.setitem(pb.CONFIG, "CACHE_DIR", str(tmp_path))
    cache_path = tmp_path / f"{SYMBOL}_60M.csv"
    cached.to_csv(cache_path, index=False)

    new_start = int(cached["time"].max()) + BAR
    new_times = [new_start + i * BAR for i in range(5)]
    monkeypatch.setattr(pb, "api_get", _new_klines_api_get_factory(_raw_rows(new_times)))

    start_ms = int(cached["time"].min())
    now_ms = new_times[-1] + BAR

    result = pb.load_hourly(SYMBOL, start_ms, now_ms)

    assert result["time"].dtype == np.int64
    for c in ("open", "high", "low", "close", "volume"):
        assert result[c].dtype == np.float64

    expected_times = list(cached["time"]) + new_times
    assert result["time"].tolist() == expected_times
    assert len(result) == 25
    # 舊資料段落數值應與快取一致
    old = result[result["time"].isin(cached["time"])].reset_index(drop=True)
    pd.testing.assert_frame_equal(
        old[["time", "open", "high", "low", "close", "volume"]],
        cached.reset_index(drop=True),
    )
    # 新資料段落數值應等於 API 回傳（float 化後）
    new_part = result[result["time"].isin(new_times)].reset_index(drop=True)
    assert new_part["open"].tolist() == [100.5, 101.5, 102.5, 103.5, 104.5]
    assert new_part["volume"].tolist() == [10.5, 11.5, 12.5, 13.5, 14.5]


def test_load_hourly_no_cache_with_new_data(tmp_path, monkeypatch):
    """回歸：無快取 + 這次抓得到新 K 棒 → 行為不變。"""
    monkeypatch.setitem(pb.CONFIG, "CACHE_DIR", str(tmp_path))
    new_times = [1_700_000_000_000 + i * BAR for i in range(10)]
    monkeypatch.setattr(pb, "api_get", _new_klines_api_get_factory(_raw_rows(new_times)))

    start_ms = new_times[0]
    now_ms = new_times[-1] + BAR

    result = pb.load_hourly(SYMBOL, start_ms, now_ms)

    assert result["time"].dtype == np.int64
    for c in ("open", "high", "low", "close", "volume"):
        assert result[c].dtype == np.float64
    assert len(result) == 10
    assert result["time"].tolist() == new_times
