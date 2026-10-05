# -*- coding: utf-8 -*-
"""
A6（TASK-023）驗收測試 — 實盤標的池排除股票／ETF／商品代幣＋新上架追蹤。

  AC-1  strategy.universe 的分類（離線）：AAPLX、VSHX → 股票類；CNLX、AVAX、DYDX → 加密（例外名單）；
        XAU、WTI → 股票類（NON_CRYPTO）；BTC、NIGHT → 加密；USDC、PAXG → 強掛勾；enable: false → 已停用；
        非 USDT 計價 → 不列出；龙虾_USDT_PERP 用 baseCurrency（CNLX）判斷；沒有 baseCurrency 退回 symbol 第一段；
        理由說得出命中的是哪一條規則
  AC-2  與 dry run 規則一致：tests/fixtures/pionex_perp_symbols_20261004.json（laptop 2026-10-04 抓一次的真實清單）
        逐筆比對 pionex_backtest.classify：「可交易與否」只有 CNLX 不同、類別對應一致；名單逐項相同（CRYPTO_X_WHITELIST
        多 CNLX）；合成的邊界 base 也逐一比對
  AC-4  標的池日誌（live.signal_feed.MarketUniverse）：第一次載入一行總結；集合沒變的刷新不記；有變只記差異
  AC-5  新上架追蹤（live.universe_seen.ListingTracker 掛在 MarketUniverse 上，假時鐘）：建檔不通知；多 2 個 → 通知恰好
        一次；同樣的清單不通知；重啟（新行程、同一個資料庫）不通知；下架 / 重新上架不通知；寫入失敗 → ERROR、不通知、
        A1 照常，修好後下一次刷新通知恰好一次；回呼拋例外 → ERROR、A1 照常；不帶 --push-tg 照樣建檔與記日誌
  AC-7  A3 不受影響：開著的部位的幣不在標的池 → 照常監控、觸價出場、出場事件照常發布；A3 沒有讀任何標的池查詢
  其他  strategy/universe.py 只用標準庫（全新直譯器 import 不拉進 live / pandas / numpy）；live/universe_seen.py 的命名
        與相依；live/ 的新檔不 import pionex_* / research

AC-3（market_static 的新查詢、MarketUniverse、A1 / A5 / 對帳拿到的標的池）在 tests/test_market_static.py 與
tests/test_signal_feed.py；AC-6（A4 的 🆕 / 🟢 / 💓）在 tests/test_ops_alert.py。

全程離線：socket 籠子（runner 記錄任何連網企圖）、假時鐘（不真的 sleep），資料庫一律開在暫存目錄；runner 另外確認
沒有在真正的 runtime/ 留下東西。測試可以 import pionex_*（AC-2 要拿 dry run 真正的規則比對）。
不依賴 pytest：直接 `python tests/test_universe.py`。
"""
import ast
import contextlib
import importlib.util
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, TESTS_DIR)

import test_notional_tracker as tnt  # noqa: E402  —— A3 的 Harness（假交易所、假時鐘、暫存資料庫）
import test_signal_feed as tsf  # noqa: E402  —— 假時鐘、FakeMarket、feed_with_market、socket 籠子、日誌擷取

import pionex_backtest as pb  # noqa: E402  —— AC-2 的對照組（測試可以 import pionex_*）
from live import config, market_static, paths, signal_feed, universe_seen  # noqa: E402
from strategy import universe as U  # noqa: E402

FIXTURE = os.path.join(TESTS_DIR, "fixtures", "pionex_perp_symbols_20261004.json")
LOBSTER = tsf.LOBSTER
capture_logs = tsf.capture_logs
SMALL_SPECS = tsf.SMALL_SPECS

# dry run 的理由字串 → strategy.universe 的類別代碼（AC-2「類別對應一致」）
DRY_RUN_CATEGORY = {
    "": U.CATEGORY_CRYPTO,
    "交易對已停用": U.CATEGORY_DISABLED,
    "強掛勾/穩定幣/包裝幣": U.CATEGORY_PEGGED,
    "槓桿代幣": U.CATEGORY_LEVERAGED,
    "股票/ETF/商品代幣（依名稱推定，可調整 CRYPTO_X_WHITELIST）": U.CATEGORY_STOCK,
    "手動排除": U.CATEGORY_MANUAL,
}


def _spec(base, quote="USDT", symbol=None, **extra):
    spec = {"symbol": symbol or "%s_%s_PERP" % (base, quote), "baseCurrency": base, "quoteCurrency": quote}
    spec.update(extra)
    return spec


def _fixture_symbols():
    with open(FIXTURE, encoding="utf-8") as f:
        return json.load(f)["symbols"]


# ============================== AC-1：分類規則 ==============================
def test_ac1_required_bases():
    cases = {
        "AAPLX": (U.CATEGORY_STOCK, "名稱 X 結尾"),
        "VSHX": (U.CATEGORY_STOCK, "名稱 X 結尾"),
        "CNLX": (U.CATEGORY_CRYPTO, "在 CRYPTO_X_WHITELIST（例外納入）"),
        "AVAX": (U.CATEGORY_CRYPTO, "在 CRYPTO_X_WHITELIST（例外納入）"),
        "DYDX": (U.CATEGORY_CRYPTO, "在 CRYPTO_X_WHITELIST（例外納入）"),
        "XAU": (U.CATEGORY_STOCK, "在 NON_CRYPTO"),
        "WTI": (U.CATEGORY_STOCK, "在 NON_CRYPTO"),
        "BTC": (U.CATEGORY_CRYPTO, "沒有命中任何排除規則"),
        "NIGHT": (U.CATEGORY_CRYPTO, "沒有命中任何排除規則"),
        "USDC": (U.CATEGORY_PEGGED, "在 PEGGED"),
        "PAXG": (U.CATEGORY_PEGGED, "在 PEGGED"),
    }
    for base, (category, rule) in cases.items():
        c = U.classify(_spec(base))
        assert c is not None, base
        assert (c.category, c.tradable) == (category, category == U.CATEGORY_CRYPTO), (base, c)
        assert c.base == base and c.symbol == "%s_USDT_PERP" % base, c
        assert c.reason.startswith(U.CATEGORY_LABELS[category] + "：") and rule in c.reason, (base, c.reason)
        assert c.verdict == ("納入" if c.tradable else "排除"), c
    # 股票類的三種規則、強掛勾的兩種、槓桿：理由都要說出命中哪一條
    assert "在 STOCK_TICKERS" in U.classify(_spec("TSLA")).reason
    assert "去掉結尾 ON 後的 AAPL 在 STOCK_TICKERS" in U.classify(_spec("AAPLON")).reason
    assert "去掉結尾 B 後的 NVDA 在 STOCK_TICKERS" in U.classify(_spec("NVDAB")).reason
    assert "STABLE_RE" in U.classify(_spec("USDQ")).reason and U.classify(_spec("USDQ")).category == U.CATEGORY_PEGGED
    lev = U.classify(_spec("BTC3L"))
    assert lev.category == U.CATEGORY_LEVERAGED and not lev.tradable and "LEVERAGED_RE" in lev.reason, lev


def test_ac1_disabled_and_non_usdt():
    off = U.classify(_spec("BTC", enable=False))
    assert off.category == U.CATEGORY_DISABLED and not off.tradable and "enable=false" in off.reason, off
    assert U.classify(_spec("USDC", enable=False)).category == U.CATEGORY_DISABLED        # 已停用最先判斷
    assert U.classify(_spec("BTC", enable=True)).tradable                                # 只有 False 才算停用
    assert U.classify(_spec("ETH", quote="BTC")) is None                                  # 非 USDT → 不列出
    assert U.classify({"symbol": "ETH_BTC_PERP"}) is None                                 # 沒有 quoteCurrency 看 symbol
    assert U.classify(_spec("ETH", quote="BTC"), quote="BTC").tradable                    # 指定計價幣
    assert U.classify({"symbol": "WEIRD"}) is None                                        # symbol 沒有計價段 → 不列出


def test_ac1_base_comes_from_base_currency_not_symbol():
    c = U.classify({"symbol": LOBSTER, "name": "CNLX USDT PERPETUAL", "baseCurrency": "CNLX",
                    "quoteCurrency": "USDT", "status": "TRADING"})
    assert (c.base, c.category, c.tradable) == ("CNLX", U.CATEGORY_CRYPTO, True), c
    assert c.display_symbol == LOBSTER + "（CNLX）", c.display_symbol
    assert "CRYPTO_X_WHITELIST" in c.reason
    # baseCurrency 與 symbol 第一段不同時一律看 baseCurrency（兩個方向都驗）
    assert U.classify({"symbol": "BTC_USDT_PERP", "baseCurrency": "AAPLX", "quoteCurrency": "USDT"}).category == \
        U.CATEGORY_STOCK
    assert U.classify({"symbol": "AAPLX_USDT_PERP", "baseCurrency": "BTC", "quoteCurrency": "USDT"}).tradable
    # 沒有 baseCurrency（或空字串）→ 退回 symbol 第一段，大寫
    for spec in ({"symbol": "aaplx_USDT_PERP", "quoteCurrency": "USDT"},
                 {"symbol": "AAPLX_USDT_PERP", "baseCurrency": "", "quoteCurrency": "USDT"}):
        c = U.classify(spec)
        assert (c.base, c.category) == ("AAPLX", U.CATEGORY_STOCK), (spec, c)
    c = U.classify({"symbol": "BTC_USDT_PERP"})
    assert (c.base, c.tradable) == ("BTC", True), c
    assert U.classify(_spec("BTC")).display_symbol == "BTC_USDT_PERP"                     # 相同就不加括號


def test_ac1_manual_exclude_and_lists_are_immutable():
    saved = U.MANUAL_EXCLUDE
    U.MANUAL_EXCLUDE = frozenset({"doge"})
    try:
        c = U.classify(_spec("DOGE"))
        assert (c.category, c.tradable) == (U.CATEGORY_MANUAL, False) and "MANUAL_EXCLUDE" in c.reason, c
    finally:
        U.MANUAL_EXCLUDE = saved
    assert U.MANUAL_EXCLUDE == frozenset()                                                # 預設空
    for name in ("PEGGED", "STOCK_TICKERS", "CRYPTO_X_WHITELIST", "NON_CRYPTO", "MANUAL_EXCLUDE"):
        assert isinstance(getattr(U, name), frozenset), name                              # 執行中改不動


def test_ac1_summarize_and_text():
    classes = [U.classify(_spec(b)) for b in ("BTC", "ETH", "AAPLX", "VSHX", "FRAX", "BTC3L")]
    s = U.summarize(classes)
    assert s == {"total": 6, "included": 2, "excluded": 4,
                 "by_category": {"stock": 2, "pegged": 1, "leveraged": 1}}, s
    assert list(s["by_category"]) == ["stock", "pegged", "leveraged"]                     # 依 EXCLUDED_CATEGORIES 的順序
    assert U.summary_text(s) == "納入 2、排除 4（股票／ETF／商品 2、強掛勾／穩定幣／包裝幣 1、槓桿代幣 1）"
    assert U.summary_text(U.summarize([U.classify(_spec("BTC"))])) == "納入 1、排除 0"


# ============================== AC-2：與 dry run 規則一致 ==============================
def test_ac2_fixture_is_what_the_prd_asks_for():
    assert re.search(r"_20\d{6}\.json$", FIXTURE), FIXTURE                                # 檔名帶日期
    with open(FIXTURE, "rb") as f:
        raw = f.read()
    assert raw.count(b"\r\r\n") == 0 and raw.count(b"\r\n") == 0, "fixture 要是 LF"
    symbols = json.loads(raw.decode("utf-8"))["symbols"]
    assert len(symbols) >= 500, len(symbols)
    keep = {"symbol", "name", "baseCurrency", "quoteCurrency", "status", "enable"}
    assert all(set(s) <= keep for s in symbols), [sorted(set(s) - keep) for s in symbols if set(s) - keep][:3]
    assert any(s.get("baseCurrency") == "CNLX" for s in symbols)
    cats = {c.category for c in (U.classify(s) for s in symbols) if c is not None}
    assert {U.CATEGORY_STOCK, U.CATEGORY_PEGGED, U.CATEGORY_CRYPTO} <= cats, cats
    assert any(U.classify(s) is None for s in symbols), "fixture 裡要有非 USDT 計價的，才驗得到「不列出」"


def _compare_with_dry_run(specs):
    """回傳 (可交易與否不同的 base, 類別不同的 (base, dry run 理由, 類別), 雙方都列出的筆數)。"""
    differ, category_mismatch, listed = [], [], 0
    for s in specs:
        base, reason = pb.classify(s)
        c = U.classify(s)
        assert (reason is None) == (c is None), (s, reason, c)              # 「不列出」一致
        if c is None:
            continue
        listed += 1
        assert c.base == base, (s, base, c.base)
        if (reason == "") != c.tradable:
            differ.append(c.base)
        elif DRY_RUN_CATEGORY[reason] != c.category:
            category_mismatch.append((c.base, reason, c.category))
    return differ, category_mismatch, listed


def test_ac2_tradability_differs_from_dry_run_only_on_cnlx():
    differ, category_mismatch, listed = _compare_with_dry_run(_fixture_symbols())
    assert differ == ["CNLX"], differ
    assert category_mismatch == [], category_mismatch
    assert listed >= 500, listed


def test_ac2_synthetic_edge_cases_agree_with_dry_run():
    """fixture 裡沒有的類別（槓桿、已停用、STABLE_RE、各種結尾）也逐一比對 dry run 真正的 classify。"""
    bases = ["BTC", "AAPLX", "TSLA", "AAPLON", "NVDAB", "BRKB", "TSLASTOCK", "XAU", "WTI", "BRENT", "KORU", "SKHY",
             "AVAX", "DYDX", "APEX", "FLUX", "IOTX", "BEAMX", "POLYX", "ZETAX", "SAFEX", "APX", "ABX", "XX", "X",
             "USDC", "USDQ", "ZUSD", "EURQ", "EUR", "PAXG", "XAUT", "WBTC", "BTC3L", "ETH5S", "1000PEPE", "1INCH",
             "SPYX", "QQQ", "QQQX", "COINON", "HOODX", "NIGHT", "MSTRB", "AMDX", "IBMX", "B", "ON", "STOCK"]
    specs = [_spec(b) for b in bases]
    specs += [_spec("BTC", enable=False), _spec("AAPLX", enable=False), _spec("ETH", quote="BTC"),
              {"symbol": "lower_USDT_PERP", "quoteCurrency": "USDT"}, {"symbol": "ETH_USDT_PERP"},
              _spec("CNLX")]
    differ, category_mismatch, _ = _compare_with_dry_run(specs)
    assert differ == ["CNLX"], differ
    assert category_mismatch == [], category_mismatch


def test_ac2_rule_lists_are_identical_except_cnlx():
    assert set(U.PEGGED) == pb.PEGGED
    assert (U.STABLE_RE.pattern, U.LEVERAGED_RE.pattern) == (pb.STABLE_RE.pattern, pb.LEVERAGED_RE.pattern)
    assert set(U.STOCK_TICKERS) == pb.STOCK_TICKERS
    assert set(U.NON_CRYPTO) == pb.NON_CRYPTO
    assert set(U.CRYPTO_X_WHITELIST) - pb.CRYPTO_X_WHITELIST == {"CNLX"}, "唯一的語意差異是 CNLX"
    assert pb.CRYPTO_X_WHITELIST <= set(U.CRYPTO_X_WHITELIST)
    assert set(U.MANUAL_EXCLUDE) == {b.upper() for b in pb.CONFIG["EXTRA_EXCLUDE"]} == set()
    bases = {U.base_of(s) for s in _fixture_symbols()} | set(pb.STOCK_TICKERS) | pb.NON_CRYPTO | pb.CRYPTO_X_WHITELIST
    differ = sorted(b for b in bases if U.is_stock_token(b) != pb.is_stock_token(b))
    assert differ == ["CNLX"], differ


# ============================== AC-4：標的池日誌 ==============================
def _universe_lines(cap):
    return [m for m in cap.messages(logging.INFO) if m.startswith(("標的池第一次載入", "標的池變動"))]


def test_ac4_first_load_logs_one_summary_line():
    feed = tsf.feed_with_market(SMALL_SPECS)
    with capture_logs() as cap:
        feed.load_universe()
    assert _universe_lines(cap) == [
        "標的池第一次載入：TRADING USDT 共 6 個，納入 3、排除 3（股票／ETF／商品 2、強掛勾／穩定幣／包裝幣 1）；"
        "被排除的 symbol：[股票／ETF／商品 2] AAPLX_USDT_PERP、VSHX_USDT_PERP；[強掛勾／穩定幣／包裝幣 1] FRAX_USDT_PERP"
    ], _universe_lines(cap)


def test_ac4_unchanged_refresh_logs_nothing_and_changes_log_only_the_diff():
    feed = tsf.feed_with_market(SMALL_SPECS)
    with capture_logs():
        feed.load_universe()
    with capture_logs() as cap:
        tsf.make_stale()
        feed.refresh_universe()                       # 刷新成功，但集合沒變
    assert len(feed.market.calls) == 4 and _universe_lines(cap) == [], _universe_lines(cap)
    feed.market.specs = [s for s in SMALL_SPECS if s["symbol"] != "ETH_USDT_PERP"] + [
        {"symbol": "NEW_USDT_PERP", "baseCurrency": "NEW", "quoteCurrency": "USDT", "status": "TRADING"},
        {"symbol": "NEWX_USDT_PERP", "baseCurrency": "NEWX", "quoteCurrency": "USDT", "status": "TRADING"}]
    with capture_logs() as cap:
        tsf.make_stale()
        feed.refresh_universe()
    assert _universe_lines(cap) == [
        "標的池變動（TRADING USDT 共 7 個，納入 3、排除 4）：納入新增 1 個：NEW_USDT_PERP；納入移除 1 個：ETH_USDT_PERP；"
        "排除新增 1 個：NEWX_USDT_PERP（股票／ETF／商品：名稱 X 結尾（長度 ≥ 4、不在 CRYPTO_X_WHITELIST））"
    ], _universe_lines(cap)
    assert "AAPLX" not in _universe_lines(cap)[0], "沒變的部分不寫"
    with capture_logs() as cap:
        tsf.make_stale()
        feed.market.specs = [s for s in feed.market.specs if s["symbol"] != "VSHX_USDT_PERP"]
        feed.refresh_universe()
    assert _universe_lines(cap) == ["標的池變動（TRADING USDT 共 6 個，納入 3、排除 3）：排除移除 1 個：VSHX_USDT_PERP"]
    with capture_logs() as cap:                        # 刷新失敗：沿用舊清單，不記標的池變動
        tsf.make_stale()
        saved = feed.market.specs
        feed.market.specs = []
        feed.refresh_universe()
        feed.market.specs = saved
    assert _universe_lines(cap) == [] and feed.universe() == ["BTC_USDT_PERP", "NEW_USDT_PERP", LOBSTER]


# ============================== AC-5：新上架追蹤 ==============================
class EpochClock:
    def __init__(self, t):
        self.t = float(t)

    def __call__(self):
        return self.t


class Notified:
    """on_new 的替身：記下每一批。raise_exc 設了就拋。"""

    def __init__(self):
        self.batches = []
        self.raise_exc = None

    def __call__(self, listings):
        self.batches.append(list(listings))
        if self.raise_exc is not None:
            raise self.raise_exc


@contextlib.contextmanager
def seen_env():
    """config.UNIVERSE_SEEN_DB_PATH 導到暫存目錄（ListingTracker 的預設路徑在呼叫當下讀它）。"""
    tmp = tempfile.mkdtemp(prefix="a6_seen_")
    saved = config.UNIVERSE_SEEN_DB_PATH
    config.UNIVERSE_SEEN_DB_PATH = os.path.join(tmp, "db", "universe_seen.sqlite3")
    try:
        yield config.UNIVERSE_SEEN_DB_PATH
    finally:
        config.UNIVERSE_SEEN_DB_PATH = saved
        shutil.rmtree(tmp, ignore_errors=True)


T_START = 1_790_146_800.0          # 假時鐘（epoch 秒）
NEW_SPECS = [{"symbol": "NEWX_USDT_PERP", "baseCurrency": "NEWX", "quoteCurrency": "USDT", "status": "TRADING"},
             {"symbol": "NEWCOIN_USDT_PERP", "baseCurrency": "NEWCOIN", "quoteCurrency": "USDT", "status": "TRADING"}]


def _tracked_feed(notified, clock, path=None):
    tracker = universe_seen.ListingTracker(path=path, on_new=notified, clock=clock)
    return tsf.feed_with_market(SMALL_SPECS, listings=tracker), tracker


def _refresh(feed, clock, dt=3600):
    clock.t += dt
    tsf.make_stale()
    feed._after_bar()                  # A1 主迴圈每根 K 棒之後走的那條路


def test_ac5_bootstrap_writes_everything_as_initial_and_does_not_notify():
    with seen_env() as path:
        notified, clock = Notified(), EpochClock(T_START)
        feed, tracker = _tracked_feed(notified, clock)
        assert not os.path.exists(path)
        with capture_logs() as cap:
            feed.load_universe()
        rows = universe_seen.read_all(path)
        trading_usdt = sorted(s["symbol"] for s in SMALL_SPECS if s["status"] == "TRADING"
                              and s["quoteCurrency"] == "USDT")
        assert [r.symbol for r in rows] == trading_usdt, "建檔要寫入過濾之前的全部（含被排除的）"
        assert all(r.initial and r.first_seen_ms == int(T_START * 1000) for r in rows), rows
        assert {r.symbol: r.tradable for r in rows}["AAPLX_USDT_PERP"] is False
        assert notified.batches == []
        infos = [m for m in cap.messages(logging.INFO) if m.startswith("新上架追蹤：建檔")]
        assert infos == ["新上架追蹤：建檔 %s，寫入目前 TRADING USDT 共 6 個（初始列，不發新上架通知）"
                         % os.path.abspath(path)], infos
        assert not [m for m in cap.messages(logging.INFO) if m.startswith("新上架 ")]


def test_ac5_new_symbols_notify_exactly_once_and_restart_delist_relist_do_not():
    with seen_env() as path:
        notified, clock = Notified(), EpochClock(T_START)
        feed, tracker = _tracked_feed(notified, clock)
        with capture_logs():
            feed.load_universe()
        feed.market.specs = SMALL_SPECS + NEW_SPECS                 # 多 2 個：一個 X 結尾、一個一般幣
        with capture_logs() as cap:
            _refresh(feed, clock)
        assert len(notified.batches) == 1 and len(notified.batches[0]) == 2, notified.batches
        got = {x.symbol: x for x in notified.batches[0]}
        assert (got["NEWX_USDT_PERP"].tradable, got["NEWX_USDT_PERP"].category) == (False, U.CATEGORY_STOCK)
        assert (got["NEWCOIN_USDT_PERP"].tradable, got["NEWCOIN_USDT_PERP"].category) == (True, U.CATEGORY_CRYPTO)
        assert "名稱 X 結尾" in got["NEWX_USDT_PERP"].reason
        assert all(not x.initial and x.first_seen_ms == int(clock.t * 1000) for x in got.values())
        assert feed.universe() == ["BTC_USDT_PERP", "ETH_USDT_PERP", "NEWCOIN_USDT_PERP", LOBSTER]
        infos = [m for m in cap.messages(logging.INFO) if m.startswith("新上架 ")]
        assert infos == ["新上架 2 個（納入 1、排除 1）：NEWCOIN_USDT_PERP：納入（加密：沒有命中任何排除規則）；"
                         "NEWX_USDT_PERP：排除（股票／ETF／商品：名稱 X 結尾（長度 ≥ 4、不在 CRYPTO_X_WHITELIST））"], infos
        stored = {r.symbol: r for r in universe_seen.read_all(path)}
        assert not stored["NEWX_USDT_PERP"].initial and stored["NEWX_USDT_PERP"].reason == got["NEWX_USDT_PERP"].reason

        with capture_logs():
            _refresh(feed, clock)                                   # 同樣的清單再刷新
        assert len(notified.batches) == 1

        # 重啟（新行程、同一個資料庫）：子行程用同一份清單跑一次 observe
        result = _child_observe(path, SMALL_SPECS + NEW_SPECS)
        assert result == {"returned": [], "notified": 0, "new": 0, "bootstrapped": 0, "db_errors": 0}, result
        # 同一個行程裡的「重啟」（新的追蹤器 + 重新載入 market_static）也一樣
        notified2 = Notified()
        feed2, _ = _tracked_feed(notified2, EpochClock(clock.t + 60))
        feed2.market.specs = SMALL_SPECS + NEW_SPECS
        with capture_logs():
            feed2.load_universe()
        assert notified2.batches == []

        feed.market.specs = [s for s in SMALL_SPECS + NEW_SPECS if s["symbol"] != "NEWCOIN_USDT_PERP"]
        with capture_logs():
            _refresh(feed, clock)                                   # 下架一個
        assert len(notified.batches) == 1 and "NEWCOIN_USDT_PERP" not in feed.universe()
        assert "NEWCOIN_USDT_PERP" in {r.symbol for r in universe_seen.read_all(path)}, "下架不刪列"
        feed.market.specs = SMALL_SPECS + NEW_SPECS
        with capture_logs():
            _refresh(feed, clock)                                   # 重新上架
        assert len(notified.batches) == 1, "下架後重新上架不算新上架"
        assert "NEWCOIN_USDT_PERP" in feed.universe()


def _child_observe(path, specs):
    """新行程：同一個資料庫、同一份清單跑一次 ListingTracker.observe()，回傳結果摘要。"""
    code = (
        "import json, sys\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "from live import universe_seen\n"
        "from strategy import universe as U\n"
        "specs = json.load(open(sys.argv[3], encoding='utf-8'))\n"
        "classes = {s['symbol']: U.classify(s) for s in specs if s['status'] == 'TRADING'}\n"
        "classes = {k: v for k, v in classes.items() if v is not None}\n"
        "got = []\n"
        "t = universe_seen.ListingTracker(path=sys.argv[2], on_new=got.append, clock=lambda: 1.8e9)\n"
        "res = t.observe(classes)\n"
        "print(json.dumps({'returned': [r.symbol for r in res], 'notified': len(got), 'new': t.stats['new'],\n"
        "                  'bootstrapped': t.stats['bootstrapped'], 'db_errors': t.stats['db_errors']}))\n")
    tmp = tempfile.mkdtemp(prefix="a6_child_")
    try:
        spec_path = os.path.join(tmp, "specs.json")
        with open(spec_path, "w", encoding="utf-8") as f:
            json.dump(specs, f, ensure_ascii=False)
        r = subprocess.run([sys.executable, "-c", code, REPO_ROOT, path, spec_path], cwd=REPO_ROOT,
                           capture_output=True, timeout=120)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    assert r.returncode == 0, r.stderr.decode("utf-8", "replace")[-1500:]
    return json.loads(r.stdout.decode("ascii").strip().splitlines()[-1])


class _FailingInsert:
    def __init__(self):
        self.calls = 0

    def __call__(self, conn, rows):
        self.calls += 1
        raise sqlite3.OperationalError("disk I/O error（測試注入）")


def test_ac5_write_failure_logs_error_does_not_notify_and_retries_on_the_next_refresh():
    with seen_env() as path:
        notified, clock = Notified(), EpochClock(T_START)
        feed, tracker = _tracked_feed(notified, clock)
        with capture_logs():
            feed.load_universe()
        feed.market.specs = SMALL_SPECS + NEW_SPECS
        tracker._insert = failing = _FailingInsert()
        with capture_logs() as cap:
            _refresh(feed, clock)                                   # A1 的 _after_bar：不可以拋出來
        assert failing.calls == 1 and notified.batches == []
        errors = [r for r in cap.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and errors[0].name == "live.universe_seen", [r.getMessage() for r in errors]
        msg = errors[0].getMessage()
        assert "寫入" in msg and "OperationalError" in msg and "這一批 2 個新上架不通知" in msg, msg
        assert "NEWCOIN_USDT_PERP" in feed.universe(), "A1 照常：標的池照樣更新"
        assert {r.symbol for r in universe_seen.read_all(path)} == {s["symbol"] for s in SMALL_SPECS
                                                                   if s["status"] == "TRADING"
                                                                   and s["quoteCurrency"] == "USDT"}
        del tracker._insert                                           # 修好
        with capture_logs() as cap:
            _refresh(feed, clock)
        assert len(notified.batches) == 1 and sorted(x.symbol for x in notified.batches[0]) == \
            ["NEWCOIN_USDT_PERP", "NEWX_USDT_PERP"], notified.batches
        assert not [r for r in cap.records if r.levelno >= logging.ERROR]
        with capture_logs():
            _refresh(feed, clock)
            _refresh(feed, clock)
        assert len(notified.batches) == 1, "已寫入的不可以每小時重複通知"


def test_ac5_unopenable_database_logs_error_and_a1_continues():
    with seen_env() as path:
        os.makedirs(path)                                           # 路徑是目錄：開不了
        notified, clock = Notified(), EpochClock(T_START)
        feed, tracker = _tracked_feed(notified, clock)
        with capture_logs() as cap:
            assert feed.load_universe() == tsf.SMALL_TRADABLE
        errors = [r.getMessage() for r in cap.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and "開不了" in errors[0], errors
        assert notified.batches == [] and tracker.stats["db_errors"] == 1


def test_ac5_wrong_schema_is_refused_without_touching_it():
    with seen_env() as path:
        os.makedirs(os.path.dirname(path))
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE something_else (x)")
        conn.commit()
        conn.close()
        notified, clock = Notified(), EpochClock(T_START)
        feed, tracker = _tracked_feed(notified, clock)
        with capture_logs() as cap:
            feed.load_universe()
        errors = [r.getMessage() for r in cap.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and "UniverseSeenError" in errors[0] and "something_else" in errors[0], errors
        conn = sqlite3.connect(path)
        assert [r[0] for r in conn.execute("SELECT name FROM sqlite_master")] == ["something_else"]
        conn.close()


def test_ac5_callback_exception_is_logged_and_a1_continues():
    with seen_env() as path:
        notified, clock = Notified(), EpochClock(T_START)
        feed, tracker = _tracked_feed(notified, clock)
        with capture_logs():
            feed.load_universe()
        notified.raise_exc = RuntimeError("告警器壞了")
        feed.market.specs = SMALL_SPECS + NEW_SPECS
        with capture_logs() as cap:
            _refresh(feed, clock)
        assert len(notified.batches) == 1
        errors = [r for r in cap.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and "新上架通知的回呼拋出例外" in errors[0].getMessage(), errors
        assert errors[0].exc_info is not None
        assert "NEWCOIN_USDT_PERP" in feed.universe()
        assert {"NEWX_USDT_PERP", "NEWCOIN_USDT_PERP"} <= {r.symbol for r in universe_seen.read_all(path)}
        notified.raise_exc = None
        with capture_logs():
            _refresh(feed, clock)
        assert len(notified.batches) == 1, "已寫入的那一批不再通知"


def test_ac5_without_push_tg_still_bootstraps_and_logs():
    """不帶 --push-tg：on_new 是 None（live.a_channel 的接線見 tests/test_ops_alert.py 的 default_feed 測試）。"""
    with seen_env() as path:
        clock = EpochClock(T_START)
        tracker = universe_seen.ListingTracker(clock=clock)          # on_new=None，路徑取 config（呼叫當下）
        feed = tsf.feed_with_market(SMALL_SPECS, listings=tracker)
        with capture_logs() as cap:
            feed.load_universe()
        assert os.path.isfile(path) and len(universe_seen.read_all(path)) == 6
        assert any(m.startswith("新上架追蹤：建檔") for m in cap.messages(logging.INFO))
        feed.market.specs = SMALL_SPECS + NEW_SPECS[:1]
        with capture_logs() as cap:
            _refresh(feed, clock)
        assert [m for m in cap.messages(logging.INFO) if m.startswith("新上架 ")] == [
            "新上架 1 個（納入 0、排除 1）：NEWX_USDT_PERP：排除（股票／ETF／商品：名稱 X 結尾（長度 ≥ 4、不在 "
            "CRYPTO_X_WHITELIST））"]
        assert tracker.stats["new"] == 1 and tracker.stats["notified"] == 0


def test_ac5_observe_never_raises_even_on_garbage():
    with seen_env():
        tracker = universe_seen.ListingTracker(clock=EpochClock(T_START))
        with capture_logs() as cap:
            assert tracker.observe(object()) == []                    # 不是 mapping
        assert [r.levelno for r in cap.records] == [logging.ERROR]


def test_ac5_tracker_is_only_wired_when_given():
    """觀察用的 python -m live.signal_feed 與既有測試不給 listings：完全不碰追蹤檔。"""
    with seen_env() as path:
        feed = tsf.feed_with_market(SMALL_SPECS)
        with capture_logs():
            feed.load_universe()
        assert not os.path.exists(os.path.dirname(path))


# ============================== AC-7：A3 不受影響 ==============================
def test_ac7_a3_keeps_monitoring_a_position_whose_symbol_left_the_universe():
    sym = "VSHX_USDT_PERP"
    spec = tnt.S4
    p = 100.0
    tp, _sl = tnt._levels(spec, p)
    with tnt.tempdir() as tmp:
        h = tnt.Harness(tmp)
        try:
            # 1) 舊版時開的部位（那時 VSHX 還在標的池）
            h.ex.put_bars(sym, "1M", tnt.flat_bars(tnt.T0, tnt.T0 + spec.main_ms, p))
            h.signal("s4", tnt.T0, p, symbol=sym)
            assert [r["symbol"] for r in h.all_positions() if r["status"] == "open"] == [sym]
            # 2) 部署新版：標的池排除 VSHX
            feed = tsf.feed_with_market(SMALL_SPECS)
            with capture_logs():
                feed.load_universe()
            assert sym not in feed.universe() and sym in market_static.excluded_symbols()
            # 3) A3 之後如果讀了任何標的池查詢，都記下來
            reads = []
            names = ("trading_symbols", "tradable_symbols", "excluded_symbols", "classified_symbols")
            saved = {n: getattr(market_static, n) for n in names}
            saved_universe = signal_feed.SignalFeed.universe

            def spy(name, fn):
                def wrapper(*a, **k):
                    reads.append(name)
                    return fn(*a, **k)
                return wrapper
            for n in names:
                setattr(market_static, n, spy(n, saved[n]))
            signal_feed.SignalFeed.universe = spy("SignalFeed.universe", saved_universe)
            try:
                # 4) 之後的 1 分K：第二根碰止盈
                close = tnt.T0 + spec.main_ms
                bars = [(p, p, p, p), (p, p, tp, tp * 1.001), (p, p, p, p)]
                for i, (o, hi, lo, c) in enumerate(bars):
                    h.ex.put(sym, "1M", close + i * tnt.M1, o, hi, lo, c)
                h.tick_until(close + len(bars) * tnt.M1)
            finally:
                for n in names:
                    setattr(market_static, n, saved[n])
                signal_feed.SignalFeed.universe = saved_universe
            assert reads == [], "A3 讀了標的池：%s" % reads
            closed = h.closed()
            assert len(closed) == 1 and closed[0]["symbol"] == sym, closed
            assert (closed[0]["exit_reason"], closed[0]["exit_price"]) == (config.EXIT_TAKE_PROFIT, tp), closed[0]
            exits = [ev for kind, _sid, ev in h.rec.deduped() if kind == "exit"]
            assert len(exits) == 1 and exits[0].symbol == sym and exits[0].reason == config.EXIT_TAKE_PROFIT, exits
        finally:
            h.close()


def test_ac7_a3_source_does_not_reference_the_universe():
    with open(os.path.join(REPO_ROOT, "live", "notional_tracker.py"), encoding="utf-8") as f:
        tree = ast.parse(f.read())
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
        {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    mods = {a.name for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom)) for a in n.names} | \
        {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    bad = {"market_static", "trading_symbols", "tradable_symbols", "excluded_symbols", "classified_symbols",
           "universe", "MarketUniverse", "universe_seen"}
    assert not (names | mods) & bad, (names | mods) & bad


# ============================== 其他：相依與命名 ==============================
def _top_imports(rel):
    with open(os.path.join(REPO_ROOT, rel), encoding="utf-8") as f:
        tree = ast.parse(f.read())
    tops = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            tops |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, rel
            tops.add(node.module.split(".")[0])
    return tops


def test_strategy_universe_is_stdlib_only():
    assert _top_imports("strategy/universe.py") <= set(sys.stdlib_module_names), _top_imports("strategy/universe.py")
    code = ("import sys; sys.dont_write_bytecode = True; sys.path.insert(0, sys.argv[1]); "
            "import strategy.universe; "
            "bad = sorted(m for m in sys.modules if m.split('.')[0] in "
            "('live', 'pandas', 'numpy', 'requests', 'research', 'openpyxl') or m.startswith('pionex_')); "
            "print(bad)")
    r = subprocess.run([sys.executable, "-c", code, REPO_ROOT], capture_output=True, timeout=60)
    assert r.returncode == 0 and r.stdout.decode().strip() == "[]", (r.stdout, r.stderr[-800:])


def test_universe_seen_module_name_and_dependencies():
    assert "universe_seen" not in sys.stdlib_module_names and importlib.util.find_spec("universe_seen") is None
    assert "universe" not in sys.stdlib_module_names
    assert _top_imports("live/universe_seen.py") <= set(sys.stdlib_module_names) | {"live", "strategy"}
    for rel in ("live/universe_seen.py", "live/market_static.py", "live/signal_feed.py", "live/ops_alert.py",
                "live/a_channel.py", "live/config.py"):
        tops = _top_imports(rel)
        assert not any(t.startswith("pionex_") or t == "research" for t in tops), (rel, tops)


# ============================== runner ==============================
if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    logging.getLogger().addHandler(logging.NullHandler())
    logging.getLogger("live").setLevel(logging.CRITICAL)        # A3 等的 INFO / WARNING 很多；要看的都用 capture_logs
    runtime_existed = os.path.exists(paths.RUNTIME_DIR)
    seen_existed = os.path.exists(config.UNIVERSE_SEEN_DB_PATH)
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    with tsf.socket_cage():
        for name, fn in tests:
            try:
                fn()
                print(f"PASS  {name}")
            except Exception as e:  # noqa: BLE001
                failed += 1
                print(f"FAIL  {name}: {type(e).__name__}: {e}")
    runner_failed = 0
    if tsf._blocked_attempts:
        runner_failed += 1
        print(f"FAIL  <runner>: 有測試企圖連網 {len(tsf._blocked_attempts)} 次：{tsf._blocked_attempts[:3]}")
    else:
        print("PASS  <runner>: 全程沒有任何連網企圖（socket 籠子記錄 0 次）")
    if os.path.exists(paths.RUNTIME_DIR) != runtime_existed or \
            os.path.exists(config.UNIVERSE_SEEN_DB_PATH) != seen_existed:
        runner_failed += 1
        print(f"FAIL  <runner>: 測試在真正的 runtime/ 留下了東西（{paths.RUNTIME_DIR}）")
    print(f"\n{len(tests) - failed} passed, {failed} failed"
          + (f", {runner_failed} runner check(s) failed" if runner_failed else ""))
    sys.exit(1 if failed or runner_failed else 0)
