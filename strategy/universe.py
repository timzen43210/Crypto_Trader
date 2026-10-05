# -*- coding: utf-8 -*-
"""
strategy.universe — 實盤標的池的分類規則（**唯一的一份**）
=========================================================
派網 /api/v1/common/symbols 沒有任何欄位標示股票或加密，股票合約也是 24 小時交易，看行為分不出來
（captain 2026-10-04 實測）。所以標的池只能靠**名稱規則 + 人工覆寫名單**判斷，本模組就是那一套規則：
實盤（live.market_static → A1 / A5 / 背景對帳共用的標的池）只用這一份。dry run 目前仍用
pionex_backtest.classify()（規則從那裡照搬），stage → main 合併時改用本模組；兩份除了 CNLX 之外結果一致，
由 tests/test_universe.py 的 AC-2 用真實的 symbols 清單逐筆比對。

判斷順序（照搬 pionex_backtest.classify / is_stock_token，不重新設計）：
    非 USDT 計價（可指定）     → 不列出（classify() 回 None）
    enable 是 False           → 已停用
    在 PEGGED、或符合 STABLE_RE → 強掛勾／穩定幣／包裝幣
    符合 LEVERAGED_RE         → 槓桿代幣
    股票規則（依序）           → 股票／ETF／商品
        在 NON_CRYPTO → X 結尾且長度 ≥ 4 且不在 CRYPTO_X_WHITELIST → 在 STOCK_TICKERS
        → 去掉結尾 X / ON / B / STOCK 之後在 STOCK_TICKERS
    在 MANUAL_EXCLUDE         → 手動排除
    其他                      → 加密（可交易）
base 取 baseCurrency，沒有才退回 symbol 的第一段（大寫）。**不可以用 symbol 名稱代替 baseCurrency**：
龙虾_USDT_PERP 的 baseCurrency 是 CNLX。

**判錯時改哪一份名單**（改完要 commit 並重新部署，執行中的程式不會自己讀到）：
    加密幣被誤排除（名稱剛好 X 結尾）  → CRYPTO_X_WHITELIST
    股票／ETF／商品沒被排除             → STOCK_TICKERS（個股、ETF 代號）或 NON_CRYPTO（商品、其他非加密）
    其他想排除的                       → MANUAL_EXCLUDE
名單不做成執行期設定檔（不引入設定檔格式）。新上架的合約不論納入或排除，實盤會把判定與理由送到維運聊天
（live.universe_seen、live.ops_alert 的 🆕），判錯就照上面改名單。

只用標準庫：不 import pandas / numpy / live，不打網路、不讀寫檔案。
"""

import re
from dataclasses import dataclass

# 標的池的計價幣。非這個計價的交易對沿用 dry run 的行為：不列出。
QUOTE = "USDT"

# ---- 類別代碼與顯示名稱 ----
CATEGORY_CRYPTO = "crypto"
CATEGORY_STOCK = "stock"
CATEGORY_PEGGED = "pegged"
CATEGORY_LEVERAGED = "leveraged"
CATEGORY_MANUAL = "manual"
CATEGORY_DISABLED = "disabled"
CATEGORIES = (CATEGORY_CRYPTO, CATEGORY_STOCK, CATEGORY_PEGGED, CATEGORY_LEVERAGED, CATEGORY_MANUAL,
              CATEGORY_DISABLED)
EXCLUDED_CATEGORIES = tuple(c for c in CATEGORIES if c != CATEGORY_CRYPTO)
CATEGORY_LABELS = {
    CATEGORY_CRYPTO: "加密",
    CATEGORY_STOCK: "股票／ETF／商品",
    CATEGORY_PEGGED: "強掛勾／穩定幣／包裝幣",
    CATEGORY_LEVERAGED: "槓桿代幣",
    CATEGORY_MANUAL: "手動排除",
    CATEGORY_DISABLED: "已停用",
}
VERDICT_INCLUDED = "納入"
VERDICT_EXCLUDED = "排除"

# ============================== 名單（從 pionex_backtest.py 照搬） ==============================
# ---- 強掛勾 / 包裝 / 穩定幣 ----
PEGGED = frozenset({
    "USDT", "USDC", "FDUSD", "TUSD", "DAI", "USDE", "USDD", "PYUSD", "USDP", "BUSD",
    "USD1", "RLUSD", "USDS", "USDX", "USDXO", "USDB", "GUSD", "LUSD", "FRAX", "SUSD",
    "USD0", "USDF", "BFUSD", "USDG", "EURC", "EURT", "EURI", "AEUR", "EURS",
    "XAUT", "PAXG", "KAU", "KAG",
    "WBTC", "WETH", "STETH", "WSTETH", "WBETH", "CBBTC", "BTCB", "CBETH", "RETH",
    "METH", "WEETH", "EZETH", "BNSOL", "JITOSOL", "MSOL", "LBTC", "SOLVBTC",
})
STABLE_RE = re.compile(r"^(USD[A-Z0-9]*|[A-Z]*USD|EUR[A-Z]?)$")
LEVERAGED_RE = re.compile(r"^[A-Z0-9]+\d+[LS]$")      # BTC3L / ETH5S 之類槓桿代幣

# ---- 股票代幣化 ----
STOCK_TICKERS = frozenset({
    "AAPL", "TSLA", "NVDA", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NFLX", "COIN",
    "MSTR", "HOOD", "CRCL", "PLTR", "AMD", "INTC", "ORCL", "AVGO", "BABA",
    "SPY", "QQQ", "IWM", "TQQQ", "SQQQ", "GLD", "SLV", "MCD", "JPM",
    "DIS", "UBER", "ABNB", "SHOP", "PYPL", "CRM", "ADBE", "TSM", "ASML", "SMCI",
    "QCOM", "NKE", "WMT", "COST", "LLY", "UNH", "JNJ", "PFE", "SBUX", "RIVN",
    "LCID", "NIO", "XPEV", "BIDU", "PDD", "CRWD", "PANW", "ROKU", "RDDT",
    "CVNA", "IBM", "CSCO", "MRVL", "AMAT", "BRKB",
})

# ---- 派網的美股/ETF 永續多以「代號+X」命名（AAOIX、SOXLX…）；以下是結尾剛好是 X 的真加密貨幣 ----
# 與 dry run 唯一的語意差異：CNLX（龙虾_USDT_PERP 的 baseCurrency）。它是加密幣，名稱規則把它當股票排除
# （captain 2026-10-04 確認），所以加進這份例外名單。
CRYPTO_X_WHITELIST = frozenset({"AVAX", "BEAMX", "POLYX", "FLUX", "APEX", "DYDX", "IOTX", "ZETAX", "SAFEX",
                                "CNLX"})
# ---- 非加密的商品/個股（不以 X 結尾）----
NON_CRYPTO = frozenset({"WTI", "BRENT", "XAG", "XAU", "XPT", "XPD", "NATGAS", "COPPER", "SKHY", "SMSN", "KORU"})

# is_stock_token 依序檢查的結尾（去掉之後在 STOCK_TICKERS 就算股票）
STOCK_SUFFIXES = ("X", "ON", "B", "STOCK")

# ---- 手動排除（相當於 dry run 的 CONFIG["EXTRA_EXCLUDE"]，預設空）。放 base，大小寫不拘 ----
MANUAL_EXCLUDE = frozenset()

# 給人看的規則說明（理由要說出命中的是哪一條規則）
_RULE_NON_CRYPTO = "在 NON_CRYPTO"
_RULE_X_SUFFIX = "名稱 X 結尾（長度 ≥ 4、不在 CRYPTO_X_WHITELIST）"
_RULE_STOCK_TICKER = "在 STOCK_TICKERS"
_RULE_STOCK_STEM = "去掉結尾 %s 後的 %s 在 STOCK_TICKERS"
_RULE_PEGGED = "在 PEGGED"
_RULE_STABLE_RE = "名稱符合穩定幣樣式（STABLE_RE）"
_RULE_LEVERAGED_RE = "名稱符合槓桿代幣樣式（LEVERAGED_RE）"
_RULE_MANUAL = "在 MANUAL_EXCLUDE"
_RULE_DISABLED = "交易對已停用（enable=false）"
_RULE_WHITELIST = "在 CRYPTO_X_WHITELIST（例外納入）"
_RULE_NO_HIT = "沒有命中任何排除規則"


@dataclass(frozen=True)
class Classification:
    """一個交易對的判定。

    symbol    交易對（例：龙虾_USDT_PERP）
    base      判定用的 base（baseCurrency，沒有才退回 symbol 第一段；大寫）
    tradable  是否可交易（只有 category == "crypto" 為真）
    category  類別代碼（CATEGORIES 之一）
    reason    給人看的理由：「<類別名>：<命中的規則>」
    """
    symbol: str
    base: str
    tradable: bool
    category: str
    reason: str

    @property
    def label(self):
        """類別的顯示名稱。"""
        return CATEGORY_LABELS[self.category]

    @property
    def verdict(self):
        """「納入」或「排除」。"""
        return VERDICT_INCLUDED if self.tradable else VERDICT_EXCLUDED

    @property
    def display_symbol(self):
        """symbol；base 與 symbol 第一段不同時在後面括號標 base（例：龙虾_USDT_PERP（CNLX））。"""
        return display_symbol(self.symbol, self.base)


def _symbol_parts(spec):
    return str(spec.get("symbol") or "").split("_")


def base_of(spec):
    """判定用的 base：baseCurrency，沒有才取 symbol 的第一段，一律大寫。"""
    return str(spec.get("baseCurrency") or _symbol_parts(spec)[0]).upper()


def quote_of(spec):
    """計價幣：quoteCurrency，沒有才取 symbol 的第二段，一律大寫（symbol 沒有第二段 → 空字串）。"""
    parts = _symbol_parts(spec)
    return str(spec.get("quoteCurrency") or (parts[1] if len(parts) > 1 else "")).upper()


def display_symbol(symbol, base):
    """symbol；base 與 symbol 第一段（大寫）不同時加「（base）」。"""
    first = str(symbol).split("_")[0].upper()
    return symbol if not base or first == base else "%s（%s）" % (symbol, base)


def stock_rule(base):
    """股票／ETF／商品規則（照搬 is_stock_token 的判斷順序）。命中回傳規則說明，沒命中回 None。"""
    if base in NON_CRYPTO:
        return _RULE_NON_CRYPTO
    if base.endswith("X") and len(base) >= 4 and base not in CRYPTO_X_WHITELIST:
        return _RULE_X_SUFFIX
    if base in STOCK_TICKERS:
        return _RULE_STOCK_TICKER
    for suf in STOCK_SUFFIXES:
        if base.endswith(suf) and base[: -len(suf)] in STOCK_TICKERS:
            return _RULE_STOCK_STEM % (suf, base[: -len(suf)])
    return None


def is_stock_token(base):
    """與 pionex_backtest.is_stock_token 同語意（差別只有 CRYPTO_X_WHITELIST 多了 CNLX）。"""
    return stock_rule(base) is not None


def classify(spec, quote=QUOTE):
    """一筆 /common/symbols 的規格 dict → Classification；計價幣不是 quote 的回 None（不列出）。"""
    symbol = str(spec.get("symbol") or "")
    base = base_of(spec)
    if quote_of(spec) != str(quote).upper():
        return None

    def result(category, rule):
        return Classification(symbol, base, category == CATEGORY_CRYPTO, category,
                              "%s：%s" % (CATEGORY_LABELS[category], rule))

    if spec.get("enable") is False:
        return result(CATEGORY_DISABLED, _RULE_DISABLED)
    if base in PEGGED:
        return result(CATEGORY_PEGGED, _RULE_PEGGED)
    if STABLE_RE.match(base):
        return result(CATEGORY_PEGGED, _RULE_STABLE_RE)
    if LEVERAGED_RE.match(base):
        return result(CATEGORY_LEVERAGED, _RULE_LEVERAGED_RE)
    rule = stock_rule(base)
    if rule is not None:
        return result(CATEGORY_STOCK, rule)
    if base in {b.upper() for b in MANUAL_EXCLUDE}:
        return result(CATEGORY_MANUAL, _RULE_MANUAL)
    return result(CATEGORY_CRYPTO, _RULE_WHITELIST if base in CRYPTO_X_WHITELIST else _RULE_NO_HIT)


def summarize(classifications):
    """一批 Classification → {"total", "included", "excluded", "by_category"}。

    by_category 只列有排除的類別，依 EXCLUDED_CATEGORIES 的順序（dict 保留插入順序）。"""
    items = list(classifications)
    counts = {}
    for c in items:
        if not c.tradable:
            counts[c.category] = counts.get(c.category, 0) + 1
    included = sum(1 for c in items if c.tradable)
    return {"total": len(items), "included": included, "excluded": len(items) - included,
            "by_category": {cat: counts[cat] for cat in EXCLUDED_CATEGORIES if counts.get(cat)}}


def summary_text(summary):
    """summarize() 的結果 → 「納入 N、排除 M（股票／ETF／商品 a、強掛勾／穩定幣／包裝幣 b）」。"""
    by_cat = summary.get("by_category") or {}
    detail = "、".join("%s %d" % (CATEGORY_LABELS[cat], n) for cat, n in by_cat.items())
    return "納入 %d、排除 %d%s" % (summary["included"], summary["excluded"], "（%s）" % detail if detail else "")
