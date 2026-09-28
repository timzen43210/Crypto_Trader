# -*- coding: utf-8 -*-
"""
live.a_channel_text — A 頻道訊息的文字格式（T2 FR-1）：進場 / 止盈出場 / 止損出場
==================================================================================
**純函式**：輸入事件（live.signal_events 的 EntryEvent / ExitEvent）與「收到當下的快照值」，輸出字串。
不讀時鐘、不打網路、不讀 market_static、不讀 strategy/ 的參數。快照值由 live.a_channel_push 在匯流排 handler
裡取好、存進 outbox，所以同一列不論何時重新組字，內容都相同。

──────────────────────────────────────────────────────────────────────
每個數字從哪裡來
──────────────────────────────────────────────────────────────────────
  編號          event.signal_id（A3 的 make_signal_id() 產生），原樣使用
  交易對        signal_id 裡的幣名（coin_from_signal_id）：A3 已經照「symbol 去掉 _USDT_PERP」的規則組好，
                這裡只把它從 signal_id 拆回來，不另寫一份去後綴的規則。拆不出來（格式不是 A3 的）就顯示
                event.symbol 原文
  策略          快照的 label（= config.STRATEGY_LABELS[strategy]）
  百分比        止盈價 ÷ 訊號價 − 1、止損價 ÷ 訊號價 − 1，**由事件的價格反算**，不讀 strategy/ 的 TAKE_PROFIT /
                STOP_LOSS。A3 重送的舊事件顯示當時的值
  名目報酬      做空：(進場價 − 出場價) ÷ 進場價，由事件的價格計算。跳空止損照實際數字顯示
  持倉時間      closed_ms − opened_ms
  發出時間      （進場）訊號 K 棒收盤 = 快照的 signal_close_ms（handler 以 bar_open_ms + A3 規格的主週期算好）
  出場時間      （出場）closed_ms。兩者一律台北時間 YYYY-MM-DD HH:MM
  價格          依快照的 price_decimals 四捨五入（ROUND_HALF_UP，以 repr(float) 的十進位值為準）。
                price_decimals 由 handler 決定：market_static.symbol_spec() 的 quotePrecision（派網 PERP 的價格
                小數位數；實測依據見 live.a_channel_push），取不到就依參考價的有效位數推一個位數
                （price_decimals_for()）。同一則訊息的所有價格用同一個位數
  建議倉位      position_advice()：lev = min(目標槓桿, 該幣上限)，建議倉位 = 本金的 下單比例 × 目標槓桿 ÷ lev，
                百分比最多一位小數。該幣上限取不到（None）時不捏造上限：以目標槓桿計並註明
  偏離警語      快照的 deviation_warn_pct（config.A_CHANNEL_DEVIATION_WARN_PCT）

出場原因（exit_reason_text，優先序由上到下；使用者 2026-09-28 AC-8 定稿）：
  止盈                              → 「觸及止盈價」
  止損且 gap_open                   → 「觸及止損價且有滑價」（gap_open 與 same_bar_both 同時為真時，跳空優先）
  其他止損（含 same_bar_both、minor_missing）→ 「觸及止損價」
**minor_resolved、data_gap、judged_interval 等旗標一律不出現在訊息裡**（A3 captain 審查裁決 1：不可以呈現
為「1分K判定先止盈 / 先止損」）。features 的內容（進場的判定特徵、出場的稽核旗標）都不印。

──────────────────────────────────────────────────────────────────────
版面（使用者 2026-09-28 看過實機樣本定案，AC-8）
──────────────────────────────────────────────────────────────────────
只有一種版面：「標籤：值」（全形冒號），不靠空白對齊 —— Telegram 用比例字型，空白對齊在手機上會歪。
PRD 原本的「標籤後補空白」版面已經移除，沒有版面開關。

  標題行（🔴 做空訊號 / ✅ 止盈出場 / ❌ 止損出場 + 兩個空白 + signal_id）
  空行
  （出場、需要延遲註記時）⚠️ 延遲發布：…見下方「出場時間」 + 空行
  各區塊（區塊之間空一行）
  空行 + 註腳（進場：兩行 ⚠️ 警語；出場：兩行 ※ 說明）
"""

import re
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal

from live import config
from live.logsetup import TAIPEI
from live.signal_events import EntryEvent, ExitEvent

# 符號（用 chr() 組，避免編輯器把看不見的 variation selector 弄丟）
RED_CIRCLE = chr(0x1F534)                       # 🔴
CHECK_MARK = chr(0x2705)                        # ✅
CROSS_MARK = chr(0x274C)                        # ❌
WARNING_SIGN = chr(0x26A0) + chr(0xFE0F)        # ⚠️（emoji 呈現）
REFERENCE_MARK = chr(0x203B)                    # ※

# 標題
ENTRY_TITLE = RED_CIRCLE + " 做空訊號"
TAKE_PROFIT_TITLE = CHECK_MARK + " 止盈出場"
STOP_LOSS_TITLE = CROSS_MARK + " 止損出場"

# 出場原因（使用者 2026-09-28 AC-8 定稿；同根兩碰 / minor_missing 與一般止損同一句）
REASON_TAKE_PROFIT = "觸及止盈價"
REASON_GAP = "觸及止損價且有滑價"
REASON_STOP_LOSS = "觸及止損價"

# 欄位標籤（進場的時間叫「發出時間」、出場的叫「出場時間」）
ENTRY_TIME_LABEL = "發出時間"
EXIT_TIME_LABEL = "出場時間"

# 標籤與值之間的全形冒號
LABEL_SEPARATOR = "："

# 註記
LEVERAGE_UNKNOWN_TEXT = "該幣上限無法取得，請以交易所為準"
DELAY_NOTE = WARNING_SIGN + " 延遲發布：本則晚於實際出場時間送出，實際出場時間見下方「" + EXIT_TIME_LABEL + "」"
ENTRY_FOOTER_DISCLAIMER = WARNING_SIGN + " 本訊號僅供參考，不構成投資建議"
EXIT_FOOTER = (REFERENCE_MARK + " 策略名目結果，未計手續費與資金費率",
               REFERENCE_MARK + " 實際損益依個人進場價與費率而異")

# signal_id = #<幣名>-<S4|S5>-<YYYYMMDD>-<HHMM>（A3 的 make_signal_id）；幣名本身可能含「-」，所以從右邊拆
_SIGNAL_ID_RE = re.compile(r"^#(?P<coin>.+)-(?P<tag>[A-Za-z0-9]+)-(?P<date>\d{8})-(?P<hm>\d{4})$")

# 取不到精度時推出來的小數位數上下限（派網實測 quotePrecision 介於 0～11）
_MAX_DECIMALS = 12


# ============================== 小工具（純函式） ==============================
def coin_from_signal_id(signal_id):
    """signal_id 裡的幣名（A3 已套用「symbol 去掉 _USDT_PERP」）；格式不符回 None。"""
    m = _SIGNAL_ID_RE.match(signal_id or "")
    return m.group("coin") if m else None


def signal_id_time_text(signal_id):
    """signal_id 裡的台北時間，轉成 YYYY-MM-DD HH:MM（給測試比對「發出時間」用）；格式不符回 None。"""
    m = _SIGNAL_ID_RE.match(signal_id or "")
    if not m:
        return None
    d, hm = m.group("date"), m.group("hm")
    return "%s-%s-%s %s:%s" % (d[:4], d[4:6], d[6:], hm[:2], hm[2:])


def taipei_text(ms):
    """UTC epoch 毫秒 → 台北時間 YYYY-MM-DD HH:MM。"""
    return datetime.fromtimestamp(int(ms) / 1000.0, TAIPEI).strftime("%Y-%m-%d %H:%M")


def _decimal(x):
    """float → Decimal，取 repr 的十進位值（0.1 就是 0.1，不是 0.1000000000000000055…）。"""
    return Decimal(repr(float(x)))


def format_price(price, decimals):
    """價格四捨五入到 decimals 位小數（ROUND_HALF_UP），固定小數位數、不用科學記號。"""
    decimals = int(decimals)
    q = _decimal(price).quantize(Decimal(1).scaleb(-decimals), rounding=ROUND_HALF_UP)
    return format(q, "f")


def price_decimals_for(reference_price, significant_digits):
    """取不到價格精度時的退回規則：讓 reference_price 顯示 significant_digits 位有效數字的小數位數。
    例：6 位有效數字時 0.15346 → 6、64069.4 → 1、0.0000071234 → 11。"""
    adjusted = _decimal(reference_price).adjusted()          # 最高位數字的 10 的次方
    return max(0, min(_MAX_DECIMALS, int(significant_digits) - 1 - adjusted))


def format_signed_pct(ratio):
    """比例 → 帶正負號、一位小數的百分比：-0.04 → "-4.0%"、0.05 → "+5.0%"。"""
    q = (_decimal(ratio) * 100).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    if q == 0:
        q = abs(q)
    sign = "+" if q > 0 else ("-" if q < 0 else "")
    return "%s%s%%" % (sign, format(abs(q), "f"))


def format_plain_pct(ratio):
    """比例 → 最多一位小數的百分比（去掉 .0）：0.02 → "2%"、0.0333… → "3.3%"。"""
    q = (_decimal(ratio) * 100).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    text = format(q, "f")
    if text.endswith(".0"):
        text = text[:-2]
    return text + "%"


def format_duration(ms):
    """持倉時間：25m / 1h 25m / 1d 3h 25m（無條件捨去到分鐘）。"""
    minutes = max(0, int(ms)) // 60000
    days, rest = divmod(minutes, 24 * 60)
    hours, mins = divmod(rest, 60)
    if days:
        return "%dd %dh %dm" % (days, hours, mins)
    if hours:
        return "%dh %dm" % (hours, mins)
    return "%dm" % mins


def position_advice(order_pct, target_leverage, max_leverage):
    """(實際槓桿, 建議倉位佔本金的比例, 上限是否取得)。max_leverage 是 None（或不是正整數）時以目標槓桿計。"""
    known = (isinstance(max_leverage, int) and not isinstance(max_leverage, bool) and max_leverage >= 1)
    lev = min(int(target_leverage), max_leverage) if known else int(target_leverage)
    return lev, float(order_pct) * float(target_leverage) / lev, known


def exit_reason_text(event):
    """出場原因（優先序見模組 docstring）。只看 reason 與 gap_open；同根兩碰、minor_missing 都是一般的「觸及止損價」。"""
    if event.reason == config.EXIT_TAKE_PROFIT:
        return REASON_TAKE_PROFIT
    if event.features.get("gap_open"):
        return REASON_GAP
    return REASON_STOP_LOSS


# ============================== 版面 ==============================
def _row(label, value):
    return label + LABEL_SEPARATOR + value


def _level_value(price_text, pct_text):
    return "%s (%s)" % (price_text, pct_text)


def _assemble(title, signal_id, blocks, footer, notes=()):
    """標題行 + 空行 +（註記 + 空行）+ 各區塊（區塊之間空一行）+ 空行 + 註腳。"""
    lines = ["%s  %s" % (title, signal_id), ""]
    for note in notes:
        lines.extend((note, ""))
    for block in blocks:
        lines.extend(_row(label, value) for label, value in block)
        lines.append("")
    lines.extend(footer)
    return "\n".join(lines)


def _pair_text(event):
    return coin_from_signal_id(event.signal_id) or event.symbol


# ============================== 三種訊息 ==============================
def format_entry(event, snapshot):
    """進場訊息。snapshot 的鍵：label, signal_close_ms, price_decimals, max_leverage, order_pct,
    target_leverage, deviation_warn_pct（由 live.a_channel_push 在收到事件時取好）。"""
    if type(event) is not EntryEvent:
        raise TypeError("format_entry 只收 EntryEvent，收到 %s" % type(event).__name__)
    d = snapshot["price_decimals"]
    price = event.signal_price
    tp_pct = format_signed_pct(event.take_profit_price / price - 1)
    sl_pct = format_signed_pct(event.stop_loss_price / price - 1)
    lev, pct, known = position_advice(snapshot["order_pct"], snapshot["target_leverage"], snapshot["max_leverage"])
    if known:
        size_text = "本金的 %s" % format_plain_pct(pct)
        lev_text = "%dx（該幣上限 %dx）" % (lev, snapshot["max_leverage"])
    else:
        size_text = "本金的 %s（以 %dx 槓桿計算）" % (format_plain_pct(pct), lev)
        lev_text = LEVERAGE_UNKNOWN_TEXT
    blocks = [
        [("策略", snapshot["label"]),
         ("交易對", _pair_text(event)),
         ("訊號價", format_price(price, d)),
         ("止盈價", _level_value(format_price(event.take_profit_price, d), tp_pct)),
         ("止損價", _level_value(format_price(event.stop_loss_price, d), sl_pct))],
        [("建議倉位", size_text),
         ("槓桿", lev_text)],
        [(ENTRY_TIME_LABEL, taipei_text(snapshot["signal_close_ms"]))],
    ]
    footer = [WARNING_SIGN + " 價格偏離訊號價超過 %s 不建議追進" % format_plain_pct(snapshot["deviation_warn_pct"]),
              ENTRY_FOOTER_DISCLAIMER]
    return _assemble(ENTRY_TITLE, event.signal_id, blocks, footer)


def format_exit(event, snapshot, *, delayed=False):
    """止盈 / 止損出場訊息（兩者欄位完全對稱）。snapshot 的鍵：label, price_decimals。
    delayed：在標題下方、「策略」那一行上面加延遲註記（呼叫端依 recovered 旗標或實際交給發送器的時刻決定，
    見 live.a_channel_push）。"""
    if type(event) is not ExitEvent:
        raise TypeError("format_exit 只收 ExitEvent，收到 %s" % type(event).__name__)
    d = snapshot["price_decimals"]
    title = TAKE_PROFIT_TITLE if event.reason == config.EXIT_TAKE_PROFIT else STOP_LOSS_TITLE
    ret = (event.entry_price - event.exit_price) / event.entry_price       # 做空的名目報酬
    blocks = [
        [("策略", snapshot["label"]),
         ("交易對", _pair_text(event)),
         ("進場價", format_price(event.entry_price, d)),
         ("出場價", format_price(event.exit_price, d)),
         ("持倉時間", format_duration(event.closed_ms - event.opened_ms))],
        [("名目報酬", format_signed_pct(ret)),
         ("出場原因", exit_reason_text(event))],
        [(EXIT_TIME_LABEL, taipei_text(event.closed_ms))],
    ]
    return _assemble(title, event.signal_id, blocks, list(EXIT_FOOTER), notes=(DELAY_NOTE,) if delayed else ())


def render(event, snapshot, *, delayed=False):
    """依事件型別組字。出場才看 delayed。"""
    if type(event) is EntryEvent:
        return format_entry(event, snapshot)
    return format_exit(event, snapshot, delayed=delayed)
