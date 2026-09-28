# -*- coding: utf-8 -*-
"""
T2（live.a_channel_text）驗收測試 — AC-1 訊息格式（黃金樣本逐字比對）。版面是「標籤：值」（使用者 2026-09-28 AC-8 定案）。

  * 每種情境都有完整的預期字串，逐字比對：s4 / s5 進場；槓桿上限 ≥ 50（75x、剛好 50x）/ < 50（20x、30x → 3.3%）/
    取不到；止盈；一般止損；跳空止損（與同根兩碰同時為真時跳空優先）；同根兩碰止損；minor_missing（A3 同時標
    same_bar_both / 只有 minor_missing）；延遲註記（止盈、跳空止損各一）。全部是字面值，沒有用別的樣本 replace 出來
  * 預期字串裡的數字**不寫死策略參數**：事件全部是本檔自建的，止盈 / 止損 / 出場價的比例刻意避開 s4、s5 的**所有**
    出場參數（test_ac1_golden_events_never_use_strategy_exit_ratios 從 strategy/ 的 exit_params() 讀參數比對，
    不寫死）。百分比、報酬、持倉時間都由這些事件的價格與時間推導；倉位規則與偏離門檻是本檔給的快照值
  * minor_resolved 為真的事件，輸出不含「1分K」「判定先」等字樣；出場稽核旗標、進場特徵一律不印
  * 止盈與止損訊息的欄位集合相同（對稱性）；出場的時間標籤是「出場時間」、進場維持「發出時間」
  * 延遲註記在標題下方、「策略」那一行上面，底部沒有註記
  * STRATEGY_LABELS 的鍵等於 STRATEGIES
  * 「發出時間」與 signal_id 裡的時間一致：signal_id 由 A3 的 make_signal_id() 產生、收盤 = bar_open_ms + A3 規格的主週期
  * 最長情境（長幣名 + 跳空 + 延遲註記）的長度遠低於 4096 UTF-16 code units
  * --sample 的第 9 則（延遲止盈）與使用者確認過的樣子一致（數字由樣本事件推導）

全程離線（socket 籠子），不依賴 pytest：直接 `python tests/test_a_channel_text.py`。
"""
import inspect
import os
import re
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, TESTS_DIR)

from test_tg_channel import OfflineCage  # noqa: E402

from live import a_channel_text as T  # noqa: E402
from live import config  # noqa: E402
from live import notional_tracker as nt  # noqa: E402
from live.signal_events import EntryEvent, ExitEvent  # noqa: E402
from live.tg_channel import utf16_units  # noqa: E402

W = T.WARNING_SIGN          # ⚠️（U+26A0 U+FE0F）
MIN = 60_000
CLOSE_1535 = 1789716900000  # 2026-09-18 15:35 台北時間（= 07:35 UTC）
TP, SL = config.EXIT_TAKE_PROFIT, config.EXIT_STOP_LOSS

# 本檔自建事件用的價格（比例刻意避開 strategy/ 的出場參數，見 test_ac1_golden_events_never_use_strategy_exit_ratios）
ACE_PRICE, ACE_TP, ACE_SL = 0.15346, 0.142718, 0.1626676      # 止盈 −7.0%、止損 +6.0%
ACE_GAP_EXIT = 0.165                                           # 跳空：名目報酬 −7.5%
BTC_PRICE, BTC_TP, BTC_SL = 64069.4, 60225.236, 68554.258      # 止盈 −6.0%、止損 +7.0%


# ============================== 本檔自建的事件與快照 ==============================
def entry(signal_id="#ACE-S4-20260918-1535", strategy="s4", symbol="ACE_USDT_PERP", price=ACE_PRICE,
          tp=ACE_TP, sl=ACE_SL, bar_open_ms=CLOSE_1535 - 5 * MIN, features=None):
    return EntryEvent(strategy=strategy, signal_id=signal_id, symbol=symbol, direction=config.DIRECTION_SHORT,
                      created_ms=bar_open_ms + 5 * MIN + 2000, bar_open_ms=bar_open_ms, signal_price=price,
                      take_profit_price=tp, stop_loss_price=sl,
                      features=features or {"ret2h": 0.1987654, "volr": float("inf"), "cpos": 0.3123})


def s5_entry():
    return entry(signal_id="#BTC-S5-20260918-1536", strategy="s5", symbol="BTC_USDT_PERP", price=BTC_PRICE,
                 tp=BTC_TP, sl=BTC_SL, bar_open_ms=CLOSE_1535)


def entry_snap(max_leverage=75, label="策略4", decimals=5, close_ms=CLOSE_1535):
    return {"label": label, "price_decimals": decimals, "price_decimals_source": "quotePrecision",
            "signal_close_ms": close_ms, "max_leverage": max_leverage, "order_pct": 0.02, "target_leverage": 50,
            "deviation_warn_pct": 0.01}


def exit_ev(reason, exit_price, hold_min, signal_id="#ACE-S4-20260918-1535", strategy="s4", symbol="ACE_USDT_PERP",
            entry_price=ACE_PRICE, opened_ms=CLOSE_1535, **flags):
    feats = {"gap_open": False, "same_bar_both": False, "minor_resolved": False, "minor_missing": False,
             "judged_interval": "1M", "exit_bar_open_ms": opened_ms, "recovered": False, "data_gap": False,
             "judged_ms": opened_ms + hold_min * MIN + 2000}
    feats.update(flags)
    return ExitEvent(strategy=strategy, signal_id=signal_id, symbol=symbol, direction=config.DIRECTION_SHORT,
                     created_ms=opened_ms + hold_min * MIN + 2000, reason=reason, exit_price=exit_price,
                     entry_price=entry_price, opened_ms=opened_ms, closed_ms=opened_ms + hold_min * MIN,
                     features=feats)


def exit_snap(label="策略4", decimals=5):
    return {"label": label, "price_decimals": decimals, "price_decimals_source": "quotePrecision"}


# 黃金樣本用到的出場事件（名稱 → 事件）
EXITS = {
    "tp": exit_ev(TP, ACE_TP, 85),
    "sl": exit_ev(SL, ACE_SL, 42),
    "gap": exit_ev(SL, ACE_GAP_EXIT, 190, gap_open=True, same_bar_both=True),
    "same_bar": exit_ev(SL, ACE_SL, 12, same_bar_both=True, minor_resolved=True),
    "minor_missing": exit_ev(SL, ACE_SL, 20, same_bar_both=True, minor_missing=True, judged_interval="5M"),
    "minor_missing_only": exit_ev(SL, ACE_SL, 25, minor_missing=True),
    "tp_delayed": exit_ev(TP, ACE_TP, 1570, recovered=True),
    "gap_delayed": exit_ev(SL, ACE_GAP_EXIT, 190, gap_open=True),
}


# ============================== 黃金樣本：進場 ==============================
ENTRY_75 = f"""🔴 做空訊號  #ACE-S4-20260918-1535

策略：策略4
交易對：ACE
訊號價：0.15346
止盈價：0.14272 (-7.0%)
止損價：0.16267 (+6.0%)

建議倉位：本金的 2%
槓桿：50x（該幣上限 75x）

發出時間：2026-09-18 15:35

{W} 價格偏離訊號價超過 1% 不建議追進
{W} 本訊號僅供參考，不構成投資建議"""

ENTRY_50 = f"""🔴 做空訊號  #ACE-S4-20260918-1535

策略：策略4
交易對：ACE
訊號價：0.15346
止盈價：0.14272 (-7.0%)
止損價：0.16267 (+6.0%)

建議倉位：本金的 2%
槓桿：50x（該幣上限 50x）

發出時間：2026-09-18 15:35

{W} 價格偏離訊號價超過 1% 不建議追進
{W} 本訊號僅供參考，不構成投資建議"""

ENTRY_20 = f"""🔴 做空訊號  #ACE-S4-20260918-1535

策略：策略4
交易對：ACE
訊號價：0.15346
止盈價：0.14272 (-7.0%)
止損價：0.16267 (+6.0%)

建議倉位：本金的 5%
槓桿：20x（該幣上限 20x）

發出時間：2026-09-18 15:35

{W} 價格偏離訊號價超過 1% 不建議追進
{W} 本訊號僅供參考，不構成投資建議"""

ENTRY_30 = f"""🔴 做空訊號  #ACE-S4-20260918-1535

策略：策略4
交易對：ACE
訊號價：0.15346
止盈價：0.14272 (-7.0%)
止損價：0.16267 (+6.0%)

建議倉位：本金的 3.3%
槓桿：30x（該幣上限 30x）

發出時間：2026-09-18 15:35

{W} 價格偏離訊號價超過 1% 不建議追進
{W} 本訊號僅供參考，不構成投資建議"""

ENTRY_UNKNOWN = f"""🔴 做空訊號  #ACE-S4-20260918-1535

策略：策略4
交易對：ACE
訊號價：0.15346
止盈價：0.14272 (-7.0%)
止損價：0.16267 (+6.0%)

建議倉位：本金的 2%（以 50x 槓桿計算）
槓桿：該幣上限無法取得，請以交易所為準

發出時間：2026-09-18 15:35

{W} 價格偏離訊號價超過 1% 不建議追進
{W} 本訊號僅供參考，不構成投資建議"""

ENTRY_S5 = f"""🔴 做空訊號  #BTC-S5-20260918-1536

策略：策略5
交易對：BTC
訊號價：64069.4
止盈價：60225.2 (-6.0%)
止損價：68554.3 (+7.0%)

建議倉位：本金的 2%
槓桿：50x（該幣上限 100x）

發出時間：2026-09-18 15:36

{W} 價格偏離訊號價超過 1% 不建議追進
{W} 本訊號僅供參考，不構成投資建議"""

# ============================== 黃金樣本：出場 ==============================
EXIT_TP = """✅ 止盈出場  #ACE-S4-20260918-1535

策略：策略4
交易對：ACE
進場價：0.15346
出場價：0.14272
持倉時間：1h 25m

名目報酬：+7.0%
出場原因：觸及止盈價

出場時間：2026-09-18 17:00

※ 策略名目結果，未計手續費與資金費率
※ 實際損益依個人進場價與費率而異"""

EXIT_SL = """❌ 止損出場  #ACE-S4-20260918-1535

策略：策略4
交易對：ACE
進場價：0.15346
出場價：0.16267
持倉時間：42m

名目報酬：-6.0%
出場原因：觸及止損價

出場時間：2026-09-18 16:17

※ 策略名目結果，未計手續費與資金費率
※ 實際損益依個人進場價與費率而異"""

# 跳空：出場價是開盤價 0.16500，比止損價差 → 名目報酬照實際數字（(0.15346 − 0.165) ÷ 0.15346 = −7.52%）
EXIT_GAP = """❌ 止損出場  #ACE-S4-20260918-1535

策略：策略4
交易對：ACE
進場價：0.15346
出場價：0.16500
持倉時間：3h 10m

名目報酬：-7.5%
出場原因：觸及止損價且有滑價

出場時間：2026-09-18 18:45

※ 策略名目結果，未計手續費與資金費率
※ 實際損益依個人進場價與費率而異"""

EXIT_SAME_BAR = """❌ 止損出場  #ACE-S4-20260918-1535

策略：策略4
交易對：ACE
進場價：0.15346
出場價：0.16267
持倉時間：12m

名目報酬：-6.0%
出場原因：觸及止損價

出場時間：2026-09-18 15:47

※ 策略名目結果，未計手續費與資金費率
※ 實際損益依個人進場價與費率而異"""

EXIT_MINOR_MISSING = """❌ 止損出場  #ACE-S4-20260918-1535

策略：策略4
交易對：ACE
進場價：0.15346
出場價：0.16267
持倉時間：20m

名目報酬：-6.0%
出場原因：觸及止損價

出場時間：2026-09-18 15:55

※ 策略名目結果，未計手續費與資金費率
※ 實際損益依個人進場價與費率而異"""

EXIT_MINOR_MISSING_ONLY = """❌ 止損出場  #ACE-S4-20260918-1535

策略：策略4
交易對：ACE
進場價：0.15346
出場價：0.16267
持倉時間：25m

名目報酬：-6.0%
出場原因：觸及止損價

出場時間：2026-09-18 16:00

※ 策略名目結果，未計手續費與資金費率
※ 實際損益依個人進場價與費率而異"""

EXIT_TP_DELAYED = f"""✅ 止盈出場  #ACE-S4-20260918-1535

{W} 延遲發布：本則晚於實際出場時間送出，實際出場時間見下方「出場時間」

策略：策略4
交易對：ACE
進場價：0.15346
出場價：0.14272
持倉時間：1d 2h 10m

名目報酬：+7.0%
出場原因：觸及止盈價

出場時間：2026-09-19 17:45

※ 策略名目結果，未計手續費與資金費率
※ 實際損益依個人進場價與費率而異"""

EXIT_GAP_DELAYED = f"""❌ 止損出場  #ACE-S4-20260918-1535

{W} 延遲發布：本則晚於實際出場時間送出，實際出場時間見下方「出場時間」

策略：策略4
交易對：ACE
進場價：0.15346
出場價：0.16500
持倉時間：3h 10m

名目報酬：-7.5%
出場原因：觸及止損價且有滑價

出場時間：2026-09-18 18:45

※ 策略名目結果，未計手續費與資金費率
※ 實際損益依個人進場價與費率而異"""

EXIT_GOLDEN = {
    "tp": (False, EXIT_TP), "sl": (False, EXIT_SL), "gap": (False, EXIT_GAP), "same_bar": (False, EXIT_SAME_BAR),
    "minor_missing": (False, EXIT_MINOR_MISSING), "minor_missing_only": (False, EXIT_MINOR_MISSING_ONLY),
    "tp_delayed": (True, EXIT_TP_DELAYED), "gap_delayed": (True, EXIT_GAP_DELAYED),
}


def _eq(got, want, what):
    if got != want:
        gl, wl = got.split("\n"), want.split("\n")
        for i, (g, w) in enumerate(zip(gl, wl)):
            if g != w:
                raise AssertionError("%s 第 %d 行不同：\n  實際 %r\n  預期 %r" % (what, i + 1, g, w))
        raise AssertionError("%s 行數不同：實際 %d、預期 %d\n%s" % (what, len(gl), len(wl), got))


# ============================== AC-1：黃金樣本 ==============================
def test_ac1_entry_golden_leverage_cases():
    e = entry()
    for max_lev, want, what in ((75, ENTRY_75, "上限 75x"), (50, ENTRY_50, "上限剛好 50x"), (20, ENTRY_20, "上限 20x"),
                                (30, ENTRY_30, "上限 30x → 3.3%"), (None, ENTRY_UNKNOWN, "上限取不到")):
        _eq(T.format_entry(e, entry_snap(max_lev)), want, "s4 進場（%s）" % what)


def test_ac1_s5_entry_golden():
    snap = entry_snap(100, label="策略5", decimals=1, close_ms=CLOSE_1535 + MIN)
    _eq(T.format_entry(s5_entry(), snap), ENTRY_S5, "s5 進場")


def test_ac1_exit_golden_cases():
    for name, (delayed, want) in EXIT_GOLDEN.items():
        _eq(T.format_exit(EXITS[name], exit_snap(), delayed=delayed), want, name)
    # render() 依事件型別分派，結果相同
    assert T.render(EXITS["tp_delayed"], exit_snap(), delayed=True) == EXIT_TP_DELAYED
    assert T.render(entry(), entry_snap(75)) == ENTRY_75


def test_ac1_exit_reason_texts_and_priority():
    """使用者 AC-8 定稿：止盈 → 觸及止盈價；止損且跳空 → 觸及止損價且有滑價（跳空優先於同根兩碰）；其他止損（含同根兩碰、
    minor_missing）→ 觸及止損價。"""
    assert (T.REASON_TAKE_PROFIT, T.REASON_GAP, T.REASON_STOP_LOSS) == ("觸及止盈價", "觸及止損價且有滑價", "觸及止損價")
    want = {"tp": T.REASON_TAKE_PROFIT, "sl": T.REASON_STOP_LOSS, "gap": T.REASON_GAP,
            "same_bar": T.REASON_STOP_LOSS, "minor_missing": T.REASON_STOP_LOSS,
            "minor_missing_only": T.REASON_STOP_LOSS, "tp_delayed": T.REASON_TAKE_PROFIT, "gap_delayed": T.REASON_GAP}
    for name, reason in want.items():
        assert T.exit_reason_text(EXITS[name]) == reason, name
    # 止盈不看任何旗標
    assert T.exit_reason_text(exit_ev(TP, ACE_TP, 5, gap_open=True, same_bar_both=True)) == T.REASON_TAKE_PROFIT


def test_ac1_delay_note_sits_below_the_title_and_above_strategy():
    assert T.DELAY_NOTE == W + " 延遲發布：本則晚於實際出場時間送出，實際出場時間見下方「出場時間」", ascii(T.DELAY_NOTE)
    assert W == chr(0x26A0) + chr(0xFE0F), ascii(W)
    for name in ("tp_delayed", "gap_delayed"):
        lines = T.format_exit(EXITS[name], exit_snap(), delayed=True).split("\n")
        assert lines[1] == "" and lines[2] == T.DELAY_NOTE and lines[3] == "" and lines[4].startswith("策略："), lines
        assert lines.count(T.DELAY_NOTE) == 1 and lines[-2].startswith("※") and lines[-1].startswith("※"), lines
    for name in ("tp", "sl", "gap"):
        assert T.DELAY_NOTE not in T.format_exit(EXITS[name], exit_snap())


def test_ac1_time_labels():
    """出場訊息的時間叫「出場時間」（值是 closed_ms），進場訊息維持「發出時間」。"""
    etext = T.format_entry(entry(), entry_snap())
    assert "發出時間：2026-09-18 15:35" in etext.split("\n") and "出場時間" not in etext
    for name in EXITS:
        xtext = T.format_exit(EXITS[name], exit_snap(), delayed=EXIT_GOLDEN[name][0])
        assert "發出時間" not in xtext, name
        assert "出場時間：" + T.taipei_text(EXITS[name].closed_ms) in xtext.split("\n"), name


def test_ac1_single_colon_layout_without_space_alignment():
    """只剩「標籤：值」一種版面：沒有版面開關；每一個欄位行都是「標籤：值」，沒有用空白對齊。"""
    assert not hasattr(T, "LAYOUTS") and not hasattr(T, "LAYOUT_ALIGNED") and not hasattr(T, "LAYOUT_COLON")
    assert not hasattr(config, "A_CHANNEL_MESSAGE_LAYOUT")
    assert "A_CHANNEL_MESSAGE_LAYOUT" not in config.execution_params()
    for fn in (T.format_entry, T.format_exit, T.render):
        assert "layout" not in inspect.signature(fn).parameters, fn.__name__
    row = re.compile(r"^[^\s：]+：\S")
    texts = [ENTRY_75, ENTRY_UNKNOWN, ENTRY_S5] + [want for _, want in EXIT_GOLDEN.values()]
    for text in texts:
        lines = text.split("\n")
        body = [x for x in lines[1:] if x and not x.startswith((W, "※"))]
        assert body and all(row.match(x) for x in body), body
        assert not any("  " in x for x in lines[1:]), "標題行以外不應該有連續空白（空白對齊）"


def test_ac1_numbers_are_derived_from_the_event_prices():
    """百分比 / 報酬由事件價格推導：同一個格式化函式給不同的價格，數字跟著變（沒有寫死任何比例）。"""
    e = entry(price=2.0, tp=1.83456, sl=2.1234, signal_id="#XYZ-S4-20260918-1535", symbol="XYZ_USDT_PERP")
    lines = T.format_entry(e, entry_snap(decimals=3)).split("\n")
    assert "止盈價：1.835 (-8.3%)" in lines, lines                # 1.83456 → 1.835（四捨五入）；1.83456 ÷ 2 − 1 = −8.272%
    assert "止損價：2.123 (+6.2%)" in lines, lines                # 2.1234 ÷ 2 − 1 = +6.17%
    x = exit_ev(SL, 2.3, 7, entry_price=2.0, signal_id=e.signal_id, symbol=e.symbol, gap_open=True)
    assert "名目報酬：-15.0%" in T.format_exit(x, exit_snap(decimals=3)).split("\n")


def _golden_ratios():
    """所有黃金樣本（含上一條的 XYZ）用到的比例：[(說明, 比例的絕對值)]。"""
    out = []
    xyz = entry(price=2.0, tp=1.83456, sl=2.1234)
    for label, e in (("ACE 進場", entry()), ("BTC 進場（s5）", s5_entry()), ("XYZ 進場", xyz)):
        out.append((label + " 止盈", abs(e.take_profit_price / e.signal_price - 1)))
        out.append((label + " 止損", abs(e.stop_loss_price / e.signal_price - 1)))
    for name, x in list(EXITS.items()) + [("xyz_gap", exit_ev(SL, 2.3, 7, entry_price=2.0, gap_open=True))]:
        out.append(("出場 %s 名目報酬" % name, abs((x.entry_price - x.exit_price) / x.entry_price)))
    return out


def test_ac1_golden_events_never_use_strategy_exit_ratios():
    """F4：黃金樣本的每一個比例都不等於 s4 / s5 的任何一個出場參數（從 strategy/ 的 exit_params() 讀，不寫死）。
    否則格式化若被改成「讀策略參數」，黃金樣本會剛好還是對的、抓不到。"""
    params = []
    for mod in (nt.s4_signal, nt.s5_signal):
        ex = mod.exit_params()
        for key in ("TAKE_PROFIT", "STOP_LOSS"):
            params.append(("%s.%s" % (mod.__name__.split(".")[-1], key), float(ex[key])))
    assert len(params) == 4 and all(v > 0 for _, v in params), params
    ratios = _golden_ratios()
    assert len(ratios) >= 15, ratios
    clashes = [(what, r, name, v) for what, r in ratios for name, v in params if abs(r - v) < 1e-3]
    assert not clashes, "黃金樣本的比例剛好等於策略參數（改挑別的價格）：%r" % clashes


def test_ac1_minor_resolved_and_audit_flags_never_appear():
    forbidden = ("1分K", "1 分K", "判定先", "先止盈", "先止損", "minor", "judged", "data_gap", "gap_open",
                 "same_bar", "recovered", "小週期", "1M", "5M", "同一根", "保守", "跳空")
    events = [exit_ev(TP, ACE_TP, 85, minor_resolved=True, judged_interval="5M"),
              exit_ev(SL, ACE_SL, 12, minor_resolved=True, same_bar_both=True),
              exit_ev(SL, ACE_SL, 12, minor_resolved=True),
              exit_ev(SL, ACE_GAP_EXIT, 30, minor_resolved=True, gap_open=True, data_gap=True)]
    for event in events:
        for delayed in (False, True):
            text = T.format_exit(event, exit_snap(), delayed=delayed)
            hits = [w for w in forbidden if w in text]
            assert not hits, (hits, text)
    # minor_resolved 的止損就是一般止損：原因照「觸及止損價」，不是任何「判定」的說法
    assert "出場原因：觸及止損價" in T.format_exit(events[2], exit_snap()).split("\n")
    # 進場的判定特徵（鍵與值）也不印
    text = T.format_entry(entry(), entry_snap())
    for word in ("ret2h", "volr", "cpos", "0.1987", "inf", "Infinity", "0.3123"):
        assert word not in text, word


def _labels(text):
    out = []
    for line in text.split("\n")[1:]:
        if line and not line.startswith((W, "※")):
            out.append(line.split(T.LABEL_SEPARATOR, 1)[0])
    return out


def test_ac1_take_profit_and_stop_loss_are_symmetric():
    tp = T.format_exit(EXITS["tp"], exit_snap())
    labels = ["策略", "交易對", "進場價", "出場價", "持倉時間", "名目報酬", "出場原因", "出場時間"]
    assert _labels(tp) == labels, _labels(tp)
    for name in ("sl", "gap", "same_bar", "minor_missing", "minor_missing_only"):
        sl = T.format_exit(EXITS[name], exit_snap())
        assert _labels(sl) == labels, (name, _labels(sl))
        assert len(tp.split("\n")) == len(sl.split("\n")), name
        assert sl.startswith("❌ 止損出場"), name
    tpd, gapd = (T.format_exit(EXITS[n], exit_snap(), delayed=True) for n in ("tp_delayed", "gap_delayed"))
    assert _labels(tpd) == _labels(gapd) == labels and len(tpd.split("\n")) == len(gapd.split("\n"))
    assert tp.startswith("✅ 止盈出場")


def test_ac1_strategy_labels_keys_equal_strategies():
    assert set(config.STRATEGY_LABELS) == set(config.STRATEGIES), (config.STRATEGY_LABELS, config.STRATEGIES)
    assert config.STRATEGY_LABELS["s4"] == "策略4" and config.STRATEGY_LABELS["s5"] == "策略5"
    assert config.execution_params()["STRATEGY_LABELS"] == config.STRATEGY_LABELS


def test_ac1_issue_time_matches_signal_id_for_both_strategies():
    """signal_id 用 A3 的 make_signal_id() 產生；「發出時間」= bar_open_ms + A3 規格的主週期，兩者必須一致。"""
    specs = nt.build_specs()
    assert set(specs) == set(config.STRATEGIES)
    for strategy, spec in specs.items():
        for close_ms in (CLOSE_1535, CLOSE_1535 + 7 * 24 * 60 * MIN + 55 * MIN):   # 另一天 16:30
            bar_open = close_ms - spec.main_ms
            sid = nt.make_signal_id(spec, "ACE_USDT_PERP", close_ms)
            e = entry(signal_id=sid, strategy=strategy, bar_open_ms=bar_open)
            lines = T.format_entry(e, entry_snap(close_ms=bar_open + spec.main_ms)).split("\n")
            assert "發出時間：" + T.signal_id_time_text(sid) in lines, (strategy, lines, sid)
            assert "交易對：ACE" in lines, lines
    # 主週期不是寫死的：取自 config 的 K 棒週期（A3 的同一個來源）
    assert specs["s4"].main_interval == config.KLINE_INTERVAL
    assert specs["s5"].main_interval == config.S5_KLINE_INTERVAL


def test_ac1_trading_pair_comes_from_signal_id():
    assert T.coin_from_signal_id("#ACE-S4-20260918-1535") == "ACE"
    assert T.coin_from_signal_id("#1000PEPE-S5-20260918-0005") == "1000PEPE"
    assert T.coin_from_signal_id("#AB-CD-S4-20260918-1535") == "AB-CD"        # 幣名裡的「-」
    assert T.coin_from_signal_id("ACE-20260918") is None
    # 與 A3 同一套規則：幣名 = symbol 去掉 _USDT_PERP（由 A3 的 make_signal_id 套用，這裡只拆回來）
    spec = nt.build_specs()["s4"]
    for symbol in ("ACE_USDT_PERP", "1000PEPE_USDT_PERP", "WEIRD_USDT", "_USDT_PERP"):
        sid = nt.make_signal_id(spec, symbol, CLOSE_1535)
        e = entry(signal_id=sid, symbol=symbol)
        coin = T.coin_from_signal_id(sid)
        assert "交易對：%s" % coin in T.format_entry(e, entry_snap()).split("\n")
    # signal_id 不是 A3 的格式（不該發生）：顯示 symbol 原文，不自己去後綴
    e = entry(signal_id="legacy-id", symbol="ACE_USDT_PERP")
    assert "交易對：ACE_USDT_PERP" in T.format_entry(e, entry_snap()).split("\n")


def test_ac1_longest_message_is_far_below_the_limit():
    coin = "VERYLONGCOINNAME" * 4
    sid = "#%s-S4-20260918-1535" % coin
    x = exit_ev(SL, 0.0000123456789, 60 * 24 * 40 + 59, signal_id=sid, symbol=coin + "_USDT_PERP",
                entry_price=0.0000098765432, gap_open=True, same_bar_both=True, recovered=True)
    e = entry(signal_id=sid, symbol=coin + "_USDT_PERP", price=0.0000098765432, tp=0.0000091, sl=0.0000104)
    longest = max(utf16_units(T.format_exit(x, exit_snap(decimals=12), delayed=True)),
                  utf16_units(T.format_entry(e, entry_snap(None, decimals=12))))
    assert longest < 1024 < config.TG_MAX_MESSAGE_CHARS, longest


def test_ac1_sample_case_9_matches_the_user_confirmed_layout():
    """--sample 第 9 則（延遲止盈）與使用者 AC-8 確認過的樣子一致。數字（進場 / 出場價、名目報酬）由樣本事件推導，
    不寫死：樣本的止盈價來自 A3 的 levels()，也就是策略參數。"""
    from live import a_channel_push as P
    label, x, snap, delayed = P.sample_cases()[8]
    d = snap["price_decimals"]
    want = "\n".join([
        "✅ 止盈出場  #ACE-S5-20260918-1536",
        "",
        W + " 延遲發布：本則晚於實際出場時間送出，實際出場時間見下方「出場時間」",
        "",
        "策略：策略5",
        "交易對：ACE",
        "進場價：" + T.format_price(x.entry_price, d),
        "出場價：" + T.format_price(x.exit_price, d),
        "持倉時間：1d 2h 10m",
        "",
        "名目報酬：" + T.format_signed_pct((x.entry_price - x.exit_price) / x.entry_price),
        "出場原因：觸及止盈價",
        "",
        "出場時間：2026-09-19 17:46",
        "",
        "※ 策略名目結果，未計手續費與資金費率",
        "※ 實際損益依個人進場價與費率而異",
    ])
    assert delayed is True
    _eq(T.render(x, snap, delayed=True), want, "--sample 第 9 則")
    msgs = P.sample_messages()
    assert len(msgs) == 9 and msgs[8][1].endswith(want) and msgs[8][1].startswith("[測試] 9/9：")


# ============================== 小工具 ==============================
def test_price_formatting_rounds_half_up_without_scientific_notation():
    assert T.format_price(0.142718, 5) == "0.14272"
    assert T.format_price(0.142715, 5) == "0.14272"          # 剛好一半 → 進位（ROUND_HALF_UP）
    assert T.format_price(0.142714999, 5) == "0.14271"
    assert T.format_price(0.0000071234, 10) == "0.0000071234"
    assert T.format_price(64069.44, 1) == "64069.4" and T.format_price(64069.45, 1) == "64069.5"
    assert T.format_price(2.5, 0) == "3" and T.format_price(0.153, 5) == "0.15300"


def test_price_decimals_fallback_uses_significant_digits():
    assert T.price_decimals_for(0.15346, 6) == 6
    assert T.price_decimals_for(64069.4, 6) == 1
    assert T.price_decimals_for(0.0000071234, 6) == 11
    assert T.price_decimals_for(123456789.0, 6) == 0
    assert T.price_decimals_for(1e-20, 6) == 12                # 上限


def test_percent_and_duration_helpers():
    assert T.format_signed_pct(-0.04) == "-4.0%" and T.format_signed_pct(0.05) == "+5.0%"
    assert T.format_signed_pct(-0.0752) == "-7.5%" and T.format_signed_pct(0.00001) == "0.0%"
    assert T.format_plain_pct(0.02) == "2%" and T.format_plain_pct(0.1 / 3) == "3.3%"
    assert T.format_plain_pct(0.015) == "1.5%" and T.format_plain_pct(0.066666) == "6.7%"
    assert T.format_duration(25 * MIN) == "25m" and T.format_duration(85 * MIN) == "1h 25m"
    assert T.format_duration(120 * MIN) == "2h 0m" and T.format_duration(24 * 60 * MIN + 5 * MIN) == "1d 0h 5m"
    assert T.format_duration(59_999) == "0m"


def test_position_advice_rule():
    assert T.position_advice(0.02, 50, 125) == (50, 0.02, True)
    assert T.position_advice(0.02, 50, 25)[:2] == (25, 0.04)
    assert T.position_advice(0.02, 50, 20)[:2] == (20, 0.05)
    assert T.position_advice(0.02, 50, None) == (50, 0.02, False)
    assert T.position_advice(0.02, 50, 0)[2] is False and T.position_advice(0.02, 50, True)[2] is False


def test_formatter_is_pure_and_rejects_wrong_event_types():
    snap = entry_snap()
    e = entry()
    assert T.format_entry(e, snap) == T.format_entry(e, snap)
    for fn, bad in ((T.format_entry, EXITS["tp"]), (T.format_exit, e)):
        try:
            fn(bad, snap)
        except TypeError:
            pass
        else:
            raise AssertionError("%s 收錯型別應該拋 TypeError" % fn.__name__)


def test_module_is_pure_no_clock_no_network_imports():
    import ast
    src = open(os.path.join(REPO_ROOT, "live", "a_channel_text.py"), encoding="utf-8").read()
    tops = set()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Import):
            tops |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            tops.add((node.module or "").split(".")[0])
    assert tops <= {"re", "datetime", "decimal", "live"}, tops
    for word in ("time.time", "datetime.now", "requests", "market_static", "strategy", "notional_tracker"):
        code = "\n".join(line for line in src.split("\n") if not line.strip().startswith("#"))
        body = code.split('"""', 2)[2]          # 模組 docstring 之後的程式碼
        assert word not in body, word


# ============================== 不用 pytest 也能跑 ==============================
if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    attempts = []
    for name, fn in tests:
        try:
            with OfflineCage() as cage:
                fn()
            attempts.extend(cage.attempts)
            print(f"PASS  {name}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL  {name}: {type(e).__name__}: {e}")
    if attempts:
        failed += 1
        print(f"FAIL  <runner>: 測試期間有連網企圖 {attempts}")
    print(f"\n{len(tests) - failed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
