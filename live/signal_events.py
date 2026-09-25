# -*- coding: utf-8 -*-
"""
live.signal_events — 訊號事件的型別（進場 / 出場）
==================================================
A3（名目部位追蹤）產生這兩種事件，經 live.bus 送給所有訂閱者（A 頻道推播、日後的
webhook 下單端……）。這裡只定義「事件長什麼樣、什麼樣的事件是壞的」，不做送達。

  EntryEvent   進場：某策略在某個幣發出做空訊號
  ExitEvent    出場：該筆名目部位止盈或止損

兩者共用 SignalEvent 的五個欄位（strategy / signal_id / symbol / direction / created_ms）。
SignalEvent 本身不可以直接建構，也不能拿去 publish。

──────────────────────────────────────────────────────────────────────
欄位約定
──────────────────────────────────────────────────────────────────────
  *_ms       UTC epoch 毫秒，int。numpy / pandas 的整數（例如 K 棒 time 欄）可以直接給，
             存進來會轉成 Python int；float 一律拒收（毫秒不該有小數，有就是單位弄錯了）。
             合理範圍 [10**12, 10**13)，也就是 2001–2286 年 —— 這不是業務規則，是用來抓
             「傳了秒 / 微秒」這種單位錯誤：秒數當毫秒看會落在 1970 年 1 月。
  *_price    有限的正實數，存成 float。NaN、inf、0、負數、bool、字串、Decimal 都拒收。
  strategy   必須在 live.config.STRATEGIES 裡（建構當下才讀，不在 import 時綁死）。
             同一個幣兩個策略可以同時持倉，所以訂閱者一律用 strategy 區分，不能只看 symbol。
  signal_id  非空字串，格式由 A3 決定。注意：同一個幣、同一根 K 棒，s4 與 s5 可能同時發
             訊號，signal_id 要跨策略唯一（建議把 strategy 編進去）。這裡無從檢查唯一性。
  created_ms 事件產生時刻，**由產生者傳入**，本模組不讀時鐘。產生者（A3）本來就要有自己
             的時鐘（可注入、可在測試裡換成假時鐘），事件是純值，建構不該偷看牆上時鐘。
  direction  目前只有 DIRECTION_SHORT。出場事件也帶方向：訂閱者（例如 webhook 要送「平空單」）
             不必回頭查進場事件就知道要平哪一邊。
  reason     出場原因，EXIT_TAKE_PROFIT 或 EXIT_STOP_LOSS。

事件本身**不計算**任何價格或報酬。止盈價 / 止損價由 A3 依 strategy/ 的出場參數算好放進來，
本模組不 import 策略參數。唯一的價格關係檢查是「止盈價與止損價在訊號價的哪一側」：做空時
必須 止盈價 < 訊號價 < 止損價。這不是計算，是擋掉「止盈止損放反了」這種產生端的 bug。

──────────────────────────────────────────────────────────────────────
不可變
──────────────────────────────────────────────────────────────────────
同一個事件物件會依序交給每一個訂閱者，前一個訂閱者不可能改掉後一個看到的內容：
  ・dataclass(frozen=True)：欄位不能重設
  ・kw_only=True：只能用關鍵字建構。好幾個欄位都是價格，位置參數一錯位（止盈跟止損對調）
    型別完全一樣、不會有任何錯誤，所以乾脆不給用位置參數
  ・features（判定特徵）建構時複製一份再包成唯讀映射（types.MappingProxyType），值只收
    純量（None / bool / str / 整數 / 實數），不收 list、dict 這類可變容器 —— 否則「唯讀」
    只唯讀到第一層。呼叫端事後改原本那個 dict，不會影響已經建好的事件。
    numpy 純量（含 numpy.bool_）會轉成 Python 的 bool / int / float；本模組不 import numpy，
    numpy.bool_ 是用鴨子型別認的（dtype.kind == "b" 且 0 維），布林陣列照樣拒收。
    NaN 轉成 None：NaN 在這裡的意思就是「沒有值」，跟 dry run 快照把 NaN 記成 None 的慣例
    一樣；留著 NaN 的話，同一個事件 to_dict() 再建回來會跟原本不相等（NaN != NaN）。
    ±inf 照樣保留：它是有意義的值（例如前 24 小時量為 0 時 volr 就是 inf），不可以誤殺。

建構時任何一個欄位不合法就拋 TypeError（型別錯）或 ValueError（值錯），缺欄位則是 dataclass
本身拋的 TypeError。寧可在產生端當場死掉，也不要把壞事件送出去。

──────────────────────────────────────────────────────────────────────
複製與序列化
──────────────────────────────────────────────────────────────────────
事件完全不可變（欄位 frozen、features 唯讀且只有純量），所以不需要複製：copy.copy() 與
copy.deepcopy() 都直接回傳同一個物件（放在 list / dict 裡整包 deepcopy 也一樣可以）。

序列化一律用 event.to_dict()（features 會變回一般 dict，可以直接 json.dumps）：
  ・**不要用 dataclasses.asdict()**：它會對每個欄位值各自 deepcopy，mappingproxy 不能被
    deepcopy，會直接拋 TypeError
  ・**不支援 pickle**（同樣卡在 mappingproxy）。匯流排只在行程內傳遞，不需要 pickle；要落地
    或跨行程就用 to_dict()
  ・features 可能含 ±inf，json.dumps 預設會寫成非標準 JSON 的 Infinity。要送嚴格 JSON 的訂閱者
    （例如 webhook）自己決定怎麼轉；用 json.dumps(..., allow_nan=False) 至少會當場拋錯，不會
    悄悄送出對方解析不了的內容
"""

import math
import numbers
import operator
import types
from collections.abc import Mapping
from dataclasses import dataclass, field, fields

from live import config

# 方向 -> 止盈價是否在訊號價下方。DIRECTIONS 由這張表導出：日後要加做多，一定得在這裡
# 寫清楚止盈在哪一側，不會出現「方向合法、價格關係卻沒人檢查」的空窗。
DIRECTION_SHORT = "short"
_TAKE_PROFIT_BELOW_PRICE = {DIRECTION_SHORT: True}
DIRECTIONS = tuple(_TAKE_PROFIT_BELOW_PRICE)

EXIT_TAKE_PROFIT = "take_profit"
EXIT_STOP_LOSS = "stop_loss"
EXIT_REASONS = (EXIT_TAKE_PROFIT, EXIT_STOP_LOSS)

# UTC 毫秒的合理範圍：2001-09-09 ~ 2286-11-20。只用來抓單位錯誤，見模組說明。
_MIN_EPOCH_MS = 10 ** 12
_MAX_EPOCH_MS = 10 ** 13


# ============================== 欄位檢查 ==============================
def _error(exc_type, event, name, problem, value):
    return exc_type("%s.%s %s，收到 %r" % (type(event).__name__, name, problem, value))


def _set(event, name, value):
    """frozen dataclass 在 __post_init__ 裡寫回正規化後的值，只能繞過 __setattr__。"""
    object.__setattr__(event, name, value)


def _text(event, name, value):
    if not isinstance(value, str):
        raise _error(TypeError, event, name, "必須是字串", value)
    if not value or value != value.strip():
        raise _error(ValueError, event, name, "不可為空字串，前後也不可有空白", value)
    return str(value)


def _choice(event, name, value, allowed):
    value = _text(event, name, value)
    if value not in allowed:
        raise _error(ValueError, event, name, "必須是 %s 其中之一" % (tuple(allowed),), value)
    return value


def _price(event, name, value):
    # bool 是 int 的子類，True 會被當成 1.0 —— 價格欄位收到 bool 一定是呼叫端傳錯了
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise _error(TypeError, event, name, "必須是實數（int / float）", value)
    try:
        number = float(value)
    except OverflowError:
        raise _error(ValueError, event, name, "超出 float 範圍", value) from None
    # `x > 0` 擋不掉 inf，NaN 則是任何比較都 False；兩個都要明確擋
    if not (math.isfinite(number) and number > 0):
        raise _error(ValueError, event, name, "必須是有限的正數", value)
    return number


def _epoch_ms(event, name, value):
    if isinstance(value, bool):
        raise _error(TypeError, event, name, "必須是整數的 UTC 毫秒", value)
    try:
        number = operator.index(value)      # 收 int 與 numpy 整數，float 直接 TypeError
    except TypeError:
        raise _error(TypeError, event, name, "必須是整數的 UTC 毫秒", value) from None
    if not _MIN_EPOCH_MS <= number < _MAX_EPOCH_MS:
        raise _error(ValueError, event, name,
                     "不像 UTC 毫秒（合理範圍是 2001–2286 年；秒 / 微秒是不是弄錯了）", value)
    return number


def _is_numpy_bool(value):
    """numpy.bool_（或 0 維的布林陣列）。用鴨子型別判斷，本模組不 import numpy。

    numpy.bool_ 不是 bool 的子類，也沒有登記成 numbers.Integral，不特別認就會被當成非純量拒收。
    要求 0 維：布林陣列（shape 不是 ()）是容器，照樣拒收。
    """
    dtype = getattr(value, "dtype", None)
    return getattr(dtype, "kind", None) == "b" and getattr(value, "shape", None) == ()


def _feature_value(event, key, value):
    if value is None or isinstance(value, bool):
        return value
    if _is_numpy_bool(value):
        return bool(value)
    if isinstance(value, str):
        return str(value)
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        try:
            number = float(value)
        except OverflowError:
            raise _error(ValueError, event, "features[%r]" % key, "超出 float 範圍", value) from None
        # NaN = 沒有值，存成 None（見模組說明）；±inf 是有意義的值，照樣保留
        return None if math.isnan(number) else number
    raise _error(TypeError, event, "features[%r]" % key,
                 "只收純量（None / bool / str / 整數 / 實數），可變容器會破壞唯讀", value)


def _features(event, name, value):
    if not isinstance(value, Mapping):
        raise _error(TypeError, event, name, "必須是 dict（或其他 Mapping）", value)
    frozen = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key:
            raise _error(TypeError, event, name, "的鍵必須是非空字串", key)
        frozen[key] = _feature_value(event, key, item)
    if not frozen:
        raise _error(ValueError, event, name, "不可為空（判定特徵是訊號的依據）", value)
    return types.MappingProxyType(frozen)


# ============================== 事件型別 ==============================
@dataclass(frozen=True, kw_only=True)
class SignalEvent:
    """進場 / 出場事件的共同欄位。只當基底用，不可以直接建構。"""

    strategy: str
    signal_id: str
    symbol: str
    direction: str
    created_ms: int

    def __post_init__(self):
        if type(self) is SignalEvent:
            raise TypeError("SignalEvent 只是共同欄位的基底，請建構 EntryEvent 或 ExitEvent")
        _set(self, "strategy", _choice(self, "strategy", self.strategy, config.STRATEGIES))
        _set(self, "signal_id", _text(self, "signal_id", self.signal_id))
        _set(self, "symbol", _text(self, "symbol", self.symbol))
        _set(self, "direction", _choice(self, "direction", self.direction, DIRECTIONS))
        _set(self, "created_ms", _epoch_ms(self, "created_ms", self.created_ms))

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        # 完全不可變，複本跟原本無從區分；回傳自己，也避開 mappingproxy 不能 deepcopy 的問題
        return self

    def to_dict(self):
        """欄位名 -> 值的一般 dict（features 變回一般 dict），可以直接 json.dumps。

        `EntryEvent(**e.to_dict()) == e` 成立。不要用 dataclasses.asdict()，見模組說明。
        features 裡可能有 ±inf，json.dumps 預設會寫成非標準的 Infinity；要送嚴格 JSON 的
        訂閱者自己處理（NaN 在建構時已經轉成 None，不會出現）。
        """
        out = {}
        for f in fields(self):
            value = getattr(self, f.name)
            out[f.name] = dict(value) if isinstance(value, types.MappingProxyType) else value
        return out


@dataclass(frozen=True, kw_only=True)
class EntryEvent(SignalEvent):
    """進場事件。

    bar_open_ms        訊號所在 K 棒的開盤時刻（UTC 毫秒）
    signal_price       訊號價
    take_profit_price  止盈價（A3 依 strategy/ 的出場參數算好）
    stop_loss_price    止損價（同上）
    features           判定特徵，唯讀映射；兩個策略的內容不同，本模組不規定有哪些鍵
    """

    bar_open_ms: int
    signal_price: float
    take_profit_price: float
    stop_loss_price: float
    # mappingproxy 不可雜湊，不列入 hash；相等比較照樣會比 features
    features: Mapping = field(hash=False)

    def __post_init__(self):
        super().__post_init__()
        _set(self, "bar_open_ms", _epoch_ms(self, "bar_open_ms", self.bar_open_ms))
        for name in ("signal_price", "take_profit_price", "stop_loss_price"):
            _set(self, name, _price(self, name, getattr(self, name)))
        price, tp, sl = self.signal_price, self.take_profit_price, self.stop_loss_price
        if _TAKE_PROFIT_BELOW_PRICE[self.direction]:
            ok, rule = tp < price < sl, "止盈價 < 訊號價 < 止損價"
        else:
            ok, rule = sl < price < tp, "止損價 < 訊號價 < 止盈價"
        if not ok:
            raise ValueError("EntryEvent 方向 %s 必須 %s，收到 止盈 %r、訊號 %r、止損 %r"
                             % (self.direction, rule, tp, price, sl))
        _set(self, "features", _features(self, "features", self.features))


@dataclass(frozen=True, kw_only=True)
class ExitEvent(SignalEvent):
    """出場事件。

    reason       EXIT_TAKE_PROFIT / EXIT_STOP_LOSS
    exit_price   出場價
    entry_price  進場價
    opened_ms    名目部位的開倉時刻（UTC 毫秒，取法由 A3 定義）
    closed_ms    名目部位的平倉時刻（UTC 毫秒），不可早於 opened_ms
    """

    reason: str
    exit_price: float
    entry_price: float
    opened_ms: int
    closed_ms: int

    def __post_init__(self):
        super().__post_init__()
        _set(self, "reason", _choice(self, "reason", self.reason, EXIT_REASONS))
        for name in ("exit_price", "entry_price"):
            _set(self, name, _price(self, name, getattr(self, name)))
        for name in ("opened_ms", "closed_ms"):
            _set(self, name, _epoch_ms(self, name, getattr(self, name)))
        if self.closed_ms < self.opened_ms:
            raise ValueError("ExitEvent.closed_ms 不可早於 opened_ms，收到 opened %r、closed %r"
                             % (self.opened_ms, self.closed_ms))


# live.bus 只接受這兩種（精確型別，不收子類）
EVENT_TYPES = (EntryEvent, ExitEvent)
