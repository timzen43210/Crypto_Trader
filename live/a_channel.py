# -*- coding: utf-8 -*-
"""
live.a_channel — A 頻道的執行入口：A1（原始訊號）→ A3（名目部位追蹤）→ A2 匯流排 → 訂閱者
===============================================================================================
把下面這些元件接起來，跑到 --duration 秒或 Ctrl+C 為止：

    B3  live.logsetup.setup()            日誌與未攔截例外
    B5  live.market_static               標的池（由 A1 的 MarketUniverse 載入與刷新）
        live.rest_gate.shared_gate()     整個行程共用的 REST 閘門（A1 與 A3 都走它）
    B4′ live.store                       A3 的工作執行緒自己開（一個 Store 只在建立它的執行緒用）
    A2  live.bus.SignalBus               T2 上線前只掛一個訂閱者：EventLog（事件寫進 INFO 日誌，
                                         加 --events-jsonl 時另外寫 jsonl）
    A3  live.notional_tracker            名目部位追蹤（自己的執行緒）
    A1  live.signal_feed.SignalFeed      on_result = A3 的回呼（只排佇列，立刻返回）

啟動時先做接線自檢：印出每種事件有幾個訂閱者（有任何一種是 0 就 exit 1，不跑）。A3 的工作執行緒開好
資料庫之後才啟動 A1（開不了資料庫就 exit 1）；A3 的重啟復原在自己的執行緒裡做，期間 A1 交來的訊號
先排在佇列裡。

事件去重約定（寫給日後的訂閱者，T2 照做）：A3 採 at-least-once 發布，**以 (signal_id, 事件種類) 去重**，
詳見 live.signal_events 的模組說明。本入口的 EventLog 只記錄，不去重（重送會看到兩行，這是預期的）。

不加進 `python -m live`（那是環境冒煙檢查，不放業務邏輯）。

執行：
    python -m live.a_channel --duration 3600 [--events-jsonl [DIR]]
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone

from live import config, logsetup, rest_gate
from live.bus import SignalBus
from live.notional_tracker import NotionalTracker
from live.signal_events import EntryEvent, ExitEvent
from live.signal_feed import SignalFeed, UniverseUnavailable, _SafeArgumentParser, check_record_dir

logger = logging.getLogger(__name__)

# A1（live.signal_feed）只產生策略4 的原始訊號。策略五資料層日後另外接 NotionalTracker.submit_signal()。
A1_STRATEGY = "s4"

EVENT_LOG_SUBSCRIBER = "event_log"


class EventLog:
    """T2 上線前唯一的匯流排訂閱者：每個事件寫一行 INFO，給了目錄就另外寫 jsonl（UTF-8，一行一個事件）。

    handler 必須很快、執行緒安全（live.bus 的規則）：只做格式化與一次本機檔案寫入，有鎖。
    jsonl 的 features 可能含 ±inf，照 Python json 的寫法記成 Infinity（與 live.store 的 features_json 相同）。
    """

    def __init__(self, directory=None, clock_ms=None):
        self._lock = threading.Lock()
        self._clock_ms = clock_ms
        self.path = None
        self._f = None
        self.count = 0
        if directory:
            os.makedirs(directory, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            self.path = os.path.join(directory, "a_channel_events_%s.jsonl" % stamp)
            self._f = open(self.path, "a", encoding="utf-8")

    def __call__(self, event):
        d = event.to_dict()
        kind = type(event).__name__
        if isinstance(event, EntryEvent):
            logger.info("事件 %s %s：%s %s 訊號價 %s 止盈 %s 止損 %s 特徵 %s", kind, event.signal_id, event.strategy,
                        event.symbol, event.signal_price, event.take_profit_price, event.stop_loss_price,
                        dict(event.features))
        else:
            logger.info("事件 %s %s：%s %s %s @ %s（進場 %s）旗標 %s", kind, event.signal_id, event.strategy,
                        event.symbol, event.reason, event.exit_price, event.entry_price, dict(event.features))
        with self._lock:
            self.count += 1
            if self._f is not None:
                rec = {"event": kind, **d}
                if self._clock_ms is not None:
                    rec["logged_ms"] = int(self._clock_ms())
                self._f.write(json.dumps(rec, ensure_ascii=False, allow_nan=True) + "\n")
                self._f.flush()

    def close(self):
        with self._lock:
            if self._f is not None and not self._f.closed:
                self._f.close()


def wiring_report(bus):
    """每種事件的訂閱者名稱。回傳 (是否每種都至少一個, 一行說明)。"""
    subs = {cls.__name__: bus.subscribers(cls) for cls in (EntryEvent, ExitEvent)}
    text = "；".join("%s %d 個訂閱者 %s" % (name, len(s), list(s)) for name, s in subs.items())
    return all(subs.values()), text


def main(argv=None, *, setup_logging=True, tracker_factory=None, feed_factory=None):
    """python -m live.a_channel 的進入點。回傳 exit code。

    tracker_factory(bus) / feed_factory(on_result)：測試注入用（正式執行不給，用共用閘門建真的 A3 / A1）。
    """
    parser = _SafeArgumentParser(
        prog="python -m live.a_channel",
        description="A 頻道：A1 原始訊號 → A3 名目部位追蹤 → 事件匯流排（T2 上線前只記錄事件）。連網實跑。")
    parser.add_argument("--duration", type=float, default=None, metavar="SECONDS",
                        help="跑幾秒後正常結束（不給就一直跑到 Ctrl+C）")
    parser.add_argument("--events-jsonl", nargs="?", const=config.A3_EVENTS_RECORD_DIR, default=None,
                        metavar="DIR", help="事件另外寫成 jsonl；只給旗標不給目錄時寫到 %s"
                                            % config.A3_EVENTS_RECORD_DIR)
    args = parser.parse_args(argv)
    if args.duration is not None and args.duration <= 0:
        parser.error("--duration 必須是正數")
    events_dir = None
    if args.events_jsonl:
        try:
            events_dir = check_record_dir(args.events_jsonl)
        except ValueError as e:
            parser.error(str(e).replace("--record", "--events-jsonl"))

    log_path = logsetup.setup() if setup_logging else None
    gate_box = []

    def gate():
        if not gate_box:
            gate_box.append(rest_gate.shared_gate())
        return gate_box[0]

    def default_tracker(bus):
        return NotionalTracker(bus=bus, gate=gate())

    def default_feed(on_result):
        from live.reconcile import Reconciler
        return SignalFeed(gate=gate(), on_result=on_result, reconciler=Reconciler(gate()))

    bus = SignalBus()
    event_log = EventLog(events_dir)
    bus.subscribe(EntryEvent, event_log, EVENT_LOG_SUBSCRIBER)
    bus.subscribe(ExitEvent, event_log, EVENT_LOG_SUBSCRIBER)
    ok, text = wiring_report(bus)
    logger.info("A 頻道啟動：duration %s、事件 jsonl %s、日誌 %s", args.duration, event_log.path or "不寫",
                log_path or "（未設定）")
    if not ok:
        logger.error("接線自檢失敗：%s。有事件沒有任何訂閱者，不啟動", text)
        event_log.close()
        return 1
    logger.info("接線自檢：%s", text)

    tracker = (tracker_factory or default_tracker)(bus)
    for spec in tracker.specs.values():
        logger.info("A3 規格 %s：主週期 %s、監控週期 %s、止盈 %s、止損 %s、冷卻 %d 根（全部取自 strategy/）",
                    spec.strategy, spec.main_interval, spec.monitor_interval, spec.take_profit, spec.stop_loss,
                    spec.cooldown_bars)
    tracker.start()
    if not tracker.wait_ready(config.A3_STORE_READY_TIMEOUT_SECONDS):
        logger.error("A3 的資料庫沒有在 %s 秒內開好（%s），不啟動 A1", config.A3_STORE_READY_TIMEOUT_SECONDS,
                     tracker.ready_error)
        tracker.stop()
        tracker.join(config.A3_JOIN_TIMEOUT_SECONDS)
        event_log.close()
        return 1

    feed = (feed_factory or default_feed)(tracker.bar_result_handler(A1_STRATEGY))
    rc = 0
    try:
        feed.run(duration_s=args.duration)
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，結束")
    except UniverseUnavailable as e:
        logger.error("無法取得標的池，結束：%s", e)
        rc = 1
    finally:
        feed.close()
        tracker.stop()
        if not tracker.join(config.A3_JOIN_TIMEOUT_SECONDS):
            logger.error("A3 工作執行緒 %s 秒內沒有結束", config.A3_JOIN_TIMEOUT_SECONDS)
            rc = rc or 1
        logger.info("A3 總結：%s；事件 %d 個%s", tracker.stats, event_log.count,
                    "；REST %s" % gate_box[0].stats() if gate_box else "")
        event_log.close()
    return rc


if __name__ == "__main__":
    # python -m live.a_channel 會把本檔載入成 __main__；從正式的模組名取 main()（同 live.signal_feed 的理由）
    from live.a_channel import main as _main
    raise SystemExit(_main())
