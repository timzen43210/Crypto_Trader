# -*- coding: utf-8 -*-
"""
live.a_channel — A 頻道的執行入口：A1 / A5（原始訊號）→ A3（名目部位追蹤）→ A2 匯流排 → 訂閱者
=====================================================================================================
把下面這些元件接起來，跑到 --duration 秒或 Ctrl+C 為止：

    B3  live.logsetup.setup()            日誌與未攔截例外
    B5  live.market_static               標的池（由 A1 的 MarketUniverse 載入與刷新）
        live.rest_gate.shared_gate()     整個行程共用的 REST 閘門（A1 與 A3 都走它）
    B4′ live.store                       A3 的工作執行緒自己開（一個 Store 只在建立它的執行緒用）
    A2  live.bus.SignalBus               訂閱者：EventLog（事件寫進 INFO 日誌，加 --events-jsonl 時另外寫 jsonl），
                                         加 --push-tg 時再掛 T2（live.a_channel_push.ChannelPusher，名稱 tg_a_channel）
    T2  live.a_channel_push              A 頻道推播：handler 只寫 outbox（runtime/db/a_channel_outbox.sqlite3）就返回，
        live.tg_channel.ChannelSender    自己的工作執行緒依 outbox 交給發送器、把實際結果記回 outbox
    R-A live.a_channel_report_push       A 頻道報表（加 --push-tg 時）：日報 / 10 日報 / 月報，自己的執行緒
        live.a_channel_report            a-channel-report；唯讀讀 live.sqlite3 與 outbox 組字，經 T2 的 ChannelSender 發送，
                                         每一期的狀態記在發送紀錄（runtime/db/a_channel_reports.sqlite3）
    A3  live.notional_tracker            名目部位追蹤（自己的執行緒）
    A1  live.signal_feed.SignalFeed      策略4；on_result = A3 的回呼（只排佇列，立刻返回）
    A5  live.s5_feed.S5Feed              策略5（自己的執行緒）；與 A1 共用閘門、價格緩衝、標的池，
                                         訊號送進 A3 的 submit_signal(strategy="s5")（只排佇列，立刻返回）

**必須明確帶 --push-tg 才推播**（比照 T1′ 的 --smoke）。不帶時行為與 T2 之前完全相同：只有 EventLog，
不開 outbox、不建報表的發送紀錄、沒有報表執行緒、不讀 TG 密鑰 —— 開發機的實跑（例如 A1 的長時間冒煙）不可以
把訊息發進正式頻道。
帶 --push-tg 時，**在 A3 啟動之前**依序做好（A3 的重啟補發會在啟動時就發布事件，T2 必須先接好）：
  1. ChannelSender.start()：缺密鑰 → 記 ERROR、exit 1，A3 / A1 都不啟動
  2. ChannelPusher.start()：建 outbox、記一行 outbox 現況（待送幾則、各狀態累計）、啟動 T2 工作執行緒
  3. 訂閱兩種事件（EventLog 之後），接線自檢會列出 T2
  4. 主週期取自 A3 的策略規格（pusher.set_specs(tracker.specs)），再 tracker.start()
  5. A3 wait_ready 成功之後、A1 之前：ChannelReporter.start()（R-A；建發送紀錄、記一行現況、啟動報表執行緒）。
     建立或啟動失敗 → 記 ERROR、exit 1，A1 / A5 不啟動（已啟動的 A3 / T2 照 A5 啟動失敗的方式收掉）
關閉順序：A5（join）→ A1 → A3（join）→ R-A 報表（停止交出新的一則 → 記回已回報的結果 → 結束執行緒）
→ T2（停止派送 → ChannelSender.stop() → 記回結果 → 結束工作執行緒）。
訊號的產生者（A5、A1）都停了才停 A3，A3 停了才停報表與 T2；報表用 T2 的發送器，所以先停報表、再停 T2。
沒送完的留在 outbox，記 WARNING 寫明幾則，下次啟動再送；報表在途的那一則若結果沒記回來，下次啟動重送同一段文字。
報表執行緒執行中意外結束 → 記 ERROR、不自動重啟（A1 / A5 / A3 / T2 照跑），結束時 exit 1。

啟動時先做接線自檢：印出每種事件有幾個訂閱者（有任何一種是 0 就 exit 1，不跑）。A3 的工作執行緒開好
資料庫之後才啟動 A1（開不了資料庫就 exit 1）；A3 的重啟復原在自己的執行緒裡做，期間 A1 / A5 交來的訊號
先排在佇列裡。

A5 在 A1 建好之後建立（要用 A1 的 gate / buffer / universe），在 A1 的主迴圈開始之前啟動（A1 的 run() 會阻塞；
標的池在 run() 裡才載入，A5 在那之前看到空的標的池只是不篩、不取數）。A5 建立或啟動失敗 → 記 ERROR、exit 1，
**不會只跑策略4**。A5 的執行緒執行中意外結束 → 記 ERROR、不自動重啟（A1 / A3 照跑），結束時 exit 1。

事件去重約定：A3 採 at-least-once 發布，**以 (signal_id, 事件種類) 去重**，詳見 live.signal_events 的模組說明。
T2 以 outbox 的 UNIQUE (signal_id, 事件種類) 去重（跨重啟有效）；EventLog 只記錄，不去重（重送會看到兩行，這是預期的）。

不加進 `python -m live`（那是環境冒煙檢查，不放業務邏輯）。

執行：
    python -m live.a_channel --duration 3600 [--events-jsonl [DIR]] [--push-tg]
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
from live.s5_feed import S5Feed
from live.signal_feed import SignalFeed, UniverseUnavailable, _SafeArgumentParser, check_record_dir

logger = logging.getLogger(__name__)

# A1（live.signal_feed）只產生策略4 的原始訊號。策略5 的原始訊號由 A5（live.s5_feed）直接送
# NotionalTracker.submit_signal(strategy="s5")。
A1_STRATEGY = "s4"

EVENT_LOG_SUBSCRIBER = "event_log"


class EventLog:
    """一定會掛的匯流排訂閱者（與 --push-tg 的 T2 並存）：每個事件寫一行 INFO，給了目錄就另外寫 jsonl（UTF-8，一行一個事件）。

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


def start_push(bus, *, sender_factory=None, pusher_factory=None):
    """--push-tg：ChannelSender.start() → ChannelPusher.start() → 訂閱。回傳 pusher；不能啟動時記 ERROR 回 None。

    缺密鑰時 sender.start() 拋 MissingSecretError（訊息只含環境變數名），這裡記 ERROR 回 None，呼叫端 exit 1。
    只有帶 --push-tg 才會 import / 建立發送器與 outbox：不帶旗標時連 TG 密鑰都不讀。
    """
    from live.a_channel_push import ChannelPusher
    from live.tg_channel import ChannelSender
    sender = (sender_factory or ChannelSender)()
    try:
        sender.start()
    except config.MissingSecretError as e:
        logger.error("--push-tg 需要 Telegram 密鑰：%s 不啟動（A3 / A1 都沒有跑）", e)
        return None
    pusher = (pusher_factory or ChannelPusher)(sender)
    try:
        pusher.start()
    except Exception:  # noqa: BLE001 —— outbox 開不了就不能保證不漏發，不跑
        logger.exception("--push-tg：A 頻道推播的 outbox 開不了，不啟動（A3 / A1 都沒有跑）")
        sender.stop(0)
        return None
    pusher.subscribe(bus)
    return pusher


def start_reporter(sender, *, reporter_factory=None):
    """--push-tg：A3 wait_ready 之後、A1 之前啟動 R-A 報表（ChannelReporter(sender).start()）。
    回傳 reporter；建立或啟動失敗（例如發送紀錄開不了）記 ERROR 回 None，呼叫端 exit 1。"""
    from live.a_channel_report_push import ChannelReporter
    reporter = None
    try:
        reporter = (reporter_factory or ChannelReporter)(sender)
        reporter.start()
    except Exception:  # noqa: BLE001 —— 發送紀錄開不了就不能保證報表不漏發、不重發，不跑
        logger.exception("--push-tg：A 頻道報表（R-A）建立或啟動失敗，不啟動 A1 / A5")
        if reporter is not None:
            try:
                reporter.stop()
            except Exception:  # noqa: BLE001
                logger.exception("A 頻道報表啟動失敗後，stop() 也拋出例外")
        return None
    return reporter


def stop_reporter(reporter):
    """停 R-A 報表執行緒（不停發送器）並記一行總結。回傳 False = 沒有在時限內結束、或執行中意外結束過。"""
    ok = bool(reporter.stop())
    report_stats = reporter.stats()
    logger.info("A 頻道報表總結：%s", report_stats)
    if report_stats.get("worker_crashed"):
        logger.error("A 頻道報表執行緒在執行中意外結束（見上面的 ERROR），這次運作期間之後的報表沒有發送")
        ok = False
    return ok


def default_s5_feed(feed, tracker, **kw):
    """A5：與 A1（feed）共用閘門、價格緩衝、標的池、時鐘，訊號送進 A3（tracker.submit_signal）。kw 給測試覆寫。"""
    return S5Feed(gate=feed.gate, buffer=feed.buffer, universe_fn=feed.universe, tickers_fn=feed.latest_tickers,
                  submit_fn=tracker.submit_signal, clock=feed.clock, **kw)


def main(argv=None, *, setup_logging=True, tracker_factory=None, feed_factory=None, sender_factory=None,
         pusher_factory=None, s5_feed_factory=None, reporter_factory=None):
    """python -m live.a_channel 的進入點。回傳 exit code。

    tracker_factory(bus) / feed_factory(on_result) / s5_feed_factory(feed, tracker)：測試注入用（正式執行不給，
    用共用閘門建真的 A3 / A1，A5 用 default_s5_feed）。
    sender_factory() / pusher_factory(sender)：--push-tg 時的測試注入（正式執行不給，用 ChannelSender() /
    ChannelPusher(sender)）。
    reporter_factory(sender)：--push-tg 時 R-A 報表的測試注入（正式執行不給，用 ChannelReporter(sender)；
    sender 是 T2 的發送器）。
    """
    parser = _SafeArgumentParser(
        prog="python -m live.a_channel",
        description="A 頻道：A1 原始訊號 → A3 名目部位追蹤 → 事件匯流排（加 --push-tg 才推播到 Telegram）。連網實跑。")
    parser.add_argument("--duration", type=float, default=None, metavar="SECONDS",
                        help="跑幾秒後正常結束（不給就一直跑到 Ctrl+C）")
    parser.add_argument("--events-jsonl", nargs="?", const=config.A3_EVENTS_RECORD_DIR, default=None,
                        metavar="DIR", help="事件另外寫成 jsonl；只給旗標不給目錄時寫到 %s"
                                            % config.A3_EVENTS_RECORD_DIR)
    parser.add_argument("--push-tg", action="store_true",
                        help="把進場 / 出場訊息與日報 / 10 日報 / 月報推播到 Telegram A 頻道（需要兩個 TG 密鑰；"
                             "outbox 在 %s，報表發送紀錄在 %s）。不帶就只記錄事件，不讀密鑰、不建 outbox 與發送紀錄"
                             % (config.A_CHANNEL_OUTBOX_DB_PATH, config.A_CHANNEL_REPORT_DB_PATH))
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
    push = None
    if args.push_tg:
        # T2 必須在 A3 啟動之前接好：A3 的重啟補發一啟動就會發布事件
        push = start_push(bus, sender_factory=sender_factory, pusher_factory=pusher_factory)
        if push is None:
            event_log.close()
            return 1
    ok, text = wiring_report(bus)
    logger.info("A 頻道啟動：duration %s、事件 jsonl %s、日誌 %s", args.duration, event_log.path or "不寫",
                log_path or "（未設定）")
    if push is not None:
        logger.info("A 頻道推播：已啟用（--push-tg），outbox %s", push.path)
    if not ok:
        logger.error("接線自檢失敗：%s。有事件沒有任何訂閱者，不啟動", text)
        if push is not None:
            push.shutdown()
        event_log.close()
        return 1
    logger.info("接線自檢：%s", text)

    tracker = (tracker_factory or default_tracker)(bus)
    for spec in tracker.specs.values():
        logger.info("A3 規格 %s：主週期 %s、監控週期 %s、止盈 %s、止損 %s、冷卻 %d 根（全部取自 strategy/）",
                    spec.strategy, spec.main_interval, spec.monitor_interval, spec.take_profit, spec.stop_loss,
                    spec.cooldown_bars)
    if push is not None:
        push.set_specs(tracker.specs)      # 進場訊息的「發出時間」用 A3 同一份規格的主週期
    tracker.start()
    if not tracker.wait_ready(config.A3_STORE_READY_TIMEOUT_SECONDS):
        logger.error("A3 的資料庫沒有在 %s 秒內開好（%s），不啟動 A1", config.A3_STORE_READY_TIMEOUT_SECONDS,
                     tracker.ready_error)
        tracker.stop()
        tracker.join(config.A3_JOIN_TIMEOUT_SECONDS)
        if push is not None:
            push.shutdown()
        event_log.close()
        return 1
    reporter = None
    if push is not None:
        # R-A 報表用 T2 的發送器；A3 開好資料庫之後、A1 之前啟動
        reporter = start_reporter(push.sender, reporter_factory=reporter_factory)
        if reporter is None:
            tracker.stop()
            tracker.join(config.A3_JOIN_TIMEOUT_SECONDS)
            push.shutdown()
            event_log.close()
            return 1
        logger.info("A 頻道報表：已啟用（--push-tg），發送紀錄 %s", reporter.path)

    feed = (feed_factory or default_feed)(tracker.bar_result_handler(A1_STRATEGY))
    s5 = None
    try:
        s5 = (s5_feed_factory or default_s5_feed)(feed, tracker)
        s5.start()
    except Exception:  # noqa: BLE001 —— 不可以悄悄只跑策略4
        logger.exception("策略五資料層（A5）建立或啟動失敗，不啟動（不會只跑策略4）")
        if s5 is not None:
            s5.close()
        feed.close()
        tracker.stop()
        tracker.join(config.A3_JOIN_TIMEOUT_SECONDS)
        if reporter is not None:
            stop_reporter(reporter)
        if push is not None:
            push.shutdown()
        event_log.close()
        return 1
    rc = 0
    try:
        feed.run(duration_s=args.duration)
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，結束")
    except UniverseUnavailable as e:
        logger.error("無法取得標的池，結束：%s", e)
        rc = 1
    finally:
        # 產生者先停（A5 → A1），再停 A3、報表，最後 T2
        if not s5.close():
            rc = rc or 1
        s5_stats = s5.stats()
        logger.info("A5 總結：%s", s5_stats)
        if s5_stats.get("worker_crashed"):
            logger.error("A5 的工作執行緒在執行中意外結束（見上面的 ERROR），這次運作的策略5 訊號不完整")
            rc = rc or 1
        feed.close()
        tracker.stop()
        if not tracker.join(config.A3_JOIN_TIMEOUT_SECONDS):
            logger.error("A3 工作執行緒 %s 秒內沒有結束", config.A3_JOIN_TIMEOUT_SECONDS)
            rc = rc or 1
        if reporter is not None:
            # A3 已停：報表停止交出新的一則、記回已回報的結果（在途的下次啟動重送），再停它用的 T2 發送器
            if not stop_reporter(reporter):
                rc = rc or 1
        if push is not None:
            # A3 已停，不會再有新事件：T2 停止派送 → ChannelSender.stop() → 記回結果（沒送完的留在 outbox）
            push.shutdown()
            sender_stats = getattr(push.sender, "stats", None)
            logger.info("A 頻道推播總結：%s；發送器 %s", push.stats, sender_stats() if callable(sender_stats) else "-")
        logger.info("A3 總結：%s；事件 %d 個%s", tracker.stats, event_log.count,
                    "；REST %s" % gate_box[0].stats() if gate_box else "")
        event_log.close()
    return rc


if __name__ == "__main__":
    # python -m live.a_channel 會把本檔載入成 __main__；從正式的模組名取 main()（同 live.signal_feed 的理由）
    from live.a_channel import main as _main
    raise SystemExit(_main())
