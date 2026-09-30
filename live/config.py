# -*- coding: utf-8 -*-
"""
live.config — 設定與密鑰的三層分離
==================================
實盤端會用到的「可調整的東西」分成三層，各有各的所在地，彼此不重疊：

  第一層　策略參數（MIN_RET_2H / MIN_VOL_RATIO / MAX_CLOSE_POS / MAX_TURN24H /
          MIN_TURN24H / COOLDOWN_HOURS）
          唯一來源是 strategy/s4_signal.py 的 DEFAULT_PARAMS。這裡只轉發，不放數值。

  第二層　執行參數（BASE URL、timeout、retries、快取逾時之類）
          就是本檔下面那幾個模組層級常數。進版控、隨程式走。

  第三層　密鑰（Telegram bot token、channel id）
          只從環境變數讀，絕不進版控、絕不寫進 repo 裡任何檔案。

為什麼要分：三者的變更頻率與風險完全不同。策略參數一動就影響回測與實盤的一致性，
要走完整驗證；執行參數幾週才調一次，要經 review 進版控；密鑰根本不該出現在 repo 裡。
混在一起的結果就是「改一個 timeout 也得動到跟策略有關的檔案」，或更糟 —— 為了方便把
token 寫進某個設定檔，然後某天被 commit 上去。

──────────────────────────────────────────────────────────────────────
第一層的規則（後續任務請務必照做）：策略參數不得在 live/ 複製一份
──────────────────────────────────────────────────────────────────────
要拿那六個參數，呼叫 strategy_params()，或直接 import strategy.s4_signal.DEFAULT_PARAMS。
**不可以**在這裡（或 live/ 任何地方）寫 MIN_RET_2H = 0.14 這種常數。

理由不是潔癖：訊號邏輯抽到 strategy/s4_signal.py，整件事就是為了讓回測端與實盤端跑的
是同一份程式、同一組參數，同一份歷史資料逐筆對得起來。一旦 live/ 有一份平行的數值，
兩邊不一致時不會有任何錯誤訊息、不會有任何測試失敗 —— 只會讓實盤訊號悄悄偏離回測，
而回測報表還是漂亮的。等到發現時，中間那段實盤紀錄已經無法解釋了。

日後若真的需要讓實盤用不同於回測預設的參數，做法是「把覆寫值傳進 s4_signal 的函式」
（它的 _params() 本來就收一份 params dict），不是在設定層放一份平行的常數。
目前沒有這個需求，所以也沒有實作覆寫機制。

──────────────────────────────────────────────────────────────────────
第二層的規則：只有常數，沒有設定檔
──────────────────────────────────────────────────────────────────────
不讀 JSON / TOML / YAML / ini，沒有 parser，沒有 schema 驗證，沒有 dev/prod 切換。
這是單租戶、自己維運的系統，參數幾週才動一次，而且每次都該經 review 進版控。引入檔案
格式要換來的是解析錯誤處理、預設值合併、型別轉換、檔案不存在的分支 —— 現在全都不需要。
要改參數就改這個檔案然後 commit。

──────────────────────────────────────────────────────────────────────
第三層的規則：密鑰只從環境變數讀
──────────────────────────────────────────────────────────────────────
不支援 .env、不支援 runtime/secrets.env、不做第二種來源。雲端主機上密鑰由 systemd unit
的 Environment= 或平台的 secret 機制注入環境變數，這是標準做法；多開一個檔案來源，就是
多開一條「不小心 commit 進 git」的路，而那正是這一層要防的事。本機要用就自己 export，
或用一個不在 repo 裡的腳本。

import 本模組不會因為缺密鑰而失敗（見 require_secret 的 docstring）——沒有用到 TG 的元件
在完全沒設任何環境變數的機器上也要能正常跑。
"""

import os

from live.paths import RUNTIME_DIR

# ============================== 第二層：執行參數 ==============================
# 派網 API 的根位址。端點路徑（/api/v1/common/symbols 之類）不放這裡：
# 換掉端點路徑等於換一支 API，那是程式邏輯，不是設定。
PIONEX_BASE_URL = "https://api.pionex.com"

# 單一 HTTP 請求的連線 + 讀取上限（秒）
HTTP_TIMEOUT_SECONDS = 20

# 網路抖動 / 429 / 5xx 的總嘗試次數（不是額外重試次數）
HTTP_RETRIES = 3

# market_static.refresh_if_stale() 的預設逾時（秒）：距上次「成功」刷新超過這個秒數才再打
MARKET_STATIC_STALE_SECONDS = 3600

# ---- 訊號事件（A2，live.signal_events / live.bus）----
# 實盤上線的策略代號，所有訊號事件的 strategy 欄位只能是其中之一。清單只在這裡，
# 事件建構時才讀它（不在 import 時綁死），其他地方不可以再寫一份。
# 這是「有哪些策略」的名單，不是策略參數；各策略的參數仍只在 strategy/ 底下。
STRATEGIES = ("s4", "s5")

# ---- 日誌（live.logsetup.setup() 的預設值）----
# 日誌檔位置。一律在 runtime/ 底下（已 .gitignore），絕不可以放進 state/ 或 output/。
# 多一層 logs/ 是為了讓輪替出來的 live.log.1 ... 跟日後的 SQLite 等檔案分開放。
# 目錄由 setup() 自己建（live.paths 刻意不建目錄）。
LOG_FILE = os.path.join(RUNTIME_DIR, "logs", "live.log")

# 根 logger 的等級（標準庫的等級名稱字串）。檔案與終端機用同一個等級。
LOG_LEVEL = "INFO"

# 輪替：單檔到這個大小（bytes）就輪替，連同目前這份最多留 LOG_BACKUP_COUNT + 1 份。
# 10 MB x (5 + 1) = 約 60 MB 上限，對小規格雲端主機是安全的量，也夠回頭查幾週。
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5

# ---- SQLite 持久層（live.store）----
# A 頻道訊號表與名目部位表的資料庫檔。跟 LOG_FILE 同理：一律在 runtime/ 底下（已 .gitignore），
# 絕不可以放進 state/ 或 output/。多一層 db/ 是因為 WAL 模式會在旁邊產生 live.sqlite3-wal /
# live.sqlite3-shm，跟日誌分開放。目錄由 live.store.open_store() 自己建（live.paths 刻意不建目錄）。
LIVE_DB_PATH = os.path.join(RUNTIME_DIR, "db", "live.sqlite3")

# A 頻道「策略層」紀錄的 user_id 保留值。A 頻道的名目部位不屬於任何一個訂閱者，但 user_id 又不能
# 是 NULL —— NULL 在唯一索引裡彼此不算重複，「同 (user_id, strategy, symbol) 只能一筆 open」會
# 形同虛設。第二階段每個訂閱者的紀錄才用真實 user_id。
# 這是資料的身分，不是可調參數：改了它，資料庫裡既有的策略層紀錄就查不回來（重啟復原會以為
# 沒有任何未平倉部位）。所以刻意不放進 execution_params()，免得看起來像是可以隨手調的旋鈕。
STRATEGY_USER_ID = "strategy"

# ---- 資料格式常數（A2 事件 live.signal_events 與 B4′ 持久層 live.store 共用；A3 前置小修 FR-0）----
# 以前兩邊各寫一份（store 的 SIDES / EXIT_REASONS、事件的 DIRECTIONS / EXIT_REASONS，epoch 範圍
# 一邊 1e14、一邊 1e13），值剛好相同，但改了一邊另一邊不會跟著變，就會出現「事件收、資料庫拒」
# 或反過來的落差。比照 STRATEGIES 收斂成這一份，兩個模組都在呼叫當下讀。
# 這些是資料格式本身的語意，不是可調的執行參數（改了它們等於改 schema 與事件契約），所以跟
# STRATEGY_USER_ID 一樣刻意不放進 execution_params()。
# 方向：目前只做空。store 的欄位叫 side、事件的欄位叫 direction，值是同一組（欄位名的對應由 A3 負責）。
DIRECTION_SHORT = "short"
DIRECTIONS = (DIRECTION_SHORT,)
# 出場原因：store 的欄位叫 exit_reason、事件的欄位叫 reason，值是同一組。
EXIT_TAKE_PROFIT = "take_profit"
EXIT_STOP_LOSS = "stop_loss"
EXIT_REASONS = (EXIT_TAKE_PROFIT, EXIT_STOP_LOSS)
# UTC epoch 毫秒的合理範圍 [EPOCH_MS_MIN, EPOCH_MS_MAX) = 2001-09-09 ～ 2286-11-20。
# 不是業務規則，是用來抓「把秒 / 微秒當成毫秒傳進來」這種單位錯誤：秒數當毫秒看會落在 1970 年，
# 微秒當毫秒看會落在 5 萬年後。
EPOCH_MS_MIN = 10 ** 12
EPOCH_MS_MAX = 10 ** 13

# ---- A 頻道資料層（A1：tickers 輪詢 → ret2h 粗篩 → 候選 klines，live/signal_feed.py）----
# 判定用的 K 棒週期。K 棒毫秒數與 bars_per_hour 一律由它推導（live.klines），別處不要寫死 12 或 300000。
KLINE_INTERVAL = "5M"

# tickers 輪詢間隔（秒）。輪詢時刻對齊「伺服器時間」的這個秒數整數倍；K 棒週期必須是它的整數倍，
# 這樣「收盤當下」與「2 小時前」都剛好各有一次輪詢（ret2h 的分子與分母）。
TICKERS_POLL_SECONDS = 10

# 收盤當下那次 tickers 失敗（非 429）時總共試幾次。429 不重試（見 REST_BAN_COOLDOWN_SECONDS）。
TICKERS_CLOSE_ATTEMPTS = 2

# 整個行程共用的 REST 速率上限：任何 1 秒窗口內最多送出幾個請求。派網單 IP 是 10 req/s，
# A0 審查 3.2 取 6 留距離。tickers、klines、冷啟動補種子、背景對帳、日後 A3 的取數全部共用這一份
# （live.rest_gate.shared_gate()），不是各自一份。
A1_REST_RATE_PER_SECOND = 6

# 收到 429 之後，整個行程停止送出任何 REST 請求的秒數。派網的 429 是封鎖 60 秒、
# 封鎖期間每多打一次再加 10 秒，所以這段期間一個請求都不可以送。
REST_BAN_COOLDOWN_SECONDS = 70

# A1 單一請求的連線 + 讀取上限（秒）。比 HTTP_TIMEOUT_SECONDS 短：K 棒收盤後的預算只有 3–5 秒，
# 卡 20 秒的請求等於這根 K 棒白做。
A1_HTTP_TIMEOUT_SECONDS = 5

# 粗篩門檻 = strategy_params()["MIN_RET_2H"] - SCREEN_RET2H_MARGIN。門檻一律現場推導，任何地方都
# 不可以寫死門檻值：寫死的話日後 MIN_RET_2H 下調，兩者之間的訊號會被粗篩靜默丟掉。
SCREEN_RET2H_MARGIN = 0.02

# 找「2 小時前」價格樣本時，可接受的樣本時間誤差（秒）。取 1 個輪詢間隔：剛好漏掉一次輪詢，
# 前後鄰居還找得到；再寬就不叫 2 小時漲幅了。
SCREEN_BASE_TOLERANCE_SECONDS = 10

# 沒有合格 2 小時前樣本的 symbol（非冷啟動），每根 K 棒最多幾個升格為候選。
# 寧可多打不要漏，但 tickers 曾中斷時整個標的池都會缺樣本，不設上限會一次打爆速率。
SCREEN_NO_BASE_MAX_CANDIDATES = 10

# 收盤當下那次 tickers 的伺服器時間最多可以比收盤晚幾秒；再晚就不算「收盤價」，該根記 degraded。
CLOSE_SAMPLE_MAX_LAG_SECONDS = 3

# 價格緩衝在「2 小時 + 樣本誤差」之外多留的秒數。
PRICE_BUFFER_EXTRA_SECONDS = 600

# 候選取 klines 的 limit。features() 要在目標列算出 ret2h / volr / turn，需要等距網格上至少
# 289 根；派網上限 500（1000 會回 limit error），與回測一樣取 500。
KLINES_LIMIT = 500

# 候選 klines 等到「收盤 + 這個秒數」（伺服器時間）之後才送出。粗篩照舊在收盤當下用 tickers 做，
# 只有 klines 延後。理由（captain 裁決 2026-09-25）：實盤見過收盤後 0.33 秒取到的 K 棒之後被派網改寫
# （NOM_USDT_PERP 14:30，close 差 1 tick、volume +0.04%，cpos 0.3056→0.2778）；「收盤後 0.3 秒已定稿」
# 的前提只來自單一邊界、兩個 symbol 的實測，不成立。2 秒是暫定值，由 --finality-probe 的量測校正。
BAR_FINALIZE_WAIT_SECONDS = 2.0

# --finality-probe（驗證用，不是正式行為）：收盤後這幾個秒數各重抓一次候選的目標 K 棒，
# 把每個時點的 OHLCV 寫進 --record 的 jsonl，事後算「最後一次變動在收盤後多久」。
FINALITY_PROBE_OFFSETS_SECONDS = (5, 15, 60)
# 定稿量測重抓的 klines limit：只需要目標 K 棒（和它後面那一根）
FINALITY_PROBE_KLINES_LIMIT = 5

# 收盤時刻已過才輪到處理它（行程停頓、輪詢卡住）：延誤不超過這個秒數照常判定，超過就標
# missed_close（degraded、不打 REST、不事後補判）。5 秒內 tickers 樣本對近似 ret2h 的影響遠小於
# 0.02 的餘裕，而且 klines 仍在定稿等待之後才取（captain 裁決 2026-09-25）。
MISSED_CLOSE_TOLERANCE_SECONDS = 5

# 目標 K 棒（剛收完那根）不在回應裡時：總共嘗試幾次、兩次之間等幾秒。用盡就記「目標 K 棒未到」，
# 絕不拿前一根或未收完的那根頂替。
TARGET_BAR_ATTEMPTS = 4
TARGET_BAR_RETRY_WAIT_SECONDS = 0.5

# 候選 klines 並發取數的 worker 數。每秒送幾個由 A1_REST_RATE_PER_SECOND 管，這裡只決定同時有幾個在飛。
A1_FETCH_CONCURRENCY = 6

# 冷啟動補種子：每個 symbol 打一次 klines 取幾根（要涵蓋 2 小時 + 餘裕），失敗時總共試幾次。
SEED_KLINES_LIMIT = 40
SEED_ATTEMPTS = 2

# K 棒收盤前幾秒起，把 REST 額度只留給前景取數，直到該根判定完成（補種子、背景對帳在這段期間不送）。
# 取 1 秒 = 速率窗口的長度：收盤那一刻，最近 1 秒內沒有任何背景請求佔著額度。
FOREGROUND_GUARD_SECONDS = 1

# 背景對帳（FR-10）：每隔多久對一批（秒）、請求攤平在區間的多少比例內、每個 symbol 取幾根、
# 批次在區間結束後多久開始（讓區間最後一根 K 棒先判定完）。
RECONCILE_INTERVAL_SECONDS = 3600
RECONCILE_SPREAD_FRACTION = 0.8
RECONCILE_KLINES_LIMIT = 100
RECONCILE_START_DELAY_SECONDS = 30

# 本機時鐘與伺服器時鐘的偏移（毫秒）超過這個值就記 WARNING。
CLOCK_OFFSET_WARN_MS = 1000

# python -m live.signal_feed --record 不給目錄時的預設位置（runtime/ 底下，不進版控）。
SIGNAL_FEED_RECORD_DIR = os.path.join(RUNTIME_DIR, "signal_feed")

# ---- Telegram Channel 發送（live.tg_channel）----
# 設計理由與各參數之間的交互作用寫在 live/tg_channel.py 的 docstring，這裡只放值。
# Bot API 的根位址。端點路徑（/bot<token>/sendMessage）是程式邏輯，不放這裡。
TG_API_BASE_URL = "https://api.telegram.org"

# 限速：同一個 channel 任何 60 秒視窗內最多送出幾次請求（429 之後的重送也算一次）。
# 20 是 Telegram 官方 FAQ 對群組 / 頻道的建議上限，保守取用。
TG_MAX_MESSAGES_PER_MINUTE = 20

# 限速：同一個 chat 相鄰兩次請求至少隔幾秒。官方 FAQ 另一條建議「單一 chat 避免每秒超過
# 一則」—— 只有每分鐘上限的話，一次湧進 20 則會在 1 秒內全部送出，照樣可能吃 429。設 0 可關閉。
TG_MIN_INTERVAL_SECONDS = 1.0

# 5xx / 連線錯誤 / 逾時的總嘗試次數（不是額外重試次數）。429 不計入這裡，另見下面。
TG_SEND_MAX_ATTEMPTS = 5

# 上面那種錯誤的退避：第 k 次失敗後等 BASE * 2**(k-1) 秒，最多等 MAX 秒。
# 預設 5 次嘗試之間依序等 2、4、8、16 秒，一則最多耽擱約 30 秒就放棄、換下一則。
TG_RETRY_BACKOFF_BASE_SECONDS = 2.0
TG_RETRY_BACKOFF_MAX_SECONDS = 30.0

# 同一則訊息最多因為 HTTP 429 重送幾次；再收到 429 就記 ERROR 放棄這一則，換下一則。
# 429 一定照 retry_after 等滿才重送，這個上限只是防止單一則訊息把整條佇列無限期卡住。
TG_MAX_RATE_LIMITED_RETRIES = 5

# 429 的回應裡找不到可用的 retry_after（JSON 與 Retry-After 標頭都沒有）時，保底等幾秒。
TG_RETRY_AFTER_FALLBACK_SECONDS = 30

# 單一 HTTP 請求的連線 + 讀取上限（秒）
TG_HTTP_TIMEOUT_SECONDS = 10

# Telegram 單則訊息的長度上限，以 UTF-16 code units 計（見 tg_channel docstring）。超過就拒收，不切段。
TG_MAX_MESSAGE_CHARS = 4096

# stop() 沒有指定 timeout 時，最多花幾秒把佇列送完；逾時就放棄剩下的並記 ERROR。
TG_STOP_TIMEOUT_SECONDS = 30

# HTTP 429 的 retry_after 超過這個秒數就不等了：放棄這一則、記 ERROR，回報「不確定 / 放棄」
# （T2 的 outbox 會在 A_CHANNEL_OUTBOX_RETRY_SECONDS 之後重試）。上限以內照舊等滿 retry_after 才重送。
# 理由：發送佇列是嚴格 FIFO，一則等 20 分鐘的 flood control 會把後面所有訊息（含止損出場）一起卡住。
TG_MAX_RETRY_AFTER_SECONDS = 300

# ---- A 頻道名目部位追蹤（A3，live/notional_tracker.py；執行入口 live/a_channel.py）----
# 策略5 的主週期（訊號所在 K 棒的週期）。策略4 的主週期是上面的 KLINE_INTERVAL（A1 的判定週期）。
# 與 dry run 的 s5 帳本相同（pionex_dryrun.BOOK_INTERVAL）；策略五資料層（A5）也用這一個。
# 出場監控週期不寫在這裡：由主週期與 strategy/ 的 RESOLVE_* 出場參數推導（見 notional_tracker）。
S5_KLINE_INTERVAL = "1M"

# ---- 策略五資料層（A5，live/s5_feed.py）----
# ① 粗篩門檻 = s5_signal 參數的 MIN_RISE_FROM_OPEN - S5_SCREEN_RISE_MARGIN（呼叫當下從注入的參數推導，
# 任何地方都不可以寫死門檻值）。近似 ① 用價格緩衝在 H:00 與 H-1:00 的樣本算，與真正的 ① 有誤差，留餘裕寧可多追。
S5_SCREEN_RISE_MARGIN = 0.02

# 候選每分鐘取 1M klines 的 limit。對齊 dry run 的 KLINE_LIMIT（500）：補格的起點一致，判定才逐根相同。
# 派網上限 500（1000 會回 limit error）。
S5_KLINES_LIMIT = 500

# 同一分鐘候選 klines 並發取數的 worker 數。與 A1_FETCH_CONCURRENCY 相同：兩者共用同一個閘門
# （每秒 A1_REST_RATE_PER_SECOND 個），在飛的請求再多也不會更快送出。
S5_FETCH_CONCURRENCY = 6

# A1 在 5M 收盤前後持有前景保留（foreground_hold）時，A5 送 klines 前先讓 A1 做完，最多讓這麼多秒；
# 超過就照送（A5 的請求本來就是前景優先權，閘門會放行）。A1 有候選的根判定完約在收盤後 2.3 秒（p95），
# A5 的取數排在收盤後 BAR_FINALIZE_WAIT_SECONDS，實際多等的只有零點幾秒。
S5_YIELD_MAX_SECONDS = 3.0

# 結束時等 A5 工作執行緒收尾（把手上那一分鐘做完）最多幾秒。最壞情況是讓 A1 的 S5_YIELD_MAX_SECONDS 加上
# 一輪候選取數（每個請求 A1_HTTP_TIMEOUT_SECONDS、最多 TARGET_BAR_ATTEMPTS 次），與 A3_JOIN_TIMEOUT_SECONDS 同值。
S5_JOIN_TIMEOUT_SECONDS = 30

# A3 取 K 棒（出場監控、重啟重判）每個請求的 limit。派網上限 500（1000 會回 limit error）；
# 需要的根數超過就用 endTime 分頁往後取。
A3_KLINES_PAGE_LIMIT = 500

# A3 的工作執行緒在啟動時開資料庫；執行入口最多等這麼多秒確認開成功，才啟動 A1。
A3_STORE_READY_TIMEOUT_SECONDS = 30

# 結束時等 A3 工作執行緒收尾（把手上那一件做完）最多幾秒。
A3_JOIN_TIMEOUT_SECONDS = 30

# python -m live.a_channel --events-jsonl 不給目錄時，事件紀錄 jsonl 的預設位置（runtime/ 底下，不進版控）。
A3_EVENTS_RECORD_DIR = os.path.join(RUNTIME_DIR, "a_channel")

# ---- A 頻道推播（T2，live/a_channel_text.py、live/a_channel_outbox.py、live/a_channel_push.py）----
# 訊息裡「策略」欄顯示的名稱。鍵必須剛好等於 STRATEGIES（tests 斷言）。只寫策略代號，不寫策略內容
# （使用者 2026-09-27 決定）。這是依策略分派的顯示表，不是第二份策略名單。
STRATEGY_LABELS = {"s4": "策略4", "s5": "策略5"}

# 倉位規則（原 WBS §2.2，使用者 2026-09-28 確認策略4、策略5 相同）：目標部位 = 本金 × ORDER_PCT × TARGET_LEVERAGE；
# 實際槓桿 lev = min(TARGET_LEVERAGE, 該幣 tier1 上限)；建議倉位 = 本金的 ORDER_PCT × TARGET_LEVERAGE ÷ lev。
# 這是訂閱者的倉位建議，不是策略參數（策略參數只在 strategy/）。
# 刻意寫成 2 / 100（= 0.02，同一個 float）：tests/test_signal_feed.py 以 token 掃描規定本檔的 0.02 只能出現在
# SCREEN_RET2H_MARGIN 與 S5_SCREEN_RISE_MARGIN 那兩行（防止粗篩門檻被寫死）。這裡的 2% 跟粗篩門檻毫無關係，只是數值剛好相同。
A_CHANNEL_ORDER_PCT = 2 / 100
A_CHANNEL_TARGET_LEVERAGE = 50

# 進場訊息的偏離警語門檻（「價格偏離訊號價超過 X% 不建議追進」）。
# R-3：待 V1 實測滑價後重新校準。
A_CHANNEL_DEVIATION_WARN_PCT = 0.01

# 延遲門檻（秒），基準是訊號 K 棒收盤（= 進場訊息的「發出時間」）。使用者 2026-09-28 決定：進場訊息實際送出時
# 已晚於它超過這個秒數就不發，之後的出場也不發，報表不統計這一筆。出場訊息晚於平倉時刻超過這個秒數照送，但加延遲註記。
A_CHANNEL_ENTRY_MAX_DELAY_SECONDS = 300

# 頻道發送紀錄（outbox，T2 FR-2）的 SQLite 檔。一律在 runtime/ 底下（已 .gitignore），不可以放進 state/ 或 output/。
# 跟 LIVE_DB_PATH 分開一個檔：T2 不動 A3 的資料庫。它也是 R-A 報表的統計依據，**不自動刪除任何一列**。
# 目錄由 live.a_channel_outbox.open_outbox() 自己建（live.paths 刻意不建目錄）。
A_CHANNEL_OUTBOX_DB_PATH = os.path.join(RUNTIME_DIR, "db", "a_channel_outbox.sqlite3")

# outbox 裡「待送」的一則結果不確定（重試用盡、429 超過上限、stop 期限到）時，隔多久再交給發送器一次（秒）。
A_CHANNEL_OUTBOX_RETRY_SECONDS = 60

# 匯流排 handler（在 A3 的執行緒裡）寫 outbox 時，資料庫被鎖住最多等幾秒；等不到就拋例外，走 A3 的重送。
# handler 不可以等很久（live.bus 的規則），所以刻意比 sqlite3 預設的 5 秒短。
A_CHANNEL_OUTBOX_BUSY_TIMEOUT_SECONDS = 2

# 取不到該幣的價格精度（symbol_spec() 查不到，或 quotePrecision 不是合理的整數）時，價格改以這麼多位有效數字顯示
# （小數位數由訊號價 / 進場價決定，同一則訊息的所有價格用同一個位數）。
A_CHANNEL_PRICE_FALLBACK_SIGNIFICANT_DIGITS = 6

# ---- A 頻道報表（R-A，live/a_channel_report.py、live/a_channel_report_push.py）----
# 設計理由與統計口徑寫在 live/a_channel_report.py 的 docstring，這裡只放值。
# 估算手續費的單邊費率。值與 dry run 的 FEE_RATE 相同（pionex_dryrun.py 兩個帳本設定裡的 FEE_RATE=0.0005，
# 報酬寫成 ret - 2 * FEE_RATE），但 live/ 不可以 import pionex_*，所以在這裡另立一份。改 dry run 的費率時要一起看這裡。
A_CHANNEL_REPORT_FEE_RATE = 0.0005

# 排定發送時刻 = 期間結束 + 這個秒數：讓 23:59 那一分鐘的出場有時間被 A3 判定、寫進資料庫。
A_CHANNEL_REPORT_SEND_DELAY_SECONDS = 300

# 報表執行緒多久檢查一次該發的報表（秒）。發送時刻落在排定時刻之後一個輪詢週期內（最晚晚 60 秒）；
# 報表一天最多幾則，不需要更密。
A_CHANNEL_REPORT_POLL_SECONDS = 60

# 補發上限：現在已晚於排定發送時刻超過這個秒數的期別不發，記成 skipped（WARNING）。預設 7 天。
A_CHANNEL_REPORT_CATCHUP_MAX_SECONDS = 7 * 24 * 3600

# 組字當下已晚於排定發送時刻超過這個秒數 → 訊息加延遲註記。
A_CHANNEL_REPORT_LATE_NOTE_SECONDS = 3600

# 一則報表交出的結果不確定（重試用盡、429 超過上限、stop 期限到）或發送器拒收時，隔多久用同一段文字再交一次（秒）。
# 報表不趕時間，刻意比 A_CHANNEL_OUTBOX_RETRY_SECONDS 長：不跟進場 / 出場訊息搶發送器，也不在 flood control 期間反覆撞 429
# （與 TG_MAX_RETRY_AFTER_SECONDS 同一個量級）。
A_CHANNEL_REPORT_RETRY_SECONDS = 300

# 報表發送紀錄的 SQLite 檔。一律在 runtime/ 底下（已 .gitignore）；跟 live.sqlite3、outbox 分開一個檔，R-A 不動那兩個檔。
# 檔案的建立時刻決定「第一次上線不補舊帳」，所以部署到正式環境前這個檔必須不存在（或沒有測試資料），見 README。
# 目錄由 live.a_channel_report_push.open_record() 自己建（live.paths 刻意不建目錄）。
A_CHANNEL_REPORT_DB_PATH = os.path.join(RUNTIME_DIR, "db", "a_channel_reports.sqlite3")

# 執行入口最多等報表執行緒開好發送紀錄幾秒；結束時最多等它收尾幾秒。
A_CHANNEL_REPORT_READY_TIMEOUT_SECONDS = 30
A_CHANNEL_REPORT_JOIN_TIMEOUT_SECONDS = 30


def execution_params():
    """目前生效的執行參數，name -> value。給 `python -m live` 報告用。

    這些值都不敏感，可以直接印。往上面加新常數時記得也加進這個 dict，
    否則冒煙檢查看不到它。
    """
    return {
        "PIONEX_BASE_URL": PIONEX_BASE_URL,
        "HTTP_TIMEOUT_SECONDS": HTTP_TIMEOUT_SECONDS,
        "HTTP_RETRIES": HTTP_RETRIES,
        "MARKET_STATIC_STALE_SECONDS": MARKET_STATIC_STALE_SECONDS,
        "STRATEGIES": STRATEGIES,
        "LOG_FILE": LOG_FILE,
        "LOG_LEVEL": LOG_LEVEL,
        "LOG_MAX_BYTES": LOG_MAX_BYTES,
        "LOG_BACKUP_COUNT": LOG_BACKUP_COUNT,
        "LIVE_DB_PATH": LIVE_DB_PATH,
        "KLINE_INTERVAL": KLINE_INTERVAL,
        "TICKERS_POLL_SECONDS": TICKERS_POLL_SECONDS,
        "TICKERS_CLOSE_ATTEMPTS": TICKERS_CLOSE_ATTEMPTS,
        "A1_REST_RATE_PER_SECOND": A1_REST_RATE_PER_SECOND,
        "REST_BAN_COOLDOWN_SECONDS": REST_BAN_COOLDOWN_SECONDS,
        "A1_HTTP_TIMEOUT_SECONDS": A1_HTTP_TIMEOUT_SECONDS,
        "SCREEN_RET2H_MARGIN": SCREEN_RET2H_MARGIN,
        "SCREEN_BASE_TOLERANCE_SECONDS": SCREEN_BASE_TOLERANCE_SECONDS,
        "SCREEN_NO_BASE_MAX_CANDIDATES": SCREEN_NO_BASE_MAX_CANDIDATES,
        "CLOSE_SAMPLE_MAX_LAG_SECONDS": CLOSE_SAMPLE_MAX_LAG_SECONDS,
        "PRICE_BUFFER_EXTRA_SECONDS": PRICE_BUFFER_EXTRA_SECONDS,
        "KLINES_LIMIT": KLINES_LIMIT,
        "BAR_FINALIZE_WAIT_SECONDS": BAR_FINALIZE_WAIT_SECONDS,
        "FINALITY_PROBE_OFFSETS_SECONDS": FINALITY_PROBE_OFFSETS_SECONDS,
        "FINALITY_PROBE_KLINES_LIMIT": FINALITY_PROBE_KLINES_LIMIT,
        "MISSED_CLOSE_TOLERANCE_SECONDS": MISSED_CLOSE_TOLERANCE_SECONDS,
        "TARGET_BAR_ATTEMPTS": TARGET_BAR_ATTEMPTS,
        "TARGET_BAR_RETRY_WAIT_SECONDS": TARGET_BAR_RETRY_WAIT_SECONDS,
        "A1_FETCH_CONCURRENCY": A1_FETCH_CONCURRENCY,
        "SEED_KLINES_LIMIT": SEED_KLINES_LIMIT,
        "SEED_ATTEMPTS": SEED_ATTEMPTS,
        "FOREGROUND_GUARD_SECONDS": FOREGROUND_GUARD_SECONDS,
        "RECONCILE_INTERVAL_SECONDS": RECONCILE_INTERVAL_SECONDS,
        "RECONCILE_SPREAD_FRACTION": RECONCILE_SPREAD_FRACTION,
        "RECONCILE_KLINES_LIMIT": RECONCILE_KLINES_LIMIT,
        "RECONCILE_START_DELAY_SECONDS": RECONCILE_START_DELAY_SECONDS,
        "CLOCK_OFFSET_WARN_MS": CLOCK_OFFSET_WARN_MS,
        "SIGNAL_FEED_RECORD_DIR": SIGNAL_FEED_RECORD_DIR,
        # ---- Telegram Channel 發送（live.tg_channel）----
        "TG_API_BASE_URL": TG_API_BASE_URL,
        "TG_MAX_MESSAGES_PER_MINUTE": TG_MAX_MESSAGES_PER_MINUTE,
        "TG_MIN_INTERVAL_SECONDS": TG_MIN_INTERVAL_SECONDS,
        "TG_SEND_MAX_ATTEMPTS": TG_SEND_MAX_ATTEMPTS,
        "TG_RETRY_BACKOFF_BASE_SECONDS": TG_RETRY_BACKOFF_BASE_SECONDS,
        "TG_RETRY_BACKOFF_MAX_SECONDS": TG_RETRY_BACKOFF_MAX_SECONDS,
        "TG_MAX_RATE_LIMITED_RETRIES": TG_MAX_RATE_LIMITED_RETRIES,
        "TG_RETRY_AFTER_FALLBACK_SECONDS": TG_RETRY_AFTER_FALLBACK_SECONDS,
        "TG_HTTP_TIMEOUT_SECONDS": TG_HTTP_TIMEOUT_SECONDS,
        "TG_MAX_MESSAGE_CHARS": TG_MAX_MESSAGE_CHARS,
        "TG_STOP_TIMEOUT_SECONDS": TG_STOP_TIMEOUT_SECONDS,
        "TG_MAX_RETRY_AFTER_SECONDS": TG_MAX_RETRY_AFTER_SECONDS,
        # ---- A 頻道名目部位追蹤（A3，live.notional_tracker / live.a_channel）----
        "S5_KLINE_INTERVAL": S5_KLINE_INTERVAL,
        "A3_KLINES_PAGE_LIMIT": A3_KLINES_PAGE_LIMIT,
        "A3_STORE_READY_TIMEOUT_SECONDS": A3_STORE_READY_TIMEOUT_SECONDS,
        "A3_JOIN_TIMEOUT_SECONDS": A3_JOIN_TIMEOUT_SECONDS,
        "A3_EVENTS_RECORD_DIR": A3_EVENTS_RECORD_DIR,
        # ---- 策略五資料層（A5，live.s5_feed）----
        "S5_SCREEN_RISE_MARGIN": S5_SCREEN_RISE_MARGIN,
        "S5_KLINES_LIMIT": S5_KLINES_LIMIT,
        "S5_FETCH_CONCURRENCY": S5_FETCH_CONCURRENCY,
        "S5_YIELD_MAX_SECONDS": S5_YIELD_MAX_SECONDS,
        "S5_JOIN_TIMEOUT_SECONDS": S5_JOIN_TIMEOUT_SECONDS,
        # ---- A 頻道推播（T2，live.a_channel_text / live.a_channel_outbox / live.a_channel_push）----
        "STRATEGY_LABELS": STRATEGY_LABELS,
        "A_CHANNEL_ORDER_PCT": A_CHANNEL_ORDER_PCT,
        "A_CHANNEL_TARGET_LEVERAGE": A_CHANNEL_TARGET_LEVERAGE,
        "A_CHANNEL_DEVIATION_WARN_PCT": A_CHANNEL_DEVIATION_WARN_PCT,
        "A_CHANNEL_ENTRY_MAX_DELAY_SECONDS": A_CHANNEL_ENTRY_MAX_DELAY_SECONDS,
        "A_CHANNEL_OUTBOX_DB_PATH": A_CHANNEL_OUTBOX_DB_PATH,
        "A_CHANNEL_OUTBOX_RETRY_SECONDS": A_CHANNEL_OUTBOX_RETRY_SECONDS,
        "A_CHANNEL_OUTBOX_BUSY_TIMEOUT_SECONDS": A_CHANNEL_OUTBOX_BUSY_TIMEOUT_SECONDS,
        "A_CHANNEL_PRICE_FALLBACK_SIGNIFICANT_DIGITS": A_CHANNEL_PRICE_FALLBACK_SIGNIFICANT_DIGITS,
        # ---- A 頻道報表（R-A，live.a_channel_report / live.a_channel_report_push）----
        "A_CHANNEL_REPORT_FEE_RATE": A_CHANNEL_REPORT_FEE_RATE,
        "A_CHANNEL_REPORT_SEND_DELAY_SECONDS": A_CHANNEL_REPORT_SEND_DELAY_SECONDS,
        "A_CHANNEL_REPORT_POLL_SECONDS": A_CHANNEL_REPORT_POLL_SECONDS,
        "A_CHANNEL_REPORT_CATCHUP_MAX_SECONDS": A_CHANNEL_REPORT_CATCHUP_MAX_SECONDS,
        "A_CHANNEL_REPORT_LATE_NOTE_SECONDS": A_CHANNEL_REPORT_LATE_NOTE_SECONDS,
        "A_CHANNEL_REPORT_RETRY_SECONDS": A_CHANNEL_REPORT_RETRY_SECONDS,
        "A_CHANNEL_REPORT_DB_PATH": A_CHANNEL_REPORT_DB_PATH,
        "A_CHANNEL_REPORT_READY_TIMEOUT_SECONDS": A_CHANNEL_REPORT_READY_TIMEOUT_SECONDS,
        "A_CHANNEL_REPORT_JOIN_TIMEOUT_SECONDS": A_CHANNEL_REPORT_JOIN_TIMEOUT_SECONDS,
    }


# ============================== 第一層：策略參數（只轉發） ==============================
def strategy_params():
    """策略4 的六個進場參數，取自 strategy.s4_signal.DEFAULT_PARAMS。

    回傳的是淺拷貝，改它不會動到策略端那一份；但值永遠是呼叫當下 DEFAULT_PARAMS 的值，
    這裡不快取、不預設、不備份。改了 s4_signal.DEFAULT_PARAMS，這裡拿到的就跟著變 ——
    這正是我們要的：只有一份來源。

    import 刻意寫在函式裡而不是模組頂端：s4_signal 會 import pandas 與 numpy，而
    `python -m live` 的職責之一就是在還沒裝齊套件的新主機上報告「哪個套件沒裝」。
    放在頂端的話，那台機器連 import live.config 都會炸，冒煙檢查就報不出東西了。
    """
    from strategy.s4_signal import DEFAULT_PARAMS
    return dict(DEFAULT_PARAMS)


# ============================== 第三層：密鑰（環境變數） ==============================
# 前綴 CRYPTO_TRADER_ 是為了不跟主機上別的程式撞名（TG_BOT_TOKEN 這種名字太通用）。
TG_BOT_TOKEN_ENV = "CRYPTO_TRADER_TG_BOT_TOKEN"
TG_CHANNEL_ID_ENV = "CRYPTO_TRADER_TG_CHANNEL_ID"

# 目前這套系統用得到的密鑰：環境變數名 -> 用途說明。
# 冒煙檢查與缺值時的錯誤訊息都吃這份，加新密鑰只要加這裡一行。
# 派網 API key 不在這裡：A 頻道只走公開端點，等真的要下單的那個任務再加，不預留欄位。
SECRET_ENV_VARS = {
    TG_BOT_TOKEN_ENV: "Telegram bot token（A 頻道推播）",
    TG_CHANNEL_ID_ENV: "Telegram A 頻道的 channel id",
}


class MissingSecretError(RuntimeError):
    """要用某個密鑰，但對應的環境變數沒設（或設成空字串）。"""


def secret_is_set(env_name):
    """這個密鑰有沒有設。只回 True / False，不回傳也不印出值。

    空字串算沒設 —— systemd 的 `Environment=X=` 會給出空字串，那跟沒設是同一件事。
    """
    return bool(os.environ.get(env_name, "").strip())


def require_secret(env_name):
    """取密鑰的值；沒設就拋 MissingSecretError，訊息寫明缺哪一個、該怎麼設。

    缺密鑰是「用到的時候才失敗」，不是 import 時就失敗：沒有用到 TG 的元件不該因為
    這台機器沒設 token 就起不來。所以取密鑰一律經過這個函式，不要在模組頂端先讀好。
    """
    value = os.environ.get(env_name, "").strip()
    if not value:
        raise MissingSecretError(
            "缺少環境變數 %s（%s）。密鑰只從環境變數讀，不支援設定檔；"
            "啟動前先設好：Linux/macOS `export %s=...`、Windows `set %s=...`，"
            "雲端主機用 systemd unit 的 Environment= 或平台的 secret 機制注入。"
            % (env_name, SECRET_ENV_VARS.get(env_name, "用途未登記"), env_name, env_name)
        )
    return value
