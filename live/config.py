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
