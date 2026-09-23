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
        "LOG_FILE": LOG_FILE,
        "LOG_LEVEL": LOG_LEVEL,
        "LOG_MAX_BYTES": LOG_MAX_BYTES,
        "LOG_BACKUP_COUNT": LOG_BACKUP_COUNT,
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
