# 派網策略 Dry Run

不下單、持續統計的前瞻測試。每次執行會處理上次之後新收完的 K 棒（依帳本分別是
1 小時、5 分鐘或 1 分鐘，見下方帳本表格），進出場規則與回測程式完全相同，結果可直接和回測比較。

目前同時追蹤五個活躍帳本（2026-09-17 起，策略1「延續」`main` 已退役，見下方說明；
2026-09-24 起加入策略5 `s5`）：

| 帳本 | 內容 | 回測表現（一年） |
|---|---|---|
| `watch` | 策略1 已退役，此帳本續跑作 ATR 區間監控（ATR ≥ 2%） | 供 ATR 區間監控用，不是要拿來操作的 |
| `rev` | 策略2 大戶提款 正式版 | 72 筆、勝率 70.8% |
| `rev_wide` | 策略2 放寬版（啟動前漲幅不限） | 179 筆、勝率 68.7% |
| `s4` | 策略4 爆量竭盡（**5分K**，只做空） | 33 天（2026-08-15~09-17）189 筆已平倉、勝率 68.3%（TP4%/SL5%，即目前生效參數：`MIN_RET_2H=0.14`／`MAX_CLOSE_POS=0.60`，見 `pionex_tpsl_sweep.py` 三組實跑驗證） |
| `s5` | 策略5 群組訊號複製（**1分K 重播**，只做空） | 目標是**複製群組機器人的訊號與出場**，不是追求勝率。目前是 2026-09-24 使用者指定的 v3 規則，**尚未用群組訊號校準**（先前「群組 1283 筆校準、46 筆 1 分K 重算 100% 一致」是舊規則 v2 的結果，不適用 v3）。成效以 `pionex_s5_compare.py` 與群組逐筆比對為準（見下方「策略5」一節） |

**`main`（策略1延續）已於 2026-09-17 退役**：用 22.6 萬筆標註資料測過核心假設不成立
（1h動能 + MA20 + OBV 這組訊號對觸發順序不帶資訊，先前看到的高勝率完全由市場漂移解釋），
不再追蹤新交易；舊紀錄與報表已一併移除，`state`/`output` 裡都不會再看到 `main`。

止盈止損與盈虧平衡勝率（= (止損% + 2×手續費%) / (止盈% + 止損%)，手續費算來回各一次）：

| 帳本 | 止盈 | 止損 | 盈虧平衡勝率 |
|---|---|---|---|
| `watch` / `rev` / `rev_wide` | 3% | 5% | 63.75% |
| `s4` | 4%（2026-09-17 由 3% 調高，理由見 `strategy/s4_signal.py` 的 `EXIT_PARAMS` 註解） | 5% | 56.67% |
| `s5` | 3% | 5%（= 群組第 1 次加倉點，群組把有加倉的單記為止損） | 63.75% |

也就是說：`watch`／`rev`／`rev_wide`／`s5` 要打平至少要贏 63.75%，`s4` 因為止盈調高，只要贏 56.67% 就打平。
資金模擬槓桿：策略1、2、4、5 皆為 50 倍（改 `BASE_CONFIG` / `S2_CONFIG` / `S4_CONFIG` / `S5_CONFIG`）。

策略4 目前生效參數（2026-09-17 調整，理由與實測依據見 `strategy/s4_signal.py` 的註解：
`MIN_RET_2H` / `MAX_CLOSE_POS` 在 `DEFAULT_PARAMS`，`TAKE_PROFIT` / `STOP_LOSS` 在 `EXIT_PARAMS`）：
`MIN_RET_2H=0.14`、`MAX_CLOSE_POS=0.60`、`TAKE_PROFIT=0.04`（`STOP_LOSS` 維持 0.05）。
帳本表格 `s4` 那一格先前列的「33天155筆、勝率71.0%；15分K 99天310筆、67.7%」是
2026-09-17 參數調整**之前**（`MIN_RET_2H=0.16`／`MAX_CLOSE_POS=0.70`／`TAKE_PROFIT=0.03`）
的回測結果，與目前生效參數不對應，已改用 `pionex_tpsl_sweep.py` 在目前參數組合下的
33天實跑驗證數字取代；15分K 週期目前沒有對應新參數的長期回測結果，如需要請重新量測。

策略4 用 5 分 K、策略5 用 1 分 K，其餘用 1 小時 K，程式會自動分開抓。
**5 分 K 只保留最近約 34 天**，所以策略4 的回補起點最早只能到那時；
`START_FROM` 設得更早時程式會印出提醒，不影響其他帳本。

### 策略5（群組訊號複製）

參數與訊號邏輯都只有一份，在 `strategy/s5_signal.py`（G5 起，比照策略4 的 `strategy/s4_signal.py`）：
進場參數在 `DEFAULT_PARAMS`，止盈、止損、出場模式、最長持倉在 `EXIT_PARAMS`；回測的 `pionex_strategy5.S5` /
`CONFIG` 與 dry run 的 `S5_RULE`（= 整份 `pionex_strategy5.S5`）/ `S5_CONFIG` 都從它取值，手續費留在
`pionex_strategy5.CONFIG`。回測的 `add_indicators()` 與 dry run 的 `s5_indicators()` 都呼叫 `s5_signal` 計算
小時脈絡與 ①②③（`evaluate()` 是 v3 的一步到位版本，只 import numpy / pandas，日後實盤端也用它）。
條件以 1 小時 K（整點切）為單位，進出場用 1 分 K（規則 v3，2026-09-24 起）：

1. 前一根 1h K：收盤 ÷ **開盤** − 1 ≥ 6%（`MIN_RISE_FROM_OPEN`）
2. 爆量，下列三個分支**符合任一個**即可：
   - A：本小時到目前為止的累計量 ≥ 2 × 前一根 1h K 的量，不限時（上一根 10，這根要 ≥ 20；`MIN_VOL_MULT = 2.0`）
   - B：開盤 **30 分鐘內**累計量曾 ≥ 1 × 前一根的量（追平）
   - C：開盤 **15 分鐘內**累計量曾 ≥ 0.5 × 前一根的量

   B、C 設在 `EARLY_VOL_RULES = ((30, 1.0), (15, 0.5))`，每個分支是 (N 分鐘, k 倍)，要增減分支只改這一處。
   「N 分鐘內」以 1 分K 收盤時刻 ≤ 第 N 分鐘判斷（含第 N 分鐘收盤那根）。
   **B、C 是「黏著」的**：一旦成立，這一整個小時的爆量都算成立，之後在突破前高那一刻進場。
   例（使用者確認）：第 20 分鐘量就追平前一根、第 40 分鐘價格才突破前高（這時量還沒到 2 倍）→ **第 40 分鐘進場**。
3. 現價（這根 1 分K 收盤）> 前一根 1h K 最高價（`MIN_ABOVE_PH = 0.0`）

三條件**第一次同時成立**的那根 1 分K 收盤進場；同一個幣每小時最多一次、持倉中不重複進場。
出場後冷卻：**出場的下一根 K 棒起就可以再進場，同一小時仍最多一次**（`COOLDOWN_HOURS = 0`，
dry run 與回測用同一個換算式 `max(1, round(COOLDOWN_HOURS × 每小時根數))` 換成 1 根，兩邊一致）。
等於重播一個「每分鐘掃描一次」的機器人。因為是拿已收盤的 1 分K 事後重播，GitHub 排程延遲或跳過
都不影響結果，下次執行會補處理。

**2026-09-24 規則修正，`s5` 帳本因此重置**（參數指紋改變 → 自動清空，依 `BOOK_START_FROM["s5"]` 以新規則回補，
最多往前 `S5_MAX_BACKFILL_HOURS` = 24 小時），
重置前的紀錄屬舊規則 v2（① 收盤 ÷ 最低、② 只要本小時量追平前根、不限時）。
v2 當時的校準數字（群組 1283 筆訊號、其中 46 筆以 1 分K 重算 100% 一致）只適用 v2；v3 **尚未校準**。

- **1 分 K 只保留最近約 7 天**。策略5 有自己的起算時間 `BOOK_START_FROM["s5"]`，且首次回補最多
  `S5_MAX_BACKFILL_HOURS`（24 小時），避免第一次執行就打大量 API。排程若中斷超過約 7 天，
  那段期間的策略5 紀錄會補不回來
- 1 分K 已是最小週期，同一根同時碰到止盈與止損時**保守計止損**（其他帳本會再抓更細的K棒判先後）
- 不另外抽同期基準：1 分K 前視 48 根只有 48 分鐘，大部分樣本不會觸發止盈止損。止盈止損與 60M 帳本
  相同（3%／5%），直接共用 60M 那一桶（若在 `strategy/s5_signal.py` 把止盈止損改成 60M 帳本沒有的組合，
  就沒有對應的基準可借，SUMMARY 會顯示「同期基準樣本不足」）
- 每次執行會另外輸出 `output/s5_signals.csv`，格式與群組的 `signals.csv` 相同，給比對工具使用；
  另附診斷欄，其中 **「② 爆量分支」** 列出進場那一刻成立的**所有**分支（例如 `2倍`、`30分1倍`、`15分0.5倍`、
  `30分1倍+15分0.5倍`，標籤由分支參數產生），方便和群組對照。`dryrun_s5.xlsx` 的交易明細也有同一欄
- **要調策略5 請改 `strategy/s5_signal.py`**（`DEFAULT_PARAMS` / `EXIT_PARAMS`；手續費在 `pionex_strategy5.CONFIG`），
  回測與 dry run 會一起變。注意改 `S5` 的**任何一個鍵**（包括 dry run
  用不到的）或上述五個出場／費用參數，`s5` 的參數指紋都會變，`s5` 帳本會自動重置
  （見下方「參數版本控管」）——只是想在本機試回測參數的話，別把改動 commit 上 main。
  刻意**不**統一的只有兩邊本質不同的設定：K 棒週期（dry run 1 分K、回測預設 15 分K）、同根雙觸的判定週期、暖機根數
- 回測（`pionex_strategy5.py`）在 `SUB_INTERVAL` 的小K收盤時判斷限時分支，所以每個分支的分鐘數都必須是
  小K分鐘數的整數倍：`5M`、`15M` 可以；`30M` 看不到 15 分、`60M` 都看不到，回測會在開頭停下並說明是哪個分支
- **dry run 只實作 v3 組合**（訊號邏輯在 `strategy/s5_signal.py` 的 `evaluate()`，`s5_indicators()` 只呼叫它）。門檻數值與 `EARLY_VOL_RULES`
  可以直接改；但若把 `S5` 切到 dry run 沒實作的變體（關掉 `REQUIRE_VOL_BURST`／`REQUIRE_HIGHER_HIGH`、
  `HH_MODE` 不是 `"price"`、設了 `MIN_TURN24H`／`MAX_TURN24H`／`MAX_PRICE`、`CONFIG["EXIT_MODE"]` 不是 `"fixed"`），
  或 `EARLY_VOL_RULES` 格式不對（每個分支須為 1～60 的整數分鐘與大於 0 的倍數），
  或留著已移除的舊鍵（`FAST_VOL_MULT`／`FAST_WINDOW_MIN`／`MIN_RISE_FROM_LOW`），dry run 會在一開始
  （任何網路請求與狀態檔寫入之前）**整個停下**，錯誤訊息寫明是哪個鍵、目前的值、dry run 支援什麼；
  GitHub Actions 會因此失敗並寄信通知。改回來（或先在 `strategy/s5_signal.py` 實作該變體）之後，下次執行會自動補處理
  漏掉的 K 棒（1 分K 保留約 7 天，停超過就補不回來）

**和群組逐筆比對**（在自己電腦上跑，不在 GitHub Actions 上）：把群組訊號轉成 `signals.csv`、
放在 repo 根目錄，拉下最新的 `output/` 與 `state/` 後執行 `python pionex_s5_compare.py`。
它只比對兩邊都有資料的期間，輸出抓到率、命中率、進場時間差與兩邊各自漏掉的清單
（`s5_compare_<時間>.xlsx`）。`signals.csv`、群組聊天匯出（`signals*.txt`）與比對報表含有群組的資料，
**已由 `.gitignore` 擋住**（`/signals*.csv`、`/signals*.txt`、`/s5_compare_*.xlsx`，只擋 repo 根目錄；
`output/s5_signals.csv` 是 dry run 自己的輸出，照常 commit）。

## 開始前先確認起算時間

`pionex_dryrun.py` 開頭的 `START_FROM` 決定從什麼時候開始統計（台北時間）：

```python
START_FROM = "2026-09-01 00:00"   # 設 None = 只從第一次執行當下開始
BOOK_START_FROM = {"s5": "2026-09-24 08:00"}   # 個別帳本另訂起算時間（沒列的沿用 START_FROM）
S5_MAX_BACKFILL_HOURS = 24                     # 策略5 首次最多往前回補幾小時
```

第一次執行時會回補這個時間之後的所有 K 棒。**派網 API 單次請求最多回傳 500 根**，
但 `fetch_klines_raw()` 會自動一直往回翻頁抓取，不是抓一次 500 根就停；真正抓不抓
得到，看的是**交易所本身的保留上限**——每個K棒週期約只保留 10,000 根，換算成天數：
1 小時K（60M，`watch`/`rev`/`rev_wide` 用）約 **416 天**、5 分K（5M，`s4` 用）約
**34 天**。只有超過這個保留上限的部分才會真的抓不到，程式會顯示警告並從能取到的
最早一根開始（這則警告目前是用一次獨立的探測性抓取比對出來的，起算時間落在
「500 根／約 20.8 天」之後、但仍在 416 天保留範圍內時，有機率印出過度保守的警告
——不代表那段時間真的抓不到，正式抓取交易對資料時仍會照樣往回翻頁）。

`START_FROM` **不在**參數指紋的涵蓋範圍內（2026-09-18 起），改它不會觸發任何帳本重置。
原因是它本來就只決定「帳本第一次執行要回補多早」：帳本一旦有紀錄，`step_symbol()` 走的是
`st["last"]`、`backfill_from` 也只對 `symbols` 為空的帳本設值，所以對運行中的帳本改
`START_FROM` 本來就沒有作用。先前版本把它算進指紋，會把一個無害的修改變成「四本帳同時
清空」，已修正。

唯一的副作用：改大（往更早）之後，**新上架的幣**第一次被抓取時會多抓一些用不到的K棒
（`since_map` 會從 `START_FROM` 起算），不影響統計結果，只是多花一點 API 請求。

## 檔案

| 檔案 | 用途 |
|---|---|
| `pionex_dryrun.py` | 主程式（策略參數在檔案開頭 `BASE_CONFIG`） |
| `pionex_backtest.py` | 策略1 回測程式，dry run 直接使用裡面的指標與出場邏輯 |
| `pionex_reversal.py` | 策略2 回測程式，同上 |
| `pionex_strategy4.py` | 策略4 回測程式，同上 |
| `pionex_strategy5.py` | 策略5 回測程式（小時K條件、盤中觸發，含校準用的變體開關、條件拆解與驗證頁）。參數與共用的訊號計算取自 `strategy/s5_signal.py`；dry run 經由它的 `S5` / `CONFIG` 取參數 |
| `strategy/s5_signal.py` | 策略5 訊號核心與參數的唯一來源（`DEFAULT_PARAMS`、`EXIT_PARAMS`、小時脈絡、①②③、分支編碼與標籤、`evaluate()`），只 import numpy / pandas；回測、dry run、實盤共用。離線測試在 `tests/test_s5_signal.py` |
| `pionex_s5_compare.py` | 策略5 與群組訊號的逐筆比對工具，在自己電腦上跑，需要群組的 `signals.csv` |
| `pionex_tpsl_sweep.py` | 策略4 的止盈/止損全組合掃描工具，用「首達根數」重放所有 TP/SL 組合，找參數用，不是 dry run 的一部分 |
| `.github/workflows/dryrun.yml` | GitHub Actions 排程設定 |
| `run.sh` | 在 VPS / 自己主機上用 crontab 執行 |
| `output/SUMMARY.md` | 自動產生的摘要（在 GitHub 網頁或手機 App 直接可看） |
| `output/dryrun_watch.xlsx` | 策略1 觀察組報表（已退役，續作 ATR 區間監控） |
| `output/dryrun_rev.xlsx` | 策略2 正式版報表 |
| `output/dryrun_rev_wide.xlsx` | 策略2 放寬版報表 |
| `output/dryrun_s4.xlsx` | 策略4 報表 |
| `output/dryrun_s5.xlsx` | 策略5 報表 |
| `output/s5_signals.csv` | 策略5 的訊號與出場，格式同群組 `signals.csv`，給 `pionex_s5_compare.py` 用；另附診斷欄（含「② 爆量分支」） |
| `state/dryrun_state.json` | 持倉與交易紀錄，**不要手動修改** |

### 實盤 / paper trading 骨架（`live/`，建置中）

`live/` 是實盤端的套件，與上面這套回測 / dry run 互不相干（不會 import 根目錄那幾支
`pionex_*.py`），目前只有一個環境冒煙檢查：在 repo 根目錄執行 `python -m live`，會印出
Python 版本、套件版本、路徑、執行參數與密鑰有沒有設、目前 commit，用來確認一台新主機
裝對了沒。缺密鑰不會讓它失敗（exit code 仍是 0），它是報告不是放行閘門。

它的相依另外列在 **`requirements-live.txt`**，和 dry run 用的 `requirements.txt` 分開：
GitHub Actions 與 `run.sh` 只裝後者，實盤主機才裝前者，兩份不要合併。實盤執行期產生的
檔案（資料庫、日誌）一律寫在 `runtime/`（已列入 `.gitignore`），**不可以寫進 `state/` 或
`output/`** —— 那兩個目錄每小時會被 dry run 自動 commit 進版控。

`live/market_static.py` 是全市場交易對規格 + 槓桿上限的記憶體快取（`/common/symbols` 與
`/common/riskTable` 兩個公開端點，每次刷新共 2 個請求，只取 tier1 `maxLeverage`）。刷新由呼叫者
觸發（`refresh()` / `refresh_if_stale()`），沒有背景執行緒；刷新失敗保留舊資料、記 `last_error`。
取數可以注入（`refresh(fetch=...)` / `refresh_if_stale(fetch=...)`）：不給就跟以前一樣直接呼叫
`live/pionex_api.py` 的 `api_get`（含它的重試）；A 頻道（A1）注入的是經過共用閘門 `live/rest_gate.py` 的取數，
刷新請求計入閘門統計，收到 429 由閘門進入冷卻、不再重試。
`python -m live.market_static` 會真實連線印出兩端點的結構、槓桿分布與抽樣，離線測試在
`tests/test_market_static.py`（不需要 pytest，直接 `python tests/test_market_static.py`）。
HTTP 取數在 `live/pionex_api.py`（原本叫 `live/http.py`，跟標準庫的 `http` 同名，被遮蔽時症狀會長成
「requests 壞掉」很難追，故改名）。程式不內建任何 CA 憑證路徑；網路有 SSL inspection 的機器請設
環境變數 `REQUESTS_CA_BUNDLE`。

設定與密鑰分三層，規則寫在 `live/config.py` 的 docstring：

| 層 | 放在哪裡 | 怎麼改 |
| --- | --- | --- |
| 策略參數（`MIN_RET_2H` 等六個） | `strategy/s4_signal.py` 的 `DEFAULT_PARAMS`，**唯一來源** | 改那裡。`live/` 只用 `config.strategy_params()` 引用，不可以抄一份數值過去 |
| 策略4 出場參數（`TAKE_PROFIT` / `STOP_LOSS` 等六個） | `strategy/s4_signal.py` 的 `EXIT_PARAMS`，**唯一來源**（與 `DEFAULT_PARAMS` 刻意分開） | 改那裡，回測 `pionex_strategy4.CONFIG` 與 dry run `S4_CONFIG` 都從它取值；要放進會被修改的字典時用 `exit_params()` 取拷貝。改任何值都會讓 dry run 的 `s4` 帳本換指紋、前瞻紀錄重新開始（`tests/test_s4_exit_params.py` 會提醒）。不可以併進 `DEFAULT_PARAMS`，也不可以在 `live/` 抄一份 |
| 策略5 參數（進場 11 個、出場 4 個） | `strategy/s5_signal.py` 的 `DEFAULT_PARAMS` / `EXIT_PARAMS`，**唯一來源**（兩者刻意分開；手續費不在這裡） | 改那裡，回測 `pionex_strategy5.S5` / `CONFIG` 與 dry run `S5_RULE` / `S5_CONFIG` 都從它取值。改任何鍵、值或數值型別都會讓 dry run 的 `s5` 帳本換指紋、前瞻紀錄重新開始（`tests/test_s5_signal.py` 會提醒）。`live/notional_tracker.py`（A3）的出場判定直接引用 `exit_params()` / `cooldown_bars()`，不可以在 `live/` 抄一份 |
| 執行參數（BASE URL、timeout、retries、快取逾時） | `live/config.py` 的模組層級常數 | 改檔案再 commit。沒有設定檔格式、沒有 parser、沒有 dev/prod 切換 |
| 資料格式常數（方向 `DIRECTIONS`、出場原因 `EXIT_REASONS`、epoch 毫秒合理範圍 `EPOCH_MS_MIN` / `EPOCH_MS_MAX`） | `live/config.py`，事件（`live/signal_events.py`）與持久層（`live/store.py`）共用這一份、呼叫當下讀 | 這是 schema 與事件契約本身，不是可調旋鈕，所以跟 `STRATEGY_USER_ID` 一樣不列在 `execution_params()` |
| 密鑰（TG bot token、A 頻道 channel id） | 環境變數 `CRYPTO_TRADER_TG_BOT_TOKEN`、`CRYPTO_TRADER_TG_CHANNEL_ID` | 啟動前 `export`，雲端主機用 systemd 的 `Environment=`。**絕不進版控**，不支援 `.env` 或任何檔案來源 |

沒設密鑰不影響用不到它的元件 —— `import live.config` 不會失敗，要用的元件會在取用當下拋出
說得出該設哪個環境變數的例外。`python -m live` 只報告每個密鑰「已設定 / 未設定」，不印值。

訊號與推播 / 下單之間用事件匯流排解耦：`live/signal_events.py` 定義不可變的 `EntryEvent`（進場）
與 `ExitEvent`（出場），每個事件都帶 `strategy`（名單只在 `live/config.py` 的 `STRATEGIES`，
同一個幣 s4、s5 可以同時持倉），欄位不合法在建構時就拋例外；`live/bus.py` 的 `SignalBus`
由進入點建一個，訂閱者在啟動時 `subscribe`，產生者 `publish` 後依訂閱順序**同步**呼叫每個 handler，
一個 handler 出錯只記 ERROR、不影響其他人，回傳送達報告。會做 I/O 的訂閱者（TG、webhook）
必須自己排佇列後立刻返回。匯流排本身不做持久化與重送；重送由 A3 依資料庫的「未發布」紀錄做，
採 at-least-once，所以**訂閱者一律以 `(signal_id, 事件種類)` 去重**（同一個 signal_id 保證先進場、後出場）。
`ExitEvent` 帶 `features`（出場稽核旗標：是否開盤跳空、是否同根兩碰保守計止損、是否為重啟補判……，
鍵見 `live/notional_tracker.py` 的 `EXIT_FEATURE_KEYS`）。`signal_id` / `symbol` 不可含 lone surrogate
（與 store 一致）。離線測試在 `tests/test_bus.py`。

日誌在 `live/logsetup.py`（刻意不叫 `logging.py`，理由同上面 `http.py` 的改名）。進入點啟動時
呼叫一次 `logsetup.setup()`，之後各模組照標準寫法 `logging.getLogger(__name__)` 即可；import 本身
沒有副作用。日誌檔預設在 `runtime/logs/live.log`（UTF-8、依大小輪替，位置 / 等級 / 輪替參數都在
`live/config.py`），同時輸出到終端機；時間戳一律是台北時間，與主機時區無關。終端機編不出的字元
（cp950 / cp1252 下的 emoji 等）會變成 Python 跳脫序列（`⚠` → `\u26a0`、`🔴` → `\U0001f534`），
不會出現 `--- Logging error ---` 把訊息吃掉；日誌檔則是 UTF-8 原字元。
未攔截的例外（含背景執行緒）會帶完整 traceback 進日誌，程序照樣結束、不自動重啟；若之後有人
停用或重設了 logging，traceback 仍會照 Python 預設印到 stderr，不會消失。
**同一個日誌檔同時只能有一個行程在寫**（Windows 上別的行程開著檔案會讓輪替失敗）。
`python -m live` 會報告日誌會寫到哪裡、能不能寫，但不會建立任何日誌檔。離線測試在
`tests/test_logsetup.py`。

A 頻道的訊號與名目部位存在 `live/store.py`（標準庫 SQLite，資料庫檔預設 `runtime/db/live.sqlite3`，
路徑在 `live/config.py` 的 `LIVE_DB_PATH`），程式重啟後靠它找回還沒平倉的名目部位與還沒發布的訊號。
**同一個資料庫檔同時只能有一個行程在寫**；旁邊的 `live.sqlite3-wal` / `-shm` 是 WAL 模式的檔案，
當掉後留下的 `-wal` **不要手動刪**，下次開啟時 SQLite 會用它復原已提交的資料。
schema 版本記在 `PRAGMA user_version`，目前是 2（A3 在名目部位表加了出場稽核旗標 `exit_features_json`
與出場事件「已發布」時刻 `exit_published_ms`）。**舊的 v1 資料庫開啟時會自動升級**（同一個交易裡
`ALTER TABLE ADD COLUMN`，既有資料一格不動；失敗整段 ROLLBACK，停在 v1）。v1 沒有記錄出場事件是否
發布過，所以升級後既有的已平倉部位一律當成「出場未發布」，A3 第一次啟動會補發（寧可重送、不可漏發）。
ROLLBACK 本身失敗時 store 會「中毒」（關閉連線，之後的操作拋 `StorePoisonedError`），呼叫端要重新
`open_store()`。離線測試在 `tests/test_store.py`。

A 頻道資料層在 `live/signal_feed.py`（A1）：每 10 秒打一次 tickers 維護 2 小時價格序列，K 棒收盤時以
近似 ret2h 粗篩（門檻 = `MIN_RET_2H - SCREEN_RET2H_MARGIN`，現場推導），只對候選打 klines，並且只在
「剛收完的那根」上跑 `strategy/s4_signal`（候選 klines 等到收盤後 `BAR_FINALIZE_WAIT_SECONDS` 才取，
避開派網收盤後改寫 K 棒）。它每根 K 棒交出一個結果物件，本身不發 TG、不追蹤部位；行程停頓或輪詢卡住
而錯過的收盤也會交出結果（`missed_close`，degraded，不補判），不會有哪一根悄悄消失。
所有派網 REST 請求都經過 `live/rest_gate.py` 的共用閘門（整個行程一份：任何 1 秒最多
`A1_REST_RATE_PER_SECOND` 個請求，收到 429 就整個行程停送 `REST_BAN_COOLDOWN_SECONDS` 秒），
標的池（`market_static`）的刷新也經過它；之後的元件要打 REST 也請走 `rest_gate.shared_gate()`。
A1 另有背景對帳（`live/reconcile.py`）：每小時用 klines 重算上一小時每根 K 棒的真實 ret2h，和當時的粗篩逐一比對，
漏失記 ERROR。degraded 的根依原因決定排除範圍：影響粗篩的（收盤 tickers 失敗 / 被封鎖、冷卻中、missed_close…）
整根不算漏失；只有個別候選 klines 沒拿到的，只排除那幾個候選，同一根其他 symbol 照算。行程停頓醒來後，
錯過的整點批次會補做（還在 klines 涵蓋內的合併成一輪取數、照常攤平），超出涵蓋（約 5 小時前的窗口）的合併成
一行 WARNING，不會誤報「完全沒有紀錄」。觀察用：
`python -m live.signal_feed --duration 900 --record`（jsonl 預設寫到 `runtime/signal_feed/`，
不可以指到 `state/` 或 `output/`；`--force-candidates N` 是壓測用；`--finality-probe` 是驗證用，
收盤後 5 / 15 / 60 秒各重抓一次候選的 K 棒寫進 jsonl，用來校正定稿等待秒數）。離線測試在
`tests/test_signal_feed.py`、`tests/test_rest_gate.py`、`tests/test_a1f_followups.py`。

A 頻道名目部位追蹤在 `live/notional_tracker.py`（A3）：收 A1 的原始訊號（策略五資料層日後接
`submit_signal()`），排除「同策略同幣持倉中」與「冷卻中」，以訊號價算止盈 / 止損、寫庫、發進場事件；
之後**每一根 1 分K** 收盤 + `BAR_FINALIZE_WAIT_SECONDS` 檢查所有未平倉的名目部位，碰止盈 / 止損就平倉寫庫、
發出場事件。判定語意對齊 dry run（`pionex_dryrun.step_symbol()` + `resolve_with_5m()`）：結果、出場價、
出場所屬的主週期 K 棒與冷卻逐筆相同，只有出場時刻會更早（1 分K 觸價就判定，不等 5 分K 收盤）；
跳空穿越止損一律用**主週期 K 棒的開盤**判斷。所有參數取自 `strategy/`（s4 / s5 的 `exit_params()`、
`cooldown_bars()`），主週期是 `KLINE_INTERVAL`（s4）與 `S5_KLINE_INTERVAL`（s5），監控週期由出場參數推導；
`MAX_HOLD_HOURS` 不是 None 或 `EXIT_MODE` 不是 `"fixed"` 時拒絕啟動。取數走共用閘門（`PRIORITY_NORMAL`，
低於 A1 的收盤前景取數），失敗就下一分鐘再試、不臆測。所有狀態以 `(strategy, symbol)` 區分，s4、s5
同一個幣可以同時持倉。`signal_id` 格式是 `#<幣名>-<S4|S5>-<YYYYMMDD>-<HHMM>`（台北時間、訊號 K 棒收盤）。

**重啟會從資料庫復原**：載入未平倉部位、從進場後第一根 1 分K 重新判定到現在（停機期間已觸價的，以歷史
出場時刻與價格平倉，事件標 `recovered`）、從最後一筆已平倉部位重建冷卻、補發所有已寫庫未發布的事件
（進場先於出場）。1 分K 只保留約 7 天，停機超過保留期的區段改用 dry run 的原始做法（5 分K 判定、兩碰再找
1 分K、找不到 → 止損）；5 分K 也拿不到（約 34 天）就記 ERROR、保持 open 繼續監控。停機期間 A1 沒有交出的
訊號不會補（沒寫過庫、也沒發過事件）。

執行入口（**不在** `python -m live` 裡）：

    python -m live.a_channel --duration 3600 [--events-jsonl [DIR]] [--push-tg]

它把 `logsetup.setup()`、共用閘門、A2 匯流排、A3、A1（標的池由 `market_static` 載入）接起來。匯流排一定會掛一個
記錄事件的訂閱者（寫 INFO 日誌，加 `--events-jsonl` 時另外寫 jsonl，預設 `runtime/a_channel/`，不可以指到
`state/` 或 `output/`）；**加 `--push-tg` 才會推播到 Telegram A 頻道**（見下面 T2）。啟動時印接線自檢
（每種事件幾個訂閱者，有 0 個就不跑）。
A3 相關的執行參數：`S5_KLINE_INTERVAL`、`A3_KLINES_PAGE_LIMIT`（分頁取 K 棒的 limit）、
`A3_STORE_READY_TIMEOUT_SECONDS`、`A3_JOIN_TIMEOUT_SECONDS`、`A3_EVENTS_RECORD_DIR`（都在 `live/config.py`，
`python -m live` 會列出）。離線測試在 `tests/test_notional_tracker.py`（含拿 dry run 真正的程式逐筆對照）。

A 頻道的發送器在 `live/tg_channel.py`（T1′）：`ChannelSender` 只負責把組好的純文字送到 Telegram Channel
（Bot API `sendMessage`，不帶 `parse_mode`、關閉連結預覽，只用 `requests`，模組刻意不叫 `telegram`）。`send()`
排進嚴格 FIFO 的記憶體佇列就返回，一條背景執行緒依序送出；限速是任何 60 秒最多 `TG_MAX_MESSAGES_PER_MINUTE` 次請求、
相鄰至少隔 `TG_MIN_INTERVAL_SECONDS` 秒；429 等滿 `retry_after` 才重送（超過 `TG_MAX_RETRY_AFTER_SECONDS` 就不等、
放棄這一則並記 ERROR），5xx / 連線錯誤 / 逾時退避重試，400 / 401 / 403 等不重試。token 在網址裡，所以所有日誌、
例外、`stats()` 都經過集中遮罩（token 與 channel id 的任何 8 字元片段都換成 `[REDACTED]`，urllib3 的 logger 也掛了
遮罩 filter）。`send(..., on_done=callback, expires_at=epoch 秒)` 兩個選用參數給 T2 用：每一則的實際結果
（已送達含 `message_id` / 永久失敗 / 不確定・放棄 / 過期）恰好回報一次；期限在每一次實際發出請求之前檢查，
但只要之前有一次結果不確定（讀取逾時、連線中斷、5xx 等請求可能已到 Telegram 的情況），就不再套期限；DNS 解析失敗、
連線被拒、連線逾時、SSL 憑證驗證失敗則確定沒送達（請求沒有離開本機），期限照常檢查。不給就跟以前完全一樣。實機冒煙：

    python -m live.tg_channel --smoke

有設兩個密鑰就送一則 `[測試] T1′ 冒煙 <台北時間>`（成功 exit 0、失敗 exit 1），沒設就回報未實測（exit 3），
不連網。離線測試在 `tests/test_tg_channel.py`。

**A 頻道推播（T2）**把 A3 的進場 / 出場事件變成頻道訊息：

- `live/a_channel_text.py`：訊息格式（純函式）。進場（訊號價、止盈 / 止損價與百分比、建議倉位、槓桿、發出時間）、
  止盈出場、止損出場（兩者欄位完全對稱）。百分比、名目報酬、持倉時間一律由事件本身的價格與時間推導，不讀
  `strategy/` 的參數；價格依該幣的價格精度（`market_static.symbol_spec()` 的 `quotePrecision`）四捨五入；
  建議倉位 = 本金的 `A_CHANNEL_ORDER_PCT × A_CHANNEL_TARGET_LEVERAGE ÷ min(目標槓桿, 該幣上限)`，上限取不到時不捏造、
  以目標槓桿計並註明。出場稽核旗標（例如 `minor_resolved`）不出現在訊息裡。策略名稱在 `config.STRATEGY_LABELS`。
  版面是「標籤：值」（使用者 2026-09-28 看過實機樣本定案，沒有版面開關）；出場原因是「觸及止盈價」「觸及止損價」，
  開盤跳空的止損是「觸及止損價且有滑價」；出場訊息底部的時間叫「出場時間」，延遲發布的註記放在標題下方。
- `live/a_channel_outbox.py`：**頻道發送紀錄（outbox）**，SQLite 檔 `runtime/db/a_channel_outbox.sqlite3`
  （`A_CHANNEL_OUTBOX_DB_PATH`，跟 `live.sqlite3` 分開）。一列 = 一個 `(signal_id, 事件種類)`，狀態是 `pending`（待送）/
  `delivered`（已送達，記 `message_id`）/ `expired`（延遲不發）/ `no_entry`（因進場未出現而不發）/ `failed`（永久失敗）。
  它同時是 **R-A 報表的統計依據**，**不自動刪除任何一列**；同一個 outbox 檔同時只能有一個行程在寫。
- `live/a_channel_push.py`：匯流排訂閱者（名稱 `tg_a_channel`）。handler 在 A3 的執行緒裡只寫 outbox 就返回
  （新鮮的進場碰到 `market_static` 還沒載入、或 outbox 寫不進去就拋例外，A3 下一分鐘重送；收到時就已過延遲門檻的舊進場、
  以及出場，都不查 `market_static`）；自己的工作執行緒依寫入順序、
  **一次交出一則**給 `ChannelSender`，把實際結果記回 outbox。當機、重啟、`stop()` 沒送完的訊息都會從 outbox 接著送；
  結果不確定的（可能已到 Telegram）一律重送同一段文字，寧可多一則、不可以紀錄說沒有而頻道上有。發送器回報的結果
  暫時寫不回 outbox（被外部程式鎖住、磁碟錯誤）時記 ERROR、結果留在記憶體、`A_CHANNEL_OUTBOX_RETRY_SECONDS` 後再記，
  記回之前不交出新的列，所以已送達的不會被重送、同一訊號的出場也不會卡到下次重啟。

**延遲進場不發（使用者 2026-09-28 決定）**：進場訊息實際送出時若已晚於訊號 K 棒收盤超過
`A_CHANNEL_ENTRY_MAX_DELAY_SECONDS`（預設 300 秒），就不發，之後的出場也不發，**報表也不統計這一筆**（outbox 記
`expired` / `no_entry`）。判定點盡量貼近實際送出：發送器在每一次發出請求之前都會再檢查一次（限速、429 等待也算）。
出場訊息晚於平倉超過同一個門檻（或是重啟後補判的出場）照送、加一行延遲註記。**A3 的名目部位與冷卻不受影響**：
這一筆 A3 照樣追蹤、冷卻照算（跟 dry run 一致），只是頻道上看不到、報表不算。

推播要明確帶旗標：

    python -m live.a_channel --duration 3600 --push-tg

不帶 `--push-tg` 時行為與 T2 之前完全相同（不讀 TG 密鑰、不建 outbox）——開發機的長時間實跑不可以把訊息發進正式頻道。
帶旗標但缺密鑰就記 ERROR、exit 1（A3 / A1 都不啟動）。A3 啟動之前 T2 就接好（A3 的重啟補發一啟動就會發布事件），
啟動時記一行 outbox 現況；結束時沒送完的留在 outbox、記 WARNING，下次啟動再送。
**部署前**確認目標機器的 `a_channel_outbox.sqlite3` 不存在或沒有測試資料（否則裡面的待送列會在第一次啟動時發進正式頻道）；
開始有訂閱者之後，開發機的 `--push-tg` 實跑要改用測試頻道（換 `CRYPTO_TRADER_TG_CHANNEL_ID`），不可以與正式環境同時推播同一個頻道。

版面確認用的樣本訊息（寫死的假資料、開頭加 `[測試]`，不碰 outbox、不碰 `live.sqlite3`、不打派網；exit 0 全部送達、
1 有失敗、3 缺密鑰未實測）：

    python -m live.a_channel_push --sample

共 9 則：策略4 進場（該幣上限 ≥ 50x / 20x / 取不到）、策略5 進場、止盈、一般止損、跳空止損、同根兩碰止損、延遲註記的出場。

頻道的設定：**私人頻道**，用「需管理員核准」的邀請連結加入，不綁討論群組；bot 要設為頻道管理員（有發文權限）；
`CRYPTO_TRADER_TG_CHANNEL_ID` 是 `-100` 開頭的數字 ID（直接當 `chat_id` 用）。頻道置頂「所有時間均為台北時間 UTC+8」
由管理者手動發文。T2 相關的執行參數都是 `A_CHANNEL_*` 與 `STRATEGY_LABELS`（`live/config.py`，`python -m live` 會列出）。
離線測試在 `tests/test_a_channel_text.py`（訊息格式黃金樣本）與 `tests/test_a_channel_push.py`（去重、順序、延遲判定、
handler 規則、接線，以及在「寫入 outbox 後」「交給發送器後」兩個時間點硬砍子行程的持久化測試）。

## 方法 A：GitHub Actions（免費、免主機）

1. 在 GitHub 建立一個新的 repo（建議 Private），把這個資料夾的所有檔案上傳，
   **包含 `.github/workflows/dryrun.yml` 這個隱藏資料夾**。
2. 到 repo 的 **Actions** 分頁 → 左側選 `pionex-dryrun` → 按 **Run workflow** 手動跑一次。
3. 看執行紀錄：
   - 成功：repo 會多出 `output/` 和 `state/`，之後依 `.github/workflows/dryrun.yml`
     的排程（目前是 `cron: "17 * * * *"`，即 UTC 每小時第 17 分）每小時自動更新一次。
   - 出現「HTTP 403 … 可能是所在地區/IP 被派網封鎖」：GitHub 的主機在美國，被派網擋掉了，請改用方法 B。
   - commit / push 失敗：到 Settings → Actions → General → Workflow permissions 選 **Read and write permissions**。
4. 看結果：直接在 GitHub 打開 `output/SUMMARY.md`；要看完整報表就下載對應帳本的
   `output/dryrun_<book>.xlsx`（例如 `dryrun_s4.xlsx`）。

備註：GitHub 的排程可能延遲甚至偶爾跳過，但程式會自動補處理漏掉的 K 棒，不影響結果。
這個 repo 目前是 **Public**，所以排程不計免費額度，實際設定就是每小時一次（見上方
`cron` 設定）；若改成 Private repo，執行時間會計入月額度，屆時可考慮把排程拉長
（例如改回每 2 小時）以節省額度。

## 方法 B：日本的 VPS（如果派網擋 GitHub）

請選**日本**機房（派網限制名單包含美國、新加坡、香港）。可用 Oracle Cloud 免費方案
（註冊時把 Home Region 選 Japan East / Tokyo，之後不能改），或每月約 5 美元的一般 VPS。

```bash
sudo apt update && sudo apt install -y python3-venv git
cd ~ && git clone <你的 repo 網址> pionex-dryrun   # 或直接上傳這個資料夾
cd pionex-dryrun
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
./run.sh                      # 先手動跑一次確認沒問題
crontab -e                    # 加入下面這行（每小時第 17 分執行）
17 * * * * /home/ubuntu/pionex-dryrun/run.sh >> /home/ubuntu/pionex-dryrun/cron.log 2>&1
```

若要在手機上隨時看結果，把 VPS 上的資料夾設成 GitHub repo 並設定 deploy key，
`run.sh` 會自動把結果推上去，一樣打開 `output/SUMMARY.md` 就能看。

## 同期基準／市場基準（判斷策略是不是真的有效）

實測「隨機進場」的當日勝率標準差高達 17pt（做多可以在 30%~93% 之間跳動），也就是說
一筆單子會不會贏，當天大盤走勢就解釋掉了絕大部分。只看絕對勝率沒辦法分辨「策略真的
有本事」還是「那幾天剛好順風」。所以每次執行都會額外抽樣「隨機進場」當對照組：從每個
幣隨機抽幾根K棒，用跟該帳本相同的止盈止損模擬「亂進場會怎樣」，逐日累積成基準。

SUMMARY.md 對每個有已平倉交易的帳本，同時列出：

- **絕對勝率**：這本帳實際的勝率。
- **同期基準**：同一段時間、同方向隨機進場的勝率。
- **超額**：絕對勝率 − 同期基準，以及兩者的 95% 信賴區間（按日叢集自助法，避免同一天
  的單子彼此高度相關、把區間算得太窄）。

**兩個指標都要看，缺一不可**：

- 絕對勝率 < 盈虧平衡勝率 → 賠錢，不管超額數字多漂亮都一樣。
- 超額 ≈ 0 → 這本帳只是在吃市場整體的漲跌漂移，不是策略本身的訊號有效，環境一反轉就死。

基準會依「K棒週期 + 止盈 + 止損 + 前視根數」這組簽章分開累積，不同簽章的帳本不會被
拿來互相比較（`watch`/`rev`/`rev_wide` 共用 60M／3%／5% 這組簽章、樣本互通；`s4` 因為
是 5M／4%／5%，另外累積自己的一份，不會被 60M 帳本的基準污染，反之亦然；`s5` 不另外抽樣，
直接借用 60M／3%／5% 那一份，理由見上方「策略5」一節）。
每個簽章各自需要累積到最低樣本數（每日至少 30 筆同方向已結算樣本才會採計）才會顯示
數字，剛換過參數或剛開始跑的帳本會先顯示「同期基準樣本不足，超額待累積」，這是正常現象。

**判定需要足夠的天數，不只是筆數**：按日叢集自助法的有效樣本是「天數」而非「筆數」，
所以要 **30 筆且 8 天**以上才會印出「判定」欄；不足時只列數字並標註「未達判定門檻」。
天數太少時 95% 區間會寬到沒有資訊量（例如只有 2 天時，t 分位是 12.71，算出來的勝率
區間可能整段涵蓋 0~100%），此時 `P(超額>0)=100%` 只是反映「那兩天剛好都贏」，
不代表策略有效。勝率區間另外會夾在 0~100% 之間。

**5 分 K 桶（`s4`）的基準只會涵蓋最近幾天**：帳本一旦有紀錄，每次執行只抓
`WARMUP["5M"]=400` 根（約 33 小時）的K棒，抽樣範圍就只有那一段。第一次回補時範圍較大，
但之後每次只會往前補一天多。這代表 `s4` 回填段（`START_FROM` 到 `forward_from`）那些
較早的交易，永遠不會有對應日期的基準可減，超額只會用得到基準的那幾天來算——這在
「只有前進測試段才算數」的前提下是可以接受的，但看數字時要留意「比對 N 筆」通常
遠小於「已平倉 N 筆」。

## 參數版本控管（中途改參數不會弄髒紀錄，但有例外）

`pionex_dryrun.py` 會對每一本帳的生效參數算一個「指紋」（`fp`，只涵蓋會影響訊號與出場
的設定，不含資金模擬、抓取速度這類不影響交易結果的項目），存進 `state`：

- **指紋沒變** → 照常累積，不受影響。
- **指紋變了**（也就是你改了會被指紋涵蓋的參數）→ 該本帳**自動**清空 `symbols`／
  `closed`，把清空前的紀錄摘要存進 `hist`，並記錄 `forward_from = 這次執行的時間`。
  從 `START_FROM` 到 `forward_from` 之間算「回填（回測）」（等同回測，參數是照著那段
  資料調出來的，不具樣本外意義），`forward_from` 之後才是真正的前進測試，`SUMMARY.md`
  會把這兩段分開統計，不會混在一起算勝率。

也就是說：**測試途中改策略參數是安全操作**，直接改沒關係，程式會自動處理，
**不需要、也不建議手動刪除 `state/` 重新開始**——那樣做會把全部帳本的長期紀錄和
「同期基準」樣本一次歸零，比讓版本控管機制自動分段還要更糟（版本控管只清空指紋
有變動的那一本帳，`state["baseline"]` 完全不受影響）。

（`START_FROM` 不在指紋涵蓋範圍內，改它不會觸發重置，見上方「開始前先確認起算時間」一節。）

### `RESET_BOOKS`：手動一次性強制重置

如果你改了「不在指紋涵蓋範圍內」的東西、或想手動強制重跑某本帳，把帳本名放進
`pionex_dryrun.py` 的 `RESET_BOOKS`：

```python
RESET_BOOKS = ["s4"]      # 2026-09-17：策略4 改為 ret2h .14 / 收盤 .60 / TP 4% —— 跑過一次後請改回 []
```

**這個機制會自動失效**（2026-09-18 起）：同一個參數指紋下只會強制重置**一次**。
帳本名忘了改回 `[]` 也不會每小時清空一次——第二次之後程式判定「這個指紋已經強制重置過」
就跳過。狀態檔裡用 `forced_fp` 記錄這件事。

之後若真的又改了會被指紋涵蓋的參數，指紋改變，本來就會正常重置（並重新記成新指紋下的
一次強制重置），不需要為此再動 `RESET_BOOKS`。

還是建議跑過一次確認生效後把它改回 `[]`，保持設定乾淨；只是忘了改不再有實質代價。

## 注意事項

- 測試途中改策略參數請放心直接改，程式的參數版本控管機制會自動處理（見上方章節）。
  **不要**手動刪除 `state/` 重新開始——那樣做代價比自動版本控管更大（見上方說明）。
- Excel 每次都會重新產生，在 Excel 裡改的參數不會保留；資金模擬的槓桿等設定請改 `BASE_CONFIG`。
- 進出場價格和回測一樣，進場用觸發當根K棒的收盤價；出場優先用止盈/止損價計算，
  沒有滑價，是「理論成交」——**唯一例外**：若某根K棒開盤就已經跳空穿越止損價
  （例如做多時開盤價本身已低於止損價），會改用該根**開盤價**成交（更保守、更貼近
  實際會發生的滑價方向），並在交易紀錄的 `note` 欄標記「開盤跳空穿越止損」；
  止盈方向與 `MAX_HOLD_HOURS` 時間出場（目前所有帳本都設 `None`，不會觸發）
  沒有這個例外。
- 這個「理論成交」跟下面這件事是兩回事，**判定順序 ≠ 成交價格**：如果同一根K棒裡
  止盈價和止損價**同時**被碰到（例如做多時該根最高價已經超過止盈價、最低價也同時
  跌破止損價，分不出來當根到底是先漲到止盈還是先跌到止損），程式不會直接當成止損，
  而是再抓更細的K棒往下判斷先後順序（`RESOLVE_INTERVALS`：`watch`/`rev`/`rev_wide`
  這些 60M 帳本依序試 `5M`→`15M`→`30M`；`s4` 用 `1M`；`s5` 本身就是 1 分K、沒有更細的
  週期可查，直接保守判定為止損），用細週期K棒裡誰先觸價來
  判定當根到底算止盈還是止損。如果細週期抓不到資料，或細週期裡同一根還是同時碰到
  兩者，才會保守判定為止損。判定出來是哪個結果之後，仍然套用上一條「理論成交」的
  同一套定價邏輯（止盈用止盈價、止損視有無跳空決定用止損價或開盤價），這個機制只
  影響「算贏還是算輸」，不影響「用什麼價格成交」。
- `START_FROM` 有設值（不是 `None`）時，**第一次執行會回補**這個時間之後的所有K棒
  （實際能回補到多早，受交易所保留上限限制：60M K棒約 416 天、5M K棒（`s4` 用）約
  34 天，取兩者中較晚的日期為準；超過時程式會印出警告。`s5` 例外：它用自己的
  `BOOK_START_FROM["s5"]`，且最多往前回補 `S5_MAX_BACKFILL_HOURS` 小時），細節見上方「開始前先確認
  起算時間」一節。**只有 `START_FROM = None` 時**才是「不回補歷史，只從第一次執行
  之後的 K 棒開始統計」。這是「第一次執行」才有的行為；帳本一旦有紀錄之後，之後每次
  執行都只處理上次執行之後新收完的K棒，不會重複回補。