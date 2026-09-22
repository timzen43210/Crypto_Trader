# G2 — 5 分 K 即時資料層可行性量測　操作說明

這份文件寫給**在 laptop 上直接開終端機跑指令的人**，不需要開 Claude。
每一段指令都可以整行複製貼上。看到 `>>>` 開頭的是**分支點**，要照上面的結果決定往哪走。

---

## 這是在量什麼（先看懂再跑，跑錯白花兩小時）

策略4 以**收盤價**進場，所以「5 分 K 收盤 → 算出訊號」這段延遲會直接變成滑價。
派網的公開 WebSocket **沒有 K 線 topic**，只有逐筆成交（`TRADE`），而且官方說明只推
**taker 方向**的成交；REST 一次只能查一個 symbol，611 個幣以 10 req/s 上限要打 61 秒，
但一根 5 分 K 只有 300 秒。

所以要在兩條路裡選一條，而這五個階段就是在蒐集選路需要的證據：

| 方案 | 做法 | 這次要量的關鍵數字 | 對應階段 |
|---|---|---|---|
| A | 訂閱 `TRADE`，本地自己聚合成 5 分 K | 本地聚合的 **volume 能不能還原**交易所 K 棒 | **Stage 1（生死關）** |
| A | 同上 | 全部候選訂閱得完嗎？收盤到算完要幾秒？ | Stage 2 |
| B | 先用 24h 成交額粗篩，只對候選打 REST | 粗篩會**漏掉多少訊號** | Stage 4 |
| 共用 | — | 611 個幣算完指標要多久 | Stage 3 |

**Stage 1 沒過，方案 A 就不成立，Stage 2 不用跑。** 這是整份流程唯一的硬分支。

---

## 0. 前置檢查（約 2 分鐘）

### 0.1 拿到程式碼

```
cd D:/Crypto_Trader
git checkout stage
git pull
```

第一行請換成你自己放 repo 的路徑（上面只是範例）。
**不要照抄成 `cd <你的目錄>`** —— PowerShell 會把 `<` `>` 當成重新導向運算子，
整行貼上會得到 `ParserError`，跟腳本無關。

### 0.2 確認 Python 與套件

```
python --version
python -m pip install -U pandas numpy requests websockets
```

- Python 需要 **3.10 以上**。
- `websockets` 需要 **12 以上**（Stage 1/2 用到 `websockets.asyncio.client`）。
  版本太舊腳本會直接告訴你，照著它說的指令升級即可。
- Stage 3/4 只需要 pandas / numpy，不需要網路。

### 0.3 確認網路

Stage 0/1/2 要連 `api.pionex.com` 與 `wss://ws.pionex.com`。
**請確認現在不是走公司網路**（公司網路會對外部 API 做 TLS 攔截，wss 通常直接被擋）。

### 0.4 確認快取（只有 Stage 4 需要）

```
python -c "import pathlib;g=sorted(pathlib.Path('pionex_cache').glob('*_5M.csv'));print(len(g),'個 5M 檔');print([f.name for f in g[:5]])"
```

（用 python 而不是 `ls ... | head`：PowerShell 沒有 `head`，Windows 的 `ls` 也不是 Unix 那個。）

數字大於 0、看得到一堆 `XXX_USDT_PERP_5M.csv` 就可以。
印出 `0 個 5M 檔` 的話把上面指令裡的 `_5M` 換成 `_60M` 再試一次；
如果只有 `_60M.csv`，Stage 4 要改成 `--interval 60M`（腳本會告訴你）。

---

## 1. 先跑離線自檢（不連網，約 30 秒）

```
python research/g2_selftest.py
```

這支用假的 API 回應與假的 WebSocket，把五個階段的主流程與錯誤處理各跑一遍。
**它跟派網完全無關，斷網也能跑。**

- 看到**倒數第二行** `結果：44 通過 / 0 失敗 / 共 44 項` → 這台機器的環境沒問題，往下走。
  （最後一行是 `全部通過。可以按 ...` 那句。）
  通過數會隨著自檢長大而變多，**只要 `0 失敗` 就算過**，不用跟這裡的 44 對得一模一樣。
- 有 FAIL → **先不要跑真實量測**。把整段輸出貼回去給開發者。
  加 `-v` 可以看到完整 traceback：`python research/g2_selftest.py -v`

自檢的產物全部在 `research/_tmp/selftest/`（不進版控），不會動到 `research/results/`。

---

## 2. 執行順序總表

| 階段 | 指令 | 要網路 | 大約耗時 | 產出檔 |
|---|---|---|---|---|
| 自檢 | `python research/g2_selftest.py` | 否 | 30 秒 | 無（`_tmp` 內） |
| 0 | `python research/g2_measure.py --stage 0` | 是 | 10 秒 | `research/results/stage0_universe.json` |
| 1 冒煙 | `python research/g2_measure.py --stage 1 --smoke` | 是 | 約 4 分 | 同下（會被正式跑覆蓋，舊檔自動備份） |
| 1 正式 | `python research/g2_measure.py --stage 1` | 是 | 約 25 分 | `research/results/stage1_aggregation.json` |
| 2 冒煙 | `python research/g2_measure.py --stage 2 --smoke` | 是 | 約 5 分 | 同下 |
| 2 正式 | `python research/g2_measure.py --stage 2 --connections 10` | 是 | 約 110 分 | `research/results/stage2_latency.json` |
| 3 | `python research/g2_measure.py --stage 3` | 否 | 1 分 | `research/results/stage3_compute.json` |
| 4 | `python research/g2_measure.py --stage 4` | 否 | 2–10 分 | `research/results/stage4_screen.json` |

**每個階段都是獨立的**，可以分次跑、換天跑。前面階段的結果不會因為後面失敗而消失。
如果某個結果檔已經存在，腳本會先把舊的搬到 `research/_tmp/` 保存再寫新的，並在畫面上告訴你搬去哪。

> 想省時間就照 Stage 3 → 4 → 0 → 1 → 2 的順序：3 和 4 不用網路，可以先確認算得出東西。
> Stage 1/2 會沿用 Stage 0 存下來的候選清單；沒跑過 Stage 0 的話它們會自己現抓一次
> （畫面上會說「找不到可用的 Stage 0 結果」），還是跑得動，但建議先跑 Stage 0，
> 這樣兩個階段用的是同一份清單，回去比對時才對得起來。

---

## 3. 逐階段

### Stage 0 — 環境探測（10 秒，個位數請求）

```
python research/g2_measure.py --stage 0
```

**畫面上要看到**：`symbols：611 個（TRADING 611 個）` 之類的數字，
以及 `候選（24h amount 落在 [20000, 500000]）：xxx 個`。

- 候選數應該是 **兩百多個**（2026-09-18 實測 277）。差太多要留意，但不影響後續執行。
- `riskTable` 抓不到會印警告然後繼續跑，**這是正常的**（那個端點路徑沒有實測過，
  它只是順手蒐集的附帶資訊，不影響 G2 的結論）。

### Stage 1 — TRADE 能不能還原 volume（**生死關**）

先冒煙。冒煙版刻意改用 **1 分 K**、2 個 symbol、1 根，所以四分鐘就能跑完整條路徑
（含收盤後回頭打 REST 比對），確認「這台機器真的跑得動」再投入 25 分鐘。

```
python research/g2_measure.py --stage 1 --smoke
```

**冒煙的「判定：」不算數。** 樣本是 1 分 K × 2 個 symbol × 1 根，統計上沒有意義，
所以畫面上那兩行會長這樣，跟正式版分得出來：

```
※ 冒煙模式：以下判定不算數，只確認流程跑得完。不論結果是什麼，都要再跑一次正式版。
判定：【冒煙・不算數】volume 可還原（初判通過） — 冒煙樣本在統計上沒有意義，...
下一步：請接著跑正式版：python research/g2_measure.py --stage 1
```

看到 `【冒煙・不算數】` 就代表這是冒煙結果，**不管它寫可還原還是不可還原，都不能當結論**，
也不要拿它去決定要不要跑 Stage 2。冒煙只回答一件事：這台機器跑得完整條路徑嗎？

冒煙跑完、沒有紅字，再跑正式版：

```
python research/g2_measure.py --stage 1
```

正式版 = 10 個樣本 symbol、4 根完整 5 分 K，最後一根收盤後再等 30 秒才去抓交易所 K 棒比對。
中間會每封存一根就印一行，並且**每根都立刻寫進結果檔**，跑到一半斷掉前面的不會白費。

**跑完看 `判定：` 與 `下一步：` 那兩行**（在最後的 `已寫出 ...` 上面），長這樣：

```
可比對（clean）K 棒：40 / 總 40
volume_ratio（本地 ÷ 交易所） 中位數 1.0000  p05 0.9998  p95 1.0002
close 相對誤差 中位數 0.000000
判定：volume 可還原（初判通過） — ...
下一步：可以進行 Stage 2
已寫出 research/results/stage1_aggregation.json
```

中間可能多插一行 `[注意] 有 N 根在封存後還收到該根的成交`，那不是錯誤，
意思是 `--seal-grace` 給得不夠久：畫面上的比值已經用回填後的事後真值算過了，
而封存當下的（被截斷的）比值另外存在 json 的 `summary.volume_ratio_at_seal`。
兩個值差很多的話，回 office 判讀時要以事後真值為準。

> `>>> 分支點 <<<` 照畫面印出的「判定：」那四個字決定：
>
> - **`volume 可還原（初判通過）`** → 往下跑 Stage 2。
> - **`疑似交易所計雙邊量`**（`volume_ratio` 中位數落在 0.5 附近）→ **還是要跑 Stage 2**。
>   這代表本地只收到 taker 單邊、量剛好是交易所的一半，可能用一個固定係數換算回來，
>   方案 A 還沒死。但係數穩不穩定要回 office 看逐根資料才知道。
> - **`volume 無法還原（初判不通過）`**（比值偏離 1 又不穩定）→ **方案 A 不成立，
>   Stage 2 不用跑**，直接跳到 Stage 3、Stage 4，然後把結果帶回。
> - **`無法判定`**（可比對的 K 棒是 0，通常是樣本太冷門完全沒成交）→ 加大樣本重跑：
>   `python research/g2_measure.py --stage 1 --bars 6 --n-symbols 12`
>
> 每種判定畫面上都會跟著印一句「這是什麼意思」和建議的下一步，照著做即可。

這個「初判」只是幫你決定要不要繼續跑，**真正的結論是回到 office 看逐根原始資料才下的**。
結果檔裡本地值與交易所值是逐根並列的，不是只有統計摘要。

### Stage 2 — 訂閱容量與端到端延遲

**只在 Stage 1 通過時才跑。**

```
python research/g2_measure.py --stage 2 --smoke
python research/g2_measure.py --stage 2 --connections 10
```

正式版要 **約 110 分鐘**（20 根 5 分 K）。開跑前：

- **把筆電的睡眠 / 休眠關掉**，螢幕可以關，但系統睡著 WebSocket 就斷了。
- 不要中途切換網路（換 Wi-Fi、開關 VPN 都會斷線）。
- 斷線腳本會自動重連，**重連期間的 K 棒會被標記 `degraded`，不列入乾淨統計**，
  所以偶爾斷一下不會毀掉整批資料，只是那幾根不算數。

跑完看：

```
訂閱 xxx 個耗時 xx.x 秒
total_ms（K 棒收盤 → 指標算完）中位數 xxxx ms，最大 xxxx ms
```

中位數在 10000 ms（10 秒）以內是好消息，30000 ms（30 秒）以上是壞消息。
**不管好壞都照樣把檔案帶回**，判斷在 office 做。

時間不夠就減少根數，例如只跑 8 根（約 45 分鐘）：

```
python research/g2_measure.py --stage 2 --connections 10 --bars 8
```

### Stage 3 — 指標計算耗時（不用網路，1 分鐘）

```
python research/g2_measure.py --stage 3
```

laptop 上有 `pionex_cache` 就會自動用真實資料（`--source auto` 是預設）。
畫面會印 `資料來源：cache（...）` 或 `synthetic（...）`，兩者都可以，帶回去時說明清楚即可。

要看的是 `推估 611 個 symbol：xxxx ms（K 棒週期 300 秒，放得下 / 放不下）` 那一行，
以及它上面兩行的每 frame 中位數與合計耗時。

**這個數字只看數量級，不要比小數點。** 同一台機器同一組參數連跑三次，中位數實測落在
1.783 / 1.200 / 1.361 ms——單次執行的抖動可以到數十 %（背景程式、CPU 降頻都會影響）。
結論只該長成「個位數毫秒、放得下」或「數百毫秒、要小心」，不該長成「1.2 ms 比 1.4 ms 快」。

### Stage 4 — 方案 B 粗篩會漏多少訊號（不用網路）

```
python research/g2_measure.py --stage 4
```

用 `pionex_cache` 的歷史資料回放，比較「全量計算」與「先粗篩再計算」兩者的訊號集合差異。

- 跑太久就限制範圍：`--max-symbols 150` 或 `--replay-bars 3000`
- 快取只有 60 分 K：`--interval 60M`

輸出的重點是那張漏失率表，四個 margin 與三個 LAG 全部印出來：

```
漏失率（列＝粗篩落後幾根 LAG，欄＝margin 放寬倍數）：
  LAG   margin=1       margin=0.75    margin=0.5     margin=0.25
  0     0.00%          0.00%          0.00%          0.00%
  1     75.00%         25.00%         0.00%          0.00%          ← 主值
  2     50.00%         25.00%         0.00%          0.00%
  每根 K 棒候選數（611 symbol 推估）：margin=1→71.9  margin=0.75→93.4  ...
```

**LAG 那一欄不是「取大一點比較保守」。** `turn` 是 24 小時滾動和，一筆大成交額
*滾入*與*滾出*窗口是兩個不同時刻的事件，所以漏失率對 LAG **不是單調的**
（上面那組實測就是 LAG=1 漏 75%、LAG=2 反而只漏 50%）。要看整條曲線，不要只挑一個數字。

margin 那幾欄則是「放寬粗篩區間能換回多少訊號」，代價是最後一行的候選數變多、
每根 K 棒要多打幾次 REST。兩邊一起看才是方案 B 的取捨。

**這個漏失率是雙向有偏、偏移量未知，既不是上界也不是下界。** 兩個方向相反的誤差同時存在：
取 LAG≥1 比實盤悲觀（讓量到的數字偏高），而實盤粗篩讀的是交易所 24h amount 欄位、
跟這裡用 klines 加總出來的 `turn` 不是同一個東西，那個定義落差會額外增加漏失（讓真值偏高）。
畫面最後兩行會再提醒一次。判讀時不要把任何一個數字當成單邊的界。

---

## 4. 跑到一半中斷了怎麼辦

- **直接重跑同一行指令就好。** 腳本會把上一次的結果搬到 `research/_tmp/` 保存
  （畫面上會印出搬到哪），再重新開始蒐集。
- Stage 1/2 是邊跑邊寫檔，所以中斷前已經封存的 K 棒都在那個被搬走的備份檔裡。
  如果那次已經跑了很久（例如 Stage 2 跑到第 15 根才斷），**備份檔也一起帶回**，
  不要覺得沒跑完就沒價值。
- 想提前收工又要保留結果：按 `Ctrl+C`，腳本會把已有資料寫檔再離開。

---

## 5. 常見錯誤與處置

腳本的錯誤訊息都會直接告訴你該做什麼（不會只吐一堆 traceback）。這裡是對照表：

| 畫面訊息（開頭） | 意思 | 怎麼辦 |
|---|---|---|
| `Stage 1 需要的套件沒有裝：websockets` | 套件沒裝 | 照訊息裡那行 `pip install` 跑一次，再重跑同一行指令 |
| `連不上 WebSocket ...` | wss 連不上 | 確認不是公司網路 / VPN；手機熱點通常可以 |
| `SSL 憑證驗證失敗 ...` | 網路中間有 TLS 攔截 | 換網路。真的必須用這個網路才加 `--ca-bundle <根憑證.pem>` |
| `HTTP 429 ...` | 打太快被限流 | 腳本會自己退避降速並繼續，**不用管**。反覆出現就加 `--rate 3` 重跑 |
| `... 回應不是 JSON（HTTP 200）` | 被 proxy 或登入頁攔截 | 換網路 |
| `所有 symbol 都取不到 24h 成交額欄位` | 派網 API 欄位改了 | 把結果檔裡的 `raw_ticker_sample` 帶回給開發者 |
| `收到 TRADE 推送但拆不出 價格/數量/時間戳` | 推送格式跟假設不同 | 把 `stage1_aggregation.json` 帶回（原始訊息已經存在裡面了） |
| `找不到快取目錄：...` | 沒有 `pionex_cache` | Stage 3 加 `--source synthetic`；Stage 4 需要真實快取才有意義 |
| `快取目錄 ... 裡沒有任何 *_5M.csv` | 快取是別的週期 | 加 `--interval 60M`（或快取裡實際有的週期） |
| `--frame-bars xxx 太少` | 根數不夠暖機 | 照訊息給的最小值調大 |
| `can't open file ... g2_measure.py` / `No such file or directory` | `git pull` 拿到的分支上還沒有 `research/` | 先確認 `git log --oneline -1` 是不是你預期的那個 commit；`research/` 還沒推上 `stage` 的話，請開發者推上去或直接把 `research/` 整個資料夾複製到 laptop 的 repo 根目錄下 |
| `[預期外的錯誤] XxxError: ...`（下面跟著一整片 traceback） | 遇到寫腳本時沒想到的情況 | 腳本會印出結果檔的位置與 `script_sha256`，**把那整段訊息連同結果檔一起帶回**，只帶結果檔沒辦法追 |

離開碼：`0` 正常、`2` 已預期的錯誤（畫面上有可操作的指引）、`3` 達到 `--max-requests` 上限、
`4` 預期外的例外、`130` 被 Ctrl+C 中斷。排程或腳本包起來跑時可以用這個判斷。

---

## 6. 把結果帶回

```
git add research/results
git commit -m "G2 measurement results from laptop"
git push
```

`research/results/` 底下的 json **全部都要帶回**，包括跑失敗那次留下的。
`research/_tmp/` 不在版控裡（故意的，那是中間產物）——但如果第 4 節提到「備份檔也要帶」，
就手動把那幾個檔複製到 `research/results/` 再 `git add`。

帶回去之後請一併說明：

- 每個階段實際是在什麼網路環境跑的（家用 Wi-Fi / 手機熱點）
- 中途有沒有斷線、睡眠、切換網路
- 有沒有任何「畫面上看起來怪怪的」訊息

這些資訊 json 裡記不到，但會影響怎麼解讀數字。

---

## 7. 不要做的事

- **不要同時跑兩個階段**。REST 與 WS 的限額是 per IP 的，同時跑會互相打架，
  量出來的延遲也沒有意義。
- **跑 Stage 0/1/2 的時候，先把 `pionex_strategy4.py` / `pionex_dryrun.py` 停掉。**
  派網的 10 req/s 與 10 條 WS 連線都是**整台機器共用一個 IP** 在算的。
  策略還在背景跑的話，兩邊會互相搶額度：你可能被 429 封鎖，而正在跑的策略也會被牽連。
  這是最容易被忽略、後果又最麻煩的一條。
- **不要為了快把 `--rate` 調到 10 以上**。派網上限就是 10 req/s，超過會封鎖 IP
  60 秒起跳，每多打一次再加 10 秒。預設的 5 是留安全邊際，夠用。
- **不要調 `--sub-rate` 超過 5**。每條連線每秒送 5 則訊息是硬上限。
- **不要手動編輯 `research/results/` 裡的 json**。要註記什麼寫在 commit message 或訊息裡。
- 這些腳本**不下單、不需要 API key、不碰帳戶**，全程只讀公開行情。
  它也不會寫入 `pionex_cache/`。
