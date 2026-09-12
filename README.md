# 派網動能策略 Dry Run

不下單、持續統計的前瞻測試。每次執行會處理上次之後新收完的 1 小時 K 棒，
進出場規則與 `pionex_backtest.py` 完全相同，結果可直接和回測比較。

## 檔案

| 檔案 | 用途 |
|---|---|
| `pionex_dryrun.py` | 主程式（策略參數在檔案開頭 `BASE_CONFIG`） |
| `pionex_backtest.py` | 回測程式，dry run 會直接使用裡面的指標與出場邏輯 |
| `.github/workflows/dryrun.yml` | GitHub Actions 排程設定 |
| `run.sh` | 在 VPS / 自己主機上用 crontab 執行 |
| `output/SUMMARY.md` | 自動產生的摘要（在 GitHub 網頁或手機 App 直接可看） |
| `output/dryrun_main.xlsx` | 主策略完整報表（含 ATR 區間監控、運行紀錄） |
| `output/dryrun_watch.xlsx` | 觀察組（ATR ≥ 2%）報表 |
| `state/dryrun_state.json` | 持倉與交易紀錄，**不要手動修改** |

## 方法 A：GitHub Actions（免費、免主機）

1. 在 GitHub 建立一個新的 repo（建議 Private），把這個資料夾的所有檔案上傳，
   **包含 `.github/workflows/dryrun.yml` 這個隱藏資料夾**。
2. 到 repo 的 **Actions** 分頁 → 左側選 `pionex-dryrun` → 按 **Run workflow** 手動跑一次。
3. 看執行紀錄：
   - 成功：repo 會多出 `output/` 和 `state/`，之後每 2 小時自動更新。
   - 出現「HTTP 403 … 可能是所在地區/IP 被派網封鎖」：GitHub 的主機在美國，被派網擋掉了，請改用方法 B。
   - commit / push 失敗：到 Settings → Actions → General → Workflow permissions 選 **Read and write permissions**。
4. 看結果：直接在 GitHub 打開 `output/SUMMARY.md`；要看完整報表就下載 `output/dryrun_main.xlsx`。

備註：GitHub 的排程可能延遲甚至偶爾跳過，但程式會自動補處理漏掉的 K 棒，不影響結果。
Private repo 的免費執行時間有月額度，預設每 2 小時一次用量不大；Public repo 不計時，可改成每小時。

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

## 注意事項

- **測試途中不要改策略參數**，否則前後紀錄會混在一起。真的要改，請刪掉 `state/` 重新開始。
- Excel 每次都會重新產生，在 Excel 裡改的參數不會保留；資金模擬的槓桿等設定請改 `BASE_CONFIG`。
- 進出場價格和回測一樣以 K 棒收盤價、止盈止損價計算，沒有滑價，是「理論成交」。
- 開始時不回補歷史，只從第一次執行之後的 K 棒開始統計。
