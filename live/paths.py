# -*- coding: utf-8 -*-
"""
live.paths — 執行期資料路徑的單一來源
=====================================
實盤系統跑起來之後產生的東西（SQLite、日誌、任何本機狀態）一律寫在 repo 根目錄的
runtime/ 底下，runtime/ 已經列進 .gitignore，不進版控。

為什麼不能寫進 state/ 或 output/：
    .github/workflows/dryrun.yml 與 run.sh 每小時跑完 dry run 之後會執行
    `git add state output`，接著 commit 並 push。實盤的資料庫與日誌只要落在那兩個
    目錄裡，就會被每小時自動 commit 進版控 —— 交易紀錄與日誌進了 repo、repo 一路
    膨脹，而且不會有人立刻發現。

路徑一律從這裡取，不要在各自的模組裡再 os.path.join 拼一次。推導方式是相對 repo
根目錄（跟 pionex_dryrun.py 的 HERE 同一招），不寫死任何機器專屬的絕對路徑，因為
這套東西最終要搬到雲端主機上跑。

import 時不會建立目錄。要寫東西進去的人自己 os.makedirs(RUNTIME_DIR, exist_ok=True)。
"""

import os

# 本檔案位在 <repo>/live/paths.py，往上兩層就是 repo 根目錄
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 實盤執行期資料的根目錄
RUNTIME_DIR = os.path.join(REPO_ROOT, "runtime")
