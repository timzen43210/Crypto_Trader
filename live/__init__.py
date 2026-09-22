# -*- coding: utf-8 -*-
"""
live — 實盤 / paper trading 執行期套件
=====================================
這個套件放「實際在跑的那一套」專屬的執行期邏輯：行情取得、事件流轉、部位追蹤、對外
推播，以及它們需要的設定、日誌與持久化。回測與 dry run 那一側不會 import 這裡的東西。

這裡不放：
  ・訊號計算邏輯 —— 那是 strategy/ 的，只能 import，不可複製一份過來自己改。
    抄一份的下場就是兩邊慢慢走樣，回測跑出來的數字不再代表實盤實際在做什麼。
  ・回測與 dry run 的程式 —— 根目錄那幾支 pionex_*.py 自成一套，不動它們。

唯一允許的跨套件相依方向：live/ → strategy/

  live/ 不可 import pionex_backtest / pionex_dryrun / pionex_strategy4。
  那三支在 import 當下就會建立全域 CONFIG 並帶其他副作用，而且正在被其他任務改動中；
  實盤端對回測端要零相依，才不會有人調回測參數時順手改掉實盤行為。

  paths   執行期資料路徑（runtime/）的單一來源
"""
