# -*- coding: utf-8 -*-
"""
strategy — 純訊號邏輯套件
=========================
這個套件只放「給定 K 棒 DataFrame → 算出進場訊號」的純函數，只 import pandas / numpy
與標準庫，不碰任何網路、檔案、Excel 或全域 CONFIG，讓回測（pionex_strategy4.py）與
實盤 / paper trading 端可以 import 同一份訊號邏輯，不會各抄一份而漸漸走樣。
標的池的分類規則（哪些合約可以交易）也放這裡，理由相同：規則只有一份可執行的來源。

  s4_signal   策略4 爆量竭盡（只做空）
  s5_signal   策略5 群組訊號複製（訊號核心與參數的唯一來源）
  universe    實盤標的池的分類規則與人工覆寫名單（唯一的一份；只用標準庫）
"""
