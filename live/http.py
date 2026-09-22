# -*- coding: utf-8 -*-
"""
live.http — 對派網公開端點的最小 HTTP 取數
=========================================
只做四件事：GET、解 result/code/message 信封、短暫重試（含 429 退避）、把 SSL 憑證錯誤
講清楚。沒有簽章、沒有認證、沒有 rate limiter、沒有連線池、沒有 class。live/ 目前
每小時只打兩個公開端點，這樣就夠了；哪天真的需要簽章端點再由那個任務加。

為什麼不直接用 pionex_backtest.api_get()：
    live/ 不可 import 根目錄那幾支 pionex_*.py（見 live/__init__.py）。這裡的寫法刻意
    對齊它的信封解析與退避節奏，但是獨立的一份，之後兩邊各走各的。

關於 CA 憑證：
    程式裡不內建任何憑證路徑，這套東西最終要跑在雲端主機上，機器專屬的東西不能寫進
    code。若某台機器所在的網路有 SSL inspection，requests 本身就認得環境變數
    REQUESTS_CA_BUNDLE：把該機器的 CA bundle 路徑放進去再啟動即可，程式不用改。

模組名的注意事項：
    這支叫 live.http，跟標準庫的 http 套件無關，一律用 `from live.http import api_get`。
    不要 `python live/http.py` 直接執行它 —— 那會把 live/ 放到 sys.path 最前面，
    requests 內部 import http.client 時就會撞到這個檔。從 repo 根目錄用
    `python -m live.<module>` 或跑測試都沒有這個問題。
"""

import time

import requests

BASE_URL = "https://api.pionex.com"
DEFAULT_TIMEOUT = 20      # 秒；單一請求的連線 + 讀取上限
DEFAULT_RETRIES = 3       # 網路抖動 / 429 / 5xx 的總嘗試次數
_HEADERS = {"User-Agent": "crypto-trader-live/1.0"}


class ApiError(Exception):
    """派網回了錯誤（HTTP 非 200、result=false、重試用盡、SSL 驗證失敗）。"""


def api_get(path, params=None, retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT):
    """GET BASE_URL + path，回傳解析後的完整信封 dict（含 result/code/message/data）。

    成功的定義：HTTP 200 且 body 是 JSON 且 result 為 true。其他一律拋 ApiError，
    訊息一定帶端點 path，讓上層記 log 時看得出是哪個端點出事。

    重試規則（總共嘗試 retries 次）：
        429            退避 2**k 秒後重試（k 從 0 起算）
        5xx / 連線錯誤 / 逾時
                       等 k+1 秒後重試
        403 / 451      不重試，直接拋（多半是地區或 IP 被封）
        其他 4xx       不重試，直接拋
        result=false   不重試，直接拋（訊息帶 code 與 message）
        SSL 憑證錯誤   不重試，直接拋，訊息說明要指定 CA 憑證
    """
    url = BASE_URL + path
    last = None
    for k in range(retries):
        try:
            r = requests.get(url, params=params, headers=_HEADERS, timeout=timeout)
        except requests.exceptions.SSLError as e:
            # SSLError 是 ConnectionError 的子類，必須先接
            raise ApiError(
                f"{path}: SSL 憑證驗證失敗（{e}）。"
                "這台機器需要指定 CA 憑證：把本機的 CA bundle 路徑放進環境變數 "
                "REQUESTS_CA_BUNDLE 後重新啟動；程式本身不內建任何憑證路徑。"
            ) from e
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last = e
            time.sleep(1.0 * (k + 1))
            continue

        if r.status_code == 429:
            last = ApiError(f"{path}: HTTP 429 請求過於頻繁")
            time.sleep(2 ** k)
            continue
        if r.status_code in (403, 451):
            raise ApiError(f"{path}: HTTP {r.status_code} 連線被拒，可能是所在地區或 IP 被派網封鎖")
        if r.status_code >= 500:
            last = ApiError(f"{path}: HTTP {r.status_code} {r.text[:200]}")
            time.sleep(1.0 * (k + 1))
            continue
        if r.status_code != 200:
            raise ApiError(f"{path}: HTTP {r.status_code} {r.text[:200]}")

        try:
            js = r.json()
        except ValueError as e:
            raise ApiError(f"{path}: 回應不是 JSON：{r.text[:200]}") from e
        if not isinstance(js, dict) or not js.get("result", False):
            code = js.get("code") if isinstance(js, dict) else None
            message = js.get("message") if isinstance(js, dict) else str(js)[:200]
            raise ApiError(f"{path}: result=false code={code} message={message}")
        return js

    raise ApiError(f"{path}: 重試 {retries} 次仍失敗：{last}")
