# -*- coding: utf-8 -*-
"""
live.pionex_api — 對派網公開端點的最小 HTTP 取數
===============================================
只做四件事：GET、解 result/code/message 信封、短暫重試（含 429 退避）、把 SSL 憑證錯誤
講清楚。沒有簽章、沒有認證、沒有 rate limiter、沒有連線池、沒有 class。live/ 目前
每小時只打兩個公開端點，這樣就夠了；哪天真的需要簽章端點再由那個任務加。

BASE URL、timeout、retries 這三個是設定，放在 live.config（執行參數那一層），這裡只取用。
端點路徑不是設定，留在各自的模組裡 —— 換掉端點路徑等於換一支 API。

為什麼不直接用 pionex_backtest.api_get()：
    live/ 不可 import 根目錄那幾支 pionex_*.py（見 live/__init__.py）。這裡的寫法刻意
    對齊它的信封解析與退避節奏，但是獨立的一份，之後兩邊各走各的。

關於 CA 憑證：
    程式裡不內建任何憑證路徑，這套東西最終要跑在雲端主機上，機器專屬的東西不能寫進
    code。若某台機器所在的網路有 SSL inspection，requests 本身就認得環境變數
    REQUESTS_CA_BUNDLE：把該機器的 CA bundle 路徑放進去再啟動即可，程式不用改。

模組名的由來：
    這支原本的檔名是 live/ 底下的 http.py，跟標準庫的 http 套件撞名。四種正規執行方式都
    沒事，但只要有人直接執行 live/ 裡的單一檔案、或設了 PYTHONPATH=live，live/ 就會排到
    sys.path 最前面，requests 內部 import http.client 時撞到自己人 —— 而症狀會長成
    「requests 壞掉」，極難追。後果不對稱，所以就算正規路徑都沒事也值得改名。
    叫 pionex_api 而不是 api，是因為它日後可能承載派網的簽章端點，名稱要說得出這是
    哪一家的 API。
"""

import time

import requests

from live import config

_HEADERS = {"User-Agent": "crypto-trader-live/1.0"}


class ApiError(Exception):
    """派網回了錯誤（HTTP 非 200、result=false、重試用盡、SSL 驗證失敗）。"""


def api_get(path, params=None, retries=config.HTTP_RETRIES, timeout=config.HTTP_TIMEOUT_SECONDS):
    """GET PIONEX_BASE_URL + path，回傳解析後的完整信封 dict（含 result/code/message/data）。

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
    只有「後面還要再試一次」才會 sleep：最後一次失敗直接拋，不再白等那一輪。
    """
    url = config.PIONEX_BASE_URL + path
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
            if k < retries - 1:
                time.sleep(1.0 * (k + 1))
            continue

        if r.status_code == 429:
            last = ApiError(f"{path}: HTTP 429 請求過於頻繁")
            if k < retries - 1:
                time.sleep(2 ** k)
            continue
        if r.status_code in (403, 451):
            raise ApiError(f"{path}: HTTP {r.status_code} 連線被拒，可能是所在地區或 IP 被派網封鎖")
        if r.status_code >= 500:
            last = ApiError(f"{path}: HTTP {r.status_code} {r.text[:200]}")
            if k < retries - 1:
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
