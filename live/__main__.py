# -*- coding: utf-8 -*-
"""
python -m live — 環境與路徑冒煙檢查
===================================
搬到一台新主機上之後第一個該跑的指令，用來回答三個問題：環境裝對了嗎、路徑對嗎、
設定與密鑰齊不齊。

印出 Python 版本、requirements-live.txt 實際列的那些套件裝了沒（版本多少）、
runtime/ 解析到哪裡與能不能寫、live.config 的執行參數實際值、日誌會寫到哪裡與能不能寫、
每個密鑰有沒有設，以及這份 code 是哪個 commit，然後 exit 0。

日誌只報告「setup() 會寫到哪裡」，這裡不呼叫 live.logsetup.setup()、不產生任何日誌檔。

stdout 接到 pipe 時 Windows 用的是系統地區編碼（cp1252 / cp950），印中文會直接
UnicodeEncodeError 死掉，所以 main() 一開始先把 stdout 換成 UTF-8（跟各測試 runner、
market_static 的 probe 同一招）。只在 main() 裡做，import 本模組不動任何全域串流。

密鑰只報告「已設定 / 未設定」，絕不印值也不印前幾碼 —— 只有一個訂閱者的 TG bot
token 露出前幾碼就已經是實質洩漏。缺密鑰時 exit code 一樣是 0：這支是報告，不是
放行閘門，沒有用到 TG 的元件在沒設密鑰的機器上照樣要能跑。

它只印資訊，不做任何事，也不建立任何目錄。業務邏輯不要加到這裡來。

執行：python -m live
"""

import os
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version

from live import config
from live.paths import REPO_ROOT, RUNTIME_DIR

REQUIREMENTS_LIVE = os.path.join(REPO_ROOT, "requirements-live.txt")


def _requirement_names(path):
    """把 requirements-live.txt 裡的套件名撈出來（略過註解與空行）。

    直接讀檔而不是寫死一份清單，是為了讓之後往 requirements-live.txt 加套件的人
    不必再回頭改這支：加了就會自動被檢查到。

    刻意寫得很笨，只取到第一個版本符號為止，不處理 -r / -e / 環境標記這些目前
    用不到的語法；哪天檔案裡真的出現那些東西再說。
    """
    names = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            for sep in ("==", ">=", "<=", "~=", "!=", ">", "<", "[", ";", " "):
                line = line.split(sep, 1)[0]
            line = line.strip()
            if line:
                names.append(line)
    return names


def _git_commit():
    """目前的 git commit；取不到就回 unknown，不讓冒煙檢查因為這個失敗。"""
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=5,
        )
    except Exception:
        return "unknown"
    if r.returncode != 0:
        return "unknown"
    return r.stdout.strip() or "unknown"


def _make_stdout_safe():
    """讓後面的 print() 在任何終端機編碼下都不會因為中文而崩潰。

    sys.stdout 可能是 None（沒有主控台的服務，此時 print 本來就什麼都不做），也可能被
    換成不支援 reconfigure 的物件（IDE、測試替身）—— 兩種都原樣放過。
    """
    stream = sys.stdout
    if stream is None or not hasattr(stream, "reconfigure"):
        return
    try:
        stream.reconfigure(encoding="utf-8", errors="replace")
    except (OSError, ValueError):
        pass


def _nearest_existing(path):
    """往上找第一個已經存在的路徑。setup() 建目錄時就是在它底下建，所以檢查它能不能寫。"""
    while not os.path.exists(path):
        parent = os.path.dirname(path)
        if parent == path:
            break
        path = parent
    return path


def main():
    _make_stdout_safe()
    print("=== live 環境冒煙檢查 ===")

    print("\n[Python]")
    print("  版本       : %s" % sys.version.split()[0])
    print("  直譯器     : %s" % sys.executable)

    print("\n[套件] 依 requirements-live.txt 實際列出的項目")
    if os.path.isfile(REQUIREMENTS_LIVE):
        names = _requirement_names(REQUIREMENTS_LIVE)
        if not names:
            print("  (檔案目前沒有列任何套件)")
        for name in names:
            try:
                print("  %-12s: %s" % (name, version(name)))
            except PackageNotFoundError:
                print("  %-12s: 未安裝" % name)
    else:
        print("  找不到 %s" % REQUIREMENTS_LIVE)

    print("\n[路徑]")
    print("  repo 根目錄: %s" % REPO_ROOT)
    print("  runtime/   : %s" % RUNTIME_DIR)
    exists = os.path.isdir(RUNTIME_DIR)
    print("  目錄存在   : %s" % ("是" if exists else "否 (第一次寫入時才建立)"))
    probe = RUNTIME_DIR if exists else REPO_ROOT
    print("  可寫       : %s (檢查對象 %s)" % ("是" if os.access(probe, os.W_OK) else "否", probe))

    print("\n[設定] live/config.py 的執行參數（不敏感，直接印）")
    for name, value in config.execution_params().items():
        print("  %-28s: %s" % (name, value))

    print("\n[日誌] live.logsetup.setup() 會寫到這裡；冒煙檢查不呼叫 setup()，不產生日誌檔")
    print("  日誌檔     : %s" % config.LOG_FILE)
    print("  等級       : %s" % config.LOG_LEVEL)
    print("  輪替       : 單檔 %d bytes，另保留 %d 份舊檔" % (config.LOG_MAX_BYTES, config.LOG_BACKUP_COUNT))
    log_exists = os.path.isfile(config.LOG_FILE)
    print("  檔案存在   : %s" % ("是" if log_exists else "否 (setup() 時才建立目錄與檔案)"))
    # 只用 os.access 判斷，不實際建目錄或開檔 —— 冒煙檢查不可以留下任何東西
    log_probe = _nearest_existing(config.LOG_FILE)
    print("  可寫       : %s (檢查對象 %s)" % ("是" if os.access(log_probe, os.W_OK) else "否", log_probe))

    print("\n[密鑰] 只從環境變數讀；這裡只報告有沒有設，不印值")
    for env_name, purpose in config.SECRET_ENV_VARS.items():
        state = "已設定" if config.secret_is_set(env_name) else "未設定"
        print("  %-28s: %s  (%s)" % (env_name, state, purpose))
    print("  未設定不影響用不到它的元件；要用的元件會在取用當下拋出說明清楚的例外。")

    print("\n[git]")
    print("  commit     : %s" % _git_commit())


if __name__ == "__main__":
    main()
