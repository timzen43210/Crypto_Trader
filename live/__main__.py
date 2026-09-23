# -*- coding: utf-8 -*-
"""
python -m live — 環境與路徑冒煙檢查
===================================
搬到一台新主機上之後第一個該跑的指令，用來回答三個問題：環境裝對了嗎、路徑對嗎、
設定與密鑰齊不齊。

印出 Python 版本、requirements-live.txt 實際列的那些套件裝了沒（版本多少）、
runtime/ 解析到哪裡與能不能寫、live.config 的執行參數實際值、每個密鑰有沒有設，
以及這份 code 是哪個 commit，然後 exit 0。

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


def main():
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

    print("\n[密鑰] 只從環境變數讀；這裡只報告有沒有設，不印值")
    for env_name, purpose in config.SECRET_ENV_VARS.items():
        state = "已設定" if config.secret_is_set(env_name) else "未設定"
        print("  %-28s: %s  (%s)" % (env_name, state, purpose))
    print("  未設定不影響用不到它的元件；要用的元件會在取用當下拋出說明清楚的例外。")

    print("\n[git]")
    print("  commit     : %s" % _git_commit())


if __name__ == "__main__":
    main()
