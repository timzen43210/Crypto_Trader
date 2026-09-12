#!/usr/bin/env bash
# VPS / 自己的主機用：由 crontab 定時呼叫
set -e
cd "$(dirname "$0")"
./venv/bin/python pionex_dryrun.py
# 若此資料夾是 git repo，順便把結果推上 GitHub（可選）
if [ -d .git ]; then
  git add state output
  git diff --cached --quiet || git commit -qm "dry run $(date -u +%Y-%m-%dT%H:%MZ)"
  git push -q || echo "git push 失敗（未設定 GitHub 金鑰時可忽略）"
fi
