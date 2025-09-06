#!/usr/bin/env bash
set -euo pipefail

# === 路径配置 ===
BASE_NTG="/mnt/Fold/hehf/nature_task_generate"
BASE_SITE="/mnt/Fold/hehf/rss_site"
DB_SRC="${BASE_NTG}/rss_state.db"
DB_DEST_DIR="${BASE_SITE}/data"
DB_DEST="${DB_DEST_DIR}/rss_state.db"

# === 日志与并发锁 ===
LOG_DIR="/mnt/Fold/hehf/nature_task_generate/cron_logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/daily_$(date +%F).log"
exec >>"$LOG_FILE" 2>&1
LOCK_FILE="/mnt/Fold/hehf/nature_task_generate/.rss_refresh.lock"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -Is)] another run is in progress, exiting."
  exit 0
fi

echo "===================="
echo "[$(date -Is)] START"

# === 环境 ===
# 保底 PATH（有些 cron 没有完整 PATH）
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

# 固定 Python 解释器（Anaconda env）
PYBIN="/home/hehf/anaconda3/envs/nature_task/bin/python"
if [[ ! -x "$PYBIN" ]]; then
  echo "[$(date -Is)] Python not found or not executable: $PYBIN" >&2
  exit 1
fi
echo "[$(date -Is)] using python: $PYBIN"

# === 1) 生成/更新 DB ===
echo "[$(date -Is)] run python rss_fetch_store.py"
cd "$BASE_NTG"
"$PYBIN" rss_fetch_store.py

# === 2) 复制 DB 到站点 ===
echo "[$(date -Is)] copy db to site"
mkdir -p "$DB_DEST_DIR"
cp -f "$DB_SRC" "$DB_DEST"

# === 3) Git 提交与推送（仅当有改动时, 走 SSH） ===
echo "[$(date -Is)] git commit & push"
cd "$BASE_SITE"
git add -A

if git diff --cached --quiet; then
  echo "[$(date -Is)] no changes to commit, skip push."
else
  git commit -m "auto: refresh db $(date -Iseconds)" || true

  # 确保远程是 SSH URL
  git remote set-url origin "git@github.com:hehuifeng/rss_site.git"

  # 推送函数：SSH + 重试（指数回退）
  push_with_retries() {
    local tries=0 max=6 sleep_s=5
    local rc=1
    while (( tries < max )); do
      ((tries++))
      echo "[$(date -Is)] git push attempt ${tries}/${max} (SSH)"
      if git push origin master; then
        rc=0; break
      fi
      if (( tries < max )); then
        echo "[$(date -Is)] push failed, retry in ${sleep_s}s..."
        sleep "$sleep_s"
        sleep_s=$(( sleep_s * 2 ))
      fi
    done
    return $rc
  }

  if ! push_with_retries; then
    echo "[$(date -Is)] push failed after retries (SSH). Remote & config snapshot:"
    git remote -v
    git config -l | tail -n 50
    exit 1
  fi
fi

echo "[$(date -Is)] DONE"