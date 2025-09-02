#!/usr/bin/env bash
set -euo pipefail

# === 路径配置 ===
BASE_NTG="/mnt/Fold/hehf/nature_task_generate"
BASE_SITE="/mnt/Fold/hehf/rss_site"
DB_SRC="${BASE_NTG}/rss_state.db"
DB_DEST_DIR="${BASE_SITE}/data"
DB_DEST="${DB_DEST_DIR}/rss_state.db"

# === 日志与并发锁 ===
LOG_DIR="/mnt/Fold/hehf/cron_logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/daily_$(date +%F).log"
exec >>"$LOG_FILE" 2>&1
LOCK_FILE="/mnt/Fold/hehf/.rss_refresh.lock"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -Is)] another run is in progress, exiting."
  exit 0
fi

echo "===================="
echo "[$(date -Is)] START"

# === 环境：优先使用本地 venv，如果存在 ===
if [[ -f "${BASE_NTG}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${BASE_NTG}/.venv/bin/activate"
  echo "[$(date -Is)] using venv at ${BASE_NTG}/.venv"
fi

# 保底 PATH（有些 cron 没有完整 PATH）
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

# === 1) 生成/更新 DB ===
echo "[$(date -Is)] run python rss_fetch_store.py"
cd "$BASE_NTG"
# 允许 python 或 python3
PYBIN="$(command -v python || true)"
[[ -z "${PYBIN}" ]] && PYBIN="$(command -v python3 || true)"
if [[ -z "${PYBIN}" ]]; then
  echo "No python found in PATH" >&2
  exit 1
fi
"$PYBIN" rss_fetch_store.py

# === 2) 复制 DB 到站点 ===
echo "[$(date -Is)] copy db to site"
mkdir -p "$DB_DEST_DIR"
cp -f "$DB_SRC" "$DB_DEST"

# === 3) Git 提交与推送（仅当有改动时） ===
echo "[$(date -Is)] git commit & push"
cd "$BASE_SITE"
git add -A

if git diff --cached --quiet; then
  echo "[$(date -Is)] no changes to commit, skip push."
else
  git commit -m "auto: refresh db $(date -Iseconds)" || true

  # 推送函数：带重试与 HTTP/1.1 降级
  push_with_retries() {
    local tries=0 max=3
    local rc=1
    while (( tries < max )); do
      ((tries++))
      echo "[$(date -Is)] git push attempt ${tries}/${max}"
      if git push origin master; then
        rc=0; break
      fi

      # 第一次失败后，强制降级到 HTTP/1.1 再试
      if (( tries == 1 )); then
        echo "[$(date -Is)] forcing http.version=HTTP/1.1 and retry"
        git config http.version HTTP/1.1
      fi

      # 第二次仍失败，打印一条提示并 sleep 10s 再试
      if (( tries < max )); then
        echo "[$(date -Is)] push failed, will retry in 10s..."
        sleep 10
      fi
    done
    return $rc
  }

  if ! push_with_retries; then
    echo "[$(date -Is)] push failed after retries. Last 20 lines of git config:"
    git config -l | tail -n 20
    # 你也可以选择在这里 fallback 到 SSH（需要你预先配置好 ssh key）：
    # git remote set-url origin git@github.com:hehuifeng/rss_site.git
    # git push origin master || true
    exit 1
  fi
fi

echo "[$(date -Is)] DONE"
