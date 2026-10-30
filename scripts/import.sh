#!/usr/bin/env bash
# =============================================================================
# import.sh —— P2 一键导入：起 MySQL → 预处理 CSV → LOAD DATA → 跑验收核对
# -----------------------------------------------------------------------------
# 用法（在仓库根目录，Git Bash 里）：
#   bash scripts/import.sh
#
# 前提：Docker Desktop 已启动。本脚本不会代替你启动它——
#      引擎没起来时 docker 命令会直接报错，脚本会给提示。
#
# 幂等性：脚本会在导入前检查 orders 表是否已有数据。
#       已有数据会直接退出，不会重复写入（主键冲突会刷一屏报错）。
#       要重来：docker compose --project-directory docker --profile source down -v
# =============================================================================

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CONTAINER="rt-mysql"

# --- 0. 环境检查 -------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  cat >&2 <<'EOF'
找不到 docker 命令。两种可能：
  1) 你用的是安装 Docker 之前就开着的终端 —— 环境变量是旧快照，新开一个窗口即可
  2) PATH 里确实没有，手动补上（Git Bash）：
     export PATH="$LOCALAPPDATA/Programs/DockerDesktop/resources/bin:$PATH"
EOF
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  cat >&2 <<'EOF'
Docker 引擎没有在运行。
请从开始菜单打开 Docker Desktop，等托盘图标变成 running 再重跑本脚本。
EOF
  exit 1
fi

# --- 1. 读取配置 -------------------------------------------------------------
PW="root123"
if [ -f docker/.env ]; then
  # shellcheck disable=SC1091
  set -a; . docker/.env; set +a
  PW="${MYSQL_ROOT_PASSWORD:-root123}"
fi

# --- 2. 起容器 ---------------------------------------------------------------
echo "==> [1/4] 启动 mysql（profile: source）"
(cd docker && docker compose --profile source up -d mysql)

echo "==> [2/4] 等待健康检查通过（首次启动要初始化数据目录，约 30~60 秒）"
for i in $(seq 1 60); do
  status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$CONTAINER" 2>/dev/null || echo missing)"
  if [ "$status" = "healthy" ]; then
    echo "    healthy（约 $((i * 3)) 秒）"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "    超时。看日志：docker logs $CONTAINER" >&2
    exit 1
  fi
  sleep 3
done

# --- 3. 检查是否已导入过 -----------------------------------------------------
existing="$(docker exec "$CONTAINER" mysql -uroot -p"$PW" -N -s \
  -e "SELECT COUNT(*) FROM olist.orders" 2>/dev/null || echo 0)"
if [ "${existing:-0}" -gt 0 ]; then
  cat >&2 <<EOF
检测到 orders 表已有 $existing 行，跳过导入以免主键冲突。

要重新导入，先清库（会删掉数据卷）：
  docker compose --project-directory docker --profile source down -v
  bash scripts/import.sh

只想起容器并表结构、不想重导数据，直接跑验收：
  docker exec -i $CONTAINER mysql -uroot -p --table < sql/mysql/02-verify.sql
EOF
  exit 0
fi

# --- 4. 预处理 + 导入 --------------------------------------------------------
echo "==> [3/4] 预处理 CSV → TSV（build/load/）"
python scripts/prepare_load.py

echo "==> [4/4] LOAD DATA 导入（127 万行，预计 1~3 分钟）"
docker exec -i "$CONTAINER" mysql -uroot -p"$PW" --default-character-set=utf8mb4 \
  < sql/mysql/03-load.sql

echo
echo "==> 验收核对（02-verify.sql）"
docker exec -i "$CONTAINER" mysql -uroot -p"$PW" --table --default-character-set=utf8mb4 \
  < sql/mysql/02-verify.sql

cat <<'EOF'

判读方法：上面每个结果集的 diff / should_be_0 列必须全是 0。
任何一处不为 0，都是导入阶段的问题，别急着往下做 CDC —— 先贴出来对账。
EOF
