#!/usr/bin/env bash
# =============================================================================
# submit-sql.sh — 把一份 Flink SQL 文件提交到 session 集群
# -----------------------------------------------------------------------------
# 为什么用 docker cp 而不是挂载：
#   Git Bash（MSYS）会把命令里所有以 / 开头的参数当成 Windows 路径改写，
#   于是 `sql-client.sh -f /tmp/job.sql` 到容器里变成了
#   `-f C:/Users/C/AppData/Local/Temp/job.sql`，报
#       Fail to read content from /opt/flink/C:/Users/C/.../job.sql
#   加 MSYS_NO_PATHCONV=1 可以解决，但那要求每个调用点都记得加。
#   docker cp 走的是 tar 流而不是路径参数，从根上绕开改写问题。
#
# 用法：
#   bash scripts/submit-sql.sh flink/sql/01-ods-orders.sql
#   bash scripts/submit-sql.sh flink/sql/01-ods-orders.sql --dry-run   # 只看不提交
#
# 注意：作业文件里必须自带 `SET 'execution.attached' = 'false';`
#   否则 sql-client 会挂在交互式前台不退出，脚本永远等不到返回。
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

SQL_FILE="${1:-}"
DRY=0
[[ "${2:-}" == "--dry-run" ]] && DRY=1

if [[ -z "$SQL_FILE" ]]; then
  echo "用法: bash scripts/submit-sql.sh <sql 文件路径> [--dry-run]" >&2
  exit 2
fi
# 允许相对仓库根或当前目录两种写法
[[ -f "$SQL_FILE" ]] || SQL_FILE="$ROOT/$SQL_FILE"
if [[ ! -f "$SQL_FILE" ]]; then
  echo "找不到 SQL 文件：$1" >&2
  exit 2
fi

JM="${FLINK_JM_CONTAINER:-rt-flink-jm}"
NAME="$(basename "$SQL_FILE")"

echo "=== 提交 $NAME → $JM ==="
docker cp "$SQL_FILE" "$JM:/tmp/job.sql" >/dev/null

if [[ $DRY -eq 1 ]]; then
  echo "[dry-run] 已放入 $JM:/tmp/job.sql，未执行。"
  exit 0
fi

# sql-client 的日志里混着大量 INFO，这里只保留它自己和引擎的报错
MSYS_NO_PATHCONV=1 docker exec "$JM" /opt/flink/bin/sql-client.sh -f /tmp/job.sql 2>&1 \
  | grep -vE '^SLF4J:|^log4j:' || true

echo
echo "=== 作业 ==="
MSYS_NO_PATHCONV=1 docker exec "$JM" /opt/flink/bin/flink list 2>/dev/null | sed -n '/Running Jobs/,$p' || true
