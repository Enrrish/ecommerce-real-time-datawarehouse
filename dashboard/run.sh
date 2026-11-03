#!/usr/bin/env bash
# =============================================================================
# dashboard/run.sh —— 一条命令把大屏跑起来
# -----------------------------------------------------------------------------
# 用法：
#   bash dashboard/run.sh                 # http://127.0.0.1:8080/（1.5 秒轮询）
#   bash dashboard/run.sh --port 9000
#   bash dashboard/run.sh --snapshot      # 只导出单文件快照，不起服务
#   bash dashboard/run.sh --no-refresh    # 只查询不刷新 ADS（排查用）
#
# 前置：6 个容器在跑（MySQL / Kafka / Flink ×2 / Doris ×2）。缺了会直接报错退出。
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")"

PORT=8080
MODE=serve
EXTRA=()

while [ $# -gt 0 ]; do
  case "$1" in
    --port)     PORT="$2"; shift 2 ;;
    --snapshot) MODE=snapshot; shift ;;
    --no-refresh) EXTRA+=(--no-refresh); shift ;;
    -h|--help)  sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)          EXTRA+=("$1"); shift ;;
  esac
done

# --- 环境自检：容器没起就别让页面白屏 -------------------------------------
missing=""
for c in rt-mysql rt-kafka rt-flink-jm rt-flink-tm rt-doris-fe rt-doris-be; do
  docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -q true || missing="$missing $c"
done
if [ -n "$missing" ]; then
  echo "✗ 这些容器没在运行：$missing" >&2
  echo "  先起集群：docker compose -f docker/docker-compose.yml --profile source --profile compute --profile olap up -d" >&2
  exit 1
fi

if [ "$MODE" = "snapshot" ]; then
  PYTHONPATH=server python server/snapshot.py
  exit 0
fi

echo "=================================================================="
echo " Olist 实时经营指挥台"
echo "   页面   http://127.0.0.1:${PORT}/"
echo "   数据日 取最完整的一天（当前数据为 2017-11-24 黑五）"
echo "   刷新   页面 1.5s · 今日 1s · 漏斗 2s · 健康度 15s · 全量 60s"
echo "  Ctrl+C 停止"
echo "=================================================================="

PYTHONPATH=server exec python server/app.py --port "$PORT" "${EXTRA[@]}"
