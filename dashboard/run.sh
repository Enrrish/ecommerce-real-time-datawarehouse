#!/usr/bin/env bash
# =============================================================================
# dashboard/run.sh —— 一条命令把大屏跑起来
# -----------------------------------------------------------------------------
# 用法：
#   bash dashboard/run.sh                 # http://127.0.0.1:8080/（1.5 秒轮询）
#   bash dashboard/run.sh --with-events   # 同时起持续埋点，漏斗会一直跳（演示用）
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
FEED=0
EXTRA=()

while [ $# -gt 0 ]; do
  case "$1" in
    --port)     PORT="$2"; shift 2 ;;
    --snapshot) MODE=snapshot; shift ;;
    --with-events) FEED=1; shift ;;
    --no-refresh) EXTRA+=(--no-refresh); shift ;;
    # 注意用 basename：上面已经 cd 到 dashboard/，$0 还是 "dashboard/run.sh" 就读不到了
    -h|--help)  sed -n '2,13p' "$(basename "$0")" | sed 's/^# \{0,1\}//'; exit 0 ;;
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
echo "   交易日 取最完整的一天（当前数据为 2017-11-24 黑五）"
echo "   刷新   页面 1.5s · 今日 1s · 漏斗 2s · 健康度 15s · 全量 60s"
echo "  Ctrl+C 停止"
echo "=================================================================="

# 持续埋点：不启动的话漏斗面板是静止的（历史事件的累计值）。
# 大屏只显示「今天」的漏斗，没人产生事件，它当然不动。
if [ "$FEED" = "1" ]; then
  echo " 埋点   持续模式：每 2 秒新开 3 个会话（Ctrl+C 一起停）"
  python ../scripts/gen_user_events.py --live 2 --sessions 3 &
  FEED_PID=$!
  trap 'kill "$FEED_PID" 2>/dev/null || true' EXIT INT TERM
  PYTHONPATH=server python server/app.py --port "$PORT" "${EXTRA[@]}"
else
  echo " 提示   想看漏斗动起来，加 --with-events，或另开终端跑"
  echo "        python scripts/gen_user_events.py --live 2 --sessions 3"
  PYTHONPATH=server exec python server/app.py --port "$PORT" "${EXTRA[@]}"
fi
