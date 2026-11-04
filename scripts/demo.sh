#!/usr/bin/env bash
# =============================================================================
# demo.sh —— 一条命令从零把整条链路跑起来
# -----------------------------------------------------------------------------
# 用法（仓库根目录，Git Bash）：
#   bash scripts/demo.sh                  # 起集群 → 建表 → 补历史 → 提作业 → 开大屏
#   bash scripts/demo.sh --no-dashboard   # 只把后端跑起来，不开大屏
#   bash scripts/demo.sh --with-events    # 同时起持续埋点（漏斗会一直动）
#   bash scripts/demo.sh --import         # 业务库是空的，顺手调 import.sh 导数据
#   bash scripts/demo.sh --skip-backfill  # 跳过 DWD/DWS 日粒度回填（已有数据时自动跳）
#
# 它做的六件事，顺序不能换：
#   1. 起六个容器（source / compute / olap 三组 profile 分批）—— BE 与 TM 同时拉起会撞 OOM
#   2. 建 Kafka topic（3 分区）—— 不建也能跑，但 Kafka 会自动建成 1 分区，
#      漏斗的会话归因和 dup/late 抽样的分区假设全部失效，而且**不报错**
#   3. 确认业务库有数据（155 万行 Olist）
#   4. 建 Doris 库表（01 ODS → 03 DWD → 05 DWS → 07 ADS，全部 IF NOT EXISTS）
#   5. 提 Flink 作业：先补两个「一次性」作业（宽表 / 日粒度），再提 4 个常驻作业
#   6. 起大屏
#
# 幂等：容器在跑就跳过、topic 存在就跳过、表有数据就跳过、作业在跑就跳过。
#       重复执行只会把缺的部分补上，不会重复写入。
#
# 为什么 5 要分两批：
#   集群只有 4 个 slot，常驻作业正好占满。`05-dwd-trade-order-detail`（订单宽表）
#   和 `08-dws-trade-daily`（日粒度汇总）的数据是「跑一次就够」的历史快照，
#   提上去等快照写完就取消，把 slot 还给常驻作业。
#   这也是 docs/05 §4 里「已停但数据完整的作业」那一节说的事。
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

NO_DASH=0
WITH_EVENTS=0
DO_IMPORT=0
SKIP_BACKFILL=0
for arg in "$@"; do
  case "$arg" in
    --no-dashboard)  NO_DASH=1 ;;
    --with-events)   WITH_EVENTS=1 ;;
    --import)        DO_IMPORT=1 ;;
    --skip-backfill) SKIP_BACKFILL=1 ;;
    -h|--help)       sed -n '2,30p' "$(basename "$0")" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数：$arg（--help 看用法）" >&2; exit 2 ;;
  esac
done

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die()  { printf '\033[31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------
doris_sql() {  # $1 = SQL
  # 必须带 -D：Doris 的 batch 模式没有默认库，不带就报 "No database selected"，
  # 而且这条错误在 -B 模式下只写到 stderr，被 2>/dev/null 吞掉后看起来像「查询返回空」。
  #
  # 用 sed -n 1p 而不是 head -1：head 读到第一行就退出，上游 mysql 会被 SIGPIPE 杀掉
  # （退出码 141），而脚本是 set -o pipefail —— 一个正常的查询会把整个脚本带崩。
  MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe \
    mysql -h127.0.0.1 -P9030 -uroot -D realtime_dw -N -B -e "$1" 2>/dev/null | sed -n '1p'
}

doris_file() {  # $1 = sql 文件路径
  MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe \
    mysql -h127.0.0.1 -P9030 -uroot --default-character-set=utf8mb4 < "$1"
}

mysql_sql() {  # $1 = SQL
  local pw="root123"
  if [[ -f docker/.env ]]; then
    # shellcheck disable=SC1091
    pw="$(
      set -a; . docker/.env; set +a
      printf '%s' "${MYSQL_ROOT_PASSWORD:-root123}"
    )"
  fi
  MSYS_NO_PATHCONV=1 docker exec -i rt-mysql mysql -uroot -p"$pw" -N -B -e "$1" 2>/dev/null | sed -n '1p'
}

flink_json() {  # $1 = REST 路径
  MSYS_NO_PATHCONV=1 docker exec rt-flink-jm curl -s "http://localhost:8081$1" 2>/dev/null
}

running_jobs() {  # 每行一个「jid<TAB>name」，只列 RUNNING
  flink_json "/jobs/overview" | python -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for j in d.get('jobs', []):
    if j.get('state') == 'RUNNING':
        print('%s\t%s' % (j['jid'], j['name']))
" 2>/dev/null || true
}

# 作业名形如 insert-into_default_catalog.default_database.sink_dws_trade_overview_1m，
# 拿 sink 表名当「这个作业是否已经在跑」的判据 —— 比数作业个数可靠：
# 数个数只能发现「满了」，发现不了「缺了哪个」。
job_jid() {
  # 不要写成 awk '... {print $1; exit}'：exit 会让上游 python 吃到 SIGPIPE，
  # pipefail 下这条 pipeline 返回 141，赋值语句就把脚本带崩了。用标志位代替 exit。
  running_jobs | awk -F'\t' -v pat="$1" '$2 ~ pat && !done {print $1; done=1}'
}

job_running() {
  [[ -n "$(job_jid "$1")" ]]
}

# ---------------------------------------------------------------------------
step "[1/6] 启动容器"
# ---------------------------------------------------------------------------
docker info >/dev/null 2>&1 || die "Docker 引擎没在运行，先打开 Docker Desktop"

(cd docker && docker compose --profile source --profile compute --profile olap up -d)

with_healthcheck() {  # 该容器有没有定义 healthcheck
  [[ -n "$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$1" 2>/dev/null)" ]]
}

wait_up() {  # $1 = 容器名，$2 = 超时秒（默认 180）
  local c="$1" limit="${2:-180}" i=0
  while (( i < limit )); do
    local running
    running="$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null || echo false)"
    if [[ "$running" == "true" ]]; then
      if ! with_healthcheck "$c"; then echo "    $c 已启动（无健康检查）"; return 0; fi
      if [[ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" == "healthy" ]]; then
        echo "    $c healthy（${i}s）"; return 0
      fi
    fi
    sleep 2; i=$((i + 2))
  done
  die "$c 在 ${limit}s 内没有就绪，看日志：docker logs --tail 40 $c"
}

# 顺序：先 source，再 compute，最后 olap —— BE 初始化会预占大块内存，
# 和 TM 同时拉起容易触发 WSL 的 OOM Killer（被杀的不一定是吃内存的那个）。
for c in rt-mysql rt-kafka; do wait_up "$c" 180; done
for c in rt-flink-jm rt-flink-tm; do wait_up "$c" 120; done
for c in rt-doris-fe rt-doris-be; do wait_up "$c" 240; done

# ---------------------------------------------------------------------------
step "[2/6] 初始化 Kafka topic"
# ---------------------------------------------------------------------------
TOPIC=user_event
PARTS=3
if MSYS_NO_PATHCONV=1 docker exec rt-kafka /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server kafka:9092 --list 2>/dev/null | grep -qx "$TOPIC"; then
  # 同样避开 head -1：describe 输出多行，head 提前退出会让上游 grep 吃到 SIGPIPE，
  # 在 pipefail 下变成「查一下分区数就把脚本查崩了」。
  desc="$(MSYS_NO_PATHCONV=1 docker exec rt-kafka /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka:9092 --describe --topic "$TOPIC" 2>/dev/null)"
  n="$(printf '%s\n' "$desc" | sed -n '1s/.*PartitionCount: *\([0-9]*\).*/\1/p')"
  echo "    topic $TOPIC 已存在（${n:-?} 分区）"
  [[ "${n:-0}" == "$PARTS" ]] || echo "    ⚠ 分区数不是 $PARTS，dup/late 抽样与漏斗归因的口径会偏"
else
  MSYS_NO_PATHCONV=1 docker exec rt-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --create --topic "$TOPIC" \
    --partitions "$PARTS" --replication-factor 1 >/dev/null
  echo "    已创建 topic $TOPIC（$PARTS 分区）"
fi

# ---------------------------------------------------------------------------
step "[3/6] 确认业务库数据"
# ---------------------------------------------------------------------------
orders="$(mysql_sql 'SELECT COUNT(*) FROM olist.orders' || echo 0)"
orders="${orders:-0}"
if (( orders == 0 )); then
  if (( DO_IMPORT == 1 )); then
    echo "    olist.orders 是空的，调用 import.sh 导入（约 1~3 分钟）"
    bash scripts/import.sh
  else
    die "olist.orders 是空的。先导数据：bash scripts/import.sh（或加 --import 让本脚本代跑）"
  fi
else
  echo "    olist.orders $orders 行"
fi

# ---------------------------------------------------------------------------
step "[4/6] 建 Doris 库表"
# ---------------------------------------------------------------------------
# 顺序固定：01 建库 + ODS，03/05/07 依次建 DWD / DWS / ADS。
# 07 依赖 05 的表（ADS 刷新要读 DWS），跑反了不会报错，只会让刷新扫空表。
for f in 01-ods 03-dwd 05-dws 07-ads; do
  echo "    sql/doris/$f.sql"
  doris_file "sql/doris/$f.sql" || die "$f.sql 执行失败"
done
tables="$(doris_sql "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='realtime_dw'")"
echo "    realtime_dw 共 ${tables:-?} 张表"

# ---------------------------------------------------------------------------
step "[5/6] 提交 Flink 作业"
# ---------------------------------------------------------------------------
submit_job() {  # $1 = 作业文件名（不含 .sql），$2 = 目标表，$3 = 说明
  local file="$1" sink="$2" desc="$3"
  if job_running "$sink"; then
    echo "    ${file} 已在运行（$desc），跳过"
    return 0
  fi
  bash scripts/submit-sql.sh "flink/sql/${file}.sql" >/tmp/rt-submit.log 2>&1 \
    || { tail -20 /tmp/rt-submit.log; die "${file}.sql 提交失败"; }
  # 提交成功 ≠ 跑起来了：等它出现在 RUNNING 列表里
  for _ in $(seq 1 30); do
    if job_running "$sink"; then echo "    ${file} RUNNING（$desc）"; return 0; fi
    sleep 2
  done
  die "${file} 提交后 60s 仍未 RUNNING，看：docker logs --tail 50 rt-flink-tm"
}

wait_rows() {  # $1 = 表，$2 = 目标行数，$3 = 超时秒
  local table="$1" target="$2" limit="${3:-420}" i=0
  while (( i < limit )); do
    local n
    n="$(doris_sql "SELECT COUNT(*) FROM $table")"
    if (( ${n:-0} >= target )); then
      echo "    $table 已写入 $n 行（${i}s）"; return 0
    fi
    sleep 5; i=$((i + 5))
  done
  die "$table 在 ${limit}s 内只有 ${n:-0} 行（期望 ≥ $target），快照没跑完"
}

cancel_sink() {  # $1 = sink 表名
  local jid
  jid="$(job_jid "$1")"
  [[ -z "$jid" ]] && return 0
  MSYS_NO_PATHCONV=1 docker exec rt-flink-jm /opt/flink/bin/flink cancel "$jid" >/dev/null 2>&1 || true
  echo "    已取消 $1（jid $jid），slot 还给常驻作业"
}

if (( SKIP_BACKFILL == 0 )); then
  # ---- 5a. 订单宽表：只在 ads_order_stream 用到，跑一次就够 ----
  rows="$(doris_sql 'SELECT COUNT(*) FROM dwd_trade_order_detail')"
  if (( ${rows:-0} >= 110000 )); then
    echo "    宽表已有 ${rows} 行，跳过回填"
  else
    echo "    回填订单宽表（MySQL 快照，预计 1~3 分钟）"
    submit_job "05-dwd-trade-order-detail" "sink_dwd_trade_order_detail" "订单宽表"
    wait_rows "dwd_trade_order_detail" 110000 420
    cancel_sink "sink_dwd_trade_order_detail"
  fi

  # ---- 5b. 日粒度汇总：品类 / 卖家 / 地域三个面板的数据源 ----
  rows="$(doris_sql 'SELECT COUNT(*) FROM dws_trade_category_1d')"
  if (( ${rows:-0} >= 18000 )); then
    echo "    日粒度已有 ${rows} 行，跳过回填"
  else
    echo "    回填日粒度汇总（预计 1~2 分钟）"
    submit_job "08-dws-trade-daily" "sink_dws_trade_category_1d" "日粒度汇总"
    wait_rows "dws_trade_category_1d" 18000 420
    cancel_sink "sink_dws_trade_category_1d"
  fi
else
  echo "    --skip-backfill：跳过两个一次性作业"
fi

# ---- 5c. 四个常驻作业，正好占满 4 个 slot ----
submit_job "04-ods-user-event"        "sink_ods_user_event"        "埋点落湖"
submit_job "06-dwd-user-event-clean"  "sink_dwd_user_event_clean"  "埋点清洗"
submit_job "07-dws-trade-overview-1m" "sink_dws_trade_overview_1m" "分钟概览"
submit_job "09-dws-event-payment"     "sink_dws_traffic_funnel_1h" "漏斗 + 支付"

# 作业从 CREATED 到 RUNNING 有几百毫秒的窗口，刚提完就数会少数一个 —— 等一下再报，
# 并且报的是「哪些 sink 在跑」而不是个数：个数只能说明「满了」，说明不了「对」。
sleep 3
echo "    当前 RUNNING：$(running_jobs | awk -F'\t' '{n = split($2, a, "."); printf "%s ", a[n]}')"

# 健康度面板是旁路采集的，不挂在主链路上 —— 手动跑一次让面板有数
if [[ -f scripts/collect-pipeline-health.sh ]]; then
  echo "    采集一次链路健康度（约 30s）"
  bash scripts/collect-pipeline-health.sh --skip-kafka >/dev/null 2>&1 || true
fi

# ---------------------------------------------------------------------------
step "[6/6] 大屏"
# ---------------------------------------------------------------------------
if (( NO_DASH == 1 )); then
  cat <<'EOF'
    已跳过。手动启动：
      bash dashboard/run.sh                 # http://127.0.0.1:8080/
      bash dashboard/run.sh --with-events   # 同时起持续埋点
EOF
  exit 0
fi

args=()
if (( WITH_EVENTS == 1 )); then args+=(--with-events); fi
exec bash dashboard/run.sh "${args[@]}"
