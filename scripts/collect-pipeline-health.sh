#!/usr/bin/env bash
# =============================================================================
# collect-pipeline-health.sh — 采集链路健康度，写入 ads_pipeline_health
# -----------------------------------------------------------------------------
# 用法：
#   bash scripts/collect-pipeline-health.sh              # 全量采集（约 30-40s）
#   bash scripts/collect-pipeline-health.sh --skip-kafka # 跳过 Kafka 抽样（约 5s）
#
# 为什么需要这个脚本，而不是一条 SQL 刷出来：
#   8 个指标分布在三个系统里 —— Doris（事件年龄 / 管道新鲜度）、
#   Flink（checkpoint / 作业状态）、Kafka（积压 / 重复率 / 乱序）。
#   没有任何一个系统能单独算全，所以监控必须是旁路采集，这也符合真实项目的做法：
#   数据质量指标是"观测者"，不应该挂在被观测的主链路上（主链路挂了，指标也停了，
#   那就永远发现不了挂）。
#
# =============================================================================
# 一个必须讲清楚的口径问题：事件年龄 ≠ 端到端延迟
# =============================================================================
# `event_age` = 入库时刻 - 事件业务时间，本脚本从 ods_user_event 的
#   (ts_ms/1000 - UNIX_TIMESTAMP(event_time)) 算。
# 实测 p50 是 65 小时 —— 因为埋点生成器刻意做了 1 小时回填，且数据是两天前的。
# 拿它当"延迟"会得出"链路延迟 65 小时"的荒谬结论（docs/03 §5 已警告）。
#
# 真正的端到端延迟必须掐表：源端写一行，轮询目标表直到可见。P3 实测单次
# 3,849 ms。脚本里用常量写入并标注来源，不在每次采集时重测 —— 重测要往业务
# 表插测试订单，会污染 GMV；要干净就得单独建测试表，为一个大屏数字不值得。
#
# 所以健康度面板上是这样分组的：
#   delay   组：e2e_latency（真实延迟，常量）+ pipeline_freshness（链路是否还活着）
#   不把 event_age 放进 delay 组，它属于 quality 组，反映数据本身有多旧。
# =============================================================================
set -euo pipefail

SKIP_KAFKA=0
[[ "${1:-}" == "--skip-kafka" ]] && SKIP_KAFKA=1

TOPIC="user_event"
GROUP_DWD="flink-dwd-user-event-clean"
GROUP_ODS="flink-ods-user-event"
GROUP_FUNNEL="flink-dws-traffic-funnel"

# ---------------------------------------------------------------------------
# 取数助手：全部走 docker exec，不依赖宿主机装 mysql 客户端
# ---------------------------------------------------------------------------
doris() {
  MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe \
    mysql -h127.0.0.1 -P9030 -uroot -D realtime_dw -N -B -e "$1" 2>/dev/null | head -1
}

flink() {
  MSYS_NO_PATHCONV=1 docker exec rt-flink-jm curl -s "http://localhost:8081$1" 2>/dev/null
}

num_or() {  # $1=值 $2=兜底
  if [[ -z "${1:-}" || "${1}" == "NULL" ]]; then echo "$2"; else echo "$1"; fi
}

# 按阈值算告警级别：level <warn> <critical> <值>
level_of() {
  awk -v w="$1" -v c="$2" -v v="$3" 'BEGIN{
    if (v >= c) print "critical"; else if (v >= w) print "warn"; else print "ok"
  }'
}
# 反向阈值（越小越好但超过才算坏，用于「越大越好」之外的场景暂不需要）

echo "==> 采集 Doris 侧指标"
EVENT_AGE_LINE=$(doris "
  SELECT CAST(PERCENTILE(x,0.5) AS BIGINT), CAST(PERCENTILE(x,0.99) AS BIGINT)
  FROM (SELECT ts_ms/1000 - UNIX_TIMESTAMP(event_time) AS x FROM ods_user_event) t;")
EVENT_AGE_P50=$(num_or "$(echo "$EVENT_AGE_LINE" | cut -f1)" 0)
EVENT_AGE_P99=$(num_or "$(echo "$EVENT_AGE_LINE" | cut -f2)" 0)
EVENT_AGE_P50=$(printf '%.0f' "$EVENT_AGE_P50")
EVENT_AGE_P99=$(printf '%.0f' "$EVENT_AGE_P99")

# 管道新鲜度取三张主表的 update_time 里最新的那个 —— 只要有一条链还在写，
# 就说明管道活着。只用一张表的话，某张表专门停掉（比如日粒度批表）会误报红。
FRESHNESS=$(doris "
  SELECT CAST(MIN(TIMESTAMPDIFF(SECOND, ut, NOW())) AS BIGINT) FROM (
    SELECT MAX(update_time) ut FROM dws_trade_overview_1m
    UNION ALL SELECT MAX(update_time) FROM dwd_user_event_clean
    UNION ALL SELECT MAX(update_time) FROM ads_realtime_overview
  ) t;")
FRESHNESS=$(num_or "$FRESHNESS" 999999)

# 取消金额监控（P6 新增）
# GMV 口径定为「含运费、不剔除状态」（docs/03 §1），所以取消单的钱仍然计入 GMV。
# 这在实时流里是必须的：取消是一次状态变更而不是行删除，用 WHERE 剔除会让已经
# 统计过的 GMV 往下降，破坏累计口径的单调性，且 Flink 侧要为此维护 retract 语义。
# 代价是 GMV 里混着无效交易，所以单独把「取消金额」作为质量指标放出来 ——
# 谁需要净 GMV，拿 GMV 减去它即可，这个减法口径是明确的。
CANCEL_LINE=$(doris "
  SELECT CAST(SUM(CASE WHEN order_status IN ('canceled','unavailable')
                       THEN item_total ELSE 0 END) AS DECIMAL(20,2)),
         CAST(SUM(item_total) AS DECIMAL(20,2))
  FROM dwd_trade_order_detail;")
CANCELED_GMV=$(num_or "$(echo "$CANCEL_LINE" | cut -f1)" 0)
TOTAL_GMV=$(num_or "$(echo "$CANCEL_LINE" | cut -f2)" 1)
CANCELED_RATIO=$(awk -v a="$CANCELED_GMV" -v b="$TOTAL_GMV" 'BEGIN{
  if (b+0 == 0) print "0"; else printf "%.4f", a*100.0/b }')
CANCELED_ORDERS=$(num_or "$(doris "SELECT COUNT(DISTINCT order_id) FROM dwd_trade_order_detail
                               WHERE order_status IN ('canceled','unavailable');")" 0)

echo "==> 采集 Flink 侧指标"
# checkpoint 耗时 P95：取运行中作业里最慢的那个 P95
# 用 Flink 自己算好的 summary.end_to_end_duration.p95，而不是从 history 里手算 ——
# /checkpoints 的 history 只保留最近若干条，手算样本量不稳定；summary 是全生命周期的，
# 对长期运行作业会略微偏保守（含早期冷启动的慢 checkpoint），这个偏差方向是安全的。
CKPT_LINE=$(flink "/jobs/overview" | python -c "
import sys, json, subprocess
try:
    d = json.load(sys.stdin)
except Exception:
    print('0\t0'); sys.exit(0)
p95s, running = [], 0
for j in d.get('jobs', []):
    if j['state'] != 'RUNNING':
        continue
    running += 1
    try:
        r = subprocess.run(['docker','exec','rt-flink-jm','curl','-s',
                            'http://localhost:8081/jobs/%s/checkpoints' % j['jid']],
                           capture_output=True, text=True, timeout=15)
        c = json.loads(r.stdout)
        p = c.get('summary', {}).get('end_to_end_duration', {}).get('p95', 0) or 0
        if p: p95s.append(p)
    except Exception:
        pass
print('%d\t%d' % (max(p95s) if p95s else 0, running))
" 2>/dev/null || echo "0	0")
CKPT_P95=$(num_or "$(echo "$CKPT_LINE" | cut -f1)" 0)
JOBS_RUNNING=$(num_or "$(echo "$CKPT_LINE" | cut -f2)" 0)

echo "==> 采集 Kafka 侧指标"
KAFKA_LAG=0
DUP_RATE=0
DUP_CNT=0
LATE_RATIO=0

if [[ "$SKIP_KAFKA" -eq 0 ]]; then
  # 三个消费者组的 lag 相加：DWD 清洗 / ODS 落湖 / 漏斗，各自独立消费同一 topic
  for g in "$GROUP_DWD" "$GROUP_ODS" "$GROUP_FUNNEL"; do
    L=$(MSYS_NO_PATHCONV=1 docker exec rt-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
          --bootstrap-server kafka:9092 --describe --group "$g" 2>/dev/null \
        | awk 'NR>1 && $6 ~ /^[0-9]+$/ { s += $6 } END { print s+0 }')
    KAFKA_LAG=$((KAFKA_LAG + $(num_or "$L" 0)))
  done

  # 重复率与乱序：**最近 N 条的滑动窗口，且跨全部分区均分**。
  #
  # 两个坑都是 P6 实测踩出来的：
  #   1) 全量口径会被历史无限稀释（第一批 2.91% 的注入，全量算只有 0.19%）。
  #   2) 只取一个分区的窗口同样会被稀释：注入的 3,000 条按 session_id 哈希散到
  #      3 个分区，单分区只增 1,000 条，而窗口取了 3,000 条 → 混进 2,000 条旧数据，
  #      3% 的重复率被摊薄成 1.27%。
  # 所以窗口要按分区均分，让总数 ≈ 一次注入的规模。
  #
  # 乱序统计**必须按分区独立维护最大事件时间**：三个分区是并行消费的，把它们
  # 合并成一条流来比较，会把「分区 A 的旧事件恰好排在分区 B 的新事件之后」
  # 误判成乱序。加 print.partition=true 拿到分区号后分桶维护。
  WINDOW="${HEALTH_WINDOW:-3000}"
  N_PART="${HEALTH_PARTITIONS:-3}"
  PER_PART=$(( WINDOW / N_PART ))
  [[ "$PER_PART" -lt 1 ]] && PER_PART=1

  OFFSETS=$(MSYS_NO_PATHCONV=1 docker exec rt-kafka /opt/kafka/bin/kafka-get-offsets.sh \
              --bootstrap-server kafka:9092 --topic "$TOPIC" --time -1 2>/dev/null)

  SAMPLE=""
  for p in $(seq 0 $((N_PART - 1))); do
    LATEST=$(echo "$OFFSETS" | awk -F: -v p="$p" '$2 == p { print $3 }')
    LATEST=$(num_or "$LATEST" 0)
    START=$(( LATEST > PER_PART ? LATEST - PER_PART : 0 ))
    PART_OUT=$(MSYS_NO_PATHCONV=1 docker exec rt-kafka /opt/kafka/bin/kafka-console-consumer.sh \
                 --bootstrap-server kafka:9092 --topic "$TOPIC" --partition "$p" \
                 --offset "$START" --max-messages "$PER_PART" --timeout-ms 15000 \
                 --property print.partition=true 2>/dev/null)
    SAMPLE="${SAMPLE}${PART_OUT}"$'\n'
  done

  # 注意 awk 用绝对路径 /usr/bin/awk：容器里没有 `gawk` 这个命令名，
  # 但 /usr/bin/awk 就是 gawk（mktime 可用），写成 gawk 会 command not found。
  STATS=$(printf '%s' "$SAMPLE" | MSYS_NO_PATHCONV=1 docker exec -i rt-kafka /usr/bin/awk '
    {
      # print.partition=true 的输出形如 "Partition:0\t{...}"，先把分区号取出来
      if (match($0, /^Partition:[0-9]+/)) {
        part = substr($0, 11, RLENGTH - 10)
        $0 = substr($0, RLENGTH + 1)
      } else part = "x"

      p = index($0, "\"event_id\": \"")
      q = index($0, "\"event_time\": \"")
      if (p == 0 || q == 0) next
      eid = substr($0, p + 13, 36)
      s   = substr($0, q + 15, 23)
      tot++
      if (eid in seen) dup++; else seen[eid] = 1

      # 事件时间转 epoch 毫秒，统计「比**本分区**已见最大值落后 > 60s」的比例
      d = substr(s,1,10); t = substr(s,12,8); ms = substr(s,21,3)
      split(d,dd,"-"); split(t,tt,":")
      ep = mktime(dd[1]" "dd[2]" "dd[3]" "tt[1]" "tt[2]" "tt[3]) * 1000 + ms
      if (ep > maxep[part]) maxep[part] = ep
      else if (maxep[part] - ep > 60000) late++
    }
    END {
      if (tot == 0) { print "0\t0\t0\t0" }
      else printf "%.4f\t%d\t%.4f\t%d\n", dup*100.0/tot, dup, late*100.0/tot, tot
    }')
  DUP_RATE=$(num_or "$(echo "$STATS" | cut -f1)" 0)
  DUP_CNT=$(num_or "$(echo "$STATS" | cut -f2)" 0)
  LATE_RATIO=$(num_or "$(echo "$STATS" | cut -f3)" 0)
  SAMPLED=$(num_or "$(echo "$STATS" | cut -f4)" 0)
  echo "    抽样 $SAMPLED 条（$N_PART 分区各 $PER_PART）：重复 $DUP_CNT 条（${DUP_RATE}%），乱序 ${LATE_RATIO}%"
else
  echo "    已跳过"
  SAMPLED=0
fi

# ---------------------------------------------------------------------------
# 阈值判定 + 写入
# ---------------------------------------------------------------------------
E2E_MS=3849          # P3 实测单次采样，见 docs/03 §5 实测基线
E2E_LEVEL=$(level_of 5000 15000 "$E2E_MS")

EVENT_AGE_LEVEL=$(level_of 86400 259200 "$EVENT_AGE_P99")   # 1天 warn / 3天 critical
FRESH_LEVEL=$(level_of 300 3600 "$FRESHNESS")
DUP_LEVEL=$(level_of 1 5 "$DUP_RATE")
LATE_LEVEL=$(level_of 5 20 "$LATE_RATIO")
LAG_LEVEL=$(level_of 1000 10000 "$KAFKA_LAG")
CKPT_LEVEL=$(level_of 5000 15000 "$CKPT_P95")
CANCEL_LEVEL=$(level_of 1 3 "$CANCELED_RATIO")

echo "==> 写入 ads_pipeline_health"
MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe mysql -h127.0.0.1 -P9030 -uroot -D realtime_dw -e "
INSERT INTO ads_pipeline_health
  (metric_key, metric_name, metric_value, metric_unit, metric_level, threshold_desc, metric_group, collect_time)
VALUES
  ('e2e_latency_ms',    '端到端延迟',      $E2E_MS,         'ms',  '$E2E_LEVEL',
   '单次实测 3849ms（P3，非 P99）；理论最差≈checkpoint 间隔 10s。<5000 ok / <15000 warn', 'delay',   NOW()),
  ('pipeline_freshness_s','管道新鲜度',    $FRESHNESS,      's',   '$FRESH_LEVEL',
   '最近一次成功写入距今。反映链路是否还活着。<300 ok / <3600 warn', 'delay',   NOW()),
  ('event_age_p99_s',   '事件年龄 P99',    $EVENT_AGE_P99,  's',   '$EVENT_AGE_LEVEL',
   '入库时刻-事件时间。**不是延迟**：生成器有 1h 回填，天然小时级。<1天 ok', 'quality', NOW()),
  ('dup_rate',          '事件重复率',      $DUP_RATE,       '%',   '$DUP_LEVEL',
   '最近 $WINDOW 条窗口内 1-COUNT(DISTINCT event_id)/COUNT(*)。**不能用全量口径**，会被历史稀释。<1% ok / <5% warn', 'quality', NOW()),
  ('late_ratio',        '事件乱序比例',    $LATE_RATIO,     '%',   '$LATE_LEVEL',
   '最近 $WINDOW 条窗口内落后最大事件时间 >60s 的比例。<5% ok / <20% warn', 'quality', NOW()),
  ('canceled_gmv',      '无效交易金额',    $CANCELED_GMV,   '元',  '$CANCEL_LEVEL',
   'canceled+unavailable 的 GMV 合计，涉及 $CANCELED_ORDERS 个订单。**仍计入 GMV 口径**，净 GMV = GMV − 本值', 'quality', NOW()),
  ('canceled_ratio',    '无效交易占比',    $CANCELED_RATIO, '%',   '$CANCEL_LEVEL',
   '无效交易金额 / 总 GMV。<1% ok / <3% warn', 'quality', NOW()),
  ('kafka_lag',         'Kafka 积压',      $KAFKA_LAG,      '条',  '$LAG_LEVEL',
   '三个消费者组 lag 之和。<1000 ok / <10000 warn', 'capacity', NOW()),
  ('checkpoint_p95_ms', 'Checkpoint P95',  $CKPT_P95,       'ms',  '$CKPT_LEVEL',
   '运行中作业最近 20 次已完成 checkpoint 的 P95。<5000 ok / <15000 warn', 'capacity', NOW()),
  ('flink_jobs_running','运行中作业数',    $JOBS_RUNNING,   '个',  'ok',
   '状态为 RUNNING 的 Flink 作业数。4 slots 集群期望 3-4 个', 'capacity', NOW());
" 2>&1 | head -5

echo
echo "==> 采集结果"
MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe mysql -h127.0.0.1 -P9030 -uroot -D realtime_dw -e "
SELECT metric_group AS grp, metric_key, metric_name, metric_value, metric_unit, metric_level
FROM ads_pipeline_health ORDER BY metric_group, metric_key;" 2>&1
