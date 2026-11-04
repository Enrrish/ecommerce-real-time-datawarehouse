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
#   (ts_ms/1000 - UNIX_TIMESTAMP(event_time)) 算，**只看最近 10 分钟写入的行**。
#
# 为什么必须加时间窗：这张表是累计的，全量口径被历史数据钉死过两次 ——
# P7 重放整条 topic 时，几万条「几天前生成的事件」在当下被重写一遍，
# 全量 P99 直接报到 4.7 天并长期常亮 critical，而链路其实完全正常。
# 全量口径度量的是「这张表里最老的数据有多老」，不是「现在的数据有多旧」。
#
# 窗口取 10 分钟而不是 1 小时：重放本身也在最近一小时内，1 小时的窗口同样会
# 把它算进去（实测还是 4.7 天）。10 分钟能过滤掉「一次性的历史回放」，
# 又足够容纳正常速率下的样本量（持续埋点每 2 秒 3 个会话）。
#
# 它也不是延迟：生成器本身就有 1h 回填（docs/03 §5 已警告），所以这个值正常
# 就在 0~3600 之间摆动，代表的是「生产者把多久以前的事补进来了」。
#
# 真正的端到端延迟必须掐表：源端写一行，轮询目标表直到可见。P3 手工测过一次
# 3,849 ms，那个数字曾经被写死在这个脚本里当常量用 —— 这不是测量，是引用测量：
# 优化了它不动，劣化了它也不动。
#
# 现在改由 `scripts/check_lag.py` 真采 200 条探针算 P99，结果落在
# `scripts/.latency.json`，本脚本读它；读不到才退回 3,849 那个历史基线，
# 并且**在阈值说明里明确写出「这是历史基线不是 P99」** —— 常量伪装成测量值
# 是这类面板最容易骗人的地方。
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
# 事件年龄：**只看最近 1 小时写入的行**（ts_ms 是入库时刻）。
#
# 为什么必须加时间窗：这张表是累计的，全量 P99 会被历史数据永远钉住。
# 实测两次都是同一个坑 —— 早期批量灌的 2 万条事件是几天前生成的，回放时
# 入库时刻与事件时间差了 3.7 天、后来变成 4.7 天，于是 P99 一直报 critical，
# 而实际上链路当下完全正常。全量口径度量的是「这张表里最老的数据有多老」，
# 不是「现在的数据有多旧」。
#
# 窗口取 1 小时，与埋点生成器的 --window 3600 对齐：它会做 1 小时回填，
# 所以正常情况下这个值就在 3600s 上下，波动代表的是生成器行为不是链路问题。
EVENT_AGE_LINE=$(doris "
  SELECT CAST(PERCENTILE(x,0.5) AS BIGINT), CAST(PERCENTILE(x,0.99) AS BIGINT), COUNT(*)
  FROM (SELECT ts_ms/1000 - UNIX_TIMESTAMP(event_time) AS x FROM ods_user_event
        WHERE ts_ms >= (UNIX_TIMESTAMP() - 600) * 1000) t;")
EVENT_AGE_P50=$(num_or "$(echo "$EVENT_AGE_LINE" | cut -f1)" 0)
EVENT_AGE_P99=$(num_or "$(echo "$EVENT_AGE_LINE" | cut -f2)" 0)
EVENT_AGE_N=$(num_or "$(echo "$EVENT_AGE_LINE" | cut -f3)" 0)
EVENT_AGE_P50=$(printf '%.0f' "$EVENT_AGE_P50")
EVENT_AGE_P99=$(printf '%.0f' "$EVENT_AGE_P99")
if [[ "${EVENT_AGE_N:-0}" -eq 0 ]]; then
  EVENT_AGE_DESC='近 10 分钟没有任何写入（没有生产者在发事件）—— 0 不代表健康，看 pipeline_freshness。<1天 ok'
else
  EVENT_AGE_DESC="近 10 分钟写入的 $EVENT_AGE_N 条：**写入那一刻**数据已有多旧（不是延迟）。生成器有 1h 回填，正常在 0~3600s 摆动；持续天级说明生产者时钟或回放出了问题。<1天 ok / <3天 warn"
fi

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
# checkpoint 耗时 P95：取运行中作业里最慢的那个 P95。
#
# 口径：**最近若干次已完成的 checkpoint**（Flink 的 history，默认保留 10 条），
# 不是 summary 里的全生命周期 P95。
#
# 为什么不用 summary —— 实测踩过：summary 的 end_to_end_duration 是**从作业启动
# 那天起**的累计分布，永不遗忘。CDC 作业初始快照期间（155 万行全量）checkpoint
# 会被快照阶段拖到十几秒，这段历史一旦进了 summary 就永远留在那里，于是一个已经
# 平稳跑了几小时的作业，P95 仍然报 19,095ms 并常亮 critical。
# 那不是一个「当前状态」指标，是「这个作业历史上最难受的时候有多难受」。
#
# history 里每条 completed 记录的字段是 end_to_end_duration（毫秒）；个别版本
# 只给 duration，两个都读一遍，读不到就退回 summary —— 宁可口径退化，不要报 0。
CKPT_LINE=$(flink "/jobs/overview" | python -c "
import sys, json, subprocess
try:
    d = json.load(sys.stdin)
except Exception:
    print('0\t0'); sys.exit(0)

def p95(vals):
    v = sorted(vals)
    if not v:
        return 0
    pos = (len(v) - 1) * 0.95
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return int(v[lo] * (1 - (pos - lo)) + v[hi] * (pos - lo))

p95s, running, recent = [], 0, 0
for j in d.get('jobs', []):
    if j['state'] != 'RUNNING':
        continue
    running += 1
    try:
        r = subprocess.run(['docker','exec','rt-flink-jm','curl','-s',
                            'http://localhost:8081/jobs/%s/checkpoints' % j['jid']],
                           capture_output=True, text=True, timeout=15)
        c = json.loads(r.stdout)
    except Exception:
        continue
    hist = [e for e in (c.get('history') or [])
            if (e.get('status') or '').upper() == 'COMPLETED']
    vals = [e.get('end_to_end_duration') or e.get('duration') or 0 for e in hist]
    vals = [v for v in vals if v > 0]
    if len(vals) >= 3:
        recent += 1
        p95s.append(p95(vals))
    else:
        # history 太短（作业刚起、还没跑满 3 次）：这时全量 summary 反而是唯一能用的
        p = c.get('summary', {}).get('end_to_end_duration', {}).get('p95', 0) or 0
        if p:
            p95s.append(p)
print('%d\t%d\t%d' % (max(p95s) if p95s else 0, running, recent))
" 2>/dev/null || echo "0	0	0")
CKPT_P95=$(num_or "$(echo "$CKPT_LINE" | cut -f1)" 0)
JOBS_RUNNING=$(num_or "$(echo "$CKPT_LINE" | cut -f2)" 0)
CKPT_RECENT=$(num_or "$(echo "$CKPT_LINE" | cut -f3)" 0)

# ---------------------------------------------------------------------------
# 交易链 join 状态：07 四个源表的累计读取行数
# ---------------------------------------------------------------------------
# 为什么必须单独有这一个指标：
#   上面那个 pipeline_freshness 是 **MIN** 口径（三张主表取最近者），而
#   ads_realtime_overview 每 60 s 被批刷重写一次、update_time 跟着刷新。
#   于是只要 ADS 还在刷，dws_trade_overview_1m 停多久都看不出来。
#
#   2026-11-04 08:49 就出过这个事：07 用 latest-offset 重启，join 的 orders 侧状态
#   是空的，每条 order_items 变更都被 join 丢掉，sink 一行不写。而那一刻健康度
#   10 项全 ok —— 作业 RUNNING、checkpoint 正常、Kafka lag 0、新鲜度 0 s。
#   详见 flink/sql/07-dws-trade-overview-1m.sql 的文件头。
#
# 判据用「四个源累计读取 ≥ 40 万行」（快照实际是 41.5 万），因为它是 join 状态被
# 填满的**直接证据**，且不依赖有没有新数据进来：静态数据集下这个数会一直停在那儿，
# 不会因为「今天没人下单」误报红。
#
# 读不到 Flink REST 就**不写**这一项（空字符串 → 跳过），不写 0：
# 写 0 会被读成「僵尸」，那是把「没测到」说成「测到了坏消息」，与 --skip-kafka
# 的处理保持同一原则。作业确实不在 RUNNING 时才是真的 0。
OVERVIEW_JOIN=$(flink "/jobs/overview" | python -c "
import sys, json, subprocess
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
jid = ''
for j in d.get('jobs', []):
    if j.get('state') == 'RUNNING' and 'sink_dws_trade_overview_1m' in (j.get('name') or ''):
        jid = j['jid']
        break
if not jid:
    print(0)
    sys.exit(0)

def get(url):
    r = subprocess.run(['docker', 'exec', 'rt-flink-jm', 'curl', '-s', url],
                       capture_output=True, text=True, timeout=20)
    return json.loads(r.stdout)

try:
    vs = [v['id'] for v in get('http://localhost:8081/jobs/%s' % jid).get('vertices', [])
          if (v.get('name') or '').startswith('Source:')]
except Exception:
    sys.exit(0)
total = 0
for v in vs:
    idx = ','.join('%d.numRecordsOut' % i for i in range(8))
    try:
        total += sum(int(x['value']) for x in
                     get('http://localhost:8081/jobs/%s/vertices/%s/metrics?get=%s' % (jid, v, idx)))
    except Exception:
        sys.exit(0)
print(total)
" 2>/dev/null)

OVERVIEW_LEVEL=ok
if [[ -n "$OVERVIEW_JOIN" && "$OVERVIEW_JOIN" -lt 400000 ]]; then
  OVERVIEW_LEVEL=critical
fi


echo "==> 采集 Kafka 侧指标"
KAFKA_LAG=0
DUP_RATE=0
DUP_CNT=0
LATE_RATIO=0
# 窗口大小要在这里就定好，不能留在下面的 if 里：--skip-kafka 时那个分支不执行，
# 而后面的 PER_PART 和采样说明都要引用 $WINDOW，留在 if 里就是 "WINDOW: unbound variable"
# （set -u 会直接中断，表现为「采集跑完但一条都没写进去」）。
WINDOW="${HEALTH_WINDOW:-3000}"
N_PART="${HEALTH_PARTITIONS:-3}"

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
  # 但 /usr/bin/awk 就是 gawk（mktime / systime 可用），写成 gawk 会 command not found。
  #
  # 为什么要按「近期」过滤掉回填事件（HORIZON）：
  #   这个窗口里经常同时有「批量回填」和「实时流」两种流量。回填事件的 event_time
  #   是 1 小时甚至更早以前的，一旦它前面出现过一条新鲜事件，本分区已见最大值就被
  #   抬到「现在」，于是整批回填事件都落后 >60s，全部计入乱序。
  #   实测：注入 5% 乱序（100/2000）后指标报 **66.67%** —— 报的根本不是那 5%，
  #   而是「这批数据整体是旧的」。同理，混合流量下这个值会结构性地飘到 30% 上下，
  #   于是阈值 <5% 天天 critical，变成一条只会喊狼来了的指标。
  #   分工：回填有多旧，看 event_age_p99_s（它就是这个用途）；late_ratio 只看
  #   近 HORIZON 秒内的事件 —— 那才是「到达顺序与事件时间不一致」。
  STATS=$(printf '%s' "$SAMPLE" | MSYS_NO_PATHCONV=1 docker exec -i rt-kafka /usr/bin/awk '
    BEGIN { nowms = systime() * 1000; HORIZON = 600000 }
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
      n_all++
      # 重复率和时间无关，照全量算（幂等是幂等，不分新旧）
      if (eid in seen) dup++; else seen[eid] = 1

      # 事件时间转 epoch 毫秒，统计「比**本分区**已见最大值落后 > 60s」的比例
      d = substr(s,1,10); t = substr(s,12,8); ms = substr(s,21,3)
      split(d,dd,"-"); split(t,tt,":")
      ep = mktime(dd[1]" "dd[2]" "dd[3]" "tt[1]" "tt[2]" "tt[3]) * 1000 + ms
      # 回填/重放事件（比现在旧 HORIZON 以上）不进乱序的分子分母 —— 见上面的说明。
      # 它们不是「乱序到达」，是「生来就旧」，那件事由 event_age_p99_s 负责。
      if (ep < nowms - HORIZON) { old++; next }
      tot++
      if (ep > maxep[part]) maxep[part] = ep
      else if (maxep[part] - ep > 60000) late++
    }
    END {
      if (n_all == 0) { print "0\t0\t0\t0\t0" }
      else printf "%.4f\t%d\t%.4f\t%d\t%d\n",
                  dup*100.0/n_all, dup, (tot ? late*100.0/tot : 0), tot, old
    }')
  DUP_RATE=$(num_or "$(echo "$STATS" | cut -f1)" 0)
  DUP_CNT=$(num_or "$(echo "$STATS" | cut -f2)" 0)
  LATE_RATIO=$(num_or "$(echo "$STATS" | cut -f3)" 0)
  SAMPLED=$(num_or "$(echo "$STATS" | cut -f4)" 0)
  BACKFILL=$(num_or "$(echo "$STATS" | cut -f5)" 0)
  echo "    抽样 $((SAMPLED + BACKFILL)) 条（$N_PART 分区各 $PER_PART）：重复 $DUP_CNT 条（${DUP_RATE}%），乱序 ${LATE_RATIO}%（参与判定 $SAMPLED 条，回填 $BACKFILL 条已排除）"
  if [[ "$SAMPLED" -eq 0 ]]; then
    echo "    ⚠ 近 10 分钟内一条都没有，乱序率无从判定 —— 这一项本次不写，不报 0"
    LATE_MEASURED=0
  else
    LATE_MEASURED=1
  fi
else
  echo "    已跳过"
  SAMPLED=0
  BACKFILL=0
  LATE_MEASURED=0
fi

# ---------------------------------------------------------------------------
# 阈值判定 + 写入
# ---------------------------------------------------------------------------
# 端到端延迟：优先用 check_lag.py 的实测 P99，没有则退回 P3 的历史基线。
# 阈值跟着架构走 —— sink 的 buffer-flush.interval 是 10s，任何测量都不可能低于它，
# 所以「5000ms 就算慢」是错的阈值：它会把一个设计正常的链路永久标黄。
#
# 为什么用 sed 读 JSON 而不是 python -c：大屏进程用 MSYS_NO_PATHCONV=1 拉起本脚本
# （为了让 `docker exec ... /opt/kafka/...` 里的容器内路径不被 Git Bash 改写），而这个
# 开关同时关掉了「POSIX 路径 → Windows 路径」的翻译。于是 `python -c ... "/d/Users/..."`
# 把一个原生 Windows 程序打不开的路径递给了它，抛 FileNotFoundError，又被 2>/dev/null
# 吞掉，E2E 静默退回 3849 —— 同一条脚本人工跑正常、大屏跑降级，两边看到不同的数。
# 改成不起解释器，路径形态 / 解释器有无 / 文件编码这三个依赖一起消失。
LATENCY_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.latency.json"

json_num() {  # $1=键名 $2=文件（只取第一条匹配，值必须是裸数字）
  sed -n 's/.*"'"$1"'"[[:space:]]*:[[:space:]]*\(-\{0,1\}[0-9][0-9.]*\).*/\1/p' "$2" 2>/dev/null \
    | sed -n '1p'
}
json_str() {  # $1=键名 $2=文件（取字符串值）
  sed -n 's/.*"'"$1"'"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$2" 2>/dev/null \
    | sed -n '1p'
}

E2E_MEASURED=0
E2E_SAMPLES=0
E2E_AT="?"
if [[ -f "$LATENCY_FILE" ]]; then
  E2E_MEASURED=$(num_or "$(json_num p99 "$LATENCY_FILE")" 0)
  # 取整：JSON 里 p99 是 10034.6 这种浮点，而 `[[ x -le 0 ]]` 只认整数，
  # 直接比会报 "invalid arithmetic operator"。以前是 python round() 之后才落到 bash，
  # 换成 sed 读之后这层保证没了。
  E2E_MEASURED=$(awk -v v="$E2E_MEASURED" 'BEGIN{if (v ~ /^-?[0-9.]+$/) printf "%d", (v < 0 ? v - 0.5 : v + 0.5); else print 0}')
  E2E_SAMPLES=$(num_or "$(json_num samples "$LATENCY_FILE")" 0)
  E2E_AT=$(json_str measured_at "$LATENCY_FILE")
  [[ -n "$E2E_AT" ]] || E2E_AT="?"
  # 文件在但读不出数字 = 格式坏了，不能装作「没测过」，得说出来
  if [[ "$E2E_MEASURED" -le 0 ]]; then
    echo "    ⚠ $LATENCY_FILE 存在但读不出 p99，按未测量处理"
  fi
fi

if [[ "$E2E_MEASURED" -gt 0 ]]; then
  E2E_MS="$E2E_MEASURED"
  E2E_DESC="kafka→doris 实测 P99（N=$E2E_SAMPLES，采于 $E2E_AT）。**下限是 sink flush 间隔 10s**，<12000 ok / <20000 warn"
else
  E2E_MS=3849
  E2E_DESC="P3 单次实测基线 **不是 P99**；跑 scripts/check_lag.py --write 换成实测值。<5000 ok / <15000 warn"
  # 退回历史基线这件事必须自己说出来。它沉默过一次：大屏进程每次都在静默改写这个值，
  # 面板上显示 3849ms「很健康」，实际 P99 是 10035ms。
  echo "    ⚠ 未读到 $LATENCY_FILE，端到端延迟退回 P3 历史基线 3849ms（跑 check_lag.py --write 更新）"
fi
E2E_LEVEL=$(level_of 12000 20000 "$E2E_MS")

EVENT_AGE_LEVEL=$(level_of 86400 259200 "$EVENT_AGE_P99")   # 1天 warn / 3天 critical
FRESH_LEVEL=$(level_of 300 3600 "$FRESHNESS")
DUP_LEVEL=$(level_of 1 5 "$DUP_RATE")
LATE_LEVEL=$(level_of 5 20 "$LATE_RATIO")
LAG_LEVEL=$(level_of 1000 10000 "$KAFKA_LAG")
CKPT_LEVEL=$(level_of 5000 15000 "$CKPT_P95")
CANCEL_LEVEL=$(level_of 1 3 "$CANCELED_RATIO")

echo "==> 写入 ads_pipeline_health"
# 拆成两条写：Kafka 那三个指标在 --skip-kafka 时**根本没采**，
# 写 0 会被读成「零重复、零积压」，又是一个「没测量却报好消息」的假象。
# 没采就不写，让它保留上一次的值 —— 面板上看到的是「上次采集」的数字，
# 而不是一个凭空捏造的 0。
#
# overview_join_rows 同理：Flink REST 读不到就不写这一行（见上面那段注释）。
EXTRA_ROW=""
if [[ -n "$OVERVIEW_JOIN" ]]; then
  # threshold_desc 是 varchar(255) 且按**字节**算（中文 3 字节/字），这条说明必须
  # 压在 255 字节内 —— 超了 Doris strict mode 会整条 INSERT 拒掉，报
  # 「Insert has filtered data in strict mode」而**不带列名**，很容易误判成统计问题。
  EXTRA_ROW=",
  ('overview_join_rows','交易链 join 行数', $OVERVIEW_JOIN, '行', '$OVERVIEW_LEVEL',
   '07 四个源累计读取行数，join 状态已填满的直接证据。**唯一能发现「作业 RUNNING 却一行不写」的指标**。≥40 万 ok；低于=快照未完或 latest-offset 僵尸', 'capacity', NOW())"
fi
MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe mysql -h127.0.0.1 -P9030 -uroot -D realtime_dw -e "
INSERT INTO ads_pipeline_health
  (metric_key, metric_name, metric_value, metric_unit, metric_level, threshold_desc, metric_group, collect_time)
VALUES
  ('e2e_latency_ms',    '端到端延迟',      $E2E_MS,         'ms',  '$E2E_LEVEL',
   '$E2E_DESC', 'delay',   NOW()),
  ('pipeline_freshness_s','管道新鲜度',    $FRESHNESS,      's',   '$FRESH_LEVEL',
   '三张主表 update_time 取**最近**者（MIN）：任一条链在写就算活。它**不能**证明每条链都在写 —— 交易链要看 overview_join_rows。<300 ok / <3600 warn', 'delay',   NOW()),
  ('event_age_p99_s',   '事件年龄 P99',    $EVENT_AGE_P99,  's',   '$EVENT_AGE_LEVEL',
   '$EVENT_AGE_DESC', 'quality', NOW()),
  ('canceled_gmv',      '无效交易金额',    $CANCELED_GMV,   '元',  '$CANCEL_LEVEL',
   'canceled+unavailable 的 GMV 合计，涉及 $CANCELED_ORDERS 个订单。**仍计入 GMV 口径**，净 GMV = GMV − 本值', 'quality', NOW()),
  ('canceled_ratio',    '无效交易占比',    $CANCELED_RATIO, '%',   '$CANCEL_LEVEL',
   '无效交易金额 / 总 GMV。<1% ok / <3% warn', 'quality', NOW()),
  ('checkpoint_p95_ms', 'Checkpoint P95',  $CKPT_P95,       'ms',  '$CKPT_LEVEL',
   '运行中作业最近 10 次已完成 checkpoint 的 P95（history 口径，不是全生命周期 summary）。$CKPT_RECENT 个作业样本足够。<5000 ok / <15000 warn', 'capacity', NOW()),
  ('flink_jobs_running','运行中作业数',    $JOBS_RUNNING,   '个',  'ok',
   '状态为 RUNNING 的 Flink 作业数。4 slots 集群期望 3-4 个', 'capacity', NOW())$EXTRA_ROW;
" 2>&1 | head -5

if [[ "$SKIP_KAFKA" -eq 0 ]]; then
  # 乱序率单独一行，且只在「确实有近期样本」时写：近 10 分钟内一条事件都没有的话，
  # 分母是 0，写 0% 就成了「零乱序，很健康」—— 又一个「没测到却报好消息」。
  LATE_ROW=""
  if [[ "$LATE_MEASURED" -eq 1 ]]; then
    LATE_ROW=",
  ('late_ratio',        '事件乱序比例',    $LATE_RATIO,     '%',   '$LATE_LEVEL',
   '近 10 分钟内 $SAMPLED 条里，比**本分区**已见最大事件时间落后 >60s 的比例。回填/重放的 $BACKFILL 条已排除 —— 那件事看「事件年龄 P99」，混在一起会报成 66%。<5% ok / <20% warn', 'quality', NOW())"
  fi
  MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe mysql -h127.0.0.1 -P9030 -uroot -D realtime_dw -e "
INSERT INTO ads_pipeline_health
  (metric_key, metric_name, metric_value, metric_unit, metric_level, threshold_desc, metric_group, collect_time)
VALUES
  ('dup_rate',          '事件重复率',      $DUP_RATE,       '%',   '$DUP_LEVEL',
   '抽样窗口内 1-COUNT(DISTINCT event_id)/COUNT(*)。**不能用全量口径**，会被历史稀释。<1% ok / <5% warn', 'quality', NOW()),
  ('kafka_lag',         'Kafka 积压',      $KAFKA_LAG,      '条',  '$LAG_LEVEL',
   '三个消费者组 lag 之和。<1000 ok / <10000 warn', 'capacity', NOW())$LATE_ROW;
" 2>&1 | head -5
else
  echo "    --skip-kafka：dup_rate / late_ratio / kafka_lag 保留上次采集值"
fi

echo
echo "==> 采集结果"
MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe mysql -h127.0.0.1 -P9030 -uroot -D realtime_dw -e "
SELECT metric_group AS grp, metric_key, metric_name, metric_value, metric_unit, metric_level
FROM ads_pipeline_health ORDER BY metric_group, metric_key;" 2>&1
