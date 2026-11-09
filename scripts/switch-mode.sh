#!/usr/bin/env bash
# =============================================================================
# scripts/switch-mode.sh —— 实时数仓模式 / BI 分析模式，二选一
# -----------------------------------------------------------------------------
# 用法：
#   bash scripts/switch-mode.sh                 # 看当前是什么模式（同 status）
#   bash scripts/switch-mode.sh realtime        # 实时模式：6 个容器全起
#   bash scripts/switch-mode.sh bi              # BI 模式：只留 Doris（fe + be）
#   bash scripts/switch-mode.sh bi --submit     # BI 模式，但把 4 个 Flink 作业也提起来
#   bash scripts/switch-mode.sh status
#
# 为什么要有这个脚本：
#   这台机器物理内存 15.69GB，WSL 配额 10GB，Docker VM 实见 9948MB。
#   实时模式 6 个容器限额合计 8960MB，只剩约 1GB 余量。
#   再往容器里塞一个 BI 工具必然爆 —— 所以 BI 不容器化。
#
#   BI 侧用的是**宿主 Windows 上的 FineBI 6.0 单机版**，直连 Doris 的
#   9030（MySQL 协议）。它跑在 Windows 里，不进 WSL2 的 10GB 配额，
#   等于把整块内存还给数仓。连接方式见 docs/note/28。
#
#   BI 模式为什么可以砍掉 Flink 和 Kafka：
#     数据已经落在 Doris 里了。Flink 停掉只是不再"继续计算"，
#     Doris 里的 112,650 行 DWD / 88,157 行 DWS 一行不少，FineBI 照样查。
#     停 Flink 释放 4096MB（tm 3328 + jm 768），停 Kafka 再释放 768MB。
#
#   内存账：
#     realtime  8960MB（mysql 768 + kafka 768 + fe 1536 + be 1792 + jm 768 + tm 3328）
#     bi        3328MB（fe 1536 + be 1792）
#
# 不做的事（故意的）：
#   - 不碰 .wslconfig。提高到 12GB 能让两套共存，但 Windows 只剩 3.6GB，
#     而 FineBI 就装在 Windows 上，抢了它的内存反而更慢。
#   - 不删数据卷。切模式只是 stop，任何分支都不带 -v。
#   - 不再维护容器版 FineBI。finekey 那套是集群编排包（21 个镜像包 + 编排器），
#     官方最低 64GB，且 bi-web(Java 8) 装不下 polars 引擎(Java 11)。
#     复盘见 docs/note/27。
# =============================================================================
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
cd "$ROOT" || exit 1

MODE=status
SUBMIT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    realtime|rt) MODE=realtime; shift ;;
    bi)          MODE=bi; shift ;;
    status)      MODE=status; shift ;;
    --submit)    SUBMIT=1; shift ;;
    -h|--help)   awk 'NR>2 && /^# =====/ {exit} NR>2 {sub(/^# ?/,""); print}' "$SELF"; exit 0 ;;
    *) echo "未知参数：$1（--help 看用法）" >&2; exit 2 ;;
  esac
done

hr()   { printf '%s\n' "------------------------------------------------------------------"; }
ok()   { printf '  ✓ %s\n' "$*"; }
warn() { printf '  ! %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
die()  { printf '✗ %s\n' "$*" >&2; exit 1; }
step() { printf '\n==> %s\n' "$*"; }

d() { MSYS_NO_PATHCONV=1 docker "$@"; }

ALL_CONTAINERS="rt-mysql rt-kafka rt-flink-jm rt-flink-tm rt-doris-fe rt-doris-be"

# --- 内存账 -----------------------------------------------------------------
# MemTotal 从 docker info 拿，是 VM 里 /proc/meminfo 的值，比 .wslconfig
# 里写的 10GB 少 300MB 左右（内核和页表），拿它算才准。
vm_total_mb() { d info --format '{{.MemTotal}}' 2>/dev/null | awk '{printf "%d", $1/1024/1024}'; }

# 求和"在跑或已配置"的容器限额。已停止的也算，因为下一步就会起。
sum_limits() {
  local total=0 c lim
  for c in "$@"; do
    lim=$(d inspect -f '{{.HostConfig.Memory}}' "$c" 2>/dev/null) || continue
    total=$((total + lim / 1024 / 1024))
  done
  echo "$total"
}

# docker stats 的 "662.3MiB" / "1.408GiB" → MB 整数
mem_to_mb() {
  awk -v s="$1" 'BEGIN{
    if      (s ~ /GiB$/) { sub(/GiB$/,"",s); printf "%d", s*1024 }
    else if (s ~ /MiB$/) { sub(/MiB$/,"",s); printf "%d", s }
    else if (s ~ /KiB$/) { sub(/KiB$/,"",s); printf "%d", s/1024 }
    else if (s ~ /B$/)   { sub(/B$/,"",s);   printf "%d", s/1048576 }
    else                 { printf "0" }
  }'
}

# 求和"实际占用"。容器没跑就按 0 算 —— 这是与 Docker Desktop 同一个口径。
sum_used() {
  local total=0 c used
  for c in "$@"; do
    used=$(d stats --no-stream --format '{{.MemUsage}}' "$c" 2>/dev/null | awk -F' / ' '{print $1}')
    [[ -n "$used" ]] && total=$((total + $(mem_to_mb "$used")))
  done
  echo "$total"
}

mem_report() {  # $1=mode
  local vm used limit over
  vm=$(vm_total_mb)
  if [[ -z "$vm" ]]; then warn "拿不到 Docker VM 内存（引擎没跑？）"; return 0; fi

  if [[ "$1" == "bi" ]]; then
    used=$(sum_used rt-doris-fe rt-doris-be)
    limit=$(sum_limits rt-doris-fe rt-doris-be)
  else
    used=$(sum_used $ALL_CONTAINERS)
    limit=$(sum_limits $ALL_CONTAINERS)
  fi

  # 两个口径必须分开说：一个是"现在吃了多少"，一个是"最多能吃多少"。
  # 混在一起讲就会出现"Docker 说还剩 5GB，脚本说超了 1GB"这种自相矛盾。
  echo "  实际占用 ${used}MB / VM ${vm}MB，余 $((vm - used))MB"
  if [[ $((vm - used)) -lt 512 ]]; then
    warn "实际剩余不足 512MB（Doris BE 导入时有峰值）"
  fi

  over=$((limit - vm))
  if [[ "$over" -gt 0 ]]; then
    info "限额合计 ${limit}MB，超卖 ${over}MB —— 允许，mem_limit 是每容器保险丝不是预占"
  else
    info "限额合计 ${limit}MB，余 $((vm - limit))MB"
  fi
}

# --- 状态 -------------------------------------------------------------------
show_status() {
  local rt=0 bi=0 c st

  step "容器"
  for c in $ALL_CONTAINERS; do
    st=$(d inspect -f '{{.State.Status}}{{if .State.Health}} / {{.State.Health.Status}}{{end}}' "$c" 2>/dev/null)
    if [[ -z "$st" ]]; then
      printf '    %-14s %s\n' "$c" "(不存在)"
      continue
    fi
    printf '    %-14s %s\n' "$c" "$st"
    [[ "$st" == running* ]] && rt=$((rt + 1))
  done

  step "模式"
  if [[ "$rt" -eq 6 ]]; then
    ok "实时数仓模式（6 个容器全开）"
  elif [[ "$rt" -eq 2 ]] && \
       [[ "$(d inspect -f '{{.State.Running}}' rt-doris-fe 2>/dev/null)" == "true" ]] && \
       [[ "$(d inspect -f '{{.State.Running}}' rt-doris-be 2>/dev/null)" == "true" ]]; then
    ok "BI 分析模式（只留 Doris，FineBI 在 Windows 宿主上跑）"
  elif [[ "$rt" -gt 0 ]]; then
    info "非标准状态：${rt} 个容器在跑"
  else
    info "全部停止"
  fi

  step "内存"
  docker_up_check
  mem_report "$([[ "$rt" -eq 2 ]] && echo bi || echo realtime)"

  step "入口"
  info "FineBI 6.0（Windows 单机版）：http://localhost:37799"
  info "  数据源 127.0.0.1:9030  root / 空密码  realtime_dw"
  info "大屏    ：http://localhost:8080（需 bash dashboard/run.sh）"
  info "Flink UI：http://localhost:8081"
}

docker_up_check() { d info >/dev/null 2>&1 || die "Docker 引擎没在运行"; }

# 轮询到 healthy。$1=容器名 $2=最多等几秒（默认 120）
# 容器退出就立刻返回，不白等 —— 引擎类容器启动失败是直接的，不会自愈。
wait_healthy() {
  local c=$1 max=${2:-120} i st
  for i in $(seq 1 $((max / 5))); do
    if [[ "$(d inspect -f '{{.State.Status}}' "$c" 2>/dev/null)" == "exited" ]]; then
      echo " → 退出了（退出码 $(d inspect -f '{{.State.ExitCode}}' "$c" 2>/dev/null)）"
      return 1
    fi
    st=$(d inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$c" 2>/dev/null)
    if [[ "$st" == "healthy" ]]; then echo " → healthy（$((i * 5))s）"; return 0; fi
    printf '.'; sleep 5
  done
  echo " → ${max}s 内没 healthy"
  return 1
}

# --- 实时模式 ---------------------------------------------------------------
mode_realtime() {
  docker_up_check
  step "切到实时数仓模式"

  step "起 6 个容器"
  ( cd "$ROOT/docker" && d compose --profile source --profile compute --profile olap up -d ) \
    | grep -E 'Started|Created|Running|Error' | sed 's/^/  /'

  mem_report realtime

  hr
  warn "Flink 是空 session —— 容器起来了，作业不会自动回来"
  if [[ "$SUBMIT" -eq 1 ]]; then
    submit_jobs
  else
    info "提作业：bash scripts/switch-mode.sh realtime --submit"
    info "或整链重建：bash scripts/demo.sh --no-dashboard"
  fi
  info "07 重提后要跑 41 万行快照（1~2 分钟），等 join 行数回到 40 万以上再验收"
}

submit_jobs() {
  step "提交 4 个作业"
  local f
  for f in flink/sql/04*.sql flink/sql/06*.sql flink/sql/07*.sql flink/sql/09*.sql; do
    [[ -f "$f" ]] || continue
    printf '    %-24s ' "$(basename "$f")"
    if bash scripts/submit-sql.sh "$f" >/tmp/submit.$$ 2>&1; then
      ok "已提交"
    else
      warn "失败（见下方）"
      sed 's/^/      /' /tmp/submit.$$ | tail -5
    fi
  done
  rm -f /tmp/submit.$$
}

# --- BI 模式 ----------------------------------------------------------------
# 顺序：先停 Flink（它每 10s 往 Doris 写一批，先断写端），再停 Kafka，
# 最后停 MySQL。反过来停会让 Flink 报一串 connection refused 再自己退出，
# 日志脏且 checkpoint 可能失败。
mode_bi() {
  docker_up_check
  step "切到 BI 分析模式"

  step "停 Flink / Kafka / MySQL"
  local c
  for c in rt-flink-tm rt-flink-jm rt-kafka rt-mysql; do
    if [[ "$(d inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" == "true" ]]; then
      d stop -t 45 "$c" >/dev/null 2>&1
      ok "$c 已停（退出码 $(d inspect -f '{{.State.ExitCode}}' "$c" 2>/dev/null)）"
    else
      info "$c 本来就没在跑"
    fi
  done

  step "确保 Doris 在跑"
  ( cd "$ROOT/docker" && d compose --profile olap up -d doris-fe doris-be ) \
    | grep -E 'Started|Created|Running|Error' | sed 's/^/  /'

  printf '    doris-fe'
  wait_healthy rt-doris-fe 120 || warn "FE 没起来，看：docker logs rt-doris-fe"
  printf '    doris-be'
  wait_healthy rt-doris-be 180 || warn "BE 没起来，看：docker logs rt-doris-be"

  mem_report bi

  hr
  echo "BI 模式下的连接参数（FineBI 在 Windows 宿主上，走发布端口）"
  info "主机 127.0.0.1   端口 9030   用户 root   密码 空"
  info "库   realtime_dw   驱动 MySQL 8（useSSL=false）"
  echo
  echo "直连的 7 张 ADS 表"
  info "ads_realtime_overview    88,157 行  分钟级 GMV/订单概览"
  info "ads_order_stream         98,666 行  订单明细流"
  info "ads_category_rank_1d     18,990 行  品类排行"
  info "ads_region_map_1d        10,701 行  州维度地图"
  info "ads_payment_method_1d     2,276 行  支付方式"
  info "ads_funnel_1d                16 行  转化漏斗"
  info "ads_pipeline_health       5,203 行  链路健康度"
  echo
  info "口径红线：cum_gmv 不可 SUM；buyer_cnt 跨维度不可加"
  info "打开 http://localhost:37799"
}

# --- 主流程 -----------------------------------------------------------------
case "$MODE" in
  status)   show_status ;;
  realtime) mode_realtime ;;
  bi)       mode_bi ;;
esac
