#!/usr/bin/env bash
# =============================================================================
# scripts/switch-mode.sh —— 实时数仓模式 / BI 分析模式，二选一
# -----------------------------------------------------------------------------
# 用法：
#   bash scripts/switch-mode.sh                 # 看当前是什么模式（同 status）
#   bash scripts/switch-mode.sh realtime        # 实时模式：6 个容器全起，停 FineBI
#   bash scripts/switch-mode.sh bi              # BI 模式：只留 Doris + FineBI
#   bash scripts/switch-mode.sh bi --submit     # BI 模式，但把 4 个 Flink 作业也提起来
#   bash scripts/switch-mode.sh status
#
# 为什么要有这个脚本（方案 D 的由来）：
#   这台机器物理内存 15.69GB，WSL 配额 10GB，Docker VM 实见 9715MB。
#   实时模式 6 个容器限额合计 8960MB，已经只剩 755MB 余量；
#   FineBI 单容器空载就要 1540MB。两套同时开必爆。
#   所以不做"共存"，做"切换"：任一时刻只让一套吃内存。
#
#   BI 模式为什么可以砍掉 Flink 和 Kafka：
#     数据已经落在 Doris 里了。Flink 停掉只是不再"继续计算"，
#     Doris 里的 112,650 行 DWD / 88,157 行 DWS 一行不少，FineBI 照样查。
#     停 Flink 释放 4096MB（tm 3328 + jm 768），停 Kafka 再释放 768MB。
#
#   内存账：
#     realtime  8960MB（mysql 768 + kafka 768 + fe 1536 + be 1792 + jm 768 + tm 3328）
#     bi       11136MB（fe 1536 + be 1792 + finebi 7808）
#     bi 的 7808 来自 FineBI 的 JVM 自检下限（四项和 7680 不可压），
#     所以它比 VM 实见的 9947MB 还高 —— 这是**纸面超卖**，不是笔误：
#     mem_limit 只是保险丝，实测三容器合计 3.7GB，余量 5.6GB。
#
# 不做的事（故意的）：
#   - 不碰 .wslconfig。提高到 12GB 能让两套共存，但 Windows 只剩 3.6GB，
#     浏览器的内存压缩会先把整机拖慢，得不偿失。
#   - 不删数据卷。切模式只是 stop，任何分支都不带 -v。
#   - 不改 finekey 那套 23 镜像的编排。FineOps 企业版最低 64GB，跑不动。
# =============================================================================
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
cd "$ROOT" || exit 1

MAIN_COMPOSE="$ROOT/docker/docker-compose.yml"
BI_COMPOSE="$ROOT/docker/docker-compose.finebi.yml"

MODE=status
SUBMIT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    realtime|rt) MODE=realtime; shift ;;
    bi)          MODE=bi; shift ;;
    status)      MODE=status; shift ;;
    --submit)    SUBMIT=1; shift ;;
    -h|--help)   sed -n '2,36p' "$SELF" | sed 's/^# \{0,1\}//'; exit 0 ;;
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

mem_report() {  # $1=mode
  local vm f fb total
  vm=$(vm_total_mb)
  if [[ -z "$vm" ]]; then warn "拿不到 Docker VM 内存（引擎没跑？）"; return 0; fi

  if [[ "$1" == "bi" ]]; then
    f=$(sum_limits rt-doris-fe rt-doris-be)
    fb=$(sum_limits rt-finebi)
    total=$((f + fb))
  else
    total=$(sum_limits rt-mysql rt-kafka rt-doris-fe rt-doris-be rt-flink-jm rt-flink-tm)
  fi

  echo "  容器限额合计 ${total}MB / VM ${vm}MB，余 $((vm - total))MB"
  if [[ $((vm - total)) -lt 512 ]]; then
    warn "余量不足 512MB，起之前先想清楚（Doris BE 导入时会有峰值）"
  fi
}

# --- 状态 -------------------------------------------------------------------
show_status() {
  local rt=0 bi=0 c st

  step "容器"
  for c in rt-mysql rt-kafka rt-flink-jm rt-flink-tm rt-doris-fe rt-doris-be rt-finebi; do
    st=$(d inspect -f '{{.State.Status}}{{if .State.Health}} / {{.State.Health.Status}}{{end}}' "$c" 2>/dev/null)
    if [[ -z "$st" ]]; then
      printf '    %-14s %s\n' "$c" "(不存在)"
      continue
    fi
    printf '    %-14s %s\n' "$c" "$st"
    [[ "$st" == running* ]] || continue
    case "$c" in
      rt-finebi) bi=$((bi + 1)) ;;
      *)         rt=$((rt + 1)) ;;
    esac
  done

  step "模式"
  if [[ "$bi" -gt 0 && "$rt" -gt 2 ]]; then
    warn "两套同时开着（${rt} 个数仓容器 + FineBI）—— 内存会超，考虑切一下"
  elif [[ "$bi" -gt 0 ]]; then
    ok "BI 分析模式（FineBI + ${rt} 个数仓容器）"
  elif [[ "$rt" -eq 6 ]]; then
    ok "实时数仓模式（6 个容器，FineBI 停着）"
  elif [[ "$rt" -gt 0 ]]; then
    info "非标准状态：${rt} 个数仓容器在跑，FineBI 没起"
  else
    info "全部停止"
  fi

  step "内存"
  docker_up_check
  mem_report "$([[ "$bi" -gt 0 ]] && echo bi || echo realtime)"

  step "入口"
  info "FineBI  ：http://localhost:${FINEBI_PORT:-9080}/webroot/decision"
  info "大屏    ：http://localhost:8080（需 bash dashboard/run.sh）"
  info "Flink UI：http://localhost:8081"
}

docker_up_check() { d info >/dev/null 2>&1 || die "Docker 引擎没在运行"; }

# --- 停 FineBI --------------------------------------------------------------
stop_finebi() {
  if [[ "$(d inspect -f '{{.State.Running}}' rt-finebi 2>/dev/null)" == "true" ]]; then
    # FineBI 的 HSQLDB 配置库要写 checkpoint，给 60s（compose 里也写了
    # stop_grace_period: 60s，但那只在重建后生效，这里显式传 -t 更稳）
    d stop -t 60 rt-finebi >/dev/null 2>&1
    ok "rt-finebi 已停（退出码 $(d inspect -f '{{.State.ExitCode}}' rt-finebi 2>/dev/null)）"
  else
    info "rt-finebi 本来就没在跑"
  fi
}

# --- 实时模式 ---------------------------------------------------------------
mode_realtime() {
  docker_up_check
  step "切到实时数仓模式"
  stop_finebi

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
  stop_finebi

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

  step "起 FineBI"
  if [[ "$(d inspect -f '{{.State.Running}}' rt-finebi 2>/dev/null)" == "true" ]]; then
    info "rt-finebi 已在跑"
  else
    ( cd "$ROOT/docker" && d compose -f docker-compose.finebi.yml up -d ) \
      | grep -E 'Started|Created|Error' | sed 's/^/  /'
    printf '    等待健康（首次启动约 25s，挂新卷要复制 webroot 会再久一点）'
    local i st
    for i in $(seq 1 40); do
      st=$(d inspect -f '{{.State.Health.Status}}' rt-finebi 2>/dev/null)
      [[ "$st" == "healthy" ]] && { echo " → healthy"; break; }
      if [[ "$(d inspect -f '{{.State.Status}}' rt-finebi 2>/dev/null)" == "exited" ]]; then
        echo " → 退出了"
        # 最常见的原因：卷被重建后，WEB-INF/lib 里又变回 client 版 polars jar，
        # bi-web 就会在 cluster 初始化时抛 ClassNotFoundException 并 shutdown。
        if d logs rt-finebi 2>&1 | grep -q 'BIWorkerModule'; then
          warn "缺 polars 引擎 jar（重建卷后会复发）—— 补法见 docs/note/27 §2.3"
        fi
        break
      fi
      printf '.'; sleep 5
    done
  fi

  mem_report bi

  hr
  echo "BI 模式下能查的数据（Doris 里是上次跑完的静态快照）"
  info "库 realtime_dw，FE 9030（MySQL 协议）"
  info "DWD dwd_trade_order_detail  112,650 行"
  info "DWS dws_trade_overview_1m    88,157 行"
  info "ADS 7 张表（ads_realtime_overview / ads_order_stream / …）"
  echo
  echo "FineBI 接数据源"
  info "新建「MySQL」连接：主机 doris-fe  端口 9030  用户 root  无密码"
  info "注意别用 localhost —— 容器内的 localhost 是容器自己"
  echo
  info "打开 http://localhost:${FINEBI_PORT:-9080}/webroot/decision"
}

# --- 主流程 -----------------------------------------------------------------
case "$MODE" in
  status)   show_status ;;
  realtime) mode_realtime ;;
  bi)       mode_bi ;;
esac
