#!/usr/bin/env bash
# =============================================================================
# scripts/stop.sh —— 安全关停：顺序、边界、怎么再开
# -----------------------------------------------------------------------------
# 用法：
#   bash scripts/stop.sh                 # 停大屏 + 停 6 个容器（数据全部保留）
#   bash scripts/stop.sh --keep-cluster  # 只停大屏，集群继续跑
#   bash scripts/stop.sh --purge         # 停完再删容器（down，卷仍保留）
#   bash scripts/stop.sh --status        # 只看状态，什么都不动
#   bash scripts/stop.sh --yes           # --purge 时跳过确认
#
# 为什么不是 `docker compose down -v`：
#   6 个组件里只有 Doris BE 用命名卷 rt-doris-be-data，其余（MySQL / Kafka）
#   都是仓库内的 bind mount。`-v` 会把这块卷删掉 —— 那不是"重启一下"，
#   是 112,650 行 DWD + 88,157 行 DWS + 8 张 ADS 全部消失，得从 MySQL
#   重跑 41 万行快照重建。这个脚本**任何分支都不会带 -v**。
#
# 为什么顺序是先停大屏再停容器：
#   大屏进程每 15 秒拉一次健康度（会 docker exec 进 FE 查表）。先停容器的话，
#   那个进程会在你眼前连续报错十几分钟，日志刷屏且状态写不进去；反过来停，
#   埋点也会先安静下来，再没有"生产者还在打、消费者已经没了"的窗口。
# =============================================================================
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# 切目录之前先把脚本自己的绝对路径算好：切到仓库根之后
# `basename "$0"` 会得到 "stop.sh"，而它在 scripts/ 下，就读不到了。
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
cd "$ROOT" || exit 1

MODE=stop
KEEP_CLUSTER=0
ASSUME_YES=0
PROC_RE='server/app\.py|gen_user_events\.py'
SNAPSHOT="scripts/.shutdown-snapshot.txt"
CONTAINERS=(rt-mysql rt-kafka rt-flink-jm rt-flink-tm rt-doris-fe rt-doris-be)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-cluster) KEEP_CLUSTER=1; shift ;;
    --purge)        MODE=purge; shift ;;
    --status)       MODE=status; shift ;;
    --yes|-y)       ASSUME_YES=1; shift ;;
    -h|--help)      sed -n '2,22p' "$SELF" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数：$1（--help 看用法）" >&2; exit 2 ;;
  esac
done

hr()   { printf '%s\n' "------------------------------------------------------------------"; }
ok()   { printf '  ✓ %s\n' "$*"; }
warn() { printf '  ! %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
die()  { printf '✗ %s\n' "$*" >&2; exit 1; }

# --- Windows 进程查询 -------------------------------------------------------
# Git Bash 的 ps 只看得到 msys 进程，看不到原生 python.exe，所以走 PowerShell。
# 这条命令必须用单引号：里面 $($_.ProcessId) 若被 bash 展开会变成命令替换。
PS_LIST='Get-CimInstance Win32_Process | ForEach-Object { "$($_.ProcessId)|$($_.CommandLine)" }'

list_procs() {
  MSYS_NO_PATHCONV=1 powershell -NoProfile -Command "$PS_LIST" 2>/dev/null
}
find_procs() {  # 输出 "PID|命令行"
  list_procs | grep -Ei "$PROC_RE" || true
}
kill_pid() {
  MSYS_NO_PATHCONV=1 powershell -NoProfile -Command \
    "Stop-Process -Id $1 -Force -ErrorAction SilentlyContinue" >/dev/null 2>&1
}
docker_up() { MSYS_NO_PATHCONV=1 docker info >/dev/null 2>&1; }

doris_rows() {  # 7 张主表的行数；FE 没起就返回空
  MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe mysql -h127.0.0.1 -P9030 -uroot \
    -D realtime_dw -N -B -e "
SELECT 'ods_user_event', COUNT(*) FROM ods_user_event
UNION ALL SELECT 'dwd_trade_order_detail', COUNT(*) FROM dwd_trade_order_detail
UNION ALL SELECT 'dwd_user_event_clean', COUNT(*) FROM dwd_user_event_clean
UNION ALL SELECT 'dws_trade_overview_1m', COUNT(*) FROM dws_trade_overview_1m
UNION ALL SELECT 'dws_traffic_funnel_1h', COUNT(*) FROM dws_traffic_funnel_1h
UNION ALL SELECT 'ads_realtime_overview', COUNT(*) FROM ads_realtime_overview
UNION ALL SELECT 'ads_order_stream', COUNT(*) FROM ads_order_stream" 2>/dev/null
}

# --- 状态 -------------------------------------------------------------------
show_status() {
  echo "==> 容器"
  if ! docker_up; then
    warn "Docker 引擎没在运行"
  else
    local n=0 st
    for c in "${CONTAINERS[@]}"; do
      st=$(MSYS_NO_PATHCONV=1 docker inspect -f '{{.State.Status}}{{if .State.Health}} / {{.State.Health.Status}}{{end}}' "$c" 2>/dev/null)
      if [[ -n "$st" ]]; then printf '    %-14s %s\n' "$c" "$st"; n=$((n + 1)); fi
    done
    [[ "$n" -eq 0 ]] && warn "6 个容器一个都没有（down 过了）"
  fi

  echo "==> 进程"
  local live
  live=$(find_procs)
  if [[ -z "$live" ]]; then
    info "大屏与埋点都没在跑"
  else
    while IFS='|' read -r pid cmd; do
      [[ -z "$pid" ]] && continue
      printf '    PID %-7s %s\n' "$pid" "$(echo "$cmd" | sed 's/.*python\.exe //; s/.*python //')"
    done <<< "$live"
  fi

  echo "==> Doris 数据"
  local rows
  rows=$(doris_rows)
  if [[ -z "$rows" ]]; then
    info "查不到（FE 没在跑）"
  else
    local base="" cur
    [[ -f "$SNAPSHOT" ]] && base="$SNAPSHOT"
    if [[ -n "$base" ]]; then
      # 快照文件最后一行是 `# 时间 关停` 的注释，必须滤掉，
      # 否则会被当成「表名 # 2026-11-04 …」去查，报「表不存在」。
      info "基线时间 $(grep '^#' "$base" | sed 's/^# //')"
      grep -v '^#' "$base" | while IFS=$'\t' read -r t c; do
        cur=$(echo "$rows" | awk -F'\t' -v k="$t" '$1==k{print $2}')
        if [[ -z "$cur" ]]; then
          printf '    %-26s %-9s → 表不存在\n' "$t" "$c"
        elif [[ "$cur" == "$c" ]]; then
          printf '    %-26s %-9s ✓ 未变\n' "$t" "$c"
        else
          printf '    %-26s %-9s → %s（差 %+d）\n' "$t" "$c" "$cur" "$((cur - c))"
        fi
      done
    else
      while IFS=$'\t' read -r t c; do
        printf '    %-26s %s\n' "$t" "$c"
      done <<< "$rows"
    fi
  fi
}

# --- 停大屏 -----------------------------------------------------------------
stop_dashboard() {
  echo "==> 停大屏"
  local live
  live=$(find_procs)
  if [[ -z "$live" ]]; then
    ok "没有在跑的大屏 / 埋点进程"
    return 0
  fi

  # 先埋点后大屏：先切生产端，避免"消费端已经没了、生产端还在打"的窗口。
  local pids
  pids=$(echo "$live" | grep -i 'gen_user_events' | cut -d'|' -f1 || true)
  for p in $pids; do
    kill_pid "$p" && ok "已停埋点 PID $p"
  done
  pids=$(echo "$live" | grep -i 'server/app\.py' | cut -d'|' -f1 || true)
  for p in $pids; do
    kill_pid "$p" && ok "已停大屏 PID $p"
  done

  # run.sh --with-events 有 trap 会顺带收掉埋点，这里等它自己退完
  local i
  for i in $(seq 1 10); do
    [[ -z "$(find_procs)" ]] && { ok "进程已全部退出"; return 0; }
    sleep 1
  done
  warn "仍有残留进程，可能需要在任务管理器里结束："
  find_procs | while IFS='|' read -r pid cmd; do info "PID $pid  $cmd"; done
  return 1
}

# --- 停容器 -----------------------------------------------------------------
# 逐个停，不用 `docker compose stop`，三个理由：
#   ① compose stop 是**并行**的，没有顺序保证 —— 可能出现「Flink 还在写、
#      Doris 已经关了」的窗口，那批 flush 就打在空气里
#   ② compose 文件里的 stop_grace_period 只对**重建后**的容器生效（它写进
#      Config.StopTimeout）。当前这批容器是旧配置创建的，实测 StopTimeout=nil，
#      于是走默认 10s：Doris BE / MySQL / Kafka 全部被 SIGKILL（退出码 137）
#   ③ 逐个停才能拿到每个容器的退出码，直接看出谁被硬杀
# 顺序按依赖倒序：计算层 → 结果层 → 消息层 → 数据源。
STOP_PLAN=(
  "rt-flink-tm:25"
  "rt-flink-jm:20"
  "rt-doris-be:45"
  "rt-doris-fe:30"
  "rt-kafka:30"
  "rt-mysql:60"
)

stop_one() {
  local name="$1" grace="$2" code t0 dt
  if [[ "$(MSYS_NO_PATHCONV=1 docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]]; then
    info "$(printf '%-13s' "$name") 本来就没在跑"
    return 0
  fi
  t0=$SECONDS
  MSYS_NO_PATHCONV=1 docker stop -t "$grace" "$name" >/dev/null 2>&1
  dt=$((SECONDS - t0))
  code=$(MSYS_NO_PATHCONV=1 docker inspect -f '{{.State.ExitCode}}' "$name" 2>/dev/null)
  case "$code" in
    0|143) ok "$(printf '%-13s' "$name") ${dt}s 退出码 $code（优雅）" ;;
    137)   warn "$(printf '%-13s' "$name") ${dt}s 退出码 137（${grace}s 内没退完，被 SIGKILL）" ;;
    *)     warn "$(printf '%-13s' "$name") ${dt}s 退出码 ${code:-?}" ;;
  esac
}

stop_cluster() {
  echo "==> 停容器"
  docker_up || die "Docker 引擎没在运行（那就已经都停了）"

  local item
  for item in "${STOP_PLAN[@]}"; do
    stop_one "${item%%:*}" "${item##*:}"
  done

  if [[ "$1" == "down" ]]; then
    if [[ "$ASSUME_YES" -eq 0 ]]; then
      echo "    接下来会删除 6 个容器和网络（**卷和 bind mount 都保留**）。"
      read -r -p "    确认？[y/N] " a
      [[ "${a:-n}" =~ ^[Yy]$ ]] || { warn "已取消删除容器，容器保持停止状态"; return 0; }
    fi
    ( cd docker && MSYS_NO_PATHCONV=1 docker compose \
        --profile source --profile compute --profile olap down )
    ok "容器已删除"
  fi

  # 卷必须还在 —— 这是整个脚本唯一真正要守的东西
  if MSYS_NO_PATHCONV=1 docker volume ls --format '{{.Name}}' 2>/dev/null \
       | grep -qx 'rt-doris-be-data'; then
    ok "卷 rt-doris-be-data 存在（Doris 数据在）"
  else
    warn "卷 rt-doris-be-data 不见了！Doris 数据已丢失"
  fi
}

tail_hint() {
  hr
  echo "数据位置（关停不影响）"
  info "MySQL   docker/volumes/mysql        （bind mount）"
  info "Kafka   docker/volumes/kafka        （bind mount）"
  info "Doris   rt-doris-be-data            （命名卷，唯一怕 down -v 的一份）"
  echo
  echo "再开起来"
  info "1) 集群 + 建表 + 提作业：bash scripts/demo.sh --no-dashboard"
  info "2) 大屏（含持续埋点）  ：bash dashboard/run.sh --with-events"
  info "3) 只看状态            ：bash scripts/stop.sh --status"
  echo
  warn "重启后 Flink 是空 session：作业不会自动恢复，必须重提"
  info "07 重提后要跑 41 万行快照（1~2 分钟），RUNNING 不等于已产出，"
  info "等 scripts/stop.sh --status 里 join 行数回到 40 万以上再验收"
}

# --- 主流程 -----------------------------------------------------------------
case "$MODE" in
  status)
    show_status
    ;;
  stop|purge)
    echo "关停前状态"
    show_status
    hr

    # 关停前记一次行数，下次 --status 就能直接看出有没有少
    rows=$(doris_rows)
    if [[ -n "$rows" ]]; then
      { echo "$rows"; echo "# $(date '+%Y-%m-%d %H:%M:%S') 关停"; } > "$SNAPSHOT"
      ok "已记录数据基线 → $SNAPSHOT"
    fi

    stop_dashboard || true
    if [[ "$KEEP_CLUSTER" -eq 1 ]]; then
      echo "==> 停容器"
      info "--keep-cluster：跳过，6 个容器继续跑"
    else
      if [[ "$MODE" == "purge" ]]; then
        stop_cluster down
      else
        stop_cluster stop
      fi
    fi
    tail_hint
    ;;
esac
