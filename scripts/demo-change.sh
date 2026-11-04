#!/usr/bin/env bash
# =============================================================================
# demo-change.sh —— 改一行 MySQL，量出它走完整条链路要多久
# -----------------------------------------------------------------------------
# 用法（仓库根目录，Git Bash；集群和大屏已在跑）：
#   bash scripts/demo-change.sh              # 默认改 +1000.00，改完自动改回去
#   bash scripts/demo-change.sh --delta 500  # 换一个改动幅度
#   bash scripts/demo-change.sh --keep       # 故意不回滚（想看大屏停在新数字）
#
# 它量的是四个观测点，不是一坨「大概几秒」：
#   MySQL  → 改完立刻返回（binlog 写入）
#   DWS    → dws_trade_overview_1m 的当日 GMV 变化   ← CDC → Flink join → 聚合 → Doris sink
#   ADS    → ads_realtime_overview 的当日 GMV 变化   ← 大屏的 full 档批刷，60 s 一轮
#   大屏   → GET /api/dashboard 的 kpi.gmv 变化       ← 面板读 ADS，跟着变
#
# 预期形状（P8 实测，两次改动取一组）：
#   DWS  1.4 ~ 4.2 s    07 的 sink flush 是 1 s（无持续数据流，不产生版本堆积）
#   ADS  ≤ 60 s 一轮    这是 ads_realtime_overview 的批刷周期，不是链路慢
#   大屏  ≈ ADS + 1 s   面板 1 s 一轮轮询
#   所以「端到端 50 s 左右」的主体是**批刷周期**，不是 Flink。要看实时性就看 DWS 那一行。
#
# -----------------------------------------------------------------------------
# 为什么要先做一次「自检」再动手改
# -----------------------------------------------------------------------------
# 这张表的历史上一共踩过两次「作业 RUNNING 但一行都不写」，两次都没有任何报错：
#   1. 07 用 latest-offset 重启后，join 的一侧（orders）状态是空的，
#      每条 order_items 变更都因为「找不到订单」被 join 丢掉 —— 见 07 的文件头
#   2. 07 用 initial 重启后，快照没跑完之前同样不产出
# 两种情况下大屏都照旧显示昨天的数字，肉眼完全看不出来。所以本脚本第一步不是改数据，
# 而是确认「这一行改动有可能传播出去」：四个源表的累计读取行数必须已经到 40 万级。
# 到不了就直接报错退出，不去改那行 MySQL —— 否则会得到一个「什么都没发生」的
# 沉默结论，而人很容易把它理解成「链路慢」。
# =============================================================================
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

DELTA=1000
KEEP=0
# 用 while + shift 而不是 for：`for arg in "$@"` 的列表在循环开始时就固定了，
# 循环体里 shift 不会改变它，`--delta 500` 会被当成两个参数、报「未知参数 500」。
while [[ $# -gt 0 ]]; do
  case "$1" in
    --delta) DELTA="${2:-1000}"; shift 2 ;;
    --keep)  KEEP=1; shift ;;
    -h|--help) sed -n '2,38p' "$(basename "$0")" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数：$1（--help 看用法）" >&2; exit 2 ;;
  esac
done

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[32m  ✓ %s\033[0m\n' "$*"; }
warn() { printf '\033[33m  ! %s\033[0m\n' "$*"; }
die()  { printf '\033[31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

ts_ms() { date +%s%3N; }

# ---------------------------------------------------------------------------
# 取数小工具
# ---------------------------------------------------------------------------
# 一律 sed -n 1p 而不是 head -1：head 读到第一行就退出，上游 mysql/curl 会被 SIGPIPE
# 杀掉，而 pipefail 下整条赋值语句返回非 0，set -e 会把脚本静默干掉。
doris() {  # $1 = SQL；-D 不能省，Doris batch 模式没默认库
  MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe \
    mysql -h127.0.0.1 -P9030 -uroot -D realtime_dw -N -B -e "$1" 2>/dev/null | sed -n '1p'
}
olist() {  # $1 = SQL
  MSYS_NO_PATHCONV=1 docker exec rt-mysql \
    mysql -uroot -p"${MYSQL_PASSWORD:-root123}" -D olist -N -B -e "$1" 2>/dev/null | sed -n '1p'
}
flink_get() {  # $1 = REST 路径
  MSYS_NO_PATHCONV=1 docker exec rt-flink-jm curl -s "http://localhost:8081$1" 2>/dev/null
}
api_gmv() {
  curl -s -m 8 http://localhost:8080/api/dashboard | python -c "
import json,sys
print(json.load(sys.stdin)['kpi']['gmv'])" 2>/dev/null
}

# ---------------------------------------------------------------------------
# 0. 自检：07 作业在跑，且快照已经把 join 状态填满了
# ---------------------------------------------------------------------------
step "0. 自检（作业存活 + 快照已灌满 join 状态）"

JID=$(flink_get /jobs/overview | python -c "
import json,sys
for j in json.load(sys.stdin)['jobs']:
    if j['state'] == 'RUNNING' and 'sink_dws_trade_overview_1m' in j['name']:
        print(j['jid']); break
" 2>/dev/null)
[[ -n "$JID" ]] || die "找不到 RUNNING 的 07 作业（sink_dws_trade_overview_1m）。先跑 scripts/demo.sh"
ok "07 作业 ${JID:0:8} RUNNING"

SRC_VIDS=$(flink_get "/jobs/$JID" | python -c "
import json,sys
print(' '.join(v['id'] for v in json.load(sys.stdin)['vertices'] if v['name'].startswith('Source:')))
" 2>/dev/null)
[[ -n "$SRC_VIDS" ]] || die "读不到作业的算子列表（Flink REST 异常？）"

# 每个源都是单并行度，但仍按下标 0..7 探一遍：越界下标 Flink 会静默忽略，
# 返回里剩下的就是要加的数。这样并行度改了脚本也不会误判。
SRC_ROWS=0
for v in $SRC_VIDS; do
  n=$(flink_get "/jobs/$JID/vertices/$v/metrics?get=0.numRecordsOut,1.numRecordsOut,2.numRecordsOut,3.numRecordsOut,4.numRecordsOut,5.numRecordsOut,6.numRecordsOut,7.numRecordsOut" \
      | python -c "
import json,sys
print(sum(int(x['value']) for x in json.load(sys.stdin)))" 2>/dev/null)
  SRC_ROWS=$(( SRC_ROWS + $(printf '%s' "${n:-0}" | tr -dc '0-9') ))
done

if [[ "$SRC_ROWS" -lt 400000 ]]; then
  die "四个源表累计只读了 $SRC_ROWS 行（应 ≥ 40 万）。
     这说明 join 的 orders / customers 侧状态是空的，改任何一行都不会传播出去 ——
     作业本身还是 RUNNING、checkpoint 也正常，所以现象是「什么都没发生」而不是报错。
     两种可能：
       a) 快照还在跑（initial 提交后约 1~2 分钟）：等它跑完再执行本脚本
       b) 是用 latest-offset 重启的（文件头记录过这个坑）：把那 4 处改回 initial 重提"
fi
ok "四个源累计读取 $SRC_ROWS 行（快照已完成，join 状态可用）"

DASH_UP=$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://localhost:8080/api/ping || true)
[[ "$DASH_UP" == "200" ]] || warn "大屏没在 8080 上响应，最后一步「大屏可见」会被跳过"

# ---------------------------------------------------------------------------
# 1. 选目标行：拿大屏正在展示的那一天，挑该日已签收订单里最贵的一条明细
# ---------------------------------------------------------------------------
step "1. 选目标行"

DATA_DATE=$(curl -s -m 8 http://localhost:8080/api/dashboard | python -c "
import json,sys
print(json.load(sys.stdin)['meta']['data_date'])" 2>/dev/null)
[[ -n "$DATA_DATE" ]] || DATA_DATE=$(doris "SELECT MAX(dt) FROM ads_realtime_overview")
[[ -n "$DATA_DATE" ]] || die "取不到大屏的数据日期"

ROW=$(olist "SELECT oi.order_id, oi.order_item_id, oi.price, oi.freight_value,
                    DATE_FORMAT(o.order_purchase_timestamp, '%H:%i')
             FROM order_items oi JOIN orders o ON oi.order_id = o.order_id
             WHERE DATE(o.order_purchase_timestamp) = '$DATA_DATE'
               AND o.order_status = 'delivered'
             ORDER BY oi.price DESC LIMIT 1")

ORDER_ID=$(echo "$ROW" | cut -f1)
ITEM_ID=$(echo "$ROW" | cut -f2)
ORIG_PRICE=$(echo "$ROW" | cut -f3)
MINUTE=$(echo "$ROW" | cut -f5)
[[ -n "$ORDER_ID" ]] || die "在 $DATA_DATE 找不到可用的订单明细（业务库空？先跑 scripts/import.sh）"

NEW_PRICE=$(awk -v a="$ORIG_PRICE" -v d="$DELTA" 'BEGIN{printf "%.2f", a + d}')

printf '    数据日   %s\n' "$DATA_DATE"
printf '    订单     %s  明细 #%s  分钟桶 %s\n' "${ORDER_ID:0:16}…" "$ITEM_ID" "$MINUTE"
printf '    价格     %s → %s（%+.2f）\n' "$ORIG_PRICE" "$NEW_PRICE" "$DELTA"

# 无论从哪一步退出，都要把这行改回去：演示脚本最忌讳改坏业务库留给人收拾。
REVERTED=0
revert_row() {
  if [[ "$REVERTED" -eq 0 && -n "${ORIG_PRICE:-}" && -n "${ORDER_ID:-}" ]]; then
    REVERTED=1
    olist "UPDATE order_items SET price = $ORIG_PRICE
           WHERE order_id = '$ORDER_ID' AND order_item_id = $ITEM_ID" >/dev/null
    printf '\033[33m  ! 已自动回滚 price → %s\033[0m\n' "$ORIG_PRICE"
  fi
}
trap 'revert_row' EXIT INT TERM

# ---------------------------------------------------------------------------
# 2. 读基线
# ---------------------------------------------------------------------------
step "2. 读基线"

DWS_SQL="SELECT CAST(SUM(gmv) AS DECIMAL(20,2)) FROM dws_trade_overview_1m WHERE dt = '$DATA_DATE'"
ADS_SQL="SELECT CAST(SUM(gmv) AS DECIMAL(20,2)) FROM ads_realtime_overview WHERE dt = '$DATA_DATE'"

DWS0=$(doris "$DWS_SQL")
ADS0=$(doris "$ADS_SQL")
API0=""
[[ "$DASH_UP" == "200" ]] && API0=$(api_gmv)

printf '    DWS  %s 日 GMV = %s\n' "$DATA_DATE" "${DWS0:-读不到}"
printf '    ADS  %s 日 GMV = %s\n' "$DATA_DATE" "${ADS0:-读不到}"
printf '    大屏 kpi.gmv      = %s\n' "${API0:-未取}"
[[ -n "$DWS0" && -n "$ADS0" ]] || die "Doris 读不到 GMV，检查 http://localhost:9030 与 realtime_dw 是否就绪"

# ---------------------------------------------------------------------------
# 3. 改一行，等它一格一格往外走
# ---------------------------------------------------------------------------
step "3. 改一行 MySQL"

CHANGE_T0=$(ts_ms)
olist "UPDATE order_items SET price = $NEW_PRICE
        WHERE order_id = '$ORDER_ID' AND order_item_id = $ITEM_ID" >/dev/null
ok "已提交（price → $NEW_PRICE），计时从这里开始"

# 轮询等某个值离开基线；$1=标签 $2=SQL/来源 $3=基线 $4=预算秒
wait_away() {
  local label=$1 src=$2 want=$3 budget=$4 t0 v
  t0=$(ts_ms)
  while [[ $(( ($(ts_ms) - t0) / 1000 )) -lt $budget ]]; do
    case "$src" in
      dws) v=$(doris "$DWS_SQL") ;;
      ads) v=$(doris "$ADS_SQL") ;;
      api) v=$(api_gmv) ;;
    esac
    if [[ -n "$v" && "$v" != "$want" ]]; then
      printf '    %-10s %7d ms   %s → %s\n' "$label" "$(( $(ts_ms) - t0 ))" "$want" "$v"
      return 0
    fi
    sleep 1
  done
  printf '    %-10s 超时 %d s   仍是 %s\n' "$label" "$budget" "$want"
  return 1
}

step "4. 观测传播"
wait_away "DWS"  dws "$DWS0" 60  || warn "DWS 没动。先看 scripts/demo-change.sh 头部的自检说明"
wait_away "ADS"  ads "$ADS0" 120 || warn "ADS 没动。看大屏日志里 full 档有没有报错"
if [[ "$DASH_UP" == "200" && -n "$API0" ]]; then
  wait_away "大屏" api "$API0" 120 || warn "API 没动。可能是 ADS 变了但面板缓存/进程异常"
fi
printf '    端到端（改 MySQL → 大屏可见）%d ms\n' "$(( $(ts_ms) - CHANGE_T0 ))"

# ---------------------------------------------------------------------------
# 5. 改回去（顺便得到第二个样本：反向也通才算链路真的双向一致）
# ---------------------------------------------------------------------------
if [[ "$KEEP" -eq 1 ]]; then
  warn "--keep：不回滚。业务库现在的 price 是 $NEW_PRICE，别忘了手工改回来"
else
  step "5. 改回原值"
  BACK_T0=$(ts_ms)
  olist "UPDATE order_items SET price = $ORIG_PRICE
          WHERE order_id = '$ORDER_ID' AND order_item_id = $ITEM_ID" >/dev/null
  REVERTED=1
  ok "已提交（price → $ORIG_PRICE）"
  # 回滚的判据是「值回到基线」，不是「值变了」，所以不能用 wait_away
  wait_back() {
    local label=$1 src=$2 want=$3 budget=$4 t0 v
    t0=$(ts_ms)
    while [[ $(( ($(ts_ms) - t0) / 1000 )) -lt $budget ]]; do
      case "$src" in
        dws) v=$(doris "$DWS_SQL") ;;
        ads) v=$(doris "$ADS_SQL") ;;
        api) v=$(api_gmv) ;;
      esac
      if [[ "$v" == "$want" ]]; then
        printf '    %-10s %7d ms   回到 %s\n' "$label" "$(( $(ts_ms) - t0 ))" "$want"
        return 0
      fi
      sleep 1
    done
    printf '    %-10s 超时 %d s   仍是 %s（应为 %s）\n' "$label" "$budget" "${v:-?}" "$want"
    return 1
  }
  wait_back "DWS" dws "$DWS0" 60  || warn "DWS 没回到基线"
  wait_back "ADS" ads "$ADS0" 120 || warn "ADS 没回到基线"
  if [[ "$DASH_UP" == "200" && -n "$API0" ]]; then
    wait_back "大屏" api "$API0" 120 || warn "大屏没回到基线"
  fi
  printf '    端到端（改回 MySQL → 大屏复位）%d ms\n' "$(( $(ts_ms) - BACK_T0 ))"
fi

# ---------------------------------------------------------------------------
# 6. 一句话结论
# ---------------------------------------------------------------------------
step "结论"
cat <<EOF
    一次单行改动，走了 MySQL binlog → Flink CDC → 三表 join → 分钟聚合
    → Doris(UNIQUE upsert) → ADS 批刷 → 大屏。DWS 那一行是链路本身的延迟，
    ADS 那一行主要是 60 s 批刷周期 —— 想让它更快，调的是
    dashboard/server/app.py 里 full 档的 period，不是 Flink 的并行度。
EOF
