#!/usr/bin/env bash
# =============================================================================
# p7-acceptance.sh — P7 验收：改一行 MySQL，大屏 5 秒内数字变化
# -----------------------------------------------------------------------------
# 做法：往 MySQL 插一条「今天」的订单（orders + order_items 各一行），
# 然后逐层观测它什么时候出现在 Doris 里、什么时候出现在大屏 API 里。
# 结束后把这两行删掉，不留痕迹（删除也是 binlog，一并验证 delete 通路）。
#
# 为什么插今日订单而不是改历史订单：
#   改历史订单会污染 2017-11-24 展示日的指标，验收完无法还原；
#   插今日订单落在今天自己的分区里，删掉即恢复原状。
#
# 退出码：
#   0  PASS —— 大屏在 5 秒内看到变化
#   1  FAIL —— 超时、或链路/服务不可用（不会打印假的毫秒数）
#   2  参数错误
#
# 为什么对「查询失败」这么谨慎：
#   上一版用 `API_GMV || echo "ERR"`，然后判 `"$api" != "$b_api"` 就算命中。
#   结果接口返回 500 时 api="ERR"、基线是 "110.00"，两者不等 → 立刻记成
#   「大屏可见 306ms」。实测把服务停掉，脚本照样打印漂亮的毫秒数且 exit 0。
#   所以现在：curl 加 -f（HTTP>=400 即失败）、Doris 输出按格式校验，
#   失败一律不计入命中，连续失败到阈值直接判 FAIL。
#
# 用法：
#   bash dashboard/p7-acceptance.sh              # 插一条并观测
#   bash dashboard/p7-acceptance.sh --port 9000  # 大屏不在 8080 时
#   bash dashboard/p7-acceptance.sh --clean      # 只清理
# =============================================================================
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ORDER_ID="p7verify0000000000000000000001"
ITEM_ID=1
PORT=8080
MAX_ROUNDS=60          # 每轮约 0.9s（一次 docker exec + 一次 curl），够 50 秒
MAX_FAIL=3             # 连续几次取数失败就判链路不可用
SLA_MS=5000            # P7 验收标准

DO_CLEAN=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean) DO_CLEAN=1; shift ;;
    --port)  PORT="${2:-}"; shift 2 ;;
    -h|--help)
      sed -n '2,28p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "未知参数：$1（用 --help 看用法）" >&2; exit 2 ;;
  esac
done

MYSQL() { MSYS_NO_PATHCONV=1 docker exec rt-mysql mysql -uroot -proot123 -D olist -N -e "$1" 2>&1 \
            | grep -viE "insecure|^Warning" | tr -d '\r'; }

# Doris 快照：只有拿到形如 `1|110.00|0.00` 的三段数字才算成功。
# 失败返回非零 —— 调用方必须处理，不能把空串当成「数值变了」。
doris_snap() {
  local raw
  raw=$(MSYS_NO_PATHCONV=1 docker exec rt-doris-fe mysql -uroot -P9030 -h127.0.0.1 \
          -D realtime_dw --batch --skip-column-names -e "$SNAP_SQL" 2>&1 | tr -d '\r')
  # 正常输出只有一行 a|b|c；错误信息（ERROR 1105 ...）会被这个正则滤掉
  raw=$(printf '%s\n' "$raw" | grep -E '^[0-9.]+\|[0-9.]+\|[0-9.]+$' || true)
  [[ -z "$raw" ]] && return 1
  printf '%s\n' "$raw"
}

# 大屏 API 的「今日实时 GMV」——用户在屏幕上看到的数字。
# -f 是关键：HTTP 500 时 curl 直接非零退出，不会把 ERR 当成一个「值」。
api_gmv() {
  local raw out
  raw=$(MSYS_NO_PATHCONV=1 curl -s -f --noproxy '*' --max-time 5 \
          "http://127.0.0.1:${PORT}/api/dashboard" 2>/dev/null) || return 1
  out=$(printf '%s' "$raw" \
        | python -c "import json,sys;print(json.load(sys.stdin)['today']['gmv'] or 0)" 2>/dev/null) || return 1
  [[ -z "$out" ]] && return 1
  printf '%s\n' "$out"
}

cleanup() {
  MYSQL "DELETE FROM order_items WHERE order_id='$ORDER_ID';
         DELETE FROM orders WHERE order_id='$ORDER_ID';" >/dev/null
  # Doris 侧必须单独清：07 作业是纯 INSERT（upsert）语义，源端删除不会让
  # 已聚合的行消失 —— 流式聚合没有 retract 传到 Doris 的路径。不清就会每次
  # 验收都在上一轮的基数上累加，基线永远对不上。
  MSYS_NO_PATHCONV=1 docker exec rt-doris-fe mysql -uroot -P9030 -h127.0.0.1 \
    -D realtime_dw -e "DELETE FROM dws_trade_overview_1m WHERE dt=CURDATE();
                       DELETE FROM ads_realtime_overview WHERE dt=CURDATE();" >/dev/null 2>&1
}

if [[ $DO_CLEAN -eq 1 ]]; then
  cleanup
  echo "已清理 $ORDER_ID（MySQL + Doris 今日残留）"
  exit 0
fi

# 中途 Ctrl-C / 超时也要清干净，否则下一轮基线带着脏数据
ARMED=0
on_exit() {
  local rc=$?
  if [[ $ARMED -eq 1 ]]; then
    cleanup
    echo "（已清理 $ORDER_ID 与 Doris 今日残留）"
  fi
  exit $rc
}
trap on_exit EXIT INT TERM

# --- 取一组真实存在的关联键，避免维表 join 出空值 -------------------------
keys=$(MYSQL "SELECT o.customer_id, oi.product_id, oi.seller_id, oi.price, oi.freight_value
              FROM order_items oi JOIN orders o ON oi.order_id = o.order_id LIMIT 1;")
if [[ -z "$keys" ]]; then
  echo "FAIL: 取不到关联键（MySQL 不可用？试 docker ps）" >&2
  exit 1
fi
IFS=$'\t' read -r CUST PROD SELL PRICE FREIGHT <<< "$keys"
echo "关联键：customer=$CUST product=$PROD seller=$SELL price=$PRICE"

ARMED=1
cleanup   # 幂等：万一上次残留

# --- 基线 -------------------------------------------------------------------
# 三条 Doris 查询合并成一条：每调一次 docker exec 有 ~800ms 的进程启动开销，
# 分开查会把采样周期拉到 4.5s，「4.4s 可见」其实可能是 0.3~4.8s 里的任意值。
SNAP_SQL="
SELECT CONCAT_WS('|',
  (SELECT COUNT(*) FROM dws_trade_overview_1m WHERE dt=CURDATE()),
  (SELECT COALESCE(SUM(gmv),0) FROM dws_trade_overview_1m WHERE dt=CURDATE()),
  (SELECT COALESCE(SUM(gmv),0) FROM ads_realtime_overview WHERE dt=CURDATE()))"

if ! _s=$(doris_snap); then
  echo "FAIL: 基线取不到（Doris 不可用，或今日无表结构）" >&2
  exit 1
fi
b_rows=$(cut -d'|' -f1 <<< "$_s")
b_gmv=$(cut -d'|' -f2 <<< "$_s")
b_ads=$(cut -d'|' -f3 <<< "$_s")

if ! b_api=$(api_gmv); then
  echo "FAIL: 大屏 API 不可达（http://127.0.0.1:${PORT}/api/dashboard）" >&2
  echo "      先起服务：bash dashboard/run.sh --with-events" >&2
  exit 1
fi
echo "基线：今日 DWS 行=$b_rows  DWS-GMV=$b_gmv  ADS-GMV=$b_ads  大屏KPI=$b_api"
echo

# --- 变更：一条订单 = 两行 --------------------------------------------------
t0=$(date +%s%3N)
ins=$(MYSQL "INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp,
                          order_approved_at, order_delivered_carrier_date,
                          order_delivered_customer_date, order_estimated_delivery_date)
       VALUES ('$ORDER_ID','$CUST','delivered', NOW(), NOW(), NOW(), NOW(), NOW());
       INSERT INTO order_items (order_id, order_item_id, product_id, seller_id,
                                shipping_limit_date, price, freight_value)
       VALUES ('$ORDER_ID',$ITEM_ID,'$PROD','$SELL', NOW(), 100.00, 10.00);")
t1=$(date +%s%3N)
if [[ -n "$ins" ]]; then
  echo "FAIL: MySQL 写入失败：$ins" >&2
  exit 1
fi
echo ">>> MySQL 写入完成 +$((t1-t0))ms   （计时起点）"
echo

# --- 逐层观测 ---------------------------------------------------------------
hit_dws=""
hit_ads=""
hit_api=""
fail_streak=0
api_err=0          # API 取数失败次数，用于把 FAIL 原因说准
verdict=""
reason=""

printf "%-9s %-8s %-12s %-12s %s\n" "t(ms)" "DWS行" "DWS今日GMV" "ADS今日GMV" "大屏今日KPI"
echo "-------------------------------------------------------------------"
for _ in $(seq 1 "$MAX_ROUNDS"); do
  el=$(( $(date +%s%3N) - t1 ))

  if ! s=$(doris_snap); then
    fail_streak=$((fail_streak + 1))
    printf "%-9s %s\n" "+$el" "Doris 取数失败（连续 $fail_streak 次）"
    if (( fail_streak >= MAX_FAIL )); then
      verdict="FAIL"; reason="Doris 连续 $MAX_FAIL 次取数失败，链路不可用"
      break
    fi
    sleep 0.2
    continue
  fi
  fail_streak=0
  rows=$(cut -d'|' -f1 <<< "$s")
  gmv=$(cut -d'|' -f2 <<< "$s")
  adg=$(cut -d'|' -f3 <<< "$s")

  # API 失败时打印 ERR，但**不参与命中判定** —— 这正是上一版假阳性的来源
  if api=$(api_gmv); then
    [[ -z "$hit_api" && "$api" != "$b_api" ]] && hit_api=$el
  else
    api="ERR"
    api_err=$((api_err + 1))
  fi

  [[ -z "$hit_dws" && "$rows" != "$b_rows" ]] && hit_dws=$el
  [[ -z "$hit_ads" && "$adg"  != "$b_ads"  ]] && hit_ads=$el
  printf "%-9s %-8s %-12s %-12s %s\n" "+$el" "$rows" "$gmv" "$adg" "$api"
  [[ -n "$hit_dws" && -n "$hit_ads" && -n "$hit_api" ]] && break
  sleep 0.2
done

echo
echo "=================================================="
echo "DWS 可见（Flink mysql-cdc → Doris）：${hit_dws:-未出现} ms"
echo "ADS 可见（今日增量刷新）：         ${hit_ads:-未出现} ms"
echo "大屏可见（KPI 数字变化）：         ${hit_api:-未出现} ms"
[[ $api_err -gt 0 ]] && echo "（期间大屏 API 取数失败 $api_err 次，这些轮次未计入命中）"
echo "--------------------------------------------------"

if [[ -z "$verdict" ]]; then
  if [[ -z "$hit_api" ]]; then
    verdict="FAIL"
    if [[ $api_err -gt 0 ]]; then
      reason="大屏 API 有 $api_err 轮取数失败，无法判定（先看服务日志）"
    else
      reason="大屏 ${MAX_ROUNDS} 轮内未看到变化（服务在跑但数字没动，查 07 作业与 today 刷新档）"
    fi
  elif (( hit_api > SLA_MS )); then
    verdict="FAIL"
    reason="大屏 ${hit_api} ms > ${SLA_MS} ms"
  else
    verdict="PASS"
    reason="大屏 ${hit_api} ms ≤ ${SLA_MS} ms"
  fi
fi

if [[ "$verdict" == "PASS" ]]; then
  echo "判定：PASS —— $reason"
  echo "=================================================="
  exit 0
else
  echo "判定：FAIL —— $reason"
  echo "=================================================="
  exit 1
fi
