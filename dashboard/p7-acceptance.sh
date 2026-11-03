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
# 用法：
#   bash dashboard/p7-acceptance.sh          # 插一条并观测
#   bash dashboard/p7-acceptance.sh --clean  # 只清理
# =============================================================================
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ORDER_ID="p7verify0000000000000000000001"
ITEM_ID=1

MYSQL() { MSYS_NO_PATHCONV=1 docker exec rt-mysql mysql -uroot -proot123 -D olist -N -e "$1" 2>&1 \
            | grep -viE "insecure|^Warning" | tr -d '\r'; }
DORIS() { MSYS_NO_PATHCONV=1 docker exec rt-doris-fe mysql -uroot -P9030 -h127.0.0.1 -D realtime_dw \
            --batch --skip-column-names -e "$1" 2>/dev/null | tr -d '\r'; }

# 大屏 API 上的「今日实时 GMV」——这才是用户在屏幕上看到的数字
API_GMV() {
  MSYS_NO_PATHCONV=1 curl -s --noproxy '*' --max-time 10 \
    http://127.0.0.1:8080/api/dashboard 2>/dev/null \
    | python -c "import json,sys;print(json.load(sys.stdin)['today']['gmv'] or 0)" 2>/dev/null \
    || echo "ERR"
}

cleanup() {
  MYSQL "DELETE FROM order_items WHERE order_id='$ORDER_ID';
         DELETE FROM orders WHERE order_id='$ORDER_ID';"
  # Doris 侧必须单独清：07 作业是纯 INSERT（upsert）语义，源端删除不会让
  # 已聚合的行消失 —— 流式聚合没有 retract 传到 Doris 的路径。不清就会每次
  # 验收都在上一轮的基数上累加，基线永远对不上。
  DORIS "DELETE FROM dws_trade_overview_1m WHERE dt=CURDATE();
         DELETE FROM ads_realtime_overview WHERE dt=CURDATE();"
}

if [[ "${1:-}" == "--clean" ]]; then
  cleanup
  echo "已清理 $ORDER_ID"
  exit 0
fi

# --- 取一组真实存在的关联键，避免维表 join 出空值 -------------------------
read -r CUST PROD SELL PRICE FREIGHT < <(
  MYSQL "SELECT o.customer_id, oi.product_id, oi.seller_id, oi.price, oi.freight_value
         FROM order_items oi JOIN orders o ON oi.order_id = o.order_id LIMIT 1;" \
  | awk -F'\t' '{print $1, $2, $3, $4, $5}'
)
echo "关联键：customer=$CUST product=$PROD seller=$SELL price=$PRICE freight=$FREIGHT"

cleanup   # 幂等：万一上次残留

# --- 基线 -------------------------------------------------------------------
SNAP_SQL="
SELECT CONCAT_WS('|',
  (SELECT COUNT(*) FROM dws_trade_overview_1m WHERE dt=CURDATE()),
  (SELECT COALESCE(SUM(gmv),0) FROM dws_trade_overview_1m WHERE dt=CURDATE()),
  (SELECT COALESCE(SUM(gmv),0) FROM ads_realtime_overview WHERE dt=CURDATE()))"
_s=$(DORIS "$SNAP_SQL")
b_rows=$(echo "$_s" | cut -d'|' -f1)
b_gmv=$(echo "$_s"  | cut -d'|' -f2)
b_ads=$(echo "$_s"  | cut -d'|' -f3)
b_api=$(API_GMV)
echo "基线：今日 DWS 行=$b_rows  DWS-GMV=$b_gmv  ADS-GMV=$b_ads  大屏KPI=$b_api"
echo

# --- 变更：一条订单 = 两行 --------------------------------------------------
t0=$(date +%s%3N)
MYSQL "INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp,
                          order_approved_at, order_delivered_carrier_date,
                          order_delivered_customer_date, order_estimated_delivery_date)
       VALUES ('$ORDER_ID','$CUST','delivered', NOW(), NOW(), NOW(), NOW(), NOW());
       INSERT INTO order_items (order_id, order_item_id, product_id, seller_id,
                                shipping_limit_date, price, freight_value)
       VALUES ('$ORDER_ID',$ITEM_ID,'$PROD','$SELL', NOW(), 100.00, 10.00);"
t1=$(date +%s%3N)
echo ">>> MySQL 写入完成 +$((t1-t0))ms   （计时起点）"

# --- 逐层观测 ---------------------------------------------------------------
# 三条 Doris 查询合并成一条：每调一次 docker exec 有 ~800ms 的进程启动开销，
# 分开查会把采样周期拉到 4.5s，「4.4s 可见」其实可能是 0.3~4.8s 里的任意值。
hit_dws=""
hit_ads=""
hit_api=""
printf "%-9s %-8s %-12s %-12s %s\n" "t(ms)" "DWS行" "DWS今日GMV" "ADS今日GMV" "大屏今日KPI"
echo "-------------------------------------------------------------------"
for i in $(seq 1 40); do
  el=$(( $(date +%s%3N) - t1 ))
  s=$(DORIS "$SNAP_SQL")
  rows=$(echo "$s" | cut -d'|' -f1)
  gmv=$(echo "$s"  | cut -d'|' -f2)
  adg=$(echo "$s"  | cut -d'|' -f3)
  api=$(API_GMV)
  [[ -z "$hit_dws" && "$rows" != "$b_rows" ]] && hit_dws=$el
  [[ -z "$hit_ads" && "$adg"  != "$b_ads"  ]] && hit_ads=$el
  [[ -z "$hit_api" && "$api"  != "$b_api"  ]] && hit_api=$el
  printf "%-9s %-8s %-12s %-12s %s\n" "+$el" "$rows" "$gmv" "$adg" "$api"
  [[ -n "$hit_dws" && -n "$hit_ads" && -n "$hit_api" ]] && break
  sleep 0.2
done

echo
echo "=================================================="
echo "DWS 可见（Flink mysql-cdc → Doris）：${hit_dws:-未出现} ms"
echo "ADS 可见（今日增量刷新）：         ${hit_ads:-未出现} ms"
echo "大屏可见（KPI 数字变化）：         ${hit_api:-未出现} ms"
echo "=================================================="
echo "清理命令：bash dashboard/p7-acceptance.sh --clean"
