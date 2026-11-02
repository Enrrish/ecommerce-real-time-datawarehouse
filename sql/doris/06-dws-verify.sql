-- =============================================================================
-- 06-dws-verify.sql —— P5 验收：DWS 6 张表 vs DWD 明细重算
-- -----------------------------------------------------------------------------
-- 执行：MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe \
--         mysql -h127.0.0.1 -P9030 -uroot < sql/doris/06-dws-verify.sql
--
-- 验收标准：除 `payment_1d` 的三项（口径本就不同，见下）外，所有 diff 必须是 0。
--
-- 为什么用「DWD 明细重算」当基准，而不是「DWS 与 DWS 环比」：
-- 实时链路和离线重算走的是两套完全不同代码（Flink SQL vs Doris SQL），
-- 两边都算错同一个数的概率极低。这才叫校验。如果拿 DWS 自己跟昨天比，
-- 只证明它稳定，不证明它对。
--
-- 唯一允许不相等的是 dws_payment_method_1d：它从支付流水原始粒度算，
-- 而 trade 系列从订单行算。下面单独说明预期差值。
-- =============================================================================

USE `realtime_dw`;


-- -----------------------------------------------------------------------------
-- 一、4 张交易表：GMV 必须等于 15,843,553.24，且四表一致
-- -----------------------------------------------------------------------------
SELECT '1. trade 四表 GMV 一致性' AS `校验项`;
SELECT
  metric                                                                     AS `指标`,
  dws                                                                        AS `DWS`,
  dwd                                                                        AS `DWD 重算`,
  CAST(dws AS DECIMAL(30,4)) - CAST(dwd AS DECIMAL(30,4))                    AS `diff`
FROM (
  SELECT 'overview_1m.gmv' AS metric,
         (SELECT SUM(gmv) FROM dws_trade_overview_1m)                        AS dws,
         (SELECT SUM(item_total) FROM dwd_trade_order_detail)                AS dwd
  UNION ALL
  SELECT 'category_1d.gmv',
         (SELECT SUM(gmv) FROM dws_trade_category_1d),
         (SELECT SUM(item_total) FROM dwd_trade_order_detail)
  UNION ALL
  SELECT 'seller_1d.gmv',
         (SELECT SUM(gmv) FROM dws_trade_seller_1d),
         (SELECT SUM(item_total) FROM dwd_trade_order_detail)
  UNION ALL
  SELECT 'region_1d.gmv',
         (SELECT SUM(gmv) FROM dws_trade_region_1d),
         (SELECT SUM(item_total) FROM dwd_trade_order_detail)
) t
ORDER BY `指标`;


-- -----------------------------------------------------------------------------
-- 二、行数：DWS 的 key 数必须等于 DWD 按同样维度去重后的 key 数
-- -----------------------------------------------------------------------------
SELECT '2. DWS 行数 vs DWD 维度基数' AS `校验项`;
SELECT
  metric                                                                     AS `指标`,
  dws                                                                        AS `DWS`,
  dwd                                                                        AS `DWD 重算`,
  CAST(dws AS BIGINT) - CAST(dwd AS BIGINT)                                  AS `diff`
FROM (
  SELECT 'overview_1m.rows' AS metric,
         (SELECT COUNT(*) FROM dws_trade_overview_1m)                        AS dws,
         (SELECT COUNT(*) FROM (SELECT DISTINCT dt,
                  DATE_FORMAT(order_purchase_timestamp,'%H:%i')
                FROM dwd_trade_order_detail) x)                              AS dwd
  UNION ALL
  SELECT 'category_1d.rows',
         (SELECT COUNT(*) FROM dws_trade_category_1d),
         (SELECT COUNT(*) FROM (SELECT DISTINCT dt, category_name_en
                FROM dwd_trade_order_detail) x)
  UNION ALL
  SELECT 'seller_1d.rows',
         (SELECT COUNT(*) FROM dws_trade_seller_1d),
         (SELECT COUNT(*) FROM (SELECT DISTINCT dt, seller_id
                FROM dwd_trade_order_detail) x)
  UNION ALL
  SELECT 'region_1d.rows',
         (SELECT COUNT(*) FROM dws_trade_region_1d),
         (SELECT COUNT(*) FROM (SELECT DISTINCT dt, customer_state
                FROM dwd_trade_order_detail) x)
  UNION ALL
  SELECT 'funnel_1h.rows',
         (SELECT COUNT(*) FROM dws_traffic_funnel_1h),
         (SELECT COUNT(*) FROM (SELECT DISTINCT dt, hour, step FROM (
                SELECT DATE(event_time) dt, HOUR(event_time) hour, event_type step
                FROM dwd_user_event_clean) e) x)
) t
ORDER BY `指标`;


-- -----------------------------------------------------------------------------
-- 三、履约三态：分母口径是这张表最容易错的地方
-- -----------------------------------------------------------------------------
-- is_late 是三值（1 迟到 / 0 准时 / NULL 未妥投），分母必须是「已签收行数」
-- 而不是「订单行数」。写错的话延迟率会从 7.91% 掉到 7.74%，看着像是精度差异，
-- 实际是 2,454 个未妥投被误算成准时。
SELECT '3. 履约三态（分母 = 已签收）' AS `校验项`;
SELECT
  metric                                                                     AS `指标`,
  dws                                                                        AS `DWS`,
  dwd                                                                        AS `DWD 重算`,
  CAST(dws AS DECIMAL(30,4)) - CAST(dwd AS DECIMAL(30,4))                    AS `diff`
FROM (
  SELECT 'seller.delivered_cnt' AS metric,
         (SELECT SUM(delivered_cnt) FROM dws_trade_seller_1d)                AS dws,
         (SELECT COUNT(delivery_days) FROM dwd_trade_order_detail)           AS dwd
  UNION ALL
  SELECT 'seller.late_cnt',
         (SELECT SUM(late_cnt) FROM dws_trade_seller_1d),
         (SELECT SUM(is_late) FROM dwd_trade_order_detail)
  UNION ALL
  SELECT 'seller.sum_delivery_days',
         (SELECT SUM(sum_delivery_days) FROM dws_trade_seller_1d),
         (SELECT SUM(delivery_days) FROM dwd_trade_order_detail)
  UNION ALL
  SELECT 'seller.late_rate_pct',
         (SELECT ROUND(SUM(late_cnt)*100.0/SUM(delivered_cnt), 2) FROM dws_trade_seller_1d),
         (SELECT ROUND(SUM(is_late)*100.0/COUNT(delivery_days), 2) FROM dwd_trade_order_detail)
  UNION ALL
  SELECT 'seller.avg_delivery_days',
         (SELECT ROUND(SUM(sum_delivery_days)/SUM(delivered_cnt), 2) FROM dws_trade_seller_1d),
         (SELECT ROUND(SUM(delivery_days)/COUNT(delivery_days), 2) FROM dwd_trade_order_detail)
) t
ORDER BY `指标`;


-- -----------------------------------------------------------------------------
-- 四、订单数：各表数值不同是设计要求，不是错误
-- -----------------------------------------------------------------------------
-- 这一节**不判 diff**，只把数值并排列出来，方便一眼确认它们符合粒度预期：
--   overview / region = 98,666（一单一分钟、一单一州，可加）
--   category          = 99,470（跨类目的订单被各算一次，多 804）
--   seller            = 100,010（跨卖家的订单被各算一次，多 1,344）
--   payment           = 101,686（跨支付方式的订单被各算一次，多 2,246）
-- 谁要是把 category 的 SUM(order_cnt) 拿去当"总订单数"，就会多算 804 单。
-- 要总订单数只能查 DWD 的 COUNT(DISTINCT order_id)，或看 overview / region。
SELECT '4. 订单数粒度差异（预期不等，不判 diff）' AS `校验项`;
SELECT 'overview_1m' AS `表`, SUM(order_cnt) AS `SUM(order_cnt)` FROM dws_trade_overview_1m
UNION ALL SELECT 'region_1d',   SUM(order_cnt) FROM dws_trade_region_1d
UNION ALL SELECT 'category_1d', SUM(order_cnt) FROM dws_trade_category_1d
UNION ALL SELECT 'seller_1d',   SUM(order_cnt) FROM dws_trade_seller_1d
UNION ALL SELECT 'payment_1d',  SUM(order_cnt) FROM dws_payment_method_1d
UNION ALL SELECT '-- 基准：COUNT(DISTINCT order_id) 全表 --', COUNT(DISTINCT order_id) FROM dwd_trade_order_detail;


-- -----------------------------------------------------------------------------
-- 五、漏斗：pv 必须等于 DWD 去重后的行数
-- -----------------------------------------------------------------------------
-- 直接从 Kafka 聚合会得到 22,064（多了 60 条重复 event_id）。09 号作业里加了
-- ROW_NUMBER 去重视图，正确值是 22,004。这一项如果对不上，先查去重视图有没有生效。
SELECT '5. 漏斗 pv / uv' AS `校验项`;
SELECT
  metric                                                                     AS `指标`,
  dws                                                                        AS `DWS`,
  dwd                                                                        AS `DWD 重算`,
  CAST(dws AS BIGINT) - CAST(dwd AS BIGINT)                                  AS `diff`
FROM (
  SELECT 'funnel.pv' AS metric,
         (SELECT SUM(pv) FROM dws_traffic_funnel_1h)                         AS dws,
         (SELECT COUNT(*) FROM dwd_user_event_clean)                         AS dwd
  UNION ALL
  SELECT 'funnel.uv',
         (SELECT SUM(uv) FROM dws_traffic_funnel_1h),
         (SELECT SUM(uv) FROM (SELECT DATE(event_time) dt, HOUR(event_time) hour,
                                      event_type step, COUNT(DISTINCT user_id) uv
                               FROM dwd_user_event_clean GROUP BY 1,2,3) x)
) t
ORDER BY `指标`;


-- -----------------------------------------------------------------------------
-- 六、支付方式：三项口径与 trade 系列不同，预期有差
-- -----------------------------------------------------------------------------
-- 差额的来源（docs/02 §1 已声明）：
--   1. trade 系列是「有商品行的订单」98,666 单、GMV 15,843,553.24
--   2. 本表是「有支付记录的订单」99,440 单、金额 16,008,872.12
--   3. 差额 165,318.88 里混着两件事：775 个无明细订单的 830 笔支付（162,591.95），
--      以及 Olist 原始数据里「支付金额 ≠ 商品金额 + 运费」的天然偏差。
-- 所以这里只校验「本表自身与支付流水源表」一致，不与 GMV 比。
SELECT '6. 支付方式 vs ods_order_payments' AS `校验项`;
SELECT
  metric                                                                     AS `指标`,
  dws                                                                        AS `DWS`,
  src                                                                        AS `ods 基准`,
  CAST(dws AS DECIMAL(30,4)) - CAST(src AS DECIMAL(30,4))                    AS `diff`
FROM (
  SELECT 'payment_1d.rows' AS metric,
         (SELECT COUNT(*) FROM dws_payment_method_1d)                        AS dws,
         (SELECT COUNT(*) FROM (SELECT DISTINCT DATE(o.order_purchase_timestamp) dt,
                                       p.payment_type
                                FROM ods_order_payments p
                                JOIN ods_orders o ON p.order_id = o.order_id) x) AS src
  UNION ALL
  SELECT 'payment_1d.pay_cnt',
         (SELECT SUM(pay_cnt) FROM dws_payment_method_1d),
         (SELECT COUNT(*) FROM ods_order_payments)
  UNION ALL
  SELECT 'payment_1d.pay_amount',
         (SELECT SUM(pay_amount) FROM dws_payment_method_1d),
         (SELECT SUM(payment_value) FROM ods_order_payments)
) t
ORDER BY `指标`;
