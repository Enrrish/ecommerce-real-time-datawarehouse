-- =============================================================================
-- 09-ads-verify.sql —— P6 验收：ADS 6 张表 vs 上游重算
-- -----------------------------------------------------------------------------
-- 执行：MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe \
--         mysql -h127.0.0.1 -P9030 -uroot < sql/doris/09-ads-verify.sql
--
-- 验收标准：**第一节到第六节的所有 diff 必须是 0**。
-- 第七、八、九节是「预期不等」的项，不判 diff，只把数字并排列出来，
-- 用来说明口径差异是设计使然，不是算错了。
--
-- 与 06-dws-verify.sql 的分工：
--   06 验的是「DWS 与 DWD 明细重算一致」—— 证明聚合没算错；
--   09 验的是「ADS 与 DWS/DWD 一致」+「派生指标定义正确」—— 证明 ADS 没算错。
-- 两份都要在改链路后跑，缺一不可：ADS 错了但 DWS 对，只跑 06 是发现不了的。
-- =============================================================================

USE `realtime_dw`;


-- -----------------------------------------------------------------------------
-- 一、行数：每张 ADS 必须与上游同粒度
-- -----------------------------------------------------------------------------
-- 行数对不上通常是三种原因：① 上游多了一个维度值没被 GROUP BY 进去；
-- ② 过滤条件不一致（比如漏斗的 is_valid）；③ 刷新时上游正在写入，读到了半个快照。
-- 第三种在批刷新里确实存在，重跑一次就一致 —— 若重跑仍不等，才是逻辑问题。
SELECT '1. 行数与上游同粒度' AS `校验项`;
SELECT
  metric                                                              AS `指标`,
  ads                                                                 AS `ADS`,
  upstream                                                            AS `上游重算`,
  CAST(ads AS BIGINT) - CAST(upstream AS BIGINT)                      AS `diff`
FROM (
  SELECT 'realtime_overview.rows' AS metric,
         (SELECT COUNT(*) FROM ads_realtime_overview)                 AS ads,
         (SELECT COUNT(*) FROM dws_trade_overview_1m WHERE gmv IS NOT NULL) AS upstream
  UNION ALL
  SELECT 'category_rank_1d.rows',
         (SELECT COUNT(*) FROM ads_category_rank_1d),
         (SELECT COUNT(*) FROM dws_trade_category_1d WHERE gmv IS NOT NULL)
  UNION ALL
  SELECT 'funnel_1d.rows',
         (SELECT COUNT(*) FROM ads_funnel_1d),
         (SELECT COUNT(*) FROM (
            SELECT DATE(event_time), event_type FROM dwd_user_event_clean
            WHERE is_valid = 1 GROUP BY 1,2) x)
  UNION ALL
  SELECT 'payment_method_1d.rows',
         (SELECT COUNT(*) FROM ads_payment_method_1d),
         (SELECT COUNT(*) FROM dws_payment_method_1d WHERE pay_amount IS NOT NULL)
  UNION ALL
  SELECT 'order_stream.rows',
         (SELECT COUNT(*) FROM ads_order_stream),
         (SELECT COUNT(DISTINCT order_id) FROM dwd_trade_order_detail)
) t
ORDER BY `指标`;


-- -----------------------------------------------------------------------------
-- 二、GMV：三张交易系 ADS 表必须全部等于 15,843,553.24
-- -----------------------------------------------------------------------------
-- 这是全链路唯一允许的 GMV 定义 = SUM(item_total)，含运费、不剔除取消单。
-- docs/03 §1 已按此修正（原文档写的「SUM(price) 不含运费、剔除取消」与实现
-- 差 234 万，P6 开工时发现并统一到实现口径）。
--
-- 注：基线的绝对数值会随 P6 的脏数据注入变化（目前 25,005 条埋点、11 万条交易是
-- 注入后的状态），所以这里不硬编码数字，而是与 DWD 重算实时对比。
SELECT '2. GMV 一致性 vs DWD' AS `校验项`;
SELECT
  metric                                                              AS `指标`,
  ads                                                                 AS `ADS`,
  dwd                                                                 AS `DWD 重算`,
  CAST(ads AS DECIMAL(30,4)) - CAST(dwd AS DECIMAL(30,4))             AS `diff`
FROM (
  SELECT 'realtime_overview.gmv' AS metric,
         (SELECT SUM(gmv) FROM ads_realtime_overview)                 AS ads,
         (SELECT SUM(item_total) FROM dwd_trade_order_detail)         AS dwd
  UNION ALL
  SELECT 'category_rank_1d.gmv',
         (SELECT SUM(gmv) FROM ads_category_rank_1d),
         (SELECT SUM(item_total) FROM dwd_trade_order_detail)
  UNION ALL
  SELECT 'order_stream.gmv',
         (SELECT SUM(gmv) FROM ads_order_stream),
         (SELECT SUM(item_total) FROM dwd_trade_order_detail)
) t
ORDER BY `指标`;


-- -----------------------------------------------------------------------------
-- 三、支付金额：必须等于 ods_order_payments 的 SUM(payment_value)
-- -----------------------------------------------------------------------------
-- **这一项与 GMV 天然不等，不要互相比**。差额来源已在 docs/03 §1 说明：
-- 775 个无明细订单的支付（162,591.95）+ Olist 自身的「支付≠商品+运费」偏差。
-- 这里比的是「ADS 的支付金额」与「支付流水源的支付金额」，两者必须严格相等。
SELECT '3. 支付金额 vs ods_order_payments' AS `校验项`;
SELECT
  (SELECT CAST(SUM(pay_amount) AS DECIMAL(30,4)) FROM ads_payment_method_1d)  AS `ADS`,
  (SELECT CAST(SUM(payment_value) AS DECIMAL(30,4)) FROM ods_order_payments)  AS `ODS 支付流水`,
  (SELECT CAST(SUM(pay_amount) AS DECIMAL(30,4)) FROM ads_payment_method_1d)
    - (SELECT CAST(SUM(payment_value) AS DECIMAL(30,4)) FROM ods_order_payments) AS `diff`;


-- -----------------------------------------------------------------------------
-- 四、漏斗：pv / uv 必须与 DWD 明细日粒度重算一致
-- -----------------------------------------------------------------------------
-- 这一项是 P6 的核心证据之一。漏斗是唯一一条「DWS 直连 Kafka、不经过 DWD 去重」
-- 的链路，P6 注入 3% 重复 event_id 后，如果去重视图失效，这里会立刻多出 90 条。
-- 实测注入 3,091 条 → DWD / DWS / ADS 三层都只增加 3,001 条，重复被逐一挡下。
SELECT '4. 漏斗 vs DWD 明细重算' AS `校验项`;
SELECT
  metric                                                              AS `指标`,
  ads                                                                 AS `ADS`,
  dwd                                                                 AS `DWD 重算`,
  CAST(ads AS BIGINT) - CAST(dwd AS BIGINT)                           AS `diff`
FROM (
  SELECT 'funnel.pv' AS metric,
         (SELECT SUM(pv) FROM ads_funnel_1d)                          AS ads,
         (SELECT COUNT(*) FROM dwd_user_event_clean WHERE is_valid = 1) AS dwd
  UNION ALL
  SELECT 'funnel.uv_sum（跨天可加，每日内去重）',
         (SELECT SUM(uv) FROM ads_funnel_1d),
         (SELECT SUM(u) FROM (
            SELECT COUNT(DISTINCT user_id) AS u FROM dwd_user_event_clean
            WHERE is_valid = 1 GROUP BY DATE(event_time), event_type) x)
  UNION ALL
  SELECT 'funnel.session_sum（跨天可加，每日内去重）',
         (SELECT SUM(session_cnt) FROM ads_funnel_1d),
         (SELECT SUM(s) FROM (
            SELECT COUNT(DISTINCT session_id) AS s FROM dwd_user_event_clean
            WHERE is_valid = 1 GROUP BY DATE(event_time), event_type) x)
) t
ORDER BY `指标`;


-- -----------------------------------------------------------------------------
-- 五、占比与排名：占比必须每天合计为 1，排名必须从 1 连续无跳号
-- -----------------------------------------------------------------------------
-- 占比合计不为 1 只可能是两个原因：分母用了全表 SUM（跨天相除）而不是
-- PARTITION BY dt；或者 DECIMAL(8,4) 的舍入。前者错得离谱，后者是 ±0.0001。
-- 排名跳号则说明用了 RANK 而不是 ROW_NUMBER（平局会跳号）。
SELECT '5. 占比合计与排名连续性' AS `校验项`;
SELECT
  metric                                                              AS `指标`,
  异常天数                                                             AS `异常天数`,
  总天数                                                               AS `总天数`
FROM (
  SELECT 'category 占比≠1 的天数' AS metric,
         (SELECT COUNT(*) FROM (
            SELECT dt FROM ads_category_rank_1d GROUP BY dt
            HAVING ABS(CAST(SUM(gmv_ratio) AS DECIMAL(20,6)) - 1) > 0.001) x) AS 异常天数,
         (SELECT COUNT(DISTINCT dt) FROM ads_category_rank_1d)         AS 总天数
  UNION ALL
  SELECT 'payment 金额占比≠1 的天数',
         (SELECT COUNT(*) FROM (
            SELECT dt FROM ads_payment_method_1d GROUP BY dt
            HAVING ABS(CAST(SUM(amount_ratio) AS DECIMAL(20,6)) - 1) > 0.001) x),
         (SELECT COUNT(DISTINCT dt) FROM ads_payment_method_1d)
  UNION ALL
  SELECT 'category 排名不连续的天数',
         (SELECT COUNT(*) FROM (
            SELECT dt FROM ads_category_rank_1d GROUP BY dt
            HAVING MAX(rank_no) <> COUNT(*) OR MIN(rank_no) <> 1) x),
         (SELECT COUNT(DISTINCT dt) FROM ads_category_rank_1d)
  UNION ALL
  SELECT 'payment 排名不连续的天数',
         (SELECT COUNT(*) FROM (
            SELECT dt FROM ads_payment_method_1d GROUP BY dt
            HAVING MAX(rank_no) <> COUNT(*) OR MIN(rank_no) <> 1) x),
         (SELECT COUNT(DISTINCT dt) FROM ads_payment_method_1d)
) t
ORDER BY `指标`;


-- -----------------------------------------------------------------------------
-- 六、累计列：每天的「最后一个分钟桶」的 cum_gmv 必须等于当天 GMV
-- -----------------------------------------------------------------------------
-- cum_gmv 的定义是「当日 00:00 至本分钟的累计」。所以对任意一天，取该日
-- 最后一行（minute 最大）的 cum_gmv，必然等于该日 SUM(gmv)。
-- 若不等，说明窗口函数的 frame 或 ORDER BY 错了。
SELECT '6. 累计列正确性（末尾值=当日总额）' AS `校验项`;
SELECT
  (SELECT COUNT(*) FROM ads_realtime_overview)                        AS `总行数`,
  (SELECT COUNT(*) FROM (
     SELECT dt FROM ads_realtime_overview GROUP BY dt
     HAVING CAST(MAX(cum_gmv) AS DECIMAL(30,4)) <> CAST(SUM(gmv) AS DECIMAL(30,4))) x)
                                                                      AS `当日末尾值≠当日总额的天数（须为 0）`,
  (SELECT COUNT(*) FROM (
     SELECT dt FROM ads_realtime_overview GROUP BY dt
     HAVING CAST(MAX(cum_order_cnt) AS BIGINT) <> CAST(SUM(order_cnt) AS BIGINT)) x)
                                                                      AS `订单累计不一致天数（须为 0）`;


-- -----------------------------------------------------------------------------
-- 七、预期不等（一）：SUM(cum_gmv) 会放大 90 倍，这一列不能相加
-- -----------------------------------------------------------------------------
-- 累计列的每一行都是「从 00:00 加起来」的值，一天 60 个行就重复计了 60 次。
-- **不判 diff**，只把放大倍数摆出来当反面教材 —— 这个数字我第一次看到时
-- 以为数据坏了，实际是读法错了。倍数 = 平均每天有单分钟数。
SELECT '7. 【预期不等】SUM(cum_gmv) 的放大倍数' AS `说明`;
SELECT
  CAST(SUM(cum_gmv) AS DECIMAL(30,2))                                 AS `累计列总和`,
  CAST(SUM(gmv) AS DECIMAL(30,2))                                     AS `GMV 总和`,
  ROUND(CAST(SUM(cum_gmv) AS DECIMAL(30,2)) / SUM(gmv), 1)            AS `放大倍数`
FROM ads_realtime_overview;


-- -----------------------------------------------------------------------------
-- 八、预期不等（二）：订单级迟到率 ≠ 行级迟到率
-- -----------------------------------------------------------------------------
-- 这两个数**都对**，只是粒度不同：
--   行级 7.91%（8,715 / 110,196）—— 一个卖家发的某个商品行是否迟到
--   订单级 8.11%（7,827 / 96,476）—— 一单里任一行迟到即整单迟到（MAX 语义）
-- 订单级的分母更小（一单多行被收敛成一行），分子也更小（多行订单的多次迟到
-- 被收敛成一次），两者相除自然不同。混用会让大屏的延迟率和卖家报表对不上。
SELECT '8. 【预期不等】履约口径：订单级 vs 行级' AS `说明`;
SELECT '订单级（ads_order_stream）' AS `口径`,
       SUM(CASE WHEN is_late = 1 THEN 1 ELSE 0 END)                    AS `迟到`,
       SUM(CASE WHEN is_late IS NOT NULL THEN 1 ELSE 0 END)            AS `已签收`,
       ROUND(SUM(CASE WHEN is_late = 1 THEN 1 ELSE 0 END) * 100.0
             / NULLIF(SUM(CASE WHEN is_late IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS `延迟率%`
FROM ads_order_stream
UNION ALL
SELECT '行级（dwd_trade_order_detail）',
       SUM(CASE WHEN is_late = 1 THEN 1 ELSE 0 END),
       SUM(CASE WHEN is_late IS NOT NULL THEN 1 ELSE 0 END),
       ROUND(SUM(CASE WHEN is_late = 1 THEN 1 ELSE 0 END) * 100.0
             / NULLIF(SUM(CASE WHEN is_late IS NOT NULL THEN 1 ELSE 0 END), 0), 2)
FROM dwd_trade_order_detail;


-- -----------------------------------------------------------------------------
-- 九、预期不等（三）：DWS / ADS 的订单数跨维度不可加
-- -----------------------------------------------------------------------------
-- 一笔订单跨 3 个类目就在 3 个类目行各计一次。要「总订单数」只能查 DWD
-- 的 COUNT(DISTINCT order_id)，或取 overview / region（一单一分钟、一单一州）。
SELECT '9. 【预期不等】订单数粒度差异' AS `说明`;
SELECT 'overview_1m（可加）' AS `表`, SUM(order_cnt) AS `SUM(order_cnt)` FROM ads_realtime_overview
UNION ALL SELECT 'region_1d（DWS，可加，一单一州）', SUM(order_cnt) FROM dws_trade_region_1d
UNION ALL SELECT 'category_rank_1d（不可加）', SUM(order_cnt) FROM ads_category_rank_1d
UNION ALL SELECT 'payment_method_1d（不可加）', SUM(order_cnt) FROM ads_payment_method_1d
UNION ALL SELECT 'order_stream（订单粒度，真值）', COUNT(*) FROM ads_order_stream
UNION ALL SELECT 'DWD COUNT(DISTINCT order_id)', COUNT(DISTINCT order_id) FROM dwd_trade_order_detail;


-- -----------------------------------------------------------------------------
-- 十、链路健康度：8 个指标必须齐全，级别只能是 ok/warn/critical
-- -----------------------------------------------------------------------------
-- 这张表由 scripts/collect-pipeline-health.sh 旁路采集，不在 08-ads-refresh.sql
-- 里刷 —— 因为它的数据源跨 Doris / Flink / Kafka 三个系统。
SELECT '10. 链路健康度指标完整性' AS `校验项`;
SELECT metric_group AS `分组`, metric_key, metric_name, metric_value, metric_unit, metric_level
FROM ads_pipeline_health
ORDER BY metric_group, metric_key;

SELECT '   指标总数（应为 10）' AS `说明`, COUNT(*) AS `实际`,
       SUM(CASE WHEN metric_level IN ('ok','warn','critical') THEN 0 ELSE 1 END) AS `非法级别的个数（须为 0）`
FROM ads_pipeline_health;
