-- =============================================================================
-- 08-ads-refresh.sql —— ADS 刷新（幂等，可随时重跑）
-- -----------------------------------------------------------------------------
-- 执行：MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe \
--         mysql -h127.0.0.1 -P9030 -uroot < sql/doris/08-ads-refresh.sql
--
-- =============================================================================
-- 为什么用 INSERT OVERWRITE 而不是 INSERT INTO
-- =============================================================================
-- UNIQUE 表上 INSERT INTO 是「按主键覆盖」，够用但有个盲区：**新结果里消失的行
-- 不会被删掉**。举例，某天某个类目的商品全被取消，重算后这类目不该再出现，
-- 但 INSERT INTO 会把它上一次的行留在表里 —— 排行里挂着一个已经不存在的类目，
-- 而 Doris 不会报错，前端也不会知道。
--
-- INSERT OVERWRITE 是整表覆盖（先清后写），结果集就是表的最终状态，
-- 天然满足「刷新」语义。代价是中途失败会留空表，重跑一次即可 —— 对 ADS
-- 这种全量重建级别的小表（最大 9.8 万行）完全可接受。
--
-- =============================================================================
-- 刷新顺序有依赖：先父后子
-- =============================================================================
--   1. ads_realtime_overview   ← dws_trade_overview_1m
--   2. ads_category_rank_1d    ← dws_trade_category_1d
--   3. ads_funnel_1d           ← dwd_user_event_clean（回明细重算 UV）
--   4. ads_payment_method_1d   ← dws_payment_method_1d
--   5. ads_order_stream        ← dwd_trade_order_detail（收敛到订单粒度）
--   6. ads_region_map_1d       ← dws_trade_region_1d + dim_geo（补坐标）
--
-- ads_pipeline_health 不在这里 —— 它的数据源跨 Doris / Flink / Kafka 三个系统，
-- 由 scripts/collect-pipeline-health.sh 统一采集写入。
--
-- =============================================================================
-- 一个必须提前知道的坑：比率列的整除
-- =============================================================================
-- Doris 里 `BIGINT / BIGINT` 是整数除法，7 / 10 会得到 0 而不是 0.7。
-- 所以每个比率都要写成 `a * 1.0 / b` 或先 CAST 成 DECIMAL。
-- 这个错误不会报错，只会让转化率整齐地变成 0 或 1。
-- 下面统一用 `CAST(x AS DECIMAL(20,6)) / y` 的写法，不用 `* 1.0` —— 后者在
-- 除数为 0 时仍会抛错，而 NULLIF 才是正解。
-- =============================================================================

USE `realtime_dw`;


-- -----------------------------------------------------------------------------
-- 1. ads_realtime_overview —— 分钟概览 + 派生指标 + 当日累计曲线
-- -----------------------------------------------------------------------------
-- cum_gmv / cum_order_cnt 用窗口函数算「当日累计」。这里能直接用 SUM() OVER
-- 而不用担心 P4 踩过的 watermark 坑，因为 ADS 是**批**刷新 —— 数据已经是完整的
-- 静态快照，不存在「窗口尾部等着后续事件推动水位线」的问题。
-- 流里绝不能这么写，批里这么写最简洁。
INSERT OVERWRITE TABLE `ads_realtime_overview`
SELECT
  `dt`,
  `minute`,
  `gmv`,
  `order_cnt`,
  `item_cnt`,
  `buyer_cnt`,
  `pay_user_cnt`,
  `freight_total`,

  -- 客单价：分母是订单数，不是订单行数（docs/03 §1）
  CAST(`gmv` AS DECIMAL(20,6)) / NULLIF(`order_cnt`, 0)  AS `aov`,
  -- 件单价：分母是订单行数
  CAST(`gmv` AS DECIMAL(20,6)) / NULLIF(`item_cnt`, 0)   AS `avg_item_price`,
  -- 运费占比：分母是含运费的 GMV，所以这个值天然 < 1
  CAST(`freight_total` AS DECIMAL(20,6)) / NULLIF(`gmv`, 0) AS `freight_ratio`,

  -- 当日累计：ORDER BY minute 升序，累计到当前行
  SUM(`gmv`)       OVER (PARTITION BY `dt` ORDER BY `minute`
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `cum_gmv`,
  SUM(`order_cnt`) OVER (PARTITION BY `dt` ORDER BY `minute`
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `cum_order_cnt`,

  NOW() AS `update_time`
FROM `dws_trade_overview_1m`
WHERE `gmv` IS NOT NULL;


-- -----------------------------------------------------------------------------
-- 2. ads_category_rank_1d —— 品类排行 + 占比
-- -----------------------------------------------------------------------------
-- `gmv_ratio` 的分母用 SUM() OVER (PARTITION BY dt)，不是全表 SUM：
-- 全表 SUM 会把 616 天的 GMV 加在一起，占比会算成 0.000x，看起来像数据坏了。
-- 这个错误在「只有一个日期有数据」时完全看不出来，等数据多起来才暴露。
INSERT OVERWRITE TABLE `ads_category_rank_1d`
SELECT
  `dt`,
  `category_name`,

  ROW_NUMBER() OVER (PARTITION BY `dt` ORDER BY `gmv` DESC NULLS LAST) AS `rank_no`,
  `gmv`,
  `order_cnt`,
  `item_cnt`,
  `seller_cnt`,

  CAST(`gmv` AS DECIMAL(20,6)) / NULLIF(`order_cnt`, 0) AS `aov`,
  CAST(`gmv` AS DECIMAL(20,6))
    / NULLIF(SUM(`gmv`) OVER (PARTITION BY `dt`), 0)    AS `gmv_ratio`,

  NOW() AS `update_time`
FROM `dws_trade_category_1d`
WHERE `gmv` IS NOT NULL;


-- -----------------------------------------------------------------------------
-- 3. ads_funnel_1d —— 日粒度漏斗（回 DWD 明细重算 UV）
-- -----------------------------------------------------------------------------
-- 两张 CTE：
--   `daily`   从明细做日去重聚合 —— 这一步不能省，不能拿小时表 SUM
--   `ordered` 给环节编号，因为 CASE 出来的别名不能在同一层 SELECT 里被 ORDER BY 引用
--
-- 转化率用 LEFT JOIN 取首环节 UV，而不是 FIRST_VALUE() OVER (...)：
-- Doris 对带 UNBOUNDED FOLLOWING 的 frame 支持有限，而 join 一份首环节的小结果集
-- 语义更直白、也更好排查（能单独跑 daily 看每天各环节到底多少 UV）。
INSERT OVERWRITE TABLE `ads_funnel_1d`
WITH `daily` AS (
  SELECT
    DATE(`event_time`)          AS `dt`,
    `event_type`                AS `step`,
    COUNT(*)                    AS `pv`,
    COUNT(DISTINCT `user_id`)   AS `uv`,
    COUNT(DISTINCT `session_id`) AS `session_cnt`
  FROM `dwd_user_event_clean`
  -- 用 is_valid = 1 过滤，而不是重写一遍校验条件：
  -- DWD 已经把「字段缺失 / 类型非法」标成 0，这里再判一遍就是两套逻辑两个未来。
  WHERE `is_valid` = 1
  GROUP BY DATE(`event_time`), `event_type`
),
`ordered` AS (
  SELECT
    `dt`, `step`, `pv`, `uv`, `session_cnt`,
    CASE `step`
      WHEN 'page_view' THEN 1
      WHEN 'add_cart'  THEN 2
      WHEN 'checkout'  THEN 3
      WHEN 'pay'       THEN 4
      ELSE 9
    END AS `step_order`
  FROM `daily`
)
SELECT
  o.`dt`,
  o.`step`,
  o.`step_order`,
  o.`pv`,
  o.`uv`,
  o.`session_cnt`,

  -- 单步转化率：本环节 UV / 上一环节 UV
  CAST(o.`uv` AS DECIMAL(20,6))
    / NULLIF(LAG(o.`uv`) OVER (PARTITION BY o.`dt` ORDER BY o.`step_order`), 0) AS `conv_rate_vs_prev`,
  -- 全程转化率：本环节 UV / page_view UV
  CAST(o.`uv` AS DECIMAL(20,6)) / NULLIF(f.`uv`, 0)                            AS `conv_rate_vs_first`,

  NOW() AS `update_time`
FROM `ordered` o
LEFT JOIN (
  SELECT `dt`, `uv` FROM `ordered` WHERE `step_order` = 1
) f ON o.`dt` = f.`dt`;


-- -----------------------------------------------------------------------------
-- 4. ads_payment_method_1d —— 支付方式分布 + 占比
-- -----------------------------------------------------------------------------
-- `avg_installments` 为 NULL 是正常的：not_defined 这类没有分期信息的支付方式，
-- sum_installments 为 0 时除法结果是 0，但若 pay_cnt 也是 0 则为 NULL。
-- 前端遇到 NULL 应显示 "—"，不要显示 0 —— 0 期分期是个不存在的东西。
INSERT OVERWRITE TABLE `ads_payment_method_1d`
SELECT
  `dt`,
  `payment_type`,

  ROW_NUMBER() OVER (PARTITION BY `dt` ORDER BY `pay_amount` DESC NULLS LAST) AS `rank_no`,
  `pay_cnt`,
  `pay_amount`,
  `order_cnt`,
  CAST(`sum_installments` AS DECIMAL(20,6)) / NULLIF(`pay_cnt`, 0) AS `avg_installments`,

  CAST(`pay_amount` AS DECIMAL(20,6))
    / NULLIF(SUM(`pay_amount`) OVER (PARTITION BY `dt`), 0) AS `amount_ratio`,
  CAST(`pay_cnt` AS DECIMAL(20,6))
    / NULLIF(SUM(`pay_cnt`) OVER (PARTITION BY `dt`), 0)    AS `cnt_ratio`,

  NOW() AS `update_time`
FROM `dws_payment_method_1d`
WHERE `pay_amount` IS NOT NULL;


-- -----------------------------------------------------------------------------
-- 5. ads_order_stream —— 订单粒度流水
-- -----------------------------------------------------------------------------
-- 从订单行粒度收敛到订单粒度。每个字段的聚合方式都不同，逐条说明：
--   MAX(dt) / MAX(order_time)  订单内所有行的下单时间相同，MAX 取任意一个
--   MAX(order_status)          同一订单状态一致；MAX 只是为了取到非 NULL
--   MAX(payment_total)         宽表里这列已经是订单级预聚合，每行都相同
--   MAX(delivery_days)         不同卖家发货速度不同，取最慢的那行 = 整单履约时长
--   MAX(is_late)               **任一行迟到即整单迟到**。取 MIN 会得出
--                              「5 个商品行里 4 个迟到，这单算准时」的荒谬结论。
--                              全是 NULL（未妥投）时 MAX 仍是 NULL，语义正确。
INSERT OVERWRITE TABLE `ads_order_stream`
SELECT
  `order_id`,
  MAX(`dt`)                      AS `dt`,
  MAX(`order_purchase_timestamp`) AS `order_time`,
  MAX(`order_status`)            AS `order_status`,

  MAX(`customer_unique_id`)      AS `customer_unique_id`,
  MAX(`customer_state`)          AS `customer_state`,
  MAX(`customer_city`)           AS `customer_city`,

  COUNT(*)                       AS `item_cnt`,
  CAST(SUM(`item_total`) AS DECIMAL(20,2)) AS `gmv`,
  CAST(MAX(`payment_total`) AS DECIMAL(20,2)) AS `payment_total`,
  MAX(`delivery_days`)           AS `delivery_days`,
  MAX(`is_late`)                 AS `is_late`,

  NOW() AS `update_time`
FROM `dwd_trade_order_detail`
GROUP BY `order_id`;


-- -----------------------------------------------------------------------------
-- 6. ads_region_map_1d —— 地域地图 + 州名 + 州中心坐标
-- -----------------------------------------------------------------------------
-- 两个 LEFT JOIN 都是必需的，且都不能写成 INNER JOIN：
--
--   * 州名映射   27 行常量。写 INNER 的话，一旦某个州漏在映射表里，
--                它整天的数据会从地图上凭空消失 —— 而地图看起来只是少一块色，
--                没人会怀疑到 SQL 上。
--   * 州中心坐标 从 dim_geo 现算。写 INNER 的后果同上。
--
-- 两个 JOIN 都用 LEFT，缺失时留 NULL。大屏渲染遇到 NULL 会显示成"无数据"，
-- 比"整块消失"更容易被发现。
--
-- 州名用内联常量表而不是再建一张维表：27 个值是稳定的行政区划，不会变；
-- 为它多一张 DWD 维表要改分层模型、要加装载脚本，收益为零。
--
-- 坐标那段的 `BETWEEN -34 AND 6 AND lng BETWEEN -74 AND -34` 不是随手写的数字，
-- 是巴西的经纬度边界。dim_geo 里有 11 个越界点（PA 的纬度跑到北纬 22 度、
-- PR 的经纬度串了位），不过滤的话这几州的中心点会被拉到海里。详见 07-ads.sql
-- 第 7 张表的注释。
INSERT OVERWRITE TABLE `ads_region_map_1d`
SELECT
  r.`dt`,
  r.`customer_state`,
  m.`state_name`,

  CAST(r.`gmv` AS DECIMAL(20,2))                            AS `gmv`,
  CAST(r.`freight_total` AS DECIMAL(20,2))                  AS `freight_total`,
  r.`item_cnt`,
  r.`order_cnt`,
  r.`buyer_cnt`,

  CAST(r.`gmv` AS DECIMAL(20,6)) / NULLIF(r.`order_cnt`, 0) AS `aov`,
  CAST(r.`freight_total` AS DECIMAL(20,6)) / NULLIF(r.`gmv`, 0) AS `freight_ratio`,

  CAST(r.`gmv` AS DECIMAL(20,6))
    / NULLIF(SUM(r.`gmv`) OVER (PARTITION BY r.`dt`), 0)    AS `gmv_ratio`,
  ROW_NUMBER() OVER (PARTITION BY r.`dt` ORDER BY r.`gmv` DESC NULLS LAST) AS `gmv_rank`,

  CAST(g.`c_lat` AS DECIMAL(10,6))                          AS `center_lat`,
  CAST(g.`c_lng` AS DECIMAL(10,6))                          AS `center_lng`,

  NOW() AS `update_time`
FROM `dws_trade_region_1d` r
LEFT JOIN (
            SELECT 'AC' AS `sc`, 'Acre'               AS `state_name`
  UNION ALL SELECT 'AL', 'Alagoas'
  UNION ALL SELECT 'AP', 'Amapá'
  UNION ALL SELECT 'AM', 'Amazonas'
  UNION ALL SELECT 'BA', 'Bahia'
  UNION ALL SELECT 'CE', 'Ceará'
  UNION ALL SELECT 'DF', 'Distrito Federal'
  UNION ALL SELECT 'ES', 'Espírito Santo'
  UNION ALL SELECT 'GO', 'Goiás'
  UNION ALL SELECT 'MA', 'Maranhão'
  UNION ALL SELECT 'MT', 'Mato Grosso'
  UNION ALL SELECT 'MS', 'Mato Grosso do Sul'
  UNION ALL SELECT 'MG', 'Minas Gerais'
  UNION ALL SELECT 'PA', 'Pará'
  UNION ALL SELECT 'PB', 'Paraíba'
  UNION ALL SELECT 'PR', 'Paraná'
  UNION ALL SELECT 'PE', 'Pernambuco'
  UNION ALL SELECT 'PI', 'Piauí'
  UNION ALL SELECT 'RJ', 'Rio de Janeiro'
  UNION ALL SELECT 'RN', 'Rio Grande do Norte'
  UNION ALL SELECT 'RS', 'Rio Grande do Sul'
  UNION ALL SELECT 'RO', 'Rondônia'
  UNION ALL SELECT 'RR', 'Roraima'
  UNION ALL SELECT 'SC', 'Santa Catarina'
  UNION ALL SELECT 'SP', 'São Paulo'
  UNION ALL SELECT 'SE', 'Sergipe'
  UNION ALL SELECT 'TO', 'Tocantins'
) m ON r.`customer_state` = m.`sc`
LEFT JOIN (
  -- 按州收敛成一个中心点：先滤掉越界点，再按 point_cnt 加权平均。
  -- 分母为 0（整州的点全越界）时返回 NULL，而不是让整个 INSERT 抛除零错误。
  SELECT `state`,
         SUM(CASE WHEN `lat` BETWEEN -34 AND 6 AND `lng` BETWEEN -74 AND -34
                  THEN `lat` * `point_cnt` ELSE 0 END)
           / NULLIF(SUM(CASE WHEN `lat` BETWEEN -34 AND 6 AND `lng` BETWEEN -74 AND -34
                             THEN `point_cnt` ELSE 0 END), 0) AS `c_lat`,
         SUM(CASE WHEN `lat` BETWEEN -34 AND 6 AND `lng` BETWEEN -74 AND -34
                  THEN `lng` * `point_cnt` ELSE 0 END)
           / NULLIF(SUM(CASE WHEN `lat` BETWEEN -34 AND 6 AND `lng` BETWEEN -74 AND -34
                             THEN `point_cnt` ELSE 0 END), 0) AS `c_lng`
  FROM `dim_geo`
  GROUP BY `state`
) g ON r.`customer_state` = g.`state`
WHERE r.`gmv` IS NOT NULL;
