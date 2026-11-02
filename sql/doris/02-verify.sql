-- =============================================================================
-- 02-verify.sql — P3（ODS 入湖）验收
-- -----------------------------------------------------------------------------
-- 执行：mysql -h127.0.0.1 -P9030 -uroot --table --default-character-set=utf8mb4 < 02-verify.sql
-- 时机：10 个 Flink 作业全部 RUNNING，且首轮全量快照跑完之后
--
-- 判读原则：**所有 diff 列必须为 0**。任何非 0 都不是"差不多"，而是链路断了一段。
-- 每条 SQL 的期望值都写死在注释里，不用回头翻文档。
-- =============================================================================

USE `realtime_dw`;


-- -----------------------------------------------------------------------------
-- 检查 1：行数对齐（最重要的一条）
-- -----------------------------------------------------------------------------
-- 期望值来自 MySQL 侧实测（P2 验收通过的 1,550,922 行）。
-- diff 为负 = Doris 少了，通常是：快照没跑完 / sink 报错被吞 / 主键冲突覆盖
-- diff 为正 = Doris 多了，通常是：作业重放过且表不是 UNIQUE（本表全是 UNIQUE，不该出现）
SELECT '1. 行数对齐' AS section;

SELECT 'ods_orders'               AS tbl, COUNT(*) AS actual, 99441    AS expected, COUNT(*) - 99441    AS diff FROM `ods_orders`
UNION ALL
SELECT 'ods_order_items',                COUNT(*), 112650, COUNT(*) - 112650 FROM `ods_order_items`
UNION ALL
SELECT 'ods_order_payments',             COUNT(*), 103886, COUNT(*) - 103886 FROM `ods_order_payments`
UNION ALL
SELECT 'ods_order_reviews',              COUNT(*), 99224,  COUNT(*) - 99224  FROM `ods_order_reviews`
UNION ALL
SELECT 'ods_customers',                  COUNT(*), 99441,  COUNT(*) - 99441  FROM `ods_customers`
UNION ALL
SELECT 'ods_sellers',                    COUNT(*), 3095,   COUNT(*) - 3095   FROM `ods_sellers`
UNION ALL
SELECT 'ods_products',                   COUNT(*), 32951,  COUNT(*) - 32951  FROM `ods_products`
UNION ALL
SELECT 'ods_geolocation',                COUNT(*), 1000163, COUNT(*) - 1000163 FROM `ods_geolocation`
UNION ALL
SELECT 'ods_category_translation',       COUNT(*), 71,     COUNT(*) - 71     FROM `ods_category_translation`
UNION ALL
SELECT 'ods_user_event',                 COUNT(*), 20000,  COUNT(*) - 20000  FROM `ods_user_event`;


-- -----------------------------------------------------------------------------
-- 检查 2：op 分布 —— 快照行应该是 r，实时行应该是 c/u
-- -----------------------------------------------------------------------------
-- 全量快照阶段 Flink CDC 产出的 op 是 'r'（read）。
-- 如果这里 op 全是 NULL 或空串，说明元数据列没声明对（WITH 里缺 'op' METADATA）。
-- 如果 op 全是 'c'，说明走的是增量而不是快照，历史数据其实没进来。
SELECT '2. op 分布（快照阶段应全为 r）' AS section;

SELECT 'ods_orders'          AS tbl, `op`, COUNT(*) AS cnt FROM `ods_orders`          GROUP BY `op`
UNION ALL
SELECT 'ods_order_items',            `op`, COUNT(*) FROM `ods_order_items`          GROUP BY `op`
UNION ALL
SELECT 'ods_order_payments',         `op`, COUNT(*) FROM `ods_order_payments`       GROUP BY `op`
UNION ALL
SELECT 'ods_order_reviews',          `op`, COUNT(*) FROM `ods_order_reviews`        GROUP BY `op`
UNION ALL
SELECT 'ods_customers',              `op`, COUNT(*) FROM `ods_customers`            GROUP BY `op`
UNION ALL
SELECT 'ods_sellers',                `op`, COUNT(*) FROM `ods_sellers`              GROUP BY `op`
UNION ALL
SELECT 'ods_products',               `op`, COUNT(*) FROM `ods_products`             GROUP BY `op`
UNION ALL
SELECT 'ods_geolocation',            `op`, COUNT(*) FROM `ods_geolocation`          GROUP BY `op`
UNION ALL
SELECT 'ods_category_translation',   `op`, COUNT(*) FROM `ods_category_translation` GROUP BY `op`;


-- -----------------------------------------------------------------------------
-- 检查 3：dt 范围 —— ODS 的 dt 是变更日期，历史快照会全部落在快照当天
-- -----------------------------------------------------------------------------
SELECT '3. dt 范围（9 张 CDC 表应集中在快照当天）' AS section;

SELECT 'ods_orders'    AS tbl, MIN(`dt`) AS dt_min, MAX(`dt`) AS dt_max, COUNT(DISTINCT `dt`) AS dt_cnt FROM `ods_orders`
UNION ALL
SELECT 'ods_customers',         MIN(`dt`), MAX(`dt`), COUNT(DISTINCT `dt`) FROM `ods_customers`
UNION ALL
SELECT 'ods_geolocation',       MIN(`dt`), MAX(`dt`), COUNT(DISTINCT `dt`) FROM `ods_geolocation`
UNION ALL
SELECT 'ods_user_event',        MIN(`dt`), MAX(`dt`), COUNT(DISTINCT `dt`) FROM `ods_user_event`;


-- -----------------------------------------------------------------------------
-- 检查 4：埋点链路 —— 事件分布与漏斗
-- -----------------------------------------------------------------------------
-- 期望分布（P2 实测，2 万条 / seed 20261030）：
--   page_view ≈ 77.9% / add_cart ≈ 14.5% / checkout ≈ 5.0% / pay ≈ 2.6%
-- 若 pay 明显偏少，检查是否被 Flink 当迟到数据丢弃（watermark 设太大）。
SELECT '4. 埋点事件分布' AS section;

SELECT `event_type`, COUNT(*) AS cnt,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `ods_user_event`), 2) AS pct
FROM `ods_user_event`
GROUP BY `event_type`
ORDER BY cnt DESC;

-- 漏斗 UV（按人，不是按事件数）。四个环节应严格递减。
SELECT '4b. 漏斗 UV（user_id 去重）' AS section;

SELECT
  COUNT(DISTINCT CASE WHEN `event_type` = 'page_view' THEN `user_id` END) AS uv_view,
  COUNT(DISTINCT CASE WHEN `event_type` = 'add_cart'  THEN `user_id` END) AS uv_cart,
  COUNT(DISTINCT CASE WHEN `event_type` = 'checkout'  THEN `user_id` END) AS uv_checkout,
  COUNT(DISTINCT CASE WHEN `event_type` = 'pay'       THEN `user_id` END) AS uv_pay;

-- 漏斗必须是递减的，任何一环大于上一环就说明生成器或清洗逻辑有问题。
-- 这条用 HAVING 直接断言，返回 0 行 = 通过。
SELECT '4c. 漏斗递减断言（应返回 0 行）' AS section;

SELECT *
FROM (
  SELECT
    COUNT(DISTINCT CASE WHEN `event_type` = 'page_view' THEN `session_id` END) AS s_view,
    COUNT(DISTINCT CASE WHEN `event_type` = 'add_cart'  THEN `session_id` END) AS s_cart,
    COUNT(DISTINCT CASE WHEN `event_type` = 'checkout'  THEN `session_id` END) AS s_checkout,
    COUNT(DISTINCT CASE WHEN `event_type` = 'pay'       THEN `session_id` END) AS s_pay
  FROM `ods_user_event`
) t
WHERE NOT (s_view >= s_cart AND s_cart >= s_checkout AND s_checkout >= s_pay);


-- -----------------------------------------------------------------------------
-- 检查 5：链路延迟 —— 从事件产生到落进 Doris 用了多久
-- -----------------------------------------------------------------------------
-- 这个数字就是大屏「数据延迟」面板要展示的值（P7）。
-- 正常情况下应该在秒级；如果超过 60 秒，检查 checkpoint 间隔是否设得太大
-- （Doris Connector 靠 checkpoint 触发写入批次，interval 就是最大延迟）。
SELECT '5. 链路延迟（秒）' AS section;

SELECT
  ROUND(AVG(TIMESTAMPDIFF(SECOND, `event_time`, NOW())), 1) AS avg_lag_s,
  ROUND(MAX(TIMESTAMPDIFF(SECOND, `event_time`, NOW())), 1) AS max_lag_s,
  MIN(`event_time`) AS earliest,
  MAX(`event_time`) AS latest
FROM `ods_user_event`;


-- -----------------------------------------------------------------------------
-- 检查 6：业务自检 —— 和三张维表的 join 是否真的能连上
-- -----------------------------------------------------------------------------
-- 埋点里的 product_id 取自真实商品表，理论上 100% 能 join 上。
-- 若这里出现未匹配，说明生成器用了假 ID，P4 的品类漏斗会整块空掉。
SELECT '6. 埋点 product_id 能否 join 上商品维表' AS section;

SELECT
  COUNT(*) AS events_with_product,
  SUM(CASE WHEN p.`product_id` IS NULL THEN 1 ELSE 0 END) AS unmatched
FROM `ods_user_event` e
LEFT JOIN `ods_products` p ON e.`product_id` = p.`product_id`
WHERE e.`product_id` IS NOT NULL;

-- 类目翻译缺口：应恰好缺 2 个类目、影响 13 个商品。
-- 这不是缺陷，是源数据事实，DWD 必须 LEFT JOIN。这里断言它没变化。
SELECT '6b. 类目翻译缺口（应缺 2 个）' AS section;

SELECT COUNT(*) AS products_without_translation
FROM `ods_products` p
LEFT JOIN `ods_category_translation` t ON p.`product_category_name` = t.`product_category_name`
WHERE p.`product_category_name` IS NOT NULL AND t.`product_category_name` IS NULL;


-- =============================================================================
-- 判读清单
-- -----------------------------------------------------------------------------
-- 检查 1 全 0            → 10 张表行数与源端一致，全量快照完整
-- 检查 2 op='r' 占多数    → 快照走通了（不是只有增量）
-- 检查 3 dt 集中在当天    → 证明 ODS 的 dt 取的是变更日期，符合设计
-- 检查 4 漏斗递减         → 埋点链路没丢数据
-- 检查 5 avg_lag < 60s    → checkpoint 间隔合理
-- 检查 6 unmatched = 0    → 埋点与维表能 join，P4 有素材
--
-- 全部通过后，做 P3 的最终验收（这个必须在 Flink 作业 RUNNING 时手工做）：
--   1. 在 MySQL 执行 UPDATE olist.orders SET order_status='shipped' WHERE order_id='xxx'
--   2. 5 秒内到 Doris 查同一行，应看到 order_status 已变、op='u'、ts_ms 变大
--   3. 再执行 DELETE，该行应从 ods_orders 消失（sink.enable-delete=true 的效果）
--   第 3 步"消失"而不是"op 变成 d"，是 UNIQUE 模型的正确语义，见 docs/02 §1 的说明。
-- =============================================================================
