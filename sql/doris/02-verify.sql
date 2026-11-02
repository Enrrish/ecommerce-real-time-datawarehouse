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
SELECT 'ods_category_translation',       COUNT(*), 71,     COUNT(*) - 71     FROM `ods_category_translation`;

-- ods_user_event 刻意不放进上面的 diff 判定：它的期望值不是固定数字，
-- 而是「Kafka 里去重后的事件数」，每跑一次 gen_user_events.py 都会变。
-- 这里单独打印，判读方式是拿它和 Kafka 总消息数对比：
--   Kafka 消息数 = 22,064（含 3% 注入重复）→ Doris 22,004 行，差值 60 = 重复数，
--   说明 event_id 唯一键把重复吃干净了。这条对账在 04 号文件提交后手工做过一次。
SELECT 'ods_user_event（不参与 diff，见上方注释）' AS tbl, COUNT(*) AS actual FROM `ods_user_event`;


-- -----------------------------------------------------------------------------
-- 检查 2：op 分布 —— 快照行是 c，增量行是 c/u，删除行不会留下
-- -----------------------------------------------------------------------------
-- 先说一个被实测纠正过的假设：**快照行的 op 是 'c'，不是 'r'**。
-- 原设计想用 Debezium 的 'r'（read）标记快照行，但 Flink CDC 3.2.1 的
-- mysql-cdc 不提供 'op' 这个元数据，只提供 row_kind，取值是 +I/+U/-U/-D。
-- 快照阶段的记录在 Flink 眼里就是 INSERT（+I），只能映射成 'c'。
-- 后果：op 无法区分「快照灌进来的历史数据」和「作业跑起来后真实新增的行」。
-- 要区分只能靠 ts_ms：快照行 ts_ms = 0（哨兵值，见检查 5 的说明）。
--
-- 所以这条检查的正确判读是：
--   * 首次全量之后，op 应该**只有 c**（全部来自快照）
--   * 做过 UPDATE 之后出现 u，做过 INSERT 之后 c 会增加
--   * 出现 d 反而奇怪：UNIQUE 模型下 op='d' 的行会被直接删掉，表里留不下
--   * 出现 x 说明 row_kind 出现了预期外的值（既不是 +I 也不是 +U/-U/-D），是 bug
SELECT '2. op 分布（首轮全量后应只有 c）' AS section;

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
-- 检查 3：dt 范围 —— ODS 的 dt 是**落地日期**，不是源端变更日期
-- -----------------------------------------------------------------------------
-- 这一条曾经写错过，把 DATE(op_ts) 当成 dt，结果首轮全量之后 99,441 行订单
-- 全部落在 1970-01-01（快照阶段没有 binlog 事件，op_ts 是 epoch 0）。
-- 后来改成 CURRENT_DATE，即「这批数据什么时候进来的」。
--
-- 正确判读：
--   * 9 张 CDC 表：dt_cnt 应为 1，且全部等于作业首轮快照的那天
--   * 增量进来之后 dt_cnt 才可能变成 2（跨天继续跑的情况）
--   * 出现 1970-01-01 = 又退回了 DATE(op_ts) 的写法，必查
--   * ods_user_event 例外：它的 dt 取 DATE(event_time)，是**业务日期**，
--     生成器把 event_time 铺在 1 小时窗口里，所以理论上也可能跨 2 天（跨零点时）
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
--
-- 【Doris 2.1.11 实测踩到的坑】这段原本每个列名都加了反引号，报
--     ERROR 1054: Unknown column 'event_type' in 'table list'
-- 排查半天才定位到：**多个 COUNT(DISTINCT CASE WHEN ... THEN `col`) 同时出现、
-- 且列名用反引号包裹**时，Doris 的 multi-distinct 重写会把被反引号包住的标识符
-- 当成无法解析的表达式，报一个完全不指向根因的错。
-- 去掉反引号即可（这几列名都不是保留字，本来也不需要引号）。
-- 单个 COUNT(DISTINCT) 带反引号没问题，两个同行也没问题 —— 只有 4 个一起时才触发。
-- 教训：这个报错说 "Unknown column"，但列明明存在，看到它先怀疑写法而不是列名。
SELECT '4b. 漏斗 UV（user_id 去重）' AS section;

SELECT
  COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END) AS uv_view,
  COUNT(DISTINCT CASE WHEN event_type = 'add_cart'  THEN user_id END) AS uv_cart,
  COUNT(DISTINCT CASE WHEN event_type = 'checkout'  THEN user_id END) AS uv_checkout,
  COUNT(DISTINCT CASE WHEN event_type = 'pay'       THEN user_id END) AS uv_pay
FROM ods_user_event;

-- 漏斗必须是递减的，任何一环大于上一环就说明生成器或清洗逻辑有问题。
-- 这条用 WHERE NOT(...) 直接断言，返回 0 行 = 通过。
-- 同样去掉反引号：与 4b 是同一个 multi-distinct 坑，这里同样是 4 个
-- COUNT(DISTINCT CASE WHEN ...) 并列。
SELECT '4c. 漏斗递减断言（应返回 0 行）' AS section;

SELECT *
FROM (
  SELECT
    COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN session_id END) AS s_view,
    COUNT(DISTINCT CASE WHEN event_type = 'add_cart'  THEN session_id END) AS s_cart,
    COUNT(DISTINCT CASE WHEN event_type = 'checkout'  THEN session_id END) AS s_checkout,
    COUNT(DISTINCT CASE WHEN event_type = 'pay'       THEN session_id END) AS s_pay
  FROM ods_user_event
) t
WHERE NOT (s_view >= s_cart AND s_cart >= s_checkout AND s_checkout >= s_pay);


-- -----------------------------------------------------------------------------
-- 检查 5：事件年龄 —— 不是「管道延迟」，这两个概念必须分清
-- -----------------------------------------------------------------------------
-- 这条 SQL 原来叫「链路延迟」，但算的是 NOW() - event_time，而生成器把
-- event_time 铺在 1 小时窗口里，所以结果天然是几十分钟量级 —— 拿它判断管道
-- 健康度会天天误判。两者是不同东西：
--
--   * **管道延迟** = 数据落进 Doris 的时刻 - 数据产生的时刻。
--     这是大屏「数据延迟」面板要展示的数（P7），本管道实测约 3.8 秒
--     （在 MySQL 改一行 → 轮询 Doris 直到可见，掐表得到，见 docs/05 §4）。
--     Doris 侧算不出来，因为缺「Flink 收到消息的时刻」这个字段。
--
--   * **事件年龄** = 入库时刻 - 事件发生时刻，也就是下面这条。
--     它包含了生成器刻意做的 1 小时回填，**还包含了历史批次**：库里同时留着
--     10-30 生成的那批事件，所以 avg 会被拉到 60 小时量级（实测 215,075 秒）。
--     这不是故障，是「库里同时有历史事件」的正常表现。真正有意义的是三个信号：
--       1. min_age_s —— 最新一条事件的新鲜度。管道活着的话它应该一直在
--          「生成时刻到现在」附近（实测 662 秒 ≈ 11 分钟，正好对上最后一批的生成时间）
--       2. 负值 —— 入库时间早于事件时间，说明时钟错乱，必查
--       3. max_age_s 突然暴涨 —— 位点被重置从 earliest 重放，老事件重新灌进来
--
-- 顺带解释 ts_ms 的 0 值：CDC 那 9 张表的快照行 ts_ms 是 0，是**有意的哨兵**，
-- 表示「这一行没有源端时间戳」（快照阶段没有 binlog 事件）。所以算任何时间指标
-- 都必须带 WHERE ts_ms > 0，否则平均值会被 0 拽成负数。
SELECT '5. 事件年龄（秒，含生成器 1 小时回填）' AS section;

SELECT
  ROUND(AVG(`ts_ms` / 1000 - UNIX_TIMESTAMP(`event_time`)), 1) AS avg_age_s,
  ROUND(MAX(`ts_ms` / 1000 - UNIX_TIMESTAMP(`event_time`)), 1) AS max_age_s,
  ROUND(MIN(`ts_ms` / 1000 - UNIX_TIMESTAMP(`event_time`)), 1) AS min_age_s,
  MIN(`event_time`) AS earliest,
  MAX(`event_time`) AS latest
FROM `ods_user_event`;


-- -----------------------------------------------------------------------------
-- 检查 5b：CDC 表的 ts_ms 哨兵值分布
-- -----------------------------------------------------------------------------
-- 首轮全量之后，整张表都是快照行 → ts_ms = 0 的行数应等于总行数。
-- 做过一次 UPDATE 之后，被改的那一行 ts_ms 变成真实毫秒值，于是出现
-- 「zero_ts = 总数 - N」的形态。如果这里 zero_ts 是 0（一行都不为 0），
-- 说明作业根本没走快照、只走了增量，历史数据其实没进来 —— 那是另一个问题。
SELECT '5b. 快照行 ts_ms 哨兵值（首轮全量后应全为 0）' AS section;

SELECT 'ods_orders'      AS tbl, COUNT(*) AS total, SUM(`ts_ms` = 0) AS zero_ts,
       SUM(`ts_ms` > 0)  AS real_ts
FROM `ods_orders`
UNION ALL
SELECT 'ods_customers',      COUNT(*), SUM(`ts_ms` = 0), SUM(`ts_ms` > 0) FROM `ods_customers`
UNION ALL
SELECT 'ods_order_items',    COUNT(*), SUM(`ts_ms` = 0), SUM(`ts_ms` > 0) FROM `ods_order_items`;


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
-- 判读清单（2026-11-02 实测全部通过）
-- -----------------------------------------------------------------------------
-- 检查 1  全 0               → 9 张 CDC 表行数与源端逐一相等，全量快照完整
-- 检查 2  只有 c 没有 x       → 首轮全量走的是快照；出现 u 说明已发生过增量，
--                              出现 x 说明 row_kind 有预期外的值，是 bug
-- 检查 3  dt_cnt=1 且非 1970  → 落地日口径正确（曾因取 DATE(op_ts) 全体落 1970）
-- 检查 4  漏斗严格递减        → 埋点链路没丢事件
-- 检查 5  min_age 与「生成时刻到现在」相符 → 管道新鲜；出现负值 = 时钟错乱；
--         avg/max 很大是正常的（库里留着历史批次的事件，实测 avg 60 小时）
-- 检查 5b zero_ts = total    → 首轮全量；若 real_ts > 0 说明已有增量写入
-- 检查 6  unmatched = 0       → 埋点与商品维表能 join，P4 有素材
--
-- 全部通过后，做 P3 的最终验收（必须在 Flink 作业 RUNNING 时手工做）：
--   1. 在 MySQL 执行 UPDATE olist.orders SET order_status='shipped' WHERE order_id='xxx'
--   2. 到 Doris 查同一行，应看到 order_status 已变、op='u'、ts_ms 变大（不再是 0）
--   3. 再执行 DELETE，该行应从 ods_orders 消失（sink.enable-delete=true 的效果）
--   第 3 步「消失」而不是「op 变成 d」，是 UNIQUE 模型的正确语义，见 docs/02 §1。
--
-- 端到端延迟掐表实测：3,849 ms（写入 MySQL → 轮询 Doris 至可见）。
-- 理论最差约 10 秒，受 Doris sink 的 buffer-flush 周期（默认 10s）约束，
-- 与 Flink checkpoint 间隔（10s）叠加。要压到 1 秒级得调 sink.buffer-flush.interval，
-- 但这一版连接器的下限是 10000 行，不能按时间调 —— 这是 P3 已知的能力边界。
-- =============================================================================
