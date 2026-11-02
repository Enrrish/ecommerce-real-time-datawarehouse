-- =============================================================================
-- 09-dws-event-payment.sql —— 小时级转化漏斗 + 支付方式日汇总
-- -----------------------------------------------------------------------------
-- 数据源：Kafka user_event（漏斗）+ MySQL order_payments / orders（支付方式）
-- 目标：realtime_dw.dws_traffic_funnel_1h / dws_payment_method_1d
-- server-id：5605-5612
--
-- =============================================================================
-- 为什么这两张表能放一起，而 trade 那几张不行
-- =============================================================================
-- 这两条链的源完全不同（一个 Kafka、一个 CDC），本来就没有公共子计划可复用，
-- 不存在 07 那个「复用失败导致状态翻倍」的问题。而各自的量都很小：
--
--   漏斗：16 个 key（2 个事件日 × 2 个小时段 × 4 个环节）
--   支付：2,276 个 key（616 天 × 5 种支付方式）
--   join 状态：支付链的 orders 只取 2 列、payments 只取 5 列，合计约 20MB
--
-- 合计 25MB 上下，塞进 217MB 的 slot 毫无压力。分开反而白占一个 slot。
--
-- =============================================================================
-- 支付方式这张表的口径与另外 4 张 trade 表不同，必须单独说明
-- =============================================================================
-- 它从 order_payments **原始流水粒度**聚合，不经过 DWD 宽表。原因是 DWD 刻意
-- 没存 payment_type —— 支付方式是订单级的一对多属性（一单可能「信用卡 + 优惠券」），
-- 塞进订单行粒度必然要挑一个，那就是近似值。这里一笔支付算一笔，是精确的。
--
-- 后果：本表 order_cnt 是「有支付记录的订单数」= 99,440，而 trade 系列是
-- 「有商品行的订单数」= 98,666。两个数都对，只是问题不同。**不要相减算差额**，
-- 那个差里混着 775 个无明细订单和 1 个无支付记录订单，不是净误差。
-- 金额同理：本表 pay_amount 合计 16,008,872.12，trade 系列 GMV 合计
-- 15,843,553.24，差额 165,318.88 主要来自无明细订单的支付和 Olist 原始数据里
-- 「支付金额 ≠ 商品金额 + 运费」的天然偏差。
-- =============================================================================

SET 'execution.attached' = 'false';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'state.backend' = 'hashmap';


-- -----------------------------------------------------------------------------
-- Source 1/3：Kafka user_event（漏斗）
-- -----------------------------------------------------------------------------
-- group.id 与 04 / 06 号作业都不同：三个作业读同一个 topic，属于三个独立消费者组，
-- 各记各的位点。这也是 DWS 能从 Kafka 直接取数、不必等 DWD 落库的原因 ——
-- 埋点这条链路的分层靠「同一份源被多个作业独立消费」实现，不是靠逐层传递。
CREATE TABLE src_user_event (
  schema_version INT,
  event_id       STRING,
  event_time     STRING,
  event_type     STRING,
  user_id        STRING,
  session_id     STRING,
  product_id     STRING,
  page_url       STRING,

  -- 解析后的时间做成计算列，供下游直接引用（省得每处都写一遍 CASE）。
  -- 这里**不加 WATERMARK** —— 没有 OVER 聚合就不再需要时间属性，
  -- 而水位线恰恰是"尾部数据出不来"的来源。普通计算列没有这个问题。
  evt_time AS (
    CASE
      WHEN event_time LIKE '____-__-__ __:__:__.___'
        THEN TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss.SSS')
      ELSE CAST(NULL AS TIMESTAMP(3))
    END
  ),

  -- 处理时间属性，只用于下面的去重排序。PROCTIME() 是 Flink 的元数据列，
  -- 它永远不会落后于最新数据，所以不存在水位线滞尾的问题 —— 这是它和
  -- event_time + WATERMARK 的关键区别。
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic'     = 'user_event',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-dws-traffic-funnel',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false'
);


-- -----------------------------------------------------------------------------
-- Source 2/3：order_payments（支付流水原始粒度）
-- -----------------------------------------------------------------------------
CREATE TABLE src_order_payments (
  `order_id`             STRING,
  `payment_sequential`   INT,
  `payment_type`         STRING,
  `payment_installments` INT,
  `payment_value`        DECIMAL(18,2),
  PRIMARY KEY (`order_id`, `payment_sequential`) NOT ENFORCED
) WITH (
  'connector'                    = 'mysql-cdc',
  'hostname'                     = 'mysql',
  'port'                         = '3306',
  'username'                     = 'root',
  'password'                     = 'root123',
  'database-name'                = 'olist',
  'table-name'                   = 'order_payments',
  'server-id'                    = '5605-5608',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 3/3：orders（只取时间，给支付流水补日期）
-- -----------------------------------------------------------------------------
-- payments 表本身没有时间列，dt 必须 join orders 取 order_purchase_timestamp。
-- 也可以退一步用装载日期，但那样 616 天历史会全落进今天，口径就废了。
CREATE TABLE src_orders (
  `order_id`                      STRING,
  `order_purchase_timestamp`      TIMESTAMP(3),
  PRIMARY KEY (`order_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'mysql-cdc',
  'hostname'                     = 'mysql',
  'port'                         = '3306',
  'username'                     = 'root',
  'password'                     = 'root123',
  'database-name'                = 'olist',
  'table-name'                   = 'orders',
  'server-id'                    = '5609-5612',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- 埋点去重视图 —— 漏斗不读原始流，读这个
-- -----------------------------------------------------------------------------
-- 实测差异：直接从 Kafka 聚合，漏斗 pv 合计 22,064；而 DWD 表只有 22,004 行。
-- 差的 60 条是生成器 `--dup-rate` 注入的**重复 event_id**（scripts/gen_user_events.py
-- 第 234 行 rng.sample 复制）。DWD 靠 UNIQUE 主键天然去重，DWS 直连 Kafka
-- 就没有这层保护了 —— 不去重的话 pv 会虚高 0.27%，而且 P6 要注入 3% 重复，
-- 届时虚高会变成 3%，漏斗转化率全线失真。
--
-- 去重的标准写法是 ROW_NUMBER。流模式下 Flink 强制 ORDER BY 落在**时间属性**上，
-- 而 event_time 加 WATERMARK 会引入"尾部数据出不来"的老问题（P4 实测 22,064 条
-- 只落 21,996 条）。用 PROCTIME() 绕开：它是 Flink 自己维护的处理时间，
-- 永远不落后于最新数据，不存在等水位线的问题。
--
-- 语义上也是对的：同一个 event_id 来了两次，取**先到的那条**，后到的丢弃。
-- 状态是 22,004 个 event_id，几 MB，可以忽略。
CREATE TEMPORARY VIEW v_event_dedup AS
SELECT `event_id`, `evt_time`, `event_type`, `user_id`, `session_id`
FROM (
  SELECT
    `event_id`, `evt_time`, `event_type`, `user_id`, `session_id`,
    ROW_NUMBER() OVER (PARTITION BY `event_id` ORDER BY `proc_time` ASC) AS `rn`
  FROM `src_user_event`
) t
WHERE t.`rn` = 1;


-- -----------------------------------------------------------------------------
-- Sink 1/2：dws_traffic_funnel_1h
-- -----------------------------------------------------------------------------
CREATE TABLE sink_dws_traffic_funnel_1h (
  `dt`           DATE,
  `hour`         INT,
  `step`         STRING,
  `pv`           BIGINT,
  `uv`           BIGINT,
  `session_cnt`  BIGINT,
  `update_time`  TIMESTAMP(3)
) WITH (
  'connector'              = 'doris',
  'fenodes'                = 'doris-fe:8030',
  'table.identifier'       = 'realtime_dw.dws_traffic_funnel_1h',
  'username'               = 'root',
  'password'               = '',
  'sink.label-prefix'      = 'dws_traffic_funnel_1h',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete'     = 'true',
  'sink.buffer-size'       = '1048576',
  'sink.buffer-count'      = '32',
  'sink.buffer-flush.max-bytes' = '10485760',
  'sink.write-mode'        = 'STREAM_LOAD_BATCH'
);


-- -----------------------------------------------------------------------------
-- Sink 2/2：dws_payment_method_1d
-- -----------------------------------------------------------------------------
CREATE TABLE sink_dws_payment_method_1d (
  `dt`               DATE,
  `payment_type`     STRING,
  `pay_cnt`          BIGINT,
  `pay_amount`       DECIMAL(20,2),
  `order_cnt`        BIGINT,
  `sum_installments` BIGINT,
  `update_time`      TIMESTAMP(3)
) WITH (
  'connector'              = 'doris',
  'fenodes'                = 'doris-fe:8030',
  'table.identifier'       = 'realtime_dw.dws_payment_method_1d',
  'username'               = 'root',
  'password'               = '',
  'sink.label-prefix'      = 'dws_payment_method_1d',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete'     = 'true',
  'sink.buffer-size'       = '1048576',
  'sink.buffer-count'      = '32',
  'sink.buffer-flush.max-bytes' = '10485760',
  'sink.write-mode'        = 'STREAM_LOAD_BATCH'
);


-- -----------------------------------------------------------------------------
-- 装载
-- -----------------------------------------------------------------------------
-- 漏斗的过滤条件与 DWD 的 is_valid 保持同一套判断，但这里直接 WHERE 掉而不是
-- 留标记列 —— DWS 是聚合结果，留着无效行会让 UV 把「无 user_id 的事件」当成
-- 一个用户（NULL 会被 COUNT(DISTINCT) 丢掉，但空字符串不会，所以必须显式 TRIM）。
--
-- HOUR() 在 Flink 里返回 **BIGINT** 而不是 INT（和 MySQL 不同）。直接写会让
-- 提交阶段报 "Incompatible types for sink column 'hour' at position 1"，
-- 而且报错只给类型不给建议。必须显式 CAST 成 INT。
EXECUTE STATEMENT SET
BEGIN

INSERT INTO sink_dws_traffic_funnel_1h
SELECT
  CAST(`evt_time` AS DATE)                               AS `dt`,
  CAST(HOUR(`evt_time`) AS INT)                          AS `hour`,
  `event_type`                                           AS `step`,
  COUNT(*)                                               AS `pv`,
  COUNT(DISTINCT `user_id`)                              AS `uv`,
  COUNT(DISTINCT `session_id`)                           AS `session_cnt`,
  LOCALTIMESTAMP                                         AS `update_time`
FROM `v_event_dedup`
WHERE `evt_time` IS NOT NULL
  AND `user_id` IS NOT NULL AND TRIM(`user_id`) <> ''
  AND `session_id` IS NOT NULL AND TRIM(`session_id`) <> ''
  AND `event_type` IN ('page_view', 'add_cart', 'checkout', 'pay')
GROUP BY CAST(`evt_time` AS DATE), HOUR(`evt_time`), `event_type`;

INSERT INTO sink_dws_payment_method_1d
SELECT
  CAST(o.`order_purchase_timestamp` AS DATE)             AS `dt`,
  p.`payment_type`,
  COUNT(*)                                               AS `pay_cnt`,
  CAST(SUM(p.`payment_value`) AS DECIMAL(20,2))          AS `pay_amount`,
  COUNT(DISTINCT p.`order_id`)                           AS `order_cnt`,
  CAST(SUM(p.`payment_installments`) AS BIGINT)          AS `sum_installments`,
  LOCALTIMESTAMP                                         AS `update_time`
FROM `src_order_payments` p
JOIN `src_orders` o
  ON p.`order_id` = o.`order_id`
GROUP BY CAST(o.`order_purchase_timestamp` AS DATE), p.`payment_type`;

END;
