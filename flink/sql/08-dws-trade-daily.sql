-- =============================================================================
-- 08-dws-trade-daily.sql —— 3 张日粒度交易汇总（类目 / 卖家 / 地域）
-- -----------------------------------------------------------------------------
-- 数据源：orders / order_items / customers / products / sellers / category_translation
-- 目标：realtime_dw 下 dws_trade_category_1d / dws_trade_seller_1d / dws_trade_region_1d
-- server-id：5581-5604
--
-- =============================================================================
-- 为什么这 3 张能放一个作业，而分钟表要单独一个
-- =============================================================================
-- 07 的实测结论是「4 张表放一个作业会因 join 子计划不复用而把状态乘 4，420MB
-- 直接 OOM」。这里敢放 3 张，是因为每张表都用**最小的 join**，单链状态估下来
-- 只有 40-55MB：
--
--   类目：orders(3 列 10MB) + items(6 列 22MB) + products(4MB) + sellers(1MB) ≈ 40MB
--   卖家：orders(5 列 15MB) + items(22MB) + sellers(1MB)                ≈ 52MB
--   地域：orders(3 列 10MB) + items(22MB) + customers(8MB)              ≈ 42MB
--   合计约 134MB，217MB 的 taskHeap 有余量。
--
-- 对比 07 那次失败的配置：每张表都 join 了全部 6 张维表，单链 105MB，四链 420MB。
-- 差别不在"表数量"，在"每张表有没有把用不到的维表 join 进来"。分钟表用不到
-- 类目、卖家、商品、州，硬 join 进公共视图就是纯浪费。
--
-- 另外记一个运维判据：状态真的超了的时候，TM 会抛
--   java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler
-- 然后进程退出、集群 slots-total 归 0；而 Flink UI 上看到的是
-- NoResourceAvailableException（招不到资源），真正的死因只在
-- `docker logs rt-flink-tm` 里。别被 UI 的异常栈带偏。
--
-- =============================================================================
-- 3 张表的 order_cnt 不可跨表相加
-- =============================================================================
-- 一笔订单买了 3 个类目的商品，会在 3 个类目行各计 1 次。实测：
--   SUM(order_cnt) over 类目 = 99,470
--   SUM(order_cnt) over 卖家 = 100,010
--   SUM(order_cnt) over 地域 = 98,666   ← 一单只对应一个买家州，恰好可加
--   COUNT(DISTINCT order_id) 全表 = 98,666
-- 前两个大于 98,666 不是 bug，是粒度的必然结果。第四个"恰好相等"也是巧合，
-- 不要当成规律。
-- =============================================================================

SET 'execution.attached' = 'false';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'state.backend' = 'hashmap';


-- -----------------------------------------------------------------------------
-- Source 1/6：orders
-- -----------------------------------------------------------------------------
CREATE TABLE src_orders (
  `order_id`                      STRING,
  `customer_id`                   STRING,
  `order_purchase_timestamp`      TIMESTAMP(3),
  `order_delivered_customer_date` TIMESTAMP(3),
  `order_estimated_delivery_date` TIMESTAMP(3),
  PRIMARY KEY (`order_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'mysql-cdc',
  'hostname'                     = 'mysql',
  'port'                         = '3306',
  'username'                     = 'root',
  'password'                     = 'root123',
  'database-name'                = 'olist',
  'table-name'                   = 'orders',
  'server-id'                    = '5581-5584',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 2/6：order_items
-- -----------------------------------------------------------------------------
CREATE TABLE src_order_items (
  `order_id`            STRING,
  `order_item_id`       INT,
  `product_id`          STRING,
  `seller_id`           STRING,
  `price`               DECIMAL(18,2),
  `freight_value`       DECIMAL(18,2),
  PRIMARY KEY (`order_id`, `order_item_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'mysql-cdc',
  'hostname'                     = 'mysql',
  'port'                         = '3306',
  'username'                     = 'root',
  'password'                     = 'root123',
  'database-name'                = 'olist',
  'table-name'                   = 'order_items',
  'server-id'                    = '5585-5588',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 3/6：customers
-- -----------------------------------------------------------------------------
CREATE TABLE src_customers (
  `customer_id`              STRING,
  `customer_unique_id`       STRING,
  `customer_state`           STRING,
  PRIMARY KEY (`customer_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'mysql-cdc',
  'hostname'                     = 'mysql',
  'port'                         = '3306',
  'username'                     = 'root',
  'password'                     = 'root123',
  'database-name'                = 'olist',
  'table-name'                   = 'customers',
  'server-id'                    = '5589-5592',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 4/6：products
-- -----------------------------------------------------------------------------
CREATE TABLE src_products (
  `product_id`                 STRING,
  `product_category_name`      STRING,
  PRIMARY KEY (`product_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'mysql-cdc',
  'hostname'                     = 'mysql',
  'port'                         = '3306',
  'username'                     = 'root',
  'password'                     = 'root123',
  'database-name'                = 'olist',
  'table-name'                   = 'products',
  'server-id'                    = '5593-5596',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 5/6：sellers
-- -----------------------------------------------------------------------------
CREATE TABLE src_sellers (
  `seller_id`       STRING,
  PRIMARY KEY (`seller_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'mysql-cdc',
  'hostname'                     = 'mysql',
  'port'                         = '3306',
  'username'                     = 'root',
  'password'                     = 'root123',
  'database-name'                = 'olist',
  'table-name'                   = 'sellers',
  'server-id'                    = '5597-5600',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 6/6：category_translation
-- -----------------------------------------------------------------------------
CREATE TABLE src_category_translation (
  `product_category_name`         STRING,
  `product_category_name_english` STRING,
  PRIMARY KEY (`product_category_name`) NOT ENFORCED
) WITH (
  'connector'                    = 'mysql-cdc',
  'hostname'                     = 'mysql',
  'port'                         = '3306',
  'username'                     = 'root',
  'password'                     = 'root123',
  'database-name'                = 'olist',
  'table-name'                   = 'category_translation',
  'server-id'                    = '5601-5604',
  'server-time-zone'             = 'Asia/Shanghai'
);


-- -----------------------------------------------------------------------------
-- 三张表共用的基础投影
-- -----------------------------------------------------------------------------
-- 三张表需要的列不同，所以下游各自的 join 无法完全共享 —— 但 source 扫描是共享的
-- （Flink 会复用 Source 顶点）。这就是"状态 134MB 而不是 315MB"的来源。
--
-- 类目必须 COALESCE 兜底：category_translation 只有 71 个类目、源表 73 个，
-- products 里还有 610 个商品类目为 NULL。明细表里同样的处理，否则会多出 NULL
-- 分组，类目行数从 18,990 变成 18,991 且 GMV 分布错位。
CREATE TEMPORARY VIEW v_order_base AS
SELECT
  oi.`order_id`,
  oi.`seller_id`,
  oi.`product_id`,
  CAST(o.`order_purchase_timestamp` AS DATE)                    AS `dt`,
  CAST(oi.`price` + oi.`freight_value` AS DECIMAL(18,2))        AS `item_total`,
  oi.`freight_value`,
  o.`customer_id`,
  CASE
    WHEN o.`order_delivered_customer_date` IS NULL THEN NULL
    WHEN o.`order_delivered_customer_date` > o.`order_estimated_delivery_date` THEN 1
    ELSE 0
  END                                                           AS `is_late`,
  CASE
    WHEN o.`order_delivered_customer_date` IS NULL THEN NULL
    ELSE TIMESTAMPDIFF(DAY, o.`order_purchase_timestamp`, o.`order_delivered_customer_date`)
  END                                                           AS `delivery_days`
FROM `src_order_items` oi
JOIN `src_orders` o
  ON oi.`order_id` = o.`order_id`;


-- -----------------------------------------------------------------------------
-- Sink 1/3：dws_trade_category_1d
-- -----------------------------------------------------------------------------
CREATE TABLE sink_dws_trade_category_1d (
  `dt`             DATE,
  `category_name`  STRING,
  `gmv`            DECIMAL(20,2),
  `freight_total`  DECIMAL(20,2),
  `item_cnt`       BIGINT,
  `order_cnt`      BIGINT,
  `seller_cnt`     BIGINT,
  `update_time`    TIMESTAMP(3)
) WITH (
  'connector'              = 'doris',
  'fenodes'                = 'doris-fe:8030',
  'table.identifier'       = 'realtime_dw.dws_trade_category_1d',
  'username'               = 'root',
  'password'               = '',
  'sink.label-prefix'      = 'dws_trade_category_1d',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete'     = 'true',
  'sink.buffer-size'       = '1048576',
  'sink.buffer-count'      = '32',
  'sink.buffer-flush.max-bytes' = '10485760',
  'sink.write-mode'        = 'STREAM_LOAD_BATCH'
);


-- -----------------------------------------------------------------------------
-- Sink 2/3：dws_trade_seller_1d
-- -----------------------------------------------------------------------------
CREATE TABLE sink_dws_trade_seller_1d (
  `dt`                DATE,
  `seller_id`         STRING,
  `gmv`               DECIMAL(20,2),
  `item_cnt`          BIGINT,
  `order_cnt`         BIGINT,
  `delivered_cnt`     BIGINT,
  `late_cnt`          BIGINT,
  `sum_delivery_days` BIGINT,
  `update_time`       TIMESTAMP(3)
) WITH (
  'connector'              = 'doris',
  'fenodes'                = 'doris-fe:8030',
  'table.identifier'       = 'realtime_dw.dws_trade_seller_1d',
  'username'               = 'root',
  'password'               = '',
  'sink.label-prefix'      = 'dws_trade_seller_1d',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete'     = 'true',
  'sink.buffer-size'       = '1048576',
  'sink.buffer-count'      = '32',
  'sink.buffer-flush.max-bytes' = '10485760',
  'sink.write-mode'        = 'STREAM_LOAD_BATCH'
);


-- -----------------------------------------------------------------------------
-- Sink 3/3：dws_trade_region_1d
-- -----------------------------------------------------------------------------
CREATE TABLE sink_dws_trade_region_1d (
  `dt`              DATE,
  `customer_state`  STRING,
  `gmv`             DECIMAL(20,2),
  `freight_total`   DECIMAL(20,2),
  `item_cnt`        BIGINT,
  `order_cnt`       BIGINT,
  `buyer_cnt`       BIGINT,
  `update_time`     TIMESTAMP(3)
) WITH (
  'connector'              = 'doris',
  'fenodes'                = 'doris-fe:8030',
  'table.identifier'       = 'realtime_dw.dws_trade_region_1d',
  'username'               = 'root',
  'password'               = '',
  'sink.label-prefix'      = 'dws_trade_region_1d',
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
-- 履约三态（seller 表）：is_late 为 1/0/NULL 三值，NULL 表示未妥投。
--   COUNT(is_late)  = 已签收行数（只数非 NULL）
--   SUM(is_late)    = 迟到行数（SUM 忽略 NULL）
-- 用这两个函数比 CASE WHEN 显式判空更难写错。延迟率 = late_cnt / delivered_cnt，
-- 分母不能写 order_cnt —— 写成 order_cnt 会让 2,454 个未妥投被当成准时，
-- 实测延迟率从 7.91% 掉到 7.74%。差 0.17 个点看着小，但这是口径错误不是精度问题。
EXECUTE STATEMENT SET
BEGIN

INSERT INTO sink_dws_trade_category_1d
SELECT
  b.`dt`,
  COALESCE(t.`product_category_name_english`,
           p.`product_category_name`,
           'unknown')                                    AS `category_name`,
  CAST(SUM(b.`item_total`) AS DECIMAL(20,2))             AS `gmv`,
  CAST(SUM(b.`freight_value`) AS DECIMAL(20,2))          AS `freight_total`,
  COUNT(*)                                               AS `item_cnt`,
  COUNT(DISTINCT b.`order_id`)                           AS `order_cnt`,
  COUNT(DISTINCT b.`seller_id`)                          AS `seller_cnt`,
  LOCALTIMESTAMP                                         AS `update_time`
FROM `v_order_base` b
LEFT JOIN `src_products` p
  ON b.`product_id` = p.`product_id`
LEFT JOIN `src_category_translation` t
  ON p.`product_category_name` = t.`product_category_name`
GROUP BY b.`dt`,
         COALESCE(t.`product_category_name_english`,
                  p.`product_category_name`,
                  'unknown');

INSERT INTO sink_dws_trade_seller_1d
SELECT
  b.`dt`,
  b.`seller_id`,
  CAST(SUM(b.`item_total`) AS DECIMAL(20,2))             AS `gmv`,
  COUNT(*)                                               AS `item_cnt`,
  COUNT(DISTINCT b.`order_id`)                           AS `order_cnt`,
  COUNT(b.`is_late`)                                     AS `delivered_cnt`,
  CAST(SUM(b.`is_late`) AS BIGINT)                       AS `late_cnt`,
  CAST(SUM(b.`delivery_days`) AS BIGINT)                 AS `sum_delivery_days`,
  LOCALTIMESTAMP                                         AS `update_time`
FROM `v_order_base` b
LEFT JOIN `src_sellers` s
  ON b.`seller_id` = s.`seller_id`
GROUP BY b.`dt`, b.`seller_id`;

INSERT INTO sink_dws_trade_region_1d
SELECT
  b.`dt`,
  c.`customer_state`,
  CAST(SUM(b.`item_total`) AS DECIMAL(20,2))             AS `gmv`,
  CAST(SUM(b.`freight_value`) AS DECIMAL(20,2))          AS `freight_total`,
  COUNT(*)                                               AS `item_cnt`,
  COUNT(DISTINCT b.`order_id`)                           AS `order_cnt`,
  COUNT(DISTINCT c.`customer_unique_id`)                 AS `buyer_cnt`,
  LOCALTIMESTAMP                                         AS `update_time`
FROM `v_order_base` b
LEFT JOIN `src_customers` c
  ON b.`customer_id` = c.`customer_id`
GROUP BY b.`dt`, c.`customer_state`;

END;
