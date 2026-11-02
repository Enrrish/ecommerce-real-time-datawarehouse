-- =============================================================================
-- 05-dwd-trade-order-detail.sql —— 交易事实宽表（P4 核心）
-- -----------------------------------------------------------------------------
-- 数据源：MySQL olist 的 6 张表 + 1 张翻译表，全部 CDC
-- 目标：realtime_dw.dwd_trade_order_detail
-- 粒度：订单行（order_id + order_item_id），验收标准是行数 = 112,650
-- server-id：5531-5558（ODS 占 5401-5484，本作业占 5531 起，两段不重叠）
--
-- =============================================================================
-- 为什么 6 表 CDC 直连 join 不会丢数据
-- =============================================================================
-- 第一直觉是：全量快照阶段各表速度不一，如果 order_items 先读完、orders 后读完，
-- 前面那批 join 不上就丢了。**这个直觉对 Flink 的 regular join 不成立。**
--
-- Flink SQL 的 regular join（不带时间属性）是**双向**的：两侧都保留完整状态，
-- 任何一侧的新记录到达，都会与另一侧所有匹配记录产生 join 输出。所以 orders 后到
-- 时会回头与已缓存的 order_items 重新匹配、补发结果。代价是同一行可能输出两次，
-- 但 Doris UNIQUE 表按主键覆盖，天然幂等。
--
-- 真正不能用的是「内连接 + 只让一侧有状态」的引擎，也不是不能先建维表——那是
-- 另一个方案，只是没必要。这里直连的好处是端到端延迟最短（少一跳）。
--
-- =============================================================================
-- 三个必须记住的坑
-- =============================================================================
-- 1) **支付必须先按订单聚合再 join**。一笔订单可能分期多次支付，直接 join
--    会把 112,650 行放大到 11 万+。这里用 GROUP BY order_id 聚合成
--    payment_total / payment_count 两个格度，聚合输出是 retract 流（-U/+U），
--    regular join 能消费。
--
-- 2) **类目必须 LEFT JOIN + COALESCE**。ods_category_translation 只有 71 个类目，
--    源表 73 个（缺 2 个），另外 610 个商品类目为 NULL。INNER JOIN 会静默丢掉
--    13 个商品、影响 GMV 5,514.48。
--
-- 3) **dt 取 DATE(order_purchase_timestamp)**，不是装载日期。UNIQUE 表的唯一性
--    只在单分区内保证，分区列若可变，同一订单会被拆进两个分区、互不覆盖，
--    GMV 凭空翻倍（docs/02 §3）。下单日期业务上永不变，所以安全。
--
-- =============================================================================
-- 状态量估算（决定 TM 内存够不够）
-- =============================================================================
--   orders            99,441 行 × ~300B ≈ 30 MB
--   order_items      112,650 行 × ~200B ≈ 22 MB
--   order_payments   103,886 行 × ~150B ≈ 16 MB（聚合后会小很多）
--   products          32,951 行 × ~500B ≈ 16 MB
--   customers         99,441 行 × ~200B ≈ 20 MB
--   sellers            3,095 行 × ~300B ≈  1 MB
--   合计约 105 MB（RocksDB 磁盘），当前 TM 有 512 MB managed memory，够用。
--   taskHeap 868 MB 分给这一个作业，也够。
-- =============================================================================

SET 'execution.attached' = 'false';
SET 'table.local-time-zone' = 'Asia/Shanghai';

-- -----------------------------------------------------------------------------
-- 状态后端：这个作业**必须**用堆内状态，不能跟着集群默认的 rocksdb 走
-- -----------------------------------------------------------------------------
-- 集群默认是 rocksdb + incremental，对 ODS 那种单流同步（状态只有几百 KB）
-- 很合适。但本作业的状态量约 115MB，而且 `v_payment_agg` 要在 99,441 个
-- order_id 上逐个更新状态，写入模式是「小 key、随机、海量」—— 正好是
-- RocksDB 在这台机器上最吃亏的形态。
--
-- 实测：聚合算子累计 CPU 只有 2.3 秒，墙钟却卡了 5 分多钟还停在 11/14 ack，
-- 线程栈里 GroupAggregate 一直停在 RocksDB.put 的 native 调用上。瓶颈不是算力，
-- 是 Docker Desktop on Windows 的磁盘 I/O —— 每个 memtable flush 都落在
-- overlay 文件系统上，而 512MB managed memory 按 4 个 slot 切完只剩 128MB，
-- 再被十几个有状态算子分，单个 RocksDB 实例的写缓冲小到一满就 flush。
--
-- 换回 hashmap：状态直接是堆内对象，checkpoint 是堆内拷贝，全程不碰磁盘。
-- 115MB 状态放进 217MB 的 taskHeap 有余量。数据量再上一个数量级（TB 级状态）
-- 才值得忍受 RocksDB 的磁盘开销，这个量级不值得。
SET 'state.backend' = 'hashmap';


-- -----------------------------------------------------------------------------
-- Source 1/7：orders（订单主表，决定客户与时间）
-- -----------------------------------------------------------------------------
CREATE TABLE src_orders (
  `order_id`                      STRING,
  `customer_id`                   STRING,
  `order_status`                  STRING,
  `order_purchase_timestamp`      TIMESTAMP(3),
  `order_approved_at`             TIMESTAMP(3),
  `order_delivered_carrier_date`  TIMESTAMP(3),
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
  'server-id'                    = '5531-5534',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 2/7：order_items（事实主表，决定宽表粒度）
-- -----------------------------------------------------------------------------
CREATE TABLE src_order_items (
  `order_id`            STRING,
  `order_item_id`       INT,
  `product_id`          STRING,
  `seller_id`           STRING,
  `shipping_limit_date` TIMESTAMP(3),
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
  'server-id'                    = '5535-5538',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 3/7：order_payments（会被聚合，不直接 join）
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
  'server-id'                    = '5539-5542',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 4/7：customers（客户维度）
-- -----------------------------------------------------------------------------
CREATE TABLE src_customers (
  `customer_id`              STRING,
  `customer_unique_id`       STRING,
  `customer_zip_code_prefix` STRING,
  `customer_city`            STRING,
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
  'server-id'                    = '5543-5546',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 5/7：products（商品维度）
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
  'server-id'                    = '5547-5550',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 6/7：sellers（卖家维度）
-- -----------------------------------------------------------------------------
CREATE TABLE src_sellers (
  `seller_id`       STRING,
  `seller_city`     STRING,
  `seller_state`    STRING,
  PRIMARY KEY (`seller_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'mysql-cdc',
  'hostname'                     = 'mysql',
  'port'                         = '3306',
  'username'                     = 'root',
  'password'                     = 'root123',
  'database-name'                = 'olist',
  'table-name'                   = 'sellers',
  'server-id'                    = '5551-5554',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000'
);


-- -----------------------------------------------------------------------------
-- Source 7/7：category_translation（类目翻译，小表）
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
  -- 表名是 category_translation，不是 product_category_translation。
  -- 猜错表名的现象很隐蔽：作业能提交、RUNNING，但 SplitEnumerator 启动时抛
  -- "Can't find any matched tables"，随即触发全局 failover，checkpoint 全部
  -- 失败；而 Flink CDC 在首次 checkpoint 完成前不读数据，于是表现为
  -- 「作业 RUNNING 但一条数据都没有」，MySQL 侧连 Binlog Dump 都看不到。
  'table-name'                   = 'category_translation',
  'server-id'                    = '5555-5558',
  'server-time-zone'             = 'Asia/Shanghai'
);


-- -----------------------------------------------------------------------------
-- 支付聚合：一单一行
-- -----------------------------------------------------------------------------
-- 输出是 retract 流（新增支付时发 -U/+U），regular join 能消费。
--
-- **刻意不输出 payment_type**。原因有两个：
--   1. Flink 1.18 没有 MAX_BY，取不到「金额最大的那笔的类型」；
--      退化用 MAX(payment_type) 是字典序最大，与金额无关，是个语义不清的假指标。
--   2. 更本质的是：支付方式是**订单级的一对多属性**，硬塞进订单行粒度的宽表，
--      无论选哪个都是近似。一个订单用「信用卡 + 优惠券」付，宽表只能记一个。
--      支付方式占比应该在 DWS 从 ods_order_payments 直接聚合，那里是准确的。
--
-- 宁可少一列，也不放一列语义不清的数据——后者会让下游做出错误判断。
CREATE TEMPORARY VIEW v_payment_agg AS
SELECT
  `order_id`,
  SUM(`payment_value`)                          AS payment_total,
  COUNT(*)                                      AS payment_count
FROM `src_order_payments`
GROUP BY `order_id`;


-- -----------------------------------------------------------------------------
-- Sink
-- -----------------------------------------------------------------------------
CREATE TABLE sink_dwd_trade_order_detail (
  `order_id`                      STRING,
  `order_item_id`                 INT,
  `dt`                            DATE,
  `customer_id`                   STRING,
  `customer_unique_id`            STRING,
  `customer_city`                 STRING,
  `customer_state`                STRING,
  `order_status`                  STRING,
  `order_purchase_timestamp`      TIMESTAMP(3),
  `order_approved_at`             TIMESTAMP(3),
  `order_delivered_carrier_date`  TIMESTAMP(3),
  `order_delivered_customer_date` TIMESTAMP(3),
  `order_estimated_delivery_date` TIMESTAMP(3),
  `product_id`                    STRING,
  `product_category_name`         STRING,
  `category_name_en`              STRING,
  `seller_id`                     STRING,
  `seller_city`                   STRING,
  `seller_state`                  STRING,
  `payment_total`                 DECIMAL(18,2),
  `payment_count`                 INT,
  `price`                         DECIMAL(18,2),
  `freight_value`                 DECIMAL(18,2),
  `item_total`                    DECIMAL(18,2),
  `delivery_days`                 INT,
  `is_late`                       TINYINT,
  `update_time`                   TIMESTAMP(3)
) WITH (
  'connector'              = 'doris',
  'fenodes'                = 'doris-fe:8030',
  'table.identifier'       = 'realtime_dw.dwd_trade_order_detail',
  -- username/password 必须显式写。connector 的默认 username 不是 'root' 而是
  -- null，漏写时 Doris 收到 user 'null' 返回 401，而 connector 把这段错误文本
  -- 当成 JSON 解析，最终报的是「Cannot deserialize value of type int」——
  -- 一个和认证毫无关系的错误。这个坑的排查成本全在错误信息上。
  'username'               = 'root',
  'password'               = '',
  'sink.label-prefix'      = 'dwd_trade_order_detail',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  -- UPDATE 会发 -U/+U 两条：-U 必须忽略（否则会先把行删掉再插），
  -- +U 按主键覆盖。DELETE 才需要真删。
  'sink.enable-delete'     = 'true',

  -- ---------------------------------------------------------------------------
  -- 下面三个参数是这块卡了最久的地方，务必保留。
  --
  -- connector 24.0.1 的 Stream Load 请求体是**流式发送**的：待发数据先塞进
  -- sink.buffer-count 个、每个 sink.buffer-size 字节的环形缓冲，HTTP 客户端
  -- 边读边往 socket 写。默认 1MB × 3 = 3MB。
  --
  -- 只要一次 flush 的数据量 ≥ 环容量，写端写满环后阻塞、读端取空环后阻塞在
  -- LinkedBlockingDeque.take()，两端互等 —— 死锁。
  --
  -- 现场表现（有迷惑性，全程零报错）：
  --   * 作业 RUNNING、7 个 CDC reader 都在正常推进 binlog offset
  --   * 各算子 write-records 都在涨，sink 的 read-records 已到 2 万多
  --   * 但 checkpoint 永远停在 3/14，Doris 表 0 行
  --   * 源端 100% 背压，下游 0% —— 镜像指标，看不出谁堵
  --   * kill -3 抓线程栈才看到 sink 线程 WAITING 在 RecordBuffer.read()
  --
  -- 把环放大到 32MB、单批上限压到 10MB（connector 允许的最小值，也是 < 环容量
  -- 的安全值）后不再触发。这两个值必须成对给：单给 max-bytes 大于环容量照样死，
  -- 而 max-bytes 低于 10MB 会被 connector 直接拒绝启动。
  'sink.buffer-size'       = '1048576',
  'sink.buffer-count'      = '32',
  'sink.buffer-flush.max-bytes' = '10485760',
  -- 环放大只能缓解、不能根治：读端取空、写端因 flush 等结果而停摆，两端互等。
  -- STREAM_LOAD_BATCH 走"先攒后发"的整批请求体，绕开这条流式管道。
  'sink.write-mode'        = 'STREAM_LOAD_BATCH'
);


-- -----------------------------------------------------------------------------
-- 装载
-- -----------------------------------------------------------------------------
-- 派生字段说明：
--   item_total    = price + freight_value（行级合计，不等于订单总额；订单总额
--                   要用 SUM(item_total) GROUP BY order_id 在 DWS 算）
--   delivery_days = 下单到签收的天数，未签收为 NULL（不能写 0，0 会被平均进去）
--   is_late       = 签收晚于预计送达，未签收为 NULL（三态，不是布尔）
--
-- 三态字段在 Doris 里用 TINYINT + NULL 表达。P5 算「延迟发货率」时分母必须
-- 限定 is_late IS NOT NULL，否则未签收订单会被算成「准时」，把指标拉低。
INSERT INTO sink_dwd_trade_order_detail
SELECT
  oi.`order_id`,
  oi.`order_item_id`,
  CAST(o.`order_purchase_timestamp` AS DATE)                     AS `dt`,

  o.`customer_id`,
  c.`customer_unique_id`,
  c.`customer_city`,
  c.`customer_state`,

  o.`order_status`,
  o.`order_purchase_timestamp`,
  o.`order_approved_at`,
  o.`order_delivered_carrier_date`,
  o.`order_delivered_customer_date`,
  o.`order_estimated_delivery_date`,

  oi.`product_id`,
  p.`product_category_name`,
  COALESCE(t.`product_category_name_english`,
           p.`product_category_name`,
           'unknown')                                            AS `category_name_en`,

  oi.`seller_id`,
  s.`seller_city`,
  s.`seller_state`,

  -- 类型必须逐列 CAST 到 sink 的宽度，否则 Flink 在提交阶段就报
  -- 「Column types of query result and sink do not match」，且只指出第一处：
  --   SUM(DECIMAL(18,2)) → DECIMAL(38,2)
  --   COUNT(*)           → BIGINT
  --   CASE ... THEN 1    → INT（不是 TINYINT）
  --   price + freight    → DECIMAL(19,2)（多一位进位）
  CAST(pa.`payment_total` AS DECIMAL(18,2))                      AS `payment_total`,
  CAST(pa.`payment_count` AS INT)                                AS `payment_count`,

  oi.`price`,
  oi.`freight_value`,
  CAST(oi.`price` + oi.`freight_value` AS DECIMAL(18,2))         AS `item_total`,

  CASE
    WHEN o.`order_delivered_customer_date` IS NULL THEN NULL
    ELSE TIMESTAMPDIFF(DAY, o.`order_purchase_timestamp`, o.`order_delivered_customer_date`)
  END                                                            AS `delivery_days`,

  CAST(CASE
    WHEN o.`order_delivered_customer_date` IS NULL THEN NULL
    WHEN o.`order_delivered_customer_date` > o.`order_estimated_delivery_date` THEN 1
    ELSE 0
  END AS TINYINT)                                                AS `is_late`,

  LOCALTIMESTAMP                                                 AS `update_time`
FROM `src_order_items` oi
JOIN `src_orders` o
  ON oi.`order_id` = o.`order_id`
LEFT JOIN `src_customers` c
  ON o.`customer_id` = c.`customer_id`
LEFT JOIN `src_products` p
  ON oi.`product_id` = p.`product_id`
LEFT JOIN `src_sellers` s
  ON oi.`seller_id` = s.`seller_id`
LEFT JOIN `src_category_translation` t
  ON p.`product_category_name` = t.`product_category_name`
LEFT JOIN `v_payment_agg` pa
  ON oi.`order_id` = pa.`order_id`;
