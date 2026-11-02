-- =============================================================================
-- 03-ods-facts.sql — ODS 3 张明细表的 CDC 作业
-- -----------------------------------------------------------------------------
-- 提交：bash scripts/submit-sql.sh flink/sql/03-ods-facts.sql
--
-- 三张表都是「一张表多行」的明细粒度，主键全部是复合键 —— 这是它们与
-- 维表（单列主键）和订单主表（单列主键）最本质的区别：
--
--   order_items      (order_id, order_item_id)    一个订单多行商品
--   order_payments   (order_id, payment_sequential) 一个订单多笔支付
--   order_reviews    (review_id, order_id)         一条评价可能挂在多单上
--
-- 复合键在 CDC 里的影响：Flink 的 PRIMARY KEY (...) NOT ENFORCED 会带上全部
-- 键列，Doris 侧 UNIQUE KEY 也是同样两列。两边必须**顺序一致**，否则 UNIQUE
-- 模型的前缀索引会错位（Doris 要求 UNIQUE KEY 列顺序与建表时一致）。
--
-- server-id 分段（续 02 号文件的表）：
--   5461-5464  order_items
--   5471-5474  order_payments
--   5481-5484  order_reviews
-- =============================================================================
SET 'execution.attached' = 'false';
SET 'table.local-time-zone' = 'Asia/Shanghai';


-- -----------------------------------------------------------------------------
-- 1. order_items → ods_order_items（112,650 行）
-- -----------------------------------------------------------------------------
-- GMV 的真正来源。orders 表里没有任何金额字段，所有营收指标都要从这里算：
--   GMV = SUM(price)，运费收入 = SUM(freight_value)，客单价 = GMV / COUNT(DISTINCT order_id)
--
-- 金额必须是 DECIMAL(18,2) 两端一致。src 侧声明成 DOUBLE 会让 112,650 行
-- 累加出 1e-10 级别的误差，对账时表现为「Doris 与 MySQL 差 0.01」这种查不动的问题。
--
-- shipping_limit_date 是卖家发货截止时间，不是实际发货时间。算「发货超时」
-- 要用它与 orders.order_delivered_carrier_date 比较，别拿它当事实时间用。
CREATE TABLE src_order_items (
  order_id            STRING,
  order_item_id       INT,
  product_id          STRING,
  seller_id           STRING,
  shipping_limit_date TIMESTAMP(3),
  price               DECIMAL(18, 2),
  freight_value       DECIMAL(18, 2),
  `row_kind` STRING METADATA FROM 'row_kind' VIRTUAL,
  `op_ts`    TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  PRIMARY KEY (order_id, order_item_id) NOT ENFORCED
) WITH (
  'connector'   = 'mysql-cdc',
  'hostname'    = 'mysql',
  'port'        = '3306',
  'username'    = 'root',
  'password'    = 'root123',
  'database-name' = 'olist',
  'table-name'  = 'order_items',
  'server-id'   = '5461-5464',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '8096'
);

CREATE TABLE sink_ods_order_items (
  order_id            STRING,
  order_item_id       INT,
  product_id          STRING,
  seller_id           STRING,
  shipping_limit_date TIMESTAMP(3),
  price               DECIMAL(18, 2),
  freight_value       DECIMAL(18, 2),
  `op`    STRING,
  `ts_ms` BIGINT,
  `dt`    DATE,
  PRIMARY KEY (order_id, order_item_id) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes'   = 'doris-fe:8030',
  'username'  = 'root',
  'password'  = '',
  'table.identifier' = 'realtime_dw.ods_order_items',
  'sink.label-prefix' = 'ods_order_items',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true'
);

INSERT INTO sink_ods_order_items
SELECT
  order_id, order_item_id, product_id, seller_id,
  shipping_limit_date, price, freight_value,
  CASE `row_kind` WHEN '+I' THEN 'c' WHEN '+U' THEN 'u' WHEN '-U' THEN 'u' WHEN '-D' THEN 'd' ELSE 'x' END,
  COALESCE(UNIX_TIMESTAMP(DATE_FORMAT(`op_ts`, 'yyyy-MM-dd HH:mm:ss')), 0) * 1000,
  CURRENT_DATE
FROM src_order_items;


-- -----------------------------------------------------------------------------
-- 2. order_payments → ods_order_payments（103,886 行）
-- -----------------------------------------------------------------------------
-- 一个订单可有多笔支付（最多 29 笔），所以「支付笔数」不能用 COUNT(order_id)
-- 直接当订单数，要 COUNT(DISTINCT order_id)。
--
-- 坑：payment_installments 有 2 行是 0，不是 1。任何「分期数 >= 1」的假设
-- 都会静默过滤掉这 2 行，让分期分布统计差 2 笔。这类 <0.01% 的偏差最难发现。
--
-- payment_type 实测只有 4 类（credit_card / boleto / voucher / debit_card），
-- 但源端是 VARCHAR(24)，别在 DWD 里写死枚举，否则将来加 "not_defined" 就丢数据。
CREATE TABLE src_order_payments (
  order_id             STRING,
  payment_sequential   INT,
  payment_type         STRING,
  payment_installments INT,
  payment_value        DECIMAL(18, 2),
  `row_kind` STRING METADATA FROM 'row_kind' VIRTUAL,
  `op_ts`    TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  PRIMARY KEY (order_id, payment_sequential) NOT ENFORCED
) WITH (
  'connector'   = 'mysql-cdc',
  'hostname'    = 'mysql',
  'port'        = '3306',
  'username'    = 'root',
  'password'    = 'root123',
  'database-name' = 'olist',
  'table-name'  = 'order_payments',
  'server-id'   = '5471-5474',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '8096'
);

CREATE TABLE sink_ods_order_payments (
  order_id             STRING,
  payment_sequential   INT,
  payment_type         STRING,
  payment_installments INT,
  payment_value        DECIMAL(18, 2),
  `op`    STRING,
  `ts_ms` BIGINT,
  `dt`    DATE,
  PRIMARY KEY (order_id, payment_sequential) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes'   = 'doris-fe:8030',
  'username'  = 'root',
  'password'  = '',
  'table.identifier' = 'realtime_dw.ods_order_payments',
  'sink.label-prefix' = 'ods_order_payments',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true'
);

INSERT INTO sink_ods_order_payments
SELECT
  order_id, payment_sequential, payment_type, payment_installments, payment_value,
  CASE `row_kind` WHEN '+I' THEN 'c' WHEN '+U' THEN 'u' WHEN '-U' THEN 'u' WHEN '-D' THEN 'd' ELSE 'x' END,
  COALESCE(UNIX_TIMESTAMP(DATE_FORMAT(`op_ts`, 'yyyy-MM-dd HH:mm:ss')), 0) * 1000,
  CURRENT_DATE
FROM src_order_payments;


-- -----------------------------------------------------------------------------
-- 3. order_reviews → ods_order_reviews（99,224 行）
-- -----------------------------------------------------------------------------
-- 全库唯一一个主键为 (review_id, order_id) 的表，原因是 review_id **不唯一**：
-- 有 814 个 review_id 重复出现（最多重复 3 次）。单用 review_id 做 Key 的话，
-- 导入直接撞主键失败；就算导入侥幸过了，UNIQUE 模型也会把 814 条合并成 1 条，
-- 行数静默少 814 —— 而且 COUNT(*) 对不上时很难想到是主键选错了。
--
-- review_comment_message 里**含换行符**（CSV 引号内的多行）。走 JSON 序列化
-- 会自动转义成 \n，所以不需要预处理；但如果你改成 CSV 格式写 Doris，
-- \n 会把一行劈成两行，报 "column count mismatch"。这就是这里坚持用 json 的原因。
--
-- review_creation_date 全部是 00:00:00（源系统只记到天），别把它当精确时间戳
-- 去算「评价响应时长」——该用 review_answer_timestamp 减它，结果精确到天即可。
CREATE TABLE src_order_reviews (
  review_id               STRING,
  order_id                STRING,
  review_score            INT,
  review_comment_title    STRING,
  review_comment_message  STRING,
  review_creation_date    TIMESTAMP(3),
  review_answer_timestamp TIMESTAMP(3),
  `row_kind` STRING METADATA FROM 'row_kind' VIRTUAL,
  `op_ts`    TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  PRIMARY KEY (review_id, order_id) NOT ENFORCED
) WITH (
  'connector'   = 'mysql-cdc',
  'hostname'    = 'mysql',
  'port'        = '3306',
  'username'    = 'root',
  'password'    = 'root123',
  'database-name' = 'olist',
  'table-name'  = 'order_reviews',
  'server-id'   = '5481-5484',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '8096'
);

CREATE TABLE sink_ods_order_reviews (
  review_id               STRING,
  order_id                STRING,
  review_score            INT,
  review_comment_title    STRING,
  review_comment_message  STRING,
  review_creation_date    TIMESTAMP(3),
  review_answer_timestamp TIMESTAMP(3),
  `op`    STRING,
  `ts_ms` BIGINT,
  `dt`    DATE,
  PRIMARY KEY (review_id, order_id) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes'   = 'doris-fe:8030',
  'username'  = 'root',
  'password'  = '',
  'table.identifier' = 'realtime_dw.ods_order_reviews',
  'sink.label-prefix' = 'ods_order_reviews',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true'
);

INSERT INTO sink_ods_order_reviews
SELECT
  review_id, order_id, review_score,
  review_comment_title, review_comment_message,
  review_creation_date, review_answer_timestamp,
  CASE `row_kind` WHEN '+I' THEN 'c' WHEN '+U' THEN 'u' WHEN '-U' THEN 'u' WHEN '-D' THEN 'd' ELSE 'x' END,
  COALESCE(UNIX_TIMESTAMP(DATE_FORMAT(`op_ts`, 'yyyy-MM-dd HH:mm:ss')), 0) * 1000,
  CURRENT_DATE
FROM src_order_reviews;
