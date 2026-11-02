-- =============================================================================
-- 02-ods-dimensions.sql — ODS 5 张维表的 CDC 作业
-- -----------------------------------------------------------------------------
-- 提交：bash scripts/submit-sql.sh flink/sql/02-ods-dimensions.sql
--
-- 为什么这 5 张合在一个文件里：
--   01-ods-orders.sql 已经用订单表把每个参数为什么这么配讲透了一次（row_kind 映射、
--   ts_ms 的 UNIX_TIMESTAMP 坑、sink.batch.* 不支持、bufferFlushMaxRows 下限）。
--   维表这边只有列名不同，逻辑完全一致，再复制 5 份完整注释只会稀释重点。
--   这里只记每张表**自己特有**的坑。
--
-- server-id 分段规则（必须遵守，否则作业互相踢下线）：
--   MySQL 主从协议里 server-id 是客户端身份，两个 CDC 客户端用同一个 id 会互相
--   把对方踢掉，表现为作业反复重启、日志里 "A slave with the same server_uuid/
--   server_id ... has connected"。所以每张表独占一段，段大小 = 该表的并行度上限。
--
--     5401-5404  orders              （01 号文件）
--     5411-5414  customers
--     5421-5424  sellers
--     5431-5434  products
--     5441-5444  category_translation
--     5451-5458  geolocation         （100 万行，留 8 个 id 以便后续提并行度）
--     5461-5464  order_items         （03 号文件）
--     5471-5474  order_payments      （03 号文件）
--     5481-5484  order_reviews       （03 号文件）
--
-- sink.label-prefix 每张表必须不同：Doris Stream Load 的 label 是幂等键，
--   两个作业用同一个 label 写同一张表，第二个会被判为重复请求而整批丢弃。
--   这里统一用表名。
--
-- 三个元信息列的算法与 01 号文件完全一致，不再重复解释：
--   op    = CASE row_kind WHEN '+I' THEN 'c' ...
--   ts_ms = COALESCE(UNIX_TIMESTAMP(DATE_FORMAT(op_ts,'yyyy-MM-dd HH:mm:ss')),0)*1000
--   dt    = CURRENT_DATE（落地日，不是源端事件日 —— 快照行的 op_ts 是 epoch 0）
-- =============================================================================
SET 'execution.attached' = 'false';
SET 'table.local-time-zone' = 'Asia/Shanghai';


-- -----------------------------------------------------------------------------
-- 1. customers → ods_customers（99,441 行）
-- -----------------------------------------------------------------------------
-- 坑：customer_id 是订单级 ID（与 orders 1:1），customer_unique_id 才是自然人。
-- 两个都要抽过来，DWD 的 RFM / 复购率必须用 unique_id，否则每人恒等于 1 单。
CREATE TABLE src_customers (
  customer_id              STRING,
  customer_unique_id       STRING,
  customer_zip_code_prefix STRING,
  customer_city            STRING,
  customer_state           STRING,
  `row_kind` STRING METADATA FROM 'row_kind' VIRTUAL,
  `op_ts`    TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector'   = 'mysql-cdc',
  'hostname'    = 'mysql',
  'port'        = '3306',
  'username'    = 'root',
  'password'    = 'root123',
  'database-name' = 'olist',
  'table-name'  = 'customers',
  'server-id'   = '5411-5414',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '8096'
);

CREATE TABLE sink_ods_customers (
  customer_id              STRING,
  customer_unique_id       STRING,
  customer_zip_code_prefix STRING,
  customer_city            STRING,
  customer_state           STRING,
  `op`    STRING,
  `ts_ms` BIGINT,
  `dt`    DATE,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes'   = 'doris-fe:8030',
  'username'  = 'root',
  'password'  = '',
  'table.identifier' = 'realtime_dw.ods_customers',
  'sink.label-prefix' = 'ods_customers',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true'
);

INSERT INTO sink_ods_customers
SELECT
  customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state,
  CASE `row_kind` WHEN '+I' THEN 'c' WHEN '+U' THEN 'u' WHEN '-U' THEN 'u' WHEN '-D' THEN 'd' ELSE 'x' END,
  COALESCE(UNIX_TIMESTAMP(DATE_FORMAT(`op_ts`, 'yyyy-MM-dd HH:mm:ss')), 0) * 1000,
  CURRENT_DATE
FROM src_customers;


-- -----------------------------------------------------------------------------
-- 2. sellers → ods_sellers（3,095 行）
-- -----------------------------------------------------------------------------
-- 行数最少的一张，但它是 GMV 按卖家拆分、卖家履约时效分析的唯一入口。
CREATE TABLE src_sellers (
  seller_id              STRING,
  seller_zip_code_prefix STRING,
  seller_city            STRING,
  seller_state           STRING,
  `row_kind` STRING METADATA FROM 'row_kind' VIRTUAL,
  `op_ts`    TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  PRIMARY KEY (seller_id) NOT ENFORCED
) WITH (
  'connector'   = 'mysql-cdc',
  'hostname'    = 'mysql',
  'port'        = '3306',
  'username'    = 'root',
  'password'    = 'root123',
  'database-name' = 'olist',
  'table-name'  = 'sellers',
  'server-id'   = '5421-5424',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '8096'
);

CREATE TABLE sink_ods_sellers (
  seller_id              STRING,
  seller_zip_code_prefix STRING,
  seller_city            STRING,
  seller_state           STRING,
  `op`    STRING,
  `ts_ms` BIGINT,
  `dt`    DATE,
  PRIMARY KEY (seller_id) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes'   = 'doris-fe:8030',
  'username'  = 'root',
  'password'  = '',
  'table.identifier' = 'realtime_dw.ods_sellers',
  'sink.label-prefix' = 'ods_sellers',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true'
);

INSERT INTO sink_ods_sellers
SELECT
  seller_id, seller_zip_code_prefix, seller_city, seller_state,
  CASE `row_kind` WHEN '+I' THEN 'c' WHEN '+U' THEN 'u' WHEN '-U' THEN 'u' WHEN '-D' THEN 'd' ELSE 'x' END,
  COALESCE(UNIX_TIMESTAMP(DATE_FORMAT(`op_ts`, 'yyyy-MM-dd HH:mm:ss')), 0) * 1000,
  CURRENT_DATE
FROM src_sellers;


-- -----------------------------------------------------------------------------
-- 3. products → ods_products（32,951 行）
-- -----------------------------------------------------------------------------
-- 坑 1：product_category_name 有 610 行（1.85%）为 NULL，src 侧不能声明 NOT NULL。
-- 坑 2：字段名 lenght 是源数据自带的拼写错误，贴源层照抄不改（改名放 DWD）。
-- 坑 3：product_description_lenght 实测最大 3992 —— Doris 侧必须 SMALLINT，
--       TINYINT 上限 127 会截断。这条在 01-ods.sql 里已经踩过一次。
CREATE TABLE src_products (
  product_id                 STRING,
  product_category_name      STRING,
  product_name_lenght        INT,
  product_description_lenght INT,
  product_photos_qty         INT,
  product_weight_g           INT,
  product_length_cm          INT,
  product_height_cm          INT,
  product_width_cm           INT,
  `row_kind` STRING METADATA FROM 'row_kind' VIRTUAL,
  `op_ts`    TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
  'connector'   = 'mysql-cdc',
  'hostname'    = 'mysql',
  'port'        = '3306',
  'username'    = 'root',
  'password'    = 'root123',
  'database-name' = 'olist',
  'table-name'  = 'products',
  'server-id'   = '5431-5434',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '8096'
);

-- src 侧小整数一律放宽成 INT：MySQL 的 SMALLINT/TINYINT UNSIGNED 在 CDC 里
-- 会被映射成对应位宽，一旦源端值超过声明宽度就是静默截断。这里宁可放宽，
-- 让 Doris 侧（SMALLINT / TINYINT）来兜底，溢出会直接报错而不是悄悄改数。
CREATE TABLE sink_ods_products (
  product_id                 STRING,
  product_category_name      STRING,
  product_name_lenght        INT,
  product_description_lenght INT,
  product_photos_qty         INT,
  product_weight_g           INT,
  product_length_cm          INT,
  product_height_cm          INT,
  product_width_cm           INT,
  `op`    STRING,
  `ts_ms` BIGINT,
  `dt`    DATE,
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes'   = 'doris-fe:8030',
  'username'  = 'root',
  'password'  = '',
  'table.identifier' = 'realtime_dw.ods_products',
  'sink.label-prefix' = 'ods_products',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true'
);

INSERT INTO sink_ods_products
SELECT
  product_id, product_category_name,
  product_name_lenght, product_description_lenght, product_photos_qty,
  product_weight_g, product_length_cm, product_height_cm, product_width_cm,
  CASE `row_kind` WHEN '+I' THEN 'c' WHEN '+U' THEN 'u' WHEN '-U' THEN 'u' WHEN '-D' THEN 'd' ELSE 'x' END,
  COALESCE(UNIX_TIMESTAMP(DATE_FORMAT(`op_ts`, 'yyyy-MM-dd HH:mm:ss')), 0) * 1000,
  CURRENT_DATE
FROM src_products;


-- -----------------------------------------------------------------------------
-- 4. category_translation → ods_category_translation（71 行）
-- -----------------------------------------------------------------------------
-- 只有 71 行，但商品表有 73 个类目 —— DWD 关联必须 LEFT JOIN + COALESCE。
-- 用 INNER JOIN 会静默丢掉 13 个商品、24 条订单行、GMV 5,514.48（docs/02 已算过）。
CREATE TABLE src_category_translation (
  product_category_name         STRING,
  product_category_name_english STRING,
  `row_kind` STRING METADATA FROM 'row_kind' VIRTUAL,
  `op_ts`    TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  PRIMARY KEY (product_category_name) NOT ENFORCED
) WITH (
  'connector'   = 'mysql-cdc',
  'hostname'    = 'mysql',
  'port'        = '3306',
  'username'    = 'root',
  'password'    = 'root123',
  'database-name' = 'olist',
  'table-name'  = 'category_translation',
  'server-id'   = '5441-5444',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '8096'
);

CREATE TABLE sink_ods_category_translation (
  product_category_name         STRING,
  product_category_name_english STRING,
  `op`    STRING,
  `ts_ms` BIGINT,
  `dt`    DATE,
  PRIMARY KEY (product_category_name) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes'   = 'doris-fe:8030',
  'username'  = 'root',
  'password'  = '',
  'table.identifier' = 'realtime_dw.ods_category_translation',
  'sink.label-prefix' = 'ods_category_translation',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true'
);

INSERT INTO sink_ods_category_translation
SELECT
  product_category_name, product_category_name_english,
  CASE `row_kind` WHEN '+I' THEN 'c' WHEN '+U' THEN 'u' WHEN '-U' THEN 'u' WHEN '-D' THEN 'd' ELSE 'x' END,
  COALESCE(UNIX_TIMESTAMP(DATE_FORMAT(`op_ts`, 'yyyy-MM-dd HH:mm:ss')), 0) * 1000,
  CURRENT_DATE
FROM src_category_translation;


-- -----------------------------------------------------------------------------
-- 5. geolocation → ods_geolocation（1,000,163 行，全库最大）
-- -----------------------------------------------------------------------------
-- 这张表有仨特殊性，都是其他表没有的：
--
-- 1) 唯一有代理键的表。源数据没有自然主键（zip+lat+lng 有 28 万条重复，
--    同一三元组最多重复 314 次），MySQL 侧加了 BIGINT UNSIGNED AUTO_INCREMENT 的 id。
--    CDC 能读到 id，所以 Doris 侧也是 UNIQUE(id) —— 100 万行一条不丢，且幂等。
--    附带好处：增量快照可以直接按自增主键切 chunk，比无主键表快一个数量级。
--
-- 2) DECIMAL(10,7)。geolocation_lat/lng 原始 14 位小数，MySQL 侧已收到 7 位
--    （约 1cm 精度，够用）。src 侧必须声明相同精度，声明成 DOUBLE 会引入浮点误差，
--    让「同邮编取中心点」这类聚合每次跑出不同结果。
--
-- 3) 分块调大 + 并行度留了 8 个 server-id。100 万行按默认 chunk 8096 走要
--    124 个 chunk，单并行度跑完实测偏慢；后续想提速直接改 sink 上游并行度即可，
--    不用回来改 server-id。
CREATE TABLE src_geolocation (
  id                          BIGINT,
  geolocation_zip_code_prefix STRING,
  geolocation_lat             DECIMAL(10, 7),
  geolocation_lng             DECIMAL(10, 7),
  geolocation_city            STRING,
  geolocation_state           STRING,
  `row_kind` STRING METADATA FROM 'row_kind' VIRTUAL,
  `op_ts`    TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector'   = 'mysql-cdc',
  'hostname'    = 'mysql',
  'port'        = '3306',
  'username'    = 'root',
  'password'    = 'root123',
  'database-name' = 'olist',
  'table-name'  = 'geolocation',
  'server-id'   = '5451-5458',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '32768'
);

CREATE TABLE sink_ods_geolocation (
  id                          BIGINT,
  geolocation_zip_code_prefix STRING,
  geolocation_lat             DECIMAL(10, 7),
  geolocation_lng             DECIMAL(10, 7),
  geolocation_city            STRING,
  geolocation_state           STRING,
  `op`    STRING,
  `ts_ms` BIGINT,
  `dt`    DATE,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes'   = 'doris-fe:8030',
  'username'  = 'root',
  'password'  = '',
  'table.identifier' = 'realtime_dw.ods_geolocation',
  'sink.label-prefix' = 'ods_geolocation',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true'
);

INSERT INTO sink_ods_geolocation
SELECT
  id, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
  geolocation_city, geolocation_state,
  CASE `row_kind` WHEN '+I' THEN 'c' WHEN '+U' THEN 'u' WHEN '-U' THEN 'u' WHEN '-D' THEN 'd' ELSE 'x' END,
  COALESCE(UNIX_TIMESTAMP(DATE_FORMAT(`op_ts`, 'yyyy-MM-dd HH:mm:ss')), 0) * 1000,
  CURRENT_DATE
FROM src_geolocation;
