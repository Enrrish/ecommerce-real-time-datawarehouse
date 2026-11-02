-- =============================================================================
-- 01-ods.sql — Doris ODS 贴源层（10 张表）
-- -----------------------------------------------------------------------------
-- 执行：mysql -h127.0.0.1 -P9030 -uroot < 01-ods.sql
-- 版本：Doris 2.1.11（单 FE 单 BE，`replication_num=1`）
--
-- 与 sql/mysql/01-schema.sql 的关系：
--   列名、列序、语义与 MySQL 源表**逐列一致**（含源端拼写错误 lenght）。
--   末尾追加元信息列，追加而不是插入，是为了让 Flink 的
--   `INSERT INTO ... SELECT` 按位置对齐源表前 N 列，少一次人工对位。
--   前 9 张表追加 op / ts_ms / dt；ods_user_event 来自 Kafka，没有 op
--   （消息体里没有变更类型这个概念），只有 dt / ts_ms，见该表注释。
--
-- 三个必须解释的设计决定
-- =============================================================================
--
-- 【决定一】ODS 一律不分区 —— docs/02 §3 原来写的「统一按 dt 动态分区」是错的
--
--   错在哪有两处，任何一处都会让 P3 当场翻车：
--
--   1. 动态分区 `start=-30, end=7` 会自动 DROP 掉 30 天以前的分区。
--      而 Olist 的业务数据是 2016-09 ~ 2018-10，首次全量快照灌进来时，
--      每个 dt 都早于 30 天前 → 分区创建后立刻被回收。
--      现象是「导入报成功、COUNT(*) 是 0」，且日志里只有分区回收记录，不像报错。
--
--   2. 更根本的问题：UNIQUE 表的分区列**必须是 Key 列的子集**，且分区列一旦
--      可变就会破坏唯一性。若 dt 取「装载日期」，同一 order_id 今天更新一次、
--      明天再更新一次，会分别落在两个分区的两个 tablet 里；Doris 的唯一性只在
--      单个 tablet 内保证，两次更新不会互相覆盖 → 同一订单出现两行。
--      这类重复不会报任何错，只在指标里表现为 GMV 偏高。
--
--   贴源层的数据量（155 万行 / 118MB）根本不需要分区裁剪。分区留给 DWD：
--   那里的事实表 dt = 下单日期，是**不可变**的，分区才安全（P4 落地）。
--
-- 【决定二】10 张表全部 UNIQUE KEY（含 geolocation，docs/02 原定 DUPLICATE）
--
--   geolocation 的「允许重复」说的是自然键 (zip, lat, lng) 有 28 万条重复，
--   但源表已经有自增代理键 id，且 1,000,163 行 id 全唯一。
--   用 UNIQUE(id) 既完整保留 100 万行，又获得幂等：
--     * DUPLICATE 模型重跑一次作业 → 行数翻倍（100 万 → 200 万），且无法察觉
--     * UNIQUE 模型重跑任意次 → 结果恒定，断点续传、改并行度重放都安全
--   顺带好处：10 张表同一个模型、同一套 sink 配置，P3 只有一种失败模式要排查。
--
-- 【决定三】VARCHAR 的长度单位是**字节**，不是字符
--
--   MySQL 的 VARCHAR(64) 是 64 个字符（utf8mb4 下最多 256 字节）；
--   Doris 的 VARCHAR(64) 是 64 个**字节**。照抄 MySQL 的长度，遇到葡语重音
--   字符（ã / ç / ó 各占 2 字节）就会写入失败，报 "Insert has filtered data"。
--   下面每个 VARCHAR 都是按实测最大字节数 × 约 1.5 倍余量定的，实测值见注释。
--   超长文本用 STRING（不限长），避免将来换个数据集就炸。
--
-- 其他约定
--   * 金额 DECIMAL(18,2)，与 MySQL 一致，不用 FLOAT
--   * 时间 DATETIME，不用 TIMESTAMP（时区换算 + 2038 溢出，CDC 后会整体偏 8 小时）
--   * 邮编 VARCHAR(5) 而不是 CHAR(5)：Doris 的 CHAR 会补空格，
--     比较时依赖隐式 rtrim，把「字符串」语义变成「定长字符」语义，没必要
--
-- 关于 ts_ms = 0：**快照行的 ts_ms 是有意的 0，不代表 1970 年**。
--   首轮全量扫表时没有对应的 binlog 事件，Flink CDC 给的 op_ts 是 epoch 0。
--   保留 0 作为哨兵，语义是「这一行没有源端时间戳」，比编一个假时间诚实。
--   所以任何基于 ts_ms 的指标都必须带 WHERE ts_ms > 0。
--   同时 dt 不能取 DATE(op_ts)：那样首轮全量后 155 万行会全部落进 1970-01-01，
--   DWD 一旦按 dt 做动态分区（start=-30），整个历史分区会被当场 DROP 掉，
--   而 ODS 看起来「数据都在」。这就是 dt 取 CURRENT_DATE（落地日）的原因。
--   * op 取值 c/u/x（由 Flink 的 row_kind 映射：+I→c、+U/-U→u、-D→d）。
--     UNIQUE 模型下 op='d' 的那次变更会把行直接删掉，所以表里最终只会看到 c/u，
--     删除痕迹在 binlog 与 Kafka 里，不在 ODS（docs/02 §5.2 已如此约定）。
--     **快照行也是 c，不是 r** —— 这一点被实测纠正过，详见 02-verify.sql 检查 2。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS `realtime_dw`;

USE `realtime_dw`;


-- -----------------------------------------------------------------------------
-- 1. ods_customers —— 客户（99,441 行）
-- -----------------------------------------------------------------------------
-- customer_id 是订单级 ID，与 orders 1:1；真正的「人」是 customer_unique_id。
-- 两者都要留：DWD 的 SCD2 拉链用 unique_id，关联 orders 用 customer_id。
CREATE TABLE IF NOT EXISTS `ods_customers` (
  `customer_id`              VARCHAR(32) NOT NULL COMMENT '订单级客户ID',
  `customer_unique_id`       VARCHAR(32) NOT NULL COMMENT '真实客户ID（跨订单唯一）',
  `customer_zip_code_prefix` VARCHAR(5)  NOT NULL COMMENT '保留前导零',
  `customer_city`            VARCHAR(64) NOT NULL COMMENT '实测最大 40 字节',
  `customer_state`           VARCHAR(2)  NOT NULL,
  `op`                       VARCHAR(1)  NOT NULL COMMENT 'c/u/d/r',
  `ts_ms`                    BIGINT      NOT NULL COMMENT 'binlog 事件时间（毫秒）；快照行为 0，见文件头说明',
  `dt`                       DATE        NOT NULL COMMENT '落地日期 CURRENT_DATE（不是源端变更日）'
) ENGINE = OLAP
UNIQUE KEY(`customer_id`)
COMMENT '客户：99,441 行 / 96,096 个自然人'
DISTRIBUTED BY HASH(`customer_id`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 2. ods_sellers —— 卖家（3,095 行）
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `ods_sellers` (
  `seller_id`              VARCHAR(32) NOT NULL,
  `seller_zip_code_prefix` VARCHAR(5)  NOT NULL,
  `seller_city`            VARCHAR(64) NOT NULL COMMENT '实测最大 40 字节',
  `seller_state`           VARCHAR(2)  NOT NULL,
  `op`                     VARCHAR(1)  NOT NULL,
  `ts_ms`                  BIGINT      NOT NULL,
  `dt`                     DATE        NOT NULL
) ENGINE = OLAP
UNIQUE KEY(`seller_id`)
COMMENT '卖家：3,095 行'
DISTRIBUTED BY HASH(`seller_id`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 3. ods_products —— 商品（32,951 行）
-- -----------------------------------------------------------------------------
-- 610 行（1.85%）类目为空 → 该列必须允许 NULL。
-- SMALLINT 而不是 TINYINT：product_description_lenght 实测最大 3992，
-- TINYINT 上限 127 会直接溢出（报错或被静默截断，取决于 strict_mode）。
CREATE TABLE IF NOT EXISTS `ods_products` (
  `product_id`                 VARCHAR(32) NOT NULL,
  `product_category_name`      VARCHAR(64) NULL     COMMENT '葡语类目，610 行为 NULL；实测最大 46 字节',
  `product_name_lenght`        SMALLINT    NULL     COMMENT '源字段拼写如此；实测最大 76',
  `product_description_lenght` SMALLINT    NULL     COMMENT '实测最大 3992',
  `product_photos_qty`         TINYINT     NULL     COMMENT '实测最大 20',
  `product_weight_g`           INT         NULL     COMMENT '克；实测最大 40,425',
  `product_length_cm`          SMALLINT    NULL     COMMENT '实测最大 105',
  `product_height_cm`          SMALLINT    NULL     COMMENT '实测最大 105',
  `product_width_cm`           SMALLINT    NULL     COMMENT '实测最大 118',
  `op`                         VARCHAR(1)  NOT NULL,
  `ts_ms`                      BIGINT      NOT NULL,
  `dt`                         DATE        NOT NULL
) ENGINE = OLAP
UNIQUE KEY(`product_id`)
COMMENT '商品：32,951 行 / 73 个类目'
DISTRIBUTED BY HASH(`product_id`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 4. ods_category_translation —— 类目葡译英（71 行）
-- -----------------------------------------------------------------------------
-- 只有 71 行，但商品表有 73 个类目 → DWD 必须 LEFT JOIN + COALESCE。
-- 用 INNER JOIN 会静默丢 13 个商品、24 条订单行、GMV 5,514.48。
CREATE TABLE IF NOT EXISTS `ods_category_translation` (
  `product_category_name`         VARCHAR(64) NOT NULL,
  `product_category_name_english` VARCHAR(64) NOT NULL COMMENT '实测最大 39 字节',
  `op`                            VARCHAR(1)  NOT NULL,
  `ts_ms`                         BIGINT      NOT NULL,
  `dt`                            DATE        NOT NULL
) ENGINE = OLAP
UNIQUE KEY(`product_category_name`)
COMMENT '类目翻译：71 行（缺 2 个类目）'
DISTRIBUTED BY HASH(`product_category_name`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 5. ods_geolocation —— 邮编坐标（1,000,163 行）
-- -----------------------------------------------------------------------------
-- 全库最大的表，也是唯一有代理键的表。
-- UNIQUE(id) 而不是 DUPLICATE：作业重跑不会让行数翻倍，见文件头【决定二】。
-- DECIMAL(10,7) 与原表一致：实测 lat 最大 45.0659331、lng 最大 121.1053938，
-- 整数位 3 位，DECIMAL(10,7) 留 3 位整数 + 7 位小数，够用。
CREATE TABLE IF NOT EXISTS `ods_geolocation` (
  `id`                          BIGINT      NOT NULL COMMENT '自增代理键（源数据无唯一自然键）',
  `geolocation_zip_code_prefix` VARCHAR(5)  NOT NULL,
  `geolocation_lat`             DECIMAL(10, 7) NOT NULL,
  `geolocation_lng`             DECIMAL(10, 7) NOT NULL,
  `geolocation_city`            VARCHAR(64) NOT NULL COMMENT '实测最大 38 字节',
  `geolocation_state`           VARCHAR(2)  NOT NULL,
  `op`                          VARCHAR(1)  NOT NULL,
  `ts_ms`                       BIGINT      NOT NULL,
  `dt`                          DATE        NOT NULL
) ENGINE = OLAP
UNIQUE KEY(`id`)
COMMENT '邮编坐标：1,000,163 行 / 19,015 个邮编'
DISTRIBUTED BY HASH(`id`) BUCKETS 8
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 6. ods_orders —— 订单主表（99,441 行）
-- -----------------------------------------------------------------------------
-- 3 个时间列允许 NULL（未审批 160 行 / 未交承运商 1,783 行 / 未送达 2,965 行）。
-- Key 只有 order_id：customer_id 不做 Key，否则同一客户的多张订单会合并
-- （UNIQUE 模型是「同 Key 覆盖」，不是「同 Key 聚合」，这里只是提醒别把
--  维度列误放进 Key）。
CREATE TABLE IF NOT EXISTS `ods_orders` (
  `order_id`                      VARCHAR(32) NOT NULL,
  `customer_id`                   VARCHAR(32) NOT NULL,
  `order_status`                  VARCHAR(24) NOT NULL COMMENT '实测最大 11 字节',
  `order_purchase_timestamp`      DATETIME    NOT NULL COMMENT '事件时间主锚点',
  `order_approved_at`             DATETIME    NULL,
  `order_delivered_carrier_date`  DATETIME    NULL,
  `order_delivered_customer_date` DATETIME    NULL,
  `order_estimated_delivery_date` DATETIME    NOT NULL,
  `op`                            VARCHAR(1)  NOT NULL,
  `ts_ms`                         BIGINT      NOT NULL,
  `dt`                            DATE        NOT NULL
) ENGINE = OLAP
UNIQUE KEY(`order_id`)
COMMENT '订单主表：99,441 行（2016-09 ~ 2018-10）'
DISTRIBUTED BY HASH(`order_id`) BUCKETS 8
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 7. ods_order_items —— 订单明细（112,650 行）
-- -----------------------------------------------------------------------------
-- GMV 的真正来源。复合主键 (order_id, order_item_id)。
-- 分桶列用 order_id（基数高、且与订单表 join 的键一致，便于 colocate join）。
CREATE TABLE IF NOT EXISTS `ods_order_items` (
  `order_id`            VARCHAR(32)    NOT NULL,
  `order_item_id`       SMALLINT       NOT NULL COMMENT '订单内行号，实测最大 21',
  `product_id`          VARCHAR(32)    NOT NULL,
  `seller_id`           VARCHAR(32)    NOT NULL,
  `shipping_limit_date` DATETIME       NOT NULL,
  `price`               DECIMAL(18, 2) NOT NULL,
  `freight_value`       DECIMAL(18, 2) NOT NULL,
  `op`                  VARCHAR(1)     NOT NULL,
  `ts_ms`               BIGINT         NOT NULL,
  `dt`                  DATE           NOT NULL
) ENGINE = OLAP
UNIQUE KEY(`order_id`, `order_item_id`)
COMMENT '订单明细：112,650 行'
DISTRIBUTED BY HASH(`order_id`) BUCKETS 8
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 8. ods_order_payments —— 支付流水（103,886 行）
-- -----------------------------------------------------------------------------
-- payment_installments 有 2 行是 0（不是 1），所以下游不能用「>=1」做假设。
CREATE TABLE IF NOT EXISTS `ods_order_payments` (
  `order_id`             VARCHAR(32)    NOT NULL,
  `payment_sequential`   SMALLINT       NOT NULL COMMENT '实测最大 29',
  `payment_type`         VARCHAR(24)    NOT NULL COMMENT '实测最大 11 字节',
  `payment_installments` TINYINT        NOT NULL COMMENT '实测最大 24，有 2 行为 0',
  `payment_value`        DECIMAL(18, 2) NOT NULL,
  `op`                   VARCHAR(1)     NOT NULL,
  `ts_ms`                BIGINT         NOT NULL,
  `dt`                   DATE           NOT NULL
) ENGINE = OLAP
UNIQUE KEY(`order_id`, `payment_sequential`)
COMMENT '支付流水：103,886 行（一单可多笔）'
DISTRIBUTED BY HASH(`order_id`) BUCKETS 8
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 9. ods_order_reviews —— 评价（99,224 行）
-- -----------------------------------------------------------------------------
-- 复合主键 (review_id, order_id)：review_id 有 814 次重复，单列做 Key 会丢 814 条。
-- 评论正文实测最长 269 字节 / 135 字符，用 VARCHAR(1024) 留足余量
-- （MySQL 端是 VARCHAR(1024) 字符，Doris 端是字节，语义不同但容量都够）。
CREATE TABLE IF NOT EXISTS `ods_order_reviews` (
  `review_id`               VARCHAR(32)   NOT NULL,
  `order_id`                VARCHAR(32)   NOT NULL,
  `review_score`            TINYINT       NOT NULL COMMENT '1~5',
  `review_comment_title`    VARCHAR(128)  NULL     COMMENT '88.3% 为 NULL；实测最大 35 字节',
  `review_comment_message`  VARCHAR(1024) NULL     COMMENT '58.7% 为 NULL；实测最大 269 字节',
  `review_creation_date`    DATETIME      NOT NULL COMMENT '源数据只到天',
  `review_answer_timestamp` DATETIME      NOT NULL,
  `op`                      VARCHAR(1)    NOT NULL,
  `ts_ms`                   BIGINT        NOT NULL,
  `dt`                      DATE          NOT NULL
) ENGINE = OLAP
UNIQUE KEY(`review_id`, `order_id`)
COMMENT '评价：99,224 行（review_id 有 814 次重复）'
DISTRIBUTED BY HASH(`review_id`) BUCKETS 8
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 10. ods_user_event —— 埋点事件（Kafka → Doris）
-- -----------------------------------------------------------------------------
-- 唯一不走 CDC 的表，数据源是 Kafka topic `user_event`（JSON）。
-- event_id 是 Unique Key：Flink 作业重启后从上次 checkpoint 或最早位点重放，
-- 重复消费不会产生重复行——这是 P3 幂等验收的核心。
--
-- event_time 用 DATETIME(3) 保留毫秒：生成器输出到毫秒，漏斗按秒聚合时
-- 毫秒决定了同一秒内事件的先后，丢精度会让「加购早于浏览」这类脏数据出现。
-- dt 取 DATE(event_time) 而不是落库日期：埋点的事件时间本身就是业务时间，
-- 也是大屏按天聚合的分区依据。这是**唯一**一张 dt 取业务日期的表 ——
-- 另外 9 张 CDC 表的 dt 取 CURRENT_DATE（落地日），因为它们的快照行没有
-- 可信的源端时间戳（见文件头「关于 ts_ms = 0」）。
CREATE TABLE IF NOT EXISTS `ods_user_event` (
  `event_id`       VARCHAR(36)  NOT NULL COMMENT 'uuid，幂等键',
  `event_time`     DATETIME(3)  NOT NULL COMMENT '事件时间，Asia/Shanghai',
  `event_type`     VARCHAR(16)  NOT NULL COMMENT 'page_view/add_cart/checkout/pay',
  `user_id`        VARCHAR(32)  NOT NULL COMMENT 'customer_unique_id',
  `session_id`     VARCHAR(36)  NOT NULL,
  `product_id`     VARCHAR(32)  NULL     COMMENT '下单/支付页可能无商品',
  `page_url`       VARCHAR(128) NULL     COMMENT '实测最大 46 字节',
  `schema_version` INT          NOT NULL COMMENT '契约版本，当前 1',
  `dt`             DATE         NOT NULL COMMENT 'DATE(event_time)，事件日期',
  `ts_ms`          BIGINT       NOT NULL COMMENT '入库时间（毫秒），算链路延迟用'
) ENGINE = OLAP
UNIQUE KEY(`event_id`)
COMMENT '埋点事件：page_view → add_cart → checkout → pay'
DISTRIBUTED BY HASH(`event_id`) BUCKETS 8
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- =============================================================================
-- 建表完成。下一步：
--   1. 起 Doris（olap profile）→ 执行本文件 → SHOW TABLES 应有 10 张
--   2. 起 Flink（compute profile）→ 上传 3 个 jar（见 docs/04 §1）
--   3. 提交 CDC 作业 → 用 sql/doris/02-verify.sql 核对行数
-- =============================================================================
