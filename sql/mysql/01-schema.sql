-- =============================================================================
-- 01-schema.sql — Olist 巴西电商 业务库（贴源层数据源）
-- -----------------------------------------------------------------------------
-- 定位：这是 MySQL 8.0.36 里的「源业务库」，不是数仓层。
--      9 张表与 data/raw/olist 下 9 个 CSV 一一对应，字段名保持源样
--      （含源端的拼写错误），不加审计列，不做业务校验。
--      所有加工都发生在 Doris（ODS → DWD → DWS → ADS），不在这里做。
--
-- 执行：mysql -uroot -p --default-character-set=utf8mb4 < 01-schema.sql
--
-- 三条硬约束（改之前先看 docs/02 §4）：
--   1. 金额一律 DECIMAL(18,2)，禁止 FLOAT/DOUBLE —— 浮点误差会让对账永远差几分钱
--   2. 时间一律 DATETIME，不用 TIMESTAMP —— TIMESTAMP 会按时区换算且 2038 溢出，
--      CDC 抽到 Doris 后时间会整体偏移 8 小时，是极难查的一类问题
--   3. 邮编一律 CHAR(5) —— 24.1% 的邮编以 0 开头（如 09790），一旦按整数处理
--      就变成 9790，之后按邮编 join 会静默丢数据，不报错
--
-- 刻意不做的两件事：
--   * 不加外键：LOAD DATA 批量导入时外键检查代价高，且 9 个 CSV 没有固定导入顺序
--   * 不加 CHECK：贴源层的职责是「原样收下」。脏数据必须能进得来，否则 P6 的
--     数据质量演示（注入 3% 重复/异常数据）就没有素材可造
--
-- 导入须知（写 import 脚本时必须处理，否则报 1292 / 1406）：
--   * 空字符串写进 DATETIME 列会报错，必须用 NULLIF(@var,'') 转成 NULL
--   * order_reviews 的 comment 字段含换行符，必须按 CSV 引号规则解析：
--     按行数它是 104,719 行，按记录数只有 99,224 条
--   * 城市名含重音字符（ã / ç / ´），客户端字符集必须 utf8mb4，否则写入报 1366
-- =============================================================================

SET NAMES utf8mb4;

CREATE DATABASE IF NOT EXISTS `olist`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE `olist`;


-- -----------------------------------------------------------------------------
-- 1. customers —— 客户
-- -----------------------------------------------------------------------------
-- 关键事实：customer_id 是「订单级」的，与 orders 1:1（99,441 个 customer_id
-- 对应 99,441 个订单）。真正的「人」是 customer_unique_id，去重后 96,096 个。
-- 所以 RFM、复购率必须用 customer_unique_id 分组，用 customer_id 算出来永远是 1。
CREATE TABLE `customers` (
  `customer_id`              VARCHAR(32) NOT NULL COMMENT '客户ID（订单级，一单一客户）',
  `customer_unique_id`       VARCHAR(32) NOT NULL COMMENT '真实客户ID（跨订单唯一）',
  `customer_zip_code_prefix` CHAR(5)     NOT NULL COMMENT '邮编前5位，保留前导零',
  `customer_city`            VARCHAR(64) NOT NULL,
  `customer_state`           CHAR(2)     NOT NULL COMMENT '巴西州缩写，如 SP / RJ',
  PRIMARY KEY (`customer_id`),
  KEY `idx_unique_id` (`customer_unique_id`),
  KEY `idx_state` (`customer_state`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT = '客户维表：99,441 行 / 96,096 个自然人';


-- -----------------------------------------------------------------------------
-- 2. sellers —— 卖家
-- -----------------------------------------------------------------------------
CREATE TABLE `sellers` (
  `seller_id`              VARCHAR(32) NOT NULL,
  `seller_zip_code_prefix` CHAR(5)     NOT NULL COMMENT '邮编前5位',
  `seller_city`            VARCHAR(64) NOT NULL,
  `seller_state`           CHAR(2)     NOT NULL,
  PRIMARY KEY (`seller_id`),
  KEY `idx_state` (`seller_state`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT = '卖家维表：3,095 行';


-- -----------------------------------------------------------------------------
-- 3. products —— 商品
-- -----------------------------------------------------------------------------
-- 字段名 lenght 是源数据自带的拼写错误，贴源层照抄不改，重命名放到 DWD 做。
-- 610 行（1.85%）类目为空，2 行尺寸/重量为空，因此这些列必须允许 NULL。
CREATE TABLE `products` (
  `product_id`                 VARCHAR(32)      NOT NULL,
  `product_category_name`      VARCHAR(64)      NULL COMMENT '葡语类目名，610 行为 NULL',
  `product_name_lenght`        SMALLINT UNSIGNED NULL COMMENT '源字段拼写如此（lenght）',
  `product_description_lenght` SMALLINT UNSIGNED NULL COMMENT '源字段拼写如此（lenght）',
  `product_photos_qty`         TINYINT UNSIGNED  NULL,
  `product_weight_g`           INT UNSIGNED      NULL COMMENT '克，实测 0 ~ 40,425',
  `product_length_cm`          SMALLINT UNSIGNED NULL,
  `product_height_cm`          SMALLINT UNSIGNED NULL,
  `product_width_cm`           SMALLINT UNSIGNED NULL,
  PRIMARY KEY (`product_id`),
  KEY `idx_category` (`product_category_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT = '商品维表：32,951 行 / 73 个类目';


-- -----------------------------------------------------------------------------
-- 4. category_translation —— 类目葡译英
-- -----------------------------------------------------------------------------
-- 只有 71 行，但商品表有 73 个类目：pc_gamer、portateis_cozinha_e_preparadores_
-- de_alimentos 这两个没有英文名。DWD 关联时必须 LEFT JOIN + COALESCE，
-- 用 INNER JOIN 会静默丢商品。
CREATE TABLE `category_translation` (
  `product_category_name`         VARCHAR(64) NOT NULL COMMENT '葡语类目名',
  `product_category_name_english` VARCHAR(64) NOT NULL COMMENT '英文类目名',
  PRIMARY KEY (`product_category_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT = '类目翻译：71 行（缺 2 个类目）';


-- -----------------------------------------------------------------------------
-- 5. geolocation —— 邮编坐标
-- -----------------------------------------------------------------------------
-- 这是全库唯一没有自然主键的表：(邮编, 纬度, 经度) 三元组有 280,009 条重复，
-- 同一三元组最多重复 314 次（同一邮编下密集采样）。因此：
--   * 用自增代理键做主键，保证 100 万行一条不丢
--   * 去重逻辑放到 DWD 的 dim_geo（按邮编取中心点），ODS 保持原始粒度
-- 顺带好处：Flink CDC 做增量快照时可以直接按自增主键切分 chunk，比无主键表快得多。
-- 导入时必须显式列出字段，把 id 排除在外，否则 5 列对 6 列会报 "Column count doesn't match"：
--   LOAD DATA ... INTO TABLE geolocation
--     (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
--      geolocation_city, geolocation_state);
CREATE TABLE `geolocation` (
  `id`                          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '代理键（源数据无唯一自然键）',
  `geolocation_zip_code_prefix` CHAR(5)         NOT NULL COMMENT '邮编前5位',
  `geolocation_lat`             DECIMAL(10, 7)  NOT NULL COMMENT '纬度，原始14位小数，约1cm精度足够',
  `geolocation_lng`             DECIMAL(10, 7)  NOT NULL COMMENT '经度',
  `geolocation_city`            VARCHAR(64)     NOT NULL,
  `geolocation_state`           CHAR(2)         NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_zip` (`geolocation_zip_code_prefix`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT = '邮编坐标：1,000,163 行 / 19,015 个邮编';


-- -----------------------------------------------------------------------------
-- 6. orders —— 订单主表
-- -----------------------------------------------------------------------------
-- 三个送达时间列有 0.16% / 1.79% / 2.98% 的空值（未审批、未发货、未送达），
-- 必须允许 NULL，否则导入直接报 1292。
CREATE TABLE `orders` (
  `order_id`                      VARCHAR(32) NOT NULL,
  `customer_id`                   VARCHAR(32) NOT NULL COMMENT '与 orders 1:1',
  `order_status`                  VARCHAR(24) NOT NULL COMMENT 'delivered/shipped/canceled/...',
  `order_purchase_timestamp`      DATETIME    NOT NULL COMMENT '下单时间，事件时间主锚点',
  `order_approved_at`             DATETIME    NULL COMMENT '160 行为 NULL（未审批）',
  `order_delivered_carrier_date`  DATETIME    NULL COMMENT '1,783 行为 NULL（未交承运商）',
  `order_delivered_customer_date` DATETIME    NULL COMMENT '2,965 行为 NULL（未送达）',
  `order_estimated_delivery_date` DATETIME    NOT NULL COMMENT '预计送达，算 is_late 的基准',
  PRIMARY KEY (`order_id`),
  KEY `idx_customer` (`customer_id`),
  KEY `idx_status` (`order_status`),
  KEY `idx_purchase_time` (`order_purchase_timestamp`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT = '订单主表：99,441 行（2016-09 ~ 2018-10）';


-- -----------------------------------------------------------------------------
-- 7. order_items —— 订单明细
-- -----------------------------------------------------------------------------
CREATE TABLE `order_items` (
  `order_id`            VARCHAR(32)     NOT NULL,
  `order_item_id`       SMALLINT UNSIGNED NOT NULL COMMENT '订单内行号，从1开始，最大21',
  `product_id`          VARCHAR(32)     NOT NULL,
  `seller_id`           VARCHAR(32)     NOT NULL,
  `shipping_limit_date` DATETIME        NOT NULL COMMENT '卖家发货截止时间',
  `price`               DECIMAL(18, 2)  NOT NULL COMMENT '单价',
  `freight_value`       DECIMAL(18, 2)  NOT NULL COMMENT '运费',
  PRIMARY KEY (`order_id`, `order_item_id`),
  KEY `idx_product` (`product_id`),
  KEY `idx_seller` (`seller_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT = '订单明细：112,650 行（GMV 的真正来源）';


-- -----------------------------------------------------------------------------
-- 8. order_payments —— 支付流水
-- -----------------------------------------------------------------------------
-- payment_installments 有 2 行是 0（不是 1），所以不能用「>=1」做假设。
CREATE TABLE `order_payments` (
  `order_id`             VARCHAR(32)      NOT NULL,
  `payment_sequential`   SMALLINT UNSIGNED NOT NULL COMMENT '同单内支付序号，最大29',
  `payment_type`         VARCHAR(24)      NOT NULL COMMENT 'credit_card/boleto/voucher/debit_card',
  `payment_installments` TINYINT UNSIGNED  NOT NULL COMMENT '分期数，2 行为 0，最大 24',
  `payment_value`        DECIMAL(18, 2)   NOT NULL COMMENT '该笔支付金额',
  PRIMARY KEY (`order_id`, `payment_sequential`),
  KEY `idx_type` (`payment_type`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT = '支付流水：103,886 行（一单可多笔）';


-- -----------------------------------------------------------------------------
-- 9. order_reviews —— 评价
-- -----------------------------------------------------------------------------
-- 重要：review_id 不唯一（814 次重复，最多重复 3 次），单靠它会撞主键导致导入
-- 直接失败。实测 (review_id, order_id) 组合 0 重复，故用复合主键。
-- docs/02 里 ODS 层的主键定义已同步改为 review_id + order_id。
-- 另注：review_creation_date 全部是 00:00:00，源系统只记到天。
CREATE TABLE `order_reviews` (
  `review_id`               VARCHAR(32)   NOT NULL,
  `order_id`                VARCHAR(32)   NOT NULL,
  `review_score`            TINYINT UNSIGNED NOT NULL COMMENT '1~5',
  `review_comment_title`    VARCHAR(128)  NULL COMMENT '88.3% 为 NULL',
  `review_comment_message`  VARCHAR(1024) NULL COMMENT '58.7% 为 NULL，含换行符',
  `review_creation_date`    DATETIME      NOT NULL COMMENT '源数据只到天',
  `review_answer_timestamp` DATETIME      NOT NULL,
  PRIMARY KEY (`review_id`, `order_id`),
  KEY `idx_order` (`order_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT = '评价：99,224 行（review_id 有 814 次重复）';


-- =============================================================================
-- 建表完成。下一步：scripts/import_olist.* 按表导入 CSV，
-- 再用 sql/mysql/02-verify.sql 核对 9 张表行数是否与 CSV 一致（P2 验收）。
-- =============================================================================
