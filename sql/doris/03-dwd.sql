-- =============================================================================
-- 03-dwd.sql —— DWD 明细层 7 张表
-- -----------------------------------------------------------------------------
-- 与 sql/doris/01-ods.sql 的区别只在两处，但两处都是硬约束：
--   1. DWD **事实表按年分区**，ODS 不分区。分区列 dt 必须不可变（见 docs/02 §3）
--   2. DWD **维表不分区**，全部小表
--
-- 分区键的选择是本文件最容易搞错的地方。dt 取「下单日期」而**不是**「装载日期」：
-- UNIQUE 表的唯一性只在单分区内保证，若 dt 可变，同一 order_id 今天更新落 2026
-- 分区、明天更新落 2027 分区，两次互不覆盖 → 同一订单两行 → GMV 凭空翻倍。
-- 下单日期在业务上永不变更，所以安全。
--
-- 执行：MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe \
--         mysql -h127.0.0.1 -P9030 -uroot < sql/doris/03-dwd.sql
-- =============================================================================

USE `realtime_dw`;


-- -----------------------------------------------------------------------------
-- 1. dwd_trade_order_detail —— 交易事实宽表（项目核心）
-- -----------------------------------------------------------------------------
-- 粒度 = 订单行（order_id + order_item_id），与 olist_order_items 一致，112,650 行。
--
-- 三个必须记住的设计约束：
--   * **支付必须先按订单聚合再关联**。一笔订单可能分期多次支付，直接 join 支付表
--     会把 112,650 行放大到 11 万+，验收标准「行数 = order_items 行数」当场失败。
--     这里存的是 payment_total / payment_count 两个聚合格度，不是支付流水明细。
--   * **类目用 LEFT JOIN + COALESCE**。ods_category_translation 只有 71 个类目，
--     源表有 73 个（缺 2 个），products 里还有 610 个商品类目为 NULL。
--     用 INNER JOIN 会静默丢掉 13 个商品、影响 GMV 5,514.48。
--   * **dt 进 Key**。Doris 要求分区列是 Key 列的子集，所以主键是三元组
--     (order_id, order_item_id, dt) 而不是 (order_id, order_item_id)。
--     dt 不可变，三元组的唯一性与二元组等价。
--
-- 派生字段 delivery_days / is_late 放在 DWD 而不是 DWS，因为它们依赖单行的
-- 两个时间戳，属于「行级派生」；放到 DWS 会让每个下游都重算一遍。
CREATE TABLE IF NOT EXISTS `dwd_trade_order_detail` (
  `order_id`                      VARCHAR(32)   NOT NULL COMMENT '订单 ID',
  `order_item_id`                 INT           NOT NULL COMMENT '订单行号，同一订单内从 1 递增',
  `dt`                            DATE          NOT NULL COMMENT '下单日期 DATE(order_purchase_timestamp)，分区列，不可变',

  -- 客户维度（来自 customers，按 customer_id 关联）
  `customer_id`                   VARCHAR(32)   COMMENT '订单级客户 ID，与 orders 1:1',
  `customer_unique_id`            VARCHAR(32)   COMMENT '自然人去重 ID，复购率/RFM 必须用它',
  `customer_city`                 VARCHAR(64)   COMMENT '客户所在城市',
  `customer_state`                VARCHAR(8)    COMMENT '客户所在州，2 字母',

  -- 订单维度
  `order_status`                  VARCHAR(20)   COMMENT '订单状态',
  `order_purchase_timestamp`      DATETIME      COMMENT '下单时间',
  `order_approved_at`             DATETIME      COMMENT '支付审核通过时间',
  `order_delivered_carrier_date`  DATETIME      COMMENT '交给承运商时间',
  `order_delivered_customer_date` DATETIME      COMMENT '客户签收时间',
  `order_estimated_delivery_date` DATETIME      COMMENT '预计送达日期',

  -- 商品维度（来自 products + category_translation）
  `product_id`                    VARCHAR(32)   COMMENT '商品 ID',
  `product_category_name`         VARCHAR(64)   COMMENT '葡语类目原名，可能为 NULL（610 个商品无类目）',
  `category_name_en`              VARCHAR(64)   COMMENT '英文类目，COALESCE 兜底为 unknown',

  -- 卖家维度（来自 sellers）
  `seller_id`                     VARCHAR(32)   COMMENT '卖家 ID',
  `seller_city`                   VARCHAR(64)   COMMENT '卖家所在城市',
  `seller_state`                  VARCHAR(8)    COMMENT '卖家所在州',

  -- 支付维度（按订单聚合后的结果，不是流水明细）
  --
  -- 这里**没有 payment_type 列**，是刻意的。初版 DDL 有过一列
  -- `VARCHAR(20) COMMENT '金额最大的那笔的支付方式'`，作业从头到尾没写过它，
  -- 于是那一列永远为 NULL。查下来不是漏写，是三个理由：
  --   1. Flink 1.18 没有 MAX_BY，「金额最大的那笔」取不到；
  --      退化用 MAX(payment_type) 是字典序最大，和金额无关，是假指标。
  --   2. 支付方式是**订单级的一对多属性**（一单可以是信用卡 + 优惠券），
  --      硬塞进订单行粒度的宽表，选哪个都是近似。
  --   3. 真正需要支付方式占比的地方在 DWS：`dws_payment_method_1d`
  --      直接从 ods_order_payments 聚合，那里是准确的（`sql/doris/05-dws.sql` §6）。
  -- 留下一个恒 NULL 的列比少一列更糟 —— 下游会以为「有值只是没填」。
  `payment_total`                 DECIMAL(18,2) COMMENT '该订单支付总额，多笔求和',
  `payment_count`                 INT           COMMENT '该订单支付笔数，>1 表示分期或组合支付',

  -- 金额与履约（行级）
  `price`                         DECIMAL(18,2) COMMENT '商品单价',
  `freight_value`                 DECIMAL(18,2) COMMENT '该行运费',
  `item_total`                    DECIMAL(18,2) COMMENT 'price + freight_value',
  `delivery_days`                 INT           COMMENT '下单到签收的天数，未签收为 NULL',
  `is_late`                       TINYINT       COMMENT '1=晚于预计送达，0=准时，NULL=未签收',

  `update_time`                   DATETIME      COMMENT '写入 Doris 的时刻'
) ENGINE = OLAP
UNIQUE KEY(`order_id`, `order_item_id`, `dt`)
COMMENT '交易事实宽表：订单行粒度，订单+客户+商品+卖家+支付打宽'
PARTITION BY RANGE(`dt`)
(
  -- 静态分区，不用动态分区：动态分区的 start=-30 会把 2016~2018 的历史分区当天回收，
  -- 现象是「导入成功但 COUNT(*)=0」（docs/02 §3 第 1 条）
  PARTITION `p2016` VALUES [("2016-01-01"), ("2017-01-01")),
  PARTITION `p2017` VALUES [("2017-01-01"), ("2018-01-01")),
  PARTITION `p2018` VALUES [("2018-01-01"), ("2019-01-01")),
  -- 实时新增订单落在这里。Olist 原始数据只到 2018-10，2026 是本次测试的写入年份。
  -- 跨年后需要手工 ALTER TABLE ADD PARTITION，这是静态分区的代价，换来的是
  -- 「历史分区永远不会被自动回收」这个保证。
  PARTITION `p2026` VALUES [("2026-01-01"), ("2027-01-01"))
)
DISTRIBUTED BY HASH(`order_id`) BUCKETS 8
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 2. dwd_user_event_clean —— 行为明细（清洗后）
-- -----------------------------------------------------------------------------
-- 与 ods_user_event 的差别是三件事：
--   ① 字段与白名单校验，结果写进 is_valid —— **不物理删除**，脏数据留在表里当证据，
--      DWS 聚合时带 `WHERE is_valid = 1` 即可。悄悄丢掉脏行是最糟的做法：
--      上游改字段名之后你会得到一张「没有脏数据」的空表，且无从发现。
--   ② event_id 为空的行硬过滤（没有主键的行既进不了表也没法去重），
--      丢了多少可以用 ods_user_event 行数减本表行数直接量出来。
--   ③ event_time 用 DATETIME(3) 保留毫秒 —— 同一秒内「浏览→加购」的先后顺序
--      全靠它，丢精度会让漏斗转化率算出负数。
--
-- **为什么没有 session_seq（试过，撤了）**
--   最初的设计里有一列 session_seq = 会话内按 event_time 的序号，用
--   ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time) 算。
--   实跑发现它有两个绕不过去的问题：
--
--   a) Flink 的流模式 OVER 聚合强制 ORDER BY 落在**时间属性**上，为此必须在
--      source 里给 event_time 加 WATERMARK。而水位线的定义决定了它永远落后于
--      最新数据：实测 topic 尾部 10:22~10:23 的 8 条支付事件，因为后面没有
--      新事件推动水位线，一直卡在 OVER 算子的状态里出不来 ——
--      22,064 条消息只落库 21,996 条，缺的正好是这 8 条。
--      生产环境持续有流量所以看不出来，离线回放就是实打实的缺数。
--   b) 序号只随到达顺序递增，不会回头修正。它「正确」的前提是同一会话的事件
--      严格有序到达 —— 这要靠生产端用 session_id 做消息 key 来保证，
--      是个隐式契约，换个上游就悄悄错了。
--
--   而序号本身并不需要在写入时固化：event_time 精确到毫秒且不可变，DWS 算漏斗
--   直接 `ORDER BY event_time` 就能拿到真实顺序，永远是准的。把排序固化进 DWD
--   是用一个会错的派生列，去换一个本来就不存在的性能问题。
--
-- 为什么是 UNIQUE KEY 而不是 DUPLICATE：
--   这张表的事件**直接从 Kafka 进来**（不经过 ods_user_event），而 Kafka 的
--   语义是 at-least-once。作业不 restore 重新提交时会从 earliest-offset 重放
--   全部历史消息，DUPLICATE 表会让行数成倍膨胀且无法回滚。用 UNIQUE(event_id)
--   把去重交给 Doris 的主键，重放多少次结果都恒定 —— 幂等性不依赖上游。
--
--   dt 取 DATE(event_time) 作分区列是安全的：事件时间一旦产生就不会变，
--   同一 event_id 永远落在同一分区，不会出现「同一主键跨两个分区、互不覆盖」。
--
--   注意 dt 在 schema 里排第 2 —— 不是随便排的。Doris 强制**主键列必须是有序前缀**，
--   而分区列必须在主键里，所以 (event_id, dt) 这个顺序只能这么落。
--   dwd_trade_order_detail 的 (order_id, order_item_id, dt) 也是同一个约束的产物。
CREATE TABLE IF NOT EXISTS `dwd_user_event_clean` (
  `event_id`        VARCHAR(40)   NOT NULL COMMENT '事件 ID，也是去重主键',
  `dt`              DATE          NOT NULL COMMENT '事件日期 DATE(event_time)，分区列；位置由主键前缀约束决定',
  `user_id`         VARCHAR(32)   COMMENT 'customer_unique_id',
  `session_id`      VARCHAR(40)   COMMENT '会话 ID',
  `product_id`      VARCHAR(32)   COMMENT '商品 ID',
  `event_type`      VARCHAR(20)   COMMENT 'page_view / add_cart / checkout / pay',
  `event_time`      DATETIME(3)   COMMENT '事件时间，保留毫秒（同秒内先后顺序靠它，也是漏斗排序依据）',
  `page_url`        VARCHAR(255)  COMMENT '页面地址',
  -- 清洗期的派生列
  `is_valid`        TINYINT       COMMENT '1=通过校验，0=字段缺失/类型非法（不物理删除，留证据）',
  `update_time`     DATETIME      COMMENT '写入时刻'
) ENGINE = OLAP
UNIQUE KEY(`event_id`, `dt`)
COMMENT '行为明细：清洗后的事件流，含校验标记'
PARTITION BY RANGE(`dt`)
(
  PARTITION `p2026` VALUES [("2026-01-01"), ("2027-01-01"))
)
DISTRIBUTED BY HASH(`event_id`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 3. dim_seller —— 卖家维（全量覆盖，不做拉链）
-- -----------------------------------------------------------------------------
-- 为什么不做 SCD2：卖家属性只有城市/州，Olist 数据里不存在变更历史，
-- 强行做拉链只会得到一张每行 is_current=1 的宽表，没有信息增量。
-- 如果真实项目里卖家等级、评分会变，才值得上拉链。
CREATE TABLE IF NOT EXISTS `dim_seller` (
  `seller_id`              VARCHAR(32) NOT NULL COMMENT '卖家 ID',
  `seller_zip_code_prefix` VARCHAR(5)  COMMENT '邮编前 5 位',
  `seller_city`            VARCHAR(64) COMMENT '城市',
  `seller_state`           VARCHAR(8)  COMMENT '州，2 字母',
  `update_time`            DATETIME    COMMENT '写入时刻'
) ENGINE = OLAP
UNIQUE KEY(`seller_id`)
COMMENT '卖家维表，全量覆盖'
DISTRIBUTED BY HASH(`seller_id`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 4. dim_geo —— 邮编维（一个邮编多个坐标点，去重成一对一）
-- -----------------------------------------------------------------------------
-- ods_geolocation 是 1,000,163 行、19,015 个邮编，平均一个邮编 52 个坐标点。
-- 直接拿它 join 事实表会让行数放大 52 倍。这里按邮编取坐标**均值**作为中心点，
-- 并保留点数 point_cnt 用于判断该中心点的可信度（点数少说明样本不足）。
--
-- 城市/州取该邮编下出现次数最多的值（近似众数）。Doris 没有 MODE 聚合函数，
-- 用「按邮编+城市分组计数，再取每组最大」的两段式实现，见 04-dwd-load.sql。
CREATE TABLE IF NOT EXISTS `dim_geo` (
  `zip_prefix`  VARCHAR(5)    NOT NULL COMMENT '邮编前 5 位',
  `lat`         DECIMAL(10,6) COMMENT '纬度均值（中心点）',
  `lng`         DECIMAL(10,6) COMMENT '经度均值（中心点）',
  `point_cnt`   INT           COMMENT '该邮编下的原始坐标点数，越小越不可信',
  `city`        VARCHAR(64)   COMMENT '出现次数最多的城市',
  `state`       VARCHAR(8)    COMMENT '出现次数最多的州',
  `update_time` DATETIME      COMMENT '写入时刻'
) ENGINE = OLAP
UNIQUE KEY(`zip_prefix`)
COMMENT '邮编维表：geolocation 按邮编去重后的中心点'
DISTRIBUTED BY HASH(`zip_prefix`) BUCKETS 2
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 5. dim_date —— 日历维（公共维度，全层复用）
-- -----------------------------------------------------------------------------
-- 覆盖 2016-01-01 ~ 2027-12-31，共 4,383 天。不按数据范围裁剪，而是给足前后余量：
-- DWS 做「同比/环比」时要往前找 365 天，日历表缺日期会让 join 静默丢行。
CREATE TABLE IF NOT EXISTS `dim_date` (
  `dt`          DATE        NOT NULL COMMENT '日期',
  `year`        INT         COMMENT '年',
  `quarter`     INT         COMMENT '季度 1~4',
  `month`       INT         COMMENT '月 1~12',
  `day`         INT         COMMENT '日 1~31',
  `week_of_year` INT        COMMENT '一年中的第几周 1~53',
  `weekday`     INT         COMMENT '星期几，1=周一 7=周日（ISO 8601）',
  `is_weekend`  TINYINT     COMMENT '1=周六周日',
  `date_str`    VARCHAR(10) COMMENT 'yyyy-MM-dd，便于字符串拼接'
) ENGINE = OLAP
UNIQUE KEY(`dt`)
COMMENT '日历维表，2016-01-01 ~ 2027-12-31'
DISTRIBUTED BY HASH(`dt`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 6. dim_product_zip —— 商品维 SCD2 拉链
-- -----------------------------------------------------------------------------
-- 拉链表的三个标配列：start_date / end_date / is_current。
--   * 新版本插入时，先把旧版本的 end_date 补上、is_current 置 0，再插入新版本
--   * 查询当前态必须带 is_current = 1，否则一条商品会匹配多行
--   * end_date 用 '9999-12-31 23:59:59' 而不是 NULL，是为了让
--     BETWEEN start_date AND end_date 这种写法不需要额外判空
--
-- 主键含 start_date：同一 product_id 可以有多个历史版本，唯一性靠
-- (product_id, start_date) 保证。这是拉链表与普通维表在 Doris 上的核心区别。
CREATE TABLE IF NOT EXISTS `dim_product_zip` (
  `product_id`                VARCHAR(32) NOT NULL COMMENT '商品 ID',
  `start_date`                DATETIME    NOT NULL COMMENT '该版本生效时间',
  `product_category_name`     VARCHAR(64) COMMENT '葡语类目名',
  `category_name_en`          VARCHAR(64) COMMENT '英文类目名',
  `product_name_length`       INT         COMMENT '商品名长度',
  `product_description_length` INT        COMMENT '描述长度',
  `product_photos_qty`        INT         COMMENT '图片数',
  `product_weight_g`          INT         COMMENT '重量（克）',
  `product_length_cm`         INT         COMMENT '长（厘米）',
  `product_height_cm`         INT         COMMENT '高（厘米）',
  `product_width_cm`          INT         COMMENT '宽（厘米）',
  `end_date`                  DATETIME    COMMENT '该版本失效时间，当前版本为 9999-12-31 23:59:59',
  `is_current`                TINYINT     COMMENT '1=当前版本'
) ENGINE = OLAP
UNIQUE KEY(`product_id`, `start_date`)
COMMENT '商品维 SCD2 拉链表'
DISTRIBUTED BY HASH(`product_id`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 7. dim_user_zip —— 用户维 SCD2 拉链（粒度是 customer_unique_id）
-- -----------------------------------------------------------------------------
-- **不能用 customer_id**：源数据里 99,441 个 customer_id 对应 99,441 个订单，是订单级
-- 主键；真正的人只有 96,096 个。按 customer_id 建拉链，每个客户只在下单那一刻出现
-- 一次、再无变更，拉链退化成订单快照，复购率与 RFM 全部失效（docs/02 §1）。
--
-- 差的那 3,345 就是复购人群，是本项目所有用户价值指标的分母。
CREATE TABLE IF NOT EXISTS `dim_user_zip` (
  `customer_unique_id` VARCHAR(32) NOT NULL COMMENT '自然人 ID，去重后 96,096 个',
  `start_date`         DATETIME    NOT NULL COMMENT '该版本生效时间',
  `customer_zip_code_prefix` VARCHAR(5) COMMENT '邮编前 5 位',
  `customer_city`      VARCHAR(64) COMMENT '城市',
  `customer_state`     VARCHAR(8)  COMMENT '州，2 字母',
  `end_date`           DATETIME    COMMENT '该版本失效时间，当前版本为 9999-12-31 23:59:59',
  `is_current`         TINYINT     COMMENT '1=当前版本'
) ENGINE = OLAP
UNIQUE KEY(`customer_unique_id`, `start_date`)
COMMENT '用户维 SCD2 拉链表，粒度 customer_unique_id'
DISTRIBUTED BY HASH(`customer_unique_id`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);
