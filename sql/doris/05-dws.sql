-- =============================================================================
-- 05-dws.sql —— DWS 汇总层 6 张表
-- -----------------------------------------------------------------------------
-- 执行：MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe \
--         mysql -h127.0.0.1 -P9030 -uroot < sql/doris/05-dws.sql
--
-- =============================================================================
-- 为什么全部用 UNIQUE KEY，而不是 docs/02 原定的 AGGREGATE
-- =============================================================================
-- 原方案写的是 AGGREGATE + `SUM`/`REPLACE` 写入时预聚合。这里的 SUM 列有个前提
-- 被忽略了：**它要求每个 key 的每一行只被导入一次**。一旦同一条聚合结果被重放
-- 两次，SUM 列会把两次的值加在一起，数字直接翻倍。
--
-- 重放在这条链路上不是小概率事件——P4 单是调 Doris connector 的死锁就重跑了
-- 7 次作业；checkpoint 回滚、TM failover、手工补数都会触发。AGGREGATE 的 SUM
-- 列对此毫无防御，而 Doris 的 label 去重只在 2PC 场景下可靠，connector 24.0.1
-- 的 2PC 又和 P4 踩过的那个 Stream Load 死锁在同一段代码上，不想再赌一次。
--
-- 改用 UNIQUE KEY（Merge-on-Write）后，语义变成「同一主键只保留最新一行」。
-- 配合 Flink 侧 SinkUpsertMaterializer，每次下发的都是该 key 的**全量新值**
-- （不是增量），所以重放多少次都收敛到同一个结果。幂等性放在「值」上，
-- 不依赖写入协议，也不依赖 checkpoint 是否恰好成功。
--
-- 代价是失去了 Doris 的写入时预聚合。但 DWS 本身的基数很小（最大的分钟表
-- 8.8 万行），ADS 和大屏在这种量级上做 GROUP BY 是毫秒级，没有预聚合的必要。
-- 用一份确定正确的数据换一个用不上的优化，划算。
--
-- =============================================================================
-- 三处口径必须在表上写清楚，否则下游一定会算错
-- =============================================================================
--   1. **GMV 口径** = SUM(item_total) = SUM(price + freight_value)，含运费。
--      基准线 15,843,553.24（docs/02）。整条链路上唯一允许的 GMV 定义。
--   2. **订单数不能用 DWD 宽表反推**。宽表是订单行粒度，且不含 775 个无明细的
--      订单（603 unavailable + 164 canceled）——它们的 830 笔支付、162,591.95 元
--      不进宽表。所以本层所有 order_cnt 都是「**有商品行的订单数**」，
--      docs/02 §1 已声明这是粒度决定的系统性差异，不是数据质量问题。
--   3. **avg / 率值一律不落表**。客单价 = gmv/order_cnt、延迟发货率 =
--      late_cnt/delivered_cnt，都留给查询时除。存进来会被下游 SUM 出假指标。
-- =============================================================================

USE `realtime_dw`;


-- -----------------------------------------------------------------------------
-- 1. dws_trade_overview_1m —— 分钟级交易概览（大屏 KPI + 成交趋势）
-- -----------------------------------------------------------------------------
-- 8.8 万行（616 个下单日 × 平均 143 个有单分钟）。
--
-- `minute` 存 'HH:mm' 而不是 0-59 的分钟数：只存分钟会让「10:30」和「11:30」
-- 撞成同一个 key，一天只剩 60 个桶。带上小时才是真正的分钟粒度。
--
-- 时间轴取 **下单时间** order_purchase_timestamp，不是入湖时间。取入湖时间的
-- 话，11 万行历史明细会在 6 秒内全部落进同一个分钟桶，大屏只有一根柱子。
--
-- 三列去重指标（order_cnt / buyer_cnt / pay_user_cnt）在流里靠 COUNT(DISTINCT)
-- 维护。这个粒度下每桶只有 1.3 个订单行，去重集合极小，状态放得下。
CREATE TABLE IF NOT EXISTS `dws_trade_overview_1m` (
  `dt`             DATE          NOT NULL COMMENT '下单日期，分区列',
  `minute`         VARCHAR(5)    NOT NULL COMMENT 'HH:mm，下单时间在一天内的分钟桶',

  `gmv`            DECIMAL(20,2) COMMENT 'GMV = SUM(item_total)，含运费',
  `freight_total`  DECIMAL(20,2) COMMENT 'SUM(freight_value)，算运费占比用',
  `item_cnt`       BIGINT        COMMENT '订单行数 COUNT(*)',
  `order_cnt`      BIGINT        COMMENT '有商品行的订单数 COUNT(DISTINCT order_id)',
  `buyer_cnt`      BIGINT        COMMENT '下单用户数 COUNT(DISTINCT customer_unique_id)',
  `pay_user_cnt`   BIGINT        COMMENT '其中存在支付记录的用户数（payment_total > 0）',

  `update_time`    DATETIME      COMMENT '写入时刻'
) ENGINE = OLAP
UNIQUE KEY(`dt`, `minute`)
COMMENT '分钟级交易概览'
PARTITION BY RANGE(`dt`) (
  PARTITION `p2016` VALUES [("2016-01-01"), ("2017-01-01")),
  PARTITION `p2017` VALUES [("2017-01-01"), ("2018-01-01")),
  PARTITION `p2018` VALUES [("2018-01-01"), ("2019-01-01")),
  PARTITION `p2026` VALUES [("2026-01-01"), ("2027-01-01"))
)
DISTRIBUTED BY HASH(`minute`) BUCKETS 8
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 2. dws_trade_category_1d —— 类目日汇总（大屏品类排行）
-- -----------------------------------------------------------------------------
-- 18,990 行（616 天 × 平均 31 个有单类目）。
--
-- 维度用 category_name_en（英文类目），不是 product_category_name：后者是葡语
-- 原名，有 610 个商品为空、还有 2 个类目在翻译表里缺失。DWD 已经用
-- COALESCE(en, 原名, 'unknown') 兜过底，这里直接用即可，不会再出现 NULL 分组。
--
-- 客单价不落表 —— 客单价 = gmv / order_cnt，存进来会被 ADS 当成可加列 SUM。
--
-- **`order_cnt` 在这张表里跨类目不可加**（卖家表同理）。一笔订单买了 3 个类目的
-- 商品，会在 3 个类目行里各计 1 次。实测：
--   SUM(order_cnt) over 类目 = 99,470
--   SUM(order_cnt) over 卖家 = 100,010
--   COUNT(DISTINCT order_id) 全表 = 98,666
-- 前者大于后者不是 bug。要"总订单数"只能取 overview_1m 或直接查 DWD；
-- 地域表因为一单只对应一个买家州，恰好可加（也是 98,666），别把这个巧合当规律。
CREATE TABLE IF NOT EXISTS `dws_trade_category_1d` (
  `dt`             DATE          NOT NULL COMMENT '下单日期，分区列',
  `category_name`  VARCHAR(64)   NOT NULL COMMENT '英文类目名，已 COALESCE 兜底为 unknown',

  `gmv`            DECIMAL(20,2) COMMENT '类目 GMV',
  `freight_total`  DECIMAL(20,2) COMMENT '类目运费',
  `item_cnt`       BIGINT        COMMENT '商品件数',
  `order_cnt`      BIGINT        COMMENT '有商品行的订单数',
  `seller_cnt`     BIGINT        COMMENT '该日该品类下有出货的卖家家数',

  `update_time`    DATETIME      COMMENT '写入时刻'
) ENGINE = OLAP
UNIQUE KEY(`dt`, `category_name`)
COMMENT '类目日汇总'
PARTITION BY RANGE(`dt`) (
  PARTITION `p2016` VALUES [("2016-01-01"), ("2017-01-01")),
  PARTITION `p2017` VALUES [("2017-01-01"), ("2018-01-01")),
  PARTITION `p2018` VALUES [("2018-01-01"), ("2019-01-01")),
  PARTITION `p2026` VALUES [("2026-01-01"), ("2027-01-01"))
)
DISTRIBUTED BY HASH(`category_name`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 3. dws_trade_seller_1d —— 卖家日汇总（履约质量）
-- -----------------------------------------------------------------------------
-- 69,413 行（616 天 × 平均 113 个有单卖家）。
--
-- 履约三态是这张表最容易算错的地方。is_late 有三个取值：
--   1   = 签收晚于预计送达
--   0   = 签收不晚于预计
--   NULL = 还没签收（未妥投）
-- **NULL 既不是准时也不是迟到**。所以：
--   * late_cnt     只累加 is_late = 1
--   * delivered_cnt 只数 is_late IS NOT NULL（已签收）
--   * 延迟发货率 = late_cnt / delivered_cnt，分母不能写成 order_cnt
-- 把未签收算成准时会让延迟率凭空腰斩 —— 实测 8,715 迟到 / 112,650 总行 = 7.74%，
-- 而正确口径是 8,715 / 110,196 已签收 = 7.91%。
--
-- avg_delivery_days 同理，sum 的时候只能加已签收行的 delivery_days。
CREATE TABLE IF NOT EXISTS `dws_trade_seller_1d` (
  `dt`             DATE          NOT NULL COMMENT '下单日期，分区列',
  `seller_id`      VARCHAR(64)   NOT NULL COMMENT '卖家 ID',

  `gmv`            DECIMAL(20,2) COMMENT '卖家 GMV',
  `item_cnt`       BIGINT        COMMENT '出货件数',
  `order_cnt`      BIGINT        COMMENT '有商品行的订单数',

  `delivered_cnt`  BIGINT        COMMENT '已签收的订单行数（is_late IS NOT NULL）',
  `late_cnt`       BIGINT        COMMENT '迟到的订单行数（is_late = 1）',
  `sum_delivery_days` BIGINT     COMMENT '已签收行的 delivery_days 之和，除 delivered_cnt 得平均履约天数',

  `update_time`    DATETIME      COMMENT '写入时刻'
) ENGINE = OLAP
UNIQUE KEY(`dt`, `seller_id`)
COMMENT '卖家日汇总'
PARTITION BY RANGE(`dt`) (
  PARTITION `p2016` VALUES [("2016-01-01"), ("2017-01-01")),
  PARTITION `p2017` VALUES [("2017-01-01"), ("2018-01-01")),
  PARTITION `p2018` VALUES [("2018-01-01"), ("2019-01-01")),
  PARTITION `p2026` VALUES [("2026-01-01"), ("2027-01-01"))
)
DISTRIBUTED BY HASH(`seller_id`) BUCKETS 8
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 4. dws_trade_region_1d —— 地域日汇总（大屏地图 + 运费占比）
-- -----------------------------------------------------------------------------
-- 10,701 行（616 天 × 平均 17 个有单州）。巴西 27 个州，只有 17 个有单量。
--
-- 维度用 customer_state 而不是 seller_state：GMV 归因到**买家所在州**，
-- 这才对应大屏上「哪里在消费」。卖家州是供给视角，两者不能混在一张表里。
--
-- 运费占比 = freight_total / gmv，同样不落表。
CREATE TABLE IF NOT EXISTS `dws_trade_region_1d` (
  `dt`             DATE          NOT NULL COMMENT '下单日期，分区列',
  `customer_state` VARCHAR(8)    NOT NULL COMMENT '买家所在州，两位缩写',

  `gmv`            DECIMAL(20,2) COMMENT '地域 GMV',
  `freight_total`  DECIMAL(20,2) COMMENT '地域运费',
  `item_cnt`       BIGINT        COMMENT '商品件数',
  `order_cnt`      BIGINT        COMMENT '有商品行的订单数',
  `buyer_cnt`      BIGINT        COMMENT '下单用户数',

  `update_time`    DATETIME      COMMENT '写入时刻'
) ENGINE = OLAP
UNIQUE KEY(`dt`, `customer_state`)
COMMENT '地域日汇总'
PARTITION BY RANGE(`dt`) (
  PARTITION `p2016` VALUES [("2016-01-01"), ("2017-01-01")),
  PARTITION `p2017` VALUES [("2017-01-01"), ("2018-01-01")),
  PARTITION `p2018` VALUES [("2018-01-01"), ("2019-01-01")),
  PARTITION `p2026` VALUES [("2026-01-01"), ("2027-01-01"))
)
DISTRIBUTED BY HASH(`customer_state`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 5. dws_traffic_funnel_1h —— 小时级转化漏斗
-- -----------------------------------------------------------------------------
-- 只有几十行（2 个事件日期 × 2 个小时段 × 4 个环节），是最小的表。
--
-- UV 用 user_id 去重 —— 埋点里的 user_id 填的是 customer_unique_id（自然人），
-- 不是 customer_id（订单级）。两者在 DWD 里分列存着，别串了。
--
-- `step` 存环节名而不是序号：序号要额外维护一张映射表，且一旦环节顺序变了，
-- 历史数据的序号含义就变了。环节名不可变，天然自解释。
--
-- **这张表的数据来自埋点流，不来自交易宽表**，所以它的 dt 是 event_time 的日期，
-- 与上面 4 张表的 dt（下单日期）不是同一个时间轴。两个体系不要关联合并。
CREATE TABLE IF NOT EXISTS `dws_traffic_funnel_1h` (
  `dt`             DATE          NOT NULL COMMENT '事件日期 DATE(event_time)，分区列',
  `hour`           INT           NOT NULL COMMENT '事件小时 0-23',
  `step`           VARCHAR(20)   NOT NULL COMMENT 'page_view / add_cart / checkout / pay',

  `pv`             BIGINT        COMMENT '事件条数',
  `uv`             BIGINT        COMMENT '去重用户数 COUNT(DISTINCT user_id)',
  `session_cnt`    BIGINT        COMMENT '去重会话数 COUNT(DISTINCT session_id)',

  `update_time`    DATETIME      COMMENT '写入时刻'
) ENGINE = OLAP
UNIQUE KEY(`dt`, `hour`, `step`)
COMMENT '小时级转化漏斗'
PARTITION BY RANGE(`dt`) (
  PARTITION `p2026` VALUES [("2026-01-01"), ("2027-01-01"))
)
DISTRIBUTED BY HASH(`step`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 6. dws_payment_method_1d —— 支付方式日汇总
-- -----------------------------------------------------------------------------
-- 6,160 行左右（616 天 × 平均 10 种支付方式，实际只有 5 种）。
--
-- **这张表的口径与上面 4 张交易表不同，必须单独说明**：
-- 它从 ods_order_payments 直接聚合，**不经过 DWD 宽表**。原因是 DWD 刻意没存
-- payment_type（见 05-dwd-trade-order-detail.sql 的注释）—— 支付方式是订单级的
-- 一对多属性，塞进订单行粒度必然要选一个，那就是近似值。这里按支付流水原始粒度
-- 聚合，一笔支付算一笔，是精确的。
--
-- 代价：这张表的 order_cnt 是「有支付记录的订单数」（99,440），比交易表的
-- 「有商品行的订单数」（98,666）大。两个数都对，只是问题不同，不要放在一起做
-- 减法算差额。
--
-- dt 需要 join orders 取 order_purchase_timestamp，payments 表本身没有时间。
CREATE TABLE IF NOT EXISTS `dws_payment_method_1d` (
  `dt`             DATE          NOT NULL COMMENT '下单日期，来自 orders.order_purchase_timestamp，分区列',
  `payment_type`   VARCHAR(32)   NOT NULL COMMENT 'credit_card / boleto / voucher / debit_card / not_defined',

  `pay_cnt`        BIGINT        COMMENT '支付笔数（payment_sequential 条数）',
  `pay_amount`     DECIMAL(20,2) COMMENT '支付金额合计',
  `order_cnt`      BIGINT        COMMENT '有支付记录的订单数',
  `sum_installments` BIGINT      COMMENT '分期数之和，除 pay_cnt 得平均分期',

  `update_time`    DATETIME      COMMENT '写入时刻'
) ENGINE = OLAP
UNIQUE KEY(`dt`, `payment_type`)
COMMENT '支付方式日汇总'
PARTITION BY RANGE(`dt`) (
  PARTITION `p2016` VALUES [("2016-01-01"), ("2017-01-01")),
  PARTITION `p2017` VALUES [("2017-01-01"), ("2018-01-01")),
  PARTITION `p2018` VALUES [("2018-01-01"), ("2019-01-01")),
  PARTITION `p2026` VALUES [("2026-01-01"), ("2027-01-01"))
)
DISTRIBUTED BY HASH(`payment_type`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);
