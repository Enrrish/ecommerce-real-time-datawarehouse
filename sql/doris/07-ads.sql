-- =============================================================================
-- 07-ads.sql —— ADS 应用层 7 张表
-- -----------------------------------------------------------------------------
-- 执行：MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe \
--         mysql -h127.0.0.1 -P9030 -uroot < sql/doris/07-ads.sql
--
-- =============================================================================
-- ADS 的定位：不是「再汇总一次」，而是「按大屏组件切成形状」
-- =============================================================================
-- DWS 是按分析维度聚合的（分钟 / 类目 / 卖家 / 地域），一张表服务多个问题；
-- ADS 反过来，一张表就对一个大屏组件，字段就是前端要绑定的字段。具体差别：
--
--   * 派生指标在这层算：客单价、件单价、运费占比、环节转化率、占比、排名。
--     DWS 刻意不存率值（见 05-dws.sql 头部第 3 条），因为率值不可加，
--     一旦落表就会被下游 SUM 出假指标。ADS 是链路终点，没人再往下加，才算。
--   * 排名（rank_no）在这层算。DWS 存全量维度值，ADS 带上名次，前端不用排。
--   * 字段名对齐大屏的语义（如 `cum_gmv` 是「今日累计」，前端直接画曲线）。
--
-- =============================================================================
-- 为什么全部用 UNIQUE KEY，而不是 docs/02 原定的「ADS 累计类用 AGGREGATE」
-- =============================================================================
-- 原方案设想 ADS 里有「累计类」指标，用 AGGREGATE 的 SUM 列做写入时累加。
-- 实现时发现这个设想没有落地场景：真正的累计值（今日累计 GMV）必须按
-- 「截至该分钟的全量值」存，而不是「本分钟的增量」—— 前端要的是曲线上每个点
-- 的绝对值，不是累加器。用 SUM 列存增量会让每次重刷都把前一次的值再叠一遍。
--
-- 所以本层没有一张表需要 AGGREGATE。全部 UNIQUE，重刷幂等。
--
-- =============================================================================
-- ADS 由 Doris 侧 SQL 刷新，不起 Flink 作业
-- =============================================================================
-- 两个理由：
--   1. **没有 slot。** 4 slots 已被 4 个常驻作业占满（分钟概览 / 漏斗支付 /
--      埋点清洗 / ods_user_event）。塞第 5 个作业要么 OOM 要么停掉现有作业。
--   2. **语义上就该是批的。** ADS 服务的是「看板刷新」这个动作，不是每来一条
--      数据就变一次。Doris 侧一条 INSERT ... SELECT 就是一次刷新，重跑任意次
--      结果一致，这比再加一层流更简单也更好验证。
--
-- 刷新脚本：sql/doris/08-ads-refresh.sql（幂等，可随时重跑）
-- 验收脚本：sql/doris/09-ads-verify.sql
--
-- =============================================================================
-- 去重指标（UV / buyer_cnt）不可加，这决定了哪些表能从 DWS 派生
-- =============================================================================
-- DWS 里的去重值是**桶内去重**：`buyer_cnt` 是「该分钟内的去重买家数」，
-- `uv` 是「该小时内的去重用户数」。同一个人 10:01 和 10:02 各下一单，
-- 在两个分钟行里各算 1 次，SUM 起来是 2 —— 而日去重买家数应该是 1。
--
-- 由此分出两类 ADS 表：
--
--   * ads_realtime_overview  从 DWS 分钟表派生。gmv / order_cnt / item_cnt
--                            **可加**，直接 SUM；buyer_cnt **不可加**，
--                            只保留分钟级值并显式注释，不提供日累计。
--   * ads_funnel_1d          **回 DWD 明细重算**，不 SUM 小时 uv。
--                            22,004 行，全表扫 < 100ms，没必要为省这点扫描
--                            去接受一个算错的 UV。
--
-- `ads_order_stream` 同理回 DWD（订单粒度的会员/地域属性只在明细里有）。
-- =============================================================================

USE `realtime_dw`;


-- -----------------------------------------------------------------------------
-- 1. ads_realtime_overview —— 大屏 KPI 条 + 成交趋势折线
-- -----------------------------------------------------------------------------
-- 88,157 行，与 dws_trade_overview_1m 一一对应，加了 4 个派生列 + 2 个累计列。
--
-- **`cum_gmv` 是这张表最容易被读错的一列**：它是「当日 00:00 到本分钟的累计
-- GMV」，不是「全天 GMV」。前端画累计曲线用这一列，画每分钟柱状用 `gmv`。
-- 两者不要混着 SUM —— SUM(cum_gmv) 是把每条曲线终点又加了一遍，值会大得离谱。
--
-- `buyer_cnt` / `pay_user_cnt` 保留分钟级去重值，**跨分钟不可加**。
-- 要「今日买家数」只有一个办法：查 dwd_trade_order_detail 做日粒度去重：
--   SELECT COUNT(DISTINCT customer_unique_id) FROM dwd_trade_order_detail WHERE dt = ?
-- 这跟 DWS 层 order_cnt 跨维度不可加是同一类陷阱（见 05-dws.sql 第 2 条注释）。
CREATE TABLE IF NOT EXISTS `ads_realtime_overview` (
  `dt`              DATE          NOT NULL COMMENT '下单日期',
  `minute`          VARCHAR(5)    NOT NULL COMMENT 'HH:mm 分钟桶',

  `gmv`             DECIMAL(20,2) COMMENT '本分钟 GMV = SUM(item_total)，含运费。全链路唯一 GMV 口径',
  `order_cnt`       BIGINT        COMMENT '本分钟有商品行的订单数',
  `item_cnt`        BIGINT        COMMENT '本分钟订单行数',
  `buyer_cnt`       BIGINT        COMMENT '本分钟去重买家数。**跨分钟不可加**，日买家数须查 DWD',
  `pay_user_cnt`    BIGINT        COMMENT '本分钟去重支付用户数。**跨分钟不可加**',
  `freight_total`   DECIMAL(20,2) COMMENT '本分钟运费合计',

  `aov`             DECIMAL(20,2) COMMENT '客单价 = gmv / order_cnt，分母是订单数不是行数',
  `avg_item_price`  DECIMAL(20,2) COMMENT '件单价 = gmv / item_cnt',
  `freight_ratio`   DECIMAL(8,4)  COMMENT '运费占比 = freight_total / gmv',

  `cum_gmv`         DECIMAL(20,2) COMMENT '当日内截至本分钟的累计 GMV。**不要 SUM 这一列**',
  `cum_order_cnt`   BIGINT        COMMENT '当日内截至本分钟的累计订单数。**不要 SUM 这一列**',

  `update_time`     DATETIME      COMMENT '刷新时刻'
) ENGINE = OLAP
UNIQUE KEY(`dt`, `minute`)
COMMENT 'ADS-实时交易概览（大屏 KPI + 趋势）'
DISTRIBUTED BY HASH(`minute`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 2. ads_category_rank_1d —— 大屏品类排行
-- -----------------------------------------------------------------------------
-- 18,990 行，与 dws_trade_category_1d 同粒度，多了 rank_no 与 gmv_ratio。
--
-- 主键用 `category_name` 而不是 `rank_no`：名次是会变的，今天第 3 名的类目
-- 明天可能第 5 名。若拿 rank_no 做主键，重刷后旧名次的行不会被新数据覆盖
-- （新数据的 rank_no 集合可能相同，只是对应的类目换了），结果是名次和类目错配，
-- 而且 Doris 不会报任何错。**主键必须是稳定标识，不能是排序结果。**
--
-- rank_no 用 ROW_NUMBER 而不是 RANK：RANK 遇平局会跳号（1,2,2,4），
-- 大屏列表上出现缺号看起来像 bug。ROW_NUMBER 强制 1,2,3,4，前端好处理。
CREATE TABLE IF NOT EXISTS `ads_category_rank_1d` (
  `dt`             DATE          NOT NULL COMMENT '下单日期',
  `category_name`  VARCHAR(64)   NOT NULL COMMENT '英文类目名，已 COALESCE 兜底',

  `rank_no`        INT           COMMENT '当日 GMV 名次，ROW_NUMBER 保证连续不跳号',
  `gmv`            DECIMAL(20,2) COMMENT '类目 GMV',
  `order_cnt`      BIGINT        COMMENT '类目订单数。**跨类目不可加**，一单跨 3 类目算 3 次',
  `item_cnt`       BIGINT        COMMENT '类目商品件数',
  `seller_cnt`     BIGINT        COMMENT '类目下有出货的卖家家数',
  `aov`            DECIMAL(20,2) COMMENT '类目客单价 = gmv / order_cnt',
  `gmv_ratio`      DECIMAL(8,4)  COMMENT '类目 GMV / 当日总 GMV',

  `update_time`    DATETIME      COMMENT '刷新时刻'
) ENGINE = OLAP
UNIQUE KEY(`dt`, `category_name`)
COMMENT 'ADS-品类排行（大屏排行榜）'
DISTRIBUTED BY HASH(`category_name`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 3. ads_funnel_1d —— 大屏转化漏斗
-- -----------------------------------------------------------------------------
-- 只有 8 行（2 个事件日期 × 4 个环节），但它是唯一一张**不从 DWS 读**的表。
--
-- **为什么不从 dws_traffic_funnel_1h 汇总**：小时表的 uv 是「小时内去重」，
-- 同一用户 10:30 浏览、11:10 加购，在两个小时行里各算一次。日 UV 若写成
-- SUM(uv)，跨小时活跃的用户会被重复计数，转化率随之失真。
-- 而从 dwd_user_event_clean 重算，22,004 行全表扫不到 100ms。
-- 用 100ms 的扫描换一个不会算错的 UV，这个交换没有犹豫的余地。
--
-- `step_order` 存环节序号（1-4）：SKU 名字（page_view 等）不可排序，前端要按
-- 业务顺序画漏斗就得有一列显式序号。序号由刷新 SQL 里的 CASE 写死，
-- 不建映射表 —— 只有 4 个恒定环节，为它多一张表是过度设计。
--
-- 转化率两列的分母不同，别混：
--   `conv_rate_vs_prev`  = 本环节 UV / 上一环节 UV   → 看单步流失
--   `conv_rate_vs_first` = 本环节 UV / page_view UV → 看全程转化
--
-- 首环节（page_view）的表现**不对称**，这不是 bug：
--   `conv_rate_vs_first` = 1.0000  —— 自己除自己，定义使然
--   `conv_rate_vs_prev`  = NULL    —— 「上一环节」不存在，除法无定义
--
-- 为什么不让它等于 1：那会暗示「进入首页的转化率是 100%」，而首页前面
-- 其实什么都没有。NULL 在语义上是「无此环节」，1.0000 是「有且为满」。
-- 前端也是按这个约定写的（charts.js 里 i > 0 才取 conv_prev），
-- 第 1 根柱子不显示单步转化率。要改成 1.0000 的话两处都得改。
CREATE TABLE IF NOT EXISTS `ads_funnel_1d` (
  `dt`                 DATE          NOT NULL COMMENT '事件日期',
  `step`               VARCHAR(20)   NOT NULL COMMENT 'page_view / add_cart / checkout / pay',

  `step_order`         INT           COMMENT '环节序号 1-4，前端按它排序',
  `pv`                 BIGINT        COMMENT '事件条数',
  `uv`                 BIGINT        COMMENT '日去重用户数。**日 UV ≠ 小时 UV 之和**，这里是从明细重算的',
  `session_cnt`        BIGINT        COMMENT '日去重会话数',

  `conv_rate_vs_prev`  DECIMAL(8,4)  COMMENT '本环节 UV / 上一环节 UV，单步转化率',
  `conv_rate_vs_first` DECIMAL(8,4)  COMMENT '本环节 UV / page_view UV，全程转化率',

  `update_time`        DATETIME      COMMENT '刷新时刻'
) ENGINE = OLAP
UNIQUE KEY(`dt`, `step`)
COMMENT 'ADS-转化漏斗（大屏漏斗图）'
DISTRIBUTED BY HASH(`step`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 4. ads_payment_method_1d —— 大屏支付方式环形图
-- -----------------------------------------------------------------------------
-- 2,276 行。口径继承 dws_payment_method_1d：从支付流水原始粒度聚合，
-- **不经过 DWD 宽表**（支付方式是订单级一对多属性，塞进订单行粒度必然是近似）。
--
-- `amount_ratio` 的分母是「当日所有支付方式的金额合计」，用窗口函数一次算出，
-- 不要写成「本行金额 / 全表金额」—— 那样会跨天相除。
--
-- 这张表的 `order_cnt` 与交易表口径不同（有支付记录的订单数 99,440 vs
-- 有商品行的订单数 98,666），且同一订单用两种支付方式会在两行各计一次。
-- 两套数都对，只是问题不同，**不要相减算差额**（docs/03 §1 已固化）。
CREATE TABLE IF NOT EXISTS `ads_payment_method_1d` (
  `dt`               DATE          NOT NULL COMMENT '下单日期',
  `payment_type`     VARCHAR(32)   NOT NULL COMMENT 'credit_card / boleto / voucher / debit_card / not_defined',

  `rank_no`          INT           COMMENT '当日支付金额名次',
  `pay_cnt`          BIGINT        COMMENT '支付笔数',
  `pay_amount`       DECIMAL(20,2) COMMENT '支付金额合计',
  `order_cnt`        BIGINT        COMMENT '有支付记录的订单数。**跨支付方式不可加**',
  `avg_installments` DECIMAL(8,2)  COMMENT '平均分期数 = sum_installments / pay_cnt',

  `amount_ratio`     DECIMAL(8,4)  COMMENT '金额占比 = 本行金额 / 当日全部支付金额',
  `cnt_ratio`        DECIMAL(8,4)  COMMENT '笔数占比 = 本行笔数 / 当日全部支付笔数',

  `update_time`      DATETIME      COMMENT '刷新时刻'
) ENGINE = OLAP
UNIQUE KEY(`dt`, `payment_type`)
COMMENT 'ADS-支付方式分布（大屏环形图）'
DISTRIBUTED BY HASH(`payment_type`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 5. ads_order_stream —— 大屏实时订单流水
-- -----------------------------------------------------------------------------
-- 98,666 行（一单一行），从 dwd_trade_order_detail 按 order_id 聚合而来。
-- 明细是 112,650 行（订单行粒度），这里收敛成订单粒度 —— 大屏滚动的单位是
-- 「一笔订单」，不是「一个商品行」。
--
-- 主键只有 `order_id`，不带 dt：订单号本身全局唯一，带 dt 反而会让
-- 「同一订单的 dt 被改了」这种情况产生两行（虽然业务上不会，但没必要留这个口子）。
-- dt 作为普通列存着，供大屏按日期筛选。
--
-- `gmv` 是订单级 GMV（该订单所有商品行的 item_total 之和），
-- 与宽表的订单行级 item_total 是两回事，两者只在 `SUM` 全表时相等。
--
-- `is_late` 在订单级取 MAX：一单里只要有一个商品行迟到就算这单迟到。
-- 取 MIN 会得出「一单 5 个商品行、4 个迟到，这单准时」的荒谬结论。
CREATE TABLE IF NOT EXISTS `ads_order_stream` (
  `order_id`           VARCHAR(32)   NOT NULL COMMENT '订单号，也是主键',
  `dt`                 DATE          COMMENT '下单日期，普通列非主键',
  `order_time`         DATETIME      COMMENT '下单时刻，大屏按它倒序滚动',
  `order_status`       VARCHAR(20)   COMMENT 'delivered / shipped / canceled / ...',

  `customer_unique_id` VARCHAR(32)   COMMENT '自然人 ID（不是订单级的 customer_id）',
  `customer_state`     VARCHAR(8)    COMMENT '买家所在州',
  `customer_city`      VARCHAR(64)   COMMENT '买家所在城市',

  `item_cnt`           BIGINT        COMMENT '该订单商品行数',
  `gmv`                DECIMAL(20,2) COMMENT '该订单 GMV（各商品行 item_total 之和）',
  `payment_total`      DECIMAL(20,2) COMMENT '该订单支付金额',
  `delivery_days`      INT           COMMENT '履约天数，未妥投为 NULL',
  `is_late`            TINYINT       COMMENT '1=迟到 0=准时 NULL=未妥投（订单内任一行迟到即算迟到）',

  `update_time`        DATETIME      COMMENT '刷新时刻'
) ENGINE = OLAP
UNIQUE KEY(`order_id`)
COMMENT 'ADS-实时订单流水（大屏滚动列表）'
DISTRIBUTED BY HASH(`order_id`) BUCKETS 8
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 6. ads_pipeline_health —— 大屏链路健康度面板
-- -----------------------------------------------------------------------------
-- 这张表替代 docs/02 原定的 `ads_user_rfm`，理由见 docs/02 §1 的修正说明。
-- 一句话：Olist 复购率只有 3%（docs/03 §4 已写明），RFM 的 F 维度 97% 都是 1，
-- 分不出层 —— 一张没有区分度的表撑不起一个面板。
-- 而链路健康度是 docs/03 §5 已经定义了 6 个指标、P6 又要逐个验证的东西，
-- 它既是大屏真实需要的面板，也是这个项目「实时」属性唯一的自证材料。
--
-- 表结构用 **key-value 纵表** 而不是宽表，原因有三：
--   1. 指标口径不同、单位不同（ms / % / 条 / s），宽表得用 DOUBLE 硬塞，
--      字段名还得带上单位，读起来别扭。
--   2. 指标会增删。P6 只要 6 个，P7 可能要加「Doris 导入延迟」「TM 堆使用率」，
--      纵表加一行就行，宽表要 ALTER。
--   3. 大屏渲染是「遍历卡片列表」，纵表查出来直接喂给列表组件，不用按字段名分支。
--
-- 10 个指标的来源分两类（采集脚本里前 5 个从 Doris 算，后 5 个从 Flink/Kafka 探）：
--
--   | metric_key          | 来源                | 口径（docs/03 §5）              |
--   |---------------------|---------------------|--------------------------------|
--   | event_age_p50/p99   | Doris 自动          | ts_ms/1000 - event_time（入库时刻-事件时间）|
--   | pipeline_freshness  | Doris 自动          | NOW() - MAX(update_time)，链路活性 |
--   | canceled_gmv        | Doris 自动          | canceled+unavailable 的 GMV 合计 |
--   | canceled_ratio      | Doris 自动          | 无效交易金额 / 总 GMV            |
--   | dup_rate            | Kafka 窗口抽样      | 1 - COUNT(DISTINCT event_id)/COUNT(*) |
--   | late_ratio          | Kafka 窗口抽样      | 落后分区内最大事件时间 >60s 的占比 |
--   | kafka_lag           | Kafka 采集          | consumer lag，三个消费组之和     |
--   | checkpoint_p95      | Flink REST          | 运行中作业的 end_to_end_duration P95 |
--   | e2e_latency_ms      | 常量（P3 实测）     | 掐表量写入→可见，3,849 ms        |
--
-- **`dup_rate` 与 `late_ratio` 必须是「最近 N 条滑动窗口」口径，不能是累计口径。**
-- P6 实测：第一批注入的重复 60 条，用全量算只有 0.19%，而注入批次内部是 2.91%，
-- 差 15 倍；改成单分区窗口后又因为混进历史数据，3% 被摊薄成 1.27%；最终改成
-- 「3 个分区各取 1/3 窗口」才准确复现出 2.9667%。监控指标一旦用累计口径，
-- 就会被历史无限稀释，越跑越"健康"，最终彻底失效。
--
-- `metric_level` 是阈值判定结果（ok / warn / critical），阈值写死在采集脚本里，
-- 同时回填到 `threshold_desc` 供大屏悬浮提示用 —— 让看板自己解释「为什么红了」，
-- 比在旁边放一份文档靠谱。
--
-- **`threshold_desc` 给到 VARCHAR(255) 而不是 128**：Doris 的 VARCHAR(n) 里
-- 那个 n 是**字节数**，不是字符数。UTF-8 的中文占 3 字节，128 字节只够放 42 个
-- 中文字。第一次建表时按「128 个字符」的直觉给了 128，采集脚本写入带中文说明的
-- 阈值时直接被 strict mode 拒掉：
--   Insert has filtered data in strict mode
-- 这个错误不带列名也不说超长，只看错误码很难定位。
CREATE TABLE IF NOT EXISTS `ads_pipeline_health` (
  `metric_key`       VARCHAR(48)   NOT NULL COMMENT '指标英文键，主键',
  `metric_name`      VARCHAR(64)   COMMENT '中文名，大屏直接显示',
  `metric_value`     DOUBLE        COMMENT '指标值，单位见 metric_unit',
  `metric_unit`      VARCHAR(16)   COMMENT 'ms / % / 条 / s / s/次',
  `metric_level`     VARCHAR(16)   COMMENT 'ok / warn / critical，由采集时按阈值判定',
  `threshold_desc`   VARCHAR(255)  COMMENT '阈值说明（字节数，中文占 3 字节/字）',
  `metric_group`     VARCHAR(24)   COMMENT 'delay / quality / capacity，大屏分组用',
  `collect_time`     DATETIME      COMMENT '采集时刻'

) ENGINE = OLAP
UNIQUE KEY(`metric_key`)
COMMENT 'ADS-链路健康度（大屏质量面板）'
DISTRIBUTED BY HASH(`metric_key`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);


-- -----------------------------------------------------------------------------
-- 7. ads_region_map_1d —— 大屏地域地图
-- -----------------------------------------------------------------------------
-- 10,701 行（与 dws_trade_region_1d 同粒度），补了占比、名次和州中心坐标。
--
-- **为什么需要这张表：** P6 收尾时发现的缺口 —— ADS 六张表没有一张能被地图
-- 组件直接消费，而大屏有地图块。选择补表而不是「让大屏直接查 DWS」，因为
-- DWS 不带占比、不带名次、不带坐标，三样都要前端算，那就等于让前端写 SQL。
--
-- -----------------------------------------------------------------------------
-- 单日只有 17 个州有成交，这是数据特性不是缺数
-- -----------------------------------------------------------------------------
-- 整期（616 天）27 个州全部出现过订单，但**任意单日平均只有 17 个州有单**：
-- 北部几个州（RR / AP / AC / RO）本身人口极少，不是每天都有交易。
-- 大屏按某一天看时地图上必然有 10 块是空的，**不要当成数据缺失去排查**。
--
-- 由此得出一条硬约束：**地图的填色不能用「绝对金额」当色阶**。
-- 各州 GMV 差了两个数量级（SP 一家占 37%，RR 占 0.01%），用绝对金额填色只有
-- SP 一州发红、其余全白。填色应该用 `gmv_ratio`（或前端自己算分位数）。
--
-- `buyer_cnt` 的适用范围只有「这一个 (dt, state) 桶」，任何跨桶聚合都会偏大，
-- 连跨州相加都不行 —— 直觉上「一个人只住一个州，所以跨州可加」，但实测有
-- **37 个自然人在两个州下过单**（占 95,420 名买家的 0.04%），大概是搬家或
-- 寄给异地收件人。所以：
--
--   SUM(buyer_cnt) 全表 = 97,734   vs   全期去重 COUNT(DISTINCT) = 95,420（差 2.4%）
--
-- 差的那 2,314 里，跨天重复占大头、跨州的 37 人占小头。要日活 / 月活只能回
-- dwd_trade_order_detail 做 COUNT(DISTINCT customer_unique_id)。
-- 教训：**可加性不能凭直觉断言，得跑一次验证**。这条是写注释时断言「跨州可加」
-- 然后被数据打脸之后补上的。
--
-- -----------------------------------------------------------------------------
-- 坐标：先按巴西合理经纬度范围过滤，再按 point_cnt 加权
-- -----------------------------------------------------------------------------
-- 坐标来自 dim_geo（邮编前缀级，19,015 行），一个州要收敛成一个「州中心点」。
-- 直接用 AVG 会出错 —— 实测 dim_geo 里有 11 个越界点（占 0.06%）：
--
--   PA  68275  lat=22.567952  lng=-27.698087   ← 纬度跑到北纬 22 度
--   PR  83252  lat=42.184003  lng=-8.723762    ← 经度和纬度串了
--   PE  53990  lat=-3.848721  lng=-32.414100   ← 经度落到大西洋里
--
-- 巴西的真实范围是 lat -33.7~+5.3、lng -73.9~-34.8。按这个框过滤掉 11 个点
-- 之后，27 个州加权平均的结果全部落在正确位置（SP -23.16/-47.08、
-- AM -3.35/-60.54、RR 2.72/-60.67 在北半球，都对得上）。
--
-- 用**加权平均**而不是中位数：`point_cnt` 是该邮编前缀下有多少个地理采样点，
-- 权重越大人越密，中心点自然向人口聚集区偏移 —— 这对地图气泡定位是想要的。
-- （顺带记一条：Doris 的 `MEDIAN()` 不接受 DECIMAL，要用得先 CAST 成 DOUBLE。）
--
-- **坐标只用于气泡定位，不要拿它做距离计算。** 它是「人口加权重心」不是
-- 「几何中心」，SP 的重心比几何中心更靠东南，这是对的。
--
-- -----------------------------------------------------------------------------
-- rank_no 不能做主键
-- -----------------------------------------------------------------------------
-- 与 ads_category_rank_1d 同一个道理：名次每天变，拿它做主键会让重刷后
-- 旧名次的行不被覆盖，出现「名次和州错配」而 Doris 不报错。
-- 主键必须是稳定标识 (dt, customer_state)。
--
-- `state_name` 存葡语全名，供地图 JSON 用名称匹配（不同图源的 key 不一样，
-- 有的用缩写 SP、有的用全名 São Paulo，两列都给前端自己选）。
CREATE TABLE IF NOT EXISTS `ads_region_map_1d` (
  `dt`             DATE          NOT NULL COMMENT '下单日期。大屏要取 MAX(dt) 而不是 CURRENT_DATE —— 数据是历史回放',
  `customer_state` VARCHAR(8)    NOT NULL COMMENT '买家所在州，两位缩写，主键',

  `state_name`     VARCHAR(32)   COMMENT '州全名（葡语），供地图按名称匹配；缩写见 customer_state',

  `gmv`            DECIMAL(20,2) COMMENT '该州当日 GMV = SUM(item_total)，含运费',
  `freight_total`  DECIMAL(20,2) COMMENT '该州当日运费合计',
  `item_cnt`       BIGINT        COMMENT '商品件数',
  `order_cnt`      BIGINT        COMMENT '有商品行的订单数。**跨州可加**（一单只归一个州）',
  `buyer_cnt`      BIGINT        COMMENT '当日该州去重买家数。**任何跨桶相加都会偏大**，日活须查 DWD',

  `aov`            DECIMAL(20,2) COMMENT '该州客单价 = gmv / order_cnt',
  `freight_ratio`  DECIMAL(8,4)  COMMENT '该州运费占比 = freight_total / gmv',

  `gmv_ratio`      DECIMAL(8,4)  COMMENT '该州 GMV / 当日全部州 GMV。**地图填色用这一列，不要用 gmv**',
  `gmv_rank`       INT           COMMENT '当日 GMV 名次，ROW_NUMBER 保证连续不跳号',

  `center_lat`     DECIMAL(10,6) COMMENT '州中心点纬度：dim_geo 按 point_cnt 加权，已滤除 11 个越界点。仅用于气泡定位',
  `center_lng`     DECIMAL(10,6) COMMENT '州中心点经度，口径同上',

  `update_time`    DATETIME      COMMENT '刷新时刻'
) ENGINE = OLAP
UNIQUE KEY(`dt`, `customer_state`)
COMMENT 'ADS-地域地图（大屏地图组件）'
DISTRIBUTED BY HASH(`customer_state`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);
