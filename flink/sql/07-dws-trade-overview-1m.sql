-- =============================================================================
-- 07-dws-trade-overview-1m.sql —— 分钟级交易概览（P5 实时主作业）
-- -----------------------------------------------------------------------------
-- 数据源：orders / order_items / customers / order_payments（4 个 CDC）
-- 目标：realtime_dw.dws_trade_overview_1m
-- server-id：5561-5576（ODS 5401-5484，P4 DWD 5531-5558，本作业 5561 起）
--
-- scan.startup.mode = 'initial'（P8 改回，理由见下）：
--   本作业是唯一保留的 mysql-cdc 直读作业（ODS 那 9 个已停，见 docs/04）。
--
--   P7 曾改成 'latest-offset'，动机是「重启时不必重放历史、聚合状态不必从零重建，
--   秒起」。这个判断错了一半，而且错得很贵 ——
--
--   关键事实：本作业的 4 张源表不是各自独立聚合，而是 **join 之后再聚合**
--   （order_items ⋈ orders ⋈ customers ⋈ payment_agg）。正常 join 的两侧状态
--   都只来自数据流：没有快照，就没有 orders / customers 侧的行。
--   而 `latest-offset` 意味着重启后从当前位点开始，一条历史行都不读。
--   于是 join 的一侧永远是空的 —— 每来一条 order_items 变更，都因为「找不到
--   对应订单」被 join 直接丢掉，sink 一条也发不出去。
--
--   实测现场（2026-11-04 08:49，BE 崩完重提后）：
--     src_order_items 源 numRecordsOut = 2   （改了 1 行 → -U/+U 两条）
--     Join[6]          numRecordsIn = 2, numRecordsOut = 0
--     sink             numRecordsIn = 0
--   而作业状态是 **RUNNING**、checkpoint 正常、lag 0、健康度 10 项全 ok。
--   目标表 8.8 万行还静静地躺在那儿，KPI 也照旧显示 —— 面板上完全看不出来。
--   这就是「UNIQUE 表不写就不会丢」这句话的陷阱：它保证了数据不丢，
--   同时也保证了你**永远发现不了它已经不写了**。
--
--   成本重估：注释里原先写「重放 155 万行」是按整个 ODS 层算的，本作业只读
--   4 张表 —— orders 9.9 万 + order_items 11.3 万 + customers 9.6 万
--   + order_payments 10.4 万 ≈ **41 万行**，实测快照 1~2 分钟。
--   用这 1~2 分钟换「重启后自愈」，比换「秒起但永久僵尸」划算得多。
--
--   代价要说清：每次重启都要重跑一遍快照，聚合状态要从 41 万行重建，
--   4 slot / 217MB 的 taskHeap 是够的（首次搭建就是这么建起来的），
--   但重启后必须等快照跑完再验收，别看到 RUNNING 就以为好了。
--   若确实只要「快起不要自愈」（例如纯演示环境），把 4 处都改回
--   'latest-offset'，但要接受：**重提之后本作业不会再产出任何一行**。
--
-- =============================================================================
-- 为什么一个作业只做一张 DWS 表（这是实测撞出来的，不是设计偏好）
-- =============================================================================
-- 最初把 4 张 trade DWS 表放进一个 STATEMENT SET，想让它们共享 join 子计划。
-- 结果子计划**没有复用**：
--   * 4 个 INSERT 的 SELECT 列表不同（各表维度列不一样），Flink 的投影裁剪
--     给 4 份 join 生成了不同的输出行类型，SubplanReuseExecutor 认不出它们是
--     同一棵树
--   * 提交后作业 35 个算子、22 个 Join —— 每条链 5.5 个，正好 4 份完整 join
--
-- 后果是状态量 ×4：单链约 105MB，四链 420MB，而 4 个 slot 下每个 slot 的
-- taskHeap 只有 217MB。现场是 TM 直接抛
--   java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler
-- 然后 TaskManager 进程退出、集群 slots-total 归 0、Doris 零行。
-- **注意这个故障的表现**：作业状态是 RESTARTING 而不是 FAILED，日志里报的是
-- NoResourceAvailableException（"Could not acquire the minimum required
-- resources"）—— 因为 TM 已经死了，JM 招不到资源。真正的死因在 TM 容器日志里，
-- 不在 Flink UI 的异常栈里。以后看到"NoResourceAvailableException"先去看
-- `docker logs rt-flink-tm` 有没有 OOM。
--
-- 所以改成「一表一作业」。本项目 4 张 trade DWS 表的状态量分别为
--   分钟表 8.8 万 key / 类目表 1.9 万 / 卖家表 6.9 万 / 地域表 1.1 万
-- 拆开之后单作业约 50-90MB，在 217MB 里有安全余量。
--
-- 顺带一个收益：拆开之后每个作业只读自己真正需要的源。分钟表根本用不到
-- products / sellers / 类目翻译，砍掉这三个 CDC reader 既省资源也省 binlog 段。
--
-- =============================================================================
-- 不用窗口，这是刻意的
-- =============================================================================
-- docs/05 原方案写的是「TUMBLE 窗口 + Watermark 延迟 30s + allowed lateness 5 分钟」。
-- 在这套数据上那个方案会失败，原因是可验证的：
--
--   * 交易数据的 order_purchase_timestamp 跨 2016-09-04 ~ 2018-09-03 共 616 天，
--     快照 6 秒导完，水位线会瞬间跳过 616 天，窗口「批量触发」，与实时无关。
--   * 只有一批数据时，水位线推进不到最后一批事件之后，尾部窗口永远不关 ——
--     这正是 P4 踩过的坑（埋点 22,064 条只落 21,996 条，缺的正是最后 8 条支付）。
--   * 迟到几分钟的数据，allowed lateness 尚可兜住；迟到几天直接丢。
--
-- 换成「无窗口 GROUP BY 派生列 + upsert 落 Doris」后：
--   * 迟到多久都能并进它该去的历史分桶，不需要 watermark 参与
--   * 每个 key 每次下发的是**全量新值**，重放收敛，天然幂等
--   * Doris 侧因此必须用 UNIQUE 模型而不是 AGGREGATE + SUM —— 见 05-dws.sql 开头
--
-- 代价是失去「窗口关闭」这个语义，迟到数据会让已经展示过的历史分钟数字变大。
-- 这是正确的行为（补数），不是 bug。
-- =============================================================================

SET 'execution.attached' = 'false';
SET 'table.local-time-zone' = 'Asia/Shanghai';

-- 与 P4 同因同解：99,441 个 order_id 上做「小 key、随机、海量」的状态更新，
-- 正是 RocksDB 在 Docker Desktop on Windows 上最吃亏的形态。实测换 hashmap
-- 之前，聚合 CPU 只花 2.3 秒、墙钟卡 5 分钟停在 11/14 ack。
SET 'state.backend' = 'hashmap';


-- -----------------------------------------------------------------------------
-- Source 1/4：orders（提供下单时间，决定 dt 与 minute）
-- -----------------------------------------------------------------------------
CREATE TABLE src_orders (
  `order_id`                      STRING,
  `customer_id`                   STRING,
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
  'server-id'                    = '5561-5564',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000',
  'scan.startup.mode' = 'initial'
);


-- -----------------------------------------------------------------------------
-- Source 2/4：order_items（事实主表，决定粒度与金额）
-- -----------------------------------------------------------------------------
CREATE TABLE src_order_items (
  `order_id`            STRING,
  `order_item_id`       INT,
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
  'server-id'                    = '5565-5568',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000',
  'scan.startup.mode' = 'initial'
);


-- -----------------------------------------------------------------------------
-- Source 3/4：customers（只取去重用户 ID）
-- -----------------------------------------------------------------------------
CREATE TABLE src_customers (
  `customer_id`              STRING,
  `customer_unique_id`       STRING,
  PRIMARY KEY (`customer_id`) NOT ENFORCED
) WITH (
  'connector'                    = 'mysql-cdc',
  'hostname'                     = 'mysql',
  'port'                         = '3306',
  'username'                     = 'root',
  'password'                     = 'root123',
  'database-name'                = 'olist',
  'table-name'                   = 'customers',
  'server-id'                    = '5569-5572',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000',
  'scan.startup.mode' = 'initial'
);


-- -----------------------------------------------------------------------------
-- Source 4/4：order_payments（只用来算「支付用户数」）
-- -----------------------------------------------------------------------------
CREATE TABLE src_order_payments (
  `order_id`             STRING,
  `payment_sequential`   INT,
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
  'server-id'                    = '5573-5576',
  'server-time-zone'             = 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '200000',
  'scan.startup.mode' = 'initial'
);


-- -----------------------------------------------------------------------------
-- 支付聚合：一单一行
-- -----------------------------------------------------------------------------
-- 一笔订单可能分期多次支付。不聚合直接 join 会把 112,650 行放大。
-- 不输出 payment_type：支付方式是订单级一对多属性，塞进订单行粒度必然是近似，
-- 占比走 dws_payment_method_1d（从支付流水原始粒度算）。
CREATE TEMPORARY VIEW v_payment_agg AS
SELECT
  `order_id`,
  SUM(`payment_value`) AS payment_total
FROM `src_order_payments`
GROUP BY `order_id`;


-- -----------------------------------------------------------------------------
-- Sink
-- -----------------------------------------------------------------------------
CREATE TABLE sink_dws_trade_overview_1m (
  `dt`             DATE,
  `minute`         STRING,
  `gmv`            DECIMAL(20,2),
  `freight_total`  DECIMAL(20,2),
  `item_cnt`       BIGINT,
  `order_cnt`      BIGINT,
  `buyer_cnt`      BIGINT,
  `pay_user_cnt`   BIGINT,
  `update_time`    TIMESTAMP(3)
) WITH (
  'connector'              = 'doris',
  'fenodes'                = 'doris-fe:8030',
  'table.identifier'       = 'realtime_dw.dws_trade_overview_1m',
  -- username/password 必须显式写。connector 默认 username 是 'null' 不是 'root'，
  -- 漏写时 Doris 收到 user 'null' 返回 401，而 connector 把这段错误文本当 JSON
  -- 解析，最终报的是「Cannot deserialize value of type int」—— 和认证毫无关系。
  'username'               = 'root',
  'password'               = '',
  'sink.label-prefix'      = 'dws_trade_overview_1m',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete'     = 'true',

  -- 下面三个参数是 P4 卡了最久的坑，务必保留：
  -- connector 24.0.1 的 STREAM_LOAD 把请求体走环形缓冲流式发送，一次 flush
  -- 数据量 ≥ 环容量时写端/读端在 LinkedBlockingDeque.take() 上互等死锁。
  -- 现场是「作业 RUNNING、各算子都在出数、checkpoint 永远 3/14、Doris 零行」，
  -- 零报错。max-bytes 低于 10485760 connector 直接拒绝启动，两个值必须成对给。
  'sink.buffer-size'       = '1048576',
  'sink.buffer-count'      = '32',
  'sink.buffer-flush.max-bytes' = '10485760',
  -- 时间触发（P7 加）：max-bytes 的 10MB 是 connector 允许的下限，而盘点一次
  -- 变更只产生几百字节，够不到阈值就只能等 checkpoint —— 实测 MySQL 写入到
  -- DWS 可见 7.5s，加 1s 时间触发后降到 1s 级。
  'sink.buffer-flush.interval'  = '1s',
  -- 环放大只缓解不根治，STREAM_LOAD_BATCH 走「先攒后发」的整批请求体才绕开。
  'sink.write-mode'        = 'STREAM_LOAD_BATCH'
);


-- -----------------------------------------------------------------------------
-- 装载
-- -----------------------------------------------------------------------------
-- 类型必须逐列 CAST 到与 sink 声明一致，否则提交阶段报
-- 「Column types of query result and sink do not match」，且一次只报第一处：
--   SUM(DECIMAL(18,2))           → DECIMAL(38,2)
--   COUNT(*) / COUNT(DISTINCT x) → BIGINT
--
-- minute 用 DATE_FORMAT(...,'HH:mm') 而不是 MINUTE()：只取分钟数会让 10:30 和
-- 11:30 撞成同一个 key，一天的桶从 1,440 个掉到 60 个。
INSERT INTO sink_dws_trade_overview_1m
SELECT
  CAST(o.`order_purchase_timestamp` AS DATE)             AS `dt`,
  DATE_FORMAT(o.`order_purchase_timestamp`, 'HH:mm')     AS `minute`,
  CAST(SUM(oi.`price` + oi.`freight_value`) AS DECIMAL(20,2)) AS `gmv`,
  CAST(SUM(oi.`freight_value`) AS DECIMAL(20,2))         AS `freight_total`,
  COUNT(*)                                               AS `item_cnt`,
  COUNT(DISTINCT oi.`order_id`)                          AS `order_cnt`,
  COUNT(DISTINCT c.`customer_unique_id`)                 AS `buyer_cnt`,
  COUNT(DISTINCT CASE WHEN pa.`payment_total` > 0
                      THEN c.`customer_unique_id` END)   AS `pay_user_cnt`,
  LOCALTIMESTAMP                                         AS `update_time`
FROM `src_order_items` oi
JOIN `src_orders` o
  ON oi.`order_id` = o.`order_id`
LEFT JOIN `src_customers` c
  ON o.`customer_id` = c.`customer_id`
LEFT JOIN `v_payment_agg` pa
  ON oi.`order_id` = pa.`order_id`
GROUP BY CAST(o.`order_purchase_timestamp` AS DATE),
         DATE_FORMAT(o.`order_purchase_timestamp`, 'HH:mm');
