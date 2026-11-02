-- =============================================================================
-- 01-ods-orders.sql —— Olist orders 表 CDC → Doris ODS（单表示范）
-- -----------------------------------------------------------------------------
-- 为什么先只写一张表：CDC 作业里有 5 个参数是「写错就静默失效」的类型
-- （server-id 冲突、时区、op 元数据列名、Doris sink 的 format、dt 推导）。
-- 一次写 9 个表，任何一处错都要改 9 遍；先用最小的表跑通，再批量生成。
--
-- 提交方式（在宿主机执行）：
--   docker cp flink/sql/01-ods-orders.sql rt-flink-jm:/tmp/job.sql
--   docker exec rt-flink-jm ./bin/sql-client.sh -f /tmp/job.sql
-- =============================================================================

SET 'execution.attached' = 'false';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'parallelism.default' = '1';

-- -----------------------------------------------------------------------------
-- 源：MySQL orders
-- -----------------------------------------------------------------------------
-- server-id 必须落在 5400-5409 且**每个作业不重叠**：MySQL 把每个 CDC 客户端
-- 当作一个 slave，server-id 相同会互相踢掉对方，现象是「两个作业交替重启、
-- 日志里没有报错只有反复 connect」。
CREATE TABLE src_orders (
  order_id                      STRING,
  customer_id                   STRING,
  order_status                  STRING,
  order_purchase_timestamp      TIMESTAMP(0),
  order_approved_at             TIMESTAMP(0),
  order_delivered_carrier_date  TIMESTAMP(0),
  order_delivered_customer_date TIMESTAMP(0),
  order_estimated_delivery_date TIMESTAMP(0),
  -- 这个版本的 mysql-cdc 没有 'op' 元数据（报错原文会列出可用键：
  -- table_name / database_name / op_ts / row_kind），只有 row_kind。
  -- row_kind 是 Flink 的 RowKind 字符串：+I / -U / +U / -D
  `row_kind` STRING METADATA FROM 'row_kind' VIRTUAL,
  -- op_ts：binlog 事件时间。注意 Binlog 头只有「秒」精度，所以 ts_ms 的
  -- 后三位恒为 000 —— 这不是 bug，是 MySQL 的存储精度。
  `op_ts` TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector'      = 'mysql-cdc',
  'hostname'       = 'mysql',
  'port'           = '3306',
  'username'       = 'root',
  'password'       = 'root123',
  'database-name'  = 'olist',
  'table-name'     = 'orders',
  'server-id'      = '5401-5404',
  -- 必须显式指定，否则 Debezium 按 UTC 解析 DATETIME，
  -- 抽到 Doris 后所有时间整体偏 8 小时，且源端和目标的「今天」不是同一天
  'server-time-zone' = 'Asia/Shanghai',
  -- 首轮快照的切分粒度。orders 只有 10 万行，用默认即可；geolocation 100 万行
  -- 那个作业会显式调大
  'scan.incremental.snapshot.chunk.size' = '8096'
);

-- -----------------------------------------------------------------------------
-- 目标：Doris ods_orders
-- -----------------------------------------------------------------------------
CREATE TABLE sink_ods_orders (
  order_id                      STRING,
  customer_id                   STRING,
  order_status                  STRING,
  order_purchase_timestamp      TIMESTAMP(0),
  order_approved_at             TIMESTAMP(0),
  order_delivered_carrier_date  TIMESTAMP(0),
  order_delivered_customer_date TIMESTAMP(0),
  order_estimated_delivery_date TIMESTAMP(0),
  `op`    STRING,
  `ts_ms` BIGINT,
  `dt`    DATE
) WITH (
  'connector'        = 'doris',
  'fenodes'          = 'doris-fe:8030',
  'table.identifier' = 'realtime_dw.ods_orders',
  'username'         = 'root',
  'password'         = '',
  -- label 前缀必须每个作业唯一。它参与 Doris 的导入事务标签，
  -- 两个作业用同一个前缀会在 2PC 提交阶段互相顶掉，报
  -- "Label [xxx] has already been used"
  'sink.label-prefix' = 'ods_orders',
  -- JSON 格式 + 逐行：比 CSV 格式更稳，因为字符串里可能有分隔符（评论正文就有）
  'sink.properties.format'           = 'json',
  'sink.properties.read_json_by_line' = 'true',
  -- 把 DELETE 同步到 Doris。这条只对 UNIQUE 模型有效，且要求 Doris 开了
  -- 批量删除（0.15+ 默认开）。关掉的话 delete 事件被静默忽略，
  -- 表现为「MySQL 删了、Doris 还在」
  'sink.enable-delete' = 'true'
  -- 这里刻意不调批次参数：这一版连接器没有 sink.batch.size / sink.batch.interval
  -- （报 Unsupported options），而 sink.buffer-flush.max-rows 的下限是 10000，
  -- 调到 2000 会直接抛 "bufferFlushMaxRows must be greater than or equal to 10000"。
  -- 数据要不要尽快可见，真正有效的旋钮是 checkpoint 间隔（已在 FLINK_PROPERTIES
  -- 里设成 10s），不是这里。
);

-- -----------------------------------------------------------------------------
-- 装载
-- -----------------------------------------------------------------------------
-- 两个时间字段的口径必须分清。这里踩过一次坑，记下来：
--   ts_ms = 源端变更发生的那一刻（取 op_ts），用来算链路延迟 ——「业务时钟」
--   dt    = 这批数据落进 ODS 的日期（取 CURRENT_DATE），用来管分区和回溯 ——「处理时钟」
--
-- 为什么 dt 绝不能取 DATE(op_ts)：快照阶段（初次全量扫表）没有对应的 binlog 事件，
-- 连接器给的 op_ts 是 epoch 0。实测 99,441 行订单全部 cast 成 1970-01-01、
-- ts_ms 全部是 0 —— 一旦 DWD 按 dt 做动态分区，整个历史数据会被甩进 1970 分区
-- 并在 start=-30 的窗口里被直接 DROP 掉，而 ODS 看起来「数据都在」。
-- ODS 是落地层，分区口径本来就该是「什么时候进来的」，不是「源端什么时候改的」。
--
-- 快照行的 ts_ms 保留 0，这是个**有意的哨兵值**，表示「这一行没有源端时间戳」，
-- 不是真的发生在 1970 年。所以算延迟的查询必须带 WHERE ts_ms > 0，
-- 否则平均值会被这些 0 拽成负数（见 sql/doris/02-verify.sql 检查 5）。
INSERT INTO sink_ods_orders
SELECT
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_carrier_date,
  order_delivered_customer_date,
  order_estimated_delivery_date,
  -- RowKind → Debezium 风格的单字母。注意两点：
  --   1. 快照阶段的行是 +I，映射成 'c'，所以 ODS 里看不到 'r'。
  --      也就是说 op 无法区分「快照插入」与「增量插入」——这是只暴露
  --      row_kind 的必然结果，要区分得另外记录起始位点。
  --   2. '-U'（update-before）也映射成 'u'。它随后一定跟着 '+U'，
  --      Doris sink 对 UPDATE_BEFORE 会忽略，不会留下错误值。
  CASE `row_kind`
    WHEN '+I' THEN 'c'
    WHEN '+U' THEN 'u'
    WHEN '-U' THEN 'u'
    WHEN '-D' THEN 'd'
    ELSE 'x'
  END                                                   AS `op`,
  -- ts_ms 的算法有坑：Flink 的 UNIX_TIMESTAMP 只接受字符串参数
  -- （'UNIX_TIMESTAMP(<TIMESTAMP_WITH_LOCAL_TIME_ZONE>)' 直接校验报错），
  -- 所以先 DATE_FORMAT 成 'yyyy-MM-dd HH:mm:ss' 再解析回来。
  -- 同一会话时区内往返是自洽的，不会偏 8 小时。
  -- 乘 1000 而不是用 TIMESTAMPDIFF(MILLISECOND)：后者返回 INT，
  -- 2026 年的 epoch 毫秒约 1.77e12，早就超过 INT 上限 2.1e9 会溢出。
  -- COALESCE 兜住 op_ts 为 NULL 的情况（快照行在部分版本里给 NULL 而非 0）。
  COALESCE(
    UNIX_TIMESTAMP(DATE_FORMAT(`op_ts`, 'yyyy-MM-dd HH:mm:ss')), 0
  ) * 1000                                              AS `ts_ms`,
  CURRENT_DATE                                          AS `dt`
FROM src_orders;
