-- =============================================================================
-- 04-ods-user-event.sql — ODS 埋点事件（Kafka → Doris）
-- -----------------------------------------------------------------------------
-- 提交：bash scripts/submit-sql.sh flink/sql/04-ods-user-event.sql
--
-- 这是 10 张 ODS 表里**唯一不走 CDC 的**，也是整个项目唯一从 Kafka 进来的链路：
--
--   9 张业务表：MySQL binlog → Flink CDC → Doris   （变更流，有 op 概念）
--   ods_user_event：Kafka topic user_event → Flink → Doris（事件流，无 op 概念）
--
-- 两条链路并存的理由，面试里必问：
--   * binlog 是「库的变更日志」，描述的是**最终状态**（一行变成什么样）。
--     它能告诉你「购物车数量从 1 变成 2」，但不知道中间点了几次。
--   * 埋点是「用户的行为流」，描述的是**过程**（点了什么、在哪个页面、什么时候）。
--     漏斗（浏览 → 加购 → 下单 → 支付）只能从埋点算，binlog 算不出来。
--   所以业务库走 CDC 保证状态一致，行为分析走 Kafka 保证过程完整。
--
-- 与另外 9 张表在字段口径上的唯一差异：
--   dt 取 DATE(event_time) —— 事件的**业务日期**，不是落地日期。
--   因为埋点的时间本身就是业务时间（用户什么时候点的），不存在 CDC 快照那种
--   「op_ts 是 epoch 0」的问题，所以直接用业务日期做分区依据，大屏按天聚合正好对上。
-- =============================================================================
SET 'execution.attached' = 'false';
SET 'table.local-time-zone' = 'Asia/Shanghai';

-- -----------------------------------------------------------------------------
-- 源表：Kafka topic user_event
-- -----------------------------------------------------------------------------
-- 消息体是 scripts/gen_user_events.py 生成的 JSON，8 个字段：
--   {"schema_version":1,"event_id":"<uuid>","event_time":"2026-11-02 10:00:00.123",
--    "event_type":"page_view","user_id":"<customer_unique_id>","session_id":"<uuid>",
--    "product_id":"<product_id>","page_url":"/product/<id>"}
--
-- 三个参数值得解释：
--
-- 1) scan.startup.mode = 'earliest-offset'
--    从头消费。理由：这个 topic 里已经有 2 万条历史消息，用 latest-offset 会
--    从「现在」开始，作业起来后一行数据都没有，看起来像链路坏了。
--    earliest-offset 会把 2 万条全部重放一遍 —— 顺便验证 event_id 唯一键的幂等：
--    重放多次结果行数恒定。
--
-- 2) properties.group.id 必须设。
--    不设的话 Kafka source 用不到 group 的位点提交，checkpoint 恢复时只能靠
--    Flink 自己存的 offset 状态；一旦作业被 cancel 后重新提交（不是 restore），
--    位点就丢了。设了 group.id 才有第二道保险。
--    注意：group.id 不能和 9 个 CDC 作业重复，它们是两套完全独立的消费者。
--
-- 3) 不设 json.ignore-parse-errors。
--    默认 false 意味着格式不对就抛异常、作业失败重启 —— 这是**故意的**。
--    埋点链路的契约（schema_version）就是要靠硬失败来守住；如果设成 true，
--    字段改名后所有消息会被静默解析成 NULL，Doris 里看到一堆空行却报不出错。
--    上游要做的是保证契约，不是让下游宽容。
CREATE TABLE src_user_event (
  schema_version INT,
  event_id       STRING,
  event_time     STRING,
  event_type     STRING,
  user_id        STRING,
  session_id     STRING,
  product_id     STRING,
  page_url       STRING
) WITH (
  'connector' = 'kafka',
  'topic'     = 'user_event',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-ods-user-event',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false'
);

-- -----------------------------------------------------------------------------
-- 目标表：Doris realtime_dw.ods_user_event
-- -----------------------------------------------------------------------------
-- 注意这张表**没有 op 列**（消息体里没有变更类型这个概念），
-- 元信息只有 dt / ts_ms 两个，与另外 9 张表不同 —— 详见 sql/doris/01-ods.sql 表 10。
CREATE TABLE sink_ods_user_event (
  event_id       STRING,
  event_time     TIMESTAMP(3),
  event_type     STRING,
  user_id        STRING,
  session_id     STRING,
  product_id     STRING,
  page_url       STRING,
  schema_version INT,
  `dt`    DATE,
  `ts_ms` BIGINT,
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes'   = 'doris-fe:8030',
  'username'  = 'root',
  'password'  = '',
  'table.identifier' = 'realtime_dw.ods_user_event',
  'sink.label-prefix' = 'ods_user_event',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete' = 'true'
);

INSERT INTO sink_ods_user_event
SELECT
  event_id,
  -- event_time 在 Kafka 里是字符串 'yyyy-MM-dd HH:mm:ss.SSS'，必须显式按格式解析。
  -- 直接 CAST 成 TIMESTAMP(3) 也能过，但会丢掉毫秒（Flink 的字符串转 timestamp
  -- 默认只认到秒），而毫秒恰好决定了同一秒内「加购」和「浏览」的先后 ——
  -- 丢了精度就会出现「加购时间早于浏览时间」的脏数据，漏斗转化率算出负数。
  TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss.SSS') AS event_time,
  event_type,
  user_id,
  session_id,
  product_id,
  page_url,
  schema_version,
  -- dt = 业务事件日期，与另 9 张表的「落地日期」口径不同，理由见文件头
  CAST(TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss.SSS') AS DATE) AS `dt`,
  -- ts_ms = 入库时刻。这里用 UNIX_TIMESTAMP() 无参版本取处理时间，
  -- **精度只到秒**（Flink SQL 没有直接返回 epoch 毫秒的函数，TIMESTAMPDIFF 的
  -- MILLISECOND 单位返回 INT，2026 年的 1.77e12 会溢出）。
  -- 对「链路延迟」这个用途足够：本管道的延迟量级是秒到十秒，±1 秒无影响。
  UNIX_TIMESTAMP() * 1000 AS `ts_ms`
FROM src_user_event;
