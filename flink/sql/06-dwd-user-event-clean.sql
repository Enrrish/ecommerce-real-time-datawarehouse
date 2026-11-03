-- =============================================================================
-- 06-dwd-user-event-clean.sql —— 行为明细（清洗后）
-- -----------------------------------------------------------------------------
-- 数据源：Kafka topic user_event（与 04-ods-user-event.sql 同一个 topic）
-- 目标：realtime_dw.dwd_user_event_clean
-- 提交：bash scripts/submit-sql.sh flink/sql/06-dwd-user-event-clean.sql
--
-- 验收：行数 = 22,004（topic 22,064 条消息，60 条 event_id 重复被主键去重）
--
-- =============================================================================
-- 清洗到底做了什么
-- =============================================================================
--   ① 形状校验 + 解析 event_time，形状不对则置 NULL（不抛异常）
--   ② 硬过滤：event_id 为空的行直接丢弃
--   ③ 软校验：其余字段问题标记 is_valid = 0，**行保留**
--
-- 为什么「硬过滤」和「软校验」要分开：
--   event_id 是这张表的去重主键，没有它这一行既进不了 Doris（NOT NULL 列），
--   也没法去重，留着毫无意义 —— 只能丢。丢了多少有据可查：拿 ods_user_event
--   的行数减 dwd_user_event_clean 的行数就是被丢掉的条数。
--   其他字段缺失不影响主键，就标 is_valid = 0 留着。悄悄丢掉脏行是最糟的做法：
--   上游改了字段名之后你会得到一张「干净」的空表，而且完全看不出来。
--
-- 为什么 is_valid 不做成过滤条件：
--   DWD 是明细层，职责是把「发生了什么」如实记下来，判断属于下游。
--   DWS 聚合时带 `WHERE is_valid = 1` 即可；反过来如果这里就丢掉，
--   数据质量问题在 DWS 侧会表现为「指标偏低」，根本定位不到原因。
--
-- =============================================================================
-- 这个作业**没有**会话内序号（试过，撤了）
-- =============================================================================
-- 最初设计里有一列 session_seq，用
--   ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time)
-- 在流里现算。实跑踩到两个坑，最后把这列删掉了：
--
-- 坑 1：Flink 流模式的 OVER 聚合强制 ORDER BY 落在**时间属性**上，于是必须给
--       event_time 加 WATERMARK。而水位线的定义就决定了它永远落后于最新数据 ——
--       实测 topic 尾部 10:22~10:23 的 8 条支付事件，因为后面再没有新事件推动
--       水位线，一直卡在 OVER 算子的状态里出不来。22,064 条消息只落库 21,996 条，
--       缺的正好是这 8 条。生产环境持续有流量所以看不出来，离线回放就是实打实
--       的缺数 —— 这种"只在安静的时候出错"的问题最难查。
--
-- 坑 2：序号只随到达顺序递增，不会回头修正。它"正确"的前提是同一会话的事件
--       严格有序到达，这要靠生产端用 session_id 做消息 key 来保证（本项目的
--       scripts/gen_user_events.py 确实是这么做的）。这是个隐式契约，换个上游
--       就悄悄错了，而且错了没有任何报错。
--
-- 关键判断：序号本身并不需要在写入时固化。event_time 精确到毫秒且不可变，
-- DWS 算漏斗直接 `ORDER BY event_time` 就能拿到真实顺序，永远是准的。
-- 把一个会错的派生列固化进 DWD，去换一个本来就不存在的性能问题，不划算。
--
-- 顺带说清之前 dim_user_zip 上 OVER 聚合失败的另一种情形（报
-- "StreamPhysicalOverAggregate doesn't support consuming update and delete changes"）：
-- 那是 **CDC changelog** 流的问题（带 -U/+U），和这里的水位线问题是两回事。
-- OVER 聚合对输入的要求是「append-only」，Kafka 事件流满足，CDC 变更流不满足。
--
-- =============================================================================
-- sink 配置为什么这么长
-- =============================================================================
-- 那几个 buffer 参数不是抄来的，是 05 号作业踩出来的：connector 24.0.1 的
-- Stream Load 请求体走流式发送，默认 1MB × 3 的环形缓冲在一次 flush 数据量
-- 超过环容量时会两端互等死锁 —— 作业 RUNNING、checkpoint 卡死、Doris 零行、
-- 日志一条错误都没有。05 号文件里有完整的排查记录，这里照搬同样的配置。
-- =============================================================================
SET 'execution.attached' = 'false';
SET 'table.local-time-zone' = 'Asia/Shanghai';

-- 与 05 号作业同理：状态量不大但写入模式碎片化，堆内状态比 RocksDB 快得多，
-- 而且在这个环境（Docker Desktop on Windows）里 RocksDB 的磁盘 I/O 是主要瓶颈。
SET 'state.backend' = 'hashmap';


-- -----------------------------------------------------------------------------
-- Source：Kafka topic user_event
-- -----------------------------------------------------------------------------
-- group.id 必须与 04-ods-user-event.sql 的 'flink-ods-user-event' 不同。
-- 两个作业读同一个 topic 但属于两个独立消费者组，各记各的位点 —— 这也是为什么
-- DWD 能直接从 Kafka 取数而不必等 ODS 落库。
--
-- scan.startup.mode 用 earliest-offset：topic 里已有 2 万多条历史消息，用
-- latest-offset 会让作业起来后一行数据都没有，看起来像链路坏了。
-- 顺便验证幂等性：这张表是 UNIQUE 主键，重放多少次行数都恒定。
CREATE TABLE src_user_event (
  schema_version INT,
  event_id       STRING,
  event_time     STRING,
  event_type     STRING,
  user_id        STRING,
  session_id     STRING,
  product_id     STRING,
  page_url       STRING,

  -- 解析后的时间做成计算列，供下游直接引用（省得每处都写一遍 CASE）。
  -- 这里**不加 WATERMARK** —— 没有 OVER 聚合就不再需要时间属性，
  -- 而水位线恰恰是上面坑 1 的来源。普通计算列没有"落后于最新数据"的问题。
  evt_time AS (
    CASE
      WHEN event_time LIKE '____-__-__ __:__:__.___'
        THEN TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss.SSS')
      ELSE CAST(NULL AS TIMESTAMP(3))
    END
  )
) WITH (
  'connector' = 'kafka',
  'topic'     = 'user_event',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-dwd-user-event-clean',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  -- 与 ODS 侧保持一致：缺字段解析成 NULL 而不是让作业挂掉。
  -- 真正的契约校验放在下面的 is_valid 里做，这样能**看见**脏数据有多少，
  -- 而不是靠作业失败来间接发现。
  'json.fail-on-missing-field' = 'false'
);


-- -----------------------------------------------------------------------------
-- Sink：Doris realtime_dw.dwd_user_event_clean
-- -----------------------------------------------------------------------------
CREATE TABLE sink_dwd_user_event_clean (
  `event_id`    STRING,
  `dt`          DATE,
  `user_id`     STRING,
  `session_id`  STRING,
  `product_id`  STRING,
  `event_type`  STRING,
  `event_time`  TIMESTAMP(3),
  `page_url`    STRING,
  `is_valid`    TINYINT,
  `update_time` TIMESTAMP(3)
) WITH (
  'connector'              = 'doris',
  'fenodes'                = 'doris-fe:8030',
  'table.identifier'       = 'realtime_dw.dwd_user_event_clean',
  -- username/password 必须显式写：connector 默认 username 是 null 不是 root，
  -- 漏写会 401，而它把 401 的文本当 JSON 解析，报的是「Cannot deserialize ...」。
  'username'               = 'root',
  'password'               = '',
  'sink.label-prefix'      = 'dwd_user_event_clean',
  'sink.properties.format' = 'json',
  'sink.properties.read_json_by_line' = 'true',
  'sink.enable-delete'     = 'true',
  -- 流式请求体的环形缓冲：必须让「一次 flush 的量」< 「环容量」，否则死锁。
  -- max-bytes 的 10MB 是 connector 允许的下限，环给到 32MB。
  'sink.buffer-size'       = '1048576',
  'sink.buffer-count'      = '32',
  'sink.buffer-flush.max-bytes' = '10485760',
  -- 时间触发（P7 加，与 09 号作业同理）：max-bytes 是 10MB 下限，而埋点每次
  -- 只有几百字节，够不到阈值就只能等 checkpoint（10s）。这一层是大屏漏斗的
  -- 上游，卡在这里等于整条链路都慢 10 秒 —— 实测加 1s flush 后端到端
  -- 从 10.5s 降到 4s 以内。
  'sink.buffer-flush.interval'  = '1s',
  -- 绕开上面那条流式管道，走「先攒后发」的整批请求体。
  'sink.write-mode'        = 'STREAM_LOAD_BATCH'
);


-- -----------------------------------------------------------------------------
-- 装载：清洗
-- -----------------------------------------------------------------------------
-- event_time 先做「形状校验」再解析，是有意的：
--   TO_TIMESTAMP(str, fmt) 遇到不符合格式的字符串会抛异常，异常一路冒到作业，
--   触发 failover 重启 —— 一条脏数据就能把整条链路打翻。而脏数据是必然会来的
--   （P6 就要主动注入不符合 schema_version 的消息来验证告警）。
--   所以只对「形状正确」的字符串做解析，形状不对的直接给 NULL，
--   由 is_valid 记录下来，作业继续跑。
--
--   形状校验用 LIKE 的下划线通配，不用正则：Flink 1.18 的 SQL 里没有 RLIKE
--   中缀运算符（会报 "No match found for function signature RLIKE"），而
--   LIKE 的 `_` 恰好是「任意单字符」，表达「23 个字符、分隔符在固定位置」
--   比正则更短也更好读，还绕开了反斜杠在 SQL 字符串里的二次转义：
--       ____-__-__ __:__:__.___
--       yyyy-MM-dd HH:mm:ss.SSS
INSERT INTO sink_dwd_user_event_clean
SELECT
  t.event_id,
  -- dt 是 NOT NULL 的分区列：解析失败的行走今天的分区。
  -- 不这么做的话这批行会因「找不到分区」整批导入失败 —— 一条脏数据毁掉一整批。
  COALESCE(CAST(t.evt_time AS DATE), CAST(CURRENT_TIMESTAMP AS DATE)) AS `dt`,
  t.user_id,
  t.session_id,
  t.product_id,
  t.event_type,
  t.evt_time AS event_time,
  t.page_url,
  -- is_valid：1 = 通过全部校验。逐条写清楚，方便 DWS 侧做数据质量看板时
  -- 按不同校验项拆分口径 —— 如果压成一个布尔，出问题时只能整体判断。
  CAST(CASE
    WHEN t.evt_time IS NULL                                          THEN 0  -- 时间/格式非法
    WHEN t.user_id IS NULL OR TRIM(t.user_id) = ''                    THEN 0  -- 匿名
    WHEN t.session_id IS NULL OR TRIM(t.session_id) = ''              THEN 0  -- 无会话，漏斗断链
    WHEN t.event_type IS NULL
         OR t.event_type NOT IN ('page_view','add_cart','checkout','pay') THEN 0  -- 白名单外
    ELSE 1
  END AS TINYINT) AS is_valid,
  LOCALTIMESTAMP AS update_time
FROM src_user_event t
-- event_id 为空的行在这里过滤掉：它是主键，没有主键既进不了 Doris 也没法去重。
-- 这是唯一一处物理丢弃，数量可查（ods_user_event 行数 - 本表行数）。
WHERE t.event_id IS NOT NULL AND TRIM(t.event_id) <> '';
