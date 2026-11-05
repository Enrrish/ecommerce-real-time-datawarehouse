# 08 · Apache Kafka

> 一句话：Kafka 是一个**只能追加、按分区有序、可被多方重复读**的日志。
> "只能追加"这一条决定了它所有优点，也决定了它最坑人的地方 —— 脏消息删不掉。

---

## 一、通用知识

### 1.1 四个概念

| 概念 | 含义 | 关键约束 |
| --- | --- | --- |
| **topic** | 逻辑上的消息类别 | 名字全局唯一 |
| **partition** | topic 的物理分片，每个是一个有序日志 | **只能增不能减** |
| **offset** | 分区内的单调递增编号 | 只在**分区内唯一**，不是全局 |
| **consumer group** | 一组消费者共同消费一个 topic | 同组内一个分区只给一个消费者 |

顺序性：**单个 partition 内保证顺序，跨 partition 不保证**。
需要"同一实体的事件有序"，就用该实体 ID 做消息 key（同 key 落同一分区）。

### 1.2 位点（offset）与 `auto.offset.reset`

消费者提交的位点存在 `__consumer_offsets` 里（老版本在 ZooKeeper）。
当**没有已提交位点**时，`auto.offset.reset` 决定从哪开始：

| 值 | 行为 |
| --- | --- |
| `earliest` | 从分区最早可读位点开始（会把历史全读一遍） |
| `latest` | 从当前末尾开始（起来后一条历史都没有） |
| `none` | 抛异常 |

Flink 的 Kafka source 里对应 `scan.startup.mode`：
`earliest-offset` / `latest-offset` / `committed-offset` / `group-offsets` / `timestamp`。

⚠️ **区分两件事**：`auto.offset.reset` 只在"没有位点"时生效；
Flink 作业的 `scan.startup.mode` 在**每次提交**时都生效（除非从 savepoint 恢复）。
所以 `latest-offset` 的作业 cancel 后重提，等于从当下重新开始 —— 历史一条不读。

### 1.3 KRaft：不要 ZooKeeper 了

Kafka 3.3 起 KRaft（Kafka Raft）可用于生产，3.5+ 新建集群默认。它把元数据管理
内置成一组 controller 节点，不再依赖 ZooKeeper。

```yaml
environment:
  KAFKA_NODE_ID: 1
  KAFKA_PROCESS_ROLES: broker,controller
  KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
  KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
```

好处：少一个组件、启动快、元数据规模不受 ZK 限制。
代价：老运维习惯的 `zkCli.sh` 那套没了，`kafka-topics.sh` 成为唯一入口。

### 1.4 保留与压缩

```properties
log.retention.hours=168          # 按时间（默认 7 天）
log.retention.bytes=-1           # 按大小（-1 不限）
log.cleanup.policy=delete        # delete / compact
```

`compact` 是"按 key 保留最后一条"，适合 changelog 场景（如 Kafka Connect 的
状态 topic），不适合事件流 —— 事件流要保留每一条。

### 1.5 常用命令

```bash
# topic
kafka-topics.sh --bootstrap-server kafka:9092 --list
kafka-topics.sh --bootstrap-server kafka:9092 --create \
  --topic user_event --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic user_event

# 位点（-1 = 最新，-2 = 最早）
kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic user_event --time -1

# 读
kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic user_event --partition 0 --offset latest --max-messages 2 --timeout-ms 6000

# 写
echo '{"event_id":"x"}' | kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic user_event

# 消费组（lag 看这里）
kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups
# GROUP  TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
```

**lag = LOG-END-OFFSET − CURRENT-OFFSET**，是"还差多少条没消费"。
lag 长期不降 = 消费端有问题；lag 为 0 且不涨 = 消费完了（或没有新数据）。

### 1.6 三个认知误区

**误区 1：消息可以被修改或删除**
不能。Kafka 不提供按 offset 删除单条消息的 API（只有 `deleteRecords` 按位点截断，
那是"删掉该位点之前的所有消息"）。所以**一条毒消息会永久留在那里**。

**误区 2：分区越多越快**
分区是并行度单位，也是元数据与文件句柄的开销。3 分区单机足够本项目用。
分区数增加会让 key 的路由变化（`hash(key) % partitions`），历史同 key 的消息
可能落在不同分区，顺序性被破坏。

**误区 3：consumer group 的位点是"消息被处理完"的标记**
自动提交（`enable.auto.commit=true`）是按**拉取**提交的，不是按处理完成。
at-least-once 语义下重复是常态，所以下游必须幂等 —— 这正是本项目全库用 UNIQUE 模型的原因。

---

## 二、在本项目中

### 2.1 一个 topic，三个消费者组

```
topic: user_event（3 分区，1 副本）

  ├─ group: flink-ods-user-event      → 04 作业 → ods_user_event
  ├─ group: flink-dwd-user-event      → 06 作业 → dwd_user_event_clean
  └─ group: flink-dws-event-payment   → 09 作业 → 漏斗 + 支付
```

三个作业**各自独立消费同一份数据**，互不影响（不同 group 的位点独立）。
这是 Kafka 相比队列（RabbitMQ）最大的区别：读不会"消耗"消息。

为什么不串成一条链（04 → 06 → 09）：那样 06 要读 Doris（多一跳，慢），
或者 04 写第二个 topic（多一层运维）。三组直连 Kafka 更简单，
代价是同一份数据被解析三次 —— 3 分区 / 每秒几十条的量级完全可以接受。

### 2.2 分区键与顺序

消息体 8 个字段：

```json
{"schema_version":1, "event_id":"<uuid>", "event_time":"2026-11-02 10:00:00.123",
 "event_type":"page_view", "user_id":"<customer_unique_id>", "session_id":"<uuid>",
 "product_id":"<product_id>", "page_url":"/product/<id>"}
```

生产端按 `session_id` 做 key（同会话落同一分区），保证"同一用户的一次会话内
事件有序" —— 漏斗计算依赖这个顺序（先 page_view 才能 add_to_cart）。

注意：**跨会话不保证有序**，所以漏斗的分母是"人次"而不是"会话流转"，
口径上必须写清楚（见 `20`）。

### 2.3 `earliest-offset` 的三个理由

```sql
'scan.startup.mode' = 'earliest-offset'
```

1. topic 里已有历史消息，用 `latest-offset` 起来后一行数据都没有，**看起来像链路坏了**
2. 重放顺便验证 `event_id` 唯一键的幂等：重放多次结果行数恒定
3. 增量快照期（如果以后开）需要读到位点之前的数据

代价：每次重提都重放整个 topic。实测 14.9 万条 / 6.5 分钟。
这个代价会带来一个**必现的假告警**，见 2.5。

### 2.4 毒消息会把链路永久打死

```
一条 2 字节的 `{}`  → event_id 为 NULL → ConstraintEnforcer 抛异常
40 条 schema_version 写成字符串 "1.0" 的探针 → JSON 解析异常
```

结果一样：**作业无限重启，位点永远停在它前面**，Kafka 积压涨到 5.4 万条，
UI 上只是反复 `RESTARTING`。

为什么"硬失败守住契约"这个想法是错的：

- Kafka 是**只读追加**的，那条消息永远在那儿，重试一万次还是它
- 唯一的恢复手段是人工改消费位点（`kafka-consumer-groups.sh --reset-offsets`）
- 这不是"守住契约"，是把"一条脏消息"放大成"整条链路停摆 + 手工救火"

正确做法：`json.ignore-parse-errors = 'true'` 让它继续跑，同时把错误暴露出来：

- source 的 `numRecordsInErrors`（解析失败条数）
- DWD 里 `is_valid` / `invalid_reason` 记录字段级问题

**"可观测的降级"优于"不可恢复的严格"。**

### 2.5 重放会点亮 `event_age_p99_s` 约 10 分钟

这是本项目最像故障的正常现象，值得记住：

```
重投 04/06/09（earliest-offset）→ 重放整个 topic（14.9 万条 / 6.5 分钟）
  → 这些事件的 event_time 是 5 天前的，但 ts_ms 被写成"现在"
  → 指标窗口是 ts_ms >= now - 600s
  → 重放本身就在窗口里 → event_age_p99 报 431,377 秒（5 天）→ critical
```

时间点是可预测的：重放 `16:23:55 → 16:26:40`，指标在 **`16:36:40`**（结束 + 600 秒）
准时回到 5 秒。

**不要改阈值、不要加 `event_time > now() - interval 1 hour` 过滤** ——
那会把真正的积压一起滤掉。判据是：

```sql
SELECT COUNT(*) FROM ods_user_event WHERE ts_ms >= (UNIX_TIMESTAMP() - 600) * 1000;
```

这个数掉回正常量级（几十~几百），指标自然恢复。

### 2.6 命令行在容器里的两个细节

```bash
# 脚本在 /opt/kafka/bin，容器里没有 PATH
docker exec rt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# 镜像里没有 gawk 这个名字，但有 /usr/bin/awk（就是 gawk）
docker exec rt-kafka /usr/bin/awk 'BEGIN{print strftime("%Y-%m-%d", 1762300000)}'
```

`kafka:9092` 是容器内监听地址；宿主访问用 `localhost:29092`（compose 里映射的）。

---

## 三、速查

```bash
# topic 概况（分区、副本、ISR）
docker exec rt-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --describe --topic user_event

# 每个分区的最早/最新位点
docker exec rt-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic user_event --time -1

# 三个消费者组的 lag
docker exec rt-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --all-groups

# 抽最新 2 条看事件时间（判"有没有新鲜数据"）
docker exec rt-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic user_event --partition 0 \
  --offset latest --max-messages 2 --timeout-ms 6000

# 造一条（验证链路）
echo '{"schema_version":1,"event_id":"probe-1","event_time":"2026-11-05 10:00:00.000","event_type":"page_view","user_id":"u1","session_id":"s1","product_id":"p1","page_url":"/p/p1"}' \
| docker exec -i rt-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic user_event
```

| 现象 | 判据 |
| --- | --- |
| lag 一直涨 | 消费端挂了 / 毒消息卡住位点 / 分区数与并行度不匹配 |
| lag 0 但目标表不写 | 不是 Kafka 的事，去看 Flink（`11`） |
| 起来后一行数据没有 | `latest-offset` + topic 里本来就没新消息 |
| 作业反复 RESTARTING | 看是不是某条消息解析失败（`numRecordsInErrors`） |
| `event_age_p99` 亮红约 10 分钟 | 刚重放过 topic，正常，别改阈值 |

延伸：`07-MySQL-Binlog-与-CDC-原理.md`（另一条链路）、`11-Flink-CDC.md`、
`13-时间语义与-Watermark.md`、`25-可观测性与链路健康度.md`
