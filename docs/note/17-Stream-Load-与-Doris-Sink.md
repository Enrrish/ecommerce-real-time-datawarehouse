# 17 · Stream Load 与 Doris Sink

> 一句话：Stream Load 是 Doris 的"一次 HTTP PUT 导入一批数据"。它没有"事务提交"这种
> 显式动作 —— **一次请求就是一个版本**，所以"多久发一次请求"同时决定了
> 可见延迟、版本增长速度和内存占用。

---

## 一、通用知识

### 1.1 Stream Load 怎么工作

```bash
curl -X PUT "http://be:8040/api/{db}/{table}/_stream_load" \
  -H "label: my-label-001" \
  -H "format: csv" \
  -H "column_separator: \t" \
  -T data.tsv
```

返回 JSON：

```json
{"Status":"Success","NumberTotalRows":1000,"NumberLoadedRows":1000,
 "NumberFilteredRows":0,"Label":"my-label-001","TxnId":12345}
```

流程：

```
客户端 → FE（拿路由）→ 重定向到某个 BE → BE 直接收数据 → 写 rowset → 返回
```

### 1.2 label：Stream Load 的唯一幂等手段

**同一个 label 重复提交，Doris 返回上一次的结果，不会重复导入。**

```bash
-H "label: order_detail_20261104_001"
```

所以生产上 label 要"可复现"（含批次号/日期）。Flink Connector 会自动生成
`{jobId}-{taskId}-{checkpointId}` 形式的 label —— 这就是"从 checkpoint 恢复不会重复导入"
的原理。

### 1.3 关键的请求头

| 头 | 含义 | 本项目用法 |
| --- | --- | --- |
| `format` | `csv` / `json` / `parquet` | csv（Connector 默认） |
| `column_separator` | 列分隔符 | `\t` |
| `strict_mode` | 严格模式：类型不符/超长直接失败 | 开 |
| `max_filter_ratio` | 允许的脏数据比例，默认 0 | 0 |
| `timeout` | 单次导入超时 | 600s |
| `label` | 幂等键 | 自动 |

`strict_mode=true` + `max_filter_ratio=0` 的组合意味着**一行不合格整批失败**。
对演示项目是对的（错了要立刻知道），对生产通常要放宽并记录 ErrorURL。

### 1.4 一次导入 = 一个版本

```
第 1 次 Stream Load → version 1
第 2 次 Stream Load → version 2
...
```

版本要 compaction 才能合并（见 `16`）。所以**导入频率有下限**：

| 频率 | 每小时版本数 | 后果 |
| --- | --- | --- |
| 1 秒 | 3,600 | 几小时就打爆 |
| 10 秒 | 360 | 可接受 |
| 1 分钟 | 60 | 很健康 |

经验值：**≥ 10 秒**。

### 1.5 Flink Doris Connector 的三个 buffer 参数

```sql
'sink.buffer-flush.max-bytes' = '10485760'    -- 攒到 10MB 就发
'sink.buffer-flush.max-rows'  = '100000'      -- 攒到 10 万行就发
'sink.buffer-flush.interval'  = '10s'         -- 或者每 10 秒发一次
```

三个条件是"或"的关系：任一满足就 flush。

两个陷阱：

**① `max-bytes` 有下限（10MB），业务数据永远够不到**

本项目的每批数据只有几十 KB，`max-bytes` 永远触发不了；
`max-rows` 也触发不了（一批不到 100 行）。
→ **实际只靠 `interval`**，也就是**只靠 checkpoint**（如果 interval 没设）。

**② `interval` 设成 1s 会让版本爆炸**

见 1.4。这是本项目真实事故的直接原因。

### 1.6 `sink.write-mode`：环形缓冲的死锁

Connector 内部有一个环形缓冲（ring buffer）承接异步 flush。

```
默认模式：一次 flush 的数据量 ≥ 环容量
  → 写端等读端腾空间，读端等写端把整批写完
  → 两端互等，**零报错**
```

现象：作业 `RUNNING`、checkpoint 死卡、Doris 里零行。

修法：

```sql
'sink.write-mode' = 'STREAM_LOAD_BATCH'
```

批模式会先攒够再一次性发，不做"边写边发"，绕开环形缓冲的容量假设。

### 1.7 Doris Connector 只接受 upsert changelog

Flink 的聚合结果是一个 changelog（`+I/-U/+U`）。Doris 的 UNIQUE 表能表达
"这一行被替换"，但不能表达"删除一行"。

所以 Connector 会在需要时自动插入 `SinkUpsertMaterializer` 算子，
把 changelog 物化成"最终状态"。代价：多一个算子、多一份状态。

这也是"DWS 必须用 UNIQUE 而不是 AGGREGATE"的原因之一（见 `15`）。

---

## 二、在本项目中

### 2.1 配置全貌（以 07 为例）

```sql
CREATE TABLE sink_dws_trade_overview_1m (...) WITH (
  'connector'                 = 'doris',
  'fenodes'                   = '172.28.0.10:8030',
  'table.identifier'          = 'realtime_dw.dws_trade_overview_1m',
  'username'                  = 'root',
  'password'                  = '',
  'sink.label-prefix'         = 'dws_overview_1m',
  'sink.properties.format'    = 'csv',
  'sink.properties.column_separator' = '\t',
  'sink.properties.strict_mode'      = 'true',
  'sink.enable-delete'        = 'true',
  -- 07 是唯一可以给 1s 的作业：它的源是业务库，没有持续事件流，
  -- 没有数据就不 flush，也就不产生版本（详见 2.5）
  'sink.buffer-flush.interval'= '1s',
  'sink.write-mode'           = 'STREAM_LOAD_BATCH'
);
```

### 2.2 "作业 RUNNING 但 Doris 查不到数据"

这是新手第一课，也是本项目注释里专门写下的：

```
Doris Connector 靠 checkpoint 触发 Stream Load 提交。
checkpoint 间隔 = 数据可见延迟上限。
不设 interval 的话，数据会一直攒在内存里，
表现为「作业 RUNNING 但 Doris 查不到数据」。
```

所以 `execution.checkpointing.interval: 10s` 不只是"备份频率"，
它是**业务参数**。大屏的时延链路里，这 10 秒是最大的一段：

```
改一行 MySQL
  → 07 算子处理（毫秒）
  → sink buffer 等 checkpoint（最多 10s）
  → Stream Load 提交
  → 大屏 today 档（1s）+ 前端轮询（1.5s）
  ≈ 1.4 ~ 4.2s（运气好赶上 checkpoint 就快）
```

### 2.3 三种"延迟"的区别

| 说法 | 数值 | 口径 |
| --- | --- | --- |
| DWS 落库 | 1.4 ~ 4.2s | 改一行 MySQL → DWS 表里能查到 |
| ADS 可见 | 10.7 ~ 56.4s | 还要等 ADS 的 60s 批刷相位 |
| 面板可见 | ≈ ADS + 1.2s | 再加前端轮询 |

**同一句"改一行 MySQL → 大屏可见"有四个不同的数字，取决于走哪条路**。
不标口径就会吵架（见 `26`）。

### 2.4 版本爆炸事故的完整链路

```
配置：sink.buffer-flush.interval = '1s'
动机：降低延迟（10s 太久）

3 小时后：
  dwd_user_event_clean   单 tablet 4,434 版本
  dws_traffic_funnel_1h  6,276 版本（表里只有 56 行）
  BE 内存 2.25GB（limit 2560m）→ compaction 停摆
  COUNT(*) GROUP BY：60ms → 28s

恢复：
  TRUNCATE 三张表 + 从 Kafka 重放 77,948 条
  行数与清空前逐表一致
```

**降延迟的正确做法不是调导入频率**，而是：
- 大屏侧用 `today` 档单独刷今天那几十行（见 `23`）
- 或者接受 10 秒的可见延迟

### 2.5 `INSERT OVERWRITE` 是 Doris 侧的事，不是 Sink

ADS 层不经过 Flink，由大屏后端的 Python 进程执行 `INSERT OVERWRITE`（见 `23`）。
两者的区别：

| | Stream Load（Flink Sink） | INSERT OVERWRITE（SQL） |
| --- | --- | --- |
| 谁发起 | Flink 算子 | 后端进程 |
| 粒度 | 一批行 | 整表 |
| 幂等 | label | 天然（先清后写） |
| 版本 | 每次一批一个版本 | 每次刷新一个版本（更大） |

ADS 每 60 秒 `INSERT OVERWRITE` 一次，等于每分钟产生 9 张表的版本 →
这就是 `trash/` 稳态增长的来源（见 `16`）。

### 2.6 踩过的坑

**坑 1：`sink.buffer-flush.max-bytes` 的下限是 10MB**

想靠"攒够 10MB 就发"来降延迟是无效的 —— 业务数据永远够不到。
必须显式设 `interval`，而 `interval` 又受 10 秒下限约束。

**坑 2：环形缓冲死锁零报错**

```
现象：作业 RUNNING、checkpoint 死卡、Doris 零行、日志里什么都没有
原因：一次 flush ≥ 环容量，写端读端互等
修法：sink.write-mode = 'STREAM_LOAD_BATCH'
```

**坑 3：删源表数据不会删 Doris 数据**

链路是 upsert，不是"同步删除"。MySQL 删一行 → CDC 发 `-D` → Flink 输出 retract →
Doris 侧需要 `sink.enable-delete=true` 且表是 UNIQUE 才能处理。
即便如此，**已聚合的行不会自动消失**，清库要同时清 Doris。

---

## 三、速查

```sql
-- 最小 sink 配置
CREATE TABLE sink_x (...) WITH (
  'connector'='doris',
  'fenodes'='172.28.0.10:8030',
  'table.identifier'='realtime_dw.x',
  'username'='root','password'='',
  'sink.label-prefix'='x',
  'sink.properties.format'='csv',
  'sink.properties.column_separator'='\t',
  'sink.properties.strict_mode'='true',
  'sink.buffer-flush.interval'='10s',
  'sink.write-mode'='STREAM_LOAD_BATCH'
);
```

```bash
# 手工打一次 Stream Load（验证表与权限）
printf '1\t2026-11-05\t100.00\n' | curl -X PUT \
  "http://127.0.0.1:8040/api/realtime_dw/test_tbl/_stream_load" \
  -H "label: manual-$(date +%s)" -H "format: csv" -H "column_separator: \t" \
  -H "strict_mode: true" --data-binary @-

# 看版本增长（导入前后各一次）
docker exec rt-doris-fe mysql -uroot -P9030 -h127.0.0.1 -e \
  "SHOW TABLETS FROM realtime_dw.dws_trade_overview_1m" | awk -F'\t' 'NR>1{print $16}' | sort -n | tail -1
```

| 现象 | 原因 |
| --- | --- |
| 作业 RUNNING 但 Doris 无数据 | checkpoint 没设 / interval 太长 / 环形缓冲死锁 |
| 版本数几小时涨到几千 | `interval` 设得太短（<10s） |
| 导入报 `filtered data in strict mode` | 列超长/类型不符；看 ErrorURL |
| 重复导入产生重复行 | label 不唯一，或表不是 UNIQUE |
| checkpoint 死卡 + Doris 零行 | 环形缓冲死锁，改 `STREAM_LOAD_BATCH` |
| `SinkUpsertMaterializer` 占大量状态 | 上游 changelog 太大，考虑先聚合 |

延伸：`15-Doris-数据模型.md`、`16-Doris-分区-分桶与-Compaction.md`、
`12-Flink-状态管理与-Checkpoint.md`、`26-时延测量与压测.md`
