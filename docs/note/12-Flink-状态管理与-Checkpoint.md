# 12 · Flink 状态管理与 Checkpoint

> 一句话：流计算的状态就是"算到一半要记住的东西"。它决定了三件事 ——
> 作业能不能恢复、内存够不够、以及**下游多久能看到数据**（最后一条最容易被忽略）。

---

## 一、通用知识

### 1.1 状态从哪来

| 算子 | 记住什么 | 规模 |
| --- | --- | --- |
| `GROUP BY` 聚合 | 每个分组的累加值 | = 分组数 |
| `JOIN` | 两侧的输入缓冲（按 key） | ≈ 参与 join 的行数 |
| `ROW_NUMBER` 去重 | 每个 key 的第一条 | = distinct key 数 |
| `DISTINCT` | 已见值集合 | = distinct 值数 |
| CDC source | 当前 binlog 位点 + 快照 chunk 进度 | 很小 |

所以"状态大"通常等于"**join 大**"或"**distinct 多**"。

### 1.2 两种 state backend

| | `hashmap` | `rocksdb` |
| --- | --- | --- |
| 存在哪 | JVM 堆内（对象） | 堆外 + 本地磁盘（LSM） |
| 容量上限 | 受 taskHeap 限制 | 受磁盘限制，可远超内存 |
| 读写速度 | 快（无序列化） | 有序列化/反序列化开销 |
| 增量 checkpoint | ❌ 每次全量 | ✅ 只传 SST 变化 |
| 适用 | 状态小（几十~几百 MB） | 状态大（GB 级） |

`state.backend.incremental: true` 只对 rocksdb 有意义。

**在 Docker Desktop on Windows 上，RocksDB 可能慢到不可用**：本项目实测 checkpoint 卡在
`RocksDB.put` 的 native 调用上 5 分钟，而进程 CPU 只用了 2.3 秒 —— 时间全花在文件 I/O。
换成 `hashmap` 后 checkpoint 秒级完成。

### 1.3 checkpoint 是怎么做出来的

```
① JobManager 定期向 source 注入 barrier
② barrier 随数据流向下游传播
③ 算子收到 barrier：对齐（等所有输入的 barrier 到齐）→ 异步快照本地状态 → 上报
④ 全部完成后，JM 写一份"这次 checkpoint 的元数据"（含各算子的状态句柄）
⑤ 完成；下次从这份元数据恢复
```

关键点：

- **barrier 对齐会阻塞**：一条输入先到 barrier，另一条还没到，先到的那条要等 ——
  反压时会拖慢整条链
- **`unaligned checkpoint`** 取消对齐（把飞行中的数据也存下来），代价是状态更大
- **checkpoint 是"一致性快照"，不是"实时备份"**：它记录的是某一刻的全局一致状态

### 1.4 关键配置

```yaml
execution.checkpointing.interval: 10s            # 间隔
execution.checkpointing.timeout: 10m             # 单次超时
execution.checkpointing.min-pause: 5s            # 两次之间的最小间隔（防止连跑）
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.tolerable-failed-checkpoints: 3
execution.checkpointing.mode: EXACTLY_ONCE
state.checkpoints.dir: file:///opt/flink/checkpoints
```

**`interval` 不只是"多久备份一次"** —— 对带缓冲 sink 的连接器（Doris、Kafka 事务），
它同时是**数据可见延迟的上限**：sink 只在 checkpoint 完成时才提交这一批。

### 1.5 checkpoint 与 savepoint

| | checkpoint | savepoint |
| --- | --- | --- |
| 谁触发 | 框架自动 | 人手动 |
| 格式 | 内部格式，可能随版本变 | 标准化，跨版本兼容 |
| 用途 | 故障恢复 | 升级、迁移、改并行度 |
| 保留 | 按 `state.checkpointing.num-retained` 轮转 | 手动删除 |

**改并行度必须用 savepoint**，checkpoint 恢复会沿用旧的并行度。

### 1.6 状态大小怎么估

```
join 状态 ≈ 两侧参与 join 的行数 × 每行序列化后字节数
```

本项目实测：四表 join 的快照状态 **134MB**（checkpoint `state_size = 140,932,698` 字节）。
这个数字是后面所有内存计算的基准。

### 1.7 判活：不要相信算子指标

```
/jobs/{jid} 的 vertices[].metrics.read-records  → 缓存快照，可能长期 0
/vertices/{vid}/metrics?get=0.numRecordsIn       → 在 source 上恒为 0
```

本项目曾据此误判"作业空转"，查了 40 分钟才发现作业一直是好的。
**唯一可信的判据是目标系统的数据**：

```sql
SELECT COUNT(*), MAX(update_time) FROM 目标表;
```

或者改一行源数据，看目标表的 `SUM(x)` 变不变。

---

## 二、在本项目中

### 2.1 配置全貌

```yaml
# docker-compose.yml，JM 与 TM 都要写（两边配置必须一致）
execution.checkpointing.interval: 10s
execution.checkpointing.mode: EXACTLY_ONCE
state.checkpoints.dir: file:///opt/flink/checkpoints
state.backend: rocksdb
state.backend.incremental: true
```

但 **`07` 在 SQL 里覆盖成 hashmap**：

```sql
SET 'state.backend' = 'hashmap';
```

集群默认留 rocksdb，是因为它理论上更省内存、支持增量；实际运行中只有 07
的状态大到需要关注（134MB），而它偏偏受不了 RocksDB 在 Windows 上的 I/O 延迟。
这个"集群默认 + 单作业覆盖"的组合是有意的，不是漏改。

### 2.2 `interval: 10s` 是一个业务参数

对 Doris sink：**checkpoint 完成才提交这一批 Stream Load**。

```
interval = 10s  →  数据对 Doris 的可见延迟上限 ≈ 10s
```

这条直接决定了大屏的时延链路：

```
改一行 MySQL
  → 07 算子处理（毫秒级）
  → 攒在 sink buffer（最多 10s，等 checkpoint）
  → Stream Load 提交
  → 大屏 today 档刷新（1s）+ 前端轮询（1.5s）
  ≈ DWS 侧 1.4~4.2s
```

`04` 的注释里写过："不设的话，数据会一直攒在内存里，表现为「作业 RUNNING 但 Doris
查不到数据」" —— 这是新手最容易踩的坑之一。

### 2.3 134MB 撑爆 217MB：一次完整的内存事故

```
现象：4 个作业同时 RESTARTING
     UI 报 NoResourceAvailableException（"Could not acquire the minimum required resources"）
     集群 slots-total 归 0
     Doris 零行
```

因果链：

```
07 的四表 join 快照状态 134MB
  ÷ 4 slots = 217MB taskHeap/slot
  → 状态占 62%
  → checkpoint 需要 double buffer（旧状态 + 新快照）
  → 申请不到 → TM 被 cgroup OOM Kill
  → RestartCount 涨 1，4 个算子状态全丢
  → join 永远卡在 initializing
```

判据只有一条：

```bash
docker inspect rt-flink-tm --format 'RestartCount={{.RestartCount}} OOM={{.State.OOMKilled}}'
```

修法：`process.size` 2304m → **3072m**，`mem_limit` → 3328m，
taskHeap 1489MB，**每 slot 372MB**。

**为什么不是加 slot**：加 slot 是把同一块蛋糕切得更碎，状态没变小。
**为什么不是只降 slot**：从 10 降到 4 已经把每 slot 从 86.8MB 提到 217MB，
再降 slot 就装不下 4 个作业了。

### 2.4 `checkpoint_p95_ms` 的统计口径

健康度里的这一项取的是 **`/jobs/{jid}/checkpoints` 的 `history` 最近 10 次**，
不是 `summary`。

原因：`summary` 是**全生命周期**统计，被 CDC 初始快照期拖住 ——
一个作业刚跑完 41 万行快照，summary 里的 P95 会长期停在几十秒，
而它现在每次 checkpoint 只要几百毫秒。用 summary 会永久误报。

### 2.5 恢复策略：为什么不用 savepoint

session 集群 + SQL 提交，作业没有开 `execution.savepoint.path`。
每次重启都是"重新提交"，也就是从 `scan.startup.mode` 重来。

对 07 这意味着重放 41 万行（1~2 分钟），但换来"重启后自愈"（见 `11` 的僵尸作业）。
对小作业（04/06/09，Kafka 源 `earliest-offset`）意味着重放整个 topic（14.9 万条 / 6.5 分钟），
且会点亮 `event_age_p99_s` 约 10 分钟 —— 这是**已知的正常现象**。

真要做 savepoint：

```bash
docker exec rt-flink-jm ./bin/flink savepoint <jid> file:///opt/flink/checkpoints
# 恢复时加 SET 'execution.savepoint.path' = '...';
```

### 2.6 踩过的坑

**坑 1：`state.backend` 在两个地方设，后设的赢**

集群配置在 `FLINK_PROPERTIES`，作业配置在 SQL 的 `SET`。SQL 的 `SET` 在提交时生效，
覆盖集群值。排查"为什么状态后端和我配的不一样"时，先 `grep "state.backend" flink/sql/*.sql`。

**坑 2：checkpoint 目录会堆几十个作业 id**

`flink/checkpoints/` 下每个 jid 一个目录，每次重提都新增一个。几十个作业 id 之后
能占几百 MB。`.gitignore` 已排除，但磁盘上要手工清（只清没有作业在用的）。

**坑 3：`tolerable-failed-checkpoints` 为 0 时，一次失败就重启**

默认是 0。网络抖动导致一次 checkpoint 失败 → 作业重启 → 重放快照。
本项目没显式设，所以偶发的 checkpoint 失败会触发重启，表现为"作业自己重启了一次"。

---

## 三、速查

```bash
# 最近的 checkpoint（含 state_size）
curl -s localhost:8081/jobs/<jid>/checkpoints | python -m json.tool | head -40

# 只取最近几次的耗时与大小
curl -s localhost:8081/jobs/<jid>/checkpoints | python -c "
import sys,json
d=json.load(sys.stdin)
for c in d.get('history',[])[-5:]:
    print(c.get('id'), c.get('status'), c.get('duration'), c.get('state_size'))"

# TM 是否被 OOM（唯一可信）
docker inspect rt-flink-tm --format 'RestartCount={{.RestartCount}} OOM={{.State.OOMKilled}}'

# 状态后端实际值
grep -rn "state.backend" flink/sql/*.sql docker/docker-compose.yml

# 磁盘占用
du -sh flink/checkpoints
```

| 现象 | 判据 |
| --- | --- |
| `slots-total=0` + 作业 RESTARTING | TM OOM，看 `RestartCount` |
| 作业 RUNNING 但下游没数据 | 不一定是状态问题，先看 sink 的 flush 策略（`17`） |
| checkpoint 一直 `IN_PROGRESS` | 状态太大 / 反压 / 外部系统慢 |
| checkpoint_p95 长期几十秒 | 看是不是用了 summary 口径（含初始快照） |
| 作业自己重启了一次 | `tolerable-failed-checkpoints=0`，一次失败就重启 |

延伸：`09-Apache-Flink.md`（slot 与内存模型）、`11-Flink-CDC.md`（僵尸作业）、
`17-Stream-Load-与-Doris-Sink.md`（checkpoint 与导入的关系）
