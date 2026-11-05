# 09 · Apache Flink

> 一句话：Flink 是**以"有状态流"为核心抽象**的计算引擎。它和"批处理 + 定时跑"的根本
> 区别不是快，而是**状态是一等公民** —— 你声明"记住什么"，它负责持久化和恢复。

---

## 一、通用知识

### 1.1 运行时三角

```
┌────────────── JobManager（大脑）──────────────┐
│  Dispatcher       接收提交、起 JobMaster       │
│  ResourceManager  管 slot、向 TM 申请资源      │
│  JobMaster        管一个作业的 DAG、checkpoint │
└───────────────────┬───────────────────────────┘
                    │ 注册 / 心跳 / 状态汇报
┌───────────────────┴───────────────────────────┐
│  TaskManager（干活的）                         │
│   ┌────────┬────────┬────────┬────────┐       │
│   │ slot 1 │ slot 2 │ slot 3 │ slot 4 │       │
│   └────────┴────────┴────────┴────────┘       │
│   共享：网络缓冲、内存管理、JVM                │
└───────────────────────────────────────────────┘
```

- **一个 slot 跑一条 task 链**（一个算子链的并行子任务）
- slot 是**内存切分单位**，不是 CPU 独占单位
- 同一个 TM 上的多个 slot **共享网络缓冲和 JVM 开销**

### 1.2 `slot` 与 `taskHeap`：最容易被误解的一处

TM 的总内存被切成几块：

```
process.size = taskHeap + managedMemory + network + jvmMetaspace + jvmOverhead
```

`taskHeap` 再**按 slot 数均分**：

```
numberOfTaskSlots = 4  →  每个 slot 拿到 taskHeap / 4
```

这意味着：**加 slot 数等于把同样的堆切得更碎**。本项目实测：

| taskHeap | slots | 每 slot | 结果 |
| --- | --- | --- | --- |
| 868MB | 10 | 86.8MB | 单流同步够用，6 表 join 直接 OOM |
| 868MB | 4 | 217MB | 能跑起来，但 134MB 的 join 状态占 62%，checkpoint 时 OOM |
| **1489MB** | **4** | **372MB** | 稳定 |

结论：**先定"最重的那个作业需要多少堆"，再倒推 slot 数**，不要反过来。

### 1.3 三种部署模式

| 模式 | 谁管作业 | 优点 | 缺点 |
| --- | --- | --- | --- |
| **session** | 客户端提交到长期运行的集群 | 起作业快、多作业共享 | **JM 挂了作业全没**，作业间互相影响 |
| per-job | 一个作业一个集群 | 隔离好 | 起得慢，资源浪费 |
| application | 集群内 `main()` | 生产推荐 | 需要打包 |

本项目用 session，因为要反复提交 4 个 SQL 作业、快速迭代。代价必须知道：
**容器重启后 `/jobs/overview` 是空数组** —— 作业定义只存在 JM 内存里，没落盘。

### 1.4 并行度

```
算子并行度 > 作业并行度（env.setParallelism） > 集群默认（parallelism.default）
```

本项目全部 **1**：

- 4 个作业 × 并行度 1 = 4 个 slot，正好占满
- 数据量小（Kafka 3 分区、CDC 41 万行），加并行度只会增加状态分片和文件句柄
- 并行度 > 分区数时，多出来的子任务**空转**（Kafka source 会报 idle）

### 1.5 Flink SQL Client

```bash
# 交互
./bin/sql-client.sh

# 批量执行一个文件（本项目的方式）
./bin/sql-client.sh -f /tmp/job.sql
```

文件里必须有：

```sql
SET 'execution.attached' = 'false';   -- 否则客户端挂在前台不退出
```

`execution.attached=false` 的含义是"提交后立即返回作业 ID"，而不是等作业结束
（流作业永远不结束）。脚本里少了这一行，`docker exec` 会一直阻塞。

### 1.6 REST API：判活与排障

```bash
curl -s localhost:8081/jobs/overview
curl -s localhost:8081/jobs/<jid>
curl -s localhost:8081/jobs/<jid>/checkpoints
curl -s -X PATCH "localhost:8081/jobs/<jid>?mode=cancel"
```

`/jobs/overview` 返回每个作业的 `jid` / `name` / `state` / `start-time`。

⚠️ **不要用算子指标判断作业是否在工作**：
`vertices[].metrics.read-records` 是缓存快照（可能长期为 0），
`/vertices/{vid}/metrics?get=0.numRecordsIn` 在 source 上恒为 0。
判活要看**目标系统的数据**（见 `12`）。

### 1.7 常见故障的因果链

| 表面现象 | 实际原因 | 看哪 |
| --- | --- | --- |
| 作业 `RESTARTING` + `NoResourceAvailableException` | TM 死了，JM 招不到资源 | `docker logs rt-flink-tm` |
| 集群 `slots-total` 归 0 | TM 进程退出 | `docker inspect ... RestartCount` |
| 作业 `RUNNING` 但无输出 | 源没数据 / join 一侧为空 / sink 失败被吞 | 目标表 `COUNT(*)` |
| checkpoint 一直 `IN_PROGRESS` | 状态太大 / 反压 / 外部系统卡住 | checkpoint 详情 + 背压面板 |

`NoResourceAvailableException` 是最误导的一条：它描述的是**结果**，不是原因。

---

## 二、在本项目中

### 2.1 集群拓扑

| 容器 | 角色 | 内存 | 端口 |
| --- | --- | --- | --- |
| rt-flink-jm | JobManager | 768m | 8081（UI/REST） |
| rt-flink-tm | TaskManager | limit 3328m / process 3072m | — |

```yaml
      taskmanager.numberOfTaskSlots: 4
      taskmanager.memory.process.size: 3072m
      state.backend: rocksdb              # 集群默认
      state.backend.incremental: true
    mem_limit: ${FLINK_TM_MEM_LIMIT:-3328m}
```

注意 `state.backend` 有两层：集群默认是 `rocksdb`（理论省内存、支持增量），
但 07 在 SQL 里 `SET 'state.backend' = 'hashmap'` 覆盖了它 —— 因为 RocksDB 在这台
Windows 机器上 checkpoint 会卡 5 分钟（见 2.5 坑 2）。**后设的赢，排查时先 grep SQL。**

### 2.2 为什么从 10 个 slot 改成 4 个

最初 10 个 slot 是为了 P3 的"10 张 ODS 表各一个作业"。到 P4 需要跑 6 表 join 时：

```
taskHeap 868MB ÷ 10 = 86.8MB/slot   → 跑不动
```

改 slot 数**不用加内存总量**，只是改"切蛋糕的方式"。这个判断在 P8 又被修正了一次：
4 slot / 217MB 仍然不够 07 的四表 join（状态 134MB），最终把 `process.size`
提到 3072m、每 slot 372MB。

完整的演进：

```
10 slots / 86.8MB   → ODS 单流够用，join 不够
 4 slots / 217MB    → 能跑，但 checkpoint 时 OOM（状态 134MB 占 62%）
 4 slots / 372MB    → 稳定
```

**"降 slot 比加内存对症"这句话只对了一半**：降 slot 是第一步（提高单 slot 份额），
当状态本身超过份额的 60% 时，还是得加总量。

### 2.3 内存参数怎么定

```yaml
taskmanager.memory.process.size: 3072m    # 容器内 JVM 总预算
mem_limit: 3328m                          # cgroup 硬上限，比 process.size 多 256m
```

`mem_limit` 要比 `process.size` 大：JVM 自身有堆外开销（线程栈、直接内存、JIT 代码缓存），
等于 cgroup 上限时会莫名其妙被 OOM Kill。

实测分配（`process.size = 3072m`）：

| 区块 | 大小 |
| --- | --- |
| taskHeap | 1489MB |
| managed memory | 512MB |
| network | 约 182MB |
| metaspace + overhead | 其余 |

### 2.4 提交作业的方式

```bash
bash scripts/submit-sql.sh flink/sql/07-dws-trade-overview-1m.sql
```

内部两步：

```bash
docker cp "$SQL_FILE" rt-flink-jm:/tmp/job.sql
docker exec rt-flink-jm ./bin/sql-client.sh -f /tmp/job.sql
```

用 `docker cp` 而不是挂载，是为了绕开 Git Bash 的路径改写（见 `05`）。

### 2.5 踩过的坑

**坑 1：TM OOM 的真正死因不在 Flink UI 里**

```
现场：4 个作业同时 RESTARTING，UI 报 NoResourceAvailableException，
      集群 slots-total 归 0，Doris 零行。
误判：以为是 JM 招不到资源 / 资源没释放。
真相：TM 被 cgroup OOM Kill 了（RestartCount 涨 1，4 个算子状态全丢，
      join 永久卡在 initializing）。
```

判据只有一条：

```bash
docker inspect rt-flink-tm --format 'RestartCount={{.RestartCount}} OOM={{.State.OOMKilled}}'
```

**坑 2：`state.backend=hashmap` 是环境决定的，不是偏好**

在 Docker Desktop on Windows 上，RocksDB 的 native I/O 极慢：CPU 只用了 2.3 秒，
墙钟卡 5 分钟在 `RocksDB.put`。checkpoint 永远做不完 → 作业反复超时。
换 `hashmap`（纯堆内）后 checkpoint 秒级完成。

代价：状态全在堆里，**受 taskHeap 限制**，所以坑 1 才那么致命。

**坑 3：容器重启 = 作业全丢，但看起来像"作业崩了"**

session 模式下 JM 不持久化作业图。判据：

```bash
# 6 个容器 StartedAt 全相同 + RestartCount=0 → 是新起的集群，不是崩溃
for c in rt-mysql rt-kafka rt-doris-fe rt-doris-be rt-flink-jm rt-flink-tm; do
  docker inspect $c --format '{{.Name}} {{.State.StartedAt}} {{.RestartCount}}'
done
```

---

## 三、速查

```bash
# 作业列表
curl -s --noproxy '*' localhost:8081/jobs/overview | \
  python -c "import sys,json;[print(j['jid'][:8], j['state'], j['name'][:50]) for j in json.load(sys.stdin)['jobs']]"

# 取消
curl -s -X PATCH "localhost:8081/jobs/<jid>?mode=cancel"

# TM 健康（唯一可信）
docker inspect rt-flink-tm --format 'RestartCount={{.RestartCount}} OOM={{.State.OOMKilled}}'

# slot 使用
curl -s localhost:8081/taskmanagers | python -m json.tool | grep -E "slotsNumber|freeSlots"

# 提交
bash scripts/submit-sql.sh flink/sql/07-dws-trade-overview-1m.sql
```

| 现象 | 判据 |
| --- | --- |
| `slots-total=0` | TM 死了，看 `docker logs rt-flink-tm` |
| 作业 `RESTARTING` 且报 `NoResourceAvailableException` | 同上，这是结果不是原因 |
| `/jobs/overview` 返空数组 | session 集群重启了，重提作业 |
| 提交后 `docker exec` 一直不返回 | SQL 文件缺 `SET 'execution.attached'='false';` |
| 作业 `RUNNING` 但无输出 | 别查 Flink 指标，查目标表 |

延伸：`10-Flink-SQL.md`、`11-Flink-CDC.md`、`12-Flink-状态管理与-Checkpoint.md`
