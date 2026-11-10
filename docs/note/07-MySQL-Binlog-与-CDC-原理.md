# 07 · MySQL Binlog 与 CDC 原理

> 一句话：binlog 是 MySQL 的**逻辑变更日志**，CDC 就是"伪装成一个从库，把这条日志
> 读出来翻译成变更流"。理解这一点，`server-id`、`row_image`、快照这些概念全都顺了。

---

## 一、通用知识

### 1.1 binlog 是什么

MySQL 有两份日志容易混：

| | redo log | binlog |
| --- | --- | --- |
| 层 | InnoDB 引擎 | **server 层**（所有引擎共用） |
| 内容 | 物理页变更 | **逻辑变更**（SQL 或行前后值） |
| 用途 | 崩溃恢复 | 主从复制、**CDC**、按时间点恢复 |
| 生命周期 | 循环写，覆盖 | 追加写，按 `expire_logs_days` 清理 |

CDC 只关心 binlog。它由一连串 **event** 组成，每个 event 有：

- 事件头：`timestamp`（**秒级**）、`event_type`、`server_id`、`log_pos`、`flags`
- 事件体：`TABLE_MAP`（表结构映射）、`WRITE_ROWS` / `UPDATE_ROWS` / `DELETE_ROWS`、`QUERY`（DDL）等

### 1.2 三种格式，CDC 只能选 ROW

| 格式 | 记什么 | 能否做 CDC |
| --- | --- | --- |
| `STATEMENT` | 原始 SQL 文本 | ❌ 拿不到行前后值，`NOW()`/`UUID()` 无法重放 |
| `ROW` | **每一行**的变更前后值 | ✅ |
| `MIXED` | 一般用 ROW，少数用 STATEMENT | ⚠️ 不可靠 |

```ini
binlog_format = ROW
binlog_row_image = FULL        # FULL / MINIMAL / NOBLOB
```

`row_image` 决定记多少列：

| 值 | UPDATE 事件里记什么 | 后果 |
| --- | --- | --- |
| `FULL` | 变更前后**所有列** | ✅ DELETE 能拿到完整旧行，下游可做全字段覆盖 |
| `MINIMAL` | 只记主键 + 被改的列 | ❌ 下游拿不到未改列，upsert 会把其它字段写成 NULL |
| `NOBLOB` | 同 MINIMAL，大字段除外 | ❌ 同上 |

**`FULL` 是 CDC 的硬要求**，不是优化项。

### 1.3 `server-id`：CDC 客户端是一个"从库"

MySQL 的复制拓扑里每个实例必须有一个唯一 `server-id`。CDC 工具伪装成从库，
所以也要一个。**两个 CDC 客户端用同一个 `server-id` 会互相踢掉对方**：

现象非常误导 —— 两个作业交替 `RESTARTING`，日志里**没有报错**，只有反复
`connect` / `disconnect`。因为 MySQL 认为"同一个从库重连了"，把旧连接断掉。

所以多作业要**分段分配**：

```sql
-- 约定：每段给一个作业，段内可以有多个并行读
server-id = '5401-5404'    -- 4 个并行快照读
server-id = '5531-5534'
```

⚠️ **范围写法只在"增量快照"模式下有效**。关掉 `scan.incremental.snapshot.enabled`
走 Debezium 老模式后，`server-id` 只接受单个整数，写 `'5573-5576'` 会在
`parseInt` 抛 `NumberFormatException` —— 而且这个异常会被 Flink 的 parser 二次格式化，
**报错位置可能显示在别的语句上**，极具误导性。

### 1.4 全量 + 增量：为什么需要"一致性快照"

只读 binlog 不够 —— 你从"现在"开始读，历史数据就没了。所以要先把存量读一遍
（快照），再接着读增量。难点是**两者交界处不能重也不能漏**。

三代做法：

| 做法 | 原理 | 代价 |
| --- | --- | --- |
| 全局读锁 `FLUSH TABLES WITH READ LOCK` | 锁住写入，记下位点，再解锁全量读 | 锁期间业务**只读**，大表不可接受 |
| 事务 + `SELECT ... FOR UPDATE` | 单事务 RR 隔离读全量 | 长事务，undo 膨胀 |
| **增量快照（DBLog 算法）** | 分 chunk 读，读 chunk 前后各记一次高位点；用"低水位"过滤 binlog 中已包含在 chunk 里的变更 | 无锁，可断点续传，**Flink CDC / Debezium 默认** |

增量快照的核心：每个 chunk 读到的是 `[start, end]` 之间的行，期间发生的变更会在
binlog 里再被读到一次 —— 靠"低水位线"去重。所以下游**必须是幂等的**（UNIQUE 表）。

### 1.5 位点与 GTID

- 传统位点：`(binlog_file, position)`，如 `mysql-bin.000003:154`
- GTID：`3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5`，全局唯一，主从切换后仍有效

CDC 工具内部都会记位点，但**作业被 cancel 后重新提交（不是 restore）时位点会丢**，
只能按 `scan.startup.mode` 重来（见 [11](11-Flink-CDC.md)）。

### 1.6 时区：最容易错的一处

- `TIMESTAMP` 列在 binlog 里是 **UTC epoch**
- `DATETIME` 列在 binlog 里是**字面值**，不带时区

CDC 客户端要用一个 `server-time-zone` 告诉它"源库的会话时区是什么"。
不指定则按 UTC 解析 `DATETIME` → 抽到下游整体偏 8 小时。而且**源端查是对的**，
只有下游错，所以第一反应往往是"数仓算错了"。

### 1.7 binlog 的边界

| 限制 | 后果 |
| --- | --- |
| 事件头时间戳只有**秒** | 下游 `ts_ms` 后三位恒为 `000`，不是 bug |
| 大事务（一次改 100 万行） | 单个 event 巨大，可能超过 `max_allowed_packet` |
| DDL 会以 `QUERY` 事件出现 | 多数 CDC 不解析 DDL，表结构变更要人工同步 |
| binlog 有保留期 | 作业停太久，位点被清理 → 只能重新全量 |

---

## 二、在本项目中

### 2.1 两条链路，只有一条走 binlog

```
9 张业务表  MySQL binlog(ROW/FULL) ──Flink CDC──▶ Doris ODS   ← 变更流，有"状态"概念
埋点事件    Kafka(user_event)      ──Flink────▶ Doris ODS   ← 事件流，无"状态"概念
```

为什么不能都用 binlog：binlog 描述的是**最终状态**（一行变成什么样），
它知道"购物车数量从 1 变成 2"，但不知道中间点了几次。
漏斗（浏览 → 加购 → 下单 → 支付）只能从行为流算。

### 2.2 server-id 分段表

| 作业 | server-id 段 | 源表数 |
| --- | --- | --- |
| ODS 9 个同步作业 | `5401` ~ `5484` | 每作业 1~2 张 |
| P4 DWD 作业 | `5531` ~ `5558` | |
| 07 分钟概览 | `5561` ~ `5576` | 4 张（orders / order_items / customers / order_payments） |

规划原则：**按作业段分配，段内按并行度细分**。共用段 = 互相踢。

### 2.3 元数据列：这个版本没有 `op`

Flink CDC 3.2.1 的 mysql-cdc 只暴露四个元数据键（报错时它会自己列出来）：

```
table_name / database_name / op_ts / row_kind
```

- `op_ts`：binlog 事件时间。**只有秒精度**，所以 `ts_ms` 后三位恒 `000`
- `row_kind`：Flink 的 `RowKind` 字符串，**不是** MySQL 的 `INSERT/UPDATE/DELETE`

```sql
`row_kind` STRING METADATA FROM 'row_kind' VIRTUAL,
`op_ts`    TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
```

`row_kind` 的取值：`+I`（插入/快照行）、`-U`（更新前）、`+U`（更新后）、`-D`（删除）。
所以一条 UPDATE 会产生**两条**记录 —— 这就是为什么 TM 里看到
`numRecordsOut = 2` 是正常的。

### 2.4 `dt` 不能取 `DATE(op_ts)`

这是本项目第一个"不报错但全错"的坑：

```
快照阶段 op_ts = epoch 0（1970-01-01 00:00:00）
  → DATE(op_ts) = 1970-01-01
  → 155 万行全部落进 1970 分区
```

而且分区是 `start=-30, end=7` 动态分区的话，1970 分区会被立刻回收 ——
现象是"导入报成功、COUNT(*) 是 0"。

正解：**`dt` 取业务时间字段**（如 `DATE(order_purchase_timestamp)`），
不取 binlog 元数据时间。

### 2.5 一个必须显式设的参数

```sql
'server-time-zone' = 'Asia/Shanghai'
```

不设的话 Debezium 按 UTC 解析 `DATETIME`，抽到 Doris 后所有时间偏 8 小时。
本项目在 `01-ods-orders.sql` 里把这段理由写在了参数旁边，因为它看起来"可有可无"。

### 2.6 快照成本是可控的（要算清）

07 作业是唯一保留的 mysql-cdc 直读作业，`scan.startup.mode = 'initial'`。
重启要重放，成本经常被高估：

| 说法 | 实际 |
| --- | --- |
| "重放 155 万行" | ❌ 那是整个 ODS 层 |
| 07 只读 4 张表 | ✅ orders 9.9 万 + order_items 11.3 万 + customers 9.6 万 + payments 10.4 万 ≈ **41 万行** |

实测 1~2 分钟。**这 1~2 分钟换"重启后自愈"，比换"秒起但永久僵尸"划算**（见 2.7）。

### 2.7 踩过的坑：带 join 的 CDC 不能用 `latest-offset`

```
动机：重启不必重放历史、状态不必重建，秒起。
错在哪：本作业是 join 之后再聚合。join 两侧状态**只来自数据流**，
        没有快照就没有 orders 侧的行 → 每条 order_items 变更都判"订单不存在"丢掉。
现场：作业 RUNNING、checkpoint 正常、lag 0、健康度 10 项全 ok，但一行不写。
判据：源 numRecordsOut = 2，Join numRecordsOut = 0，sink numRecordsIn = 0。
```

`UNIQUE` 表"不写就不丢"这句话反过来也成立：它保证了数据不丢，同时保证了
**你永远发现不了它已经不写了**。详见 [11](11-Flink-CDC.md)、`12`。

---

## 三、速查

```sql
-- 源库侧：确认配置
SHOW VARIABLES WHERE Variable_name IN
  ('binlog_format','binlog_row_image','server_id','log_bin','binlog_expire_logs_seconds');
SHOW MASTER STATUS\G          -- 当前位点
SHOW BINARY LOGS;             -- 有哪些文件、多大
SHOW BINLOG EVENTS IN 'mysql-bin.000003' LIMIT 20;   -- 肉眼看事件
```

```bash
# 看正在跑的 Flink CDC 作业用了哪些 server-id
grep -rn "server-id" flink/sql/*.sql

# 确认某个作业是否在读 binlog
docker exec rt-mysql mysql -uroot -proot123 -e "SHOW PROCESSLIST" | grep -i "Binlog Dump"
```

| 现象 | 原因 |
| --- | --- |
| 两个 CDC 作业交替重启、日志无报错 | `server-id` 撞了 |
| `NumberFormatException: "5573-5576"` | 关了增量快照，范围写法不支持 |
| 所有时间偏 8 小时 | 缺 `server-time-zone` |
| 全落 1970 分区 | `dt` 取了 `DATE(op_ts)` |
| `ts_ms` 后三位总是 000 | binlog 事件头只有秒精度 |
| 时间下游对、上游错 | 少见，通常是 `TIMESTAMP` + 会话时区 |

延伸：`06-MySQL.md`、`11-Flink-CDC.md`、`08-Apache-Kafka.md`（另一条链路）
