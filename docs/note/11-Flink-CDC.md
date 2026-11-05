# 11 · Flink CDC

> 一句话：Flink CDC 把"全量导数"和"增量订阅"合成一个 source。它最难的地方不是配置，
> 而是**快照与增量的交界**，以及一组"写错不报错"的参数。

---

## 一、通用知识

### 1.1 它解决什么问题

传统做法是两套：

```
① 用 sqoop/DataX 全量导一遍（离线，几小时）
② 用 Canal/Debezium 增量订阅（实时，但从某个位点开始）
→ 交界处的变更要么重复、要么丢失，还得自己合并
```

Flink CDC 的 mysql-cdc source 一个 DDL 同时做两件事：先按 chunk 读全量（**增量快照**），
再无缝接上 binlog。状态存在 checkpoint 里，挂了能从断点续。

### 1.2 一份典型配置

```sql
CREATE TABLE src_orders (
  order_id STRING,
  ...
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector'       = 'mysql-cdc',
  'hostname'        = 'mysql',
  'port'            = '3306',
  'username'        = 'root',
  'password'        = 'root123',
  'database-name'   = 'olist',
  'table-name'      = 'orders',
  'server-id'       = '5401-5404',
  'server-time-zone'= 'Asia/Shanghai',
  'scan.incremental.snapshot.chunk.size' = '8096'
);
```

### 1.3 五个"写错就静默失效"的参数

| 参数 | 写错的后果 | 是否报错 |
| --- | --- | --- |
| `server-id` | 与别的作业冲突 → 互相踢，**交替重启** | ❌ 日志里只有反复 connect |
| `server-time-zone` | 缺省按 UTC 解析 `DATETIME` → 下游整体偏 8 小时 | ❌ |
| `scan.startup.mode` | 用错会让作业"跑着但读不到历史" | ❌ |
| `scan.incremental.snapshot.enabled` | 关掉后 `server-id` 范围写法失效 → `NumberFormatException` | ✅ 但报错位置错位 |
| `table-name` / `database-name` | 表名大小写、库名拼错 → 起来后一行没有 | ❌ |

所以本项目的做法是：**先写一张最小的表跑通，再批量生成另外 8 张**。
一次写 9 张，任何一处错都要改 9 遍。

### 1.4 并行度与 `server-id` 范围

`server-id = '5401-5404'` 的意思是"这个 source 最多用 4 个 server-id"。
它必须 **≥ 并行度**，否则快照阶段的多个 reader 抢不到 id。

所以范围写法与增量快照是绑定的：

```
scan.incremental.snapshot.enabled = true  → server-id 可以是 '5401-5404'
scan.incremental.snapshot.enabled = false → server-id 必须是单个整数
                                            （'5401-5404' 会在 parseInt 抛异常）
```

而且这个异常会被 Flink 的 parser 二次格式化，**报错位置可能指向别的语句** ——
排查时不要只看报错行号。

### 1.5 `scan.startup.mode` 的四种值

| 值 | 行为 | 适用 |
| --- | --- | --- |
| `initial`（默认） | 先全量快照，再增量 | ✅ 大多数情况 |
| `latest-offset` | 只从当前位点读增量 | ⚠️ 只在"目标表已有全量数据"且**作业不做 join** 时安全 |
| `earliest-offset` | 从 binlog 最早位点读 | 需要重放全部变更 |
| `specific-offset` / `timestamp` | 指定位点/时间 | 修数据 |

⚠️ 关键认知：**`latest-offset` 不是"从上次停的地方继续"**，是"从此刻开始，历史一条不读"。
从 savepoint/checkpoint 恢复时才叫"继续"。

### 1.6 快照的成本要按"这个作业读几张表"算

很多人算错：把整个库的行数当成"重放成本"。
实际成本 = 该作业 `FROM` 里出现的表行数之和（join 的每一侧都算）。

### 1.7 元数据列

```sql
`row_kind` STRING METADATA FROM 'row_kind' VIRTUAL,
`op_ts`    TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
`table_name`  STRING METADATA FROM 'table_name' VIRTUAL,
`database_name` STRING METADATA FROM 'database_name' VIRTUAL
```

`row_kind` 是 Flink 的 `RowKind` 字符串：`+I` 插入、`-U` 更新前、`+U` 更新后、`-D` 删除。

⚠️ **落地前先抽样确认**：不同 connector 版本 / 不同启动模式下，快照行的表现可能不同
（有的表现为 `+I`，有的被标成 `c` 之类）。用一句 `SELECT row_kind, COUNT(*) ... GROUP BY row_kind`
在目标表上对一遍，比记文档可靠。

`op_ts` 是 **binlog 事件时间**，只有秒精度 —— 所以由它派生的 `ts_ms` 后三位恒为 `000`。

---

## 二、在本项目中

### 2.1 谁在用 CDC

| 作业 | 是否常驻 | 源 |
| --- | --- | --- |
| `01/02/03` ODS 九表同步 | 按需（数据已落好，不常驻） | 各 1~2 张表 |
| **`07` 分钟概览** | ✅ 常驻 | 4 张（orders / order_items / customers / order_payments） |
| `05` DWD 打宽 | 按需 | 6 张 |
| `08` 日粒度 DWS | 按需 | |
| `04/06/09` | ✅ 常驻 | Kafka（不走 CDC） |

所以"**07 是唯一常驻的 mysql-cdc 直读作业**"。这个事实决定了它的重启成本必须算清
（见 2.4）。

### 2.2 server-id 分段

```
ODS 九表    5401 ~ 5484
P4 DWD      5531 ~ 5558
07 分钟表   5561 ~ 5576
```

为什么必须分段：MySQL 把每个 CDC 客户端当 slave，`server-id` 相同会**互相踢掉**。
症状是两个作业交替重启，日志里没有任何报错，只有反复 connect —— 极难联想到 server-id。

### 2.3 先写一张表跑通

`01-ods-orders.sql` 是刻意的单表示范，注释里写了理由：

```
CDC 作业里有 5 个参数是「写错就静默失效」的类型
（server-id 冲突、时区、op 元数据列名、Doris sink 的 format、dt 推导）。
一次写 9 个表，任何一处错都要改 9 遍。
```

### 2.4 07 的成本重估与 `initial` 的取舍

```
错误说法：重启要重放 155 万行
实际：    orders 9.9 万 + order_items 11.3 万 + customers 9.6 万
          + order_payments 10.4 万 ≈ 41 万行，实测快照 1~2 分钟
```

用这 1~2 分钟换"重启后自愈"，比换"秒起但永久僵尸"划算得多（见 2.5）。

代价要说清：每次重启都要重建聚合状态；重启后必须**等快照跑完再验收**，
别看到 `RUNNING` 就以为好了。判据是健康度的 `overview_join_rows`（应 ≥ 40 万）。

### 2.5 僵尸作业：最值得记住的一次排查

```
配置：四表 join 再聚合，scan.startup.mode = 'latest-offset'
动机：重启不必重放历史、状态不必重建，秒起
现场：作业 RUNNING、checkpoint 正常、Kafka lag 0、健康度 10 项全 ok
      目标表 8.8 万行静静躺着，KPI 照旧显示
      改一行 MySQL → 90 秒无变化
真相：join 两侧状态只来自数据流。latest-offset 不读快照 → orders / customers
      侧没有行 → 每条 order_items 变更都判"订单不存在" → join 直接丢弃
判据：src_order_items numRecordsOut = 2
      Join[6]          numRecordsIn  = 2, numRecordsOut = 0
      sink             numRecordsIn  = 0
```

**为什么面板全绿**：`UNIQUE` 表"不写就不会丢"，所以目标表的旧数据完好；
`pipeline_freshness_s` 又取三张表的**最近**更新时间，ADS 每 60 秒批刷会刷新它 ——
交易链停多久都显示 0 秒。

修法：改回 `initial`。新增判据：健康度第 11 项 `overview_join_rows`
（07 四个源累计读取行数，≥ 40 万才是真读了快照）。

这条的通用形式：**"作业 RUNNING" 只说明进程活着，不说明数据在流动。
证明数据在流动的唯一方式是看目标系统的变化。**

### 2.6 踩过的坑

**坑 1：关掉增量快照后 `server-id` 范围写法炸**

```
'5573-5576' → NumberFormatException: For input string: "5573-5576"
且报错位置显示在别的语句上（parser 格式化位置时又抛一次）
```

**坑 2：快照期 `op_ts` 是 epoch 0**

`DATE(op_ts)` 会让 155 万行全落 1970 分区（动态分区还会立刻回收 → 导入"成功"但 0 行）。
`dt` 必须取业务时间。

**坑 3：MySQL JDBC 驱动不在 connector 包里**

GPLv2 的原因，`flink-sql-connector-mysql-cdc` 不含 `mysql-connector-j`。
缺它的表现：作业能提交，TM 启动即 `ClassNotFoundException`，**报错不会提"缺驱动"**。
所以要单独放 `flink/lib/mysql-connector-java-8.0.27.jar`。

---

## 三、速查

```sql
-- 单表最小模板
CREATE TABLE src_x (
  id STRING, ..., PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='mysql','port'='3306',
  'username'='root','password'='root123',
  'database-name'='olist','table-name'='x',
  'server-id'='54xx-54xx',
  'server-time-zone'='Asia/Shanghai'
);

-- 看 row_kind 的真实分布（落地前必做）
SELECT row_kind, COUNT(*) FROM ods_xxx GROUP BY row_kind;
```

```bash
# 哪些作业在读 binlog
docker exec rt-mysql mysql -uroot -proot123 -e "SHOW PROCESSLIST" | grep -i "Binlog Dump"

# 07 是否读满四源（僵尸判据）
bash scripts/collect-pipeline-health.sh 2>&1 | grep overview_join_rows

# 确认 server-id 没撞
grep -rn "server-id" flink/sql/*.sql
```

| 现象 | 判据 |
| --- | --- |
| 两个 CDC 作业交替重启 | `server-id` 冲突 |
| 作业 RUNNING 但目标表不写 | 先算 `overview_join_rows`，再查是不是 `latest-offset` |
| 时间整体偏 8 小时 | 缺 `server-time-zone` |
| 全落 1970 分区 | `dt` 取了 `DATE(op_ts)` |
| `ClassNotFoundException: com.mysql.cj.jdbc.Driver` | 缺 mysql-connector-j |
| `NumberFormatException: "5573-5576"` | 关了增量快照 |

延伸：`07-MySQL-Binlog-与-CDC-原理.md`、`10-Flink-SQL.md`、
`12-Flink-状态管理与-Checkpoint.md`、`25-可观测性与链路健康度.md`
