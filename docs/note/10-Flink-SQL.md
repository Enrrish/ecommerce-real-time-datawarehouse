# 10 · Flink SQL

> 一句话：Flink SQL 把"流"抽象成"一张永远在变的表"。你写的是 `SELECT`，它跑的是
> 有状态的增量计算 —— 所以**能写出来**和**能跑得动**是两件事。

---

## 一、通用知识

### 1.1 一张 SQL 文件的骨架

```sql
SET 'execution.attached' = 'false';        -- 提交后返回（脚本必需）
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'parallelism.default' = '1';
SET 'state.backend' = 'hashmap';

CREATE TABLE src (...)
WITH ('connector' = 'mysql-cdc', ...);     -- 源

CREATE TABLE sink (...)
WITH ('connector' = 'doris', ...);         -- 汇

INSERT INTO sink SELECT ... FROM src;      -- 作业图
```

三类表：

| 类型 | 是否落存储 | 用途 |
| --- | --- | --- |
| source / sink 表 | 有连接器 | 出入口 |
| `CREATE VIEW` | 否，只是查询别名 | 拆解复杂逻辑，**不产生物理算子** |
| `CREATE TEMPORARY VIEW` | 否，会话级 | 同上，但会话结束即消失 |

### 1.2 时间属性两套，别混

| | `TIMESTAMP` / `TIMESTAMP_LTZ` | 处理时间 `PROCTIME()` |
| --- | --- | --- |
| 含义 | 事件自带的业务时间 | 算子处理这条记录时的机器时间 |
| 需要 watermark | 是（事件时间） | 否 |
| 用途 | 窗口、乱序处理 | 去重、维表 lookup、限流 |

```sql
-- 事件时间：声明 watermark
event_time TIMESTAMP(3),
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND

-- 处理时间：计算列，不需要 watermark
proc_time AS PROCTIME()
```

**混用的后果**：拿事件时间做 `ROW_NUMBER()` 排序，在流模式下 Flink 会要求
`ORDER BY` 落在时间属性上，并对每个 key 维护"未来是否还有更早记录"的状态 ——
水位线不推进，尾部记录永远不出。

### 1.3 聚合的三种写法与代价

**① 普通 GROUP BY（更新流）**

```sql
SELECT dt, category, SUM(gmv) FROM t GROUP BY dt, category;
```
输出是 changelog（`+I/-U/+U`），下游要能接更新。状态量 = 分组数。

**② 窗口聚合**

```sql
SELECT window_start, window_end, SUM(gmv)
FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;
```
窗口结束才输出（或按 `CUMULATE` 提前输出），状态随窗口关闭而清理。

**③ OVER 聚合 / 去重**

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY proc_time) AS rn
  FROM t
) WHERE rn = 1;
```
`ROW_NUMBER` 是流里唯一的"按 key 取一条"手段。**排序键必须用 `PROCTIME()`**，
用事件时间会因为水位线不推进而卡住（见 1.2）。

### 1.4 维表 join：`LOOKUP JOIN`

```sql
SELECT o.order_id, g.state
FROM orders AS o
LEFT JOIN dim_geo FOR SYSTEM_TIME AS OF o.proc_time AS g
  ON o.zip = g.zip;
```

注意三点：

- `FOR SYSTEM_TIME AS OF <时间列>` 必须写，否则 Flink 不知道怎么定位"当时的维表版本"
- 右侧必须是**带主键的 lookup 表**（JDBC / HBase / Redis 等）
- 默认是同步查询、无缓存 → 每条记录一次 RPC。生产上要配 `lookup.cache`

### 1.5 `STATEMENT SET`：一次提交多个 INSERT

```sql
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO a SELECT ...;
  INSERT INTO b SELECT ...;
END;
```

设计意图是**共享子计划**（相同的 join 只算一次）。但它**不保证**共享 ——
如果几个 `INSERT` 的投影不同，Flink 会认为它们的输出行类型不同，认不出是同一棵树。

### 1.6 类型与时区

- `TIMESTAMP` = 无时区字面值；`TIMESTAMP_LTZ` = 带时区（内部存 epoch）
- `table.local-time-zone` 影响 `CURRENT_TIMESTAMP`、`PROCTIME()` 的显示
- 字符串里比较时间要格式一致：`LIKE '____-__-__ __:__:__.___'`

### 1.7 几个"报了错也不知道在说什么"的语法差异

| 想写 | Flink 1.18 的现实 |
| --- | --- |
| `a RLIKE 'x'` | ❌ 不支持中缀，用 `a LIKE '%x%'` 或 `REGEXP(a,'x')` |
| `HOUR(ts)` 返回 INT | ❌ 返回 **BIGINT**，参与 `||` 拼接或 `CASE` 时要 CAST |
| `INSERT INTO TABLE t` | ❌ 只有 `INSERT OVERWRITE TABLE` 能带 `TABLE` |
| `ORDER BY` 任意列 + `ROW_NUMBER` | ⚠️ 流模式要求时间属性 |

---

## 二、在本项目中

### 2.1 九份 SQL，各有分工

| 文件 | 行数 | 干什么 |
| --- | --- | --- |
| `01-ods-orders.sql` | 146 | 单表 CDC 示范（先跑通再批量生成） |
| `02-ods-dimensions.sql` | 348 | 5 张维表 CDC（含 100 万行 geolocation） |
| `03-ods-facts.sql` | 229 | 3 张事实表 CDC |
| `04-ods-user-event.sql` | 168 | Kafka → ODS 埋点 |
| `05-dwd-trade-order-detail.sql` | 419 | 六表 join 打宽，112,650 行 |
| `06-dwd-user-event-clean.sql` | 210 | 埋点清洗 + 校验标记 |
| `07-dws-trade-overview-1m.sql` | 278 | 四表 join → 分钟聚合 |
| `08-dws-trade-daily.sql` | 364 | 类目/卖家/地域三个日粒度 |
| `09-dws-event-payment.sql` | 280 | 漏斗 + 支付方式 |

合计 **5,609 行**（含注释和建表 DDL，纯逻辑约三分之一）。

### 2.2 一表一作业：`STATEMENT SET` 的实测反例

最初把 4 张 trade DWS 表放进一个 `STATEMENT SET`，想让它们共享 join：

```
结果：作业 35 个算子、22 个 Join —— 每条链 5.5 个，正好 4 份完整 join
原因：4 个 INSERT 的 SELECT 列表不同（各表维度列不一样），
      投影裁剪给 4 份 join 生成了不同的输出行类型，
      SubplanReuseExecutor 认不出它们是同一棵树
后果：状态量 ×4，单链 105MB → 四链 420MB，而 4 个 slot 下每 slot 只有 217MB
      → TM OOM，slots-total 归 0
```

改成"一表一作业"后，单作业 50~90MB，在 372MB 里有安全余量。
顺带的收益：每个作业只读自己需要的源，省 binlog 段和资源。

**这条经验的通用形式**：`STATEMENT SET` 的复用条件是"子计划完全等价"，
任何投影 / 过滤 / 列序差异都会让它退化成 N 份。想复用就抽 `VIEW` 并且**保证
各个 INSERT 的 SELECT 列表一致**，否则不如拆作业。

### 2.3 07 为什么不用窗口

分钟概览需要"每个 dt + minute 一行，值随新数据更新"，这是**更新流**语义，
不是"窗口结束时输出一次"。

```sql
-- 用的是 GROUP BY（更新流），不是 TUMBLE
INSERT INTO dws_trade_overview_1m
SELECT DATE(order_purchase_timestamp) AS dt,
       DATE_FORMAT(order_purchase_timestamp, 'HH:mm') AS minute,
       SUM(item_total), COUNT(DISTINCT order_id), ...
FROM ... GROUP BY DATE(...), DATE_FORMAT(...);
```

代价：状态量 = 分组数（8.8 万），比窗口大。收益：大屏今天的那一分钟能实时跳。

### 2.4 用 `PROCTIME()` 去重，不用事件时间

`09` 从 Kafka 直连算漏斗，同一 `event_id` 可能重复（生产端重试、作业重放）。
DWD 靠 UNIQUE 表去重，DWS 没有这层保护，所以必须显式去重：

```sql
SELECT ... FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY PROCTIME()) AS rn
  FROM src_user_event
) WHERE rn = 1
```

为什么是 `PROCTIME()` 不是 `event_time`：流模式下 OVER 聚合强制 `ORDER BY` 落在
时间属性上，用事件时间会绑定 watermark；而上游是 `earliest-offset` 重放，
水位线永远落后 → 尾部记录卡在状态里不出（实测 22,064 条只落 21,996）。

### 2.5 三个语法坑的真实位置

**① `HOUR()` 返回 BIGINT**

```sql
-- 错：拼接时类型不匹配
CONCAT(dt, ' ', HOUR(event_time), ':00')
-- 对：
CONCAT(dt, ' ', CAST(HOUR(event_time) AS INT), ':00')
```

**② 没有 `RLIKE` 中缀**

```sql
-- 错：event_time RLIKE '^[0-9]{4}-'
-- 对：用 LIKE 的下划线通配
event_time LIKE '____-__-__ __:__:__.___'
```

**③ `INSERT INTO TABLE x` 是语法错误**

```sql
INSERT INTO TABLE foo SELECT ...        -- ❌ 报错自相矛盾
INSERT INTO foo SELECT ...              -- ✅
INSERT OVERWRITE TABLE foo SELECT ...   -- ✅（只有 OVERWRITE 能跟 TABLE）
```

### 2.6 `dt` 的推导：每个作业都要想清楚

| 层 | `dt` 取什么 | 为什么 |
| --- | --- | --- |
| ODS（CDC） | `DATE(业务时间)`，**不是** `DATE(op_ts)` | 快照期 `op_ts` 是 epoch 0 → 全落 1970 |
| ODS（埋点） | `DATE(event_time)` | 事件本身就是业务时间 |
| DWD | `DATE(order_purchase_timestamp)` | 不可变，分区才安全 |
| DWS / ADS | 同源事实表 | 与下游对齐 |

---

## 三、速查

```sql
-- 时间属性声明
event_time TIMESTAMP(3),
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
proc_time AS PROCTIME()

-- 去重
ROW_NUMBER() OVER (PARTITION BY id ORDER BY PROCTIME()) = 1

-- 维表 join
LEFT JOIN dim FOR SYSTEM_TIME AS OF t.proc_time AS d ON t.k = d.k

-- 安全除法（Doris 侧同理）
CAST(a AS DECIMAL(20,6)) / NULLIF(b, 0)
```

| 症状 | 原因 |
| --- | --- |
| 提交后不返回 | 缺 `SET 'execution.attached'='false';` |
| 尾部几十条数据不出 | OVER 聚合用了事件时间，水位线不推进 |
| 作业 22 个 Join、状态爆炸 | `STATEMENT SET` 没共享子计划，拆作业 |
| `Could not find any factory` | 连接器 jar 不在 `flink/lib`，或挂载源不存在变成了目录 |
| `ClassNotFoundException: com.mysql.cj.jdbc.Driver` | 缺 mysql-connector-j（不在 connector 包里） |
| 时间偏 8 小时 | `table.local-time-zone` + 源端 `server-time-zone` 都要设 |

延伸：`09-Apache-Flink.md`、`11-Flink-CDC.md`、`13-时间语义与-Watermark.md`、
`12-Flink-状态管理与-Checkpoint.md`
