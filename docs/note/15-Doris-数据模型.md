# 15 · Doris 数据模型

> 一句话：Doris 的三张表模型（DUPLICATE / AGGREGATE / UNIQUE）本质是在回答
> **"同一个主键出现多次时怎么办"**。选错模型不会报错，只会让数字慢慢偏。

---

## 一、通用知识

### 1.1 三种模型

| 模型 | 同 key 多行 | 适用 | 风险 |
| --- | --- | --- | --- |
| **DUPLICATE** | 全部保留 | 原始日志、明细 | 重跑一次作业行数翻倍 |
| **AGGREGATE** | 按 value 列的聚合函数合并 | 预聚合指标 | 只能聚合，不能"改成一个新值" |
| **UNIQUE** | 只保留最新一行 | 需要幂等的表 | 无（代价是写入稍重） |

示例：

```sql
-- DUPLICATE：三行都在
DUPLICATE KEY(id)  → (1,'a'), (1,'b'), (1,'a')

-- AGGREGATE：value 列按 SUM/MAX/REPLACE 合并
AGGREGATE KEY(id)  → id=1, cnt=SUM(1,1), name=REPLACE('a','b')

-- UNIQUE：最后写入的那行赢
UNIQUE KEY(id)     → (1,'b')   -- 取决于写入顺序
```

### 1.2 UNIQUE 的两种实现

| | Merge-on-Read（MoR） | Merge-on-Write（MoW） |
| --- | --- | --- |
| 写入 | 直接追加，读时合并 | 写入时标记旧行删除 |
| 读性能 | 差（要合并多版本） | 好（只读最新） |
| 点查 | 不支持 |
| 版本 | 老默认 | **1.2 起默认** |

```sql
PROPERTIES ("enable_unique_key_merge_on_write" = "true")
```

MoW 的代价：每次写入要维护删除标记，写放大；收益：读快、支持 `point query`（按主键查一行）。

### 1.3 Key 列的两条硬约束

**① Key 必须是 schema 的"有序前缀"**

```sql
-- ✅ 合法：key 是前两列
UNIQUE KEY(`order_id`, `dt`), c1, c2, ...
-- ❌ 非法：key 跳跃
UNIQUE KEY(`order_id`, `c2`), c1, c2, ...
```

所以定义表时要先想清"主键是什么"，再排字段顺序。

**② 分区列必须在 Key 里**

```sql
UNIQUE KEY(`event_id`, `dt`)
PARTITION BY RANGE(`dt`)         -- dt 必须在 key 里
```

这条会导致一个反直觉的结果：`UNIQUE KEY(event_id, dt)` 要求 `dt` 排在**第 2 列**
（如果 event_id 是第 1 列），因为分区列必须在 key 中，而 key 又必须是有序前缀。

### 1.4 唯一性只在 tablet 内保证

```
table → partition → tablet(bucket)
```

UNIQUE 的"唯一"作用域是 **单个 tablet**。同一主键如果落进不同分区，
会出现两行 —— 而且不报错。

这就是为什么 ODS 层**不能**用"装载日期"当分区列：

```
order_id=1 今天更新 → dt=2026-11-04 分区
order_id=1 明天更新 → dt=2026-11-05 分区
→ 同一订单两行 → GMV 偏高
```

分区列必须是**不可变的业务时间**（如下单日期），同一实体的所有版本才会落在同一分区。

### 1.5 类型选择

| 类型 | 要点 |
| --- | --- |
| `VARCHAR(n)` | **n 是字节不是字符**。中文 3 字节、葡语重音 2 字节 |
| `STRING` | 不限长，超长文本用它 |
| `CHAR(n)` | **会补空格**，比较时可能出意外，多数场景用 VARCHAR |
| `DECIMAL(p,s)` | 金额必用，不用 FLOAT/DOUBLE |
| `DATETIME` | 不用 TIMESTAMP（时区 + 2038） |
| `TINYINT` | 布尔用 0/1，Doris 没有 BOOLEAN 的独立存储 |

`VARCHAR(n)` 超长的报错是：

```
Insert has filtered data in strict mode
```

**不带列名**。所以要自己按"实测最大字节数 × 1.5"给余量（见 `06` 的同类问题）。

### 1.6 排序键与前缀索引

Doris 会为每个 tablet 建**前缀索引**（前 36 字节），查询条件命中前缀时能跳读。
所以 Key 列的顺序也影响查询性能：**最常用的过滤列放前面**。

本项目的查询模式是"按 dt 过滤 + 按维度聚合"，所以 Key 多以 `dt` 或"实体 ID + dt"开头。

---

## 二、在本项目中

### 2.1 全库统一 UNIQUE（含 geolocation）

`docs/02` 最初的设计是"ODS 贴源层按原样：事实表 UNIQUE、日志表 DUPLICATE、
geolocation 用 DUPLICATE（因为自然键有 28 万重复）"。实际落地全部改成 UNIQUE，理由：

**geolocation 的"重复"是自然键重复，不是行重复**

```
自然键 (zip, lat, lng) → 28 万条重复
但源表有自增代理键 id，1,000,163 行 id 全唯一
→ UNIQUE(id) 完整保留 100 万行，同时获得幂等
```

如果按原设计用 DUPLICATE：

```
重跑一次作业 → 100 万变 200 万，且无法察觉
UNIQUE 重跑任意次 → 结果恒定
```

顺带好处：10 张 ODS 表同一个模型、同一套 sink 配置，P3 只有一种失败模式要排查。

### 2.2 为什么不用 AGGREGATE

原设计里 DWS 用 AGGREGATE（预聚合模型），实际改 UNIQUE，原因有三：

**① AGGREGATE 只能"加"，不能"改"**

```sql
-- AGGREGATE 模型：value 列声明 SUM
SUM(gmv) → 每次写入是"累加"
-- 但流计算输出的是"这个分组的当前总值"，不是增量
-- 用 SUM 会让 100 → 110 → 121（把总值当增量加）
```

**② Doris Sink 只接受 upsert changelog**

Flink 的 Doris Connector 会在需要时自动插入 `SinkUpsertMaterializer`，
它要求下游能表达"这一行被替换了" —— 这是 UNIQUE 语义，不是 AGGREGATE 语义。

**③ UNIQUE + MoW 的读性能已经够**

DWS 表最大 8.8 万行，MoW 的读路径与 AGGREGATE 差别可以忽略。

### 2.3 各层的主键设计

| 层 | 表 | 主键 | 粒度 |
| --- | --- | --- | --- |
| ODS | `ods_orders` | `order_id` | 订单 |
| ODS | `ods_order_items` | `order_id, order_item_id` | 订单行 |
| ODS | `ods_geolocation` | `id`（代理键） | 坐标点 |
| ODS | `ods_user_event` | `event_id, dt` | 事件 |
| DWD | `dwd_trade_order_detail` | `order_id, order_item_id, dt` | 订单行 |
| DWD | `dim_user_zip` | `customer_unique_id, start_date` | 用户版本 |
| DWS | `dws_trade_overview_1m` | `dt, minute` | 分钟 |
| DWS | `dws_traffic_funnel_1h` | `dt, hour, step` | 小时 × 漏斗级 |
| ADS | `ads_order_stream` | `order_id` | 订单 |

`dim_user_zip` 的主键值得单独看：`(customer_unique_id, start_date)` —— 拉链表的
"一个用户的多个版本"天然是复合主键，`start_date` 是版本标识（见 [19](19-维度建模与拉链表.md)）。

### 2.4 `dt` 为什么排在第 2 列

```sql
UNIQUE KEY(`event_id`, `dt`)
```

两个约束叠在一起：

- 分区列（`dt`）必须在 Key 里
- Key 必须是有序前缀，且**第一列**通常放实体 ID（前缀索引用得上）

所以只能 `(event_id, dt)`。这也意味着**不能**按 `dt` 做前缀过滤加速 ——
查询时要走分区裁剪而不是索引。

### 2.5 VARCHAR 长度是实测出来的

`sql/doris/01-ods.sql` 的注释里写了每个字段的实测最大字节数，例如：

```sql
`customer_city` VARCHAR(64)   -- 实测最长 43 字节（葡语重音 2 字节/字符）
`product_category_name` VARCHAR(128)
```

规则：**实测最大字节 × 1.5 倍余量**，超长文本用 `STRING`。
照抄 MySQL 的 `VARCHAR(64)`（字符）会在遇到重音字符时写入失败。

### 2.6 踩过的坑

**坑 1：`VARCHAR(255)` 装不下中文说明**

```sql
`threshold_desc` VARCHAR(255)   -- 按字节！中文 3 字节/字 → 实际只能写 85 个汉字
```

超了之后整条 INSERT 被 strict mode 拒掉，报 `Insert has filtered data in strict mode`，
**不带列名**。健康度脚本里那条 `overview_join_rows` 的说明就是压着 255 字节写的。

**坑 2：`INSERT INTO` 在 UNIQUE 表上只覆盖不删除**

```
某天某个类目的商品全被取消 → 重算后这类目不该再出现
但 INSERT INTO 会把它上一次的行留在表里 → 排行里挂着一个已不存在的类目
```

刷全量必须 `INSERT OVERWRITE`（见 [17](17-Stream-Load-与-Doris-Sink.md)）。

**坑 3：删 MySQL 的行不会让 Doris 已聚合的行消失**

链路是"upsert"：MySQL 删除 → CDC 发 `-D` → Flink 输出 retract → Doris Sink 需要
`SinkUpsertMaterializer` 才能处理。**纯 upsert 的 UNIQUE 表没有"删除"通路**，
所以清库必须同时清 Doris。

---

## 三、速查

```sql
-- 建表骨架
CREATE TABLE IF NOT EXISTS `t` (
  `id`   VARCHAR(32) NOT NULL,
  `dt`   DATE        NOT NULL,
  `v`    DECIMAL(18,2)
) ENGINE = OLAP
UNIQUE KEY(`id`, `dt`)
COMMENT '...'
PARTITION BY RANGE(`dt`) ( PARTITION `p2026` VALUES [("2026-01-01"), ("2027-01-01")) )
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
  "replication_num" = "1",
  "enable_unique_key_merge_on_write" = "true"
);
```

| 现象 | 原因 |
| --- | --- |
| 建表报 key 顺序错 | Key 不是 schema 有序前缀 |
| 建表报分区列不在 key 里 | 分区列必须加入 Key |
| 同一实体出现两行 | 分区列取了可变值（如装载日期） |
| `filtered data in strict mode` | 列超长/类型不符，且不带列名 |
| 重跑作业行数翻倍 | 用了 DUPLICATE 模型 |
| 聚合值越加越大 | 用 AGGREGATE + SUM 承接了"总值"而非"增量" |

延伸：`14-Apache-Doris.md`、`16-Doris-分区-分桶与-Compaction.md`、
`17-Stream-Load-与-Doris-Sink.md`、`../02-数仓分层模型.md`
