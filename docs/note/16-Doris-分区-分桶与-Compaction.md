# 16 · Doris 分区、分桶与 Compaction

> 一句话：分区和分桶决定"数据怎么摊开"，compaction 决定"摊开之后能不能读得动"。
> 前者是建表时一次性的选择，后者是运行期会**悄悄恶化**的东西。

---

## 一、通用知识

### 1.1 分区（partition）

```sql
PARTITION BY RANGE(`dt`) (
  PARTITION `p2026` VALUES [("2026-01-01"), ("2027-01-01"))
)
```

用途：**裁剪**。查询带 `WHERE dt = '2026-11-04'` 时只扫那个分区。

两种管理方式：

| | 静态分区 | 动态分区 |
| --- | --- | --- |
| 谁建 | 建表时写死，或 `ALTER TABLE ADD PARTITION` | 导入时自动建 |
| 回收 | **永不自动回收** | 可按 `start/end` 自动 DROP |
| 风险 | 跨年要手工加 | **`start=-30` 会把历史分区当天回收** |

动态分区的 `start/end` 是"相对于当前时间的范围"：

```sql
"dynamic_partition.start" = "-30"     -- 只保留最近 30 天
"dynamic_partition.end"   = "7"
```

如果历史数据是 2016~2018 年，导入时每个分区都"早于 30 天前" → 建完立刻被回收。
现象是 **导入报成功、`COUNT(*)` 是 0**，日志里只有分区回收记录。

### 1.2 分桶（bucket）

```sql
DISTRIBUTED BY HASH(`order_id`) BUCKETS 8
```

- `HASH(col)`：按列哈希，同 key 落同一 tablet（利于聚合与点查）
- `RANDOM`：随机，写入均匀但不保证同 key 同 tablet
- `BUCKETS` 建议让单个 tablet 在 **1~10GB** 之间

tablet 总数 = `分区数 × 桶数 × 副本数`。桶数太大会产生大量小文件，
太小则并行度不足。

### 1.3 rowset 与"版本"

**每次导入 = 一个 rowset = 表的一个新版本**。

```
version 1  [rowset A]
version 2  [rowset A][rowset B]
version 3  [rowset A][rowset B][rowset C]
...
```

读的时候要把这些 rowset 合并（或按主键取最新）。版本越多：

- 读放大越严重（要扫的 rowset 越多）
- 内存占用越高（元数据 + 索引）
- 查询越慢

### 1.4 Compaction：把多个 rowset 合成一个

| 类型 | 作用 |
| --- | --- |
| cumulative compaction | 把"新版本"合并成较大的 rowset |
| base compaction | 把 cumulative 的结果合并进 base（最老的那份） |
| full compaction | 全量重写（手动触发，很重） |

compaction 由 BE 后台线程做，**受内存和 I/O 影响**。内存紧张时它会停摆，
而停摆**不报错** —— 只是版本数一直涨。

### 1.5 版本积压的恶性循环

```
导入频率过高（如 1 秒一次）
  → 版本产生速度 > compaction 速度
  → 版本堆积
  → 元数据与索引占内存上涨
  → 内存更紧张 → compaction 更慢
  → 读放大严重 → 查询从 60ms 退化到 28s
```

而且**版本清不掉**：不能删单个版本，只能 `TRUNCATE` 整表重来。

### 1.6 trash：删除的 rowset 去哪了

`TRUNCATE` / `DROP` / compaction 产生的旧 rowset 会被移进 `storage/trash/`，
按 `trash_file_expire_time_sec`（默认 86400 秒 = 24 小时）回收。

两个要点：

- **只有 BE 运行中才调度回收**。长期硬杀（退出码 137）会让回收积压
- **`ADMIN CLEAN TRASH` 是"立刻全清"，不是"只清过期部分"** ——
  它会把 10 分钟前的目录也删掉。要连跑几轮才能见底

### 1.7 运维命令

```sql
SHOW PARTITIONS FROM t;                 -- 分区与行数
SHOW TABLETS FROM t;                    -- 每个 tablet 的版本数、大小
ADMIN SHOW REPLICA STATUS FROM t;       -- 副本状态
ALTER TABLE t ADD PARTITION p2027 VALUES [("2027-01-01"), ("2028-01-01"));
TRUNCATE TABLE t;                       -- 清空（保留表结构）
ADMIN COMPACT TABLE t;                  -- 手动触发 compaction
ADMIN CLEAN TRASH;                      -- 清回收站
```

---

## 二、在本项目中

### 2.1 ODS 一律不分区

`docs/02` 最初写"统一按 dt 动态分区"，落地时改成"ODS 不分区"，理由两条：

**① 动态分区会立刻回收历史**

Olist 数据是 2016-09 ~ 2018-10。`start=-30` 意味着导入时每个分区都早于 30 天前，
建完就被回收 → "导入成功但 0 行"。

**② 更根本：UNIQUE 表的分区列必须不可变**

若 `dt` 取"装载日期"，同一 `order_id` 今天更新落一个分区、明天更新落另一个分区 →
**同一订单两行** → GMV 偏高，且不报错。

ODS 的数据量（155 万行 / 118MB）根本不需要分区裁剪。

### 2.2 DWD 用静态分区

```sql
PARTITION BY RANGE(`dt`) (
  PARTITION `p2016` VALUES [("2016-01-01"), ("2017-01-01")),
  PARTITION `p2017` VALUES [("2017-01-01"), ("2018-01-01")),
  PARTITION `p2018` VALUES [("2018-01-01"), ("2019-01-01")),
  PARTITION `p2026` VALUES [("2026-01-01"), ("2027-01-01"))
)
```

DWD 的 `dt` 是**下单日期**，不可变 → 分区安全。

`p2026` 是给"实时新增订单"用的（Olist 原始数据只到 2018-10，测试写入是 2026 年）。
**跨年要手工 `ALTER TABLE ADD PARTITION`** —— 这是静态分区的代价，换来的是
"历史分区永远不会被自动回收"。

### 2.3 桶数按数据量定

| 表 | 行数 | BUCKETS |
| --- | --- | --- |
| `dwd_trade_order_detail` | 112,650 | 8 |
| `dwd_user_event_clean` | 22,004 | 4 |
| `dws_trade_overview_1m` | 88,157 | 4 |
| `ads_order_stream` | 98,666 | 4 |

单 tablet 几 MB 到几十 MB，远小于 1GB 的建议值 —— 因为这是单机演示，
桶数的作用主要是**并行度**而不是容量。

### 2.4 版本积压的真实事故

```
起因：sink 的 buffer-flush.interval 设成 1s（想降低延迟）
     → 每秒一次 Stream Load
     → 每秒一个数据版本

3 小时后：
  dwd_user_event_clean   单 tablet 积压 4,434 版本
  dws_traffic_funnel_1h  6,276 版本（表里只有 56 行！）

后果链：
  BE 内存顶到 2.25GB → compaction 08:19 起停摆
  → COUNT(*) GROUP BY 从 60ms 退化到 28 秒
  → 7.7 万行的表读不动
```

**这个故障全程不报错，只是变慢。** 而且版本清不掉：

```sql
TRUNCATE TABLE dwd_user_event_clean;      -- 唯一的办法
TRUNCATE TABLE dws_traffic_funnel_1h;
TRUNCATE TABLE ods_user_event;
-- 然后从 Kafka 重放（77,948 条），行数与清空前逐表一致
```

### 2.5 现在的导入频率

| 作业 | sink 频率 | 说明 |
| --- | --- | --- |
| 04 / 06 / 09 | **10s** | 跟随 checkpoint |
| 07 | 1s | **无数据流时不产生版本**，所以安全 |

结论：**任何有持续数据流的 sink，导入频率不得低于 10 秒。**
10 秒一次的代价是"数据可见延迟上限 10s"，这个代价必须接受。

### 2.6 trash 的稳态与清理

```
现象：BE 卷标称 9.6GB，du -sh data 只有 406MB
真相：差额 9.1GB 全在 trash/，35,080 个目录

判据（区分稳态与残留）：
  按小时分布 —— 11-03 15:36 一直均匀排到 11-04 15:26，约 3,000 个/小时
  → 稳态：大屏每 60s 对 ADS 做 INSERT OVERWRITE，旧 rowset 全进 trash
  集中在几个时刻 → 才是 TRUNCATE 残留
```

处理：

```sql
ADMIN CLEAN TRASH;    -- 连跑三轮
-- 第一轮 9.2GB / 35,080 → 3.6GB / 6,713
-- 第二轮 → 14.8MB / 54
-- 第三轮 → 1.3MB / 0
```

卷从 9.7GB 降到 552MB，七表行数逐表不变。

**保留期已从 86400 改成 3600 秒**（写在 `docker/be-conf/be.conf`，只读挂载），
稳态从 ~9GB 降到 ~700MB。代价：误删后的找回窗口从 1 天缩到 1 小时。

⚠️ 注意 `ADMIN CLEAN TRASH` 的语义：**它是"立刻全清"，不是"只清过期"**。
保留期已经 3600 秒时，它依然把 10 分钟前的目录删了。

### 2.7 慢查询的排查顺序

```
查询变慢
 ① 看版本数：SHOW TABLETS FROM t; → MaxVersionCount 是否 > 100
 ② 看 compaction：BE 日志里 compaction 是否停摆
 ③ 看内存：docker stats，BE 是否长期接近 limit
 ④ 看数据量：是不是扫了全表（缺分区裁剪）
 ⑤ 才是看 SQL 写法
```

本项目一次实测：三条 `COUNT(*)` 都是 1.2~1.6 秒（含 `docker exec` 固定开销 0.8 秒），
版本数 922 / 597 / 591 —— 离危险区很远，不需要干预。

---

## 三、速查

```sql
-- 分区
SHOW PARTITIONS FROM realtime_dw.dwd_trade_order_detail;
ALTER TABLE t ADD PARTITION p2027 VALUES [("2027-01-01"), ("2028-01-01"));

-- 版本数（每表取最大值）
SHOW TABLETS FROM realtime_dw.dws_trade_overview_1m;
-- +-----------+--------+-------------+...
-- | TabletId  | ...    | VersionCount|

-- 清
TRUNCATE TABLE t;
ADMIN CLEAN TRASH;
```

```bash
# BE 卷与 trash 体积
docker run --rm -v rt-doris-be-data:/d alpine sh -c \
  "du -sh /d /d/data /d/trash; ls -1 /d/trash | wc -l"

# trash 按小时分布（判稳态 vs 残留）
docker run --rm -v rt-doris-be-data:/d alpine sh -c \
  "ls -1 /d/trash | cut -c1-10 | sort | uniq -c"

# 保留期是否生效
docker exec rt-doris-be sh -c "curl -s http://localhost:8040/varz | grep trash_file_expire"
```

| 现象 | 判据 |
| --- | --- |
| 导入成功但 `COUNT(*)=0` | 动态分区把历史分区回收了 |
| 同一实体两行 | 分区列取了可变值 |
| 查询从 60ms 变 28s | 版本积压，`SHOW TABLETS` 看 VersionCount |
| BE 卷比 `data/` 大一个数量级 | `trash/` 占的，看按小时分布判稳态 |
| `ADMIN CLEAN TRASH` 删了不该删的 | 它是"立刻全清"，不是"只清过期" |
| compaction 停摆 | BE 内存不足，先解决内存 |

延伸：`14-Apache-Doris.md`、`15-Doris-数据模型.md`、`17-Stream-Load-与-Doris-Sink.md`、
`../04-环境搭建.md` §4（BE 内存与 trash 的关系）
