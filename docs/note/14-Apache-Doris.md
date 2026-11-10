# 14 · Apache Doris

> 一句话：Doris 是 **MPP 架构的 OLAP 数据库**，用 MySQL 协议当门面。它的设计取舍是
> "为聚合查询和批量导入优化，牺牲点更新与复杂事务" —— 所以它适合当数仓的终点，
> 不适合当业务库。

---

## 一、通用知识

### 1.1 两个角色

| | FE（Frontend，Java） | BE（Backend，C++） |
| --- | --- | --- |
| 职责 | 元数据、连接管理、查询解析与规划、调度 | 数据存储、执行、compaction |
| 规模 | 3 副本（1 follower + 2 follower/observer） | 任意，按数据量扩 |
| 状态 | 有元数据（edit log + image） | 有数据（tablet） |
| 单点 | 单 FE 会单点故障 | 多 BE 自动分片与副本 |

FE 的三种角色：

- **Follower**：参与选举、可读写元数据
- **Observer**：同步元数据但不参与选举，用来扩展读
- **Leader**：Follower 中选出来的那个

### 1.2 端口（最容易记混的一组）

| 端口 | 属于 | 用途 |
| --- | --- | --- |
| **9030** | FE | MySQL 协议（`mysql -P9030`） |
| 8030 | FE | HTTP Web UI / REST |
| 9010 | FE | edit log（FE 之间同步元数据） |
| **8040** | BE | HTTP（`/api/health`、`/metrics`、`/varz`、Stream Load） |
| 9050 | BE | brpc（心跳、FE 下发任务） |
| 9060 | BE | brpc（数据交换） |

排障时最常用的是 **9030**（查数据）和 **8040**（查 BE 状态）。

### 1.3 数据组织

```
database
 └─ table
     └─ partition（按 Range/List 切，通常按日期）
         └─ tablet（= 分桶 bucket，数据分布与副本的最小单位）
             └─ rowset（一次导入产生一个版本）
                 └─ segment（列式文件）
```

关键推论：

- **分区裁剪**按 partition 走，分桶决定并行度
- **一次导入 = 一个 rowset = 一个数据版本**，版本多了要 compaction（见 [16](16-Doris-分区-分桶与-Compaction.md)）
- 唯一性只在 **tablet 内**保证 → 同一主键落在不同分区会出现两行（见 [15](15-Doris-数据模型.md)）

### 1.4 三种导入方式

| 方式 | 谁推数据 | 适用 |
| --- | --- | --- |
| **Stream Load** | 客户端 HTTP PUT | 实时写入（Flink Doris Sink 用的就是这个） |
| Broker Load | Doris 主动拉 HDFS/S3 | 大批量离线 |
| Routine Load | Doris 持续消费 Kafka | Kafka → Doris 直连 |
| `INSERT INTO` | SQL | 小批量、ADS 刷新 |

### 1.5 常用 SQL

```sql
SHOW FRONTENDS\G
SHOW BACKENDS\G                      -- Alive / LastHeartbeat / TabletNum / TrashUsedCapacity
SHOW TABLES FROM realtime_dw;
SHOW CREATE TABLE realtime_dw.ads_realtime_overview\G
SHOW TABLETS FROM realtime_dw.dws_trade_overview_1m;   -- 看版本数
SHOW DATA;                            -- 各表体积与副本数

ADMIN SHOW FRONTEND CONFIG LIKE '%trash%';
ADMIN CLEAN TRASH;                    -- 清回收站（注意语义，见 16）
```

```sql
-- 会话变量（只影响当前连接）
SET exec_mem_limit = 8589934592;
SET query_timeout = 300;
```

### 1.6 FE 的"查询黑名单"

BE 重启后，FE 第一次向它发 fragment 可能失败（BE 还没准备好），
FE 会把它拉进**查询黑名单**：

```
There is no scanNode Backend available.[10227: in black list(... UNAVAILABLE: io exception)]
```

迷惑之处：`SHOW BACKENDS` 里 `Alive=true`、心跳正常、`/api/health` 返回 200、
`isQueryDisabled=false` —— **只有查询挂，看起来像数据坏了**。

FE 会按固定间隔重试（默认 90 秒左右），成功后打一条
`remove backend from black list. reach max try time` 自愈。**不要重启 FE**。

### 1.7 副本数

```sql
"replication_num" = "1"
```

单机测试必须写 1，否则 Doris 默认 3 副本会一直等第二个 BE，表创建后不可写。

### 1.8 与 MySQL 的兼容面

| 兼容 | 不兼容 |
| --- | --- |
| MySQL 协议、大部分 DDL/DML 语法 | 没有真正的事务（`BEGIN/COMMIT` 支持有限） |
| `information_schema`（部分表） | 没有外键、没有存储过程、没有触发器 |
| `SHOW` 系列 | 部分函数语义不同（如整数除法） |

---

## 二、在本项目中

### 2.1 单 FE 单 BE

```yaml
  doris-fe:
    image: apache/doris:fe-2.1.11
    environment:
      FE_SERVERS: fe1:172.28.0.10:9010
      FE_ID: 1
  doris-be:
    image: apache/doris:be-2.1.11
    environment:
      FE_SERVERS: fe1:172.28.0.10:9010
      BE_ADDR: 172.28.0.11:9050
```

单机单副本，所有表 `replication_num=1`。这是"资源换可用性"的取舍：
16GB 内存的机器上跑 3 副本没有意义。

### 2.2 库与表的规模

```
realtime_dw
 ├─ ODS  10 张（贴源，全量 UNIQUE，不分区）
 ├─ DWD   7 张（宽表 + 5 张维表，按 dt 静态分区）
 ├─ DWS   6 张（分钟/日粒度聚合，UNIQUE Merge-on-Write）
 └─ ADS   7 张（大屏直读，INSERT OVERWRITE 刷新）
```

`sql/doris/*.sql` 里共 **30 条建表语句**（ODS 10 + DWD 7 + DWS 6 + ADS 7）。
四层都用同一个库，靠表名前缀区分 —— 换来的是"一条 SQL 能跨层 join"，
代价是没有物理隔离（生产上通常按层分库）。

### 2.3 连库方式

```bash
# 从容器内
docker exec -it rt-doris-fe mysql -uroot -P9030 -h127.0.0.1

# 从宿主（如果映射了 9030）
mysql -h127.0.0.1 -P9030 -uroot
```

**注意**：`docker exec rt-doris-fe mysql ...` 里的 `mysql` 是 FE 镜像自带的客户端，
不是 MySQL 容器。两个都叫 `mysql`，很容易在脚本里连错库。

### 2.4 七张验收表

P3~P6 的验收都落到这七张上（逐表 diff 全 0）：

| 表 | 行数 |
| --- | --- |
| `dwd_trade_order_detail` | 112,650 |
| `dwd_user_event_clean` | 22,004（后期随埋点增长） |
| `dws_trade_overview_1m` | 88,157 |
| `dws_traffic_funnel_1h` | 16 |
| `ads_realtime_overview` | 88,157 |
| `ads_order_stream` | 98,666 |
| `dim_geo` | 19,015 |

`dim_geo` 被选进"七表"是因为它是唯一带坐标计算的维表，能验证 `MEDIAN` / 加权平均这类
非平凡聚合没算错。

### 2.5 踩过的坑

**坑 1：重建 BE 必现黑名单，约 100 秒自愈**

本项目重建过三次 BE（改内存、改 be.conf、迁移存储），每次都是同一个剧本：

```
重建 → 立刻查表 → There is no scanNode Backend available[10227]
     → SHOW BACKENDS 显示 Alive=true（所以会怀疑是数据坏了）
     → 等 90~100 秒 → 自动恢复
```

**重建 BE 前先 cancel 重作业**：否则 sink 连续失败会触发 Flink 作业重启，
07 又要重放 41 万行快照。

**坑 2：`INSERT INTO TABLE x` 是语法错误**

```sql
INSERT INTO TABLE foo SELECT ...        -- ❌
INSERT INTO foo SELECT ...              -- ✅
INSERT OVERWRITE TABLE foo SELECT ...   -- ✅
```

报错信息自相矛盾（说 `TABLE` 附近有语法问题，但 `OVERWRITE TABLE` 又是合法的）。

**坑 3：整数除法**

```sql
SELECT 7 / 10;            -- 0，不是 0.7
SELECT 7 * 1.0 / 10;      -- 0.7
SELECT CAST(7 AS DECIMAL(20,6)) / NULLIF(10, 0);   -- 0.700000，且除零返回 NULL
```

这个错误不会报错，只会让所有转化率整齐地变成 0 或 1。

**坑 4：`NOW()` 返回的是 FE 所在机器的时区时间**

容器 `TZ=Asia/Shanghai`，所以与宿主一致。但如果只改了宿主时区没改容器，
指标窗口（`ts_ms >= now - 600s`）会整体错位，表现为"指标永远不恢复"。

---

## 三、速查

```bash
# 连库
docker exec -it rt-doris-fe mysql -uroot -P9030 -h127.0.0.1 -D realtime_dw

# 集群状态
docker exec rt-doris-fe mysql -uroot -P9030 -h127.0.0.1 -e "SHOW BACKENDS\G" | head -25

# 表清点
docker exec rt-doris-fe mysql -uroot -P9030 -h127.0.0.1 -N -e "SHOW TABLES FROM realtime_dw" | wc -l

# BE 健康（HTTP）
docker exec rt-doris-be sh -c "curl -s http://localhost:8040/api/health"

# BE 关键配置
docker exec rt-doris-be sh -c "curl -s http://localhost:8040/varz | grep -E 'trash_file_expire|mem_limit'"
```

| 现象 | 判据 |
| --- | --- |
| `no scanNode Backend available[10227]` | BE 刚重启，等 90~100 秒 |
| `SHOW BACKENDS` 里 Alive=false | 真的挂了，看 BE 日志 |
| 建表成功但写不进去 | `replication_num` 不是 1 |
| 查询结果比预期少 | 分区裁剪把数据裁掉了（见 [16](16-Doris-分区-分桶与-Compaction.md)） |
| 导入报 `filtered data in strict mode` | 列超长/类型不符（见 [15](15-Doris-数据模型.md)） |

延伸：`15-Doris-数据模型.md`、`16-Doris-分区-分桶与-Compaction.md`、
`17-Stream-Load-与-Doris-Sink.md`、`../01-架构设计.md`
