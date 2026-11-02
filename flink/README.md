# flink

Flink SQL 作业，全部使用 SQL 而非 DataStream API（本项目定位是数仓建模，不是引擎开发）。

```
flink/
├── sql/
│   ├── 01-ods-orders.sql        # 订单表 CDC 示范（参数逐条注释，其余作业照此复制）
│   ├── 02-ods-dimensions.sql    # 5 张维表：customers / sellers / products /
│   │                            #            category_translation / geolocation
│   ├── 03-ods-facts.sql         # 3 张明细表：order_items / order_payments / order_reviews
│   ├── 04-ods-user-event.sql    # Kafka topic user_event → ods_user_event
│   ├── 05-dwd-trade-order-detail.sql  # 6 表 CDC 直连 join → 交易宽表（P4 核心）
│   └── 06-dwd-user-event-clean.sql    # Kafka 埋点清洗 → dwd_user_event_clean
├── checkpoints/                 # Checkpoint 落盘目录（按 job id 分子目录，已 gitignore）
├── lib/                         # 四个 connector jar（gitignore，见 scripts/fetch-flink-jars.sh）
└── jobs/                        # 空。早期规划的目录名，已由 sql/ 取代，保留占位
```

`sql/doris/04-dwd-load.sql` 不在这个目录里 —— 它是**在 Doris 侧跑的批处理 SQL**，
负责 5 张维表的一次性装载，不经过 Flink。分层依据见文末「P4 实测结论」。

**为什么按「批」分文件而不是一表一文件**：10 张 ODS 表分 4 个文件，一次 `submit-sql.sh` 提交
一批。理由不是省事，而是 slot —— 单 TM 的 slot 数是固定的，一次提交 5 个作业便于观察
「第 5 个还能不能拿到 slot」，出问题时能立刻判断是配置错还是资源不够。

## 提交方式

```bash
bash scripts/submit-sql.sh flink/sql/02-ods-dimensions.sql
```

脚本做三件事：`docker cp` 把 SQL 送进 JM → `sql-client.sh -f` 执行 → 打印 `flink list`。
用 `docker cp` 而不是 `-f /tmp/job.sql`，是为了绕开 Git Bash 的路径改写（MSYS 会把
`/tmp/x.sql` 改成 `C:/Users/.../tmp/x.sql` 再传给容器，报 `Fail to read content from`）。

> 脚本末尾那个 `flink list` 只是给人看的，**不能当成功判据** —— 刚提交的作业它经常抓不到。
> 判断作业是否真在跑，用 `curl -s localhost:8081/jobs/overview` 看 `state`。

每个 SQL 文件开头必须有 `SET 'execution.attached' = 'false';`，否则 sql-client 会挂在前台
不退出，脚本永远等不到结束。

## 作业规范

| 项 | 约定 |
| --- | --- |
| 文件名 | 两位编号 + 目标层，编号即执行顺序 |
| 文件头 | 注释写明：数据源、目标表、粒度、依赖、`server-id` 段 |
| 表名 | 与 Doris 侧完全一致，便于排查 |
| 并行度 | **全部 1**。slot 数是内存切分粒度，见下面「关键配置」 |
| `server-id` | 每表一段，互不重叠（见下表） |
| Watermark | ODS / DWD 层不设。CDC 是 changelog 流，没有乱序问题；watermark 从 DWS 窗口聚合才开始用 |

`server-id` 分配（MySQL CDC 靠它伪装成 slave，**两个作业撞号会互相踢下线**，
现象是「两个作业交替重启」，而不是报错）：

| 作业 | 段 | 宽度 |
| --- | --- | --- |
| `orders` | 5401–5404 | 4 |
| `customers` | 5411–5414 | 4 |
| `sellers` | 5421–5424 | 4 |
| `products` | 5431–5434 | 4 |
| `category_translation` | 5441–5444 | 4 |
| `geolocation` | 5451–5458 | **8**（100 万行，多分片并行快照） |
| `order_items` | 5461–5464 | 4 |
| `order_payments` | 5471–5474 | 4 |
| `order_reviews` | 5481–5484 | 4 |
| `05-dwd` 的 `orders` | 5531–5534 | 4 |
| `05-dwd` 的 `order_items` | 5535–5538 | 4 |
| `05-dwd` 的 `order_payments` | 5539–5542 | 4 |
| `05-dwd` 的 `customers` | 5543–5546 | 4 |
| `05-dwd` 的 `products` | 5547–5550 | 4 |
| `05-dwd` 的 `sellers` | 5551–5554 | 4 |
| `05-dwd` 的 `category_translation` | 5555–5558 | 4 |

DWD 作业读的是**和 ODS 完全相同的 7 张表**，但必须另开一段 server-id 段——
它和 ODS 是两套独立的 binlog 消费者，撞号会让两个作业互相踢下线。

每段留 4 个而不是 1 个，是为将来调大并行度留余量——改并行度不用回来重排 server-id。

## 关键配置

配置不写在 SQL 里，而是 compose 的 `FLINK_PROPERTIES`（JM/TM 各一份，必须一致）：

```yaml
taskmanager.numberOfTaskSlots: 4
parallelism.default: 1
execution.checkpointing.interval: 10s
execution.checkpointing.mode: EXACTLY_ONCE
state.checkpoints.dir: file:///opt/flink/checkpoints
state.backend: rocksdb            # 集群默认；DWD 作业在 SQL 里覆盖成 hashmap
state.backend.incremental: true
```

> **`numberOfTaskSlots` 是内存切分粒度，不是"能跑几个作业"的开关。**
> TM 的 taskHeap（实测 868MB）按 slot 数**均分**：10 个 slot 时每个只有 86.8MB，
> 跑单流同步够，跑 6 表 join 不够；降到 4 个后每 slot 217MB。作业少的时候降 slot 数
> 集中内存给重作业，ODS 那种单流作业多的时候再调回 10。这一步不需要改内存总量。
>
> **Checkpoint 间隔就是数据可见延迟的上限**，不是「30s 更保险」。Doris Connector 靠
> checkpoint 触发 Stream Load 提交，间隔设太大，现象是「作业 RUNNING 但 Doris 查不到数据」。
> 本项目设 10s，实测端到端 3,849 ms。
>
> 早期文档写过「Doris Sink 需要 Checkpoint ≥ 30s」——**这条在本版本不成立**，10s 实测正常。
> 真正需要长间隔的是某些要跨 checkpoint 严格对齐的两阶段提交实现，不是这个 connector。
>
> **`state.backend` 要按作业选，不要一刀切。** 集群默认 rocksdb 适合状态小的单流同步；
> DWD 宽表的状态约 115MB 且写入模式是「小 key、随机、海量」，在这个环境
> （Docker Desktop on Windows）上 RocksDB 的磁盘 I/O 会成为瓶颈 —— 实测聚合算子
> CPU 只花 2.3 秒，墙钟磨了 5 分钟还停在 11/14 ack。SQL 里 `SET 'state.backend' = 'hashmap'`
> 覆盖后秒过。TB 级状态才值得忍受 RocksDB 的磁盘开销。

## Doris Sink 的必填项

```sql
'connector'         = 'doris',
'fenodes'           = 'doris-fe:8030',
'username'          = 'root',            -- 必须显式写，默认是 null 不是 root
'password'          = '',
'table.identifier'  = 'realtime_dw.ods_xxx',
'sink.label-prefix' = 'ods_xxx',        -- 必须全局唯一，否则并发写同一 label 会报错
'sink.properties.format' = 'json',
'sink.properties.read_json_by_line' = 'true',
'sink.enable-delete' = 'true',          -- 不设的话 DELETE 同步不过来，行会一直留着

-- 下面三行在 24.0.1 上是**保命的**，见「已知坑」第一条
'sink.buffer-size'   = '1048576',
'sink.buffer-count'  = '32',
'sink.buffer-flush.max-bytes' = '10485760',
'sink.write-mode'    = 'STREAM_LOAD_BATCH'
```

几个实测到的限制：

- 该版本**不支持** `sink.batch.interval` / `sink.batch.size`，只有 `sink.buffer-flush.*`；
- `sink.buffer-flush.max-rows` 的下限是 **10000**，想调小压延迟会直接被拒；
- `sink.buffer-flush.max-bytes` 的下限是 **10485760（10MB）**，低于它 connector 直接拒绝启动；
- `sink.write-mode` 除了默认的 `STREAM_LOAD`，还支持 `STREAM_LOAD_BATCH` / `COPY` / `VALUES`。

## 已知坑（Flink SQL 侧）

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| 作业提交成功、拿到 Job ID，TM 启动即 `ClassNotFoundException: com.mysql.cj.jdbc.Driver` | CDC connector 因 GPLv2 不打包 MySQL JDBC 驱动 | 单独放 `mysql-connector-java-8.0.27.jar`。报错文本完全不提"缺驱动" |
| `Could not find any factory for identifier 'mysql-cdc'` | jar 没进容器。多半是 compose 挂载源路径写错，Docker 静默建了空目录 | `docker exec rt-flink-jm ls -la /opt/flink/lib` 看 jar 是不是 `drwxrwxrwx` |
| `Invalid metadata key 'op'` | 该版本只暴露 `row_kind`，没有 `op` 元数据 | 用 `row_kind` 映射，见 01 号文件 |
| `Cannot apply 'UNIX_TIMESTAMP' to arguments of type TIMESTAMP_WITH_LOCAL_TIME_ZONE` | `UNIX_TIMESTAMP` 只接受字符串 | 先 `DATE_FORMAT` 再解析 |
| 快照行 `ts_ms = 0` / `dt = 1970-01-01` | 快照阶段没有 binlog 事件，`op_ts` 是 epoch 0 | `dt` 取 `CURRENT_DATE`，`ts_ms` 保留 0 当哨兵 |
| `bufferFlushMaxRows must be greater than or equal to 10000` | 下限校验 | 去掉该项，用默认值 |
| **作业 RUNNING、Doris 表 0 行、checkpoint 永远卡在 3/14、日志一条错误都没有** | 24.0.1 的 Stream Load 请求体走环形缓冲（默认 1MB×3），一次 flush 超过环容量时写端把环写满、读端取空环，两端都阻塞在 `LinkedBlockingDeque.take()` | 设 `sink.write-mode='STREAM_LOAD_BATCH'` 走整批请求体；同时把环放大到 32MB 并把单批压到 10MB。**只放大环没用**，必须换写入模式。定位靠 `docker exec rt-flink-tm kill -3 1` 抓线程栈 |
| **所有算子都在出数，但某个算子 ack 不动** | 若线程栈停在 `RocksDB.put` 的 native 调用且 CPU 时间极低，是磁盘 I/O 等待不是死锁 | `SET 'state.backend' = 'hashmap'` |
| `OVER windows' ordering in stream mode must be defined on a time attribute` | 流模式 OVER 聚合强制 ORDER BY 落在时间属性上 | 在 source 里用计算列 + `WATERMARK FOR` 提升成时间属性。但要想清楚尾部滞回（见下一条） |
| **加了 watermark 后表里少了几十行，缺的总是最新的那几条** | 水位线永远落后于最新数据，`max - delay` 之后的行会卡在 OVER 算子里出不来；没有新数据推动就永远不出 | 别为了排序而加 watermark。排序交给下游 `ORDER BY event_time`，event_time 本身不可变、毫秒精度够用 |

完整版（含环境层）在 `docs/04 §6`。

## P4 实测结论（写给 P5 的自己）

DWD 是第一次遇到 **join** 的层，和 ODS 的单流同步是两类问题。实跑下来有四条结论：

1. **6 表 CDC 直连 join 不会丢数据。** Flink 的 regular join 是双向的，两侧都保留状态，
   `orders` 后到时会和已缓存的 `order_items` 重新匹配补发。代价是同一行可能输出两次，
   由 Doris UNIQUE 主键覆盖。所以不需要「先建维表再 lookup」那一套。
2. **`ods_category_translation` 必须 `LEFT JOIN + COALESCE`。** 它只有 71 个类目，
   源表 73 个，另有一批商品类目为 NULL；`INNER JOIN` 会静默丢 13 个商品。
3. **DWD 的 `dt` 第一次成为分区列**，取 `DATE(order_purchase_timestamp)` —— 不可变，分区安全。
   但要注意它是**订单行粒度**：`orders` 里 775 个没有明细的订单（603 unavailable + 164 canceled）
   不会进宽表，所以订单数、支付总额这类指标必须从 `orders` / `order_payments` 取。
4. **维表不走流式 SCD2。** `LAG` 是 OVER 聚合、吃不了 CDC changelog；lookup join 在
   24.0.1 上认证链路走不通。维表是一次性加载的静态历史，用 Doris 批处理
   （`sql/doris/04-dwd-load.sql`）更短更好验证。

## 下一步（P5）

DWS 6 张预聚合表。三个要点：

- 窗口聚合会第一次真正用上 watermark，`docs/05` P5 段写的「延迟 30s + allowed lateness 5 分钟」
  要在实测里校准 —— 别照抄参数，先量出数据的实际乱序分布。
- 支付方式占比从 `ods_order_payments` 算，不在 DWD 宽表里 —— 支付方式是订单级的一对多属性，
  硬塞进订单行粒度的宽表只能是近似（见 `sql/doris/03-dwd.sql` 里 `dwd_trade_order_detail` 的注释）。
- DWS 的 GMV 要和 P4 宽表的口径对齐：`SUM(item_total)` = 15,843,553.24 是基准线。
