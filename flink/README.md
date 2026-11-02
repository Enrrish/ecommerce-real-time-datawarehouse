# flink

Flink SQL 作业，全部使用 SQL 而非 DataStream API（本项目定位是数仓建模，不是引擎开发）。

```
flink/
├── sql/
│   ├── 01-ods-orders.sql        # 订单表 CDC 示范（参数逐条注释，其余作业照此复制）
│   ├── 02-ods-dimensions.sql    # 5 张维表：customers / sellers / products /
│   │                            #            category_translation / geolocation
│   ├── 03-ods-facts.sql         # 3 张明细表：order_items / order_payments / order_reviews
│   └── 04-ods-user-event.sql    # Kafka topic user_event → ods_user_event
├── checkpoints/                 # Checkpoint 落盘目录（按 job id 分子目录，已 gitignore）
├── lib/                         # 四个 connector jar（gitignore，见 scripts/fetch-flink-jars.sh）
└── jobs/                        # 空。早期规划的目录名，已由 sql/ 取代，保留占位
```

**为什么按「批」分文件而不是一表一文件**：10 张表分 4 个文件，一次 `submit-sql.sh` 提交
一批。理由不是省事，而是 slot —— 单 TM 只有 10 slots，一次提交 5 个作业便于观察
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
| 并行度 | **全部 1**。10 个作业 × 1 = 10 slots，正好占满单 TM |
| `server-id` | 每表一段，互不重叠（见下表） |
| Watermark | ODS 层不设。CDC 是 changelog 流，没有乱序问题；watermark 从 DWS 窗口聚合才开始用 |

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

每段留 4 个而不是 1 个，是为将来调大并行度留余量——改并行度不用回来重排 server-id。

## 关键配置

配置不写在 SQL 里，而是 compose 的 `FLINK_PROPERTIES`（JM/TM 各一份，必须一致）：

```yaml
taskmanager.numberOfTaskSlots: 10
parallelism.default: 1
execution.checkpointing.interval: 10s
execution.checkpointing.mode: EXACTLY_ONCE
state.checkpoints.dir: file:///opt/flink/checkpoints
state.backend: rocksdb
state.backend.incremental: true
```

> **Checkpoint 间隔就是数据可见延迟的上限**，不是「30s 更保险」。Doris Connector 靠
> checkpoint 触发 Stream Load 提交，间隔设太大，现象是「作业 RUNNING 但 Doris 查不到数据」。
> 本项目设 10s，实测端到端 3,849 ms。
>
> 早期文档写过「Doris Sink 需要 Checkpoint ≥ 30s」——**这条在本版本不成立**，10s 实测正常。
> 真正需要长间隔的是某些要跨 checkpoint 严格对齐的两阶段提交实现，不是这个 connector。

## Doris Sink 的必填项

```sql
'connector'         = 'doris',
'fenodes'           = 'doris-fe:8030',
'table.identifier'  = 'realtime_dw.ods_xxx',
'sink.label-prefix' = 'ods_xxx',        -- 必须全局唯一，否则并发写同一 label 会报错
'sink.properties.format' = 'json',
'sink.properties.read_json_by_line' = 'true',
'sink.enable-delete' = 'true'           -- 不设的话 DELETE 同步不过来，行会一直留着
```

两个实测踩到的限制：

- 该版本**不支持** `sink.batch.interval` / `sink.batch.size`，只有 `sink.buffer-flush.*`；
- `sink.buffer-flush.max-rows` 的下限是 **10000**，想调小压延迟会直接被拒。压延迟只能靠
  checkpoint 间隔。

## 已知坑（Flink SQL 侧）

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| 作业提交成功、拿到 Job ID，TM 启动即 `ClassNotFoundException: com.mysql.cj.jdbc.Driver` | CDC connector 因 GPLv2 不打包 MySQL JDBC 驱动 | 单独放 `mysql-connector-java-8.0.27.jar`。报错文本完全不提"缺驱动" |
| `Could not find any factory for identifier 'mysql-cdc'` | jar 没进容器。多半是 compose 挂载源路径写错，Docker 静默建了空目录 | `docker exec rt-flink-jm ls -la /opt/flink/lib` 看 jar 是不是 `drwxrwxrwx` |
| `Invalid metadata key 'op'` | 该版本只暴露 `row_kind`，没有 `op` 元数据 | 用 `row_kind` 映射，见 01 号文件 |
| `Cannot apply 'UNIX_TIMESTAMP' to arguments of type TIMESTAMP_WITH_LOCAL_TIME_ZONE` | `UNIX_TIMESTAMP` 只接受字符串 | 先 `DATE_FORMAT` 再解析 |
| 快照行 `ts_ms = 0` / `dt = 1970-01-01` | 快照阶段没有 binlog 事件，`op_ts` 是 epoch 0 | `dt` 取 `CURRENT_DATE`，`ts_ms` 保留 0 当哨兵 |
| `bufferFlushMaxRows must be greater than or equal to 10000` | 下限校验 | 去掉该项，用默认值 |

完整版（含环境层）在 `docs/04 §6`。

## 下一步（P4）

DWD 作业会第一次遇到 **join**，和 ODS 的单流同步是两类问题：

- `dwd_trade_order_detail` 要 join 6 张 ODS 表，状态量比现在 10 个作业加起来还大。
  **先把用不上的 ODS 作业停掉腾 slot**，不要硬挤。
- `ods_category_translation` 缺 2 个类目，必须 `LEFT JOIN + COALESCE`，`INNER JOIN` 会
  静默丢 13 个商品（影响 GMV 5,514.48）。
- DWD 的 `dt` 才第一次成为分区列，取 `DATE(order_purchase_timestamp)` —— 不可变，分区安全。
