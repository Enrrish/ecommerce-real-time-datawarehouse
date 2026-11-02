# scripts

数据装载与埋点生成脚本。

```
scripts/
├── requirements.txt
├── prepare_load.py       # 原始 CSV → LOAD DATA 可读的 TSV
├── import.sh             # 一键：起 MySQL → 预处理 → 导入 → 验收
├── gen_user_events.py    # 合成埋点事件 → Kafka
├── fetch-flink-jars.sh   # 下载 4 个 connector jar（含 MySQL JDBC 驱动）
├── submit-sql.sh         # 提交 Flink SQL 作业并打印运行状态
└── collect-pipeline-health.sh  # 采集链路健康度 → ads_pipeline_health（P6）
```

## 依赖

```bash
python -m pip install -r scripts/requirements.txt
```

只有 `gen_user_events.py --sink kafka` 需要第三方库；`prepare_load.py` 和
`--sink stdout` 纯标准库，不装任何东西也能跑。

---

## `prepare_load.py`

`data/raw/olist/*.csv` → `build/load/*.tsv`。

这不是"数据清洗"，是 **`LOAD DATA` 吃不下原始文件**，具体四件事：

| 问题 | 后果 |
| --- | --- |
| `order_reviews` 有正文含双引号（RFC4180 用 `""` 转义） | MySQL 的 `ENCLOSED BY '"'` 不认这种转义，字段被切断 |
| `order_reviews` / `category_translation` 是 CRLF | 按 `\n` 解析会让最后一列尾部多一个 `\r`，DATETIME 报 1292 |
| 空字段是 `""` 而非 `NULL` | 空串进 DATETIME/INT 直接报错；进文本列则让空值核对永远失败 |
| 评论正文内含换行 | 一条记录被切成多行，后续全部错位 |

做法：空串 → `\N`，其余做反斜杠转义（`\t` `\n` `\r` `\\`）。**不改任何业务值**——
不动大小写、不补零、不四舍五入、不删行。唯一有损的地方是 geolocation 的经纬度
（原始 14 位小数，`DECIMAL(10,7)` 保留 7 位）。

输出 `build/load/manifest.json` 记录行数与空值分布，供导入后对账。

## `import.sh`

```bash
bash scripts/import.sh
```

起容器 → 等健康检查 → 检查是否已导入（幂等）→ 预处理 → `LOAD DATA` → 跑 `02-verify.sql`。

已导入过会直接退出，不会重复写入。要重来：

```bash
docker compose --project-directory docker --profile source down -v
rm -rf docker/volumes/mysql/*
```

## `gen_user_events.py`

合成用户行为埋点写 Kafka。为什么是合成：Olist 只有交易结果（订单、支付），
没有任何行为过程，而大屏的转化漏斗必须由行为流算出来。

```bash
# 批量 2 万条
python scripts/gen_user_events.py --events 20000

# 持续压测：每秒 30 条，跑 10 分钟
python scripts/gen_user_events.py --rate 30 --duration 600

# 只打印，不连 Kafka
python scripts/gen_user_events.py --events 5 --sink stdout

# 幂等验证：混入 3% 重复事件（event_id 相同）
python scripts/gen_user_events.py --events 20000 --dup-rate 0.03

# 质量验证：3% 重复 + 5% 乱序到达（迟到 ≥ 5 分钟）
python scripts/gen_user_events.py --events 3000 --dup-rate 0.03 --late-rate 0.05
```

**`--late-rate` 只改发送顺序，不改 `event_time` —— 这一点是必须的。**
把 `event_time` 改早但投递顺序不变，在 event-time 语义下仍然算准时到达，
下游（无窗口聚合）测不出任何区别，验证不到东西。真正的乱序必须让事件
「晚到」：先把它从有序队列里抽出来、挪到队尾再发。
抽取条件是 `event_time <= 队尾 - late_min_sec`，保证迟到量**至少** N 秒，
而不是「平均 N 秒」——否则抽出来的事件可能只早 3 秒，落在任何合理水位线内。

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--events` | 20000 | 事件总数（实际会略超，不会少） |
| `--rate` | 0 | 每秒条数，0 = 不限速 |
| `--duration` | 0 | 持续秒数，配合 `--rate` |
| `--window` | 3600 | `event_time` 铺开的时间窗口（秒） |
| `--dup-rate` | 0 | 重复事件比例（同 `event_id` 发两次） |
| `--late-rate` | 0 | 乱序到达比例（只改发送顺序，不改 `event_time`） |
| `--late-min-sec` | 300 | 乱序事件的迟到量下限（秒） |
| `--p-add` / `--p-checkout` / `--p-pay` | 0.18 / 0.35 / 0.50 | 漏斗各环节转化率 |
| `--bootstrap` | `localhost:29092` | 宿主机侧地址（容器内用 `kafka:9092`） |
| `--seed` | 无 | 随机种子，便于复现 |

设计要点：

1. **实体用真实 ID**：`product_id` 取自 `olist_products`，`user_id` 取自
   `customer_unique_id`。这样 `ods_user_event` 才能和 `dim_product_zip` / `dim_user_zip`
   （均带 `is_current=1`）真 join 出品类漏斗，而不是自造 ID 再自己 join 自己。
2. **漏斗按商品独立转化**：一次会话浏览 1~3 个商品，每个商品各自走
   加购→提交→支付。对齐 `docs/03 §2`「同一用户同一商品分别计入各自环节」。
3. **`event_time` 不晚于生产时刻**：会话起点在窗口内随机，末尾越界就把整段前移。
   违反这条的后果是 Flink 当迟到数据丢掉，现象是「生产了几万条，漏斗只统计一半」，
   而且日志不报错。
4. **`session_id` 做 Kafka message key**：同一会话进同一分区，分区内保序，
   漏斗按 session 归因依赖这一点。

实测（2 万条，`--seed 20261030`）：`page_view` 77.9% / `add_cart` 14.5% /
`checkout` 5.0% / `pay` 2.6%，支付转化率 3.36%，投递 11,810 条/秒。

---

## `fetch-flink-jars.sh`

```bash
bash scripts/fetch-flink-jars.sh
```

从 Maven 中央仓库拉 **4 个** jar 到 `docker/flink/lib/`（约 52MB，已 gitignore）。
前三个是 connector，版本**强耦合**，改任何一个都要回 `docs/04 §1` 重新核对：

| jar | 版本 | 强耦合点 |
| --- | --- | --- |
| `flink-sql-connector-mysql-cdc-3.2.1.jar` | 3.2.1 | 支持 Flink 1.17~1.19 |
| `flink-sql-connector-kafka-3.1.0-1.18.jar` | 3.1.0-1.18 | 制品名带 Flink 次版本，必须 1.18 |
| `flink-doris-connector-1.18-24.0.1.jar` | 24.0.1 | 同上，1.18 专用包 |
| `mysql-connector-java-8.0.27.jar` | 8.0.27 | **不在上面任何一个 fat jar 里**，见下 |

两个最隐蔽的坑：

1. **Doris connector 的制品名带 Flink 次版本**。升级 Flink 忘了换它，集群能起、作业能提交，
   第一条数据写 Doris 时才报 `NoSuchMethodError`。
2. **MySQL JDBC 驱动必须单独放**。`flink-sql-connector-mysql-cdc` 因 GPLv2 许可刻意不打包它，
   缺了之后作业**能编译、能提交、能拿到 Job ID**，只在 TaskManager 连库那一刻抛
   `ClassNotFoundException: com.mysql.cj.jdbc.Driver`，报错文本完全不提"缺驱动"。

脚本会校验文件名里的 `1.18` 和文件头是否为 `PK`（防止下载到 HTML 错误页）。

Maven 中央慢的时候换镜像：

```bash
MAVEN_MIRROR=https://maven.aliyun.com/repository/public bash scripts/fetch-flink-jars.sh --force
```

---

## `submit-sql.sh`

```bash
bash scripts/submit-sql.sh flink/sql/01-ods-orders.sql
```

提交一个 Flink SQL 文件（可含多条 `CREATE TABLE` + `INSERT`），然后打印作业是否真的 RUNNING。

三个实现细节，都是踩坑后定下来的：

| 做法 | 原因 |
| --- | --- |
| 先用 `docker cp` 把 SQL 送进 JM，再 `sql-client.sh -f` | 直接 `-f /tmp/x.sql` 会被 Git Bash 的 MSYS 路径改写毁掉，报 `Fail to read content from /opt/flink/C:/Users/.../job.sql` |
| 每条命令加 `MSYS_NO_PATHCONV=1` | 同上，防止以 `/` 开头的参数被当成 Windows 路径 |
| `--dry-run` 只拷贝不执行 | 改完 SQL 想先看一眼有没有语法问题，又不想污染集群时用 |

> 脚本末尾打印的 `flink list` **不能当成功判据**：刚提交的作业它经常抓不到（读的是 CLI
> 自己的缓存）。真正的判据是 REST 接口
> `curl -s localhost:8081/jobs/overview`，看 `state` 是不是 `RUNNING`。

> SQL 文件里必须有 `SET 'execution.attached' = 'false';`，否则 sql-client 挂在前台不退出，
> 脚本一直等不到结束。

---

## `collect-pipeline-health.sh`

采集 10 个链路指标写入 `ads_pipeline_health`，供大屏健康度面板读取。

```bash
bash scripts/collect-pipeline-health.sh              # 全量采集（约 30-40s）
bash scripts/collect-pipeline-health.sh --skip-kafka # 跳过 Kafka 抽样（约 5s）
```

为什么是脚本而不是一条 SQL：指标分布在三个系统 —— Doris（事件年龄 / 管道新鲜度 /
无效交易）、Flink（checkpoint / 作业数）、Kafka（积压 / 重复率 / 乱序率）。
没有任何一个系统能单独算全。**监控必须旁路采集**，不能挂在被观测的主链路上：
主链路挂了、指标也停了，那就永远发现不了挂。

**质量指标的口径是这张表最容易做错的地方。** `dup_rate` / `late_ratio` 用
「最近 N 条滑动窗口 + 跨分区均分」，实测试了三次才对：

| 口径 | 测出值（真实注入 3%） | 为什么错 |
| --- | --- | --- |
| 全 topic 累计 | 0.19% | 60 条重复被 2 万条历史稀释 15 倍 |
| 单分区最近 3,000 条 | 1.27% | 窗口大于注入批次，混进 2,000 条旧数据 |
| **3 分区各取 1,000 条** | **2.97%** | ✅ |

累计口径会让质量指标越跑越「健康」，最后彻底失效 —— 这跟「平均值掩盖长尾」
是同一类错误，但更隐蔽：指标本身在动，只是永远不动到告警线。

---

## 待补充（P7）

- [x] 乱序注入 —— 已内联到 `gen_user_events.py --late-rate`，没有单独建脚本
      （重复事件由 `--dup-rate` 覆盖，两者共用同一批生成逻辑更省事）
- [x] 健康度采集 —— `collect-pipeline-health.sh`
- [ ] `check_lag.py`：端到端延迟 P99 采样（当前用 P3 的 3,849ms 单次实测作常量，
      需要采 200 次以上才够报 P99，见 `docs/03 §5`）
