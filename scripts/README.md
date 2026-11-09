# scripts

数据装载、埋点生成、一键启动与健康度采集脚本。

```
scripts/
├── requirements.txt
├── demo.sh               # 一键：起容器 → topic → 建表 → 回填 → 提作业 → 开大屏
├── prepare_load.py       # 原始 CSV → LOAD DATA 可读的 TSV
├── import.sh             # 一键：起 MySQL → 预处理 → 导入 → 验收
├── gen_user_events.py    # 合成埋点事件 → Kafka
├── fetch-flink-jars.sh   # 下载 4 个 connector jar（含 MySQL JDBC 驱动）
├── submit-sql.sh         # 提交 Flink SQL 作业并打印运行状态
├── collect-pipeline-health.sh  # 采集链路健康度 → ads_pipeline_health
├── demo-change.sh        # 改一行 MySQL，量出它走到大屏要多久（含僵尸自检）
├── calibrate_late_ratio.py  # 给 late_ratio 做正向对照（注入已知比例的迟到事件）
├── check_lag.py          # 端到端延迟真采样 → P50/P95/P99
├── stop.sh               # 安全关停：停大屏 → 按依赖倒序停容器 → 打印怎么再开
└── switch-mode.sh        # 实时数仓模式 / BI 分析模式二选一（内存只够跑一套）
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

采集 11 个链路指标写入 `ads_pipeline_health`，供大屏健康度面板读取。

```bash
bash scripts/collect-pipeline-health.sh              # 全量采集（约 30-40s）
bash scripts/collect-pipeline-health.sh --skip-kafka # 跳过 Kafka 抽样（约 5s）
```

为什么是脚本而不是一条 SQL：指标分布在三个系统 —— Doris（事件年龄 / 管道新鲜度 /
无效交易）、Flink（checkpoint / 作业数）、Kafka（积压 / 重复率 / 乱序率）。
没有任何一个系统能单独算全。**监控必须旁路采集**，不能挂在被观测的主链路上：
主链路挂了、指标也停了，那就永远发现不了挂。

两个口径上的坑：

**① `e2e_latency_ms` 不能是常量。** 它原来是 P3 手工测的一次 3,849ms，写死在脚本里 ——
那不是测量，是引用测量，优化不动、劣化也不动。现在读 `scripts/.latency.json`
（`check_lag.py --write` 落的实测 P99）；没有这个文件才退回历史基线，并且**在阈值
说明里写清「这是基线不是 P99」**。阈值也跟着架构走：`<12000 ok`，因为 sink 的
flush 间隔就是 10s。

**② `checkpoint_p95_ms` 不能用 `summary`。** Flink 的 `/checkpoints` 有两个口径：
`summary` 是**全生命周期**分布（从作业启动那天算起），`history` 是最近若干次。
CDC 作业初始快照期间 checkpoint 会被拖到十几秒，这历史一旦进了 summary 就永远留着 ——
实测一个已经平稳跑了几小时的作业仍然报 **19,095ms** 并常亮 critical。
现在改成从 `history` 里取最近 10 次 COMPLETED 自己算 P95。

**③ `pipeline_freshness_s` 是 MIN 口径，它证明不了每条链都在写。** 三张主表
（`dws_trade_overview_1m` / `dwd_user_event_clean` / `ads_realtime_overview`）取
`update_time` 的最近者，只要有一条链在写，这个值就是 0~几秒。而
`ads_realtime_overview` 每 60 秒被批刷重写一次、`update_time` 跟着刷新 —— 于是
**交易链停多久都看不出来**。

真实现场（2026-11-04 08:49）：07 用 `latest-offset` 重启，join 的 orders 侧状态是空的，
每条 `order_items` 变更都因为「找不到订单」被丢掉，sink 一行不写。那一刻健康度
10 项全 `ok` —— 作业 RUNNING、checkpoint 正常、lag 0、新鲜度 0 s，大屏照旧显示昨天的数字。

补的第 11 项 `overview_join_rows`（07 四个源累计读取行数，实测 415,426）是 join 状态
被填满的**直接证据**，也是目前唯一能发现这件事的指标。它不随「有没有新数据」变化，
所以静态数据集下不会误报；判据 `≥40 万 ok`，低于多半是快照没跑完（等 1~2 分钟）
或用 `latest-offset` 重启后成了僵尸（改回 `initial` 重提）。读不到 Flink REST 时
这一项**不写**，不是写 0。

**④ 别在 `MSYS_NO_PATHCONV=1` 下面起解释器读绝对路径。** 大屏进程用这个开关拉起本脚本
（它保护 `docker exec ... /opt/kafka/...` 里的容器内路径不被 Git Bash 改写），代价是
**同时关掉了「POSIX 路径 → Windows 路径」的翻译**：`python -c ... "/d/Users/..."` 把一个
原生 Windows 程序打不开的路径递了过去，`FileNotFoundError` 又被 `2>/dev/null` 吞掉，
`e2e_latency_ms` 静默退回常量 3849。于是**人工跑脚本正常、大屏跑降级**，两边看到的数不一样，
而且退回去的那个值看着还很健康。现在读 `.latency.json` 改用 `sed`，不起解释器，
路径形态 / 解释器有无 / 文件编码三个依赖一起消失，退回基线时也会打一行显式警告。

**质量指标的口径是这张表最容易做错的地方。** `dup_rate` / `late_ratio` 用
「最近 N 条滑动窗口 + 跨分区均分」，实测试了三次才对：
| 口径 | 测出值（真实注入 3%） | 为什么错 |
| --- | --- | --- |
| 全 topic 累计 | 0.19% | 60 条重复被 2 万条历史稀释 15 倍 |
| 单分区最近 3,000 条 | 1.27% | 窗口大于注入批次，混进 2,000 条旧数据 |
| **3 分区各取 1,000 条** | **2.97%** | ✅ |

累计口径会让质量指标越跑越「健康」，最后彻底失效 —— 这跟「平均值掩盖长尾」
是同一类错误，但更隐蔽：指标本身在动，只是永远不动到告警线。

**`late_ratio` 还要再做一次时间维度的切分。** 光按分区切还不够：采样窗口里经常同时
有「批量回填」和「实时流」，回填事件的 `event_time` 是 1 小时甚至更早以前的，一旦它前面
出现过一条新鲜事件，本分区已见最大值就被抬到「现在」，于是整批回填事件都落后 >60s，
全部计入乱序。实测：

| 场景 | 报出值 | 说明 |
| --- | --- | --- |
| 注入 5% 乱序（100/2000），不做时间过滤 | **66.67%** | 报的是「这批数据整体是旧的」，不是乱序 |
| 同上，只判定近 10 分钟的事件 | 0%（回填 2,000 条被排除） | 分工：回填有多旧看 `event_age_p99_s` |
| 正向对照：发 2,117 条近期事件、其中 290 条迟到 5 分钟 | **13.6986% = 290/2117** | ✅ 分子分母都对 |

最后一行是必须做的**正向对照**：只验证「过滤后变成 0」是不够的，那可能只是把信号
一起滤掉了，等于又造一条永远绿的指标。过滤生效 + 注入能如实报出，两件事都验过才算数。
判据在 `HORIZON`（10 分钟），超过这个年龄的事件不进分子分母。

对照本身可以复现：

```bash
python scripts/calibrate_late_ratio.py --dry-run   # 先看这次会注入多少条
python scripts/calibrate_late_ratio.py             # 1500 条 / 20% 迟到 5 分钟
bash scripts/collect-pipeline-health.sh            # 读 late_ratio 那一行
```

---

## `demo.sh`

```bash
bash scripts/demo.sh                  # 起集群 → 建表 → 回填 → 提作业 → 开大屏
bash scripts/demo.sh --no-dashboard   # 只把后端跑起来
bash scripts/demo.sh --with-events    # 同时起持续埋点（漏斗会一直动）
bash scripts/demo.sh --import         # 业务库是空的，顺手调 import.sh
```

六步，顺序不能换：

| 步 | 动作 | 换顺序的后果 |
| --- | --- | --- |
| 1 | 起六个容器（source → compute → olap 分批） | BE 与 TM 同时拉起会撞 WSL 的 OOM Killer |
| 2 | 建 Kafka topic（3 分区） | 不建 Kafka 会自己建，但是 **1 分区**；漏斗会话归因与 dup/late 抽样的分区假设全部失效，且不报错 |
| 3 | 确认业务库有 155 万行 | 空库提作业 = 快照 0 行，大屏全是空面板，看不出哪里错了 |
| 4 | 建 Doris 库表（01 → 03 → 05 → 07） | 07 的刷新要读 05 建的表，跑反了不报错，只是刷出空表 |
| 5 | 提作业：先两个一次性，再四个常驻 | 4 个 slot 装不下 6 个作业；一次性的跑完必须取消 |
| 6 | 起大屏 | 前置没就绪时 `run.sh` 会直接报错退出，不会白屏 |

幂等判据分别是：容器 `.State.Health` / topic 是否在 list 里 / `olist.orders` 行数 /
`CREATE TABLE IF NOT EXISTS` / 作业名里是否含该 sink 表 / `dwd_trade_order_detail`
与 `dws_trade_category_1d` 的行数。重复执行只补缺的部分。

**为什么第 5 步要分两批**：`05-dwd-trade-order-detail`（订单宽表，112,650 行）和
`08-dws-trade-daily`（品类/卖家/地域日粒度）是「跑一次就够」的历史快照，变更频率是天级。
把它们和常驻作业一起提，4 个 slot 必然不够；所以「提上去 → 等行数到位 → cancel」，
这个流程在脚本里是显式的，不是靠人记得。

## `check_lag.py`

端到端延迟真采样，输出 P50 / P95 / P99。

```bash
python scripts/check_lag.py              # 200 条探针，ODS + DWD 两条链路都测（约 2 分钟）
python scripts/check_lag.py --samples 60 # 快速跑一轮
python scripts/check_lag.py --chain ods  # 只测 Kafka → ODS
python scripts/check_lag.py --write      # 把 P99 写回 ads_pipeline_health
```

做法：往 Kafka 打一条 `event_type=probe_latency` 的探针，轮询 Doris 直到它出现，
`latency = 查到时刻 - 交给 producer 的时刻`。

三个约束决定了它必须这么写：

1. **不能往业务表插测试订单**。GMV 是累计口径，插一行就永久多一行（Doris 纯 upsert
   没有 retract），减不掉。所以探针只能走埋点流。
2. **探针不能污染指标**。`probe_latency` 不在 DWD 的 event_type 白名单里 →
   `is_valid = 0` → 不进漏斗（DWS 带 `WHERE is_valid = 1`）。跑完还会删掉，
   贴源层不留垃圾。
3. **轮询不能用 `docker exec` 裸跑**。单次 826ms，比被测对象还慢。复用
   `dashboard/server/doris.py` 的常驻管道（12~26ms/次）。

已知偏差：轮询粒度 100ms（每个样本 +0~100ms）；sink 的 `buffer-flush.interval = 10s`
是**架构下限**，所以 P99 在 10s 附近是正常的 —— 想压到秒级只能改 flush 间隔，
而 1s 会把 Doris 版本打爆（见 `flink/sql/06` 的注释）。

## `demo-change.sh`

改一行 MySQL，量出它走到大屏要多久。这是「这个项目真的是实时的」最直接的一个证据 ——
不是看链路图，是看一个数字因为一次 `UPDATE` 变了。

```bash
bash scripts/demo-change.sh              # 默认 +1000.00，改完自动改回去
bash scripts/demo-change.sh --delta 500  # 换个幅度
bash scripts/demo-change.sh --keep       # 故意不回滚
```

一次运行的实测输出（P8）：

```
==> 0. 自检（作业存活 + 快照已灌满 join 状态）
  ✓ 07 作业 fd043ef8 RUNNING
  ✓ 四个源累计读取 415422 行（快照已完成，join 状态可用）

==> 1. 选目标行
    交易日   2017-11-24
    订单     27db1a079a22bec1…  明细 #1  分钟桶 01:34
    价格     2400.00 → 3400.00（+1000.00）

==> 4. 观测传播
    DWS           3724 ms   178377.63 → 179377.63
    ADS          10655 ms   178377.63 → 179377.63
    大屏        1254 ms   178377.63 → 179377.63
    端到端（改 MySQL → 大屏可见）17303 ms
```

三个观测点慢的原因完全不同，别混着看：

| 观测点 | 实测 | 时间花在哪 |
| --- | --- | --- |
| DWS | 1.4 ~ 4.2 s | binlog → CDC → 三表 join → 分钟聚合 → Doris sink（07 的 flush 是 1 s） |
| ADS | 10 ~ 57 s | `ads_realtime_overview` 是 60 s 一轮的**批刷**，撞在周期哪个位置全看运气 |
| 大屏 | ≈ ADS + 1.2 s | 面板 1 s 一轮轮询 API |

所以「端到端十几秒到一分钟」的主体是批刷周期，不是 Flink 慢。想让面板更快，
该调的是 `dashboard/server/app.py` 里 full 档的 period，不是并行度。

**第 0 步的自检比演示本身更重要。** 这张表上出过两次「作业 RUNNING 但一行不写」，
两次都没有报错：`latest-offset` 重启后 join 状态为空、`initial` 重启后快照没跑完。
这两种情况下直接改数据会得到「什么都没发生」，而人几乎一定会把它理解成
「链路慢」或者「改动没生效」。所以脚本先确认四个源已累计读满 40 万行，
不到就直接报错退出，不去动那行 MySQL。

**回滚是有保障的**：`trap ... EXIT INT TERM` 会在任何退出路径上把 `price` 改回原值，
包括 Ctrl-C。`--keep` 是唯一不回滚的入口，且会明确提示当前值是脏的。

## `stop.sh`

```bash
bash scripts/stop.sh                 # 停大屏 + 停 6 个容器（数据全部保留）
bash scripts/stop.sh --keep-cluster  # 只停大屏，集群继续跑
bash scripts/stop.sh --purge         # 停完再删容器（down，卷仍保留）
bash scripts/stop.sh --status        # 只看状态，什么都不动
```

### 为什么不直接 `docker compose down -v`

6 个组件里只有 Doris BE 用命名卷，其余都是仓库内的 bind mount：

| 组件 | 数据位置 | `down -v` 会怎样 |
| --- | --- | --- |
| MySQL | `docker/volumes/mysql`（bind mount） | 不受影响 |
| Kafka | `docker/volumes/kafka`（bind mount） | 不受影响 |
| Doris | `rt-doris-be-data`（**命名卷**） | **删掉**，8 张 ADS / 112,650 行 DWD 全没 |

所以 `stop.sh` 任何分支都不会带 `-v`，停完还会再查一次卷在不在。

### 顺序：先停大屏，再按依赖倒序停容器

停大屏要在停容器之前。大屏每 15 秒 `docker exec` 进 FE 查一次表，先停容器的话
它会连续报错十几分钟、日志刷屏，而且那几轮健康度写不进去。

容器不是一个 `docker compose stop` 完事，而是逐个停，顺序按依赖倒序：

```
flink-tm → flink-jm → doris-be → doris-fe → kafka → mysql
计算层        结果层          消息层    数据源
```

三个理由，都是实测踩出来的：

1. `docker compose stop` 是**并行**的，没有顺序保证，会出现「Flink 还在写、
   Doris 已经关了」的窗口，那批 flush 打在空气里
2. compose 文件里的 `stop_grace_period` **只对重建后的容器生效**（写进
   `Config.StopTimeout`）。第一次加这个配置时容器还是旧配置（实测 `StopTimeout=nil`），
   于是走默认 10s：Doris BE / MySQL / Kafka 全部退出码 **137**（SIGKILL）。
   重建容器后 `docker inspect` 才看到 45 / 60 / 30
3. 逐个停才能拿到每个容器的退出码，直接看出谁被硬杀

所以脚本用 `docker stop -t <grace>` 显式指定，不依赖容器创建时的配置。输出长这样：

```
  ✓ rt-flink-tm    2s 退出码 143（优雅）
  ! rt-doris-be    45s 退出码 137（45s 内没退完，被 SIGKILL）
```

`143` = 收到 SIGTERM 自己退的，`137` = 超时被 SIGKILL。看到 137 不一定是坏事
（Doris 下次启动会做 rowset 恢复），但连续出现就说明 grace 该调大。

### 数据基线：关停前记一次行数

每次关停会把 7 张主表的行数写进 `scripts/.shutdown-snapshot.txt`，
下次 `--status` 直接对比：

```
基线时间 2026-11-04 11:28:18 关停
    ads_realtime_overview      88157     ✓ 未变
    dwd_trade_order_detail     112650    ✓ 未变
    dwd_user_event_clean       104464    → 106469（差 +2005）
```

埋点那两张表行数上升是正常的（停机窗口里 Kafka 积压被补消费），
**下降才是问题**——那说明有数据丢了。

### 重启后 Flink 是空 session

容器起来不等于作业在跑。Docker 重启会丢掉整个 Flink session，4 个作业一个都不在。
重启走 `bash scripts/demo.sh --no-dashboard`（幂等，会补提作业），
然后**等 07 的快照跑完**（41 万行，1~2 分钟）再验收：`RUNNING` 不等于在产出。

## `switch-mode.sh`

```bash
bash scripts/switch-mode.sh                  # 看当前模式 + 内存账（同 status）
bash scripts/switch-mode.sh bi               # BI 模式：只留 Doris，起 FineBI
bash scripts/switch-mode.sh realtime         # 实时模式：起 6 容器，停 FineBI
bash scripts/switch-mode.sh realtime --submit  # 顺带把 4 个作业提起来
```

### 为什么是"切换"而不是"共存"

Docker VM 实见 9947MB，实时模式 6 个容器限额已经是 8960MB，只剩 987MB 余量。
FineBI 单容器空载就要 1540MB。两套同时开必然超。

FineBI 那一档只能给 7808MB —— 这不是选出来的，是它的 JVM 自检顶出来的**下限**
（四项和 7680MB，没压是因为压了省不出用量，见 `docs/note/27`）：

| 模式 | 组成 | 限额 |
| --- | --- | --- |
| realtime | mysql + kafka + fe + be + jm + tm | 8960MB |
| bi | fe + be + finebi | 11136MB |

`bi` 的 11136MB 比 VM 实见的 9947MB 还高，属于**纸面超卖**：`mem_limit` 是保险丝
不是预占，实测三容器合计只用 3.7GB，VM 内还剩 5.6GB 可用。

### BI 模式为什么能砍掉 Flink 和 Kafka

数据已经落在 Doris 里了。停 Flink 只是不再"继续计算"，不是丢数据——
`dwd_trade_order_detail` 112,650 行、`dws_trade_overview_1m` 88,157 行一行不少。

代价是 Doris 里变成**静态快照**，BI 模式下不会更新。要"实时"就切回 realtime。

### 停的顺序：Flink 先走

`flink-tm → flink-jm → kafka → mysql`。先停 Flink 是因为它每 10s 往 Doris 写一批，
先断写端再断上下游；反过来停会让 Flink 报一串 connection refused 再自己退出，
日志脏，且 checkpoint 可能失败。

### 没动的东西

- 不动 `.wslconfig`。提到 12GB 能让两套共存，但 Windows 只剩 3.6GB，内存压缩会先拖慢整机
- 不删任何卷。切模式只 `stop`，任何分支都不带 `-v`
- 不改 finekey 那套 23 镜像的编排。FineOps 企业版官方最低 64GB，跑不动

细节和实测数据见 `docs/note/27-FineBI-与内存预算.md`。
