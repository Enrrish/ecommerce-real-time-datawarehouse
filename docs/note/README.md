# 技术笔记 · 索引

这套笔记按**技术/工具**组织，不按项目阶段组织：每一项先讲它本身（是什么、怎么用、坑在哪），
再讲**本项目里具体怎么用**（真实配置、真实数字、真实报错）。

写作目的是查得动，不是通读。面试前翻 `25`、`20`；环境起不来翻 `02`、`03`；作业不写数据翻 `11`、`12`；
Doris 查询变慢翻 `16`。

- 项目主文档：`../01-架构设计.md` ~ `../05-实施路线图.md`
- 本目录：27 篇，编号即阅读顺序，也是依赖顺序（后面的篇目默认你读过前面的）
- 所有命令在 **Git Bash** 下执行；涉及容器内路径时统一加 `MSYS_NO_PATHCONV=1` 前缀（原因见 `05`）

---

## 一、技术栈总览

```
                        ┌──────────────── 行为流（过程）────────────────┐
  scripts/gen_user_events.py ──JSON──▶ Kafka(user_event, 3 分区) ──▶ Flink 04 ──┐
                                                                                  │
  ┌──────── 交易流（状态）────────┐                                              ▼
  MySQL 8.0.36 (binlog ROW/FULL) ──binlog──▶ Flink CDC ──▶ Flink SQL ──▶ Apache Doris 2.1.11
  Olist 9 张业务表                           (01/02/03/05)    (06/07/09)      ODS 10 → DWD 7 → DWS 6 → ADS 7
                                                                                  │
                                             Python 大屏后端 (http.server) ◀──────┘ MySQL 协议 :9030
                                                        │
                                            原生 JS + 手写 SVG（零依赖）
```

运行时：WSL2（10GB / 8 核）→ Docker → 6 容器（mysql / kafka / flink-jm / flink-tm / doris-fe / doris-be）

---

## 二、篇目

### A 基础设施

| # | 篇目 | 一句话 | 本项目用在哪 |
| --- | --- | --- | --- |
| 01 | [WSL2](01-WSL2.md) | Windows 上的轻量虚拟机，Docker Desktop 的后端 | `.wslconfig` 分配 10GB / 8 核；磁盘放 D 盘 |
| 02 | [Docker](02-Docker.md) | 镜像 / 容器 / 卷 / 网络四件套与调试命令 | 6 个容器；命名卷只给 Doris BE |
| 03 | [Docker Compose](03-Docker-Compose.md) | 用一份 YAML 描述多容器拓扑 | `docker/docker-compose.yml`，profiles / healthcheck / mem_limit |
| 04 | [Git 与仓库卫生](04-Git-与仓库卫生.md) | 提交、忽略、换行、体积 | `.gitattributes` 锁 LF；`flink/lib` 不入库 |
| 05 | [Bash 脚本工程](05-Bash-脚本工程.md) | `set -euo pipefail`、trap、参数解析、MSYS 路径 | `scripts/` 下 8 个脚本，`demo.sh` 六步幂等 |

### B 数据源

| # | 篇目 | 一句话 | 本项目用在哪 |
| --- | --- | --- | --- |
| 06 | [MySQL 8](06-MySQL.md) | 关系库、字符集、导入、校验 | `olist` 库 9 张表 155 万行 |
| 07 | [MySQL Binlog 与 CDC 原理](07-MySQL-Binlog-与-CDC-原理.md) | ROW 格式 binlog 如何变成变更流 | 9 张 ODS 表全部走 binlog |
| 08 | [Apache Kafka](08-Apache-Kafka.md) | 分区、位点、消费者组、KRaft | `user_event` 3 分区，两条链路唯一的事件入口 |

### C 计算

| # | 篇目 | 一句话 | 本项目用在哪 |
| --- | --- | --- | --- |
| 09 | [Apache Flink](09-Apache-Flink.md) | JM/TM、slot、部署模式、提交作业 | 1 JM + 1 TM 4 slots，session 模式 |
| 10 | [Flink SQL](10-Flink-SQL.md) | 用 SQL 写流作业、连接器、STATEMENT SET | 9 份 SQL，5,609 行 |
| 11 | [Flink CDC](11-Flink-CDC.md) | 增量快照、server-id、时区、元数据列 | 9 张 ODS + 07 四表 join |
| 12 | [Flink 状态管理与 Checkpoint](12-Flink-状态管理与-Checkpoint.md) | 状态后端、checkpoint、恢复与 OOM | 134MB join 状态撑爆 217MB/slot 的现场 |
| 13 | [时间语义与 Watermark](13-时间语义与-Watermark.md) | event time / proctime、水位线、迟到 | 07 用 `PROCTIME()` 而非窗口；埋点不许「未来时间」 |

### D 存储

| # | 篇目 | 一句话 | 本项目用在哪 |
| --- | --- | --- | --- |
| 14 | [Apache Doris](14-Apache-Doris.md) | FE/BE 架构、端口、查询入口 | 四层 30 张表都在这一个库里 |
| 15 | [Doris 数据模型](15-Doris-数据模型.md) | UNIQUE / AGGREGATE / DUPLICATE、Key 前缀 | 全库统一 UNIQUE + Merge-on-Write |
| 16 | [Doris 分区、分桶与 Compaction](16-Doris-分区-分桶与-Compaction.md) | 分区裁剪、tablet、版本、compaction | 静态分区保历史；版本积压 4,434 → 查询 28 秒 |
| 17 | [Stream Load 与 Doris Sink](17-Stream-Load-与-Doris-Sink.md) | 导入事务、flush 策略、版本生成 | sink 10s 一批，环形缓冲死锁现场 |

### E 建模与口径

| # | 篇目 | 一句话 | 本项目用在哪 |
| --- | --- | --- | --- |
| 18 | [实时数仓分层建模](18-实时数仓分层建模.md) | ODS/DWD/DWS/ADS 职责与流批边界 | 10 / 7 / 6 / 7 张表，每层 diff 全 0 |
| 19 | [维度建模与拉链表](19-维度建模与拉链表.md) | 星型、SCD2、代理键、退化维 | `dim_user_zip` 96,096 人；`dim_geo` 19,015 |
| 20 | [指标口径设计](20-指标口径设计.md) | GMV、漏斗、迟到率、可加性 | GMV 15,843,553.24；7.91% 迟到率 |
| 21 | [数据质量与一致性校验](21-数据质量与一致性校验.md) | diff、幂等重放、脏数据、正向对照 | 逐表 diff 全 0；注入 5% 却报 66.67% 的排查 |

### F 应用、可观测性与扩展

| # | 篇目 | 一句话 | 本项目用在哪 |
| --- | --- | --- | --- |
| 22 | [Python 工程](22-Python-工程.md) | 标准库够用的部分、argparse、Kafka 客户端 | 埋点生成、时延探针、校验脚本 |
| 23 | [HTTP 服务与大屏后端](23-HTTP-服务与大屏后端.md) | `http.server`、缓存控制、常驻管道 | `dashboard/server/app.py`，四档刷新 |
| 24 | [原生 JS 与 SVG 可视化](24-原生-JS-与-SVG-可视化.md) | DOM、fetch、轮询、手写 SVG、CSS Grid | 零依赖大屏，8 个面板 |
| 25 | [可观测性与链路健康度](25-可观测性与链路健康度.md) | 黄金指标、判活口径、阈值设计 | 11 项指标写进 `ads_pipeline_health` |
| 26 | [时延测量与压测](26-时延测量与压测.md) | 探针、分位数、端到端拆解 | 改一行 MySQL 到大屏可见的完整拆账 |
| 27 | [FineBI 与内存预算](27-FineBI-与内存预算.md) | 容器化 BI 的组件拆分、JVM 堆外内存、cgroup OOMKill | 13GB FineOps 包怎么只取一个镜像；双模式切换的内存账 |

---

## 三、按问题反查

| 现象 | 先看 |
| --- | --- |
| 作业 `RUNNING` 但目标表一行不写 | `11`（僵尸作业）、`12`（判活反模式） |
| 作业反复 `RESTARTING`，集群 `slots-total` 归 0 | `12`（TM OOM）、`09`（slot 与 taskHeap） |
| Kafka 积压一直涨，作业不消费 | `08`（位点）、`11`（毒消息硬失败） |
| Doris 查询从 60ms 退化到几十秒 | `16`（版本与 compaction） |
| `There is no scanNode Backend available[10227]` | `14`（FE 黑名单自愈） |
| 导入报 `Insert has filtered data in strict mode` 且不带列名 | `15`（VARCHAR 按字节）、`25`（varchar 255 的坑） |
| 中文写进去报超长 | `15`（`VARCHAR(n)` 的 n 是字节） |
| 时间整体偏 8 小时 / 全落 1970 分区 | `11`（`server-time-zone`、`op_ts`） |
| 刷新页面全屏「加载中…」 | `23`（缓存控制）、`24`（render 抛错） |
| 面板有数但永远不变 | `23`（`subprocess` 命中 WSL 启动器）、`26`（静默降级） |
| 数字对不上 / 口径吵架 | `20`（可加性、分母） |
| 想验证「改了之后真的生效」 | `21`（正向对照）、`26`（端到端拆账） |
| 想接个 BI 工具看数据，怕内存不够 | `27`（组件拆分、内存测算、双模式切换） |
| 容器起 20 秒就退出，退出码 1，日志没异常 | `27`（JVM 参数改坏会静默 exit，不是 OOMKill） |
| 两份 compose 互相报 `orphan containers` | `27`（项目名冲突） |

---

## 四、约定

**版本**（全项目锁死，不追新）

| 组件 | 版本 | 说明 |
| --- | --- | --- |
| MySQL | 8.0.36 | binlog `ROW` + `FULL` |
| Kafka | 3.7.1 | KRaft 模式，无 ZooKeeper |
| Flink | 1.18.1-scala_2.12-java11 | session 集群 |
| Doris | 2.1.11 | 单 FE 单 BE |
| Python | 3.11+ | 只用标准库 + `kafka-python` |

**四个连接器 jar**（`flink/lib/`，不入库，由 `scripts/fetch-flink-jars.sh` 下载）

- `flink-sql-connector-mysql-cdc-3.2.1.jar`
- `flink-sql-connector-kafka-3.1.0-1.18.jar`
- `flink-doris-connector-1.18-24.0.1.jar`
- `mysql-connector-java-8.0.27.jar` —— **不在 connector 里**（GPLv2），少它会在 TM 启动时
  `ClassNotFoundException`，而报错不会提「缺驱动」

**命令前缀**：容器内路径统一 `MSYS_NO_PATHCONV=1`；访问本机端口统一 `curl --noproxy '*'`。
两条都是 Git Bash / 本机代理环境特有的，原因见 `05`。

---

## 五、这套笔记和主文档的分工

| 想知道 | 去哪 |
| --- | --- |
| 为什么这么设计（选型、分层理由） | `../01-架构设计.md` |
| 每张表的字段、粒度、主键 | `../02-数仓分层模型.md` |
| 某个指标怎么算、分母是谁 | `../03-指标口径.md` |
| 版本矩阵、环境怎么搭、踩过哪些坑 | `../04-环境搭建.md` |
| 每一步做了什么、验收数字 | `../05-实施路线图.md` |
| **某个技术本身怎么用、报错怎么解** | **本目录** |
