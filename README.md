# 电商实时数仓 · E-Commerce Realtime Data Warehouse

基于 **Kafka + Flink CDC + Flink SQL + Apache Doris** 构建的电商实时数仓，覆盖「业务库 CDC + 埋点日志」双数据源接入、流式 ETL、四层数仓建模（ODS / DWD / DWS / ADS）与实时指标看板。

全链路在 Docker 环境下单机可复现（16GB 内存即可跑通）。

![实时经营大屏](docs/assets/dashboard.png)

*上图是 `dashboard/` 里零依赖大屏的实拍。另有一份**三页经营驾驶舱**的高保真设计稿
（P1 经营总览 / P2 实时监控 / P3 履约转化），逐图给出取数表、字段与口径，
搭 FineBI 可以直接照着复刻 → [docs/06 可视化驾驶舱](docs/06-可视化驾驶舱.md)*

---

## 0. 快速开始（6 条命令：跑起来 5 条，关掉 1 条）

前置：Docker Desktop 已启动（WSL2 后端，内存上限 10GB）；`data/raw/olist/` 下已放好 9 个 CSV
（下载方式见 [data/README.md](data/README.md)）。首次跑要拉 11.6GB 镜像，约 25 分钟。

```bash
# 1. 依赖（只有埋点生成要第三方库；其余脚本纯标准库）
python -m pip install -r scripts/requirements.txt

# 2. 四个 connector jar → docker/flink/lib/（约 52MB，含单独放的 MySQL JDBC 驱动）
bash scripts/fetch-flink-jars.sh

# 3. 起 MySQL、导入 Olist 155 万行、跑九表核对（约 3 分钟）
bash scripts/import.sh

# 4. 一条命令把链路跑起来：六个容器 → 建 Kafka topic → 建 Doris 库表
#    → 回填宽表与日粒度 → 提 4 个常驻作业 → 开大屏（带持续埋点）
bash scripts/demo.sh --with-events

# 5. 浏览器打开 http://127.0.0.1:8080/

# 6. 关掉（顺序、边界都在脚本里；数据全部保留，下次第 4 步秒起）
bash scripts/stop.sh
```

**别用 `docker compose down -v` 关。** 那会把 Doris BE 的命名卷 `rt-doris-be-data`
一起删掉——不是「重启一下」，是 112,650 行 DWD + 88,157 行 DWS + 7 张 ADS 全部消失，
得从 MySQL 重跑 41 万行快照重建。`stop.sh` 任何分支都不带 `-v`。

`demo.sh` 是幂等的：容器在跑就跳过、topic 存在就跳过、表里有数就跳过、作业在跑就跳过。
重复执行只会补上缺的部分。

> **在 PowerShell 里跑？** 上面前提是 Git Bash。PowerShell 里直接敲 `bash scripts/stop.sh`
> 会失败——PATH 里没有 `…\Git\bin`，`bash` 被 `C:\Windows\System32\bash.exe`（WSL 的启动垫片）
> 抢走，回车后只会得到 `Access is denied` 或一句 WSL 的报错，**看起来像脚本坏了**。
> 用绝对路径调它（`where.exe git` 确认装在哪，把 `\cmd\git.exe` 换成 `\bin\bash.exe`）：
>
> ```powershell
> & "C:\Program Files\Git\bin\bash.exe" scripts\stop.sh
> ```
>
> 往 PATH 里加 `…\Git\bin` 没用：Windows 把机器级 PATH 排在用户级前面，System32 仍然赢。
> 想长期省事就在 PowerShell 配置文件里加
> `function bash { & "C:\Program Files\Git\bin\bash.exe" @args }`。

四件容易踩的事（前三件 `demo.sh` 已经替你处理，第四件由 `demo-change.sh` 的自检兜住）：

| 坑 | 不处理的后果 |
| --- | --- |
| Kafka topic 必须先建成 **3 分区** | Kafka 会自动建，但建成 **1 分区**。漏斗的会话归因、dup/late 抽样的分区假设全部失效，且**不报任何错** |
| Doris BE 的 storage 必须用**命名卷** | 挂到宿主机目录时 Docker Desktop 导成 9p，`statvfs` 的 inode 是假数据（可用 inode 为负），BE 判「所有盘都损坏」无限重启。见 `docs/04 §6` |
| 4 个 slot 装不下 6 个作业 | 订单宽表与日粒度汇总是**一次性**的，`demo.sh` 跑完就取消，把 slot 还给常驻作业 |
| 重提 `07` 后要等 1~2 分钟快照 | 作业状态立刻变 RUNNING，但 `initial` 快照还没跑完时它一行都不写。**RUNNING 不等于在产出** —— 判据是 `bash scripts/demo-change.sh` 第 0 步的自检（四个源累计读满 40 万行） |

不想开大屏只想把后端跑起来：`bash scripts/demo.sh --no-dashboard`。
想验证端到端延迟：`python scripts/check_lag.py --write`（真采 200 条探针算 P99）。
想验证「改一行真的会传到大屏」：`bash scripts/demo-change.sh`（先自检作业是否真在产出，
再改一行、量出 DWS / ADS / 面板各自的时延、最后自动改回去）。

想把机器让给 BI 做自助分析：`bash scripts/switch-mode.sh bi`（停 Flink / Kafka / MySQL，
只留 Doris 约 3.3GB），分析完 `switch-mode.sh realtime` 切回来。三页驾驶舱怎么复刻见
[docs/06](docs/06-可视化驾驶舱.md)，内存账见 [note 27](docs/note/27-FineBI-与内存预算.md)。

---

## 1. 项目目标

| 目标 | 落地方式 |
| --- | --- |
| 双源接入 | MySQL 业务库走 Flink CDC（binlog），用户行为走进 Kafka |
| 流式清洗 | Flink SQL 完成去重、脏数据过滤、维表 Join、双时间戳补全 |
| 分层建模 | Doris 内实现 ODS → DWD → DWS → ADS，明细与汇总分离 |
| 一致性保障 | 主键幂等 + Checkpoint + Doris 两阶段提交，端到端精确一次 |
| 指标可解释 | 每个指标有唯一定义与口径文档，避免同名不同义 |

---

## 2. 整体架构

```mermaid
flowchart LR
    subgraph SRC["数据源"]
        MYSQL[("MySQL 8.0.36<br/>Olist 业务库")]
        GEN["埋点生成脚本<br/>gen_user_events.py"]
    end

    subgraph ING["采集层"]
        CDC["Flink CDC<br/>全量 + 增量快照"]
        KAFKA[("Kafka 3.7.1<br/>topic: user_event")]
    end

    subgraph COMPUTE["计算层"]
        FLINK["Flink SQL<br/>去重 / 清洗 / 维表 Join / 窗口聚合"]
    end

    subgraph OLAP["Doris（存储与查询）"]
        ODS["ODS<br/>贴源 + binlog 元信息"]
        DWD["DWD<br/>明细宽表 / SCD2 拉链"]
        DWS["DWS<br/>分钟 · 小时 · 日汇总"]
        ADS["ADS<br/>指标应用层"]
    end

    DASH["实时大屏"]

    MYSQL --> CDC --> FLINK
    GEN --> KAFKA --> FLINK
    FLINK --> ODS
    FLINK --> DWD
    FLINK --> DWS
    ODS -. 下游作业读取 .-> DWD
    DWD -. 下游作业读取 .-> DWS
    DWS -. SQL 全量刷新 .-> ADS
    ADS --> DASH
```

> 实线 = Flink SQL 写入；虚线 = 读出上游表再加工。**ODS / DWD / DWS 三层是 Flink 算完写进
> Doris 的，ADS 是唯一例外** —— 它在 Doris 内部用 `INSERT OVERWRITE` 从 DWS 全量刷新
> （`sql/doris/08-ads-refresh.sql`），不起 Flink 作业。原因很实际：4 个 slot 已被常驻作业
> 占满，而 ADS 是「整表可重建」的小表（最大 9.8 万行），全量刷比重建一条增量流简单得多。
> 除此之外 Doris 不做层间加工（没有物化视图链、没有定时调度），所以「改一行 MySQL 到
> 大屏变化」依然只需一条流，不依赖任何离线任务。

---

## 3. 技术栈

版本不是各挑最新，而是从 Flink 反推出来的交集。选型理由与兼容性链路见 [docs/04 §1~§2](docs/04-环境搭建.md)。

| 组件 | 锁定版本 | 角色 |
| --- | --- | --- |
| Apache Flink | **1.18.1**（scala 2.12 / JDK 11） | 流式计算引擎，全部作业使用 Flink SQL |
| Flink CDC | **3.2.1** | MySQL binlog 实时捕获（+ JDBC 驱动 8.0.27，需单独放） |
| Flink Kafka Connector | **3.1.0-1.18** | 埋点日志消费 |
| Doris Flink Connector | **1.18-24.0.1** | 写入 Doris，Checkpoint 触发 Stream Load |
| Apache Doris | **2.1.11**（单 FE 单 BE） | OLAP 存储与查询，承载四层数仓 |
| Apache Kafka | **3.7.1**（KRaft 单节点） | 埋点日志消息队列 |
| MySQL | **8.0.36** | 业务库（OLTP 源），binlog `ROW` + `FULL` |
| Docker Desktop / Engine | **4.97.0 / 29.9.2** | 环境编排，profile 分层启动 |
| Python | 3.11 | 数据装载与埋点生成脚本 |
| FineBI | **6.1 单机版（Windows 侧）** | 可选：BI 分析模式下直连 ADS 做自助分析，与实时链路二选一（`scripts/switch-mode.sh`） |

---

## 4. 仓库结构

```
.
├── data/                  # 数据集（原始数据不入库，见 data/README.md）
│   └── raw/olist/         # Olist 9 个 CSV，已 gitignore
├── docker/                # 容器编排
│   ├── docker-compose.yml # 三 profile：source / compute / olap
│   ├── be-conf/be.conf    # BE 配置（trash 保留期等），只读挂进容器
│   ├── volumes/           # MySQL / Kafka / FE 的数据目录（gitignore；BE 用命名卷）
│   └── flink/lib/         # 四个 connector jar，由脚本下载（gitignore）
├── sql/
│   ├── mysql/             # 业务库建表 01-schema + 验收 02-verify
│   └── doris/             # 四层建表 01/03/05/07 + 刷新 08 + 验收 02/06/09
├── flink/sql/             # Flink SQL 作业，按分层编号
├── scripts/               # 一键起停 / 装载 / 埋点 / jar / 作业提交 / 健康度 / 改数验收 / 延迟采样
├── dashboard/             # 实时大屏：静态页 + Python 查询代理 + 四档 ADS 刷新
└── docs/
    ├── 01~06-*.md         # 架构、分层模型、指标口径、环境、路线图、可视化驾驶舱
    ├── note/              # 28 篇技术笔记（索引见 docs/note/README.md）
    └── assets/            # 大屏截图、三页驾驶舱设计稿
```

---

## 5. 数据集

使用 **Olist Brazilian E-Commerce Public Dataset**（约 10 万笔订单、9 张关联表），叠加脚本生成的用户行为埋点流，构成「业务库 + 日志」双源场景。

数据不随仓库分发，下载与放置方式见 [data/README.md](data/README.md)。

---

## 6. 文档

| 文档 | 内容 |
| --- | --- |
| [docs/01-架构设计.md](docs/01-架构设计.md) | 链路设计、时间语义、幂等与一致性、资源预算 |
| [docs/02-数仓分层模型.md](docs/02-数仓分层模型.md) | 四层表设计、粒度、主键、Doris 表模型选型 |
| [docs/03-指标口径.md](docs/03-指标口径.md) | 核心指标定义与易错点 |
| [docs/04-环境搭建.md](docs/04-环境搭建.md) | 版本矩阵、兼容性链路、宿主机准备、已知坑 |
| [docs/05-实施路线图.md](docs/05-实施路线图.md) | 九阶段排期、每阶段验收标准、当前进度 |
| [docs/06-可视化驾驶舱.md](docs/06-可视化驾驶舱.md) | 三页驾驶舱的视觉规范、逐图字段清单、口径红线、FineBI 搭建步骤 |
| [docs/07-事故库.md](docs/07-事故库.md) | 8 个「不报错但不对」的坑：现象 / 发现 / 根因 / 判据（唯一事实源） |
| [**docs/note/**](docs/note/README.md) | **技术笔记：28 篇，每项技术先讲通用知识再讲本项目用法** |

> `docs/01`~`07` 回答"为什么这么设计、坑在哪"，`docs/note/` 回答"这个技术怎么用、报错怎么解"。
> 环境起不来查 note 的 `02`/`03`；作业不写数据查 `11`/`12`；Doris 查询变慢查 `16`；
> BI 接入与两套内存配置查 `27`/`28`。

---

## 7. Roadmap

- [x] 数据集选型与目录规范化
- [x] 数仓分层模型与指标口径设计
- [x] 实时大屏视觉稿
- [x] 环境搭建（WSL2 + Docker Desktop + 镜像加速）
- [x] Docker Compose 编排（profile 分层启动）
- [x] MySQL 业务库建表与 Olist 数据装载（155 万行，九表 diff 全 0）
- [x] Flink CDC 接入 ODS（9 张表全量 + 增量，端到端 3.8s）
- [x] 埋点生成脚本与去重验证（3% 重复注入，唯一键吃干净）
- [x] DWD 明细层（交易宽表 112,650 行 / 行为清洗 / SCD2 拉链维表）
- [x] DWS 汇总层（分钟 · 小时 · 日共 6 张预聚合表，与 DWD 重算 diff 全 0）
- [x] ADS 应用层（7 张表，Doris 侧全量刷新）
- [x] 脏数据注入与指标基线对照（3,001 条 + 3% 重复 / 5% 乱序，三层都不被污染）
- [x] 大屏接入 ADS 实数据（三屏九块全真数据；改 MySQL → 大屏 **294~4,019 ms**，P7 口径 = 主动触发 ADS 刷新）
  - 不触发刷新、只等 60s 批刷周期的真实等待是 **17~60s**，两个都对，别混着引（见 [docs/03 §实测基线](docs/03-指标口径.md)）
- [x] 一键启动脚本 `demo.sh` 与端到端延迟真采样 `check_lag.py`（P8）
- [x] 技术笔记 28 篇（[docs/note/](docs/note/README.md)，按技术栈组织，编号即阅读与依赖顺序）
- [x] 三页经营驾驶舱设计稿 + 逐图字段清单与口径（[docs/06](docs/06-可视化驾驶舱.md)）
- [x] FineBI 6.1 单机版接入 ADS，BI 分析模式与实时模式二选一（`scripts/switch-mode.sh`）

---

## 8. 运行环境

开发机配置：AMD R9 7945HX / RTX 4060 Laptop / 16GB RAM / Windows 11 + WSL2 + Docker Desktop。

16GB 内存是本项目的主要约束，因此：

- WSL2 上限锁 10GB，六个容器 limit 合计 **8.75GB**（明细见 [docs/04 §4](docs/04-环境搭建.md)）
- 容器按 `source` / `compute` / `olap` 三组 profile 分批启动，避免 BE 与 TM 同时拉起触发 OOM Killer
- Doris 单 BE、单副本（`replication_num=1`）、BE 内存上限 1.75G（活跃数据只有 400MB）
- Flink 单 TM / **4 slots**，每个作业并行度 1（4 个常驻作业占满），Checkpoint 10s 落本地文件系统
  - 不用 10 slots：TM 的 taskHeap 按 slot 数均分（1489MB ÷ 10 = 149MB），六表 join 跑不动；
    ÷ 4 = 372MB 才行。降 slot 数比加内存总量对症（见 `docs/04 §6`）

---

## 9. License

代码部分采用 MIT License。数据集版权归原作者所有，遵循 **CC BY-NC-SA 4.0**，仅用于学习与研究，不随本仓库分发。
