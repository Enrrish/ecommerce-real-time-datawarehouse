# 27 · FineBI 与内存预算

> 这篇记录一次真实的技术选型：拿到一个 13GB 的「FineBI 离线部署包」，要判断这台 16GB 的机器能不能跑，
> 最后怎么跑。结论是**能跑，但不是按包的设计意图跑**——包里那套东西的官方最低配置是 64GB。
>
> 全文只有两个数字来源：帆软官方文档，和本机实测。凡是估的都会写明是估的。

---

## 一、通用知识

### 1.1 BI 工具的两种交付形态

同一个 BI 产品，厂商通常同时提供两种形态，能力一样但资源模型完全不同：

| 形态 | 长什么样 | 进程数 | 典型内存 | 适用 |
| --- | --- | --- | --- | --- |
| **单体安装包** | 一个 Tomcat，解压即用 | 1 个 JVM | 2~4GB | 个人、部门、PoC |
| **容器化集群** | web + 引擎 + 存储 + 网关，多个容器 | 5~20 个 | 32GB 起 | 企业生产 |

关键点：**容器化不等于单体拆分**。有些产品只是把单体塞进容器（还是 1 个进程），有些是真的做了服务化拆分。
判断方法不是看有没有 Dockerfile，而是看**镜像有几个、每个镜像里是什么**。FineBI 属于后者：

```
bi-web            应用 + 元数据（Tomcat + decision servlet）
bi-engine-master  计算引擎协调节点（Polars）
bi-engine-worker  计算引擎计算节点
fine-canvas       仪表板渲染服务
bi-minio          数据存储（对象存储）
```

官方约束：`engine-master` **强制 1 个**，`bi-web : engine-worker` **强制 1:1 或 1:2**，不能自定义配比。

### 1.2 JVM 在容器里的内存模型

这是容器化 Java 应用最容易踩的地方。

**（1）不写 `-Xmx` 会发生什么**

JDK 8u191 之后 JVM 支持容器感知（`UseContainerSupport`），默认 `MaxRAMPercentage=25.0`：

```
docker run --memory=4g my-java-app     # 堆上限 = 4GB × 25% = 1GB
```

看起来"自适应"很聪明，实际是**把容量决策交给了容器限额**。改限额堆就跟着变，线上很难复现。
所以生产上应该显式钉住 `-Xmx`。

**（2）BI 类应用的内存不止堆**

帆软官方给了一个公式（FineBI 帮助文档《修改内存》）：

```
BIMax = Xmx + MaxDirectMemorySize + FineIO + Other
约束：BIMax ≤ 物理内存 × 7/8
```

| 参数 | 是什么 | 官方建议 |
| --- | --- | --- |
| `-Xmx` | Java 堆上限 | 8GB ≤ Xmx < 32GB，**< 8GB 有宕机风险** |
| `-Xms` | Java 堆初始值 | = Xmx |
| `-XX:MaxDirectMemorySize` | 堆外直接内存（NIO/Netty/gRPC） | 4GB |
| `-Dfineio.read_mem_limit` | FineIO 读缓存 | 2GB |
| `-Dfineio.write_mem_limit` | FineIO 写缓存 | 1GB |

按官方建议配下来 `BIMax ≈ 8 + 4 + 3 = 15GB`，再加 `Other`，
所以官方说"**强烈建议物理内存至少 16GB**"。这不是拍脑袋，是公式推出来的。

**（3）"上限"不是"预占"**

`MaxDirectMemorySize=2048M` 只是"最多能用 2G"，不代表启动就占 2G。
实测一个配了 `Xmx2560 + Direct2048 + Metaspace2048 + CodeCache1024`（理论上限 7.5GB）的
FineBI 容器，空载只占 **1.54GB**。所以：

- 判断"装不装得下"看**实测空闲占用**
- 判断"会不会被 OOMKill"看**理论上限和容器限额的关系**

两件事都要看，只看一个会误判。

### 1.3 容器内存限额与 OOMKill

`docker run --memory=5632m` 是 **cgroup 硬上限**，超了内核直接 SIGKILL，
退出码 **137**（128 + 9）。这和 JVM 自己抛 `OutOfMemoryError` 是两回事：

| 现象 | 退出码 | 日志 | 含义 |
| --- | --- | --- | --- |
| 超容器限额 | 137 | 日志戛然而止 | 被内核杀，JVM 来不及说话 |
| 堆不够 | 1 | `java.lang.OutOfMemoryError: Java heap space` | JVM 自己抛的 |
| Metaspace 不够 | 1 | `OutOfMemoryError: Metaspace` | 类加载太多 |
| **进程主动 exit** | **1** | **可能什么都没有** | 应用自己的逻辑 |

最后一行是这篇要重点讲的坑——见 §2.4。

### 1.4 Compose 多项目共存

一个仓库里放两份 compose（比如主栈 + 一个可选组件）时，有三个概念会咬人：

**（1）项目名（project name）**

compose 默认用**所在目录名**当项目名。`docker/docker-compose.yml` 和
`docker/docker-compose.finebi.yml` 都在 `docker/` 下，于是**项目名相同**，后果：

- 两边 `up` 都会警告 `Found orphan containers (…)：这个容器不属于我的服务列表`
- 一个项目的 `down --remove-orphans` 会把另一个项目的容器当垃圾删掉

解法：在 compose 顶层写 `name: rt-bi`，让两份文件属于不同项目。

```yaml
name: rt-bi
services:
  finebi: …
```

**（2）网络**

想复用主栈的网络（才能用 `doris-fe:9030` 这种服务名），必须声明成 external：

```yaml
networks:
  rt-net:
    external: true
    name: rt-net      # ← 必须写，否则 compose 会加项目前缀变成 rt-bi_rt-net
```

**（3）卷**

卷的 ownership 记在 label 上。改了项目名之后，旧卷会报
`volume "x" already exists but was created for project "docker" (expected "rt-bi")`。
带用户数据的卷不能删，这时改成 `external: true`；没数据的直接删了重建更干净。

---

## 二、在本项目中

### 2.1 那个 13GB 的包到底是什么

`D:\迅雷下载\finekey-amd64-tomcat-all-publish.tar.gz`，13GB。解开只有 33 个条目：

```
finekey/
├── bin/finekey                部署工具本体
├── conf/finekey.yaml          节点 IP / SSH / registry 配置
└── resources/
    ├── docker.tgz             自带 Docker 引擎
    └── images/                23 个镜像
```

镜像清单（体积为包内压缩后大小）：

| 镜像 | 体积 | 是什么 |
| --- | --- | --- |
| `bi70all-tomcat` | 2.51 GB | FineBI 7.5 全套（4 个镜像） |
| `bi61all-tomcat` | 2.06 GB | FineBI 6.1 全套（4 个镜像） |
| `fdl50-tomcat` | 1.27 GB | FineDataLink 5.0 |
| `fr-tomcat` | 1.09 GB | FineReport |
| `fine-flink` | 1.02 GB | Flink |
| `ops-tomcat` | 0.77 GB | FineOps 运维平台自身 |
| `elasticsearch` | 0.72 GB | 日志存储 |
| `skywalking_oap` | 0.44 GB | APM |
| `nacos-server` | 0.42 GB | 注册中心 |
| `kafka` / `mysql` / `grafana` / `minio` / `redis` / `prometheus` / `nginx` / `filebeat` / `alertmanager` / `pushgateway` / `registry` / `iotest` / `ops_agent` | 合计约 2.0 GB | 中间件与监控栈 |

**这不是"一个 FineBI 镜像"，是帆软 FineOps 企业版的整套离线编排包。**
`all` 后缀的意思是"组件齐全"，不是"单体"。

### 2.2 为什么 7.5 连试都不用试

帆软官方《确认 FineBI 项目服务器配置》给出的**最低配置方案**（面向日活 0~50 人的最小客户）：

```
CPU 16 核 / 内存 64GB / 磁盘 500GB
部署：1×bi-web + 1×engine-worker + 1×engine-master + 1×bi-minio
      + nginx 内网关 + mysql 配置库 + es 日志存储 + 集群组件
      + 运维平台全部组件
```

**64GB 是这台机器的 4 倍。** 而且引擎节点是强制的，不能只起 web（见 §2.3 的证据）。

### 2.3 只起 bi-web 行不行？——不行，但有个例外

把 `bi70all` 和 `bi61all` 都 load 进来，各得到 4 个镜像。看 `bi-web` 里有什么：

```bash
$ docker run --rm --entrypoint sh bi-web:6.1-… -c 'ls /opt/polars; du -sh /opt/polars'
arthas
8.0K    /opt/polars          # 只有诊断工具，没有引擎

$ docker run --rm --entrypoint sh bi-engine-master:6.1-… -c 'du -sh /opt/polars'
32M     /opt/polars          # 这里才是引擎
```

但 `bi-web` 的 `WEB-INF/lib` 里有这么几个 jar：

```
fine-bi-engine-spider-6.1.jar        ← 内置计算引擎（FineBI 5.x 起就有）
fine-bi-engine-polars-client-6.1.jar ← polars 只是个**客户端**
fine-bi-spider-cluster-6.1.jar
```

**结论：`bi-web` 自带 `spider` 引擎，能独立做基础分析；`polars` 三件套是给大数据量加速的。**
这就是本文选 6.1 单容器的依据——不是为了省镜像数，是因为它真的能单跑。

### 2.4 一次静默退出，和它教我的事

第一版 compose 我按"节约"的思路把厂商默认参数全压了一轮：

| 参数 | 厂商默认 | 我压到 |
| --- | --- | --- |
| `MaxDirectMemorySize` | 2048M | 1024M |
| `MaxMetaspaceSize` | 2048m | 512m |
| `ReservedCodeCacheSize` | 1024m | 256M |
| `fineio.read_mem_limit` | 2 | 1 |

结果：容器启动约 20 秒后**退出码 1，日志里没有异常栈**：

```
2026/11/09 09:43:29 main INFO [com.finebi.cluster] [ClusterModule] cluster update activator start
（到此为止）
```

排查过程：

1. 先怀疑是挂卷导致的 —— 去掉卷重跑，**一样退**
2. 再怀疑是堆参数 —— 换成**镜像默认 JAVA_OPTS**（一个都不改），**起来了，20 秒 healthy**

于是二分。最终结论：**厂商那串参数不能随便砍**，换成"原串 + 只加 `-Xmx`"就正常：

```
厂商原串 + -Xms1024m -Xmx2560m   →  25 秒 healthy，空载 1.547GiB
```

为什么没有继续逐项二分？因为：
- 每试一轮 40 秒，四个参数两两组合要十几次
- 这些值本就是厂商按 746MB 的 `WEB-INF/lib` 算出来的，我并没有比它更懂的依据
- 真正的目标只是"钉住堆上限"，其余照抄成本最低、风险最小

**这里有一条值得记住的判据**：容器退出时，
`OOMKilled=false` + `ExitCode=1` + 日志无异常栈 + 应用自己的日志目录（`FineLog/`）里只有
gc/safepoint 没有业务日志 —— 说明进程是自己 `exit(1)` 的，不是被杀的、也不是 JVM 崩的。
这类问题查系统日志没用，只能靠**改配置二分**。

### 2.5 内存测算

| 项 | 数值 | 来源 |
| --- | --- | --- |
| 物理内存 | 15.69 GB | `Win32_OperatingSystem` |
| WSL2 配额 | 10 GB | `.wslconfig` |
| Docker VM 实见 | **9947 MB** | `docker info .MemTotal` |
| 实时模式 6 容器限额 | **8960 MB** | `docker inspect .HostConfig.Memory` 求和 |
| 实时模式余量 | 987 MB | 9947 − 8960 |
| FineBI 空载实测 | **1540 MB** | `docker stats` |
| FineBI 容器限额 | **5632 MB** | compose `mem_limit` |

**如果两套同时开**：8960 + 5632 = 14592MB > 9947MB，超 4645MB。不可能。
即使 FineBI 只给 2GB，8960 + 2048 = 11008 也超了。**共存这条路是堵死的。**

**为什么按 8960MB 配 FineBI**：实时模式（6 容器）刚好是 8960MB，
BI 模式（fe + be + finebi）配成同一个数，两种模式的峰值一致，
切换前后内存水位不跳变，也就不用动 `.wslconfig`。不是巧合。

### 2.6 最终方案：二选一，而不是共存

```
实时模式（realtime）           BI 分析模式（bi）
─────────────────             ─────────────────
mysql        768              doris-fe      1536
kafka        768              doris-be      1792
doris-fe    1536              finebi        5632
doris-be    1792              ────────────────────
flink-jm     768                       合计 8960
flink-tm    3328
─────────────────
        合计 8960
```

BI 模式砍掉 Flink 和 Kafka 的**依据**：数据已经在 Doris 里了。
Flink 停掉只是不再"继续计算"，Doris 里的 112,650 行 DWD / 88,157 行 DWS 一行不少。
实测验证过：

```bash
$ docker exec rt-doris-fe mysql -h127.0.0.1 -P9030 -uroot -D realtime_dw -N -B -e \
    "SELECT COUNT(*) FROM dwd_trade_order_detail"
112650
```

切换用 `scripts/switch-mode.sh`：

```bash
bash scripts/switch-mode.sh status          # 看现在是什么模式 + 内存账
bash scripts/switch-mode.sh bi              # 停 Flink/Kafka/MySQL，留 Doris，起 FineBI
bash scripts/switch-mode.sh realtime        # 起 6 容器，停 FineBI
bash scripts/switch-mode.sh realtime --submit   # 顺带把 4 个作业提起来
```

### 2.7 实测结果

BI 模式下三个容器的真实占用：

| 容器 | 实测 | 限额 | 使用率 |
| --- | --- | --- | --- |
| rt-doris-fe | 571 MiB | 1536 MiB | 37% |
| rt-doris-be | 1368 MiB | 1792 MiB | 78% |
| rt-finebi | 1519 MiB | 5632 MiB | 28% |
| **合计** | **3458 MiB** | 8960 MiB | — |

VM 内 `MemAvailable` 还有 **6.16 GiB**。也就是说限额是"保险丝"，日常远没到。

连通性验证：

```bash
$ docker exec rt-finebi getent hosts doris-fe
172.28.0.10     doris-fe
$ docker exec rt-finebi bash -c 'exec 3<>/dev/tcp/doris-fe/9030'
9030 可达
```

### 2.8 FineBI 接 Doris

Doris FE 的 9030 是 MySQL 协议，FineBI 直接当 MySQL 数据源接，**不用额外起 MySQL**：

| 字段 | 值 |
| --- | --- |
| 类型 | MySQL |
| 主机 | `doris-fe`（**不能写 localhost**，容器内的 localhost 是容器自己） |
| 端口 | 9030 |
| 用户名 | root |
| 密码 | 空 |
| 数据库 | `realtime_dw` |

能查的表：`dwd_trade_order_detail`(112,650) / `dws_trade_overview_1m`(88,157) /
`ads_realtime_overview` / `ads_order_stream` 等。

注意口径：这些是**上次 Flink 跑完的静态快照**，BI 模式下不会更新。
`cum_gmv` 不可 SUM、`buyer_cnt` 跨维度不可加这些红线（见 `20-指标口径设计.md`）在 BI 里同样成立，
在 FineBI 里做计算字段时要自己守住。

---

## 三、速查

### 3.1 从 13GB 包里只取一个镜像

```bash
cd "/d/迅雷下载"
mkdir -p /d/finebi-pkg
# 只解出需要的那一个 tgz，不展开整个包（13GB 解完要 40GB 磁盘）
tar -xzf finekey-amd64-tomcat-all-publish.tar.gz -C /d/finebi-pkg \
    finekey/resources/images/bi61all-tomcat-amd64-publish.tgz
docker load -i /d/finebi-pkg/finekey/resources/images/bi61all-tomcat-amd64-publish.tgz
```

`docker load` 一个 2GB 的 tgz 约 3 分钟。load 完记得删掉解出来的 tgz（`/d/finebi-pkg` 可以整个删）。

### 3.2 常用命令

```bash
# 起停（独立 compose，项目名 rt-bi）
cd docker && docker compose -f docker-compose.finebi.yml up -d
cd docker && docker compose -f docker-compose.finebi.yml down     # 卷保留
cd docker && docker compose -f docker-compose.finebi.yml down -v  # 卷也删（工程配置丢失）

# 看真实占用
docker stats --no-stream rt-finebi rt-doris-fe rt-doris-be

# 看 JVM 实际生效的堆参数（别信 compose，信 ps）
docker exec rt-finebi ps -ef | grep -o '\-Xmx[0-9a-zA-Z]*'

# 看 FineBI 自己的日志
docker exec rt-finebi ls /usr/local/tomcat/logs/FineLog/

# 健康状态
curl -s --noproxy '*' -o /dev/null -w '%{http_code}\n' \
  http://localhost:9080/webroot/decision/system/health
```

### 3.3 现象 → 原因

| 现象 | 原因 | 怎么确认 |
| --- | --- | --- |
| 容器 20 秒后退出，**退出码 1**，日志无异常栈 | 应用自己 exit。JVM 参数被改坏了，或必要组件缺失 | `docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}'` 看是不是 false/1 |
| 退出码 **137** | 超 `mem_limit`，被内核 SIGKILL | 同上，`OOMKilled=true` 就是它 |
| `Found orphan containers (rt-finebi)` | 两份 compose 项目名相同 | compose 顶层加 `name:` |
| `network rt-net not found` | 主栈没起过，external 网络不存在 | 先起一次 Doris |
| 卷报 `created for project "docker" (expected "rt-bi")` | 改了项目名，卷 label 没变 | 有数据改 `external: true`，无数据删卷重建 |
| FineBI 里连不上 Doris | 数据源写了 `localhost` | 容器内 localhost 是容器自己，必须写 `doris-fe` |
| `docker exec` 里路径变成 `D:/…` | Git Bash 把容器路径翻译成 Windows 路径 | 加 `MSYS_NO_PATHCONV=1` |
| `docker cp` 报 `directory "c:\tmp" does not exist` | 上一条的反面：`MSYS_NO_PATHCONV=1` 又关掉了 POSIX→Windows 翻译 | 目标写 Windows 形式 `"D:/finebi-pkg/x"` |

### 3.4 想再省内存可以试的方向

按性价比排，都没实测过，写在这里备查：

1. **Xmx 再降到 1536m** —— 11 万行数据量足够。风险是复杂仪表板多组件并发时 GC 频繁
2. **停掉 engine 相关的 `/opt/polars` 检查** —— 无所谓，它只有 8K
3. **FineBI 用外接 MySQL 配置库** —— 能省 HSQLDB 的内存，但要多起一个 MySQL，**不划算**
4. **把 `max_connections` / Tomcat 线程数调小** —— `TOMCAT_MAX_THREADS` 默认 500，改 100 能省线程栈
5. **不推荐**：把 `.wslconfig` 提到 12GB 让两套共存 —— Windows 只剩 3.6GB，
   内存压缩会先拖慢整机，体验比"切换"差得多

### 3.5 延伸

- `04-环境搭建.md` §4 —— 10GB / 8960MB 的原始内存定案
- `25-可观测性与链路健康度.md` —— 切换后怎么确认链路真的活着
- `20-指标口径设计.md` —— 在 BI 里做计算字段时要守的口径红线
- `docs/04-环境搭建.md` §6 —— 环境类坑表
- 帆软官方：《修改内存》`help.finebi.com/doc-view-56.html`
- 帆软官方：《确认 FineBI 项目服务器配置》`help.fanruan.com/fineops/edition-view-3341-0.html`
