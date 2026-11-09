# docker

容器编排与本地开发环境。

## 服务清单

| 服务 | profile | 内存 limit | 端口 | 说明 |
| --- | --- | --- | --- | --- |
| `mysql` | `source` | 768 MB | 3306 | 业务库，开启 binlog（`ROW` + `FULL`） |
| `kafka` | `source` | 768 MB | **29092**（宿主）/ 9092（容器内） | KRaft 单节点，免 ZooKeeper |
| `flink-jobmanager` | `compute` | 768 MB | 8081 | Web UI |
| `flink-taskmanager` | `compute` | 3.25 GB | — | **4 slots**，taskHeap 1489MB / managed 512MB |
| `doris-fe` | `olap` | 1.5 GB | 8030 / 9030 | Web UI / MySQL 协议端口 |
| `doris-be` | `olap` | 1.75 GB | 8040 | 单 BE，`replication_num=1`，活跃数据仅 400MB |

合计约 **8.75 GB**，WSL 上限 10GB（`docs/04` §4），余量约 1.2GB 留给内核与 dockerd。

> TM 与 BE 在 2026-11-04 做过一次**等量交换**：TM 2.5 → 3.25GB（07 的 join 状态要 134MB，
> 217MB/slot 不够），BE 2.5 → 1.75GB，一进一出正好 0.75GB，合计仍是 8.75GB。

> **slot 数不是"能跑几个作业"的开关，是内存切分粒度。** TM 的 taskHeap 按 slot 数均分：
> 10 个 slot 时每个只有 86.8MB（P3 跑 10 个单流作业正合适），4 个时每个 372MB（P4 跑
> 6 表 join 需要）。改这个数字比改内存总量更对症，且不用重启 Doris / MySQL。

`kafka-ui`（256 MB / 8080）暂不启用：用 `kafka-console-consumer.sh` 查积压即可，省一个容器。

### Kafka 为什么有两个端口

Kafka 返回给客户端的是 `advertised.listeners`，客户端拿到后会**再连一次**。所以两边必须各有一个：

| 客户端位置 | 使用地址 | 对应配置 |
| --- | --- | --- |
| 宿主机（Python 生成器） | `localhost:29092` | `EXTERNAL`，映射到宿主机 |
| 容器内（Flink 作业） | `kafka:9092` | `PLAINTEXT`，只在 docker 网络内 |

只配一个的典型症状：producer 显示连上，但随后一直超时，日志里是 `Connection to node -1 could not be established`。9092 刻意不映射到宿主机，避免误连到错误地址。

## 启动顺序

```bash
docker compose --profile source  up -d    # 先起数据源
docker compose --profile compute up -d    # 再起计算
docker compose --profile olap    up -d    # 最后起 Doris
```

分三批启动而不是一次全起的理由：Doris BE 启动阶段会预分配大量内存，与 Flink TM 同时拉起容易触发 OOM Killer。

## 目录约定

```
docker/
├── docker-compose.yml
├── docker-compose.finebi.yml   # 可选：FineBI 分析模式（独立项目名 rt-bi，与主栈二选一）
├── .env.example          # 端口、密码、内存 limit 等可覆盖变量（.env 本身不入库）
├── volumes/              # 容器数据目录：mysql/、kafka/、doris-fe/、doris-be/（整目录不入库）
├── flink/
│   └── lib/              # 四个 connector jar，由 scripts/fetch-flink-jars.sh 下载（不入库）
└── be-conf/
    └── be.conf           # Doris BE 的配置覆盖，只读挂到 /opt/apache-doris/be/conf/be.conf
```

> **`docker-compose.finebi.yml` 为什么不写进主 compose**：它开三个容器
> （web + engine-master + engine-worker），web 的 JVM 自检要求
> `Xmx + Direct + Metaspace + CodeCache ≤ 容器内存`，厂商那四项加起来 7680MB，
> 所以 web 的 `mem_limit` 下限就是 7808MB；worker 因为 polars 的分账模型
> `Xmx ≥ 1883MB`，限额也顶到 3584MB。三项合计已超过 VM 的一半，
> 而主栈 6 个容器占了 8960MB / 9947MB，两套加起来远超 VM。所以它是一份独立的
> compose，用独立的项目名 `rt-bi`（否则两边互相报 `orphan containers`），
> 共用 `rt-net` 网络（`external: true`）才能用 `doris-fe:9030` 这个服务名接数据源。
>
> 三个容器的启动有顺序：**master 先 healthy，worker 才注册得上去**。
> worker 一启动就连 `master:8000`，连不上它的 monitor 直接 `exit 66` 不重试。
> 用 `scripts/switch-mode.sh bi` 会自动按顺序起并逐个等就绪。
>
> 另外：web 容器里的 **polars jar 是被移除的**（`WEB-INF/lib.disabled/`）。
> `bi-web` 绑定 Java 8（class 52），而 polars 的实现类（包括"进程内迷你集群"模式
> 用的 `BIWorkerModule`）全是 class 55。放回去 web 会在
> `LocalMiniProcessStart` 处 `ClassNotFoundException` 并 shutdown。
> **卷重建后要重新移除一次**，命令见 `docs/note/27` §3.2。
>
> 这意味着 master/worker 的集群是真的在跑，但 web 用不上它 —— 部署向导会一直停在
> 「引擎」这一步报 `BI-Design_Engine_Error`。要用 FineBI 界面的只能走官网单机版安装包。
>
> 切换用 `bash scripts/switch-mode.sh bi|realtime`。实测数据和选型依据见
> [`docs/note/27-FineBI-与内存预算.md`](../docs/note/27-FineBI-与内存预算.md) §2.11。

> **`be-conf/` 是唯一进 git 的配置目录**，因为它改的是运行行为而不是运行数据
> （`volumes/` 整个被忽略）。目前里面只有一处改动：`trash_file_expire_time_sec = 3600`。
>
> 为什么直接挂 `be.conf` 而不走官方的 `CONFIGMAP_MOUNT_PATH`：这个镜像里有两个入口脚本，
> PID 1 实际跑的是 `/usr/local/bin/entry_point.sh`，它**不读那个变量**；有这个函数的是
> `/opt/apache-doris/be_entrypoint.sh`，但没被调用。设了 configmap 会静默失效 ——
> 容器起来了、配置却还是默认值，日志里连一句 skip 都没有。
>
> 只读挂载是安全的：`entry_point.sh` 全程不写 `be.conf`，`start_be.sh` 也只读。

> **Flink 的 checkpoint 不在 `docker/volumes/` 下**，而是仓库根的 `flink/checkpoints/`
> （compose 里写的是 `../flink/checkpoints:/opt/flink/checkpoints`）。原因是它属于
> 计算状态而非容器数据，跟 `flink/sql/` 放一起更顺；已在 `.gitignore` 排除。

建表 SQL 不放在 `docker/` 下，而是仓库根的 `sql/mysql/`：它同时被 compose 挂进容器（首次启动自动执行）和手工导入流程使用，放在通用位置才不用两头维护。`docker/mysql/init/` 这个方案已经弃用。

> `flink/lib/` 里的 jar **必须逐个文件挂载**，不能挂整个目录。挂目录会用宿主机的空目录
> 覆盖镜像自带的 `/opt/flink/lib`（里面有 flink-dist），结果是集群起来就
> `ClassNotFoundException: TaskManagerRunner`，看起来像镜像坏了。
> 反向的坑同样致命：如果挂载的**宿主机路径写错**，Docker 会静默创建一个空目录，
> 容器里那个 jar 变成 `drwxrwxrwx` 的目录，报 `Could not find any factory`。

## 数据流

```
                    ┌─ mysql:3306 ──binlog(ROW/FULL)──┐
Olist CSV ──load──▶ │                                 │──▶ Flink CDC ──▶ Doris ODS
                    └─ kafka:29092 ──user_event───────┘
                          ▲
      scripts/gen_user_events.py（合成埋点，3 分区，key=session_id）
```

## 当前状态（2026-11-02）

- [x] `docker-compose.yml`（六服务，三 profile，固定 IP 网段 172.28.0.0/24）
- [x] `.env.example`
- [x] WSL2 `.wslconfig` 内存上限（`memory=10GB`，见 `docs/04-环境搭建.md` §4）
- [x] `flink-jobmanager` / `flink-taskmanager`（1 TM / **4 slots**，四个 jar 挂载）
- [x] `doris-fe` / `doris-be`（BE 30 秒内注册 Alive）
- [x] 六容器全部 healthy
- [x] P3 阶段 10 个作业同时 RUNNING（9 CDC + 1 埋点，占满当时的 10 slots）
- [x] P4 阶段改为 2 个作业常驻（`dwd_trade_order_detail` + `dwd_user_event_clean`），
      腾出的 slot 换成每 slot 372MB 内存 —— 详见 `flink/README.md`「关键配置」

**内存是这个文件里最不能动的参数**。改任何 `mem_limit` 都要回到 `docs/04 §4` 重算合计，
六项之和必须压在 8.75GB 以内——超过 10GB 会触发 WSL 的 OOM Killer，而被杀的往往不是
吃内存最多的那个进程，现象是「Kafka 无故掉线」。
