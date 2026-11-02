# docker

容器编排与本地开发环境。

## 服务清单

| 服务 | profile | 内存 limit | 端口 | 说明 |
| --- | --- | --- | --- | --- |
| `mysql` | `source` | 768 MB | 3306 | 业务库，开启 binlog（`ROW` + `FULL`） |
| `kafka` | `source` | 768 MB | **29092**（宿主）/ 9092（容器内） | KRaft 单节点，免 ZooKeeper |
| `flink-jobmanager` | `compute` | 768 MB | 8081 | Web UI |
| `flink-taskmanager` | `compute` | 2.5 GB | — | 10 slots（9 CDC + 1 埋点），RocksDB |
| `doris-fe` | `olap` | 1.5 GB | 8030 / 9030 | Web UI / MySQL 协议端口 |
| `doris-be` | `olap` | 2.5 GB | 8040 | 单 BE，`replication_num=1` |

合计约 **8.75 GB**，WSL 上限 10GB（`docs/04` §4），余量约 1GB 留给内核与 dockerd。

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
├── .env.example          # 端口、密码等可覆盖变量（.env 本身不入库）
├── volumes/              # 数据目录：mysql/、kafka/（整目录不入库）
├── flink/conf/           # flink-conf.yaml 覆盖项（待建）
└── doris/                # FE/BE 自定义配置（待建）
```

建表 SQL 不放在 `docker/` 下，而是仓库根的 `sql/mysql/`：它同时被 compose 挂进容器（首次启动自动执行）和手工导入流程使用，放在通用位置才不用两头维护。`docker/mysql/init/` 这个方案已经弃用。

## 数据流

```
                    ┌─ mysql:3306 ──binlog(ROW/FULL)──┐
Olist CSV ──load──▶ │                                 │──▶ Flink CDC ──▶ Doris ODS
                    └─ kafka:29092 ──user_event───────┘
                          ▲
      scripts/gen_user_events.py（合成埋点，3 分区，key=session_id）
```

## 待补充

- [x] `docker-compose.yml`（mysql + kafka 已实测跑通）
- [x] `.env.example`
- [x] WSL2 `.wslconfig` 内存上限（`memory=10GB`，见 `docs/04-环境搭建.md` §4）
- [ ] `flink-jobmanager` / `flink-taskmanager` 服务定义（P3）
- [ ] `doris-fe` / `doris-be` 服务定义（P3）
