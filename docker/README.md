# docker

容器编排与本地开发环境。

## 服务清单

| 服务 | profile | 内存 limit | 端口 | 说明 |
| --- | --- | --- | --- | --- |
| `mysql` | `source` | 768 MB | 3306 | 业务库，开启 binlog（`ROW` + `FULL`） |
| `kafka` | `source` | 768 MB | 9092 | KRaft 单节点，免 ZooKeeper |
| `flink-jobmanager` | `compute` | 768 MB | 8081 | Web UI |
| `flink-taskmanager` | `compute` | 2 GB | — | 2 slots，RocksDB 状态后端 |
| `doris-fe` | `olap` | 1.5 GB | 8030 / 9030 | Web UI / MySQL 协议端口 |
| `doris-be` | `olap` | 3 GB | 8040 | 单 BE，`replication_num=1` |

合计约 **8.8 GB**，WSL 上限 10GB（`docs/04` §4），余量约 1.2GB 留给内核与 dockerd。

`kafka-ui`（256 MB / 8080）暂不启用：用 `docker exec kafka kafka-console-consumer.sh` 查积压即可，省一个容器。

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
├── mysql/init/           # 容器首次启动自动执行的建表与初始化 SQL
├── flink/conf/           # flink-conf.yaml 覆盖项
└── doris/                # FE/BE 自定义配置
```

## 待补充

- [ ] `docker-compose.yml`
- [ ] `.env.example`
- [x] WSL2 `.wslconfig` 内存上限（`memory=10GB`，见 `docs/04-环境搭建.md` §4）
