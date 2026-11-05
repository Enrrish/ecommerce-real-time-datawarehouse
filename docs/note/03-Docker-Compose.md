# 03 · Docker Compose

> 一句话：Compose 是**声明式的多容器拓扑**。你描述"要有哪些服务、它们怎么连、各占多少资源"，
> 它负责算出该创建/重建/启动哪些容器。它不是启动脚本，是状态收敛器。

---

## 一、通用知识

### 1.1 文件骨架

```yaml
name: realtime                  # 项目名，决定容器/网络前缀（默认取目录名）

services:
  mysql:
    image: mysql:8.0.36
    container_name: rt-mysql    # 显式命名，避免 realtime-mysql-1 这种带序号的名字
    command: ["--binlog-format=ROW"]
    environment:
      MYSQL_ROOT_PASSWORD: root123
      TZ: Asia/Shanghai
    ports: ["3306:3306"]        # 宿主:容器
    volumes:
      - ./volumes/mysql:/var/lib/mysql
    networks: [realtime-net]
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h127.0.0.1"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 30s
    restart: "no"

volumes:
  rt-doris-be-data:             # 命名卷，声明在这里
    name: rt-doris-be-data      # 固定名字，避免 <项目>_<键> 前缀

networks:
  realtime-net:
    name: realtime-net
    ipam:
      config:
        - subnet: 172.28.0.0/16
```

### 1.2 healthcheck 各字段的真实含义

| 字段 | 含义 | 易错点 |
| --- | --- | --- |
| `test` | 退出码 0 = healthy | `CMD` 不经过 shell；要管道就用 `CMD-SHELL` |
| `interval` | 间隔 | 从容器启动就开始算 |
| `timeout` | 单次超时 | 超时算一次失败 |
| `retries` | 连续失败几次判 unhealthy | 不是总次数 |
| `start_period` | 启动宽限期，**期间失败不计入 retries** | 长启动服务必须给，否则前几次失败就把状态打成 unhealthy |

`depends_on` 有两种：

```yaml
depends_on:
  mysql:
    condition: service_healthy    # 等 healthcheck 通过（推荐）
# 或简写
depends_on: [mysql]               # 只等"容器启动"，不等"服务可用"
```

简写形式几乎没用 —— 容器 `Created` 了但 MySQL 还在初始化，下游照样连不上。

### 1.3 profiles：按需启停

```yaml
services:
  mysql:
    profiles: ["source"]
  doris-fe:
    profiles: ["olap"]
```

```bash
docker compose --profile olap up -d doris-be     # 只起 olap 组里的 be
docker compose --profile source --profile compute --profile olap up -d   # 全起
docker compose up -d                              # 不带 profile 的服务（通常没有）
```

用途：重建 Doris 时不必把 Flink、MySQL 一起卷进来。本项目 BE 改了三次配置，
每次都是 `--profile olap up -d doris-be`，其他五个容器纹丝不动。

### 1.4 资源限制写在哪个字段

```yaml
    mem_limit: 1792m            # ← 非 Swarm 模式用这个（compose 会翻译成 --memory）
    cpus: 2.0
    # deploy:                   # ← Swarm 专用；docker compose 在 2.x 之后部分支持，
    #   resources:              #    但历史上会被静默忽略，别用
    #     limits: {memory: 1g}
```

判断有没有生效**不要看 YAML，看容器**：

```bash
docker inspect rt-doris-be --format '{{.HostConfig.Memory}}'
```

本项目踩过一次：只改了 YAML，以为生效了，实际容器还是旧的 cgroup 值 ——
`docker compose up -d` 在镜像和配置都没变时**不会重建容器**。

### 1.5 `up` 什么时候才会重建容器

`docker compose up -d` 只在"期望状态与现状不一致"时重建：

| 变了什么 | 会重建吗 |
| --- | --- |
| `image` 标签 | 会（且会先 pull） |
| `environment` / `command` / `volumes` / `mem_limit` | 会 |
| 只是宿主上的挂载文件内容变了（如 `be.conf`） | **不会**（容器内看到新内容，但进程不重启） |
| `docker-compose.yml` 的注释 | 不会 |

所以改完配置要确认：

```bash
docker compose config --quiet          # 语法校验，无输出=OK
docker compose up -d <service>         # 必要时先 rm -f 再 up
docker inspect <容器> --format '{{.State.StartedAt}} {{.HostConfig.Memory}}'
```

### 1.6 `stop` / `down` / `down -v` 的破坏面

| 命令 | 容器 | 网络 | 命名卷 | bind mount |
| --- | --- | --- | --- | --- |
| `compose stop` | 保留（Exited） | 保留 | 保留 | 保留 |
| `compose down` | **删除** | 删除 | 保留 | 保留 |
| `compose down -v` | 删除 | 删除 | **删除** | 保留 |

`down -v` 的 `-v` 只针对**命名卷**。这条在本项目里是决定性的（见 2.3）。

---

## 二、在本项目中

### 2.1 三个 profile 对应三层依赖

```yaml
mysql / kafka          → profiles: ["source"]     # 数据源
flink-jm / flink-tm    → profiles: ["compute"]    # 计算
doris-fe / doris-be    → profiles: ["olap"]       # 存储
```

`scripts/demo.sh` 第 1 步就是三条 profile 全带上：

```bash
docker compose --profile source --profile compute --profile olap up -d
```

`depends_on` 只在同一次 `up` 里生效。本项目的实际写法：

```yaml
flink-jobmanager:
  # 无 depends_on
flink-taskmanager:
  depends_on:
    - flink-jobmanager          # 简写 = 只等容器启动，不等 healthcheck
doris-be:
  depends_on:
    doris-fe:
      condition: service_healthy
```

`flink-tm` 用简写而不是 `service_healthy`：TM 只需要 JM 的 RPC 端口（6123）通，
而 JM 的 healthcheck 探的是 REST 8081 —— 多等几秒没有收益，TM 自己会重试注册。
`doris-be` 则必须等 FE 真的 healthy，否则 BE 拿不到 FE 地址会直接退出。

### 2.2 网络与固定 IP

自定义网络 `rt-net`，网段 `172.28.0.0/16`，六个服务全部指定 `ipv4_address`：

| 容器 | IP | 为什么固定 |
| --- | --- | --- |
| rt-mysql | 172.28.0.12 | Flink CDC 的 `hostname` 写服务名也行，但排障时 IP 更直观 |
| rt-kafka | 172.28.0.13 | 同上 |
| rt-doris-fe / be | .10 / .11 | **BE 向 FE 注册自己的 IP**，变了就掉线 |
| rt-flink-jm / tm | .20 / .21 | TM 注册到 JM 用的是 IP |

Doris 最挑：`FE_SERVERS=fe1:172.28.0.10:9010`、`BE_ADDR=172.28.0.11:9050` 都写死在
compose 里。用服务名也能通，但 `SHOW BACKENDS` 显示的地址会和 `/api/health` 报的不一致，
排查时容易绕。

### 2.3 六份 healthcheck，各自在测什么

| 服务 | test | 为什么这么写 |
| --- | --- | --- |
| mysql | `mysqladmin ping -h127.0.0.1` | 不走 socket，验证 TCP 也在听 |
| kafka | `kafka-broker-api-versions.sh --bootstrap-server localhost:9092` | KRaft 模式下没有 zk，只能问 broker 自己 |
| doris-fe | `curl -sf http://localhost:8030/api/bootstrap` | FE 的 HTTP 端口，比 9030 的 MySQL 协议更早可用 |
| doris-be | `curl -sf http://localhost:8040/api/health` | BE 的 HTTP 健康检查 |
| flink-jm | `curl -fsS http://localhost:8081/overview` | REST 端口起来 = JM 可用 |
| flink-tm | 无 | 镜像里没有探测 TM 就绪的轻量命令；TM 是否就绪由 JM 的 `slots-total` 间接反映 |

`start_period` 给得比较宽（Doris FE 60s、BE 45s）：冷启动时 JVM + 元数据加载确实慢，
给窄了会在第一次就判 unhealthy，`depends_on: service_healthy` 的下游直接起不来。

### 2.4 只读挂载 be.conf

```yaml
  doris-be:
    volumes:
      - rt-doris-be-data:/opt/apache-doris/be/storage
      - ./be-conf/be.conf:/opt/apache-doris/be/conf/be.conf:ro
      - ./volumes/doris-be/log:/opt/apache-doris/be/log
```

为什么不用官方的 `CONFIGMAP_MOUNT_PATH`：试过，**静默失效**。镜像里有两个入口脚本，
PID 1 实际跑的是 `/usr/local/bin/entry_point.sh`，它只从 `FE_SERVERS` / `BE_ADDR`
推配置，不读那个变量；有 `update_conf_from_configmap()` 的是
`/opt/apache-doris/be_entrypoint.sh`，没被调用。日志里连一句 skip 都没有。

直接挂 `be.conf` 是唯一可靠的方式，前提是确认进程不写它（`start_be.sh` 只读）。

### 2.5 内存用环境变量留出调节余地

```yaml
      taskmanager.memory.process.size: 3072m
    mem_limit: ${FLINK_TM_MEM_LIMIT:-3328m}
```

`${VAR:-default}` 是 compose 的默认值语法，从 shell 环境或 `.env` 读。
调 TM 内存时不用改文件：

```bash
FLINK_TM_MEM_LIMIT=3840m docker compose --profile compute up -d flink-tm
```

（实际项目里没这么用过 —— 因为 TM 内存调一次就定案了，加变量是为了让"限制"这个
概念显式。这是个小设计：默认值写在 YAML 里，覆盖方式在文档里，读的人不用翻 `.env`。）

### 2.6 踩过的坑

**坑 1：`stop_grace_period` 只对新建的容器生效**

```yaml
    stop_grace_period: 60s
```

这个值被写进容器的 `Config.StopTimeout`，**在创建时固化**。改了 YAML 但容器是旧的，
`docker inspect --format '{{.Config.StopTimeout}}'` 仍然是 `<nil>` —— 于是走默认 10 秒。

现象：`docker compose stop` 之后 mysql / kafka / be 的退出码全是 **137**（被 SIGKILL），
不是 143。数据没坏（InnoDB / Kafka 都能从日志恢复），但每次都要多等一轮启动检查。

结论：**别指望 compose 的宽限期**。`scripts/stop.sh` 改成逐个
`docker stop -t <秒数>`，显式传参，不受容器创建时间影响。

**坑 2：`docker compose down -v` 在这是不可逆操作**

因为只有 BE 用命名卷，`-v` 删的恰好是 Doris 的整个 storage：

- ODS 10 张表、DWD 7 张、DWS 6 张、ADS 7 张全部消失
- 恢复要重跑 41 万行 CDC 快照（1~2 分钟）+ 9 张 ODS 全量（更久）+ 所有验收数字重对

`scripts/stop.sh` 的三个分支**都不带 `-v`**，`--purge` 也只是 `compose down`，
而且执行前会问一句。这不是过度谨慎 —— 它是唯一一个"一条命令毁掉两天工作"的操作。

**坑 3：`config` 输出里的 `${...}` 已经展开**

```bash
docker compose config | grep -A2 mem_limit     # 看到的是展开后的值，不是 ${VAR}
docker compose config --quiet                  # 只校验，无输出
```

排查"为什么内存限制不对"时，先 `config` 看 compose **算出来**是什么，再看
`docker inspect` 看容器**实际**是什么。两者不一致 = 容器没重建。

---

## 三、速查

```bash
cd docker

# 校验
docker compose --profile source --profile compute --profile olap config --quiet

# 全起 / 单起
docker compose --profile source --profile compute --profile olap up -d
docker compose --profile olap up -d doris-be

# 看状态与健康
docker compose ps
docker inspect rt-doris-be --format '{{.State.Health.Status}} {{.HostConfig.Memory}} {{.Config.StopTimeout}}'

# 停（不带 -v！）
docker compose stop
```

| 现象 | 原因 |
| --- | --- |
| 改了 `mem_limit` 但 `docker inspect` 没变 | 容器没重建，`up -d` 认为无变化 |
| `depends_on: service_healthy` 卡住 | 上游 `start_period` 太短，或 healthcheck 命令在镜像里不存在 |
| 停容器退出码 137 | `StopTimeout` 是 `<nil>`（旧容器），显式 `docker stop -t N` |
| 挂载的 jar 在容器里变成目录 | bind mount 源路径不存在，Docker 静默建了空目录 |

延伸：`02-Docker.md`（单容器视角）、`01-WSL2.md`（宿主资源）、`../04-环境搭建.md` §4
