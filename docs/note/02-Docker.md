# 02 · Docker

> 一句话：镜像是**只读的层叠文件系统**，容器是它上面加一层可写层，卷是**绕过可写层**的
> 持久化目录。理解这三句话，90% 的"数据怎么没了"就有答案。

---

## 一、通用知识

### 1.1 四个对象

| 对象 | 是什么 | 生命周期 |
| --- | --- | --- |
| **镜像 image** | 只读层叠（layer），由 `Dockerfile` 构建或从仓库拉 | 除非 `rmi`，一直在 |
| **容器 container** | 镜像 + 可写层 + 一组 namespace/cgroup + 一个主进程（PID 1） | **主进程退出 = 容器停止** |
| **卷 volume** | 由 Docker 管理的目录，通常在 `/var/lib/docker/volumes/` | 删容器不删卷，`down -v` / `volume rm` 才删 |
| **网络 network** | 自定义 bridge，容器间用**服务名**做 DNS | `down` 会删自定义网络 |

关键：**容器的可写层随容器删除而消失**。所以任何需要留下的数据必须落在
卷或 bind mount 上，否则 `docker rm` 那一刻就没了。

### 1.2 bind mount vs 命名卷

```bash
-v /宿主机/路径:/容器/路径          # bind mount：宿主机目录直接映射进去
-v myvolume:/容器/路径              # 命名卷：Docker 管理，位置由 Docker 决定
```

| | bind mount | 命名卷 |
| --- | --- | --- |
| 位置 | 你指定的路径 | Docker 自己的目录（Linux 上是 ext4） |
| 性能 | 取决于宿主文件系统（**9p 极慢**） | 通常最好 |
| 备份 | 直接拷目录 | 得 `docker run --rm -v vol:/x alpine tar` |
| 跨平台 | Windows 路径语义差异多 | 一致 |
| 权限/属主 | 容易 UID/GID 不匹配 | Docker 初始化时按镜像设置 |

**Docker 对不存在的 bind mount 源会静默创建一个空目录**（不报错）。这条害过本项目：
`flink/lib/xxx.jar` 路径写错一个字母，容器里那个"jar"变成一个目录，Flink 报
`Could not find any factory for identifier 'mysql-cdc'` —— 报错完全不提路径。

### 1.3 退出码

| 码 | 含义 | 常见原因 |
| --- | --- | --- |
| 0 | 正常退出 | 一次性任务跑完 |
| 1 | 应用报错 | 脚本 `exit 1` |
| **125** | docker 自己出错 | 参数错、镜像不存在 |
| **126** | 命令不可执行 | 没有 `+x` 权限 |
| **127** | 命令找不到 | entrypoint 里的命令不存在；**也可能是被 kill 后拿不到真实码** |
| **137** | 128 + 9 = 被 SIGKILL | **cgroup OOM** 或宽限期到了被硬杀 |
| **143** | 128 + 15 = 被 SIGTERM | 优雅停止（正常） |

看到 137 先别归因 OOM：`docker inspect --format '{{.State.OOMKilled}}'` 说不是 OOM 的话，
就是 `docker stop` 的宽限期到了。本项目 BE 每次都是 137 而不是 OOM —— 因为 Doris BE
关闭本身就慢，10 秒宽限期不够（见 `03` 的 `stop_grace_period`）。

### 1.4 内存限制是 cgroup，不是"建议"

```bash
docker run --memory 2g --memory-swap 2g ...
docker inspect --format '{{.HostConfig.Memory}}' <容器>     # 字节数
```

三条容易误解的规则：

1. **`--memory` 是硬上限**。超了是内核直接 SIGKILL 容器里的进程，不是"变慢"。
2. **改限制要重建容器**。`docker update --memory` 对运行中的容器**不能降低**（能调的
   只有少数项），实际项目里等于无效。改 compose 后 `up -d` 会重建。
3. **容器内的 JVM/服务不一定知道这个限制**。Java 8u191+ 会读 cgroup，但很多服务
   （包括 Doris BE）要自己按比例算预算 —— 它们看到的是 cgroup 值，再乘一个系数。

`docker stats` 里的 `MEM USAGE / LIMIT` 是**当前用量 / 限制**。像 Doris 这种会把内存
吃到接近限制的服务，长期显示 85% 是**正常**的，不是泄漏。要看的是它有没有
`OOMKilled`、日志里有没有 `memory not enough`。

### 1.5 常用命令（按用途分组）

```bash
# 看
docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.RunningFor}}'
docker inspect <容器> --format '{{.State.ExitCode}} {{.State.OOMKilled}} {{.RestartCount}}'
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}'
docker logs --tail 200 -f <容器>
docker events --since 2h --filter 'type=container' \
  --format '{{.Time}} {{.Action}} {{.Actor.Attributes.name}}'

# 进
docker exec -it <容器> sh
docker exec <容器> ls -la /path

# 拷（不经过宿主机路径参数，绕开 MSYS 改写）
docker cp ./file.sql rt-flink-jm:/tmp/job.sql

# 卷
docker volume ls
docker volume inspect <卷> --format '{{.Mountpoint}} {{.CreatedAt}}'
docker run --rm -v <卷>:/d alpine du -sh /d        # 量体积

# 清理（危险等级递增）
docker container prune          # 删所有已停止容器
docker image prune -a           # 删所有未被使用的镜像
docker volume prune             # 删所有未被使用的卷  ← 会删数据
docker compose down -v          # 删容器 + 网络 + **卷**
```

`docker exec` 有固定开销（本项目实测 **826ms/次**，其中大部分是 exec 握手 + shell 启动）。
所以不能在采样循环里每次 `exec` 一次 —— 详见 [23](23-HTTP-服务与大屏后端.md) 的常驻管道。

### 1.6 只读挂载

```yaml
volumes:
  - ./be.conf:/opt/apache-doris/be/conf/be.conf:ro
```

`:ro` 让容器内不能改。这既是安全边界，也是**契约**：配置文件只能来自仓库，
容器里改了也不该生效（否则"我这里能跑"就说不清了）。

前提是确认进程不会写它。本项目验证方式：`grep -nE ">>|sed -i|mv" start_be.sh` 为空，
且 `start_be.sh` 对 `be.conf` 只做 `done < be.conf` 和 `grep`。

---

## 二、在本项目中

### 2.1 六个容器与固定 IP

自定义网络 `realtime-net`，网段 `172.28.0.0/16`，全部指定 `ipv4_address`：

| 容器 | IP | 为什么固定 |
| --- | --- | --- |
| rt-mysql | 172.28.0.12 | Flink CDDL 的 `hostname` 可以写服务名，但 Doris BE 注册要用 IP |
| rt-kafka | 172.28.0.13 | 同上 |
| rt-doris-fe / be | .10 / .11 | **BE 向 FE 注册自己的 IP**，变了就掉线 |
| rt-flink-jm / tm | .20 / .21 | TM 注册到 JM 用的是 IP |

Doris 是最挑的一个：`FE_SERVERS=fe1:172.28.0.10:9010`、`BE_ADDR=172.28.0.11:9050`
都写死在 compose 里。用服务名也能通，但 `SHOW BACKENDS` 里显示的地址会和
`/api/health` 里报的不一致，排查时容易绕。

### 2.2 只有 Doris BE 用命名卷

```yaml
volumes:
  - rt-doris-be-data:/opt/apache-doris/be/storage   # 命名卷
  - ./volumes/doris-be/log:/opt/apache-doris/be/log # bind mount（日志方便翻）
```

理由见 `01`：BE 会对 `storage` 做 `statvfs`，bind mount 到 9p 会拿到假 inode 数。
其余组件：

- MySQL：`./volumes/mysql:/var/lib/mysql`（bind mount，能用资源管理器直接看）
- Kafka：`./volumes/kafka:/var/lib/kafka/data`
- Flink checkpoint：`./flink/checkpoints:/opt/flink/checkpoints`

**这个不对称直接决定了 `docker compose down -v` 的破坏面**：`-v` 只删命名卷，
所以它删的恰好是 Doris 那一层（ODS 10 + DWD 7 + DWS 6 + ADS 7 张表），
MySQL / Kafka 的 bind mount 反而活着。

### 2.3 用 `docker cp` 提交作业，而不是挂载

`scripts/submit-sql.sh` 的做法：

```bash
docker cp "$SQL_FILE" rt-flink-jm:/tmp/job.sql
docker exec rt-flink-jm ./bin/sql-client.sh -f /tmp/job.sql
```

为什么不直接 `-v $(pwd)/flink:/opt/flink/sql` 或 `-f /宿主机/路径`：Git Bash（MSYS）
会把以 `/` 开头的参数改写成 `C:/Users/...`，到容器里变成
`Fail to read content from /opt/flink/C:/Users/.../job.sql`。
`MSYS_NO_PATHCONV=1` 能解决，但那要求每个调用点都记得加；`docker cp` 走 tar 流，
从根上绕开（详见 `05`）。

### 2.4 `docker events` 是关停排查的权威时间线

有一次后台任务报 exit 127，看起来像脚本崩了。用 `docker events` 一查：

```
17:33:01  kill  rt-flink-tm      → 143
17:33:03  kill  rt-flink-jm      → 143
17:33:50  kill  rt-doris-be      → 137
17:33:53  kill  rt-doris-fe      → 143
17:33:57  kill  rt-kafka         → 143
17:34:11  kill  rt-mysql         → 0
```

顺序正好是 `stop.sh` 的依赖倒序，时间点与用户手动执行吻合 —— 结论是"人停的，
不是故障"。`docker logs` 里看不到这种全局信息，日志只有单个容器视角。

### 2.5 踩过的坑

**坑 1：`docker inspect` 的 `RestartCount` 才是 OOM 的唯一证据**

Flink UI 上作业显示 `RESTARTING`，异常栈报 `NoResourceAvailableException`
（"Could not acquire the minimum required resources"）—— 看着像 JM 招不到资源，
实际是 **TM 被 cgroup 杀了**，JM 当然招不到。真正的死因在 `docker logs rt-flink-tm`，
判据是：

```bash
docker inspect rt-flink-tm --format 'RestartCount={{.RestartCount}} OOM={{.State.OOMKilled}}'
```

**坑 2：容器里没有 `gawk` 这个名字**

```bash
docker exec rt-kafka awk 'BEGIN{print mktime("2026 11 04 10 00 00")}'
# awk: not found      ← 有 /usr/bin/awk（就是 gawk），但没有 awk 这个命令名
docker exec rt-kafka /usr/bin/awk '...'
```

`mktime` / `strftime` 是 gawk 扩展，busybox awk 没有。Kafka 官方镜像基于
`apache/kafka`（rockylinux 系），要写全路径。

**坑 3：`TZ` 只影响显示，不影响数据**

compose 里给每个服务加了 `TZ: Asia/Shanghai`，它改的是容器内 `/etc/localtime`
与 `date` 的输出。**数据库里存的值由列类型和连接参数决定**，不受 `TZ` 影响 ——
MySQL 的 `DATETIME` 无时区，Flink 要单独设 `table.local-time-zone`
和 `server-time-zone`（见 [11](11-Flink-CDC.md)）。曾经因为只设了 `TZ` 而漏了 JDBC 的时区，
导致抽到 Doris 后所有时间整体偏 8 小时。

---

## 三、速查

```bash
# 六容器一览
docker ps --format '{{.Names}}\t{{.Status}}'
docker inspect rt-doris-be --format 'RestartCount={{.RestartCount}} OOM={{.State.OOMKilled}} Started={{.State.StartedAt}}'

# 内存限制（字节 → MB）
docker inspect rt-doris-be --format '{{.HostConfig.Memory}}' | awk '{printf "%d MB\n", $1/1048576}'

# 量命名卷体积
docker run --rm -v rt-doris-be-data:/d alpine du -sh /d

# 关停时间线
docker events --since 3h --until 0s --filter 'type=container' \
  --format '{{.Time}} {{.Action}} {{.Actor.Attributes.name}}'
```

| 退出码 | 先查什么 |
| --- | --- |
| 137 | `OOMKilled` 是 true → 加内存/降负载；false → 宽限期不够，`docker stop -t N` |
| 143 | 正常，SIGTERM 优雅退出 |
| 127 | 命令不存在；**或父进程组被杀导致拿不到真实码**，用 `docker events` 对时间线 |
| 125 | docker 参数错，不是容器里的问题 |

延伸：`01-WSL2.md`（宿主层）、`03-Docker-Compose.md`（编排层）、`../04-环境搭建.md` §6
