# 27 · FineBI 与内存预算

> 这篇记录一次真实的技术选型：拿到一个 13GB 的「FineBI 离线部署包」，要判断这台 16GB 的机器能不能跑，
> 最后怎么跑。结论分两段：
>
> - **引擎集群能跑**：master + worker 两个容器在这台机器上起来了、注册成功、内存还剩一半
> - **FineBI 本身用不了**：`bi-web` 是 Java 8，而 polars 引擎（包括它的"进程内迷你模式"）
>   全部是 Java 11 编译的类。web 要么没有引擎，要么加载失败直接崩，两条路都进不去系统
>
> 中途踩了四个坑，都在正文里留了完整复盘：
>
> 1. `docker logs` 会骗人 —— `BIDiskFull: proxied System.out and System.err` 之后真日志只在 `fanruan.log`
> 2. `bi-web` 单镜像**缺 357MB 的引擎实现包**，而 `polars-client`（141MB）里只有 RPC 桩
> 3. 补上 jar 之后撞到更硬的一层：**实现包是 class 55，`bi-web` 是 class 52**，换 JDK 又会崩另外三处
> 4. worker 的 `Xmx` 低于 2048 时，polars 的内存分账会算出负数直接崩（见 2.11）
>
> 第 2、3 条是这篇最贵的一课。最终做法是：**三容器的编排留着（引擎侧真的能跑），
> web 侧退回内置 spider 并接受部署向导过不去**——要用 FineBI 只能走官网单机版安装包。
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

`docker run --memory=7808m` 是 **cgroup 硬上限**，超了内核直接 SIGKILL，
退出码 **137**（128 + 9）。这和 JVM 自己抛 `OutOfMemoryError` 是两回事：

| 现象 | 退出码 | 日志 | 含义 |
| --- | --- | --- | --- |
| 超容器限额 | 137 | 日志戛然而止 | 被内核杀，JVM 来不及说话 |
| 堆不够 | 1 | `java.lang.OutOfMemoryError: Java heap space` | JVM 自己抛的 |
| Metaspace 不够 | 1 | `OutOfMemoryError: Metaspace` | 类加载太多 |
| **进程主动 exit** | **1** | **可能什么都没有** | 应用自己的逻辑 |

最后一行是这篇要重点讲的坑——见 §2.4。注意"可能什么都没有"通常不是真的没有，
而是**日志被应用重定向到了另一个文件**，`docker logs` 看不到而已。

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

### 2.3 只起 bi-web 行不行？——缺一个 341MB 的 jar，补了也不行

把 `bi70all` 和 `bi61all` 都 load 进来，各得到 4 个镜像。看 `bi-web` 里有什么：

```bash
$ docker run --rm --entrypoint sh bi-web:6.1-… -c 'ls /opt/polars; du -sh /opt/polars'
arthas
8.0K    /opt/polars          # 只有线程诊断工具，不是引擎

$ docker run --rm --entrypoint sh bi-engine-master:6.1-… -c 'du -sh /opt/polars'
32M     /opt/polars          # 启动脚本 + 日志配置，也没有引擎本体
```

引擎本体在 worker 镜像的 `WEB-INF/lib` 里。把两个镜像的 jar 列表做差集，结果干净得意外：

```bash
$ comm -13 web-jars.txt worker-jars.txt        # worker 有、web 没有
fine-bi-engine-polars-6.1.jar

$ comm -23 web-jars.txt worker-jars.txt | head
fine-accumulator-11.0.jar
fine-bi-adapter-6.1.jar
fine-bi-engine-polars-client-6.1.jar           # ← 关键就是它
fine-cbb-11.0.jar
...
```

**`worker 的 lib = web 的 lib − client 版 + engine 版`**，34 个 jar 只差这一个：

| jar | 体积 | 内容 |
| --- | --- | --- |
| `fine-bi-engine-polars-client-6.1.jar` | 134.9 MB | 9,287 个 polars 条目，**只有客户端 API** |
| `fine-bi-engine-polars-6.1.jar` | 340.8 MB | **175,922 个条目**，含整个 `com/fanruan/polars/bi/` 引擎包 |

抽查 `SessionProvider` / `BlockPipelineDataSource` / `Config$ServerMode` 三个类，engine 版里全有
—— 它是 client 版的**超集**，所以该**替换**而不是叠加。

`bi-web` 只拿到 client 版，但它启动时会走 `LocalMiniProcessStart`（在同一 JVM 里起 mini polars
集群），那条路径要反射加载 engine 版里的 `com.fanruan.polars.bi.BIWorkerModule`。找不到就抛
`ClassNotFoundException`，再触发全局 shutdown（详见 2.4 的日志）。

**修法**：把 worker 的 engine jar 换进 web 的 `WEB-INF/lib`。

```bash
# 1. 从 worker 镜像取 jar（341MB，约 3 秒）
docker create --name tmp-w bi-engine-worker:6.1-…
docker cp tmp-w:/usr/local/tomcat/webapps/webroot/WEB-INF/lib/fine-bi-engine-polars-6.1.jar .
docker rm tmp-w

# 2. 替换卷里的 client 版（保留 jar 总数 35 个不变）
docker run --rm -v "D:/…:/src:ro" -v rt-finebi-webroot:/w alpine sh -c '
  rm -f /w/WEB-INF/lib/fine-bi-engine-polars-client-6.1.jar
  cp /src/fine-bi-engine-polars-6.1.jar /w/WEB-INF/lib/
'
```

换完 `docker logs` 里 `polars mini cluster error` 从有到无，重启两次都是 25 秒 healthy。
**到这里我以为问题解决了。其实只是把第一层盖子揭开。**

> 这是发布包的结构问题，不是配置错误：引擎类只打在 worker 镜像里，而 `bi-web` 的
> `LocalMiniProcessStart` 写死了要加载它。官方推荐的四容器部署（web + master + worker + minio）
> 下引擎由 worker 提供，踩不到这条路；单容器属于非预期用法，所以厂商没覆盖到。

**补完 jar 的第一个坏消息**：容器能起、页面能开，但部署向导报 `BI_Design_Engine_Error`。
翻 `logs/fanruan.log`：

```
java.lang.UnsupportedClassVersionError: com/fanruan/polars/core/XPath
  has been compiled by a more recent version of the Java Runtime (class file version 55.0),
  this version of the Java Runtime only recognizes class file versions up to 52.0
	at com.finebi.activate.environment.checker.PolarsEnvironmentChecker.check(PolarsEnvironmentChecker.java:34)
```

- class file version **55.0** = Java 11
- 52.0 = Java 8

```bash
$ docker exec rt-finebi java -version
openjdk version "1.8.0_492"          # bi-web 是 Java 8

$ docker run --rm --entrypoint sh bi-engine-worker:6.1-… -c 'java -version'
openjdk version "11.0.31"            # engine 镜像才是 Java 11
```

**engine jar 是 Java 11 编译的，`bi-web` 跑在 Java 8 上。** 这不是打错包，
是厂商把运行时按 JDK 版本切开了 —— 接下来 2.4 会证明 `bi-web` 根本离不开 Java 8。

### 2.4 补了 jar 之后：Java 8 与 Java 11 是个死结

既然缺的是 Java 11，那就给它 Java 11。两个镜像的 `JAVA_HOME` 恰好都是
`/opt/java/openjdk`，路径完全一致，把 worker 的 JDK 拷成卷挂上去就能透明覆盖：

```bash
docker volume create rt-java11
docker run --rm --entrypoint sh -v rt-java11:/dest bi-engine-worker:6.1-… \
  -c 'cp -a /opt/java/openjdk/. /dest/'      # 268MB，约 6 秒
```

覆盖后 `java -version` 确实变成 `11.0.31`，然后**连踩三个坑，一个比一个深**：

**坑一：entrypoint 硬拼 Java 8 的 GC 参数**

```
Unrecognized VM option 'PrintGCDateStamps'
Error: Could not create the Java Virtual Machine.
```

来源是 `/docker-entrypoint.sh` 第 117 行：

```bash
JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation $FINE_OPTS"
JAVA_OPTS="$JAVA_OPTS -Xloggc:${GC_LOG_PATH}/gc.%t.log ..."
```

`PrintGCDateStamps` / `UseGCLogFileRotation` 在 JDK 9 就被移除了。加一句
`-XX:+IgnoreUnrecognizedVMOptions` 可以静默跳过它们，而 `-Xloggc` 和 `PrintGCDetails`
在 Java 11 里是 deprecated 别名，会自动映射成 `-Xlog:gc`：

```bash
$ java -XX:+IgnoreUnrecognizedVMOptions -XX:+PrintGCDateStamps -Xloggc:/tmp/gc.log -version
[warning][gc] -Xloggc is deprecated. Will use -Xlog:gc:/tmp/gc.log instead.
openjdk version "11.0.31"            # 能起
```

**坑二：FineAssist 是 JNI native agent，只认 Java 8**

JVM 起来后日志里刷 `Fine assist start failed`：

```
Caused by: java.io.IOException: Can not attach to current VM
	at com.fr.jvm.assist.FineAssist.<clinit>(FineAssist.java:62)
	at com.fr.decision.deployment.foundation.impl.AbstractFoundationChecker.checkAssist(...)
```

Java 9+ 默认禁止 VM 自附加（Java 8 允许）。看着像是加个
`-Djdk.attach.allowAttachSelf=true` 就完事 —— 我加了，然后**问题变得更糟**：

```
Caused by: java.lang.ClassFormatError: Incompatible magic value 2751307326
  in class file com/fr/plugin/manage/PluginManager
	at WebappClassLoaderBase.findClassInternal
→ LifecycleException: 无法启动组件 StandardContext[/webroot]
```

`magic value` 每次都不一样（第二次是 `50661129`，类名变成 `FinePluginBridge`），
这是**内存层面的随机污染**。查 `FineAssist` 的方法签名就明白了：

```java
private static final String[] NATIVE_LIBS;
private static native int forceGC();
public static native int signature();
```

它带 native 库（`.so`），而那个 `.so` 是按 Java 8 编译的。attach 失败时它是**安全降级**的，
一旦 attach 成功，native 层的不兼容就会踩坏数据。**所以 `allowAttachSelf` 不该加。**

**坑三：`.classx` —— 厂商的类加密格式**

去掉 `allowAttachSelf` 后 `StandardContext` 能起来了，但还有零星的 `ClassFormatError`。
去 jar 里翻：

```bash
$ jar tf fine-core-11.0.jar | grep FinePluginBridge
com/fr/plugin/bridge/FinePluginBridge.classx
com/fr/plugin/bridge/FinePluginBridge__1.classx
```

**后缀是 `.classx` 不是 `.class`** —— FineBI 有自己的一套类加密，运行时解密后再 defineClass。
Java 11 下这套封装失效，密文被直接当成 class 读，于是报 `Incompatible magic value`。

**这三个坑指向同一个结论**：`bi-web` 镜像是**绑定 Java 8** 的 —— 类解密、native agent、
启动脚本的 GC 参数，全都只为 Java 8 准备；而 polars 引擎是 Java 11 编译的。
**两者不可能塞进同一个 JVM。**

于是有了第四条路，也是最终采用的那条：**既然引擎在这个镜像里注定跑不起来，就别让它存在。**

```bash
# 把 polars 的 jar 整个移走，只留内置的 spider 引擎
docker run --rm -v rt-finebi-webroot:/w alpine sh -c '
  mkdir -p /w/WEB-INF/lib.disabled
  mv /w/WEB-INF/lib/fine-bi-engine-polars-6.1.jar /w/WEB-INF/lib.disabled/'
```

移走之后（`lib` 从 35 个变 34 个，`fine-bi-engine-spider-6.1.jar` 588KB 留着）：

| 指标 | 移走前（Java 11 + engine jar） | 移走后（Java 8 + 无 polars） |
| --- | --- | --- |
| 容器状态 | running / unhealthy | **running / healthy，25 秒** |
| `/system/health` | 400（`Fine module health recorder is null`） | **200** |
| `BIWorkerModule` 报错 | 0（被 Java 11 绕过） | **0** |
| `ClassFormatError` | 多次，随机 | **0** |
| 空载内存 | 1.55 GiB | 1.587 GiB |

> 这一步的代价：**polars 加速引擎用不了，只剩 spider 引擎**。
> 对 11 万行这个数据量没有实际影响；如果以后要做大数据量分析，正确做法是
> 按厂商设计起 `bi-engine-master` + `bi-engine-worker` 两个独立容器，
> 让它们用各自的 Java 11，而不是把 Java 11 塞进 web。

### 2.5 那次"静默退出"，真凶是缺类，不是参数

第一版 compose 起容器，20 秒后**退出码 1**，`docker logs` 停在：

```
2026/11/09 09:43:29 main INFO [com.finebi.cluster] [ClusterModule] cluster update activator start
BIDiskFull: proxied System.out and System.err
```

当时判断是"JVM 参数压得太狠"——因为换成厂商原串就起来了。**这个结论是错的。**

**误导点一**：`BIDiskFull: proxied System.out and System.err` 不是"磁盘满"，
是 FineBI 把 `System.out` / `System.err` **重定向到日志文件**的通知。从这一行之后
`docker logs` 就看不到应用日志了 —— **"日志断在 cluster start"是假象，不是退出点**。

**误导点二**：真日志在容器里的 `logs/fanruan.log`，不在 stdout：

```bash
docker cp rt-finebi:/usr/local/tomcat/logs D:/tmp/
tail -20 D:/tmp/logs/fanruan.log
```

```
ERROR [standard]  start polars mini cluster error. Please check error message
com.fr.invoke.ReflectException: java.lang.ClassNotFoundException: com.fanruan.polars.bi.BIWorkerModule
    at com.finebi.adapt.polars.impl.polars.mini.BIWorkerModule.registerInProcess(BIWorkerModule.java:14)
    at com.finebi.adapt.polars.impl.polars.LocalMiniProcessStart.start(LocalMiniProcessStart.java:114)
    at com.finebi.spider.impl.startup.PolarsModule.start(PolarsModule.java:163)
    at com.finebi.direct.impl.datasource.ExcelDataSourceEngineDriver.init(ExcelDataSourceEngineDriver.java:63)
    ...
CacheManager-GlobalShutdownHook INFO  CacheManager global JVM shutdown hook triggered (fallback cleanup)
```

**退出码 1、没有异常栈、`FineLog/` 里只有 gc/safepoint** —— 这三条当初让我认定
"进程自己 `exit(1)`，只能改配置二分"。实际是异常打在了**另一个文件**里：
`FineLog/` 只放 JVM 级日志（gc、safepoint、hs_err），业务日志在 `logs/` 根下。

**为什么会误判成参数问题**：这个异常抛在异步线程（`Thread-21`，经
`CompletableFuture.register` 的 `AsyncRun`），**触发时机和主线程启动速度赛跑**。
厂商原串那次恰好主线程先跑完，异常被吞掉，于是就"好"了。后来同一串厂商参数
连续失败三次 —— **是竞态，不是参数**。

**教训**：

| 曾经的判据 | 靠不住在哪 |
| --- | --- |
| `docker logs` 断在哪就是死在哪 | `System.out` 被应用重定向后，日志换文件了 |
| `FineLog/` 没有业务日志 | 那里只有 JVM 级日志，业务日志在 `logs/` 根 |
| `ExitCode=1` + 无异常栈 | 栈可能在另一个文件里 |
| "改了 X 就好了" | 有竞态时，一次成功不构成证据 |

**可靠的排查顺序**：`docker cp` 整个 `/usr/local/tomcat/logs` 出来，按修改时间找最新的
`*.log`，grep `ERROR|Exception`。`docker logs` 只覆盖 stdout 那一段，仅够看启动进度。

### 2.6 部署向导报「JVM参数配置存在问题」

容器起来、页面能开之后，`/webroot/decision/v10/deployment/recommendation` 会红字报：

> **检测到1项配置异常** — JVM参数配置存在问题
> 检测到 xmx + directMemory + metaspace + codecache > 容器/服务器分配的内存
> 请检查内存JVM参数设置是否合理，并确认系统剩余可用内存是否充足。

判据就是这四项之和 ≤ 容器可见内存（读的是 cgroup 限额，不是 `/proc/meminfo`）：

```
Xmx                   2560 MB
MaxDirectMemorySize   2048 MB
MaxMetaspaceSize      2048 MB   ← 厂商值，实测只用到 132 MB
ReservedCodeCacheSize 1024 MB   ← 厂商值，实测只用到 18 MB
─────────────────────────────
合计                  7680 MB   >  容器 5632 MB   → 报错
```

**第一次想的是"那就压小一点"**，压到 5376 时容器反而起不来（原因见 2.4，
是 polars 缺类的竞态，不是参数值本身）。2.3 补了 jar 之后理论上可以压了。

**但没有压，改的是容器限额。理由**：

- `MaxMetaspaceSize` / `ReservedCodeCacheSize` / `MaxDirectMemorySize` 都是**上限不是预占**，
  实测 Metaspace 只用 132 MB、CodeCache 只用 18 MB，压小它们省不出实际内存
- 压小会在数据量上来时撞 Metaspace OOM，那比"限额数字难看"严重得多
- 这几个值是厂商按 746 MB 的 `WEB-INF/lib` 算的，没有比它更懂的依据

```
7680 (四项和)  +  128 (Tomcat native / 线程栈 / mmap)  =  7808 MB
```

改 `mem_limit` 重启，红字消失。

**副作用要正视**：`bi` 模式的限额合计变成 `1536 + 1792 + 7808 = 11136 MB`，
比 VM 实见的 9947 MB 还高。这是**纸面超卖**，不是算错：
`mem_limit` 是 per-container 的保险丝而不是预占，实测三容器合计只用 3.7 GB，
VM 内 `MemAvailable` 还剩 5.6 GB。真实天花板由 `.wslconfig` 的 10 GB 兜底，
不由各容器 `mem_limit` 之和决定。

> 这个检查只看**四项配置的和**，不是看实际占用。所以"我明明只用 1.6GB"不构成申诉理由——
> 它防的是极端负载下 JVM 把承诺的上限全用光然后被 cgroup 打死。

### 2.7 内存测算

| 项 | 数值 | 来源 |
| --- | --- | --- |
| 物理内存 | 15.69 GB | `Win32_OperatingSystem` |
| WSL2 配额 | 10 GB | `.wslconfig` |
| Docker VM 实见 | **9947 MB** | `docker info` / `grep MemTotal /proc/meminfo` |
| 实时模式 6 容器限额 | **8960 MB** | `docker inspect .HostConfig.Memory` 求和 |
| 实时模式余量 | 987 MB | 9947 − 8960 |
| FineBI 空载实测 | **1587 MB** | `docker stats`（移走 polars jar 后，见 2.4） |
| FineBI 容器限额 | **7808 MB** | compose `mem_limit`，由 JVM 自检下限 7680 反推 |

**如果两套同时开**：8960 + 7808 = 16768MB > 9947MB，超 6821MB。**共存这条路是堵死的。**

**为什么 `bi` 是 7808 而不是和 `realtime` 一样的 8960**：8960 是 realtime 的自然结果
（6 个容器厂商推荐值相加），而 7808 是 FineBI 的**下限**——低于它 JVM 自检直接报红。
两个数接近但来路完全不同，别当成"同一档"。

### 2.8 最终方案：二选一，而不是共存

```
实时模式（realtime）           BI 分析模式（bi）
─────────────────             ─────────────────
mysql        768              doris-fe      1536
kafka        768              doris-be      1792
doris-fe    1536              finebi        7808
doris-be    1792              ────────────────────
flink-jm     768                       合计 11136
flink-tm    3328                          （超卖，见 2.7）
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

### 2.9 实测结果

BI 模式下三个容器的真实占用：

| 容器 | 实测 | 限额 | 使用率 |
| --- | --- | --- | --- |
| rt-doris-fe | 665 MiB | 1536 MiB | 43% |
| rt-doris-be | 1408 MiB | 1792 MiB | 80% |
| rt-finebi | 1587 MiB | 7808 MiB | 21% |
| **合计** | **3660 MiB** | 11136 MiB | — |

VM 内 `MemAvailable` 还有 **5.6 GiB**。限额离实际用量的距离，就是"纸面超卖"仍然安全的余量。

> 上表是**单容器阶段**的数字。补齐 master / worker 之后的五容器实测见 2.11：
> 合计 **4914 MiB**，限额 16768 MiB，余 5033 MiB。多两个容器只多吃 1.3 GB。

**这里必须分清两个口径** —— 混在一起讲就会自相矛盾：

| 口径 | 怎么算 | 数值 | 语义 |
| --- | --- | --- | --- |
| **实际占用** | `docker stats` 的 `MemUsage` 求和 | 3660 MB | 现在真的吃了多少 |
| **限额合计** | `docker inspect .HostConfig.Memory` 求和 | 11136 MB | 最多**允许**吃多少 |

Docker Desktop 的 `3.68GB / 9.49GB` 是前者，`switch-mode.sh` 早期版本报
`11136MB / 9947MB，余 -1189MB` 是后者。两个数都对，只是回答的不是同一个问题。

早期脚本只算限额，于是明明还剩 5.6GB，却报"超了 1189MB"—— 判断依据用错了地方。
现在脚本两个都报，主行是实际占用：

```
==> 内存
  实际占用 3660MB / VM 9947MB，余 6287MB
    限额合计 11136MB，超卖 1189MB —— 允许，mem_limit 是每容器保险丝不是预占
```

连通性验证：

```bash
$ docker exec rt-finebi getent hosts doris-fe
172.28.0.10     doris-fe
$ docker exec rt-finebi bash -c 'exec 3<>/dev/tcp/doris-fe/9030'
9030 可达
```

### 2.10 FineBI 接 Doris

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

### 2.11 补齐 master / worker：集群能起，但 web 接不上去

上一版只跑 bi-web 一个容器，被"缺 341MB 引擎 jar"卡住。既然那 357MB 的实现包在
`bi-engine-master` / `bi-engine-worker` 镜像里各自带着，正确做法不是往 web 里塞，
而是**按角色各起一个容器**，让 web 通过 RPC 去连。

#### 三个镜像其实是同一套基础层的三种剪裁

| 镜像 | 入口 | `WEB-INF/lib` | JDK |
| --- | --- | --- | --- |
| `bi-web` | `/docker-entrypoint.sh` + `catalina.sh run` | 37 个，含 polars-**client** 141MB | 8 |
| `bi-engine-master` | `/opt/polars/docker-entrypoint.sh` + `startup.sh` | 6 个，含 polars-**实现** 357MB | 11 |
| `bi-engine-worker` | 同上 | 同上（与 master 完全一致） | 11 |

master 和 worker 的 lib **完全一样**，角色靠 `APP_TYPE` 环境变量分支
（`startup.sh` 里 `if [ "$APP_TYPE" = "bi-engine-master" ]`）。
两个镜像的 Env 里还塞着全套 `RUNTIME_ENV_bi-engine-*` / `RUNTIME_CMD_bi-web=` ——
那是 finekey 编排产物，手写 compose 用不到。

#### 关键环境变量（`run_bi_master_slim.sh` / `run_bi_worker_slim.sh` 读的）

| 变量 | master | worker | 说明 |
| --- | --- | --- | --- |
| `APP_TYPE` | `bi-engine-master` | `bi-engine-worker` | 角色开关 |
| `POLARS_HOST` | `172.28.0.31` | `172.28.0.32` | **必须写容器 IP**，默认 127.0.0.1 会绑 loopback，别人连不上 |
| `POLARS_PORT` | 8000 | 9001 | worker 的 monitor 占 `port-1`（9000） |
| `ENV_MASTER_HOST` / `PORT` | — | `172.28.0.31` / 8000 | 单 master 模式的坐标 |
| `POLARS_HOME` | `/data/polars` | `/data/polars` | 元数据、conf、gc、dump |
| `ENV_POLARS_TIERS` | `sync`（默认） | `sync` | **不需要 MinIO**，见下 |
| `IS_CONTAINER` | `true` | `true` | `start.sh` 里 `exec $cmd` 前台运行；不设会 nohup 后 PID1 秒退 |
| `OPS_URL` | **不设** | **不设** | 设了会多起一个 256MB 的 crash 监控 JVM |
| `BI_JAR_HOME` | **不设** | **不设** | 默认指向自己容器的 lib，正好是那 6 个 jar |

`sync` 是 polars 的存储层默认档：节点间直接传数据，不落共享存储。
官方推荐 `s3v2`（要 MinIO）是因为多机弹性伸缩需要共享数据面；
单 worker + 11 万行用 `sync` 足够，省掉一个 MinIO 容器。

#### 坑：worker 的 Xmx 低于 2048 会直接崩

第一版给了 `-Xmx1024m`，容器 25 秒后 `exit 255`：

```
java.lang.IllegalArgumentException: The memory size for polars too small : -198642237
    at com.fanruan.polars.execution.ResourcePlate.start0(ResourcePlate.java:133)
```

值是**负数**。反编译 `fine-bi-engine-polars-6.1.jar` 才看明白 polars 的分账模型：

| 配置键 | 默认值 | 含义 |
| --- | --- | --- |
| `polars.core.memory.system.max.size` | **本进程 Xmx** | "polars 认为的可用内存上限" |
| `polars.core.memory.system.reserve.radio` | `0.12` | 给系统预留，先从上限里扣 |
| `polars.spark.memory.ratio` | `0.4` | Spark 引擎占上限的比例 |
| `SPARK_MEMORY_MIN` | **1GB** | 上一项的**硬下限**，`Math.max(ratio×上限, 1GB)` |
| lake 写入 + 队列 | ≈ 6.5% | `LAKE_WRITER_MEMORY_RATIO` 等 |

Xmx=1024 时：`1024 - 122.9(12%) - 1024(Spark 下限) - 69.8(写入) = -198.6 MB`，
和报错值 `-198642237` 逐字节吻合。

反解下限：`0.815 × Xmx > 1024` → `Xmx > 1256`；留 500MB 真实计算空间 → `Xmx ≥ 1883`。
所以定 **2048**。**这不是"数据小用不着更大"，是引擎的分账模型不允许更小。**

#### 结果：集群成型，但 web 连不上

master 起来了，worker 也注册进去了：

```
Sync Worker | Worker(Worker) | address:/172.28.0.32, port:9001
```

但 web 侧卡死。把 client jar 放回去，容器 25 秒 `exit 1`，`fanruan.log` 里：

```
ExcelDataSourceEngineDriver.init
  → ClientInProcessConnector.start
    → PolarsModule.start
      → LocalMiniProcessStart.start          ← 进程内迷你集群
        → BIWorkerModule.registerInProcess
          → Reflect.forName("com.fanruan.polars.bi.BIWorkerModule")
            → ClassNotFoundException
```

`LocalMiniProcessStart` 的方法名里就写着 InProcess —— 它**在当前 JVM 里**用
`Class.forName` 加载 `com.fanruan.polars.bi.BIWorkerModule`。而那个类是 **class 55（Java 11）**，
web 是 **class 52（Java 8）**。所以：

- 不给 client jar → web 活着，但 `cluster=false`、没有 polars，页面报 `BI-Design_Engine_Error`
- 给 client jar → web 在 `LocalMiniProcessStart` 处崩，进不去

`PolarsUpdateRegister.registerIfOpen` 的三种 role（`local`/`minicluster`/`direct`）
全部依赖这批 Java 11 的类，**没有一条通向"只做客户端连远程 master"**（那条在 finekey
的 K8s 编排层里，由 `DefaultK8sConfig` 从 `master` 这个服务名解析，web 自身没有这个开关）。

**结论**：finekey 这套包在"手工 compose + 单机"下拼不出可用的 FineBI。
集群能起，web 用不上。要用 FineBI 只能走官网的**单机版安装包**（一个 tar.gz 解压即用，
lib 里是完整的 Java 8 实现），不是这个集群镜像包。

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

`docker load` 一个 2GB 的 tgz 约 3 分钟。load 完可以删掉解出来的 tgz。

但 `/d/finebi-pkg` 里那个 341MB 的 `fine-bi-engine-polars-6.1.jar` **先别删**：
它是从 worker 镜像里单独提取的，将来若改走官方三容器方案，直接从这里取，不用再解 13GB 的包。
（卷里 `WEB-INF/lib.disabled/` 也留了一份。）

### 3.2 常用命令

```bash
# 起停（独立 compose，项目名 rt-bi）
# 三容器有严格顺序：master 先 healthy，worker 才注册得上去
bash scripts/switch-mode.sh bi        # 推荐：按 master → worker → web 起，并逐个等就绪
cd docker && docker compose -f docker-compose.finebi.yml up -d
cd docker && docker compose -f docker-compose.finebi.yml down     # 卷保留
cd docker && docker compose -f docker-compose.finebi.yml down -v  # 卷也删（工程配置丢失）

# 三容器状态一眼看
docker ps --filter name=rt-finebi --format '{{.Names}}\t{{.Status}}'

# worker 有没有注册进 master（唯一可信的集群健康判据）
docker logs rt-finebi-master 2>&1 | grep 'Sync Worker' | tail -2

# 看真实占用
docker stats --no-stream rt-finebi rt-finebi-master rt-finebi-worker rt-doris-fe rt-doris-be

# 看 JVM 实际生效的堆参数（别信 compose，信 ps）
docker exec rt-finebi ps -ef | grep -o '\-Xmx[0-9a-zA-Z]*'
docker exec rt-finebi-worker ps -ef | grep -o '\-Xmx[0-9a-zA-Z]*'   # monitor 256M + worker 2048M

# 看 JVM 是几 —— 排查 UnsupportedClassVersionError 的第一步
docker exec rt-finebi java -version          # 1.8
docker exec rt-finebi-worker java -version   # 11

# 看 polars 引擎 jar 是被启用还是被禁用（web 侧本项目最终是禁用）
docker run --rm -v rt-finebi-webroot:/w alpine sh -c \
  'ls /w/WEB-INF/lib/ /w/WEB-INF/lib.disabled/ 2>/dev/null | grep -i polars'

# 恢复 web 的 client jar（会崩，只在复现问题时用）
docker run --rm -v rt-finebi-webroot:/w alpine sh -c \
  'mv /w/WEB-INF/lib.disabled/fine-bi-engine-polars-client-6.1.jar /w/WEB-INF/lib/'

# worker 起不来先看这两行
docker exec rt-finebi-worker sh -c 'tail -20 /data/polars/logs/error.log'
docker exec rt-finebi-worker sh -c 'tail -5  /data/polars/monitor/logs/error.log'
# 日志里 "memory size for polars too small : -xxx" = Xmx 给小了（见 2.11）

# 看 FineBI 业务日志 —— 注意不是 FineLog/（那里只有 gc/safepoint/hs_err）
docker exec rt-finebi sh -c 'ls -lt /usr/local/tomcat/logs/*.log | head -3'
docker exec rt-finebi sh -c 'grep -E "ERROR|Exception" /usr/local/tomcat/logs/fanruan.log | tail -20'
# stdout 被 `BIDiskFull: proxied System.out and System.err` 重定向之后，
# docker logs 就看不到错，真错误一定在 fanruan.log 里
# stdout 被重定向过就看这个提示：BIDiskFull: proxied System.out and System.err

# 健康状态
curl -s --noproxy '*' -o /dev/null -w '%{http_code}\n' \
  http://localhost:9080/webroot/decision/system/health
```

### 3.3 现象 → 原因

| 现象 | 原因 | 怎么确认 |
| --- | --- | --- |
| 容器 20 秒后退出，**退出码 1**，`docker logs` 断在 `cluster update activator start` | `bi-web` 缺 polars 引擎 jar，加载不到 `com.fanruan.polars.bi.BIWorkerModule` | `docker cp rt-finebi:/usr/local/tomcat/logs .`，grep `fanruan.log` 里的 `Exception` |
| 同上，但 `FineLog/` 里只有 gc/safepoint | 正常。JVM 级日志在 `FineLog/`，**业务日志在 `logs/` 根** | 别在 `FineLog/` 里找业务栈 |
| 补了 engine jar 后报 `UnsupportedClassVersionError`，`55.0` vs `52.0` | 那个 jar 是 **Java 11** 编译（55.0），`bi-web` 跑 **Java 8**（52.0） | `docker exec rt-finebi java -version` 对比 jar 的 class 版本 |
| `Unrecognized VM option 'PrintGCDateStamps'` | `/docker-entrypoint.sh` 硬拼了 Java 8 的 GC 参数（JDK 9 已移除） | 只在换了 Java 11 后出现，Java 8 下正常 |
| `ClassFormatError: Incompatible magic value <每次不同的数>` | 两种来源：① Java 11 下 `FineAssist` 的 native agent 踩内存；② `fine-core-11.0.jar` 里 `*.classx` 加密类在 Java 11 下解不开 | 数值随机 + 类名随机 = 内存污染；类名固定 = `.classx`，`jar tf \| grep classx` 可证 |
| `/system/health` 返回 **400**，body 写 `Fine module health recorder is null` | cluster 模块没完整启动（通常是上面那些类加载失败的连带） | 先修类加载，再测 health；`decision` 302 不代表健康 |
| 部署向导红字「JVM参数配置存在问题」 | `Xmx + Direct + Metaspace + CodeCache` > `mem_limit` | 四项相加与限额比；本例 7680 vs 7808 |
| 退出码 **137** | 超 `mem_limit`，被内核 SIGKILL | `OOMKilled=true` 就是它 |
| `Found orphan containers (rt-finebi)` | 两份 compose 项目名相同 | compose 顶层加 `name:` |
| `network rt-net not found` | 主栈没起过，external 网络不存在 | 先起一次 Doris |
| 卷报 `created for project "docker" (expected "rt-bi")` | 改了项目名，卷 label 没变 | 有数据改 `external: true`，无数据删卷重建 |
| **Docker Desktop 说还剩 5GB，脚本说超了 1GB** | 两个口径：前者是实际占用，后者是限额之和 | 见 2.9；看 `docker stats`，不是 `HostConfig.Memory` |
| FineBI 里连不上 Doris | 数据源写了 `localhost` | 容器内 localhost 是容器自己，必须写 `doris-fe` |
| `docker exec` 里路径变成 `D:/…` | Git Bash 把容器路径翻译成 Windows 路径 | 加 `MSYS_NO_PATHCONV=1` |
| `docker cp` 报 `directory "c:\tmp" does not exist` | 上一条的反面：`MSYS_NO_PATHCONV=1` 又关掉了 POSIX→Windows 翻译 | 目标写 Windows 形式 `"D:/finebi-pkg/x"` |

### 3.4 想再省内存可以试的方向

先明确一点：**降 JVM 参数不是为了"省实际内存"**——它们是上限不是预占，压小不省用量。
真正的收益是**把自检的四项和降下来，从而把 `mem_limit` 也降下来**。

按性价比排，除第 1 条外都没实测过，写在这里备查：

1. **Xmx 降到 1536m** —— 11 万行足够（实测 Old Gen 只用到 146MB）。四项和变 6656，
   `mem_limit` 可降到 6784。代价是复杂仪表板并发时 GC 变频繁
2. **Metaspace 降到 1024m** —— 实测只用 132MB，表面上还有七倍余量。但**没实测**，
   改了要重跑"起容器 + 看自检红字"两步。注意早期那次 1024 失败过，
   但那是 polars 缺类的竞态（见 2.5），**不能当作"1024 不行"的证据**；
   现在 polars jar 已经移除（2.4），这条路比当时干净
3. **FineBI 用外接 MySQL 配置库** —— 能省 HSQLDB 的内存，但要多起一个 MySQL，**不划算**
4. **Tomcat 线程数调小** —— `TOMCAT_MAX_THREADS` 默认 500，改 100 能省线程栈
5. **不推荐**：把 `.wslconfig` 提到 12GB 让两套共存 —— Windows 只剩 3.6GB，
   内存压缩会先拖慢整机，体验比"切换"差得多

### 3.5 延伸

- `04-环境搭建.md` §4 —— 10GB / 8960MB 的原始内存定案
- `25-可观测性与链路健康度.md` —— 切换后怎么确认链路真的活着
- `20-指标口径设计.md` —— 在 BI 里做计算字段时要守的口径红线
- `docs/04-环境搭建.md` §6 —— 环境类坑表
- 帆软官方：《修改内存》`help.finebi.com/doc-view-56.html`
- 帆软官方：《确认 FineBI 项目服务器配置》`help.fanruan.com/fineops/edition-view-3341-0.html`
