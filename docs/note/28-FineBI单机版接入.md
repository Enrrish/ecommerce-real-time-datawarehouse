# 28 · FineBI 单机版接入 Doris

> [27](27-FineBI-与内存预算.md) 讲的是**为什么容器化 BI 走不通**，这篇讲**那怎么用**。
>
> 一次实际配置：Windows 上的 FineBI 6.0 单机版，直连 Docker 里的 Doris，
> 只接 7 张 ADS 宽表做看板。全程不碰 WSL 的 10GB 配额。
>
> 三个关键数字：`127.0.0.1:9030` / `realtime_dw` / 最大一张表 98,666 行。
> 前两个是连接参数，第三个决定了**可以直连、不需要抽取**。

---

## 一、通用知识

### 1.1 跨系统连数据库，本质是"谁解析地址"

宿主上的原生程序和容器里的数据库之间，只有三种走法：

| 走法 | 地址写成 | 谁做解析 | 典型问题 |
| --- | --- | --- | --- |
| **宿主 → 发布端口** | `127.0.0.1:9030` | Docker Desktop 的端口代理 | 只能用 IP，不能用服务名 |
| **容器 → 容器** | `doris-fe:9030` | Docker 内置 DNS | 宿主上解析不了这个名字 |
| 隧道 / 反向代理 | 自建 | 自己 | 多一层要维护 |

本项目两种都用过：容器版 FineBI 必须写 `doris-fe:9030`（容器内 `localhost` 是它自己），
Windows 版必须写 `127.0.0.1:9030`（宿主没有 Docker DNS）。
**同一个库，两个客户端，地址写法正好相反** —— 这是跨系统部署最容易搞混的一点。

### 1.2 直连还是抽取

BI 工具接数据库有两种模式，选错了后面全是麻烦：

| | 直连（Direct） | 抽取（Extract） |
| --- | --- | --- |
| 数据在哪 | 留在数据库 | 复制进 BI 自己的引擎 |
| 查询延迟 | 取决于数据库 | 本地内存，快 |
| 数据新鲜度 | 实时 | 取决于抽取频率 |
| 占用 | BI 侧几乎不占 | BI 侧吃内存/磁盘 |
| 适合 | 聚合后的宽表、行数小 | 明细大表、要跨源 join |

判据不是"数据量大不大"，而是**每次查询要扫多少行**。
一个 100 万行的事实表，如果看板只查 `GROUP BY dt`，Doris 用分区裁剪后可能只扫几千行，
直连照样是毫秒级；反过来一张 5 万行的表，如果每次都要全表排序，抽取更划算。

### 1.3 Windows 上的 BI + 容器里的库，四个隐性差异

不是"能不能连通"的问题，而是连通之后才会暴露的：

1. **字符集**：容器里通常是 UTF-8，Windows 中文版的 JVM 默认字符集常是 GBK。
   表现是中文维度值乱码，英文和数字正常
2. **时区**：容器显式设了 `TZ`，Windows 跟系统时区走。表现是时间列整体偏移几个小时
3. **网络**：多一跳 NAT，单次往返 +1~3ms；大结果集回传吞吐低于容器内直连
4. **生命周期**：两个独立的东西，谁也不管谁。库没起时 BI 会缓存"连接失败"状态，
   库起来之后要手动点一次"测试连接"才会恢复

前两条是**数据正确性**问题，后两条是**使用体验**问题。排查顺序应该反过来：
先看连通，再看数据。

---

## 二、在本项目中

### 2.1 为什么最终选 Windows 单机版

三条理由，按权重排：

1. **内存**。容器版 FineBI 的 `mem_limit` 下限是 7808MB（JVM 自检四项和 7680 不可压），
   加上 master/worker 一共吃掉 VM 的三分之二。Windows 版跑在宿主，不进 WSL2 的 10GB 配额，
   等于把内存整块还给数仓
2. **没有 Java 版本死结**。单机包是配套编译的，不存在 `bi-web`(class 52) 与
   polars 引擎(class 55) 装不进一个 JVM 的问题（见 27 §2.4）
3. **能直接读本地 Excel**。做对比分析时省一步挂载

代价是**不可复现**：容器版写在 compose 里，别人 clone 就能起；Windows 安装包不在仓库里。
所以这篇笔记承担了"安装说明书"的角色。

### 2.2 链路长什么样

```
FineBI 6.0（Windows 原生进程，JVM 8）
   │  JDBC  jdbc:mysql://127.0.0.1:9030
   ▼
Windows localhost:9030
   │  Docker Desktop 的端口代理（com.docker.backend）
   ▼
WSL2 虚拟机 → rt-doris-fe:9030（MySQL 协议）
   │
   ├─ 语义解析、查询计划
   └─ 下发到 rt-doris-be:9060 取数，结果回流 FE 再回传
```

关键点：**BE 不需要对外暴露**。FE 会把结果汇总后通过 9030 这条连接回传，
所以只发布 FE 的两个端口（8030 Web UI / 9030 MySQL 协议）就够了。

### 2.3 连接参数

| 项 | 值 |
| --- | --- |
| 主机 | `127.0.0.1` |
| 端口 | `9030` |
| 库 | `realtime_dw` |
| 只读账号 | `bi_ro` / `bi_ro_2026` |
| 管理员账号 | `root` / 空密码 |
| 驱动 | MySQL 8（FineBI 内置） |

URL 写全：

```
jdbc:mysql://127.0.0.1:9030/realtime_dw?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true
```

`useSSL=false` 不能省 —— Doris 的 9030 不支持 TLS 握手，FineBI 默认会尝试加密连接。

实测验证（用 mysql 客户端经发布端口握手，等价于 FineBI 走的那条路）：

```bash
$ mysql -h 127.0.0.1 -P 9030 -ubi_ro -pbi_ro_2026 -e "SHOW DATABASES"
Database
__internal_schema
information_schema
mysql
realtime_dw
```

### 2.4 七张 ADS 表接什么

全部走**直连**。最大的 98,666 行，Doris 用分区裁剪后单次查询只扫当天分区。

| 表 | 行数 | 粒度 | 适合的图 | 注意 |
| --- | --- | --- | --- | --- |
| `ads_realtime_overview` | 88,157 | 分钟 | 折线 / 面积 / 指标卡 | `cum_gmv` 是累计值，**不可 SUM** |
| `ads_order_stream` | 98,666 | 订单 | 明细表 / 状态分布 | `is_late` 三态 1/0/NULL |
| `ads_category_rank_1d` | 18,990 | 日 × 品类 | 条形图 TopN | `rank_no` 已排好，别再排一次 |
| `ads_region_map_1d` | 10,701 | 日 × 州 | 地图（`center_lat/lng`） | `gmv_ratio` 四位小数，相加会偏 |
| `ads_payment_method_1d` | 2,276 | 日 × 支付方式 | 饼图 / 环图 | `amount_ratio` 与 `cnt_ratio` 分母不同 |
| `ads_funnel_1d` | 16 | 日 × 步骤 | 漏斗图 | 4 步 × 4 天；`conv_rate_vs_prev` 已是比率 |
| `ads_pipeline_health` | 5,203 | 指标快照 | 状态灯 / 指标卡 | 11 个指标 × 473 个时间点 |

列清单（建数据集时对照用）：

```
ads_realtime_overview   dt, minute, gmv, order_cnt, item_cnt, buyer_cnt, pay_user_cnt,
                        freight_total, aov, avg_item_price, freight_ratio, cum_gmv,
                        cum_order_cnt, update_time
ads_order_stream        order_id, dt, order_time, order_status, customer_unique_id,
                        customer_state, customer_city, item_cnt, gmv, payment_total,
                        delivery_days, is_late, update_time
ads_category_rank_1d    dt, category_name, rank_no, gmv, order_cnt, item_cnt, seller_cnt,
                        aov, gmv_ratio, update_time
ads_region_map_1d       dt, customer_state, state_name, gmv, freight_total, item_cnt,
                        order_cnt, buyer_cnt, aov, freight_ratio, gmv_ratio, gmv_rank,
                        center_lat, center_lng, update_time
ads_payment_method_1d   dt, payment_type, rank_no, pay_cnt, pay_amount, order_cnt,
                        avg_installments, amount_ratio, cnt_ratio, update_time
ads_funnel_1d           dt, step, step_order, pv, uv, session_cnt, conv_rate_vs_prev,
                        conv_rate_vs_first, update_time
ads_pipeline_health     metric_key, metric_name, metric_value, metric_unit, metric_level,
                        threshold_desc, metric_group, collect_time
```

漏斗四步是 `page_view → add_cart → checkout → pay`；
健康度 11 项分三组：`capacity`（4 项）/ `delay`（2 项）/ `quality`（5 项）。

### 2.5 建连接的步骤

1. 打开 `http://localhost:37799`（FineBI 6.0 的默认端口），用管理员登录
2. **管理系统 → 数据连接 → 新建数据连接 → MySQL**
3. 填 2.3 那张表。驱动选内置的 `com.mysql.cj.jdbc.Driver`
4. 点「测试连接」，成功后保存
5. 新建分析主题 → 选这个连接 → 勾 7 张 `ads_*` 表
6. 表选「直连」，不要开「抽取」

第 6 步是重点：这些表本来就是聚合好的宽表，抽取只会多一份副本、多一个刷新任务、
多一处口径不一致的风险。

### 2.6 踩过的坑

**① 空密码可能过不了表单校验。** FineBI 的密码框在部分版本里不接受空值，
而 Doris 的 root 默认没有密码。解法是建一个只读账号（已在本项目建好）：

```sql
CREATE USER 'bi_ro'@'%' IDENTIFIED BY 'bi_ro_2026';
GRANT SELECT_PRIV ON realtime_dw.* TO 'bi_ro'@'%';
```

`SELECT_PRIV` 只能读，BI 侧永远不需要写权限 —— 这也顺便防住了误操作改数。

**② 中文乱码 / 时间偏 8 小时。** 如果出现，改 FineBI 安装目录下的 `finebi.vmoptions`：

```
-Dfile.encoding=UTF-8
-Duser.timezone=Asia/Shanghai
```

改完重启 FineBI。验证方法：拉一张含 `step` 或 `payment_type` 的表，
看中文是不是正常；再拉 `ads_realtime_overview` 看 `minute` 落在哪个区间。

**③ `information_schema` 查询偏慢。** FineBI 建数据集时会读元数据，
Doris 的 `information_schema` 是实时拼的，表多时会有几百毫秒。
只在打开数据集列表时感知到，不影响查询本身。

**④ 库没起时连接会"卡住失败状态"。** 先 `switch-mode.sh bi` 把 Doris 拉起来，
再开 FineBI。顺序反了就在数据连接页点一次「测试连接」即可恢复，不用重建连接。

**⑤ 别用 `localhost` 之外的写法。** 宿主上 `doris-fe` 这个名字解析不了，
`172.28.0.10` 也不通 —— Docker Desktop 只代理**发布出来的端口**，
容器内部 IP 在宿主侧是不可路由的。

---

## 三、速查

### 3.1 连接参数卡

```
主机  127.0.0.1          端口 9030
用户  bi_ro              密码 bi_ro_2026
库    realtime_dw        驱动 MySQL 8
URL   jdbc:mysql://127.0.0.1:9030/realtime_dw?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true
```

### 3.2 起停顺序

```bash
# 1. 先起库（只留 Doris，腾出 Flink 的 4GB）
bash scripts/switch-mode.sh bi

# 2. 确认端口活着
curl -s --noproxy '*' -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8030/api/bootstrap

# 3. 再开 FineBI（Windows 开始菜单，或安装目录的启动脚本）
#    默认地址 http://localhost:37799

# 4. 用完切回实时链路
bash scripts/switch-mode.sh realtime --submit
```

停的顺序反过来：先关 FineBI，再切模式。FineBI 不停就切，它会在下一次查询时超时重连。

### 3.3 常用查询

```bash
# 从宿主验证连通（不用装 MySQL 客户端，借一个临时容器）
docker run --rm mysql:8.0.36 mysql -h host.docker.internal -P 9030 \
  -ubi_ro -pbi_ro_2026 -e "SELECT COUNT(*) FROM realtime_dw.ads_realtime_overview"

# 看 ADS 各表行数
docker exec rt-doris-fe mysql -uroot -P9030 -h127.0.0.1 -N -B -e \
  "SELECT table_name, table_rows FROM information_schema.tables
   WHERE table_schema='realtime_dw' AND table_name LIKE 'ads_%' ORDER BY table_name"
```

### 3.4 现象 → 原因

| 现象 | 原因 | 怎么办 |
| --- | --- | --- |
| `Communications link failure` | Doris 没起，或端口没发布 | `switch-mode.sh bi`；`docker ps` 看 9030 有没有映射 |
| `Access denied for user` | 密码错，或账号不存在 | 用 `bi_ro`；root 是空密码，别填 `root` |
| 中文维度值乱码 | JVM 字符集是 GBK | `finebi.vmoptions` 加 `-Dfile.encoding=UTF-8` |
| 时间列整体偏 8 小时 | JVM 时区不是 Asia/Shanghai | 同上，加 `-Duser.timezone=Asia/Shanghai` |
| 连接测试通过但拉不出数 | 账号只有库权限没有表权限 | `GRANT SELECT_PRIV ON realtime_dw.* TO ...` |
| 表列表里看不到 `ads_*` | 连的是 `information_schema` 或 `mysql` 库 | 重新选库 `realtime_dw` |
| 看板数字对不上 | `cum_gmv` 被 SUM 了 | 累计值只取 `MAX` 或按 `minute` 取末行 |
| 查询很慢 | 没走分区裁剪，全表扫 | WHERE 里带上 `dt`，Doris 按 `dt` 分区 |

### 3.5 延伸

- [27 · FineBI 与内存预算](27-FineBI-与内存预算.md) —— 容器化为什么走不通的完整复盘
- [20 · 指标口径设计](20-指标口径设计.md) —— 在 BI 里做计算字段要守的红线
- [16 · Doris 分区、分桶与 Compaction](16-Doris-分区-分桶与-Compaction.md) —— 分区裁剪为什么能让 8 万行表直连也够快
- `dashboard/` —— 零依赖大屏，跟 FineBI 查的是同一批 ADS 表，可以互相验数
