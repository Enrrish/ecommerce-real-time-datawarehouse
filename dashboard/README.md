# dashboard

实时大屏。1920×1080 深色，三屏九块，数据全部来自 Doris 的 `ads_*` 表 —— 没有一处是写死的假数字。

## 怎么跑

```bash
bash dashboard/run.sh --with-events  # 起服务 + 持续埋点（演示用这个，漏斗会一直跳）
bash dashboard/run.sh                 # 只起服务，默认 http://127.0.0.1:8080
bash dashboard/run.sh --port 9000     # 换端口
bash dashboard/run.sh --no-refresh    # 只读：不起后台 ADS 刷新线程
bash dashboard/run.sh --snapshot      # 生成单文件离线快照 snapshot.html
```

`run.sh` 会先做容器自检（6 个容器是否都在），再拉起服务。服务本身零第三方依赖，只用 Python 标准库。

**漏斗面板静止，通常不是链路坏了。** 大屏只显示「今天」的漏斗，而
`scripts/gen_user_events.py` 默认是一次性批处理：生成一批 → 发完 → 退出。
没有新事件，累计值当然不动。要让它活起来：

```bash
python scripts/gen_user_events.py --live 2 --sessions 3   # 每 2 秒新开 3 个会话，Ctrl-C 停
```

`--live` 不是「把时间戳往前拨」的假实时：每个会话在**此刻**开始，会话内的动作
按各自的自然延迟（浏览停 4~35 s、提交后再犹豫 30~240 s）排队，到点才发。
所以消息的 `event_time` 与到达时间只差一个发送周期，健康度面板里的
「管道新鲜度」才是真的。

## 面板与数据源

| 分区 | 内容 | 数据来源 | 刷新 |
| --- | --- | --- | --- |
| 顶栏 · 今日 | 今日累计 GMV、订单数（**活的**） | `ads_realtime_overview` where `dt = CURDATE()` | 1 s |
| 顶栏 · 状态 | 链路健康、数据日、时钟、刷新倒计时 | `/api/dashboard` | 1.5 s |
| KPI 条 | GMV / 支付订单 / 成交买家 / 客单价 / 无效交易占比 / 链路 P99 | `ads_realtime_overview` + `ads_pipeline_health` | 60 s |
| 实时转化漏斗 | 商详 → 加购 → 提交 → 支付 | `ads_funnel_1d` | 2 s |
| 成交趋势 | 分钟级累计 GMV + 前一日对比 | `ads_realtime_overview` | 60 s |
| 品类排行 | GMV TOP 6 | `ads_category_rank_1d` | 60 s |
| 地域分布 | 巴西各州气泡地图（气泡面积 ∝ 州 GMV） | `ads_region_map_1d` | 60 s |
| 支付方式 | 环形图 + 占比 | `ads_payment_method_1d` | 60 s |
| 实时订单流水 | 最近 8 笔 | `ads_order_stream` | 60 s |
| 链路健康度 | Kafka 积压 / Checkpoint 耗时 / 端到端 P99 / 反压 | `ads_pipeline_health` | 15 s |

> **两个时间轴是故意的，不是 bug。**
> 大屏主体看的是「分钟覆盖最全的一天」—— 实测选出来是 **2017-11-24 黑五**（743 个分钟点、1,166 单、GMV 178,377.63）。这是历史复盘，业务价值在于完整的一天长什么样。
> 而「改一行 MySQL 5 秒内看到变化」必须有一块今日口径，所以顶栏并排放了 `今日 ¥xxx · n 单`。
> 埋点那条时间轴又是另一个（2026-11），漏斗面板自己标注自己的日期。

## 架构

```
浏览器 ──GET /api/dashboard──► server/app.py ──► server/queries.py ──► Doris
                                   │                  （9 条 SQL，实测 194~324 ms）
                                   └─ 三条后台刷新线程 ──► ADS 层
```

三个文件，职责单一：

| 文件 | 职责 |
| --- | --- |
| `server/doris.py` | 常驻 `docker exec` 查询管道。单次查询 826 ms → 复用 12~26 ms，差 30 倍 |
| `server/queries.py` | 9 条面板 SQL + 中英文映射 + 整形 |
| `server/app.py` | HTTP 路由 + 四档刷新调度 |

**为什么是常驻管道而不是每次新起进程**：`docker exec` 一次要付进程启动 + MySQL 握手约 800 ms，9 条 SQL 就是 7 秒，比链路本身还慢。常驻一条管道后单轮取数 194~324 ms。

**为什么刷新放在服务端而不是页面里**：ADS 是 Doris 侧 `INSERT OVERWRITE` 物化的，不是 Flink 写活的。页面刷新再快，ADS 不变就等于看旧快照。

## 刷新档位

| 档 | 周期 | 做什么 | 为什么是这个周期 |
| --- | --- | --- | --- |
| `today` | 1 s | `INSERT INTO ads_realtime_overview` where `dt = CURDATE()` | 只覆盖今天那 1 行，毫秒级。60 s 的全量档等不起 P7 验收 |
| `funnel` | 2 s | `INSERT OVERWRITE ads_funnel_1d`（从 `dwd_user_event_clean` 重算） | 埋点链路是活的，且单表只有 8 行 |
| `health` | 15 s | `scripts/collect-pipeline-health.sh` | 要跨 Flink / Kafka / Doris 三处采集，单次约 3 s |
| `full` | 60 s | `sql/doris/08-ads-refresh.sql` 全量 7 张表 | 8.8 万 + 9.9 万行，刷一次 2.6 s。交易数据是历史回放，本来就不变 |

`today` 与 `full` 用一把锁互斥：`full` 是 `INSERT OVERWRITE`（先清表再写），与 `today` 的 `INSERT INTO` 并发时，后者的行可能落在「已清空、未写完」的窗口里被吞掉，表现为数字偶尔倒退。

## 技术选型：为什么手写 SVG 而不是 ECharts

环境无外网（见 `docs/04`），ECharts 装不上。但这反而逼出了更好的结果：

- 零依赖，`node_modules` 都没有，`git clone` 完就能跑
- 7 个图表函数（`src/charts.js`，约 600 行）：面积图、横向条形、环形图、漏斗、气泡地图、订单表、健康面板
- 每个都是 40~120 行，比配 ECharts 的 option 更好调 —— 尤其是漏斗和气泡地图，ECharts 得靠 `custom` series 拼

`src/charts.js` 里的两个坑值得记：

- **面积图的横轴必须固定铺满 0–24h**（`X = pad.l + minute / 1440 * iw`），不能按数据范围自适应。否则前后两日的时间轴对不齐，「对比」就没意义了。
- **气泡面积用 `sqrt` 映射**，不是半径直接乘比例。半径 ∝ 面积会让大州视觉上膨胀一个数量级。

## 布局红线：`1fr` 会自己长高

页面曾经出现「每隔一秒整屏往下拉长」——不是动画，是栅格被内容一点点撑开：

```
1fr 等于 minmax(auto, 1fr)
        ↑ auto 的最小值取内容的 min-content
```

图表是按 `panel-body` 的 `clientHeight` 画的，而 `<svg>` 默认是**行内元素**，底部留着
4~6 px 的基线间隙。于是「画满容器」的图比容器高一点点 → 行被内容撑高 → 下一轮量到
更高的容器 → 画更高的图。自增强循环，实测每 1.5 s 长 6 px，十分钟后整屏溢出。

三处一起改才彻底（缺一处都能重新长起来）：

| 位置 | 改法 | 作用 |
| --- | --- | --- |
| `#screen` | `grid-template-rows: 52px 126px minmax(0, 1fr) minmax(0, 1fr)` | 把行的最小尺寸压到 0，内容再高也不许撑行 |
| `.panel-body` | `overflow: hidden` | 真的放不下就裁，不外溢 |
| `.panel-body svg` | `display: block` | 去掉行内基线间隙，图表严格等于容器高 |

修复后 CDP 连续采样 10 秒，七块面板高度恒为 412 px（缩放前）、
`document.scrollHeight` 恒等于视口高，不再出现滚动条。

## 取数红线

- **`ads_*` 的 `dt` 是业务日期（2016-09 ~ 2018-09），不是今天。** 时间条件用 `dt = (SELECT dt FROM ... GROUP BY dt ORDER BY COUNT(*) DESC LIMIT 1)` 定位展示日，不要用 `CURRENT_DATE`。
- **`buyer_cnt` 任何跨桶相加都会偏大**，连跨州都不行（实测 37 个自然人在两个州下过单）。买家数一律从 `ads_order_stream` 去重。
- **地图用 `gmv_ratio` 不用 `gmv`**：各州差两个数量级（SP 32%、RR 0.01%），用绝对金额只有 SP 一州发红。
- **今日口径不能进 `META_CACHE`**：那个缓存 60 s，会把实时性直接吃掉。

## 验收

```bash
bash dashboard/p7-acceptance.sh          # 插一条今日订单，逐层观测，再清理
bash dashboard/p7-acceptance.sh --clean  # 只清理
```

实测 13 轮：改 MySQL → 大屏顶栏数字变化 **237 ~ 3,086 ms**。

`--clean` 必须同时清 Doris 侧：07 作业是纯 `INSERT`（upsert）语义，源端删除**不会**让已聚合的行消失 —— 流式聚合没有 retract 传到 Doris 的路径。

## 目录

```
dashboard/
├── index.html            # 1920×1080 网格骨架（52px / 126px / minmax(0,1fr) ×2）
├── src/
│   ├── theme.css         # 色板与组件样式
│   ├── charts.js         # 7 个零依赖 SVG 图表
│   └── app.js            # 取数、渲染、1.5 s 轮询、等比缩放
├── server/
│   ├── doris.py          # 常驻 docker exec 查询管道
│   ├── queries.py        # 9 条面板 SQL
│   ├── app.py            # HTTP + 四档刷新调度
│   └── snapshot.py       # 生成单文件离线快照（CSS/JS/数据全内联）
├── run.sh
└── p7-acceptance.sh
```

> 色板：背景 `#080B10`，面板 `#0F151C`，主色 `#35D6E8`，上涨 `#FF6A55`（涨红），下跌 `#2FD07A`（跌绿）。
