# scripts

数据装载与埋点生成脚本。

```
scripts/
├── requirements.txt
├── prepare_load.py      # 原始 CSV → LOAD DATA 可读的 TSV
├── import.sh            # 一键：起 MySQL → 预处理 → 导入 → 验收
└── gen_user_events.py   # 合成埋点事件 → Kafka
```

## 依赖

```bash
python -m pip install -r scripts/requirements.txt
```

只有 `gen_user_events.py --sink kafka` 需要第三方库；`prepare_load.py` 和
`--sink stdout` 纯标准库，不装任何东西也能跑。

---

## `prepare_load.py`

`data/raw/olist/*.csv` → `build/load/*.tsv`。

这不是"数据清洗"，是 **`LOAD DATA` 吃不下原始文件**，具体四件事：

| 问题 | 后果 |
| --- | --- |
| `order_reviews` 有正文含双引号（RFC4180 用 `""` 转义） | MySQL 的 `ENCLOSED BY '"'` 不认这种转义，字段被切断 |
| `order_reviews` / `category_translation` 是 CRLF | 按 `\n` 解析会让最后一列尾部多一个 `\r`，DATETIME 报 1292 |
| 空字段是 `""` 而非 `NULL` | 空串进 DATETIME/INT 直接报错；进文本列则让空值核对永远失败 |
| 评论正文内含换行 | 一条记录被切成多行，后续全部错位 |

做法：空串 → `\N`，其余做反斜杠转义（`\t` `\n` `\r` `\\`）。**不改任何业务值**——
不动大小写、不补零、不四舍五入、不删行。唯一有损的地方是 geolocation 的经纬度
（原始 14 位小数，`DECIMAL(10,7)` 保留 7 位）。

输出 `build/load/manifest.json` 记录行数与空值分布，供导入后对账。

## `import.sh`

```bash
bash scripts/import.sh
```

起容器 → 等健康检查 → 检查是否已导入（幂等）→ 预处理 → `LOAD DATA` → 跑 `02-verify.sql`。

已导入过会直接退出，不会重复写入。要重来：

```bash
docker compose --project-directory docker --profile source down -v
rm -rf docker/volumes/mysql/*
```

## `gen_user_events.py`

合成用户行为埋点写 Kafka。为什么是合成：Olist 只有交易结果（订单、支付），
没有任何行为过程，而大屏的转化漏斗必须由行为流算出来。

```bash
# 批量 2 万条
python scripts/gen_user_events.py --events 20000

# 持续压测：每秒 30 条，跑 10 分钟
python scripts/gen_user_events.py --rate 30 --duration 600

# 只打印，不连 Kafka
python scripts/gen_user_events.py --events 5 --sink stdout

# 幂等验证：混入 3% 重复事件（event_id 相同）
python scripts/gen_user_events.py --events 20000 --dup-rate 0.03
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--events` | 20000 | 事件总数（实际会略超，不会少） |
| `--rate` | 0 | 每秒条数，0 = 不限速 |
| `--duration` | 0 | 持续秒数，配合 `--rate` |
| `--window` | 3600 | `event_time` 铺开的时间窗口（秒） |
| `--dup-rate` | 0 | 重复事件比例 |
| `--p-add` / `--p-checkout` / `--p-pay` | 0.18 / 0.35 / 0.50 | 漏斗各环节转化率 |
| `--bootstrap` | `localhost:29092` | 宿主机侧地址（容器内用 `kafka:9092`） |
| `--seed` | 无 | 随机种子，便于复现 |

设计要点：

1. **实体用真实 ID**：`product_id` 取自 `olist_products`，`user_id` 取自
   `customer_unique_id`。这样 `ods_user_event` 才能和 `dim_product` / `dim_user`
   真 join 出品类漏斗，而不是自造 ID 再自己 join 自己。
2. **漏斗按商品独立转化**：一次会话浏览 1~3 个商品，每个商品各自走
   加购→提交→支付。对齐 `docs/03 §2`「同一用户同一商品分别计入各自环节」。
3. **`event_time` 不晚于生产时刻**：会话起点在窗口内随机，末尾越界就把整段前移。
   违反这条的后果是 Flink 当迟到数据丢掉，现象是「生产了几万条，漏斗只统计一半」，
   而且日志不报错。
4. **`session_id` 做 Kafka message key**：同一会话进同一分区，分区内保序，
   漏斗按 session 归因依赖这一点。

实测（2 万条，`--seed 20261030`）：`page_view` 77.9% / `add_cart` 14.5% /
`checkout` 5.0% / `pay` 2.6%，支付转化率 3.36%，投递 11,810 条/秒。

---

## 待补充（P6）

- [ ] `inject_dirty_data.py`：乱序、超时迟到、字段缺失注入
      （重复事件已由 `gen_user_events.py --dup-rate` 覆盖）
- [ ] 迟到数据必须**延迟投递**才有意义——只把 `event_time` 改早、投递顺序不变，
      在 event-time 语义下仍然算准时到达，起不到验证 watermark 的作用
- [ ] `check_lag.py`：端到端延迟采样
