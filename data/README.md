# 数据集说明

## Olist Brazilian E-Commerce Public Dataset

巴西电商平台 Olist 的真实脱敏交易数据，订单时间跨度 **2016-09-04 ~ 2018-10-17**（773 天），99,441 笔订单。

- 来源：<https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>
- 协议：**CC BY-NC-SA 4.0**（署名 / 非商业 / 相同方式共享）
- 因体积与协议限制，**原始 CSV 不提交到仓库**，已由 `.gitignore` 排除

### 下载与放置

1. 从上述 Kaggle 链接下载 `archive.zip` 并解压
2. 将 9 个 CSV 放入 `data/raw/olist/`，文件名保持原样：

```
data/raw/olist/
├── olist_customers_dataset.csv
├── olist_geolocation_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_orders_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
└── product_category_name_translation.csv
```

3. 校验行数：

```bash
# 注意：不要用 wc -l，见下方说明
python - <<'EOF'
import csv, glob, os
for f in sorted(glob.glob('data/raw/olist/*.csv')):
    with open(f, newline='', encoding='utf-8') as fh:
        print(f"{os.path.basename(f):45s} {sum(1 for _ in csv.reader(fh)) - 1:>10,}")
EOF
```

> **`wc -l` 在这份数据集上会算错，而且错得毫无提示。** 两个具体原因：
>
> 1. `order_reviews` 的评语正文里含换行（RFC4180 允许字段内换行），`wc -l` 按物理行数，
>    算出来是 **104,719**，真实记录数是 **99,224**，虚高 5,495 行。
> 2. `product_category_name_translation.csv` 最后一行没有结尾换行，`wc -l` 少算 1 行，
>    报 70，实际 71 条。
>
> 这两处正好是 `scripts/prepare_load.py` 要处理的前两类问题（字段内换行、CRLF），
> 所以导入流程本身没问题，只是「拿 `wc -l` 对账」这个方法不成立。

### 数据清单

下表是**真实记录数**（用 `csv` 模块解析得到，不是 `wc -l`），也是 MySQL 导入后应得到的行数：

| 文件 | 记录数 | 体积 | 业务含义 |
| --- | ---: | ---: | --- |
| `olist_orders_dataset.csv` | 99,441 | 17 MB | 订单主表，含下单 / 审核 / 发货 / 送达 / 预计送达时间 |
| `olist_order_items_dataset.csv` | 112,650 | 15 MB | 订单行，含价格、运费、卖家 |
| `olist_order_payments_dataset.csv` | 103,886 | 5.6 MB | 支付流水，含支付方式与分期 |
| `olist_order_reviews_dataset.csv` | 99,224 | 14 MB | 评价，含评分与评语（`wc -l` 会虚报 104,719） |
| `olist_customers_dataset.csv` | 99,441 | 8.6 MB | 客户与地理位置 |
| `olist_products_dataset.csv` | 32,951 | 2.3 MB | 商品，含类目与尺寸重量 |
| `olist_sellers_dataset.csv` | 3,095 | 171 KB | 卖家与地理位置 |
| `olist_geolocation_dataset.csv` | 1,000,163 | 58 MB | 邮编经纬度映射（DWD 维表） |
| `product_category_name_translation.csv` | 71 | 2.6 KB | 葡萄牙语类目名 → 英文（末行无换行，`wc -l` 会少算 1） |
| **合计** | **1,550,922** | **121 MB** | 与 `sql/mysql/02-verify.sql` 的期望值一致 |

> `order_reviews` 的 99,224 里有 814 个 `review_id` 出现多次（最多 3 次），因此主键是
> `(review_id, order_id)` 复合键——单列会在导入阶段直接撞主键失败。详见 `docs/02 §1`。

### 与数仓分层的对应

| CSV | ODS 表 | 备注 |
| --- | --- | --- |
| orders | `ods_orders` | 订单状态流转是 CDC 的核心观察对象 |
| order_items | `ods_order_items` | DWD 明细宽表的事实来源 |
| order_payments | `ods_order_payments` | 一笔订单可能多次支付（`payment_sequential` 是第二主键） |
| order_reviews | `ods_order_reviews` | 评价可晚于送达数天产生；复合主键 |
| customers / sellers | `ods_customers` / `ods_sellers` | 维度表 |
| products | `ods_products` | 维度表，类目需翻译 |
| geolocation | `ods_geolocation` | 百万行维表，用于练 Doris Join；源表加了自增代理键 `id` |
| product_category_name_translation | `ods_category_translation` | 小维表，主键 `product_category_name` |

## 行为埋点数据（自建）

Olist 只有交易数据、没有行为日志。为覆盖「日志 → Kafka」这条链路，由
`scripts/gen_user_events.py` 按 Olist 真实的 `customer_unique_id` / `product_id` 生成埋点事件。

消息契约（Kafka topic `user_event`，JSON，key = `session_id`）：

```json
{
  "schema_version": 1,
  "event_id": "uuid",
  "event_time": "2026-11-02 10:23:41.512",
  "event_type": "page_view | add_cart | checkout | pay",
  "user_id": "customer_unique_id",
  "session_id": "uuid",
  "product_id": "真实 product_id",
  "page_url": "/product/{id} | /checkout | /pay/result"
}
```

四条约定，每条都对应一类会静默失败的问题：

1. **`user_id` 用 `customer_unique_id` 而不是 `customer_id`**。后者是订单级的（99,441 个，
   与 `orders` 1:1），拿它算 UV 会把同一个人按订单数重复计数——真实人数只有 96,096。
2. **`event_time` 不允许晚于生产时刻**。生成器会把越界的会话整体前移；违反的后果是
   Flink 当迟到数据丢掉，现象是「生产了几万条，漏斗只统计到一半」，日志里没有报错。
3. **`session_id` 做 message key**，同一会话进同一分区，分区内保序——漏斗按 session
   归因依赖这一点。
4. **`event_id` 全局唯一**，是 `ods_user_event` 的 Unique Key。生成器支持 `--dup-rate`
   故意发重复消息，用来验证唯一键去重（P3 已实测：Kafka 22,064 条 → 表里 22,004 行）。

> 关键点：埋点必须复用业务库中真实存在的主键，否则漏斗与成交无法 Join，整条链路的指标会失去意义。
