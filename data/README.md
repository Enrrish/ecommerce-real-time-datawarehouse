# 数据集说明

## Olist Brazilian E-Commerce Public Dataset

巴西电商平台 Olist 的真实脱敏交易数据，2016-09 ~ 2018-08，约 10 万笔订单。

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

3. 校验行数（Linux / macOS / Git Bash）：

```bash
for f in data/raw/olist/*.csv; do
  echo "$(basename "$f"): $(( $(wc -l < "$f") - 1 ))"
done
```

### 数据清单

| 文件 | 行数 | 体积 | 业务含义 |
| --- | ---: | ---: | --- |
| `olist_orders_dataset.csv` | 99,441 | 17 MB | 订单主表，含下单 / 审核 / 发货 / 送达 / 预计送达时间 |
| `olist_order_items_dataset.csv` | 112,650 | 15 MB | 订单行，含价格、运费、卖家 |
| `olist_order_payments_dataset.csv` | 103,886 | 5.6 MB | 支付流水，含支付方式与分期 |
| `olist_order_reviews_dataset.csv` | 104,719 | 14 MB | 评价，含评分与评语 |
| `olist_customers_dataset.csv` | 99,441 | 8.6 MB | 客户与地理位置 |
| `olist_products_dataset.csv` | 32,951 | 2.3 MB | 商品，含类目与尺寸重量 |
| `olist_sellers_dataset.csv` | 3,095 | 171 KB | 卖家与地理位置 |
| `olist_geolocation_dataset.csv` | 1,000,163 | 58 MB | 邮编经纬度映射（DWD 维表） |
| `product_category_name_translation.csv` | 70 | 2.6 KB | 葡萄牙语类目名 → 英文 |

合计约 **121 MB**。

### 与数仓分层的对应

| CSV | ODS 表 | 备注 |
| --- | --- | --- |
| orders | `ods_orders` | 订单状态流转是 CDC 的核心观察对象 |
| order_items | `ods_order_items` | DWD 明细宽表的事实来源 |
| payments | `ods_payments` | 一笔订单可能多次支付 |
| reviews | `ods_reviews` | 评价可晚于送达数天产生 |
| customers / sellers | `ods_customers` / `ods_sellers` | 维度表 |
| products | `ods_products` | 维度表，类目需翻译 |
| geolocation | `ods_geolocation` | 百万行维表，用于练 Doris Join 与物化视图 |
| translation | `ods_category_translation` | 小维表 |

## 行为埋点数据（自建）

Olist 只有交易数据、没有行为日志。为覆盖「日志 → Kafka」这条链路，由 `scripts/replay_events.py` 按 Olist 真实的 `customer_id` / `product_id` 生成埋点事件：

```
event_id, user_id, session_id, product_id, event_type, event_ts, page, referrer
```

`event_type` 枚举：`page_view` / `search` / `add_cart` / `checkout` / `pay`

生成的数据写入 `data/staging/`（同样被 gitignore），或直接推送到 Kafka topic `ods_user_event`。

> 关键点：埋点必须复用业务库中真实存在的主键，否则漏斗与成交无法 Join，整条链路的指标会失去意义。
