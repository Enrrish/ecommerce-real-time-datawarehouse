# scripts

数据装载、埋点回放与脏数据注入脚本。

```
scripts/
├── requirements.txt
├── load_olist_to_mysql.py    # CSV → MySQL，支持分批提交与断点续传
├── replay_events.py          # 生成并回放埋点事件到 Kafka
├── inject_dirty_data.py      # 重复 / 乱序 / 迟到 / 字段缺失注入
└── check_lag.py             # 端到端延迟采样与统计
```

## 脚本说明

### `load_olist_to_mysql.py`

把 `data/raw/olist/*.csv` 装入 MySQL 业务库。注意点：

- 按外键依赖顺序装载：`customers → products → sellers → orders → order_items → payments → reviews → geolocation`
- 用 `executemany` 分批（每批 5000 行）提交，避免单事务过大
- 时间字段需处理空值（Olist 中未发货订单的 `delivered_at` 为空）
- geolocation 100 万行走 `LOAD DATA LOCAL INFILE`，比 executemany 快一个数量级

### `replay_events.py`

**整个项目里最能体现工程能力的一个脚本。**

```
python replay_events.py --speed 1000 --duration 3600 --target kafka
```

| 参数 | 说明 |
| --- | --- |
| `--speed` | 加速倍率，1 ~ 1000 |
| `--duration` | 回放的数据时间跨度（秒） |
| `--target` | `kafka` 或 `stdout`（离线生成 CSV） |
| `--dup-rate` | 重复事件率，默认 0.03 |
| `--late-rate` | 迟到事件率，默认 0.05 |
| `--late-max` | 最大迟到时长（秒），默认 300 |

设计要点：

1. 按 `event_ts` 排序后用 5 分钟滑动窗口投递，天然产生乱序
2. 复用 Olist 真实 `customer_id` / `product_id`，保证与业务库可 Join
3. `event_id` 用 `uuid5(namespace, user_id + product_id + ts)` 生成，重复投递时天然同 ID
4. 支持 Ctrl+C 优雅退出并打印已发送条数，便于断点续传

### `inject_dirty_data.py`

按比例注入四类脏数据，用于验证 DWD 清洗规则与幂等：

| 类型 | 默认比例 | 期望行为 |
| --- | --- | --- |
| 重复消息 | 3% | ODS 主键去重后行数不变 |
| 时间乱序 | 5% | 窗口结果与有序基线一致 |
| 超时迟到 | 5% | 触发 allowed lateness 更新，整点补数兜底 |
| 字段缺失 | 2% | DWD 过滤并计入质量监控表 |

## 待补充

- [ ] 上述全部脚本
- [ ] 单元测试（`tests/test_replay.py`）
