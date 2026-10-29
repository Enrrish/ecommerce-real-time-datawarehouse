# flink

Flink SQL 作业，全部使用 SQL 而非 DataStream API（本项目定位是数仓建模，不是引擎开发）。

```
flink/
└── jobs/
    ├── 01_ods_mysql_cdc.sql        # MySQL 9 张表 → ODS
    ├── 02_ods_kafka_event.sql      # Kafka topic → ods_user_event
    ├── 03_dwd_order_detail.sql     # 打宽 + 派生履约字段
    ├── 04_dwd_event_clean.sql      # 去重 / 脏数据过滤
    ├── 05_dws_overview_1m.sql      # 分钟级大盘
    ├── 06_dws_category_1d.sql      # 类目日汇总
    ├── 07_dws_funnel_1h.sql        # 小时漏斗
    └── 08_ads_*.sql                # 应用层指标
```

## 作业规范

| 项 | 约定 |
| --- | --- |
| 文件名 | 两位编号 + 目标表名，编号即执行顺序 |
| 文件头 | 注释写明：数据源、目标表、粒度、依赖作业、Checkpoint 策略 |
| 表名 | 与 Doris 侧完全一致，便于排查 |
| UID | 每个算子显式设置 `uid`，保证从 savepoint 恢复时拓扑稳定 |
| Watermark | 事件时间字段 `event_ts`，延迟 30s，允许迟到 5 分钟 |

## 关键配置

```yaml
execution.checkpointing.interval: 30s
execution.checkpointing.mode: EXACTLY_ONCE
state.backend: rocksdb
state.backend.incremental: true
table.exec.state.ttl: 24h
pipeline.parallelism.default: 2
```

> Doris Sink 需要 Checkpoint 间隔 ≥ 30s，且 BE 侧开启两阶段提交（`enable_stream_load_commit=true`），否则无法做到精确一次。

## 待补充

- [ ] 上述全部作业文件
- [ ] 作业提交脚本（`flink run -d -f`）
