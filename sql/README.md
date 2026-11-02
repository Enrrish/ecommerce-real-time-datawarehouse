# sql

建表与初始化 SQL，按目标引擎分目录、按分层编号。

```
sql/
├── mysql/
│   ├── 01-schema.sql          # 业务库 9 张表 + 索引 + binlog 相关参数
│   ├── 02-verify.sql          # 导入后行数与字段核对
│   └── 03-load.sql            # 9 个 CSV 的装载脚本
└── doris/
    ├── 01-ods.sql             # ODS 贴源层 10 张表
    ├── 02-verify.sql          # ODS 层七类核对
    ├── 03-dwd.sql             # DWD 明细层 + 维度表 7 张
    └── 04-dwd-load.sql        # 5 张维表的一次性装载（在 Doris 侧跑，不经过 Flink）
```

## 层的边界：哪些建表、哪些装数

`sql/doris/` 下的文件分两类，别混：

| 类型 | 文件 | 做什么 |
| --- | --- | --- |
| 建表 DDL | `01-ods.sql`、`03-dwd.sql` | 只 `CREATE TABLE`，不含数据 |
| 校验 | `02-verify.sql` | 只 `SELECT`，不改数据 |
| 装载 | `04-dwd-load.sql` | `INSERT ... SELECT`，把 ODS 加工成 5 张维表 |

`dwd_trade_order_detail` 和 `dwd_user_event_clean` 不在这里建数据 —— 它们由 `flink/sql/05`、`06` 两条
流式作业持续写入。**维表走批、事实走流**，这是 P4 定下的分层依据（见 `flink/README.md`「P4 实测结论」）。

## 命名规范

| 对象 | 规则 | 示例 |
| --- | --- | --- |
| ODS 表 | `ods_<源系统表名>` | `ods_orders` |
| DWD 表 | `dwd_<业务域>_<实体>` | `dwd_trade_order_detail` |
| DWD 维表 | `dim_<实体>` | `dim_geo` |
| DWS 表 | `dws_<业务域>_<维度>_<周期>` | `dws_trade_category_1d` |
| ADS 表 | `ads_<应用场景>` | `ads_realtime_overview` |

周期后缀固定为 `1m` / `1h` / `1d`，禁止使用 `minute` / `hourly` 等变体。

## 强制约定

- 明细表必须含 `dt`（DATE）**分区列**，且 `dt` 必须取**不可变**的业务时间
  （下单时间 / 事件时间），不能取装载日期 —— UNIQUE 表的唯一性只在单分区内保证，
  分区列可变会让同一主键落两个分区、互不覆盖、行数翻倍。维表不分区
- 金额字段一律 `DECIMAL(18,2)`
- 明细表保留业务时间（`event_time` 或下单/妥投时间）+ 一个写入时刻字段（`update_time`）
- 每个字段加中文 `COMMENT`，ADS 层字段注释直接引用 `docs/03-指标口径.md`
- 单机演示环境 `replication_num = 1`，并在文件头注释中说明生产应为 3

## 待补充

- [x] `mysql/01-schema.sql`、`03-load.sql`
- [x] `doris/01-ods.sql`、`02-verify.sql`
- [x] `doris/03-dwd.sql`、`04-dwd-load.sql`
- [ ] `doris/05-dws.sql`（P5）
- [ ] `doris/06-ads.sql`（P6）
