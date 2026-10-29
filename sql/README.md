# sql

建表与初始化 SQL，按目标引擎分目录、按分层编号。

```
sql/
├── mysql/
│   ├── 01_schema.sql          # 业务库 9 张表 + 索引 + binlog 相关参数
│   └── 02_seed.sql            # 类目翻译等小表初始化（大表由脚本装载）
└── doris/
    ├── 01_ods.sql             # ODS 贴源层
    ├── 02_dwd.sql             # DWD 明细层 + 维度表
    ├── 03_dws.sql             # DWS 汇总层
    └── 04_ads.sql             # ADS 应用层
```

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

- 所有表必须含 `dt`（DATE）分区列，开启动态分区
- 金额字段一律 `DECIMAL(18,2)`
- 明细表必须含 `event_time` 与 `proc_time` 双时间戳
- 每个字段加中文 `COMMENT`，ADS 层字段注释直接引用 `docs/03-指标口径.md`
- 单机演示环境 `replication_num = 1`，并在文件头注释中说明生产应为 3

## 待补充

- [ ] 上述全部 SQL 文件
