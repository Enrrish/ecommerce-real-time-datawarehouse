# 06 · MySQL 8

> 一句话：MySQL 在本项目里**只是数据源**，不做任何加工。它的价值是"有一条完整、
> 可回放的变更日志（binlog）"，而不是"能存数据"。

---

## 一、通用知识

### 1.1 存储引擎

默认（也是唯一现实选项）是 **InnoDB**：支持事务、行锁、崩溃恢复、外键。
`MyISAM` 只在极老的系统里见到，不支持事务，binlog 的 `ROW` 格式对它也不友好。

一行关键配置：

```ini
innodb_buffer_pool_size = 256M    # 热数据缓存，默认 128M。容器里别给太大
```

### 1.2 字符集：`utf8` 是陷阱

| 名字 | 实际 | 能存 emoji/生僻字 |
| --- | --- | --- |
| `utf8` | **别名，等于 utf8mb3**，最多 3 字节 | 不能 |
| `utf8mb4` | 真 UTF-8，最多 4 字节 | 能 |

建库建表一律 `utf8mb4` + `utf8mb4_unicode_ci`（或 `0900_ai_ci`）。
否则葡萄牙语的重音、中文的生僻字会报 `1366 Incorrect string value`。

客户端也要声明，否则连接层按 latin1 解释：

```bash
mysql --default-character-set=utf8mb4 ...
```

### 1.3 三个类型选择，各有"错了不报错"的后果

**金额：`DECIMAL(18,2)`，绝不用 `FLOAT`/`DOUBLE`**

```sql
DECIMAL(18,2)   -- 精确十进制，适合钱
DOUBLE          -- 二进制浮点，0.1+0.2 != 0.3
```

浮点误差在对账时表现为"永远差几分钱"，而且每台机器可能不一样（取决于计算顺序）。

**时间：`DATETIME`，不用 `TIMESTAMP`**

| | `DATETIME` | `TIMESTAMP` |
| --- | --- | --- |
| 存什么 | 字面值，不带时区 | UTC 时间戳，读写时按会话时区换算 |
| 范围 | 1000 ~ 9999 | 1970 ~ **2038**（4 字节） |
| CDC 后 | 原样 | 极易整体偏移 8 小时 |

`TIMESTAMP` 的时区换算依赖 `time_zone` 会话变量。CDC 工具用自己的连接，
如果它以为自己是 UTC，抽到数仓后所有时间就整体偏 8 小时 —— 而且**源端查是对的**，
只有数仓里错，非常难查。

**邮编/编号：`CHAR(5)` 或 `VARCHAR`，绝不用整数**

24.1% 的巴西邮编以 0 开头（`09790`）。存成 `INT` 就变成 `9790`，之后按邮编 join
会**静默丢数据**：不报错，只是匹配不上。

### 1.4 严格模式与 `sql_mode`

MySQL 5.7+ 默认 `STRICT_TRANS_TABLES`：超长、类型不符、非空插 NULL 都**报错**而不是截断。
这是好事，但要注意它的报错常常不带列名（Doris 也是，见 `15`）。

```sql
SELECT @@sql_mode;
-- ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,...
```

`ONLY_FULL_GROUP_BY` 会让 `SELECT a, b FROM t GROUP BY a` 报错 —— 这是 SQL 标准行为，
不要关它，改写 SQL 就好。

### 1.5 `LOAD DATA` 与 `secure-file-priv`

```sql
LOAD DATA INFILE '/load/orders.tsv'
INTO TABLE orders
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@purchase, @approved, ...)
SET order_purchase_timestamp = NULLIF(@purchase, ''),
    order_approved_at        = NULLIF(@approved, '');
```

两个安全开关：

- `--secure-file-priv=/load`：只允许从该目录读写文件
- `--local-infile=0`：禁止客户端 `LOAD DATA LOCAL INFILE`（数据不经过客户端）

**空字符串写进 `DATETIME` 会报 1292**，必须 `NULLIF(@var,'')`。这条在导入 Olist 时
是第一个坑：`order_approved_at` 大量为空。

### 1.6 CSV 不等于"一行一条记录"

`order_reviews` 的 `comment` 字段含换行符，按行数是 104,719 行，按记录数只有 99,224 条。
必须按 CSV 的引号规则解析（`csv` 模块的 `QUOTE_MINIMAL` + 正确处理 `""` 转义），
不能用 `readlines()`。

### 1.7 常用命令

```bash
# 连接（容器内）
docker exec -it rt-mysql mysql -uroot -proot123 olist

# 非交互
docker exec rt-mysql mysql -uroot -proot123 -N -e "SELECT COUNT(*) FROM orders"

# 看表
SHOW TABLES;  DESC orders;  SHOW CREATE TABLE orders\G
SHOW TABLE STATUS LIKE 'orders'\G       # 行数估算、体积

# 执行计划
EXPLAIN SELECT ...;
EXPLAIN ANALYZE SELECT ...;             # 8.0.18+，真实耗时

# 进程与锁
SHOW PROCESSLIST;
SHOW ENGINE INNODB STATUS\G

# 导入/导出
mysqldump -uroot -p --single-transaction olist > olist.sql
```

### 1.8 外键与 CHECK：贴源层要克制

外键在批量导入时逐行检查，代价高，且要求导入顺序正确。`CHECK` 会让"脏数据进不来"，
而数仓的贴源层恰恰需要**原样收下**脏数据（否则数据质量演示没有素材）。

---

## 二、在本项目中

### 2.1 九张表，155 万行

| 表 | 行数 | 说明 |
| --- | --- | --- |
| `customers` | 99,441 | 注意：`customer_id` 是订单级，`customer_unique_id` 才是人（96,096） |
| `sellers` | 3,095 | |
| `products` | 32,951 | 610 行（1.85%）类目为空 |
| `category_translation` | 71 | 商品表有 73 个类目，71 条翻译 → 有 2 个类目翻不出来 |
| `geolocation` | **1,000,163** | 单表占全库 2/3，`(zip,lat,lng)` 有 28 万重复 |
| `orders` | 99,441 | 订单主表 |
| `order_items` | 112,650 | 订单行，含运费 |
| `order_payments` | 103,886 | 一个订单可有多笔支付 |
| `order_reviews` | 99,224 | 按行数 104,719，因为 comment 含换行 |

数据库 `olist`，`utf8mb4` / `utf8mb4_unicode_ci`，表名与字段名**保持源样**（含源端拼写
错误 `lenght`），不加审计列。

### 2.2 三条硬约束写进了 schema 注释

```sql
order_purchase_timestamp DATETIME,          -- 不用 TIMESTAMP
payment_value            DECIMAL(18,2),     -- 不用 FLOAT
customer_zip_code_prefix CHAR(5),           -- 不用 INT
```

这三条不是为了"规范"，是因为每一条错了都会**静默产生错数据**（见 1.3）。
`sql/mysql/01-schema.sql` 顶部把理由和后果都写下来了。

### 2.3 binlog 参数（详见 `07`）

```yaml
command:
  - --binlog-format=ROW          # 记行变更，不记 SQL 文本
  - --binlog-row-image=FULL      # 记变更前后完整行
  - --server-id=1                # binlog 协议要求
  - --character-set-server=utf8mb4
  - --local-infile=0
  - --secure-file-priv=/load
```

### 2.4 导入流程：预处理成 TSV，再服务端读取

```
data/raw/olist/*.csv
  → scripts/prepare_load.py     （csv 模块解析引号/换行，空串转 \N，写出 TSV）
  → build/load/*.tsv
  → LOAD DATA INFILE '/load/*.tsv'   （--secure-file-priv=/load）
  → MySQL
```

为什么不 `LOAD DATA LOCAL INFILE` 直接读宿主机 CSV：那样数据要经过客户端，
既慢又多一个安全面，而且 Git Bash 的路径改写会让"文件找不到"变成一个玄学问题。

### 2.5 踩过的坑

**坑 1：`/load` 挂载不能加 `:ro`**

```yaml
      - ../build/load:/load        # 注意：没有 :ro
```

MySQL 的 entrypoint 会解析 `mysqld --verbose --help`，把 `secure-file-priv` 指向的目录
当作"必须属主为 mysql"，无条件 `mkdir -p` 后递归 `chown`：

```bash
find "$dir" \! -user mysql -exec chown --no-dereference mysql '{}' +
```

挂成只读时 `chown` 报 `Read-only file system`，而 entrypoint 头部是 `set -eo pipefail`
→ 容器在 `mysqld` 启动之前就 `exit 1`。日志里只有一行 `Entrypoint started` 加一屏 chown
报错，很难联想到是挂载选项。

**坑 2：`initdb.d` 只执行一次**

```yaml
      - ../sql/mysql/01-schema.sql:/docker-entrypoint-initdb.d/10-schema.sql:ro
```

只在 `./volumes/mysql` **为空**时执行。改了 schema 之后重启容器不会生效，必须
`docker compose down`（不删卷）+ 手工 `ALTER`，或者清空数据目录重导。

文件名 `10-schema.sql` 也有讲究：entrypoint 按字典序执行目录下所有 `.sql`，
`02-verify.sql`、`03-load.sql` 也会被扫到 —— 用 `10-` 前缀 + 单文件挂载，确保只有
建表脚本被自动执行。

**坑 3：`2>/dev/null` 让"没选库"变成空结果**

```bash
docker exec rt-mysql mysql -uroot -proot123 -N -e "SELECT COUNT(*) FROM orders"
# ERROR 1046 (3D000): No database selected   ← 被 2>/dev/null 吞掉，$rows 为空
```

要么加库名 `olist.orders`，要么 `-D olist`。

---

## 三、速查

```bash
# 进库
docker exec -it rt-mysql mysql -uroot -proot123 olist

# 校验 9 张表行数
docker exec rt-mysql mysql -uroot -proot123 -N -D olist -e "
SELECT 'orders', COUNT(*) FROM orders UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items UNION ALL
SELECT 'geolocation', COUNT(*) FROM geolocation;"

# 看 binlog 状态
docker exec rt-mysql mysql -uroot -proot123 -e "SHOW MASTER STATUS\G"
docker exec rt-mysql mysql -uroot -proot123 -e "SHOW BINARY LOGS;"

# 改一行（端到端演示的起点）
docker exec rt-mysql mysql -uroot -proot123 -D olist -e "
UPDATE order_items SET price = price + 100 WHERE order_id='...' AND order_item_id=1;"
```

| 报错 | 原因 |
| --- | --- |
| `1292 Incorrect datetime value: ''` | 空串写进 DATETIME，用 `NULLIF(@v,'')` |
| `1366 Incorrect string value` | 客户端字符集不是 utf8mb4 |
| `1406 Data too long` | 字段长度按字符算，但源数据里有超长值 |
| `1046 No database selected` | 缺 `-D` 或库名限定 |
| entrypoint 报 chown `Read-only file system` | 挂载加了 `:ro` |

延伸：`07-MySQL-Binlog-与-CDC-原理.md`、`22-Python-工程.md`（`prepare_load.py`）、
`../02-数仓分层模型.md` §4（9 张表的字段设计）
