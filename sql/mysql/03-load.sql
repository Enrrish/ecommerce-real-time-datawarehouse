-- =============================================================================
-- 03-load.sql — 把预处理后的 TSV 装进 9 张业务表
-- -----------------------------------------------------------------------------
-- 前置条件（缺一不可）：
--   1. 容器已起：docker compose --profile source up -d mysql
--   2. TSV 已生成：python scripts/prepare_load.py
--   3. 容器启动参数含 --secure-file-priv=/load，且 ../build/load 已挂到 /load
--      （见 docker/docker-compose.yml）
--
-- 执行：bash scripts/import.sh
--   或手动：docker exec -i rt-mysql mysql -uroot -p --default-character-set=utf8mb4 \
--             < sql/mysql/03-load.sql
--
-- 为什么要先过一层 TSV，而不直接 LOAD DATA 原始 CSV：
--   原始 CSV 有 174 条评论正文里带双引号（RFC4180 的 "" 转义，MySQL 不认）、
--   有两个文件是 CRLF、空字段是 "" 而不是 NULL。直接导会静默错位或报 1292，
--   细节见 scripts/prepare_load.py 的文件头注释。
--
-- 关于 binlog：这里没有关 sql_log_bin。127 万行的初始装载会写进 binlog
-- （实测约 150~200MB，D 盘剩 219G 完全够），好处是导入本身也成了可回放的
-- 变更流。若将来空间紧张，可在本文件首行加 SET sql_log_bin = 0;
-- —— 但只能在 Flink CDC 作业**尚未启动**时这么做，否则这批数据不会进 ODS。
-- =============================================================================

USE `olist`;

-- -----------------------------------------------------------------------------
-- 维表
-- -----------------------------------------------------------------------------

LOAD DATA INFILE '/load/customers.tsv'
INTO TABLE `customers`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
LINES TERMINATED BY '\n';
SELECT 'customers' AS table_name, ROW_COUNT() AS rows_loaded;

LOAD DATA INFILE '/load/sellers.tsv'
INTO TABLE `sellers`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
LINES TERMINATED BY '\n';
SELECT 'sellers' AS table_name, ROW_COUNT() AS rows_loaded;

LOAD DATA INFILE '/load/products.tsv'
INTO TABLE `products`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
LINES TERMINATED BY '\n';
SELECT 'products' AS table_name, ROW_COUNT() AS rows_loaded;

LOAD DATA INFILE '/load/category_translation.tsv'
INTO TABLE `category_translation`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
LINES TERMINATED BY '\n';
SELECT 'category_translation' AS table_name, ROW_COUNT() AS rows_loaded;

-- -----------------------------------------------------------------------------
-- 事实表
-- -----------------------------------------------------------------------------

LOAD DATA INFILE '/load/orders.tsv'
INTO TABLE `orders`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
LINES TERMINATED BY '\n';
SELECT 'orders' AS table_name, ROW_COUNT() AS rows_loaded;

LOAD DATA INFILE '/load/order_items.tsv'
INTO TABLE `order_items`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
LINES TERMINATED BY '\n';
SELECT 'order_items' AS table_name, ROW_COUNT() AS rows_loaded;

LOAD DATA INFILE '/load/order_payments.tsv'
INTO TABLE `order_payments`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
LINES TERMINATED BY '\n';
SELECT 'order_payments' AS table_name, ROW_COUNT() AS rows_loaded;

LOAD DATA INFILE '/load/order_reviews.tsv'
INTO TABLE `order_reviews`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
LINES TERMINATED BY '\n';
SELECT 'order_reviews' AS table_name, ROW_COUNT() AS rows_loaded;

-- -----------------------------------------------------------------------------
-- 坐标表（100 万行，放最后）
-- -----------------------------------------------------------------------------
-- 表里第一个列是自增代理键 id，TSV 没有这一列，所以必须显式给出列清单，
-- 否则会报 1136: Column count doesn't match value count at row 1。
LOAD DATA INFILE '/load/geolocation.tsv'
INTO TABLE `geolocation`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
(`geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng`,
 `geolocation_city`, `geolocation_state`);
SELECT 'geolocation' AS table_name, ROW_COUNT() AS rows_loaded;

-- -----------------------------------------------------------------------------
-- 收尾：立刻核对一次行数，不合预期就不用往下走了
-- -----------------------------------------------------------------------------
SELECT
  '合计' AS table_name,
  (SELECT COUNT(*) FROM `customers`) + (SELECT COUNT(*) FROM `sellers`)
  + (SELECT COUNT(*) FROM `products`) + (SELECT COUNT(*) FROM `category_translation`)
  + (SELECT COUNT(*) FROM `orders`) + (SELECT COUNT(*) FROM `order_items`)
  + (SELECT COUNT(*) FROM `order_payments`) + (SELECT COUNT(*) FROM `order_reviews`)
  + (SELECT COUNT(*) FROM `geolocation`) AS rows_loaded,
  1550922 AS rows_expected;
