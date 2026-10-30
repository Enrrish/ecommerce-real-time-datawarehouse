-- =============================================================================
-- 02-verify.sql — P2 验收：9 张表行数必须与 CSV 一致
-- -----------------------------------------------------------------------------
-- 执行：docker exec -i <mysql容器> mysql -uroot -p --table < 02-verify.sql
-- 判读：diff 列全部为 0 才算通过。任何一张表不为 0，都是导入被静默截断
--      （最常见原因：DATETIME 空串报错后整批回滚、CSV 引号解析错位、
--       字符集不是 utf8mb4 导致字符串截断）。
-- =============================================================================

USE `olist`;

-- -----------------------------------------------------------------------------
-- 1. 行数核对
-- -----------------------------------------------------------------------------
SELECT 'customers'            AS tbl, COUNT(*) AS actual,   99441 AS expected, COUNT(*) -   99441 AS diff FROM `customers`
UNION ALL
SELECT 'sellers',                      COUNT(*),              3095,          COUNT(*) -    3095 FROM `sellers`
UNION ALL
SELECT 'products',                     COUNT(*),             32951,          COUNT(*) -   32951 FROM `products`
UNION ALL
SELECT 'category_translation',         COUNT(*),                71,          COUNT(*) -      71 FROM `category_translation`
UNION ALL
SELECT 'geolocation',                  COUNT(*),           1000163,          COUNT(*) - 1000163 FROM `geolocation`
UNION ALL
SELECT 'orders',                       COUNT(*),             99441,          COUNT(*) -   99441 FROM `orders`
UNION ALL
SELECT 'order_items',                  COUNT(*),            112650,          COUNT(*) -  112650 FROM `order_items`
UNION ALL
SELECT 'order_payments',               COUNT(*),            103886,          COUNT(*) -  103886 FROM `order_payments`
UNION ALL
SELECT 'order_reviews',                COUNT(*),             99224,          COUNT(*) -   99224 FROM `order_reviews`;


-- -----------------------------------------------------------------------------
-- 2. 前导零是否被破坏（应为 0）
-- -----------------------------------------------------------------------------
-- 若这里不为 0，说明导入时把 CHAR(5) 当数字处理了，09790 变成了 9790。
-- 这是最容易被忽略、又最难事后补救的一类错误：join 不报错，只是查不到数据。
SELECT 'customers.zip 前导零丢失' AS chk, COUNT(*) - 23995 AS should_be_0
FROM `customers` WHERE `customer_zip_code_prefix` LIKE '0%'
UNION ALL
SELECT 'sellers.zip 前导零丢失',          COUNT(*) - 1027
FROM `sellers`  WHERE `seller_zip_code_prefix` LIKE '0%'
UNION ALL
SELECT 'geolocation.zip 前导零丢失',      COUNT(*) - 245733
FROM `geolocation` WHERE `geolocation_zip_code_prefix` LIKE '0%';


-- -----------------------------------------------------------------------------
-- 3. 空值分布核对（对应 DDL 里标注的 NULL 列）
-- -----------------------------------------------------------------------------
SELECT 'orders.approved_at 应为 160'          AS chk, COUNT(*) - 160   AS should_be_0 FROM `orders`         WHERE `order_approved_at`             IS NULL
UNION ALL
SELECT 'orders.carrier_date 应为 1783',               COUNT(*) - 1783              FROM `orders`         WHERE `order_delivered_carrier_date`  IS NULL
UNION ALL
SELECT 'orders.customer_date 应为 2965',              COUNT(*) - 2965              FROM `orders`         WHERE `order_delivered_customer_date` IS NULL
UNION ALL
SELECT 'products.category 应为 610',                  COUNT(*) - 610               FROM `products`       WHERE `product_category_name`         IS NULL
UNION ALL
SELECT 'reviews.title 应为 87658',                    COUNT(*) - 87658             FROM `order_reviews`  WHERE `review_comment_title`          IS NULL
UNION ALL
SELECT 'reviews.message 应为 58274',                   COUNT(*) - 58274             FROM `order_reviews`  WHERE `review_comment_message`        IS NULL;


-- -----------------------------------------------------------------------------
-- 4. 关系完整性（应全部为 0）
-- -----------------------------------------------------------------------------
SELECT '订单无对应客户'        AS chk, COUNT(*) AS should_be_0
FROM `orders` o LEFT JOIN `customers` c ON o.`customer_id` = c.`customer_id`
WHERE c.`customer_id` IS NULL
UNION ALL
SELECT '明细无对应订单',              COUNT(*)
FROM `order_items` i LEFT JOIN `orders` o ON i.`order_id` = o.`order_id`
WHERE o.`order_id` IS NULL
UNION ALL
SELECT '明细无对应商品',              COUNT(*)
FROM `order_items` i LEFT JOIN `products` p ON i.`product_id` = p.`product_id`
WHERE p.`product_id` IS NULL
UNION ALL
SELECT '支付无对应订单',              COUNT(*)
FROM `order_payments` pm LEFT JOIN `orders` o ON pm.`order_id` = o.`order_id`
WHERE o.`order_id` IS NULL
UNION ALL
SELECT '评价无对应订单',              COUNT(*)
FROM `order_reviews` rv LEFT JOIN `orders` o ON rv.`order_id` = o.`order_id`
WHERE o.`order_id` IS NULL;


-- -----------------------------------------------------------------------------
-- 5. .wslconfig 之外的两个业务口径自检
-- -----------------------------------------------------------------------------
-- 5.1 「人」的数量：customer_unique_id 去重应为 96096，而 customer_id 是 99,441。
--     这两个数字差 3,345，就是复购人群的规模——RFM 分析必须用前者。
SELECT '自然人(customer_unique_id)' AS metric, COUNT(DISTINCT `customer_unique_id`) AS val FROM `customers`
UNION ALL
SELECT '订单级(customer_id)',                 COUNT(DISTINCT `customer_id`)                FROM `customers`
UNION ALL
SELECT '类目数(products)',                    COUNT(DISTINCT `product_category_name`)      FROM `products`
UNION ALL
SELECT '可翻译类目数(translation)',           COUNT(DISTINCT `product_category_name`)      FROM `category_translation`;

-- 5.2 缺翻译的类目（应为 pc_gamer 和 portateis_cozinha_e_preparadores_de_alimentos）
SELECT DISTINCT p.`product_category_name` AS missing_translation
FROM `products` p
LEFT JOIN `category_translation` t ON p.`product_category_name` = t.`product_category_name`
WHERE p.`product_category_name` IS NOT NULL AND t.`product_category_name` IS NULL;
