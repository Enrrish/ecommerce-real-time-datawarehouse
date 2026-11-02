-- =============================================================================
-- 04-dwd-load.sql —— DWD 层的「离线初始化」装载
-- -----------------------------------------------------------------------------
-- 为什么有些维表用 Doris SQL 而不是 Flink SQL：
--
--   dim_date 与 dim_geo 都是**静态维表**，数据来源已经完整躺在 ODS 层（或根本
--   没有来源，如日历）。用 Flink 作业去跑它们，等于为了 4,383 行日历常驻一个
--   slot、为一个不再变化的百万行表维护 CDC 位点，纯属浪费。
--
--   判断标准很简单：**数据源会不会变**。会变 → Flink 流；不会变 → Doris 一次性
--   计算。geolocation 是邮编坐标，现实中几乎不变；日历更是纯推导。
--
-- 这两步不消耗 Flink slot，先跑完它们，后面 SCD2 维表才有可 join 的对象。
--
-- 执行：MSYS_NO_PATHCONV=1 docker exec -i rt-doris-fe \
--         mysql -h127.0.0.1 -P9030 -uroot realtime_dw < sql/doris/04-dwd-load.sql
-- =============================================================================

USE `realtime_dw`;


-- -----------------------------------------------------------------------------
-- 1. dim_date —— 2016-01-01 ~ 2027-12-31，4,383 天
-- -----------------------------------------------------------------------------
-- 用 numbers() 表函数生成等差数列，再逐日推导日历属性。比在 Python 里生成 CSV
-- 再导入少一个环节，也不会出现「导入脚本漏了闰年」这种低级错误。
--
-- WEEKDAY() 的返回值是 0=周一 … 6=周日（不是 MySQL 的 DAYOFWEEK 1=周日），
-- 所以 +1 得到 ISO 8601 的 1=周一。这个差异坑过不少人，日历维表里星期错一位
-- 会让「工作日 GMV」整个结论反过来。
--
-- WEEKOFYEAR() 是 ISO 周（周一开始、跨年那一周归属含 1 月 4 日的那年），
-- 所以 2016-01-01 返回 53 而不是 1 —— 它属于 2015 年的第 53 周。这是正确行为。
--
-- 范围给到 2027 是为了留余量：DWS 做同比要往前找 365 天，做趋势要往后留空行。
-- 日历表缺日期会让 join 静默丢行，不报错。
TRUNCATE TABLE `dim_date`;

INSERT INTO `dim_date`
SELECT
  d.dt                                                AS `dt`,
  YEAR(d.dt)                                          AS `year`,
  QUARTER(d.dt)                                       AS `quarter`,
  MONTH(d.dt)                                         AS `month`,
  DAY(d.dt)                                           AS `day`,
  WEEKOFYEAR(d.dt)                                    AS `week_of_year`,
  WEEKDAY(d.dt) + 1                                   AS `weekday`,
  CASE WHEN WEEKDAY(d.dt) >= 5 THEN 1 ELSE 0 END      AS `is_weekend`,
  DATE_FORMAT(d.dt, '%Y-%m-%d')                       AS `date_str`
FROM (
  SELECT DATE_ADD('2016-01-01', INTERVAL `number` DAY) AS dt
  FROM numbers('number' = '4383')
) d;


-- -----------------------------------------------------------------------------
-- 2. dim_geo —— 19,015 个邮编的中心点
-- -----------------------------------------------------------------------------
-- 两段式：先按邮编算坐标均值与点数，再按「邮编+城市」计数取众数。
--
-- 为什么城市要取众数而不是任意一个：
--   Olist 的 geolocation 来自不同数据源，同一个邮编下可能出现
--   'sao paulo' / 'Sao Paulo' / 'são paulo'（编码损坏）多个写法。
--   取众数至少能拿到最主流的那个；取 MIN/MAX 会随机落到损坏的拼写上。
--
-- point_cnt 保留原始点数是有用的：点数 = 1 的邮编，其「中心点」就是那个唯一
-- 坐标，可信度高但可能是个错误录入；点数 > 500 的邮编，中心点被大量点平均，
-- 空间精度低。下游做地域分析时可以用它做加权或过滤。
--
-- 注意 AVG 之后要 ROUND 到 6 位：源列是 DECIMAL(10,7)，目标列是 DECIMAL(10,6)，
-- 不 ROUND 会靠隐式截断，导致不同版本行为不一致。
TRUNCATE TABLE `dim_geo`;

INSERT INTO `dim_geo`
SELECT
  g.zip_prefix                                        AS `zip_prefix`,
  g.lat                                               AS `lat`,
  g.lng                                               AS `lng`,
  g.point_cnt                                         AS `point_cnt`,
  COALESCE(c.city, 'unknown')                         AS `city`,
  COALESCE(c.state, 'NA')                             AS `state`,
  NOW()                                               AS `update_time`
FROM (
  SELECT
    `geolocation_zip_code_prefix`                     AS zip_prefix,
    ROUND(AVG(`geolocation_lat`), 6)                  AS lat,
    ROUND(AVG(`geolocation_lng`), 6)                  AS lng,
    COUNT(*)                                          AS point_cnt
  FROM `ods_geolocation`
  GROUP BY `geolocation_zip_code_prefix`
) g
LEFT JOIN (
  -- 众数：按「邮编+城市+州」分组计数，再用窗口函数取每组内计数最大的那一行
  SELECT zip_prefix, city, state
  FROM (
    SELECT
      `geolocation_zip_code_prefix`                   AS zip_prefix,
      `geolocation_city`                              AS city,
      `geolocation_state`                             AS state,
      ROW_NUMBER() OVER (
        PARTITION BY `geolocation_zip_code_prefix`
        ORDER BY COUNT(*) DESC, `geolocation_city`
      )                                               AS rn
    FROM `ods_geolocation`
    GROUP BY `geolocation_zip_code_prefix`, `geolocation_city`, `geolocation_state`
  ) x
  WHERE rn = 1
) c ON g.zip_prefix = c.zip_prefix;


-- -----------------------------------------------------------------------------
-- 3. dim_seller —— 卖家维（全量覆盖）
-- -----------------------------------------------------------------------------
-- 3,095 行、属性只有城市/州，现实中不会变。为它常驻一个 Flink CDC 作业是浪费
-- slot（本项目单 TM 只有 10 个 slot，每个都值钱）。
--
-- 这个取舍的代价要说清楚：**卖家信息变更后 dim_seller 不会自动跟随**，需要重跑
-- 这一段。如果真实业务里卖家等级、评分会变，就应该改成 Flink 流作业。
--
-- 判断标准与 dim_date / dim_geo 相同：数据源会不会变。这里答案是「几乎不会」。
TRUNCATE TABLE `dim_seller`;

INSERT INTO `dim_seller`
SELECT
  `seller_id`                    AS `seller_id`,
  `seller_zip_code_prefix`       AS `seller_zip_code_prefix`,
  `seller_city`                  AS `seller_city`,
  `seller_state`                 AS `seller_state`,
  NOW()                          AS `update_time`
FROM `ods_sellers`;


-- -----------------------------------------------------------------------------
-- 4. dim_user_zip —— 用户维 SCD2 拉链（批处理生成）
-- -----------------------------------------------------------------------------
-- **为什么拉链用批处理而不是 Flink 流**
--
-- 纯 SQL 做流式 SCD2 卡在一个硬约束上：Flink 的 OVER 聚合（LAG）与 lookup join
-- 都不接受 CDC 的 changelog 流。两条路都试过：
--   * LAG：报 StreamPhysicalOverAggregate doesn't support consuming update and
--     delete changes —— OVER 只能吃 append 流
--   * lookup join 回查 Doris：connector 传的用户名恒为 null（认证失败），且
--     回查自己正在写的表有「读到的版本是否已提交」的一致性问题
--
-- 要真做流式拉链，需要额外一张 append-only 的变更流水表（把 CDC 的 -U/+U 转成
-- 两条插入），再由它驱动拉链。那会新增一张 docs/02 里没有定义的表，属于 P5 的
-- 优化项，不阻塞 P4。
--
-- **批处理拉链在这里是正确解，不是妥协**：
--   Olist 的 customers 是静态快照，属性不会随时间变。所谓「拉链」在这个数据集
--   上的价值不是跟踪历史，而是**把同一自然人的多条订单地址收敛成唯一当前版本**。
--   源数据里 96,096 个 customer_unique_id 中有 **122 个填了不同的城市**（同一人
--   不同订单地址不一致），这才是本表要解决的真实问题。
--
-- start_date 统一取 '2016-01-01'（数据起始日）：源数据没有属性变更时间，
-- 编造一个更精确的时间反而是假数据。
--
-- 同一 unique_id 有多条时按 customer_id 倒序取最后一条，保证结果确定（重跑不变）。
TRUNCATE TABLE `dim_user_zip`;

INSERT INTO `dim_user_zip`
SELECT
  `customer_unique_id`                                  AS `customer_unique_id`,
  '2016-01-01 00:00:00'                                 AS `start_date`,
  `customer_zip_code_prefix`                            AS `customer_zip_code_prefix`,
  `customer_city`                                       AS `customer_city`,
  `customer_state`                                      AS `customer_state`,
  '9999-12-31 23:59:59'                                 AS `end_date`,
  1                                                     AS `is_current`
FROM (
  SELECT
    `customer_unique_id`,
    `customer_zip_code_prefix`,
    `customer_city`,
    `customer_state`,
    ROW_NUMBER() OVER (
      PARTITION BY `customer_unique_id`
      ORDER BY `customer_id` DESC
    ) AS rn
  FROM `ods_customers`
) t
WHERE rn = 1;


-- -----------------------------------------------------------------------------
-- 5. dim_product_zip —— 商品维 SCD2 拉链（批处理生成）
-- -----------------------------------------------------------------------------
-- 与 dim_user_zip 同理。商品的类目/尺寸在源数据里也是静态的。
--
-- 类目翻译同样必须 LEFT JOIN + COALESCE：71 个翻译类目对不上 73 个源类目，
-- 另有 610 个商品类目为 NULL。INNER JOIN 会静默丢掉这些商品，
-- COALESCE 的兜底顺序是「英文名 → 葡语原名 → unknown」。
TRUNCATE TABLE `dim_product_zip`;

INSERT INTO `dim_product_zip`
SELECT
  p.`product_id`                                        AS `product_id`,
  '2016-01-01 00:00:00'                                 AS `start_date`,
  p.`product_category_name`                             AS `product_category_name`,
  COALESCE(t.`product_category_name_english`,
           p.`product_category_name`,
           'unknown')                                   AS `category_name_en`,
  p.`product_name_lenght`                               AS `product_name_length`,
  p.`product_description_lenght`                        AS `product_description_length`,
  p.`product_photos_qty`                                AS `product_photos_qty`,
  p.`product_weight_g`                                  AS `product_weight_g`,
  p.`product_length_cm`                                 AS `product_length_cm`,
  p.`product_height_cm`                                 AS `product_height_cm`,
  p.`product_width_cm`                                  AS `product_width_cm`,
  '9999-12-31 23:59:59'                                 AS `end_date`,
  1                                                     AS `is_current`
FROM `ods_products` p
LEFT JOIN `ods_category_translation` t
  ON p.`product_category_name` = t.`product_category_name`;
