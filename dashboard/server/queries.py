"""大屏各面板的取数 SQL 与结果整形。

设计前提（P7 最重要的一个事实）：
    交易数据是 **2016-09 ~ 2018-09 的历史回放**，埋点数据是 **2026 年实时产生的**，
    两条时间轴差 8 年。所以不存在一个统一的「今天」：
      * 交易类面板（KPI / 趋势 / 品类 / 地域 / 支付 / 流水）用「数据最完整的一天」
      * 漏斗面板用埋点自己的最新日
    两个日期都在 meta 里吐给前端，页面上如实标注，不做「假装是今天」。

为什么展示日不是 MAX(dt)：
    2018-09-03 只有 1 单 1 个分钟点（Olist 数据尾部截断），拿它当「今日」
    会让成交趋势只剩一个点、KPI 只有 166 元。改为「分钟覆盖最全的一天」，
    实测选出来是 2017-11-24（黑五，743 个分钟点、1,166 单）。
"""

from __future__ import annotations

from doris import Doris, fmt_num

# ---------------------------------------------------------------------------
# 维度值的中文映射
# ---------------------------------------------------------------------------
# Olist 的 71 个类目名是葡萄牙语/英语混写，直接上屏在中文大屏里很突兀。
# 只映射可能出现的那部分，未命中的原样透出 —— 静默回退比编造一个错译好。
CATEGORY_CN = {
    "bed_bath_table": "家居厨卫",
    "watches_gifts": "手表礼品",
    "furniture_decor": "家具装饰",
    "health_beauty": "美妆个护",
    "computers_accessories": "电脑配件",
    "sports_leisure": "运动休闲",
    "housewares": "家居日用",
    "telephony": "手机通讯",
    "garden_tools": "园艺工具",
    "auto": "汽车用品",
    "toys": "玩具",
    "cool_stuff": "潮玩",
    "perfumery": "香水",
    "baby": "母婴",
    "electronics": "电子设备",
    "stationery": "文具",
    "fashion_bags_accessories": "箱包配饰",
    "pet_shop": "宠物用品",
    "furniture_living_room": "客厅家具",
    "construction_tools_construction": "建材工具",
    "home_construction": "家装",
    "air_conditioning": "空调",
    "signaling_and_security": "安防",
    "office_furniture": "办公家具",
    "audio": "音频设备",
    "computers": "电脑整机",
    "fixed_telephony": "固话",
    "food": "食品",
    "drinks": "饮料",
    "books_general_interest": "图书",
    "small_appliances": "小家电",
    "home_appliances": "家电",
    "home_appliances_2": "家电",
    "furniture_mattress_and_upholstery": "床垫布艺",
    "musical_instruments": "乐器",
    "art": "艺术品",
    "agro_industry_and_commerce": "农工商",
    "industry_commerce_and_business": "工业商贸",
    "fashion_shoes": "鞋类",
    "fashion_underwear_beach": "内衣泳装",
    "luggage_accessories": "旅行箱包",
    "consoles_games": "游戏主机",
    "dvds_blu_ray": "影音碟片",
    "cds_dvds_musicals": "音乐碟片",
    "cine_photo": "影音摄影",
    "christmas_supplies": "圣诞用品",
    "kitchen_dining_laundry_garden_furniture": "厨卫家具",
    "la_cuisine": "厨房用品",
    "costruction_tools_garden": "园艺工具",
    "construction_tools_lights": "灯具工具",
    "construction_tools_safety": "安全防护",
    "home_comfort_2": "家居舒适",
    "home_confort": "家居舒适",
    "bed_bath_table_2": "家居厨卫",
    "food_drink": "食品饮料",
    "flowers": "鲜花",
    "diapers_and_hygiene": "母婴卫生",
    "party_supplies": "派对用品",
    "pc_gamer": "电竞设备",
    "portateis_casa_forno_e_cafe": "厨房电器",
    "portateis_cozinha_e_preparadores_de_alimentos": "厨房电器",
    "tablets_printing_image": "平板影像",
    "telephony_2": "手机通讯",
    "traffic_signals": "交通标识",
    "music": "音乐",
    "fashion_male_clothing": "男装",
    "furniture_bedroom": "卧室家具",
}

PAYMENT_CN = {
    "credit_card": "信用卡",
    "boleto": "Boleto",
    "voucher": "代金券",
    "debit_card": "借记卡",
    "not_defined": "未定义",
}

ORDER_STATUS_CN = {
    "delivered": "已签收",
    "shipped": "运输中",
    "canceled": "已取消",
    "invoiced": "已开票",
    "processing": "处理中",
    "unavailable": "无法履约",
    "approved": "已批准",
    "created": "已创建",
}

FUNNEL_CN = {
    "page_view": "商品详情浏览",
    "add_cart": "加入购物车",
    "checkout": "提交订单",
    "pay": "完成支付",
}

STEPS = ["page_view", "add_cart", "checkout", "pay"]


# ---------------------------------------------------------------------------
# 元信息：展示日、对比日、漏斗日
# ---------------------------------------------------------------------------
_META_SQL = """
SELECT
  (SELECT dt FROM realtime_dw.ads_realtime_overview
     GROUP BY dt ORDER BY COUNT(*) DESC, dt DESC LIMIT 1)               AS data_date,
  (SELECT MAX(dt) FROM realtime_dw.ads_realtime_overview)               AS latest_trade_date,
  (SELECT MAX(dt) FROM realtime_dw.ads_funnel_1d)                       AS funnel_date,
  (SELECT ROUND(SUM(gmv), 2) FROM realtime_dw.ads_realtime_overview
     WHERE dt = (SELECT dt FROM realtime_dw.ads_realtime_overview
                   GROUP BY dt ORDER BY COUNT(*) DESC, dt DESC LIMIT 1)) AS day_gmv
"""

# 对比日取展示日的前一天；用子查询而不是写死 2017-11-23，换展示日时自动跟随
_PREV_SQL = """
SELECT dt, CAST(SUM(gmv) AS DECIMAL(20,2)) AS gmv, SUM(order_cnt) AS orders
FROM realtime_dw.ads_realtime_overview
WHERE dt = (SELECT MAX(dt) FROM realtime_dw.ads_realtime_overview WHERE dt < '{date}')
GROUP BY dt
"""

# 今日实时：P7 的验收锚点。
# 大屏主体看的是「分钟覆盖最全的一天」（2017-11-24 黑五），那是历史复盘；
# 而「改一行 MySQL，5 秒内数字变化」必须有一块今日的口径，所以单独给一条。
# 这条**不能**进 META_CACHE —— 60 秒缓存会把实时性直接吃掉。
_TODAY_SQL = """
SELECT
  CAST(COALESCE(SUM(gmv), 0) AS DECIMAL(20,2)) AS gmv,
  COALESCE(SUM(order_cnt), 0)                  AS orders,
  COALESCE(SUM(item_cnt), 0)                   AS items,
  COUNT(*)                                     AS minutes
FROM realtime_dw.ads_realtime_overview
WHERE dt = CURDATE()
"""


def _kpi_sql(date: str) -> str:
    return f"""
SELECT
  CAST(SUM(gmv)  AS DECIMAL(20,2))                             AS gmv,
  SUM(order_cnt)                                               AS orders,
  SUM(item_cnt)                                                AS items,
  CAST(SUM(gmv) / NULLIF(SUM(order_cnt), 0) AS DECIMAL(20,2))   AS aov,
  COUNT(*)                                                     AS minutes
FROM realtime_dw.ads_realtime_overview
WHERE dt = '{date}'
"""


def _buyers_sql(date: str) -> str:
    # 买家数必须从订单粒度去重，不能 SUM(overview.buyer_cnt)：
    # 分钟级 buyer_cnt 是同一个人在多个分钟各算一次，相加会偏大（docs/03 有实测）
    return f"""
SELECT COUNT(DISTINCT customer_unique_id) AS buyers,
       SUM(CASE WHEN is_late = 1 THEN 1 ELSE 0 END) AS late_orders,
       SUM(CASE WHEN is_late IS NOT NULL THEN 1 ELSE 0 END) AS delivered_orders
FROM realtime_dw.ads_order_stream
WHERE dt = '{date}'
"""


def _trend_sql(date: str, prev: str) -> str:
    return f"""
SELECT 'today' AS tag, minute, CAST(cum_gmv AS DECIMAL(20,2)) AS cum
FROM realtime_dw.ads_realtime_overview WHERE dt = '{date}'
UNION ALL
SELECT 'prev'  AS tag, minute, CAST(cum_gmv AS DECIMAL(20,2)) AS cum
FROM realtime_dw.ads_realtime_overview WHERE dt = '{prev}'
ORDER BY tag, minute
"""


def _category_sql(date: str) -> str:
    return f"""
SELECT category_name, rank_no, CAST(gmv AS DECIMAL(20,2)) AS gmv,
       order_cnt, CAST(gmv_ratio AS DECIMAL(10,6)) AS ratio
FROM realtime_dw.ads_category_rank_1d
WHERE dt = '{date}' AND rank_no <= 6
ORDER BY rank_no
"""


def _payment_sql(date: str) -> str:
    return f"""
SELECT payment_type, pay_cnt, CAST(pay_amount AS DECIMAL(20,2)) AS amount,
       CAST(amount_ratio AS DECIMAL(10,6)) AS amt_ratio,
       CAST(cnt_ratio AS DECIMAL(10,6)) AS cnt_ratio
FROM realtime_dw.ads_payment_method_1d
WHERE dt = '{date}'
ORDER BY rank_no
"""


def _map_sql(date: str) -> str:
    return f"""
SELECT customer_state, state_name, CAST(gmv AS DECIMAL(20,2)) AS gmv,
       CAST(gmv_ratio AS DECIMAL(10,6)) AS ratio, gmv_rank,
       order_cnt, CAST(center_lat AS DECIMAL(10,4)) AS lat,
       CAST(center_lng AS DECIMAL(10,4)) AS lng
FROM realtime_dw.ads_region_map_1d
WHERE dt = '{date}'
ORDER BY gmv_rank
"""


def _orders_sql(date: str) -> str:
    return f"""
SELECT order_id, order_time, order_status, customer_state, customer_city,
       item_cnt, CAST(gmv AS DECIMAL(20,2)) AS gmv
FROM realtime_dw.ads_order_stream
WHERE dt = '{date}'
ORDER BY order_time DESC
LIMIT 8
"""


def _funnel_sql() -> str:
    # 漏斗取它自己的最新日 —— 埋点时间轴与交易不同（2026 vs 2018）
    return """
SELECT dt, step, step_order, pv, uv, session_cnt,
       CAST(conv_rate_vs_prev AS DECIMAL(10,6))  AS conv_prev,
       CAST(conv_rate_vs_first AS DECIMAL(10,6)) AS conv_first
FROM realtime_dw.ads_funnel_1d
WHERE dt = (SELECT MAX(dt) FROM realtime_dw.ads_funnel_1d)
ORDER BY step_order
"""


def _health_sql() -> str:
    return """
SELECT metric_key, metric_name, CAST(metric_value AS DOUBLE) AS v,
       metric_unit, metric_level, metric_group, threshold_desc
FROM realtime_dw.ads_pipeline_health
ORDER BY metric_group, metric_key
"""


# ---------------------------------------------------------------------------
# 组装
# ---------------------------------------------------------------------------
def _first(rows: list[list[str]]) -> list[str]:
    return rows[0] if rows else []


def collect(doris: Doris, meta_cache: dict) -> dict:
    """跑完一轮面板取数，返回可以直接 JSON 化的字典。"""

    # --- 元信息：展示日变化不频繁，缓存 60 秒，避免每轮都做一次 GROUP BY 排序
    import time as _t

    now = _t.time()
    if meta_cache.get("at", 0) < now - 60 or "date" not in meta_cache:
        row = _first(doris.query(_META_SQL))
        meta_cache["date"] = row[0] if len(row) > 0 else None
        meta_cache["latest_trade"] = row[1] if len(row) > 1 else None
        meta_cache["funnel_date"] = row[2] if len(row) > 2 else None
        meta_cache["at"] = now
    date = meta_cache["date"]
    funnel_date = meta_cache["funnel_date"]

    row = _first(doris.query(_PREV_SQL.format(date=date)))
    prev = row[0] if row else None
    prev_gmv = fmt_num(row[1], 2) if len(row) > 1 else None
    prev_orders = fmt_num(row[2], 0) if len(row) > 2 else None

    # --- 今日实时（不缓存，每次请求都查；P7 验收的观察点）
    t = _first(doris.query(_TODAY_SQL))
    today_live = {
        "date": _t.strftime("%Y-%m-%d", _t.localtime()),
        "gmv": fmt_num(t[0], 2) if len(t) > 0 else None,
        "orders": fmt_num(t[1], 0) if len(t) > 1 else None,
        "items": fmt_num(t[2], 0) if len(t) > 2 else None,
        "minutes": fmt_num(t[3], 0) if len(t) > 3 else None,
    }

    # --- KPI
    k = _first(doris.query(_kpi_sql(date)))
    b = _first(doris.query(_buyers_sql(date)))
    health_rows = doris.query(_health_sql())
    health_idx = {r[0]: r for r in health_rows if len(r) >= 6}
    p99 = fmt_num(health_idx.get("e2e_latency_ms", [None, None, None])[2], 0)
    cancel_ratio = fmt_num(health_idx.get("canceled_ratio", [None, None, None])[2], 2)
    late_ratio = fmt_num(health_idx.get("late_ratio", [None, None, None])[2], 2)

    kpi = {
        "gmv": fmt_num(k[0] if len(k) > 0 else None, 2),
        "orders": fmt_num(k[1] if len(k) > 1 else None, 0),
        "items": fmt_num(k[2] if len(k) > 2 else None, 0),
        "aov": fmt_num(k[3] if len(k) > 3 else None, 2),
        "minutes": fmt_num(k[4] if len(k) > 4 else None, 0),
        "buyers": fmt_num(b[0] if len(b) > 0 else None, 0),
        "late_ratio": late_ratio,
        "canceled_ratio": cancel_ratio,
        "p99_ms": p99,
        "prev_gmv": prev_gmv,
        "prev_orders": prev_orders,
    }

    # --- 趋势
    trend_rows = doris.query(_trend_sql(date, prev))
    today_pts, prev_pts = [], []
    for r in trend_rows:
        if len(r) < 3:
            continue
        pt = [r[1], fmt_num(r[2], 2)]
        (today_pts if r[0] == "today" else prev_pts).append(pt)

    # --- 品类
    cat = []
    for r in doris.query(_category_sql(date)):
        if len(r) < 5:
            continue
        raw = r[0]
        cat.append({
            "name": CATEGORY_CN.get(raw, raw.replace("_", " ")),
            "raw": raw,
            "rank": fmt_num(r[1], 0),
            "gmv": fmt_num(r[2], 2),
            "orders": fmt_num(r[3], 0),
            "ratio": fmt_num(r[4], 4),
        })

    # --- 支付方式（5 列：type / cnt / amount / amt_ratio / cnt_ratio）
    pay = []
    for r in doris.query(_payment_sql(date)):
        if len(r) < 5:
            continue
        raw = r[0]
        pay.append({
            "type": PAYMENT_CN.get(raw, raw),
            "raw": raw,
            "cnt": fmt_num(r[1], 0),
            "amount": fmt_num(r[2], 2),
            "amount_ratio": fmt_num(r[3], 4),
            "cnt_ratio": fmt_num(r[4], 4),
        })

    # --- 地域
    geo = []
    for r in doris.query(_map_sql(date)):
        if len(r) < 8:
            continue
        geo.append({
            "uf": r[0],
            "name": r[1],
            "gmv": fmt_num(r[2], 2),
            "ratio": fmt_num(r[3], 4),
            "rank": fmt_num(r[4], 0),
            "orders": fmt_num(r[5], 0),
            "lat": fmt_num(r[6], 4),
            "lng": fmt_num(r[7], 4),
        })

    # --- 订单流水
    orders = []
    for r in doris.query(_orders_sql(date)):
        if len(r) < 7:
            continue
        orders.append({
            "id": r[0],
            "short": r[0][:8].upper(),
            "time": (r[1] or "")[11:19] or (r[1] or ""),
            "status": ORDER_STATUS_CN.get(r[2], r[2]),
            "status_raw": r[2],
            "uf": r[3],
            "city": (r[4] or "").title(),
            "items": fmt_num(r[5], 0),
            "gmv": fmt_num(r[6], 2),
        })

    # --- 漏斗
    funnel = []
    for r in doris.query(_funnel_sql()):
        if len(r) < 8:
            continue
        funnel.append({
            "step": r[1],
            "label": FUNNEL_CN.get(r[1], r[1]),
            "pv": fmt_num(r[3], 0),
            "uv": fmt_num(r[4], 0),
            "sessions": fmt_num(r[5], 0),
            "conv_prev": fmt_num(r[6], 4),
            "conv_first": fmt_num(r[7], 4),
        })

    # --- 链路健康度
    health = []
    for r in health_rows:
        if len(r) < 6:
            continue
        health.append({
            "key": r[0],
            "name": r[1],
            "value": fmt_num(r[2], 2) if r[2] not in ("", None) else None,
            "unit": r[3],
            "level": r[4],
            "group": r[5],
            # 阈值说明必须传出去：像 e2e_latency_ms 这种「单次实测值当常量用」的
            # 指标，只有这一句能说清它不是 P99。前端拿它做 title 提示。
            "desc": r[6] if len(r) > 6 else "",
        })

    return {
        "meta": {
            "data_date": date,
            "prev_date": prev,
            "latest_trade_date": meta_cache.get("latest_trade"),
            "funnel_date": funnel_date,
        },
        "kpi": kpi,
        "today": today_live,
        "trend": {"today": today_pts, "prev": prev_pts},
        "category": cat,
        "payment": pay,
        "map": geo,
        "orders": orders,
        "funnel": funnel,
        "health": health,
    }
