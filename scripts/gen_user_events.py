#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gen_user_events.py —— 合成用户行为埋点日志，写 Kafka

为什么需要合成而不是找现成数据：
  Olist 是交易数据集，只有「结果」（订单、支付、评价），没有任何「过程」。
  而大屏的实时转化漏斗（商详 → 加购 → 提交 → 支付）和「支付转化率」KPI
  必须由行为流算出来。没有埋点，这两块只能写死假数字。

三条设计原则：

1. **实体用真实 ID**。product_id 取自 olist_products，user_id 取自
   customer_unique_id。这样 ods_user_event 才能和 dim_product / dim_user
   真 join 出「品类漏斗」，而不是自己造一套 ID 再自己 join 自己。

2. **漏斗按商品走，不按会话走**。真实电商里「看了 A 加购了 B」是常态，
   但那样算不出「商详→加购」的同商品转化。这里的做法是：每个商品独立
   走一遍 曝光→加购→提交→支付，和 docs/03 §2 的
   「同一用户同一商品分别计入各自环节」口径对齐。

3. **event_time 严格不晚于当前时刻**。会话起点在窗口内随机，后续动作
   按偏移累加；若起点太晚导致末尾越界，就把整个会话往前挪。
   否则 Flink 的 watermark 会把这些「未来数据」判为迟到并丢弃，
   现象是「生产了几万条，漏斗只统计到一半」，极难排查。

用法：
  # 批量灌 2 万条到 Kafka（起容器后跑）
  python scripts/gen_user_events.py --events 20000

  # 持续压测：每秒 30 条，跑 10 分钟
  python scripts/gen_user_events.py --rate 30 --duration 600

  # 只打印到屏幕，不连 Kafka（用来肉眼检查字段）
  python scripts/gen_user_events.py --events 5 --sink stdout

  # P6 幂等验证：混入 3% 的重复事件（event_id 相同）
  python scripts/gen_user_events.py --events 20000 --dup-rate 0.03

依赖：生成部分只用标准库；--sink kafka 需要 kafka-python-ng
      （python -m pip install kafka-python-ng）
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import statistics
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCHEMA_VERSION = 1
TZ = timezone(timedelta(hours=8))  # Asia/Shanghai，容器和宿主机都按这个时区

# 漏斗四步。docs/03 §2：商详 PV / 各环节 UV / 支付转化率 全部基于 event_type
STEPS = ("page_view", "add_cart", "checkout", "pay")


# -----------------------------------------------------------------------------
# 实体字典：从原始 CSV 读，不依赖 MySQL 或 build/ 目录
# -----------------------------------------------------------------------------
def load_products(path: Path) -> tuple[list[str], dict[str, tuple[str, float]]]:
    """返回 (product_id 列表, product_id → (seller_id, 成交均价))。

    价格和卖家都从 order_items 反推：products 表本身没有价格，
    而埋点里的商品价格如果随机编，后面的「加购金额」就对不上任何东西。
    """
    items_csv = path / "olist_order_items_dataset.csv"
    prices: dict[str, list[float]] = defaultdict(list)
    sellers: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    with items_csv.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            pid = row["product_id"]
            try:
                prices[pid].append(float(row["price"]))
            except ValueError:
                pass
            sellers[pid][row["seller_id"]] += 1

    products_csv = path / "olist_products_dataset.csv"
    pid_list: list[str] = []
    with products_csv.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            pid = row["product_id"]
            pid_list.append(pid)

    meta: dict[str, tuple[str, float]] = {}
    for pid in pid_list:
        seller = ""
        if pid in sellers:
            # 一个商品可能被多个卖家卖过，取出现次数最多的那个
            seller = max(sellers[pid].items(), key=lambda kv: kv[1])[0]
        if pid in prices and prices[pid]:
            # 用中位数而不是均值：Olist 里有极端高价会拉偏均值
            price = round(statistics.median(prices[pid]), 2)
        else:
            price = 0.0
        meta[pid] = (seller, price)
    return pid_list, meta


def load_users(path: Path) -> list[str]:
    """自然人 ID（customer_unique_id，96,096 个），不是订单级的 customer_id。

    docs/02 里 dim_user_zip 的粒度就是它：customer_id 与 orders 1:1，
    按它算 UV 会把同一个人重复计数。
    """
    seen: dict[str, None] = {}
    with (path / "olist_customers_dataset.csv").open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            seen[row["customer_unique_id"]] = None
    return list(seen)


# -----------------------------------------------------------------------------
# 会话生成
# -----------------------------------------------------------------------------
def build_session(
    rng: random.Random,
    start: datetime,
    users: list[str],
    products: list[str],
    meta: dict[str, tuple[str, float]],
    p_add: float,
    p_checkout: float,
    p_pay: float,
) -> list[dict]:
    """一个会话：浏览 1~3 个商品，各自独立走漏斗，时间上分阶段推进。"""
    user_id = rng.choice(users)
    session_id = str(uuid.uuid4())
    n_view = rng.choices((1, 2, 3), weights=(55, 32, 13))[0]
    viewed = rng.sample(products, n_view)

    events: list[dict] = []
    t = start

    # 每个环节落在哪个页面上。加购的点击发生在商详页内，所以 URL 仍是商品页；
    # 提交和支付是独立页面，写成 /product/xxx 会让「按 page_url 统计页面 PV」失真。
    PAGE = {
        "page_view": "/product/{pid}",
        "add_cart": "/product/{pid}",
        "checkout": "/checkout",
        "pay": "/pay/result",
    }

    def emit(step: str, pid: str, at: datetime) -> None:
        events.append({
            "schema_version": SCHEMA_VERSION,
            "event_id": str(uuid.uuid4()),
            "event_time": at.strftime("%Y-%m-%d %H:%M:%S.") + f"{at.microsecond // 1000:03d}",
            "event_type": step,
            "user_id": user_id,
            "session_id": session_id,
            "product_id": pid,
            # 排查「明明有 page_view 但 join 不上商品」时，能直接从 URL 看出是哪个商品
            "page_url": PAGE[step].format(pid=pid),
        })

    # 阶段 1：连续浏览。每个商品停留 4~35 秒
    for pid in viewed:
        emit("page_view", pid, t)
        t += timedelta(seconds=rng.randint(4, 35))

    # 阶段 2：逐个商品决定是否加购（同商品独立转化，见文件头原则 2）
    carted = [pid for pid in viewed if rng.random() < p_add]
    for pid in carted:
        t += timedelta(seconds=rng.randint(2, 25))
        emit("add_cart", pid, t)

    # 阶段 3：提交订单。真实场景里一次提交通常只结算一个商品
    checked = [pid for pid in carted if rng.random() < p_checkout][:1]
    for pid in checked:
        t += timedelta(seconds=rng.randint(15, 90))
        emit("checkout", pid, t)

    # 阶段 4：支付。提交到支付之间用户会犹豫，间隔明显更长
    paid = [pid for pid in checked if rng.random() < p_pay]
    for pid in paid:
        t += timedelta(seconds=rng.randint(30, 240))
        emit("pay", pid, t)

    return events


def generate(args) -> list[dict]:
    root = Path(__file__).resolve().parent.parent
    src = Path(args.src) if args.src else root / "data" / "raw" / "olist"

    for name in ("olist_products_dataset.csv", "olist_order_items_dataset.csv", "olist_customers_dataset.csv"):
        if not (src / name).is_file():
            raise SystemExit(f"缺少源文件: {src / name}")

    products, meta = load_products(src)
    users = load_users(src)
    rng = random.Random(args.seed)

    now = datetime.now(TZ).replace(tzinfo=None)
    window_end = now
    window_start = now - timedelta(seconds=args.window)

    # 事件数 → 会话数。不能简单按「平均 2.4 事件/会话」反算：
    # 会话的事件数本身是随机的（1~3 次浏览 + 0~1 加购 + 0~1 提交 + 0~1 支付），
    # 反算会系统性偏少（实测要 20000 条只出 15000 条）。改成边生成边计数，
    # 够了就停，最多超出最后一个会话的几条。
    if args.duration:
        target_events = int(args.rate * args.duration)
    else:
        target_events = args.events

    # 起点随机落在窗口内，但留出 15 分钟给会话自身的动作时长，
    # 保证最后一个事件的 event_time 也不超过 now（见文件头原则 3）
    latest_start = window_end - timedelta(minutes=15)
    if latest_start <= window_start:
        latest_start = window_end
    span = max(0.0, (latest_start - window_start).total_seconds())

    all_events: list[dict] = []
    while len(all_events) < target_events:
        start = window_start + timedelta(seconds=rng.uniform(0, span))
        all_events.extend(
            build_session(rng, start, users, products, meta, args.p_add, args.p_checkout, args.p_pay)
        )

    # 混入重复事件：同一个 event_id 发两次，用来验证下游去重（P6）
    if args.dup_rate > 0:
        n_dup = int(len(all_events) * args.dup_rate)
        all_events.extend(rng.sample(all_events, min(n_dup, len(all_events))))

    all_events.sort(key=lambda e: e["event_time"])
    return all_events


# -----------------------------------------------------------------------------
# 输出
# -----------------------------------------------------------------------------
def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="gen-user-events")
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
        print(f"[kafka] 已创建 topic: {topic}（{partitions} 分区）")
    except TopicAlreadyExistsError:
        print(f"[kafka] topic 已存在: {topic}")
    finally:
        admin.close()


def sink_kafka(events: list[dict], args) -> None:
    from kafka import KafkaProducer

    ensure_topic(args.bootstrap, args.topic, args.partitions)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        # 按 session_id 做 key：同一会话进同一分区，分区内保序。
        # 漏斗按 session 归因（docs/03 §2 归因规则）依赖这个顺序。
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks=1,                # 单节点，acks=all 没有额外收益，只多一次等待
        linger_ms=20,          # 攒 20ms 再发，批量提升吞吐
        retries=3,
    )

    total = len(events)
    t0 = time.monotonic()
    sent = 0
    try:
        for i, ev in enumerate(events):
            if args.rate > 0:
                # 匀速模式：按目标速率对齐，落后就追、超前就等
                target = t0 + (i + 1) / args.rate
                gap = target - time.monotonic()
                if gap > 0:
                    time.sleep(gap)
            producer.send(args.topic, key=ev["session_id"], value=ev)
            sent += 1
            if total >= 2000 and sent % 2000 == 0:
                print(f"\r  已发送 {sent}/{total}", end="", flush=True)
        producer.flush()
    finally:
        producer.close()
    elapsed = time.monotonic() - t0
    print(f"\r  已发送 {sent}/{total}，耗时 {elapsed:.1f}s（{sent / elapsed:.0f} 条/秒）")
    print(f"[kafka] 完成：topic={args.topic} bootstrap={args.bootstrap}")


def sink_stdout(events: list[dict], args) -> None:
    for ev in events:
        print(json.dumps(ev, ensure_ascii=False))


def same_time_guard(events: list[dict]) -> None:
    """断言没有未来时间。宁可在这里报错，也别让 Flink 静默丢数据。"""
    now = datetime.now(TZ).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.999")
    future = [e for e in events if e["event_time"] > now]
    if future:
        raise SystemExit(f"生成了 {len(future)} 条未来时间的事件，已中止（首条: {future[0]['event_time']}）")


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser(description="合成 Olist 用户行为埋点并投递 Kafka")
    ap.add_argument("--sink", choices=("kafka", "stdout"), default="kafka")
    ap.add_argument("--bootstrap", default="localhost:29092",
                    help="宿主机侧地址；容器内客户端应使用 kafka:9092")
    ap.add_argument("--topic", default="user_event")
    ap.add_argument("--partitions", type=int, default=3)
    ap.add_argument("--events", type=int, default=20000, help="事件总数（批量模式）")
    ap.add_argument("--rate", type=float, default=0,
                    help="每秒条数；0 = 不限速。配合 --duration 使用")
    ap.add_argument("--duration", type=int, default=0, help="持续秒数；>0 时按 rate*duration 计算事件数")
    ap.add_argument("--window", type=int, default=3600, help="event_time 铺开的时间窗口（秒）")
    ap.add_argument("--dup-rate", type=float, default=0.0, help="重复事件比例，用于幂等验证")
    ap.add_argument("--p-add", type=float, default=0.18, help="商品级加购率")
    ap.add_argument("--p-checkout", type=float, default=0.35, help="加购到提交的转化率")
    ap.add_argument("--p-pay", type=float, default=0.50, help="提交到支付的转化率")
    ap.add_argument("--seed", type=int, default=None, help="随机种子，便于复现")
    ap.add_argument("--src", default=None, help="原始 CSV 目录，默认 data/raw/olist")
    args = ap.parse_args()

    if args.duration and args.rate <= 0:
        ap.error("--duration 需要配合 --rate > 0")

    t_start = time.monotonic()
    events = generate(args)
    build_sec = time.monotonic() - t_start
    same_time_guard(events)

    steps = {s: 0 for s in STEPS}
    for e in events:
        steps[e["event_type"]] += 1
    total = len(events)
    uniq_users = len({e["user_id"] for e in events})
    uniq_sessions = len({e["session_id"] for e in events})

    print(f"事件 {total} 条 / 会话 {uniq_sessions} / 用户 {uniq_users}（生成耗时 {build_sec:.1f}s）")
    for s in STEPS:
        pct = steps[s] / total * 100
        print(f"  {s:<10}{steps[s]:>8}  {pct:5.1f}%")
    if steps["page_view"]:
        print(f"  支付转化率（支付 UV / 商详 PV UV）≈ "
              f"{steps['pay'] / steps['page_view'] * 100:.2f}%")
    if args.dup_rate:
        print(f"  含重复事件 {int(total * args.dup_rate / (1 + args.dup_rate))} 条（dup-rate={args.dup_rate}）")

    if args.sink == "kafka":
        sink_kafka(events, args)
    else:
        sink_stdout(events, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
