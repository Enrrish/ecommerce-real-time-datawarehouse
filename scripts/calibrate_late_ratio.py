#!/usr/bin/env python3
"""calibrate_late_ratio.py —— 给 `late_ratio` 做正向对照（注入已知比例的迟到事件）

## 为什么需要这个脚本

`late_ratio` 在 P8 被改成了「只判定近 10 分钟的事件」。理由见
`collect-pipeline-health.sh` 里那一段：采样窗口混入批量回填时，整批回填事件都会
被判成乱序，实测注入 5% 却报出 66.67%。

但改完之后有个必须回答的问题：**它会不会把信号一起滤掉了，变成一条永远 0 的指标？**
「过滤后测出来是 0」这件事本身不能证明指标是好的 —— 一个被写死的 0 也会是 0。
所以要有正向对照：发一批**近期**事件，其中固定比例故意迟到，看指标能不能如实报出。

预期的报出值 = 注入的迟到条数 / 窗口内近期事件总数。分母会包含同时段的实时流量，
所以报出值通常**略低于**注入比例，这是正常稀释，不是错误。

## 用法

    python scripts/calibrate_late_ratio.py                 # 1500 条，20% 迟到 5 分钟
    python scripts/calibrate_late_ratio.py -n 3000 -p 0.1  # 3000 条，10% 迟到
    python scripts/calibrate_late_ratio.py --dry-run       # 只算不发

发完之后跑一次采集，读 `late_ratio` 那一行：

    bash scripts/collect-pipeline-health.sh
    #     抽样 3000 条（3 分区各 1000）：…，乱序 13.6986%（参与判定 2117 条，回填 883 条已排除）
    # quality  late_ratio  事件乱序比例  13.6986  %  warn

## 为什么用 `probe_latency` 这个 event_type

它不在 DWD 的事件类型白名单里 → `is_valid = 0` → 不进漏斗（DWS 带 `WHERE is_valid = 1`），
所以标定用的几百条事件**不会污染任何业务指标**，只在贴源层多几百行可识别的探针。
这和 `check_lag.py` 的做法是同一个理由。

## 为什么用容器的 kafka-console-producer

宿主机没有 kafka-python（离线装不上），而每条都起一个 `docker exec` 的话，光 JVM
启动就 1~2 秒。常驻一个 producer、之后逐行喂 stdin，1500 条约 1 秒发完 —— 只有发得够快，
这批事件才会**连续占据**采样窗口，扰动才可控。
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import subprocess
import sys
import time
import uuid

KAFKA_CONTAINER = "rt-kafka"
TOPIC = "user_event"
PROBE_TYPE = "probe_latency"


def build_producer():
    env = dict(os.environ, MSYS_NO_PATHCONV="1")   # 别让 Git Bash 改写 /opt/kafka/...
    proc = subprocess.Popen(
        [
            "docker", "exec", "-i", KAFKA_CONTAINER,
            "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server", "kafka:9092",
            "--topic", TOPIC,
            "--producer-property", "linger.ms=0",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
        bufsize=0,
    )
    time.sleep(1.5)          # 等 JVM 起来并完成 metadata 拉取
    return proc


def make_event(seq: int, t: dt.datetime) -> dict:
    """字段与真实埋点同构，只有 event_type 不同。

    schema_version 必须是 int。踩过这个坑：写成字符串 "1.0" 会让 Flink 的 JSON 解析
    抛 JsonParseException，每条探针都变成毒消息，把三个埋点作业一起打进无限重启。
    """
    return {
        "schema_version": 1,
        "event_id": f"calib-{int(time.time() * 1000)}-{seq:05d}-{uuid.uuid4().hex[:6]}",
        "event_time": t.strftime("%Y-%m-%d %H:%M:%S.") + f"{t.microsecond // 1000:03d}",
        "event_type": PROBE_TYPE,
        "session_id": f"calib-session-{seq}",
        "user_id": "calib-user",
        "device": "mobile",
        "channel": "organic",
        "province": "SP",
        "city": "Sao Paulo",
        "product_id": f"p{seq % 50}",
        "category": "bed_bath_table",
        "price": 100.0,
        "quantity": 1,
        "search_keyword": None,
        "referrer": None,
        "app_version": "2.0.0",
        "os": "Android",
        "is_new_user": 0,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="late_ratio 正向对照：注入已知比例的迟到事件")
    ap.add_argument("-n", "--events", type=int, default=1500, help="事件总数（默认 1500）")
    ap.add_argument("-p", "--late-pct", type=float, default=0.20,
                    help="迟到比例 0~1（默认 0.20）")
    ap.add_argument("--late-sec", type=int, default=300,
                    help="迟到多少秒（必须 > 60，判据是落后本分区最大值 >60s；默认 300）")
    ap.add_argument("--recent-sec", type=int, default=50,
                    help="非迟到事件的时间散布范围（默认 50s，必须 < 600 的 HORIZON）")
    ap.add_argument("--dry-run", action="store_true", help="只算不发")
    args = ap.parse_args()

    if args.late_sec <= 60:
        print("--late-sec 必须 > 60（判据是落后 >60s）", file=sys.stderr)
        return 2
    if args.recent_sec >= 600:
        print("--recent-sec 必须 < 600（HORIZON 之外的事件会被排除，标定就没有意义）",
              file=sys.stderr)
        return 2

    now = dt.datetime.now()
    random.seed(7)
    events, n_late = [], 0
    for i in range(args.events):
        if random.random() < args.late_pct:
            back, n_late = args.late_sec, n_late + 1
        else:
            back = random.randint(0, args.recent_sec)
        t = now - dt.timedelta(seconds=back, milliseconds=random.randint(0, 999))
        events.append(make_event(i, t))

    print(f"准备 {len(events)} 条：迟到 {n_late} 条（{n_late * 100 / len(events):.1f}%），"
          f"其余散布在最近 {args.recent_sec}s")
    if args.dry_run:
        print("--dry-run：不发送")
        return 0

    proc = build_producer()
    assert proc.stdin
    t0 = time.time()
    for ev in events:
        proc.stdin.write((json.dumps(ev, ensure_ascii=False) + "\n").encode("utf-8"))
        # 不 flush 会躺在 Python 的 8KB 缓冲里，这批事件就不再连续，标定会被打散
        proc.stdin.flush()
    dt_s = time.time() - t0
    proc.stdin.close()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()

    print(f"已发送 {len(events)} 条，耗时 {dt_s:.1f}s（{len(events) / max(dt_s, 1e-3):.0f} 条/秒）")
    print()
    print("下一步：等 10~20 秒让作业消费完，然后跑")
    print("  bash scripts/collect-pipeline-health.sh")
    print(f"预期 late_ratio ≈ {n_late}/{len(events)} = {n_late * 100 / len(events):.2f}%，")
    print("实际会略低 —— 分母里还有同时段的实时流量，那部分通常不乱序。")
    print()
    print("注意：这批事件在 event_time 起算的 10 分钟内都算「近期」，所以指标会维持偏高")
    print("一段时间；超过 10 分钟后它们变成「回填」被自动排除，指标回落。这是设计行为。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
