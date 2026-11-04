#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""check_lag.py — 端到端延迟真采样，输出 P50 / P95 / P99。

## 为什么需要它

大屏健康度里的 `e2e_latency_ms` 原来是一个常量 3849 —— P3 阶段手工掐表测了**一次**，
就写死进 `collect-pipeline-health.sh`。这不是测量，是引用了测量。

单次采样的问题不在于「可能不准」，而在于**无法改进**：优化之后这个数字不会动，
劣化之后也不会动。P7 排查刷新延迟时它就完全帮不上忙。

## 怎么测

往 Kafka 的 `user_event` 打一条探针事件，然后轮询 Doris 直到它出现：

    t0 = 交给 producer 的时刻（time.monotonic）
    t1 = 在 ods_user_event 里查到 event_id 的时刻
    latency = t1 - t0

探针的 `event_type` 用 `probe_latency`，不在 DWD 的白名单里 →
`dwd_user_event_clean.is_valid = 0` → 不进入漏斗统计（`WHERE is_valid = 1`）。
所以这个测量**不会污染任何业务指标**，只会在贴源层多几百行可识别的探针。

## 三条设计约束（都是踩坑得来的）

1. **不能往业务表插测试订单**。docs/03 里写死了这条：GMV 是全量累计口径，
   插一行就永久多一行，减不掉（Doris 纯 upsert 没有 retract）。所以探针只能走埋点流。

2. **轮询必须用常驻管道**。`docker exec` 单次 826ms，比被测对象还慢 ——
   用它轮询等于用秒表量秒表。这里的做法是复用 `dashboard/server/doris.py`
   里那条 stdin/stdout 复用连接（12~26ms/次），不抄第二遍。

3. **采样要并发在途，不能一条一测**。一条一测 = 每条等 5~10s（sink flush 间隔），
   200 条要半小时。这里每 0.5s 发一条探针、每 100ms 批量查一次未到的事件，
   200 条约 2 分钟。

## 已知偏差

* 轮询粒度 100ms → 每个样本有 +0~100ms 的量化误差（中位数影响约 +50ms）
* sink 的 `buffer-flush.interval = 10s` 是**架构下限**：数据攒在 connector 里
  最多 10 秒才发出去，任何测量都绕不过它。所以 P99 在 10s 附近是正常的，
  不是链路慢；想让它变小只能改 flush 间隔，而 1s 会把 Doris 版本打爆（见 docs/05）。

## 用法

    python scripts/check_lag.py                    # 200 条，测 ods + dwd
    python scripts/check_lag.py --samples 60       # 快速跑一轮
    python scripts/check_lag.py --chain ods        # 只测 ODS（Kafka→ODS 落湖）
    python scripts/check_lag.py --write            # 结果写回 ads_pipeline_health
    python scripts/check_lag.py --keep             # 保留探针行（默认删除）
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dashboard" / "server"))
from doris import Doris, DorisError  # noqa: E402  (路径注入后才能导入)

KAFKA_CONTAINER = "rt-kafka"
TOPIC = "user_event"
PROBE_PREFIX = "probe-"
PROBE_TYPE = "probe_latency"
SENTINEL_ID = "__NO_SUCH_EVENT__"


# ---------------------------------------------------------------------------
# Kafka 生产端：常驻一个 console-producer，逐行喂
# ---------------------------------------------------------------------------
class KafkaProducer:
    """用容器里的 kafka-console-producer.sh 当生产端。

    选它的理由和选常驻 Doris 管道一样：宿主机没有 kafka-python（离线装不上），
    而每次 `docker exec` 单独起一个 producer，光是 JVM 启动就要 1~2 秒，
    足够把测量结果污染到没意义。常驻一个进程，之后每条只需写一行 stdin。
    """

    def __init__(self) -> None:
        env = dict(os.environ)
        env["MSYS_NO_PATHCONV"] = "1"     # 别让 Git Bash 改写 /opt/kafka/...
        self._proc = subprocess.Popen(
            [
                "docker", "exec", "-i", KAFKA_CONTAINER,
                "/opt/kafka/bin/kafka-console-producer.sh",
                "--bootstrap-server", "kafka:9092",
                "--topic", TOPIC,
                # linger.ms=0：不让 producer 侧攒批，否则测出来的是「攒批策略」
                # 而不是「链路延迟」。默认是 0，但写成显式参数免得日后被改。
                "--producer-property", "linger.ms=0",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
            bufsize=0,
        )
        time.sleep(1.5)                   # 等 JVM 起来并完成 metadata 拉取

    def send(self, payload: dict) -> None:
        assert self._proc.stdin
        line = json.dumps(payload, ensure_ascii=False) + "\n"
        self._proc.stdin.write(line.encode("utf-8"))
        # 不 flush 的话这行会躺在 Python 的 8KB 缓冲区里，测得的是「本地缓冲延迟」
        self._proc.stdin.flush()

    def close(self) -> None:
        try:
            if self._proc.stdin:
                self._proc.stdin.close()
            self._proc.wait(timeout=5)
        except Exception:                 # noqa: BLE001 - 关不掉也不影响结论
            self._proc.kill()


def make_probe(seq: int, now: dt.datetime) -> dict:
    """一条探针事件。字段与真实埋点同构，只有 event_type 是新的。"""
    eid = f"{PROBE_PREFIX}{int(now.timestamp() * 1000)}-{seq:04d}-{uuid.uuid4().hex[:6]}"
    return {
        # 必须和 gen_user_events.py 的 SCHEMA_VERSION 同类型（int，不是 "1.0"）。
        # 实测踩过：写成字符串 "1.0" 会让 Flink 的 JSON 解析在 schema_version 上抛
        # JsonParseException —— 每一条探针都变成毒消息，把 ODS/DWD/漏斗三个作业
        # 一起打进无限重启。字段类型这件事，测的是链路，先得保证探针本身合法。
        "schema_version": 1,
        "event_id": eid,
        "event_time": now.strftime("%Y-%m-%d %H:%M:%S.") + f"{now.microsecond // 1000:03d}",
        "event_type": PROBE_TYPE,
        # 每条探针独立会话：Kafka 按 session_id 分区，独立 key 才能把探针打散到 3 个分区，
        # 否则 200 条全挤在一个分区，测的是单分区吞吐不是链路延迟。
        "session_id": f"probe-session-{seq}",
        "user_id": "probe-user",
        "product_id": "probe-product",
        "page_url": "/probe",
    }


# ---------------------------------------------------------------------------
# 百分位
# ---------------------------------------------------------------------------
def percentile(sorted_vals: list[float], q: float) -> float:
    """线性插值百分位（与 numpy.percentile 的默认口径一致）。

    样本量 200 时 p99 落在第 198 和第 199 个之间插值 —— 也就是说它由 2 个样本决定。
    这就是为什么 --samples 默认给 200 而不是 20：20 个样本的 p99 只是一个最大值。
    """
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = (len(sorted_vals) - 1) * q / 100.0
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def summarize(latencies: list[float], timeouts: int = 0) -> dict:
    vals = sorted(latencies)
    if not vals:
        return {"samples": 0, "timeouts": timeouts}
    return {
        "samples": len(vals),
        "timeouts": timeouts,
        "p50": round(percentile(vals, 50), 1),
        "p95": round(percentile(vals, 95), 1),
        "p99": round(percentile(vals, 99), 1),
        "max": round(vals[-1], 1),
        "min": round(vals[0], 1),
    }


# ---------------------------------------------------------------------------
# 采样主循环
# ---------------------------------------------------------------------------
def warmup(doris: Doris, producer: KafkaProducer, table: str, timeout: float = 40.0) -> bool:
    """先发一条不计入统计的探针，等到它可见为止。

    为什么必须有这一步：console-producer 是个 JVM，启动到能真正 send 要 1~3 秒
    （拉 metadata、建连接）。跳过 warmup 时，前几条探针的时间戳是在「JVM 还没
    就绪」时打的，它们会把 P99 抬高好几秒 —— 而这个偏差只出现在样本头部，
    百分比统计完全看不出异常。同理，Doris 那条常驻管道第一次查询也要握手。
    """
    probe = make_probe(0, dt.datetime.now())
    producer.send(probe)
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            rows = doris.query(
                f"SELECT event_id FROM {table} WHERE event_id = '{probe['event_id']}'")
            if rows:
                print(f"    warmup 就位（{time.monotonic() - t0:.1f}s）"
                      f"—— 这一段不计入统计")
                return True
        except DorisError as exc:
            print(f"    [warn] warmup 查询失败：{exc}", file=sys.stderr)
        time.sleep(0.3)
    print(f"    [warn] warmup {timeout:.0f}s 内没看到探针，直接开始采样"
          f"（结果可能偏高）", file=sys.stderr)
    return False


def sample_table(doris: Doris, producer: KafkaProducer, table: str,
                 samples: int, produce_interval: float, poll: float,
                 timeout: float) -> dict:
    """对一张目标表做采样：定时发探针 + 批量轮询可见性。"""
    pending: dict[str, float] = {}
    latencies: list[float] = []
    timeouts = 0
    sent = 0
    next_send = time.monotonic()

    def visible(event_ids: list[str]) -> set[str]:
        """一次查出这批 event_id 里已经落到目标表的那些。"""
        if not event_ids:
            return set()
        quoted = ",".join(f"'{e}'" for e in event_ids)
        rows = doris.query(f"SELECT event_id FROM {table} WHERE event_id IN ({quoted})")
        return {r[0] for r in rows if r}

    while sent < samples or pending:
        now = time.monotonic()

        # ---- 发探针 ----
        if sent < samples and now >= next_send:
            probe = make_probe(sent, dt.datetime.now())
            producer.send(probe)
            pending[probe["event_id"]] = time.monotonic()
            sent += 1
            next_send = time.monotonic() + produce_interval

        # ---- 查可见性 ----
        if pending:
            ids = list(pending.keys())[:60]        # 一次最多 60 个 IN 条件
            try:
                seen = visible(ids)
            except DorisError as exc:
                print(f"    [warn] 查询失败，重建通道后重试：{exc}", file=sys.stderr)
                seen = set()
            now = time.monotonic()
            for eid in seen:
                if eid in pending:
                    latencies.append((now - pending.pop(eid)) * 1000.0)
            # 超时未到：计入 timeouts，不参与百分位
            for eid, t0 in list(pending.items()):
                if now - t0 > timeout:
                    pending.pop(eid)
                    timeouts += 1

        time.sleep(poll)

    return summarize(latencies, timeouts)


def cleanup(doris: Doris, tables: list[str]) -> None:
    """删掉本次（及历史）探针行，让贴源层回到干净状态。

    Doris 的 DELETE 是异步的：语句返回只代表「删除任务已提交」，
    真正的 compaction 在后台跑。所以这里不校验删除后的行数 ——
    刚提交就 SELECT COUNT 会看到旧数据，误判成没删掉。
    """
    for table in tables:
        try:
            doris.query(f"DELETE FROM {table} WHERE event_id LIKE '{PROBE_PREFIX}%'")
            print(f"    已提交清理：{table}")
        except DorisError as exc:
            print(f"    [warn] 清理 {table} 失败：{exc}", file=sys.stderr)


def write_health(doris: Doris | None, result: dict, chain: str) -> None:
    """把 P99 写回 ads_pipeline_health，并留一份 json 给采集脚本读。"""
    p99 = int(round(result.get("p99", 0)))
    # 阈值跟着架构走：sink 的 flush 间隔是 10s，任何测量都不可能低于它。
    # 旧阈值 5000 是按「P3 单次 3849ms」定的，那条基线本身就没算 flush 等待。
    level = "critical" if p99 >= 20000 else ("warn" if p99 >= 12000 else "ok")
    desc = (f"{chain} 链路实测 P99（N={result.get('samples', 0)}，"
            f"produce→Doris 可见）。**下限是 sink flush 间隔 10s**，"
            f"<12000 ok / <20000 warn")

    out = ROOT / "scripts" / ".latency.json"
    out.write_text(json.dumps({
        "measured_at": dt.datetime.now().isoformat(timespec="seconds"),
        "chain": chain,
        **result,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"    已写入 {out.relative_to(ROOT)}")

    if doris is None:
        return
    doris.query(f"""
        INSERT INTO ads_pipeline_health
          (metric_key, metric_name, metric_value, metric_unit, metric_level,
           threshold_desc, metric_group, collect_time)
        VALUES
          ('e2e_latency_ms', '端到端延迟', {p99}, 'ms', '{level}',
           '{desc}', 'delay', NOW())
    """)
    print(f"    已写回 ads_pipeline_health：e2e_latency_ms = {p99}（{level}）")


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="端到端延迟真采样（P50/P95/P99）")
    ap.add_argument("--samples", type=int, default=200, help="探针条数，默认 200")
    ap.add_argument("--chain", choices=["both", "ods", "dwd"], default="both",
                    help="测哪条链路：ods=Kafka→ODS，dwd=Kafka→DWD，默认两条都测")
    ap.add_argument("--produce-interval", type=float, default=0.5,
                    help="发包间隔秒，默认 0.5（200 条约 100 秒）")
    ap.add_argument("--poll", type=float, default=0.1, help="轮询间隔秒，默认 0.1")
    ap.add_argument("--timeout", type=float, default=45.0,
                    help="单条超时秒，默认 45（必须大于 flush 间隔 10s）")
    ap.add_argument("--keep", action="store_true", help="保留探针行，不清理")
    ap.add_argument("--write", action="store_true",
                    help="把 P99 写回 ads_pipeline_health 与 scripts/.latency.json")
    args = ap.parse_args()

    targets = []
    if args.chain in ("both", "ods"):
        targets.append(("ods", "ods_user_event"))
    if args.chain in ("both", "dwd"):
        targets.append(("dwd", "dwd_user_event_clean"))

    print(f"==> 探针 {args.samples} 条，间隔 {args.produce_interval}s，"
          f"轮询 {args.poll}s，链路 {'/'.join(t[0] for t in targets)}")
    print("    探针只写 Kafka 与贴源/DWD 明细，event_type=probe_latency 不进漏斗统计")

    doris = Doris()
    producer = KafkaProducer()
    results: dict[str, dict] = {}
    try:
        # 先把 producer 与 Doris 管道都捂热，再开始计时（见 warmup 的注释）
        warmup(doris, producer, targets[0][1])
        for label, table in targets:
            print(f"==> 采样 {label} → {table}")
            t_start = time.monotonic()
            results[label] = sample_table(
                doris, producer, table,
                samples=args.samples,
                produce_interval=args.produce_interval,
                poll=args.poll,
                timeout=args.timeout,
            )
            r = results[label]
            print(f"    {r.get('samples', 0)} 个样本 / {r.get('timeouts', 0)} 个超时，"
                  f"耗时 {time.monotonic() - t_start:.0f}s")
            if r.get("samples"):
                print(f"    P50 {r['p50']:.0f}ms  P95 {r['p95']:.0f}ms  "
                      f"P99 {r['p99']:.0f}ms  max {r['max']:.0f}ms")
    finally:
        producer.close()

    if not args.keep:
        print("==> 清理探针行")
        cleanup(doris, [t for _, t in targets])

    # 写回时用 dwd 链路的 P99：漏斗/大屏读的是 DWD 下游，它才是「看板上看到的延迟」
    primary = results.get("dwd") or results.get("ods") or {}
    if args.write:
        print("==> 写回健康度指标")
        write_health(None if not primary.get("samples") else doris,
                     primary, "dwd" if "dwd" in results else "ods")

    doris.close()

    # 退出码给 CI / 脚本用：有样本且 p99 超过 20s 算失败
    if not primary.get("samples"):
        return 2
    return 1 if primary["p99"] >= 20000 else 0


if __name__ == "__main__":
    sys.exit(main())
