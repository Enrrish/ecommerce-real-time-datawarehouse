"""实时大屏的查询代理 + ADS 刷新调度。

为什么需要一个代理进程，而不是纯静态页 + 前端直连：
    浏览器的 JS 发不出 MySQL 协议，也连不上 9030。代理只做两件事 ——
    把 SQL 结果转成 JSON、按固定节奏把 ADS 层重刷一遍。没有业务逻辑。

为什么需要「刷新调度」：
    ADS 是 Doris 侧的 INSERT OVERWRITE 物化的，不是 Flink 写入的（见 README 架构图）。
    没有调度器，ADS 就是一张死快照 —— 埋点数据一直在变，大屏却永远显示旧值。
    这个进程补上了 P6 里刻意留白的那一环（当时说「Doris 内部不做层间加工」）。

四档刷新节奏，来源是实测：
    today    1 s   只刷 overview 的「今天」那几十行，走 INSERT INTO 按主键覆盖。
                   P7 验收要求「改一行 MySQL，5 秒内大屏变化」，而 full 档 60 s
                   一轮等不起 —— 全量 8.8 万行里今天只有零星几行，没必要为了
                   几行把整表重算一遍。链路预算：07 作业 flush 1 s + 本档 1 s
                   + 前端轮询 1.5 s + 取数 0.3 s ≈ 3.8 s 最坏值。
    funnel   2 s   唯一会实时变的面板（埋点链路是活的），单表 8 行，刷一次 <0.5 s
    health  15 s   要跨 Flink / Kafka / Doris 三处采集，单次约 3 s，太快会互相追尾
    full    60 s   overview 8.8 万行 + order_stream 9.9 万行，全量刷一次 2.6 s
                   交易数据是历史回放，本来就不变，没必要高频刷。它同时负责
                   纠正 today 档漏掉的「行消失」场景（INSERT INTO 只覆盖不删除）
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import traceback
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent              # dashboard/
PROJECT = ROOT.parent           # 项目根

sys.path.insert(0, str(HERE))

from doris import Doris, DorisError  # noqa: E402
import queries                        # noqa: E402

DORIS = Doris()          # 面板查询专用
DORIS_W = Doris()        # 刷新专用：与查询分开两条连接，否则 200ms 的 INSERT
                         # 会把同一时刻的页面请求堵在锁上，图表看着一卡一卡的
META_CACHE: dict = {}
CACHE_LOCK = threading.Lock()

# 最近一次成功取数的时间戳与大屏轮询次数，用于 /health 自检
STATS = {"last_ok": 0.0, "served": 0, "errors": 0, "last_error": ""}
REFRESH_STATE = {"funnel": 0.0, "health": 0.0, "full": 0.0, "today": 0.0,
                 "funnel_err": 0, "today_err": 0}

# full 档是 INSERT OVERWRITE（先清表再写），与 today 档的 INSERT INTO 并发跑时，
# 后者的行可能正好落在「已清空、未写完」的窗口里被吞掉，表现为数字偶尔倒退。
# 两档互斥后，today 最多被一次全量刷新阻塞 2.6 s。
REFRESH_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# 刷新任务
# ---------------------------------------------------------------------------
FUNNEL_REFRESH_SQL = """
INSERT OVERWRITE TABLE `ads_funnel_1d`
WITH `daily` AS (
  SELECT DATE(`event_time`) AS `dt`, `event_type` AS `step`,
         COUNT(*) AS `pv`, COUNT(DISTINCT `user_id`) AS `uv`,
         COUNT(DISTINCT `session_id`) AS `session_cnt`
  FROM `realtime_dw`.`dwd_user_event_clean`
  WHERE `is_valid` = 1
  GROUP BY DATE(`event_time`), `event_type`
),
`ordered` AS (
  SELECT `dt`, `step`, `pv`, `uv`, `session_cnt`,
         CASE `step` WHEN 'page_view' THEN 1 WHEN 'add_cart' THEN 2
                     WHEN 'checkout' THEN 3 WHEN 'pay' THEN 4 ELSE 9 END AS `step_order`
  FROM `daily`
)
SELECT o.`dt`, o.`step`, o.`step_order`, o.`pv`, o.`uv`, o.`session_cnt`,
       CAST(o.`uv` AS DECIMAL(20,6))
         / NULLIF(LAG(o.`uv`) OVER (PARTITION BY o.`dt` ORDER BY o.`step_order`), 0),
       CAST(o.`uv` AS DECIMAL(20,6)) / NULLIF(f.`uv`, 0),
       NOW()
FROM `ordered` o
LEFT JOIN (SELECT `dt`, `uv` FROM `ordered` WHERE `step_order` = 1) f ON o.`dt` = f.`dt`
"""


# 注意：Doris 的 INSERT INTO 后面**不能**跟 TABLE 关键字（只有
# INSERT OVERWRITE TABLE 能跟）。写成 INSERT INTO TABLE 会报
# 「Encountered: TABLE | Expected: TABLE is keyword」，报错信息自相矛盾。
TODAY_REFRESH_SQL = """
INSERT INTO `ads_realtime_overview`
SELECT
  `dt`, `minute`, `gmv`, `order_cnt`, `item_cnt`, `buyer_cnt`, `pay_user_cnt`,
  `freight_total`,
  CAST(`gmv` AS DECIMAL(20,6)) / NULLIF(`order_cnt`, 0),
  CAST(`gmv` AS DECIMAL(20,6)) / NULLIF(`item_cnt`, 0),
  CAST(`freight_total` AS DECIMAL(20,6)) / NULLIF(`gmv`, 0),
  SUM(`gmv`)       OVER (PARTITION BY `dt` ORDER BY `minute`
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
  SUM(`order_cnt`) OVER (PARTITION BY `dt` ORDER BY `minute`
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
  NOW()
FROM `dws_trade_overview_1m`
WHERE `gmv` IS NOT NULL AND `dt` = CURDATE()
"""


def _run_today_refresh() -> None:
    """只覆盖「今天」那几行。UNIQUE 表上 INSERT INTO 就是按 (dt, minute) 覆盖，
    行数少所以是毫秒级；历史 8.8 万行交给 full 档 60 s 一次兜底。"""
    with REFRESH_LOCK:
        DORIS_W.query(TODAY_REFRESH_SQL)


def _run_full_refresh() -> None:
    with REFRESH_LOCK:
        _run_sql_file("sql/doris/08-ads-refresh.sql")


def _run_sql_file(rel: str) -> None:
    env = dict(os.environ, MSYS_NO_PATHCONV="1")
    path = PROJECT / rel
    with open(path, "rb") as fh:
        res = subprocess.run(
            ["docker", "exec", "-i", "rt-doris-fe", "mysql", "-uroot", "-P9030", "-h127.0.0.1"],
            stdin=fh, capture_output=True, env=env, timeout=180,
        )
    if res.returncode != 0:
        tail = res.stderr.decode("utf-8", "replace").strip().splitlines()[-3:]
        raise RuntimeError(f"{rel} 退出码 {res.returncode}: {' / '.join(tail)}")


def _bash_exe() -> str:
    """Git Bash 的绝对路径。

    为什么不能直接 spawn "bash"：Windows 的 PATH 里还有两个同名程序 ——
    System32\\bash.exe（WSL 启动器）和 WindowsApps 里的应用别名。从 Python
    里按裸名字调用时命中哪个是不确定的，命中 WSL 就是一句中文报错 + 退出码 1，
    子进程根本没有跑到脚本。这个坑让健康度面板整整一天都在显示昨天的快照，
    因为 subprocess.run 的返回码没人看。

    固定走绝对路径；可用 BASH_EXE 覆盖。
    """
    bad = ("\\windows\\system32\\", "\\windowsapps\\", "\\wsl")
    cand = [
        os.environ.get("BASH_EXE", ""),
        shutil.which("bash") or "",
        r"C:\Program Files\Git\bin\bash.exe",
        r"C:\Program Files (x86)\Git\bin\bash.exe",
        r"C:\Program Files\Git\usr\bin\bash.exe",
    ]
    for p in cand:
        if p and Path(p).is_file() and not any(b in p.lower() for b in bad):
            return p
    raise RuntimeError("找不到 Git Bash；设 BASH_EXE 指向 bash.exe")


def _run_health_collect() -> None:
    env = dict(os.environ, MSYS_NO_PATHCONV="1")
    res = subprocess.run(
        [_bash_exe(), str(PROJECT / "scripts" / "collect-pipeline-health.sh")],
        cwd=str(PROJECT), capture_output=True, text=True,
        encoding="utf-8", errors="replace", env=env, timeout=120,
    )
    if res.returncode != 0:
        # 以前这里是 DEVNULL + 不检查返回码，脚本挂了也一声不响，
        # 结果 ads_pipeline_health 停在旧快照上，面板「看着有数，其实是昨天的」。
        tail = (res.stderr or res.stdout).strip().splitlines()[-3:]
        raise RuntimeError(f"健康度采集退出码 {res.returncode}: {' / '.join(tail)}")


def _loop(name: str, period: float, fn) -> None:
    err_key = f"{name}_err"
    while True:
        t0 = time.time()
        try:
            fn()
            REFRESH_STATE[name] = time.time()
            if err_key in REFRESH_STATE:
                REFRESH_STATE[err_key] = 0
        except Exception:                       # noqa: BLE001 - 守护线程不能死
            if err_key in REFRESH_STATE:
                REFRESH_STATE[err_key] += 1
            traceback.print_exc()
        time.sleep(max(0.5, period - (time.time() - t0)))


def start_refreshers() -> None:
    threading.Thread(
        target=_loop, args=("today", 1.0, _run_today_refresh),
        daemon=True, name="refresh-today",
    ).start()
    threading.Thread(
        target=_loop, args=("funnel", 2.0, lambda: DORIS_W.query(FUNNEL_REFRESH_SQL)),
        daemon=True, name="refresh-funnel",
    ).start()
    threading.Thread(
        target=_loop, args=("health", 15.0, _run_health_collect),
        daemon=True, name="refresh-health",
    ).start()
    threading.Thread(
        target=_loop, args=("full", 60.0, _run_full_refresh),
        daemon=True, name="refresh-full",
    ).start()


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(ROOT), **kw)

    def log_message(self, fmt, *args):          # 静音：每 2 秒一次日志会淹掉终端
        if "/api/" in (self.path or ""):
            return

    def _json(self, payload: dict, code: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):                            # noqa: N802 - 标准库命名
        if self.path.startswith("/api/dashboard"):
            return self._dashboard()
        if self.path.startswith("/api/ping"):
            return self._json({"ok": True, "ts": time.time()})
        if self.path.startswith("/health"):
            return self._json(self._health())
        return super().do_GET()

    def _dashboard(self) -> None:
        t0 = time.time()
        try:
            with CACHE_LOCK:
                data = queries.collect(DORIS, META_CACHE)
            data["meta"]["query_ms"] = int((time.time() - t0) * 1000)
            data["meta"]["server_ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
            data["meta"]["refresh"] = {
                k: int(time.time() - v) if v else None
                for k, v in REFRESH_STATE.items() if not k.endswith("_err")
            }
            STATS["last_ok"] = time.time()
            STATS["served"] += 1
            self._json(data)
        except DorisError as exc:
            STATS["errors"] += 1
            STATS["last_error"] = str(exc)
            self._json({"error": "doris_unavailable", "detail": str(exc)}, 503)
        except Exception as exc:                 # noqa: BLE001
            STATS["errors"] += 1
            STATS["last_error"] = str(exc)
            traceback.print_exc()
            self._json({"error": "internal", "detail": str(exc)}, 500)

    def _health(self) -> dict:
        return {
            "ok": STATS["errors"] == 0 or time.time() - STATS["last_ok"] < 10,
            "served": STATS["served"],
            "errors": STATS["errors"],
            "last_error": STATS["last_error"],
            "seconds_since_ok": round(time.time() - STATS["last_ok"], 1) if STATS["last_ok"] else None,
            "refresh_lag": {k: round(time.time() - v, 1) if v else None
                            for k, v in REFRESH_STATE.items() if not k.endswith("_err")},
        }


def main() -> None:
    ap = argparse.ArgumentParser(description="Olist 实时大屏查询代理")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--no-refresh", action="store_true", help="只开查询，不跑 ADS 刷新（调试用）")
    args = ap.parse_args()

    # 先探一次，Doris 没起来就直接报错，别让页面白屏到用户来找原因
    try:
        row = DORIS.query("SELECT 1")
        print(f"[doris] 连接正常 {row}")
    except Exception as exc:                     # noqa: BLE001
        print(f"[doris] 连接失败：{exc}", file=sys.stderr)
        print("        先确认容器在跑：docker ps | grep doris-fe", file=sys.stderr)
        sys.exit(1)

    if not args.no_refresh:
        start_refreshers()
        print("[refresh] funnel 1.5s / health 15s / full 60s")
    else:
        print("[refresh] 已关闭（--no-refresh）")

    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    srv.daemon_threads = True
    print(f"[http]    http://{args.host}:{args.port}/  （Ctrl+C 停止）")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[exit] 停止")
    finally:
        DORIS.close()
        DORIS_W.close()
        srv.server_close()


if __name__ == "__main__":
    main()
