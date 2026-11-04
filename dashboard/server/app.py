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
import re
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

# 三档刷新写两张表，冲突关系不一样，所以用两把锁而不是一把：
#
#   ads_realtime_overview ← today(每秒) + full(60秒)   → 需要互斥
#   ads_funnel_1d         ← funnel(2秒) + full(60秒)   → 需要互斥
#   today 与 funnel 写不同的表                          → 不必互斥
#
# 一开始图省事只用一把锁，结果是 funnel 卡住时 today 跟着饿死：埋点数据涨到
# 7.7 万行后 funnel 的 multi-distinct 要跑十几秒，today 的 1 秒周期完全失效，
# 大屏顶栏的「今日」就不动了 —— 修一个并发问题换来一个更难查的可用性问题。
#
# full 同时写两张表，必须两把锁都拿。固定「先 FUNNEL 后 OVERVIEW」的获取顺序，
# 否则 full 与 funnel/today 交叉时会死锁。
FUNNEL_LOCK = threading.Lock()
OVERVIEW_LOCK = threading.Lock()


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
    with OVERVIEW_LOCK:
        DORIS_W.query(TODAY_REFRESH_SQL)


def _run_funnel_refresh() -> None:
    """漏斗刷新。这把锁只跟 full 档抢 —— full 也 OVERWRITE 同一张 ads_funnel_1d，
    两边都是「先清分区再写」，并发时会互相删行。

    注意它不跟 today 抢锁：两者写不同的表，而 funnel 的 multi-distinct 在
    埋点表涨到十万级后可能要十几秒，共用一把锁会把 today 饿死。"""
    with FUNNEL_LOCK:
        DORIS_W.query(FUNNEL_REFRESH_SQL)


def _run_full_refresh() -> None:
    # 顺序固定为 FUNNEL → OVERVIEW，与上面两个函数的获取顺序一致，避免死锁
    with FUNNEL_LOCK, OVERVIEW_LOCK:
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
    # MSYS_NO_PATHCONV=1 是给脚本里那些 `docker exec ... /opt/kafka/...` 用的：不加它，
    # Git Bash 会把容器内的 POSIX 路径当成自己的路径去改写（详见 docs/04 的坑表）。
    #
    # 但它是个双刃剑，代价必须记住：这个开关同时**关掉了**「POSIX 路径 → Windows 路径」
    # 的翻译。所以被它拉起的脚本里，不能再把 `/d/Users/...` 这种路径递给任何原生 Windows
    # 程序（python.exe 首当其冲）——对方打不开，报 FileNotFoundError，如果错误又被
    # 2>/dev/null 吞掉，就变成一个只在「从大屏调用」时才出现的静默降级。
    # 真实案例：健康度的 e2e_latency_ms 因此长期回退到常量 3849，人工跑脚本却正常。
    # 结论：flag 只用来保护 docker/容器路径，脚本内部别拿它当「通用安全设置」。
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
        target=_loop, args=("funnel", 2.0, _run_funnel_refresh),
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
# index.html 里对 src/*.js|css 的引用，返回前统一追加 ?v=<mtime>。
#
# 为什么非加不可：浏览器对没有 Cache-Control 的静态文件走「启发式缓存」，
# 通常取「Last-Modified 距今时长的 10%」当新鲜期。改完前端立刻刷新时，
# 它很可能直接复用旧 JS 而不发条件请求。旧 JS 去操作新 HTML 里已经删掉的
# 元素 id（这次是 #todayGmv 改成了 #todayText），getElementById 返回 None，
# 一次 .textContent 赋值就抛 TypeError，render() 从那一行断掉 ——
# 8 个面板全停在初始的「加载中…」，顶栏却只是「数据延迟」，看起来像后端挂了。
#
# 这个 bug 有个恶劣的性质：只有「刚改完前端且浏览器有旧缓存」的那个人能看到。
# 无头浏览器每次是全新上下文，开发时永远复现不出来。所以不能靠「记得强刷」，
# 得让 URL 跟着文件走 —— 文件一改，URL 就变，缓存天然失效。
_ASSET_REF = re.compile(r'(src|href)="(src/[^"?#]+)"')


def _index_html() -> bytes:
    try:
        html = (ROOT / "index.html").read_text(encoding="utf-8")
    except OSError:
        return b""

    def stamp(m: re.Match) -> str:
        attr, rel = m.group(1), m.group(2)
        try:
            ver = int((ROOT / rel).stat().st_mtime)
        except OSError:
            ver = 0
        return f'{attr}="{rel}?v={ver}"'

    return _ASSET_REF.sub(stamp, html).encode("utf-8")


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
        self._no_store = True                    # 见 end_headers，别加两遍
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def end_headers(self) -> None:
        # 静态资源（index.html / src/*.js / src/*.css）必须禁缓存。
        #
        # 标准库 SimpleHTTPRequestHandler 只发 Last-Modified，不发 Cache-Control，
        # 浏览器于是走「启发式缓存」——通常取距今时间的 10% 当新鲜期。改完前端
        # 立刻刷新时，浏览器很可能直接复用旧 JS 而不发条件请求。
        #
        # 后果不是「样式没更新」这么轻：新旧混搭会让 getElementById 返回 null。
        # 曾经改版删掉 #todayGmv（换成 #todayText）、JS 也同步改了，但浏览器拿
        # 的是旧 JS —— 它在 null 上取 .textContent 抛 TypeError，render() 从那一行
        # 断掉，8 个面板全部停在初始的「加载中…」，而顶栏仍是「数据延迟」。
        # 看起来像后端挂了，实际后端 200 且数据齐全，只有刷新页面的那个人能看到。
        if not getattr(self, "_no_store", False):
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
        super().end_headers()

    def do_GET(self):                            # noqa: N802 - 标准库命名
        path = (self.path or "/").split("?")[0]  # ?once=1 之类不能影响路由
        if path.startswith("/api/dashboard"):
            return self._dashboard()
        if path.startswith("/api/ping"):
            return self._json({"ok": True, "ts": time.time()})
        if path.startswith("/health"):
            return self._json(self._health())
        if path in ("/", "/index.html"):
            return self._index()
        return super().do_GET()

    def _index(self) -> None:
        body = _index_html()
        if not body:                             # 读不到就退回标准库，别 500
            return super().do_GET()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

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


class QuietServer(ThreadingHTTPServer):
    """浏览器每 1.5 秒轮询一次 /api/dashboard，关页面 / 刷新 / 强制刷新时，
    那条在途请求会被对端直接掐断。标准库默认把这个异常原样打到 stderr：

        ConnectionAbortedError: [WinError 10053] 你的主机中的软件中止了一个已建立的连接

    最难受的场景是退出：按 Ctrl+C 停服务，终端上先刷一大片红色 traceback，
    看起来像"程序崩了"，实际只是浏览器走了。这类异常在服务端没有意义
    （对端已经不要那个字节了），静默掉。真正的代码 bug 仍然照常打印。
    """

    daemon_threads = True

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionError, TimeoutError)):
            return
        super().handle_error(request, client_address)


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

    srv = QuietServer((args.host, args.port), Handler)
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
