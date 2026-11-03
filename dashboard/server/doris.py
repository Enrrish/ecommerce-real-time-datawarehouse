"""常驻的 Doris 查询通道。

为什么不直接连 9030：宿主机没有 MySQL 客户端库（离线环境装不了 pymysql），
而 Doris FE 的 9030 是 MySQL 协议，浏览器又发不了这个协议。

实测两种走法的单次查询延迟：
  docker exec 每次起进程      826 ms
  常驻 docker exec -i 复用    12 ~ 26 ms

差 30 倍的原因不在 SQL，而在「起一个容器进程 + 一次 MySQL 握手」的固定开销。
大屏每 2 秒轮询、一轮要打 8 个面板，这个开销是决定性的，所以复用一条 stdin/stdout。

协议细节：
  * `--batch`          输出 TSV（tab 分隔、行分隔），不要表格边框
  * `--skip-column-names`  去掉表头，省得每轮都要跳过第一行
  * `--unbuffered`     关键。不加的话 mysql 客户端写管道时全缓冲，
                       语句执行完了但结果还压在缓冲区里，readline 会一直等
  * 每条语句结尾必须带分号；用 `SELECT '__END__'` 当作一次查询的结束哨兵，
    因为 batch 输出本身没有「这是第几条语句」的标记
"""

from __future__ import annotations

import os
import subprocess
import threading
from collections import deque

CONTAINER = "rt-doris-fe"
SENTINEL = "__END_OF_QUERY__"
# 单条语句的服务端超时（秒）。Doris 默认 300s，等于没有 —— BE 一旦被
# 内存压力拖住，大屏的 readline 会一直等到 5 分钟。5s 足够跑完本项目
# 最重的全量刷新（实测 2.6s），又能在卡住时快速失败。
INIT_SQL = "SET query_timeout = 5"

_CMD = [
    "docker", "exec", "-i", CONTAINER,
    "mysql", "-uroot", "-P9030", "-h127.0.0.1",
    # 必须带默认库：刷新语句写的是 `INSERT OVERWRITE TABLE ads_funnel_1d`，
    # 没有 USE 也没有库前缀时 Doris 报 "Current database is not set"。
    # 这条错误还有个坑 —— batch 模式下 mysql 客户端会直接退出，
    # 从管道这侧只能看到 EOF，不知道是 SQL 写错了。
    "-D", "realtime_dw",
    "--batch", "--skip-column-names", "--unbuffered",
]


class DorisError(RuntimeError):
    pass


class Doris:
    """单连接、串行化的查询封装。

    Doris FE 只有一个 mysql 客户端进程，任何时刻只能跑一条语句，
    所以用一把可重入锁把 HTTP 层的并发压平成串行 —— 我们一轮只查 8 条、
    总共 150 ms 左右，串行的代价远小于维护连接池的复杂度。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._proc: subprocess.Popen | None = None
        # 最近几条 stderr：mysql 客户端把 SQL 错误写在 stderr 上，而 batch 模式
        # 遇到错误会**直接退出**，管道这侧只看得到 EOF。不留一份就无法解释
        # 「通道已关闭」到底是容器重启了还是语句写错了。
        self._err: deque[str] = deque(maxlen=8)

    # ---------------------------------------------------------------- 进程管理
    def _spawn(self) -> subprocess.Popen:
        env = dict(os.environ)
        # Git Bash 会把以 / 开头的参数改写成 Windows 路径，docker exec 的
        # 容器内命令一被改写就找不到可执行文件。这个变量是 MSYS 的开关。
        env["MSYS_NO_PATHCONV"] = "1"
        proc = subprocess.Popen(
            _CMD,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            bufsize=0,
        )
        # stderr 必须有人读，否则管道写满后 mysql 会阻塞在写错误信息上，
        # 表现为「查询既不出结果也不退出」。守护线程只保留最后几条。
        threading.Thread(target=self._drain_err, args=(proc,), daemon=True).start()

        # 建连后先设语句超时。没有它的时候，Doris BE 内存不足会把语句挂住
        # 不返回，readline 无限等 → 整屏永久冻结、只能重启服务（server.log
        # 里两次 120 s 的 TimeoutExpired 就是这么来的）。
        # query_timeout 以秒计，超时后 FE 主动返回 ERROR，readline 立刻可读，
        # 再经 stderr 排泄线程变成一条 DorisError —— 面板显示「取数失败」
        # 而不是转圈，比整屏卡死好得多。
        try:
            assert proc.stdin and proc.stdout
            proc.stdin.write(f"{INIT_SQL};\nSELECT '{SENTINEL}';\n".encode("utf-8"))
            for raw in iter(proc.stdout.readline, b""):
                if raw.decode("utf-8", "replace").strip() == SENTINEL:
                    break
        except Exception:            # noqa: BLE001 - 初始化失败不该挡住启动
            pass
        return proc

    def _drain_err(self, proc: subprocess.Popen) -> None:
        try:
            for raw in iter(proc.stderr.readline, b""):
                line = raw.decode("utf-8", "replace").strip()
                if line:
                    self._err.append(line)
        except Exception:            # noqa: BLE001 - 进程结束时管道关闭，正常
            pass

    def _ensure(self) -> subprocess.Popen:
        if self._proc is None or self._proc.poll() is not None:
            self._proc = self._spawn()
        return self._proc

    def close(self) -> None:
        with self._lock:
            if self._proc is not None:
                try:
                    if self._proc.stdin:
                        self._proc.stdin.close()
                    self._proc.terminate()
                except Exception:
                    pass
                self._proc = None

    # ------------------------------------------------------------------- 查询
    def query(self, sql: str, retries: int = 1) -> list[list[str]]:
        """执行一条 SQL，返回按行切好的字符串二维数组。

        失败（容器重启、连接被踢）时重建进程重试一次 —— 大屏是长时间挂着的，
        跨过一次 Docker 重启比让页面白屏更合理。
        """
        sql = sql.strip().rstrip(";")
        payload = f"{sql};\nSELECT '{SENTINEL}';\n".encode("utf-8")

        last_err: Exception | None = None
        for attempt in range(retries + 1):
            with self._lock:
                try:
                    proc = self._ensure()
                    assert proc.stdin and proc.stdout
                    proc.stdin.write(payload)
                    proc.stdin.flush()

                    rows: list[list[str]] = []
                    while True:
                        raw = proc.stdout.readline()
                        if not raw:
                            detail = " | ".join(self._err) or "无 stderr 输出"
                            raise DorisError(f"查询通道已关闭：{detail}")
                        line = raw.decode("utf-8", "replace").rstrip("\r\n")
                        if line == SENTINEL:
                            return rows
                        if line:
                            rows.append(line.split("\t"))
                except Exception as exc:      # noqa: BLE001 - 需要兜住管子异常
                    last_err = exc
                    self.close()
                    if attempt >= retries:
                        raise DorisError(str(exc)) from exc
        raise DorisError(str(last_err))


def fmt_num(value: str | None, digits: int = 0) -> float | int | None:
    """Doris 的 TSV 会给出空字符串表示 NULL，转成 Python 的 None。"""
    if value is None or value == "" or value == "NULL" or value == "\\N":
        return None
    try:
        if digits == 0:
            return int(float(value))
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None
