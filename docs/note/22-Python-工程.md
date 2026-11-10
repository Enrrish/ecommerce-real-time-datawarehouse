# 22 · Python 工程

> 一句话：这个项目里的 Python 只做三件事 —— **造数据、量延迟、跑校验**。
> 没有 Web 框架，没有 ORM，全是标准库 + 一个 Kafka 客户端。这也是一种设计。

---

## 一、通用知识

### 1.1 依赖管理：能用标准库就不装包

| 需求 | 标准库 | 第三方 |
| --- | --- | --- |
| JSON | `json` | — |
| CSV（含引号内换行） | `csv` | — |
| HTTP 请求 | `urllib.request` | `requests`（更好用） |
| HTTP 服务 | `http.server` | Flask / FastAPI |
| 数据库 | 无 | `pymysql` / `mysql-connector` |
| 命令行参数 | `argparse` | `click` |
| 路径 | `pathlib` | — |
| 子进程 | `subprocess` | — |

本项目的原则：**只有"必须"才引入第三方**。`requirements.txt` 里只有 `kafka-python`。

好处：换机器不用装环境；坏处：`http.server` 这种"玩具级"服务器要自己补缓存头、
自己做连接复用（见 [23](23-HTTP-服务与大屏后端.md)）。

### 1.2 `argparse` 的三种参数类型

```python
p = argparse.ArgumentParser()
p.add_argument("--events", type=int, default=20000)     # 可选带默认
p.add_argument("--sink", choices=["kafka", "stdout"], default="kafka")
p.add_argument("--dry-run", action="store_true")        # 布尔开关
p.add_argument("--live", type=float, metavar="SEC")     # 无默认 = None
```

校验要写进 `type` 或 `choices`，别在业务代码里散落 `if`：

```python
def positive(s):
    v = int(s)
    if v <= 0:
        raise argparse.ArgumentTypeError("必须 > 0")
    return v
p.add_argument("--events", type=positive)
```

### 1.3 `csv` 模块：不要用 `readlines()`

```python
# ❌ 按行读：comment 字段含换行就错位
with open(path) as f:
    for line in f:
        ...

# ✅ 按记录读：正确处理引号、转义、字段内换行
import csv
with open(path, newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        ...
```

`newline=""` 是必须的 —— 否则 Python 会把 `\r\n` 二次处理，某些字段末尾多出 `\r`。

本项目 `order_reviews` 按行数 104,719，按记录数 99,224 —— 差的就是字段内换行。

### 1.4 `subprocess`：三个必须

```python
subprocess.run(
    [sys.executable, "-c", code],       # 1. 用列表，不用字符串（避免 shell 解析）
    capture_output=True, text=True,     # 2. 明确要不要捕获
    check=True,                          # 3. 非 0 抛异常，而不是静默继续
)
```

不用 `check=True` 就必须自己判 `returncode`，否则"命令失败"会被当成"输出为空"。

### 1.5 异常处理：不要用 `except Exception: pass`

```python
# ❌ 把真实错误吃掉了
try:
    data = json.load(open(p))
except Exception:
    pass

# ✅ 明确预期
try:
    data = json.load(open(p))
except FileNotFoundError:
    data = None
except json.JSONDecodeError as e:
    log.warning("JSON 坏了: %s", e)
    data = None
```

本项目踩过的具体案例：`FileNotFoundError` 被 `2>/dev/null`（shell 层）吞掉，
导致一个指标静默退回硬编码常量，只在某条调用路径上发生（见 [26](26-时延测量与压测.md)）。

### 1.6 持续运行的脚本：三件事

```python
stop = threading.Event()

def on_signal(signum, frame):
    stop.set()

signal.signal(signal.SIGINT, on_signal)
signal.signal(signal.SIGTERM, on_signal)

while not stop.is_set():
    do_one_round()
    stop.wait(interval)          # 可被信号打断的 sleep
```

- `time.sleep(60)` 收到 SIGTERM 不会立刻退出，`Event.wait(60)` 会
- 别忘了 `flush=True`（输出重定向到文件时，print 是块缓冲的，看不到日志）
- 退出前把缓冲刷掉

### 1.7 命名与类型

```python
def parse_row(row: dict[str, str]) -> Event | None: ...
```

不用 mypy 也能写类型注解 —— 它的价值是给读代码的人看。
本项目所有脚本都有模块级 docstring，说明"为什么需要这个脚本"，而不只是"它做什么"。

---

## 二、在本项目中

### 2.1 四个脚本，四种用途

| 脚本 | 行数量级 | 用途 | 关键设计 |
| --- | --- | --- | --- |
| `gen_user_events.py` | ~500 | 造埋点写 Kafka | 三种模式、可注入重复/乱序 |
| `prepare_load.py` | ~200 | CSV → TSV | 处理引号/换行/空串 |
| `check_lag.py` | ~200 | 量端到端延迟 | 探针 + 常驻管道 |
| `calibrate_late_ratio.py` | ~200 | 指标正向对照 | 发已知比例的迟到事件 |

### 2.2 `gen_user_events.py` 的三种模式

```bash
# ① 批量（一次性灌历史）
python scripts/gen_user_events.py --events 20000

# ② 持续压测（固定速率 / 时长）
python scripts/gen_user_events.py --rate 30 --duration 600

# ③ 实时演示（每 2 秒开 3 个会话，事件按自然延迟到点才发）
python scripts/gen_user_events.py --live 2 --sessions 3
```

第三种是"大屏演示"用的：会话在"此刻"开始，后续动作按自然延迟排队发出，
所以漏斗面板会**持续在动**。

### 2.3 三条设计原则（写在文件头）

**① 实体用真实 ID**

```
product_id 取自 olist_products，user_id 取自 customer_unique_id
→ ods_user_event 才能和 dim_product / dim_user 真 join 出"品类漏斗"
→ 不是自己造一套 ID 再自己 join 自己
```

**② 漏斗按商品走，不按会话走**

```
真实电商里"看了 A 加购了 B"是常态，但那样算不出"商详→加购"的同商品转化
做法：每个商品独立走一遍 曝光→加购→提交→支付
口径：同一用户同一商品分别计入各自环节（与 docs/03 §2 对齐）
```

**③ `event_time` 严格不晚于当前时刻**

```
会话起点在窗口内随机，后续动作按偏移累加
若起点太晚导致末尾越界，就把整个会话往前挪
否则 Flink 的 watermark 会把这些"未来数据"判为迟到并丢弃
现象：生产了几万条，漏斗只统计到一半，极难排查
```

第三条是"数据源的问题在数据源修"的典型（见 [13](13-时间语义与-Watermark.md)）。

### 2.4 可注入的脏数据

```bash
--dup-rate 0.03      # 3% 事件重复（同 event_id）
--late-rate 0.05     # 5% 乱序到达（迟到 ≥ 5 分钟，只改发送顺序不改时间）
```

用途：

- `--dup-rate` 验证 UNIQUE 模型的幂等
- `--late-rate` 验证 `late_ratio` 指标能算对（配合正向对照）

**注入量必须可计算**，否则"看起来差不多"不算验证（见 [21](21-数据质量与一致性校验.md)）。

### 2.5 `prepare_load.py`：把 CSV 变成 LOAD DATA 能吃的 TSV

三件必须处理的事：

```python
# 1. 空字符串 → \N（LOAD DATA 的 NULL 表示）
val = row[col].strip()
out = "\\N" if val == "" else val

# 2. 字段内的制表符/换行要转义（否则列错位）
out = out.replace("\t", " ").replace("\n", " ").replace("\r", " ")

# 3. 引号由 csv 模块处理，这里不再包引号
```

为什么不直接 `LOAD DATA LOCAL INFILE` 读 CSV：CSV 的引号规则和 MySQL 的转义规则不一致，
而且 `LOCAL` 要走客户端、多一个安全面。

### 2.6 踩过的坑

**坑 1：Windows 上 `subprocess.run(["bash", ...])` 命中的是 WSL 启动器**

```
PATH 里 System32 优先 → bash = C:\Windows\System32\bash.exe（WSL）
本机 WSL 被策略禁用 → 退出码 1
不看 returncode 就是"面板有数但永远不变"的静默失败
```

修法：走 Git Bash 绝对路径（见 `05`、`23`）。

**坑 2：`MSYS_NO_PATHCONV=1` 让 Python 打不开 `/d/...`**

```
shell 侧：MSYS_NO_PATHCONV=1 关掉路径翻译
Python 侧：Windows 版 Python 不认识 /d/Users
→ FileNotFoundError，且被 2>/dev/null 吞掉
```

修法：不要在整脚本层 `export`，只加在需要它的那条命令前（见 `05`）。

**坑 3：`print()` 重定向到文件后看不到输出**

Python 对非 tty 使用块缓冲（4~8KB）。演示脚本要 `print(..., flush=True)`
或启动时 `python -u`。

**坑 4：Kafka producer 不 `flush()` 就退出，最后一批丢**

```python
producer.send(topic, value=payload)
producer.flush()          # 必须
producer.close()
```

本项目在 `--live` 模式的退出路径里做了 flush，否则 Ctrl-C 时最后几条会丢。

---

## 三、速查

```python
# 标准骨架
#!/usr/bin/env python3
"""一句话说明这个脚本解决什么问题，以及为什么不用别的方式。"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    ...
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

| 症状 | 原因 |
| --- | --- |
| 输出到文件后看不到日志 | 块缓冲，加 `flush=True` 或 `-u` |
| `subprocess` 返回 1 但命令明明存在 | 命中了 System32 的 WSL 启动器 |
| `FileNotFoundError` 但文件确实在 | `MSYS_NO_PATHCONV=1` 关掉了路径翻译 |
| CSV 行数比预期多 | 用了 `readlines()`，字段内有换行 |
| 最后一批发不出去 | producer 没 `flush()` |

延伸：`05-Bash-脚本工程.md`、`23-HTTP-服务与大屏后端.md`、
`26-时延测量与压测.md`、`../04-环境搭建.md`
