# 23 · HTTP 服务与大屏后端

> 一句话：浏览器不会说 MySQL 协议，所以中间必须有一个代理。这个代理只做两件事 ——
> **把 SQL 结果转成 JSON**、**按节奏把 ADS 重刷一遍**。它的难点全在"别把延迟浪费在
> 自己身上"。

---

## 一、通用知识

### 1.1 `http.server` 能干什么、不能干什么

```python
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/api/"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
        else:
            super().do_GET()          # 静态文件

ThreadingHTTPServer(("127.0.0.1", 8080), Handler).serve_forever()
```

| 能做 | 不能做 |
| --- | --- |
| 静态文件、几个 JSON 接口 | 高并发、HTTP/2、WebSocket |
| 单机演示、内网工具 | 生产部署（没有进程管理、没有优雅重启） |
| 零依赖 | 复杂路由（要自己写 if/elif） |

`ThreadingHTTPServer` 每个请求一个线程。**默认的 `HTTPServer` 是单线程的**，
一个慢查询会阻塞所有请求 —— 本项目必须用线程版。

### 1.2 静态文件的缓存：最容易出诡异 bug 的地方

`SimpleHTTPRequestHandler` 只发两个缓存相关头：

```
Last-Modified: Wed, 04 Nov 2026 08:00:00 GMT
（没有 Cache-Control，没有 ETag）
```

浏览器的行为（RFC 9111 的"启发式缓存"）：

```
没有 Cache-Control / Expires 时，按 (Date - Last-Modified) 的 10% 估算新鲜期
→ 文件 10 天没改，就缓存 1 天
```

于是：**改了 JS 但浏览器还在用旧的**，而且**刷新（F5）不一定能刷新它**。

三种修法，强度递增：

| 做法 | 效果 |
| --- | --- |
| `Cache-Control: no-store` | 完全禁缓存，每次都拉（流量大但最可靠） |
| `Cache-Control: no-cache` + `ETag` | 每次都问，没变则 304（省流量） |
| **URL 加版本号** `app.js?v=<mtime>` | 文件变了 URL 就变，浏览器必然重新拉 |

**前两种管不住"已经缓存的那一份"** —— 用户上次访问时缓存了 `app.js`，
这次服务器改成 no-store，浏览器仍可能用旧的。只有改 URL 能强制失效。

本项目两者都用：`no-store` 管以后的，`?v=<mtime>` 管已经缓存的。

### 1.3 一次响应要发哪些头

```python
self.send_response(200)
self.send_header("Content-Type", "application/json; charset=utf-8")
self.send_header("Content-Length", str(len(body)))     # 不写会走 chunked/close
self.send_header("Cache-Control", "no-store")
self.end_headers()
self.wfile.write(body)
```

`Content-Length` 不写也能工作（HTTP/1.0 语义是读到连接关闭），
但会让 keep-alive 失效，每个请求重新建连接。

### 1.4 连接复用：`docker exec` 的 826ms

```
每次 docker exec mysql -e "SELECT ..."  ≈ 826ms
  ├─ docker CLI 启动 + API 调用
  ├─ 容器内 fork mysql 客户端
  ├─ 建立连接、认证
  └─ 查询本身（毫秒级）
```

如果 8 个面板各查一次 → 6.6 秒，测量误差比被测对象还大。

解法：**起一个常驻的 `mysql` 客户端进程，用 stdin/stdout 复用同一条连接**。

```python
proc = subprocess.Popen(["docker", "exec", "-i", "rt-doris-fe",
                         "mysql", "-uroot", "-P9030", "-h127.0.0.1", "-N", "-B"],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE)

def query(sql):
    proc.stdin.write(f"{sql};\nSELECT '__SENTINEL__';\n".encode())
    proc.stdin.flush()
    rows = []
    for raw in iter(proc.stdout.readline, b""):
        line = raw.decode().rstrip("\n")
        if line == "__SENTINEL__":
            return rows           # 用哨兵判断"这批结果读完了"
        rows.append(line.split("\t"))
```

要点：

- **必须用哨兵**：`mysql` 的 `-N -B` 输出没有"结束标记"，只能自己插一个
- `flush()` 必须调，否则数据留在缓冲区
- 进程挂了要能重连（本项目 `_ensure()` + 重试一次）

实测：826ms → **12~26ms**。

### 1.5 后台刷新：锁的粒度按"写哪张表"分

```
ads_realtime_overview ← today(1s) + full(60s)   → 需要互斥
ads_funnel_1d         ← funnel(2s) + full(60s)  → 需要互斥
today 与 funnel 写不同的表                       → 不必互斥
```

一把大锁的问题：funnel 的 multi-distinct 在数据涨到 7.7 万行后要跑十几秒，
`today` 的 1 秒周期被完全饿死 —— 顶栏"今日成交"49 秒不动。

两把锁 + 固定获取顺序（`FUNNEL → OVERVIEW`）防死锁。

### 1.6 优雅处理客户端断开

浏览器关标签页/刷新时，`wfile.write()` 会抛 `ConnectionResetError` /
`BrokenPipeError`。不处理就是满屏 traceback（而且日志迅速涨到几百 MB）。

```python
class QuietServer(ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionError, TimeoutError)):
            return          # 客户端走了，正常
        super().handle_error(request, client_address)
```

`ConnectionResetError` 是 `ConnectionError` 的子类，一条就够。

---

## 二、在本项目中

### 2.1 四个文件的分工

| 文件 | 职责 |
| --- | --- |
| `server/app.py` | HTTP 路由、静态文件、四档刷新调度、锁 |
| `server/doris.py` | 常驻 mysql 管道、查询与重连 |
| `server/queries.py` | 所有面板 SQL（一行一个查询，集中管理） |
| `server/snapshot.py` | 取数的快照与降级（查不到时用上一次成功的） |

### 2.2 四档刷新，每一档都有理由

| 档 | 周期 | 写什么 | 为什么是这个数 |
| --- | --- | --- | --- |
| `today` | **1s** | `ads_realtime_overview` 只今天那几十行 | P7 要求"改一行 MySQL，5 秒内大屏变化"，full 60s 等不起 |
| `funnel` | **2s** | `ads_funnel_1d`（8 行） | 唯一会实时变的面板（埋点链路是活的） |
| `health` | **15s** | `ads_pipeline_health` | 跨 Flink/Kafka/Doris 三处采集，单次约 3s，太快会追尾 |
| `full` | **60s** | overview 8.8 万 + order_stream 9.9 万 | 交易数据是历史回放，本来就不变 |

`full` 还有一个职责：**纠正 today 漏掉的"行消失"**。
`INSERT INTO` 只覆盖不删除，某行如果在新结果里不存在，它会一直留着。
60 秒一次的 `INSERT OVERWRITE` 兜底。

### 2.3 链路预算

```
today 档：07 作业 flush 1s + 本档 1s + 前端轮询 1.5s + 取数 0.3s ≈ 3.8s 最坏值
```

这就是"改一行 MySQL → 大屏可见"在 1.4~4.2s 之间的原因（见 `26`）。

### 2.4 缓存头 + 版本号：一次真实的"全屏加载中"

```
现象：刷新页面后全屏「加载中…」，8 个面板全部停在初始文案
误判：后端挂了 / 数据库连不上
真相：旧 JS 配新 HTML
      ① SimpleHTTPRequestHandler 只发 Last-Modified，不发 Cache-Control
      ② 浏览器启发式缓存 → 用的是旧 app.js
      ③ 旧 JS 找新 HTML 里已删的 #todayGmv（改名成 #todayText）
      ④ getElementById 返回 None → .textContent 抛 TypeError
      ⑤ render() 从那一行断掉，后面 8 个面板全没渲染
      ⑥ 但 fetch 是成功的，所以顶栏显示「数据延迟」而不是「连接中断」
```

修法两件：

```python
# 1. 所有非 API 响应补 no-store
def end_headers(self):
    if not self.path.startswith("/api/"):
        self.send_header("Cache-Control", "no-store")
    super().end_headers()

# 2. 返回 index.html 时把 src/*.js|css 重写成 ?v=<mtime>
html = re.sub(r'(src|href)="(src/[^"]+\.(?:js|css))"',
              lambda m: f'{m.group(1)}="{m.group(2)}?v={mtime(m.group(2))}"',
              html)
```

**无头浏览器永远复现不了这个 bug** —— 每次都是全新上下文，没有缓存。

### 2.5 端口探测

```bash
# run.sh 启动前
if netstat -ano | grep -qE ':8080\s'; then
  echo "8080 已被占用，退出" >&2
  exit 1
fi
```

因为 **Windows 允许两个进程同时 LISTEN 同一个端口**（`SO_REUSEADDR`），
两个 `server/app.py` 都能起来、都不报错、连接随机落 ——
表现为"刷新一次好一次坏"。

更隐蔽的是：两个实例可能用**不同的 Python**（`.workbuddy/3.13.12` vs `anaconda`），
加载不同时期的模块。

### 2.6 判断"页面坏了"还是"接口坏了"

看顶栏文案：

| 顶栏 | 含义 |
| --- | --- |
| `数据延迟`（黄） | fetch 成功，但 `render()` 抛错 → **前端问题** |
| `连接中断，重试中`（红，连续 3 次） | 请求真的失败 → **后端问题** |

这个区分是刻意设计的，因为两类问题的排查方向完全相反。

### 2.7 一次请求返回 8 个面板

`/api/dashboard` 一次返回所有面板数据，而不是 8 个接口。

理由：

- 8 个请求 = 8 次连接 + 8 次查询，其中 7 次的数据在同一个快照里
- 一次返回可以让 8 个面板"同一时刻"的数据，不会出现"漏斗是新的、KPI 是旧的"

代价：单次响应体大（约 40KB）。在局域网里可忽略。

---

## 三、速查

```python
# 最小可用骨架（带缓存控制）
class H(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/api/"):
            body = json.dumps(handle(self.path)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        else:
            super().do_GET()
    def end_headers(self):
        if not self.path.startswith("/api/"):
            self.send_header("Cache-Control", "no-store")
        super().end_headers()
```

```bash
# 探活
curl -s --noproxy '*' -o /dev/null -w "index=%{http_code}\n" http://127.0.0.1:8080/
curl -s --noproxy '*' -o /dev/null -w "api=%{http_code} time=%{time_total}s\n" \
  http://127.0.0.1:8080/api/dashboard

# 谁占了 8080
netstat -ano | grep -E ':8080\s'
```

| 现象 | 原因 |
| --- | --- |
| 全屏「加载中…」，顶栏「数据延迟」 | 前端 render 抛错（常见：旧 JS 配新 HTML） |
| 全屏「加载中…」，顶栏「连接中断」 | 后端真挂了 |
| 面板有数但永远不变 | 刷新线程挂了 / 子进程命令命中 WSL 启动器 |
| 刷新一次好一次坏 | 8080 被两个进程同时监听 |
| 单次查询 800ms+ | 每次重新 `docker exec`，改用常驻管道 |
| 日志迅速涨到几百 MB | 客户端断开没处理，`ConnectionResetError` 刷屏 |

延伸：`22-Python-工程.md`、`24-原生-JS-与-SVG-可视化.md`、
`26-时延测量与压测.md`、`../04-环境搭建.md` §6
