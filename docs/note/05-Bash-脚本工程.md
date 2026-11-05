# 05 · Bash 脚本工程

> 一句话：Bash 的默认行为是**出错继续往下跑**。写自动化脚本的第一件事不是写逻辑，
> 是把"出错必须停"这条改成默认，然后逐个地方显式放开。

---

## 一、通用知识

### 1.1 开头三行决定一切

```bash
#!/usr/bin/env bash
set -euo pipefail
```

| 开关 | 作用 | 不设的后果 |
| --- | --- | --- |
| `-e` | 任意命令返回非 0 立即退出 | 中间某步失败，后面继续跑，最后"成功" |
| `-u` | 引用未定义变量报错退出 | `rm -rf "$DIR/"` 里 `DIR` 拼错 = `rm -rf /` |
| `-o pipefail` | 管道中任一环失败，整条管道失败 | `cmd | grep xxx` 里 `cmd` 崩了，`grep` 找不到东西返回 1，看似"没匹配" |

`-e` 有**七个例外**（不会退出），这是最容易误判的地方：

```bash
if cmd; then ...            # 在 if / while / until 的条件位置
cmd || true                 # 在 || 右边
cmd && other                # 在 && 左边（除了最后一条）
! cmd                       # 取反
cmd | other                 # 只在管道最后一环（不加 pipefail 时）
local x=$(cmd)              # 声明语句里的赋值，失败被吞
cmd; echo $?                # 显式取返回码的上下文
```

最后一条要特别注意：

```bash
local out=$(false)          # 不会退出！$? 是 0（local 成功了）
local out; out=$(false)     # 这才符合预期
```

### 1.2 `pipefail` 与 SIGPIPE 的合谋

```bash
set -euo pipefail
x="$(docker ps -a --format '{{.Names}}' | head -1)"
```

`head -1` 读一行就关闭管道，写端收到 `SIGPIPE`（退出码 141）。加了 `pipefail`，
整条命令返回非 0 → `set -e` 直接退出脚本，**而且没有任何报错**。

本项目在 `demo.sh` 第 2 步真实踩过：`x="$(kafka-topics.sh --list | head -1)"` 让
整个脚本静默退出，前面几步都成功了，看起来像"脚本自己停了"。

三个修法（按推荐度）：

```bash
# 1. 用工具自带的限行（首选，不产生管道）
docker ps -a --format '{{.Names}}' -n 1

# 2. 用 sed 取第一行（读完整个输入，不提前关闭）
... | sed -n '1p'

# 3. awk 取到就退出但仍读完输入
... | awk 'NR==1{print; }'          # NR==1 不 break，读完才退
```

### 1.3 参数解析必须用 `while` + `shift`

```bash
while [[ $# -gt 0 ]]; do
  case "$1" in
    --delta)   DELTA="${2:?缺参数值}"; shift 2 ;;
    --dry-run) DRY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *)         echo "未知参数: $1" >&2; exit 2 ;;
  esac
done
```

**`for arg in "$@"` 里的 `shift` 是无效的** —— `"$@"` 在循环开始那一刻就被展开成
固定列表，`shift` 只改位置参数，不改已经展开的列表。本项目一个脚本因此把
`--delta 500` 解析成了两个独立参数。

另一个细节：`${2:?缺参数值}` 在 `set -u` 下等价于"缺失就报错退出"，比手动判断短。

### 1.4 `trap`：清理动作必须挂在 EXIT 上

```bash
cleanup() {
  # 无论正常结束、Ctrl-C、还是被 kill，都跑一次
  mysql -e "UPDATE ... WHERE id='$TMP_ID'"
}
trap cleanup EXIT INT TERM
```

只 `trap cleanup EXIT` 也能覆盖 Ctrl-C（因为 shell 收到 SIGINT 后退出时会跑 EXIT trap），
但显式列上 `INT TERM` 更清楚。**不要在 trap 里用 `exit`** —— 会递归。

本项目 `demo-change.sh` 用它保证"改了一行 MySQL 价格"之后无论怎么中断都会还原，
否则数据被改脏，后面所有验收数字都不对。

### 1.5 路径解析：别信 `$0` 与当前目录

```bash
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
```

- `$0` 是"调用时写的路径"，可能是 `./demo.sh`、`scripts/demo.sh`、也可能是绝对路径
- 在 `cd` 之后再 `basename "$0"` 会丢掉目录前缀（本项目 `run.sh --help` 因此读不到文件）
- `BASH_SOURCE[0]` 在被 `source` 时指向被 source 的文件，比 `$0` 可靠

### 1.6 输出与退出码

```bash
step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; exit 1; }
```

约定：**进度走 stdout，错误走 stderr**。这样 `script > log` 时错误仍能看到。

退出码用起来：

| 码 | 含义 |
| --- | --- |
| 0 | 成功 |
| 1 | 一般失败 |
| 2 | 用法错误（参数不对） |
| 3+ | 具体的失败分类（本项目 `p7-acceptance.sh` 用它区分"接口失败"与"超时") |

### 1.7 浮点数与算术

Bash 的 `(( ))` 和 `[[ -le ]]` **只支持整数**：

```bash
[[ 10034.6 -le 0 ]]      # bash: [[: 10034.6: 语法错误: 需要整数表达式
```

修法：先取整，再用整数比较。

```bash
INT=$(awk -v v="$FLOAT" 'BEGIN{printf "%d", (v<0 ? v-0.5 : v+0.5)}')
[[ "$INT" -le 0 ]]
```

`bc` 也能算，但很多精简镜像里没有；`awk` 一定有。

### 1.8 `MSYS_NO_PATHCONV=1`：Git Bash 特有的双刃剑

Git Bash（MSYS2）会把看起来像 POSIX 路径的参数改写成 Windows 路径：

```bash
docker exec rt-kafka ls /opt/kafka
# 实际执行：ls C:/Program Files/Git/opt/kafka   ← 容器里当然没有
```

`MSYS_NO_PATHCONV=1` 关掉这个改写。但它**同时**关掉了反向翻译：

```bash
MSYS_NO_PATHCONV=1 python -c "open('/d/Users/.../file.json')"
# FileNotFoundError: 因为 Python 是 Windows 版，它不认识 /d/Users
```

本项目真实事故：`collect-pipeline-health.sh` 里用 `MSYS_NO_PATHCONV=1` 包住了整个
子进程，其中 `python -c` 打不开 `/d/...` 的 JSON，`FileNotFoundError` 又被
`2>/dev/null` 吞掉 —— 结果指标静默退回硬编码常量 3849，只在"从大屏调用"时发生，
人工跑脚本永远正常。

结论：**`MSYS_NO_PATHCONV=1` 只加在真正需要它的那条命令前面**，不要 `export` 到整个脚本。

---

## 二、在本项目中

### 2.1 八个脚本的分工

| 脚本 | 类型 | 作用 |
| --- | --- | --- |
| `demo.sh` | 编排 | 六步从零到有数据，全幂等 |
| `demo-change.sh` | 演示/测量 | 改一行 MySQL，量 DWS/ADS/大屏时延，含僵尸自检与自动回滚 |
| `submit-sql.sh` | 工具 | 把一份 Flink SQL 提交到 session 集群 |
| `stop.sh` | 运维 | 停大屏 + 逐个停容器，停前拍行数快照 |
| `collect-pipeline-health.sh` | 采集 | 11 项健康度指标写进 ADS |
| `fetch-flink-jars.sh` | 引导 | 下载 4 个连接器 jar |
| `import.sh` | 引导 | Olist CSV → MySQL |
| `dashboard/p7-acceptance.sh` | 验收 | 13 轮端到端时延，自己判 PASS/FAIL |

### 2.2 `demo.sh` 的幂等设计

每一步都先问"是不是已经好了"，好了就跳过：

```bash
# 容器：已在跑就跳过
if [[ -z "$(docker ps -q --filter name=rt-mysql)" ]]; then
  docker compose ... up -d
fi

# topic：已存在就跳过（不用 --if-not-exists，因为它不校验分区数）
if ! kafka-topics.sh --list | grep -qx user_event; then
  kafka-topics.sh --create --topic user_event --partitions 3 ...
fi

# Doris 表：CREATE TABLE IF NOT EXISTS，直接跑
# Flink 作业：/jobs/overview 里已有同名 RUNNING 就跳过
# 大屏：端口被占就报错退出（不静默抢端口）
```

幂等的价值：出错时不用"先清理再重来"，直接再跑一遍。

### 2.3 `stop.sh`：为什么不用 `docker compose stop`

```bash
# 依赖倒序，逐个停，显式给宽限期
for pair in "rt-flink-tm:25" "rt-flink-jm:20" "rt-doris-be:45" \
            "rt-doris-fe:30" "rt-kafka:30" "rt-mysql:60"; do
  name="${pair%%:*}"; grace="${pair##*:}"
  docker stop -t "$grace" "$name"
  echo "$name exit=$?"
done
```

三个理由：

1. **顺序**：`compose stop` 是并行的，会出现"Flink 还在 flush、Doris 已经关了"的窗口
2. **宽限期**：`stop_grace_period` 只对**创建时**生效（见 `03`），旧容器是 10 秒默认值
3. **可观测**：逐个打印退出码，137 一眼能看出谁被硬杀

### 2.4 关停快照：把"有没有丢数据"变成可比较的数字

```bash
# 停之前
for t in dwd_trade_order_detail ... ads_order_stream; do
  echo "$t=$(count $t)" >> scripts/.shutdown-snapshot.txt
done
```

下次 `stop.sh --status` 逐表对比。判据要写清楚：

- **核心表（DWD/DWS/ADS）行数必须完全一致**
- **埋点表（ods_user_event / dwd_user_event_clean）行数上升是正常的** —— 那是停机窗口
  里 Kafka 积压的补消费；**下降才是丢数据**

没有这条基线，"数据没丢"只能靠感觉。

### 2.5 踩过的坑

**坑 1：`| head -1` + `pipefail` 静默退出**（见 1.2，`demo.sh` 第 2 步真实现场）

**坑 2：`for arg in "$@"` 里 `shift` 无效**（见 1.3）

**坑 3：`--help` 读不到脚本自己**

```bash
cd "$(dirname "$0")/.."        # 先 cd 到根
sed -n '1,30p' "scripts/$(basename "$0")"   # 再拼路径 → scripts/run.sh 变成 run.sh
```

修法：先算绝对路径，再 cd：

```bash
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
```

**坑 4：`2>/dev/null` 吞掉了真正的错误**

```bash
rows=$(docker exec ... mysql -N -e "SELECT COUNT(*) FROM realtime_dw.$t" 2>/dev/null)
# 缺 -D realtime_dw 时报 "No database selected"，被吞 → $rows 为空 → 后面一路空
```

规则：**只在你确知错误类型且要忽略它时才重定向 stderr**。否则先让它打出来。

**坑 5：`grep -c` 在 Git Bash 里的误报**

```bash
grep -c $'\r' file      # 报 116（等于总行数），但 od -c 显示只有 \n
```

查换行符用 `od -c` / `file`，别用 grep（见 `04`）。

---

## 三、速查

```bash
#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; exit 1; }

cleanup() { :; }
trap cleanup EXIT INT TERM

while [[ $# -gt 0 ]]; do
  case "$1" in
    --opt) VAL="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "未知参数: $1" ;;
  esac
done
```

| 症状 | 原因 |
| --- | --- |
| 脚本无声无息停了 | `pipefail` + `head`（SIGPIPE 141），或 `set -e` 撞上某个返回非 0 的命令 |
| 参数解析串位 | `for arg in "$@"` 里 `shift` |
| 容器里路径变成 `C:/Program Files/Git/...` | MSYS 路径改写，加 `MSYS_NO_PATHCONV=1` |
| 反之，脚本里 Python 打不开 `/d/...` | 同上，但方向相反 —— 别把变量 export 到整个脚本 |
| `[[: 10034.6: 需要整数表达式` | Bash 只算整数，先 `awk` 取整 |
| `local x=$(cmd)` 不退出 | `local` 吞掉了返回码，拆成两行 |

延伸：`04-Git-与仓库卫生.md`（可执行位与换行）、`02-Docker.md`（容器内无 gawk）、
`26-时延测量与压测.md`（测量脚本的失败处理）
