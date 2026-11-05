# 04 · Git 与仓库卫生

> 一句话：Git 的价值不在于"能回滚"，而在于**提交信息记的是"为什么"**。
> 半年后你只会记得"当时好像有个坑"，不会记得理由 —— 理由只能写在 commit body 里。

---

## 一、通用知识

### 1.1 三个区域与一次提交

```
工作区            暂存区(index)         本地仓库          远端
working tree  →  git add  →  staging  →  git commit  →  HEAD  →  git push  →  origin
```

```bash
git status --short          # 两列：暂存区状态 + 工作区状态（M/A/D/?）
git diff                    # 工作区 vs 暂存区
git diff --cached           # 暂存区 vs HEAD（下次提交会是什么）
git diff --stat             # 只看改了哪些文件、多少行
git add -A                  # 包含新增与删除
git commit -F - <<'EOF'     # 多行提交信息，避免 shell 转义
...
EOF
```

用 `-F -` 而不是 `-m "..."`：`-m` 里的换行要靠 `$'...'`，引号和反引号还会被 shell 吃掉。

### 1.2 提交信息：subject 说"做了什么"，body 说"为什么"

本项目实际用的格式（Conventional Commits 的简化版）：

```
<type>(<scope>): <一句话，不超过 72 字符>

<为什么这么改。至少写清：原来的做法、遇到的问题、判据>
<如果推翻了自己之前的判断，明确写"上一版是错的，错在哪">

<实测数据：改前 → 改后，用数字>
```

`type` 只用五个：`feat` / `fix` / `docs` / `refactor` / `build`。
`scope` 是模块名：`doris` / `flink` / `dashboard` / `memory` / `gitattributes`。

反例（常见但没用）：

```
fix: 修复 bug
update
修改配置
```

三个月后看这三条，等于没提交。正例见本仓库 `git log`：

```
refactor(memory): TM 3.25GB / BE 1.75GB 等量交换，六容器合计仍 8.75GB

07 的四表 join 快照状态实测 134MB，撑爆 217MB/slot（TM taskHeap 868MB ÷ 4），
表现为 TM 被 cgroup OOM Kill、RestartCount 涨 1、4 个算子永久卡 initializing。
...
```

### 1.3 `.gitignore`：不该进库的三类东西

| 类型 | 例子 | 为什么不进库 |
| --- | --- | --- |
| 构建产物 / 依赖 | `__pycache__/`、`node_modules/` | 可从源码重建，体积大 |
| 运行期数据 | `docker/volumes/`、`flink/checkpoints/`、`*.log` | 每台机器都不同，且可能几百 MB |
| 密钥 / 本地配置 | `.env`、`*.local` | 安全 |

`.gitignore` 只对**未跟踪**文件生效。已经 `git add` 过的文件要：

```bash
git rm --cached path/to/file        # 停止跟踪，保留本地文件
```

检查某文件为什么被忽略：

```bash
git check-ignore -v docker/volumes/mysql
# .gitignore:23:docker/volumes/    docker/volumes/mysql
```

### 1.4 `.gitattributes`：换行符是团队协作的第一号事故源

背景：Windows 用 `CRLF`，Linux/macOS 用 `LF`。Git 有三种策略：

| 配置 | 行为 |
| --- | --- |
| `core.autocrlf=true`（Windows 默认） | 检出转 CRLF，提交转 LF |
| `core.autocrlf=input` | 提交转 LF，检出**不转** |
| `.gitattributes` 的 `eol=lf` | **强制**该文件在仓库和工作区都是 LF，覆盖 `autocrlf` |

`* text=auto` 是"让 Git 自己判断是不是文本"，**判断结果仍受 `autocrlf` 影响**。
所以在 Windows 上，`text=auto` 的文件照样会被检出成 CRLF。

这条对本项目是致命的：`docker/be-conf/be.conf` 被 `start_be.sh` 逐行 source：

```bash
while read -r line; do ... done < be.conf
```

CRLF 会让每个值的末尾带上 `\r` —— `trash_file_expire_time_sec = 3600` 变成 `"3600\r"`，
解析失败或取到错值，而**报错不会指向换行符**。原来 `.gitattributes` 只锁了
`.sh/.sql/.py/.yml/.md/.json/.env*`，漏了 `*.conf`，于是别人 clone 下来就是坏的。

修法：

```gitattributes
*.conf    text eol=lf
# 自身与 .gitignore 也锁上，否则每次 git add 都刷 "LF will be replaced by CRLF" 告警
.gitattributes text eol=lf
.gitignore     text eol=lf
```

```bash
git check-attr text eol -- docker/be-conf/be.conf
# docker/be-conf/be.conf: text: set
# docker/be-conf/be.conf: eol: lf

git add --renormalize .        # 按新规则重新归一化已跟踪文件
```

### 1.5 体积：仓库不该装二进制

```bash
# 谁在占体积
git count-objects -vH
git ls-files -z | xargs -0 du -h | sort -hr | head -20
```

本项目 `flink/lib/*.jar`（4 个，约 60MB）和 Olist 的 9 个 CSV（121MB）都不入库，
由 `scripts/fetch-flink-jars.sh` 和 `scripts/import.sh` 下载/生成。
`.gitignore` 里对 `flink/lib/` 的处理要保留 `.gitkeep`，否则空目录不会被跟踪
（而空目录不存在，Docker 会静默建一个空目录当 jar —— 见 `02`）。

### 1.6 常用排查

```bash
git log --oneline -10
git log --oneline origin/main..HEAD        # 哪些提交还没推
git rev-list --count origin/main..HEAD     # 未推送数量
git show HEAD:path/to/file                 # 看仓库里的版本（不是工作区）
git cat-file blob :path/to/file | od -c    # 看暂存区的原始字节（查换行符）
git blame -L 40,60 path/to/file            # 这几十行是谁、哪次提交改的
git restore --staged file                  # 取消暂存，不动工作区
```

---

## 二、在本项目中

### 2.1 单分支，线性历史

只有 `main`，没有 feature 分支。理由：单人项目，分支带来的并行收益为零，
而 rebase/merge 的成本是实打实的。提交粒度是"一件事做完且验证过"，
不是一个文件一次。

### 2.2 提交信息承载了"为什么"（这是本仓库最重要的约定）

因为代码里的注释会被后来的人删掉，而 commit 不会。举两个真实例子：

```
fix(flink): 07 改回 scan.startup.mode=initial —— latest-offset 会造僵尸作业

原来写的是 latest-offset，动机是"重启不必重放历史、状态不必重建，秒起"。
这个判断错了一半：本作业是 join 之后再聚合，join 两侧状态都只来自数据流，
没有快照就没有 orders 侧的行 → 每条变更都被判"订单不存在"丢掉。
现场：作业 RUNNING、checkpoint 正常、lag 0、健康度 10 项全 ok，但一行不写。
```

```
build(gitattributes): 锁定 *.conf 为 LF，避免 be.conf 被检出成 CRLF

be.conf 是被 start_be.sh 逐行 source 的，CRLF 会让每个值尾部带上 \r，
且报错不会指向换行符。
```

这两条的共同点：**记录的是"如果不写下来，下一个人会重新踩一遍"的东西**。

### 2.3 `.gitignore` 里三条关键规则

```gitignore
docker/volumes/          # 6 个组件的运行期数据（MySQL 655MB / Kafka 86MB / 日志）
flink/checkpoints/       # 每次 checkpoint 几十 MB，几十个作业 id 目录
flink/lib/*.jar          # 4 个连接器约 60MB，脚本下载
!flink/lib/.gitkeep
data/raw/                # Olist 原始 CSV 121MB
```

`docker/be-conf/` **故意不在** `volumes/` 下 —— 它要进 git（配置即代码），
而 `volumes/` 整个被忽略。这个目录位置的选择就是为了让"配置"和"数据"分开。

### 2.4 `.gitattributes` 的完整规则

```gitattributes
* text=auto

*.sh   text eol=lf
*.sql  text eol=lf
*.py   text eol=lf
*.yml  text eol=lf
*.yaml text eol=lf
*.md   text eol=lf
*.json text eol=lf
*.conf text eol=lf        # ← be.conf 靠这一条
*.env* text eol=lf
Dockerfile text eol=lf
.gitattributes text eol=lf
.gitignore     text eol=lf
```

验证方式（两条都要）：

```bash
git check-attr text eol -- docker/be-conf/be.conf     # 规则命中了吗
git cat-file blob :docker/be-conf/be.conf | od -c | head -3   # 实际字节里有 \r 吗
```

注意：`grep -c $'\r'` 在 Git Bash 里可能给出误报（本项目实测 116 行全"命中"，
但 `od -c` 显示只有 `\n`）。**判换行符用 `od -c` 或 `file`，不要用 grep。**

### 2.5 推送受阻的处理

本项目的开发环境出口代理对 `github.com` 返回 `CONNECT tunnel failed, response 502`，
HTTPS 还遇到 `CRYPT_E_NO_REVOKE_CHECK`（证书吊销检查失败）。这是环境问题，不是仓库问题。

处理原则：**不要在仓库里做任何"绕过"的改动**（不改 remote 协议、不关 SSL 校验、
不加 `http.sslVerify=false`）。确认本地提交完整，然后在自己终端 `git push`：

```bash
git log --oneline origin/main..HEAD    # 确认有哪些没推
git push
```

### 2.6 踩过的坑

**坑 1：`git add -A` 把 20MB 的日志加进去了**

`dashboard/server.log` 曾经被跟踪过一次，之后即使加进 `.gitignore` 也仍然被跟踪
（忽略规则只对未跟踪文件生效）。修法：

```bash
git rm --cached dashboard/server.log
```

**坑 2：改完 `.gitattributes` 不 renormalize 等于没改**

规则只影响**以后**的检出。已入库的文件内容不变：

```bash
git add --renormalize .
git status --short          # 会看到一批 M，全是换行符变化
```

**坑 3：`git diff` 看不到换行符差异**

如果只是 CRLF ↔ LF，`git diff` 可能一片空白（取决于 `core.whitespace`）。
用 `git cat-file blob :file | od -c` 看原始字节。

---

## 三、速查

```bash
# 日常
git status --short
git diff --stat
git add -A && git commit -F - <<'EOF'
type(scope): subject

why...
EOF

# 查
git log --oneline -8
git log --oneline origin/main..HEAD
git show HEAD:path/to/file
git blame -L 40,60 path/to/file

# 仓库卫生
git check-ignore -v <path>
git check-attr text eol -- <path>
git add --renormalize .
git rm --cached <path>
git count-objects -vH
```

| 现象 | 原因 |
| --- | --- |
| 每次 `git add` 都告警 `LF will be replaced by CRLF` | 该文件没被 `eol=lf` 覆盖 |
| 别人 clone 后脚本报奇怪的语法错 | 文件被检出成 CRLF |
| `.gitignore` 加了规则但仍被跟踪 | 需要 `git rm --cached` |
| clone 下来某个空目录不存在 | Git 不跟踪空目录，要放 `.gitkeep` |

延伸：`05-Bash-脚本工程.md`（脚本可执行位与换行）、`../04-环境搭建.md` §6
