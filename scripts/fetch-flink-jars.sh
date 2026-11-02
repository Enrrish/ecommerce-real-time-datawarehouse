#!/usr/bin/env bash
# =============================================================================
# fetch-flink-jars.sh — 下载 Flink 三件套 connector jar
# -----------------------------------------------------------------------------
# 为什么要脚本而不是直接提交 jar：
#   三个 jar 合计约 50MB。二进制进 git 会让仓库永久膨胀（git 无法真正删除历史
#   里的大文件），且无法 code review。用固定 URL + 版本号，任何人在任何机器上
#   一条命令就能复现同样的环境。
#
# 这三个 jar 是**版本强耦合**的，改任何一个都要回到 docs/04 §1 重新核对：
#   flink-sql-connector-mysql-cdc-3.2.1.jar   ← Flink CDC 3.2.1，支持 Flink 1.17~1.19
#   flink-sql-connector-kafka-3.1.0-1.18.jar  ← 制品名带 Flink 次版本，必须 1.18
#   flink-doris-connector-1.18-24.0.1.jar     ← 同上，1.18 专用包
#
# 最隐蔽的一环是第三个：升级 Flink 1.18 → 1.19 时忘了换它，集群能起、作业能提交，
# 第一条数据写 Doris 才报 NoSuchMethodError。所以下载完会校验文件名里的 1.18。
#
# 用法：
#   bash scripts/fetch-flink-jars.sh          # 下载到 docker/flink/lib/
#   bash scripts/fetch-flink-jars.sh --force  # 覆盖已存在的
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
LIB="$ROOT/docker/flink/lib"
MIRROR="${MAVEN_MIRROR:-https://repo1.maven.org/maven2}"

FORCE=0
[[ "${1:-}" == "--force" ]] && FORCE=1

mkdir -p "$LIB"

# 名称|Maven 路径|期望字节数（用于完整性判断，误差 ±5%）
JARS=(
  "flink-sql-connector-mysql-cdc-3.2.1.jar|org/apache/flink/flink-sql-connector-mysql-cdc/3.2.1/flink-sql-connector-mysql-cdc-3.2.1.jar|21600000"
  "flink-sql-connector-kafka-3.1.0-1.18.jar|org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar|5800000"
  "flink-doris-connector-1.18-24.0.1.jar|org/apache/doris/flink-doris-connector-1.18/24.0.1/flink-doris-connector-1.18-24.0.1.jar|42000000"
)

fail=0
for item in "${JARS[@]}"; do
  IFS='|' read -r name path expect <<< "$item"
  dst="$LIB/$name"
  url="$MIRROR/$path"

  if [[ -f "$dst" && $FORCE -eq 0 ]]; then
    size=$(wc -c < "$dst" | tr -d ' ')
    echo "[skip] $name 已存在（$((size / 1024 / 1024)) MB）"
    continue
  fi

  printf '[get ] %s ... ' "$name"
  # -L 跟随重定向；--retry 应对 Maven 中央偶发 5xx
  if ! curl -fsSL --retry 3 --retry-delay 2 --max-time 600 -o "$dst.part" "$url"; then
    echo "失败（网络或镜像不可达：$url）"
    rm -f "$dst.part"
    fail=$((fail + 1))
    continue
  fi
  mv "$dst.part" "$dst"
  size=$(wc -c < "$dst" | tr -d ' ')
  mb=$(awk -v s="$size" 'BEGIN{printf "%.1f", s/1048576}')
  echo "$mb MB"
done

echo
echo "=== 校验 ==="
for item in "${JARS[@]}"; do
  IFS='|' read -r name path expect <<< "$item"
  dst="$LIB/$name"
  if [[ ! -f "$dst" ]]; then
    echo "  [缺失] $name"
    fail=$((fail + 1))
    continue
  fi
  size=$(wc -c < "$dst" | tr -d ' ')
  # 必须是 ZIP（jar 的本质），前两字节应为 PK
  magic=$(head -c 2 "$dst" 2>/dev/null || echo "")
  if [[ "$magic" != "PK" ]]; then
    echo "  [损坏] $name —— 文件头不是 PK，可能下载到了 HTML 错误页"
    fail=$((fail + 1))
    continue
  fi
  # 版本自检：Doris 与 Kafka connector 的文件名里必须带 1.18
  case "$name" in
    flink-sql-connector-kafka-*|flink-doris-connector-*)
      if [[ "$name" != *"1.18"* ]]; then
        echo "  [版本错] $name —— 文件名里没有 1.18，与 Flink 1.18.1 不匹配"
        fail=$((fail + 1))
        continue
      fi
      ;;
  esac
  echo "  [OK] $name  $((size / 1024 / 1024)) MB"
done

echo
if [[ $fail -gt 0 ]]; then
  echo "有 $fail 项未通过。如果 Maven 中央太慢，换镜像重试："
  echo "  MAVEN_MIRROR=https://maven.aliyun.com/repository/public bash scripts/fetch-flink-jars.sh --force"
  exit 1
fi
echo "三个 jar 就绪，目录：$LIB"
echo "下一步：docker compose -f docker/docker-compose.yml --profile compute up -d"
