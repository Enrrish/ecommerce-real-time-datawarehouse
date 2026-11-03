"""把当前大屏渲染成一份**单文件 HTML 快照**（CSS / JS / 数据全部内联）。

用途有两个：
  1. 演示兜底 —— 面试现场万一集群没起来，双击这个文件也能看到完整大屏
  2. 截图验证 —— 无头浏览器在 --virtual-time-budget 下会跳过网络请求，
     只有把数据直接内联进页面，截图才能拿到真实画面

用法（在 dashboard/ 目录下）：
    python server/snapshot.py                 # 输出 snapshot.html
    python server/snapshot.py -o out/demo.html
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))

from doris import Doris          # noqa: E402
import queries                   # noqa: E402


def build() -> str:
    doris = Doris()
    try:
        data = queries.collect(doris, {})
    finally:
        doris.close()
    data["meta"]["server_ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
    data["meta"]["query_ms"] = None

    html = (ROOT / "index.html").read_text(encoding="utf-8")
    css = (ROOT / "src" / "theme.css").read_text(encoding="utf-8")
    charts = (ROOT / "src" / "charts.js").read_text(encoding="utf-8")
    app = (ROOT / "src" / "app.js").read_text(encoding="utf-8")

    html = html.replace(
        '<link rel="stylesheet" href="src/theme.css">',
        f"<style>\n{css}\n</style>",
    )
    # 数据必须在两个脚本之前注入，app.js 启动时会读 window.__DATA__
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    inline_scripts = (
        f"<script>window.__DATA__={payload};</script>\n"
        f"<script>\n{charts}\n</script>\n"
        f"<script>\n{app}\n</script>"
    )
    html = re.sub(
        r'<script src="src/charts\.js"></script>\s*<script src="src/app\.js"></script>',
        lambda _m: inline_scripts,
        html,
    )
    if "__DATA__" not in html:
        raise RuntimeError("注入失败：index.html 里的 script 标签形态可能变了")
    return html


def main() -> None:
    ap = argparse.ArgumentParser(description="生成大屏离线快照")
    ap.add_argument("-o", "--out", default=str(ROOT / "snapshot.html"))
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    html = build()
    out.write_text(html, encoding="utf-8")
    print(f"[snapshot] {out}  {len(html) / 1024:.1f} KB")


if __name__ == "__main__":
    main()
