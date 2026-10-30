#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
prepare_load.py —— 把 Olist 原始 CSV 预处理成 MySQL LOAD DATA 可直接读的 TSV

为什么需要这一步（不是为了"清洗数据"，是 LOAD DATA 吃不下原始文件）：
  1. order_reviews 有 174 条记录的正文里含双引号，按 RFC4180 是用 "" 转义的，
     而 MySQL LOAD DATA 只认"反斜杠转义"，用 ENCLOSED BY '"' 会把字段切断
  2. order_reviews / category_translation 是 CRLF 换行，其余 7 个是 LF。
     统一按 '\\n' 解析会让最后一列尾部多一个 \\r，DATETIME 直接报 1292，
     文本列则更糟——不报错，只是永远 join 不上
  3. 源文件里的空字段是 ""，语义是 NULL，但 LOAD DATA 会读成空字符串。
     空字符串进 DATETIME/INT 列报错；进文本列则会让 02-verify 的空值核对失败
  4. 正文里含换行符（评论），必须先转义成 \\n，否则一条记录会被切成多行

本脚本只做「表示层转换」，不改任何业务值：不动大小写、不补零、不四舍五入、
不删行。行数必须与源文件逐行对齐，对不上就报错退出。
（唯一的有损转换是 geolocation 的经纬度：原始 14 位小数，DECIMAL(10,7) 保留 7 位。）

输出：
  build/load/<table>.tsv      制表符分隔，\\N 表示 NULL，反斜杠转义
  build/load/manifest.json    行数、空值分布，供导入后核对

用法：
  python scripts/prepare_load.py
  python scripts/prepare_load.py --out build/load --verbose
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

# 表名 → 源 CSV → 列顺序（必须与 sql/mysql/01-schema.sql 完全一致）
# geolocation 少一列：DDL 里的自增代理键 id 不参与导入
TABLES: list[tuple[str, str, list[str]]] = [
    ("customers", "olist_customers_dataset.csv", [
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state",
    ]),
    ("sellers", "olist_sellers_dataset.csv", [
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
    ]),
    ("products", "olist_products_dataset.csv", [
        "product_id", "product_category_name", "product_name_lenght",
        "product_description_lenght", "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm",
    ]),
    ("category_translation", "product_category_name_translation.csv", [
        "product_category_name", "product_category_name_english",
    ]),
    ("orders", "olist_orders_dataset.csv", [
        "order_id", "customer_id", "order_status", "order_purchase_timestamp",
        "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
    ]),
    ("order_items", "olist_order_items_dataset.csv", [
        "order_id", "order_item_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value",
    ]),
    ("order_payments", "olist_order_payments_dataset.csv", [
        "order_id", "payment_sequential", "payment_type",
        "payment_installments", "payment_value",
    ]),
    ("order_reviews", "olist_order_reviews_dataset.csv", [
        "review_id", "order_id", "review_score", "review_comment_title",
        "review_comment_message", "review_creation_date", "review_answer_timestamp",
    ]),
    ("geolocation", "olist_geolocation_dataset.csv", [
        "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
        "geolocation_city", "geolocation_state",
    ]),
]

NULL = r"\N"


def escape(value: str) -> str:
    """空字符串 → NULL 标记；其余做反斜杠转义，保证 TSV 里不出现裸的 tab / 换行。"""
    if value == "":
        return NULL
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )


def convert(table: str, src: Path, cols: list[str], out: Path) -> dict:
    rows = 0
    skipped = 0
    nulls: Counter[str] = Counter()
    null_keys = {"\t", "\n", "\r"}  # 出现即说明转义漏了

    with src.open("r", encoding="utf-8-sig", newline="") as fh, \
            out.open("w", encoding="utf-8", newline="\n") as dst:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            raise SystemExit(f"[{table}] 源文件为空: {src}")

        if header != cols:
            raise SystemExit(
                f"[{table}] 列不匹配，拒绝继续\n"
                f"  期望: {cols}\n  实际: {header}"
            )

        width = len(cols)
        for row in reader:
            # 列数不一致说明 CSV 解析已经错位，宁可报错也不能写进去
            if len(row) != width:
                skipped += 1
                continue
            for name, value in zip(cols, row):
                if value == "":
                    nulls[name] += 1
            parts = [escape(v) for v in row]
            for part in parts:
                if any(ch in part for ch in "\t\n\r"):
                    raise SystemExit(
                        f"[{table}] 第 {rows + 2} 行转义不完整，字段内含裸控制字符"
                    )
            dst.write("\t".join(parts))
            dst.write("\n")
            rows += 1

    if skipped:
        raise SystemExit(f"[{table}] 有 {skipped} 行列数不一致，已中止，请先检查源文件")

    return {"rows": rows, "nulls": {k: v for k, v in nulls.items() if v}}


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser(description="Olist CSV → LOAD DATA 用 TSV")
    ap.add_argument("--src", default=str(root / "data" / "raw" / "olist"))
    ap.add_argument("--out", default=str(root / "build" / "load"))
    args = ap.parse_args()

    src_dir = Path(args.src)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not src_dir.is_dir():
        print(f"源目录不存在: {src_dir}", file=sys.stderr)
        return 1

    manifest: dict[str, dict] = {}
    total = 0
    print(f"{'table':<22}{'rows':>9}{'nulls':>8}  top-null")
    print("-" * 74)
    for table, filename, cols in TABLES:
        src = src_dir / filename
        if not src.is_file():
            print(f"缺少源文件: {src}", file=sys.stderr)
            return 1
        out = out_dir / f"{table}.tsv"
        info = convert(table, src, cols, out)
        manifest[table] = info
        total += info["rows"]
        top = sorted(info["nulls"].items(), key=lambda kv: -kv[1])[:2]
        top_s = ", ".join(f"{k}={v}" for k, v in top) or "-"
        print(f"{table:<22}{info['rows']:>9}{sum(info['nulls'].values()):>8}  {top_s}")

    print("-" * 74)
    print(f"{'合计':<22}{total:>9}")

    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"\n输出目录: {out_dir}")
    print(f"清单文件: {out_dir / 'manifest.json'}")
    print("\n下一步: bash scripts/import.sh   （起容器 → LOAD DATA → 跑 02-verify 核对）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
