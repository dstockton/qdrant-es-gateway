#!/usr/bin/env python3
"""Convert the public Qdrant H&M parquet catalogue to benchmark JSONL.

The parquet file is intentionally not stored in this repository. Install
pyarrow in a local virtual environment before running this importer.
"""

import argparse
import json
from pathlib import Path

import pyarrow.parquet as parquet


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path, help="H&M parquet file")
    parser.add_argument("output", type=Path, help="benchmark JSONL output")
    parser.add_argument("--limit", type=int, default=0, help="optional row limit")
    args = parser.parse_args()

    table = parquet.read_table(args.input, columns=[
        "article_id", "prod_name", "product_type_name", "product_group_name",
        "colour_group_name", "department_name", "index_group_name", "section_name",
        "detail_desc",
    ])
    rows = table.to_pylist()
    if args.limit:
        rows = rows[: args.limit]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as output:
        for index, row in enumerate(rows):
            article_id = str(row["article_id"])
            title = row.get("prod_name") or ""
            description = row.get("detail_desc") or ""
            source = {
                "title": title,
                "description": description,
                "search_text": " ".join(str(row.get(key) or "") for key in (
                    "prod_name", "product_type_name", "product_group_name",
                    "colour_group_name", "department_name", "index_group_name",
                    "section_name", "detail_desc",
                )),
                "product_type": row.get("product_type_name") or "",
                "product_group": row.get("product_group_name") or "",
                "colour": row.get("colour_group_name") or "",
                "department": row.get("department_name") or "",
                "index_group": row.get("index_group_name") or "",
                "section": row.get("section_name") or "",
            }
            output.write(json.dumps({"_id": f"product-{index:07d}", "_source": {"article_id": article_id, **source}}) + "\n")

    print(f"wrote {len(rows)} documents to {args.output}")


if __name__ == "__main__":
    main()
