#!/usr/bin/env python3
"""Replay the AWS Retail Demo Store's exact OpenSearch request shapes."""

import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


INDEX = "retail_demo_validation"
PRODUCTS = [
    ("p-001", {"name": "Red cotton running shoes", "category": "shoes", "style": "sport", "description": "Lightweight trainers for daily running"}),
    ("p-002", {"name": "Blue walking shoes", "category": "shoes", "style": "casual", "description": "Comfortable shoes for travel"}),
    ("p-003", {"name": "Canvas travel bag", "category": "bags", "style": "casual", "description": "Durable carry bag for weekends"}),
    ("p-004", {"name": "Ceramic coffee mug", "category": "home", "style": "classic", "description": "Handmade mug for coffee"}),
    ("p-005", {"name": "Running jacket", "category": "clothing", "style": "sport", "description": "Light jacket for running"}),
]

SEARCH_BODY = {
    "from": 0,
    "size": 5,
    "query": {"dis_max": {"queries": [
        {"match_bool_prefix": {"name": {"query": "running", "boost": 1.2}}},
        {"match_bool_prefix": {"category": "running"}},
        {"match_bool_prefix": {"style": "running"}},
        {"match_bool_prefix": {"description": {"query": "running", "boost": 0.6}}},
    ], "tie_breaker": 0.7}},
    "fields": ["_id"],
    "_source": False,
    "collapse": {"field": "category.keyword", "inner_hits": {
        "name": "category_hits", "size": 15, "fields": ["_id"], "_source": False,
    }},
}


def product_for(number):
    product_id, template = PRODUCTS[number % len(PRODUCTS)]
    suffix = f"catalog item {number:07d}"
    return f"{product_id}-{number}", {
        **template,
        "name": f"{template['name']} {suffix}",
        "description": f"{template['description']}; {suffix}.",
    }


def call(base, path, method="GET", body=None, content_type="application/json"):
    data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
    request = urllib.request.Request(
        base.rstrip("/") + path,
        data=data,
        method=method,
        headers={"content-type": content_type},
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            raw = response.read().decode()
            return response.status, json.loads(raw), (time.perf_counter() - started) * 1000
    except urllib.error.HTTPError as error:
        raw = error.read().decode()
        return error.code, json.loads(raw), (time.perf_counter() - started) * 1000


def run_endpoint(base, docs, keep=False, workers=1, batch=0):
    call(base, f"/{INDEX}", "DELETE")
    index_status, _, _ = call(base, f"/{INDEX}", "PUT", {"settings": {"number_of_shards": 1, "number_of_replicas": 0}})
    if index_status // 100 != 2:
        raise RuntimeError(f"could not create index on {base}: HTTP {index_status}")

    def write_product(number):
        product_id, product = product_for(number)
        status, body, _ = call(base, f"/{INDEX}/_doc/{product_id}", "PUT", product)
        if status // 100 != 2:
            raise RuntimeError(f"could not index product on {base}: {body}")

    if batch:
        for start in range(0, docs, batch):
            lines = []
            for number in range(start, min(start + batch, docs)):
                product_id, product = product_for(number)
                lines.extend((json.dumps({"index": {"_id": product_id}}), json.dumps(product)))
            status, body, _ = call(
                base,
                f"/{INDEX}/_bulk",
                "POST",
                ("\n".join(lines) + "\n").encode(),
                "application/x-ndjson",
            )
            if status // 100 != 2 or body.get("errors"):
                raise RuntimeError(f"could not bulk-index products on {base}: HTTP {status} {body}")
    elif workers == 1:
        for number in range(docs):
            write_product(number)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(write_product, number) for number in range(docs)]
            for future in as_completed(futures):
                future.result()
    time.sleep(2)

    search_status, search, search_ms = call(base, f"/{INDEX}/_search", "POST", SEARCH_BODY)
    groups = search.get("hits", {}).get("hits", [])
    group_ids = [hit.get("_id") for hit in groups]
    inner_ids = [
        inner.get("_id")
        for group in groups
        for inner in group.get("inner_hits", {}).get("category_hits", {}).get("hits", {}).get("hits", [])
    ]
    search_ok = (
        search_status == 200
        and bool(groups)
        and all("category_hits" in group.get("inner_hits", {}) for group in groups)
        and all("_source" not in inner for group in groups for inner in group["inner_hits"]["category_hits"]["hits"]["hits"])
    )

    like_id = "p-001-0"
    mlt_body = {"from": 0, "size": 5, "query": {"more_like_this": {
        "fields": ["name", "category", "style", "description"],
        "like": [{"_index": INDEX, "_id": like_id}],
        "min_term_freq": 1,
        "max_query_terms": 10,
    }}}
    similar_status, similar, similar_ms = call(base, f"/{INDEX}/_search", "POST", mlt_body)
    similar_ids = [hit.get("_id") for hit in similar.get("hits", {}).get("hits", [])]
    similar_ok = similar_status == 200 and like_id not in similar_ids

    search_samples = []
    for _ in range(20):
        status, _, elapsed = call(base, f"/{INDEX}/_search", "POST", SEARCH_BODY)
        if status != 200:
            raise RuntimeError(f"benchmark search failed on {base}: HTTP {status}")
        search_samples.append(elapsed)
    if not keep:
        call(base, f"/{INDEX}", "DELETE")
    search_samples.sort()
    return {
        "reported_statuses": {"index": index_status, "search": search_status, "similar": similar_status},
        "search_group_ids": group_ids,
        "search_inner_ids": inner_ids,
        "search_compatible": search_ok,
        "similar_ids": similar_ids,
        "similar_compatible": similar_ok,
        "first_search_ms": round(search_ms, 2),
        "similar_search_ms": round(similar_ms, 2),
        "repeated_search_ms": {
            "p50": round(statistics.median(search_samples), 2),
            "p95": round(search_samples[int(len(search_samples) * 0.95) - 1], 2),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", required=True)
    parser.add_argument("--gateway", required=True)
    parser.add_argument("--docs", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=1, help="concurrent individual document writes (default: 1)")
    parser.add_argument("--batch", type=int, default=0, help="bulk document batch size; 0 keeps individual PUT writes")
    parser.add_argument("--output", required=True)
    parser.add_argument("--keep", action="store_true", help="leave the validation index for resource inspection")
    args = parser.parse_args()
    if args.docs < len(PRODUCTS):
        parser.error("--docs must be at least 5")
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    if args.batch < 0:
        parser.error("--batch must be zero or greater")

    report = {
        "fixture": "AWS Retail Demo Store search-service request bodies",
        "documents": args.docs,
        "workers": args.workers,
        "batch": args.batch,
        "native_opensearch": run_endpoint(args.native, args.docs, args.keep, args.workers, args.batch),
        "qdrant_gateway": run_endpoint(args.gateway, args.docs, args.keep, args.workers, args.batch),
    }
    report["identical_request_shapes"] = True
    report["ok"] = all(
        report[key][check]
        for key in ("native_opensearch", "qdrant_gateway")
        for check in ("search_compatible", "similar_compatible")
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"ok": report["ok"], "output": str(output)}))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
