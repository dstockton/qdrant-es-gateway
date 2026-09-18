#!/usr/bin/env python3
"""Replay a deterministic product-search workflow against two ES-compatible URLs."""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request


PRODUCTS = [
    ("p-001", {"sku": "p-001", "title": "Wireless headphones", "description": "Noise cancelling audio with long battery life", "brand": "Acme", "category": "audio", "price": 129.0, "stock": 42}),
    ("p-002", {"sku": "p-002", "title": "USB-C charger", "description": "Fast compact wall charger for laptops", "brand": "Northstar", "category": "accessories", "price": 39.0, "stock": 85}),
    ("p-003", {"sku": "p-003", "title": "Mechanical keyboard", "description": "Quiet low profile keyboard for office", "brand": "Acme", "category": "office", "price": 99.0, "stock": 17}),
    ("p-004", {"sku": "p-004", "title": "Travel mug", "description": "Insulated stainless steel cup", "brand": "Zenith", "category": "home", "price": 24.0, "stock": 0}),
    ("p-005", {"sku": "p-005", "title": "Standing desk", "description": "Electric height adjustable workspace", "brand": "Northstar", "category": "office", "price": 499.0, "stock": 8}),
    ("p-006", {"sku": "p-006", "title": "Web camera", "description": "Sharp video with built-in microphone", "brand": "Acme", "category": "office", "price": 79.0, "stock": 31}),
]

MAPPING = {"mappings": {"properties": {
    "title": {"type": "text"}, "description": {"type": "text"},
    "brand": {"type": "keyword"}, "category": {"type": "keyword"},
    "price": {"type": "float"}, "stock": {"type": "integer"}, "sku": {"type": "keyword"},
}}}


def call(base, path, method="GET", body=None, content_type="application/json"):
    payload = None if body is None else body.encode("utf-8")
    request = urllib.request.Request(base.rstrip("/") + path, data=payload, method=method,
                                     headers={"content-type": content_type})
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read().decode("utf-8")
            status = response.status
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8")
        status = error.code
    except Exception as error:  # transport failures are retained in the report
        return {"status": None, "elapsed_ms": round((time.perf_counter() - started) * 1000, 2), "error": str(error)}
    try:
        parsed = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        parsed = raw
    return {"status": status, "elapsed_ms": round((time.perf_counter() - started) * 1000, 2), "body": parsed}


def ndjson_bulk():
    lines = []
    for doc_id, source in PRODUCTS:
        lines.extend([json.dumps({"index": {"_id": doc_id}}), json.dumps(source)])
    return "\n".join(lines) + "\n"


def ids(response):
    return [hit.get("_id") for hit in response.get("hits", {}).get("hits", [])]


def sources(response):
    return {hit.get("_id"): hit.get("_source") for hit in response.get("hits", {}).get("hits", [])}


def run_endpoint(base, index):
    calls = []
    def do(path, method="GET", body=None, content_type="application/json"):
        result = call(base, path, method, body, content_type)
        calls.append({"path": path, "method": method, **result})
        return result

    do(f"/{index}", "DELETE")
    created = do(f"/{index}", "PUT", json.dumps(MAPPING))
    bulk = do(f"/{index}/_bulk?refresh=wait_for", "POST", ndjson_bulk(), "application/x-ndjson")
    search_body = {"query": {"bool": {"must": {"match": {"title": "wireless headphones"}}, "filter": [{"term": {"brand": "Acme"}}, {"range": {"price": {"lte": 400}}}]}}, "_source": ["title", "brand", "price"], "sort": [{"price": "asc"}], "size": 3}
    filtered = do(f"/{index}/_search", "POST", json.dumps(search_body))
    facet = do(f"/{index}/_search", "POST", json.dumps({"size": 0, "aggs": {"brands": {"terms": {"field": "brand", "size": 10}}}}))
    prefix = do(f"/{index}/_search", "POST", json.dumps({"query": {"prefix": {"title": "wire"}}, "size": 10}))
    wildcard = do(f"/{index}/_search", "POST", json.dumps({"query": {"wildcard": {"title": "*keyboard*"}}, "size": 10}))
    regexp = do(f"/{index}/_search", "POST", json.dumps({"query": {"regexp": {"sku": "p-00[12]"}}, "size": 10}))
    msearch = do(
        f"/{index}/_msearch",
        "POST",
        "{}\n" + json.dumps({"query": {"match": {"title": "charger"}}, "size": 2}) + "\n"
        + "{}\n" + json.dumps({"query": {"term": {"brand": "Acme"}}, "size": 2}) + "\n",
        "application/x-ndjson",
    )
    first_page = do(f"/{index}/_search", "POST", json.dumps({"query": {"match_all": {}}, "sort": [{"price": "asc"}], "size": 2}))
    cursor = ((first_page.get("body") or {}).get("hits", {}).get("hits", [{}])[-1].get("sort") or [None])
    second_page = do(f"/{index}/_search", "POST", json.dumps({"query": {"match_all": {}}, "sort": [{"price": "asc"}], "search_after": cursor, "size": 2}))
    fetched = do(f"/{index}/_doc/p-001")
    updated = do(f"/{index}/_update/p-001", "POST", json.dumps({"doc": {"price": 119.0, "stock": 40}}))
    refreshed = do(f"/{index}/_refresh", "POST")
    after_update = do(f"/{index}/_doc/p-001")
    deleted = do(f"/{index}/_doc/p-004", "DELETE", None)
    count = do(f"/{index}/_count", "POST", json.dumps({"query": {"match_all": {}}}))
    return {"calls": calls, "key_results": {
        "filtered": filtered.get("body") if filtered.get("status") else None,
        "facet": facet.get("body") if facet.get("status") else None,
        "prefix": prefix.get("body") if prefix.get("status") else None,
        "wildcard": wildcard.get("body") if wildcard.get("status") else None,
        "regexp": regexp.get("body") if regexp.get("status") else None,
        "msearch": msearch.get("body") if msearch.get("status") else None,
        "first_page": first_page.get("body") if first_page.get("status") else None,
        "second_page": second_page.get("body") if second_page.get("status") else None,
        "fetched": fetched.get("body") if fetched.get("status") else None,
        "after_update": after_update.get("body") if after_update.get("status") else None,
        "count": count.get("body") if count.get("status") else None,
    }, "mutations": {"created": created, "bulk": bulk, "updated": updated, "refreshed": refreshed, "deleted": deleted}}


def compare(native, gateway):
    checks = []
    def check(name, ok, detail): checks.append({"name": name, "ok": bool(ok), "detail": detail})
    required_paths = {"PUT", "POST"}
    for side, result in (("native", native), ("gateway", gateway)):
        failures = [{"method": c["method"], "path": c["path"], "status": c.get("status"), "error": c.get("error")}
                    for c in result["calls"] if c["method"] in required_paths and c.get("status") not in range(200, 300)]
        check(side + " required requests", not failures, failures)
    nr, gr = native["key_results"], gateway["key_results"]
    nfiltered, gfiltered = nr.get("filtered") or {}, gr.get("filtered") or {}
    nids, gids = ids(nfiltered), ids(gfiltered)
    check("filtered top-k overlap", bool(set(nids) & set(gids)), {"native": nids, "gateway": gids})
    check("filtered sources returned", all(v for v in sources(gfiltered).values()), sources(gfiltered))
    for name in ("prefix", "wildcard", "regexp"):
        ni, gi = set(ids(nr.get(name) or {})), set(ids(gr.get(name) or {}))
        check(name + " result IDs", ni == gi, {"native": sorted(ni), "gateway": sorted(gi)})
    nm = nr.get("msearch") or {}
    gm = gr.get("msearch") or {}
    check("multi-search response count", len(nm.get("responses", [])) == len(gm.get("responses", [])) == 2, {"native": nm, "gateway": gm})
    nsecond, gsecond = ids(nr.get("second_page") or {}), ids(gr.get("second_page") or {})
    check("search_after page overlap", bool(set(nsecond) & set(gsecond)), {"native": nsecond, "gateway": gsecond})
    nfacet, gfacet = nr.get("facet") or {}, gr.get("facet") or {}
    nb = [b.get("key") for b in nfacet.get("aggregations", {}).get("brands", {}).get("buckets", [])]
    gb = [b.get("key") for b in gfacet.get("aggregations", {}).get("brands", {}).get("buckets", [])]
    check("brand facet keys", set(nb) == set(gb), {"native": nb, "gateway": gb})
    check("updated source", (nr.get("after_update") or {}).get("_source") == (gr.get("after_update") or {}).get("_source"), {"native": nr.get("after_update"), "gateway": gr.get("after_update")})
    check("count after delete", (nr.get("count") or {}).get("count") == (gr.get("count") or {}).get("count"), {"native": nr.get("count"), "gateway": gr.get("count")})
    return {"passed": sum(c["ok"] for c in checks), "total": len(checks), "checks": checks}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", default="http://localhost:19200")
    parser.add_argument("--gateway", default="http://localhost:9200")
    parser.add_argument("--index", default="validation_products")
    parser.add_argument("--output", default="validation/results/latest.json")
    args = parser.parse_args()
    native = run_endpoint(args.native, args.index)
    gateway = run_endpoint(args.gateway, args.index)
    report = {"index": args.index, "documents": len(PRODUCTS), "native": native, "gateway": gateway, "comparison": compare(native, gateway)}
    parent = os.path.dirname(args.output)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    print(json.dumps(report["comparison"], indent=2))
    return 0 if report["comparison"]["passed"] == report["comparison"]["total"] else 1


if __name__ == "__main__":
    sys.exit(main())
