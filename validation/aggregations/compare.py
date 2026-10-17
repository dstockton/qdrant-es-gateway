#!/usr/bin/env python3
"""Compare exact filter-only reductions, calendar buckets, and sorted pages.
Run against disposable instances; creates/deletes only the named demo index.
"""
import argparse
import json
import time
import urllib.error
import urllib.request

INDEX = "gateway-aggregation-demo"
DOCS = [
    {"rank": 1, "price": 50, "brand": "A", "date": "2026-03-28T12:00:00Z"},
    {"rank": 2, "price": 40, "brand": "A", "date": "2026-03-29T12:00:00Z"},
    {"rank": 3, "price": 30, "brand": "B", "date": "2026-03-30T12:00:00Z"},
    {"rank": 4, "price": 20, "brand": "B", "date": "2026-10-24T12:00:00Z"},
    {"rank": 5, "price": 10, "brand": "B", "date": "2026-10-25T12:00:00Z"},
    {"rank": 6, "price": 2, "brand": "C", "date": "2026-10-26T12:00:00Z"},
]
AGGS = {"lowest": {"min": {"field": "price"}}, "highest": {"max": {"field": "price"}},
        "brands": {"terms": {"field": "brand", "size": 2}},
        "cheap": {"filter": {"range": {"price": {"lte": 10}}}},
        "groups": {"filters": {"filters": {"B": {"term": {"brand": "B"}}}}},
        "days": {"date_histogram": {"field": "date", "calendar_interval": "day", "time_zone": "Europe/London", "min_doc_count": 1}}}
CASES = {"aggregation_only": {"size": 0, "aggs": AGGS},
         "post_filter": {"size": 2, "sort": [{"rank": "asc"}], "post_filter": {"term": {"brand": "B"}}, "aggs": AGGS},
         "sorted": {"size": 2, "sort": [{"rank": "asc"}]},
         "next_page": {"size": 2, "sort": [{"rank": "asc"}], "search_after": [2]}}


def request(endpoint, method, path, body=None):
    data = None if body is None else (body.encode() if isinstance(body, str) else json.dumps(body).encode())
    req = urllib.request.Request(endpoint.rstrip("/") + path, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"{method} {path}: {error.code} {error.read().decode()}") from error


def run(endpoint):
    try:
        request(endpoint, "DELETE", "/" + INDEX)
    except RuntimeError as error:
        if ": 404 " not in str(error):
            raise
    request(endpoint, "PUT", "/" + INDEX, {"mappings": {"properties": {"rank": {"type": "integer"}, "price": {"type": "double"}, "brand": {"type": "keyword"}, "date": {"type": "date"}}}})
    bulk = "".join(json.dumps({"index": {"_id": str(number)}}) + "\n" + json.dumps(document) + "\n" for number, document in enumerate(DOCS, 1))
    response = request(endpoint, "POST", f"/{INDEX}/_bulk", bulk)
    assert not response["errors"], response
    request(endpoint, "POST", f"/{INDEX}/_refresh")
    for attempt in range(100):
        if request(endpoint, "POST", f"/{INDEX}/_count", {"query": {"match_all": {}}})["count"] == len(DOCS):
            break
        time.sleep(0.1)
    else:
        raise RuntimeError("fixture ingestion did not become visible")
    results = {}
    for name, query in CASES.items():
        start = time.perf_counter()
        response = request(endpoint, "POST", f"/{INDEX}/_search", query)
        results[name] = {"elapsed_ms": round((time.perf_counter() - start) * 1000, 3), "total": response["hits"]["total"], "ids": [hit["_id"] for hit in response["hits"]["hits"]], "aggregations": response.get("aggregations")}
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--native", required=True)
    parser.add_argument("--gateway", required=True)
    args = parser.parse_args()
    native, gateway = run(args.native), run(args.gateway)
    for name in CASES:
        for field in ("total", "ids", "aggregations"):
            assert native[name][field] == gateway[name][field], (name, field, native[name][field], gateway[name][field])
    assert gateway["aggregation_only"]["aggregations"]["lowest"]["value"] == 2
    assert gateway["post_filter"]["ids"] == ["3", "4"]
    assert gateway["next_page"]["ids"] == ["3", "4"]
    print(json.dumps({"native": native, "gateway": gateway, "correctness": "all four cases match"}, indent=2))


if __name__ == "__main__":
    main()
