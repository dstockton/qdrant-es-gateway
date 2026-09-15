import concurrent.futures
import json
import os
import statistics
import time
import urllib.request

ES = os.environ.get("ES_URL", "http://localhost:19200")
GW = os.environ.get("GATEWAY_URL", "http://localhost:9200")
DOCS = int(os.environ.get("DOCS", "50000"))
BATCH = int(os.environ.get("BATCH", "500"))
CONCURRENCY = [int(x) for x in os.environ.get("CONCURRENCY", "1,10,50").split(",") if x]
REPEATS = int(os.environ.get("REPEATS", "1"))
UPDATE_DOCS = int(os.environ.get("UPDATE_DOCS", "1000"))
MIXED_REQUESTS = int(os.environ.get("MIXED_REQUESTS", "500"))
WARMUP_SECONDS = int(os.environ.get("WARMUP_SECONDS", "15"))
OUTPUT = os.environ.get("OUTPUT", "/results/benchmark.json")

ES_INDEX = "bench_native"
GW_INDEX = "bench_gateway"
MAPPING = {"mappings": {"properties": {"title": {"type": "text"}, "description": {"type": "text"}, "brand": {"type": "keyword"}, "price": {"type": "float"}, "category": {"type": "keyword"}}}}
PRODUCTS = [("wireless headphones", "noise cancelling audio with long battery life", "audio"), ("usb-c charger", "fast compact wall charger for laptops", "accessories"), ("mechanical keyboard", "quiet low profile keyboard for office", "office"), ("travel mug", "insulated stainless steel cup", "home"), ("standing desk", "electric height adjustable workspace", "office"), ("web camera", "sharp video with built-in microphone", "office")]
BRANDS = ["Acme", "Northstar", "Zenith", "Kite", "Orbital", "Mosaic"]

def request(base, path, method="GET", body=None, content_type="application/json"):
    data = None if body is None else body.encode()
    req = urllib.request.Request(f"{base}{path}", data=data, method=method, headers={"content-type": content_type})
    with urllib.request.urlopen(req, timeout=180) as r:
        return r.read().decode()

def documents():
    for i in range(DOCS):
        title, description, category = PRODUCTS[i % len(PRODUCTS)]
        yield f"product-{i:07d}", {"title": title, "description": description, "brand": BRANDS[i % len(BRANDS)], "price": 19 + (i * 17) % 480, "category": category}

def load(base, index):
    try: request(base, f"/{index}", "DELETE")
    except Exception: pass
    request(base, f"/{index}", "PUT", json.dumps(MAPPING))
    batch = []
    started = time.perf_counter()
    for doc_id, source in documents():
        batch.extend((json.dumps({"index": {"_id": doc_id}}), json.dumps(source)))
        if len(batch) >= BATCH * 2:
            suffix = "?refresh=wait_for" if base == ES else ""
            request(base, f"/{index}/_bulk{suffix}", "POST", "\n".join(batch) + "\n", "application/x-ndjson")
            batch = []
    if batch:
        suffix = "?refresh=wait_for" if base == ES else ""
        request(base, f"/{index}/_bulk{suffix}", "POST", "\n".join(batch) + "\n", "application/x-ndjson")
    return round(time.perf_counter() - started, 3)

def query(base, index):
    body = {"query": {"bool": {"must": {"match": {"title": "wireless headphones"}}, "filter": [{"term": {"brand": "Acme"}}, {"range": {"price": {"lte": 400}}}]}}, "size": 10}
    started = time.perf_counter()
    value = json.loads(request(base, f"/{index}/_search", "POST", json.dumps(body)))
    return (time.perf_counter() - started) * 1000, len(value.get("hits", {}).get("hits", []))

def quality_check(base, index):
    body = {"query": {"match": {"title": "wireless headphones"}}, "size": 10}
    value = json.loads(request(base, f"/{index}/_search", "POST", json.dumps(body)))
    hits = value.get("hits", {}).get("hits", [])
    return {"ids": [hit.get("_id") for hit in hits], "sources": [hit.get("_source") for hit in hits], "all_have_source": all(bool(hit.get("_source")) for hit in hits)}

def update_documents(base, index):
    started = time.perf_counter()
    for i in range(UPDATE_DOCS):
        doc_id = f"product-{i:07d}"
        body = {"doc": {"price": 29 + (i * 19) % 470, "updated": True}}
        request(base, f"/{index}/_update/{doc_id}", "POST", json.dumps(body))
    return round(time.perf_counter() - started, 3)

def read_document(base, index, i):
    started = time.perf_counter()
    value = json.loads(request(base, f"/{index}/_doc/product-{i % DOCS:07d}"))
    return (time.perf_counter() - started) * 1000, bool(value.get("_source"))

def mixed_request(base, index, i):
    operation = i % 10
    if operation < 4:
        elapsed, _ = query(base, index)
        return "search", elapsed, True
    if operation < 7:
        elapsed, has_source = read_document(base, index, i)
        return "read", elapsed, has_source
    if operation < 9:
        started = time.perf_counter()
        doc_id = f"product-{i % DOCS:07d}"
        request(base, f"/{index}/_update/{doc_id}", "POST", json.dumps({"doc": {"updated": True}}))
        return "update", (time.perf_counter() - started) * 1000, True
    started = time.perf_counter()
    doc_id = f"product-{(DOCS - 1 - i) % DOCS:07d}"
    request(base, f"/{index}/_doc/{doc_id}", "DELETE")
    return "delete", (time.perf_counter() - started) * 1000, True

def mixed_workload(base, index, concurrency):
    started = time.perf_counter()
    samples = {"search": [], "read": [], "update": [], "delete": []}
    source_hits = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        for operation, elapsed, has_source in pool.map(lambda i: mixed_request(base, index, i), range(MIXED_REQUESTS)):
            samples[operation].append(elapsed)
            source_hits += int(has_source)
    elapsed = time.perf_counter() - started
    summary = {"requests": MIXED_REQUESTS, "wall_seconds": round(elapsed, 3), "throughput_rps": round(MIXED_REQUESTS / elapsed, 2), "source_responses": source_hits, "operations": {}}
    for operation, values in samples.items():
        if values:
            values.sort()
            summary["operations"][operation] = {"requests": len(values), "p50_ms": round(statistics.median(values), 2), "p95_ms": round(values[int(len(values) * .95) - 1], 2)}
    return summary

def latency(base, index, concurrency):
    for _ in range(max(3, concurrency)): query(base, index)
    samples = []
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        for elapsed, _ in pool.map(lambda _: query(base, index), range(100)):
            samples.append(elapsed)
    samples.sort()
    return {"requests": len(samples), "wall_seconds": round(time.perf_counter() - started, 3), "throughput_rps": round(len(samples) / (time.perf_counter() - started), 2), "p50_ms": round(statistics.median(samples), 2), "p95_ms": round(samples[int(len(samples) * .95) - 1], 2), "p99_ms": round(samples[int(len(samples) * .99) - 1], 2), "min_ms": round(samples[0], 2), "max_ms": round(samples[-1], 2)}

def main():
    native_runs = [load(ES, ES_INDEX) for _ in range(REPEATS)]
    gateway_runs = [load(GW, GW_INDEX) for _ in range(REPEATS)]
    results = {"dataset_documents": DOCS, "batch_documents": BATCH, "index_repeats": REPEATS, "native_elasticsearch": {"index_seconds": native_runs[0], "index_runs_seconds": native_runs, "queries": {}}, "qdrant_gateway": {"index_seconds": gateway_runs[0], "index_runs_seconds": gateway_runs, "queries": {}}}
    for name, base, index in (("native_elasticsearch", ES, ES_INDEX), ("qdrant_gateway", GW, GW_INDEX)):
        results[name]["updates_seconds"] = update_documents(base, index)
    time.sleep(WARMUP_SECONDS)
    native_quality = quality_check(ES, ES_INDEX)
    gateway_quality = quality_check(GW, GW_INDEX)
    results["quality"] = {"native_ids": native_quality["ids"], "gateway_ids": gateway_quality["ids"], "same_ids": native_quality["ids"] == gateway_quality["ids"], "native_all_have_source": native_quality["all_have_source"], "gateway_all_have_source": gateway_quality["all_have_source"], "same_sources": native_quality["sources"] == gateway_quality["sources"]}
    for concurrency in CONCURRENCY:
        results["native_elasticsearch"]["queries"][str(concurrency)] = latency(ES, ES_INDEX, concurrency)
        results["qdrant_gateway"]["queries"][str(concurrency)] = latency(GW, GW_INDEX, concurrency)
    mixed_concurrency = max(CONCURRENCY)
    results["native_elasticsearch"]["mixed"] = mixed_workload(ES, ES_INDEX, mixed_concurrency)
    results["qdrant_gateway"]["mixed"] = mixed_workload(GW, GW_INDEX, mixed_concurrency)
    results["created_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    with open(OUTPUT, "w") as f: json.dump(results, f, indent=2); f.write("\n")
    print(json.dumps(results, indent=2))

if __name__ == "__main__": main()
