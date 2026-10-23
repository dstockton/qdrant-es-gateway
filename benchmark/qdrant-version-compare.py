#!/usr/bin/env python3
"""Compare two Qdrant images through one gateway image on disposable storage."""
import concurrent.futures
import collections
import hashlib
import http.client
import json
import os
import statistics
import subprocess
import threading
import time
import uuid
from pathlib import Path

VERSIONS = ("1.19.2", "1.20.0", "1.20.0", "1.19.2")
GATEWAY_IMAGE = os.environ.get("GATEWAY_IMAGE", "qeg-gateway:qdrant-1-20-compare")
DOCS = int(os.environ.get("DOCS", "10000"))
REQUESTS = int(os.environ.get("REQUESTS", "5000"))
WORKERS = int(os.environ.get("WORKERS", "8"))
OUTPUT = Path(os.environ.get("OUTPUT", "qdrant-version-compare.json"))
INDEX = "qdrant_version_probe"
MAPPING = {"mappings": {"properties": {"title": {"type": "text"}, "category": {"type": "keyword"}, "number": {"type": "integer"}}}}
thread_local = threading.local()


def docker(*args):
    return subprocess.check_output(("docker", *args), text=True).strip()


def request(port, method, path, body=None, content_type="application/json"):
    conn = getattr(thread_local, "conn", None)
    if conn is None or getattr(thread_local, "port", None) != port:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        thread_local.conn, thread_local.port = conn, port
    data = None if body is None else body.encode()
    started = time.perf_counter_ns()
    try:
        conn.request(method, path, body=data, headers={"Content-Type": content_type})
        response = conn.getresponse()
        payload = response.read()
    except (OSError, http.client.HTTPException):
        conn.close()
        thread_local.conn = None
        raise
    elapsed = (time.perf_counter_ns() - started) / 1e6
    if response.status >= 300:
        raise RuntimeError(f"{method} {path}: HTTP {response.status}: {payload[:300]!r}")
    return elapsed, json.loads(payload) if payload else {}


def wait_ready(port):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            request(port, "GET", "/")
            return
        except (OSError, http.client.HTTPException, RuntimeError):
            time.sleep(.25)
    raise TimeoutError(f"gateway on {port} did not start")


def percentile(values, p):
    ordered = sorted(values)
    return round(ordered[max(0, int(len(ordered) * p + .999999) - 1)], 3)


def measure(port, kind):
    def one(i):
        if kind == "search":
            body = json.dumps({"query": {"match": {"title": "wireless headphones"}}, "size": 10})
            latency, result = request(port, "POST", f"/{INDEX}/_search", body)
            assert len(result["hits"]["hits"]) == 10
        else:
            latency, result = request(port, "GET", f"/{INDEX}/_doc/product-{i % DOCS:06d}")
            assert result["found"] and result["_source"]["number"] == i % DOCS
        return latency

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        list(pool.map(one, range(40)))
        started = time.perf_counter()
        samples = list(pool.map(one, range(REQUESTS)))
        wall = time.perf_counter() - started
    histogram = collections.Counter(f"{int(value * 10) / 10:.1f}" for value in samples)
    return {"requests": REQUESTS, "workers": WORKERS, "p50_ms": round(statistics.median(samples), 3),
            "p95_ms": percentile(samples, .95), "throughput_rps": round(REQUESTS / wall, 2),
            "wall_seconds": round(wall, 3), "min_ms": round(min(samples), 3),
            "max_ms": round(max(samples), 3),
            "histogram_0_1ms_bins": dict(sorted(histogram.items(), key=lambda item: float(item[0])))}


def load(port):
    request(port, "PUT", f"/{INDEX}", json.dumps(MAPPING))
    started = time.perf_counter()
    for offset in range(0, DOCS, 250):
        lines = []
        for i in range(offset, min(offset + 250, DOCS)):
            lines.append(json.dumps({"index": {"_id": f"product-{i:06d}"}}))
            lines.append(json.dumps({"title": "wireless headphones for travel" if i % 2 else "compact usb charger",
                                     "category": "audio" if i % 2 else "power", "number": i}))
        _, response = request(port, "POST", f"/{INDEX}/_bulk", "\n".join(lines) + "\n", "application/x-ndjson")
        assert not response.get("errors"), response
    request(port, "POST", f"/{INDEX}/_refresh")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        _, result = request(port, "GET", f"/{INDEX}/_count")
        if result["count"] == DOCS:
            break
        time.sleep(.2)
    else:
        raise RuntimeError("indexed document count did not become visible")
    return round(time.perf_counter() - started, 3)


def run_round(network, prefix, version, number):
    qname, gname = f"{prefix}-q{number}", f"{prefix}-g{number}"
    try:
        docker("run", "-d", "--name", qname, "--network", network, "--network-alias", "qdrant",
               "--cpus", "1.5", "--memory", "2g", "--tmpfs", "/qdrant/storage:rw,size=1g",
               f"qdrant/qdrant:v{version}")
        docker("run", "-d", "--name", gname, "--network", network, "--cpus", "0.5", "--memory", "512m",
               "--tmpfs", "/data:rw,uid=10001,gid=10001,mode=700,size=256m",
               "-e", "QDRANT_URL=http://qdrant:6333", "-e", "METADATA_DB=/data/gateway.db",
               "-p", "127.0.0.1::9200", GATEWAY_IMAGE)
        binding = docker("port", gname, "9200/tcp")
        port = int(binding.rsplit(":", 1)[1])
        wait_ready(port)
        ingest = load(port)
        time.sleep(2)
        search = measure(port, "search")
        get = measure(port, "get")
        _, count = request(port, "GET", f"/{INDEX}/_count")
        assert count["count"] == DOCS
        return {"version": version, "order": number + 1, "ingest_seconds": ingest,
                "visible_count": count["count"], "search": search, "get": get}
    finally:
        subprocess.run(("docker", "rm", "-f", gname, qname), capture_output=True, text=True)


def main():
    prefix = "qeg-qv-" + uuid.uuid4().hex[:8]
    network = prefix + "-net"
    docker("network", "create", network)
    result = {"status": "incomplete", "method": "fresh isolated storage; A-B-B-A; gateway HTTP; deterministic documents; 40 warmups per workload",
              "versions": list(VERSIONS), "docs": DOCS, "requests_per_workload_per_round": REQUESTS,
              "workers": WORKERS, "gateway_image": GATEWAY_IMAGE,
              "gateway_image_id": docker("image", "inspect", "--format", "{{.Id}}", GATEWAY_IMAGE),
              "cargo_lock_sha256": hashlib.sha256((Path(__file__).resolve().parents[1] / "Cargo.lock").read_bytes()).hexdigest(),
              "docker_vm": {"logical_cpus": int(docker("info", "--format", "{{.NCPU}}")),
                            "memory_bytes": int(docker("info", "--format", "{{.MemTotal}}"))},
              "limits": {"qdrant": {"cpus": 1.5, "memory_bytes": 2147483648},
                         "gateway": {"cpus": 0.5, "memory_bytes": 536870912}},
              "qdrant_image_digests": {v: docker("image", "inspect", "--format", "{{index .RepoDigests 0}}", f"qdrant/qdrant:v{v}") for v in set(VERSIONS)},
              "rounds": []}
    try:
        for n, version in enumerate(VERSIONS):
            result["rounds"].append(run_round(network, prefix, version, n))
            OUTPUT.parent.mkdir(parents=True, exist_ok=True)
            OUTPUT.write_text(json.dumps(result, indent=2) + "\n")
            print(f"round {n + 1}: Qdrant {version} complete", flush=True)
        result["status"] = "passed"
    finally:
        docker("network", "rm", network)
        OUTPUT.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"status": result["status"], "rounds": [{k: v for k, v in r.items() if k not in ("search", "get")}
                                                        | {"search": {k: v for k, v in r["search"].items() if k != "histogram_0_1ms_bins"},
                                                           "get": {k: v for k, v in r["get"].items() if k != "histogram_0_1ms_bins"}}
                                                        for r in result["rounds"]]}, indent=2))


if __name__ == "__main__":
    main()
