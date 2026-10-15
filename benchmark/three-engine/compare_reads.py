#!/usr/bin/env python3
"""Read-only trials on the corpus already loaded by run.py.

Run once per image/allocation round; do not build images during timing. Preserve
all rounds, including regressions. The same strict response checks as run.py apply.
"""
import concurrent.futures
import datetime
import json
import math
import os
import random
import statistics
import subprocess
import time
from pathlib import Path

import run as benchmark

REQUESTS = int(os.getenv("REQUESTS", "200"))
TRIALS = int(os.getenv("TRIALS", "3"))
WARMUP = int(os.getenv("WARMUP", "40"))
OUTPUT = Path(os.getenv("OUTPUT", "results/read-only.json"))
ROUND = os.getenv("ROUND", "unspecified")
REQUEST_CACHE = os.getenv("REQUEST_CACHE", "default")
WORKLOADS = ["point_read", "filtered_search", "sorted_pagination",
             "terms_aggregation", "filter_aggregation"]
SERVICES = ["elasticsearch", "opensearch", "qdrant", "gateway"]


def provenance():
    """Record images, limits and versions without host paths or credentials."""
    services = {}
    for service in SERVICES:
        raw = subprocess.check_output(
            ["docker", "inspect", f"qeg-balanced-{service}-1"], text=True)
        data = json.loads(raw)[0]
        env = dict(item.split("=", 1) for item in data["Config"]["Env"] if "=" in item)
        services[service] = {
            "image": data["Config"]["Image"], "image_id": data["Image"],
            "cpu_limit": data["HostConfig"]["NanoCpus"] / 1e9,
            "memory_limit_bytes": data["HostConfig"]["Memory"],
        }
        if service == "gateway":
            services[service]["tokio_worker_threads"] = env.get("TOKIO_WORKER_THREADS", "default")
            services[service]["read_transport"] = "grpc_opt_in" if env.get("QDRANT_GRPC_URL") else "rest"
    return services


def main():
    assert REQUESTS > 0 and TRIALS > 0 and WARMUP > 0
    assert REQUEST_CACHE in ("default", "false")
    if REQUEST_CACHE == "false":
        original_request = benchmark.request

        def uncached_request(base, path, *args, **kwargs):
            if path.endswith("/_search"):
                path += "?request_cache=false"
            return original_request(base, path, *args, **kwargs)

        benchmark.request = uncached_request
    rng = random.Random(20261015)
    result = {
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "round": ROUND,
        "config": {"read_only": True, "documents": benchmark.N,
                   "requests_per_trial": REQUESTS, "trials": TRIALS,
                   "warmup_per_trial": WARMUP, "concurrency": benchmark.CONCURRENCY,
                   "seed": 20261015, "persistent_http_per_worker": True,
                   "request_cache": REQUEST_CACHE,
                   "track_total_hits": benchmark.TRACK_TOTAL_HITS},
        "provenance": provenance(), "measurements": [],
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    try:
        # Existing roots must answer before measurements. This is not a readiness
        # retry loop; callers must wait after replacing a service.
        for base in benchmark.ENDPOINTS.values():
            benchmark.request(base, "/", "GET")
        for concurrency in benchmark.CONCURRENCY:
            for workload in WORKLOADS:
                for trial in range(TRIALS):
                    engines = list(benchmark.ENDPOINTS)
                    rng.shuffle(engines)
                    for engine in engines:
                        base = benchmark.ENDPOINTS[engine]
                        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                            list(pool.map(lambda i: benchmark.operation(base, workload, i), range(WARMUP)))
                            started = time.perf_counter()
                            samples = list(pool.map(lambda i: benchmark.operation(base, workload, i), range(REQUESTS)))
                            wall = time.perf_counter() - started
                        values = sorted(item[0] for item in samples)
                        row = {"engine": engine, "workload": workload,
                               "concurrency": concurrency, "trial": trial,
                               "requests": REQUESTS, "wall_seconds": wall,
                               "p50_ms": statistics.median(values),
                               "p95_ms": values[math.ceil(len(values) * .95) - 1],
                               "throughput_rps": len(values) / wall,
                               "latencies_ms": values,
                               "response_bytes": sum(item[1] for item in samples),
                               "all_responses_correct": True}
                        result["measurements"].append(row)
                        print(engine, workload, concurrency, trial, round(row["p50_ms"], 3), flush=True)
                        OUTPUT.write_text(json.dumps(result, indent=2) + "\n")
        result["status"] = "passed"
    except Exception as error:
        result["status"] = "failed"
        result["failure"] = str(error)
        raise
    finally:
        OUTPUT.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
