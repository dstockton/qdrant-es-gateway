#!/usr/bin/env python3
"""Small, repeatable three-node failure/recovery workload runner."""

import argparse
import concurrent.futures
import json
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.request


MAPPING = {
    "settings": {"number_of_replicas": 2},
    "mappings": {
        "properties": {
            "title": {"type": "text"},
            "description": {"type": "text"},
            "category": {"type": "keyword"},
            "price": {"type": "float"},
        }
    },
}


def http(base, path, method="GET", body=None, content_type="application/json"):
    data = None if body is None else body.encode()
    req = urllib.request.Request(
        f"{base}{path}",
        data=data,
        method=method,
        headers={"content-type": content_type},
    )
    try:
        with urllib.request.urlopen(req, timeout=4) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return 0, ""


def retry(base, path, method="GET", body=None, content_type="application/json", attempts=30):
    for attempt in range(attempts):
        status, response = http(base, path, method, body, content_type)
        if status and status < 500:
            return status, response
        time.sleep(min(0.2 * (attempt + 1), 1.0))
    return status, response


def document(i):
    return {
        "title": "wireless headphones" if i % 2 else "mechanical keyboard",
        "description": "noise cancelling audio with long battery life",
        "category": "audio" if i % 2 else "office",
        "price": 19 + (i * 17) % 480,
    }


def seed(base, index, count):
    retry(base, f"/{index}", "DELETE")
    status, _ = retry(base, f"/{index}", "PUT", json.dumps(MAPPING), attempts=120)
    if status not in (200, 201):
        raise RuntimeError(f"index creation failed: {status}")
    lines = []
    for i in range(count):
        lines.append(json.dumps({"index": {"_id": f"product-{i:07d}"}}))
        lines.append(json.dumps(document(i)))
        if len(lines) >= 1000:
            status, _ = retry(base, f"/{index}/_bulk?refresh=wait_for", "POST", "\n".join(lines) + "\n", "application/x-ndjson", attempts=120)
            if status not in (200, 201):
                raise RuntimeError(f"bulk seed failed: {status}")
            lines = []
    if lines:
        status, _ = retry(base, f"/{index}/_bulk?refresh=wait_for", "POST", "\n".join(lines) + "\n", "application/x-ndjson", attempts=120)
        if status not in (200, 201):
            raise RuntimeError(f"bulk seed failed: {status}")


def operation(base, index, kind, sequence, count):
    started = time.perf_counter()
    if kind == "read":
        status, _ = http(base, f"/{index}/_doc/product-{sequence % count:07d}")
    elif kind == "search":
        body = {"query": {"match": {"title": "wireless headphones"}}, "size": 10}
        status, _ = http(base, f"/{index}/_search", "POST", json.dumps(body))
    elif kind == "insert":
        doc_id = f"stream-{sequence:09d}"
        status, _ = http(base, f"/{index}/_doc/{doc_id}?refresh=false", "PUT", json.dumps(document(sequence)))
    elif kind == "update":
        doc_id = f"product-{sequence % count:07d}"
        status, _ = http(base, f"/{index}/_update/{doc_id}", "POST", json.dumps({"doc": {"price": 42.0, "updated": True}}))
    else:
        doc_id = f"product-{sequence % count:07d}"
        status, _ = http(base, f"/{index}/_doc/{doc_id}?refresh=false", "DELETE")
    elapsed_ms = (time.perf_counter() - started) * 1000
    ok = 200 <= status < 300 or (kind == "delete" and status == 404)
    return kind, elapsed_ms, ok


def run_workload(base, index, scenario, duration, concurrency, count, fail_after, fail_for, compose_file, service):
    started = time.perf_counter()
    events = {"failure_start": None, "failure_end": None}
    samples = {kind: [] for kind in ("search", "read", "insert", "update", "delete")}
    failures = 0
    sequence = 0

    def next_kind(n):
        if scenario != "mixed":
            return scenario
        slot = n % 10
        if slot < 4:
            return "search"
        if slot < 7:
            return "read"
        if slot < 9:
            return "update"
        return "delete"

    def disrupt_node():
        time.sleep(fail_after)
        events["failure_start"] = time.perf_counter()
        # SIGKILL models an abrupt node loss rather than a graceful drain.
        subprocess.run(["docker", "compose", "-f", compose_file, "kill", service], check=True)
        time.sleep(fail_for)
        subprocess.run(["docker", "compose", "-f", compose_file, "start", service], check=True)
        events["failure_end"] = time.perf_counter()

    controller = threading.Thread(target=disrupt_node, daemon=True)
    controller.start()
    while time.perf_counter() - started < duration:
        elapsed = time.perf_counter() - started
        if elapsed >= duration:
            break
        jobs = []
        for _ in range(concurrency * 4):
            kind = next_kind(sequence)
            jobs.append((kind, sequence))
            sequence += 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            results = list(pool.map(lambda job: operation(base, index, job[0], job[1], count), jobs))
        for kind, latency, ok in results:
            samples[kind].append((latency, ok))
            failures += int(not ok)
    controller.join(timeout=max(0, fail_after + fail_for - (time.perf_counter() - started)) + 2)
    if events["failure_start"] and not events["failure_end"]:
        subprocess.run(["docker", "compose", "-f", compose_file, "start", service], check=True)
        events["failure_end"] = time.perf_counter()
    elapsed = time.perf_counter() - started
    summary = {
        "scenario": scenario,
        "duration_seconds": round(elapsed, 2),
        "concurrency": concurrency,
        "requests": sum(len(v) for v in samples.values()),
        "failures": failures,
        "throughput_rps": round(sum(len(v) for v in samples.values()) / elapsed, 2),
        "failure_after_seconds": round(events["failure_start"] - started, 2) if events["failure_start"] else None,
        "recovery_after_seconds": round(events["failure_end"] - events["failure_start"], 2) if events["failure_end"] else None,
        "operations": {},
    }
    for kind, values in samples.items():
        if values:
            values.sort(key=lambda item: item[0])
            latencies = [item[0] for item in values]
            summary["operations"][kind] = {
                "requests": len(values),
                "successes": sum(1 for _, ok in values if ok),
                "p50_ms": round(statistics.median(latencies), 2),
                "p95_ms": round(latencies[max(0, int(len(latencies) * 0.95) - 1)], 2),
                "max_ms": round(latencies[-1], 2),
            }
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--index", required=True)
    parser.add_argument("--scenario", choices=["search", "insert", "update", "delete", "mixed"], required=True)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--docs", type=int, default=5000)
    parser.add_argument("--fail-after", type=float, default=8)
    parser.add_argument("--fail-for", type=float, default=10)
    parser.add_argument("--compose-file", required=True)
    parser.add_argument("--service", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    seed(args.base, args.index, args.docs)
    result = run_workload(args.base, args.index, args.scenario, args.duration, args.concurrency, args.docs, args.fail_after, args.fail_for, args.compose_file, args.service)
    with open(args.output, "w", encoding="utf-8") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
