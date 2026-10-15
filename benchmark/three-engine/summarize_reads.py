#!/usr/bin/env python3
"""Combine every trial from completed read-only rounds; never pick fastest runs."""
import json
import statistics
import sys
from pathlib import Path


def summarize(paths):
    groups = {}
    configs = []
    for path in paths:
        data = json.loads(Path(path).read_text())
        if data.get("status") != "passed" or not data["config"].get("read_only"):
            raise ValueError(f"Not a completed read-only round: {path}")
        config = data["config"]
        configs.append(config)
        expected = {(engine, workload, concurrency, trial)
                    for engine in ("elasticsearch", "opensearch", "gateway")
                    for workload in ("point_read", "filtered_search", "sorted_pagination", "terms_aggregation", "filter_aggregation")
                    for concurrency in config["concurrency"]
                    for trial in range(config["trials"])}
        seen = set()
        for row in data["measurements"]:
            key = (row["engine"], row["workload"], row["concurrency"], row["trial"])
            if key in seen or not row.get("all_responses_correct") or row["requests"] != config["requests_per_trial"]:
                raise ValueError("Duplicate, incorrect, or incomplete trial")
            seen.add(key)
            groups.setdefault(key[:3], []).append(row)
        if seen != expected:
            raise ValueError("Missing or unexpected trials")
    if not configs or any(config != configs[0] for config in configs):
        raise ValueError("Round configurations must match")
    return [{"engine": engine, "workload": workload, "concurrency": concurrency,
             "trials": len(rows),
             **{metric: statistics.median(row[metric] for row in rows)
                for metric in ("p50_ms", "p95_ms", "throughput_rps")},
             "p50_range_ms": [min(row["p50_ms"] for row in rows), max(row["p50_ms"] for row in rows)]}
            for (engine, workload, concurrency), rows in sorted(groups.items())]


if __name__ == "__main__":
    print(json.dumps(summarize(sys.argv[1:]), indent=2))
