#!/usr/bin/env python3
"""Summarize retained raw trials without selecting the fastest trial."""
import json
import statistics
import sys
from pathlib import Path


def summarize(path):
    data = json.loads(Path(path).read_text())
    if data.get("status") != "passed":
        raise ValueError("Only completed, correct runs can be summarized")
    groups = {}
    for row in data["measurements"]:
        if not row.get("all_responses_correct"):
            raise ValueError("A trial did not pass correctness checks")
        key = (row["workload"], row["concurrency"], row["engine"])
        groups.setdefault(key, []).append(row)
    rows = []
    for (workload, concurrency, engine), trials in sorted(groups.items()):
        rows.append({"workload": workload, "concurrency": concurrency, "engine": engine,
                     "trials": len(trials),
                     **{metric: statistics.median(t[metric] for t in trials)
                        for metric in ("p50_ms", "p95_ms", "throughput_rps")},
                     "trial_p50_range_ms": [min(t["p50_ms"] for t in trials), max(t["p50_ms"] for t in trials)]})
    return {"config": data["config"], "measurements": rows,
            "visible_ingest_documents_per_second": {
                engine: statistics.median(row["visible_documents_per_second"] for row in data["ingest"] if row["engine"] == engine)
                for engine in sorted({row["engine"] for row in data["ingest"]})}}


if __name__ == "__main__":
    print(json.dumps({Path(path).stem: summarize(path) for path in sys.argv[1:]}, indent=2))
