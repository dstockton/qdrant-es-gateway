# Reproducible benchmark

This stack runs native Elasticsearch, Qdrant, the gateway, and a containerized client/runner. The profile supports large corpora such as 500,000 documents in batches of 5,000, and concurrency levels 1, 10, and 50. It measures bulk setup, a representative update pass, full-source search/read responses, and a mixed workload of 40% search, 30% read, 20% update, and 10% delete operations. Resource limits total approximately four CPUs and fifteen GB of memory across the services.

Run on a benchmark host:

```bash
DOCS=50000 BATCH=500 ./run.sh
```

Results are written to `results/benchmark.json` and `results/resources.txt`. Set `ES_HEAP=1g` (or another explicit value) to control the Elasticsearch JVM heap. Record machine, filesystem, Docker version, image digests, CPU/memory limits, warm-up state, and relevance before comparing runs. The result includes indexing and update time, throughput, p50, p95, p99, min, and max latency, mixed operation breakdowns, source-response checks, and memory snapshots.

Set `REPEATS=2` to repeat each ingest within the same service lifetime; the JSON then includes `index_runs_seconds`, which helps distinguish service startup from steady-state bulk-ingest behaviour.
