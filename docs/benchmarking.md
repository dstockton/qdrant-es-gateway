# Benchmarking

The reproducible comparative benchmark is `benchmark/run.sh`. It starts native Elasticsearch, Qdrant, the Rust gateway, and a containerized Python client on one Docker host. A large profile loads 500,000 synthetic products in batches of 5,000, measures updates and full-source reads, then runs a mixed workload of searches, reads, updates, and deletes at concurrency 50. Compose limits the services to approximately four CPUs and fifteen GB of memory, while `ES_HEAP` explicitly controls the Elasticsearch JVM heap for a comparable memory profile.

```bash
cd benchmark
DOCS=50000 BATCH=500 ./run.sh
```

Results are written to `benchmark/results/benchmark.json` and `benchmark/results/resources.txt`. Re-run with the same corpus, image versions, warm-up behaviour, and host profile when comparing changes.
