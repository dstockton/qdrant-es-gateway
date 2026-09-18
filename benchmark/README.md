# Reproducible benchmark

This stack runs native Elasticsearch, Qdrant, the gateway, and a containerized client/runner. The profile supports large corpora such as 500,000 documents in batches of 5,000, and concurrency levels 1, 10, and 50. It measures bulk setup, a representative update pass, full-source search/read responses, and a mixed workload of 40% search, 30% read, 20% update, and 10% delete operations. Resource limits total approximately four CPUs and fifteen GB of memory across the services.

Run on a benchmark host:

```bash
DOCS=50000 BATCH=500 ./run.sh
```

Results are written to `results/benchmark.json`, `results/resources.txt`, and `results/disk-usage.txt`. The disk report measures the mounted service data directories while the containers are still running: Elasticsearch data, Qdrant storage, and gateway metadata are listed separately. For a fair projection comparison, report Qdrant searchable storage plus the optional durable source collection, and report Elasticsearch data separately from its configured JVM heap. Docker image layers are not included because they are deployment-cache details, not corpus footprint.

Set `ES_HEAP=1g` (or another explicit value) to control the Elasticsearch JVM heap. Record machine, filesystem, Docker version, image digests, CPU/memory limits, warm-up state, and relevance before comparing runs. The result includes indexing and update time, throughput, p50, p95, p99, min, and max latency, mixed operation breakdowns, source-response checks, and memory snapshots.

Set `REPEATS=2` to repeat each ingest within the same service lifetime; the JSON then includes `index_runs_seconds`, which helps distinguish service startup from steady-state bulk-ingest behaviour.

## Public catalogue datasets

The runner can import a prepared JSONL corpus instead of its synthetic products. Each line must contain `{"_id": "...", "_source": {...}}`; set `DATASET_JSONL=/datasets/catalog.jsonl`. The benchmark still uses the same client code and endpoint-only swap for native Elasticsearch and the gateway.

The repository includes an importer for the public [Qdrant H&M catalogue](https://huggingface.co/datasets/Qdrant/hm_ecommerce_products), which contains about 106,000 products and is published under CC BY 4.0. The parquet file and generated JSONL stay outside Git:

```bash
python3 -m venv /tmp/qeg-data-tools
/tmp/qeg-data-tools/bin/pip install pyarrow
curl -L -o /tmp/hm.parquet https://huggingface.co/datasets/Qdrant/hm_ecommerce_products/resolve/main/hm_ecommerce_products_enriched.parquet
python3 import_hm.py /tmp/hm.parquet datasets/hm.jsonl
DATASET_JSONL=/datasets/hm.jsonl SEARCH_TERM=dress DOCS=105126 BATCH=500 ./run.sh
```

The default `ES_BULK_REFRESH=wait_for` preserves the explicit refresh policy used by the existing benchmark workflow. To separate refresh-wait cost from raw bulk throughput, run a second profile with `ES_BULK_REFRESH=` and allow the warm-up period to complete before reads. Report both profiles when the application explicitly requests immediate visibility.

Open Food Facts is a second, much larger option: its food split is about 4.7 million rows and roughly 8 GB in parquet form. It is available under the Open Database License; use its attribution and share-alike terms when publishing derived data. Because it is too large for a routine CI run, use it as a separately provisioned benchmark corpus after adapting the importer to the selected fields. Do not commit either source dataset or generated corpus.

To benchmark the stateless two-collection source projection, run the same client unchanged with `DOCUMENT_PROJECTION=true`. Add `ASYNC_SEARCH_PROJECTION=true` to measure the write-heavy eventual-consistency profile: the authoritative document write is waited on, while sparse search projection work is accepted asynchronously. The benchmark should record source freshness separately from request latency when using this mode.

The gateway's two collections have different jobs. The searchable collection carries sparse text vectors and filter payloads; the optional `<index>_documents` collection is a low-overhead durable source store with a single zero vector and no payload indexes. This preserves a stateless gateway tier and makes the storage trade-off visible instead of hiding source data in a local cache. Compare both gateway directories together against Elasticsearch's data directory, then also show the searchable-only subtotal when the source already exists in another system.
