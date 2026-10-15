# Three-engine correctness and latency comparison

This deliberately small deterministic profile compares Elasticsearch 8.15.0,
OpenSearch 3.9.0, and the gateway with Qdrant 1.19.2. All three receive the same
index name, mapping, documents, bulk request parameters, and read requests; only
the endpoint changes. It measures keyword/integer document retrieval and
filtering, **not full-text relevance or a general search-engine ranking**.

```sh
cd benchmark/three-engine
docker compose -p qeg-balanced -f compose.yml up -d --build
# Wait until all three HTTP endpoints answer before starting.
DOCS=10000 TRIALS=5 REQUESTS=100 CONCURRENCY=1,8 python3 run.py
# Preserve evidence before removing this benchmark's disposable volumes.
docker compose -p qeg-balanced -f compose.yml images > results/images.txt
docker compose -p qeg-balanced -f compose.yml config > results/config.yml
docker info > results/docker-info.txt
# Only after retaining results:
docker compose -p qeg-balanced -f compose.yml down -v
```

Use an idle host with at least 20 GiB available disk and sufficient Docker VM
memory for all services. The compose profile caps each native engine at two CPUs
and 4 GiB. Qdrant plus the gateway share the same aggregate cap (1.5 + 0.5 CPUs,
3.5 + 0.5 GiB). These are limits, not reservations; record the Docker VM's actual
resources. The gateway is built with the repository's release Dockerfile. There
is one shard and no replicas in the native mappings, and one Qdrant replica.
SQLite stores the single gateway's small catalog; Redis/Valkey is not needed for
this single-replica data-plane experiment.

Every ingest trial deletes and recreates the benchmark index. Bulk requests use
`refresh=false` identically. The runner reports both accepted-ingest duration and
duration through explicit refresh plus exact count verification. Qdrant's
synchronous write acknowledgment differs from Lucene refresh visibility; these
are stated endpoint-visible measurements, not claims of identical durability.
After the final ingest, the runner waits 15 seconds, then warms every engine
before each measurement. It does not force-merge any engine or change index
settings for a favorable result.

Five repeated trials interleave the engines in seeded shuffled order per
workload and concurrency. Requests use persistent HTTP connections per client
worker. Each trial reports raw latency samples, p50, nearest-rank p95, response
bytes and achieved throughput. Throughput includes client-side correctness
checks; latency measures the HTTP round trip through reading the full response,
excluding JSON decoding and assertions. All responses must match independently
calculated IDs, full sources or aggregation counts. Errors abort the run and
leave partial evidence with `status: failed`; they are never counted as wins.
Sorted pages use unique integer sort keys and `search_after`, avoiding ambiguous
ties. Their total-hit fields are not compared because pagination-total semantics
are a separate compatibility question; full returned pages are checked.

`docker stats` snapshots after each workload record idle resident resource use,
not peak memory or per-request CPU. Record active/peak observations separately
when making resource claims. Engine root responses record reported versions;
retain image digests and the gateway Git revision alongside the JSON. No dataset
or generated results need to be committed.

Environment overrides: `ES_URL`, `OS_URL`, `GATEWAY_URL`, `DOCS`, `BATCH`,
`REQUESTS`, `TRIALS`, `CONCURRENCY`, `SETTLE_SECONDS`, `OUTPUT`. The harness
**deletes `balanced_benchmark` at the configured endpoints**; use disposable
instances, never production endpoints. The default corpus is 10,000 documents
with 20 balanced categories and approximately 200 bytes of source text each.
