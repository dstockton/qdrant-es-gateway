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
duration through explicit refresh plus exact count verification. If data is not
yet visible, every engine gets the same bounded polling loop (50 ms interval,
120-second timeout); poll counts and refresh-to-visible duration are retained. Qdrant's
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
ties. Their exact total-hit values and full returned pages are checked. Both native
JVM heaps are explicitly fixed at 512 MiB; memory caps remain 4 GiB each.

`docker stats` snapshots include only this profile's services, labeled by service
without container IDs or host details. They record idle resident resource use,
not peak memory or per-request CPU. Record active/peak observations separately
when making resource claims. Sanitized engine root responses record reported versions;
retain image digests and the gateway Git revision alongside the JSON. No dataset
or generated results need to be committed.

Environment overrides: `ES_URL`, `OS_URL`, `GATEWAY_URL`, `DOCS`, `BATCH`,
`REQUESTS`, `TRIALS`, `CONCURRENCY`, `SETTLE_SECONDS`, `OUTPUT`. The harness
**deletes `balanced_benchmark` at the configured endpoints**; use disposable
instances, never production endpoints. The default corpus is 10,000 documents
with 20 balanced categories and approximately 200 bytes of source text each.

## Retained comparison

The October 15 comparison uses three trials of 40 measured requests per engine,
workload and concurrency, plus warmups (`TRIALS=3 REQUESTS=40`). The default five
trials above are useful for a longer rerun. Summarize every retained trial with:

```sh
python3 summarize.py results/baseline.json results/optimized.json
```

The script reports the median trial p50, p95 and throughput, and the range of
trial medians. It refuses incomplete or failed runs. The gateway's reported
Elasticsearch version is a compatibility response, not its build version; use
the recorded gateway Git revision and container image digest for provenance.

## Read-only paired image comparisons

To separate read-path changes from ingestion and JVM warmup, first populate the
same disposable corpus with `run.py`, build both images before timing, and then
use `compare_reads.py` against the retained data. It never deletes or writes data.
The compose project name must remain `qeg-balanced` for image and limit provenance.

```sh
# Replace only the gateway image between rounds, preserving its catalog volume.
# Wait for the endpoint to answer after replacement. Do not build during timing.
ROUND=baseline-a REQUESTS=200 TRIALS=3 WARMUP=40 \
  OUTPUT=results/baseline-a.json python3 compare_reads.py
# Repeat with candidate-b, candidate-c, then baseline-d (A-B-B-A order).
```

Each round interleaves all three engines in seeded shuffled order for every
workload, concurrency and trial, uses the same correctness assertions as the
full benchmark, and records every latency sample plus container image IDs and
resource limits. Each trial creates persistent connections per worker and runs
40 unmeasured warmups by default. Keep all four rounds and compare the two
baseline rounds with both candidate rounds; a single fastest round is not an
improvement claim. Repeated native aggregation requests may use native request
caches; this is the default endpoint behavior, not an uncached engine comparison.

For a separate uncached diagnostic, set `REQUEST_CACHE=false`. This appends
`?request_cache=false` to every search URL for all three engines and records the
choice in the output. Preserve the default-cache results alongside this profile;
it answers a different question and must not replace the default comparison.

Elasticsearch and OpenSearch both cache eligible `size: 0` requests by default.
See the [Elasticsearch request-cache documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-request-cache.html)
and [OpenSearch request-cache documentation](https://docs.opensearch.org/latest/search-plugins/caching/request-cache/).
Record `/{index}/_stats/request_cache` before and after a diagnostic to verify
which cache path was exercised; disabling this cache does not disable every
internal engine cache.

Combine both baseline rounds, then both candidate rounds, using all six trial
medians per engine/workload/concurrency:

```sh
python3 summarize_reads.py results/baseline-a.json results/baseline-d.json
python3 summarize_reads.py results/candidate-b.json results/candidate-c.json
```
