# Sorting twenty records should not fetch ten thousand

Our first three-engine benchmark exposed a straightforward problem. To return
20 sorted products, the gateway fetched all 10,000 matching documents from
Qdrant. It also fetched those documents to count a handful of categories.
The answers were correct, but moving and reducing all that data was expensive:
median sorted-page latency was 55.6 ms, and terms aggregation took 39.1 ms.
Elasticsearch and OpenSearch completed those operations in roughly 1–2 ms.

We changed where the work happens. For compatible numeric fields, Qdrant now
orders the records and returns the requested page. Keyword buckets use exact
Qdrant facets; simple filter counts use its count API. Independent reads run
concurrently, and a count-only request does not fetch an unused document.
No client request changes, result cache, or Redis/Valkey service were needed.

Each cell shows **one client / eight clients**, in milliseconds.

| Operation | Elasticsearch | OpenSearch | Gateway + Qdrant |
|---|---:|---:|---:|
| Read one document | 0.466 / 1.194 | 0.428 / 1.047 | 0.502 / 1.298 |
| Filtered, sorted page | 1.023 / 2.960 | 0.939 / 3.251 | 0.917 / 3.190 |
| Page after a sort cursor | 0.948 / 2.263 | 0.733 / 2.602 | 1.228 / 3.565 |
| Keyword terms buckets | 0.586 / 1.996 | 0.605 / 1.791 | 1.068 / 4.302 |
| Two filter counts | 0.714 / 2.928 | 0.718 / 2.854 | 1.135 / 4.167 |

These are median trial p50 latencies in milliseconds, measured with one and
eight concurrent clients. Every measured response passed correctness checks:
IDs, full sources, sort values, exact totals, bucket order and counts. We kept
all trials, including slower ones. The [full summary](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15/summary.json)
contains p95 latency, throughput and the range of trial medians.

The optimized gateway reduced observed sorted-page latency to 1.228 ms,
about 45 times lower than the first run. Native engines also became faster
across runs, so that ratio is an observed before/after result, not an isolated
production speedup guarantee. The gateway narrowly led filtered search at one
client; that small margin is not a broad performance win. In this measured run, native engines had lower median latency in nine of the ten
workload/concurrency combinations. They also led searchable ingest throughput: roughly
31,900 documents/s for Elasticsearch, 41,600 for OpenSearch and 22,600 for the
gateway.

The clearer advantage was the observed memory footprint. During separate
five-second, eight-client filter-count probes, gateway + Qdrant used about
327 MiB, versus 973 MiB for Elasticsearch and 912 MiB for OpenSearch. Those are
single Docker memory observations, not peaks or production sizing estimates.
For a compact catalogue service already using Qdrant, this is a useful trade:
near-millisecond scalar queries with a smaller observed footprint. For the
lowest latency or fastest ingest in this profile, the native engines remain
stronger choices.

There is a correctness condition behind the speedup. New writes record which
fields have values the indexed operation can handle exactly. Queries check that
coverage before using the fast result. Old documents, mixed types, missing
numeric values, multiple sort fields and page-boundary ties use the complete
scan. Existing documents gain eligibility when rewritten; deploying the new
binary alone does not accelerate every old index. Existing indexes that already
own the new internal field retain their
original data and behavior. This remains stateless request processing; it does
not turn pagination into a snapshot during concurrent writes.

### Reproduce the comparison

The [container profile and runner](https://github.com/dstockton/qdrant-es-gateway/tree/main/benchmark/three-engine)
use Elasticsearch 8.15.0, OpenSearch 3.9.0 and Qdrant 1.19.2. Each native engine
has a two-CPU, 4-GiB cap; Qdrant and the gateway share the same total cap. Both
JVM heaps are 512 MiB. The dataset has 10,000 deterministic documents with
integer rank/price fields, 20 keyword categories and a description. All engines
receive the same mapping, documents and requests; only the endpoint changes.
This tests scalar catalogue operations, not text relevance or vector search.

```sh
cd benchmark/three-engine
docker compose -p qeg-balanced -f compose.yml up -d --build
# Once all three endpoints respond:
DOCS=10000 TRIALS=3 REQUESTS=40 CONCURRENCY=1,8 python3 run.py
python3 summarize.py results/three-engine.json
```

The runner uses persistent HTTP connections, warms each workload, shuffles engine
order with a fixed seed, and measures three trials of 40 requests. Bulk timings
include waiting until all documents are searchable. It deletes only its named
benchmark index, so use the disposable containers. All services share one Docker
VM; this is a small reproducible lab profile, not a production capacity estimate.

Download the [baseline trials](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15/baseline.json),
[optimized trials](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15/optimized.json) and
[build/resource provenance](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15/provenance.json).
The practical improvement is that compatible catalogue queries move a page or
counts across the network instead of the entire matching catalogue. SQLite was
not the measured bottleneck, so replacing its small metadata catalog would not
have addressed the expensive work this benchmark revealed.

A [later paired comparison](https://dstockton.github.io/qdrant-es-gateway/markdown/2026-10-15-measuring-small-read-path-changes.md)
examines warmup, native caches and smaller read-path changes.
