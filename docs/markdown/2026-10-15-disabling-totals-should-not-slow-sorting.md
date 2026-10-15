# Asking for less should not make sorting slower

A client that only needs the next page can set `track_total_hits: false` and
avoid asking for the total number of matching documents. In the gateway, that
apparently cheaper request had an expensive surprise: it disabled indexed
sorting and fetched every matching document from Qdrant instead.

On our 10,000-product catalogue, a single-client sorted page took **45.340 ms**.
After separating the sorting decision from the total-count decision, the same
request took **0.689 ms**. Both requests return the same twenty products, source
fields and sort values, with no `hits.total` field.

### Keep the page, skip the count

The gateway now uses its guarded indexed sorting path whether or not totals are
requested. When totals are disabled, it skips the exact count. A zero-size
aggregation response also skips the otherwise unused hit lookup. These are
request-local changes: no result cache, new database or Redis service is needed.

The existing safety checks still apply. Eligible scalar numeric fields can use
Qdrant's indexed ordering; unsupported values, old documents without eligibility
metadata and ambiguous page-boundary ties keep the complete-scan fallback.
Disabling totals does not relax sorting correctness or provide snapshot isolation
while documents change.

We measured **old, new, new, old** images against the same retained corpus. Each
image contributes six trial medians per table cell. Values below are median
trial p50 milliseconds, shown as **one client / eight clients**.

| Gateway operation, totals disabled | Before | After |
|---|---:|---:|
| Read one document | 0.384 / 0.842 | 0.374 / 0.769 |
| Filtered, sorted page | 2.888 / 10.093 | 0.691 / 1.797 |
| Page after a sort cursor | 45.340 / 305.877 | 0.689 / 1.833 |
| Keyword terms buckets | 0.870 / 2.470 | 0.675 / 1.970 |
| Two filter counts | 1.102 / 3.305 | 0.858 / 2.954 |

The point-read request is unaffected by the totals option; its smaller movement
helps show ordinary measurement variation. The large sorting improvement removes
an identified full scan. It does **not** establish that the gateway is faster
than the native engines.

### Native engines still lead

All three engines received identical requests within this disabled-total profile;
only the endpoint changed. Here are the final candidate-round results, including
every workload rather than selecting the gateway's strongest result.

| Operation | Elasticsearch | OpenSearch | Gateway + Qdrant |
|---|---:|---:|---:|
| Read one document | 0.307 / 0.599 | 0.289 / 0.579 | 0.374 / 0.769 |
| Filtered, sorted page | 0.487 / 1.082 | 0.468 / 1.020 | 0.691 / 1.797 |
| Page after a sort cursor | 0.554 / 1.185 | 0.442 / 1.028 | 0.689 / 1.833 |
| Keyword terms buckets | 0.355 / 0.863 | 0.354 / 0.856 | 0.675 / 1.970 |
| Two filter counts | 0.446 / 2.745 | 0.494 / 2.745 | 0.858 / 2.954 |

Both native engines remain faster in all ten workload/concurrency combinations.
For applications already using Qdrant, this fix makes requesting only a page
substantially more practical. It is not a reason to promise lower latency than
Elasticsearch or OpenSearch.

We separately repeated the four-round comparison with exact totals enabled.
That profile passed another 72,000 response checks, with no material measured
regression: single-client filtered search changed from 0.699 to 0.701 ms and
sorted pagination from 1.010 to 1.011 ms. Exact totals and disabled totals are
separate profiles; we did not give the gateway a cheaper request than its peers.
Across both profiles, **144,000 measured responses passed** checks for IDs, full
sources, ordering, bucket counts, and either exact totals or their absence.

### Reproduce it

Use the [three-engine harness](https://github.com/dstockton/qdrant-es-gateway/tree/main/benchmark/three-engine)
with Elasticsearch 8.15.0, OpenSearch 3.9.0 and Qdrant 1.19.2. Build both gateway
images before timing and populate the disposable 10,000-document corpus once.
Each native engine has a two-CPU, 4-GiB cap; Qdrant and the gateway share the same
total cap. Native cache settings remain at their defaults.

```sh
# From benchmark/three-engine, after loading data and waiting for readiness:
TRACK_TOTAL_HITS=false ROUND=old-1 REQUESTS=200 TRIALS=3 WARMUP=40 \
  OUTPUT=results/old-1.json python3 compare_reads.py
# Replace only the gateway image, preserving volumes, and wait for readiness.
# Repeat as new-1, new-2, then old-2.
python3 summarize_reads.py results/old-1.json results/old-2.json
python3 summarize_reads.py results/new-1.json results/new-2.json
# Repeat all four rounds with TRACK_TOTAL_HITS=true into separate output files.
```

Each round interleaves all engines, uses persistent connections and warms every
trial. No builds or ingestion run during timing. The shared Docker host and small
scalar catalogue limit what these measurements establish; this is not a text,
vector, durability or production-capacity benchmark.

The [evidence manifest](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-disabled-totals/manifest.json)
links all eight raw rounds, revisions and image provenance. Separate summaries
retain [disabled-total results](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-disabled-totals/disabled-summary.json)
and [exact-total results](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-disabled-totals/exact-summary.json),
including p95 latency, throughput and trial ranges. The improvement is concrete:
a client asking for less work no longer accidentally triggers a full scan.
