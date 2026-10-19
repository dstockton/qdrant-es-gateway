---
layout: post
title: "Testing a cache before building it"
date: 2026-10-15
categories: [performance]
---

Would Valkey make this gateway fast enough to beat native search? Before adding
a production cache, we tested a narrower question: **is there enough latency
headroom in an ideal warm hit to justify the work?**

The answer is cautiously useful. A standalone synthetic probe returned aggregation
responses about **7–15% faster** than the faster native engine's median latency
across the corrected measurement rounds. Its aggregation p95 values were also
lower than both native engines in both rounds. This is a reason to investigate,
not a gateway performance win: the production gateway still trailed both native
engines in all ten measured workload/concurrency combinations.

### What the probe actually does

We captured 644 distinct, correctness-checked responses from the real gateway
using the public 10,000-document catalogue. A separate probe validates each
incoming request, hashes its identity and performs one Valkey Lua lookup. The
script checks synthetic control state and returns an already populated response.
The probe parses that response, updates `took` and serializes JSON normally.

Every request is a warm hit. There are no cache misses, changing documents,
invalidation, crash recovery or real writer coordination. Qdrant remains
provisioned but receives no reads on hits. The probe is deliberately named
`cache_probe` in the evidence, rather than “gateway + Qdrant”. It is not a
production feature, and its guard tests do not establish cache consistency.

The combined budget remains two CPUs and 4 GiB: probe 0.5 CPU/512 MiB, Valkey
0.25 CPU/256 MiB, and Qdrant 1.25 CPU/3328 MiB. Each native engine retains two
CPUs and 4 GiB. Normal production rounds instead allocate 0.5 CPU/512 MiB to the
gateway and 1.5 CPUs/3584 MiB to Qdrant. Native caching stays enabled.

### First, fix the measuring instrument

The initial probe run exposed a benchmark problem. For every filter-aggregation
response, the Python verifier recomputed the expected count by walking the entire
corpus. That work sat outside the nominal HTTP timer, but with concurrent clients
it still competed for Python's global interpreter lock and delayed other clients.
Trial throughput also included this verification work.

The corrected harness computes the same expected count once and retains the
strict response assertions. Correctness conclusions did not change. Historical
filter-aggregation timing rows need caution; we retain them with their original
validation profile instead of pooling old and new medians. This identified issue
concerns filter-count validation, not evidence that every historical workload was
invalid.

We then measured two corrected production rounds and two corrected probe rounds,
with native engines interleaved in each. Each profile passed **36,000 measured
response checks**. The earlier 36,000-response probe profile is separately retained.

### The available margin is small

The table gives each corrected probe round separately. Values are median trial
p50 milliseconds, shown as **Elasticsearch / OpenSearch / synthetic probe**.

| Aggregation | Clients | Round 1 | Round 2 |
|---|---:|---:|---:|
| Terms buckets | 1 | 0.360 / 0.347 / 0.307 | 0.362 / 0.354 / 0.315 |
| Two filter counts | 1 | 0.347 / 0.336 / 0.300 | 0.347 / 0.340 / 0.298 |
| Terms buckets | 8 | 0.888 / 0.840 / 0.783 | 0.815 / 0.822 / 0.738 |
| Two filter counts | 8 | 0.795 / 0.811 / 0.673 | 0.780 / 0.786 / 0.700 |

These are optimistic ceilings on potential performance, not a demonstrated
production advantage. Correct write fencing, epoch changes, miss publication and
recovery all have costs. Even a modest extra round trip or a lower hit rate could
erase the observed margin. A broader cache for document reads and sorted pages needs its own correctness
design and production measurements.

The [complete corrected probe summary]({{ '/assets/benchmarks/2026-10-15-cache-ceiling/summary-probe-v2.json' | relative_url }})
includes every participant and all five workloads, with p50, p95 and throughput
for both rounds. The [production summary]({{ '/assets/benchmarks/2026-10-15-cache-ceiling/summary-production-v2.json' | relative_url }})
shows the actual gateway alongside its own native controls. We have not combined
those separate profiles into an implementation speedup claim.

To reproduce the diagnostic, follow the
[standalone probe instructions](https://github.com/dstockton/qdrant-es-gateway/tree/main/benchmark/cache-upper-bound):
capture from the retained production image, build before timing, then measure
with `REQUESTS=200 TRIALS=3 WARMUP=40` and exact totals. Restore the production
image and original Qdrant allocation afterward. The
[evidence manifest]({{ '/assets/benchmarks/2026-10-15-cache-ceiling/manifest.json' | relative_url }})
records all six raw rounds, separate summaries, image provenance, the fixture
hash and resource budgets.

Valkey now has a measured reason to remain under consideration. The remaining
question is whether a correct cache can preserve enough of this small margin.
