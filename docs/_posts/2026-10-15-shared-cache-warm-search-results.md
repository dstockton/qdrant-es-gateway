---
layout: post
title: "Repeated searches get faster; fresh queries still cost more"
date: 2026-10-15
categories: [performance]
---

The gateway now has a measured advantage for repeated, unchanged searches.
With its optional shared cache enabled, warm search medians were **12–30% lower
than the faster native engine** across our eight search/concurrency comparisons.
Point reads were close to parity, with only a 3–5% combined median advantage.
That advantage was not consistent in every round: in the second enabled round,
single-client point reads were about 2% slower than the faster native engine.

This is the actual gateway, not the earlier synthetic cache probe. It uses Qdrant
for documents and Valkey for shared cache entries, the authoritative catalog and
persistent write coordination. The trade-off is real: fresh queries and
write-followed-by-point-read cycles remained substantially slower than native
Elasticsearch and OpenSearch. This is useful for a read-heavy catalogue with
repeated queries, not a blanket search-engine win.

### Warm searches, same requests

We used Elasticsearch 8.15.0, OpenSearch 3.9.0 and Qdrant 1.19.2 with 10,000
synthetic catalogue documents. Every engine received the same requests and exact
total-hit requirements. Native request caches stayed at their defaults.

The table contains every warm workload. Values are median trial p50 milliseconds,
shown as **one client / eight clients**. All six trial medians from both
cache-enabled rounds contribute; we did not select the fastest trial.

| Operation | Elasticsearch | OpenSearch | Gateway + Qdrant + Valkey |
|---|---:|---:|---:|
| Read one document | 0.350 / 0.707 | 0.345 / 0.661 | 0.333 / 0.630 |
| Filtered, sorted page | 0.579 / 1.401 | 0.547 / 1.343 | 0.417 / 1.033 |
| Page after a sort cursor | 0.643 / 1.405 | 0.541 / 1.289 | 0.382 / 1.026 |
| Keyword terms buckets | 0.460 / 1.081 | 0.453 / 1.136 | 0.360 / 0.951 |
| Two filter counts | 0.416 / 0.981 | 0.409 / 1.057 | 0.339 / 0.800 |

Warm p95 values were also lower than both native engines in these ten cells.
The [complete enabled summary]({{ '/assets/benchmarks/2026-10-15-shared-cache/summary-on.json' | relative_url }})
includes p95, throughput and the range of trial medians.

We alternated **serving off, on, on, off**, keeping the same image, data and
resource caps. Write coordination remained enabled in both modes. With serving
off, the gateway's single-client sorted page was 1.362 ms; with serving on it was
0.382 ms. Those off-mode results include the shared-catalog and coordination
architecture, so they do not measure total overhead versus a gateway without
cache infrastructure. The [off-mode summary]({{ '/assets/benchmarks/2026-10-15-shared-cache/summary-off.json' | relative_url }})
retains every native control and gateway result too.

### Misses and writes change the answer

We separately tested unused cache keys and real writes. “Cold” here means a new
response-cache key on already loaded data, not cold operating-system caches or
storage. Values below are median trial p50 milliseconds with cache serving on.
Write rows measure the combined write and following read at one client.

| Separate workload | Elasticsearch | OpenSearch | Gateway + Qdrant + Valkey |
|---|---:|---:|---:|
| New sorted-filter query, one client | 0.582 | 0.522 | 2.085 |
| New sorted-filter query, eight clients | 1.446 | 1.344 | 5.454 |
| Write then read the document | 1.070 | 1.006 | 2.712 |
| Write then read filter counts | 3.964 | 4.439 | 3.273 |

Misses pay for the Qdrant query and cache publication. The gateway's cold-query
median increased from 1.567 to 2.085 ms at one client when serving was enabled.
At eight clients it increased from 3.993 to 5.454 ms; that enabled run's p95 was
45.499 ms, versus roughly 2.7–2.8 ms for native engines. Tail latency matters as
much as the warm median.

Point-write cycles omit `refresh` for every engine because native GET is
realtime. Filter-count cycles use `refresh=true` everywhere because the next
search requires immediate visibility. The gateway led that latter comparison,
but caching itself did not improve the write cycle: its median rose from
2.942 ms with serving off to 3.273 ms with serving on. Point-write cycles also
rose, from 2.310 to 2.712 ms. These separate off/on profiles were not an ABBA
experiment, so their differences should not be treated as isolated effect sizes.
The [complete mixed-workload summary]({{ '/assets/benchmarks/2026-10-15-shared-cache/summary-mixed.json' | relative_url }})
includes all participants, both modes and p95 values.

### The cache has to know when an answer is unsafe

Before a mutation starts, the gateway advances a shared generation and records a
persistent writer token. A cached answer is usable only in the same quiet
generation. A cold read captures that generation before loading metadata; if a
writer intervenes, its result cannot be published into the cache.

The token is released only after backend completion and catalog publication are
known complete. A timeout, cancellation or uncertain operation leaves a fence in
place. That sacrifices cache availability until deliberate recovery instead of
reusing an answer after an unresolved write. Replicas remain stateless, but this
is not a system without state: Valkey's durability and the exclusive writer
contract are essential. Direct Qdrant writes, unfenced gateway replicas and
automatic failover to older coordination state are unsupported.

Every native engine had a two-CPU, 4-GiB cap. The gateway setup shared the same
aggregate budget: gateway 0.5 CPU/512 MiB, Qdrant 1.25 CPUs/3328 MiB, and Valkey
0.25 CPU/256 MiB. Valkey used durable AOF with `appendfsync always` and a 64-MiB
memory limit inside its container. We included its resource and write costs.
An idle post-warm-run memory snapshot totaled approximately 265 MiB for those
three services, versus 984 MiB and 923 MiB for the native engines. These are
single container observations, not peak memory or capacity estimates.

### Reproduce and decide

These results measure revision
[`187944e4cc274a0ae61a6c79de885179319e9d4a`](https://github.com/dstockton/qdrant-es-gateway/commit/187944e4cc274a0ae61a6c79de885179319e9d4a).
Later optimizations require separate measurements; they are not represented by
this baseline. The [harness and full preparation procedure at that revision](https://github.com/dstockton/qdrant-es-gateway/tree/187944e4cc274a0ae61a6c79de885179319e9d4a/benchmark/shared-cache)
prepare a fresh disposable deployment and reuse the three-engine correctness
checks. Build that revision, run its preparation helper, load the saved deployment
identity into `CACHE_BENCH_DEPLOYMENT`, and select `CACHE_BENCH_SERVE=true` using
the documented Compose command. Once ready, this measures one enabled round:

```sh
ROUND=on-1 REQUESTS=200 TRIALS=3 WARMUP=40 \
  OUTPUT=benchmark/shared-cache/results/on-1.json \
  python3 benchmark/shared-cache/measure.py
# Preserve the other three rounds and run the separate cold/invalidation profiles.
```

Across the retained profiles, **86,400 measured HTTP responses passed correctness
checks**: 72,000 warm reads, 7,200 cold reads and 7,200 responses from 3,600
write/read cycles. Checks cover sources, IDs, ordering, totals, buckets and
changed document contents. The
[evidence manifest]({{ '/assets/benchmarks/2026-10-15-shared-cache/manifest.json' | relative_url }})
links all eight raw rounds, the exact build, image IDs and resource observations.
These runs use the v3 client-validation profile and are kept separate from older
measurements.

For repeated catalogue searches between writes, the cache offers a demonstrated
latency benefit in this lab profile. For mostly new queries or frequent document
updates, the native engines remain stronger in the measured cases. Start with
your actual reuse and write rate, and follow the
[deployment and recovery requirements]({{ '/shared-read-cache/' | relative_url }})
before enabling it.
