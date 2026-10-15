---
layout: post
title: "One Qdrant search thread improves tails, but hurts typical cold queries"
date: 2026-10-15
categories: [performance]
---

Limiting Qdrant search pools to one thread produced a clear trade-off in our small
CPU-budget experiment. With eight clients, gateway cold-query p95 fell from
**45.610 to 11.088 ms**, while p50 rose from **4.805 to 7.568 ms**. That is a
75.7% reduction in the tail measurement and a 57.5% increase in typical latency.
Throughput rose 20.1%. It is not an overall latency win or a reason to change
the default everywhere.

### Why test one thread?

Qdrant had a 1.25-CPU quota in our two-CPU gateway deployment. Its telemetry
reported two high-CPU search threads and eight high-I/O search threads, with
high-I/O mode selected. In Qdrant 1.19.2, the automatic budgets are the detected
CPU count and four times that count. An explicit `max_search_threads` sets each
pool's budget instead; it does not cap every thread in the process.
See the versioned [runtime implementation](https://github.com/qdrant/qdrant/blob/v1.19.2/lib/storage/src/content_manager/toc/runtimes.rs#L34-L107)
and [default calculation](https://github.com/qdrant/qdrant/blob/v1.19.2/lib/common/common/src/defaults.rs#L83-L94).

The pinned CPU-detection library rounds fractional cgroup quotas upward, subject
to available CPUs: 1.25 becomes two here. That explains the reported two/eight
budgets. A CPU quota limits scheduled CPU time, not worker count; extra workers
can be useful while waiting for I/O. These facts motivated a measurement, not an
assumption that fewer workers must be faster.
[CPU detection source](https://github.com/seanmonstar/num_cpus/blob/v1.17.0/src/linux.rs#L201-L215).

### Cold queries: better tails, worse medians

We ran automatic, one, one, automatic order, retaining native controls and all
trials. Only Qdrant's search-thread setting changed. Each value below is the
median of six trial quantiles, in milliseconds. Cold means unused response-cache
keys on loaded data, not cold OS caches or storage.

| Cold sorted filter | Automatic p50 → one | Automatic p95 → one |
|---|---:|---:|
| Gateway, one client | 1.970 → 2.172 | 2.314 → 2.452 |
| Gateway, eight clients | 4.805 → 7.568 | 45.610 → 11.088 |
| Elasticsearch, one client | 0.578 → 0.610 | 0.658 → 0.702 |
| Elasticsearch, eight clients | 1.264 → 1.334 | 2.323 → 2.835 |
| OpenSearch, one client | 0.481 → 0.491 | 0.595 → 0.571 |
| OpenSearch, eight clients | 1.162 → 1.169 | 2.305 → 2.169 |

Both native engines remained faster for these cold queries. Single-client gateway
p50 also worsened, by 10.3%. The eight-client tail improvement does not erase those
costs. Eight-client cold throughput increased from **849.210 to 1,019.941 requests
per second**, using the same median-of-trials calculation. Valkey completed an AOF rewrite during the second one-thread cold profile;
we retain that possible confounder rather than attributing every change to threads.

### Warm reads stayed mixed

The gateway image and enabled response cache were unchanged. These rows show
p50 for one client / eight clients, in milliseconds.

| Warm gateway operation | Automatic | One |
|---|---:|---:|
| Read one document | 0.301 / 0.580 | 0.300 / 0.573 |
| Filtered, sorted page | 0.371 / 0.916 | 0.391 / 0.940 |
| Page after a sort cursor | 0.389 / 0.908 | 0.388 / 0.910 |
| Keyword terms buckets | 0.326 / 0.788 | 0.326 / 0.772 |
| Two filter counts | 0.311 / 0.733 | 0.315 / 0.722 |

Most movements were small. Filtered-page p50 rose 5.3% with one client and 2.7%
with eight; single-client p95 rose 13.6%. Two-filter single-client p95 rose 9.1%.
The [full summary]({{ '/assets/benchmarks/2026-10-15-qdrant-search-threads/summary.json' | relative_url }})
includes all 36 native and gateway comparisons and their tails. These warm
observations do not establish a benefit from a setting that acts on backend work.

### Reproduce the comparison

All **86,400 measured responses** passed correctness checks: 14,400 cold and
72,000 warm. The corpus had 10,000 documents. Requests, exact totals, validation
profile v3 and native cache settings stayed the same. Elasticsearch 8.15.0 and
OpenSearch 3.9.0 each had two CPUs and 4 GiB. Gateway, Qdrant 1.19.2 and durable
Valkey shared that aggregate budget: 0.5, 1.25 and 0.25 CPUs respectively.

Use the [shared-cache preparation procedure](https://github.com/dstockton/qdrant-es-gateway/tree/2deda5e43b72de9b2c2ad6f9043df2a01217fd55/benchmark/shared-cache)
and the gateway source at that revision. Copy the retained
[round driver]({{ '/assets/benchmarks/2026-10-15-qdrant-search-threads/thread_round.py' | relative_url }}),
[image override]({{ '/assets/benchmarks/2026-10-15-qdrant-search-threads/thread-image.yml' | relative_url }})
and [snapshot helper]({{ '/assets/benchmarks/2026-10-15-qdrant-search-threads/snapshot_round.py' | relative_url }})
into `benchmark/shared-cache/`. The driver pins the historical
`qdrant-es-gateway:atomic-snapshot` image. A fresh build may have a different ID:
verify its source, explicitly update the driver's `DIGEST` to that local image ID,
and ensure `SOURCE` records the revision actually built. Retain any driver changes
with your results. Do not bypass image verification.

From the repository root, after preparation:

```sh
python3 benchmark/shared-cache/thread_round.py auto 1
python3 benchmark/shared-cache/thread_round.py one 1
python3 benchmark/shared-cache/thread_round.py one 2
python3 benchmark/shared-cache/thread_round.py auto 2
```

The driver restarts Qdrant and the gateway in the same sequence each round,
retains volumes, checks readiness and verifies effective pool budgets. Before each
profile it primes all three engines with identical workloads and additional
uncached sorted queries, outside timing. Collections had 10,000 points and two
segments after each restart. Pool telemetry remained high-I/O at the sampled
boundaries, with budgets two/eight or one/one as intended. Boundary samples do
not prove that no transitions occurred between them.

The [manifest]({{ '/assets/benchmarks/2026-10-15-qdrant-search-threads/manifest.json' | relative_url }})
links all eight raw profiles, four resource records, image provenance and hashes.
Resource intervals include priming and native-engine phases; throttling counters
cannot assign a cause to individual latency changes. Qdrant cold-profile throttled
time fell from 1.054/0.912 seconds in the automatic rounds to 8.407/3.609
milliseconds in the one-thread rounds. The second one-thread cold profile also
recorded 262.916 ms of Valkey throttling and the AOF rewrite. Those observations
provide context, not a per-request diagnosis. Delayed-fsync counts remained
zero. Only ephemeral container IDs were removed from public evidence.

This experiment stays separate from earlier gateway code comparisons. One search
thread is a possible tail-latency trade-off for this workload and quota, with a
substantial median cost. It is not a generally faster configuration.
