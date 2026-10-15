# Changing transport was not enough

> Measurement note: these historical filter-aggregation trials recomputed the expected count in each client worker. The calculation was outside that request's timer, but affected throughput and could delay concurrent requests. Later trials tagged `precomputed-filter-count-v2` compute it once before timing. The figures here are preserved without adjustment; do not combine them with the corrected trials.

After removing unnecessary scans, we tested another plausible optimization:
send gateway reads to Qdrant over gRPC instead of REST. The smaller typed
messages and shared connection might reduce overhead. The results were mixed,
so we removed the prototypes and kept REST in production.

### Compare one change at a time

We built each prototype once, then ran the same image with gRPC disabled and
enabled. The client-facing requests, retained 10,000-document corpus and resource
caps stayed identical. Four rounds ran in **REST, candidate, candidate, REST**
order, with Elasticsearch and OpenSearch interleaved throughout. No builds or
ingestion ran during timing.

The first prototype moved supported document reads, scrolling, facets and counts
to gRPC. It improved nine of ten median-latency combinations by roughly 1–11%,
but single-client filter aggregation became **15.4% slower: 1.085 → 1.251 ms**.
Those extra costs mattered more than an attractive count of winning cells.

We then tried a hybrid: keep counts on REST and use gRPC for the other supported
reads. The table shows all gateway results for that second experiment. Each cell
is median trial p50 milliseconds, **one client / eight clients**, using all six
trial medians per variant.

| Operation | REST | Hybrid |
|---|---:|---:|
| Read one document | 0.377 / 0.784 | 0.371 / 0.703 |
| Filtered, sorted page | 0.703 / 2.017 | 0.706 / 1.851 |
| Page after a sort cursor | 1.015 / 2.519 | 0.990 / 2.531 |
| Keyword terms buckets | 0.901 / 3.080 | 0.928 / 2.744 |
| Two filter counts | 1.117 / 3.211 | 1.145 / 3.273 |

Five combinations improved and five regressed. Keeping counts on REST reduced
the earlier single-client regression, but did not produce a consistent win.
These are observations from separate paired experiments, not proof that one
transport operation alone caused every difference.

Both native engines remained faster in all ten candidate workload/concurrency
combinations in each experiment. We did not change their requests or disable
their default caching to make the gateway look better.

### Keep the evidence, not the extra machinery

Across the two paired experiments, **144,000 measured responses passed strict
checks** for full sources, IDs, sort values, exact totals and aggregation results.
Correctness was necessary, but it did not justify the additional adapter,
conversion rules and dependencies for these mixed latency results. The gateway
continues to use REST; no new transport setting is required.

This does not establish that gRPC is generally slower, or that it cannot help a
different workload. It shows that changing transport was insufficient to close
this gateway's measured latency gap on a small scalar catalogue. The larger
[disabled-total sorting improvement](https://dstockton.github.io/qdrant-es-gateway/markdown/2026-10-15-disabling-totals-should-not-slow-sorting.md)
came from eliminating work rather than changing its encoding.

The [evidence manifest](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-transport/manifest.json)
retains all eight rounds, exact image IDs, resource limits and replayable prototype
patches. Complete summaries include native results, p95 latency, throughput and
trial ranges for the [full gRPC experiment](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-transport/full-summary.json)
and [hybrid experiment](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-transport/hybrid-summary.json).
A separate REST-only disabled-total diagnostic is retained but excluded from the
paired results.

To replay, apply one archived patch and its companion module to the manifest's
base revision, build the image, and use the three-engine read-only runner with
`REQUESTS=200 TRIALS=3 WARMUP=40 TRACK_TOTAL_HITS=true`. Alternate only the backend
transport between rounds, preserving data and waiting for readiness. The native
versions remain Elasticsearch 8.15.0 and OpenSearch 3.9.0, with Qdrant 1.19.2;
each engine receives the same aggregate two-CPU, 4-GiB limit. This is a reproducible
lab experiment, not a production-capacity or general engine ranking.
