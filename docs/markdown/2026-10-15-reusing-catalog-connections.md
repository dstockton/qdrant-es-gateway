# Reusing catalog connections helps misses and writes, with a warm-read warning

The shared catalog was opening a new Valkey connection whenever a request loaded
its metadata snapshot. We changed it to reuse a persistent multiplexed connection.
A failed operation is still returned as a failure: reconnecting later must not
silently replay a write whose outcome is uncertain.

In a new paired experiment, gateway cold-query and write/read medians generally
improved by **12–17%**. The exception was eight-client cold queries with cache
serving off, which improved only 1.4%. Warm performance was mixed, and one
important case regressed. This change reduces connection work; it does not
establish an across-the-board latency improvement.

### Compare within this experiment

We measured the original revision
[`187944e`](https://github.com/dstockton/qdrant-es-gateway/commit/187944e4cc274a0ae61a6c79de885179319e9d4a)
against connection-reuse change
[`49da1cb`](https://github.com/dstockton/qdrant-es-gateway/commit/49da1cb22279d22761e18cb38480c36d0bea943b),
built from validated source
[`c420872`](https://github.com/dstockton/qdrant-es-gateway/commit/c4208725b240289a3c0890e576904b80bdf7a0e7).
Each profile used **old, new, new, old** order, retaining native controls and all
trials. Cache coordination stayed enabled; serving off/on were separate profiles.

Native and old-gateway absolute latency were higher than in the
[earlier shared-cache experiment](https://dstockton.github.io/qdrant-es-gateway/markdown/2026-10-15-shared-cache-warm-search-results.md).
We therefore keep these experiments separate. Their differences cannot identify
a cause, and pooling their medians would be misleading.

Values below are gateway median trial p50 milliseconds. Cold means unused
response-cache keys on loaded data. Write rows measure a write and its following
read with one client.

| Workload | Serving off: old → new | Serving on: old → new |
|---|---:|---:|
| Cold sorted filter, one client | 2.590 → 2.159 | 3.155 → 2.788 |
| Cold sorted filter, eight clients | 7.761 → 7.650 | 10.692 → 9.344 |
| Write then point read | 3.524 → 2.914 | 4.008 → 3.392 |
| Write then filter counts | 4.673 → 3.974 | 5.137 → 4.481 |

Median improvements do not describe the whole result. Eight-client cold p95
increased from **64.454 to 71.301 ms** with serving off and **51.765 to 57.449 ms**
with serving on. The new enabled gateway's cold medians remained approximately
4.1 and 4.8 times the faster native engine at one and eight clients. Native
engines also remained faster for write/point-read cycles. The gateway led the
write/filter-count comparison, whose identical `refresh=true` requests require
immediate search visibility; point-write cycles omit refresh on every engine.

### Warm reads were not uniformly preserved

These are the separate cache-enabled warm rounds. Each cell shows **one client /
eight clients**, in milliseconds.

| Gateway operation | Old | New |
|---|---:|---:|
| Read one document | 0.380 / 0.845 | 0.381 / 0.985 |
| Filtered, sorted page | 0.593 / 1.324 | 0.571 / 1.403 |
| Page after a sort cursor | 0.603 / 1.344 | 0.462 / 1.411 |
| Keyword terms buckets | 0.417 / 1.351 | 0.413 / 1.136 |
| Two filter counts | 0.403 / 1.037 | 0.403 / 1.046 |

Eight-client point-read p50 rose **16.5%**, and p95 rose **27.5%**, from 1.242 to
1.583 ms. The native point-read medians also increased roughly 12% between the
corresponding old/new rounds. That drift prevents attributing the gateway change
to connection reuse alone, but it does not make the adverse result disappear.
**Warm-read neutrality remains unproven.**

The [full summary](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-catalog-transport/summary.json)
includes all native, old-gateway and new-gateway rows, with p95 and throughput.
The new gateway still led the warm search medians in this experiment; native
point reads and the cold/write costs prevent a blanket engine-performance claim.

### Replay the change

The corpus remained 10,000 documents, with identical client requests and v3
correctness assertions. Elasticsearch 8.15.0 and OpenSearch 3.9.0 each retained
two CPUs and 4 GiB. Gateway, Qdrant 1.19.2 and durable Valkey shared that same
aggregate budget. No builds ran during timing.

Build both pinned revisions before measuring and prepare the disposable shared
cache deployment using the
[pinned harness instructions](https://github.com/dstockton/qdrant-es-gateway/tree/c4208725b240289a3c0890e576904b80bdf7a0e7/benchmark/shared-cache).
The retained [round driver](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-catalog-transport/transport_round.py)
and [image override](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-catalog-transport/transport-image.yml)
belong in that harness directory. They replace only the gateway image, wait for
readiness, preserve volumes and refuse to overwrite evidence. Defaults verify the
recorded image IDs before changing the container. For a rebuilt image, pass its
explicit `--old-image-id` or `--new-image-id` and retain the resulting provenance.

Build the old revision as `qdrant-es-gateway:cache-runtime` and the new revision
as `qdrant-es-gateway:catalog-transport`. Fresh local builds generally have different
image IDs from the retained lab images. After verifying their source revisions,
record those local IDs and pass them explicitly:

```sh
old_image_id=$(docker image inspect qdrant-es-gateway:cache-runtime | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Id"])')
new_image_id=$(docker image inspect qdrant-es-gateway:catalog-transport | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Id"])')
python3 benchmark/shared-cache/transport_round.py off old 1 mixed \
  --old-image-id "$old_image_id" --new-image-id "$new_image_id"
# Keep both ID arguments for each later call.
# Continue off: new 1, new 2, old 2; repeat the four rounds with on.
# Then run on old 1 warm, on new 1 warm, on new 2 warm, on old 2 warm.
```

All **129,600 measured HTTP responses** passed checks across twenty raw rounds.
The [manifest](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-catalog-transport/manifest.json)
records exact image IDs, revisions, every raw file and post-run observations.
Memory snapshots are not peaks, and container-lifetime throttling counters cannot
attribute individual latency changes. Valkey key-access counters are not response-cache
hit rates; the retained observations do not provide a causal diagnosis.
Published raw files add experiment/build labels to prevent accidental pooling;
the manifest preserves their original hashes and identifies that metadata-only
annotation. Connection reuse removes avoidable work
from misses and writes; the warm-read regression and cold tails remain visible
in the evidence.
