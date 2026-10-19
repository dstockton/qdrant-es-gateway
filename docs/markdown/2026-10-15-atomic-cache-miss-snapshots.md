# One fewer catalog request, without a proven latency win

A quiet cache miss now gets its generation ticket and catalog snapshot in one
Valkey operation. This removes a second metadata request before querying Qdrant.
In our paired test, cold-query medians fell **3.0% with one client and 7.9% with
eight clients**. Native controls moved too, and warm reads were mixed. These
measurements do **not** isolate a performance win from the change.

### Keep the ticket and snapshot together

The catalog maps index and alias names to collections. The cache ticket identifies
the generation under which a response may be stored. If an alias changes between
independent reads, pairing an older catalog with a newer ticket could allow an
outdated response into the cache.
The previous two-call path already enforced safe ordering by obtaining the ticket
before the catalog. This change preserves that guarantee with one call; it does
not fix a stale-cache bug in the previous path.

The new script checks that no writers are active, captures the generation and
reads the catalog atomically. It bounds the catalog size before fetching its
contents. The gateway loads those bytes through its existing validated SQLite
catalog loader; malformed or duplicate records fail before reaching Qdrant.

A writer starting afterward changes the generation, preventing the earlier
response from filling the cache. That request may still return its response.
This protects cache publication; it does not create a transaction spanning
Valkey and Qdrant or provide backend snapshot isolation. Warm hits retain their
existing path. Busy or failed lookups, and corrupt cached responses, use the
ordinary catalog path.

### What changed in the measurements

We compared the retained connection-reuse image with
[`2deda5e`](https://github.com/dstockton/qdrant-es-gateway/commit/2deda5e43b72de9b2c2ad6f9043df2a01217fd55),
using old, new, new, old order. Cache-enabled rounds measured cold then warm
queries. A separate serving-off control measured cold queries only; coordination
remained enabled. Cold means unused response-cache keys on loaded data, not cold
OS caches or storage. There were no timed write/read cycles in this experiment.

Each value is the median of six trial p50 measurements, in milliseconds. Native
columns are controls from the same enabled rounds; only the gateway image changes.

| Cold sorted filter | Gateway old → new | Elasticsearch old → new | OpenSearch old → new |
|---|---:|---:|---:|
| One client | 4.268 → 4.139 | 1.375 → 1.516 | 1.202 → 1.117 |
| Eight clients | 16.067 → 14.800 | 3.343 → 3.087 | 3.324 → 2.946 |

At eight clients, Elasticsearch improved 7.7% and OpenSearch 11.4%, despite neither
engine changing. Gateway p95 barely moved: **5.099 → 5.122 ms** with one client
and **70.192 → 68.663 ms** with eight. Both native engines remained substantially
faster for these cold queries. Serving-off gateway p50 moved **2.677 → 2.707 ms**
and **10.851 → 10.581 ms**, providing another indication of variation.

Warm results also varied. Cells below show one client / eight clients.

| Warm gateway operation | Old p50 ms | New p50 ms |
|---|---:|---:|
| Read one document | 0.519 / 1.392 | 0.569 / 1.230 |
| Filtered, sorted page | 0.759 / 2.426 | 0.643 / 2.251 |
| Page after a sort cursor | 0.700 / 2.649 | 0.694 / 2.165 |
| Keyword terms buckets | 0.536 / 1.799 | 0.546 / 1.736 |
| Two filter counts | 0.527 / 1.725 | 0.506 / 1.664 |

Single-client point reads worsened **9.6% at p50 and 39.7% at p95**
(0.730 → 1.020 ms). Terms p95 also worsened at both concurrency levels, by 10.7%
and 7.4%. The first baseline warm profile included an AOF rewrite; later profiles
did not. Native controls drifted too. We retain that confounder and every adverse
result rather than attributing warm changes to an optimization that targets misses.
The [complete summary](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-atomic-snapshot/atomic-snapshot-summary.json)
contains all 42 native and gateway comparisons, including p95.

### Replay this experiment

All **93,600 measured HTTP responses** passed the existing correctness checks:
72,000 warm, 14,400 enabled cold and 7,200 serving-off cold. Fifteen smoke-test
operations are recorded separately. The corpus was 10,000 documents with identical
client requests, exact totals and validation profile v3. Elasticsearch 8.15.0 and
OpenSearch 3.9.0 each had two CPUs and 4 GiB. Gateway, Qdrant 1.19.2 and durable
Valkey shared the same aggregate budget. No builds ran during timing.

Use the [pinned preparation procedure](https://github.com/dstockton/qdrant-es-gateway/tree/2deda5e43b72de9b2c2ad6f9043df2a01217fd55/benchmark/shared-cache)
to create the disposable deployment and its preparation file. Build the baseline
from source `c4208725b240289a3c0890e576904b80bdf7a0e7` as
`qdrant-es-gateway:catalog-transport` and the candidate from `2deda5e` as
`qdrant-es-gateway:atomic-snapshot`. Record each locally built image ID; fresh
builds need not match the historical IDs in the
[manifest](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-atomic-snapshot/manifest.json).
Then, from the repository root:

```sh
old_id=$(docker image inspect qdrant-es-gateway:catalog-transport | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Id"])')
new_id=$(docker image inspect qdrant-es-gateway:atomic-snapshot | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Id"])')
python3 benchmark/shared-cache/snapshot_round.py on-main old 1 \
  --old-image-id "$old_id" \
  --new-image qdrant-es-gateway:atomic-snapshot --new-image-id "$new_id" \
  --new-source 2deda5e43b72de9b2c2ad6f9043df2a01217fd55
# Repeat with on-main new 1, on-main new 2, on-main old 2.
# Then off-control old 1, new 1, new 2, old 2, keeping all flags.
```

The manifest links all twelve raw measurement files, eight resource records,
smoke checks, summaries and the retained driver/override. Resource counters span
setup, priming and native-engine phases; they cannot attribute individual request
latency. AOF rewrites advanced from zero to one during the first baseline warm
profile, while delayed-fsync counts remained zero. Ephemeral container IDs were
removed from public records; original hashes and unchanged timing data are retained.

These results remain separate from earlier experiments. Tests demonstrate that
a quiet miss avoids the second catalog fetch without weakening generation checks.
The latency evidence supports a smaller request path, not a broad performance claim.
