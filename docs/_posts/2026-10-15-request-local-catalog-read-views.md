---
layout: post
title: "Removing request-local SQLite reconstruction: modest gains, one worse tail"
date: 2026-10-15
categories: [performance]
---

The shared-catalog read path used to rebuild an in-memory SQLite database for
every uncached request. Document reads and searches now load the same validated
snapshot into request-local maps. In our paired experiment, cold-query medians
fell **4.5–5.5% with cache serving enabled** and **4.3–7.6% with serving disabled**.
The enabled eight-client p95 worsened **10.5%**. This is a modest reduction in
request overhead, not an across-the-board latency win.

### Change the representation, preserve the boundary

The change applies to exact document GET and index search GET/POST handlers in
shared-catalog mode. It includes requests that bypass the response cache or run
with serving disabled. Each request still obtains a fresh authoritative snapshot,
validates every mapping and vector list, and rejects duplicate records. Concrete
indices retain precedence over single-hop aliases. There is no persistent local
metadata cache or fallback to an old local database.

Metadata and mutation handlers still use SQLite; default local SQLite deployments
are unchanged. Quiet cache misses keep the atomic generation/snapshot operation,
and the original ticket still guards cache publication after an alias or document
change. This remains a full-catalog fetch and validation, bounded at 8 MiB, with
work proportional to catalog size. It does not establish that SQLite generally
limits search performance. The [operational guide]({{ '/shared-catalog/' | relative_url }})
describes the scope.

### Cold medians improved, but the enabled tail did not

We compared baseline
[`2deda5e`](https://github.com/dstockton/qdrant-es-gateway/commit/2deda5e43b72de9b2c2ad6f9043df2a01217fd55)
with candidate
[`04b6d7e`](https://github.com/dstockton/qdrant-es-gateway/commit/04b6d7eecce6230634109ec1905509666f00b8bc)
in old, new, new, old order. Serving-enabled and serving-disabled profiles remained
separate; coordination stayed enabled in both. Cold means new response-cache keys
on loaded data. Values below are median trial quantiles in milliseconds.

| Gateway cold sorted filter | p50 old → new | p95 old → new |
|---|---:|---:|
| Serving enabled, one client | 1.911 → 1.826 | 2.374 → 2.201 |
| Serving enabled, eight clients | 4.782 → 4.521 | 55.131 → 60.917 |
| Serving disabled, one client | 1.372 → 1.312 | 1.479 → 1.435 |
| Serving disabled, eight clients | 3.234 → 2.989 | 55.984 → 43.680 |

The enabled eight-client p95 increase repeated in both rounds: baseline round
medians were 55.141/55.121 ms, versus 61.437/60.396 ms for the candidate. The
disabled p95 reduction was less stable: baseline 79.107/25.479 ms versus candidate
32.133/55.227 ms. Cold p50 improvements repeated in both rounds of both modes.
Both native engines remained substantially faster for cold queries. These are
their p50 controls from the same enabled rounds:

| Cold sorted filter | Elasticsearch old → new | OpenSearch old → new |
|---|---:|---:|
| One client | 0.570 → 0.575 | 0.488 → 0.493 |
| Eight clients | 1.255 → 1.251 | 1.169 → 1.265 |

Native movement and the adverse gateway tail rule out a blanket performance claim.
The [complete summary]({{ '/assets/benchmarks/2026-10-15-typed-readview/typed-readview-summary.json' | relative_url }})
retains all 42 native and gateway comparisons, including the disabled controls.

### Warm behavior is separate

Warm cache hits do not reconstruct the catalog. Their p50 results were mostly
close, with some reductions and some increases. Cells show one / eight clients.

| Warm gateway operation | Old p50 ms | New p50 ms |
|---|---:|---:|
| Read one document | 0.305 / 0.582 | 0.302 / 0.578 |
| Filtered, sorted page | 0.386 / 0.954 | 0.388 / 0.900 |
| Page after a sort cursor | 0.388 / 0.913 | 0.383 / 0.898 |
| Keyword terms buckets | 0.329 / 0.773 | 0.329 / 0.780 |
| Two filter counts | 0.318 / 0.742 | 0.314 / 0.716 |

Single-client filtered-page p50 rose 0.45%, and eight-client terms p50 rose 0.93%.
Single-client terms p95 rose **4.19%** (0.401 → 0.418 ms); eight-client point p95
rose **1.19%** (0.838 → 0.848 ms). These changes should not be presented as a warm
read improvement caused by removing work from misses.

### Reproduce and inspect

All **93,600 measured responses** passed correctness checks: 72,000 warm,
14,400 enabled cold and 7,200 disabled cold. The corpus contained 10,000 documents,
with unchanged requests, exact totals and validation profile v3. Each profile used
three trials per round, giving six trial quantiles per image for each comparison.
No builds or tests ran during measurement.

Elasticsearch 8.15.0 and OpenSearch 3.9.0 each had two CPUs and 4 GiB. Gateway,
Qdrant 1.19.2 and durable Valkey shared that aggregate budget. Qdrant retained its
automatic two/eight search-pool budgets; both profile boundaries reported high-I/O
mode. The driver primes all engines identically before each profile, including
uncached sorted queries, and recreates the gateway consistently between rounds.

Follow the [pinned shared-cache preparation procedure](https://github.com/dstockton/qdrant-es-gateway/tree/04b6d7eecce6230634109ec1905509666f00b8bc/benchmark/shared-cache).
Build the two revisions as `qdrant-es-gateway:atomic-snapshot` and
`qdrant-es-gateway:typed-readview`. Record their local image IDs; rebuilt images
need not have the historical IDs in the
[manifest]({{ '/assets/benchmarks/2026-10-15-typed-readview/manifest.json' | relative_url }}).
From the repository root after preparation:

```sh
old_id=$(docker image inspect qdrant-es-gateway:atomic-snapshot | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Id"])')
new_id=$(docker image inspect qdrant-es-gateway:typed-readview | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Id"])')
python3 benchmark/shared-cache/readview_round.py on-main old 1 \
  --old-image-id "$old_id" \
  --new-image qdrant-es-gateway:typed-readview --new-image-id "$new_id" \
  --new-source 04b6d7eecce6230634109ec1905509666f00b8bc
# Repeat on-main new 1, on-main new 2, on-main old 2, keeping all flags.
# Then off-control old 1, new 1, new 2, old 2, keeping all flags.
```

All twelve raw profiles, eight resource records and the exact driver, override
and helpers are retained in the manifest. Only ephemeral container IDs were
removed; original and published hashes are recorded. Resource intervals cover
profile setup and native-engine phases as well as gateway work. Qdrant was
throttled in roughly 15.8–20.6% of its cold-profile scheduling periods and none
of the warm-profile periods. These counters do not explain individual requests.
No AOF rewrite increments or delayed fsync were recorded within sampled profile
intervals. A rewrite completed between the final baseline cold and warm snapshots
(the cumulative count rose from two to three). The second enabled candidate cold
profile recorded 43.949 ms of Valkey throttling. These observations remain in the
evidence rather than being excluded from the comparison.

These measurements are separate from earlier experiments. The read view removes
avoidable reconstruction while keeping complete validation and request-local
metadata. The modest median gains, worse enabled tail and native cold-query
advantage all remain part of the result.
