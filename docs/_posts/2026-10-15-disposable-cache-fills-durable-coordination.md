---
layout: post
title: "Keep coordination durable, let response-cache fills be disposable"
date: 2026-10-15
categories: [performance]
---

A response cache should be cheap to refill. Our cache entries shared Valkey's
append-only log with catalog metadata and writer coordination, so filling a cold
response also added durable log traffic. We changed only response publication to
skip incremental AOF propagation. Catalog updates, generations and writer tokens
retain their required durability.

In a paired experiment, enabled cold-query p50 improved about **21%** at both
one and eight clients. Eight-client p95 fell **67.8%**, from **43.949 to 14.169 ms**.
Native engines still handled these cold queries faster, and warm results included
regressions. The improvement is specific to this cache-fill workload.

### Disposable entries, durable authority

The publication script validates the deployment, generation and quiet writer state
before storing a bounded response with an expiry. It disables propagation only
around that response write, then restores normal propagation. Administrative
claims, catalog publication and writer admission/release keep the existing
`appendfsync always` requirements.

This does not mean response entries can never survive a restart. An AOF rewrite
or snapshot can capture entries already in memory. A restored entry must still
pass identity, build/configuration, generation, expiry and writer-state checks.
An old response cannot become current merely because a rewrite saved it.
The [deployment guide]({{ '/shared-read-cache/' | relative_url }}) explains these
boundaries. This is not a guarantee that every fill avoids all fsync activity.

### Cold-query results and log growth

We compared baseline
[`04b6d7e`](https://github.com/dstockton/qdrant-es-gateway/commit/04b6d7eecce6230634109ec1905509666f00b8bc)
with measured candidate
[`a778a35`](https://github.com/dstockton/qdrant-es-gateway/commit/a778a357f677345939e61b559bf6beac234624d7),
using old, new, new, old order in separate serving-enabled and serving-disabled
profiles. These measurements describe those builds, not a later merge.
Values are median trial quantiles in milliseconds.

| Gateway cold sorted filter | p50 old → new | p95 old → new |
|---|---:|---:|
| Serving enabled, one client | 1.943 → 1.537 | 2.277 → 1.715 |
| Serving enabled, eight clients | 4.578 → 3.614 | 43.949 → 14.169 |
| Serving disabled, one client | 1.353 → 1.359 | 1.515 → 1.562 |
| Serving disabled, eight clients | 3.336 → 3.150 | 44.380 → 50.073 |

The enabled cold profiles added **9,103,721 and 9,103,723 bytes** to the baseline
AOF, compared with **2,016 bytes in each candidate round**. Disabled cold profiles
also added 2,016 bytes; warm profiles added none. No rewrite occurred within these
sampled intervals, so log compaction does not account for that difference.
The observations support reduced incremental log work, not a count of individual
fsync calls or proof that all latency variation has one cause.

Native cold p50 controls from the enabled rounds remained lower:

| Cold sorted filter | Elasticsearch old → new | OpenSearch old → new |
|---|---:|---:|
| One client | 0.579 → 0.581 | 0.487 → 0.498 |
| Eight clients | 1.201 → 1.295 | 1.247 → 1.182 |

The disabled eight-client p95 worsened **12.8%**, even though response fills were
bypassed. Native controls also moved. Those results remain in the
[complete 42-row summary]({{ '/assets/benchmarks/2026-10-15-ephemeral-publish/ephemeral-publish-summary.json' | relative_url }}).

### Warm results were mixed

Warm hits do not publish another response. Cells show one / eight clients, in
p50 milliseconds.

| Warm gateway operation | Old | New |
|---|---:|---:|
| Read one document | 0.302 / 0.585 | 0.305 / 0.567 |
| Filtered, sorted page | 0.396 / 0.911 | 0.387 / 0.914 |
| Page after a sort cursor | 0.374 / 0.923 | 0.395 / 0.905 |
| Keyword terms buckets | 0.328 / 0.776 | 0.331 / 0.778 |
| Two filter counts | 0.321 / 0.768 | 0.322 / 0.709 |

Single-client sorted pagination worsened **5.8% at p50 and 21.7% at p95**
(0.439 → 0.535 ms). Eight-client terms p95 rose **5.15%** (1.210 → 1.272 ms).
We do not claim a general warm-read improvement from this change.

### Check crashes as well as speed

An isolated Valkey 8.1.10 proof exercised the production Lua scripts. Publishing
a response produced a hit while AOF size stayed **257 → 257 bytes**. The final
six-check run also verified persistent unresolved claims and writers, durable
catalog commit and releases, rewrite-restored entries with their original expiry,
rejection after a durable generation change, and writer durability after a
publication error.

The [final proof]({{ '/assets/benchmarks/2026-10-15-ephemeral-publish/aof-proof-3.json' | relative_url }})
and [proof script]({{ '/assets/benchmarks/2026-10-15-ephemeral-publish/cache_ephemeral_aof.py' | relative_url }})
are retained. So are both earlier attempts: the first failed before checks because
the harness mishandled INFO JSON readiness parsing; the second passed an earlier
five-check suite. They are not silently discarded or presented as equivalent to
the final proof.

### Replay the comparison

All **93,600 measured HTTP responses** passed correctness checks: 72,000 warm,
14,400 enabled cold and 7,200 disabled cold. Each comparison uses six trial
quantiles per image. The 10,000-document corpus, requests, exact totals and
validation profile v3 stayed fixed. Cold means new cache keys on loaded data.

Elasticsearch 8.15.0 and OpenSearch 3.9.0 each had two CPUs and 4 GiB. Gateway,
Qdrant 1.19.2 and durable Valkey shared the same aggregate budget. Qdrant retained
automatic search pools. The driver consistently recreates the gateway and primes
all engines outside timing. No builds or tests ran during measurement.

Follow the [pinned preparation instructions](https://github.com/dstockton/qdrant-es-gateway/tree/a778a357f677345939e61b559bf6beac234624d7/benchmark/shared-cache).
Build the baseline as `qdrant-es-gateway:typed-readview` and candidate as
`qdrant-es-gateway:ephemeral-publish`, using the revisions above. Record local
image IDs; rebuilt images may differ from the historical manifest. After preparation:

```sh
old_id=$(docker image inspect qdrant-es-gateway:typed-readview | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Id"])')
new_id=$(docker image inspect qdrant-es-gateway:ephemeral-publish | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Id"])')
python3 benchmark/shared-cache/ephemeral_round.py on-main old 1 \
  --old-image-id "$old_id" \
  --new-image qdrant-es-gateway:ephemeral-publish --new-image-id "$new_id" \
  --new-source a778a357f677345939e61b559bf6beac234624d7
# Repeat on-main new 1, new 2, old 2, retaining all flags.
# Then off-control old 1, new 1, new 2, old 2, retaining all flags.
python3 tests/cache_ephemeral_aof.py --output /tmp/cache-aof-proof.json
```

The [manifest]({{ '/assets/benchmarks/2026-10-15-ephemeral-publish/manifest.json' | relative_url }})
retains twelve raw profiles, eight resource records, all proof attempts, exact
scripts and hashes. Public records omit only ephemeral container identifiers and
failed-proof resource names. Resource counters span setup and native phases as
well as gateway requests; they cannot attribute individual latencies. No delayed
fsync increments were recorded in sampled profiles. Earlier experiments are kept
separate.

For a workload that repeatedly fills the cache, disposable publication removes
unnecessary durable response storage while preserving durable coordination.
That benefit coexists with the native engines’ cold-query advantage and the adverse
warm results above.
