# Inside a cold search: a backend diagnostic, not another engine benchmark

After reducing cache-fill overhead, we inspected three Qdrant operations used by
a filtered, sorted search: exact counting, checking indexed-field coverage, and
retrieving an ordered page. The diagnostic found that exact counting had the
largest single-client server-reported time of those operations. It also showed a
large gap between server-reported timings and parallel client wall time under
concurrency. Neither observation identifies the entire gateway bottleneck.

**This is not an Elasticsearch-compatible or native-engine comparison.** The
runner sends Qdrant RPCs directly inside the container network. It bypasses the
gateway, catalog, response cache and public client path. Its Python client is
not the gateway's Rust client.

### Three pieces of work

The requests use a range filter over the retained 10,000-document corpus. The
count is exact. The coverage probe asks whether any matching document lacks the
expected indexed-field marker. The ordered request retrieves 21 full-source documents with their document IDs
in ascending rank order. All results are checked, including counts, ordering,
document IDs and full sources.

A separate [payload-schema snapshot](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-backend-diagnostic/backend-payload-schema.json)
captured **after the diagnostic** found an integer index on `rank` and a keyword
index on the coverage marker, each covering all 10,000 points. It does not support
a missing-rank-index explanation for the observed count cost, but it is not
contemporaneous proof of the index state during timing.

These are pooled p50 **Qdrant response `time` fields**, in milliseconds, from
400 measured calls per configuration:

| RPC | Run alone, one / eight clients | Run in parallel, one / eight clients |
|---|---:|---:|
| Exact count | 0.698 / 0.892 | 0.734 / 0.873 |
| Coverage probe | 0.163 / 0.298 | 0.180 / 0.320 |
| Ordered page | 0.136 / 0.190 | 0.170 / 0.263 |

The parallel group waits for all three operations and includes Python dispatch,
JSON parsing and correctness validation. Its median trial p50/p95 was
**1.023 / 1.161 ms with one client** and **4.426 / 41.780 ms with eight clients**.
Per-RPC client wall time ends when the response body is received; it has a different
boundary from both group time and the server's reported field.

Do not add parallel quantiles or subtract these independently aggregated timings
to estimate gateway overhead. Network transit, client scheduling, executor work,
validation and server queueing can contribute at different boundaries. This
experiment does not isolate their individual costs or prove count is the sole
bottleneck. The [full summary](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-backend-diagnostic/backend-decomposition-summary.json)
retains standalone and parallel group quantiles plus each RPC's client and
server-reported quantiles.

### Resource context and reproducibility

All **4,800 measured RPC responses across 3,200 groups** passed checks. Four trials
rotate the operation order at each concurrency level. Connections are persistent
per worker and RPC. The isolated Python generator has two CPUs and 256 MiB,
outside the server budget; its execution model can influence group wall time.

Qdrant 1.19.2 retained 1.25 CPUs and 3,328 MiB. The collection was green at both sampled boundaries, with
10,000 points and two segments. Search-pool telemetry reported high-I/O mode and
two/eight pool budgets at both boundaries. Across the whole capture, Qdrant was
throttled in **13 of 32 scheduling periods (40.6%)**, with **1.107 seconds** of
reported throttled time. Other recorded services had no throttled periods.
These whole-run counters do not assign the long parallel tail to a particular
request or exclude client effects.

Use the [prepared harness at revision 47113fc](https://github.com/dstockton/qdrant-es-gateway/tree/47113fc5a0bc1f3ab4aad6728a480626c7aa006f/benchmark/shared-cache)
with its retained corpus and resource settings. Copy the
[launcher](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-backend-diagnostic/backend_diagnostic.py)
and [RPC runner](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-backend-diagnostic/backend_decomposition.py)
into `benchmark/shared-cache/`, then run from the repository root:

```sh
python3 benchmark/shared-cache/backend_diagnostic.py --prefix backend-decomposition
```

The launcher requires the recorded Python image digest to be available, checks
the prepared corpus and resource limits, and refuses to overwrite evidence.
It creates a restricted temporary runner without changing the engines. The
[manifest](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-backend-diagnostic/manifest.json)
records the image provenance, exact runner hashes, raw samples and resource
snapshots. The two diagnostic scripts were uncommitted at execution; their exact
copies are retained. The gateway image was built from `47113fc`, but the gateway
was not in this measured request path. Only ephemeral container IDs were removed
from public records.

The next useful question is which changes reduce complete request latency while
preserving exact counts and result correctness. These measurements help choose
experiments; they are not evidence of a new production speedup.
