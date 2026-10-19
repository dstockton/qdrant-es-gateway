# Measure the request timeline before rewriting JSON handling

The gateway's own diagnostic traces show that backend waiting occupies most of
the measured cold-search dispatch interval. The median **per-request** fraction
spent in the backend join was **79.4% with one client and 73.6% with eight**.
Cache-envelope JSON parsing took a single-client median of **0.017 ms**, compared
with **1.162 ms** for the whole dispatch interval. These traces do not justify a
JSON-handling rewrite as the next optimization.

This is an instrumented diagnostic, not a production performance benchmark or a
native-engine comparison. The `diagnostic-timing` compile-time feature is off by
default. Its timers, trace storage and response header add overhead.

### Follow the actual request

We captured 600 correct sorted-filter responses: 100 cold, 100 warm and 100
explicitly bypassed requests at each of one and eight clients. Each request keeps
exact totals and returns 20 full-source documents in rank order. The corpus has
10,000 documents. Cold bounds were ranks 1–100 with one client and 301–400 with
eight, so the exact counts include most of the corpus. Warm profiles repeat bounds
101 and 401; bypass profiles use bounds 201–300 and 501–600. These selectivities
matter when comparing this diagnostic with another workload.

The following values are per-phase p50 milliseconds for cold requests:

| Interval | One client | Eight clients |
|---|---:|---:|
| Complete dispatch scope | 1.162 | 2.513 |
| Cache lookup | 0.059 | 0.223 |
| Catalog read-view parsing | 0.002 | 0.001 |
| Backend join | 0.916 | 1.708 |
| Work after the join | 0.044 | 0.040 |
| Response JSON encoding | 0.013 | 0.011 |
| Cache-envelope parsing | 0.017 | 0.015 |
| Cache-envelope encoding | 0.006 | 0.005 |
| Cache publication | 0.088 | 0.277 |

**These rows overlap and must not be added.** Response encoding sits inside the
work after the join; the join includes concurrent count, coverage and ordered-page
RPCs plus their local response handling. The fractions above were calculated for
each individual request before taking the median, not by dividing table medians.

The count RPC finished last among those three operations in **100 of 100**
single-client cold requests and **86 of 100** eight-client cold requests. Ordered
response parsing had medians of **0.015 / 0.016 ms**. That makes backend work a
better-supported investigation target than JSON parsing for this workload. It
does not prove the count endpoint is the only cost or that dropping exact totals
would preserve the same request contract.

Warm dispatch medians were 0.092 / 0.228 ms; bypass medians were 1.018 / 2.080 ms.
The [full phase summary](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-gateway-phases/gateway-phases-summary.json)
and [per-request relationships](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-gateway-phases/gateway-phases-relationships.json)
retain every profile, including tails. Profiles ran in a fixed order and are not
paired speed comparisons.

### What the clocks include

Dispatch timing excludes HTTP request-body ingress, final diagnostic-header
serialization and network delivery. Client-observed cold p50 was 1.500 / 3.442 ms;
subtracting those independent medians would not measure a specific overhead.
The RPC send-to-headers interval includes connection-pool waits, socket work,
backend execution and scheduling. Body receipt and parsing have separate nested
intervals. Wall intervals are not CPU measurements.

Resource snapshots include untimed worker connection primers. A zero rounded
microsecond interval does not mean work is free. The trace records no request
bodies, credentials or backend URLs. Production builds omit the timing header.

### Reproduce the diagnostic, then restore production

The diagnostic image was built from
[`68add4d`](https://github.com/dstockton/qdrant-es-gateway/commit/68add4dc8aecd8b01ad501ee30ab4d94518f03b6)
with `diagnostic-timing` enabled. The runner is pinned separately in the
[manifest](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-gateway-phases/manifest.json),
which also contains both image identities and the restoration record.
Use the prepared shared-cache corpus and resource settings from that revision:
gateway 0.5 CPU/512 MiB, Qdrant 1.25 CPUs/3,328 MiB and Valkey 0.25 CPU/256 MiB.
Qdrant keeps its automatic two/eight pool budgets and cache serving is enabled.

In an isolated checkout of the diagnostic source, build its dedicated image:

```sh
docker build -f benchmark/shared-cache/diagnostic.Dockerfile \
  -t qdrant-es-gateway:phase-diagnostic .
```

Copy the retained [runner](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-gateway-phases/gateway_phases.py)
and [Compose override](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-gateway-phases/phase-image.yml)
into `benchmark/shared-cache/`. Load `CACHE_BENCH_DEPLOYMENT` from the preparation
file, set `CACHE_BENCH_SERVE=true`, and use `PHASE_GATEWAY_IMAGE` with the existing
three-engine/shared-cache Compose files plus the override to recreate only the
gateway. Wait for readiness before capture. Record the locally built image ID:

```sh
python3 benchmark/shared-cache/gateway_phases.py \
  --image-id '<full locally verified diagnostic image ID>' \
  --source 68add4dc8aecd8b01ad501ee30ab4d94518f03b6
```

The runner requires fresh cold keys and refuses to overwrite evidence. For another
capture, use a new prefix and an unused `--start` range; retain that changed
selectivity. It does not deploy or restore images. After capture, recreate the
gateway with the retained production image and verify readiness and the absence
of diagnostic headers. Our [restoration record](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-gateway-phases/gateway-phases-restoration.json)
confirms production was restored with cache serving enabled.

All raw traces, resource observations, runner copies and hashes are retained;
public records remove only ephemeral container IDs. No additional performance
claim follows from instrumenting the path. The useful result is a better basis
for choosing the next correctness-preserving experiment.
