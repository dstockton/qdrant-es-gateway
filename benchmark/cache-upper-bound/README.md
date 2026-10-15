# Synthetic cache performance ceiling

This is a standalone **benchmark diagnostic, not a gateway cache implementation**.
It asks whether an optimistic warm Valkey lookup plus normal HTTP/JSON work has
sufficient latency headroom to justify implementing a correct cache. Results
must use the participant name `cache_probe`, never “gateway + Qdrant”.

The corpus and requests come unchanged from `../three-engine/run.py`. Capture
reads the retained production gateway, independently checks every result using
the existing benchmark assertions, and stores each distinct request/response.
The default 200-request profile produces 644 distinct entries across both
`track_total_hits` modes. Fixtures are public synthetic data, not production data.

On each request, the probe validates JSON, hashes length-prefixed method, full
URI and raw request body, and makes one preloaded Lua call over a persistent
multiplexed connection. Lua checks fixture identity and epoch, zero active
writers in a required control hash, catalog presence and absence of an administrative claim, then reads the
matching entry. The response is parsed, `took` is refreshed and JSON is serialized
normally. It does not return a prebuilt HTTP byte string. Missing state, guard
failure or an unknown request returns 503; writes and foreign routes are rejected.

The protocol is intentionally incomplete: startup creates synthetic state and
prepopulates immutable entries. There is no write fencing, miss publication,
invalidation, crash recovery, authorization partitioning or external-writer
support. **Passing guard tests does not prove a production cache correct.** All
requests are assumed warm hits. A failure here to beat native latency is useful
negative evidence; a win only motivates further investigation.

## Reproduce

Build all images and finish all other compilation before measuring. The commands
below use the existing disposable `qeg-balanced` project and its 10,000-document
corpus. First ensure its gateway is the retained production image; capture must
not run against this probe.

```sh
# From the repository root, while the real gateway is still on port 19202:
REQUESTS=200 python3 benchmark/cache-upper-bound/capture.py
# Build the isolated binary, not the production gateway.
docker build -t qdrant-es-gateway:cache-upper-bound benchmark/cache-upper-bound
# This creates a separate Valkey; it does not reuse the shared catalog service.
docker compose -p qeg-balanced -f benchmark/three-engine/compose.yml \
  -f benchmark/cache-upper-bound/compose.yml up -d --no-build
# Wait for GET http://localhost:19202/ to report synthetic-probe.
REQUESTS=200 TRIALS=3 WARMUP=40 ROUND=probe-1 \
  OUTPUT=benchmark/cache-upper-bound/results/probe-1.json \
  python3 benchmark/cache-upper-bound/measure.py
```

Repeat with both total-hit modes, interleaved native controls and all rounds
retained. The wrapper reuses the existing strict correctness checks and stores
raw samples, response bytes, images and actual limits. Unknown fixture requests
fail rather than silently reducing the measured sample.

Each native engine retains its 2-CPU/4-GiB cap. The synthetic setup is capped at
probe 0.5 CPU/512 MiB, Valkey 0.25 CPU/256 MiB, and Qdrant 1.25 CPU/3328 MiB:
**2 CPUs and 4 GiB combined**. Qdrant remains provisioned but receives no reads on
hits. Valkey uses an isolated service with no persistence and `noeviction`; no
existing cache or catalog is cleared. Fixture identity is derived from fixture
contents, so leftover entries from a different fixture cannot become hits.

After the experiment, restore the retained production image (build/tag it before
starting) and Qdrant's original 1.5-CPU/3584-MiB limit. Preserve evidence first.

```sh
docker compose -p qeg-balanced -f benchmark/three-engine/compose.yml \
  -f benchmark/cache-upper-bound/restore.yml up -d --no-build --no-deps qdrant gateway
docker stop qeg-balanced-cache-probe-valkey-1
```

## Verify separately from timing

```sh
# Reuse a target directory if disk is constrained.
PROBE_TEST_VALKEY_URL=redis://127.0.0.1:16380 \
  cargo test --manifest-path benchmark/cache-upper-bound/Cargo.toml -- --include-ignored
cargo clippy --manifest-path benchmark/cache-upper-bound/Cargo.toml \
  --all-targets -- -D warnings
```

The live guard test uses its own key prefix on the isolated Valkey and exercises
writer presence, changed/missing epoch and identity, missing writer_count or the
entire control hash, missing catalog,
administrative claims and unknown entries. Unit tests check request-key
boundaries and rejection of write routes.

All five workloads are diagnostic ceilings. If the initial production-cache
scope is only size-zero aggregations, only the terms and filter aggregation
results inform that scope; hit-page and point-read gains do not extend it.
