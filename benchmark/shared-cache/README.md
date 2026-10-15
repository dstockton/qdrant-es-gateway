# Actual shared-cache comparison

This exercises the real gateway cache, not the synthetic ceiling probe. It has
no recorded performance result until the integration checks and runs finish.

Use a separate disposable Compose project, `qeg-shared-cache`, retaining the
original three-engine mapping, deterministic corpus and strict v3 assertions.
Ports 19300, 19301 and 19302 avoid modifying the existing benchmark deployment.
The override requires Compose support for `!override`. Build the production
image `qdrant-es-gateway:cache-runtime` before preparation or timing.

Each native engine keeps 2 CPUs and 4 GiB. The gateway setup shares that exact
aggregate cap: gateway 0.5 CPU/512 MiB, Qdrant 1.25 CPU/3328 MiB, and Valkey
0.25 CPU/256 MiB. Valkey uses its own durable named volume, AOF with
`appendfsync always`, `no-appendfsync-on-rewrite no`, `noeviction`, and a 64-MiB
`maxmemory` ceiling inside its 256-MiB container. Its memory and durable-write
costs count toward the comparison. All replicas are one; native heaps remain
512 MiB. Do not run two full benchmark deployments concurrently if the Docker VM
cannot support both; coordinate stopping the other experiment first.

## Prepare, without reusing an active catalog

```sh
python3 benchmark/shared-cache/prepare.py
export CACHE_BENCH_DEPLOYMENT="$(python3 -c 'import json; print(json.load(open("benchmark/shared-cache/results/preparation.json"))["deployment"])')"
```

The helper refuses existing project containers, volumes or preparation output.
It starts a fresh catalog with coordination unset, ingests identical data into
all three engines through the existing runner, verifies search-visible counts,
stops the gateway, explicitly bootstraps while quiescent, and restarts with
coordination enabled. It never deletes existing volumes or recovers uncertain
state. A failed preparation is retained for investigation. Wait until all
endpoints answer and Valkey reports completed AOF initialization before timing.
No writes, builds or tests may run concurrently with read measurements.

## Serving-off/on paired warm rounds

Compare the **same image** with shared coordination enabled in both variants.
Only `READ_CACHE_SERVE` changes; native requests and native cache settings stay
identical. The serving-off baseline includes the authoritative catalog and write
coordination, so it does not measure total overhead versus cache-free deployment.

```sh
export CACHE_BENCH_SERVE=false
# Use this command after every serving-mode change, then wait for readiness:
docker compose -p qeg-shared-cache -f benchmark/three-engine/compose.yml \
  -f benchmark/shared-cache/compose.yml up -d --no-build --no-deps gateway
ROUND=off-1 REQUESTS=200 TRIALS=3 WARMUP=40 \
  OUTPUT=benchmark/shared-cache/results/off-1.json python3 benchmark/shared-cache/measure.py
# Repeat as on-1, on-2, off-2, changing CACHE_BENCH_SERVE accordingly.
python3 benchmark/three-engine/summarize_reads.py \
  benchmark/shared-cache/results/off-1.json benchmark/shared-cache/results/off-2.json
python3 benchmark/three-engine/summarize_reads.py \
  benchmark/shared-cache/results/on-1.json benchmark/shared-cache/results/on-2.json
```

The wrapper primes every measured request for every engine before the ordinary
warmups, since forty warmups do not cover two hundred distinct point/cursor
requests. It retains native cache counters and sanitized Valkey observations,
actual image IDs, caps and serving mode. Exact totals are the primary profile;
repeat `TRACK_TOTAL_HITS=false` into separate files if useful. Never combine the
two request profiles or discard slower trials. Cache entry TTL is 300 seconds;
expiration during a run is part of observed behavior, not a reason to delete it.

`REQUEST_CACHE=false` may be a separate diagnostic applied identically to all
engines. Do not substitute it for the default native-cache comparison.

## Cold requests and invalidation

Warm results alone cannot establish an application advantage. Use
`mixed.py` for separately reported cold unique sorted-filter queries and
write-followed-by-read cycles. It retains all engine rows and strict results;
these profiles must never be pooled into warm-read medians.

```sh
PROFILE=cold REQUESTS=200 TRIALS=3 OUTPUT=benchmark/shared-cache/results/cold-on.json \
  python3 benchmark/shared-cache/mixed.py
PROFILE=invalidation REQUESTS=100 TRIALS=3 OUTPUT=benchmark/shared-cache/results/writes-on.json \
  python3 benchmark/shared-cache/mixed.py
```

Repeat both with serving off and on, using the same image/caps. The invalidation
profile changes a real document. Point-read cycles omit `refresh` for every
engine because native GET is realtime; forcing refresh would unnecessarily
penalize those engines. Filter-count cycles use `refresh=true` for every engine
because the next search requires immediate visibility. Durable write costs are
included rather than hidden by a preloaded fixture. Cold-profile setup and final
restoration use `refresh=true` outside the read timers. The source is restored
afterward. “Cold” here means previously unused response-cache keys on already
loaded data, not cold operating-system caches or storage. Do not run this
profile alongside any other benchmark, as it deliberately invalidates caches.

Preserve raw outputs and preparation provenance. Stopping task containers is
sufficient afterward; retain volumes for replay. No cleanup command here deletes
the authoritative catalog, cache control, or durable Valkey volume.

The v3 validation profile uses a `range` object for filtered-search expected IDs
instead of allocating the corpus-sized list in each client worker. Assertions
remain identical; no quantified timing effect is claimed. Keep historical v2
and current v3 measurements separate. Recorded configurations also distinguish
serving mode, so summary tools reject accidental off/on or historical pooling.
