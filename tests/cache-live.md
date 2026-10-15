# Coordinated cache correctness check

These disposable services are independent of the three-engine benchmark. They
exercise real Qdrant writes and authoritative Valkey coordination through two
gateway replicas, with document projection enabled. This is a correctness test,
not a performance measurement. Ports 16380, 19203 and 19204 must be unused.

From the repository root:

```sh
docker compose -f tests/cache-live.compose.yml up -d qdrant valkey
docker compose -f tests/cache-live.compose.yml build gateway-a gateway-b
# Initialize only this NEW, disposable catalog before cache bootstrap.
docker compose -f tests/cache-live.compose.yml exec valkey valkey-cli SET 'qeg:{cache-live}:v1' '{"indices":[],"aliases":[]}' NX
docker compose -f tests/cache-live.compose.yml run --rm gateway-a cache bootstrap --quiesced
docker compose -f tests/cache-live.compose.yml up -d gateway-a gateway-b
python3 tests/cache_live.py
```

Bootstrap validates AOF readiness. If Valkey is still initializing/re-writing its
AOF, wait for that operation to finish and retry bootstrap; do not weaken the
checks. Gateways must start only after bootstrap succeeds. The test creates a
UUID-named index, exercises collection/mapping/alias and all supported document
write routes, checks epoch advances and released writer tokens, compares point
and sorted/aggregate responses across replicas, exercises explicit cache bypass,
and verifies oversized responses stay intact. It removes its index afterward.

Run a replica with serving disabled to verify it still participates in write
coordination:

```sh
CACHE_TEST_SERVE_B=false docker compose -f tests/cache-live.compose.yml up -d --force-recreate gateway-b
python3 tests/cache_live.py
docker compose -f tests/cache-live.compose.yml run --rm gateway-a cache status
```

The Rust `cache_runtime` tests add deterministic barriers for cold-read/alias
publication races, cancellation after backend acceptance, acknowledged writes,
HTTP-200 bulk item uncertainty, and simultaneous catalog administration. Against
an isolated Valkey endpoint:

```sh
CATALOG_TEST_REDIS_URL=redis://127.0.0.1:16380 cargo test --bin qdrant-es-gateway cache_runtime -- --include-ignored
```

Remove only these disposable services and their data when finished:

```sh
docker compose -f tests/cache-live.compose.yml down -v
```

For an optional destructive fault check on **these disposable services only**,
restore B's serving flag, then use the actual container names from Compose:

```sh
CACHE_TEST_SERVE_B=true docker compose -f tests/cache-live.compose.yml up -d --force-recreate gateway-b
CACHE_TEST_QDRANT_CONTAINER=qeg-cache-live-test-qdrant-1 \
CACHE_TEST_VALKEY_CONTAINER=qeg-cache-live-test-valkey-1 \
python3 tests/cache_failure_live.py
```

This pauses only the disposable Qdrant process to prove a second replica serves
warm point/search hits without a backend, while explicit bypass reaches the
backend and fails. An attempted collection creation then times out; its writer
and administrative claims must remain. Valkey is restarted and the test proves
the writer survives and old responses remain fenced. Qdrant is unpaused in a
`finally` block. The test intentionally leaves uncertain state: stop the two
gateways and either reconcile it using the documented operator recovery process,
or discard this entire disposable stack with `down -v`. Never clear its token
just because a timeout elapsed.

## Disposable response-entry persistence proof

The standalone [cache_ephemeral_aof.py](cache_ephemeral_aof.py) test reads the
production Lua scripts and creates its own isolated Valkey container and durable
volume. It uses no host ports or existing stack data, so it does not require the
Compose services above. Run it against the candidate source containing response-only
propagation suppression:

```sh
python3 tests/cache_ephemeral_aof.py --output /tmp/cache-ephemeral-aof-proof.json
```

Choose a fresh output filename; the test refuses to overwrite evidence. It checks
that response fills skip incremental AOF persistence while catalog changes,
generation advances and persistent writer tokens survive an abrupt restart.
It also exercises a rewrite that retains an entry with its original expiry,
rejection of that restored entry after a durable generation change, and a
publication error followed by a durable writer admission. This is a correctness
proof for the production scripts, not a performance benchmark or a substitute for
the two-gateway runtime tests above.

Rewrites and snapshots may include response entries even though ordinary fills
skip incremental AOF propagation. Identity and generation validation remain
mandatory, and this does not promise that all fsync work disappears. Successful
runs remove only the container and volume they created. Failed runs retain their
own stopped container and volume for inspection, recording their names in the
JSON output.
