---
layout: page
title: Shared metadata across gateway replicas
permalink: /shared-catalog/
---

The default SQLite catalog is still the simplest choice for one gateway. No measurement has established SQLite as a bottleneck. The optional Redis/Valkey catalog addresses **metadata ownership across replicas**, not faster search.

Set `CATALOG_REDIS_URL` to a `redis://` or TLS `rediss://` URL. All replicas serving the same Qdrant collections must use the same `CATALOG_REDIS_KEY` (default `qeg:{catalog}:v1`) and collection-related configuration. The URL may contain credentials; supply it through a secret. Redis Cluster is not a supported connection topology.

The authoritative catalog contains mappings, named vector metadata, and single-index aliases. Every request loads the current committed snapshot into an ephemeral SQLite database. There is no replica metadata cache and no persistent local metadata dependency in shared mode. Snapshot size is limited to 8 MiB. Fetching and materializing the whole catalog on each request favors small catalogs and correctness; it is not a high-throughput optimization. Network operations time out after five seconds.

## Starting a new deployment

Use a dedicated durable Redis/Valkey instance, disable eviction, enable AOF persistence and select the durability/HA policy appropriate to your deployment. An acknowledged metadata update is only as durable as that server's configuration. Losing an acknowledged catalog update during asynchronous failover can invalidate ownership guarantees: do not automatically fail over to a stale replica and resume writes. Stop gateways, restore/reconcile the catalog and Qdrant, then restart. Redis/Valkey is an authoritative database here, not a disposable cache.

1. Start Redis/Valkey with `--appendonly yes --appendfsync always --maxmemory-policy noeviction` and a persistent volume.
2. For an **empty** deployment, run one gateway once with `CATALOG_INITIALIZE=true` and `CATALOG_REDIS_URL` set. This creates an empty catalog only if absent.
3. Remove `CATALOG_INITIALIZE` before normal operation. Leaving it enabled would permit accidental reinitialization after data loss.
4. Start replicas with the same catalog URL/key and Qdrant configuration. Readiness checks now require an accessible, unlocked catalog; liveness remains independent.

Existing SQLite catalogs are **not automatically migrated**. Do not switch an existing deployment to an empty shared catalog: stop writes and export/validate the existing mappings, vector lists and aliases first. The versioned JSON format is `{ "indices": [["name", "mapping JSON", "vector-list JSON"]], "aliases": [["alias", "index"]] }`. Back up both stores, validate in staging, and only then switch all replicas together. Do not mix SQLite and shared-catalog replicas against the same collections.

For Helm, create a Secret containing the URL and set:

```yaml
replicaCount: 2
catalog:
  existingSecret: gateway-catalog
  existingSecretKey: redis-url
  key: 'qeg:{catalog}:v1'
persistence:
  enabled: false
```

Initialize the catalog separately before deploying these values. The chart deliberately never enables automatic initialization. It does not provision Redis/Valkey.

## Administration and recovery

Creating/deleting an index, changing mappings, or changing aliases acquires one non-expiring administrative claim. Another replica cannot mutate metadata concurrently; new requests fail closed while the claim exists. Requests already in flight may finish using their previously acquired mapping. A successful operation publishes the entire updated catalog and releases its claim in one Redis/Valkey script.

The claim deliberately has **no TTL**: expiring a lock while a slow or disconnected writer still changes Qdrant permits two administrators to act at once. An upstream error after a mutation was attempted, cancelled request, crash or ambiguous timeout therefore leaves the claim in place. Validation errors before an upstream mutation release the claim safely. The gateway returns the original error and logs that recovery is required. This prioritizes safe ownership over automatic availability.

Recovery requires an operator:

1. Stop all gateway replicas and ensure their outstanding Qdrant operations have completed.
2. Back up and inspect the catalog key and its `<catalog-key>:admin` claim. Compare the affected mappings/collections/aliases with Qdrant; resolve any partial creation/deletion or payload-index changes. A successful-but-unacknowledged commit may already have removed the claim.
3. Restore or repair the catalog to match the intended Qdrant state. Only after reconciliation, delete the administrative claim with Redis/Valkey's `DEL` command.
4. Restart the gateways and verify readiness and the affected operation.

Do not delete a claim while a writer may still be running. A Redis/Valkey transaction cannot atomically include Qdrant writes. This backend does not supply document `_create` or update compare-and-set across replicas, point-in-time search, exactly-once projections, multi-index aliases, or Lucene analyzer equivalence.

## Verification

Unit tests cover snapshot round-tripping, malformed metadata and administrative route classification. Run the real-server coordination test against a disposable Redis/Valkey instance:

```sh
CATALOG_TEST_REDIS_URL=redis://127.0.0.1:16379 cargo test shared_catalog_serializes_replicas -- --ignored
```

The test checks fresh cross-client reads, concurrent administration exclusion, absence of lock expiry, wrong-owner rejection and safe release before an upstream mutation and retention of a claim after its owner disappears. It creates unique temporary keys and deletes them on success.
