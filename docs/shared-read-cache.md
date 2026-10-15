---
layout: page
title: Shared read cache
permalink: /shared-read-cache/
---

The optional shared read cache keeps gateway replicas stateless while reusing successful point reads and filter searches. Qdrant remains the document store. Valkey holds the authoritative catalog, cache entries, and persistent write coordination. Caching is off by default.

The [measured three-engine comparison]({% post_url 2026-10-15-shared-cache-warm-search-results %}) found 12–30% lower warm-search median latency than the faster native engine in a 10,000-document catalogue profile. Fresh queries and write-followed-by-point-read cycles were slower than native engines; point-read warm gains were small. These results do not establish a general production advantage. Measure cold reads, repeated reads, writes, and your actual hit rate before adopting it.

## Supported deployment

Use a dedicated standalone Valkey primary with no attached replicas, persistent storage, `appendonly yes`, `appendfsync always`, `no-appendfsync-on-rewrite no`, a nonzero `maxmemory`, and a supported eviction policy that cannot evict persistent catalog or control keys. `noeviction` is the simplest configuration. Cached entries have bounded sizes and expire; control keys and unresolved writer tokens do not.

Qdrant must run as a standalone node with distributed mode disabled. Every existing collection must have replication factor one. Set `QDRANT_REPLICATION_FACTOR=1` so new collections have the same constraint. Both asynchronous write settings must be off. Startup checks these requirements; a configuration value alone does not prove existing collections are suitable.

Every gateway writing to this deployment must participate in coordination. Stop and drain old replicas before activation. An old binary, a direct Qdrant writer, or an independent metadata writer can bypass invalidation and is unsupported. Do not use automatic Valkey failover or restore an older control snapshot. A surviving but rolled-back control record is not detectable from its contents. A gateway without `CATALOG_REDIS_URL` cannot discover this deployment at all; it is an unsupported unfenced writer even if it runs a new binary.

## Activate deliberately

First provision the authoritative catalog as described in [Shared catalog]({{ '/shared-catalog/' | relative_url }}). Stop all gateway writers and external writers. Use the same catalog endpoint and namespace for every command and replica. Do not set `CATALOG_INITIALIZE=true` during cache operation.

Choose a new deployment identity, for example a UUID, and configure:

```sh
export READ_CACHE_DEPLOYMENT="<new-deployment-uuid>"
export QDRANT_REPLICATION_FACTOR=1
export ASYNC_PAYLOAD_WRITES=false
export ASYNC_SEARCH_PROJECTION=false
export READ_CACHE_SERVE=true
export READ_CACHE_TTL_SECONDS=60
export READ_CACHE_MAX_BYTES=1048576
qdrant-es-gateway cache bootstrap --quiesced
```

The command requires an existing catalog and no unresolved administrative claim. It will not overwrite an existing control record. Persistent lineage also prevents bootstrap from treating a lost control record as a new deployment; use explicit recovery instead. Then start all replicas with this configuration. `READ_CACHE_SERVE=false` disables cache reads and fills on a replica while retaining mandatory write coordination.

Deployment identity is separate from response compatibility: cache keys also include a digest of the actual executable, response-affecting configuration, method, URI, and request body. Different builds do not reuse each other's responses. Keys do not contain plaintext credentials.

## What happens around a write

Before the gateway starts a mutation, it advances the shared generation and records a persistent writer token. A cached response is usable only in the same quiet generation with no writer or catalog administrative claim. A cold read captures its generation before loading catalog metadata; publication fails if a writer intervenes.

Each backend mutation must report completion. An HTTP success or bulk HTTP 200 alone is not sufficient. The gateway releases its token only when all attempted backend operations and its own catalog publication are known complete. Cancellation, a crash, an ambiguous backend response, or failed publication leaves the token in place. This disables cached reads and fills; it does not silently serve stale results after a timeout.

This does not add transactions, optimistic concurrency control, point-in-time snapshots, or full Elasticsearch/OpenSearch semantics. Uncached reads retain existing gateway behavior, including visibility during multistep writes. Explicit `request_cache=false`, ranked queries, vector queries, unsupported shapes, and oversized responses remain uncached.

## Maintenance and recovery

Inspect outstanding tokens with:

```sh
qdrant-es-gateway cache status
qdrant-es-gateway cache maintenance
```

Maintenance prevents new coordinated mutations and cached reads. Stop and drain all replicas before changing Qdrant data, topology, credentials, catalog state, or Valkey persistence. A cached hit cannot discover a revoked upstream credential without contacting Qdrant, so upstream authorization changes require this maintenance procedure too.

After a crash or uncertain write, reconcile Qdrant and catalog state and account for every pending backend operation. An elapsed timeout is not proof that an operation cannot finish later. Resolve any catalog administrative claim using its documented recovery procedure. Keep all writers stopped. Choose a fresh identity and explicitly acknowledge that reconciliation is complete:

```sh
qdrant-es-gateway cache recover --new-identity "<fresh-deployment-uuid>" --reconciled
```

The expected old identity comes from `READ_CACHE_DEPLOYMENT`. Configure every replica with the new identity before restarting. Recovery invalidates old entries and must never reuse an earlier identity. The command cannot establish the operator's reconciliation proof. Control-only loss can be recovered using the persisted lineage and expected old identity. Total namespace or lineage loss, or restoration of older state, requires stopping all writers, reconciling Qdrant and the catalog, and provisioning a fresh catalog/cache namespace with a fresh identity. Never recreate the catalog and resume the old cache identity.

## Costs and limits

Warm reads still make a Valkey request. Cold reads also run the ordinary Qdrant query and may write a cache entry. Every mutation adds durable coordination writes, and any write invalidates the shared generation. Frequent writes or low reuse can erase the benefit. The aggregate resource budget must include Valkey as well as the gateway and Qdrant when comparing native engines.

Valkey AOF durability follows its [persistence configuration](https://valkey.io/topics/persistence/). Qdrant completion and replication are separate guarantees; see [Qdrant consistency guarantees](https://qdrant.tech/documentation/scaling/consistency-guarantees/).

For Helm, set `readCache.deployment`, `catalog.existingSecret`, and `qdrant.replicationFactor: 1`, and disable both `gateway.asyncPayloadWrites` and `gateway.asyncSearchProjection`. Set `readCache.serve: false` to bypass cache serving while preserving coordination. The chart does not bootstrap or recover state automatically. Scale every writer to zero for initial activation and perform the operator command before restoring replicas.
