# Architecture

The gateway is a small Axum service with three boundaries:

```text
Elasticsearch client / REST
          |
   compatibility handlers
          |
 translation + response shaping
          |
      Qdrant REST API
          |
      Qdrant collections
```

Each ES index maps to a Qdrant collection named `es_<safe-index-name>`. SQLite stores mappings, vector names, and aliases in the default single-gateway deployment. The optional [Redis/Valkey catalog](shared-catalog.md) keeps authoritative metadata outside the gateway; each request materializes a fresh ephemeral SQLite view. Gateway replicas then require no durable local catalog. Metadata administration uses an explicit ownership/recovery protocol; there is no transaction spanning the catalog and Qdrant. Documents are payload-first: the original source is stored under `_source`, while mapped non-text fields are mirrored for Qdrant filtering, sorting, and aggregations.

When `DOCUMENT_PROJECTION=true`, every index also has `es_<safe-index-name>_documents`. This second collection is a durable document projection with no payload indexes and no HNSW graph. Qdrant 1.15 requires a vector field on points, so the gateway uses a one-dimensional on-disk placeholder vector and `m=0`; it is never searched. The searchable collection contains sparse representations plus only `_es_id`, `_es_index`, and mapped non-text fields. GETs and search response hydration read the authoritative `_source` from the document collection in a batch, which avoids storing the full source in the searchable collection. A full write persists the document projection first, then updates the searchable projection. `ASYNC_SEARCH_PROJECTION=true` uses Qdrant's asynchronous operation acknowledgement for the second step, so metadata-only writes can be acknowledged without waiting for sparse-index maintenance. The gateway itself owns no document queue or source cache.

Synchronous point writes and payload-index creation send Qdrant's `wait=true` URL parameter, so acknowledgment waits for the operation to finish. Asynchronous modes send `wait=false`. Earlier gateway revisions placed this control in the JSON body, which did not enforce completion; historical ingestion timings from those revisions measure acknowledgment rather than guaranteed completed writes. Completion is distinct from a transaction across collections or a guarantee about every replica's read visibility.

The two collections are intentionally not treated as one transaction. The source write is the durability boundary; projection updates must be idempotent and should be retried/reconciled by an external durable worker in an HA deployment. Filter and sort fields that must be read-after-write correct must remain mirrored in the searchable collection. Unmapped metadata can remain source-only. This is an explicit eventual-consistency mode, not an attempt to hide the trade-off behind local state.

Text fields get named sparse representations populated using Qdrant's server-side `qdrant/bm25` Document model. `multi_match` queries each requested field and merges unique hits with the declared boost. This is intentionally a useful application-search approximation, not Lucene score identity.

Point IDs are UUID-shaped values derived from SHA-256(index, NUL, ES ID). The original ID remains in `_es_id`; GET/update/delete therefore remain deterministic across restarts.
