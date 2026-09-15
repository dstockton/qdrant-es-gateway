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

Each ES index maps to a Qdrant collection named `es_<safe-index-name>`. SQLite stores the original mapping, text-field vector names, and aliases so restarts do not depend on collection introspection. This makes the current service stateless with respect to documents—there is no local source cache—but not fully stateless as a horizontally replicated service: the metadata database must be persisted or shared by replicas. Moving this small, rebuildable metadata catalog into Qdrant would be the follow-up required for true replica-safe statelessness. Documents are payload-first: the original source is stored under `_source`, while top-level fields are duplicated for Qdrant filtering.

Text fields get named sparse representations populated using Qdrant's server-side `qdrant/bm25` Document model. `multi_match` queries each requested field and merges unique hits with the declared boost. This is intentionally a useful application-search approximation, not Lucene score identity.

Point IDs are UUID-shaped values derived from SHA-256(index, NUL, ES ID). The original ID remains in `_es_id`; GET/update/delete therefore remain deterministic across restarts.
