---
layout: post
title: "Two collections, one stateless gateway"
date: 2026-09-18
categories: [operations]
---

Reading a product's current source and finding it through search are different operations. Qdrant ES Gateway can store the source separately from its searchable projection, allowing a write to wait for durable source storage while the search projection converges asynchronously.

With `DOCUMENT_PROJECTION=true`, Qdrant ES Gateway uses the Qdrant engine to keep a searchable collection and a durable document collection named `es_<index>_documents` for each Elasticsearch index. The document collection has no payload indexes and supplies GETs and full `_source` responses. The searchable collection carries the fields needed for retrieval, filters, and sorts. Search hits' sources are fetched from the document collection in one batch.

Asynchronous projection is a separate choice: set `ASYNC_SEARCH_PROJECTION=true` to wait for the source write while using Qdrant's asynchronous operation acknowledgement for the search projection. A successful source write does not imply that search has caught up. Unmapped metadata-only updates can avoid sparse-index work, while fields used for filters or sorts remain mirrored in the search projection.

The stateless gateway tier still depends on persisted state. Qdrant holds the documents and search projection, and the gateway-owned SQLite metadata database must also be persisted. For production asynchronous projection, the [production guide]({{ "/production/" | relative_url }}) recommends a durable projection worker or reconciliation process rather than relying on an in-process queue.

For a write-heavy catalogue, this design makes source durability and search freshness separate deployment concerns. It does not provide Elasticsearch-identical refresh, version, or sequence semantics; those remain approximations to evaluate against the application's requirements.
