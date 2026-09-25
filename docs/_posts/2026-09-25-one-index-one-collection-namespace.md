---
layout: post
title: "One index, one collection namespace"
date: 2026-09-25
categories: [production, correctness]
---

The gateway derives Qdrant collection names from Elasticsearch index names. Its original mapping kept ASCII letters, numbers, underscores, and hyphens, while replacing other characters with underscores. That makes `catalog.v2` and `catalog_v2` distinct Elasticsearch names but the same Qdrant collection name. A second edge case exists when document projection is enabled: `catalog_documents` uses the collection name reserved for `catalog`'s document store.

Those collisions are more serious than a confusing create error. If both names reached the same collection, reads and writes could cross an index boundary. The gateway now checks the durable index catalogue before making any Qdrant request and rejects a name that overlaps either the search or document collection namespace of an existing index. The check reserves both collection names even when document projection is currently disabled, so turning the mode on later cannot expose a previously accepted naming collision.

The regression test covers punctuation normalization and the `_documents` suffix case. It also asserts that rejected creation makes zero upstream requests and publishes no index metadata.

This is a guard around the existing naming scheme, not a new reversible encoder. Existing deployments keep their current collection names. Operators importing metadata created outside the gateway should still audit collection ownership before startup; the check prevents new collisions recorded through the gateway but cannot repair an already-colliding external state.
