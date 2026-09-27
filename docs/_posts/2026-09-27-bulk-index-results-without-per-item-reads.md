---
layout: post
title: "Accurate bulk index results without per-item reads"
date: 2026-09-27
---

Elasticsearch bulk `index` items distinguish inserts from replacements. A new ID reports HTTP 201 with `result: "created"`; an existing ID reports HTTP 200 with `result: "updated"`. The gateway previously returned 201 for every successful item, which made replacement-heavy imports look like all-new ingestion.

Bulk writes still need to remain bulk writes. The gateway now sends one Qdrant retrieve request for all IDs in each contiguous same-index batch, then keeps the existing single upsert request. It tracks repeated IDs in request order, so two index actions for the same initially missing ID report `created` and then `updated`. Requests made through an alias report the concrete index in each result.

The regression test covers existing, new, and repeated IDs in both storage modes. Across those two cases it observes two batched reads and three writes: one write for embedded-source mode, and the document plus search-projection writes required by dedicated document storage.

```sh
cargo test bulk_index_reports_created_and_updated_without_losing_batching
```

This adds one authoritative point-retrieval round trip per contiguous bulk index batch, not one per document. It is still a preflight observation rather than an atomic compare-and-write guarantee: another writer can change an ID between the lookup and upsert. The gateway also does not emulate Elasticsearch version counters or sequence-number concurrency controls.
