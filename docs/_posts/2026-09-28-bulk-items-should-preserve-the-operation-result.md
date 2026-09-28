---
layout: post
title: "Bulk items should preserve the operation result"
date: 2026-09-28
---

An Elasticsearch bulk response is more than a status code. Each successful item identifies the concrete index that handled the operation and describes whether the document was created, updated, deleted, or not found. That distinction matters when a request uses an alias: clients need the backing index in the response, not the alias they sent.

The gateway's batched `index` path already built that response deliberately. The individual `create`, `update`, and `delete` paths did not. They discarded the successful standalone response, reconstructed a smaller item, and copied `_index` from the request. Alias-routed writes therefore reported the alias, while successful create and update items also omitted `result` and the version/shard metadata that the operation had already produced.

Bulk assembly now preserves the successful operation body and adds only the item-level HTTP status. A regression test sends create, update, and delete actions through `current-products` and verifies that every item reports the concrete `products` index, the correct result, and the existing version and shard fields:

```bash
cargo test bulk_alias_items_preserve_concrete_operation_results
```

This is a response-fidelity change; it adds no Qdrant requests and does not change write ordering or acknowledgement. It also does not make a bulk request transactional, add Elasticsearch version counters, or close the documented preflight/write races. Failed items retain their existing structured error shape, and deleting a missing document remains a non-error item with status 404 and `result: "not_found"`.
