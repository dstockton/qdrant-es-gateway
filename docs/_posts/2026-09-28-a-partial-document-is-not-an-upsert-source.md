---
layout: post
title: "A partial document is not an upsert source"
date: 2026-09-28
---

Elasticsearch's Update API supports two source documents with different jobs. `doc` is merged into a document that already exists. A separate `upsert` object is inserted when the ID is absent. They need not contain the same fields:

```json
{
  "doc": {"price": 42},
  "upsert": {"title": "Initial", "price": 10}
}
```

The gateway already supported `doc_as_upsert`, but it required `doc` to double as the creation source. It now accepts the separate form for both standalone and bulk updates. An absent ID writes the `upsert` object and returns HTTP 201 with `result: "created"`; an existing ID continues to merge `doc` and ignores the creation source.

The regression test exercises both gateway storage layouts, verifies the exact `_source` sent to Qdrant, and proves that creation uses one existence lookup rather than the two previously incurred by the `doc_as_upsert` path:

```bash
cargo test supported_update_upserts_write_selected_source_with_one_preflight_each
```

This is not support for scripted updates. It also does not make the lookup and insert atomic across gateway replicas, implement Elasticsearch sequence numbers, or remove the normal projection-write costs. Invalid non-object `upsert` values are rejected before any document lookup or write.
