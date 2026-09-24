---
layout: post
title: "A missing update is not an upsert"
date: 2026-09-24
---

Elasticsearch's update API does not create a missing document unless the request explicitly asks for upsert behavior. The gateway previously lost that distinction: an update to an absent ID could return `"result": "updated"`, even though Qdrant had no point to change. A text-field update could go further and create the document from an empty source.

The gateway now checks existence before every partial update. Missing documents return HTTP 404 with Elasticsearch's `document_missing_exception` shape:

```json
{
  "error": {
    "type": "document_missing_exception",
    "reason": "[missing]: document missing",
    "index": "products",
    "shard": "0"
  },
  "status": 404
}
```

Bulk updates report the same item-level error and set the top-level `errors` flag to `true`. Requests with `"doc_as_upsert": true` remain explicit creation requests; they return HTTP 201, including as bulk item statuses.

The regression test runs the missing, bulk, and `doc_as_upsert` cases against mock Qdrant responses in both storage modes. This correctness check adds one point lookup to partial updates that previously avoided a read. It does not add support for scripted updates or the separate `upsert` document form, and the gateway still does not emulate Elasticsearch's optimistic-concurrency or version counters.
