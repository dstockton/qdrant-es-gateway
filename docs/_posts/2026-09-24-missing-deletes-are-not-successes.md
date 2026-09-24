---
layout: post
title: "A missing delete is not a successful delete"
date: 2026-09-24
---

Deleting an absent document is intentionally idempotent, but Elasticsearch still tells the client that nothing was removed. The gateway previously returned HTTP 200 with `"result": "deleted"` because Qdrant's point-delete operation succeeds even when the point does not exist.

The gateway now checks the authoritative document store before deleting. A missing ID returns HTTP 404 with `"result": "not_found"`, matching Elasticsearch's [delete result vocabulary](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-delete):

```json
{
  "_index": "products",
  "_id": "missing",
  "_version": 1,
  "result": "not_found",
  "_shards": {
    "total": 1,
    "successful": 1,
    "failed": 0
  }
}
```

Bulk delete keeps Elasticsearch's useful nuance: the item has status 404 and `"result": "not_found"`, but it has no `error` object and the top-level `errors` flag remains `false`. Missing updates are different because they failed to apply; those still carry `document_missing_exception` and set `errors` to `true`.

The regression test exercises standalone and bulk missing deletes in both storage modes. The trade-off is one point lookup before each delete, including successful deletes. As with the update existence check, a concurrent writer can race between the lookup and delete; the gateway does not yet implement Elasticsearch sequence numbers or primary-term concurrency controls.
