---
layout: post
title: "Bulk action metadata is a write boundary"
date: 2026-09-30
---

A bulk action line decides which operation, index, and document ID the gateway will mutate. Treating malformed metadata as if it were absent is therefore unsafe: a numeric `_id`, for example, previously fell through to generated-ID behavior for `index` and `create` actions. An action line containing more than one operation also silently selected whichever key the JSON object exposed first.

The gateway now validates the complete bulk request before executing any item. Every action line must contain exactly one of `index`, `create`, `update`, or `delete`; its metadata must be an object; and explicit `_index` and `_id` values must be strings. A malformed later action returns a structured HTTP 400 before an earlier valid action can write anything.

The regression test covers five cases, including malformed metadata after a valid item:

```bash
cargo test malformed_bulk_actions_fail_before_any_item_is_executed
```

This is request-level validation, not transactionality. A syntactically valid bulk request still has Elasticsearch-style item responses and may partially succeed when an upstream operation fails. Generated IDs also remain supported for `index` and `create` only when `_id` is genuinely omitted.
