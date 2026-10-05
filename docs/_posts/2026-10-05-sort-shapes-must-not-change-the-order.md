---
layout: post
title: "Sort syntax must not change the order"
date: 2026-10-05
categories: [compatibility, correctness]
---

[Elasticsearch's sort reference](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/sort-search-results) permits a basic field sort as `"price"`, `{"price":"asc"}`, or `{"price":{"order":"desc"}}`. The gateway previously understood only the direct-order object. It treated the standard options object as ascending regardless of its `order`, while a string field silently became `_score` descending. The same misread specification was then used to generate and consume `search_after` values.

Sort clauses now pass through one validated parser before any Qdrant request. Field names default to ascending, `_score` defaults to descending, and both direct and options-object `asc`/`desc` forms drive response ordering, returned sort values, and cursor comparison. `_id` and `_score` are read from hit metadata rather than incorrectly looked up in `_source`. Unsupported options and invalid directions return a structured 400 instead of producing a plausible but wrong page.

The regression uses a three-document response with a price tie. It verifies descending price with `_id` as a tie-breaker, advances past a two-value `search_after` cursor, and checks the single-string ascending form. Reproduce it without a live Qdrant instance:

```bash
cargo test elasticsearch_sort_shapes_drive_order_and_search_after
```

This does not turn response sorting into Qdrant-native global ordering. The gateway still sorts only the bounded candidate window it retrieves, and the cursor remains stable only while that ordered set is unchanged. Large collections or deep ordered pagination should use a native Qdrant ordering design rather than treating this compatibility layer as a distributed sort engine.
