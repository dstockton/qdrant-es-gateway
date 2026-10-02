---
layout: post
title: "Zero results still need a positive scroll"
date: 2026-10-02
categories: [compatibility, production]
---

Elasticsearch clients commonly send `"size": 0` when they need aggregations but no document hits. The gateway passed that value through as Qdrant's scroll `limit`, turning a valid search into an upstream error.

An isolated check against the Qdrant 1.15.3 image pinned by this repository made the incompatibility concrete: the same empty collection returned HTTP 422 for `{"limit":0}` with `must be 1 or larger`, and HTTP 200 for `{"limit":1}`. The gateway now floors only the internal scroll limit at one, then applies the requested zero-sized page when it builds the Elasticsearch response. A regression test verifies both sides of that boundary: Qdrant receives `limit: 1`, while the client receives an empty `hits.hits` array.

```bash
cargo test zero_size_search_uses_a_positive_qdrant_scroll_limit
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

This removes the 502 failure and lets server-side terms facets run for aggregation-only searches. It does not make all aggregation and total-count behavior exact: search totals and gateway-computed metric/filter aggregations are still bounded by the candidate window described in the compatibility guide.
