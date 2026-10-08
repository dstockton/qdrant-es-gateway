---
layout: post
title: "Invalid pages should not query Qdrant"
date: 2026-10-08
categories: [compatibility, resource-usage]
---

Pagination inputs sit on a resource boundary. [Elasticsearch documents `from` as non-negative, `size` as non-negative, and `search_after` as an array of scalar sort values](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search). It also limits the ordinary `from` plus `size` result window to 10,000 by default and directs deeper pagination to `search_after`.

The gateway previously used defaults when `from` or `size` had the wrong JSON type, silently capped an oversized `size`, ignored a non-array `search_after`, and fetched candidates from Qdrant before discovering some cursor shape errors. Those responses could look valid while representing a different page from the one requested, and malformed requests still consumed upstream work.

Search pagination now passes through one fail-fast validator before index lookup or Qdrant access. `from` and `size` must be non-negative integers, `size` cannot exceed the configured `MAX_PAGE_SIZE`, and `from + size` cannot exceed 10,000. `search_after` requires one string, number, boolean, or null per explicit sort field and cannot be combined with a non-zero offset. A regression sends malformed and over-limit variants to a mock Qdrant server and verifies both the structured compatibility error and a zero upstream request count.

```bash
cargo test invalid_pagination_fails_before_qdrant_work
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

This is validation, not unbounded pagination. The gateway still sorts only the bounded candidate window it retrieves, its 10,000 result-window boundary is fixed rather than index-configurable, and its cursor is stable only while that ordered set is unchanged. `MAX_PAGE_SIZE` may be set below 10,000 as a stricter operator-controlled resource limit.
