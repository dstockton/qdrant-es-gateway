---
layout: post
title: "A post-filter is not an aggregation filter"
date: 2026-10-07
categories: [compatibility, correctness]
---

Elasticsearch applies `post_filter` after aggregations. This lets a catalogue page show only one selected brand while its facet and metric aggregations still describe every document matched by the main query. The gateway applied the post-filter to its hydrated candidate hits before computing min, max, filter, and keyed-filter aggregations, so those aggregations described only the visible hits. Server-side terms facets did not have this bug, which made mixed aggregation responses especially misleading.

The search path now preserves its query-matched candidate window before applying hit-only transformations. Gateway-computed aggregations read that preserved window, while `post_filter`, collapse, and `search_after` continue to affect the returned hits. A request-level regression supplies three products, post-filters hits to the single Acme product, and verifies that a min aggregation still sees the lower-priced Beta products and that a Beta filter aggregation still counts both of them.

```bash
cargo test post_filter_does_not_narrow_gateway_computed_aggregations
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

This is a semantic correction, not an exact-aggregation implementation. These gateway-computed aggregations still see only the bounded candidate window retrieved for the search, so large or highly selective result sets can remain incomplete. Preserving the pre-filter window also temporarily keeps a second copy of those in-memory candidate values for requests that contain aggregations. Terms aggregations continue to use Qdrant's facet API and do not need that copy.
