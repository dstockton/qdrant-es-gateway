---
layout: post
title: "Find the next page across the whole catalogue"
date: 2026-10-15
categories: [compatibility]
---

Sorting the first few candidates is not the same as finding the first few sorted
results. Likewise, asking for a page after price 20 cannot work reliably if the
gateway only fetched records priced below 20.

Filter-only searches with explicit field sorting now inspect every matching
Qdrant scroll page. The gateway keeps the best `from + size` candidates, drops
records before `search_after`, and continues until the scroll ends. Without
collapse, retained hit candidates are bounded by the requested window plus the
current upstream page.

```json
{"size":2,"sort":[{"rank":"asc"}],"search_after":[2]}
```

The six-document [comparison fixture](https://github.com/dstockton/qdrant-es-gateway/tree/main/validation/aggregations)
expects records 3 and 4 from that request. It sends the same request to
Elasticsearch and to the gateway. A deterministic regression also puts the
cheapest record on a later page and verifies that it wins the first sorted page.

`post_filter` now scans beyond the initial candidate window too. Matching records
on later pages can fill the requested page, and the hit count covers the complete
post-filtered set. Aggregations continue to see the query before `post_filter`.

Run `cargo test aggregation_scroll_visits_every_page_and_uses_search_projection_values`
to exercise those cases without a running database.

This preserves stateless request handling: no server-side cursor session or Redis
entry is required. The trade-off is scanning matching records for each request.
Use a unique final sort field for predictable cursor boundaries. These changes
apply to filter-only searches; ranked text pagination retains its documented
candidate limits. Collapse still needs retained group members, so its memory cost
is different. Concurrent writes can move records between pages because this is
not a point in time snapshot.

The container replay against Elasticsearch 8.15.0 and Qdrant 1.19.2 passed all four cases: complete aggregations, post-filtered hits, the first sorted page, and its cursor continuation. The [captured results](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/aggregations/results.json) include actual request timings. They establish correctness for this fixture, not a speed advantage.
