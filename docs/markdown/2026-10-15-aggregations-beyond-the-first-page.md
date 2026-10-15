# The cheapest item may be on the next page

A price filter should describe the matching catalogue, even when the shopper only
asks for one product. The gateway previously calculated `min`, `max`, and filter
counts from the hit candidates it had fetched. A small `size` could therefore
produce a plausible but incomplete answer.

The fix separates those two jobs. Qdrant scrolls through every matching page;
the gateway retains the current extrema and bucket counters, then discards each
page. The hit limit no longer sets the scope of an aggregation. `post_filter`,
collapse, and `search_after` still affect hits, not aggregation membership.

For example:

```json
{"size":0,"aggs":{"lowest_price":{"min":{"field":"price"}},"brands":{"terms":{"field":"brand","size":2}}}}
```

Our regression puts prices 50 and 2 on separate pages. The result is 2, including
when the search returns no hits. Terms now count the complete matching set too,
deduplicate repeated values within one document, and report the actual count
outside the returned top buckets.

Run `cargo test aggregation_scroll_visits_every_page_and_uses_search_projection_values`
for the deterministic two-page check. The repository's
[`validation/aggregations` comparison](https://github.com/dstockton/qdrant-es-gateway/tree/main/validation/aggregations)
also sends the same six-document fixture and requests to Elasticsearch 8.15.0 and
the gateway with Qdrant. It prints the observed results and individual timings.

This improves correctness within the gateway's supported subset; it is not a
claim that scanning is faster than Elasticsearch's native aggregations. A full
scan costs work proportional to the matching data. Metrics use constant state;
buckets have an explicit 10,000-distinct-value limit per aggregation and fail
rather than return truncated totals. There are at most 100 aggregations per request.

Ranked text and pattern queries are rejected when combined with aggregations,
because their candidate membership cannot currently be reduced exactly this way.
Concurrent writes can change a scroll while it runs. With document projection,
reductions use mapped non-text values in the search projection, keeping membership
and values together instead of fetching a newer source document.

The container replay against Elasticsearch 8.15.0 and Qdrant 1.19.2 passed all four cases: complete aggregations, post-filtered hits, the first sorted page, and its cursor continuation. The [captured results](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/aggregations/results.json) include actual request timings. They establish correctness for this fixture, not a speed advantage.

A later [three-engine benchmark](https://dstockton.github.io/qdrant-es-gateway/markdown/2026-10-15-indexed-query-performance.md)
adds guarded indexed paths for compatible numeric sorting, keyword facets and
simple filter counts. The complete scan described here remains the fallback
when the gateway cannot prove those operations preserve the result.
