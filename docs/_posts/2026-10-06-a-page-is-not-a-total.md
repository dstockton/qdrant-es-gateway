---
layout: post
title: "A page is not a total"
date: 2026-10-06
categories: [compatibility, production]
---

The gateway used to retrieve at most `from + size` filter matches and then report the length of that candidate window as an exact Elasticsearch `hits.total`. A request for zero hits could therefore say that zero documents matched even when the collection contained many matches. Pagination made the same error less obvious: a ten-hit page could claim the total was ten.

Filter-only searches now reuse the exact Qdrant count operation already used by the `_count` endpoint. A regression fixture exposes 37 matching documents while the search asks for `size: 0`; the gateway sends `{"exact":true}` to Qdrant, keeps the internal scroll limit at one, returns no hit documents, and reports `{"value":37,"relation":"eq"}`. This verifies the protocol behavior without pretending that a local mock is a latency benchmark.

A live check against the repository's pinned Qdrant 1.15.5 image indexed three documents, two with the requested keyword, and returned an exact total of two with an empty hit page. The check waited one second after the bulk write: an immediate probe observed the project's existing refresh approximation and returned zero before the write became visible. That timing is a separate limitation, not evidence that the count path is inaccurate once the write is visible.

```bash
cargo test zero_size_search_uses_a_positive_qdrant_scroll_limit
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

The accuracy has a cost: an eligible filter-only search now makes a count request as well as its hit-retrieval request. Text searches and searches with `post_filter` still use a bounded candidate window; they now return `relation: "gte"` instead of incorrectly describing that lower bound as exact. Approximate-text and pattern paths already scan all candidates and remain exact. The gateway does not yet implement Elasticsearch's configurable `track_total_hits` threshold, and gateway-computed metric/filter aggregations remain bounded.
