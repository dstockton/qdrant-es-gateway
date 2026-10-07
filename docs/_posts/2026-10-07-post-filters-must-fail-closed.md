---
layout: post
title: "Unsupported post filters must fail closed"
date: 2026-10-07
categories: [compatibility, correctness]
---

The gateway documents a deliberately small `post_filter` subset: term, terms, numeric range, exists, match-all, and bool combinations of those clauses. An unsupported clause previously fell through the hydrated-source predicate as `false`. The request then succeeded with no hits, making an unsupported query indistinguishable from a valid filter that matched nothing. A multi-field term clause had a related problem: only the first field was evaluated.

The gateway now validates the complete `post_filter` tree before it looks up the index or contacts Qdrant. Unsupported query types, unknown bool options, non-numeric range bounds, and clauses with more than one field receive the existing structured HTTP 400 compatibility error. A regression calls search with a nonexistent index and unreachable Qdrant URL, then verifies that an unsupported wildcard post-filter still fails specifically as `post_filter.wildcard`; this makes the fail-before-upstream ordering reproducible without external services.

```bash
cargo test post_filter_validation
cargo test invalid_post_filter_fails_before_index_or_qdrant_lookup
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

This change does not expand the supported filter set or make `post_filter` counts exact. Post-filtering still operates on the bounded candidate window returned by the main query, and date range evaluation remains outside this hydrated-source path. The benefit is narrower: unsupported semantics are reported honestly instead of being converted into plausible but incorrect empty search results.
