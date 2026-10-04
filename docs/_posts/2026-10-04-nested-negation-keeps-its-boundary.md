---
layout: post
title: "Nested negation keeps its boundary"
date: 2026-10-04
categories: [compatibility, correctness]
---

Elasticsearch bool clauses accept either one query object or an array. They also preserve the nested bool boundary: excluding a bool that requires both `status = deleted` and `tenant = internal` means `NOT (status = deleted AND tenant = internal)`, not `status != deleted AND tenant != internal`.

The gateway previously handled only the array form of `must_not`. A scalar negative clause was silently omitted. When a nested bool appeared inside `must_not` or `should`, its inner `must_not` clauses were also discarded, and multiple required conditions under one negation were flattened into separate exclusions. Those cases widened or narrowed the result set without warning.

The Qdrant filter translator now builds one explicit filter condition for each nested clause. Required and excluded children stay grouped before the parent applies negation or `min_should`. The regression covers scalar double negation, a negative bool inside `should`, and the distinction between `NOT (A AND B)` and `NOT A AND NOT B`.

Run the focused check without Qdrant:

```bash
cargo test bool_negative_clauses_preserve_scalar_and_nested_semantics
```

The emitted shapes were also exercised against the pinned Qdrant 1.15.5 image. With one `deleted` and one `live` point, double negation selected only `deleted`; the negative `should` alternative selected only `live`.

This correction covers filter-only term, terms, range, exists, IDs, and nested bool conditions. Text queries under `must_not` or filter-only `should` remain rejected because translating their scoring and exclusion semantics safely requires a different execution path.
