---
layout: post
title: "Arrays are multi-valued fields, not scalar mismatches"
date: 2026-10-05
---

Elasticsearch does not need a separate array field type: an ordinary field can carry several values, and a document matches when an applicable value satisfies the query. Qdrant payload filters use the same practical rule—an array filter succeeds when at least one value meets the condition ([Qdrant payload documentation](https://qdrant.tech/documentation/concepts/payload/)).

The gateway already inherited that behavior for filters executed inside Qdrant. Its local source evaluator did not. That evaluator is used by `post_filter`, filter aggregations, and gateway-side `prefix`, `wildcard`, and `regexp` queries. It compared the complete JSON array with one scalar term, required ranges to contain a scalar number, and required pattern fields to be scalar strings. The same document could therefore match a native filter but disappear when the equivalent clause moved to `post_filter`.

The source evaluator now recursively tests scalar values inside arrays and succeeds when any value matches. One regression covers exact term, terms-set, numeric range, prefix, wildcard, and regular-expression predicates over three array-valued fields. It also checks a non-matching range so the rule does not turn into an unconditional array match.

Reproduce the focused check with:

```bash
cargo test gateway_predicates_treat_arrays_as_multi_valued_fields
```

The full Rust suite contains 45 passing unit tests; the live Compose smoke remains opt-in. This change does not add Elasticsearch `nested` query semantics for arrays of objects, and it does not change the gateway's documented response-level sorting or collapse approximations. It only aligns scalar-array predicate evaluation across the native and hydrated-source paths.
