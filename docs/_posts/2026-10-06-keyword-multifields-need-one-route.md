---
layout: post
title: "Keyword multi-fields need one route"
date: 2026-10-06
---

Elasticsearch clients and application code commonly address an exact-value view as `brand.keyword`. The gateway already treated that conventional suffix as the base `brand` field when it evaluated hydrated source, sorted results, collapsed groups, or requested a terms facet. Native Qdrant filters did not: a `term`, `terms`, `range`, or `exists` clause was sent upstream with the dotted key unchanged. With a base keyword payload, that request searched a field that was never written and could quietly return no hits.

The filter translator and gateway-side pattern collector now use the same suffix normalization as the existing source paths. The focused regression covers all four native predicate shapes, confirms that source evaluation agrees, and exercises a `prefix` query against `sku.keyword`.

Reproduce it with:

```bash
cargo test keyword_multifield_aliases_use_the_base_payload_field
cargo test gateway_patterns_match_keyword_values
```

This is a compatibility alias, not full Elasticsearch multi-field emulation. It does not create a second analyzed value, support custom analyzers, or make an arbitrary text field safe for exact filtering. The base field must already be mapped and mirrored as a filterable payload for native filters to work.
