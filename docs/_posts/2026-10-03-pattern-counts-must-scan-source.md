---
layout: post
title: "Pattern counts must scan the source"
date: 2026-10-03
categories: [compatibility, correctness]
---

The gateway evaluates `wildcard`, `prefix`, and `regexp` clauses against hydrated `_source` documents because Qdrant does not receive an equivalent native filter. Search already followed that rule, but `_count` did not: it removed the pattern while building the Qdrant filter and then called Qdrant's count endpoint. A pattern-only count therefore returned the size of the whole collection.

Pattern counts now use the same paginated source scan and matching rules as pattern searches. A regression serves two mock Qdrant scroll pages containing `ABC-100`, `XYZ-100`, and `ABC-200`; `{"wildcard":{"sku":"ABC-*"}}` returns `2` and makes two scroll requests instead of an unfiltered count request.

Run the focused check without a Qdrant process:

```bash
cargo test count_applies_gateway_patterns_across_scroll_pages
```

This improves correctness, not query cost. A pattern-only count is linear in the candidate set, retains matching hits while the shared search path computes its total, and can observe concurrent writes between scroll pages. It is appropriate for modest catalog fields; large or latency-sensitive deployments should model a natively indexed exact-match field instead.
