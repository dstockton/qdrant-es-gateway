---
layout: post
title: "Optional should really is optional"
date: 2026-10-04
categories: [compatibility, correctness]
---

Elasticsearch changes the default for `minimum_should_match` according to the other clauses in the same `bool`. A `should`-only query requires one match, but `should` becomes optional when `must` or `filter` is present. The [Elasticsearch bool query reference](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html) documents the defaults as one and zero respectively.

The gateway used to reject the common required-plus-optional shape unless the client explicitly sent `minimum_should_match`. Its gateway-side source evaluator also required one `should` match, which could incorrectly remove hits from `post_filter`, filter aggregations, and approximate lexical scans.

The default is now resolved once and shared by both the Qdrant filter translator and the source evaluator. This regression covers a document that matches `filter.brand = Acme` but not `should.featured = true`: it remains a hit when the minimum is omitted and is excluded when the client explicitly requests one match. A second regression covers the same boundary in the approximate text evaluator.

Run the focused checks without Qdrant:

```bash
cargo test bool_should
```

This aligns hit selection, not scoring. Elasticsearch uses optional query-context clauses to boost relevant documents. Filter-only optional clauses are omitted from the Qdrant filter when their effective minimum is zero, so they do not change Qdrant scores. Clients that require a `should` clause to affect eligibility should continue to send an explicit `minimum_should_match`.
