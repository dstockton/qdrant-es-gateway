---
layout: post
title: "Highlight snippets cannot vanish"
date: 2026-10-17
categories: [production, compatibility, elasticsearch, opensearch]
---

The gateway accepted a search `highlight` object but ignored it. That changes the response contract: clients commonly request snippets to show where a result matched, and a successful response without those snippets can render an empty preview or force an unsafe fallback to the complete source text.

A focused probe used the same one-document corpus and request against separate Elasticsearch 8.15.0 and OpenSearch 3.9.0 containers. Both returned HTTP 200 and added `{"title":["wireless <em>headphones</em> for travel"]}` under the matching hit's `highlight` field. This independently confirms the contract for both targets; it does not assume OpenSearch behavior from Elasticsearch. The sanitized [request and response evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/elasticsearch-8.15-opensearch-3.9-highlight-2026-10-17.json) records the images, versions, fixture, request, result, reproduction sequence, and limitations. The [Elasticsearch](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/highlighting) and [OpenSearch](https://docs.opensearch.org/latest/search-plugins/searching-data/highlight/) references likewise describe the per-hit response field.

The gateway now rejects `highlight` before index lookup or Qdrant access. Implementing it honestly would require analyzer-aware match offsets, fragment boundaries and scoring, multi-field rules, source/stored-field behavior, and engine-specific option validation. Returning ordinary hits while dropping the request is not a safe approximation. The focused regression proves both `_search` and each `_msearch` subrequest inherit the same fail-closed path without sending an upstream request:

```console
cargo test invalid_search_options_fail_before_qdrant_work
```

This is request-level compatibility evidence, not a performance result. It covers one ordinary text field with the default highlighter; it does not claim identical Elasticsearch and OpenSearch behavior for other highlighters, analyzers, fragments, or versions. Existing searches without `highlight` are unchanged.
