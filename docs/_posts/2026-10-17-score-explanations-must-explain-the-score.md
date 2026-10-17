---
layout: post
title: "Score explanations must explain the score"
date: 2026-10-17
categories: [production, compatibility, elasticsearch, opensearch]
---

The gateway accepted `explain: true` in a search body but ignored it. That is not merely missing decoration: clients use each hit's `_explanation` tree to debug ranking, and the gateway's score comes from Qdrant rather than Lucene. Fabricating a Lucene-shaped explanation or silently omitting it would both misdescribe how the result was ranked.

A focused probe used the same mapped text field, one-document corpus, and match query against separate Elasticsearch 8.15.0 and OpenSearch 3.9.0 containers. Both added `_shard`, `_node`, and a nested `_explanation` to the matching hit. The engines did not produce identical output: Elasticsearch reported score `0.2876821` and included an explicit boost component, while OpenSearch reported `0.13076457` without that component in this version pair. The sanitized [request and response evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/elasticsearch-8.15-opensearch-3.9-search-explain-2026-10-17.json) records the images, request, selected response fields, reproduction sequence, and limitations. The [Elasticsearch Search API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html) and [OpenSearch Search API](https://docs.opensearch.org/latest/api-reference/search-apis/search/) independently document the per-hit explanation contract.

The gateway now rejects `explain: true` before index lookup or Qdrant access, including in each `_msearch` subrequest. Explicit `explain: false` remains accepted, and non-boolean values fail validation instead of being ignored. The focused regression proves the rejected requests perform no upstream work:

```console
cargo test invalid_search_options_fail_before_qdrant_work
```

This is request/response compatibility evidence, not a relevance or performance benchmark. The score difference should not be generalized to all Elasticsearch and OpenSearch versions, mappings, or queries. Existing searches that omit `explain` or set it to `false` are unchanged.
