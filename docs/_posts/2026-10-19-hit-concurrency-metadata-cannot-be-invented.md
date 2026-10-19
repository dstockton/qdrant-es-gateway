---
layout: post
title: "Hit concurrency metadata cannot be invented"
date: 2026-10-19
categories: [production, compatibility, elasticsearch, opensearch]
---

The gateway accepted `version: true` and `seq_no_primary_term: true` in search bodies but omitted the requested per-hit metadata. That is more serious than a cosmetic response difference: native clients can use sequence numbers and primary terms as optimistic-concurrency tokens, while the gateway's approximate write fields do not carry Elasticsearch or OpenSearch's shard history.

A focused probe used the same one-document index in separate Elasticsearch 8.15.0 and OpenSearch 3.9.0 containers. After creating and then updating the document once, both engines returned `_version: 2`, `_seq_no: 1`, and `_primary_term: 1` on the search hit when the two flags were enabled. Both omitted all three fields when the flags were explicitly `false`. The [Elasticsearch Search API](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/search-search.html) and [OpenSearch Search API](https://docs.opensearch.org/latest/api-reference/search-apis/search/) describe the same response controls.

The sanitized [request and response evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/elasticsearch-8.15-opensearch-3.9-hit-concurrency-metadata-2026-10-19.json) records the images, corpus, update sequence, selected response fields, and limitations. This is request/response compatibility evidence, not a test of concurrent writers or token enforcement.

The gateway now rejects either flag when `true`, before index lookup or Qdrant access, including in each `_msearch` subrequest. Explicit `false` remains accepted. Non-boolean values fail gateway validation instead of relying on the string coercion observed in the two probed native versions.

```console
cargo test invalid_search_options_fail_before_qdrant_work
```

Ordinary searches are unchanged. Supporting these flags later requires a real concurrency model whose tokens can be honored by conditional writes; copying the gateway's fixed response counters into hits would make unsafe metadata look authoritative.
