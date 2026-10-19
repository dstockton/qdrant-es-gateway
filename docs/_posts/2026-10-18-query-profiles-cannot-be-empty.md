---
layout: post
title: "Query profiles cannot be empty"
date: 2026-10-18
categories: [production, compatibility, elasticsearch, opensearch]
---

The gateway accepted `profile: true` in a search body but ignored it. That turns a diagnostic request into an ordinary search while still returning HTTP 200. It is especially misleading here because the gateway executes against Qdrant, not Lucene: a native-looking query and collector timing tree cannot be inferred from the returned hits.

A focused probe used the same mapped fields, three-document corpus, and match query against separate Elasticsearch 8.15.0 and OpenSearch 3.9.0 containers. Both returned a top-level `profile` object with one shard, a `TermQuery` timing breakdown, and collector data. The observed collectors differed: Elasticsearch reported `QueryPhaseCollector`, while OpenSearch reported `TopScoreDocCollector`. For this small compact response, enabling profiling increased the body from 419 to 2,124 bytes in Elasticsearch and from 420 to 1,758 bytes in OpenSearch. Those byte counts illustrate response shaping only; they are not a latency, CPU, allocation, or throughput benchmark.

The sanitized [request and response evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/elasticsearch-8.15-opensearch-3.9-search-profile-2026-10-18.json) records the images, corpus, selected response fields, reproduction sequence, and limitations. The [Elasticsearch search profiling documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-profile.html) and [OpenSearch Profile API documentation](https://docs.opensearch.org/latest/api-reference/search-apis/profile/) independently describe their profiling responses and overhead cautions.

The gateway now rejects `profile: true` before index lookup or Qdrant access, including in each `_msearch` subrequest. Explicit `profile: false` remains accepted, and non-boolean JSON values fail validation instead of being ignored. The focused regression proves the rejected requests perform no upstream work:

```console
cargo test invalid_search_options_fail_before_qdrant_work
```

Existing searches that omit `profile` or set it to `false` are unchanged. Implementing gateway-native profiling later would require an explicitly different schema that reports gateway and Qdrant phases honestly rather than borrowing Lucene component names.
