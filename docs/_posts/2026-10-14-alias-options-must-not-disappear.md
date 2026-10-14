---
layout: post
title: "Alias options must not disappear"
date: 2026-10-14
categories: [production, compatibility, elasticsearch, opensearch]
---

The gateway deliberately implements unfiltered, unrouted aliases with one concrete target. Its alias action parser previously read only `index` and `alias`, but silently ignored every other metadata field. A request for a tenant filter, custom routing, or a write index could therefore return `{"acknowledged":true}` while the stored alias exposed all documents and routed every operation normally. That is more dangerous than an unsupported-feature error because the response tells the caller that a data-access or write-routing boundary exists when it does not.

Elasticsearch 9.5.5 and OpenSearch 3.9.0 expose the same relevant action fields, but they were checked independently. The versioned [Elasticsearch request parser](https://github.com/elastic/elasticsearch/blob/v9.5.5/server/src/main/java/org/elasticsearch/action/admin/indices/alias/IndicesAliasesRequest.java#L98-L108) accepts `filter`, `routing`, `index_routing`, `search_routing`, `is_write_index`, and `is_hidden` for add actions, plus `must_exist` for removals. The versioned [OpenSearch request parser](https://github.com/opensearch-project/OpenSearch/blob/3.9.0/server/src/main/java/org/opensearch/action/admin/indices/alias/IndicesAliasesRequest.java#L106-L116) declares the corresponding fields, and the separate [OpenSearch alias documentation](https://docs.opensearch.org/latest/api-reference/alias/aliases-api/) describes their effects. This is source-level evidence for the two engines, not an assumption that verification against one applies to the other.

The gateway now rejects every action metadata field outside its supported `index` and `alias` subset. Known native options and unknown fields receive a structured HTTP 400 naming the exact path, such as `_aliases.add.filter` or `_aliases.remove.must_exist`. Validation happens before index lookup and before the SQLite transaction, so a rejected request cannot create, remove, or modify an alias and never contacts Qdrant.

```console
cargo test alias_actions_reject_options_the_gateway_cannot_apply
```

The regression covers filters, all three routing forms, write-index and hidden flags, removal `must_exist`, and an unknown option. It also asserts that the catalogue still contains only its original alias after all failures. Docker was not responsive during this run, so no live Elasticsearch, OpenSearch, or Qdrant endpoint result is claimed. This change does not add filtered, routed, hidden, or multi-index aliases; it makes the existing compatibility boundary honest and fail closed.
