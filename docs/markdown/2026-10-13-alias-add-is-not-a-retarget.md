# Alias add is not a retarget

The gateway stores one concrete index per alias. Its alias transaction used SQLite `INSERT OR REPLACE` for an `add` action, so adding an existing alias to a different index silently discarded the original target. That looked like a convenient retarget, but it changed the meaning of a standard Elasticsearch/OpenSearch request and could redirect application reads and writes without the caller expressing a swap.

A direct endpoint probe against Elasticsearch 9.5.5 and OpenSearch 3.9.0 used the same five-request sequence: create `products-v1` and `products-v2`, add `current-products` to the first, add it to the second, then inspect the alias. Both native engines acknowledged both adds and returned both indices as targets. This agrees with the independent [Elasticsearch alias documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/aliases.html) and [OpenSearch manage-aliases documentation](https://docs.opensearch.org/latest/api-reference/alias/aliases-api/): aliases can cover multiple indices, and a move is expressed as remove plus add in one atomic request. The sanitized [probe evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/elasticsearch-9.5.5-opensearch-3.9-alias-add-2026-10-13.json) records the versions, requests, acknowledgements, and final targets.

The gateway still deliberately supports only single-target aliases. It now evaluates the final topology of the complete action batch before opening the write transaction. An add-only request that would require two targets receives a structured HTTP 400 compatibility error, and the existing target remains unchanged. A remove+add swap remains accepted in either action order because the final topology has one target and the SQLite transaction makes the change atomically visible.

```console
cargo test alias_add_does_not_silently_retarget_a_single_index_alias
```

This is a fail-closed compatibility improvement, not multi-index alias support. The gateway does not yet merge searches across indices, select a write index, apply alias filters, or reproduce native per-action partial-failure responses. The endpoint probe measures request semantics only; it makes no latency or resource-use claim.
