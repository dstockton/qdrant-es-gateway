# Search controls cannot be decoration

The gateway accepted `min_score`, `terminate_after`, and per-search `timeout` fields but did not apply them. That is more dangerous than a clear unsupported-feature response: a caller could ask to remove low-scoring hits, stop collection early, or return within a deadline and instead receive an ordinary complete page.

Separate ephemeral-container probes exercised Elasticsearch 9.5.5 and OpenSearch 3.9.0 with the same one-shard, three-document corpus. `min_score: 2` removed all three `match_all` hits on both engines. `terminate_after: 1` returned one hit with `terminated_early: true` on both, but Elasticsearch reported a total of three while OpenSearch reported one. Both engines accepted `timeout: "1s"`, returned `timed_out: false` for this small request, and rejected the invalid value `"banana"` with HTTP 400. The sanitized [request and response evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/elasticsearch-9.5.5-opensearch-3.9-search-controls-2026-10-16.json) records the versions, images, requests, and observed fields.

The gateway now rejects all three controls before index lookup or Qdrant access. Supporting them requires explicit decisions about score comparability, partial-result behavior, and the Elasticsearch/OpenSearch early-termination total difference; silently choosing one engine's shape would misrepresent the other. The focused regression includes valid-looking values for every field and proves that none reaches the mocked Qdrant endpoint:

```console
cargo test invalid_search_options_fail_before_qdrant_work
```

This is request/response evidence, not a latency benchmark. The one-second timeout did not expire, and the small one-shard corpus does not characterize distributed early termination. Existing searches that omit these fields are unchanged; `_msearch` inherits the same fail-closed validation for each query.
