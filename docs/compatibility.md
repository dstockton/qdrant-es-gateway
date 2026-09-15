# Compatibility and semantic differences

Supported requests cover CRUD, mappings, bulk ingestion, basic lexical search, payload filters, source projection, count, indexed keyword terms facets, health, and durable aliases. Contiguous bulk index/create actions are translated into one Qdrant upsert batch. The compatibility analyzer at `GET /_qdrant_gateway/compatibility` is local-only and reports request counts; no telemetry leaves the process.

Qdrant's server-side BM25 uses sparse vectors and its own tokenization/model implementation. Results should be evaluated by relevant document IDs and product metrics, not numerical `_score` equality with Elasticsearch. `match_phrase` currently uses the same native lexical representation and is therefore not a Lucene positional phrase guarantee.

Filter-only `should` clauses and `minimum_should_match` are supported when they can be represented as Qdrant filter alternatives. The gateway rejects scored `should`, `search_after`, fuzzy/regexp/wildcard, scripts, scroll, arbitrary aggregations, and analyzer settings that would materially change semantics. Every rejection identifies the request feature and a safe alternative where one exists.
# Pattern queries

`regexp`, `prefix`, and `wildcard` clauses are evaluated by the gateway using Rust's regular-expression engine after candidate documents are read from Qdrant. Positive pattern clauses can be combined with ordinary Qdrant-translated filters in `bool.must` or `bool.filter`; pattern clauses in `should` and `must_not` are rejected rather than approximated. A pattern-only query scans the collection in bounded scroll pages, so it is useful for modest catalog and identifier searches but should not be used as a substitute for a dedicated indexed pattern field at very large scale. Wildcards use Elasticsearch-style `*` and `?`; regular expressions use Rust/RE2-style syntax and reject invalid expressions with a structured 400 response.
