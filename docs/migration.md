# Migration guide

1. Create a representative index and mapping through the normal ES API.
2. Re-ingest documents with `_bulk` or use Qdrant's official migration tooling for the data-movement portion.
3. Point the existing Elasticsearch client at the gateway URL.
4. Run representative traffic and inspect `/_qdrant_gateway/compatibility`.
5. Compare matching IDs, filter correctness, p95 latency, and relevance labels against Elasticsearch.

Do not migrate a workload that depends on Kibana, observability APIs, scripts, nested/parent-child semantics, or arbitrary aggregations without redesigning those paths.
