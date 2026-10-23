# Migration guide

1. Create a representative index and mapping through the normal ES API.
2. Re-ingest documents with `_bulk` or use Qdrant's official migration tooling for the data-movement portion.
3. Point the existing Elasticsearch client at the gateway URL.
4. Run representative traffic and inspect `/_qdrant_gateway/compatibility`.
5. Compare matching IDs, filter correctness, p95 latency, and relevance labels against Elasticsearch.

Do not migrate a workload that depends on Kibana, observability APIs, scripts, nested/parent-child semantics, or arbitrary aggregations without redesigning those paths.

## Upgrading an existing Qdrant volume

The Compose files use Qdrant 1.20.0 for new deployments. If an existing volume
was last opened by Qdrant 1.15.x, do not start it directly with that image.
Back up the data first, then start the latest patch of each minor release in
order: 1.15.5, 1.16.3, 1.17.1, 1.18.3, 1.19.2, and finally 1.20.0. Wait for each
version to finish starting and become healthy before moving to the next. For
the resilience cluster, roll each version through the nodes one at a time and
wait for the cluster to recover before advancing. Follow Qdrant's
[upgrade instructions](https://qdrant.tech/documentation/upgrades/) and
[snapshot guidance](https://qdrant.tech/documentation/snapshots/).
Qdrant storage migrations are not reversible; retain the pre-upgrade backup.

Before upgrading an authenticated cluster to 1.20.0, check its internal
authentication configuration. Qdrant now enables `service.enforce_internal_auth`
by default when an API key is configured. A cluster configured with only
`read_only_api_key` needs an `api_key` for peer communication, or an explicit
decision to disable internal authentication. A cluster using only `alt_api_key`
on 1.19.0 or 1.19.1 must reach 1.19.2 first. Tools calling the internal port
must send an `api-key` header. See Qdrant's
[1.20.0 release notes](https://github.com/qdrant/qdrant/releases/tag/v1.20.0)
and [upgrade instructions](https://qdrant.tech/documentation/upgrades/).
