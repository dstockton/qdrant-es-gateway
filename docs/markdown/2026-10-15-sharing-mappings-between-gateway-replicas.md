# Two gateways need one view of their mappings

Adding a second gateway sounds straightforward: start another process and send it traffic. But if each process has its own SQLite file, an alias created through one process is invisible to the other. A restart-safe local database does not automatically become shared metadata.

Qdrant ES Gateway now has an optional Redis/Valkey catalog for that deployment. Mappings, vector names and aliases live in one authoritative store. Each request reads its current committed catalog, so the next request can land on either replica without relying on a local metadata cache. Qdrant continues to store the documents and perform searches.

SQLite remains the default. We have not measured a SQLite bottleneck, and there is no reason to add another service to a single-gateway setup just to replace a small local catalog.

## What changes for an operator

For a new empty deployment, configure `CATALOG_REDIS_URL`, initialize the catalog once with `CATALOG_INITIALIZE=true`, then remove that initialization flag and start the other replicas. Use a persistent, non-evicting Redis/Valkey deployment. Existing SQLite installations need a deliberate migration; simply pointing them at an empty catalog will not discover their mappings.

Administrative changes have one owner at a time. After a successful change, the gateway publishes the metadata and releases ownership atomically within Redis/Valkey. If a gateway crashes while changing a Qdrant collection, it leaves its administrative claim in place. New requests fail closed until an operator reconciles the two stores.

That recovery step is intentional. A lock that expires after thirty seconds does not prove a slow Qdrant request stopped executing. Letting another gateway take over at that point could make the catalog less trustworthy.

## What this does—and what it costs

The shared catalog solves visibility and administrative ownership. It does not turn Qdrant and Redis/Valkey into one transaction, or make concurrent document updates atomic. Nor is this a performance claim: every request fetches a bounded metadata snapshot and builds an ephemeral SQLite view. Large catalogs and high request rates need measurement before adopting this approach.

The real-server test uses two independent clients to verify that metadata becomes visible after commit, concurrent administrators are refused, and a vanished owner does not silently lose its claim. These are correctness checks, not benchmark results.

For configuration, the test command, durability requirements and the recovery procedure, see [the shared catalog guide](https://dstockton.github.io/qdrant-es-gateway/shared-catalog/). Start with SQLite for one gateway; choose shared metadata when adding replicas creates a real ownership problem.
