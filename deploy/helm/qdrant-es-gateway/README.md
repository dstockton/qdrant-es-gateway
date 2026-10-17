# qdrant-es-gateway Helm chart

The chart deploys the gateway tier. Qdrant is an external dependency and is not bundled; set `qdrant.url` to the Qdrant cluster endpoint and provide an existing Kubernetes Secret for `QDRANT_API_KEY` in production.

```bash
helm upgrade --install search-gateway ./deploy/helm/qdrant-es-gateway \
  --set qdrant.url=https://qdrant.example.com:6333 \
  --set qdrant.existingSecret=qdrant-credentials
```

The SQLite metadata database is persisted on a PVC, so the chart deliberately defaults to one replica. The default `ReadWriteOnce` volume is not a horizontally shared metadata service, and a second pod may be unschedulable on another node or may observe a different catalog if each pod gets separate storage.

Do not increase `replicaCount` merely to add HTTP capacity. Multiple replicas require one shared metadata database with SQLite-compatible locking and a `ReadWriteMany` storage implementation, and concurrent index or alias administration still needs external serialization. The current chart does not provision that control plane. Follow the metadata-sharing guidance in `docs/production.md` before overriding the default.

### Shared catalog (optional)

For multiple replicas, configure `catalog.existingSecret` and
`catalog.existingSecretKey` with a secret containing `CATALOG_REDIS_URL`, and use
one `catalog.key` across all replicas. Initialize an empty deployment's catalog
once before rollout; the chart intentionally does not auto-initialize it.
`persistence.enabled=false` is appropriate when metadata is entirely in the
shared catalog. Redis/Valkey must be durable and non-evicting.

See [the shared catalog guide](../../../docs/shared-catalog.md) before enabling
this mode: administrative failures after attempted upstream writes require
reconciliation, and shared metadata does not provide distributed document CAS.
