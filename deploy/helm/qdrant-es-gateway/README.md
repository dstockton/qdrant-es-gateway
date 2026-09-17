# qdrant-es-gateway Helm chart

The chart deploys the gateway tier. Qdrant is an external dependency and is not bundled; set `qdrant.url` to the Qdrant cluster endpoint and provide an existing Kubernetes Secret for `QDRANT_API_KEY` in production.

```bash
helm upgrade --install search-gateway ./deploy/helm/qdrant-es-gateway \
  --set qdrant.url=https://qdrant.example.com:6333 \
  --set qdrant.existingSecret=qdrant-credentials
```

The SQLite metadata database is persisted on a PVC. For multiple replicas, use storage with the access-mode and locking guarantees required by your platform, or follow the metadata-sharing guidance in `docs/production.md`.
