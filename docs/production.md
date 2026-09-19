# Production deployment guide

The gateway is designed to be a stateless HTTP tier for document and search state held in Qdrant. It does not provide authentication, authorization, TLS termination, or Elasticsearch cluster semantics; place it behind an ingress/API gateway and restrict network access to the Qdrant endpoint.

## Container

Images are published to `ghcr.io/dstockton/qdrant-es-gateway` for version tags. The image runs as UID 10001, has no Linux capabilities, and uses a read-only root filesystem in the Helm chart. Persist `/data` because SQLite stores mappings and aliases there.

Use immutable version tags or image digests in production:

```bash
docker pull ghcr.io/dstockton/qdrant-es-gateway:0.1.0
```

Every release workflow produces a multi-architecture image, BuildKit provenance, an SPDX SBOM, a cosign SBOM attestation, and Trivy vulnerability results. The release job fails on known fixed HIGH or CRITICAL image vulnerabilities; unfixed findings are retained in the scan results for review.

After the first release, open the repository's Packages settings and change the GHCR package visibility to Public if GitHub created it privately. Link the package to this repository and enable Dependabot alerts, security updates, and secret scanning in repository Security settings.

## Kubernetes

The chart is in `deploy/helm/qdrant-es-gateway`. Qdrant is intentionally external so its clustering, replication, backups, and storage lifecycle can be managed independently.

```bash
helm upgrade --install search-gateway deploy/helm/qdrant-es-gateway \
  --set image.tag=0.1.0 \
  --set qdrant.url=https://qdrant.example.internal:6334 \
  --set qdrant.existingSecret=qdrant-credentials
```

Before exposing the service:

1. Configure ingress TLS and client authentication.
2. Create a Kubernetes NetworkPolicy allowing egress only to Qdrant and DNS.
3. Set resource requests and limits from workload measurements.
4. Use a StorageClass/PVC with appropriate SQLite locking semantics. For multiple gateway replicas, use shared metadata storage or move the small metadata catalog to a replicated control-plane store before relying on concurrent writers.
5. Configure Qdrant replication, snapshots, monitoring, and disk alerts independently.
6. Enable `DOCUMENT_PROJECTION=true` and `ASYNC_SEARCH_PROJECTION=true` only with a durable reconciliation process for projection lag or failed asynchronous operations.

## Operational checks

- `/healthz` is a liveness check.
- `/readyz` checks Qdrant connectivity and should gate traffic.
- `GET /` exposes the compatibility/version response.
- Watch Qdrant segment growth, optimizer backlog, disk utilization, search latency, and projection lag.
- Keep request and bulk limits bounded; request bodies are buffered in memory, and each concurrent `POST /_bulk` or `POST /<index>/_bulk` may consume up to `MAX_BULK_BYTES` before parsing. Do not expose the service directly to the public internet.
- Test the actual mappings and unsupported-query behavior before migration.

## Release process

1. Merge a tested change to `main`.
2. Create an annotated semantic-version tag, for example `git tag -a v0.1.0 -m 'v0.1.0'`.
3. Push the tag: `git push origin v0.1.0`.
4. Review the GitHub Actions image scan and SBOM attestation.
5. Promote the immutable image digest through environments.

The repository's CI does not publish private voice, video-generation, or local development artifacts.
