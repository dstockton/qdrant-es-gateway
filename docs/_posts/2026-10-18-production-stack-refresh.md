---
layout: post
title: "Refreshing the production stack without opening old volumes"
date: 2026-10-18
categories: [maintenance, production, elasticsearch]
---

The development and benchmark stack now uses Rust 1.99.1, Qdrant 1.19.2, Valkey 9.1.2, HAProxy 3.4.7, Python 3.15.0, and Elasticsearch 9.5.5. The Rust client layer also moves to reqwest 0.13 and redis-rs 1.7. This is a maintenance update: the existing client and demo request shapes are unchanged.

Fresh ephemeral containers provide the evidence for this update. Elasticsearch reported version **9.5.5** and a green single-node cluster; Qdrant reported **1.19.2** and passed the gateway's live create, bulk, search, and delete smoke path; all seven shared-catalog tests passed against Valkey **9.1.2**. The gateway image built from an empty cache, the benchmark image built, all three Compose files parsed, 92 isolated Rust tests and one live integration test passed, and formatting, Clippy, and `cargo audit` were clean. A refreshed Trivy database found no fixed HIGH or CRITICAL vulnerabilities in the gateway image. The [sanitized evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/production-stack-refresh-2026-10-18.json) records the versions, results, and limitations.

The important operational result is a boundary, not a shortcut. Elasticsearch 9.5.5 reports minimum wire compatibility with 8.19.0, so an 8.15 benchmark volume must first follow Elastic's [upgrade preparation](https://www.elastic.co/docs/deploy-manage/upgrade/prepare-to-upgrade) through the latest 8.19 patch. Qdrant documents consecutive minor upgrades, so a 1.15 volume must be backed up and opened by the latest 1.15, 1.16, 1.17, and 1.18 patches before 1.19.2; the [migration guide]({{ '/migration/' | relative_url }}) records that sequence. No existing volume was opened during validation.

This run did **not** change or exercise OpenSearch. The Elasticsearch 9.5.5 result is an Elasticsearch-specific benchmark-environment check, not evidence that OpenSearch behaves the same way. It is also not a compatibility replay or throughput benchmark. Reproduce the bounded startup checks on fresh storage with:

```sh
docker run --rm -d --name qeg-es-check \
  -p 127.0.0.1:19200:9200 \
  -e discovery.type=single-node -e xpack.security.enabled=false \
  docker.elastic.co/elasticsearch/elasticsearch:9.5.5
curl -fsS http://127.0.0.1:19200/
curl -fsS http://127.0.0.1:19200/_cluster/health

docker run --rm -d --name qeg-qdrant-check \
  -p 127.0.0.1:16333:6333 qdrant/qdrant:v1.19.2
curl -fsS http://127.0.0.1:16333/
curl -fsS http://127.0.0.1:16333/readyz
```

Stop those temporary containers after the check; do not attach old data volumes to these commands.
