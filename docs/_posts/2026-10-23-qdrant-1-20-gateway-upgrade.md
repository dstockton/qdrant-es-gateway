---
layout: post
title: "Qdrant 1.20.0 through the gateway: almost identical read performance and an auth upgrade check"
date: 2026-10-23
categories: [qdrant, performance, migration]
---

Qdrant [released 1.20.0](https://github.com/qdrant/qdrant/releases/tag/v1.20.0) on October 22. The proposed gateway stack update advances its development, example, test, and benchmark Qdrant images from 1.19.2 to 1.20.0. This article describes a candidate change under review; deployment stacks gain the new pin when the pull request is merged.

The release adds Qdrant-side capabilities including BM25 scoring for full-text payload indexes, TurboQuant variants, per-element payload indexes, and a code tokenizer, as well as fixes and internal changes. These are upstream Qdrant features; this gateway benchmark exercises its existing Elasticsearch-compatible match search and document GET routes, and does not establish that every new Qdrant feature is exposed through the gateway.

The proposed gateway lockfile also patches `rustls-webpki` from 0.103.15 to 0.103.16 and Tokio from 1.54.0 to 1.54.1, addressing [RUSTSEC-2026-0382](https://rustsec.org/advisories/RUSTSEC-2026-0382.html) and [RUSTSEC-2026-0384](https://rustsec.org/advisories/RUSTSEC-2026-0384.html). The measurements below use that patched gateway build.

## Paired gateway result

For the tested warm read workload, **1.20.0 performed almost identically to 1.19.2**. Two A-B-B-A passes produced four rounds per version. Each round built a fresh 10,000-document index through the same gateway image, then sent 5,000 match searches and 5,000 document GETs through its HTTP API with eight workers, after 40 warmups per operation. Latency measures the complete HTTP round trip through response reading; throughput also includes client JSON decoding and correctness checks. All rounds returned the expected 10 search hits, document source fields, and exact count. Qdrant had a 1.5 CPU and 2 GiB cap; the gateway had a 0.5 CPU and 512 MiB cap. Containers used separate temporary storage. No pre-existing volume was opened.

| Gateway operation; median of four round metrics | Qdrant 1.19.2 | Qdrant 1.20.0 |
| --- | ---: | ---: |
| Match search p50 | 1.26 ms | 1.25 ms |
| Match search p95 | 1.99 ms | 1.90 ms |
| Match search throughput | 4,714 req/s | 4,744 req/s |
| Document GET p50 | 0.79 ms | 0.80 ms |
| Document GET p95 | 1.12 ms | 1.14 ms |
| Document GET throughput | 8,418 req/s | 8,209 req/s |

Match search p95 was 4.6% lower on 1.20.0 in this run. Search throughput differed by about +0.6%, while GET throughput was about -2.5%; GET p95 was 1.9% higher. These small mixed changes support the overall “almost identical” description, with a measured improvement in this search tail latency rather than a broad speedup claim. This is a small, warm, single-node synthetic corpus on one Docker Desktop host, not a large-scale relevance, indexing, authenticated-cluster, or production latency study. The [benchmark script](https://github.com/dstockton/qdrant-es-gateway/blob/main/benchmark/qdrant-version-compare.py) and [first](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/qdrant-1-20-gateway-2026-10-23.json) and [second](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/qdrant-1-20-gateway-repeat-2026-10-23.json) per-round results and latency histograms allow a rerun and inspection once this change is merged.

To reproduce on a machine with Docker and Python 3, build the gateway once and run the isolated comparison twice:

```sh
docker pull qdrant/qdrant:v1.19.2
docker pull qdrant/qdrant:v1.20.0
docker build --pull -t qeg-gateway:qdrant-1-20-compare .
OUTPUT=first.json python3 benchmark/qdrant-version-compare.py
OUTPUT=second.json python3 benchmark/qdrant-version-compare.py
python3 benchmark/qdrant-upgrade-smoke.py
```

The scripts create uniquely named temporary containers and storage and do not attach existing Docker volumes. A separate disposable 1.19.2-to-1.20.0 storage test retained a point and its payload after restart; its exact count remained one. That smoke test establishes only this small upgrade path, not recovery or rolling-cluster behavior.

## Upgrade existing storage and authenticated clusters

Qdrant requires the latest patch of **each consecutive minor release** when opening an existing data directory. Back up first. For a volume last opened by 1.15.x, the path documented in the [migration guide]({{ '/migration/' | relative_url }}) is 1.15.5 → 1.16.3 → 1.17.1 → 1.18.3 → 1.19.2 → 1.20.0, waiting for each version to become healthy. Upgrade replica clusters one node at a time and wait for recovery at each step. Follow Qdrant's [upgrade instructions](https://qdrant.tech/documentation/upgrades/) and retain the pre-upgrade backup because storage migrations are not reversible.

Version 1.20.0 also enables `service.enforce_internal_auth` by default when an API key is configured. Clusters configured with only `read_only_api_key` need an `api_key` for peer traffic, or an explicit decision to disable internal authentication. External tools using the internal port need an `api-key` header. Qdrant also calls out clusters configured only with `alt_api_key` on 1.19.0/1.19.1: they must reach 1.19.2 before 1.20.0. Check the [upstream release notes](https://github.com/qdrant/qdrant/releases/tag/v1.20.0) and [upgrade guide](https://qdrant.tech/documentation/upgrades/) against your deployment before switching images.
