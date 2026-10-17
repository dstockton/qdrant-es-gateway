---
layout: post
title: "AWS Retail Demo Store: compatible search requests, different trade-offs"
date: 2026-10-17
categories: [compatibility, opensearch, benchmarking]
---

The [AWS Retail Demo Store](https://github.com/aws-samples/retail-demo-store) is a useful reality check because its search service talks to OpenSearch through the ordinary `opensearch-py` API. Its product search uses `dis_max`, `match_bool_prefix`, category collapse with named `inner_hits`, and `_source: false`. Its similar-products route uses `more_like_this`.

I replayed those request bodies unchanged against native OpenSearch 3.9.0 and this gateway backed by Qdrant 1.19.2. The replay created a settings-only `products` index and wrote individual products, just as the demo's local indexing script does. Both endpoints returned the fields the application consumes, including product IDs, named category hits, and similar-product results that exclude the source product.

## What passed

| Check | OpenSearch | Gateway + Qdrant |
| --- | ---: | ---: |
| Index, search, and similar-product HTTP contract | Pass | Pass |
| Repeated search p50 at 1K | 5.75 ms | 1.37 ms |
| Repeated search p95 at 1K | 7.10 ms | 2.36 ms |
| Repeated search p50 at 1M | 30.08 ms | 1.63 ms |
| Repeated search p95 at 1M | 33.62 ms | 2.85 ms |
| First similar-product request at 1M | 135.15 ms | 33.04 ms |

The gateway also needed two small compatibility fixes exposed by this replay: settings-only indexes now route dynamically mapped text fields through the catch-all lexical vector, and a missing Qdrant point is treated like a missing Elasticsearch document. The latter matters when `more_like_this` looks up its source document.

## The resource trade-off

These are fresh local Docker volumes, after loading unique generated product names and descriptions and while the indexes were retained. They are footprint snapshots, not a production sizing or cloud-billing benchmark.

| Corpus | Service | CPU snapshot | Memory snapshot | Data volume |
| --- | --- | ---: | ---: | ---: |
| 1K | OpenSearch 3.9.0 | 3.83% | 1.090 GiB | 630 KiB |
| 1K | Gateway + Qdrant 1.19.2 | 1.31% combined | 310 MiB combined | 419 MB combined |
| 1M | OpenSearch 3.9.0 | 0.92% | 1.241 GiB | 75.2 MB |
| 1M | Gateway + Qdrant 1.19.2 | 0.08% combined | 581 MiB combined | 876 MB combined |

The negative trade-off is real: Qdrant used more disk. At 1M documents, the gateway-plus-Qdrant volume was about 11.6× the OpenSearch volume. The gateway process itself remained small; Qdrant carries the searchable representation and sparse index. OpenSearch's JVM used more memory in these snapshots, but CPU and memory readings are instantaneous and environment-dependent.

The disk did not grow linearly in this experiment. Moving from 1K to 1M documents increased the OpenSearch volume about 119× and the Qdrant volume about 2.1×. That is evidence of substantial fixed index overhead at the 1K point, not a promise that Qdrant storage is always sublinear. The generator uses five product families with unique names and descriptions; a production catalogue with more varied fields, longer text, replicas, retention, or different sparse-index settings can have a different slope.

I also retried the same 10M-document bulk run after a complete Docker cleanup,
with about 27 GiB free. Qdrant reached roughly 9.57M documents, but the
process was killed for exceeding available memory before the final search could
complete. That is a capacity limit of this test host, not a failed
compatibility assertion, so there is no honest 10M performance number to
publish from this machine. A completed 10M comparison needs more memory.

## What did not become identical

Passing the application contract does not make the two engines rank identically. The native result produced two category groups for the fixture; the gateway produced one bounded candidate group. Scores and similar-product ordering also differ because Qdrant's sparse retrieval is not Lucene/OpenSearch scoring. Refresh timing and the gateway's bounded candidate window remain explicit trade-offs.

So the honest conclusion is narrow: the Retail Demo Store's product-search and similar-product request shapes are a good fit for the gateway, with no application-side URL or response-handling changes in this replay. If exact OpenSearch ranking or category diversity is part of the product contract, validate that separately before migrating.

Reproduce the check with:

```sh
python3 validation/retail_demo.py \
  --native http://localhost:19311 \
  --gateway http://localhost:19313 \
  --docs 1000000 \
  --workers 32 \
  --output validation/evidence/retail-demo-2026-10-17-1m-clean.json
```

The request shapes come from the demo's [search service](https://github.com/aws-samples/retail-demo-store/blob/master/src/search/src/search-service/app.py), and its settings-only indexing flow is shown in [`local_index_products.py`](https://github.com/aws-samples/retail-demo-store/blob/master/src/search/local_index_products.py). The clean 1K and 1M reports are [`retail-demo-2026-10-17-1k-clean.json`](../../validation/evidence/retail-demo-2026-10-17-1k-clean.json) and [`retail-demo-2026-10-17-1m-clean.json`](../../validation/evidence/retail-demo-2026-10-17-1m-clean.json).
