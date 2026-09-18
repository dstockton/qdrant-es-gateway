# External corpus results

This is a reproducible snapshot, not a capacity promise. It records the first external-corpus comparison so later gateway changes can be checked against a real catalogue rather than only the synthetic fixture.

## H&M product catalogue

Source: [Qdrant's `hm_ecommerce_products` dataset](https://huggingface.co/datasets/Qdrant/hm_ecommerce_products), 105,126 product records in the downloaded parquet snapshot, published under CC BY 4.0. The repository stores only the importer and benchmark instructions; the parquet and generated JSONL remain local and are not redistributed here.

The benchmark ran native Elasticsearch 8.15.0 and the Rust gateway backed by the Qdrant engine 1.15.3 in Docker Desktop, with the repository's approximate 1.5 CPU / 8 GB Elasticsearch, 1 CPU / 4 GB Qdrant, 1 CPU / 2 GB gateway and 0.5 CPU / 1 GB runner limits. It used identical JSONL batches and client code; only the ES-compatible endpoint changed. The query was a title match for `dress`, with 1,000 updates and 500 mixed requests at concurrency 10.

| Profile | Native Elasticsearch | Qdrant gateway | Gateway relative result |
|---|---:|---:|---:|
| Bulk ingest, `refresh=` disabled | 5.69 s | 10.14 s | 1.78x slower |
| Bulk ingest, `refresh=wait_for` per batch | 215.45 s (two-repeat median) | 2.90 s | Refresh semantics dominate this comparison |
| Search p95, concurrency 1 | 17.99 ms | 0.99 ms | 18.2x lower latency |
| Search p95, concurrency 10 | 82.40 ms | 3.54 ms | 23.3x lower latency |
| Mixed throughput, 500 requests | 740 req/s | 1,255 req/s | 1.70x higher throughput |
| 1,000 updates | 3.14 s | 1.82 s | 1.72x faster |

The refresh-wait profile is useful because it reflects an application that explicitly requests immediate visibility after every bulk batch. It must not be described as raw Elasticsearch indexing capacity: the no-refresh run shows native Elasticsearch completing the same corpus faster than the gateway. Conversely, the gateway's bulk path does not reproduce Elasticsearch refresh scheduling because Qdrant remains continuously queryable.

## Result quality

Both engines returned ten source-bearing hits for the same query. The top-ten IDs did not match exactly. That is expected from the current gateway's Qdrant sparse ranking versus Elasticsearch/Lucene analysis and scoring, but it is a material compatibility result: the gateway is currently a strong transport and performance candidate for application-search workloads, not a claim of 100% relevance equivalence. Future reports should add recall@k / overlap, category coverage and an application-specific judged relevance set.

The run also captured peak container memory: Elasticsearch 1.45 GiB, Qdrant 411 MiB and gateway 13 MiB at the sampling point. Disk measurements were unavailable from the host-side collector on Docker Desktop because its Linux VM volume paths are not exposed; run the same profile on Linux or collect the mounted paths from inside the containers before publishing storage comparisons.

## Reproduce

See [`benchmark/README.md`](../benchmark/README.md) for the download, attribution, conversion and benchmark commands. Run both profiles when comparing a client that requests immediate visibility and a client that allows refresh to be decoupled from bulk ingestion.
