# Real-application validation targets

The validation strategy is endpoint-only: run one application workflow twice and change only its Elasticsearch-compatible base URL. This proves both that the application needs no gateway-specific client code and that returned documents are usable by the application.

## First target: Spinscale catalogue search

[Spinscale's ecommerce search app](https://github.com/spinscale/elasticsearch-ecommerce-search-app) is the best first published application target. It exercises index mappings, bulk ingestion, full-text product search, filters, sorting, pagination, facets, and source-bearing hits. Its original build is tied to an older Java/Gradle/Micronaut stack, so the practical validation path is to preserve its request shapes and fixture data in a modern, pinned replay harness before attempting a full application upgrade.

The current gateway can validate the basic catalogue slice directly. The remaining gaps before claiming unchanged full-application compatibility are custom analyzers/synonyms, exact fuzzy and prefix ranking, search-scoped aggregation membership, and the complete Elasticsearch bulk response contract. `post_filter`, basic metric/filter aggregations, `dis_max`, fuzzy-shaped requests, and retail-style `more_like_this` plus `collapse`/`inner_hits` response shapes are now accepted, but their semantics remain explicitly approximate where the underlying Qdrant query is not Lucene-equivalent.

## Second target: AWS Retail Demo Store search

[AWS Retail Demo Store](https://github.com/aws-samples/retail-demo-store) has a useful isolated search service, although the full storefront is AWS-heavy and uses OpenSearch. Its search API returns product IDs and then retrieves product details separately. The request shapes include typeahead `match_bool_prefix`, `dis_max`, category diversity through `collapse`/`inner_hits`, and “similar products” via `more_like_this`. The gateway now accepts these shapes, so the next step is live request replay and product-quality comparison; exact prefix ranking, category distribution, and similarity relevance still need validation before a no-code application claim.

The smallest credible sequence is:

1. run the repository's `validation/replay.py` against native Elasticsearch and the gateway;
2. add a pinned Spinscale request fixture and compare its top-k IDs, sources, filters, facets, and pagination;
3. run the AWS search container locally and capture its real HTTP requests;
4. replay those requests against both endpoints, measuring result equivalence separately from ranking quality.

The current request-level audit is maintained in [demo-app-compatibility.md](demo-app-compatibility.md). It separates transport/response compatibility from semantic equivalence so “the application can make the request” is not confused with “the two engines rank and aggregate identically.”

Do not compare raw `_score` values. Compare exact sources for the same IDs where ordering is not the product contract, and use top-k overlap, nDCG, zero-result rate, facet equality, and pagination continuity for ranked results.

## Performance protocol

Use identical payloads, IDs, refresh policy, warm-up, concurrency, and client connection settings for both endpoints. Record bulk time, update latency, search p50/p95/p99, mixed-workload throughput, error rate, CPU, memory, disk, and freshness. Run the lightweight fixture first, then 50,000 and 500,000 document corpora. Keep compatibility assertions and performance measurements as separate report sections so a faster but semantically different result cannot be mistaken for a successful application validation.
