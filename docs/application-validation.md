# Real-application validation targets

The validation strategy is endpoint-only: run one application workflow twice and change only its Elasticsearch-compatible base URL. This proves both that the application needs no gateway-specific client code and that returned documents are usable by the application.

## First target: Spinscale catalogue search

[Spinscale's ecommerce search app](https://github.com/spinscale/elasticsearch-ecommerce-search-app) is the best first published application target. It exercises index mappings, bulk ingestion, full-text product search, filters, sorting, pagination, facets, and source-bearing hits. Its original build is tied to an older Java/Gradle/Micronaut stack, so the practical validation path is to preserve its request shapes and fixture data in a modern, pinned replay harness before attempting a full application upgrade.

The current gateway can validate the basic catalogue slice directly. The higher-value gaps to close before claiming an unchanged full application are custom analyzers/synonyms, fuzzy matching, `post_filter`, metric and filter aggregations, and the complete Elasticsearch bulk response contract. These are tracked as compatibility work rather than silently approximated.

## Second target: AWS Retail Demo Store search

[AWS Retail Demo Store](https://github.com/aws-samples/retail-demo-store) has a useful isolated search service, although the full storefront is AWS-heavy and uses OpenSearch. Its search API returns product IDs and then retrieves product details separately. The request shapes include typeahead `match_bool_prefix`, `dis_max`, category diversity through `collapse`/`inner_hits`, and “similar products” via `more_like_this`. Those make it a valuable second compatibility target, but they are distinct gateway features and should be implemented or explicitly fixture-adapted before a no-code application claim.

The smallest credible sequence is:

1. run the repository's `validation/replay.py` against native Elasticsearch and the gateway;
2. add a pinned Spinscale request fixture and compare its top-k IDs, sources, filters, facets, and pagination;
3. run the AWS search container locally and capture its real HTTP requests;
4. replay those requests against both endpoints, measuring result equivalence separately from ranking quality.

Do not compare raw `_score` values. Compare exact sources for the same IDs where ordering is not the product contract, and use top-k overlap, nDCG, zero-result rate, facet equality, and pagination continuity for ranked results.

## Performance protocol

Use identical payloads, IDs, refresh policy, warm-up, concurrency, and client connection settings for both endpoints. Record bulk time, update latency, search p50/p95/p99, mixed-workload throughput, error rate, CPU, memory, disk, and freshness. Run the lightweight fixture first, then 50,000 and 500,000 document corpora. Keep compatibility assertions and performance measurements as separate report sections so a faster but semantically different result cannot be mistaken for a successful application validation.
