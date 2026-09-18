# Demo-application compatibility

This page records request-level compatibility against the two public application targets that motivated the gateway work. “Supported” means that the gateway accepts the request shape and returns the fields the application consumes. It does not mean Lucene-identical ranking, analyzer behavior, or refresh/version semantics.

## AWS Retail Demo Store search service

The isolated search service in [aws-samples/retail-demo-store](https://github.com/aws-samples/retail-demo-store) uses an OpenSearch-compatible client and sends:

| Request feature | Gateway status | Notes |
|---|---:|---|
| `match_bool_prefix` across several fields | ✅ | Accepted through the gateway lexical path; typo/prefix ranking is approximate |
| `dis_max` with boosts and tie breaker | ✅ | Query shape accepted; Qdrant scoring is not Lucene/OpenSearch scoring |
| `collapse` on `category.keyword` | ✅ | Gateway-side grouping after retrieval |
| `inner_hits` with `_source: false` and `_id` consumption | ✅ | Response shape includes grouped hits and totals; source suppression is supported for primary hits |
| `more_like_this` by document ID | ✅ | Expanded into a gateway lexical query using the referenced source |
| `from`/`size` | ✅ | Bounded pagination |

The service's two search routes—product search and similar products—are therefore request-compatible with the gateway's supported subset. The remaining validation work is result-quality comparison on the demo corpus: category diversity, top-k overlap, and similar-product relevance must be measured rather than inferred from HTTP compatibility.

## Spinscale Elasticsearch e-commerce search app

The [spinscale/elasticsearch-ecommerce-search-app](https://github.com/spinscale/elasticsearch-ecommerce-search-app) exercises a broader catalogue workflow:

| Request feature | Gateway status | Notes |
|---|---:|---|
| index existence, create, delete | ✅ | Durable gateway metadata and Qdrant collections |
| close/open/settings bootstrap calls | ⚠️ | Common request shapes are acknowledged; Elasticsearch analyzer/settings effects are not emulated |
| bulk indexing with immediate refresh policy | ✅ | Bulk response and refresh endpoint are supported; Qdrant is continuously available |
| `multi_match` with fuzziness and `minimum_should_match` | ⚠️ | Accepted lexical path; ranking and exact minimum-term semantics are approximate |
| `post_filter` | ⚠️ | Simple term/terms/range/exists/bool shapes are applied to hydrated hits |
| terms, min/max, filters aggregations | ⚠️ | Supported shapes are bounded by the gateway's candidate/facet implementation |
| source-bearing hits, updates, deletes, counts | ✅ | Covered by `validation/replay.py` |

The AWS target is the closest current fit for a no-client-change search path. The Spinscale target is a useful compatibility and relevance test, but it depends more heavily on Elasticsearch-specific analysis and aggregation semantics. The project should not claim 100% semantic replacement for either application until the real application workflows have been replayed and their product-level assertions pass.

## Additional application targets

These are worthwhile next targets because they are well-known open-source applications with documented Elasticsearch integrations. They are intentionally listed as targets, not as compatibility claims:

| Application | Elasticsearch role | Likely gateway fit | What a real replay must prove |
|---|---|---|---|
| [Zammad](https://github.com/zammad/zammad) | Ticket, user, organisation, knowledge-base and global search; Elasticsearch is enabled by default in the documented deployment configuration | Medium for simple text/filter/search flows; low for a drop-in replacement until Zammad's index schema and query builder are replayed | Search result IDs, field projections, pagination, sorting, counts, reindex lifecycle and freshness |
| [GitLab](https://gitlab.com/gitlab-org/gitlab) | Optional advanced/global/code search across projects, work items, merge requests and other content | Low for the complete product; potentially useful for an isolated application-search slice, not GitLab's code-search workload | Exact search modes, permissions, aggregations, aliases, refresh behaviour and the multi-index schema |
| [Adobe Commerce / Magento Open Source](https://github.com/magento/magento2) | Required catalog search engine in current 2.4.x deployments; product search, filters, sorting and catalog indexing | Medium for a bounded catalogue-only test; low for the full adapter without analyzers, facets and Magento-specific mappings | Catalog reindex, searchable attributes, layered navigation facets, price/range filters, sort order, store scope and product visibility |
| [Open edX `edx-search`](https://github.com/openedx/edx-search) | Pluggable search backend; Elasticsearch is a supported legacy backend for LMS search | Medium for document indexing/search APIs; not a claim about the full Open edX platform | Document type filtering, field filters, pagination, exact result objects and update/delete freshness |

The first three applications use Elasticsearch as part of a larger product rather than exposing a simple public search API. That makes them valuable semantic tests, but permissions, analyzers, aliases, aggregations and refresh semantics can dominate the result.

## Public corpus benchmark targets

The reproducible benchmark now accepts an external JSONL corpus and includes an importer for the [Qdrant H&M product catalogue](https://huggingface.co/datasets/Qdrant/hm_ecommerce_products). It contains 105,126 product records in the downloaded snapshot and is published under CC BY 4.0. The corpus is downloaded and generated locally; it is not committed to this repository. The benchmark command and importer are documented in [benchmarking.md](benchmarking.md).

[Open Food Facts](https://huggingface.co/datasets/openfoodfacts/product-database) is a second useful scale target: its food split is millions of products and is available under the Open Database License. It is too large for routine CI and requires attribution/share-alike handling, so it should be run as an explicitly provisioned benchmark rather than bundled into the repository.

## Reproducible validation

Run the deterministic endpoint-only replay against native Elasticsearch/OpenSearch and the gateway:

```sh
python3 validation/replay.py \
  --native http://localhost:19200 \
  --gateway http://localhost:9200 \
  --output validation/results/latest.json
```

The replay now covers lifecycle calls, bulk ingestion, filters, facets, pattern queries, `_msearch`, cursor pagination, reads, updates, deletes, and counts. Compare IDs, sources, facet keys, and pagination behavior; do not compare raw engine scores.
