# Requested doc values cannot silently disappear

The gateway accepted search bodies containing `docvalue_fields` but silently returned hits without the requested `fields` object. This can break clients that deliberately read keyword, numeric, or date values from doc values instead of `_source`; preserving `_source` does not satisfy that response contract.

A focused probe used the same one-document mapping and requests in separate Elasticsearch 8.15.0 and OpenSearch 3.9.0 containers. Both returned `fields.price: [42]` and `fields.sku: ["A-1"]` for body `docvalue_fields: ["price", "sku"]` and for the URL form `docvalue_fields=price,sku`. Both omitted `fields` for an empty body array. The [Elasticsearch field retrieval reference](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/search-fields.html#docvalue-fields) and [OpenSearch Search API](https://docs.opensearch.org/latest/api-reference/search-apis/search/) describe doc-value retrieval independently.

The sanitized [request and response evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/elasticsearch-8.15-opensearch-3.9-docvalue-fields-2026-10-19.json) records the images, versions, fixture, selected hit fields, reproduction sequence, and limitations. This establishes request/response behavior for the two recorded versions; it is not a doc-value storage or latency benchmark.

The gateway now rejects a non-empty body `docvalue_fields` array or the corresponding URL parameter before index lookup or Qdrant access, including in each `_msearch` subrequest. An empty body array remains an accepted no-op, and malformed non-array body values fail validation. Ordinary `_source` responses are unchanged.

```console
cargo test invalid_search_options_fail_before_qdrant_work
```

Native support would require a defined mapping and formatting contract for every returned field, including multivalued fields and date/numeric formats. Copying arbitrary `_source` values into `fields` would falsely label source data as doc values and would miss those semantics.
