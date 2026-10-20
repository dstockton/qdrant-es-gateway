---
layout: post
title: "Script fields cannot be empty promises"
date: 2026-10-20
categories: [production, compatibility, elasticsearch, opensearch]
---

The gateway accepted search bodies containing `script_fields`, did not execute the scripts, and returned ordinary hits without the requested `fields` object. That is a false success for clients that calculate display values or other per-hit data at search time.

A focused probe used the same one-document mapping, script, and request against separate Elasticsearch 8.15.0 and OpenSearch 3.9.0 containers. With `_source: false`, both engines evaluated `doc["price"].value + params.increment` and returned `fields.next_price: [43]`. Both omitted `fields` when `script_fields` was an empty object. The engines document this behavior independently in the [Elasticsearch Search API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html) and [OpenSearch script-field guide](https://docs.opensearch.org/latest/scripting/accessing-fields/#search-and-aggregation-scripts).

The sanitized [request and response evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/elasticsearch-8.15-opensearch-3.9-script-fields-2026-10-20.json) records the images, versions, fixture, selected hit fields, reproduction sequence, and limitations. This establishes request/response behavior for the two recorded versions; it is not a script-performance or sandbox-security benchmark.

The gateway now rejects every non-empty `script_fields` object before index lookup or Qdrant access, including in each `_msearch` subrequest. An empty object remains an accepted no-op, and malformed non-object values fail validation. Searches that omit `script_fields` are unchanged.

```console
cargo test invalid_search_options_fail_before_qdrant_work
```

Native support would require a deliberate scripting language, execution limits, field-access semantics, error contract, and security model. Copying a guessed value from `_source` would not execute the requested program and would be less honest than a structured compatibility error.
