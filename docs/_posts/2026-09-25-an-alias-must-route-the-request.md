---
layout: post
title: "An alias must route the request"
date: 2026-09-25
categories: [compatibility, correctness]
---

The gateway has stored aliases durably from its first release, but persistence alone did not make them functional. Index lookup still derived the Qdrant collection name and deterministic point ID from the name in the request. A request to `current-products/_doc/1` therefore looked in the alias namespace rather than the `products` namespace recorded as its target.

Index lookup now resolves a single-target alias to its concrete index before any collection or point ID is selected. This applies to document CRUD, bulk operations, search, count, mappings, multi-search, and more-like-this document references. Responses identify the concrete index, and `GET /_alias/<name>` is now keyed by that concrete index, matching Elasticsearch's response shape. Index deletion rejects alias names explicitly, and deleting a concrete index removes its aliases from the SQLite catalogue.

The regression test uses a mock Qdrant endpoint and an alias named `current-products`. It verifies that the document read uses `GET /collections/es_products/points/<products-derived-id>`, that match-all search uses `POST /collections/es_products/points/scroll`, and that both hits and alias inspection name `products`. Run it with:

```sh
cargo test aliases_route_document_and_search_requests_to_the_concrete_index
```

This does not make aliases equivalent to Elasticsearch's full alias subsystem. The current schema deliberately permits one concrete target per alias. Filtered aliases, multi-index fan-out, write-index selection, and a Qdrant-native atomic switch are still future work. Alias action batches also remain gateway-local metadata operations rather than a distributed transaction with Qdrant.
