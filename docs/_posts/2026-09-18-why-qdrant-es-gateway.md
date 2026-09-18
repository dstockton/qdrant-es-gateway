---
layout: post
title: "Keep the Elasticsearch client, change the engine"
date: 2026-09-18
categories: [architecture]
---

Changing search engines can mean changing application code that already works. In a product catalogue, documentation site, jobs board, or support portal, the application may already know how to create an index, bulk documents, search with filters, and render `_source` through an Elasticsearch client.

Qdrant ES Gateway takes a narrower route. The client keeps talking to an Elasticsearch-shaped HTTP endpoint. The gateway translates the application-search subset into Qdrant operations, preserves document IDs and sources, and makes unsupported semantics explicit instead of silently pretending that two different ranking engines are identical.

The gateway targets ordinary application search: index lifecycle, bulk ingestion, lexical queries, filters, source projection, updates, and deletes, with limited support for facets and sorting. Terms facets require indexed keyword fields, sorting has basic response-level semantics, and pagination is bounded. Lucene-specific analyzer settings, scripts, scroll, and arbitrary aggregations are outside the supported surface; Kibana and logging are outside the project's intended scope.

For applications whose requests fit the supported subset, the client-side migration can be small. Check the [compatibility guide](../compatibility.md), point the existing Elasticsearch client at the gateway, replay the requests, and compare returned IDs and sources. Then measure the workload that matters: ingestion, reads, mixed traffic, memory, CPU, and disk. Search scores are engine-specific, and accepted query shapes can still differ in behavior: `match_phrase`, for example, does not guarantee Lucene-style positional phrase matching. Evaluate the documents returned, facet behavior, pagination, and product-level relevance metrics.

The compatibility boundary makes the migration decision concrete: which existing requests can the application retain, and do Qdrant's retrieval semantics fit the workload? Start with the [compatibility matrix]({{ "/compatibility.html" | relative_url }}) and treat its ✅ and ⚠️ entries as a checklist for the application's actual requests.
