---
layout: post
title: "Pattern queries without a client rewrite"
date: 2026-09-18
categories: [features]
---

Catalogue search often has two different jobs hiding behind one endpoint. Natural-language search wants token-aware retrieval. Operational search wants to find an SKU prefix, a serial number, or a wildcarded identifier.

Qdrant ES Gateway accepts Elasticsearch-shaped `prefix`, `wildcard`, and `regexp` clauses in positive `bool.must` and `bool.filter` paths. For these supported placements, the client can retain the request shape. The gateway reads candidate documents from Qdrant and evaluates patterns against their fields, combining pattern clauses with ordinary Qdrant-translated filters where supplied. Pattern clauses in either `should` or `must_not` are rejected.

Wildcards support Elasticsearch-style `*` and `?`, while regular expressions use Rust/RE2-style syntax. Invalid regular expressions receive a structured `400` response. The [compatibility guide]({{ "/compatibility/" | relative_url }}) lists the boundary.

Pattern matching is a gateway-side scan. A pattern-only query scans the collection in bounded scroll pages; the page size does not bound the total work. This suits modest catalogues and identifier lookups, but not unbounded, high-cardinality pattern queries. If pattern traffic becomes a primary workload, use a dedicated indexed pattern field or a purpose-built search path.
