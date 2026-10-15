---
layout: post
title: "A shared catalog is not a snapshot"
date: 2026-10-15
categories: [architecture]
---

Moving metadata out of a gateway process makes replicas easier to run. It does not make every Elasticsearch consistency promise available.

A `search_after` cursor describes where the next page should start. If documents change between pages, the next request sees those changes. A point-in-time search needs a retained view of the data, or retained results with clearly defined creation semantics. Saving a cursor in Valkey does not create that view.

The same distinction matters for writes. An `if_seq_no` and `if_primary_term` pair asks the engine to reject a write if the document changed. Checking a value in Redis and then writing to Qdrant is two operations across two systems. A failure or competing writer between them can break the promise. A fast key-value store cannot make them one transaction.

The gateway now rejects point-in-time search bodies and consistency-sensitive URL parameters before executing a request. For example, `PUT /products/_doc/1?if_seq_no=0&if_primary_term=1` returns a structured `400` instead of silently performing an unconditional write. Explicit routing, external versions, scroll contexts, and conflict retries are also rejected when supplied as URL parameters. The focused tests check encoded parameter names as well as ordinary requests.

This keeps the boundary visible while shared metadata and stateless query translation improve. Atomic concurrent document updates still need an authoritative conditional-write mechanism and recovery design. Full Lucene phrase semantics need positional indexing. Neither is supplied by adding a metadata cache.

For applications that use only the supported subset, Qdrant can remain the document and retrieval engine while gateway replicas keep no durable local catalog. Applications requiring snapshot pagination or concurrent compare-and-set writes should retain their native engine for those operations until the gateway can demonstrate the same guarantee.
