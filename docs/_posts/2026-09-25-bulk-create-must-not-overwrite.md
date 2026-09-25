---
layout: post
title: "Bulk create must not become upsert"
date: 2026-09-25
categories: [compatibility, correctness]
---

Elasticsearch gives `index` and `create` different meanings inside a bulk request. `index` may replace a document at the same ID, while `create` must fail with an item-level HTTP 409 conflict when that ID already exists.

The gateway previously combined adjacent `index` and `create` actions into the same Qdrant upsert batch. That was efficient, but it erased the distinction: a retry or duplicate `create` could silently replace an existing document while reporting status 201.

Bulk `create` now performs a point lookup before writing. If the document exists, the item returns `version_conflict_engine_exception`, the bulk response sets `errors` to `true`, and no upsert is sent. Ordinary adjacent `index` actions keep their batched write path. A mock-Qdrant regression test exercises both embedded-source and dedicated document-projection storage and verifies that the conflict path issues zero writes:

```shell
cargo test bulk_create_rejects_an_existing_document_without_overwriting_it
```

This correction has a cost: `create` actions now require one lookup and an individual write instead of sharing the `index` batch. Create checks are serialized inside a gateway process, so concurrent creates handled by that process cannot both pass the preflight. The lookup and write are still not one atomic Qdrant operation, however, and independently running gateway replicas could race. The default single-replica deployment avoids that specific race; operators should not treat this as distributed compare-and-insert semantics.
