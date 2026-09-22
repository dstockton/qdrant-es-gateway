---
layout: post
title: "No phantom indices after creation failures"
date: 2026-09-22
categories: [production, reliability]
---

Creating an index spans two stores: Qdrant owns the collections and payload indexes, while SQLite keeps the gateway's mappings and aliases. That boundary needs a deliberate publication order.

Previously, the gateway wrote SQLite metadata before asking Qdrant to create anything. If Qdrant was unavailable, rejected the collection configuration, or failed while creating the optional document projection, the request returned an error but the gateway still considered the index present. Later index checks succeeded against metadata and document operations failed against a collection that did not exist.

Index creation now treats SQLite as the publication step. The gateway creates the sparse collection, optional document collection, and payload indexes first. Only after those operations succeed does it record the index metadata. If a later setup step or the metadata write fails, it makes a best-effort attempt to delete the collections created by that request. Cleanup failures are logged without hiding the original creation error.

A regression test runs against an in-process mock Qdrant. It accepts the primary collection, fails the document collection, and verifies both observable properties: the index is absent from SQLite and the primary collection receives a cleanup request.

To reproduce the verification:

```bash
cargo test failed_index_creation_is_not_published_and_cleans_up
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

This is compensating cleanup rather than a distributed transaction. A process crash between Qdrant creation and SQLite publication can still leave an orphaned collection, and a Qdrant outage can prevent cleanup. Operators should still monitor unexpected collections and retain reconciliation or backup procedures for the metadata database. The change prevents failed requests from publishing unusable gateway metadata and preserves the original metadata when an attempt to recreate an existing index is rejected upstream.
