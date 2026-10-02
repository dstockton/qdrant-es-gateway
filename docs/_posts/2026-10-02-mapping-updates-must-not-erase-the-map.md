---
layout: post
title: "Mapping updates must not erase the map"
date: 2026-10-02
categories: [compatibility, production]
---

Elasticsearch sends a mapping update as `{"properties": ...}`, while index creation nests the same object under `mappings`. The gateway reused the index-creation parser for both shapes. A normal `PUT /products/_mapping` therefore interpreted the update as an empty mapping and replaced the stored definition with `{}`, silently losing the fields used to build search vectors and filter payloads for later writes.

Mapping updates now merge properties under the same index-administration lock used by other catalogue changes. Supported keyword, boolean, integer, float, and date additions also create the corresponding Qdrant payload index before the merged mapping is published in SQLite. Repeating an identical field definition is a no-op; changing an existing definition returns a structured 400 instead of rewriting search behavior.

The regression starts with a `title` text mapping, adds a `brand` keyword property using the Elasticsearch request shape, and verifies all three boundaries: `title` remains stored, `brand` gets a Qdrant keyword index, and the existing sparse-vector list is unchanged.

```bash
cargo test mapping_updates_merge_properties_and_create_payload_indexes
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

An isolated check against the repository's pinned Qdrant 1.15.3 image confirmed that the emitted payload-index request creates `payload_schema.brand` as `keyword`. This does not make every Elasticsearch mapping mutation available. In particular, new text fields are rejected because that Qdrant version cannot add the required named sparse vector to an existing collection; create a new index and reindex when the searchable text schema changes. If a multi-field payload-index update fails partway through, Qdrant may retain an unused index, but the gateway does not publish the partial mapping.
