---
layout: post
title: "Exists means not empty"
date: 2026-10-03
categories: [compatibility, correctness]
---

The gateway translated an Elasticsearch `exists` query directly into Qdrant's `is_empty` condition. That is the opposite predicate: documents with a missing, `null`, or empty-array value were selected, while documents with a real value were excluded.

The translation now negates `is_empty`. The same definition is used when the gateway evaluates a `post_filter` against hydrated `_source`: missing values, `null`, `[]`, and `[null]` do not exist; `""` and `[null, "Acme"]` do.

An isolated check against the repository's pinned Qdrant 1.15.3 image inserted those five boundary cases. The emitted filter returned only point 4 (the empty string) and point 5 (the array containing `"Acme"`):

```json
{"filter":{"must":[{"must_not":[{"is_empty":{"key":"brand"}}]}]}}
```

The regression checks are reproducible without a running Qdrant instance:

```bash
cargo test exists_query_excludes_empty_qdrant_payloads
cargo test post_filter_exists_rejects_missing_null_and_empty_arrays
```

This is not a claim of complete Elasticsearch field-existence parity. Server-side filtering applies to mapped fields mirrored into the searchable Qdrant payload, and the gateway does not emulate mapping-dependent cases such as `index: false`, `ignore_above`, or malformed values ignored during indexing.
