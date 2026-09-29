---
layout: post
title: "Partial updates should not erase sibling fields"
date: 2026-09-29
---

Elasticsearch describes an Update API `doc` as a partial document merged into the stored source. That merge is recursive for inner objects: changing `details.warranty.years` must preserve both `details.manufacturer` and `details.warranty.region`. Arrays and scalar values, by contrast, replace their previous values.

The gateway previously merged only the top level. A patch such as:

```json
{"doc":{"details":{"warranty":{"years":3}}}}
```

replaced the complete `details` object and silently removed its other members. The update path now performs a simple recursive object merge before writing either the embedded source payload or the dedicated document projection. A mock-Qdrant regression test exercises both storage modes and also confirms that an array in the patch still replaces the stored array.

This is a source-integrity correction, not support for Elasticsearch's `nested` field query semantics. The operation still reads and writes the complete source, and the read/merge/write sequence is not an atomic compare-and-write across concurrent gateway replicas. Scripts and optimistic-concurrency controls also remain unsupported.
