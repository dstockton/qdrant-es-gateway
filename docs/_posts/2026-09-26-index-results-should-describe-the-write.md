---
title: "Index results should describe the write"
date: 2026-09-26
categories: [compatibility, correctness]
---

Elasticsearch's index API is an upsert: the same request shape can either create a new document or replace an existing one. That distinction is observable. A create returns HTTP 201 with `result: "created"`; a replacement returns HTTP 200 with `result: "updated"`.

The gateway previously returned HTTP 200 with `result: "created"` for both cases. The write itself succeeded, but clients using the status or result to count inserts, identify replacements, or drive follow-up work received misleading information.

Standalone `PUT /<index>/_doc/<id>` now checks the authoritative document store before writing. The regression test covers missing and existing IDs in both source-storage modes, sends the request through an alias, and verifies the concrete index name, status, result, and Elasticsearch product header.

This accuracy costs one point read per standalone index request. It is also a statement about what existed at preflight time, not an atomic compare-and-write guarantee; a concurrent writer can change the document after that lookup. Contiguous bulk `index` actions deliberately keep their single-upsert batching path and do not yet pay a per-item created-versus-updated preflight; their successful item statuses remain a documented compatibility limitation.
