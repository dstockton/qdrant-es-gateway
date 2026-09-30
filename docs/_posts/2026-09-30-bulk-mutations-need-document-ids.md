---
layout: post
title: "Bulk mutations need document IDs"
date: 2026-09-30
---

Elasticsearch requires bulk `update` and `delete` actions to identify the document they mutate. The gateway previously generated a random ID whenever `_id` was absent. That was particularly misleading for deletes: a malformed request could report `not_found` for a generated ID even though the caller never selected a document.

The bulk parser now returns a structured HTTP 400 before executing any item when an `update` or `delete` action omits `_id`. This preflight behavior also prevents earlier valid items in the same request from being applied before the malformed action is discovered. `index` and `create` retain their existing generated-ID behavior.

The same change accepts both `POST` and `PUT` for root and index-scoped `_bulk` routes, matching the documented Elasticsearch API surface. Regression tests exercise both missing-ID action types and verify that PUT requests receive the dedicated bulk body limit.

This does not add optimistic concurrency control: bulk sequence-number, primary-term, and version semantics remain outside the current compatibility subset.
