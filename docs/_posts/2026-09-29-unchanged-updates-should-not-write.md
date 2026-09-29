---
layout: post
title: "Unchanged updates should not write"
date: 2026-09-29
---

Elasticsearch's Update API detects no-op updates by default. The gateway already had to read the stored source to merge a partial `doc`, but it wrote the merged source back to Qdrant even when no value changed. Repeated idempotent updates therefore consumed write capacity and reported `result: "updated"` instead of `result: "noop"`.

The update path now compares the recursively merged source with the stored source. If they are equal, it returns a no-op response and skips the Qdrant write. Callers that intentionally want a rewrite can send `detect_noop: false`.

A mock-Qdrant regression test covers both embedded-source and dedicated document-projection modes. For the same unchanged update, the measured upstream write count is zero with the default and one with no-op detection disabled. This is a deterministic request-count test, not a throughput benchmark; actual latency and capacity gains depend on the workload's share of unchanged updates.

The comparison covers JSON value equality after the gateway's recursive object merge. Updates remain read-merge-write operations rather than atomic compare-and-write transactions across gateway replicas.
