---
layout: post
title: "Missing alias removals are not success"
date: 2026-10-15
categories: [production, compatibility, elasticsearch, opensearch]
---

The gateway used to acknowledge every syntactically valid alias removal, even when the alias did not exist or belonged to another index. That made a stale deployment or rollover request look successful while leaving the catalogue unchanged. The risk is operational rather than cosmetic: a caller can continue believing that an old read or write route was detached.

Separate clean-container probes exercised Elasticsearch 9.5.5 and OpenSearch 3.9.0 with the same requests. Both returned HTTP 404 with `aliases_not_found_exception` for a single missing alias and for an alias attached to a different index. Both also applied the successful add in a mixed missing-remove plus add batch and returned HTTP 200. Their response bodies diverged: Elasticsearch reported `errors: true` and one result per action, while OpenSearch returned only `{"acknowledged":true}`. The sanitized [request and response evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/elasticsearch-9.5.5-opensearch-3.9-alias-remove-2026-10-15.json) records the versions and observed shapes.

The gateway now tracks whether any action in its supported single-target subset can succeed before opening the SQLite transaction. When every removal misses, it returns the shared native 404 shape and leaves the catalogue unchanged. A mixed batch still commits its successful action and returns 200. Exact idempotent adds count as successful, matching native behavior.

```console
cargo test alias_removal_reports_when_no_action_can_succeed
```

This is deliberately not a claim of identical partial-failure responses. The gateway retains the simpler OpenSearch-shaped success body for mixed batches and does not yet emit Elasticsearch per-action alias results. It also continues to reject `must_exist`, alias arrays and patterns, filters, routing, write-index flags, and multi-index final states rather than approximating them. The probe measures request semantics only; it is not a latency or resource benchmark.
