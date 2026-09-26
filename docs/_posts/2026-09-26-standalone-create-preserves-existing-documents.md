---
layout: post
title: "Standalone create now preserves existing documents"
date: 2026-09-26
---

Elasticsearch's `PUT /<index>/_create/<id>` and `POST /<index>/_create/<id>` endpoints are deliberately stricter than the ordinary index API: they create a missing document but return HTTP 409 rather than replace an existing ID. This matters for importers and job processors that use create-only writes as an idempotency guard.

The gateway already enforced that distinction for bulk `create`, but the standalone endpoint was not routed. Both standalone forms now reuse the same existence check. A new document returns HTTP 201 with `result: "created"`; an existing document returns `version_conflict_engine_exception`, and the regression test verifies that Qdrant receives no write. Both success and error responses include the Elasticsearch product header expected by official clients. The test covers both embedded-source and dedicated document-projection storage:

```sh
cargo test standalone_create
```

This adds one point lookup before each create-only write. The lookup and write are serialized inside one gateway process, but they are not an atomic compare-and-insert across replicas or independently running gateway processes. Deployments that require distributed exactly-once creation still need a stronger coordination mechanism; the endpoint should not be treated as a substitute for Qdrant-side conditional insertion.
