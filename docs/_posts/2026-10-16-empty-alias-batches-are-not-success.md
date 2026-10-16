---
layout: post
title: "An empty alias batch is not success"
date: 2026-10-16
categories: [production, compatibility, elasticsearch, opensearch]
---

The aliases API is an administrative write boundary. The gateway previously treated `{"actions":[]}` as a valid request: it opened and committed an empty catalogue transaction, then returned `{"acknowledged":true}`. A client bug that accidentally dropped every action could therefore look like a completed routing change.

Separate clean-container probes sent the same empty action list and missing action field to Elasticsearch 9.5.5 and OpenSearch 3.9.0. Both engines returned HTTP 400 with `illegal_argument_exception` and the reason `No action specified` for both requests. The sanitized [request and response evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/elasticsearch-9.5.5-opensearch-3.9-empty-alias-actions-2026-10-16.json) records the exact engine versions, images, and observed fields.

The gateway now checks for at least one action before index lookup or the alias handler's catalogue transaction and returns the shared native error shape. In shared-catalog mode, the outer administrative wrapper still loads a snapshot and acquires then releases its claim on this validation error; it does not publish a changed snapshot. A focused regression covers both `{}` and `{"actions":[]}` and proves an existing alias target remains unchanged:

```console
cargo test empty_alias_batches_are_rejected_without_catalogue_changes
```

This is request-level compatibility evidence, not a performance benchmark. It does not claim that every malformed alias body has identical parsing errors: for example, non-array `actions` values continue to use the gateway's structured unsupported-request response. Supported non-empty batches and the documented Elasticsearch/OpenSearch mixed-action response difference are unchanged.
