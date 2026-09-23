---
layout: post
title: "Oversized requests are not bad JSON"
date: 2026-09-23
categories: [production, compatibility]
---

The gateway bounds request bodies with `MAX_BODY_BYTES` and gives bulk ingestion a separate `MAX_BULK_BYTES` ceiling. Those limits protect memory, but the rejection previously looked like a generic HTTP 400 compatibility error. A client could not reliably tell whether to fix malformed JSON or split a valid payload into smaller requests.

Oversized requests now return HTTP 413 with Elasticsearch's `content_too_long_exception` error type. The response names the active setting and its byte limit, so an operator can decide whether to batch more narrowly or deliberately change the deployment limit. Both the early gateway check and the streaming body collector use the same response shape; bulk routes identify `MAX_BULK_BYTES`, while other routes identify `MAX_BODY_BYTES`.

The regression test sets deliberately tiny limits and verifies the status, error type, setting name, and exact configured ceiling for a search request and a bulk request:

```bash
cargo test oversized_requests_return_413_with_the_active_limit
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

This does not implement automatic retrying or request splitting. HTTP 413 is intentionally non-transient: clients should reduce the body size, and operators should raise a limit only after considering the gateway's memory budget and concurrency.
