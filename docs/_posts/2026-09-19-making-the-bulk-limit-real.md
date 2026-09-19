---
layout: post
title: "Making the bulk request limit real"
date: 2026-09-19
categories: [production, compatibility]
---

The gateway has separate request-size controls for ordinary JSON requests and bulk NDJSON: `MAX_BODY_BYTES` defaults to 10 MiB, while `MAX_BULK_BYTES` defaults to 50 MiB. That distinction matters because a useful bulk batch is often much larger than a search or single-document request.

Until now, the HTTP body reader applied `MAX_BODY_BYTES` before routing the request. The bulk handler did check `MAX_BULK_BYTES`, but it could never see a request larger than the general limit. With the defaults, a 10–50 MiB bulk request was therefore rejected before the bulk-specific check ran.

The reader now selects the limit from the method and path before consuming the body. `POST /_bulk` and `POST /<index>/_bulk` use `MAX_BULK_BYTES`; every other request, including a non-POST request to a bulk-shaped path, continues to use `MAX_BODY_BYTES`. A regression test covers all four cases.

To reproduce the verification:

```bash
cargo test bulk_requests_use_the_dedicated_body_limit
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

This is a correctness fix, not a streaming implementation. Accepted bodies are still buffered before NDJSON parsing, so operators should keep `MAX_BULK_BYTES` bounded below the memory available per concurrent request and enforce compatible limits at the ingress. The change does not alter bulk parsing, batching, acknowledgement, or search visibility semantics.
