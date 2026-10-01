---
layout: post
title: "Invalid UTF-8 is not source data"
date: 2026-10-01
categories: [production, compatibility]
---

The gateway used to convert every HTTP body with a lossy UTF-8 decoder before parsing JSON or bulk NDJSON. An invalid byte inside a quoted value therefore became the Unicode replacement character (`U+FFFD`). If the value was source data or a bulk `_id`, the request could remain syntactically valid and write data under a value the client never sent.

The HTTP boundary now requires strict UTF-8 and returns a structured HTTP 400 response with `feature: request.body` before endpoint dispatch. The same check covers ordinary JSON, bulk, and multi-search requests. It runs after the configured byte limit, so oversized bodies still receive the more useful HTTP 413 response without an additional allocation beyond the already bounded body.

The regression injects byte `0xff` into a quoted document field and a quoted bulk identifier, then verifies that both requests are rejected with the Elasticsearch product header:

```bash
cargo test invalid_utf8_request_bodies_are_rejected_without_lossy_replacement
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

This validation does not normalize valid Unicode or require ASCII. UTF-8 document content and identifiers continue to work unchanged, while clients that produce malformed encodings must correct or explicitly transcode them before sending JSON.
