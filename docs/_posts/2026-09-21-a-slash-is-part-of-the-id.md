---
layout: post
title: "A slash is part of the document ID"
date: 2026-09-21
categories: [compatibility, correctness]
---

Elasticsearch clients put document IDs in URL path segments. An ID such as `order/42` therefore reaches the server as `order%2F42`; spaces and non-ASCII text are encoded for the same reason. The gateway previously routed on the raw URI path, so it stored the encoded spelling as `_id` instead of the caller's original value. A later request produced by a client from `order/42` could still find that spelling, but responses exposed `order%2F42`, breaking identity round trips and the documented arbitrary-ID guarantee.

Routing now percent-decodes each segment independently. Decoding after splitting is important: `%2F` becomes part of the document ID rather than a new route separator. Invalid UTF-8 is rejected as a structured bad request instead of being replaced with lossy text.

The focused verification covers an ID containing an encoded slash, Unicode, and a space, plus a non-UTF-8 path:

```bash
cargo test path_segments_
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

This is route-level compatibility, not a relaxation of index-name rules or an implementation of Elasticsearch's full URL grammar. Reverse proxies must also preserve encoded slashes when forwarding requests; deployments should include one representative encoded ID in ingress smoke tests.
