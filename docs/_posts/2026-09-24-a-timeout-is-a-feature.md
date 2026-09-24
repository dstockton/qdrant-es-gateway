---
layout: post
title: "A timeout is a feature"
date: 2026-09-24
categories: [production, reliability]
---

What happens when Qdrant stops responding?

`QDRANT_CONNECT_TIMEOUT_MS` and `QDRANT_REQUEST_TIMEOUT_MS` configure upstream timeouts, defaulting to five seconds and three minutes. The gateway logs both values at startup.

With `ASYNC_PAYLOAD_WRITES=true`, eligible payload updates run in background tasks. `ASYNC_WRITE_QUEUE` limits concurrent tasks to 256 by default; further updates receive an error while all slots are occupied. Failed background writes are logged.

CI checks Rust dependencies for known advisories. A manual release dry run builds and scans the image and packages the Helm chart without logging into GHCR or publishing a tag.
