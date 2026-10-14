---
layout: post
title: "OpenSearch wildcards keep the leading star"
date: 2026-10-09
categories: [compatibility, opensearch]
---

A catalogue replay against native OpenSearch 3.6.0 found a gateway-only empty result for `{"wildcard":{"title":"*keyboard*"}}`. The document was present and other filters worked. The defect was in the gateway's wildcard-to-regular-expression translation: a leading `*` disappeared, turning a contains query into a starts-with query.

The translator now handles the pattern one character at a time. `*` becomes any-length text, `?` becomes one character, and every other character is escaped as a literal. A focused regression covers leading, trailing, and consecutive stars, `?`, and regular-expression punctuation in source text.

The reproducible endpoint replay used native OpenSearch 3.6.0, Qdrant ES Gateway from this change, and the repository's pinned Qdrant 1.15.5 image. With a one-second post-ingest settle, 10 of 12 application checks passed after the fix. The remaining differences are explicit limitations: a lowercase prefix over an analyzed OpenSearch `text` field does not match the gateway's mixed-case stored source, and gateway-side `search_after` ordering operates over a bounded candidate window. The saved [request-level evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/opensearch-3.6-2026-10-09.json) records both endpoint identities, requests, responses, and the settle interval.

The settle is part of the result, not a hidden correction. OpenSearch honored `refresh=wait_for`; the gateway's sparse search projection still needs a convergence allowance. The replay now records that allowance and sends `refresh=wait_for` for the final delete on both endpoints, preventing unmatched refresh timing from masquerading as a count incompatibility.

The current official Python client was checked separately at the transport boundary: `opensearch-py` 3.2.0 completed info, index creation, bulk indexing, search, count, update, get, and delete against the gateway. That is client-shape evidence, not a claim that the gateway identifies itself as OpenSearch or implements the full OpenSearch API.
