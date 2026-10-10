---
layout: post
title: "Disabled hit totals should skip the count"
date: 2026-10-10
categories: [compatibility, opensearch, performance]
---

OpenSearch lets a search caller choose the cost and precision of `hits.total`. A live probe against OpenSearch 3.6.0 with 15 matching documents confirmed the three useful modes: the default returned `15/eq`, `track_total_hits: false` omitted `hits.total`, a threshold of 5 returned `5/gte`, and a threshold of 20 returned `15/eq`. The gateway previously ignored all four request shapes and always paid for its exact Qdrant count on eligible filter-only searches.

The gateway now accepts boolean and non-negative integer modes. Disabling totals omits the response field and, importantly, skips the exact-count request. A live gateway backed by the repository's pinned Qdrant 1.15.5 image matched all four OpenSearch response shapes after write visibility converged. A request-level regression with a mocked Qdrant endpoint records one upstream scroll request instead of the usual count plus scroll pair. Numeric thresholds cap exact or bounded totals and preserve an honest `eq` or `gte` relation.

The [saved native-engine evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/opensearch-3.6-track-total-hits-2026-10-10.json) contains the fixture and response values without endpoint or container identifiers. This is a request-count result, not a latency benchmark: the actual saving depends on collection size, filters, Qdrant load, and network placement.

There are two deliberate boundaries. A numeric threshold shapes the gateway response but does not terminate Qdrant's exact count early, so only `false` has a demonstrated upstream request saving. OpenSearch 3.6.0 also coerced some undocumented string, fractional, and negative values during the probe; the gateway accepts the documented boolean and non-negative integer forms and rejects other types before index lookup or Qdrant work. Elasticsearch was not run for this field note, so the native evidence here is OpenSearch-specific even though the supported request forms are shared by both APIs.
