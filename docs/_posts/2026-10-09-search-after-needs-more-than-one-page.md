---
layout: post
title: "Search-after needs more than one page"
date: 2026-10-09
categories: [compatibility, elasticsearch]
---

A deterministic catalogue replay against native Elasticsearch 9.5.5 exposed an empty second page from the gateway. Six products were sorted by ascending price, the first two sort values were returned as the cursor, and Elasticsearch returned the next two products. The gateway returned none.

The cursor comparison itself was correct. The gateway had asked Qdrant for only `size` candidates, sorted those candidates locally, and then tried to find a cursor from the previous request inside that new first-page-sized set. The same boundary could also make the first sorted page depend on Qdrant's unrelated point order.

Explicitly sorted searches now retrieve a candidate window of at least the configured `MAX_PAGE_SIZE` before ordering and applying `search_after`. A request-level regression uses a Qdrant mock that really truncates its response to the requested limit; with a three-candidate window, a two-hit first page and its cursor produce the remaining document. The live replay with the default 1,000-candidate window improved from 10/12 to 11/12 checks against Elasticsearch 9.5.5. The saved [endpoint evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/elasticsearch-9.5.5-2026-10-09.json) records the exact requests, endpoint identities, and one-second gateway convergence allowance.

This is bounded compatibility, not global Qdrant field ordering. A cursor cannot advance beyond the configured candidate window, changes to that window can invalidate a cursor, and each explicitly sorted request can now retrieve and hydrate up to `MAX_PAGE_SIZE` candidates even when the returned page is small. The remaining replay difference is the already documented analyzer boundary: Elasticsearch's lowercase prefix over a `text` field matches `Wireless`, while gateway-side stored-source matching remains case-sensitive. This run validated Elasticsearch 9.5.5; it did not treat the prior OpenSearch 3.6.0 replay as proof of identical server behavior.
