---
layout: post
title: "Batching searches and paging through ordered results"
date: 2026-09-18
categories: [features]
---

An application may need several searches to populate a catalogue page, or successive pages from an ordered result set. Qdrant ES Gateway supports `_msearch` for batching searches and `search_after` for cursor pagination within its supported search API.

The `_msearch` endpoint accepts Elasticsearch-style NDJSON header/query pairs. Each sub-search uses the same client request shape as an individual search and is subject to the gateway’s query compatibility limits.

For ordered pages, `search_after` requires explicit sort fields. Each returned hit includes the corresponding `sort` values, which the client can pass as `search_after` from the last hit when requesting the next page. The cursor is stable only while the ordered result set is unchanged; changes to documents or their ordering between requests can affect pagination.

These features support existing batching and pagination request shapes, but do not imply full Elasticsearch compatibility. Field sorting has limits, deep pagination is capped, and deep scored pagination remains outside the supported scope. Applications should check their queries and pagination needs against the gateway’s compatibility documentation before migration.
