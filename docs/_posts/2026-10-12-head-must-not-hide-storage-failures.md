---
layout: post
title: "HEAD must not hide storage failures"
date: 2026-10-12
categories: [production, compatibility, elasticsearch, opensearch]
---

Document-existence checks have a deliberately small contract. The [Elasticsearch 8 API reference](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-exists) and the [OpenSearch Get Document API reference](https://docs.opensearch.org/latest/api-reference/document-apis/get-documents/#checking-document-existence) both describe `HEAD /<index>/_doc/<id>` as HTTP 200 when the document exists and HTTP 404 when it does not. A storage outage is neither result.

The gateway's HEAD path previously caught every Qdrant error and returned 404. A connection failure, missing collection, or Qdrant 5xx therefore looked like a trustworthy “document absent” answer. That can make health checks, conditional writes, and reconciliation jobs take destructive action on false information.

HEAD now uses the same narrow missing-point classification as GET without retrieving the document source. The known Qdrant missing-point response remains a 404, while transport failures and upstream errors return 502. All gateway HEAD routes also attach `X-Elastic-Product: Elasticsearch`, and the response body remains empty. The focused regression injects a Qdrant 503 and verifies an empty 502 response in both embedded-source and document-projection modes:

```console
cargo test head_document_does_not_hide_qdrant_failures_as_not_found
```

This validation is request-level and uses a mocked Qdrant response; it is not a live Elasticsearch, OpenSearch, or Qdrant benchmark. The native status contract comes from the public engine documentation above. Query parameters such as `routing`, `refresh`, `realtime`, and explicit version checks remain outside the gateway's current HEAD compatibility surface.
