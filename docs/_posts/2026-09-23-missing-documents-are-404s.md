---
layout: post
title: "A missing document is a 404"
date: 2026-09-23
---

Elasticsearch's get-document API distinguishes a missing document at both layers of the response: the JSON body contains `"found": false`, and the HTTP status is 404. The gateway previously returned the right body with HTTP 200.

That mismatch matters to clients. An application using the status code to select its not-found path could treat an absent document as a successful fetch, even though the response body said otherwise. The gateway now returns 404 while preserving the Elasticsearch-shaped body and `X-Elastic-Product` response header.

The regression test exercises both source-storage modes: `_source` embedded in the searchable Qdrant collection and the optional dedicated document projection. In each mode, an empty Qdrant lookup must produce the same result:

```http
HTTP/1.1 404 Not Found
X-Elastic-Product: Elasticsearch

{"_index":"products","_id":"missing","found":false}
```

This change is deliberately narrow. It does not yet emulate Elasticsearch's version, sequence-number, or primary-term behavior, and delete/update behavior for missing documents remains a separate compatibility surface.
