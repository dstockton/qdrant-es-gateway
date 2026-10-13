---
layout: post
title: "Creating an index is not a mapping update"
date: 2026-10-13
categories: [production, compatibility, elasticsearch, opensearch]
---

Provisioning code often treats “already exists” as a narrow, recoverable race. It can do that reliably only when the server identifies the condition correctly. The gateway already rejected a second `PUT /products` before contacting Qdrant, but it grouped the request with unsupported query features and returned `qdrant_gateway_unsupported_query`. That classification could make an Elasticsearch or OpenSearch client treat a normal create race as an incompatible request.

Elasticsearch and OpenSearch implement this case independently but agree on the exception class. [Elasticsearch 9.5.5's create-index service](https://github.com/elastic/elasticsearch/blob/v9.5.5/server/src/main/java/org/elasticsearch/cluster/metadata/MetadataCreateIndexService.java#L291-L295) throws `ResourceAlreadyExistsException` when either routing or index metadata already contains the name. [OpenSearch 3.9's corresponding service](https://github.com/opensearch-project/OpenSearch/blob/3.9/server/src/main/java/org/opensearch/cluster/metadata/MetadataCreateIndexService.java#L275-L279) performs the same two checks. OpenSearch also publishes the resulting REST example as HTTP 400 with `resource_already_exists_exception` in its [create/delete error-shape discussion](https://github.com/opensearch-project/OpenSearch/issues/14805).

The gateway now returns the shared client-visible shape, including a root cause, top-level exception, reason, index name, and status 400. The check runs under the existing index-administration lock and before any Qdrant request. A focused regression also proves that the original mapping remains unchanged:

```console
cargo test duplicate_index_creation_returns_native_conflict_without_qdrant_work
```

This is request-level validation with a mocked upstream request counter, not a live-engine benchmark. Cached Elasticsearch 9.5.5 and OpenSearch 3.9.0 containers were available for a native probe, but the local Docker storage backend returned an input/output error before either container started. The version-specific source above is therefore the native evidence for this run. The gateway does not include a native index UUID because its metadata catalogue does not currently assign one. A different requested name that merely normalizes to an occupied Qdrant collection remains a gateway-specific `index.name` validation error rather than pretending that the Elasticsearch/OpenSearch index itself already exists.
