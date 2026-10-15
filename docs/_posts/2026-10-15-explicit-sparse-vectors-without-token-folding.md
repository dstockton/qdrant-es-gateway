---
layout: post
title: "Keep your sparse token IDs intact"
categories: compatibility
---

A sparse embedding is a small dictionary: token IDs paired with weights. If the
same token changes identity between indexing and searching, a successful HTTP
response can hide the wrong answer.

An [OpenSearch report](https://github.com/opensearch-project/neural-search/issues/2048)
describes exactly that problem in the SEISMIC approximate path of OpenSearch
3.9.0: “the query token is folded, the document tokens are not.” The report also
says the Lucene exact path returns the correct results.

The gateway now accepts explicit sparse embeddings and passes their numeric IDs
to Qdrant unchanged. IDs such as `32768`, `65535` and `65536` stay distinct.
Documents and queries share the same validation: canonical decimal IDs in the
unsigned 32-bit range and positive, finite float32 weights. Invalid vectors fail
before document writes. You supply embeddings; the gateway does not run a model.

Create a field with the common OpenSearch mapping shape:

```json
{"settings":{"index.sparse":true},"mappings":{"properties":{"v":{"type":"sparse_vector","method":{"name":"seismic","engine":"lucene"}}}}}
```

Index a document containing `{"v":{"65536":1.0}}`, then search:

```json
{"size":10,"query":{"neural_sparse":{"v":{"query_tokens":{"65536":1.0}}}}}
```

Qdrant executes an exact sparse dot product. The method labels in the mapping
are accepted compatibility syntax, not a request to run Lucene inside Qdrant.
Engine-specific tuning and model inference are rejected with a 400 response.

We checked the common request shape against real OpenSearch 3.9.0 and Qdrant
1.19.2. Eight documents used tokens `0`, `1`, `32767`, `32768`, `40342`, `65535`,
`65536` and `105878`. Every query returned exactly its own document on both
endpoints. This is a correctness check, **not a reproduction of the reported
approximate-path bug** or a speed comparison.

To repeat it from the repository checkout:

```sh
docker compose -f examples/explicit-sparse/compose.yml up -d --build
# Wait for both endpoints to answer GET /.
python3 examples/explicit-sparse/compare.py
docker compose -f examples/explicit-sparse/compose.yml down -v
```

The [demo and captured results](https://github.com/dstockton/qdrant-es-gateway/tree/main/examples/explicit-sparse)
include the complete fixture and individual timings. The script sends identical
requests to both endpoints, changing only the endpoint address. It replaces its
own demo index, so run it against disposable instances.

For an application already using Qdrant, this adds an OpenSearch-shaped way to
search precomputed embeddings without another inference service or gateway
session store. It does not establish that migrating from OpenSearch is better:
the tested native exact path is correct too. Sparse queries currently support a
single top-level field and bounded `from`/`size`; total hits are a lower bound.
They reject aggregations, sorting, collapse, post-filtering and `search_after`.
Use a new index when adding sparse fields to an existing mapping.
