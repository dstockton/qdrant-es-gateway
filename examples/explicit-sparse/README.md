# Explicit sparse token comparison

Run from the repository root:

```sh
docker compose -f examples/explicit-sparse/compose.yml up -d --build
# Wait until both endpoints answer GET / before running the comparison.
python3 examples/explicit-sparse/compare.py
# Remove only this demo's containers and anonymous volumes afterwards.
docker compose -f examples/explicit-sparse/compose.yml down -v
```

The script sends identical index settings, mapping, eight bulk documents, refresh,
and eight search requests to OpenSearch 3.9.0 and the gateway. It asserts exactly
one correct document for tokens 0, 1, 32767, 32768, 40342, 65535, 65536 and 105878.
Only the endpoint differs. Every run deletes and recreates `explicit-sparse-demo`.
Use dedicated disposable endpoints.

`results.json` records a correctness smoke run on 2026-10-15: OpenSearch 3.9.0
and Qdrant 1.19.2 in local containers; gateway debug build on the host. Both
returned eight correct results. Timings are individual cold/warm mixed requests,
not a throughput or comparative performance benchmark. Container settings are
512 MiB OpenSearch JVM heap; no memory or CPU limits for Qdrant or gateway.

This uses OpenSearch's default Lucene sparse path on eight documents. It does
**not** reproduce the SEISMIC approximate-path bug in neural-search issue #2048,
which needs different mapping and query tuning. The gateway rejects those tuning
parameters rather than claiming they have the same effect in Qdrant. The
`seismic`/`lucene` mapping labels are accepted as compatibility syntax; execution
uses exact Qdrant sparse dot products, without token-ID remapping.
