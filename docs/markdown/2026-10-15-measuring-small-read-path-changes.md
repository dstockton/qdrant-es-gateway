# Fewer requests did not make this query faster

> Measurement note: these historical filter-aggregation trials recomputed the expected count in each client worker. The calculation was outside that request's timer, but affected throughput and could delay concurrent requests. Later trials tagged `precomputed-filter-count-v2` compute it once before timing. The figures here are preserved without adjustment; do not combine them with the corrected trials.

We tried to speed up gateway aggregations by combining their safety checks.
Two filter counts would need four Qdrant calls instead of five, including the
exact total. That sounded promising. After measuring the final implementation,
we removed it: fewer calls did not produce a reliable latency improvement, and
filter counts were slightly slower.

This followed our [indexed-query optimization](https://dstockton.github.io/qdrant-es-gateway/markdown/2026-10-15-indexed-query-performance.md).
The large win there came from avoiding a complete document scan. The next round
was about much smaller costs: database query preparation, duplicated payloads,
and coordination between independent reads.

### Compare the code, not the warmup

Fresh runs changed the ranking even when the gateway binary stayed identical.
We therefore kept the same 10,000 documents and alternated the old and new
images in **old, new, new, old** order. Each round shuffled Elasticsearch,
OpenSearch and gateway + Qdrant trials, used persistent connections, and warmed
each workload before measuring it. No image builds ran during these trials.

The retained read-path change passed **72,000 measured response checks** across
four rounds. That includes full sources, IDs, ordering, exact totals and buckets.
Each variant contributes six trial medians to each table cell; values are
**one client / eight clients**, in milliseconds.

| Gateway operation | Before | After |
|---|---:|---:|
| Read one document | 0.380 / 0.781 | 0.370 / 0.783 |
| Filtered, sorted page | 0.702 / 2.055 | 0.688 / 2.037 |
| Page after a sort cursor | 1.022 / 2.613 | 1.009 / 2.521 |
| Keyword terms buckets | 0.892 / 3.382 | 0.896 / 3.159 |
| Two filter counts | 1.109 / 3.332 | 1.129 / 3.283 |

These small, mixed differences do not establish a broad speedup. Native engines
remained faster in all ten workload/concurrency combinations in this warmed
comparison. The [complete summary](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-read-path/summary.json)
includes their results, p95 latency, throughput and trial ranges.

### What we kept

Document reads now ask Qdrant only for the payload fields they consume. For one
representative benchmark document, the internal payload shrank from **684 to
274 bytes**, while the returned source stayed identical. Bulk existence checks
request only document IDs. GET and HEAD no longer parse unused mapping metadata;
SQLite reuses prepared statements, and parsing happens outside the catalog lock.
The final search page also takes ownership of its hits instead of copying them.

These remove specific work without a result cache or another service. Alias
changes still take effect immediately: prepared statements cache query plans,
not catalog answers. The evidence supports reduced data movement; it does not
support a promised percentage improvement in application latency.

### What we rejected

The combined aggregation check required a more complicated backend predicate.
Review also caught an intermediate-memory problem with launching every facet at
once. We bounded concurrency and reduced responses immediately, then reran the
comparison. The final result still did not justify the extra complexity, so the
original aggregation path remains. CPU redistribution, one runtime worker and
an exploratory in-memory payload configuration also failed to establish a
consistent benefit; their defaults remain unchanged.

Native caching matters, but it is not the whole explanation. Both native engines
recorded roughly 18,000 request-cache hits and three misses. [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-request-cache.html)
and [OpenSearch](https://docs.opensearch.org/latest/search-plugins/caching/request-cache/)
cache eligible aggregation requests by default.
A separate `request_cache=false` diagnostic left those counters unchanged, yet
native engines remained faster. Adding Redis without locating the remaining
cost would not by itself solve that difference.

### Reproduce the read comparison

Build both gateway revisions before timing, load the shared corpus using the
[three-engine runner](https://github.com/dstockton/qdrant-es-gateway/tree/main/benchmark/three-engine),
and replace only the gateway image between rounds. Retain the data volumes and
wait for readiness after each replacement.

```sh
ROUND=baseline-a REQUESTS=200 TRIALS=3 WARMUP=40 \
  OUTPUT=results/baseline-a.json python3 compare_reads.py
# Repeat with candidate-b, candidate-c and baseline-d.
python3 summarize_reads.py results/baseline-a.json results/baseline-d.json
python3 summarize_reads.py results/candidate-b.json results/candidate-c.json
```

Every engine receives the same requests; the original aggregate two-CPU, 4-GiB
limits remain. The [evidence manifest](https://dstockton.github.io/qdrant-es-gateway/assets/benchmarks/2026-10-15-read-path/manifest.json)
identifies revisions, raw round files, image IDs and the rejected experiment.
This remains an investigation, not a claim that the gateway has overtaken the
native engines.
