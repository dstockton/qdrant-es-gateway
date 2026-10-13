# Endpoint-only application validation

`replay.py` is a small, dependency-free application-shaped compatibility check. It uses the same HTTP request sequence against two endpoints, so the only intended application change is the base URL. The fixture is deliberately small and deterministic; it is a contract smoke test, not a replacement for the large performance benchmark.

The suite covers the workflow a product catalogue commonly needs:

1. index creation and bulk ingestion;
2. lexical search with filters, source projection, sorting, and pagination;
3. keyword facets and pattern queries;
4. document reads, partial updates, deletes, and count/freshness checks.

Run it with Elasticsearch and the gateway already listening:

```sh
python3 validation/replay.py \
  --native http://localhost:19200 \
  --gateway http://localhost:9200 \
  --output validation/results/latest.json
```

For a gateway using Qdrant's sparse index, add a deliberate post-ingest settle
when measuring search semantics rather than refresh timing:

```sh
python3 validation/replay.py \
  --native http://localhost:19200 \
  --gateway http://localhost:9200 \
  --settle-seconds 1 \
  --output validation/results/latest.json
```

The report records both root endpoint identities, including their advertised
engine versions, and the settle interval. Deletes use `refresh=wait_for` on
both endpoints so the final count does not compare a refreshed gateway write
with an unrefreshed native-engine write. The settle interval remains an honest
gateway limitation: it does not turn `_refresh` into an Elasticsearch or
OpenSearch visibility guarantee.

The [2026-10-09 Elasticsearch evidence](evidence/elasticsearch-9.5.5-2026-10-09.json)
records 11/12 checks passing against native Elasticsearch 9.5.5 and the gateway
backed by Qdrant 1.15.5, with a one-second gateway convergence allowance. The
remaining difference is the documented case-sensitive stored-source prefix
behavior on an analyzed `text` field. This is request-level compatibility
evidence over six documents, not a performance or complete API-parity claim.

The [OpenSearch 3.6.0 `track_total_hits` probe](evidence/opensearch-3.6-track-total-hits-2026-10-10.json)
records the native response shapes for enabled, disabled, and thresholded hit
counts over 15 documents. A live gateway backed by Qdrant 1.15.5 matched all
four response shapes after write visibility converged. A separate request-level
mock proves that disabling totals reduces the filter-only Qdrant request count
from two to one. This is not a latency benchmark or a synchronized cross-engine
replay.

The [OpenSearch 3.9.0 integer-total probe](evidence/opensearch-3.9-rest-total-hits-as-int-2026-10-11.json)
records `_search` and `_msearch` response shapes for
`rest_total_hits_as_int=true`, including the `-1` sentinel paired with
`track_total_hits: false`, lowercase and valueless boolean parsing, and rejection
of a numeric tracking threshold. A live gateway backed by Qdrant 1.15.5 matched the
six saved single-search cases and the two multi-search totals. This is
OpenSearch-specific request/response evidence over three documents, not an
Elasticsearch claim or a performance benchmark.

The [Elasticsearch 9.5.5 source-filtering probe](evidence/elasticsearch-9.5.5-source-filtering-2026-10-11.json)
records matching native and live-gateway response shapes for a single dotted
field, a nested wildcard, include/exclude composition, and an invalid numeric
filter. The gateway endpoint used Qdrant 1.15.5. This evidence is specific to
Elasticsearch; OpenSearch was not probed for this field note.

The [missing-point index probe](evidence/elasticsearch-9.5.5-opensearch-3.9-qdrant-missing-point-2026-10-12.json)
records the first-write 201/`created` and replacement 200/`updated` contract
against both Elasticsearch 9.5.5 and OpenSearch 3.9.0. It also records Qdrant
1.15.5's HTTP 404 response for an absent single point and the matching live
gateway GET/index sequence after the fix. This is a single-document correctness
check, not a concurrency or performance benchmark.

The command exits non-zero when transport errors occur or when a required semantic assertion fails. Search ranking is compared by top-k overlap and returned `_source` values rather than raw `_score`; equivalent engines are not expected to produce identical scores. The report records every request, status, latency, and assertion so a failed case can be reproduced.

For an endpoint-only application check, point the same client at each URL. For example, the repository's Python example changes only its `Elasticsearch(...)` URL; its index, index, search, and response-handling calls remain the same.

The AWS Retail Demo Store and Spinscale applications are useful follow-on targets, but both use features outside this gateway's deliberately supported subset. Their request shapes and current gaps are documented in `docs/application-validation.md`.

## Manufacturing maintenance queue

The [field note](../docs/_posts/2026-10-03-manufacturing-maintenance-queue.md)
uses five synthetic work orders in
[`fixtures/manufacturing.json`](fixtures/manufacturing.json). Run the exact same
request sequence on both endpoints:

```sh
python3 validation/manufacturing.py \
  --native http://localhost:19200 \
  --gateway http://localhost:9200 \
  --output validation/results/manufacturing.json
```

Use disposable endpoints with no `field_note_maintenance` index. The replay
creates that index and deletes it on completion; an existing index causes failure
without deleting existing data. It requires Python 3 and no extra packages.
The same five-second pause follows bulk ingestion and the update on both servers;
`--settle-seconds` can change it for both, but a pass is not a refresh guarantee.

The replay asserts exact HTTP statuses, bulk item IDs/statuses/results, every
stored source, filtered search IDs and sources, exact team facet counts, and the
closed job's preserved source and exclusion from the open queue. Each endpoint
must satisfy independent fixture expectations, and their request sequences must
match. Transport or semantic failures exit nonzero and remain in the report.

The [2026-10-03 evidence](evidence/manufacturing-2026-10-03.json) records **62/62**
checks passing against native Elasticsearch **8.15.0**, gateway **0.1.3** at the
recorded source revision, and Qdrant **1.15.3**, using the default embedded-source
mode and synchronous writes. `reported_version` is the endpoint's Elasticsearch
API version advertisement, not the gateway binary version. No latency or ranking
comparison is claimed. No gateway implementation change was needed.

## Support first-reply queue

The [support field note](../docs/_posts/2026-10-13-support-first-reply-queue.md)
uses seven synthetic tickets in [`fixtures/support.json`](fixtures/support.json).
It selects open billing tickets with zero replies and no assignee, counts their
channels, then assigns one ticket and verifies its preserved source and exclusion.
Missing and null assignees qualify; an empty string counts as present.

```sh
python3 validation/support.py \
  --native http://localhost:19200 \
  --gateway http://localhost:9200 \
  --output validation/results/support.json
```

Use disposable endpoints without `field_note_support`. The replay creates and
removes that index; a failed create aborts without deleting existing data.
It uses Python's standard library and pauses five seconds after bulk ingestion
and update on each server (`--settle-seconds` controls both). This is not a
refresh timing guarantee. Exact statuses, bulk IDs/results, every source,
search IDs and totals, channel counts, and assignment exclusion are checked
against independent expectations; requests must also match between endpoints.

The [2026-10-13 replay evidence](evidence/support-2026-10-13.json) records
**74/74** checks passing with native Elasticsearch **8.15.0**, gateway **0.2.0**
at the recorded revision, and Qdrant **1.15.5**, with embedded sources and
synchronous writes. The [article curl evidence](evidence/support-curl-2026-10-13.json)
records all nine responses per endpoint, including repeated searches after the
assignment; statuses, bulk results, sources, totals, and buckets were asserted.
Optional `--gateway-version` and `--qdrant-version` arguments record verified
versions; the endpoint's `reported_version` is its API advertisement.
No gateway code change, performance claim, or atomic concurrent claim is implied.
