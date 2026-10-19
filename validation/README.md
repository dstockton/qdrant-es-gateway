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

The AWS Retail Demo Store and Spinscale applications are useful follow-on targets. Their request shapes and current gaps are documented in `docs/application-validation.md`.

## AWS Retail Demo Store replay

[`retail_demo.py`](retail_demo.py) replays the isolated search service from the
[AWS Retail Demo Store](https://github.com/aws-samples/retail-demo-store) against
native OpenSearch and the gateway. It creates the `products` index with settings
only, writes individual products, then sends the application's real
`dis_max`/`match_bool_prefix` search and `more_like_this` similar-products
requests. It checks the response fields the service consumes, including named
`inner_hits` and `_source: false`.

```sh
python3 validation/retail_demo.py \
  --native http://localhost:19311 \
  --gateway http://localhost:19313 \
  --docs 1000 \
  --output validation/evidence/retail-demo-2026-10-17.json
```

For a larger generated corpus, `--workers` keeps the writes as individual
document PUTs while avoiding an unnecessarily serial harness run. The clean
1,000 and 1,000,000 document comparison used 16 and 32 workers respectively;
the generated names and descriptions are unique while retaining five product
families. Resource snapshots from both runs are recorded in
`validation/evidence/retail-demo-2026-10-17-1k-clean.json` and
`validation/evidence/retail-demo-2026-10-17-1m-clean.json`.

A 10,000,000-document bulk attempt is recorded in
`validation/evidence/retail-demo-2026-10-17-10m-capacity-limit.json`. After a
complete Docker cleanup, it reached about 9.57M documents before Qdrant was
killed with exit code 137 during the final search phase. Treat that as a
test-host memory limit, not as a completed 10M benchmark.

This is an application-contract replay, not a claim of identical OpenSearch
ranking. The gateway's lexical scores, category diversity, and similar-product
ordering can differ even when the application's request and response contract
passes.

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

## Media archive clearance shortlist

The [media field note](../docs/_posts/2026-10-09-media-clearance-shortlist.md)
uses six synthetic clips in [`fixtures/media.json`](fixtures/media.json).
It checks array membership for topics and cleared territories, a duration limit,
format facets, and removal of UK clearance while preserving France and all other
source fields. This is metadata retrieval, not rights adjudication.

```sh
python3 validation/media.py \
  --native http://localhost:19200 \
  --gateway http://localhost:9200 \
  --output validation/results/media.json
```

Use disposable endpoints without `field_note_media`. The replay creates and
removes that index; a failed create aborts without deleting existing data.
It uses Python's standard library and pauses five seconds after bulk ingestion
and update on each server (`--settle-seconds` controls both). This is not a
refresh timing guarantee. Exact statuses, bulk IDs/results, every source,
search IDs and totals, facet counts, and array replacement are checked against
independent expectations; requests must also match between endpoints.

The [2026-10-09 evidence](evidence/media-2026-10-09.json) records **70/70**
checks passing with native Elasticsearch **8.15.0**, gateway **0.2.0** at the
recorded revision, and Qdrant **1.15.5**, with embedded sources and synchronous
writes. Optional `--gateway-version` and `--qdrant-version` arguments record
operator-verified versions; the endpoint's `reported_version` is its API
advertisement. No gateway implementation change or performance claim was needed.

## Agriculture irrigation inspection queue

The [field note](../docs/_posts/2026-10-19-agriculture-irrigation-inspection.md)
uses six synthetic sensor snapshots in
[`fixtures/agriculture.json`](fixtures/agriculture.json). It excludes readings
at the moisture threshold, stale or missing reading ages, and closed plots,
then checks that marking a plot inspected preserves its source and removes it
from both the queue and crop facets.

```sh
python3 validation/agriculture.py \
  --native http://localhost:19200 \
  --opensearch http://localhost:19201 \
  --gateway http://localhost:9200 \
  --output validation/results/agriculture.json
```

Use disposable endpoints without `field_note_irrigation`. The replay creates
and removes that index; a failed create leaves existing data untouched.
Python 3 alone is required. All engines use the same five-second settling pauses
(`--settle-seconds` changes all three); a pass does not guarantee refresh timing.
Reading ages are fixed snapshots, not a live clock. No irrigation advice or
atomic equipment-control behavior is asserted.

The [2026-10-19 evidence](evidence/agriculture-2026-10-19.json) records **105/105**
checks passing on Elasticsearch **8.15.0**, OpenSearch **3.9.0**, and gateway
**0.2.0** at the recorded revision with Qdrant **1.15.5**, embedded sources,
and synchronous writes. Requests are identical across all three endpoints;
statuses, bulk IDs/results, all stored sources, filtered IDs/totals, facets,
and the updated source are asserted independently against the fixture.
No OpenSearch-specific assertion difference was observed. The
[article curl evidence](evidence/agriculture-curl-2026-10-19.json) separately
records all nine article responses per engine, including the repeated searches.
No gateway implementation change or performance claim was needed.
