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
