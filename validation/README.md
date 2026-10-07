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

## Logistics: parcels missing tracking

The [field note](../docs/_posts/2026-10-07-logistics-missing-tracking.md) uses
[five synthetic parcels](fixtures/logistics.json) to find packed York shipments
with missing or null tracking, count them by carrier, and assign one tracking
number without changing the parcel's other fields.

```sh
python3 validation/logistics.py \
  --native http://localhost:19200 \
  --gateway http://localhost:9200 \
  --gateway-version 0.2.0 --qdrant-version 1.15.5 \
  --output validation/results/logistics.json
```

Use disposable endpoints with no `field_note_parcels` index. Creation failure
aborts that endpoint without deleting existing data; successful creation is
cleaned up after replay. Python 3 is the only dependency. Supply the actual
binary/backend versions; the report also records the current checkout revision
and each endpoint's advertised API version (not the gateway binary version).
Run from the same revision used to build the gateway, with default embedded-source
storage and synchronous writes. The same five-second pause follows bulk and
update on both servers; `--settle-seconds` changes both equally.

The [2026-10-07 evidence](evidence/logistics-2026-10-07.json) records **66/66**
checks passing on Elasticsearch **8.15.0**, gateway **0.2.0** at `7e56704`, and
Qdrant **1.15.5**. The replay verifies identical request sequences, exact HTTP
and bulk statuses/results, all stored IDs/sources, search IDs/sources and exact
totals, carrier buckets, and preservation after adding tracking. Missing and null
tracking are included; labelled, dispatched, and other-depot parcels are excluded.
Failures exit nonzero and remain in the report. This is correctness evidence,
not a performance, refresh-timing, or concurrent-update guarantee.
