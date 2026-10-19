---
layout: post
title: "Which batteries belong on the dispatch shortlist?"
date: 2026-10-17
categories: [field-notes]
---

An energy cooperative needs northern battery sites marked ready with at least 100 kWh available. Six synthetic snapshots include a site exactly at the threshold, one just below it, a southern site, a held site, and an offline site. Operator facets count sites, not stored energy.

Use a fresh `field_note_energy` index on running servers. Curl is the same client for native Elasticsearch, native OpenSearch, and qdrant-es-gateway: change only `ES` to the target endpoint URL.

```sh
ES=http://localhost:19200
curl -fsS -X PUT "$ES/field_note_energy" \
  -H 'Content-Type: application/json' -d '{"mappings":{"properties":{"site":{"type":"keyword"},"region":{"type":"keyword"},"operator":{"type":"keyword"},"state":{"type":"keyword"},"available_kwh":{"type":"integer"}}}}'
curl -fsS -X POST "$ES/field_note_energy/_bulk?refresh=wait_for" \
  -H 'Content-Type: application/x-ndjson' --data-binary @- <<'NDJSON'
{"index":{"_id":"battery-101"}}
{"site":"Moor Lane","region":"north","operator":"Aster","state":"ready","available_kwh":120}
{"index":{"_id":"battery-102"}}
{"site":"Quarry Farm","region":"north","operator":"Birch","state":"ready","available_kwh":100}
{"index":{"_id":"battery-103"}}
{"site":"Mill Yard","region":"north","operator":"Aster","state":"ready","available_kwh":99}
{"index":{"_id":"battery-104"}}
{"site":"South Meadow","region":"south","operator":"Birch","state":"ready","available_kwh":180}
{"index":{"_id":"battery-105"}}
{"site":"Hill Barn","region":"north","operator":"Cedar","state":"hold","available_kwh":200}
{"index":{"_id":"battery-106"}}
{"site":"Canal Depot","region":"north","operator":"Cedar","state":"offline","available_kwh":250}
NDJSON
sleep 5
```

Use the same five-second pause after bulk and update on all three servers; this tests settled data, not an immediate refresh guarantee. Find the eligible sites:

```sh
curl -fsS -X POST "$ES/field_note_energy/_search" \
  -H 'Content-Type: application/json' -d '{"size":10,"query":{"bool":{"filter":[{"term":{"region":"north"}},{"term":{"state":"ready"}},{"range":{"available_kwh":{"gte":100}}}]}}}'
```

Expect `battery-101` and `battery-102`, total `2`, with complete sources; hit order is immaterial. The 100 kWh boundary is inclusive. Count operators over exactly that shortlist:

```sh
curl -fsS -X POST "$ES/field_note_energy/_search" \
  -H 'Content-Type: application/json' -d '{"size":0,"query":{"bool":{"filter":[{"term":{"region":"north"}},{"term":{"state":"ready"}},{"range":{"available_kwh":{"gte":100}}}]}},"aggs":{"operators":{"terms":{"field":"operator","size":10}}}}'
```

Expect `Aster: 1` and `Birch: 1`, with no hits. Now place Moor Lane on hold without changing its reported capacity:

```sh
curl -fsS -X POST "$ES/field_note_energy/_update/battery-101?refresh=wait_for" \
  -H 'Content-Type: application/json' -d '{"doc":{"state":"hold"}}'
sleep 5
curl -fsS "$ES/field_note_energy/_doc/battery-101"
```

The update returns `updated`; the read shows `state: "hold"` and preserves `site`, `region`, `operator`, and `available_kwh: 120`. Repeat both search requests: only `battery-102` remains, total `1`, with `Birch: 1`. A held site's sufficient energy must not keep it eligible. These snapshots support planning; they do not issue dispatch commands.

Boundary: atomic concurrent energy reservations remain [unsupported]({{ '/compatibility/' | relative_url }}).

The [fixture](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/fixtures/energy.json) and [replay](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/energy.py) check statuses, bulk IDs/results, every source, exact search IDs/totals, operator counts, and hold exclusion. The [recorded run](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/energy-2026-10-17.json) passed **105/105** checks on Elasticsearch **8.15.0**, OpenSearch **3.9.0**, and gateway **0.2.0** with Qdrant **1.15.5**, using identical requests and embedded sources with synchronous writes. No OpenSearch-specific behavior or difference in these assertions was observed. The [article curl run](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/energy-curl-2026-10-17.json) independently checked all nine responses per engine. No gateway code change or performance claim is involved.

Remove the example index when finished:

```sh
curl -fsS -X DELETE "$ES/field_note_energy"
```
