---
layout: post
title: "Two passengers, one cancelled departure"
date: 2026-10-11
categories: [field-notes]
---

A travel disruption desk needs departures from York with at least two seats, either direct to London or to Peterborough for an onward connection. Six synthetic services include a one-seat departure, a different origin, an unsuitable destination, and an already cancelled train. These are illustrative inventory snapshots, not live timetables.

Use running Elasticsearch-compatible servers and a fresh `field_note_travel` index. Curl is the client: run these commands on each server, changing only `ES` to the gateway URL (for example, `http://localhost:9200`).

```sh
ES=http://localhost:19200
curl -fsS -X PUT "$ES/field_note_travel" \
  -H 'Content-Type: application/json' -d '{"mappings":{"properties":{"title":{"type":"text"},"origin":{"type":"keyword"},"destination":{"type":"keyword"},"operator":{"type":"keyword"},"seats_available":{"type":"integer"},"status":{"type":"keyword"}}}}'
curl -fsS -X POST "$ES/field_note_travel/_bulk?refresh=wait_for" \
  -H 'Content-Type: application/x-ndjson' --data-binary @- <<'NDJSON'
{"index":{"_id":"service-101"}}
{"title":"York to London morning","origin":"York","destination":"London","operator":"North Rail","seats_available":4,"status":"scheduled"}
{"index":{"_id":"service-102"}}
{"title":"York to Peterborough connector","origin":"York","destination":"Peterborough","operator":"East Rail","seats_available":2,"status":"scheduled"}
{"index":{"_id":"service-103"}}
{"title":"York to London last seat","origin":"York","destination":"London","operator":"North Rail","seats_available":1,"status":"scheduled"}
{"index":{"_id":"service-104"}}
{"title":"Leeds to London morning","origin":"Leeds","destination":"London","operator":"East Rail","seats_available":8,"status":"scheduled"}
{"index":{"_id":"service-105"}}
{"title":"York to Edinburgh morning","origin":"York","destination":"Edinburgh","operator":"North Rail","seats_available":6,"status":"scheduled"}
{"index":{"_id":"service-106"}}
{"title":"York to London cancelled","origin":"York","destination":"London","operator":"East Rail","seats_available":9,"status":"cancelled"}
NDJSON
sleep 5
```

Both endpoints use the same five-second settling pause; this does not establish immediate gateway refresh visibility.

Find departures that can accommodate both passengers. `terms` accepts either destination; all four filters must hold:

```sh
curl -fsS -X POST "$ES/field_note_travel/_search" \
  -H 'Content-Type: application/json' -d '{"size":10,"query":{"bool":{"filter":[{"term":{"origin":"York"}},{"terms":{"destination":["London","Peterborough"]}},{"term":{"status":"scheduled"}},{"range":{"seats_available":{"gte":2}}}]}}}'
```

Expect exactly `service-101` and `service-102`, with complete sources and total `2`; hit order is immaterial. Count operators over that same eligible set:

```sh
curl -fsS -X POST "$ES/field_note_travel/_search" \
  -H 'Content-Type: application/json' -d '{"size":0,"query":{"bool":{"filter":[{"term":{"origin":"York"}},{"terms":{"destination":["London","Peterborough"]}},{"term":{"status":"scheduled"}},{"range":{"seats_available":{"gte":2}}}]}},"aggs":{"operators":{"terms":{"field":"operator","size":10}}}}'
```

Expect `North Rail: 1` and `East Rail: 1`, with no hits. Now cancel the direct departure:

```sh
curl -fsS -X POST "$ES/field_note_travel/_update/service-101?refresh=wait_for" \
  -H 'Content-Type: application/json' -d '{"doc":{"status":"cancelled"}}'
sleep 5
curl -fsS "$ES/field_note_travel/_doc/service-101"
```

The update returns `updated`. The read shows `status: "cancelled"`, preserving the title, origin, destination, operator, and four available seats. Repeat both search requests: only `service-102` remains, total `1`, and the sole operator bucket is `East Rail: 1`. Cancellation must exclude the service even when its seat count still looks sufficient. The booking system must separately validate the connection and reserve seats.

The [fixture](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/fixtures/travel.json) and [replay](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/travel.py) check statuses, bulk results, every stored ID/source, search totals, operator facets, and cancellation exclusion. The [recorded run](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/travel-2026-10-11.json) passed 70/70 checks on Elasticsearch 8.15.0 and gateway 0.2.0 with Qdrant 1.15.5, using identical requests and default embedded-source, synchronous writes. The curl blocks were also executed on both endpoints. No gateway code change or performance claim is involved.

Boundary: atomic seat reservations through concurrent updates remain [unsupported]({{ '/compatibility/' | relative_url }}).

Remove the example index when finished:

```sh
curl -fsS -X DELETE "$ES/field_note_travel"
```
