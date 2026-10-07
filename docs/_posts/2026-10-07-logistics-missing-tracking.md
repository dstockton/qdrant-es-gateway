---
layout: post
title: "Packed, but where is the tracking number?"
date: 2026-10-07
categories: [field-notes]
---

A dispatch coordinator in York needs packed parcels that still lack a tracking number before the carrier arrives. These five synthetic shipments include an omitted field, an explicit `null`, an already labelled parcel, another depot, and a dispatched parcel. Adding tracking must remove only the newly labelled parcel from the exception list.

Start with a running Elasticsearch-compatible endpoint and an unused `field_note_parcels` index. The client is curl; run exactly the same commands on each server, changing only `ES` (`http://localhost:19200` for Elasticsearch, `http://localhost:9200` for the gateway).

```sh
ES=http://localhost:19200
curl -fsS -X PUT "$ES/field_note_parcels" \
  -H 'Content-Type: application/json' -d '{"mappings":{"properties":{"contents":{"type":"keyword"},"depot":{"type":"keyword"},"carrier":{"type":"keyword"},"status":{"type":"keyword"},"tracking":{"type":"keyword"}}}}'

curl -fsS -X POST "$ES/field_note_parcels/_bulk?refresh=wait_for" \
  -H 'Content-Type: application/x-ndjson' --data-binary @- <<'NDJSON'
{"index":{"_id":"parcel-101"}}
{"contents":"Ceramic mugs","depot":"York","carrier":"Parcel North","status":"packed"}
{"index":{"_id":"parcel-102"}}
{"contents":"Cotton towels","depot":"York","carrier":"City Express","status":"packed","tracking":null}
{"index":{"_id":"parcel-103"}}
{"contents":"Desk lamps","depot":"York","carrier":"Parcel North","status":"packed","tracking":"PN-103"}
{"index":{"_id":"parcel-104"}}
{"contents":"Plant pots","depot":"Leeds","carrier":"Parcel North","status":"packed"}
{"index":{"_id":"parcel-105"}}
{"contents":"Wool blankets","depot":"York","carrier":"City Express","status":"dispatched","tracking":"CE-105"}
NDJSON
sleep 5
```

Both servers get the same five-second settling pause after each write phase. Search the packed York parcels with no indexed tracking value:

```sh
curl -fsS -X POST "$ES/field_note_parcels/_search" \
  -H 'Content-Type: application/json' -d '{"size":10,"query":{"bool":{"filter":[{"term":{"depot":"York"}},{"term":{"status":"packed"}}],"must_not":[{"exists":{"field":"tracking"}}]}}}'
```

Expect exactly `parcel-101` and `parcel-102`, with their complete sources and a total of `2`. An omitted field and JSON `null` both fail `exists`; an empty string would count as present, so ingest unknown tracking as `null` or omit it.

Count that same exception list by carrier:

```sh
curl -fsS -X POST "$ES/field_note_parcels/_search" \
  -H 'Content-Type: application/json' -d '{"size":0,"query":{"bool":{"filter":[{"term":{"depot":"York"}},{"term":{"status":"packed"}}],"must_not":[{"exists":{"field":"tracking"}}]}},"aggs":{"carriers":{"terms":{"field":"carrier","size":10}}}}'
```

Expect `Parcel North: 1` and `City Express: 1`, with no hits because `size` is zero. Assign the first parcel's tracking number and read it back:

```sh
curl -fsS -X POST "$ES/field_note_parcels/_update/parcel-101?refresh=wait_for" \
  -H 'Content-Type: application/json' -d '{"doc":{"tracking":"PN-101"}}'
sleep 5
curl -fsS "$ES/field_note_parcels/_doc/parcel-101"
```

The update returns `updated`. The read must preserve `Ceramic mugs`, York, Parcel North, and `status: packed`, adding only `tracking: PN-101`. Repeat the search and facet commands: only `parcel-102` remains, the total is `1`, and only `City Express: 1` remains in the buckets. This records a label assignment, not proof of carrier collection.

The [fixture](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/fixtures/logistics.json), [replay](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/logistics.py), and [recorded run](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/logistics-2026-10-07.json) verify identical requests, HTTP and bulk statuses, IDs, every source, totals, and carrier counts. Elasticsearch 8.15.0 and gateway 0.2.0 at `7e56704`, backed by Qdrant 1.15.5 in default embedded-source mode with synchronous writes, passed **66/66 checks**. No gateway code changed; this five-record check makes no performance claim.

Boundary: refresh timing guarantees and atomic concurrent updates remain outside the gateway's [supported semantics]({{ '/compatibility/' | relative_url }}).

Remove only this example's index when finished:

```sh
curl -fsS -X DELETE "$ES/field_note_parcels"
```
