---
layout: post
title: "The pump is fixed. Take it off the queue."
date: 2026-10-03
categories: [field-notes]
---

A maintenance planner at a Leeds factory needs open pump faults, not last week's completed inspections or another plant's backlog. These five synthetic work orders make that distinction testable. Closing a job should remove it from the queue while preserving its original notes.

Use a running Elasticsearch-compatible endpoint and a fresh `field_note_maintenance` index. The client below is curl: run the same commands for each server, changing only `ES` (`http://localhost:19200` for Elasticsearch or `http://localhost:9200` for the gateway).

```sh
ES=http://localhost:19200
curl -fsS -X PUT "$ES/field_note_maintenance" \
  -H 'Content-Type: application/json' -d '{"mappings":{"properties":{"title":{"type":"text"},"plant":{"type":"keyword"},"team":{"type":"keyword"},"status":{"type":"keyword"},"downtime_minutes":{"type":"integer"}}}}'

curl -fsS -X POST "$ES/field_note_maintenance/_bulk?refresh=wait_for" \
  -H 'Content-Type: application/x-ndjson' --data-binary @- <<'NDJSON'
{"index":{"_id":"wo-101"}}
{"title":"Pump seal leaking","plant":"Leeds","team":"mechanical","status":"open","downtime_minutes":45}
{"index":{"_id":"wo-102"}}
{"title":"Conveyor belt slipping","plant":"Leeds","team":"mechanical","status":"open","downtime_minutes":20}
{"index":{"_id":"wo-103"}}
{"title":"Pump motor overheating","plant":"Bristol","team":"electrical","status":"open","downtime_minutes":60}
{"index":{"_id":"wo-104"}}
{"title":"Pump inspection complete","plant":"Leeds","team":"mechanical","status":"closed","downtime_minutes":0}
{"index":{"_id":"wo-105"}}
{"title":"Sensor signal intermittent","plant":"Leeds","team":"electrical","status":"open","downtime_minutes":10}
NDJSON
sleep 5
```

The five-second pause is the same on both endpoints; this check does not establish immediate search visibility after a gateway refresh.

Find open pump jobs at Leeds:

```sh
curl -fsS -X POST "$ES/field_note_maintenance/_search" \
  -H 'Content-Type: application/json' -d '{"size":10,"query":{"bool":{"must":{"match":{"title":"pump"}},"filter":[{"term":{"plant":"Leeds"}},{"term":{"status":"open"}}]}}}'
```

Only `wo-101`, “Pump seal leaking”, should return, with its complete source. The Bristol pump fault and the completed Leeds inspection must stay out.

Count all open Leeds jobs by team, independently of the pump text search:

```sh
curl -fsS -X POST "$ES/field_note_maintenance/_search" \
  -H 'Content-Type: application/json' -d '{"size":0,"query":{"bool":{"filter":[{"term":{"plant":"Leeds"}},{"term":{"status":"open"}}]}},"aggs":{"teams":{"terms":{"field":"team","size":10}}}}'
```

Expect `mechanical: 2` and `electrical: 1`, with no hits because `size` is zero. Now close the leaking-pump job and read it back:

```sh
curl -fsS -X POST "$ES/field_note_maintenance/_update/wo-101?refresh=wait_for" \
  -H 'Content-Type: application/json' -d '{"doc":{"status":"closed"}}'
sleep 5
curl -fsS "$ES/field_note_maintenance/_doc/wo-101"
```

The update returns `updated`; the read retains the title, plant, team, and 45 minutes of downtime, with `status: closed`. Repeat the two search requests: the pump result is empty and both team buckets are now `1`. The history survives; the actionable queue shrinks.

The [deterministic replay](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/manufacturing.py) checks these requests against explicit expectations on each server, including every document's ID and source. The [recorded run](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/manufacturing-2026-10-03.json) used Elasticsearch 8.15.0 and gateway 0.1.3 backed by Qdrant 1.15.3; all 62 checks passed with identical application requests and no gateway code change. This is a five-document correctness check, with no performance claim.

Boundary: Lucene score equivalence, refresh timing guarantees, and atomic concurrent updates are outside this example's [supported semantics]({{ '/compatibility/' | relative_url }}).

To remove only this example's index afterward:

```sh
curl -fsS -X DELETE "$ES/field_note_maintenance"
```
