---
layout: post
title: "Is this coastal clip cleared for the UK edit?"
date: 2026-10-09
categories: [field-notes]
---

A media producer needs coastal footage under 31 seconds with UK clearance. A French-only clip, a long coastal walk, and an uncleared aerial should stay out of the shortlist. These six synthetic archive records use topic and territory arrays; matching either array means matching any element.

Use running Elasticsearch-compatible servers and a fresh `field_note_media` index. Curl is the client: execute the same commands against each server, changing only `ES` to the gateway URL (for example, `http://localhost:9200`).

```sh
ES=http://localhost:19200
curl -fsS -X PUT "$ES/field_note_media" \
  -H 'Content-Type: application/json' -d '{"mappings":{"properties":{"title":{"type":"text"},"topics":{"type":"keyword"},"territories":{"type":"keyword"},"format":{"type":"keyword"},"duration_seconds":{"type":"integer"}}}}'
curl -fsS -X POST "$ES/field_note_media/_bulk?refresh=wait_for" \
  -H 'Content-Type: application/x-ndjson' --data-binary @- <<'NDJSON'
{"index":{"_id":"clip-101"}}
{"title":"Whitby harbour at dawn","topics":["coast","harbour"],"territories":["GB","FR"],"format":"landscape","duration_seconds":20}
{"index":{"_id":"clip-102"}}
{"title":"Cornish surf close-up","topics":["coast","surf"],"territories":["GB"],"format":"portrait","duration_seconds":12}
{"index":{"_id":"clip-103"}}
{"title":"Brittany lighthouse","topics":["coast"],"territories":["FR"],"format":"landscape","duration_seconds":18}
{"index":{"_id":"clip-104"}}
{"title":"Northumberland coastal walk","topics":["coast","walking"],"territories":["GB"],"format":"landscape","duration_seconds":75}
{"index":{"_id":"clip-105"}}
{"title":"York market stalls","topics":["city","food"],"territories":["GB"],"format":"portrait","duration_seconds":15}
{"index":{"_id":"clip-106"}}
{"title":"Uncleared beach aerial","topics":["coast"],"territories":[],"format":"landscape","duration_seconds":25}
NDJSON
sleep 5
```

The same five-second settling pause is used on both endpoints; it does not establish immediate gateway refresh visibility.

Find clips with the `coast` topic, `GB` clearance, and at most 30 seconds of footage:

```sh
curl -fsS -X POST "$ES/field_note_media/_search" \
  -H 'Content-Type: application/json' -d '{"size":10,"query":{"bool":{"filter":[{"term":{"topics":"coast"}},{"term":{"territories":"GB"}},{"range":{"duration_seconds":{"lte":30}}}]}}}'
```

Expect exactly `clip-101` (Whitby harbour) and `clip-102` (Cornish surf), with their complete sources and total `2`. Hit order is immaterial. Count formats over that same filtered shortlist:

```sh
curl -fsS -X POST "$ES/field_note_media/_search" \
  -H 'Content-Type: application/json' -d '{"size":0,"query":{"bool":{"filter":[{"term":{"topics":"coast"}},{"term":{"territories":"GB"}},{"range":{"duration_seconds":{"lte":30}}}]}},"aggs":{"formats":{"terms":{"field":"format","size":10}}}}'
```

Expect `landscape: 1` and `portrait: 1`, with no hits. Now the archive removes UK clearance from the harbour clip, leaving France:

```sh
curl -fsS -X POST "$ES/field_note_media/_update/clip-101?refresh=wait_for" \
  -H 'Content-Type: application/json' -d '{"doc":{"territories":["FR"]}}'
sleep 5
curl -fsS "$ES/field_note_media/_doc/clip-101"
```

The update returns `updated`. The read must show `territories: ["FR"]`, preserving the title, both topics, landscape format, and 20-second duration. Repeat the two search requests above: only `clip-102` remains, total `1`, and the sole format bucket is `portrait: 1`. Replacing an array must remove the old clearance, not append to it. This is metadata retrieval; the archive remains responsible for clearance decisions.

The [fixture](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/fixtures/media.json) and [replay](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/media.py) check statuses, bulk results, every stored ID/source, exact search totals, format counts, and clearance removal. The [recorded run](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/media-2026-10-09.json) passed 70/70 checks on Elasticsearch 8.15.0 and gateway 0.2.0 with Qdrant 1.15.5, using identical requests and default embedded-source, synchronous writes. The curl blocks were also run on both endpoints. No gateway code change or performance claim is involved.

Boundary: concurrent atomic updates and Elasticsearch `nested` semantics for per-territory rights objects remain [unsupported]({{ '/compatibility/' | relative_url }}).

Remove this example's index when finished:

```sh
curl -fsS -X DELETE "$ES/field_note_media"
```
