# Which dry plots need an irrigation inspection?

A market garden's morning queue should include dry plots with recent sensor readings. An old low reading is a reason to check the sensor, not evidence to irrigate. These six synthetic snapshots deliberately include a threshold reading, stale data, a closed plot, and a missing reading age. The cutoffs are illustrative, not agronomic advice.

Use fresh `field_note_irrigation` indexes on running Elasticsearch, OpenSearch, and gateway endpoints. Curl is the same client throughout: change only `ES` to select the server. Create the mapping and load the snapshots:

```sh
ES=http://localhost:19200
curl -fsS -X PUT "$ES/field_note_irrigation" \
  -H 'Content-Type: application/json' -d '{"mappings":{"properties":{"plot":{"type":"keyword"},"crop":{"type":"keyword"},"state":{"type":"keyword"},"moisture_percent":{"type":"integer"},"reading_age_minutes":{"type":"integer"}}}}'
curl -fsS -X POST "$ES/field_note_irrigation/_bulk?refresh=wait_for" \
  -H 'Content-Type: application/x-ndjson' --data-binary @- <<'NDJSON'
{"index":{"_id":"plot-101"}}
{"plot":"North tunnel","crop":"tomato","state":"open","moisture_percent":18,"reading_age_minutes":10}
{"index":{"_id":"plot-102"}}
{"plot":"East beds","crop":"lettuce","state":"open","moisture_percent":19,"reading_age_minutes":30}
{"index":{"_id":"plot-103"}}
{"plot":"West beds","crop":"lettuce","state":"open","moisture_percent":20,"reading_age_minutes":5}
{"index":{"_id":"plot-104"}}
{"plot":"South tunnel","crop":"tomato","state":"open","moisture_percent":12,"reading_age_minutes":31}
{"index":{"_id":"plot-105"}}
{"plot":"Trial beds","crop":"lettuce","state":"closed","moisture_percent":15,"reading_age_minutes":8}
{"index":{"_id":"plot-106"}}
{"plot":"Herb beds","crop":"basil","state":"open","moisture_percent":16}
NDJSON
sleep 5
```

The same five-second pause follows each write phase on all three servers; this is a settling allowance, not a refresh guarantee. Reading ages are fixed snapshots for a deterministic example; a real ingestion process must refresh them.

Find open plots below 20% moisture with a reading no more than 30 minutes old:

```sh
curl -fsS -X POST "$ES/field_note_irrigation/_search" \
  -H 'Content-Type: application/json' -d '{"size":10,"query":{"bool":{"filter":[{"term":{"state":"open"}},{"range":{"moisture_percent":{"lt":20}}},{"range":{"reading_age_minutes":{"lte":30}}}]}}}'
```

Expect exactly `plot-101` and `plot-102`, complete sources, and total `2`; order does not matter. The 20% boundary, 31-minute reading, closed plot, and missing age must all be excluded. Count crops within that same queue:

```sh
curl -fsS -X POST "$ES/field_note_irrigation/_search" \
  -H 'Content-Type: application/json' -d '{"size":0,"query":{"bool":{"filter":[{"term":{"state":"open"}},{"range":{"moisture_percent":{"lt":20}}},{"range":{"reading_age_minutes":{"lte":30}}}]}},"aggs":{"crops":{"terms":{"field":"crop","size":10}}}}'
```

Expect `tomato: 1` and `lettuce: 1`, with no hits. These are plot counts, not water requirements. After inspecting the North tunnel, remove it from the queue without overwriting its sensor snapshot:

```sh
curl -fsS -X POST "$ES/field_note_irrigation/_update/plot-101?refresh=wait_for" \
  -H 'Content-Type: application/json' -d '{"doc":{"state":"inspected"}}'
sleep 5
curl -fsS -X GET "$ES/field_note_irrigation/_doc/plot-101"
```

The update returns `updated`. The read retains the plot, crop, 18% moisture, and 10-minute reading age, with `state: "inspected"`. Repeat both search requests: only `plot-102` remains, total `1`, and the only bucket is `lettuce: 1`.

The [fixture](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/fixtures/agriculture.json), [replay](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/agriculture.py), and [recorded evidence](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/agriculture-2026-10-19.json) verify statuses, bulk IDs/results, every source, exact search IDs/totals, crop counts, and inspection exclusion. The run passed 105/105 checks on native Elasticsearch 8.15.0, native OpenSearch 3.9.0, and gateway 0.2.0 with Qdrant 1.15.5, using identical requests and embedded sources with synchronous writes. No OpenSearch-specific behavior or assertion difference was observed. The article's curl requests were also executed on all three endpoints. No gateway change or performance claim is involved.

Boundary: [atomic concurrent updates](https://dstockton.github.io/qdrant-es-gateway/compatibility/) are unsupported; this queue does not control irrigation equipment.

Remove the example index:

```sh
curl -fsS -X DELETE "$ES/field_note_irrigation"
```
