---
layout: post
title: "An evening class with room for one more"
date: 2026-10-05
categories: [field-notes]
---

An adult learner wants an evening Python class in York. A full course, a daytime class, or a place in Leeds is no help. These six synthetic course listings test that distinction, including the moment the last introductory place disappears.

Start with a running Elasticsearch-compatible endpoint and a fresh `field_note_courses` index. The client is curl; run the identical commands on each server, changing only `ES`: `http://localhost:19200` for Elasticsearch, `http://localhost:9200` for the gateway.

```sh
ES=http://localhost:19200
curl -fsS -X PUT "$ES/field_note_courses" \
  -H 'Content-Type: application/json' -d '{"mappings":{"properties":{"title":{"type":"text"},"campus":{"type":"keyword"},"session":{"type":"keyword"},"level":{"type":"keyword"},"places":{"type":"integer"}}}}'

curl -fsS -X POST "$ES/field_note_courses/_bulk?refresh=wait_for" \
  -H 'Content-Type: application/x-ndjson' --data-binary @- <<'NDJSON'
{"index":{"_id":"course-101"}}
{"title":"Python for beginners","campus":"York","session":"evening","level":"introductory","places":1}
{"index":{"_id":"course-102"}}
{"title":"Python data analysis","campus":"York","session":"evening","level":"intermediate","places":4}
{"index":{"_id":"course-103"}}
{"title":"Python automation","campus":"York","session":"evening","level":"intermediate","places":0}
{"index":{"_id":"course-104"}}
{"title":"Python for beginners","campus":"York","session":"daytime","level":"introductory","places":8}
{"index":{"_id":"course-105"}}
{"title":"Python for beginners","campus":"Leeds","session":"evening","level":"introductory","places":6}
{"index":{"_id":"course-106"}}
{"title":"Watercolour basics","campus":"York","session":"evening","level":"introductory","places":3}
NDJSON
sleep 5
```

Both runs use the same five-second settling pause. Find Python courses with at least one place, at the right campus and time:

```sh
curl -fsS -X POST "$ES/field_note_courses/_search" \
  -H 'Content-Type: application/json' -d '{"size":10,"query":{"bool":{"must":{"match":{"title":"python"}},"filter":[{"term":{"campus":"York"}},{"term":{"session":"evening"}},{"range":{"places":{"gte":1}}}]}}}'
```

Expect exactly `course-101` and `course-102`, with complete sources; order and scores are not asserted. The full course, daytime class, Leeds class, and watercolour class must stay out.

Separately, count **all available evening classes in York** by level, including watercolour:

```sh
curl -fsS -X POST "$ES/field_note_courses/_search" \
  -H 'Content-Type: application/json' -d '{"size":0,"query":{"bool":{"filter":[{"term":{"campus":"York"}},{"term":{"session":"evening"}},{"range":{"places":{"gte":1}}}]}},"aggs":{"levels":{"terms":{"field":"level","size":10}}}}'
```

Expect `introductory: 2`, `intermediate: 1`, and no hits (`size: 0`). These are course counts, not a sum of available places.

After the enrolment system reports that the last introductory place is taken, publish its new availability and read the listing back:

```sh
curl -fsS -X POST "$ES/field_note_courses/_update/course-101?refresh=wait_for" \
  -H 'Content-Type: application/json' -d '{"doc":{"places":0}}'
sleep 5
curl -fsS "$ES/field_note_courses/_doc/course-101"
```

The update returns `updated`; the read keeps the title, campus, session, and level, with `places: 0`. Repeat both search requests: only `course-102` remains, and both level buckets are `1` (watercolour still has places). Discovery reflects availability while the full course remains readable by ID.

The [replay](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/education.py) independently checks statuses, bulk results, every stored ID/source, matching IDs/sources, and level counts on both endpoints. The [recorded run](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/education-2026-10-05.json) passed 66/66 checks with identical requests on Elasticsearch 8.15.0 and gateway 0.2.0 backed by Qdrant 1.15.5. No gateway change or performance claim is involved.

Boundary: text-scoped terms facets, atomic seat reservation, Lucene score equivalence, and refresh timing guarantees are outside this example's [supported semantics]({{ '/compatibility/' | relative_url }}).

Remove the example index afterward:

```sh
curl -fsS -X DELETE "$ES/field_note_courses"
```
