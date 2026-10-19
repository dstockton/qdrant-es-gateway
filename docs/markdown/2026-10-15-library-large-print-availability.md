# Can I borrow the large-print edition today?

A public-library reader wants a large-print book available at York today. A standard edition of the same title, an ebook, a copy at Leeds, and a fully borrowed title should stay out. These six synthetic holdings represent one title/format/branch combination per document; no borrower data is needed.

Use running Elasticsearch-compatible servers and a fresh `field_note_library` index. Curl is the client: run these same commands against each server, changing only `ES` to the gateway URL (for example, `http://localhost:9200`).

```sh
ES=http://localhost:19200
curl -fsS -X PUT "$ES/field_note_library" \
  -H 'Content-Type: application/json' -d '{"mappings":{"properties":{"title":{"type":"text"},"branch":{"type":"keyword"},"format":{"type":"keyword"},"genre":{"type":"keyword"},"available_copies":{"type":"integer"}}}}'
curl -fsS -X POST "$ES/field_note_library/_bulk?refresh=wait_for" \
  -H 'Content-Type: application/x-ndjson' --data-binary @- <<'NDJSON'
{"index":{"_id":"book-101"}}
{"title":"The Harbour Mystery","branch":"York","format":"large-print","genre":"crime","available_copies":1}
{"index":{"_id":"book-102"}}
{"title":"A Garden in Winter","branch":"York","format":"large-print","genre":"fiction","available_copies":2}
{"index":{"_id":"book-103"}}
{"title":"The Harbour Mystery","branch":"York","format":"standard","genre":"crime","available_copies":3}
{"index":{"_id":"book-104"}}
{"title":"Mountain Letters","branch":"York","format":"large-print","genre":"biography","available_copies":0}
{"index":{"_id":"book-105"}}
{"title":"River Detectives","branch":"Leeds","format":"large-print","genre":"crime","available_copies":2}
{"index":{"_id":"book-106"}}
{"title":"Summer on the Moors","branch":"York","format":"ebook","genre":"fiction","available_copies":4}
NDJSON
sleep 5
```

The same five-second settling pause is used on both endpoints; it does not establish immediate gateway refresh visibility.

Find the available large-print holdings at York:

```sh
curl -fsS -X POST "$ES/field_note_library/_search" \
  -H 'Content-Type: application/json' -d '{"size":10,"query":{"bool":{"filter":[{"term":{"branch":"York"}},{"term":{"format":"large-print"}},{"range":{"available_copies":{"gte":1}}}]}}}'
```

Expect exactly `book-101` (The Harbour Mystery) and `book-102` (A Garden in Winter), total `2`, with their complete sources; hit order is immaterial. Count titles by genre over that same filtered selection:

```sh
curl -fsS -X POST "$ES/field_note_library/_search" \
  -H 'Content-Type: application/json' -d '{"size":0,"query":{"bool":{"filter":[{"term":{"branch":"York"}},{"term":{"format":"large-print"}},{"range":{"available_copies":{"gte":1}}}]}},"aggs":{"genres":{"terms":{"field":"genre","size":10}}}}'
```

Expect `crime: 1` and `fiction: 1`, with no hits. These buckets count holdings records, not copies: the two available copies of A Garden in Winter contribute one fiction record.

The lending system now reports that the final large-print copy of The Harbour Mystery has been borrowed. Publish its new availability and read the record back:

```sh
curl -fsS -X POST "$ES/field_note_library/_update/book-101?refresh=wait_for" \
  -H 'Content-Type: application/json' -d '{"doc":{"available_copies":0}}'
sleep 5
curl -fsS "$ES/field_note_library/_doc/book-101"
```

The update returns `updated`; the read retains the title, branch, format and genre, with `available_copies: 0`. Repeat both searches above: only `book-102` remains, total `1`, and the sole genre bucket is `fiction: 1`. The standard edition of The Harbour Mystery must not substitute for the borrowed large-print edition.

The [fixture](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/fixtures/library.json) and [replay](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/library.py) verify HTTP statuses, bulk IDs/results, every stored source, exact search totals, genre counts and last-copy exclusion. The [recorded run](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/library-2026-10-15.json) passed 70/70 checks on Elasticsearch 8.15.0 and gateway 0.2.0 with Qdrant 1.15.5, using identical requests, embedded sources and synchronous writes. The article's curl requests were also executed on both endpoints. No gateway code change or performance claim is involved.

Boundary: updates are not [atomic concurrent reservations](https://dstockton.github.io/qdrant-es-gateway/compatibility/); the lending system remains authoritative for loans.

Remove the example index when finished:

```sh
curl -fsS -X DELETE "$ES/field_note_library"
```
