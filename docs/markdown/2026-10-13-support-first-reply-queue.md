# Who is still waiting for a first reply?

A support lead needs open billing tickets with no replies and no assigned agent. Seven synthetic tickets include assigned, answered, closed, and technical cases that must stay out of the queue. Missing and null assignees qualify; an empty string counts as present, so producers should normalize blank assignments to null.

Use running Elasticsearch-compatible servers and a fresh `field_note_support` index. Curl is the client: run these commands on each server, changing only `ES` to the gateway URL (for example, `http://localhost:9200`).

```sh
ES=http://localhost:19200
curl -fsS -X PUT "$ES/field_note_support" \
  -H 'Content-Type: application/json' -d '{"mappings":{"properties":{"subject":{"type":"text"},"team":{"type":"keyword"},"status":{"type":"keyword"},"channel":{"type":"keyword"},"reply_count":{"type":"integer"},"assignee":{"type":"keyword"}}}}'
curl -fsS -X POST "$ES/field_note_support/_bulk?refresh=wait_for" \
  -H 'Content-Type: application/x-ndjson' --data-binary @- <<'NDJSON'
{"index":{"_id":"ticket-101"}}
{"subject":"Duplicate subscription charge","team":"billing","status":"open","channel":"email","reply_count":0}
{"index":{"_id":"ticket-102"}}
{"subject":"Invoice address correction","team":"billing","status":"open","channel":"chat","reply_count":0,"assignee":null}
{"index":{"_id":"ticket-103"}}
{"subject":"Refund progress","team":"billing","status":"open","channel":"email","reply_count":0,"assignee":"agent-7"}
{"index":{"_id":"ticket-104"}}
{"subject":"Receipt request","team":"billing","status":"open","channel":"chat","reply_count":1}
{"index":{"_id":"ticket-105"}}
{"subject":"Cannot sign in","team":"technical","status":"open","channel":"email","reply_count":0}
{"index":{"_id":"ticket-106"}}
{"subject":"Payment confirmation","team":"billing","status":"closed","channel":"email","reply_count":0}
{"index":{"_id":"ticket-107"}}
{"subject":"Missing invoice number","team":"billing","status":"open","channel":"email","reply_count":0,"assignee":""}
NDJSON
sleep 5
```

The same five-second settling pause follows writes on both endpoints; this does not establish immediate gateway refresh visibility. Find tickets needing an owner and a first reply:

```sh
curl -fsS -X POST "$ES/field_note_support/_search" \
  -H 'Content-Type: application/json' -d '{"size":10,"query":{"bool":{"filter":[{"term":{"team":"billing"}},{"term":{"status":"open"}},{"term":{"reply_count":0}}],"must_not":[{"exists":{"field":"assignee"}}]}}}'
```

Expect exactly `ticket-101` (duplicate charge) and `ticket-102` (address correction), their complete sources, and total `2`; hit order is immaterial. Count contact channels over this same queue:

```sh
curl -fsS -X POST "$ES/field_note_support/_search" \
  -H 'Content-Type: application/json' -d '{"size":0,"query":{"bool":{"filter":[{"term":{"team":"billing"}},{"term":{"status":"open"}},{"term":{"reply_count":0}}],"must_not":[{"exists":{"field":"assignee"}}]}},"aggs":{"channels":{"terms":{"field":"channel","size":10}}}}'
```

Expect `email: 1` and `chat: 1`, with no hits. Assign the duplicate-charge ticket:

```sh
curl -fsS -X POST "$ES/field_note_support/_update/ticket-101?refresh=wait_for" \
  -H 'Content-Type: application/json' -d '{"doc":{"assignee":"agent-12"}}'
sleep 5
curl -fsS "$ES/field_note_support/_doc/ticket-101"
```

The update returns `updated`. The read shows `assignee: "agent-12"` while preserving the subject, team, status, channel, and zero replies. Repeat both search requests: only `ticket-102` remains, total `1`, and the sole channel bucket is `chat: 1`. Assignment alone removes a ticket from the unowned queue even before anyone replies.

The [fixture](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/fixtures/support.json) and [replay](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/support.py) check statuses, bulk results, every stored ID/source, exact search totals, channel counts, and assignment exclusion. The [recorded run](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/evidence/support-2026-10-13.json) passed 74/74 checks on Elasticsearch 8.15.0 and gateway 0.2.0 with Qdrant 1.15.5, using identical requests and default embedded-source, synchronous writes. The curl blocks were also run on both endpoints. No gateway code change or performance claim is involved.

Boundary: updates are read-merge-write, so atomic concurrent ticket claiming remains [unsupported](https://dstockton.github.io/qdrant-es-gateway/compatibility/).

Remove the example index when finished:

```sh
curl -fsS -X DELETE "$ES/field_note_support"
```
