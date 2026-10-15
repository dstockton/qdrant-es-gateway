---
layout: post
title: "A reporting day is not always 24 hours"
date: 2026-10-15
categories: [compatibility]
---

A sales report grouped by London day should move its boundaries when the clocks
change. Adding 86,400 seconds to yesterday's boundary gets that wrong twice a year.

The gateway now supports a small, explicit `date_histogram` subset: calendar days,
Monday-starting weeks, months, and years in an IANA time zone. It converts each
timestamp into local calendar time and converts that calendar boundary back to UTC.

```json
{"size":0,"aggs":{"sales_days":{"date_histogram":{"field":"date","calendar_interval":"day","time_zone":"Europe/London","min_doc_count":1}}}}
```

The deterministic test measures 23 hours between the March 29 and March 30, 2026
London boundaries and 25 hours between October 25 and October 26. Documents with
multiple timestamps count once per bucket. Run:

```sh
cargo test calendar_days_follow_dst_and_weeks_start_monday
```

The [comparison fixture](https://github.com/dstockton/qdrant-es-gateway/tree/main/validation/aggregations)
contains dates on both sides of those transitions and sends identical requests to
Elasticsearch and the gateway. This is compatibility work for teams already using
Qdrant; there is no claimed speed advantage over a native calendar histogram.

Only nonempty buckets are supported, so `min_doc_count:1` is required. Custom
formats, fixed intervals, offsets, bounds, subaggregations, and nonexistent local
midnight boundaries fail explicitly. Input dates accept epoch milliseconds,
RFC3339 timestamps, and `YYYY-MM-DD` dates. Date-only input denotes midnight UTC.

Time-zone rules come from the compiled `chrono-tz` dependency. Keeping that data
current still matters: implementing a calendar reducer does not repair stale rules.
This does **not** claim to resolve the
[Graylog OpenSearch tzdata report](https://github.com/Graylog2/graylog2-server/issues/27755),
which describes "events near midnight land on the wrong day" under outdated rules.
That reported deployment needs separate reproduction with its exact engine and
updated zone data. The London checks exercise established DST rules, not those
reported Canadian and Moroccan rule changes.

The container replay against Elasticsearch 8.15.0 and Qdrant 1.19.2 passed all four cases: complete aggregations, post-filtered hits, the first sorted page, and its cursor continuation. The [captured results](https://github.com/dstockton/qdrant-es-gateway/blob/main/validation/aggregations/results.json) include actual request timings. They establish correctness for this fixture, not a speed advantage.
