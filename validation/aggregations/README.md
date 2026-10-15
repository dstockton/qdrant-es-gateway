# Complete aggregations and sorted pages

`compare.py` creates six documents and sends identical requests to Elasticsearch
and the gateway. It checks min/max, exact terms and other-bucket counts, filter
counts, London calendar days, global ordering, `search_after`, and `post_filter`.
It deletes and recreates only `gateway-aggregation-demo`; use disposable services.

Start Elasticsearch 8.15.0 and Qdrant 1.19.2 in containers, and the gateway with
`MAX_PAGE_SIZE=2`. The six records force three scroll pages. The Qdrant version is
explicit: this harness does not establish compatibility with every older version.

```sh
docker run --rm --name aggregation-es -p 19293:9200 \
  -e discovery.type=single-node -e xpack.security.enabled=false \
  -e 'ES_JAVA_OPTS=-Xms512m -Xmx512m' \
  docker.elastic.co/elasticsearch/elasticsearch:8.15.0
docker run --rm --name aggregation-qdrant -p 16393:6333 \
  qdrant/qdrant:v1.19.2
```

In another terminal, with the containers ready:

```sh
LISTEN_ADDR=127.0.0.1:19294 QDRANT_URL=http://127.0.0.1:16393 \
  METADATA_DB=aggregation-demo.db MAX_PAGE_SIZE=2 cargo run --release
```

Then:

```sh
python3 validation/aggregations/compare.py \
  --native http://localhost:19293 --gateway http://localhost:19294
```

The harness polls `_count` until writes become visible before comparing. The
current gateway refresh endpoint is a compatibility acknowledgement, not an
Elasticsearch refresh barrier. Output includes actual individual request timings;
the tiny fixture and single samples are correctness checks, not a throughput
benchmark. No writes run during the comparison. Scrolls do not provide a point in
time snapshot.

The deterministic mock-backed regression is also available without containers:

```sh
cargo test aggregation_scroll_visits_every_page_and_uses_search_projection_values
cargo test calendar_days_follow_dst_and_weeks_start_monday
```

It deliberately puts the cheapest document on the second page, verifies the
unchanged filter and continuation offset, then checks that sorting, cursor
pagination, and post-filtering find that page.
