#!/usr/bin/env bash
set -euo pipefail
base="${GATEWAY_URL:-http://localhost:9200}"
index="${BENCH_INDEX:-bench}"
curl -fsS -X PUT "$base/$index" -H content-type:application/json -d '{"mappings":{"properties":{"title":{"type":"text"},"category":{"type":"keyword"},"price":{"type":"float"}}}}' >/dev/null || true
echo "Benchmark target: $base/$index"
echo "Use BENCH_INDEX, GATEWAY_URL, and an application-generated NDJSON corpus; record p50/p95/p99 externally."
time curl -fsS -X POST "$base/$index/_search" -H content-type:application/json -d '{"query":{"match":{"title":"wireless headphones"}},"size":10}' >/dev/null
