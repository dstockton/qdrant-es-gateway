#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results
docker compose up -d --build elasticsearch qdrant gateway
docker compose run --rm runner
docker compose ps -q elasticsearch qdrant gateway | xargs docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}' > results/resources.txt
./disk_usage.sh results/disk-usage.txt
docker compose down
