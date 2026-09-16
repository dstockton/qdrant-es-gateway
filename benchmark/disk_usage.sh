#!/usr/bin/env bash
set -euo pipefail

# Report on-disk bytes for the actual service volumes, not Docker image sizes.
# Run from benchmark/ while the stack is still up so the volume mounts resolve.
output="${1:-results/disk-usage.txt}"
mkdir -p "$(dirname "$output")"
: > "$output"

for service in elasticsearch qdrant gateway; do
  container="$(docker compose ps -q "$service")"
  if [[ -z "$container" ]]; then
    echo -e "$service\tcontainer-not-running" >> "$output"
    continue
  fi
  case "$service" in
    elasticsearch) destination="/usr/share/elasticsearch/data" ;;
    qdrant) destination="/qdrant/storage" ;;
    gateway) destination="/data" ;;
  esac
  source="$(docker inspect -f "{{range .Mounts}}{{if eq .Destination \"$destination\"}}{{.Source}}{{end}}{{end}}" "$container")"
  if [[ -n "$source" && -d "$source" ]]; then
    bytes="$(du -sk "$source" | awk '{print $1 * 1024}')"
    echo -e "$service\t${bytes}\t$source" >> "$output"
  else
    echo -e "$service\tstorage-path-unavailable\t$source" >> "$output"
  fi
done

cat "$output"
