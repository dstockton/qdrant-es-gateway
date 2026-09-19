#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

ROOT_COMPOSE="$PWD/docker-compose.yml"
RESULTS_DIR="${RESULTS_DIR:-$PWD/results}"
DOCS="${DOCS:-5000}"
DURATION="${DURATION:-30}"
FAIL_AFTER="${FAIL_AFTER:-8}"
FAIL_FOR="${FAIL_FOR:-10}"
CONCURRENCY="${CONCURRENCY:-12}"
mkdir -p "$RESULTS_DIR"

cleanup() {
  docker compose -f "$ROOT_COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f "$ROOT_COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
docker compose -f "$ROOT_COMPOSE" up -d --build es1 es2 es3 es-lb
until curl -fsS http://localhost:19200/_cluster/health?wait_for_status=yellow >/dev/null; do sleep 2; done

for scenario in search insert update delete mixed; do
  python3 resilience.py --base http://localhost:19200 --index "resilience_es_${scenario}" --scenario "$scenario" --docs "$DOCS" --duration "$DURATION" --fail-after "$FAIL_AFTER" --fail-for "$FAIL_FOR" --concurrency "$CONCURRENCY" --compose-file "$ROOT_COMPOSE" --service es1 --output "$RESULTS_DIR/es_${scenario}.json"
  until curl -fsS http://localhost:19200/ >/dev/null; do sleep 2; done
done
docker compose -f "$ROOT_COMPOSE" down -v --remove-orphans >/dev/null

docker compose -f "$ROOT_COMPOSE" up -d --build qdrant1 qdrant2 qdrant3 qdrant-lb gateway1 gateway2 gateway3 gateway-lb
until curl -fsS http://localhost:19201/readyz >/dev/null; do sleep 2; done
for scenario in search insert update delete mixed; do
  python3 resilience.py --base http://localhost:19201 --index "resilience_gateway_${scenario}" --scenario "$scenario" --docs "$DOCS" --duration "$DURATION" --fail-after "$FAIL_AFTER" --fail-for "$FAIL_FOR" --concurrency "$CONCURRENCY" --compose-file "$ROOT_COMPOSE" --service qdrant1 --output "$RESULTS_DIR/gateway_${scenario}.json"
  until curl -fsS http://localhost:19201/readyz >/dev/null; do sleep 2; done
done

python3 - "$RESULTS_DIR" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
combined = {}
for path in sorted(root.glob("*.json")):
    combined[path.stem] = json.loads(path.read_text())
(root / "resilience.json").write_text(json.dumps(combined, indent=2) + "\n")
print(json.dumps(combined, indent=2))
PY
