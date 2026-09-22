#!/usr/bin/env bash
# Slice 1 smoke test: bring the stack up, wait for health, hit both health endpoints, tear down.
set -euo pipefail
cd "$(dirname "$0")/.."

BACKEND_PORT="${BACKEND_PORT:-8000}"

cleanup() {
  docker compose down
}
trap cleanup EXIT

docker compose up --build -d

echo "Waiting for backend healthcheck..."
for _ in $(seq 1 60); do
  status=$(docker compose ps --format json backend | python3 -c "import json,sys; print(json.load(sys.stdin).get('Health',''))" 2>/dev/null || echo "")
  if [ "$status" = "healthy" ]; then
    break
  fi
  sleep 2
done

echo "--- docker compose ps ---"
docker compose ps

echo "--- GET /health ---"
curl -sf "http://localhost:${BACKEND_PORT}/health"
echo

echo "--- GET /health/ready ---"
curl -sf "http://localhost:${BACKEND_PORT}/health/ready"
echo

echo "Smoke test passed."
