#!/usr/bin/env bash
set -euo pipefail

EXPORTER_URL=${EXPORTER_URL:-http://localhost:5556/metrics}
OUT=metrics.out

docker compose -f test/e2e/docker-compose.yml up -d

for i in {1..60}; do
  if curl -fsS $EXPORTER_URL -o $OUT; then break; fi
  sleep 5
done

test -s $OUT

grep -q '^jmx_exporter_build_info{' $OUT

count=$(grep -E '^cassandra_' $OUT | wc -l | tr -d ' ')
echo $count
if [ $count -lt ${MIN_EXPECTED:-50} ]; then exit 1; fi
