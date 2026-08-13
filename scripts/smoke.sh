#!/bin/sh
set -eu
base=${SPRUCE_URL:-http://localhost:8080}
curl -fsS --retry 5 --retry-all-errors --max-time 2 "$base/health/ready" >/dev/null
key="smoke-$(date +%s)-$$"
id=$(curl -fsS --retry 5 --retry-all-errors --max-time 2 -X POST -H 'Content-Type: application/octet-stream' -H 'Spruce-Producer-ID: smoke' -H "Spruce-Idempotency-Key: $key" --data-binary 'smoke-test' "$base/v1/topics/smoke/messages" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
test -n "$id"
curl -fsS --retry 5 --retry-all-errors --max-time 2 "$base/metrics" | grep -q 'spruce_publish_total'
printf 'published %s\n' "$id"
