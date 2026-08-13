#!/bin/sh
set -eu

base=${SPRUCE_URL:-http://localhost:8080}
stamp=$$
go_topic="validate-go-$stamp"
cs_topic="validate-csharp-$stamp"
go_out=/tmp/spruce-go-consumer-$stamp.out
cs_out=/tmp/spruce-csharp-consumer-$stamp.out
go_pid=
cs_pid=

cleanup() {
	status=$?
	if test "$status" -ne 0; then
		printf '%s\n' '--- Go consumer failure output ---' >&2
		cat "$go_out" >&2 2>/dev/null || true
		printf '%s\n' '--- C# consumer failure output ---' >&2
		cat "$cs_out" >&2 2>/dev/null || true
		docker compose ps >&2 2>/dev/null || true
	fi
  test -z "$go_pid" || kill "$go_pid" 2>/dev/null || true
  test -z "$cs_pid" || kill "$cs_pid" 2>/dev/null || true
  rm -f "$go_out" "$cs_out"
  docker compose start broker1 >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

wait_for_text() {
	file=$1 pattern=$2 i=0
	until grep -q "$pattern" "$file" 2>/dev/null; do
		i=$((i + 1))
		test "$i" -lt 40 || return 1
		sleep 0.25
	done
}

wait_for_message() {
	broker=$1 id=$2 i=0
	until docker compose exec -T proxy wget -qO/dev/null "http://$broker:8080/v1/status/messages/$id" 2>/dev/null; do
		i=$((i + 1))
		test "$i" -lt 40 || return 1
		sleep 0.25
	done
}

docker compose up -d --build --force-recreate
i=0
until curl -fsS "$base/health/ready" >/dev/null 2>&1; do
  i=$((i + 1)); test "$i" -lt 30 || { docker compose logs; exit 1; }
  sleep 1
done

for broker in broker1 broker2 broker3; do
  docker compose exec -T proxy wget -qO- "http://$broker:8080/v1/status" | grep -q '"peers":2'
done

./bin/spruce-consumer -server "$base" -topic "$go_topic" >"$go_out" 2>&1 & go_pid=$!
dotnet clients/csharp/Spruce.Example/bin/Debug/net10.0/Spruce.Example.dll consume "$cs_topic" >"$cs_out" 2>&1 & cs_pid=$!
sleep 2

go_id=$(./bin/spruce-producer -server "$base" -topic "$cs_topic" -message go-to-csharp | tail -1)
cs_id=$(dotnet clients/csharp/Spruce.Example/bin/Debug/net10.0/Spruce.Example.dll produce "$go_topic" csharp-to-go | tail -1)
wait_for_text "$go_out" 'csharp-to-go'
wait_for_text "$cs_out" 'go-to-csharp'

for broker in broker1 broker2 broker3; do
  wait_for_message "$broker" "$go_id"
  wait_for_message "$broker" "$cs_id"
done

docker compose stop broker1 >/dev/null
sh scripts/smoke.sh >/dev/null
docker compose start broker1 >/dev/null

./bin/spruce-bench -server "$base" -n 10000 -size 256 -workers 8 -batch 128
printf '%s\n' 'local validation passed: Go/C# interoperability, full replication, load-balanced broker loss, and batch throughput'
