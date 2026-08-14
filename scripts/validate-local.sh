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
	file=$1 pattern=$2 pid=$3 i=0
	until grep -q "$pattern" "$file" 2>/dev/null; do
		kill -0 "$pid" 2>/dev/null || return 1
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

status_field() {
	broker=$1 field=$2
	docker compose exec -T proxy wget -qO- "http://$broker:8080/v1/status" | sed -n "s/.*\"$field\":\([0-9][0-9]*\).*/\1/p"
}
cache_digest() {
	broker=$1
	docker compose exec -T proxy wget -qO- \
		--header="Spruce-Peer-Token: $SPRUCE_PEER_TOKEN" \
		--header="Spruce-Cluster-ID: $SPRUCE_CLUSTER_ID" \
		"http://$broker:8080/internal/cache-digest"
}
metric_value() {
	broker=$1 metric=$2
	docker compose exec -T proxy wget -qO- "http://$broker:8080/metrics" | awk -v metric="$metric" '$1 == metric { print $2 }'
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
DOTNET_ROLL_FORWARD=Major dotnet clients/csharp/Spruce.Example/bin/Debug/net8.0/Spruce.Example.dll consume "$cs_topic" >"$cs_out" 2>&1 & cs_pid=$!
sleep 2
kill -0 "$go_pid"
kill -0 "$cs_pid"

go_id=$(./bin/spruce-producer -server "$base" -topic "$cs_topic" -message go-to-csharp | tail -1)
cs_id=$(DOTNET_ROLL_FORWARD=Major dotnet clients/csharp/Spruce.Example/bin/Debug/net8.0/Spruce.Example.dll produce "$go_topic" csharp-to-go | tail -1)
wait_for_text "$go_out" 'csharp-to-go' "$go_pid"
wait_for_text "$cs_out" 'go-to-csharp' "$cs_pid"

for broker in broker1 broker2 broker3; do
  wait_for_message "$broker" "$go_id"
  wait_for_message "$broker" "$cs_id"
done

docker compose stop broker1 >/dev/null
sh scripts/smoke.sh >/dev/null
docker compose start broker1 >/dev/null
i=0
until docker compose exec -T proxy wget -qO/dev/null "http://broker1:8080/health/ready" 2>/dev/null; do
	i=$((i + 1)); test "$i" -lt 60 || exit 1
	sleep 0.5
done

drop1_before=$(metric_value broker1 spruce_replication_dropped_messages_total)
drop2_before=$(metric_value broker2 spruce_replication_dropped_messages_total)
drop3_before=$(metric_value broker3 spruce_replication_dropped_messages_total)
error1_before=$(metric_value broker1 spruce_replication_errors_total)
error2_before=$(metric_value broker2 spruce_replication_errors_total)
error3_before=$(metric_value broker3 spruce_replication_errors_total)

./bin/spruce-bench -server "$base" -n 10000 -size 256 -workers 8 -batch 128

i=0
while :; do
	m1=$(status_field broker1 messages); m2=$(status_field broker2 messages); m3=$(status_field broker3 messages)
	d1=$(cache_digest broker1); d2=$(cache_digest broker2); d3=$(cache_digest broker3)
	q1=$(status_field broker1 replication_queue_bytes); q2=$(status_field broker2 replication_queue_bytes); q3=$(status_field broker3 replication_queue_bytes)
	test -n "$m1" && test -n "$d1" && test "$m1" = "$m2" && test "$m2" = "$m3" && test "$d1" = "$d2" && test "$d2" = "$d3" && test "$q1" = 0 && test "$q2" = 0 && test "$q3" = 0 && break
	i=$((i + 1)); test "$i" -lt 80 || exit 1
	sleep 0.25
done
drop1_after=$(metric_value broker1 spruce_replication_dropped_messages_total)
drop2_after=$(metric_value broker2 spruce_replication_dropped_messages_total)
drop3_after=$(metric_value broker3 spruce_replication_dropped_messages_total)
error1_after=$(metric_value broker1 spruce_replication_errors_total)
error2_after=$(metric_value broker2 spruce_replication_errors_total)
error3_after=$(metric_value broker3 spruce_replication_errors_total)
test "$drop1_after" = "$drop1_before" && test "$drop2_after" = "$drop2_before" && test "$drop3_after" = "$drop3_before"
printf 'local validation passed: Go/C# interoperability, full replication, load-balanced broker loss, batch throughput, converged_messages=%s, cache_digest=%s, throughput_replication_error_delta=%s/%s/%s, throughput_replication_drop_delta=0/0/0\n' \
	"$m1" \
	"$d1" \
	"$((error1_after - error1_before))" \
	"$((error2_after - error2_before))" \
	"$((error3_after - error3_before))"
