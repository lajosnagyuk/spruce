#!/bin/sh
set -eu

context=${KUBE_CONTEXT:-$(kubectl config current-context)}
namespace=${SPRUCE_NAMESPACE:-spruce}
release=${SPRUCE_RELEASE:-spruce}
image=${SPRUCE_CURVE_IMAGE:-}
rates=${SPRUCE_CURVE_RATES:-"5 10 15 20 25"}
seconds=${SPRUCE_CURVE_SECONDS:-60}
payload_bytes=${SPRUCE_CURVE_PAYLOAD_BYTES:-921600}
workers=${SPRUCE_CURVE_WORKERS:-4}
report=${SPRUCE_CURVE_REPORT:-/tmp/spruce-binary-curve.csv}
test -n "$image" || { printf '%s\n' 'SPRUCE_CURVE_IMAGE is required and must be available on every target node' >&2; exit 2; }

secret=$(kubectl --context "$context" -n "$namespace" get sts "$release" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="SPRUCE_CLIENT_TOKEN")].valueFrom.secretKeyRef.name}')
key=$(kubectl --context "$context" -n "$namespace" get sts "$release" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="SPRUCE_CLIENT_TOKEN")].valueFrom.secretKeyRef.key}')
baseline=$(mktemp)
trap 'rm -f "$baseline"' EXIT INT TERM
kubectl --context "$context" -n "$namespace" get pods -l app.kubernetes.io/component=broker -o jsonpath='{range .items[*]}{.metadata.name}{"="}{.status.containerStatuses[0].restartCount}{"\n"}{end}' >"$baseline"
printf 'requested_rate,offered_rate,accepted_rate,payload_bytes,seconds,exit_code,peak_sampled_rss_mib,restart_delta,result\n' >"$report"

for rate in $rates; do
  job="spruce-binary-curve-$rate"
  kubectl --context "$context" -n "$namespace" delete job "$job" --ignore-not-found >/dev/null
  cat <<EOF | kubectl --context "$context" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: {name: "$job", namespace: "$namespace"}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: $((seconds + 240))
  template:
    metadata:
      labels:
        app.kubernetes.io/name: spruce
        app.kubernetes.io/instance: "$release"
        app.kubernetes.io/component: test
        spruce.io/client-access: "true"
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      containers:
        - name: verifier
          image: "$image"
          imagePullPolicy: ${SPRUCE_CURVE_PULL_POLICY:-Never}
          command: [/spruce-binary-soak]
          args: [-server, "http://$release:8080", -seconds, "$seconds", -rate, "$rate", -workers, "$workers", -size, "$payload_bytes"]
          env:
            - name: SPRUCE_TOKEN
              valueFrom:
                secretKeyRef: {name: "$secret", key: "$key"}
          resources:
            requests: {cpu: 250m, memory: 128Mi}
            limits: {cpu: "2", memory: 512Mi}
EOF
  rc=0
  peak_rss=0
  terminal_at=$(( $(date +%s) + seconds + 240 ))
  while :; do
    succeeded=$(kubectl --context "$context" -n "$namespace" get job "$job" -o jsonpath='{.status.succeeded}')
    failed=$(kubectl --context "$context" -n "$namespace" get job "$job" -o jsonpath='{.status.failed}')
    if test "$succeeded" = 1; then
      break
    fi
    if test -n "$failed" && test "$failed" -gt 0; then
      rc=1
      break
    fi
    if test "$(date +%s)" -ge "$terminal_at"; then
      rc=1
      break
    fi
    for broker in $(kubectl --context "$context" -n "$namespace" get pods -l app.kubernetes.io/component=broker -o name); do
      rss=$(kubectl --context "$context" -n "$namespace" top "$broker" --no-headers 2>/dev/null | awk '{value=$3; sub(/Mi$/, "", value); print value+0}' || true)
      test -z "$rss" || test "$rss" -le "$peak_rss" || peak_rss=$rss
    done
    sleep 1
  done
  output=$(kubectl --context "$context" -n "$namespace" logs "job/$job" 2>&1 || true)
  printf '%s\n' "$output"
  restart_delta=0
  for pod in $(kubectl --context "$context" -n "$namespace" get pods -l app.kubernetes.io/component=broker -o name); do
    name=${pod#pod/}
    rss=$(kubectl --context "$context" -n "$namespace" top pod "$name" --no-headers 2>/dev/null | awk '{value=$3; sub(/Mi$/, "", value); print value+0}' || true)
    test -z "$rss" || test "$rss" -le "$peak_rss" || peak_rss=$rss
    before=$(awk -F= -v pod="$name" '$1 == pod {print $2}' "$baseline")
    now=$(kubectl --context "$context" -n "$namespace" get pod "$name" -o jsonpath='{.status.containerStatuses[0].restartCount}')
    restart_delta=$((restart_delta + now - before))
  done
  result=$(printf '%s' "$output" | tr '\n' ' ' | sed 's/,/;/g')
  offered_rate=$(printf '%s\n' "$output" | sed -n 's/.* offered_rate=\([^ ]*\).*/\1/p' | tail -1)
  accepted_rate=$(printf '%s\n' "$output" | sed -n 's/.* accepted_rate=\([^ ]*\).*/\1/p' | tail -1)
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$rate" "${offered_rate:-0}" "${accepted_rate:-0}" "$payload_bytes" "$seconds" "$rc" "$peak_rss" "$restart_delta" "$result" >>"$report"
  test "$rc" -eq 0
  if test "$restart_delta" -ne 0; then
    printf 'stopping curve: broker restart detected at rate=%s\n' "$rate" >&2
    exit 1
  fi
  sleep 15
done

printf 'binary_curve_report=%s\n' "$report"
