#!/bin/sh
set -eu

context=${KUBE_CONTEXT:-$(kubectl config current-context)}
namespace=${SPRUCE_NAMESPACE:-spruce}
release=${SPRUCE_RELEASE:-spruce}
case "$release" in *spruce*) default_fullname=$release;; *) default_fullname=${release}-spruce;; esac
fullname=${SPRUCE_FULLNAME:-$default_fullname}
cluster_domain=${SPRUCE_CLUSTER_DOMAIN:-cluster.local}
auth=${SPRUCE_AUTH_SECRET:-${fullname}-auth}
tls_secret=${SPRUCE_TLS_SECRET:-${fullname}-tls}
service_port=${SPRUCE_SERVICE_PORT:-8080}
image=${SPRUCE_TOOLS_IMAGE:-}
test -n "$image" || { printf '%s\n' 'SPRUCE_TOOLS_IMAGE is required and must be pullable by every target node (prefer an immutable registry digest)' >&2; exit 2; }
duration=${SOAK_SECONDS:-7200}
ttl=${SOAK_TTL:-10m}
settle=${SOAK_SETTLE_SECONDS:-0}
internal_scheme=https
wget_tls_args=--ca-certificate=/tls/ca.crt
server=https://$fullname-0.$fullname-headless.$namespace.svc.$cluster_domain:$service_port
insecure_arg=
if test "${SPRUCE_ALLOW_INSECURE:-0}" = 1; then
  internal_scheme=http; wget_tls_args=; server=http://$fullname:$service_port; insecure_arg=', -allow-insecure-credentials'
fi
report=${SOAK_REPORT:-/tmp/spruce-soak.csv}
messages=$((duration / 6))
fill_messages=${SOAK_FILL_MESSAGES:-300000}
test "$messages" -gt 0

rss_mib() {
  bytes=$(kubectl --context "$context" -n "$namespace" exec "$1" -- cat /sys/fs/cgroup/memory.current)
  printf '%s\n' $(((bytes + 1048575) / 1048576))
}
initial_peak=0
for pod in $(kubectl --context "$context" -n "$namespace" get pod -l app.kubernetes.io/component=broker -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}'); do
  rss=$(rss_mib "$pod"); test "$rss" -le "$initial_peak" || initial_peak=$rss
done

kubectl --context "$context" -n "$namespace" delete job spruce-continuous-soak --ignore-not-found >/dev/null
cat <<EOF | kubectl --context "$context" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: {name: spruce-continuous-soak, namespace: $namespace}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: $((duration + 300))
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
        - name: integration
          image: "$image"
          imagePullPolicy: ${SPRUCE_TOOLS_PULL_POLICY:-IfNotPresent}
          command: [/spruce-integration]
          args: [-server, "$server", -topics, "6", -messages, "$messages", -producers, "10", -broadcast-consumers, "3", -group-consumers, "5", -publish-rate, "1", -ttl, "$ttl", -dedupe, -max-missing, "0", -max-duplicates, "0", -timeout, "$((duration + 180))s"$insecure_arg]
          env:
            - {name: SSL_CERT_FILE, value: /tls/ca.crt}
            - name: SPRUCE_TOKEN
              valueFrom:
                secretKeyRef: {name: $auth, key: client-token}
          volumeMounts: [{name: tls, mountPath: /tls, readOnly: true}]
        - name: cache-fill
          image: "$image"
          imagePullPolicy: ${SPRUCE_TOOLS_PULL_POLICY:-IfNotPresent}
          command: [/spruce-bench]
          args: [-server, "$server", -n, "$fill_messages", -size, "256", -workers, "16", -batch, "256"$insecure_arg]
          env:
            - {name: SSL_CERT_FILE, value: /tls/ca.crt}
            - name: SPRUCE_TOKEN
              valueFrom:
                secretKeyRef: {name: $auth, key: client-token}
          volumeMounts: [{name: tls, mountPath: /tls, readOnly: true}]
      volumes:
        - name: tls
          secret: {secretName: $tls_secret, optional: true}
EOF

printf 'timestamp,pod,rss_mib,heap_bytes,goroutines,replication_errors,replication_drops,delivery_drops\n' >"$report"
peak=0
sample=0
while :; do
  succeeded=$(kubectl --context "$context" -n "$namespace" get job spruce-continuous-soak -o jsonpath='{.status.succeeded}')
  failed=$(kubectl --context "$context" -n "$namespace" get job spruce-continuous-soak -o jsonpath='{.status.failed}')
  test "$succeeded" != 1 || break
  test -z "$failed" || test "$failed" = 0
  admin=$(kubectl --context "$context" -n "$namespace" get secret "$auth" -o jsonpath='{.data.admin-token}' | base64 -d)
  probe=$(kubectl --context "$context" -n "$namespace" get pod -l app.kubernetes.io/component=gateway -o jsonpath='{.items[0].metadata.name}')
  sampled_at=$(date -u +%FT%TZ)
  for pod in $(kubectl --context "$context" -n "$namespace" get pod -l app.kubernetes.io/component=broker -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}'); do
    rss=$(rss_mib "$pod")
    metrics=$(kubectl --context "$context" -n "$namespace" exec "$probe" -- wget -qO- --header="Authorization: Bearer $admin" $wget_tls_args "$internal_scheme://$pod.$fullname-headless.$namespace.svc.$cluster_domain:$service_port/metrics")
    metric() { printf '%s\n' "$metrics" | awk -v name="$1" '$1 == name {print $2}'; }
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$sampled_at" "$pod" "$rss" "$(metric spruce_process_heap_bytes)" "$(metric spruce_process_goroutines)" "$(metric spruce_replication_errors_total)" "$(metric spruce_replication_dropped_messages_total)" "$(metric spruce_delivery_dropped_total)" >>"$report"
    test "$rss" -le 240
    test "$(metric spruce_replication_errors_total)" = 0
    test "$(metric spruce_replication_dropped_messages_total)" = 0
    test "$(metric spruce_delivery_dropped_total)" = 0
    test "$rss" -le "$peak" || peak=$rss
  done
  sample=$((sample + 1))
  sleep 60
done
kubectl --context "$context" -n "$namespace" wait --for=condition=complete job/spruce-continuous-soak --timeout=30s >/dev/null
kubectl --context "$context" -n "$namespace" logs job/spruce-continuous-soak -c integration
last_timestamp=$(tail -1 "$report" | cut -d, -f1)
final=$(awk -F, -v timestamp="$last_timestamp" '$1 == timestamp && $3>m {m=$3} END {print m+0}' "$report")
if test "$settle" -gt 0; then
  sleep "$settle"
  final=0
  for pod in $(kubectl --context "$context" -n "$namespace" get pod -l app.kubernetes.io/component=broker -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}'); do
    rss=$(rss_mib "$pod"); test "$rss" -le "$final" || final=$rss
  done
  test "$final" -le $((initial_peak + 32))
fi
printf 'soak_seconds=%s peak_rss_mib=%s initial_rss_mib=%s final_rss_mib=%s report=%s\n' "$duration" "$peak" "$initial_peak" "$final" "$report"
