#!/bin/sh
set -eu

context=${KUBE_CONTEXT:-$(kubectl config current-context)}
namespace=${SPRUCE_NAMESPACE:-spruce}
release=${SPRUCE_RELEASE:-spruce}
case "$release" in *spruce*) default_fullname=$release;; *) default_fullname=${release}-spruce;; esac
fullname=${SPRUCE_FULLNAME:-$default_fullname}
auth=${SPRUCE_AUTH_SECRET:-${fullname}-auth}
image=${SPRUCE_TOOLS_IMAGE:-spruce:tools}
duration=${SOAK_SECONDS:-7200}
report=${SOAK_REPORT:-/tmp/spruce-soak.csv}
messages=$((duration / 6))
test "$messages" -gt 0

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
        app.kubernetes.io/name: spruce-soak
        spruce.io/client-access: "true"
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      containers:
        - name: integration
          image: "$image"
          imagePullPolicy: Never
          command: [/spruce-integration]
          args: [-server, http://$fullname:8080, -topics, "6", -messages, "$messages", -producers, "10", -broadcast-consumers, "3", -group-consumers, "5", -publish-rate, "1", -ttl, "10m", -dedupe, -max-missing, "0", -max-duplicates, "0", -timeout, "$((duration + 180))s", -allow-insecure-credentials]
          env:
            - name: SPRUCE_TOKEN
              valueFrom:
                secretKeyRef: {name: $auth, key: client-token}
EOF

printf 'timestamp,pod,rss_mib,heap_bytes,goroutines,replication_errors,replication_drops,delivery_drops\n' >"$report"
initial_peak=0
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
    rss=$(kubectl --context "$context" top pod -n "$namespace" "$pod" --no-headers | awk '{gsub("Mi","",$3); print $3+0}')
    metrics=$(kubectl --context "$context" -n "$namespace" exec "$probe" -- wget -qO- --header="Authorization: Bearer $admin" --no-check-certificate "https://$pod.$fullname-headless.$namespace.svc.cluster.local:8080/metrics")
    metric() { printf '%s\n' "$metrics" | awk -v name="$1" '$1 == name {print $2}'; }
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$sampled_at" "$pod" "$rss" "$(metric spruce_process_heap_bytes)" "$(metric spruce_process_goroutines)" "$(metric spruce_replication_errors_total)" "$(metric spruce_replication_dropped_messages_total)" "$(metric spruce_delivery_dropped_total)" >>"$report"
    test "$rss" -le 240
    test "$(metric spruce_replication_errors_total)" = 0
    test "$(metric spruce_replication_dropped_messages_total)" = 0
    test "$(metric spruce_delivery_dropped_total)" = 0
    test "$rss" -le "$peak" || peak=$rss
    if test "$sample" -eq 0 && test "$rss" -gt "$initial_peak"; then initial_peak=$rss; fi
  done
  sample=$((sample + 1))
  sleep 60
done
kubectl --context "$context" -n "$namespace" wait --for=condition=complete job/spruce-continuous-soak --timeout=30s >/dev/null
kubectl --context "$context" -n "$namespace" logs job/spruce-continuous-soak
last_timestamp=$(tail -1 "$report" | cut -d, -f1)
final=$(awk -F, -v timestamp="$last_timestamp" '$1 == timestamp && $3>m {m=$3} END {print m+0}' "$report")
test "$final" -le $((initial_peak + 32))
printf 'soak_seconds=%s peak_rss_mib=%s initial_rss_mib=%s final_rss_mib=%s report=%s\n' "$duration" "$peak" "$initial_peak" "$final" "$report"
