#!/bin/sh
set -eu

context=${KUBE_CONTEXT:-$(kubectl config current-context)}
namespace=${SPRUCE_NAMESPACE:-spruce}
release=${SPRUCE_RELEASE:-spruce}
case "$release" in *spruce*) default_fullname=$release;; *) default_fullname=${release}-spruce;; esac
fullname=${SPRUCE_FULLNAME:-$default_fullname}
secret=${SPRUCE_AUTH_SECRET:-${fullname}-auth}
probe=spruce-rotation-probe

read_key() { kubectl --context "$context" -n "$namespace" get secret "$secret" -o "jsonpath={.data.$1}" | base64 -d; }
old_peer=$(read_key peer-token); old_client=$(read_key client-token); old_admin=$(read_key admin-token); cluster=$(read_key cluster-id)
new_peer=${SPRUCE_NEW_PEER_TOKEN:-$(openssl rand -hex 24)}
new_client=${SPRUCE_NEW_CLIENT_TOKEN:-$(openssl rand -hex 24)}
new_admin=${SPRUCE_NEW_ADMIN_TOKEN:-$(openssl rand -hex 24)}

apply_secret() {
  kubectl --context "$context" -n "$namespace" create secret generic "$secret" \
    --from-literal=peer-token="$1" --from-literal=client-token="$2" --from-literal=admin-token="$3" \
    --from-literal=cluster-id="$cluster" ${4:+--from-literal=previous-peer-token=$4} \
    ${5:+--from-literal=previous-client-token=$5} ${6:+--from-literal=previous-admin-token=$6} \
    --dry-run=client -o yaml | kubectl --context "$context" apply -f - >/dev/null
}
roll() {
  helm --kube-context "$context" -n "$namespace" upgrade "$release" deploy/helm/spruce --reuse-values \
    --set benchmark.enabled=false --set gateway.allowInsecureClientTransport=true --wait --timeout 8m >/dev/null
  i=0
  until kubectl --context "$context" -n "$namespace" exec "$probe" -- \
    curl -ksSf -o /dev/null -H "Authorization: Bearer $(read_key admin-token)" \
    "https://$fullname-0.$fullname-headless.$namespace.svc.cluster.local:8080/metrics"; do
    i=$((i + 1)); test "$i" -lt 30 || return 1; sleep 2
  done
}

kubectl --context "$context" -n "$namespace" delete pod "$probe" --ignore-not-found >/dev/null
kubectl --context "$context" -n "$namespace" run "$probe" --restart=Never \
  --image=curlimages/curl@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6 \
  --labels="app.kubernetes.io/name=spruce,app.kubernetes.io/instance=$release,app.kubernetes.io/component=test,spruce.io/client-access=true" \
  --command -- sleep 1800 >/dev/null
trap 'kubectl --context "$context" -n "$namespace" delete pod "$probe" --wait=false >/dev/null 2>&1 || true' EXIT INT TERM
kubectl --context "$context" -n "$namespace" wait pod/"$probe" --for=condition=Ready --timeout=120s >/dev/null
client() { kubectl --context "$context" -n "$namespace" exec "$probe" -- curl -sS -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $1" --data-binary rotation "http://$fullname:8080/v1/topics/rotation/messages"; }
admin() { kubectl --context "$context" -n "$namespace" exec "$probe" -- curl -ksS -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $1" "https://$fullname-admin:8080/metrics"; }

apply_secret "$old_peer" "$old_client" "$old_admin" "$new_peer" "$new_client" "$new_admin"; roll
test "$(client "$old_client")" = 202; test "$(client "$new_client")" = 202
test "$(admin "$old_admin")" = 200; test "$(admin "$new_admin")" = 200
apply_secret "$new_peer" "$new_client" "$new_admin" "$old_peer" "$old_client" "$old_admin"
(
  i=0
  while test "$i" -lt 60; do
    i=$((i + 1)); token=$old_client; test $((i % 2)) -eq 0 && token=$new_client
    test "$(client "$token")" = 202
    sleep 1
  done
) & traffic=$!
roll; wait "$traffic"
apply_secret "$new_peer" "$new_client" "$new_admin"; roll
test "$(client "$new_client")" = 202; test "$(client "$old_client")" = 401
test "$(admin "$new_admin")" = 200; test "$(admin "$old_admin")" = 401
for broker in $(seq 0 $(( $(kubectl --context "$context" -n "$namespace" get statefulset "$fullname" -o jsonpath='{.spec.replicas}') - 1 ))); do
  metrics=$(kubectl --context "$context" -n "$namespace" exec "$probe" -- curl -ksS -H "Authorization: Bearer $new_admin" "https://$fullname-$broker.$fullname-headless.$namespace.svc.cluster.local:8080/metrics")
  test "$(printf '%s\n' "$metrics" | awk '/^spruce_replication_errors_total / {print $2}')" = 0
  test "$(printf '%s\n' "$metrics" | awk '/^spruce_replication_dropped_messages_total / {print $2}')" = 0
done
printf 'credential_rotation=passed mixed_publishes=60 retired_credentials_rejected=true\n'
