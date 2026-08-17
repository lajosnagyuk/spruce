#!/bin/sh
set -eu

namespace=${SPRUCE_NAMESPACE:-spruce}; release=${SPRUCE_RELEASE:-spruce}
case "$release" in *spruce*) default_fullname=$release;; *) default_fullname=${release}-spruce;; esac
fullname=${SPRUCE_FULLNAME:-$default_fullname}
cluster_domain=${SPRUCE_CLUSTER_DOMAIN:-cluster.local}
extra_dns=${SPRUCE_TLS_EXTRA_DNS:-}
service_port=${SPRUCE_SERVICE_PORT:-8080}
if test "${SPRUCE_PRINT_TARGETS:-0}" = 1; then
  printf 'publish_url=https://%s-0.%s-headless.%s.svc.%s:%s\n' "$fullname" "$fullname" "$namespace" "$cluster_domain" "$service_port"
  exit 0
fi
context=${KUBE_CONTEXT:-$(kubectl config current-context)}
secret=${SPRUCE_TLS_SECRET:-${fullname}-tls}; auth=${SPRUCE_AUTH_SECRET:-${fullname}-auth}
work=${TMPDIR:-/tmp}/spruce-tls-rotation-$$
mkdir -m 700 "$work"
kubectl --context "$context" -n "$namespace" get secret "$secret" -o yaml >"$work/original-secret.yaml"
committed=0
cleanup() {
  if test "$committed" -ne 1; then
    kubectl --context "$context" apply -f "$work/original-secret.yaml" >/dev/null 2>&1 || true
    helm --kube-context "$context" -n "$namespace" upgrade "$release" deploy/helm/spruce --reuse-values --set benchmark.enabled=false --wait --timeout 8m >/dev/null 2>&1 || true
  fi
  find "$work" -type f -exec sh -c 'for f do : >"$f"; done' sh {} +
  find "$work" -type f -delete
  rmdir "$work" 2>/dev/null || true
}
trap cleanup EXIT INT TERM
decode() { kubectl --context "$context" -n "$namespace" get secret "$secret" -o "go-template={{index .data \"$1\"}}" | base64 -d >"$2"; }
decode tls.crt "$work/old.crt"; decode tls.key "$work/old.key"; decode ca.crt "$work/old-ca.crt"
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj '/CN=spruce-rotation-ca' -keyout "$work/new-ca.key" -out "$work/new-ca.crt" >/dev/null 2>&1
cat >"$work/leaf.cnf" <<EOF
[req]
distinguished_name=dn
req_extensions=ext
prompt=no
[dn]
CN=$fullname-headless.$namespace.svc.$cluster_domain
[ext]
subjectAltName=DNS:$fullname-headless.$namespace.svc.$cluster_domain,DNS:*.$fullname-headless.$namespace.svc.$cluster_domain${extra_dns:+,DNS:$extra_dns}
extendedKeyUsage=serverAuth
EOF
openssl req -new -newkey rsa:2048 -nodes -keyout "$work/new.key" -out "$work/new.csr" -config "$work/leaf.cnf" >/dev/null 2>&1
openssl x509 -req -in "$work/new.csr" -CA "$work/new-ca.crt" -CAkey "$work/new-ca.key" -CAcreateserial -days 30 -extensions ext -extfile "$work/leaf.cnf" -out "$work/new.crt" >/dev/null 2>&1
cat "$work/old-ca.crt" "$work/new-ca.crt" >"$work/bundle.crt"
apply_tls() {
  cert=$1 key=$2 ca=$3 manifest="$work/secret.yaml"
  test -s "$cert"; test -s "$key"; test -s "$ca"
  openssl x509 -in "$cert" -noout >/dev/null
  openssl pkey -in "$key" -noout >/dev/null
  openssl verify -CAfile "$ca" "$cert" >/dev/null
  kubectl --context "$context" -n "$namespace" create secret generic "$secret" \
    --from-file=tls.crt="$cert" --from-file=tls.key="$key" --from-file=ca.crt="$ca" \
    --dry-run=client -o yaml >"$manifest"
  test "$(awk '/  tls.crt:/{print length($2)}' "$manifest")" -gt 100
  test "$(awk '/  tls.key:/{print length($2)}' "$manifest")" -gt 100
  kubectl --context "$context" apply -f "$manifest" >/dev/null
}
roll() { helm --kube-context "$context" -n "$namespace" upgrade "$release" deploy/helm/spruce --reuse-values --set benchmark.enabled=false --wait --timeout 8m >/dev/null; }
token=$(kubectl --context "$context" -n "$namespace" get secret "$auth" -o jsonpath='{.data.client-token}' | base64 -d)
publish() {
  pod=$(kubectl --context "$context" -n "$namespace" get pod -l app.kubernetes.io/component=broker -o jsonpath='{.items[0].metadata.name}')
  url="https://$pod.$fullname-headless.$namespace.svc.$cluster_domain:$service_port/v1/topics/tls-rotation/messages"
  kubectl --context "$context" -n "$namespace" exec deploy/"$fullname-gateway" -- \
    wget -qO- --header="Authorization: Bearer $token" --ca-certificate=/tls/ca.crt --post-data=tls-rotation "$url" >/dev/null
}
apply_tls "$work/old.crt" "$work/old.key" "$work/bundle.crt"; roll; publish
apply_tls "$work/new.crt" "$work/new.key" "$work/bundle.crt"; roll; publish
apply_tls "$work/new.crt" "$work/new.key" "$work/new-ca.crt"; roll; publish
admin=$(kubectl --context "$context" -n "$namespace" get secret "$auth" -o jsonpath='{.data.admin-token}' | base64 -d)
for pod in $(kubectl --context "$context" -n "$namespace" get pod -l app.kubernetes.io/component=broker -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}'); do
  metrics=$(kubectl --context "$context" -n "$namespace" exec deploy/"$fullname-gateway" -- wget -qO- --header="Authorization: Bearer $admin" --ca-certificate=/tls/ca.crt "https://$pod.$fullname-headless.$namespace.svc.$cluster_domain:$service_port/metrics")
  test "$(printf '%s\n' "$metrics" | awk '/^spruce_replication_errors_total / {print $2}')" = 0
  test "$(printf '%s\n' "$metrics" | awk '/^spruce_replication_dropped_messages_total / {print $2}')" = 0
done
committed=1
printf 'tls_rotation=passed trust_bundle_stages=3 replication_errors=0 replication_drops=0\n'
