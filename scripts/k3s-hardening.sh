#!/bin/sh
set -eu

context=${KUBE_CONTEXT:-spruce-dev}
namespace=${SPRUCE_NAMESPACE:-spruce-hardening}
release=${SPRUCE_RELEASE:-hardened}
image=${SPRUCE_TOOLS_IMAGE:-spruce:tools}
service="http://${release}-spruce.${namespace}.svc:8080"
run_case() {
  name=$1; shift
  kubectl --context "$context" -n "$namespace" delete job "$name" --ignore-not-found >/dev/null
  args=""
  for arg in "$@"; do args="$args\n            - \"$arg\""; done
  printf '%b\n' "apiVersion: batch/v1
kind: Job
metadata: {name: $name, namespace: $namespace}
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app.kubernetes.io/name: spruce-hardening-test
        spruce.io/client-access: 'true'
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      nodeSelector: {kubernetes.io/hostname: spruce-k3s-1}
      containers:
        - name: integration
          image: \"$image\"
          imagePullPolicy: Never
          command: [/spruce-integration]
          args:
            - -server
            - \"$service\"$args
            - -allow-insecure-credentials
          env:
            - name: SPRUCE_TOKEN
              valueFrom:
                secretKeyRef: {name: hardening-auth, key: client-token}" | kubectl --context "$context" apply -f - >/dev/null
  kubectl --context "$context" -n "$namespace" wait --for=condition=complete "job/$name" --timeout=540s >/dev/null
  kubectl --context "$context" -n "$namespace" logs "job/$name"
}

run_case spruce-baseline -topics 6 -messages 500 -producers 10 -broadcast-consumers 3 -group-consumers 5 -dedupe -max-missing 0 -max-duplicates 0 -timeout 90s

run_case spruce-rollout -topics 6 -messages 20 -producers 10 -broadcast-consumers 3 -group-consumers 5 -publish-rate 1 -pause-after 1 -pause-for 240s -ttl 10m -dedupe -max-missing 0 -max-duplicates 0 -timeout 480s &
traffic=$!
sleep 2
kubectl --context "$context" -n "$namespace" rollout restart statefulset/${release}-spruce >/dev/null
kubectl --context "$context" -n "$namespace" rollout status statefulset/${release}-spruce --timeout=180s >/dev/null
kubectl --context "$context" -n "$namespace" rollout restart deployment/${release}-spruce-gateway >/dev/null
kubectl --context "$context" -n "$namespace" rollout status deployment/${release}-spruce-gateway --timeout=180s >/dev/null
wait "$traffic"

run_case spruce-post-rollout -topics 6 -messages 500 -producers 10 -broadcast-consumers 3 -group-consumers 5 -dedupe -max-missing 0 -max-duplicates 0 -timeout 90s

run_case spruce-broker-kill -topics 6 -messages 20 -producers 10 -broadcast-consumers 3 -group-consumers 5 -publish-rate 1 -ttl 10m -dedupe -max-missing 0 -max-duplicates 0 -timeout 240s &
traffic=$!
sleep 2
kubectl --context "$context" -n "$namespace" delete pod ${release}-spruce-1 --wait=false >/dev/null
kubectl --context "$context" -n "$namespace" rollout status statefulset/${release}-spruce --timeout=180s >/dev/null
wait "$traffic"

run_case spruce-scale-down -topics 6 -messages 20 -producers 10 -broadcast-consumers 3 -group-consumers 5 -publish-rate 1 -pause-after 1 -pause-for 240s -ttl 10m -dedupe -max-missing 0 -max-duplicates 0 -timeout 480s &
traffic=$!
sleep 2
helm --kube-context "$context" -n "$namespace" upgrade "$release" deploy/helm/spruce --reuse-values --set replicaCount=1 --set benchmark.enabled=false --set gateway.allowInsecureClientTransport=true --wait --timeout 180s >/dev/null
wait "$traffic"
run_case spruce-scale-one -topics 3 -messages 500 -producers 4 -broadcast-consumers 2 -group-consumers 3 -dedupe -max-missing 0 -max-duplicates 0 -timeout 90s
helm --kube-context "$context" -n "$namespace" upgrade "$release" deploy/helm/spruce --reuse-values --set replicaCount=3 --set benchmark.enabled=false --set gateway.allowInsecureClientTransport=true --wait --timeout 180s >/dev/null

trap 'kubectl --context "$context" uncordon spruce-k3s-2 >/dev/null 2>&1 || true' EXIT INT TERM
run_case spruce-node-drain -topics 6 -messages 20 -producers 10 -broadcast-consumers 3 -group-consumers 5 -publish-rate 1 -ttl 10m -dedupe -max-missing 20 -max-duplicates 0 -timeout 240s &
traffic=$!
sleep 2
kubectl --context "$context" drain spruce-k3s-2 --ignore-daemonsets --delete-emptydir-data --force --timeout=180s >/dev/null
kubectl --context "$context" uncordon spruce-k3s-2 >/dev/null
wait "$traffic"
trap - EXIT INT TERM

run_case spruce-final -topics 6 -messages 500 -producers 10 -broadcast-consumers 3 -group-consumers 5 -dedupe -max-missing 0 -max-duplicates 0 -timeout 90s
