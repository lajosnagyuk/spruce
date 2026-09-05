#!/bin/sh
set -eu

chart=${1:-deploy/helm/spruce}
base='--set image.repository=spruce --set image.tag=dev --set image.pullPolicy=Never --set tls.allowInsecureTransport=true --set gateway.allowInsecureClientTransport=true'

render() { # shellcheck disable=SC2086
  helm template spruce "$chart" $base "$@" >/dev/null
}
reject() {
  if render "$@" 2>/dev/null; then
    printf 'expected Helm render rejection: %s\n' "$*" >&2
    exit 1
  fi
}

render
render --set clusterDomain=corp.internal --set networkPolicy.dns.serviceName=coredns
custom_ports=$(helm template spruce "$chart" $base --set service.port=18080 --set gateway.service.port=18081)
printf '%s\n' "$custom_ports" | grep -q 'listen 18081;'
printf '%s\n' "$custom_ports" | grep -q 'containerPort: 18081'
printf '%s\n' "$custom_ports" | grep -q 'port: 18080'
printf '%s\n' "$custom_ports" | grep -q 'port: 18081'
reject --set replicaCount=0
reject --set gateway.replicaCount=0
reject --set service.port=0
reject --set config.cacheBytes=0
reject --set config.defaultTTL=0s
reject --set config.publishAdmissionBytes=0
reject --set config.publishAdmissionWait=0s
reject --set config.deliveryLagLimit=0s
reject --set config.goMemoryLimit=0MiB
reject --set-string config.streamMemoryBytes=262143
reject --set-string config.streamMemoryBytes=67108864
# Check the exact boundary and each budget one byte over it, independently of defaults.
render --set resources.limits.memory=320Mi --set config.goMemoryLimit=176MiB --set-string config.memorySafetyMarginBytes=150994944
reject --set resources.limits.memory=320Mi --set-string config.goMemoryLimit=184549377 --set-string config.memorySafetyMarginBytes=150994944
reject --set resources.limits.memory=320Mi --set config.goMemoryLimit=176MiB --set-string config.memorySafetyMarginBytes=150994945
render --set resources.limits.memory=512Mi --set config.goMemoryLimit=384MiB --set-string config.memorySafetyMarginBytes=134217728
render --set resources.limits.memory=300M --set config.goMemoryLimit=192MiB --set-string config.memorySafetyMarginBytes=67108864
render --set resources.limits.memory=0.5Gi --set config.goMemoryLimit=384MiB --set-string config.memorySafetyMarginBytes=67108864
reject --set resources.limits.memory=watts
reject --set replicaCount=3 --set podDisruptionBudget.maxUnavailable=3
reject --set gateway.replicaCount=2 --set gateway.podDisruptionBudget.maxUnavailable=2
reject --set replicaCount=1 --set podDisruptionBudget.maxUnavailable=1
reject --set gateway.replicaCount=1 --set gateway.podDisruptionBudget.maxUnavailable=1
reject --set networkPolicy.dns.namespaceSelector=null
reject --set auth.requireExistingSecret=true
render --set auth.requireExistingSecret=true --set auth.existingSecret=spruce-auth --set auth.allowMissingExistingSecretForRender=true

spread=$(helm template spruce "$chart" $base)
printf '%s\n' "$spread" | grep -q 'whenUnsatisfiable: DoNotSchedule'
printf '%s\n' "$spread" | grep -q 'map $arg_topic $stream_key'
printf '%s\n' "$spread" | grep -q 'map $uri $publish_key'
printf '%s\n' "$spread" | grep -q '(?<publish_topic>'
printf '%s\n' "$spread" | grep -q 'hash $stream_key consistent;'
printf '%s\n' "$spread" | grep -q 'hash $publish_key consistent;'

target=$(SPRUCE_RELEASE=platform SPRUCE_NAMESPACE=events SPRUCE_CLUSTER_DOMAIN=corp.internal SPRUCE_PRINT_TARGETS=1 sh scripts/k3s-rotate-tls.sh)
test "$target" = 'publish_url=https://platform-spruce-0.platform-spruce-headless.events.svc.corp.internal:8080'
