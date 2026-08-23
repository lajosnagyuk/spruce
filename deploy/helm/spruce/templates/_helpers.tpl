{{- define "spruce.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "spruce.authChecksum" -}}
{{- $name := include "spruce.secretName" . -}}
{{- if .Values.auth.existingSecret -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace $name -}}
{{- if $secret }}{{ toJson $secret.data | sha256sum }}{{ else }}{{ $name | sha256sum }}{{ end -}}
{{- else }}{{ toJson .Values.auth | sha256sum }}{{ end -}}
{{- end }}

{{- define "spruce.tlsChecksum" -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace .Values.tls.existingSecret -}}
{{- if $secret }}{{ toJson $secret.data | sha256sum }}{{ else }}{{ .Values.tls.existingSecret | sha256sum }}{{ end -}}
{{- end }}

{{- define "spruce.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := include "spruce.name" . }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "spruce.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "spruce.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "spruce.selectorLabels" -}}
app.kubernetes.io/name: {{ include "spruce.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "spruce.secretName" -}}
{{- default (printf "%s-auth" (include "spruce.fullname" .)) .Values.auth.existingSecret }}
{{- end }}

{{- define "spruce.scheme" -}}{{ ternary "https" "http" .Values.tls.enabled }}{{- end }}

{{- define "spruce.memoryBytes" -}}
{{- $value := toString . -}}
{{- if not (regexMatch "^[0-9]+([.][0-9]+)?(KiB|MiB|GiB|TiB|PiB|EiB|Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)?$" $value) -}}
{{- fail (printf "unsupported memory quantity %q; use bytes, decimal SI (for example 300M), or binary SI (for example 0.5Gi)" $value) -}}
{{- end -}}
{{- $number := float64 (regexFind "^[0-9]+([.][0-9]+)?" $value) -}}
{{- $suffix := regexFind "(KiB|MiB|GiB|TiB|PiB|EiB|Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)$" $value -}}
{{- $factors := dict "" 1.0 "K" 1000.0 "M" 1000000.0 "G" 1000000000.0 "T" 1000000000000.0 "P" 1000000000000000.0 "E" 1000000000000000000.0 "Ki" 1024.0 "KiB" 1024.0 "Mi" 1048576.0 "MiB" 1048576.0 "Gi" 1073741824.0 "GiB" 1073741824.0 "Ti" 1099511627776.0 "TiB" 1099511627776.0 "Pi" 1125899906842624.0 "PiB" 1125899906842624.0 "Ei" 1152921504606846976.0 "EiB" 1152921504606846976.0 -}}
{{- int64 (mulf $number (index $factors $suffix)) -}}
{{- end }}

{{- define "spruce.validate" -}}
{{- if lt (int .Values.replicaCount) 1 -}}{{- fail "replicaCount must be at least 1" -}}{{- end -}}
{{- if lt (int .Values.gateway.replicaCount) 1 -}}{{- fail "gateway.replicaCount must be at least 1" -}}{{- end -}}
{{- if or (lt (int .Values.service.port) 1) (gt (int .Values.service.port) 65535) -}}{{- fail "service.port must be between 1 and 65535" -}}{{- end -}}
{{- if or (lt (int .Values.gateway.service.port) 1) (gt (int .Values.gateway.service.port) 65535) -}}{{- fail "gateway.service.port must be between 1 and 65535" -}}{{- end -}}
{{- range $name, $duration := dict "config.defaultTTL" .Values.config.defaultTTL "config.maxTTL" .Values.config.maxTTL "config.ackDeadline" .Values.config.ackDeadline "config.drainDelay" .Values.config.drainDelay -}}
{{- if regexMatch "^([0]+(ns|us|µs|ms|s|m|h))+$" $duration -}}{{- fail (printf "%s must be greater than zero" $name) -}}{{- end -}}
{{- end -}}
{{- $memoryLimit := int64 (include "spruce.memoryBytes" .Values.resources.limits.memory) -}}
{{- $goMemoryLimit := int64 (include "spruce.memoryBytes" .Values.config.goMemoryLimit) -}}
{{- $memoryMargin := int64 .Values.config.memorySafetyMarginBytes -}}
{{- if gt (add $goMemoryLimit $memoryMargin) $memoryLimit -}}
{{- fail "config.goMemoryLimit plus config.memorySafetyMarginBytes must not exceed resources.limits.memory" -}}{{- end -}}
{{- $boundedMemory := add (int64 .Values.config.cacheBytes) (int64 .Values.config.replicationQueueBytes) (int64 .Values.config.actionQueueBytes) (int64 .Values.config.maxInflightBytes) (int64 .Values.config.publishAdmissionBytes) $memoryMargin -}}
{{- if gt $boundedMemory $memoryLimit -}}
{{- fail "cache, replication queue, action queue, inflight, publish admission, and memory safety margin budgets must fit resources.limits.memory" -}}{{- end -}}
{{- if and .Values.podDisruptionBudget.enabled (ge (int .Values.podDisruptionBudget.maxUnavailable) (int .Values.replicaCount)) -}}
{{- fail "podDisruptionBudget.maxUnavailable must be less than replicaCount" -}}{{- end -}}
{{- if and .Values.gateway.podDisruptionBudget.enabled (ge (int .Values.gateway.podDisruptionBudget.maxUnavailable) (int .Values.gateway.replicaCount)) -}}
{{- fail "gateway.podDisruptionBudget.maxUnavailable must be less than gateway.replicaCount" -}}{{- end -}}
{{- if and (not .Values.image.digest) (ne .Values.image.pullPolicy "Never") -}}
{{- fail "image.digest is required for deployable releases; mutable tags are allowed only with image.pullPolicy=Never for local development" -}}
{{- end -}}
{{- if and (not .Values.tls.enabled) (not .Values.tls.allowInsecureTransport) -}}
{{- fail "TLS is disabled; set tls.allowInsecureTransport=true only for isolated development clusters" -}}
{{- end -}}
{{- if and (not .Values.auth.allowAnonymous) (not .Values.gateway.allowInsecureClientTransport) (or (not .Values.ingress.enabled) (eq (len .Values.ingress.tls) 0)) -}}
{{- fail "authenticated client traffic requires ingress.enabled with TLS; set gateway.allowInsecureClientTransport=true only for isolated development clusters or a separately enforced service-mesh TLS policy" -}}
{{- end -}}
{{- if and (not .Values.auth.allowAnonymous) (not .Values.gateway.allowInsecureClientTransport) .Values.ingress.enabled -}}
{{- range .Values.ingress.tls }}{{- if or (not .secretName) (eq (len .hosts) 0) }}{{- fail "each authenticated ingress TLS entry requires secretName and at least one host" -}}{{- end }}{{- end -}}
{{- end -}}
{{- if not .Values.gateway.enabled -}}
{{- fail "gateway.enabled=false is unsupported because it bypasses public/admin/internal route separation" -}}
{{- end -}}
{{- if and .Values.auth.requireExistingSecret (not .Values.auth.existingSecret) -}}
{{- fail "auth.requireExistingSecret=true requires auth.existingSecret; generated credentials are unsafe for offline/GitOps rendering" -}}
{{- end -}}
{{- if and .Values.benchmark.enabled (or (not .Values.benchmark.image.repository) (and (not .Values.benchmark.image.digest) (not .Values.benchmark.image.tag))) -}}
{{- fail "benchmark.enabled requires a separately built benchmark.image (Docker target: tools)" -}}
{{- end -}}
{{- if and (not .Values.auth.allowAnonymous) .Values.auth.existingSecret -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace .Values.auth.existingSecret -}}
{{- if and (not $secret) (not .Values.auth.allowMissingExistingSecretForRender) -}}
{{- fail "auth.existingSecret does not exist; create it first or explicitly allow offline rendering" -}}
{{- end -}}
{{- if and $secret (or (not (index $secret.data "client-token")) (not (index $secret.data "admin-token")) (not (index $secret.data "peer-token")) (not (index $secret.data "cluster-id"))) -}}
{{- fail "auth.existingSecret must contain peer-token, cluster-id, client-token, and admin-token" -}}
{{- end -}}
{{- end -}}
{{- end }}
