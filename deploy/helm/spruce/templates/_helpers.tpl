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

{{- define "spruce.validate" -}}
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
