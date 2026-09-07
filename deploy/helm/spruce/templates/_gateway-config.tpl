{{- define "spruce.gatewayConfig" -}}
events { worker_connections 4096; }
http {
  access_log off;
  map $http_spruce_producer_id $spruce_has_producer { "" 0; default 1; }
  map $http_spruce_idempotency_key $spruce_has_operation { "" 0; default 1; }
  map "$spruce_has_producer:$spruce_has_operation:$uri" $spruce_retryable_publish {
    default 0;
    ~^1:1:/v1/topics/[A-Za-z0-9._-]+/messages$ 1;
  }
  resolver {{ .Values.networkPolicy.dns.serviceName }}.{{ .Values.networkPolicy.dns.namespace }}.svc.{{ .Values.clusterDomain }} valid=1s ipv6=off;
  {{- if .Values.tls.enabled }}
  proxy_ssl_trusted_certificate /tls/{{ .Values.tls.caKey }};
  proxy_ssl_verify on;
  proxy_ssl_server_name on;
  proxy_ssl_name {{ include "spruce.fullname" . }}-headless.{{ .Release.Namespace }}.svc.{{ .Values.clusterDomain }};
  {{- end }}
  map $http_spruce_delivery_affinity $scoped_completion { "" 0; default 1; }
  map $arg_topic $legacy_stream_key {
    "" "$remote_addr:$remote_port:$connection";
    default "$arg_topic:$arg_group";
  }
  map $http_spruce_delivery_affinity $stream_key {
    "" $legacy_stream_key;
    default $http_spruce_delivery_affinity;
  }
  map $uri $publish_key {
    ~^/v1/topics/(?<publish_topic>[A-Za-z0-9._-]+)/(messages|batches)$ $publish_topic;
    default "$uri";
  }
  upstream brokers {
    zone brokers 64k;
    least_conn;
    {{- range $i := until (int .Values.replicaCount) }}
    server {{ include "spruce.fullname" $ }}-{{ $i }}.{{ include "spruce.fullname" $ }}-headless.{{ $.Release.Namespace }}.svc.{{ $.Values.clusterDomain }}:{{ $.Values.service.port }} resolve fail_timeout=1s;
    {{- end }}
    keepalive 64;
  }
  upstream streams {
    zone streams 64k;
    hash $stream_key consistent;
    {{- range $i := until (int .Values.replicaCount) }}
    server {{ include "spruce.fullname" $ }}-{{ $i }}.{{ include "spruce.fullname" $ }}-headless.{{ $.Release.Namespace }}.svc.{{ $.Values.clusterDomain }}:{{ $.Values.service.port }} resolve fail_timeout=1s;
    {{- end }}
  }
  upstream topic_publishes {
    zone topic_publishes 64k;
    hash $publish_key consistent;
    {{- range $i := until (int .Values.replicaCount) }}
    server {{ include "spruce.fullname" $ }}-{{ $i }}.{{ include "spruce.fullname" $ }}-headless.{{ $.Release.Namespace }}.svc.{{ $.Values.clusterDomain }}:{{ $.Values.service.port }} resolve fail_timeout=1s;
    {{- end }}
    keepalive 64;
  }
  server {
    listen {{ .Values.gateway.service.port }};
    client_max_body_size 17m;
    location ~ ^/(internal/|metrics$|v1/status) { return 404; }
    location = /v1/subscriptions/stream {
      proxy_pass {{ include "spruce.scheme" . }}://streams;
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_buffering off;
      proxy_connect_timeout 250ms;
      proxy_read_timeout 30s;
      proxy_next_upstream error timeout http_502 http_503 http_504;
      proxy_next_upstream_tries 3;
    }
    location ~ ^/v1/topics/[A-Za-z0-9._-]+/(messages|batches)$ {
      error_page 418 = @retryable_publish;
      if ($spruce_retryable_publish) { return 418; }
      proxy_pass {{ include "spruce.scheme" . }}://topic_publishes;
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_buffering off;
      proxy_connect_timeout 250ms;
      proxy_read_timeout 1h;
      proxy_next_upstream error timeout http_502 http_503 http_504;
      proxy_next_upstream_tries 3;
    }
    location @retryable_publish {
      proxy_pass {{ include "spruce.scheme" . }}://topic_publishes;
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_buffering off;
      proxy_connect_timeout 250ms;
      proxy_read_timeout 1h;
      proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
      proxy_next_upstream_tries 3;
    }
    location ~ ^/v1/deliveries/(ack|nack)$ {
      error_page 418 = @scoped_completion;
      if ($scoped_completion) { return 418; }
      proxy_pass {{ include "spruce.scheme" . }}://brokers;
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_connect_timeout 250ms;
    }
    location @scoped_completion {
      proxy_pass {{ include "spruce.scheme" . }}://streams;
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_connect_timeout 250ms;
    }
    location / {
      proxy_pass {{ include "spruce.scheme" . }}://brokers;
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_buffering off;
      proxy_connect_timeout 250ms;
      proxy_read_timeout 1h;
      proxy_next_upstream error timeout http_502 http_503 http_504;
      proxy_next_upstream_tries 3;
    }
  }
}
{{- end -}}
