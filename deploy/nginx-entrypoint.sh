#!/bin/sh
set -eu
# Docker and Podman use different embedded DNS addresses. Read this container's
# resolver and refresh broker records after container restarts change their IPs.
SPRUCE_DNS_RESOLVER=${SPRUCE_DNS_RESOLVER:-$(awk '$1 == "nameserver" { print $2; exit }' /etc/resolv.conf)}
: "${SPRUCE_DNS_RESOLVER:?container DNS resolver is required}"
export SPRUCE_DNS_RESOLVER
envsubst '${SPRUCE_DNS_RESOLVER}' < /spruce-nginx.conf.template > /etc/nginx/nginx.conf
exec nginx -g 'daemon off;'
