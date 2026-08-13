# Agent guide

## Purpose

Spruce is a fast, bounded, memory-only event bus. Optimize in this order: correctness
of bounded transmission, operator simplicity, latency, memory, CPU, and convenience.
Do not add durability, consensus, databases, or a control plane without changing the
product definition and architecture decisions.

## Repository map

- `internal/broker`: cache, replication, subscriptions, delivery, HTTP API, and metrics.
- `pkg/spruce`: first-party Go client.
- `clients/csharp/Spruce`: first-party .NET client.
- `cmd`: broker, examples, and benchmark tools.
- `deploy/helm/spruce`: production Helm chart and deployment defaults.
- `deploy/nginx.conf`: sticky topic/group stream routing for the local cluster.
- `scripts`: smoke and local multi-broker validation.

## Non-negotiable invariants

- No unbounded queue, cache, replay state, goroutine fan-out, or retained payload graph.
- Validate a complete batch before publishing any entry.
- Preserve opaque payload bytes exactly.
- Register pending delivery state before a consumer can ACK it.
- Retry only the original broadcast subscriber or consumer group.
- Preserve the original message expiry across retries.
- Never weaken TLS verification or accept an empty peer credential.
- Keep `/internal/*` authenticated and unreachable through the public gateway.
- A joining broker remains unready until bounded peer-cache bootstrap completes or its
  bootstrap deadline proves that no peer is available.
- Draining must withdraw readiness before its bounded grace interval begins.
- Basic auth must be documented and deployed only with TLS.
- A retrying publish requires both producer ID and idempotency key.
- Explicit-completion client APIs ACK only after the caller completes the envelope.
- Document best-effort semantics plainly; never claim durable at-least-once delivery.

## Development

```sh
make build
make test
make test-race
make csharp
make csharp-pack
make helm-lint
make helm-render >/dev/null
docker compose config --quiet
```

Run `make validate-local` when Docker is available. Add correctness tests for every
state-machine or resource-accounting change and benchmark hot-path changes rather than
assuming an optimization helps.

For Kubernetes changes, also run `scripts/k3s-hardening.sh`. It separately tests burst
throughput, rolling replacement, abrupt broker loss, scale-down, node drain, and final
recovery. Do not turn Spruce's documented loss envelope into a zero-loss claim.

## Security and repository hygiene

- Never commit `.env` files, credentials, certificates, keys, generated binaries, or
  package outputs.
- Test credentials must be obviously synthetic and confined to tests.
- Runtime secrets come from environment variables or Kubernetes Secrets.
- Keep public docs current-state, concise, and explicit about trade-offs.
