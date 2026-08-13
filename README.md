# Spruce

Spruce is a small, leaderless, in-memory event bus for service-to-service glue where Kafka is operationally excessive and rare message loss is acceptable.

It provides opaque binary messages over HTTPS, N producers and consumers, broadcast and consumer-group delivery, bounded replay, Go and C# clients, Prometheus metrics, and a Kubernetes deployment.

## Delivery contract

- Delivery is at least once while a message remains in a replica's bounded cache.
- Messages are not persisted. Restarting every replica can lose all cached messages.
- ACK and NACK are replica-local by default; retry is best effort.
- Consumer groups deliver to one healthy member. Ungrouped subscribers receive broadcasts.
- Duplicate delivery is expected. First-party clients provide bounded client-side deduplication.
- Payloads are opaque bytes. Spruce does not inspect schemas or formats.

These are deliberate constraints, not missing features. They remove disks, consensus, leaders, partition management, and database operations from the hot path. See [ARCHITECTURE.md](ARCHITECTURE.md) for the decisions and consequences.

## Run locally

Requirements: Go, Docker with Compose, and `make`.

```sh
export SPRUCE_PEER_TOKEN="$(openssl rand -hex 32)"
export SPRUCE_CLUSTER_ID=local
make compose-up
make smoke
```

The Compose cluster exposes the HTTPS API at `https://localhost:8443`. Its development certificate is generated locally and must be trusted explicitly by clients.

Stop it with:

```sh
make compose-down
```

## HTTP API

Publish any bytes as the request body:

```sh
curl --fail --cacert .cache/tls/ca.crt \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @message.bin \
  https://localhost:8443/v1/topics/orders/messages
```

Consume a streaming response:

```text
GET /v1/topics/{topic}/stream?group={group}&after={message-id}
```

The stream uses length-delimited binary frames. ACK and NACK endpoints accept batched message IDs. The Go and C# clients handle framing, reconnects, acknowledgement batching, bounded concurrency, and optional deduplication.

## Kubernetes

The manifests under `deploy/kubernetes` deploy three interchangeable replicas behind Services. Every replica can publish, stream, replay, ACK, and NACK; there is no leader or quorum.

Create runtime credentials outside source control:

```sh
kubectl create namespace spruce
kubectl -n spruce create secret generic spruce-auth \
  --from-literal=peer-token="$(openssl rand -hex 32)" \
  --from-literal=cluster-id=production
kubectl -n spruce create secret tls spruce-tls \
  --cert=server.crt \
  --key=server.key
kubectl apply -k deploy/kubernetes
```

The supplied certificate must cover the Service DNS names used by clients and peers. Pin the container image tag in `deploy/kubernetes/kustomization.yaml` before production rollout; do not deploy mutable tags.

Recommended starting resources per replica:

```yaml
resources:
  requests:
    cpu: 25m
    memory: 32Mi
  limits:
    cpu: 500m
    memory: 256Mi
```

Tune cache, pending-delivery, subscriber, and replication limits together with the pod memory limit. Kubernetes limits are the final safety boundary; Spruce's internal byte limits protect the normal operating envelope.

## Clients

Go:

```sh
go get github.com/lajosnagyuk/spruce/pkg/spruce
```

C# source and packaging live under `clients/csharp`. The library exposes publish, batch publish, subscribe, ACK/NACK, retry, bounded handler concurrency, and deduplication primitives.

Both libraries use the public HTTPS API. They are conveniences, not protocol requirements.

## Operations

- `GET /healthz` reports process health.
- `GET /readyz` reports readiness.
- `GET /metrics` exposes Prometheus counters and gauges.
- Graceful shutdown stops admission, drains bounded work, and then exits.
- Cache pressure, expired messages, dropped replication, pending bytes, retries, and action-queue saturation are observable separately.

Run at least two replicas for availability and three for routine rolling maintenance. Because state is memory-only and loosely replicated, losing all replicas at once loses replay state.

## Build and verification

```sh
make build
make test
make test-race
make test-csharp
make image
```

Performance results and methodology are documented in [docs/PERFORMANCE.md](docs/PERFORMANCE.md). CI runs correctness, race, C# conformance, build, and Kubernetes render checks.

## Repository guide

- [ARCHITECTURE.md](ARCHITECTURE.md): implemented architectural decisions and tradeoffs.
- [AGENTS.md](AGENTS.md): contributor and coding-agent invariants.
- `cmd/spruce`: broker entry point.
- `internal/broker`: cache, routing, replay, replication, and metrics.
- `pkg/spruce`: Go client.
- `clients/csharp`: C# client and conformance tests.
- `deploy`: container-edge and Kubernetes configuration.

## License

MIT. See [LICENSE](LICENSE).
