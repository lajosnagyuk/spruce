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

The Compose cluster exposes an intentionally anonymous HTTP API at
`http://localhost:8080` for isolated local development only.

Stop it with:

```sh
make compose-down
```

## HTTP API

Publish any bytes as the request body:

```sh
curl --fail \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @message.bin \
  http://localhost:8080/v1/topics/orders/messages
```

## Go client

```go
client := spruce.New("https://spruce.example.com")
client.Token = os.Getenv("SPRUCE_TOKEN")

_, err := client.Publish(ctx, "orders", payload, spruce.PublishOptions{
	ContentType: "application/octet-stream",
})

batcher := spruce.NewProducerBatcher(client, spruce.BatcherOptions{})
defer batcher.Close(ctx)
_, err = batcher.Publish(ctx, "orders", payload, spruce.PublishOptions{})

err = client.Subscribe(ctx, spruce.SubscribeOptions{Topic: "orders", Group: "billing"},
	func(ctx context.Context, delivery spruce.Delivery) error {
		return handle(delivery.Payload) // nil ACKs; an error NACKs
	})
```

The producer batcher defaults to 256 messages, 1 MiB including framing, a 250 us
first-item timer, and bounded backpressure. `PublishRetry` requires a producer ID and
idempotency key. Use `spruce.NewDeduper` when duplicate handler execution is unsafe.

## C# client

```csharp
var client = new SpruceClient("https://spruce.example.com", token: token);
await client.PublishAsync("orders", payload, contentType: "application/octet-stream");

await using var batcher = new ProducerBatcher(client);
await batcher.PublishAsync("orders", payload);

await client.SubscribeAsync("orders", "billing", async (delivery, ct) =>
{
    await HandleAsync(delivery.Payload, ct); // return ACKs; throw NACKs
}, cancellationToken);
```

The C# batcher has the same count, byte, delay, backpressure, flush, and disposal
contract as Go. Both clients reject credentials over plaintext HTTP unless the caller
uses the conspicuous development override.

Consume a streaming response:

```text
GET /v1/subscriptions/stream?topic={topic}&group={group}&since={unix-ms}
```

The stream uses length-delimited binary frames. ACK and NACK endpoints accept batched message IDs. The Go and C# clients handle framing, reconnects, acknowledgement batching, bounded concurrency, and optional deduplication.

## Kubernetes

The Helm chart deploys one to many interchangeable brokers plus a tiny stateless gateway. Every broker can publish, stream, replay, ACK, and NACK; there is no leader or quorum. The gateway consistently hashes streaming consumers by topic and group so all members of a consumer group reach the same broker, while publishes remain load-balanced.

Create runtime credentials and TLS material outside source control, then install with an
Ingress (or service mesh) terminating client TLS before the HTTP gateway:

```sh
helm upgrade --install spruce deploy/helm/spruce \
  --namespace spruce \
  --create-namespace \
  --set image.digest=sha256:<release-digest> \
  --set auth.existingSecret=spruce-auth \
  --set tls.enabled=true \
  --set tls.existingSecret=spruce-internal-tls \
  --set ingress.enabled=true \
  --set ingress.hosts[0].host=spruce.example.com \
  --set ingress.tls[0].secretName=spruce-public-tls \
  --set ingress.tls[0].hosts[0]=spruce.example.com
```

Allow the Ingress controller through the default NetworkPolicy by labeling its
namespace once, for example:

```sh
kubectl label namespace ingress-nginx spruce.io/ingress-access=true
```

The chart refuses plaintext internal transport unless
`tls.allowInsecureTransport=true` is explicitly set. `tls.enabled` encrypts the
gateway-to-broker and peer links; it does not make the gateway Service HTTPS. Use an
Ingress or service mesh for the client hop. The internal certificate must cover every
StatefulSet pod and the headless-Service DNS name. Pin the broker by digest.

Recommended starting resources per replica:

```yaml
resources:
  requests:
    cpu: 25m
    memory: 96Mi
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

- `GET /health/live` reports process health.
- `GET /health/ready` reports readiness.
- `GET /metrics` exposes Prometheus counters and gauges.
- Graceful shutdown withdraws readiness, serves existing work for the configured drain
  interval, and then exits. A replacement broker merges its bounded cache from every
  reachable authenticated peer before becoming ready.
- Cache pressure, expired messages, dropped replication, pending bytes, retries, and action-queue saturation are observable separately.

Run three replicas for failure tolerance. Abrupt loss of one replica is normally masked
by replication and reconnect. Replacing every replica can still lose in-flight
deliveries because there is no durable source of truth. Pause publishers for strict
maintenance continuity, or accept the measured loss envelope in
[docs/OPERATIONS.md](docs/OPERATIONS.md).

## Build and verification

```sh
make build
make test
make test-race
make csharp
make image
make helm-lint
```

Performance results and methodology are documented in [docs/PERFORMANCE.md](docs/PERFORMANCE.md). CI runs correctness, race, C# conformance, build, and Kubernetes render checks.

## Repository guide

- [ARCHITECTURE.md](ARCHITECTURE.md): implemented architectural decisions and tradeoffs.
- [docs/OPERATIONS.md](docs/OPERATIONS.md): secure deployment, rotation, rollout, alerts, and recovery.
- [AGENTS.md](AGENTS.md): contributor and coding-agent invariants.
- `cmd/spruce`: broker entry point.
- `internal/broker`: cache, routing, replay, replication, and metrics.
- `pkg/spruce`: Go client.
- `clients/csharp`: C# client and conformance tests.
- `deploy/helm/spruce`: Kubernetes Helm chart.

## License

MIT. See [LICENSE](LICENSE).
