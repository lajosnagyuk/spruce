# Spruce

Spruce is a small, leaderless, in-memory event bus for service-to-service glue where Kafka is operationally excessive and rare message loss is acceptable.

It provides opaque binary messages over HTTPS, N producers and consumers, broadcast and consumer-group delivery, bounded replay, Go, C#, and Python clients, Prometheus metrics, and a Kubernetes deployment.

## Delivery contract

- Delivery uses memory-bounded ACK/NACK retries and replay. Group work retries until completion or original TTL expiry; broadcasts retain a retry-attempt limit. TTL expiry or loss of every retained copy can prevent delivery. Capacity pressure rejects new writes instead of evicting accepted events.
- Messages are not persisted. Restarting every replica can lose all cached messages.
- Group ACK checkpoints are propagated and bootstrapped between replicas on a best-effort basis; NACK retry remains best effort.
- Consumer groups deliver to one healthy member. Keyed messages use rendezvous hashing so
  the same key stays with the same member while membership remains stable. Each broker
  gates the next keyed event on completion of its predecessor; a saturated member leaves
  that key queued. These local gates do not fence duplicate attempts or independent live
  partitions. Ungrouped subscribers receive broadcasts.
- Reconnecting consumer groups skip acknowledged messages while their bounded in-memory checkpoints and messages remain cached.
- Duplicate delivery is expected. First-party clients provide bounded client-side deduplication.
- Payloads are opaque bytes. Spruce does not inspect schemas or formats.
- First-party Go, C#, and Python SDKs adaptively Zstandard-compress payloads of at least 1 KiB by default. Compression is retained through caching, replication, replay, and delivery, and is used only when it saves at least 10% and 128 bytes. Select `gzip` for ecosystem compatibility or `off` to disable adaptive compression. Literal compression-envelope bytes are escaped to preserve payload identity.

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
`http://localhost:8080` for isolated local development only. Set `SPRUCE_LOCAL_PORT`
to choose another loopback port. The gateway reads the container DNS resolver at
startup and refreshes broker addresses after restarts.

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
idempotency key. That operation receives a stable message ID across brokers; a replica
with the original cached message preserves its expiry on retry. Never reuse an operation
key for another event. Conflicts are checked against locally available state, not by
cluster-wide consensus. Use `spruce.NewDeduper` to suppress repeated message IDs within
its bounded window; persistent business-side effects still need application idempotency.

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
GET /v1/subscriptions/stream?topic={topic}&group={group}&cursor={opaque-resume-token}
```

The stream uses length-delimited binary frames. ACK and NACK endpoints accept batched message IDs. Resume cursors are opaque and must be returned unchanged; timestamp cursors are not supported. For consumer groups, recovery scans retained history and filters group completion checkpoints: one member's cursor cannot exclude work assigned to another member. A supplied cursor can still report expired history; it is not a group-wide seek offset. The Go, C#, and Python clients handle framing, reconnects, acknowledgement batching, bounded concurrency, and optional deduplication.

All three clients default to adaptive `zstd` and also support `gzip` and explicit `off`. Compression
is retained only when it materially reduces the payload, remains opaque to brokers, and
is transparently decoded under the subscriber's message-size limit. A payload that already
starts with a valid envelope prefix is wrapped with an existing codec to preserve its
literal bytes, including with `off`; readers decode only one layer. If escaping would
exceed the 1 MiB wire limit, publishing fails explicitly.

## Kubernetes

The Helm chart deploys one to many interchangeable brokers plus a tiny stateless gateway. Every broker can publish, stream, replay, ACK, and NACK; there is no leader or quorum. The gateway consistently hashes publishes and streaming consumers by topic so a topic's ordinary live-delivery decisions remain local to one broker; replicas still receive bounded asynchronous copies.

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

With these defaults the public Service is `spruce`, the headless Service is
`spruce-headless`, and the namespace is `spruce`.

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
    memory: 224Mi
  limits:
    memory: 320Mi
```

Tune cache, pending-delivery, subscriber, and replication limits together with the pod memory limit. Kubernetes limits are the final safety boundary; Spruce's internal byte limits protect the normal operating envelope.

## Clients

Go:

```sh
go get github.com/lajosnagyuk/spruce/pkg/spruce
```

C# source and packaging live under `clients/csharp`. The library exposes publish, batch publish, subscribe, ACK/NACK, retry, bounded handler concurrency, and deduplication primitives.

Python:

```sh
pip install spruce-client
```

The Python 3.11+ package, with its declared Zstandard dependency, provides the same publish, automatic batching,
streaming consumption, explicit completion, retry, deduplication, diagnostics, and
credential-safety contracts as Go and C#.

All libraries use the public HTTPS API. They are conveniences, not protocol requirements.

Tagged releases always build and retain the Python distributions. Publishing them to
PyPI is opt-in: set the repository variable `PUBLISH_PYPI=true` only after configuring
the repository as a PyPI trusted publisher. C#, image, and Helm release
failures remain release-blocking and are not suppressed by this switch. Client package
versions are derived from the `vMAJOR.MINOR.PATCH` release tag.

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
python3 -m venv .cache/python
. .cache/python/bin/activate
python -m pip install ./clients/python build
make python
make image
make helm-lint
```

Performance results and methodology are documented in [docs/PERFORMANCE.md](docs/PERFORMANCE.md).
PR and main-branch CI run correctness, race, performance, client conformance, package
builds, and Helm validation without creating a Kubernetes cluster. Superseded PR runs
are cancelled. The separate **Kubernetes integration** workflow deploys a disposable
kind cluster for live client conformance and public/internal TLS rotation checks. It
can run manually and must pass before tagged releases publish any packages.
The manually triggered **Bounded memory soak** workflow checks the tested memory envelope.

## Repository guide

- [ARCHITECTURE.md](ARCHITECTURE.md): implemented architectural decisions and tradeoffs.
- [docs/OPERATIONS.md](docs/OPERATIONS.md): secure deployment, rotation, rollout, alerts, and recovery.
- [AGENTS.md](AGENTS.md): contributor and coding-agent invariants.
- `cmd/spruce`: broker entry point.
- `internal/broker`: cache, routing, replay, replication, and metrics.
- `pkg/spruce`: Go client.
- `clients/csharp`: C# client and conformance tests.
- `clients/python`: Python client, packaging, and conformance tests.
- `deploy/helm/spruce`: Kubernetes Helm chart.

## License

MIT. See [LICENSE](LICENSE).

### Degraded memory-only operation

Single-message producers may select `Ack: "available"` (HTTP `Spruce-Ack: available`)
to keep serving when only one broker remains. The response includes `confirmed_copies`
and `degraded`. Healthy peers are attempted within a 100 ms confirmation window;
unfinished copies use bounded background replication. `one-peer` retains its strict
peer requirement. These receipts do not prevent TTL expiry or later loss of
all copies. Complete-cluster restart survival is outside the product contract.
See [ADR 0002](docs/adr/0002-memory-availability.md).

Current gateways distribute topic/group streams across replicas, and first-party SDKs
route completion using the same canonical affinity digest. This spreads independent
delivery work while preserving full memory replication for resilience. It does not
increase retained capacity or provide exclusive group ownership during partitions.
See [the accepted scaling direction](docs/adr/0003-resilience-and-delivery-capacity.md).

Grouped delivery now queues message IDs per key and waits for completion before sending
that key's next event. NACK/timeout keeps the head outstanding until completion or its
original expiry. Brokers retain accepted cache entries until expiry and return explicit
429 backpressure instead of pressure-evicting older events. Retention includes completed
history, so size TTL and cache capacity together. See
[ADR 0004](docs/adr/0004-outstanding-work.md) for bounds, reconnection headroom and the
limits of local ordering during partitions or overlapping handler attempts.
