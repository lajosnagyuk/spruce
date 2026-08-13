# Architecture decisions

Spruce is a bounded, memory-only event bus for workloads where Kafka is operationally
disproportionate and rare message loss is acceptable. This document describes the
implemented system, not a roadmap.

## ADR-001: Memory instead of durable storage

**Decision:** Messages and delivery state live only in bounded process memory.

**Why:** Avoiding disks, databases, consensus, and recovery logs keeps latency low and
operation simple.

**Consequence:** Pod loss, cache eviction, exhausted queues, or full-cluster restart can
lose messages. Spruce is not a durable log.

## ADR-002: Identical leaderless replicas

**Decision:** Every broker accepts every API operation and replicates messages to its
configured peers. There is no elected leader or partition owner.

**Why:** Failover requires only ordinary load balancing; there is no control plane to
repair or operate.

**Consequence:** Replication multiplies network and cache use. Consumer-group membership
is broker-local, so grouped streams require sticky routing by topic and group.

## ADR-003: Best-effort delivery with bounded replay

**Decision:** Consumers use binary-framed HTTP streams, explicit ACK/NACK, retries, and
a timestamp replay cursor. Every queue and in-flight window is bounded by count, bytes,
or both.

**Why:** A slow consumer must not exhaust broker memory or block producers.

**Consequence:** Delivery is not exactly-once or durable at-least-once. Overflow closes
the stream so clients can reconnect and replay what remains cached. Duplicates and
ordering changes are valid; first-party clients provide deduplication.

## ADR-004: Opaque payloads and simple HTTP

**Decision:** Payloads are arbitrary bytes. Publishing supports raw single messages and
length-prefixed binary batches. Delivery uses an eight-byte binary frame header, JSON
metadata, and the unchanged payload.

**Why:** Any wire format works without schema coupling, while raw HTTPS remains usable
without a custom library.

**Consequence:** Applications own schemas, compatibility, and payload compression.

## ADR-005: Local acknowledgement by default

**Decision:** `local` accepts after insertion into the local cache. `one-peer` waits for
one peer. Replication queues retry briefly and then report loss through metrics.

**Why:** The normal path stays one network hop; callers can selectively buy more failure
tolerance without introducing quorum machinery.

**Consequence:** Neither mode is durable. Safe publish retries require a stable producer
ID and idempotency key.

## ADR-006: Explicit resource ceilings

**Decision:** Cache, replication, ACK/NACK propagation, pending delivery, subscriber
in-flight bytes, request concurrency, stream count, frame size, and idempotency state all
have hard limits.

**Why:** Predictable degradation is more valuable than preserving weak guarantees until
the kernel or Kubernetes kills the process.

**Consequence:** Operators must alert on drop counters and queue growth. Accounted cache
bytes are a logical admission metric; container memory needs runtime/index headroom.

## ADR-007: TLS and deliberately simple authentication

**Decision:** The broker can serve TLS directly. Public/admin APIs accept static Bearer
or HTTP Basic credentials. Peer traffic requires a shared token, cluster ID, HTTPS, and
an explicit trusted CA in the scratch image.

**Why:** This supports secure in-cluster and direct deployment without an identity
provider. Basic auth exists for simple integrations.

**Consequence:** Basic credentials are safe only over TLS. Credentials are cluster-wide,
not topic ACLs. Use separate clusters or network policy for distinct trust boundaries.

## ADR-008: Small static container

**Decision:** The broker is a statically linked Go binary in a scratch image, running as
a non-root numeric user with no writable filesystem requirement.

**Why:** This minimizes image size and attack surface.

**Consequence:** Certificates and trust roots must be mounted explicitly.
