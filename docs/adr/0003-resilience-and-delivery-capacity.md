# Resilience first; distribute independent delivery work

Status: accepted. [ADR 0004](0004-outstanding-work.md) defines per-key completion
scheduling and conservative TTL retention.

## Product decisions

Memory replication primarily buys resilience. A surviving minority remains available;
network partitions can therefore produce overlapping delivery. Consumers must tolerate
duplicates and make external side effects idempotent. Total loss of retained memory is
accepted. No disk log or mandatory majority is introduced.

The desired ordering scope is a topic/key within each consumer group. Unfinished work
blocks later work for that key while unrelated keys continue. A running partition can
only establish its own acceptance order; independent producers do not supply a global
causal order, and healing partitions cannot undo side effects already performed.
Strict cross-partition exclusivity is not part of the availability contract.

Publishers encounter bounded admission pressure instead of capacity eviction.
Expiry is an explicit loss boundary. Group indexes account for unfinished work,
including disconnected groups, within the stream-memory budget.

## Scaling geometry

Keep full replication for the current small resilience set. Distribute independent
(topic, consumer group) delivery work across that set instead of putting every group
for a topic on the same broker. This can distribute framing, sockets, pending delivery
and completion work without weakening the surviving-copy model. Publish ingress stays
topic-affine; replication supplies the other delivery brokers.

Implemented: local and Helm gateways hash topic/group for streams. First-party SDKs
attach a `Spruce-Delivery-Affinity` header to streams and ACK/NACK requests. Its
lowercase hex value is SHA-256 of UTF-8 topic, a zero byte, and UTF-8 group (empty for
broadcast). This gives identical routing across SDKs even when query escaping differs.
Gateways use the same stream upstream for scoped completion. Older clients retain the
forwarding path and query-based routing. Hints are routing aids, not authorization or
ownership proofs. Mixed old/new SDKs need a coordinated reconnect when adopting the
new affinity scheme; they can temporarily land on different brokers during migration.

These hashes do not elect or fence an owner. Gateways can disagree during failure;
local completion gates cannot fence handlers on another broker. A single hot topic/group is not split across brokers by this change. Retained
capacity remains limited by each full replica, and publishing still pays replication
cost. No linear throughput-scaling claim is made.

## Completion under partial failure

An ACK at its delivery owner now records bounded local completion without waiting for
an unavailable peer's action queue. Best-effort checkpoint propagation continues and
its drops remain observable. This supersedes the earlier requirement to retain local
pending state whenever checkpoint forwarding admission failed. Losing that owner
before completion propagates can redeliver completed work; it must not block the lone
survivor's consumers indefinitely. Unknown delivery owners still require forwarding.
