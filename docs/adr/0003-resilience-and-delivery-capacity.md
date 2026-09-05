# Resilience first; distribute independent delivery work

Status: accepted direction, 2026-09-05. Implementation is incremental; this document
separates the agreed contract from guarantees already established by tests.

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

Publishers should encounter bounded admission pressure before outstanding retained work
is evicted for capacity. Expiry is an explicit loss boundary. This requires outstanding
work and group lifetime to be accounted for, including disconnected groups. The current
cache/lag limits do not yet implement that complete retention contract.

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
joining group members and concurrent broker recovery still require ordering/handoff
work. A single hot topic/group is not split across brokers by this change. Retained
capacity remains limited by each full replica, and publishing still pays replication
cost. No linear throughput-scaling claim is made.

A later larger deployment can partition resilience sets with a bounded copy factor,
but membership, handoff and recovery must be explicit before moving away from full
replication. Do not add arbitrary replica count and assume free capacity.

## Completion under partial failure

An ACK at its delivery owner now records bounded local completion without waiting for
an unavailable peer's action queue. Best-effort checkpoint propagation continues and
its drops remain observable. This supersedes the earlier requirement to retain local
pending state whenever checkpoint forwarding admission failed. Losing that owner
before completion propagates can redeliver completed work; it must not block the lone
survivor's consumers indefinitely. Unknown delivery owners still require forwarding.

## Acceptance work still required

- Explicit per-key outstanding-work scheduling through member changes, NACK, expiry,
  slow handlers and replay; passing first-delivery order tests alone is insufficient.
- Capacity admission protecting unfinished retained work, with a bounded definition of
  registered/disconnected groups and explicit expiry behaviour.
- Partition/heal tests and repeated rolling/abrupt recovery with active backlog.
- Fixed workload comparisons measuring completed group deliveries, tail latency, CPU,
  memory and duplicate amplification as broker/group counts change.
