# Completion-gated group work and conservative retention

Status: implemented, 2026-09-05. Refines ADR 0003 without adding durable storage,
majority gating, or exclusive ownership across live network partitions.

## Retention and producer admission

Broker caches no longer pressure-evict accepted events. Retain every accepted event
until its original expiry, including events already completed by current groups.
If a new message or complete public batch cannot fit, reject it before sequence
assignment and admission with retryable HTTP 429 `retention_capacity`. Retry the same
operation identity. TTL expiry and loss of all retained copies remain loss boundaries.
Oversized invalid work still fails capacity validation rather than entering the cache.

This deliberately conservative rule protects disconnected groups even when publish
ingress and group delivery run on different brokers. It requires no replicated group
registry or global completion knowledge. A reconnecting/new group can recover retained
history and filter available completion checkpoints. ACK does not immediately reclaim
cache space: size the retention window and budget together. The implementation does not
claim successful acceptance by a peer whose bounded copy/admission path rejected work.

Public batches preflight cache and group-index expansion before inserting any entry.
Replica sequence-gap buffering remains bounded; index pressure must preserve buffered
successors so a retry can continue convergence without silently creating a hole.

## Group scheduler

Maintain a FIFO of message IDs for each local topic/group/key. Unkeyed messages use
message identity as their lane and do not gain an ordering guarantee. Only the head
of a key can have a pending delivery. An ACK completes that event and releases its
successor. A NACK, timeout or member replacement retries the same head. Group work no
longer disappears on MaxAttempts: the reported attempt saturates at that configured
value while retries continue until completion or original expiry. Broadcast retry
limits retain their prior behaviour.

Other keys remain eligible for delivery. A saturated member leaves the selected head
queued, rather than immediately moving it to another member. Grouped publishing uses
actual byte admission instead of the legacy broadcast topic-wide delivery-lag rejection.
Broadcast streams retain their existing replay/lag behaviour.

Local completion gates enforce ordering of distinct outstanding event identities at
one broker. They cannot cancel an external side effect or fence an old handler. The
same event may be retried while an earlier attempt is still running after a timeout;
independent partitions may process overlapping group work. Idempotent side effects
remain necessary. Incoming acceptance order from independent origins is not a global
causal order, and reconnect/bootstrap do not introduce consensus over that order.

## Bounds and recovery

Group registration count is bounded by MaxStreams. Group state survives disconnection
while it has unfinished work; empty disconnected state is reclaimed. A group's queue
index contains IDs, keys and expiry metadata, not another retained payload graph.
Indexes and registration share the configured stream-memory budget, with explicit
charges and at least one stream reservation excluded from group usage so a full
backlog cannot prevent all consumer reconnection. New group registration or work-index
expansion can return 429. Pending payloads retain their existing separate byte bounds.

A single worker dispatches queued heads, with a bounded polling cadence for temporary
channel pressure. ACKs wake it directly. Full expiry cleanup runs at most once a second;
expired heads can be released on dispatch. Expiry of unfinished indexed work increments
`spruce_group_expired_messages_total` instead of silently being treated as completion.

Status/metrics expose registered groups, outstanding group messages, active keys and
group memory. Cache expiry counters include completed retained history; the group-expiry
counter specifically identifies expiry while indexed work was unfinished.

## Compatibility and operational effect

This changes overload behaviour: a busy cache now returns 429 instead of recycling
older retained entries, and ACKs do not create cache space before TTL. Grouped NACK/
retry exhaustion holds a key until completion or expiry. Do not deploy expecting a
poison event to disappear merely because its attempt counter reaches the old limit.
The Python status decoder now ignores additional fields, matching the other SDKs'
forward-compatible behaviour. All SDK status models expose the new group metrics.
