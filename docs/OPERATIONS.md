# Operations

## Production invariants

- Use three brokers and two gateways across nodes.
- Pin the broker image by digest. Verify its Cosign signature and retain the release SBOM.
- Terminate client TLS at the Ingress or service mesh. Enable `tls.enabled` for internal
  links. The gateway Service itself is HTTP.
- Use external auth and TLS Secrets. Do not expose the headless or admin Services.
- Label authorized client pods `spruce.io/client-access=true`. Label both the namespace
  and pod identity used by Prometheus `spruce.io/admin-access=true`.
- Give the container memory headroom above `cacheBytes + maxInflightBytes +
  replicationQueueBytes`; the default 320 MiB limit is sized for the default 64 MiB cache.

## Delivery and maintenance

Spruce offers best-effort delivery with bounded retries and replay from replica memory.
Cache eviction, TTL expiry, exhausted retries and infrastructure failures can prevent
delivery. Client deduplication suppresses repeated message IDs within its configured
window; it does not make external side effects exactly-once. Group ACK checkpoints suppress
completed cached messages after ordinary consumer reconnects and replica replacement,
but are non-durable and bounded by `config.checkpointEntries`. Abrupt loss of one broker
delivered zero missing logical messages in the local K3s gate at one message/second; a
node drain lost 14 of 480 expected deliveries in the final production-candidate run.
Replacing all three brokers while publishing lost 24-62 of 480 expected deliveries
across repeated runs. These are observations, not an SLA: there is no durable source of
truth, so infrastructure disruption can lose messages even when publishes return 202.

For strict planned maintenance: stop or buffer publishers for the complete ordered
rollout or replica-count change (the local three-broker gate allows four minutes), wait
one TTL/ACK window as appropriate, run `helm test`, verify replication/drop counters,
then resume. Replica-count changes regenerate the gateway's explicit StatefulSet
upstream list and roll the gateways, so they are topology maintenance rather than an
online autoscaling operation. Abort if readiness does not converge within the window,
replication errors increase, or any queue remains saturated. Never scale down while
replay state is valuable.

The stream memory budget defaults to 16 MiB. Each stream reserves 256 KiB plus its
replay ID index, so byte admission can reject streams before `maxStreams` is reached.
Watch `spruce_stream_memory_bytes` and `spruce_stream_memory_capacity_bytes`;
`stream_memory_capacity` and `replay_memory_capacity` are retryable 429 responses.

## Credential rotation

The auth Secret requires `peer-token`, `cluster-id`, `client-token`, and `admin-token`.
Optional `previous-peer-token`, `previous-client-token`, and `previous-admin-token` enable
overlap. Rotate in three broker rollouts:

1. Keep current tokens and set each previous token to its future value.
2. Make the future tokens current and move old tokens to previous.
3. Remove previous tokens after all clients and scrapers use the new values.

Brokers emit only the current peer token and accept current or previous. Do not roll the
gateway solely for auth rotation. The staged K3s test sustained mixed old/new traffic,
rejected retired credentials, and recorded zero peer replication errors or drops.

TLS Secrets are loaded at process start. For CA changes, first deploy a trust bundle
containing old and new roots, then switch leaf certificates, and only then remove the old
root. Each stage triggers an ordered broker and zero-unavailable gateway rollout. Keep
certificate validity overlapping and complete the same post-rollout checks at each stage.

## Alerts

Alert immediately on increases in replication dropped messages, delivery drops,
ACK/NACK action drops, or authentication failures. Alert on sustained replication errors,
cache pressure eviction, inflight/queue bytes near configured limits, readiness loss, or
unexpected restart/heap/goroutine growth. TTL expiry is normal and is reported separately
from pressure eviction.

## Verification

```sh
helm test spruce -n spruce
KUBE_CONTEXT=<context> SPRUCE_NAMESPACE=<namespace> SPRUCE_RELEASE=<release> \
  scripts/k3s-hardening.sh
```

After an incident, confirm all replicas are ready, replication error/drop counters are
stable, queues return to zero, authenticated publish/consume succeeds, and retired
credentials fail. There is no backup or restore operation because Spruce persists no data.

## Availability during partial cluster loss

Use single-message `available` acknowledgement when continued service from a lone
survivor is preferred. Observe `confirmed_copies` and `degraded` on the publish result.
`one-peer` still requires a fresh peer confirmation, including on retries. Eight default
publish attempts allow transient drain/routing recovery; configure a caller deadline
and retry policy to match the application's acceptable wait.

Draining now closes active streams and rejects new publication/subscription admission;
ACKs and peer recovery remain enabled during the grace period. The Helm gateway's
rendered configuration checksum triggers rollout for DNS and routing changes.

For isolated test namespaces labelled `spruce.io/test-environment=true`,
`scripts/k3s-lifecycle.py` runs baseline, scale-to-one/recover, and one/two-container SIGKILL
cases (`--ssh-user` and optionally `--ssh-identity` select node access). It restores the StatefulSet replica count after scale tests and
retains job results for an hour. It does not cordon or drain cluster hosts.

Gateway retries of forwarded POSTs are restricted to single-message publishes with
both operation-identity headers.

### Delivery distribution and completion

Current SDKs send a canonical `Spruce-Delivery-Affinity` digest on streams and ACK/NACK
requests. The gateway spreads topic/group pairs across brokers and sends scoped
completion through that same upstream. Coordinate client stream reconnection when
migrating from older SDKs, whose routing can differ. This does not fence independent
brokers during partitions. Full replication still stores every event on each broker.

Owner-local ACKs now complete even when peer checkpoint queues are full. Monitor action
drops: subsequent owner loss can redeliver work whose checkpoint did not propagate.

### Retention and unfinished grouped work

Accepted events now remain cached until their original expiry, even after completion.
A full retention or group-index budget returns HTTP 429 `retention_capacity`; producers
must handle rejection and retry the same operation identity. ACK does not immediately
make cache space available. Choose TTL and memory together: approximately arrival rate
× retention seconds × accounted message size, plus the separately bounded stream,
in-flight, replication and runtime memory. The chart still defaults to a one-minute
TTL; set an explicit window appropriate to the tolerated consumer outage.

Groups now gate each key on completion. NACK and timeout retry its current head;
MaxAttempts caps the reported group attempt rather than discarding the work. Expiry
releases the key and may require the consumer to handle a cursor-expired error. Broadcast
retry limits retain their existing behaviour. A slow grouped key does not impose the
broadcast topic-wide lag rejection on unrelated keys; actual byte pressure still applies.

Group ID queues share `streamMemoryBytes`, with one stream reservation protected from
queue usage to permit reconnection. A large backlog can reject additional group/stream
admission. Group state survives disconnection while it contains work; retained TTL
history provides recovery on other brokers without distributed group registration.

Monitor `spruce_group_outstanding_messages`, `spruce_group_active_keys`,
`spruce_group_memory_bytes`, `spruce_registered_groups`, and especially increases in
`spruce_group_expired_messages_total`. The latter means indexed unfinished work reached
expiry. Normal cache expiry also includes already completed history and is a different
signal. Status exposes corresponding fields in all three SDKs.

These gates order distinct event identities at one broker. They do not fence an old
application handler after timeout or an independently serving partition. Keep side
effects idempotent; completion gates do not establish exclusive ownership across brokers.

### Partition recovery and idle streams

Dropped replication work schedules a retained-cache repair on the existing peer worker.
A receiver also requests repair when a retained copy is blocked behind a missing
predecessor; that predecessor may already have expired upstream. The sender first
checks that the gap remains stalled, avoiding cache walks for normal transient reordering.
Repair sends bounded pages through the authenticated internal API, reserves the existing
replication queue budget, and retries while the peer remains incomplete. Healthy peers
are not periodically sent full snapshots. Monitor `spruce_repair_pending_peers`,
`spruce_repair_pages_total`, `spruce_repair_messages_total` and
`spruce_repair_errors_total`; sustained pending repair means redundancy is reduced.
Repairs preserve original expiry and cannot recover events after every copy expires.
Repair reconciles payloads, not lost consumer completion checkpoints; a healed replica
can therefore redeliver previously completed work.
The repair API requires updated peers; older brokers continue ordinary replication but
cannot accept background repair pages.

Recovery may import events after a gap in a replica's history. It marks that topic's
cursor history unsafe rather than pretending the gap never existed. Partition recovery
can duplicate or reorder delivery already performed by independently available brokers.
Consumers must make side effects idempotent; a cursor-expired error requires an explicit
application recovery decision.

Brokers send stream heartbeats every 15 seconds. Gateways close a stream after 30 seconds
without upstream data. SDKs also bound a stalled frame read with a default 45-second
`StreamReadTimeout` (Go/C#) or `stream_read_timeout` (Python client constructor). Keep
these intervals above the heartbeat interval. Handler execution does not consume the
SDK read deadline. These bounds detect silent broken connections; TCP connection state
alone does not establish that a consumer can still receive events.

The disposable-container test driver exercises delivery and cache convergence using
baseline, network partition/heal, and two-broker SIGKILL scenarios:

```sh
python3 scripts/test-container-resilience.py --image spruce:dev --case partition
python3 scripts/test-container-resilience.py --image spruce:dev --case kill-two
python3 scripts/test-container-resilience.py --image spruce:dev --brokers 5 --case baseline
```

Build `bin/spruce-lifecycle` first. `--resources` samples Linux cgroup CPU and memory;
`--retention-seconds` permits expiry-cycle soaks. The driver fails missing or corrupt
accepted deliveries and checks retained-cache digests after recovery. Partition cases
report reordering separately from loss because cross-partition order is not guaranteed.

### Long-running clients and retention

Receive-origin metadata and consumer checkpoint history are reclaimed after expiry;
refreshing an existing identity does not retain an unlimited update history. Payload
expiry remains independent of the once-per-second metadata cleanup cadence.
Long-lived streams discard cursor origins only when their completed history is known
to have expired. Unknown history remains conservative; exhausting the bounded cursor
produces an explicit cursor-expired recovery path rather than a silent reconnect loop.

Producer batchers reserve queue admission before copying payloads. Queue depth bounds
owned queued entries, while maximum batch bytes bounds the active batch; configure both
for the application's payload sizes. Closing rejects further admission and unblocks
waiting callers. Python cannot interrupt an arbitrary client implementation already
inside a request: a close timeout reports that the worker is still draining, and a later
close can wait again. A timed-out publish may already have reached the broker; use the
unbatched idempotent retry API when acceptance certainty is required.
