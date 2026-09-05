# Lifecycle event delivery review

Status: initial repair work and local validation; the architecture proposal below is
not implemented. Base commit: `057675b498cdeea3286be975828604955362fd84`.

## Workload and acceptance contract

The target is microservice lifecycle events, with a few thousand messages per hour,
multiple topics, keyed entities, and independent consumer groups. Payload bytes must
survive unchanged. Each subscribed group needs the event; members within a group
share work. Speed is useful only when accepted events remain recoverable and failures
are visible.

The desired contract should distinguish these boundaries:

| Boundary | Required meaning for lifecycle events | Current implementation after these repairs |
| --- | --- | --- |
| Publish succeeds | Event survives the agreed broker failure envelope | `one-peer` confirms local memory plus one peer's acceptance; no disk persistence |
| Consumer ACK succeeds | Group completion remains known after failover | Bounded asynchronous checkpoint propagation; duplicates remain possible |
| Handler rejects event | Event remains outstanding | SDKs reconnect before failed completion; retained history and checkpoints still limit recovery |
| Same entity changes twice | Consumers apply a meaningful sequence | Key affinity and contiguous client completion; no fenced cross-broker processing order |
| Consumer is offline | Backlog remains recoverable up to explicit policy | Cache eviction, TTL, checkpoint bounds and retry limits can lose progress or events |
| More brokers are added | Useful aggregate capacity increases | Every broker retains every message; retained capacity does not scale with node count |
| All brokers restart | Previously accepted events remain available | Memory-only history is lost |

An ACK is evidence that a handler reported success, not proof that an external database
transaction happened exactly once. Consumer-side idempotency remains necessary.

## Repairs in this branch

- Reserve stream memory before allocation; cap it independently of stream count.
  Replay and deferred delivery retain message IDs instead of pinning evicted payloads.
  An evicted replay entry ends the stream rather than silently skipping it.
- Derive stable message IDs from topic, producer ID and operation key. A retry routed
  to another replica reuses retained content and its original expiry. Conflicting
  retained content returns an error. Concurrent conflicting requests to isolated
  brokers still have no distributed conflict arbitration.
- Negotiate replication format immediately at startup. A transient capability error
  no longer silently selects the legacy format, which cannot support safe cursors.
  Capture the format with the encoded body so concurrent negotiation cannot mislabel it.
- After `one-peer` succeeds, queue copies for the remaining peers. Cancelling slower
  synchronous requests must not leave permanent holes in otherwise healthy replicas.
- Reject oversized admission before consuming a source sequence number.
- Forward group completion checkpoints when an ACK reaches its delivery owner through
  another broker. Preserve pending state if that forwarding cannot enter the bounded
  action queue. Bound checkpoint-only action batches as well as delivery-ID batches.
- Stop treating NACK as completed cursor progress in Go, .NET and Python. Reconnect
  before the failed event, allowing redelivery instead of skipping a retry lost with
  the broker. Successfully processed concurrent messages may be repeated. Reconnect
  does not provide a durable poison-message attempt count or dead-letter policy.
- Recover group backlog independently of the reconnecting member's cursor and defer
  new work behind the group's replay owner. A member's cursor is not a group-wide seek
  offset or proof that other members finished their assigned work.
- Match local gateway topic affinity to the Helm gateway and block internal endpoints.
  Refresh broker DNS records when local container restarts change their addresses.
- Retry gateway/server errors in Go and Python with the same operation identity,
  matching .NET. Retry attempts remain bounded; permanent client errors are not retried.
- Add an independent per-group delivery oracle and a container resource sampler.

These repair specific failure paths. They do not turn bounded memory retention and
asynchronous checkpoints into a durable log or make topic affinity a distributed lock.

## Local evidence

Host: AMD Ryzen 7 8845HS, 16 logical CPUs, Fedora 44, rootless Podman. Three brokers
plus nginx on one machine; 64 MiB cache per broker. Synthetic events use 256 padding
bytes plus JSON metadata, compression off, individual retryable publishes with
`one-peer`. Four producers, four topics, two groups per topic, two members per group,
16 keys per producer. Handler concurrency is one per stream. This checks first-observed
per-producer/key order, not global order or concurrent application side effects.

- Before the replication repairs, killing broker1 for five seconds during an
  8,000-event run at 200 events/s left **8,654 of 16,000 expected group deliveries
  missing**, with 12 terminal cursor errors. All publishes reported success. This
  baseline already included the initial stream-memory work; it is not untouched main.
- With the first replication repairs, the same scenario delivered all 16,000, with
  30 duplicates and no observed first-delivery ordering regression. This is one fault
  trial, not a quantified reliability rate.
- A later repeat after ACK-forwarding repairs still failed: five missing group
  deliveries, 308 duplicates and 44 ordering regressions. That invalidates a broad
  success claim from the earlier trial and motivated the group-replay repairs above.
- At two events/s for one minute (120 events, 240 group deliveries), all expected
  deliveries arrived without duplicates, corruption or observed ordering regressions.
  Publish p99 was 1.92 ms; first-delivery p99 was 1.96 ms. Across 65 one-second samples,
  all three brokers plus gateway averaged **30.09 MiB** of cgroup memory and
  **0.00794 CPU cores**, with a sampled aggregate memory maximum of **34.66 MiB**.
  This short low-rate run preceded the ACK-forwarding and SDK NACK changes. It excludes
  producer/consumer CPU, container-engine overhead and any durable storage cost.

Raw local JSON results live under `.cache/lifecycle-*.json`; they are not source files.
The driver checks accepted events separately for each group, payload content, duplicate
first deliveries, ordering regressions and terminal errors. Its own tests deliberately
inject missing, duplicate and reordered delivery and require failure. Passing with
`-allow-duplicates` still counts duplicates; it does not hide them or suppress loss.

Example (local gateway port 18080):

```sh
make build
./bin/spruce-lifecycle -server http://127.0.0.1:18080 \
  -producers 4 -messages 30 -rate 2 -topics 4 -groups 2 -members 2 \
  -keys 16 -timeout 75s
scripts/sample-container-resources.py --engine podman --seconds 65 \
  spruce-dev_broker1_1 spruce-dev_broker2_1 spruce-dev_broker3_1 spruce-dev_proxy_1
```

For the single-container fault trial, use `-messages 2000 -rate 200
-allow-duplicates`, kill broker1 with SIGKILL ten seconds into the run, and start that
same container five seconds later. Do this only against the isolated test project.
Container kill does not simulate disk failure, network partition or a Kubernetes node
loss. A common host also means these brokers do not have independent failure domains.

The next group-replay trials had no missing accepted deliveries, but still showed
first-delivery reordering during failover. A sequential three-broker restart also
exposed stale local gateway DNS and missing gateway-error retry classifications. Those
local infrastructure/client issues are repaired separately; the earlier failed trial
must not be presented as a successful availability test.

## Final local fault and burst results

Broker image: `bccbc3a82f15649c5c9cfb4640a7c35b83720de2317a7c4448983663e68cbbc6`.
The gateway includes dynamic DNS refresh; the Go driver includes gateway-error retries.
Each fault run publishes 8,000 events at 200/s and expects 16,000 group deliveries.
Brokers were killed in succession, each ten seconds into its own run and restarted
five seconds later. Every run had zero publish errors, missing group deliveries,
corruption and terminal subscription errors.

| Broker killed | Group deliveries | Duplicates | First-delivery order regressions | Delivery p99 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 16,000 / 16,000 | 173 | 24 | 2,442 ms |
| 2 | 16,000 / 16,000 | 96 | 70 | 2,499 ms |
| 3 | 16,000 / 16,000 | 421 | 0 | 3,162 ms |

The first two runs **fail the ordering check**. All three together delivered 48,000
expected group observations, with 690 duplicates and 94 ordering regressions. These
three trials establish observed results only; they do not establish a loss probability.
Routed stream failover, concurrent group members and unfenced broker ownership still
prevent a strict processing-order guarantee.

An unpaced 8,000-event run on the same final cluster accepted 4,523 events/s and
completed all 16,000 group deliveries with zero duplicates, corruption or ordering
regressions. Publish p99 was 2.07 ms, but delivery p99 was **989 ms**. Consumer backlog
under the burst is material; accepted ingress latency must not be reported as delivery
latency. This is still far above the intended hourly event count, but it is not evidence
of sub-millisecond delivery under saturation.

## Regression and performance checks

Go unit and race tests, .NET conformance and packaging, Python conformance (20 tests),
Helm lint/render and stream-budget validation pass locally. The Compose validation
checks ran through a local Docker-command adapter to Podman, using already built
images after the public smoke correction. Go/.NET interoperability, broker loss and
full replication passed: all three replicas converged on 10,003 messages, with zero
replication-error or drop increases during the 10,000-message batch phase. That short
batch run accepted 24,978 events/s; it is not a sustained-capacity or delivery-latency
measurement. The default local stop timeout forced SIGKILL before the broker's drain
delay elapsed, so this run is evidence of broker-loss recovery, not graceful draining.

Three one-second microbenchmark samples with Go 1.26.7 and `GOMAXPROCS=4` measured
362–366 microseconds per 512-message batch, versus 344–350 on base main using the same
benchmark helper. Publish/deliver/ACK measured 318–336 microseconds per 256-message
iteration versus 307–334 on main. Allocation counts stayed at 2,083 and 1,572 per
iteration respectively. The batch repair has a small measured CPU cost; this is not a
speedup claim or a statistically established regression bound. Allocation gates pass.

## Proposed architecture for the stronger contract

For this workload I recommend relaxing the prohibition on all leaders, retaining no
single global message-path leader, and using small replicated partitions with fenced
ownership. I also recommend an optional persisted lifecycle mode if accepted events
must survive complete restart. Both are changes to the product definition, not hidden
implementation details. This proposal needs the storage requirement settled before
implementation.

1. Map each topic/key to a stable partition. A single committed sequence within that
   partition defines broker order. Concurrent independent publishers have no intrinsic
   wall-clock order; the assigned sequence is the order consumers can rely on. An
   entity version in a payload can express a stronger application ordering requirement.
2. Give each partition one elected, term-fenced writer with a fixed replica count
   (initially three). A majority commits entries. During a split, a side unable to
   establish that authority rejects or delays writes rather than accepting competing
   histories. Calling this process an owner does not remove its leader semantics.
3. Spread partitions across nodes while keeping replication factor fixed. Adding nodes
   can then add aggregate storage and throughput across partitions. A single hot key
   remains serial; adding nodes cannot parallelise that key's ordered state transitions.
   Membership changes require a protocol that cannot create two committing majorities.
4. In persisted mode, acknowledge only after the agreed replicas have persisted the
   committed entry. Use bounded segmented logs and bounded memory buffers. Rebalance
   and restart recover from committed history. In memory mode, clearly retain the
   total-restart loss limitation.
5. Commit consumer-group progress with the same recovery discipline. Fence obsolete
   assignments and allow at most one outstanding event per key where processing order
   is requested. A lease cannot stop an old process from completing an external side
   effect; the consumer still needs idempotency or a version/fencing check at that
   side-effect boundary.
6. Backpressure or reject before acknowledging when the retention budget is exhausted.
   For lifecycle mode, do not evict unprocessed accepted events merely to keep an
   ingress benchmark fast. Define subscription start offsets, group deletion, offline
   group retention and poison-message handling explicitly. Park a failed key or use an
   explicit dead-letter policy rather than silently discard it after retries.
7. Preserve operation IDs through retries. Define their retention window and reject
   conflicting content consistently. Publish timeout means an unknown outcome and a
   retry with the same operation ID, not proof that nothing was accepted.

This intentionally starts with a narrow contract. A few thousand events per hour does
not require a complex elastic control plane. Measure the cost of three small replicated
processes and disk commits before adding optimisation machinery. The transport cannot
make an application's database update and a separate publish atomic by itself; that
boundary needs an outbox or equivalent in the shared integration layer when required.

## Remaining validation before replacement

- Obtain the exact `spruce-dev` Kubernetes target. The available context points to the
  existing application cluster, with `quix-spruce` namespaces; the Proxmox inventory
  contains five `spruce-k3s-*` VMs. No separately named dev cluster was identified.
  Do not run the current hardening script's node drains against those app hosts.
- Repeat container faults across every broker and at multiple offsets, including
  producer timeout/retry, handler NACK, slow/offline consumer and gateway restart.
- Test network partitions, simultaneous loss of the agreed number of replicas,
  full restart, scale up/down and rejoining stale replicas against the chosen contract.
- Run scale curves with fixed workload, replication factor and resource accounting;
  measure delivery and convergence as well as accepted ingress. Current full replication
  cannot substantiate a retained-capacity scaling claim.
- Exercise opaque binary payloads and each supported SDK together, TLS/auth rotation,
  resource exhaustion, long retention, checkpoint eviction and poison messages.
- Require end-to-end zero unexplained loss within the declared failure envelope. Report
  duplicates and processing order separately, and surface outside-envelope failures.

The current repair set is not yet a sign-off to replace the Kafka event bus.
