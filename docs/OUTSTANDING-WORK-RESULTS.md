# Outstanding-work implementation and validation

2026-09-05; following commit `9631ba4`. Contract and operational trade-offs are in
[ADR 0004](adr/0004-outstanding-work.md). This report supplements earlier failed and
passing trials; it does not replace them or establish a production SLA.

## Implementation evidence

Deterministic tests verify:

- A key's second event waits for the first ACK while another key progresses.
- NACK and timeout retry the same head beyond MaxAttempts; ACK advances it.
- A replacement group member receives unfinished work before its successor.
- Original expiry releases a blocked key and its index accounting.
- Cache pressure rejects the whole public batch before insertion or sequence assignment.
- HTTP pressure is retryable 429 and retained entries are not pressure-evicted.
- Group-index pressure rejects before cache/sequence mutation and recovers after release.
- Replica index pressure preserves a buffered sequence successor across retry.
- A full disconnected group queue leaves a stream reservation for reconnection.
- A slow grouped key does not reject an unrelated-key publish merely due to lag.

Go race tests cover these paths. Group queues use the existing stream-memory budget;
no payload log, consensus service or unbounded work queue was added. Status/metrics
expose outstanding work, registered groups, active keys, group memory and unfinished
expiry. SDK status types include these fields; Python now tolerates additional fields.

The allocation benchmark previously could exhaust retention during calibration without
failing the outer test. It now checks request success and expires history outside the
timed section. The identical benchmark measures 2,101 allocations on both previous
commit `9631ba4` and this implementation; its ceiling preserves seven allocations of
headroom (2,108). This is a harness correction, not an allocation regression excused by
raising the old threshold. The byte ceiling remains unchanged. Other throughput
benchmarks use a short retention window or complete their queued work.

## Initial cluster trials

Isolated Andromeda `spruce-dev`; three brokers, two gateways; same four producers,
four topics, two groups per topic, two members per group, 16 keys per producer,
256 padding bytes plus metadata, compression off and `ack=available` as earlier trials.
The first implementation image is `localhost/spruce:dev-ordered`; later final trials
include protected reconnect headroom, group metrics and group-specific admission.
The driver image `localhost/spruce-tools:dev-ordered` adds handler-delay, concurrency,
injected first-event NACKs and concurrent same-key handler detection.

| Initial trial | Accepted events / group deliveries | Errors / missing / duplicates / order regressions / same-key overlaps | Delivery p99 |
| --- | --- | --- | --- |
| Healthy burst | 8,000 / 16,000, all delivered | 0 / 0 / 0 / 0 / 0 | 17.87 ms |
| 100 ms key-zero handlers, concurrency 8, eight injected NACKs | 4,000 / 8,000, all delivered | 0 / 0 / 1 / 0 / 0 | 22.48 s |

Healthy accepted throughput was 1,359/s. The stressed run accepted 1,352/s but its tail
shows significant backlog/reconnection cost; it is not a clean low-latency result.
Slow-handler latency is measured at handler observation after the injected delay,
whereas the normal handler has no delay. The final oracle fails overlapping distinct events for the same key, as well as
missing/corrupt or first-observed reordered events. It separately counts all overlaps,
including attempts of the same event; such retries require idempotent consumers.
A fake-server regression test verifies that concurrent distinct events fail the oracle. Passing these
trials does not prove external side-effect fencing, especially beyond an ACK deadline
or across live partitions.

## Final-image disruption trials

Broker `localhost/spruce:dev-work-final` manifest
`sha256:fa6966a73f067a20e28f3b4571b170ebb1f739bb56f812a4cd636d09ea61e971`;
final driver `localhost/spruce-tools:dev-work-final` manifest
`sha256:778849714d1a7a658eb0e1a0c8010037ede59959eaf8f9096f3cb47fbb473e1d`.
Both are development builds from this change. Brokers use 10-minute retention for
these tests; the chart default is unchanged.

The first final-image scale trial **failed** the original all-overlap check: all
8,000 events reached both groups with no order regressions, but one duplicate and
one same-key handler overlap were observed (delivery p99 12.74 s). That driver could
not distinguish concurrent retries of one event from concurrent distinct events.
This result remains unresolved at that finer level; the later passing trial does
not retroactively classify it. The driver now records both counts and fails distinct
same-key overlap; it does not promise to fence duplicate attempts.

A repeat with the final driver scaled 3→1→3 at 200 events/s, with 100 ms key-zero
handlers, concurrency eight and eight injected NACKs. All 8,000 events reached both
groups: zero publish errors, missing deliveries, duplicates, order regressions or
overlapping handlers. Delivery p99 was 12.28 s, publish p99 2.51 ms. Confirmed-copy
receipts were 3,122 with one copy, two with two copies and 4,876 with three copies.
This demonstrates available admission during degradation, not redundant protection
for a receipt reporting only one copy.

Two actual broker-container SIGKILLs under the same slow-handler/NACK workload
also delivered all 8,000 events to both groups, with zero publish errors, missing
messages, duplicates, order regressions or either kind of handler overlap. Delivery
p99 was 13.25 s and publish p99 2.69 ms; confirmed-copy counts were 188 / 27 / 7,785
for one / two / three copies. Kubernetes restarted both containers.

The final healthy, unpaced burst accepted 8,000 events at 1,371/s and delivered
all 16,000 group deliveries, with zero errors, duplicates, order regressions or
handler overlaps. All receipts confirmed three copies. Delivery p99 was 22.34 ms
and publish p99 8.74 ms. An earlier baseline invocation used the script default
50/s unintentionally and was cancelled; it is not included as a completed trial.

One resource snapshot during this disruption workload showed brokers at 122–137
millicores and 66–110 MiB each, and gateways at 35–40 millicores and 4 MiB each.
These are momentary readings after multiple retained test workloads, not steady-state
averages or memory ceilings. They do not substitute for a long resource soak.

Local validation passed: full Go tests and race tests, allocation gates and hot-path
benchmarks, C# conformance and package build, 21 Python tests and package build,
Helm lint and chart validation. Cluster tests used the Go client; the SDK conformance
tests do not establish equivalent fault coverage for every language.

## Remaining boundary

Per-key gates are broker-local. Network partition/heal and sustained mixed-origin
recovery still need dedicated validation; availability permits overlapping group work
on independently live partitions. Fixed TTL retention deliberately keeps completed
history and may reject new writes until expiry. It trades capacity utilization for
simple protection of disconnected consumers. Full replication still does not increase
retained capacity as brokers are added, and a single hot group is not sharded.

The shared application's VMs were not drained or shut down. Namespace-local broker
replacement tests do not establish whole-host failure safety. Development image imports
and this draft PR are not release artifacts or replacement sign-off.
