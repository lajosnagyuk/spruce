# Resilience and delivery distribution: follow-up evidence

2026-09-05, following `9adbd65`. Product direction is recorded in
[ADR 0003](adr/0003-resilience-and-delivery-capacity.md). Earlier failed trials remain
in [the initial cluster report](ELASTIC-MEMORY-RESULTS.md); this report does not erase them.

## Repairs and routing change

A deterministic regression test reproduced a replay-tail defect: an event arriving
after the replay snapshot remained buffered until later traffic or heartbeat. Deferred
replay now flushes at the same bounded frame/byte thresholds as initial replay and
flushes its tail before waiting for live traffic.

The earlier ACK forwarding rule also conflicted with minority availability. An owner
now records local completion even when an unreachable peer's bounded checkpoint queue
rejects admission. Both public and forwarded ACK tests check that pending state is
removed, the local checkpoint survives, drop counters increase and queue accounting is
released. Checkpoint loss can still cause duplicates after owner failure.

Gateways now distribute independent topic/group streams across brokers, and SDK
completion requests carry the same canonical affinity digest. This distributes delivery
work while full memory replication provides copies. Real Nginx tests check that 20
groups on one topic use all three upstreams, that ACK/NACK reaches the stream's upstream,
and that identified-publish retry boundaries remain intact. UTF-8 test vectors check
identical digest encoding in Go, .NET and Python. This establishes routing behaviour,
not exclusive ownership or measured linear throughput scaling.

## Cluster trials

The same isolated `spruce-dev` namespace on Andromeda: three brokers on nodes 2/4/5,
two gateways, 256 bytes of padding plus JSON metadata, compression off, four producers,
four topics, two groups per topic, two members per group, 16 keys per producer.
Each run publishes 8,000 events and expects 16,000 group deliveries. Clients use
`ack=available` and stable operation identities. The oracle counts duplicates and
checks first-observed per-producer/key order; it does not prove ordered external
side effects or completion-gated scheduling. These are individual trials, not an SLA.

Broker image `localhost/spruce:dev-completion`, containerd manifest
`sha256:d1f0764d9100ab1ec5896761f12c1632723dc8f02815b07a0bc2b3f4114f1695`.
Final tool image `localhost/spruce-tools:dev-affinity`, manifest
`sha256:40e8e277c533997657005fa7e1598e8fbebbb880c7b712dd672ffc39eeba14a1`.
The first two repair trials use the older tool/gateway routing to isolate those repairs.

| Trial | Accepted/s | Delivery p99 | Publish errors / missing / duplicates / order regressions |
| --- | ---: | ---: | --- |
| Repairs only, three-broker burst | 1,242 | 11.48 ms | 0 / 0 / 0 / 0 |
| Repairs only, one-survivor burst | 1,438 | 907.12 ms | 0 / 0 / 0 / 0 |
| Initial topic/group routing, three-broker burst | 1,285 | 21.55 ms | 0 / 0 / 0 / 0 |
| Final canonical routing, three-broker burst | 1,182 | 13.63 ms | 0 / 0 / 0 / 0 |
| Final canonical routing, 3 → 1 → 3 at 200/s | 200 | 2.50 ms | 0 / 0 / 0 / 0 |
| Final canonical routing, two-container SIGKILL at 200/s | 200 | 2.15 ms | 0 / 0 / 0 / 0 |

The prior one-survivor burst had 45.97 s p99, 1,115 duplicates and 680 order regressions.
The clean repair trial is encouraging, but comparing accepted/s alone would have made
the prior broken run appear faster. The first two repair trials confirmed three and
one copies respectively for every event. The final scale trial reported one=2,771,
two=247, three=4,982; reduced copy protection remains explicit. The SIGKILL trial reported one=74, two=173,
three=7,753. Kubernetes restarted both killed processes; all three brokers are restored.

## Limits and next acceptance work

Per-key completion gating, pressure protection for unfinished retained work, bounded
disconnected-group registration, network partition/heal and repeated recovery with
slow/NACKing consumers remain unfinished. Healthy bursts no longer reproduce the earlier
large amplification, but there is no general duplicate-free or failover-order guarantee.
Full replication still limits retained capacity; group distribution does not split a
single hot group across brokers. Migration between old and new SDK affinity schemes
needs coordinated stream reconnection to limit overlapping group placement.

Raw results are `.cache/andromeda/completion-*.log`, `groups-*.log` and `affinity-*.log`.
No application namespace or VM was disrupted. The legacy host-drain hardening script
was not run against the shared application cluster.

Local Go tests/race and allocation gates, .NET conformance/package, Python 21 tests,
Helm validation and real Nginx tests passed. The deferred-tail regression was observed
failing before the flush repair and passing afterward. Source changes and development
images remain on a draft PR; this is not production replacement sign-off.
