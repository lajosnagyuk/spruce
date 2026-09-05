# Elastic memory transport: Andromeda validation

2026-09-05; branch `reliability/delivery-contract`, following `9e1e4e2`.
The implemented contract is [ADR 0002](adr/0002-memory-availability.md).

## Environment and oracle

An isolated `spruce-dev` namespace on Andromeda's five-node Debian 13/k3s cluster;
three broker pods placed on nodes 2, 4 and 5, two Nginx gateways. Other application
namespaces were not changed. Brokers use image `localhost/spruce:dev-bounded`
(containerd manifest `sha256:3a9ec573fa94b46fed6246a6c868472671b310ad795bcbe85859e9de1e4b2162`),
and the lifecycle tool uses `localhost/spruce-tools:dev-elastic`. The gateway config
includes identified-publish retries and is deployed through its content checksum.
These locally imported development images are not release artifacts.

Each paced scenario publishes 2,000 events at 50/s: four producers, four topics,
16 keys per producer, two groups per topic and two members per group. Payloads contain
256 bytes of padding plus JSON metadata; compression is off. Producers use stable
operation IDs, eight bounded SDK attempts and `ack=available`. Every accepted event
is checked independently in each group for intact content and first-observed
per-producer/key order. Duplicates are counted and allowed; missing events, publish
errors and ordering regressions fail the test. This does not measure database side
effects or establish global order.

## Failed trials drove the fixes

Keep these failures when interpreting the passing trials:

- Before drain changes, 3-to-1-to-3 delivered all accepted events, but recorded seven
  duplicates, 51 ordering regressions and 9.55 s delivery p99.
- Closing streams during drain, refreshing gateway DNS each second and shortening
  failed-upstream suppression eliminated observed regressions in the next scale trial,
  but one publish exhausted its retry budget with `503 broker_draining`.
- Eight SDK attempts alone still left two failed publishes in another scale trial.
- Nginx now retries a forwarded single-message POST only when both operation-identity
  headers are present. Real Nginx tests verify the original bytes, URI and headers
  survive that retry and that plain/batch POSTs are not retried. Both deployment
  configurations are covered in ordinary CI without spinning up Kubernetes.

## Final results

Results below are individual trials, not failure probabilities or an SLA.

| Scenario | Accepted / expected group deliveries | Publish errors / missing / duplicates / order regressions | Delivery p99 |
| --- | --- | --- | --- |
| 3 → 1 → 3, one broker for roughly 15 s | 2,000 / 4,000, all delivered | 0 / 0 / 0 / 0 | 2.03 ms |
| SIGKILL two broker containers on separate VMs | 2,000 / 4,000, all delivered | 0 / 0 / 0 / 0 | 2.20 ms |

Scale confirmed-copy counts: one=587, two=6, three=1,407. SIGKILL counts:
one=52, two=6, three=1,942. New events with only one confirmed copy remain vulnerable
to loss of that copy. Copy receipts count acceptances during the request, not a future
retention guarantee. Kubernetes restarted the killed processes; no VM was shut down.
An earlier force-deleted-pod trial also passed, but force deletion alone does not prove
immediate process death and is not our evidence for SIGKILL recovery.

## Burst behaviour and remaining limits

With all three brokers and two gateways, unpaced runs of 8,000 events confirmed three
copies for every publish and delivered all 16,000 expected group deliveries. They
reached 1,020 and 917 accepted events/s, with delivery p99 of 34 and 354 ms. However,
they also produced **3,965 and 5,993 duplicate group deliveries**. No first-observed
per-producer/key ordering regressions or publish errors occurred. The measured broker's
redelivery counter remained zero; overlapping group delivery is a working hypothesis,
not an established root cause. These are not clean throughput successes. The driver
allows counted duplicates because the delivery contract permits redelivery; that is
not a claim that this level of amplification is acceptable for an invisible backend.

A separate one-survivor unpaced run accepted all 8,000 events at 3,148/s and ultimately
reached both groups, but delivery p99 was **45.97 seconds**, with 1,115 duplicates and
680 ordering regressions. The ordering oracle correctly failed. Accept throughput alone
hides this delivery backlog; this is not evidence of useful horizontal scaling or
acceptable degraded burst latency. The harness restored three replicas afterward.

Full replication still multiplies copy work and stores every event on every broker.
Additional nodes have not been shown to increase aggregate retained capacity or useful
throughput. Two independent gateways can make independent failover decisions; topic
hashing does not enforce exclusive group ownership. Partition behaviour and overlapping
group handoff remain open work before a Kafka-replacement sign-off. Complete-cluster
memory loss is accepted product behaviour, not an outstanding durability requirement.

An initial short idle snapshot showed roughly 7–9 MiB per broker and 3–9 MiB per
gateway. After burst traffic, one metrics snapshot showed 76/47/41 MiB in brokers and
4 MiB per gateway, with 0.84 aggregate broker CPU cores. These snapshots include retained
test history and are neither averages nor peak-resource measurements. They must not
be compared directly with the earlier short, low-rate local cgroup sample.

## Reproduction and validation

```sh
python3 scripts/k3s-lifecycle.py --namespace spruce-dev \
  --image localhost/spruce-tools:dev-elastic --case scale
python3 scripts/k3s-lifecycle.py --namespace spruce-dev \
  --image localhost/spruce-tools:dev-elastic --case kill-two --ssh-user USER \
  --ssh-identity /path/to/key
python3 scripts/test-gateway-retry.py --engine podman
python3 scripts/test-gateway-retry.py --engine podman --helm
```

The tool image must be imported on the selected runner node; pass `--runner-node` if
needed. The fault harness refuses namespaces without the explicit test label and checks
that SIGKILL targets belong to the test StatefulSet. SSH requires noninteractive sudo
access to k3s containerd. It never kills unrelated containers or drains hosts.

Go tests/race/allocation gates, .NET conformance/package, Python's 20 tests and Helm
validation passed locally. The real-gateway tests pass for both configurations.
The legacy `k3s-hardening.sh` host-drain scenario was not run on the shared application
cluster; these namespace-local tests do not establish whole-host failure behaviour.
Raw records remain in `.cache/andromeda/gateway-*.log` on the development machine.
