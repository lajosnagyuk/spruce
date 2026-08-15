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
  replicationQueueBytes`; the default 256 MiB limit is sized for the default 64 MiB cache.

## Delivery and maintenance

Spruce is at-least-once while data remains in bounded replica memory. Client deduplication
is required when duplicate handler execution is unsafe. Group ACK checkpoints suppress
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
