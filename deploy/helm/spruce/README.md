# Spruce Helm chart

This chart deploys bounded, interchangeable Spruce brokers and a stateless Nginx gateway.
The gateway consistently routes publishes and subscriptions for the same topic to one
broker. Partition keys retain per-key ordering within that topic owner; replication is
used for bounded replay and broker replacement rather than ordinary delivery routing.
Release charts are published as `oci://ghcr.io/lajosnagyuk/charts/spruce` with the
release semantic version as the OCI tag.

```sh
helm upgrade --install spruce deploy/helm/spruce \
  --namespace spruce \
  --create-namespace \
  --set image.digest=sha256:<release-digest> \
  --set auth.existingSecret=spruce-auth \
  --set tls.enabled=true \
  --set tls.existingSecret=spruce-internal-tls \
  --set ingress.enabled=true \
  --set ingress.hosts[0].host=spruce.example.com \
  --set ingress.tls[0].secretName=spruce-public-tls \
  --set ingress.tls[0].hosts[0]=spruce.example.com
```

The default release produces `spruce`, `spruce-headless`, and `spruce-admin`
Services in the `spruce` namespace. `nameOverride` and `fullnameOverride` remain
available when platform naming policy requires different resource names.

Label the Ingress controller namespace before installation so the default NetworkPolicy
admits it: `kubectl label namespace ingress-nginx spruce.io/ingress-access=true`.

## Important values

| Value | Default | Purpose |
|---|---:|---|
| `replicaCount` | `3` | Interchangeable broker replicas |
| `gateway.enabled` | `true` | Correct topic/group stream routing across replicas |
| `gateway.replicaCount` | `2` | Stateless routing availability |
| `image.repository` | `ghcr.io/lajosnagyuk/spruce` | Broker image |
| `image.digest` | empty | Immutable broker digest; required outside local `pullPolicy=Never` development |
| `auth.existingSecret` | empty | Secret containing peer, cluster, client, and admin credentials |
| `auth.requireExistingSecret` | `false` | Fail rendering unless externally managed credentials are selected |
| `clusterDomain` | `cluster.local` | Kubernetes cluster DNS suffix |
| `tls.enabled` | `false` | Gateway-to-broker and peer HTTPS; plaintext requires explicit opt-in |
| `tls.existingSecret` | empty | TLS Secret with `tls.crt`, `tls.key`, and `ca.crt` |
| `config.cacheBytes` | `67108864` | Per-broker payload and metadata cache budget |
| `config.goMemoryLimit` | `176MiB` | Go runtime soft memory limit; must leave the safety margin below the pod limit |
| `config.memorySafetyMarginBytes` | `150994944` | Space reserved for stacks, executable mappings, allocator overhead, TLS, and other non-heap memory |
| `config.deliveryLagLimit` | `1s` | Oldest unacknowledged topic delivery age before new publishes receive retryable overload |
| `resources.requests.memory` | `96Mi` | Broker memory request |
| `resources.limits.memory` | `256Mi` | Broker memory hard limit |
| `topologySpreadConstraints` | `true` | Require brokers on distinct nodes; disable explicitly only when reduced failure isolation is acceptable |

When `auth.existingSecret` is empty, Helm generates credentials and preserves them only
during a live Helm upgrade where `lookup` can read the existing Secret. Offline/GitOps
rendering cannot preserve generated values. Production and GitOps installations should
set `auth.requireExistingSecret=true` and use an externally managed Secret. See
`docs/OPERATIONS.md` for the three-stage current/previous token rotation protocol.

The chart rejects memory configurations where `config.goMemoryLimit` plus
`config.memorySafetyMarginBytes` exceeds the pod limit. It also requires the cache,
replication queue, action queue, inflight delivery, publish admission, and safety-margin
budgets to fit inside that limit. These are conservative simultaneous upper bounds, not
a prediction of steady-state RSS. Re-measure peak RSS after changing message sizes or
concurrency. The default broker spread is strict: a three-replica release needs three
eligible Kubernetes nodes. Set `topologySpreadConstraints=false` only for development
or when scheduling availability is deliberately preferred over replica failure isolation.

The default NetworkPolicy expects CoreDNS at `kube-dns.kube-system` with label
`k8s-app=kube-dns`, and permits clients selected by `spruce.io/client-access=true`.
Override `networkPolicy.dns`, `networkPolicy.ingressControllerIngress`, and
`clusterDomain` for other DNS, Ingress, or service-mesh layouts. Short-lived Jobs should
retry the readiness endpoint before publishing because policy selectors can converge
after Pod startup.

`scripts/k3s-soak.sh` requires `SPRUCE_TOOLS_IMAGE` to name an image pullable by every
node, preferably by immutable digest. A node-local `spruce:tools` image is suitable only
when explicitly supplied with `SPRUCE_TOOLS_PULL_POLICY=Never` and preloaded everywhere.
The soak defaults to verified broker HTTPS using `SPRUCE_TLS_SECRET`; plaintext gateway
testing requires the explicit local-only `SPRUCE_ALLOW_INSECURE=1` override.

## Why the gateway exists

Kubernetes Service balancing cannot hash a streaming request by its `topic` and `group` query parameters. Without stable routing, replicas independently see partial group membership and can each deliver the same replicated message. The gateway hashes grouped streams by `topic+group`, distributes broadcast streams, and least-connection balances ordinary requests. It re-resolves StatefulSet DNS names so pod replacement cannot leave stale upstream addresses.

Client TLS should normally terminate at an Ingress or service mesh in front of the gateway. Enabling `tls.enabled` protects gateway-to-broker and broker-to-broker traffic. The shared broker certificate must be valid for the headless-Service DNS name and every StatefulSet pod DNS name; the gateway verifies it using `ca.crt`.

Gateway rollouts use `maxUnavailable: 0` and `maxSurge: 1`. Brokers withdraw readiness,
drain, and merge cache state from every reachable peer during ordered replacement. This reduces but
does not eliminate the in-memory bus's rolling-replacement loss window.
