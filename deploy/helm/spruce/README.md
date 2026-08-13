# Spruce Helm chart

This chart deploys bounded, interchangeable Spruce brokers and a stateless Nginx gateway.

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
| `tls.enabled` | `false` | Gateway-to-broker and peer HTTPS; plaintext requires explicit opt-in |
| `tls.existingSecret` | empty | TLS Secret with `tls.crt`, `tls.key`, and `ca.crt` |
| `config.cacheBytes` | `67108864` | Per-broker payload and metadata cache budget |
| `resources.requests.memory` | `96Mi` | Broker memory request |
| `resources.limits.memory` | `256Mi` | Broker memory hard limit |

When `auth.existingSecret` is empty, Helm generates and preserves credentials. Production
installations should use an externally managed Secret. See `docs/OPERATIONS.md` for the
three-stage current/previous token rotation protocol.

## Why the gateway exists

Kubernetes Service balancing cannot hash a streaming request by its `topic` and `group` query parameters. Without stable routing, replicas independently see partial group membership and can each deliver the same replicated message. The gateway hashes grouped streams by `topic+group`, distributes broadcast streams, and least-connection balances ordinary requests. It re-resolves StatefulSet DNS names so pod replacement cannot leave stale upstream addresses.

Client TLS should normally terminate at an Ingress or service mesh in front of the gateway. Enabling `tls.enabled` protects gateway-to-broker and broker-to-broker traffic. The shared broker certificate must be valid for the headless-Service DNS name and every StatefulSet pod DNS name; the gateway verifies it using `ca.crt`.

Gateway rollouts use `maxUnavailable: 0` and `maxSurge: 1`. Brokers withdraw readiness,
drain, and merge cache state from every reachable peer during ordered replacement. This reduces but
does not eliminate the in-memory bus's rolling-replacement loss window.
