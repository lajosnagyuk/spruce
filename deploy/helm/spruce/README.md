# Spruce Helm chart

This chart deploys bounded, interchangeable Spruce brokers and a stateless Nginx gateway.

```sh
helm upgrade --install spruce deploy/helm/spruce \
  --namespace spruce \
  --create-namespace \
  --set image.tag=v0.1.0 \
  --set replicaCount=3
```

## Important values

| Value | Default | Purpose |
|---|---:|---|
| `replicaCount` | `3` | Interchangeable broker replicas |
| `gateway.enabled` | `true` | Correct topic/group stream routing across replicas |
| `gateway.replicaCount` | `2` | Stateless routing availability |
| `image.repository` | `ghcr.io/lajosnagyuk/spruce` | Broker image |
| `image.tag` | `v0.1.0` | Broker version; pin an immutable tag |
| `auth.existingSecret` | empty | Existing Secret containing `peer-token` and `cluster-id` |
| `auth.clientToken` | empty | Optional public API bearer token |
| `tls.enabled` | `false` | Broker and peer HTTPS |
| `tls.existingSecret` | empty | TLS Secret with `tls.crt`, `tls.key`, and `ca.crt` |
| `config.cacheBytes` | `67108864` | Per-broker payload and metadata cache budget |
| `resources.requests.memory` | `96Mi` | Broker memory request |
| `resources.limits.memory` | `256Mi` | Broker memory hard limit |

When `auth.existingSecret` is empty, Helm creates a random peer token and preserves it across upgrades. Production installations should use an externally managed Secret.

## Why the gateway exists

Kubernetes Service balancing cannot hash a streaming request by its `topic` and `group` query parameters. Without stable routing, replicas independently see partial group membership and can each deliver the same replicated message. The gateway hashes grouped streams by `topic+group`, distributes broadcast streams, and least-connection balances ordinary requests. It re-resolves StatefulSet DNS names so pod replacement cannot leave stale upstream addresses.

Client TLS should normally terminate at an Ingress or service mesh in front of the gateway. Enabling `tls.enabled` protects gateway-to-broker and broker-to-broker traffic. The shared broker certificate must be valid for the headless-Service DNS name and every StatefulSet pod DNS name; the gateway verifies it using `ca.crt`.
