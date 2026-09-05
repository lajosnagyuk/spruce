# Memory availability and graceful degradation

Status: accepted product contract, 2026-09-05. Implements the user's clarification
that Spruce is an elastic, memory-only transport, not a durable event log.

## Decision

Keep payloads and delivery checkpoints in bounded memory. Recover from retained
surviving copies as brokers disappear and return. Total loss of all copies, including
a complete cluster restart, may lose events. Do not introduce disk persistence merely
to claim a stronger guarantee than the product requires.

A running minority, including one broker, may continue carrying new traffic when the
producer explicitly selects availability. Do not silently weaken a requested replica
acknowledgement. The single-message modes are:

- `local`: local acceptance with existing bounded replication-queue admission.
- `one-peer`: local acceptance plus a peer acknowledgement; reject if no peer confirms.
- `available`: accept locally and attempt all configured peers within 100 ms. Report
  `confirmed_copies` and `degraded` (fewer confirmed copies than configured brokers).
  Queue unfinished copies within existing replication budgets. Recently failed peers
  have a one-second synchronous-attempt cooldown, cleared by successful background
  replication. Each peer admits at most 32 synchronous confirmation RPCs; excess
  attempts use the bounded background path in available mode or fail the strict
  confirmation requirement. Retries reassess copies rather than reuse an old success receipt.

A receipt counts confirmed acceptance during that request. It is not a lease on cache
residency: TTL, eviction and subsequent failures still apply. With two confirmed copies,
loss of those particular two brokers can lose the event even if other brokers survive.
An event with three retained copies can survive loss of two of those brokers. While
only one remains, new events cannot have redundant protection. A timeout has an unknown
outcome; retry the same operation identity.

`available` is opt-in, including in the SDKs and test driver. It does not apply to
binary batches yet. Its 100 ms bound covers waiting for peer confirmations; it is not
a bound on the whole HTTP request, admission, scheduling, delivery or queue convergence.

## Ordering and partitions

Availability on an isolated minority is incompatible with unconditionally exclusive
ownership shared with another live partition. Do not describe topic routing as a lock,
or introduce mandatory majority gating while claiming that one of three brokers can
always continue. Allow redelivery; never treat a NACK as completed work. Preserve
per-producer/key order in ordinary operation and explicitly measure regressions through
failover. Consumers need idempotent side effects; there is no exactly-once claim.

Further work should reduce overlap during group handoff and make replica placement
independent of total broker count. Current full replication still does not scale
retained capacity with nodes. A partitioned implementation needs bounded membership
handoff, explicit copy receipts, and recovery tests before replacing full replication.
Strict partition ownership, if offered later, must state its unavailable-partition
behaviour separately from this availability contract.

## Validation

Unit tests check three, two, one and recovered copies, stable retry identity, and
strict one-peer rejection with no reachable peers. The Kubernetes lifecycle driver
runs baseline, abrupt broker replacement and 3-to-1-to-3 scale transitions only in a
namespace labelled `spruce.io/test-environment=true`. It does not drain Kubernetes
hosts. Retained-copy loss and ordering observations remain separate acceptance checks.

## Drain and retry behaviour

Draining withdraws readiness, closes existing consumer streams and rejects new
publishes/subscriptions. ACKs and peer recovery traffic remain available during the
grace interval. The gateway retries stream handshakes on transient upstream errors,
refreshes DNS every second and uses a one-second failed-peer interval. Rendered gateway
configuration content is hashed into the Deployment so configuration changes restart
nginx rather than silently leaving the old routing rules running.

Publish retry defaults are eight attempts, with jittered exponential backoff starting
at 50 ms and capped at two seconds per wait (server Retry-After can request longer).
Caller cancellation/deadlines still apply where supported by the SDK. This allows
routing to move away from draining brokers; it does not retry permanent client errors
or generate a new operation ID. Python's synchronous retry API uses its configured
attempt budget and HTTP timeout rather than a cancellation token.

The gateway permits upstream POST retries only for single-message requests carrying
both producer ID and idempotency key. Binary batches and unidentified publishes keep
normal POST retry restrictions. A named Nginx location preserves the method, URI and
body; a real-container test checks both local and Helm configurations in ordinary CI.
See Nginx's [upstream retry rules](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_next_upstream)
and [named error locations](https://nginx.org/en/docs/http/ngx_http_core_module.html#error_page).
