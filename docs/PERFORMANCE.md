# Local performance record

These numbers are engineering baselines, not portable product claims.

## Reference environment

```text
host CPU       Apple M1 Pro
host OS        macOS, arm64
Go             1.26.5
topology       three scratch-image brokers plus nginx in Docker Compose
cache          64 MiB per broker
payload        256 bytes
publishers     16
replication    asynchronous to both peer brokers
```

## Results

### Single-message HTTP path

```text
messages/s     8,250
p50            1,211 us
p95            3,090 us
p99            4,938 us
```

This is the convenience path and pays for one HTTP request and JSON response per
message.

### Binary batch path, 512 messages/request

```text
messages       2,000,000
messages/s     539,339
payload MiB/s  131.67
amortized p50  23 us/message
amortized p95  57 us/message
amortized p99  92 us/message
```

All three replicas converged at the configured cache limit with identical message
counts. Peer replication used the bounded binary protocol and reported no drops in the
100,000-message convergence run.

The in-process 512-message handler benchmark measured:

```text
504 us/request
260 MB/s
364,051 bytes allocated/request
2,085 allocations/request
4.07 allocations/message
```

## Reproduction

```sh
make validate-local
go test -run '^$' -bench BenchmarkPublishBatch -benchmem -benchtime=2s ./internal/broker
./bin/spruce-bench -server http://localhost:8080 -n 2000000 -size 256 -workers 16 -batch 512
```

The short benchmark in `make validate-local` is a regression signal, not a hard
throughput gate, because developer hardware and concurrent workloads vary.

## Findings

- Binary ingress batching is essential; HTTP request overhead dominates single publish.
- Binary peer batching removed JSON/base64 amplification and improved replicated
  throughput by about 18% in this environment.
- The 6.23 MB scratch image is comfortably below the small-image objective.
- The current cache has bounded logical bytes but uses a Go object per message. During
  extreme continuous eviction, transient heap and indexes require substantially more
  RSS than payload capacity. Compose therefore has a 512 MiB hard limit for the 64 MiB
  stress-test cache.
- A payload-only arena was tested and rejected: it added a copy without removing index
  allocations and worsened retained memory. Any future arena design must combine direct
  request decoding, compact value indexes, and segment-level eviction.

## Performance rules

- Report accepted throughput and replica convergence together. Ingress throughput that
  silently overwhelms replication is not a valid HA result.
- Always include payload size, batch size, producer count, cache size, and replica count.
- Run correctness and race tests before comparative benchmarks.
- Do not compare amortized batch latency with end-to-end latency for an individual
  unbatched message.
- Full cache convergence uses the peer-authenticated internal digest diagnostic. It
  snapshots message references briefly under the cache lock, then hashes immutable
  message content after releasing the lock. A dedicated capacity-one gate rejects
  concurrent diagnostics, and routine status remains constant-time.

## Regression surface

`make perf-test` has hardware-independent allocation-count and allocation-byte gates. It
also executes hardware-dependent smoke samples for batch ingress, cache admission,
delivery framing, and consumer-group routing. `make benchmark` runs longer comparative
measurements across batch sizes, payload sizes, and group sizes. Absolute throughput is
reported rather than gated because shared CI hardware is not stable enough for a useful
latency threshold.

The first profile-guided optimization pools the 64 KiB batch parser and expires the
cache once per atomic batch. On the reference host parser pooling reduced a 512-message
request from roughly 351 KiB to 285 KiB allocated; the byte ceiling exists to
prevent that request-scoped memory from drifting back.

Removing the redundant expiry lookup map then reduced the same handler to roughly
381 us/request and 274 KiB/request on the reference host, about 23% faster and 22%
fewer allocated bytes than the original baseline. Messages link into an exact-deadline
expiry bucket; CI checks the bucket map, heap indexes, intrusive links, cache membership,
order, and topic indexes under sustained pressure.

Exact-deadline expiry buckets reduce a batch to one heap node without rounding TTLs;
the repeated 512-message handler reached roughly 288 us/request, about 42% faster than
the original baseline. Replication serialization uses an append-only buffer fast path,
while snapshot responses retain direct bounded streaming.

The fixed 128-bit message ID encoder measures about 26 ns and one 24-byte retained
string allocation. It is not a CPU hotspot, and replacing the internal representation
would expand the integrity-sensitive cache, replay, replication, delivery, status, and
deduplication surface. Compact binary IDs therefore remain rejected until retained-heap
profiles demonstrate that the allocation, rather than payload or indexes, is material.

After expiry bucketing and allocation-free steady-state peer encoding for encoded
batches up to the 1 MiB retained-buffer ceiling, the `spruce-dev`
three-broker scenario delivered 3,000 messages across six topics, 10 producers, three
broadcast consumers/topic, and five group members/topic with zero missing, duplicate,
invalid, or failed operations. The 100,000-message, 256-message batch run through
port-forward and both gateways measured 150,748 messages/s and 36.80 MiB/s; this remains
a single observation rather than a portable throughput claim. The run used the `perf2`
test tag built from the uncommitted working tree, with no warmup or repeated-sample
dispersion; the in-process repeated benchmarks are the comparative evidence.

Pending delivery state references the immutable message rather than embedding duplicate
topic, key, header, payload, and timestamp fields. Terminal pending objects are zeroed
and reused only after removal from the pending map and deadline heap. On the reference
host this reduced a 256-message publish/deliver/ACK cycle from roughly 229 KiB and 1,826
allocations to 164 KiB and 1,570 allocations, with no material latency regression.

Parallel 64-message ingress scaled from roughly 289 MB/s on one core to 335 MB/s on two
and 364 MB/s on four, then remained flat at eight. The global cache lock is therefore a
saturation ceiling rather than a collapse mode. Cache sharding is rejected for now:
two-core Kubernetes brokers remain below that point and horizontal replicas scale
without splitting the correctness-sensitive global eviction and expiry order.

## K3s topology baseline

Measured on the local `spruce-dev` K3s cluster on 2026-08-13. Each Debian 13 VM had
2 vCPU and 4 GiB RAM. Traffic crossed a local `kubectl port-forward`, two Nginx
gateways, and 1-3 Spruce brokers. Payloads were 256 bytes with 16 publishers.

| Brokers | Single msg/s | Batch msg/s | Batch size |
|---:|---:|---:|---:|
| 1 | 1,535 | 108,698 | 256 |
| 2 | 1,572 | 138,553 | 256 |
| 3 | 1,569 | 128,643 | 256 |

Each batch run published 100,000 messages after a 3,000-message single-request run.
Every broker converged at 103,000 messages and 39,864,000 accounted cache bytes. The
three-broker run reported zero replication errors, zero replication drops, and an empty
replication queue after convergence.

At that cache occupancy, broker RSS was 79-88 MiB and each gateway used 3 MiB. These
are regression baselines, not portable capacity claims; port forwarding and VM network
placement materially affect latency.

The steady-state mixed correctness matrix passed without missing deliveries. Raw
at-least-once duplicates were observed and are removed when the SDK deduper is enabled:

| Brokers | Topics | Messages | Producers | Broadcast/topic | Group members/topic |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 500 | 1 | 1 | 1 |
| 2 | 3 | 1,500 | 4 | 2 | 3 |
| 3 | 6 | 3,000 | 10 | 3 | 5 |

Reproduce a scenario after port-forwarding the Helm Service:

```sh
./bin/spruce-integration -server http://127.0.0.1:8080 \
  -topics 6 -messages 500 -producers 10 \
  -broadcast-consumers 3 -group-consumers 5
```

The hardened K3s failure matrix used six topics, 10 producers, three broadcast
consumers/topic, five group members/topic, and SDK deduplication. Abrupt broker deletion
and node drain each had zero missing logical deliveries at one aggregate message/second.
Full three-broker rolling replacements lost 24-62 of 480 expected logical deliveries
(5-12.9%) while publishes continued across repeated runs. This is an explicit
non-durable maintenance observation, not a portable SLA; pause publishers when
maintenance loss is unacceptable.

## Adaptive payload compression

An isolated three-broker K3s comparison used eight topics, 16 publishers, and payloads
of 4 KiB, 64 KiB, and 900 KiB containing repetitive JSON-shaped data. The offered rate
ramped through 100, 250, and 400 messages/second. Brokers were restarted before each
60-second case. Every case completed without missing, duplicate, or invalid payloads,
delivery drops, redeliveries, or replication drops.

| Codec | Accepted | Logical GiB | Publish p50/p95/p99 | Cache/messages per broker |
|---|---:|---:|---:|---:|
| off | 13,951 | 4.29 | 6.7 / 29.3 / 53.2 ms | ~66.3 MiB / 196-197 |
| gzip fastest | 14,912 | 4.59 | 1.2 / 4.0 / 12.9 ms | 39.3 MiB / 14,912 |
| zstd fastest | 14,802 | 4.55 | 1.9 / 11.9 / 27.3 ms | 4.7 MiB / 14,802 |

The first codec run constructed encoder state per message. Pooling that state changed a
subsequent identical 30-second comparison to gzip p50/p95/p99 of 0.89/2.58/6.12 ms and
zstd 0.54/1.34/3.01 ms. Sampled producer CPU was 323m for gzip and 395m for zstd;
producer memory was 17 MiB and 56 MiB respectively. Broker and gateway CPU decreased
substantially for both codecs because they handled fewer wire bytes.

These deliberately compressible payloads represent an upper-bound benefit, not a
portable claim for arbitrary events. Adaptive mode skips payloads below 1 KiB and keeps
the original bytes unless compression saves at least 10% and 128 bytes. Incompressible
payloads therefore preserve wire/cache size but still pay the attempted codec CPU cost.

A separate 15-second run NACKed every 50th first delivery. All planned retries completed
without missing, duplicate, or invalid payloads. First-delivery p50/p95/p99 was
12.9/52.7/104.6 ms raw, 1.14/5.31/9.72 ms with pooled gzip, and
0.93/4.36/8.16 ms with pooled zstd. NACK-to-redelivery p50 remained 0.51-0.53 seconds
and p99 0.98-1.01 seconds for every codec, showing that the broker retry schedule, not
compression, dominates redelivery latency.

Zstandard support was subsequently completed in the C# and Python SDKs. Their live
cluster probes each delivered 100 exact mixed raw, gzip, and Zstandard messages through
the three-broker sandbox without loss or corruption. On the same Apple M1 Pro used for
the Go measurements, pooled Python Zstandard compression sustained approximately
2.39/7.10/10.85 GiB/s for 4 KiB/64 KiB/900 KiB compressible payloads, compared with
0.20/0.86/0.82 GiB/s for gzip. These codec microbenchmarks exclude HTTP and broker work;
the cluster measurements above remain the end-to-end evidence for latency, CPU, and
cache effects.

A later ten-minute Zstandard curve varied 4 KiB, 64 KiB, and 900 KiB payloads from
100 to 1,200 requested messages/s and back down. It verified 292,969 messages and
90.15 GiB byte-for-byte with no missing, duplicate, invalid, publish, subscription, or
overload result. Achieved throughput was producer-limited to 488 messages/s; publish
p50/p95/p99 was 0.60/7.13/24.86 ms and first-delivery was 1.95/9.78/28.19 ms.

A four-producer request-rate test exposed the bounded-retention edge at approximately
1,700 accepted 4 KiB messages/s: broker CPU approached two cores, best-effort third
replica queues shed copies while every local publish still reserved one peer, and active
consumers could not drain all messages before the deliberately short two-minute TTL.
This is capacity evidence, not a portable throughput claim. Operators must size TTL for
the worst credible catch-up interval and alert on replication drops and delivery latency.
The test also exposed a gateway mismatch: publishes and subscriptions used different
hash keys, so a dropped best-effort replica copy could miss the consumer-owning broker.
The gateway now hashes both paths by topic, keeping ordinary delivery local to the same
owner while retaining replication for replay and replacement.

After correcting the topic extraction to use an Nginx named capture and adding
topic-local one-second delivery-lag admission, four independent producers completed
145,127 accepted 4 KiB messages at 2,417 messages/s aggregate. Every accepted message
was delivered exactly once with no transport, subscription, corruption, or expiry
failure; one publish received the intended retryable overload response. This was a
short saturation validation on the sandbox topology, not a production throughput SLA.

## Key-affinity hot-topic pressure

A three-broker K3s curve used one topic, four consumer-group members, 128 keys, 32
unbatched HTTPS publishers, and compressible 4 KiB events. Requested load changed every
25 seconds through 500, 1,500, 3,000, 5,000, 3,000, and 1,000 messages/second. The
generator reached approximately 2,800 messages/second at its highest phase.

With a 256 MiB container limit and cache accounting biased toward payload bytes, one
broker reached 255 MiB RSS and was OOM-killed. Consumers then correctly failed closed
with `cursor_expired` rather than skipping the replacement broker's uncertain history.
Before termination, 249,979 unique deliveries had zero duplicates, corruption, or key
ownership changes.

The cache now reserves a conservative per-message structural allowance, the default pod
limit is 320 MiB with 144 MiB explicitly reserved outside the 176 MiB Go soft limit,
and SDK subscription attempts retain a client-lifetime affinity identity across HTTP
reconnects. Repeating the same curve accepted and delivered all 279,116 messages with
zero missing, duplicate, invalid, overload, publish, subscription, or key-movement
result. Delivery p50/p95/p99 was 169/235/356 ms, peak single-broker CPU was 1.75 cores,
peak sampled RSS was 211 MiB, and no broker restarted. These are sandbox observations,
not portable capacity guarantees.

A follow-up structural-overhead case removed the 4 KiB body and sent 188,997 tiny keyed
messages over two minutes, maximizing retained message/index objects per logical byte.
Every message was accepted and delivered exactly once with no invalid result, key move,
cursor expiry, or restart. Container cgroup `memory.peak` was 13.4, 15.0, and 50.8 MiB
across the three brokers; the topic owner recorded the highest value. This bounds the
tested tiny-message shape but does not replace workload-specific capacity testing.
