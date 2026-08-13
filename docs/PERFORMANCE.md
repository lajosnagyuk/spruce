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

The mixed correctness matrix also passed without missing or duplicate delivery:

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
