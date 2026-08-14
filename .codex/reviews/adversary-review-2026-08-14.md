# Adversary review: performance delta

Date: 2026-08-14

## Result

| Review | Final result |
|---|---|
| Correctness and concurrency | STABLE |
| Memory and availability | STABLE |
| Tests, benchmarks, and operations | STABLE |

No unresolved Critical, Major, or Minor findings remain.

## Findings and resolutions

- Batch allocation regression coverage initially measured allocation count but not
  bytes. Added independent allocation-count and allocation-byte ceilings.
- The consumer-group benchmark bypassed production pending cleanup. It now performs
  complete terminal bookkeeping and returns objects to the pool.
- Local throughput validation initially checked ingress only. It now requires drained
  replication queues, unchanged drop counters, equal message counts, and equal
  content-sensitive cache digests across all three brokers.
- Expiry and pending pools lacked direct lifecycle invariants. Added structural expiry
  checks and tests for ACK, NACK/retry, timeout, cache pressure, and object scrubbing.
- Cache convergence hashing initially ran under the cache lock on `/v1/status`. It is
  now a peer-authenticated internal diagnostic that snapshots references briefly and
  hashes immutable content after releasing the lock; routine status is constant-time.
- Concurrent authenticated digest requests could multiply CPU and memory use. A
  dedicated capacity-one gate now rejects concurrent work with HTTP 429 before any
  snapshot allocation.

## Review themes

- Preserve immutable message ownership across cache, pending delivery, retry, and
  replication paths.
- Gate performance regressions with hardware-independent allocation invariants and
  use hardware-dependent benchmarks as comparative evidence only.
- Never report ingress throughput without proving replica convergence and zero new
  replication drops.
- Keep diagnostic work isolated from publication, delivery, and replication capacity.

## Priority

The reviewed performance work is ready for deployment validation and release. Further
optimization should be profile-led; low-impact micro-optimizations are intentionally
deferred.
