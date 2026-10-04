# ADR-006: In-memory bounded admission semantics (M1)

- Status: Superseded by [ADR-007](ADR-007-postgresql-durable-admission-and-leases.md) (M2)
- Date: 2026-09-25
- Milestone: M1

> **Historical.** This records the M1 design. From M2, admission is durable in
> PostgreSQL and the in-memory queue described here no longer exists. The
> M1 boundedness principles carry over into ADR-007.

## Context

M1 introduces the first event path: HTTP ingestion, then admission, then
processing. The durable event store is not designed until M2 (ADR-003,
ADR-005). We still want M1 to establish the parts that don't depend on
durability: explicit bounds (ADR-004), deterministic overload behavior,
graceful drain, and tests that prove them.

## Decision

1. **Queue.** Admission uses one Tokio bounded `mpsc` channel with capacity
   `PULSESTREAM_QUEUE_CAPACITY` (default 256, valid range 1–65536).
2. **No waiting at admission.** The API calls `try_send`. A full queue
   returns `429 QUEUE_FULL` with `Retry-After: 1` immediately. The event is
   not admitted and is never silently dropped after a `202`.
3. **Concurrency.** A single dispatcher task consumes the queue. It dequeues
   an event only while its `JoinSet` holds fewer than
   `PULSESTREAM_WORKER_CONCURRENCY` tasks (default 4, valid range 1–64). A task
   is removed from the set only when the dispatcher joins it. The code spawns
   no per-event tasks outside this bound.
4. **Occupancy bound.** At most `queue_capacity + worker_concurrency` events are
   held in memory: queued, plus being processed. This bounds the logical
   backlog, not exact bytes. Each event is at most 64 KiB of request body plus
   overhead.
5. **Acceptance semantics.** `202 Accepted` means *validated and admitted to
   the bounded in-memory pipeline*. It does **not** mean persisted, durable,
   processed, or exactly-once. **M1 acceptance is process-local and
   non-durable. A process crash may lose accepted but unfinished events.**
6. **Shutdown.** On SIGINT/SIGTERM, the shutdown order is:
   1. Admission closes and new events get `503 PROCESSING_UNAVAILABLE`.
   2. The HTTP server stops.
   3. The queue receiver is closed and buffered events drain.
   4. In-flight tasks finish.

   The drain is bounded by `PULSESTREAM_SHUTDOWN_TIMEOUT_MS` (default 10000).
   On timeout, the remaining tasks are aborted, the abandoned counts are
   logged, and the process exits with code 1.
7. **Placement.** In M1 the pipeline runs inside the API process. It is
   implemented in the `pulsestream-worker` library, so the M2 worker binary can
   reuse the same bounded execution model. This is a deliberate, temporary
   exception to ADR-002's rule that processes communicate only through the
   durable store.
8. **Failures.** A processor error or panic is logged with the event ID and
   counted. The event is not retried; retries and dead-lettering come in M3.
   A panic does not stop other processing.

## Consequences

- Overload behavior is explicit and tested: the concurrency bound, a full
  queue returning 429, closed admission returning 503, drain on shutdown, and
  drain timeout.
- Producers must treat `429` and `503` as retryable. Without idempotency (M2),
  retrying a request that actually succeeded can create a duplicate logical
  event.
- M2 replaces the admission point with durable acceptance. The HTTP contract
  (`202`, error codes) is designed to carry over, but the meaning of `202`
  will become stronger and must be re-documented then.
- Defaults are conservative development values, not tuned numbers. Tuning
  waits for M4 benchmarks.
