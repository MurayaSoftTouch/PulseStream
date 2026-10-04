# PulseStream Architecture Overview

Status: **M3 (Retry, dead-letter, and failure handling)**.
**IMPLEMENTED** means it is in the code and covered by tests. **PLANNED** means
it is a design intention only.

**PulseStream provides durable admission with at-least-once processing.** It
does not provide exactly-once processing, and retries do not change that.

## Data plane

```text
Client
  |
  |  POST /v1/events  (Idempotency-Key required)
  v
PulseStream API ............................. IMPLEMENTED
  |
  |  one transaction: INSERT ... ON CONFLICT DO NOTHING, then fingerprint check
  |  202 only after COMMIT
  v
PostgreSQL `events` table (source of truth) . IMPLEMENTED
  |
  |  eligible: PENDING with available_at <= now()   (database clock)
  |         or PROCESSING with an expired lease and attempts left
  v
claim: FOR UPDATE SKIP LOCKED, at most free capacity, delivery_attempts + 1
  |
PROCESSING .................................. IMPLEMENTED: owner ID, lease
  |
  +--> success ------------------------> PROCESSED ........... IMPLEMENTED (owner-checked)
  |
  +--> retryable failure, attempts left
  |       |
  |       v
  |    PENDING + available_at = now + backoff ................ IMPLEMENTED (owner-checked)
  |
  +--> permanent failure, or retryable on the final attempt
  |       |
  |       v
  |    DEAD_LETTERED ......................................... IMPLEMENTED (owner-checked, terminal)
  |
  +--> lease expiry -> reclaim (no backoff) .................. IMPLEMENTED (crash recovery)
          on the final attempt -> DEAD_LETTERED (LEASE_EXPIRED)
```

The API and the worker are separate processes. They communicate only through
PostgreSQL, as ADR-002 intended. The M1 in-memory queue (ADR-006) has been
removed.

### Components

| Component | Where | Status |
| --- | --- | --- |
| HTTP admission | `pulsestream-api` `events.rs` | **IMPLEMENTED.** Validation, 64 KiB limit, required `Idempotency-Key`, `202`/`409`/`503` |
| Status lookup | `GET /v1/events/{event_id}` | **IMPLEMENTED.** Metadata only, never the payload |
| Event model, idempotency key, fingerprint | `pulsestream-core` | **IMPLEMENTED.** No HTTP, database, or runtime dependencies |
| Durable store | `pulsestream-store` + `migrations/` | **IMPLEMENTED.** Admission, claim, complete, retry, dead-letter, status. Schema constraints and immutability trigger |
| Worker runtime | `pulsestream-worker` `runtime.rs` | **IMPLEMENTED.** Bounded claims, leases, graceful shutdown, failure handling |
| Retry policy, backoff, dead-letter, poison events | `pulsestream-core` `retry.rs`, `failure.rs` | **IMPLEMENTED** ([ADR-008](../adr/ADR-008-retry-scheduling-and-dead-letter-policy.md)) |
| Readiness | `GET /health/ready` | **IMPLEMENTED.** The `database` check |
| Dead-letter inspection API and redrive | | **PLANNED** (M5/M6, behind authentication) |
| Admission backlog limits, benchmarks, load tests | | **PLANNED** (M4) |
| Authentication and authorization, metrics, TLS hardening | | **PLANNED** (M5) |
| Operations dashboard | | **PLANNED** (M6) |

### Acceptance and idempotency

- `202 Accepted` means the event row **has committed** to PostgreSQL. A
  database failure never produces `202`.
- Scope: `UNIQUE (source, idempotency_key)`. The same key under different
  sources gives independent events.
- Exact replay (same scoped key, same fingerprint) returns `202` with the
  original event ID and `Idempotency-Replayed: true`. New events carry
  `Idempotency-Replayed: false`.
- Conflicting reuse (same scoped key, different fingerprint) returns `409
  IDEMPOTENCY_CONFLICT`. The stored event is never changed.
- Fingerprint: SHA-256 over a versioned encoding of `source`, `event_type`, and
  the canonical payload. Object keys are sorted recursively and array order is
  significant.

### Processing and recovery

- A worker claims at most `concurrency - active` rows per statement and never
  prefetches. Live tasks never exceed `concurrency` (default 4).
- Claims record `processing_owner` (the worker's ID), `lease_expires_at` (now +
  30 s by default), and `delivery_attempts + 1`.
- Completion requires that the worker still owns the claim, so a stale worker
  cannot overwrite a reclaimed event.
- If a worker crashes, its rows stay `PROCESSING`. After the lease expires, any
  worker reclaims them.
- Shutdown: claiming stops, active events finish within the timeout, and
  anything left is abandoned to lease recovery. Rows are not reset.

**At-least-once example.** A worker performs an external effect, then crashes
before `PROCESSED` commits. After the lease expires, the event is reclaimed and
the effect runs again. Processors with side effects must be idempotent.

### Failure handling

- A processor returns a **retryable** or **permanent** failure with a stable
  code. A panic counts as retryable (`PROCESSOR_PANICKED`).
- `delivery_attempts` counts claims. An event is processed at most
  `PULSESTREAM_MAX_DELIVERY_ATTEMPTS` (default 5) times.
- A retryable failure with attempts left sets the event back to `PENDING`
  with `available_at = now + min(max, base × 2^(n−1)) × [0.8, 1.0]`. The
  jitter is deterministic per event and attempt.
- A permanent failure, or a retryable failure on the final attempt, moves the
  event to `DEAD_LETTERED`. It stays in the `events` table with its content,
  attempt count, and bounded failure metadata, and it is never claimed again.
- Every outcome write (complete, retry, dead-letter) requires
  `status = 'PROCESSING' AND processing_owner = <me>`. A stale worker changes
  nothing.
- **Crash recovery vs. retry.** Lease expiry means no outcome was recorded, so
  the event is reclaimed without backoff. A retryable failure was recorded,
  so the event waits for its backoff. An expired lease on the final attempt
  is dead-lettered with `LEASE_EXPIRED`, so a worker-crashing poison event
  still terminates.

### Boundedness

| Resource | Bound |
| --- | --- |
| API database connections | `PULSESTREAM_DB_MAX_CONNECTIONS` (default 10). Requests wait at most `PULSESTREAM_DB_ACQUIRE_TIMEOUT_MS` (default 3000), then get `503` |
| Statement duration | `statement_timeout = 10s` on every connection |
| Worker in-memory events | `PULSESTREAM_WORKER_CONCURRENCY` (default 4) per worker |
| Processing attempts per event | `PULSESTREAM_MAX_DELIVERY_ATTEMPTS` (default 5) |
| Retry delay | `PULSESTREAM_RETRY_MAX_DELAY_MS` (default 60000) |
| Stored failure message | 1024 characters |
| Durable backlog | Bounded by PostgreSQL storage only. Admission-side limits are planned for M4 |

## Operational plane

- **Liveness (IMPLEMENTED).** `/health/live` returns `200` whenever the process
  runs, including during a database outage.
- **Readiness (IMPLEMENTED).** `/health/ready` runs `SELECT 1` with a 2 s
  timeout. It returns `200` with `checks.database = "ready"`, or `503` with
  `"unavailable"`. The pool validates connections before use, so readiness
  recovers after a database restart without restarting the API. Processing
  failures and dead-lettered events never affect readiness.
- **Lifecycle logs (IMPLEMENTED).** The logged events are: event persisted,
  idempotency replay, idempotency conflict, event claimed, retry claimed,
  expired lease reclaimed, event processed, processing failed, retry
  scheduled, event dead-lettered (`reason`: `permanent_failure`,
  `retry_exhausted`, or `lease_expired_on_final_attempt`), claim lost, and
  database unavailable. Failure logs carry a `lifecycle` field with these
  names. Fields include event ID, source, type, worker ID, attempt, failure
  code, delay, and next available time. Payloads, raw idempotency keys,
  failure messages, and connection strings are never logged.
- **Metrics (PLANNED, M4/M5)**, an **operations dashboard (PLANNED, M6)**, and
  **load and benchmark evidence (PLANNED, M4)**.

## Architectural principles

1. **Boundedness.** No unbounded queue, channel, prefetch, or task-spawning
   loop ([ADR-004](../adr/ADR-004-bounded-concurrency-backpressure.md)).
2. **Backpressure.** Overload produces explicit behavior (`503` after a bounded
   wait) rather than uncontrolled memory growth.
3. **Durability.** `202` means committed. Accepted events survive process
   failure ([ADR-007](../adr/ADR-007-postgresql-durable-admission-and-leases.md)).
4. **Idempotency.** Producer retries with the same key never create a second
   event. The database enforces this.
5. **Explicit delivery semantics.** Durable admission with at-least-once
   processing. No exactly-once claims.
6. **Failure visibility.** Lifecycle state, attempt counts, and the last
   failure are persisted. Dead-lettered events stay queryable.
7. **Measured performance.** No performance claims without benchmarks (M4).

## Process model

```text
pulsestream-api ─────┐                      ┌── pulsestream-core (domain, config, idempotency)
                     ├── pulsestream-store ─┤
pulsestream-worker ──┘   (sqlx, migrations) └── PostgreSQL
```

`pulsestream-core` has no HTTP, database, or runtime dependencies.
`pulsestream-store` holds all SQL. Both binaries run migrations at startup by
default (`PULSESTREAM_MIGRATE_ON_START`). The migrator takes an advisory lock,
so concurrent startups are safe.
