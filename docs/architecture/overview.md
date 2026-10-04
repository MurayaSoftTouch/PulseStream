# PulseStream Architecture Overview

Status: **M2 (Durable persistence, idempotency, and crash recovery)**.
**IMPLEMENTED** means it is in the code and covered by tests. **PLANNED** means
it is a design intention only.

**PulseStream provides durable admission with at-least-once processing.** It
does not provide exactly-once processing.

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
PostgreSQL `events` table (source of truth) . IMPLEMENTED: PENDING -> PROCESSING -> PROCESSED
  |
  |  atomic claim: FOR UPDATE SKIP LOCKED, at most free capacity
  v
Bounded worker runtime(s) ................... IMPLEMENTED: owner ID, lease, conditional completion
  |
  +--> PROCESSED ............................ IMPLEMENTED (owner-checked)
  |
  +--> lease expiry -> reclaim .............. IMPLEMENTED (crash recovery; delivery_attempts + 1)
  |
  +--> retry with backoff ................... PLANNED (M3)
  |
  +--> dead-letter .......................... PLANNED (M3)
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
| Durable store | `pulsestream-store` + `migrations/` | **IMPLEMENTED.** Admission, claim, complete, status. Schema constraints and immutability trigger |
| Worker runtime | `pulsestream-worker` `runtime.rs` | **IMPLEMENTED.** Bounded claims, leases, graceful shutdown |
| Readiness | `GET /health/ready` | **IMPLEMENTED.** The `database` check |
| Retry policy, backoff, dead-letter, poison events | | **PLANNED** (M3) |
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

### Boundedness

| Resource | Bound |
| --- | --- |
| API database connections | `PULSESTREAM_DB_MAX_CONNECTIONS` (default 10). Requests wait at most `PULSESTREAM_DB_ACQUIRE_TIMEOUT_MS` (default 3000), then get `503` |
| Statement duration | `statement_timeout = 10s` on every connection |
| Worker in-memory events | `PULSESTREAM_WORKER_CONCURRENCY` (default 4) per worker |
| Durable backlog | Bounded by PostgreSQL storage only. Admission-side limits are planned for M4 |

## Operational plane

- **Liveness (IMPLEMENTED).** `/health/live` returns `200` whenever the process
  runs, including during a database outage.
- **Readiness (IMPLEMENTED).** `/health/ready` runs `SELECT 1` with a 2 s
  timeout. It returns `200` with `checks.database = "ready"`, or `503` with
  `"unavailable"`. The pool validates connections before use, so readiness
  recovers after a database restart without restarting the API.
- **Lifecycle logs (IMPLEMENTED).** The logged events are: event persisted,
  idempotency replay, idempotency conflict, event claimed, expired lease
  reclaimed, event processed, claim lost, and database unavailable. Fields
  include event ID, source, type, worker ID, and attempt. Payloads, raw
  idempotency keys, and connection strings are never logged.
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
6. **Failure visibility.** Lifecycle state is persisted and queryable. M3 adds
   explicit failed, retrying, and dead-letter states.
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
