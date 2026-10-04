# ADR-007: PostgreSQL durable admission and lease-based processing

- Status: Accepted
- Date: 2026-10-04
- Milestone: M2
- Supersedes: [ADR-006](ADR-006-in-memory-bounded-admission.md) (in-memory admission)

## Context

In M1, `202 Accepted` meant an event had entered a bounded **in-memory** queue
inside the API process. A crash could lose accepted events, and a producer's
retry created a duplicate. ADR-003 chose PostgreSQL as the durable store, and
ADR-005 deferred the delivery semantics until the durable pipeline existed. M2
builds that pipeline.

## Decision

### PostgreSQL is the source of truth

The M1 in-memory queue is removed, not kept alongside the database. The API
writes events to the `events` table. The worker discovers work only by
claiming rows from that table. There is no second, competing queue.

### Durable acceptance

`POST /v1/events` returns `202` **only after the admission transaction has
committed**. A committed event survives API and worker restarts. If the
database is unavailable, the API returns `503 PERSISTENCE_UNAVAILABLE` and
never `202`.

A commit can succeed on the server while the connection fails before the
acknowledgement arrives. The client then sees an error even though the event
exists. Required idempotency keys make that safe: the client retries with the
same key and receives the original event ID.

### Scoped idempotency

- `Idempotency-Key` is required: 1–128 visible ASCII characters.
- Its scope is `(source, idempotency_key)`, enforced by `UNIQUE (source,
  idempotency_key)` in PostgreSQL, not by application checks.
- A request fingerprint is computed as SHA-256 over a versioned,
  length-prefixed encoding of `source`, `event_type`, and the canonical
  payload. In the canonical payload, object keys are sorted recursively and
  array order is kept. The exact equality rules are documented in
  `pulsestream_core::idempotency`.
- Admission is `INSERT ... ON CONFLICT DO NOTHING`, followed by a lookup of the
  existing row, in one transaction:
  - New key: the row is created, and the response is `202` with
    `Idempotency-Replayed: false`.
  - Same key and same fingerprint: `202` with the **original** event ID and
    `Idempotency-Replayed: true`. No row is written.
  - Same key and a different fingerprint: `409 IDEMPOTENCY_CONFLICT`. The
    stored row is never modified.
- Concurrent requests with the same key serialize on the unique index. The
  conflicting insert waits for the winner's commit, and the lookup then sees
  the winner, so exactly one row and one event ID result.

### Lifecycle

States are `PENDING`, `PROCESSING`, and `PROCESSED`. CHECK constraints tie the
lifecycle columns to the status: owner and lease only while `PROCESSING`, and
`processed_at` only when `PROCESSED`. A trigger rejects any change to the event
content (`event_id`, `source`, `event_type`, `payload`, `idempotency_key`,
`request_fingerprint`, `accepted_at`). All lifecycle timestamps use the
database clock.

### Claiming

A worker claims at most its **free capacity** (`concurrency - active`) in one
atomic statement. The statement selects claimable rows with
`FOR UPDATE SKIP LOCKED`, oldest first. Claimable means `PENDING`, or
`PROCESSING` with an expired lease. Each selected row becomes `PROCESSING`
with `processing_owner` set to the worker's ID, a fresh lease, and
`delivery_attempts + 1`. Concurrent claimers skip each other's locked rows,
so no row is claimed twice. Nothing is prefetched, which keeps ADR-004's
memory bound: at most `concurrency` events per worker are held in memory.

With free capacity but no work, the worker sleeps for
`PULSESTREAM_POLL_INTERVAL_MS` (default 250). With no free capacity, it waits
for a task to finish. M2 does not use `LISTEN/NOTIFY`: polling is simpler, and
notifications would only reduce latency, not change correctness.

### Leases and ownership

- Each worker process has a random UUID, its `WorkerId`, stored as
  `processing_owner`.
- A claim's lease lasts `PULSESTREAM_PROCESSING_LEASE_MS` (default 30000).
- Completion is conditional: `UPDATE ... WHERE status = 'PROCESSING' AND
  processing_owner = <me>`. If the lease expired and another worker reclaimed
  the event, the stale worker's completion affects no rows. It cannot overwrite
  the new owner's state.
- If a worker dies, its claims stay `PROCESSING` until their leases expire.
  Then any worker reclaims them and `delivery_attempts` increments. This is
  **crash recovery**, not a retry policy.
- On graceful shutdown, a worker stops claiming and lets active events finish
  within `PULSESTREAM_SHUTDOWN_TIMEOUT_MS`. Anything still running is
  abandoned and left `PROCESSING` for lease recovery. Rows are not reset to
  `PENDING`, because the worker cannot prove the work did not happen.
- M2 does not extend leases with heartbeats. Processing that outlasts the lease
  can be reclaimed and run concurrently elsewhere. The ownership check keeps
  the stored state correct, and the processor's idempotency must cover the
  effects.

### Delivery semantics

**PulseStream M2 provides durable admission with at-least-once processing.**
It does not provide exactly-once processing. Example:

```text
worker performs an external effect
  -> process crashes
  -> the PROCESSED update never commits
  -> lease expires
  -> another worker reclaims the event and performs the effect again
```

Processors with external side effects must be idempotent. For example, they
can key effects on `event_id`.

### Failures in M2

If a processor returns an error or panics, the event is logged and its claim
is left to expire. It is then reclaimed after the lease. There is no retry
limit, backoff, or dead-letter state yet; M3 owns that policy. The default M2
processor never fails.

## Consequences

- A fourth crate, `pulsestream-store`, holds all SQL and the embedded
  migrations. It is shared by the API and the worker and keeps
  `pulsestream-core` free of database code. This is the justified-boundary
  rule from ADR-002.
- Restarting the API no longer requires a drain to preserve accepted events.
  API shutdown only stops HTTP.
- `429 QUEUE_FULL` from M1 no longer exists. Memory stays bounded by the
  connection pool (`PULSESTREAM_DB_MAX_CONNECTIONS`, default 10) and the
  worker's concurrency. The durable backlog in PostgreSQL is bounded only by
  storage. Admission-side backlog limits and load testing are M4 work.
- An event that always fails is reclaimed after every lease expiry, with no
  end. M3 must add retry limits and a dead-letter state.
- PostgreSQL availability now gates admission and readiness. `/health/ready`
  checks the database.
- TLS to PostgreSQL is supported (rustls) through `sslmode` in `DATABASE_URL`.
  Production hardening of that configuration belongs to M5.
