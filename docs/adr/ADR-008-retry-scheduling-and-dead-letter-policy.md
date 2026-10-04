# ADR-008: Retry scheduling and dead-letter policy

- Status: Accepted
- Date: 2026-10-04
- Milestone: M3
- Builds on: [ADR-007](ADR-007-postgresql-durable-admission-and-leases.md)
  (durable admission and leases). ADR-007 is unchanged.

## Context

In M2, a processor error was only logged. The claim stayed `PROCESSING` until
its lease expired and the event was then reclaimed. Nothing limited the
attempts, nothing distinguished a timeout from a malformed event, and an event
that always failed (a poison event) was reclaimed every lease period without
end. ADR-007 assigned the retry policy to M3.

## Decision

### Smallest state model

The states are `PENDING`, `PROCESSING`, `PROCESSED`, and the new terminal
`DEAD_LETTERED`. A scheduled retry is **not** a separate state: it is a
`PENDING` event whose new `available_at` column lies in the future. There is
no `RETRYING`, `FAILED_TEMPORARILY`, or `FAILED_PERMANENTLY` status.

```text
PENDING (available_at <= now) --claim--> PROCESSING
   ^                                        |
   |                                        +-- success ---------------------> PROCESSED
   +-- retryable failure, attempts left ----+
       (available_at = now + backoff)       +-- permanent failure -----------> DEAD_LETTERED
                                            +-- retryable, budget spent -----> DEAD_LETTERED
                                            +-- lease expiry ----------------> reclaimable
                                                (on the final attempt -------> DEAD_LETTERED)
```

### Explicit failure classification

A processor returns `ProcessError::Retryable(detail)` or
`ProcessError::Permanent(detail)`. The variant decides what happens next.
Messages are never parsed to infer retryability. A processor panic is treated
as a retryable `PROCESSOR_PANICKED` failure, so it is bounded by the attempt
limit instead of orphaning the claim.

Each failure carries a stable, machine-readable code (`^[A-Z][A-Z0-9_]{0,63}$`,
for example `UPSTREAM_TIMEOUT`) and a message. Codes are part of the
operational contract, so Rust type names are not used as codes.
`LEASE_EXPIRED` and `PROCESSOR_PANICKED` are reserved for the runtime.

### Attempt accounting

`delivery_attempts` (from M2) is the number of times the event has been
claimed into `PROCESSING`. The first claim is attempt 1. A failed claim
transaction does not commit, so it consumes nothing. An idempotent HTTP replay
writes no row, so it consumes nothing either.

With `PULSESTREAM_MAX_DELIVERY_ATTEMPTS = N` (default 5), **an event is
processed at most N times**. After a retryable failure on attempt `n`:

- if `n < N`, the event is scheduled again;
- if `n >= N`, it is dead-lettered.

### Persistent scheduling with `available_at`

`available_at TIMESTAMPTZ NOT NULL` is set to `accepted_at` at admission (both
default to the same transaction's `now()`). A retry sets it to
`now() + delay`. The claim selects only

```sql
(status = 'PENDING' AND available_at <= now())
OR (status = 'PROCESSING' AND lease_expires_at <= now() AND delivery_attempts < max)
```

with `FOR UPDATE SKIP LOCKED` and the existing claim limit. Eligibility uses
the **database clock**, never a worker's wall clock. The worker only computes
the delay. A future retry cannot be claimed early, and because the schedule is
a column, a scheduled retry survives worker restarts.

### Bounded exponential backoff with deterministic jitter

```text
capped = min(max_delay, base_delay * 2^(n - 1))     -- checked arithmetic; overflow -> max_delay
delay  = capped * f,   f in [0.80, 1.00]
```

`f` comes from an FNV-1a hash of `(event_id, n)`. It is deterministic, so tests
are reproducible and the value is pinned in a unit test. Different events
still spread out, which reduces synchronized retry storms.

The suggested range was roughly 80–120%. We use **80–100%** so that `max_delay`
stays a hard ceiling and retries keep spreading out once the exponential
reaches the cap. With ±20% and a cap, half of all capped retries would land
exactly on the cap.

| Variable | Default | Range |
| --- | --- | --- |
| `PULSESTREAM_MAX_DELIVERY_ATTEMPTS` | `5` | `1`–`100` |
| `PULSESTREAM_RETRY_BASE_DELAY_MS` | `1000` | `1`–`600000` |
| `PULSESTREAM_RETRY_MAX_DELAY_MS` | `60000` | `1`–`86400000`, and at least the base |

Invalid values, including a maximum below the base, stop the worker at
startup. They are never normalized. With the defaults, the unjittered delays
after attempts 1–4 are 1 s, 2 s, 4 s, and 8 s, and attempt 5 dead-letters.

### Permanent failure vs. retry exhaustion

- **Permanent failure.** The event moves `PROCESSING -> DEAD_LETTERED`
  immediately, whatever budget remains. Example: an invalid destination.
- **Retry exhaustion.** A retryable failure on the final permitted attempt
  moves the event `PROCESSING -> DEAD_LETTERED`. No further retry is
  scheduled.

Both are logged as `event dead-lettered`. The `reason` field is
`permanent_failure` or `retry_exhausted`.

### Crash recovery is not a processing retry

| | Lease expiry (crash recovery, ADR-007) | Retryable failure (this ADR) |
| --- | --- | --- |
| Cause | The worker never recorded an outcome | The processor returned `Retryable` |
| Failure recorded | No | Yes (`last_failure_*`) |
| Next attempt | Immediately after lease expiry | After backoff (`available_at`) |
| Attempt counted | Yes (each claim) | Yes (each claim) |

Backoff is **not** applied to lease expiry: no failure was observed, and the
lease duration already delays the reclaim.

One gap needed closing. If an event's **final** permitted attempt loses its
lease (for example, the event crashes the worker process every time), the
claim condition `delivery_attempts < max` stops it from being reclaimed. On
every poll, each worker runs a bounded sweep that moves such events to
`DEAD_LETTERED` with code `LEASE_EXPIRED`. That sweep is
`Store::dead_letter_expired_final_attempts`, at most 100 rows, using
`SKIP LOCKED`. As a result, the "at most N processing attempts" rule holds even
for poison events that crash the worker.

### Durable, database-backed dead-letter state

A `DEAD_LETTERED` row stays in the `events` table. It is not moved to another
table and not deleted. It keeps its original identity and content, its
`delivery_attempts`, and its final failure metadata. It is in neither claim
predicate, so the standard claim path never selects it, and no transition out
of it exists in M3.

### Safe failure metadata

| Column | Notes |
| --- | --- |
| `last_failure_code` | Stable code, checked by the schema |
| `last_failure_message` | At most **1024 characters**, enforced by a CHECK. Truncated in Rust first, with control characters replaced by spaces |
| `last_failed_at` | Database clock |
| `dead_lettered_at` | Set only for `DEAD_LETTERED` |

A schema constraint keeps code, message, and time all-or-nothing. Stack
traces, payload copies, and panic payloads are never stored. Logs include
the code, never the message.

When an event finally succeeds after a retry, `last_failure_*` is **kept**
for diagnostics. `PROCESSED` with a `last_failure_code` means the event
succeeded after at least one recorded failure.

### Ownership-checked transitions

Retry scheduling and dead-lettering use the same guard as M2's completion:

```sql
WHERE event_id = $1 AND status = 'PROCESSING' AND processing_owner = $2
```

If a worker's lease expired and another worker reclaimed the event, the stale
worker's failure report affects no rows. It cannot reschedule or dead-letter
an event it no longer owns. That case is counted as a lost claim.

### Event content stays immutable

The M2 trigger still rejects any change to `event_id`, `source`, `event_type`,
`payload`, `idempotency_key`, `request_fingerprint`, and `accepted_at`. Retries
change lifecycle columns only.

### Migration

`0002_add_retry_and_dead_letter.sql` is a new, immutable migration.
`0001_create_events.sql` is unchanged. It upgrades a populated M2 database in
place:

- `available_at` is backfilled from `accepted_at`, so no M2 row becomes
  unclaimable or delayed;
- every M2 `PENDING`, `PROCESSING`, and `PROCESSED` row satisfies the new
  constraints;
- a test applies 0001, writes rows in every M2 state, and upgrades.

### API and operations surface

- `GET /v1/events/{id}` can now report `dead_lettered`, along with
  `dead_lettered_at` and `delivery_attempts`. A scheduled retry reports
  `pending`. The failure code and message are not exposed: there is no
  authentication yet.
- `/health/ready` is unchanged. A dead-lettered event is a business outcome,
  not an infrastructure outage.
- **Deferred:** a dead-letter listing endpoint and a **redrive** operation.
  An unauthenticated endpoint that can list processor failure details or
  re-run arbitrary events is unsafe. Both belong behind the authenticated
  operations surface in M5 and M6. Until then, inspect dead-lettered events
  with SQL.

## Consequences

- Poison events terminate: at most `N` attempts, then `DEAD_LETTERED`.
- Processing is still **at-least-once**. Retries do not make it exactly-once:
  a handler can run again after a crash, after lease expiry, and after every
  retryable failure. Consumers with external side effects must be idempotent.
- Each poll now issues two statements (the final-attempt sweep, then the
  claim). Throughput has not been measured (M4).
- Lowering `PULSESTREAM_MAX_DELIVERY_ATTEMPTS` does not retroactively
  dead-letter events that are already `PENDING`. Each gets one more attempt
  and is then dead-lettered if it fails. An expired final-attempt lease is
  judged against the current limit.
- No lease extension yet (ADR-007). A slow attempt that outlives its lease on
  the final attempt is dead-lettered by the sweep. Its late outcome is then
  rejected as a lost claim.
