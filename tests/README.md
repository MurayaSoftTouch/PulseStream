# Tests

Tests live beside the code they exercise: `#[cfg(test)]` modules, plus
`crates/pulsestream-store/tests/postgres.rs`.

There are two tiers:

- **Unit tests** need no database: `cargo test --workspace`.
- **Real-PostgreSQL tests** are marked `#[ignore = "requires PostgreSQL ..."]`.
  Run them with `cargo test --workspace -- --ignored` and
  `PULSESTREAM_TEST_DATABASE_URL` set. CI runs them in the
  `postgres-integration` job.

## Test database safety

- Tests read `PULSESTREAM_TEST_DATABASE_URL`, never `DATABASE_URL`.
- The database name must be a plain identifier ending in `_test`. Any other
  name makes the test panic before it does anything.
- Each test runs `CREATE DATABASE <name>_<random hex>`, applies the embedded
  migrations to that empty database, and drops it with `DROP DATABASE ... WITH
  (FORCE)` when the test ends, even if the test panics. Only databases the
  harness created are dropped, and nothing is truncated.
- Because every test owns its database, tests run in parallel and can assert
  exact row counts.

## Coverage

| Area | Where | What is proven |
| --- | --- | --- |
| Validation | core, api (unit) | Field limits, control characters, null payload, unknown fields, malformed JSON, 415, 413 with and without `Content-Length` |
| Idempotency keys | core, api (unit) | Required, 1–128 visible ASCII, non-ASCII header rejected, redacted in `Debug` |
| Fingerprint | core (unit) | Recursive key-order independence, array order matters, `1` ≠ `1.0`, source and type included, a value pinned against an independent Python computation |
| Migrations | store (PG) | Applies 0001 and 0002 to an empty database, and a re-run is a no-op. A populated M2 database (0001 only, rows in every M2 state) upgrades in place: rows, content, states, and attempts preserved, `available_at` backfilled from `accepted_at`, and claims behave as before |
| Admission | store and api (PG) | New key returns 202. Exact replay (reordered keys) returns the same ID and `Idempotency-Replayed: true`. Conflict returns 409 and the row is unchanged. Same key under another source is independent |
| Races | store and api (PG) | 50 concurrent identical admissions give one row and one ID. 50 concurrent conflicting ones give one winner and 49 × 409. 30 concurrent HTTP requests return one ID |
| Durability | store and api (PG) | `202`, then the instance is destroyed, then a new pool still finds the event, then a worker processes it and the status becomes `processed` |
| Claiming | store (PG) | Bounded by the limit. 20 concurrent claimers over 60 events never claim one twice |
| Leases | store and worker (PG) | An unexpired lease is not stolen. An expired lease is reclaimed with `delivery_attempts` 2. A stale owner cannot complete a reclaimed event |
| Worker runtime | worker (PG) | Never more than `concurrency` claimed or processing (checked in the database). Shutdown stops claiming and finishes active work. Abandoned claims are recovered by another worker. Two workers share 40 events with no overlap |
| Schema | store (PG) | Content immutability trigger, lifecycle CHECKs, status set (`DEAD_LETTERED` requires its timestamp, a failure code, and at least one attempt), failure-code format, 1024-character message limit, all-or-nothing failure metadata |
| Retry policy | core (unit) | Backoff series (100, 200, 400, 400 ms with a 400 ms cap, and the defaults), overflow safety up to `u32::MAX` attempts, deterministic jitter in [80%, 100%] pinned against an independent Python computation, retry decision at the attempt limit, config ranges and max ≥ base |
| Failure codes | core (unit) | Stable upper-snake-case codes only, messages truncated to 1024 characters with control characters replaced |
| Retry scheduling | store and worker (PG) | A retryable failure gives `PENDING` with `available_at` in the future (database clock). Polls before then claim nothing. Claimed after, as attempt 2. A 60 s delay holds through about 20 worker polls |
| Eligibility | store (PG) | Of 40 pending events, only the 20 due ones are claimed. Future retries are not |
| Max attempts | worker (PG) | Max 3, always retryable: 3 calls, 2 retries, then `DEAD_LETTERED`, with no fourth claim |
| Permanent failure | worker (PG) | Dead-lettered on attempt 1 with budget left. No retry scheduled |
| Success after retry | worker (PG) | Fail, then succeed: `PROCESSED`, 2 attempts, last failure kept. A panic, then success: the same, with `PROCESSOR_PANICKED` |
| Dead-letter | store and api (PG) | Terminal, never claimed, and content and attempts preserved. No later complete, retry, or dead-letter applies. The status API shows `dead_lettered` without failure details. Readiness stays `200`. A replay is still idempotent and consumes no attempt |
| Stale failure | store and worker (PG) | A claims, its lease expires, B reclaims, then A's retry or dead-letter changes nothing and B stays owner |
| Concurrent retry claim | store and worker (PG) | 20 concurrent claimers (barrier-started) over 10 due retries claim each exactly once. Two workers retrying 30 events never process one at the same time |
| Final-attempt lease expiry | store and worker (PG) | Not reclaimed beyond the limit. Swept to `DEAD_LETTERED` (`LEASE_EXPIRED`). The sweep ignores live leases and non-final attempts. Ordinary crash recovery has no backoff |
| Outage | api (unit and PG) | Unreachable DB: live 200, ready 503, POST 503 `PERSISTENCE_UNAVAILABLE`. Through a toggleable TCP proxy: outage, then recovery **without restarting** the app |

Lease expiry is simulated by moving `lease_expires_at` into the past in the
test's own database, not by sleeping. Retry eligibility is controlled the same
way (`make_available`), except in one store test that deliberately waits a
real 400 ms delay on the database clock. Worker retry tests use 20–80 ms
backoff. Concurrency tests coordinate through
semaphores (`GatedProcessor`). The only sleeps are bounded polling for a
database state, plus one 200 ms window that gives an over-claiming worker a
chance to show itself.
