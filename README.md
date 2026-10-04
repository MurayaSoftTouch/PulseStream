# PulseStream

[![CI](https://github.com/MurayaSoftTouch/PulseStream/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/MurayaSoftTouch/PulseStream/actions/workflows/ci.yml)

A Rust event-processing service with durable PostgreSQL admission, idempotent
ingestion, lease-based concurrent workers, retries with backoff, and
dead-lettering. Metrics and broader observability are planned, not built.

> **Status: in active development.** Milestones 0–3 are merged; M3 (retry,
> dead-letter, and failure handling) is the latest. Next is M4: benchmarks,
> load tests, and admission backpressure. See the [roadmap](docs/backlog/roadmap.md).
>
> **PulseStream provides durable admission with at-least-once processing.**
> `202 Accepted` means the event has been committed to PostgreSQL, so it
> survives API and worker restarts. Processing is at-least-once, **not**
> exactly-once. A handler may execute more than once: after a crash, after a
> lease expiry, and after every retryable failure. Consumers performing
> external side effects should be idempotent. Retries and the dead-letter
> state bound how often an event is attempted. They do not make processing
> exactly-once.

## Current vs. planned

| Area | Current (M3) | Planned |
| --- | --- | --- |
| Admission | `POST /v1/events` commits to PostgreSQL before returning `202`. A database outage returns `503` and never `202` | Backlog limits (M4) |
| Idempotency | Required `Idempotency-Key`, scoped by `source` and enforced by a unique constraint. An exact replay returns the original ID; reuse with a different request returns `409` | |
| Lifecycle | Persisted `pending` → `processing` → `processed` or `dead_lettered`. A scheduled retry is `pending` with a future `available_at`. `GET /v1/events/{id}` reports status and `delivery_attempts` | |
| Processing | Workers claim atomically (`FOR UPDATE SKIP LOCKED`) only events that are due, at most their free capacity, with owner IDs and leases | |
| Failures | Processors return **retryable** or **permanent** failures with a stable code. Retryable failures back off exponentially (deterministic jitter, capped). After the maximum attempts, or on a permanent failure, the event is **dead-lettered** in PostgreSQL with safe failure metadata | Authenticated dead-letter inspection and redrive (M5/M6) |
| Recovery | An expired lease is reclaimed by any worker, without backoff. An expired lease on the final permitted attempt is dead-lettered (`LEASE_EXPIRED`). Stale owners cannot complete, retry, or dead-letter reclaimed events | |
| Delivery semantics | **Durable admission, at-least-once processing.** No exactly-once claim | |
| Observability | Structured lifecycle logs. Readiness checks the database | Metrics (M5), dashboard (M6) |
| Security | No authentication or authorization | M5 |
| Performance | **Not benchmarked. No performance claims** | Benchmarks and load tests (M4) |

## Architecture

```text
Client ─▶ API ─(one transaction, 202 after COMMIT)─▶ PostgreSQL events ─(SKIP LOCKED claim of due events)─▶ bounded workers
```

Event lifecycle, as enforced by the `events` table's status and lifecycle
CHECK constraints and the owner-checked store operations:

```mermaid
stateDiagram-v2
    [*] --> PENDING: POST /v1/events (202 after COMMIT)
    PENDING --> PROCESSING: claim when due (SKIP LOCKED, lease, attempt +1)
    PROCESSING --> PROCESSED: processor succeeds
    PROCESSING --> PENDING: retryable failure, attempts left (available_at = now + backoff)
    PROCESSING --> DEAD_LETTERED: permanent failure, or retries exhausted
    PROCESSING --> PROCESSING: lease expired, reclaimed by another worker (no backoff)
    PROCESSING --> DEAD_LETTERED: lease expired on the final attempt (LEASE_EXPIRED)
    PROCESSED --> [*]
    DEAD_LETTERED --> [*]
```

| Crate | Kind | Responsibility |
| --- | --- | --- |
| [`pulsestream-core`](crates/pulsestream-core) | library | Event model, idempotency keys, request fingerprints, failure codes, retry policy, env config. No HTTP, DB, or runtime deps |
| [`pulsestream-store`](crates/pulsestream-store) | library | All SQL: admission, status, claim, complete, retry, dead-letter. Embeds [`migrations/`](migrations) |
| [`pulsestream-api`](crates/pulsestream-api) | binary | HTTP admission, status lookup, health |
| [`pulsestream-worker`](crates/pulsestream-worker) | library + binary | Bounded claim/process runtime with leases and the retry policy |

Read more in the [architecture overview](docs/architecture/overview.md), the
[ADRs](docs/adr/) (especially
[ADR-007](docs/adr/ADR-007-postgresql-durable-admission-and-leases.md) and
[ADR-008](docs/adr/ADR-008-retry-scheduling-and-dead-letter-policy.md)), and the
[roadmap](docs/backlog/roadmap.md).

## Requirements

- Rust **1.98.0**, pinned in [`rust-toolchain.toml`](rust-toolchain.toml)
  (rustup installs it automatically, with `rustfmt` and `clippy`). Install
  Rust through [rustup](https://rustup.rs), not a distro package.
  If your `stable` toolchain is already 1.98.0 and disk is tight, you can use
  it in this checkout without downloading a second copy:
  `rustup override set stable`. CI always uses the exact pin.
- A C toolchain (`build-essential` on Ubuntu). rustls' `ring` compiles C code.
- Docker with Compose v2, for PostgreSQL. `sqlx-cli` is **not** required:
  migrations are embedded and applied by the binaries and the tests.

## Local setup

```bash
git clone git@github.com:MurayaSoftTouch/PulseStream.git
cd PulseStream
cp .env.example .env              # local-only placeholder values; .env is git-ignored
docker compose up -d postgres     # wait for (healthy): docker compose ps
cargo build --workspace
set -a; source .env; set +a       # the binaries read the process environment
```

A fresh data volume creates `pulsestream_test` automatically. On a volume
created before M2, run this once:
`docker compose exec postgres createdb -U pulsestream pulsestream_test`.

### Configuration

| Variable | Used by | Default | Notes |
| --- | --- | --- | --- |
| `DATABASE_URL` | api, worker | **required** | `postgres://` or `postgresql://`. Supports `sslmode`. Never logged |
| `PULSESTREAM_DB_MAX_CONNECTIONS` | api, worker | `10` | Pool size per process, `1`–`50` |
| `PULSESTREAM_DB_ACQUIRE_TIMEOUT_MS` | api, worker | `3000` | Maximum wait for a connection, including connecting, `100`–`60000` |
| `PULSESTREAM_MIGRATE_ON_START` | api, worker | `true` | Apply embedded migrations at startup |
| `PULSESTREAM_API_BIND` | api | `127.0.0.1:8088` | Socket address |
| `PULSESTREAM_WORKER_CONCURRENCY` | worker | `4` | Maximum events claimed and processed at once, `1`–`64` |
| `PULSESTREAM_POLL_INTERVAL_MS` | worker | `250` | Sleep when no work is available, `10`–`60000` |
| `PULSESTREAM_PROCESSING_LEASE_MS` | worker | `30000` | Claim lease before another worker may reclaim, `1000`–`3600000` |
| `PULSESTREAM_SHUTDOWN_TIMEOUT_MS` | worker | `10000` | Wait for active events at shutdown, `1`–`300000` |
| `PULSESTREAM_MAX_DELIVERY_ATTEMPTS` | worker | `5` | Maximum times an event is processed (claimed) before it is dead-lettered, `1`–`100` |
| `PULSESTREAM_RETRY_BASE_DELAY_MS` | worker | `1000` | Backoff after attempt 1, `1`–`600000` |
| `PULSESTREAM_RETRY_MAX_DELAY_MS` | worker | `60000` | Backoff cap, `1`–`86400000`. Must not be below the base delay |
| `RUST_LOG` | api, worker | `info` | `EnvFilter` syntax. An invalid filter logs a warning and uses `info` |

Invalid values stop startup with a clear error. They are never silently
replaced by defaults. Every statement also runs with a server-side
`statement_timeout` of 10 s. M1's `PULSESTREAM_QUEUE_CAPACITY` no longer
exists.

### Run

```bash
cargo run -p pulsestream-api      # terminal 1
cargo run -p pulsestream-worker   # terminal 2 (run several to share the work)
```

### Event API

```bash
curl -si http://127.0.0.1:8088/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-12345-created' \
  -d '{"source":"orders-api","event_type":"order.created","payload":{"order_id":"12345"}}'
# HTTP/1.1 202 Accepted
# idempotency-replayed: false
# {"event_id":"<uuid>","status":"accepted"}

curl -s http://127.0.0.1:8088/v1/events/<uuid>
# {"event_id":"<uuid>","source":"orders-api","event_type":"order.created",
#  "status":"processed","accepted_at":"2026-...Z","processed_at":"2026-...Z",
#  "dead_lettered_at":null,"delivery_attempts":1}
```

**Request rules.** `source` is 1–100 characters and `event_type` is 1–150
characters, with no control characters. `payload` is any non-null JSON value.
No other fields are allowed, so clients cannot choose the event ID. The body
is limited to 64 KiB. `Idempotency-Key` is 1–128 visible ASCII characters with
no spaces, and is scoped by `source`.

**Replay and conflict rules.** Two requests with the same `source` and key are
an exact replay if they have the same `event_type` and an equal payload. Object
key order is ignored, array order matters, and `1` differs from `1.0`. An exact
replay returns the original `event_id` with `idempotency-replayed: true`. A
different request with the same `source` and key returns `409`. Every `202`
carries `idempotency-replayed`, set to `true` or `false`.

| Status | `code` | When |
| --- | --- | --- |
| `202` | (none) | Committed (or exact replay of a committed event) |
| `400` | `IDEMPOTENCY_KEY_REQUIRED` / `IDEMPOTENCY_KEY_INVALID` | Missing or malformed key |
| `400` | `INVALID_EVENT` | Malformed JSON, wrong shape, or a failed validation rule |
| `409` | `IDEMPOTENCY_CONFLICT` | Key already used by this source for a different event |
| `413` | `PAYLOAD_TOO_LARGE` | Body over 64 KiB |
| `415` | `UNSUPPORTED_MEDIA_TYPE` | Not `application/json` |
| `503` | `PERSISTENCE_UNAVAILABLE` | Database unreachable. Sent with `Retry-After: 1`. Retry with the same key |
| `500` | `INTERNAL_ERROR` | Unexpected server error. Details are logged, not returned |

`GET /v1/events/{event_id}` returns metadata, status (`pending`,
`processing`, `processed`, or `dead_lettered`), `delivery_attempts`, and
`dead_lettered_at`. A scheduled retry reports `pending`. It deliberately omits
the **payload** (payloads may hold sensitive business data), the failure code
and message, and internal fields such as the key, owner, lease, and
`available_at`. Unknown IDs return `404 EVENT_NOT_FOUND`, and malformed IDs return
`400 INVALID_EVENT_ID`.

### Failure handling (M3)

A processor returns `Ok`, `ProcessError::Retryable`, or
`ProcessError::Permanent`. A failure carries a stable code such as
`UPSTREAM_TIMEOUT` and a short message. Retryability comes from the variant,
never from parsing the message. A panic counts as a retryable
`PROCESSOR_PANICKED` failure.

| Outcome on delivery attempt `n` | Result |
| --- | --- |
| Success | `PROCESSED` |
| Retryable, `n < max` | `PENDING`, eligible again at `available_at = now + delay` (database clock) |
| Retryable, `n = max` | `DEAD_LETTERED` (`retry_exhausted`) |
| Permanent | `DEAD_LETTERED` immediately, whatever budget remains |
| Lease expired (crash), `n < max` | Reclaimed by any worker, without backoff |
| Lease expired on the final attempt | `DEAD_LETTERED` with `LEASE_EXPIRED` |

- **Attempts.** `delivery_attempts` counts claims into `processing`. With
  the default of 5, an event is processed **at most five times**. HTTP
  replays and failed claim transactions do not consume attempts.
- **Backoff.** `min(max_delay, base_delay × 2^(n−1))`, scaled by a
  deterministic jitter factor between 80% and 100%. The factor is derived
  from the event ID and attempt, so it never exceeds the cap. With the
  defaults: about 1 s, 2 s, 4 s, then 8 s.
- **Dead letters** stay in the `events` table with their original content,
  attempt count, `dead_lettered_at`, and `last_failure_code`,
  `last_failure_message` (at most 1024 characters), and `last_failed_at`.
  They are never claimed again. After an eventual success, `last_failure_*`
  is kept for diagnostics.
- **Inspecting dead letters** uses SQL for now, for example:
  `SELECT event_id, source, event_type, delivery_attempts, last_failure_code,
  dead_lettered_at FROM events WHERE status = 'DEAD_LETTERED';`
- **No redrive yet.** There is no API to list or re-run dead-lettered events,
  because the API has no authentication (M5).

See [ADR-008](docs/adr/ADR-008-retry-scheduling-and-dead-letter-policy.md).

### Health

- `GET /health/live` returns `200` while the process runs, even during a
  database outage.
- `GET /health/ready` returns `200` with `{"checks":{"database":"ready"}}` when
  PostgreSQL answers a lightweight query within 2 s. Otherwise it returns `503`
  with `"unavailable"`. It recovers on its own when the database returns.
  Processing failures and dead-lettered events never make the service
  unready.

### Shutdown

- **API:** stops accepting HTTP and finishes in-flight requests. Committed
  events are already durable, so there is no queue to drain.
- **Worker:** stops claiming and lets active events finish within
  `PULSESTREAM_SHUTDOWN_TIMEOUT_MS`. Anything unfinished stays `processing`
  and is reclaimed after its lease expires. On timeout the process exits with
  code 1.

## Tests

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace                                   # unit tests; no database needed

export PULSESTREAM_TEST_DATABASE_URL=postgres://pulsestream:pulsestream_local_dev_only@127.0.0.1:55432/pulsestream_test
cargo test --workspace -- --ignored                      # real-PostgreSQL tests
```

The real-PostgreSQL tests are marked `#[ignore]` so `cargo test` works without
a database. The test database name **must end in `_test`**; anything else is
refused. Each test creates and drops its own disposable database, so tests are
isolated and never touch `DATABASE_URL`. See [tests/README.md](tests/README.md).

### Manual retry and dead-letter smoke

[`crates/pulsestream-worker/examples/scripted_worker.rs`](crates/pulsestream-worker/examples/scripted_worker.rs)
runs the real worker runtime with a processor
that follows `PULSESTREAM_SMOKE_SCRIPT` (`succeed`, `panic`,
`retryable:<CODE>`, `permanent:<CODE>`; the last step repeats):

```bash
PULSESTREAM_SMOKE_SCRIPT=retryable:UPSTREAM_TIMEOUT,succeed \
  cargo run -p pulsestream-worker --features test-util --example scripted_worker
```

It is a development tool only, not part of the worker binary.

### Disk footprint

`[profile.dev]` and `[profile.test]` disable incremental compilation and keep
only line-table debug info, because local WSL disk is constrained. The release
profile uses Cargo's defaults.

## CI

[GitHub Actions](.github/workflows/ci.yml) runs on pull requests to and pushes
to `main`:
- `rust-quality`: rustfmt, plus clippy with `-D warnings`.
- `rust-test`: unit tests.
- `rust-build`.
- `postgres-integration`: a PostgreSQL 18 service container that runs the
  `--ignored` tests, each migrating a fresh database from empty.

All jobs use the pinned toolchain with `--locked`.

## Current limitations

- **Processing is at-least-once.** A handler may execute more than once
  across crashes, lease expiry, and retries. Processors with external side
  effects must be idempotent, for example by keying effects on `event_id`.
- **No exactly-once guarantee.** Retries and dead-lettering do not change
  this.
- **No authenticated redrive API** and **no dead-letter operations UI** yet.
  Dead-lettered events are inspected with SQL (M5/M6).
- No lease extension: processing that outlasts the lease may run concurrently
  on another worker. Every outcome write stays owner-checked.
- No admission-side backlog limit. The durable backlog is bounded by
  PostgreSQL storage.
- No authentication or authorization yet.
- PostgreSQL is the only durable work store. There is no external message
  broker (ADR-003).
- Not benchmarked or load-tested. No throughput or latency claims.

## Roadmap

See [docs/backlog/roadmap.md](docs/backlog/roadmap.md).

## Contributors

PulseStream is being developed collaboratively by **Ngetich-86**, **LMichy1**,
and **MurayaSoftTouch**. Milestone ownership and actual contributions are
preserved through Git history and pull requests.

### Repository history

Development through Milestone 2 originated in
[Ngetich-86/PulseStream](https://github.com/Ngetich-86/PulseStream), where the
M0–M2 pull requests and their reviews remain.
[MurayaSoftTouch/PulseStream](https://github.com/MurayaSoftTouch/PulseStream)
is the canonical repository beginning with Milestone 3. The Git history was
carried over unchanged.
