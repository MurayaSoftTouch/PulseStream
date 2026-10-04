# PulseStream Roadmap

This is a **plan**, not a record of completed work. Actual contributions,
reviews, and ownership are preserved in Git history and pull requests.

Development through M2 originated in
[Ngetich-86/PulseStream](https://github.com/Ngetich-86/PulseStream), where pull
requests #1–#3 and their reviews remain.
[MurayaSoftTouch/PulseStream](https://github.com/MurayaSoftTouch/PulseStream) is
the canonical repository beginning with M3.

| Milestone | Scope | Owner | Reviewer | Status |
| --- | --- | --- | --- | --- |
| **M0** | Foundation / architecture | Ngetich-86 | LMichy1 | Merged ([#1](https://github.com/Ngetich-86/PulseStream/pull/1)) |
| **M1** | Event ingestion + bounded concurrency | Ngetich-86 | LMichy1 | Merged ([#2](https://github.com/Ngetich-86/PulseStream/pull/2)) with green CI but 0 GitHub-recorded reviews |
| **M2** | Persistence + idempotency + recovery | LMichy1 | MurayaSoftTouch (primary), Ngetich-86 | Merged ([Ngetich-86/PulseStream#3](https://github.com/Ngetich-86/PulseStream/pull/3)) |
| **M3** | Retry / dead-letter / failure handling | MurayaSoftTouch | Ngetich-86 (primary), LMichy1 | Merged ([#1](https://github.com/MurayaSoftTouch/PulseStream/pull/1)) |
| **M4** | Performance + backpressure + benchmarks | Ngetich-86 | LMichy1 | Planned |
| **M5** | Operational / security / reliability hardening | MurayaSoftTouch | LMichy1 | Planned |
| **M6** | Operations dashboard + end-to-end integration | LMichy1 | Ngetich-86 | Planned |
| **M7** | Release readiness | Collaborative | Collaborative | Planned |

## Milestone notes

- **M0.** Rust workspace, API and worker skeletons, health endpoints, env
  config, structured logging, graceful shutdown, local PostgreSQL, CI, ADRs.
- **M1.** `POST /v1/events`, validation, a bounded in-memory queue, bounded
  processing concurrency, `429` on overload, and a bounded shutdown drain.
  Acceptance is **non-durable**
  ([ADR-006](../adr/ADR-006-in-memory-bounded-admission.md)).
- **M2.** Durable PostgreSQL admission (`202` after commit), required
  idempotency keys scoped by source, persisted lifecycle, `SKIP LOCKED`
  claiming with leases, crash recovery, and the delivery semantics: durable
  admission with at-least-once processing
  ([ADR-007](../adr/ADR-007-postgresql-durable-admission-and-leases.md)).
- **M3.** Retryable and permanent processor failures, persistent retry
  scheduling (`available_at`), capped exponential backoff with deterministic
  jitter, a maximum attempt count, and a durable `DEAD_LETTERED` state with
  safe failure metadata. Processing stays at-least-once. Authenticated
  dead-letter inspection and redrive are deferred
  ([ADR-008](../adr/ADR-008-retry-scheduling-and-dead-letter-policy.md)).
- **M4.** Benchmarks and load tests. Evidence-driven tuning, and the broker
  question ([ADR-003](../adr/ADR-003-postgresql-initial-durable-store.md)).
- **M5.** Security review, dependency policy, metrics, and operational runbooks.
- **M6.** Small operations frontend and end-to-end tests.
- **M7.** Release checklist, documentation pass, versioning.

## Implemented and planned

**Implemented (M0–M3):** durable PostgreSQL admission (`202` after commit),
source-scoped idempotency keys with request fingerprints, `SKIP LOCKED`
claiming with leases and crash recovery, bounded worker concurrency,
retryable and permanent failures, capped exponential backoff with
deterministic jitter, a durable `DEAD_LETTERED` state, structured logs, a
database readiness check, and graceful shutdown.

**Planned, not implemented:** benchmarks and load tests (M4), admission-side
backpressure (M4), metrics and Prometheus export (M5), authentication and
authorization (M5), authenticated dead-letter inspection and redrive (M5/M6),
lease extension, an operations dashboard (M6), and release versioning (M7).

## Benchmark methodology (M4)

M4 results will only be published together with the conditions that produced
them. Each benchmark report should record:

- **Runtime:** CPU, memory, OS, Rust toolchain, build profile.
- **Database:** PostgreSQL version, configuration changes from defaults, and
  whether it runs locally, in a container, or over the network.
- **Workload:** event count, payload size, number of sources, and the share
  of duplicate (idempotent replay) submissions.
- **Workers:** process count, `PULSESTREAM_WORKER_CONCURRENCY`, lease
  duration, poll interval, and connection pool size.
- **Failure injection:** the share of retryable and permanent failures,
  worker kills, and database interruptions, if any.
- **Method:** warm-up, run length, repetitions, and how throughput and
  latency percentiles were measured.
- **Interpretation:** what the numbers do and do not show, including the
  bottleneck observed.
