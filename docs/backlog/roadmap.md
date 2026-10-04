# PulseStream Roadmap

This is a **plan**, not a record of completed work. Actual contributions,
reviews, and ownership are preserved in Git history and pull requests.

| Milestone | Scope | Owner | Reviewer | Status |
| --- | --- | --- | --- | --- |
| **M0** | Foundation / architecture | Ngetich-86 | LMichy1 | Merged ([#1](https://github.com/Ngetich-86/PulseStream/pull/1)) |
| **M1** | Event ingestion + bounded concurrency | Ngetich-86 | LMichy1 | Merged ([#2](https://github.com/Ngetich-86/PulseStream/pull/2)) with green CI but 0 GitHub-recorded reviews |
| **M2** | Persistence + idempotency + recovery | LMichy1 | MurayaSoftTouch (primary), Ngetich-86 | In progress |
| **M3** | Retry / dead-letter / failure handling | MurayaSoftTouch | Ngetich-86 | Planned |
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
- **M3.** Retry policy with backoff, dead-letter state, and failure visibility.
- **M4.** Benchmarks and load tests. Evidence-driven tuning, and the broker
  question ([ADR-003](../adr/ADR-003-postgresql-initial-durable-store.md)).
- **M5.** Security review, dependency policy, metrics, and operational runbooks.
- **M6.** Small operations frontend and end-to-end tests.
- **M7.** Release checklist, documentation pass, versioning.
