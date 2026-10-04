# ADR-005: Delivery semantics deferred until durable pipeline design

- Status: Superseded by [ADR-007](ADR-007-postgresql-durable-admission-and-leases.md) (M2), which defines durable admission with at-least-once processing
- Date: 2026-09-25
- Milestone: M0

## Context

Event systems are often described as "exactly-once". In practice, end-to-end
guarantees depend on where acceptance is acknowledged, how work is claimed and
leased, what happens when a worker crashes mid-processing, and whether side
effects are idempotent. None of that exists yet in PulseStream.

## Decision

1. PulseStream makes **no delivery-guarantee claim** until the durable pipeline
   is designed and implemented, which is planned for M2.
2. The README, docs, and PR descriptions must not describe PulseStream as
   "exactly-once". Until the semantics are defined, the approved wording is:
   *"delivery and idempotency semantics are defined in later milestones."*
3. When semantics are defined, they are documented precisely. The document
   covers the acceptance point, the delivery guarantee (for example
   at-least-once processing with idempotent acceptance), and the specific
   failure scenarios under which duplicates or delays can occur. Tests must
   back the documented behavior.

## Consequences

- Documentation stays honest while the implementation catches up.
- The M2 design must include a semantics document and failure-mode tests before
  any guarantee is advertised.
