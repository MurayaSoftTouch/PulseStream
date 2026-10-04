# PostgreSQL (local development)

PostgreSQL is PulseStream's durable event store and its source of truth
([ADR-003](../../docs/adr/ADR-003-postgresql-initial-durable-store.md),
[ADR-007](../../docs/adr/ADR-007-postgresql-durable-admission-and-leases.md)).
Locally it runs through Docker Compose. Do not install PostgreSQL directly in WSL.

| Setting | Value |
| --- | --- |
| Compose service | `postgres` |
| Image | `postgres:18.6-alpine` (CI uses the same image) |
| Host port | `127.0.0.1:55432` (override with `PULSESTREAM_POSTGRES_PORT`) |
| Named volume | `pulsestream_postgres-data` |
| Databases | `pulsestream` (development), `pulsestream_test` (base for tests) |

```bash
docker compose up -d postgres
docker compose ps          # wait for "(healthy)"
docker compose down        # keeps the volume
```

`docker compose down -v` **deletes all local PulseStream data**. Only use it
deliberately.

## Schema and migrations

Migrations live in [`/migrations`](../../migrations). They are embedded in the
binaries by `sqlx::migrate!` and applied at startup
(`PULSESTREAM_MIGRATE_ON_START=true`) and by every database test. `sqlx-cli`
is optional.

Published migrations are immutable. Changes go in a new, higher-numbered file.

## Test database

[`init/01-create-test-database.sql`](init/01-create-test-database.sql) creates
`pulsestream_test`, but only when the volume is first initialized. On an older
volume, create it once:

```bash
docker compose exec postgres createdb -U pulsestream pulsestream_test
```
