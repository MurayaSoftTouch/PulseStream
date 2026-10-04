//! PulseStream's durable event store on PostgreSQL.
//!
//! PostgreSQL is the source of truth (ADR-007):
//!
//! - [`Store::admit`] commits an event, or resolves an idempotent replay or
//!   conflict, in one transaction. A `202` may be returned only after it
//!   succeeds.
//! - [`Store::claim`] atomically moves up to `limit` claimable events (pending,
//!   or processing with an expired lease) to `PROCESSING` under one owner,
//!   using `FOR UPDATE SKIP LOCKED`, so concurrent workers never claim the same
//!   row.
//! - [`Store::complete`] marks an event `PROCESSED` only if the caller still
//!   owns its claim, so a stale worker cannot overwrite a reclaimed event.
//!
//! Processing is at-least-once: a worker that crashes after doing its work but
//! before `complete` commits leaves the event to be reclaimed after its lease
//! expires.

#[cfg(feature = "test-util")]
pub mod testing;

use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pulsestream_core::config::DatabaseConfig;
use pulsestream_core::event::{Event, EventId, EventSource, EventStatus, EventType, WorkerId};
use pulsestream_core::idempotency::{IdempotencyKey, RequestFingerprint};
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{Postgres, Transaction};
use thiserror::Error;
use uuid::Uuid;

/// Embedded migrations from the repository's `migrations/` directory.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Server-side limit on any single statement, so a stuck query cannot hold a
/// request or claim indefinitely.
pub const STATEMENT_TIMEOUT: &str = "10s";

#[derive(Debug, Error)]
pub enum StoreError {
    /// PostgreSQL cannot be reached or used right now: connection failure,
    /// pool timeout, or server shutdown. Callers must not report success.
    #[error("database unavailable: {0}")]
    Unavailable(#[source] sqlx::Error),

    /// Any other database error.
    #[error("database error: {0}")]
    Database(#[source] sqlx::Error),

    #[error("migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    /// A stored row violated an invariant the schema should guarantee.
    #[error("inconsistent stored data: {0}")]
    Inconsistent(&'static str),

    #[error("invalid database configuration")]
    InvalidConfig,
}

impl StoreError {
    pub fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }
}

impl From<sqlx::Error> for StoreError {
    fn from(err: sqlx::Error) -> Self {
        let unavailable = match &err {
            sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed => true,
            sqlx::Error::Database(db) => db.code().is_some_and(|code| {
                // 08xxx connection exception, 57P01-57P03 shutdown / cannot
                // connect now, 53300 too many connections.
                code.starts_with("08") || code.starts_with("57P") || code == "53300"
            }),
            _ => false,
        };
        if unavailable {
            Self::Unavailable(err)
        } else {
            Self::Database(err)
        }
    }
}

/// Result of a durable admission attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitOutcome {
    /// A new event row was committed.
    Created(EventId),
    /// The same `(source, key)` was already committed with an identical
    /// fingerprint. This is the original event; no row was written.
    Replayed(EventId),
    /// The same `(source, key)` was already committed with a different
    /// fingerprint. Nothing was written.
    Conflict,
}

/// Public, non-sensitive view of a stored event (no payload, key, owner, or
/// lease).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    pub event_id: EventId,
    pub source: String,
    pub event_type: String,
    pub status: EventStatus,
    /// RFC 3339 UTC with microseconds, for example `2026-09-25T18:08:00.845679Z`.
    pub accepted_at: String,
    pub processed_at: Option<String>,
}

/// An event claimed by a worker.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedEvent {
    pub event: Event,
    /// Delivery attempt number, starting at 1 and incremented on every claim.
    pub attempt: u32,
    /// True if this claim took over an expired lease from another claim.
    pub reclaimed: bool,
}

/// Shared handle to the PostgreSQL pool. Cheap to clone.
#[derive(Debug, Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    /// Creates a bounded, lazily connecting pool. No connection is attempted
    /// until first use, so a process can start (and report not-ready) while
    /// PostgreSQL is down.
    pub fn connect_lazy(
        config: &DatabaseConfig,
        application_name: &str,
    ) -> Result<Self, StoreError> {
        // The parse error could echo the URL, so it is deliberately discarded.
        let options = PgConnectOptions::from_str(config.url.expose())
            .map_err(|_| StoreError::InvalidConfig)?
            .application_name(application_name)
            .options([("statement_timeout", STATEMENT_TIMEOUT)]);
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(0)
            .acquire_timeout(config.acquire_timeout)
            // Validate pooled connections before use, so the pool recovers
            // after a database restart without restarting the process.
            .test_before_acquire(true)
            .connect_lazy_with(options);
        Ok(Self { pool })
    }

    /// Applies embedded migrations. Safe to run concurrently from several
    /// processes (the migrator holds an advisory lock), and a no-op when the
    /// schema is current.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// A lightweight round trip that proves the database can be used.
    pub async fn ping(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Closes the pool, waiting for checked-out connections to be returned.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Durably admits `event`, or resolves an idempotent replay or conflict.
    ///
    /// The insert and any conflict lookup run in one transaction. Uniqueness
    /// of `(source, idempotency_key)` is enforced by PostgreSQL:
    /// `INSERT ... ON CONFLICT DO NOTHING` waits for a concurrent insert of the
    /// same key to commit, and the lookup that follows (a new READ COMMITTED
    /// snapshot) then sees the winning row. Concurrent identical requests
    /// therefore converge on one row and one event ID.
    pub async fn admit(
        &self,
        event: &Event,
        key: &IdempotencyKey,
        fingerprint: &RequestFingerprint,
    ) -> Result<AdmitOutcome, StoreError> {
        let mut tx: Transaction<'_, Postgres> = self.pool.begin().await?;
        let inserted: Option<Uuid> = sqlx::query_scalar(
            "INSERT INTO events \
                 (event_id, source, event_type, payload, idempotency_key, request_fingerprint) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT ON CONSTRAINT events_source_idempotency_key_key DO NOTHING \
             RETURNING event_id",
        )
        .bind(event.id.as_uuid())
        .bind(event.source.as_str())
        .bind(event.event_type.as_str())
        .bind(&event.payload)
        .bind(key.expose())
        .bind(fingerprint.as_bytes().as_slice())
        .fetch_optional(&mut *tx)
        .await?;

        let outcome = match inserted {
            Some(id) => AdmitOutcome::Created(EventId::from_uuid(id)),
            None => {
                let existing: Option<(Uuid, Vec<u8>)> = sqlx::query_as(
                    "SELECT event_id, request_fingerprint FROM events \
                     WHERE source = $1 AND idempotency_key = $2",
                )
                .bind(event.source.as_str())
                .bind(key.expose())
                .fetch_optional(&mut *tx)
                .await?;
                match existing {
                    Some((id, stored)) if stored.as_slice() == fingerprint.as_bytes() => {
                        AdmitOutcome::Replayed(EventId::from_uuid(id))
                    }
                    Some(_) => AdmitOutcome::Conflict,
                    // Rows are never deleted, so a conflict implies a visible row.
                    None => return Err(StoreError::Inconsistent("conflicting row not found")),
                }
            }
        };
        tx.commit().await?;
        Ok(outcome)
    }

    /// Looks up an event's public status.
    pub async fn get(&self, id: EventId) -> Result<Option<EventRecord>, StoreError> {
        // Timestamps are formatted as RFC 3339 UTC by PostgreSQL, which avoids a
        // date/time dependency just for serialization.
        let row: Option<(Uuid, String, String, String, String, Option<String>)> =
            sqlx::query_as(
                "SELECT event_id, source, event_type, status, \
                        to_char(accepted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'), \
                        to_char(processed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') \
                 FROM events WHERE event_id = $1",
            )
                .bind(id.as_uuid())
                .fetch_optional(&self.pool)
                .await?;
        row.map(
            |(id, source, event_type, status, accepted_at, processed_at)| {
                Ok(EventRecord {
                    event_id: EventId::from_uuid(id),
                    source,
                    event_type,
                    status: EventStatus::from_db_str(&status)
                        .ok_or(StoreError::Inconsistent("unknown status"))?,
                    accepted_at,
                    processed_at,
                })
            },
        )
        .transpose()
    }

    /// Atomically claims up to `limit` events for `owner`, oldest first.
    ///
    /// Claimable means `PENDING`, or `PROCESSING` with an expired lease (the
    /// previous owner is presumed dead). Each claimed row becomes `PROCESSING`
    /// with `owner`, a lease of `lease` from now (database clock), and
    /// `delivery_attempts + 1`. Rows locked by a concurrent claim are skipped,
    /// never double-claimed.
    pub async fn claim(
        &self,
        owner: WorkerId,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<ClaimedEvent>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows: Vec<(Uuid, String, String, Value, i64, i32, String)> = sqlx::query_as(
            "WITH candidates AS ( \
                 SELECT event_id, status AS previous_status FROM events \
                 WHERE status = 'PENDING' \
                    OR (status = 'PROCESSING' AND lease_expires_at <= now()) \
                 ORDER BY accepted_at, event_id \
                 LIMIT $2 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             UPDATE events e SET \
                 status = 'PROCESSING', \
                 processing_owner = $1, \
                 processing_started_at = now(), \
                 lease_expires_at = now() + make_interval(secs => $3), \
                 delivery_attempts = e.delivery_attempts + 1 \
             FROM candidates c \
             WHERE e.event_id = c.event_id \
             RETURNING e.event_id, e.source, e.event_type, e.payload, \
                       (extract(epoch FROM e.accepted_at) * 1000000)::bigint, \
                       e.delivery_attempts, c.previous_status",
        )
        .bind(owner.as_uuid())
        .bind(limit)
        .bind(lease.as_secs_f64())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(
                |(id, source, event_type, payload, accepted_us, attempts, previous)| {
                    let accepted_at = u64::try_from(accepted_us)
                        .map(|us| UNIX_EPOCH + Duration::from_micros(us))
                        .unwrap_or(SystemTime::UNIX_EPOCH);
                    Ok(ClaimedEvent {
                        event: Event {
                            id: EventId::from_uuid(id),
                            source: EventSource::parse(source)
                                .map_err(|_| StoreError::Inconsistent("invalid stored source"))?,
                            event_type: EventType::parse(event_type).map_err(|_| {
                                StoreError::Inconsistent("invalid stored event_type")
                            })?,
                            payload,
                            accepted_at,
                        },
                        attempt: u32::try_from(attempts)
                            .map_err(|_| StoreError::Inconsistent("negative delivery_attempts"))?,
                        reclaimed: previous == EventStatus::Processing.as_db_str(),
                    })
                },
            )
            .collect()
    }

    /// Marks `id` processed if, and only if, `owner` still holds its claim.
    ///
    /// Returns `false` when the claim was lost (lease expired and the event
    /// was reclaimed by another worker, or it is already processed). A stale
    /// owner can never overwrite the current owner's state.
    pub async fn complete(&self, id: EventId, owner: WorkerId) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "UPDATE events SET \
                 status = 'PROCESSED', \
                 processed_at = now(), \
                 processing_owner = NULL, \
                 lease_expires_at = NULL \
             WHERE event_id = $1 AND status = 'PROCESSING' AND processing_owner = $2",
        )
        .bind(id.as_uuid())
        .bind(owner.as_uuid())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// The underlying pool, for tests and diagnostics.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}
