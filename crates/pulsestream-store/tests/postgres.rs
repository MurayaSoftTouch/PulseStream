//! Real-PostgreSQL tests for the durable store.
//!
//! Run with `PULSESTREAM_TEST_DATABASE_URL=postgres://.../pulsestream_test
//! cargo test --workspace -- --ignored`. Each test owns a disposable database
//! (see `pulsestream_store::testing`).

use std::collections::HashSet;
use std::time::Duration;

use pulsestream_core::event::{Event, EventId, EventStatus, WorkerId};
use pulsestream_core::failure::{FailureCode, FailureDetail};
use pulsestream_core::idempotency::{IdempotencyKey, RequestFingerprint};
use pulsestream_store::testing::TestDatabase;
use pulsestream_store::{AdmitOutcome, MIGRATOR, Store};
use serde_json::{Value, json};
use tokio::task::JoinSet;

const LEASE: Duration = Duration::from_secs(30);
/// Default maximum delivery attempts.
const MAX: u32 = 5;
/// Long enough that a scheduled retry is never due during a test unless the
/// test makes it due.
const LONG_DELAY: Duration = Duration::from_secs(600);

fn failure(code: &str) -> FailureDetail {
    FailureDetail::new(FailureCode::new(code).unwrap(), format!("{code} in test"))
}

fn request(source: &str, payload: Value) -> (Event, RequestFingerprint) {
    let event = Event::new(source, "order.created", payload).unwrap();
    let fingerprint = RequestFingerprint::compute(&event.source, &event.event_type, &event.payload);
    (event, fingerprint)
}

fn key(value: &str) -> IdempotencyKey {
    IdempotencyKey::parse(value).unwrap()
}

async fn admit(store: &Store, source: &str, idem: &str, payload: Value) -> AdmitOutcome {
    let (event, fingerprint) = request(source, payload);
    store.admit(&event, &key(idem), &fingerprint).await.unwrap()
}

async fn admit_new(store: &Store, source: &str, idem: &str) -> EventId {
    match admit(store, source, idem, json!({"k": idem})).await {
        AdmitOutcome::Created(id) => id,
        other => panic!("expected Created, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn migrations_apply_to_empty_database_and_rerun_is_a_no_op() {
    let db = TestDatabase::create_empty().await;
    let exists = || async {
        sqlx::query_scalar::<_, bool>("SELECT to_regclass('public.events') IS NOT NULL")
            .fetch_one(db.store.pool())
            .await
            .unwrap()
    };
    assert!(!exists().await, "fresh database must have no schema");

    db.store.migrate().await.unwrap();
    assert!(exists().await);
    db.store.migrate().await.unwrap(); // idempotent

    let applied: Vec<(i64, bool)> =
        sqlx::query_as("SELECT version, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(db.store.pool())
            .await
            .unwrap();
    assert_eq!(applied, vec![(1, true), (2, true)]);
}

/// The M3 migration upgrades a populated M2 database: every row is kept, each
/// M2 state stays valid, and claiming works as before.
#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn m2_database_with_data_upgrades_in_place() {
    let db = TestDatabase::create_empty().await;
    MIGRATOR.run_to(1, db.store.pool()).await.unwrap(); // the published M2 schema only

    // M2 rows in every M2 state, written with M2's own column set.
    let pending = admit_new(&db.store, "orders-api", "pending").await;
    let processing = admit_new(&db.store, "orders-api", "processing").await;
    let expired = admit_new(&db.store, "orders-api", "expired").await;
    let processed = admit_new(&db.store, "orders-api", "processed").await;
    let m2_lease = |id: EventId, lease: &'static str| {
        let pool = db.store.pool().clone();
        async move {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE events SET status = 'PROCESSING', processing_owner = gen_random_uuid(), \
                 processing_started_at = now(), lease_expires_at = now() + interval '{lease}', \
                 delivery_attempts = 1 WHERE event_id = $1"
            )))
            .bind(id.as_uuid())
            .execute(&pool)
            .await
            .unwrap();
        }
    };
    m2_lease(processing, "1 hour").await;
    m2_lease(expired, "-1 second").await;
    m2_lease(processed, "1 hour").await;
    sqlx::query(
        "UPDATE events SET status = 'PROCESSED', processed_at = now(), processing_owner = NULL, \
         lease_expires_at = NULL WHERE event_id = $1",
    )
    .bind(processed.as_uuid())
    .execute(db.store.pool())
    .await
    .unwrap();
    let before: Vec<(uuid::Uuid, Value, String, i32)> = sqlx::query_as(
        "SELECT event_id, payload, status, delivery_attempts FROM events ORDER BY event_id",
    )
    .fetch_all(db.store.pool())
    .await
    .unwrap();

    db.store.migrate().await.unwrap(); // M2 -> M3

    let after: Vec<(uuid::Uuid, Value, String, i32)> = sqlx::query_as(
        "SELECT event_id, payload, status, delivery_attempts FROM events ORDER BY event_id",
    )
    .fetch_all(db.store.pool())
    .await
    .unwrap();
    assert_eq!(
        before, after,
        "rows, content, states, and attempts preserved"
    );
    for id in [pending, processing, expired, processed] {
        let state = db.failure(id).await;
        assert!(
            state.available_at_is_accepted_at,
            "backfilled from accepted_at"
        );
        assert_eq!(state.code, None);
        assert!(!state.dead_lettered);
    }

    // Claims behave as in M2: the pending row and the expired lease are
    // claimable; the live lease and the processed row are not.
    let claimed: HashSet<_> = db
        .store
        .claim(WorkerId::new(), 10, LEASE, MAX)
        .await
        .unwrap()
        .into_iter()
        .map(|c| (c.event.id, c.attempt))
        .collect();
    assert_eq!(claimed, HashSet::from([(pending, 1), (expired, 2)]));

    // And new M3 events work alongside the upgraded rows.
    let fresh = admit_new(&db.store, "orders-api", "fresh").await;
    assert!(db.failure(fresh).await.available_at_is_accepted_at);
    let applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(db.store.pool())
            .await
            .unwrap();
    assert_eq!(applied, [1, 2]);
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn new_key_creates_one_pending_event() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "key-1").await;

    let record = db.store.get(id).await.unwrap().unwrap();
    assert_eq!(record.status, EventStatus::Pending);
    assert_eq!(record.source, "orders-api");
    assert_eq!(record.event_type, "order.created");
    assert!(record.accepted_at.ends_with('Z') && record.accepted_at.contains('T'));
    assert_eq!(record.processed_at, None);
    assert_eq!(record.dead_lettered_at, None);
    assert_eq!(record.delivery_attempts, 0);
    assert!(
        db.failure(id).await.available_at_is_accepted_at,
        "a new event is available as soon as it is accepted"
    );
    assert_eq!(db.count_for_source("orders-api").await, 1);
    assert_eq!(db.store.get(EventId::new()).await.unwrap(), None);
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn identical_replay_returns_original_event_without_new_row() {
    let db = TestDatabase::create().await;
    let payload = json!({"order_id": "12345", "lines": [1, 2]});
    let AdmitOutcome::Created(original) = admit(&db.store, "orders-api", "k", payload).await else {
        panic!("first request must create");
    };
    // Same semantics, different object key order: still an exact replay.
    let reordered = json!({"lines": [1, 2], "order_id": "12345"});
    assert_eq!(
        admit(&db.store, "orders-api", "k", reordered).await,
        AdmitOutcome::Replayed(original)
    );
    assert_eq!(db.count_for_source("orders-api").await, 1);
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn conflicting_reuse_is_rejected_and_never_rewrites_the_row() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "k").await;
    assert_eq!(
        admit(&db.store, "orders-api", "k", json!({"different": true})).await,
        AdmitOutcome::Conflict
    );
    let payload: Value = sqlx::query_scalar("SELECT payload FROM events WHERE event_id = $1")
        .bind(id.as_uuid())
        .fetch_one(db.store.pool())
        .await
        .unwrap();
    assert_eq!(payload, json!({"k": "k"}));
    assert_eq!(db.count_for_source("orders-api").await, 1);
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn same_key_under_different_sources_is_independent() {
    let db = TestDatabase::create().await;
    let orders = admit_new(&db.store, "orders-api", "abc123").await;
    let billing = admit_new(&db.store, "billing-api", "abc123").await;
    assert_ne!(orders, billing);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn concurrent_identical_admissions_converge_on_one_row() {
    let db = TestDatabase::create().await;
    let mut tasks = JoinSet::new();
    for _ in 0..50 {
        let store = db.store.clone();
        tasks.spawn(async move {
            admit(&store, "orders-api", "race", json!({"order_id": "12345"})).await
        });
    }
    let outcomes = tasks.join_all().await;

    let created: Vec<_> = outcomes
        .iter()
        .filter_map(|o| match o {
            AdmitOutcome::Created(id) => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(created.len(), 1, "exactly one request creates the row");
    let winner = created[0];
    for outcome in &outcomes {
        assert!(
            matches!(outcome, AdmitOutcome::Created(id) | AdmitOutcome::Replayed(id) if *id == winner),
            "{outcome:?}"
        );
    }
    assert_eq!(db.count_for_source("orders-api").await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn concurrent_conflicting_admissions_leave_exactly_one_winner() {
    let db = TestDatabase::create().await;
    let mut tasks = JoinSet::new();
    for variant in 0..50 {
        let store = db.store.clone();
        tasks.spawn(async move {
            admit(
                &store,
                "orders-api",
                "contested",
                json!({"variant": variant}),
            )
            .await
        });
    }
    let outcomes = tasks.join_all().await;
    let created = outcomes
        .iter()
        .filter(|o| matches!(o, AdmitOutcome::Created(_)))
        .count();
    let conflicts = outcomes
        .iter()
        .filter(|o| **o == AdmitOutcome::Conflict)
        .count();
    assert_eq!((created, conflicts), (1, 49), "{outcomes:?}");
    assert_eq!(db.count_for_source("orders-api").await, 1);
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn committed_event_survives_a_new_pool() {
    let db = TestDatabase::create().await;
    let first = Store::connect_lazy(&db.config(), "instance-1").unwrap();
    let (event, fingerprint) = request("orders-api", json!({"n": 1}));
    let AdmitOutcome::Created(id) = first
        .admit(&event, &key("durable"), &fingerprint)
        .await
        .unwrap()
    else {
        panic!("expected Created");
    };
    first.close().await; // the first "application instance" is gone

    let second = Store::connect_lazy(&db.config(), "instance-2").unwrap();
    let record = second.get(id).await.unwrap().expect("event survived");
    assert_eq!(record.status, EventStatus::Pending);
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn claim_is_bounded_and_records_owner_lease_and_attempt() {
    let db = TestDatabase::create().await;
    let ids: Vec<_> = (0..5).map(|i| format!("k{i}")).collect::<Vec<_>>();
    for k in &ids {
        admit_new(&db.store, "orders-api", k).await;
    }
    let owner = WorkerId::new();
    let claimed = db.store.claim(owner, 2, LEASE, MAX).await.unwrap();
    assert_eq!(claimed.len(), 2, "never claims more than the limit");
    assert_eq!(db.status_counts().await, (3, 2, 0));
    for c in &claimed {
        assert_eq!(c.attempt, 1);
        assert!(!c.reclaimed);
        let lifecycle = db.lifecycle(c.event.id).await;
        assert_eq!(lifecycle.status, "PROCESSING");
        assert_eq!(lifecycle.owner, Some(owner.as_uuid()));
        assert_eq!(lifecycle.attempts, 1);
        assert_eq!(lifecycle.lease_active, Some(true));
    }
    assert!(
        db.store
            .claim(owner, 0, LEASE, MAX)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn concurrent_claimers_never_claim_the_same_event() {
    let db = TestDatabase::create().await;
    for i in 0..60 {
        admit_new(&db.store, "orders-api", &format!("k{i}")).await;
    }
    let mut tasks = JoinSet::new();
    for _ in 0..20 {
        let store = db.store.clone();
        tasks.spawn(async move {
            let owner = WorkerId::new();
            let mut mine = Vec::new();
            loop {
                let batch = store.claim(owner, 3, LEASE, MAX).await.unwrap();
                if batch.is_empty() {
                    break;
                }
                mine.extend(batch.into_iter().map(|c| (c.event.id, c.attempt)));
            }
            (owner, mine)
        });
    }
    let results = tasks.join_all().await;

    let mut seen = HashSet::new();
    for (owner, claims) in &results {
        for (id, attempt) in claims {
            assert!(seen.insert(*id), "event {id} claimed twice");
            assert_eq!(*attempt, 1);
            assert_eq!(db.lifecycle(*id).await.owner, Some(owner.as_uuid()));
        }
    }
    assert_eq!(seen.len(), 60, "every event claimed exactly once");
    assert_eq!(db.status_counts().await, (0, 60, 0));
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn unexpired_lease_is_not_stolen() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "k").await;
    let a = WorkerId::new();
    assert_eq!(db.store.claim(a, 10, LEASE, MAX).await.unwrap().len(), 1);

    let b = WorkerId::new();
    assert!(db.store.claim(b, 10, LEASE, MAX).await.unwrap().is_empty());
    assert_eq!(db.lifecycle(id).await.owner, Some(a.as_uuid()));
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn expired_lease_is_reclaimed_with_incremented_attempts() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "k").await;
    let a = WorkerId::new();
    db.store.claim(a, 1, LEASE, MAX).await.unwrap();
    // Worker A "crashes": its claim stays PROCESSING until the lease expires.
    db.expire_lease(id).await;

    let b = WorkerId::new();
    let reclaimed = db.store.claim(b, 1, LEASE, MAX).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].event.id, id);
    assert_eq!(reclaimed[0].attempt, 2);
    assert!(reclaimed[0].reclaimed);
    assert_eq!(reclaimed[0].event.payload, json!({"k": "k"}));

    assert!(db.store.complete(id, b).await.unwrap());
    let record = db.store.get(id).await.unwrap().unwrap();
    assert_eq!(record.status, EventStatus::Processed);
    assert!(record.processed_at.is_some());
    assert_eq!(db.lifecycle(id).await.attempts, 2);
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn stale_owner_cannot_complete_a_reclaimed_event() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "k").await;
    let a = WorkerId::new();
    let b = WorkerId::new();

    db.store.claim(a, 1, LEASE, MAX).await.unwrap(); // 1. A claims
    db.expire_lease(id).await; // 2. A's lease expires
    assert_eq!(db.store.claim(b, 1, LEASE, MAX).await.unwrap().len(), 1); // 3. B reclaims

    assert!(!db.store.complete(id, a).await.unwrap()); // 4. A's stale completion
    let lifecycle = db.lifecycle(id).await; // 5. B's claim is untouched
    assert_eq!(lifecycle.status, "PROCESSING");
    assert_eq!(lifecycle.owner, Some(b.as_uuid()));
    assert_eq!(lifecycle.attempts, 2);

    assert!(db.store.complete(id, b).await.unwrap());
    assert!(
        !db.store.complete(id, b).await.unwrap(),
        "completion is not repeatable"
    );
    assert!(!db.store.complete(id, a).await.unwrap());
    assert_eq!(db.lifecycle(id).await.status, "PROCESSED");
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn schema_rejects_content_changes_and_inconsistent_lifecycles() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "k").await;
    let rejects = |sql: &'static str| {
        let pool = db.store.pool().clone();
        async move {
            sqlx::query(sql)
                .bind(id.as_uuid())
                .execute(&pool)
                .await
                .is_err()
        }
    };

    // Immutable content.
    assert!(rejects("UPDATE events SET payload = '{\"x\":1}' WHERE event_id = $1").await);
    assert!(rejects("UPDATE events SET source = 'other' WHERE event_id = $1").await);
    assert!(rejects("UPDATE events SET idempotency_key = 'other' WHERE event_id = $1").await);
    assert!(rejects("UPDATE events SET event_id = gen_random_uuid() WHERE event_id = $1").await);
    assert!(
        rejects("UPDATE events SET accepted_at = now() - interval '1 day' WHERE event_id = $1")
            .await
    );
    // Lifecycle consistency and the status set.
    assert!(rejects("UPDATE events SET status = 'PROCESSED' WHERE event_id = $1").await);
    assert!(rejects("UPDATE events SET status = 'PROCESSING' WHERE event_id = $1").await);
    assert!(rejects("UPDATE events SET status = 'RETRYING' WHERE event_id = $1").await);
    assert!(rejects("UPDATE events SET status = 'FAILED' WHERE event_id = $1").await);
    assert!(rejects("UPDATE events SET delivery_attempts = -1 WHERE event_id = $1").await);
    assert!(rejects("UPDATE events SET available_at = NULL WHERE event_id = $1").await);
    // DEAD_LETTERED needs its timestamp, a failure code, and at least one attempt.
    assert!(rejects("UPDATE events SET status = 'DEAD_LETTERED' WHERE event_id = $1").await);
    assert!(
        rejects(
            "UPDATE events SET status = 'DEAD_LETTERED', dead_lettered_at = now(), \
             last_failure_code = 'X', last_failure_message = 'm', last_failed_at = now() \
             WHERE event_id = $1"
        )
        .await,
        "delivery_attempts is 0"
    );
    assert!(rejects("UPDATE events SET dead_lettered_at = now() WHERE event_id = $1").await);
    // Failure metadata: a stable code, a bounded message, all-or-nothing.
    assert!(
        rejects(
            "UPDATE events SET last_failure_code = 'lower_case', last_failure_message = 'm', \
             last_failed_at = now() WHERE event_id = $1"
        )
        .await
    );
    assert!(
        rejects(
            "UPDATE events SET last_failure_code = 'CODE', last_failure_message = repeat('x', 1025), \
             last_failed_at = now() WHERE event_id = $1"
        )
        .await
    );
    assert!(rejects("UPDATE events SET last_failure_code = 'CODE' WHERE event_id = $1").await);

    let lifecycle = db.lifecycle(id).await;
    assert_eq!(
        (lifecycle.status.as_str(), lifecycle.attempts),
        ("PENDING", 0)
    );
}

// ---- M3: retry scheduling and dead-lettering ---------------------------------

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn retryable_failure_schedules_a_future_retry_that_is_claimed_only_when_due() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "k").await;
    let a = WorkerId::new();
    assert_eq!(db.store.claim(a, 1, LEASE, MAX).await.unwrap().len(), 1);

    // 1-2. A retryable failure makes the event PENDING again, in the future.
    // The store applies the delay it is given (jitter is the worker's
    // concern), so the persisted delay must be exactly this. 2 s leaves a
    // wide margin for the "not yet claimable" poll below on a loaded host.
    let delay = Duration::from_secs(2);
    let available_at = db
        .store
        .schedule_retry(id, a, delay, &failure("UPSTREAM_TIMEOUT"))
        .await
        .unwrap()
        .expect("A owns the claim");
    assert!(available_at.ends_with('Z'));
    let lifecycle = db.lifecycle(id).await;
    assert_eq!(
        (
            lifecycle.status.as_str(),
            lifecycle.owner,
            lifecycle.lease_active
        ),
        ("PENDING", None, None)
    );
    // 3. `available_at` is scheduled in the future relative to the database
    // `now()` that recorded the failure, by exactly `delay`. This compares
    // two persisted timestamps, so host scheduling delays cannot affect it.
    let state = db.failure(id).await;
    let scheduled = state.scheduled_delay_ms.expect("failure recorded");
    assert!(
        (scheduled - 2_000.0).abs() < 0.01,
        "persisted delay {scheduled} ms, expected 2000 ms"
    );
    // Seen from now, it is still in the future and never beyond the delay.
    assert!(
        state.available_in_ms > 0.0 && state.available_in_ms <= 2_000.0,
        "available in {} ms",
        state.available_in_ms
    );
    assert_eq!(state.code.as_deref(), Some("UPSTREAM_TIMEOUT"));
    assert_eq!(state.message.as_deref(), Some("UPSTREAM_TIMEOUT in test"));
    assert!(state.failed && !state.dead_lettered);

    // 4. A poll before eligibility claims nothing.
    let b = WorkerId::new();
    assert!(db.store.claim(b, 10, LEASE, MAX).await.unwrap().is_empty());
    assert_eq!(db.lifecycle(id).await.attempts, 1, "no claim consumed");

    // 5. Once due (real time passes on the database clock), it is claimed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let claimed = loop {
        let batch = db.store.claim(b, 10, LEASE, MAX).await.unwrap();
        if !batch.is_empty() {
            break batch;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "retry never became claimable"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert!(
        db.failure(id).await.available_in_ms <= 0.0,
        "claimed only after available_at"
    );
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].event.id, id);
    assert_eq!(claimed[0].attempt, 2);
    assert!(claimed[0].is_retry() && !claimed[0].reclaimed);
    assert_eq!(
        claimed[0].event.payload,
        json!({"k": "k"}),
        "content unchanged"
    );
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn claim_selects_only_currently_eligible_pending_events() {
    let db = TestDatabase::create().await;
    let owner = WorkerId::new();
    let mut eligible = HashSet::new();
    let mut scheduled = HashSet::new();
    for i in 0..40 {
        let id = admit_new(&db.store, "orders-api", &format!("k{i}")).await;
        if i % 2 == 0 {
            eligible.insert(id);
        } else {
            scheduled.insert(id);
        }
    }
    // Move the odd events into the future-scheduled retry state.
    let mut to_schedule = scheduled.clone();
    while !to_schedule.is_empty() {
        for claim in db.store.claim(owner, 40, LEASE, MAX).await.unwrap() {
            let id = claim.event.id;
            if to_schedule.remove(&id) {
                db.store
                    .schedule_retry(id, owner, LONG_DELAY, &failure("UPSTREAM_TIMEOUT"))
                    .await
                    .unwrap()
                    .unwrap();
            } else {
                // Put eligible events back (attempt 1 consumed, due now).
                db.store
                    .schedule_retry(id, owner, Duration::ZERO, &failure("UPSTREAM_TIMEOUT"))
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
    }
    assert_eq!(db.count_with_status("PENDING").await, 40);

    let claimed: HashSet<_> = db
        .store
        .claim(WorkerId::new(), 100, LEASE, MAX)
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.event.id)
        .collect();
    assert_eq!(claimed, eligible, "only due events, never future retries");
    for id in &scheduled {
        assert_eq!(db.lifecycle(*id).await.status, "PENDING");
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn dead_letter_is_terminal_and_never_claimed() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "k").await;
    let other = admit_new(&db.store, "orders-api", "other").await;
    let a = WorkerId::new();
    let claimed = db.store.claim(a, 1, LEASE, MAX).await.unwrap();
    assert_eq!(claimed[0].event.id, id);
    assert!(
        db.store
            .dead_letter(id, a, &failure("INVALID_DESTINATION"))
            .await
            .unwrap()
    );

    let record = db.store.get(id).await.unwrap().unwrap();
    assert_eq!(record.status, EventStatus::DeadLettered);
    assert!(
        record
            .dead_lettered_at
            .as_deref()
            .is_some_and(|t| t.ends_with('Z'))
    );
    assert_eq!(record.processed_at, None);
    assert_eq!(record.delivery_attempts, 1, "attempt count preserved");
    let state = db.failure(id).await;
    assert_eq!(state.code.as_deref(), Some("INVALID_DESTINATION"));
    assert!(state.failed && state.dead_lettered && !state.processed);
    let payload: Value = sqlx::query_scalar("SELECT payload FROM events WHERE event_id = $1")
        .bind(id.as_uuid())
        .fetch_one(db.store.pool())
        .await
        .unwrap();
    assert_eq!(payload, json!({"k": "k"}), "original content preserved");

    // The standard claim path never selects it, even with a large limit and
    // every other event drained.
    let next = db
        .store
        .claim(WorkerId::new(), 100, LEASE, MAX)
        .await
        .unwrap();
    assert_eq!(next.iter().map(|c| c.event.id).collect::<Vec<_>>(), [other]);
    assert!(
        db.store
            .claim(WorkerId::new(), 100, LEASE, MAX)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        db.store
            .dead_letter_expired_final_attempts(MAX, 100)
            .await
            .unwrap()
            .is_empty()
    );
    // No further transition applies to a dead-lettered event.
    assert!(!db.store.complete(id, a).await.unwrap());
    assert!(!db.store.dead_letter(id, a, &failure("X")).await.unwrap());
    assert_eq!(
        db.store
            .schedule_retry(id, a, Duration::ZERO, &failure("X"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(db.lifecycle(id).await.status, "DEAD_LETTERED");
    assert_eq!(
        db.failure(id).await.code.as_deref(),
        Some("INVALID_DESTINATION")
    );
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn stale_owner_cannot_retry_or_dead_letter_a_reclaimed_event() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "k").await;
    let a = WorkerId::new();
    let b = WorkerId::new();

    db.store.claim(a, 1, LEASE, MAX).await.unwrap(); // 1. A claims
    db.expire_lease(id).await; // 2. A's lease expires
    assert_eq!(db.store.claim(b, 1, LEASE, MAX).await.unwrap().len(), 1); // 3. B reclaims

    // 4. A reports failures late.
    let retry = db
        .store
        .schedule_retry(id, a, Duration::ZERO, &failure("UPSTREAM_TIMEOUT"))
        .await
        .unwrap();
    assert_eq!(retry, None, "stale retry rejected");
    assert!(
        !db.store
            .dead_letter(id, a, &failure("PROCESSOR_REJECTED"))
            .await
            .unwrap()
    );

    // 5-6. Nothing changed; B is still the valid owner.
    let lifecycle = db.lifecycle(id).await;
    assert_eq!(lifecycle.status, "PROCESSING");
    assert_eq!(lifecycle.owner, Some(b.as_uuid()));
    assert_eq!(lifecycle.lease_active, Some(true));
    assert_eq!(lifecycle.attempts, 2);
    let state = db.failure(id).await;
    assert_eq!((state.code, state.failed), (None, false));

    assert!(db.store.complete(id, b).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn due_retry_is_claimed_by_exactly_one_of_many_concurrent_workers() {
    let db = TestDatabase::create().await;
    let mut ids = Vec::new();
    for i in 0..10 {
        ids.push(admit_new(&db.store, "orders-api", &format!("k{i}")).await);
    }
    let first = WorkerId::new();
    for claim in db.store.claim(first, 10, LEASE, MAX).await.unwrap() {
        db.store
            .schedule_retry(
                claim.event.id,
                first,
                LONG_DELAY,
                &failure("UPSTREAM_TIMEOUT"),
            )
            .await
            .unwrap()
            .unwrap();
    }
    for id in &ids {
        db.make_available(*id).await; // every retry becomes due at once
    }

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(20));
    let mut tasks = JoinSet::new();
    for _ in 0..20 {
        let store = db.store.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            let owner = WorkerId::new();
            barrier.wait().await;
            let claims = store.claim(owner, 2, LEASE, MAX).await.unwrap();
            (owner, claims)
        });
    }
    let results = tasks.join_all().await;

    let mut seen = HashSet::new();
    for (owner, claims) in &results {
        for claim in claims {
            assert!(
                seen.insert(claim.event.id),
                "retry {} claimed twice",
                claim.event.id
            );
            assert_eq!(claim.attempt, 2);
            assert!(claim.is_retry());
            assert_eq!(
                db.lifecycle(claim.event.id).await.owner,
                Some(owner.as_uuid())
            );
        }
    }
    assert_eq!(seen.len(), 10, "every due retry claimed exactly once");
    assert_eq!(db.status_counts().await, (0, 10, 0));
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn expired_final_attempt_is_dead_lettered_instead_of_reclaimed() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "k").await;
    let max = 2;
    db.store
        .claim(WorkerId::new(), 1, LEASE, max)
        .await
        .unwrap(); // attempt 1
    db.expire_lease(id).await;
    let second = db
        .store
        .claim(WorkerId::new(), 1, LEASE, max)
        .await
        .unwrap(); // attempt 2
    assert_eq!(second[0].attempt, 2);
    db.expire_lease(id).await; // the final attempt never reported

    assert!(
        db.store
            .claim(WorkerId::new(), 10, LEASE, max)
            .await
            .unwrap()
            .is_empty(),
        "no attempt 3"
    );
    let dead = db
        .store
        .dead_letter_expired_final_attempts(max, 10)
        .await
        .unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!((dead[0].event_id, dead[0].delivery_attempts), (id, 2));
    assert_eq!(db.lifecycle(id).await.status, "DEAD_LETTERED");
    assert_eq!(db.failure(id).await.code.as_deref(), Some("LEASE_EXPIRED"));
    assert!(
        db.store
            .dead_letter_expired_final_attempts(max, 10)
            .await
            .unwrap()
            .is_empty(),
        "the sweep is idempotent"
    );
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn sweep_leaves_live_leases_and_attempts_below_the_limit_alone() {
    let db = TestDatabase::create().await;
    let live = admit_new(&db.store, "orders-api", "live").await;
    let expired = admit_new(&db.store, "orders-api", "expired").await;
    let owner = WorkerId::new();
    assert_eq!(db.store.claim(owner, 2, LEASE, MAX).await.unwrap().len(), 2);
    db.expire_lease(expired).await;

    // Attempt 1 of 5 expired: crash recovery will reclaim it; not final.
    assert!(
        db.store
            .dead_letter_expired_final_attempts(MAX, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(db.lifecycle(expired).await.status, "PROCESSING");

    // With a limit of 1 attempt the expired claim is final; the live lease,
    // also on its final attempt, is untouched until it expires.
    let dead = db
        .store
        .dead_letter_expired_final_attempts(1, 10)
        .await
        .unwrap();
    assert_eq!(
        dead.iter().map(|d| d.event_id).collect::<Vec<_>>(),
        [expired]
    );
    let lifecycle = db.lifecycle(live).await;
    assert_eq!(lifecycle.status, "PROCESSING");
    assert_eq!(lifecycle.owner, Some(owner.as_uuid()));
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn crash_recovery_does_not_apply_retry_backoff() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "k").await;
    db.store
        .claim(WorkerId::new(), 1, LEASE, MAX)
        .await
        .unwrap();
    db.expire_lease(id).await;
    // No failure was recorded, so the event is reclaimable immediately.
    assert!(
        db.store
            .dead_letter_expired_final_attempts(MAX, 10)
            .await
            .unwrap()
            .is_empty()
    );
    let reclaimed = db
        .store
        .claim(WorkerId::new(), 1, LEASE, MAX)
        .await
        .unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert!(reclaimed[0].reclaimed && !reclaimed[0].is_retry());
    assert_eq!(db.failure(id).await.code, None);
}
