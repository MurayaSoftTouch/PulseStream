//! Real-PostgreSQL tests for the durable store.
//!
//! Run with `PULSESTREAM_TEST_DATABASE_URL=postgres://.../pulsestream_test
//! cargo test --workspace -- --ignored`. Each test owns a disposable database
//! (see `pulsestream_store::testing`).

use std::collections::HashSet;
use std::time::Duration;

use pulsestream_core::event::{Event, EventId, EventStatus, WorkerId};
use pulsestream_core::idempotency::{IdempotencyKey, RequestFingerprint};
use pulsestream_store::testing::TestDatabase;
use pulsestream_store::{AdmitOutcome, Store};
use serde_json::{Value, json};
use tokio::task::JoinSet;

const LEASE: Duration = Duration::from_secs(30);

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
    assert_eq!(applied, vec![(1, true)]);
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
    let claimed = db.store.claim(owner, 2, LEASE).await.unwrap();
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
    assert!(db.store.claim(owner, 0, LEASE).await.unwrap().is_empty());
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
                let batch = store.claim(owner, 3, LEASE).await.unwrap();
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
    assert_eq!(db.store.claim(a, 10, LEASE).await.unwrap().len(), 1);

    let b = WorkerId::new();
    assert!(db.store.claim(b, 10, LEASE).await.unwrap().is_empty());
    assert_eq!(db.lifecycle(id).await.owner, Some(a.as_uuid()));
}

#[tokio::test]
#[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
async fn expired_lease_is_reclaimed_with_incremented_attempts() {
    let db = TestDatabase::create().await;
    let id = admit_new(&db.store, "orders-api", "k").await;
    let a = WorkerId::new();
    db.store.claim(a, 1, LEASE).await.unwrap();
    // Worker A "crashes": its claim stays PROCESSING until the lease expires.
    db.expire_lease(id).await;

    let b = WorkerId::new();
    let reclaimed = db.store.claim(b, 1, LEASE).await.unwrap();
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

    db.store.claim(a, 1, LEASE).await.unwrap(); // 1. A claims
    db.expire_lease(id).await; // 2. A's lease expires
    assert_eq!(db.store.claim(b, 1, LEASE).await.unwrap().len(), 1); // 3. B reclaims

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
    // Lifecycle consistency and the M2 status set.
    assert!(rejects("UPDATE events SET status = 'PROCESSED' WHERE event_id = $1").await);
    assert!(rejects("UPDATE events SET status = 'PROCESSING' WHERE event_id = $1").await);
    assert!(rejects("UPDATE events SET status = 'RETRYING' WHERE event_id = $1").await);
    assert!(rejects("UPDATE events SET delivery_attempts = -1 WHERE event_id = $1").await);

    let lifecycle = db.lifecycle(id).await;
    assert_eq!(
        (lifecycle.status.as_str(), lifecycle.attempts),
        ("PENDING", 0)
    );
}
