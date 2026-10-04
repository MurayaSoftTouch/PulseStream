//! The bounded, PostgreSQL-backed worker loop (ADR-007).
//!
//! ```text
//! loop:
//!   free = concurrency - active tasks
//!   if free > 0: claim ≤ free events (FOR UPDATE SKIP LOCKED) and spawn one task per claim
//!   wait for: shutdown | a task finishing | poll interval (only if a slot is still free)
//! ```
//!
//! - **Bounded.** A claim never asks for more than the free capacity, and
//!   tasks leave the `JoinSet` only when joined, so claimed-but-unfinished
//!   events and live tasks never exceed `concurrency`. Nothing is prefetched.
//! - **No busy polling.** With free capacity but no work, the loop sleeps for
//!   `poll_interval`. With no free capacity, it waits for a task to finish.
//! - **Recovery.** Claims carry a lease. If this process dies, its claims stay
//!   `PROCESSING` until their leases expire, and any worker then reclaims them.
//! - **Shutdown.** Stops claiming, waits up to `shutdown_timeout` for active
//!   events, and then aborts the rest. Aborted claims are *not* reset: they
//!   stay `PROCESSING` and are recovered by lease expiry, like a crash.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pulsestream_core::config::WorkerRuntimeConfig;
use pulsestream_core::event::{EventId, WorkerId};
use pulsestream_store::{ClaimedEvent, Store};
use tokio::task::{JoinError, JoinSet};
use tracing::{error, info, warn};

use crate::processor::EventProcessor;

/// Outcome of [`run`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RunReport {
    /// Events this worker processed and durably completed.
    pub processed: u64,
    /// Processor errors or panics, plus completions that could not be written.
    /// These claims stay `PROCESSING` until their lease expires.
    pub failed: u64,
    /// Completions rejected because the claim had been lost (lease expired and
    /// the event was reclaimed elsewhere).
    pub lost_claims: u64,
    /// True if shutdown gave up waiting for active events.
    pub timed_out: bool,
    /// Active claims aborted at shutdown. They are recovered after lease expiry.
    pub abandoned: usize,
}

#[derive(Debug, Default)]
struct Counters {
    processed: AtomicU64,
    failed: AtomicU64,
    lost_claims: AtomicU64,
}

/// Runs the claim/process loop until `shutdown` resolves, then drains.
pub async fn run<P: EventProcessor>(
    store: Store,
    owner: WorkerId,
    config: WorkerRuntimeConfig,
    processor: P,
    shutdown: impl Future<Output = ()>,
) -> RunReport {
    assert!(config.concurrency > 0, "concurrency must be > 0");
    let processor = Arc::new(processor);
    let counters = Arc::new(Counters::default());
    let mut tasks: JoinSet<()> = JoinSet::new();
    // Running task -> its event, so a panic can be attributed. At most `concurrency` entries.
    let mut running: HashMap<tokio::task::Id, EventId> = HashMap::new();
    let mut database_available = true;
    tokio::pin!(shutdown);

    info!(
        worker_id = %owner,
        concurrency = config.concurrency,
        lease_ms = u64::try_from(config.lease.as_millis()).unwrap_or(u64::MAX),
        poll_interval_ms = u64::try_from(config.poll_interval.as_millis()).unwrap_or(u64::MAX),
        "worker runtime started"
    );

    loop {
        while let Some(result) = tasks.try_join_next_with_id() {
            record_join(result, &mut running, &counters);
        }

        let free = config.concurrency - tasks.len();
        if free > 0 {
            match store.claim(owner, free, config.lease).await {
                Ok(claims) => {
                    if !database_available {
                        info!(worker_id = %owner, "database available again; claiming resumed");
                        database_available = true;
                    }
                    for claim in claims {
                        let event_id = claim.event.id;
                        let handle = tasks.spawn(process_claim(
                            store.clone(),
                            owner,
                            Arc::clone(&processor),
                            claim,
                            Arc::clone(&counters),
                        ));
                        running.insert(handle.id(), event_id);
                    }
                }
                Err(err) => {
                    if database_available {
                        warn!(
                            worker_id = %owner,
                            error = %err,
                            unavailable = err.is_unavailable(),
                            "database unavailable; claiming paused"
                        );
                        database_available = false;
                    }
                }
            }
        }

        let slot_free = tasks.len() < config.concurrency;
        tokio::select! {
            biased;
            () = &mut shutdown => break,
            Some(result) = tasks.join_next_with_id(), if !tasks.is_empty() => {
                record_join(result, &mut running, &counters);
            }
            () = tokio::time::sleep(config.poll_interval), if slot_free => {}
        }
    }

    let active = tasks.len();
    info!(
        worker_id = %owner,
        active,
        "shutdown started; claiming stopped, waiting for active events"
    );
    let drained = tokio::time::timeout(config.shutdown_timeout, async {
        while let Some(result) = tasks.join_next_with_id().await {
            record_join(result, &mut running, &counters);
        }
    })
    .await
    .is_ok();

    let abandoned = if drained {
        0
    } else {
        let abandoned = tasks.len();
        tasks.shutdown().await;
        error!(
            worker_id = %owner,
            abandoned,
            "shutdown timed out; abandoned claims stay PROCESSING until their lease expires"
        );
        abandoned
    };

    let report = RunReport {
        processed: counters.processed.load(Ordering::Acquire),
        failed: counters.failed.load(Ordering::Acquire),
        lost_claims: counters.lost_claims.load(Ordering::Acquire),
        timed_out: !drained,
        abandoned,
    };
    info!(
        worker_id = %owner,
        processed = report.processed,
        failed = report.failed,
        lost_claims = report.lost_claims,
        abandoned = report.abandoned,
        "worker runtime stopped"
    );
    report
}

fn record_join(
    result: Result<(tokio::task::Id, ()), JoinError>,
    running: &mut HashMap<tokio::task::Id, EventId>,
    counters: &Counters,
) {
    match result {
        Ok((id, ())) => {
            running.remove(&id);
        }
        Err(err) => {
            let event_id = running.remove(&err.id());
            if err.is_panic() {
                counters.failed.fetch_add(1, Ordering::AcqRel);
                error!(
                    event_id = event_id.map(|id| id.to_string()),
                    state = "failed",
                    "event processing failed: processor panicked; claim left to expire"
                );
            }
        }
    }
}

async fn process_claim<P: EventProcessor>(
    store: Store,
    owner: WorkerId,
    processor: Arc<P>,
    claim: ClaimedEvent,
    counters: Arc<Counters>,
) {
    let event = &claim.event;
    let message = if claim.reclaimed {
        "expired lease reclaimed; event claimed"
    } else {
        "event claimed"
    };
    info!(
        event_id = %event.id,
        source = event.source.as_str(),
        event_type = event.event_type.as_str(),
        worker_id = %owner,
        attempt = claim.attempt,
        reclaimed = claim.reclaimed,
        state = "processing",
        "{message}"
    );

    if let Err(err) = processor.process(event).await {
        counters.failed.fetch_add(1, Ordering::AcqRel);
        warn!(
            event_id = %event.id,
            worker_id = %owner,
            attempt = claim.attempt,
            error = %err,
            state = "failed",
            "event processing failed; no retry policy until M3, claim left to expire"
        );
        return;
    }

    match store.complete(event.id, owner).await {
        Ok(true) => {
            counters.processed.fetch_add(1, Ordering::AcqRel);
            info!(
                event_id = %event.id,
                source = event.source.as_str(),
                event_type = event.event_type.as_str(),
                worker_id = %owner,
                attempt = claim.attempt,
                state = "processed",
                "event processed"
            );
        }
        Ok(false) => {
            counters.lost_claims.fetch_add(1, Ordering::AcqRel);
            warn!(
                event_id = %event.id,
                worker_id = %owner,
                attempt = claim.attempt,
                "claim lost before completion (lease expired and event was reclaimed); \
                 completion not recorded by this worker"
            );
        }
        Err(err) => {
            counters.failed.fetch_add(1, Ordering::AcqRel);
            warn!(
                event_id = %event.id,
                worker_id = %owner,
                attempt = claim.attempt,
                error = %err,
                "could not record completion; event stays PROCESSING and will be reclaimed after lease expiry"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use pulsestream_core::config::{DatabaseConfig, DatabaseUrl};
    use pulsestream_core::event::Event;
    use pulsestream_core::idempotency::{IdempotencyKey, RequestFingerprint};
    use pulsestream_store::testing::TestDatabase;
    use pulsestream_store::{AdmitOutcome, Store};
    use tokio::sync::oneshot;

    use super::*;
    use crate::processor::AcknowledgeProcessor;
    use crate::testing::GatedProcessor;

    fn config(concurrency: usize) -> WorkerRuntimeConfig {
        WorkerRuntimeConfig {
            concurrency,
            poll_interval: Duration::from_millis(10),
            lease: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(5),
        }
    }

    async fn seed(db: &TestDatabase, count: usize) -> Vec<EventId> {
        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            let event =
                Event::new("orders-api", "order.created", serde_json::json!({"n": i})).unwrap();
            let fp = RequestFingerprint::compute(&event.source, &event.event_type, &event.payload);
            let key = IdempotencyKey::parse(&format!("seed-{i}")).unwrap();
            match db.store.admit(&event, &key, &fp).await.unwrap() {
                AdmitOutcome::Created(id) => ids.push(id),
                other => panic!("{other:?}"),
            }
        }
        ids
    }

    /// Polls the database until `(pending, processing, processed)` matches.
    async fn wait_for_counts(db: &TestDatabase, expected: (i64, i64, i64)) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let counts = db.status_counts().await;
            if counts == expected {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out: counts {counts:?}, expected {expected:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    type Handle = (oneshot::Sender<()>, tokio::task::JoinHandle<RunReport>);

    fn start<P: EventProcessor>(
        store: Store,
        owner: WorkerId,
        config: WorkerRuntimeConfig,
        processor: P,
    ) -> Handle {
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(run(store, owner, config, processor, async {
            let _ = stopped.await;
        }));
        (stop, task)
    }

    async fn stop((stop, task): Handle) -> RunReport {
        let _ = stop.send(());
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("worker stopped")
            .unwrap()
    }

    #[tokio::test]
    async fn stops_promptly_while_database_is_unreachable() {
        // No PostgreSQL needed: nothing listens on port 1. Claims fail fast, the
        // loop sleeps between attempts, and shutdown still completes.
        let url = DatabaseUrl::parse("postgres://u:p@127.0.0.1:1/unreachable").unwrap();
        let mut db_config = DatabaseConfig::with_url(url);
        db_config.acquire_timeout = Duration::from_millis(100);
        let store = Store::connect_lazy(&db_config, "test").unwrap();
        let handle = start(store, WorkerId::new(), config(2), AcknowledgeProcessor);
        tokio::time::sleep(Duration::from_millis(250)).await;
        let report = stop(handle).await;
        assert_eq!(report, RunReport::default());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn processes_pending_events_to_processed() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 10).await;
        let handle = start(
            db.store.clone(),
            WorkerId::new(),
            config(4),
            AcknowledgeProcessor,
        );
        wait_for_counts(&db, (0, 0, 10)).await;
        let report = stop(handle).await;
        assert_eq!(
            (report.processed, report.failed, report.lost_claims),
            (10, 0, 0)
        );
        for id in ids {
            assert_eq!(db.lifecycle(id).await.attempts, 1);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn never_claims_or_processes_more_than_concurrency() {
        let db = TestDatabase::create().await;
        seed(&db, 20).await;
        let processor = GatedProcessor::new();
        let handle = start(
            db.store.clone(),
            WorkerId::new(),
            config(4),
            processor.clone(),
        );

        processor.wait_started(4).await;
        // Many poll intervals pass while every slot is blocked. A worker that
        // over-claimed or prefetched would show here as extra PROCESSING rows.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(processor.started(), 4);
        assert_eq!(db.status_counts().await, (16, 4, 0));

        processor.release(20);
        wait_for_counts(&db, (0, 0, 20)).await;
        let report = stop(handle).await;
        assert_eq!(report.processed, 20);
        assert_eq!(processor.max_observed(), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn shutdown_stops_claiming_and_finishes_active_events() {
        let db = TestDatabase::create().await;
        seed(&db, 6).await;
        let processor = GatedProcessor::new();
        let (stop_tx, task) = start(
            db.store.clone(),
            WorkerId::new(),
            config(2),
            processor.clone(),
        );
        processor.wait_started(2).await;

        stop_tx.send(()).unwrap(); // stop claiming first
        processor.release(2); // then let the two active events finish
        let report = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (report.processed, report.timed_out, report.abandoned),
            (2, false, 0)
        );
        assert_eq!(
            db.status_counts().await,
            (4, 0, 2),
            "nothing new was claimed"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn abandoned_claims_are_recovered_by_another_worker_after_lease_expiry() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 3).await;
        let stuck = GatedProcessor::new(); // never released: simulates a hung or crashed worker
        let worker_a = WorkerId::new();
        let mut short_shutdown = config(2);
        short_shutdown.shutdown_timeout = Duration::from_millis(100);
        let handle = start(db.store.clone(), worker_a, short_shutdown, stuck.clone());
        stuck.wait_started(2).await;

        let report = stop(handle).await;
        assert!(report.timed_out);
        assert_eq!(report.abandoned, 2);
        // A's claims were not reset; they wait for lease expiry.
        assert_eq!(db.status_counts().await, (1, 2, 0));
        let mut abandoned = Vec::new();
        for id in &ids {
            let lifecycle = db.lifecycle(*id).await;
            if lifecycle.status == "PROCESSING" {
                assert_eq!(lifecycle.owner, Some(worker_a.as_uuid()));
                abandoned.push(*id);
            }
        }
        for id in &abandoned {
            db.expire_lease(*id).await;
        }

        let handle = start(
            db.store.clone(),
            WorkerId::new(),
            config(2),
            AcknowledgeProcessor,
        );
        wait_for_counts(&db, (0, 0, 3)).await;
        assert_eq!(stop(handle).await.processed, 3);
        for id in &ids {
            let expected = if abandoned.contains(id) { 2 } else { 1 };
            assert_eq!(db.lifecycle(*id).await.attempts, expected);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn stale_worker_cannot_complete_after_its_claim_was_reclaimed() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 1).await;
        let slow = GatedProcessor::new();
        let handle_a = start(db.store.clone(), WorkerId::new(), config(1), slow.clone());
        slow.wait_started(1).await;

        db.expire_lease(ids[0]).await; // A is still working, but its lease has expired
        let worker_b = WorkerId::new();
        let handle_b = start(db.store.clone(), worker_b, config(1), AcknowledgeProcessor);
        wait_for_counts(&db, (0, 0, 1)).await; // B reclaimed and completed

        slow.release(1); // A finishes late
        let report_a = stop(handle_a).await;
        assert_eq!((report_a.processed, report_a.lost_claims), (0, 1));
        assert_eq!(stop(handle_b).await.processed, 1);
        assert_eq!(db.lifecycle(ids[0]).await.attempts, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn two_workers_share_work_without_overlap() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 40).await;
        // One processor shared by both workers detects cross-worker overlap.
        let processor = GatedProcessor::open();
        let a = start(
            db.store.clone(),
            WorkerId::new(),
            config(3),
            processor.clone(),
        );
        let b = start(
            db.store.clone(),
            WorkerId::new(),
            config(3),
            processor.clone(),
        );
        wait_for_counts(&db, (0, 0, 40)).await;
        let (ra, rb) = (stop(a).await, stop(b).await);

        assert_eq!(ra.processed + rb.processed, 40);
        assert_eq!(
            processor.overlaps(),
            0,
            "an event was processed by two workers at once"
        );
        let seen: HashSet<_> = processor.seen().into_iter().collect();
        assert_eq!(seen.len(), 40);
        assert_eq!(
            processor.seen().len(),
            40,
            "each event processed exactly once here"
        );
        assert!(processor.max_observed() <= 6);
        for id in ids {
            assert_eq!(db.lifecycle(id).await.attempts, 1);
        }
    }
}
