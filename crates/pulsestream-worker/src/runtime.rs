//! The bounded, PostgreSQL-backed worker loop (ADR-007, ADR-008).
//!
//! ```text
//! loop:
//!   free = concurrency - active tasks
//!   if free > 0:
//!     dead-letter events whose final attempt's lease expired
//!     claim ≤ free eligible events (FOR UPDATE SKIP LOCKED) and spawn one task per claim
//!   wait for: shutdown | a task finishing | poll interval (only if a slot is still free)
//!
//! task: process -> success          -> PROCESSED
//!               -> retryable, n < max -> PENDING, available_at = now + backoff
//!               -> retryable, n = max -> DEAD_LETTERED
//!               -> permanent          -> DEAD_LETTERED
//! ```
//!
//! - **Bounded.** A claim never asks for more than the free capacity, and
//!   tasks leave the `JoinSet` only when joined, so claimed-but-unfinished
//!   events and live tasks never exceed `concurrency`. Nothing is prefetched.
//! - **No busy polling.** With free capacity but no work, the loop sleeps for
//!   `poll_interval`. With no free capacity, it waits for a task to finish.
//! - **Recovery.** Claims carry a lease. If this process dies, its claims stay
//!   `PROCESSING` until their leases expire, and any worker then reclaims them
//!   without backoff: no failure was recorded. If that was the event's final
//!   permitted attempt, it is dead-lettered with `LEASE_EXPIRED` instead.
//! - **Failures.** A processor error is classified by its variant and handled
//!   by the [`RetryPolicy`]. Every outcome write is conditional on this worker
//!   still owning the claim.
//! - **Shutdown.** Stops claiming, waits up to `shutdown_timeout` for active
//!   events, and then aborts the rest. Aborted claims are *not* reset: they
//!   stay `PROCESSING` and are recovered by lease expiry, like a crash.

use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use pulsestream_core::config::WorkerRuntimeConfig;
use pulsestream_core::event::{EventId, WorkerId};
use pulsestream_core::failure::FailureDetail;
use pulsestream_core::retry::{RetryDecision, RetryPolicy};
use pulsestream_store::{ClaimedEvent, Store, StoreError};
use tokio::task::{JoinError, JoinSet};
use tracing::{error, info, warn};

use crate::processor::{EventProcessor, ProcessError};

/// Upper bound on events dead-lettered by one lease-expiry sweep. The sweep
/// holds no events in memory beyond its returned IDs.
const SWEEP_LIMIT: usize = 100;

/// Outcome of [`run`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RunReport {
    /// Events this worker processed and durably completed.
    pub processed: u64,
    /// Attempts that failed: processor errors and panics, plus outcomes that
    /// could not be written (those claims stay `PROCESSING` until their lease
    /// expires).
    pub failed: u64,
    /// Retries this worker scheduled after retryable failures.
    pub retries_scheduled: u64,
    /// Events this worker dead-lettered: permanent failures, exhausted retry
    /// budgets, and final attempts whose lease expired.
    pub dead_lettered: u64,
    /// Outcomes (success or failure) rejected because the claim had been lost
    /// (lease expired and the event was reclaimed elsewhere).
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
    retries_scheduled: AtomicU64,
    dead_lettered: AtomicU64,
    lost_claims: AtomicU64,
}

impl Counters {
    fn add(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::AcqRel);
    }
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
        lease_ms = millis(config.lease),
        poll_interval_ms = millis(config.poll_interval),
        max_delivery_attempts = config.retry.max_delivery_attempts(),
        retry_base_delay_ms = millis(config.retry.base_delay()),
        retry_max_delay_ms = millis(config.retry.max_delay()),
        "worker runtime started"
    );

    loop {
        while let Some(result) = tasks.try_join_next_with_id() {
            record_join(result, &mut running, &counters);
        }

        let free = config.concurrency - tasks.len();
        if free > 0 {
            match claim_work(&store, owner, free, &config, &counters).await {
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
                            config.retry,
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
        retries_scheduled: counters.retries_scheduled.load(Ordering::Acquire),
        dead_lettered: counters.dead_lettered.load(Ordering::Acquire),
        lost_claims: counters.lost_claims.load(Ordering::Acquire),
        timed_out: !drained,
        abandoned,
    };
    info!(
        worker_id = %owner,
        processed = report.processed,
        failed = report.failed,
        retries_scheduled = report.retries_scheduled,
        dead_lettered = report.dead_lettered,
        lost_claims = report.lost_claims,
        abandoned = report.abandoned,
        "worker runtime stopped"
    );
    report
}

fn millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Dead-letters final attempts whose lease expired, then claims up to `free`
/// eligible events.
async fn claim_work(
    store: &Store,
    owner: WorkerId,
    free: usize,
    config: &WorkerRuntimeConfig,
    counters: &Counters,
) -> Result<Vec<ClaimedEvent>, StoreError> {
    let max_attempts = config.retry.max_delivery_attempts();
    for dead in store
        .dead_letter_expired_final_attempts(max_attempts, SWEEP_LIMIT)
        .await?
    {
        Counters::add(&counters.dead_lettered);
        warn!(
            event_id = %dead.event_id,
            source = dead.source.as_str(),
            event_type = dead.event_type.as_str(),
            worker_id = %owner,
            attempt = dead.delivery_attempts,
            failure_code = pulsestream_core::failure::LEASE_EXPIRED,
            reason = "lease_expired_on_final_attempt",
            lifecycle = "event_dead_lettered",
            state = "dead_lettered",
            "event dead-lettered: final attempt's lease expired without a result"
        );
    }
    store.claim(owner, free, config.lease, max_attempts).await
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
            // Processor panics are caught inside the task; this only fires
            // if recording an outcome itself panicked.
            if err.is_panic() {
                Counters::add(&counters.failed);
                error!(
                    event_id = event_id.map(|id| id.to_string()),
                    state = "failed",
                    "worker task panicked; claim left to expire"
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
    policy: RetryPolicy,
    counters: Arc<Counters>,
) {
    let event = &claim.event;
    let (message, lifecycle) = if claim.reclaimed {
        ("expired lease reclaimed; event claimed", "lease_reclaimed")
    } else if claim.is_retry() {
        ("retry claimed", "retry_claimed")
    } else {
        ("event claimed", "claimed")
    };
    info!(
        event_id = %event.id,
        source = event.source.as_str(),
        event_type = event.event_type.as_str(),
        worker_id = %owner,
        attempt = claim.attempt,
        reclaimed = claim.reclaimed,
        lifecycle,
        state = "processing",
        "{message}"
    );

    let outcome = match CatchUnwind(Box::pin(processor.process(event))).await {
        Ok(outcome) => outcome,
        Err(_panic) => Err(ProcessError::Retryable(FailureDetail::processor_panicked())),
    };
    match outcome {
        Ok(()) => record_success(&store, owner, &claim, &counters).await,
        Err(err) => record_failure(&store, owner, &claim, err, policy, &counters).await,
    }
}

async fn record_success(store: &Store, owner: WorkerId, claim: &ClaimedEvent, counters: &Counters) {
    let event = &claim.event;
    match store.complete(event.id, owner).await {
        Ok(true) => {
            Counters::add(&counters.processed);
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
        Ok(false) => lost_claim(owner, claim, counters, "completion"),
        Err(err) => {
            Counters::add(&counters.failed);
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

/// Applies the retry policy to a failed attempt. The failure message is
/// persisted but deliberately not logged: only the code is.
async fn record_failure(
    store: &Store,
    owner: WorkerId,
    claim: &ClaimedEvent,
    err: ProcessError,
    policy: RetryPolicy,
    counters: &Counters,
) {
    let event = &claim.event;
    Counters::add(&counters.failed);
    let failure_code = err.detail().code.as_str();
    warn!(
        event_id = %event.id,
        source = event.source.as_str(),
        event_type = event.event_type.as_str(),
        worker_id = %owner,
        attempt = claim.attempt,
        failure_class = err.class(),
        failure_code,
        lifecycle = "processing_failed",
        "event processing failed"
    );

    let (detail, reason) = match err {
        ProcessError::Permanent(detail) => (detail, "permanent_failure"),
        ProcessError::Retryable(detail) => {
            match policy.after_retryable_failure(event.id, claim.attempt) {
                RetryDecision::RetryAfter(delay) => {
                    match store.schedule_retry(event.id, owner, delay, &detail).await {
                        Ok(Some(next_available_at)) => {
                            Counters::add(&counters.retries_scheduled);
                            info!(
                                event_id = %event.id,
                                source = event.source.as_str(),
                                event_type = event.event_type.as_str(),
                                worker_id = %owner,
                                attempt = claim.attempt,
                                failure_code = detail.code.as_str(),
                                delay_ms = millis(delay),
                                next_available_at,
                                lifecycle = "retry_scheduled",
                                state = "pending",
                                "retry scheduled"
                            );
                        }
                        Ok(None) => lost_claim(owner, claim, counters, "retry"),
                        Err(err) => outcome_write_failed(owner, claim, &err, "retry"),
                    }
                    return;
                }
                RetryDecision::Exhausted => (detail, "retry_exhausted"),
            }
        }
    };

    match store.dead_letter(event.id, owner, &detail).await {
        Ok(true) => {
            Counters::add(&counters.dead_lettered);
            warn!(
                event_id = %event.id,
                source = event.source.as_str(),
                event_type = event.event_type.as_str(),
                worker_id = %owner,
                attempt = claim.attempt,
                max_delivery_attempts = policy.max_delivery_attempts(),
                failure_code = detail.code.as_str(),
                reason,
                lifecycle = "event_dead_lettered",
                state = "dead_lettered",
                "event dead-lettered"
            );
        }
        Ok(false) => lost_claim(owner, claim, counters, "dead-letter"),
        Err(err) => outcome_write_failed(owner, claim, &err, "dead-letter"),
    }
}

fn lost_claim(owner: WorkerId, claim: &ClaimedEvent, counters: &Counters, outcome: &'static str) {
    Counters::add(&counters.lost_claims);
    warn!(
        event_id = %claim.event.id,
        worker_id = %owner,
        attempt = claim.attempt,
        outcome,
        "claim lost before the outcome was recorded (lease expired and event was reclaimed); \
         not recorded by this worker"
    );
}

fn outcome_write_failed(
    owner: WorkerId,
    claim: &ClaimedEvent,
    err: &StoreError,
    outcome: &'static str,
) {
    warn!(
        event_id = %claim.event.id,
        worker_id = %owner,
        attempt = claim.attempt,
        outcome,
        error = %err,
        "could not record failure outcome; event stays PROCESSING and will be reclaimed after lease expiry"
    );
}

/// Resolves to `Err` if the wrapped future panics, so a processor panic
/// becomes a recorded failure instead of an orphaned claim. The future is
/// boxed so polling it needs no `unsafe` pin projection.
struct CatchUnwind<F>(Pin<Box<F>>);

impl<F: Future> Future for CatchUnwind<F> {
    type Output = std::thread::Result<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.get_mut().0.as_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use pulsestream_core::config::{DatabaseConfig, DatabaseUrl, default_retry_policy};
    use pulsestream_core::event::Event;
    use pulsestream_core::idempotency::{IdempotencyKey, RequestFingerprint};
    use pulsestream_store::testing::TestDatabase;
    use pulsestream_store::{AdmitOutcome, Store};
    use tokio::sync::oneshot;

    use super::*;
    use crate::processor::AcknowledgeProcessor;
    use crate::testing::{GatedProcessor, ScriptedProcessor, Step};

    fn config(concurrency: usize) -> WorkerRuntimeConfig {
        WorkerRuntimeConfig {
            concurrency,
            poll_interval: Duration::from_millis(10),
            lease: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(5),
            retry: default_retry_policy(),
        }
    }

    /// Short real delays, so retry tests finish quickly without sleeping on
    /// arbitrary guesses.
    fn retry_config(concurrency: usize, max_attempts: u32) -> WorkerRuntimeConfig {
        WorkerRuntimeConfig {
            retry: RetryPolicy::new(
                max_attempts,
                Duration::from_millis(20),
                Duration::from_millis(80),
            )
            .unwrap(),
            ..config(concurrency)
        }
    }

    /// Polls until `id` has `status`.
    async fn wait_for_status(db: &TestDatabase, id: EventId, status: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let lifecycle = db.lifecycle(id).await;
            if lifecycle.status == status {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {status}: {lifecycle:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
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

    // ---- M3: retry and dead-letter ------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn retryable_failures_dead_letter_after_max_attempts() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 1).await;
        let processor = ScriptedProcessor::new([Step::Retryable("UPSTREAM_TIMEOUT")]);
        let handle = start(
            db.store.clone(),
            WorkerId::new(),
            retry_config(2, 3),
            processor.clone(),
        );
        wait_for_status(&db, ids[0], "DEAD_LETTERED").await;
        // Many poll intervals pass: a fourth claim would show up here.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let report = stop(handle).await;

        assert_eq!(
            processor.calls(ids[0]),
            3,
            "attempts 1 and 2 retried, 3 dead-lettered"
        );
        assert_eq!(
            (
                report.failed,
                report.retries_scheduled,
                report.dead_lettered,
                report.processed
            ),
            (3, 2, 1, 0)
        );
        let lifecycle = db.lifecycle(ids[0]).await;
        assert_eq!(lifecycle.attempts, 3);
        assert_eq!(lifecycle.owner, None);
        let failure = db.failure(ids[0]).await;
        assert_eq!(failure.code.as_deref(), Some("UPSTREAM_TIMEOUT"));
        assert_eq!(
            failure.message.as_deref(),
            Some("scripted retryable failure")
        );
        assert!(failure.dead_lettered && failure.failed && !failure.processed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn permanent_failure_dead_letters_on_the_first_attempt() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 1).await;
        let processor = ScriptedProcessor::new([Step::Permanent("INVALID_DESTINATION")]);
        let handle = start(
            db.store.clone(),
            WorkerId::new(),
            retry_config(1, 5),
            processor.clone(),
        );
        wait_for_status(&db, ids[0], "DEAD_LETTERED").await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let report = stop(handle).await;

        assert_eq!(processor.calls(ids[0]), 1, "budget remained, but no retry");
        assert_eq!((report.retries_scheduled, report.dead_lettered), (0, 1));
        assert_eq!(db.lifecycle(ids[0]).await.attempts, 1);
        let failure = db.failure(ids[0]).await;
        assert_eq!(failure.code.as_deref(), Some("INVALID_DESTINATION"));
        assert!(
            failure.available_at_is_accepted_at,
            "no retry was ever scheduled"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn event_succeeds_after_a_retry_and_keeps_its_failure_history() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 1).await;
        let processor =
            ScriptedProcessor::new([Step::Retryable("PROCESSOR_REJECTED"), Step::Succeed]);
        let handle = start(
            db.store.clone(),
            WorkerId::new(),
            retry_config(1, 5),
            processor.clone(),
        );
        wait_for_status(&db, ids[0], "PROCESSED").await;
        let report = stop(handle).await;

        assert_eq!(processor.calls(ids[0]), 2);
        assert_eq!(
            (
                report.processed,
                report.retries_scheduled,
                report.dead_lettered
            ),
            (1, 1, 0)
        );
        assert_eq!(db.lifecycle(ids[0]).await.attempts, 2);
        let failure = db.failure(ids[0]).await;
        assert!(failure.processed && !failure.dead_lettered);
        // Documented policy: the last failure is kept for diagnostics.
        assert_eq!(failure.code.as_deref(), Some("PROCESSOR_REJECTED"));
        assert!(failure.failed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn processor_panic_is_a_retryable_failure() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 1).await;
        let processor = ScriptedProcessor::new([Step::Panic, Step::Succeed]);
        let handle = start(
            db.store.clone(),
            WorkerId::new(),
            retry_config(1, 5),
            processor.clone(),
        );
        wait_for_status(&db, ids[0], "PROCESSED").await;
        let report = stop(handle).await;
        assert_eq!((report.processed, report.retries_scheduled), (1, 1));
        assert_eq!(db.lifecycle(ids[0]).await.attempts, 2);
        assert_eq!(
            db.failure(ids[0]).await.code.as_deref(),
            Some(pulsestream_core::failure::PROCESSOR_PANICKED)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn scheduled_retry_is_not_claimed_before_it_is_due() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 1).await;
        let processor =
            ScriptedProcessor::new([Step::Retryable("UPSTREAM_TIMEOUT"), Step::Succeed]);
        // A 60 s delay: only `make_available` can make the retry due in time.
        let mut slow_retry = retry_config(1, 5);
        slow_retry.retry =
            RetryPolicy::new(5, Duration::from_secs(60), Duration::from_secs(60)).unwrap();
        let handle = start(
            db.store.clone(),
            WorkerId::new(),
            slow_retry,
            processor.clone(),
        );

        // The event starts PENDING, so wait for the recorded failure instead.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while db.failure(ids[0]).await.code.is_none() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "no failure recorded"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(db.lifecycle(ids[0]).await.status, "PENDING");
        let failure = db.failure(ids[0]).await;
        assert!(
            failure.available_in_ms > 40_000.0,
            "retry scheduled in the future: {failure:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await; // ~20 polls
        assert_eq!(
            processor.calls(ids[0]),
            1,
            "not claimed before available_at"
        );
        assert_eq!(db.lifecycle(ids[0]).await.attempts, 1);

        db.make_available(ids[0]).await;
        wait_for_status(&db, ids[0], "PROCESSED").await;
        stop(handle).await;
        assert_eq!(processor.calls(ids[0]), 2);
        assert_eq!(db.lifecycle(ids[0]).await.attempts, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn stale_worker_cannot_record_a_failure_after_its_claim_was_reclaimed() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 1).await;
        let slow = GatedProcessor::new();
        let worker_a = WorkerId::new();
        // A blocks, then (once released) its processor would fail permanently.
        let failing_after_gate = FailAfterGate(slow.clone());
        let handle_a = start(db.store.clone(), worker_a, config(1), failing_after_gate);
        slow.wait_started(1).await;

        db.expire_lease(ids[0]).await; // 2. A's lease expires while it works
        let worker_b = WorkerId::new();
        let b_processor = GatedProcessor::new();
        let handle_b = start(db.store.clone(), worker_b, config(1), b_processor.clone());
        b_processor.wait_started(1).await; // 3. B reclaimed (attempt 2) and is processing

        slow.release(1); // 4. A reports its failure late
        let report_a = stop(handle_a).await;
        assert_eq!(
            (
                report_a.lost_claims,
                report_a.dead_lettered,
                report_a.retries_scheduled
            ),
            (1, 0, 0)
        );
        let lifecycle = db.lifecycle(ids[0]).await; // 5. A changed nothing
        assert_eq!(lifecycle.status, "PROCESSING");
        assert_eq!(
            lifecycle.owner,
            Some(worker_b.as_uuid()),
            "6. B is still the owner"
        );
        assert_eq!(lifecycle.attempts, 2);
        assert_eq!(db.failure(ids[0]).await.code, None);

        b_processor.release(1);
        wait_for_status(&db, ids[0], "PROCESSED").await;
        assert_eq!(stop(handle_b).await.processed, 1);
    }

    /// Blocks like `GatedProcessor`, then fails permanently.
    #[derive(Clone)]
    struct FailAfterGate(GatedProcessor);

    impl EventProcessor for FailAfterGate {
        async fn process(&self, event: &Event) -> Result<(), ProcessError> {
            self.0.process(event).await?;
            Err(ProcessError::permanent(
                pulsestream_core::failure::FailureCode::new("PROCESSOR_REJECTED").unwrap(),
                "late failure",
            ))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn final_attempt_with_an_expired_lease_is_dead_lettered_not_reclaimed() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 1).await;
        let stuck = GatedProcessor::new(); // never released: a hung worker
        let mut short_shutdown = retry_config(1, 1);
        short_shutdown.shutdown_timeout = Duration::from_millis(50);
        let handle = start(
            db.store.clone(),
            WorkerId::new(),
            short_shutdown,
            stuck.clone(),
        );
        stuck.wait_started(1).await;
        assert!(stop(handle).await.timed_out);
        db.expire_lease(ids[0]).await; // attempt 1 of 1 never reported

        let fresh = ScriptedProcessor::new([Step::Succeed]);
        let handle = start(
            db.store.clone(),
            WorkerId::new(),
            retry_config(1, 1),
            fresh.clone(),
        );
        wait_for_status(&db, ids[0], "DEAD_LETTERED").await;
        let report = stop(handle).await;
        assert_eq!(fresh.total_calls(), 0, "no attempt beyond the maximum");
        assert_eq!(report.dead_lettered, 1);
        assert_eq!(db.lifecycle(ids[0]).await.attempts, 1);
        assert_eq!(
            db.failure(ids[0]).await.code.as_deref(),
            Some("LEASE_EXPIRED")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn two_workers_share_retries_without_overlap() {
        let db = TestDatabase::create().await;
        let ids = seed(&db, 30).await;
        // Every event fails once, then succeeds. One shared processor detects
        // an event being processed by both workers at once.
        let processor =
            ScriptedProcessor::new([Step::Retryable("UPSTREAM_TIMEOUT"), Step::Succeed]);
        let a = start(
            db.store.clone(),
            WorkerId::new(),
            retry_config(3, 5),
            processor.clone(),
        );
        let b = start(
            db.store.clone(),
            WorkerId::new(),
            retry_config(3, 5),
            processor.clone(),
        );
        wait_for_counts(&db, (0, 0, 30)).await;
        let (ra, rb) = (stop(a).await, stop(b).await);

        assert_eq!(
            processor.overlaps(),
            0,
            "an event was processed twice at once"
        );
        assert_eq!(ra.processed + rb.processed, 30);
        assert_eq!(ra.retries_scheduled + rb.retries_scheduled, 30);
        for id in ids {
            assert_eq!(processor.calls(id), 2);
            assert_eq!(db.lifecycle(id).await.attempts, 2);
        }
    }
}
