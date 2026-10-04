//! Deterministic test processors (enabled for tests and by the `test-util`
//! feature). Not used in production builds.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pulsestream_core::event::{Event, EventId};
use tokio::sync::Semaphore;

use crate::processor::{EventProcessor, ProcessError};

/// A valid event for tests.
pub fn event() -> Event {
    Event::new("test-source", "test.event", serde_json::json!({"n": 1}))
        .expect("test event is valid")
}

/// Records concurrency and, when gated, blocks each event until the test
/// releases it. Tests coordinate through semaphores rather than sleeps.
#[derive(Debug, Clone)]
pub struct GatedProcessor {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// `None` means events pass straight through.
    gate: Option<Semaphore>,
    /// One permit is added per started event.
    started_signal: Semaphore,
    started: AtomicUsize,
    current: AtomicUsize,
    max_observed: AtomicUsize,
    completed: AtomicUsize,
    seen: Mutex<Vec<EventId>>,
    /// Events currently inside `process`, to detect the same event being
    /// processed twice at once (for example, by two workers).
    active: Mutex<HashSet<EventId>>,
    overlaps: AtomicUsize,
}

impl GatedProcessor {
    /// Every event blocks until [`release`](Self::release) is called.
    pub fn new() -> Self {
        Self::build(Some(Semaphore::new(0)))
    }

    /// Events are not blocked. They yield once, to encourage interleaving.
    pub fn open() -> Self {
        Self::build(None)
    }

    fn build(gate: Option<Semaphore>) -> Self {
        Self {
            inner: Arc::new(Inner {
                gate,
                started_signal: Semaphore::new(0),
                started: AtomicUsize::new(0),
                current: AtomicUsize::new(0),
                max_observed: AtomicUsize::new(0),
                completed: AtomicUsize::new(0),
                seen: Mutex::new(Vec::new()),
                active: Mutex::new(HashSet::new()),
                overlaps: AtomicUsize::new(0),
            }),
        }
    }

    /// Waits until `n` more events have started processing.
    pub async fn wait_started(&self, n: u32) {
        self.inner
            .started_signal
            .acquire_many(n)
            .await
            .expect("semaphore is never closed")
            .forget();
    }

    /// Lets `n` blocked or future events finish.
    pub fn release(&self, n: usize) {
        if let Some(gate) = &self.inner.gate {
            gate.add_permits(n);
        }
    }

    pub fn started(&self) -> usize {
        self.inner.started.load(Ordering::Acquire)
    }

    /// Events currently inside `process`.
    pub fn current(&self) -> usize {
        self.inner.current.load(Ordering::Acquire)
    }

    pub fn max_observed(&self) -> usize {
        self.inner.max_observed.load(Ordering::Acquire)
    }

    pub fn completed(&self) -> usize {
        self.inner.completed.load(Ordering::Acquire)
    }

    /// Times an event started while the same event was already being processed.
    pub fn overlaps(&self) -> usize {
        self.inner.overlaps.load(Ordering::Acquire)
    }

    /// Event IDs in the order processing started.
    pub fn seen(&self) -> Vec<EventId> {
        self.inner.seen.lock().expect("not poisoned").clone()
    }
}

impl Default for GatedProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl EventProcessor for GatedProcessor {
    async fn process(&self, event: &Event) -> Result<(), ProcessError> {
        let inner = &self.inner;
        let now = inner.current.fetch_add(1, Ordering::AcqRel) + 1;
        inner.max_observed.fetch_max(now, Ordering::AcqRel);
        inner.seen.lock().expect("not poisoned").push(event.id);
        if !inner.active.lock().expect("not poisoned").insert(event.id) {
            inner.overlaps.fetch_add(1, Ordering::AcqRel);
        }
        inner.started.fetch_add(1, Ordering::AcqRel);
        inner.started_signal.add_permits(1);

        match &inner.gate {
            Some(gate) => gate.acquire().await.expect("gate is never closed").forget(),
            None => tokio::task::yield_now().await,
        }

        inner.active.lock().expect("not poisoned").remove(&event.id);
        inner.current.fetch_sub(1, Ordering::AcqRel);
        inner.completed.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}
