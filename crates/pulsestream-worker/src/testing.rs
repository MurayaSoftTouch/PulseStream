//! Deterministic test processors (enabled for tests and by the `test-util`
//! feature). Not used in production builds.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pulsestream_core::event::{Event, EventId};
use pulsestream_core::failure::FailureCode;
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

/// One scripted outcome of [`ScriptedProcessor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Succeed,
    /// A retryable failure with this code.
    Retryable(&'static str),
    /// A permanent failure with this code.
    Permanent(&'static str),
    /// The processor panics.
    Panic,
}

impl Step {
    /// Parses `succeed`, `panic`, `retryable:<CODE>`, or `permanent:<CODE>`.
    pub fn parse(value: &'static str) -> Option<Self> {
        match value.split_once(':') {
            None if value == "succeed" => Some(Self::Succeed),
            None if value == "panic" => Some(Self::Panic),
            Some(("retryable", code)) => FailureCode::new(code).ok().map(|_| Self::Retryable(code)),
            Some(("permanent", code)) => FailureCode::new(code).ok().map(|_| Self::Permanent(code)),
            _ => None,
        }
    }
}

/// Fails or succeeds per event according to a script: the n-th delivery of an
/// event gets `script[n]`, and deliveries past the end repeat the last step.
/// It also records calls per event and detects the same event being processed
/// twice at once.
#[derive(Debug, Clone)]
pub struct ScriptedProcessor {
    inner: Arc<ScriptInner>,
}

#[derive(Debug)]
struct ScriptInner {
    script: Vec<Step>,
    calls: Mutex<HashMap<EventId, usize>>,
    active: Mutex<HashSet<EventId>>,
    overlaps: AtomicUsize,
}

impl ScriptedProcessor {
    /// # Panics
    ///
    /// Panics if `script` is empty or a step's failure code is invalid.
    pub fn new(script: impl Into<Vec<Step>>) -> Self {
        let script = script.into();
        assert!(!script.is_empty(), "script must have at least one step");
        for step in &script {
            if let Step::Retryable(code) | Step::Permanent(code) = step {
                FailureCode::new(*code).expect("scripted failure codes are valid");
            }
        }
        Self {
            inner: Arc::new(ScriptInner {
                script,
                calls: Mutex::new(HashMap::new()),
                active: Mutex::new(HashSet::new()),
                overlaps: AtomicUsize::new(0),
            }),
        }
    }

    /// Times `process` was called for `id`.
    pub fn calls(&self, id: EventId) -> usize {
        self.inner
            .calls
            .lock()
            .expect("not poisoned")
            .get(&id)
            .copied()
            .unwrap_or(0)
    }

    /// Calls across all events.
    pub fn total_calls(&self) -> usize {
        self.inner
            .calls
            .lock()
            .expect("not poisoned")
            .values()
            .sum()
    }

    /// Times an event started while the same event was already being processed.
    pub fn overlaps(&self) -> usize {
        self.inner.overlaps.load(Ordering::Acquire)
    }
}

impl EventProcessor for ScriptedProcessor {
    async fn process(&self, event: &Event) -> Result<(), ProcessError> {
        let inner = &self.inner;
        let delivery = {
            let mut calls = inner.calls.lock().expect("not poisoned");
            let count = calls.entry(event.id).or_insert(0);
            *count += 1;
            *count - 1
        };
        let step = inner
            .script
            .get(delivery)
            .or(inner.script.last())
            .copied()
            .expect("script is not empty");
        if !inner.active.lock().expect("not poisoned").insert(event.id) {
            inner.overlaps.fetch_add(1, Ordering::AcqRel);
        }
        tokio::task::yield_now().await; // encourage interleaving
        inner.active.lock().expect("not poisoned").remove(&event.id);

        let code = |code: &'static str| FailureCode::new(code).expect("validated in new");
        match step {
            Step::Succeed => Ok(()),
            Step::Retryable(c) => Err(ProcessError::retryable(
                code(c),
                "scripted retryable failure",
            )),
            Step::Permanent(c) => Err(ProcessError::permanent(
                code(c),
                "scripted permanent failure",
            )),
            Step::Panic => panic!("scripted processor panic"),
        }
    }
}
