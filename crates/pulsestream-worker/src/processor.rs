//! The event-processing extension point.

use std::future::Future;

use pulsestream_core::event::Event;

/// A processing failure. It is logged; M2 has no retry policy (M3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessError(pub String);

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ProcessError {}

/// Handles one claimed event.
///
/// Processing is **at-least-once**: an event can be delivered again if a
/// worker crashes, or loses its lease, after `process` returns but before the
/// completion commits. Implementations with external side effects must
/// therefore be idempotent, for example by keying effects on
/// [`Event::id`](pulsestream_core::event::Event).
///
/// This is a trait so tests can substitute processors that block on demand
/// and record concurrency. The runtime, not the processor, enforces the
/// concurrency bound.
pub trait EventProcessor: Send + Sync + 'static {
    fn process(&self, event: &Event) -> impl Future<Output = Result<(), ProcessError>> + Send;
}

/// The default processor. It performs no business action; completion is
/// recorded as the durable `PROCESSED` state.
#[derive(Debug, Default, Clone, Copy)]
pub struct AcknowledgeProcessor;

impl EventProcessor for AcknowledgeProcessor {
    async fn process(&self, _event: &Event) -> Result<(), ProcessError> {
        Ok(())
    }
}
