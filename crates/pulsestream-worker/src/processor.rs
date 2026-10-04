//! The event-processing extension point.

use std::future::Future;

use pulsestream_core::event::Event;
use pulsestream_core::failure::{FailureCode, FailureDetail};

/// A classified processing failure (ADR-008).
///
/// The variant, not the message, decides what happens next:
///
/// - [`Retryable`](Self::Retryable): the event is scheduled again with
///   backoff, or dead-lettered if this was its final permitted attempt.
/// - [`Permanent`](Self::Permanent): the event is dead-lettered immediately,
///   whatever retry budget remains.
///
/// The code and message are persisted. Keep them free of payload data,
/// credentials, and stack traces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessError {
    Retryable(FailureDetail),
    Permanent(FailureDetail),
}

impl ProcessError {
    /// A failure that may succeed on a later attempt, such as a timeout.
    pub fn retryable(code: FailureCode, message: impl Into<String>) -> Self {
        Self::Retryable(FailureDetail::new(code, message))
    }

    /// A failure that no retry can fix, such as an invalid destination.
    pub fn permanent(code: FailureCode, message: impl Into<String>) -> Self {
        Self::Permanent(FailureDetail::new(code, message))
    }

    pub fn detail(&self) -> &FailureDetail {
        match self {
            Self::Retryable(detail) | Self::Permanent(detail) => detail,
        }
    }

    /// `"retryable"` or `"permanent"`, for logs.
    pub fn class(&self) -> &'static str {
        match self {
            Self::Retryable(_) => "retryable",
            Self::Permanent(_) => "permanent",
        }
    }
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let detail = self.detail();
        write!(
            f,
            "{} failure {}: {}",
            self.class(),
            detail.code,
            detail.message
        )
    }
}

impl std::error::Error for ProcessError {}

/// Handles one claimed event.
///
/// Processing is **at-least-once**: an event can be delivered again if a
/// worker crashes, or loses its lease, after `process` returns but before the
/// outcome commits, and again after every retryable failure. Implementations
/// with external side effects must therefore be idempotent, for example by
/// keying effects on [`Event::id`](pulsestream_core::event::Event).
///
/// A panic is treated as a retryable `PROCESSOR_PANICKED` failure.
///
/// This is a trait so tests can substitute processors that block on demand,
/// record concurrency, or fail on a script. The runtime, not the processor,
/// enforces the concurrency bound and the retry policy.
pub trait EventProcessor: Send + Sync + 'static {
    fn process(&self, event: &Event) -> impl Future<Output = Result<(), ProcessError>> + Send;
}

/// The default processor. It performs no business action and never fails;
/// completion is recorded as the durable `PROCESSED` state.
#[derive(Debug, Default, Clone, Copy)]
pub struct AcknowledgeProcessor;

impl EventProcessor for AcknowledgeProcessor {
    async fn process(&self, _event: &Event) -> Result<(), ProcessError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_is_explicit_and_displayed() {
        let code = FailureCode::new("UPSTREAM_TIMEOUT").unwrap();
        let retryable = ProcessError::retryable(code.clone(), "took longer than 5s");
        assert!(matches!(retryable, ProcessError::Retryable(_)));
        assert_eq!(
            retryable.to_string(),
            "retryable failure UPSTREAM_TIMEOUT: took longer than 5s"
        );

        let permanent =
            ProcessError::permanent(FailureCode::new("INVALID_DESTINATION").unwrap(), "no route");
        assert_eq!(permanent.class(), "permanent");
        assert_eq!(permanent.detail().code.as_str(), "INVALID_DESTINATION");
    }
}
