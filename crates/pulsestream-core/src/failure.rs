//! Structured processing failures (ADR-008).
//!
//! A processor reports *why* an attempt failed with a stable, machine-readable
//! [`FailureCode`] and a short, bounded [`FailureMessage`]. Retryability is
//! decided by which variant the processor returns, never by parsing text.
//! Both values are persisted as `last_failure_code` and
//! `last_failure_message`, so they must never carry stack traces, payload
//! copies, or credentials.

use std::fmt;

use thiserror::Error;

/// Maximum length of a [`FailureCode`], in characters.
pub const MAX_FAILURE_CODE_CHARS: usize = 64;
/// Maximum stored length of a [`FailureMessage`], in characters. Longer
/// messages are truncated.
pub const MAX_FAILURE_MESSAGE_CHARS: usize = 1024;

/// Recorded when an event's final permitted attempt lost its lease without
/// reporting a result (for example, the worker crashed). Reserved for the
/// runtime.
pub const LEASE_EXPIRED: &str = "LEASE_EXPIRED";
/// Recorded when a processor panicked. Treated as a retryable failure.
/// Reserved for the runtime.
pub const PROCESSOR_PANICKED: &str = "PROCESSOR_PANICKED";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error(
    "failure code must be 1-{MAX_FAILURE_CODE_CHARS} characters of A-Z, 0-9, and `_`, starting with a letter"
)]
pub struct InvalidFailureCode;

/// A stable, machine-readable failure code such as `UPSTREAM_TIMEOUT`.
///
/// Upper-case ASCII letters, digits, and underscores, starting with a letter,
/// at most [`MAX_FAILURE_CODE_CHARS`]. Codes are persisted, so they are part of
/// the operational contract: do not use Rust type names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FailureCode(String);

impl FailureCode {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidFailureCode> {
        let value = value.into();
        let mut bytes = value.bytes();
        let valid = value.len() <= MAX_FAILURE_CODE_CHARS
            && bytes.next().is_some_and(|b| b.is_ascii_uppercase())
            && bytes.all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
        if valid {
            Ok(Self(value))
        } else {
            Err(InvalidFailureCode)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FailureCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A short, human-readable failure description, safe to persist.
///
/// Construction never fails: control characters (which could forge log lines)
/// become spaces, and the text is truncated to [`MAX_FAILURE_MESSAGE_CHARS`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureMessage(String);

impl FailureMessage {
    pub fn new(value: impl Into<String>) -> Self {
        let value = value.into();
        Self(
            value
                .chars()
                .take(MAX_FAILURE_MESSAGE_CHARS)
                .map(|c| if c.is_control() { ' ' } else { c })
                .collect(),
        )
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FailureMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The persisted description of one failed attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureDetail {
    pub code: FailureCode,
    pub message: FailureMessage,
}

impl FailureDetail {
    pub fn new(code: FailureCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: FailureMessage::new(message),
        }
    }

    /// The runtime-reserved [`LEASE_EXPIRED`] failure.
    pub fn lease_expired() -> Self {
        Self::reserved(
            LEASE_EXPIRED,
            "lease expired on the final delivery attempt without a recorded result",
        )
    }

    /// The runtime-reserved [`PROCESSOR_PANICKED`] failure. The panic payload
    /// is deliberately not recorded: it may contain arbitrary data.
    pub fn processor_panicked() -> Self {
        Self::reserved(PROCESSOR_PANICKED, "processor panicked")
    }

    fn reserved(code: &'static str, message: &'static str) -> Self {
        Self::new(
            FailureCode::new(code).expect("reserved codes are valid"),
            message,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_stable_upper_snake_case_codes() {
        for code in ["UPSTREAM_TIMEOUT", "INVALID_DESTINATION", "E1", "A"] {
            assert_eq!(FailureCode::new(code).unwrap().as_str(), code);
        }
        assert!(FailureCode::new("A".repeat(MAX_FAILURE_CODE_CHARS)).is_ok());
    }

    #[test]
    fn rejects_codes_that_are_not_stable_identifiers() {
        for code in [
            "",
            "upstream_timeout",
            "_LEADING",
            "1LEADING",
            "HAS SPACE",
            "HAS-DASH",
            "MyError",
            "ÜMLAUT",
        ] {
            assert_eq!(FailureCode::new(code), Err(InvalidFailureCode), "{code:?}");
        }
        assert!(FailureCode::new("A".repeat(MAX_FAILURE_CODE_CHARS + 1)).is_err());
    }

    #[test]
    fn messages_are_bounded_in_characters_and_stripped_of_control_characters() {
        let long = "é".repeat(MAX_FAILURE_MESSAGE_CHARS + 10);
        let message = FailureMessage::new(long);
        assert_eq!(message.as_str().chars().count(), MAX_FAILURE_MESSAGE_CHARS);

        let forged = FailureMessage::new("timeout\nINFO forged log line\r\t!");
        assert_eq!(forged.as_str(), "timeout INFO forged log line  !");
    }

    #[test]
    fn reserved_failures_are_valid() {
        assert_eq!(FailureDetail::lease_expired().code.as_str(), LEASE_EXPIRED);
        assert_eq!(
            FailureDetail::processor_panicked().code.as_str(),
            PROCESSOR_PANICKED
        );
    }
}
