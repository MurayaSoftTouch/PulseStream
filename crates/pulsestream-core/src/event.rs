//! The validated event domain model.
//!
//! HTTP request shapes live in `pulsestream-api`; they are converted into these
//! types only after validation, so everything downstream of admission can rely
//! on the invariants enforced here.

use std::fmt;
use std::time::SystemTime;

use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

/// Maximum length of [`EventSource`], in Unicode scalar values.
pub const MAX_SOURCE_CHARS: usize = 100;
/// Maximum length of [`EventType`], in Unicode scalar values.
pub const MAX_EVENT_TYPE_CHARS: usize = 150;

/// A reason an event was rejected. Messages are safe to return to clients.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidationError {
    #[error("{field} must not be empty")]
    Empty { field: &'static str },

    #[error("{field} must be at most {max} characters")]
    TooLong { field: &'static str, max: usize },

    #[error("{field} must not contain control characters")]
    ControlCharacter { field: &'static str },

    #[error("payload must not be null")]
    NullPayload,
}

/// Server-generated identity of an admitted event (UUID v4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventId(Uuid);

impl EventId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Parses the canonical hyphenated UUID form used in API paths.
    pub fn parse(value: &str) -> Option<Self> {
        Uuid::try_parse(value).ok().map(Self)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Identity of one worker process instance (UUID v4, generated at startup).
/// Stored as the owner of the events it claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId(Uuid);

impl WorkerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for WorkerId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Durable lifecycle state of an event (ADR-007, ADR-008).
///
/// ```text
/// PENDING (available_at <= now) ──claim──▶ PROCESSING ──success (owner only)──▶ PROCESSED
///    ▲                                       │
///    └──── retryable failure, attempts left ─┤
///          (available_at = now + backoff)    ├── permanent failure ───────▶ DEAD_LETTERED
///                                            ├── retryable, budget spent ─▶ DEAD_LETTERED
///                                            └── lease expires ──▶ reclaimable by any worker
///                                                (on the final attempt ──▶ DEAD_LETTERED)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventStatus {
    Pending,
    Processing,
    Processed,
    /// Terminal: never claimed again. The row, its content, its attempt count,
    /// and its final failure metadata are kept.
    DeadLettered,
}

impl EventStatus {
    /// Value stored in the `events.status` column.
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Processing => "PROCESSING",
            Self::Processed => "PROCESSED",
            Self::DeadLettered => "DEAD_LETTERED",
        }
    }

    pub fn from_db_str(value: &str) -> Option<Self> {
        match value {
            "PENDING" => Some(Self::Pending),
            "PROCESSING" => Some(Self::Processing),
            "PROCESSED" => Some(Self::Processed),
            "DEAD_LETTERED" => Some(Self::DeadLettered),
            _ => None,
        }
    }

    /// Stable, lowercase value used in the public API.
    pub const fn as_api_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Processed => "processed",
            Self::DeadLettered => "dead_lettered",
        }
    }
}

/// Validates a short identifier-like field. Control characters are rejected
/// because these values are written to logs.
fn validate_label(field: &'static str, value: &str, max: usize) -> Result<(), ValidationError> {
    if value.trim().is_empty() {
        return Err(ValidationError::Empty { field });
    }
    if value.chars().count() > max {
        return Err(ValidationError::TooLong { field, max });
    }
    if value.chars().any(char::is_control) {
        return Err(ValidationError::ControlCharacter { field });
    }
    Ok(())
}

/// The producer that emitted an event, e.g. `orders-api`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSource(String);

impl EventSource {
    pub fn parse(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_label("source", &value, MAX_SOURCE_CHARS)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The kind of event, e.g. `order.created`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventType(String);

impl EventType {
    pub fn parse(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_label("event_type", &value, MAX_EVENT_TYPE_CHARS)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A validated event, assigned an identity at admission time.
///
/// `payload` is opaque to PulseStream and is never logged.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub id: EventId,
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: Value,
    /// Time the event was accepted. For stored events this is the
    /// database's commit-time clock (`accepted_at` column).
    pub accepted_at: SystemTime,
}

impl Event {
    /// Validates raw fields and assigns a fresh [`EventId`].
    pub fn new(
        source: impl Into<String>,
        event_type: impl Into<String>,
        payload: Value,
    ) -> Result<Self, ValidationError> {
        let source = EventSource::parse(source)?;
        let event_type = EventType::parse(event_type)?;
        if payload.is_null() {
            return Err(ValidationError::NullPayload);
        }
        Ok(Self {
            id: EventId::new(),
            source,
            event_type,
            payload,
            accepted_at: SystemTime::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_valid_event_and_assigns_unique_ids() {
        let a = Event::new("orders-api", "order.created", json!({"order_id": "1"})).unwrap();
        let b = Event::new("orders-api", "order.created", json!({"order_id": "1"})).unwrap();
        assert_eq!(a.source.as_str(), "orders-api");
        assert_eq!(a.event_type.as_str(), "order.created");
        assert_ne!(a.id, b.id);
        assert_eq!(a.id.to_string().len(), 36);
    }

    #[test]
    fn rejects_empty_or_whitespace_labels() {
        assert_eq!(
            Event::new("", "t", json!({})).unwrap_err(),
            ValidationError::Empty { field: "source" }
        );
        assert_eq!(
            Event::new("s", "   ", json!({})).unwrap_err(),
            ValidationError::Empty {
                field: "event_type"
            }
        );
    }

    #[test]
    fn enforces_length_limits_in_characters() {
        let at_limit = "é".repeat(MAX_SOURCE_CHARS); // multi-byte: counts chars, not bytes
        assert!(EventSource::parse(at_limit).is_ok());
        assert_eq!(
            EventSource::parse("a".repeat(MAX_SOURCE_CHARS + 1)).unwrap_err(),
            ValidationError::TooLong {
                field: "source",
                max: MAX_SOURCE_CHARS
            }
        );
        assert!(EventType::parse("t".repeat(MAX_EVENT_TYPE_CHARS)).is_ok());
        assert_eq!(
            EventType::parse("t".repeat(MAX_EVENT_TYPE_CHARS + 1)).unwrap_err(),
            ValidationError::TooLong {
                field: "event_type",
                max: MAX_EVENT_TYPE_CHARS
            }
        );
    }

    #[test]
    fn event_ids_parse_from_their_display_form() {
        let id = EventId::new();
        assert_eq!(EventId::parse(&id.to_string()), Some(id));
        assert_eq!(EventId::parse("not-a-uuid"), None);
        assert_eq!(EventId::parse(""), None);
    }

    #[test]
    fn statuses_round_trip_and_have_stable_api_names() {
        for status in [
            EventStatus::Pending,
            EventStatus::Processing,
            EventStatus::Processed,
            EventStatus::DeadLettered,
        ] {
            assert_eq!(EventStatus::from_db_str(status.as_db_str()), Some(status));
        }
        assert_eq!(EventStatus::Pending.as_api_str(), "pending");
        assert_eq!(EventStatus::Processing.as_api_str(), "processing");
        assert_eq!(EventStatus::Processed.as_api_str(), "processed");
        assert_eq!(EventStatus::DeadLettered.as_db_str(), "DEAD_LETTERED");
        assert_eq!(EventStatus::DeadLettered.as_api_str(), "dead_lettered");
        assert_eq!(EventStatus::from_db_str("RETRYING"), None);
    }

    #[test]
    fn rejects_control_characters_and_null_payload() {
        assert_eq!(
            EventType::parse("order.created\nforged-log-line").unwrap_err(),
            ValidationError::ControlCharacter {
                field: "event_type"
            }
        );
        assert_eq!(
            Event::new("s", "t", Value::Null).unwrap_err(),
            ValidationError::NullPayload
        );
        // Any non-null JSON value is an acceptable opaque payload.
        assert!(Event::new("s", "t", json!("text")).is_ok());
    }
}
