//! Stable API error responses: `{"code": "...", "message": "..."}`.
//!
//! Messages never include database errors, hostnames, connection strings, or
//! framework internals.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use pulsestream_core::idempotency::IdempotencyKeyError;
use serde::Serialize;

/// Seconds a client should wait before retrying after `PERSISTENCE_UNAVAILABLE`.
pub const RETRY_AFTER_SECS: u32 = 1;

// Keep the PAYLOAD_TOO_LARGE message in sync with the enforced limit.
const _: () = assert!(crate::events::MAX_BODY_BYTES == 64 * 1024);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    /// 400: malformed JSON, wrong shape, or failed validation.
    InvalidEvent(String),
    /// 413: request body exceeds the configured limit.
    PayloadTooLarge,
    /// 415: request is not `application/json`.
    UnsupportedMediaType,
    /// 400: no `Idempotency-Key` header.
    IdempotencyKeyRequired,
    /// 400: `Idempotency-Key` fails validation.
    IdempotencyKeyInvalid,
    /// 409: the scoped key was already used for a different request.
    IdempotencyConflict,
    /// 400: the path is not a valid event ID.
    InvalidEventId,
    /// 404: no event with this ID.
    EventNotFound,
    /// 503: the database cannot durably accept or serve events right now.
    PersistenceUnavailable,
    /// 500: unexpected server-side failure. Details are logged, not returned.
    Internal,
}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    code: &'static str,
    message: &'a str,
}

impl ApiError {
    /// Maps extractor rejections to stable errors without exposing
    /// framework or deserializer internals.
    pub fn from_json_rejection(rejection: &JsonRejection) -> Self {
        match rejection {
            JsonRejection::MissingJsonContentType(_) => Self::UnsupportedMediaType,
            JsonRejection::BytesRejection(_)
                if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE =>
            {
                Self::PayloadTooLarge
            }
            JsonRejection::JsonSyntaxError(_) => {
                Self::InvalidEvent("request body is not valid JSON".to_owned())
            }
            JsonRejection::JsonDataError(_) => Self::InvalidEvent(
                "request body must be a JSON object with string fields `source` and \
                 `event_type`, a `payload`, and no other fields"
                    .to_owned(),
            ),
            _ => Self::InvalidEvent("request body could not be read".to_owned()),
        }
    }

    fn parts(&self) -> (StatusCode, &'static str, &str) {
        match self {
            Self::InvalidEvent(message) => (StatusCode::BAD_REQUEST, "INVALID_EVENT", message),
            Self::PayloadTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "PAYLOAD_TOO_LARGE",
                "request body exceeds the 64 KiB limit",
            ),
            Self::UnsupportedMediaType => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "UNSUPPORTED_MEDIA_TYPE",
                "Content-Type must be application/json",
            ),
            Self::IdempotencyKeyRequired => (
                StatusCode::BAD_REQUEST,
                "IDEMPOTENCY_KEY_REQUIRED",
                "Idempotency-Key header is required",
            ),
            Self::IdempotencyKeyInvalid => (
                StatusCode::BAD_REQUEST,
                "IDEMPOTENCY_KEY_INVALID",
                "Idempotency-Key must be 1-128 characters of visible ASCII (no spaces)",
            ),
            Self::IdempotencyConflict => (
                StatusCode::CONFLICT,
                "IDEMPOTENCY_CONFLICT",
                "Idempotency-Key was already used for a different event from this source",
            ),
            Self::InvalidEventId => (
                StatusCode::BAD_REQUEST,
                "INVALID_EVENT_ID",
                "event_id must be a UUID",
            ),
            Self::EventNotFound => (StatusCode::NOT_FOUND, "EVENT_NOT_FOUND", "event not found"),
            Self::PersistenceUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "PERSISTENCE_UNAVAILABLE",
                "event persistence is temporarily unavailable",
            ),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "internal server error",
            ),
        }
    }
}

impl From<IdempotencyKeyError> for ApiError {
    fn from(err: IdempotencyKeyError) -> Self {
        match err {
            IdempotencyKeyError::Missing => Self::IdempotencyKeyRequired,
            IdempotencyKeyError::Invalid => Self::IdempotencyKeyInvalid,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = self.parts();
        let mut response = (status, Json(ErrorBody { code, message })).into_response();
        if self == Self::PersistenceUnavailable {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(RETRY_AFTER_SECS));
        }
        response
    }
}
