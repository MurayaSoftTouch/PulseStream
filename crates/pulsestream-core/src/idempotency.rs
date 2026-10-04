//! Producer idempotency: validated keys and deterministic request fingerprints.
//!
//! A key is scoped by event source: `(source, idempotency_key)` identifies one
//! logical producer request. The fingerprint decides whether a repeated request
//! with the same scoped key is an exact replay or a conflicting reuse.

use std::fmt;

use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::event::{EventSource, EventType};

/// Maximum idempotency key length, in characters.
pub const MAX_IDEMPOTENCY_KEY_CHARS: usize = 128;

/// Domain separator and version of the fingerprint encoding. Changing the
/// encoding requires a new version, because stored fingerprints must remain
/// comparable.
pub const FINGERPRINT_VERSION: &[u8] = b"pulsestream.request-fingerprint.v1";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdempotencyKeyError {
    #[error("Idempotency-Key header is required")]
    Missing,

    #[error(
        "Idempotency-Key must be 1-128 characters of visible ASCII (letters, digits, and punctuation; no spaces)"
    )]
    Invalid,
}

/// A producer-supplied idempotency key: 1–128 visible ASCII characters
/// (`0x21`–`0x7E`). This covers UUIDs, ULIDs, and base64url tokens, and keeps
/// keys safe to carry in HTTP headers.
///
/// Keys may be sensitive correlation identifiers, so `Debug` redacts them.
#[derive(Clone, PartialEq, Eq)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn parse(value: &str) -> Result<Self, IdempotencyKeyError> {
        let valid = !value.is_empty()
            && value.len() <= MAX_IDEMPOTENCY_KEY_CHARS
            && value.bytes().all(|b| b.is_ascii_graphic());
        if valid {
            Ok(Self(value.to_owned()))
        } else {
            Err(IdempotencyKeyError::Invalid)
        }
    }

    /// The raw key, for storage only. Do not log it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IdempotencyKey(<redacted>)")
    }
}

/// SHA-256 over a canonical encoding of the semantically relevant request
/// fields: the fingerprint version, `source`, `event_type`, and `payload`.
///
/// Canonical encoding and equality semantics:
///
/// - Object keys are sorted recursively (by Unicode code point, via Rust
///   `String` ordering), so `{"a":1,"b":2}` and `{"b":2,"a":1}` match.
/// - Array order is preserved and significant.
/// - Strings are compared after JSON unescaping: `"A"` equals `"A"`.
/// - Numbers are compared by serde_json's parsed representation: `1` and
///   `1.0` differ, and `1e2` equals `100.0`.
/// - Whitespace is insignificant. For a duplicated object key, the last
///   value wins (serde_json parsing behavior).
///
/// The request ID, timestamps, headers, and the generated event ID are
/// deliberately excluded.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct RequestFingerprint([u8; 32]);

impl RequestFingerprint {
    pub fn compute(source: &EventSource, event_type: &EventType, payload: &Value) -> Self {
        let mut hasher = Sha256::new();
        // Length-prefixed fields prevent ambiguity between field boundaries.
        for part in [
            FINGERPRINT_VERSION,
            source.as_str().as_bytes(),
            event_type.as_str().as_bytes(),
        ] {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part);
        }
        let mut canonical = Vec::new();
        write_canonical(payload, &mut canonical);
        hasher.update((canonical.len() as u64).to_be_bytes());
        hasher.update(&canonical);
        Self(hasher.finalize().into())
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for RequestFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RequestFingerprint(")?;
        for byte in &self.0[..4] {
            write!(f, "{byte:02x}")?;
        }
        f.write_str("…)")
    }
}

/// Writes `value` as JSON with recursively sorted object keys. This does not
/// depend on serde_json's map ordering, which a `preserve_order` feature
/// anywhere in the dependency graph could change.
fn write_canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_unstable_by_key(|&(key, _)| key);
            out.push(b'{');
            for (i, (key, value)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_scalar(&Value::String(key.clone()), out);
                out.push(b':');
                write_canonical(value, out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        scalar => write_scalar(scalar, out),
    }
}

fn write_scalar(value: &Value, out: &mut Vec<u8>) {
    serde_json::to_writer(out, value).expect("serializing a JSON scalar to memory cannot fail");
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fp(source: &str, event_type: &str, payload: Value) -> RequestFingerprint {
        RequestFingerprint::compute(
            &EventSource::parse(source).unwrap(),
            &EventType::parse(event_type).unwrap(),
            &payload,
        )
    }

    #[test]
    fn accepts_visible_ascii_keys_up_to_128_chars() {
        for key in [
            "a",
            "order-12345",
            "4f6c1e9d-ec77-4565-b736-974f6c1e9de3",
            "ab_C.d~e:f/g+h=",
            &"k".repeat(128),
        ] {
            assert!(IdempotencyKey::parse(key).is_ok(), "{key:?}");
        }
    }

    #[test]
    fn rejects_empty_long_whitespace_and_non_ascii_keys() {
        for key in [
            "",
            " ",
            "has space",
            "tab\t",
            "new\nline",
            "clé",
            &"k".repeat(129),
        ] {
            assert_eq!(
                IdempotencyKey::parse(key).unwrap_err(),
                IdempotencyKeyError::Invalid,
                "{key:?}"
            );
        }
    }

    #[test]
    fn keys_are_redacted_in_debug_output() {
        let key = IdempotencyKey::parse("secret-correlation-id").unwrap();
        assert!(!format!("{key:?}").contains("secret"));
    }

    #[test]
    fn object_key_order_does_not_matter_recursively() {
        let a = fp("s", "t", json!({"a": 1, "b": {"x": [1, 2], "y": null}}));
        let b = fp("s", "t", json!({"b": {"y": null, "x": [1, 2]}, "a": 1}));
        assert_eq!(a, b);
    }

    #[test]
    fn array_order_and_values_matter() {
        assert_ne!(fp("s", "t", json!([1, 2])), fp("s", "t", json!([2, 1])));
        assert_ne!(fp("s", "t", json!({"a": 1})), fp("s", "t", json!({"a": 2})));
        assert_ne!(
            fp("s", "t", json!({"a": 1})),
            fp("s", "t", json!({"a": 1.0}))
        );
        assert_ne!(
            fp("s", "t", json!({"a": "1"})),
            fp("s", "t", json!({"a": 1}))
        );
    }

    #[test]
    fn source_and_event_type_are_part_of_the_fingerprint() {
        let base = fp("orders", "order.created", json!({}));
        assert_ne!(base, fp("billing", "order.created", json!({})));
        assert_ne!(base, fp("orders", "order.updated", json!({})));
        // Length prefixes keep field boundaries unambiguous.
        assert_ne!(fp("ab", "c", json!({})), fp("a", "bc", json!({})));
    }

    #[test]
    fn fingerprint_is_stable_across_releases() {
        // Pinned value: fingerprints are persisted, so the encoding must never
        // change silently. A change here requires bumping FINGERPRINT_VERSION.
        let value = fp("orders-api", "order.created", json!({"order_id": "12345"}));
        let hex: String = value
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(hex, PINNED_FINGERPRINT);
    }

    // Independently derived with Python: sha256 over length-prefixed version,
    // source, event_type, and canonical payload bytes.
    const PINNED_FINGERPRINT: &str =
        "87054adceb2813f70ce604e705e6dc22ead8b6e4dcf4513de5eafd3e3c16a73a";
}
