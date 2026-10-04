//! Foundational PulseStream concepts shared by the API and worker processes.
//!
//! This crate deliberately contains no HTTP, runtime, or database code. It owns
//! service identity, environment-based configuration parsing, the validated
//! event domain model, and idempotency keys and request fingerprints.

pub mod config;
pub mod event;
pub mod idempotency;

use std::fmt;

/// Identifies a PulseStream process in logs and health responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceName {
    Api,
    Worker,
}

impl ServiceName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "pulsestream-api",
            Self::Worker => "pulsestream-worker",
        }
    }
}

impl fmt::Display for ServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::ServiceName;

    #[test]
    fn service_names_are_stable() {
        // These strings appear in health responses and logs; changing them is a
        // breaking change for dashboards and probes.
        assert_eq!(ServiceName::Api.to_string(), "pulsestream-api");
        assert_eq!(ServiceName::Worker.to_string(), "pulsestream-worker");
    }
}
