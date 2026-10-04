//! PulseStream event processing.
//!
//! The [`runtime`] claims durably accepted events from PostgreSQL
//! (`pulsestream-store`) and processes them with bounded concurrency and
//! lease-based crash recovery (ADR-007). Processing is at-least-once.

pub mod processor;
pub mod runtime;

#[cfg(any(test, feature = "test-util"))]
pub mod testing;
