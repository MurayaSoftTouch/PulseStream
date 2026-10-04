//! Retry scheduling policy (ADR-008).
//!
//! # Attempt accounting
//!
//! `delivery_attempts` is the number of times an event has been claimed into
//! `PROCESSING`. The first claim is attempt 1. Admission replays and failed
//! claim transactions never change it. With `max_delivery_attempts = 5`, an
//! event is processed at most five times and is then dead-lettered.
//!
//! # Backoff
//!
//! After a retryable failure on attempt `n` (with `n < max_delivery_attempts`)
//! the event becomes eligible again after
//!
//! ```text
//! capped = min(max_delay, base_delay * 2^(n - 1))      (overflow-safe)
//! delay  = capped * jitter(event_id, n),  jitter in [0.80, 1.00]
//! ```
//!
//! The jitter is **deterministic**: an FNV-1a hash of the event ID and the
//! attempt number, so retries of different events spread out while tests stay
//! reproducible. It only scales the delay down, so `max_delay` is a hard
//! ceiling and retries still spread out once the exponential reaches the cap.

use std::time::Duration;

use thiserror::Error;

use crate::event::EventId;

/// Lowest jitter factor, in thousandths. The highest is 1000 (no reduction).
pub const JITTER_MIN_PERMILLE: u64 = 800;
const JITTER_SPAN_PERMILLE: u64 = 1000 - JITTER_MIN_PERMILLE;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RetryPolicyError {
    #[error("max delivery attempts must be at least 1")]
    ZeroAttempts,
    #[error("retry base delay must be greater than zero")]
    ZeroBaseDelay,
    #[error("retry max delay ({max:?}) must not be less than the base delay ({base:?})")]
    MaxBelowBase { base: Duration, max: Duration },
}

/// What to do with an event whose processing failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Make the event `PENDING` again, eligible after this delay.
    RetryAfter(Duration),
    /// Retry budget exhausted: dead-letter the event.
    Exhausted,
}

/// Validated retry limits. Construct with [`RetryPolicy::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    max_delivery_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    pub fn new(
        max_delivery_attempts: u32,
        base_delay: Duration,
        max_delay: Duration,
    ) -> Result<Self, RetryPolicyError> {
        if max_delivery_attempts == 0 {
            return Err(RetryPolicyError::ZeroAttempts);
        }
        if base_delay.is_zero() {
            return Err(RetryPolicyError::ZeroBaseDelay);
        }
        if max_delay < base_delay {
            return Err(RetryPolicyError::MaxBelowBase {
                base: base_delay,
                max: max_delay,
            });
        }
        Ok(Self {
            max_delivery_attempts,
            base_delay,
            max_delay,
        })
    }

    pub fn max_delivery_attempts(&self) -> u32 {
        self.max_delivery_attempts
    }

    pub fn base_delay(&self) -> Duration {
        self.base_delay
    }

    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    /// The decision after a **retryable** failure on delivery `attempt`.
    /// Permanent failures are dead-lettered regardless of the remaining budget.
    pub fn after_retryable_failure(&self, event_id: EventId, attempt: u32) -> RetryDecision {
        if attempt < self.max_delivery_attempts {
            RetryDecision::RetryAfter(self.delay(event_id, attempt))
        } else {
            RetryDecision::Exhausted
        }
    }

    /// The capped exponential delay before jitter: `min(max, base * 2^(attempt - 1))`.
    /// Attempt 0 is treated as attempt 1.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let doubling = 1u32.checked_shl(attempt.saturating_sub(1));
        doubling
            .and_then(|factor| self.base_delay.checked_mul(factor))
            .map_or(self.max_delay, |delay| delay.min(self.max_delay))
    }

    /// [`backoff`](Self::backoff) scaled by the deterministic jitter factor.
    /// Always at least 80% of the backoff, and never above `max_delay`.
    pub fn delay(&self, event_id: EventId, attempt: u32) -> Duration {
        let permille = jitter_permille(event_id, attempt);
        // backoff <= max_delay, whose range keeps this product far below u128::MAX.
        let micros = self.backoff(attempt).as_micros() * u128::from(permille) / 1000;
        Duration::from_micros(u64::try_from(micros).unwrap_or(u64::MAX))
    }
}

/// Jitter factor in thousandths, in `[JITTER_MIN_PERMILLE, 1000]`.
pub fn jitter_permille(event_id: EventId, attempt: u32) -> u64 {
    // FNV-1a (64-bit): stable across processes, platforms, and Rust versions.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in event_id
        .as_uuid()
        .as_bytes()
        .iter()
        .chain(attempt.to_le_bytes().iter())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    JITTER_MIN_PERMILLE + hash % (JITTER_SPAN_PERMILLE + 1)
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    fn policy(max_attempts: u32, base_ms: u64, max_ms: u64) -> RetryPolicy {
        RetryPolicy::new(max_attempts, ms(base_ms), ms(max_ms)).unwrap()
    }

    #[test]
    fn rejects_invalid_policies() {
        assert_eq!(
            RetryPolicy::new(0, ms(1), ms(1)),
            Err(RetryPolicyError::ZeroAttempts)
        );
        assert_eq!(
            RetryPolicy::new(1, Duration::ZERO, ms(1)),
            Err(RetryPolicyError::ZeroBaseDelay)
        );
        assert_eq!(
            RetryPolicy::new(1, ms(200), ms(100)),
            Err(RetryPolicyError::MaxBelowBase {
                base: ms(200),
                max: ms(100)
            })
        );
        assert!(RetryPolicy::new(1, ms(100), ms(100)).is_ok());
    }

    #[test]
    fn backoff_doubles_from_the_base_and_is_capped() {
        let p = policy(10, 100, 400);
        let series: Vec<_> = (1..=6).map(|n| p.backoff(n).as_millis()).collect();
        assert_eq!(series, [100, 200, 400, 400, 400, 400]);
        assert_eq!(p.backoff(0), ms(100), "attempt 0 is treated as attempt 1");

        let defaults = policy(5, 1_000, 60_000);
        let series: Vec<_> = (1..=8).map(|n| defaults.backoff(n).as_millis()).collect();
        assert_eq!(
            series,
            [1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000]
        );
    }

    #[test]
    fn backoff_never_overflows() {
        let p = policy(100, 600_000, 86_400_000);
        for attempt in [31, 32, 33, 63, 64, 65, 1_000, u32::MAX] {
            assert_eq!(p.backoff(attempt), ms(86_400_000), "attempt {attempt}");
            assert!(p.delay(EventId::new(), attempt) <= ms(86_400_000));
        }
    }

    #[test]
    fn jitter_is_deterministic_and_bounded() {
        let p = policy(10, 100, 400);
        let id = EventId::from_uuid(Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef));
        for attempt in 1..=6 {
            let delay = p.delay(id, attempt);
            assert_eq!(delay, p.delay(id, attempt), "same input, same delay");
            let backoff = p.backoff(attempt);
            assert!(delay <= backoff, "{delay:?} > {backoff:?}");
            assert!(
                delay * 1000 >= backoff * 800,
                "{delay:?} < 80% of {backoff:?}"
            );
        }
        // Pinned against an independent Python FNV-1a computation, so an
        // accidental change to the hash is caught.
        let pinned: Vec<_> = (1..=6).map(|n| jitter_permille(id, n)).collect();
        assert_eq!(pinned, [939, 928, 965, 950, 987, 976]);
        assert_eq!(p.delay(id, 1), Duration::from_micros(93_900));
    }

    #[test]
    fn jitter_spreads_retries_across_events() {
        let p = policy(10, 1_000, 60_000);
        let delays: std::collections::HashSet<_> = (0..100u128)
            .map(|n| p.delay(EventId::from_uuid(Uuid::from_u128(n)), 7))
            .collect();
        // All of these are at the 60 s cap before jitter.
        assert!(delays.len() > 50, "only {} distinct delays", delays.len());
    }

    #[test]
    fn smallest_delay_is_still_positive() {
        let p = policy(2, 1, 1);
        for n in 0..50u128 {
            assert!(
                p.delay(EventId::from_uuid(Uuid::from_u128(n)), 1) >= Duration::from_micros(800)
            );
        }
    }

    #[test]
    fn retries_until_the_attempt_budget_is_spent() {
        let p = policy(3, 100, 400);
        let id = EventId::new();
        assert_eq!(
            p.after_retryable_failure(id, 1),
            RetryDecision::RetryAfter(p.delay(id, 1))
        );
        assert_eq!(
            p.after_retryable_failure(id, 2),
            RetryDecision::RetryAfter(p.delay(id, 2))
        );
        assert_eq!(p.after_retryable_failure(id, 3), RetryDecision::Exhausted);
        assert_eq!(p.after_retryable_failure(id, 4), RetryDecision::Exhausted);
        assert_eq!(
            policy(1, 100, 400).after_retryable_failure(id, 1),
            RetryDecision::Exhausted,
            "max attempts 1 means no retries"
        );
    }
}
