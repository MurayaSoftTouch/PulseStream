//! Environment-based configuration.
//!
//! Parsing takes a lookup function instead of reading `std::env` directly so it
//! can be tested without mutating process-global state. A variable that is set
//! but invalid is always an error; defaults apply only to unset variables.

use std::fmt;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::time::Duration;

use thiserror::Error;

use crate::retry::{RetryPolicy, RetryPolicyError};

pub const API_BIND_VAR: &str = "PULSESTREAM_API_BIND";
pub const DATABASE_URL_VAR: &str = "DATABASE_URL";
pub const DB_MAX_CONNECTIONS_VAR: &str = "PULSESTREAM_DB_MAX_CONNECTIONS";
pub const DB_ACQUIRE_TIMEOUT_MS_VAR: &str = "PULSESTREAM_DB_ACQUIRE_TIMEOUT_MS";
pub const MIGRATE_ON_START_VAR: &str = "PULSESTREAM_MIGRATE_ON_START";
pub const WORKER_CONCURRENCY_VAR: &str = "PULSESTREAM_WORKER_CONCURRENCY";
pub const POLL_INTERVAL_MS_VAR: &str = "PULSESTREAM_POLL_INTERVAL_MS";
pub const PROCESSING_LEASE_MS_VAR: &str = "PULSESTREAM_PROCESSING_LEASE_MS";
pub const SHUTDOWN_TIMEOUT_MS_VAR: &str = "PULSESTREAM_SHUTDOWN_TIMEOUT_MS";
pub const MAX_DELIVERY_ATTEMPTS_VAR: &str = "PULSESTREAM_MAX_DELIVERY_ATTEMPTS";
pub const RETRY_BASE_DELAY_MS_VAR: &str = "PULSESTREAM_RETRY_BASE_DELAY_MS";
pub const RETRY_MAX_DELAY_MS_VAR: &str = "PULSESTREAM_RETRY_MAX_DELAY_MS";

/// Loopback-only default so an unconfigured process is never exposed publicly.
pub const DEFAULT_API_BIND: &str = "127.0.0.1:8088";

pub const DEFAULT_DB_MAX_CONNECTIONS: u32 = 10;
pub const DB_MAX_CONNECTIONS_RANGE: RangeInclusive<u64> = 1..=50;
pub const DEFAULT_DB_ACQUIRE_TIMEOUT_MS: u64 = 3_000;
pub const DB_ACQUIRE_TIMEOUT_MS_RANGE: RangeInclusive<u64> = 100..=60_000;
pub const DEFAULT_WORKER_CONCURRENCY: usize = 4;
pub const WORKER_CONCURRENCY_RANGE: RangeInclusive<u64> = 1..=64;
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 250;
pub const POLL_INTERVAL_MS_RANGE: RangeInclusive<u64> = 10..=60_000;
pub const DEFAULT_PROCESSING_LEASE_MS: u64 = 30_000;
pub const PROCESSING_LEASE_MS_RANGE: RangeInclusive<u64> = 1_000..=3_600_000;
pub const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 10_000;
pub const SHUTDOWN_TIMEOUT_MS_RANGE: RangeInclusive<u64> = 1..=300_000;
pub const DEFAULT_MAX_DELIVERY_ATTEMPTS: u32 = 5;
pub const MAX_DELIVERY_ATTEMPTS_RANGE: RangeInclusive<u64> = 1..=100;
pub const DEFAULT_RETRY_BASE_DELAY_MS: u64 = 1_000;
/// Up to 10 minutes.
pub const RETRY_BASE_DELAY_MS_RANGE: RangeInclusive<u64> = 1..=600_000;
pub const DEFAULT_RETRY_MAX_DELAY_MS: u64 = 60_000;
/// Up to 24 hours.
pub const RETRY_MAX_DELAY_MS_RANGE: RangeInclusive<u64> = 1..=86_400_000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{var} must be set")]
    Missing { var: &'static str },

    #[error("{var} must be a socket address such as 127.0.0.1:8088, got {value:?}")]
    InvalidBindAddress { var: &'static str, value: String },

    #[error("{var} must not be empty when set")]
    Empty { var: &'static str },

    #[error("{var} must use the postgres:// or postgresql:// scheme")]
    UnsupportedDatabaseScheme { var: &'static str },

    #[error("{var} is missing a host")]
    MissingDatabaseHost { var: &'static str },

    #[error("{var} must be an integer between {min} and {max}, got {value:?}")]
    OutOfRange {
        var: &'static str,
        value: String,
        min: u64,
        max: u64,
    },

    #[error("{var} must be `true` or `false`, got {value:?}")]
    InvalidBool { var: &'static str, value: String },

    #[error("invalid retry policy: {0}")]
    InvalidRetryPolicy(#[from] RetryPolicyError),
}

/// A validated PostgreSQL connection URL.
///
/// The raw value may contain credentials, so neither `Debug` nor `Display`
/// ever print it.
#[derive(Clone, PartialEq, Eq)]
pub struct DatabaseUrl(String);

impl DatabaseUrl {
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        let var = DATABASE_URL_VAR;
        let value = value.trim();
        if value.is_empty() {
            return Err(ConfigError::Empty { var });
        }
        let rest = value
            .strip_prefix("postgres://")
            .or_else(|| value.strip_prefix("postgresql://"))
            .ok_or(ConfigError::UnsupportedDatabaseScheme { var })?;
        let authority = rest.split(['/', '?']).next().unwrap_or_default();
        let host = authority.rsplit('@').next().unwrap_or_default();
        if host.is_empty() || host.starts_with(':') {
            return Err(ConfigError::MissingDatabaseHost { var });
        }
        Ok(Self(value.to_owned()))
    }

    /// The raw connection string, for handing to a database driver only.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for DatabaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DatabaseUrl(<redacted>)")
    }
}

/// Reads `PULSESTREAM_API_BIND`, defaulting to [`DEFAULT_API_BIND`].
pub fn api_bind(lookup: &impl Fn(&str) -> Option<String>) -> Result<SocketAddr, ConfigError> {
    let raw = lookup(API_BIND_VAR).unwrap_or_else(|| DEFAULT_API_BIND.to_owned());
    raw.trim()
        .parse()
        .map_err(|_| ConfigError::InvalidBindAddress {
            var: API_BIND_VAR,
            value: raw,
        })
}

fn bounded_integer(
    lookup: &impl Fn(&str) -> Option<String>,
    var: &'static str,
    default: u64,
    range: RangeInclusive<u64>,
) -> Result<u64, ConfigError> {
    let Some(raw) = lookup(var) else {
        return Ok(default);
    };
    match raw.trim().parse::<u64>() {
        Ok(value) if range.contains(&value) => Ok(value),
        _ => Err(ConfigError::OutOfRange {
            var,
            value: raw,
            min: *range.start(),
            max: *range.end(),
        }),
    }
}

fn millis(
    lookup: &impl Fn(&str) -> Option<String>,
    var: &'static str,
    default: u64,
    range: RangeInclusive<u64>,
) -> Result<Duration, ConfigError> {
    bounded_integer(lookup, var, default, range).map(Duration::from_millis)
}

fn boolean(
    lookup: &impl Fn(&str) -> Option<String>,
    var: &'static str,
    default: bool,
) -> Result<bool, ConfigError> {
    match lookup(var).as_deref().map(str::trim) {
        None => Ok(default),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(other) => Err(ConfigError::InvalidBool {
            var,
            value: other.to_owned(),
        }),
    }
}

/// PostgreSQL connection settings shared by the API and worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseConfig {
    pub url: DatabaseUrl,
    /// Upper bound on pooled connections per process.
    pub max_connections: u32,
    /// How long a request waits for a pooled connection, including connecting.
    pub acquire_timeout: Duration,
    /// Apply embedded migrations at startup.
    pub migrate_on_start: bool,
}

impl DatabaseConfig {
    pub fn from_lookup(lookup: &impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let raw = lookup(DATABASE_URL_VAR).ok_or(ConfigError::Missing {
            var: DATABASE_URL_VAR,
        })?;
        Ok(Self {
            url: DatabaseUrl::parse(&raw)?,
            max_connections: bounded_integer(
                lookup,
                DB_MAX_CONNECTIONS_VAR,
                u64::from(DEFAULT_DB_MAX_CONNECTIONS),
                DB_MAX_CONNECTIONS_RANGE,
            )? as u32,
            acquire_timeout: millis(
                lookup,
                DB_ACQUIRE_TIMEOUT_MS_VAR,
                DEFAULT_DB_ACQUIRE_TIMEOUT_MS,
                DB_ACQUIRE_TIMEOUT_MS_RANGE,
            )?,
            migrate_on_start: boolean(lookup, MIGRATE_ON_START_VAR, true)?,
        })
    }

    /// Settings for a given URL with every other value at its default.
    pub fn with_url(url: DatabaseUrl) -> Self {
        Self {
            url,
            max_connections: DEFAULT_DB_MAX_CONNECTIONS,
            acquire_timeout: Duration::from_millis(DEFAULT_DB_ACQUIRE_TIMEOUT_MS),
            migrate_on_start: true,
        }
    }
}

/// Limits and timings of the worker's claim/process loop.
///
/// At most `concurrency` events are claimed and processed at once. The
/// worker never prefetches beyond its free capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRuntimeConfig {
    pub concurrency: usize,
    /// Sleep between claim attempts when no work is available.
    pub poll_interval: Duration,
    /// How long a claim is exclusively owned before another worker may reclaim it.
    pub lease: Duration,
    /// How long shutdown waits for active processing to finish.
    pub shutdown_timeout: Duration,
    /// Attempt limit and backoff for failed processing (ADR-008).
    pub retry: RetryPolicy,
}

impl WorkerRuntimeConfig {
    pub fn from_lookup(lookup: &impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        Ok(Self {
            // The range fits in usize on every supported (>= 32-bit) target.
            concurrency: bounded_integer(
                lookup,
                WORKER_CONCURRENCY_VAR,
                DEFAULT_WORKER_CONCURRENCY as u64,
                WORKER_CONCURRENCY_RANGE,
            )? as usize,
            poll_interval: millis(
                lookup,
                POLL_INTERVAL_MS_VAR,
                DEFAULT_POLL_INTERVAL_MS,
                POLL_INTERVAL_MS_RANGE,
            )?,
            lease: millis(
                lookup,
                PROCESSING_LEASE_MS_VAR,
                DEFAULT_PROCESSING_LEASE_MS,
                PROCESSING_LEASE_MS_RANGE,
            )?,
            shutdown_timeout: millis(
                lookup,
                SHUTDOWN_TIMEOUT_MS_VAR,
                DEFAULT_SHUTDOWN_TIMEOUT_MS,
                SHUTDOWN_TIMEOUT_MS_RANGE,
            )?,
            retry: retry_policy(lookup)?,
        })
    }
}

/// Reads the retry variables. Each must be in range, and the maximum delay
/// must not be below the base delay; nothing is silently adjusted.
fn retry_policy(lookup: &impl Fn(&str) -> Option<String>) -> Result<RetryPolicy, ConfigError> {
    let max_attempts = bounded_integer(
        lookup,
        MAX_DELIVERY_ATTEMPTS_VAR,
        u64::from(DEFAULT_MAX_DELIVERY_ATTEMPTS),
        MAX_DELIVERY_ATTEMPTS_RANGE,
    )? as u32;
    let base = millis(
        lookup,
        RETRY_BASE_DELAY_MS_VAR,
        DEFAULT_RETRY_BASE_DELAY_MS,
        RETRY_BASE_DELAY_MS_RANGE,
    )?;
    let max = millis(
        lookup,
        RETRY_MAX_DELAY_MS_VAR,
        DEFAULT_RETRY_MAX_DELAY_MS,
        RETRY_MAX_DELAY_MS_RANGE,
    )?;
    Ok(RetryPolicy::new(max_attempts, base, max)?)
}

/// The default retry policy: 5 attempts, 1 s base delay, 60 s maximum delay.
pub fn default_retry_policy() -> RetryPolicy {
    RetryPolicy::new(
        DEFAULT_MAX_DELIVERY_ATTEMPTS,
        Duration::from_millis(DEFAULT_RETRY_BASE_DELAY_MS),
        Duration::from_millis(DEFAULT_RETRY_MAX_DELAY_MS),
    )
    .expect("default retry policy is valid")
}

impl Default for WorkerRuntimeConfig {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_WORKER_CONCURRENCY,
            poll_interval: Duration::from_millis(DEFAULT_POLL_INTERVAL_MS),
            lease: Duration::from_millis(DEFAULT_PROCESSING_LEASE_MS),
            shutdown_timeout: Duration::from_millis(DEFAULT_SHUTDOWN_TIMEOUT_MS),
            retry: default_retry_policy(),
        }
    }
}

/// Configuration for the `pulsestream-api` process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiConfig {
    pub bind: SocketAddr,
    pub database: DatabaseConfig,
}

impl ApiConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        Ok(Self {
            bind: api_bind(&lookup)?,
            database: DatabaseConfig::from_lookup(&lookup)?,
        })
    }
}

/// Configuration for the `pulsestream-worker` process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    pub database: DatabaseConfig,
    pub runtime: WorkerRuntimeConfig,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        Ok(Self {
            database: DatabaseConfig::from_lookup(&lookup)?,
            runtime: WorkerRuntimeConfig::from_lookup(&lookup)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const URL: &str = "postgres://user:pw@127.0.0.1:55432/pulsestream";

    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |key| map.get(key).cloned()
    }

    fn api(vars: &[(&str, &str)]) -> Result<ApiConfig, ConfigError> {
        let mut all = vec![(DATABASE_URL_VAR, URL)];
        all.extend_from_slice(vars);
        ApiConfig::from_lookup(env(&all))
    }

    fn worker(vars: &[(&str, &str)]) -> Result<WorkerConfig, ConfigError> {
        let mut all = vec![(DATABASE_URL_VAR, URL)];
        all.extend_from_slice(vars);
        WorkerConfig::from_lookup(env(&all))
    }

    #[test]
    fn database_url_is_required() {
        let missing = ConfigError::Missing {
            var: DATABASE_URL_VAR,
        };
        assert_eq!(ApiConfig::from_lookup(env(&[])).unwrap_err(), missing);
        assert_eq!(WorkerConfig::from_lookup(env(&[])).unwrap_err(), missing);
    }

    #[test]
    fn defaults_are_conservative() {
        let cfg = api(&[]).unwrap();
        assert_eq!(cfg.bind, "127.0.0.1:8088".parse().unwrap());
        assert!(cfg.bind.ip().is_loopback());
        assert_eq!(cfg.database.max_connections, 10);
        assert_eq!(cfg.database.acquire_timeout, Duration::from_secs(3));
        assert!(cfg.database.migrate_on_start);

        let runtime = worker(&[]).unwrap().runtime;
        assert_eq!(runtime, WorkerRuntimeConfig::default());
        assert_eq!(runtime.concurrency, 4);
        assert_eq!(runtime.poll_interval, Duration::from_millis(250));
        assert_eq!(runtime.lease, Duration::from_secs(30));
        assert_eq!(runtime.shutdown_timeout, Duration::from_secs(10));
        assert_eq!(runtime.retry.max_delivery_attempts(), 5);
        assert_eq!(runtime.retry.base_delay(), Duration::from_secs(1));
        assert_eq!(runtime.retry.max_delay(), Duration::from_secs(60));
    }

    #[test]
    fn rejects_invalid_bind_address() {
        let err = api(&[(API_BIND_VAR, "localhost")]).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidBindAddress { .. }));
    }

    #[test]
    fn accepts_postgres_urls() {
        for url in [URL, "postgresql://db.internal/pulsestream?sslmode=require"] {
            let cfg = DatabaseConfig::from_lookup(&env(&[(DATABASE_URL_VAR, url)])).unwrap();
            assert_eq!(cfg.url.expose(), url);
        }
    }

    #[test]
    fn rejects_malformed_database_urls() {
        let parse = |url| DatabaseConfig::from_lookup(&env(&[(DATABASE_URL_VAR, url)]));
        assert_eq!(
            parse("  ").unwrap_err(),
            ConfigError::Empty {
                var: DATABASE_URL_VAR
            }
        );
        assert_eq!(
            parse("mysql://h/db").unwrap_err(),
            ConfigError::UnsupportedDatabaseScheme {
                var: DATABASE_URL_VAR
            }
        );
        assert_eq!(
            parse("postgres://user:pw@:5432/db").unwrap_err(),
            ConfigError::MissingDatabaseHost {
                var: DATABASE_URL_VAR
            }
        );
    }

    #[test]
    fn accepts_limits_at_range_bounds() {
        let cfg = worker(&[
            (DB_MAX_CONNECTIONS_VAR, "50"),
            (DB_ACQUIRE_TIMEOUT_MS_VAR, "100"),
            (MIGRATE_ON_START_VAR, "false"),
            (WORKER_CONCURRENCY_VAR, "1"),
            (POLL_INTERVAL_MS_VAR, "60000"),
            (PROCESSING_LEASE_MS_VAR, " 1000 "),
            (SHUTDOWN_TIMEOUT_MS_VAR, "300000"),
            (MAX_DELIVERY_ATTEMPTS_VAR, "100"),
            (RETRY_BASE_DELAY_MS_VAR, "600000"),
            (RETRY_MAX_DELAY_MS_VAR, "86400000"),
        ])
        .unwrap();
        assert_eq!(cfg.database.max_connections, 50);
        assert_eq!(cfg.database.acquire_timeout, Duration::from_millis(100));
        assert!(!cfg.database.migrate_on_start);
        assert_eq!(cfg.runtime.concurrency, 1);
        assert_eq!(cfg.runtime.poll_interval, Duration::from_secs(60));
        assert_eq!(cfg.runtime.lease, Duration::from_secs(1));
        assert_eq!(cfg.runtime.shutdown_timeout, Duration::from_secs(300));
        assert_eq!(cfg.runtime.retry.max_delivery_attempts(), 100);
        assert_eq!(cfg.runtime.retry.base_delay(), Duration::from_secs(600));
        assert_eq!(cfg.runtime.retry.max_delay(), Duration::from_secs(86_400));

        let minimal = worker(&[
            (MAX_DELIVERY_ATTEMPTS_VAR, "1"),
            (RETRY_BASE_DELAY_MS_VAR, "1"),
            (RETRY_MAX_DELAY_MS_VAR, "1"),
        ])
        .unwrap();
        assert_eq!(minimal.runtime.retry.max_delivery_attempts(), 1);
        assert_eq!(minimal.runtime.retry.max_delay(), Duration::from_millis(1));
    }

    #[test]
    fn rejects_out_of_range_integers() {
        let cases = [
            (DB_MAX_CONNECTIONS_VAR, "0"),
            (DB_MAX_CONNECTIONS_VAR, "51"),
            (DB_ACQUIRE_TIMEOUT_MS_VAR, "99"),
            (WORKER_CONCURRENCY_VAR, "0"),
            (WORKER_CONCURRENCY_VAR, "65"),
            (WORKER_CONCURRENCY_VAR, "4.5"),
            (POLL_INTERVAL_MS_VAR, "9"),
            (PROCESSING_LEASE_MS_VAR, "999"),
            (PROCESSING_LEASE_MS_VAR, "18446744073709551615"),
            (SHUTDOWN_TIMEOUT_MS_VAR, "0"),
            (SHUTDOWN_TIMEOUT_MS_VAR, "-1"),
            (SHUTDOWN_TIMEOUT_MS_VAR, ""),
            (MAX_DELIVERY_ATTEMPTS_VAR, "0"),
            (MAX_DELIVERY_ATTEMPTS_VAR, "101"),
            (RETRY_BASE_DELAY_MS_VAR, "0"),
            (RETRY_BASE_DELAY_MS_VAR, "600001"),
            (RETRY_MAX_DELAY_MS_VAR, "0"),
            (RETRY_MAX_DELAY_MS_VAR, "86400001"),
        ];
        for (var, value) in cases {
            let err = worker(&[(var, value)]).unwrap_err();
            assert!(
                matches!(err, ConfigError::OutOfRange { var: v, .. } if v == var),
                "{var}={value:?} -> {err:?}"
            );
        }
    }

    #[test]
    fn rejects_max_retry_delay_below_base() {
        let err = worker(&[
            (RETRY_BASE_DELAY_MS_VAR, "5000"),
            (RETRY_MAX_DELAY_MS_VAR, "4999"),
        ])
        .unwrap_err();
        assert_eq!(
            err,
            ConfigError::InvalidRetryPolicy(RetryPolicyError::MaxBelowBase {
                base: Duration::from_millis(5000),
                max: Duration::from_millis(4999),
            })
        );
        // The default maximum (60 s) is below a 10-minute base: also rejected,
        // never silently raised.
        let err = worker(&[(RETRY_BASE_DELAY_MS_VAR, "600000")]).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidRetryPolicy(_)), "{err:?}");
    }

    #[test]
    fn rejects_non_boolean_migrate_flag() {
        let err = api(&[(MIGRATE_ON_START_VAR, "yes")]).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidBool { .. }));
    }

    #[test]
    fn database_url_never_leaks_credentials() {
        let cfg = worker(&[(DATABASE_URL_VAR, "postgres://user:s3cret@host/db")]).unwrap();
        let rendered = format!("{cfg:?}");
        assert!(!rendered.contains("s3cret"), "{rendered}");
    }
}
