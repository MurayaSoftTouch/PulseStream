//! A worker that fails on a script, for manual retry and dead-letter smoke
//! tests against a local database. Not a production binary.
//!
//! It runs the real worker runtime with the real environment configuration
//! (`DATABASE_URL`, `PULSESTREAM_MAX_DELIVERY_ATTEMPTS`, ...), but each event's
//! n-th delivery gets step n of `PULSESTREAM_SMOKE_SCRIPT` (the last step
//! repeats). Steps: `succeed`, `panic`, `retryable:<CODE>`, `permanent:<CODE>`.
//!
//! ```bash
//! PULSESTREAM_SMOKE_SCRIPT=retryable:UPSTREAM_TIMEOUT,succeed \
//!   cargo run -p pulsestream-worker --features test-util --example scripted_worker
//! ```

use std::process::ExitCode;

use pulsestream_core::config::WorkerConfig;
use pulsestream_core::event::WorkerId;
use pulsestream_store::Store;
use pulsestream_worker::runtime;
use pulsestream_worker::testing::{ScriptedProcessor, Step};
use tracing_subscriber::EnvFilter;

const SCRIPT_VAR: &str = "PULSESTREAM_SMOKE_SCRIPT";

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let Ok(raw) = std::env::var(SCRIPT_VAR) else {
        eprintln!("{SCRIPT_VAR} must be set, for example retryable:UPSTREAM_TIMEOUT,succeed");
        return ExitCode::FAILURE;
    };
    // Parsed once at startup; leaking gives the steps the `'static` codes they borrow.
    let raw: &'static str = Box::leak(raw.into_boxed_str());
    let Some(script) = raw
        .split(',')
        .map(str::trim)
        .map(Step::parse)
        .collect::<Option<Vec<_>>>()
    else {
        eprintln!("{SCRIPT_VAR} has an invalid step: {raw:?}");
        return ExitCode::FAILURE;
    };
    let config = match WorkerConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("invalid configuration: {err}");
            return ExitCode::FAILURE;
        }
    };
    let store = match Store::connect_lazy(&config.database, "pulsestream-scripted-worker") {
        Ok(store) => store,
        Err(err) => {
            eprintln!("invalid database configuration: {err}");
            return ExitCode::FAILURE;
        }
    };
    if config.database.migrate_on_start
        && let Err(err) = store.migrate().await
    {
        eprintln!("database migration failed: {err}");
        return ExitCode::FAILURE;
    }

    tracing::info!(script = raw, "scripted worker starting");
    let report = runtime::run(
        store.clone(),
        WorkerId::new(),
        config.runtime,
        ScriptedProcessor::new(script),
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await;
    store.close().await;
    tracing::info!(?report, "scripted worker stopped");
    ExitCode::SUCCESS
}
