//! PulseStream worker process.
//!
//! Claims durably accepted events from PostgreSQL and processes them with
//! bounded concurrency. Every process gets a fresh worker ID, which it records
//! as the owner of its claims. On shutdown it stops claiming, lets active
//! events finish within the timeout, and leaves anything unfinished to lease
//! recovery.

mod shutdown;
mod telemetry;

use std::process::ExitCode;

use pulsestream_core::ServiceName;
use pulsestream_core::config::WorkerConfig;
use pulsestream_core::event::WorkerId;
use pulsestream_store::Store;
use pulsestream_worker::processor::AcknowledgeProcessor;
use pulsestream_worker::runtime;
use tracing::{error, info};

const SERVICE: ServiceName = ServiceName::Worker;

#[tokio::main]
async fn main() -> ExitCode {
    telemetry::init();

    let config = match WorkerConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            error!(service = %SERVICE, error = %err, "invalid configuration");
            return ExitCode::FAILURE;
        }
    };
    let owner = WorkerId::new();
    info!(
        service = %SERVICE,
        version = env!("CARGO_PKG_VERSION"),
        worker_id = %owner,
        max_connections = config.database.max_connections,
        "starting"
    );

    let store = match Store::connect_lazy(&config.database, SERVICE.as_str()) {
        Ok(store) => store,
        Err(err) => {
            error!(service = %SERVICE, error = %err, "invalid database configuration");
            return ExitCode::FAILURE;
        }
    };
    if config.database.migrate_on_start {
        if let Err(err) = store.migrate().await {
            error!(service = %SERVICE, error = %err, "database migration failed");
            return ExitCode::FAILURE;
        }
        info!(service = %SERVICE, "database migrations applied");
    }

    let report = runtime::run(
        store.clone(),
        owner,
        config.runtime,
        AcknowledgeProcessor,
        shutdown::signal(),
    )
    .await;
    store.close().await;
    info!(service = %SERVICE, worker_id = %owner, "shutdown complete");
    if report.timed_out {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
