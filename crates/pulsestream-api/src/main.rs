//! PulseStream HTTP API.
//!
//! Durably admits events into PostgreSQL (`POST /v1/events`) and reports their
//! status (`GET /v1/events/{event_id}`). A `202` means the event is committed.
//! There is no in-memory queue, so shutdown has no volatile work to drain:
//! it only stops accepting HTTP and lets in-flight requests finish.

mod app;
mod error;
mod events;
mod health;
mod shutdown;
mod telemetry;

use std::future::Future;
use std::process::ExitCode;

use axum::Router;
use pulsestream_core::ServiceName;
use pulsestream_core::config::ApiConfig;
use pulsestream_store::Store;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::app::AppState;

const SERVICE: ServiceName = ServiceName::Api;

#[tokio::main]
async fn main() -> ExitCode {
    telemetry::init();

    let config = match ApiConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            error!(service = %SERVICE, error = %err, "invalid configuration");
            return ExitCode::FAILURE;
        }
    };
    info!(
        service = %SERVICE,
        version = env!("CARGO_PKG_VERSION"),
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

    let listener = match TcpListener::bind(config.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            error!(service = %SERVICE, bind = %config.bind, error = %err, "failed to bind");
            return ExitCode::FAILURE;
        }
    };
    match listener.local_addr() {
        Ok(addr) => info!(service = %SERVICE, bind = %addr, "listening"),
        Err(err) => info!(service = %SERVICE, bind = %config.bind, error = %err, "listening"),
    }

    let app = app::router(AppState {
        store: store.clone(),
    });
    let served = serve(listener, app, shutdown::signal()).await;
    if let Err(err) = &served {
        error!(service = %SERVICE, error = %err, "server error");
    }
    // Committed events are already durable; nothing in memory needs draining.
    store.close().await;
    info!(service = %SERVICE, "shutdown complete");
    if served.is_ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Serves `app` until `shutdown` resolves, then stops accepting connections
/// and lets in-flight requests finish.
async fn serve(
    listener: TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pulsestream_core::config::{DatabaseConfig, DatabaseUrl};
    use pulsestream_store::Store;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    use crate::app::{AppState, router};

    #[tokio::test]
    async fn serves_requests_then_shuts_down_gracefully() {
        let url = DatabaseUrl::parse("postgres://u:p@127.0.0.1:1/unreachable").unwrap();
        let store = Store::connect_lazy(&DatabaseConfig::with_url(url), "test").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(super::serve(listener, router(AppState { store }), async {
            let _ = stop_rx.await;
        }));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");

        stop_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("server did not stop after shutdown was signalled")
            .unwrap();
        assert!(result.is_ok());
    }
}
