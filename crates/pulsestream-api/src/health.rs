//! Liveness and readiness endpoints.
//!
//! - `/health/live`: the process is alive. It stays `200` during a database outage.
//! - `/health/ready`: the service can durably accept events, proven by a
//!   lightweight PostgreSQL round trip. `503` when the database is unreachable.

use std::collections::BTreeMap;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Serialize;
use tracing::warn;

use crate::SERVICE;
use crate::app::AppState;

/// Upper bound on the readiness database check, independent of pool settings,
/// so probes never hang.
pub const READINESS_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Serialize)]
pub struct Liveness {
    status: &'static str,
    service: &'static str,
}

#[derive(Debug, Serialize)]
pub struct Readiness {
    status: &'static str,
    service: &'static str,
    /// Dependency name -> `"ready"` or `"unavailable"`.
    checks: BTreeMap<&'static str, &'static str>,
}

pub async fn live() -> Json<Liveness> {
    Json(Liveness {
        status: "ok",
        service: SERVICE.as_str(),
    })
}

pub async fn ready(State(state): State<AppState>) -> (StatusCode, Json<Readiness>) {
    let database_ready = match tokio::time::timeout(READINESS_TIMEOUT, state.store.ping()).await {
        Ok(Ok(())) => true,
        Ok(Err(err)) => {
            warn!(check = "database", error = %err, "readiness check failed");
            false
        }
        Err(_) => {
            warn!(check = "database", "readiness check timed out");
            false
        }
    };
    let checks = BTreeMap::from([(
        "database",
        if database_ready {
            "ready"
        } else {
            "unavailable"
        },
    )]);
    let (code, status) = if database_ready {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "unavailable")
    };
    (
        code,
        Json(Readiness {
            status,
            service: SERVICE.as_str(),
            checks,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use pulsestream_core::config::{DatabaseConfig, DatabaseUrl};
    use pulsestream_store::Store;
    use pulsestream_store::testing::TestDatabase;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use crate::app::{AppState, router};

    fn unreachable_store() -> Store {
        let url = DatabaseUrl::parse("postgres://u:p@127.0.0.1:1/unreachable").unwrap();
        let mut config = DatabaseConfig::with_url(url);
        config.acquire_timeout = Duration::from_millis(200);
        Store::connect_lazy(&config, "test").unwrap()
    }

    async fn get(store: Store, path: &str) -> (StatusCode, Option<String>, Value) {
        let response = router(AppState { store })
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap().to_owned());
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let json = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        };
        (status, content_type, json)
    }

    #[tokio::test]
    async fn live_is_ok_even_when_database_is_down() {
        let (status, content_type, body) = get(unreachable_store(), "/health/live").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type.as_deref(), Some("application/json"));
        assert_eq!(body, json!({"status": "ok", "service": "pulsestream-api"}));
    }

    #[tokio::test]
    async fn ready_is_503_when_database_is_down() {
        let (status, _, body) = get(unreachable_store(), "/health/ready").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body,
            json!({
                "status": "unavailable",
                "service": "pulsestream-api",
                "checks": {"database": "unavailable"}
            })
        );
    }

    #[tokio::test]
    async fn unknown_routes_are_not_found() {
        let (status, _, _) = get(unreachable_store(), "/health").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn ready_reports_database_ready() {
        let db = TestDatabase::create().await;
        let (status, _, body) = get(db.store.clone(), "/health/ready").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({"status": "ok", "service": "pulsestream-api", "checks": {"database": "ready"}})
        );
    }
}
