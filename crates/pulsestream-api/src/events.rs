//! `POST /v1/events` (durable, idempotent admission) and
//! `GET /v1/events/{event_id}` (status lookup).
//!
//! `202 Accepted` means the event row has been **committed to PostgreSQL**. It
//! survives API and worker restarts. Processing is at-least-once. Every `202`
//! carries `Idempotency-Replayed: false` (new event) or `true` (exact replay of
//! an earlier request, returning the original event ID).

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use pulsestream_core::event::{Event, EventId};
use pulsestream_core::idempotency::{IdempotencyKey, IdempotencyKeyError, RequestFingerprint};
use pulsestream_store::{AdmitOutcome, StoreError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, info, warn};

use crate::app::AppState;
use crate::error::ApiError;

/// Maximum accepted request body size, in bytes.
pub const MAX_BODY_BYTES: usize = 64 * 1024;

pub const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
pub const IDEMPOTENCY_REPLAYED: HeaderName = HeaderName::from_static("idempotency-replayed");

/// HTTP request shape. Clients cannot supply an event ID; it is server-generated.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRequest {
    source: String,
    event_type: String,
    payload: Value,
}

#[derive(Debug, Serialize)]
pub struct AcceptedResponse {
    event_id: String,
    /// Always `"accepted"`. It describes admission, not processing progress.
    status: &'static str,
}

/// Public status view. The payload, idempotency key, fingerprint, owner, and
/// lease are deliberately not exposed: payloads may carry sensitive business
/// data, and the rest are implementation details.
#[derive(Debug, Serialize)]
pub struct EventStatusResponse {
    event_id: String,
    source: String,
    event_type: String,
    status: &'static str,
    accepted_at: String,
    processed_at: Option<String>,
}

fn idempotency_key(headers: &HeaderMap) -> Result<IdempotencyKey, IdempotencyKeyError> {
    let value = headers
        .get(IDEMPOTENCY_KEY)
        .ok_or(IdempotencyKeyError::Missing)?;
    let value = value.to_str().map_err(|_| IdempotencyKeyError::Invalid)?;
    IdempotencyKey::parse(value)
}

fn store_error(err: &StoreError, operation: &'static str) -> ApiError {
    if err.is_unavailable() {
        warn!(operation, error = %err, "database unavailable");
        ApiError::PersistenceUnavailable
    } else {
        error!(operation, error = %err, "database error");
        ApiError::Internal
    }
}

pub async fn ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Json<EventRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let key = idempotency_key(&headers)?;
    let Json(request) = body.map_err(|rejection| ApiError::from_json_rejection(&rejection))?;
    let event = Event::new(request.source, request.event_type, request.payload)
        .map_err(|err| ApiError::InvalidEvent(err.to_string()))?;
    let fingerprint = RequestFingerprint::compute(&event.source, &event.event_type, &event.payload);

    let outcome = state
        .store
        .admit(&event, &key, &fingerprint)
        .await
        .map_err(|err| store_error(&err, "admit"))?;

    let (event_id, replayed) = match outcome {
        AdmitOutcome::Created(id) => {
            info!(
                event_id = %id,
                source = event.source.as_str(),
                event_type = event.event_type.as_str(),
                state = "pending",
                "event persisted"
            );
            (id, false)
        }
        AdmitOutcome::Replayed(id) => {
            info!(
                event_id = %id,
                source = event.source.as_str(),
                event_type = event.event_type.as_str(),
                "idempotency replay; returning original event"
            );
            (id, true)
        }
        AdmitOutcome::Conflict => {
            warn!(
                source = event.source.as_str(),
                event_type = event.event_type.as_str(),
                "idempotency conflict; key already used for a different request"
            );
            return Err(ApiError::IdempotencyConflict);
        }
    };

    let mut response = (
        StatusCode::ACCEPTED,
        Json(AcceptedResponse {
            event_id: event_id.to_string(),
            status: "accepted",
        }),
    )
        .into_response();
    response.headers_mut().insert(
        IDEMPOTENCY_REPLAYED,
        HeaderValue::from_static(if replayed { "true" } else { "false" }),
    );
    Ok(response)
}

pub async fn get(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> Result<Json<EventStatusResponse>, ApiError> {
    let id = EventId::parse(&event_id).ok_or(ApiError::InvalidEventId)?;
    let record = state
        .store
        .get(id)
        .await
        .map_err(|err| store_error(&err, "get"))?
        .ok_or(ApiError::EventNotFound)?;
    Ok(Json(EventStatusResponse {
        event_id: record.event_id.to_string(),
        source: record.source,
        event_type: record.event_type,
        status: record.status.as_api_str(),
        accepted_at: record.accepted_at,
        processed_at: record.processed_at,
    }))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use futures_util::stream;
    use pulsestream_core::config::{DatabaseConfig, DatabaseUrl, WorkerRuntimeConfig};
    use pulsestream_core::event::WorkerId;
    use pulsestream_store::Store;
    use pulsestream_store::testing::{OutageProxy, TestDatabase};
    use pulsestream_worker::processor::AcknowledgeProcessor;
    use serde_json::{Value, json};
    use tokio::task::JoinSet;
    use tower::ServiceExt;

    use super::MAX_BODY_BYTES;
    use crate::app::{AppState, router};

    struct Reply {
        status: StatusCode,
        json: Value,
        replayed: Option<String>,
        retry_after: Option<String>,
    }

    fn app(store: &Store) -> Router {
        router(AppState {
            store: store.clone(),
        })
    }

    /// A store whose database is unreachable (nothing listens on port 1).
    fn unreachable_store() -> Store {
        let url = DatabaseUrl::parse("postgres://u:p@127.0.0.1:1/unreachable").unwrap();
        let mut config = DatabaseConfig::with_url(url);
        config.acquire_timeout = Duration::from_millis(200);
        Store::connect_lazy(&config, "test").unwrap()
    }

    async fn send(app: Router, request: Request<Body>) -> Reply {
        fn header(headers: &axum::http::HeaderMap, name: &str) -> Option<String> {
            headers.get(name).map(|v| v.to_str().unwrap().to_owned())
        }
        let response = app.oneshot(request).await.unwrap();
        let replayed = header(response.headers(), "idempotency-replayed");
        let retry_after = header(response.headers(), "retry-after");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let json = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        Reply {
            status,
            json,
            replayed,
            retry_after,
        }
    }

    fn post(key: Option<&str>, body: impl Into<Body>) -> Request<Body> {
        let mut builder =
            Request::post("/v1/events").header(header::CONTENT_TYPE, "application/json");
        if let Some(key) = key {
            builder = builder.header("Idempotency-Key", key);
        }
        builder.body(body.into()).unwrap()
    }

    fn post_json(key: &str, body: &Value) -> Request<Body> {
        post(Some(key), body.to_string())
    }

    fn get(id: &str) -> Request<Body> {
        Request::get(format!("/v1/events/{id}"))
            .body(Body::empty())
            .unwrap()
    }

    fn valid() -> Value {
        json!({"source": "orders-api", "event_type": "order.created", "payload": {"order_id": "12345"}})
    }

    async fn assert_rejected(request: Request<Body>, status: StatusCode, code: &str) -> Value {
        // The database is unreachable: these requests must fail validation
        // before any database access.
        let reply = send(app(&unreachable_store()), request).await;
        assert_eq!(reply.status, status, "{}", reply.json);
        assert_eq!(reply.json["code"], code);
        reply.json
    }

    // ---- Validation (no database) -------------------------------------------

    #[tokio::test]
    async fn requires_an_idempotency_key() {
        assert_rejected(
            post(None, valid().to_string()),
            StatusCode::BAD_REQUEST,
            "IDEMPOTENCY_KEY_REQUIRED",
        )
        .await;
    }

    #[tokio::test]
    async fn rejects_invalid_idempotency_keys() {
        for key in ["", "has space", &"k".repeat(129)] {
            assert_rejected(
                post_json(key, &valid()),
                StatusCode::BAD_REQUEST,
                "IDEMPOTENCY_KEY_INVALID",
            )
            .await;
        }
        let non_ascii = Request::post("/v1/events")
            .header(header::CONTENT_TYPE, "application/json")
            .header(
                "Idempotency-Key",
                axum::http::HeaderValue::from_bytes(b"cl\xc3\xa9").unwrap(),
            )
            .body(Body::from(valid().to_string()))
            .unwrap();
        assert_rejected(
            non_ascii,
            StatusCode::BAD_REQUEST,
            "IDEMPOTENCY_KEY_INVALID",
        )
        .await;
    }

    #[tokio::test]
    async fn validates_event_fields() {
        let cases = [
            ("source", json!(""), "source must not be empty"),
            ("event_type", json!(" "), "event_type must not be empty"),
            (
                "source",
                json!("s".repeat(101)),
                "source must be at most 100 characters",
            ),
            (
                "event_type",
                json!("t".repeat(151)),
                "event_type must be at most 150 characters",
            ),
            ("payload", Value::Null, "payload must not be null"),
        ];
        for (field, value, message) in cases {
            let mut body = valid();
            body[field] = value;
            let json = assert_rejected(
                post_json("k", &body),
                StatusCode::BAD_REQUEST,
                "INVALID_EVENT",
            )
            .await;
            assert_eq!(json["message"], message);
        }
    }

    #[tokio::test]
    async fn rejects_malformed_or_misshapen_bodies() {
        let json = assert_rejected(
            post(Some("k"), r#"{"source": "a", "#),
            StatusCode::BAD_REQUEST,
            "INVALID_EVENT",
        )
        .await;
        assert_eq!(json["message"], "request body is not valid JSON");

        let mut client_id = valid();
        client_id["event_id"] = json!("chosen-by-client");
        assert_rejected(
            post_json("k", &client_id),
            StatusCode::BAD_REQUEST,
            "INVALID_EVENT",
        )
        .await;
        let mut missing = valid();
        missing.as_object_mut().unwrap().remove("payload");
        assert_rejected(
            post_json("k", &missing),
            StatusCode::BAD_REQUEST,
            "INVALID_EVENT",
        )
        .await;

        let text = Request::post("/v1/events")
            .header(header::CONTENT_TYPE, "text/plain")
            .header("Idempotency-Key", "k")
            .body(Body::from(valid().to_string()))
            .unwrap();
        assert_rejected(
            text,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UNSUPPORTED_MEDIA_TYPE",
        )
        .await;
    }

    fn oversized_body() -> String {
        let mut body = valid();
        body["payload"] = json!({"blob": "x".repeat(MAX_BODY_BYTES)});
        body.to_string()
    }

    #[tokio::test]
    async fn enforces_the_body_limit_with_and_without_content_length() {
        let body = oversized_body();
        let sized = Request::post("/v1/events")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, body.len())
            .header("Idempotency-Key", "k")
            .body(Body::from(body.clone()))
            .unwrap();
        assert_rejected(sized, StatusCode::PAYLOAD_TOO_LARGE, "PAYLOAD_TOO_LARGE").await;

        let chunks: Vec<Result<String, std::io::Error>> = body
            .into_bytes()
            .chunks(8 * 1024)
            .map(|c| Ok(String::from_utf8(c.to_vec()).unwrap()))
            .collect();
        let streamed = post(Some("k"), Body::from_stream(stream::iter(chunks)));
        assert_rejected(streamed, StatusCode::PAYLOAD_TOO_LARGE, "PAYLOAD_TOO_LARGE").await;
    }

    #[tokio::test]
    async fn rejects_malformed_event_ids() {
        assert_rejected(
            get("not-a-uuid"),
            StatusCode::BAD_REQUEST,
            "INVALID_EVENT_ID",
        )
        .await;
    }

    // ---- Database unavailable (no database) ---------------------------------

    #[tokio::test]
    async fn database_outage_never_returns_202() {
        let store = unreachable_store();
        let reply = send(app(&store), post_json("k", &valid())).await;
        assert_eq!(reply.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            reply.json,
            json!({"code": "PERSISTENCE_UNAVAILABLE", "message": "event persistence is temporarily unavailable"})
        );
        assert_eq!(reply.retry_after.as_deref(), Some("1"));
        assert_eq!(reply.replayed, None);

        let reply = send(
            app(&store),
            get(&pulsestream_core::event::EventId::new().to_string()),
        )
        .await;
        assert_eq!(reply.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(reply.json["code"], "PERSISTENCE_UNAVAILABLE");
    }

    // ---- Real PostgreSQL ----------------------------------------------------

    #[tokio::test]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn first_request_replay_and_conflict() {
        let db = TestDatabase::create().await;
        let first = send(app(&db.store), post_json("order-12345", &valid())).await;
        assert_eq!(first.status, StatusCode::ACCEPTED);
        assert_eq!(first.replayed.as_deref(), Some("false"));
        assert_eq!(first.json["status"], "accepted");
        assert_eq!(first.json.as_object().unwrap().len(), 2);
        let event_id = first.json["event_id"].as_str().unwrap().to_owned();

        // Exact replay, even with payload keys and top-level fields reordered.
        let reordered = r#"{"payload":{"order_id":"12345"},"event_type":"order.created","source":"orders-api"}"#;
        let replay = send(app(&db.store), post(Some("order-12345"), reordered)).await;
        assert_eq!(replay.status, StatusCode::ACCEPTED);
        assert_eq!(replay.replayed.as_deref(), Some("true"));
        assert_eq!(replay.json["event_id"], event_id.as_str());

        let mut changed = valid();
        changed["payload"]["order_id"] = json!("99999");
        let conflict = send(app(&db.store), post_json("order-12345", &changed)).await;
        assert_eq!(conflict.status, StatusCode::CONFLICT);
        assert_eq!(conflict.json["code"], "IDEMPOTENCY_CONFLICT");

        let mut other_type = valid();
        other_type["event_type"] = json!("order.updated");
        let conflict = send(app(&db.store), post_json("order-12345", &other_type)).await;
        assert_eq!(conflict.status, StatusCode::CONFLICT);

        assert_eq!(db.count_for_source("orders-api").await, 1);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn same_key_from_another_source_is_a_separate_event() {
        let db = TestDatabase::create().await;
        let orders = send(app(&db.store), post_json("abc123", &valid())).await;
        let mut billing = valid();
        billing["source"] = json!("billing-api");
        let billing = send(app(&db.store), post_json("abc123", &billing)).await;
        assert_eq!(
            (orders.status, billing.status),
            (StatusCode::ACCEPTED, StatusCode::ACCEPTED)
        );
        assert_eq!(billing.replayed.as_deref(), Some("false"));
        assert_ne!(orders.json["event_id"], billing.json["event_id"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn concurrent_identical_http_requests_share_one_event() {
        let db = TestDatabase::create().await;
        let mut tasks = JoinSet::new();
        for _ in 0..30 {
            let app = app(&db.store);
            tasks.spawn(async move { send(app, post_json("race", &valid())).await });
        }
        let replies = tasks.join_all().await;
        let ids: std::collections::HashSet<_> = replies
            .iter()
            .map(|r| {
                assert_eq!(r.status, StatusCode::ACCEPTED);
                r.json["event_id"].as_str().unwrap().to_owned()
            })
            .collect();
        assert_eq!(ids.len(), 1, "all callers observe one event_id");
        let fresh = replies
            .iter()
            .filter(|r| r.replayed.as_deref() == Some("false"))
            .count();
        assert_eq!(fresh, 1, "exactly one response reports a new event");
        assert_eq!(db.count_for_source("orders-api").await, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn accepted_event_survives_application_restart_and_gets_processed() {
        let db = TestDatabase::create().await;

        // Instance 1 accepts the event, then is destroyed.
        let store_1 = Store::connect_lazy(&db.config(), "api-instance-1").unwrap();
        let accepted = send(app(&store_1), post_json("durable-1", &valid())).await;
        assert_eq!(accepted.status, StatusCode::ACCEPTED);
        let event_id = accepted.json["event_id"].as_str().unwrap().to_owned();
        store_1.close().await;
        drop(store_1);

        // Instance 2 is a fresh pool against the same database.
        let store_2 = Store::connect_lazy(&db.config(), "api-instance-2").unwrap();
        let status = send(app(&store_2), get(&event_id)).await;
        assert_eq!(status.status, StatusCode::OK);
        assert_eq!(status.json["status"], "pending");
        assert_eq!(status.json["processed_at"], Value::Null);
        let fields: Vec<_> = status.json.as_object().unwrap().keys().cloned().collect();
        assert_eq!(
            fields,
            [
                "accepted_at",
                "event_id",
                "event_type",
                "processed_at",
                "source",
                "status"
            ],
            "no payload, key, fingerprint, owner, or lease is exposed"
        );

        // A worker processes it; the status reflects the durable transition.
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let runtime = WorkerRuntimeConfig {
            poll_interval: Duration::from_millis(10),
            ..WorkerRuntimeConfig::default()
        };
        let worker = tokio::spawn(pulsestream_worker::runtime::run(
            store_2.clone(),
            WorkerId::new(),
            runtime,
            AcknowledgeProcessor,
            async {
                let _ = stop_rx.await;
            },
        ));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let processed = loop {
            let reply = send(app(&store_2), get(&event_id)).await;
            if reply.json["status"] == "processed" {
                break reply;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "event was not processed"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(
            processed.json["processed_at"]
                .as_str()
                .unwrap()
                .ends_with('Z')
        );
        stop_tx.send(()).unwrap();
        assert_eq!(worker.await.unwrap().processed, 1);

        let missing = send(
            app(&store_2),
            get(&pulsestream_core::event::EventId::new().to_string()),
        )
        .await;
        assert_eq!(missing.status, StatusCode::NOT_FOUND);
        assert_eq!(missing.json["code"], "EVENT_NOT_FOUND");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires PostgreSQL (PULSESTREAM_TEST_DATABASE_URL)"]
    async fn outage_rejects_admission_and_recovers_without_restart() {
        let db = TestDatabase::create().await;
        let proxy = OutageProxy::start(db.url()).await;
        let mut config =
            DatabaseConfig::with_url(DatabaseUrl::parse(&proxy.proxied_url(db.url())).unwrap());
        config.acquire_timeout = Duration::from_millis(500);
        let store = Store::connect_lazy(&config, "api-outage-test").unwrap();
        let app = || app(&store);
        let ready = || Request::get("/health/ready").body(Body::empty()).unwrap();
        let live = || Request::get("/health/live").body(Body::empty()).unwrap();

        assert_eq!(send(app(), ready()).await.status, StatusCode::OK);
        assert_eq!(
            send(app(), post_json("before", &valid())).await.status,
            StatusCode::ACCEPTED
        );

        proxy.go_down();
        assert_eq!(send(app(), live()).await.status, StatusCode::OK);
        let not_ready = send(app(), ready()).await;
        assert_eq!(not_ready.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(not_ready.json["checks"]["database"], "unavailable");
        let rejected = send(app(), post_json("during", &valid())).await;
        assert_eq!(rejected.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(rejected.json["code"], "PERSISTENCE_UNAVAILABLE");

        proxy.come_up(); // same application instance, same pool
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while send(app(), ready()).await.status != StatusCode::OK {
            assert!(
                tokio::time::Instant::now() < deadline,
                "readiness did not recover"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            send(app(), post_json("after", &valid())).await.status,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            db.count_for_source("orders-api").await,
            2,
            "nothing admitted during the outage"
        );
    }
}
