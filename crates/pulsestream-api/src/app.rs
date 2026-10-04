//! Router and shared request state.

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use pulsestream_store::Store;

use crate::{events, health};

#[derive(Debug, Clone)]
pub struct AppState {
    pub store: Store,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health/live", get(health::live))
        .route("/health/ready", get(health::ready))
        .route(
            "/v1/events",
            post(events::ingest).layer(DefaultBodyLimit::max(events::MAX_BODY_BYTES)),
        )
        .route("/v1/events/{event_id}", get(events::get))
        .with_state(state)
}
