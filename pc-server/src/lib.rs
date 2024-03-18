use std::{any::Any, collections::HashMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, FromRequestParts},
    http::{header::CONTENT_TYPE, request::Parts, Response, StatusCode},
    routing::get,
};
use http_body_util::Full;
use tokio::sync::RwLock;
use tower::ServiceBuilder;
use tower_http::{
    add_extension::AddExtensionLayer,
    catch_panic::CatchPanicLayer,
    compression::CompressionLayer,
    cors::CorsLayer,
    trace::{DefaultMakeSpan, TraceLayer},
};

mod config;
mod error;
mod handlers;
mod state;

pub use config::Config;
use state::AppState;

pub fn app(config: Config) -> axum::Router {
    let state = AppState {
        config,
        workers: Default::default(),
        data: HashMap::default(),
    };

    axum::Router::new()
        .route("/status", get(handlers::status))
        .route(
            "/workers",
            get(handlers::workers)
                .post(handlers::register_worker)
                .delete(handlers::deregister_worker),
        )
        .route(
            "/load",
            get(handlers::load)
                .post(handlers::push_batch)
                .delete(handlers::delete)
                .layer(DefaultBodyLimit::max(1024 * 1024 * 1024)),
        )
        .route(
            "/index",
            get(handlers::index).delete(handlers::remove_index),
        )
        .route("/points", get(handlers::points))
        .layer(
            ServiceBuilder::new()
                .layer(AddExtensionLayer::new(Arc::new(RwLock::new(state))))
                .layer(TraceLayer::new_for_http().make_span_with(DefaultMakeSpan::new()))
                .layer(CompressionLayer::new())
                .layer(CorsLayer::permissive())
                .layer(CatchPanicLayer::custom(handle_panic)),
        )
}

fn handle_panic(err: Box<dyn Any + Send + 'static>) -> Response<Full<Bytes>> {
    let details = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "Unknown panic message".to_string()
    };

    tracing::error!(details);

    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(CONTENT_TYPE, "application/text")
        .body(Full::from(details))
        .unwrap()
}

/// Extractor that deserializes query strings into some type `T` with [`serde_qs`]
pub(crate) struct Qs<T>(pub(crate) T);

#[axum::async_trait]
impl<S, T> FromRequestParts<S> for Qs<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let qs = parts.uri.query().unwrap_or("");
        match serde_qs::from_str(qs) {
            Ok(query) => Ok(Self(query)),
            Err(e) => Err((StatusCode::BAD_REQUEST, e.to_string())),
        }
    }
}
