use axum::{Extension, Json};

use crate::state::SharedState;

// list workers
pub(crate) async fn workers(Extension(state): Extension<SharedState>) -> Json<Vec<String>> {
    tracing::info!("List workers");
    Json(state.read().await.workers.to_owned())
}

// register worker
pub(crate) async fn register_worker(Extension(state): Extension<SharedState>, body: String) {
    tracing::info!("Registering worker {body}");
    state.write().await.workers.push(body);
}

// register worker
pub(crate) async fn deregister_worker(Extension(state): Extension<SharedState>, body: String) {
    tracing::info!("Deregistering worker {body}");
    state.write().await.workers.retain(|w| w != &body);
}
