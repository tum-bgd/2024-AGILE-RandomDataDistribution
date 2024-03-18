use axum::{Extension, Json};
use serde::Serialize;

use crate::{state::SharedState, Config};

// Status
#[derive(Serialize)]
pub(crate) struct Status {
    config: Config,
    workers: Vec<String>,
}

// #[axum::debug_handler]
pub(crate) async fn status(Extension(state): Extension<SharedState>) -> Json<Status> {
    let state = state.read().await;

    let status = Status {
        config: state.config.to_owned(),
        workers: state.workers.to_owned(),
    };

    Json(status)
}
