use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use pc_format::ArrowPointCloud;

use crate::Config;

pub(crate) struct AppState {
    pub(crate) config: Config,
    pub(crate) workers: Vec<String>,
    pub(crate) data: HashMap<String, ArrowPointCloud>,
}

unsafe impl Send for AppState {}

pub(crate) type SharedState = Arc<RwLock<AppState>>;
