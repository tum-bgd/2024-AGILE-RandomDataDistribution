pub mod aabb;
pub use aabb::AABB;

pub mod aos;
pub use aos::VecPointCloud;

pub mod compute;

pub mod framework;
pub use framework::{Cell, Framework};

pub mod point;
pub use point::{Coord, Point, PointTrait};

pub mod pointcloud;
pub use pointcloud::PointCloudTrait;

pub mod schema;

pub mod soa;
pub use soa::ArrowPointCloud;

#[derive(thiserror::Error, Debug)]
pub enum PointCloudError {
    #[error("arrow error")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("schema validation error: {0}")]
    SchemaError(String),
    #[error("cache error: {0}")]
    CacheError(String),
}
