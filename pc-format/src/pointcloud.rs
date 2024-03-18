use arrow::datatypes::SchemaRef;

use crate::{PointCloudError, PointTrait, AABB};

/// Point cloud trait
pub trait PointCloudTrait: Sized {
    fn schema(&self) -> SchemaRef;

    /// number of points the cloud is made of
    fn num_points(&self) -> usize;

    /// get iterator over points
    fn points<'a, P>(&'a self) -> Box<dyn Iterator<Item = P> + 'a>
    where
        P: PointTrait + 'a,
        <P as rstar::Point>::Scalar: num_traits::NumCast;

    /// get axis aligned bounding box of points
    fn aabb<P>(&self) -> AABB<P>
    where
        P: PointTrait,
        <P as rstar::Point>::Scalar: num_traits::NumCast,
    {
        let points: Vec<P> = self.points().collect();
        AABB::from_points(points.iter())
    }

    fn from_iter<P>(iter: impl Iterator<Item = P>) -> Result<Self, PointCloudError>
    where
        P: PointTrait,
        <P as rstar::Point>::Scalar: num_traits::NumCast;
}
