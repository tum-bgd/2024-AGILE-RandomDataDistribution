use arrow::datatypes::SchemaRef;

use crate::{PointCloudError, PointCloudTrait, PointTrait};

/// Point cloud
#[derive(Clone)]
pub struct VecPointCloud<P>
where
    P: PointTrait,
{
    points: Vec<P>,
}

impl<P> VecPointCloud<P>
where
    P: PointTrait,
{
    /// get points mutable
    pub fn points_mut(&mut self) -> impl Iterator<Item = &'_ mut P> {
        self.points.iter_mut()
    }
}

impl<P> PointCloudTrait for VecPointCloud<P>
where
    P: PointTrait,
    <P as rstar::Point>::Scalar: num_traits::NumCast,
{
    fn schema(&self) -> SchemaRef {
        P::schema()
    }

    fn num_points(&self) -> usize {
        self.points.len()
    }

    fn from_iter<Q>(iter: impl Iterator<Item = Q>) -> Result<Self, PointCloudError>
    where
        Q: PointTrait,
        <Q as rstar::Point>::Scalar: num_traits::NumCast,
    {
        Ok(Self {
            points: iter.map(|p| crate::point::cast(&p)).collect(),
        })
    }

    fn points<'a, Q>(&'a self) -> Box<dyn Iterator<Item = Q> + 'a>
    where
        Q: PointTrait + 'a,
        <Q as rstar::Point>::Scalar: num_traits::NumCast,
    {
        Box::new(
            self.points
                .clone()
                .into_iter()
                .map(|p| crate::point::cast(&p)),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{aabb::AABB, point::Point, point::PointTrait};
    use rstar::{Envelope, PointDistance};

    use super::*;

    #[test]
    fn aabb() {
        let pc: VecPointCloud<Point<f64, 3>> = VecPointCloud::from_iter(
            [
                Point::<f64, 3>::from_slice(&[0.0, 1.0, 2.0]),
                Point::from_slice(&[2.0, 0.0, 1.0]),
                Point::from_slice(&[1.0, 2.0, 0.0]),
            ]
            .into_iter(),
        )
        .unwrap();

        dbg!(AABB::<Point<f64, 3>>::from_points([]));

        assert_eq!(pc.num_points(), 3);
        assert_eq!(
            pc.aabb::<Point<f64, 3>>(),
            AABB::from_corners(
                Point::from_slice(&[0.0, 0.0, 0.0]),
                Point::from_slice(&[2.0, 2.0, 2.0]),
            )
        );
    }

    #[test]
    fn filter() {
        // filter by axis aligned bounding box
        let pc: VecPointCloud<Point<f64, 3>> = VecPointCloud::from_iter(
            [
                Point::<f64, 3>::from_slice(&[1.; 3]),
                Point::from_slice(&[2.; 3]),
                Point::from_slice(&[3.; 3]),
            ]
            .into_iter(),
        )
        .unwrap();

        let aabb: AABB<Point<f64, 3>> =
            AABB::from_corners(Point::from_slice(&[0.5; 3]), Point::from_slice(&[1.5; 3]));

        assert_eq!(pc.points().filter(|p| aabb.contains_point(p)).count(), 1);
    }

    #[test]
    fn scale() {
        // scale aspect-correct into the unit cube
        let pc: VecPointCloud<Point<f64, 3>> = VecPointCloud::from_iter(
            [
                Point::<f64, 3>::from_slice(&[0.0, 1.0, 2.0]),
                Point::from_slice(&[2.0, 0.0, 1.0]),
                Point::from_slice(&[1.0, 2.0, 0.0]),
            ]
            .into_iter(),
        )
        .unwrap();

        let aabb: AABB<Point<f64, 3>> = pc.aabb();

        let scaler = *aabb
            .upper()
            .sub(&aabb.lower())
            .coords()
            .iter()
            .max_by(|a: &&f64, b| a.partial_cmp(b).unwrap())
            .unwrap();

        let scale = Point::from_slice(&[scaler; 3]);

        let scaled: VecPointCloud<Point<f64, 3>> = VecPointCloud::from_iter(
            pc.points()
                .map(|p: Point<f64, 3>| p.sub(&aabb.lower()).div(&scale)),
        )
        .unwrap();

        let unit_cube: AABB<Point<f64, 3>> =
            AABB::from_corners(Point::from_slice(&[0.; 3]), Point::from_slice(&[1.; 3]));

        assert_eq!(pc.num_points(), scaled.num_points());
        assert!(unit_cube.contains_envelope(&scaled.aabb()))
    }

    #[test]
    fn product() {
        // caculate distance matrix
        let pc: VecPointCloud<Point<f64, 3>> = VecPointCloud::from_iter(
            [
                Point::<f64, 3>::from_slice(&[0.0, 1.0, 2.0]),
                Point::from_slice(&[2.0, 0.0, 1.0]),
                Point::from_slice(&[1.0, 2.0, 0.0]),
            ]
            .into_iter(),
        )
        .unwrap();

        // distance function
        let d: fn(&Point<f64, 3>, &Point<f64, 3>) -> f64 = |p1, p2| p1.distance_2(p2).sqrt();

        // map distance function to cartesian product
        let dm = nalgebra::DMatrix::from_iterator(
            pc.num_points(),
            pc.num_points(),
            pc.points()
                .flat_map(|p1| pc.points().map(move |p2| d(&p1, &p2))),
        );

        assert_eq!(dm.sum() / (2 * pc.points.len()) as f64, 2.4494897427831783);
    }
}
