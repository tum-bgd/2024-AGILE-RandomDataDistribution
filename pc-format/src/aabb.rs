use num_traits::{Bounded, NumCast};
use serde::{Deserialize, Serialize};

use crate::PointTrait;

/// Axis aligned bounding box, wraps [rstar::AABB]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AABB<P: PointTrait>(rstar::AABB<P>);

impl<P: PointTrait> std::ops::Deref for AABB<P> {
    type Target = rstar::AABB<P>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P> AABB<P>
where
    P: PointTrait,
{
    /// Returns the AABB encompassing a single point.
    pub fn from_point(p: P) -> Self {
        AABB(rstar::AABB::from_point(p))
    }

    /// Returns the AABB's lower corner.
    ///
    /// This is the point contained within the AABB with the smallest coordinate value in each
    /// dimension.
    pub fn lower(&self) -> P {
        self.0.lower()
    }

    /// Returns the AABB's upper corner.
    ///
    /// This is the point contained within the AABB with the largest coordinate value in each
    /// dimension.
    pub fn upper(&self) -> P {
        self.0.upper()
    }

    /// Creates a new AABB encompassing two points.
    pub fn from_corners(p1: P, p2: P) -> Self {
        AABB(rstar::AABB::from_corners(p1, p2))
    }

    /// Creates a new AABB encompassing a collection of points.
    pub fn from_points<'a, I>(i: I) -> Self
    where
        I: IntoIterator<Item = &'a P> + 'a,
        P: 'a,
    {
        i.into_iter()
            .fold(<Self as rstar::Envelope>::new_empty(), |aabb, p| {
                aabb.add_point(p)
            })
    }

    /// Returns the AABB that contains `self` and another point.
    fn add_point(&self, point: &P) -> Self {
        let lower = self.lower();
        let upper = self.upper();
        AABB(rstar::AABB::from_corners(
            P::generate(|i| {
                if lower.nth(i) < point.nth(i) {
                    lower.nth(i)
                } else {
                    point.nth(i)
                }
            }),
            P::generate(|i| {
                if upper.nth(i) > point.nth(i) {
                    upper.nth(i)
                } else {
                    point.nth(i)
                }
            }),
        ))
    }

    // /// Returns the point within this AABB closest to a given point.
    // ///
    // /// If `point` is contained within the AABB, `point` will be returned.
    // pub fn min_point(&self, point: &P) -> P {
    //     self.upper().min_point(&self.lower().max_point(point))
    // }

    // /// Returns the squared distance to the AABB's [min_point](AABB::min_point)
    // pub fn distance_2(&self, point: &P) -> P::Scalar {
    //     if self.contains_point(point) {
    //         Zero::zero()
    //     } else {
    //         self.min_point(point).sub(point).length_2()
    //     }
    // }
}

impl<P: PointTrait> std::ops::DerefMut for AABB<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<P: PointTrait> rstar::RTreeObject for AABB<P> {
    type Envelope = AABB<P>;

    fn envelope(&self) -> Self::Envelope {
        self.clone()
    }
}

impl<P: PointTrait> rstar::Envelope for AABB<P> {
    type Point = P;

    fn new_empty() -> Self {
        AABB(rstar::AABB::new_empty())
    }

    fn contains_point(&self, point: &Self::Point) -> bool {
        self.0.contains_point(point)
    }

    fn contains_envelope(&self, aabb: &Self) -> bool {
        self.0.contains_envelope(&aabb.0)
    }

    fn merge(&mut self, other: &Self) {
        self.0.merge(&other.0)
    }

    fn merged(&self, other: &Self) -> Self {
        AABB(self.0.merged(&other.0))
    }

    fn intersects(&self, other: &Self) -> bool {
        self.0.intersects(&other.0)
    }

    fn intersection_area(&self, other: &Self) -> <Self::Point as rstar::Point>::Scalar {
        self.0.intersection_area(&other.0)
    }

    fn area(&self) -> <Self::Point as rstar::Point>::Scalar {
        self.0.area()
    }

    fn distance_2(&self, point: &Self::Point) -> <Self::Point as rstar::Point>::Scalar {
        self.0.distance_2(point)
    }

    fn min_max_dist_2(&self, point: &Self::Point) -> <Self::Point as rstar::Point>::Scalar {
        self.0.min_max_dist_2(point)
    }

    fn center(&self) -> Self::Point {
        self.0.center()
    }

    fn perimeter_value(&self) -> <Self::Point as rstar::Point>::Scalar {
        self.0.perimeter_value()
    }

    fn sort_envelopes<T: rstar::RTreeObject<Envelope = Self>>(axis: usize, envelopes: &mut [T]) {
        envelopes.sort_by(|l, r| {
            l.envelope()
                .lower()
                .nth(axis)
                .partial_cmp(&r.envelope().lower().nth(axis))
                .unwrap()
        });
    }

    fn partition_envelopes<T: rstar::RTreeObject<Envelope = Self>>(
        axis: usize,
        envelopes: &mut [T],
        selection_size: usize,
    ) {
        envelopes.select_nth_unstable_by(selection_size, |l, r| {
            l.envelope()
                .lower()
                .nth(axis)
                .partial_cmp(&r.envelope().lower().nth(axis))
                .unwrap()
        });
    }
}

pub fn cast<P: PointTrait, Q: PointTrait>(aabb: &AABB<P>) -> AABB<Q>
where
    P::Scalar: NumCast,
    Q::Scalar: NumCast,
{
    let lower = aabb.lower();
    let lower = Q::generate(|i| {
        if i < P::DIMENSIONS {
            num_traits::cast(lower.nth(i)).unwrap()
        } else {
            Q::Scalar::min_value()
        }
    });
    let upper = aabb.upper();
    let upper = Q::generate(|i| {
        if i < P::DIMENSIONS {
            num_traits::cast(upper.nth(i)).unwrap()
        } else {
            Q::Scalar::max_value()
        }
    });

    AABB(rstar::AABB::from_corners(lower, upper))
}
