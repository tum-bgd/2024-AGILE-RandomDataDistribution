use itertools::Itertools;
use rstar::RTreeObject;
use serde::{Deserialize, Serialize};

use crate::{PointTrait, AABB};

/// Framework cell
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Cell<P: PointTrait> {
    bounds: AABB<P>,
}

impl<P: PointTrait> std::ops::Deref for Cell<P> {
    type Target = AABB<P>;

    fn deref(&self) -> &Self::Target {
        &self.bounds
    }
}

impl<P: PointTrait> Cell<P> {
    pub fn new(bounds: AABB<P>) -> Self {
        Cell { bounds }
    }

    pub fn id(&self) -> String
    where
        <P as rstar::Point>::Scalar: num_traits::NumCast,
    {
        self.bounds
            .lower()
            .coords()
            .iter()
            .map(|c| format!("{:.9}", num_traits::cast::<P::Scalar, f64>(*c).unwrap()))
            .join("-")
    }
}

impl<P: PointTrait> RTreeObject for Cell<P> {
    type Envelope = AABB<P>;

    fn envelope(&self) -> Self::Envelope {
        self.bounds.to_owned()
    }
}

/// Spatial framework for query decomposition
#[derive(Default, Clone, Copy)]
pub struct Framework<P: PointTrait> {
    pub delta: Option<P>,
    // pub cells: RTree<Cell<P>>,
}

// impl<P: PointTrait> std::ops::Deref for Framework<P> {
//     type Target = RTree<Cell<P>>;

//     fn deref(&self) -> &Self::Target {
//         &self.cells
//     }
// }

impl<P: PointTrait> Framework<P> {
    pub fn new() -> Self {
        Framework { delta: None }
    }

    pub fn with_delta(mut self, delta: P) -> Self {
        self.delta = Some(delta);
        self
    }

    pub fn set_delta(&mut self, delta: P) {
        self.delta = Some(delta);
    }

    pub fn create_cells(&self, content: &AABB<P>) -> Vec<Cell<P>>
    where
        P::Scalar: num_traits::NumCast,
    {
        if let Some(delta) = &self.delta {
            let lower = content.lower().div(delta);
            let upper = content.upper().div(delta);

            itertools::Itertools::multi_cartesian_product(
                lower
                    .coords()
                    .iter()
                    .zip(upper.coords().iter())
                    .map(|(l, h)| {
                        num_traits::cast::<P::Scalar, f64>(*l).unwrap().floor() as i64
                            ..num_traits::cast::<P::Scalar, f64>(*h).unwrap().ceil() as i64
                    }),
            )
            .map(|c| {
                let lower = P::generate(|i| {
                    num_traits::cast::<f64, P::Scalar>(c[i] as f64).unwrap() * delta.nth(i)
                });
                let upper = P::generate(|i| {
                    num_traits::cast::<f64, P::Scalar>((c[i] + 1) as f64).unwrap() * delta.nth(i)
                });

                Cell::new(AABB::from_corners(lower, upper))
            })
            .collect()
        } else {
            vec![Cell::new(crate::aabb::cast(content))]
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::point::Point;

    use super::*;

    #[test]
    fn framework() {
        let aabb: AABB<Point<f64, 3>> = AABB::from_corners(
            Point::from_slice(&[-0.9, -0.9, -0.9]),
            Point::from_slice(&[0.9, 0.9, 0.9]),
        );

        // on cell per content
        let fw: Framework<Point<f64, 3>> = Framework::new();

        let cells = fw.create_cells(&aabb);
        assert_eq!(cells.len(), 1);

        // fixed grid
        let fw = Framework::new().with_delta(Point::from_slice(&[1., 1., 1.]));

        let cells = fw.create_cells(&aabb);
        assert_eq!(cells.len(), 8);
    }
}
