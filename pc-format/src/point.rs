use std::{
    collections::HashMap,
    fmt::Debug,
    ops::{Add, Div, Mul, Sub},
};

use arrow::datatypes::{ArrowNativeType, DataType, Field, Fields, Schema, SchemaRef};
use num_traits::{Bounded, Num, NumCast, Signed, Zero};

use crate::schema::{PCE_DIMENSION_KEY, PCE_IMPORTANCE_KEY, PCE_LOCATION_KEY};

/// Coordinate trait
pub trait Coord
where
    Self: Bounded + Num + NumCast + Signed + PartialOrd + Clone + Copy + Debug + ArrowNativeType,
{
    const DATA_TYPE: DataType;
}

// Coordinate trait implementations
impl Coord for f64 {
    const DATA_TYPE: DataType = DataType::Float64;
}
impl Coord for f32 {
    const DATA_TYPE: DataType = DataType::Float32;
}
impl Coord for i64 {
    const DATA_TYPE: DataType = DataType::Int64;
}
impl Coord for i32 {
    const DATA_TYPE: DataType = DataType::Int32;
}

/// Point trait
pub trait PointTrait: rstar::Point + rstar::RTreeObject + std::marker::Send {
    fn data_type() -> DataType;

    fn schema() -> SchemaRef;

    #[inline]
    fn from_slice(components: &[Self::Scalar]) -> Self {
        Self::generate(|i| {
            components
                .get(i)
                .cloned()
                .unwrap_or_else(Self::Scalar::zero)
        })
    }

    fn coords(&self) -> &[Self::Scalar];

    fn x(&self) -> Self::Scalar {
        if Self::DIMENSIONS > 0 {
            self.nth(0)
        } else {
            Self::Scalar::zero()
        }
    }

    fn y(&self) -> Self::Scalar {
        if Self::DIMENSIONS > 1 {
            self.nth(1)
        } else {
            Self::Scalar::zero()
        }
    }

    fn z(&self) -> Self::Scalar {
        if Self::DIMENSIONS > 2 {
            self.nth(2)
        } else {
            Self::Scalar::zero()
        }
    }

    fn i(&self) -> Self::Scalar {
        if Self::DIMENSIONS > 3 {
            self.nth(3)
        } else {
            Self::Scalar::zero()
        }
    }

    fn add(&self, other: &Self) -> Self {
        Self::generate(|i| self.nth(i).add(other.nth(i)))
    }

    fn sub(&self, other: &Self) -> Self {
        Self::generate(|i| self.nth(i).sub(other.nth(i)))
    }

    fn mul(&self, other: &Self) -> Self {
        Self::generate(|i| self.nth(i).mul(other.nth(i)))
    }

    fn div(&self, other: &Self) -> Self {
        Self::generate(|i| self.nth(i).div(other.nth(i)))
    }
}

/// Convert between different point traits
#[inline]
pub fn cast<P: PointTrait, Q: PointTrait>(p: &P) -> Q
where
    P::Scalar: NumCast,
    Q::Scalar: NumCast,
{
    Q::generate(|i| {
        if i < P::DIMENSIONS {
            num_traits::cast(p.nth(i)).unwrap()
        } else {
            Q::Scalar::zero()
        }
    })
}

/// Point
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Point<T, const D: usize> {
    location: [T; D],
}

impl<T: Coord, const D: usize> PointTrait for Point<T, D> {
    fn data_type() -> DataType {
        T::DATA_TYPE
    }

    fn schema() -> SchemaRef {
        Schema::new(Fields::from_iter((1..=D).map(|i| {
            let name = match i {
                1 => "x".to_string(),
                2 => "y".to_string(),
                3 => "z".to_string(),
                4 => "i".to_string(),
                _ => unimplemented!("higher dimensions unreachable"),
            };
            let mut field = Field::new(name, T::DATA_TYPE, false);

            field.set_metadata(HashMap::from(if field.name() == "i" {
                [
                    (PCE_DIMENSION_KEY.to_owned(), i.to_string()),
                    (PCE_IMPORTANCE_KEY.to_owned(), "unknown".to_string()),
                ]
            } else {
                [
                    (PCE_DIMENSION_KEY.to_owned(), i.to_string()),
                    (PCE_LOCATION_KEY.to_owned(), field.name().to_string()),
                ]
            }));
            field
        })))
        .into()
    }

    fn coords(&self) -> &[T] {
        &self.location
    }
}

impl<T: Coord, const D: usize> rstar::Point for Point<T, D> {
    type Scalar = T;

    const DIMENSIONS: usize = D;

    #[inline]
    fn generate(mut generator: impl FnMut(usize) -> Self::Scalar) -> Self {
        let mut i = 0;
        let coords = [(); D].map(|_| {
            let res = generator(i);
            i += 1;
            res
        });

        Point { location: coords }
    }

    fn nth(&self, index: usize) -> Self::Scalar {
        self.location[index]
    }

    fn nth_mut(&mut self, index: usize) -> &mut Self::Scalar {
        &mut self.location[index]
    }
}

#[cfg(test)]
mod tests {
    use crate::{aos::VecPointCloud, pointcloud::PointCloudTrait};

    use super::*;

    #[test]
    fn schema() {
        let pc: VecPointCloud<Point<i32, 2>> =
            VecPointCloud::from_iter([Point::<i32, 2>::from_slice(&[1, 1])].into_iter()).unwrap();
        dbg!(pc.schema());
    }

    #[test]
    fn slice() {
        let p: Point<f64, 2> = Point::from_slice(&[1.; 3]);
        assert_eq!(p, Point::from_slice(&[1.; 2]));

        let p: Point<f64, 4> = Point::from_slice(&[1.; 3]);
        assert_eq!(p, Point::from_slice(&[1., 1., 1., 0.]));
    }

    #[test]
    fn cast() {
        let p1: Point<f64, 3> = Point::from_slice(&[1.; 3]);

        let p2: Point<i64, 3> = Point::from_slice(&[1; 3]);
        assert_eq!(super::cast::<Point<f64, 3>, Point<i64, 3>>(&p1), p2);

        let p2: Point<i64, 2> = Point::from_slice(&[1; 2]);
        assert_eq!(super::cast::<Point<f64, 3>, Point<i64, 2>>(&p1), p2);

        let p2: Point<i64, 4> = Point::from_slice(&[1, 1, 1, 0]);
        assert_eq!(super::cast::<Point<f64, 3>, Point<i64, 4>>(&p1), p2);
    }

    #[test]
    fn ops() {
        let one: Point<i64, 3> = Point::from_slice(&[1; 3]);
        let two: Point<i64, 3> = Point::from_slice(&[2; 3]);
        let three: Point<i64, 3> = Point::from_slice(&[3; 3]);
        let four: Point<i64, 3> = Point::from_slice(&[4; 3]);

        assert_eq!(one.add(&one), two);
        assert_eq!(three.sub(&one), two);
        assert_eq!(two.mul(&two), four);
        assert_eq!(four.div(&two), two);
    }

    #[test]
    fn components() {
        let p: Point<i64, 4> = Point::from_slice(&[1, 2, 3, 4]);

        assert_eq!(p.x(), 1);
        assert_eq!(p.y(), 2);
        assert_eq!(p.z(), 3);
        assert_eq!(p.i(), 4);
    }
}
