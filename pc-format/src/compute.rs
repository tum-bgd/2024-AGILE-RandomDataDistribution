use std::sync::Arc;

use arrow::{
    array::{as_primitive_array, AsArray, Float32Array, Float64Array, UInt32Array, UInt64Array},
    compute::{
        and, filter_record_batch,
        kernels::cmp::{gt_eq, lt},
        max, min,
    },
    datatypes::{DataType, Float32Type, Float64Type, Int32Type, Int64Type, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use rstar::Envelope;

use crate::{schema, PointTrait, AABB};

/// add random importance
pub fn add_importance(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    // skip if existing
    if schema.contains(&batch.schema()) {
        return Ok(batch);
    }

    match schema::importance(schema) {
        Some(i) => {
            let mut rng = SmallRng::from_entropy();
            let mut columns = batch.columns().to_vec();

            match &schema.fields[i].data_type() {
                DataType::Float32 => {
                    let mut importance = vec![0.; batch.num_rows()];
                    rng.fill(&mut importance[..]);
                    columns.insert(i, Arc::new(Float32Array::from(importance)));
                }
                DataType::Float64 => {
                    let mut importance = vec![0.; batch.num_rows()];
                    rng.fill(&mut importance[..]);
                    columns.insert(i, Arc::new(Float64Array::from(importance)));
                }
                data_type => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Unsuported data type for importance: `{data_type:?}`"
                    )))
                }
            }

            RecordBatch::try_new(schema.to_owned(), columns)
        }
        None => panic!("schema does not contain importance field"),
    }
}

/// calculate bounds
pub fn aabb<P>(batch: &RecordBatch) -> AABB<P>
where
    P: PointTrait,
    <P as rstar::Point>::Scalar: num_traits::NumCast,
{
    let aabb: AABB<P> = AABB::new_empty();

    if batch.num_rows() == 0 {
        return aabb;
    }

    let (mut lower, mut upper) = (aabb.lower(), aabb.upper());

    for (d, c) in schema::dimensions(&batch.schema())
        .iter()
        .enumerate()
        .take(P::DIMENSIONS)
    {
        let column = batch.column(*c);
        let (l, u) = match column.data_type() {
            DataType::Int32 => {
                let column = as_primitive_array(column);
                (
                    min::<Int32Type>(column).unwrap() as f64,
                    max::<Int32Type>(column).unwrap() as f64,
                )
            }
            DataType::Int64 => {
                let column = as_primitive_array(column);
                (
                    min::<Int64Type>(column).unwrap() as f64,
                    max::<Int64Type>(column).unwrap() as f64,
                )
            }
            DataType::Float32 => {
                let column = as_primitive_array(column);
                (
                    min::<Float32Type>(column).unwrap() as f64,
                    max::<Float32Type>(column).unwrap() as f64,
                )
            }
            DataType::Float64 => {
                let column = as_primitive_array(column);
                (
                    min::<Float64Type>(column).unwrap(),
                    max::<Float64Type>(column).unwrap(),
                )
            }
            _ => unimplemented!(),
        };

        *rstar::Point::nth_mut(&mut lower, d) = num_traits::cast(l).unwrap();
        *rstar::Point::nth_mut(&mut upper, d) = num_traits::cast(u).unwrap();
    }

    AABB::from_corners(lower, upper)
}

// filter by bounds
#[inline]
pub fn filter_by_aabb<P>(batch: &RecordBatch, aabb: &AABB<P>) -> RecordBatch
where
    P: PointTrait,
    <P as rstar::Point>::Scalar: num_traits::NumCast,
{
    let min = aabb.lower();
    let max = aabb.upper();

    let filter = schema::dimensions(&batch.schema())
        .iter()
        .enumerate()
        .take(P::DIMENSIONS)
        .map(|(nth, c)| match batch.schema().field(*c).data_type() {
            DataType::Float64 => {
                let array = batch.column(*c).as_primitive::<Float64Type>();
                let l = Float64Array::new_scalar(
                    num_traits::cast(rstar::Point::nth(&min, nth)).unwrap(),
                );
                let h = Float64Array::new_scalar(
                    num_traits::cast(rstar::Point::nth(&max, nth)).unwrap(),
                );
                and(&gt_eq(array, &l).unwrap(), &lt(array, &h).unwrap()).unwrap()
            }
            DataType::Float32 => {
                let array = batch.column(*c).as_primitive::<Float32Type>();
                let l = Float32Array::new_scalar(
                    num_traits::cast(rstar::Point::nth(&min, nth)).unwrap(),
                );
                let h = Float32Array::new_scalar(
                    num_traits::cast(rstar::Point::nth(&max, nth)).unwrap(),
                );
                and(&gt_eq(array, &l).unwrap(), &lt(array, &h).unwrap()).unwrap()
            }
            DataType::UInt64 => {
                let array = batch.column(*c).as_primitive::<Int64Type>();
                let l = UInt64Array::new_scalar(
                    num_traits::cast(rstar::Point::nth(&min, nth)).unwrap(),
                );
                let h = UInt64Array::new_scalar(
                    num_traits::cast(rstar::Point::nth(&max, nth)).unwrap(),
                );
                and(&gt_eq(array, &l).unwrap(), &lt(array, &h).unwrap()).unwrap()
            }
            DataType::UInt32 => {
                let array = batch.column(*c).as_primitive::<Int32Type>();
                let l = UInt32Array::new_scalar(
                    num_traits::cast(rstar::Point::nth(&min, nth)).unwrap(),
                );
                let h = UInt32Array::new_scalar(
                    num_traits::cast(rstar::Point::nth(&max, nth)).unwrap(),
                );
                and(&gt_eq(array, &l).unwrap(), &lt(array, &h).unwrap()).unwrap()
            }
            _ => unimplemented!(),
        })
        .reduce(|acc, e| and(&acc, &e).unwrap());

    filter_record_batch(batch, &filter.unwrap()).unwrap()
}
