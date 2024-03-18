use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use itertools::Itertools;

use crate::PointCloudError;

/// Indexable dimension like location, time or importance.
///
/// Integer values define order.
pub const PCE_DIMENSION_KEY: &str = "PCE:dimension";

pub const PCE_LOCATION_KEY: &str = "PCE:location";
pub const PCE_IMPORTANCE_KEY: &str = "PCE:importance";

pub const PCE_OFFSET_KEY: &str = "PCE:offset";
pub const PCE_SCALE_KEY: &str = "PCE:scale";

/// extract dimensions from schema
pub fn dimensions(schema: &SchemaRef) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.metadata().contains_key(PCE_DIMENSION_KEY))
        .sorted_by_key(|(_, f)| f.metadata().get(PCE_DIMENSION_KEY))
        .map(|(i, _)| i)
        .collect_vec()
}

/// test whether schema has importance dimension
pub fn importance(schema: &SchemaRef) -> Option<usize> {
    schema
        .fields()
        .iter()
        .find_position(|f| {
            f.metadata().contains_key(PCE_DIMENSION_KEY)
                && f.metadata().contains_key(PCE_IMPORTANCE_KEY)
        })
        .map(|(i, _)| i)
}

/// add random importance
pub fn add_importance(
    schema: SchemaRef,
    name: impl Into<String>,
    data_type: DataType,
    index: usize,
) -> SchemaRef {
    let field = Field::new(name, data_type, false);

    match schema.field_with_name(field.name()) {
        Ok(_) => {
            // TODO: check consistency
            schema
        }
        Err(_) => {
            // update metadate if missing
            let mut metadata = field.metadata().to_owned();
            if !metadata.contains_key(PCE_DIMENSION_KEY) {
                metadata.insert(
                    PCE_DIMENSION_KEY.to_owned(),
                    (dimensions(&schema).len() + 1).to_string(),
                );
            }
            if !metadata.contains_key(PCE_IMPORTANCE_KEY) {
                metadata.insert(PCE_IMPORTANCE_KEY.to_owned(), "random".to_string());
            }

            let mut fields = schema.fields().to_vec();
            fields.insert(index, Arc::new(field.to_owned().with_metadata(metadata)));
            Arc::new(Schema::new(fields))
        }
    }
}

/// check for point cloud schema validity
pub fn validate(schema: &SchemaRef) -> Result<(), PointCloudError> {
    let dimensions = dimensions(schema);

    // assert schema has at least 3 dimensions
    if dimensions.len() < 3 {
        return Err(PointCloudError::SchemaError(
            "schema has at least 3 dimensions".to_string(),
        ));
    }

    // assert all dimensions have a numeric data type
    if !dimensions
        .iter()
        .all(|i| schema.field(*i).data_type().is_numeric())
    {
        return Err(PointCloudError::SchemaError(
            "schema has non numeric dimensions specified".to_string(),
        ));
    }

    Ok(())
}
