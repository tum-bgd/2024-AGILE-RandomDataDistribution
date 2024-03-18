use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::{
    array::{
        ArrayRef, AsArray, Float32Array, Float64Array, Int16Array, Int32Array, Int8Array,
        UInt16Array, UInt32Array, UInt8Array,
    },
    datatypes::{
        DataType, Field, Float32Type, Float64Type, Int16Type, Int32Type, Schema, SchemaRef,
        UInt16Type, UInt32Type, UInt8Type,
    },
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader, RecordBatchWriter},
};
use ply_rs::ply::{
    Addable, DefaultElement, ElementDef, Encoding, Header, Ply, Property, PropertyDef,
    PropertyType, ScalarType,
};

use pc_format::schema::{PCE_DIMENSION_KEY, PCE_LOCATION_KEY};

use crate::{PointCloudReader, PointCloudWriter, DEFAULT_BATCH_SIZE};

const DEFAULT_VERTEX_ELEMENT_NAME: &str = "vertex";

/// Ply point cloud reader
pub struct PlyReader {
    reader: Box<dyn BufRead + Send>,
    header: Header,
}

impl<'a> PointCloudReader<'a> for PlyReader {
    type T = PlyRecordBatchReader;

    fn from_path<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);

        // create a parser
        let p = ply_rs::parser::Parser::<DefaultElement>::new();

        // read the header
        let header = p.read_header(&mut reader).expect("parse ply header");
        Ok(PlyReader {
            reader: Box::new(reader),
            header,
        })
    }

    fn record_batch_reader(&mut self) -> Self::T {
        self.into_iter()
    }
}

impl IntoIterator for &mut PlyReader {
    type Item = Result<RecordBatch, ArrowError>;

    type IntoIter = PlyRecordBatchReader;

    fn into_iter(self) -> Self::IntoIter {
        // create a parser
        let p = ply_rs::parser::Parser::<DefaultElement>::new();

        // get vertex element
        let element = self
            .header
            .elements
            .get(DEFAULT_VERTEX_ELEMENT_NAME)
            .expect("vertex element");

        // read payload
        let payload = p
            .read_payload_for_element(&mut self.reader, element, &self.header)
            .unwrap();

        // extract fields and columns
        let mut fields = Vec::new();
        let mut columns = Vec::new();

        for (name, property) in &element.properties {
            match &property.data_type {
                PropertyType::Scalar(s) => match s {
                    ScalarType::Char => {
                        fields.push(Field::new(name, DataType::Int8, false));

                        let column = Int8Array::from_iter(payload.iter().map(|r| match r[name] {
                            Property::Char(v) => v,
                            _ => panic!(),
                        }));
                        columns.push(Arc::new(column) as ArrayRef);
                    }
                    ScalarType::UChar => {
                        fields.push(Field::new(name, DataType::UInt8, false));

                        let column = UInt8Array::from_iter(payload.iter().map(|r| match r[name] {
                            Property::UChar(v) => v,
                            _ => panic!(),
                        }));
                        columns.push(Arc::new(column) as ArrayRef);
                    }
                    ScalarType::Short => {
                        fields.push(Field::new(name, DataType::Int16, false));

                        let column = Int16Array::from_iter(payload.iter().map(|r| match r[name] {
                            Property::Short(v) => v,
                            _ => panic!(),
                        }));
                        columns.push(Arc::new(column) as ArrayRef);
                    }
                    ScalarType::UShort => {
                        fields.push(Field::new(name, DataType::UInt16, false));

                        let column =
                            UInt16Array::from_iter(payload.iter().map(|r| match r[name] {
                                Property::UShort(v) => v,
                                _ => panic!(),
                            }));
                        columns.push(Arc::new(column) as ArrayRef);
                    }
                    ScalarType::Int => {
                        fields.push(Field::new(name.to_owned(), DataType::Int32, false));

                        let column = Int32Array::from_iter(payload.iter().map(|r| match r[name] {
                            Property::Int(v) => v,
                            _ => panic!(),
                        }));
                        columns.push(Arc::new(column) as ArrayRef);
                    }
                    ScalarType::UInt => {
                        fields.push(Field::new(name.to_owned(), DataType::UInt32, false));

                        let column =
                            UInt32Array::from_iter(payload.iter().map(|r| match r[name] {
                                Property::UInt(v) => v,
                                _ => panic!(),
                            }));
                        columns.push(Arc::new(column) as ArrayRef);
                    }
                    ScalarType::Float => {
                        fields.push(Field::new(name.to_owned(), DataType::Float32, false));

                        let column =
                            Float32Array::from_iter(payload.iter().map(|r| match r[name] {
                                Property::Float(v) => v,
                                _ => panic!(),
                            }));
                        columns.push(Arc::new(column) as ArrayRef);
                    }
                    ScalarType::Double => {
                        fields.push(Field::new(name.to_owned(), DataType::Float64, false));

                        let column =
                            Float64Array::from_iter(payload.iter().map(|r| match r[name] {
                                Property::Double(v) => v,
                                _ => panic!(),
                            }));
                        columns.push(Arc::new(column) as ArrayRef);
                    }
                },
                PropertyType::List(_, _) => unimplemented!(),
            };

            if let Some(metadata) = match name.as_str() {
                "x" => Some(HashMap::from([
                    (PCE_DIMENSION_KEY.to_owned(), "1".to_owned()),
                    (PCE_LOCATION_KEY.to_owned(), "x".to_string()),
                ])),
                "y" => Some(HashMap::from([
                    (PCE_DIMENSION_KEY.to_owned(), "2".to_owned()),
                    (PCE_LOCATION_KEY.to_owned(), "y".to_string()),
                ])),
                "z" => Some(HashMap::from([
                    (PCE_DIMENSION_KEY.to_owned(), "3".to_owned()),
                    (PCE_LOCATION_KEY.to_owned(), "z".to_string()),
                ])),
                _ => None,
            } {
                fields.last_mut().unwrap().set_metadata(metadata);
            }
        }

        let schema = Schema::new(fields);
        let batch = RecordBatch::try_new(schema.into(), columns).unwrap();

        PlyRecordBatchReader { batch, offset: 0 }
    }
}

pub struct PlyRecordBatchReader {
    batch: RecordBatch,
    offset: usize,
}

impl RecordBatchReader for PlyRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
}

impl Iterator for PlyRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset < self.batch.num_rows() {
            let length = DEFAULT_BATCH_SIZE.min(self.batch.num_rows() - self.offset);
            let batch = self.batch.slice(self.offset, length);
            self.offset += length;

            Some(Ok(batch))
        } else {
            None
        }
    }
}

/// Ply point cloud writer
pub struct PlyWriter {
    target: PathBuf,
    schema: SchemaRef,
    elements: Vec<DefaultElement>,
}

impl PlyWriter {
    pub fn new<P: AsRef<Path>>(path: P, schema: SchemaRef) -> Self {
        PlyWriter {
            target: path.as_ref().to_owned(),
            schema,
            elements: Vec::new(),
        }
    }
}

impl PointCloudWriter for PlyWriter {}

impl RecordBatchWriter for PlyWriter {
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        for i in 0..batch.num_rows() {
            let element = element_from_row(i, batch);
            self.elements.push(element);
        }
        Ok(())
    }

    fn close(self) -> Result<(), ArrowError> {
        let mut dst = File::create(self.target).unwrap();

        // crete a ply objet
        let mut ply = Ply::<DefaultElement>::new();
        ply.header.encoding = Encoding::Ascii;

        // Define the elements we want to write. When writing, the `count` will
        // be set automatically to the correct value by calling `make_consistent`.
        let element = element_definition_from_schema(&self.schema);
        ply.header.elements.add(element);

        ply.payload.insert("vertex".to_string(), self.elements);

        // create writer
        let w = ply_rs::writer::Writer::new();

        // write ply
        w.write_ply(&mut dst, &mut ply).unwrap();
        Ok(())
    }
}

fn element_definition_from_schema(schema: &Schema) -> ElementDef {
    let mut element = ElementDef::new("vertex".to_string());

    schema.fields().iter().for_each(|f| {
        let property = property_definition_from_field(f);
        element.properties.add(property);
    });

    element
}

fn property_definition_from_field(field: &Field) -> PropertyDef {
    let data_type = match field.data_type() {
        DataType::Int8 => PropertyType::Scalar(ScalarType::Char),
        DataType::Int16 => PropertyType::Scalar(ScalarType::Short),
        DataType::Int32 => PropertyType::Scalar(ScalarType::Int),
        DataType::Int64 => todo!("try cast to i32 or use list"),
        DataType::UInt8 => PropertyType::Scalar(ScalarType::UChar),
        DataType::UInt16 => PropertyType::Scalar(ScalarType::UShort),
        DataType::UInt32 => PropertyType::Scalar(ScalarType::UInt),
        DataType::UInt64 => todo!("try cast to u32 or use list"),
        DataType::Float32 => PropertyType::Scalar(ScalarType::Float),
        DataType::Float64 => PropertyType::Scalar(ScalarType::Double),
        x => unimplemented!("{x}"),
    };
    PropertyDef::new(field.name().to_owned(), data_type)
}

fn element_from_row(i: usize, batch: &RecordBatch) -> DefaultElement {
    let mut element = DefaultElement::new();
    for f in batch.schema().fields().iter() {
        let column = batch.column_by_name(f.name()).unwrap();
        let value = match f.data_type() {
            DataType::Int8 => Property::Short(column.as_primitive::<Int16Type>().value(i)),
            DataType::Int16 => Property::UShort(column.as_primitive::<UInt16Type>().value(i)),
            DataType::Int32 => Property::Int(column.as_primitive::<Int32Type>().value(i)),
            DataType::Int64 => todo!(),
            DataType::UInt8 => Property::UChar(column.as_primitive::<UInt8Type>().value(i)),
            DataType::UInt16 => Property::UShort(column.as_primitive::<UInt16Type>().value(i)),
            DataType::UInt32 => Property::UInt(column.as_primitive::<UInt32Type>().value(i)),
            DataType::UInt64 => todo!(),
            DataType::Float32 => Property::Float(column.as_primitive::<Float32Type>().value(i)),
            DataType::Float64 => Property::Double(column.as_primitive::<Float64Type>().value(i)),
            x => unimplemented!("{x}"),
        };

        element.insert(f.name().to_owned(), value);
    }

    element
}

#[cfg(test)]
mod tests {
    use rstar::Envelope;

    use pc_format::{ArrowPointCloud, Point, PointCloudTrait, PointTrait, AABB};

    use super::*;

    #[test]
    fn ascii_to_binary() {
        // read
        let mut f = std::fs::File::open("../assets/sofa.ply").unwrap();
        let p = ply_rs::parser::Parser::<ply_rs::ply::DefaultElement>::new();
        let mut ply = p.read_ply(&mut f).unwrap();
        // println!("Ply header: {:#?}", ply.header);

        // convert
        ply.header.encoding = ply_rs::ply::Encoding::BinaryLittleEndian;

        // writer
        let mut out = std::fs::File::create("../data/sofa_bin.ply").unwrap();
        let w = ply_rs::writer::Writer::new();
        w.write_ply(&mut out, &mut ply).unwrap();
    }

    #[test]
    fn read() {
        let mut reader = PlyReader::from_path("../assets/sofa.ply").unwrap();

        let pc = ArrowPointCloud::from(reader.record_batch_reader());

        assert_eq!(pc.num_points(), 10000);

        dbg!(pc.aabb::<Point<f64, 3>>());
    }

    #[test]
    fn transform() {
        let mut reader = PlyReader::from_path("../assets/sofa.ply").unwrap();

        let pc = ArrowPointCloud::from(reader.record_batch_reader());

        let aabb: AABB<Point<f64, 3>> = pc.aabb();

        // translate
        let center = aabb.center();
        let translate =
            nalgebra::Translation3::from(nalgebra::Point::from_slice(center.coords())).inverse();

        // scale
        let scaler = *aabb
            .upper()
            .sub(&aabb.lower())
            .coords()
            .iter()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();

        let scale = nalgebra::Scale3::from([2. / scaler; 3]);

        // tranform
        let pc = ArrowPointCloud::from_iter(pc.points().map(|p: Point<f64, 3>| -> Point<f64, 3> {
            let coords = scale
                .transform_point(
                    &translate.transform_point(&nalgebra::Point::from_slice(p.coords())),
                )
                .coords;
            Point::from_slice(coords.as_slice())
        }))
        .unwrap();

        // write
        let mut writer = PlyWriter::new("../data/sofa_transformed.ply", pc.schema());
        for batch in pc.store.iter().map(|e| pc.store.batches(e.key())).flatten() {
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();

        let extent: AABB<Point<f64, 3>> = pc.aabb();
        let bounds = AABB::from_corners(Point::from_slice(&[-1.; 3]), Point::from_slice(&[1.; 3]));

        assert!(bounds.contains_envelope(&extent));
    }
}
