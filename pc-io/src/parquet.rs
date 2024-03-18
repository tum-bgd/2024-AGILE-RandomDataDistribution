use std::{fs::File, path::PathBuf};

use arrow::{
    compute::concat_batches,
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, parquet_to_arrow_schema},
    file::{
        metadata::ParquetMetaData, reader::FileReader, serialized_reader::SerializedFileReader,
    },
};

use crate::PointCloudReader;

pub struct ParquetReader {
    path: PathBuf,
    metadata: ParquetMetaData,
    i: usize,
}

impl<'a> PointCloudReader<'a> for ParquetReader {
    type T = Self;

    fn from_path<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
        let reader = SerializedFileReader::new(File::open(&path)?).unwrap();

        Ok(ParquetReader {
            path: path.as_ref().to_path_buf(),
            metadata: reader.metadata().to_owned(),
            i: 0,
        })
    }

    fn record_batch_reader(&'a mut self) -> Self::T {
        todo!()
    }
}

impl RecordBatchReader for ParquetReader {
    fn schema(&self) -> SchemaRef {
        let parquet_schema = self.metadata.file_metadata().schema_descr();
        let key_value_metadata = self.metadata.file_metadata().key_value_metadata();
        parquet_to_arrow_schema(parquet_schema, key_value_metadata)
            .unwrap()
            .into()
    }
}

impl Iterator for ParquetReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i < self.metadata.num_row_groups() {
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&self.path).unwrap()).unwrap();
            let reader = builder.with_row_groups(vec![self.i]).build().unwrap();
            let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
            let batch = concat_batches(&self.schema(), &batches).unwrap();
            self.i += 1;
            Some(Ok(batch))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use pc_format::{ArrowPointCloud, Point, PointCloudTrait};

    use super::*;

    #[test]
    fn read() {
        let start = std::time::Instant::now();

        let reader = ParquetReader::from_path("../data/AHN3/C_69AZ1.parquet").unwrap();

        let pc = ArrowPointCloud::from(reader);

        let duration = start.elapsed();

        println!(
            "Load {} points ({:.2} points/s)",
            pc.num_points(),
            pc.num_points() as f64 / duration.as_secs_f64()
        );

        println!("{:#?}", pc.aabb::<Point<f64, 4>>());
    }
}
