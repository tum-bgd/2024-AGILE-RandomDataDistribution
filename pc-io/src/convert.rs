use std::path::Path;

use arrow::{
    datatypes::DataType,
    record_batch::{RecordBatchIterator, RecordBatchReader},
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding, ZstdLevel},
    file::properties::WriterProperties,
};

use pc_format::{
    compute::add_importance,
    schema::{self},
    Point,
};

use crate::{las::LasDataSource, ply::PlyReader, FormatExt, PointCloudReader};

pub type PointLocation = rstar::primitives::GeomWithData<Point<f64, 4>, u64>;

#[derive(clap::Args, Debug)]
pub struct ConversionArgs {
    /// Conversion source
    pub src: Vec<String>,
    /// Conversion destination
    #[arg(short, long)]
    pub dst: Option<String>,
    /// Create random importance
    #[arg(long)]
    pub importance: bool,
    /// Overwrite destination if exists
    #[arg(long)]
    pub overwrite: bool,
}

pub fn convert<P: AsRef<Path>>(
    src: P,
    dst: Option<P>,
    overwrite: bool,
    importance: bool,
) -> std::io::Result<()> {
    // read input
    let input = src
        .as_ref()
        .extension()
        .expect("missing extension")
        .try_into()
        .expect("known input format");

    match input {
        FormatExt::IPC => todo!(),
        FormatExt::LAS | FormatExt::LAZ => {
            let reader = LasDataSource::try_new(&[src.as_ref().to_string_lossy()]).unwrap();
            let schema = reader.schema();
            let reader = RecordBatchIterator::new(reader.record_batch_iter(), schema);

            write(src, dst, reader, overwrite, importance);
        }
        FormatExt::Parquet => todo!(),
        FormatExt::PLY => {
            let mut reader = PlyReader::from_path(&src).unwrap();
            let reader = reader.record_batch_reader();

            write(src, dst, reader, overwrite, importance);
        }
    };

    Ok(())
}

fn write<P: AsRef<Path>>(
    src: P,
    dst: Option<P>,
    reader: impl RecordBatchReader,
    overwrite: bool,
    importance: bool,
) {
    // setup writer
    let dst = dst
        .map(|d| d.as_ref().to_owned())
        .unwrap_or_else(|| src.as_ref().with_extension("parquet"));

    if dst.exists() && !overwrite {
        println!("Destination already, may use `--overwrite`");
        return;
    }

    let mut schema = reader.schema();
    if importance {
        schema = schema::add_importance(schema, "i", DataType::Float32, 0)
    }

    let file = std::fs::File::create(dst).unwrap();
    let options = WriterProperties::builder()
        // .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_column_encoding("x".into(), Encoding::PLAIN)
        .set_column_encoding("y".into(), Encoding::PLAIN)
        .set_column_encoding("z".into(), Encoding::PLAIN)
        .set_column_encoding("i".into(), Encoding::PLAIN)
        .set_column_dictionary_enabled("x".into(), false)
        .set_column_dictionary_enabled("y".into(), false)
        .set_column_dictionary_enabled("z".into(), false)
        .set_column_dictionary_enabled("i".into(), false)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.to_owned(), Some(options)).unwrap();

    for batch in reader {
        let mut batch = batch.unwrap();
        if importance {
            batch = add_importance(batch, &schema).unwrap();
        }
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
}
