use std::{
    any::Any,
    collections::HashMap,
    fmt::{Debug, Formatter},
    fs::File,
    io::BufReader,
    path::PathBuf,
    sync::Arc,
    thread,
};

use arrow::{
    array::{
        ArrayRef, BooleanArray, BooleanBuilder, Float32Array, Float32Builder, Float64Array,
        Float64Builder, StructArray, UInt16Array, UInt16Builder, UInt8Array, UInt8Builder,
    },
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};

use datafusion::{
    datasource::TableProvider,
    execution::{context::SessionState, TaskContext},
    logical_expr::{Expr, TableType},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        project_schema, stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType,
        ExecutionPlan, SendableRecordBatchStream,
    },
};
use las::{Header, Read};
use laz::{las::file::read_header_and_vlrs, laszip::ChunkTable};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

use pc_format::{
    schema::{PCE_DIMENSION_KEY, PCE_LOCATION_KEY},
    PointCloudError,
};

use crate::DEFAULT_BATCH_SIZE;

// Arrow schema for LAS points
fn schema_from_header(header: &las::Header) -> SchemaRef {
    let mut fields = vec![
        Field::new("x", DataType::Float64, false).with_metadata(HashMap::from_iter([
            (PCE_DIMENSION_KEY.to_owned(), "1".to_owned()),
            (PCE_LOCATION_KEY.to_owned(), "x".to_string()),
        ])),
        Field::new("y", DataType::Float64, false).with_metadata(HashMap::from_iter([
            (PCE_DIMENSION_KEY.to_owned(), "2".to_owned()),
            (PCE_LOCATION_KEY.to_owned(), "y".to_string()),
        ])),
        Field::new("z", DataType::Float64, false).with_metadata(HashMap::from_iter([
            (PCE_DIMENSION_KEY.to_owned(), "3".to_owned()),
            (PCE_LOCATION_KEY.to_owned(), "z".to_string()),
        ])),
        Field::new("intensity", DataType::UInt16, true),
        Field::new("return_number", DataType::UInt8, false),
        Field::new("number_of_returns", DataType::UInt8, false),
        Field::new("is_synthetic", DataType::Boolean, false),
        Field::new("is_key_point", DataType::Boolean, false),
        Field::new("is_withheld", DataType::Boolean, false),
        Field::new("is_overlap", DataType::Boolean, false),
        Field::new("scanner_channel", DataType::UInt8, false),
        // Field::new("scan_direction", DataType::UInt16, false),
        Field::new("is_edge_of_flight_line", DataType::Boolean, false),
        Field::new("classification", DataType::UInt8, false),
        Field::new("user_data", DataType::UInt8, false),
        Field::new("scan_angle", DataType::Float32, false),
        Field::new("point_source_id", DataType::UInt16, false),
    ];
    if header.point_format().has_gps_time {
        fields.push(Field::new("gps_time", DataType::Float64, false));
    }
    if header.point_format().has_color {
        fields.extend([
            Field::new("red", DataType::UInt16, false),
            Field::new("green", DataType::UInt16, false),
            Field::new("blue", DataType::UInt16, false),
        ])
    }
    if header.point_format().has_nir {
        fields.push(Field::new("nir", DataType::UInt16, false));
    }
    Arc::new(Schema::new(fields))
}

#[derive(Debug)]
struct RowBuilder {
    x: Float64Builder,
    y: Float64Builder,
    z: Float64Builder,
    intensity: UInt16Builder,
    return_number: UInt8Builder,
    number_of_returns: UInt8Builder,
    is_synthetic: BooleanBuilder,
    is_key_point: BooleanBuilder,
    is_withheld: BooleanBuilder,
    is_overlap: BooleanBuilder,
    scanner_channel: UInt8Builder,
    // scan_direction: BooleanBuilder,
    is_edge_of_flight_line: BooleanBuilder,
    classification: UInt8Builder,
    user_data: UInt8Builder,
    scan_angle: Float32Builder,
    point_source_id: UInt16Builder,
    gps_time: Float64Builder,
    red: UInt16Builder,
    green: UInt16Builder,
    blue: UInt16Builder,
    nir: UInt16Builder,
}

impl RowBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            x: Float64Array::builder(capacity),
            y: Float64Array::builder(capacity),
            z: Float64Array::builder(capacity),
            intensity: UInt16Array::builder(capacity),
            return_number: UInt8Array::builder(capacity),
            number_of_returns: UInt8Array::builder(capacity),
            is_synthetic: BooleanArray::builder(capacity),
            is_key_point: BooleanArray::builder(capacity),
            is_withheld: BooleanArray::builder(capacity),
            is_overlap: BooleanArray::builder(capacity),
            scanner_channel: UInt8Array::builder(capacity),
            // scan_direction: BooleanArray::builder(capacity),
            is_edge_of_flight_line: BooleanArray::builder(capacity),
            classification: UInt8Array::builder(capacity),
            user_data: UInt8Array::builder(capacity),
            scan_angle: Float32Array::builder(capacity),
            point_source_id: UInt16Array::builder(capacity),
            gps_time: Float64Array::builder(capacity),
            red: UInt16Array::builder(capacity),
            green: UInt16Array::builder(capacity),
            blue: UInt16Array::builder(capacity),
            nir: UInt16Array::builder(capacity),
        }
    }

    fn append(&mut self, p: las::Point, header: &las::Header) {
        // let transforms = header.transforms();

        self.x.append_value(p.x);
        self.y.append_value(p.y);
        self.z.append_value(p.z);
        // self.x_offset.append_value(transforms.x.offset);
        self.intensity.append_option(Some(p.intensity));
        self.return_number.append_value(p.return_number);
        self.number_of_returns.append_value(p.number_of_returns);
        self.is_synthetic.append_value(p.is_synthetic);
        self.is_key_point.append_value(p.is_key_point);
        self.is_withheld.append_value(p.is_withheld);
        self.is_overlap.append_value(p.is_overlap);
        self.scanner_channel.append_value(p.scanner_channel);
        // self.scan_direction.append_value(p.scan_direction());  // xxx: map to boolean or u8
        self.is_edge_of_flight_line
            .append_value(p.is_edge_of_flight_line);
        self.classification.append_value(u8::from(p.classification));
        self.user_data.append_value(p.user_data);
        self.scan_angle.append_value(p.scan_angle);
        self.point_source_id.append_value(p.point_source_id);
        if header.point_format().has_gps_time {
            self.gps_time.append_value(p.gps_time.unwrap());
        }
        if header.point_format().has_color {
            let color = p.color.unwrap();
            self.red.append_value(color.red);
            self.green.append_value(color.green);
            self.blue.append_value(color.blue);
        }
        if header.point_format().has_nir {
            self.nir.append_value(p.nir.unwrap());
        }
    }

    /// Note: returns StructArray to allow nesting within another array if desired
    fn finish(&mut self, schema: &SchemaRef, header: &las::Header) -> StructArray {
        let mut columns = vec![
            Arc::new(self.x.finish()) as ArrayRef,
            Arc::new(self.y.finish()) as ArrayRef,
            Arc::new(self.z.finish()) as ArrayRef,
            // Arc::new(self.x_offset.finish()) as ArrayRef,
            Arc::new(self.intensity.finish()) as ArrayRef,
            Arc::new(self.return_number.finish()) as ArrayRef,
            Arc::new(self.number_of_returns.finish()) as ArrayRef,
            Arc::new(self.is_synthetic.finish()) as ArrayRef,
            Arc::new(self.is_key_point.finish()) as ArrayRef,
            Arc::new(self.is_withheld.finish()) as ArrayRef,
            Arc::new(self.is_overlap.finish()) as ArrayRef,
            Arc::new(self.scanner_channel.finish()) as ArrayRef,
            // Arc::new(self.scan_direction.finish()) as ArrayRef,
            Arc::new(self.is_edge_of_flight_line.finish()) as ArrayRef,
            Arc::new(self.classification.finish()) as ArrayRef,
            Arc::new(self.user_data.finish()) as ArrayRef,
            Arc::new(self.scan_angle.finish()) as ArrayRef,
            Arc::new(self.point_source_id.finish()) as ArrayRef,
        ];
        if header.point_format().has_gps_time {
            columns.push(Arc::new(self.gps_time.finish()) as ArrayRef);
        }
        if header.point_format().has_color {
            columns.extend([
                Arc::new(self.red.finish()) as ArrayRef,
                Arc::new(self.green.finish()) as ArrayRef,
                Arc::new(self.blue.finish()) as ArrayRef,
            ]);
        }
        if header.point_format().has_nir {
            columns.push(Arc::new(self.nir.finish()) as ArrayRef);
        }
        StructArray::new(schema.fields.to_owned(), columns, None)
    }
}

// pub struct LazReader {
//     url: String,
// }

// impl PointCloudReader<'_> for LazReader {
//     type T = LasRecordBatchReader;

//     fn from_path<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
//         let url = path.as_ref().to_str().unwrap().to_owned();

//         Ok(LazReader { url })
//     }

//     fn record_batch_reader(&mut self) -> Self::T {
//         LasRecordBatchReader::new(self.url.clone())
//     }
// }

// pub struct LasRecordBatchReader {
//     url: String,
//     schema: SchemaRef,
//     header: las::Header,
//     chunk_table: ChunkTable,
//     chunk: usize,
//     offset: u64,
//     builder: RowBuilder,
// }

// impl LasRecordBatchReader {
//     fn new(url: String) -> Self {
//         let reader = las::reader::Reader::from_path(&url).unwrap();
//         let header = las::Read::header(&reader).to_owned();

//         let schema = schema_from_header(&header);

//         let mut reader = BufReader::new(File::open(&url).unwrap());
//         let (_, laz_vlr) = laz::las::file::read_header_and_vlrs(&mut reader).unwrap();
//         let laz_vlr = laz_vlr.expect("Expected a laszip VLR for laz file");
//         let chunk_table = ChunkTable::read_from(reader, &laz_vlr).unwrap();

//         let batch_size = chunk_table
//             .as_ref()
//             .iter()
//             .map(|c| c.point_count)
//             .max()
//             .unwrap();

//         Self {
//             url,
//             schema,
//             header,
//             chunk_table,
//             chunk: 0,
//             offset: 0,
//             builder: RowBuilder::new(batch_size as usize),
//         }
//     }
// }

// impl RecordBatchReader for LasRecordBatchReader {
//     fn schema(&self) -> SchemaRef {
//         self.schema.clone()
//     }
// }

// impl Iterator for LasRecordBatchReader {
//     type Item = Result<RecordBatch, ArrowError>;

//     fn next(&mut self) -> Option<Self::Item> {
//         if let Some(chunk) = self.chunk_table.as_ref().get(self.chunk) {
//             self.chunk += 1;

//             let mut reader = las::Reader::from_path(&self.url).unwrap();
//             reader.seek(self.offset).unwrap();

//             for point in reader.points().take(chunk.point_count as usize) {
//                 self.builder.append(point.unwrap(), &self.header);
//             }

//             self.offset += chunk.point_count;

//             Some(Ok(RecordBatch::from(
//                 self.builder.finish(&self.schema, &self.header),
//             )))
//         } else {
//             None
//         }
//     }
// }

/// A custom datasource, used to represent a datastore with a single index
#[derive(Clone)]
pub struct LasDataSource {
    table_paths: Vec<String>,
    table_schema: SchemaRef,
}

impl Debug for LasDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("las_files")
    }
}

impl LasDataSource {
    pub fn try_new(table_paths: &[impl AsRef<str>]) -> Result<Self, PointCloudError> {
        assert!(!table_paths.is_empty());

        let table_paths = table_paths.iter().map(|s| s.as_ref().to_owned()).collect();

        let mut table_schema = None;

        for table_path in &table_paths {
            let reader = las::Reader::from_path(table_path).unwrap();
            let header = las::Read::header(&reader);
            if let Some(schema) = table_schema.as_ref() {
                assert_eq!(schema, &schema_from_header(header));
            } else {
                table_schema = Some(schema_from_header(header));
            }
        }

        Ok(Self {
            table_paths,
            table_schema: table_schema.unwrap(),
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    fn chunk_table(&self) -> impl ParallelIterator<Item = (usize, u64, u64)> {
        self.table_paths
            .clone()
            .into_par_iter()
            .enumerate()
            .flat_map(|(i, p)| {
                let path = PathBuf::from(&p);
                let file = File::open(&path).unwrap();

                let mut reader = BufReader::new(file);
                let mut offset = 0;

                match path
                    .extension()
                    .unwrap()
                    .to_ascii_lowercase()
                    .to_str()
                    .unwrap()
                {
                    "las" => {
                        let header = las::Reader::new(&mut reader).unwrap().header().to_owned();

                        let mut total_points = header.number_of_points();

                        let chunck_size = DEFAULT_BATCH_SIZE as u64;

                        (0..total_points.div_ceil(chunck_size))
                            .map(|_| {
                                let num_points = total_points.min(chunck_size);

                                let res = (i, offset, num_points);
                                offset += chunck_size;
                                total_points -= chunck_size;
                                res
                            })
                            .collect::<Vec<_>>()
                    }
                    "laz" => {
                        let (_, laz_vlr) = read_header_and_vlrs(&mut reader).unwrap();
                        let laz_vlr = laz_vlr.expect("Expected a laszip VLR for laz file");
                        let chunk_table = ChunkTable::read_from(reader, &laz_vlr).unwrap();

                        chunk_table
                            .as_ref()
                            .iter()
                            .map(|chunk| {
                                let res = (i, offset, chunk.point_count);
                                offset += chunk.point_count;
                                res
                            })
                            .collect::<Vec<_>>()
                    }
                    ext => {
                        eprintln!("Unsupported extension `{ext}`");
                        Vec::new()
                    }
                }
            })
    }

    pub fn par_record_batch_iter(
        &self,
    ) -> impl ParallelIterator<Item = Result<RecordBatch, ArrowError>> {
        let schema = self.schema();
        let table_meta: Vec<(String, Header)> = self
            .table_paths
            .par_iter()
            .map(|p| {
                let reader = las::Reader::from_path(p).unwrap();
                (p.to_owned(), las::Read::header(&reader).to_owned())
            })
            .collect();

        self.chunk_table().map(move |(i, offset, point_count)| {
            let mut reader = las::Reader::from_path(table_meta[i].0.as_str()).unwrap();
            reader.seek(offset).unwrap();

            let mut builder = RowBuilder::new(point_count as usize);
            for point in reader.points().take(point_count as usize) {
                builder.append(point.unwrap(), &table_meta[i].1);
            }

            Ok(RecordBatch::from(builder.finish(&schema, &table_meta[i].1)))
        })
    }

    pub fn record_batch_iter(&self) -> impl Iterator<Item = Result<RecordBatch, ArrowError>> {
        into_iter(self.par_record_batch_iter())
    }
}

#[async_trait::async_trait]
impl TableProvider for LasDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LasExec::new(self.clone(), projection)))
    }
}

#[derive(Debug)]
struct LasExec {
    laz_source: LasDataSource,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
}

impl LasExec {
    fn new(laz_source: LasDataSource, projection: Option<&Vec<usize>>) -> Self {
        let projected_schema = project_schema(&laz_source.schema(), projection).unwrap();
        Self {
            laz_source,
            projection: projection.cloned(),
            projected_schema,
        }
    }
}

impl DisplayAs for LasExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LasExec")
    }
}

impl ExecutionPlan for LasExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        // dbg!(_context);
        let projection = self.projection.clone();
        let stream = RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::iter(into_iter(self.laz_source.par_record_batch_iter().map(
                move |b| {
                    match &projection {
                        Some(indices) => b.unwrap().project(indices),
                        None => b,
                    }
                    .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))
                },
            ))),
        );

        Ok(Box::pin(stream))
    }
}

fn into_iter<T: Send + 'static>(
    iter: impl ParallelIterator<Item = T> + 'static,
) -> impl Iterator<Item = T> {
    let (send, recv) = std::sync::mpsc::sync_channel(16);
    thread::spawn(move || {
        iter.for_each(|el| {
            let _ = send.send(el);
        })
    });
    recv.into_iter()
}

#[cfg(test)]
mod tests {

    use pc_format::{ArrowPointCloud, Point, PointCloudTrait};

    use super::*;

    #[test]
    fn count_points() {
        let filenames = [
            "C_69AZ1", "C_69AZ2", "C_69BZ1", "C_69AN1", "C_69AN2", "C_69BN1", "C_69CN2", "C_69DN1",
        ];

        let mut total_points = 0;

        for filename in filenames {
            let reader = las::Reader::from_path(format!("../data/AHN4/{filename}.LAZ")).unwrap();
            let header = las::Read::header(&reader);
            let num_points = header.number_of_points();
            println!("Number of Points ({filename}): {num_points}",);
            total_points += num_points;
        }

        println!("Total number of points: {total_points}")
    }

    #[test]
    fn baseline() {
        let mut reader =
            las::Reader::from_path("../data/Vaihingen/ALS/Vaihingen_Strip_03.LAS").unwrap();

        // header
        let header = las::Read::header(&reader);
        println!("Version: {} - {}", header.version(), header.point_format());
        println!("Number of Points: {}", header.number_of_points());
        // println!("{header:#?}");

        // points
        let start = std::time::Instant::now();
        let points: Vec<_> = las::Read::points(&mut reader)
            .map(|p| {
                let p = p.unwrap();
                [p.x, p.y, p.z]
            })
            .collect();
        println!(
            "Loaded {} points ({:.2} points/s)",
            points.len(),
            points.len() as f64 / start.elapsed().as_secs_f64()
        );
    }

    #[test]
    fn record_batch_reader() {
        let start = std::time::Instant::now();

        let ds = LasDataSource::try_new(&["../data/Vaihingen/ALS/Vaihingen_Strip_03.LAS"]).unwrap();

        let mut pc = ArrowPointCloud::try_new(ds.schema()).unwrap();
        for batch in ds.record_batch_iter() {
            pc.append(batch.unwrap()).unwrap();
        }

        let duration = start.elapsed();

        println!(
            "Load {} points ({:.2} points/s)",
            pc.num_points(),
            pc.num_points() as f64 / duration.as_secs_f64()
        );

        println!("{:#?}", pc.aabb::<Point<f64, 4>>());
    }

    #[tokio::test]
    async fn sql() {
        let start = std::time::Instant::now();

        let ds = LasDataSource::try_new(&["../data/Vaihingen/ALS/Vaihingen_Strip_03.LAS"]).unwrap();

        let ctx = datafusion::execution::context::SessionContext::new();

        ctx.register_table("laz", Arc::new(ds)).unwrap();

        let results = ctx
            .sql("SELECT * FROM 'laz'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // println!(
        //     "{}",
        //     arrow::util::pretty::pretty_format_batches(&results)
        //         .unwrap()
        //         .to_string()
        // );

        let duration = start.elapsed();

        let num_points: usize = results.iter().map(|b| b.num_rows()).sum();

        println!(
            "Load {} points ({:.2} points/s)",
            num_points,
            num_points as f64 / duration.as_secs_f64()
        );

        // aggregation
        let results = ctx
            .sql("SELECT mean(x) FROM 'laz'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        println!(
            "{}",
            arrow::util::pretty::pretty_format_batches(&results)
                .unwrap()
                .to_string()
        );
    }
}
