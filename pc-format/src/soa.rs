use std::{
    fs::File,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use ahash::RandomState;
use arrow::{
    array::{
        as_primitive_array, ArrayBuilder, ArrayRef, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder,
    },
    datatypes::{DataType, Float32Type, Float64Type, Int32Type, Int64Type, SchemaRef},
    ipc::{
        reader::FileReader,
        writer::{FileWriter, IpcWriteOptions},
        CompressionType,
    },
    record_batch::{RecordBatch, RecordBatchReader},
};
use dashmap::DashMap;
use moka::{notification::RemovalCause, sync::Cache};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rstar::{primitives::GeomWithData, Envelope, RStarInsertionStrategy, RTree, RTreeParams};
use uuid::Uuid;

use crate::{
    compute::{aabb, filter_by_aabb},
    schema::{dimensions, validate},
    Framework, Point, PointCloudError, PointCloudTrait, PointTrait, AABB,
};

/// Point cloud data store
#[derive(Clone)]
pub struct PointCloudStore {
    pub dir: PathBuf,
    store: Arc<DashMap<String, PathBuf, RandomState>>,
    cache: Cache<String, Arc<RwLock<Vec<RecordBatch>>>, RandomState>,
}

impl Deref for PointCloudStore {
    type Target = DashMap<String, PathBuf, RandomState>;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl PointCloudStore {
    pub fn try_new<P: AsRef<Path>>(
        capacity: u64,
        dir: P,
        compress: bool,
    ) -> Result<Self, PointCloudError> {
        let dir = dir.as_ref().to_path_buf();
        if !&dir.is_dir() {
            match std::fs::create_dir(dir.as_path()) {
                Ok(_) => (),
                Err(_) => {
                    return Err(PointCloudError::CacheError(format!(
                        "Failed to create store directory: {dir:?}"
                    )))
                }
            }
        }

        let eviction_dir = dir.clone();

        let cache = Cache::builder()
            .max_capacity(capacity)
            // .time_to_idle(Duration::from_secs(5))
            // .weigher(|_: &String, v: &Arc<RwLock<Vec<RecordBatch>>>| {
            //     v.read()
            //         .unwrap()
            //         .iter()
            //         .map(|batch| batch.num_rows())
            //         .sum::<usize>() as u32
            // })
            .eviction_listener(move |k, v: Arc<RwLock<Vec<RecordBatch>>>, cause| {
                let batches = v.read().unwrap();

                // do not spill on replacement
                if cause == RemovalCause::Replaced || batches.is_empty() {
                    return;
                }

                // splill
                let path = format!("{}/{k}.arrow", eviction_dir.to_string_lossy());
                let file = File::create(path).expect("failed to create file");

                let schema = batches[0].schema();

                let options = if compress {
                    IpcWriteOptions::default()
                        .try_with_compression(Some(CompressionType::ZSTD))
                        .expect("failed to set compression option")
                } else {
                    IpcWriteOptions::default()
                };

                let mut writer = FileWriter::try_new_with_options(file, &schema, options)
                    .expect("failed to create writer");

                for batch in batches.iter() {
                    writer.write(batch).expect("failed to write batch");
                }

                writer.finish().expect("failed to finish writer");
            })
            .build_with_hasher(RandomState::default());

        Ok(Self {
            dir,
            store: Arc::new(DashMap::default()),
            cache,
        })
    }

    pub fn batches(&self, key: &str) -> Vec<RecordBatch> {
        if let Some(v) = self.cache.get(key) {
            v.read().unwrap().to_owned()
        } else {
            self.store
                .get(key)
                .map(|path| {
                    let file = File::open(path.as_path()).unwrap();
                    let reader = FileReader::try_new(file, None).unwrap();
                    // XXX: cache?
                    reader.map(Result::unwrap).collect()
                })
                .unwrap()
        }
    }

    pub fn push(&self, id: String, batch: RecordBatch) {
        // create store entry if missing
        self.store
            .entry(id.clone())
            .or_insert_with(|| self.dir.clone().join(format!("{id}.arrow")));
        // insert batch
        self.cache
            .entry(id.clone())
            .or_insert_with(|| {
                let path = self.store.get(&id).unwrap();
                if path.exists() {
                    eprintln!("CACHE MISS!!!");
                    let file = File::open(path.as_path()).unwrap();
                    let reader = FileReader::try_new(file, None).unwrap();
                    let batches = reader.map(Result::unwrap).collect();
                    Arc::new(RwLock::new(batches))
                } else {
                    Arc::new(RwLock::new(Vec::new()))
                }
            })
            .value()
            .write()
            .unwrap()
            .push(batch);
    }
}

/// Point cloud data index
pub enum Index {
    Point(PointIndex),
    Batch(BatchIndex),
    Multi(MultiLevelIndex),
    None,
}

pub struct PointCloudParams;

impl RTreeParams for PointCloudParams {
    const MIN_SIZE: usize = 4;
    const MAX_SIZE: usize = 16;
    const REINSERTION_COUNT: usize = 2;
    type DefaultInsertionStrategy = RStarInsertionStrategy;
}

pub type PointIndex = RTree<GeomWithData<Point<f64, 4>, u64>, PointCloudParams>;
pub type BatchIndex = RTree<GeomWithData<AABB<Point<f64, 4>>, String>, PointCloudParams>;
pub type MultiLevelIndex = RTree<GeomWithData<AABB<Point<f64, 4>>, (String, PointIndex)>>;

/// Point cloud
pub struct ArrowPointCloud {
    pub schema: SchemaRef,
    pub store: PointCloudStore,
    pub index: Index,
    framework: Framework<Point<f64, 4>>,
}

impl ArrowPointCloud {
    pub fn try_new(schema: SchemaRef) -> Result<Self, PointCloudError> {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path().to_string_lossy().to_string();

        let store = PointCloudStore::try_new(u64::MAX, dir, false)?;

        ArrowPointCloud::try_new_with(schema, store)
    }

    pub fn try_new_with(
        schema: SchemaRef,
        store: PointCloudStore,
    ) -> Result<Self, PointCloudError> {
        validate(&schema)?;

        Ok(Self {
            schema,
            store,
            index: Index::None,
            framework: Framework::new(),
        })
    }

    pub fn append(&mut self, batch: RecordBatch) -> Result<(), PointCloudError> {
        let batch = batch.with_schema(self.schema())?;

        let aabb: AABB<Point<f64, 4>> = aabb(&batch);

        if self.framework.delta.is_some() {
            // create missing cells
            let cells = self.framework.create_cells(&aabb);

            for cell in cells {
                // get points for cell
                let partition = filter_by_aabb(&batch, &cell);

                // insert records
                if partition.num_rows() != 0 {
                    let id = cell.id();
                    self.store.push(id, partition);
                }
            }
        } else {
            self.store.push(Uuid::new_v4().to_string(), batch);
        };

        Ok(())
    }

    pub fn flush(&self) {
        self.store.cache.invalidate_all();
        self.store.cache.run_pending_tasks();
        self.store.shrink_to_fit();
    }
}

impl PointCloudTrait for ArrowPointCloud {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn num_points(&self) -> usize {
        self.store
            .par_iter()
            .flat_map_iter(|e| {
                self.store
                    .batches(e.key())
                    .iter()
                    .map(|batch| batch.num_rows())
                    .collect::<Vec<_>>()
            })
            .sum()
    }

    fn points<'a, P>(&'a self) -> Box<dyn Iterator<Item = P> + 'a>
    where
        P: PointTrait + 'a,
        <P as rstar::Point>::Scalar: num_traits::NumCast,
    {
        let dimensions = dimensions(&self.schema());
        let d = dimensions.len().min(P::DIMENSIONS);
        let iter = self.store.iter().flat_map(move |e| {
            self.store
                .batches(e.key())
                .iter()
                .flat_map(|batch| match P::data_type() {
                    DataType::Int32 => {
                        let columns: Vec<_> = (0..d)
                            .map(|j| {
                                let column = batch.column(dimensions[j]);
                                let column =
                                    arrow::compute::cast(&column, &P::data_type()).unwrap();
                                as_primitive_array::<Int32Type>(&column).to_owned()
                            })
                            .collect();

                        (0..batch.num_rows())
                            .map(|i| {
                                P::generate(|nth| {
                                    columns
                                        .get(nth)
                                        .and_then(|column| num_traits::cast(column.value(i)))
                                        .unwrap_or_else(num_traits::zero)
                                })
                            })
                            .collect::<Vec<P>>()
                    }
                    DataType::Int64 => {
                        let columns: Vec<_> = (0..d)
                            .map(|j| {
                                let column = batch.column(dimensions[j]);
                                let column =
                                    arrow::compute::cast(&column, &P::data_type()).unwrap();
                                as_primitive_array::<Int64Type>(&column).to_owned()
                            })
                            .collect();

                        (0..batch.num_rows())
                            .map(|i| {
                                P::generate(|nth| {
                                    columns
                                        .get(nth)
                                        .and_then(|column| num_traits::cast(column.value(i)))
                                        .unwrap_or_else(num_traits::zero)
                                })
                            })
                            .collect()
                    }
                    DataType::Float32 => {
                        let columns: Vec<_> = (0..d)
                            .map(|j| {
                                let column = batch.column(dimensions[j]);
                                let column =
                                    arrow::compute::cast(&column, &P::data_type()).unwrap();
                                as_primitive_array::<Float32Type>(&column).to_owned()
                            })
                            .collect();

                        (0..batch.num_rows())
                            .map(|i| {
                                P::generate(|nth| {
                                    columns
                                        .get(nth)
                                        .and_then(|column| num_traits::cast(column.value(i)))
                                        .unwrap_or_else(num_traits::zero)
                                })
                            })
                            .collect()
                    }
                    DataType::Float64 => {
                        let columns: Vec<_> = (0..d)
                            .map(|j| {
                                let column = batch.column(dimensions[j]);
                                let column =
                                    arrow::compute::cast(&column, &P::data_type()).unwrap();
                                as_primitive_array::<Float64Type>(&column).to_owned()
                            })
                            .collect();

                        (0..batch.num_rows())
                            .map(|i| {
                                P::generate(|nth| {
                                    columns
                                        .get(nth)
                                        .and_then(|column| num_traits::cast(column.value(i)))
                                        .unwrap_or_else(num_traits::zero)
                                })
                            })
                            .collect()
                    }
                    _ => unimplemented!(),
                })
                .collect::<Vec<_>>()
        });

        Box::new(iter)
    }

    fn aabb<P>(&self) -> AABB<P>
    where
        P: PointTrait,
        <P as rstar::Point>::Scalar: num_traits::NumCast,
    {
        self.store
            .par_iter()
            .map(|e| {
                self.store
                    .batches(e.key())
                    .iter()
                    .fold(AABB::new_empty(), |acc, batch| acc.merged(&aabb(batch)))
            })
            .reduce(AABB::new_empty, |a, b| a.merged(&b))
    }

    fn from_iter<P>(iter: impl Iterator<Item = P>) -> Result<Self, PointCloudError>
    where
        P: PointTrait,
        <P as rstar::Point>::Scalar: num_traits::NumCast,
    {
        let schema = P::schema();
        validate(&schema)?;

        let mut data_builders: Vec<Box<dyn ArrayBuilder>> = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Int32 => Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
                DataType::Int64 => Box::new(Int64Builder::new()),
                DataType::Float32 => Box::new(Float32Builder::new()),
                DataType::Float64 => Box::new(Float64Builder::new()),
                x => unimplemented!("{x}"),
            })
            .collect();

        for p in iter.into_iter() {
            for (i, f) in schema.fields().iter().enumerate() {
                match f.data_type() {
                    DataType::Int32 => data_builders[i]
                        .as_any_mut()
                        .downcast_mut::<Int32Builder>()
                        .unwrap()
                        .append_value(num_traits::cast(p.nth(i)).unwrap()),
                    DataType::Int64 => data_builders[i]
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .unwrap()
                        .append_value(num_traits::cast(p.nth(i)).unwrap()),
                    DataType::Float32 => data_builders[i]
                        .as_any_mut()
                        .downcast_mut::<Float32Builder>()
                        .unwrap()
                        .append_value(num_traits::cast(p.nth(i)).unwrap()),
                    DataType::Float64 => data_builders[i]
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .unwrap()
                        .append_value(num_traits::cast(p.nth(i)).unwrap()),
                    _ => unimplemented!(),
                }
            }
        }

        let array_refs: Vec<ArrayRef> = data_builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), array_refs)?;

        let mut pc = Self::try_new(schema).unwrap();
        pc.append(batch)?;

        Ok(pc)
    }
}

impl<RBR> From<RBR> for ArrowPointCloud
where
    RBR: RecordBatchReader,
{
    fn from(value: RBR) -> Self {
        let schema = value.schema();

        let mut pc = Self::try_new(schema).unwrap();

        for batch in value {
            pc.append(batch.unwrap()).unwrap();
        }

        pc
    }
}
