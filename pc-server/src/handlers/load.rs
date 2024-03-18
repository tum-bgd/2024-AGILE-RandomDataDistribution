use anyhow::Context;
use arrow::{
    array::BooleanArray,
    compute::filter_record_batch,
    datatypes::DataType,
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use axum::{
    body::Bytes,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension,
};
use rand::{
    distributions::{Standard, Uniform},
    rngs::SmallRng,
    Rng, SeedableRng,
};
use rayon::iter::ParallelIterator;
use serde::{Deserialize, Serialize};
use serde_with::{formats::CommaSeparator, serde_as, StringWithSeparator};
use tokio::{runtime::Handle, task::JoinSet};

use pc_format::{
    compute::{aabb, filter_by_aabb},
    soa::PointCloudStore,
    ArrowPointCloud, Framework, Point, PointTrait,
};
use pc_io::las::LasDataSource;

use crate::{error::AppError, state::SharedState, Qs};

#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct LoadRequest {
    collection: Option<String>,
    #[serde(default)]
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    uris: Vec<String>,
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, f64>>")]
    delta: Option<Vec<f64>>,
    from: Option<f64>,
    to: Option<f64>,
    workers: Option<Vec<String>>,
    store: Option<String>,
    #[serde(default)]
    compress: bool,
}

#[axum::debug_handler]
pub(crate) async fn load(
    Extension(state): Extension<SharedState>,
    Qs(mut query): Qs<LoadRequest>,
) -> Result<Response, AppError> {
    // Set default collection (FIXME: should be required)
    query.collection.get_or_insert("default".to_string());

    tracing::info!("{:#?}", &query);

    // Distribute or load
    match query.workers {
        None => {
            if state.read().await.workers.is_empty() {
                // load fraction
                load_fraction(&query, &state).await?;
            } else {
                // distribute the dataset for load among all workers
                distribute_load(&mut query, &state).await?;
            }
        }
        Some(ref workers) => {
            // load and distribute fractions among all workers
            let client = reqwest::Client::new();

            let mut set = JoinSet::new();

            for uri in &query.uris {
                tracing::info!("Processing uri {}", &uri);

                let path = std::path::Path::new(&uri).to_owned();

                // check if path exists
                if !path.exists() {
                    tracing::warn!("File does not exist: {path:?}");
                    return Err(AppError::NotFound);
                }

                // read points from file
                let reader = LasDataSource::try_new(&[uri]).unwrap();

                let schema = reader.schema();

                let step = Uniform::new(0, workers.len());

                for batch in reader.record_batch_iter() {
                    let batch = batch.unwrap();

                    let fraction: Vec<usize> = SmallRng::seed_from_u64(0)
                        .sample_iter(step)
                        .take(batch.num_rows())
                        .collect();

                    for (i, url) in workers.iter().enumerate() {
                        let filter =
                            BooleanArray::from_iter(fraction.iter().map(|f| Some(f == &i)));
                        let selection = filter_record_batch(&batch, &filter).unwrap();

                        let mut writer = StreamWriter::try_new(Vec::new(), &schema)
                            .context("Create stream writer")?;
                        writer.write(&selection).context("Write batch to stream")?;
                        writer.finish().context("Finish stream")?;

                        let buffer = writer.into_inner().unwrap();

                        let url = format!("{}/load?{}", url, serde_qs::to_string(&query).unwrap());

                        set.spawn(client.post(url).body(buffer).send());
                    }

                    while let Some(res) = set.join_next().await {
                        let out = res.unwrap();
                        out.unwrap().error_for_status().unwrap();
                    }
                }
            }
        }
    }

    Ok(StatusCode::OK.into_response())
}

async fn distribute_load(
    query: &mut LoadRequest,
    state: &SharedState,
) -> Result<Response, AppError> {
    let workers = state.read().await.workers.to_owned();
    tracing::debug!("Distribute load among {} workers", workers.len());

    let uris = query.uris.to_owned();

    // XXX: load by message passing
    if false {
        query.workers = Some(workers.clone());
    }

    let fraction = query.from.unwrap_or(0.)..query.to.unwrap_or(1.);
    let fraction_width = (fraction.end - fraction.start) / workers.len() as f64;

    let mut set = JoinSet::new();

    for (i, url) in workers.iter().enumerate() {
        // XXX: load by message passing
        if false {
            // select some files
            query.uris = uris.iter().skip(i).step_by(uris.len()).cloned().collect();

            if query.uris.is_empty() {
                continue;
            }
        } else {
            // define fraction
            query.from = Some(fraction.start + fraction_width * i as f64);
            query.to = Some(if i == workers.len() - 1 {
                fraction.end
            } else {
                fraction.start + fraction_width * (i + 1) as f64
            });
        }

        let url = format!("{}/load?{}", url, serde_qs::to_string(&query).unwrap());
        set.spawn(reqwest::get(url));
    }

    while let Some(res) = set.join_next().await {
        let out = res.unwrap();
        out.unwrap().error_for_status().unwrap();
    }

    Ok(StatusCode::OK.into_response())
}

async fn load_fraction(query: &LoadRequest, state: &SharedState) -> Result<Response, AppError> {
    for uri in &query.uris {
        tracing::info!("Processing uri {}", &uri);

        let path = std::path::Path::new(&uri).to_owned();

        // check if path exists
        if !path.exists() {
            tracing::warn!("File does not exist: {path:?}");
            return Err(AppError::NotFound);
        }

        // load points from file
        let reader = LasDataSource::try_new(&[uri]).unwrap();

        let schema = pc_format::schema::add_importance(reader.schema(), "i", DataType::Float32, 0);

        // extract fraction
        let fraction = query
            .from
            .map(|_| query.from.unwrap_or(0.)..query.to.unwrap_or(1.));
        tracing::info!("Set fraction to {fraction:?}");

        let handle = Handle::current();
        reader.par_record_batch_iter().for_each(|batch| {
            let mut batch = batch.unwrap();
            // apply fraction
            if let Some(fraction) = &fraction {
                // generate random fraction value
                let f: Vec<bool> = SmallRng::seed_from_u64(0)
                    .sample_iter(Standard)
                    .take(batch.num_rows())
                    .map(|f| fraction.contains(&f))
                    .collect();

                batch = filter_record_batch(&batch, &f.into()).unwrap();
            };

            // add importance
            batch = pc_format::compute::add_importance(batch, &schema).unwrap();

            handle.block_on(insert_batch(batch, query, state));
        });
    }

    if query.store.is_some() {
        state
            .read()
            .await
            .data
            .get(query.collection.as_ref().unwrap())
            .unwrap()
            .flush();
    }

    Ok(StatusCode::OK.into_response())
}

async fn insert_batch(batch: RecordBatch, query: &LoadRequest, state: &SharedState) {
    // set delta
    if let Some(delta) = &query.delta {
        let delta = Point::from_slice(delta);
        let framework: Framework<Point<f64, 4>> = Framework::new().with_delta(delta);

        for cell in framework.create_cells(&aabb(&batch)) {
            // get points for cell
            let partition = filter_by_aabb(&batch, &cell);

            if partition.num_rows() == 0 {
                continue;
            }

            state
                .write()
                .await
                .data
                .entry(query.collection.clone().unwrap())
                .or_insert_with(|| {
                    if let Some(store) = query.store.as_ref() {
                        let capacity = 1000; // Cells
                        let store =
                            PointCloudStore::try_new(capacity, store, query.compress).unwrap();
                        ArrowPointCloud::try_new_with(partition.schema(), store).unwrap()
                    } else {
                        ArrowPointCloud::try_new(partition.schema()).unwrap()
                    }
                })
                .store
                .push(cell.id(), partition);
        }
    } else {
        state
            .write()
            .await
            .data
            .entry(query.collection.clone().unwrap())
            .or_insert_with(|| {
                if let Some(store) = query.store.as_ref() {
                    let capacity = 1000; // Cells
                    let store = PointCloudStore::try_new(capacity, store, query.compress).unwrap();
                    ArrowPointCloud::try_new_with(batch.schema(), store).unwrap()
                } else {
                    ArrowPointCloud::try_new(batch.schema()).unwrap()
                }
            })
            .store
            .push(uuid::Uuid::new_v4().to_string(), batch);
    }
}

pub(crate) async fn push_batch(
    Qs(query): Qs<LoadRequest>,
    Extension(state): Extension<SharedState>,
    body: Bytes,
) {
    let reader = StreamReader::try_new(std::io::Cursor::new(body), None).unwrap();

    let schema = pc_format::schema::add_importance(reader.schema(), "i", DataType::Float32, 0);

    for batch in reader {
        let batch = pc_format::compute::add_importance(batch.unwrap(), &schema).unwrap();
        insert_batch(batch, &query, &state).await;
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct DeleteRequest {
    collection: Option<String>,
}

#[axum::debug_handler]
pub(crate) async fn delete(
    Extension(state): Extension<SharedState>,
    Qs(mut query): Qs<LoadRequest>,
) -> Result<Response, AppError> {
    // Set default collection (FIXME: should be required)
    query.collection.get_or_insert("default".to_string());

    tracing::info!("{:#?}", &query);

    // Distribute or load
    match query.workers {
        None => {
            if let Some(pc) = state.write().await.data.remove(&query.collection.unwrap()) {
                std::fs::remove_dir_all(pc.store.dir).unwrap();
            };
        }
        Some(ref workers) => {
            // dirstribute request
            let client = reqwest::Client::new();

            let mut set = JoinSet::new();

            for url in workers.iter() {
                let url = format!("{}/load?{}", url, serde_qs::to_string(&query).unwrap());

                set.spawn(client.delete(url).send());
            }

            while let Some(res) = set.join_next().await {
                let out = res.unwrap();
                out.unwrap().error_for_status().unwrap();
            }
        }
    }

    Ok(StatusCode::OK.into_response())
}
