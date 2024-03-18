use std::{
    io::{BufReader, Cursor},
    sync::RwLock,
};

use anyhow::Context;
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use axum::{
    body::Bytes,
    http::header::CONTENT_TYPE,
    response::{IntoResponse, Response},
    Extension,
};
use once_cell::sync::OnceCell;
use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};
use rstar::Envelope;
use serde::{Deserialize, Serialize};

use pc_format::{compute::filter_by_aabb, soa::Index, Point, PointTrait, AABB};
use serde_with::{formats::CommaSeparator, serde_as, StringWithSeparator};
use tokio::runtime::Handle;

use crate::{error::AppError, state::SharedState, Qs};

#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct BoxQuery {
    collection: Option<String>,
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, f64>>")]
    bounds: Option<Vec<f64>>,
    p: Option<f64>,
    budget: Option<u64>,
}

#[axum::debug_handler]
pub(crate) async fn points(
    Extension(state): Extension<SharedState>,
    Qs(mut query): Qs<BoxQuery>,
) -> Result<Response, AppError> {
    // Set default collection (FIXME: should be collections and required)
    query.collection.get_or_insert("default".to_string());

    // get extent
    let mut lower: Point<f64, 4> = query.bounds.as_ref().map_or_else(
        || Point::from_slice(&[f64::MIN; 4]),
        |c| Point::from_slice(&c[..(c.len() / 2)]),
    );
    let mut upper: Point<f64, 4> = query.bounds.as_ref().map_or_else(
        || Point::from_slice(&[f64::MAX; 4]),
        |c| Point::from_slice(&c[(c.len() / 2)..]),
    );

    *rstar::Point::nth_mut(&mut lower, 3) = 0.;
    *rstar::Point::nth_mut(&mut upper, 3) = query.p.unwrap_or(1.);

    let aabb: AABB<Point<f64, 4>> = AABB::from_corners(lower, upper);
    tracing::debug!("{query:#?}");

    // setup writer
    let mut writer: OnceCell<RwLock<StreamWriter<Vec<u8>>>> = OnceCell::new();

    // get points
    let workers = state.read().await.workers.to_owned();
    if !workers.is_empty() {
        // Distribute query if workers are registered
        let handle = Handle::current();
        workers.par_iter().for_each(|url| {
            let url = format!("{url}/points?{}", serde_qs::to_string(&query).unwrap());
            let reader = handle.block_on(get_points(url)).unwrap();
            reader.par_bridge().for_each(|batch| {
                let batch = batch.as_ref().unwrap();
                let writer = writer.get_or_init(|| {
                    RwLock::new(
                        StreamWriter::try_new(Vec::new(), &batch.schema())
                            .context("Create stream writer")
                            .unwrap(),
                    )
                });
                writer.write().unwrap().write(batch).unwrap();
            })
        });
    } else {
        // Execute query
        let collection = query.collection.as_ref().unwrap();

        if !state.read().await.data.contains_key(collection) {
            tracing::warn!("No data for collection `{collection}`");
            return Err(AppError::NotFound);
        }

        let state = state.read().await;
        let pc = state.data.get(collection).unwrap();
        match &pc.index {
            Index::Point(_index) => {
                todo!()
                // tracing::info!("Querying point-indexed collection `{collection}`");

                // let mut indices: Vec<u64> = state
                //     .read()
                //     .await
                //     .index
                //     .locate_in_envelope_intersecting(&aabb)
                //     .map(|p| p.data)
                //     .collect();
                // indices.sort_unstable();

                // if !indices.is_empty() {
                //     let last = indices.last().unwrap().to_owned();

                //     let mut i = 0;
                //     let mut j = 0;

                //     let mut cell_ids: Vec<String> = state
                //         .read()
                //         .await
                //         .framework
                //         .iter()
                //         .map(|cell| cell.id())
                //         .collect();
                //     cell_ids.sort();

                //     for id in cell_ids {
                //         for batch in state
                //             .read()
                //             .await
                //             .ctx
                //             .table(id)
                //             .await
                //             .unwrap()
                //             .collect()
                //             .await
                //             .unwrap()
                //         {
                //             let to = i + batch.num_rows() as u64;
                //             let filter: Vec<bool> = (i..to)
                //                 .map(|id| {
                //                     if id > last {
                //                         false
                //                     } else if id == indices[j] {
                //                         j += 1;
                //                         true
                //                     } else {
                //                         false
                //                     }
                //                 })
                //                 .collect();

                //             i = to;

                //             pcs.push(filter_record_batch(&batch, &filter.into()).unwrap());
                //         }
                //     }
                // }
            }
            Index::Batch(index) => {
                tracing::info!("Querying block-indexed collection `{collection}`");
                index
                    .locate_in_envelope_intersecting(&aabb)
                    .par_bridge()
                    .for_each(|object| {
                        pc.store
                            .batches(&object.data)
                            .into_iter()
                            .for_each(|batch| {
                                let batch = if aabb.contains_envelope(object.geom()) {
                                    batch
                                } else {
                                    filter_by_aabb(&batch, &aabb)
                                };
                                let writer = writer.get_or_init(|| {
                                    RwLock::new(
                                        StreamWriter::try_new(Vec::new(), &batch.schema())
                                            .context("Create stream writer")
                                            .unwrap(),
                                    )
                                });
                                writer.write().unwrap().write(&batch).unwrap();
                            });
                    })
            }
            Index::Multi(_) => todo!(),
            Index::None => {
                tracing::info!("Querying unindexed collection `{collection}`");
                pc.store.par_iter().for_each(|e| {
                    pc.store.batches(e.key()).par_iter().for_each(|batch| {
                        let batch = filter_by_aabb(batch, &aabb);
                        if batch.num_rows() > 0 {
                            let writer = writer.get_or_init(|| {
                                RwLock::new(
                                    StreamWriter::try_new(Vec::new(), &batch.schema())
                                        .context("Create stream writer")
                                        .unwrap(),
                                )
                            });
                            writer.write().unwrap().write(&batch).unwrap();
                        }
                    })
                });
            }
        }
    };

    let writer = writer.take().unwrap();

    writer.write().unwrap().finish().context("Finish stream")?;

    let buffer = writer
        .into_inner()
        .unwrap()
        .into_inner()
        .context("Get stream buffer")?;

    let header = [(CONTENT_TYPE, "application/vnd.apache.arrow.stream")];
    let body: Bytes = buffer.into();

    Ok((header, body).into_response())
}

async fn get_points(url: String) -> Result<StreamReader<BufReader<Cursor<Bytes>>>, AppError> {
    let response = reqwest::get(url).await.context("Request error")?;

    let response = response
        .error_for_status()
        .context("Request status error")?;

    let body = response.bytes().await.context("Read body to bytes")?;

    let reader = Cursor::new(body);
    let reader = StreamReader::try_new(reader, None).context("Missing schema")?;

    Ok(reader)
}
