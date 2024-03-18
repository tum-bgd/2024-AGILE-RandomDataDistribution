use arrow::compute::concat_batches;
use axum::{http::StatusCode, response::IntoResponse, Extension};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rstar::{primitives::GeomWithData, RTree};
use tokio::task::JoinSet;

use pc_format::{compute::aabb, soa::Index, Point};

use crate::state::SharedState;

#[axum::debug_handler]
pub(crate) async fn index(Extension(state): Extension<SharedState>) -> impl IntoResponse {
    if !state.read().await.workers.is_empty() {
        let mut set = JoinSet::new();

        for worker in &state.read().await.workers {
            set.spawn(reqwest::get(format!("{worker}/index")));
        }

        while let Some(res) = set.join_next().await {
            let out = res.unwrap();
            out.unwrap().error_for_status().unwrap();
        }
    } else {
        // tracing::info!("Indexing points");

        // let mut points = Vec::new();

        // for cell in state.read().await.framework.iter() {
        //     for batch in state
        //         .read()
        //         .await
        //         .ctx
        //         .table(cell.id())
        //         .await
        //         .unwrap()
        //         .collect()
        //         .await
        //         .unwrap()
        //     {
        //         let mut pc = ArrowPointCloud::new(batch.schema(), None, None).unwrap();
        //         pc.append(batch).unwrap();

        //         let locations: Vec<GeomWithData<Point<f64, 4>, u64>> = pc
        //             .points::<Point<f64, 4>>()
        //             .zip(SmallRng::seed_from_u64(0).sample_iter::<f64, Standard>(Standard))
        //             .enumerate()
        //             .map(|(i, (mut point, importance))| {
        //                 *rstar::Point::nth_mut(&mut point, 3) = importance;
        //                 PointLocation::new(point, i as u64)
        //             })
        //             .collect();
        //         points.extend(locations);
        //     }
        // }

        // let n = points.len();
        // tracing::info!("Extracted {} point locations", n);

        // state.write().await.index = RTree::bulk_load_with_params(points);
        // tracing::info!("Indexed {} point locations", n);

        let mut state = state.write().await;
        let pc = state.data.get_mut("default").unwrap();
        let objects = pc
            .store
            .par_iter()
            .map(|e| {
                let batches = pc.store.batches(e.key());

                let aabb = if batches.len() == 1 {
                    aabb::<Point<f64, 4>>(&batches[0])
                } else {
                    aabb(&concat_batches(&batches[0].schema(), batches.iter()).unwrap())
                };

                // XXX: Memory usage (failure to free?)
                // match e.value_mut() {
                //     PointCloudData::InMemory(batches) => {
                //         batches.clear();
                //         batches.push(batch);
                //     }
                //     PointCloudData::OnDisk(_) => todo!(),
                // }

                GeomWithData::new(aabb, e.key().to_string())
            })
            .collect();

        let index = RTree::bulk_load_with_params(objects);
        tracing::info!("Indexed {} point batches", index.size());

        pc.index = Index::Batch(index);
    }

    StatusCode::OK
}

#[axum::debug_handler]
pub(crate) async fn remove_index(Extension(state): Extension<SharedState>) -> impl IntoResponse {
    // remove all indices
    state
        .write()
        .await
        .data
        .values_mut()
        .for_each(|pc| pc.index = Index::None)
}
