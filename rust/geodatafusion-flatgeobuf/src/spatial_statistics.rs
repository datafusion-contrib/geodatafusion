//! Publish FlatGeobuf header envelopes as spatial statistics.
//!
//! FlatGeobuf stores the dataset envelope in the file header. Wrapping a
//! [`ListingTable`] keeps that metadata available to `ST_EstimatedExtent`
//! without scanning features and without fabricating DataFusion's generic
//! scalar column statistics.

use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::listing::ListingTable;
use datafusion::error::Result;
use futures::TryStreamExt;
use geodatafusion::spatial_statistics::{SpatialExtent, SpatialStatistics, SpatialStatisticsTable};

use crate::utils::open_flatgeobuf_reader;

/// Wrap `table` so `ST_EstimatedExtent` can read its metadata envelope.
///
/// Only file headers are read. Tables without an envelope keep an empty
/// statistics set, which makes `ST_EstimatedExtent` return `NULL`.
pub async fn with_spatial_statistics(
    session: &dyn Session,
    table: ListingTable,
) -> Result<Arc<dyn TableProvider>> {
    let statistics = spatial_statistics(session, &table).await?;
    Ok(Arc::new(SpatialStatisticsTable::new(
        Arc::new(table),
        statistics,
    )))
}

/// Read and union FlatGeobuf header envelopes for every file in `table`.
pub async fn spatial_statistics(
    session: &dyn Session,
    table: &ListingTable,
) -> Result<SpatialStatistics> {
    let mut statistics = SpatialStatistics::new();
    let extension = table.options().file_extension.clone();

    for table_path in table.table_paths() {
        let store = session
            .runtime_env()
            .object_store(table_path.object_store())?;
        let mut files = table_path
            .list_all_files(session, store.as_ref(), &extension)
            .await?;

        while let Some(object) = files.try_next().await? {
            let reader = open_flatgeobuf_reader(store.clone(), object.location.clone()).await?;
            let Some(envelope) = reader.header().envelope() else {
                continue;
            };
            if envelope.len() != 4 {
                continue;
            }

            let bbox = [
                envelope.get(0),
                envelope.get(1),
                envelope.get(2),
                envelope.get(3),
            ];
            let Some(extent) = SpatialExtent::from_bbox(&bbox) else {
                continue;
            };
            let extent = statistics
                .get("geometry")
                .map_or(extent, |existing| existing.union(extent));
            statistics.insert("geometry", extent);
        }
    }

    Ok(statistics)
}
