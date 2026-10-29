//! Publish GeoParquet column bounding boxes as spatial statistics.
//!
//! GeoParquet stores per-column bounding boxes in its `geo` schema metadata.
//! Wrapping a [`ListingTable`] keeps those bounds available to
//! `ST_EstimatedExtent` without scanning row groups or fabricating DataFusion's
//! generic scalar column statistics.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::listing::ListingTable;
use datafusion::error::{DataFusionError, Result};
use geodatafusion::spatial_statistics::{SpatialExtent, SpatialStatistics, SpatialStatisticsTable};
use geoparquet::metadata::GeoParquetMetadata;

/// Wrap `table` so `ST_EstimatedExtent` can read its GeoParquet bounding boxes.
///
/// Tables without GeoParquet metadata keep an empty statistics set, which
/// makes `ST_EstimatedExtent` return `NULL`.
pub fn with_spatial_statistics(table: ListingTable) -> Result<Arc<dyn TableProvider>> {
    let statistics = spatial_statistics(&table)?;
    Ok(Arc::new(SpatialStatisticsTable::new(
        Arc::new(table),
        statistics,
    )))
}

/// Read per-column `bbox` values from the table's GeoParquet metadata.
pub fn spatial_statistics(table: &ListingTable) -> Result<SpatialStatistics> {
    let mut statistics = SpatialStatistics::new();
    let schema = table.schema();
    let Some(geo_meta) = schema.metadata().get("geo") else {
        return Ok(statistics);
    };
    let metadata: GeoParquetMetadata = serde_json::from_str(geo_meta)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;

    for (column, column_metadata) in &metadata.columns {
        if let Some(extent) = column_metadata
            .bbox
            .as_deref()
            .and_then(SpatialExtent::from_bbox)
        {
            statistics.insert(column.clone(), extent);
        }
    }

    Ok(statistics)
}
