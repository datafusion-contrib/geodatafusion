//! Probe for the catalog statistics surface that `ST_EstimatedExtent` cannot
//! use directly.
//!
//! PostGIS reads a spatial index or ND statistics collected by `ANALYZE`.
//! DataFusion providers expose optional `TableProvider::statistics` instead,
//! and core tables do not populate it. These tests record that absence: it is
//! why `SpatialStatisticsTable` exists as an explicit provider wrapper instead
//! of the function trusting generic scalar statistics.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use datafusion::catalog::TableProvider;
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_schema::{CoordType, Crs, Dimension, Metadata, PointType};

const CRS_AUTHORITY: &str = "EPSG:4326";

fn geometry_table() -> Arc<MemTable> {
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code(CRS_AUTHORITY.to_string()),
        None,
    ));
    let point_type = PointType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
    let points = PointBuilder::from_nullable_points(
        [
            Some(&geo::Point::new(1.0, 2.0)),
            Some(&geo::Point::new(3.0, 4.0)),
        ]
        .into_iter(),
        point_type,
    )
    .finish();
    let schema = Arc::new(Schema::new(vec![Arc::new(
        points.data_type().to_field("geom", false),
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![points.to_array_ref()]).unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

#[tokio::test]
async fn memtable_statistics_do_not_expose_geometry_extents() {
    let table = geometry_table();
    assert!(
        table.statistics().is_none(),
        "MemTable does not expose provider-level statistics"
    );
}

#[tokio::test]
async fn catalog_lookup_exposes_the_same_absent_geometry_extents() {
    let table = geometry_table();
    let ctx = SessionContext::new();
    ctx.register_table("points", Arc::clone(&table) as Arc<dyn TableProvider>)
        .unwrap();

    let state = ctx.state();
    let catalog = state
        .catalog_list()
        .catalog("datafusion")
        .expect("default catalog");
    let schema = catalog.schema("public").expect("default schema");
    let resolved = schema
        .table("points")
        .await
        .unwrap()
        .expect("registered table");
    assert!(
        resolved.statistics().is_none(),
        "catalog lookup reaches the same provider without geometry statistics"
    );
}
