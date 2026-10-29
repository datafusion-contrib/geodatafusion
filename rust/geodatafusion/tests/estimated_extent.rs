//! Public SQL contract for the engine-native `ST_EstimatedExtent` scalar function.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::catalog::MemTable;
use datafusion::prelude::*;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_schema::{Dimension, PointType};
use geodatafusion::spatial_statistics::{SpatialExtent, SpatialStatistics, SpatialStatisticsTable};

fn context() -> SessionContext {
    let context = SessionContext::new();
    geodatafusion::register(&context);
    context
}

fn points_table() -> Arc<dyn datafusion::catalog::TableProvider> {
    let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
    builder.push_null();
    let geometry = builder.finish();
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::new(geometry.data_type().to_field("geometry", true)),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow_array::Int32Array::from(vec![1])) as _,
            geometry.into_array_ref(),
        ],
    )
    .unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

fn statistics_with_geometry_extent() -> SpatialStatistics {
    let mut statistics = SpatialStatistics::new();
    statistics.insert("geometry", SpatialExtent::new(1.0, 2.0, 5.0, 6.0));
    statistics
}

async fn collect_extrema(context: &SessionContext, sql: &str) -> RecordBatch {
    let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .unwrap()
        .clone()
}

fn f64_value(batch: &RecordBatch, column: &str) -> f64 {
    let array = batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    array.value(0)
}

#[tokio::test]
async fn reads_extent_from_catalog_provider() {
    let context = context();
    context
        .register_table(
            "points",
            Arc::new(SpatialStatisticsTable::new(
                points_table(),
                statistics_with_geometry_extent(),
            )),
        )
        .unwrap();

    let batch = collect_extrema(
        &context,
        "SELECT ST_XMin(ST_EstimatedExtent('points', 'geometry')) AS xmin, \
                ST_YMin(ST_EstimatedExtent('points', 'geometry')) AS ymin, \
                ST_XMax(ST_EstimatedExtent('points', 'geometry')) AS xmax, \
                ST_YMax(ST_EstimatedExtent('points', 'geometry')) AS ymax",
    )
    .await;
    assert_eq!(f64_value(&batch, "xmin"), 1.0);
    assert_eq!(f64_value(&batch, "ymin"), 2.0);
    assert_eq!(f64_value(&batch, "xmax"), 5.0);
    assert_eq!(f64_value(&batch, "ymax"), 6.0);

    let batch = collect_extrema(
        &context,
        "SELECT ST_XMin(ST_EstimatedExtent('public', 'points', 'geometry')) AS xmin",
    )
    .await;
    assert_eq!(f64_value(&batch, "xmin"), 1.0);

    // The wrapper must not change the underlying table scan.
    let batches = context
        .sql("SELECT id FROM points ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[tokio::test]
async fn returns_null_without_metadata_statistics() {
    let context = context();
    context.register_table("plain", points_table()).unwrap();

    let batch = collect_extrema(
        &context,
        "SELECT ST_EstimatedExtent('plain', 'geometry') IS NULL AS missing",
    )
    .await;
    assert!(
        batch
            .column_by_name("missing")
            .unwrap()
            .as_boolean()
            .value(0)
    );

    let batch = collect_extrema(
        &context,
        "SELECT ST_EstimatedExtent(NULL, 'geometry') IS NULL AS missing",
    )
    .await;
    assert!(
        batch
            .column_by_name("missing")
            .unwrap()
            .as_boolean()
            .value(0)
    );
}

#[tokio::test]
async fn reports_catalog_and_column_errors() {
    let context = context();
    context
        .register_table(
            "points",
            Arc::new(SpatialStatisticsTable::new(
                points_table(),
                statistics_with_geometry_extent(),
            )),
        )
        .unwrap();

    for (sql, expected) in [
        (
            "SELECT ST_EstimatedExtent('missing', 'geometry')",
            "table `datafusion.public.missing` does not exist",
        ),
        (
            "SELECT ST_EstimatedExtent('missing_schema', 'points', 'geometry')",
            "schema `datafusion.missing_schema` does not exist",
        ),
        (
            "SELECT ST_EstimatedExtent('missing_catalog.public.points', 'geometry')",
            "catalog `missing_catalog` does not exist",
        ),
        (
            "SELECT ST_EstimatedExtent('points', 'missing_column')",
            "column `missing_column` does not exist in table `datafusion.public.points`",
        ),
        (
            "SELECT ST_EstimatedExtent('points', 'id')",
            "column `id` must be a geometry in table `datafusion.public.points`",
        ),
    ] {
        let error = context.sql(sql).await.unwrap().collect().await.unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "expected `{expected}` in `{error}`"
        );
    }
}

#[tokio::test]
async fn rejects_invalid_arguments() {
    let context = context();

    for (sql, expected) in [
        (
            "SELECT ST_EstimatedExtent()",
            "does not support zero arguments",
        ),
        (
            "SELECT ST_EstimatedExtent('points')",
            "No function matches the given name and argument types 'st_estimatedextent(Utf8)'",
        ),
        (
            "SELECT ST_EstimatedExtent('points', 'geometry', 'extra', 'more')",
            "No function matches the given name and argument types 'st_estimatedextent(Utf8, Utf8, Utf8, Utf8)'",
        ),
    ] {
        let error = context.sql(sql).await.unwrap_err().to_string();
        assert!(
            error.contains(expected),
            "expected `{expected}` in `{error}`"
        );
    }

    let error = context
        .sql("SELECT ST_EstimatedExtent(42, 'geometry')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    // DataFusion coerces castable literals to the exact Utf8 signature, so the
    // integer reaches the catalog lookup as the string "42".
    assert!(
        error.contains("table `datafusion.public.42` does not exist"),
        "unexpected integer argument error: {error}"
    );
}
