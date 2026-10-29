//! Real FlatGeobuf entry for the engine-native `ST_EstimatedExtent` function.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Float64Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_flatgeobuf::writer::{FlatGeobufWriter, FlatGeobufWriterOptions};
use geoarrow_schema::{Dimension, PointType};
use geodatafusion_flatgeobuf::file_format::FlatGeobufFormat;
use geodatafusion_flatgeobuf::with_spatial_statistics;
use tempfile::TempDir;
use wkt::wkt;

fn write_fixture(dir: &TempDir) -> PathBuf {
    let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
    builder.push_point(Some(&wkt!(POINT(1.0 2.0))));
    builder.push_point(Some(&wkt!(POINT(3.0 4.0))));
    builder.push_point(Some(&wkt!(POINT(5.0 6.0))));
    let geometry = builder.finish();

    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::new(geometry.data_type().to_field("geometry", false)),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
            geometry.into_array_ref(),
        ],
    )
    .unwrap();

    let path = dir.path().join("points.fgb");
    let output = BufWriter::new(File::create(&path).unwrap());
    let mut writer = FlatGeobufWriter::try_new(
        output,
        schema,
        FlatGeobufWriterOptions::new("points".to_string()),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap().flush().unwrap();
    path
}

async fn listing_table(context: &SessionContext, path: &Path) -> ListingTable {
    let config = ListingTableConfig::new(ListingTableUrl::parse(path.to_str().unwrap()).unwrap())
        .with_listing_options(ListingOptions::new(Arc::new(FlatGeobufFormat::default())))
        .infer_schema(&context.state())
        .await
        .unwrap();
    ListingTable::try_new(config).unwrap()
}

fn f64_value(batch: &RecordBatch, column: &str) -> f64 {
    batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn reads_header_envelope_through_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_fixture(&dir);
    let context = SessionContext::new();
    geodatafusion::register(&context);

    let table = listing_table(&context, &path).await;
    let provider = with_spatial_statistics(&context.state(), table)
        .await
        .unwrap();
    context.register_table("points", provider).unwrap();

    let batches = context
        .sql(
            "SELECT ST_XMin(ST_EstimatedExtent('points', 'geometry')) AS xmin, \
                    ST_YMin(ST_EstimatedExtent('points', 'geometry')) AS ymin, \
                    ST_XMax(ST_EstimatedExtent('points', 'geometry')) AS xmax, \
                    ST_YMax(ST_EstimatedExtent('points', 'geometry')) AS ymax",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = batches.iter().find(|batch| batch.num_rows() > 0).unwrap();
    assert_eq!(f64_value(batch, "xmin"), 1.0);
    assert_eq!(f64_value(batch, "ymin"), 2.0);
    assert_eq!(f64_value(batch, "xmax"), 5.0);
    assert_eq!(f64_value(batch, "ymax"), 6.0);

    // Confirm the underlying FlatGeobuf scan still returns real rows.
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
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn returns_null_for_unwrapped_listing_table() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_fixture(&dir);
    let context = SessionContext::new();
    geodatafusion::register(&context);

    let table = listing_table(&context, &path).await;
    context.register_table("points", Arc::new(table)).unwrap();

    let batches = context
        .sql("SELECT ST_EstimatedExtent('points', 'geometry') IS NULL AS missing")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert!(
        batches
            .iter()
            .find(|batch| batch.num_rows() > 0)
            .unwrap()
            .column_by_name("missing")
            .unwrap()
            .as_boolean()
            .value(0)
    );
}
