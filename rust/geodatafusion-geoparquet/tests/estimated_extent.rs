//! Real GeoParquet entry for the engine-native `ST_EstimatedExtent` function.

use std::fs::File;
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
use geoarrow_schema::{Dimension, PointType};
use geodatafusion_geoparquet::file_format::GeoParquetFormat;
use geodatafusion_geoparquet::with_spatial_statistics;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptionsBuilder};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;
use wkt::wkt;

fn sample_table() -> (RecordBatch, Schema) {
    let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
    builder.push_point(Some(&wkt!(POINT(1.0 2.0))));
    builder.push_point(Some(&wkt!(POINT(3.0 4.0))));
    builder.push_point(Some(&wkt!(POINT(5.0 6.0))));
    let geometry = builder.finish();

    let schema = Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::new(geometry.data_type().to_field("geometry", false)),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
            geometry.into_array_ref(),
        ],
    )
    .unwrap();
    (batch, schema)
}

fn write_geoparquet_fixture(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("points.parquet");
    let (batch, schema) = sample_table();
    let options = GeoParquetWriterOptionsBuilder::default()
        .set_primary_column("geometry".to_string())
        .build();
    let mut encoder = GeoParquetRecordBatchEncoder::try_new(&schema, &options).unwrap();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, encoder.target_schema(), None).unwrap();
    let encoded_batch = encoder.encode_record_batch(&batch).unwrap();
    writer.write(&encoded_batch).unwrap();
    writer.append_key_value_metadata(encoder.into_keyvalue().unwrap());
    writer.close().unwrap();
    path
}

fn write_plain_parquet_fixture(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("plain.parquet");
    let (batch, schema) = sample_table();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path
}

async fn listing_table(context: &SessionContext, path: &Path) -> ListingTable {
    let config = ListingTableConfig::new(ListingTableUrl::parse(path.to_str().unwrap()).unwrap())
        .with_listing_options(ListingOptions::new(Arc::new(GeoParquetFormat::default())))
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
async fn reads_geoparquet_bbox_through_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_geoparquet_fixture(&dir);
    let context = SessionContext::new();
    geodatafusion::register(&context);

    let table = listing_table(&context, &path).await;
    let provider = with_spatial_statistics(table).unwrap();
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

    // Confirm the underlying GeoParquet scan still returns real rows.
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
async fn returns_null_without_geoparquet_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_plain_parquet_fixture(&dir);
    let context = SessionContext::new();
    geodatafusion::register(&context);

    let table = listing_table(&context, &path).await;
    let provider = with_spatial_statistics(table).unwrap();
    context.register_table("points", provider).unwrap();

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
