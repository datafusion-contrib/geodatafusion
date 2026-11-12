#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

pub mod file_format;
pub mod source;

pub use file_format::{GeoParquetFormat, GeoParquetFormatFactory};

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::{BinaryArray, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
    use datafusion::datasource::listing::{ListingOptions, ListingTableConfig, ListingTableUrl};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_datasource_parquet::ParquetFormat;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::array::{PointArray, from_arrow_array};
    use geoarrow_array::builder::PointBuilder;
    use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
    use geoarrow_schema::{CoordType, Dimension, PointType};
    use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptionsBuilder};
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use tempfile::TempDir;
    use wkt::wkt;

    use crate::file_format::{GeoParquetFormat, GeoParquetFormatFactory};

    #[tokio::test]
    async fn test_geoparquet_centroid() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_geoparquet_fixture(&dir);
        let ctx = geoparquet_context();

        let df = ctx
            .sql(&format!(
                "SELECT id, ST_Centroid(geometry) AS centroid FROM '{}' ORDER BY id",
                path.to_str().unwrap()
            ))
            .await
            .unwrap();
        let schema = df.schema().clone();
        let batches = df.collect().await.unwrap();
        let ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 2, 3]);

        let centroid_field = schema.field_with_unqualified_name("centroid").unwrap();
        let centroid_column = batches[0].column_by_name("centroid").unwrap();
        let centroid_array = from_arrow_array(centroid_column.as_ref(), centroid_field).unwrap();
        let point_array = centroid_array
            .as_any()
            .downcast_ref::<PointArray>()
            .unwrap();
        let coordinates = (0..point_array.len())
            .map(|index| {
                let coord = point_array.value(index).unwrap().coord().unwrap();
                (coord.x(), coord.y())
            })
            .collect::<Vec<_>>();
        assert_eq!(coordinates, vec![(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]);
    }

    #[tokio::test]
    async fn test_geoparquet_geoarrow_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_geoparquet_fixture(&dir);
        let ctx = geoparquet_context();

        let df = ctx
            .sql(&format!(
                "SELECT geometry FROM '{}' AS table LIMIT 3",
                path.to_str().unwrap()
            ))
            .await
            .unwrap();
        let schema = df.schema().clone();
        let field = schema.field_with_unqualified_name("geometry").unwrap();
        assert_eq!(field.extension_type_name().unwrap(), "geoarrow.wkb");

        let batches = df.collect().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let geometry_column = batches[0].column_by_name("geometry").unwrap();
        let geometry_array = from_arrow_array(geometry_column.as_ref(), field).unwrap();
        assert_eq!(geometry_array.len(), 3);
    }

    fn geoparquet_context() -> SessionContext {
        geoparquet_context_with(Arc::new(GeoParquetFormatFactory::new()))
    }

    fn geoparquet_context_with(file_format: Arc<dyn FileFormatFactory>) -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state).enable_url_table();
        geodatafusion::register(&ctx);
        ctx
    }

    #[tokio::test]
    async fn test_parse_to_native_reads_wkb_with_both_layouts() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_geoparquet_fixture(&dir);

        for coord_type in [CoordType::Separated, CoordType::Interleaved] {
            let file_format: Arc<dyn FileFormatFactory> = Arc::new(
                GeoParquetFormatFactory::new()
                    .with_parse_to_native(true)
                    .with_coord_type(coord_type),
            );
            let ctx = geoparquet_context_with(file_format);
            let df = ctx
                .sql(&format!(
                    "SELECT geometry FROM '{}' AS table ORDER BY id",
                    path.to_str().unwrap()
                ))
                .await
                .unwrap();
            let schema = df.schema().clone();
            let field = schema.field_with_unqualified_name("geometry").unwrap();
            let point_type = field.extension_type::<PointType>();
            assert_eq!(point_type.dimension(), Dimension::XY);
            assert_eq!(point_type.coord_type(), coord_type);

            let batches = df.collect().await.unwrap();
            let points = batches
                .iter()
                .flat_map(|batch| {
                    let array =
                        PointArray::try_from((batch.column(0).as_ref(), field.as_ref())).unwrap();
                    (0..array.len())
                        .map(|index| {
                            let coord = array.value(index).unwrap().coord().unwrap();
                            (coord.x(), coord.y())
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert_eq!(points, vec![(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]);
        }
    }

    #[tokio::test]
    async fn test_native_encoding_with_ambiguous_geometry_types_is_rejected() {
        for geometry_types in ["[]", "[\"Point\", \"MultiPoint\"]"] {
            let dir = tempfile::tempdir().unwrap();
            let path = write_malformed_geo_parquet(&dir, geometry_types);
            let ctx = geoparquet_context();
            let format: Arc<dyn FileFormat> =
                Arc::new(GeoParquetFormat::new(ParquetFormat::default()));
            let result =
                ListingTableConfig::new(ListingTableUrl::parse(path.to_str().unwrap()).unwrap())
                    .with_listing_options(ListingOptions::new(format))
                    .infer_schema(&ctx.state())
                    .await;
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("exactly one geometry type is required"),
                "unexpected error for {geometry_types}: {err}"
            );
        }
    }

    fn write_malformed_geo_parquet(dir: &TempDir, geometry_types: &str) -> std::path::PathBuf {
        let path = dir.path().join("malformed.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "geometry",
            DataType::Binary,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BinaryArray::from_vec(vec![b"not-wkb".as_slice()])) as _],
        )
        .unwrap();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        let geo = format!(
            "{{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{{\"geometry\":{{\"encoding\":\"point\",\"geometry_types\":{geometry_types}}}}}}}"
        );
        writer.append_key_value_metadata(KeyValue::new("geo".to_string(), geo));
        writer.close().unwrap();
        path
    }

    fn write_geoparquet_fixture(dir: &TempDir) -> std::path::PathBuf {
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

    fn sample_table() -> (RecordBatch, Schema) {
        let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
        builder.push_point(Some(&wkt!(POINT(1.0 2.0))));
        builder.push_point(Some(&wkt!(POINT(3.0 4.0))));
        builder.push_point(Some(&wkt!(POINT(5.0 6.0))));
        let geometry = builder.finish();

        let fields = vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(geometry.data_type().to_field("geometry", false)),
        ];
        let schema = Schema::new(fields);
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
}
