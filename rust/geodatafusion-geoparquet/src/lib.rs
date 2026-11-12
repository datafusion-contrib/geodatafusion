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

    use arrow_array::{BinaryArray, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_datasource_parquet::ParquetFormat;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::array::{PointArray, from_arrow_array};
    use geoarrow_array::builder::PointBuilder;
    use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
    use geoarrow_schema::{CoordType, Dimension, PointType};
    use geoparquet::writer::{
        GeoParquetRecordBatchEncoder, GeoParquetWriterEncoding, GeoParquetWriterOptionsBuilder,
    };
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
    async fn test_native_geometry_reads_with_both_encodings_and_layouts() {
        for (encoding, parse_to_native) in [
            (GeoParquetWriterEncoding::WKB, true),
            (GeoParquetWriterEncoding::GeoArrow, true),
            (GeoParquetWriterEncoding::GeoArrow, false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = write_encoded_fixture(&dir, encoding);

            for coord_type in [CoordType::Separated, CoordType::Interleaved] {
                let file_format: Arc<dyn FileFormatFactory> = Arc::new(
                    GeoParquetFormatFactory::new()
                        .with_parse_to_native(parse_to_native)
                        .with_coord_type(coord_type),
                );
                let config = SessionConfig::new()
                    .with_target_partitions(2)
                    .with_repartition_file_min_size(0)
                    .with_batch_size(2);
                let state = SessionStateBuilder::new()
                    .with_config(config)
                    .with_file_formats(vec![file_format])
                    .build();
                let ctx = SessionContext::new_with_state(state).enable_url_table();
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
                            PointArray::try_from((batch.column(0).as_ref(), field.as_ref()))
                                .unwrap();
                        (0..array.len())
                            .map(|index| {
                                let coord = array.value(index).unwrap().coord().unwrap();
                                (coord.x(), coord.y())
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(points, vec![(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]);
                let batches = ctx
                    .sql(&format!("SELECT label FROM '{}'", path.display()))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
                assert!(batches.iter().all(|batch| batch.num_rows() <= 2));
            }
        }
    }

    #[tokio::test]
    async fn test_native_encoding_with_ambiguous_geometry_types_is_rejected() {
        for geometry_types in [
            "[]",
            "[\"Point\", \"MultiPoint\"]",
            "[\"GeometryCollection\"]",
        ] {
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
                err.contains("exactly one geometry type is required")
                    || err.contains("does not match native encoding"),
                "unexpected error for {geometry_types}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn test_partitioned_native_scan_adapts_missing_and_reordered_columns() {
        let dir = tempfile::tempdir().unwrap();
        let (batch, _) = sample_table();
        for (region, batch) in [
            ("new", batch.clone()),
            ("old", batch.project(&[2, 0]).unwrap()),
        ] {
            let partition = dir.path().join(format!("region={region}"));
            std::fs::create_dir(&partition).unwrap();
            write_encoded_batch(
                &partition.join("points.parquet"),
                &batch,
                GeoParquetWriterEncoding::WKB,
            );
        }
        let ctx = geoparquet_context();
        let format = Arc::new(GeoParquetFormat::default().with_parse_to_native(true));
        let options = ListingOptions::new(format)
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![("region".to_string(), DataType::Utf8)]);
        let config =
            ListingTableConfig::new(ListingTableUrl::parse(dir.path().to_str().unwrap()).unwrap())
                .with_listing_options(options)
                .infer_schema(&ctx.state())
                .await
                .unwrap();
        ctx.register_table("points", Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();
        let batches = ctx
            .sql("SELECT region, id, label, ST_X(geometry) AS x FROM points ORDER BY region, id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        datafusion::assert_batches_eq!(
            [
                "+--------+----+-------+-----+",
                "| region | id | label | x   |",
                "+--------+----+-------+-----+",
                "| new    | 1  | one   | 1.0 |",
                "| new    | 2  | two   | 3.0 |",
                "| new    | 3  | three | 5.0 |",
                "| old    | 1  |       | 1.0 |",
                "| old    | 2  |       | 3.0 |",
                "| old    | 3  |       | 5.0 |",
                "+--------+----+-------+-----+",
            ],
            &batches
        );
        let batches = ctx
            .sql("SELECT region FROM points")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
    }

    #[tokio::test]
    async fn test_default_formats_preserve_geometry_metadata_and_reader_options() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_geoparquet_fixture(&dir);
        let mut options = datafusion::common::config::TableParquetOptions::default();
        options.global.binary_as_string = true;
        options.global.schema_force_view_types = false;
        let formats: [(Arc<dyn FileFormat>, DataType); 4] = [
            (Arc::new(GeoParquetFormat::default()), DataType::BinaryView),
            (
                GeoParquetFormatFactory::new().default(),
                DataType::BinaryView,
            ),
            (
                GeoParquetFormatFactory::new()
                    .with_coord_type(CoordType::Interleaved)
                    .default(),
                DataType::BinaryView,
            ),
            (
                GeoParquetFormatFactory::new_with_options(options)
                    .with_parse_to_native(true)
                    .default(),
                DataType::Utf8,
            ),
        ];
        for (format, payload_type) in formats {
            let ctx = geoparquet_context();
            let config =
                ListingTableConfig::new(ListingTableUrl::parse(path.to_str().unwrap()).unwrap())
                    .with_listing_options(ListingOptions::new(format))
                    .infer_schema(&ctx.state())
                    .await
                    .unwrap();
            let schema = config.file_schema.as_ref().unwrap();
            assert!(
                schema
                    .field_with_name("geometry")
                    .unwrap()
                    .extension_type_name()
                    .unwrap()
                    .starts_with("geoarrow.")
            );
            assert_eq!(
                schema.field_with_name("payload").unwrap().data_type(),
                &payload_type
            );
            ctx.register_table("points", Arc::new(ListingTable::try_new(config).unwrap()))
                .unwrap();
            let batches = ctx.sql("SELECT ST_X(geometry) AS x, CAST(payload AS VARCHAR) AS payload FROM points ORDER BY id")
                .await.unwrap().collect().await.unwrap();
            datafusion::assert_batches_eq!(
                [
                    "+-----+---------+",
                    "| x   | payload |",
                    "+-----+---------+",
                    "| 1.0 | a       |",
                    "| 3.0 | b       |",
                    "| 5.0 | c       |",
                    "+-----+---------+",
                ],
                &batches
            );
        }
    }

    #[tokio::test]
    async fn test_native_scans_populate_the_parquet_metadata_cache() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_geoparquet_fixture(&dir);
        for (parse_to_native, coord_type) in [
            (true, CoordType::Separated),
            (false, CoordType::Interleaved),
        ] {
            let ctx = geoparquet_context();
            let format = Arc::new(
                GeoParquetFormat::new(ParquetFormat::default().with_metadata_size_hint(Some(4096)))
                    .with_parse_to_native(parse_to_native)
                    .with_coord_type(coord_type),
            );
            let config =
                ListingTableConfig::new(ListingTableUrl::parse(path.to_str().unwrap()).unwrap())
                    .with_listing_options(ListingOptions::new(format))
                    .infer_schema(&ctx.state())
                    .await
                    .unwrap();
            ctx.register_table("points", Arc::new(ListingTable::try_new(config).unwrap()))
                .unwrap();
            let plan = ctx
                .sql("SELECT ST_X(geometry) AS x FROM points ORDER BY id")
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
            // Schema inference and planning also fill this cache. Only execution counts here.
            cache.clear();
            let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
                .await
                .unwrap();
            datafusion::assert_batches_eq!(
                [
                    "+-----+", "| x   |", "+-----+", "| 1.0 |", "| 3.0 |", "| 5.0 |", "+-----+"
                ],
                &batches
            );
            assert_eq!(
                cache.len(),
                1,
                "native scan must retain Parquet's footer cache"
            );
        }
    }

    #[tokio::test]
    async fn test_mixed_wkb_metadata_with_struct_storage_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("malformed.parquet");
        let (batch, _) = sample_table();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.append_key_value_metadata(KeyValue::new("geo".to_string(),
            r#"{"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":["Point","LineString"]}}}"#.to_string()));
        writer.close().unwrap();

        for coord_type in [CoordType::Separated, CoordType::Interleaved] {
            let ctx = geoparquet_context();
            let format = GeoParquetFormatFactory::new()
                .with_parse_to_native(true)
                .with_coord_type(coord_type)
                .default();
            let config =
                ListingTableConfig::new(ListingTableUrl::parse(path.to_str().unwrap()).unwrap())
                    .with_listing_options(ListingOptions::new(format))
                    .infer_schema(&ctx.state())
                    .await
                    .unwrap();
            ctx.register_table(
                "malformed",
                Arc::new(ListingTable::try_new(config).unwrap()),
            )
            .unwrap();
            let error = ctx
                .sql("SELECT geometry FROM malformed")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("GeoParquet column 'geometry' cannot decode"),
                "{error}"
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
        write_encoded_fixture(dir, GeoParquetWriterEncoding::WKB)
    }

    fn write_encoded_fixture(
        dir: &TempDir,
        encoding: GeoParquetWriterEncoding,
    ) -> std::path::PathBuf {
        let path = dir.path().join("points.parquet");
        let (batch, _) = sample_table();
        write_encoded_batch(&path, &batch, encoding);
        path
    }

    fn write_encoded_batch(
        path: &std::path::Path,
        batch: &RecordBatch,
        encoding: GeoParquetWriterEncoding,
    ) {
        let schema = batch.schema();
        let options = GeoParquetWriterOptionsBuilder::default()
            .set_encoding(encoding)
            .set_primary_column("geometry".to_string())
            .build();
        let mut encoder = GeoParquetRecordBatchEncoder::try_new(&schema, &options).unwrap();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, encoder.target_schema(), None).unwrap();
        let encoded_batch = encoder.encode_record_batch(batch).unwrap();
        writer.write(&encoded_batch).unwrap();
        writer.append_key_value_metadata(encoder.into_keyvalue().unwrap());
        writer.close().unwrap();
    }

    fn sample_table() -> (RecordBatch, Schema) {
        let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
        builder.push_point(Some(&wkt!(POINT(1.0 2.0))));
        builder.push_point(Some(&wkt!(POINT(3.0 4.0))));
        builder.push_point(Some(&wkt!(POINT(5.0 6.0))));
        let geometry = builder.finish();

        let fields = vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(Field::new("label", DataType::Utf8, true)),
            Arc::new(geometry.data_type().to_field("geometry", false)),
            Arc::new(Field::new("payload", DataType::Binary, true)),
        ];
        let schema = Schema::new(fields);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
                Arc::new(StringArray::from(vec!["one", "two", "three"])) as _,
                geometry.into_array_ref(),
                Arc::new(BinaryArray::from_vec(vec![b"a", b"b", b"c"])) as _,
            ],
        )
        .unwrap();
        (batch, schema)
    }
}
