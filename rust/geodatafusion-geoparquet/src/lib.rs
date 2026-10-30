#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
// #![warn(missing_docs)]
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

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::array::{PointArray, from_arrow_array};
    use geoarrow_array::builder::PointBuilder;
    use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
    use geoarrow_schema::{Dimension, PointType};
    use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptionsBuilder};
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;
    use wkt::wkt;

    use crate::file_format::GeoParquetFormatFactory;

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
        let file_format = Arc::new(GeoParquetFormatFactory::default());
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state).enable_url_table();
        geodatafusion::register(&ctx);
        ctx
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
