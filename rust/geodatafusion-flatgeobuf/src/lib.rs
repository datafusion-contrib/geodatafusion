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
mod utils;

pub use file_format::{FlatGeobufFileFactory, FlatGeobufFormat, FlatGeobufFormatFactory};

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::fs::File;
    use std::io::{BufReader, BufWriter, Write};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::arrow::array::AsArray;
    use datafusion::catalog::MemTable;
    use datafusion::catalog::memory::DataSourceExec;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;
    use flatgeobuf::FallibleStreamingIterator;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::array::{PointArray, from_arrow_array};
    use geoarrow_array::builder::{LineStringBuilder, PointBuilder};
    use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
    use geoarrow_flatgeobuf::writer::{FlatGeobufWriter, FlatGeobufWriterOptions};
    use geoarrow_schema::{Dimension, LineStringType, PointType};
    use object_store::ClientOptions;
    use object_store::http::HttpBuilder;
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;
    use wkt::wkt;

    use super::*;
    use crate::source::FlatGeobufSource;

    #[tokio::test]
    async fn test_column_projection_with_geometry() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_flatgeobuf_fixture(&dir, "points", sample_table());
        let ctx = flatgeobuf_context();
        register_local_listing(&ctx, &path, "points").await;

        let df = ctx
            .sql("SELECT id, name, geometry FROM points WHERE id = 2")
            .await
            .unwrap();
        let schema = df.schema().clone();
        let batches = df.collect().await.unwrap();

        assert_eq!(
            schema
                .field_with_unqualified_name("id")
                .unwrap()
                .data_type(),
            &DataType::Int32
        );
        let batch = single_nonempty_batch(&batches);
        let id_column = batch.column_by_name("id").unwrap();
        assert_eq!(
            id_column
                .as_primitive::<arrow_array::types::Int32Type>()
                .value(0),
            2
        );
        let name_column = batch.column_by_name("name").unwrap();
        assert_eq!(name_column.as_string_view().value(0), "bravo");

        let geometry_field = schema.field_with_unqualified_name("geometry").unwrap();
        let geometry_column = batch.column_by_name("geometry").unwrap();
        let geometry_array = from_arrow_array(geometry_column.as_ref(), geometry_field).unwrap();
        let point_array = geometry_array
            .as_any()
            .downcast_ref::<PointArray>()
            .unwrap();
        let coord = point_array.value(0).unwrap().coord().unwrap();
        assert_eq!((coord.x(), coord.y()), (3.0, 4.0));
    }

    #[tokio::test]
    async fn test_column_projection_without_geometry() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_flatgeobuf_fixture(&dir, "points", sample_table());
        let ctx = flatgeobuf_context();
        register_local_listing(&ctx, &path, "points").await;

        let df = ctx
            .sql("SELECT id, name FROM points ORDER BY id")
            .await
            .unwrap();
        assert_eq!(df.schema().fields().len(), 2);
        assert!(df.schema().field_with_unqualified_name("geometry").is_err());

        let batches = df.collect().await.unwrap();
        let ids = collect_i32_column(&batches, "id");
        let names = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("name")
                    .unwrap()
                    .as_string_view()
                    .iter()
                    .map(|value| value.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(names, vec!["alpha", "bravo", "charlie"]);
    }

    #[tokio::test]
    async fn test_centroid_through_registered_udf() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_flatgeobuf_fixture(&dir, "points", sample_table());
        let ctx = flatgeobuf_context();
        register_local_listing(&ctx, &path, "points").await;

        let df = ctx
            .sql("SELECT ST_Centroid(geometry) AS centroid FROM points WHERE id = 3")
            .await
            .unwrap();
        let schema = df.schema().clone();
        let batches = df.collect().await.unwrap();
        let batch = single_nonempty_batch(&batches);

        let centroid_field = schema.field_with_unqualified_name("centroid").unwrap();
        let centroid_array = from_arrow_array(
            batch.column_by_name("centroid").unwrap().as_ref(),
            centroid_field,
        )
        .unwrap();
        let point_array = centroid_array
            .as_any()
            .downcast_ref::<PointArray>()
            .unwrap();
        let coord = point_array.value(0).unwrap().coord().unwrap();
        assert_eq!((coord.x(), coord.y()), (5.0, 6.0));
    }

    #[tokio::test]
    async fn test_bbox_index_candidates_are_exactly_rechecked() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_flatgeobuf_fixture(&dir, "lines", indexed_table());

        let reader =
            flatgeobuf::FgbReader::open(BufReader::new(File::open(&path).unwrap())).unwrap();
        assert_eq!(reader.header().features_count(), 3);
        assert!(reader.header().index_node_size() > 0);
        let mut candidates = reader.select_bbox(0.0, 0.0, 10.0, 10.0).unwrap();
        let mut candidate_count = 0;
        while candidates.next().unwrap().is_some() {
            candidate_count += 1;
        }
        assert_eq!(candidate_count, 2);

        let server = TestHttpServer::start(std::fs::read(&path).unwrap()).await;
        let ctx = flatgeobuf_context();
        let object_store_url = ObjectStoreUrl::parse(&server.base_url).unwrap();
        let object_store = Arc::new(
            HttpBuilder::new()
                .with_url(&server.base_url)
                .with_client_options(ClientOptions::new().with_allow_http(true))
                .build()
                .unwrap(),
        );
        ctx.register_object_store(object_store_url.as_ref(), object_store);
        register_listing(
            &ctx,
            ListingTableUrl::parse(format!("{}/lines.fgb", server.base_url)).unwrap(),
            "lines",
        )
        .await;

        let geometry_predicate =
            "ST_Intersects(geometry, ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))";
        let indexed_df = ctx
            .sql(&format!(
                "SELECT id FROM lines WHERE {geometry_predicate} ORDER BY id"
            ))
            .await
            .unwrap();
        let indexed_plan = indexed_df.create_physical_plan().await.unwrap();
        let source = find_flatgeobuf_source(&indexed_plan).unwrap();
        assert_eq!(source.bbox(), Some([0.0, 0.0, 10.0, 10.0]));
        assert!(plan_contains_node(indexed_plan.as_ref(), "FilterExec"));
        let indexed_ids = collect_ids(&ctx, indexed_plan).await;
        assert_eq!(indexed_ids, vec![1]);

        let box_df = ctx
            .sql(
                "SELECT id FROM lines \
                 WHERE ST_Intersects(geometry, Box2D(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))) \
                 ORDER BY id",
            )
            .await
            .unwrap();
        let box_plan = box_df.create_physical_plan().await.unwrap();
        let box_source = find_flatgeobuf_source(&box_plan).unwrap();
        assert_eq!(box_source.bbox(), Some([0.0, 0.0, 10.0, 10.0]));
        assert!(plan_contains_node(box_plan.as_ref(), "FilterExec"));
        let box_ids = collect_ids(&ctx, box_plan).await;
        assert_eq!(box_ids, vec![1]);

        let full_scan_df = ctx
            .sql(&format!(
                "SELECT id FROM lines WHERE ({geometry_predicate}) IS TRUE ORDER BY id"
            ))
            .await
            .unwrap();
        let full_scan_plan = full_scan_df.create_physical_plan().await.unwrap();
        let full_scan_source = find_flatgeobuf_source(&full_scan_plan).unwrap();
        assert_eq!(full_scan_source.bbox(), None);
        assert!(plan_contains_node(full_scan_plan.as_ref(), "FilterExec"));
        let full_scan_ids = collect_ids(&ctx, full_scan_plan).await;
        assert_eq!(full_scan_ids, indexed_ids);
    }

    fn sample_table() -> (Vec<RecordBatch>, Arc<Schema>) {
        let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
        builder.push_point(Some(&wkt!( POINT(1.0 2.0) )));
        builder.push_point(Some(&wkt!( POINT(3.0 4.0) )));
        builder.push_point(Some(&wkt!( POINT(5.0 6.0) )));
        let geometry = builder.finish();

        let fields = vec![
            Arc::new(Field::new("id", DataType::Int32, true)),
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(geometry.data_type().to_field("geometry", true)),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
                Arc::new(StringArray::from(vec!["alpha", "bravo", "charlie"])) as _,
                geometry.into_array_ref(),
            ],
        )
        .unwrap();

        (vec![batch], schema)
    }

    fn indexed_table() -> (Vec<RecordBatch>, Arc<Schema>) {
        let mut builder =
            LineStringBuilder::new(LineStringType::new(Dimension::XY, Default::default()));
        builder
            .push_line_string(Some(&wkt!(LINESTRING(-5.0 5.0, 15.0 5.0))))
            .unwrap();
        builder
            .push_line_string(Some(&wkt!(
                LINESTRING(-1.0 -1.0, -1.0 11.0, 11.0 11.0, 11.0 -1.0)
            )))
            .unwrap();
        builder
            .push_line_string(Some(&wkt!(LINESTRING(20.0 20.0, 30.0 30.0))))
            .unwrap();
        let geometry = builder.finish();

        let fields = vec![
            Arc::new(Field::new("id", DataType::Int32, true)),
            Arc::new(geometry.data_type().to_field("geometry", true)),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
                geometry.into_array_ref(),
            ],
        )
        .unwrap();

        (vec![batch], schema)
    }

    fn write_flatgeobuf_fixture(
        dir: &TempDir,
        name: &str,
        (batches, schema): (Vec<RecordBatch>, Arc<Schema>),
    ) -> PathBuf {
        let path = dir.path().join(format!("{name}.fgb"));
        let output = BufWriter::new(File::create(&path).unwrap());
        let mut writer = FlatGeobufWriter::try_new(
            output,
            schema,
            FlatGeobufWriterOptions::new(name.to_string()),
        )
        .unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap().flush().unwrap();
        path
    }

    fn flatgeobuf_context() -> SessionContext {
        let file_format = Arc::new(FlatGeobufFileFactory::default());
        let state = SessionStateBuilder::new()
            .with_file_formats(vec![file_format])
            .build();
        let ctx = SessionContext::new_with_state(state);
        geodatafusion::register(&ctx);
        ctx
    }

    async fn register_local_listing(ctx: &SessionContext, path: &Path, name: &str) {
        register_listing(
            ctx,
            ListingTableUrl::parse(path.to_str().unwrap()).unwrap(),
            name,
        )
        .await;
    }

    async fn register_listing(ctx: &SessionContext, url: ListingTableUrl, name: &str) {
        let config = ListingTableConfig::new(url)
            .with_listing_options(ListingOptions::new(Arc::new(FlatGeobufFormat::default())))
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        let table = ListingTable::try_new(config).unwrap();
        ctx.register_table(name, Arc::new(table)).unwrap();
    }

    fn single_nonempty_batch(batches: &[RecordBatch]) -> &RecordBatch {
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        batches.iter().find(|batch| batch.num_rows() > 0).unwrap()
    }

    fn collect_i32_column(batches: &[RecordBatch], name: &str) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    async fn collect_ids(ctx: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> Vec<i32> {
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        collect_i32_column(&batches, "id")
    }

    fn find_flatgeobuf_source(plan: &Arc<dyn ExecutionPlan>) -> Option<&FlatGeobufSource> {
        let plan_any = plan.as_ref() as &dyn Any;
        if let Some(exec) = plan_any.downcast_ref::<DataSourceExec>() {
            return exec
                .downcast_to_file_source::<FlatGeobufSource>()
                .map(|(_, source)| source);
        }

        plan.children().into_iter().find_map(find_flatgeobuf_source)
    }

    fn plan_contains_node(plan: &dyn ExecutionPlan, name: &str) -> bool {
        plan.name() == name
            || plan
                .children()
                .into_iter()
                .any(|child| plan_contains_node(child.as_ref(), name))
    }

    struct TestHttpServer {
        base_url: String,
        task: JoinHandle<()>,
    }

    impl TestHttpServer {
        async fn start(bytes: Vec<u8>) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = listener.local_addr().unwrap();
            let bytes = Arc::new(bytes);
            let task = tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    let bytes = Arc::clone(&bytes);
                    tokio::spawn(async move {
                        serve_http_connection(&mut stream, bytes).await;
                    });
                }
            });

            Self {
                base_url: format!("http://{addr}"),
                task,
            }
        }
    }

    impl Drop for TestHttpServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn serve_http_connection(stream: &mut TcpStream, bytes: Arc<Vec<u8>>) {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer).await.unwrap();
            if read == 0 {
                return;
            }
            request.extend_from_slice(&buffer[..read]);
        }

        let request = String::from_utf8_lossy(&request);
        let mut lines = request.split("\r\n");
        let method = lines
            .next()
            .and_then(|line| line.split_whitespace().next())
            .unwrap();
        let range = lines.find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("range")
                .then(|| value.trim().to_string())
        });

        let total_size = bytes.len();
        let is_head = method == "HEAD";
        let (status, start, end, content_length, content_range) = if is_head {
            ("200 OK", 0, 0, total_size, None)
        } else if let Some((start, end)) = range
            .as_deref()
            .and_then(|value| parse_http_range(value, total_size))
        {
            (
                "206 Partial Content",
                start,
                end,
                end - start,
                Some(format!("bytes {start}-{}/{total_size}", end - 1)),
            )
        } else {
            ("200 OK", 0, total_size, total_size, None)
        };

        let mut response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {content_length}\r\nAccept-Ranges: bytes\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n"
        );
        if let Some(content_range) = content_range {
            response.push_str(&format!("Content-Range: {content_range}\r\n"));
        }
        response.push_str("\r\n");
        stream.write_all(response.as_bytes()).await.unwrap();

        if !is_head {
            stream.write_all(&bytes[start..end]).await.unwrap();
        }
        stream.shutdown().await.ok();
    }

    fn parse_http_range(value: &str, total_size: usize) -> Option<(usize, usize)> {
        let range = value.trim().strip_prefix("bytes=")?;
        let (start, end) = range.split_once('-')?;
        let start = start.parse::<usize>().ok()?;
        if start >= total_size {
            return None;
        }
        let end = if end.is_empty() {
            total_size
        } else {
            end.parse::<usize>().ok()?.min(total_size - 1) + 1
        };
        (start < end).then_some((start, end))
    }

    #[tokio::test]
    async fn test_write_flatgeobuf_sink() {
        let ctx = flatgeobuf_context();

        let (batches, schema) = sample_table();
        let mem_table = Arc::new(MemTable::try_new(schema.clone(), vec![batches]).unwrap());
        ctx.register_table("mem_table", mem_table).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test_fgb_sink.fgb");

        ctx.sql(&format!("COPY mem_table TO '{}';", file_path.display()))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let file = File::open(&file_path).unwrap();
        let reader = BufReader::new(file);
        let fgb_reader = flatgeobuf::FgbReader::open(reader).unwrap();
        assert_eq!(fgb_reader.header().features_count(), 3);
    }
}
