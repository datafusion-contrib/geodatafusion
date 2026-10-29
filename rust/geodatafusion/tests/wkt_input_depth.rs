//! Public SQL regressions for the WKT input nesting bound.
//!
//! `wkt` 0.14 parses collections recursively without a depth limit, so the
//! shared input boundary rejects deeper input instead of overflowing the stack.
//! Typed WKT storage keeps serialized text readable through `ST_AsText`, which
//! never parses, and every parsing consumer reports the same depth error.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geoarrow_schema::{Metadata, WktType};

const DEPTH_ERROR: &str = "geometry collection nesting depth exceeds 128";

fn new_ctx() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

fn nested_wkt(depth: usize, innermost: &str) -> String {
    let mut value = String::with_capacity(depth * 20 + innermost.len());
    for _ in 0..depth {
        value.push_str("GEOMETRYCOLLECTION(");
    }
    value.push_str(innermost);
    for _ in 0..depth {
        value.push(')');
    }
    value
}

fn typed_wkt_batch(rows: &[Option<String>]) -> RecordBatch {
    let field = Field::new("geometry", DataType::Utf8, true)
        .with_extension_type(WktType::new(Arc::new(Metadata::default())));
    let array: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|row| row.as_deref()),
    ));
    RecordBatch::try_new(Arc::new(Schema::new([Arc::new(field)])), vec![array]).unwrap()
}

async fn query_batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan failed for {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("execution failed for {sql}: {error}"))
        .into_iter()
        .next()
        .expect("SELECT should return one batch")
}

async fn query_error(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Ok(dataframe) => dataframe
            .collect()
            .await
            .expect_err("query should fail")
            .to_string(),
        Err(error) => error.to_string(),
    }
}

#[tokio::test]
async fn deep_wkt_text_inputs_report_the_depth_bound() {
    let ctx = new_ctx();
    for depth in [500, 1000] {
        let deep = nested_wkt(depth, "LINESTRING(0 0,3 4)");
        let queries = [
            format!("SELECT ST_GeomFromText('{deep}')"),
            format!("SELECT ST_GeomFromEWKT('SRID=4326;{deep}')"),
        ];
        for sql in queries {
            let error = query_error(&ctx, &sql).await;
            assert!(error.contains(DEPTH_ERROR), "{depth}: {error}");
        }
    }

    // The EMPTY member repair walks the same parser, so it is bounded too.
    let deep = nested_wkt(1000, "MULTIPOINT(EMPTY)");
    let error = query_error(&ctx, &format!("SELECT ST_GeomFromText('{deep}')")).await;
    assert!(error.contains(DEPTH_ERROR), "{error}");
}

#[tokio::test]
async fn nested_wkt_text_inside_the_bound_keeps_the_native_rejection() {
    let ctx = new_ctx();
    let text = nested_wkt(128, "LINESTRING(0 0,3 4)");
    let error = query_error(&ctx, &format!("SELECT ST_GeomFromText('{text}')")).await;
    assert!(
        error.contains("nested GeometryCollection is not supported"),
        "expected the native nested-collection rejection, got: {error}"
    );
}

#[tokio::test]
async fn deep_typed_wkt_consumers_report_the_depth_bound() {
    let ctx = new_ctx();
    let deep = nested_wkt(1000, "LINESTRING(0 0,3 4)");
    ctx.register_batch("typed_wkt", typed_wkt_batch(&[Some(deep.clone())]))
        .unwrap();

    // Serialized text is passed through byte-for-byte, so no parse happens.
    let batch = query_batch(&ctx, "SELECT ST_AsText(geometry) FROM typed_wkt").await;
    assert_eq!(batch.column(0).as_string::<i32>().value(0), deep);

    for sql in [
        "SELECT ST_Length(geometry) FROM typed_wkt",
        "SELECT ST_3DLength(geometry) FROM typed_wkt",
        "SELECT ST_Area(geometry) FROM typed_wkt",
        "SELECT ST_Summary(geometry) FROM typed_wkt",
        "SELECT ST_IsClosed(geometry) FROM typed_wkt",
        "SELECT ST_IsEmpty(geometry) FROM typed_wkt",
        "SELECT ST_MemSize(geometry) FROM typed_wkt",
    ] {
        let error = query_error(&ctx, sql).await;
        assert!(error.contains(DEPTH_ERROR), "{sql}: {error}");
    }
}

#[tokio::test]
async fn nested_typed_wkt_reads_inside_the_bound_stay_positive() {
    for depth in [1, 128] {
        let ctx = new_ctx();
        let nested = nested_wkt(depth, "LINESTRING(0 0,3 4)");
        ctx.register_batch(
            "typed_wkt",
            typed_wkt_batch(&[Some(nested.clone()), None, Some("POINT(1 2)".to_string())]),
        )
        .unwrap();

        let batch = query_batch(
            &ctx,
            "SELECT ST_Length(geometry), ST_AsText(geometry), ST_Summary(geometry), ST_Area(geometry) \
             FROM typed_wkt",
        )
        .await;
        let lengths = batch.column(0).as_primitive::<Float64Type>();
        assert_eq!(lengths.value(0), 5.0, "depth {depth}");
        assert!(lengths.is_null(1), "depth {depth}");
        assert_eq!(lengths.value(2), 0.0, "depth {depth}");
        let texts = batch.column(1).as_string::<i32>();
        assert_eq!(texts.value(0), nested, "depth {depth}");
        assert!(texts.is_null(1), "depth {depth}");
        let summaries = batch.column(2).as_string::<i32>();
        assert!(summaries.value(0).contains("LineString[] with 2 points"));
        assert!(summaries.is_null(1), "depth {depth}");
        let areas = batch.column(3).as_primitive::<Float64Type>();
        assert_eq!(areas.value(0), 0.0, "depth {depth}");
    }
}

#[tokio::test]
async fn typed_wkt_slices_only_validate_visible_rows() {
    let ctx = new_ctx();
    let deep = nested_wkt(1000, "LINESTRING(0 0,3 4)");
    let batch = typed_wkt_batch(&[Some(deep), Some("LINESTRING(0 0,3 4)".to_string()), None]);
    ctx.register_batch("typed_wkt", batch.slice(1, 2)).unwrap();

    let batch = query_batch(
        &ctx,
        "SELECT ST_Length(geometry), ST_AsText(geometry) FROM typed_wkt",
    )
    .await;
    let lengths = batch.column(0).as_primitive::<Float64Type>();
    assert_eq!(lengths.value(0), 5.0);
    assert!(lengths.is_null(1));
    let texts = batch.column(1).as_string::<i32>();
    assert_eq!(texts.value(0), "LINESTRING(0 0,3 4)");
}

#[tokio::test]
async fn deep_plain_wkt_inputs_to_geohash_report_the_depth_bound() {
    let ctx = new_ctx();
    for depth in [500, 1000] {
        let deep = nested_wkt(depth, "POINT(1 2)");
        let error = query_error(&ctx, &format!("SELECT ST_GeoHash('{deep}')")).await;
        assert!(error.contains(DEPTH_ERROR), "{depth}: {error}");
    }

    // Legal plain WKT still reaches the bounding-box center path.
    let legal = nested_wkt(2, "POINT(1 2)");
    let batch = query_batch(&ctx, &format!("SELECT ST_GeoHash('{legal}')")).await;
    assert!(!batch.column(0).is_null(0));
}
