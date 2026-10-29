//! Linear referencing contracts checked through the public registration entry point.
//!
//! Expected values were collected from PostGIS 3.6.4 (PostgreSQL 18.6,
//! GEOS 3.14.1) using the pinned oracle instance. Property checks cover the
//! inverse relationship between interpolation and location, and substring
//! length partitioning.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::registry::FunctionRegistry;
use datafusion::prelude::SessionContext;
use geo_traits::{
    CoordTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};
use geoarrow_array::array::{
    LineStringArray, MultiLineStringArray, MultiPolygonArray, PointArray, from_arrow_array,
};
use geoarrow_array::builder::{
    LineStringBuilder, MultiLineStringBuilder, MultiPolygonBuilder, PolygonBuilder,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{
    CoordType, Crs, Dimension, Edges, LineStringType, Metadata, MultiLineStringType,
    MultiPolygonType, PointType, PolygonType,
};
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString,
    MultiLineString as WktMultiLineString, MultiPolygon as WktMultiPolygon, Polygon as WktPolygon,
};

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

async fn collect_one(ctx: &SessionContext, sql: &str) -> RecordBatch {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan failed for {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("execution failed for {sql}: {error}"));
    assert_eq!(batches.len(), 1, "unexpected batch count for {sql}");
    batches.into_iter().next().unwrap()
}

async fn scalar_text(ctx: &SessionContext, expression: &str) -> Option<String> {
    let batch = collect_one(ctx, &format!("SELECT ST_AsText({expression})")).await;
    let values = batch.column(0).as_string::<i32>();
    (!values.is_null(0)).then(|| values.value(0).to_string())
}

async fn scalar_f64(ctx: &SessionContext, expression: &str) -> Option<f64> {
    let batch = collect_one(ctx, &format!("SELECT {expression}")).await;
    let values = batch.column(0).as_primitive::<Float64Type>();
    (!values.is_null(0)).then(|| values.value(0))
}

async fn execution_error(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Ok(dataframe) => match dataframe.collect().await {
            Ok(_) => panic!("expected error for {sql}"),
            Err(error) => error.to_string(),
        },
        Err(error) => error.to_string(),
    }
}

#[test]
fn registry_contains_all_linear_reference_functions() {
    let ctx = context();
    let names = ctx.state().udfs();
    for name in [
        "st_lineinterpolatepoint",
        "st_3dlineinterpolatepoint",
        "st_lineinterpolatepoints",
        "st_linelocatepoint",
        "st_linesubstring",
        "st_locatealong",
        "st_locatebetween",
        "st_locatebetweenelevations",
        "st_interpolatepoint",
        "st_addmeasure",
    ] {
        assert!(names.contains(name), "missing {name}: {names:#?}");
    }
}

#[tokio::test]
async fn postgis_interpolation_examples() {
    let ctx = context();

    assert_eq!(
        scalar_f64(
            &ctx,
            "ST_X(ST_LineInterpolatePoint('LINESTRING(0 0,10 0)', 0.5))"
        )
        .await,
        Some(5.0)
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING(25 50,100 125,150 190)'), 0.2)",
        )
        .await
        .as_deref(),
        Some("POINT(51.5974135047432 76.5974135047432)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING ZM (0 0 100 10,10 0 200 20)'), 0.5)",
        )
        .await
        .as_deref(),
        Some("POINT ZM(5 0 150 15)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_3DLineInterpolatePoint(ST_GeomFromText('LINESTRING Z (25 50 70,100 125 90,150 190 200)'), 0.2)",
        )
        .await
        .as_deref(),
        Some("POINT Z(59.06758929108215 84.06758929108216 79.0846904776219)")
    );

    let two_d = scalar_f64(
        &ctx,
        "ST_M(ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING Z (0 0 0,0 0 10,10 0 10)'), 0.5))",
    )
    .await;
    assert_eq!(two_d, None, "2D interpolation cannot invent an M value");
    assert_eq!(
        scalar_f64(
            &ctx,
            "ST_Z(ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING Z (0 0 0,0 0 10,10 0 10)'), 0.5))",
        )
        .await,
        Some(10.0)
    );
    assert_eq!(
        scalar_f64(
            &ctx,
            "ST_Z(ST_3DLineInterpolatePoint(ST_GeomFromText('LINESTRING Z (0 0 0,0 0 10,10 0 10)'), 0.5))",
        )
        .await,
        Some(10.0)
    );

    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineInterpolatePoints(ST_GeomFromText('LINESTRING(0 0,10 0)'), 0.2)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT((2 0),(4 0),(6.000000000000001 0),(8 0),(10 0))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineInterpolatePoints(ST_GeomFromText('LINESTRING(0 0,10 0)'), 0.6)",
        )
        .await
        .as_deref(),
        Some("POINT(6 0)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineInterpolatePoints(ST_GeomFromText('LINESTRING(0 0,10 0)'), 0.2, false)",
        )
        .await
        .as_deref(),
        Some("POINT(2 0)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineInterpolatePoints(ST_GeomFromText('LINESTRING Z (0 0 0,0 0 10,10 0 10)'), 0.5)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT Z((5 0 10),(10 0 10))")
    );
}

#[tokio::test]
async fn postgis_locate_and_substring_examples() {
    let ctx = context();

    assert_eq!(
        scalar_f64(
            &ctx,
            "ST_LineLocatePoint(ST_GeomFromText('LINESTRING(0 0,10 0,10 10)'), ST_GeomFromText('POINT(15 5)'))",
        )
        .await,
        Some(0.75)
    );
    assert_eq!(
        scalar_f64(
            &ctx,
            "ST_LineLocatePoint(ST_GeomFromText('LINESTRING(0 1,0 1,0 1)'), ST_GeomFromText('POINT(0 1)'))",
        )
        .await,
        Some(0.0)
    );
    assert_eq!(
        scalar_f64(
            &ctx,
            "ST_InterpolatePoint(ST_GeomFromText('LINESTRING M (0 0 0,10 0 20)'), ST_GeomFromText('POINT(5 5)'))",
        )
        .await,
        Some(10.0)
    );

    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineSubstring(ST_GeomFromText('LINESTRING ZM (0 0 0 10,10 0 10 20)'), 0.25, 0.75)",
        )
        .await
        .as_deref(),
        Some("LINESTRING ZM(2.5 0 2.5 12.5,7.5 0 7.5 17.5)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineSubstring(ST_GeomFromText('LINESTRING(0 0,10 0)'), 0.5, 0.5)",
        )
        .await
        .as_deref(),
        Some("POINT(5 0)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineSubstring(ST_GeomFromText('MULTILINESTRING((0 0,10 0),(20 0,30 0))'), 0.5, 0.5)",
        )
        .await
        .as_deref(),
        Some("GEOMETRYCOLLECTION(POINT(10 0),POINT(20 0))")
    );
}

#[tokio::test]
async fn postgis_measure_functions_examples() {
    let ctx = context();

    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateAlong(ST_GeomFromText('MULTILINESTRING M ((1 2 3,3 4 2,9 4 3),(1 2 3,5 4 5))'), 3)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M((1 2 3),(9 4 3),(1 2 3))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateAlong(ST_GeomFromText('LINESTRING M (0 0 0,10 0 10)'), 5, 2)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M((5 2 5))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetween(ST_GeomFromText('LINESTRING M (0 0 0,10 0 10)'), 8, 2)",
        )
        .await
        .as_deref(),
        Some("MULTILINESTRING M((2 0 2,8 0 8))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetween(ST_GeomFromText('LINESTRING M (0 0 0,10 0 10,20 0 0)'), 10, 15)",
        )
        .await
        .as_deref(),
        Some("GEOMETRYCOLLECTION M(POINT M(10 0 10))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetweenElevations(ST_GeomFromText('LINESTRING Z (1 2 3,4 5 6)'), 2, 4)",
        )
        .await
        .as_deref(),
        Some("MULTILINESTRING Z((1 2 3,2 3 4))")
    );

    assert_eq!(
        scalar_text(
            &ctx,
            "ST_AddMeasure(ST_GeomFromText('LINESTRING(0 0,10 0)'), 1, 4)",
        )
        .await
        .as_deref(),
        Some("LINESTRING M(0 0 1,10 0 4)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_AddMeasure(ST_GeomFromText('MULTILINESTRING((0 0,10 0),(20 0,30 0))'), 1, 4)",
        )
        .await
        .as_deref(),
        Some("MULTILINESTRING M((0 0 1,10 0 2.5),(20 0 2.5,30 0 4))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_AddMeasure(ST_GeomFromText('LINESTRING Z (0 0 5,10 0 6)'), 1, 4)",
        )
        .await
        .as_deref(),
        Some("LINESTRING ZM(0 0 5 1,10 0 6 4)")
    );
}

#[tokio::test]
async fn postgis_degenerate_point_multipoint_and_reverse_range_contracts() {
    let ctx = context();

    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING ZM (0 0 1 10,0 0 2 20)'), 0.5)",
        )
        .await
        .as_deref(),
        Some("POINT ZM(0 0 2 20)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_3DLineInterpolatePoint(ST_GeomFromText('LINESTRING ZM (0 0 1 10,0 0 2 20)'), 0.5)",
        )
        .await
        .as_deref(),
        Some("POINT ZM(0 0 1.5 15)")
    );
    assert_eq!(
        scalar_f64(
            &ctx,
            "ST_LineLocatePoint(ST_GeomFromText('LINESTRING ZM (0 0 1 10,0 0 2 20)'), ST_GeomFromText('POINT(0 0)'))",
        )
        .await,
        Some(1.0)
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineSubstring(ST_GeomFromText('LINESTRING ZM (0 0 1 10,0 0 2 20)'), 0.2, 0.8)",
        )
        .await
        .as_deref(),
        Some("POINT ZM(0 0 2 20)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineSubstring(ST_GeomFromText('LINESTRING ZM (0 0 1 10,0 0 2 20,0 0 3 30)'), 0.2, 0.8)",
        )
        .await
        .as_deref(),
        Some("LINESTRING ZM(0 0 2 20,0 0 3 30)")
    );
    assert_eq!(
        scalar_f64(
            &ctx,
            "ST_LineLocatePoint(ST_GeomFromText('LINESTRING ZM (0 0 1 10,0 0 2 20,0 0 3 30)'), ST_GeomFromText('POINT(0 0)'))",
        )
        .await,
        Some(0.0)
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineSubstring(ST_GeomFromText('MULTILINESTRING ZM ((0 0 1 10,0 0 2 20),(0 0 3 30,0 0 4 40))'), 0.2, 0.8)",
        )
        .await
        .as_deref(),
        Some("GEOMETRYCOLLECTION ZM(POINT ZM(0 0 2 20),POINT ZM(0 0 4 40))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineSubstring(ST_GeomFromText('LINESTRING Z (0 0 0,0 0 1,0 0 2,0 0 3)'), 0.2, 0.8)",
        )
        .await
        .as_deref(),
        Some("LINESTRING Z(0 0 2,0 0 3)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineSubstring(ST_GeomFromText('MULTILINESTRING Z ((0 0 0,0 0 1,0 0 2),(1 1 0,1 1 1,1 1 2))'), 0.2, 0.8)",
        )
        .await
        .as_deref(),
        Some("MULTILINESTRING Z((0 0 1,0 0 2),(1 1 1,1 1 2))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineSubstring(ST_GeomFromText('MULTILINESTRING((0 0,10 0),(10 0,10 0))'), 0.5, 0.5)",
        )
        .await
        .as_deref(),
        Some("GEOMETRYCOLLECTION(POINT(5 0))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineSubstring(ST_GeomFromText('MULTILINESTRING((0 0,10 0),(10 0,10 0))'), 0.5, 1)",
        )
        .await
        .as_deref(),
        Some("GEOMETRYCOLLECTION(LINESTRING(5 0,10 0),POINT(10 0))")
    );

    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineSubstring(ST_GeomFromText('MULTILINESTRING((0 0,10 0),(20 0,30 0))'), 0.25, 0.75)",
        )
        .await
        .as_deref(),
        Some("MULTILINESTRING((5 0,10 0),(20 0,25 0))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateAlong(ST_GeomFromText('POINT M (1 2 3)'), 3)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M((1 2 3))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetween(ST_GeomFromText('MULTIPOINT M ((1 2 2),(2 2 5),(2 1 0))'), 5, 2)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M((1 2 2),(2 2 5))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetweenElevations(ST_GeomFromText('LINESTRING Z (1 2 3,4 5 6)'), 4, 2)",
        )
        .await
        .as_deref(),
        Some("MULTILINESTRING Z((1 2 3,2 3 4))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetweenElevations(ST_GeomFromText('LINESTRING Z (1 2 3,4 5 6)'), 3, 3)",
        )
        .await
        .as_deref(),
        Some("GEOMETRYCOLLECTION Z(POINT Z(1 2 3))")
    );

    assert_eq!(
        scalar_text(
            &ctx,
            "ST_AddMeasure(ST_GeomFromText('LINESTRING M (0 0 99,10 0 99)'), 1, 4)",
        )
        .await
        .as_deref(),
        Some("LINESTRING M(0 0 1,10 0 4)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_AddMeasure(ST_GeomFromText('LINESTRING(0 0,10 0)'), 4, 1)",
        )
        .await
        .as_deref(),
        Some("LINESTRING M(0 0 4,10 0 1)")
    );

    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetween(ST_GeomFromText('LINESTRING M (0 0 0,10 0 10,20 0 0)'), 10, 15, 2)",
        )
        .await
        .as_deref(),
        Some("MULTILINESTRING EMPTY")
    );
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn locate_between_nonzero_offset_uses_geos_offset_curve() {
    let ctx = context();
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetween(ST_GeomFromText('LINESTRING M (0 0 0,10 0 10,10 10 20)'), 2, 18, 2)",
        )
        .await
        .as_deref(),
        Some("MULTILINESTRING((2 2,8 2,8 8))")
    );
}

#[tokio::test]
async fn postgis_lrs_regression_cases_for_zm_clipping() {
    let ctx = context();
    let cases = [
        (
            "ST_LocateBetween(ST_GeomFromText('LINESTRING ZM (0 10 100 0, 0 0 0 10)'), 2, 18)",
            "MULTILINESTRING ZM((0 8 80 2,0 0 0 10))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('LINESTRING ZM (0 10 100 0, 0 0 0 20, 10 0 0 0)'), 2, 18)",
            "MULTILINESTRING ZM((0 9 90 2,0 1 10 18),(1 0 0 18,9 0 0 2))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('LINESTRING ZM (0 10 100 0, 0 0 0 20, 0 10 10 40, 10 0 0 0)'), 2, 18)",
            "MULTILINESTRING ZM((0 9 90 2,0 1 10 18),(5.5 4.5 4.5 18,9.5 0.5 0.5 2))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('LINESTRING ZM (0 10 10 40, 10 0 0 0)'), 2, 2)",
            "MULTIPOINT ZM((9.5 0.5 0.5 2))",
        ),
        (
            "ST_LocateBetweenElevations(ST_GeomFromText('LINESTRING Z (1 2 6,4 5 -1,7 8 9)'), 6, 9)",
            "GEOMETRYCOLLECTION Z(POINT Z(1 2 6),LINESTRING Z(6.1 7.1 6,7 8 9))",
        ),
        (
            "ST_LocateBetweenElevations(ST_GeomFromText('GEOMETRYCOLLECTION Z (LINESTRING Z (0 0 0,10 10 10))'), 2, 5)",
            "MULTILINESTRING Z((2 2 2,5 5 5))",
        ),
        (
            "ST_LocateBetweenElevations(ST_GeomFromText('MULTILINESTRING Z EMPTY'), 2, 5)",
            "MULTILINESTRING Z EMPTY",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('MULTILINESTRING M ((0 0 0,10 0 10),(20 0 20,30 0 30))'), 5, 25)",
            "MULTILINESTRING M((5 0 5,10 0 10),(20 0 20,25 0 25))",
        ),
        (
            "ST_LocateBetweenElevations(ST_GeomFromText('LINESTRING ZM (1 2 3 5,4 5 6 10)'), 3, 4)",
            "MULTILINESTRING ZM((1 2 3 5,2 3 4 6.666666666666666))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('POINT M (1 2 3)'), 10, 20)",
            "MULTIPOINT M EMPTY",
        ),
    ];
    for (expression, expected) in cases {
        assert_eq!(
            scalar_text(&ctx, expression).await.as_deref(),
            Some(expected),
            "failed for {expression}"
        );
    }
}

#[tokio::test]
async fn measure_interpolation_and_empty_point_contracts() {
    let ctx = context();
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineInterpolatePoints(ST_GeomFromText('LINESTRING ZM (0 0 3 10,10 0 13 20)'), 0.5)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT ZM((5 0 8 15),(10 0 13 20))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateAlong(ST_GeomFromText('LINESTRING M (0 0 5,10 0 5)'), 5)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M((5 0 5))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateAlong(ST_GeomFromText('LINESTRING M (0 0 0,10 0 10)'), 5, -2)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M((5 -2 5))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateAlong(ST_GeomFromText('POINT M (1 2 3)'), 3, 10)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M((1 2 3))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateAlong(ST_GeomFromText('LINESTRING M (0 0 0,10 0 10,0 0 0)'), 0)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M((0 0 0))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateAlong(ST_GeomFromText('MULTILINESTRING M (EMPTY,(0 0 1,10 0 2))'), 1.5)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M((5 0 1.5))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetween(ST_GeomFromText('LINESTRING M (0 0 0,10 0 10)'), 5, 5, 2)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M((5 2 5))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetween(ST_GeomFromText('POINT M EMPTY'), 1, 2)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M EMPTY")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetweenElevations(ST_GeomFromText('POINT Z EMPTY'), 1, 2)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT Z EMPTY")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_AddMeasure(ST_GeomFromText('LINESTRING(0 0,0 0,0 0)'), 1, 4)",
        )
        .await
        .as_deref(),
        Some("LINESTRING M(0 0 1,0 0 2.5,0 0 4)")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_AddMeasure(ST_GeomFromText('MULTILINESTRING((0 0,0 0),(0 0,0 0))'), 1, 4)",
        )
        .await
        .as_deref(),
        Some("MULTILINESTRING M((0 0 NaN,0 0 NaN),(0 0 NaN,0 0 NaN))")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_AddMeasure(ST_GeomFromText('MULTILINESTRING M ((1 0 4,2 0 4,4 0 4),(1 0 4,2 0 4,4 0 4))'), 10, 70)",
        )
        .await
        .as_deref(),
        Some("MULTILINESTRING M((1 0 10,2 0 20,4 0 40),(1 0 40,2 0 50,4 0 70))")
    );
    // PostGIS 3.6.4 returns a case-specific second point for this zero-length
    // repeated interpolation. The shared oracle intentionally omits the case;
    // returning the last vertex repeatedly keeps this result deterministic
    // while preserving the Z/M dimension.
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineInterpolatePoints(ST_GeomFromText('LINESTRING ZM (0 0 1 10,0 0 2 20,0 0 3 30)'), 0.5)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT ZM((0 0 3 30),(0 0 3 30))")
    );
}

#[tokio::test]
async fn postgis_polygon_and_multipolygon_clipping_contracts() {
    let ctx = context();
    let cases = [
        (
            "ST_LocateBetween(ST_GeomFromText('POLYGON M ((0 0 0,4 0 4,4 4 8,0 4 4,0 0 0))'), 2, 6)",
            "MULTIPOLYGON M(((2 0 2,4 0 4,4 2 6,2 4 6,0 4 4,0 2 2,2 0 2)))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('POLYGON M ((0 0 0,4 0 4,4 4 8,0 4 4,0 0 0))'), 6, 2)",
            "MULTIPOLYGON M(((2 0 2,4 0 4,4 2 6,2 4 6,0 4 4,0 2 2,2 0 2)))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('POLYGON M ((0 0 0,4 0 4,4 4 8,0 4 4,0 0 0))'), 20, 30)",
            "MULTIPOLYGON M EMPTY",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('POLYGON M EMPTY'), 2, 6)",
            "MULTIPOLYGON M EMPTY",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('POLYGON M ((0 0 0,10 0 10,10 10 20,0 10 10,0 0 0),(2 2 3,4 2 5,4 4 7,2 4 5,2 2 3))'), 2, 8)",
            "MULTIPOLYGON M(((2 0 2,8 0 8,0 8 8,0 2 2,2 0 2),(2 2 3,4 2 5,4 4 7,2 4 5,2 2 3)))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('POLYGON M ((0 0 0,10 0 10,10 10 20,0 10 10,0 0 0),(2 2 3,4 2 5,4 4 7,2 4 5,2 2 3))'), 4, 6)",
            "MULTIPOLYGON M(((4 0 4,6 0 6,0 6 6,0 4 4,4 0 4),(3 2 4,4 2 5,4 3 6,3 4 6,2 4 5,2 3 4,3 2 4)))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('POLYGON M ((0 0 0,10 0 10,10 10 20,0 10 10,0 0 0),(2 2 3,4 2 5,4 4 7,2 4 5,2 2 3))'), 0, 2)",
            "MULTIPOLYGON M(((0 0 0,2 0 2,0 2 2,0 0 0)))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('MULTIPOLYGON M (((0 0 0,4 0 4,4 4 8,0 4 4,0 0 0)),((10 0 20,14 0 24,14 4 28,10 4 24,10 0 20)))'), 2, 6)",
            "MULTIPOLYGON M(((2 0 2,4 0 4,4 2 6,2 4 6,0 4 4,0 2 2,2 0 2)))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('MULTIPOLYGON M EMPTY'), 2, 6)",
            "MULTIPOLYGON M EMPTY",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('POLYGON ZM ((0 0 0 0,10 0 10 10,10 10 20 20,0 10 10 10,0 0 0 0))'), 2, 8)",
            "MULTIPOLYGON ZM(((2 0 2 2,8 0 8 8,0 8 8 8,0 2 2 2,2 0 2 2)))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('GEOMETRYCOLLECTION M (POLYGON M ((0 0 0,4 0 4,4 4 8,0 4 4,0 0 0)))'), 2, 6)",
            "MULTIPOLYGON M(((2 0 2,4 0 4,4 2 6,2 4 6,0 4 4,0 2 2,2 0 2)))",
        ),
        (
            "ST_LocateBetween(ST_GeomFromText('GEOMETRYCOLLECTION M (POLYGON M ((0 0 0,4 0 4,4 4 8,0 4 4,0 0 0)),LINESTRING M (0 0 0,4 0 4),POINT M (2 2 5))'), 2, 5)",
            "GEOMETRYCOLLECTION M(POLYGON M((2 0 2,4 0 4,4 1 5,1 4 5,0 4 4,0 2 2,2 0 2)),LINESTRING M(2 0 2,4 0 4),POINT M(2 2 5))",
        ),
        (
            "ST_LocateBetweenElevations(ST_GeomFromText('POLYGON Z ((0 0 0,4 0 4,4 4 8,0 4 4,0 0 0))'), 6, 2)",
            "MULTIPOLYGON Z(((2 0 2,4 0 4,4 2 6,2 4 6,0 4 4,0 2 2,2 0 2)))",
        ),
        (
            "ST_LocateBetweenElevations(ST_GeomFromText('MULTIPOLYGON Z (((0 0 0,4 0 4,4 4 8,0 4 4,0 0 0)),((10 0 0,14 0 4,14 4 8,10 4 4,10 0 0)))'), 2, 6)",
            "MULTIPOLYGON Z(((2 0 2,4 0 4,4 2 6,2 4 6,0 4 4,0 2 2,2 0 2)),((12 0 2,14 0 4,14 2 6,12 4 6,10 4 4,10 2 2,12 0 2)))",
        ),
        (
            "ST_LocateBetweenElevations(ST_GeomFromText('POLYGON Z EMPTY'), 2, 6)",
            "MULTIPOLYGON Z EMPTY",
        ),
        (
            "ST_LocateBetweenElevations(ST_GeomFromText('POLYGON Z ((0 0 0,4 0 4,4 4 8,0 4 4,0 0 0))'), 4, 4)",
            "MULTIPOLYGON Z EMPTY",
        ),
    ];
    for (expression, expected) in cases {
        assert_eq!(
            scalar_text(&ctx, expression).await.as_deref(),
            Some(expected),
            "failed for {expression}"
        );
    }

    let error = execution_error(
        &ctx,
        "SELECT ST_LocateBetween(ST_GeomFromText('POLYGON M ((0 0 0,4 0 4,4 4 8,0 4 4,0 0 0))'), 4, 4)",
    )
    .await;
    assert!(
        error.contains("Only linear geometries are supported"),
        "{error}"
    );
    let error = execution_error(
        &ctx,
        "SELECT ST_LocateBetween(ST_GeomFromText('POLYGON M ((0 0 0,4 0 4,4 4 8,0 4 4,0 0 0))'), 2, 6, 2)",
    )
    .await;
    assert!(
        error.contains("unexpected type (Polygon) in the offset routine"),
        "{error}"
    );
    // TIN has no WKT representation in the current `wkt`/GeoArrow surface,
    // so the input is rejected before linear referencing runs.
    let error = execution_error(
        &ctx,
        "SELECT ST_LocateBetween(ST_GeomFromText('TIN M (((0 0 0,4 0 4,0 4 4,0 0 0)))'), 2, 6)",
    )
    .await;
    assert!(!error.is_empty());
}

#[tokio::test]
async fn null_empty_and_malformed_arguments_are_distinct() {
    let ctx = context();

    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING M EMPTY'), 0.5)",
        )
        .await
        .as_deref(),
        Some("POINT M EMPTY")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateAlong(ST_GeomFromText('LINESTRING M EMPTY'), 1)",
        )
        .await
        .as_deref(),
        Some("MULTIPOINT M EMPTY")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_LocateBetween(ST_GeomFromText('LINESTRING M EMPTY'), 2, 8)",
        )
        .await
        .as_deref(),
        Some("MULTILINESTRING M EMPTY")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_AddMeasure(ST_GeomFromText('LINESTRING EMPTY'), 1, 4)",
        )
        .await
        .as_deref(),
        Some("LINESTRING M EMPTY")
    );

    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_LineSubstring(ST_GeomFromText('LINESTRING M EMPTY'), 0, 1) IS NULL,
            ST_LocateAlong(ST_GeomFromText('MULTILINESTRING M EMPTY'), 1) IS NULL,
            ST_LineInterpolatePoint(ST_GeomFromText(NULL), 0.5) IS NULL,
            ST_LocateBetween(ST_GeomFromText(NULL), 2, 8) IS NULL,
            ST_AddMeasure(ST_GeomFromText(NULL), 1, 4) IS NULL
        "#,
    )
    .await;
    for index in 0..5 {
        assert!(
            batch.column(index).as_boolean().value(0),
            "NULL/EMPTY contract failed at column {index}"
        );
    }

    let error = execution_error(
        &ctx,
        "SELECT ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), 1.1)",
    )
    .await;
    assert!(error.contains("isn't within [0,1]"), "{error}");
    let error = execution_error(
        &ctx,
        "SELECT ST_LineSubstring(ST_GeomFromText('LINESTRING(0 0,10 0)'), 0.75, 0.25)",
    )
    .await;
    assert!(error.contains("2nd arg must be smaller"), "{error}");
    let error = execution_error(
        &ctx,
        "SELECT ST_LocateAlong(ST_GeomFromText('LINESTRING(0 0,10 0)'), 1)",
    )
    .await;
    assert!(error.contains("measure dimension"), "{error}");
    let error = execution_error(
        &ctx,
        "SELECT ST_LocateBetweenElevations(ST_GeomFromText('LINESTRING(0 0,10 0)'), 1, 2)",
    )
    .await;
    assert!(error.contains("Z dimensions"), "{error}");
    let error = execution_error(
        &ctx,
        "SELECT ST_InterpolatePoint(ST_GeomFromText('LINESTRING M EMPTY'), ST_GeomFromText('POINT(1 1)'))",
    )
    .await;
    assert!(error.contains("Input geometry is empty"), "{error}");
    let error = execution_error(
        &ctx,
        "SELECT ST_LineLocatePoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), ST_GeomFromText('POINT EMPTY'))",
    )
    .await;
    assert!(error.contains("empty geometry"), "{error}");
    let error = execution_error(
        &ctx,
        "SELECT ST_LocateBetween(ST_GeomFromText('LINESTRING(0 0,10 0)'), 1, 2)",
    )
    .await;
    assert!(error.contains("M dimension"), "{error}");
    let error = execution_error(
        &ctx,
        "SELECT ST_LineSubstring(ST_GeomFromText('POINT(0 0)'), 0, 1)",
    )
    .await;
    assert!(error.contains("isn't a line"), "{error}");
}

#[tokio::test]
async fn sql_null_arguments_are_visible_to_is_null() {
    let ctx = context();

    let bare_null_expressions = [
        "ST_LineInterpolatePoint(NULL, 0.5)",
        "ST_3DLineInterpolatePoint(NULL, 0.5)",
        "ST_LineInterpolatePoints(NULL, 0.5)",
        "ST_LineSubstring(NULL, 0.25, 0.75)",
        "ST_LocateAlong(NULL, 1)",
        "ST_LocateBetween(NULL, 1, 2)",
        "ST_LocateBetweenElevations(NULL, 1, 2)",
        "ST_AddMeasure(NULL, 1, 2)",
        "ST_LineLocatePoint(NULL, NULL)",
        "ST_InterpolatePoint(NULL, NULL)",
        "ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), NULL)",
        "ST_3DLineInterpolatePoint(ST_GeomFromText('LINESTRING Z (0 0 0,10 0 10)'), NULL)",
        "ST_LineInterpolatePoints(ST_GeomFromText('LINESTRING(0 0,10 0)'), NULL)",
        "ST_LineSubstring(ST_GeomFromText('LINESTRING(0 0,10 0)'), NULL, 0.5)",
        "ST_LocateAlong(ST_GeomFromText('LINESTRING M (0 0 0,10 0 10)'), NULL)",
        "ST_LocateBetween(ST_GeomFromText('LINESTRING M (0 0 0,10 0 10)'), NULL, 5)",
        "ST_LocateBetweenElevations(ST_GeomFromText('LINESTRING Z (0 0 0,10 0 10)'), NULL, 5)",
        "ST_AddMeasure(ST_GeomFromText('LINESTRING(0 0,10 0)'), NULL, 5)",
        "ST_LineLocatePoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), NULL)",
        "ST_InterpolatePoint(ST_GeomFromText('LINESTRING M (0 0 0,10 0 10)'), NULL)",
    ];
    for expression in bare_null_expressions {
        let batch = collect_one(&ctx, &format!("SELECT {expression} IS NULL AS is_null")).await;
        assert!(
            batch.column(0).as_boolean().value(0),
            "{expression} was not SQL NULL"
        );
    }

    let metadata = Arc::new(Metadata::default());
    let typ = LineStringType::new(Dimension::XYZM, metadata).with_coord_type(CoordType::Separated);
    let mut builder = LineStringBuilder::new(typ.clone());
    for coords in [
        vec![
            WktCoord {
                x: 0.0,
                y: 0.0,
                z: Some(0.0),
                m: Some(0.0),
            },
            WktCoord {
                x: 10.0,
                y: 0.0,
                z: Some(10.0),
                m: Some(10.0),
            },
        ],
        vec![
            WktCoord {
                x: 0.0,
                y: 0.0,
                z: Some(0.0),
                m: Some(0.0),
            },
            WktCoord {
                x: 0.0,
                y: 10.0,
                z: Some(10.0),
                m: Some(10.0),
            },
        ],
    ] {
        builder
            .push_geometry(Some(&WktLineString::new(coords, WktDimension::XYZM)))
            .unwrap();
    }
    let field = typ.to_field("geometry", false);
    assert!(!field.is_nullable());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Arc::new(field)])),
        vec![builder.finish().to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("non_nullable_lines", batch).unwrap();

    let null_extra_expressions = [
        "ST_LineInterpolatePoint(geometry, NULL)",
        "ST_3DLineInterpolatePoint(geometry, NULL)",
        "ST_LineInterpolatePoints(geometry, NULL)",
        "ST_LineSubstring(geometry, NULL, 0.5)",
        "ST_LineSubstring(geometry, 0.25, NULL)",
        "ST_LocateAlong(geometry, NULL)",
        "ST_LocateBetween(geometry, NULL, 5)",
        "ST_LocateBetween(geometry, 2, NULL)",
        "ST_LocateBetweenElevations(geometry, NULL, 5)",
        "ST_AddMeasure(geometry, NULL, 5)",
        "ST_AddMeasure(geometry, 2, NULL)",
        "ST_LineLocatePoint(geometry, NULL)",
        "ST_InterpolatePoint(geometry, NULL)",
    ];
    for expression in null_extra_expressions {
        let batch = collect_one(
            &ctx,
            &format!("SELECT {expression} IS NULL AS is_null FROM non_nullable_lines"),
        )
        .await;
        let values = batch.column(0).as_boolean();
        assert_eq!(values.len(), 2, "{expression}");
        for row in 0..values.len() {
            assert!(values.value(row), "{expression} row {row} was not SQL NULL");
        }
    }
}

#[tokio::test]
async fn native_null_extra_arguments_preserve_output_schema() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4979".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let typ =
        LineStringType::new(Dimension::XYZM, metadata).with_coord_type(CoordType::Interleaved);
    let mut builder = LineStringBuilder::new(typ.clone());
    for _ in 0..2 {
        builder
            .push_geometry(Some(&WktLineString::new(
                vec![
                    WktCoord {
                        x: 0.0,
                        y: 0.0,
                        z: Some(0.0),
                        m: Some(0.0),
                    },
                    WktCoord {
                        x: 10.0,
                        y: 0.0,
                        z: Some(10.0),
                        m: Some(10.0),
                    },
                ],
                WktDimension::XYZM,
            )))
            .unwrap();
    }
    let field = typ.to_field("geometry", false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Arc::new(field)])),
        vec![builder.finish().to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("non_nullable_lines", batch).unwrap();

    let batch = collect_one(
        &ctx,
        "SELECT ST_LineSubstring(geometry, NULL, 0.5) AS substring_result, \
         ST_LineInterpolatePoint(geometry, NULL) AS point_result, \
         ST_AddMeasure(geometry, NULL, 5) AS measured \
         FROM non_nullable_lines",
    )
    .await;

    let substring_type = batch
        .schema()
        .field(0)
        .try_extension_type::<geoarrow_schema::GeometryType>()
        .unwrap();
    assert_eq!(substring_type.coord_type(), CoordType::Interleaved);
    assert_eq!(substring_type.metadata().crs(), &crs);
    let substring = from_arrow_array(batch.column(0).as_ref(), batch.schema().field(0)).unwrap();
    assert!(substring.is_null(0));

    let point_type = batch
        .schema()
        .field(1)
        .try_extension_type::<PointType>()
        .unwrap();
    assert_eq!(point_type.coord_type(), CoordType::Interleaved);
    assert_eq!(point_type.dimension(), Dimension::XYZM);
    assert_eq!(point_type.metadata().crs(), &crs);
    let points = PointArray::try_from((batch.column(1).as_ref(), batch.schema().field(1))).unwrap();
    assert!(points.is_null(0));

    let measured_type = batch
        .schema()
        .field(2)
        .try_extension_type::<LineStringType>()
        .unwrap();
    assert_eq!(measured_type.coord_type(), CoordType::Interleaved);
    assert_eq!(measured_type.dimension(), Dimension::XYZM);
    assert_eq!(measured_type.metadata().crs(), &crs);
    let measured =
        LineStringArray::try_from((batch.column(2).as_ref(), batch.schema().field(2))).unwrap();
    assert!(measured.is_null(0));
}

#[tokio::test]
async fn native_xyz_m_sliced_mult_row_input_preserves_schema_and_values() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4979".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let typ = LineStringType::new(Dimension::XYZM, metadata.clone())
        .with_coord_type(CoordType::Interleaved);
    let mut builder = LineStringBuilder::new(typ.clone());
    builder
        .push_geometry(Some(&WktLineString::new(
            vec![
                WktCoord {
                    x: 0.0,
                    y: 0.0,
                    z: Some(0.0),
                    m: Some(100.0),
                },
                WktCoord {
                    x: 10.0,
                    y: 0.0,
                    z: Some(10.0),
                    m: Some(200.0),
                },
            ],
            WktDimension::XYZM,
        )))
        .unwrap();
    builder.push_empty();
    builder.push_geometry(None::<&WktLineString>).unwrap();
    builder
        .push_geometry(Some(&WktLineString::new(
            vec![
                WktCoord {
                    x: 0.0,
                    y: 0.0,
                    z: Some(1.0),
                    m: Some(10.0),
                },
                WktCoord {
                    x: 20.0,
                    y: 0.0,
                    z: Some(21.0),
                    m: Some(30.0),
                },
            ],
            WktDimension::XYZM,
        )))
        .unwrap();
    let lines = builder.finish().slice(1, 3);
    let field = typ.to_field("geometry", true);
    let fractions = Float64Array::from(vec![0.25, 0.5, 0.75]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Arc::new(field),
            Arc::new(Field::new("fraction", DataType::Float64, false)),
        ])),
        vec![lines.to_array_ref(), Arc::new(fractions)],
    )
    .unwrap();
    ctx.register_batch("lines", batch).unwrap();

    let batch = collect_one(
        &ctx,
        "SELECT ST_LineInterpolatePoint(geometry, fraction) AS point FROM lines",
    )
    .await;
    let point_type = batch
        .schema()
        .field(0)
        .try_extension_type::<PointType>()
        .unwrap();
    assert_eq!(point_type.coord_type(), CoordType::Interleaved);
    assert_eq!(point_type.dimension(), Dimension::XYZM);
    assert_eq!(point_type.metadata().crs(), &crs);

    let points = PointArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
    assert!(!points.is_null(0), "the sliced first row remains EMPTY");
    let empty_point = points.value(0).unwrap();
    let geo_traits::GeometryType::Point(empty) = empty_point.as_type() else {
        panic!("expected an empty Point");
    };
    assert!(empty.coord().is_none());
    assert!(points.is_null(1), "the sliced second row remains NULL");
    let point = points.value(2).unwrap();
    let geo_traits::GeometryType::Point(point) = point.as_type() else {
        panic!("expected a Point");
    };
    let coord = point.coord().unwrap();
    assert_eq!([coord.x(), coord.y()], [15.0, 0.0]);
    assert_eq!(coord.nth(2), Some(16.0));
    assert_eq!(coord.nth(3), Some(25.0));
}

#[tokio::test]
async fn native_multiline_add_measure_preserves_crs_and_output_type() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:3857".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let typ =
        MultiLineStringType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
    let mut builder = MultiLineStringBuilder::new(typ.clone());
    builder
        .push_geometry(Some(&WktMultiLineString::new(
            vec![
                WktLineString::new(
                    vec![
                        WktCoord {
                            x: 0.0,
                            y: 0.0,
                            z: None,
                            m: None,
                        },
                        WktCoord {
                            x: 10.0,
                            y: 0.0,
                            z: None,
                            m: None,
                        },
                    ],
                    WktDimension::XY,
                ),
                WktLineString::new(
                    vec![
                        WktCoord {
                            x: 20.0,
                            y: 0.0,
                            z: None,
                            m: None,
                        },
                        WktCoord {
                            x: 30.0,
                            y: 0.0,
                            z: None,
                            m: None,
                        },
                    ],
                    WktDimension::XY,
                ),
            ],
            WktDimension::XY,
        )))
        .unwrap();
    let lines = builder.finish();
    let field = typ.to_field("geometry", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Arc::new(field)])),
        vec![lines.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("lines", batch).unwrap();

    let batch = collect_one(
        &ctx,
        "SELECT ST_AddMeasure(geometry, 1, 4) AS measured FROM lines",
    )
    .await;
    let output_type = batch
        .schema()
        .field(0)
        .try_extension_type::<MultiLineStringType>()
        .unwrap();
    assert_eq!(output_type.coord_type(), CoordType::Separated);
    assert_eq!(output_type.dimension(), Dimension::XYM);
    assert_eq!(output_type.metadata().crs(), &crs);
    let output =
        MultiLineStringArray::try_from((batch.column(0).as_ref(), batch.schema().field(0)))
            .unwrap();
    assert_eq!(
        output
            .value(0)
            .unwrap()
            .line_string(1)
            .unwrap()
            .coord(1)
            .unwrap()
            .nth(2),
        Some(4.0)
    );
}

#[tokio::test]
async fn native_locate_along_preserves_multipoint_schema_and_crs() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let typ = LineStringType::new(Dimension::XYM, metadata).with_coord_type(CoordType::Interleaved);
    let mut builder = LineStringBuilder::new(typ.clone());
    builder
        .push_geometry(Some(&WktLineString::new(
            vec![
                WktCoord {
                    x: 0.0,
                    y: 0.0,
                    z: None,
                    m: Some(0.0),
                },
                WktCoord {
                    x: 10.0,
                    y: 0.0,
                    z: None,
                    m: Some(10.0),
                },
            ],
            WktDimension::XYM,
        )))
        .unwrap();
    let lines = builder.finish();
    let field = typ.to_field("geometry", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Arc::new(field)])),
        vec![lines.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("lines", batch).unwrap();

    let batch = collect_one(
        &ctx,
        "SELECT ST_LocateAlong(geometry, 5) AS located FROM lines",
    )
    .await;
    let output_type = batch
        .schema()
        .field(0)
        .try_extension_type::<geoarrow_schema::MultiPointType>()
        .unwrap();
    assert_eq!(output_type.coord_type(), CoordType::Interleaved);
    assert_eq!(output_type.dimension(), Dimension::XYM);
    assert_eq!(output_type.metadata().crs(), &crs);
    let output = geoarrow_array::array::MultiPointArray::try_from((
        batch.column(0).as_ref(),
        batch.schema().field(0),
    ))
    .unwrap();
    let point = output.value(0).unwrap().point(0).unwrap();
    let coord = point.coord().unwrap();
    assert_eq!([coord.x(), coord.y()], [5.0, 0.0]);
    assert_eq!(coord.nth(2), Some(5.0));
}

#[tokio::test]
async fn native_polygon_and_multipolygon_clipping_preserve_output_schema() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:3857".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));

    let polygon_type =
        PolygonType::new(Dimension::XYM, metadata.clone()).with_coord_type(CoordType::Separated);
    let polygon = WktPolygon::new(
        vec![WktLineString::new(
            vec![
                WktCoord {
                    x: 0.0,
                    y: 0.0,
                    z: None,
                    m: Some(0.0),
                },
                WktCoord {
                    x: 4.0,
                    y: 0.0,
                    z: None,
                    m: Some(4.0),
                },
                WktCoord {
                    x: 4.0,
                    y: 4.0,
                    z: None,
                    m: Some(8.0),
                },
                WktCoord {
                    x: 0.0,
                    y: 4.0,
                    z: None,
                    m: Some(4.0),
                },
                WktCoord {
                    x: 0.0,
                    y: 0.0,
                    z: None,
                    m: Some(0.0),
                },
            ],
            WktDimension::XYM,
        )],
        WktDimension::XYM,
    );
    let mut polygon_builder = PolygonBuilder::new(polygon_type.clone());
    polygon_builder.push_geometry(Some(&polygon)).unwrap();
    let field = polygon_type.to_field("geometry", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Arc::new(field)])),
        vec![polygon_builder.finish().to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("polygons", batch).unwrap();

    let batch = collect_one(
        &ctx,
        "SELECT ST_LocateBetween(geometry, 2, 6) AS clipped FROM polygons",
    )
    .await;
    let output_type = batch
        .schema()
        .field(0)
        .try_extension_type::<MultiPolygonType>()
        .unwrap();
    assert_eq!(output_type.coord_type(), CoordType::Separated);
    assert_eq!(output_type.dimension(), Dimension::XYM);
    assert_eq!(output_type.metadata().crs(), &crs);
    let output =
        MultiPolygonArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
    let polygon = output.value(0).unwrap().polygon(0).unwrap();
    assert_eq!(polygon.exterior().unwrap().num_coords(), 7);

    let z_polygon = WktPolygon::new(
        vec![WktLineString::new(
            vec![
                WktCoord {
                    x: 0.0,
                    y: 0.0,
                    z: Some(0.0),
                    m: None,
                },
                WktCoord {
                    x: 4.0,
                    y: 0.0,
                    z: Some(4.0),
                    m: None,
                },
                WktCoord {
                    x: 4.0,
                    y: 4.0,
                    z: Some(8.0),
                    m: None,
                },
                WktCoord {
                    x: 0.0,
                    y: 4.0,
                    z: Some(4.0),
                    m: None,
                },
                WktCoord {
                    x: 0.0,
                    y: 0.0,
                    z: Some(0.0),
                    m: None,
                },
            ],
            WktDimension::XYZ,
        )],
        WktDimension::XYZ,
    );
    let multi_type =
        MultiPolygonType::new(Dimension::XYZ, metadata).with_coord_type(CoordType::Interleaved);
    let shifted_polygon = WktPolygon::new(
        vec![WktLineString::new(
            z_polygon
                .rings()
                .first()
                .unwrap()
                .coords()
                .iter()
                .map(|coord| WktCoord {
                    x: coord.x + 10.0,
                    y: coord.y,
                    z: coord.z,
                    m: coord.m,
                })
                .collect(),
            WktDimension::XYZ,
        )],
        WktDimension::XYZ,
    );
    let multi_polygon = WktMultiPolygon::new(vec![z_polygon, shifted_polygon], WktDimension::XYZ);
    let mut multi_builder = MultiPolygonBuilder::new(multi_type.clone());
    multi_builder.push_geometry(Some(&multi_polygon)).unwrap();
    let field = multi_type.to_field("geometry", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Arc::new(field)])),
        vec![multi_builder.finish().to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("multi_polygons", batch).unwrap();

    let batch = collect_one(
        &ctx,
        "SELECT ST_LocateBetweenElevations(geometry, 2, 6) AS clipped FROM multi_polygons",
    )
    .await;
    let output_type = batch
        .schema()
        .field(0)
        .try_extension_type::<MultiPolygonType>()
        .unwrap();
    assert_eq!(output_type.coord_type(), CoordType::Interleaved);
    assert_eq!(output_type.dimension(), Dimension::XYZ);
    let output =
        MultiPolygonArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
    let output = output.value(0).unwrap();
    assert_eq!(output.num_polygons(), 2);
    assert_eq!(
        output.polygon(0).unwrap().exterior().unwrap().num_coords(),
        7
    );
}

#[tokio::test]
async fn native_srid_mismatch_is_rejected_before_geometry_math() {
    let ctx = context();
    let line_metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4979".to_string()),
        None,
    ));
    let point_metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));

    let line_type =
        LineStringType::new(Dimension::XY, line_metadata).with_coord_type(CoordType::Separated);
    let mut line_builder = LineStringBuilder::new(line_type.clone());
    line_builder
        .push_geometry(Some(&WktLineString::new(
            vec![
                WktCoord {
                    x: 0.0,
                    y: 0.0,
                    z: None,
                    m: None,
                },
                WktCoord {
                    x: 10.0,
                    y: 0.0,
                    z: None,
                    m: None,
                },
            ],
            WktDimension::XY,
        )))
        .unwrap();
    let line_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Arc::new(
            line_type.to_field("geometry", true),
        )])),
        vec![line_builder.finish().to_array_ref()],
    )
    .unwrap();

    let point_type = geoarrow_schema::PointType::new(Dimension::XY, point_metadata)
        .with_coord_type(CoordType::Separated);
    let mut point_builder = geoarrow_array::builder::PointBuilder::new(point_type.clone());
    point_builder.push_coord(Some(&WktCoord {
        x: 5.0,
        y: 1.0,
        z: None,
        m: None,
    }));
    let point_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Arc::new(
            point_type.to_field("geometry", true),
        )])),
        vec![point_builder.finish().to_array_ref()],
    )
    .unwrap();

    ctx.register_batch("lines", line_batch).unwrap();
    ctx.register_batch("points", point_batch).unwrap();
    let error = execution_error(
        &ctx,
        "SELECT ST_LineLocatePoint(l.geometry, p.geometry) FROM lines l CROSS JOIN points p",
    )
    .await;
    assert!(error.contains("mixed SRID"), "{error}");
}

#[tokio::test]
async fn non_planar_edge_metadata_is_rejected() {
    let expressions = [
        "ST_LineInterpolatePoint(geometry, 0.5)",
        "ST_3DLineInterpolatePoint(geometry, 0.5)",
        "ST_LineInterpolatePoints(geometry, 0.25)",
        "ST_LineSubstring(geometry, 0.25, 0.75)",
        "ST_LocateAlong(geometry, 0.5)",
        "ST_LocateBetween(geometry, 0.25, 0.75)",
        "ST_LocateBetweenElevations(geometry, 0.25, 0.75)",
        "ST_AddMeasure(geometry, 0.0, 1.0)",
        "ST_LineLocatePoint(geometry, geometry)",
        "ST_InterpolatePoint(geometry, geometry)",
    ];
    let edge_types = [
        Edges::Andoyer,
        Edges::Karney,
        Edges::Spherical,
        Edges::Thomas,
        Edges::Vincenty,
    ];

    for (index, edges) in edge_types.into_iter().enumerate() {
        let ctx = context();
        let metadata = Arc::new(Metadata::new(Crs::default(), Some(edges)));
        let typ =
            LineStringType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
        let mut builder = LineStringBuilder::new(typ.clone());
        builder
            .push_geometry(Some(&WktLineString::new(
                vec![
                    WktCoord {
                        x: 0.0,
                        y: 0.0,
                        z: None,
                        m: None,
                    },
                    WktCoord {
                        x: 10.0,
                        y: 0.0,
                        z: None,
                        m: None,
                    },
                ],
                WktDimension::XY,
            )))
            .unwrap();
        let field = typ.to_field("geometry", true);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Arc::new(field)])),
            vec![builder.finish().to_array_ref()],
        )
        .unwrap();
        ctx.register_batch("lines", batch).unwrap();

        for expression in expressions {
            let error = execution_error(&ctx, &format!("SELECT {expression} FROM lines")).await;
            assert!(
                error.contains("planar edge metadata"),
                "{edges:?} case {index} failed for {expression}: {error}"
            );
        }
    }
}

#[tokio::test]
async fn interpolation_and_substring_properties() {
    let ctx = context();
    for fraction in [0.1, 0.25, 0.5, 0.75, 0.9] {
        let locate = scalar_f64(
            &ctx,
            &format!(
                "ST_LineLocatePoint(\
                 ST_GeomFromText('LINESTRING(0 0,4 0,4 3,10 3)'), \
                 ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING(0 0,4 0,4 3,10 3)'), {fraction}))"
            ),
        )
        .await
        .unwrap();
        assert!(
            (locate - fraction).abs() <= 1e-12,
            "round trip failed for {fraction}: {locate}"
        );

        let partition_error = scalar_f64(
            &ctx,
            &format!(
                "abs(\
                 ST_Length(ST_LineSubstring(ST_GeomFromText('LINESTRING(0 0,4 0,4 3,10 3)'), 0, {fraction})) + \
                 ST_Length(ST_LineSubstring(ST_GeomFromText('LINESTRING(0 0,4 0,4 3,10 3)'), {fraction}, 1)) - \
                 ST_Length(ST_GeomFromText('LINESTRING(0 0,4 0,4 3,10 3)'))\
                 )"
            ),
        )
        .await
        .unwrap();
        assert!(
            partition_error <= 1e-12,
            "substring partition drifted for {fraction}: {partition_error}"
        );
    }

    let batch = collect_one(
        &ctx,
        r#"
        SELECT
            ST_X(ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), 0)),
            ST_Y(ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), 0)),
            ST_X(ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), 1)),
            ST_Y(ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), 1))
        "#,
    )
    .await;
    for (index, expected) in [0.0, 0.0, 10.0, 0.0].into_iter().enumerate() {
        assert_eq!(
            batch.column(index).as_primitive::<Float64Type>().value(0),
            expected
        );
    }
}

#[tokio::test]
async fn single_member_geometry_collection_result_keeps_identity() {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        r#"
        SELECT GeometryType(
            ST_LocateBetween(
                ST_GeomFromText('LINESTRING M (0 0 0,10 0 10,20 0 0)'),
                10,
                15
            )
        )
        "#,
    )
    .await;
    assert_eq!(
        batch.column(0).as_string_view().value(0),
        "GEOMETRYCOLLECTIONM"
    );
}

#[tokio::test]
async fn native_sliced_linestring_input_keeps_rows_aligned() {
    let ctx = context();
    let metadata = Arc::new(Metadata::default());
    let typ = LineStringType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
    let mut builder = LineStringBuilder::new(typ.clone());
    for x in [0.0, 100.0, 200.0] {
        builder
            .push_geometry(Some(&WktLineString::new(
                vec![
                    WktCoord {
                        x,
                        y: 0.0,
                        z: None,
                        m: None,
                    },
                    WktCoord {
                        x: x + 10.0,
                        y: 0.0,
                        z: None,
                        m: None,
                    },
                ],
                WktDimension::XY,
            )))
            .unwrap();
    }
    let lines = builder.finish().slice(1, 2);
    let field = typ.to_field("geometry", true);
    let fractions = Float64Array::from(vec![0.0, 1.0]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Arc::new(field),
            Arc::new(Field::new("fraction", DataType::Float64, false)),
        ])),
        vec![lines.to_array_ref(), Arc::new(fractions)],
    )
    .unwrap();
    ctx.register_batch("lines", batch).unwrap();
    let batch = collect_one(
        &ctx,
        "SELECT ST_X(ST_LineInterpolatePoint(geometry, fraction)) FROM lines",
    )
    .await;
    let values = batch.column(0).as_primitive::<Float64Type>();
    assert_eq!(values.value(0), 100.0);
    assert_eq!(values.value(1), 210.0);
}
