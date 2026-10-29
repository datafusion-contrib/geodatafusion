//! Public-register SQL regressions for the first accessor function batch.
//!
//! Semantic references are the PostGIS manual (`reference_accessor.xml`) and
//! `regress/core/empty.sql`, `regress/core/iscollection.sql`, and
//! `regress/core/regress_ogc.sql` at postgis/postgis commit 80fc3d3.
//!

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int16Type, UInt8Type, UInt32Type};
use arrow_array::{Array, RecordBatch};
use datafusion::execution::registry::FunctionRegistry;
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::{
    GeometryCollectionBuilder, LineStringBuilder, MultiPointBuilder, PointBuilder,
};
use geoarrow_array::test::polygon;
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeoArrowType, GeometryCollectionType, LineStringType, Metadata,
    MultiPointType, PointType,
};
use wkt::Wkt as WktValue;
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, GeometryCollection as WktGeometryCollection,
    LineString as WktLineString, MultiPoint as WktMultiPoint, Point as WktPoint,
};

fn new_ctx() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

#[test]
fn registry_has_new_accessors() {
    let ctx = new_ctx();
    let names = ctx.state().udfs();
    for name in [
        "st_dimension",
        "st_iscollection",
        "st_numgeometries",
        "st_geometryn",
        "st_pointn",
        "st_exteriorring",
        "st_interiorringn",
        "st_nrings",
        "st_zmflag",
        "st_hasz",
        "st_hasm",
    ] {
        assert!(
            names.contains(name),
            "missing {name}; registered: {names:#?}"
        );
    }
}

async fn query_batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("plan failed for {sql:?}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("execution failed for {sql:?}: {e}"))
        .into_iter()
        .next()
        .expect("SELECT should return one batch")
}

async fn assert_u8(ctx: &SessionContext, sql: &str, expected: u8) {
    let batch = query_batch(ctx, sql).await;
    assert_eq!(
        batch.column(0).as_primitive::<UInt8Type>().value(0),
        expected,
        "failed for {sql}"
    );
}

async fn assert_u32(ctx: &SessionContext, sql: &str, expected: u32) {
    let batch = query_batch(ctx, sql).await;
    assert_eq!(
        batch.column(0).as_primitive::<UInt32Type>().value(0),
        expected,
        "failed for {sql}"
    );
}

async fn assert_i16(ctx: &SessionContext, sql: &str, expected: i16) {
    let batch = query_batch(ctx, sql).await;
    assert_eq!(
        batch.column(0).as_primitive::<Int16Type>().value(0),
        expected,
        "failed for {sql}"
    );
}

async fn assert_bool(ctx: &SessionContext, sql: &str, expected: bool) {
    let batch = query_batch(ctx, sql).await;
    assert_eq!(
        batch.column(0).as_boolean().value(0),
        expected,
        "failed for {sql}"
    );
}

async fn assert_wkt(ctx: &SessionContext, sql: &str, expected: &str) {
    let batch = query_batch(ctx, sql).await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        expected,
        "failed for {sql}"
    );
}

async fn assert_wkt_null(ctx: &SessionContext, sql: &str) {
    let batch = query_batch(ctx, sql).await;
    let col = batch.column(0).as_string::<i32>();
    assert!(col.is_null(0), "expected NULL for {sql}");
}

async fn assert_binary_hex(ctx: &SessionContext, sql: &str, expected: &str) {
    let batch = query_batch(ctx, sql).await;
    let value = batch.column(0).as_binary::<i32>().value(0);
    assert_eq!(hex::encode_upper(value), expected, "failed for {sql}");
}

#[tokio::test]
async fn dimension_and_zm_flags() {
    let ctx = new_ctx();

    assert_u8(
        &ctx,
        "SELECT ST_Dimension(ST_GeomFromText('POINT(1 2)'))",
        0,
    )
    .await;
    assert_u8(
        &ctx,
        "SELECT ST_Dimension(ST_GeomFromText('LINESTRING(0 0, 1 1)'))",
        1,
    )
    .await;
    assert_u8(
        &ctx,
        "SELECT ST_Dimension(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))'))",
        2,
    )
    .await;
    assert_u8(
        &ctx,
        "SELECT ST_Dimension(ST_GeomFromText('GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1), POINT(1 1))'))",
        1,
    ).await;
    assert_u8(
        &ctx,
        "SELECT ST_Dimension(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 1), POLYGON((0 0, 1 0, 1 1, 0 0)))'))",
        2,
    ).await;
    assert_u8(
        &ctx,
        "SELECT ST_Dimension(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'))",
        0,
    )
    .await;
    assert_u8(
        &ctx,
        "SELECT ST_Dimension(ST_GeomFromText('POINT EMPTY'))",
        0,
    )
    .await;
    assert_u8(
        &ctx,
        "SELECT ST_Dimension(ST_GeomFromText('LINESTRING EMPTY'))",
        1,
    )
    .await;
    assert_u8(
        &ctx,
        "SELECT ST_Dimension(ST_GeomFromText('POLYGON EMPTY'))",
        2,
    )
    .await;
    assert_u8(
        &ctx,
        "SELECT ST_Dimension(ST_GeomFromText('MULTIPOLYGON EMPTY'))",
        2,
    )
    .await;

    assert_bool(
        &ctx,
        "SELECT ST_HasZ(ST_GeomFromText('POINT Z (1 2 3)'))",
        true,
    )
    .await;
    assert_bool(
        &ctx,
        "SELECT ST_HasZ(ST_GeomFromText('POINT M (1 2 3)'))",
        false,
    )
    .await;
    assert_bool(
        &ctx,
        "SELECT ST_HasZ(ST_GeomFromText('POINT ZM (1 2 3 4)'))",
        true,
    )
    .await;
    assert_bool(
        &ctx,
        "SELECT ST_HasM(ST_GeomFromText('POINT M (1 2 3)'))",
        true,
    )
    .await;
    assert_bool(
        &ctx,
        "SELECT ST_HasM(ST_GeomFromText('POINT Z (1 2 3)'))",
        false,
    )
    .await;
    assert_bool(
        &ctx,
        "SELECT ST_HasM(ST_GeomFromText('POINT ZM (1 2 3 4)'))",
        true,
    )
    .await;

    assert_i16(
        &ctx,
        "SELECT ST_Zmflag(ST_GeomFromText('LINESTRING(0 0, 1 1)'))",
        0,
    )
    .await;
    assert_i16(
        &ctx,
        "SELECT ST_Zmflag(ST_GeomFromText('LINESTRING M (0 0 5, 1 1 6)'))",
        1,
    )
    .await;
    assert_i16(
        &ctx,
        "SELECT ST_Zmflag(ST_GeomFromText('LINESTRING Z (0 0 5, 1 1 6)'))",
        2,
    )
    .await;
    assert_i16(
        &ctx,
        "SELECT ST_Zmflag(ST_GeomFromText('LINESTRING ZM (0 0 5 9, 1 1 6 8)'))",
        3,
    )
    .await;
}

#[tokio::test]
async fn nested_geometry_collections_are_rejected_instead_of_flattened() {
    let ctx = new_ctx();

    for nested in [
        "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2)))",
        "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY)",
        "GEOMETRYCOLLECTION(POINT(1 2),GEOMETRYCOLLECTION EMPTY)",
    ] {
        let error = ctx
            .sql(&format!("SELECT ST_AsText(ST_GeomFromText('{nested}'))"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("nested GeometryCollection"),
            "expected an explicit nested-collection rejection for {nested}, got: {error}"
        );
    }

    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))'))",
        "GEOMETRYCOLLECTION(POINT(1 2))",
    )
    .await;
}

#[tokio::test]
async fn multipoint_empty_members_round_trip_through_public_sql() {
    let ctx = new_ctx();

    let cases = [
        (
            "MULTIPOINT(EMPTY,(1 2))",
            "MULTIPOINT(EMPTY,1 2)",
            0,
            2,
            "0104000000020000000101000000000000000000F87F000000000000F87F0101000000000000000000F03F0000000000000040",
        ),
        (
            "MULTIPOINT Z (EMPTY,(1 2 3))",
            "MULTIPOINT(EMPTY,1 2 3)",
            2,
            2,
            "01EC0300000200000001E9030000000000000000F87F000000000000F87F000000000000F87F01E9030000000000000000F03F00000000000000400000000000000840",
        ),
        (
            "MULTIPOINT M (EMPTY,(1 2 3))",
            "MULTIPOINTM(EMPTY,1 2 3)",
            1,
            2,
            "01D40700000200000001D1070000000000000000F87F000000000000F87F000000000000F87F01D1070000000000000000F03F00000000000000400000000000000840",
        ),
        (
            "MULTIPOINT ZM (EMPTY,(1 2 3 4))",
            "MULTIPOINT(EMPTY,1 2 3 4)",
            3,
            2,
            "01BC0B00000200000001B90B0000000000000000F87F000000000000F87F000000000000F87F000000000000F87F01B90B0000000000000000F03F000000000000004000000000000008400000000000001040",
        ),
        (
            "MULTIPOINT(EMPTY,EMPTY,(1 2))",
            "MULTIPOINT(EMPTY,EMPTY,1 2)",
            0,
            3,
            "0104000000030000000101000000000000000000F87F000000000000F87F0101000000000000000000F87F000000000000F87F0101000000000000000000F03F0000000000000040",
        ),
        (
            "MULTIPOINT((1 2),EMPTY,(3 4))",
            "MULTIPOINT(1 2,EMPTY,3 4)",
            0,
            3,
            "0104000000030000000101000000000000000000F03F00000000000000400101000000000000000000F87F000000000000F87F010100000000000000000008400000000000001040",
        ),
        (
            "GEOMETRYCOLLECTION Z (MULTIPOINT Z (EMPTY,(1 2 5)))",
            "GEOMETRYCOLLECTION(MULTIPOINT(EMPTY,1 2 5))",
            2,
            1,
            "01EF0300000100000001EC0300000200000001E9030000000000000000F87F000000000000F87F000000000000F87F01E9030000000000000000F03F00000000000000400000000000001440",
        ),
    ];

    for (wkt, expected_ewkt, zmflag, num_geometries, expected_wkb) in cases {
        let geometry = format!("ST_GeomFromText('{wkt}')");
        assert_wkt(
            &ctx,
            &format!("SELECT ST_AsEWKT({geometry})"),
            expected_ewkt,
        )
        .await;
        assert_i16(&ctx, &format!("SELECT ST_Zmflag({geometry})"), zmflag).await;
        assert_u32(
            &ctx,
            &format!("SELECT ST_NumGeometries({geometry})"),
            num_geometries,
        )
        .await;
        assert_binary_hex(
            &ctx,
            &format!("SELECT ST_AsBinary({geometry})"),
            expected_wkb,
        )
        .await;
        assert_wkt(
            &ctx,
            &format!("SELECT ST_AsEWKT(ST_GeomFromWKB(ST_AsBinary({geometry})))"),
            expected_ewkt,
        )
        .await;
        assert_wkt(
            &ctx,
            &format!("SELECT ST_AsEWKT(ST_GeomFromEWKB(ST_AsEWKB({geometry})))"),
            expected_ewkt,
        )
        .await;
    }

    assert_wkt(
        &ctx,
        "SELECT ST_AsEWKT(ST_GeomFromEWKT('SRID=4326;MULTIPOINT Z (EMPTY,(1 2 3))'))",
        "SRID=4326;MULTIPOINT(EMPTY,1 2 3)",
    )
    .await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsEWKT(ST_MPointFromText('MULTIPOINT M (EMPTY,(1 2 3))'))",
        "MULTIPOINTM(EMPTY,1 2 3)",
    )
    .await;
}
#[tokio::test]
async fn collection_counts_and_geometry_n() {
    let ctx = new_ctx();

    assert_u32(
        &ctx,
        "SELECT ST_NumGeometries(ST_GeomFromText('LINESTRING(0 0, 1 1)'))",
        1,
    )
    .await;
    assert_u32(
        &ctx,
        "SELECT ST_NumGeometries(ST_GeomFromText('MULTIPOINT(0 0, 1 1)'))",
        2,
    )
    .await;
    assert_u32(
        &ctx,
        "SELECT ST_NumGeometries(ST_GeomFromText('POINT EMPTY'))",
        0,
    )
    .await;
    assert_u32(
        &ctx,
        "SELECT ST_NumGeometries(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'))",
        0,
    )
    .await;
    assert_u32(
        &ctx,
        "SELECT ST_NumGeometries(ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON EMPTY)'))",
        0,
    )
    .await;
    assert_u32(
        &ctx,
        "SELECT ST_NumGeometries(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0), MULTIPOINT(0 0, 1 1), LINESTRING(0 0, 1 1))'))",
        3,
    ).await;

    assert_bool(
        &ctx,
        "SELECT ST_IsCollection(ST_GeomFromText('LINESTRING(0 0, 1 1)'))",
        false,
    )
    .await;
    assert_bool(
        &ctx,
        "SELECT ST_IsCollection(ST_GeomFromText('MULTIPOINT EMPTY'))",
        true,
    )
    .await;
    assert_bool(
        &ctx,
        "SELECT ST_IsCollection(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'))",
        true,
    )
    .await;
    assert_bool(
        &ctx,
        "SELECT ST_IsCollection(ST_GeomFromText('POINT EMPTY'))",
        false,
    )
    .await;
    assert_bool(
        &ctx,
        "SELECT ST_IsCollection(ST_GeomFromText('MULTIPOINT((0 0))'))",
        true,
    )
    .await;
    assert_bool(
        &ctx,
        "SELECT ST_IsCollection(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0), POINT(4 4))'))",
        true,
    )
    .await;

    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('MULTIPOINT(1 2, 3 4)'), 1))",
        "POINT(1 2)",
    )
    .await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('MULTIPOINT(1 2, 3 4)'), 2))",
        "POINT(3 4)",
    )
    .await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('MULTIPOINT Z (1 2 7, 3 4 8)'), 2))",
        "POINT Z(3 4 8)",
    )
    .await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('LINESTRING(0 0, 1 1)'), 1))",
        "LINESTRING(0 0,1 1)",
    )
    .await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1))'), 2))",
        "LINESTRING(0 0,1 1)",
    ).await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('MULTILINESTRING((0 0, 1 1), (2 2, 3 3))'), 2))",
        "LINESTRING(2 2,3 3)",
    ).await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((5 5, 6 5, 6 6, 5 5)))'), 2))",
        "POLYGON((5 5,6 5,6 6,5 5))",
    ).await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('POINT(1 1)'), 1))",
        "POINT(1 1)",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('MULTIPOINT(1 2, 3 4)'), 3))",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('MULTIPOINT(1 2, 3 4)'), 0))",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('MULTIPOINT(1 2, 3 4)'), -1))",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('LINESTRING(0 0, 1 1)'), 2))",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('POINT EMPTY'), 1))",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON EMPTY)'), 1))",
    )
    .await;

    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('MULTIPOINT(1 2, 3 4)'))), 2))",
        "POINT(3 4)",
    ).await;
}

#[tokio::test]
async fn point_n_through_public_register() {
    let ctx = new_ctx();

    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_PointN(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'), 1))",
        "POINT(0 0)",
    )
    .await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_PointN(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'), -1))",
        "POINT(2 2)",
    )
    .await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_PointN(ST_GeomFromText('LINESTRING Z (0 0 5, 1 1 6, 2 2 7)'), -2))",
        "POINT Z(1 1 6)",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_PointN(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'), 0))",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_PointN(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'), 4))",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_PointN(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))'), 1))",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_PointN(ST_GeomFromText('MULTILINESTRING((0 0, 1 1))'), 1))",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_PointN(ST_GeomFromText('LINESTRING EMPTY'), 1))",
    )
    .await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_PointN(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'))), -1))",
        "POINT(2 2)",
    ).await;
}

#[tokio::test]
async fn ring_accessors_and_nrings() {
    let ctx = new_ctx();
    let polygon_with_hole = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), \
        (2 2, 2 4, 4 4, 4 2, 2 2))";

    assert_wkt(
        &ctx,
        &format!("SELECT ST_AsText(ST_ExteriorRing(ST_GeomFromText('{polygon_with_hole}')))"),
        "LINESTRING(0 0,10 0,10 10,0 10,0 0)",
    )
    .await;
    assert_wkt(
        &ctx,
        &format!("SELECT ST_AsText(ST_InteriorRingN(ST_GeomFromText('{polygon_with_hole}'), 1))"),
        "LINESTRING(2 2,2 4,4 4,4 2,2 2)",
    )
    .await;
    assert_wkt_null(
        &ctx,
        &format!("SELECT ST_AsText(ST_InteriorRingN(ST_GeomFromText('{polygon_with_hole}'), 2))"),
    )
    .await;
    assert_wkt_null(
        &ctx,
        &format!("SELECT ST_AsText(ST_InteriorRingN(ST_GeomFromText('{polygon_with_hole}'), 0))"),
    )
    .await;
    assert_wkt_null(
        &ctx,
        &format!("SELECT ST_AsText(ST_InteriorRingN(ST_GeomFromText('{polygon_with_hole}'), -1))"),
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_InteriorRingN(ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))'), 1))",
    ).await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_InteriorRingN(ST_GeomFromText('POLYGON EMPTY'), 1))",
    )
    .await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_ExteriorRing(ST_GeomFromText('POLYGON EMPTY')))",
        "LINESTRING EMPTY",
    )
    .await;
    assert_wkt_null(
        &ctx,
        "SELECT ST_AsText(ST_ExteriorRing(ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))')))",
    ).await;
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_ExteriorRing(ST_GeomFromText('POLYGON Z ((0 0 5, 4 0 5, 4 3 5, 0 3 5, 0 0 5))')))",
        "LINESTRING Z(0 0 5,4 0 5,4 3 5,0 3 5,0 0 5)",
    ).await;

    assert_u32(
        &ctx,
        "SELECT ST_NRings(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))'))",
        1,
    )
    .await;
    assert_u32(
        &ctx,
        &format!("SELECT ST_NRings(ST_GeomFromText('{polygon_with_hole}'))"),
        2,
    )
    .await;
    assert_u32(
        &ctx,
        "SELECT ST_NRings(ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((5 5, 6 5, 6 6, 5 5),(6 6, 6 7, 7 7, 6 6)), ((10 10, 11 10, 11 11, 10 10)))'))",
        4,
    ).await;
    assert_u32(
        &ctx,
        "SELECT ST_NRings(ST_GeomFromText('LINESTRING(0 0, 1 1)'))",
        0,
    )
    .await;
    assert_u32(
        &ctx,
        "SELECT ST_NRings(ST_GeomFromText('POLYGON EMPTY'))",
        0,
    )
    .await;
    assert_u32(
        &ctx,
        "SELECT ST_NRings(ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON((0 0, 1 0, 1 1, 0 0)), LINESTRING(0 0, 1 1))'))",
        1,
    ).await;

    let empty_member_multi_polygon = "MULTIPOLYGON(EMPTY,((0 0,1 0,1 1,0 1,0 0)))";
    assert_u32(
        &ctx,
        &format!("SELECT ST_NRings(ST_GeomFromText('{empty_member_multi_polygon}'))"),
        1,
    )
    .await;
    assert_u32(
        &ctx,
        &format!("SELECT ST_NumGeometries(ST_GeomFromText('{empty_member_multi_polygon}'))"),
        2,
    )
    .await;
    assert_u32(
        &ctx,
        &format!(
            "SELECT ST_NRings(ST_GeometryN(ST_GeomFromText('{empty_member_multi_polygon}'), 1))"
        ),
        0,
    )
    .await;
    assert_u32(
        &ctx,
        &format!(
            "SELECT ST_NRings(ST_GeometryN(ST_GeomFromText('{empty_member_multi_polygon}'), 2))"
        ),
        1,
    )
    .await;
    assert_binary_hex(
        &ctx,
        &format!(
            "SELECT ST_AsBinary(ST_GeometryN(ST_GeomFromText('{empty_member_multi_polygon}'), 1))"
        ),
        "010300000000000000",
    )
    .await;

    for constructor in [
        "ST_GeomFromWKB",
        "ST_MPolyFromWKB",
        "ST_MultiPolygonFromWKB",
    ] {
        let wkb =
            format!("{constructor}(ST_AsBinary(ST_GeomFromText('{empty_member_multi_polygon}')))");
        assert_u32(&ctx, &format!("SELECT ST_NRings({wkb})"), 1).await;
        assert_u32(&ctx, &format!("SELECT ST_NumGeometries({wkb})"), 2).await;
        assert_u32(
            &ctx,
            &format!("SELECT ST_NRings(ST_GeometryN({wkb}, 1))"),
            0,
        )
        .await;
        assert_binary_hex(
            &ctx,
            &format!("SELECT ST_AsBinary(ST_GeometryN({wkb}, 1))"),
            "010300000000000000",
        )
        .await;
    }
}

#[tokio::test]
async fn native_geoarrow_arrays_preserve_coordinates_and_metadata() {
    let ctx = new_ctx();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs, None));

    let mp_geometry = WktMultiPoint::new(
        vec![
            WktPoint::from_coord(WktCoord {
                x: 1.,
                y: 2.,
                z: Some(7.),
                m: Some(8.),
            }),
            WktPoint::from_coord(WktCoord {
                x: 3.,
                y: 4.,
                z: Some(8.),
                m: Some(9.),
            }),
        ],
        WktDimension::XYZM,
    );
    let mp_type = MultiPointType::new(Dimension::XYZM, metadata.clone())
        .with_coord_type(CoordType::Separated);
    let mp = MultiPointBuilder::from_multi_points(&[mp_geometry], mp_type).finish();
    let mp_field = mp.data_type().to_field("geometry", true);
    let mp_batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new([Arc::new(mp_field)])),
        vec![mp.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("mp", mp_batch).unwrap();

    let df = ctx
        .sql("SELECT ST_GeometryN(geometry, 2) AS out FROM mp LIMIT 1")
        .await
        .unwrap();
    let batch = df.schema().inner().clone();
    let field = batch.field(0).clone();
    let output_type = GeoArrowType::from_extension_field(&field).unwrap().unwrap();
    assert!(matches!(output_type, GeoArrowType::Geometry(_)));
    assert_eq!(output_type.metadata().as_ref(), metadata.as_ref());
    assert_eq!(output_type.coord_type(), Some(CoordType::Separated));
    let rows = df.collect().await.unwrap();
    let out = rows[0].column(0);
    assert_eq!(out.len(), 1);
    assert!(!out.is_null(0));
    let df = ctx
        .sql("SELECT ST_AsText(ST_GeometryN(geometry, 2)) FROM mp LIMIT 1")
        .await
        .unwrap();
    let batch = df.collect().await.unwrap().into_iter().next().unwrap();
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "POINT ZM(3 4 8 9)"
    );

    let ls_geometry = WktLineString::new(
        vec![
            WktCoord {
                x: 0.,
                y: 0.,
                z: Some(5.),
                m: None,
            },
            WktCoord {
                x: 1.,
                y: 1.,
                z: Some(6.),
                m: None,
            },
        ],
        WktDimension::XYZ,
    );
    let ls_type = LineStringType::new(Dimension::XYZ, metadata.clone())
        .with_coord_type(CoordType::Interleaved);
    let ls = LineStringBuilder::from_line_strings(&[ls_geometry], ls_type).finish();
    let ls_field = ls.data_type().to_field("geometry", true);
    let ls_batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new([Arc::new(ls_field)])),
        vec![ls.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("ls", ls_batch).unwrap();
    let df = ctx
        .sql("SELECT ST_AsText(ST_PointN(geometry, -1)) FROM ls LIMIT 1")
        .await
        .unwrap();
    let batch = df.collect().await.unwrap().into_iter().next().unwrap();
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "POINT Z(1 1 6)"
    );

    let polygon = polygon::array(CoordType::Separated, Dimension::XY).with_metadata(metadata);
    let polygon_field = polygon.data_type().to_field("geometry", true);
    let polygon_batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new([Arc::new(polygon_field)])),
        vec![polygon.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("poly", polygon_batch).unwrap();
    let df = ctx
        .sql("SELECT ST_ExteriorRing(geometry) FROM poly LIMIT 1")
        .await
        .unwrap();
    let schema = df.schema().inner().clone();
    let output_type = GeoArrowType::from_extension_field(schema.field(0))
        .unwrap()
        .unwrap();
    assert!(matches!(output_type, GeoArrowType::Geometry(_)));
    assert_eq!(output_type.coord_type(), Some(CoordType::Separated));
    let batch = df.collect().await.unwrap().into_iter().next().unwrap();
    assert!(!batch.column(0).is_null(0));
}

#[tokio::test]
async fn coordinate_getters_declare_nullable_results() {
    let ctx = new_ctx();

    let df = ctx
        .sql(
            "SELECT \
             ST_M(ST_GeomFromText('POINT(1 2)')), \
             ST_Z(ST_GeomFromText('POINT(1 2)'))",
        )
        .await
        .unwrap();
    let schema = df.schema().inner().clone();
    assert!(schema.field(0).is_nullable());
    assert!(schema.field(1).is_nullable());
    let batch = df.collect().await.unwrap().into_iter().next().unwrap();
    assert!(batch.column(0).is_null(0));
    assert!(batch.column(1).is_null(0));

    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let point_type = PointType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
    let mut points = PointBuilder::with_capacity(point_type.clone(), 2);
    points.push_empty();
    points.push_coord(Some(&WktCoord {
        x: 1.,
        y: 2.,
        z: None,
        m: None,
    }));
    let points = points.finish();
    let point_field = point_type.to_field("geometry", false);
    let point_batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new([Arc::new(point_field)])),
        vec![points.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("points", point_batch).unwrap();

    let df = ctx
        .sql(
            "SELECT ST_X(geometry), ST_Y(geometry), ST_Z(geometry), ST_M(geometry) \
             FROM points",
        )
        .await
        .unwrap();
    let schema = df.schema().inner().clone();
    for field in schema.fields() {
        assert_eq!(field.data_type(), &arrow_schema::DataType::Float64);
        assert!(field.is_nullable());
    }
    let batch = df.collect().await.unwrap().into_iter().next().unwrap();
    for index in 0..4 {
        assert!(batch.column(index).is_null(0));
    }
    assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(1), 1.0);
    assert_eq!(batch.column(1).as_primitive::<Float64Type>().value(1), 2.0);
    assert!(batch.column(2).is_null(1));
    assert!(batch.column(3).is_null(1));
}

#[tokio::test]
async fn native_single_member_geometry_collection_keeps_collection_identity() {
    let ctx = new_ctx();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let point = WktPoint::from_coord(WktCoord {
        x: 1.,
        y: 2.,
        z: None,
        m: None,
    });
    let gc_geometry = WktGeometryCollection::new(vec![WktValue::Point(point)], WktDimension::XY);
    let gc_type =
        GeometryCollectionType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
    let gc = GeometryCollectionBuilder::from_geometry_collections(&[gc_geometry], gc_type)
        .unwrap()
        .finish();
    let gc_field = gc.data_type().to_field("geometry", true);
    let gc_batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new([Arc::new(gc_field)])),
        vec![gc.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("gc", gc_batch).unwrap();

    let batch = query_batch(
        &ctx,
        "SELECT ST_IsCollection(geometry), ST_NumGeometries(geometry), \
         ST_AsText(ST_GeometryN(geometry, 1)) FROM gc",
    )
    .await;
    assert!(batch.column(0).as_boolean().value(0));
    assert_eq!(batch.column(1).as_primitive::<UInt32Type>().value(0), 1);
    assert_eq!(batch.column(2).as_string::<i32>().value(0), "POINT(1 2)");
}

#[tokio::test]
async fn wkt_single_member_geometry_collection_matches_postgis() {
    let ctx = new_ctx();
    assert_bool(
        &ctx,
        "SELECT ST_IsCollection(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))'))",
        true,
    )
    .await;
    assert_u32(
        &ctx,
        "SELECT ST_NumGeometries(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))'))",
        1,
    )
    .await;

    let batch = query_batch(
        &ctx,
        "SELECT ST_GeometryType(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))'))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string_view().value(0),
        "ST_GeometryCollection"
    );
    assert_wkt(
        &ctx,
        "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))'), 1))",
        "POINT(1 2)",
    )
    .await;
}
