//! Public-register SQL regressions for the structural accessor functions.
//!
//! Semantic references are the PostGIS manual (`reference_accessor.xml`,
//! `reference_processing.xml`) and `regress/core/dumppoints.sql`,
//! `dumpsegments.sql`, `dump.sql`, and `summary.sql` at postgis/postgis
//! commit 80fc3d3.

use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema};
use datafusion::execution::registry::FunctionRegistry;
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::LineStringBuilder;
use geoarrow_array::cast::to_wkt;
use geoarrow_array::test::{linestring, multipoint};
use geoarrow_schema::{CoordType, Crs, Dimension, GeoArrowType, LineStringType, Metadata, WkbType};
use wkt::types::{Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString};

fn new_ctx() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

#[test]
fn registry_has_structural_accessors() {
    let ctx = new_ctx();
    let names = ctx.state().udfs();
    for name in [
        "st_dumppoints",
        "st_dumpsegments",
        "st_dumprings",
        "st_points",
        "st_summary",
        "st_memsize",
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

/// Decode one `list<struct<path, geom>>` row into `(path, wkt)` pairs, or
/// `None` when the list itself is NULL.
fn decode_dump_row(column: &ArrayRef, row: usize) -> Option<Vec<(Vec<i32>, String)>> {
    let list = column.as_list::<i32>();
    if list.is_null(row) {
        return None;
    }

    let components = list.value(row);
    let structs = components.as_struct();
    let path_col = structs.column(0).as_list::<i32>();
    let DataType::Struct(fields) = structs.data_type() else {
        panic!("dump output must be a list of structs");
    };
    let geo = from_arrow_array(structs.column(1).as_ref(), fields[1].as_ref()).unwrap();
    let wkt_ref = to_wkt::<i32>(geo.as_ref()).unwrap().into_array_ref();
    let wkts = wkt_ref.as_string::<i32>();

    Some(
        (0..structs.len())
            .map(|i| {
                let path = path_col
                    .value(i)
                    .as_primitive::<Int32Type>()
                    .values()
                    .to_vec();
                (path, wkts.value(i).to_string())
            })
            .collect(),
    )
}

/// Run `sql`, which must produce one list<struct<path, geom>> column, and
/// decode its first row into `(path, wkt)` pairs.
async fn dump_rows(ctx: &SessionContext, sql: &str) -> Vec<(Vec<i32>, String)> {
    let batch = query_batch(ctx, sql).await;
    decode_dump_row(batch.column(0), 0)
        .unwrap_or_else(|| panic!("unexpected NULL dump row for {sql}"))
}

async fn dump_rows_for(ctx: &SessionContext, function: &str, wkt: &str) -> Vec<(Vec<i32>, String)> {
    let sql = format!("SELECT {function}(ST_GeomFromText('{wkt}'))");
    dump_rows(ctx, &sql).await
}

async fn assert_scalar_text(ctx: &SessionContext, sql: &str, expected: &str) {
    let batch = query_batch(ctx, sql).await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        expected,
        "failed for {sql}"
    );
}

#[tokio::test]
async fn dump_points_paths_follow_postgis() {
    let ctx = new_ctx();

    assert_eq!(
        dump_rows_for(&ctx, "ST_DumpPoints", "POINT(1 2)").await,
        vec![(vec![1], "POINT(1 2)".to_string())],
    );
    assert_eq!(
        dump_rows_for(&ctx, "ST_DumpPoints", "LINESTRING(0 0,1 1,2 0)").await,
        vec![
            (vec![1], "POINT(0 0)".to_string()),
            (vec![2], "POINT(1 1)".to_string()),
            (vec![3], "POINT(2 0)".to_string()),
        ],
    );
    assert_eq!(
        dump_rows_for(
            &ctx,
            "ST_DumpPoints",
            "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))"
        )
        .await,
        vec![
            (vec![1, 1], "POINT(0 0)".to_string()),
            (vec![1, 2], "POINT(4 0)".to_string()),
            (vec![1, 3], "POINT(4 4)".to_string()),
            (vec![1, 4], "POINT(0 4)".to_string()),
            (vec![1, 5], "POINT(0 0)".to_string()),
            (vec![2, 1], "POINT(1 1)".to_string()),
            (vec![2, 2], "POINT(2 1)".to_string()),
            (vec![2, 3], "POINT(2 2)".to_string()),
            (vec![2, 4], "POINT(1 2)".to_string()),
            (vec![2, 5], "POINT(1 1)".to_string()),
        ],
    );
    assert_eq!(
        dump_rows_for(
            &ctx,
            "ST_DumpPoints",
            "MULTILINESTRING((0 0,1 1),(2 2,3 3))"
        )
        .await,
        vec![
            (vec![1, 1], "POINT(0 0)".to_string()),
            (vec![1, 2], "POINT(1 1)".to_string()),
            (vec![2, 1], "POINT(2 2)".to_string()),
            (vec![2, 2], "POINT(3 3)".to_string()),
        ],
    );
    assert_eq!(
        dump_rows_for(
            &ctx,
            "ST_DumpPoints",
            "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))"
        )
        .await,
        vec![
            (vec![1, 1], "POINT(1 2)".to_string()),
            (vec![2, 1], "POINT(0 0)".to_string()),
            (vec![2, 2], "POINT(1 1)".to_string()),
        ],
    );

    // Topologically empty inputs produce zero rows per input row.
    assert!(
        dump_rows_for(&ctx, "ST_DumpPoints", "POINT EMPTY")
            .await
            .is_empty()
    );
    assert!(
        dump_rows_for(&ctx, "ST_DumpPoints", "GEOMETRYCOLLECTION EMPTY")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn dump_segments_paths_follow_postgis() {
    let ctx = new_ctx();

    assert_eq!(
        dump_rows_for(&ctx, "ST_DumpSegments", "LINESTRING(0 0,1 1,2 0)").await,
        vec![
            (vec![1], "LINESTRING(0 0,1 1)".to_string()),
            (vec![2], "LINESTRING(1 1,2 0)".to_string()),
        ],
    );
    assert_eq!(
        dump_rows_for(
            &ctx,
            "ST_DumpSegments",
            "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))"
        )
        .await,
        vec![
            (vec![1, 1], "LINESTRING(0 0,4 0)".to_string()),
            (vec![1, 2], "LINESTRING(4 0,4 4)".to_string()),
            (vec![1, 3], "LINESTRING(4 4,0 4)".to_string()),
            (vec![1, 4], "LINESTRING(0 4,0 0)".to_string()),
            (vec![2, 1], "LINESTRING(1 1,2 1)".to_string()),
            (vec![2, 2], "LINESTRING(2 1,2 2)".to_string()),
            (vec![2, 3], "LINESTRING(2 2,1 2)".to_string()),
            (vec![2, 4], "LINESTRING(1 2,1 1)".to_string()),
        ],
    );
    assert_eq!(
        dump_rows_for(
            &ctx,
            "ST_DumpSegments",
            "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))"
        )
        .await,
        vec![(vec![2, 1], "LINESTRING(0 0,1 1)".to_string())],
    );

    // Points and topologically empty inputs produce zero rows.
    assert!(
        dump_rows_for(&ctx, "ST_DumpSegments", "POINT(1 2)")
            .await
            .is_empty()
    );
    assert!(
        dump_rows_for(&ctx, "ST_DumpSegments", "LINESTRING EMPTY")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn dump_rings_returns_single_ring_polygons() {
    let ctx = new_ctx();

    assert_eq!(
        dump_rows_for(
            &ctx,
            "ST_DumpRings",
            "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))"
        )
        .await,
        vec![
            (vec![0], "POLYGON((0 0,4 0,4 4,0 4,0 0))".to_string()),
            (vec![1], "POLYGON((1 1,2 1,2 2,1 2,1 1))".to_string()),
        ],
    );
    assert!(
        dump_rows_for(&ctx, "ST_DumpRings", "POLYGON EMPTY")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn dump_rings_rejects_non_polygons() {
    let ctx = new_ctx();
    let error = ctx
        .sql("SELECT ST_DumpRings(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 0)))'))")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("ST_DumpRings must reject MULTIPOLYGON input");
    assert!(
        error
            .to_string()
            .contains("ST_DumpRings input must be a POLYGON"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn points_concatenates_all_coordinates() {
    let ctx = new_ctx();

    assert_scalar_text(
        &ctx,
        "SELECT ST_AsText(ST_Points(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)')))",
        "MULTIPOINT((0 0),(1 1),(2 0))",
    )
    .await;
    assert_scalar_text(
        &ctx,
        "SELECT ST_AsText(ST_Points(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 0))')))",
        "MULTIPOINT((0 0),(4 0),(4 4),(0 0))",
    )
    .await;
    assert_scalar_text(
        &ctx,
        "SELECT ST_AsText(ST_Points(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))')))",
        "MULTIPOINT((1 2),(0 0),(1 1))",
    )
    .await;
    assert_scalar_text(
        &ctx,
        "SELECT ST_AsText(ST_Points(ST_GeomFromText('POINT EMPTY')))",
        "MULTIPOINT EMPTY",
    )
    .await;
}

#[tokio::test]
async fn summary_matches_postgis_layout() {
    let ctx = new_ctx();

    assert_scalar_text(
        &ctx,
        "SELECT ST_Summary(ST_GeomFromText('POINT(1 2)'))",
        "Point[]",
    )
    .await;
    assert_scalar_text(
        &ctx,
        "SELECT ST_Summary(ST_GeomFromText('LINESTRING EMPTY'))",
        "LineString[] with 0 points",
    )
    .await;
    assert_scalar_text(
        &ctx,
        "SELECT ST_Summary(ST_GeomFromText('POLYGON EMPTY'))",
        "Polygon[] with 0 rings",
    )
    .await;
    assert_scalar_text(
        &ctx,
        "SELECT ST_Summary(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))'))",
        "Polygon[] with 2 rings:\n   ring 0 has 5 points\n   ring 1 has 5 points",
    )
    .await;
    assert_scalar_text(
        &ctx,
        "SELECT ST_Summary(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))'))",
        "GeometryCollection[] with 2 elements:\n  Point[]\n  LineString[] with 2 points",
    )
    .await;
}

#[tokio::test]
async fn summary_reports_z_m_and_srid_flags() {
    let ctx = new_ctx();

    assert_scalar_text(
        &ctx,
        "SELECT ST_Summary(ST_GeomFromText('POINT ZM (1 2 3 4)'))",
        "Point[ZM]",
    )
    .await;
    assert_scalar_text(
        &ctx,
        "SELECT ST_Summary(ST_GeomFromText('POINT M (1 2 3)'))",
        "Point[M]",
    )
    .await;
    assert_scalar_text(
        &ctx,
        "SELECT ST_Summary(ST_GeomFromEWKT('SRID=4326;POINT(1 2)'))",
        "Point[S]",
    )
    .await;
    assert_scalar_text(
        &ctx,
        "SELECT ST_Summary(ST_GeomFromEWKT('SRID=4326;MULTIPOINT((1 2),(3 4))'))",
        "MultiPoint[S] with 2 elements:\n  Point[S]\n  Point[S]",
    )
    .await;
}

fn register_lines(ctx: &SessionContext, array: geoarrow_array::array::LineStringArray) {
    let field = array.data_type().to_field("geometry", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![array.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("lines", batch).unwrap();
}

#[tokio::test]
async fn dump_points_preserves_zm_on_sliced_input() {
    let ctx = new_ctx();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs, None));
    let array = linestring::array(CoordType::Interleaved, Dimension::XYZM)
        .with_metadata(metadata.clone())
        // Slice away the leading rows so the UDF sees a non-zero offset.
        .slice(1, 2);
    register_lines(&ctx, array);

    let batch = query_batch(&ctx, "SELECT ST_DumpPoints(geometry) FROM lines").await;
    assert_eq!(
        decode_dump_row(batch.column(0), 0).unwrap(),
        vec![
            (vec![1], "POINT ZM(40 20 60 800)".to_string()),
            (vec![2], "POINT ZM(20 40 60 800)".to_string()),
            (vec![3], "POINT ZM(50 50 100 2500)".to_string()),
        ],
    );
    assert!(decode_dump_row(batch.column(0), 1).is_none());

    let schema = batch.schema();
    let DataType::List(list_field) = schema.field(0).data_type() else {
        panic!("ST_DumpPoints must return a list");
    };
    let DataType::Struct(fields) = list_field.data_type() else {
        panic!("ST_DumpPoints must return a list of structs");
    };
    // Dump output is a generic Geometry column, so its dimension is variable,
    // but the CRS metadata must be preserved.
    let output_type = GeoArrowType::from_extension_field(&fields[1])
        .unwrap()
        .unwrap();
    assert!(matches!(output_type, GeoArrowType::Geometry(_)));
    assert_eq!(output_type.metadata().as_ref(), metadata.as_ref());
}

#[tokio::test]
async fn points_preserves_zm_and_metadata() {
    let ctx = new_ctx();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs, None));
    let array = multipoint::array(CoordType::Separated, Dimension::XYZM)
        .with_metadata(metadata.clone())
        .slice(1, 1);
    let field = array.data_type().to_field("geometry", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![array.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("mpts", batch).unwrap();

    let df = ctx
        .sql("SELECT ST_Points(geometry), ST_AsText(ST_Points(geometry)) FROM mpts")
        .await
        .unwrap();
    let schema = df.schema().inner().clone();
    let output_type = GeoArrowType::from_extension_field(schema.field(0))
        .unwrap()
        .unwrap();
    assert_eq!(output_type.coord_type(), Some(CoordType::Separated));
    assert_eq!(output_type.metadata().as_ref(), metadata.as_ref());

    let batch = df.collect().await.unwrap().into_iter().next().unwrap();
    assert!(!batch.column(0).is_null(0));
    assert_eq!(
        batch.column(1).as_string::<i32>().value(0),
        "MULTIPOINT ZM((10 40 50 400),(40 30 70 1200),(20 20 40 400),(30 10 40 300))"
    );
}

#[tokio::test]
async fn summary_handles_zm_empty_and_null_rows() {
    let ctx = new_ctx();
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs, None));
    let array = linestring::array(CoordType::Separated, Dimension::XYZM)
        .with_metadata(metadata)
        .slice(2, 2);
    register_lines(&ctx, array);

    let batch = query_batch(&ctx, "SELECT ST_Summary(geometry) FROM lines").await;
    let summaries = batch.column(0).as_string::<i32>();
    assert!(summaries.is_null(0));
    assert_eq!(summaries.value(1), "LineString[ZMS] with 0 points");

    // A NULL input row yields a NULL dump list; an EMPTY row yields an empty list.
    let batch = query_batch(&ctx, "SELECT ST_DumpPoints(geometry) FROM lines").await;
    assert!(decode_dump_row(batch.column(0), 0).is_none());
    assert_eq!(decode_dump_row(batch.column(0), 1).unwrap(), Vec::new());
}

#[tokio::test]
async fn out_of_range_and_empty_results_are_nullable() {
    let ctx = new_ctx();

    let batch = query_batch(
        &ctx,
        "SELECT \
         ST_PointN(ST_GeomFromText('LINESTRING(0 0,1 1)'), 5) IS NULL, \
         ST_PointN(ST_GeomFromText('LINESTRING EMPTY'), 1) IS NULL, \
         ST_GeometryN(ST_GeomFromText('MULTIPOINT((1 2))'), 3) IS NULL, \
         ST_InteriorRingN(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))'), 1) IS NULL",
    )
    .await;
    for index in 0..4 {
        assert!(
            batch.column(index).as_boolean().value(0),
            "column {index} should be NULL"
        );
    }

    // A non-nullable geometry column must still produce a nullable output field,
    // because out-of-range indices and EMPTY geometries return NULL.
    let ls_type = LineStringType::new(Dimension::XY, Default::default());
    let empty = WktLineString::new(vec![], WktDimension::XY);
    let non_empty = WktLineString::new(
        vec![
            WktCoord {
                x: 0.,
                y: 0.,
                z: None,
                m: None,
            },
            WktCoord {
                x: 1.,
                y: 1.,
                z: None,
                m: None,
            },
        ],
        WktDimension::XY,
    );
    let lines = LineStringBuilder::from_line_strings(&[empty, non_empty], ls_type).finish();
    let field = lines.data_type().to_field("geometry", false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![lines.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("non_null_lines", batch).unwrap();

    let df = ctx
        .sql(
            "SELECT ST_GeometryN(geometry, 1), ST_PointN(geometry, 1), \
             ST_InteriorRingN(geometry, 1) FROM non_null_lines",
        )
        .await
        .unwrap();
    for field in df.schema().inner().fields() {
        assert!(field.is_nullable(), "{} must be nullable", field.name());
    }

    let batch = query_batch(
        &ctx,
        "SELECT ST_GeometryN(geometry, 1) IS NULL, ST_GeometryN(geometry, 9) IS NULL, \
         ST_PointN(geometry, 1) IS NULL, ST_InteriorRingN(geometry, 1) IS NULL \
         FROM non_null_lines",
    )
    .await;
    for column in 0..4 {
        assert!(
            batch.column(column).as_boolean().value(0),
            "column {column} of the EMPTY row must be NULL"
        );
    }
    assert!(!batch.column(0).as_boolean().value(1));
    assert!(batch.column(1).as_boolean().value(1));
    assert!(!batch.column(2).as_boolean().value(1));
    assert!(batch.column(3).as_boolean().value(1));
}

/// The pinned PostGIS reference for
/// `GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2)),POINT(3 4))` with the
/// cached-bbox `B` flag removed, matching the oracle normalization.
const NESTED_GEOMETRY_COLLECTION_SUMMARY: &str = "GeometryCollection[] with 2 elements:\n  \
     GeometryCollection[] with 1 element:\n    Point[]\n  Point[]";

/// Nested geometry collections cannot be built as a native GeoArrow array, but
/// typed WKB can represent them. ST_Summary reads the WKB directly, so the
/// nested structure must survive and match the pinned PostGIS summary.
#[tokio::test]
async fn summary_supports_nested_geometry_collections_from_typed_wkb() {
    let ctx = new_ctx();
    // EWKB captured from the pinned PostGIS 3.6.4 reference.
    let nested = hex::decode(
        "0107000000020000000107000000010000000101000000000000000000f03f0000000000000040\
         010100000000000000000008400000000000001040",
    )
    .unwrap();
    let point = hex::decode("0101000000000000000000f03f0000000000000040").unwrap();

    let wkb_type = GeoArrowType::Wkb(WkbType::new(Arc::new(Metadata::default())));
    let field = wkb_type.to_field("geometry", true);
    let mut builder = BinaryBuilder::new();
    builder.append_value(&nested);
    builder.append_null();
    builder.append_value(&point);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(builder.finish())],
    )
    .unwrap();
    ctx.register_batch("nested_wkb", batch).unwrap();

    let batch = query_batch(&ctx, "SELECT ST_Summary(geometry) FROM nested_wkb").await;
    let summaries = batch.column(0).as_string::<i32>();
    assert_eq!(summaries.value(0), NESTED_GEOMETRY_COLLECTION_SUMMARY);
    assert!(summaries.is_null(1));
    assert_eq!(summaries.value(2), "Point[]");
}

const NESTED_GEOMETRY_COLLECTION_WKB: &str = "01070000000200000001070000000100000001020000000200000000000000000000000000000000000000000000000000084000000000000010400101000000000000000000f03f0000000000000040";
const NESTED_GEOMETRY_COLLECTION_TEXT: &str =
    "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING(0 0,3 4)),POINT(1 2))";
const NESTED_GEOMETRY_COLLECTION_LINE_SUMMARY: &str = "GeometryCollection[] with 2 elements:\n  \
     GeometryCollection[] with 1 element:\n    LineString[] with 2 points\n  Point[]";

/// Typed WKB keeps nested collections representable. The text, summary, and
/// length consumers must read that representation, while the native builder
/// still refuses to materialize it.
#[tokio::test]
async fn typed_wkb_nested_collections_flow_through_text_summary_and_length() {
    let ctx = new_ctx();
    let nested = hex::decode(NESTED_GEOMETRY_COLLECTION_WKB).unwrap();

    let wkb_type = GeoArrowType::Wkb(WkbType::new(Arc::new(Metadata::default())));
    let field = wkb_type.to_field("geometry", true);
    let mut builder = BinaryBuilder::new();
    builder.append_value(&nested);
    builder.append_null();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(builder.finish())],
    )
    .unwrap();
    ctx.register_batch("nested_wkb", batch).unwrap();

    let batch = query_batch(
        &ctx,
        "SELECT ST_AsText(geometry), ST_Summary(geometry), ST_Length(geometry) FROM nested_wkb",
    )
    .await;
    let texts = batch.column(0).as_string::<i32>();
    assert_eq!(texts.value(0), NESTED_GEOMETRY_COLLECTION_TEXT);
    assert!(texts.is_null(1));
    let summaries = batch.column(1).as_string::<i32>();
    assert_eq!(summaries.value(0), NESTED_GEOMETRY_COLLECTION_LINE_SUMMARY);
    assert!(summaries.is_null(1));
    let lengths = batch.column(2).as_primitive::<Float64Type>();
    assert_eq!(lengths.value(0), 5.0);
    assert!(lengths.is_null(1));

    let error = ctx
        .sql(&format!(
            "SELECT ST_GeomFromWKB(X'{}')",
            hex::encode_upper(&nested)
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("nested GeometryCollection"),
        "expected an explicit native nested-collection rejection, got: {error}"
    );
}

/// Typed WKB nesting is bounded instead of overflowing the formatter stack.
#[tokio::test]
async fn typed_wkb_nested_collections_report_depth_limit() {
    let ctx = new_ctx();
    let mut nested = Vec::with_capacity(1000 * 9 + 9);
    for _ in 0..1000 {
        nested.extend_from_slice(&[0x01, 0x07, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00]);
    }
    nested.extend_from_slice(&[0x01, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

    let wkb_type = GeoArrowType::Wkb(WkbType::new(Arc::new(Metadata::default())));
    let field = wkb_type.to_field("geometry", true);
    let mut builder = BinaryBuilder::new();
    builder.append_value(&nested);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(field)])),
        vec![Arc::new(builder.finish())],
    )
    .unwrap();
    ctx.register_batch("deep_nested_wkb", batch).unwrap();

    let error = ctx
        .sql("SELECT ST_AsText(geometry) FROM deep_nested_wkb")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("nesting depth exceeds"),
        "expected a controlled nesting-depth error, got: {error}"
    );
}
