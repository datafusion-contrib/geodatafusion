//! Public SQL and raw Arrow contracts for native planar processing.
//!
//! The expected values are the pinned PostGIS 3.6.4 / GEOS 3.14.1 reference
//! results unless a test names an explicit GeoDataFusion support boundary.

mod support;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geo_traits::{Dimensions, GeometryTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray, PointArray, from_arrow_array};
use geoarrow_array::builder::{
    LineStringBuilder, MultiLineStringBuilder, MultiPolygonBuilder, PolygonBuilder,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeometryType, LineStringType, Metadata, MultiLineStringType,
    MultiPolygonType, PointType, PolygonType,
};
use support::context::context;
use support::geoarrow::collect;
use wkt::Wkt;

async fn query(context: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    collect(context, sql).await.unwrap()
}

async fn scalar_f64(context: &SessionContext, sql: &str) -> Option<f64> {
    let batches = query(context, sql).await;
    let values = batches[0].column(0).as_primitive::<Float64Type>();
    (!values.is_null(0)).then(|| values.value(0))
}

async fn scalar_text(context: &SessionContext, sql: &str) -> Option<String> {
    let batches = query(context, sql).await;
    let values = batches[0].column(0).as_string::<i32>();
    (!values.is_null(0)).then(|| values.value(0).to_string())
}

async fn scalar_bool(context: &SessionContext, sql: &str) -> bool {
    let batches = query(context, sql).await;
    batches[0].column(0).as_boolean().value(0)
}

fn assert_close(actual: f64, expected: f64, context: &str) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "{context}: {actual} != {expected}"
    );
}

fn metadata_4326() -> Arc<Metadata> {
    Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ))
}

fn parse_wkt(value: &str) -> Wkt<f64> {
    value.parse().unwrap()
}

fn register_geometry(
    context: &SessionContext,
    name: &str,
    array: Arc<dyn GeoArrowArray>,
    nullable: bool,
) -> Field {
    let field = array.data_type().to_field("geom", nullable);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field.clone()])),
        vec![array.to_array_ref()],
    )
    .unwrap();
    context.register_batch(name, batch).unwrap();
    field
}

#[tokio::test]
async fn area_matches_postgis_values_for_planar_and_measured_inputs() {
    let context = context();

    assert_eq!(
        scalar_f64(
            &context,
            "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'))",
        )
        .await,
        Some(100.0)
    );
    assert_eq!(
        scalar_f64(
            &context,
            "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 8,8 8,8 2,2 2))'))",
        )
        .await,
        Some(64.0)
    );
    assert_eq!(
        scalar_f64(
            &context,
            "SELECT ST_Area(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((10 10,11 10,11 11,10 11,10 10)))'))",
        )
        .await,
        Some(2.0)
    );
    assert_eq!(
        scalar_f64(
            &context,
            "SELECT ST_Area(ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON((0 0,1 0,1 1,0 1,0 0)),LINESTRING(0 0,5 0))'))",
        )
        .await,
        Some(1.0)
    );
    assert_eq!(
        scalar_f64(&context, "SELECT ST_Area(ST_GeomFromText('POINT(1 2)'))").await,
        Some(0.0)
    );
    assert_eq!(
        scalar_f64(
            &context,
            "SELECT ST_Area(ST_GeomFromText('LINESTRING(0 0,3 4)'))",
        )
        .await,
        Some(0.0)
    );
    assert_eq!(
        scalar_f64(&context, "SELECT ST_Area(ST_GeomFromText('POLYGON EMPTY'))",).await,
        Some(0.0)
    );
    assert_eq!(
        scalar_f64(
            &context,
            "SELECT ST_Area(ST_GeomFromText('POLYGON Z ((0 0 1,10 0 2,10 10 3,0 10 4,0 0 1))'))",
        )
        .await,
        Some(100.0)
    );
    assert_eq!(
        scalar_f64(
            &context,
            "SELECT ST_Area(ST_GeomFromText('POLYGON M ((0 0 1,10 0 2,10 10 3,0 10 4,0 0 1))'))",
        )
        .await,
        Some(100.0)
    );
    assert_eq!(
        scalar_f64(
            &context,
            "SELECT ST_Area(ST_GeomFromText('POLYGON ZM ((0 0 1 5,10 0 2 6,10 10 3 7,0 10 4 8,0 0 1 5))'))",
        )
        .await,
        Some(100.0)
    );
    assert!(scalar_bool(&context, "SELECT ST_Area(NULL) IS NULL").await);
}

#[tokio::test]
async fn centroid_matches_postgis_points_empties_and_null() {
    let context = context();

    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_Centroid(ST_GeomFromText('POINT(1 2)')))",
        )
        .await,
        Some("POINT(1 2)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_Centroid(ST_GeomFromText('LINESTRING(0 0,10 0)')))",
        )
        .await,
        Some("POINT(5 0)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_Centroid(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,1 9,9 9,9 1,1 1))')))",
        )
        .await,
        Some("POINT(5 5)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_Centroid(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,2 2))')))",
        )
        .await,
        Some("POINT(1 1)".to_string())
    );
    for wkt in [
        "POINT EMPTY",
        "LINESTRING EMPTY",
        "POLYGON EMPTY",
        "GEOMETRYCOLLECTION EMPTY",
    ] {
        assert_eq!(
            scalar_text(
                &context,
                &format!("SELECT ST_AsText(ST_Centroid(ST_GeomFromText('{wkt}')))"),
            )
            .await,
            Some("POINT EMPTY".to_string()),
            "{wkt}"
        );
    }
    // PostGIS always computes ST_Centroid over XY, so Z and M inputs also
    // produce a two-dimensional point.
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_Centroid(ST_GeomFromText('LINESTRING Z (0 0 1,2 2 3)')))",
        )
        .await,
        Some("POINT(1 1)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_Centroid(ST_GeomFromText('LINESTRING M (0 0 1,2 2 3)')))",
        )
        .await,
        Some("POINT(1 1)".to_string())
    );
    assert!(scalar_bool(&context, "SELECT ST_Centroid(NULL) IS NULL").await);
}

#[tokio::test]
async fn point_on_surface_matches_postgis_planar_contract() {
    let context = context();

    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_PointOnSurface(ST_GeomFromText('POINT(1 2)')))",
        )
        .await,
        Some("POINT(1 2)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_PointOnSurface(ST_GeomFromText('LINESTRING(0 0,5 5,10 0)')))",
        )
        .await,
        Some("POINT(5 5)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_PointOnSurface(ST_GeomFromText('MULTILINESTRING((0 0,10 0),(0 5,10 5))')))",
        )
        .await,
        Some("POINT(0 0)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_PointOnSurface(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,1 9,9 9,9 1,1 1))')))",
        )
        .await,
        Some("POINT(0.5 5)".to_string())
    );
    for wkt in ["POINT EMPTY", "LINESTRING EMPTY", "POLYGON EMPTY"] {
        assert_eq!(
            scalar_text(
                &context,
                &format!("SELECT ST_AsText(ST_PointOnSurface(ST_GeomFromText('{wkt}')))"),
            )
            .await,
            Some("POINT EMPTY".to_string()),
            "{wkt}"
        );
    }
    // GeoDataFusion exposes a two-dimensional point. PostGIS keeps the Z
    // ordinate for lineal inputs such as `LINESTRING Z (0 0 9,5 5 5,10 0 1)`,
    // where it returns `POINT(5 5 5)`.
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_PointOnSurface(ST_GeomFromText('LINESTRING Z (0 0 9,5 5 5,10 0 1)')))",
        )
        .await,
        Some("POINT(5 5)".to_string())
    );
    assert!(scalar_bool(&context, "SELECT ST_PointOnSurface(NULL) IS NULL").await);
}

#[tokio::test]
async fn convex_hull_keeps_empty_family_and_degenerate_shapes() {
    let context = context();

    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_ConvexHull(ST_GeomFromText('POINT(1 2)')))",
        )
        .await,
        Some("POINT(1 2)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_ConvexHull(ST_GeomFromText('MULTIPOINT((0 0),(2 2))')))",
        )
        .await,
        Some("LINESTRING(0 0,2 2)".to_string())
    );
    for (wkt, expected) in [
        ("POINT EMPTY", "POINT EMPTY"),
        ("LINESTRING EMPTY", "LINESTRING EMPTY"),
        ("POLYGON EMPTY", "POLYGON EMPTY"),
        ("GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION EMPTY"),
    ] {
        assert_eq!(
            scalar_text(
                &context,
                &format!("SELECT ST_AsText(ST_ConvexHull(ST_GeomFromText('{wkt}')))"),
            )
            .await,
            Some(expected.to_string()),
            "{wkt}"
        );
    }
    assert!(scalar_bool(
        &context,
        "SELECT ST_GeometryType(ST_ConvexHull(ST_GeomFromText('MULTIPOINT((0 0),(0 1),(1 1),(1 0))'))) = 'ST_Polygon'",
    )
    .await);
    assert!(scalar_bool(
        &context,
        "SELECT abs(ST_Area(ST_ConvexHull(ST_GeomFromText('MULTIPOINT((0 0),(0 1),(1 1),(1 0))'))) - 1) < 1e-6",
    )
    .await);
    // GeoDataFusion computes the hull over XY. PostGIS preserves Z on
    // non-empty hulls (`POINT Z (1 2 3)` stays three-dimensional there).
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_ConvexHull(ST_GeomFromText('POINT Z (1 2 3)')))",
        )
        .await,
        Some("POINT(1 2)".to_string())
    );
    assert!(scalar_bool(&context, "SELECT ST_ConvexHull(NULL) IS NULL").await);
}

#[tokio::test]
async fn oriented_envelope_uses_point_line_and_polygon_shapes() {
    let context = context();

    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_OrientedEnvelope(ST_GeomFromText('POINT(1 2)')))",
        )
        .await,
        Some("POINT(1 2)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_OrientedEnvelope(ST_GeomFromText('LINESTRING(0 0,2 2)')))",
        )
        .await,
        Some("LINESTRING(0 0,2 2)".to_string())
    );
    for wkt in ["POINT EMPTY", "LINESTRING EMPTY", "POLYGON EMPTY"] {
        assert_eq!(
            scalar_text(
                &context,
                &format!("SELECT ST_AsText(ST_OrientedEnvelope(ST_GeomFromText('{wkt}')))"),
            )
            .await,
            Some("POLYGON EMPTY".to_string()),
            "{wkt}"
        );
    }
    assert!(scalar_bool(
        &context,
        "SELECT ST_GeometryType(ST_OrientedEnvelope(ST_GeomFromText('MULTIPOINT((0 0),(0 2),(2 2),(2 0))'))) = 'ST_Polygon'",
    )
    .await);
    assert!(scalar_bool(
        &context,
        "SELECT abs(ST_Area(ST_OrientedEnvelope(ST_GeomFromText('MULTIPOINT((0 0),(0 2),(2 2),(2 0))'))) - 4) < 1e-6",
    )
    .await);
    // PostGIS also drops Z on oriented envelopes, so the 2D output matches the
    // reference for this function.
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_OrientedEnvelope(ST_GeomFromText('LINESTRING Z (0 0 1,2 2 3)')))",
        )
        .await,
        Some("LINESTRING(0 0,2 2)".to_string())
    );
    assert!(scalar_bool(&context, "SELECT ST_OrientedEnvelope(NULL) IS NULL").await);
}

#[tokio::test]
async fn hull_degeneracy_uses_robust_orientation() {
    let context = context();

    // A tiny triangle must remain a polygon even though its cross product is
    // smaller than an epsilon scaled by the absolute coordinate magnitude.
    for function in ["ST_ConvexHull", "ST_OrientedEnvelope"] {
        assert!(
            scalar_bool(
                &context,
                &format!(
                    "SELECT ST_GeometryType({function}(ST_GeomFromText('MULTIPOINT((0 0),(0.00000001 0),(0 0.00000001))'))) = 'ST_Polygon'"
                ),
            )
            .await,
            "{function} tiny triangle"
        );
        assert!(
            scalar_bool(
                &context,
                &format!(
                    "SELECT ST_GeometryType({function}(ST_GeomFromText('MULTIPOINT((1000000 1000000),(1000000.000001 1000000),(1000000 1000000.000001))'))) = 'ST_Polygon'"
                ),
            )
            .await,
            "{function} translated tiny triangle"
        );
    }

    // Translation by a large coordinate offset must not change the hull area.
    assert_close(
        scalar_f64(
            &context,
            "SELECT ST_Area(ST_ConvexHull(ST_GeomFromText('MULTIPOINT((1000000000000 1000000000000),(1000000000010 1000000000000),(1000000000000 1000000000010))')))",
        )
        .await
        .unwrap(),
        50.0,
        "translated hull area",
    );
}

#[tokio::test]
async fn simplify_matches_postgis_planar_contract_and_negative_tolerance() {
    let context = context();

    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_Simplify(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)'), 2))",
        )
        .await,
        Some("LINESTRING(0 0,2 0)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_Simplify(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)'), -2))",
        )
        .await,
        Some("LINESTRING(0 0,2 0)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_Simplify(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)'), 0.5))",
        )
        .await,
        Some("LINESTRING(0 0,1 1,2 0)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_Simplify(ST_GeomFromText('LINESTRING EMPTY'), 1))",
        )
        .await,
        Some("LINESTRING EMPTY".to_string())
    );
    assert!(scalar_bool(&context, "SELECT ST_Simplify(NULL, 1) IS NULL").await);
    assert!(
        scalar_bool(
            &context,
            "SELECT ST_Simplify(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)'), NULL) IS NULL",
        )
        .await
    );
    // GeoDataFusion keeps the planar XY contract. PostGIS preserves Z here
    // and returns `LINESTRING Z(0 0 1,2 0 3)`.
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_Simplify(ST_GeomFromText('LINESTRING Z (0 0 1,1 1 2,2 0 3)'), 2))",
        )
        .await,
        Some("LINESTRING(0 0,2 0)".to_string())
    );
}

#[tokio::test]
async fn simplify_vw_and_preserve_topology_contracts() {
    let context = context();

    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_SimplifyVW(ST_GeomFromText('LINESTRING(5 2,3 8,6 20,7 25,10 10)'), 30))",
        )
        .await,
        Some("LINESTRING(5 2,7 25,10 10)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_SimplifyVW(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)'), 0))",
        )
        .await,
        Some("LINESTRING(0 0,1 1,2 0)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_SimplifyVW(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)'), -0.5))",
        )
        .await,
        Some("LINESTRING(0 0,1 1,2 0)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_SimplifyVW(ST_GeomFromText('LINESTRING EMPTY'), 1))",
        )
        .await,
        Some("LINESTRING EMPTY".to_string())
    );
    assert!(scalar_bool(&context, "SELECT ST_SimplifyVW(NULL, 1) IS NULL").await);
    assert!(
        scalar_bool(
            &context,
            "SELECT ST_SimplifyVW(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)'), NULL) IS NULL",
        )
        .await
    );

    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_SimplifyPreserveTopology(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)'), 2))",
        )
        .await,
        Some("LINESTRING(0 0,2 0)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_SimplifyPreserveTopology(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)'), 0.5))",
        )
        .await,
        Some("LINESTRING(0 0,1 1,2 0)".to_string())
    );
    assert_eq!(
        scalar_text(
            &context,
            "SELECT ST_AsText(ST_SimplifyPreserveTopology(ST_GeomFromText('LINESTRING EMPTY'), 1))",
        )
        .await,
        Some("LINESTRING EMPTY".to_string())
    );
    assert!(
        scalar_bool(
            &context,
            "SELECT ST_SimplifyPreserveTopology(NULL, 1) IS NULL",
        )
        .await
    );
    assert!(scalar_bool(
        &context,
        "SELECT ST_SimplifyPreserveTopology(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)'), NULL) IS NULL",
    )
    .await);
    let error = context
        .sql("SELECT ST_SimplifyPreserveTopology(ST_GeomFromText('LINESTRING(0 0,1 1,2 0)'), -0.5)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Tolerance must be non-negative"), "{error}");
}

#[tokio::test]
async fn measured_typed_inputs_follow_the_documented_xy_boundary() {
    for (dimension, wkt) in [
        (Dimension::XYZ, "LINESTRING Z (0 0 1,5 0.1 2,10 0 3)"),
        (Dimension::XYM, "LINESTRING M (0 0 1,5 0.1 2,10 0 3)"),
        (
            Dimension::XYZM,
            "LINESTRING ZM (0 0 1 9,5 0.1 2 8,10 0 3 7)",
        ),
    ] {
        let context = context();
        let typ =
            LineStringType::new(dimension, metadata_4326()).with_coord_type(CoordType::Separated);
        let mut builder = LineStringBuilder::new(typ);
        let Wkt::LineString(line) = wkt.parse::<Wkt<f64>>().unwrap() else {
            panic!("expected LINESTRING")
        };
        builder.push_line_string(Some(&line)).unwrap();
        let array = builder.finish();
        let field = array.data_type().to_field("geom", true);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![array.to_array_ref()],
        )
        .unwrap();
        context.register_batch("measured", batch).unwrap();

        let batches = query(
            &context,
            "SELECT ST_GeometryType(ST_ConvexHull(geom)) = 'ST_Polygon' AS is_polygon, \
                    ST_ConvexHull(geom) AS hull, \
                    ST_PointOnSurface(geom) AS pos, \
                    ST_Simplify(geom, 2) AS simplified, \
                    ST_AsText(ST_Simplify(geom, 2)) AS simplified_text, \
                    ST_SimplifyVW(geom, 30) AS vw, \
                    ST_AsText(ST_SimplifyVW(geom, 30)) AS vw_text, \
                    ST_SimplifyPreserveTopology(geom, 2) AS topo, \
                    ST_AsText(ST_SimplifyPreserveTopology(geom, 2)) AS topo_text \
             FROM measured",
        )
        .await;
        let batch = &batches[0];
        let schema = batch.schema_ref();
        assert!(batch.column(0).as_boolean().value(0), "{dimension:?}");
        let hull = GeometryArray::try_from((batch.column(1).as_ref(), schema.field(1))).unwrap();
        assert_eq!(
            hull.value(0).unwrap().dim(),
            Dimensions::Xy,
            "{dimension:?}"
        );
        assert_eq!(
            schema
                .field(2)
                .try_extension_type::<PointType>()
                .unwrap()
                .dimension(),
            Dimension::XY,
            "{dimension:?}"
        );
        let pos = PointArray::try_from((batch.column(2).as_ref(), schema.field(2))).unwrap();
        assert_eq!(pos.value(0).unwrap().dim(), Dimensions::Xy, "{dimension:?}");
        for (text_index, geometry_index, label) in [
            (4usize, 3usize, "ST_Simplify"),
            (6, 5, "ST_SimplifyVW"),
            (8, 7, "ST_SimplifyPreserveTopology"),
        ] {
            let typ = schema
                .field(geometry_index)
                .try_extension_type::<LineStringType>()
                .unwrap();
            assert_eq!(typ.dimension(), Dimension::XY, "{label} {dimension:?}");
            let line = LineStringArray::try_from((
                batch.column(geometry_index).as_ref(),
                schema.field(geometry_index),
            ))
            .unwrap();
            assert_eq!(
                line.value(0).unwrap().dim(),
                Dimensions::Xy,
                "{label} {dimension:?}"
            );
            assert_eq!(
                batch.column(text_index).as_string::<i32>().value(0),
                "LINESTRING(0 0,10 0)",
                "{label} {dimension:?}"
            );
        }
    }
}

#[tokio::test]
async fn simplify_normalizes_measured_areal_and_multi_inputs_to_xy() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        for (wkt, kind) in [
            (
                "POLYGON ZM ((0 0 1 10,10 0 2 20,10 10 3 30,0 10 4 40,0 0 1 10))",
                "Polygon",
            ),
            (
                "MULTILINESTRING ZM ((0 0 1 10,1 1 2 20,2 0 3 30),(10 10 4 40,11 11 5 50))",
                "MultiLineString",
            ),
            (
                "MULTIPOLYGON ZM (((0 0 1 10,1 0 2 20,1 1 3 30,0 1 4 40,0 0 1 10)))",
                "MultiPolygon",
            ),
        ] {
            let context = context();
            let array: Arc<dyn GeoArrowArray> = match wkt.parse::<Wkt<f64>>().unwrap() {
                Wkt::Polygon(geometry) => {
                    let typ = PolygonType::new(Dimension::XYZM, metadata_4326())
                        .with_coord_type(coord_type);
                    let mut builder = PolygonBuilder::new(typ);
                    builder.push_geometry(Some(&geometry)).unwrap();
                    Arc::new(builder.finish())
                }
                Wkt::MultiLineString(geometry) => {
                    let typ = MultiLineStringType::new(Dimension::XYZM, metadata_4326())
                        .with_coord_type(coord_type);
                    let mut builder = MultiLineStringBuilder::new(typ);
                    builder.push_geometry(Some(&geometry)).unwrap();
                    Arc::new(builder.finish())
                }
                Wkt::MultiPolygon(geometry) => {
                    let typ = MultiPolygonType::new(Dimension::XYZM, metadata_4326())
                        .with_coord_type(coord_type);
                    let mut builder = MultiPolygonBuilder::new(typ);
                    builder.push_geometry(Some(&geometry)).unwrap();
                    Arc::new(builder.finish())
                }
                other => panic!("unexpected geometry {other:?}"),
            };
            let field = array.data_type().to_field("geom", true);
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![field])),
                vec![array.to_array_ref()],
            )
            .unwrap();
            context.register_batch("measured", batch).unwrap();

            let batches = query(
                &context,
                "SELECT ST_Simplify(geom, 1) AS simplified, \
                        ST_SimplifyVW(geom, 1) AS vw, \
                        ST_SimplifyPreserveTopology(geom, 1) AS topo \
                 FROM measured",
            )
            .await;
            let batch = &batches[0];
            for index in 0..3 {
                let array = from_arrow_array(
                    batch.column(index).as_ref(),
                    batch.schema_ref().field(index),
                )
                .unwrap();
                assert_eq!(
                    array.data_type().dimension(),
                    Some(Dimension::XY),
                    "{kind} {coord_type:?} column {index}"
                );
            }
        }
    }
}

#[tokio::test]
async fn planar_processing_preserves_crs_coord_type_and_typed_slices() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let context = context();
        let metadata = metadata_4326();
        let mut builder = geoarrow_array::builder::GeometryBuilder::new(
            GeometryType::new(metadata.clone()).with_coord_type(coord_type),
        );
        for geometry in ["POLYGON((0 0,10 0,10 10,0 10,0 0))", "POINT(1 2)"] {
            builder.push_geometry(Some(&parse_wkt(geometry))).unwrap();
        }
        builder.push_geometry(None::<&Wkt<f64>>).unwrap();
        let array = builder.finish();
        register_geometry(&context, "t", Arc::new(array.clone()), true);

        let batches = query(
            &context,
            "SELECT ST_Area(geom) AS area, \
                    ST_Centroid(geom) AS centroid, \
                    ST_ConvexHull(geom) AS hull, \
                    ST_OrientedEnvelope(geom) AS envelope, \
                    ST_Simplify(geom, 1) AS simplified \
             FROM t",
        )
        .await;
        let schema = batches[0].schema_ref();
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);
        let centroid_type = schema.field(1).try_extension_type::<PointType>().unwrap();
        assert_eq!(centroid_type.metadata().crs(), metadata.crs());
        assert_eq!(centroid_type.coord_type(), CoordType::Separated);
        for index in [2usize, 3] {
            let output_type = schema
                .field(index)
                .try_extension_type::<GeometryType>()
                .unwrap();
            assert_eq!(output_type.metadata().crs(), metadata.crs());
            assert_eq!(output_type.coord_type(), coord_type);
        }
        let simplified_type = schema
            .field(4)
            .try_extension_type::<GeometryType>()
            .unwrap();
        assert_eq!(simplified_type.metadata().crs(), metadata.crs());
        assert_eq!(simplified_type.coord_type(), CoordType::Separated);

        let areas = batches[0].column(0).as_primitive::<Float64Type>();
        assert_close(areas.value(0), 100.0, "typed area polygon");
        assert_close(areas.value(1), 0.0, "typed area point");
        assert!(areas.is_null(2));
        for column in 1..5 {
            let output = from_arrow_array(
                batches[0].column(column).as_ref(),
                batches[0].schema().field(column),
            )
            .unwrap();
            assert!(output.is_null(2), "column {column}");
        }

        let text = query(
            &context,
            "SELECT ST_AsText(ST_Centroid(geom)) AS centroid, \
                    ST_AsText(ST_OrientedEnvelope(geom)) AS envelope \
             FROM t",
        )
        .await;
        assert_eq!(text[0].column(0).as_string::<i32>().value(0), "POINT(5 5)");
        assert_eq!(text[0].column(0).as_string::<i32>().value(1), "POINT(1 2)");
        assert_eq!(text[0].column(1).as_string::<i32>().value(1), "POINT(1 2)");

        let areas = query(
            &context,
            "SELECT ST_Area(ST_ConvexHull(geom)) AS hull, \
                    ST_Area(ST_OrientedEnvelope(geom)) AS envelope, \
                    ST_Area(ST_Simplify(geom, 1)) AS simplified \
             FROM t",
        )
        .await;
        for column in 0..3 {
            let values = areas[0].column(column).as_primitive::<Float64Type>();
            assert_close(values.value(0), 100.0, &format!("column {column}"));
            assert_close(values.value(1), 0.0, &format!("column {column}"));
            assert!(values.is_null(2), "column {column}");
        }

        let sliced = array.slice(1, 2);
        register_geometry(&context, "sliced", Arc::new(sliced), true);
        let text = query(
            &context,
            "SELECT ST_AsText(ST_Centroid(geom)) AS centroid FROM sliced",
        )
        .await;
        assert_eq!(text[0].column(0).as_string::<i32>().value(0), "POINT(1 2)");
        assert!(text[0].column(0).is_null(1));
    }
}
