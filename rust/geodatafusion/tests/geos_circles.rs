#![cfg(feature = "geos-3_11")]

//! Raw-Arrow contracts for the GEOS/native circle function family.

mod support;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema};
use datafusion::prelude::SessionContext;
use geo::polygon;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_array::test::polygon as polygon_test;
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, Metadata};
use support::context::context;
use support::geoarrow::collect;

const CRS_AUTHORITY: &str = "EPSG:4326";

fn metadata() -> Arc<Metadata> {
    Arc::new(Metadata::new(
        geoarrow_schema::crs::Crs::from_authority_code(CRS_AUTHORITY.to_string()),
        None,
    ))
}

async fn first_batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
    collect(ctx, sql)
        .await
        .unwrap_or_else(|error| panic!("query failed: {error}\n{sql}"))
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("query returned no batches: {sql}"))
}

async fn scalar_f64(ctx: &SessionContext, sql: &str) -> Option<f64> {
    let batch = first_batch(ctx, sql).await;
    let values = batch
        .column(0)
        .as_primitive::<arrow_array::types::Float64Type>();
    (!values.is_null(0)).then(|| values.value(0))
}

async fn scalar_bool(ctx: &SessionContext, sql: &str) -> bool {
    let batch = first_batch(ctx, sql).await;
    batch.column(0).as_boolean().value(0)
}

#[tokio::test]
async fn circle_functions_accept_native_boxes() {
    let ctx = context();
    let bbox = "Box2D(ST_GeomFromText('LINESTRING(0 0,10 10)'))";
    let batch = first_batch(
        &ctx,
        &format!(
            "SELECT circle.radius, ST_X(circle.center), ST_Y(circle.center) \
             FROM (SELECT ST_MinimumBoundingRadius({bbox}) AS circle)"
        ),
    )
    .await;
    let values: Vec<_> = batch
        .columns()
        .iter()
        .map(|column| {
            let values = column.as_primitive::<arrow_array::types::Float64Type>();
            assert!(!values.is_null(0), "a non-empty box must produce a circle");
            values.value(0)
        })
        .collect();
    assert!((values[0] - 50_f64.sqrt()).abs() < 1e-12);
    assert_eq!(&values[1..], &[5.0, 5.0]);
    assert!(
        scalar_bool(
            &ctx,
            &format!("SELECT NOT ST_IsEmpty(ST_MinimumBoundingCircle({bbox}))")
        )
        .await
    );
}

#[tokio::test]
async fn maximum_inscribed_circle_has_unique_square_optimum_and_tied_witness() {
    let ctx = context();
    let geometry = "POLYGON((0 0,10 0,10 10,0 10,0 0))";

    let center_x = scalar_f64(
        &ctx,
        &format!(
            "SELECT ST_X(circle.center) FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText('{geometry}')) AS circle)"
        ),
    )
    .await
    .unwrap();
    let center_y = scalar_f64(
        &ctx,
        &format!(
            "SELECT ST_Y(circle.center) FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText('{geometry}')) AS circle)"
        ),
    )
    .await
    .unwrap();
    let nearest_x = scalar_f64(
        &ctx,
        &format!(
            "SELECT ST_X(circle.nearest) FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText('{geometry}')) AS circle)"
        ),
    )
    .await
    .unwrap();
    let nearest_y = scalar_f64(
        &ctx,
        &format!(
            "SELECT ST_Y(circle.nearest) FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText('{geometry}')) AS circle)"
        ),
    )
    .await
    .unwrap();
    let radius = scalar_f64(
        &ctx,
        &format!(
            "SELECT circle.radius FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText('{geometry}')) AS circle)"
        ),
    )
    .await
    .unwrap();

    assert!(
        (center_x - 5.0).abs() < 1e-12 && (center_y - 5.0).abs() < 1e-12,
        "unique MIC center drifted: ({center_x}, {center_y})"
    );
    let tied_witnesses = [(5.0, 0.0), (10.0, 5.0), (5.0, 10.0), (0.0, 5.0)];
    assert!(
        tied_witnesses
            .iter()
            .any(|(x, y)| { (nearest_x - x).abs() < 1e-12 && (nearest_y - y).abs() < 1e-12 }),
        "MIC witness is not one of the four tied square-boundary midpoints: ({nearest_x}, {nearest_y})"
    );
    assert!(
        (radius - 5.0).abs() < 1e-12,
        "unique MIC radius drifted: {radius}"
    );

    assert!(
        scalar_bool(
            &ctx,
            &format!(
                "SELECT circle.radius = ST_Distance(circle.center, circle.nearest) \
             FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText('{geometry}')) AS circle)"
            ),
        )
        .await
    );
}

#[tokio::test]
async fn maximum_inscribed_circle_approximation_stays_within_tolerance() {
    let ctx = context();
    let geometry = "POLYGON((0 0,10 0,0 10,0 0))";
    let exact = 10.0 - 5.0 * 2.0_f64.sqrt();
    let batch = first_batch(
        &ctx,
        &format!(
            "SELECT ST_Within(circle.center, geom), \
                    ST_Intersects(circle.nearest, ST_ExteriorRing(geom)), \
                    abs(circle.radius - ST_Distance(circle.center, circle.nearest)) <= 1e-12, \
                    abs(ST_X(circle.center) - {exact}) <= 0.01, \
                    abs(ST_Y(circle.center) - {exact}) <= 0.01, \
                    abs(circle.radius - {exact}) <= 0.01 \
             FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText('{geometry}')) AS circle, \
                          ST_GeomFromText('{geometry}') AS geom)"
        ),
    )
    .await;
    assert_eq!(
        (0..6)
            .map(|index| batch.column(index).as_boolean().value(0))
            .collect::<Vec<_>>(),
        vec![true; 6]
    );
}

#[tokio::test]
async fn largest_empty_circle_approximation_stays_within_tolerance() {
    let ctx = context();
    let geometry = "MULTIPOINT((0 0),(10 0),(0 6),(3 1))";
    // The optimum is where the hull edge 3x + 5y = 30 meets the bisector
    // 7x - y = 45, giving radius sqrt(21250) / 38.
    let optimum = 21250.0_f64.sqrt() / 38.0;
    let batch = first_batch(
        &ctx,
        &format!(
            "SELECT ST_Intersects(circle.center, ST_ConvexHull(geom)), \
                    ST_Intersects(circle.nearest, geom), \
                    abs(circle.radius - ST_Distance(circle.center, circle.nearest)) <= 1e-12, \
                    ST_Distance(circle.center, geom) >= circle.radius - 1e-12, \
                    circle.radius >= {optimum} - 0.01, \
                    circle.radius <= {optimum} + 1e-12 \
             FROM (SELECT ST_LargestEmptyCircle(ST_GeomFromText('{geometry}'), 0.01) AS circle, \
                          ST_GeomFromText('{geometry}') AS geom)"
        ),
    )
    .await;
    assert_eq!(
        (0..6)
            .map(|index| batch.column(index).as_boolean().value(0))
            .collect::<Vec<_>>(),
        vec![true; 6]
    );
}

#[tokio::test]
async fn minimum_bounding_circle_matches_postgis_values() {
    let ctx = context();
    let geometry = "POLYGON((0 0,10 0,10 10,0 10,0 0))";

    let center_x = scalar_f64(
        &ctx,
        &format!(
            "SELECT ST_X(bounds.center) FROM (SELECT ST_MinimumBoundingRadius(ST_GeomFromText('{geometry}')) AS bounds)"
        ),
    )
    .await
    .unwrap();
    let center_y = scalar_f64(
        &ctx,
        &format!(
            "SELECT ST_Y(bounds.center) FROM (SELECT ST_MinimumBoundingRadius(ST_GeomFromText('{geometry}')) AS bounds)"
        ),
    )
    .await
    .unwrap();
    let radius = scalar_f64(
        &ctx,
        &format!(
            "SELECT bounds.radius FROM (SELECT ST_MinimumBoundingRadius(ST_GeomFromText('{geometry}')) AS bounds)"
        ),
    )
    .await
    .unwrap();

    assert_eq!((center_x, center_y), (5.0, 5.0));
    assert_eq!(radius, 7.0710678118654755);

    let npoints = scalar_f64(
        &ctx,
        &format!(
            "SELECT CAST(ST_NPoints(ST_MinimumBoundingCircle(ST_GeomFromText('{geometry}'))) AS DOUBLE)"
        ),
    )
    .await
    .unwrap();
    let area = scalar_f64(
        &ctx,
        &format!("SELECT ST_Area(ST_MinimumBoundingCircle(ST_GeomFromText('{geometry}')))"),
    )
    .await
    .unwrap();
    assert_eq!(npoints, f64::from(48 * 4 + 1));
    assert!(
        (area - std::f64::consts::PI * radius * radius).abs()
            / (std::f64::consts::PI * radius * radius)
            < 1e-4,
        "minimum bounding circle area is not an accurate circle approximation: {area}"
    );
}

#[tokio::test]
async fn largest_empty_circle_verifies_membership_and_witness() {
    let ctx = context();
    let geometry = "POLYGON((0 0,10 0,10 10,0 10,0 0))";
    let batch = first_batch(
        &ctx,
        &format!(
            "SELECT circle.radius = ST_Distance(circle.center, circle.nearest), \
                    ST_Within(circle.center, ST_GeomFromText('{geometry}')), \
                    ST_Intersects(circle.nearest, ST_GeomFromText('{geometry}')), \
                    circle.radius, \
                    ST_Distance(circle.center, ST_ExteriorRing(ST_GeomFromText('{geometry}'))) \
             FROM (SELECT ST_LargestEmptyCircle(ST_GeomFromText('{geometry}'), 0.1) AS circle)"
        ),
    )
    .await;
    let radius = batch
        .column(3)
        .as_primitive::<arrow_array::types::Float64Type>()
        .value(0);
    let boundary_distance = batch
        .column(4)
        .as_primitive::<arrow_array::types::Float64Type>()
        .value(0);
    assert_eq!(
        (0..3)
            .map(|index| batch.column(index).as_boolean().value(0))
            .collect::<Vec<_>>(),
        vec![true, true, true],
        "radius={radius}, boundary_distance={boundary_distance}"
    );
    assert!(
        (radius - boundary_distance).abs() <= 1e-12,
        "radius={radius}, boundary_distance={boundary_distance}"
    );
}

#[tokio::test]
async fn non_unique_circle_members_are_verified_without_scan_order() {
    let ctx = context();
    let geometry = "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((20 0,30 0,30 10,20 10,20 0)))";
    let query = format!(
        "SELECT ST_Within(circle.center, ST_GeomFromText('{geometry}')) \
                AND ST_Intersects(circle.nearest, ST_GeomFromText('{geometry}')) \
                AND circle.radius = ST_Distance(circle.center, circle.nearest) \
         FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText('{geometry}')) AS circle)"
    );
    assert!(scalar_bool(&ctx, &query).await);
}

#[tokio::test]
async fn maximum_inscribed_circle_respects_interior_ring_obstacles() {
    let ctx = context();
    let geometry = "POLYGON((0 0,100 0,100 100,0 100,0 0),(40 40,60 40,60 60,40 60,40 40))";
    let query = format!(
        "SELECT ST_Within(circle.center, ST_GeomFromText('{geometry}')), \
                ST_Intersects(circle.nearest, ST_GeomFromText('{geometry}')), \
                circle.radius = ST_Distance(circle.center, circle.nearest), \
                ST_Distance(circle.center, ST_GeomFromText('LINESTRING(0 0,100 0,100 100,0 100,0 0)')) >= circle.radius - 1e-9, \
                ST_Distance(circle.center, ST_GeomFromText('LINESTRING(40 40,60 40,60 60,40 60,40 40)')) >= circle.radius - 1e-9 \
         FROM (SELECT ST_MaximumInscribedCircle(ST_GeomFromText('{geometry}')) AS circle)"
    );
    let batch = first_batch(&ctx, &query).await;
    assert_eq!(
        (0..5)
            .map(|index| batch.column(index).as_boolean().value(0))
            .collect::<Vec<_>>(),
        vec![true, true, true, true, true]
    );
}

#[tokio::test]
async fn empty_geometries_match_postgis_empty_outputs() {
    let ctx = context();
    for function in [
        "ST_MaximumInscribedCircle(ST_GeomFromText('POLYGON EMPTY'))",
        "ST_MinimumBoundingRadius(ST_GeomFromText('POLYGON EMPTY'))",
    ] {
        let batch = first_batch(
            &ctx,
            &format!(
                "SELECT ST_AsText(value.center), value.radius FROM (SELECT {function} AS value)"
            ),
        )
        .await;
        assert_eq!(batch.column(0).as_string::<i32>().value(0), "POINT EMPTY");
        assert_eq!(
            batch
                .column(1)
                .as_primitive::<arrow_array::types::Float64Type>()
                .value(0),
            0.0
        );
    }
    assert!(scalar_bool(
        &ctx,
        "SELECT ST_AsText(ST_MinimumBoundingCircle(ST_GeomFromText('POLYGON EMPTY'))) = 'POINT EMPTY'",
    )
    .await);
}

#[tokio::test]
async fn null_arguments_are_strict_under_non_nullable_fields() {
    let ctx = context();
    let polygon = polygon![(x: 0., y: 0.), (x: 10., y: 0.), (x: 10., y: 10.), (x: 0., y: 10.)];
    let typ = geoarrow_schema::PolygonType::new(Dimension::XY, metadata())
        .with_coord_type(CoordType::Separated);
    let array = PolygonBuilder::from_polygons(&[polygon], typ).finish();
    let field = array.data_type().to_field("geom", false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Arc::new(field)])),
        vec![array.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("non_nullable_input", batch).unwrap();

    for query in [
        "SELECT ST_MaximumInscribedCircle(geom) IS NULL FROM non_nullable_input",
        "SELECT ST_MinimumBoundingRadius(geom) IS NULL FROM non_nullable_input",
        "SELECT ST_MinimumBoundingCircle(geom) IS NULL FROM non_nullable_input",
        "SELECT ST_LargestEmptyCircle(geom) IS NULL FROM non_nullable_input",
    ] {
        assert!(!scalar_bool(&ctx, query).await, "{query}");
    }

    for query in [
        "SELECT ST_MaximumInscribedCircle(NULL) IS NULL",
        "SELECT ST_MinimumBoundingRadius(NULL) IS NULL",
        "SELECT ST_MinimumBoundingCircle(NULL) IS NULL",
        "SELECT ST_LargestEmptyCircle(NULL) IS NULL",
        "SELECT ST_LargestEmptyCircle(ST_Point(0, 0), NULL) IS NULL",
        "SELECT ST_LargestEmptyCircle(ST_Point(0, 0), 1, NULL) IS NULL",
        "SELECT ST_MinimumBoundingCircle(ST_Point(0, 0), NULL) IS NULL",
    ] {
        assert!(scalar_bool(&ctx, query).await, "{query}");
    }
}

#[tokio::test]
async fn four_dimensional_coord_types_slices_and_crs_keep_xy_output() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let ctx = context();
        let array = polygon_test::array(coord_type, Dimension::XYZM)
            .with_metadata(metadata())
            .slice(0, 2);
        let field = array.data_type().to_field("geom", true);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Arc::new(field)])),
            vec![array.into_array_ref()],
        )
        .unwrap();
        ctx.register_batch("four_dimensional_input", batch).unwrap();

        let batch = first_batch(
            &ctx,
            "SELECT ST_MaximumInscribedCircle(geom) AS circle, \
                    ST_MinimumBoundingRadius(geom) AS bounds, \
                    ST_MinimumBoundingCircle(geom) AS mbc \
             FROM four_dimensional_input",
        )
        .await;
        assert_eq!(batch.num_rows(), 2);

        let schema = batch.schema();
        let DataType::Struct(circle_fields) = schema.field(0).data_type() else {
            panic!("MIC must return a struct");
        };
        match GeoArrowType::from_arrow_field(circle_fields[0].as_ref()).unwrap() {
            GeoArrowType::Point(point_type) => {
                assert_eq!(point_type.dimension(), Dimension::XY);
                assert_eq!(
                    point_type.metadata().crs(),
                    &Crs::from_authority_code(CRS_AUTHORITY.to_string())
                );
            }
            other => panic!("MIC center must be a Point, got {other:?}"),
        }

        let DataType::Struct(bound_fields) = schema.field(1).data_type() else {
            panic!("minimum bounding radius must return a struct");
        };
        match GeoArrowType::from_arrow_field(bound_fields[0].as_ref()).unwrap() {
            GeoArrowType::Point(point_type) => {
                assert_eq!(point_type.dimension(), Dimension::XY);
                assert_eq!(
                    point_type.metadata().crs(),
                    &Crs::from_authority_code(CRS_AUTHORITY.to_string())
                );
            }
            other => panic!("minimum bounding radius center must be a Point, got {other:?}"),
        }

        match GeoArrowType::from_arrow_field(schema.field(2)).unwrap() {
            GeoArrowType::Geometry(geometry_type) => {
                assert_eq!(
                    geometry_type.metadata().crs(),
                    &Crs::from_authority_code(CRS_AUTHORITY.to_string())
                );
                assert_eq!(geometry_type.coord_type(), coord_type);
            }
            other => panic!("minimum bounding circle must return generic Geometry, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn minimum_bounding_circle_segments_and_invalid_values() {
    let ctx = context();
    let npoints = scalar_f64(
        &ctx,
        "SELECT CAST(ST_NPoints(ST_MinimumBoundingCircle(
             ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'), 1
         )) AS DOUBLE)",
    )
    .await;
    assert_eq!(npoints, Some(5.0));

    let point = first_batch(
        &ctx,
        "SELECT ST_AsText(ST_MinimumBoundingCircle(ST_Point(1, 2), 0))",
    )
    .await;
    assert_eq!(point.column(0).as_string::<i32>().value(0), "POINT(1 2)");

    let error = collect(
        &ctx,
        "SELECT ST_MinimumBoundingCircle(
             ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'), 0
         )",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("segments per quarter-circle"), "{error}");

    let error = collect(
        &ctx,
        "SELECT ST_MinimumBoundingCircle(
             ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'), -1
         )",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("segments per quarter-circle"), "{error}");
}
