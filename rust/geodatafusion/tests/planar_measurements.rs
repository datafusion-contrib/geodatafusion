//! Public contracts for the planar measurement UDFs.
//!
//! The geometry and numeric expectations in this file come from the pinned
//! PostGIS reference environment and the published PostGIS examples.

#![cfg(feature = "geos-3_11")]

mod support;

use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geo_traits::{
    CoordTrait, Dimensions, GeometryTrait, GeometryType, LineStringTrait, PointTrait,
};
use geoarrow_array::array::GeometryArray;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Crs, Dimension, GeometryType as GeoArrowGeometryType, Metadata};
use geos::{CoordSeq, CoordType as GeosCoordType, Geom, Geometry};
use support::context::context;
use support::geoarrow::collect;
use wkt::Wkt;

fn parse_wkt(value: &str) -> Wkt<f64> {
    Wkt::from_str(value).unwrap()
}

fn geometry_builder(coord_type: CoordType, metadata: Arc<Metadata>) -> GeometryBuilder {
    GeometryBuilder::new(GeoArrowGeometryType::new(metadata).with_coord_type(coord_type))
}

fn register_geometry_batch(context: &SessionContext, name: &str, columns: Vec<(Field, ArrayRef)>) {
    let fields = columns
        .iter()
        .map(|(field, _)| field.clone())
        .collect::<Vec<_>>();
    let arrays = columns
        .into_iter()
        .map(|(_, array)| array)
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    context.register_batch(name, batch).unwrap();
}

async fn text_value(context: &SessionContext, sql: &str) -> Option<String> {
    let batches = collect(context, sql).await.unwrap();
    let array = batches[0].column(0).as_string::<i32>();
    (!array.is_null(0)).then(|| array.value(0).to_string())
}

async fn float_values(context: &SessionContext, sql: &str) -> Vec<Option<f64>> {
    let batches = collect(context, sql).await.unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch.column(0).as_primitive::<Float64Type>();
            (0..values.len())
                .map(|row| (!values.is_null(row)).then(|| values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn geos_version_at_least(major: u32, minor: u32) -> bool {
    let version = geos::version().expect("GEOS version must be available");
    let mut parts = version
        .split(|character: char| !character.is_ascii_digit())
        .filter(|part| !part.is_empty());
    let actual_major = parts
        .next()
        .expect("GEOS version must start with a major number")
        .parse::<u32>()
        .expect("GEOS major version must be numeric");
    let actual_minor = parts
        .next()
        .expect("GEOS version must contain a minor number")
        .parse::<u32>()
        .expect("GEOS minor version must be numeric");
    (actual_major, actual_minor) >= (major, minor)
}

fn geoarrow_dimensions(dimensions: geos::CoordDimensions) -> Dimensions {
    let dimensions: i32 = dimensions.into();
    match dimensions {
        2 => Dimensions::Xy,
        3 => Dimensions::Xyz,
        4 => Dimensions::Xyzm,
        other => panic!("unexpected GEOS coordinate dimension: {other}"),
    }
}

async fn query_error(context: &SessionContext, sql: &str) -> String {
    context
        .sql(sql)
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("query must fail")
        .to_string()
}

#[tokio::test]
async fn geometry_measurements_match_reference_examples() {
    let context = context();
    let cases = [
        (
            "ST_ClosestPoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), ST_GeomFromText('POINT(3 4)'))",
            "POINT(3 0)",
        ),
        (
            "ST_ShortestLine(ST_GeomFromText('LINESTRING(0 0,10 0)'), ST_GeomFromText('POINT(3 4)'))",
            "LINESTRING(3 0,3 4)",
        ),
        (
            "ST_ClosestPoint(ST_GeomFromText('LINESTRING(0 0,10 10)'), ST_GeomFromText('LINESTRING(0 10,10 0)'))",
            "POINT(5 5)",
        ),
        (
            "ST_ShortestLine(ST_GeomFromText('LINESTRING(0 0,10 10)'), ST_GeomFromText('LINESTRING(0 10,10 0)'))",
            "LINESTRING(5 5,5 5)",
        ),
        (
            "ST_ClosestPoint(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'), ST_GeomFromText('POINT(5 5)'))",
            "POINT(5 5)",
        ),
        (
            "ST_MinimumClearanceLine(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'))",
            "LINESTRING(0 0,1 1)",
        ),
        (
            "ST_MinimumClearanceLine(ST_GeomFromText('POINT(1 1)'))",
            "LINESTRING EMPTY",
        ),
        (
            "ST_MinimumClearanceLine(ST_GeomFromText('POINT Z (1 2 3)'))",
            "LINESTRING EMPTY",
        ),
        (
            "ST_MinimumClearanceLine(ST_GeomFromText('GEOMETRYCOLLECTION Z EMPTY'))",
            "LINESTRING EMPTY",
        ),
        (
            "ST_MinimumClearanceLine(ST_GeomFromText('LINESTRING Z (0 0 1,10 0 2)'))",
            "LINESTRING(0 0,10 0)",
        ),
        (
            "ST_ClosestPoint(ST_GeomFromText('LINESTRING Z (0 0 1,10 0 2)'), ST_GeomFromText('POINT Z (3 4 9)'))",
            "POINT(3 0)",
        ),
        (
            "ST_LongestLine(ST_GeomFromText('LINESTRING Z (0 0 1,10 0 2)'), ST_GeomFromText('POINT Z (3 4 9)'))",
            "LINESTRING(10 0,3 4)",
        ),
        (
            "ST_ShortestLine(\
                ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))'),\
                ST_GeomFromText('POINT(3 4)')\
             )",
            "LINESTRING(1 2,3 4)",
        ),
        (
            "ST_ClosestPoint(\
                ST_GeomFromText('GEOMETRYCOLLECTION(POINT EMPTY,POINT(1 2))'),\
                ST_GeomFromText('POINT(3 4)')\
             )",
            "POINT(1 2)",
        ),
    ];

    for (expression, expected) in cases {
        let actual = text_value(&context, &format!("SELECT ST_AsText({expression})"))
            .await
            .expect("geometry result must not be NULL");
        assert_eq!(parse_wkt(&actual), parse_wkt(expected), "{expression}");
    }
}

#[tokio::test]
async fn float_measurements_match_reference_examples() {
    let context = context();
    let batches = collect(
        &context,
        "SELECT ST_MaxDistance(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'), ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')) AS square, \
                ST_MaxDistance(ST_GeomFromText('LINESTRING(0 0,5 0)'), ST_GeomFromText('POINT(0 10)')) AS line_point, \
                ST_HausdorffDistance(ST_GeomFromText('LINESTRING (130 0, 0 0, 0 150)'), ST_GeomFromText('LINESTRING (10 10, 10 150, 130 10)')) AS hausdorff, \
                ST_HausdorffDistance(ST_GeomFromText('LINESTRING (130 0, 0 0, 0 150)'), ST_GeomFromText('LINESTRING (10 10, 10 150, 130 10)'), 0.5) AS hausdorff_densified, \
                ST_FrechetDistance(ST_GeomFromText('LINESTRING (0 0, 100 0)'), ST_GeomFromText('LINESTRING (0 0, 50 50, 100 0)')) AS frechet, \
                ST_FrechetDistance(ST_GeomFromText('LINESTRING (0 0, 100 0)'), ST_GeomFromText('LINESTRING (0 0, 50 50, 100 0)'), 0.5) AS frechet_densified, \
                ST_MinimumClearance(ST_GeomFromText('POINT(1 1)')) AS clearance_point, \
                ST_MinimumClearance(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)')) AS clearance_line, \
                ST_MinimumClearance(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))')) AS clearance_polygon, \
                ST_MinimumClearance(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')) AS clearance_empty",
    )
    .await
    .unwrap();
    let batch = &batches[0];
    let row = |name: &str, expected: f64, tolerance: f64| {
        let column = batch.schema().index_of(name).unwrap();
        let value = batch.column(column).as_primitive::<Float64Type>().value(0);
        assert!(
            (value - expected).abs() <= tolerance,
            "{name}: {value} != {expected}"
        );
    };
    row("square", 2.0_f64.sqrt(), f64::EPSILON);
    row("line_point", (125.0_f64).sqrt(), f64::EPSILON);
    row("hausdorff", 14.142135623730951, 1e-12);
    row("hausdorff_densified", 70.0, 1e-12);
    row("frechet", 70.71067811865476, 1e-12);
    row("frechet_densified", 50.0, 1e-12);
    assert!(
        batch
            .column(batch.schema().index_of("clearance_point").unwrap())
            .as_primitive::<Float64Type>()
            .value(0)
            .is_infinite()
    );
    row("clearance_line", 2.0_f64.sqrt(), f64::EPSILON);
    row("clearance_polygon", 4.0, 0.0);
    assert!(
        batch
            .column(batch.schema().index_of("clearance_empty").unwrap())
            .as_primitive::<Float64Type>()
            .value(0)
            .is_infinite()
    );
}

#[tokio::test]
async fn nearest_results_are_valid_minimum_witnesses_when_ties_exist() {
    let context = context();
    // The reference distances are fixed by the inputs, not by the UDFs under
    // test. The endpoint membership and length checks below use raw Arrow
    // output and independent GEOS predicates.
    let cases = [
        ("POINT(0 0)", "MULTIPOINT((1 0),(0 1))", 1.0),
        (
            "POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,6 4,6 6,4 6,4 4))",
            "LINESTRING(2 2,8 8)",
            0.0,
        ),
        (
            "MULTILINESTRING((-1 5,-6 5),(5 5,-4 6))",
            "POLYGON((-3 -5,3 -5,3 -1,-3 -1,-3 -5))",
            6.0,
        ),
    ];

    for (left_wkt, right_wkt, reference_distance) in cases {
        let sql = format!(
            "SELECT ST_ClosestPoint(\
                ST_GeomFromText('{left_wkt}'), ST_GeomFromText('{right_wkt}')\
             ) AS closest, \
             ST_ShortestLine(\
                ST_GeomFromText('{left_wkt}'), ST_GeomFromText('{right_wkt}')\
             ) AS shortest"
        );
        let batches = collect(&context, &sql).await.unwrap();
        let batch = &batches[0];

        let closest_field = batch.schema().field(0).clone();
        let closest = GeometryArray::try_from((batch.column(0).as_ref(), &closest_field)).unwrap();
        let shortest_field = batch.schema().field(1).clone();
        let shortest =
            GeometryArray::try_from((batch.column(1).as_ref(), &shortest_field)).unwrap();

        let closest = closest.value(0).unwrap();
        let GeometryType::Point(closest_point) = closest.as_type() else {
            panic!("ST_ClosestPoint must return a Point for {left_wkt}");
        };
        let closest_coord = closest_point.coord().unwrap();

        let shortest = shortest.value(0).unwrap();
        let GeometryType::LineString(shortest_line) = shortest.as_type() else {
            panic!("ST_ShortestLine must return a LineString for {left_wkt}");
        };
        let mut coordinates = shortest_line.coords();
        let start = coordinates.next().unwrap();
        let end = coordinates.next().unwrap();
        assert!(
            coordinates.next().is_none(),
            "ST_ShortestLine must have exactly two points"
        );

        assert_eq!(
            (start.x(), start.y()),
            (closest_coord.x(), closest_coord.y()),
            "ST_ClosestPoint must be the start of ST_ShortestLine"
        );

        let left = Geometry::new_from_wkt(left_wkt).unwrap();
        let right = Geometry::new_from_wkt(right_wkt).unwrap();
        assert!(
            left.intersects(&geos_point(start.x(), start.y())).unwrap(),
            "shortest-line start must lie on the left geometry"
        );
        assert!(
            right.intersects(&geos_point(end.x(), end.y())).unwrap(),
            "shortest-line end must lie on the right geometry"
        );

        let length = ((end.x() - start.x()).powi(2) + (end.y() - start.y()).powi(2)).sqrt();
        assert!(
            (length - reference_distance).abs() <= 1e-12,
            "shortest-line length {length} differs from reference {reference_distance}"
        );
    }
}

#[tokio::test]
async fn longest_results_are_valid_maximum_witnesses_when_ties_exist() {
    let context = context();
    let cases = [
        ("POINT(0 0)", "MULTIPOINT((1 0),(0 1),(-1 0),(0 -1))", 1.0),
        (
            "POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,6 4,6 6,4 6,4 4))",
            "LINESTRING(2 2,8 8)",
            128.0_f64.sqrt(),
        ),
    ];

    for (left_wkt, right_wkt, reference_distance) in cases {
        let sql = format!(
            "SELECT ST_LongestLine(\
                ST_GeomFromText('{left_wkt}'), ST_GeomFromText('{right_wkt}')\
             )"
        );
        let batches = collect(&context, &sql).await.unwrap();
        let batch = &batches[0];
        let field = batch.schema().field(0).clone();
        let longest = GeometryArray::try_from((batch.column(0).as_ref(), &field)).unwrap();
        let longest = longest.value(0).unwrap();
        let GeometryType::LineString(longest_line) = longest.as_type() else {
            panic!("ST_LongestLine must return a LineString for {left_wkt}");
        };
        let mut coordinates = longest_line.coords();
        let start = coordinates.next().unwrap();
        let end = coordinates.next().unwrap();
        assert!(
            coordinates.next().is_none(),
            "ST_LongestLine must have exactly two points"
        );

        let left = Geometry::new_from_wkt(left_wkt).unwrap();
        let right = Geometry::new_from_wkt(right_wkt).unwrap();
        assert!(
            left.intersects(&geos_point(start.x(), start.y())).unwrap(),
            "longest-line start must lie on the left geometry"
        );
        assert!(
            right.intersects(&geos_point(end.x(), end.y())).unwrap(),
            "longest-line end must lie on the right geometry"
        );

        let length = ((end.x() - start.x()).powi(2) + (end.y() - start.y()).powi(2)).sqrt();
        assert!(
            (length - reference_distance).abs() <= 1e-12,
            "longest-line length {length} differs from reference {reference_distance}"
        );
    }
}

fn geos_point(x: f64, y: f64) -> Geometry {
    let mut coordinates = CoordSeq::new(1, GeosCoordType::XY).unwrap();
    coordinates.set_x(0, x).unwrap();
    coordinates.set_y(0, y).unwrap();
    Geometry::create_point(coordinates).unwrap()
}

#[tokio::test]
async fn empty_and_null_inputs_follow_postgis_contract() {
    let context = context();
    let empty_left = "ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')";
    let empty_right = "ST_GeomFromText('POINT EMPTY')";

    for function in [
        "ST_ClosestPoint",
        "ST_ShortestLine",
        "ST_LongestLine",
        "ST_MaxDistance",
        "ST_HausdorffDistance",
        "ST_FrechetDistance",
    ] {
        for (left, right) in [
            (empty_left, "ST_GeomFromText('POINT(0 0)')"),
            ("ST_GeomFromText('POINT(0 0)')", empty_right),
            (empty_left, empty_right),
        ] {
            let sql = format!("SELECT {function}({left}, {right}) IS NULL");
            let batches = collect(&context, &sql).await.unwrap();
            assert!(
                batches[0].column(0).as_boolean().value(0),
                "{function} must return NULL for empty inputs: {left}, {right}"
            );
        }

        for (left, right) in [
            ("ST_GeomFromText(NULL)", "ST_GeomFromText('POINT(0 0)')"),
            ("ST_GeomFromText('POINT(0 0)')", "NULL"),
        ] {
            let sql = format!("SELECT {function}({left}, {right}) IS NULL");
            let batches = collect(&context, &sql).await.unwrap();
            assert!(
                batches[0].column(0).as_boolean().value(0),
                "{function} must return NULL for NULL inputs: {left}, {right}"
            );
        }
    }

    let batches = collect(
        &context,
        "SELECT ST_MinimumClearance(ST_GeomFromText('POINT EMPTY')) IS NOT NULL, \
                ST_MinimumClearance(ST_GeomFromText('POINT EMPTY')) = CAST('Infinity' AS DOUBLE), \
                ST_AsText(ST_MinimumClearanceLine(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')))",
    )
    .await
    .unwrap();
    assert!(batches[0].column(0).as_boolean().value(0));
    assert!(batches[0].column(1).as_boolean().value(0));
    assert_eq!(
        batches[0].column(2).as_string::<i32>().value(0),
        "LINESTRING EMPTY"
    );

    let batches = collect(
        &context,
        "SELECT ST_MinimumClearanceLine(ST_GeomFromText('POINT Z (1 2 3)'))",
    )
    .await
    .unwrap();
    let field = batches[0].schema().field(0).clone();
    let array = GeometryArray::try_from((batches[0].column(0).as_ref(), &field)).unwrap();
    assert!(
        array.has_only_dimension(Dimension::XY),
        "empty clearance lines must be normalized to 2D"
    );
    let value = array.value(0).unwrap();
    let GeometryType::LineString(line) = value.as_type() else {
        panic!("ST_MinimumClearanceLine must return a LineString");
    };
    assert_eq!(line.num_coords(), 0);
    assert_eq!(
        text_value(
            &context,
            "SELECT ST_AsText(ST_MinimumClearanceLine(ST_GeomFromText('POINT Z (1 2 3)')))",
        )
        .await
        .unwrap(),
        "LINESTRING EMPTY"
    );
}

#[tokio::test]
async fn densify_overloads_and_invalid_values_are_explicit() {
    let context = context();
    let sqrt_two = 2.0_f64.sqrt();
    let non_densified_frechet = 70.71067811865476;

    for (sql, expected) in [
        (
            "SELECT ST_HausdorffDistance(\
                ST_GeomFromText('LINESTRING (130 0, 0 0, 0 150)'),\
                ST_GeomFromText('LINESTRING (10 10, 10 150, 130 10)'),\
                0.5\
             )",
            70.0,
        ),
        (
            "SELECT ST_FrechetDistance(\
                ST_GeomFromText('LINESTRING (0 0, 100 0)'),\
                ST_GeomFromText('LINESTRING (0 0, 50 50, 100 0)'),\
                0.5\
             )",
            50.0,
        ),
        (
            "SELECT ST_FrechetDistance(\
                ST_GeomFromText('LINESTRING (0 0, 100 0)'),\
                ST_GeomFromText('LINESTRING (0 0, 50 50, 100 0)'),\
                0\
             )",
            non_densified_frechet,
        ),
        (
            "SELECT ST_FrechetDistance(\
                ST_GeomFromText('LINESTRING (0 0, 100 0)'),\
                ST_GeomFromText('LINESTRING (0 0, 50 50, 100 0)'),\
                -0.1\
             )",
            non_densified_frechet,
        ),
        (
            "SELECT ST_HausdorffDistance(\
                ST_GeomFromText('POINT (0 0)'), ST_GeomFromText('POINT (1 1)'), 0.0001\
             )",
            sqrt_two,
        ),
        (
            "SELECT ST_FrechetDistance(\
                ST_GeomFromText('POINT (0 0)'), ST_GeomFromText('POINT (1 1)'), 0.0011\
             )",
            sqrt_two,
        ),
    ] {
        let values = float_values(&context, sql).await;
        assert!(
            (values[0].unwrap() - expected).abs() < 1e-12,
            "{sql}: {:?}",
            values[0]
        );
    }

    // GEOS 3.14 added a CPU-protection lower bound to the Frechet densify
    // fraction. Do not make older GEOS versions emulate that newer error.
    for fraction in ["0.001", "0.0001"] {
        let sql = format!(
            "SELECT ST_FrechetDistance(\
                ST_GeomFromText('POINT (0 0)'), ST_GeomFromText('POINT (1 1)'), {fraction}\
             )"
        );
        if geos_version_at_least(3, 14) {
            let error = query_error(&context, &sql).await;
            assert!(
                error.contains("Fraction is not in range") && error.contains("0.001 - 1.0]"),
                "{sql}: {error}"
            );
        } else {
            let values = float_values(&context, &sql).await;
            assert!(
                (values[0].unwrap() - sqrt_two).abs() < 1e-12,
                "{sql}: {:?}",
                values[0]
            );
        }
    }

    for sql in [
        "SELECT ST_HausdorffDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), 0)",
        "SELECT ST_HausdorffDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), -0.1)",
        "SELECT ST_HausdorffDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), 1.5)",
        "SELECT ST_FrechetDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), 1.5)",
        "SELECT ST_FrechetDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), CAST('NaN' AS DOUBLE))",
        "SELECT ST_FrechetDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), CAST('Infinity' AS DOUBLE))",
        "SELECT ST_HausdorffDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), CAST('NaN' AS DOUBLE))",
        "SELECT ST_HausdorffDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), CAST('Infinity' AS DOUBLE))",
    ] {
        let error = query_error(&context, sql).await;
        assert!(error.contains("Fraction is not in range"), "{sql}: {error}");
    }

    for sql in [
        "SELECT ST_HausdorffDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), CAST(NULL AS DOUBLE)) IS NULL",
        "SELECT ST_FrechetDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), CAST(NULL AS DOUBLE)) IS NULL",
    ] {
        let batches = collect(&context, sql).await.unwrap();
        assert!(batches[0].column(0).as_boolean().value(0), "{sql}");
    }
}

#[tokio::test]
async fn z_ordinates_are_ignored_and_m_is_dropped() {
    let context = context();
    for (expression, expected) in [
        ("ST_ClosestPoint", "POINT(3 0)"),
        ("ST_ShortestLine", "LINESTRING(3 0,3 4)"),
        ("ST_LongestLine", "LINESTRING(10 0,3 4)"),
    ] {
        let actual = text_value(
            &context,
            &format!(
                "SELECT ST_AsText({expression}(\
                    ST_GeomFromText('LINESTRING M (0 0 1,10 0 2)'),\
                    ST_GeomFromText('POINT M (3 4 9)')\
                 ))"
            ),
        )
        .await
        .unwrap();
        assert_eq!(parse_wkt(&actual), parse_wkt(expected));

        let actual = text_value(
            &context,
            &format!(
                "SELECT ST_AsText({expression}(\
                    ST_GeomFromText('LINESTRING ZM (0 0 1 1,10 0 2 2)'),\
                    ST_GeomFromText('POINT ZM (3 4 9 9)')\
                 ))"
            ),
        )
        .await
        .unwrap();
        assert_eq!(parse_wkt(&actual), parse_wkt(expected));
    }

    let batches = collect(
        &context,
        "SELECT ST_MaxDistance(\
                ST_GeomFromText('LINESTRING M (0 0 1,10 0 2)'), ST_GeomFromText('POINT M (3 4 9)')\
             ), \
             ST_MaxDistance(\
                ST_GeomFromText('LINESTRING ZM (0 0 1 1,10 0 2 2)'), ST_GeomFromText('POINT ZM (3 4 9 9)')\
             ), \
             ST_HausdorffDistance(\
                ST_GeomFromText('LINESTRING M (0 0 1,10 0 2)'), ST_GeomFromText('LINESTRING M (0 1 3,10 1 4)')\
             ), \
             ST_FrechetDistance(\
                ST_GeomFromText('LINESTRING ZM (0 0 1 1,10 0 2 2)'), ST_GeomFromText('LINESTRING ZM (0 1 3 3,10 1 4 4)')\
             ), \
             ST_MinimumClearance(\
                ST_GeomFromText('LINESTRING M (0 0 1,1 1 2,2 2 3)')\
             ), \
             ST_MinimumClearance(\
                ST_GeomFromText('LINESTRING ZM (0 0 1 1,1 1 2 2,2 2 3 3)')\
             ), \
             ST_AsText(ST_MinimumClearanceLine(\
                ST_GeomFromText('LINESTRING M (0 0 1,1 1 2,2 2 3)')\
             )), \
             ST_AsText(ST_MinimumClearanceLine(\
                ST_GeomFromText('LINESTRING ZM (0 0 1 1,1 1 2 2,2 2 3 3)')\
             ))",
    )
    .await
    .unwrap();
    let batch = &batches[0];
    for (column, expected) in [
        (0, 8.06225774829855),
        (1, 8.06225774829855),
        (2, 1.0),
        (3, 1.0),
        (4, 2.0_f64.sqrt()),
        (5, 2.0_f64.sqrt()),
    ] {
        let value = batch.column(column).as_primitive::<Float64Type>().value(0);
        assert!(
            (value - expected).abs() <= 1e-12,
            "column {column}: {value}"
        );
    }
    assert_eq!(
        batch.column(6).as_string::<i32>().value(0),
        "LINESTRING(0 0,1 1)"
    );
    assert_eq!(
        batch.column(7).as_string::<i32>().value(0),
        "LINESTRING(0 0,1 1)"
    );
}

#[tokio::test]
async fn bare_null_resolves_and_nullability_survives_optimization() {
    let context = context();
    let batches = collect(
        &context,
        "SELECT ST_ClosestPoint(NULL, ST_GeomFromText('POINT(0 0)')) IS NULL, \
                ST_ShortestLine(ST_GeomFromText('POINT(0 0)'), NULL) IS NULL, \
                ST_LongestLine(NULL, ST_GeomFromText('POINT(0 0)')) IS NULL, \
                ST_MaxDistance(NULL, ST_GeomFromText('POINT(0 0)')) IS NULL, \
                ST_HausdorffDistance(NULL, ST_GeomFromText('POINT(0 0)')) IS NULL, \
                ST_FrechetDistance(NULL, ST_GeomFromText('POINT(0 0)')) IS NULL, \
                ST_MinimumClearance(NULL) IS NULL, \
                ST_MinimumClearanceLine(NULL) IS NULL, \
                ST_HausdorffDistance(\
                    ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), NULL\
                ) IS NULL, \
                ST_FrechetDistance(\
                    ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), NULL\
                ) IS NULL",
    )
    .await
    .unwrap();
    for column in 0..10 {
        assert!(
            batches[0].column(column).as_boolean().value(0),
            "column {column} must be NULL"
        );
    }

    let geometry_type =
        GeoArrowGeometryType::new(Default::default()).with_coord_type(CoordType::Separated);
    let mut left = GeometryBuilder::new(geometry_type.clone());
    left.push_geometry(Some(&parse_wkt("POINT(0 0)"))).unwrap();
    left.push_geometry(Some(&parse_wkt("POINT(1 1)"))).unwrap();
    let mut right = GeometryBuilder::new(geometry_type);
    right.push_geometry(Some(&parse_wkt("POINT(3 4)"))).unwrap();
    right.push_geometry(Some(&parse_wkt("POINT(5 6)"))).unwrap();

    let geometry_field = |name: &str, nullable: bool| {
        geoarrow_schema::GeometryType::new(Default::default())
            .with_coord_type(CoordType::Separated)
            .to_field(name, nullable)
    };
    register_geometry_batch(
        &context,
        "nullable_parameters",
        vec![
            (geometry_field("left", false), left.finish().to_array_ref()),
            (
                geometry_field("right", false),
                right.finish().to_array_ref(),
            ),
            (
                Field::new("fraction", DataType::Float64, true),
                Arc::new(Float64Array::from(vec![Some(1.0), None])) as ArrayRef,
            ),
        ],
    );

    let batches = collect(
        &context,
        "SELECT ST_HausdorffDistance(left, right, fraction) IS NULL AS nullable_result, \
                ST_HausdorffDistance(left, right, fraction) \
         FROM nullable_parameters ORDER BY left",
    )
    .await
    .unwrap();
    let nullable = batches[0].column(0).as_boolean();
    assert!(!nullable.value(0));
    assert!(nullable.value(1));
    assert!(
        batches[0]
            .column(1)
            .as_primitive::<Float64Type>()
            .is_null(1)
    );

    let batches = collect(
        &context,
        "SELECT ST_ClosestPoint(left, right), ST_LongestLine(left, right) FROM nullable_parameters",
    )
    .await
    .unwrap();
    for column in 0..2 {
        let field = batches[0].schema().field(column).clone();
        assert!(field.is_nullable(), "column {column} must remain nullable");
    }
}

#[tokio::test]
async fn geometry_kinds_coord_types_and_crs_are_preserved() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        for dimension in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let (kinds, right_wkt): ([&str; 7], &str) = match dimension {
                Dimension::XY => (
                    [
                        "POINT(0 0)",
                        "LINESTRING(0 0,2 2)",
                        "POLYGON((0 0,2 0,2 2,0 2,0 0))",
                        "MULTIPOINT((0 0),(2 2))",
                        "MULTILINESTRING((0 0,1 1),(1 1,2 2))",
                        "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((1 1,2 1,2 2,1 2,1 1)))",
                        "GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 1,2 2))",
                    ],
                    "POINT(1 1)",
                ),
                Dimension::XYZ => (
                    [
                        "POINT Z (0 0 1)",
                        "LINESTRING Z (0 0 1,2 2 1)",
                        "POLYGON Z ((0 0 1,2 0 1,2 2 1,0 2 1,0 0 1))",
                        "MULTIPOINT Z ((0 0 1),(2 2 1))",
                        "MULTILINESTRING Z ((0 0 1,1 1 1),(1 1 1,2 2 1))",
                        "MULTIPOLYGON Z (((0 0 1,1 0 1,1 1 1,0 1 1,0 0 1)),((1 1 1,2 1 1,2 2 1,1 2 1,1 1 1)))",
                        "GEOMETRYCOLLECTION Z (POINT Z (0 0 1),LINESTRING Z (1 1 1,2 2 1))",
                    ],
                    "POINT Z (1 1 9)",
                ),
                Dimension::XYM => (
                    [
                        "POINT M (0 0 1)",
                        "LINESTRING M (0 0 1,2 2 1)",
                        "POLYGON M ((0 0 1,2 0 1,2 2 1,0 2 1,0 0 1))",
                        "MULTIPOINT M ((0 0 1),(2 2 1))",
                        "MULTILINESTRING M ((0 0 1,1 1 1),(1 1 1,2 2 1))",
                        "MULTIPOLYGON M (((0 0 1,1 0 1,1 1 1,0 1 1,0 0 1)),((1 1 1,2 1 1,2 2 1,1 2 1,1 1 1)))",
                        "GEOMETRYCOLLECTION M (POINT M (0 0 1),LINESTRING M (1 1 1,2 2 1))",
                    ],
                    "POINT M (1 1 9)",
                ),
                Dimension::XYZM => (
                    [
                        "POINT ZM (0 0 1 1)",
                        "LINESTRING ZM (0 0 1 1,2 2 1 1)",
                        "POLYGON ZM ((0 0 1 1,2 0 1 1,2 2 1 1,0 2 1 1,0 0 1 1))",
                        "MULTIPOINT ZM ((0 0 1 1),(2 2 1 1))",
                        "MULTILINESTRING ZM ((0 0 1 1,1 1 1 1),(1 1 1 1,2 2 1 1))",
                        "MULTIPOLYGON ZM (((0 0 1 1,1 0 1 1,1 1 1 1,0 1 1 1,0 0 1 1)),((1 1 1 1,2 1 1 1,2 2 1 1,1 2 1 1,1 1 1 1)))",
                        "GEOMETRYCOLLECTION ZM (POINT ZM (0 0 1 1),LINESTRING ZM (1 1 1 1,2 2 1 1))",
                    ],
                    "POINT ZM (1 1 9 9)",
                ),
            };
            let crs = Crs::from_authority_code("EPSG:4326".to_string());
            let metadata = Arc::new(Metadata::new(crs.clone(), None));
            let mut left = geometry_builder(coord_type, metadata.clone());
            let mut right = geometry_builder(coord_type, metadata.clone());
            for kind in kinds {
                left.push_geometry(Some(&parse_wkt(kind))).unwrap();
                right.push_geometry(Some(&parse_wkt(right_wkt))).unwrap();
            }

            let context = context();
            let left_field = geometry_builder(coord_type, metadata.clone())
                .finish()
                .data_type()
                .to_field("left", false);
            let right_field = geometry_builder(coord_type, metadata.clone())
                .finish()
                .data_type()
                .to_field("right", false);
            register_geometry_batch(
                &context,
                "kinds",
                vec![
                    (left_field, left.finish().to_array_ref()),
                    (right_field, right.finish().to_array_ref()),
                ],
            );

            let batches = collect(
                &context,
                "SELECT ST_ClosestPoint(left, right) AS closest, \
                        ST_ShortestLine(left, right) AS shortest, \
                        ST_LongestLine(left, right) AS longest, \
                        ST_MaxDistance(left, right) AS max_distance, \
                        ST_MinimumClearanceLine(left) AS clearance_line, \
                        ST_HausdorffDistance(left, right) AS hausdorff, \
                        ST_FrechetDistance(left, right) AS frechet, \
                        ST_MinimumClearance(left) AS clearance \
                 FROM kinds",
            )
            .await
            .unwrap();
            let batch = &batches[0];
            assert_eq!(batch.num_rows(), kinds.len());

            for (index, name) in [
                (0, "closest"),
                (1, "shortest"),
                (2, "longest"),
                (4, "clearance_line"),
            ] {
                let field = batch.schema().field(index).clone();
                let geometry_type = field.try_extension_type::<GeoArrowGeometryType>().unwrap();
                assert_eq!(geometry_type.coord_type(), coord_type, "{name}");
                assert_eq!(geometry_type.metadata().crs(), &crs, "{name}");
                assert!(field.is_nullable(), "{name}");

                let array =
                    GeometryArray::try_from((batch.column(index).as_ref(), &field)).unwrap();
                assert_eq!(array.len(), kinds.len());
                for row in 0..array.len() {
                    let value = array.value(row).expect("output row must be valid");
                    let output_dimension = value.dim();
                    assert!(
                        matches!(output_dimension, Dimensions::Xy | Dimensions::Xyz),
                        "{name} row {row} must not carry M, got {output_dimension:?}"
                    );
                    if name == "closest"
                        || name == "longest"
                        || (name == "clearance_line" && row == 0)
                    {
                        assert_eq!(
                            output_dimension,
                            Dimensions::Xy,
                            "{name} row {row} must be 2D"
                        );
                    }
                }
            }

            let values = batch.column(3).as_primitive::<Float64Type>();
            assert!((values.value(0) - 2.0_f64.sqrt()).abs() <= f64::EPSILON);

            for column in 5..8 {
                let values = batch.column(column).as_primitive::<Float64Type>();
                for row in 0..values.len() {
                    assert!(
                        !values.is_null(row),
                        "numeric measurement column {column}, row {row} is NULL"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn non_empty_geometry_dimensions_follow_geos() {
    let context = context();
    for (left_wkt, right_wkt) in [
        ("POINT Z (0 0 1)", "POINT Z (1 1 9)"),
        ("LINESTRING Z (0 0 1,10 0 2)", "POINT Z (3 4 9)"),
    ] {
        let sql = format!(
            "SELECT ST_ShortestLine(\
                ST_GeomFromText('{left_wkt}'), ST_GeomFromText('{right_wkt}')\
             )"
        );
        let batches = collect(&context, &sql).await.unwrap();
        let field = batches[0].schema().field(0).clone();
        let array = GeometryArray::try_from((batches[0].column(0).as_ref(), &field)).unwrap();
        let actual = array.value(0).unwrap().dim();

        let left = Geometry::new_from_wkt(left_wkt).unwrap();
        let right = Geometry::new_from_wkt(right_wkt).unwrap();
        let expected =
            geoarrow_dimensions(left.nearest_points(&right).unwrap().dimensions().unwrap());
        assert_eq!(actual, expected, "{sql}");
    }

    let wkt = "LINESTRING Z (0 0 1,10 0 2)";
    let sql = format!("SELECT ST_MinimumClearanceLine(ST_GeomFromText('{wkt}'))");
    let batches = collect(&context, &sql).await.unwrap();
    let field = batches[0].schema().field(0).clone();
    let array = GeometryArray::try_from((batches[0].column(0).as_ref(), &field)).unwrap();
    let actual = array.value(0).unwrap().dim();
    let expected = geoarrow_dimensions(
        Geometry::new_from_wkt(wkt)
            .unwrap()
            .minimum_clearance_line()
            .unwrap()
            .get_coordinate_dimension()
            .unwrap(),
    );
    assert_eq!(actual, expected, "{sql}");
}

#[tokio::test]
async fn sliced_multi_row_arrays_are_processed_with_offsets() {
    let coord_type = CoordType::Separated;
    let mut left = geometry_builder(coord_type, Default::default());
    let mut right = geometry_builder(coord_type, Default::default());
    for (left_wkt, right_wkt) in [
        ("POINT(0 0)", "POINT(0 1)"),
        ("POINT(3 4)", "POINT(0 0)"),
        ("POINT(10 0)", "POINT(0 10)"),
        ("POINT(6 8)", "POINT(0 0)"),
        ("POINT(1 1)", "POINT(2 2)"),
    ] {
        left.push_geometry(Some(&parse_wkt(left_wkt))).unwrap();
        right.push_geometry(Some(&parse_wkt(right_wkt))).unwrap();
    }

    let left_field = geometry_builder(coord_type, Default::default())
        .finish()
        .data_type()
        .to_field("left", false);
    let right_field = geometry_builder(coord_type, Default::default())
        .finish()
        .data_type()
        .to_field("right", false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![left_field, right_field])),
        vec![left.finish().to_array_ref(), right.finish().to_array_ref()],
    )
    .unwrap()
    .slice(1, 3);

    let context = context();
    context.register_batch("sliced", batch).unwrap();
    let batches = collect(
        &context,
        "SELECT ST_MaxDistance(left, right), \
                ST_AsText(ST_ClosestPoint(left, right)) \
         FROM sliced",
    )
    .await
    .unwrap();
    let distances = batches[0].column(0).as_primitive::<Float64Type>();
    assert_eq!(distances.len(), 3);
    assert_eq!(distances.value(0), 5.0);
    assert!((distances.value(1) - 200.0_f64.sqrt()).abs() <= f64::EPSILON);
    assert_eq!(distances.value(2), 10.0);
    let closest = batches[0].column(1).as_string::<i32>();
    assert_eq!(closest.value(0), "POINT(3 4)");
    assert_eq!(closest.value(1), "POINT(10 0)");
    assert_eq!(closest.value(2), "POINT(6 8)");
}
