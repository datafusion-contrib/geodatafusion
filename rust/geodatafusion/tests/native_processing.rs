//! Public SQL and raw Arrow contracts for native geometry processing.

mod support;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use geo_traits::{
    CoordTrait, Dimensions, GeometryTrait, MultiPointTrait, PointTrait, PolygonTrait,
};
use geoarrow_array::array::GeometryArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Crs, GeometryType, Metadata};
use support::context::context;
use support::geoarrow::collect;
use wkt::Wkt;

async fn query(context: &datafusion::prelude::SessionContext, sql: &str) -> Vec<RecordBatch> {
    collect(context, sql).await.unwrap()
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
    context: &datafusion::prelude::SessionContext,
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
async fn processing_preserves_crs_coord_type_dimensions_empty_and_slices() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let context = context();
        let metadata = metadata_4326();
        let geometry_type = GeometryType::new(metadata.clone()).with_coord_type(coord_type);
        let mut builder = geoarrow_array::builder::GeometryBuilder::new(geometry_type);
        for geometry in [
            "LINESTRING ZM (0 0 1 2,4 0 3 4,4 4 5 6)",
            "POLYGON ZM ((0 0 1 2,4 0 2 3,4 4 3 4,0 4 4 5,0 0 1 2))",
            "LINESTRING ZM EMPTY",
        ] {
            builder.push_geometry(Some(&parse_wkt(geometry))).unwrap();
        }
        builder.push_geometry(None::<&Wkt<f64>>).unwrap();
        let array = builder.finish();
        let field = register_geometry(&context, "t", Arc::new(array.clone()), true);
        assert_eq!(
            field
                .try_extension_type::<GeometryType>()
                .unwrap()
                .coord_type(),
            coord_type
        );

        let batches = query(
            &context,
            "SELECT ST_ChaikinSmoothing(geom, 1, false) AS chaikin, \
                    ST_FilterByM(geom, 2, 5, true) AS filtered, \
                    ST_SetEffectiveArea(geom, -1, 1) AS area \
             FROM t",
        )
        .await;
        let schema = batches[0].schema_ref();
        for index in 0..3 {
            let output_type = schema
                .field(index)
                .try_extension_type::<GeometryType>()
                .unwrap();
            assert_eq!(output_type.metadata().crs(), metadata.crs());
            assert_eq!(output_type.coord_type(), coord_type);
        }

        for column in 0..3 {
            let output =
                GeometryArray::try_from((batches[0].column(column).as_ref(), schema.field(column)))
                    .unwrap();
            assert_eq!(output.len(), 4);
            assert_eq!(output.value(0).unwrap().dim(), Dimensions::Xyzm);
            assert_eq!(output.value(1).unwrap().dim(), Dimensions::Xyzm);
            assert_eq!(output.value(2).unwrap().dim(), Dimensions::Xyzm);
            assert!(output.is_null(3));
        }

        let sliced = array.slice(1, 2);
        let _ = register_geometry(&context, "sliced", Arc::new(sliced), true);
        let batches = query(
            &context,
            "SELECT ST_ChaikinSmoothing(geom, 1, false) AS geom FROM sliced",
        )
        .await;
        let output =
            GeometryArray::try_from((batches[0].column(0).as_ref(), batches[0].schema().field(0)))
                .unwrap();
        assert_eq!(output.len(), 2);
        assert!(matches!(
            output.value(0).unwrap().as_type(),
            geo_traits::GeometryType::Polygon(_)
        ));
        assert!(matches!(
            output.value(1).unwrap().as_type(),
            geo_traits::GeometryType::LineString(_)
        ));
    }
}

#[tokio::test]
async fn nullable_false_geometry_fields_with_null_arguments_follow_is_null() {
    let context = context();

    let line = parse_wkt("LINESTRING M (0 0 1,1 0 2,2 0 3)");
    let polygon = parse_wkt("POLYGON((0 0,1 0,1 1,0 1,0 0))");

    for (name, geometry) in [("lines_nn", &line), ("polygons_nn", &polygon)] {
        let mut builder =
            geoarrow_array::builder::GeometryBuilder::new(GeometryType::new(metadata_4326()));
        builder.push_geometry(Some(geometry)).unwrap();
        let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
        register_geometry(&context, name, array, false);
    }

    let texts: ArrayRef = Arc::new(StringArray::from(vec!["A"]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "letters",
            DataType::Utf8,
            true,
        )])),
        vec![texts],
    )
    .unwrap();
    context.register_batch("letters_nn", batch).unwrap();

    for (sql, expected_null) in [
        (
            "SELECT ST_ChaikinSmoothing(NULL) AS out FROM lines_nn",
            true,
        ),
        (
            "SELECT ST_ChaikinSmoothing(geom, CAST(NULL AS BIGINT)) AS out FROM lines_nn",
            true,
        ),
        (
            "SELECT ST_ChaikinSmoothing(geom, 1, CAST(NULL AS BOOLEAN)) AS out FROM lines_nn",
            true,
        ),
        ("SELECT ST_FilterByM(NULL, 1) AS out FROM lines_nn", true),
        (
            "SELECT ST_FilterByM(geom, CAST(NULL AS DOUBLE), 3) AS out FROM lines_nn",
            false,
        ),
        (
            "SELECT ST_GeneratePoints(polygons_nn.geom, 2, CAST(NULL AS BIGINT)) AS out FROM polygons_nn",
            true,
        ),
        ("SELECT ST_GeometricMedian(NULL) AS out FROM lines_nn", true),
        (
            "SELECT ST_SetEffectiveArea(geom, CAST(NULL AS DOUBLE)) AS out FROM lines_nn",
            true,
        ),
        (
            "SELECT ST_SetEffectiveArea(NULL) AS out FROM lines_nn",
            true,
        ),
        ("SELECT ST_Letters(NULL) AS out FROM lines_nn", true),
        (
            "SELECT ST_Letters(letters, CAST(NULL AS VARCHAR)) AS out FROM letters_nn",
            false,
        ),
    ] {
        let batches = query(&context, sql).await;
        let field = batches[0].schema_ref().field(0).clone();
        assert!(field.is_nullable(), "{sql} output must be nullable");
        let output =
            GeometryArray::try_from((batches[0].column(0).as_ref(), field.as_ref())).unwrap();
        assert_eq!(output.is_null(0), expected_null, "{sql}");
    }
}

#[tokio::test]
async fn generate_points_cardinality_membership_and_seed_replay() {
    let context = context();
    let polygon = "POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,6 4,6 6,4 6,4 4))";
    let sql = format!("SELECT ST_GeneratePoints(ST_GeomFromText('{polygon}'), 25, 4242) AS geom");
    let batches = query(&context, &sql).await;
    let output =
        GeometryArray::try_from((batches[0].column(0).as_ref(), batches[0].schema().field(0)))
            .unwrap();
    let geometry = output.value(0).unwrap();
    let geo_traits::GeometryType::MultiPoint(multi_point) = geometry.as_type() else {
        panic!("ST_GeneratePoints must return a MultiPoint");
    };
    assert_eq!(multi_point.num_points(), 25);
    for point in multi_point.points() {
        let coord = point.coord().unwrap();
        assert!(point_in_polygon(coord.x(), coord.y(), polygon));
    }

    let first = query(&context, &sql).await;
    let second = query(&context, &sql).await;
    assert_eq!(
        first[0].column(0).as_ref(),
        second[0].column(0).as_ref(),
        "the same seed must replay the same points"
    );

    let other = query(
        &context,
        &format!("SELECT ST_GeneratePoints(ST_GeomFromText('{polygon}'), 25, 4243) AS geom"),
    )
    .await;
    assert_ne!(
        first[0].column(0).as_ref(),
        other[0].column(0).as_ref(),
        "different seeds should produce different sequences"
    );
}

fn point_in_polygon(x: f64, y: f64, wkt: &str) -> bool {
    let geometry = parse_wkt(wkt);
    let geo_traits::GeometryType::Polygon(polygon) = geometry.as_type() else {
        panic!("membership fixture must be a polygon");
    };
    let mut inside = false;
    for ring in std::iter::once(polygon.exterior().unwrap()).chain(polygon.interiors()) {
        let coords = ring.coords().to_vec();
        for edge in coords.windows(2) {
            let (x1, y1) = (edge[0].x(), edge[0].y());
            let (x2, y2) = (edge[1].x(), edge[1].y());
            if (y1 > y) != (y2 > y) {
                let crossing = x1 + (y - y1) * (x2 - x1) / (y2 - y1);
                if x < crossing {
                    inside = !inside;
                }
            }
        }
    }
    inside
}

#[tokio::test]
async fn median_and_letters_preserve_arrow_contracts_and_reject_bad_input() {
    let context = context();
    let metadata = metadata_4326();
    let mut builder = geoarrow_array::builder::GeometryBuilder::new(
        GeometryType::new(metadata.clone()).with_coord_type(CoordType::Separated),
    );
    for geometry in [
        "MULTIPOINT ZM ((0 0 0 1),(10 0 10 1),(10 10 20 1),(0 10 30 1))",
        "MULTIPOINT EMPTY",
    ] {
        builder.push_geometry(Some(&parse_wkt(geometry))).unwrap();
    }
    builder.push_geometry(None::<&Wkt<f64>>).unwrap();
    let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
    register_geometry(&context, "median_input", array, true);

    let batches = query(
        &context,
        "SELECT ST_GeometricMedian(geom, NULL, 10000, false) AS geom FROM median_input",
    )
    .await;
    let field = batches[0].schema_ref().field(0).clone();
    let output_type = field.try_extension_type::<GeometryType>().unwrap();
    assert_eq!(output_type.metadata().crs(), metadata.crs());
    assert_eq!(output_type.coord_type(), CoordType::Separated);
    let output = GeometryArray::try_from((batches[0].column(0).as_ref(), field.as_ref())).unwrap();
    assert_eq!(output.value(0).unwrap().dim(), Dimensions::Xyz);
    assert!(matches!(
        output.value(1).unwrap().as_type(),
        geo_traits::GeometryType::Point(_)
    ));
    assert!(output.is_null(2));

    let error = collect(
        &context,
        "SELECT ST_GeometricMedian(ST_GeomFromText('MULTIPOINT M ((0 0 -1),(10 0 1))'))",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("negative weights"), "{error}");

    let error = collect(
        &context,
        "SELECT ST_GeometricMedian(ST_GeomFromText('MULTIPOINT((0 0),(10 0))'), -1)",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("Tolerance must be positive"), "{error}");

    // Letters is exercised as a multi-row column, including a null row.
    let texts: ArrayRef = Arc::new(StringArray::from(vec![Some("A"), Some(""), None]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "letters",
            DataType::Utf8,
            true,
        )])),
        vec![texts],
    )
    .unwrap();
    context.register_batch("letters_input", batch).unwrap();
    let batches = query(
        &context,
        "SELECT ST_Letters(letters) AS geom FROM letters_input",
    )
    .await;
    let output =
        GeometryArray::try_from((batches[0].column(0).as_ref(), batches[0].schema().field(0)))
            .unwrap();
    assert!(matches!(
        output.value(0).unwrap().as_type(),
        geo_traits::GeometryType::MultiPolygon(_)
    ));
    assert!(output.is_null(1));
    assert!(output.is_null(2));

    for (font, expected_dimension) in [
        (
            r#"{"m":"AwABBQAA6AcAAPgK5wcAAPcK","A":"AwABBcgBAMAMAADADL8MAAC/DA=="}"#,
            Dimensions::Xy,
        ),
        (
            r#"{"m":"AwgBAQUAAADoBwAAAPgKAOcHAAAA9woA","A":"AwgBAQXIAQAKwAwAAADADAC/DAAAAL8MAA=="}"#,
            Dimensions::Xyz,
        ),
        (
            r#"{"m":"AwgDAQUAAAAA6AcAAAAA+AoAAOcHAAAAAPcKAAA=","A":"AwgDAQXIAQAKAsAMAAACAMAMAAK/DAAAAgC/DAAF"}"#,
            Dimensions::Xyzm,
        ),
    ] {
        let sql = format!("SELECT ST_Letters('A', '{font}') AS geom");
        let batches = query(&context, &sql).await;
        let output =
            GeometryArray::try_from((batches[0].column(0).as_ref(), batches[0].schema().field(0)))
                .unwrap();
        assert_eq!(output.value(0).unwrap().dim(), expected_dimension, "{font}");
    }

    for (sql, expected) in [
        ("SELECT ST_Letters('A', 'not-json')", "not valid JSON"),
        (
            "SELECT ST_Letters('A', '{\"m\":\"not-base64\",\"A\":\"not-base64\"}')",
            "invalid base64",
        ),
    ] {
        let error = collect(&context, sql).await.unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
    }
}

#[tokio::test]
async fn letters_reuses_the_shared_twkb_decoder() {
    let context = context();

    // XYM, bbox/size, and id-list metadata all come from the shared decoder.
    for (font, expected_dimension) in [
        (r#"{"m":"AQgCAgQG","A":"AQgCAgQG"}"#, Dimensions::Xym),
        (
            r#"{"m":"QQsrEvoBAPQDAO4FAFIA+gH0A+4FUg==","A":"QQsrEvoBAPQDAO4FAFIA+gH0A+4FUg=="}"#,
            Dimensions::Xyzm,
        ),
        (r#"{"m":"BAQCFCgCBAQE","A":"BAQCFCgCBAQE"}"#, Dimensions::Xy),
    ] {
        let batches = query(
            &context,
            &format!("SELECT ST_Letters('A', '{font}') AS geom"),
        )
        .await;
        let output =
            GeometryArray::try_from((batches[0].column(0).as_ref(), batches[0].schema().field(0)))
                .unwrap();
        assert_eq!(output.value(0).unwrap().dim(), expected_dimension, "{font}");
    }

    let font = r#"{"m":"AQgCAgQG","A":"AhgC"}"#;
    let batches = query(
        &context,
        &format!("SELECT ST_Letters('A', '{font}') AS geom"),
    )
    .await;
    let output =
        GeometryArray::try_from((batches[0].column(0).as_ref(), batches[0].schema().field(0)))
            .unwrap();
    assert!(
        output.is_null(0),
        "an EMPTY custom glyph should render no geometry"
    );

    for (glyph, expected) in [
        ("AQ==", "truncated TWKB buffer"),
        ("AgD///////////8C", "TWKB varint overflow"),
    ] {
        let font = format!(r#"{{"m":"AQgCAgQG","A":"{glyph}"}}"#);
        let error = collect(&context, &format!("SELECT ST_Letters('A', '{font}')"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{glyph}: {error}");
    }
}

#[tokio::test]
async fn letters_default_font_is_independent_and_preserves_order_and_spacing() {
    let context = context();
    // PostGIS 3.6.4's embedded font renders 'A' as one 20-point part with
    // xmax 55.8. The independent Hershey data intentionally differs, so this
    // checks the shared ordering/spacing contract instead of PostGIS glyph
    // coordinates.
    let batches = query(
        &context,
        "SELECT \
            ST_GeometryType(ST_Letters('A')) = 'ST_MultiPolygon', \
            ST_IsEmpty(ST_Letters('A')) = false, \
            ST_NPoints(ST_Letters('A')) > 0, \
            ST_XMin(ST_Letters('A')) = 0, \
            ST_XMax(ST_Letters('AB')) > ST_XMax(ST_Letters('A')), \
            ST_XMax(ST_Letters('AB')) > \
                ST_XMax(ST_Letters('A')) + (ST_XMax(ST_Letters('B')) - ST_XMin(ST_Letters('B'))), \
            ST_AsText(ST_GeometryN(ST_Letters('AB'), 1)) = \
                ST_AsText(ST_GeometryN(ST_Letters('A'), 1)), \
            ST_AsText(ST_GeometryN(ST_Letters('BA'), 1)) = \
                ST_AsText(ST_GeometryN(ST_Letters('B'), 1)), \
            ST_Letters('') IS NULL, \
            ST_Letters(' ') IS NULL, \
            ST_Letters(NULL) IS NULL\
        ",
    )
    .await;
    assert_eq!(batches.len(), 1);
    for column in 0..batches[0].num_columns() {
        assert!(
            batches[0].column(column).as_boolean().value(0),
            "column {column}"
        );
    }
}

#[tokio::test]
async fn native_processing_argument_errors_are_stable() {
    let context = context();
    for (sql, expected) in [
        (
            "SELECT ST_ChaikinSmoothing(ST_GeomFromText('LINESTRING(0 0,1 0,1 1)'), 0)",
            "Number of iterations must be between 1 and 5",
        ),
        (
            "SELECT ST_FilterByM(ST_GeomFromText('LINESTRING M (0 0 1,1 0 2)'), 3, 2)",
            "Min-value cannot be larger than Max value",
        ),
        (
            "SELECT ST_GeneratePoints(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'), 1, 0)",
            "seed must be greater than zero",
        ),
        (
            "SELECT ST_GeneratePoints(ST_GeomFromText('POINT(1 2)'), 1, 1)",
            "unsupported geometry type",
        ),
        (
            "SELECT ST_GeometricMedian(ST_GeomFromText('MULTIPOINT((0 0),(10 0))'), 0.1, -1)",
            "Maximum iterations must be positive",
        ),
        (
            "SELECT ST_GeometricMedian(ST_GeomFromText('LINESTRING(0 0,1 1)'))",
            "Unsupported geometry type: LineString",
        ),
        (
            "SELECT ST_SetEffectiveArea(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,1 0,2 0))'))",
            "Dimensions mismatch in lwcollection",
        ),
    ] {
        let error = collect(&context, sql).await.unwrap_err().to_string();
        assert!(error.contains(expected), "{sql}\n{error}");
    }
}
