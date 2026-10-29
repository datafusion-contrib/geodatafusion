//! Raw Arrow and error contracts for GeoJSON, GeoHash, polyline, and SVG UDFs.

mod support;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::WktArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{
    CoordType, Dimension, GeometryType, LineStringType, Metadata, PointType, PolygonType,
};
use support::context::context;
use support::geoarrow::collect;
use wkt::types::{Coord as WktCoord, Point as WktPoint};

fn first_batch(batches: &[RecordBatch]) -> &RecordBatch {
    assert!(!batches.is_empty(), "query must return at least one batch");
    &batches[0]
}

fn point(x: f64, y: f64) -> WktPoint<f64> {
    WktPoint::from_coord(WktCoord {
        x,
        y,
        z: None,
        m: None,
    })
}

#[tokio::test]
async fn format_functions_emit_expected_extension_metadata() {
    let ctx = context();

    let batches = collect(
        &ctx,
        "SELECT ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":[1,2]}')",
    )
    .await
    .unwrap();
    let field = first_batch(&batches).schema().field(0).clone();
    assert_eq!(field.extension_type_name(), Some("geoarrow.geometry"));
    let geometry_type = field.extension_type::<GeometryType>();
    assert_eq!(
        geometry_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
    assert_eq!(geometry_type.coord_type(), CoordType::Separated);

    let batches = collect(
        &ctx,
        "SELECT ST_GeomFromGeoJSON(\
         '{\"type\":\"Point\",\"coordinates\":[1,2],\
         \"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:3857\"}}}')",
    )
    .await
    .unwrap();
    let field = first_batch(&batches).schema().field(0).clone();
    assert_eq!(
        field.extension_type::<GeometryType>().metadata().crs(),
        &Crs::from_authority_code("EPSG:3857".to_string())
    );

    let batches = collect(&ctx, "SELECT ST_GeomFromGeoHash('u4pruydqqvj')")
        .await
        .unwrap();
    let field = first_batch(&batches).schema().field(0).clone();
    assert_eq!(field.extension_type_name(), Some("geoarrow.polygon"));
    let polygon_type = field.extension_type::<PolygonType>();
    assert_eq!(polygon_type.dimension(), Dimension::XY);
    assert!(polygon_type.metadata().crs().crs_value().is_none());

    let batches = collect(
        &ctx,
        "SELECT ST_LineFromEncodedPolyline('_p~iF~ps|U_ulLnnqC_mqNvxq`@')",
    )
    .await
    .unwrap();
    let field = first_batch(&batches).schema().field(0).clone();
    assert_eq!(field.extension_type_name(), Some("geoarrow.linestring"));
    let line_type = field.extension_type::<LineStringType>();
    assert_eq!(
        line_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
}

#[tokio::test]
async fn bare_null_arguments_resolve_to_nullable_typed_outputs() {
    let ctx = context();

    for (sql, extension) in [
        ("SELECT ST_GeomFromGeoJSON(NULL)", Some("geoarrow.geometry")),
        ("SELECT ST_GeomFromGeoHash(NULL)", Some("geoarrow.polygon")),
        (
            "SELECT ST_LineFromEncodedPolyline(NULL)",
            Some("geoarrow.linestring"),
        ),
        ("SELECT ST_AsGeoJSON(NULL)", None),
        ("SELECT ST_AsSVG(NULL)", None),
        ("SELECT ST_AsEncodedPolyline(NULL, 5)", None),
    ] {
        let batches = collect(&ctx, sql).await.unwrap();
        let field = first_batch(&batches).schema().field(0).clone();
        assert!(field.is_nullable(), "{sql} must be declared nullable");
        if let Some(extension) = extension {
            assert_eq!(field.extension_type_name(), Some(extension), "{sql}");
        }
        let column = first_batch(&batches).column(0);
        assert!(
            column.logical_nulls().is_some_and(|nulls| nulls.is_null(0)),
            "{sql}"
        );
    }

    let batches = collect(&ctx, "SELECT ST_GeomFromGeoJSON(NULL) IS NULL")
        .await
        .unwrap();
    assert!(
        first_batch(&batches)
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
}

#[tokio::test]
async fn nonnullable_geometry_with_nullable_parameters_stays_nullable() {
    let ctx = context();
    let point_type = PointType::new(Dimension::XY, Arc::new(Metadata::default()))
        .with_coord_type(CoordType::Separated);
    let points = PointBuilder::from_nullable_points(
        [Some(&point(1.0, 2.0)), Some(&point(3.0, 4.0))].into_iter(),
        point_type,
    )
    .finish();

    let schema = Arc::new(Schema::new(vec![
        points.data_type().to_field("geometry", false),
        Field::new("precision", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            points.to_array_ref(),
            Arc::new(Int64Array::from(vec![Some(3), None])),
        ],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();

    let batches = collect(&ctx, "SELECT ST_AsGeoJSON(geometry, precision) FROM t")
        .await
        .unwrap();
    let field = first_batch(&batches).schema().field(0).clone();
    assert!(field.is_nullable());

    let batches = collect(&ctx, "SELECT ST_AsGeoJSON(geometry) FROM t")
        .await
        .unwrap();
    let field = first_batch(&batches).schema().field(0).clone();
    assert!(!field.is_nullable());

    let batches = collect(
        &ctx,
        "SELECT ST_AsGeoJSON(geometry, precision) IS NULL FROM t",
    )
    .await
    .unwrap();
    let values = first_batch(&batches)
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(values, &BooleanArray::from(vec![false, true]));

    let batches = collect(
        &ctx,
        "SELECT ST_AsSVG(geometry, CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)) FROM t",
    )
    .await
    .unwrap();
    let batch = first_batch(&batches);
    assert!(batch.column(0).is_null(0));
    assert!(batch.column(0).is_null(1));

    let batches = collect(&ctx, "SELECT ST_AsGeoJSON(geometry, NULL, NULL) FROM t")
        .await
        .unwrap();
    let batch = first_batch(&batches);
    assert!(batch.column(0).is_null(0));
    assert!(batch.column(0).is_null(1));
}

#[tokio::test]
async fn multiple_rows_batches_and_slices_preserve_row_alignment() {
    let ctx = context();
    let point_type = PointType::new(Dimension::XY, Arc::new(Metadata::default()))
        .with_coord_type(CoordType::Separated);
    let points = PointBuilder::from_nullable_points(
        [
            Some(&point(1.0, 2.0)),
            Some(&point(3.0, 4.0)),
            Some(&point(5.0, 6.0)),
        ]
        .into_iter(),
        point_type,
    )
    .finish();
    let field = points.data_type().to_field("geometry", false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field.clone()])),
        vec![points.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("points", batch).unwrap();

    let batches = collect(&ctx, "SELECT ST_AsGeoJSON(geometry) FROM points")
        .await
        .unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .map(|value| value.unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            "{\"type\":\"Point\",\"coordinates\":[1,2]}",
            "{\"type\":\"Point\",\"coordinates\":[3,4]}",
            "{\"type\":\"Point\",\"coordinates\":[5,6]}",
        ]
    );

    let sliced = points.slice(1, 2);
    let sliced_field = field.clone();
    ctx.register_batch(
        "sliced_points",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![sliced_field])),
            vec![sliced.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();
    let batches = collect(&ctx, "SELECT ST_AsGeoJSON(geometry) FROM sliced_points")
        .await
        .unwrap();
    assert_eq!(
        first_batch(&batches).column(0).as_string::<i32>().value(0),
        "{\"type\":\"Point\",\"coordinates\":[3,4]}"
    );

    let inputs = StringArray::from(vec![
        Some("{\"type\":\"Point\",\"coordinates\":[7,8]}"),
        None,
        Some("{\"type\":\"Point\",\"coordinates\":[9,10]}"),
    ]);
    let input_field = Arc::new(Field::new("json", DataType::Utf8, true));
    ctx.register_batch(
        "geojson_inputs",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![input_field])),
            vec![Arc::new(inputs)],
        )
        .unwrap(),
    )
    .unwrap();
    let batches = collect(
        &ctx,
        "SELECT ST_AsText(ST_GeomFromGeoJSON(json)) FROM geojson_inputs",
    )
    .await
    .unwrap();
    let texts = batches
        .iter()
        .flat_map(|batch| {
            let array = batch.column(0).as_string::<i32>();
            (0..array.len())
                .map(|row| {
                    if array.is_null(row) {
                        None
                    } else {
                        Some(array.value(row).to_string())
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        texts,
        vec![
            Some("POINT(7 8)".to_string()),
            None,
            Some("POINT(9 10)".to_string())
        ]
    );
}

#[tokio::test]
async fn geojson_column_crs_boundaries_and_unknown_epsg() {
    let ctx = context();
    let input = StringArray::from(vec![
        Some(r#"{"type":"Point","coordinates":[1,2]}"#),
        Some(
            r#"{"type":"Point","coordinates":[3,4],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}"#,
        ),
        None,
        Some(r#"{"type":"Point","coordinates":[]}"#),
    ]);
    let input_field = Arc::new(Field::new("json", DataType::Utf8, true));
    ctx.register_batch(
        "geojson_column_crs",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![input_field])),
            vec![Arc::new(input)],
        )
        .unwrap(),
    )
    .unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_GeomFromGeoJSON(json) FROM geojson_column_crs",
    )
    .await
    .unwrap();
    let field = first_batch(&batches).schema().field(0).clone();
    assert_eq!(
        field.extension_type::<GeometryType>().metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
    assert!(field.is_nullable());

    let batches = collect(
        &ctx,
        "SELECT ST_AsText(ST_GeomFromGeoJSON(json)) FROM geojson_column_crs",
    )
    .await
    .unwrap();
    let values = first_batch(&batches).column(0).as_string::<i32>();
    assert_eq!(values.value(0), "POINT(1 2)");
    assert_eq!(values.value(1), "POINT(3 4)");
    assert!(values.is_null(2));
    assert_eq!(values.value(3), "POINT EMPTY");

    let mismatched = StringArray::from(vec![Some(
        r#"{"type":"Point","coordinates":[1,2],"crs":{"type":"name","properties":{"name":"EPSG:3857"}}}"#,
    )]);
    let mismatched_field = Arc::new(Field::new("json", DataType::Utf8, false));
    ctx.register_batch(
        "geojson_mismatched_crs",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![mismatched_field])),
            vec![Arc::new(mismatched)],
        )
        .unwrap(),
    )
    .unwrap();
    let error = collect(
        &ctx,
        "SELECT ST_GeomFromGeoJSON(json) FROM geojson_mismatched_crs",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains(
            "ST_GeomFromGeoJSON row 0 has SRID 3857, but the output field is declared with SRID 4326"
        ),
        "{error}"
    );

    let batches = collect(
        &ctx,
        "SELECT ST_GeomFromGeoJSON(\
         '{\"type\":\"Point\",\"coordinates\":[1,2],\
         \"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:998999\"}}}')",
    )
    .await
    .unwrap();
    let field = first_batch(&batches).schema().field(0).clone();
    assert_eq!(
        field.extension_type::<GeometryType>().metadata().crs(),
        &Crs::from_authority_code("EPSG:998999".to_string())
    );
}

#[tokio::test]
async fn invalid_inputs_fail_explicitly_instead_of_silently_corrupting() {
    let ctx = context();
    let error = collect(
        &ctx,
        "SELECT ST_GeomFromGeoJSON('{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1,2]},\"properties\":{}}')",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("invalid GeoJson representation"), "{error}");

    let error = collect(
        &ctx,
        "SELECT ST_GeomFromGeoJSON('{\"type\":\"FeatureCollection\",\"features\":[]}')",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("invalid GeoJson representation"), "{error}");

    let error = collect(&ctx, "SELECT ST_GeomFromGeoHash('abc')")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Invalid character"), "{error}");

    let error = collect(&ctx, "SELECT ST_LineFromEncodedPolyline('zzzzzzzzzzzzz')")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("64-bit delta range"), "{error}");

    let error = collect(&ctx, "SELECT ST_LineFromEncodedPolyline('_')")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("truncated"), "{error}");

    let error = collect(&ctx, "SELECT ST_AsEncodedPolyline(ST_MakePoint(1, 2), 5)")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Only SRID 4326"), "{error}");

    let error = collect(&ctx, "SELECT ST_AsEncodedPolyline('LINESTRING(1 2,3 4)')")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Only SRID 4326"), "{error}");
}

#[tokio::test]
async fn svg_collections_skip_empty_members_without_failing() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_AsSVG(ST_GeomFromText(\
         'GEOMETRYCOLLECTION(POINT(1 2),LINESTRING EMPTY,LINESTRING(3 4,5 6))'))",
    )
    .await
    .unwrap();
    assert_eq!(
        first_batch(&batches).column(0).as_string::<i32>().value(0),
        "cx=\"1\" cy=\"-2\";M 3 -4 L 5 -6"
    );
}

#[tokio::test]
async fn format_boundary_errors_are_explicit() {
    let ctx = context();

    // A delta cut off inside its varint and a delta that does not fit in the
    // signed 32-bit range both fail instead of returning wrapped coordinates.
    for (encoded, expected) in [("__", "truncated"), ("zzzzzzzzzzzzz", "64-bit delta range")] {
        let error = collect(
            &ctx,
            &format!("SELECT ST_LineFromEncodedPolyline('{encoded}')"),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains(expected), "{encoded}: {error}");
    }
    // PostGIS reads the string terminator for a value starting after the last
    // encoded byte; the pinned `?` case keeps that behavior.
    let batches = collect(&ctx, "SELECT ST_AsText(ST_LineFromEncodedPolyline('?'))")
        .await
        .unwrap();
    assert_eq!(
        first_batch(&batches).column(0).as_string::<i32>().value(0),
        "LINESTRING(-0.00001 0)"
    );

    let error = collect(
        &ctx,
        "SELECT ST_AsEncodedPolyline(\
         ST_GeomFromEWKT('SRID=4326;LINESTRING(0 0,1 1)'), 19)",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("0..=18"), "{error}");
    let batches = collect(
        &ctx,
        "SELECT ST_AsEncodedPolyline(\
         ST_GeomFromEWKT('SRID=4326;LINESTRING(0 0,1 1)'), 18)",
    )
    .await
    .unwrap();
    let encoded = first_batch(&batches).column(0).as_string::<i32>();
    assert!(!encoded.is_null(0));
    assert!(!encoded.value(0).is_empty());
    // Every encoding at the supported maximum precision decodes back.
    let batches = collect(
        &ctx,
        "SELECT ST_AsText(ST_LineFromEncodedPolyline(ST_AsEncodedPolyline(\
         ST_GeomFromEWKT('SRID=4326;LINESTRING(0 0,1 1)'), 18), 18))",
    )
    .await
    .unwrap();
    assert_eq!(
        first_batch(&batches).column(0).as_string::<i32>().value(0),
        "LINESTRING(0 0,1 1)"
    );
    // +2^63 is one past i64::MAX and must not saturate to i64::MAX; -2^63
    // remains representable and encodes.
    let error = collect(
        &ctx,
        "SELECT ST_AsEncodedPolyline(\
         ST_GeomFromEWKT('SRID=4326;LINESTRING(0 0,9223372036854775808 0)'), 0)",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("exceeds the supported integer range"),
        "{error}"
    );
    let batches = collect(
        &ctx,
        "SELECT ST_AsEncodedPolyline(\
         ST_GeomFromEWKT('SRID=4326;LINESTRING(0 0,-9223372036854775808 0)'), 0)",
    )
    .await
    .unwrap();
    assert!(
        !first_batch(&batches)
            .column(0)
            .as_string::<i32>()
            .is_null(0)
    );

    let error = collect(
        &ctx,
        "SELECT ST_AsSVG(ST_GeomFromText('POINT(1 2)'), 0, 309)",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("exceeds the supported range"), "{error}");
    let batches = collect(
        &ctx,
        "SELECT ST_AsSVG(ST_GeomFromText('POINT(1 2)'), 0, 308)",
    )
    .await
    .unwrap();
    assert_eq!(
        first_batch(&batches).column(0).as_string::<i32>().value(0),
        "cx=\"1\" cy=\"-2\""
    );
}

#[tokio::test]
async fn svg_empty_multipart_members_emit_no_fragment() {
    let ctx = context();
    for (sql, expected) in [
        (
            "SELECT ST_AsSVG(ST_GeomFromText('MULTIPOINT(EMPTY,(1 2))'))",
            "cx=\"1\" cy=\"-2\"",
        ),
        (
            "SELECT ST_AsSVG(ST_GeomFromText('MULTILINESTRING((0 0,1 1),EMPTY)'))",
            "M 0 0 L 1 -1",
        ),
        (
            "SELECT ST_AsSVG(ST_GeomFromText('MULTIPOLYGON(EMPTY,((0 0,1 0,1 1,0 0)))'))",
            "M 0 0 L 1 0 1 -1 Z",
        ),
        ("SELECT ST_AsSVG(ST_GeomFromText('MULTIPOINT EMPTY'))", ""),
    ] {
        let batches = collect(&ctx, sql).await.unwrap();
        assert_eq!(
            first_batch(&batches).column(0).as_string::<i32>().value(0),
            expected,
            "{sql}"
        );
    }
}

#[tokio::test]
async fn typed_wkt_and_wkb_crs_metadata_is_honored() {
    let ctx = context();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:2154".to_string()),
        None,
    ));
    let wkt = WktArray::new(
        StringArray::from(vec![Some("POINT ZM (1 2 3 4)")]),
        metadata,
    );
    let wkt_field = wkt.data_type().to_field("geometry", false);
    ctx.register_batch(
        "typed_wkt",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![wkt_field])),
            vec![wkt.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();

    let wkb = to_wkb::<i32>(&wkt).unwrap();
    let wkb_field = wkb.data_type().to_field("geometry", false);
    ctx.register_batch(
        "typed_wkb",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![wkb_field])),
            vec![wkb.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();

    for table in ["typed_wkt", "typed_wkb"] {
        for (options, name) in [(8, "EPSG:2154"), (4, "urn:ogc:def:crs:EPSG::2154")] {
            let sql = format!("SELECT ST_AsGeoJSON(geometry, 9, {options}) FROM {table}");
            let batches = collect(&ctx, &sql).await.unwrap();
            assert_eq!(
                first_batch(&batches).column(0).as_string::<i32>().value(0),
                format!(
                    "{{\"type\":\"Point\",\"crs\":{{\"type\":\"name\",\"properties\":\
                     {{\"name\":\"{name}\"}}}},\"coordinates\":[1,2,3]}}"
                ),
                "{sql}"
            );
        }

        let sql = format!("SELECT ST_AsSVG(geometry) FROM {table}");
        let batches = collect(&ctx, &sql).await.unwrap();
        assert_eq!(
            first_batch(&batches).column(0).as_string::<i32>().value(0),
            "cx=\"1\" cy=\"-2\"",
            "{sql}"
        );
    }

    let line_metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let line_wkt = WktArray::new(
        StringArray::from(vec![Some("LINESTRING (-120.2 38.5,-120.95 40.7)")]),
        line_metadata,
    );
    let line_field = line_wkt.data_type().to_field("geometry", false);
    ctx.register_batch(
        "typed_wkt_line",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![line_field])),
            vec![line_wkt.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();
    let line_wkb = to_wkb::<i32>(&line_wkt).unwrap();
    let line_wkb_field = line_wkb.data_type().to_field("geometry", false);
    ctx.register_batch(
        "typed_wkb_line",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![line_wkb_field])),
            vec![line_wkb.to_array_ref()],
        )
        .unwrap(),
    )
    .unwrap();

    for table in ["typed_wkt_line", "typed_wkb_line"] {
        let sql = format!("SELECT ST_AsEncodedPolyline(geometry) FROM {table}");
        let batches = collect(&ctx, &sql).await.unwrap();
        assert_eq!(
            first_batch(&batches).column(0).as_string::<i32>().value(0),
            "_p~iF~ps|U_ulLnnqC",
            "{sql}"
        );

        let sql = format!("SELECT ST_AsEncodedPolyline(geometry, NULL) FROM {table}");
        let batches = collect(&ctx, &sql).await.unwrap();
        assert!(first_batch(&batches).column(0).is_null(0), "{sql}");
    }
}
