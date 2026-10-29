use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_schema::Schema;
use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray, PolygonArray};
use geoarrow_array::builder::{LineStringBuilder, PolygonBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Crs, Dimension, LineStringType, Metadata, PolygonType};
use wkt::Wkt;
use wkt::types::Coord;

fn new_context() -> SessionContext {
    let context = SessionContext::new();
    geodatafusion::register(&context);
    context
}

fn metadata() -> Arc<Metadata> {
    Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ))
}

async fn collect(context: &SessionContext, sql: &str) -> RecordBatch {
    let dataframe = context
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}\n{error}"));
    let batches = dataframe
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}\n{error}"));
    assert_eq!(batches.len(), 1);
    batches.into_iter().next().unwrap()
}

async fn collect_error(context: &SessionContext, sql: &str) -> String {
    match context.sql(sql).await {
        Ok(dataframe) => match dataframe.collect().await {
            Ok(_) => panic!("expected query to fail: {sql}"),
            Err(error) => error.to_string(),
        },
        Err(error) => error.to_string(),
    }
}

fn assert_wkt(actual: &str, expected: &str) {
    let actual: Wkt<f64> = Wkt::from_str(actual)
        .unwrap_or_else(|error| panic!("failed to parse actual WKT {actual:?}: {error}"));
    let expected: Wkt<f64> = Wkt::from_str(expected)
        .unwrap_or_else(|error| panic!("failed to parse expected WKT {expected:?}: {error}"));
    assert_eq!(actual, expected, "actual={actual:?} expected={expected:?}");
}

fn text_value(batch: &RecordBatch, column: usize) -> Option<&str> {
    let values = batch.column(column).as_string::<i32>();
    (!values.is_null(0)).then(|| values.value(0))
}

#[tokio::test]
async fn coordinate_editors_match_postgis_defaults_in_sql() {
    let context = new_context();
    for (sql, expected) in [
        (
            "SELECT ST_AsText(ST_LineExtend(ST_GeomFromText('LINESTRING(0 0,1 0)'), 1.0))",
            Some("LINESTRING(0 0,1 0,2 0)"),
        ),
        (
            "SELECT ST_AsText(ST_LineExtend(ST_GeomFromText('LINESTRING ZM (0 0 1 1,1 0 2 2)'), 1.0, 2.0))",
            Some("LINESTRING ZM (-2 0 -1 -1,0 0 1 1,1 0 2 2,2 0 3 3)"),
        ),
        (
            "SELECT ST_AsText(ST_LineExtend(ST_GeomFromText('LINESTRING(0 0,0 0)'), 1.0))",
            Some("LINESTRING(0 0,0 0)"),
        ),
        (
            "SELECT ST_AsText(ST_LineExtend(ST_GeomFromText('LINESTRING(0 0,0 0,1 0)'), 1.0, 1.0))",
            Some("LINESTRING(-1 0,0 0,0 0,1 0,2 0)"),
        ),
        (
            "SELECT ST_AsText(ST_LineExtend(ST_GeomFromText('LINESTRING EMPTY'), 1.0))",
            None,
        ),
        (
            "SELECT ST_AsText(ST_QuantizeCoordinates(ST_GeomFromText('POINT(1.2345678901234 2.3456789012345)'), 3))",
            Some("POINT(1.234375 2.34521484375)"),
        ),
        (
            "SELECT ST_AsText(ST_QuantizeCoordinates(ST_GeomFromText('POINT ZM (1.2345678901234 2.3456789012345 3.4567890123456 4.5678901234567)'), 3, 2, 1, 0))",
            Some("POINT ZM (1.234375 2.34375 3.4375 4.5)"),
        ),
        (
            "SELECT ST_AsText(ST_QuantizeCoordinates(ST_GeomFromText('POINT ZM (1.2345678901234 2.3456789012345 3.4567890123456 4.5678901234567)'), 3, NULL, 1, NULL))",
            Some("POINT ZM (1.234375 2.34521484375 3.4375 4.56787109375)"),
        ),
        (
            "SELECT ST_AsText(ST_RemoveRepeatedPoints(ST_GeomFromText('LINESTRING(0 0,1 1,1 1,2 2)')))",
            Some("LINESTRING(0 0,1 1,2 2)"),
        ),
        (
            "SELECT ST_AsText(ST_RemoveRepeatedPoints(ST_GeomFromText('LINESTRING(1 1,1 1,1 1)')))",
            Some("LINESTRING(1 1,1 1)"),
        ),
        (
            "SELECT ST_AsText(ST_RemoveRepeatedPoints(ST_GeomFromText('POLYGON((0 0,1 0,1 0,1 1,0 1,0 0))')))",
            Some("POLYGON((0 0,1 0,1 1,0 1,0 0))"),
        ),
        (
            "SELECT ST_AsText(ST_RemoveRepeatedPoints(ST_GeomFromText('MULTIPOINT(2 2,1 1,2 2)')))",
            Some("MULTIPOINT((1 1),(2 2))"),
        ),
        (
            "SELECT ST_AsText(ST_RemoveRepeatedPoints(ST_GeomFromText('LINESTRING(0 0,1 0,1.4 0,2 0)'), 0.5))",
            Some("LINESTRING(0 0,1 0,2 0)"),
        ),
        (
            "SELECT ST_AsText(ST_Segmentize(ST_GeomFromText('LINESTRING(0 0,10 0)'), 3.0))",
            Some("LINESTRING(0 0,2.5 0,5 0,7.5 0,10 0)"),
        ),
        (
            "SELECT ST_AsText(ST_Segmentize(ST_GeomFromText('LINESTRING ZM (0 0 0 0,10 0 10 20)'), 4.0))",
            Some(
                "LINESTRING ZM (0 0 0 0,3.3333333333333335 0 3.3333333333333335 6.666666666666667,6.666666666666667 0 6.666666666666667 13.333333333333334,10 0 10 20)",
            ),
        ),
        (
            "SELECT ST_AsText(ST_Segmentize(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'), 6.0))",
            Some("POLYGON((0 0,5 0,10 0,10 5,10 10,5 10,0 10,0 5,0 0))"),
        ),
        (
            "SELECT ST_AsText(ST_Segmentize(ST_GeomFromText('POINT(1 2)'), 0.0))",
            Some("POINT(1 2)"),
        ),
        (
            "SELECT ST_AsText(ST_ShiftLongitude(ST_GeomFromText('LINESTRING(-200 0,-10 0,0 0,180 0,181 0,400 0)')))",
            Some("LINESTRING(160 0,350 0,0 0,180 0,-179 0,40 0)"),
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POINT(-10 0)'), 0.0, 360.0))",
            Some("POINT(350 0)"),
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('MULTIPOINT(-10 0,10 0)'), 0.0, 360.0))",
            Some("MULTIPOINT((350 0),(10 0))"),
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('LINESTRING(0 0,10 0)'), 5.0, 100.0))",
            Some("MULTILINESTRING((100 0,105 0),(5 0,10 0))"),
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('LINESTRING(0 0,10 0)'), 5.0, -100.0))",
            Some("MULTILINESTRING((0 0,5 0),(-95 0,-90 0))"),
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('LINESTRING(0 0,10 0)'), 5.0, 5.0))",
            Some("LINESTRING(5 0,10 0)"),
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('LINESTRING(-10 0,20 0)'), 0.0, 5.0))",
            Some("MULTILINESTRING((-5 0,0 0),(0 0,5 0),(5 0,20 0))"),
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POLYGON ZM ((0 0 1 5,10 0 2 6,10 10 3 7,0 10 4 8,0 0 1 5))'), 20.0, 5.0))",
            Some("POLYGON ZM((5 0 1 5,15 0 2 6,15 10 3 7,5 10 4 8,5 0 1 5))"),
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POLYGON Z ((10 0 1,20 0 2,20 10 3,10 10 4,10 0 1))'), 0.0, 5.0))",
            Some("POLYGON Z((10 0 1,20 0 2,20 10 3,10 10 4,10 0 1))"),
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('LINESTRING ZM (0 0 1 1,10 0 2 2)'), 5.0, 100.0))",
            Some("MULTILINESTRING Z ((100 0 1,105 0 1.5),(5 0 1.5,10 0 2))"),
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('LINESTRING EMPTY'), 5.0, 100.0))",
            Some("LINESTRING EMPTY"),
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POINT(1 2)'), 5.0, 0.0))",
            Some("POINT(1 2)"),
        ),
    ] {
        let batch = collect(&context, sql).await;
        match (text_value(&batch, 0), expected) {
            (Some(actual), Some(expected)) => assert_wkt(actual, expected),
            (None, None) => {}
            (actual, expected) => panic!("{sql}: actual={actual:?} expected={expected:?}"),
        }
    }

    for (sql, expected) in [
        (
            "SELECT ST_LineExtend(ST_GeomFromText('POINT(0 0)'), 1.0)",
            "Argument must be LINESTRING geometry",
        ),
        (
            "SELECT ST_LineExtend(ST_GeomFromText('LINESTRING(0 0,1 0)'), -1.0)",
            "lwline_extend: distances must be non-negative",
        ),
        (
            "SELECT ST_QuantizeCoordinates(ST_GeomFromText('POINT(1 2)'), NULL)",
            "Must specify precision",
        ),
        (
            "SELECT ST_Segmentize(ST_GeomFromText('LINESTRING EMPTY'), 0.0)",
            "invalid max_distance",
        ),
        (
            "SELECT ST_Segmentize(ST_GeomFromText('LINESTRING(0 0,1 1)'), -1.0)",
            "invalid max_distance",
        ),
    ] {
        let error = collect_error(&context, sql).await;
        assert!(error.contains(expected), "{sql}: {error}");
    }
}

/// Crossing polygons are split through GEOS, exactly like PostGIS, so the
/// notched and multi-part results below cannot be produced by the native
/// half-plane clip.
#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn wrap_x_polygon_splits_match_postgis_defaults() {
    let context = new_context();
    for (sql, expected) in [
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'), 5.0, 100.0))",
            "MULTIPOLYGON(((5 10,10 10,10 0,5 0,5 10)),((105 0,100 0,100 10,105 10,105 0)))",
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POLYGON((0 0,20 0,20 20,0 20,0 0),(8 8,12 8,12 12,8 12,8 8))'), 10.0, 100.0))",
            "MULTIPOLYGON(((10 20,20 20,20 0,10 0,10 8,12 8,12 12,10 12,10 20)),((110 0,100 0,100 20,110 20,110 12,108 12,108 8,110 8,110 0)))",
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 6,9 6,9 4,0 4,0 0))'), 5.0, 100.0))",
            "MULTIPOLYGON(((105 0,100 0,100 4,105 4,105 0)),((5 10,10 10,10 0,5 0,5 4,9 4,9 6,5 6,5 10)),((105 6,100 6,100 10,105 10,105 6)))",
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POLYGON Z ((-10 0 0,20 0 30,20 10 30,-10 10 0,-10 0 0))'), 0.0, 5.0))",
            "POLYGON Z((5 0 10,0 0 10,-5 0 0,-5 10 0,0 10 10,5 10 10,20 10 30,20 0 30,5 0 10))",
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POLYGON Z ((-10 0 0,20 0 2,20 20 4,-10 20 2,-10 0 0),(8 8 0.5,12 8 0.5,12 12 0.5,8 12 0.5,8 8 0.5))'), 0.0, 5.0))",
            "POLYGON Z((5 0 0.6666666666666666,0 0 0.6666666666666666,-5 0 0,-5 20 2,0 20 2.666666666666667,5 20 2.666666666666667,20 20 4,20 0 2,5 0 0.6666666666666666),(8 8 0.5,12 8 0.5,12 12 0.5,8 12 0.5,8 8 0.5))",
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POLYGON ZM ((-10 0 0 1,20 0 30 2,20 10 30 3,-10 10 0 4,-10 0 0 1))'), 0.0, 5.0))",
            "POLYGON Z((5 0 10,0 0 10,-5 0 0,-5 10 0,0 10 10,5 10 10,20 10 30,20 0 30,5 0 10))",
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POLYGON M ((-10 0 1,20 0 2,20 10 3,-10 10 4,-10 0 1))'), 0.0, 5.0))",
            "POLYGON((5 0,0 0,-5 0,-5 10,0 10,5 10,20 10,20 0,5 0))",
        ),
        (
            "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('MULTIPOLYGON Z (((-10 0 0,20 0 30,20 10 30,-10 10 0,-10 0 0)),((40 0 1,50 0 2,50 10 3,40 10 4,40 0 1)))'), 0.0, 5.0))",
            "MULTIPOLYGON Z(((5 0 10,0 0 10,-5 0 0,-5 10 0,0 10 10,5 10 10,20 10 30,20 0 30,5 0 10)),((40 0 1,50 0 2,50 10 3,40 10 4,40 0 1)))",
        ),
    ] {
        let batch = collect(&context, sql).await;
        let actual = text_value(&batch, 0).unwrap_or_else(|| panic!("{sql}: NULL result"));
        assert_wkt(actual, expected);
    }
}

#[cfg(not(feature = "geos-3_11"))]
#[tokio::test]
async fn wrap_x_polygon_splits_require_geos() {
    let context = new_context();
    let sql = "SELECT ST_AsText(ST_WrapX(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'), 5.0, 100.0))";
    let error = collect_error(&context, sql).await;
    assert!(
        error.contains("splitting polygons requires the geos-3_11 feature"),
        "{sql}: {error}"
    );
}

#[tokio::test]
async fn coordinate_editors_preserve_native_metadata_and_slices() {
    let context = new_context();
    let metadata = metadata();
    let line_type = LineStringType::new(Dimension::XYZM, metadata.clone())
        .with_coord_type(CoordType::Separated);
    let mut builder = LineStringBuilder::new(line_type.clone());
    builder
        .push_line_string(Some(&wkt::types::LineString::new(
            vec![
                Coord {
                    x: 0.0,
                    y: 0.0,
                    z: Some(1.0),
                    m: Some(1.0),
                },
                Coord {
                    x: 10.0,
                    y: 0.0,
                    z: Some(5.0),
                    m: Some(9.0),
                },
            ],
            wkt::types::Dimension::XYZM,
        )))
        .unwrap();
    builder.push_geometry(None::<&Wkt<f64>>).unwrap();
    let line = builder.finish();
    let field = line.data_type().to_field("geom", true);
    context
        .register_batch(
            "t",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![field])),
                vec![line.to_array_ref()],
            )
            .unwrap(),
        )
        .unwrap();

    for sql in [
        "SELECT ST_QuantizeCoordinates(geom, 1) FROM t",
        "SELECT ST_RemoveRepeatedPoints(geom) FROM t",
        "SELECT ST_ShiftLongitude(geom) FROM t",
    ] {
        let batch = collect(&context, sql).await;
        let field = batch.schema_ref().field(0);
        let output_type = field.try_extension_type::<LineStringType>().unwrap();
        assert_eq!(output_type.dimension(), Dimension::XYZM, "{sql}");
        assert_eq!(output_type.metadata().crs(), metadata.crs(), "{sql}");
        let output = LineStringArray::try_from((batch.column(0).as_ref(), field)).unwrap();
        assert!(output.is_null(1), "{sql}");
        let coords = output.value(0).unwrap().coords().collect::<Vec<_>>();
        assert_eq!(coords.len(), 2, "{sql}");
    }

    let batch = collect(&context, "SELECT ST_Segmentize(geom, 5.0) FROM t").await;
    let field = batch.schema_ref().field(0);
    let output = LineStringArray::try_from((batch.column(0).as_ref(), field)).unwrap();
    let coords = output.value(0).unwrap().coords().collect::<Vec<_>>();
    assert_eq!(coords.len(), 3);
    assert_eq!(coords[1].nth(2), Some(3.0));
    assert_eq!(coords[1].nth(3), Some(5.0));

    let batch = collect(&context, "SELECT ST_LineExtend(geom, 2.0) FROM t").await;
    let field = batch.schema_ref().field(0);
    let output = LineStringArray::try_from((batch.column(0).as_ref(), field)).unwrap();
    let coords = output.value(0).unwrap().coords().collect::<Vec<_>>();
    assert_eq!(coords.len(), 3);
    assert_eq!(coords[2].nth(2), Some(5.8));
    assert_eq!(coords[2].nth(3), Some(10.6));

    let batch = collect(&context, "SELECT ST_WrapX(geom, 5.0, 100.0) FROM t").await;
    let field = batch.schema_ref().field(0);
    assert_eq!(
        field
            .try_extension_type::<geoarrow_schema::GeometryType>()
            .unwrap()
            .metadata()
            .crs(),
        metadata.crs()
    );
    let output = GeometryArray::try_from((batch.column(0).as_ref(), field)).unwrap();
    assert!(output.is_null(1));
    assert_eq!(output.value(0).unwrap().dim(), geo_traits::Dimensions::Xyz);

    let context = new_context();
    let mut builder = LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata.clone()));
    for (start, end) in [(0.0, 1.0), (2.0, 3.0), (4.0, 5.0), (6.0, 7.0)] {
        builder
            .push_line_string(Some(&wkt::types::LineString::new(
                vec![
                    Coord {
                        x: start,
                        y: 0.0,
                        z: None,
                        m: None,
                    },
                    Coord {
                        x: end,
                        y: 0.0,
                        z: None,
                        m: None,
                    },
                ],
                wkt::types::Dimension::XY,
            )))
            .unwrap();
    }
    let line = builder.finish().slice(1, 2);
    let field = line.data_type().to_field("geom", true);
    context
        .register_batch(
            "sliced",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![field])),
                vec![line.to_array_ref()],
            )
            .unwrap(),
        )
        .unwrap();
    let batch = collect(&context, "SELECT ST_Segmentize(geom, 1.0) FROM sliced").await;
    let output =
        LineStringArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0))).unwrap();
    let first = output.value(0).unwrap().coords().next().unwrap();
    assert_eq!((first.x(), first.y()), (2.0, 0.0));
}

#[tokio::test]
async fn strict_null_contracts_over_non_null_native_source() {
    let context = new_context();
    let line_type = LineStringType::new(Dimension::XY, metadata());
    let mut builder = LineStringBuilder::new(line_type);
    builder
        .push_line_string(Some(&wkt::types::LineString::new(
            vec![
                Coord {
                    x: 0.0,
                    y: 0.0,
                    z: None,
                    m: None,
                },
                Coord {
                    x: 4.0,
                    y: 0.0,
                    z: None,
                    m: None,
                },
            ],
            wkt::types::Dimension::XY,
        )))
        .unwrap();
    let line = builder.finish();
    let field = line.data_type().to_field("geom", false);
    context
        .register_batch(
            "nn",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![field])),
                vec![line.to_array_ref()],
            )
            .unwrap(),
        )
        .unwrap();

    for expression in [
        "ST_LineExtend(geom, CAST(NULL AS DOUBLE))",
        "ST_LineExtend(geom, 1.0, CAST(NULL AS DOUBLE))",
        "ST_RemoveRepeatedPoints(geom, CAST(NULL AS DOUBLE))",
        "ST_Segmentize(geom, CAST(NULL AS DOUBLE))",
        "ST_WrapX(geom, CAST(NULL AS DOUBLE), 1.0)",
        "ST_WrapX(geom, 0.0, CAST(NULL AS DOUBLE))",
    ] {
        let batch = collect(&context, &format!("SELECT {expression} IS NULL FROM nn")).await;
        assert!(batch.column(0).as_boolean().value(0), "{expression}");
        let batch = collect(&context, &format!("SELECT {expression} AS out FROM nn")).await;
        assert!(batch.schema_ref().field(0).is_nullable(), "{expression}");
    }

    let batch = collect(&context, "SELECT ST_Segmentize(geom, 2.0) IS NULL FROM nn").await;
    assert!(!batch.column(0).as_boolean().value(0));
    let batch = collect(&context, "SELECT ST_Segmentize(geom, 2.0) AS out FROM nn").await;
    assert!(!batch.schema_ref().field(0).is_nullable());

    let error = collect_error(
        &context,
        "SELECT ST_QuantizeCoordinates(geom, CAST(NULL AS INT)) FROM nn",
    )
    .await;
    assert!(error.contains("Must specify precision"), "{error}");
}

#[tokio::test]
async fn empty_polygon_native_geometry_is_supported() {
    let context = new_context();
    let polygon_type = PolygonType::new(Dimension::XY, metadata());
    let mut builder = PolygonBuilder::new(polygon_type);
    builder
        .push_polygon(Some(&wkt::types::Polygon::new(
            Vec::new(),
            wkt::types::Dimension::XY,
        )))
        .unwrap();
    let polygon = builder.finish();
    let field = polygon.data_type().to_field("geom", true);
    context
        .register_batch(
            "p",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![field])),
                vec![polygon.to_array_ref()],
            )
            .unwrap(),
        )
        .unwrap();
    for sql in [
        "SELECT ST_QuantizeCoordinates(geom, 3) FROM p",
        "SELECT ST_RemoveRepeatedPoints(geom) FROM p",
        "SELECT ST_Segmentize(geom, 1.0) FROM p",
        "SELECT ST_ShiftLongitude(geom) FROM p",
        "SELECT ST_WrapX(geom, 0.0, 1.0) FROM p",
    ] {
        let batch = collect(&context, sql).await;
        if sql.contains("ST_WrapX") {
            let geometry =
                GeometryArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0)))
                    .unwrap();
            assert!(!geometry.is_null(0), "{sql}");
        } else {
            let output =
                PolygonArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0)))
                    .unwrap();
            assert!(!output.is_null(0), "{sql}");
        }
    }
}
