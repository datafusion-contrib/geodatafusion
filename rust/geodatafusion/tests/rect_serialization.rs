//! Box/rect serialization contracts shared by IO and GEOS-backed UDFs.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_schema::Schema;
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::RectBuilder;
use geoarrow_schema::{BoxType, Crs, Dimension, Metadata};
use wkt::types::Coord;

/// The WKB that PostGIS 3.6.4 returns for `BOX(0 0,10 10)`.
const POSTGIS_BOX2D_WKB_HEX: &str = "010300000001000000050000000000000000000000000000000000000000000000000000000000000000002440000000000000244000000000000024400000000000002440000000000000000000000000000000000000000000000000";

fn new_context() -> SessionContext {
    let context = SessionContext::new();
    geodatafusion::register(&context);
    context
}

async fn collect(context: &SessionContext, sql: &str) -> RecordBatch {
    let batches = context
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}\n{error}"))
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
    let actual: wkt::Wkt<f64> = actual.parse().unwrap();
    let expected: wkt::Wkt<f64> = expected.parse().unwrap();
    assert_eq!(actual, expected, "actual={actual:?} expected={expected:?}");
}

fn coord(x: f64, y: f64, z: Option<f64>, m: Option<f64>) -> Coord<f64> {
    Coord { x, y, z, m }
}

fn register_rect(
    context: &SessionContext,
    name: &str,
    typ: BoxType,
    values: Vec<Option<(Coord<f64>, Coord<f64>)>>,
) {
    let mut builder = RectBuilder::with_capacity(typ, values.len());
    for value in values {
        match value {
            Some((min, max)) => builder.push_min_max(&min, &max),
            None => builder.push_null(),
        }
    }
    let array = builder.finish();
    let field = array.data_type().to_field("geom", true);
    context
        .register_batch(
            name,
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![field])),
                vec![array.to_array_ref()],
            )
            .unwrap(),
        )
        .unwrap();
}

fn default_box_type(dimension: Dimension) -> BoxType {
    BoxType::new(dimension, Arc::new(Metadata::default()))
}

#[tokio::test]
async fn box_wkb_and_ewkb_roundtrip_as_polygons() {
    let context = new_context();
    let batch = collect(
        &context,
        "SELECT
            encode(ST_AsBinary(ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10))), 'hex'),
            encode(ST_AsEWKB(ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10))), 'hex'),
            ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10))))),
            ST_AsText(ST_GeomFromEWKB(ST_AsEWKB(ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10)))))",
    )
    .await;

    let wkb = batch.column(0).as_string::<i32>().value(0);
    let ewkb = batch.column(1).as_string::<i32>().value(0);
    assert_eq!(wkb, POSTGIS_BOX2D_WKB_HEX);
    assert_eq!(ewkb, POSTGIS_BOX2D_WKB_HEX);
    assert_wkt(
        batch.column(2).as_string::<i32>().value(0),
        "POLYGON((0 0,0 10,10 10,10 0,0 0))",
    );
    assert_wkt(
        batch.column(3).as_string::<i32>().value(0),
        "POLYGON((0 0,0 10,10 10,10 0,0 0))",
    );
}

#[tokio::test]
async fn box_serialization_keeps_declared_crs_and_null_rows() {
    let context = new_context();
    register_rect(
        &context,
        "boxes",
        BoxType::new(
            Dimension::XY,
            Arc::new(Metadata::new(
                Crs::from_authority_code("EPSG:4326".to_string()),
                None,
            )),
        ),
        vec![
            Some((coord(0.0, 0.0, None, None), coord(10.0, 10.0, None, None))),
            None,
        ],
    );

    let batch = collect(
        &context,
        "SELECT
            encode(ST_AsEWKB(geom), 'hex'),
            ST_AsText(ST_GeomFromWKB(ST_AsBinary(geom))),
            ST_AsText(ST_GeomFromEWKB(ST_AsEWKB(geom)))
        FROM boxes",
    )
    .await;

    let ewkb = batch.column(0).as_string::<i32>().value(0);
    assert!(
        ewkb.starts_with("0103000020e6100000"),
        "EWKB must carry SRID 4326: {ewkb}"
    );
    assert_wkt(
        batch.column(1).as_string::<i32>().value(0),
        "POLYGON((0 0,0 10,10 10,10 0,0 0))",
    );
    assert_wkt(
        batch.column(2).as_string::<i32>().value(0),
        "POLYGON((0 0,0 10,10 10,10 0,0 0))",
    );

    let batch = collect(
        &context,
        "SELECT ST_AsBinary(geom) IS NULL, ST_AsEWKB(geom) IS NULL FROM boxes",
    )
    .await;
    for column in 0..2 {
        let values = batch.column(column).as_boolean();
        assert_eq!(
            (0..values.len())
                .map(|row| values.value(row))
                .collect::<Vec<_>>(),
            vec![false, true]
        );
    }
}

#[tokio::test]
async fn flat_z_box_serializes_as_polygon_z() {
    let context = new_context();
    register_rect(
        &context,
        "boxes",
        default_box_type(Dimension::XYZ),
        vec![Some((
            coord(0.0, 0.0, Some(5.0), None),
            coord(10.0, 10.0, Some(5.0), None),
        ))],
    );

    let batch = collect(
        &context,
        "SELECT
            ST_AsText(ST_GeomFromWKB(ST_AsBinary(geom))),
            ST_AsText(ST_GeomFromEWKB(ST_AsEWKB(geom)))
        FROM boxes",
    )
    .await;
    for column in 0..2 {
        assert_wkt(
            batch.column(column).as_string::<i32>().value(0),
            "POLYGON Z((0 0 5,0 10 5,10 10 5,10 0 5,0 0 5))",
        );
    }
}

#[tokio::test]
async fn full_box3d_is_rejected_instead_of_flattened() {
    let context = new_context();
    register_rect(
        &context,
        "boxes",
        default_box_type(Dimension::XYZ),
        vec![Some((
            coord(0.0, 0.0, Some(0.0), None),
            coord(10.0, 10.0, Some(20.0), None),
        ))],
    );

    for sql in [
        "SELECT ST_AsBinary(geom) FROM boxes",
        "SELECT ST_AsEWKB(geom) FROM boxes",
        "SELECT ST_SnapToGrid(geom, 1) FROM boxes",
    ] {
        let error = collect_error(&context, sql).await;
        assert!(error.contains("constant Z"), "unexpected error: {error}");
    }
}

#[tokio::test]
async fn other_native_outputs_use_the_shared_box_conversion() {
    let context = new_context();
    let box2d = "ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10))";
    let batch = collect(
        &context,
        &format!(
            "SELECT ST_AsGeoJSON({box2d}), ST_AsSVG({box2d}), ST_AsEWKT({box2d}), ST_AsText({box2d})"
        ),
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[0,10],[10,10],[10,0],[0,0]]]}"
    );
    assert_eq!(
        batch.column(1).as_string::<i32>().value(0),
        "M 0 0 L 0 -10 10 -10 10 0 Z"
    );
    for column in 2..4 {
        assert_wkt(
            batch.column(column).as_string::<i32>().value(0),
            "POLYGON((0 0,0 10,10 10,10 0,0 0))",
        );
    }

    let error = collect_error(&context, &format!("SELECT ST_AsEncodedPolyline({box2d})")).await;
    assert!(
        error.contains("Only SRID 4326 is supported."),
        "unexpected error: {error}"
    );

    let context = new_context();
    register_rect(
        &context,
        "boxes",
        BoxType::new(
            Dimension::XY,
            Arc::new(Metadata::new(
                Crs::from_authority_code("EPSG:4326".to_string()),
                None,
            )),
        ),
        vec![Some((
            coord(0.0, 0.0, None, None),
            coord(10.0, 10.0, None, None),
        ))],
    );
    let error = collect_error(&context, "SELECT ST_AsEncodedPolyline(geom) FROM boxes").await;
    assert!(
        error.contains("geometry type not supported"),
        "unexpected error: {error}"
    );
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn geos_uses_the_shared_box_contract() {
    let context = new_context();
    register_rect(
        &context,
        "flat",
        default_box_type(Dimension::XYZ),
        vec![Some((
            coord(0.0, 0.0, Some(5.0), None),
            coord(10.0, 10.0, Some(5.0), None),
        ))],
    );
    let batch = collect(&context, "SELECT ST_AsText(ST_Normalize(geom)) FROM flat").await;
    assert_wkt(
        batch.column(0).as_string::<i32>().value(0),
        "POLYGON Z((0 0 5,0 10 5,10 10 5,10 0 5,0 0 5))",
    );

    let context = new_context();
    register_rect(
        &context,
        "full",
        default_box_type(Dimension::XYZ),
        vec![Some((
            coord(0.0, 0.0, Some(0.0), None),
            coord(10.0, 10.0, Some(20.0), None),
        ))],
    );
    let error = collect_error(&context, "SELECT ST_Normalize(geom) FROM full").await;
    assert!(error.contains("constant Z"), "unexpected error: {error}");
}
