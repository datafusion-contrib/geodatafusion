//! Public-register Arrow tests for the boundary and predicate accessor batch.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geo_traits::{
    CoordTrait, GeometryTrait, GeometryType, LineStringTrait, MultiPointTrait, PointTrait,
};
use geoarrow_array::array::GeometryArray;
use geoarrow_array::builder::{GeometryBuilder, RectBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{BoxType, Crs, Dimension, GeometryType as GeoArrowGeometryType, Metadata};
use wkt::Wkt;
use wkt::types::{Coord, Dimension as WktDimension, GeometryCollection, MultiPoint, Point};

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

async fn collect_one(ctx: &SessionContext, sql: &str) -> RecordBatch {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan failed for {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("execution failed for {sql}: {error}"))
        .into_iter()
        .next()
        .expect("query returned one batch")
}

fn wkt_batch(name: &str, values: Vec<Option<&str>>) -> RecordBatch {
    let field = Field::new(name, DataType::Utf8, true);
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(StringArray::from(values))],
    )
    .unwrap()
}

fn coord_values(coord: &impl CoordTrait<T = f64>) -> Vec<f64> {
    match coord.dim() {
        geo_traits::Dimensions::Xy => vec![coord.x(), coord.y()],
        geo_traits::Dimensions::Xyz => vec![coord.x(), coord.y(), coord.nth_or_panic(2)],
        geo_traits::Dimensions::Xym => vec![coord.x(), coord.y(), coord.nth_or_panic(2)],
        geo_traits::Dimensions::Xyzm => vec![
            coord.x(),
            coord.y(),
            coord.nth_or_panic(2),
            coord.nth_or_panic(3),
        ],
        geo_traits::Dimensions::Unknown(dim) => panic!("unexpected dimension {dim}"),
    }
}

fn line_string_values(array: &GeometryArray, index: usize) -> Vec<Vec<f64>> {
    let geometry = array.value(index).unwrap();
    let GeometryType::LineString(line_string) = geometry.as_type() else {
        panic!("expected a LineString");
    };
    line_string
        .coords()
        .map(|coord| coord_values(&coord))
        .collect()
}

fn multi_point_values(array: &GeometryArray, index: usize) -> Vec<Option<Vec<f64>>> {
    let geometry = array.value(index).unwrap();
    let GeometryType::MultiPoint(multi_point) = geometry.as_type() else {
        panic!("expected a MultiPoint");
    };
    multi_point
        .points()
        .map(|point| point.coord().map(|coord| coord_values(&coord)))
        .collect()
}

#[tokio::test]
async fn boundary_preserves_dimensions_empty_and_nulls_across_slices() {
    let ctx = context();
    let batch = wkt_batch(
        "wkt",
        vec![
            Some("POINT(9 9)"),
            Some("LINESTRING ZM(1 2 3 9,3 4 5 6)"),
            Some("LINESTRING M(1 2 9,3 4 5)"),
            Some("POLYGON Z((0 0 1,1 0 2,1 1 3,0 0 1))"),
            Some("POINT EMPTY"),
            None,
        ],
    );
    ctx.register_batch("inputs", batch.slice(1, 5)).unwrap();
    let batch = collect_one(
        &ctx,
        "SELECT ST_Boundary(ST_GeomFromText(wkt)) AS boundary FROM inputs",
    )
    .await;
    let schema = batch.schema();
    assert_eq!(
        schema.field(0).extension_type_name(),
        Some("geoarrow.geometry")
    );
    let output = GeometryArray::try_from((batch.column(0).as_ref(), schema.field(0))).unwrap();

    assert_eq!(
        multi_point_values(&output, 0),
        vec![
            Some(vec![1.0, 2.0, 3.0, 9.0]),
            Some(vec![3.0, 4.0, 5.0, 6.0])
        ]
    );
    assert_eq!(
        multi_point_values(&output, 1),
        vec![Some(vec![1.0, 2.0, 9.0]), Some(vec![3.0, 4.0, 5.0])]
    );
    assert_eq!(
        line_string_values(&output, 2),
        vec![
            vec![0.0, 0.0, 1.0],
            vec![1.0, 0.0, 2.0],
            vec![1.0, 1.0, 3.0],
            vec![0.0, 0.0, 1.0],
        ]
    );
    let empty = output.value(3).unwrap();
    assert_eq!(empty.dim(), geo_traits::Dimensions::Xy);
    let GeometryType::Point(point) = empty.as_type() else {
        panic!("expected an empty Point");
    };
    assert!(point.coord().is_none());
    assert!(output.is_null(4));
}

#[tokio::test]
async fn boundary_preserves_crs_metadata() {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        "SELECT ST_Boundary(ST_GeomFromEWKT('SRID=4326;LINESTRING(0 0,1 1)')) AS boundary",
    )
    .await;
    let output_type = batch
        .schema()
        .field(0)
        .try_extension_type::<GeoArrowGeometryType>()
        .unwrap();
    assert_eq!(
        output_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
}

#[tokio::test]
async fn polygon_orientation_ignores_zm_and_handles_nulls() {
    let ctx = context();
    let batch = wkt_batch(
        "wkt",
        vec![
            Some("POINT(9 9)"),
            Some("POLYGON((0 0,1 0,1 1,0 1,0 0))"),
            Some("POLYGON Z((0 0 1,0 1 2,1 1 3,1 0 4,0 0 1))"),
            Some("POLYGON M((0 0 1,1 0 2,1 1 3,0 1 4,0 0 1))"),
            Some("POLYGON ZM((0 0 1 9,1 0 2 8,1 1 3 7,0 1 4 6,0 0 1 9))"),
            Some("POLYGON EMPTY"),
            None,
        ],
    );
    ctx.register_batch("inputs", batch.slice(1, 6)).unwrap();
    let batch = collect_one(
        &ctx,
        "SELECT
            ST_IsPolygonCCW(ST_GeomFromText(wkt)) AS ccw,
            ST_IsPolygonCW(ST_GeomFromText(wkt)) AS cw
         FROM inputs",
    )
    .await;
    let ccw = batch.column(0).as_boolean();
    let cw = batch.column(1).as_boolean();
    assert!(ccw.value(0));
    assert!(!cw.value(0));
    assert!(!ccw.value(1));
    assert!(cw.value(1));
    assert!(ccw.value(2));
    assert!(!cw.value(2));
    assert!(ccw.value(3));
    assert!(!cw.value(3));
    assert!(ccw.value(4));
    assert!(cw.value(4));
    assert!(ccw.is_null(5));
    assert!(cw.is_null(5));
}

#[tokio::test]
async fn line_crossing_direction_dimensions_slices_and_nulls() {
    let ctx = context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let a = StringArray::from(vec![
        Some("LINESTRING(0 0,1 1)"),
        Some("LINESTRING(0 0,2 2)"),
        Some("LINESTRING(0 2,2 0)"),
        Some("LINESTRING Z (0 0 1,2 2 2)"),
        Some("LINESTRING M (0 0 1,10 0 2)"),
        Some("LINESTRING(0 0,1 1)"),
    ]);
    let b = StringArray::from(vec![
        Some("LINESTRING(1 1,2 2)"),
        Some("LINESTRING(0 2,2 0)"),
        Some("LINESTRING(0 0,2 2)"),
        Some("LINESTRING Z (0 2 3,2 0 4)"),
        Some("LINESTRING M (2 -1 0,2 1 0,8 -1 0,8 1 0)"),
        None,
    ]);
    ctx.register_batch(
        "inputs",
        RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)])
            .unwrap()
            .slice(1, 5),
    )
    .unwrap();
    let batch = collect_one(
        &ctx,
        "SELECT ST_LineCrossingDirection(ST_GeomFromText(a), ST_GeomFromText(b)) AS value
         FROM inputs",
    )
    .await;
    let values = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(values.value(0), 1);
    assert_eq!(values.value(1), -1);
    assert_eq!(values.value(2), 1);
    assert_eq!(values.value(3), -2);
    assert!(values.is_null(4));
}

#[tokio::test]
async fn line_crossing_direction_validates_inputs() {
    let ctx = context();
    let error = ctx
        .sql(
            "SELECT ST_LineCrossingDirection(
            ST_GeomFromText('POINT(0 0)'),
            ST_GeomFromText('LINESTRING(0 0,1 1)')
        )",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("only accepts LINESTRING"),
        "unexpected error: {error}"
    );

    let error = ctx
        .sql(
            "SELECT ST_LineCrossingDirection(
                ST_GeomFromEWKT('SRID=4326;LINESTRING(0 0,1 1)'),
                ST_GeomFromEWKT('SRID=3857;LINESTRING(0 1,1 0)')
            )",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("mixed SRID"), "unexpected error: {error}");
}

#[tokio::test]
async fn line_crossing_direction_null_extra_argument_is_sql_null() {
    let ctx = context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, true),
    ]));
    let a = StringArray::from(vec![Some("LINESTRING(0 0,2 2)")]);
    let b = StringArray::from(vec![None::<&str>]);
    ctx.register_batch(
        "inputs",
        RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap(),
    )
    .unwrap();
    let batch = collect_one(
        &ctx,
        "SELECT
            ST_LineCrossingDirection(ST_GeomFromText(a), ST_GeomFromText(b)) IS NULL AS is_null
         FROM inputs",
    )
    .await;
    assert!(batch.column(0).as_boolean().value(0));
}

#[tokio::test]
async fn boundary_rect_2d_returns_a_closed_ring() {
    let ctx = context();
    let box_type = BoxType::new(Dimension::XY, Default::default());
    let mut builder = RectBuilder::with_capacity(box_type.clone(), 1);
    builder.push_min_max(
        &wkt::types::Coord {
            x: 0.0,
            y: 0.0,
            z: None,
            m: None,
        },
        &wkt::types::Coord {
            x: 1.0,
            y: 1.0,
            z: None,
            m: None,
        },
    );
    let array = builder.finish();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![box_type.to_field("geom", true)])),
        vec![array.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("rects", batch).unwrap();
    let batch = collect_one(&ctx, "SELECT ST_AsText(ST_Boundary(geom)) FROM rects").await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "LINESTRING(0 0,1 0,1 1,0 1,0 0)"
    );
}

#[tokio::test]
async fn boundary_rejects_higher_dimensional_rects() {
    let ctx = context();
    let box_type = BoxType::new(Dimension::XYZ, Default::default());
    let mut builder = RectBuilder::with_capacity(box_type.clone(), 1);
    builder.push_min_max(
        &Coord {
            x: 0.0,
            y: 0.0,
            z: Some(1.0),
            m: None,
        },
        &Coord {
            x: 1.0,
            y: 1.0,
            z: Some(2.0),
            m: None,
        },
    );
    let array = builder.finish();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![box_type.to_field("geom", true)])),
        vec![array.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("rects", batch).unwrap();
    let error = ctx
        .sql("SELECT ST_Boundary(geom) FROM rects")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("only 2D Box/Rect inputs"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn astext_handles_empty_multipoint_members_and_boundary_output() {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        "SELECT ST_AsText(ST_Boundary(ST_GeomFromText(\
            'GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,1 1))'\
        )))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "MULTIPOINT(EMPTY,(0 0),(1 1))"
    );
}

#[tokio::test]
async fn astext_keeps_existing_multipoint_format() {
    let ctx = context();
    let batch = collect_one(
        &ctx,
        "SELECT ST_AsText(ST_GeomFromText('MULTIPOINT ZM((1 2 3 4),(5 6 7 8))'))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "MULTIPOINT ZM((1 2 3 4),(5 6 7 8))"
    );
}

#[tokio::test]
async fn astext_handles_native_xyz_m_empty_members_and_collections() {
    let ctx = context();
    let multipoint = MultiPoint::new(
        vec![
            Point::empty(WktDimension::XYZM),
            Point::new(
                Some(Coord {
                    x: 1.0,
                    y: 2.0,
                    z: Some(3.0),
                    m: Some(4.0),
                }),
                WktDimension::XYZM,
            ),
        ],
        WktDimension::XYZM,
    );
    let collection = GeometryCollection::new(
        vec![
            Wkt::MultiPoint(multipoint.clone()),
            Wkt::Point(Point::new(
                Some(Coord {
                    x: 5.0,
                    y: 6.0,
                    z: Some(7.0),
                    m: Some(8.0),
                }),
                WktDimension::XYZM,
            )),
        ],
        WktDimension::XYZM,
    );
    let mut builder = GeometryBuilder::new(
        GeoArrowGeometryType::new(Arc::new(Metadata::default()))
            .with_coord_type(Default::default()),
    );
    builder.push_geometry(Some(&multipoint)).unwrap();
    builder.push_geometry(Some(&collection)).unwrap();
    let array = builder.finish();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            array.data_type().to_field("geometry", true),
        ])),
        vec![array.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("native_geometry", batch).unwrap();

    let batch = collect_one(&ctx, "SELECT ST_AsText(geometry) FROM native_geometry").await;
    let values = batch.column(0).as_string::<i32>();
    assert_eq!(values.value(0), "MULTIPOINT ZM(EMPTY,(1 2 3 4))");
    assert_eq!(
        values.value(1),
        "GEOMETRYCOLLECTION ZM(MULTIPOINT ZM(EMPTY,(1 2 3 4)),POINT ZM(5 6 7 8))"
    );
}

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn ring_and_simple_predicates_handle_dimensions_empty_and_nulls() {
    let ctx = context();
    let batch = wkt_batch(
        "wkt",
        vec![
            Some("POINT(9 9)"),
            Some("LINESTRING(0 0,1 0,1 1,0 0)"),
            Some("LINESTRING M(0 0 1,1 0 2,1 1 3,0 0 1)"),
            Some("LINESTRING ZM(0 0 1 9,1 1 2 8,0 1 3 7,1 0 4 6,0 0 1 9)"),
            Some("LINESTRING EMPTY"),
            None,
        ],
    );
    ctx.register_batch("inputs", batch.slice(1, 5)).unwrap();
    let batch = collect_one(
        &ctx,
        "SELECT
            ST_IsRing(ST_GeomFromText(wkt)) AS is_ring,
            ST_IsSimple(ST_GeomFromText(wkt)) AS is_simple
         FROM inputs",
    )
    .await;
    let is_ring = batch.column(0).as_boolean();
    let is_simple = batch.column(1).as_boolean();
    assert!(is_ring.value(0));
    assert!(is_ring.value(1));
    assert!(!is_ring.value(2));
    assert!(!is_ring.value(3));
    assert!(is_ring.is_null(4));
    assert!(is_simple.value(0));
    assert!(is_simple.value(1));
    assert!(!is_simple.value(2));
    assert!(is_simple.value(3));
    assert!(is_simple.is_null(4));
}
