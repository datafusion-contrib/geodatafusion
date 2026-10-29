//! Release contracts for the geometry attribute accessors.
//!
//! Expected values are the reviewed PostGIS 3.6 baselines checked into
//! `tests/postgis/accessors/{attributes,point_coordinates,start_end,empty_and_closed,counts}`
//! and were also confirmed directly against the pinned PostGIS container.
//! Semantic references are the PostGIS manual and `regress/core/empty.sql`,
//! `regress/core/iscollection.sql`, and `regress/core/regress_ogc.sql` at
//! postgis/postgis commit 80fc3d3.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, UInt8Type, UInt32Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema};
use datafusion::execution::registry::FunctionRegistry;
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::{MultiPointBuilder, PointBuilder};
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeoArrowType, Metadata, MultiPointType, PointType,
};
use wkt::types::{Coord as WktCoord, Dimension as WktDimension, Point as WktPoint};

fn new_ctx() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

async fn query(ctx: &SessionContext, sql: &str) -> RecordBatch {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan failed for {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("execution failed for {sql}: {error}"))
        .into_iter()
        .next()
        .expect("SELECT should return one batch")
}

/// Build the shared `(id, geometry)` VALUES input used by the contract tables.
fn geometry_values(rows: &[(usize, Option<&str>)]) -> String {
    let values = rows
        .iter()
        .map(|(id, wkt)| match wkt {
            Some(wkt) => format!("({id}, '{wkt}')"),
            None => format!("({id}, CAST(NULL AS VARCHAR))"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT id, ST_GeomFromText(wkt) AS g FROM (VALUES {values}) AS inputs(id, wkt)")
}

fn text_value(array: &dyn Array, row: usize) -> Option<String> {
    match array.data_type() {
        DataType::Utf8View => array.as_string_view().value(row).to_string().into(),
        DataType::Utf8 => array.as_string::<i32>().value(row).to_string().into(),
        other => panic!("expected a string column, found {other}"),
    }
}

fn assert_text_column(batch: &RecordBatch, column: usize, expected: &[Option<&str>]) {
    assert_eq!(batch.num_rows(), expected.len(), "row count");
    for (row, want) in expected.iter().enumerate() {
        let array = batch.column(column);
        let got = if array.is_null(row) {
            None
        } else {
            text_value(array.as_ref(), row)
        };
        assert_eq!(
            got.as_deref(),
            *want,
            "column {column} row {row} ({})",
            batch.schema().field(column).name()
        );
    }
}

fn assert_float_column(batch: &RecordBatch, column: usize, expected: &[Option<f64>]) {
    assert_eq!(batch.num_rows(), expected.len(), "row count");
    let array = batch.column(column).as_primitive::<Float64Type>();
    for (row, want) in expected.iter().enumerate() {
        let got = (!array.is_null(row)).then(|| array.value(row));
        assert_eq!(got, *want, "column {column} row {row}");
    }
}

fn assert_u8_column(batch: &RecordBatch, column: usize, expected: &[Option<u8>]) {
    assert_eq!(batch.num_rows(), expected.len(), "row count");
    let array = batch.column(column).as_primitive::<UInt8Type>();
    for (row, want) in expected.iter().enumerate() {
        let got = (!array.is_null(row)).then(|| array.value(row));
        assert_eq!(got, *want, "column {column} row {row}");
    }
}

fn assert_u32_column(batch: &RecordBatch, column: usize, expected: &[Option<u32>]) {
    assert_eq!(batch.num_rows(), expected.len(), "row count");
    let array = batch.column(column).as_primitive::<UInt32Type>();
    for (row, want) in expected.iter().enumerate() {
        let got = (!array.is_null(row)).then(|| array.value(row));
        assert_eq!(got, *want, "column {column} row {row}");
    }
}

fn assert_bool_column(batch: &RecordBatch, column: usize, expected: &[Option<bool>]) {
    assert_eq!(batch.num_rows(), expected.len(), "row count");
    let array = batch.column(column).as_boolean();
    for (row, want) in expected.iter().enumerate() {
        let got = (!array.is_null(row)).then(|| array.value(row));
        assert_eq!(got, *want, "column {column} row {row}");
    }
}

async fn assert_error_contains(ctx: &SessionContext, sql: &str, needle: &str) {
    let message = match ctx.sql(sql).await {
        Ok(dataframe) => dataframe
            .collect()
            .await
            .err()
            .map(|error| error.to_string()),
        Err(error) => Some(error.to_string()),
    };
    let message =
        message.unwrap_or_else(|| panic!("{sql} was expected to fail with `{needle}` but passed"));
    assert!(
        message.contains(needle),
        "{sql} failed with `{message}` instead of containing `{needle}`"
    );
}

const ATTRIBUTES: &[(usize, Option<&str>)] = &[
    (1, Some("POINT(1 2)")),
    (2, Some("POINT Z (1 2 3)")),
    (3, Some("POINT M (1 2 3)")),
    (4, Some("POINT ZM (1 2 3 4)")),
    (5, Some("LINESTRING Z (0 0 1,1 1 2)")),
    (6, Some("LINESTRING M (0 0 1,1 1 2)")),
    (7, Some("MULTIPOINT M ((1 2 3),(3 4 5))")),
    (8, Some("POLYGON M ((0 0 1,4 0 2,4 4 3,0 0 1))")),
    (9, Some("GEOMETRYCOLLECTION Z (POINT Z (1 2 3))")),
    (10, Some("POINT EMPTY")),
    (11, Some("POINT M EMPTY")),
    (12, Some("POINT ZM EMPTY")),
    (13, Some("LINESTRING EMPTY")),
    (14, Some("POLYGON EMPTY")),
    (15, Some("GEOMETRYCOLLECTION EMPTY")),
    (16, None),
];

#[test]
fn public_registry_exposes_attribute_accessors() {
    let ctx = new_ctx();
    let names = ctx.state().udfs();
    for name in [
        "geometrytype",
        "st_geometrytype",
        "st_coorddim",
        "st_ndims",
        "st_isclosed",
        "st_isempty",
        "st_m",
        "st_npoints",
        "st_numinteriorrings",
        "st_numinteriorring",
        "st_numpoints",
        "st_startpoint",
        "st_endpoint",
        "st_x",
        "st_y",
        "st_z",
    ] {
        assert!(
            names.contains(name),
            "missing {name}; registered: {names:#?}"
        );
    }
}

#[tokio::test]
async fn geometry_type_and_dimension_attributes_match_postgis() {
    let ctx = new_ctx();
    let batch = query(
        &ctx,
        &format!(
            "SELECT GeometryType(g), ST_GeometryType(g), ST_CoordDim(g), ST_NDims(g) \
             FROM ({}) AS t ORDER BY id",
            geometry_values(ATTRIBUTES)
        ),
    )
    .await;

    assert_text_column(
        &batch,
        0,
        &[
            Some("POINT"),
            Some("POINT"),
            Some("POINTM"),
            Some("POINT"),
            Some("LINESTRING"),
            Some("LINESTRINGM"),
            Some("MULTIPOINTM"),
            Some("POLYGONM"),
            Some("GEOMETRYCOLLECTION"),
            Some("POINT"),
            Some("POINTM"),
            Some("POINT"),
            Some("LINESTRING"),
            Some("POLYGON"),
            Some("GEOMETRYCOLLECTION"),
            None,
        ],
    );
    assert_text_column(
        &batch,
        1,
        &[
            Some("ST_Point"),
            Some("ST_Point"),
            Some("ST_Point"),
            Some("ST_Point"),
            Some("ST_LineString"),
            Some("ST_LineString"),
            Some("ST_MultiPoint"),
            Some("ST_Polygon"),
            Some("ST_GeometryCollection"),
            Some("ST_Point"),
            Some("ST_Point"),
            Some("ST_Point"),
            Some("ST_LineString"),
            Some("ST_Polygon"),
            Some("ST_GeometryCollection"),
            None,
        ],
    );
    assert_u8_column(
        &batch,
        2,
        &[
            Some(2),
            Some(3),
            Some(3),
            Some(4),
            Some(3),
            Some(3),
            Some(3),
            Some(3),
            Some(3),
            Some(2),
            Some(3),
            Some(4),
            Some(2),
            Some(2),
            Some(2),
            None,
        ],
    );
    assert_u8_column(
        &batch,
        3,
        &[
            Some(2),
            Some(3),
            Some(3),
            Some(4),
            Some(3),
            Some(3),
            Some(3),
            Some(3),
            Some(3),
            Some(2),
            Some(3),
            Some(4),
            Some(2),
            Some(2),
            Some(2),
            None,
        ],
    );
}

#[tokio::test]
async fn point_coordinate_getters_match_postgis() {
    let ctx = new_ctx();
    let batch = query(
        &ctx,
        &format!(
            "SELECT ST_X(g), ST_Y(g), ST_Z(g), ST_M(g) FROM ({}) AS t ORDER BY id",
            geometry_values(&[
                (1, Some("POINT(1 2)")),
                (2, Some("POINT Z (1 2 3)")),
                (3, Some("POINT M (1 2 3)")),
                (4, Some("POINT ZM (1 2 3 4)")),
                (5, Some("POINT EMPTY")),
                (6, Some("POINT M EMPTY")),
                (7, Some("POINT ZM EMPTY")),
                (8, None),
            ])
        ),
    )
    .await;

    assert_float_column(
        &batch,
        0,
        &[
            Some(1.0),
            Some(1.0),
            Some(1.0),
            Some(1.0),
            None,
            None,
            None,
            None,
        ],
    );
    assert_float_column(
        &batch,
        1,
        &[
            Some(2.0),
            Some(2.0),
            Some(2.0),
            Some(2.0),
            None,
            None,
            None,
            None,
        ],
    );
    assert_float_column(
        &batch,
        2,
        &[None, Some(3.0), None, Some(3.0), None, None, None, None],
    );
    assert_float_column(
        &batch,
        3,
        &[None, None, Some(3.0), Some(4.0), None, None, None, None],
    );

    // PostGIS raises `Argument to ST_X() must have type POINT` for other types.
    for function in ["ST_X", "ST_Y", "ST_Z", "ST_M"] {
        for geometry in ["LINESTRING(0 0,1 1)", "POLYGON((0 0,1 0,1 1,0 0))"] {
            assert_error_contains(
                &ctx,
                &format!("SELECT {function}(ST_GeomFromText('{geometry}'))"),
                "must have type POINT",
            )
            .await;
        }
    }
}

#[tokio::test]
async fn start_and_end_point_follow_postgis_element_order() {
    let ctx = new_ctx();
    let batch = query(
        &ctx,
        &format!(
            "SELECT ST_AsText(ST_StartPoint(g)), ST_AsText(ST_EndPoint(g)) \
             FROM ({}) AS t ORDER BY id",
            geometry_values(&[
                (1, Some("POINT(1 2)")),
                (2, Some("LINESTRING(0 0,1 1,2 2)")),
                (3, Some("LINESTRING Z (0 0 1,1 1 2)")),
                (4, Some("LINESTRING M (0 0 1,1 1 2)")),
                (5, Some("LINESTRING ZM (0 0 1 2,1 1 3 4)")),
                (6, Some("POLYGON((0 0,4 0,4 4,0 4,0 0))")),
                (7, Some("MULTIPOINT((1 2),(3 4))")),
                (8, Some("MULTILINESTRING((0 0,1 1),(2 2,3 3))")),
                (9, Some("MULTIPOLYGON(((0 0,1 0,1 1,0 0)))")),
                (
                    10,
                    Some("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))"),
                ),
                (11, Some("POINT EMPTY")),
                (12, Some("LINESTRING EMPTY")),
                (13, Some("MULTIPOINT(EMPTY,(3 4))")),
                (
                    14,
                    Some("GEOMETRYCOLLECTION(POINT EMPTY,LINESTRING(0 0,1 1))"),
                ),
                (15, Some("MULTILINESTRING EMPTY")),
                (16, None),
            ])
        ),
    )
    .await;

    assert_text_column(
        &batch,
        0,
        &[
            Some("POINT(1 2)"),
            Some("POINT(0 0)"),
            Some("POINT Z(0 0 1)"),
            Some("POINT M(0 0 1)"),
            Some("POINT ZM(0 0 1 2)"),
            Some("POINT(0 0)"),
            Some("POINT(1 2)"),
            Some("POINT(0 0)"),
            Some("POINT(0 0)"),
            Some("POINT(1 2)"),
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    );
    assert_text_column(
        &batch,
        1,
        &[
            None,
            Some("POINT(2 2)"),
            Some("POINT Z(1 1 2)"),
            Some("POINT M(1 1 2)"),
            Some("POINT ZM(1 1 3 4)"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    );
}

#[tokio::test]
async fn closure_and_emptiness_match_postgis() {
    let ctx = new_ctx();
    let batch = query(
        &ctx,
        &format!(
            "SELECT ST_IsEmpty(g), ST_IsClosed(g) FROM ({}) AS t ORDER BY id",
            geometry_values(&[
                (1, Some("POINT(0 0)")),
                (2, Some("POINT EMPTY")),
                (3, Some("LINESTRING(0 0,1 1,0 0)")),
                (4, Some("LINESTRING(0 0,1 1)")),
                (5, Some("LINESTRING EMPTY")),
                (6, Some("MULTILINESTRING((0 0,1 1,0 0),(2 2,3 3,2 2))"),),
                (7, Some("MULTILINESTRING((0 0,1 1,0 0),(2 2,3 3,2 3))"),),
                (8, Some("MULTILINESTRING(EMPTY,(2 2,3 3,2 2))")),
                (9, Some("MULTILINESTRING EMPTY")),
                (10, Some("POLYGON((0 0,1 0,1 1,0 0))")),
                (11, Some("POLYGON EMPTY")),
                (12, Some("MULTIPOINT((1 2),(3 4))")),
                (13, Some("MULTIPOINT(EMPTY,(3 4))")),
                (14, Some("MULTIPOINT EMPTY")),
                (15, Some("MULTIPOLYGON(((0 0,1 0,1 1,0 0)))")),
                (16, Some("MULTIPOLYGON(EMPTY,((0 0,1 0,1 1,0 0)))")),
                (17, Some("MULTIPOLYGON EMPTY")),
                (
                    18,
                    Some("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1,0 0))"),
                ),
                (
                    19,
                    Some("GEOMETRYCOLLECTION(POINT EMPTY,LINESTRING(0 0,1 1,0 0))"),
                ),
                (20, Some("GEOMETRYCOLLECTION EMPTY")),
                (21, Some("GEOMETRYCOLLECTION(POINT EMPTY,POLYGON EMPTY)")),
                (22, None),
            ])
        ),
    )
    .await;

    assert_bool_column(
        &batch,
        0,
        &[
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            None,
        ],
    );
    assert_bool_column(
        &batch,
        1,
        &[
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
        ],
    );
}

#[tokio::test]
async fn vertex_and_ring_counts_match_postgis() {
    let ctx = new_ctx();
    let batch = query(
        &ctx,
        &format!(
            "SELECT ST_NPoints(g), ST_NumPoints(g), ST_NumInteriorRings(g), \
             ST_NumInteriorRing(g) FROM ({}) AS t ORDER BY id",
            geometry_values(&[
                (1, Some("POINT(1 2)")),
                (2, Some("POINT EMPTY")),
                (3, Some("LINESTRING(0 0,1 1,2 0)")),
                (4, Some("LINESTRING EMPTY")),
                (5, Some("POLYGON((0 0,4 0,4 4,0 4,0 0))")),
                (
                    6,
                    Some("POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))"),
                ),
                (7, Some("POLYGON EMPTY")),
                (8, Some("MULTIPOINT((1 2),(3 4))")),
                (9, Some("MULTIPOINT(EMPTY,(3 4))")),
                (10, Some("MULTILINESTRING((0 0,1 1),(2 2,3 3))")),
                (11, Some("MULTILINESTRING(EMPTY,(2 2,3 3,2 2))")),
                (12, Some("MULTIPOLYGON(((0 0,1 0,1 1,0 0)))")),
                (
                    13,
                    Some("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))"),
                ),
                (14, Some("GEOMETRYCOLLECTION EMPTY")),
                (15, None),
            ])
        ),
    )
    .await;

    assert_u32_column(
        &batch,
        0,
        &[
            Some(1),
            Some(0),
            Some(3),
            Some(0),
            Some(5),
            Some(10),
            Some(0),
            Some(2),
            Some(1),
            Some(4),
            Some(3),
            Some(4),
            Some(3),
            Some(0),
            None,
        ],
    );
    assert_u32_column(
        &batch,
        1,
        &[
            None,
            None,
            Some(3),
            Some(0),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    );
    assert_u32_column(
        &batch,
        2,
        &[
            None,
            None,
            None,
            None,
            Some(0),
            Some(1),
            Some(0),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    );
    assert_u32_column(
        &batch,
        3,
        &[
            None,
            None,
            None,
            None,
            Some(0),
            Some(1),
            Some(0),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    );
}

#[tokio::test]
async fn native_inputs_preserve_crs_and_declare_nullable_outputs() {
    let ctx = new_ctx();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let point_type =
        PointType::new(Dimension::XYZM, metadata.clone()).with_coord_type(CoordType::Separated);
    // A non-nullable geometry column cannot carry Arrow nulls, so the EMPTY row
    // is the "missing value" case here; a nullable duplicate batch covers NULL.
    let mut points = PointBuilder::with_capacity(point_type.clone(), 2);
    points.push_empty();
    points.push_coord(Some(&WktCoord {
        x: 1.,
        y: 2.,
        z: Some(3.),
        m: Some(4.),
    }));
    let points = points.finish();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(
            point_type.to_field("geometry", false),
        )])),
        vec![points.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("points", batch.clone()).unwrap();
    ctx.register_batch("points_sliced", batch.slice(1, 1))
        .unwrap();

    let mut nullable_points = PointBuilder::with_capacity(point_type.clone(), 1);
    nullable_points.push_null();
    let nullable_points = nullable_points.finish();
    let nullable_batch = RecordBatch::try_new(
        Arc::new(Schema::new([Arc::new(
            point_type.to_field("geometry", true),
        )])),
        vec![nullable_points.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("nullable_points", nullable_batch)
        .unwrap();

    let dataframe = ctx
        .sql(
            "SELECT ST_X(geometry), ST_Y(geometry), ST_Z(geometry), ST_M(geometry), \
             ST_NPoints(geometry), ST_IsEmpty(geometry) FROM points",
        )
        .await
        .unwrap();
    let schema = dataframe.schema().inner().clone();
    for field in schema.fields() {
        assert!(field.is_nullable(), "{} must stay nullable", field.name());
    }
    let rows = dataframe.collect().await.unwrap();
    let batch = &rows[0];
    assert_eq!(batch.num_rows(), 2);
    for column in 0..4 {
        assert!(
            batch.column(column).is_null(0),
            "EMPTY point has no ordinate"
        );
        assert!(!batch.column(column).is_null(1));
    }
    assert_eq!(batch.column(2).as_primitive::<Float64Type>().value(1), 3.0);
    assert_eq!(batch.column(3).as_primitive::<Float64Type>().value(1), 4.0);
    assert_eq!(batch.column(4).as_primitive::<UInt32Type>().value(1), 1);
    assert!(batch.column(5).as_boolean().value(0));

    // A nullable geometry column propagates SQL NULL through every accessor.
    let batch = query(
        &ctx,
        "SELECT ST_X(geometry), ST_NPoints(geometry), ST_IsEmpty(geometry) FROM nullable_points",
    )
    .await;
    for column in 0..3 {
        assert!(batch.column(column).is_null(0));
    }

    let dataframe = ctx
        .sql("SELECT ST_StartPoint(geometry), ST_EndPoint(geometry) FROM points")
        .await
        .unwrap();
    let schema = dataframe.schema().inner().clone();
    let start_type = GeoArrowType::from_extension_field(schema.field(0))
        .unwrap()
        .unwrap();
    assert_eq!(start_type.metadata().as_ref(), metadata.as_ref());
    assert!(schema.field(0).is_nullable());
    let rows = dataframe.collect().await.unwrap();
    // Geometry results are union-encoded, so Arrow-level `is_null` cannot see
    // SQL NULL; the SQL contract is asserted through the text rendering.
    assert_eq!(rows[0].num_rows(), 2);
    let text = query(
        &ctx,
        "SELECT ST_AsText(ST_StartPoint(geometry)), ST_AsText(ST_EndPoint(geometry)) FROM points",
    )
    .await;
    assert_text_column(&text, 0, &[None, Some("POINT ZM(1 2 3 4)")]);
    assert_text_column(&text, 1, &[None, None]);

    // Sliced arrays keep the same contract as the whole batch.
    let batch = query(
        &ctx,
        "SELECT ST_X(geometry), ST_NPoints(geometry) FROM points_sliced",
    )
    .await;
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 1.0);
    assert_eq!(batch.column(1).as_primitive::<UInt32Type>().value(0), 1);
}

#[tokio::test]
async fn collection_index_getters_return_nullable_fields() {
    let ctx = new_ctx();
    let multipoint_type = MultiPointType::new(Dimension::XY, Arc::new(Metadata::default()))
        .with_coord_type(CoordType::Separated);
    let point = |x, y| {
        WktPoint::from_coord(WktCoord {
            x,
            y,
            z: None,
            m: None,
        })
    };
    let multipoints = [
        wkt::types::MultiPoint::new(vec![point(1., 2.), point(3., 4.)], WktDimension::XY),
        wkt::types::MultiPoint::new(vec![point(5., 5.)], WktDimension::XY),
        wkt::types::MultiPoint::new(vec![point(7., 7.), point(8., 8.)], WktDimension::XY),
    ];
    let multipoint =
        MultiPointBuilder::from_multi_points(&multipoints, multipoint_type.clone()).finish();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new([
            Arc::new(multipoint_type.to_field("geometry", false)),
            Arc::new(arrow_schema::Field::new("index", DataType::Int64, true)),
        ])),
        vec![
            multipoint.into_array_ref(),
            Arc::new(arrow_array::Int64Array::from(vec![Some(2), Some(3), None])),
        ],
    )
    .unwrap();
    ctx.register_batch("multi", batch).unwrap();

    let dataframe = ctx
        .sql(
            "SELECT ST_AsText(ST_GeometryN(geometry, index)), \
             ST_AsText(ST_PointN(geometry, index)), \
             ST_AsText(ST_InteriorRingN(geometry, index)) FROM multi",
        )
        .await
        .unwrap();
    let schema = dataframe.schema().inner().clone();
    for field in schema.fields() {
        assert!(field.is_nullable(), "{} must stay nullable", field.name());
    }
    let rows = dataframe.collect().await.unwrap();
    let batch = &rows[0];
    assert_eq!(batch.column(0).as_string::<i32>().value(0), "POINT(3 4)");
    assert!(
        batch.column(0).is_null(1),
        "out-of-range index returns NULL"
    );
    assert!(batch.column(0).is_null(2), "NULL index returns NULL");
    for column in 1..3 {
        for row in 0..3 {
            assert!(
                batch.column(column).is_null(row),
                "ST_PointN/ST_InteriorRingN only accept line string and polygon input"
            );
        }
    }
}
