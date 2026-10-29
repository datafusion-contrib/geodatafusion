use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, LineStringTrait, PointTrait, PolygonTrait,
};
use geoarrow_array::array::{
    GeometryArray, GeometryCollectionArray, LineStringArray, MultiLineStringArray, MultiPointArray,
    MultiPolygonArray, PointArray, PolygonArray,
};
use geoarrow_array::builder::{
    GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder,
    MultiPolygonBuilder, PointBuilder, PolygonBuilder,
};
use geoarrow_array::cast::to_wkb;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::type_id::GeometryTypeId;
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeometryCollectionType, GeometryType, LineStringType, Metadata,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType,
};

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

#[derive(Debug, PartialEq)]
enum ObservedPoint {
    Null,
    Empty,
    Coord([f64; 4]),
}

async fn coordinates(ctx: &SessionContext, expression: &str) -> [Option<f64>; 4] {
    let sql = format!(
        "SELECT ST_X(g), ST_Y(g), ST_Z(g), ST_M(g) \
         FROM (SELECT {expression} AS g) q"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    std::array::from_fn(|index| {
        let array = batch.column(index).as_primitive::<Float64Type>();
        (!array.is_null(0)).then(|| array.value(0))
    })
}

async fn xy_coordinates(ctx: &SessionContext, expression: &str) -> [Option<f64>; 2] {
    let sql = format!("SELECT ST_X(g), ST_Y(g) FROM (SELECT {expression} AS g) q");
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    std::array::from_fn(|index| {
        let array = batch.column(index).as_primitive::<Float64Type>();
        (!array.is_null(0)).then(|| array.value(0))
    })
}

async fn scalar_text(ctx: &SessionContext, expression: &str) -> Option<String> {
    let sql = format!("SELECT ST_AsText({expression}) AS text");
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let array = batches[0].column(0).as_string::<i32>();
    (!array.is_null(0)).then(|| array.value(0).to_string())
}

fn assert_coordinates(actual: [Option<f64>; 4], expected: [f64; 4]) {
    for (index, (actual, expected)) in actual.into_iter().zip(expected).enumerate() {
        let actual = actual.unwrap_or_else(|| panic!("coordinate {index} was null"));
        assert!(
            (actual - expected).abs() <= 1e-10,
            "coordinate {index}: expected {expected}, got {actual}"
        );
    }
}

fn assert_xy(actual: [Option<f64>; 2], expected_x: f64, expected_y: f64) {
    assert!((actual[0].unwrap() - expected_x).abs() <= 1e-10);
    assert!((actual[1].unwrap() - expected_y).abs() <= 1e-10);
}

#[tokio::test]
async fn all_eight_functions_follow_postgis_semantics() {
    let ctx = context();
    let cases = [
        (
            "ST_Affine(ST_GeomFromText('POINT ZM (1 2 3 4)'), \
             2,3,4,5,6,7,8,9,10,11,12,13)",
            [31.0, 50.0, 69.0, 4.0],
        ),
        (
            "ST_Rotate(ST_GeomFromText('POINT ZM (1 0 3 4)'), pi() / 2)",
            [0.0, 1.0, 3.0, 4.0],
        ),
        (
            "ST_RotateX(ST_GeomFromText('POINT ZM (1 2 3 4)'), pi() / 2)",
            [1.0, -3.0, 2.0, 4.0],
        ),
        (
            "ST_RotateY(ST_GeomFromText('POINT ZM (1 2 3 4)'), pi() / 2)",
            [3.0, 2.0, -1.0, 4.0],
        ),
        (
            "ST_RotateZ(ST_GeomFromText('POINT ZM (1 2 3 4)'), pi() / 2)",
            [-2.0, 1.0, 3.0, 4.0],
        ),
        (
            "ST_Scale(ST_GeomFromText('POINT ZM (1 2 3 4)'), \
             ST_MakePoint(2, 3, 4, 5))",
            [2.0, 6.0, 12.0, 20.0],
        ),
        (
            "ST_Translate(ST_GeomFromText('POINT ZM (1 2 3 4)'), 5, 12, 3)",
            [6.0, 14.0, 6.0, 4.0],
        ),
        (
            "ST_TransScale(ST_GeomFromText('POINT ZM (1 2 3 4)'), 0.5, 1, 1, 2)",
            [1.5, 6.0, 3.0, 4.0],
        ),
    ];

    for (expression, expected) in cases {
        assert_coordinates(coordinates(&ctx, expression).await, expected);
    }
}

#[tokio::test]
async fn postgis_origin_and_overload_examples() {
    let ctx = context();

    let actual = xy_coordinates(
        &ctx,
        "ST_Rotate(ST_GeomFromText('POINT(1 1)'), pi() / 2, 10, 20)",
    )
    .await;
    assert_xy(actual, 29.0, 11.0);
    let actual = xy_coordinates(
        &ctx,
        "ST_Rotate(ST_GeomFromText('POINT(1 1)'), pi() / 2, \
             ST_GeomFromText('POINT(10 10)'))",
    )
    .await;
    assert_xy(actual, 19.0, 1.0);
    let actual = xy_coordinates(
        &ctx,
        "ST_Scale(ST_GeomFromText('POINT(2 2)'), \
             ST_GeomFromText('POINT(2 2)'), \
             ST_GeomFromText('POINT(1 1)'))",
    )
    .await;
    assert_xy(actual, 3.0, 3.0);
    assert_coordinates(
        coordinates(
            &ctx,
            "ST_Translate(ST_GeomFromText('POINT ZM (0 0 7 11)'), 5, 12, 3)",
        )
        .await,
        [5.0, 12.0, 10.0, 11.0],
    );
    let actual = coordinates(
        &ctx,
        "ST_RotateZ(ST_GeomFromText('POINT ZM (1 1 3 4)'), pi() + pi() / 2)",
    )
    .await;
    assert_coordinates(actual, [1.0, -1.0, 3.0, 4.0]);
}

#[tokio::test]
async fn postgis_scale_and_2d_overloads() {
    let ctx = context();

    let actual = xy_coordinates(
        &ctx,
        "ST_Affine(ST_GeomFromText('POINT(2 3)'), 1,2,4,5,10,11)",
    )
    .await;
    assert_xy(actual, 18.0, 34.0);
    let actual = xy_coordinates(&ctx, "ST_Scale(ST_GeomFromText('POINT(1 1)'), 5, 5)").await;
    assert_xy(actual, 5.0, 5.0);
    let actual = xy_coordinates(
        &ctx,
        "ST_TransScale(ST_GeomFromText('POINT(1 1)'), 2, 3, 5, 7)",
    )
    .await;
    assert_xy(actual, 15.0, 28.0);

    assert_coordinates(
        coordinates(
            &ctx,
            "ST_Scale(ST_GeomFromText('POINT ZM (2 3 4 5)'), 10, 20)",
        )
        .await,
        [20.0, 60.0, 4.0, 5.0],
    );
    assert_coordinates(
        coordinates(
            &ctx,
            "ST_Scale(ST_GeomFromText('POINT ZM (2 3 4 5)'), 10, 20, 30)",
        )
        .await,
        [20.0, 60.0, 120.0, 5.0],
    );
    assert_coordinates(
        coordinates(
            &ctx,
            "ST_Scale(ST_GeomFromText('POINT ZM (2 3 4 5)'), \
             ST_MakePointM(10, 20, 30))",
        )
        .await,
        [20.0, 60.0, 4.0, 150.0],
    );
}

#[tokio::test]
async fn null_empty_and_invalid_arguments() {
    let ctx = context();

    assert_eq!(scalar_text(&ctx, "ST_Translate(NULL, 1, 2)").await, None);
    assert_eq!(
        scalar_text(&ctx, "ST_Rotate(ST_GeomFromText('POINT ZM EMPTY'), pi())",)
            .await
            .as_deref(),
        Some("POINT ZM EMPTY")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_Affine(\
             ST_GeomFromText('GEOMETRYCOLLECTION ZM EMPTY'), \
             1,0,0,0,1,0,0,0,1,1,2,3)",
        )
        .await
        .as_deref(),
        Some("GEOMETRYCOLLECTION ZM EMPTY")
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_Translate(ST_GeomFromText('POINT(1 2)'), CAST(NULL AS DOUBLE), 2)",
        )
        .await,
        None
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_Rotate(ST_GeomFromText('POINT(1 2)'), pi(), \
             ST_GeomFromText('POINT EMPTY'))",
        )
        .await,
        None
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_Scale(ST_GeomFromText('POINT(1 2)'), NULL, \
             ST_GeomFromText('POINT(1 1)'))",
        )
        .await,
        None
    );
    assert_eq!(
        scalar_text(
            &ctx,
            "ST_Scale(ST_GeomFromText('POINT ZM EMPTY'), ST_MakePoint(2, 3, 4, 5))",
        )
        .await
        .as_deref(),
        Some("POINT ZM EMPTY")
    );

    let error = ctx
        .sql(
            "SELECT ST_Rotate(ST_GeomFromText('POINT(1 2)'), pi(), \
             ST_GeomFromText('LINESTRING(0 0,1 1)'))",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("origin must be a POINT"));

    let error = ctx
        .sql(
            "SELECT ST_Scale(ST_GeomFromText('POINT(1 2)'), \
             ST_GeomFromText('LINESTRING(0 0,1 1)'))",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("factor geometry parameter must be a POINT")
    );
    let error = ctx
        .sql(
            "SELECT ST_Scale(ST_GeomFromText('POINT(1 2)'), \
             ST_GeomFromText('POINT EMPTY'))",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("factor point must not be empty"));

    let origins = geoarrow_array::test::point::array(CoordType::Separated, Dimension::XYZM);
    let field = origins.data_type().to_field("geometry", true);
    let schema = Arc::new(Schema::new(vec![Arc::new(field)]));
    let batch = RecordBatch::try_new(schema, vec![origins.to_array_ref()]).unwrap();
    ctx.register_batch("origins", batch).unwrap();
    let batches = ctx
        .sql(
            "SELECT ST_Scale(\
             ST_GeomFromText('POINT ZM (2 2 2 2)'), \
             ST_GeomFromText('POINT ZM (3 3 3 3)'), geometry) AS geometry \
             FROM origins",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let field = batches[0].schema_ref().field(0);
    let output = GeometryArray::try_from((batches[0].column(0).as_ref(), field)).unwrap();
    assert!(!output.is_null(0));
    assert!(!output.is_null(1));
    assert!(output.is_null(2));
    assert!(!output.is_null(3));
}

#[tokio::test]
async fn native_xyz_m_array_preserves_type_crs_and_column_arguments() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4979".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let points = geoarrow_array::test::point::array(CoordType::Separated, Dimension::XYZM)
        .with_metadata(metadata.clone());
    let geometry_field = points.data_type().to_field("geometry", true);
    let schema = Arc::new(Schema::new(vec![
        Arc::new(geometry_field),
        Arc::new(Field::new("dx", DataType::Float64, true)),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            points.to_array_ref(),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
        ],
    )
    .unwrap();
    ctx.register_batch("points", batch).unwrap();

    let batches = ctx
        .sql("SELECT ST_Translate(geometry, dx, 1, 2) AS geometry FROM points")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let field = batch.schema_ref().field(0);
    let point_type = field.try_extension_type::<PointType>().unwrap();
    assert_eq!(point_type.coord_type(), CoordType::Separated);
    assert_eq!(point_type.dimension(), Dimension::XYZM);
    assert_eq!(&crs, point_type.metadata().crs());

    let output = PointArray::try_from((batch.column(0).as_ref(), field)).unwrap();
    assert_eq!(
        observed_point(&output, 0),
        ObservedPoint::Coord([31.0, 11.0, 42.0, 300.0])
    );
    assert_eq!(
        observed_point(&output, 1),
        ObservedPoint::Coord([42.0, 21.0, 62.0, 800.0])
    );
    assert_eq!(observed_point(&output, 2), ObservedPoint::Null);
    assert_eq!(observed_point(&output, 3), ObservedPoint::Empty);
}

#[tokio::test]
async fn native_linestring_xyz_m_preserves_encoded_type() {
    let ctx = context();
    let lines = geoarrow_array::test::linestring::array(CoordType::Interleaved, Dimension::XYZM);
    let field = lines.data_type().to_field("geometry", true);
    let schema = Arc::new(Schema::new(vec![Arc::new(field)]));
    let batch = RecordBatch::try_new(schema, vec![lines.to_array_ref()]).unwrap();
    ctx.register_batch("lines", batch).unwrap();

    let batches = ctx
        .sql("SELECT ST_Translate(geometry, 1, 2, 3) AS geometry FROM lines")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let field = batches[0].schema_ref().field(0);
    let line_type = field.try_extension_type::<LineStringType>().unwrap();
    assert_eq!(line_type.coord_type(), CoordType::Interleaved);
    assert_eq!(line_type.dimension(), Dimension::XYZM);
    let output = LineStringArray::try_from((batches[0].column(0).as_ref(), field)).unwrap();
    assert!(output.is_null(2));
    assert!(output.value(3).unwrap().coords().next().is_none());
    let coords = output
        .value(0)
        .unwrap()
        .coords()
        .map(|coord| coord_values(&coord))
        .collect::<Vec<_>>();
    assert_eq!(
        coords,
        vec![
            [31.0, 12.0, 43.0, 300.0],
            [11.0, 32.0, 43.0, 300.0],
            [41.0, 42.0, 83.0, 1600.0],
        ]
    );
}

#[tokio::test]
async fn sliced_inputs_and_multiple_batches_are_vectorized() {
    let ctx = context();
    let points = geoarrow_array::test::point::array(CoordType::Interleaved, Dimension::XYZM);
    let field = points.data_type().to_field("geometry", true);
    let schema = Arc::new(Schema::new(vec![
        Arc::new(field),
        Arc::new(Field::new("angle", DataType::Float64, true)),
    ]));

    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            points.slice(0, 2).to_array_ref(),
            Arc::new(Float64Array::from(vec![0.0, std::f64::consts::FRAC_PI_2])),
        ],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            points.slice(2, 2).to_array_ref(),
            Arc::new(Float64Array::from(vec![0.0, std::f64::consts::PI])),
        ],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![first, second]]).unwrap();
    ctx.register_table("points", Arc::new(table)).unwrap();

    let batches = ctx
        .sql("SELECT ST_RotateZ(geometry, angle) AS geometry FROM points")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut observed = Vec::new();
    for batch in &batches {
        let field = batch.schema_ref().field(0);
        let point_type = field.try_extension_type::<PointType>().unwrap();
        assert_eq!(point_type.coord_type(), CoordType::Interleaved);
        assert_eq!(point_type.dimension(), Dimension::XYZM);
        let output = PointArray::try_from((batch.column(0).as_ref(), field)).unwrap();
        for index in 0..output.len() {
            observed.push(observed_point(&output, index));
        }
    }

    assert_eq!(observed.len(), 4);
    assert_eq!(observed[0], ObservedPoint::Coord([30.0, 10.0, 40.0, 300.0]));
    match observed[1] {
        ObservedPoint::Coord(coord) => {
            assert_coordinates(
                [
                    Some(coord[0]),
                    Some(coord[1]),
                    Some(coord[2]),
                    Some(coord[3]),
                ],
                [-20.0, 40.0, 60.0, 800.0],
            );
        }
        ref other => panic!("expected a point, got {other:?}"),
    }
    assert_eq!(observed[2], ObservedPoint::Null);
    assert_eq!(observed[3], ObservedPoint::Empty);

    let batches = ctx
        .sql(
            "SELECT ST_Translate(ST_GeomFromText('POINT ZM (0 0 0 7)'), \
             angle, 1, 2) AS geometry FROM points",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut observed = Vec::new();
    for batch in &batches {
        let field = batch.schema_ref().field(0);
        field.try_extension_type::<GeometryType>().unwrap();
        let output = GeometryArray::try_from((batch.column(0).as_ref(), field)).unwrap();
        for index in 0..output.len() {
            observed.push(observed_geometry_point(&output, index));
        }
    }
    let expected = [
        [0.0, 1.0, 2.0, 7.0],
        [std::f64::consts::FRAC_PI_2, 1.0, 2.0, 7.0],
        [0.0, 1.0, 2.0, 7.0],
        [std::f64::consts::PI, 1.0, 2.0, 7.0],
    ];
    for (observed, expected) in observed.into_iter().zip(expected) {
        let ObservedPoint::Coord(coord) = observed else {
            panic!("expected point");
        };
        assert_coordinates(
            [
                Some(coord[0]),
                Some(coord[1]),
                Some(coord[2]),
                Some(coord[3]),
            ],
            expected,
        );
    }
}

#[tokio::test]
async fn wkb_input_preserves_crs_and_xyz_m() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4979".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let points = geoarrow_array::test::point::array(CoordType::Separated, Dimension::XYZM)
        .with_metadata(metadata.clone());
    let wkb = to_wkb::<i32>(&points).unwrap();
    let field =
        Field::new("geometry", DataType::Binary, true).with_extension_type(WkbType::new(metadata));
    let schema = Arc::new(Schema::new(vec![Arc::new(field)]));
    let batch = RecordBatch::try_new(schema, vec![wkb.to_array_ref()]).unwrap();
    ctx.register_batch("wkb_points", batch).unwrap();

    let batches = ctx
        .sql("SELECT ST_Translate(geometry, 1, 2, 3) AS geometry FROM wkb_points")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let field = batches[0].schema_ref().field(0);
    let geometry_type = field.try_extension_type::<GeometryType>().unwrap();
    assert_eq!(&crs, geometry_type.metadata().crs());
    let output = GeometryArray::try_from((batches[0].column(0).as_ref(), field)).unwrap();
    let geometry = output.value(0).unwrap();
    let point = match geometry.as_type() {
        geo_traits::GeometryType::Point(point) => point,
        _ => panic!("expected point geometry"),
    };
    let coord = point.coord().unwrap();
    assert!((coord.x() - 31.0).abs() < 1e-10);
    assert!((coord.y() - 12.0).abs() < 1e-10);
    assert!((coord.nth(2).unwrap() - 43.0).abs() < 1e-10);
    assert!((coord.nth(3).unwrap() - 300.0).abs() < 1e-10);
}

#[tokio::test]
async fn wkt_collections_transform_every_nested_coordinate() {
    let ctx = context();
    let batches = ctx
        .sql(
            "SELECT ST_Affine(\
             ST_GeomFromText('GEOMETRYCOLLECTION ZM (\
               POINT ZM (1 2 3 4),\
               LINESTRING ZM (1 2 3 4, 5 6 7 8),\
               POLYGON ZM ((0 0 0 0, 2 0 0 0, 2 2 0 0, 0 0 0 0))\
             )'), \
             1,0,0,0,1,0,0,0,1,10,20,30) AS geometry",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let field = batches[0].schema_ref().field(0);
    field.try_extension_type::<GeometryType>().unwrap();
    let output = GeometryArray::try_from((batches[0].column(0).as_ref(), field)).unwrap();
    let geometry = output.value(0).unwrap();
    let collection = match geometry.as_type() {
        geo_traits::GeometryType::GeometryCollection(collection) => collection,
        _ => panic!("expected geometry collection"),
    };
    let mut children = collection.geometries();

    let point_geometry = children.next().unwrap();
    let point_values = match point_geometry.as_type() {
        geo_traits::GeometryType::Point(point) => coord_values(&point.coord().unwrap()),
        _ => panic!("expected point"),
    };
    assert_eq!(point_values, [11.0, 22.0, 33.0, 4.0]);

    let line_geometry = children.next().unwrap();
    let line = match line_geometry.as_type() {
        geo_traits::GeometryType::LineString(line) => line,
        _ => panic!("expected line string"),
    };
    let line_values = line
        .coords()
        .map(|coord| coord_values(&coord))
        .collect::<Vec<_>>();
    assert_eq!(
        line_values,
        vec![[11.0, 22.0, 33.0, 4.0], [15.0, 26.0, 37.0, 8.0]]
    );

    let polygon_geometry = children.next().unwrap();
    let polygon = match polygon_geometry.as_type() {
        geo_traits::GeometryType::Polygon(polygon) => polygon,
        _ => panic!("expected polygon"),
    };
    assert_eq!(polygon.num_interiors(), 0);
    let ring_values = polygon
        .exterior()
        .unwrap()
        .coords()
        .map(|coord| coord_values(&coord))
        .collect::<Vec<_>>();
    assert_eq!(
        ring_values,
        vec![
            [10.0, 20.0, 30.0, 0.0],
            [12.0, 20.0, 30.0, 0.0],
            [12.0, 22.0, 30.0, 0.0],
            [10.0, 20.0, 30.0, 0.0],
        ]
    );
    assert!(children.next().is_none());
}

#[tokio::test]
async fn single_member_geometry_collection_identity_is_preserved() {
    let ctx = context();

    let batches = ctx
        .sql(
            "SELECT ST_GeometryType(\
             ST_Translate('GEOMETRYCOLLECTION(POINT(1 2))', 1, 1)) AS geometry_type",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0].column(0).as_string_view().value(0),
        "ST_GeometryCollection"
    );

    let batches = ctx
        .sql(
            "SELECT ST_GeometryType(ST_Translate(column1, 1, 1)) AS geometry_type \
             FROM (VALUES \
               ('GEOMETRYCOLLECTION(POINT(1 2))'), \
               ('POINT(3 4)'), \
               (NULL)\
             )",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let geometry_types = batches[0].column(0).as_string_view();
    assert_eq!(geometry_types.value(0), "ST_GeometryCollection");
    assert_eq!(geometry_types.value(1), "ST_Point");
    assert!(geometry_types.is_null(2));

    let crs = Crs::from_authority_code("EPSG:4979".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let collections = geoarrow_array::test::geometrycollection::array(
        CoordType::Separated,
        Dimension::XYZM,
        false,
    )
    .slice(0, 1)
    .with_metadata(metadata);
    let field = collections.data_type().to_field("geometry", true);
    let schema = Arc::new(Schema::new(vec![Arc::new(field)]));
    let batch = RecordBatch::try_new(schema, vec![collections.to_array_ref()]).unwrap();
    ctx.register_batch("collections", batch).unwrap();

    let batches = ctx
        .sql(
            "SELECT ST_GeometryType(geometry) AS geometry_type, geometry \
             FROM (SELECT ST_Translate(geometry, 1, 1, 1) AS geometry FROM collections) q",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let geometry_types = batches[0].column(0).as_string_view();
    assert_eq!(geometry_types.value(0), "ST_GeometryCollection");

    let field = batches[0].schema_ref().field(1);
    let collection_type = field
        .try_extension_type::<GeometryCollectionType>()
        .unwrap();
    assert_eq!(collection_type.coord_type(), CoordType::Separated);
    assert_eq!(collection_type.dimension(), Dimension::XYZM);
    assert_eq!(&crs, collection_type.metadata().crs());
    let output = GeometryCollectionArray::try_from((batches[0].column(1).as_ref(), field)).unwrap();
    let collection = output.value(0).unwrap();
    assert_eq!(collection.num_geometries(), 1);
    let point_geometry = collection.geometry(0).unwrap();
    let point = match point_geometry.as_type() {
        geo_traits::GeometryType::Point(point) => point,
        _ => panic!("expected point child"),
    };
    assert_eq!(
        coord_values(&point.coord().unwrap()),
        [31.0, 11.0, 41.0, 300.0]
    );
}

#[tokio::test]
async fn strict_generic_builder_handles_mixed_collection_batch() {
    let ctx = context();
    let crs = Crs::from_authority_code("EPSG:4979".to_string());
    let metadata = Arc::new(Metadata::new(crs.clone(), None));
    let coord_type = CoordType::Interleaved;

    let xy_point = wkt::wkt! { POINT (1. 2.) };
    let second_xy_point = wkt::wkt! { POINT (14. 15.) };
    let mut xy_points = PointBuilder::new(
        PointType::new(Dimension::XY, Default::default()).with_coord_type(coord_type),
    );
    xy_points.push_point(Some(&xy_point));
    xy_points.push_point(Some(&second_xy_point));
    xy_points.push_null();
    xy_points.push_empty();

    let xyz_m_point = wkt::wkt! { POINT ZM (5. 6. 7. 8.) };
    let mut xyz_m_points = PointBuilder::new(
        PointType::new(Dimension::XYZM, Default::default()).with_coord_type(coord_type),
    );
    xyz_m_points.push_point(Some(&xyz_m_point));

    let mut points = empty_point_arrays(coord_type);
    points[0] = xy_points.finish();
    points[3] = xyz_m_points.finish();

    let gc_xy = wkt::wkt! { GEOMETRYCOLLECTION (POINT (3. 4.)) };
    let gc_xyz = wkt::wkt! { GEOMETRYCOLLECTION Z (POINT Z (9. 10. 11.)) };
    let gc_xym = wkt::wkt! { GEOMETRYCOLLECTION M (POINT M (12. 13. 100.)) };
    let gc_xyzm = wkt::wkt! { GEOMETRYCOLLECTION ZM (POINT ZM (16. 17. 18. 200.)) };
    let gc_xyzm_empty = wkt::wkt! { GEOMETRYCOLLECTION ZM EMPTY };

    let mut geometry_collections = empty_geometry_collection_arrays(coord_type);
    let mut gc_xy_builder = GeometryCollectionBuilder::new(
        GeometryCollectionType::new(Dimension::XY, Default::default()).with_coord_type(coord_type),
    );
    gc_xy_builder.push_geometry(Some(&gc_xy)).unwrap();
    geometry_collections[0] = gc_xy_builder.finish();

    let mut gc_xyz_builder = GeometryCollectionBuilder::new(
        GeometryCollectionType::new(Dimension::XYZ, Default::default()).with_coord_type(coord_type),
    );
    gc_xyz_builder.push_geometry(Some(&gc_xyz)).unwrap();
    geometry_collections[1] = gc_xyz_builder.finish();

    let mut gc_xym_builder = GeometryCollectionBuilder::new(
        GeometryCollectionType::new(Dimension::XYM, Default::default()).with_coord_type(coord_type),
    );
    gc_xym_builder.push_geometry(Some(&gc_xym)).unwrap();
    geometry_collections[2] = gc_xym_builder.finish();

    let mut gc_xyzm_builder = GeometryCollectionBuilder::new(
        GeometryCollectionType::new(Dimension::XYZM, Default::default())
            .with_coord_type(coord_type),
    );
    gc_xyzm_builder.push_geometry(Some(&gc_xyzm)).unwrap();
    gc_xyzm_builder.push_geometry(Some(&gc_xyzm_empty)).unwrap();
    geometry_collections[3] = gc_xyzm_builder.finish();

    let point_xy_type_id = PointType::new(Dimension::XY, Default::default()).geometry_type_id();
    let point_xyzm_type_id = PointType::new(Dimension::XYZM, Default::default()).geometry_type_id();
    let gc_xy_type_id =
        GeometryCollectionType::new(Dimension::XY, Default::default()).geometry_type_id();
    let gc_xyz_type_id =
        GeometryCollectionType::new(Dimension::XYZ, Default::default()).geometry_type_id();
    let gc_xym_type_id =
        GeometryCollectionType::new(Dimension::XYM, Default::default()).geometry_type_id();
    let gc_xyzm_type_id =
        GeometryCollectionType::new(Dimension::XYZM, Default::default()).geometry_type_id();

    let geometry_array = GeometryArray::new(
        vec![
            point_xy_type_id,
            gc_xy_type_id,
            point_xyzm_type_id,
            gc_xyz_type_id,
            gc_xym_type_id,
            point_xy_type_id,
            gc_xyzm_type_id,
            point_xy_type_id,
            point_xy_type_id,
            gc_xyzm_type_id,
        ]
        .into(),
        vec![0, 0, 0, 0, 0, 1, 0, 2, 3, 1].into(),
        points,
        empty_line_string_arrays(coord_type),
        empty_polygon_arrays(coord_type),
        empty_multi_point_arrays(coord_type),
        empty_multi_line_string_arrays(coord_type),
        empty_multi_polygon_arrays(coord_type),
        geometry_collections,
        metadata,
    );
    let field = geometry_array.data_type().to_field("geometry", true);
    let schema = Arc::new(Schema::new(vec![Arc::new(field)]));
    let batch = RecordBatch::try_new(schema, vec![geometry_array.to_array_ref()]).unwrap();
    ctx.register_batch("mixed_collections", batch).unwrap();

    let batches = ctx
        .sql("SELECT ST_Translate(geometry, 1, 1, 1) AS geometry FROM mixed_collections")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let field = batches[0].schema_ref().field(0);
    let geometry_type = field.try_extension_type::<GeometryType>().unwrap();
    assert_eq!(geometry_type.coord_type(), coord_type);
    assert_eq!(&crs, geometry_type.metadata().crs());
    let output = GeometryArray::try_from((batches[0].column(0).as_ref(), field)).unwrap();

    assert_eq!(geometry_coord_values(&output, 0), vec![2.0, 3.0]);
    assert_eq!(collection_child_coord_values(&output, 1), vec![4.0, 5.0]);
    assert_eq!(geometry_coord_values(&output, 2), vec![6.0, 7.0, 8.0, 8.0]);
    assert_eq!(
        collection_child_coord_values(&output, 3),
        vec![10.0, 11.0, 12.0]
    );
    assert_eq!(
        collection_child_coord_values(&output, 4),
        vec![13.0, 14.0, 100.0]
    );
    assert_eq!(geometry_coord_values(&output, 5), vec![15.0, 16.0]);
    assert_eq!(
        collection_child_coord_values(&output, 6),
        vec![17.0, 18.0, 19.0, 200.0]
    );
    assert!(output.is_null(7));
    assert!(geometry_coord_values(&output, 8).is_empty());
    let empty_collection = output.value(9).unwrap();
    let empty_collection = match empty_collection.as_type() {
        geo_traits::GeometryType::GeometryCollection(collection) => collection,
        _ => panic!("expected geometry collection"),
    };
    assert_eq!(empty_collection.num_geometries(), 0);
}

#[tokio::test]
async fn inverse_and_composition_properties_hold() {
    let ctx = context();
    assert_coordinates(
        coordinates(
            &ctx,
            "ST_RotateX(ST_RotateX(ST_GeomFromText('POINT ZM (1 2 3 4)'), \
             pi() / 3), -pi() / 3)",
        )
        .await,
        [1.0, 2.0, 3.0, 4.0],
    );
    assert_coordinates(
        coordinates(
            &ctx,
            "ST_Translate(ST_Translate(ST_GeomFromText('POINT ZM (1 2 3 4)'), \
             5, -7, 9), -5, 7, -9)",
        )
        .await,
        [1.0, 2.0, 3.0, 4.0],
    );
    assert_coordinates(
        coordinates(
            &ctx,
            "ST_Scale(\
             ST_Scale(ST_GeomFromText('POINT ZM (1 2 3 4)'), \
             ST_MakePoint(2, 3, 4, 5)), \
             ST_MakePoint(0.5, 1.0 / 3.0, 0.25, 0.2))",
        )
        .await,
        [1.0, 2.0, 3.0, 4.0],
    );
    assert_coordinates(
        coordinates(
            &ctx,
            "ST_Scale(ST_GeomFromText('POINT ZM (2 3 4 5)'), \
             ST_MakePoint(2, 3, 4, 5), \
             ST_MakePoint(1, 1, 1, 100))",
        )
        .await,
        [3.0, 7.0, 13.0, 25.0],
    );
}

fn observed_point(array: &PointArray, index: usize) -> ObservedPoint {
    if array.is_null(index) {
        return ObservedPoint::Null;
    }
    let point = array.value(index).unwrap();
    let Some(coord) = point.coord() else {
        return ObservedPoint::Empty;
    };
    ObservedPoint::Coord([
        coord.x(),
        coord.y(),
        coord.nth(2).unwrap(),
        coord.nth(3).unwrap(),
    ])
}

fn coord_values(coord: &impl CoordTrait<T = f64>) -> [f64; 4] {
    [
        coord.x(),
        coord.y(),
        coord.nth(2).unwrap(),
        coord.nth(3).unwrap(),
    ]
}

fn observed_geometry_point(array: &GeometryArray, index: usize) -> ObservedPoint {
    if array.is_null(index) {
        return ObservedPoint::Null;
    }
    let geometry = array.value(index).unwrap();
    match geometry.as_type() {
        geo_traits::GeometryType::Point(point) => match point.coord() {
            Some(coord) => ObservedPoint::Coord(coord_values(&coord)),
            None => ObservedPoint::Empty,
        },
        _ => panic!("expected point geometry"),
    }
}

fn geometry_coord_values(array: &GeometryArray, index: usize) -> Vec<f64> {
    let geometry = array.value(index).unwrap();
    let point = match geometry.as_type() {
        geo_traits::GeometryType::Point(point) => point,
        _ => panic!("expected point geometry"),
    };
    point
        .coord()
        .map(|coord| coord_values_vec(&coord))
        .unwrap_or_default()
}

fn collection_child_coord_values(array: &GeometryArray, index: usize) -> Vec<f64> {
    let geometry = array.value(index).unwrap();
    let collection = match geometry.as_type() {
        geo_traits::GeometryType::GeometryCollection(collection) => collection,
        _ => panic!("expected geometry collection"),
    };
    assert_eq!(collection.num_geometries(), 1);
    let child = collection.geometry(0).unwrap();
    let point = match child.as_type() {
        geo_traits::GeometryType::Point(point) => point,
        _ => panic!("expected point child"),
    };
    coord_values_vec(&point.coord().unwrap())
}

fn coord_values_vec(coord: &impl CoordTrait<T = f64>) -> Vec<f64> {
    match coord.dim() {
        geo_traits::Dimensions::Xy => vec![coord.x(), coord.y()],
        geo_traits::Dimensions::Xyz => vec![coord.x(), coord.y(), coord.nth(2).unwrap()],
        geo_traits::Dimensions::Xym => vec![coord.x(), coord.y(), coord.nth(2).unwrap()],
        geo_traits::Dimensions::Xyzm => vec![
            coord.x(),
            coord.y(),
            coord.nth(2).unwrap(),
            coord.nth(3).unwrap(),
        ],
        geo_traits::Dimensions::Unknown(_) => panic!("unsupported coordinate dimensions"),
    }
}

fn dimensions() -> [Dimension; 4] {
    [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ]
}

fn empty_point_arrays(coord_type: CoordType) -> [PointArray; 4] {
    dimensions().map(|dimension| {
        PointBuilder::new(PointType::new(dimension, Default::default()).with_coord_type(coord_type))
            .finish()
    })
}

fn empty_line_string_arrays(coord_type: CoordType) -> [LineStringArray; 4] {
    dimensions().map(|dimension| {
        LineStringBuilder::new(
            LineStringType::new(dimension, Default::default()).with_coord_type(coord_type),
        )
        .finish()
    })
}

fn empty_polygon_arrays(coord_type: CoordType) -> [PolygonArray; 4] {
    dimensions().map(|dimension| {
        PolygonBuilder::new(
            PolygonType::new(dimension, Default::default()).with_coord_type(coord_type),
        )
        .finish()
    })
}

fn empty_multi_point_arrays(coord_type: CoordType) -> [MultiPointArray; 4] {
    dimensions().map(|dimension| {
        MultiPointBuilder::new(
            MultiPointType::new(dimension, Default::default()).with_coord_type(coord_type),
        )
        .finish()
    })
}

fn empty_multi_line_string_arrays(coord_type: CoordType) -> [MultiLineStringArray; 4] {
    dimensions().map(|dimension| {
        MultiLineStringBuilder::new(
            MultiLineStringType::new(dimension, Default::default()).with_coord_type(coord_type),
        )
        .finish()
    })
}

fn empty_multi_polygon_arrays(coord_type: CoordType) -> [MultiPolygonArray; 4] {
    dimensions().map(|dimension| {
        MultiPolygonBuilder::new(
            MultiPolygonType::new(dimension, Default::default()).with_coord_type(coord_type),
        )
        .finish()
    })
}

fn empty_geometry_collection_arrays(coord_type: CoordType) -> [GeometryCollectionArray; 4] {
    dimensions().map(|dimension| {
        GeometryCollectionBuilder::new(
            GeometryCollectionType::new(dimension, Default::default()).with_coord_type(coord_type),
        )
        .finish()
    })
}
