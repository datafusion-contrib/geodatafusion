use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    ArrayRef, Float64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use arrow_schema::{Field, Schema};
use datafusion::prelude::SessionContext;
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, LineStringTrait, PointTrait,
    RectTrait,
};
use geoarrow_array::array::{
    GeometryArray, GeometryCollectionArray, LargeWktArray, PointArray, RectArray, WktArray,
    WktViewArray,
};
use geoarrow_array::builder::{
    GeometryBuilder, GeometryCollectionBuilder, PointBuilder, RectBuilder,
};
use geoarrow_array::cast::{from_wkt, to_wkb, to_wkb_view};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{
    BoxType, CoordType, Crs, Dimension, GeoArrowType, GeometryCollectionType, GeometryType,
    LineStringType, Metadata, MultiLineStringType, MultiPointType, MultiPolygonType, PointType,
    PolygonType,
};
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

fn parse_wkt(value: &str) -> Wkt<f64> {
    Wkt::from_str(value).unwrap()
}

fn register_geometry_batch(context: &SessionContext, array: Arc<dyn GeoArrowArray>) -> Field {
    let field = array.data_type().to_field("geom", true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field.clone()])),
        vec![array.to_array_ref()],
    )
    .unwrap();
    context.register_batch("t", batch).unwrap();
    field
}

async fn collect(context: &SessionContext, sql: &str) -> RecordBatch {
    let dataframe = context.sql(sql).await.unwrap();
    let batches = dataframe.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    batches.into_iter().next().unwrap()
}

fn assert_wkt(actual: &str, expected: &str) {
    let actual_text = actual;
    let expected_text = expected;
    let actual: Wkt<f64> = Wkt::from_str(actual)
        .unwrap_or_else(|error| panic!("failed to parse actual WKT {actual:?}: {error}"));
    let expected: Wkt<f64> = Wkt::from_str(expected)
        .unwrap_or_else(|error| panic!("failed to parse expected WKT {expected:?}: {error}"));
    assert_eq!(
        actual, expected,
        "actual={actual_text:?} expected={expected_text:?}"
    );
}

#[tokio::test]
async fn force_functions_match_postgis_defaults_in_sql() {
    let context = new_context();
    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "wkt",
                    arrow_schema::DataType::Utf8,
                    false,
                )])),
                vec![Arc::new(StringArray::from(vec![
                    "POINT(1 2)",
                    "POINT Z (3 4 5)",
                    "POINT M (6 7 8)",
                    "POINT ZM (9 10 11 12)",
                ])) as ArrayRef],
            )
            .unwrap(),
        )
        .unwrap();

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_Force2D(ST_GeomFromText(wkt))) FROM input",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_wkt(values.value(0), "POINT(1 2)");
    assert_wkt(values.value(1), "POINT(3 4)");
    assert_wkt(values.value(2), "POINT(6 7)");
    assert_wkt(values.value(3), "POINT(9 10)");

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_Force3D(ST_GeomFromText(wkt))) FROM input",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_wkt(values.value(0), "POINT Z (1 2 0)");
    assert_wkt(values.value(1), "POINT Z (3 4 5)");
    assert_wkt(values.value(2), "POINT Z (6 7 0)");
    assert_wkt(values.value(3), "POINT Z (9 10 11)");

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_Force3DZ(ST_GeomFromText(wkt), 20.0)) FROM input",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_wkt(values.value(0), "POINT Z (1 2 20)");
    assert_wkt(values.value(1), "POINT Z (3 4 5)");
    assert_wkt(values.value(2), "POINT Z (6 7 20)");
    assert_wkt(values.value(3), "POINT Z (9 10 11)");

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_Force3DM(ST_GeomFromText(wkt))) FROM input",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_wkt(values.value(0), "POINT M (1 2 0)");
    assert_wkt(values.value(1), "POINT M (3 4 0)");
    assert_wkt(values.value(2), "POINT M (6 7 8)");
    assert_wkt(values.value(3), "POINT M (9 10 12)");

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_Force3DM(ST_GeomFromText(wkt), 30.0)) FROM input",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_wkt(values.value(0), "POINT M (1 2 30)");
    assert_wkt(values.value(1), "POINT M (3 4 30)");
    assert_wkt(values.value(2), "POINT M (6 7 8)");
    assert_wkt(values.value(3), "POINT M (9 10 12)");

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_Force4D(ST_GeomFromText(wkt), 40.0, 50.0)) FROM input",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_wkt(values.value(0), "POINT ZM (1 2 40 50)");
    assert_wkt(values.value(1), "POINT ZM (3 4 5 50)");
    assert_wkt(values.value(2), "POINT ZM (6 7 40 8)");
    assert_wkt(values.value(3), "POINT ZM (9 10 11 12)");

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_Force4D(ST_GeomFromText(wkt))) FROM input",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_wkt(values.value(0), "POINT ZM (1 2 0 0)");
    assert_wkt(values.value(1), "POINT ZM (3 4 5 0)");
    assert_wkt(values.value(2), "POINT ZM (6 7 0 8)");
    assert_wkt(values.value(3), "POINT ZM (9 10 11 12)");
}

#[tokio::test]
async fn flip_and_swap_preserve_z_and_m_in_sql() {
    let context = new_context();
    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("wkt", arrow_schema::DataType::Utf8, false),
                    Field::new("ords", arrow_schema::DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec![
                        "POINT(1 2)",
                        "POINT Z (3 4 5)",
                        "POINT M (6 7 8)",
                        "POINT ZM (9 10 11 12)",
                    ])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["xy", "xy", "xy", "xy"])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_FlipCoordinates(ST_GeomFromText(wkt))) FROM input",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_wkt(values.value(0), "POINT(2 1)");
    assert_wkt(values.value(1), "POINT Z (4 3 5)");
    assert_wkt(values.value(2), "POINT M (7 6 8)");
    assert_wkt(values.value(3), "POINT ZM (10 9 11 12)");

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_FlipCoordinates(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 1,1 0))')))",
    )
    .await;
    assert_wkt(
        batch.column(0).as_string::<i32>().value(0),
        "GEOMETRYCOLLECTION(POINT(2 1),LINESTRING(1 0,0 1))",
    );

    context
        .register_batch(
            "swaps",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("wkt", arrow_schema::DataType::Utf8, false),
                    Field::new("ords", arrow_schema::DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec![
                        "POINT ZM (1 2 3 4)",
                        "POINT ZM (5 6 7 8)",
                        "POINT ZM (9 10 11 12)",
                        "POINT M (0 1 2)",
                    ])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["xy", "Xz", "mm", "my"])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_SwapOrdinates(ST_GeomFromText(wkt), ords)) FROM swaps",
    )
    .await;
    let values = batch.column(0).as_string::<i32>();
    assert_wkt(values.value(0), "POINT ZM (2 1 3 4)");
    assert_wkt(values.value(1), "POINT ZM (7 6 5 8)");
    assert_wkt(values.value(2), "POINT ZM (9 10 11 12)");
    assert_wkt(values.value(3), "POINT M (0 2 1)");

    let dimensions = collect(
        &context,
        "SELECT ST_NDims(ST_SwapOrdinates(ST_GeomFromText(wkt), ords)) FROM swaps",
    )
    .await;
    assert_eq!(
        dimensions
            .column(0)
            .as_primitive::<arrow_array::types::UInt8Type>()
            .values(),
        &[4, 4, 4, 3]
    );

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_SwapOrdinates(ST_GeomFromText('POINT ZM (1 2 3 4)'), 'xy'))",
    )
    .await;
    assert_wkt(
        batch.column(0).as_string::<i32>().value(0),
        "POINT ZM (2 1 3 4)",
    );
}

#[tokio::test]
async fn swap_validates_ordinate_specs_and_dimensions() {
    let context = new_context();

    for (ords, expected) in [
        ("", "Invalid ordinate specification"),
        ("ab", "Invalid ordinate name 'a'"),
        ("x y", "Invalid ordinate specification"),
    ] {
        let error = context
            .sql(&format!(
                "SELECT ST_SwapOrdinates(ST_GeomFromText('POINT ZM (1 2 3 4)'), '{ords}')"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
    }

    let error = context
        .sql("SELECT ST_SwapOrdinates(ST_GeomFromText('POINT(1 2)'), 'xz')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("Geometry does not have a Z ordinate"),
        "{error}"
    );

    let error = context
        .sql("SELECT ST_SwapOrdinates(ST_GeomFromText('POINT Z (1 2 3)'), 'xm')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("Geometry does not have an M ordinate"),
        "{error}"
    );
}

#[tokio::test]
async fn mixed_native_rows_preserve_dimensions_crs_and_coord_type() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let context = new_context();
        let geometry_type = GeometryType::new(metadata()).with_coord_type(coord_type);
        let mut builder = GeometryBuilder::new(geometry_type.clone());
        let geometries = [
            parse_wkt("POINT M (1 2 3)"),
            parse_wkt("LINESTRING Z (10 20 30,40 50 60)"),
            parse_wkt("POLYGON M ((0 0 1,0 5 2,5 5 3,0 0 1))"),
        ];
        for geometry in &geometries {
            builder.push_geometry(Some(geometry)).unwrap();
        }
        builder.push_geometry(None::<&Wkt<f64>>).unwrap();
        let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
        register_geometry_batch(&context, array);

        let batch = collect(&context, "SELECT ST_FlipCoordinates(geom) FROM t").await;
        let output_field = batch.schema_ref().field(0);
        assert_eq!(
            output_field
                .try_extension_type::<GeometryType>()
                .unwrap()
                .coord_type(),
            coord_type
        );
        assert_eq!(
            output_field
                .try_extension_type::<GeometryType>()
                .unwrap()
                .metadata()
                .crs(),
            metadata().crs()
        );
        let output = GeometryArray::try_from((batch.column(0).as_ref(), output_field)).unwrap();
        assert_eq!(output.len(), 4);

        let point = output.value(0).unwrap();
        let geo_traits::GeometryType::Point(point) = point.as_type() else {
            panic!("expected a point");
        };
        let coord = point.coord().unwrap();
        assert_eq!(coord.dim(), Dimensions::Xym);
        assert_eq!((coord.x(), coord.y(), coord.nth(2)), (2.0, 1.0, Some(3.0)));

        let line = output.value(1).unwrap();
        let geo_traits::GeometryType::LineString(line) = line.as_type() else {
            panic!("expected a line string");
        };
        let coord = line.coords().next().unwrap();
        assert_eq!(coord.dim(), Dimensions::Xyz);
        assert_eq!(
            (coord.x(), coord.y(), coord.nth(2)),
            (20.0, 10.0, Some(30.0))
        );

        let polygon = output.value(2).unwrap();
        let geo_traits::GeometryType::Polygon(polygon) = polygon.as_type() else {
            panic!("expected a polygon");
        };
        assert_eq!(polygon.dim(), Dimensions::Xym);
        assert!(output.is_null(3));

        let batch = collect(&context, "SELECT ST_Force4D(geom, 9.0, 8.0) FROM t").await;
        let output_field = batch.schema_ref().field(0);
        let output = GeometryArray::try_from((batch.column(0).as_ref(), output_field)).unwrap();
        let point = output.value(0).unwrap();
        let geo_traits::GeometryType::Point(point) = point.as_type() else {
            panic!("expected a point");
        };
        let coord = point.coord().unwrap();
        assert_eq!(coord.dim(), Dimensions::Xyzm);
        assert_eq!(
            (coord.x(), coord.y(), coord.nth(2), coord.nth(3)),
            (1.0, 2.0, Some(9.0), Some(3.0))
        );

        let line = output.value(1).unwrap();
        let geo_traits::GeometryType::LineString(line) = line.as_type() else {
            panic!("expected a line string");
        };
        let coord = line.coords().next().unwrap();
        assert_eq!(
            (coord.x(), coord.y(), coord.nth(2), coord.nth(3)),
            (10.0, 20.0, Some(30.0), Some(8.0))
        );
        assert!(output.is_null(3));
    }
}

#[tokio::test]
async fn single_member_geometry_collection_identity_is_preserved() {
    let context = new_context();
    let collection = parse_wkt("GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4))");
    let mut builder =
        GeometryCollectionBuilder::new(GeometryCollectionType::new(Dimension::XYZM, metadata()));
    builder.push_geometry(Some(&collection)).unwrap();
    let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
    let field = register_geometry_batch(&context, array);
    assert_eq!(
        field
            .try_extension_type::<GeometryCollectionType>()
            .unwrap()
            .dimension(),
        Dimension::XYZM
    );

    let batch = collect(&context, "SELECT ST_Force3DM(geom, 8.0) FROM t").await;
    let output_field = batch.schema_ref().field(0);
    let output_type = output_field
        .try_extension_type::<GeometryCollectionType>()
        .unwrap();
    assert_eq!(output_type.dimension(), Dimension::XYM);
    assert_eq!(output_type.metadata().crs(), metadata().crs());

    let output =
        GeometryCollectionArray::try_from((batch.column(0).as_ref(), output_field)).unwrap();
    let collection = output.value(0).unwrap();
    assert_eq!(collection.num_geometries(), 1);
    let child = collection.geometry(0).unwrap();
    assert_eq!(child.dim(), Dimensions::Xym);
    let geo_traits::GeometryType::Point(point) = child.as_type() else {
        panic!("expected a point child");
    };
    let coord = point.coord().unwrap();
    assert_eq!((coord.x(), coord.y(), coord.nth(2)), (1.0, 2.0, Some(4.0)));
}

#[tokio::test]
async fn typed_empty_and_crs_survive_coordinate_layout_changes() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let context = new_context();
        let point_type = PointType::new(Dimension::XYZ, metadata()).with_coord_type(coord_type);
        let mut builder = PointBuilder::with_capacity(point_type.clone(), 1);
        builder.push_coord(Some(&Coord {
            x: 1.0,
            y: 2.0,
            z: Some(3.0),
            m: None,
        }));
        let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
        register_geometry_batch(&context, array);

        let batch = collect(&context, "SELECT ST_Force3DM(geom, 8.0) FROM t").await;
        let field = batch.schema_ref().field(0);
        let output_type = field.try_extension_type::<PointType>().unwrap();
        assert_eq!(output_type.dimension(), Dimension::XYM);
        assert_eq!(output_type.coord_type(), coord_type);
        assert_eq!(output_type.metadata().crs(), metadata().crs());

        let output = PointArray::try_from((batch.column(0).as_ref(), field)).unwrap();
        let coord = output.value(0).unwrap().coord().unwrap();
        assert_eq!(coord.dim(), Dimensions::Xym);
        assert_eq!((coord.x(), coord.y(), coord.nth(2)), (1.0, 2.0, Some(8.0)));
    }

    let context = new_context();
    let point_type =
        PointType::new(Dimension::XYZM, metadata()).with_coord_type(CoordType::Separated);
    let mut builder = PointBuilder::with_capacity(point_type, 1);
    builder.push_empty();
    let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
    register_geometry_batch(&context, array);

    for (sql, expected_dimension) in [
        ("SELECT ST_FlipCoordinates(geom) FROM t", Dimension::XYZM),
        ("SELECT ST_Force2D(geom) FROM t", Dimension::XY),
        ("SELECT ST_Force3D(geom) FROM t", Dimension::XYZ),
        ("SELECT ST_Force3DZ(geom) FROM t", Dimension::XYZ),
        ("SELECT ST_Force3DM(geom) FROM t", Dimension::XYM),
        ("SELECT ST_Force4D(geom) FROM t", Dimension::XYZM),
    ] {
        let batch = collect(&context, sql).await;
        let field = batch.schema_ref().field(0);
        let output_type = field.try_extension_type::<PointType>().unwrap();
        assert_eq!(output_type.dimension(), expected_dimension);
        let output = PointArray::try_from((batch.column(0).as_ref(), field)).unwrap();
        assert!(output.value(0).unwrap().coord().is_none());
    }
}

#[tokio::test]
async fn box_inputs_keep_box_encoding_and_metadata() {
    let context = new_context();
    let box_type = BoxType::new(Dimension::XYM, metadata());
    let mut builder = RectBuilder::with_capacity(box_type, 1);
    builder.push_min_max(
        &Coord {
            x: 1.0,
            y: 2.0,
            z: None,
            m: Some(3.0),
        },
        &Coord {
            x: 4.0,
            y: 5.0,
            z: None,
            m: Some(6.0),
        },
    );
    let array: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
    register_geometry_batch(&context, array);

    let batch = collect(&context, "SELECT ST_FlipCoordinates(geom) FROM t").await;
    let field = batch.schema_ref().field(0);
    let output_type = field.try_extension_type::<BoxType>().unwrap();
    assert_eq!(output_type.dimension(), Dimension::XYM);
    assert_eq!(output_type.metadata().crs(), metadata().crs());
    let output = RectArray::try_from((batch.column(0).as_ref(), field)).unwrap();
    let rect = output.value(0).unwrap();
    assert_eq!(
        (rect.min().x(), rect.min().y(), rect.min().nth(2)),
        (2.0, 1.0, Some(3.0))
    );
    assert_eq!(
        (rect.max().x(), rect.max().y(), rect.max().nth(2)),
        (5.0, 4.0, Some(6.0))
    );

    let batch = collect(&context, "SELECT ST_Force2D(geom) FROM t").await;
    let field = batch.schema_ref().field(0);
    let output_type = field.try_extension_type::<BoxType>().unwrap();
    assert_eq!(output_type.dimension(), Dimension::XY);
    let output = RectArray::try_from((batch.column(0).as_ref(), field)).unwrap();
    let rect = output.value(0).unwrap();
    assert_eq!(rect.dim(), Dimensions::Xy);
}

#[tokio::test]
async fn all_native_fixed_geometry_array_types_are_editable() {
    let types = [
        (
            GeoArrowType::Point(PointType::new(Dimension::XYM, metadata())),
            "POINT M (1 2 3)",
        ),
        (
            GeoArrowType::LineString(LineStringType::new(Dimension::XYM, metadata())),
            "LINESTRING M (0 0 1,1 1 2)",
        ),
        (
            GeoArrowType::Polygon(PolygonType::new(Dimension::XYM, metadata())),
            "POLYGON M ((0 0 1,0 1 2,1 1 3,0 0 1))",
        ),
        (
            GeoArrowType::MultiPoint(MultiPointType::new(Dimension::XYM, metadata())),
            "MULTIPOINT M ((0 0 1),(1 1 2))",
        ),
        (
            GeoArrowType::MultiLineString(MultiLineStringType::new(Dimension::XYM, metadata())),
            "MULTILINESTRING M ((0 0 1,1 1 2))",
        ),
        (
            GeoArrowType::MultiPolygon(MultiPolygonType::new(Dimension::XYM, metadata())),
            "MULTIPOLYGON M (((0 0 1,0 1 2,1 1 3,0 0 1)))",
        ),
        (
            GeoArrowType::GeometryCollection(GeometryCollectionType::new(
                Dimension::XYM,
                metadata(),
            )),
            "GEOMETRYCOLLECTION M (POINT M (1 2 3),LINESTRING M (0 0 1,1 1 2))",
        ),
    ];

    for (geometry_type, wkt) in types {
        let context = new_context();
        let wkt = WktArray::new(StringArray::from(vec![wkt]), metadata());
        let array = from_wkt(&wkt, geometry_type.clone()).unwrap();
        register_geometry_batch(&context, array);

        let batch = collect(&context, "SELECT ST_Force4D(geom, 9.0, 8.0) FROM t").await;
        let output_type = GeoArrowType::from_arrow_field(batch.schema_ref().field(0)).unwrap();
        assert_eq!(output_type.dimension(), Some(Dimension::XYZM));
        assert_eq!(
            std::mem::discriminant(&output_type),
            std::mem::discriminant(&geometry_type)
        );
        assert_eq!(output_type.metadata().crs(), metadata().crs());
    }
}

#[tokio::test]
async fn null_and_broadcast_arguments_are_strict_per_row() {
    let context = new_context();
    let point_type = PointType::new(Dimension::XY, metadata());
    let mut builder = PointBuilder::with_capacity(point_type, 3);
    builder.push_coord(Some(&Coord {
        x: 1.0,
        y: 2.0,
        z: None,
        m: None,
    }));
    builder.push_null();
    builder.push_coord(Some(&Coord {
        x: 3.0,
        y: 4.0,
        z: None,
        m: None,
    }));
    let geometry: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
    let z = Arc::new(Float64Array::from(vec![Some(10.0), Some(20.0), None])) as ArrayRef;
    let schema = Schema::new(vec![
        geometry.data_type().to_field("geom", true),
        Field::new("z", arrow_schema::DataType::Float64, true),
    ]);
    context
        .register_batch(
            "t",
            RecordBatch::try_new(Arc::new(schema), vec![geometry.to_array_ref(), z]).unwrap(),
        )
        .unwrap();

    let batch = collect(&context, "SELECT ST_Force3D(geom, z) FROM t").await;
    let field = batch.schema_ref().field(0);
    let output = PointArray::try_from((batch.column(0).as_ref(), field)).unwrap();
    assert_eq!(output.value(0).unwrap().coord().unwrap().nth(2), Some(10.0));
    assert!(output.is_null(1));
    assert!(output.is_null(2));

    let sliced = output.slice(1, 2);
    assert_eq!(sliced.len(), 2);
    assert!(sliced.is_null(0));
    assert!(sliced.is_null(1));

    let context = new_context();
    let point_type = PointType::new(Dimension::XYZM, metadata());
    let mut builder = PointBuilder::with_capacity(point_type, 2);
    builder.push_null();
    builder.push_coord(Some(&Coord {
        x: 1.0,
        y: 2.0,
        z: Some(3.0),
        m: Some(4.0),
    }));
    let geometry: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
    let ordinates = Arc::new(StringArray::from(vec![Some("xy"), None])) as ArrayRef;
    let schema = Schema::new(vec![
        geometry.data_type().to_field("geom", true),
        Field::new("ords", arrow_schema::DataType::Utf8, true),
    ]);
    context
        .register_batch(
            "s",
            RecordBatch::try_new(Arc::new(schema), vec![geometry.to_array_ref(), ordinates])
                .unwrap(),
        )
        .unwrap();

    let batch = collect(&context, "SELECT ST_SwapOrdinates(geom, ords) FROM s").await;
    let field = batch.schema_ref().field(0);
    let output = PointArray::try_from((batch.column(0).as_ref(), field)).unwrap();
    assert!(output.is_null(0));
    assert!(output.is_null(1));
}

#[tokio::test]
async fn public_null_arguments_follow_strict_editor_contracts() {
    let context = new_context();
    let point_type = PointType::new(Dimension::XY, metadata());
    let mut builder = PointBuilder::with_capacity(point_type, 1);
    builder.push_coord(Some(&Coord {
        x: 1.0,
        y: 2.0,
        z: None,
        m: None,
    }));
    let geometry: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
    let schema = Schema::new(vec![geometry.data_type().to_field("geom", false)]);
    context
        .register_batch(
            "nn",
            RecordBatch::try_new(Arc::new(schema), vec![geometry.to_array_ref()]).unwrap(),
        )
        .unwrap();

    for expression in [
        "ST_FlipCoordinates(NULL)",
        "ST_Force2D(NULL)",
        "ST_Force3D(NULL)",
        "ST_Force3D(NULL, NULL)",
        "ST_Force3DZ(NULL)",
        "ST_Force3DM(NULL)",
        "ST_Force4D(NULL)",
        "ST_Force4D(NULL, NULL, NULL)",
        "ST_Force4D(NULL, 1, NULL)",
        "ST_SwapOrdinates(NULL, 'xy')",
        "ST_SwapOrdinates(NULL, NULL)",
        "ST_SwapOrdinates(NULL, 'xq')",
    ] {
        let batch = collect(&context, &format!("SELECT {expression} IS NULL")).await;
        assert!(
            batch.column(0).as_boolean().value(0),
            "{expression} is not NULL"
        );
    }

    for expression in [
        "ST_Force3D(geom, CAST(NULL AS DOUBLE))",
        "ST_Force3DZ(geom, CAST(NULL AS DOUBLE))",
        "ST_Force3DM(geom, CAST(NULL AS DOUBLE))",
        "ST_Force4D(geom, CAST(NULL AS DOUBLE), 0)",
        "ST_Force4D(geom, 0, CAST(NULL AS DOUBLE))",
        // DataFusion Utf8 stands in for the PostGIS cstring ordinate argument;
        // PG only accepts an untyped NULL there, so this typed case is DataFusion-only.
        "ST_SwapOrdinates(geom, CAST(NULL AS VARCHAR))",
        "ST_SwapOrdinates(geom, NULL)",
    ] {
        let frame = context
            .sql(&format!("SELECT {expression} AS out FROM nn"))
            .await
            .unwrap();
        let batches = frame.collect().await.unwrap();
        let batch = &batches[0];
        let field = batch.schema_ref().field(0);
        assert!(field.is_nullable(), "{expression} output is not nullable");
        let output = PointArray::try_from((batch.column(0).as_ref(), field)).unwrap();
        assert!(output.is_null(0), "{expression} did not return NULL");
    }

    let frame = context
        .sql("SELECT ST_Force3D(geom) AS out FROM nn")
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    let batch = &batches[0];
    let field = batch.schema_ref().field(0);
    assert!(
        !field.is_nullable(),
        "non-null input produced nullable output"
    );
    let output = PointArray::try_from((batch.column(0).as_ref(), field)).unwrap();
    assert!(!output.is_null(0));

    let error = context
        .sql("SELECT ST_SwapOrdinates(geom, 'xq') FROM nn")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Invalid ordinate name 'q'"), "{error}");
}

#[tokio::test]
async fn sliced_native_inputs_are_evaluated_from_their_logical_range() {
    let context = new_context();
    let point_type = PointType::new(Dimension::XY, metadata());
    let mut builder = PointBuilder::with_capacity(point_type, 4);
    for (x, y) in [(0.0, 1.0), (2.0, 3.0), (4.0, 5.0), (6.0, 7.0)] {
        builder.push_coord(Some(&Coord {
            x,
            y,
            z: None,
            m: None,
        }));
    }
    let geometry: Arc<dyn GeoArrowArray> = Arc::new(builder.finish().slice(1, 2));
    let z = Arc::new(Float64Array::from(vec![10.0, 20.0])) as ArrayRef;
    let schema = Schema::new(vec![
        geometry.data_type().to_field("geom", true),
        Field::new("z", arrow_schema::DataType::Float64, false),
    ]);
    context
        .register_batch(
            "t",
            RecordBatch::try_new(Arc::new(schema), vec![geometry.to_array_ref(), z]).unwrap(),
        )
        .unwrap();

    let batch = collect(&context, "SELECT ST_Force3D(geom, z) FROM t").await;
    let field = batch.schema_ref().field(0);
    let output = PointArray::try_from((batch.column(0).as_ref(), field)).unwrap();
    let first = output.value(0).unwrap();
    let first = first.coord().unwrap();
    assert_eq!((first.x(), first.y(), first.nth(2)), (2.0, 3.0, Some(10.0)));
    let second = output.value(1).unwrap();
    let second = second.coord().unwrap();
    assert_eq!(
        (second.x(), second.y(), second.nth(2)),
        (4.0, 5.0, Some(20.0))
    );
}

#[tokio::test]
async fn wkt_and_wkb_arrays_are_supported_inputs() {
    let context = new_context();
    let metadata = metadata();
    let wkt = WktArray::new(
        StringArray::from(vec!["POINT M (1 2 3)", "LINESTRING Z (4 5 6,7 8 9)"]),
        metadata.clone(),
    );
    let wkt_array: Arc<dyn GeoArrowArray> = Arc::new(wkt.clone());
    register_geometry_batch(&context, wkt_array);

    let batch = collect(&context, "SELECT ST_FlipCoordinates(geom) FROM t").await;
    assert_eq!(
        batch
            .schema_ref()
            .field(0)
            .try_extension_type::<GeometryType>()
            .unwrap()
            .metadata()
            .crs(),
        metadata.crs()
    );
    let output =
        GeometryArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0))).unwrap();
    assert_eq!(output.len(), 2);
    let geometry = output.value(0).unwrap();
    let geo_traits::GeometryType::Point(point) = geometry.as_type() else {
        panic!("expected a point");
    };
    assert_eq!(point.coord().unwrap().dim(), Dimensions::Xym);

    let wkb_context = new_context();
    let wkb = to_wkb::<i32>(&wkt).unwrap();
    let wkb_array: Arc<dyn GeoArrowArray> = Arc::new(wkb);
    register_geometry_batch(&wkb_context, wkb_array);

    let batch = collect(&wkb_context, "SELECT ST_Force2D(geom) FROM t").await;
    assert_eq!(
        batch
            .schema_ref()
            .field(0)
            .try_extension_type::<GeometryType>()
            .unwrap()
            .metadata()
            .crs(),
        metadata.crs()
    );
    let output =
        GeometryArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0))).unwrap();
    assert_eq!(output.value(0).unwrap().dim(), Dimensions::Xy);
    assert_eq!(output.value(1).unwrap().dim(), Dimensions::Xy);
}

#[tokio::test]
async fn all_wkt_and_wkb_storage_layouts_are_supported_inputs() {
    let metadata = metadata();
    let values = vec!["POINT M (1 2 3)", "LINESTRING Z (4 5 6,7 8 9)"];
    let wkt = WktArray::new(StringArray::from(values.clone()), metadata.clone());
    let inputs: Vec<(&str, Arc<dyn GeoArrowArray>)> = vec![
        ("wkt", Arc::new(wkt.clone())),
        (
            "large_wkt",
            Arc::new(LargeWktArray::new(
                LargeStringArray::from(values.clone()),
                metadata.clone(),
            )),
        ),
        (
            "wkt_view",
            Arc::new(WktViewArray::new(
                StringViewArray::from(values.clone()),
                metadata.clone(),
            )),
        ),
        ("wkb", Arc::new(to_wkb::<i32>(&wkt).unwrap())),
        ("large_wkb", Arc::new(to_wkb::<i64>(&wkt).unwrap())),
        ("wkb_view", Arc::new(to_wkb_view(&wkt).unwrap())),
    ];

    for (name, input) in inputs {
        let context = new_context();
        register_geometry_batch(&context, input);
        let batch = collect(&context, "SELECT ST_FlipCoordinates(geom) FROM t").await;
        let output =
            GeometryArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0)))
                .unwrap();
        let geometry = output.value(0).unwrap();
        let geo_traits::GeometryType::Point(point) = geometry.as_type() else {
            panic!("{name}: expected a point");
        };
        let coord = point.coord().unwrap();
        assert_eq!(
            (coord.x(), coord.y(), coord.nth(2)),
            (2.0, 1.0, Some(3.0)),
            "{name}"
        );
    }
}

#[tokio::test]
async fn malformed_wkt_fails_at_the_editor_boundary() {
    let context = new_context();
    let wkt = WktArray::new(StringArray::from(vec!["POINT Z (1"]), metadata());
    let array: Arc<dyn GeoArrowArray> = Arc::new(wkt);
    register_geometry_batch(&context, array);

    let error = context
        .sql("SELECT ST_FlipCoordinates(geom) FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.to_lowercase().contains("wkt"), "{error}");
}

#[tokio::test]
async fn existing_editors_preserve_gc1_and_rows_through_geom_from_text() {
    let context = new_context();
    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", arrow_schema::DataType::Int32, false),
                    Field::new("wkt", arrow_schema::DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        "GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4))",
                        "POINT M (10 20 30)",
                        "LINESTRING Z (0 2 1,3 4 2)",
                    ])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();

    let batch = collect(
        &context,
        r#"
        SELECT
            ST_AsText(ST_FlipCoordinates(g)) AS flip,
            ST_AsText(ST_Force2D(g)) AS force_2d,
            ST_AsText(ST_Force3D(g)) AS force_3d,
            ST_AsText(ST_Force3DZ(g)) AS force_3dz,
            ST_AsText(ST_Force3DM(g)) AS force_3dm,
            ST_AsText(ST_Force4D(g)) AS force_4d,
            ST_AsText(ST_SwapOrdinates(g, 'xy')) AS swap
        FROM (
            SELECT id, ST_GeomFromText(wkt) AS g
            FROM input
        )
        ORDER BY id
        "#,
    )
    .await;

    for (row, expected) in [
        (
            0,
            [
                "GEOMETRYCOLLECTION ZM (POINT ZM (2 1 3 4))",
                "GEOMETRYCOLLECTION (POINT (1 2))",
                "GEOMETRYCOLLECTION Z (POINT Z (1 2 3))",
                "GEOMETRYCOLLECTION Z (POINT Z (1 2 3))",
                "GEOMETRYCOLLECTION M (POINT M (1 2 4))",
                "GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4))",
                "GEOMETRYCOLLECTION ZM (POINT ZM (2 1 3 4))",
            ],
        ),
        (
            1,
            [
                "POINT M (20 10 30)",
                "POINT(10 20)",
                "POINT Z (10 20 0)",
                "POINT Z (10 20 0)",
                "POINT M (10 20 30)",
                "POINT ZM (10 20 0 30)",
                "POINT M (20 10 30)",
            ],
        ),
        (
            2,
            [
                "LINESTRING Z (2 0 1,4 3 2)",
                "LINESTRING(0 2,3 4)",
                "LINESTRING Z (0 2 1,3 4 2)",
                "LINESTRING Z (0 2 1,3 4 2)",
                "LINESTRING M (0 2 0,3 4 0)",
                "LINESTRING ZM (0 2 1 0,3 4 2 0)",
                "LINESTRING Z (2 0 1,4 3 2)",
            ],
        ),
    ] {
        for (column, expected_wkt) in expected.into_iter().enumerate() {
            assert_wkt(
                batch.column(column).as_string::<i32>().value(row),
                expected_wkt,
            );
        }
    }
}
