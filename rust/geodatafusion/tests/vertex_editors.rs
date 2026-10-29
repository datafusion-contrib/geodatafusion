use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, Int32Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait};
use geoarrow_array::array::{
    GeometryArray, LargeWktArray, LineStringArray, RectArray, WktArray, WktViewArray,
};
use geoarrow_array::builder::{LineStringBuilder, PointBuilder, RectBuilder};
use geoarrow_array::cast::{from_wkt, to_wkb, to_wkb_view};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeoArrowType, GeometryCollectionType, LineStringType, Metadata,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
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

fn string_column(batch: &RecordBatch, index: usize) -> &arrow_array::StringArray {
    batch.column(index).as_string::<i32>()
}

fn assert_column_wkt(batch: &RecordBatch, index: usize, expected: &str) {
    let values = string_column(batch, index);
    assert!(!values.is_null(0), "column {index} is unexpectedly NULL");
    assert_wkt(values.value(0), expected);
}

fn assert_geometry_null(batch: &RecordBatch, column: usize, row: usize) {
    let geometry = GeometryArray::try_from((
        batch.column(column).as_ref(),
        batch.schema_ref().field(column),
    ))
    .unwrap();
    assert!(
        geometry.is_null(row),
        "column {column} row {row} is not NULL"
    );
}

#[tokio::test]
async fn add_point_uses_zero_based_positions_and_vector_arguments() {
    let context = new_context();
    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("line", DataType::Utf8, true),
                    Field::new("point", DataType::Utf8, true),
                    Field::new("position", DataType::Int32, true),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        Some("LINESTRING(0 0,1 1,3 3,4 4)"),
                        Some("LINESTRING(0 0,1 1,3 3,4 4)"),
                        Some("LINESTRING(0 0,1 1,3 3,4 4)"),
                        Some("LINESTRING EMPTY"),
                        Some("LINESTRING(0 0,1 1)"),
                        Some("LINESTRING Z (0 0 1,1 1 1)"),
                    ])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        Some("POINT(2 2)"),
                        Some("POINT(2 2)"),
                        Some("POINT(2 2)"),
                        Some("POINT(2 2)"),
                        Some("POINT EMPTY"),
                        Some("POINT M (2 2 9)"),
                    ])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![
                        Some(0),
                        Some(1),
                        Some(4),
                        Some(0),
                        Some(5),
                        Some(2),
                    ])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();

    let batch = collect(
        &context,
        r#"
        SELECT ST_AsText(ST_AddPoint(ST_GeomFromText(line), ST_GeomFromText(point), position))
        FROM input
        ORDER BY id
        "#,
    )
    .await;
    let values = string_column(&batch, 0);
    assert_wkt(values.value(0), "LINESTRING(2 2,0 0,1 1,3 3,4 4)");
    assert_wkt(values.value(1), "LINESTRING(0 0,2 2,1 1,3 3,4 4)");
    assert_wkt(values.value(2), "LINESTRING(0 0,1 1,3 3,4 4,2 2)");
    assert_wkt(values.value(3), "LINESTRING(2 2)");
    assert_wkt(values.value(4), "LINESTRING(0 0,1 1)");
    assert_wkt(values.value(5), "LINESTRING Z (0 0 1,1 1 1,2 2 0)");

    let batch = collect(
        &context,
        r#"
        SELECT
            ST_AsText(ST_AddPoint(ST_GeomFromText('LINESTRING(0 0,1 1)'), ST_GeomFromText('POINT(2 2)'))) AS default_append,
            ST_AsText(ST_AddPoint(ST_GeomFromText('LINESTRING(0 0,1 1)'), ST_GeomFromText('POINT(2 2)'), -1)) AS negative_one,
            ST_AsText(ST_AddPoint(ST_GeomFromText('LINESTRING M (0 0 1,1 1 1)'), ST_GeomFromText('POINT(2 2)'))) AS missing_m,
            ST_AsText(ST_AddPoint(ST_GeomFromText('LINESTRING ZM (0 0 1 2,1 1 3 4)'), ST_GeomFromText('POINT Z (2 2 9)'))) AS missing_m_zero,
            ST_AsText(ST_AddPoint(ST_GeomFromText('LINESTRING(0 0,1 1)'), ST_GeomFromText('POINT ZM (2 2 9 8)'))) AS truncate_zm
        "#,
    )
    .await;
    assert_column_wkt(&batch, 0, "LINESTRING(0 0,1 1,2 2)");
    assert_column_wkt(&batch, 1, "LINESTRING(0 0,1 1,2 2)");
    assert_column_wkt(&batch, 2, "LINESTRING M (0 0 1,1 1 1,2 2 0)");
    assert_column_wkt(&batch, 3, "LINESTRING ZM (0 0 1 2,1 1 3 4,2 2 9 0)");
    assert_column_wkt(&batch, 4, "LINESTRING(0 0,1 1,2 2)");
}

#[tokio::test]
async fn add_point_rejects_invalid_offsets_and_geometry_types() {
    let context = new_context();
    let cases = [
        (
            "SELECT ST_AddPoint(ST_GeomFromText('LINESTRING(0 0,1 1,3 3,4 4)'), ST_GeomFromText('POINT(2 2)'), 5)",
            "LWGEOM_addpoint: Invalid offset",
        ),
        (
            "SELECT ST_AddPoint(ST_GeomFromText('LINESTRING(0 0,1 1,3 3,4 4)'), ST_GeomFromText('POINT(2 2)'), -2)",
            "LWGEOM_addpoint: Invalid offset",
        ),
        (
            "SELECT ST_AddPoint(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(2 2)'))",
            "First argument must be a LINESTRING",
        ),
        (
            "SELECT ST_AddPoint(ST_GeomFromText('LINESTRING(0 0,1 1)'), ST_GeomFromText('MULTIPOINT((2 2))'))",
            "Second argument must be a POINT",
        ),
    ];
    for (sql, expected) in cases {
        let error = collect_error(&context, sql).await;
        assert!(error.contains(expected), "{error}");
    }
}

#[tokio::test]
async fn remove_point_handles_bounds_dimensions_and_null() {
    let context = new_context();
    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("wkt", DataType::Utf8, false),
                    Field::new("offset", DataType::Int32, true),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        "LINESTRING(0 0,1 1,2 2)",
                        "LINESTRING(0 0,1 1,2 2)",
                        "LINESTRING Z (0 0 0,1 1 1,2 2 2)",
                        "LINESTRING M (0 0 0,1 1 1,2 2 2)",
                        "LINESTRING ZM (0 0 0 0,1 1 1 1,2 2 2 2,3 3 3 3,4 4 4 4)",
                        "LINESTRING(0 0,1 1,2 2)",
                    ])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![
                        Some(0),
                        Some(2),
                        Some(0),
                        Some(2),
                        Some(2),
                        None,
                    ])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_RemovePoint(ST_GeomFromText(wkt), offset)) FROM input ORDER BY id",
    )
    .await;
    let values = string_column(&batch, 0);
    assert_wkt(values.value(0), "LINESTRING(1 1,2 2)");
    assert_wkt(values.value(1), "LINESTRING(0 0,1 1)");
    assert_wkt(values.value(2), "LINESTRING Z (1 1 1,2 2 2)");
    assert_wkt(values.value(3), "LINESTRING M (0 0 0,1 1 1)");
    assert_wkt(
        values.value(4),
        "LINESTRING ZM (0 0 0 0,1 1 1 1,3 3 3 3,4 4 4 4)",
    );
    assert!(values.is_null(5));

    for (sql, expected) in [
        (
            "SELECT ST_RemovePoint(ST_GeomFromText('LINESTRING(0 0,1 1)'), 0)",
            "Can't remove points from a single segment line",
        ),
        (
            "SELECT ST_RemovePoint(ST_GeomFromText('LINESTRING EMPTY'), 0)",
            "Can't remove points from a single segment line",
        ),
        (
            "SELECT ST_RemovePoint(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'), 3)",
            "Point index out of range (0..2)",
        ),
        (
            "SELECT ST_RemovePoint(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'), -1)",
            "Point index out of range (0..2)",
        ),
        (
            "SELECT ST_RemovePoint(ST_GeomFromText('POINT(0 0)'), 0)",
            "First argument must be a LINESTRING",
        ),
    ] {
        let error = collect_error(&context, sql).await;
        assert!(error.contains(expected), "{error}");
    }
}

#[tokio::test]
async fn set_point_supports_negative_indexes_and_dimension_adaptation() {
    let context = new_context();
    let batch = collect(
        &context,
        r#"
        SELECT
            ST_AsText(ST_SetPoint(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'), -1, ST_GeomFromText('POINT(9 9)'))) AS negative,
            ST_AsText(ST_SetPoint(ST_GeomFromText('LINESTRING(0 0,1 1,2 2,3 3,4 4)'), -3, ST_GeomFromText('POINT(9 9)'))) AS negative_middle,
            ST_AsText(ST_SetPoint(ST_GeomFromText('LINESTRING ZM (0 0 0 0,1 1 1 1,2 2 2 2,4 4 4 4)'), 3, ST_GeomFromText('POINT(90 91)'))) AS xy,
            ST_AsText(ST_SetPoint(ST_GeomFromText('LINESTRING ZM (0 0 0 0,1 1 1 1,2 2 2 2,4 4 4 4)'), 2, ST_GeomFromText('POINT Z (90 91 92)'))) AS xyz,
            ST_AsText(ST_SetPoint(ST_GeomFromText('LINESTRING ZM (0 0 0 0,1 1 1 1,2 2 2 2,4 4 4 4)'), 1, ST_GeomFromText('POINT M (90 91 92)'))) AS xym,
            ST_AsText(ST_SetPoint(ST_GeomFromText('LINESTRING ZM (0 0 0 0,1 1 1 1,2 2 2 2,4 4 4 4)'), 0, ST_GeomFromText('POINT ZM (90 91 92 93)'))) AS xyzm,
            ST_AsText(ST_SetPoint(ST_GeomFromText('LINESTRING Z (0 0 0,1 1 1)'), 0, ST_GeomFromText('POINT M (90 91 92)'))) AS truncate_m,
            ST_AsText(ST_SetPoint(ST_GeomFromText('LINESTRING M (0 0 0,1 1 1)'), 0, ST_GeomFromText('POINT Z (90 91 92)'))) AS truncate_z
        "#,
    )
    .await;
    assert_column_wkt(&batch, 0, "LINESTRING(0 0,1 1,9 9)");
    assert_column_wkt(&batch, 1, "LINESTRING(0 0,1 1,9 9,3 3,4 4)");
    assert_column_wkt(
        &batch,
        2,
        "LINESTRING ZM (0 0 0 0,1 1 1 1,2 2 2 2,90 91 0 0)",
    );
    assert_column_wkt(
        &batch,
        3,
        "LINESTRING ZM (0 0 0 0,1 1 1 1,90 91 92 0,4 4 4 4)",
    );
    assert_column_wkt(
        &batch,
        4,
        "LINESTRING ZM (0 0 0 0,90 91 0 92,2 2 2 2,4 4 4 4)",
    );
    assert_column_wkt(
        &batch,
        5,
        "LINESTRING ZM (90 91 92 93,1 1 1 1,2 2 2 2,4 4 4 4)",
    );
    assert_column_wkt(&batch, 6, "LINESTRING Z (90 91 0,1 1 1)");
    assert_column_wkt(&batch, 7, "LINESTRING M (90 91 0,1 1 1)");

    for (sql, expected) in [
        (
            "SELECT ST_SetPoint(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'), 3, ST_GeomFromText('POINT(9 9)'))",
            "abs(Point index) out of range (-)(0..2)",
        ),
        (
            "SELECT ST_SetPoint(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'), -10, ST_GeomFromText('POINT(9 9)'))",
            "abs(Point index) out of range (-)(0..2)",
        ),
        (
            "SELECT ST_SetPoint(ST_GeomFromText('LINESTRING EMPTY'), 1, ST_GeomFromText('POINT(40 50)'))",
            "Line has no points",
        ),
        (
            "SELECT ST_SetPoint(ST_GeomFromText('POINT(0 0)'), 0, ST_GeomFromText('POINT(9 9)'))",
            "First argument must be a LINESTRING",
        ),
        (
            "SELECT ST_SetPoint(ST_GeomFromText('LINESTRING(0 0,1 1)'), 0, ST_GeomFromText('MULTIPOINT((9 9))'))",
            "Third argument must be a POINT",
        ),
        (
            "SELECT ST_SetPoint(ST_GeomFromText('LINESTRING(0 0,1 1)'), 0, ST_GeomFromText('POINT EMPTY'))",
            "non-empty point",
        ),
    ] {
        let error = collect_error(&context, sql).await;
        assert!(error.contains(expected), "{error}");
    }
}

#[tokio::test]
async fn set_point_supports_vector_arguments() {
    let context = new_context();
    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("line", DataType::Utf8, false),
                    Field::new("index", DataType::Int32, false),
                    Field::new("point", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        "LINESTRING(0 0,1 1,2 2)",
                        "LINESTRING Z (0 0 1,1 1 2)",
                        "LINESTRING M (0 0 1,1 1 2)",
                    ])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![0, -1, 1])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        "POINT(9 9)",
                        "POINT Z (8 8 7)",
                        "POINT M (6 6 5)",
                    ])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_SetPoint(ST_GeomFromText(line), index, ST_GeomFromText(point))) FROM input ORDER BY id",
    )
    .await;
    let values = string_column(&batch, 0);
    assert_wkt(values.value(0), "LINESTRING(9 9,1 1,2 2)");
    assert_wkt(values.value(1), "LINESTRING Z (0 0 1,8 8 7)");
    assert_wkt(values.value(2), "LINESTRING M (0 0 1,6 6 5)");
}

#[tokio::test]
async fn reverse_preserves_collection_order_and_reverses_vertex_sequences() {
    let context = new_context();
    let batch = collect(
        &context,
        r#"
        SELECT
            ST_AsText(ST_Reverse(ST_GeomFromText('POINT M (1 2 3)'))) AS point,
            ST_AsText(ST_Reverse(ST_GeomFromText('MULTIPOINT M ((0 0 1),(1 1 2))'))) AS multipoint,
            ST_AsText(ST_Reverse(ST_GeomFromText('LINESTRING Z (0 0 1,1 1 2)'))) AS line,
            ST_AsText(ST_Reverse(ST_GeomFromText('MULTILINESTRING Z ((0 0 1,1 1 2),(2 2 3,3 3 4))'))) AS multiline,
            ST_AsText(ST_Reverse(ST_GeomFromText('POLYGON Z ((0 0 1,8 0 2,8 8 3,0 8 4,0 0 1),(2 2 5,2 4 6,4 4 7,4 2 8,2 2 5))'))) AS polygon,
            ST_AsText(ST_Reverse(ST_GeomFromText('MULTIPOLYGON M (((0 0 1,5 0 2,5 5 3,0 5 4,0 0 1)),((6 6 2,7 6 4,7 7 6,6 6 2)))'))) AS multipolygon,
            ST_AsText(ST_Reverse(ST_GeomFromText('GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4),MULTILINESTRING ZM ((0 0 1 2,1 1 3 4)))'))) AS collection,
            ST_AsText(ST_Reverse(ST_GeomFromText('GEOMETRYCOLLECTION Z EMPTY'))) AS empty,
            ST_AsText(ST_Reverse(ST_GeomFromText(NULL))) AS null_value
        "#,
    )
    .await;
    assert_column_wkt(&batch, 0, "POINT M (1 2 3)");
    assert_column_wkt(&batch, 1, "MULTIPOINT M ((0 0 1),(1 1 2))");
    assert_column_wkt(&batch, 2, "LINESTRING Z (1 1 2,0 0 1)");
    assert_column_wkt(&batch, 3, "MULTILINESTRING Z ((1 1 2,0 0 1),(3 3 4,2 2 3))");
    assert_column_wkt(
        &batch,
        4,
        "POLYGON Z ((0 0 1,0 8 4,8 8 3,8 0 2,0 0 1),(2 2 5,4 2 8,4 4 7,2 4 6,2 2 5))",
    );
    assert_column_wkt(
        &batch,
        5,
        "MULTIPOLYGON M (((0 0 1,0 5 4,5 5 3,5 0 2,0 0 1)),((6 6 2,7 7 6,7 6 4,6 6 2)))",
    );
    assert_column_wkt(
        &batch,
        6,
        "GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4),MULTILINESTRING ZM ((1 1 3 4,0 0 1 2)))",
    );
    assert_column_wkt(&batch, 7, "GEOMETRYCOLLECTION Z EMPTY");
    assert!(string_column(&batch, 8).is_null(0));
}

#[tokio::test]
async fn reverse_supports_multiple_table_rows() {
    let context = new_context();
    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("wkt", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        "LINESTRING Z (0 0 1,1 1 2,2 2 3)",
                        "GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4))",
                        "POLYGON M ((0 0 1,0 2 2,2 2 3,0 0 1),(1 1 4,1 2 5,2 2 6,1 1 4))",
                    ])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_Reverse(ST_GeomFromText(wkt))) FROM input ORDER BY id",
    )
    .await;
    let values = string_column(&batch, 0);
    assert_wkt(values.value(0), "LINESTRING Z (2 2 3,1 1 2,0 0 1)");
    assert_wkt(
        values.value(1),
        "GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4))",
    );
    assert_wkt(
        values.value(2),
        "POLYGON M ((0 0 1,2 2 3,0 2 2,0 0 1),(1 1 4,2 2 6,1 2 5,1 1 4))",
    );
}

#[tokio::test]
async fn reverse_supports_all_native_fixed_geometry_array_types() {
    let cases = [
        (
            GeoArrowType::Point(PointType::new(Dimension::XYM, metadata())),
            "POINT M (1 2 3)",
            "POINT M (1 2 3)",
        ),
        (
            GeoArrowType::LineString(LineStringType::new(Dimension::XYM, metadata())),
            "LINESTRING M (0 0 1,1 1 2)",
            "LINESTRING M (1 1 2,0 0 1)",
        ),
        (
            GeoArrowType::Polygon(PolygonType::new(Dimension::XYM, metadata())),
            "POLYGON M ((0 0 1,0 1 2,1 1 3,0 0 1))",
            "POLYGON M ((0 0 1,1 1 3,0 1 2,0 0 1))",
        ),
        (
            GeoArrowType::MultiPoint(MultiPointType::new(Dimension::XYM, metadata())),
            "MULTIPOINT M ((0 0 1),(1 1 2))",
            "MULTIPOINT M ((0 0 1),(1 1 2))",
        ),
        (
            GeoArrowType::MultiLineString(MultiLineStringType::new(Dimension::XYM, metadata())),
            "MULTILINESTRING M ((0 0 1,1 1 2))",
            "MULTILINESTRING M ((1 1 2,0 0 1))",
        ),
        (
            GeoArrowType::MultiPolygon(MultiPolygonType::new(Dimension::XYM, metadata())),
            "MULTIPOLYGON M (((0 0 1,0 1 2,1 1 3,0 0 1)))",
            "MULTIPOLYGON M (((0 0 1,1 1 3,0 1 2,0 0 1)))",
        ),
        (
            GeoArrowType::GeometryCollection(GeometryCollectionType::new(
                Dimension::XYM,
                metadata(),
            )),
            "GEOMETRYCOLLECTION M (POINT M (1 2 3),LINESTRING M (0 0 1,1 1 2))",
            "GEOMETRYCOLLECTION M (POINT M (1 2 3),LINESTRING M (1 1 2,0 0 1))",
        ),
    ];

    for (geometry_type, wkt_text, expected) in cases {
        let context = new_context();
        let wkt = WktArray::new(StringArray::from(vec![wkt_text]), metadata());
        let array = from_wkt(&wkt, geometry_type.clone()).unwrap();
        let field = array.data_type().to_field("geom", true);
        context
            .register_batch(
                "t",
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![field])),
                    vec![array.to_array_ref()],
                )
                .unwrap(),
            )
            .unwrap();

        let batch = collect(
            &context,
            "SELECT ST_Reverse(geom), ST_AsText(ST_Reverse(geom)) FROM t",
        )
        .await;
        let output_type = GeoArrowType::from_arrow_field(batch.schema_ref().field(0)).unwrap();
        assert_eq!(output_type.dimension(), Some(Dimension::XYM), "{wkt_text}");
        assert_eq!(
            std::mem::discriminant(&output_type),
            std::mem::discriminant(&geometry_type),
            "{wkt_text}"
        );
        assert_eq!(output_type.metadata().crs(), metadata().crs(), "{wkt_text}");
        assert_wkt(string_column(&batch, 1).value(0), expected);
    }
}

#[tokio::test]
async fn scroll_changes_closed_line_start_and_reports_invalid_points() {
    let context = new_context();
    let batch = collect(
        &context,
        r#"
        SELECT
            ST_AsText(ST_Scroll(ST_GeomFromText('LINESTRING(0 0,10 0,5 5,0 0)'), ST_GeomFromText('POINT(0 0)'))) AS start,
            ST_AsText(ST_Scroll(ST_GeomFromText('LINESTRING(0 0,10 0,5 5,0 0)'), ST_GeomFromText('POINT(5 5)'))) AS middle,
            ST_AsText(ST_Scroll(ST_GeomFromText('LINESTRING(0 0,10 0,5 5,0 0)'), ST_GeomFromText('POINT(10 0)'))) AS end,
            ST_AsText(ST_Scroll(ST_GeomFromText('LINESTRING(0 0,6 0,5 5,6 0,0 0)'), ST_GeomFromText('POINT(6 0)'))) AS repeated,
            ST_AsText(ST_Scroll(ST_GeomFromText('LINESTRING M (0 0 0,10 0 2,5 5 4,0 0 0)'), ST_GeomFromText('POINT Z (10 0 2)'))) AS positional_m,
            ST_AsText(ST_Scroll(ST_GeomFromText('LINESTRING ZM (0 0 1 1,10 0 2 2,0 0 1 1)'), ST_GeomFromText('POINT ZM (10 0 2 2)'))) AS xyzm
        "#,
    )
    .await;
    assert_column_wkt(&batch, 0, "LINESTRING(0 0,10 0,5 5,0 0)");
    assert_column_wkt(&batch, 1, "LINESTRING(5 5,0 0,10 0,5 5)");
    assert_column_wkt(&batch, 2, "LINESTRING(10 0,5 5,0 0,10 0)");
    assert_column_wkt(&batch, 3, "LINESTRING(6 0,5 5,6 0,0 0,6 0)");
    assert_column_wkt(&batch, 4, "LINESTRING M (10 0 2,5 5 4,0 0 0,10 0 2)");
    assert_column_wkt(&batch, 5, "LINESTRING ZM (10 0 2 2,0 0 1 1,10 0 2 2)");
    for (sql, expected) in [
        (
            "SELECT ST_Scroll(ST_GeomFromText('LINESTRING(0 0,10 0,5 5)'), ST_GeomFromText('POINT(0 0)'))",
            "input POINTARRAY is not closed",
        ),
        (
            "SELECT ST_Scroll(ST_GeomFromText('LINESTRING EMPTY'), ST_GeomFromText('POINT(0 0)'))",
            "input POINTARRAY is not closed",
        ),
        (
            "SELECT ST_Scroll(ST_GeomFromText('LINESTRING(0 0,10 0,5 5,0 0)'), ST_GeomFromText('POINT(9 9)'))",
            "input POINTARRAY does not contain the given point",
        ),
        (
            "SELECT ST_Scroll(ST_GeomFromText('LINESTRING(0 0,10 0,5 5,0 0)'), ST_GeomFromText('POINT EMPTY'))",
            "Second argument must be a non-empty point",
        ),
        (
            "SELECT ST_Scroll(ST_GeomFromText('LINESTRING M (0 0 0,10 0 2,0 0 0)'), ST_GeomFromText('POINT M (10 0 2)'))",
            "input POINTARRAY does not contain the given point",
        ),
        (
            "SELECT ST_Scroll(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(0 0)'))",
            "First argument must be a line",
        ),
        (
            "SELECT ST_Scroll(ST_GeomFromText('LINESTRING(0 0,10 0,0 0)'), ST_GeomFromText('MULTIPOINT((10 0))'))",
            "Second argument must be a point",
        ),
    ] {
        let error = collect_error(&context, sql).await;
        assert!(error.contains(expected), "{error}");
    }
}

#[tokio::test]
async fn scroll_supports_vector_arguments() {
    let context = new_context();
    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("line", DataType::Utf8, false),
                    Field::new("point", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        "LINESTRING(0 0,10 0,5 5,0 0)",
                        "LINESTRING M (0 0 0,10 0 2,0 0 0)",
                        "LINESTRING ZM (0 0 1 1,10 0 2 2,0 0 1 1)",
                    ])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        "POINT(5 5)",
                        "POINT Z (10 0 2)",
                        "POINT ZM (10 0 2 2)",
                    ])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_Scroll(ST_GeomFromText(line), ST_GeomFromText(point))) FROM input ORDER BY id",
    )
    .await;
    let values = string_column(&batch, 0);
    assert_wkt(values.value(0), "LINESTRING(5 5,0 0,10 0,5 5)");
    assert_wkt(values.value(1), "LINESTRING M (10 0 2,0 0 0,10 0 2)");
    assert_wkt(values.value(2), "LINESTRING ZM (10 0 2 2,0 0 1 1,10 0 2 2)");
}

#[tokio::test]
async fn scroll_matches_raw_point_slots_like_postgis() {
    let context = new_context();
    let cases = [
        (
            "LINESTRING(0 0,10 0,0 0)",
            "POINT(10 0)",
            Some("LINESTRING(10 0,0 0,10 0)"),
        ),
        (
            "LINESTRING(0 0,10 0,0 0)",
            "POINT Z (10 0 5)",
            Some("LINESTRING(10 0,0 0,10 0)"),
        ),
        (
            "LINESTRING(0 0,10 0,0 0)",
            "POINT M (10 0 5)",
            Some("LINESTRING(10 0,0 0,10 0)"),
        ),
        (
            "LINESTRING(0 0,10 0,0 0)",
            "POINT ZM (10 0 5 6)",
            Some("LINESTRING(10 0,0 0,10 0)"),
        ),
        ("LINESTRING Z (0 0 5,10 0 5,0 0 5)", "POINT(10 0)", None),
        (
            "LINESTRING Z (0 0 5,10 0 5,0 0 5)",
            "POINT Z (10 0 5)",
            Some("LINESTRING Z (10 0 5,0 0 5,10 0 5)"),
        ),
        (
            "LINESTRING Z (0 0 5,10 0 5,0 0 5)",
            "POINT M (10 0 5)",
            None,
        ),
        (
            "LINESTRING Z (0 0 5,10 0 5,0 0 5)",
            "POINT ZM (10 0 5 6)",
            Some("LINESTRING Z (10 0 5,0 0 5,10 0 5)"),
        ),
        ("LINESTRING M (0 0 5,10 0 5,0 0 5)", "POINT(10 0)", None),
        (
            "LINESTRING M (0 0 5,10 0 5,0 0 5)",
            "POINT Z (10 0 5)",
            Some("LINESTRING M (10 0 5,0 0 5,10 0 5)"),
        ),
        (
            "LINESTRING M (0 0 5,10 0 5,0 0 5)",
            "POINT M (10 0 5)",
            None,
        ),
        (
            "LINESTRING M (0 0 5,10 0 5,0 0 5)",
            "POINT ZM (10 0 5 6)",
            Some("LINESTRING M (10 0 5,0 0 5,10 0 5)"),
        ),
        (
            "LINESTRING ZM (0 0 5 6,10 0 5 6,0 0 5 6)",
            "POINT(10 0)",
            None,
        ),
        (
            "LINESTRING ZM (0 0 5 6,10 0 5 6,0 0 5 6)",
            "POINT Z (10 0 5)",
            None,
        ),
        (
            "LINESTRING ZM (0 0 5 6,10 0 5 6,0 0 5 6)",
            "POINT M (10 0 5)",
            None,
        ),
        (
            "LINESTRING ZM (0 0 5 6,10 0 5 6,0 0 5 6)",
            "POINT ZM (10 0 5 6)",
            Some("LINESTRING ZM (10 0 5 6,0 0 5 6,10 0 5 6)"),
        ),
    ];

    for (line, point, expected) in cases {
        let sql = format!(
            "SELECT ST_AsText(ST_Scroll(ST_GeomFromText('{line}'), ST_GeomFromText('{point}')))"
        );
        match expected {
            Some(expected) => {
                let batch = collect(&context, &sql).await;
                assert_wkt(string_column(&batch, 0).value(0), expected);
            }
            None => {
                let error = collect_error(&context, &sql).await;
                assert!(
                    error.contains("does not contain the given point"),
                    "{line} + {point}: {error}"
                );
            }
        }
    }
}

#[tokio::test]
async fn vertex_editors_preserve_line_metadata_and_coordinate_layout() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let context = new_context();
        let metadata = metadata();
        let line_type =
            LineStringType::new(Dimension::XYZ, metadata.clone()).with_coord_type(coord_type);
        let point_metadata = Arc::new(Metadata::new(
            Crs::from_authority_code("EPSG:3857".to_string()),
            None,
        ));
        let point_type =
            PointType::new(Dimension::XYZ, point_metadata.clone()).with_coord_type(coord_type);

        let mut line_builder = LineStringBuilder::new(line_type.clone());
        let line = wkt::types::LineString::new(
            vec![
                Coord {
                    x: 0.0,
                    y: 0.0,
                    z: Some(1.0),
                    m: None,
                },
                Coord {
                    x: 1.0,
                    y: 1.0,
                    z: Some(2.0),
                    m: None,
                },
                Coord {
                    x: 2.0,
                    y: 2.0,
                    z: Some(3.0),
                    m: None,
                },
                Coord {
                    x: 0.0,
                    y: 0.0,
                    z: Some(4.0),
                    m: None,
                },
            ],
            wkt::types::Dimension::XYZ,
        );
        line_builder.push_line_string(Some(&line)).unwrap();
        let line_array = line_builder.finish();
        let mut point_builder = PointBuilder::with_capacity(point_type.clone(), 1);
        point_builder.push_coord(Some(&Coord {
            x: 9.0,
            y: 8.0,
            z: Some(7.0),
            m: None,
        }));
        let point_array: Arc<dyn GeoArrowArray> = Arc::new(point_builder.finish());
        let mut scroll_point_builder = PointBuilder::with_capacity(point_type.clone(), 1);
        scroll_point_builder.push_coord(Some(&Coord {
            x: 2.0,
            y: 2.0,
            z: Some(3.0),
            m: None,
        }));
        let scroll_point_array: Arc<dyn GeoArrowArray> = Arc::new(scroll_point_builder.finish());

        let line_field = LineStringType::new(Dimension::XYZ, metadata.clone())
            .with_coord_type(coord_type)
            .to_field("line", true);
        let point_field = PointType::new(Dimension::XYZ, point_metadata)
            .with_coord_type(coord_type)
            .to_field("point", true);
        let schema = Schema::new(vec![
            line_field.clone(),
            point_field.clone(),
            PointType::new(
                Dimension::XYZ,
                Arc::new(Metadata::new(
                    Crs::from_authority_code("EPSG:3857".to_string()),
                    None,
                )),
            )
            .with_coord_type(coord_type)
            .to_field("scroll_point", true),
            Field::new("index", DataType::Int32, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                line_array.to_array_ref(),
                point_array.to_array_ref(),
                scroll_point_array.to_array_ref(),
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            ],
        )
        .unwrap();
        context.register_batch("t", batch).unwrap();

        let batch = collect(
            &context,
            "SELECT ST_AddPoint(line, point, 1), ST_SetPoint(line, index, point), ST_RemovePoint(line, 1), ST_Reverse(line), ST_Scroll(line, scroll_point) FROM t",
        )
        .await;

        for column in 0..5 {
            let field = batch.schema_ref().field(column);
            let output_type = field.try_extension_type::<LineStringType>().unwrap();
            assert_eq!(output_type.coord_type(), coord_type, "column {column}");
            assert_eq!(output_type.dimension(), Dimension::XYZ, "column {column}");
            assert_eq!(
                output_type.metadata().crs(),
                metadata.crs(),
                "column {column}"
            );
        }

        let add =
            LineStringArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0)))
                .unwrap();
        let add_coords = add.value(0).unwrap();
        let coords = add_coords.coords().collect::<Vec<_>>();
        assert_eq!(coords.len(), 5);
        assert_eq!(
            (coords[1].x(), coords[1].y(), coords[1].nth(2)),
            (9.0, 8.0, Some(7.0))
        );

        let reverse =
            LineStringArray::try_from((batch.column(3).as_ref(), batch.schema_ref().field(3)))
                .unwrap();
        let reverse_coords = reverse.value(0).unwrap().coords().collect::<Vec<_>>();
        assert_eq!(
            (reverse_coords[0].x(), reverse_coords[0].nth(2)),
            (0.0, Some(4.0))
        );

        let scroll =
            LineStringArray::try_from((batch.column(4).as_ref(), batch.schema_ref().field(4)))
                .unwrap();
        let scroll_coords = scroll.value(0).unwrap().coords().collect::<Vec<_>>();
        assert_eq!(
            (
                scroll_coords[0].x(),
                scroll_coords[0].y(),
                scroll_coords[0].nth(2)
            ),
            (2.0, 2.0, Some(3.0))
        );
    }
}

#[tokio::test]
async fn vertex_editors_accept_wkt_wkb_and_rect_storage_paths() {
    let metadata = metadata();
    let wkt = WktArray::new(
        StringArray::from(vec!["LINESTRING Z (0 0 1,1 1 2,0 0 1)"]),
        metadata.clone(),
    );
    let inputs: Vec<(&str, Arc<dyn GeoArrowArray>)> = vec![
        ("wkt", Arc::new(wkt.clone())),
        (
            "large_wkt",
            Arc::new(LargeWktArray::new(
                LargeStringArray::from(vec!["LINESTRING Z (0 0 1,1 1 2,0 0 1)"]),
                metadata.clone(),
            )),
        ),
        (
            "wkt_view",
            Arc::new(WktViewArray::new(
                StringViewArray::from(vec!["LINESTRING Z (0 0 1,1 1 2,0 0 1)"]),
                metadata.clone(),
            )),
        ),
        ("wkb", Arc::new(to_wkb::<i32>(&wkt).unwrap())),
        ("large_wkb", Arc::new(to_wkb::<i64>(&wkt).unwrap())),
        ("wkb_view", Arc::new(to_wkb_view(&wkt).unwrap())),
    ];

    for (_name, input) in inputs {
        let context = new_context();
        let field = input.data_type().to_field("geom", true);
        context
            .register_batch(
                "t",
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![field])),
                    vec![input.to_array_ref()],
                )
                .unwrap(),
            )
            .unwrap();
        let batch = collect(
            &context,
            "SELECT ST_AsText(ST_Reverse(geom)), ST_AsText(ST_AddPoint(geom, ST_GeomFromText('POINT Z (9 9 9)'), 1)) FROM t",
        )
        .await;
        assert_wkt(
            string_column(&batch, 0).value(0),
            "LINESTRING Z (0 0 1,1 1 2,0 0 1)",
        );
        assert_wkt(
            string_column(&batch, 1).value(0),
            "LINESTRING Z (0 0 1,9 9 9,1 1 2,0 0 1)",
        );
    }

    let context = new_context();
    let box_type = geoarrow_schema::BoxType::new(Dimension::XYM, metadata);
    let mut builder = RectBuilder::with_capacity(box_type, 1);
    builder.push_min_max(
        &Coord {
            x: 0.0,
            y: 0.0,
            z: None,
            m: Some(1.0),
        },
        &Coord {
            x: 2.0,
            y: 3.0,
            z: None,
            m: Some(4.0),
        },
    );
    let rect: Arc<dyn GeoArrowArray> = Arc::new(builder.finish());
    let field = rect.data_type().to_field("geom", true);
    context
        .register_batch(
            "r",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![field])),
                vec![rect.to_array_ref()],
            )
            .unwrap(),
        )
        .unwrap();
    let batch = collect(&context, "SELECT ST_Reverse(geom) FROM r").await;
    let output =
        RectArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0))).unwrap();
    assert_eq!(output.value(0).unwrap().dim(), geo_traits::Dimensions::Xym);
}

#[tokio::test]
async fn vertex_editors_keep_null_rows_null() {
    let context = new_context();
    context
        .register_batch(
            "nulls",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("line", DataType::Utf8, true),
                    Field::new("point", DataType::Utf8, true),
                    Field::new("index", DataType::Int32, true),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                    Arc::new(StringArray::from(vec![None, Some("LINESTRING(0 0,1 1)")]))
                        as ArrayRef,
                    Arc::new(StringArray::from(vec![Some("POINT(2 2)"), None])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![Some(0), None])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let batch = collect(
        &context,
        r#"
        SELECT
            ST_AddPoint(ST_GeomFromText(line), ST_GeomFromText(point), index),
            ST_SetPoint(ST_GeomFromText(line), index, ST_GeomFromText(point)),
            ST_RemovePoint(ST_GeomFromText(line), index)
        FROM nulls
        ORDER BY id
        "#,
    )
    .await;
    for column in 0..3 {
        assert_geometry_null(&batch, column, 0);
        assert_geometry_null(&batch, column, 1);
    }

    let context = new_context();
    context
        .register_batch(
            "scroll_nulls",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("line", DataType::Utf8, true),
                    Field::new("point", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                    Arc::new(StringArray::from(vec![
                        None,
                        Some("LINESTRING(0 0,1 0,0 0)"),
                    ])) as ArrayRef,
                    Arc::new(StringArray::from(vec![Some("POINT(1 0)"), None])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let batch = collect(
        &context,
        "SELECT ST_Scroll(ST_GeomFromText(line), ST_GeomFromText(point)) FROM scroll_nulls ORDER BY id",
    )
    .await;
    assert_geometry_null(&batch, 0, 0);
    assert_geometry_null(&batch, 0, 1);

    let batch = collect(
        &context,
        "SELECT ST_AddPoint(NULL, ST_GeomFromText('POINT(1 2)'))",
    )
    .await;
    assert_geometry_null(&batch, 0, 0);
}

#[tokio::test]
async fn vertex_editors_declare_nullable_outputs_for_null_arguments() {
    let context = new_context();
    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("line", DataType::Utf8, false),
                    Field::new("point", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec!["LINESTRING(0 0,10 0,5 5,0 0)"])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["POINT(5 5)"])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();

    for expression in [
        "ST_AddPoint(ST_GeomFromText(line), ST_GeomFromText(point), NULL)",
        "ST_AddPoint(ST_GeomFromText(line), NULL, 0)",
        "ST_AddPoint(NULL, ST_GeomFromText(point))",
        "ST_RemovePoint(ST_GeomFromText(line), NULL)",
        "ST_SetPoint(ST_GeomFromText(line), NULL, ST_GeomFromText(point))",
        "ST_SetPoint(ST_GeomFromText(line), 0, NULL)",
        "ST_Scroll(ST_GeomFromText(line), NULL)",
        "ST_Reverse(NULL)",
    ] {
        let batch = collect(&context, &format!("SELECT {expression} IS NULL FROM input")).await;
        let values = batch.column(0).as_boolean();
        assert!(values.value(0), "{expression} is not NULL");
    }

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
                    x: 10.0,
                    y: 0.0,
                    z: None,
                    m: None,
                },
                Coord {
                    x: 0.0,
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
            "native_input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![field])),
                vec![line.to_array_ref()],
            )
            .unwrap(),
        )
        .unwrap();

    for expression in [
        "ST_AddPoint(geom, geom, CAST(NULL AS INT))",
        "ST_RemovePoint(geom, CAST(NULL AS INT))",
        "ST_SetPoint(geom, 0, NULL)",
        "ST_Scroll(geom, NULL)",
    ] {
        let batch = collect(
            &context,
            &format!("SELECT {expression} IS NULL FROM native_input"),
        )
        .await;
        let values = batch.column(0).as_boolean();
        assert!(values.value(0), "{expression} is not NULL");
    }

    let batch = collect(
        &context,
        "SELECT ST_AddPoint(geom, geom, CAST(NULL AS INT)) AS out FROM native_input",
    )
    .await;
    assert!(batch.schema_ref().field(0).is_nullable());
}
