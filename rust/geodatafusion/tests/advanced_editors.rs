use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BooleanArray, Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geo_traits::GeometryTrait;
use geoarrow_array::array::GeometryArray;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Crs, GeoArrowType, GeometryType, Metadata};
use wkt::Wkt;

fn new_context() -> SessionContext {
    let context = SessionContext::new();
    geodatafusion::register(&context);
    context
}

async fn collect(context: &SessionContext, sql: &str) -> RecordBatch {
    let dataframe = context.sql(sql).await.unwrap();
    let batches = dataframe.collect().await.unwrap();
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

fn metadata() -> Arc<Metadata> {
    Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ))
}

fn output_geometry(batch: &RecordBatch) -> GeometryArray {
    GeometryArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0))).unwrap()
}

fn geometry_field(batch: &RecordBatch) -> Arc<Field> {
    batch.schema_ref().field(0).clone().into()
}

#[tokio::test]
async fn native_advanced_editors_match_postgis_examples() {
    let context = new_context();

    let snap_to_grid = collect(
        &context,
        "SELECT ST_AsText(ST_SnapToGrid(ST_GeomFromText('LINESTRING(1.1115678 2.123,4.111111 3.2374897,4.11112 3.23748667)'), 0.001))",
    )
    .await;
    assert_wkt(
        snap_to_grid.column(0).as_string::<i32>().value(0),
        "LINESTRING(1.112 2.123,4.111 3.237)",
    );

    let snap_to_grid = collect(
        &context,
        "SELECT ST_AsText(ST_SnapToGrid(ST_GeomFromText('LINESTRING ZM(-1.1115678 2.123 2.3456 1.11111,4.111111 3.2374897 3.1234 1.1111,-1.11111112 2.123 2.3456 1.1111112)'), ST_GeomFromText('POINT ZM(1.12 2.22 3.2 4.4444)'), 0.1, 0.1, 0.1, 0.01))",
    )
    .await;
    assert_wkt(
        snap_to_grid.column(0).as_string::<i32>().value(0),
        "LINESTRING ZM (-1.08 2.12 2.3 1.1144,4.12 3.22 3.1 1.1144,-1.08 2.12 2.3 1.1144)",
    );

    let remove_small_parts = collect(
        &context,
        "SELECT ST_AsText(ST_RemoveSmallParts(ST_GeomFromText('MULTIPOLYGON(((60 160,120 160,120 220,60 220,60 160),(70 170,70 210,110 210,110 170,70 170)),((85 75,155 75,155 145,85 145,85 75)),((50 110,70 110,70 130,50 130,50 110)))'), 50, 50))",
    )
    .await;
    assert_wkt(
        remove_small_parts.column(0).as_string::<i32>().value(0),
        "MULTIPOLYGON(((60 160,120 160,120 220,60 220,60 160)),((85 75,155 75,155 145,85 145,85 75)))",
    );

    let remove_irrelevant = collect(
        &context,
        "SELECT ST_AsText(ST_RemoveIrrelevantPointsForView(ST_GeomFromText('MULTILINESTRING((0 0,10 0,20 0,30 0),(0 15,5 15,10 15,15 15,20 15,25 15,30 15,40 15),(13 13,15 15,17 17))'), ST_MakeBox2D(ST_Point(12,12), ST_Point(18,18)), true))",
    )
    .await;
    assert_wkt(
        remove_irrelevant.column(0).as_string::<i32>().value(0),
        "MULTILINESTRING((10 15,15 15,20 15),(13 13,15 15,17 17))",
    );

    let remove_irrelevant = collect(
        &context,
        "SELECT ST_AsText(ST_RemoveIrrelevantPointsForView(ST_GeomFromText('POLYGON((0 30,15 30,30 30,30 0,0 0,0 30))'), ST_MakeBox2D(ST_Point(12,12), ST_Point(18,18))))",
    )
    .await;
    assert_wkt(
        remove_irrelevant.column(0).as_string::<i32>().value(0),
        "POLYGON((0 30,30 30,30 0,0 0,0 30))",
    );
}

#[tokio::test]
async fn native_advanced_editors_preserve_crs_dimension_and_slices() {
    let context = new_context();
    let geometry_type = GeometryType::new(metadata()).with_coord_type(CoordType::Separated);
    let mut builder = GeometryBuilder::new(geometry_type);
    builder
        .push_geometry(Some(&parse_wkt("POINT ZM(1.1 2.2 3.3 4.4)")))
        .unwrap();
    builder
        .push_geometry(Some(&parse_wkt("LINESTRING ZM(0 0 1 2,100 0 1 2)")))
        .unwrap();
    let input = builder.finish().slice(1, 1);
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

    for sql in [
        "SELECT ST_SnapToGrid(geom, 0.5, 0.5) FROM t",
        "SELECT ST_RemoveSmallParts(geom, 1, 1) FROM t",
        "SELECT ST_RemoveIrrelevantPointsForView(geom, ST_MakeBox2D(ST_Point(0, -1), ST_Point(200, 1))) FROM t",
    ] {
        let batch = collect(&context, sql).await;
        let field = geometry_field(&batch);
        let output_type = GeoArrowType::from_arrow_field(field.as_ref()).unwrap();
        assert_eq!(
            output_type.metadata().crs(),
            &Crs::from_authority_code("EPSG:4326".to_string()),
            "{sql}"
        );
        let output = output_geometry(&batch);
        assert_eq!(output.len(), 1);
        let geometry = output.value(0).unwrap();
        assert_eq!(geometry.dim().size(), 4, "{sql}");
    }
}

#[tokio::test]
async fn native_advanced_editors_follow_strict_null_and_empty_contracts() {
    let context = new_context();
    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("geom", DataType::Utf8, false),
                    Field::new("size", DataType::Float64, true),
                    Field::new("hint", DataType::Boolean, true),
                ])),
                vec![
                    Arc::new(StringArray::from(vec![
                        Some("LINESTRING(0 0,10 0)"),
                        Some("LINESTRING(0 0,10 0)"),
                        Some("LINESTRING EMPTY"),
                        Some("LINESTRING(0 0,10 0)"),
                    ])) as ArrayRef,
                    Arc::new(Float64Array::from(vec![Some(1.0), None, Some(1.0), None]))
                        as ArrayRef,
                    Arc::new(BooleanArray::from(vec![
                        Some(true),
                        Some(true),
                        Some(false),
                        Some(false),
                    ])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();

    let batch = collect(
        &context,
        "SELECT
            ST_SnapToGrid(ST_GeomFromText(geom), size) IS NULL,
            ST_RemoveSmallParts(ST_GeomFromText(geom), size, 1) IS NULL,
            ST_RemoveIrrelevantPointsForView(
                ST_GeomFromText(geom),
                ST_MakeBox2D(ST_Point(-1, -1), ST_Point(20, 1)),
                hint
            ) IS NULL
        FROM input",
    )
    .await;
    let values = batch.column(0).as_boolean();
    assert_eq!(
        (0..values.len())
            .map(|index| values.value(index))
            .collect::<Vec<_>>(),
        vec![false, true, false, true]
    );
    let values = batch.column(1).as_boolean();
    assert_eq!(
        (0..values.len())
            .map(|index| values.value(index))
            .collect::<Vec<_>>(),
        vec![false, true, false, true]
    );
    let values = batch.column(2).as_boolean();
    assert_eq!(
        (0..values.len())
            .map(|index| values.value(index))
            .collect::<Vec<_>>(),
        vec![false, false, false, false]
    );

    let empty = collect(
        &context,
        "SELECT ST_AsText(ST_SnapToGrid(ST_GeomFromText('LINESTRING EMPTY'), 1))",
    )
    .await;
    assert_wkt(
        empty.column(0).as_string::<i32>().value(0),
        "LINESTRING EMPTY",
    );
}

#[tokio::test]
async fn native_advanced_editors_reject_invalid_grid_origins() {
    let context = new_context();

    let error = collect_error(
        &context,
        "SELECT ST_SnapToGrid(
            ST_GeomFromText('POINT(1 2)'),
            ST_GeomFromText('LINESTRING(0 0,1 1)'),
            1, 1, 1, 1
        )",
    )
    .await;
    assert!(
        error.contains("Offset geometry must be a point"),
        "unexpected error: {error}"
    );

    let error = collect_error(
        &context,
        "SELECT ST_SnapToGrid(
            ST_GeomFromText('POINT(1 2)'),
            ST_GeomFromText('POINT EMPTY'),
            1, 1, 1, 1
        )",
    )
    .await;
    assert!(
        error.contains("Offset geometry must be a point"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn native_advanced_editors_handle_box_geometry_inputs() {
    let context = new_context();

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_SnapToGrid(ST_MakeBox2D(ST_Point(0.1, 0.1), ST_Point(1.9, 1.9)), 1))",
    )
    .await;
    assert_wkt(
        batch.column(0).as_string::<i32>().value(0),
        "POLYGON((0 0,0 2,2 2,2 0,0 0))",
    );

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_RemoveSmallParts(ST_MakeBox2D(ST_Point(0, 0), ST_Point(1, 1)), 5, 5))",
    )
    .await;
    assert_wkt(batch.column(0).as_string::<i32>().value(0), "POLYGON EMPTY");

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_RemoveIrrelevantPointsForView(
            ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10)),
            ST_MakeBox2D(ST_Point(4, 4), ST_Point(6, 6))
        ))",
    )
    .await;
    assert_wkt(
        batch.column(0).as_string::<i32>().value(0),
        "POLYGON((0 0,0 10,10 10,10 0,0 0))",
    );
}

#[tokio::test]
#[cfg(feature = "geos-3_11")]
async fn geos_advanced_editors_match_postgis_examples() {
    let context = new_context();

    let normalize = collect(
        &context,
        "SELECT
            ST_AsText(ST_Normalize(ST_GeomFromText('POINT ZM(1 2 3 4)'))),
            ST_AsText(ST_Normalize(ST_GeomFromText('POINT M(1 2 3)'))),
            ST_AsText(ST_Normalize(ST_GeomFromText('LINESTRING Z(2 2 4,1 1 3)'))),
            ST_AsText(ST_Normalize(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'))),
            ST_AsEWKT(ST_Normalize(ST_GeomFromEWKT('SRID=3857;POINT(1 2)')))",
    )
    .await;
    assert_wkt(
        normalize.column(0).as_string::<i32>().value(0),
        "POINT Z (1 2 3)",
    );
    assert_wkt(
        normalize.column(1).as_string::<i32>().value(0),
        "POINT(1 2)",
    );
    assert_wkt(
        normalize.column(2).as_string::<i32>().value(0),
        "LINESTRING Z (1 1 3,2 2 4)",
    );
    assert_wkt(
        normalize.column(3).as_string::<i32>().value(0),
        "POLYGON((0 0,0 10,10 10,10 0,0 0))",
    );
    assert_eq!(
        normalize.column(4).as_string::<i32>().value(0),
        "SRID=3857;POINT(1 2)"
    );

    let normalize = collect(
        &context,
        "SELECT ST_AsText(ST_Normalize(ST_GeomFromEWKT('SRID=4326;GEOMETRYCOLLECTION(POINT(2 3),MULTILINESTRING((0 0,1 1),(2 2,3 3)),POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,4 2,4 4,2 4,2 2),(6 6,8 6,8 8,6 8,6 6)))')))",
    )
    .await;
    assert_wkt(
        normalize.column(0).as_string::<i32>().value(0),
        "GEOMETRYCOLLECTION(POLYGON((0 0,0 10,10 10,10 0,0 0),(6 6,8 6,8 8,6 8,6 6),(2 2,4 2,4 4,2 4,2 2)),MULTILINESTRING((2 2,3 3),(0 0,1 1)),POINT(2 3))",
    );

    let snap = collect(
        &context,
        "SELECT
            ST_AsText(ST_Snap(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'), ST_GeomFromText('LINESTRING(0 0,5 0,10 10)'), 1)),
            ST_AsText(ST_Snap(ST_GeomFromText('POINT(30 30)'), ST_GeomFromText('LINESTRING(0 0,10 0)'), 1)),
            ST_AsText(ST_Snap(ST_GeomFromText('POINT ZM(3 3 30 300)'), ST_GeomFromText('POINT(5 5)'), 10)),
            ST_AsText(ST_Snap(ST_GeomFromText('LINESTRING M(0 0 5,10 0 5)'), ST_GeomFromText('POINT M(0 0 7)'), 1)),
            ST_AsText(ST_Snap(ST_GeomFromText('LINESTRING Z(0 0 1,10 0 2)'), ST_GeomFromText('POINT Z(0 0 5)'), 1)),
            ST_AsText(ST_Snap(ST_GeomFromText('LINESTRING EMPTY'), ST_GeomFromText('POINT(1 1)'), 1))",
    )
    .await;
    assert_wkt(
        snap.column(0).as_string::<i32>().value(0),
        "POLYGON((0 0,5 0,10 0,10 10,0 10,0 0))",
    );
    assert_wkt(snap.column(1).as_string::<i32>().value(0), "POINT(30 30)");
    assert_wkt(snap.column(2).as_string::<i32>().value(0), "POINT(5 5)");
    assert_wkt(
        snap.column(3).as_string::<i32>().value(0),
        "LINESTRING(0 0,10 0)",
    );
    assert_wkt(
        snap.column(4).as_string::<i32>().value(0),
        "LINESTRING Z (0 0 5,10 0 2)",
    );
    assert_wkt(
        snap.column(5).as_string::<i32>().value(0),
        "LINESTRING EMPTY",
    );

    let boxes = collect(
        &context,
        "SELECT ST_AsText(ST_Normalize(ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10))))",
    )
    .await;
    assert_wkt(
        boxes.column(0).as_string::<i32>().value(0),
        "POLYGON((0 0,0 10,10 10,10 0,0 0))",
    );

    let boxes = collect(
        &context,
        "SELECT ST_AsText(ST_Snap(
            ST_MakeBox2D(ST_Point(0, 0), ST_Point(10, 10)),
            ST_GeomFromText('POINT(10 10)'),
            6
        ))",
    )
    .await;
    assert_wkt(
        boxes.column(0).as_string::<i32>().value(0),
        "POLYGON((0 0,0 10,10 10,10 0,0 0))",
    );
}

#[tokio::test]
#[cfg(feature = "geos-3_11")]
async fn geos_snap_empty_dimensions_match_postgis() {
    let context = new_context();
    let batch = collect(
        &context,
        "SELECT
            ST_AsText(ST_Snap(ST_GeomFromText('LINESTRING EMPTY'), ST_GeomFromText('POINT(1 1)'), 1)),
            ST_AsText(ST_Snap(ST_GeomFromText('LINESTRING Z EMPTY'), ST_GeomFromText('POINT Z(1 1 2)'), 1)),
            ST_AsText(ST_Snap(ST_GeomFromText('LINESTRING M EMPTY'), ST_GeomFromText('POINT M(1 1 2)'), 1)),
            ST_AsText(ST_Snap(ST_GeomFromText('LINESTRING ZM EMPTY'), ST_GeomFromText('POINT ZM(1 1 2 3)'), 1)),
            ST_AsText(ST_Snap(ST_GeomFromText('LINESTRING EMPTY'), ST_GeomFromText('POINT Z(1 1 2)'), 1)),
            ST_AsText(ST_Snap(ST_GeomFromText('LINESTRING Z EMPTY'), ST_GeomFromText('POINT(1 1)'), 1)),
            ST_AsText(ST_Snap(ST_GeomFromText('POINT ZM EMPTY'), ST_GeomFromText('POINT(1 1)'), 1))",
    )
    .await;
    for (column, expected) in [
        "LINESTRING EMPTY",
        "LINESTRING Z EMPTY",
        "LINESTRING EMPTY",
        "LINESTRING Z EMPTY",
        "LINESTRING Z EMPTY",
        "LINESTRING Z EMPTY",
        "POINT Z EMPTY",
    ]
    .iter()
    .enumerate()
    {
        assert_wkt(batch.column(column).as_string::<i32>().value(0), expected);
    }

    let batch = collect(
        &context,
        "SELECT
            ST_Snap(ST_GeomFromText(NULL), ST_GeomFromText('POINT(1 1)'), 1) IS NULL,
            ST_Snap(ST_GeomFromText('LINESTRING EMPTY'), ST_GeomFromText(NULL), 1) IS NULL,
            ST_Snap(ST_GeomFromText('LINESTRING EMPTY'), ST_GeomFromText('POINT(1 1)'), NULL) IS NULL",
    )
    .await;
    for column in 0..3 {
        let values = batch.column(column).as_boolean();
        assert!(values.value(0), "column {column} is not NULL");
    }
}

#[tokio::test]
#[cfg(feature = "geos-3_11")]
async fn geos_advanced_editors_reject_invalid_arguments() {
    let context = new_context();

    for sql in [
        "SELECT ST_Normalize(ST_GeomFromText('POINT(1 2)'), 1)",
        "SELECT ST_Snap(ST_GeomFromText('POINT(1 2)'), 1)",
        "SELECT ST_Snap(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(3 4)'), 'bad')",
    ] {
        let _ = collect_error(&context, sql).await;
    }

    let mixed_srid = collect_error(
        &context,
        "SELECT ST_Snap(ST_GeomFromEWKT('SRID=4326;POINT(1 2)'), ST_GeomFromEWKT('SRID=3857;POINT(3 4)'), 1)",
    )
    .await;
    assert!(
        mixed_srid.contains("mixed SRID"),
        "unexpected error: {mixed_srid}"
    );
}

fn parse_wkt(value: &str) -> Wkt<f64> {
    value.parse().unwrap()
}

fn assert_wkt(actual: &str, expected: &str) {
    let _: Wkt<f64> = actual.parse().unwrap();
    let _: Wkt<f64> = expected.parse().unwrap();
    assert_eq!(
        normalize_numbers(actual),
        normalize_numbers(expected),
        "actual={actual:?} expected={expected:?}"
    );
}

// PostGIS prints doubles with limited precision, while GeoDataFusion keeps the
// shortest round-trip representation. Compare both at a fixed decimal precision.
fn normalize_numbers(text: &str) -> String {
    fn flush(token: &mut String, output: &mut String) {
        if token.is_empty() {
            return;
        }
        if let Ok(value) = token.parse::<f64>() {
            let mut formatted = format!("{value:.9}");
            while formatted.contains('.') && formatted.ends_with('0') {
                formatted.pop();
            }
            if formatted.ends_with('.') {
                formatted.pop();
            }
            output.push_str(&formatted);
        } else {
            output.push_str(token);
        }
        token.clear();
    }

    let mut output = String::new();
    let mut token = String::new();
    for character in text.chars() {
        let part_of_number = character.is_ascii_digit()
            || character == '.'
            || character == '-'
            || character == '+'
            || character == 'e'
            || character == 'E';
        if part_of_number {
            token.push(character);
        } else {
            flush(&mut token, &mut output);
            output.push(character);
        }
    }
    flush(&mut token, &mut output);
    output.chars().filter(|c| !c.is_whitespace()).collect()
}
