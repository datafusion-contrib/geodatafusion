//! Public SQL contracts for the bounding-box function family.
//!
//! These tests cover the typed and PostGIS-visible behavior of Box2D, Box3D,
//! ST_Envelope, ST_Expand, ST_MakeBox2D, ST_3DMakeBox, and the six
//! ST_*Min/ST_*Max accessors without duplicating the aggregate coverage in
//! `extent_registration.rs`.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::Schema;
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::{PointBuilder, RectBuilder};
use geoarrow_schema::{BoxType, CoordType, Crs, Dimension, Metadata, PointType};
use wkt::types::Coord;

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

async fn collect(ctx: &SessionContext, sql: &str) -> RecordBatch {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("failed to plan {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("failed to execute {sql}: {error}"));
    assert_eq!(batches.len(), 1, "{sql}");
    batches.into_iter().next().unwrap()
}

async fn collect_error(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Ok(dataframe) => match dataframe.collect().await {
            Ok(_) => panic!("expected query to fail: {sql}"),
            Err(error) => error.to_string(),
        },
        Err(error) => error.to_string(),
    }
}

fn float_column(batch: &RecordBatch, index: usize) -> &arrow_array::PrimitiveArray<Float64Type> {
    batch.column(index).as_primitive::<Float64Type>()
}

fn assert_f64_row(batch: &RecordBatch, expected: &[f64]) {
    assert_eq!(batch.num_columns(), expected.len(), "column count");
    for (index, expected) in expected.iter().enumerate() {
        let column = float_column(batch, index);
        assert!(!column.is_null(0), "column {index} is null");
        assert_eq!(column.value(0), *expected, "column {index}");
    }
}

#[tokio::test]
async fn box_and_extrema_cover_dimensions_m_and_nulls() {
    let ctx = context();
    let batch = collect(
        &ctx,
        "SELECT \
            ST_XMin(Box2D(ST_GeomFromText('LINESTRING(1 2,3 4)'))), \
            ST_YMin(Box2D(ST_GeomFromText('LINESTRING(1 2,3 4)'))), \
            ST_XMax(Box2D(ST_GeomFromText('LINESTRING(1 2,3 4)'))), \
            ST_YMax(Box2D(ST_GeomFromText('LINESTRING(1 2,3 4)'))), \
            ST_ZMin(Box2D(ST_GeomFromText('LINESTRING(1 2,3 4)'))), \
            ST_ZMax(Box2D(ST_GeomFromText('LINESTRING(1 2,3 4)'))), \
            ST_XMin(Box3D(ST_GeomFromText('LINESTRING Z (1 2 3,3 4 5)'))), \
            ST_ZMin(Box3D(ST_GeomFromText('LINESTRING Z (1 2 3,3 4 5)'))), \
            ST_ZMax(Box3D(ST_GeomFromText('LINESTRING Z (1 2 3,3 4 5)'))), \
            ST_XMin(Box3D(ST_GeomFromText('LINESTRING(1 2,3 4)'))), \
            ST_ZMin(Box3D(ST_GeomFromText('LINESTRING(1 2,3 4)'))), \
            ST_ZMax(Box3D(ST_GeomFromText('LINESTRING(1 2,3 4)'))), \
            ST_XMin(Box2D(Box3D(ST_GeomFromText('LINESTRING Z (1 2 3,3 4 5)')))), \
            ST_ZMin(Box3D(ST_GeomFromText('POINT ZM (1 2 3 4)'))), \
            ST_ZMin(Box3D(ST_GeomFromText('LINESTRING M (1 2 9,3 4 7)'))), \
            ST_ZMin(Box2D(ST_GeomFromText('LINESTRING Z (1 2 3,3 4 5)')))\
        ",
    )
    .await;
    assert_f64_row(
        &batch,
        &[
            1.0, 2.0, 3.0, 4.0, 0.0, 0.0, 1.0, 3.0, 5.0, 1.0, 0.0, 0.0, 1.0, 3.0, 0.0, 0.0,
        ],
    );

    let batch = collect(
        &ctx,
        "SELECT \
            Box2D(NULL) IS NULL, \
            Box3D(NULL) IS NULL, \
            ST_XMin(NULL) IS NULL, \
            ST_ZMin(NULL) IS NULL, \
            Box2D(ST_GeomFromText('POINT EMPTY')) IS NULL, \
            Box3D(ST_GeomFromText('POLYGON EMPTY')) IS NULL, \
            ST_XMin(ST_GeomFromText('POINT EMPTY')) IS NULL, \
            ST_XMin(ST_GeomFromText('LINESTRING EMPTY')) IS NULL\
        ",
    )
    .await;
    for index in 0..batch.num_columns() {
        assert!(batch.column(index).as_boolean().value(0), "column {index}");
    }
}

#[tokio::test]
async fn box_contracts_cover_sliced_rect_inputs() {
    let ctx = context();
    let box_type = BoxType::new(Dimension::XY, Arc::new(Metadata::default()));
    let mut builder = RectBuilder::with_capacity(box_type, 3);
    builder.push_min_max(
        &Coord {
            x: 0.0,
            y: 0.0,
            z: None,
            m: None,
        },
        &Coord {
            x: 1.0,
            y: 1.0,
            z: None,
            m: None,
        },
    );
    builder.push_min_max(
        &Coord {
            x: 1.0,
            y: 2.0,
            z: None,
            m: None,
        },
        &Coord {
            x: 3.0,
            y: 4.0,
            z: None,
            m: None,
        },
    );
    builder.push_min_max(
        &Coord {
            x: -5.0,
            y: -6.0,
            z: None,
            m: None,
        },
        &Coord {
            x: -1.0,
            y: -2.0,
            z: None,
            m: None,
        },
    );
    let rects = builder.finish();
    let field = rects.data_type().to_field("box", false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![rects.to_array_ref()],
    )
    .unwrap();
    ctx.register_batch("sliced_boxes", batch.slice(1, 2))
        .unwrap();

    let batch = collect(
        &ctx,
        "SELECT \
            ST_XMin(box), ST_YMin(box), ST_XMax(box), ST_YMax(box), \
            ST_ZMin(Box3D(box)), ST_ZMax(Box3D(box)) \
         FROM sliced_boxes ORDER BY ST_XMin(box)",
    )
    .await;
    assert_eq!(batch.num_rows(), 2);
    for (row, expected) in [
        [-5.0, -6.0, -1.0, -2.0, 0.0, 0.0],
        [1.0, 2.0, 3.0, 4.0, 0.0, 0.0],
    ]
    .into_iter()
    .enumerate()
    {
        for (column, expected) in expected.into_iter().enumerate() {
            assert_eq!(
                float_column(&batch, column).value(row),
                expected,
                "row {row} column {column}"
            );
        }
    }
}

#[tokio::test]
async fn make_box_normalizes_corners_and_preserves_crs() {
    let ctx = context();
    let batch = collect(
        &ctx,
        "SELECT \
            ST_XMin(ST_MakeBox2D(ST_Point(10, 20), ST_Point(0, 5))), \
            ST_YMin(ST_MakeBox2D(ST_Point(10, 20), ST_Point(0, 5))), \
            ST_XMax(ST_MakeBox2D(ST_Point(10, 20), ST_Point(0, 5))), \
            ST_YMax(ST_MakeBox2D(ST_Point(10, 20), ST_Point(0, 5)))\
        ",
    )
    .await;
    assert_f64_row(&batch, &[0.0, 5.0, 10.0, 20.0]);

    let batch = collect(
        &ctx,
        "SELECT \
            ST_XMin(ST_3DMakeBox(ST_PointZ(10, 10, 20), ST_PointZ(0, 0, 0))), \
            ST_YMin(ST_3DMakeBox(ST_PointZ(10, 10, 20), ST_PointZ(0, 0, 0))), \
            ST_ZMin(ST_3DMakeBox(ST_PointZ(10, 10, 20), ST_PointZ(0, 0, 0))), \
            ST_XMax(ST_3DMakeBox(ST_PointZ(10, 10, 20), ST_PointZ(0, 0, 0))), \
            ST_YMax(ST_3DMakeBox(ST_PointZ(10, 10, 20), ST_PointZ(0, 0, 0))), \
            ST_ZMax(ST_3DMakeBox(ST_PointZ(10, 10, 20), ST_PointZ(0, 0, 0)))\
        ",
    )
    .await;
    assert_f64_row(&batch, &[0.0, 0.0, 0.0, 10.0, 10.0, 20.0]);

    let batch = collect(
        &ctx,
        "SELECT \
            ST_MakeBox2D(NULL, ST_Point(0, 5)) IS NULL, \
            ST_3DMakeBox(ST_PointZ(0, 0, 1), NULL) IS NULL\
        ",
    )
    .await;
    for index in 0..batch.num_columns() {
        assert!(batch.column(index).as_boolean().value(0), "column {index}");
    }

    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:3857".to_string()),
        None,
    ));
    let point_type =
        PointType::new(Dimension::XY, Arc::clone(&metadata)).with_coord_type(CoordType::Separated);
    let lower = PointBuilder::from_nullable_points(
        [Some(&geo::Point::new(0.0, 5.0))].into_iter(),
        point_type.clone(),
    )
    .finish();
    let upper = PointBuilder::from_nullable_points(
        [Some(&geo::Point::new(10.0, 20.0))].into_iter(),
        point_type,
    )
    .finish();
    let lower_field = lower.data_type().to_field("lower", false);
    let upper_field = upper.data_type().to_field("upper", false);
    let schema = Arc::new(Schema::new(vec![lower_field.clone(), upper_field]));
    let lower_array = lower.to_array_ref();
    let batch =
        RecordBatch::try_new(schema, vec![lower_array.clone(), upper.to_array_ref()]).unwrap();
    ctx.register_batch("corners", batch).unwrap();

    let batch = collect(&ctx, "SELECT ST_MakeBox2D(lower, upper) AS b FROM corners").await;
    let box_type = batch
        .schema()
        .field(0)
        .try_extension_type::<BoxType>()
        .unwrap();
    assert_eq!(box_type.dimension(), Dimension::XY);
    assert_eq!(
        box_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:3857".to_string())
    );

    let mixed_metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let mixed_upper = PointBuilder::from_nullable_points(
        [Some(&geo::Point::new(1.0, 2.0))].into_iter(),
        PointType::new(Dimension::XY, mixed_metadata).with_coord_type(CoordType::Separated),
    )
    .finish();
    let schema = Arc::new(Schema::new(vec![
        lower_field,
        mixed_upper.data_type().to_field("upper", false),
    ]));
    let batch =
        RecordBatch::try_new(schema, vec![lower_array, mixed_upper.to_array_ref()]).unwrap();
    ctx.register_batch("mixed_corners", batch).unwrap();
    let error = collect_error(&ctx, "SELECT ST_MakeBox2D(lower, upper) FROM mixed_corners").await;
    assert!(error.contains("mixed SRID"), "unexpected error: {error}");
}

#[tokio::test]
async fn make_box_documents_typed_dimension_boundaries() {
    let ctx = context();
    let error = collect_error(
        &ctx,
        "SELECT ST_MakeBox2D(ST_PointZ(0, 0, 5), ST_PointZ(1, 1, 6))",
    )
    .await;
    let error = error.to_lowercase();
    assert!(
        error.contains("failed to coerce"),
        "unexpected error: {error}"
    );
    assert!(error.contains("st_makebox2d"), "unexpected error: {error}");

    let error = collect_error(&ctx, "SELECT ST_3DMakeBox(ST_Point(0, 0), ST_Point(1, 1))").await;
    let error = error.to_lowercase();
    assert!(
        error.contains("failed to coerce"),
        "unexpected error: {error}"
    );
    assert!(error.contains("st_3dmakebox"), "unexpected error: {error}");
}

#[tokio::test]
async fn expand_matches_postgis_boundary_values() {
    let ctx = context();
    let batch = collect(
        &ctx,
        "SELECT ST_AsText(ST_Expand(ST_GeomFromText('POINT(1 2)'), -1))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "POLYGON((2 3,2 1,0 1,0 3,2 3))"
    );

    let batch = collect(
        &ctx,
        "SELECT \
            ST_XMin(ST_Expand(ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20)), 10, 20)), \
            ST_YMin(ST_Expand(ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20)), 10, 20)), \
            ST_ZMin(ST_Expand(ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20)), 10, 20)), \
            ST_XMax(ST_Expand(ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20)), 10, 20)), \
            ST_YMax(ST_Expand(ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20)), 10, 20)), \
            ST_ZMax(ST_Expand(ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20)), 10, 20))\
        ",
    )
    .await;
    assert_f64_row(&batch, &[-10.0, -15.0, 0.0, 20.0, 40.0, 0.0]);

    let batch = collect(
        &ctx,
        "SELECT \
            ST_XMin(ST_Expand(ST_3DMakeBox(ST_PointZ(1, 2, 3), ST_PointZ(4, 5, 6)), 1, 2, 3)), \
            ST_YMin(ST_Expand(ST_3DMakeBox(ST_PointZ(1, 2, 3), ST_PointZ(4, 5, 6)), 1, 2, 3)), \
            ST_ZMin(ST_Expand(ST_3DMakeBox(ST_PointZ(1, 2, 3), ST_PointZ(4, 5, 6)), 1, 2, 3)), \
            ST_XMax(ST_Expand(ST_3DMakeBox(ST_PointZ(1, 2, 3), ST_PointZ(4, 5, 6)), 1, 2, 3)), \
            ST_YMax(ST_Expand(ST_3DMakeBox(ST_PointZ(1, 2, 3), ST_PointZ(4, 5, 6)), 1, 2, 3)), \
            ST_ZMax(ST_Expand(ST_3DMakeBox(ST_PointZ(1, 2, 3), ST_PointZ(4, 5, 6)), 1, 2, 3))\
        ",
    )
    .await;
    assert_f64_row(&batch, &[0.0, 0.0, 0.0, 5.0, 7.0, 9.0]);

    let batch = collect(
        &ctx,
        "SELECT ST_AsText(ST_GeomFromEWKB(ST_AsEWKB(ST_3DMakeBox(ST_PointZ(0, 0, 5), ST_PointZ(1, 1, 5)))))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "POLYGON Z((0 0 5,0 1 5,1 1 5,1 0 5,0 0 5))"
    );

    let batch = collect(
        &ctx,
        "SELECT ST_Expand(ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20)), CAST(NULL AS DOUBLE)) IS NULL",
    )
    .await;
    assert!(batch.column(0).as_boolean().value(0));

    let error = collect_error(
        &ctx,
        "SELECT ST_Expand(ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20)), 1, 2, 3)",
    )
    .await;
    assert!(error.contains("box input"), "unexpected error: {error}");

    let error = collect_error(
        &ctx,
        "SELECT ST_AsEWKB(ST_3DMakeBox(ST_PointZ(0, 0, 0), ST_PointZ(1, 1, 20)))",
    )
    .await;
    assert!(error.contains("constant Z"), "unexpected error: {error}");
}

#[tokio::test]
async fn envelope_shapes_and_empty_inputs_match_postgis() {
    let ctx = context();
    let batch = collect(
        &ctx,
        "SELECT \
            ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING(0 1,5 1)'))), \
            ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING(1 0,1 5)'))), \
            ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING(0 0,1 3)'))), \
            ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING Z (1 2 3,4 5 7)'))), \
            ST_AsText(ST_Envelope(ST_GeomFromText('POINT EMPTY'))), \
            ST_GeometryType(ST_Envelope(ST_GeomFromText('GEOMETRYCOLLECTION(POINT EMPTY)')))\
        ",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "LINESTRING(0 1,5 1)"
    );
    assert_eq!(
        batch.column(1).as_string::<i32>().value(0),
        "LINESTRING(1 0,1 5)"
    );
    assert_eq!(
        batch.column(2).as_string::<i32>().value(0),
        "POLYGON((0 0,0 3,1 3,1 0,0 0))"
    );
    assert_eq!(
        batch.column(3).as_string::<i32>().value(0),
        "POLYGON((1 2,1 5,4 5,4 2,1 2))"
    );
    assert_eq!(batch.column(4).as_string::<i32>().value(0), "POINT EMPTY");
    assert_eq!(
        batch.column(5).as_string_view().value(0),
        "ST_GeometryCollection"
    );
}
