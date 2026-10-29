use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geo_traits::GeometryTrait;
use geoarrow_array::array::GeometryArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Crs, Dimension, GeoArrowType, Metadata, PointType, PolygonType};

#[path = "support/oracle.rs"]
#[allow(dead_code)]
mod oracle;

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|error| panic!("failed to plan {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("failed to execute {sql}: {error}"))
}

#[tokio::test]
async fn public_registration_exposes_scalar_and_table_grids() {
    let ctx = context();

    let batches = collect(
        &ctx,
        "SELECT ST_AsText(ST_Hexagon(1, 0, 0)) AS hexagon, \
                ST_AsText(ST_Square(1, 0, 0)) AS square",
    )
    .await;
    let batch = &batches[0];
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "POLYGON((-1 0,-0.5 -0.8660254037844387,0.5 -0.8660254037844387,1 0,0.5 0.8660254037844387,-0.5 0.8660254037844387,-1 0))"
    );
    assert_eq!(
        batch.column(1).as_string::<i32>().value(0),
        "POLYGON((0 0,0 1,1 1,1 0,0 0))"
    );

    let batches = collect(
        &ctx,
        "SELECT i, j FROM ST_SquareGrid(1, ST_MakeEnvelope(-1, -1, 1, 1)) AS grid \
         ORDER BY i, j",
    )
    .await;
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 9);
    let i = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int32Type>();
    let j = batch
        .column(1)
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!((i.value(0), j.value(0)), (-1, -1));
    assert_eq!((i.value(8), j.value(8)), (1, 1));

    let batches = collect(
        &ctx,
        "SELECT i, j FROM st_hexagongrid(1, ST_MakeEnvelope(-1, -1, 1, 1)) AS grid \
         ORDER BY i, j",
    )
    .await;
    assert_eq!(batches[0].num_rows(), 7);
}

#[tokio::test]
async fn scalar_grids_preserve_crs_and_drop_zm_coordinates() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_Hexagon(1, 0, 0, ST_PointFromText('POINT ZM(10 20 30 40)', 4326)) AS hexagon, \
                ST_Square(1, 0, 0, ST_PointFromText('POINT ZM(10 20 30 40)', 4326)) AS square, \
                ST_AsText(ST_Hexagon(1, 0, 0, ST_PointFromText('POINT ZM(10 20 30 40)', 4326))) AS hexagon_text",
    )
    .await;
    let batch = &batches[0];

    for column in 0..2 {
        let polygon_type = batch
            .schema()
            .field(column)
            .try_extension_type::<PolygonType>()
            .unwrap();
        assert_eq!(polygon_type.dimension(), Dimension::XY);
        assert_eq!(
            polygon_type.metadata().crs(),
            &Crs::from_authority_code("EPSG:4326".to_string())
        );
    }
    assert_eq!(
        batch.column(2).as_string::<i32>().value(0),
        "POLYGON((9 20,9.5 19.133974596215563,10.5 19.133974596215563,11 20,10.5 20.866025403784437,9.5 20.866025403784437,9 20))"
    );
}

#[tokio::test]
async fn grid_tables_preserve_bounds_crs_and_dimensions() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT i, j, geom \
         FROM st_squaregrid(0.5, ST_PointFromText('POINT ZM(-0.25 0.25 1 2)', 4326)) AS grid",
    )
    .await;
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    let schema = batch.schema();
    let geometry_type = GeoArrowType::from_arrow_field(schema.field(2)).unwrap();
    assert_eq!(
        geometry_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
    assert!(!schema.field(0).is_nullable());
    assert!(!schema.field(1).is_nullable());
    assert!(!schema.field(2).is_nullable());

    let geometries = GeometryArray::try_from((batch.column(2).as_ref(), schema.field(2))).unwrap();
    let geometry = geometries.value(0).unwrap();
    assert_eq!(geometry.dim(), geo_traits::Dimensions::Xy);
    assert_eq!(
        geometry_type.metadata().crs(),
        &Crs::from_authority_code("EPSG:4326".to_string())
    );
}

#[tokio::test]
async fn grid_tables_return_no_rows_for_negative_size_empty_or_null_bounds() {
    let ctx = context();
    for query in [
        "SELECT count(*) FROM st_hexagongrid(1, ST_GeomFromText('POLYGON EMPTY'))",
        "SELECT count(*) FROM st_hexagongrid(1, NULL)",
        "SELECT count(*) FROM st_squaregrid(0, ST_MakeEnvelope(-1, -1, 1, 1))",
        "SELECT count(*) FROM st_squaregrid(-1, ST_MakeEnvelope(-1, -1, 1, 1))",
        "SELECT count(*) FROM st_squaregrid(NULL, ST_MakeEnvelope(-1, -1, 1, 1))",
    ] {
        let batches = collect(&ctx, query).await;
        let batch = &batches[0];
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            0,
            "{query}"
        );
    }
}

#[tokio::test]
async fn grid_tables_reject_non_finite_sizes() {
    let ctx = context();
    for sql in [
        "SELECT * FROM st_squaregrid(CAST('NaN' AS DOUBLE), ST_MakeEnvelope(0, 0, 1, 1))",
        "SELECT * FROM st_hexagongrid(CAST('Infinity' AS DOUBLE), ST_MakeEnvelope(0, 0, 1, 1))",
    ] {
        let error = ctx
            .sql(sql)
            .await
            .expect_err("non-finite grid size must fail");
        assert!(
            error.to_string().contains("requires a finite size"),
            "expected finite-size error, got {error}"
        );
    }
}

#[tokio::test]
async fn scalar_grids_handle_sliced_nonnullable_inputs_and_nullable_parameters() {
    let ctx = context();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let point_type = PointType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
    let origin = PointBuilder::from_nullable_points(
        [
            Some(&geo::Point::new(1.0, 2.0)),
            Some(&geo::Point::new(10.0, 20.0)),
        ]
        .into_iter(),
        point_type,
    )
    .finish();

    let schema = Arc::new(Schema::new(vec![
        Arc::new(origin.data_type().to_field("origin", false)),
        Arc::new(Field::new("size", DataType::Float64, true)),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            origin.to_array_ref(),
            Arc::new(Float64Array::from(vec![Some(1.0), None])),
        ],
    )
    .unwrap();

    ctx.register_batch("full", batch.clone()).unwrap();
    ctx.register_batch("sliced", batch.slice(1, 1)).unwrap();

    let batches = collect(
        &ctx,
        "SELECT ST_Square(size, 0, 0, origin) AS square, \
                ST_Hexagon(size, 0, 0, origin) AS hexagon, \
                ST_Square(size, 0, 0, origin) IS NULL AS square_is_null, \
                ST_Hexagon(size, 0, 0, origin) IS NULL AS hexagon_is_null \
         FROM full ORDER BY size NULLS LAST",
    )
    .await;
    let batch = &batches[0];
    let square_is_null = batch.column(2).as_boolean();
    let hexagon_is_null = batch.column(3).as_boolean();
    assert!(!square_is_null.value(0));
    assert!(!hexagon_is_null.value(0));
    assert!(square_is_null.value(1));
    assert!(hexagon_is_null.value(1));

    let batches = collect(
        &ctx,
        "SELECT ST_Square(size, 0, 0, origin) IS NULL AS square_is_null, \
                ST_Hexagon(size, 0, 0, origin) IS NULL AS hexagon_is_null \
         FROM sliced",
    )
    .await;
    let batch = &batches[0];
    assert!(batch.column(0).as_boolean().value(0));
    assert!(batch.column(1).as_boolean().value(0));
}

#[tokio::test]
async fn scalar_grids_reject_invalid_origins() {
    let ctx = context();
    for (sql, expected) in [
        (
            "SELECT ST_Hexagon(1, 0, 0, ST_GeomFromText('LINESTRING(0 0,1 1)'))",
            "origin argument is not a point",
        ),
        (
            "SELECT ST_Square(1, 0, 0, ST_GeomFromText('POINT EMPTY'))",
            "origin point is empty",
        ),
    ] {
        let error = ctx
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("invalid origin must fail");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[tokio::test]
async fn grids_match_reviewed_postgis_baselines()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ctx = context();
    for group_name in [
        "grids/hexagon",
        "grids/square",
        "grids/nullability",
        "grids/hexagon_grid",
        "grids/square_grid",
    ] {
        let group = oracle::load_group(group_name, true)?;
        let stats = oracle::run_offline(&ctx, &group).await?;
        assert!(
            stats.compared > 0 && stats.rows_compared > 0,
            "expected reviewed grid cases in {group_name}: {}",
            stats.summary()
        );
    }
    Ok(())
}
