use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, UInt32Type};
use arrow_array::{Array, BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{CoordType, GeometryType, Metadata, WktType};
use geodatafusion::udf::geo::processing::ConcaveHull;

const CRS_AUTHORITY: &str = "EPSG:3857";

fn crs_metadata() -> Arc<Metadata> {
    Arc::new(Metadata::new(
        Crs::from_authority_code(CRS_AUTHORITY.to_string()),
        Default::default(),
    ))
}

fn context_with_geometry_table(name: &str) -> SessionContext {
    let wkt_field =
        Field::new("wkt", DataType::Utf8, true).with_extension_type(WktType::new(crs_metadata()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        wkt_field,
        Field::new("pctconvex", DataType::Float64, true),
        Field::new("allow_holes", DataType::Boolean, true),
    ]));

    // Slice every column so row offsets and scalar/vector parameter handling are exercised.
    let ids = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).slice(1, 8);
    let wkt = StringArray::from(vec![
        Some("POINT(99 99)"),
        Some("MULTIPOINT((0 0),(3 0),(1 2),(0 4))"),
        Some("MULTIPOINT((1 2),(1 2))"),
        Some("MULTIPOINT((0 0),(1 1),(2 2))"),
        Some("POINT EMPTY"),
        Some("POLYGON((0 0,0 10,10 10,10 0,0 0),(4 4,4 6,6 6,6 4,4 4))"),
        Some("POLYGON((0 0,0 10,10 10,10 0,0 0),(4 4,4 6,6 6,6 4,4 4))"),
        Some("MULTIPOINT((0 0),(1 0),(1 1),(0 1))"),
        Some("MULTIPOINT((0 0),(1 0),(1 1),(0 1))"),
        Some("POINT(100 100)"),
    ])
    .slice(1, 8);
    let pctconvex = Float64Array::from(vec![
        Some(99.0),
        Some(0.1),
        Some(0.5),
        Some(0.5),
        Some(0.5),
        Some(0.5),
        Some(0.5),
        None,
        Some(0.5),
        Some(99.0),
    ])
    .slice(1, 8);
    let allow_holes = BooleanArray::from(vec![
        Some(false),
        Some(false),
        Some(false),
        Some(false),
        Some(true),
        Some(true),
        Some(false),
        Some(false),
        None,
        Some(false),
    ])
    .slice(1, 8);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(ids),
            Arc::new(wkt),
            Arc::new(pctconvex),
            Arc::new(allow_holes),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_table(
        name,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    geodatafusion::register(&ctx);
    ctx
}

async fn assert_planning_error(ctx: &SessionContext, sql: &str, expected: &str) {
    let error = ctx.sql(sql).await.expect_err("query planning should fail");
    assert!(
        error.to_string().contains(expected),
        "expected planning error containing `{expected}`, got: {error}"
    );
}

async fn assert_query_error(ctx: &SessionContext, sql: &str, expected: &str) {
    let result = async {
        let dataframe = ctx.sql(sql).await?;
        dataframe.collect().await
    }
    .await;
    let error = result.expect_err("query should fail");
    assert!(
        error.to_string().contains(expected),
        "expected query error containing `{expected}`, got: {error}"
    );
}

#[tokio::test]
async fn public_registration_and_parameter_semantics() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let batch = ctx
        .sql(
            "SELECT
                ST_Area(ST_ConcaveHull(
                    ST_GeomFromText('MULTIPOINT((0 0),(3 0),(1 2),(0 4))'), 1
                )) AS convex_area,
                ST_Area(ST_ConcaveHull(
                    ST_GeomFromText('MULTIPOINT((0 0),(3 0),(1 2),(0 4))'), 0.1
                )) AS concave_area,
                ST_Equals(
                    ST_ConcaveHull(
                        ST_GeomFromText('MULTIPOINT((0 0),(3 0),(1 2),(0 4))'), 0.1
                    ),
                    ST_GeomFromText('POLYGON((0 0,0 4,1 2,3 0,0 0))')
                ) AS matches_postgis,
                ST_AsText(ST_ConcaveHull(ST_GeomFromText('POINT EMPTY'), 0.5)) AS empty_point",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 6.0);
    assert_eq!(batch.column(1).as_primitive::<Float64Type>().value(0), 5.0);
    assert!(batch.column(2).as_boolean().value(0));
    assert_eq!(batch.column(3).as_string::<i32>().value(0), "POINT EMPTY");
}

#[tokio::test]
async fn nearly_collinear_points_match_postgis_polygon_classification() {
    // PostGIS 3.6.4 / GEOS 3.14.1 classifies this input as ST_Polygon.
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT
                GeometryType(ST_ConcaveHull(ST_GeomFromText(
                    'MULTIPOINT((0 0),(1 1),(2 2.000000000000001))'
                ), 0.5)) AS hull_type,
                ST_Equals(
                    ST_ConcaveHull(ST_GeomFromText(
                        'MULTIPOINT((0 0),(1 1),(2 2.000000000000001))'
                    ), 0.5),
                    ST_GeomFromText(
                        'POLYGON((2 2.000000000000001,1 1,0 0,2 2.000000000000001))'
                    )
                ) AS matches_postgis",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(batch.column(0).as_string_view().value(0), "POLYGON");
    assert!(batch.column(1).as_boolean().value(0));
}

#[tokio::test]
async fn vectorized_sliced_inputs_and_hole_policy_are_per_row() {
    let ctx = context_with_geometry_table("geometries");
    let batch = ctx
        .sql(
            "SELECT
                ST_AsText(ST_ConcaveHull(ST_GeomFromText(wkt), pctconvex, allow_holes)) AS hull,
                GeometryType(ST_ConcaveHull(ST_GeomFromText(wkt), pctconvex, allow_holes)) AS hull_type,
                ST_ConcaveHull(ST_GeomFromText(wkt), pctconvex, allow_holes) AS hull_geom
             FROM geometries
             ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let hulls = batch.column(0).as_string::<i32>();
    assert_eq!(hulls.value(0), "POLYGON((3 0,1 2,0 4,0 0,3 0))");
    assert_eq!(hulls.value(1), "POINT(1 2)");
    assert_eq!(hulls.value(2), "LINESTRING(0 0,2 2)");
    assert_eq!(hulls.value(3), "POINT EMPTY");
    assert_eq!(
        hulls.value(4),
        "POLYGON((10 0,10 10,0 10,0 0,10 0),(4 4,4 6,6 6,6 4,4 4))"
    );
    assert_eq!(hulls.value(5), "POLYGON((10 0,10 10,0 10,0 0,10 0))");
    assert!(hulls.is_null(6));
    assert!(hulls.is_null(7));

    let hull_types = batch.column(1).as_string_view();
    assert_eq!(hull_types.value(0), "POLYGON");
    assert_eq!(hull_types.value(1), "POINT");
    assert_eq!(hull_types.value(2), "LINESTRING");
    assert_eq!(hull_types.value(3), "POINT");

    let polygon_metrics = ctx
        .sql(
            "SELECT
                ST_NRings(ST_ConcaveHull(ST_GeomFromText(wkt), pctconvex, allow_holes)) AS rings,
                ST_Area(ST_ConcaveHull(ST_GeomFromText(wkt), pctconvex, allow_holes)) AS area,
                ST_IsValid(ST_ConcaveHull(ST_GeomFromText(wkt), pctconvex, allow_holes)) AS valid
             FROM geometries
             WHERE id IN (1, 5, 6)
             ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let rings = polygon_metrics.column(0).as_primitive::<UInt32Type>();
    assert_eq!(rings.value(1), 2);
    assert_eq!(rings.value(2), 1);

    let areas = polygon_metrics.column(1).as_primitive::<Float64Type>();
    assert_eq!(areas.value(0), 5.0);
    assert_eq!(areas.value(1), 96.0);
    assert_eq!(areas.value(2), 100.0);

    let geometry_type = batch
        .schema()
        .field(2)
        .try_extension_type::<GeometryType>()
        .unwrap();
    assert_eq!(geometry_type.coord_type(), CoordType::Separated);
    assert_eq!(
        geometry_type.metadata().crs(),
        &Crs::from_authority_code(CRS_AUTHORITY.to_string())
    );

    let valid = polygon_metrics.column(2).as_boolean();
    assert!(valid.value(0));
    assert!(valid.value(1));
    assert!(valid.value(2));
}

#[tokio::test]
async fn scalar_geometry_broadcasts_with_per_row_factors() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "WITH factors(pctconvex) AS (VALUES (0.1), (1.0))
             SELECT ST_Area(ST_ConcaveHull(
                 ST_GeomFromText('MULTIPOINT((0 0),(3 0),(1 2),(0 4))'),
                 pctconvex
             )) AS area
             FROM factors
             ORDER BY pctconvex",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let areas = batch.column(0).as_primitive::<Float64Type>();
    assert_eq!(areas.value(0), 5.0);
    assert_eq!(areas.value(1), 6.0);
}

#[tokio::test]
async fn nulls_are_propagated_and_bare_null_geometry_is_supported() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT
                ST_ConcaveHull(NULL, 0.5) IS NULL AS bare_null_geometry,
                ST_ConcaveHull(NULL, 1.1) IS NULL AS invalid_pct_with_null_geometry,
                ST_ConcaveHull(ST_GeomFromText(CAST(NULL AS VARCHAR)), 0.5) IS NULL AS typed_null_geometry,
                ST_ConcaveHull(ST_GeomFromText('POINT(1 2)'), CAST(NULL AS DOUBLE)) IS NULL AS null_pctconvex,
                ST_ConcaveHull(
                    ST_GeomFromText('POINT(1 2)'), 0.5, CAST(NULL AS BOOLEAN)
                ) IS NULL AS null_allow_holes
             FROM (VALUES (1), (2)) AS rows(id)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    for column in 0..5 {
        for row in 0..2 {
            assert!(batch.column(column).as_boolean().value(row));
        }
    }
}

#[tokio::test]
async fn empty_inputs_preserve_geometry_type() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT
                GeometryType(ST_ConcaveHull(ST_GeomFromText('POINT EMPTY'), 0.5)) AS point,
                GeometryType(ST_ConcaveHull(ST_GeomFromText('LINESTRING EMPTY'), 0.5)) AS line,
                GeometryType(ST_ConcaveHull(ST_GeomFromText('POLYGON EMPTY'), 0.5)) AS polygon,
                GeometryType(ST_ConcaveHull(ST_GeomFromText('MULTIPOINT EMPTY'), 0.5)) AS multipoint,
                GeometryType(ST_ConcaveHull(ST_GeomFromText('MULTILINESTRING EMPTY'), 0.5)) AS multiline,
                GeometryType(ST_ConcaveHull(ST_GeomFromText('MULTIPOLYGON EMPTY'), 0.5)) AS multipolygon,
                GeometryType(ST_ConcaveHull(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'), 0.5)) AS collection,
                ST_NDims(ST_ConcaveHull(ST_GeomFromText('POINT Z EMPTY'), 0.5)) AS point_z_ndims,
                ST_NDims(ST_ConcaveHull(ST_GeomFromText('POINT M EMPTY'), 0.5)) AS point_m_ndims",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let expected = [
        "POINT",
        "LINESTRING",
        "POLYGON",
        "MULTIPOINT",
        "MULTILINESTRING",
        "MULTIPOLYGON",
        "GEOMETRYCOLLECTION",
    ];
    for (column, expected) in expected.into_iter().enumerate() {
        assert_eq!(
            batch.column(column).as_string_view().value(0),
            expected,
            "column {column}"
        );
    }
    assert_eq!(
        batch
            .column(7)
            .as_primitive::<arrow_array::types::UInt8Type>()
            .value(0),
        3
    );
    assert_eq!(
        batch
            .column(8)
            .as_primitive::<arrow_array::types::UInt8Type>()
            .value(0),
        2
    );
}

#[tokio::test]
async fn non_empty_supported_geometry_families_are_processed() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT
                ST_IsEmpty(ST_ConcaveHull(ST_GeomFromText('POINT(0 0)'), 0.5)) AS point,
                ST_IsEmpty(ST_ConcaveHull(ST_GeomFromText('LINESTRING(0 0,1 0,0 1)'), 0.5)) AS line,
                ST_IsEmpty(ST_ConcaveHull(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))'), 0.5)) AS polygon,
                ST_IsEmpty(ST_ConcaveHull(ST_GeomFromText('MULTIPOINT((0 0),(1 0),(0 1))'), 0.5)) AS multipoint,
                ST_IsEmpty(ST_ConcaveHull(ST_GeomFromText(
                    'MULTILINESTRING((0 0,1 0),(0 1,1 1))'
                ), 0.5)) AS multiline,
                ST_IsEmpty(ST_ConcaveHull(ST_GeomFromText(
                    'MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)),((2 0,2 1,3 1,3 0,2 0)))'
                ), 0.5)) AS multipolygon,
                ST_IsEmpty(ST_ConcaveHull(ST_GeomFromText(
                    'GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 1,1 2),POLYGON((2 0,2 1,3 1,3 0,2 0)))'
                ), 0.5)) AS collection",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    for column in 0..7 {
        assert!(!batch.column(column).as_boolean().value(0));
    }
}

#[tokio::test]
async fn z_is_preserved_and_m_is_dropped() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT
                ST_AsText(ST_ConcaveHull(
                    ST_GeomFromText('MULTIPOINT Z ((0 0 1),(1 0 2),(0 1 3),(1 1 4))'),
                    0.5
                )) AS with_z,
                ST_AsText(ST_ConcaveHull(
                    ST_GeomFromText('MULTIPOINT M ((0 0 1),(1 0 2),(0 1 3),(1 1 4))'),
                    0.5
                )) AS with_m",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "POLYGON Z((1 0 2,1 1 4,0 1 3,0 0 1,1 0 2))"
    );
    assert_eq!(
        batch.column(1).as_string::<i32>().value(0),
        "POLYGON((1 0,1 1,0 1,0 0,1 0))"
    );
}

#[tokio::test]
async fn custom_coord_type_is_honored() {
    let ctx = SessionContext::new();
    ctx.register_udf(ConcaveHull::new(CoordType::Interleaved).into());
    ctx.register_udf(geodatafusion::udf::native::io::GeomFromText::default().into());
    let batch = ctx
        .sql(
            "SELECT ST_ConcaveHull(
                ST_GeomFromText('MULTIPOINT((0 0),(3 0),(1 2),(0 4))'), 0.1
            ) AS hull",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let geometry_type = batch
        .schema()
        .field(0)
        .try_extension_type::<GeometryType>()
        .unwrap();
    assert_eq!(geometry_type.coord_type(), CoordType::Interleaved);
}

#[tokio::test]
async fn invalid_parameters_fail_with_clear_errors() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    for pctconvex in [
        "-0.1",
        "1.1",
        "CAST('NaN' AS DOUBLE)",
        "CAST('Infinity' AS DOUBLE)",
    ] {
        assert_query_error(
            &ctx,
            &format!("SELECT ST_ConcaveHull(ST_GeomFromText('POINT(1 2)'), {pctconvex})"),
            "pctconvex",
        )
        .await;
    }

    assert_planning_error(
        &ctx,
        "SELECT ST_ConcaveHull(ST_GeomFromText('POINT(1 2)'), '0.5')",
        "numeric pctconvex",
    )
    .await;
    assert_planning_error(
        &ctx,
        "SELECT ST_ConcaveHull(1, 0.5)",
        "does not support input type",
    )
    .await;
    assert_planning_error(
        &ctx,
        "SELECT ST_ConcaveHull(ST_GeomFromText('POINT(1 2)'), 0.5, 'yes')",
        "boolean allow_holes",
    )
    .await;
    assert_query_error(
        &ctx,
        "SELECT ST_ConcaveHull(ST_Point(CAST('NaN' AS DOUBLE), 0), 0.5)",
        "non-finite",
    )
    .await;

    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT ST_ConcaveHull(
                ST_GeomFromText(wkt),
                pctconvex
            ) FROM (VALUES ('POINT(1 2)', -0.1)) AS inputs(wkt, pctconvex)",
        )
        .await
        .unwrap()
        .collect()
        .await;
    assert!(batch.is_err(), "per-row invalid pctconvex must fail");
}
