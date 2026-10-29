#![cfg(feature = "geos-3_11")]

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{GeometryType, Metadata, WktType};

const CRS_AUTHORITY: &str = "EPSG:4326";

#[tokio::test]
async fn public_registration_exposes_concave_hull() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT ST_AsText(ST_ConcaveHull(
                ST_GeomFromText('MULTIPOINT((0 0),(1 0),(0 1),(1 1))'),
                0.5
            ))",
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
        "POLYGON((1 0,0 0,0 1,1 1,1 0))"
    );
}

#[tokio::test]
async fn concave_hull_bare_null_returns_null() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql("SELECT ST_ConcaveHull(NULL, 0.5) IS NULL")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    assert!(batch.column(0).as_boolean().value(0));
}

#[tokio::test]
async fn concave_hull_vectorizes_parameters_and_preserves_crs() {
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code(CRS_AUTHORITY.to_string()),
        Default::default(),
    ));
    let wkt_field =
        Field::new("wkt", DataType::Utf8, true).with_extension_type(WktType::new(metadata));
    let schema = Arc::new(Schema::new(vec![Arc::new(wkt_field)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![
            Some("MULTIPOINT((0 0),(1 0),(0 1),(1 1))"),
            Some("POINT EMPTY"),
        ]))],
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_table(
        "geometries",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    geodatafusion::register(&ctx);

    let metadata_batch = ctx
        .sql("SELECT ST_ConcaveHull(ST_GeomFromText(wkt), 0.5) FROM geometries")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        metadata_batch
            .schema()
            .field(0)
            .try_extension_type::<GeometryType>()
            .unwrap()
            .metadata()
            .crs(),
        &Crs::from_authority_code(CRS_AUTHORITY.to_string())
    );

    let batches = ctx
        .sql(
            "WITH input(id, wkt, ratio, allow_holes) AS (VALUES \
                (1, 'MULTIPOINT((0 0),(1 0),(0 1),(1 1))', 0.5, false), \
                (2, 'POINT EMPTY', 0.5, true), \
                (3, 'POINT(1 2)', CAST(NULL AS DOUBLE), false), \
                (4, 'POINT(1 2)', 0.5, CAST(NULL AS BOOLEAN))) \
             SELECT id, ST_AsText(ST_ConcaveHull(ST_GeomFromText(wkt), ratio, allow_holes)) AS hull \
             FROM input ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 4);

    let hulls = batch.column(1).as_string::<i32>();
    assert_eq!(hulls.value(0), "POLYGON((1 0,0 0,0 1,1 1,1 0))");
    assert_eq!(hulls.value(1), "POLYGON EMPTY");
    assert!(hulls.is_null(2));
    assert!(hulls.is_null(3));
}
