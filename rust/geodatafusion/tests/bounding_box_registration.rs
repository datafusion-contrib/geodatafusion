use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{GeometryType, Metadata, WktType};

const CRS_AUTHORITY: &str = "EPSG:3857";

fn context_with_crs() -> SessionContext {
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
            Some("LINESTRING(1 2,3 4)"),
            None,
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
    ctx
}

#[tokio::test]
async fn public_registration_exposes_envelope_and_expand() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT \
                ST_AsText(ST_Envelope(ST_GeomFromText('POINT(1 2)'))) AS envelope, \
                ST_AsText(ST_Expand(ST_GeomFromText('POINT(1 2)'), 1)) AS expand",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(batch.column(0).as_string::<i32>().value(0), "POINT(1 2)");
    assert_eq!(
        batch.column(1).as_string::<i32>().value(0),
        "POLYGON((0 1,0 3,2 3,2 1,0 1))"
    );
}

#[tokio::test]
async fn null_literals_return_null_geometries() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT \
                ST_Envelope(NULL) IS NULL AS envelope_null, \
                ST_Expand(NULL, 1) IS NULL AS expand_null",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    assert!(batch.column(0).as_boolean().value(0));
    assert!(batch.column(1).as_boolean().value(0));
}

#[tokio::test]
async fn envelope_and_expand_preserve_crs_metadata() {
    let ctx = context_with_crs();
    let batch = ctx
        .sql(
            "SELECT \
                ST_Envelope(ST_GeomFromText(wkt)) AS envelope, \
                ST_Expand(ST_GeomFromText(wkt), 1) AS expand \
             FROM geometries ORDER BY wkt NULLS LAST",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let schema = batch.schema();
    for column in 0..2 {
        let field = schema.field(column);
        let geometry_type = field.try_extension_type::<GeometryType>().unwrap();
        assert_eq!(
            geometry_type.metadata().crs(),
            &Crs::from_authority_code(CRS_AUTHORITY.to_string())
        );
    }
}

#[tokio::test]
async fn expand_handles_nulls_empty_inputs_and_implicit_axes() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT \
                ST_AsText(ST_Expand(ST_GeomFromText('POINT Z (2 3 4)'), 1)) AS two_args, \
                ST_AsText(ST_Expand(ST_GeomFromText('POINT Z (2 3 4)'), 1, 2)) AS three_args, \
                ST_AsText(ST_Expand(ST_GeomFromText('POINT EMPTY'), 1)) AS empty, \
                ST_Expand(ST_GeomFromText(CAST(NULL AS TEXT)), 1) IS NULL AS null_geometry, \
                ST_Expand(ST_GeomFromText('POINT(0 0)'), CAST(NULL AS DOUBLE)) IS NULL AS null_factor",
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
        "POLYGON Z((1 2 3,1 4 3,3 4 5,3 2 5,1 2 3))"
    );
    assert_eq!(
        batch.column(1).as_string::<i32>().value(0),
        "POLYGON Z((1 1 4,1 5 4,3 5 4,3 1 4,1 1 4))"
    );
    assert_eq!(batch.column(2).as_string::<i32>().value(0), "POINT EMPTY");
    assert!(batch.column(3).as_boolean().value(0));
    assert!(batch.column(4).as_boolean().value(0));
    assert!(batch.column(1).is_valid(0));
}

#[tokio::test]
async fn empty_geometry_collection_bounds_preserve_collection_identity() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT \
                ST_GeometryType(ST_GeomFromText('GEOMETRYCOLLECTION(POINT EMPTY)')) AS input_type, \
                ST_GeometryType(ST_Envelope(ST_GeomFromText('GEOMETRYCOLLECTION(POINT EMPTY)'))), \
                ST_GeometryType(ST_Expand(ST_GeomFromText('GEOMETRYCOLLECTION(POINT EMPTY)'), 1))",
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
        (
            batch.column(0).as_string_view().value(0),
            batch.column(1).as_string_view().value(0),
            batch.column(2).as_string_view().value(0)
        ),
        (
            "ST_GeometryCollection",
            "ST_GeometryCollection",
            "ST_GeometryCollection"
        )
    );
}
