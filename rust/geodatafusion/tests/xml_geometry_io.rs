//! Public SQL and raw-Arrow contracts for GML/KML input and coordinate text output.

mod support;

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::{CoordType, Dimension};
use support::context::context;

async fn sql_text(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 1);
    assert!(!batch.column(0).is_null(0));
    batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap()
        .value(0)
        .to_string()
}

async fn sql_boolean(ctx: &SessionContext, sql: &str) -> bool {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::BooleanArray>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn gml_and_kml_constructors_through_public_register() {
    let ctx = context();

    assert_eq!(
        sql_text(
            &ctx,
            r#"SELECT ST_AsEWKT(ST_GeomFromGML(
                '<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2"><gml:pos>30 10</gml:pos></gml:Point>'
            ))"#
        ).await,
        "POINT(30 10)"
    );
    assert_eq!(
        sql_text(
            &ctx,
            r#"SELECT ST_AsEWKT(ST_GeomFromGML(
                '<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2" srsName="EPSG:4326"><gml:pos>30 10</gml:pos></gml:Point>'
            ))"#
        ).await,
        "SRID=4326;POINT(30 10)"
    );
    assert_eq!(
        sql_text(
            &ctx,
            r#"SELECT ST_AsEWKT(ST_GMLToSQL(
                '<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::4326"><gml:pos>10 20</gml:pos></gml:Point>'
            ))"#
        ).await,
        "SRID=4326;POINT(20 10)"
    );
    assert_eq!(
        sql_text(
            &ctx,
            r#"SELECT ST_AsEWKT(ST_GeomFromKML('<Point><coordinates>1,2</coordinates></Point>'))"#
        )
        .await,
        "SRID=4326;POINT(1 2)"
    );
}

#[tokio::test]
async fn gml_preserves_four_dimensions_and_empty_geometries() {
    let ctx = context();

    assert_eq!(
        sql_text(
            &ctx,
            r#"SELECT ST_AsText(ST_GeomFromGML(
                '<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2"><gml:pos srsDimension="4">1 2 3 4</gml:pos></gml:Point>'
            ))"#
        ).await,
        "POINT ZM(1 2 3 4)"
    );
    assert!(
        sql_boolean(
            &ctx,
            r#"SELECT ST_AsEWKT(ST_GeomFromGML(
            '<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2"/>'
        )) = 'POINT EMPTY'"#
        )
        .await
    );
    assert!(sql_boolean(&ctx, "SELECT ST_AsEWKT(ST_GeomFromGML(NULL)) IS NULL").await);
    assert!(sql_boolean(&ctx, "SELECT ST_AsEWKT(ST_GeomFromKML(NULL)) IS NULL").await);
}

#[tokio::test]
async fn gml_output_options_match_postgis_examples() {
    let ctx = context();

    assert_eq!(
        sql_text(
            &ctx,
            "SELECT ST_AsGML(ST_GeomFromEWKT('SRID=4326;POLYGON((0 0,0 1,1 1,1 0,0 0))'))"
        )
        .await,
        "<gml:Polygon srsName=\"EPSG:4326\"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>0,0 0,1 1,1 1,0 0,0</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>"
    );
    assert_eq!(
        sql_text(
            &ctx,
            "SELECT ST_AsGML(3, ST_GeomFromEWKT('SRID=4326;POINT(5.234234233242 6.34534534534)'), 5, 17)"
        ).await,
        "<gml:Point srsName=\"urn:ogc:def:crs:EPSG::4326\"><gml:pos srsDimension=\"2\">6.34535 5.23423</gml:pos></gml:Point>"
    );
    assert_eq!(
        sql_text(
            &ctx,
            "SELECT ST_AsGML(3, ST_GeomFromEWKT('SRID=4326;LINESTRING(1 2,3 4,10 20)'), 5, 32)"
        )
        .await,
        "<gml:Envelope srsName=\"EPSG:4326\" srsDimension=\"2\"><gml:lowerCorner>1 2</gml:lowerCorner><gml:upperCorner>10 20</gml:upperCorner></gml:Envelope>"
    );
}

#[tokio::test]
async fn kml_and_latlon_outputs_match_postgis_examples() {
    let ctx = context();

    assert_eq!(
        sql_text(
            &ctx,
            "SELECT ST_AsKML(ST_GeomFromEWKT('SRID=4326;POLYGON((0 0,0 1,1 1,1 0,0 0))'))"
        )
        .await,
        "<Polygon><outerBoundaryIs><LinearRing><coordinates>0,0 0,1 1,1 1,0 0,0</coordinates></LinearRing></outerBoundaryIs></Polygon>"
    );
    assert_eq!(
        sql_text(
            &ctx,
            "SELECT ST_AsKML(ST_GeomFromEWKT('SRID=4326;LINESTRING Z(1 2 3,4 5 6)'))"
        )
        .await,
        "<LineString><coordinates>1,2,3 4,5,6</coordinates></LineString>"
    );
    assert_eq!(
        sql_text(
            &ctx,
            "SELECT ST_AsLatLonText(ST_GeomFromText('POINT(-3.2342342 -2.32498)'))"
        )
        .await,
        "2°19'29.928\"S 3°14'3.243\"W"
    );
}

#[tokio::test]
async fn non_literal_gml_columns_cannot_declare_per_row_srid() {
    let ctx = context();
    let error = ctx
        .sql(
            r#"SELECT ST_AsEWKT(ST_GeomFromGML(gml)) FROM (VALUES
                ('<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2" srsName="EPSG:4326"><gml:pos>1 2</gml:pos></gml:Point>')
            ) AS t(gml)"#,
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("SRID 4326"));
}

#[tokio::test]
async fn raw_arrow_columns_and_coordinate_layouts_work() {
    let ctx = context();
    let schema = Arc::new(Schema::new(vec![Field::new("kml", DataType::Utf8, false)]));
    let values = StringArray::from(vec![
        Some("<Point><coordinates>1,2</coordinates></Point>"),
        Some("<Point><coordinates>3,4,7</coordinates></Point>"),
    ]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(values.clone())]).unwrap();
    ctx.register_batch("kml_rows", batch).unwrap();

    let batches = ctx
        .sql("SELECT ST_AsText(ST_GeomFromKML(kml)) FROM kml_rows")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let strings = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(strings.value(0), "POINT(1 2)");
    assert_eq!(strings.value(1), "POINT Z(3 4 7)");

    let sliced = Arc::new(values.slice(1, 1));
    let slice_schema = Arc::new(Schema::new(vec![Field::new("kml", DataType::Utf8, false)]));
    ctx.register_batch(
        "kml_slice",
        RecordBatch::try_new(slice_schema, vec![sliced]).unwrap(),
    )
    .unwrap();
    let batches = ctx
        .sql("SELECT ST_AsText(ST_GeomFromKML(kml)) FROM kml_slice")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .value(0),
        "POINT Z(3 4 7)"
    );

    let separated = geoarrow_array::test::point::array(CoordType::Separated, Dimension::XY);
    let interleaved = geoarrow_array::test::point::array(CoordType::Interleaved, Dimension::XY);
    let batch_schema = Arc::new(Schema::new([
        separated.data_type().to_field("separated", true).into(),
        interleaved.data_type().to_field("interleaved", true).into(),
    ]));
    let batch = RecordBatch::try_new(
        batch_schema,
        vec![separated.into_array_ref(), interleaved.into_array_ref()],
    )
    .unwrap();
    ctx.register_batch("coord_types", batch).unwrap();
    let batches = ctx
        .sql("SELECT ST_AsGML(separated), ST_AsGML(interleaved) FROM coord_types")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let left = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap()
        .value(0);
    let right = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(left, right);
    assert!(left.contains("<gml:Point>"));
}
