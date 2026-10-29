use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use geo_traits::{GeometryCollectionTrait, GeometryTrait, PointTrait};
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

fn parse_wkt(value: &str) -> Wkt<f64> {
    Wkt::from_str(value).unwrap()
}

fn register_geometry_batch(context: &SessionContext, array: impl GeoArrowArray) -> Field {
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

async fn collect_result(
    context: &SessionContext,
    sql: &str,
) -> std::result::Result<RecordBatch, DataFusionError> {
    let dataframe = context.sql(sql).await?;
    let batches = dataframe.collect().await?;
    assert_eq!(batches.len(), 1);
    Ok(batches.into_iter().next().unwrap())
}

fn output_geometry(batch: &RecordBatch) -> GeometryArray {
    GeometryArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0))).unwrap()
}

#[tokio::test]
async fn force_collection_keeps_explicit_geometry_collections() {
    let context = new_context();
    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "wkt",
                    arrow_schema::DataType::Utf8,
                    true,
                )])),
                vec![Arc::new(StringArray::from(vec![
                    Some("GEOMETRYCOLLECTION(POINT(1 2))"),
                    Some("POINT(3 4)"),
                    Some("MULTIPOINT((5 6))"),
                    None,
                ])) as ArrayRef],
            )
            .unwrap(),
        )
        .unwrap();

    let batch = collect(
        &context,
        "SELECT ST_ForceCollection(ST_GeomFromText(wkt)) FROM input",
    )
    .await;
    assert_eq!(
        GeoArrowType::from_arrow_field(batch.schema().field(0)).unwrap(),
        GeoArrowType::Geometry(
            GeometryType::new(Default::default()).with_coord_type(CoordType::Separated)
        )
    );

    let output = output_geometry(&batch);
    assert_eq!(output.len(), 4);
    assert!(output.is_null(3));

    for index in 0..3 {
        let geometry = output.value(index).unwrap();
        let geo_traits::GeometryType::GeometryCollection(collection) = geometry.as_type() else {
            panic!("expected a geometry collection at row {index}");
        };
        assert_eq!(collection.num_geometries(), 1);
        let child = collection.geometry(0).unwrap();
        let geo_traits::GeometryType::Point(point) = child.as_type() else {
            panic!("expected a point child at row {index}");
        };
        assert!(point.coord().is_some());
    }
}

#[tokio::test]
async fn collection_editors_emit_heterogeneous_generic_geometry_values() {
    let context = new_context();
    let geometry_type = GeometryType::new(Default::default()).with_coord_type(CoordType::Separated);
    let mut builder = GeometryBuilder::new(geometry_type.clone());
    builder
        .push_geometry(Some(&parse_wkt("POINT(1 2)")))
        .unwrap();
    builder
        .push_geometry(Some(&parse_wkt("POLYGON((0 0,1 0,1 1,0 0))")))
        .unwrap();
    let input = builder.finish();

    let field = register_geometry_batch(&context, input);
    assert_eq!(
        GeoArrowType::from_arrow_field(&field).unwrap(),
        GeoArrowType::Geometry(geometry_type)
    );

    let batch = collect(&context, "SELECT ST_Multi(geom) FROM t").await;
    let output = output_geometry(&batch);
    assert_eq!(output.len(), 2);
    assert!(matches!(
        output.value(0).unwrap().as_type(),
        geo_traits::GeometryType::MultiPoint(_)
    ));
    assert!(matches!(
        output.value(1).unwrap().as_type(),
        geo_traits::GeometryType::MultiPolygon(_)
    ));

    let batch = collect(&context, "SELECT ST_CollectionExtract(geom, 1) FROM t").await;
    let output = output_geometry(&batch);
    let first = output.value(0).unwrap();
    let geo_traits::GeometryType::Point(point) = first.as_type() else {
        panic!("expected the matching atomic point");
    };
    assert!(point.coord().is_some());
    let second = output.value(1).unwrap();
    let geo_traits::GeometryType::Point(point) = second.as_type() else {
        panic!("expected an empty point");
    };
    assert!(point.coord().is_none());
}

#[tokio::test]
async fn collection_editors_preserve_crs_metadata() {
    let context = new_context();
    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("EPSG:4326".to_string()),
        None,
    ));
    let geometry_type = GeometryType::new(metadata).with_coord_type(CoordType::Separated);
    let mut builder = GeometryBuilder::new(geometry_type);
    builder
        .push_geometry(Some(&parse_wkt("GEOMETRYCOLLECTION(POINT(1 2))")))
        .unwrap();
    let input = builder.finish();
    register_geometry_batch(&context, input);

    for sql in [
        "SELECT ST_CollectionExtract(geom) FROM t",
        "SELECT ST_CollectionHomogenize(geom) FROM t",
        "SELECT ST_ForceCollection(geom) FROM t",
        "SELECT ST_Multi(geom) FROM t",
        "SELECT ST_ForcePolygonCCW(geom) FROM t",
        "SELECT ST_ForcePolygonCW(geom) FROM t",
        "SELECT ST_ForceRHR(geom) FROM t",
        "SELECT ST_ForceSFS(geom) FROM t",
    ] {
        let batch = collect(&context, sql).await;
        let metadata = Metadata::try_from(batch.schema().field(0)).unwrap();
        assert_eq!(
            metadata.crs(),
            &Crs::from_authority_code("EPSG:4326".to_string()),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn collection_editors_preserve_coordinate_layout() {
    let context = new_context();
    let geometry_type =
        GeometryType::new(Default::default()).with_coord_type(CoordType::Interleaved);
    let mut builder = GeometryBuilder::new(geometry_type);
    builder
        .push_geometry(Some(&parse_wkt("POINT ZM(1 2 3 4)")))
        .unwrap();
    builder
        .push_geometry(Some(&parse_wkt("MULTIPOINT((1 2),(3 4))")))
        .unwrap();
    let input = builder.finish().slice(1, 1);
    register_geometry_batch(&context, input);

    for (sql, expected_collection) in [
        ("SELECT ST_CollectionExtract(geom) FROM t", false),
        ("SELECT ST_ForceCollection(geom) FROM t", true),
        ("SELECT ST_Multi(geom) FROM t", false),
        ("SELECT ST_ForceSFS(geom) FROM t", false),
    ] {
        let batch = collect(&context, sql).await;
        let output_type = GeoArrowType::from_arrow_field(batch.schema().field(0)).unwrap();
        assert_eq!(
            output_type.coord_type(),
            Some(CoordType::Interleaved),
            "{sql}"
        );
        let output = output_geometry(&batch);
        assert_eq!(output.len(), 1);
        let value = output.value(0).unwrap();
        if expected_collection {
            assert!(matches!(
                value.as_type(),
                geo_traits::GeometryType::GeometryCollection(_)
            ));
        } else {
            assert!(matches!(
                value.as_type(),
                geo_traits::GeometryType::MultiPoint(_)
            ));
        }
    }
}

#[tokio::test]
async fn collection_editor_arguments_follow_postgis_strictness() {
    let context = new_context();

    for sql in [
        "SELECT ST_CollectionExtract(ST_GeomFromText('POINT(1 2)'), NULL)",
        "SELECT ST_ForceSFS(ST_GeomFromText('POINT(1 2)'), NULL)",
    ] {
        let batch = collect(&context, sql).await;
        assert!(output_geometry(&batch).is_null(0), "{sql}");
    }

    context
        .register_batch(
            "input",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "wkt",
                    arrow_schema::DataType::Utf8,
                    true,
                )])),
                vec![Arc::new(StringArray::from(vec![None, Some("POINT(1 2)")])) as ArrayRef],
            )
            .unwrap(),
        )
        .unwrap();

    let error = collect_result(
        &context,
        "SELECT ST_CollectionExtract(ST_GeomFromText(wkt), 4) FROM input",
    )
    .await
    .expect_err("the second row must reject extract type 4");
    assert!(
        error
            .to_string()
            .contains("only point, linestring and polygon may be extracted"),
        "{error}"
    );
}

#[tokio::test]
async fn force_collection_and_multi_keep_nested_structure() {
    let context = new_context();

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_ForceCollection(ST_GeomFromText('MULTIPOINT((0 0),(1 1))')))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "GEOMETRYCOLLECTION(POINT(0 0),POINT(1 1))"
    );

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_ForceCollection(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0))')))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "GEOMETRYCOLLECTION(POINT(0 0))"
    );

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_Multi(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0))')))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "GEOMETRYCOLLECTION(POINT(0 0))"
    );
}

#[tokio::test]
async fn polygon_orientation_matches_postgis_36_contract() {
    let context = new_context();

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_ForcePolygonCCW(ST_GeomFromText('POLYGON((0 0,0 4,4 4,4 0,0 0),(1 1,3 1,3 3,1 3,1 1))')))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,1 3,3 3,3 1,1 1))"
    );

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_ForcePolygonCW(ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON((0 0,4 0,4 4,0 4,0 0)))')))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "GEOMETRYCOLLECTION(POLYGON((0 0,0 4,4 4,4 0,0 0)))"
    );

    let batch = collect(
        &context,
        "SELECT ST_AsText(ST_ForcePolygonCCW(ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON((0 0,4 0,4 4,0 4,0 0)))')))",
    )
    .await;
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "GEOMETRYCOLLECTION(POLYGON((0 0,4 0,4 4,0 4,0 0)))"
    );
}
