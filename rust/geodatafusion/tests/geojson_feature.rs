//! GeoJSON Feature contracts for the ST_AsGeoJSON record overload.

mod support;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_schema::{CoordType, Crs, Dimension, Metadata, PointType};
use serde_json::json;
use support::context::context;
use support::geoarrow::collect;
use wkt::types::{Coord as WktCoord, Point as WktPoint};

fn point(x: f64, y: f64) -> WktPoint<f64> {
    WktPoint::from_coord(WktCoord {
        x,
        y,
        z: None,
        m: None,
    })
}

#[tokio::test]
async fn record_feature_preserves_slices_nulls_nested_values_and_crs() {
    let point_type = PointType::new(
        Dimension::XY,
        Arc::new(Metadata::new(
            Crs::from_authority_code("EPSG:3857".to_string()),
            None,
        )),
    )
    .with_coord_type(CoordType::Separated);
    let points = PointBuilder::from_nullable_points(
        [
            Some(&point(1.0, 2.0)),
            Some(&point(3.4567, 4.5678)),
            Some(&point(5.0, 6.0)),
        ]
        .into_iter(),
        point_type.clone(),
    )
    .finish();
    let alternate_point_type = PointType::new(
        Dimension::XY,
        Arc::new(Metadata::new(
            Crs::from_authority_code("EPSG:4326".to_string()),
            None,
        )),
    )
    .with_coord_type(CoordType::Separated);
    let alternate_points = PointBuilder::from_nullable_points(
        [
            Some(&point(7.0, 8.0)),
            Some(&point(9.0, 10.0)),
            Some(&point(11.0, 12.0)),
        ]
        .into_iter(),
        alternate_point_type.clone(),
    )
    .finish();

    let tags = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
        Some(vec![Some(1), None, Some(3)]),
        None,
        Some(vec![Some(4)]),
    ]);
    let details = StructArray::new(
        Fields::from(vec![
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Field::new("label", DataType::Utf8, true)),
        ]),
        vec![
            Arc::new(Float64Array::from(vec![0.5, 2.5, 4.5])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
        ],
        None,
    );
    let nested_geometry = StructArray::new(
        Fields::from(vec![Arc::new(
            alternate_point_type.to_field("shape", false),
        )]),
        vec![alternate_points.to_array_ref()],
        None,
    );

    let row_fields = Fields::from(vec![
        Arc::new(point_type.to_field("geom", false)),
        Arc::new(alternate_point_type.to_field("alt_geom", false)),
        Arc::new(Field::new("id", DataType::Int64, false)),
        Arc::new(Field::new("name", DataType::Utf8, true)),
        Arc::new(Field::new("tags", tags.data_type().clone(), true)),
        Arc::new(Field::new(
            "nested_geom",
            nested_geometry.data_type().clone(),
            false,
        )),
        Arc::new(Field::new("details", details.data_type().clone(), false)),
    ]);
    let rows = StructArray::new(
        row_fields.clone(),
        vec![
            points.to_array_ref(),
            alternate_points.to_array_ref(),
            Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")])) as ArrayRef,
            Arc::new(tags) as ArrayRef,
            Arc::new(nested_geometry) as ArrayRef,
            Arc::new(details) as ArrayRef,
        ],
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "record",
        DataType::Struct(row_fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(rows.slice(1, 2)) as ArrayRef]).unwrap();

    let ctx = context();
    ctx.register_batch("records", batch).unwrap();
    let batches = collect(
        &ctx,
        "SELECT ST_AsGeoJSON(record, 'geom', 3, false, 'id') FROM records",
    )
    .await
    .unwrap();
    let batch = &batches[0];
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    assert!(!batch.schema().field(0).is_nullable());
    let values = batch.column(0).as_string::<i32>();

    let first: serde_json::Value = serde_json::from_str(values.value(0)).unwrap();
    assert_eq!(
        first,
        json!({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "crs": {
                    "type": "name",
                    "properties": {"name": "EPSG:3857"}
                },
                "coordinates": [3.457, 4.568]
            },
            "id": 20,
            "properties": {
                "alt_geom": {
                    "type": "Point",
                    "crs": {
                        "type": "name",
                        "properties": {"name": "EPSG:4326"}
                    },
                    "coordinates": [9, 10]
                },
                "name": null,
                "tags": null,
                "nested_geom": {
                    "shape": {
                        "type": "Point",
                        "crs": {
                            "type": "name",
                            "properties": {"name": "EPSG:4326"}
                        },
                        "coordinates": [9, 10]
                    }
                },
                "details": {"x": 2.5, "label": "b"}
            }
        })
    );

    let second: serde_json::Value = serde_json::from_str(values.value(1)).unwrap();
    assert_eq!(second["id"], json!(30));
    assert_eq!(second["properties"]["name"], json!("gamma"));
    assert_eq!(
        second["properties"]["alt_geom"],
        json!({
            "type": "Point",
            "crs": {
                "type": "name",
                "properties": {"name": "EPSG:4326"}
            },
            "coordinates": [11, 12]
        })
    );
    assert_eq!(second["properties"]["tags"], json!([4]));
    assert_eq!(
        second["properties"]["nested_geom"]["shape"],
        json!({
            "type": "Point",
            "crs": {
                "type": "name",
                "properties": {"name": "EPSG:4326"}
            },
            "coordinates": [11, 12]
        })
    );
    assert_eq!(
        second["properties"]["details"],
        json!({"x": 4.5, "label": null})
    );
}

#[tokio::test]
async fn record_feature_selects_geometry_columns_and_serializes_pretty_output() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_AsGeoJSON(named_struct(\
            'id', 7,\
            'g1', ST_GeomFromEWKT('SRID=3857;POINT(1.2345 2.3456)'),\
            'g2', ST_GeomFromEWKT('SRID=4326;POINT(3.4567 4.5678)')\
        ))",
    )
    .await
    .unwrap();
    let value = batches[0].column(0).as_string::<i32>().value(0);
    let parsed: serde_json::Value = serde_json::from_str(value).unwrap();
    assert_eq!(
        parsed["geometry"],
        json!({
            "type": "Point",
            "coordinates": [1.2345, 2.3456]
        })
    );
    assert_eq!(parsed["properties"]["id"], json!(7));
    assert_eq!(
        parsed["properties"]["g2"],
        json!({
            "type": "Point",
            "coordinates": [3.4567, 4.5678]
        })
    );

    let batches = collect(
        &ctx,
        "SELECT ST_AsGeoJSON(named_struct(\
            'id', 7,\
            'name', 'alpha',\
            'g1', ST_GeomFromEWKT('SRID=3857;POINT(1.2345 2.3456)'),\
            'g2', ST_GeomFromEWKT('SRID=4326;POINT(3.4567 4.5678)')\
        ), 'g2', 2, false, 'id')",
    )
    .await
    .unwrap();
    assert_eq!(
        batches[0].column(0).as_string::<i32>().value(0),
        "{\"type\": \"Feature\", \"geometry\": {\"type\":\"Point\",\"coordinates\":[3.46,4.57]}, \"id\": 7, \"properties\": {\"name\": \"alpha\", \"g1\": {\"type\":\"Point\",\"coordinates\":[1.2345,2.3456]}}}"
    );

    let batches = collect(
        &ctx,
        "SELECT ST_AsGeoJSON(named_struct(\
            'id', 1,\
            'name', 'one',\
            'geom', ST_GeomFromText('POINT(1 2)')\
        ), '', 9, true, 'id')",
    )
    .await
    .unwrap();
    let pretty = batches[0].column(0).as_string::<i32>().value(0);
    assert!(pretty.ends_with("\"properties\": {\"name\": \"one\"}}"));
}

#[tokio::test]
async fn record_feature_null_geometry_and_invalid_records_fail_explicitly() {
    let ctx = context();
    let batches = collect(
        &ctx,
        "SELECT ST_AsGeoJSON(named_struct(\
            'geom', ST_GeomFromText(NULL),\
            'name', 'missing'\
        ))",
    )
    .await
    .unwrap();
    assert_eq!(
        batches[0].column(0).as_string::<i32>().value(0),
        "{\"type\": \"Feature\", \"geometry\": null, \"properties\": {\"name\": \"missing\"}}"
    );

    for (sql, expected) in [
        (
            "SELECT ST_AsGeoJSON(named_struct('id', 1, 'name', 'no geometry'))",
            "geometry column is missing",
        ),
        (
            "SELECT ST_AsGeoJSON(named_struct(\
                'id', 1, 'geom', ST_GeomFromText('POINT(1 2)')\
            ), 'id')",
            "is not a GeoArrow geometry",
        ),
        (
            "SELECT ST_AsGeoJSON(named_struct(\
                'id', 1, 'geom', ST_GeomFromText('POINT(1 2)')\
            ), 'missing')",
            "geometry column is missing",
        ),
        (
            "SELECT ST_AsGeoJSON(named_struct(\
                'id', 1, 'geom', ST_GeomFromText('POINT(1 2)')\
            ), 'geom', 9, false, 'missing')",
            "Specified id column \"missing\" is missing",
        ),
        (
            "SELECT ST_AsGeoJSON(named_struct(\
                'geom', ST_GeomFromText('POINT(1 2)')\
            ), 3)",
            "record column names must be text",
        ),
        (
            "SELECT ST_AsGeoJSON(named_struct(\
                'geom', ST_GeomFromText('POINT(1 2)')\
            ), 'geom', 9, 'yes')",
            "pretty_bool must be boolean",
        ),
    ] {
        let error = collect(&ctx, sql).await.unwrap_err().to_string();
        assert!(error.contains(expected), "{sql}: {error}");
    }
}
