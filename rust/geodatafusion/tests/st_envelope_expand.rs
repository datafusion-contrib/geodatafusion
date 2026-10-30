use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, RectTrait};
use geoarrow_array::array::RectArray;
use geoarrow_array::builder::RectBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::crs::Crs;
use geoarrow_schema::{BoxType, CoordType, Dimension, GeometryType, Metadata, WktType};
use geodatafusion::udf::native::accessors::Envelope;
use geodatafusion::udf::native::bounding_box::Expand;
use geodatafusion::udf::native::io::{AsText, GeomFromText};
use wkt::types::Coord as WktCoord;

const CRS_AUTHORITY: &str = "EPSG:3857";

fn crs_metadata() -> Arc<Metadata> {
    Arc::new(Metadata::new(
        Crs::from_authority_code(CRS_AUTHORITY.to_string()),
        Default::default(),
    ))
}

fn context_with_wkt_table(name: &str) -> SessionContext {
    let metadata = crs_metadata();
    let wkt_field =
        Field::new("wkt", DataType::Utf8, true).with_extension_type(WktType::new(metadata));
    let dx_field = Field::new("dx", DataType::Float64, true);
    let dy_field = Field::new("dy", DataType::Float64, true);
    let schema = Arc::new(Schema::new(vec![wkt_field, dx_field, dy_field]));

    // Prefix and trim the vectors so every function input is a nonzero-offset slice.
    let wkt = StringArray::from(vec![
        Some("POINT(99 99)"),
        Some("POINT(1 2)"),
        Some("LINESTRING Z (1 2 3,4 5 6)"),
        Some("POLYGON EMPTY"),
        Some("POINT(9 9)"),
        Some("POINT(100 100)"),
    ])
    .slice(1, 4);
    let dx = Float64Array::from(vec![
        Some(99.0),
        Some(1.0),
        Some(-1.0),
        Some(1.0),
        None,
        Some(99.0),
    ])
    .slice(1, 4);
    let dy = Float64Array::from(vec![
        Some(99.0),
        Some(2.0),
        Some(1.0),
        Some(2.0),
        Some(2.0),
        Some(99.0),
    ])
    .slice(1, 4);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(wkt), Arc::new(dx), Arc::new(dy)],
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

#[tokio::test]
async fn public_registration_returns_general_geometry_arrays() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT \
                ST_Envelope(ST_GeomFromText('LINESTRING(0 0,1 3)')) AS envelope, \
                ST_Expand(ST_GeomFromText('POINT(1 2)'), 1) AS expand, \
                ST_Envelope(NULL) AS null_envelope, \
                ST_Expand(NULL, 1) AS null_expand",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    for column in 0..4 {
        let geometry_type = batch
            .schema()
            .field(column)
            .try_extension_type::<GeometryType>()
            .unwrap();
        assert_eq!(geometry_type.coord_type(), CoordType::Separated);
    }

    let text = ctx
        .sql(
            "SELECT \
                ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING(0 0,1 3)'))), \
                ST_AsText(ST_Expand(ST_GeomFromText('POINT(1 2)'), 1))",
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
        text.column(0).as_string::<i32>().value(0),
        "POLYGON((0 0,0 3,1 3,1 0,0 0))"
    );
    assert_eq!(
        text.column(1).as_string::<i32>().value(0),
        "POLYGON((0 1,0 3,2 3,2 1,0 1))"
    );

    let nulls = ctx
        .sql(
            "SELECT \
                ST_Envelope(NULL) IS NULL AS envelope, \
                ST_Expand(NULL, 1) IS NULL AS expanded \
             FROM (VALUES (1), (2)) AS rows(n)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(nulls.num_rows(), 2);
    for row in 0..2 {
        assert!(nulls.column(0).as_boolean().value(row));
        assert!(nulls.column(1).as_boolean().value(row));
    }
}

#[tokio::test]
async fn sliced_inputs_and_vectorized_factors_are_handled_per_row() {
    let ctx = context_with_wkt_table("geometries");
    let batch = ctx
        .sql(
            "SELECT \
                ST_AsText(ST_Envelope(ST_GeomFromText(wkt))) AS envelope, \
                ST_AsText(ST_Expand(ST_GeomFromText(wkt), dx, dy)) AS expanded, \
                ST_GeometryType(ST_Envelope(ST_GeomFromText(wkt))) AS envelope_type, \
                ST_GeometryType(ST_Expand(ST_GeomFromText(wkt), dx, dy)) AS expanded_type \
             FROM geometries",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let envelopes = batch.column(0).as_string::<i32>();
    assert_eq!(envelopes.value(0), "POINT(1 2)");
    assert_eq!(envelopes.value(1), "POLYGON((1 2,1 5,4 5,4 2,1 2))");
    assert_eq!(envelopes.value(2), "POLYGON EMPTY");
    assert_eq!(envelopes.value(3), "POINT(9 9)");
    let envelope_types = batch.column(2).as_string_view();
    assert_eq!(envelope_types.value(2), "ST_Polygon");

    let expanded_types = batch.column(3).as_string_view();
    assert_eq!(expanded_types.value(2), "ST_Polygon");
    assert!(expanded_types.is_null(3));

    let expanded = batch.column(1).as_string::<i32>();
    assert_eq!(expanded.value(0), "POLYGON((0 0,0 4,2 4,2 0,0 0))");
    assert_eq!(
        expanded.value(1),
        "POLYGON Z((2 1 3,2 6 3,3 6 6,3 1 6,2 1 3))"
    );
    assert_eq!(expanded.value(2), "POLYGON EMPTY");
    assert!(expanded.is_null(3));

    let scalar = ctx
        .sql(
            "SELECT ST_AsText(ST_Expand(ST_GeomFromText(wkt), 2, 3)) AS expanded \
             FROM geometries LIMIT 2",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let scalar = scalar.column(0).as_string::<i32>();
    assert_eq!(scalar.value(0), "POLYGON((-1 -1,-1 5,3 5,3 -1,-1 -1))");
    assert_eq!(
        scalar.value(1),
        "POLYGON Z((-1 -1 3,-1 8 3,6 8 6,6 -1 6,-1 -1 3))"
    );
}

#[tokio::test]
async fn envelope_and_geometry_expand_preserve_crs_metadata() {
    let ctx = context_with_wkt_table("geometries");
    let batch = ctx
        .sql(
            "SELECT \
                ST_Envelope(ST_GeomFromText(wkt)) AS envelope, \
                ST_Expand(ST_GeomFromText(wkt), 1) AS expanded \
             FROM geometries",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    for column in 0..2 {
        let geometry_type = batch
            .schema()
            .field(column)
            .try_extension_type::<GeometryType>()
            .unwrap();
        assert_eq!(
            geometry_type.metadata().crs(),
            &Crs::from_authority_code(CRS_AUTHORITY.to_string())
        );
    }
}

#[tokio::test]
async fn custom_coord_type_is_honored_for_both_functions() {
    let ctx = SessionContext::new();
    ctx.register_udf(Envelope::new(CoordType::Interleaved).into());
    ctx.register_udf(Expand::new(CoordType::Interleaved).into());
    ctx.register_udf(GeomFromText::default().into());
    ctx.register_udf(AsText.into());

    let batch = ctx
        .sql(
            "SELECT \
                ST_Envelope(ST_GeomFromText('LINESTRING(0 0,1 1)')) AS envelope, \
                ST_Expand(ST_GeomFromText('POINT(1 2)'), 1) AS expanded",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    for column in 0..2 {
        let geometry_type = batch
            .schema()
            .field(column)
            .try_extension_type::<GeometryType>()
            .unwrap();
        assert_eq!(geometry_type.coord_type(), CoordType::Interleaved);
    }
}

#[tokio::test]
async fn boxes_keep_their_type_normalize_negative_expansion_and_preserve_crs() {
    let metadata = crs_metadata();
    let box_type = BoxType::new(Dimension::XY, Arc::clone(&metadata));
    let mut builder = RectBuilder::with_capacity(box_type.clone(), 1);
    builder.push_min_max(
        &WktCoord {
            x: 0.0,
            y: 5.0,
            z: None,
            m: None,
        },
        &WktCoord {
            x: 10.0,
            y: 20.0,
            z: None,
            m: None,
        },
    );
    let schema = Arc::new(Schema::new(vec![Arc::new(box_type.to_field("geom", true))]));
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![builder.finish().into_array_ref()]).unwrap();

    let ctx = SessionContext::new();
    ctx.register_table(
        "boxes",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    geodatafusion::register(&ctx);
    let batch = ctx
        .sql(
            "SELECT \
                ST_Expand(geom, -20, 30) AS box2d, \
                ST_Expand(ST_3DMakeBox(ST_PointZ(-1, 3, 5), ST_PointZ(-1, 6, 8)), -4, 2) AS box3d, \
                ST_Expand(ST_3DMakeBox(ST_PointZ(-1, 3, 5), ST_PointZ(-1, 6, 8)), 2) AS uniform_box3d, \
                ST_Expand(geom, CAST(NULL AS DOUBLE), 2) AS null_box \
             FROM boxes",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let box2d_type = batch
        .schema()
        .field(0)
        .try_extension_type::<BoxType>()
        .unwrap();
    assert_eq!(box2d_type.dimension(), Dimension::XY);
    assert_eq!(
        box2d_type.metadata().crs(),
        &Crs::from_authority_code(CRS_AUTHORITY.to_string())
    );
    let box2d = RectArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
    let rect = box2d.value(0).unwrap();
    assert_eq!(
        (
            rect.min().x(),
            rect.min().y(),
            rect.max().x(),
            rect.max().y()
        ),
        (-10.0, -25.0, 20.0, 50.0)
    );

    let box3d = RectArray::try_from((batch.column(1).as_ref(), batch.schema().field(1))).unwrap();
    let rect = box3d.value(0).unwrap();
    assert_eq!(
        (
            rect.min().x(),
            rect.min().y(),
            rect.min().nth_or_panic(2),
            rect.max().x(),
            rect.max().y(),
            rect.max().nth_or_panic(2)
        ),
        (-5.0, 1.0, 5.0, 3.0, 8.0, 8.0)
    );

    let uniform = RectArray::try_from((batch.column(2).as_ref(), batch.schema().field(2))).unwrap();
    let rect = uniform.value(0).unwrap();
    assert_eq!(
        (
            rect.min().x(),
            rect.min().y(),
            rect.min().nth_or_panic(2),
            rect.max().x(),
            rect.max().y(),
            rect.max().nth_or_panic(2)
        ),
        (-3.0, 1.0, 3.0, 1.0, 8.0, 10.0)
    );

    assert!(batch.column(3).is_null(0));
}
