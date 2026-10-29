use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, Int32Array, NullArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::WktArray;
use geoarrow_array::builder::{GeometryBuilder, MultiPointBuilder, PointBuilder, WkbBuilder};
use geoarrow_schema::{
    CoordType, Dimension, GeometryType as GeoArrowGeometryType, MultiPointType, PointType, WkbType,
};
use wkt::Wkt;
use wkt::types::{Coord, MultiPoint, Point};

fn wkt_point() -> Point {
    Point::from_coord(Coord {
        x: 1.0,
        y: 2.0,
        z: None,
        m: None,
    })
}

fn register_geo_array(ctx: &SessionContext, name: &str, array: impl GeoArrowArray + 'static) {
    let ids = Arc::new(Int32Array::from_iter_values(0..array.len() as i32));
    let geometry_field = array.data_type().to_field("geometry", true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        geometry_field,
    ]));
    let batch = RecordBatch::try_new(schema, vec![ids, array.to_array_ref()]).expect("valid batch");
    ctx.register_batch(name, batch).expect("register batch");
}

async fn is_collection_values(ctx: &SessionContext, table: &str) -> Vec<Option<bool>> {
    let batches = ctx
        .sql(&format!(
            "SELECT ST_IsCollection(geometry) AS is_collection FROM {table} ORDER BY id"
        ))
        .await
        .expect("plan ST_IsCollection query")
        .collect()
        .await
        .expect("execute ST_IsCollection query");
    let column = batches[0].column(0).as_boolean();
    (0..column.len())
        .map(|index| (!column.is_null(index)).then(|| column.value(index)))
        .collect()
}

#[tokio::test]
async fn typed_atomic_and_collection_layouts_preserve_nulls_and_slices() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    for (layout_index, coord_type) in [CoordType::Separated, CoordType::Interleaved]
        .into_iter()
        .enumerate()
    {
        let point = wkt_point();
        let mut point_builder = PointBuilder::with_capacity(
            PointType::new(Dimension::XY, Default::default()).with_coord_type(coord_type),
            3,
        );
        point_builder.push_point(Some(&point));
        point_builder.push_null();
        point_builder.push_empty();
        let point_array = point_builder.finish();
        let sliced = GeoArrowArray::slice(&point_array, 1, 2);
        let table = format!("points_{layout_index}");
        register_geo_array(&ctx, &table, sliced);
        assert_eq!(
            is_collection_values(&ctx, &table).await,
            vec![None, Some(false)]
        );

        let multi_point = MultiPoint::new(vec![point], wkt::types::Dimension::XY);
        let empty_multi_point = MultiPoint::new(vec![], wkt::types::Dimension::XY);
        let multi_points = vec![Some(multi_point), None, Some(empty_multi_point)];
        let multi_point_array = MultiPointBuilder::from_nullable_multi_points(
            &multi_points,
            MultiPointType::new(Dimension::XY, Default::default()).with_coord_type(coord_type),
        )
        .finish();
        let sliced = GeoArrowArray::slice(&multi_point_array, 1, 2);
        let table = format!("multi_points_{layout_index}");
        register_geo_array(&ctx, &table, sliced);
        assert_eq!(
            is_collection_values(&ctx, &table).await,
            vec![None, Some(true)]
        );
    }
}

#[tokio::test]
async fn native_generic_wkt_and_wkb_handle_collection_types_and_nulls() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let values = [
        Some("POINT(1 2)"),
        Some("MULTIPOINT(1 2)"),
        Some("LINESTRING M(0 0 1, 1 1 2)"),
        Some("MULTILINESTRING((0 0,1 1))"),
        Some("POLYGON((0 0,1 0,1 1,0 0))"),
        Some("MULTIPOLYGON(((0 0,1 0,1 1,0 0)))"),
        Some("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))"),
        None,
    ];
    let expected = vec![
        Some(false),
        Some(true),
        Some(false),
        Some(true),
        Some(false),
        Some(true),
        Some(true),
        None,
    ];

    let geometries: Vec<Option<Wkt>> = values
        .iter()
        .map(|value| value.map(str::parse).transpose())
        .collect::<Result<_, _>>()
        .expect("valid WKT fixtures");

    for (index, coord_type) in [CoordType::Separated, CoordType::Interleaved]
        .into_iter()
        .enumerate()
    {
        let mut builder = GeometryBuilder::new(
            GeoArrowGeometryType::new(Default::default()).with_coord_type(coord_type),
        );
        for geometry in &geometries {
            builder
                .push_geometry(geometry.as_ref())
                .expect("push generic geometry");
        }
        let table = format!("generic_{index}");
        register_geo_array(&ctx, &table, builder.finish());
        assert_eq!(
            is_collection_values(&ctx, &table).await,
            expected,
            "{coord_type:?}"
        );
    }

    let wkt_array = WktArray::new(
        StringArray::from(
            values
                .iter()
                .map(|value| value.map(str::to_owned))
                .collect::<Vec<_>>(),
        ),
        Default::default(),
    );
    register_geo_array(&ctx, "wkt", wkt_array);
    assert_eq!(is_collection_values(&ctx, "wkt").await, expected);

    let mut wkb_builder = WkbBuilder::<i32>::new(WkbType::default());
    for geometry in &geometries {
        wkb_builder
            .push_geometry(geometry.as_ref())
            .expect("write WKB fixture");
    }
    register_geo_array(&ctx, "wkb", wkb_builder.finish());
    assert_eq!(is_collection_values(&ctx, "wkb").await, expected);
}

#[tokio::test]
async fn raw_null_array_propagates() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let ids = Arc::new(Int32Array::from_iter_values(0..3));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("geometry", DataType::Null, true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![ids, Arc::new(NullArray::new(3))])
        .expect("valid null batch");
    ctx.register_batch("raw_nulls", batch)
        .expect("register null batch");

    assert_eq!(
        is_collection_values(&ctx, "raw_nulls").await,
        vec![None, None, None]
    );
}

#[tokio::test]
async fn non_geometry_type_is_rejected() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let error = match ctx.sql("SELECT ST_IsCollection(42)").await {
        Ok(dataframe) => dataframe.collect().await.expect_err("query should fail"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("Invalid WKT format"),
        "unexpected error: {error}"
    );
}
