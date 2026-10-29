use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::WktArray;
use geoarrow_array::builder::{GeometryBuilder, PointBuilder, WkbBuilder};
use geoarrow_schema::{
    CoordType, Dimension, GeometryType as GeoArrowGeometryType, PointType, WkbType,
};
use wkt::Wkt;
use wkt::types::{Coord, Point};

fn wkt_point(dimension: Dimension) -> Point {
    let coord = Coord {
        x: 1.0,
        y: 2.0,
        z: matches!(dimension, Dimension::XYZ | Dimension::XYZM).then_some(3.0),
        m: matches!(dimension, Dimension::XYM | Dimension::XYZM).then_some(4.0),
    };
    let wkt_dimension = match dimension {
        Dimension::XY => wkt::types::Dimension::XY,
        Dimension::XYZ => wkt::types::Dimension::XYZ,
        Dimension::XYM => wkt::types::Dimension::XYM,
        Dimension::XYZM => wkt::types::Dimension::XYZM,
    };
    Point::new(Some(coord), wkt_dimension)
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

async fn has_z_values(ctx: &SessionContext, table: &str) -> Vec<Option<bool>> {
    let batches = ctx
        .sql(&format!(
            "SELECT ST_HasZ(geometry) AS has_z FROM {table} ORDER BY id"
        ))
        .await
        .expect("plan ST_HasZ query")
        .collect()
        .await
        .expect("execute ST_HasZ query");
    let column = batches[0].column(0).as_boolean();
    (0..column.len())
        .map(|index| (!column.is_null(index)).then(|| column.value(index)))
        .collect()
}

#[tokio::test]
async fn typed_point_layouts_preserve_nulls_and_slices() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    for (layout_index, coord_type) in [CoordType::Separated, CoordType::Interleaved]
        .into_iter()
        .enumerate()
    {
        for (dimension_index, dimension) in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ]
        .into_iter()
        .enumerate()
        {
            let point = wkt_point(dimension);
            let mut builder = PointBuilder::with_capacity(
                PointType::new(dimension, Default::default()).with_coord_type(coord_type),
                3,
            );
            builder.push_point(Some(&point));
            builder.push_null();
            builder.push_empty();
            let array = builder.finish();
            let sliced = GeoArrowArray::slice(&array, 1, 2);
            let table = format!("points_{layout_index}_{dimension_index}");
            register_geo_array(&ctx, &table, sliced);

            assert_eq!(
                has_z_values(&ctx, &table).await,
                vec![
                    None,
                    Some(matches!(dimension, Dimension::XYZ | Dimension::XYZM))
                ],
                "{coord_type:?} {dimension:?}"
            );
        }
    }
}

#[tokio::test]
async fn native_generic_wkt_and_wkb_handle_mixed_dimensions_and_nulls() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let values = [
        Some("POINT(1 2)"),
        Some("POINT Z(1 2 3)"),
        None,
        Some("POINT M(1 2 4)"),
        Some("POINT ZM(1 2 3 4)"),
        Some("GEOMETRYCOLLECTION Z(POINT Z(1 2 3), POINT Z(4 5 6))"),
        Some("LINESTRING M(0 0 1, 1 1 2)"),
        Some("POLYGON ZM((0 0 1 2, 1 0 3 4, 1 1 5 6, 0 0 1 2))"),
    ];
    let expected = vec![
        Some(false),
        Some(true),
        None,
        Some(false),
        Some(true),
        Some(true),
        Some(false),
        Some(true),
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
        assert_eq!(has_z_values(&ctx, &table).await, expected, "{coord_type:?}");
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
    assert_eq!(has_z_values(&ctx, "wkt").await, expected);

    let mut wkb_builder = WkbBuilder::<i32>::new(WkbType::default());
    for geometry in &geometries {
        wkb_builder
            .push_geometry(geometry.as_ref())
            .expect("write WKB fixture");
    }
    register_geo_array(&ctx, "wkb", wkb_builder.finish());
    assert_eq!(has_z_values(&ctx, "wkb").await, expected);
}
