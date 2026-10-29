use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{Array, UInt32Array};
use arrow_schema::{DataType, Field};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::{MultiPolygonBuilder, PolygonBuilder};
use geoarrow_schema::{CoordType, Dimension, MultiPolygonType, PolygonType};
use geodatafusion::udf::native::accessors::NRings;
use wkt::types::{
    Coord, Dimension as WktDimension, LineString, MultiPolygon as WktMultiPolygon,
    Polygon as WktPolygon,
};

#[test]
fn typed_polygon_slices_preserve_nulls_empty_and_both_layouts() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let typ = PolygonType::new(Dimension::XY, Default::default()).with_coord_type(coord_type);
        let mut builder = PolygonBuilder::new(typ);
        builder
            .push_polygon(Some(&polygon_with_interiors(2)))
            .unwrap();
        builder.push_polygon(None::<&WktPolygon>).unwrap();
        builder
            .push_polygon(Some(&WktPolygon::empty(WktDimension::XY)))
            .unwrap();
        builder
            .push_polygon(Some(&polygon_with_interiors(0)))
            .unwrap();
        builder
            .push_polygon(Some(&polygon_with_interiors(1)))
            .unwrap();
        builder
            .push_polygon(Some(&polygon_with_interiors(2)))
            .unwrap();
        let input = builder.finish().slice(1, 4);
        let output = invoke_nrings(&input);

        assert_eq!(output.len(), 4);
        assert!(output.is_null(0));
        assert_eq!(output.value(1), 0);
        assert_eq!(output.value(2), 1);
        assert_eq!(output.value(3), 2);
    }
}

#[test]
fn typed_multi_polygon_layout_and_null_preserve_owned_geometry_path() {
    let multi_polygon = WktMultiPolygon::new(
        vec![polygon_with_interiors(0), polygon_with_interiors(1)],
        WktDimension::XY,
    );
    let typ = MultiPolygonType::new(Dimension::XY, Default::default())
        .with_coord_type(CoordType::Interleaved);
    let mut builder = MultiPolygonBuilder::new(typ);
    builder.push_multi_polygon(Some(&multi_polygon)).unwrap();
    builder
        .push_multi_polygon(None::<&WktMultiPolygon>)
        .unwrap();
    let input = builder.finish();
    let output = invoke_nrings(&input);

    assert_eq!(output.len(), 2);
    assert_eq!(output.value(0), 3);
    assert!(output.is_null(1));
}

#[tokio::test]
async fn untyped_null_literal_and_all_null_column_return_nulls() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let df = ctx
        .sql(
            r#"
            SELECT
                ST_NRings(NULL) AS null_literal,
                ST_NRings(geometry) AS null_column
            FROM (VALUES (NULL), (NULL)) AS inputs(geometry)
            "#,
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = &batches[0];

    for name in ["null_literal", "null_column"] {
        let array = batch
            .column(batch.schema().index_of(name).unwrap())
            .as_primitive::<UInt32Type>();
        assert_eq!(array.len(), 2);
        assert!(array.is_null(0));
        assert!(array.is_null(1));
    }
}

fn invoke_nrings(input: &impl GeoArrowArray) -> UInt32Array {
    let udf = NRings::new();
    let input_field = Arc::new(input.data_type().to_field("geometry", true));
    let return_field = Arc::new(Field::new("rings", DataType::UInt32, true));
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(input.to_array_ref())],
        arg_fields: vec![input_field],
        number_rows: input.len(),
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
    };
    let ColumnarValue::Array(array) = udf.invoke_with_args(args).unwrap() else {
        panic!("expected an array result");
    };
    array
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap()
        .clone()
}

fn polygon_with_interiors(count: usize) -> WktPolygon {
    let exterior = ring(&[
        (0.0, 0.0),
        (0.0, 100.0),
        (100.0, 100.0),
        (100.0, 0.0),
        (0.0, 0.0),
    ]);
    let interiors = (0..count)
        .map(|index| {
            let offset = index as f64 * 2.0 + 1.0;
            ring(&[
                (offset, offset),
                (offset, offset + 0.5),
                (offset + 0.5, offset + 0.5),
                (offset, offset),
            ])
        })
        .collect::<Vec<_>>();
    WktPolygon::from_rings(std::iter::once(exterior).chain(interiors))
        .expect("polygon has an exterior ring")
}

fn ring(coords: &[(f64, f64)]) -> LineString {
    LineString::from_coords(
        coords
            .iter()
            .map(|&(x, y)| Coord {
                x,
                y,
                z: None,
                m: None,
            })
            .collect::<Vec<_>>(),
    )
    .expect("ring is non-empty")
}
