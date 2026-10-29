use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{Array, UInt32Array};
use arrow_schema::{DataType, Field};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::prelude::SessionContext;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::MultiPointBuilder;
use geoarrow_schema::{CoordType, Dimension, MultiPointType};
use geodatafusion::udf::native::accessors::NumGeometries;
use wkt::types::{Coord, Dimension as WktDimension, MultiPoint, Point};

#[test]
fn typed_multi_point_slices_preserve_empty_children_nulls_and_both_layouts() {
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        let typ =
            MultiPointType::new(Dimension::XY, Default::default()).with_coord_type(coord_type);
        let mut builder = MultiPointBuilder::new(typ);
        builder
            .push_multi_point(Some(&multi_point(&[None, Some((0.0, 1.0))])))
            .unwrap();
        builder.push_multi_point(None::<&MultiPoint>).unwrap();
        builder
            .push_multi_point(Some(&MultiPoint::empty(WktDimension::XY)))
            .unwrap();
        builder
            .push_multi_point(Some(&multi_point(&[None, Some((1.0, 2.0))])))
            .unwrap();
        builder
            .push_multi_point(Some(&multi_point(&[Some((3.0, 4.0)), Some((5.0, 6.0))])))
            .unwrap();
        let input = builder.finish().slice(1, 3);
        let output = invoke_num_geometries(&input);

        assert_eq!(output.len(), 3);
        assert!(output.is_null(0));
        assert_eq!(output.value(1), 0);
        assert_eq!(output.value(2), 2);
    }
}

#[tokio::test]
async fn untyped_null_literal_and_all_null_column_return_nulls() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let df = ctx
        .sql(
            r#"
            SELECT
                ST_NumGeometries(NULL) AS null_literal,
                ST_NumGeometries(geometry) AS null_column
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

fn invoke_num_geometries(input: &impl GeoArrowArray) -> UInt32Array {
    let udf = NumGeometries::new();
    let input_field = Arc::new(input.data_type().to_field("geometry", true));
    let return_field = Arc::new(Field::new("members", DataType::UInt32, true));
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

fn multi_point(points: &[Option<(f64, f64)>]) -> MultiPoint {
    MultiPoint::new(
        points
            .iter()
            .map(|point| match point {
                Some((x, y)) => Point::from_coord(Coord {
                    x: *x,
                    y: *y,
                    z: None,
                    m: None,
                }),
                None => Point::empty(WktDimension::XY),
            })
            .collect(),
        WktDimension::XY,
    )
}
