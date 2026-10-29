use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray, MultiPointArray};
use geoarrow_array::builder::MultiPointBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Crs, Dimension, GeometryType, Metadata, MultiPointType};
use geodatafusion::udf::native::constructors::LineFromMultiPoint;
use wkt::types::{Coord, Dimension as WktDimension, MultiPoint as WktMultiPoint, Point};

#[test]
fn typed_multi_points_preserve_dimensions_crs_layouts_and_order() {
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    for (dimension, wkt_dimension, input_coord_type, expected) in [
        (
            Dimension::XY,
            WktDimension::XY,
            CoordType::Separated,
            vec![vec![1.0, 2.0], vec![3.0, 4.0]],
        ),
        (
            Dimension::XYZ,
            WktDimension::XYZ,
            CoordType::Interleaved,
            vec![vec![1.0, 2.0, 5.0], vec![3.0, 4.0, 6.0]],
        ),
        (
            Dimension::XYM,
            WktDimension::XYM,
            CoordType::Separated,
            vec![vec![1.0, 2.0, 7.0], vec![3.0, 4.0, 8.0]],
        ),
        (
            Dimension::XYZM,
            WktDimension::XYZM,
            CoordType::Interleaved,
            vec![vec![1.0, 2.0, 5.0, 7.0], vec![3.0, 4.0, 6.0, 8.0]],
        ),
    ] {
        let metadata = Metadata::new(crs.clone(), None);
        let multi_point = sample_multi_point(wkt_dimension);
        let input = MultiPointBuilder::from_multi_points(
            &[multi_point],
            MultiPointType::new(dimension, Arc::new(metadata)).with_coord_type(input_coord_type),
        )
        .finish();
        let output_coord_type = match input_coord_type {
            CoordType::Separated => CoordType::Interleaved,
            CoordType::Interleaved => CoordType::Separated,
        };
        let (return_field, output) = invoke_line_from_multi_point(&input, output_coord_type);

        let output_type = return_field.extension_type::<geoarrow_schema::LineStringType>();
        assert_eq!(output_type.dimension(), dimension);
        assert_eq!(output_type.coord_type(), output_coord_type);
        assert_eq!(output_type.metadata().crs(), &crs);

        let line = output.value(0).unwrap();
        let actual = (0..line.num_coords())
            .map(|index| {
                let coord = line.coord(index).unwrap();
                (0..dimension.size())
                    .map(|dimension_index| coord.nth_or_panic(dimension_index))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}

#[test]
fn typed_multi_point_slices_preserve_nulls_empty_lines_and_order() {
    let typ = MultiPointType::new(Dimension::XY, Default::default())
        .with_coord_type(CoordType::Separated);
    let mut builder = MultiPointBuilder::new(typ);
    builder
        .push_multi_point(Some(&multi_point(&[(0.0, 1.0)])))
        .unwrap();
    builder.push_multi_point(None::<&WktMultiPoint>).unwrap();
    builder
        .push_multi_point(Some(&WktMultiPoint::empty(WktDimension::XY)))
        .unwrap();
    builder
        .push_multi_point(Some(&multi_point(&[(3.0, 4.0), (5.0, 6.0)])))
        .unwrap();
    let input = builder.finish().slice(1, 3);
    let (_, output) = invoke_line_from_multi_point(&input, CoordType::Interleaved);

    assert_eq!(output.len(), 3);
    assert!(output.is_null(0));
    assert_eq!(output.value(1).unwrap().num_coords(), 0);
    let line = output.value(2).unwrap();
    assert_eq!(
        (0..line.num_coords())
            .map(|index| {
                let coord = line.coord(index).unwrap();
                (coord.x(), coord.y())
            })
            .collect::<Vec<_>>(),
        vec![(3.0, 4.0), (5.0, 6.0)]
    );
}

#[test]
fn typed_multi_point_with_empty_member_returns_execution_error() {
    let multi_point = WktMultiPoint::new(
        vec![
            Point::empty(WktDimension::XY),
            Point::from_coord(coord_xy(1.0, 2.0)),
        ],
        WktDimension::XY,
    );
    let typ = MultiPointType::new(Dimension::XY, Default::default());
    let input = MultiPointBuilder::from_multi_points(&[multi_point], typ).finish();
    let udf = LineFromMultiPoint::default();
    let input_field = Arc::new(input.data_type().to_field("geometry", true));
    let return_field = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&input_field),
            scalar_arguments: &[],
        })
        .unwrap();
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(input.to_array_ref())],
        arg_fields: vec![input_field],
        number_rows: input.len(),
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
    };

    let error = udf.invoke_with_args(args).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not support MULTIPOINT values containing EMPTY points")
    );
}

#[tokio::test]
async fn untyped_null_literal_and_all_null_column_have_matching_geometry_fields() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let df = ctx
        .sql(
            r#"
            SELECT
                ST_LineFromMultiPoint(NULL) AS null_literal,
                ST_LineFromMultiPoint(geometry) AS null_column
            FROM (VALUES (NULL), (NULL)) AS inputs(geometry)
            "#,
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = &batches[0];
    let schema = batch.schema();

    for name in ["null_literal", "null_column"] {
        let field = schema.field_with_name(name).unwrap();
        field.try_extension_type::<GeometryType>().unwrap();
        let array =
            GeometryArray::try_from((batch.column(schema.index_of(name).unwrap()).as_ref(), field))
                .unwrap();
        assert_eq!(array.len(), 2);
        assert!(array.is_null(0));
        assert!(array.is_null(1));
    }
}

#[tokio::test]
async fn geometry_collection_wrapping_multi_point_returns_execution_error() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let error = ctx
        .sql(
            "SELECT ST_LineFromMultiPoint(\
             ST_GeomFromText('GEOMETRYCOLLECTION(MULTIPOINT((1 2),(3 4)))'))",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("GEOMETRYCOLLECTION"));
}

fn invoke_line_from_multi_point(
    input: &MultiPointArray,
    coord_type: CoordType,
) -> (Arc<arrow_schema::Field>, LineStringArray) {
    let udf = LineFromMultiPoint::new(coord_type);
    let input_field = Arc::new(input.data_type().to_field("geometry", true));
    let return_field = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&input_field),
            scalar_arguments: &[],
        })
        .unwrap();
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(input.to_array_ref())],
        arg_fields: vec![input_field],
        number_rows: input.len(),
        return_field: Arc::clone(&return_field),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let ColumnarValue::Array(array) = udf.invoke_with_args(args).unwrap() else {
        panic!("expected an array result");
    };
    let output = LineStringArray::try_from((array.as_ref(), return_field.as_ref())).unwrap();
    (return_field, output)
}

fn sample_multi_point(dimension: WktDimension) -> WktMultiPoint {
    let (first_z, first_m, second_z, second_m) = match dimension {
        WktDimension::XY => (None, None, None, None),
        WktDimension::XYZ => (Some(5.0), None, Some(6.0), None),
        WktDimension::XYM => (None, Some(7.0), None, Some(8.0)),
        WktDimension::XYZM => (Some(5.0), Some(7.0), Some(6.0), Some(8.0)),
    };
    WktMultiPoint::new(
        vec![
            Point::new(
                Some(Coord {
                    x: 1.0,
                    y: 2.0,
                    z: first_z,
                    m: first_m,
                }),
                dimension,
            ),
            Point::new(
                Some(Coord {
                    x: 3.0,
                    y: 4.0,
                    z: second_z,
                    m: second_m,
                }),
                dimension,
            ),
        ],
        dimension,
    )
}

fn multi_point(coords: &[(f64, f64)]) -> WktMultiPoint {
    WktMultiPoint::new(
        coords
            .iter()
            .map(|&(x, y)| Point::from_coord(coord_xy(x, y)))
            .collect(),
        WktDimension::XY,
    )
}

fn coord_xy(x: f64, y: f64) -> Coord {
    Coord {
        x,
        y,
        z: None,
        m: None,
    }
}
