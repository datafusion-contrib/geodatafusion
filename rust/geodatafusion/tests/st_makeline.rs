use std::sync::Arc;

use arrow_schema::Field;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::prelude::SessionContext;
use geo_traits::{CoordTrait, LineStringTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray};
use geoarrow_array::builder::{LineStringBuilder, PointBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeometryType, LineStringType, Metadata, PointType,
};
use geodatafusion::udf::native::constructors::MakeLine;
use wkt::types::{
    Coord, Dimension as WktDimension, LineString as WktLineString, Point as WktPoint,
};

#[test]
fn typed_points_preserve_dimensions_crs_layouts_and_union_dimensions() {
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    for (
        left_dim,
        left_wkt_dim,
        right_dim,
        right_wkt_dim,
        expected_dim,
        output_coord_type,
        expected_coords,
    ) in [
        (
            Dimension::XY,
            WktDimension::XY,
            Dimension::XY,
            WktDimension::XY,
            Dimension::XY,
            CoordType::Interleaved,
            vec![vec![1.0, 2.0], vec![3.0, 4.0]],
        ),
        (
            Dimension::XYZ,
            WktDimension::XYZ,
            Dimension::XYZ,
            WktDimension::XYZ,
            Dimension::XYZ,
            CoordType::Separated,
            vec![vec![1.0, 2.0, 5.0], vec![3.0, 4.0, 6.0]],
        ),
        (
            Dimension::XYM,
            WktDimension::XYM,
            Dimension::XYM,
            WktDimension::XYM,
            Dimension::XYM,
            CoordType::Interleaved,
            vec![vec![1.0, 2.0, 7.0], vec![3.0, 4.0, 8.0]],
        ),
        (
            Dimension::XYZ,
            WktDimension::XYZ,
            Dimension::XYM,
            WktDimension::XYM,
            Dimension::XYZM,
            CoordType::Separated,
            vec![vec![1.0, 2.0, 5.0, 0.0], vec![3.0, 4.0, 0.0, 8.0]],
        ),
    ] {
        let metadata = Arc::new(Metadata::new(crs.clone(), None));
        let left_point = sample_point(1.0, 2.0, 5.0, 7.0, left_wkt_dim);
        let right_point = sample_point(3.0, 4.0, 6.0, 8.0, right_wkt_dim);
        let left = PointBuilder::from_points(
            [&left_point].into_iter(),
            PointType::new(left_dim, Arc::clone(&metadata)).with_coord_type(CoordType::Separated),
        )
        .finish();
        let right = PointBuilder::from_points(
            [&right_point].into_iter(),
            PointType::new(right_dim, Arc::clone(&metadata))
                .with_coord_type(CoordType::Interleaved),
        )
        .finish();

        let (return_field, output) = invoke_typed_make_line(&left, &right, output_coord_type);
        let output_type = return_field.extension_type::<LineStringType>();
        assert_eq!(output_type.dimension(), expected_dim);
        assert_eq!(output_type.coord_type(), output_coord_type);
        assert_eq!(output_type.metadata().crs(), &crs);

        let line = output.value(0).unwrap();
        let actual = (0..line.num_coords())
            .map(|index| {
                let coord = line.coord(index).unwrap();
                (0..expected_dim.size())
                    .map(|dim_index| coord.nth_or_panic(dim_index))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected_coords);
    }
}

#[test]
fn typed_point_and_linestring_slices_preserve_nulls_empty_and_dedup() {
    let point_type =
        PointType::new(Dimension::XY, Default::default()).with_coord_type(CoordType::Separated);
    let mut left_builder = PointBuilder::new(point_type);
    left_builder.push_point(Some(&point_xy(9.0, 9.0)));
    left_builder.push_point(None::<&WktPoint>);
    left_builder.push_point(Some(&WktPoint::empty(WktDimension::XY)));
    left_builder.push_point(Some(&point_xy(0.0, 0.0)));
    let left = left_builder.finish().slice(1, 3);

    let line_type = LineStringType::new(Dimension::XY, Default::default())
        .with_coord_type(CoordType::Interleaved);
    let mut right_builder = LineStringBuilder::new(line_type);
    right_builder
        .push_line_string(Some(&line_xy(&[(9.0, 9.0), (10.0, 10.0)])))
        .unwrap();
    right_builder
        .push_line_string(Some(&line_xy(&[(1.0, 1.0), (2.0, 2.0)])))
        .unwrap();
    right_builder
        .push_line_string(Some(&WktLineString::empty(WktDimension::XY)))
        .unwrap();
    right_builder
        .push_line_string(Some(&line_xy(&[(0.0, 0.0), (1.0, 2.0), (3.0, 4.0)])))
        .unwrap();
    let right = right_builder.finish().slice(1, 3);

    let (_, output) = invoke_typed_make_line(&left, &right, CoordType::Separated);
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
        vec![(0.0, 0.0), (1.0, 2.0), (3.0, 4.0)]
    );
}

#[test]
fn missing_extension_metadata_is_not_treated_as_untyped_null() {
    let line_type = LineStringType::new(Dimension::XY, Default::default());
    let mut builder = LineStringBuilder::new(line_type);
    builder.push_line_string(None::<&WktLineString>).unwrap();
    let valid_line = builder.finish();
    let raw_array = valid_line.to_array_ref();
    let valid_field = Arc::new(valid_line.data_type().to_field("geom", true));
    let untyped_field = Arc::new(Field::new("geom", raw_array.data_type().clone(), true));
    let null_field = Arc::new(Field::new("null_geom", arrow_schema::DataType::Null, true));
    let udf = MakeLine::default();

    for fields in [
        [Arc::clone(&untyped_field), Arc::clone(&valid_field)],
        [Arc::clone(&valid_field), Arc::clone(&untyped_field)],
        [Arc::clone(&untyped_field), Arc::clone(&null_field)],
        [Arc::clone(&null_field), Arc::clone(&untyped_field)],
    ] {
        let error = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[],
            })
            .unwrap_err();
        assert!(error.to_string().contains("without extension metadata"));
    }

    let return_field = Arc::new(GeometryType::new(Default::default()).to_field("geom", true));
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::clone(&raw_array)),
            ColumnarValue::Array(raw_array),
        ],
        arg_fields: vec![untyped_field, valid_field],
        number_rows: 1,
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
    };
    let error = udf.invoke_with_args(args).unwrap_err();
    assert!(error.to_string().contains("without extension metadata"));
}

#[tokio::test]
async fn untyped_null_literals_and_typed_crs_preserve_geometry_fields() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let df = ctx
        .sql(
            r#"
            SELECT
                ST_MakeLine(NULL, NULL) AS both_null,
                ST_MakeLine(NULL, ST_Point(1, 2, 4326)) AS left_null,
                ST_MakeLine(ST_Point(1, 2, 4326), NULL) AS right_null
            "#,
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let batch = &batches[0];
    let schema = batch.schema();

    let both_null_field = schema.field_with_name("both_null").unwrap();
    let both_null_type = both_null_field
        .try_extension_type::<GeometryType>()
        .unwrap();
    assert_eq!(both_null_type.metadata().crs(), &Crs::default());

    let expected_crs = Crs::from_authority_code("EPSG:4326".to_string());
    for name in ["left_null", "right_null"] {
        let field = schema.field_with_name(name).unwrap();
        let geo_type = field.try_extension_type::<GeometryType>().unwrap();
        assert_eq!(geo_type.metadata().crs(), &expected_crs);
        let array =
            GeometryArray::try_from((batch.column(schema.index_of(name).unwrap()).as_ref(), field))
                .unwrap();
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
    }
}

fn invoke_typed_make_line(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> (Arc<Field>, LineStringArray) {
    let udf = MakeLine::new(coord_type);
    let left_field = Arc::new(left.data_type().to_field("left", true));
    let right_field = Arc::new(right.data_type().to_field("right", true));
    let arg_fields = vec![left_field, right_field];
    let return_field = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[],
        })
        .unwrap();
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(left.to_array_ref()),
            ColumnarValue::Array(right.to_array_ref()),
        ],
        arg_fields,
        number_rows: left.len(),
        return_field: Arc::clone(&return_field),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let ColumnarValue::Array(array) = udf.invoke_with_args(args).unwrap() else {
        panic!("expected an array result");
    };
    let output = LineStringArray::try_from((array.as_ref(), return_field.as_ref())).unwrap();
    (return_field, output)
}

fn sample_point(x: f64, y: f64, z: f64, m: f64, dimension: WktDimension) -> WktPoint {
    let (z, m) = match dimension {
        WktDimension::XY => (None, None),
        WktDimension::XYZ => (Some(z), None),
        WktDimension::XYM => (None, Some(m)),
        WktDimension::XYZM => (Some(z), Some(m)),
    };
    WktPoint::new(Some(Coord { x, y, z, m }), dimension)
}

fn point_xy(x: f64, y: f64) -> WktPoint {
    WktPoint::from_coord(Coord {
        x,
        y,
        z: None,
        m: None,
    })
}

fn line_xy(coords: &[(f64, f64)]) -> WktLineString {
    WktLineString::new(
        coords
            .iter()
            .map(|&(x, y)| Coord {
                x,
                y,
                z: None,
                m: None,
            })
            .collect(),
        WktDimension::XY,
    )
}
