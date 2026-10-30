use std::sync::{Arc, LazyLock, OnceLock};

use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    CoordTrait, GeometryTrait, GeometryType, LineStringTrait, LineTrait, MultiLineStringTrait,
};
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, array::from_arrow_array, downcast_geoarrow_array,
};
use geoarrow_schema::error::GeoArrowResult;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Length;

impl Length {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Length {
    fn default() -> Self {
        Self::new()
    }
}

static ALIASES: LazyLock<Vec<String>> = LazyLock::new(|| vec!["st_length2d".to_string()]);
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Length {
    fn name(&self) -> &str {
        "st_length"
    }

    fn aliases(&self) -> &[String] {
        ALIASES.as_slice()
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(length_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the 2D Cartesian length of the geometry if it is a LineString or MultiLineString. For areal geometries 0 is returned; use ST_Perimeter instead.",
                "ST_Length(geom)",
            )
            .with_argument("geom", "geometry")
            .with_sql_example(
                "SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)'));",
            )
            .build()
        }))
    }
}

fn length_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let field = &args.arg_fields[0];
    let geo_array = from_arrow_array(&array, field)?;
    let result = euclidean_length(&geo_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn euclidean_length(array: &dyn GeoArrowArray) -> GeoArrowResult<arrow_array::Float64Array> {
    downcast_geoarrow_array!(array, _euclidean_length_impl)
}

fn _euclidean_length_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<arrow_array::Float64Array> {
    let mut builder = arrow_array::Float64Array::builder(array.len());
    for item in array.iter() {
        if let Some(geometry) = item {
            builder.append_value(geometry_length(&geometry?));
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn geometry_length(geometry: &impl GeometryTrait<T = f64>) -> f64 {
    use GeometryType::*;

    match geometry.as_type() {
        Line(line) => coord_distance(&line.start(), &line.end()),
        LineString(line_string) => line_string_length(line_string),
        MultiLineString(multi_line_string) => multi_line_string_length(multi_line_string),
        _ => 0.0,
    }
}

fn line_string_length(line_string: &impl LineStringTrait<T = f64>) -> f64 {
    let mut coords = line_string.coords();
    let Some(mut previous) = coords.next() else {
        return 0.0;
    };

    let mut length = 0.0;
    for coordinate in coords {
        length += coord_distance(&previous, &coordinate);
        previous = coordinate;
    }
    length
}

fn multi_line_string_length(multi_line_string: &impl MultiLineStringTrait<T = f64>) -> f64 {
    let mut length = 0.0;
    for line_string in multi_line_string.line_strings() {
        length += line_string_length(&line_string);
    }
    length
}

fn coord_distance(first: &impl CoordTrait<T = f64>, second: &impl CoordTrait<T = f64>) -> f64 {
    (first.x() - second.x()).hypot(first.y() - second.y())
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use arrow_array::{Array, Float64Array, StringArray};
    use datafusion::prelude::SessionContext;
    use geoarrow_array::array::from_arrow_array;
    use geoarrow_array::builder::RectBuilder;
    use geoarrow_array::cast::to_wkb;
    use geoarrow_array::test;
    use geoarrow_schema::{BoxType, CoordType, Dimension, GeoArrowType, WktType};

    use super::*;
    use crate::udf::native::io::GeomFromText;

    fn assert_float_arrays_eq(actual: Float64Array, expected: Float64Array) {
        assert_eq!(actual.len(), expected.len());
        for i in 0..actual.len() {
            assert_eq!(actual.is_null(i), expected.is_null(i), "row {i}");
            if actual.is_valid(i) {
                assert_eq!(
                    actual.value(i).to_bits(),
                    expected.value(i).to_bits(),
                    "row {i}"
                );
            }
        }
    }

    #[test]
    fn matches_geoarrow_expr_for_all_array_types() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            for dim in [
                Dimension::XY,
                Dimension::XYZ,
                Dimension::XYM,
                Dimension::XYZM,
            ] {
                assert_float_arrays_eq(
                    euclidean_length(&test::point::array(coord_type, dim)).unwrap(),
                    geoarrow_expr_geo::euclidean_length(&test::point::array(coord_type, dim))
                        .unwrap(),
                );
                assert_float_arrays_eq(
                    euclidean_length(&test::linestring::array(coord_type, dim)).unwrap(),
                    geoarrow_expr_geo::euclidean_length(&test::linestring::array(coord_type, dim))
                        .unwrap(),
                );
                assert_float_arrays_eq(
                    euclidean_length(&test::multilinestring::array(coord_type, dim)).unwrap(),
                    geoarrow_expr_geo::euclidean_length(&test::multilinestring::array(
                        coord_type, dim,
                    ))
                    .unwrap(),
                );
                assert_float_arrays_eq(
                    euclidean_length(&test::polygon::array(coord_type, dim)).unwrap(),
                    geoarrow_expr_geo::euclidean_length(&test::polygon::array(coord_type, dim))
                        .unwrap(),
                );
                assert_float_arrays_eq(
                    euclidean_length(&test::geometrycollection::array(coord_type, dim, false))
                        .unwrap(),
                    geoarrow_expr_geo::euclidean_length(&test::geometrycollection::array(
                        coord_type, dim, false,
                    ))
                    .unwrap(),
                );
            }

            let geometry = test::geometry::array(coord_type, false);
            assert_float_arrays_eq(
                euclidean_length(&geometry).unwrap(),
                geoarrow_expr_geo::euclidean_length(&geometry).unwrap(),
            );
            assert_float_arrays_eq(
                euclidean_length(&to_wkb::<i32>(&geometry).unwrap()).unwrap(),
                geoarrow_expr_geo::euclidean_length(&to_wkb::<i32>(&geometry).unwrap()).unwrap(),
            );

            let rects = [
                Some(geo::Rect::new(
                    geo::coord! { x: 10.0, y: 10.0 },
                    geo::coord! { x: 30.0, y: 20.0 },
                )),
                None,
                Some(geo::Rect::new(
                    geo::coord! { x: 300.0, y: 100.0 },
                    geo::coord! { x: 100.0, y: 200.0 },
                )),
            ];
            let rect = RectBuilder::from_nullable_rects(
                rects.iter().map(|rect| rect.as_ref()),
                BoxType::new(Dimension::XY, Default::default()),
            )
            .finish();
            assert_float_arrays_eq(
                euclidean_length(&rect).unwrap(),
                geoarrow_expr_geo::euclidean_length(&rect).unwrap(),
            );
        }

        let wkt_values = StringArray::from(vec![
            Some("LINESTRING (0 0, 3 4)"),
            Some("LINESTRING Z (0 0 7, 3 4 100)"),
            Some("LINESTRING EMPTY"),
            None,
        ]);
        let field = GeoArrowType::Wkt(WktType::new(Default::default())).to_field("geometry", true);
        let wkt = from_arrow_array(&wkt_values, &field).unwrap();
        assert_float_arrays_eq(
            euclidean_length(wkt.as_ref()).unwrap(),
            geoarrow_expr_geo::euclidean_length(wkt.as_ref()).unwrap(),
        );
    }

    #[tokio::test]
    async fn test_linestring_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 5.0);
    }

    #[tokio::test]
    async fn test_point_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('POINT(1 2)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 0.0);
    }

    #[tokio::test]
    async fn test_multilinestring_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('MULTILINESTRING((0 0, 3 4), (0 0, 4 3))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 10.0); // 5.0 + 5.0
    }

    #[tokio::test]
    async fn test_polygon_length() {
        let ctx = SessionContext::new();

        ctx.register_udf(Length::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Length(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 0.0); // Polygons return 0 for length
    }
}
