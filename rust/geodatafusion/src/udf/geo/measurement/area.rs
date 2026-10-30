use std::sync::{Arc, OnceLock};

use arrow_array::builder::Float64Builder;
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait, TriangleTrait,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, WrapArray, downcast_geoarrow_array};
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Area;

impl Area {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Area {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Area {
    fn name(&self) -> &str {
        "st_area"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(area_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the area of a polygonal geometry.",
                "ST_Area(geom)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn area_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let geo_type = GeoArrowType::from_arrow_field(&args.arg_fields[0])?;
    let result = unsigned_area(&geo_type.wrap_array(&array)?)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn unsigned_area(array: &dyn GeoArrowArray) -> GeoArrowResult<arrow_array::Float64Array> {
    downcast_geoarrow_array!(array, _unsigned_area_impl)
}

fn _zeros(len: usize, nulls: Option<NullBuffer>) -> arrow_array::Float64Array {
    arrow_array::Float64Array::new(vec![0.0; len].into(), nulls)
}

fn _unsigned_area_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<arrow_array::Float64Array> {
    use GeoArrowType::*;

    match array.data_type() {
        Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) => {
            Ok(_zeros(array.len(), array.logical_nulls()))
        }
        _ => {
            let mut builder = Float64Builder::with_capacity(array.len());
            for item in array.iter() {
                if let Some(geometry) = item {
                    builder.append_value(geometry_unsigned_area(&geometry?)?);
                } else {
                    builder.append_null();
                }
            }
            Ok(builder.finish())
        }
    }
}

fn geometry_unsigned_area(geometry: &impl GeometryTrait<T = f64>) -> GeoArrowResult<f64> {
    use GeometryType::*;

    Ok(match geometry.as_type() {
        Point(point) => {
            if point.coord().is_none() {
                return Err(empty_point_error());
            }
            0.0
        }
        LineString(_) | Line(_) => 0.0,
        Polygon(polygon) => polygon_unsigned_area(polygon),
        MultiPoint(multi_point) => {
            for point in multi_point.points() {
                if point.coord().is_none() {
                    return Err(empty_point_error());
                }
            }
            0.0
        }
        MultiLineString(_) => 0.0,
        MultiPolygon(multi_polygon) => multi_polygon.polygons().fold(0.0, |total, polygon| {
            total + polygon_unsigned_area(&polygon)
        }),
        GeometryCollection(collection) => {
            let mut total = 0.0;
            for child in collection.geometries() {
                total += geometry_unsigned_area(&child)?;
            }
            total
        }
        Rect(rect) => rect_unsigned_area(rect),
        Triangle(triangle) => triangle_unsigned_area(triangle),
    })
}

fn polygon_unsigned_area(polygon: &impl PolygonTrait<T = f64>) -> f64 {
    let area = polygon
        .exterior()
        .map(|ring| ring_unsigned_area(&ring))
        .unwrap_or(0.0);
    let is_negative = area < 0.0;
    let area = polygon.interiors().fold(area.abs(), |total, ring| {
        total - ring_unsigned_area(&ring).abs()
    });
    if is_negative { -area } else { area }.abs()
}

// `geo::Polygon::new` closes unclosed rings before applying the shoelace formula.
fn ring_unsigned_area(ring: &impl LineStringTrait<T = f64>) -> f64 {
    let num_coords = ring.num_coords();
    if num_coords < 2 {
        return 0.0;
    }

    let first = ring.coord(0).unwrap();
    let second = ring.coord(1).unwrap();
    let last = ring.coord(num_coords - 1).unwrap();
    let needs_close = !coords_equal(&first, &last);
    if num_coords + usize::from(needs_close) < 3 {
        return 0.0;
    }

    // A point whose coordinate contains NaN cannot compare equal to the
    // freshly appended closing coordinate, so geo's ring area returns zero.
    let shift_x = first.x();
    let shift_y = first.y();
    if shift_x.is_nan() || shift_y.is_nan() {
        return 0.0;
    }

    let mut previous_x = second.x() - shift_x;
    let mut previous_y = second.y() - shift_y;
    let mut sum = 0.0;

    for coordinate in ring.coords().skip(2) {
        let x = coordinate.x() - shift_x;
        let y = coordinate.y() - shift_y;
        sum += previous_x * y - previous_y * x;
        previous_x = x;
        previous_y = y;
    }

    if needs_close {
        sum += previous_x * 0.0 - previous_y * 0.0;
    }

    sum / 2.0
}

fn rect_unsigned_area(rect: &impl RectTrait<T = f64>) -> f64 {
    let c1 = rect.min();
    let c2 = rect.max();
    let (min_x, max_x) = if c1.x() < c2.x() {
        (c1.x(), c2.x())
    } else {
        (c2.x(), c1.x())
    };
    let (min_y, max_y) = if c1.y() < c2.y() {
        (c1.y(), c2.y())
    } else {
        (c2.y(), c1.y())
    };
    (max_x - min_x) * (max_y - min_y)
}

fn triangle_unsigned_area(triangle: &impl TriangleTrait<T = f64>) -> f64 {
    let mut first = triangle.first();
    let second = triangle.second();
    let mut third = triangle.third();
    let orientation = (second.x() - first.x()) * (third.y() - first.y())
        - (second.y() - first.y()) * (third.x() - first.x());
    if orientation < 0.0 {
        std::mem::swap(&mut first, &mut third);
    }

    let shift_x = first.x();
    let shift_y = first.y();
    let previous_x = second.x() - shift_x;
    let previous_y = second.y() - shift_y;
    let x = third.x() - shift_x;
    let y = third.y() - shift_y;
    ((previous_x * y - previous_y * x) / 2.0).abs()
}

fn coords_equal(first: &impl CoordTrait<T = f64>, second: &impl CoordTrait<T = f64>) -> bool {
    first.x() == second.x() && first.y() == second.y()
}

fn empty_point_error() -> GeoArrowError {
    GeoArrowError::IncorrectGeometryType("geo crate does not support empty points.".to_string())
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use arrow_array::{Array, Float64Array, StringArray};
    use datafusion::prelude::SessionContext;
    use geo::Area as _;
    use geoarrow_array::array::from_arrow_array;
    use geoarrow_array::builder::RectBuilder;
    use geoarrow_array::cast::to_wkb;
    use geoarrow_array::test;
    use geoarrow_schema::{BoxType, CoordType, Dimension, WktType};

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
                    unsigned_area(&test::point::array(coord_type, dim)).unwrap(),
                    geoarrow_expr_geo::unsigned_area(&test::point::array(coord_type, dim)).unwrap(),
                );
                assert_float_arrays_eq(
                    unsigned_area(&test::linestring::array(coord_type, dim)).unwrap(),
                    geoarrow_expr_geo::unsigned_area(&test::linestring::array(coord_type, dim))
                        .unwrap(),
                );
                assert_float_arrays_eq(
                    unsigned_area(&test::polygon::array(coord_type, dim)).unwrap(),
                    geoarrow_expr_geo::unsigned_area(&test::polygon::array(coord_type, dim))
                        .unwrap(),
                );
                assert_float_arrays_eq(
                    unsigned_area(&test::multipolygon::array(coord_type, dim)).unwrap(),
                    geoarrow_expr_geo::unsigned_area(&test::multipolygon::array(coord_type, dim))
                        .unwrap(),
                );
                assert_float_arrays_eq(
                    unsigned_area(&test::geometrycollection::array(coord_type, dim, false))
                        .unwrap(),
                    geoarrow_expr_geo::unsigned_area(&test::geometrycollection::array(
                        coord_type, dim, false,
                    ))
                    .unwrap(),
                );
            }

            let geometry = test::geometry::array(coord_type, false);
            assert_float_arrays_eq(
                unsigned_area(&geometry).unwrap(),
                geoarrow_expr_geo::unsigned_area(&geometry).unwrap(),
            );
            assert_float_arrays_eq(
                unsigned_area(&to_wkb::<i32>(&geometry).unwrap()).unwrap(),
                geoarrow_expr_geo::unsigned_area(&to_wkb::<i32>(&geometry).unwrap()).unwrap(),
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
                unsigned_area(&rect).unwrap(),
                geoarrow_expr_geo::unsigned_area(&rect).unwrap(),
            );
        }

        let wkt_values = StringArray::from(vec![
            Some("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"),
            Some("POLYGON Z ((0 0 7, 2 0 7, 2 3 7, 0 3 7, 0 0 7))"),
            Some("POLYGON EMPTY"),
            None,
        ]);
        let field = GeoArrowType::Wkt(WktType::new(Default::default())).to_field("geometry", true);
        let wkt = from_arrow_array(&wkt_values, &field).unwrap();
        assert_float_arrays_eq(
            unsigned_area(wkt.as_ref()).unwrap(),
            geoarrow_expr_geo::unsigned_area(wkt.as_ref()).unwrap(),
        );
    }

    #[test]
    fn triangle_matches_geo_area() {
        let triangle = geo::Triangle::new(
            geo::coord! { x: 0.0, y: 0.0 },
            geo::coord! { x: 10.0, y: 20.0 },
            geo::coord! { x: 20.0, y: -10.0 },
        );
        assert_eq!(
            triangle_unsigned_area(&triangle).to_bits(),
            triangle.unsigned_area().to_bits()
        );
    }

    #[test]
    fn empty_point_error_matches_geoarrow_expr() {
        let wkt_values = StringArray::from(vec![Some("GEOMETRYCOLLECTION (POINT EMPTY)")]);
        let field = GeoArrowType::Wkt(WktType::new(Default::default())).to_field("geometry", true);
        let wkt = from_arrow_array(&wkt_values, &field).unwrap();
        let actual = unsigned_area(wkt.as_ref()).unwrap_err().to_string();
        let expected = geoarrow_expr_geo::unsigned_area(wkt.as_ref())
            .unwrap_err()
            .to_string();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn test() {
        let ctx = SessionContext::new();

        ctx.register_udf(Area::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql(
                "select ST_Area(ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,
				 743265 2967450,743265.625 2967416,743238 2967416))'));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let val = col.as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 928.625);
    }
}
