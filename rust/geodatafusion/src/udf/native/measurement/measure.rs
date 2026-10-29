//! Shared geometry measurement kernels used by the native measurement UDFs.

use arrow_array::builder::Float64Builder;
use arrow_array::{ArrayRef, Float64Array};
use arrow_schema::DataType;
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPolygonTrait, PolygonTrait,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

/// Which coordinate axes participate in a length measurement.
///
/// `ThreeDimensional` uses Z when a geometry has it and otherwise falls back to
/// the same XY computation as `TwoDimensional`. M is never treated as Z.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum Axis {
    TwoDimensional,
    ThreeDimensional,
}

pub(crate) fn is_null_array(array: &ArrayRef) -> bool {
    array.data_type() == &DataType::Null
}

pub(crate) fn null_float64(len: usize) -> Float64Array {
    Float64Array::new_null(len)
}

/// Sum the lengths of all linear components in a geometry array.
///
/// Polygon rings and other areal components contribute zero, matching
/// PostGIS `ST_Length` and `ST_3DLength`.
pub(crate) fn linear_length(array: &dyn GeoArrowArray, axis: Axis) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, linear_length_impl, axis)
}

fn linear_length_impl<'a, A>(array: &'a A, axis: Axis) -> GeoArrowResult<Float64Array>
where
    A: GeoArrowArrayAccessor<'a>,
    A::Item: GeometryTrait<T = f64>,
{
    let mut builder = Float64Builder::with_capacity(array.len());
    for item in array.iter() {
        match item {
            Some(item) => builder.append_value(linear_length_value(&item?, axis)),
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

/// Sum the lengths of all linear components in one geometry.
pub(crate) fn linear_length_value<G: GeometryTrait<T = f64>>(geometry: &G, axis: Axis) -> f64 {
    match geometry.as_type() {
        GeometryType::LineString(line_string) => line_string_length(line_string, axis),
        GeometryType::MultiLineString(multi_line_string) => multi_line_string
            .line_strings()
            .map(|line_string| line_string_length(&line_string, axis))
            .fold(0.0, |total, length| total + length),
        GeometryType::GeometryCollection(collection) => collection
            .geometries()
            .map(|geometry| linear_length_value(&geometry, axis))
            .fold(0.0, |total, length| total + length),
        _ => 0.0,
    }
}

/// Sum the perimeters of all polygonal components in a geometry array.
///
/// Linear and point components contribute zero, matching PostGIS
/// `ST_Perimeter` and `ST_3DPerimeter`.
pub(crate) fn polygonal_perimeter(
    array: &dyn GeoArrowArray,
    axis: Axis,
) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, polygonal_perimeter_impl, axis)
}

fn polygonal_perimeter_impl<'a, A>(array: &'a A, axis: Axis) -> GeoArrowResult<Float64Array>
where
    A: GeoArrowArrayAccessor<'a>,
    A::Item: GeometryTrait<T = f64>,
{
    let mut builder = Float64Builder::with_capacity(array.len());
    for item in array.iter() {
        match item {
            Some(item) => builder.append_value(polygonal_perimeter_value(&item?, axis)),
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

/// Sum the perimeters of all polygonal components in one geometry.
pub(crate) fn polygonal_perimeter_value<G: GeometryTrait<T = f64>>(
    geometry: &G,
    axis: Axis,
) -> f64 {
    match geometry.as_type() {
        GeometryType::Polygon(polygon) => polygon_perimeter(polygon, axis),
        GeometryType::MultiPolygon(multi_polygon) => multi_polygon
            .polygons()
            .map(|polygon| polygon_perimeter(&polygon, axis))
            .fold(0.0, |total, perimeter| total + perimeter),
        GeometryType::GeometryCollection(collection) => collection
            .geometries()
            .map(|geometry| polygonal_perimeter_value(&geometry, axis))
            .fold(0.0, |total, perimeter| total + perimeter),
        _ => 0.0,
    }
}

fn polygon_perimeter<P: PolygonTrait<T = f64>>(polygon: &P, axis: Axis) -> f64 {
    let mut perimeter = 0.0;
    if let Some(exterior) = polygon.exterior() {
        perimeter += line_string_length(&exterior, axis);
    }
    for interior in polygon.interiors() {
        perimeter += line_string_length(&interior, axis);
    }
    perimeter
}

fn line_string_length<L: LineStringTrait<T = f64>>(line_string: &L, axis: Axis) -> f64 {
    let mut coordinates = line_string.coords();
    let Some(mut previous) = coordinates.next() else {
        return 0.0;
    };

    let mut length = 0.0;
    for coordinate in coordinates {
        length += coordinate_distance(&previous, &coordinate, axis);
        previous = coordinate;
    }
    length
}

fn coordinate_distance<C: CoordTrait<T = f64>>(left: &C, right: &C, axis: Axis) -> f64 {
    let dx = left.x() - right.x();
    let dy = left.y() - right.y();

    match axis {
        Axis::TwoDimensional => (dx * dx + dy * dy).sqrt(),
        Axis::ThreeDimensional => {
            let dz = coordinate_z(left) - coordinate_z(right);
            (dx * dx + dy * dy + dz * dz).sqrt()
        }
    }
}

fn coordinate_z<C: CoordTrait<T = f64>>(coordinate: &C) -> f64 {
    match coordinate.dim() {
        Dimensions::Xyz | Dimensions::Xyzm => coordinate.nth_or_panic(2),
        _ => 0.0,
    }
}
