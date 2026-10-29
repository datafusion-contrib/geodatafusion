//! SVG path output for native geometry arrays.

use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType as NativeGeometryType,
    LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait,
    PolygonTrait, RectTrait,
};
use geoarrow_array::cast::to_wkb;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use super::format::{
    coerce_geometry_with_optional_ints, format_ordinate, is_plain_wkt_field, plain_wkt_array,
};
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{expand_rect_to_geometry, geometry_array_from_arrow};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsSVG {
    signature: Signature,
}

impl AsSVG {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for AsSVG {
    fn default() -> Self {
        Self::new()
    }
}

static AS_SVG_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsSVG {
    fn name(&self) -> &str {
        "st_assvg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_geometry_with_optional_ints(arg_types, 2, "ST_AsSVG", true)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "return_field_from_args computes ST_AsSVG output".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_field = &args.arg_fields[0];
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Field::new(input_field.name(), DataType::Utf8, nullable).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(as_svg_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_SVG_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns SVG path data for a geometry. Relative mode (rel != 0) uses relative \
                 line commands, top-level points use x/y instead of cx/cy, and Y coordinates are \
                 negated as required by the SVG coordinate system. maxdecimaldigits above 308 \
                 raises an error, and EMPTY members contribute no output.",
                "ST_AsSVG(geometry[, rel[, maxdecimaldigits]])",
            )
            .with_argument("geometry", "input geometry")
            .with_argument("rel", "optional relative-path selector")
            .with_argument("maxdecimaldigits", "optional maximum decimal digits")
            .build()
        }))
    }
}

fn as_svg_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays[0].data_type() == &DataType::Null {
        let mut builder = arrow_array::builder::StringBuilder::new();
        for _ in 0..args.number_rows {
            builder.append_null();
        }
        return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
    }

    let input: Arc<dyn GeoArrowArray> = if is_plain_wkt_field(&args.arg_fields[0]) {
        plain_wkt_array(&arrays[0])?
    } else {
        geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?
    };
    let geometry_nulls = input.logical_nulls();
    let expanded = expand_rect_to_geometry(input.as_ref())?;
    let input: &dyn GeoArrowArray = expanded
        .as_ref()
        .map_or(input.as_ref(), |expanded| expanded);
    let wkb = to_wkb::<i32>(input)?;
    let mut output = arrow_array::builder::StringBuilder::new();

    for row in 0..wkb.len() {
        if geometry_nulls
            .as_ref()
            .is_some_and(|nulls| nulls.is_null(row))
            || arrays.iter().skip(1).any(|array| array.is_null(row))
        {
            output.append_null();
            continue;
        }
        let relative = arrays
            .get(1)
            .and_then(|array| int_value(array, row))
            .map(|value| value != 0)
            .unwrap_or(false);
        let precision = arrays
            .get(2)
            .and_then(|array| int_value(array, row))
            .map(|value| i32::try_from(value).unwrap_or(i32::MAX))
            .unwrap_or(15);
        if precision > 308 {
            return Err(DataFusionError::Execution(format!(
                "ST_AsSVG precision {precision} exceeds the supported range"
            ))
            .into());
        }
        let geometry = wkb
            .value(row)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let mut svg = String::new();
        write_geometry(&mut svg, &geometry, relative, precision)
            .map_err(DataFusionError::Execution)?;
        output.append_value(svg);
    }

    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

fn int_value(array: &ArrayRef, row: usize) -> Option<i64> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Int64 => Some(
            array
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row),
        ),
        DataType::Int32 => Some(i64::from(
            array
                .as_primitive::<arrow_array::types::Int32Type>()
                .value(row),
        )),
        DataType::Null => None,
        other => panic!("unexpected SVG integer array type {other}"),
    }
}

fn is_empty(geometry: &impl GeometryTrait<T = f64>) -> bool {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => point.coord().is_none(),
        NativeGeometryType::LineString(line) => line.num_coords() == 0,
        NativeGeometryType::Polygon(polygon) => {
            polygon.exterior().is_none_or(|ring| ring.num_coords() == 0)
        }
        NativeGeometryType::MultiPoint(multi_point) => {
            multi_point.num_points() == 0
                || (0..multi_point.num_points()).all(|index| {
                    multi_point
                        .point(index)
                        .is_none_or(|point| point.coord().is_none())
                })
        }
        NativeGeometryType::MultiLineString(multi_line) => {
            multi_line.num_line_strings() == 0
                || (0..multi_line.num_line_strings()).all(|index| {
                    multi_line
                        .line_string(index)
                        .is_none_or(|line| line.num_coords() == 0)
                })
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            multi_polygon.num_polygons() == 0
                || (0..multi_polygon.num_polygons()).all(|index| {
                    multi_polygon.polygon(index).is_none_or(|polygon| {
                        polygon.exterior().is_none_or(|ring| ring.num_coords() == 0)
                            && polygon.interiors().all(|ring| ring.num_coords() == 0)
                    })
                })
        }
        NativeGeometryType::GeometryCollection(collection) => {
            collection.num_geometries() == 0
                || (0..collection.num_geometries()).all(|index| {
                    collection
                        .geometry(index)
                        .is_none_or(|child| is_empty(&child))
                })
        }
        NativeGeometryType::Rect(rect) => {
            let min = rect.min();
            let max = rect.max();
            min.x() == max.x() && min.y() == max.y()
        }
        NativeGeometryType::Triangle(_) | NativeGeometryType::Line(_) => true,
    }
}

fn write_geometry(
    output: &mut String,
    geometry: &impl GeometryTrait<T = f64>,
    relative: bool,
    precision: i32,
) -> std::result::Result<(), String> {
    if is_empty(geometry) {
        return Ok(());
    }

    match geometry.as_type() {
        NativeGeometryType::Point(point) => {
            let coord = point.coord().ok_or_else(|| "empty point".to_string())?;
            write_point(output, &coord, relative, precision);
        }
        NativeGeometryType::LineString(line) => {
            write_line(output, line, relative, precision)?;
        }
        NativeGeometryType::Polygon(polygon) => {
            write_polygon(output, polygon, relative, precision)?;
        }
        NativeGeometryType::MultiPoint(multi_point) => {
            let mut written = 0usize;
            for index in 0..multi_point.num_points() {
                if let Some(point) = multi_point.point(index)
                    && let Some(coord) = point.coord()
                {
                    if written > 0 {
                        output.push(',');
                    }
                    write_point(output, &coord, relative, precision);
                    written += 1;
                }
            }
        }
        NativeGeometryType::MultiLineString(multi_line) => {
            let mut written = 0usize;
            for index in 0..multi_line.num_line_strings() {
                if let Some(line) = multi_line.line_string(index) {
                    if is_empty(&line) {
                        continue;
                    }
                    if written > 0 {
                        output.push(' ');
                    }
                    write_line(output, &line, relative, precision)?;
                    written += 1;
                }
            }
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            let mut written = 0usize;
            for index in 0..multi_polygon.num_polygons() {
                if let Some(polygon) = multi_polygon.polygon(index) {
                    if is_empty(&polygon) {
                        continue;
                    }
                    if written > 0 {
                        output.push(' ');
                    }
                    write_polygon(output, &polygon, relative, precision)?;
                    written += 1;
                }
            }
        }
        NativeGeometryType::GeometryCollection(collection) => {
            let mut written = 0usize;
            for index in 0..collection.num_geometries() {
                let child = collection
                    .geometry(index)
                    .ok_or_else(|| "missing geometry collection member".to_string())?;
                if is_empty(&child) {
                    continue;
                }
                if written > 0 {
                    output.push(';');
                }
                write_geometry(output, &child, relative, precision)?;
                written += 1;
            }
        }
        NativeGeometryType::Rect(rect) => {
            let min = rect.min();
            let max = rect.max();
            let ring = [
                (min.x(), min.y()),
                (min.x(), max.y()),
                (max.x(), max.y()),
                (max.x(), min.y()),
                (min.x(), min.y()),
            ];
            write_ring(output, &ring, relative, precision);
        }
        NativeGeometryType::Triangle(_) | NativeGeometryType::Line(_) => {
            return Err("SVG does not support this geometry type".to_string());
        }
    }
    Ok(())
}

fn write_point(
    output: &mut String,
    coord: &impl CoordTrait<T = f64>,
    relative: bool,
    precision: i32,
) {
    let x = format_ordinate(coord.x(), precision);
    let y = format_ordinate(-coord.y(), precision);
    if relative {
        output.push_str(&format!("x=\"{x}\" y=\"{y}\""));
    } else {
        output.push_str(&format!("cx=\"{x}\" cy=\"{y}\""));
    }
}

fn write_line(
    output: &mut String,
    line: &impl LineStringTrait<T = f64>,
    relative: bool,
    precision: i32,
) -> std::result::Result<(), String> {
    output.push_str("M ");
    if relative {
        write_relative_polyline(output, line, true, precision)
    } else {
        write_absolute_polyline(output, line, true, precision)
    }
}

fn write_polygon(
    output: &mut String,
    polygon: &impl PolygonTrait<T = f64>,
    relative: bool,
    precision: i32,
) -> std::result::Result<(), String> {
    let mut written = false;
    if let Some(exterior) = polygon.exterior() {
        if exterior.num_coords() == 0 {
            return Ok(());
        }
        if written {
            output.push(' ');
        }
        output.push_str("M ");
        if relative {
            write_relative_polyline(output, &exterior, false, precision)?;
            output.push_str(" z");
        } else {
            write_absolute_polyline(output, &exterior, false, precision)?;
            output.push_str(" Z");
        }
        written = true;
    }
    for ring in polygon.interiors() {
        if ring.num_coords() == 0 {
            continue;
        }
        if written {
            output.push(' ');
        }
        output.push_str("M ");
        if relative {
            write_relative_polyline(output, &ring, false, precision)?;
            output.push_str(" z");
        } else {
            write_absolute_polyline(output, &ring, false, precision)?;
            output.push_str(" Z");
        }
        written = true;
    }
    Ok(())
}

fn write_ring(output: &mut String, ring: &[(f64, f64)], relative: bool, precision: i32) {
    output.push_str("M ");
    if relative {
        write_relative_points(output, ring, false, precision);
        output.push_str(" z");
    } else {
        write_absolute_points(output, ring, false, precision);
        output.push_str(" Z");
    }
}

fn write_relative_polyline(
    output: &mut String,
    line: &impl LineStringTrait<T = f64>,
    close_ring: bool,
    precision: i32,
) -> std::result::Result<(), String> {
    let end = if close_ring {
        line.num_coords()
    } else {
        line.num_coords().saturating_sub(1)
    };
    if line.num_coords() == 0 {
        return Ok(());
    }

    let factor = relative_factor(precision);
    let first = line
        .coord(0)
        .ok_or_else(|| "missing line start".to_string())?;
    let mut accumulated_x = round_half_away(first.x() * factor) / factor;
    let mut accumulated_y = round_half_away(first.y() * factor) / factor;
    output.push_str(&format_ordinate(accumulated_x, precision));
    output.push(' ');
    output.push_str(&format_ordinate(-accumulated_y, precision));
    output.push_str(" l");

    for index in 1..end {
        let coord = line
            .coord(index)
            .ok_or_else(|| "missing line coordinate".to_string())?;
        let x = round_half_away(coord.x() * factor) / factor;
        let y = round_half_away(coord.y() * factor) / factor;
        let delta_x = x - accumulated_x;
        let delta_y = y - accumulated_y;
        accumulated_x += delta_x;
        accumulated_y += delta_y;
        output.push(' ');
        output.push_str(&format_ordinate(delta_x, precision));
        output.push(' ');
        output.push_str(&format_ordinate(-delta_y, precision));
    }
    Ok(())
}

fn write_absolute_polyline(
    output: &mut String,
    line: &impl LineStringTrait<T = f64>,
    close_ring: bool,
    precision: i32,
) -> std::result::Result<(), String> {
    let end = if close_ring {
        line.num_coords()
    } else {
        line.num_coords().saturating_sub(1)
    };
    for index in 0..end {
        if index == 1 {
            output.push_str(" L ");
        } else if index > 0 {
            output.push(' ');
        }
        let coord = line
            .coord(index)
            .ok_or_else(|| "missing line coordinate".to_string())?;
        output.push_str(&format_ordinate(coord.x(), precision));
        output.push(' ');
        output.push_str(&format_ordinate(-coord.y(), precision));
    }
    Ok(())
}

fn write_relative_points(
    output: &mut String,
    points: &[(f64, f64)],
    close_ring: bool,
    precision: i32,
) {
    let end = if close_ring {
        points.len()
    } else {
        points.len().saturating_sub(1)
    };
    if points.is_empty() {
        return;
    }
    let factor = relative_factor(precision);
    let mut accumulated_x = round_half_away(points[0].0 * factor) / factor;
    let mut accumulated_y = round_half_away(points[0].1 * factor) / factor;
    output.push_str(&format_ordinate(accumulated_x, precision));
    output.push(' ');
    output.push_str(&format_ordinate(-accumulated_y, precision));
    output.push_str(" l");
    for (x, y) in points.iter().take(end).skip(1) {
        let x = round_half_away(*x * factor) / factor;
        let y = round_half_away(*y * factor) / factor;
        let delta_x = x - accumulated_x;
        let delta_y = y - accumulated_y;
        accumulated_x += delta_x;
        accumulated_y += delta_y;
        output.push(' ');
        output.push_str(&format_ordinate(delta_x, precision));
        output.push(' ');
        output.push_str(&format_ordinate(-delta_y, precision));
    }
}

fn write_absolute_points(
    output: &mut String,
    points: &[(f64, f64)],
    close_ring: bool,
    precision: i32,
) {
    let end = if close_ring {
        points.len()
    } else {
        points.len().saturating_sub(1)
    };
    for (index, (x, y)) in points.iter().take(end).enumerate() {
        if index == 1 {
            output.push_str(" L ");
        } else if index > 0 {
            output.push(' ');
        }
        output.push_str(&format_ordinate(*x, precision));
        output.push(' ');
        output.push_str(&format_ordinate(-*y, precision));
    }
}

fn relative_factor(precision: i32) -> f64 {
    if precision >= 0 {
        10f64.powi(precision)
    } else {
        1.0
    }
}

fn round_half_away(value: f64) -> f64 {
    value.round()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use wkt::Wkt;

    use super::*;

    fn svg(wkt: &str, relative: bool, precision: i32) -> String {
        let geometry: Wkt<f64> = Wkt::from_str(wkt).unwrap();
        let mut output = String::new();
        write_geometry(&mut output, &geometry, relative, precision).unwrap();
        output
    }

    #[test]
    fn basic_svg_shapes_match_postgis() {
        assert_eq!(svg("POINT(1.25 -2.5)", false, 15), "cx=\"1.25\" cy=\"2.5\"");
        assert_eq!(svg("POINT(1.25 -2.5)", true, 15), "x=\"1.25\" y=\"2.5\"");
        assert_eq!(
            svg("LINESTRING(0 0,1 -1,-2 3)", false, 15),
            "M 0 0 L 1 1 -2 -3"
        );
        assert_eq!(
            svg("LINESTRING(0 0,1 -1,-2 3)", true, 15),
            "M 0 0 l 1 1 -3 -4"
        );
        assert_eq!(
            svg("POLYGON((0 0,0 1,1 1,1 0,0 0))", false, 15),
            "M 0 0 L 0 -1 1 -1 1 0 Z"
        );
        assert_eq!(
            svg("POLYGON((0 0,0 1,1 1,1 0,0 0))", true, 15),
            "M 0 0 l 0 -1 1 0 0 1 z"
        );
    }
}
