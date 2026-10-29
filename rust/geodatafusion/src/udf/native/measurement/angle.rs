//! Azimuth and angle measurement UDFs.

use std::f64::consts::PI;
use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::builder::Float64Builder;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, LineStringTrait, PointTrait};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use super::measure::{is_null_array, null_float64};
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    geometry_array_from_arrow, is_null_geometry_field, merge_metadata,
};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Azimuth;

impl Azimuth {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Azimuth {
    fn default() -> Self {
        Self::new()
    }
}

static AZIMUTH_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::any(2, Volatility::Immutable));
static AZIMUTH_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Azimuth {
    fn name(&self) -> &str {
        "st_azimuth"
    }

    fn signature(&self) -> &Signature {
        &AZIMUTH_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        merge_metadata(
            args.arg_fields.iter().map(|field| field.as_ref()),
            "ST_Azimuth",
        )?;
        Ok(Arc::new(Field::new("", DataType::Float64, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(azimuth_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AZIMUTH_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the north-based clockwise azimuth in radians of the segment from the first point to the second point. Z and M are ignored. Coincident points return NULL.",
                "ST_Azimuth(pointA, pointB)",
            )
            .with_argument("pointA", "point geometry")
            .with_argument("pointB", "point geometry")
            .with_sql_example(
                "SELECT ST_Azimuth(ST_Point(0, 0), ST_Point(1, 0));",
            )
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Angle;

impl Angle {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Angle {
    fn default() -> Self {
        Self::new()
    }
}

static ANGLE_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::one_of(
        vec![
            TypeSignature::Any(2),
            TypeSignature::Any(3),
            TypeSignature::Any(4),
        ],
        Volatility::Immutable,
    )
});
static ANGLE_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Angle {
    fn name(&self) -> &str {
        "st_angle"
    }

    fn signature(&self) -> &Signature {
        &ANGLE_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        merge_metadata(
            args.arg_fields.iter().map(|field| field.as_ref()),
            "ST_Angle",
        )?;
        Ok(Arc::new(Field::new("", DataType::Float64, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(angle_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(ANGLE_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the clockwise angle in radians between two vectors. The two-line form uses each line's first and last point; the three-point form uses the second point as the vertex; the four-point form uses the first pair and the second pair. Z and M are ignored. Degenerate vectors return NULL.",
                "ST_Angle(lineA, lineB), ST_Angle(point1, point2, point3), or ST_Angle(point1, point2, point3, point4)",
            )
            .with_argument("geometry", "lines or points forming the measured vectors")
            .with_sql_example(
                "SELECT ST_Angle(ST_Point(1, 0), ST_Point(0, 0), ST_Point(0, 1));",
            )
            .build()
        }))
    }
}

fn azimuth_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    ensure_array_lengths("ST_Azimuth", &arrays, args.number_rows)?;
    if arrays.iter().any(is_null_array)
        || args
            .arg_fields
            .iter()
            .any(|field| is_null_geometry_field(field.as_ref()))
    {
        return Ok(ColumnarValue::Array(Arc::new(null_float64(
            args.number_rows,
        ))));
    }
    let left_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let right_array = geometry_array_from_arrow(&arrays[1], &args.arg_fields[1])?;
    let left = point_values(left_array.as_ref())?;
    let right = point_values(right_array.as_ref())?;

    let mut builder = Float64Builder::with_capacity(args.number_rows);
    for index in 0..args.number_rows {
        let first = left[index];
        let second = right[index];
        if matches!(first, PointValue::Null) || matches!(second, PointValue::Null) {
            builder.append_null();
            continue;
        }

        validate_points("ST_Azimuth", "Error extracting point", &[first, second])?;
        let (PointValue::Coordinate { x: x1, y: y1 }, PointValue::Coordinate { x: x2, y: y2 }) =
            (first, second)
        else {
            unreachable!("validated azimuth arguments are points")
        };

        match azimuth((x1, y1), (x2, y2)) {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn angle_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    ensure_array_lengths("ST_Angle", &arrays, args.number_rows)?;
    if arrays.iter().any(is_null_array)
        || args
            .arg_fields
            .iter()
            .any(|field| is_null_geometry_field(field.as_ref()))
    {
        return Ok(ColumnarValue::Array(Arc::new(null_float64(
            args.number_rows,
        ))));
    }
    match arrays.len() {
        2 => angle_lines_impl(&args, &arrays),
        3 => angle_points_impl(&args, &arrays),
        4 => angle_four_points_impl(&args, &arrays),
        count => Err(DataFusionError::Internal(format!(
            "ST_Angle expects 2, 3, or 4 arguments, got {count}"
        ))
        .into()),
    }
}

fn ensure_array_lengths(
    function: &str,
    arrays: &[ArrayRef],
    number_rows: usize,
) -> GeoDataFusionResult<()> {
    if arrays.iter().any(|array| array.len() != number_rows) {
        return Err(DataFusionError::Internal(format!(
            "{function} input arrays have mixed lengths"
        ))
        .into());
    }
    Ok(())
}

fn angle_lines_impl(
    args: &ScalarFunctionArgs,
    arrays: &[ArrayRef],
) -> GeoDataFusionResult<ColumnarValue> {
    let first_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let second_array = geometry_array_from_arrow(&arrays[1], &args.arg_fields[1])?;
    let first = segment_values(first_array.as_ref())?;
    let second = segment_values(second_array.as_ref())?;

    let mut builder = Float64Builder::with_capacity(args.number_rows);
    for index in 0..args.number_rows {
        let (Some(first), Some(second)) = (first[index], second[index]) else {
            builder.append_null();
            continue;
        };

        match angle_between(first.start, first.end, second.start, second.end) {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn angle_points_impl(
    args: &ScalarFunctionArgs,
    arrays: &[ArrayRef],
) -> GeoDataFusionResult<ColumnarValue> {
    let points = point_columns(args, arrays)?;
    let mut builder = Float64Builder::with_capacity(args.number_rows);

    for index in 0..args.number_rows {
        let points = points
            .iter()
            .map(|column| column[index])
            .collect::<Vec<_>>();
        if points.iter().any(|point| matches!(point, PointValue::Null)) {
            builder.append_null();
            continue;
        }

        validate_points("ST_Angle", "Empty geometry", &points)?;
        let (
            PointValue::Coordinate { x: x1, y: y1 },
            PointValue::Coordinate { x: x2, y: y2 },
            PointValue::Coordinate { x: x3, y: y3 },
        ) = (points[0], points[1], points[2])
        else {
            unreachable!("validated angle arguments are points")
        };

        match angle_three_points((x1, y1), (x2, y2), (x3, y3)) {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn angle_four_points_impl(
    args: &ScalarFunctionArgs,
    arrays: &[ArrayRef],
) -> GeoDataFusionResult<ColumnarValue> {
    let points = point_columns(args, arrays)?;
    let mut builder = Float64Builder::with_capacity(args.number_rows);

    for index in 0..args.number_rows {
        let points = points
            .iter()
            .map(|column| column[index])
            .collect::<Vec<_>>();
        if points.iter().any(|point| matches!(point, PointValue::Null)) {
            builder.append_null();
            continue;
        }

        // PostGIS represents the three-point form as a four-point call with an
        // EMPTY fourth point, so an empty fourth argument must fall back to
        // the three-point calculation.
        if matches!(points[3], PointValue::Empty) {
            validate_points("ST_Angle", "Empty geometry", &points[..3])?;
            let (
                PointValue::Coordinate { x: x1, y: y1 },
                PointValue::Coordinate { x: x2, y: y2 },
                PointValue::Coordinate { x: x3, y: y3 },
            ) = (points[0], points[1], points[2])
            else {
                unreachable!("validated angle arguments are points")
            };
            match angle_three_points((x1, y1), (x2, y2), (x3, y3)) {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
            continue;
        }

        validate_points("ST_Angle", "Empty geometry", &points)?;
        let (
            PointValue::Coordinate { x: x1, y: y1 },
            PointValue::Coordinate { x: x2, y: y2 },
            PointValue::Coordinate { x: x3, y: y3 },
            PointValue::Coordinate { x: x4, y: y4 },
        ) = (points[0], points[1], points[2], points[3])
        else {
            unreachable!("validated angle arguments are points")
        };
        match angle_between((x1, y1), (x2, y2), (x3, y3), (x4, y4)) {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn point_columns(
    args: &ScalarFunctionArgs,
    arrays: &[ArrayRef],
) -> GeoDataFusionResult<Vec<Vec<PointValue>>> {
    let mut columns = Vec::with_capacity(arrays.len());
    for (index, array) in arrays.iter().enumerate() {
        let geo_array = geometry_array_from_arrow(array, &args.arg_fields[index])?;
        columns.push(point_values(geo_array.as_ref())?);
    }
    Ok(columns)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PointValue {
    Null,
    Empty,
    NotPoint,
    Coordinate { x: f64, y: f64 },
}

fn point_values(array: &dyn GeoArrowArray) -> GeoArrowResult<Vec<PointValue>> {
    downcast_geoarrow_array!(array, point_values_impl)
}

fn point_values_impl<'a, A>(array: &'a A) -> GeoArrowResult<Vec<PointValue>>
where
    A: GeoArrowArrayAccessor<'a>,
    A::Item: GeometryTrait<T = f64>,
{
    let mut values = Vec::with_capacity(array.len());
    for item in array.iter() {
        let value = match item {
            None => PointValue::Null,
            Some(item) => match item?.as_type() {
                GeometryType::Point(point) => match point.coord() {
                    Some(coord) => PointValue::Coordinate {
                        x: coord.x(),
                        y: coord.y(),
                    },
                    None => PointValue::Empty,
                },
                _ => PointValue::NotPoint,
            },
        };
        values.push(value);
    }
    Ok(values)
}

#[derive(Debug, Clone, Copy)]
struct Segment {
    start: (f64, f64),
    end: (f64, f64),
}

fn segment_values(array: &dyn GeoArrowArray) -> GeoArrowResult<Vec<Option<Segment>>> {
    downcast_geoarrow_array!(array, segment_values_impl)
}

fn segment_values_impl<'a, A>(array: &'a A) -> GeoArrowResult<Vec<Option<Segment>>>
where
    A: GeoArrowArrayAccessor<'a>,
    A::Item: GeometryTrait<T = f64>,
{
    let mut values = Vec::with_capacity(array.len());
    for item in array.iter() {
        let value = match item {
            None => None,
            Some(item) => match item?.as_type() {
                GeometryType::LineString(line_string) => {
                    let num_coords = line_string.num_coords();
                    if num_coords < 2 {
                        None
                    } else {
                        match (line_string.coord(0), line_string.coord(num_coords - 1)) {
                            (Some(start), Some(end)) => Some(Segment {
                                start: (start.x(), start.y()),
                                end: (end.x(), end.y()),
                            }),
                            _ => None,
                        }
                    }
                }
                _ => None,
            },
        };
        values.push(value);
    }
    Ok(values)
}

fn validate_points(
    function: &str,
    empty_message: &str,
    points: &[PointValue],
) -> GeoDataFusionResult<()> {
    for point in points {
        match point {
            PointValue::Null | PointValue::Coordinate { .. } => {}
            PointValue::Empty => {
                return Err(
                    DataFusionError::Execution(format!("{function}: {empty_message}")).into(),
                );
            }
            PointValue::NotPoint => {
                return Err(DataFusionError::Execution(format!(
                    "{function}: Argument must be POINT geometries"
                ))
                .into());
            }
        }
    }
    Ok(())
}

/// PostGIS azimuth formula: north-based clockwise radians in `[0, 2*pi)`.
fn azimuth(first: (f64, f64), second: (f64, f64)) -> Option<f64> {
    if first.0 == second.0 && first.1 == second.1 {
        return None;
    }

    let dx = second.0 - first.0;
    let dy = second.1 - first.1;
    Some((2.0 * PI + PI / 2.0 - dy.atan2(dx)) % (2.0 * PI))
}

fn angle_three_points(first: (f64, f64), vertex: (f64, f64), third: (f64, f64)) -> Option<f64> {
    angle_between(first, vertex, third, vertex)
}

fn angle_between(
    first_start: (f64, f64),
    first_end: (f64, f64),
    second_start: (f64, f64),
    second_end: (f64, f64),
) -> Option<f64> {
    let first_azimuth = azimuth(first_start, first_end)?;
    let second_azimuth = azimuth(second_start, second_end)?;
    let mut result = second_azimuth - first_azimuth;
    if result < 0.0 {
        result += 2.0 * PI;
    }
    Some(result)
}
