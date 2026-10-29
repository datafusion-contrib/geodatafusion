use std::sync::{Arc, OnceLock};

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use geo_traits::{GeometryTrait, GeometryType, MultiLineStringTrait};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{Dimension, GeoArrowType, GeometryType as GeometryDataType, PointType};
use wkt::Wkt;
use wkt::types::{
    GeometryCollection as WktGeometryCollection, LineString as WktLineString,
    MultiLineString as WktMultiLineString,
};

use super::util::{
    Coord4, LineString4, build_generic_geometry_array, build_point_array, datafusion_error,
    finite_fraction, generic_geometry_type, geometry_dimension, has_sql_null_argument,
    multi_point_wkt, null_geometry_value, planar_metadata, point_wkt, wkt_dimension,
};
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineInterpolatePoint;

impl LineInterpolatePoint {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LineInterpolatePoint {
    fn default() -> Self {
        Self::new()
    }
}

static LINE_INTERPOLATE_POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineInterpolatePoint {
    fn name(&self) -> &str {
        "st_lineinterpolatepoint"
    }

    fn signature(&self) -> &Signature {
        line_interpolate_point_signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(point_return_field(&args, self.name())?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(interpolate_point_impl(&args, false, self.name())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LINE_INTERPOLATE_POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a point interpolated along a LineString at a fractional location. The location uses 2D length and Z and M are then interpolated from the containing segment.",
                "ST_LineInterpolatePoint(line_string, fraction)",
            )
            .with_argument("line_string", "geometry")
            .with_argument("fraction", "float8 in [0, 1]")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineInterpolatePoint3D;

impl LineInterpolatePoint3D {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LineInterpolatePoint3D {
    fn default() -> Self {
        Self::new()
    }
}

static LINE_INTERPOLATE_POINT_3D_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineInterpolatePoint3D {
    fn name(&self) -> &str {
        "st_3dlineinterpolatepoint"
    }

    fn signature(&self) -> &Signature {
        line_interpolate_point_signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(point_return_field(&args, self.name())?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(interpolate_point_impl(&args, true, self.name())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LINE_INTERPOLATE_POINT_3D_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a point interpolated along a line using 3D length when Z is present. M is interpolated from the containing segment; XY and XYM inputs use 2D length.",
                "ST_3DLineInterpolatePoint(line_string, fraction)",
            )
            .with_argument("line_string", "geometry")
            .with_argument("fraction", "float8 in [0, 1]")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineInterpolatePoints;

impl LineInterpolatePoints {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineSubstring;

impl LineSubstring {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LineSubstring {
    fn default() -> Self {
        Self::new()
    }
}

static LINE_SUBSTRING_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineSubstring {
    fn name(&self) -> &str {
        "st_linesubstring"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(3, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            generic_geometry_type(&args.arg_fields[0], self.name())?.to_field("", true),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(line_substring_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LINE_SUBSTRING_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the part of a LineString or MultiLineString between two fractional 2D-length locations. Equal fractions return a POINT.",
                "ST_LineSubstring(line_string, start_fraction, end_fraction)",
            )
            .with_argument("line_string", "LineString or MultiLineString")
            .with_argument("start_fraction", "float8 in [0, 1]")
            .with_argument("end_fraction", "float8 in [0, 1]")
            .build()
        }))
    }
}

impl Default for LineInterpolatePoints {
    fn default() -> Self {
        Self::new()
    }
}

static LINE_INTERPOLATE_POINTS_DOC: OnceLock<Documentation> = OnceLock::new();
static LINE_INTERPOLATE_POINTS_SIGNATURE: std::sync::LazyLock<Signature> =
    std::sync::LazyLock::new(|| {
        Signature::one_of(
            vec![TypeSignature::Any(2), TypeSignature::Any(3)],
            Volatility::Immutable,
        )
    });

impl ScalarUDFImpl for LineInterpolatePoints {
    fn name(&self) -> &str {
        "st_lineinterpolatepoints"
    }

    fn signature(&self) -> &Signature {
        &LINE_INTERPOLATE_POINTS_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            generic_geometry_type(&args.arg_fields[0], self.name())?.to_field("", true),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(interpolate_points_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LINE_INTERPOLATE_POINTS_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns points interpolated along a LineString at a fractional interval. The result is a POINT when it contains at most one point and a MULTIPOINT otherwise.",
                "ST_LineInterpolatePoints(line_string, fraction[, repeat])",
            )
            .with_argument("line_string", "geometry")
            .with_argument("fraction", "float8 in [0, 1]")
            .with_argument("repeat", "boolean default true")
            .build()
        }))
    }
}

fn line_interpolate_point_signature() -> &'static Signature {
    static SIGNATURE: std::sync::LazyLock<Signature> =
        std::sync::LazyLock::new(|| Signature::any(2, Volatility::Immutable));
    &SIGNATURE
}

fn point_return_field(
    args: &ReturnFieldArgs,
    function_name: &str,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = planar_metadata(&args.arg_fields[0], function_name)?;
    if args.arg_fields[0].data_type() == &DataType::Null {
        return Ok(Arc::new(GeometryDataType::new(metadata).to_field("", true)));
    }
    let input_type = GeoArrowType::from_arrow_field(args.arg_fields[0].as_ref())?;
    let field = match input_type {
        GeoArrowType::LineString(typ) => PointType::new(typ.dimension(), metadata)
            .with_coord_type(typ.coord_type())
            .to_field("", true),
        _ => GeometryDataType::new(metadata)
            .with_coord_type(input_type.coord_type().unwrap_or_default())
            .to_field("", true),
    };
    Ok(Arc::new(field))
}

fn interpolate_point_impl(
    args: &ScalarFunctionArgs,
    use_3d_length: bool,
    function_name: &str,
) -> GeoDataFusionResult<ColumnarValue> {
    if has_sql_null_argument(args) {
        return null_geometry_value(&args.return_field, args.number_rows);
    }
    let fraction = args.args[1].cast_to(&DataType::Float64, None)?;
    let arrays = ColumnarValue::values_to_arrays(&[args.args[0].clone(), fraction])?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let fractions = arrays[1].as_primitive::<arrow_array::types::Float64Type>();
    let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;

    let geometry_ref = geo_array.as_ref();
    let rows = downcast_geoarrow_array!(
        geometry_ref,
        interpolate_point_rows,
        fractions,
        use_3d_length
    )?;

    match output_type {
        GeoArrowType::Point(typ) => {
            let result = build_point_array(rows, typ)?;
            Ok(ColumnarValue::Array(result.to_array_ref()))
        }
        _ => {
            let result = build_generic_geometry_array(
                rows,
                generic_geometry_type(&args.return_field, function_name)?,
            )?;
            Ok(ColumnarValue::Array(result.to_array_ref()))
        }
    }
}

fn interpolate_point_rows<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    fractions: &arrow_array::Float64Array,
    use_3d_length: bool,
) -> GeoDataFusionResult<Vec<Option<wkt::Wkt<f64>>>> {
    let mut rows = Vec::with_capacity(array.len());
    for (geometry, fraction) in array.iter().zip(fractions.iter()) {
        let Some(geometry) = geometry else {
            rows.push(None);
            continue;
        };
        let Some(fraction) = fraction else {
            rows.push(None);
            continue;
        };
        finite_fraction(fraction, "2nd")?;
        let geometry = geometry?;
        let GeometryType::LineString(line) = geometry.as_type() else {
            return Err(datafusion_error(
                "line_interpolate_point: 1st arg isn't a line",
            ));
        };
        let line = LineString4::from_trait(line)?;
        let dim = geometry_dimension(&geometry)?;
        let coord = if line.is_empty() {
            None
        } else {
            Some(line.interpolate(fraction, use_3d_length)?)
        };
        rows.push(Some(point_wkt(coord, dim)));
    }
    Ok(rows)
}

fn interpolate_points_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    if has_sql_null_argument(args) {
        return null_geometry_value(&args.return_field, args.number_rows);
    }
    let fraction = args.args[1].cast_to(&DataType::Float64, None)?;
    let mut values = vec![args.args[0].clone(), fraction];
    if let Some(repeat) = args.args.get(2) {
        values.push(repeat.cast_to(&DataType::Boolean, None)?);
    }
    let arrays = ColumnarValue::values_to_arrays(&values)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let fractions = arrays[1].as_primitive::<arrow_array::types::Float64Type>();
    let repeat = arrays.get(2).map(|array| array.as_boolean());

    let geometry_ref = geo_array.as_ref();
    let rows = downcast_geoarrow_array!(geometry_ref, interpolate_points_rows, fractions, repeat)?;
    let result = build_generic_geometry_array(
        rows,
        generic_geometry_type(&args.return_field, "st_lineinterpolatepoints")?,
    )?;
    Ok(ColumnarValue::Array(result.to_array_ref()))
}

fn interpolate_points_rows<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    fractions: &arrow_array::Float64Array,
    repeat: Option<&arrow_array::BooleanArray>,
) -> GeoDataFusionResult<Vec<Option<wkt::Wkt<f64>>>> {
    let mut rows = Vec::with_capacity(array.len());
    for (index, (geometry, fraction)) in array.iter().zip(fractions.iter()).enumerate() {
        let Some(geometry) = geometry else {
            rows.push(None);
            continue;
        };
        let Some(fraction) = fraction else {
            rows.push(None);
            continue;
        };
        let repeat = match repeat {
            Some(array) if array.is_null(index) => {
                rows.push(None);
                continue;
            }
            Some(array) => array.value(index),
            None => true,
        };
        finite_fraction(fraction, "2nd")?;
        let geometry = geometry?;
        let GeometryType::LineString(line) = geometry.as_type() else {
            return Err(datafusion_error(
                "line_interpolate_point: 1st arg isn't a line",
            ));
        };
        let line = LineString4::from_trait(line)?;
        let dim = geometry_dimension(&geometry)?;
        if line.is_empty() {
            rows.push(Some(point_wkt(None, dim)));
            continue;
        }

        let points = if fraction == 0.0 || fraction == 1.0 || !repeat {
            vec![line.interpolate(fraction, false)?]
        } else {
            let count = (1.0 / fraction).floor();
            if !count.is_finite() || count > u32::MAX as f64 {
                return Err(datafusion_error(
                    "line_interpolate_points: fraction produces too many points",
                ));
            }
            let count = count as usize;
            let mut points = Vec::with_capacity(count);
            for point_index in 1..=count {
                let target = (fraction * point_index as f64).min(1.0);
                points.push(line.interpolate(target, false)?);
            }
            points
        };

        let result = if points.len() <= 1 {
            point_wkt(points.into_iter().next(), dim)
        } else {
            multi_point_wkt(points, dim)
        };
        rows.push(Some(result));
    }
    Ok(rows)
}

fn line_substring_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    if has_sql_null_argument(args) {
        return null_geometry_value(&args.return_field, args.number_rows);
    }
    let start = args.args[1].cast_to(&DataType::Float64, None)?;
    let end = args.args[2].cast_to(&DataType::Float64, None)?;
    let arrays = ColumnarValue::values_to_arrays(&[args.args[0].clone(), start, end])?;
    let geometry_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let starts = arrays[1].as_primitive::<arrow_array::types::Float64Type>();
    let ends = arrays[2].as_primitive::<arrow_array::types::Float64Type>();
    let geometry_ref = geometry_array.as_ref();
    let rows = downcast_geoarrow_array!(geometry_ref, line_substring_rows, starts, ends)?;
    let result = build_generic_geometry_array(
        rows,
        generic_geometry_type(&args.return_field, "st_linesubstring")?,
    )?;
    Ok(ColumnarValue::Array(result.to_array_ref()))
}

fn line_substring_rows<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    starts: &arrow_array::Float64Array,
    ends: &arrow_array::Float64Array,
) -> GeoDataFusionResult<Vec<Option<Wkt<f64>>>> {
    let mut rows = Vec::with_capacity(array.len());
    for ((geometry, start), end) in array.iter().zip(starts.iter()).zip(ends.iter()) {
        let Some(geometry) = geometry else {
            rows.push(None);
            continue;
        };
        let (Some(start), Some(end)) = (start, end) else {
            rows.push(None);
            continue;
        };
        finite_fraction(start, "2nd")?;
        finite_fraction(end, "3rd")?;
        if start > end {
            return Err(datafusion_error("2nd arg must be smaller then 3rd arg"));
        }
        let geometry = geometry?;
        let dimension = geometry_dimension(&geometry)?;
        let result = match geometry.as_type() {
            GeometryType::LineString(line) => {
                let line = LineString4::from_trait(line)?;
                if line.is_empty() {
                    None
                } else {
                    Some(substring_line_wkt(&line, start, end)?)
                }
            }
            GeometryType::MultiLineString(multi_line) => {
                substring_multi_line_wkt(multi_line, dimension, start, end)?
            }
            _ => {
                return Err(datafusion_error("line_substring: 1st arg isn't a line"));
            }
        };
        rows.push(result);
    }
    Ok(rows)
}

fn substring_line_wkt(line: &LineString4, start: f64, end: f64) -> GeoDataFusionResult<Wkt<f64>> {
    let coords = line.substring(start, end)?;
    if coords.len() == 1 {
        Ok(point_wkt(Some(coords[0]), line.dim))
    } else {
        Ok(Wkt::LineString(WktLineString::new(
            coords
                .into_iter()
                .map(super::util::Coord4::to_wkt)
                .collect(),
            wkt_dimension(line.dim),
        )))
    }
}

fn substring_multi_line_wkt(
    multi_line: &impl MultiLineStringTrait<T = f64>,
    dimension: Dimension,
    start: f64,
    end: f64,
) -> GeoDataFusionResult<Option<Wkt<f64>>> {
    let lines = multi_line
        .line_strings()
        .map(|line| LineString4::from_trait(&line))
        .collect::<GeoDataFusionResult<Vec<_>>>()?;
    if lines.iter().all(LineString4::is_empty) {
        return Ok(None);
    }

    let total_length: f64 = lines.iter().map(LineString4::length_2d).sum();
    let mut parts = Vec::new();

    if total_length == 0.0 {
        // With no measurable extent, every non-empty member contributes its
        // final segment or vertex.
        for line in &lines {
            if !line.is_empty() {
                push_substring_part(&mut parts, line.substring(0.0, 1.0)?, dimension);
            }
        }
    } else {
        let mut traversed = 0.0;
        for line in &lines {
            if line.is_empty() {
                continue;
            }

            let line_length = line.length_2d();
            let line_start = traversed / total_length;
            let line_end = (traversed + line_length) / total_length;
            traversed += line_length;

            if line_length == 0.0 {
                if line_start >= start && line_start <= end {
                    push_substring_part(&mut parts, line.substring(0.0, 1.0)?, dimension);
                }
                continue;
            }
            if end < line_start || start > line_end {
                continue;
            }

            let local_start = if start <= line_start {
                0.0
            } else if start >= line_end {
                1.0
            } else {
                (start - line_start) / (line_end - line_start)
            };
            let local_end = if end <= line_start {
                0.0
            } else if end >= line_end {
                1.0
            } else {
                (end - line_start) / (line_end - line_start)
            };

            push_substring_part(
                &mut parts,
                line.substring(local_start, local_end)?,
                dimension,
            );
        }
    }

    if parts.iter().any(|part| !matches!(part, Wkt::LineString(_))) {
        Ok(Some(Wkt::GeometryCollection(WktGeometryCollection::new(
            parts,
            wkt_dimension(dimension),
        ))))
    } else {
        Ok(Some(Wkt::MultiLineString(WktMultiLineString::new(
            parts
                .into_iter()
                .filter_map(|part| match part {
                    Wkt::LineString(line) => Some(line),
                    _ => None,
                })
                .collect(),
            wkt_dimension(dimension),
        ))))
    }
}

fn push_substring_part(parts: &mut Vec<Wkt<f64>>, coords: Vec<Coord4>, dimension: Dimension) {
    if coords.len() == 1 {
        parts.push(point_wkt(Some(coords[0]), dimension));
    } else if coords.len() > 1 {
        parts.push(Wkt::LineString(WktLineString::new(
            coords
                .into_iter()
                .map(super::util::Coord4::to_wkt)
                .collect(),
            wkt_dimension(dimension),
        )));
    }
}
