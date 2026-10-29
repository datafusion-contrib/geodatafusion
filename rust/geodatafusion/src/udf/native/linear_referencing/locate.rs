use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::builder::Float64Builder;
use arrow_array::cast::AsArray;
use arrow_array::{Array, Float64Array};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use geo_traits::{GeometryTrait, GeometryType, MultiLineStringTrait, MultiPointTrait};
use geoarrow_array::builder::MultiPolygonBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{
    GeoArrowType, GeometryType as GeometryDataType, MultiPointType, MultiPolygonType,
};
use wkt::Wkt;

use super::util::{
    ClipOrdinate, Coord4, LineString4, build_generic_geometry_array, build_multi_point_array,
    clip_geometry_to_ordinate, datafusion_error, ensure_planar_metadata, fp_equal,
    generic_geometry_type, geometry_dimension, geometry_has_m, geometry_has_z,
    has_sql_null_argument, multi_point_wkt, null_float64_value, planar_metadata, point_coord,
    push_unique,
};
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

static LOCATE_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::any(2, Volatility::Immutable));
static LOCATE_WITH_OFFSET_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::one_of(
        vec![TypeSignature::Any(2), TypeSignature::Any(3)],
        Volatility::Immutable,
    )
});
static LOCATE_BETWEEN_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::one_of(
        vec![TypeSignature::Any(3), TypeSignature::Any(4)],
        Volatility::Immutable,
    )
});

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineLocatePoint;

impl LineLocatePoint {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LineLocatePoint {
    fn default() -> Self {
        Self::new()
    }
}

static LINE_LOCATE_POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineLocatePoint {
    fn name(&self) -> &str {
        "st_linelocatepoint"
    }

    fn signature(&self) -> &Signature {
        &LOCATE_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(arrow_schema::Field::new(
            "",
            DataType::Float64,
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(line_locate_point_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LINE_LOCATE_POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the fractional location in [0, 1] of the closest point on a LineString to a Point, measured by 2D length.",
                "ST_LineLocatePoint(line_string, point)",
            )
            .with_argument("line_string", "geometry")
            .with_argument("point", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct InterpolatePoint;

impl InterpolatePoint {
    pub fn new() -> Self {
        Self
    }
}

impl Default for InterpolatePoint {
    fn default() -> Self {
        Self::new()
    }
}

static INTERPOLATE_POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for InterpolatePoint {
    fn name(&self) -> &str {
        "st_interpolatepoint"
    }

    fn signature(&self) -> &Signature {
        &LOCATE_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(arrow_schema::Field::new(
            "",
            DataType::Float64,
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(interpolate_point_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(INTERPOLATE_POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the interpolated M value of a measured LineString at the location closest to a Point.",
                "ST_InterpolatePoint(line_string_with_measure, point)",
            )
            .with_argument("line_string", "geometry with M")
            .with_argument("point", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LocateAlong;

impl LocateAlong {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LocateAlong {
    fn default() -> Self {
        Self::new()
    }
}

static LOCATE_ALONG_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LocateAlong {
    fn name(&self) -> &str {
        "st_locatealong"
    }

    fn signature(&self) -> &Signature {
        &LOCATE_WITH_OFFSET_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(multi_point_return_field(&args, self.name())?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(locate_along_impl(&args, self.name())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LOCATE_ALONG_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the point or points along a measured linear geometry whose M value equals the requested measure. The optional offset moves line-derived points to the left for positive values and to the right for negative values.",
                "ST_LocateAlong(geom_with_measure, measure[, offset])",
            )
            .with_argument("geom_with_measure", "geometry with M")
            .with_argument("measure", "float8")
            .with_argument("offset", "float8 default 0")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LocateBetween;

impl LocateBetween {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LocateBetween {
    fn default() -> Self {
        Self::new()
    }
}

static LOCATE_BETWEEN_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LocateBetween {
    fn name(&self) -> &str {
        "st_locatebetween"
    }

    fn signature(&self) -> &Signature {
        &LOCATE_BETWEEN_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(locate_between_return_field(&args, self.name())?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(locate_between_impl(&args, ClipOrdinate::M, self.name())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LOCATE_BETWEEN_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the portions of a measured geometry whose M values lie in the inclusive range. Reversed range arguments are normalized. Equal bounds are equivalent to ST_LocateAlong. A nonzero offset for line results requires the geos-3_11 feature.",
                "ST_LocateBetween(geom, measure_start, measure_end[, offset])",
            )
            .with_argument("geom", "geometry with M")
            .with_argument("measure_start", "float8")
            .with_argument("measure_end", "float8")
            .with_argument("offset", "float8 default 0")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LocateBetweenElevations;

impl LocateBetweenElevations {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LocateBetweenElevations {
    fn default() -> Self {
        Self::new()
    }
}

static LOCATE_BETWEEN_ELEVATIONS_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LocateBetweenElevations {
    fn name(&self) -> &str {
        "st_locatebetweenelevations"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(3, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(locate_between_return_field(&args, self.name())?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(locate_between_impl(&args, ClipOrdinate::Z, self.name())?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LOCATE_BETWEEN_ELEVATIONS_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the portions of a geometry whose Z values lie in the inclusive elevation range. Reversed range arguments are normalized.",
                "ST_LocateBetweenElevations(geom, elevation_start, elevation_end)",
            )
            .with_argument("geom", "geometry with Z")
            .with_argument("elevation_start", "float8")
            .with_argument("elevation_end", "float8")
            .build()
        }))
    }
}

fn multi_point_return_field(
    args: &ReturnFieldArgs,
    function_name: &str,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = planar_metadata(&args.arg_fields[0], function_name)?;
    if args.arg_fields[0].data_type() == &DataType::Null {
        return Ok(Arc::new(GeometryDataType::new(metadata).to_field("", true)));
    }
    let input_type = GeoArrowType::from_arrow_field(args.arg_fields[0].as_ref())?;
    let field = match input_type {
        GeoArrowType::Point(typ) => MultiPointType::new(typ.dimension(), metadata)
            .with_coord_type(typ.coord_type())
            .to_field("", true),
        GeoArrowType::MultiPoint(typ) => MultiPointType::new(typ.dimension(), metadata)
            .with_coord_type(typ.coord_type())
            .to_field("", true),
        GeoArrowType::LineString(typ) => MultiPointType::new(typ.dimension(), metadata)
            .with_coord_type(typ.coord_type())
            .to_field("", true),
        GeoArrowType::MultiLineString(typ) => MultiPointType::new(typ.dimension(), metadata)
            .with_coord_type(typ.coord_type())
            .to_field("", true),
        _ => GeometryDataType::new(metadata)
            .with_coord_type(input_type.coord_type().unwrap_or_default())
            .to_field("", true),
    };
    Ok(Arc::new(field))
}

fn locate_between_return_field(
    args: &ReturnFieldArgs,
    function_name: &str,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = planar_metadata(&args.arg_fields[0], function_name)?;
    if args.arg_fields[0].data_type() == &DataType::Null {
        return Ok(Arc::new(GeometryDataType::new(metadata).to_field("", true)));
    }
    let input_type = GeoArrowType::from_arrow_field(args.arg_fields[0].as_ref())?;
    let field = match input_type {
        GeoArrowType::Point(typ) => MultiPointType::new(typ.dimension(), metadata)
            .with_coord_type(typ.coord_type())
            .to_field("", true),
        GeoArrowType::MultiPoint(typ) => MultiPointType::new(typ.dimension(), metadata)
            .with_coord_type(typ.coord_type())
            .to_field("", true),
        GeoArrowType::Polygon(typ) => MultiPolygonType::new(typ.dimension(), metadata)
            .with_coord_type(typ.coord_type())
            .to_field("", true),
        GeoArrowType::MultiPolygon(typ) => MultiPolygonType::new(typ.dimension(), metadata)
            .with_coord_type(typ.coord_type())
            .to_field("", true),
        _ => GeometryDataType::new(metadata)
            .with_coord_type(input_type.coord_type().unwrap_or_default())
            .to_field("", true),
    };
    Ok(Arc::new(field))
}

fn line_locate_point_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    ensure_compatible_metadata(
        &args.arg_fields[0],
        &args.arg_fields[1],
        "st_linelocatepoint",
    )?;
    if has_sql_null_argument(args) {
        return Ok(null_float64_value(args.number_rows));
    }
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let line_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let point_array = geometry_array_from_arrow(&arrays[1], &args.arg_fields[1])?;
    let line_ref = line_array.as_ref();
    let result = downcast_geoarrow_array!(line_ref, line_locate_point_outer, point_array.as_ref())?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn line_locate_point_outer<'a>(
    line_array: &'a impl GeoArrowArrayAccessor<'a>,
    point_array: &dyn GeoArrowArray,
) -> GeoDataFusionResult<Float64Array> {
    let point_ref = point_array;
    downcast_geoarrow_array!(point_ref, line_locate_point_rows, line_array)
}

fn line_locate_point_rows<'a, 'b>(
    point_array: &'a impl GeoArrowArrayAccessor<'a>,
    line_array: &'b impl GeoArrowArrayAccessor<'b>,
) -> GeoDataFusionResult<Float64Array> {
    let mut builder = Float64Builder::with_capacity(line_array.len());
    for (line, point) in line_array.iter().zip(point_array.iter()) {
        let (Some(line), Some(point)) = (line, point) else {
            builder.append_null();
            continue;
        };
        let line = line?;
        let point = point?;
        let GeometryType::LineString(line_string) = line.as_type() else {
            return Err(datafusion_error("line_locate_point: 1st arg isn't a line"));
        };
        let GeometryType::Point(point_geometry) = point.as_type() else {
            return Err(datafusion_error("line_locate_point: 2nd arg isn't a point"));
        };
        let point_coord = point_coord(point_geometry)?
            .ok_or_else(|| datafusion_error("lwpoint_getPoint4d_p called with empty geometry"))?;
        let line = LineString4::from_trait(line_string)?;
        builder.append_value(line.locate_fraction_2d(&point_coord)?);
    }
    Ok(builder.finish())
}

fn interpolate_point_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    ensure_compatible_metadata(
        &args.arg_fields[0],
        &args.arg_fields[1],
        "st_interpolatepoint",
    )?;
    if has_sql_null_argument(args) {
        return Ok(null_float64_value(args.number_rows));
    }
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let line_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let point_array = geometry_array_from_arrow(&arrays[1], &args.arg_fields[1])?;
    let line_ref = line_array.as_ref();
    let result = downcast_geoarrow_array!(line_ref, interpolate_point_outer, point_array.as_ref())?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn interpolate_point_outer<'a>(
    line_array: &'a impl GeoArrowArrayAccessor<'a>,
    point_array: &dyn GeoArrowArray,
) -> GeoDataFusionResult<Float64Array> {
    let point_ref = point_array;
    downcast_geoarrow_array!(point_ref, interpolate_point_rows, line_array)
}

fn interpolate_point_rows<'a, 'b>(
    point_array: &'a impl GeoArrowArrayAccessor<'a>,
    line_array: &'b impl GeoArrowArrayAccessor<'b>,
) -> GeoDataFusionResult<Float64Array> {
    let mut builder = Float64Builder::with_capacity(line_array.len());
    for (line, point) in line_array.iter().zip(point_array.iter()) {
        let (Some(line), Some(point)) = (line, point) else {
            builder.append_null();
            continue;
        };
        let line = line?;
        let point = point?;
        let GeometryType::LineString(line_string) = line.as_type() else {
            return Err(datafusion_error(
                "ST_InterpolatePoint: 1st argument isn't a line",
            ));
        };
        let GeometryType::Point(point_geometry) = point.as_type() else {
            return Err(datafusion_error(
                "ST_InterpolatePoint: 2nd argument isn't a point",
            ));
        };
        if !geometry_has_m(&line) {
            return Err(datafusion_error(
                "ST_InterpolatePoint only accepts geometries that have an M dimension",
            ));
        }
        let point = point_coord(point_geometry)?
            .ok_or_else(|| datafusion_error("Input geometry is empty"))?;
        let line = LineString4::from_trait(line_string)?;
        if line.is_empty() {
            return Err(datafusion_error("Input geometry is empty"));
        }
        let fraction = line.locate_fraction_2d(&point)?;
        let projected = line.interpolate(fraction, false)?;
        let measure = projected
            .m
            .ok_or_else(|| datafusion_error("Input geometry does not have a measure dimension"))?;
        builder.append_value(measure);
    }
    Ok(builder.finish())
}

fn locate_along_impl(
    args: &ScalarFunctionArgs,
    function_name: &str,
) -> GeoDataFusionResult<ColumnarValue> {
    if has_sql_null_argument(args) {
        return finish_geometry_rows(
            vec![None; args.number_rows],
            &args.return_field,
            function_name,
        );
    }
    let measure = args.args[1].cast_to(&DataType::Float64, None)?;
    let mut values = vec![args.args[0].clone(), measure];
    if let Some(offset) = args.args.get(2) {
        values.push(offset.cast_to(&DataType::Float64, None)?);
    }
    let arrays = ColumnarValue::values_to_arrays(&values)?;
    let geometry_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let measures = arrays[1].as_primitive::<arrow_array::types::Float64Type>();
    let offsets = arrays.get(2).map(|array| {
        array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("offset is coerced to Float64")
    });
    let geometry_ref = geometry_array.as_ref();
    let rows = downcast_geoarrow_array!(geometry_ref, locate_along_rows, measures, offsets)?;
    finish_geometry_rows(rows, &args.return_field, function_name)
}

fn locate_along_rows<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    measures: &Float64Array,
    offsets: Option<&Float64Array>,
) -> GeoDataFusionResult<Vec<Option<Wkt<f64>>>> {
    let mut rows = Vec::with_capacity(array.len());
    for (index, (geometry, measure)) in array.iter().zip(measures.iter()).enumerate() {
        let Some(geometry) = geometry else {
            rows.push(None);
            continue;
        };
        let Some(measure) = measure else {
            rows.push(None);
            continue;
        };
        let offset = match offsets {
            Some(offsets) if offsets.is_null(index) => {
                rows.push(None);
                continue;
            }
            Some(offsets) => offsets.value(index),
            None => 0.0,
        };
        let geometry = geometry?;
        rows.push(locate_along_geometry(&geometry, measure, offset)?);
    }
    Ok(rows)
}

fn locate_between_impl(
    args: &ScalarFunctionArgs,
    ordinate: ClipOrdinate,
    function_name: &str,
) -> GeoDataFusionResult<ColumnarValue> {
    if has_sql_null_argument(args) {
        return finish_geometry_rows(
            vec![None; args.number_rows],
            &args.return_field,
            function_name,
        );
    }
    let lower = args.args[1].cast_to(&DataType::Float64, None)?;
    let upper = args.args[2].cast_to(&DataType::Float64, None)?;
    let mut values = vec![args.args[0].clone(), lower, upper];
    if ordinate == ClipOrdinate::M
        && let Some(offset) = args.args.get(3)
    {
        values.push(offset.cast_to(&DataType::Float64, None)?);
    }
    let arrays = ColumnarValue::values_to_arrays(&values)?;
    let geometry_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let lower = arrays[1].as_primitive::<arrow_array::types::Float64Type>();
    let upper = arrays[2].as_primitive::<arrow_array::types::Float64Type>();
    let offset = arrays.get(3).map(|array| {
        array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("offset is coerced to Float64")
    });
    let geometry_ref = geometry_array.as_ref();
    let rows = downcast_geoarrow_array!(
        geometry_ref,
        locate_between_rows,
        lower,
        upper,
        offset,
        ordinate
    )?;
    finish_geometry_rows(rows, &args.return_field, function_name)
}

fn locate_between_rows<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    lower: &Float64Array,
    upper: &Float64Array,
    offset: Option<&Float64Array>,
    ordinate: ClipOrdinate,
) -> GeoDataFusionResult<Vec<Option<Wkt<f64>>>> {
    let mut rows = Vec::with_capacity(array.len());
    for (index, ((geometry, lower), upper)) in
        array.iter().zip(lower.iter()).zip(upper.iter()).enumerate()
    {
        let Some(geometry) = geometry else {
            rows.push(None);
            continue;
        };
        let (Some(lower), Some(upper)) = (lower, upper) else {
            rows.push(None);
            continue;
        };
        let offset = match offset {
            Some(offset) if offset.is_null(index) => {
                rows.push(None);
                continue;
            }
            Some(offset) => offset.value(index),
            None => 0.0,
        };
        if !lower.is_finite() || !upper.is_finite() || !offset.is_finite() {
            return Err(datafusion_error(
                "locate range and offset arguments must be finite",
            ));
        }

        let geometry = geometry?;
        match ordinate {
            ClipOrdinate::M => {
                if !geometry_has_m(&geometry) {
                    return Err(datafusion_error(
                        "This function only accepts geometries that have an M dimension.",
                    ));
                }
                if lower == upper {
                    rows.push(locate_along_geometry(&geometry, lower, offset)?);
                } else {
                    let clipped = clip_geometry_to_ordinate(&geometry, ordinate, lower, upper)?;
                    rows.push(Some(apply_clip_offset(clipped, offset)?));
                }
            }
            ClipOrdinate::Z => {
                if !geometry_has_z(&geometry) {
                    return Err(datafusion_error(
                        "This function only accepts geometries with Z dimensions.",
                    ));
                }
                rows.push(Some(clip_geometry_to_ordinate(
                    &geometry, ordinate, lower, upper,
                )?));
            }
        }
    }
    Ok(rows)
}

fn locate_along_geometry(
    geometry: &impl GeometryTrait<T = f64>,
    measure: f64,
    offset: f64,
) -> GeoDataFusionResult<Option<Wkt<f64>>> {
    if !geometry_has_m(geometry) {
        return Err(datafusion_error(
            "Input geometry does not have a measure dimension",
        ));
    }
    let dimension = geometry_dimension(geometry)?;
    let points = match geometry.as_type() {
        GeometryType::Point(point) => {
            let point = point_coord(point)?
                .ok_or_else(|| datafusion_error("lwpoint_get_m called with empty geometry"))?;
            let point_measure = point.m.ok_or_else(|| {
                datafusion_error("Input geometry does not have a measure dimension")
            })?;
            if fp_equal(measure, point_measure) {
                vec![point]
            } else {
                Vec::new()
            }
        }
        GeometryType::MultiPoint(multi_point) => {
            let mut points = Vec::new();
            for point in multi_point.points() {
                let point = point_coord(&point)?
                    .ok_or_else(|| datafusion_error("lwpoint_get_m called with empty geometry"))?;
                let point_measure = point.m.ok_or_else(|| {
                    datafusion_error("Input geometry does not have a measure dimension")
                })?;
                if fp_equal(measure, point_measure) {
                    points.push(point);
                }
            }
            points
        }
        GeometryType::LineString(line) => {
            let line = LineString4::from_trait(line)?;
            locate_along_line(&line, measure, offset)
        }
        GeometryType::MultiLineString(multi_line) => {
            let lines = multi_line.line_strings().collect::<Vec<_>>();
            if lines.is_empty() {
                return Ok(None);
            }
            let mut points = Vec::new();
            for line in lines {
                let line = LineString4::from_trait(&line)?;
                points.extend(locate_along_line(&line, measure, offset));
            }
            points
        }
        _ => {
            return Err(datafusion_error(format!(
                "Only linear geometries are supported, {} provided.",
                super::util::geometry_name(geometry)
            )));
        }
    };
    Ok(Some(multi_point_wkt(points, dimension)))
}

fn locate_along_line(line: &LineString4, measure: f64, offset: f64) -> Vec<Coord4> {
    if line.coords.len() < 2 {
        return Vec::new();
    }
    let mut points = Vec::new();
    for pair in line.coords.windows(2) {
        let first = pair[0];
        let second = pair[1];
        let first_measure = first.m.unwrap_or(0.0);
        let second_measure = second.m.unwrap_or(0.0);
        if measure < first_measure.min(second_measure)
            || measure > first_measure.max(second_measure)
        {
            continue;
        }
        let fraction = if first_measure == second_measure {
            if first.same_all(&second) {
                push_unique(&mut points, first);
                continue;
            }
            0.5
        } else {
            (measure - first_measure) / (second_measure - first_measure)
        };
        let mut point = first.interpolate(&second, fraction).with_m(measure);
        if offset != 0.0 {
            let theta = (second.y - first.y).atan2(second.x - first.x);
            point.x -= theta.sin() * offset;
            point.y += theta.cos() * offset;
        }
        let duplicate = points
            .last()
            .is_some_and(|previous: &Coord4| previous.same_all(&point));
        if !duplicate {
            points.push(point);
        }
    }
    points
}

fn apply_clip_offset(geometry: Wkt<f64>, offset: f64) -> GeoDataFusionResult<Wkt<f64>> {
    if offset == 0.0 {
        return Ok(geometry);
    }
    if wkt_contains_polygon(&geometry) {
        return Err(datafusion_error(
            "lwgeom_clip_to_ordinate_range found an unexpected type (Polygon) in the offset routine",
        ));
    }
    if !wkt_contains_line(&geometry) {
        return Ok(Wkt::MultiLineString(wkt::types::MultiLineString::empty(
            wkt::types::Dimension::XY,
        )));
    }
    offset_clipped_lines(geometry, offset)
}

fn wkt_contains_line(geometry: &Wkt<f64>) -> bool {
    match geometry {
        Wkt::LineString(_) | Wkt::MultiLineString(_) => true,
        Wkt::GeometryCollection(collection) => {
            collection.geometries().iter().any(wkt_contains_line)
        }
        _ => false,
    }
}

fn wkt_contains_polygon(geometry: &Wkt<f64>) -> bool {
    match geometry {
        Wkt::Polygon(_) | Wkt::MultiPolygon(_) => true,
        Wkt::GeometryCollection(collection) => {
            collection.geometries().iter().any(wkt_contains_polygon)
        }
        _ => false,
    }
}

#[cfg(feature = "geos-3_11")]
fn offset_clipped_lines(geometry: Wkt<f64>, offset: f64) -> GeoDataFusionResult<Wkt<f64>> {
    let mut lines = Vec::new();
    append_offset_lines(&mut lines, geometry, offset)?;
    Ok(Wkt::MultiLineString(wkt::types::MultiLineString::new(
        lines,
        wkt::types::Dimension::XY,
    )))
}

#[cfg(feature = "geos-3_11")]
fn append_offset_lines(
    output: &mut Vec<wkt::types::LineString<f64>>,
    geometry: Wkt<f64>,
    offset: f64,
) -> GeoDataFusionResult<()> {
    use std::str::FromStr;

    use geos::{Geom, Geometry, JoinStyle};

    match geometry {
        Wkt::LineString(line) => {
            let geometry = Geometry::new_from_wkt(&line.to_string()).map_err(geos_error)?;
            let offset_geometry = geometry
                .offset_curve(offset, 8, JoinStyle::Round, 5.0)
                .map_err(geos_error)?;
            let offset_wkt = Wkt::<f64>::from_str(&offset_geometry.to_wkt().map_err(geos_error)?)
                .map_err(|error| {
                datafusion_error(format!("invalid GEOS offset WKT: {error}"))
            })?;
            collect_wkt_lines(output, offset_wkt)?;
        }
        Wkt::MultiLineString(multi_line) => {
            for line in multi_line.line_strings().iter().cloned() {
                append_offset_lines(output, Wkt::LineString(line), offset)?;
            }
        }
        Wkt::GeometryCollection(collection) => {
            for geometry in collection.geometries().iter().cloned() {
                append_offset_lines(output, geometry, offset)?;
            }
        }
        Wkt::Point(_) | Wkt::MultiPoint(_) => {}
        other => {
            return Err(datafusion_error(format!(
                "cannot offset clipped geometry type {:?}",
                super::util::wkt_type_name(&other)
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "geos-3_11")]
fn collect_wkt_lines(
    output: &mut Vec<wkt::types::LineString<f64>>,
    geometry: Wkt<f64>,
) -> GeoDataFusionResult<()> {
    match geometry {
        Wkt::LineString(line) => output.push(line),
        Wkt::MultiLineString(multi_line) => output.extend(multi_line.into_inner().0),
        Wkt::GeometryCollection(collection) => {
            for geometry in collection.into_inner().0 {
                collect_wkt_lines(output, geometry)?;
            }
        }
        other => {
            return Err(datafusion_error(format!(
                "GEOS offset curve returned unexpected {}",
                super::util::wkt_type_name(&other)
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "geos-3_11")]
fn geos_error(error: geos::Error) -> crate::error::GeoDataFusionError {
    crate::error::GeoDataFusionError::Geos(error)
}

#[cfg(not(feature = "geos-3_11"))]
fn offset_clipped_lines(_geometry: Wkt<f64>, _offset: f64) -> GeoDataFusionResult<Wkt<f64>> {
    Err(datafusion_error(
        "nonzero ST_LocateBetween offsets require the geos-3_11 feature",
    ))
}

fn finish_geometry_rows(
    rows: Vec<Option<Wkt<f64>>>,
    return_field: &FieldRef,
    function_name: &str,
) -> GeoDataFusionResult<ColumnarValue> {
    let output_type = GeoArrowType::from_arrow_field(return_field.as_ref())?;
    match output_type {
        GeoArrowType::MultiPoint(typ) => {
            let result = build_multi_point_array(rows, typ)?;
            Ok(ColumnarValue::Array(result.to_array_ref()))
        }
        GeoArrowType::MultiPolygon(typ) => {
            let mut builder = MultiPolygonBuilder::new(typ);
            for row in rows {
                builder.push_geometry(row.as_ref())?;
            }
            Ok(ColumnarValue::Array(builder.finish().to_array_ref()))
        }
        _ => {
            let result = build_generic_geometry_array(
                rows,
                generic_geometry_type(return_field, function_name)?,
            )?;
            Ok(ColumnarValue::Array(result.to_array_ref()))
        }
    }
}

fn ensure_compatible_metadata(
    left: &FieldRef,
    right: &FieldRef,
    function_name: &str,
) -> GeoDataFusionResult<()> {
    ensure_planar_metadata([left.as_ref(), right.as_ref()], function_name)?;
    Ok(())
}
