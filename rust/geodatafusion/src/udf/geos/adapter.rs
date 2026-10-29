use std::borrow::Cow;
use std::ffi::CStr;
use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, BinaryArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::GenericWkbArray;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::{CoordType, Edges, GeoArrowType, GeometryType, Metadata};
use geos::{CoordDimensions, Geom, Geometry, WKBWriter, sys};
use wkb::reader::{Dimension, Wkb};

use super::m_dimension;
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    StrictGeometryBuilder, expand_rect_to_geometry, field_metadata, geometry_array_from_arrow,
    merge_metadata,
};

pub(super) fn signature() -> Signature {
    Signature::user_defined(Volatility::Immutable)
}

pub(super) fn check_arg_count(arg_types: &[DataType], allowed: &[usize], name: &str) -> Result<()> {
    if allowed.contains(&arg_types.len()) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} expects {} arguments, got {}",
            allowed
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(" or "),
            arg_types.len()
        )))
    }
}

pub(super) fn check_geometry_type(data_type: &DataType, position: usize, name: &str) -> Result<()> {
    if matches!(data_type, DataType::Null) || any_geometry_type().contains(data_type) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} argument {} must be a geometry, got {data_type}",
            position + 1
        )))
    }
}

pub(super) fn coerce_float64(
    data_type: &DataType,
    position: usize,
    name: &str,
) -> Result<DataType> {
    if matches!(data_type, DataType::Null) || data_type.is_numeric() {
        Ok(DataType::Float64)
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} argument {} must be numeric, got {data_type}",
            position + 1
        )))
    }
}

pub(super) fn coerce_int64(data_type: &DataType, position: usize, name: &str) -> Result<DataType> {
    if matches!(data_type, DataType::Null) || data_type.is_integer() {
        Ok(DataType::Int64)
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} argument {} must be an integer, got {data_type}",
            position + 1
        )))
    }
}

pub(super) fn coerce_text(data_type: &DataType, position: usize, name: &str) -> Result<DataType> {
    match data_type {
        DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(DataType::Utf8)
        }
        _ => Err(DataFusionError::Plan(format!(
            "{name} argument {} must be text, got {data_type}",
            position + 1
        ))),
    }
}

pub(super) fn coerce_buffer_style(data_type: &DataType, name: &str) -> Result<DataType> {
    match data_type {
        DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(DataType::Utf8)
        }
        data_type if data_type.is_integer() => Ok(DataType::Int64),
        _ => Err(DataFusionError::Plan(format!(
            "{name} third argument must be buffer style text or an integer quad-segment count, got {data_type}"
        ))),
    }
}

pub(super) fn geometry_return_field(
    args: &ReturnFieldArgs,
    geometry_positions: &[usize],
    name: &str,
) -> Result<FieldRef> {
    geometry_return_field_with_coord_type(args, geometry_positions, name, None)
}

pub(super) fn geometry_return_field_with_coord_type(
    args: &ReturnFieldArgs,
    geometry_positions: &[usize],
    name: &str,
    coord_type_override: Option<CoordType>,
) -> Result<FieldRef> {
    let mut coord_type = coord_type_override;
    let mut fields = Vec::with_capacity(geometry_positions.len());
    for &position in geometry_positions {
        if metadata_argument_skippable(args, position) {
            continue;
        }

        let field: &Field = args.arg_fields[position].as_ref();
        let geo_type = GeoArrowType::from_arrow_field(field)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        coord_type = coord_type.or_else(|| geo_type.coord_type());
        fields.push(field);
    }

    let metadata = merge_metadata(fields, name)?;
    if metadata.edges().is_some_and(is_non_planar_edge) {
        return Err(DataFusionError::Plan(format!(
            "{name} supports only planar edge metadata"
        )));
    }

    let metadata = Metadata::new(metadata.crs().clone(), None);
    let output_type =
        GeometryType::new(Arc::new(metadata)).with_coord_type(coord_type.unwrap_or_default());
    Ok(Arc::new(output_type.to_field("", true)))
}

/// Build a generic geometry type from one input field.
///
/// This is shared by aggregate and window coverage functions, which do not
/// receive `ReturnFieldArgs` and must derive their output metadata directly
/// from the field passed by DataFusion.
pub(super) fn geometry_type_from_field(field: &Field, name: &str) -> Result<GeometryType> {
    if field.data_type() == &DataType::Null || field.extension_type_name().is_none() {
        return Ok(GeometryType::new(Arc::new(Metadata::default())));
    }

    let geo_type = GeoArrowType::from_arrow_field(field)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let metadata = field_metadata(field)?;
    if metadata.edges().is_some_and(is_non_planar_edge) {
        return Err(DataFusionError::Plan(format!(
            "{name} supports only planar edge metadata"
        )));
    }

    let metadata = Metadata::new(metadata.crs().clone(), None);
    Ok(GeometryType::new(Arc::new(metadata))
        .with_coord_type(geo_type.coord_type().unwrap_or_default()))
}

fn metadata_argument_skippable(args: &ReturnFieldArgs, position: usize) -> bool {
    matches!(args.arg_fields[position].data_type(), DataType::Null)
        || matches!(
            args.scalar_arguments.get(position),
            Some(Some(ScalarValue::Null))
        )
}

fn is_non_planar_edge(edges: Edges) -> bool {
    // geoarrow-schema 0.9.0 has no Planar variant. None is the planar
    // representation, so every present Edges value is non-planar.
    match edges {
        Edges::Andoyer | Edges::Karney | Edges::Spherical | Edges::Thomas | Edges::Vincenty => true,
    }
}

pub(super) fn float64_value(
    arrays: &[ArrayRef],
    position: usize,
    row: usize,
    name: &str,
) -> GeoDataFusionResult<Option<f64>> {
    let array = arrays.get(position).ok_or_else(|| {
        DataFusionError::Internal(format!("{name} is missing argument {}", position + 1))
    })?;
    if !matches!(array.data_type(), DataType::Float64) {
        return Err(DataFusionError::Internal(format!(
            "{name} argument {} was not coerced to Float64",
            position + 1
        ))
        .into());
    }
    let values = array.as_primitive::<Float64Type>();
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

pub(super) fn boolean_value(
    arrays: &[ArrayRef],
    position: usize,
    row: usize,
    name: &str,
) -> GeoDataFusionResult<Option<bool>> {
    let array = arrays.get(position).ok_or_else(|| {
        DataFusionError::Internal(format!("{name} is missing argument {}", position + 1))
    })?;
    if !matches!(array.data_type(), DataType::Boolean) {
        return Err(DataFusionError::Internal(format!(
            "{name} argument {} was not coerced to Boolean",
            position + 1
        ))
        .into());
    }
    let values = array.as_boolean();
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

pub(super) fn int64_value(
    arrays: &[ArrayRef],
    position: usize,
    row: usize,
    name: &str,
) -> GeoDataFusionResult<Option<i64>> {
    let array = arrays.get(position).ok_or_else(|| {
        DataFusionError::Internal(format!("{name} is missing argument {}", position + 1))
    })?;
    if !matches!(array.data_type(), DataType::Int64) {
        return Err(DataFusionError::Internal(format!(
            "{name} argument {} was not coerced to Int64",
            position + 1
        ))
        .into());
    }
    let values = array.as_primitive::<Int64Type>();
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

pub(super) fn string_value<'a>(
    arrays: &'a [ArrayRef],
    position: usize,
    row: usize,
    name: &str,
) -> GeoDataFusionResult<Option<&'a str>> {
    let array = arrays.get(position).ok_or_else(|| {
        DataFusionError::Internal(format!("{name} is missing argument {}", position + 1))
    })?;
    if !matches!(array.data_type(), DataType::Utf8) {
        return Err(DataFusionError::Internal(format!(
            "{name} argument {} was not coerced to Utf8",
            position + 1
        ))
        .into());
    }
    let values = array.as_string::<i32>();
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

pub(crate) struct GeosWkbWriter {
    writer: WKBWriter,
}

impl GeosWkbWriter {
    pub(crate) fn new() -> GeoDataFusionResult<Self> {
        Ok(Self {
            writer: WKBWriter::new()?,
        })
    }

    pub(crate) fn write<G: Geom>(&mut self, geometry: &G) -> GeoDataFusionResult<Vec<u8>> {
        let dimension = geometry.get_coordinate_dimension()?;
        self.write_with_dimension(geometry, dimension)
    }

    pub(super) fn write_with_dimension<G: Geom>(
        &mut self,
        geometry: &G,
        dimension: CoordDimensions,
    ) -> GeoDataFusionResult<Vec<u8>> {
        self.writer.set_output_dimension(dimension);
        Ok(self.writer.write_wkb(geometry)?)
    }
}

pub(super) fn unary_geos<F>(
    args: &ScalarFunctionArgs,
    operation: F,
) -> GeoDataFusionResult<ColumnarValue>
where
    F: FnMut(
        usize,
        &[ArrayRef],
        &[u8],
        &Geometry,
        &mut GeosWkbWriter,
    ) -> GeoDataFusionResult<Option<Vec<u8>>>,
{
    unary_geos_with_nullable_arguments(args, &[], operation)
}

/// Run a unary GEOS operation while allowing selected trailing arguments to
/// carry SQL `NULL` as a meaningful value for the operation.
pub(super) fn unary_geos_with_nullable_arguments<F>(
    args: &ScalarFunctionArgs,
    nullable_arguments: &[usize],
    operation: F,
) -> GeoDataFusionResult<ColumnarValue>
where
    F: FnMut(
        usize,
        &[ArrayRef],
        &[u8],
        &Geometry,
        &mut GeosWkbWriter,
    ) -> GeoDataFusionResult<Option<Vec<u8>>>,
{
    let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
    let values = unary_geos_map(args, nullable_arguments, operation)?;
    finish_geos_result(values.into_iter().collect(), output_type)
}

/// Applies a unary GEOS operation and returns an arbitrary scalar value per
/// row. WKB preparation, M normalization, and NULL handling are shared with
/// the geometry-returning unary path.
pub(super) fn unary_geos_values<T, F>(
    args: &ScalarFunctionArgs,
    mut operation: F,
) -> GeoDataFusionResult<Vec<Option<T>>>
where
    F: FnMut(usize, &[ArrayRef], &[u8], &Geometry) -> GeoDataFusionResult<Option<T>>,
{
    unary_geos_map(args, &[], |row, arrays, original, geometry, _writer| {
        operation(row, arrays, original, geometry)
    })
}

fn unary_geos_map<T, F>(
    args: &ScalarFunctionArgs,
    nullable_arguments: &[usize],
    mut operation: F,
) -> GeoDataFusionResult<Vec<Option<T>>>
where
    F: FnMut(
        usize,
        &[ArrayRef],
        &[u8],
        &Geometry,
        &mut GeosWkbWriter,
    ) -> GeoDataFusionResult<Option<T>>,
{
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays.first().map_or(0, |array| array.len());

    if matches!(args.arg_fields[0].data_type(), DataType::Null) {
        return Ok(std::iter::repeat_with(|| None).take(row_count).collect());
    }

    let geo_array = geometry_array_from_arrow(arrays[0].as_ref(), args.arg_fields[0].as_ref())?;
    let wkb_array = to_geos_wkb(geo_array.as_ref())?;
    let mut output = Vec::with_capacity(row_count);
    let mut writer = GeosWkbWriter::new()?;

    for index in 0..row_count {
        if wkb_array.inner().is_null(index)
            || trailing_args_have_null_except(&arrays, 1, index, nullable_arguments)
        {
            output.push(None);
            continue;
        }

        let wkb = wkb_array.inner().value(index);
        let prepared = prepare_geos_wkb(wkb)?;
        let geometry = Geometry::new_from_wkb(prepared.as_ref())?;
        output.push(operation(index, &arrays, wkb, &geometry, &mut writer)?);
    }

    Ok(output)
}

pub(super) fn binary_geos<F>(
    args: &ScalarFunctionArgs,
    mut operation: F,
) -> GeoDataFusionResult<ColumnarValue>
where
    F: FnMut(
        usize,
        &[ArrayRef],
        &[u8],
        &Geometry,
        &[u8],
        &Geometry,
        &mut GeosWkbWriter,
    ) -> GeoDataFusionResult<Option<Vec<u8>>>,
{
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
    let row_count = arrays.first().map_or(0, |array| array.len());

    if matches!(args.arg_fields[0].data_type(), DataType::Null)
        || matches!(args.arg_fields[1].data_type(), DataType::Null)
    {
        return finish_geos_result(
            vec![None::<Vec<u8>>; row_count].into_iter().collect(),
            output_type,
        );
    }

    let left = geometry_array_from_arrow(arrays[0].as_ref(), args.arg_fields[0].as_ref())?;
    let right = geometry_array_from_arrow(arrays[1].as_ref(), args.arg_fields[1].as_ref())?;
    let left_wkb = to_geos_wkb(left.as_ref())?;
    let right_wkb = to_geos_wkb(right.as_ref())?;

    if left_wkb.inner().len() != right_wkb.inner().len() {
        return Err(DataFusionError::Internal(format!(
            "GEOS binary adapter produced mismatched input lengths: {} != {}",
            left_wkb.inner().len(),
            right_wkb.inner().len()
        ))
        .into());
    }

    let mut output = Vec::with_capacity(row_count);
    let mut writer = GeosWkbWriter::new()?;
    for index in 0..row_count {
        if left_wkb.inner().is_null(index)
            || right_wkb.inner().is_null(index)
            || trailing_args_have_null(&arrays, 2, index)
        {
            output.push(None);
            continue;
        }

        let left_bytes = left_wkb.inner().value(index);
        let right_bytes = right_wkb.inner().value(index);
        let prepared_left = prepare_geos_wkb(left_bytes)?;
        let prepared_right = prepare_geos_wkb(right_bytes)?;
        let left_geometry = Geometry::new_from_wkb(prepared_left.as_ref())?;
        let right_geometry = Geometry::new_from_wkb(prepared_right.as_ref())?;
        output.push(operation(
            index,
            &arrays,
            left_bytes,
            &left_geometry,
            right_bytes,
            &right_geometry,
            &mut writer,
        )?);
    }

    finish_geos_result(output.into_iter().collect(), output_type)
}

pub(super) fn binary_geos_values<T, F>(
    args: &ScalarFunctionArgs,
    mut operation: F,
) -> GeoDataFusionResult<Vec<Option<T>>>
where
    F: FnMut(
        usize,
        &[ArrayRef],
        &[u8],
        &Geometry,
        &[u8],
        &Geometry,
    ) -> GeoDataFusionResult<Option<T>>,
{
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if matches!(args.arg_fields[0].data_type(), DataType::Null)
        || matches!(args.arg_fields[1].data_type(), DataType::Null)
    {
        return Ok(std::iter::repeat_with(|| None)
            .take(args.number_rows)
            .collect());
    }

    let left = geometry_array_from_arrow(arrays[0].as_ref(), args.arg_fields[0].as_ref())?;
    let right = geometry_array_from_arrow(arrays[1].as_ref(), args.arg_fields[1].as_ref())?;
    let left_wkb = to_geos_wkb(left.as_ref())?;
    let right_wkb = to_geos_wkb(right.as_ref())?;

    if left_wkb.inner().len() != right_wkb.inner().len() {
        return Err(DataFusionError::Internal(format!(
            "GEOS binary adapter produced mismatched input lengths: {} != {}",
            left_wkb.inner().len(),
            right_wkb.inner().len()
        ))
        .into());
    }

    let mut output = Vec::with_capacity(args.number_rows);
    for index in 0..args.number_rows {
        if left_wkb.inner().is_null(index)
            || right_wkb.inner().is_null(index)
            || trailing_args_have_null(&arrays, 2, index)
        {
            output.push(None);
            continue;
        }

        let left_bytes = prepare_geos_wkb(left_wkb.inner().value(index))?;
        let right_bytes = prepare_geos_wkb(right_wkb.inner().value(index))?;
        let left_geometry = Geometry::new_from_wkb(left_bytes.as_ref())?;
        let right_geometry = Geometry::new_from_wkb(right_bytes.as_ref())?;
        output.push(operation(
            index,
            &arrays,
            left_bytes.as_ref(),
            &left_geometry,
            right_bytes.as_ref(),
            &right_geometry,
        )?);
    }

    Ok(output)
}

/// Computes a DE-9IM matrix with an explicit boundary node rule.
///
/// Boundary node rules 3 (`MultivalentEndPoint`) and 4 (`MonovalentEndPoint`)
/// depend on endpoint valence counting. GEOS < 3.13 used the legacy relate
/// engine, which produced non-conforming matrices for both rules; GEOS 3.13
/// introduced RelateNG and implements them correctly. The wrapper rejects
/// rules 3 and 4 on older runtimes with an explicit version error, while
/// rules 1 and 2 remain fully supported everywhere. PostGIS built against
/// an affected runtime returns the legacy matrix; this implementation
/// rejects that result rather than exposing a known non-conforming one.
pub(super) fn relate_boundary_node_rule(
    left_wkb: &[u8],
    right_wkb: &[u8],
    rule: i32,
) -> GeoDataFusionResult<String> {
    if matches!(rule, 3 | 4) {
        let version = linked_geos_version()?;
        if !supports_boundary_node_rules_3_and_4(version)? {
            return Err(DataFusionError::Execution(format!(
                "ST_Relate boundary node rule {rule} requires GEOS >= 3.13; linked GEOS is {version}"
            ))
            .into());
        }
    }

    // This is the one GEOS C API used by the relation family that the safe
    // `geos` crate does not expose. Use an owned context so it stays reentrant.
    unsafe {
        let context = sys::GEOS_init_r();
        if context.is_null() {
            return Err(DataFusionError::Execution(
                "GEOSRelate: failed to initialize a GEOS context".to_string(),
            )
            .into());
        }

        let reader = sys::GEOSWKBReader_create_r(context);
        if reader.is_null() {
            sys::GEOS_finish_r(context);
            return Err(DataFusionError::Execution(
                "GEOSRelate: failed to create a WKB reader".to_string(),
            )
            .into());
        }

        let left = sys::GEOSWKBReader_read_r(context, reader, left_wkb.as_ptr(), left_wkb.len());
        let right = sys::GEOSWKBReader_read_r(context, reader, right_wkb.as_ptr(), right_wkb.len());
        sys::GEOSWKBReader_destroy_r(context, reader);

        if left.is_null() || right.is_null() {
            destroy_geometry(context, left);
            destroy_geometry(context, right);
            sys::GEOS_finish_r(context);
            return Err(DataFusionError::Execution(
                "GEOSRelate: failed to decode input geometry".to_string(),
            )
            .into());
        }

        let result = sys::GEOSRelateBoundaryNodeRule_r(context, left, right, rule);
        if result.is_null() {
            destroy_geometry(context, left);
            destroy_geometry(context, right);
            sys::GEOS_finish_r(context);
            return Err(DataFusionError::Execution(
                "GEOSRelate: failed to compute an intersection matrix".to_string(),
            )
            .into());
        }

        let matrix = CStr::from_ptr(result).to_string_lossy().into_owned();
        sys::GEOSFree_r(context, result.cast());
        destroy_geometry(context, left);
        destroy_geometry(context, right);
        sys::GEOS_finish_r(context);
        Ok(matrix)
    }
}

fn linked_geos_version() -> GeoDataFusionResult<&'static str> {
    static VERSION: OnceLock<Result<String, String>> = OnceLock::new();
    match VERSION.get_or_init(|| geos::version().map_err(|error| error.to_string())) {
        Ok(version) => Ok(version.as_str()),
        Err(error) => Err(DataFusionError::Execution(format!(
            "ST_Relate: failed to read the linked GEOS version: {error}"
        ))
        .into()),
    }
}

fn supports_boundary_node_rules_3_and_4(version: &str) -> GeoDataFusionResult<bool> {
    let mut parts = version.split('.');
    let major = parts.next().and_then(|part| part.parse::<u32>().ok());
    let minor = parts.next().and_then(|part| part.parse::<u32>().ok());
    let (Some(major), Some(minor)) = (major, minor) else {
        return Err(DataFusionError::Execution(format!(
            "ST_Relate: failed to parse the linked GEOS version `{version}`"
        ))
        .into());
    };
    Ok(major > 3 || (major == 3 && minor >= 13))
}

unsafe fn destroy_geometry(context: sys::GEOSContextHandle_t, geometry: *mut sys::GEOSGeometry) {
    if !geometry.is_null() {
        unsafe { sys::GEOSGeom_destroy_r(context, geometry) };
    }
}

fn trailing_args_have_null(arrays: &[ArrayRef], start: usize, row: usize) -> bool {
    trailing_args_have_null_except(arrays, start, row, &[])
}

fn trailing_args_have_null_except(
    arrays: &[ArrayRef],
    start: usize,
    row: usize,
    nullable_arguments: &[usize],
) -> bool {
    (start..arrays.len())
        .any(|position| !nullable_arguments.contains(&position) && arrays[position].is_null(row))
}

pub(crate) fn prepare_geos_wkb(wkb: &[u8]) -> GeoDataFusionResult<Cow<'_, [u8]>> {
    let geometry = Wkb::try_new(wkb).map_err(|error| {
        DataFusionError::Execution(format!("GEOS input WKB is invalid: {error}"))
    })?;
    if matches!(geometry.dimension(), Dimension::Xym | Dimension::Xyzm) {
        Ok(Cow::Owned(m_dimension::strip_m(wkb)?))
    } else {
        Ok(Cow::Borrowed(wkb))
    }
}

/// Coordinate dimension of an input WKB after M is stripped at the GEOS boundary.
///
/// Empty GEOS geometries do not reliably report the input dimension on every
/// supported runtime, so callers that must preserve an empty result's
/// dimension read it from the WKB instead of the GEOS object.
pub(super) fn wkb_coord_dimensions(wkb: &[u8]) -> GeoDataFusionResult<CoordDimensions> {
    let geometry = Wkb::try_new(wkb).map_err(|error| {
        DataFusionError::Execution(format!("GEOS input WKB is invalid: {error}"))
    })?;
    Ok(match geometry.dimension() {
        Dimension::Xy | Dimension::Xym => CoordDimensions::TwoD,
        Dimension::Xyz | Dimension::Xyzm => CoordDimensions::ThreeD,
    })
}

fn to_geos_wkb(array: &dyn GeoArrowArray) -> GeoDataFusionResult<GenericWkbArray<i32>> {
    match expand_rect_to_geometry(array)? {
        Some(expanded) => Ok(to_wkb::<i32>(&expanded)?),
        None => Ok(to_wkb::<i32>(array)?),
    }
}

pub(super) fn finish_geos_result(
    values: BinaryArray,
    output_type: GeoArrowType,
) -> GeoDataFusionResult<ColumnarValue> {
    Ok(ColumnarValue::Array(geos_values_to_array(
        values,
        output_type,
    )?))
}

pub(super) fn geos_values_to_array(
    values: BinaryArray,
    output_type: GeoArrowType,
) -> GeoDataFusionResult<ArrayRef> {
    let GeoArrowType::Geometry(output_type) = output_type else {
        return Err(DataFusionError::Internal(
            "GEOS result field is not a generic Geometry".to_string(),
        )
        .into());
    };

    let mut builder = StrictGeometryBuilder::new(output_type);
    for value in values.iter() {
        match value {
            Some(value) => {
                let geometry = Wkb::try_new(value).map_err(|error| {
                    DataFusionError::Execution(format!("GEOS result WKB is invalid: {error}"))
                })?;
                builder.push_geometry(Some(&geometry))?;
            }
            None => {
                builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
            }
        }
    }
    Ok(builder.finish().into_array_ref())
}

/// Little-endian 2D WKB for an empty geometry of the given type.
///
/// PostGIS reports 2D typed empties for operations where GEOS may return a
/// generic empty collection, and drops Z/M flags on empty results.
pub(super) fn empty_2d_wkb(geometry_type: geos::GeometryTypes) -> Option<&'static [u8]> {
    use geos::GeometryTypes::*;
    Some(match geometry_type {
        Point => &[
            1, 1, 0, 0, 0, // little-endian POINT
            0, 0, 0, 0, 0, 0, 0xf8, 0x7f, // NaN X
            0, 0, 0, 0, 0, 0, 0xf8, 0x7f, // NaN Y
        ],
        LineString => &[1, 2, 0, 0, 0, 0, 0, 0, 0],
        Polygon => &[1, 3, 0, 0, 0, 0, 0, 0, 0],
        MultiPoint => &[1, 4, 0, 0, 0, 0, 0, 0, 0],
        MultiLineString => &[1, 5, 0, 0, 0, 0, 0, 0, 0],
        MultiPolygon => &[1, 6, 0, 0, 0, 0, 0, 0, 0],
        GeometryCollection => &[1, 7, 0, 0, 0, 0, 0, 0, 0],
        LinearRing => return None,
    })
}

#[cfg(test)]
mod tests {
    use geo_traits::{GeometryTrait, LineStringTrait, PolygonTrait};
    use geoarrow_array::builder::RectBuilder;
    use geoarrow_schema::BoxType;
    use wkt::types::Coord;

    use super::*;

    #[test]
    fn box_inputs_are_encoded_as_polygons() {
        let metadata = Arc::new(Metadata::default());
        let mut builder =
            RectBuilder::with_capacity(BoxType::new(geoarrow_schema::Dimension::XY, metadata), 1);
        builder.push_min_max(
            &Coord {
                x: 0.0,
                y: 0.0,
                z: None,
                m: None,
            },
            &Coord {
                x: 10.0,
                y: 10.0,
                z: None,
                m: None,
            },
        );

        let encoded = to_geos_wkb(&builder.finish()).unwrap();
        let geometry = Wkb::try_new(encoded.inner().value(0)).unwrap();
        let geo_traits::GeometryType::Polygon(polygon) = geometry.as_type() else {
            panic!("Box input must be encoded as a polygon");
        };
        assert_eq!(polygon.exterior().unwrap().num_coords(), 5);
    }
}
