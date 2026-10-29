//! Shared argument, metadata, and Arrow construction helpers for native
//! processing algorithms.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, Signature, Volatility,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::{GeoArrowType, GeometryType};
use wkt::Wkt;

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::editors::wkt_values;
use crate::udf::native::geometry::StrictGeometryBuilder;

pub(super) fn signature() -> Signature {
    Signature::user_defined(Volatility::Immutable)
}

pub(super) fn mutable_signature() -> Signature {
    Signature::user_defined(Volatility::Volatile)
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

pub(super) fn coerce_boolean(
    data_type: &DataType,
    position: usize,
    name: &str,
) -> Result<DataType> {
    if matches!(data_type, DataType::Null | DataType::Boolean) {
        Ok(DataType::Boolean)
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} argument {} must be boolean, got {data_type}",
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

/// Return a generic Geometry field so each row can preserve it's actual
/// GeoArrow type and dimension while sharing input CRS and coord layout.
pub(super) fn geometry_return_field(args: &ReturnFieldArgs) -> Result<FieldRef> {
    let input = args.arg_fields.first().ok_or_else(|| {
        DataFusionError::Internal("native processing function has no geometry argument".to_string())
    })?;
    let output_type = if input.data_type() == &DataType::Null {
        GeometryType::new(Default::default())
    } else {
        let input_type = GeoArrowType::from_arrow_field(input)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        GeometryType::new(input_type.metadata().clone())
            .with_coord_type(input_type.coord_type().unwrap_or_default())
    };
    Ok(Arc::new(output_type.to_field(input.name(), true)))
}

pub(super) fn input_geometry_values(
    args: &ScalarFunctionArgs,
    name: &str,
) -> GeoDataFusionResult<Vec<Option<Wkt<f64>>>> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geometry = arrays.first().ok_or_else(|| {
        DataFusionError::Internal(format!("{name} is missing its geometry argument"))
    })?;
    let row_count = geometry.len();

    if args.arg_fields[0].data_type() == &DataType::Null || geometry.data_type() == &DataType::Null
    {
        return Ok(vec![None; row_count]);
    }

    wkt_values(geometry, &args.arg_fields[0])?
        .into_iter()
        .map(|value| match value {
            None => Ok(None),
            Some(Ok(geometry)) => Ok(Some(geometry)),
            Some(Err(error)) => Err(DataFusionError::Execution(format!(
                "{name} input geometry is invalid: {error}"
            ))
            .into()),
        })
        .collect()
}

pub(super) fn geometry_columnar(
    args: &ScalarFunctionArgs,
    values: Vec<Option<Wkt<f64>>>,
) -> GeoDataFusionResult<ColumnarValue> {
    let input = &args.arg_fields[0];
    let output_type = if input.data_type() == &DataType::Null {
        GeometryType::new(Default::default())
    } else {
        let input_type = GeoArrowType::from_arrow_field(input)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        GeometryType::new(input_type.metadata().clone())
            .with_coord_type(input_type.coord_type().unwrap_or_default())
    };

    let mut builder = StrictGeometryBuilder::new(output_type);
    for value in &values {
        builder.push_geometry(value.as_ref())?;
    }
    Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
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
    if array.data_type() != &DataType::Float64 {
        return Err(DataFusionError::Internal(format!(
            "{name} argument {} was not coerced to Float64",
            position + 1
        ))
        .into());
    }
    let values = array.as_primitive::<Float64Type>();
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
    if array.data_type() != &DataType::Int64 {
        return Err(DataFusionError::Internal(format!(
            "{name} argument {} was not coerced to Int64",
            position + 1
        ))
        .into());
    }
    let values = array.as_primitive::<Int64Type>();
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
    if array.data_type() != &DataType::Boolean {
        return Err(DataFusionError::Internal(format!(
            "{name} argument {} was not coerced to Boolean",
            position + 1
        ))
        .into());
    }
    let values = array.as_boolean();
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

pub(super) fn text_value(
    arrays: &[ArrayRef],
    position: usize,
    row: usize,
    name: &str,
) -> GeoDataFusionResult<Option<String>> {
    let array = arrays.get(position).ok_or_else(|| {
        DataFusionError::Internal(format!("{name} is missing argument {}", position + 1))
    })?;
    if array.data_type() != &DataType::Utf8 {
        return Err(DataFusionError::Internal(format!(
            "{name} argument {} was not coerced to Utf8",
            position + 1
        ))
        .into());
    }
    let values = array.as_string::<i32>();
    Ok((!values.is_null(row)).then(|| values.value(row).to_string()))
}
