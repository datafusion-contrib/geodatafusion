//! Shared argument coercion and scalar extraction for the clustering functions.

use std::sync::LazyLock;

use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Signature, Volatility};
use datafusion::scalar::ScalarValue;

use crate::data_types::any_geometry_type;

static GEOMETRY_TYPES: LazyLock<Vec<DataType>> = LazyLock::new(any_geometry_type);

/// Convert a GeoArrow error into the DataFusion error type used by the clustering functions.
pub(super) fn arrow_error(error: geoarrow_schema::error::GeoArrowError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

pub(super) fn geometry_types() -> &'static [DataType] {
    GEOMETRY_TYPES.as_slice()
}

/// A user-defined signature: the exact argument shapes depend on the function, and the
/// planner coercion below also accepts bare `NULL` and numeric literals.
pub(super) fn user_defined_signature(volatility: Volatility) -> Signature {
    Signature::user_defined(volatility)
}

/// Validate a geometry argument and keep its exact type so the planner does not insert a
/// cast that would drop extension metadata.
pub(super) fn coerce_geometry(
    function: &str,
    argument_index: usize,
    data_type: &DataType,
) -> Result<DataType> {
    if data_type == &DataType::Null || geometry_types().contains(data_type) {
        Ok(data_type.clone())
    } else {
        Err(DataFusionError::Plan(format!(
            "Function '{function}' argument {} must be a geometry, got {data_type}",
            argument_index + 1
        )))
    }
}

/// Coerce any numeric argument to the float64 parameter type used by PostGIS.
pub(super) fn coerce_float(
    function: &str,
    argument_index: usize,
    data_type: &DataType,
) -> Result<DataType> {
    match data_type {
        DataType::Null
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64 => Ok(DataType::Float64),
        _ => Err(DataFusionError::Plan(format!(
            "Function '{function}' argument {} must be numeric, got {data_type}",
            argument_index + 1
        ))),
    }
}

/// Coerce any integer argument to int64; execution then enforces the 32-bit range.
pub(super) fn coerce_int(
    function: &str,
    argument_index: usize,
    data_type: &DataType,
) -> Result<DataType> {
    match data_type {
        DataType::Null
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => Ok(DataType::Int64),
        _ => Err(DataFusionError::Plan(format!(
            "Function '{function}' argument {} must be an integer, got {data_type}",
            argument_index + 1
        ))),
    }
}

/// Read one scalar argument value from an array.
pub(super) fn scalar_at(array: &arrow_array::ArrayRef, row: usize) -> Result<ScalarValue> {
    ScalarValue::try_from_array(array, row)
}

/// Read a float64 parameter value.
pub(super) fn float_at(
    function: &str,
    array: &arrow_array::ArrayRef,
    row: usize,
) -> Result<Option<f64>> {
    match scalar_at(array, row)? {
        ScalarValue::Float64(value) => Ok(value),
        ScalarValue::Float32(value) => Ok(value.map(f64::from)),
        ScalarValue::Float16(value) => Ok(value.map(f64::from)),
        ScalarValue::Int8(value) => Ok(value.map(f64::from)),
        ScalarValue::Int16(value) => Ok(value.map(f64::from)),
        ScalarValue::Int32(value) => Ok(value.map(f64::from)),
        ScalarValue::Int64(value) => Ok(value.map(|value| value as f64)),
        ScalarValue::UInt8(value) => Ok(value.map(f64::from)),
        ScalarValue::UInt16(value) => Ok(value.map(f64::from)),
        ScalarValue::UInt32(value) => Ok(value.map(f64::from)),
        ScalarValue::Null => Ok(None),
        other => Err(DataFusionError::Internal(format!(
            "{function} received an unsupported numeric value: {other:?}"
        ))),
    }
}

/// Read an int64 parameter value.
pub(super) fn int_at(
    function: &str,
    array: &arrow_array::ArrayRef,
    row: usize,
) -> Result<Option<i64>> {
    match scalar_at(array, row)? {
        ScalarValue::Int8(value) => Ok(value.map(i64::from)),
        ScalarValue::Int16(value) => Ok(value.map(i64::from)),
        ScalarValue::Int32(value) => Ok(value.map(i64::from)),
        ScalarValue::Int64(value) => Ok(value),
        ScalarValue::UInt8(value) => Ok(value.map(i64::from)),
        ScalarValue::UInt16(value) => Ok(value.map(i64::from)),
        ScalarValue::UInt32(value) => Ok(value.map(i64::from)),
        ScalarValue::Null => Ok(None),
        other => Err(DataFusionError::Internal(format!(
            "{function} received an unsupported integer value: {other:?}"
        ))),
    }
}
