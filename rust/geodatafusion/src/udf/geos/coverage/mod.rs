//! PostGIS polygonal coverage functions backed by GEOS.
//!
//! `ST_CoverageInvalidEdges` and `ST_CoverageSimplify` are window functions:
//! all polygon and multipolygon members of a window partition participate in
//! one coverage calculation, and each input row receives its corresponding
//! output geometry. NULL, EMPTY, and non-polygonal members do not participate
//! and return SQL NULL, matching PostGIS.
//!
//! `ST_CoverageUnion` is an aggregate. Its `geometry[]` scalar overload is not
//! registered yet; a same-name scalar currently shadows the aggregate, so the
//! overload depends on the shared scalar/aggregate SQL dispatch work.

mod ffi;
#[cfg(feature = "geos-3_12")]
mod invalid_edges;
#[cfg(feature = "geos-3_12")]
mod simplify;
mod union;
#[cfg(feature = "geos-3_12")]
mod window;

#[cfg(feature = "geos-3_12")]
use arrow_array::Array;
#[cfg(feature = "geos-3_12")]
use arrow_schema::DataType;
use arrow_schema::Field;
#[cfg(feature = "geos-3_12")]
use datafusion::error::DataFusionError;
use datafusion::error::Result;
#[cfg(feature = "geos-3_12")]
#[cfg(feature = "geos-3_12")]
use geoarrow_array::cast::to_wkb;
#[cfg(feature = "geos-3_12")]
pub use invalid_edges::CoverageInvalidEdges;
#[cfg(feature = "geos-3_12")]
pub use simplify::CoverageSimplify;
pub use union::CoverageUnion;

use crate::error::GeoDataFusionResult;
#[cfg(feature = "geos-3_12")]
use crate::udf::native::geometry::geometry_array_from_arrow;
#[cfg(feature = "geos-3_12")]
use crate::udf::native::geometry::is_null_geometry_input;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udaf(CoverageUnion::default().into());

    #[cfg(feature = "geos-3_12")]
    {
        session_context.register_udwf(CoverageInvalidEdges::default().into());
        session_context.register_udwf(CoverageSimplify::default().into());
    }
}

pub(super) fn geometry_type_from_field(
    field: &Field,
    function: &str,
) -> Result<geoarrow_schema::GeometryType> {
    super::adapter::geometry_type_from_field(field, function)
}

/// Convert one GeoArrow geometry array to WKB values.
#[cfg(feature = "geos-3_12")]
pub(super) fn geometry_wkb_values(
    field: &Field,
    array: &dyn Array,
) -> GeoDataFusionResult<Vec<Option<Vec<u8>>>> {
    if is_null_geometry_input(field, array) {
        return Ok(vec![None; array.len()]);
    }

    let geometry = geometry_array_from_arrow(array, field)?;
    let wkb = to_wkb::<i32>(geometry.as_ref())?;
    Ok(wkb
        .inner()
        .iter()
        .map(|value| value.map(Vec::from))
        .collect())
}

/// Resolve the first value of a Float64 argument using PostGIS defaults.
#[cfg(feature = "geos-3_12")]
pub(super) fn float64_argument(
    values: &[arrow_array::ArrayRef],
    position: usize,
    default: f64,
    function: &str,
) -> Result<f64> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;

    let array = values.get(position).ok_or_else(|| {
        DataFusionError::Internal(format!("{function} is missing argument {}", position + 1))
    })?;
    match array.data_type() {
        DataType::Null => Ok(default),
        DataType::Float64 => {
            let array = array.as_primitive::<Float64Type>();
            if array.is_empty() || array.is_null(0) {
                Ok(default)
            } else {
                Ok(array.value(0))
            }
        }
        data_type => Err(DataFusionError::Internal(format!(
            "{function} argument {} was not coerced to Float64: {data_type}",
            position + 1
        ))),
    }
}

/// Resolve the first value of a Boolean argument using PostGIS defaults.
#[cfg(feature = "geos-3_12")]
pub(super) fn boolean_argument(
    values: &[arrow_array::ArrayRef],
    position: usize,
    default: bool,
    function: &str,
) -> Result<bool> {
    use arrow_array::cast::AsArray;

    let array = values.get(position).ok_or_else(|| {
        DataFusionError::Internal(format!("{function} is missing argument {}", position + 1))
    })?;
    match array.data_type() {
        DataType::Null => Ok(default),
        DataType::Boolean => {
            let array = array.as_boolean();
            if array.is_empty() || array.is_null(0) {
                Ok(default)
            } else {
                Ok(array.value(0))
            }
        }
        data_type => Err(DataFusionError::Internal(format!(
            "{function} argument {} was not coerced to Boolean: {data_type}",
            position + 1
        ))),
    }
}
