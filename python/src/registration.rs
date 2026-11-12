use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::constants::{AGGREGATE_UDF_CAPSULE_NAME, SCALAR_UDF_CAPSULE_NAME};
use crate::utils::PyCoordType;

type ScalarUdfExport<'py> = (String, Vec<String>, Bound<'py, PyCapsule>);
type AggregateUdfExport<'py> = (String, Vec<String>, Bound<'py, PyCapsule>);

fn register_group(
    ctx: &SessionContext,
    group: &str,
    coord_type: geodatafusion::CoordType,
) -> PyResult<()> {
    match group {
        "all" => geodatafusion::register_with_coord_type(ctx, coord_type),
        "geo" => geodatafusion::udf::geo::register_with_coord_type(ctx, coord_type),
        "geohash" => geodatafusion::udf::geohash::register_with_coord_type(ctx, coord_type),
        "native" => geodatafusion::udf::native::register_with_coord_type(ctx, coord_type),
        _ => {
            return Err(PyValueError::new_err(
                "registration group must be one of: all, geo, geohash, native",
            ));
        }
    }

    Ok(())
}

/// Export the scalar and aggregate UDFs registered by the Rust registration path.
///
/// The registry starts without DataFusion's built-in functions. Only canonical
/// names are exported; aliases travel inside each UDF and are returned for
/// parity checks.
#[pyfunction]
#[pyo3(signature = (group, *, coord_type=None))]
pub(crate) fn _registration_exports<'py>(
    py: Python<'py>,
    group: &str,
    coord_type: Option<PyCoordType>,
) -> PyResult<(Vec<ScalarUdfExport<'py>>, Vec<AggregateUdfExport<'py>>)> {
    let ctx = SessionContext::new_with_state(SessionStateBuilder::new().build());
    register_group(&ctx, group, coord_type.map(Into::into).unwrap_or_default())?;

    let mut scalar_udfs: Vec<_> = ctx
        .state()
        .scalar_functions()
        .iter()
        .filter(|(name, udf)| name.as_str() == udf.name())
        .map(|(name, udf)| (name.clone(), Arc::clone(udf)))
        .collect();
    scalar_udfs.sort_by(|(left, _), (right, _)| left.cmp(right));

    let scalar_exports = scalar_udfs
        .into_iter()
        .map(|(name, udf)| {
            let aliases = udf.aliases().to_vec();
            let capsule = PyCapsule::new_with_value(
                py,
                FFI_ScalarUDF::from(Arc::clone(&udf)),
                SCALAR_UDF_CAPSULE_NAME,
            )?;
            Ok((name, aliases, capsule))
        })
        .collect::<PyResult<Vec<_>>>()?;

    let mut aggregate_udfs: Vec<_> = ctx
        .state()
        .aggregate_functions()
        .iter()
        .filter(|(name, udf)| name.as_str() == udf.name())
        .map(|(name, udf)| (name.clone(), Arc::clone(udf)))
        .collect();
    aggregate_udfs.sort_by(|(left, _), (right, _)| left.cmp(right));

    let aggregate_exports = aggregate_udfs
        .into_iter()
        .map(|(name, udf)| {
            let aliases = udf.aliases().to_vec();
            let capsule = PyCapsule::new_with_value(
                py,
                FFI_AggregateUDF::from(Arc::clone(&udf)),
                AGGREGATE_UDF_CAPSULE_NAME,
            )?;
            Ok((name, aliases, capsule))
        })
        .collect::<PyResult<Vec<_>>>()?;

    Ok((scalar_exports, aggregate_exports))
}
