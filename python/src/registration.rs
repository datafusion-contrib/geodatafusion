use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_ffi::udtf::FFI_TableFunction;
use datafusion_ffi::udwf::FFI_WindowUDF;
use pyo3::exceptions::{PyImportError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::constants::{
    AGGREGATE_UDF_CAPSULE_NAME, SCALAR_UDF_CAPSULE_NAME, TABLE_FUNCTION_CAPSULE_NAME,
    WINDOW_UDF_CAPSULE_NAME,
};

type ScalarUdfExport<'py> = (String, Vec<String>, Bound<'py, PyCapsule>);
type AggregateUdfExport<'py> = (String, Vec<String>, Bound<'py, PyCapsule>);
type WindowUdfExport<'py> = (String, Vec<String>, Bound<'py, PyCapsule>);
type TableFunctionExport<'py> = (String, Py<PyTableFunctionExport>);
type RegistrationExports<'py> = (
    Vec<ScalarUdfExport<'py>>,
    Vec<AggregateUdfExport<'py>>,
    Vec<WindowUdfExport<'py>>,
    Vec<TableFunctionExport<'py>>,
);

/// Registration-time handle for one table function.
///
/// Table functions are the only kind whose FFI capsule cannot be built up
/// front: the capsule must carry the *caller's* logical extension codec, so the
/// host asks for it through `__datafusion_table_function__(session)` at
/// registration time.
#[pyclass(module = "geodatafusion", name = "TableFunctionExport", frozen)]
pub(crate) struct PyTableFunctionExport {
    udtf: Arc<dyn TableFunctionImpl>,
}

#[pymethods]
impl PyTableFunctionExport {
    fn __datafusion_table_function__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let codec = host_logical_codec(&session)?;
        let udtf = FFI_TableFunction::new_with_ffi_codec(Arc::clone(&self.udtf), None, codec);
        PyCapsule::new_with_value(py, udtf, TABLE_FUNCTION_CAPSULE_NAME)
    }
}

/// Read the host's logical extension codec from its session object.
///
/// The host exposes `__datafusion_logical_extension_codec__()` and validates
/// the capsule name on its side; the codec is required because its task context
/// provider belongs to the session that will execute the query.
fn host_logical_codec(session: &Bound<'_, PyAny>) -> PyResult<FFI_LogicalExtensionCodec> {
    let capsule = session.call_method0("__datafusion_logical_extension_codec__")?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let data: NonNull<c_void> =
        capsule.pointer_checked(Some(c"datafusion_logical_extension_codec"))?;
    let codec = unsafe { data.cast::<FFI_LogicalExtensionCodec>().as_ref() };

    let found = unsafe { (codec.version)() };
    let expected = datafusion_ffi::version();
    if found != expected {
        return Err(PyImportError::new_err(format!(
            "Incompatible DataFusion logical extension codec major version {found}; \
             expected {expected}. Rebuild against a matching DataFusion."
        )));
    }

    Ok(codec.clone())
}

/// Whether a scalar UDF can travel through the synchronous FFI capsule.
///
/// Async scalar UDFs such as `ST_EstimatedExtent` resolve tables from the
/// catalog captured when they were registered: DataFusion hands async scalar
/// functions the same session-free `ScalarFunctionArgs` as the synchronous
/// path, so their state is bound to that registering session. The FFI capsule
/// is a synchronous, session-agnostic ABI, so exporting one would only produce
/// "async functions should not be called directly" in another session.
///
/// Detect them from the registry rather than from a name list:
/// `ScalarUDF::as_async` is exactly the downcast the physical planner uses.
fn is_exportable_scalar(udf: &datafusion::logical_expr::ScalarUDF) -> bool {
    udf.as_async().is_none()
}

fn register_group(ctx: &SessionContext, group: &str) -> PyResult<()> {
    match group {
        "all" => geodatafusion::register(ctx),
        "geo" => {
            geodatafusion::udf::geo::measurement::register(ctx);
            geodatafusion::udf::geo::processing::register(ctx);
            geodatafusion::udf::geo::relationships::register(ctx);
            geodatafusion::udf::geo::three_d::register(ctx);
            geodatafusion::udf::geo::validation::register(ctx);
        }
        "geohash" => geodatafusion::udf::geohash::register(ctx),
        "native" => {
            geodatafusion::udf::native::register(ctx);
            // The FFI export path can carry the FlatGeobuf aggregate. Its table
            // functions need the Python caller's catalog and are intentionally
            // not exported through this temporary SessionContext.
            geodatafusion::udf::flatgeobuf::register(ctx);
        }
        _ => {
            return Err(PyValueError::new_err(
                "registration group must be one of: all, geo, geohash, native",
            ));
        }
    }

    Ok(())
}

/// Export the scalar, aggregate, window, and table functions registered by the
/// Rust registration path.
///
/// The registry starts without DataFusion's built-in functions. Only canonical
/// names are exported; aliases travel inside each UDF and are returned for
/// parity checks.
#[pyfunction]
pub(crate) fn _registration_exports<'py>(
    py: Python<'py>,
    group: &str,
) -> PyResult<RegistrationExports<'py>> {
    let ctx = SessionContext::new_with_state(SessionStateBuilder::new().build());
    register_group(&ctx, group)?;

    let mut scalar_udfs: Vec<_> = ctx
        .state()
        .scalar_functions()
        .iter()
        .filter(|(name, udf)| name.as_str() == udf.name() && is_exportable_scalar(udf))
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

    let mut window_udfs: Vec<_> = ctx
        .state()
        .window_functions()
        .iter()
        .filter(|(name, udf)| name.as_str() == udf.name())
        .map(|(name, udf)| (name.clone(), Arc::clone(udf)))
        .collect();
    window_udfs.sort_by(|(left, _), (right, _)| left.cmp(right));

    let window_exports = window_udfs
        .into_iter()
        .map(|(name, udf)| {
            let aliases = udf.aliases().to_vec();
            let capsule = PyCapsule::new_with_value(
                py,
                FFI_WindowUDF::from(Arc::clone(&udf)),
                WINDOW_UDF_CAPSULE_NAME,
            )?;
            Ok((name, aliases, capsule))
        })
        .collect::<PyResult<Vec<_>>>()?;

    let mut table_functions: Vec<_> = ctx
        .state()
        .table_functions()
        .iter()
        .map(|(name, function)| (name.clone(), Arc::clone(function.function())))
        .collect();
    table_functions.sort_by(|(left, _), (right, _)| left.cmp(right));

    let table_exports = table_functions
        .into_iter()
        .map(|(name, udtf)| Ok((name, Py::new(py, PyTableFunctionExport { udtf })?)))
        .collect::<PyResult<Vec<_>>>()?;

    Ok((
        scalar_exports,
        aggregate_exports,
        window_exports,
        table_exports,
    ))
}
