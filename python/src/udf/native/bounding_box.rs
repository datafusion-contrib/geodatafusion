use std::sync::Arc;

use datafusion::logical_expr::AggregateUDF;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use geodatafusion::udf::native::bounding_box::{
    Box2D, Box3D, Extent, MakeBox2D, MakeBox3D, XMax, XMin, YMax, YMin, ZMax, ZMin,
};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::constants::AGGREGATE_UDF_CAPSULE_NAME;
use crate::impl_udf;

impl_udf!(Box2D, PyBox2D, "Box2D");
impl_udf!(Box3D, PyBox3D, "Box3D");
impl_udf!(XMin, PyXMin, "XMin");
impl_udf!(XMax, PyXMax, "XMax");
impl_udf!(YMin, PyYMin, "YMin");
impl_udf!(YMax, PyYMax, "YMax");
impl_udf!(ZMin, PyZMin, "ZMin");
impl_udf!(ZMax, PyZMax, "ZMax");
impl_udf!(MakeBox2D, PyMakeBox2D, "MakeBox2D");
impl_udf!(MakeBox3D, PyMakeBox3D, "MakeBox3D");

#[pyclass(module = "geodatafusion", name = "Extent", frozen)]
#[derive(Debug, Clone)]
pub struct PyExtent(Arc<Extent>);

#[pymethods]
impl PyExtent {
    #[new]
    fn new() -> Self {
        PyExtent(Arc::new(Extent::new()))
    }

    fn __datafusion_aggregate_udf__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let udf = Arc::new(AggregateUDF::new_from_shared_impl(self.0.clone()));
        PyCapsule::new(
            py,
            FFI_AggregateUDF::from(udf),
            Some(AGGREGATE_UDF_CAPSULE_NAME.into()),
        )
    }
}
