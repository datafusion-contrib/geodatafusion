use geodatafusion::udf::geohash::{Box2DFromGeoHash, GeoHash, PointFromGeoHash};
use pyo3::prelude::*;

use crate::{impl_udf, impl_udf_coord_type_arg};

impl_udf!(
    Box2DFromGeoHash,
    PyBox2DFromGeoHash,
    "Box2DFromGeoHash",
    "geodatafusion.geohash"
);
impl_udf!(GeoHash, PyGeoHash, "GeoHash", "geodatafusion.geohash");

impl_udf_coord_type_arg!(
    PointFromGeoHash,
    PyPointFromGeoHash,
    "PointFromGeoHash",
    "geodatafusion.geohash"
);

#[pymodule]
pub(crate) fn geohash(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyGeoHash>()?;
    m.add_class::<PyBox2DFromGeoHash>()?;
    m.add_class::<PyPointFromGeoHash>()?;

    Ok(())
}
