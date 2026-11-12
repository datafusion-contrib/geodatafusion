use geodatafusion::udf::native::io::{AsBinary, AsText, GeomFromText, GeomFromWKB};

use crate::{impl_udf, impl_udf_coord_type_arg};

impl_udf!(AsBinary, PyAsBinary, "AsBinary", "geodatafusion.native");
impl_udf!(AsText, PyAsText, "AsText", "geodatafusion.native");

impl_udf_coord_type_arg!(
    GeomFromWKB,
    PyGeomFromWKB,
    "GeomFromWKB",
    "geodatafusion.native"
);
impl_udf_coord_type_arg!(
    GeomFromText,
    PyGeomFromText,
    "GeomFromText",
    "geodatafusion.native"
);
