use geodatafusion::udf::native::constructors::{
    LineFromMultiPoint, MakeEnvelope, MakeLine, MakePoint, MakePointM, Point, PointM, PointZ,
    PointZM,
};

use crate::impl_udf_coord_type_arg;

impl_udf_coord_type_arg!(
    LineFromMultiPoint,
    PyLineFromMultiPoint,
    "LineFromMultiPoint",
    "geodatafusion.native"
);
impl_udf_coord_type_arg!(
    MakeEnvelope,
    PyMakeEnvelope,
    "MakeEnvelope",
    "geodatafusion.native"
);
impl_udf_coord_type_arg!(MakeLine, PyMakeLine, "MakeLine", "geodatafusion.native");
impl_udf_coord_type_arg!(Point, PyPoint, "Point", "geodatafusion.native");
impl_udf_coord_type_arg!(PointZ, PyPointZ, "PointZ", "geodatafusion.native");
impl_udf_coord_type_arg!(PointM, PyPointM, "PointM", "geodatafusion.native");
impl_udf_coord_type_arg!(PointZM, PyPointZM, "PointZM", "geodatafusion.native");
impl_udf_coord_type_arg!(MakePoint, PyMakePoint, "MakePoint", "geodatafusion.native");
impl_udf_coord_type_arg!(
    MakePointM,
    PyMakePointM,
    "MakePointM",
    "geodatafusion.native"
);
