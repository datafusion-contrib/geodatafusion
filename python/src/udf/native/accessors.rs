use geodatafusion::udf::native::accessors::{
    CoordDim, EndPoint, M, NDims, NPoints, StartPoint, X, Y, Z,
};

use crate::{impl_udf, impl_udf_coord_type_arg};

impl_udf!(CoordDim, PyCoordDim, "CoordDim");
impl_udf!(NDims, PyNDims, "NDims");
impl_udf!(X, PyX, "X");
impl_udf!(Y, PyY, "Y");
impl_udf!(Z, PyZ, "Z");
impl_udf!(M, PyM, "M");
impl_udf_coord_type_arg!(EndPoint, PyEndPoint, "EndPoint");
impl_udf_coord_type_arg!(StartPoint, PyStartPoint, "StartPoint");
impl_udf!(NPoints, PyNPoints, "NPoints");
