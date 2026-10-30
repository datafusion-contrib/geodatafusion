use geodatafusion::udf::native::accessors::{
    CoordDim, Dimension, Dump, EndPoint, ExteriorRing, GeometryType, HasM, HasZ, InteriorRingN,
    IsClosed, IsCollection, IsEmpty, M, NDims, NPoints, NRings, NumGeometries, NumInteriorRings,
    PointN, ST_GeometryType, StartPoint, X, Y, Z, ZmFlag,
};

use crate::{impl_udf, impl_udf_coord_type_arg};

impl_udf!(CoordDim, PyCoordDim, "CoordDim");
impl_udf!(Dimension, PyDimension, "Dimension");
impl_udf_coord_type_arg!(Dump, PyDump, "Dump");
impl_udf!(NDims, PyNDims, "NDims");
impl_udf!(X, PyX, "X");
impl_udf!(Y, PyY, "Y");
impl_udf!(Z, PyZ, "Z");
impl_udf!(M, PyM, "M");
impl_udf!(HasM, PyHasM, "HasM");
impl_udf!(HasZ, PyHasZ, "HasZ");
impl_udf!(IsClosed, PyIsClosed, "IsClosed");
impl_udf!(IsCollection, PyIsCollection, "IsCollection");
impl_udf!(IsEmpty, PyIsEmpty, "IsEmpty");
impl_udf_coord_type_arg!(EndPoint, PyEndPoint, "EndPoint");
impl_udf_coord_type_arg!(ExteriorRing, PyExteriorRing, "ExteriorRing");
impl_udf_coord_type_arg!(InteriorRingN, PyInteriorRingN, "InteriorRingN");
impl_udf_coord_type_arg!(PointN, PyPointN, "PointN");
impl_udf_coord_type_arg!(StartPoint, PyStartPoint, "StartPoint");
impl_udf!(NPoints, PyNPoints, "NPoints");
impl_udf!(NRings, PyNRings, "NRings");
impl_udf!(NumGeometries, PyNumGeometries, "NumGeometries");
impl_udf!(NumInteriorRings, PyNumInteriorRings, "NumInteriorRings");
impl_udf!(GeometryType, PyGeometryType, "GeometryType");
impl_udf!(ST_GeometryType, PySTGeometryType, "STGeometryType");
impl_udf!(ZmFlag, PyZmFlag, "ZmFlag");
