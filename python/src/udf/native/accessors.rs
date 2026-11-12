use geodatafusion::udf::native::accessors::{
    CoordDim, Dimension, Dump, EndPoint, Envelope, ExteriorRing, GeometryType, HasM, HasZ,
    InteriorRingN, IsClosed, IsCollection, IsEmpty, M, NPoints, NRings, NumGeometries,
    NumInteriorRings, PointN, STGeometryType, StartPoint, X, Y, Z, ZmFlag,
};

use crate::{impl_udf, impl_udf_coord_type_arg};

impl_udf!(CoordDim, PyCoordDim, "CoordDim", "geodatafusion.native");
impl_udf!(Dimension, PyDimension, "Dimension", "geodatafusion.native");
impl_udf_coord_type_arg!(Dump, PyDump, "Dump", "geodatafusion.native");
impl_udf_coord_type_arg!(Envelope, PyEnvelope, "Envelope", "geodatafusion.native");
impl_udf!(X, PyX, "X", "geodatafusion.native");
impl_udf!(Y, PyY, "Y", "geodatafusion.native");
impl_udf!(Z, PyZ, "Z", "geodatafusion.native");
impl_udf!(M, PyM, "M", "geodatafusion.native");
impl_udf!(HasM, PyHasM, "HasM", "geodatafusion.native");
impl_udf!(HasZ, PyHasZ, "HasZ", "geodatafusion.native");
impl_udf!(IsClosed, PyIsClosed, "IsClosed", "geodatafusion.native");
impl_udf!(
    IsCollection,
    PyIsCollection,
    "IsCollection",
    "geodatafusion.native"
);
impl_udf!(IsEmpty, PyIsEmpty, "IsEmpty", "geodatafusion.native");
impl_udf_coord_type_arg!(EndPoint, PyEndPoint, "EndPoint", "geodatafusion.native");
impl_udf_coord_type_arg!(
    ExteriorRing,
    PyExteriorRing,
    "ExteriorRing",
    "geodatafusion.native"
);
impl_udf_coord_type_arg!(
    InteriorRingN,
    PyInteriorRingN,
    "InteriorRingN",
    "geodatafusion.native"
);
impl_udf_coord_type_arg!(PointN, PyPointN, "PointN", "geodatafusion.native");
impl_udf_coord_type_arg!(
    StartPoint,
    PyStartPoint,
    "StartPoint",
    "geodatafusion.native"
);
impl_udf!(NPoints, PyNPoints, "NPoints", "geodatafusion.native");
impl_udf!(NRings, PyNRings, "NRings", "geodatafusion.native");
impl_udf!(
    NumGeometries,
    PyNumGeometries,
    "NumGeometries",
    "geodatafusion.native"
);
impl_udf!(
    NumInteriorRings,
    PyNumInteriorRings,
    "NumInteriorRings",
    "geodatafusion.native"
);
impl_udf!(
    GeometryType,
    PyGeometryType,
    "GeometryType",
    "geodatafusion.native"
);
impl_udf!(
    STGeometryType,
    PySTGeometryType,
    "STGeometryType",
    "geodatafusion.native"
);
impl_udf!(ZmFlag, PyZmFlag, "ZmFlag", "geodatafusion.native");
