use geodatafusion::udf::geo::processing::{
    Centroid, ConcaveHull, ConvexHull, LineMerge, OrientedEnvelope, PointOnSurface, Simplify,
    SimplifyPreserveTopology, SimplifyVW,
};

use crate::impl_udf_coord_type_arg;

impl_udf_coord_type_arg!(Centroid, PyCentroid, "Centroid", "geodatafusion.geo");
impl_udf_coord_type_arg!(
    ConcaveHull,
    PyConcaveHull,
    "ConcaveHull",
    "geodatafusion.geo"
);
impl_udf_coord_type_arg!(ConvexHull, PyConvexHull, "ConvexHull", "geodatafusion.geo");
impl_udf_coord_type_arg!(LineMerge, PyLineMerge, "LineMerge", "geodatafusion.geo");
impl_udf_coord_type_arg!(
    OrientedEnvelope,
    PyOrientedEnvelope,
    "OrientedEnvelope",
    "geodatafusion.geo"
);
impl_udf_coord_type_arg!(
    PointOnSurface,
    PyPointOnSurface,
    "PointOnSurface",
    "geodatafusion.geo"
);
impl_udf_coord_type_arg!(Simplify, PySimplify, "Simplify", "geodatafusion.geo");
impl_udf_coord_type_arg!(
    SimplifyPreserveTopology,
    PySimplifyPreserveTopology,
    "SimplifyPreserveTopology",
    "geodatafusion.geo"
);
impl_udf_coord_type_arg!(SimplifyVW, PySimplifyVW, "SimplifyVW", "geodatafusion.geo");
