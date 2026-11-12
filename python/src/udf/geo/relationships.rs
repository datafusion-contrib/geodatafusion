use geodatafusion::udf::geo::relationships::{
    Contains, CoveredBy, Covers, Crosses, Disjoint, Equals, Intersects, Overlaps, Touches, Within,
};

use crate::impl_udf;

impl_udf!(Contains, PyContains, "Contains", "geodatafusion.geo");
impl_udf!(CoveredBy, PyCoveredBy, "CoveredBy", "geodatafusion.geo");
impl_udf!(Covers, PyCovers, "Covers", "geodatafusion.geo");
impl_udf!(Crosses, PyCrosses, "Crosses", "geodatafusion.geo");
impl_udf!(Disjoint, PyDisjoint, "Disjoint", "geodatafusion.geo");
impl_udf!(Equals, PyEquals, "Equals", "geodatafusion.geo");
impl_udf!(Intersects, PyIntersects, "Intersects", "geodatafusion.geo");
impl_udf!(Overlaps, PyOverlaps, "Overlaps", "geodatafusion.geo");
impl_udf!(Touches, PyTouches, "Touches", "geodatafusion.geo");
impl_udf!(Within, PyWithin, "Within", "geodatafusion.geo");
