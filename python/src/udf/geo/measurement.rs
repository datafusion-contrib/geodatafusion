use geodatafusion::udf::geo::measurement::{Area, DWithin, Distance, Length, Perimeter};

use crate::impl_udf;

impl_udf!(Area, PyArea, "Area", "geodatafusion.geo");
impl_udf!(Distance, PyDistance, "Distance", "geodatafusion.geo");
impl_udf!(DWithin, PyDWithin, "DWithin", "geodatafusion.geo");
impl_udf!(Length, PyLength, "Length", "geodatafusion.geo");
impl_udf!(Perimeter, PyPerimeter, "Perimeter", "geodatafusion.geo");
