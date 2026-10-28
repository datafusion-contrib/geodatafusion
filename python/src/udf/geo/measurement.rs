use geodatafusion::udf::geo::measurement::{Area, DWithin, Distance, Length};

use crate::impl_udf;

impl_udf!(Area, PyArea, "Area");
impl_udf!(Distance, PyDistance, "Distance");
impl_udf!(DWithin, PyDWithin, "DWithin");
impl_udf!(Length, PyLength, "Length");
