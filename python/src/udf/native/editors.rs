use geodatafusion::udf::native::editors::{Force2D, Force3DM, Force3DZ, Force4D};

use crate::impl_udf_coord_type_arg;

impl_udf_coord_type_arg!(Force2D, PyForce2D, "Force2D", "geodatafusion.native");
impl_udf_coord_type_arg!(Force3DZ, PyForce3DZ, "Force3DZ", "geodatafusion.native");
impl_udf_coord_type_arg!(Force3DM, PyForce3DM, "Force3DM", "geodatafusion.native");
impl_udf_coord_type_arg!(Force4D, PyForce4D, "Force4D", "geodatafusion.native");
