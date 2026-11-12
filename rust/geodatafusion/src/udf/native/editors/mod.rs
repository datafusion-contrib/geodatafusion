//! Geometry Editors

mod force_dimensions;

pub(crate) use force_dimensions::force_array_to_dimension;
pub use force_dimensions::{Force2D, Force3DM, Force3DZ, Force4D};

/// Register all provided [datafusion::logical_expr::ScalarUDF] for geometry editors
pub fn register(session_context: &datafusion::prelude::SessionContext) {
    register_with_coord_type(session_context, crate::CoordType::default());
}

/// Register this group with the requested coordinate layout for geometry-producing UDFs.
///
/// Functions whose output layout is fixed by their type, such as bounding boxes, keep that layout.
pub fn register_with_coord_type(
    session_context: &datafusion::prelude::SessionContext,
    coord_type: crate::CoordType,
) {
    session_context.register_udf(Force2D::new(coord_type).into());
    session_context.register_udf(Force3DZ::new(coord_type).into());
    session_context.register_udf(Force3DM::new(coord_type).into());
    session_context.register_udf(Force4D::new(coord_type).into());
}
