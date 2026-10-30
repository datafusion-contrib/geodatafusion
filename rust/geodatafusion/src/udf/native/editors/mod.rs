//! Geometry Editors

mod force_dimensions;

pub use force_dimensions::{Force2D, Force3D, Force3DM, Force3DZ, Force4D};

/// Register all provided [datafusion::logical_expr::ScalarUDF] for geometry editors
pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(Force2D::default().into());
    session_context.register_udf(Force3DZ::default().into());
    session_context.register_udf(Force3DM::default().into());
    session_context.register_udf(Force4D::default().into());
}
