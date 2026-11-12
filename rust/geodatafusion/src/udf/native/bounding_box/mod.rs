//! Bounding-box construction, extraction, extrema, and aggregate (`ST_Extent`) UDFs.

mod r#box;
mod expand;
mod extent;
mod extrema;
mod make_box;
/// Bounding-box computation utilities on GeoArrow arrays.
pub mod util;

pub use r#box::{Box2D, Box3D};
pub use expand::Expand;
pub use extent::Extent;
pub use extrema::{XMax, XMin, YMax, YMin, ZMax, ZMin};
pub use make_box::{MakeBox2D, MakeBox3D};

/// Register all native bounding-box UDFs and UDAFs into the given [`SessionContext`](datafusion::prelude::SessionContext).
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
    session_context.register_udf(Box2D::default().into());
    session_context.register_udf(Box3D::default().into());
    session_context.register_udf(Expand::new(coord_type).into());
    session_context.register_udaf(Extent.into());
    session_context.register_udf(XMax::default().into());
    session_context.register_udf(XMin::default().into());
    session_context.register_udf(YMax::default().into());
    session_context.register_udf(YMin::default().into());
    session_context.register_udf(ZMax::default().into());
    session_context.register_udf(ZMin::default().into());
    session_context.register_udf(MakeBox2D::default().into());
    session_context.register_udf(MakeBox3D::default().into());
}
