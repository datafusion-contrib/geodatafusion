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
    session_context.register_udf(Box2D.into());
    session_context.register_udf(Box3D.into());
    session_context.register_udf(Expand::default().into());
    session_context.register_udaf(Extent.into());
    session_context.register_udf(XMax.into());
    session_context.register_udf(XMin.into());
    session_context.register_udf(YMax.into());
    session_context.register_udf(YMin.into());
    session_context.register_udf(ZMax.into());
    session_context.register_udf(ZMin.into());
    session_context.register_udf(MakeBox2D.into());
    session_context.register_udf(MakeBox3D.into());
}
