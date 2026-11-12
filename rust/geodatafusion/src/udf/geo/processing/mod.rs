//! Geometry processing and simplification UDFs implemented with [`geo`](https://docs.rs/geo).

mod centroid;
mod concave_hull;
mod convex_hull;
mod line_merge;
mod oriented_envelope;
mod point_on_surface;
mod simplify;

pub use centroid::Centroid;
pub use concave_hull::ConcaveHull;
pub use convex_hull::ConvexHull;
pub use line_merge::LineMerge;
pub use oriented_envelope::OrientedEnvelope;
pub use point_on_surface::PointOnSurface;
pub use simplify::{Simplify, SimplifyPreserveTopology, SimplifyVW};

/// Register all `geo` processing UDFs into the given [`SessionContext`](datafusion::prelude::SessionContext).
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
    session_context.register_udf(Centroid::new(coord_type).into());
    session_context.register_udf(ConcaveHull::new(coord_type).into());
    session_context.register_udf(ConvexHull::new(coord_type).into());
    session_context.register_udf(LineMerge::new(coord_type).into());
    session_context.register_udf(OrientedEnvelope::new(coord_type).into());
    session_context.register_udf(PointOnSurface::new(coord_type).into());
    session_context.register_udf(Simplify::new(coord_type).into());
    session_context.register_udf(SimplifyPreserveTopology::new(coord_type).into());
    session_context.register_udf(SimplifyVW::new(coord_type).into());
}
