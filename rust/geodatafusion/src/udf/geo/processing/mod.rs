mod centroid;
mod convex_hull;
mod oriented_envelope;
mod point_on_surface;
mod simplify;

pub use centroid::Centroid;
pub use convex_hull::ConvexHull;
pub use oriented_envelope::OrientedEnvelope;
pub use point_on_surface::PointOnSurface;
pub use simplify::{Simplify, SimplifyPreserveTopology, SimplifyVW};

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(Centroid::default().into());
    session_context.register_udf(ConvexHull::default().into());
    session_context.register_udf(OrientedEnvelope::default().into());
    session_context.register_udf(PointOnSurface::default().into());
    session_context.register_udf(Simplify::default().into());
    session_context.register_udf(SimplifyPreserveTopology::default().into());
    session_context.register_udf(SimplifyVW::default().into());
}
