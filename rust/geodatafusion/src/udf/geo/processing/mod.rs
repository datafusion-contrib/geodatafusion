mod advanced;
mod affine;
mod buffer;
mod centroid;
mod convex_hull;
mod editors;
mod linear_ref;
mod make_valid;
mod oriented_envelope;
mod overlay;
mod point_on_surface;
mod simplify;
mod transform;

pub use advanced::{ChaikinSmoothing, ReducePrecision, Segmentize, UnaryUnion};
pub use affine::{StAffine, StRotate, StScale, StTranslate};
pub use buffer::StBuffer;
pub use centroid::Centroid;
pub use convex_hull::ConvexHull;
pub use editors::{
    CollectionExtract, FlipCoordinates, Force2D, Multi, Normalize, RemoveRepeatedPoints, Reverse,
};
pub use linear_ref::{LineInterpolatePoint, LineLocatePoint, LineSubstring};
pub use make_valid::MakeValid;
pub use oriented_envelope::OrientedEnvelope;
pub use overlay::{StDifference, StIntersection, StSymDifference, StUnion};
pub use point_on_surface::PointOnSurface;
pub use simplify::{Simplify, SimplifyPreserveTopology, SimplifyVW};
pub use transform::StTransform;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(Centroid::default().into());
    session_context.register_udf(ChaikinSmoothing::default().into());
    session_context.register_udf(CollectionExtract::default().into());
    session_context.register_udf(ConvexHull::default().into());
    session_context.register_udf(FlipCoordinates::default().into());
    session_context.register_udf(Force2D::default().into());
    session_context.register_udf(LineInterpolatePoint::default().into());
    session_context.register_udf(LineLocatePoint::default().into());
    session_context.register_udf(LineSubstring::default().into());
    session_context.register_udf(MakeValid::default().into());
    session_context.register_udf(Multi::default().into());
    session_context.register_udf(Normalize::default().into());
    session_context.register_udf(OrientedEnvelope::default().into());
    session_context.register_udf(PointOnSurface::default().into());
    session_context.register_udf(ReducePrecision::default().into());
    session_context.register_udf(RemoveRepeatedPoints::default().into());
    session_context.register_udf(Reverse::default().into());
    session_context.register_udf(Segmentize::default().into());
    session_context.register_udf(Simplify::default().into());
    session_context.register_udf(SimplifyPreserveTopology::default().into());
    session_context.register_udf(SimplifyVW::default().into());
    session_context.register_udf(StAffine::default().into());
    session_context.register_udf(StBuffer::default().into());
    session_context.register_udf(StDifference::default().into());
    session_context.register_udf(StIntersection::default().into());
    session_context.register_udf(StRotate::default().into());
    session_context.register_udf(StScale::default().into());
    session_context.register_udf(StSymDifference::default().into());
    // StTransform not registered: requires proj crate for actual coordinate reprojection
    session_context.register_udf(StTranslate::default().into());
    session_context.register_udf(StUnion::default().into());
    session_context.register_udf(UnaryUnion::default().into());
}
