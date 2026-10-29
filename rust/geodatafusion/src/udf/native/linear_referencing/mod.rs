//! PostGIS-compatible linear referencing functions.

mod interpolate;
mod locate;
mod measure;
mod util;

pub use interpolate::{
    LineInterpolatePoint, LineInterpolatePoint3D, LineInterpolatePoints, LineSubstring,
};
pub use locate::{
    InterpolatePoint, LineLocatePoint, LocateAlong, LocateBetween, LocateBetweenElevations,
};
pub use measure::AddMeasure;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(LineInterpolatePoint::new().into());
    session_context.register_udf(LineInterpolatePoint3D::new().into());
    session_context.register_udf(LineInterpolatePoints::new().into());
    session_context.register_udf(LineSubstring::new().into());
    session_context.register_udf(LineLocatePoint::new().into());
    session_context.register_udf(LocateAlong::new().into());
    session_context.register_udf(LocateBetween::new().into());
    session_context.register_udf(LocateBetweenElevations::new().into());
    session_context.register_udf(InterpolatePoint::new().into());
    session_context.register_udf(AddMeasure::new().into());
}
