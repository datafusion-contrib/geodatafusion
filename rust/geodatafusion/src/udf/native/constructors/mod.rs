mod envelope;
mod line_from_multipoint;
mod point;

pub use envelope::MakeEnvelope;
pub use line_from_multipoint::LineFromMultiPoint;
pub use point::{MakePoint, MakePointM, Point, PointM, PointZ, PointZM};

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(MakeEnvelope::default().into());
    session_context.register_udf(LineFromMultiPoint::default().into());
    session_context.register_udf(MakePoint::default().into());
    session_context.register_udf(MakePointM::default().into());
    session_context.register_udf(Point::default().into());
    session_context.register_udf(PointM::default().into());
    session_context.register_udf(PointZ::default().into());
    session_context.register_udf(PointZM::default().into());
}
