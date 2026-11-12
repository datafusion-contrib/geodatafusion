//! Geometry constructor UDFs (`ST_Point*`, `ST_MakePoint*`, `ST_MakeLine`, `ST_LineFromMultiPoint`, `ST_MakeEnvelope`).

mod envelope;
mod line_from_multipoint;
mod make_line;
mod point;

pub use envelope::MakeEnvelope;
pub use line_from_multipoint::LineFromMultiPoint;
pub use make_line::MakeLine;
pub use point::{MakePoint, MakePointM, Point, PointM, PointZ, PointZM};

/// Register all native geometry constructor UDFs into the given [`SessionContext`](datafusion::prelude::SessionContext).
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
    session_context.register_udf(MakeEnvelope::new(coord_type).into());
    session_context.register_udf(LineFromMultiPoint::new(coord_type).into());
    session_context.register_udf(MakeLine::new(coord_type).into());
    session_context.register_udf(MakePoint::new(coord_type).into());
    session_context.register_udf(MakePointM::new(coord_type).into());
    session_context.register_udf(Point::new(coord_type).into());
    session_context.register_udf(PointM::new(coord_type).into());
    session_context.register_udf(PointZ::new(coord_type).into());
    session_context.register_udf(PointZM::new(coord_type).into());
}
