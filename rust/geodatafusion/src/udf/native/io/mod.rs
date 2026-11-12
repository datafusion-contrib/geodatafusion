//! Geometry Input and Output

pub(crate) mod geometry;
mod wkb;
mod wkt;

pub use wkb::{AsBinary, GeomFromWKB};
pub use wkt::{AsText, GeomFromText};

/// Register all native WKT and WKB input/output UDFs into the given [`SessionContext`](datafusion::prelude::SessionContext).
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
    session_context.register_udf(AsBinary::default().into());
    session_context.register_udf(GeomFromWKB::new(coord_type).into());
    session_context.register_udf(AsText::default().into());
    session_context.register_udf(GeomFromText::new(coord_type).into());
}
