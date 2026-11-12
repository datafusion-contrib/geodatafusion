//! User-defined UDFs that wrap native Rust implementations.

/// Geometry coordinate, dimension, ring, and component accessor UDFs.
pub mod accessors;
/// Bounding-box construction, extraction, extrema, and aggregate (`ST_Extent`) UDFs.
pub mod bounding_box;
/// Geometry constructor UDFs (`ST_Point*`, `ST_MakePoint*`, `ST_MakeLine`, `ST_LineFromMultiPoint`, `ST_MakeEnvelope`).
pub mod constructors;
pub mod editors;
pub mod io;

/// Register all native GeoArrow UDFs and UDAFs into the given [`SessionContext`](datafusion::prelude::SessionContext).
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
    accessors::register_with_coord_type(session_context, coord_type);
    bounding_box::register_with_coord_type(session_context, coord_type);
    constructors::register_with_coord_type(session_context, coord_type);
    editors::register_with_coord_type(session_context, coord_type);
    io::register_with_coord_type(session_context, coord_type);
}
