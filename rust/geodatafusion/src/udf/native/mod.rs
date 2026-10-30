//! User-defined functions that wrap native Rust implementations.

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
    accessors::register(session_context);
    bounding_box::register(session_context);
    constructors::register(session_context);
    editors::register(session_context);
    io::register(session_context);
}
