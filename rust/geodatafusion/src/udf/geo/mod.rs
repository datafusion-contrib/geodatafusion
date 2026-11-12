//! Spatial UDFs implemented using the [`geo`](https://docs.rs/geo) crate.

/// 2D Cartesian measurement UDFs (`ST_Area`, `ST_Distance`, `ST_DWithin`, `ST_Length`, `ST_Perimeter`).
pub mod measurement;
/// Geometry processing and simplification UDFs (`ST_Centroid`, `ST_ConcaveHull`, `ST_ConvexHull`, `ST_OrientedEnvelope`, `ST_PointOnSurface`, `ST_Simplify*`).
pub mod processing;
/// DE-9IM topological relationship predicate UDFs (`ST_Contains`, `ST_Intersects`, `ST_Within`, etc.).
pub mod relationships;
/// Geometry validation UDFs (`ST_IsValid`, `ST_IsValidReason`).
pub mod validation;

/// Register all [`geo`](https://docs.rs/geo)-backed UDFs into the given [`SessionContext`](datafusion::prelude::SessionContext).
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
    measurement::register(session_context);
    processing::register_with_coord_type(session_context, coord_type);
    relationships::register(session_context);
    validation::register(session_context);
}
