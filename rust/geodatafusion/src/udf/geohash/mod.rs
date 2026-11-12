//! GeoHash encoding and decoding UDFs (`ST_GeoHash`, `ST_Box2dFromGeoHash`, `ST_PointFromGeoHash`).

mod box2d_from_geohash;
#[allow(clippy::module_inception)]
mod geohash;
mod point_from_geohash;

pub use box2d_from_geohash::Box2DFromGeoHash;
pub use geohash::GeoHash;
pub use point_from_geohash::PointFromGeoHash;

/// Register all GeoHash UDFs into the given [`SessionContext`](datafusion::prelude::SessionContext).
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
    session_context.register_udf(GeoHash::default().into());
    session_context.register_udf(Box2DFromGeoHash::default().into());
    session_context.register_udf(PointFromGeoHash::new(coord_type).into());
}
