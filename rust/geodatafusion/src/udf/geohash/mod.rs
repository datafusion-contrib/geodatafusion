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
    session_context.register_udf(GeoHash.into());
    session_context.register_udf(Box2DFromGeoHash.into());
    session_context.register_udf(PointFromGeoHash::default().into());
}
