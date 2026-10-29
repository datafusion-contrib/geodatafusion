//! Mapbox Vector Tile output functions.

mod aggregate;
mod encoder;
#[cfg(feature = "geos-3_11")]
mod geom;

pub use aggregate::AsMvt;
#[cfg(feature = "geos-3_11")]
pub use geom::AsMvtGeom;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udaf(AsMvt::default().into());

    #[cfg(feature = "geos-3_11")]
    session_context.register_udf(AsMvtGeom::default().into());
}
