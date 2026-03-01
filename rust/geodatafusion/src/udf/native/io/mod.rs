//! Geometry Input and Output

mod ewkt;
mod geojson;
mod wkb;
mod wkt;

pub use ewkt::AsEWKT;
pub use geojson::{AsGeoJSON, GeomFromGeoJSON};
pub use wkb::{AsBinary, GeomFromWKB};
pub use wkt::{AsText, GeomFromText};

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(AsBinary.into());
    session_context.register_udf(GeomFromWKB::default().into());
    session_context.register_udf(AsText.into());
    session_context.register_udf(GeomFromText::default().into());
    session_context.register_udf(AsGeoJSON.into());
    session_context.register_udf(GeomFromGeoJSON::default().into());
    session_context.register_udf(AsEWKT.into());
}
