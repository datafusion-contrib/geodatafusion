// Temporary
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

pub(crate) mod data_types;
pub(crate) mod error;
pub mod udf;

/// Register all UDFs defined in geodatafusion
pub fn register(session_context: &datafusion::prelude::SessionContext) {
    // Aggregate spatial functions
    crate::udf::geo::aggregate::register(session_context);

    // Measurement functions (area, distance, length, perimeter, azimuth, etc.)
    crate::udf::geo::measurement::register(session_context);

    // Processing functions (buffer, overlay, simplify, affine, editors, linear ref, etc.)
    crate::udf::geo::processing::register(session_context);

    // Spatial relationships (contains, intersects, dwithin, relate, etc.)
    crate::udf::geo::relationships::register(session_context);

    // Validation functions (is_valid, is_valid_reason)
    crate::udf::geo::validation::register(session_context);

    // GeoHash functions
    crate::udf::geohash::register(session_context);

    // Native accessors (x, y, z, m, geometry_type, dimension, is_empty, etc.)
    crate::udf::native::accessors::register(session_context);

    // Bounding box functions
    crate::udf::native::bounding_box::register(session_context);

    // Constructors (point, make_line, make_polygon, make_envelope)
    crate::udf::native::constructors::register(session_context);

    // I/O functions (WKT, WKB, GeoJSON, EWKT)
    crate::udf::native::io::register(session_context);
}
