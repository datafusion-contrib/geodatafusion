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
    crate::udf::geo::measurement::register(session_context);

    crate::udf::geo::processing::register(session_context);

    crate::udf::geo::relationships::register(session_context);

    crate::udf::geo::validation::register(session_context);

    crate::udf::geohash::register(session_context);

    crate::udf::native::accessors::register(session_context);

    crate::udf::native::bounding_box::register(session_context);

    crate::udf::native::constructors::register(session_context);

    crate::udf::native::io::register(session_context);
}
