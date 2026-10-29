// Temporary
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

pub(crate) mod data_types;
pub(crate) mod error;
pub mod spatial_statistics;
pub mod udf;

/// Register all UDFs defined in geodatafusion
pub fn register(session_context: &datafusion::prelude::SessionContext) {
    crate::udf::geo::register(session_context);

    #[cfg(feature = "geos-3_11")]
    crate::udf::geos::register(session_context);

    crate::udf::geohash::register(session_context);

    crate::udf::flatgeobuf::register(session_context);

    crate::udf::native::register(session_context);
}
