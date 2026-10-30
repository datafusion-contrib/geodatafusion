//! Spatial extensions for [Apache DataFusion](https://datafusion.apache.org/).
//!
//! `geodatafusion` provides PostGIS-compatible spatial SQL functions backed by the
//! [GeoArrow](https://geoarrow.org/) columnar memory specification.
//!
//! # Quickstart
//!
//! Use [`register`] to register all available spatial User-Defined Functions (UDFs) and
//! User-Defined Aggregate Functions (UDAFs) with a DataFusion
//! [`SessionContext`](datafusion::prelude::SessionContext):
//!
//! ```
//! use datafusion::prelude::SessionContext;
//!
//! let ctx = SessionContext::new();
//! geodatafusion::register(&ctx);
//! ```
//!
//! # Function Providers
//!
//! UDFs are organized under [`udf`] by implementation backend:
//!
//! - [`udf::native`]: Geometry accessors, bounding-box operations, constructors, coordinate-dimension editors, and WKT/WKB I/O implemented directly on `geoarrow-array` and `geo-traits`.
//! - [`udf::geo`]: 2D measurement, processing, topological relationship, and validation functions implemented with the [`geo`](https://docs.rs/geo) crate.
//! - [`udf::geohash`]: GeoHash encoding and decoding functions (`ST_GeoHash`, `ST_Box2dFromGeoHash`, `ST_PointFromGeoHash`).
//! - `udf::geos` (requires the `geos-3_11` feature): GEOS-backed geometry processing functions such as `ST_LineMerge`.
//!
//! You can register an individual provider module instead of the full suite:
//!
//! ```
//! use datafusion::prelude::SessionContext;
//! use geodatafusion::udf;
//!
//! let ctx = SessionContext::new();
//! udf::native::register(&ctx);
//! udf::geo::register(&ctx);
//! udf::geohash::register(&ctx);
//! ```
//!
//! # Custom Coordinate Layout
//!
//! UDFs that return newly constructed GeoArrow geometries accept a [`geoarrow_schema::CoordType`]
//! (`CoordType::Separated` by default, or `CoordType::Interleaved`) when instantiated directly:
//!
//! ```
//! use datafusion::logical_expr::ScalarUDF;
//! use datafusion::prelude::SessionContext;
//! use geoarrow_schema::CoordType;
//! use geodatafusion::udf::native::constructors::Point;
//!
//! let ctx = SessionContext::new();
//! ctx.register_udf(ScalarUDF::from(Point::new(CoordType::Interleaved)));
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

pub(crate) mod data_types;
pub(crate) mod error;
pub mod udf;

/// Register all UDFs defined in geodatafusion
pub fn register(session_context: &datafusion::prelude::SessionContext) {
    crate::udf::geo::register(session_context);

    #[cfg(feature = "geos-3_11")]
    crate::udf::geos::processing::register(session_context);

    crate::udf::geohash::register(session_context);

    crate::udf::native::register(session_context);
}
