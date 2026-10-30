//! Spatial User-Defined Functions (UDFs) and Aggregate Functions (UDAFs), grouped by backend provider.

pub mod geo;
pub mod geohash;
#[cfg(feature = "geos-3_11")]
/// Spatial UDFs implemented via [GEOS](https://libgeos.org/) bindings (requires the `geos-3_11` feature).
pub mod geos;
pub mod native;
