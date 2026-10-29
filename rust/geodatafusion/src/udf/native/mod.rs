//! User-defined functions that wrap native Rust implementations.

pub mod accessors;
pub mod bounding_box;
pub mod clustering;
pub mod constructors;
pub(crate) mod crs;
pub mod editors;
pub(crate) mod geometry;
pub mod io;
pub mod linear_referencing;
pub mod measurement;
pub mod mvt;
pub(crate) mod owned_geometry;
pub mod processing;
mod relationships;
pub mod transformation;

/// Register all native-Rust UDFs.
pub fn register(ctx: &datafusion::prelude::SessionContext) {
    accessors::register(ctx);
    bounding_box::register(ctx);
    clustering::register(ctx);
    constructors::register(ctx);
    editors::register(ctx);
    io::register(ctx);
    linear_referencing::register(ctx);
    measurement::register(ctx);
    processing::register(ctx);
    mvt::register(ctx);
    relationships::register(ctx);
    transformation::register(ctx);
}
