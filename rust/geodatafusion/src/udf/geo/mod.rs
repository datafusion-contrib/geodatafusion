pub mod measurement;
pub mod processing;
pub mod relationships;
pub mod three_d;
pub(crate) mod to_geo;
pub mod validation;

/// Register every UDF implemented against the geo traits.
///
/// The crate-level [`crate::register`] and the Python bindings both call this
/// function, so a new geo submodule is added in one place and neither entry
/// point can fall behind the other.
pub fn register(ctx: &datafusion::prelude::SessionContext) {
    measurement::register(ctx);
    processing::register(ctx);
    relationships::register(ctx);
    three_d::register(ctx);
    validation::register(ctx);
}
