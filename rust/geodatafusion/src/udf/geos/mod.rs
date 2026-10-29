//! UDFs implemented via GEOS bindings.

mod accessors;
pub(crate) mod adapter;
mod aggregates;
mod boundary_polygon;
pub mod coverage;
pub(crate) mod editors;
mod m_dimension;
pub mod measurement;
pub mod processing;
mod raw;
mod relationships;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    accessors::register(session_context);
    aggregates::register(session_context);
    session_context.register_udf(boundary_polygon::BdPolyFromText::default().into());
    session_context.register_udf(boundary_polygon::BdMPolyFromText::default().into());
    measurement::register(session_context);
    processing::register(session_context);
    relationships::register(session_context);
    coverage::register(session_context);
}
