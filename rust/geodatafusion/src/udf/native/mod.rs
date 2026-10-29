//! User-defined functions that wrap native Rust implementations.

pub mod accessors;
pub mod bounding_box;
pub mod constructors;
pub mod io;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    accessors::register(session_context);
    bounding_box::register(session_context);
    constructors::register(session_context);
    io::register(session_context);
}
