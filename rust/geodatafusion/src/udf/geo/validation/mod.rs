//! Geometry validation UDFs implemented with [`geo`](https://docs.rs/geo).

mod is_valid;
mod is_valid_reason;

pub use is_valid::IsValid;
pub use is_valid_reason::IsValidReason;

/// Register all `geo` validation UDFs into the given [`SessionContext`](datafusion::prelude::SessionContext).
pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(IsValid.into());
    session_context.register_udf(IsValidReason.into());
}
