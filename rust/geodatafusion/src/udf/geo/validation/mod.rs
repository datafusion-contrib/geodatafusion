mod is_valid;
mod is_valid_reason;

pub use is_valid::IsValid;
pub use is_valid_reason::IsValidReason;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(IsValid::default().into());
    session_context.register_udf(IsValidReason::default().into());
}
