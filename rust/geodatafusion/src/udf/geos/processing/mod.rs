mod line_merge;

pub use line_merge::LineMerge;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(LineMerge::default().into());
}
