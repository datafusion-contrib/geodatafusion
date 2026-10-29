mod line_merge;

#[cfg(feature = "geos-3_11")]
pub use line_merge::LineMerge;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    #[cfg(feature = "geos-3_11")]
    session_context.register_udf(LineMerge::default().into());
}
