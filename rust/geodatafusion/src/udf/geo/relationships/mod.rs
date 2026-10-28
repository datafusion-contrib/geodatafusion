mod topological;

pub use topological::{
    Contains, CoveredBy, Covers, Crosses, Disjoint, Equals, Intersects, Overlaps, Touches, Within,
};

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(Contains::default().into());
    session_context.register_udf(CoveredBy::default().into());
    session_context.register_udf(Covers::default().into());
    session_context.register_udf(Crosses::default().into());
    session_context.register_udf(Disjoint::default().into());
    session_context.register_udf(Equals::default().into());
    session_context.register_udf(Intersects::default().into());
    session_context.register_udf(Overlaps::default().into());
    session_context.register_udf(Touches::default().into());
    session_context.register_udf(Within::default().into());
}
