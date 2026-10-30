//! 2D Cartesian measurement UDFs implemented with [`geo`](https://docs.rs/geo).

mod area;
mod distance;
mod dwithin;
mod length;

pub use area::Area;
pub use distance::Distance;
pub use dwithin::{DWithin, DWithinIndexFilter, extract_dwithin_index_filter};
pub use length::Length;

/// Register all `geo` measurement UDFs into the given [`SessionContext`](datafusion::prelude::SessionContext).
pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(Area.into());
    session_context.register_udf(Distance.into());
    session_context.register_udf(DWithin.into());
    session_context.register_udf(Length.into());
}
