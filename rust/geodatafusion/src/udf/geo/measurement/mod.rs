mod area;
mod distance;
mod dwithin;
mod geodesic;
mod length;

pub use area::Area;
pub use distance::Distance;
pub use dwithin::{DWithin, DWithinIndexFilter, extract_dwithin_index_filter};
pub use geodesic::{DistanceSphere, DistanceSpheroid, LengthSpheroid};
pub use length::Length;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(Area.into());
    session_context.register_udf(Distance.into());
    session_context.register_udf(DWithin.into());
    session_context.register_udf(DistanceSphere.into());
    session_context.register_udf(DistanceSpheroid.into());
    session_context.register_udf(Length.into());
    session_context.register_udf(LengthSpheroid.into());
}
