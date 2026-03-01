mod advanced;
mod area;
mod distance;
mod length;

pub use advanced::{Azimuth, ClosestPoint, FrechetDistance, HausdorffDistance, MaxDistance, Perimeter};
pub use area::Area;
pub use distance::Distance;
pub use length::Length;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(Area.into());
    session_context.register_udf(Azimuth.into());
    session_context.register_udf(ClosestPoint::default().into());
    session_context.register_udf(Distance.into());
    session_context.register_udf(FrechetDistance.into());
    session_context.register_udf(HausdorffDistance.into());
    session_context.register_udf(Length.into());
    session_context.register_udf(MaxDistance.into());
    session_context.register_udf(Perimeter.into());
}
